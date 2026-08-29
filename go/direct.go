package agentkit

// RunDirect is the lifecycle for work that is NOT an inference loop.
//
// Some turns are deterministic: a bulk edit, a scripted transformation, a
// command the user typed that needs no model to interpret. Running those
// through a network would invent an inference nobody asked for, so
// applications tend to hand-roll them — opening a stream, appending history,
// publishing a terminal — and end up with a second, subtly different lifecycle
// that pause, cancel and the finalizer never reach.
//
// RunDirect gives that work the SAME lifecycle a network run gets: one
// journaled stream, the user turn persisted up front, safe-boundary
// checkpoints the control plane can pause or cancel at, a finalizer, and
// exactly one terminal event on every exit path.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/eadwinCode/agent-kit/go/durable"
	"github.com/google/uuid"
)

// DirectRunOptions configures one direct run. Everything the network reads
// from its own definition is passed explicitly here, because a direct run has
// no agents to inherit from.
type DirectRunOptions[T any] struct {
	// Name labels this unit of work in the transcript and terminal events,
	// the way an agent name does. Required.
	Name string

	// RunID is the application's stable run identity. Empty mints one behind a
	// durable checkpoint, which is a replay cost worth avoiding.
	RunID string

	// State carries the thread identity and the application's own data.
	// Required.
	State *State[T]

	// Step is the durability seam. Nil uses the Inngest driver.
	Step durable.Step

	// Ports supply journal, session state, control, approvals, step results,
	// sink and finalizer — exactly as they do for a network run.
	Ports *RuntimePorts

	// Streaming configures live delivery. Nil runs silently, which is valid
	// for work whose result is fetched rather than watched.
	Streaming *StreamingConfig

	// History persists the user turn up front and the summary at the end.
	History *HistoryConfig[T]

	// Input is the user's message text.
	Input string

	// UserMessage adopts the client's message identity so the optimistic
	// bubble, the published user turn and canonical history share one id.
	UserMessage *UserMessage
}

// DirectRun is the handle the work callback receives.
type DirectRun[T any] struct {
	// RunID is this run's stable identity.
	RunID string

	// State is the run's state; mutate Data freely.
	State *State[T]

	// Step is the durability seam, already wrapped for external step results
	// when the application supplies that port.
	Step durable.Step

	// Stream is the typed emitter: semantic status, domain data parts,
	// progress, and this work's own declared safe boundaries. Never nil.
	//
	// Use Stream.Checkpoint before an irreversible write and between items of
	// an iterative job. That is what makes a direct run pausable and
	// cancellable at the points where stopping is actually safe.
	Stream StructuredStream
}

// DirectWork is the application's unit of work. Its returned summary becomes
// the assistant's message in canonical history; an empty summary appends
// nothing, which suits work whose visible output was already streamed.
type DirectWork[T any] func(ctx context.Context, run *DirectRun[T]) (string, error)

// RunDirect executes one unit of work under the full run lifecycle.
//
// Returns the work's error unchanged, so callers can classify it. A cancel
// observed at a checkpoint surfaces as an error wrapping ErrRunCancelled, and
// the terminal event is published as cancelled rather than failed.
func RunDirect[T any](
	ctx context.Context,
	opts *DirectRunOptions[T],
	work DirectWork[T],
) (err error) {
	if opts == nil || opts.State == nil {
		return errors.New("agentkit: RunDirect needs options and state")
	}
	if work == nil {
		return errors.New("agentkit: RunDirect needs work to run")
	}
	name := strings.TrimSpace(opts.Name)
	if name == "" {
		return errors.New("agentkit: RunDirect needs a name")
	}

	step := opts.Step
	if step == nil {
		step = durable.Inngest()
	}
	runID := strings.TrimSpace(opts.RunID)
	if runID == "" {
		runID, err = durable.Run(ctx, step, "generate-direct-run-id", func(ctx context.Context) (string, error) {
			return uuid.NewString(), nil
		})
		if err != nil {
			return err
		}
	}

	ports := opts.Ports
	if ports != nil && ports.StepResults != nil {
		wrapped, wrapErr := NewStepResultStep(step, ports.StepResults, StepResultStepConfig{
			Scope: ports.Scope, RunID: runID, SchemaVersion: ContractSchemaVersion,
		})
		if wrapErr != nil {
			return wrapErr
		}
		step = wrapped
	}

	controller := newRunController(ports, nil, nil)
	approvals := newApprovalController(ports, nil)

	input := opts.Input
	if opts.UserMessage != nil && opts.UserMessage.Content != "" {
		input = opts.UserMessage.Content
	}
	cfg := threadOpConfig[T]{State: opts.State, History: opts.History, Input: input, Step: step}

	hadThreadID := opts.State.ThreadID != ""
	if err := initializeThread(ctx, cfg); err != nil {
		return err
	}
	// The user's turn is persisted BEFORE the work runs, so a failure still
	// leaves something to retry.
	var accepted *UserMessageRecord
	if opts.History != nil && opts.History.AppendUserMessage != nil {
		record := directUserMessageRecord(input, opts.UserMessage, runID)
		if err := appendUserMessage(ctx, cfg, record); err != nil {
			return err
		}
		accepted = &record
	}
	if hadThreadID {
		if err := loadThreadFromStorage(ctx, cfg); err != nil {
			return err
		}
	}

	var sc *StreamingContext
	if opts.Streaming != nil && (opts.Streaming.Publish != nil || ports.wantsStream()) {
		sc = streamingContextFromState(opts.State, *opts.Streaming, ports, runID, runID, "direct")
		controller.stream = sc
		controller.journal = sc.journal
		approvals.stream = sc
		sc.PublishEvent(ctx, EventRunStarted, map[string]any{
			"runId": runID, "scope": "direct", "name": name, "messageId": runID,
		})
		markRunExecuting(ctx, ports, runID, opts.State.ThreadID)
		if accepted != nil {
			sc.PublishEvent(ctx, EventUserMessage, map[string]any{
				"messageId": accepted.ID, "runId": runID,
				"role": string(accepted.Role), "content": accepted.Content,
				"timestamp": accepted.Timestamp,
			})
		}

		// ONE terminal emitter for every exit path, exactly as the network
		// has. The Finalizer settles the application's durable facts before
		// the terminal is published.
		terminal := newTerminalEmitter(sc, ports, sc.journal, controller, "direct", name, runID)
		defer func() {
			if p := recover(); p != nil {
				// A control hijack is the executor parking this invocation to
				// run a step: the run has NOT finished, so emitting here would
				// publish a premature terminal on every step boundary.
				if durable.IsControlHijack(p) {
					panic(p)
				}
				terminal.Emit(ctx, fmt.Errorf("agentkit: direct run panicked: %v", p), "", nil)
				panic(p)
			}
			terminal.Emit(ctx, err, "", map[string]any{"messageId": runID})
		}()
	}

	// Before any work: nothing irreversible has happened, so a cancel is free
	// and a pause costs nothing.
	if err = controller.Checkpoint(ctx, Checkpoint{
		Kind: CheckpointRunStart, AgentName: name, Resumable: true,
	}); err != nil {
		return err
	}

	run := &DirectRun[T]{RunID: runID, State: opts.State, Step: step}
	run.Stream = noopStream{}
	if sc != nil {
		run.Stream = newRunStream(sc, controller, name, "")
	}

	summary, workErr := work(ctx, run)
	if workErr != nil {
		err = workErr
		return err
	}

	if err = appendDirectSummary(ctx, cfg, opts, name, runID, summary); err != nil {
		return err
	}
	return nil
}

// directUserMessageRecord adopts the client's message identity when it sent
// one, so the optimistic bubble and canonical history agree.
func directUserMessageRecord(input string, msg *UserMessage, runID string) UserMessageRecord {
	record := UserMessageRecord{
		ID: runID + "-user", Content: input, Role: RoleUser,
		Timestamp: Time{Time: time.Now().UTC()},
	}
	if msg != nil {
		if id := strings.TrimSpace(msg.ID); id != "" {
			record.ID = id
		}
		if msg.Content != "" {
			record.Content = msg.Content
		}
		if msg.Role != "" {
			record.Role = msg.Role
		}
	}
	return record
}

// appendDirectSummary persists the work's result as the assistant's turn.
func appendDirectSummary[T any](
	ctx context.Context,
	cfg threadOpConfig[T],
	opts *DirectRunOptions[T],
	name, runID, summary string,
) error {
	if opts.History == nil || opts.History.AppendResults == nil ||
		strings.TrimSpace(summary) == "" {
		return nil
	}
	result := NewAgentResult(name, []Message{{
		Type: MessageText, Role: RoleAssistant, Content: TextContent(summary),
	}}, nil, Time{Time: time.Now().UTC()})
	result.ID = runID + "-direct"
	return PersistResults(ctx, PersistConfig[T]{
		State: cfg.State, History: cfg.History, Input: cfg.Input, Step: cfg.Step,
	}, []*AgentResult{result}, FinalAppendStepID)
}
