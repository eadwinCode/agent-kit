package agentkit

// Finalizer is the terminal-coordination contract.
//
// AgentKit knows when a run stopped producing. It does not know when the
// run is *finished*: canonical history still has to be written, usage
// billed, repository state published, the active-run lease cleared, and the
// live writer drained. An application that needs those facts settled before
// clients see a terminal has, until now, had to suppress AgentKit's terminal
// and publish its own — which means two sources of truth for the single most
// important event in the protocol.
//
// With a Finalizer, AgentKit holds the terminal. It calls Finalize exactly
// once after the run's own work is done, and publishes run.completed /
// run.failed / stream.ended only after Finalize returns. A finalizer that
// fails does not lose the run: it returns a typed terminal failure that
// becomes the run's published outcome.

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

// FinalizeRequest describes the settled run.
type FinalizeRequest struct {
	Scope    SessionScope   `json:"scope"`
	Identity StreamIdentity `json:"identity"`

	// RunScope is "network" or "agent", matching the terminal event's scope.
	// It describes the SHAPE of the run, not who owns the conversation —
	// Scope above is the owner.
	RunScope string `json:"runScope"`
	// Name is the network or agent name.
	Name string `json:"name"`
	// MessageID is the terminal message identity.
	MessageID string `json:"messageId"`

	// Outcome is AgentKit's view of how the run ended, before application
	// finalization runs.
	Outcome RunOutcome `json:"outcome"`
	// Err is the run error when Outcome is OutcomeFailed.
	Err error `json:"-"`
	// StopReason is the network StopWhen reason, when one applied.
	StopReason string `json:"stopReason,omitempty"`

	// LastCursor is the highest event AgentKit produced, so the finalizer
	// can reconcile the journal against canonical history.
	LastCursor JournalCursor `json:"lastCursor"`
	// ReconcileRequired reports that at least one journal append failed, so
	// the durable tail has holes.
	ReconcileRequired bool `json:"reconcileRequired"`
	// PausedTotalMs is the wall-clock time this SESSION spent paused,
	// carried across replays by persisted state rather than tallied per
	// execution.
	PausedTotalMs int64 `json:"pausedTotalMs"`
}

// FinalizeResult is the application's authorization of the terminal.
type FinalizeResult struct {
	// Outcome may override AgentKit's view — a finalizer that could not
	// bill or persist may downgrade a completion to a failure.
	Outcome RunOutcome `json:"outcome"`
	// ErrorCode is a bounded operational code published with a failure. It
	// must never carry prompts, tool output, or provider payloads.
	ErrorCode string `json:"errorCode,omitempty"`
	// Message is a user-safe terminal message.
	Message string `json:"message,omitempty"`
	// Metadata is bounded terminal metadata for clients.
	Metadata map[string]any `json:"metadata,omitempty"`
	// ReconcileRequired asks clients to reconcile from canonical history
	// rather than trusting the tail.
	ReconcileRequired bool `json:"reconcileRequired"`

	// CompactUpTo authorizes AgentKit to drop journal records up to and
	// including this cursor, once the terminal is published. Only the
	// application knows when canonical history has proven equivalent, so
	// only the finalizer may authorize it; AgentKit owns the timing. Leave
	// nil to keep the whole tail for the retention window.
	CompactUpTo *JournalCursor `json:"compactUpTo,omitempty"`
}

// Finalizer authorizes the one terminal event.
type Finalizer interface {
	// Finalize settles the application's durable facts. AgentKit calls it
	// exactly once per run, after execution stops and before any terminal
	// event is published.
	//
	// Returning an error is a typed terminal failure: AgentKit publishes a
	// failed terminal carrying the error's bounded code instead of
	// swallowing it or emitting a success.
	Finalize(ctx context.Context, req FinalizeRequest) (FinalizeResult, error)
}

// FinalizerFunc adapts a function to Finalizer.
type FinalizerFunc func(ctx context.Context, req FinalizeRequest) (FinalizeResult, error)

// Finalize implements Finalizer.
func (f FinalizerFunc) Finalize(ctx context.Context, req FinalizeRequest) (FinalizeResult, error) {
	return f(ctx, req)
}

// terminalEmitter guarantees exactly one terminal sequence per run scope.
//
// The deferred emitters in Agent.Run and NetworkRun.execute both route
// through it, so a panic path, an early stop, a cancel and a normal drain
// cannot each publish their own terminal. `once` is the enforcement: the
// exactly-one-terminal invariant is a blocking regression in every phase of
// this runtime, so it is structural here rather than a convention.
type terminalEmitter struct {
	sc        *StreamingContext
	ports     *RuntimePorts
	journal   *journalWriter
	runScope  string
	name      string
	messageID string
	once      sync.Once
	// controller supplies the paused-time accounting.
	controller *RunController
}

func newTerminalEmitter(sc *StreamingContext, ports *RuntimePorts, jw *journalWriter, controller *RunController, runScope, name, messageID string) *terminalEmitter {
	return &terminalEmitter{
		sc: sc, ports: ports, journal: jw, controller: controller,
		runScope: runScope, name: name, messageID: messageID,
	}
}

// Emit publishes the run's single terminal sequence, after asking the
// Finalizer (when configured) to settle the application's durable facts.
//
// runErr is AgentKit's own error, stopReason the StopWhen reason, and extra
// carries scope-specific fields already used by the existing emitters.
func (t *terminalEmitter) Emit(ctx context.Context, runErr error, stopReason string, extra map[string]any) {
	if t == nil || t.sc == nil {
		return
	}
	if t.terminalAlreadyJournaled(ctx) {
		return
	}
	t.once.Do(func() {
		outcome := OutcomeCompleted
		switch {
		case runErr != nil && IsCancelled(runErr):
			outcome = OutcomeCancelled
		case runErr != nil:
			outcome = OutcomeFailed
		}

		result := FinalizeResult{
			Outcome:           outcome,
			ErrorCode:         ErrorCode(runErr),
			ReconcileRequired: t.journal.ReconcileRequired(),
		}
		if fin := t.finalizer(); fin != nil {
			t.markTerminalizing(ctx, outcome)
			req := FinalizeRequest{
				Scope:             t.ports.scope(),
				Identity:          t.sc.Identity(),
				RunScope:          t.runScope,
				Name:              t.name,
				MessageID:         t.messageID,
				Outcome:           outcome,
				Err:               runErr,
				StopReason:        stopReason,
				LastCursor:        t.sc.Cursor(),
				ReconcileRequired: t.journal.ReconcileRequired(),
				PausedTotalMs:     t.controller.PausedTotal(ctx).Milliseconds(),
			}
			out, err := fin.Finalize(ctx, req)
			switch {
			case err != nil:
				result.Outcome = OutcomeFailed
				result.ErrorCode = firstNonEmpty(ErrorCode(err), "FINALIZER_FAILED")
				result.Message = "The assistant could not finish settling this turn."
				result.ReconcileRequired = true
				if runErr == nil {
					runErr = err
				}
			default:
				if out.Outcome != OutcomeNone {
					result.Outcome = out.Outcome
				}
				result.ErrorCode = out.ErrorCode
				result.Message = out.Message
				result.Metadata = out.Metadata
				result.CompactUpTo = out.CompactUpTo
				result.ReconcileRequired = result.ReconcileRequired || out.ReconcileRequired
			}
		}

		t.publish(ctx, result, runErr, stopReason, extra)
		t.markTerminal(ctx, result)
		t.compact(ctx, result)
	})
}

func (t *terminalEmitter) finalizer() Finalizer {
	if t.ports == nil {
		return nil
	}
	return t.ports.Finalizer
}

// terminalAlreadyJournaled reports whether an earlier execution of this run
// already published its terminal sequence.
//
// Durable executors are re-entrant: the function body is re-invoked for every
// step, and once the run's own work is memoized, every later invocation
// unwinds through the deferred Emit with a FRESH terminalEmitter. The
// process-local sync.Once above therefore cannot hold the
// exactly-one-terminal invariant on its own — without this gate each unwind
// re-runs the finalizer (which bills) and re-publishes run.completed /
// stream.ended under newly allocated sequence numbers. Those copies drift
// from the original numbering, so the client's sequence-keyed steady-state
// tracker cannot dedupe them: a late run.started re-opens a finished turn,
// and a colliding sequence drops the run's real content frames. The journal
// is the only memory that spans invocations, so it owns this gate.
func (t *terminalEmitter) terminalAlreadyJournaled(ctx context.Context) bool {
	if t.journal == nil || !t.journal.enabled() || t.ports == nil {
		return false
	}
	records, _, err := ReadJournalTail(ctx, t.journal.journal, JournalQuery{
		Scope:    t.ports.scope(),
		ThreadID: t.sc.ThreadID,
		After: JournalCursor{
			RunID:          t.sc.RunID,
			StreamEpoch:    t.sc.streamEpoch,
			SequenceNumber: JournalStart,
		},
	})
	if err != nil {
		// Failing closed would strand the client with no terminal at all, and
		// a journal this sick is already latching reconcile on every append —
		// emit and let reconciliation converge the client instead.
		slog.WarnContext(ctx, "agentkit: terminal gate could not read the journal; emitting",
			"runId", t.sc.RunID, "code", ErrorCode(err), "error", err)
		return false
	}
	for _, record := range records {
		if record.RunID != t.sc.RunID || record.StreamEpoch != t.sc.streamEpoch {
			continue
		}
		switch record.Event {
		case EventRunCompleted, EventRunFailed, EventStreamEnded:
			return true
		}
	}
	return false
}

func (t *terminalEmitter) publish(ctx context.Context, result FinalizeResult, runErr error, stopReason string, extra map[string]any) {
	base := func() map[string]any {
		data := map[string]any{"runId": t.sc.RunID, "scope": t.runScope, "name": t.name}
		for k, v := range extra {
			data[k] = v
		}
		return data
	}

	if result.Outcome == OutcomeFailed || result.Outcome == OutcomeCancelled {
		failed := base()
		failed["error"] = terminalMessage(result, runErr)
		failed["recoverable"] = false
		if result.ErrorCode != "" {
			failed["code"] = result.ErrorCode
		}
		if result.Outcome == OutcomeCancelled {
			failed["cancelled"] = true
		}
		t.sc.PublishEvent(ctx, EventRunFailed, failed)
	}

	completed := base()
	completed["outcome"] = string(result.Outcome)
	if stopReason != "" {
		completed["reason"] = stopReason
	}
	if result.ReconcileRequired {
		completed["reconcileRequired"] = true
	}
	if len(result.Metadata) > 0 {
		completed["metadata"] = result.Metadata
	}
	t.sc.PublishEvent(ctx, EventRunCompleted, completed)

	ended := map[string]any{"scope": t.runScope, "messageId": t.messageID, "outcome": string(result.Outcome)}
	if stopReason != "" {
		ended["reason"] = stopReason
	}
	if result.ReconcileRequired {
		ended["reconcileRequired"] = true
	}
	t.sc.PublishEvent(ctx, EventStreamEnded, ended)
}

// compact drops journal deltas the finalizer said canonical history has
// already absorbed. It runs after the terminal is on the wire: compacting
// first would race a client that is still draining the tail.
func (t *terminalEmitter) compact(ctx context.Context, result FinalizeResult) {
	if result.CompactUpTo == nil || t.ports == nil {
		return
	}
	compactor, ok := t.ports.Journal.(JournalCompactor)
	if !ok {
		return
	}
	if err := compactor.Compact(ctx, t.ports.scope(), t.sc.ThreadID, *result.CompactUpTo); err != nil {
		slog.WarnContext(ctx, "agentkit: journal compaction failed; the tail keeps its retention window",
			"code", ErrorCode(err), "error", err)
	}
}

func (t *terminalEmitter) markTerminalizing(ctx context.Context, outcome RunOutcome) {
	if t.ports == nil || t.ports.State == nil {
		return
	}
	_, _ = mutateState(ctx, t.ports.State, t.ports.scope(), "run.terminalizing", func(s *SessionState) {
		if s.ActiveRun != nil {
			s.ActiveRun.Lifecycle = LifecycleTerminalizing
		}
		s.Activity = Activity{Kind: ActivityFinalizing, Label: "Finishing up", Source: ActivityFromServer}
		_ = outcome
	})
}

func (t *terminalEmitter) markTerminal(ctx context.Context, result FinalizeResult) {
	if t.ports == nil || t.ports.State == nil {
		return
	}
	now := time.Now().UTC()
	_, _ = mutateState(ctx, t.ports.State, t.ports.scope(), "run.terminal", func(s *SessionState) {
		if s.ActiveRun != nil {
			s.ActiveRun.Outcome = result.Outcome
			s.ActiveRun.Lifecycle = LifecycleIdle
		}
		s.Pause = PauseInfo{State: PauseNone, AccumulatedPausedMs: s.Pause.AccumulatedPausedMs, Epoch: s.Pause.Epoch}
		s.Activity = Activity{Kind: ActivityNone}
		if s.Approval.Status == ApprovalPending || s.Approval.Status == ApprovalSettling {
			s.Approval = ApprovalInfo{Status: ApprovalExpired, ApprovalID: s.Approval.ApprovalID}
		}
		s.ReconcileRequired = result.ReconcileRequired
		if result.ErrorCode != "" {
			s.LastErrorCode = result.ErrorCode
		}
		s.UpdatedAt = now
	})
}

func terminalMessage(result FinalizeResult, runErr error) string {
	if result.Message != "" {
		return result.Message
	}
	if result.Outcome == OutcomeCancelled {
		return errors.New("agentkit: run cancelled").Error()
	}
	if runErr != nil || result.Outcome == OutcomeFailed {
		// Runtime errors may contain provider payloads, tool inputs, file paths,
		// query text or credentials. Public events carry only this user-safe
		// message and the bounded ErrorCode above.
		return "The assistant could not complete this turn."
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
