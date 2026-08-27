package agentkit

import (
	"context"
	"errors"
	"testing"

	"github.com/eadwinCode/agent-kit/go/durable"
)

type directHistory struct {
	users   []UserMessageRecord
	results []*AgentResult
}

func directHistoryConfig(h *directHistory) *HistoryConfig[map[string]any] {
	return &HistoryConfig[map[string]any]{
		AppendUserMessage: func(
			_ context.Context, _ HistoryContext[map[string]any], record UserMessageRecord,
		) error {
			h.users = append(h.users, record)
			return nil
		},
		AppendResults: func(
			_ context.Context, _ HistoryContext[map[string]any], results []*AgentResult,
		) error {
			h.results = append(h.results, results...)
			return nil
		},
	}
}

type directFinalizer struct {
	calls    int
	outcomes []RunOutcome
}

func (f *directFinalizer) Finalize(
	_ context.Context, req FinalizeRequest,
) (FinalizeResult, error) {
	f.calls++
	f.outcomes = append(f.outcomes, req.Outcome)
	return FinalizeResult{}, nil
}

func directRunOptions(
	t *testing.T, history *directHistory, control ControlStore, events *[]AgentMessageChunk,
) *DirectRunOptions[map[string]any] {
	t.Helper()
	scope := SessionScope("owner")
	ports := &RuntimePorts{Scope: scope, Control: control, State: newPauseStateStore(scope)}
	return &DirectRunOptions[map[string]any]{
		Name:  "component-update",
		RunID: "run-1",
		State: NewState(StateConfig[map[string]any]{
			Data: map[string]any{}, ThreadID: "thread-1",
		}),
		Step:    durable.Inline{},
		Ports:   ports,
		History: directHistoryConfig(history),
		Input:   "update the hero",
		Streaming: &StreamingConfig{
			Publish: func(_ context.Context, chunk AgentMessageChunk) error {
				*events = append(*events, chunk)
				return nil
			},
		},
	}
}

func terminalCount(events []AgentMessageChunk) int {
	count := 0
	for _, event := range events {
		if event.Event == EventStreamEnded {
			count++
		}
	}
	return count
}

// The whole point of the primitive: deterministic work gets the lifecycle an
// inference run gets, instead of an application hand-rolling a second one.
func TestRunDirectPersistsTheTurnAndPublishesOneTerminal(t *testing.T) {
	history := &directHistory{}
	var events []AgentMessageChunk
	opts := directRunOptions(t, history, nil, &events)
	finalizer := &directFinalizer{}
	opts.Ports.Finalizer = finalizer

	err := RunDirect(context.Background(), opts,
		func(_ context.Context, run *DirectRun[map[string]any]) (string, error) {
			if run.RunID != "run-1" {
				t.Fatalf("run id = %q", run.RunID)
			}
			return "Updated 3 components.", nil
		})
	if err != nil {
		t.Fatal(err)
	}

	// The user's turn is persisted BEFORE the work, so a failure leaves
	// something to retry.
	if len(history.users) != 1 || history.users[0].Content != "update the hero" {
		t.Fatalf("user history = %+v", history.users)
	}
	if len(history.results) != 1 {
		t.Fatalf("result history = %+v", history.results)
	}
	if got := terminalCount(events); got != 1 {
		t.Fatalf("published %d terminal events, want exactly 1", got)
	}
	if finalizer.calls != 1 || finalizer.outcomes[0] != OutcomeCompleted {
		t.Fatalf("finalizer calls=%d outcomes=%v", finalizer.calls, finalizer.outcomes)
	}
}

// A cancel recorded before the work starts must stop it, and settle as
// cancelled rather than failed.
func TestRunDirectStopsAtTheControlPlaneBeforeDoingWork(t *testing.T) {
	history := &directHistory{}
	var events []AgentMessageChunk
	control := &pauseControlStore{signal: ControlSignal{Action: ControlCancel}}
	opts := directRunOptions(t, history, control, &events)
	finalizer := &directFinalizer{}
	opts.Ports.Finalizer = finalizer

	ran := false
	err := RunDirect(context.Background(), opts,
		func(context.Context, *DirectRun[map[string]any]) (string, error) {
			ran = true
			return "should not happen", nil
		})
	if !errors.Is(err, ErrRunCancelled) {
		t.Fatalf("error = %v, want a cancel", err)
	}
	if ran {
		t.Fatal("the work ran after the control plane said cancel")
	}
	if len(history.results) != 0 {
		t.Fatalf("a cancelled run wrote a result: %+v", history.results)
	}
	if got := terminalCount(events); got != 1 {
		t.Fatalf("published %d terminal events, want exactly 1", got)
	}
	if finalizer.calls != 1 || finalizer.outcomes[0] != OutcomeCancelled {
		t.Fatalf("finalizer calls=%d outcomes=%v", finalizer.calls, finalizer.outcomes)
	}
}

// Failing work still settles: one terminal, through the finalizer.
func TestRunDirectSettlesFailedWorkThroughTheFinalizer(t *testing.T) {
	history := &directHistory{}
	var events []AgentMessageChunk
	opts := directRunOptions(t, history, nil, &events)
	finalizer := &directFinalizer{}
	opts.Ports.Finalizer = finalizer

	failure := errors.New("the registry was unreachable")
	err := RunDirect(context.Background(), opts,
		func(context.Context, *DirectRun[map[string]any]) (string, error) {
			return "", failure
		})
	if !errors.Is(err, failure) {
		t.Fatalf("error = %v, want the work's own", err)
	}
	if len(history.results) != 0 {
		t.Fatalf("failed work wrote a result: %+v", history.results)
	}
	if got := terminalCount(events); got != 1 {
		t.Fatalf("published %d terminal events, want exactly 1", got)
	}
	if finalizer.outcomes[0] != OutcomeFailed {
		t.Fatalf("outcome = %v, want failed", finalizer.outcomes[0])
	}
}

// The work can declare its own safe boundaries, which is what makes a long
// direct job pausable at points where stopping is actually safe.
func TestRunDirectGivesTheWorkACheckpointItCanBeStoppedAt(t *testing.T) {
	history := &directHistory{}
	var events []AgentMessageChunk
	control := &pauseControlStore{}
	opts := directRunOptions(t, history, control, &events)

	err := RunDirect(context.Background(), opts,
		func(ctx context.Context, run *DirectRun[map[string]any]) (string, error) {
			// Nothing outstanding at run start; the cancel lands mid-work.
			control.signal = ControlSignal{Action: ControlCancel}
			if err := run.Stream.Checkpoint(ctx, CheckpointBeforeSideEffect); err != nil {
				return "", err
			}
			t.Fatal("the tool-declared boundary did not stop the work")
			return "", nil
		})
	if !errors.Is(err, ErrRunCancelled) {
		t.Fatalf("error = %v, want a cancel", err)
	}
}
