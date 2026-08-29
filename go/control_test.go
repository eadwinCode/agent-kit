package agentkit

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// pauseStateStore is a minimal in-process StateStore. The controller tests
// live in package agentkit (newRunController is unexported), so they cannot
// import memadapter without a cycle.
type pauseStateStore struct {
	mu    sync.Mutex
	state SessionState
}

func newPauseStateStore(scope SessionScope) *pauseStateStore {
	return &pauseStateStore{state: SessionState{Scope: scope, Revision: 1}}
}

func (s *pauseStateStore) Load(context.Context, SessionScope) (SessionState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state, nil
}

func (s *pauseStateStore) CompareAndSwap(_ context.Context, t StateTransition) (SessionState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t.ExpectedRevision != 0 && t.ExpectedRevision != s.state.Revision {
		return s.state, ErrRevisionMismatch
	}
	next := s.state
	t.Apply(&next)
	next.Revision = s.state.Revision + 1
	s.state = next
	return next, nil
}

// errSuspended stands in for the workflow engine parking the invocation.
// A real durable Wait unwinds the goroutine the same way, which is why
// pause() has to be re-entrant.
var errSuspended = errors.New("suspended")

type pauseControlStore struct {
	signal  ControlSignal
	waitIDs []string
	// suspend makes Wait unwind instead of returning, simulating the engine
	// parking this invocation to run the wait as a durable step.
	suspend bool
	resume  ControlSignal
}

func (c *pauseControlStore) Record(context.Context, ControlCommand) (CommandResult, error) {
	return CommandResult{}, nil
}

func (c *pauseControlStore) Poll(context.Context, Checkpoint) (ControlSignal, error) {
	return c.signal, nil
}

func (c *pauseControlStore) Wait(_ context.Context, w PauseWait) (ControlSignal, error) {
	c.waitIDs = append(c.waitIDs, w.WaitID)
	if c.suspend {
		panic(errSuspended)
	}
	return c.resume, nil
}

type pausedEvent struct {
	state string
	epoch int
	total int64
}

// pauseHarness is one execution of a run: a fresh controller over shared
// durable state, exactly as a workflow replay rebuilds it.
type pauseHarness struct {
	controller *RunController
	control    *pauseControlStore
	events     *[]pausedEvent
}

func newPauseHarness(
	store *pauseStateStore,
	control *pauseControlStore,
	scope SessionScope,
	now func() time.Time,
	events *[]pausedEvent,
) pauseHarness {
	ports := &RuntimePorts{Control: control, State: store, Scope: scope, StreamEpoch: 3}
	stream := newStreamingContext(StreamingConfig{
		Publish: func(_ context.Context, chunk AgentMessageChunk) error {
			state, ok := chunk.Data["pauseState"].(string)
			if !ok {
				return nil
			}
			event := pausedEvent{state: state}
			if epoch, ok := chunk.Data["pauseEpoch"].(int); ok {
				event.epoch = epoch
			}
			if total, ok := chunk.Data["accumulatedPausedMs"].(int64); ok {
				event.total = total
			}
			*events = append(*events, event)
			return nil
		},
	}, ports, "run-1", "message-1", "network", "thread-1", "user-1")
	controller := newRunController(ports, stream, nil)
	controller.now = now
	controller.withinStep = func(context.Context) bool { return false }
	return pauseHarness{controller: controller, control: control, events: events}
}

// suspendingCheckpoint runs one checkpoint whose Wait parks the invocation,
// recovering the unwind the way agent.go and network.go re-panic it.
func suspendingCheckpoint(t *testing.T, h pauseHarness, cp Checkpoint) {
	t.Helper()
	defer func() {
		if p := recover(); p != nil && p != error(errSuspended) {
			t.Fatalf("unexpected panic: %v", p)
		}
	}()
	_ = h.controller.Checkpoint(context.Background(), cp)
	t.Fatal("Wait returned instead of suspending the invocation")
}

func TestPausedTimeIsMeasuredAcrossAWorkflowReplay(t *testing.T) {
	scope := SessionScope("session-1")
	store := newPauseStateStore(scope)
	clock := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	now := func() time.Time { return clock }
	var events []pausedEvent
	cp := Checkpoint{Kind: CheckpointAfterInference, Resumable: true}

	// Execution 1: the control plane asks for a pause and the durable wait
	// parks the invocation.
	first := newPauseHarness(store,
		&pauseControlStore{signal: ControlSignal{Action: ControlPause}, suspend: true},
		scope, now, &events)
	suspendingCheckpoint(t, first, cp)

	parked, _ := store.Load(context.Background(), scope)
	if parked.Pause.State != PausePaused || parked.Pause.Epoch != 1 {
		t.Fatalf("parked state = %+v", parked.Pause)
	}

	// The user resumes five minutes later. The engine replays the function,
	// so the controller is rebuilt with a clock that never saw the pause.
	clock = clock.Add(5 * time.Minute)
	second := newPauseHarness(store,
		&pauseControlStore{signal: ControlSignal{Action: ControlContinue}},
		scope, now, &events)
	if err := second.controller.Checkpoint(context.Background(), cp); err != nil {
		t.Fatal(err)
	}

	settled, _ := store.Load(context.Background(), scope)
	if settled.Pause.State != PauseNone || settled.Pause.PausedAt != nil {
		t.Fatalf("settled state = %+v", settled.Pause)
	}
	if settled.Pause.AccumulatedPausedMs != (5 * time.Minute).Milliseconds() {
		t.Fatalf("accumulated = %dms, want %dms",
			settled.Pause.AccumulatedPausedMs, (5 * time.Minute).Milliseconds())
	}
	// The terminal event is emitted by THIS execution, which never entered
	// pause(); the total must still come back.
	if got := second.controller.PausedTotal(context.Background()); got != 5*time.Minute {
		t.Fatalf("PausedTotal = %v, want 5m", got)
	}
}

func TestReplayedPauseAdoptsTheInFlightPauseInsteadOfAnnouncingASecond(t *testing.T) {
	scope := SessionScope("session-2")
	store := newPauseStateStore(scope)
	clock := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	now := func() time.Time { return clock }
	var events []pausedEvent
	cp := Checkpoint{Kind: CheckpointBeforeTool, Resumable: true}

	first := newPauseHarness(store,
		&pauseControlStore{signal: ControlSignal{Action: ControlPause}, suspend: true},
		scope, now, &events)
	suspendingCheckpoint(t, first, cp)

	// A retry while the run is still paused: the control plane still reports
	// the pause, so this execution re-enters pause() and must adopt it.
	clock = clock.Add(90 * time.Second)
	retryControl := &pauseControlStore{
		signal: ControlSignal{Action: ControlPause},
		resume: ControlSignal{Action: ControlContinue},
	}
	second := newPauseHarness(store, retryControl, scope, now, &events)
	if err := second.controller.Checkpoint(context.Background(), cp); err != nil {
		t.Fatal(err)
	}

	paused := 0
	for _, event := range events {
		if event.state == string(PausePaused) {
			paused++
		}
	}
	if paused != 1 {
		t.Fatalf("published %d paused events across the replay, want 1: %+v", paused, events)
	}
	if got := first.control.waitIDs[0]; got != retryControl.waitIDs[0] {
		t.Fatalf("wait ids diverged across the replay: %q vs %q", got, retryControl.waitIDs[0])
	}
	if got := first.control.waitIDs[0]; got != "pause-e3-1:before_tool" {
		t.Fatalf("wait id = %q", got)
	}
	settled, _ := store.Load(context.Background(), scope)
	if settled.Pause.Epoch != 1 {
		t.Fatalf("epoch = %d, want the adopted 1", settled.Pause.Epoch)
	}
	// The pause began before the replay, so the whole 90s is banked.
	if settled.Pause.AccumulatedPausedMs != (90 * time.Second).Milliseconds() {
		t.Fatalf("accumulated = %dms", settled.Pause.AccumulatedPausedMs)
	}
}

func TestPauseEpochsDoNotCollideAcrossExecutions(t *testing.T) {
	scope := SessionScope("session-3")
	store := newPauseStateStore(scope)
	clock := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	now := func() time.Time { return clock }
	var events []pausedEvent
	cp := Checkpoint{Kind: CheckpointNetworkIteration, Resumable: true}

	// A pause that completes inside one execution.
	first := newPauseHarness(store, &pauseControlStore{
		signal: ControlSignal{Action: ControlPause},
		resume: ControlSignal{Action: ControlContinue},
	}, scope, now, &events)
	if err := first.controller.Checkpoint(context.Background(), cp); err != nil {
		t.Fatal(err)
	}

	// A second pause, from a rebuilt controller. An in-memory counter would
	// hand out epoch 1 again and a stale resume could then wake this pause.
	second := newPauseHarness(store, &pauseControlStore{
		signal: ControlSignal{Action: ControlPause},
		resume: ControlSignal{Action: ControlContinue},
	}, scope, now, &events)
	if err := second.controller.Checkpoint(context.Background(), cp); err != nil {
		t.Fatal(err)
	}

	settled, _ := store.Load(context.Background(), scope)
	if settled.Pause.Epoch != 2 {
		t.Fatalf("second pause epoch = %d, want 2", settled.Pause.Epoch)
	}
	if second.control.waitIDs[0] == first.control.waitIDs[0] {
		t.Fatalf("both pauses reused wait id %q", first.control.waitIDs[0])
	}
}

func TestCheckpointInsideADurableStepIsNotResumable(t *testing.T) {
	scope := SessionScope("session-4")
	store := newPauseStateStore(scope)
	now := func() time.Time { return time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC) }
	var events []pausedEvent
	control := &pauseControlStore{signal: ControlSignal{Action: ControlPause}}

	h := newPauseHarness(store, control, scope, now, &events)
	// No workflow engine can park a function from inside one of its own
	// steps, so a boundary raised there must report intent and continue.
	h.controller.withinStep = func(context.Context) bool { return true }

	if err := h.controller.Checkpoint(context.Background(), Checkpoint{
		Kind: CheckpointBeforeSideEffect, Resumable: true,
	}); err != nil {
		t.Fatal(err)
	}
	if len(control.waitIDs) != 0 {
		t.Fatalf("entered a durable wait from inside a step: %v", control.waitIDs)
	}
	state, _ := store.Load(context.Background(), scope)
	if state.Pause.State != PauseRequested {
		t.Fatalf("pause state = %q, want %q", state.Pause.State, PauseRequested)
	}
}
