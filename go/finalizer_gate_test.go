package agentkit

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// gateJournal is the smallest EventJournal that honours the contract the
// terminal gate relies on: idempotent append keyed by EventID and an
// ordered, cursor-filtered read.
type gateJournal struct {
	mu      sync.Mutex
	records []JournalRecord
	readErr error
}

func (j *gateJournal) Append(_ context.Context, records []JournalRecord) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, record := range records {
		duplicate := false
		for _, existing := range j.records {
			if existing.EventID == record.EventID {
				duplicate = true
				break
			}
		}
		if !duplicate {
			j.records = append(j.records, record)
		}
	}
	return nil
}

func (j *gateJournal) Read(_ context.Context, q JournalQuery) (JournalPage, error) {
	if j.readErr != nil {
		return JournalPage{Next: q.After}, j.readErr
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	page := JournalPage{Next: q.After}
	for _, record := range j.records {
		if record.ThreadID != q.ThreadID || record.RunID != q.After.RunID ||
			record.StreamEpoch != q.After.StreamEpoch ||
			record.SequenceNumber <= q.After.SequenceNumber {
			continue
		}
		page.Records = append(page.Records, record)
		page.Next = JournalCursor{
			RunID: record.RunID, StreamEpoch: record.StreamEpoch,
			SequenceNumber: record.SequenceNumber,
		}
	}
	return page, nil
}

type publishCapture struct {
	mu     sync.Mutex
	chunks []AgentMessageChunk
}

func (c *publishCapture) publish(_ context.Context, chunk AgentMessageChunk) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.chunks = append(c.chunks, chunk)
	return nil
}

func (c *publishCapture) events() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	events := make([]string, 0, len(c.chunks))
	for _, chunk := range c.chunks {
		events = append(events, chunk.Event)
	}
	return events
}

func gateFixture(t *testing.T, journal EventJournal) (
	*terminalEmitter, *publishCapture, *int,
) {
	t.Helper()
	capture := &publishCapture{}
	finalizerCalls := new(int)
	ports := &RuntimePorts{
		Journal: journal, Scope: "scope_1", StreamEpoch: 7,
		Finalizer: FinalizerFunc(func(context.Context, FinalizeRequest) (FinalizeResult, error) {
			*finalizerCalls++
			return FinalizeResult{}, nil
		}),
	}
	sc := newStreamingContext(
		StreamingConfig{Publish: capture.publish},
		ports, "run_1", "msg_1", "network", "th_1", "user_1",
	)
	emitter := newTerminalEmitter(sc, ports, sc.journal, nil, "network", "net", "msg_1")
	return emitter, capture, finalizerCalls
}

// The journal already holds this run's terminal: a later executor invocation
// unwinding through the deferred Emit must not re-run the finalizer (it
// bills) or re-publish the terminal sequence.
func TestTerminalEmitterSkipsWhenTerminalIsJournaled(t *testing.T) {
	journal := &gateJournal{records: []JournalRecord{
		{Scope: "scope_1", EventID: "run_1:7:0", Event: EventRunStarted,
			ThreadID: "th_1", RunID: "run_1", StreamEpoch: 7, SequenceNumber: 0},
		{Scope: "scope_1", EventID: "run_1:7:9", Event: EventStreamEnded,
			ThreadID: "th_1", RunID: "run_1", StreamEpoch: 7, SequenceNumber: 9},
	}}
	emitter, capture, finalizerCalls := gateFixture(t, journal)
	emitter.Emit(context.Background(), nil, "", map[string]any{"messageId": "msg_1"})
	if got := capture.events(); len(got) != 0 {
		t.Fatalf("republished terminal sequence: %v", got)
	}
	if *finalizerCalls != 0 {
		t.Fatalf("finalizer ran %d times on a replayed unwind, want 0", *finalizerCalls)
	}
}

// A clean journal gets exactly one terminal sequence; a second emitter — the
// next executor invocation, with its own sync.Once — adds nothing.
func TestTerminalEmitterEmitsOnceAcrossInvocations(t *testing.T) {
	journal := &gateJournal{}
	emitter, capture, finalizerCalls := gateFixture(t, journal)
	emitter.Emit(context.Background(), nil, "", map[string]any{"messageId": "msg_1"})
	want := []string{EventRunCompleted, EventStreamEnded}
	if got := capture.events(); len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("terminal sequence = %v, want %v", got, want)
	}

	replayed, capture2, _ := gateFixture(t, journal)
	replayed.sc.lastSeq.Store(10)
	replayed.Emit(context.Background(), nil, "", map[string]any{"messageId": "msg_1"})
	if got := capture2.events(); len(got) != 0 {
		t.Fatalf("second invocation published %v, want nothing", got)
	}
	if *finalizerCalls != 1 {
		t.Fatalf("finalizer ran %d times, want exactly 1", *finalizerCalls)
	}
}

// A terminal from a DIFFERENT epoch belongs to a run this one replaced; it
// must not suppress this epoch's terminal.
func TestTerminalEmitterIgnoresOtherEpochs(t *testing.T) {
	journal := &gateJournal{records: []JournalRecord{
		{Scope: "scope_1", EventID: "run_1:6:9", Event: EventStreamEnded,
			ThreadID: "th_1", RunID: "run_1", StreamEpoch: 6, SequenceNumber: 9},
	}}
	emitter, capture, _ := gateFixture(t, journal)
	emitter.Emit(context.Background(), nil, "", map[string]any{"messageId": "msg_1"})
	if got := capture.events(); len(got) != 2 {
		t.Fatalf("events = %v, want the terminal sequence to publish", got)
	}
}

// A journal that cannot be read fails open: stranding the client with no
// terminal is worse than a duplicate reconciliation will converge.
func TestTerminalGateFailsOpenOnReadError(t *testing.T) {
	journal := &gateJournal{readErr: errors.New("journal unreachable")}
	emitter, capture, _ := gateFixture(t, journal)
	emitter.Emit(context.Background(), nil, "", map[string]any{"messageId": "msg_1"})
	if got := capture.events(); len(got) != 2 {
		t.Fatalf("events = %v, want the terminal sequence despite the read error", got)
	}
}
