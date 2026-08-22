// Package conformance is the executable definition of AgentKit's runtime
// port contracts.
//
// A contract written only in prose drifts: every adapter reads it slightly
// differently, and the differences surface as production incidents —
// duplicated side effects after a replay, a tail with silent holes, a
// compare-and-swap that quietly overwrites a concurrent command. These
// suites turn the prose into assertions any adapter can run:
//
//	func TestMyJournal(t *testing.T) {
//		conformance.VerifyEventJournal(t, func() agentkit.EventJournal {
//			return newMyJournal(t)
//		})
//	}
//
// Each Verify* function is self-contained and uses only the public API, so
// an application adapter in another repository can import it without
// depending on AgentKit internals. A factory is called once per subtest and
// must return a fresh, empty store.
package conformance

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	agentkit "github.com/eadwinCode/agent-kit/go"
)

// JournalFactory returns a fresh, empty EventJournal.
type JournalFactory func() agentkit.EventJournal

// StateStoreFactory returns a fresh, empty StateStore.
type StateStoreFactory func() agentkit.StateStore

// ControlStoreFactory returns a fresh, empty ControlStore.
type ControlStoreFactory func() agentkit.ControlStore

// ApprovalStoreFactory returns a fresh, empty ApprovalStore.
type ApprovalStoreFactory func() agentkit.ApprovalStore

// testScope is an opaque owner token. Its internal shape is the
// application's business; the contract only requires that it is stable and
// comparable, so the suite uses a composite string to make that concrete
// without implying AgentKit understands the parts.
const testScope agentkit.SessionScope = "owner-scope-1"

func record(seq int, event string) agentkit.JournalRecord {
	return agentkit.JournalRecord{
		Scope:          testScope,
		SchemaVersion:  agentkit.ContractSchemaVersion,
		EventID:        eventID(seq, 0),
		Event:          event,
		ThreadID:       "thread_1",
		RunID:          "run_1",
		StreamEpoch:    0,
		SequenceNumber: seq,
		Timestamp:      int64(1_700_000_000_000 + seq),
		Data:           json.RawMessage(`{"threadId":"thread_1"}`),
	}
}

func eventID(seq, epoch int) string {
	return "run_1:" + itoa(epoch) + ":" + itoa(seq)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func startQuery() agentkit.JournalQuery {
	return agentkit.JournalQuery{
		Scope:    testScope,
		ThreadID: "thread_1",
		After: agentkit.JournalCursor{
			RunID: "run_1", StreamEpoch: 0, SequenceNumber: agentkit.JournalStart,
		},
	}
}

// VerifyEventJournal checks every property AgentKit relies on: ordered
// append, exclusive cursor reads, idempotency on EventID, epoch isolation,
// and pagination that terminates.
func VerifyEventJournal(t *testing.T, newJournal JournalFactory) {
	t.Helper()
	ctx := context.Background()

	t.Run("appends and reads back in order", func(t *testing.T) {
		j := newJournal()
		for i := 0; i < 5; i++ {
			if err := j.Append(ctx, []agentkit.JournalRecord{record(i, "text.delta")}); err != nil {
				t.Fatalf("Append(%d): %v", i, err)
			}
		}
		got, _, err := agentkit.ReadJournalTail(ctx, j, startQuery())
		if err != nil {
			t.Fatalf("ReadJournalTail: %v", err)
		}
		if len(got) != 5 {
			t.Fatalf("read %d records, want 5", len(got))
		}
		for i, rec := range got {
			if rec.SequenceNumber != i {
				t.Fatalf("record %d has sequenceNumber %d; the tail must be ordered", i, rec.SequenceNumber)
			}
		}
	})

	t.Run("round-trips the opaque payload byte for byte", func(t *testing.T) {
		j := newJournal()
		rec := record(0, "data.delta")
		// Key order and spacing must survive: AgentKit never reinterprets a
		// record's data, and a re-marshaled map would reorder keys.
		rec.Data = json.RawMessage(`{"z":1,"a":{"nested":[1,2,3]},"m":"<&>"}`)
		if err := j.Append(ctx, []agentkit.JournalRecord{rec}); err != nil {
			t.Fatalf("Append: %v", err)
		}
		got, _, err := agentkit.ReadJournalTail(ctx, j, startQuery())
		if err != nil {
			t.Fatalf("ReadJournalTail: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("read %d records, want 1", len(got))
		}
		if string(got[0].Data) != string(rec.Data) {
			t.Fatalf("payload round-trip changed the bytes:\n got %s\nwant %s", got[0].Data, rec.Data)
		}
	})

	t.Run("append is idempotent on EventID", func(t *testing.T) {
		j := newJournal()
		rec := record(0, "text.delta")
		for i := 0; i < 3; i++ {
			if err := j.Append(ctx, []agentkit.JournalRecord{rec}); err != nil {
				t.Fatalf("Append attempt %d: %v", i, err)
			}
		}
		got, _, err := agentkit.ReadJournalTail(ctx, j, startQuery())
		if err != nil {
			t.Fatalf("ReadJournalTail: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("re-appending one EventID produced %d records; replays must not duplicate the tail", len(got))
		}
	})

	t.Run("read is exclusive of the cursor", func(t *testing.T) {
		j := newJournal()
		for i := 0; i < 4; i++ {
			if err := j.Append(ctx, []agentkit.JournalRecord{record(i, "text.delta")}); err != nil {
				t.Fatalf("Append(%d): %v", i, err)
			}
		}
		q := startQuery()
		q.After.SequenceNumber = 1
		got, _, err := agentkit.ReadJournalTail(ctx, j, q)
		if err != nil {
			t.Fatalf("ReadJournalTail: %v", err)
		}
		if len(got) != 2 || got[0].SequenceNumber != 2 {
			t.Fatalf("read after cursor 1 returned %d records starting at %v; want 2 starting at 2",
				len(got), firstSeq(got))
		}
	})

	t.Run("keeps epochs distinct", func(t *testing.T) {
		j := newJournal()
		old := record(0, "text.delta")
		fresh := record(0, "text.delta")
		fresh.StreamEpoch = 1
		fresh.EventID = eventID(0, 1)
		if err := j.Append(ctx, []agentkit.JournalRecord{old, fresh}); err != nil {
			t.Fatalf("Append: %v", err)
		}
		q := startQuery()
		q.After.StreamEpoch = 1
		got, _, err := agentkit.ReadJournalTail(ctx, j, q)
		if err != nil {
			t.Fatalf("ReadJournalTail: %v", err)
		}
		if len(got) != 1 || got[0].StreamEpoch != 1 {
			t.Fatalf("reading epoch 1 returned %d records; a new epoch must not replay the old one's events", len(got))
		}
	})

	t.Run("pagination terminates", func(t *testing.T) {
		j := newJournal()
		for i := 0; i < 7; i++ {
			if err := j.Append(ctx, []agentkit.JournalRecord{record(i, "text.delta")}); err != nil {
				t.Fatalf("Append(%d): %v", i, err)
			}
		}
		q := startQuery()
		q.Limit = 2
		got, page, err := agentkit.ReadJournalTail(ctx, j, q)
		if err != nil {
			t.Fatalf("ReadJournalTail: %v", err)
		}
		if len(got) != 7 {
			t.Fatalf("paged drain returned %d records, want 7", len(got))
		}
		if page.HasMore {
			t.Fatal("drained journal still reports HasMore")
		}
	})

	t.Run("empty read returns the query cursor", func(t *testing.T) {
		j := newJournal()
		_, page, err := agentkit.ReadJournalTail(ctx, j, startQuery())
		if err != nil {
			t.Fatalf("ReadJournalTail: %v", err)
		}
		if page.Next.SequenceNumber != agentkit.JournalStart {
			t.Fatalf("empty read advanced the cursor to %d; it must stay put", page.Next.SequenceNumber)
		}
	})

	t.Run("isolates identical stream identities by opaque scope", func(t *testing.T) {
		j := newJournal()
		first := record(0, "text.delta")
		second := first
		second.Scope = "owner-scope-2"
		if err := j.Append(ctx, []agentkit.JournalRecord{first, second}); err != nil {
			t.Fatalf("Append: %v", err)
		}
		got, _, err := agentkit.ReadJournalTail(ctx, j, startQuery())
		if err != nil {
			t.Fatalf("ReadJournalTail: %v", err)
		}
		if len(got) != 1 || got[0].Scope != testScope {
			t.Fatalf("scope-isolated read returned %+v", got)
		}
	})
}

func firstSeq(records []agentkit.JournalRecord) any {
	if len(records) == 0 {
		return "none"
	}
	return records[0].SequenceNumber
}

// VerifyStateStore checks load-creates-idle, monotonic revisions, real CAS
// rejection, and that a rejected swap changes nothing.
func VerifyStateStore(t *testing.T, newStore StateStoreFactory) {
	t.Helper()
	ctx := context.Background()

	t.Run("load returns an idle state for an unknown session", func(t *testing.T) {
		s := newStore()
		state, err := s.Load(ctx, testScope)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if state.ActiveRun != nil {
			t.Fatal("a session with no history must load with no active run")
		}
		if state.Pause.State != agentkit.PauseNone && state.Pause.State != "" {
			t.Fatalf("pause state %q; want none", state.Pause.State)
		}
	})

	t.Run("revision increases exactly once per commit", func(t *testing.T) {
		s := newStore()
		before, err := s.Load(ctx, testScope)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		after, err := s.CompareAndSwap(ctx, agentkit.StateTransition{
			Scope:            testScope,
			ExpectedRevision: before.Revision,
			Reason:           "test",
			Apply: func(st *agentkit.SessionState) {
				st.Activity = agentkit.Activity{Kind: agentkit.ActivityResponding}
			},
		})
		if err != nil {
			t.Fatalf("CompareAndSwap: %v", err)
		}
		if after.Revision != before.Revision+1 {
			t.Fatalf("revision went %d -> %d; want exactly one increment", before.Revision, after.Revision)
		}
		if after.Activity.Kind != agentkit.ActivityResponding {
			t.Fatalf("committed activity %q; the transition was not applied", after.Activity.Kind)
		}
	})

	t.Run("stale compare-and-swap is rejected and changes nothing", func(t *testing.T) {
		s := newStore()
		base, err := s.Load(ctx, testScope)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		winner, err := s.CompareAndSwap(ctx, agentkit.StateTransition{
			Scope: testScope, ExpectedRevision: base.Revision,
			Apply: func(st *agentkit.SessionState) { st.CheckpointKind = "winner" },
		})
		if err != nil {
			t.Fatalf("first CompareAndSwap: %v", err)
		}

		loser, err := s.CompareAndSwap(ctx, agentkit.StateTransition{
			Scope: testScope, ExpectedRevision: base.Revision,
			Apply: func(st *agentkit.SessionState) { st.CheckpointKind = "loser" },
		})
		if !errors.Is(err, agentkit.ErrRevisionMismatch) {
			t.Fatalf("stale swap error = %v; want ErrRevisionMismatch", err)
		}
		if loser.Revision != winner.Revision {
			t.Fatalf("rejected swap returned revision %d; it must return the authoritative state (%d)",
				loser.Revision, winner.Revision)
		}

		current, err := s.Load(ctx, testScope)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if current.CheckpointKind != "winner" {
			t.Fatalf("stored checkpoint %q; a rejected swap must not mutate state", current.CheckpointKind)
		}
	})

	t.Run("unconditional swap commits against any revision", func(t *testing.T) {
		s := newStore()
		if _, err := s.CompareAndSwap(ctx, agentkit.StateTransition{
			Scope: testScope,
			Apply: func(st *agentkit.SessionState) { st.CheckpointKind = "first" },
		}); err != nil {
			t.Fatalf("CompareAndSwap: %v", err)
		}
		got, err := s.CompareAndSwap(ctx, agentkit.StateTransition{
			Scope: testScope,
			Apply: func(st *agentkit.SessionState) { st.CheckpointKind = "second" },
		})
		if err != nil {
			t.Fatalf("second CompareAndSwap: %v", err)
		}
		if got.CheckpointKind != "second" {
			t.Fatalf("checkpoint %q; want second", got.CheckpointKind)
		}
	})
}

// VerifyControlStore checks command idempotency, key-reuse detection, that
// Poll observes accepted intent, that a resume recorded before the wait
// still wakes it, and that cancel is terminal.
func VerifyControlStore(t *testing.T, newStore ControlStoreFactory) {
	t.Helper()
	ctx := context.Background()

	identity := agentkit.StreamIdentity{ThreadID: "thread_1", RunID: "run_1"}
	checkpoint := agentkit.Checkpoint{
		Scope: testScope, Identity: identity,
		Kind: agentkit.CheckpointAfterInference, Resumable: true,
	}

	t.Run("replaying a command returns the recorded result", func(t *testing.T) {
		c := newStore()
		cmd := agentkit.ControlCommand{
			Scope: testScope, ID: "cmd_1", Type: agentkit.CommandPause,
			RunID: "run_1", PayloadHash: "hash_1",
		}
		first, err := c.Record(ctx, cmd)
		if err != nil {
			t.Fatalf("Record: %v", err)
		}
		if first.Duplicate {
			t.Fatal("the first Record must not be marked duplicate")
		}
		second, err := c.Record(ctx, cmd)
		if err != nil {
			t.Fatalf("replayed Record: %v", err)
		}
		if !second.Duplicate {
			t.Fatal("replaying a command ID with the same payload must return the recorded result")
		}
	})

	t.Run("reusing a command ID with a different payload is rejected", func(t *testing.T) {
		c := newStore()
		cmd := agentkit.ControlCommand{
			Scope: testScope, ID: "cmd_1", Type: agentkit.CommandPause,
			RunID: "run_1", PayloadHash: "hash_1",
		}
		if _, err := c.Record(ctx, cmd); err != nil {
			t.Fatalf("Record: %v", err)
		}
		cmd.PayloadHash = "hash_2"
		if _, err := c.Record(ctx, cmd); !errors.Is(err, agentkit.ErrIdempotencyKeyReuse) {
			t.Fatalf("error = %v; want ErrIdempotencyKeyReuse", err)
		}
	})

	t.Run("poll observes an accepted pause and is otherwise continue", func(t *testing.T) {
		c := newStore()
		signal, err := c.Poll(ctx, checkpoint)
		if err != nil {
			t.Fatalf("Poll: %v", err)
		}
		if signal.Action != agentkit.ControlContinue {
			t.Fatalf("idle poll returned %q; want continue", signal.Action)
		}
		if _, err := c.Record(ctx, agentkit.ControlCommand{
			Scope: testScope, ID: "cmd_pause", Type: agentkit.CommandPause, RunID: "run_1",
		}); err != nil {
			t.Fatalf("Record: %v", err)
		}
		signal, err = c.Poll(ctx, checkpoint)
		if err != nil {
			t.Fatalf("Poll: %v", err)
		}
		if signal.Action != agentkit.ControlPause {
			t.Fatalf("poll after pause returned %q; want pause", signal.Action)
		}
	})

	t.Run("a resume recorded before the wait still wakes it", func(t *testing.T) {
		// Inngest documents that an event sent before its wait registers can
		// be missed. The durable command record — not the wake event — is
		// what makes resume reliable, so this ordering must work.
		c := newStore()
		if _, err := c.Record(ctx, agentkit.ControlCommand{
			Scope: testScope, ID: "cmd_pause", Type: agentkit.CommandPause, RunID: "run_1",
		}); err != nil {
			t.Fatalf("Record pause: %v", err)
		}
		if _, err := c.Record(ctx, agentkit.ControlCommand{
			Scope: testScope, ID: "cmd_resume", Type: agentkit.CommandResume,
			RunID: "run_1", PauseEpoch: 1,
		}); err != nil {
			t.Fatalf("Record resume: %v", err)
		}

		waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		signal, err := c.Wait(waitCtx, agentkit.PauseWait{
			Scope: testScope, Identity: identity, PauseEpoch: 1,
			Checkpoint: agentkit.CheckpointAfterInference,
		})
		if err != nil {
			t.Fatalf("Wait: %v", err)
		}
		if signal.Action != agentkit.ControlContinue {
			t.Fatalf("wait woke with %q; want continue", signal.Action)
		}
	})

	t.Run("a resume recorded after the wait began wakes it", func(t *testing.T) {
		c := newStore()
		if _, err := c.Record(ctx, agentkit.ControlCommand{
			Scope: testScope, ID: "cmd_pause", Type: agentkit.CommandPause, RunID: "run_1",
		}); err != nil {
			t.Fatalf("Record pause: %v", err)
		}
		done := make(chan error, 1)
		go func() {
			waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			_, err := c.Wait(waitCtx, agentkit.PauseWait{
				Scope: testScope, Identity: identity, PauseEpoch: 1,
				Checkpoint: agentkit.CheckpointAfterInference,
			})
			done <- err
		}()
		// Give the waiter a chance to register before the resume lands.
		time.Sleep(20 * time.Millisecond)
		if _, err := c.Record(ctx, agentkit.ControlCommand{
			Scope: testScope, ID: "cmd_resume", Type: agentkit.CommandResume,
			RunID: "run_1", PauseEpoch: 1,
		}); err != nil {
			t.Fatalf("Record resume: %v", err)
		}
		if err := <-done; err != nil {
			t.Fatalf("Wait: %v", err)
		}
	})

	t.Run("cancel wakes a wait and is terminal", func(t *testing.T) {
		c := newStore()
		if _, err := c.Record(ctx, agentkit.ControlCommand{
			Scope: testScope, ID: "cmd_pause", Type: agentkit.CommandPause, RunID: "run_1",
		}); err != nil {
			t.Fatalf("Record pause: %v", err)
		}
		done := make(chan error, 1)
		go func() {
			waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			_, err := c.Wait(waitCtx, agentkit.PauseWait{
				Scope: testScope, Identity: identity, PauseEpoch: 1,
				Checkpoint: agentkit.CheckpointAfterInference,
			})
			done <- err
		}()
		time.Sleep(20 * time.Millisecond)
		if _, err := c.Record(ctx, agentkit.ControlCommand{
			Scope: testScope, ID: "cmd_cancel", Type: agentkit.CommandCancel, RunID: "run_1",
		}); err != nil {
			t.Fatalf("Record cancel: %v", err)
		}
		if err := <-done; !errors.Is(err, agentkit.ErrRunCancelled) {
			t.Fatalf("wait error = %v; cancel must be terminal", err)
		}
	})
}

// VerifyApprovalStore checks idempotent issue, decision settlement,
// one-time consumption, and refusal to consume an unapproved request.
func VerifyApprovalStore(t *testing.T, newStore ApprovalStoreFactory, decide func(store agentkit.ApprovalStore, scope agentkit.SessionScope, requestID string, status agentkit.ApprovalStatus)) {
	t.Helper()
	ctx := context.Background()

	req := agentkit.ApprovalRequest{
		Scope:     testScope,
		Identity:  agentkit.StreamIdentity{ThreadID: "thread_1", RunID: "run_1"},
		RequestID: "approval_1",
		ToolName:  "write_file",
		Summary:   "Write to src/index.ts",
	}

	t.Run("issue is idempotent", func(t *testing.T) {
		s := newStore()
		first, err := s.Issue(ctx, req)
		if err != nil {
			t.Fatalf("Issue: %v", err)
		}
		second, err := s.Issue(ctx, req)
		if err != nil {
			t.Fatalf("re-Issue: %v", err)
		}
		if first.RequestID != second.RequestID || second.Status != first.Status {
			t.Fatal("re-issuing one request ID must return the existing record, not ask again")
		}
	})

	t.Run("approval can be consumed exactly once", func(t *testing.T) {
		s := newStore()
		if _, err := s.Issue(ctx, req); err != nil {
			t.Fatalf("Issue: %v", err)
		}
		decide(s, testScope, req.RequestID, agentkit.ApprovalApproved)

		settled, err := s.Wait(ctx, agentkit.ApprovalWait{Scope: testScope, RequestID: req.RequestID})
		if err != nil {
			t.Fatalf("Wait: %v", err)
		}
		if !settled.Approved() {
			t.Fatalf("settled status %q; want approved", settled.Status)
		}

		consumed, err := s.Consume(ctx, testScope, req.RequestID)
		if err != nil {
			t.Fatalf("Consume: %v", err)
		}
		if !consumed.Consumed {
			t.Fatal("Consume must mark the record consumed")
		}
		if _, err := s.Consume(ctx, testScope, req.RequestID); !errors.Is(err, agentkit.ErrApprovalConsumed) {
			t.Fatalf("second Consume error = %v; an approved capability is one-time", err)
		}
	})

	t.Run("a denied request cannot be consumed", func(t *testing.T) {
		s := newStore()
		if _, err := s.Issue(ctx, req); err != nil {
			t.Fatalf("Issue: %v", err)
		}
		decide(s, testScope, req.RequestID, agentkit.ApprovalDenied)
		if _, err := s.Consume(ctx, testScope, req.RequestID); err == nil {
			t.Fatal("consuming a denied approval must fail")
		}
	})

	t.Run("isolates identical request IDs by opaque scope", func(t *testing.T) {
		s := newStore()
		other := req
		other.Scope = "owner-scope-2"
		if _, err := s.Issue(ctx, req); err != nil {
			t.Fatalf("Issue first scope: %v", err)
		}
		if _, err := s.Issue(ctx, other); err != nil {
			t.Fatalf("Issue second scope: %v", err)
		}
		decide(s, testScope, req.RequestID, agentkit.ApprovalApproved)
		first, err := s.Wait(ctx, agentkit.ApprovalWait{Scope: testScope, RequestID: req.RequestID})
		if err != nil || !first.Approved() {
			t.Fatalf("first scope decision = %+v, %v", first, err)
		}
		otherCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		defer cancel()
		if settled, err := s.Wait(otherCtx, agentkit.ApprovalWait{Scope: other.Scope, RequestID: other.RequestID}); err == nil || settled.Approved() {
			t.Fatalf("decision crossed scopes: %+v, %v", settled, err)
		}
	})

	t.Run("one decision wakes every concurrent waiter", func(t *testing.T) {
		s := newStore()
		if _, err := s.Issue(ctx, req); err != nil {
			t.Fatalf("Issue: %v", err)
		}
		results := make(chan agentkit.ApprovalRecord, 2)
		errorsSeen := make(chan error, 2)
		for i := 0; i < 2; i++ {
			go func() {
				record, err := s.Wait(ctx, agentkit.ApprovalWait{Scope: testScope, RequestID: req.RequestID})
				results <- record
				errorsSeen <- err
			}()
		}
		decide(s, testScope, req.RequestID, agentkit.ApprovalApproved)
		for i := 0; i < 2; i++ {
			select {
			case err := <-errorsSeen:
				if err != nil {
					t.Fatalf("Wait: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("a concurrent approval waiter remained parked")
			}
			if record := <-results; !record.Approved() {
				t.Fatalf("settled status = %q, want approved", record.Status)
			}
		}
	})

	t.Run("expiration wakes every concurrent waiter", func(t *testing.T) {
		s := newStore()
		expiring := req
		expiring.ExpiresAt = time.Now().Add(25 * time.Millisecond)
		if _, err := s.Issue(ctx, expiring); err != nil {
			t.Fatalf("Issue: %v", err)
		}

		errorsSeen := make(chan error, 2)
		go func() {
			_, err := s.Wait(ctx, agentkit.ApprovalWait{
				Scope: testScope, RequestID: req.RequestID, Deadline: expiring.ExpiresAt,
			})
			errorsSeen <- err
		}()
		go func() {
			_, err := s.Wait(ctx, agentkit.ApprovalWait{Scope: testScope, RequestID: req.RequestID})
			errorsSeen <- err
		}()

		for i := 0; i < 2; i++ {
			select {
			case err := <-errorsSeen:
				if !errors.Is(err, agentkit.ErrApprovalExpired) {
					t.Fatalf("Wait error = %v, want ErrApprovalExpired", err)
				}
			case <-time.After(time.Second):
				t.Fatal("a concurrent approval waiter remained parked after expiration")
			}
		}
	})
}
