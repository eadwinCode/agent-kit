package agentkit

// EventJournal is the durable replay contract behind live streaming.
//
// Live delivery is best-effort: a WebSocket can drop, coalesce or arrive
// out of order, and the initiating browser may not exist any more. The
// journal is what makes recovery possible — AgentKit appends every
// standard envelope to it BEFORE handing the envelope to the live sink, so
// a client that reconnects can replay the exact ordered tail it missed and
// converge on the same transcript.
//
// The journal is a short-lived transport journal, not conversation
// history. Canonical content lives in HistoryConfig's storage; the journal
// only has to outlive the run plus a recovery window.

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
)

// JournalRecord is one standard event, storage-neutral. It is the exact
// envelope that reaches clients plus the identity fields a replay store
// indexes on.
type JournalRecord struct {
	// Scope is the opaque conversation owner. It is required on the stored
	// record so an adapter can enforce the same isolation on Append, Read and
	// Compact without learning the application's ownership model.
	Scope SessionScope `json:"scope"`
	// SchemaVersion is ContractSchemaVersion at the time of production.
	SchemaVersion int `json:"schemaVersion"`
	// EventID is stable for the same logical event across replays and
	// retries; it is the client's dedupe key.
	EventID string `json:"eventId"`
	// Event is the event name, e.g. "text.delta".
	Event string `json:"event"`
	// ThreadID, RunID and StreamEpoch scope the record.
	ThreadID    string `json:"threadId"`
	RunID       string `json:"runId"`
	StreamEpoch int    `json:"streamEpoch"`
	// SequenceNumber is gapless and monotonic within one epoch.
	SequenceNumber int `json:"sequenceNumber"`
	// Timestamp is Unix epoch milliseconds.
	Timestamp int64 `json:"timestamp"`
	// Data is the opaque event payload. AgentKit never reinterprets a
	// record's data on read; adapters must round-trip it byte-for-byte.
	Data json.RawMessage `json:"data"`
}

// Identity returns the record's stream identity.
func (r JournalRecord) Identity() StreamIdentity {
	return StreamIdentity{ThreadID: r.ThreadID, RunID: r.RunID, StreamEpoch: r.StreamEpoch}
}

// JournalCursor addresses a position in one run's ordered tail. The zero
// SequenceNumber is a valid position, so "start from the beginning" is
// expressed by SequenceNumber -1 (see JournalStart).
type JournalCursor struct {
	RunID          string `json:"runId"`
	StreamEpoch    int    `json:"streamEpoch"`
	SequenceNumber int    `json:"sequenceNumber"`
}

// JournalStart is the cursor meaning "everything from the first event".
const JournalStart = -1

// JournalQuery reads an ordered page strictly after After.
type JournalQuery struct {
	// Session scopes the read; the adapter reauthorizes it.
	Scope SessionScope `json:"scope"`
	// ThreadID narrows the read to one conversation.
	ThreadID string `json:"threadId"`
	// After is exclusive. Use SequenceNumber == JournalStart to read from
	// the first event of the epoch.
	After JournalCursor `json:"after"`
	// Limit bounds the page; 0 means the adapter's default.
	Limit int `json:"limit"`
}

// JournalPage is one ordered page of records.
type JournalPage struct {
	// Records are ordered by (StreamEpoch, SequenceNumber, EventID).
	Records []JournalRecord `json:"records"`
	// Next is the cursor to pass as After for the following page. It
	// equals the query's After when Records is empty.
	Next JournalCursor `json:"next"`
	// HasMore reports that more records exist after Next right now.
	HasMore bool `json:"hasMore"`
	// RetentionGap reports that records at or after the requested cursor
	// were dropped by retention and can never be served. The client must
	// restart from a fresh snapshot instead of waiting for a backfill.
	RetentionGap bool `json:"retentionGap"`
}

// EventJournal persists the ordered standard-event tail of a run.
//
// Implementations MUST be idempotent: appending a record whose EventID (or
// whose (RunID, StreamEpoch, SequenceNumber) triple) already exists is a
// no-op, not a duplicate row and not an error. Inngest replays and HTTP
// retries make this the normal case, not an edge case.
//
// Append is invoked on the run's execution path, so it must be fast and
// must not start a durable step of its own.
type EventJournal interface {
	// Append durably records envelopes in order, before live fan-out.
	Append(ctx context.Context, records []JournalRecord) error

	// Read returns the ordered page strictly after q.After. Returning a
	// page with RetentionGap set is preferred over returning
	// ErrRetentionGap, because the page can still carry whatever survives.
	Read(ctx context.Context, q JournalQuery) (JournalPage, error)
}

// JournalCompactor is the optional half of the journal contract: dropping
// deltas once canonical history has proven equivalent. AgentKit calls it
// only after a Finalizer authorized the terminal, never mid-run.
type JournalCompactor interface {
	// Compact deletes records up to and including the cursor. It must be
	// safe to call repeatedly and must never remove records of a run that
	// is still non-terminal.
	Compact(ctx context.Context, scope SessionScope, threadID string, upTo JournalCursor) error
}

// journalWriter appends to the journal on the streaming path and records
// whether durability was ever lost, which surfaces to clients as
// reconcileRequired.
type journalWriter struct {
	journal EventJournal
	// reconcileRequired latches once any append fails.
	reconcileRequired atomic.Bool
	// failures counts failed appends, for observability.
	failures atomic.Int64
}

// append records one envelope. A journal failure never breaks execution:
// it latches reconcileRequired and returns the typed error for logging.
// This is the documented degrade policy — AgentKit must not pretend the
// tail is complete, and must not abort a run because a replay store blipped.
func (w *journalWriter) append(ctx context.Context, rec JournalRecord) error {
	if w == nil || w.journal == nil {
		return nil
	}
	if err := w.journal.Append(ctx, []JournalRecord{rec}); err != nil {
		w.reconcileRequired.Store(true)
		w.failures.Add(1)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return NewPortError("EventJournal", "Append", "JOURNAL_APPEND_FAILED", true, err)
	}
	return nil
}

// enabled reports whether a journal is wired.
func (w *journalWriter) enabled() bool {
	return w != nil && w.journal != nil
}

// ReconcileRequired reports whether any journal append failed, meaning the
// durable tail has holes and clients must reconcile from canonical history
// rather than trusting backfill.
func (w *journalWriter) ReconcileRequired() bool {
	return w != nil && w.reconcileRequired.Load()
}

// ReadJournalTail drains the journal from after into an ordered slice,
// following pages until the store reports no more. It is the reference
// implementation of the replay read loop that a transport adapter exposes
// to clients; a retention gap stops the drain and is reported through the
// returned page flag.
func ReadJournalTail(ctx context.Context, j EventJournal, q JournalQuery) ([]JournalRecord, JournalPage, error) {
	var all []JournalRecord
	page := JournalPage{Next: q.After}
	if j == nil {
		return nil, page, nil
	}
	cursor := q.After
	for {
		if err := contextErr(ctx, "EventJournal", "Read"); err != nil {
			return all, page, err
		}
		next := q
		next.After = cursor
		p, err := j.Read(ctx, next)
		if err != nil {
			return all, page, err
		}
		all = append(all, p.Records...)
		page = p
		if p.RetentionGap || !p.HasMore || len(p.Records) == 0 {
			return all, page, nil
		}
		if p.Next == cursor {
			// Adapter did not advance: stop rather than spin.
			return all, page, nil
		}
		cursor = p.Next
	}
}
