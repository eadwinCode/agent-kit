// Package memadapter provides in-memory reference implementations of every
// AgentKit runtime port.
//
// They exist for three reasons: to prove the contracts are implementable
// without a database, to give tests and examples a working runtime without
// standing up Postgres and a workflow engine, and to serve as the reference
// an application adapter is checked against. They are NOT production
// storage — nothing here survives a process restart, which is precisely
// what the durable contracts exist to provide.
//
// Every type is safe for concurrent use.
package memadapter

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	agentkit "github.com/eadwinCode/agent-kit/go"
)

// Journal is an in-memory EventJournal.
//
// It enforces the two properties the contract requires of every
// implementation: appends are idempotent on EventID, and reads return an
// ordered page strictly after the cursor.
type Journal struct {
	mu sync.Mutex
	// records is the append-ordered log, keyed by thread.
	records map[string][]agentkit.JournalRecord
	// seen indexes EventIDs for idempotent appends.
	seen map[string]bool
	// compactedTo records the highest sequence dropped per (thread, run,
	// epoch), so a read from before it reports a retention gap.
	compactedTo map[string]int
	// FailAppend, when set, makes Append fail. Tests use it to prove the
	// degrade policy: a journal outage marks reconcile-required instead of
	// breaking the run.
	FailAppend error
	// PageLimit caps a page when the query does not (0 = unlimited).
	PageLimit int
}

// NewJournal creates an empty journal.
func NewJournal() *Journal {
	return &Journal{
		records:     map[string][]agentkit.JournalRecord{},
		seen:        map[string]bool{},
		compactedTo: map[string]int{},
	}
}

func journalKey(scope agentkit.SessionScope, threadID string) string {
	return string(scope) + "\x00" + threadID
}

func epochKey(scope agentkit.SessionScope, threadID, runID string, epoch int) string {
	return journalKey(scope, threadID) + "\x00" + runID + "\x00" + itoa(epoch)
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

// Append implements agentkit.EventJournal.
func (j *Journal) Append(ctx context.Context, records []agentkit.JournalRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.FailAppend != nil {
		return j.FailAppend
	}
	for _, rec := range records {
		seenKey := string(rec.Scope) + "\x00" + rec.EventID
		if rec.EventID != "" && j.seen[seenKey] {
			// Idempotent: an Inngest replay or an HTTP retry re-appends the
			// same logical event, which must not duplicate the tail.
			continue
		}
		if rec.EventID != "" {
			j.seen[seenKey] = true
		}
		key := journalKey(rec.Scope, rec.ThreadID)
		j.records[key] = append(j.records[key], rec)
	}
	return nil
}

// Read implements agentkit.EventJournal.
func (j *Journal) Read(ctx context.Context, q agentkit.JournalQuery) (agentkit.JournalPage, error) {
	if err := ctx.Err(); err != nil {
		return agentkit.JournalPage{}, err
	}
	j.mu.Lock()
	defer j.mu.Unlock()

	limit := q.Limit
	if limit <= 0 {
		limit = j.PageLimit
	}

	all := append([]agentkit.JournalRecord(nil), j.records[journalKey(q.Scope, q.ThreadID)]...)
	sort.SliceStable(all, func(a, b int) bool {
		if all[a].StreamEpoch != all[b].StreamEpoch {
			return all[a].StreamEpoch < all[b].StreamEpoch
		}
		if all[a].SequenceNumber != all[b].SequenceNumber {
			return all[a].SequenceNumber < all[b].SequenceNumber
		}
		return all[a].EventID < all[b].EventID
	})

	page := agentkit.JournalPage{Next: q.After}
	if dropped, ok := j.compactedTo[epochKey(q.Scope, q.ThreadID, q.After.RunID, q.After.StreamEpoch)]; ok &&
		q.After.SequenceNumber < dropped {
		// The caller is asking for records retention already removed.
		page.RetentionGap = true
		return page, nil
	}

	for _, rec := range all {
		if q.After.RunID != "" && rec.RunID != q.After.RunID {
			continue
		}
		if rec.StreamEpoch < q.After.StreamEpoch {
			continue
		}
		if rec.StreamEpoch == q.After.StreamEpoch && rec.SequenceNumber <= q.After.SequenceNumber {
			continue
		}
		if limit > 0 && len(page.Records) == limit {
			page.HasMore = true
			break
		}
		page.Records = append(page.Records, rec)
		page.Next = agentkit.JournalCursor{
			RunID: rec.RunID, StreamEpoch: rec.StreamEpoch, SequenceNumber: rec.SequenceNumber,
		}
	}
	return page, nil
}

// Compact implements agentkit.JournalCompactor.
func (j *Journal) Compact(ctx context.Context, scope agentkit.SessionScope, threadID string, upTo agentkit.JournalCursor) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	key := journalKey(scope, threadID)
	kept := j.records[key][:0]
	for _, rec := range j.records[key] {
		if rec.RunID == upTo.RunID && rec.StreamEpoch == upTo.StreamEpoch &&
			rec.SequenceNumber <= upTo.SequenceNumber {
			continue
		}
		kept = append(kept, rec)
	}
	j.records[key] = kept
	ek := epochKey(scope, threadID, upTo.RunID, upTo.StreamEpoch)
	if upTo.SequenceNumber > j.compactedTo[ek] {
		j.compactedTo[ek] = upTo.SequenceNumber
	}
	return nil
}

// Len reports how many records the journal holds for a thread.
func (j *Journal) Len(threadID string) int {
	j.mu.Lock()
	defer j.mu.Unlock()
	total := 0
	for _, records := range j.records {
		for _, rec := range records {
			if rec.ThreadID == threadID {
				total++
			}
		}
	}
	return total
}

// LenFor reports how many records the journal holds for one opaque scope and
// thread. Len is retained as a test convenience across all scopes.
func (j *Journal) LenFor(scope agentkit.SessionScope, threadID string) int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return len(j.records[journalKey(scope, threadID)])
}

// StateStore is an in-memory agentkit.StateStore with real CAS semantics.
type StateStore struct {
	mu     sync.Mutex
	states map[agentkit.SessionScope]agentkit.SessionState
}

// NewStateStore creates an empty store.
func NewStateStore() *StateStore {
	return &StateStore{states: map[agentkit.SessionScope]agentkit.SessionState{}}
}

// Load implements agentkit.StateStore.
func (s *StateStore) Load(ctx context.Context, scope agentkit.SessionScope) (agentkit.SessionState, error) {
	if err := ctx.Err(); err != nil {
		return agentkit.SessionState{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadLocked(scope), nil
}

func (s *StateStore) loadLocked(scope agentkit.SessionScope) agentkit.SessionState {
	state, ok := s.states[scope]
	if !ok {
		state = agentkit.SessionState{
			SchemaVersion:      agentkit.ContractSchemaVersion,
			Scope:              scope,
			Pause:              agentkit.PauseInfo{State: agentkit.PauseNone},
			Activity:           agentkit.Activity{Kind: agentkit.ActivityNone},
			Approval:           agentkit.ApprovalInfo{Status: agentkit.ApprovalNone},
			LastSequenceNumber: agentkit.JournalStart,
			// Revisions start at 1 so revision 0 can mean "no CAS
			// precondition" without colliding with a real stored value.
			Revision: agentkit.InitialStateRevision,
		}
	}
	if state.ActiveRun != nil {
		run := *state.ActiveRun
		state.ActiveRun = &run
	}
	return state
}

// CompareAndSwap implements agentkit.StateStore.
func (s *StateStore) CompareAndSwap(ctx context.Context, t agentkit.StateTransition) (agentkit.SessionState, error) {
	if err := ctx.Err(); err != nil {
		return agentkit.SessionState{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	current := s.loadLocked(t.Scope)
	if t.ExpectedRevision != 0 && t.ExpectedRevision != current.Revision {
		// Lost race: return the authoritative state so the caller can
		// reconcile without a second read.
		return current, agentkit.NewPortError("StateStore", "CompareAndSwap",
			"STATE_REVISION_MISMATCH", true, agentkit.ErrRevisionMismatch)
	}

	next := current
	if t.Apply != nil {
		t.Apply(&next)
	}
	next.Scope = t.Scope
	next.SchemaVersion = agentkit.ContractSchemaVersion
	next.Revision = current.Revision + 1
	next.UpdatedAt = time.Now().UTC()
	s.states[t.Scope] = next
	return next, nil
}

// Set installs a state directly, for test setup.
func (s *StateStore) Set(state agentkit.SessionState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[state.Scope] = state
}

// ControlStore is an in-memory agentkit.ControlStore.
//
// Two behaviors here are contract behaviors, not conveniences. Its Wait
// models the documented race — the durable command record is authoritative,
// so a resume recorded BEFORE the wait began still wakes it. And a command
// recorded with no RunID applies to whichever run the scope is currently
// executing, which is how a client that has only just read a snapshot can
// pause a run whose id it does not know yet.
type ControlStore struct {
	mu       sync.Mutex
	commands map[string]agentkit.CommandResult
	hashes   map[string]string
	// pending is the outstanding intent, keyed by run id ("" = any run).
	pending map[string]agentkit.ControlSignal
	// resumed records resume/cancel signals per (run, pauseEpoch).
	resumed map[string]agentkit.ControlSignal
	// waiters maps a correlation key to every channel parked on it.
	waiters map[string][]chan agentkit.ControlSignal
}

// NewControlStore creates an empty control store.
func NewControlStore() *ControlStore {
	return &ControlStore{
		commands: map[string]agentkit.CommandResult{},
		hashes:   map[string]string{},
		pending:  map[string]agentkit.ControlSignal{},
		resumed:  map[string]agentkit.ControlSignal{},
		waiters:  map[string][]chan agentkit.ControlSignal{},
	}
}

func pauseKey(runID string, epoch int) string { return runID + "\x00" + itoa(epoch) }

// correlationKeys are the keys a wait for (runID, epoch) answers to: its own
// run and the any-run wildcard.
func correlationKeys(runID string, epoch int) []string {
	keys := []string{pauseKey(runID, epoch)}
	if runID != "" {
		keys = append(keys, pauseKey("", epoch))
	}
	return keys
}

// Record implements agentkit.ControlStore.
func (c *ControlStore) Record(ctx context.Context, cmd agentkit.ControlCommand) (agentkit.CommandResult, error) {
	if err := ctx.Err(); err != nil {
		return agentkit.CommandResult{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if prev, ok := c.commands[cmd.ID]; ok {
		if c.hashes[cmd.ID] != cmd.PayloadHash {
			return prev, agentkit.NewPortError("ControlStore", "Record",
				"IDEMPOTENCY_KEY_REUSED", false, agentkit.ErrIdempotencyKeyReuse)
		}
		prev.Duplicate = true
		return prev, nil
	}

	now := time.Now().UTC()
	result := agentkit.CommandResult{
		CommandID: cmd.ID, Status: agentkit.CommandAccepted, AppliedAt: &now,
	}
	c.commands[cmd.ID] = result
	c.hashes[cmd.ID] = cmd.PayloadHash

	switch cmd.Type {
	case agentkit.CommandPause:
		c.pending[cmd.RunID] = agentkit.ControlSignal{
			Action: agentkit.ControlPause, CommandID: cmd.ID,
		}
	case agentkit.CommandCancel:
		signal := agentkit.ControlSignal{Action: agentkit.ControlCancel, CommandID: cmd.ID}
		c.pending[cmd.RunID] = signal
		// Cancel is terminal: it wakes every outstanding pause and beats a
		// pending resume.
		for key, chans := range c.waiters {
			c.resumed[key] = signal
			for _, ch := range chans {
				deliver(ch, signal)
			}
			delete(c.waiters, key)
		}
	case agentkit.CommandResume:
		signal := agentkit.ControlSignal{Action: agentkit.ControlContinue, CommandID: cmd.ID}
		key := pauseKey(cmd.RunID, cmd.PauseEpoch)
		// Durable-first: record the resume whether or not a waiter exists
		// yet, so a wake sent before the wait registered is not lost.
		c.resumed[key] = signal
		delete(c.pending, cmd.RunID)
		if chans, ok := c.waiters[key]; ok {
			for _, ch := range chans {
				deliver(ch, signal)
			}
			delete(c.waiters, key)
		}
	}
	return result, nil
}

func deliver(ch chan agentkit.ControlSignal, signal agentkit.ControlSignal) {
	select {
	case ch <- signal:
	default:
	}
}

// Poll implements agentkit.ControlStore.
func (c *ControlStore) Poll(ctx context.Context, cp agentkit.Checkpoint) (agentkit.ControlSignal, error) {
	if err := ctx.Err(); err != nil {
		return agentkit.ControlSignal{Action: agentkit.ControlContinue}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if signal, ok := c.pending[cp.Identity.RunID]; ok {
		return signal, nil
	}
	if signal, ok := c.pending[""]; ok {
		return signal, nil
	}
	return agentkit.ControlSignal{Action: agentkit.ControlContinue}, nil
}

// Wait implements agentkit.ControlStore.
func (c *ControlStore) Wait(ctx context.Context, w agentkit.PauseWait) (agentkit.ControlSignal, error) {
	keys := correlationKeys(w.Identity.RunID, w.PauseEpoch)

	c.mu.Lock()
	// Check the durable record BEFORE parking: a resume event that arrived
	// before this wait registered must still wake it.
	for _, key := range keys {
		if signal, ok := c.resumed[key]; ok {
			delete(c.resumed, key)
			delete(c.pending, w.Identity.RunID)
			delete(c.pending, "")
			c.mu.Unlock()
			return c.settle(signal)
		}
	}
	ch := make(chan agentkit.ControlSignal, 1)
	for _, key := range keys {
		c.waiters[key] = append(c.waiters[key], ch)
	}
	c.mu.Unlock()

	var timeout <-chan time.Time
	if !w.Deadline.IsZero() {
		timer := time.NewTimer(time.Until(w.Deadline))
		defer timer.Stop()
		timeout = timer.C
	}

	select {
	case signal := <-ch:
		c.mu.Lock()
		c.detach(keys, ch)
		delete(c.pending, w.Identity.RunID)
		delete(c.pending, "")
		c.mu.Unlock()
		return c.settle(signal)
	case <-timeout:
		c.mu.Lock()
		c.detach(keys, ch)
		c.mu.Unlock()
		return agentkit.ControlSignal{Action: agentkit.ControlCancel, Reason: "pause_expired"},
			agentkit.NewPortError("ControlStore", "Wait", "PAUSE_EXPIRED", false, agentkit.ErrPauseExpired)
	case <-ctx.Done():
		c.mu.Lock()
		c.detach(keys, ch)
		c.mu.Unlock()
		return agentkit.ControlSignal{}, ctx.Err()
	}
}

// detach removes ch from every key it parked on. Callers hold the lock.
func (c *ControlStore) detach(keys []string, ch chan agentkit.ControlSignal) {
	for _, key := range keys {
		chans := c.waiters[key]
		kept := chans[:0]
		for _, existing := range chans {
			if existing != ch {
				kept = append(kept, existing)
			}
		}
		if len(kept) == 0 {
			delete(c.waiters, key)
			continue
		}
		c.waiters[key] = kept
	}
}

func (c *ControlStore) settle(signal agentkit.ControlSignal) (agentkit.ControlSignal, error) {
	if signal.Action == agentkit.ControlCancel {
		return signal, agentkit.NewPortError("ControlStore", "Wait", "RUN_CANCELLED", false, agentkit.ErrRunCancelled)
	}
	return signal, nil
}

// ApprovalStore is an in-memory agentkit.ApprovalStore with one-time
// consumption.
type ApprovalStore struct {
	mu      sync.Mutex
	records map[approvalKey]agentkit.ApprovalRecord
	waiters map[approvalKey][]chan agentkit.ApprovalRecord
	// AutoDecide, when set, settles every request immediately with this
	// status. Tests that are not about the waiting itself use it.
	AutoDecide agentkit.ApprovalStatus
}

type approvalKey struct {
	scope     agentkit.SessionScope
	requestID string
}

// NewApprovalStore creates an empty approval store.
func NewApprovalStore() *ApprovalStore {
	return &ApprovalStore{
		records: map[approvalKey]agentkit.ApprovalRecord{},
		waiters: map[approvalKey][]chan agentkit.ApprovalRecord{},
	}
}

// Issue implements agentkit.ApprovalStore.
func (a *ApprovalStore) Issue(ctx context.Context, req agentkit.ApprovalRequest) (agentkit.ApprovalRecord, error) {
	if err := ctx.Err(); err != nil {
		return agentkit.ApprovalRecord{}, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	key := approvalKey{scope: req.Scope, requestID: req.RequestID}
	if rec, ok := a.records[key]; ok {
		// Idempotent: a replay must not ask the user a second time.
		return rec, nil
	}
	rec := agentkit.ApprovalRecord{RequestID: req.RequestID, Status: agentkit.ApprovalPending}
	if !req.ExpiresAt.IsZero() {
		expires := req.ExpiresAt
		rec.ExpiresAt = &expires
	}
	if a.AutoDecide != "" {
		rec.Status = a.AutoDecide
		now := time.Now().UTC()
		rec.DecidedAt = &now
	}
	a.records[key] = rec
	return rec, nil
}

// Wait implements agentkit.ApprovalStore.
func (a *ApprovalStore) Wait(ctx context.Context, w agentkit.ApprovalWait) (agentkit.ApprovalRecord, error) {
	a.mu.Lock()
	key := approvalKey{scope: w.Scope, requestID: w.RequestID}
	rec, ok := a.records[key]
	if !ok {
		a.mu.Unlock()
		return agentkit.ApprovalRecord{}, agentkit.NewPortError("ApprovalStore", "Wait",
			"APPROVAL_NOT_FOUND", false, errors.New("memadapter: unknown approval"))
	}
	if rec.Status != agentkit.ApprovalPending {
		a.mu.Unlock()
		return approvalWaitResult(rec)
	}
	ch := make(chan agentkit.ApprovalRecord, 1)
	a.waiters[key] = append(a.waiters[key], ch)
	a.mu.Unlock()

	var timeout <-chan time.Time
	if !w.Deadline.IsZero() {
		timer := time.NewTimer(time.Until(w.Deadline))
		defer timer.Stop()
		timeout = timer.C
	}

	select {
	case settled := <-ch:
		return approvalWaitResult(settled)
	case <-timeout:
		a.mu.Lock()
		rec = a.records[key]
		if rec.Status == agentkit.ApprovalPending {
			// Expiration settles the request for every observer, just like an
			// explicit decision. Leaving the other channels registered would park
			// a waiter without its own deadline forever.
			a.decideLocked(key, agentkit.ApprovalExpired, "approval deadline elapsed")
			rec = a.records[key]
		} else {
			a.detachApprovalWaiter(key, ch)
		}
		a.mu.Unlock()
		return approvalWaitResult(rec)
	case <-ctx.Done():
		a.mu.Lock()
		a.detachApprovalWaiter(key, ch)
		a.mu.Unlock()
		return agentkit.ApprovalRecord{}, ctx.Err()
	}
}

func approvalWaitResult(rec agentkit.ApprovalRecord) (agentkit.ApprovalRecord, error) {
	if rec.Status == agentkit.ApprovalExpired {
		return rec, agentkit.NewPortError("ApprovalStore", "Wait", "APPROVAL_EXPIRED", false, agentkit.ErrApprovalExpired)
	}
	return rec, nil
}

// Consume implements agentkit.ApprovalStore.
func (a *ApprovalStore) Consume(ctx context.Context, scope agentkit.SessionScope, requestID string) (agentkit.ApprovalRecord, error) {
	if err := ctx.Err(); err != nil {
		return agentkit.ApprovalRecord{}, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	key := approvalKey{scope: scope, requestID: requestID}
	rec, ok := a.records[key]
	if !ok {
		return agentkit.ApprovalRecord{}, agentkit.NewPortError("ApprovalStore", "Consume",
			"APPROVAL_NOT_FOUND", false, errors.New("memadapter: unknown approval"))
	}
	if rec.Consumed {
		return rec, agentkit.NewPortError("ApprovalStore", "Consume",
			"APPROVAL_ALREADY_CONSUMED", false, agentkit.ErrApprovalConsumed)
	}
	if rec.Status != agentkit.ApprovalApproved {
		return rec, agentkit.NewPortError("ApprovalStore", "Consume",
			"APPROVAL_NOT_APPROVED", false, agentkit.ErrApprovalDenied)
	}
	rec.Consumed = true
	a.records[key] = rec
	return rec, nil
}

// Decide settles a request ID only when it is unambiguous across scopes. It is
// a convenience for single-scope tests; applications should use DecideFor.
func (a *ApprovalStore) Decide(requestID string, status agentkit.ApprovalStatus, reason string) {
	a.mu.Lock()
	var match approvalKey
	found := false
	for key := range a.records {
		if key.requestID != requestID {
			continue
		}
		if found {
			a.mu.Unlock()
			return
		}
		match, found = key, true
	}
	if !found {
		a.mu.Unlock()
		return
	}
	a.decideLocked(match, status, reason)
	a.mu.Unlock()
}

// DecideFor settles one pending request in one opaque owner scope.
func (a *ApprovalStore) DecideFor(scope agentkit.SessionScope, requestID string, status agentkit.ApprovalStatus, reason string) {
	a.mu.Lock()
	key := approvalKey{scope: scope, requestID: requestID}
	a.decideLocked(key, status, reason)
	a.mu.Unlock()
}

func (a *ApprovalStore) decideLocked(key approvalKey, status agentkit.ApprovalStatus, reason string) {
	rec, ok := a.records[key]
	if !ok || rec.Status != agentkit.ApprovalPending {
		return
	}
	rec.Status = status
	rec.Reason = reason
	now := time.Now().UTC()
	rec.DecidedAt = &now
	a.records[key] = rec
	waiters := a.waiters[key]
	delete(a.waiters, key)
	for _, ch := range waiters {
		ch <- rec
		close(ch)
	}
}

// Record returns the stored record for a request.
func (a *ApprovalStore) Record(requestID string) (agentkit.ApprovalRecord, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var found agentkit.ApprovalRecord
	matched := false
	for key, rec := range a.records {
		if key.requestID != requestID {
			continue
		}
		if matched {
			return agentkit.ApprovalRecord{}, false
		}
		found, matched = rec, true
	}
	return found, matched
}

// RecordFor returns one stored record in one opaque owner scope.
func (a *ApprovalStore) RecordFor(scope agentkit.SessionScope, requestID string) (agentkit.ApprovalRecord, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	rec, ok := a.records[approvalKey{scope: scope, requestID: requestID}]
	return rec, ok
}

// detachApprovalWaiter removes one waiter. Callers hold a.mu.
func (a *ApprovalStore) detachApprovalWaiter(key approvalKey, target chan agentkit.ApprovalRecord) {
	waiters := a.waiters[key]
	kept := waiters[:0]
	for _, waiter := range waiters {
		if waiter != target {
			kept = append(kept, waiter)
		}
	}
	if len(kept) == 0 {
		delete(a.waiters, key)
		return
	}
	a.waiters[key] = kept
}

// Sink is an in-memory agentkit.StreamSink that records everything it was
// handed, in order.
type Sink struct {
	mu     sync.Mutex
	chunks []agentkit.AgentMessageChunk
	// Fail, when set, makes Deliver fail. Delivery is best-effort by
	// contract, so a failing sink must not break a run.
	Fail error
}

// NewSink creates an empty sink.
func NewSink() *Sink { return &Sink{} }

// Deliver implements agentkit.StreamSink.
func (s *Sink) Deliver(_ context.Context, chunk agentkit.AgentMessageChunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Fail != nil {
		return s.Fail
	}
	s.chunks = append(s.chunks, chunk)
	return nil
}

// Chunks returns a copy of everything delivered.
func (s *Sink) Chunks() []agentkit.AgentMessageChunk {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]agentkit.AgentMessageChunk(nil), s.chunks...)
}

// Events returns the delivered event names, in order.
func (s *Sink) Events() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.chunks))
	for i, c := range s.chunks {
		out[i] = c.Event
	}
	return out
}

// Finalizer is a recording agentkit.Finalizer.
type Finalizer struct {
	mu sync.Mutex
	// Requests records every Finalize call, proving exactly-once.
	Requests []agentkit.FinalizeRequest
	// Result overrides the returned result.
	Result agentkit.FinalizeResult
	// Err, when set, makes Finalize fail — the typed terminal failure path.
	Err error
	// OnFinalize runs inside Finalize, for tests that need to observe
	// ordering against the terminal event.
	OnFinalize func(agentkit.FinalizeRequest)
}

// NewFinalizer creates a recording finalizer.
func NewFinalizer() *Finalizer { return &Finalizer{} }

// Finalize implements agentkit.Finalizer.
func (f *Finalizer) Finalize(_ context.Context, req agentkit.FinalizeRequest) (agentkit.FinalizeResult, error) {
	f.mu.Lock()
	f.Requests = append(f.Requests, req)
	hook := f.OnFinalize
	result, err := f.Result, f.Err
	f.mu.Unlock()
	if hook != nil {
		hook(req)
	}
	return result, err
}

// Calls reports how many times Finalize ran.
func (f *Finalizer) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.Requests)
}

// Ports builds a RuntimePorts wired to a fresh set of in-memory adapters.
type Ports struct {
	Journal   *Journal
	State     *StateStore
	Control   *ControlStore
	Approvals *ApprovalStore
	Sink      *Sink
	Finalizer *Finalizer
}

// NewPorts creates every adapter and returns the handles plus the
// RuntimePorts value to pass into a run.
func NewPorts(scope agentkit.SessionScope, epoch int) (*Ports, *agentkit.RuntimePorts) {
	p := &Ports{
		Journal:   NewJournal(),
		State:     NewStateStore(),
		Control:   NewControlStore(),
		Approvals: NewApprovalStore(),
		Sink:      NewSink(),
		Finalizer: NewFinalizer(),
	}
	return p, &agentkit.RuntimePorts{
		Journal:     p.Journal,
		State:       p.State,
		Control:     p.Control,
		Approvals:   p.Approvals,
		Sink:        p.Sink,
		Finalizer:   p.Finalizer,
		Scope:       scope,
		StreamEpoch: epoch,
	}
}

// Compile-time proof that every adapter satisfies its contract.
var (
	_ agentkit.EventJournal     = (*Journal)(nil)
	_ agentkit.JournalCompactor = (*Journal)(nil)
	_ agentkit.StateStore       = (*StateStore)(nil)
	_ agentkit.ControlStore     = (*ControlStore)(nil)
	_ agentkit.ApprovalStore    = (*ApprovalStore)(nil)
	_ agentkit.StreamSink       = (*Sink)(nil)
	_ agentkit.Finalizer        = (*Finalizer)(nil)
)

// History is an in-memory agentkit.HistoryConfig implementation.
//
// It stores user turns and results per thread and dedupes results by their
// replay-stable checksum, which is what a real adapter's unique index does.
// Threads are upserted, never inserted blindly: AgentKit memoizes the create
// step, but an adapter that cannot be called twice safely is one durable
// retry away from a duplicate conversation.
type History[T any] struct {
	mu       sync.Mutex
	threads  map[string]bool
	messages map[string][]agentkit.UserMessageRecord
	results  map[string][]*agentkit.AgentResult
	seen     map[string]bool
	// NextThreadID mints thread ids; override for deterministic tests.
	NextThreadID func() string
	counter      int
}

// NewHistory creates an empty in-memory history adapter.
func NewHistory[T any]() *History[T] {
	return &History[T]{
		threads:  map[string]bool{},
		messages: map[string][]agentkit.UserMessageRecord{},
		results:  map[string][]*agentkit.AgentResult{},
		seen:     map[string]bool{},
	}
}

// Config returns the HistoryConfig to hand to an agent or network.
func (h *History[T]) Config() *agentkit.HistoryConfig[T] {
	return &agentkit.HistoryConfig[T]{
		CreateThread: func(_ context.Context, hctx agentkit.HistoryContext[T]) (agentkit.CreateThreadResult, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			id := hctx.ThreadID
			if id == "" {
				h.counter++
				if h.NextThreadID != nil {
					id = h.NextThreadID()
				} else {
					id = "thread_" + itoa(h.counter)
				}
			}
			// Upsert: an existing thread keeps its contents.
			h.threads[id] = true
			return agentkit.CreateThreadResult{ThreadID: id}, nil
		},
		Get: func(_ context.Context, hctx agentkit.HistoryContext[T]) ([]*agentkit.AgentResult, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			return append([]*agentkit.AgentResult(nil), h.results[hctx.ThreadID]...), nil
		},
		AppendUserMessage: func(_ context.Context, hctx agentkit.HistoryContext[T], msg agentkit.UserMessageRecord) error {
			h.mu.Lock()
			defer h.mu.Unlock()
			key := "user:" + hctx.ThreadID + ":" + msg.ID
			if h.seen[key] {
				return nil
			}
			h.seen[key] = true
			h.messages[hctx.ThreadID] = append(h.messages[hctx.ThreadID], msg)
			return nil
		},
		AppendResults: func(_ context.Context, hctx agentkit.HistoryContext[T], newResults []*agentkit.AgentResult) error {
			h.mu.Lock()
			defer h.mu.Unlock()
			for _, result := range newResults {
				// The checksum is replay-stable, so it is the natural unique
				// key — the same role a unique index plays in real storage.
				// A result that cannot be checksummed falls back to its id,
				// which the runtime also mints inside a durable step.
				checksum, err := result.Checksum()
				if err != nil {
					checksum = result.ID
				}
				key := "result:" + hctx.ThreadID + ":" + checksum
				if h.seen[key] {
					continue
				}
				h.seen[key] = true
				h.results[hctx.ThreadID] = append(h.results[hctx.ThreadID], result)
			}
			return nil
		},
	}
}

// UserMessages returns the user turns stored for a thread, in order.
func (h *History[T]) UserMessages(threadID string) []agentkit.UserMessageRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]agentkit.UserMessageRecord(nil), h.messages[threadID]...)
}

// Results returns the results stored for a thread, in order.
func (h *History[T]) Results(threadID string) []*agentkit.AgentResult {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*agentkit.AgentResult(nil), h.results[threadID]...)
}

// Threads returns the thread ids the adapter has created.
func (h *History[T]) Threads() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, 0, len(h.threads))
	for id := range h.threads {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}
