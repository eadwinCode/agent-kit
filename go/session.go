package agentkit

// StateStore is the durable session-state contract: the small, bounded
// record every authorized client hydrates from before it replays a tail.
//
// It deliberately does NOT hold the transcript. Messages live in
// HistoryConfig's storage and deltas live in the EventJournal; this record
// carries only what a client cannot derive — which thread is current, which
// run is active, whether it is paused, what it is doing, whether an approval
// is outstanding, and the revision that serializes concurrent commands.
//
// Lifecycle and activity are orthogonal on purpose. Lifecycle is coarse and
// machine-driven (accepted → executing → waiting → terminalizing); activity
// is the truthful, user-facing description of the current work. Neither may
// be inferred from the other, and neither may be inferred from elapsed time.

import (
	"context"
	"errors"
	"time"
)

// RunLifecycle is the coarse machine state of the session's active run.
type RunLifecycle string

const (
	// LifecycleIdle means no conversational run is active.
	LifecycleIdle RunLifecycle = "idle"
	// LifecycleAccepted means a command was durably accepted but the
	// workflow has not started executing yet.
	LifecycleAccepted RunLifecycle = "accepted"
	// LifecycleExecuting means the workflow is running inference or tools.
	LifecycleExecuting RunLifecycle = "executing"
	// LifecycleWaiting means the run is parked at a durable wait: an
	// approval, a pause, or another external correlation.
	LifecycleWaiting RunLifecycle = "waiting"
	// LifecycleTerminalizing means the outcome is decided and the
	// application is settling history, billing and cleanup before the one
	// terminal event.
	LifecycleTerminalizing RunLifecycle = "terminalizing"
)

// RunOutcome is the terminal classification of a finished run.
type RunOutcome string

const (
	// OutcomeNone means the run has not reached a terminal state.
	OutcomeNone RunOutcome = ""
	// OutcomeCompleted means the run finished normally.
	OutcomeCompleted RunOutcome = "completed"
	// OutcomeFailed means the run ended on an error.
	OutcomeFailed RunOutcome = "failed"
	// OutcomeCancelled means a cancel command ended the run.
	OutcomeCancelled RunOutcome = "cancelled"
)

// PauseState is the pause dimension. It is a control *intent* first: a
// requested pause is not a claim that the provider or tool has stopped.
type PauseState string

const (
	// PauseNone means no pause intent exists.
	PauseNone PauseState = "none"
	// PauseRequested means a pause command was accepted and execution will
	// stop at its next safe boundary.
	PauseRequested PauseState = "requested"
	// PausePaused means execution reached a safe boundary and checkpointed.
	PausePaused PauseState = "paused"
	// PauseResuming means a resume command was accepted and the workflow
	// has not yet consumed it.
	PauseResuming PauseState = "resuming"
)

// ActivityKind is the bounded, user-facing description of current work.
//
// ActivityThinking is special: it may only be set while the provider is
// actively streaming a reasoning part it returned. Elapsed time, a slow
// model, or a generic wait is never evidence of thinking — use
// ActivityPreparing or the truthful tool activity instead.
type ActivityKind string

const (
	ActivityNone            ActivityKind = "none"
	ActivityPreparing       ActivityKind = "preparing"
	ActivityThinking        ActivityKind = "thinking"
	ActivityResponding      ActivityKind = "responding"
	ActivityReading         ActivityKind = "reading"
	ActivityWriting         ActivityKind = "writing"
	ActivityTool            ActivityKind = "tool"
	ActivityWaitingExternal ActivityKind = "waiting_external"
	ActivityFinalizing      ActivityKind = "finalizing"
)

// ActivitySource records who observed the activity.
type ActivitySource string

const (
	ActivityFromProvider ActivitySource = "provider"
	ActivityFromTool     ActivitySource = "tool"
	ActivityFromServer   ActivitySource = "server"
)

// Activity is the semantic activity dimension.
type Activity struct {
	Kind ActivityKind `json:"kind"`
	// Label is a short user-safe string such as "Reading project files".
	// It must never contain prompt text, tool output or provider payloads.
	Label  string         `json:"label,omitempty"`
	Source ActivitySource `json:"source,omitempty"`
}

// ApprovalStatus is the human-in-the-loop dimension projected into state.
type ApprovalStatus string

const (
	ApprovalNone     ApprovalStatus = "none"
	ApprovalPending  ApprovalStatus = "pending"
	ApprovalSettling ApprovalStatus = "settling"
	ApprovalApproved ApprovalStatus = "approved"
	ApprovalDenied   ApprovalStatus = "denied"
	ApprovalExpired  ApprovalStatus = "expired"
)

// PauseInfo is the pause dimension of a snapshot.
type PauseInfo struct {
	State       PauseState `json:"state"`
	RequestedAt *time.Time `json:"requestedAt,omitempty"`
	PausedAt    *time.Time `json:"pausedAt,omitempty"`
	ExpiresAt   *time.Time `json:"expiresAt,omitempty"`
	// AccumulatedPausedMs supports an optional "active time" display; it
	// never replaces the wall-clock turn timer.
	AccumulatedPausedMs int64 `json:"accumulatedPausedMs"`
	// Epoch increments on every pause request so a stale resume for an
	// earlier pause cannot wake a later one.
	Epoch int `json:"epoch"`
}

// ApprovalInfo is the approval dimension of a snapshot.
type ApprovalInfo struct {
	Status     ApprovalStatus `json:"status"`
	ApprovalID string         `json:"approvalId,omitempty"`
	ExpiresAt  *time.Time     `json:"expiresAt,omitempty"`
}

// ActiveRun is the active-run dimension of a snapshot.
type ActiveRun struct {
	RunID      string       `json:"runId"`
	Lifecycle  RunLifecycle `json:"lifecycle"`
	Outcome    RunOutcome   `json:"outcome,omitempty"`
	AcceptedAt time.Time    `json:"acceptedAt"`
}

// SessionState is the durable, storage-neutral agent-session record.
//
// Invariants AgentKit relies on, and that adapters must preserve:
//
//  1. CurrentThreadID, when set, belongs to the same session scope.
//  2. At most one non-terminal ActiveRun exists per session.
//  3. Pause.State != PauseNone requires a non-terminal ActiveRun.
//  4. Pause.State == PausePaused requires CheckpointKind and Pause.PausedAt.
//  5. Revision increases exactly once per committed transition; a stale
//     compare-and-swap changes nothing.
//  6. LastSequenceNumber is monotonic within StreamEpoch and resets to -1
//     when the epoch changes.
//  7. Once an outcome is committed, no command resurrects the run.
type SessionState struct {
	SchemaVersion   int          `json:"schemaVersion"`
	Scope           SessionScope `json:"scope"`
	CurrentThreadID string       `json:"currentThreadId,omitempty"`
	ActiveRun       *ActiveRun   `json:"activeRun"`

	Pause    PauseInfo    `json:"pause"`
	Activity Activity     `json:"activity"`
	Approval ApprovalInfo `json:"approval"`

	// CheckpointKind is the last durable boundary category reached. It is
	// a bounded category name, never model or provider detail.
	CheckpointKind string `json:"checkpointKind,omitempty"`

	// StreamEpoch and LastSequenceNumber form the client's replay cursor.
	StreamEpoch        int `json:"streamEpoch"`
	LastSequenceNumber int `json:"lastSequenceNumber"`

	// Revision is the monotonic CAS token.
	Revision int64 `json:"revision"`

	// LastErrorCode is a bounded operational code; detailed errors stay in
	// the application's own records.
	LastErrorCode string `json:"lastErrorCode,omitempty"`

	// ReconcileRequired tells clients the durable tail has holes, so they
	// must reconcile from canonical history instead of trusting backfill.
	ReconcileRequired bool `json:"reconcileRequired"`

	UpdatedAt time.Time `json:"updatedAt"`
}

// Cursor returns the replay cursor a client should tail from.
func (s SessionState) Cursor() JournalCursor {
	runID := ""
	if s.ActiveRun != nil {
		runID = s.ActiveRun.RunID
	}
	return JournalCursor{RunID: runID, StreamEpoch: s.StreamEpoch, SequenceNumber: s.LastSequenceNumber}
}

// IsTerminal reports whether the session has no non-terminal active run.
func (s SessionState) IsTerminal() bool {
	return s.ActiveRun == nil || s.ActiveRun.Outcome != OutcomeNone
}

// InitialStateRevision is the revision a session starts at. Revisions are
// 1-based so that StateTransition.ExpectedRevision == 0 unambiguously means
// "no precondition" and can never collide with a real stored revision.
const InitialStateRevision int64 = 1

// StateTransition is one compare-and-swap request. Apply receives a copy of
// the currently stored state and returns the state to commit; the adapter
// commits it only if the stored revision still equals ExpectedRevision.
//
// Modeling the transition as a function (rather than a full replacement
// record) keeps adapters free to hold columns AgentKit does not know about,
// and keeps the mutation inside the adapter's own transaction.
type StateTransition struct {
	Scope SessionScope `json:"scope"`
	// ExpectedRevision is the CAS precondition. Zero means "no
	// precondition": commit against whatever revision is stored. Real
	// revisions start at InitialStateRevision, so zero is never ambiguous.
	ExpectedRevision int64 `json:"expectedRevision"`
	// Reason is a bounded code for audit, e.g. "pause.requested".
	Reason string `json:"reason,omitempty"`
	// Apply mutates the loaded state in place. It must be pure and
	// re-runnable: an adapter may call it again after a lost race.
	Apply func(*SessionState) `json:"-"`
}

// StateStore is the durable session-state contract.
type StateStore interface {
	// Load returns the current state for the session, creating an idle
	// state at InitialStateRevision when none exists yet. It never returns
	// a nil state with a nil error.
	Load(ctx context.Context, scope SessionScope) (SessionState, error)

	// CompareAndSwap applies the transition under its revision
	// precondition and returns the committed state. On a lost race it
	// returns the authoritative stored state together with an error
	// wrapping ErrRevisionMismatch, so the caller can reconcile without a
	// second read.
	CompareAndSwap(ctx context.Context, t StateTransition) (SessionState, error)
}

// mutateState is the retry-once-on-conflict helper AgentKit uses for its
// own observation writes (activity, cursor, checkpoint). Application
// commands that genuinely need CAS semantics call CompareAndSwap directly
// with an explicit ExpectedRevision.
//
// Observation writes must never fail a run, so the error is returned for
// logging and callers treat it as a degrade.
func mutateState(ctx context.Context, store StateStore, scope SessionScope, reason string, apply func(*SessionState)) (SessionState, error) {
	if store == nil {
		return SessionState{}, nil
	}
	var last error
	for attempt := 0; attempt < 2; attempt++ {
		if err := contextErr(ctx, "StateStore", "CompareAndSwap"); err != nil {
			return SessionState{}, err
		}
		current, err := store.Load(ctx, scope)
		if err != nil {
			return SessionState{}, err
		}
		next, err := store.CompareAndSwap(ctx, StateTransition{
			Scope:            scope,
			ExpectedRevision: current.Revision,
			Reason:           reason,
			Apply:            apply,
		})
		if err == nil {
			return next, nil
		}
		last = err
		if !isRevisionMismatch(err) {
			return next, err
		}
	}
	return SessionState{}, last
}

func isRevisionMismatch(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, ErrRevisionMismatch)
}
