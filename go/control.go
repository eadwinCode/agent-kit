package agentkit

// ControlStore and RunController are the pause/resume/cancel contract.
//
// Pause is a control intent, not a promise that the provider has already
// stopped. A provider request cannot be frozen mid-token and resumed from
// its internal model state, so AgentKit takes a pause at the next SAFE
// BOUNDARY: after the active inference records its complete result, before
// a returned tool executes, between iterations of the network loop, and at
// whatever additional checkpoints a tool declares. An atomic external side
// effect that has already begun finishes first — a half-applied edit is
// worse than a late pause.
//
// The durable command record is authoritative, never the wake event.
// Inngest documents that an event sent before its wait registers can be
// missed, so the controller checks the store both before entering a wait
// and after returning from it, and a resume dispatcher retries the wake
// until state actually advances.

import (
	"context"
	"errors"
	"time"
)

// CommandType is the bounded set of control commands.
type CommandType string

const (
	CommandSend    CommandType = "send"
	CommandPause   CommandType = "pause"
	CommandResume  CommandType = "resume"
	CommandCancel  CommandType = "cancel"
	CommandApprove CommandType = "approve"
	CommandDeny    CommandType = "deny"
	CommandRetry   CommandType = "retry"
	CommandEdit    CommandType = "edit"
	CommandNewChat CommandType = "new_chat"
)

// CommandStatus is the recorded lifecycle of a command.
type CommandStatus string

const (
	CommandAccepted CommandStatus = "accepted"
	CommandApplied  CommandStatus = "applied"
	CommandRejected CommandStatus = "rejected"
	CommandExpired  CommandStatus = "expired"
)

// ControlCommand is one idempotent control request.
type ControlCommand struct {
	Scope SessionScope `json:"scope"`
	ID    string       `json:"commandId"`
	Type  CommandType  `json:"commandType"`

	ThreadID string `json:"threadId,omitempty"`
	RunID    string `json:"runId,omitempty"`

	// PayloadHash detects reuse of one idempotency key with a different
	// request. It is a hash, never prompt or tool content.
	PayloadHash string `json:"payloadHash,omitempty"`

	// ExpectedRevision is an optional client CAS precondition.
	ExpectedRevision int64 `json:"expectedRevision,omitempty"`

	// PauseEpoch correlates a resume with the pause it is answering, so a
	// stale resume cannot wake a later pause.
	PauseEpoch int `json:"pauseEpoch,omitempty"`

	// ApprovalID correlates approve/deny commands with the durable
	// approval record. The approval payload itself is never duplicated here.
	ApprovalID string `json:"approvalId,omitempty"`

	CreatedAt time.Time  `json:"createdAt"`
	ExpiresAt *time.Time `json:"expiresAt,omitempty"`
}

// CommandResult is the recorded outcome of a command. Replaying the same
// command ID with the same payload hash must return the recorded result
// verbatim rather than applying the command twice.
type CommandResult struct {
	CommandID string        `json:"commandId"`
	Status    CommandStatus `json:"status"`
	// OutcomeCode is a bounded structured result code.
	OutcomeCode string `json:"outcomeCode,omitempty"`
	// Duplicate reports that the recorded result was returned rather than
	// a fresh application of the command.
	Duplicate bool `json:"duplicate"`
	// State is the authoritative session state after the command.
	State SessionState `json:"state"`
	// AppliedAt is set once the command took effect.
	AppliedAt *time.Time `json:"appliedAt,omitempty"`
}

// ControlAction is what the controller tells the run to do at a boundary.
type ControlAction string

const (
	// ControlContinue means no control intent is outstanding.
	ControlContinue ControlAction = "continue"
	// ControlPause means the run must checkpoint and wait here.
	ControlPause ControlAction = "pause"
	// ControlCancel means the run must stop; cancel is terminal and beats
	// a concurrent pause.
	ControlCancel ControlAction = "cancel"
)

// ControlSignal is the control plane's answer at a safe boundary.
type ControlSignal struct {
	Action ControlAction `json:"action"`
	// CommandID is the command that produced this signal, for audit.
	CommandID string `json:"commandId,omitempty"`
	// PauseEpoch identifies the pause a subsequent resume must match.
	PauseEpoch int `json:"pauseEpoch,omitempty"`
	// Reason is a bounded code such as "pause_expired".
	Reason string `json:"reason,omitempty"`
	// Deadline is when a pause expires, if the adapter enforces one.
	Deadline *time.Time `json:"deadline,omitempty"`
}

// CheckpointKind names the safe boundaries AgentKit checks. Tools declare
// their own via StructuredStream.Checkpoint.
type CheckpointKind string

const (
	// CheckpointRunStart is before any inference for the run.
	CheckpointRunStart CheckpointKind = "run_start"
	// CheckpointBeforeInference is before a provider call.
	CheckpointBeforeInference CheckpointKind = "before_inference"
	// CheckpointAfterInference is after the provider result is recorded and
	// before any returned tool executes. This is where a pause requested
	// during inference actually takes effect.
	CheckpointAfterInference CheckpointKind = "after_inference"
	// CheckpointBeforeTool is before one tool handler runs.
	CheckpointBeforeTool CheckpointKind = "before_tool"
	// CheckpointAfterTool is after one tool handler's result is recorded.
	CheckpointAfterTool CheckpointKind = "after_tool"
	// CheckpointBeforeSideEffect is a tool-declared boundary before its
	// first irreversible external write.
	CheckpointBeforeSideEffect CheckpointKind = "before_side_effect"
	// CheckpointAfterSideEffect is a tool-declared boundary after an atomic
	// external write completed.
	CheckpointAfterSideEffect CheckpointKind = "after_side_effect"
	// CheckpointBetweenItems is a tool-declared boundary in an iterative or
	// batch tool; completed items stay checkpointed and are not repeated.
	CheckpointBetweenItems CheckpointKind = "between_items"
	// CheckpointRetryBackoff is between provider/tool retry attempts. A
	// pause here must not start another attempt.
	CheckpointRetryBackoff CheckpointKind = "retry_backoff"
	// CheckpointNetworkIteration is between agents in the network loop.
	CheckpointNetworkIteration CheckpointKind = "network_iteration"
)

// Checkpoint describes one safe boundary being offered to the control plane.
type Checkpoint struct {
	Scope    SessionScope   `json:"scope"`
	Identity StreamIdentity `json:"identity"`
	Kind     CheckpointKind `json:"kind"`
	// AgentName and ToolName are bounded labels for audit and UI, never
	// prompt or tool payloads.
	AgentName string `json:"agentName,omitempty"`
	ToolName  string `json:"toolName,omitempty"`
	// Resumable reports whether the run can genuinely stop here. A boundary
	// inside an atomic side effect is offered with Resumable false so the
	// control plane can display "finishing current action" truthfully.
	Resumable bool `json:"resumable"`
}

// PauseWait parks a run at a paused checkpoint until it may continue.
type PauseWait struct {
	Scope    SessionScope   `json:"scope"`
	Identity StreamIdentity `json:"identity"`
	// PauseEpoch is the pause being waited on; a resume for a different
	// epoch must not wake it.
	PauseEpoch int `json:"pauseEpoch"`
	// Checkpoint is the boundary the run stopped at.
	Checkpoint CheckpointKind `json:"checkpoint"`
	// Deadline bounds the wait. Zero means the adapter's default policy
	// (24 hours for Pause v1).
	Deadline time.Time `json:"deadline,omitempty"`
}

// ControlStore is the durable control-plane contract.
//
// Poll must be cheap: AgentKit calls it at every safe boundary of every
// iteration. An adapter that cannot answer should return ControlContinue
// with an error — an unreachable control plane means "no pause intent",
// never a stalled run.
type ControlStore interface {
	// Record persists a command idempotently and returns its recorded
	// result. The same ID with the same PayloadHash returns the first
	// result with Duplicate set; the same ID with a different hash returns
	// an error wrapping ErrIdempotencyKeyReuse.
	Record(ctx context.Context, cmd ControlCommand) (CommandResult, error)

	// Poll reports the outstanding control intent for a run at a safe
	// boundary, without blocking.
	Poll(ctx context.Context, c Checkpoint) (ControlSignal, error)

	// Wait parks the run until a correlated resume or cancel arrives, or
	// the pause deadline passes. Implementations back this with their
	// durable workflow engine's wait primitive AND re-read the command
	// store around it, because a wake event can be sent before the wait
	// registers.
	Wait(ctx context.Context, w PauseWait) (ControlSignal, error)
}

// RunController evaluates safe boundaries against a ControlStore and
// records the resulting pause/cancel transitions in the StateStore. It is
// AgentKit's internal user of the two ports; applications implement the
// ports, not this type.
type RunController struct {
	control ControlStore
	state   StateStore
	scope   SessionScope
	stream  *StreamingContext
	journal *journalWriter

	// pauseEpoch increments on every effective pause so a stale resume
	// cannot wake a later one.
	pauseEpoch int
	// cancelled latches once cancel wins; cancel is terminal.
	cancelled bool
	// pausedTotal accumulates paused wall-clock for the "active time"
	// display.
	pausedTotal time.Duration
	// now is injectable for deterministic tests.
	now func() time.Time
}

// newRunController builds a controller; a nil ControlStore yields a
// controller whose Checkpoint always continues, so callers need no nil
// checks.
func newRunController(ports *RuntimePorts, sc *StreamingContext, jw *journalWriter) *RunController {
	c := &RunController{stream: sc, journal: jw, now: time.Now}
	if ports != nil {
		c.control = ports.Control
		c.state = ports.State
		c.scope = ports.Scope
	}
	return c
}

// Enabled reports whether a control plane is wired.
func (c *RunController) Enabled() bool { return c != nil && c.control != nil }

// PausedTotal is the accumulated paused duration of this execution.
func (c *RunController) PausedTotal() time.Duration {
	if c == nil {
		return 0
	}
	return c.pausedTotal
}

// Checkpoint offers one safe boundary to the control plane and applies the
// answer:
//
//   - continue → returns nil immediately;
//   - cancel   → returns an error wrapping ErrRunCancelled (terminal);
//   - pause    → records the paused state, publishes a state event, waits
//     for a correlated resume, then returns nil so execution
//     continues from exactly this boundary.
//
// A non-resumable checkpoint reports the pause intent but does not park;
// the run continues to its next resumable boundary, which is how "finishing
// current action" stays truthful.
//
// Control-plane failures degrade to continue: a run must never stall
// because a command table was briefly unreachable.
func (c *RunController) Checkpoint(ctx context.Context, cp Checkpoint) error {
	if !c.Enabled() {
		return nil
	}
	if c.cancelled {
		return NewPortError("ControlStore", "Checkpoint", "RUN_CANCELLED", false, ErrRunCancelled)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	cp.Scope = c.scope
	if cp.Identity.RunID == "" && c.stream != nil {
		cp.Identity = c.stream.Identity()
	}

	signal, err := c.control.Poll(ctx, cp)
	if err != nil {
		// Degrade: no control intent observed.
		return nil
	}

	switch signal.Action {
	case ControlCancel:
		c.cancelled = true
		c.recordTerminalIntent(ctx, signal)
		return NewPortError("ControlStore", "Checkpoint", "RUN_CANCELLED", false, ErrRunCancelled)
	case ControlPause:
		if !cp.Resumable {
			// Truthful intermediate state: intent acknowledged, execution
			// continues to the next boundary that can actually stop.
			c.publishPauseIntent(ctx, cp)
			return nil
		}
		return c.pause(ctx, cp, signal)
	default:
		return nil
	}
}

// pause parks the run at cp until a correlated resume or cancel arrives.
func (c *RunController) pause(ctx context.Context, cp Checkpoint, signal ControlSignal) error {
	c.pauseEpoch++
	epoch := c.pauseEpoch
	pausedAt := c.now()

	c.updateState(ctx, "pause.paused", func(s *SessionState) {
		s.Pause.State = PausePaused
		s.Pause.PausedAt = &pausedAt
		s.Pause.Epoch = epoch
		s.Pause.ExpiresAt = signal.Deadline
		s.CheckpointKind = string(cp.Kind)
		if s.ActiveRun != nil {
			s.ActiveRun.Lifecycle = LifecycleWaiting
		}
	})
	c.publishState(ctx, EventStateUpdated, map[string]any{
		"pauseState": string(PausePaused),
		"checkpoint": string(cp.Kind),
		"pauseEpoch": epoch,
	})

	wait := PauseWait{
		Scope: c.scope, Identity: cp.Identity,
		PauseEpoch: epoch, Checkpoint: cp.Kind,
	}
	if signal.Deadline != nil {
		wait.Deadline = *signal.Deadline
	}

	resumed, err := c.control.Wait(ctx, wait)
	elapsed := c.now().Sub(pausedAt)
	if elapsed > 0 {
		c.pausedTotal += elapsed
	}

	if err != nil {
		if errors.Is(err, ErrRunCancelled) || errors.Is(err, ErrPauseExpired) {
			c.cancelled = true
			c.recordTerminalIntent(ctx, ControlSignal{Action: ControlCancel, Reason: reasonOf(err)})
			return NewPortError("ControlStore", "Wait", "RUN_CANCELLED", false, err)
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		// Degrade: a failed wait must not strand the run forever.
		resumed = ControlSignal{Action: ControlContinue}
	}
	if resumed.Action == ControlCancel {
		c.cancelled = true
		c.recordTerminalIntent(ctx, resumed)
		return NewPortError("ControlStore", "Wait", "RUN_CANCELLED", false, ErrRunCancelled)
	}

	paused := c.pausedTotal.Milliseconds()
	c.updateState(ctx, "pause.resumed", func(s *SessionState) {
		s.Pause.State = PauseNone
		s.Pause.PausedAt = nil
		s.Pause.RequestedAt = nil
		s.Pause.ExpiresAt = nil
		s.Pause.AccumulatedPausedMs = paused
		if s.ActiveRun != nil {
			s.ActiveRun.Lifecycle = LifecycleExecuting
		}
	})
	c.publishState(ctx, EventStateUpdated, map[string]any{
		"pauseState":          string(PauseNone),
		"checkpoint":          string(cp.Kind),
		"pauseEpoch":          epoch,
		"accumulatedPausedMs": paused,
	})
	return nil
}

func (c *RunController) publishPauseIntent(ctx context.Context, cp Checkpoint) {
	c.updateState(ctx, "pause.requested", func(s *SessionState) {
		if s.Pause.State == PauseNone {
			now := c.now()
			s.Pause.RequestedAt = &now
		}
		s.Pause.State = PauseRequested
		s.CheckpointKind = string(cp.Kind)
	})
	c.publishState(ctx, EventStateUpdated, map[string]any{
		"pauseState": string(PauseRequested),
		"checkpoint": string(cp.Kind),
		"resumable":  false,
	})
}

func (c *RunController) recordTerminalIntent(ctx context.Context, signal ControlSignal) {
	c.updateState(ctx, "run.cancelling", func(s *SessionState) {
		s.Pause.State = PauseNone
		if s.ActiveRun != nil {
			s.ActiveRun.Lifecycle = LifecycleTerminalizing
			s.ActiveRun.Outcome = OutcomeCancelled
		}
		if signal.Reason != "" {
			s.LastErrorCode = signal.Reason
		}
	})
}

func (c *RunController) updateState(ctx context.Context, reason string, apply func(*SessionState)) {
	if c.state == nil {
		return
	}
	// Observation writes degrade: they must never fail a run.
	_, _ = mutateState(ctx, c.state, c.scope, reason, apply)
}

func (c *RunController) publishState(ctx context.Context, event string, data map[string]any) {
	if c.stream == nil {
		return
	}
	c.stream.PublishEvent(ctx, event, data)
}

func reasonOf(err error) string {
	switch {
	case errors.Is(err, ErrPauseExpired):
		return "pause_expired"
	case errors.Is(err, ErrRunCancelled):
		return "cancelled"
	default:
		return ""
	}
}

// IsCancelled reports whether err is the controller's terminal cancel.
func IsCancelled(err error) bool { return errors.Is(err, ErrRunCancelled) }
