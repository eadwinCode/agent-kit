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
//
// Three contract points that adapters get wrong:
//
//   - Record is APPLICATION-owned. AgentKit never calls it; it only reads
//     intent through Poll and parks through Wait. Admitting a run — send,
//     edit, retry, new chat — is likewise not AgentKit's concern.
//
//   - Wait MAY UNWIND ITS CALLER. An adapter backing it with a workflow
//     engine's wait primitive suspends the whole invocation, and the engine
//     later replays the function from the top. AgentKit is built for that:
//     the terminal emitters re-panic a control unwind un-emitted, pause()
//     re-enters idempotently, and pause epochs and durations come from
//     persisted state rather than from anything counted in memory.
//
//   - PauseWait.WaitID is the wait's replay-stable step id. Use it verbatim;
//     deriving one locally reintroduces the collisions it exists to prevent.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/eadwinCode/agent-kit/go/durable"
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
	// WaitID is a replay-stable identifier for THIS pause, unique across the
	// pauses of one run. An adapter backing Wait with a workflow engine's
	// wait primitive must use it verbatim as that primitive's step id:
	// AgentKit owns the pause epoch, so AgentKit owns the id derived from it.
	WaitID string `json:"waitId,omitempty"`
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

	// pauseEpoch is the epoch of the pause this execution last handled. It
	// is derived from persisted state or the control plane, never counted in
	// memory, so a stale resume cannot wake a later pause.
	pauseEpoch int
	// cancelled latches once cancel wins; cancel is terminal.
	cancelled bool
	// pausedTotal mirrors the PERSISTED accumulated pause for this session.
	// It is never a per-execution tally: a replay re-enters pause() with a
	// fresh clock, so anything measured in memory would reset to zero.
	pausedTotal time.Duration
	// streamEpoch is this execution's production epoch, used to keep one
	// pause's wait id distinct from the same pause in a restarted run.
	streamEpoch int
	// reconciled latches once this execution has settled any pause left in
	// flight by a previous invocation.
	reconciled bool
	// now is injectable for deterministic tests.
	now func() time.Time
	// withinStep reports whether the caller is inside a durable step body.
	// Injectable for the same reason now is: inngestgo's step-context marker
	// is unexported, so a test cannot fabricate one.
	withinStep func(context.Context) bool
}

// newRunController builds a controller; a nil ControlStore yields a
// controller whose Checkpoint always continues, so callers need no nil
// checks.
func newRunController(ports *RuntimePorts, sc *StreamingContext, jw *journalWriter) *RunController {
	c := &RunController{stream: sc, journal: jw, now: time.Now, withinStep: durable.IsWithinStep}
	if ports != nil {
		c.control = ports.Control
		c.state = ports.State
		c.scope = ports.Scope
		c.streamEpoch = ports.epoch()
	}
	return c
}

// Enabled reports whether a control plane is wired.
func (c *RunController) Enabled() bool { return c != nil && c.control != nil }

// PausedTotal is the session's accumulated paused duration.
//
// It reads PERSISTED state rather than an in-memory tally. After a pause
// resumes, the replayed execution polls a control plane with no outstanding
// intent, so it never re-enters pause() at all — an in-memory total would
// be zero in exactly the execution that emits the terminal event. The
// in-memory value is only the fallback for a run with no StateStore.
func (c *RunController) PausedTotal(ctx context.Context) time.Duration {
	if c == nil {
		return 0
	}
	if c.state == nil {
		return c.pausedTotal
	}
	state, err := c.state.Load(ctx, c.scope)
	if err != nil {
		return c.pausedTotal
	}
	return time.Duration(state.Pause.AccumulatedPausedMs) * time.Millisecond
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
	// A boundary raised from inside a durable step body cannot park: no
	// workflow engine can suspend a function from within one of its own
	// steps, so no ControlStore.Wait could honour it. Downgrade it to a
	// non-resumable boundary — the pause intent is still reported, and the
	// run stops at its next real boundary instead of entering a wait that
	// cannot work.
	if cp.Resumable && c.withinStep != nil && c.withinStep(ctx) {
		cp.Resumable = false
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
		// Nothing outstanding. If a previous invocation parked at a pause,
		// this is where that pause is settled — see reconcileOnce.
		c.reconcileOnce(ctx)
		return nil
	}
}

// pauseRecord is what persisted state knows about this session's pause.
//
// It replaces an in-memory pause counter, which cannot work across a durable
// wait: the wait suspends the whole invocation and the engine replays it from
// the top, so any counter restarts and two different pauses collide on one
// epoch number.
type pauseRecord struct {
	// inFlight reports a pause that is recorded as paused right now, which
	// on a replayed invocation is the pause this execution is re-entering.
	inFlight bool
	// epoch is the last pause epoch persisted for the session.
	epoch int
	// pausedAt is when the pause ACTUALLY began — after a replay that is
	// earlier than anything this execution's clock can observe.
	pausedAt time.Time
	// kind is the boundary the run stopped at.
	kind CheckpointKind
	// accumulated is the session's persisted paused milliseconds so far.
	accumulated int64
}

// loadPause reads the session's pause from persisted state. Without a
// StateStore it degrades to the in-memory values, which is correct for a run
// that cannot be replayed anyway.
func (c *RunController) loadPause(ctx context.Context) pauseRecord {
	if c.state == nil {
		return pauseRecord{epoch: c.pauseEpoch, accumulated: c.pausedTotal.Milliseconds()}
	}
	state, err := c.state.Load(ctx, c.scope)
	if err != nil {
		return pauseRecord{epoch: c.pauseEpoch, accumulated: c.pausedTotal.Milliseconds()}
	}
	record := pauseRecord{
		epoch:       state.Pause.Epoch,
		accumulated: state.Pause.AccumulatedPausedMs,
		kind:        CheckpointKind(state.CheckpointKind),
	}
	if state.Pause.State == PausePaused && state.Pause.PausedAt != nil {
		record.inFlight = true
		record.pausedAt = *state.Pause.PausedAt
	}
	return record
}

// pauseWaitID is the replay-stable id for one pause. The stream epoch keeps
// a resumed run's pauses distinct from the ones it replaced, and the pause
// epoch keeps two pauses of one run distinct from each other.
func pauseWaitID(streamEpoch, pauseEpoch int, kind CheckpointKind) string {
	return fmt.Sprintf("pause-e%d-%d:%s", streamEpoch, pauseEpoch, kind)
}

// reconcileOnce settles a pause that a PREVIOUS invocation parked at.
//
// It runs at the first boundary where the control plane reports nothing
// outstanding, which is exactly the situation after a resume: the durable
// wait completed, the engine replayed the function, and this execution polls
// a clean control plane — so pause() is never re-entered and nothing else
// would ever clear the paused state or bank the time spent in it.
//
// Once per execution: it costs one state read, and only on the continue path.
func (c *RunController) reconcileOnce(ctx context.Context) {
	if c.reconciled {
		return
	}
	c.reconciled = true
	record := c.loadPause(ctx)
	if !record.inFlight {
		return
	}
	c.settlePause(ctx, record.kind, record.epoch, record.pausedAt, record.accumulated)
}

// settlePause banks the elapsed pause onto the persisted total and returns
// the session to executing. Paused time accrues in SEGMENTS, so a pause that
// spanned a replay is still counted exactly once.
func (c *RunController) settlePause(
	ctx context.Context,
	kind CheckpointKind,
	epoch int,
	pausedAt time.Time,
	accumulated int64,
) {
	elapsed := c.now().Sub(pausedAt)
	if elapsed < 0 {
		elapsed = 0
	}
	total := accumulated + elapsed.Milliseconds()
	c.pausedTotal = time.Duration(total) * time.Millisecond
	c.updateState(ctx, "pause.resumed", func(s *SessionState) {
		s.Pause.State = PauseNone
		s.Pause.PausedAt = nil
		s.Pause.RequestedAt = nil
		s.Pause.ExpiresAt = nil
		s.Pause.AccumulatedPausedMs = total
		if s.ActiveRun != nil {
			s.ActiveRun.Lifecycle = LifecycleExecuting
		}
	})
	c.publishState(ctx, EventStateUpdated, map[string]any{
		"pauseState":          string(PauseNone),
		"checkpoint":          string(kind),
		"pauseEpoch":          epoch,
		"accumulatedPausedMs": total,
	})
}

// pause parks the run at cp until a correlated resume or cancel arrives.
//
// A durable ControlStore.Wait suspends the whole invocation and the engine
// replays it, so pause() is written to be re-entrant: a replay that finds
// this pause still recorded adopts its epoch, its wait id and its original
// timestamp instead of announcing a second pause over the top of it.
func (c *RunController) pause(ctx context.Context, cp Checkpoint, signal ControlSignal) error {
	record := c.loadPause(ctx)
	epoch, pausedAt := record.epoch, record.pausedAt
	if !record.inFlight {
		// The control plane owns epoch identity when it supplies one; the
		// persisted epoch only seeds the fallback.
		epoch = record.epoch + 1
		if signal.PauseEpoch > 0 {
			epoch = signal.PauseEpoch
		}
		pausedAt = c.now()
		state := c.updateState(ctx, "pause.paused", func(s *SessionState) {
			s.Pause.State = PausePaused
			s.Pause.PausedAt = &pausedAt
			s.Pause.Epoch = epoch
			s.Pause.ExpiresAt = signal.Deadline
			s.CheckpointKind = string(cp.Kind)
			if s.ActiveRun != nil {
				s.ActiveRun.Lifecycle = LifecycleWaiting
			}
		})
		if state.Pause.PausedAt != nil {
			pausedAt = *state.Pause.PausedAt
		}
		c.publishState(ctx, EventStateUpdated, map[string]any{
			"pauseState": string(PausePaused),
			"checkpoint": string(cp.Kind),
			"pauseEpoch": epoch,
		})
	}
	c.pauseEpoch = epoch

	wait := PauseWait{
		Scope: c.scope, Identity: cp.Identity,
		PauseEpoch: epoch, Checkpoint: cp.Kind,
		WaitID: pauseWaitID(c.streamEpoch, epoch, cp.Kind),
	}
	if signal.Deadline != nil {
		wait.Deadline = *signal.Deadline
	}

	resumed, err := c.control.Wait(ctx, wait)

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

	c.settlePause(ctx, cp.Kind, epoch, pausedAt, record.accumulated)
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

func (c *RunController) updateState(ctx context.Context, reason string, apply func(*SessionState)) SessionState {
	if c.state == nil {
		return SessionState{}
	}
	// Observation writes degrade: they must never fail a run.
	state, _ := mutateState(ctx, c.state, c.scope, reason, apply)
	return state
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
