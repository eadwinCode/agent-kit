package agentkit

// Ports are the storage-neutral contracts AgentKit invokes at defined
// lifecycle points. They follow the same dependency-inversion pattern as
// HistoryConfig: AgentKit publishes the interface and owns *when* it is
// called; the application supplies an implementation backed by its own
// database, workflow engine, authorization model and product policy.
//
// No port may leak an application schema. Every method takes a
// context.Context, storage-neutral records and stable IDs, and returns
// typed errors classified by PortError so AgentKit can decide whether a
// failure is fatal, recoverable, or merely degrades durability guarantees.
//
// Failure policy, by port:
//
//	EventJournal  degrade — a failed append sets ReconcileRequired on the run
//	              and execution continues. Replay guarantees are not faked.
//	StateStore    degrade for observation writes, fatal for a CAS the run
//	              needs to own (typed as ErrRevisionMismatch).
//	ControlStore  degrade — an unreachable control plane means "no pause
//	              intent", never a stalled run.
//	ApprovalStore fatal — an approval that cannot be issued, awaited or
//	              consumed must not fall through to executing the tool.
//	Finalizer     fatal for the terminal outcome — AgentKit publishes the
//	              terminal only after Finalize returns, and a typed terminal
//	              failure is reported as the run's outcome.

import (
	"context"
	"errors"
	"fmt"
)

// ContractSchemaVersion is the schema version stamped on every standard
// event envelope and journal record this package produces. Consumers pin
// it; a breaking wire change increments it.
const ContractSchemaVersion = 1

// SessionScope identifies one private conversation owner.
//
// It is deliberately an opaque token, not a struct of named fields. AgentKit
// never parses it, never authorizes with it, and never assumes a tenancy
// model: teams, projects, workspaces, organizations, or a single-tenant
// deployment with none of those are the application's business, and a
// contract that named any of them would force every other consumer to carry
// empty strings to satisfy a shape it does not use.
//
// Compose whatever identifies the owner in your model — a composite key, a
// UUID, an opaque handle. Structured context that adapters genuinely need
// travels in context.Context, which every port method already receives and
// which the application's own request handler populated before AgentKit was
// called.
type SessionScope string

// IsZero reports whether no scope was supplied.
func (s SessionScope) IsZero() bool { return s == "" }

// StreamIdentity names one production epoch of one conversational run.
// A run that is replayed, resumed or restarted keeps its RunID and
// increments StreamEpoch; sequence numbers are monotonic *within* an
// epoch and reset to -1 when the epoch changes.
type StreamIdentity struct {
	ThreadID    string `json:"threadId"`
	RunID       string `json:"runId"`
	StreamEpoch int    `json:"streamEpoch"`
}

// RuntimePorts bundles the adapter implementations plus the scope they are
// invoked under. A nil field disables that capability; AgentKit degrades to
// its pre-port behavior rather than failing.
//
// Pass it through NetworkRunOptions.Ports or RunOptions.Ports (or set a
// default on NetworkConfig/AgentConfig).
type RuntimePorts struct {
	// Journal durably records standard events BEFORE they fan out live.
	Journal EventJournal

	// State loads and compare-and-swaps the durable session state that
	// clients hydrate from.
	State StateStore

	// Control records and observes pause/resume/cancel intent, consulted
	// at every safe boundary.
	Control ControlStore

	// Approvals implements the human-in-the-loop request/wait/consume
	// lifecycle for tools that require it.
	Approvals ApprovalStore

	// Finalizer authorizes the single terminal event after the
	// application's durable facts have settled.
	Finalizer Finalizer

	// StepResults stores exact memoized durable-step results outside the
	// workflow engine. Network runs with a stable RunID wrap their Step so
	// Inngest receives only a bounded reference. Nil keeps legacy inline step
	// results.
	StepResults StepResultStore

	// Sink receives every standard envelope. When set it replaces
	// StreamingConfig.Publish as the delivery path, so an adapter can own
	// ordering, batching and backpressure.
	Sink StreamSink

	// Scope identifies the conversation owner all of the above are keyed by.
	Scope SessionScope

	// StreamEpoch is this execution's production epoch. Increment it when
	// a run is resumed or restarted so clients can discard a stale tail.
	StreamEpoch int
}

// journal returns the journal or nil.
func (p *RuntimePorts) journal() EventJournal {
	if p == nil {
		return nil
	}
	return p.Journal
}

func (p *RuntimePorts) epoch() int {
	if p == nil {
		return 0
	}
	return p.StreamEpoch
}

// sink returns the outbound delivery port or nil.
func (p *RuntimePorts) sink() StreamSink {
	if p == nil {
		return nil
	}
	return p.Sink
}

// wantsStream reports whether these ports alone justify building a
// streaming context — an adapter may want durability or its own sink even
// with no Publish function configured.
func (p *RuntimePorts) wantsStream() bool {
	return p != nil && (p.Sink != nil || p.Journal != nil)
}

func (p *RuntimePorts) scope() SessionScope {
	if p == nil {
		return ""
	}
	return p.Scope
}

// Sentinel errors every adapter should wrap (via PortError or errors.Join)
// so AgentKit and application code can branch on cause, not message text.
var (
	// ErrRevisionMismatch reports a failed compare-and-swap: another
	// writer advanced the session first. The caller reconciles from the
	// returned authoritative state instead of retrying blindly.
	ErrRevisionMismatch = errors.New("agentkit: state revision mismatch")

	// ErrRetentionGap reports that the journal no longer holds events the
	// caller asked for. Clients must fall back to a fresh snapshot.
	ErrRetentionGap = errors.New("agentkit: event retention gap")

	// ErrIdempotencyKeyReuse reports the same command ID submitted with a
	// different payload.
	ErrIdempotencyKeyReuse = errors.New("agentkit: idempotency key reused with a different payload")

	// ErrRunTerminal reports a control command against an already-terminal
	// run. Terminal outcomes are never resurrected.
	ErrRunTerminal = errors.New("agentkit: run is terminal")

	// ErrApprovalExpired reports an approval that passed its deadline
	// before a decision was recorded.
	ErrApprovalExpired = errors.New("agentkit: approval expired")

	// ErrApprovalConsumed reports a second attempt to spend a one-time
	// approved capability.
	ErrApprovalConsumed = errors.New("agentkit: approval already consumed")

	// ErrApprovalDenied reports a recorded denial.
	ErrApprovalDenied = errors.New("agentkit: approval denied")

	// ErrRunCancelled reports that execution stopped because a cancel
	// command was accepted.
	ErrRunCancelled = errors.New("agentkit: run cancelled")

	// ErrPauseExpired reports a pause that outlived its deadline.
	ErrPauseExpired = errors.New("agentkit: pause expired")
)

// PortError is the typed failure adapters return. Code is a bounded,
// user-safe machine code (never a message containing prompts, tool output
// or provider payloads); Recoverable tells AgentKit whether retrying or
// degrading is legitimate.
type PortError struct {
	// Port is the contract name, e.g. "EventJournal".
	Port string
	// Op is the method, e.g. "Append".
	Op string
	// Code is a bounded structured code, e.g. "STATE_REVISION_MISMATCH".
	Code string
	// Recoverable marks a transient failure worth retrying.
	Recoverable bool
	// Err is the wrapped cause; use one of the sentinels above where it
	// applies.
	Err error
}

func (e *PortError) Error() string {
	code := e.Code
	if code == "" {
		code = "PORT_ERROR"
	}
	if e.Err == nil {
		return fmt.Sprintf("agentkit: %s.%s failed (%s)", e.Port, e.Op, code)
	}
	return fmt.Sprintf("agentkit: %s.%s failed (%s): %v", e.Port, e.Op, code, e.Err)
}

// Unwrap exposes the cause to errors.Is/errors.As.
func (e *PortError) Unwrap() error { return e.Err }

// NewPortError builds a PortError. Adapters should prefer it over ad-hoc
// fmt.Errorf so callers keep a machine-readable code.
func NewPortError(port, op, code string, recoverable bool, err error) *PortError {
	return &PortError{Port: port, Op: op, Code: code, Recoverable: recoverable, Err: err}
}

// IsRecoverable reports whether err is a PortError marked recoverable.
func IsRecoverable(err error) bool {
	var pe *PortError
	if errors.As(err, &pe) {
		return pe.Recoverable
	}
	return false
}

// ErrorCode returns the bounded code of a PortError, or "" for any other
// error. Safe to log: codes never carry user content.
func ErrorCode(err error) string {
	var pe *PortError
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

// contextErr normalizes a cancelled context into a typed port error so a
// cancelled run is distinguishable from an adapter failure.
func contextErr(ctx context.Context, port, op string) error {
	if err := ctx.Err(); err != nil {
		return NewPortError(port, op, "CONTEXT_CANCELLED", false, err)
	}
	return nil
}
