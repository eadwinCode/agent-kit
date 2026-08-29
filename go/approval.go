package agentkit

// ApprovalStore is the human-in-the-loop contract: issue a stable approval
// request, wait for a decision, resolve it once, and spend it once.
//
// AgentKit owns the *primitives and their lifecycle timing*. It deliberately
// does not own policy: which tools need approval, who may decide, how long a
// request lives, what gets audited, and how a decision correlates to a
// workflow event are application concerns the adapter enforces on the server.
//
// Replay safety is the reason the contract has three separate verbs.
// Issue is idempotent on RequestID, so an Inngest replay re-issues the same
// request instead of asking the user twice. Wait is re-enterable, so a
// replay after a decision returns the recorded decision instead of parking
// again. Consume is one-time, so an approved capability cannot be spent by a
// replay, a retry, or a second tool call.

import (
	"context"
	"errors"
	"time"
)

// ApprovalRequest is a request for a human decision about one tool call.
type ApprovalRequest struct {
	Scope    SessionScope   `json:"scope"`
	Identity StreamIdentity `json:"identity"`

	// RequestID is stable across replays for the same logical request. It
	// is the idempotency key: re-issuing it returns the existing record.
	RequestID string `json:"requestId"`

	// MessageID and ToolCallID tie the request to the transcript so a
	// reconnecting client can rebuild the approval card.
	MessageID  string `json:"messageId,omitempty"`
	ToolCallID string `json:"toolCallId,omitempty"`
	ToolName   string `json:"toolName"`
	AgentName  string `json:"agentName,omitempty"`

	// Input is the tool input the decision is about. It is passed only to the
	// server-side ApprovalStore for policy/audit and is never copied into the
	// public standard-event stream. Use Summary for bounded display text.
	Input JSONValue `json:"input,omitempty"`

	// Summary is a short user-safe description of the action.
	Summary string `json:"summary,omitempty"`

	// ExpiresAt is the requested deadline. Zero lets the adapter apply its
	// own policy — a ten-minute deadline is typical.
	ExpiresAt time.Time `json:"expiresAt,omitempty"`
}

// ApprovalRecord is the durable state of an approval.
type ApprovalRecord struct {
	RequestID string         `json:"requestId"`
	Status    ApprovalStatus `json:"status"`
	// DecidedBy is a bounded actor identifier for audit; never a name or
	// email unless the application chooses to put one there.
	DecidedBy string `json:"decidedBy,omitempty"`
	// Reason is the decision reason when the application collects one.
	Reason    string     `json:"reason,omitempty"`
	DecidedAt *time.Time `json:"decidedAt,omitempty"`
	ExpiresAt *time.Time `json:"expiresAt,omitempty"`
	// Consumed reports that the approved capability has been spent.
	Consumed bool `json:"consumed"`
}

// Approved reports whether the record authorizes the side effect.
func (r ApprovalRecord) Approved() bool { return r.Status == ApprovalApproved }

// ApprovalWait parks until a decision exists.
type ApprovalWait struct {
	Scope     SessionScope `json:"scope"`
	RequestID string       `json:"requestId"`
	// Deadline bounds the wait; zero uses the record's own expiry.
	Deadline time.Time `json:"deadline,omitempty"`
}

// ApprovalStore is the durable HITL contract.
type ApprovalStore interface {
	// Issue records the request and returns its record. Re-issuing the same
	// RequestID returns the existing record without asking again.
	Issue(ctx context.Context, req ApprovalRequest) (ApprovalRecord, error)

	// Wait blocks until the request is approved, denied or expired.
	// Returning a record with a settled status is preferred over returning
	// an error; ErrApprovalExpired is reserved for a store that cannot
	// represent expiry as a status.
	Wait(ctx context.Context, w ApprovalWait) (ApprovalRecord, error)

	// Consume marks an approved capability spent, exactly once. A second
	// call returns an error wrapping ErrApprovalConsumed.
	Consume(ctx context.Context, scope SessionScope, requestID string) (ApprovalRecord, error)
}

// ApprovalController drives the issue → publish → wait → consume sequence
// and keeps the session state and standard event stream in agreement with
// the store. Tools reach it through ToolOptions.Approvals.
type ApprovalController struct {
	store  ApprovalStore
	state  StateStore
	scope  SessionScope
	stream *StreamingContext
	now    func() time.Time
}

// NewApprovalController builds a controller over one ApprovalStore, for
// applications that need to exercise a gated tool handler directly.
//
// A production run never calls this: AgentKit builds the controller from
// RuntimePorts and supplies it as ToolOptions.Approvals. It exists because
// ToolOptions.Approvals is a concrete type, so without an exported
// constructor a tool's approve/deny behaviour cannot be tested at all
// without standing up a whole network run.
func NewApprovalController(store ApprovalStore, scope SessionScope) *ApprovalController {
	return &ApprovalController{store: store, scope: scope, now: time.Now}
}

func newApprovalController(ports *RuntimePorts, sc *StreamingContext) *ApprovalController {
	c := &ApprovalController{stream: sc, now: time.Now}
	if ports != nil {
		c.store = ports.Approvals
		c.state = ports.State
		c.scope = ports.Scope
	}
	return c
}

// Enabled reports whether an approval store is wired. A tool that requires
// approval MUST refuse to act when this is false rather than proceeding
// unapproved.
func (c *ApprovalController) Enabled() bool { return c != nil && c.store != nil }

// Require runs the full approval lifecycle for one tool call and returns
// the settled record. It returns an error wrapping ErrApprovalDenied,
// ErrApprovalExpired or ErrApprovalConsumed when the side effect must not
// happen — approval failures are fatal by contract, never a degrade.
//
// The approved capability is consumed before Require returns, so the caller
// holds a spent, single-use authorization and a replay cannot spend it again.
func (c *ApprovalController) Require(ctx context.Context, req ApprovalRequest) (ApprovalRecord, error) {
	if !c.Enabled() {
		return ApprovalRecord{}, NewPortError("ApprovalStore", "Require", "APPROVAL_STORE_MISSING", false,
			errors.New("agentkit: a tool required approval but no ApprovalStore is configured"))
	}
	req.Scope = c.scope
	if req.Identity.RunID == "" && c.stream != nil {
		req.Identity = c.stream.Identity()
	}

	record, err := c.store.Issue(ctx, req)
	if err != nil {
		return record, NewPortError("ApprovalStore", "Issue", "APPROVAL_ISSUE_FAILED", false, err)
	}

	c.publishRequested(ctx, req, record)
	c.setApprovalState(ctx, "approval.pending", ApprovalInfo{
		Status: ApprovalPending, ApprovalID: req.RequestID, ExpiresAt: record.ExpiresAt,
	}, LifecycleWaiting)

	if record.Status == ApprovalPending || record.Status == ApprovalNone {
		wait := ApprovalWait{Scope: c.scope, RequestID: req.RequestID}
		if !req.ExpiresAt.IsZero() {
			wait.Deadline = req.ExpiresAt
		}
		settled, waitErr := c.store.Wait(ctx, wait)
		if waitErr != nil {
			if errors.Is(waitErr, ErrApprovalExpired) {
				settled.Status = ApprovalExpired
			} else {
				return settled, NewPortError("ApprovalStore", "Wait", "APPROVAL_WAIT_FAILED", false, waitErr)
			}
		}
		record = settled
	}

	c.publishResolved(ctx, req, record)
	c.setApprovalState(ctx, "approval.resolved", ApprovalInfo{
		Status: record.Status, ApprovalID: req.RequestID, ExpiresAt: record.ExpiresAt,
	}, LifecycleExecuting)

	switch record.Status {
	case ApprovalApproved:
		consumed, err := c.store.Consume(ctx, c.scope, req.RequestID)
		if err != nil {
			return record, NewPortError("ApprovalStore", "Consume", "APPROVAL_CONSUME_FAILED", false, err)
		}
		return consumed, nil
	case ApprovalDenied:
		return record, NewPortError("ApprovalStore", "Require", "APPROVAL_DENIED", false, ErrApprovalDenied)
	case ApprovalExpired:
		return record, NewPortError("ApprovalStore", "Require", "APPROVAL_EXPIRED", false, ErrApprovalExpired)
	default:
		return record, NewPortError("ApprovalStore", "Require", "APPROVAL_UNRESOLVED", false,
			errors.New("agentkit: approval wait returned an unsettled record"))
	}
}

func (c *ApprovalController) publishRequested(ctx context.Context, req ApprovalRequest, rec ApprovalRecord) {
	if c.stream == nil {
		return
	}
	data := map[string]any{
		"approvalId": req.RequestID,
		"toolName":   req.ToolName,
		"messageId":  c.stream.MessageID,
		"runId":      c.stream.RunID,
		"status":     string(ApprovalPending),
	}
	if req.ToolCallID != "" {
		data["toolCallId"] = req.ToolCallID
	}
	if req.Summary != "" {
		data["summary"] = req.Summary
	}
	if rec.ExpiresAt != nil {
		data["expiresAt"] = rec.ExpiresAt.UTC().Format(time.RFC3339Nano)
	}
	c.stream.PublishEvent(ctx, EventHITLRequested, data)
}

func (c *ApprovalController) publishResolved(ctx context.Context, req ApprovalRequest, rec ApprovalRecord) {
	if c.stream == nil {
		return
	}
	data := map[string]any{
		"approvalId": req.RequestID,
		"toolName":   req.ToolName,
		"messageId":  c.stream.MessageID,
		"runId":      c.stream.RunID,
		"status":     string(rec.Status),
		"approved":   rec.Approved(),
	}
	if req.ToolCallID != "" {
		data["toolCallId"] = req.ToolCallID
	}
	if rec.Reason != "" {
		data["reason"] = rec.Reason
	}
	c.stream.PublishEvent(ctx, EventHITLResolved, data)
}

func (c *ApprovalController) setApprovalState(ctx context.Context, reason string, info ApprovalInfo, lifecycle RunLifecycle) {
	if c.state == nil {
		return
	}
	_, _ = mutateState(ctx, c.state, c.scope, reason, func(s *SessionState) {
		s.Approval = info
		if s.ActiveRun != nil && s.ActiveRun.Outcome == OutcomeNone {
			s.ActiveRun.Lifecycle = lifecycle
		}
		if lifecycle == LifecycleWaiting {
			s.Activity = Activity{
				Kind: ActivityWaitingExternal, Label: "Waiting for approval", Source: ActivityFromServer,
			}
		}
	})
}
