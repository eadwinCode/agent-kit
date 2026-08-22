package agentkit

// StructuredStream is the typed emitter AgentKit hands to tools and to the
// network router.
//
// Before it existed, an application that wanted to show "Reading project
// files" or push a domain payload alongside the transcript had to wrap the
// publish function and inject envelopes into someone else's stream — which
// means inspecting raw outbound chunks, guessing at sequence numbers, and
// depending on internals that are not part of any contract. StructuredStream
// replaces that with a public API: stable part identity, ordered
// created → delta → completed lifecycles, and the same journal-before-fan-out
// durability every other event gets.
//
// StreamSink is the other half: the outbound delivery port. An adapter that
// implements it owns batching, backpressure and transport, and receives
// exactly the envelopes AgentKit produced — in order, already journaled.

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
)

// JSONValue is an opaque JSON payload. It is carried end-to-end as raw
// bytes: decoding it into a map would re-order keys and break wire parity
// with the TypeScript package.
type JSONValue = json.RawMessage

// Additional standard event names introduced by the port contracts.
const (
	// EventStateUpdated carries session control/revision metadata. It never
	// duplicates transcript content.
	EventStateUpdated = "state.updated"
	// EventStatusUpdated carries a semantic activity change.
	EventStatusUpdated = "status.updated"
	// EventDataPart carries an application domain payload as a structured
	// part of the transcript.
	EventDataPart = "data.part"
	// EventUserMessage carries the server-accepted user turn so every
	// authorized client renders it immediately, with one stable message ID.
	EventUserMessage = "user.message"
)

// PartLifecycle is the ordered lifecycle every structured part follows.
// Emitting a delta or a completion for an unopened part is a contract
// violation; the reducer on the client side treats it as a gap.
type PartLifecycle string

const (
	PartOpen      PartLifecycle = "created"
	PartDelta     PartLifecycle = "delta"
	PartClose     PartLifecycle = "completed"
	PartFailClose PartLifecycle = "failed"
)

// StatusUpdate is a truthful semantic activity change.
//
// Kind must describe work that is actually happening. In particular
// ActivityThinking requires a provider-returned reasoning signal: a tool
// may not claim the model is thinking because it is slow.
type StatusUpdate struct {
	Kind ActivityKind `json:"kind"`
	// Label is a short user-safe string, e.g. "Reading project files".
	Label string `json:"label,omitempty"`
	// Source records the observer.
	Source ActivitySource `json:"source,omitempty"`
	// Metadata is optional bounded context for the UI.
	Metadata map[string]any `json:"metadata,omitempty"`
}

// DataPart is an application domain payload rendered as part of the
// transcript.
type DataPart struct {
	// PartID is stable for the part's whole lifecycle. Leave it empty to
	// have the stream mint one.
	PartID string `json:"partId,omitempty"`
	// Type names the application's part kind, e.g. "file-diff".
	Type string `json:"type"`
	// Payload is the opaque domain payload.
	Payload JSONValue `json:"payload,omitempty"`
	// Metadata is bounded display context.
	Metadata map[string]any `json:"metadata,omitempty"`
}

// ToolProgress reports incremental progress of a long-running tool.
type ToolProgress struct {
	ToolName string `json:"toolName"`
	// PartID ties progress to the tool-call part when known.
	PartID string `json:"partId,omitempty"`
	// Completed and Total describe an iterative tool's item counts. Total 0
	// means the tool cannot predict the count.
	Completed int `json:"completed"`
	Total     int `json:"total,omitempty"`
	// Label is a short user-safe description of the current item.
	Label string `json:"label,omitempty"`
}

// StructuredStream is the public emitter passed to tools and the router.
//
// Every method is best-effort with respect to *delivery* and durable with
// respect to *ordering*: envelopes are journaled before fan-out and carry
// the run's shared, gapless sequence numbers. A method never returns an
// error, because a tool must not fail because a socket did.
type StructuredStream interface {
	// Status publishes a semantic activity change.
	Status(ctx context.Context, u StatusUpdate)

	// Data opens, updates and closes a structured domain part. Calling
	// Data with the same PartID more than once updates the open part;
	// CompleteData closes it.
	Data(ctx context.Context, p DataPart) string

	// DataDelta appends an incremental payload to an open data part.
	DataDelta(ctx context.Context, partID string, delta JSONValue)

	// CompleteData closes an open data part with its final payload.
	CompleteData(ctx context.Context, partID string, final JSONValue)

	// Progress publishes tool progress.
	Progress(ctx context.Context, p ToolProgress)

	// Checkpoint offers a tool-declared safe boundary to the control plane.
	// It returns an error only when the run must stop here — a cancel, or a
	// context that is already done. A pause parks inside the call and
	// returns nil when the run resumes.
	//
	// Declare before_side_effect before the first irreversible write,
	// after_side_effect once an atomic write completed, and between_items
	// in an iterative tool.
	Checkpoint(ctx context.Context, kind CheckpointKind) error

	// Identity is the stream identity of the current run epoch.
	Identity() StreamIdentity
}

// StreamSink is the outbound delivery port. When RuntimePorts.Sink is set
// it replaces StreamingConfig.Publish, so an adapter owns transport,
// batching and backpressure while AgentKit keeps ordering and durability.
//
// Deliver must not block for long: it runs on the execution path. Returning
// an error is legitimate — delivery is best-effort by contract and the
// journal is what makes the tail recoverable.
type StreamSink interface {
	Deliver(ctx context.Context, chunk AgentMessageChunk) error
}

// SinkFunc adapts a function to StreamSink.
type SinkFunc func(ctx context.Context, chunk AgentMessageChunk) error

// Deliver implements StreamSink.
func (f SinkFunc) Deliver(ctx context.Context, chunk AgentMessageChunk) error { return f(ctx, chunk) }

// runStream is the StructuredStream implementation bound to one run scope.
type runStream struct {
	sc         *StreamingContext
	controller *RunController
	agentName  string
	toolName   string
	partSeq    atomic.Int64
	// open tracks data parts opened through this stream so DataDelta and
	// CompleteData can enforce the created → delta → completed order.
	open sync.Map
}

// newRunStream builds the emitter for one scope.
func newRunStream(sc *StreamingContext, controller *RunController, agentName, toolName string) *runStream {
	return &runStream{sc: sc, controller: controller, agentName: agentName, toolName: toolName}
}

func (s *runStream) Identity() StreamIdentity {
	if s == nil || s.sc == nil {
		return StreamIdentity{}
	}
	return s.sc.Identity()
}

func (s *runStream) Status(ctx context.Context, u StatusUpdate) {
	if s == nil || s.sc == nil || u.Kind == "" {
		return
	}
	data := map[string]any{
		"kind":      string(u.Kind),
		"messageId": s.sc.MessageID,
		"runId":     s.sc.RunID,
	}
	if u.Label != "" {
		data["label"] = u.Label
	}
	if u.Source != "" {
		data["source"] = string(u.Source)
	}
	if s.agentName != "" {
		data["agentName"] = s.agentName
	}
	if s.toolName != "" {
		data["toolName"] = s.toolName
	}
	if len(u.Metadata) > 0 {
		data["metadata"] = u.Metadata
	}
	s.sc.PublishEvent(ctx, EventStatusUpdated, data)
	s.sc.recordActivity(ctx, Activity{Kind: u.Kind, Label: u.Label, Source: u.Source})
}

func (s *runStream) Data(ctx context.Context, p DataPart) string {
	if s == nil || s.sc == nil || p.Type == "" {
		return ""
	}
	partID := p.PartID
	if partID == "" {
		partID = s.mintPartID(p.Type)
	}
	if _, existed := s.open.LoadOrStore(partID, p.Type); !existed {
		created := map[string]any{
			"partId": partID, "runId": s.sc.RunID, "messageId": s.sc.MessageID,
			"type": p.Type,
		}
		if md := s.partMetadata(p.Metadata); md != nil {
			created["metadata"] = md
		}
		s.sc.PublishEvent(ctx, EventPartCreated, created)
	}
	if len(p.Payload) > 0 {
		s.sc.PublishEvent(ctx, EventDataDelta, map[string]any{
			"partId": partID, "messageId": s.sc.MessageID, "delta": p.Payload,
			"type": p.Type,
		})
	}
	return partID
}

func (s *runStream) DataDelta(ctx context.Context, partID string, delta JSONValue) {
	if s == nil || s.sc == nil || partID == "" || len(delta) == 0 {
		return
	}
	if _, ok := s.open.Load(partID); !ok {
		// created → delta ordering is part of the contract; a delta for an
		// unopened part is dropped rather than published out of order.
		return
	}
	s.sc.PublishEvent(ctx, EventDataDelta, map[string]any{
		"partId": partID, "messageId": s.sc.MessageID, "delta": delta,
	})
}

func (s *runStream) CompleteData(ctx context.Context, partID string, final JSONValue) {
	if s == nil || s.sc == nil || partID == "" {
		return
	}
	kind, ok := s.open.LoadAndDelete(partID)
	if !ok {
		return
	}
	data := map[string]any{
		"partId": partID, "runId": s.sc.RunID, "messageId": s.sc.MessageID,
		"type": kind,
	}
	if len(final) > 0 {
		data["finalContent"] = final
	}
	s.sc.PublishEvent(ctx, EventPartCompleted, data)
}

func (s *runStream) Progress(ctx context.Context, p ToolProgress) {
	if s == nil || s.sc == nil {
		return
	}
	name := p.ToolName
	if name == "" {
		name = s.toolName
	}
	data := map[string]any{
		"messageId": s.sc.MessageID, "runId": s.sc.RunID,
		"toolName": name, "completed": p.Completed,
	}
	if p.PartID != "" {
		data["partId"] = p.PartID
	}
	if p.Total > 0 {
		data["total"] = p.Total
	}
	if p.Label != "" {
		data["label"] = p.Label
	}
	s.sc.PublishEvent(ctx, EventStatusUpdated, data)
}

func (s *runStream) Checkpoint(ctx context.Context, kind CheckpointKind) error {
	if s == nil || !s.controller.Enabled() {
		return ctx.Err()
	}
	return s.controller.Checkpoint(ctx, Checkpoint{
		Kind: kind, AgentName: s.agentName, ToolName: s.toolName,
		Resumable: kind != CheckpointAfterSideEffect,
	})
}

func (s *runStream) mintPartID(kind string) string {
	n := s.partSeq.Add(1) - 1
	return fmt.Sprintf("data_%s_%s_%d", shortID(s.sc.MessageID, 8), sanitizePartKind(kind), n)
}

func (s *runStream) partMetadata(extra map[string]any) map[string]any {
	if len(extra) == 0 && s.agentName == "" && s.toolName == "" {
		return nil
	}
	md := make(map[string]any, len(extra)+2)
	for k, v := range extra {
		md[k] = v
	}
	if s.agentName != "" {
		md["agentName"] = s.agentName
	}
	if s.toolName != "" {
		md["toolName"] = s.toolName
	}
	return md
}

func sanitizePartKind(kind string) string {
	out := make([]rune, 0, len(kind))
	for _, r := range kind {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			out = append(out, r)
		case r >= 'A' && r <= 'Z':
			out = append(out, r+('a'-'A'))
		default:
			out = append(out, '_')
		}
	}
	if len(out) > 16 {
		out = out[:16]
	}
	return string(out)
}

func shortID(id string, n int) string {
	cleaned := make([]rune, 0, len(id))
	for _, r := range id {
		if r != '-' {
			cleaned = append(cleaned, r)
		}
	}
	if len(cleaned) > n {
		cleaned = cleaned[:n]
	}
	return string(cleaned)
}

// noopStream satisfies StructuredStream when no streaming context exists,
// so tools can call opts.Stream unconditionally.
type noopStream struct{}

func (noopStream) Status(context.Context, StatusUpdate)                   {}
func (noopStream) Data(context.Context, DataPart) string                  { return "" }
func (noopStream) DataDelta(context.Context, string, JSONValue)           {}
func (noopStream) CompleteData(context.Context, string, JSONValue)        {}
func (noopStream) Progress(context.Context, ToolProgress)                 {}
func (noopStream) Checkpoint(ctx context.Context, _ CheckpointKind) error { return ctx.Err() }
func (noopStream) Identity() StreamIdentity                               { return StreamIdentity{} }
