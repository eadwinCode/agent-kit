package agentkit

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/eadwinCode/agent-kit/go/internal/jsonutil"
)

// Time is the wire-parity timestamp used on AgentResult and history
// records: it marshals in Date#toISOString() form so rows written by Go
// match those written by the TypeScript package.
//
// Exported as an alias because AgentResult and NewAgentResult are part of
// the public API — without it, code outside this module could not
// construct or persist a result.
type Time = jsonutil.Time

// Now returns the current instant as a [Time].
func Now() Time { return jsonutil.Now() }

// Role identifies a message sender. Mirrors the TS union
// "system" | "user" | "assistant" | "tool_result".
type Role string

const (
	RoleSystem     Role = "system"
	RoleUser       Role = "user"
	RoleAssistant  Role = "assistant"
	RoleToolResult Role = "tool_result"
)

// MessageType discriminates the Message union.
type MessageType string

const (
	MessageText       MessageType = "text"
	MessageReasoning  MessageType = "reasoning"
	MessageToolCall   MessageType = "tool_call"
	MessageToolResult MessageType = "tool_result"
)

// StopReason mirrors the TS "tool" | "stop" literal union.
type StopReason string

const (
	StopTool StopReason = "tool"
	StopStop StopReason = "stop"
)

// Message is the TS Message union (TextMessage | ToolCallMessage |
// ToolResultMessage | ReasoningMessage) as a single struct discriminated by
// Type. A single struct rather than an interface keeps JSON wire parity
// mechanical: MarshalJSON emits exactly the keys of the active variant, in
// the same order the TypeScript construction sites insert them (converters.ts
// resultToMessages, agent.ts invokeTools), so freshly built results serialize
// byte-identically across the two runtimes.
//
// Field applicability by Type:
//
//	text:        Role, Content (string or []ContentPart), StopReason?
//	reasoning:   Role, Content (string), StopReason?, Details?, Signature?
//	tool_call:   Role, StopReason, Tools
//	tool_result: Role, Tool, Content (arbitrary JSON), StopReason
type Message struct {
	Type MessageType
	Role Role

	// Content carries the variant's content and preserves the original raw
	// bytes across a round-trip (parity decision 12: arbitrary JSON must
	// never pass through map[string]any, which would reorder keys).
	Content MessageContent

	// Tools is the tool_call variant's calls.
	Tools []ToolMessage

	// Tool is the tool_result variant's originating call.
	Tool *ToolMessage

	StopReason StopReason

	// Signature is the primary reasoning block's signature (reasoning only).
	Signature string

	// Details are the structured reasoning blocks, preserved for provider
	// round-tripping (reasoning only).
	Details []ReasoningDetail
}

// messageShadow is the permissive decode target for all variants.
type messageShadow struct {
	Type       MessageType       `json:"type"`
	Role       Role              `json:"role"`
	Content    json.RawMessage   `json:"content"`
	Tools      []ToolMessage     `json:"tools"`
	Tool       *ToolMessage      `json:"tool"`
	StopReason StopReason        `json:"stop_reason"`
	Signature  string            `json:"signature"`
	Details    []ReasoningDetail `json:"details"`
}

func (m *Message) UnmarshalJSON(b []byte) error {
	var s messageShadow
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	*m = Message{
		Type:       s.Type,
		Role:       s.Role,
		Content:    MessageContent{raw: compactRaw(s.Content)},
		Tools:      s.Tools,
		Tool:       s.Tool,
		StopReason: s.StopReason,
		Signature:  s.Signature,
		Details:    s.Details,
	}
	return nil
}

func (m Message) MarshalJSON() ([]byte, error) {
	o := newObjWriter()
	switch m.Type {
	case MessageText:
		// Construction order: converters.ts resultToMessages / agent.ts agentPrompt.
		o.field("type", m.Type)
		o.field("role", m.Role)
		o.rawField("content", m.Content.rawOrNull())
		if m.StopReason != "" {
			o.field("stop_reason", m.StopReason)
		}
	case MessageReasoning:
		// Construction order: converters.ts reasoningResultToMessage — the
		// literal inserts type/role/content/stop_reason, then details and
		// signature are conditionally assigned afterwards.
		o.field("type", m.Type)
		o.field("role", m.Role)
		o.rawField("content", m.Content.rawOrNull())
		if m.StopReason != "" {
			o.field("stop_reason", m.StopReason)
		}
		if len(m.Details) > 0 {
			o.field("details", m.Details)
		}
		if m.Signature != "" {
			o.field("signature", m.Signature)
		}
	case MessageToolCall:
		// Construction order: converters.ts resultToMessages — note
		// stop_reason precedes tools at the construction site (the TS
		// interface declares them the other way around; insertion order is
		// what JSON.stringify emits).
		o.field("type", m.Type)
		o.field("role", m.Role)
		if m.StopReason != "" {
			o.field("stop_reason", m.StopReason)
		}
		o.field("tools", m.Tools)
	case MessageToolResult:
		// Construction order: agent.ts invokeTools — role precedes type here.
		o.field("role", m.Role)
		o.field("type", m.Type)
		o.field("tool", m.Tool)
		o.rawField("content", m.Content.rawOrNull())
		if m.StopReason != "" {
			o.field("stop_reason", m.StopReason)
		}
	default:
		return nil, fmt.Errorf("agentkit: cannot marshal message with unknown type %q", m.Type)
	}
	return o.finish()
}

// MessageContent holds a message's content while preserving the exact bytes
// it arrived with. TS types it as string | Array<TextContent|ImageContent>
// for text messages and unknown for tool results; interpreting it is the
// caller's business, keeping it byte-stable is this type's.
type MessageContent struct {
	raw json.RawMessage
}

// TextContent builds content from a plain string.
func TextContent(s string) MessageContent {
	b, err := jsonutil.Marshal(s)
	if err != nil {
		// Marshaling a string cannot fail.
		panic(err)
	}
	return MessageContent{raw: b}
}

// PartsContent builds content from rich parts (text + images).
func PartsContent(parts []ContentPart) (MessageContent, error) {
	b, err := jsonutil.Marshal(parts)
	if err != nil {
		return MessageContent{}, err
	}
	return MessageContent{raw: b}, nil
}

// RawContent wraps pre-serialized JSON (tool results, unknown payloads).
// The bytes are stored verbatim.
func RawContent(raw json.RawMessage) MessageContent {
	return MessageContent{raw: compactRaw(raw)}
}

// IsZero reports whether no content was ever set.
func (c MessageContent) IsZero() bool { return c.raw == nil }

// Raw returns the underlying JSON bytes (nil when unset).
func (c MessageContent) Raw() json.RawMessage { return c.raw }

// AsString returns the content when it is a JSON string.
func (c MessageContent) AsString() (string, bool) {
	if len(c.raw) == 0 || c.raw[0] != '"' {
		return "", false
	}
	var s string
	if err := json.Unmarshal(c.raw, &s); err != nil {
		return "", false
	}
	return s, true
}

// AsParts returns the content when it is a parts array.
func (c MessageContent) AsParts() ([]ContentPart, bool) {
	if len(c.raw) == 0 || c.raw[0] != '[' {
		return nil, false
	}
	var parts []ContentPart
	if err := json.Unmarshal(c.raw, &parts); err != nil {
		return nil, false
	}
	return parts, true
}

func (c MessageContent) rawOrNull() json.RawMessage {
	if c.raw == nil {
		return json.RawMessage("null")
	}
	return c.raw
}

func (c MessageContent) MarshalJSON() ([]byte, error) {
	return c.rawOrNull(), nil
}

func (c *MessageContent) UnmarshalJSON(b []byte) error {
	c.raw = compactRaw(b)
	return nil
}

// ContentPart is the TS TextContent | ImageContent union: a rich content
// element inside a text message.
type ContentPart struct {
	// Type is "text" or "image".
	Type string
	// Text is the text part's content.
	Text string
	// Image is an http(s)/data URL or raw base64 string.
	Image string
	// MimeType accompanies raw base64 images.
	MimeType string
}

func (p ContentPart) MarshalJSON() ([]byte, error) {
	o := newObjWriter()
	switch p.Type {
	case "text":
		o.field("type", p.Type)
		o.field("text", p.Text) // always present, even when empty
	case "image":
		o.field("type", p.Type)
		o.field("image", p.Image)
		if p.MimeType != "" {
			o.field("mimeType", p.MimeType)
		}
	default:
		return nil, fmt.Errorf("agentkit: cannot marshal content part with unknown type %q", p.Type)
	}
	return o.finish()
}

func (p *ContentPart) UnmarshalJSON(b []byte) error {
	var s struct {
		Type     string `json:"type"`
		Text     string `json:"text"`
		Image    string `json:"image"`
		MimeType string `json:"mimeType"`
	}
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	*p = ContentPart(s)
	return nil
}

// ReasoningDetail mirrors the TS union: a thinking block (text, optionally
// signed) or a redacted thinking block (opaque data).
type ReasoningDetail struct {
	// Type is "text" or "redacted".
	Type string
	// Text is the thinking text (type "text").
	Text string
	// Signature signs the thinking block (type "text", optional).
	Signature string
	// Data is the encrypted payload (type "redacted").
	Data string
}

func (d ReasoningDetail) MarshalJSON() ([]byte, error) {
	o := newObjWriter()
	switch d.Type {
	case "text":
		o.field("type", d.Type)
		o.field("text", d.Text) // always present, even when empty
		if d.Signature != "" {
			o.field("signature", d.Signature)
		}
	case "redacted":
		o.field("type", d.Type)
		o.field("data", d.Data)
	default:
		return nil, fmt.Errorf("agentkit: cannot marshal reasoning detail with unknown type %q", d.Type)
	}
	return o.finish()
}

func (d *ReasoningDetail) UnmarshalJSON(b []byte) error {
	var s struct {
		Type      string `json:"type"`
		Text      string `json:"text"`
		Signature string `json:"signature"`
		Data      string `json:"data"`
	}
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	*d = ReasoningDetail(s)
	return nil
}

// ToolMessage is a tool call request: {type:"tool", id, name, input}.
// Input stays json.RawMessage end-to-end so provider-supplied argument
// objects keep their key order (parity decision 12).
type ToolMessage struct {
	Type  string          `json:"type"` // always "tool"
	ID    string          `json:"id"`
	Name  string          `json:"name"`
	Input json.RawMessage `json:"input"`
}

// NewToolMessage builds a ToolMessage with the fixed "tool" type.
func NewToolMessage(id, name string, input json.RawMessage) ToolMessage {
	return ToolMessage{Type: "tool", ID: id, Name: name, Input: compactRaw(input)}
}

// UserMessage is a rich client message: content plus client-side state,
// timestamps and an optional per-turn system prompt.
type UserMessage struct {
	// ID is the canonical, client-generated unique identifier.
	ID string `json:"id"`
	// Content is the text of the user's message.
	Content string `json:"content"`
	// Role is always "user".
	Role Role `json:"role"`
	// State is an optional client-provided snapshot, passed through verbatim.
	State json.RawMessage `json:"state,omitempty"`
	// ClientTimestamp orders optimistic UI updates (Date | string in TS;
	// both serialize to a string).
	ClientTimestamp string `json:"clientTimestamp,omitempty"`
	// SystemPrompt is a one-time system prompt for this turn.
	SystemPrompt string `json:"systemPrompt,omitempty"`
}

// AgentResult is a single iteration of an agent call: the parsed output, any
// tool results, and tracking metadata. See the TS types.ts doc comment for
// how networks assemble chat history from a stack of these.
//
// Field order mirrors the TS constructor assignment order so a memoized
// AgentResult serializes identically from either runtime.
type AgentResult struct {
	AgentName string        `json:"agentName"`
	Output    []Message     `json:"output"`
	ToolCalls []Message     `json:"toolCalls"`
	CreatedAt jsonutil.Time `json:"createdAt"`
	// Prompt is the input instructions without history — tracking/debugging
	// only, never used to build future calls.
	Prompt []Message `json:"prompt,omitempty"`
	// History is what was appended to the prompt for the inference call —
	// tracking/debugging only.
	History []Message `json:"history,omitempty"`
	// Raw is the raw API response as a JSON string (SerializableResult).
	Raw string `json:"raw,omitempty"`
	// ID is the durable identifier used for persistence and streaming.
	ID string `json:"id,omitempty"`

	// Checksum memo. Mirrors TS: the memo is invalidated when ID changes,
	// because ID is assigned AFTER construction (network.run sets the
	// durable agentMessageId) and an early read must not pin a stale hash.
	// Not synchronized: an AgentResult belongs to one run's goroutine.
	checksum    string
	checksumID  string
	checksumSet bool
}

// NewAgentResult constructs the required fields; optional fields are set
// directly on the struct.
func NewAgentResult(agentName string, output, toolCalls []Message, createdAt jsonutil.Time) *AgentResult {
	return &AgentResult{
		AgentName: agentName,
		Output:    output,
		ToolCalls: toolCalls,
		CreatedAt: createdAt,
	}
}

// Checksum is a replay-stable unique id for this result: the xxhash64 of
// the serialized output+toolCalls plus the durably minted ID, rendered in
// decimal exactly like xxhashjs.h64(s,0).toString().
//
// CreatedAt is deliberately excluded — it is re-stamped on every Inngest
// replay, and including it made the same logical result hash differently
// between the incremental history append and the end-of-run backstop (TS
// fix 00ff5e8). Output/ToolCalls come from memoized steps and ID from a
// durable id-generation step, so this input is replay-stable.
func (r *AgentResult) Checksum() (string, error) {
	if r.checksumSet && r.checksumID == r.ID {
		return r.checksum, nil
	}
	combined := make([]Message, 0, len(r.Output)+len(r.ToolCalls))
	combined = append(combined, r.Output...)
	combined = append(combined, r.ToolCalls...)
	b, err := jsonutil.Marshal(combined)
	if err != nil {
		return "", fmt.Errorf("agentkit: serialize AgentResult for checksum: %w", err)
	}
	r.checksum = checksumOf(string(b) + r.ID)
	r.checksumID = r.ID
	r.checksumSet = true
	return r.checksum, nil
}

// AgentResultExport is the persistence shape returned by Export — the TS
// export() object, checksum included.
type AgentResultExport struct {
	AgentName string        `json:"agentName"`
	Output    []Message     `json:"output"`
	ToolCalls []Message     `json:"toolCalls"`
	CreatedAt jsonutil.Time `json:"createdAt"`
	Checksum  string        `json:"checksum"`
}

// Export returns the fields to store for future use.
func (r *AgentResult) Export() (AgentResultExport, error) {
	sum, err := r.Checksum()
	if err != nil {
		return AgentResultExport{}, err
	}
	return AgentResultExport{
		AgentName: r.AgentName,
		Output:    r.Output,
		ToolCalls: r.ToolCalls,
		CreatedAt: r.CreatedAt,
		Checksum:  sum,
	}, nil
}

// --- internal helpers ---

// compactRaw strips insignificant whitespace so stored bytes are stable
// regardless of source formatting. Key order and string escaping are
// untouched. Invalid or empty input is returned as-is (surfaced on marshal).
func compactRaw(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return raw
	}
	return append(json.RawMessage(nil), buf.Bytes()...)
}

// objWriter emits a JSON object with explicit key order, using the parity
// encoder for values.
type objWriter struct {
	buf   bytes.Buffer
	first bool
	err   error
}

func newObjWriter() *objWriter {
	o := &objWriter{first: true}
	o.buf.WriteByte('{')
	return o
}

func (o *objWriter) sep(name string) {
	if !o.first {
		o.buf.WriteByte(',')
	}
	o.first = false
	o.buf.WriteByte('"')
	o.buf.WriteString(name)
	o.buf.WriteString(`":`)
}

func (o *objWriter) field(name string, v any) {
	if o.err != nil {
		return
	}
	b, err := jsonutil.Marshal(v)
	if err != nil {
		o.err = err
		return
	}
	o.sep(name)
	o.buf.Write(b)
}

func (o *objWriter) rawField(name string, raw json.RawMessage) {
	if o.err != nil {
		return
	}
	o.sep(name)
	o.buf.Write(raw)
}

func (o *objWriter) finish() ([]byte, error) {
	if o.err != nil {
		return nil, o.err
	}
	o.buf.WriteByte('}')
	return o.buf.Bytes(), nil
}
