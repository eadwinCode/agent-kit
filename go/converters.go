package agentkit

// Converters between internal Message/Tool types and goai provider types.
//
// The TS package converts to Vercel AI SDK types; this file is its goai
// counterpart, adjusted for three spike-verified behaviors of goai v0.9.0:
//
//  1. RoleSystem messages are silently DROPPED by goai's Anthropic provider
//     ("system handled separately") — the system prompt must be extracted
//     and passed via goai.WithSystem, and its Anthropic cache breakpoint via
//     goai.WithPromptCaching. See ConvertedMessages.System.
//  2. Reasoning signatures come back in ProviderMetadata["anthropic"]
//     ["reasoning"], not in ResponseMessages — ToSerializableResult reads
//     the metadata (the TS converter reads the AI SDK's reasoning parts).
//  3. Tool results are string-only on the wire (Part.ToolOutput) — image
//     blocks inside tool_result content are NOT supported by goai v0.9.0
//     and are stringified (known gap vs the TS multi-part vision output).
//
// The TS last-tool cache breakpoint is also dropped: goai has no per-tool
// cache_control, and Anthropic's prefix hierarchy (tools → system →
// messages) means the system breakpoint caches tool definitions anyway.

import (
	"encoding/json"
	"strings"

	"github.com/zendev-sh/goai"
	"github.com/zendev-sh/goai/provider"
)

// ConvertedMessages is the goai-ready projection of internal messages.
type ConvertedMessages struct {
	// System is the extracted system prompt (multiple system messages are
	// joined with a blank line). Pass via goai.WithSystem — goai's
	// Anthropic provider drops RoleSystem entries from Messages.
	System string
	// Messages are the non-system conversation messages.
	Messages []provider.Message
}

// MessagesToProviderMessages converts internal Message[] to goai provider
// messages, extracting the system prompt.
func MessagesToProviderMessages(msgs []Message) ConvertedMessages {
	var out ConvertedMessages
	var system []string

	for _, msg := range msgs {
		switch msg.Type {
		case MessageText:
			if msg.Role == RoleSystem {
				system = append(system, textOf(msg.Content))
				continue
			}
			out.Messages = append(out.Messages, textMessageToProviderMessage(msg))
		case MessageReasoning:
			parts := reasoningMessageToParts(msg)
			// Drop empty reasoning turns rather than emit a contentless
			// assistant message (which some providers reject).
			if len(parts) > 0 {
				out.Messages = append(out.Messages, provider.Message{
					Role: provider.RoleAssistant, Content: parts,
				})
			}
		case MessageToolCall:
			parts := make([]provider.Part, 0, len(msg.Tools))
			for _, t := range msg.Tools {
				parts = append(parts, provider.Part{
					Type:       provider.PartToolCall,
					ToolCallID: t.ID,
					ToolName:   t.Name,
					ToolInput:  t.Input,
				})
			}
			out.Messages = append(out.Messages, provider.Message{
				Role: provider.RoleAssistant, Content: parts,
			})
		case MessageToolResult:
			if msg.Tool == nil {
				continue
			}
			out.Messages = append(out.Messages, provider.Message{
				Role: provider.RoleTool,
				Content: []provider.Part{{
					Type:       provider.PartToolResult,
					ToolCallID: msg.Tool.ID,
					ToolName:   msg.Tool.Name,
					ToolOutput: toolResultOutput(msg.Content),
				}},
			})
		}
	}

	out.System = strings.Join(system, "\n\n")
	return out
}

// textMessageToProviderMessage converts a text message. User messages with
// image parts emit structured parts so vision models can see them;
// everything else collapses to a single joined text part.
func textMessageToProviderMessage(msg Message) provider.Message {
	if parts, ok := msg.Content.AsParts(); ok && msg.Role == RoleUser && hasImagePart(parts) {
		pparts := make([]provider.Part, 0, len(parts))
		for _, p := range parts {
			if p.Type == "image" {
				pparts = append(pparts, imageContentToPart(p))
			} else {
				pparts = append(pparts, provider.Part{Type: provider.PartText, Text: p.Text})
			}
		}
		return provider.Message{Role: provider.RoleUser, Content: pparts}
	}
	return provider.Message{
		Role:    provider.Role(msg.Role),
		Content: []provider.Part{{Type: provider.PartText, Text: textOf(msg.Content)}},
	}
}

func hasImagePart(parts []ContentPart) bool {
	for _, p := range parts {
		if p.Type == "image" {
			return true
		}
	}
	return false
}

// imageContentToPart maps an internal image part to goai's URL-based image
// part. goai expects a data: or http(s) URL; raw base64 is wrapped into a
// data URL using MimeType (default image/png, matching the TS converter's
// fallback).
func imageContentToPart(c ContentPart) provider.Part {
	url := c.Image
	if !strings.HasPrefix(url, "data:") && !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		mime := c.MimeType
		if mime == "" {
			mime = "image/png"
		}
		url = "data:" + mime + ";base64," + c.Image
	}
	return provider.Part{Type: provider.PartImage, URL: url, MediaType: c.MimeType}
}

// reasoningMessageToParts converts a reasoning message back into provider
// reasoning parts. Prefers the structured Details (which preserve per-block
// signatures Anthropic needs to replay a thinking block before a tool-use
// block); falls back to the flat text. Signatures travel in
// ProviderOptions["signature"], redacted blocks in
// ProviderOptions["redactedData"] — the exact keys goai's Anthropic
// provider replays (anthropic.go:606,616).
func reasoningMessageToParts(msg Message) []provider.Part {
	if len(msg.Details) > 0 {
		parts := make([]provider.Part, 0, len(msg.Details))
		for _, d := range msg.Details {
			switch d.Type {
			case "redacted":
				parts = append(parts, provider.Part{
					Type:            provider.PartReasoning,
					ProviderOptions: map[string]any{"redactedData": d.Data},
				})
			default:
				p := provider.Part{Type: provider.PartReasoning, Text: d.Text}
				if d.Signature != "" {
					p.ProviderOptions = map[string]any{"signature": d.Signature}
				}
				parts = append(parts, p)
			}
		}
		return parts
	}
	if text, _ := msg.Content.AsString(); text != "" {
		p := provider.Part{Type: provider.PartReasoning, Text: text}
		if msg.Signature != "" {
			p.ProviderOptions = map[string]any{"signature": msg.Signature}
		}
		return []provider.Part{p}
	}
	return nil
}

// toolResultOutput renders tool-result content for the wire. Strings pass
// through; everything else is compact JSON (the TS "json" output type).
// Image-carrying arrays are stringified too — goai's string-only tool
// results cannot carry image blocks (known gap, see package doc).
func toolResultOutput(c MessageContent) string {
	if s, ok := c.AsString(); ok {
		return s
	}
	return string(c.rawOrNull())
}

func textOf(c MessageContent) string {
	if s, ok := c.AsString(); ok {
		return s
	}
	if parts, ok := c.AsParts(); ok {
		var b strings.Builder
		for _, p := range parts {
			if p.Type == "text" {
				b.WriteString(p.Text)
			}
		}
		return b.String()
	}
	return ""
}

// emptyObjectSchema mirrors the TS fallback for tools without parameters.
var emptyObjectSchema = json.RawMessage(`{"type":"object","properties":{}}`)

// ToolsToProviderTools converts tool definitions to goai tools.
//
// Execute is deliberately nil on every tool: goai then skips its auto tool
// loop entirely (generate.go:1342) and returns tool calls unexecuted, so
// AgentKit's own loop runs each tool inside its own durable step. Setting
// Execute here would not just break durability — goai's recover would
// swallow inngestgo's ControlHijack panic if a tool suspended (see the
// durable package doc).
func ToolsToProviderTools(defs []ToolDef) []goai.Tool {
	out := make([]goai.Tool, 0, len(defs))
	for _, d := range defs {
		schema := d.InputSchema
		if len(schema) == 0 {
			schema = emptyObjectSchema
		}
		out = append(out, goai.Tool{
			Name:        d.Name,
			Description: d.Description,
			InputSchema: schema,
			// Execute: nil — AgentKit owns tool execution.
		})
	}
	return out
}

// MapToolChoice maps AgentKit's tool choice ("auto" | "any" | tool name) to
// goai's vocabulary ("auto" | "required" | tool name).
func MapToolChoice(choice string) string {
	switch choice {
	case "", "auto":
		return goai.ToolChoiceAuto
	case "any":
		return goai.ToolChoiceRequired
	default:
		return choice
	}
}

// SerializableResult is the serializable subset of a generation result that
// survives the durable step and feeds AgentResult.Raw. The JSON shape is
// the TS SerializableResult — Clevix's parseUsageFromRaw reads
// raw.usage's snake_case keys.
type SerializableResult struct {
	Text      string                 `json:"text"`
	ToolCalls []SerializableToolCall `json:"toolCalls"`
	// FinishReason uses goai's vocabulary, which matches the AI SDK's
	// ("stop", "tool-calls", "length", ...).
	FinishReason string `json:"finishReason"`
	// Usage is nil when the provider reported nothing.
	Usage *SerializableUsage `json:"usage,omitempty"`
	// Reasoning is the concatenated chain-of-thought text, when exposed.
	Reasoning string `json:"reasoning,omitempty"`
	// ReasoningDetails preserves structured blocks and signatures for
	// provider round-tripping.
	ReasoningDetails []ReasoningDetail `json:"reasoningDetails,omitempty"`
}

// SerializableToolCall mirrors the TS shape ({toolCallId, toolName, args}).
type SerializableToolCall struct {
	ToolCallID string          `json:"toolCallId"`
	ToolName   string          `json:"toolName"`
	Args       json.RawMessage `json:"args"`
}

// SerializableUsage mirrors the Anthropic API's field names. input_tokens
// is the NON-cache prompt count — goai's Usage.InputTokens maps the raw
// Anthropic input_tokens, which is already cache-exclusive (spike-verified),
// so no subtraction is needed (the TS v6 converter had to back it out of a
// cache-inclusive total).
//
// Divergence from TS: the cache fields are always present (0 when the
// provider has no cache concept) rather than conditionally omitted; and
// total_tokens is goai's input+output (cache-exclusive input) rather than
// the AI SDK's cache-inclusive total. Clevix billing reads input + the two
// cache buckets and is unaffected by either.
type SerializableUsage struct {
	InputTokens              int `json:"input_tokens"`
	OutputTokens             int `json:"output_tokens"`
	TotalTokens              int `json:"total_tokens"`
	CacheCreationInputTokens int `json:"cache_creation_input_tokens"`
	CacheReadInputTokens     int `json:"cache_read_input_tokens"`
}

// ToSerializableResult projects a goai result down to the serializable
// subset. Reasoning details come from ProviderMetadata["anthropic"]
// ["reasoning"] — the ONLY place goai's non-streaming path surfaces
// thinking signatures (spike finding; ResponseMessages omits them).
func ToSerializableResult(res *goai.TextResult) SerializableResult {
	out := SerializableResult{
		Text:         res.Text,
		ToolCalls:    make([]SerializableToolCall, 0, len(res.ToolCalls)),
		FinishReason: string(res.FinishReason),
	}
	for _, tc := range res.ToolCalls {
		out.ToolCalls = append(out.ToolCalls, SerializableToolCall{
			ToolCallID: tc.ID,
			ToolName:   tc.Name,
			Args:       compactRaw(tc.Input),
		})
	}

	if res.TotalUsage != (provider.Usage{}) {
		out.Usage = &SerializableUsage{
			InputTokens:              res.TotalUsage.InputTokens,
			OutputTokens:             res.TotalUsage.OutputTokens,
			TotalTokens:              res.TotalUsage.TotalTokens,
			CacheCreationInputTokens: res.TotalUsage.CacheWriteTokens,
			CacheReadInputTokens:     res.TotalUsage.CacheReadTokens,
		}
	}

	if strings.TrimSpace(res.Reasoning) != "" {
		out.Reasoning = res.Reasoning
	}
	out.ReasoningDetails = reasoningDetailsFromMetadata(res.ProviderMetadata)
	return out
}

// toSerializableStreamResult projects a completed StreamText result without
// leaking GoAI's historical Text behavior, where Text contains both reasoning
// and answer tokens. Step text is the text-only equivalent of GenerateText's
// Text. Streaming reasoning metadata is recovered from ResponseMessages,
// because providers such as Anthropic deliver signatures on reasoning chunks.
func toSerializableStreamResult(res *goai.TextResult) SerializableResult {
	out := ToSerializableResult(res)

	var text strings.Builder
	for _, step := range res.Steps {
		text.WriteString(step.Text)
	}
	if len(res.Steps) > 0 {
		out.Text = text.String()
	} else if responseText, ok := textFromResponseMessages(res.ResponseMessages); ok {
		out.Text = responseText
	}

	if details := reasoningDetailsFromResponseMessages(res.ResponseMessages); len(details) > 0 {
		out.ReasoningDetails = details
	}
	return out
}

func textFromResponseMessages(messages []provider.Message) (string, bool) {
	var text strings.Builder
	found := false
	for _, message := range messages {
		if message.Role != provider.RoleAssistant {
			continue
		}
		for _, part := range message.Content {
			if part.Type == provider.PartText {
				found = true
				text.WriteString(part.Text)
			}
		}
	}
	return text.String(), found
}

func reasoningDetailsFromResponseMessages(messages []provider.Message) []ReasoningDetail {
	var details []ReasoningDetail
	for _, message := range messages {
		if message.Role != provider.RoleAssistant {
			continue
		}
		for _, part := range message.Content {
			if part.Type != provider.PartReasoning {
				continue
			}
			if data, _ := part.ProviderOptions["redactedData"].(string); data != "" {
				details = append(details, ReasoningDetail{Type: "redacted", Data: data})
				continue
			}
			signature, _ := part.ProviderOptions["signature"].(string)
			if signature != "" {
				details = append(details, ReasoningDetail{Type: "text", Text: part.Text, Signature: signature})
			}
		}
	}
	return details
}

// reasoningDetailsFromMetadata extracts thinking blocks (with signatures)
// from provider metadata. Handles both the in-process shape the Anthropic
// provider constructs ([]map[string]any) and a JSON-round-tripped []any.
func reasoningDetailsFromMetadata(meta map[string]map[string]any) []ReasoningDetail {
	am, ok := meta["anthropic"]
	if !ok {
		return nil
	}
	var entries []map[string]any
	switch v := am["reasoning"].(type) {
	case []map[string]any:
		entries = v
	case []any:
		for _, e := range v {
			if m, ok := e.(map[string]any); ok {
				entries = append(entries, m)
			}
		}
	default:
		return nil
	}

	details := make([]ReasoningDetail, 0, len(entries))
	for _, e := range entries {
		typ, _ := e["type"].(string)
		switch typ {
		case "redacted_thinking":
			data, _ := e["data"].(string)
			details = append(details, ReasoningDetail{Type: "redacted", Data: data})
		default: // "thinking"
			text, _ := e["text"].(string)
			sig, _ := e["signature"].(string)
			details = append(details, ReasoningDetail{Type: "text", Text: text, Signature: sig})
		}
	}
	return details
}

// ResultToMessages converts a serializable result to internal messages.
// Construction mirrors the TS resultToMessages exactly (reasoning first —
// Anthropic requires the thinking block to precede tool-use — then text,
// then tool calls, with an empty-text fallback), so fresh results serialize
// byte-identically across runtimes.
func ResultToMessages(r SerializableResult) []Message {
	var messages []Message
	hasToolCalls := len(r.ToolCalls) > 0

	if reasoning, ok := reasoningResultToMessage(r, hasToolCalls); ok {
		messages = append(messages, reasoning)
	}

	if strings.TrimSpace(r.Text) != "" {
		stop := StopStop
		if hasToolCalls {
			stop = StopTool
		}
		messages = append(messages, Message{
			Type: MessageText, Role: RoleAssistant,
			Content: TextContent(r.Text), StopReason: stop,
		})
	}

	if hasToolCalls {
		tools := make([]ToolMessage, 0, len(r.ToolCalls))
		for _, tc := range r.ToolCalls {
			tools = append(tools, NewToolMessage(tc.ToolCallID, tc.ToolName, tc.Args))
		}
		messages = append(messages, Message{
			Type: MessageToolCall, Role: RoleAssistant,
			StopReason: StopTool, Tools: tools,
		})
	}

	if len(messages) == 0 {
		messages = append(messages, Message{
			Type: MessageText, Role: RoleAssistant,
			Content: TextContent(""), StopReason: StopStop,
		})
	}
	return messages
}

func reasoningResultToMessage(r SerializableResult, hasToolCalls bool) (Message, bool) {
	details := r.ReasoningDetails
	text := r.Reasoning
	if text == "" {
		var b strings.Builder
		for _, d := range details {
			if d.Type == "text" {
				b.WriteString(d.Text)
			}
		}
		text = b.String()
	}

	if strings.TrimSpace(text) == "" && len(details) == 0 {
		return Message{}, false
	}

	signature := ""
	for _, d := range details {
		if d.Type == "text" && d.Signature != "" {
			signature = d.Signature
			break
		}
	}

	stop := StopStop
	if hasToolCalls {
		stop = StopTool
	}
	msg := Message{
		Type: MessageReasoning, Role: RoleAssistant,
		Content: TextContent(text), StopReason: stop,
	}
	if len(details) > 0 {
		msg.Details = details
	}
	msg.Signature = signature
	return msg, true
}
