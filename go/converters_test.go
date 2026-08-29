package agentkit

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/zendev-sh/goai"
	"github.com/zendev-sh/goai/provider"

	"github.com/eadwinCode/agent-kit/go/internal/jsonutil"
)

// fixtureSerializableResult reproduces the inference result that the
// fixture emitter's reasoning_signed + text_assistant_tool + tool_call trio
// was constructed from.
func fixtureSerializableResult() SerializableResult {
	return SerializableResult{
		Text:         "Let me check the weather 🌍 in São Paulo…",
		FinishReason: "tool-calls",
		ToolCalls: []SerializableToolCall{{
			ToolCallID: "toolu_01ABC",
			ToolName:   "edit_file",
			Args:       json.RawMessage(`{"path":"src/<main>.ts","find":"a & b","replace":"a && b","allOccurrences":true,"limit":3}`),
		}},
		Reasoning: "The user wants weather data — I should call the tool. Note: 1 < 2.",
		ReasoningDetails: []ReasoningDetail{{
			Type:      "text",
			Text:      "The user wants weather data — I should call the tool. Note: 1 < 2.",
			Signature: "EqQBCkYIBRgCIkDBaK3clevixSig+/f8wPCJ7wE=",
		}},
	}
}

// TestResultToMessagesMatchesTSFixtures is the cross-runtime keystone: the
// same logical inference result must produce byte-identical messages from
// Go's ResultToMessages and TS's resultToMessages (captured in fixtures).
func TestResultToMessagesMatchesTSFixtures(t *testing.T) {
	f := loadFixtures(t)
	want := map[string]string{}
	for _, c := range f.Cases {
		want[c.Name] = c.JSON
	}

	msgs := ResultToMessages(fixtureSerializableResult())
	if len(msgs) != 3 {
		t.Fatalf("want reasoning+text+tool_call, got %d messages", len(msgs))
	}
	for i, name := range []string{"reasoning_signed", "text_assistant_tool", "tool_call"} {
		got, err := jsonutil.Marshal(msgs[i])
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != want[name] {
			t.Errorf("%s:\n ts: %s\n go: %s", name, want[name], got)
		}
	}
}

func TestResultToMessagesEmptyFallback(t *testing.T) {
	msgs := ResultToMessages(SerializableResult{FinishReason: "stop"})
	if len(msgs) != 1 || msgs[0].Type != MessageText {
		t.Fatalf("want single empty text message, got %+v", msgs)
	}
	if s, _ := msgs[0].Content.AsString(); s != "" || msgs[0].StopReason != StopStop {
		t.Errorf("empty fallback wrong: %+v", msgs[0])
	}
}

func TestMessagesToProviderMessagesSystemExtraction(t *testing.T) {
	conv := MessagesToProviderMessages([]Message{
		{Type: MessageText, Role: RoleSystem, Content: TextContent("You are terse.")},
		{Type: MessageText, Role: RoleSystem, Content: TextContent("Answer in French.")},
		{Type: MessageText, Role: RoleUser, Content: TextContent("hi")},
	})
	if conv.System != "You are terse.\n\nAnswer in French." {
		t.Errorf("system join wrong: %q", conv.System)
	}
	if len(conv.Messages) != 1 || conv.Messages[0].Role != provider.RoleUser {
		t.Errorf("system leaked into messages: %+v", conv.Messages)
	}
}

func TestMessagesToProviderMessagesVision(t *testing.T) {
	parts, err := PartsContent([]ContentPart{
		{Type: "text", Text: "What is this?"},
		{Type: "image", Image: "data:image/png;base64,AAA="},
		{Type: "image", Image: "aGVsbG8=", MimeType: "image/jpeg"},
		{Type: "image", Image: "https://example.com/x.png"},
	})
	if err != nil {
		t.Fatal(err)
	}
	conv := MessagesToProviderMessages([]Message{{Type: MessageText, Role: RoleUser, Content: parts}})
	got := conv.Messages[0].Content
	if len(got) != 4 {
		t.Fatalf("want 4 parts, got %d", len(got))
	}
	if got[1].URL != "data:image/png;base64,AAA=" {
		t.Errorf("data URL must pass through: %q", got[1].URL)
	}
	if got[2].URL != "data:image/jpeg;base64,aGVsbG8=" {
		t.Errorf("raw base64 must wrap into a data URL: %q", got[2].URL)
	}
	if got[3].URL != "https://example.com/x.png" {
		t.Errorf("http URL must pass through: %q", got[3].URL)
	}

	// Non-user parts collapse to joined text (prior TS behaviour).
	conv2 := MessagesToProviderMessages([]Message{{Type: MessageText, Role: RoleAssistant, Content: parts}})
	if len(conv2.Messages[0].Content) != 1 || conv2.Messages[0].Content[0].Text != "What is this?" {
		t.Errorf("assistant parts should collapse to text: %+v", conv2.Messages[0].Content)
	}
}

func TestMessagesToProviderMessagesReasoningRoundTrip(t *testing.T) {
	msg := Message{
		Type: MessageReasoning, Role: RoleAssistant,
		Content: TextContent("thinking..."),
		Details: []ReasoningDetail{
			{Type: "text", Text: "thinking...", Signature: "sig123"},
			{Type: "redacted", Data: "opaque=="},
			{Type: "text", Text: "unsigned block"},
		},
	}
	conv := MessagesToProviderMessages([]Message{msg})
	parts := conv.Messages[0].Content
	if len(parts) != 3 {
		t.Fatalf("want 3 reasoning parts, got %d", len(parts))
	}
	if sig, _ := parts[0].ProviderOptions["signature"].(string); sig != "sig123" {
		t.Errorf("signature must ride ProviderOptions: %+v", parts[0])
	}
	if data, _ := parts[1].ProviderOptions["redactedData"].(string); data != "opaque==" {
		t.Errorf("redacted data must ride ProviderOptions[redactedData]: %+v", parts[1])
	}
	if parts[2].ProviderOptions != nil {
		t.Errorf("unsigned block must carry no options: %+v", parts[2])
	}

	// Fallback: no details → single part from content + signature.
	flat := Message{Type: MessageReasoning, Role: RoleAssistant, Content: TextContent("t"), Signature: "s"}
	fp := MessagesToProviderMessages([]Message{flat}).Messages[0].Content
	if len(fp) != 1 || fp[0].ProviderOptions["signature"] != "s" {
		t.Errorf("flat fallback wrong: %+v", fp)
	}

	// Empty reasoning turns are dropped entirely.
	empty := Message{Type: MessageReasoning, Role: RoleAssistant, Content: TextContent("")}
	if n := len(MessagesToProviderMessages([]Message{empty}).Messages); n != 0 {
		t.Errorf("empty reasoning must be dropped, got %d messages", n)
	}
}

func TestMessagesToProviderMessagesToolFlow(t *testing.T) {
	input := json.RawMessage(`{"city":"Tokyo"}`)
	conv := MessagesToProviderMessages([]Message{
		{Type: MessageToolCall, Role: RoleAssistant, StopReason: StopTool,
			Tools: []ToolMessage{NewToolMessage("t1", "get_weather", input)}},
		{Type: MessageToolResult, Role: RoleToolResult, StopReason: StopTool,
			Tool:    &ToolMessage{Type: "tool", ID: "t1", Name: "get_weather", Input: input},
			Content: RawContent(json.RawMessage(`{"data":{"temp":21}}`))},
		{Type: MessageToolResult, Role: RoleToolResult, StopReason: StopTool,
			Tool:    &ToolMessage{Type: "tool", ID: "t2", Name: "say", Input: input},
			Content: TextContent("plain string result")},
	})
	if len(conv.Messages) != 3 {
		t.Fatalf("want 3 messages, got %d", len(conv.Messages))
	}
	call := conv.Messages[0].Content[0]
	if call.Type != provider.PartToolCall || call.ToolCallID != "t1" || string(call.ToolInput) != `{"city":"Tokyo"}` {
		t.Errorf("tool call part wrong: %+v", call)
	}
	res := conv.Messages[1].Content[0]
	if res.Type != provider.PartToolResult || res.ToolOutput != `{"data":{"temp":21}}` {
		t.Errorf("json tool result must stringify verbatim: %+v", res)
	}
	if conv.Messages[2].Content[0].ToolOutput != "plain string result" {
		t.Errorf("string tool result must pass through unquoted: %q", conv.Messages[2].Content[0].ToolOutput)
	}
}

func TestToolsToProviderToolsNilExecute(t *testing.T) {
	defs := []ToolDef{
		{Name: "a", Description: "with schema", InputSchema: json.RawMessage(`{"type":"object","properties":{"x":{"type":"integer"}},"required":["x"]}`)},
		{Name: "b", Description: "no schema"},
	}
	tools := ToolsToProviderTools(defs)
	for _, tl := range tools {
		if tl.Execute != nil {
			t.Fatalf("tool %s has Execute set — goai would run its own loop and swallow ControlHijack", tl.Name)
		}
	}
	if string(tools[1].InputSchema) != `{"type":"object","properties":{}}` {
		t.Errorf("schemaless tool must default to empty object schema: %s", tools[1].InputSchema)
	}
}

func TestMapToolChoice(t *testing.T) {
	for in, want := range map[string]string{
		"":            goai.ToolChoiceAuto,
		"auto":        goai.ToolChoiceAuto,
		"any":         goai.ToolChoiceRequired,
		"get_weather": "get_weather",
	} {
		if got := MapToolChoice(in); got != want {
			t.Errorf("MapToolChoice(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestToSerializableResult(t *testing.T) {
	res := &goai.TextResult{
		Text:         "done",
		FinishReason: provider.FinishToolCalls,
		ToolCalls: []provider.ToolCall{{
			ID: "toolu_1", Name: "get_weather", Input: json.RawMessage(`{"city":"Tokyo"}`),
		}},
		Reasoning: "let me think",
		TotalUsage: provider.Usage{
			InputTokens: 517, OutputTokens: 115, TotalTokens: 632,
			CacheReadTokens: 0, CacheWriteTokens: 3850,
		},
		ProviderMetadata: map[string]map[string]any{
			"anthropic": {
				"reasoning": []map[string]any{
					{"type": "thinking", "text": "let me think", "signature": "sig_abc"},
					{"type": "redacted_thinking", "data": "enc=="},
				},
			},
		},
	}
	sr := ToSerializableResult(res)

	if sr.Usage == nil || sr.Usage.InputTokens != 517 || sr.Usage.CacheCreationInputTokens != 3850 {
		t.Errorf("usage mapping wrong: %+v", sr.Usage)
	}
	if len(sr.ReasoningDetails) != 2 ||
		sr.ReasoningDetails[0].Signature != "sig_abc" ||
		sr.ReasoningDetails[1].Type != "redacted" || sr.ReasoningDetails[1].Data != "enc==" {
		t.Errorf("reasoning details from metadata wrong: %+v", sr.ReasoningDetails)
	}

	// The billing contract: raw.usage must expose these exact snake_case
	// keys (Clevix parseUsageFromRaw).
	rawJSON, err := jsonutil.Marshal(sr)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{
		`"input_tokens":517`, `"output_tokens":115`, `"total_tokens":632`,
		`"cache_creation_input_tokens":3850`, `"cache_read_input_tokens":0`,
		`"toolCallId":"toolu_1"`, `"toolName":"get_weather"`, `"finishReason":"tool-calls"`,
	} {
		if !strings.Contains(string(rawJSON), key) {
			t.Errorf("serialized raw missing %s:\n%s", key, rawJSON)
		}
	}

	// Empty usage → omitted, mirroring TS.
	if got := ToSerializableResult(&goai.TextResult{Text: "x"}); got.Usage != nil {
		t.Errorf("zero usage should be omitted, got %+v", got.Usage)
	}
}

func TestSerializableResultCarriesTheGatewayCost(t *testing.T) {
	// A gateway in front of the provider knows what a call actually cost;
	// token counts alone cannot reconstruct it, because the gateway may route
	// to different upstream providers at different prices and may add
	// surcharges. Carrying it here is what makes it survive a durable replay.
	res := &goai.TextResult{
		Text:         "ok",
		FinishReason: provider.FinishStop,
		TotalUsage:   provider.Usage{InputTokens: 10, OutputTokens: 2, TotalTokens: 12},
		ProviderMetadata: map[string]map[string]any{
			"gateway": {
				"cost":         "0.00434166",
				"generationId": "gen_01M14YC4JFBAYRHBGKHFC5KP5A",
			},
		},
	}
	sr := ToSerializableResult(res)

	if sr.ProviderMetadata == nil || sr.ProviderMetadata.Gateway == nil {
		t.Fatalf("gateway metadata was dropped: %+v", sr.ProviderMetadata)
	}
	if got := sr.ProviderMetadata.Gateway.Cost; got != "0.00434166" {
		t.Errorf("cost = %q, want the value carried through verbatim", got)
	}
	if got := sr.ProviderMetadata.Gateway.GenerationID; got != "gen_01M14YC4JFBAYRHBGKHFC5KP5A" {
		t.Errorf("generation id = %q, want it carried", got)
	}

	// The consumer reads this off raw JSON, so the shape is the contract.
	rawJSON, err := jsonutil.Marshal(sr)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		ProviderMetadata struct {
			Gateway struct {
				Cost         string `json:"cost"`
				GenerationID string `json:"generationId"`
			} `json:"gateway"`
		} `json:"providerMetadata"`
	}
	if err := json.Unmarshal(rawJSON, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ProviderMetadata.Gateway.Cost != "0.00434166" {
		t.Errorf("raw JSON shape wrong: %s", rawJSON)
	}
}

func TestSerializableResultDropsMetadataItDoesNotModel(t *testing.T) {
	// The point of a closed struct. This value is memoized for every
	// inference and re-read on every replay, so an open map would let a
	// caller attach a provider's whole block — routing prose included —
	// and have it paid for repeatedly without anyone noticing.
	res := &goai.TextResult{
		FinishReason: provider.FinishStop,
		ProviderMetadata: map[string]map[string]any{
			"gateway": {
				"cost":          "0.001",
				"generationId":  "gen_1",
				"surchargeCost": "0",
				"marketCost":    "0.002",
				"routing": map[string]any{
					"resolvedProvider":  "openai",
					"planningReasoning": strings.Repeat("prose ", 200),
				},
			},
		},
	}
	rawJSON, err := jsonutil.Marshal(ToSerializableResult(res))
	if err != nil {
		t.Fatal(err)
	}
	for _, unwanted := range []string{"routing", "planningReasoning", "marketCost", "surchargeCost"} {
		if strings.Contains(string(rawJSON), unwanted) {
			t.Errorf("%q survived serialization; the struct must be closed:\n%s", unwanted, rawJSON)
		}
	}
	// surchargeCost is deliberately absent: it is a COMPONENT of cost, not an
	// addition to it, so carrying it invites a consumer to add it twice.
	if len(rawJSON) > 400 {
		t.Errorf("serialized result is %d bytes; the narrowing is not working", len(rawJSON))
	}
}

func TestGatewayCostAcceptsAStringOrANumber(t *testing.T) {
	// Gateways report money both ways, sometimes across two endpoints of the
	// same vendor. Typing it to one shape would mean a gateway that switched
	// silently stopped reporting cost — the least visible way to break billing.
	for name, value := range map[string]any{
		"string": "0.000137544",
		"float":  0.000137544,
		"number": json.Number("0.000137544"),
	} {
		res := &goai.TextResult{
			FinishReason:     provider.FinishStop,
			ProviderMetadata: map[string]map[string]any{"gateway": {"cost": value}},
		}
		sr := ToSerializableResult(res)
		if sr.ProviderMetadata == nil || sr.ProviderMetadata.Gateway == nil {
			t.Fatalf("%s: gateway metadata dropped", name)
		}
		// Never exponent notation: a decimal parser downstream would reject it.
		if got := sr.ProviderMetadata.Gateway.Cost; got != "0.000137544" {
			t.Errorf("%s: cost = %q, want 0.000137544", name, got)
		}
	}
}

func TestSerializableResultOmitsMetadataEntirelyWithoutAGateway(t *testing.T) {
	// A deployment talking straight to a provider must pay nothing for a
	// field it never populates.
	res := &goai.TextResult{
		FinishReason: provider.FinishStop,
		ProviderMetadata: map[string]map[string]any{
			"anthropic": {"reasoning": []map[string]any{}},
		},
	}
	sr := ToSerializableResult(res)
	if sr.ProviderMetadata != nil {
		t.Fatalf("provider metadata = %+v, want nil without a gateway", sr.ProviderMetadata)
	}
	rawJSON, err := jsonutil.Marshal(sr)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(rawJSON), "providerMetadata") {
		t.Errorf("empty metadata still emitted a key: %s", rawJSON)
	}
}

func TestGatewayCostSurvivesADurableReplay(t *testing.T) {
	// The whole reason this field exists. durable.Run memoizes the marshalled
	// SerializableResult and, on replay, unmarshals it back into the same
	// struct without calling the provider — so a field the struct does not
	// declare is silently dropped by encoding/json and the cost is lost.
	original := ToSerializableResult(&goai.TextResult{
		FinishReason: provider.FinishStop,
		TotalUsage:   provider.Usage{InputTokens: 10, OutputTokens: 2},
		ProviderMetadata: map[string]map[string]any{
			"gateway": {"cost": "0.0891031", "generationId": "gen_replay"},
		},
	})
	memoized, err := jsonutil.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var replayed SerializableResult
	if err := json.Unmarshal(memoized, &replayed); err != nil {
		t.Fatal(err)
	}
	if replayed.ProviderMetadata == nil || replayed.ProviderMetadata.Gateway == nil {
		t.Fatal("the cost did not survive the memoize/replay round trip")
	}
	if got := replayed.ProviderMetadata.Gateway.Cost; got != "0.0891031" {
		t.Errorf("replayed cost = %q, want 0.0891031", got)
	}
}
