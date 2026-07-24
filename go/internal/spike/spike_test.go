// Package spike holds the phase-1 live-API risk spikes from the port plan.
// They verify, against a real Anthropic model, the three goai behaviors the
// entire port depends on and which were otherwise only verified by reading
// goai v0.9.0 source:
//
//  1. Tools with nil Execute are passed through: goai skips its auto tool
//     loop (generate.go:1342) and returns tool calls unexecuted, so AgentKit
//     owns the tool loop and each tool can run in its own durable step.
//  2. Reasoning signatures round-trip: replaying ResponseMessages (thinking
//     block + signature before tool_use) into a follow-up call is accepted
//     by the API — the exact behavior types.ts documents as mandatory.
//  3. Anthropic cache_control works and cache tokens surface in Usage, which
//     Clevix's billing (parseUsageFromRaw) depends on.
//
// Run with: ANTHROPIC_API_KEY=... go test ./internal/spike/ -v
// Skipped when no key is set. Costs a few cents (two small calls).
package spike

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/zendev-sh/goai"
	"github.com/zendev-sh/goai/provider"
	"github.com/zendev-sh/goai/provider/anthropic"
)

const spikeModel = "claude-sonnet-4-5"

func thinkingOpts() goai.Option {
	return goai.WithProviderOptions(map[string]any{
		// Key names follow the Vercel AI SDK convention (camelCase), same as
		// Clevix's defaultSettingsMiddleware: goai maps budgetTokens →
		// budget_tokens on the wire.
		"thinking": map[string]any{"type": "enabled", "budgetTokens": 1024},
	})
}

func TestAnthropicSpike(t *testing.T) {
	key := os.Getenv("ANTHROPIC_API_KEY")
	if key == "" {
		t.Skip("ANTHROPIC_API_KEY not set; skipping live spike")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	model := anthropic.Chat(spikeModel, anthropic.WithAPIKey(key))

	// System prompt long enough to clear Anthropic's minimum cacheable
	// prompt length (1024 tokens for Sonnet models).
	longSystem := "You are a precise weather assistant. Always use the provided tool. " +
		strings.Repeat("Background context for caching purposes: AgentKit is a framework for durable AI agents built on Inngest. ", 150)

	weather := goai.Tool{
		Name:        "get_weather",
		Description: "Get the current weather for a city.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}`),
		// Execute deliberately nil: AgentKit owns tool execution. Spike
		// assertion #1 is that goai returns the call unexecuted.
	}

	// SPIKE FINDING (feeds converters.go): the Anthropic provider silently
	// DROPS RoleSystem messages from the messages slice ("system handled
	// separately", anthropic.go:569) — the system prompt only reaches the
	// API via WithSystem, and its cache breakpoint only via
	// WithPromptCaching(true). Converters must extract the system message
	// into WithSystem and map cacheControl → WithPromptCaching, never emit
	// RoleSystem into Messages. (Anthropic's cache prefix is tools → system
	// → messages, so the system breakpoint also covers tool definitions;
	// the TS last-tool breakpoint has no goai equivalent and is dropped.)
	msgs := []provider.Message{
		{Role: provider.RoleUser, Content: []provider.Part{{
			Type: provider.PartText, Text: "What's the weather in Tokyo right now? Use the get_weather tool.",
		}}},
	}

	res, err := goai.GenerateText(ctx, model,
		goai.WithSystem(longSystem),
		goai.WithPromptCaching(true),
		goai.WithMessages(msgs...),
		goai.WithTools(weather),
		goai.WithMaxOutputTokens(2048), // must exceed the thinking budget
		thinkingOpts(),
	)
	if err != nil {
		t.Fatalf("call 1 failed: %v", err)
	}

	// --- Assertion 1: single step, tool call returned unexecuted ---
	if len(res.Steps) != 1 {
		t.Errorf("expected exactly 1 generation step (no auto loop), got %d", len(res.Steps))
	}
	if len(res.ToolCalls) != 1 || res.ToolCalls[0].Name != "get_weather" {
		t.Fatalf("expected one get_weather tool call, got %+v", res.ToolCalls)
	}
	if res.FinishReason != provider.FinishToolCalls {
		t.Errorf("FinishReason = %q, want %q", res.FinishReason, provider.FinishToolCalls)
	}
	var input struct {
		City string `json:"city"`
	}
	if err := json.Unmarshal(res.ToolCalls[0].Input, &input); err != nil || input.City == "" {
		t.Errorf("tool input not parseable: %s (%v)", res.ToolCalls[0].Input, err)
	}

	// --- Assertion 2 (part 1): reasoning blocks with signatures surface ---
	//
	// NOTE (spike finding, feeds converters.go): goai's non-streaming
	// GenerateText builds ResponseMessages with reasoning=nil
	// (generate.go:1355,1438) — signatures are ONLY available in
	// ProviderMetadata["anthropic"]["reasoning"] as
	// []map[string]any{{type:"thinking", text, signature}}. The port's
	// converters must reconstruct ReasoningMessage from provider metadata,
	// not from ResponseMessages — same as the TS converters, which read
	// reasoningDetails off the AI SDK result.
	type thinkingBlock struct {
		text, signature string
	}
	var blocks []thinkingBlock
	if am, ok := res.ProviderMetadata["anthropic"]; ok {
		if entries, ok := am["reasoning"].([]map[string]any); ok {
			for _, e := range entries {
				text, _ := e["text"].(string)
				sig, _ := e["signature"].(string)
				blocks = append(blocks, thinkingBlock{text, sig})
			}
		}
	}
	if len(blocks) == 0 || blocks[0].signature == "" {
		t.Fatalf("no signed thinking block in ProviderMetadata[anthropic][reasoning]: %+v", res.ProviderMetadata)
	}
	t.Logf("signed thinking blocks: %d (sig %d chars)", len(blocks), len(blocks[0].signature))

	// --- Assertion 3 (part 1): cache write tokens surfaced ---
	if res.TotalUsage.CacheWriteTokens == 0 {
		t.Errorf("CacheWriteTokens = 0; cache_control breakpoint did not take (usage: %+v)", res.TotalUsage)
	}
	if res.TotalUsage.InputTokens == 0 || res.TotalUsage.OutputTokens == 0 {
		t.Errorf("token counts missing: %+v", res.TotalUsage)
	}

	// --- Call 2: replay thinking block + tool result ---
	//
	// Build the assistant turn the way converters.go will: reasoning parts
	// (text + signature in ProviderOptions) BEFORE the tool_use part —
	// Anthropic rejects the follow-up otherwise. goai's convertMessages
	// reads the signature from Part.ProviderOptions["signature"]
	// (anthropic.go:606) and ReorderAssistantParts enforces ordering.
	tc := res.ToolCalls[0]
	assistant := provider.Message{Role: provider.RoleAssistant}
	for _, b := range blocks {
		assistant.Content = append(assistant.Content, provider.Part{
			Type:            provider.PartReasoning,
			Text:            b.text,
			ProviderOptions: map[string]any{"signature": b.signature},
		})
	}
	assistant.Content = append(assistant.Content, provider.Part{
		Type:       provider.PartToolCall,
		ToolCallID: tc.ID,
		ToolName:   tc.Name,
		ToolInput:  tc.Input,
	})
	msgs = append(msgs, assistant)
	msgs = append(msgs, goai.ToolMessage(tc.ID, tc.Name, `{"temperature_c":21,"conditions":"sunny"}`))

	res2, err := goai.GenerateText(ctx, model,
		goai.WithSystem(longSystem),
		goai.WithPromptCaching(true),
		goai.WithMessages(msgs...),
		goai.WithTools(weather),
		goai.WithMaxOutputTokens(2048),
		thinkingOpts(),
	)
	// --- Assertion 2 (part 2): the API accepted the replayed signed
	// thinking block before the tool_use block. Anthropic rejects the
	// request outright when the signature is missing or misordered.
	if err != nil {
		t.Fatalf("call 2 rejected — reasoning signature round-trip failed: %v", err)
	}
	if strings.TrimSpace(res2.Text) == "" {
		t.Error("call 2 returned empty text")
	}

	// --- Assertion 3 (part 2): second call reads the cache ---
	if res2.TotalUsage.CacheReadTokens == 0 {
		t.Errorf("CacheReadTokens = 0 on call 2; expected a cache hit (usage: %+v)", res2.TotalUsage)
	}

	t.Logf("call 1 usage: %+v", res.TotalUsage)
	t.Logf("call 2 usage: %+v", res2.TotalUsage)
	t.Logf("call 2 text: %.120s", res2.Text)
}
