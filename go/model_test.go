package agentkit

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/zendev-sh/goai/provider"

	"github.com/eadwinCode/agent-kit/go/durable"
)

// fakeModel implements provider.LanguageModel, capturing the params goai
// hands it and returning a canned result (or a queue of them, one per
// call, for multi-iteration tests).
type fakeModel struct {
	id         string
	lastParams provider.GenerateParams
	allParams  []provider.GenerateParams
	result     *provider.GenerateResult
	queue      []*provider.GenerateResult
	calls      int
	err        error
}

func (f *fakeModel) ModelID() string { return f.id }

func (f *fakeModel) DoGenerate(ctx context.Context, params provider.GenerateParams) (*provider.GenerateResult, error) {
	f.lastParams = params
	f.allParams = append(f.allParams, params)
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	if len(f.queue) > 0 {
		r := f.queue[0]
		f.queue = f.queue[1:]
		return r, nil
	}
	return f.result, nil
}

func (f *fakeModel) DoStream(ctx context.Context, params provider.GenerateParams) (*provider.StreamResult, error) {
	return nil, errors.New("fakeModel: streaming not expected in AgenticModel.Infer")
}

func weatherToolDef() ToolDef {
	return ToolDef{
		Name:        "get_weather",
		Description: "Get weather.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}`),
	}
}

func TestAgenticModelInfer(t *testing.T) {
	fake := &fakeModel{
		id: "claude-test",
		result: &provider.GenerateResult{
			Text:         "checking",
			FinishReason: provider.FinishToolCalls,
			ToolCalls:    []provider.ToolCall{{ID: "t1", Name: "get_weather", Input: json.RawMessage(`{"city":"Tokyo"}`)}},
			Usage:        provider.Usage{InputTokens: 10, OutputTokens: 5, TotalTokens: 15, CacheWriteTokens: 100},
			Reasoning:    "hm",
			ProviderMetadata: map[string]map[string]any{
				"anthropic": {"reasoning": []map[string]any{{"type": "thinking", "text": "hm", "signature": "s1"}}},
			},
		},
	}
	m := NewAgenticModel(fake, WithCacheControl(true))

	resp, err := m.Infer(context.Background(),
		"step-1",
		[]Message{
			{Type: MessageText, Role: RoleSystem, Content: TextContent("Be helpful.")},
			{Type: MessageText, Role: RoleUser, Content: TextContent("Weather in Tokyo?")},
		},
		[]ToolDef{weatherToolDef()},
		"auto",
	)
	if err != nil {
		t.Fatal(err)
	}

	// --- What reached the provider ---
	p := fake.lastParams
	if p.System != "Be helpful." {
		t.Errorf("system not extracted to params.System: %q", p.System)
	}
	if !p.PromptCaching {
		t.Error("cacheControl did not set PromptCaching")
	}
	if len(p.Messages) != 1 || p.Messages[0].Role != provider.RoleUser {
		t.Errorf("messages should exclude system: %+v", p.Messages)
	}
	if len(p.Tools) != 1 || p.Tools[0].Name != "get_weather" {
		t.Errorf("tools not passed: %+v", p.Tools)
	}
	if p.ToolChoice != "auto" {
		t.Errorf("tool choice = %q", p.ToolChoice)
	}

	// --- What came back (through a real JSON round-trip: the default
	// durable step marshals even outside Inngest) ---
	if resp.Raw.Usage == nil || resp.Raw.Usage.CacheCreationInputTokens != 100 {
		t.Errorf("usage lost through the step round-trip: %+v", resp.Raw.Usage)
	}
	if len(resp.Raw.ReasoningDetails) != 1 || resp.Raw.ReasoningDetails[0].Signature != "s1" {
		t.Errorf("reasoning signature lost: %+v", resp.Raw.ReasoningDetails)
	}
	// Output: reasoning, text, tool_call — in that order.
	if len(resp.Output) != 3 ||
		resp.Output[0].Type != MessageReasoning ||
		resp.Output[1].Type != MessageText ||
		resp.Output[2].Type != MessageToolCall {
		t.Fatalf("output order wrong: %+v", resp.Output)
	}
	if resp.Output[2].Tools[0].Name != "get_weather" {
		t.Errorf("tool call lost: %+v", resp.Output[2])
	}
}

func TestAgenticModelInferPropagatesError(t *testing.T) {
	fake := &fakeModel{id: "claude-test", err: errors.New("api down")}
	m := NewAgenticModel(fake, WithStep(durable.Inline{}))
	_, err := m.Infer(context.Background(), "step-1", []Message{
		{Type: MessageText, Role: RoleUser, Content: TextContent("hi")},
	}, nil, "auto")
	if err == nil {
		t.Fatal("expected provider error to propagate")
	}
}

func TestIsAnthropicModelHeuristics(t *testing.T) {
	if !isAnthropicModel(&fakeModel{id: "claude-sonnet-4-5"}) {
		t.Error("claude-* model id must trigger auto cache control")
	}
	if isAnthropicModel(&fakeModel{id: "gpt-4o"}) {
		t.Error("non-anthropic model must not trigger cache control")
	}
	// Auto default flows into the model.
	m := NewAgenticModel(&fakeModel{id: "claude-x"})
	if !m.cacheControl {
		t.Error("NewAgenticModel should default cacheControl on for claude ids")
	}
}
