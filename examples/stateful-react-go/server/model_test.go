package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	agentkit "github.com/eadwinCode/agent-kit/go"
	"github.com/zendev-sh/goai/provider"
)

// scriptedModel is test-only. The runnable server always uses GPT-5.6 Luna.
type scriptedModel struct {
	mu      sync.Mutex
	scripts [][]provider.StreamChunk
	results []*provider.GenerateResult
	calls   int
	delay   time.Duration
}

func (m *scriptedModel) ModelID() string { return "stateful-session-test" }

func (m *scriptedModel) next() ([]provider.StreamChunk, *provider.GenerateResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	i := m.calls
	m.calls++
	if i >= len(m.scripts) {
		i = len(m.scripts) - 1
	}
	return m.scripts[i], m.results[i]
}

func (m *scriptedModel) DoGenerate(context.Context, provider.GenerateParams) (*provider.GenerateResult, error) {
	_, result := m.next()
	return result, nil
}

func (m *scriptedModel) DoStream(ctx context.Context, _ provider.GenerateParams) (*provider.StreamResult, error) {
	script, result := m.next()
	out := make(chan provider.StreamChunk, len(script)+1)
	go func() {
		defer close(out)
		for _, chunk := range script {
			if m.delay > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(m.delay):
				}
			}
			if !provider.TrySend(ctx, out, chunk) {
				return
			}
		}
		provider.TrySend(ctx, out, provider.StreamChunk{
			Type: provider.ChunkFinish, FinishReason: result.FinishReason,
			Usage: result.Usage, Response: result.Response,
			Metadata: map[string]any{"providerMetadata": result.ProviderMetadata},
		})
	}()
	return &provider.StreamResult{Stream: out}, nil
}

func testModelFor(scenario string) (provider.LanguageModel, []agentkit.AgenticModelOption) {
	usage := provider.Usage{InputTokens: 12, OutputTokens: 8, TotalTokens: 20}
	switch scenario {
	case "structured", "slow":
		return &scriptedModel{
			delay: 120 * time.Millisecond,
			scripts: [][]provider.StreamChunk{
				{
					{Type: provider.ChunkReasoning, Text: "I should inspect the workspace safely."},
					{Type: provider.ChunkToolCallStreamStart, ToolCallID: "scan_1", ToolName: "scan_workspace"},
					{Type: provider.ChunkToolCallDelta, ToolCallID: "scan_1", ToolName: "scan_workspace", ToolInput: `{"depth":`},
					{Type: provider.ChunkToolCallDelta, ToolCallID: "scan_1", ToolName: "scan_workspace", ToolInput: `5}`},
					{Type: provider.ChunkToolCall, ToolCallID: "scan_1", ToolName: "scan_workspace", ToolInput: `{"depth":5}`},
				},
				{{Type: provider.ChunkText, Text: "The workspace scan completed after five safe checkpoints."}},
			},
			results: []*provider.GenerateResult{
				{FinishReason: provider.FinishToolCalls, ToolCalls: []provider.ToolCall{{ID: "scan_1", Name: "scan_workspace", Input: json.RawMessage(`{"depth":5}`)}}, Usage: usage},
				{Text: "The workspace scan completed after five safe checkpoints.", FinishReason: provider.FinishStop, Usage: usage},
			},
		}, nil
	case "approval":
		return &scriptedModel{
			delay: 100 * time.Millisecond,
			scripts: [][]provider.StreamChunk{
				{{Type: provider.ChunkToolCall, ToolCallID: "publish_1", ToolName: "publish_demo", ToolInput: `{}`}},
				{{Type: provider.ChunkText, Text: "The approved demo release was published."}},
			},
			results: []*provider.GenerateResult{
				{FinishReason: provider.FinishToolCalls, ToolCalls: []provider.ToolCall{{ID: "publish_1", Name: "publish_demo", Input: json.RawMessage(`{}`)}}, Usage: usage},
				{Text: "The approved demo release was published.", FinishReason: provider.FinishStop, Usage: usage},
			},
		}, nil
	case "error":
		return &scriptedModel{
			delay: 100 * time.Millisecond,
			scripts: [][]provider.StreamChunk{{
				{Type: provider.ChunkText, Text: "This partial response will fail"},
				{Type: provider.ChunkError, Error: errors.New("scripted provider failure")},
			}},
			results: []*provider.GenerateResult{{FinishReason: provider.FinishError, Usage: usage}},
		}, nil
	default:
		return &scriptedModel{
			delay: 140 * time.Millisecond,
			scripts: [][]provider.StreamChunk{{
				{Type: provider.ChunkReasoning, Text: "I can answer this test request."},
				{Type: provider.ChunkText, Text: "Hello from the Go AgentKit test runtime."},
			}},
			results: []*provider.GenerateResult{{
				Text: "Hello from the Go AgentKit test runtime.", Reasoning: "I can answer this test request.",
				FinishReason: provider.FinishStop, Usage: usage,
			}},
		}, nil
	}
}
