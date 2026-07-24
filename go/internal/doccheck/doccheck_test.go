package doccheck

// Compile-checks the code samples in README.md and doc.go so the docs
// cannot drift from the API without a test failure.

import (
	"context"
	"net/http"
	"testing"

	"github.com/inngest/inngestgo"
	"github.com/zendev-sh/goai"
	"github.com/zendev-sh/goai/provider"
	"github.com/zendev-sh/goai/provider/anthropic"

	agentkit "github.com/eadwinCode/agent-kit/go"
	"github.com/eadwinCode/agent-kit/go/durable"
)

type state struct {
	City string `json:"city,omitempty"`
	SKU  int    `json:"sku,omitempty"`
}

// Quick start (README + doc.go).
func TestQuickStartCompiles(t *testing.T) {
	weather := agentkit.NewTool[state]("get_weather",
		"Get the current weather for a city.",
		func(ctx context.Context, in struct {
			City string `json:"city" jsonschema:"description=The city to check"`
		}, opts agentkit.ToolOptions[state]) (any, error) {
			opts.State.Data.City = in.City
			return map[string]any{"city": in.City, "temperature_c": 21}, nil
		})

	assistant := agentkit.NewAgent(agentkit.AgentConfig[state]{
		Name:   "assistant",
		System: "You are concise. Use get_weather when asked about weather.",
		Tools:  []agentkit.Tool[state]{weather},
		Model:  anthropic.Chat("claude-sonnet-4-5"),
	})
	_ = assistant

	// Reading text off a result, as the README shows.
	res := agentkit.NewAgentResult("assistant", []agentkit.Message{{
		Type: agentkit.MessageText, Role: agentkit.RoleAssistant,
		Content: agentkit.TextContent("hi"),
	}}, nil, agentkit.Now())
	for _, m := range res.Output {
		if _, ok := m.Content.AsString(); !ok {
			t.Fatal("expected string content")
		}
	}
}

// Optional tool parameters are pointers (strict-mode schemas).
type editInput struct {
	Path   string  `json:"path" jsonschema:"description=File to edit"`
	Reason *string `json:"reason"`
}

func TestToolDocsCompile(t *testing.T) {
	manual := agentkit.NewTool[state]("wait_for_approval", "Pause for a human.",
		func(ctx context.Context, in editInput, opts agentkit.ToolOptions[state]) (any, error) {
			return nil, nil
		}, agentkit.WithManualStep[state]())
	if !manual.ManualStep {
		t.Fatal("WithManualStep did not apply")
	}
}

// Network + router sample.
func TestNetworkDocsCompile(t *testing.T) {
	var model provider.LanguageModel
	assistant := agentkit.NewAgent(agentkit.AgentConfig[state]{Name: "assistant", System: "s"})

	net := agentkit.NewNetwork(agentkit.NetworkConfig[state]{
		Name:         "support",
		Agents:       []*agentkit.Agent[state]{assistant},
		DefaultModel: model,
		MaxIter:      5,
		Router: &agentkit.Router[state]{
			Fn: func(ctx context.Context, args agentkit.RouterArgs[state]) (*agentkit.RouterResult[state], error) {
				if args.CallCount == 0 {
					return agentkit.RouteTo(assistant), nil
				}
				if last := args.LastResult; last != nil && len(last.ToolCalls) > 0 {
					return agentkit.RouteTo(assistant), nil
				}
				return nil, nil
			},
		},
		History: &agentkit.HistoryConfig[state]{},
	})
	_ = net
}

// Streaming + model options + serving.
func TestStreamingAndServerDocsCompile(t *testing.T) {
	step := durable.Inline{}
	publish := agentkit.DurablePublish(step, func(ctx context.Context, c agentkit.AgentMessageChunk) error {
		return nil
	})
	_ = &agentkit.StreamingConfig{Publish: publish, StreamReasoning: true}

	_ = agentkit.WithCallOptions(
		goai.WithMaxOutputTokens(4096),
		goai.WithProviderOptions(map[string]any{
			"thinking": map[string]any{"type": "enabled", "budgetTokens": 1024},
		}),
	)

	var handler http.Handler
	handler, err := agentkit.NewServer("my-app", func(c inngestgo.Client) error {
		_, err := agentkit.RegisterNetwork(c, agentkit.NewNetwork(agentkit.NetworkConfig[state]{Name: "n"}))
		return err
	})
	if err != nil || handler == nil {
		t.Fatalf("NewServer: %v", err)
	}
}
