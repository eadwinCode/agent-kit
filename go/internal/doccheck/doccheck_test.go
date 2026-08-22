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
	"github.com/eadwinCode/agent-kit/go/conformance"
	"github.com/eadwinCode/agent-kit/go/durable"
	"github.com/eadwinCode/agent-kit/go/memadapter"
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

// Runtime ports (README "Runtime ports"). Compile-checks the port wiring and
// the tool-side structured stream / approval example, so the documented
// contract cannot drift from the API without a test failure.
func TestRuntimePortsCompile(t *testing.T) {
	ports := &agentkit.RuntimePorts{
		Journal:   memadapter.NewJournal(),
		State:     memadapter.NewStateStore(),
		Control:   memadapter.NewControlStore(),
		Approvals: memadapter.NewApprovalStore(),
		Finalizer: memadapter.NewFinalizer(),
		Sink:      memadapter.NewSink(),
		// The scope is opaque: compose whatever identifies the conversation
		// owner in your own model.
		Scope:       agentkit.SessionScope("owner-scope-1"),
		StreamEpoch: 1,
	}

	publish := agentkit.NewTool[state]("publish", "Publish the site.",
		func(ctx context.Context, in struct{}, opts agentkit.ToolOptions[state]) (any, error) {
			opts.Stream.Status(ctx, agentkit.StatusUpdate{
				Kind: agentkit.ActivityWriting, Label: "Publishing",
			})
			if err := opts.Stream.Checkpoint(ctx, agentkit.CheckpointBeforeSideEffect); err != nil {
				return nil, err
			}
			if _, err := opts.Approvals.Require(ctx, agentkit.ApprovalRequest{
				RequestID: "approval_" + opts.ToolCallID,
				ToolName:  "publish",
				Summary:   "Publish the site",
			}); err != nil {
				return nil, err
			}
			return map[string]any{"published": true}, nil
		})

	agent := agentkit.NewAgent(agentkit.AgentConfig[state]{
		Name: "publisher", Model: anthropic.Chat("claude-sonnet-4-5"),
		Tools: []agentkit.Tool[state]{publish},
	})
	net := agentkit.NewNetwork(agentkit.NetworkConfig[state]{
		Name: "site", Agents: []*agentkit.Agent[state]{agent}, Ports: ports,
	})

	opts := &agentkit.NetworkRunOptions[state]{
		Ports:     ports,
		Streaming: &agentkit.StreamingConfig{Publish: func(context.Context, agentkit.AgentMessageChunk) error { return nil }},
	}
	_, _ = net, opts
}

// Adapter conformance (README "Runtime ports"). The suite an application's own
// adapter runs against its own storage.
func TestConformanceSuiteCompiles(t *testing.T) {
	conformance.VerifyEventJournal(t, func() agentkit.EventJournal {
		return memadapter.NewJournal()
	})
}
