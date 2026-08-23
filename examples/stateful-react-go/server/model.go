package main

import (
	agentkit "github.com/eadwinCode/agent-kit/go"
	"github.com/zendev-sh/goai"
	"github.com/zendev-sh/goai/provider"
	"github.com/zendev-sh/goai/provider/openai"
)

const (
	openAIModelID         = "gpt-5.6-luna"
	openAIReasoningEffort = "high"
)

type modelFactory func(string) (provider.LanguageModel, []agentkit.AgenticModelOption)

func openAIProviderOptions() map[string]any {
	return map[string]any{
		"reasoning_effort":  openAIReasoningEffort,
		"reasoning_summary": "auto",
		// AgentKit owns canonical history, so the provider is not a second
		// conversation store.
		"store": false,
	}
}

// modelFor is the only production model factory. Every browser scenario uses
// the real OpenAI Responses API; deterministic providers live in _test.go.
func modelFor(_ string) (provider.LanguageModel, []agentkit.AgenticModelOption) {
	return openai.Chat(openAIModelID), []agentkit.AgenticModelOption{
		agentkit.WithCallOptions(
			goai.WithMaxOutputTokens(4096),
			goai.WithProviderOptions(openAIProviderOptions()),
		),
	}
}
