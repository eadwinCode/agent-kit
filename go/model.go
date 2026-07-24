package agentkit

import (
	"context"
	"reflect"
	"strings"

	"github.com/zendev-sh/goai"
	"github.com/zendev-sh/goai/provider"

	"github.com/eadwinCode/agent-kit/go/durable"
)

// AgenticModel wraps a goai language model for agentic use: each Infer is a
// single non-streaming generation executed inside a durable step, with
// AgentKit — never goai — owning the tool loop (port decision 9).
type AgenticModel struct {
	model        provider.LanguageModel
	cacheControl bool
	step         durable.Step
	callOptions  []goai.Option
}

// AgenticModelOption configures NewAgenticModel.
type AgenticModelOption func(*AgenticModel)

// WithCacheControl forces Anthropic prompt caching on or off. The default
// ("auto") enables it only for Anthropic models, detected from the model's
// package or id. The breakpoint goes on the system prompt, which — given
// Anthropic's tools → system → messages prefix order — caches the tool
// definitions too.
func WithCacheControl(enabled bool) AgenticModelOption {
	return func(m *AgenticModel) { m.cacheControl = enabled }
}

// WithCallOptions appends goai options to every inference call — the place
// to bake max output tokens, temperature, or provider options such as
// Anthropic thinking:
//
//	agentkit.WithCallOptions(
//		goai.WithMaxOutputTokens(8192),
//		goai.WithProviderOptions(map[string]any{
//			"thinking": map[string]any{"type": "enabled", "budgetTokens": 2048},
//		}),
//	)
//
// (This replaces the TS wrapLanguageModel + defaultSettingsMiddleware
// pattern Clevix uses; note thinking keys are AI-SDK camelCase.)
func WithCallOptions(opts ...goai.Option) AgenticModelOption {
	return func(m *AgenticModel) { m.callOptions = append(m.callOptions, opts...) }
}

// WithStep overrides the durability seam (tests use durable.Inline).
func WithStep(step durable.Step) AgenticModelOption {
	return func(m *AgenticModel) { m.step = step }
}

// NewAgenticModel wraps a goai model. Anthropic prompt caching defaults to
// auto-detection; the durable seam defaults to durable.Inngest(), which is
// safe in and outside Inngest alike.
func NewAgenticModel(model provider.LanguageModel, opts ...AgenticModelOption) *AgenticModel {
	m := &AgenticModel{
		model:        model,
		cacheControl: isAnthropicModel(model),
		step:         durable.Inngest(),
	}
	for _, o := range opts {
		o(m)
	}
	return m
}

// Model returns the wrapped goai model.
func (m *AgenticModel) Model() provider.LanguageModel { return m.model }

// InferenceResponse is a model response: parsed messages plus the raw
// serializable result (usage, cache tokens, reasoning) for billing and
// rendering.
type InferenceResponse struct {
	Output []Message
	Raw    SerializableResult
}

// Infer runs one generation inside a durable step under stepID. Tools are
// passed as definitions only — goai returns tool calls unexecuted and the
// caller (the agent loop) executes them in their own durable steps.
func (m *AgenticModel) Infer(ctx context.Context, stepID string, input []Message, tools []ToolDef, toolChoice string) (*InferenceResponse, error) {
	conv := MessagesToProviderMessages(input)
	goaiTools := ToolsToProviderTools(tools)

	raw, err := durable.Run(ctx, m.step, stepID, func(ctx context.Context) (SerializableResult, error) {
		opts := []goai.Option{goai.WithMessages(conv.Messages...)}
		if conv.System != "" {
			opts = append(opts, goai.WithSystem(conv.System))
		}
		if m.cacheControl {
			opts = append(opts, goai.WithPromptCaching(true))
		}
		if len(goaiTools) > 0 {
			opts = append(opts,
				goai.WithTools(goaiTools...),
				goai.WithToolChoice(MapToolChoice(toolChoice)),
			)
		}
		opts = append(opts, m.callOptions...)

		res, err := goai.GenerateText(ctx, m.model, opts...)
		if err != nil {
			return SerializableResult{}, err
		}
		return ToSerializableResult(res), nil
	})
	if err != nil {
		return nil, err
	}

	return &InferenceResponse{Output: ResultToMessages(raw), Raw: raw}, nil
}

// isAnthropicModel detects Anthropic for cacheControl auto mode. goai's
// LanguageModel interface exposes no provider id, so detection uses the
// implementation's package path (".../provider/anthropic") with the model
// id ("claude-*") as fallback — covering Anthropic models served through
// gateways under different package paths.
func isAnthropicModel(model provider.LanguageModel) bool {
	if model == nil {
		return false
	}
	t := reflect.TypeOf(model)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if strings.Contains(t.PkgPath(), "/provider/anthropic") {
		return true
	}
	return strings.Contains(strings.ToLower(model.ModelID()), "claude")
}
