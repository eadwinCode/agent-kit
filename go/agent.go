package agentkit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/zendev-sh/goai/mcp"
	"github.com/zendev-sh/goai/provider"

	"github.com/eadwinCode/agent-kit/go/durable"
	"github.com/eadwinCode/agent-kit/go/internal/jsonutil"
)

// DefaultMaxIterPerRun caps the inferences a single Agent.Run performs in
// its internal tool-round loop. Deliberately decoupled from a Network's
// MaxIter (which bounds how many times the network calls agents): inside a
// network the network drives iteration and the agent does ONE inference
// per call, so total inferences stay ≤ MaxIter rather than MaxIter².
const DefaultMaxIterPerRun = 1

// Agent is a single agent responsible for a set of tasks. Immutable after
// construction (port decision 11) except for the lazily-initialized MCP
// client cache, which is mutex-guarded.
type Agent[T any] struct {
	// Name identifies the agent (also the base of its durable step ids).
	Name string

	Description string

	// System is the static system prompt. SystemFn takes precedence when
	// set, and may inspect the network to build a dynamic prompt.
	System   string
	SystemFn func(ctx context.Context, network *NetworkRun[T]) (string, error)

	// Assistant is an optional assistant message appended after the user
	// input (completion steering).
	Assistant string

	// ToolChoice: "auto" (default), "any", or a specific tool name.
	ToolChoice string

	// Lifecycle hooks. For a RoutingAgent use RoutingLifecycle.
	Lifecycle *Lifecycle[T]

	// Model is this agent's model; falls back to the network's default.
	Model provider.LanguageModel

	// ModelOptions configure the AgenticModel built around Model (cache
	// control, goai call options such as thinking budgets). This replaces
	// the TS pattern of baking settings into the model with middleware.
	ModelOptions []AgenticModelOption

	// MaxIterPerRun caps inferences within one Run (0 = DefaultMaxIterPerRun).
	MaxIterPerRun int

	// MCPServers provide additional tools, resolved on first Run.
	MCPServers []MCPServer

	// History persists standalone-run conversations.
	History *HistoryConfig[T]

	// tools in insertion order (order is request-visible: it affects the
	// provider tool list and hence prompt-cache stability), plus an index.
	tools     []Tool[T]
	toolIndex map[string]int

	// MCP tool cache — the one mutable corner; guarded for concurrent runs.
	mcpMu      sync.Mutex
	mcpClients []*mcp.Client
}

// AgentConfig configures NewAgent.
type AgentConfig[T any] struct {
	Name          string
	Description   string
	System        string
	SystemFn      func(ctx context.Context, network *NetworkRun[T]) (string, error)
	Assistant     string
	Tools         []Tool[T]
	ToolChoice    string
	Lifecycle     *Lifecycle[T]
	Model         provider.LanguageModel
	ModelOptions  []AgenticModelOption
	MaxIterPerRun int
	MCPServers    []MCPServer
	History       *HistoryConfig[T]
}

// NewAgent creates an agent.
func NewAgent[T any](cfg AgentConfig[T]) *Agent[T] {
	a := &Agent[T]{
		Name:          cfg.Name,
		Description:   cfg.Description,
		System:        cfg.System,
		SystemFn:      cfg.SystemFn,
		Assistant:     cfg.Assistant,
		ToolChoice:    cfg.ToolChoice,
		Lifecycle:     cfg.Lifecycle,
		Model:         cfg.Model,
		ModelOptions:  cfg.ModelOptions,
		MaxIterPerRun: cfg.MaxIterPerRun,
		MCPServers:    cfg.MCPServers,
		History:       cfg.History,
		toolIndex:     map[string]int{},
	}
	for _, t := range cfg.Tools {
		a.addTool(t)
	}
	return a
}

func (a *Agent[T]) addTool(t Tool[T]) {
	if i, exists := a.toolIndex[t.Name]; exists {
		a.tools[i] = t
		return
	}
	a.toolIndex[t.Name] = len(a.tools)
	a.tools = append(a.tools, t)
}

// Tools returns the agent's tools in insertion order.
func (a *Agent[T]) Tools() []Tool[T] {
	return append([]Tool[T](nil), a.tools...)
}

// WithModel returns a copy of the agent bound to a specific model.
func (a *Agent[T]) WithModel(model provider.LanguageModel, opts ...AgenticModelOption) *Agent[T] {
	return NewAgent(AgentConfig[T]{
		Name:          a.Name,
		Description:   a.Description,
		System:        a.System,
		SystemFn:      a.SystemFn,
		Assistant:     a.Assistant,
		Tools:         a.Tools(),
		ToolChoice:    a.ToolChoice,
		Lifecycle:     a.Lifecycle,
		Model:         model,
		ModelOptions:  opts,
		MaxIterPerRun: a.MaxIterPerRun,
		MCPServers:    a.MCPServers,
		History:       a.History,
	})
}

// Lifecycle hooks manage an agent programmatically around inference.
type Lifecycle[T any] struct {
	// Enabled selectively enables this agent based on network state
	// (nil = always enabled).
	Enabled func(ctx context.Context, args LifecycleBase[T]) (bool, error)

	// OnStart runs just before inference; it can adjust the prompt and
	// history, or stop the call entirely.
	OnStart func(ctx context.Context, args LifecycleBefore[T]) (LifecycleStartResult, error)

	// OnResponse runs after inference, before tools — moderate the
	// response prior to running tools.
	OnResponse func(ctx context.Context, args LifecycleResult[T]) (*AgentResult, error)

	// OnFinish receives the finalized result, tool calls included. Its
	// return value is what gets saved to network history.
	OnFinish func(ctx context.Context, args LifecycleResult[T]) (*AgentResult, error)
}

// LifecycleBase identifies the agent (and network, when networked).
type LifecycleBase[T any] struct {
	Agent   *Agent[T]
	Network *NetworkRun[T]
}

// LifecycleBefore is OnStart's payload.
type LifecycleBefore[T any] struct {
	LifecycleBase[T]
	// Input is the user request for the entire agentic operation.
	Input string
	// Prompt is the system/user/assistant prompt (no history).
	Prompt []Message
	// History is the past conversation, appended after the prompt.
	History []Message
}

// LifecycleStartResult is OnStart's verdict.
type LifecycleStartResult struct {
	Prompt  []Message
	History []Message
	// Stop prevents calling the model.
	Stop bool
}

// LifecycleResult carries a result through OnResponse/OnFinish/OnRoute.
type LifecycleResult[T any] struct {
	LifecycleBase[T]
	Result *AgentResult
}

// RouterFn picks the next agent names from a routing agent's result.
// Returning nil stops the network loop.
type RouterFn[T any] func(ctx context.Context, args LifecycleResult[T]) []string

// RoutingAgent is an Agent whose result is interpreted by OnRoute to pick
// the next agents in a network loop.
type RoutingAgent[T any] struct {
	Agent[T]
	OnRoute RouterFn[T]
}

// NewRoutingAgent creates a routing agent.
func NewRoutingAgent[T any](cfg AgentConfig[T], onRoute RouterFn[T]) *RoutingAgent[T] {
	return &RoutingAgent[T]{Agent: *NewAgent(cfg), OnRoute: onRoute}
}

// RunOptions configures a single Agent.Run.
type RunOptions[T any] struct {
	// UserMessage is the rich client input; when set, its Content and
	// per-turn SystemPrompt take precedence over the plain input string.
	UserMessage *UserMessage

	// Model overrides the agent's model for this run.
	Model        provider.LanguageModel
	ModelOptions []AgenticModelOption

	// Network is set when the agent runs inside a network loop.
	Network *NetworkRun[T]

	// State passes custom state into a standalone run (networks supply
	// their own).
	State *State[T]

	// MaxIterPerRun caps this run's internal tool-round loop.
	MaxIterPerRun int

	// Step overrides the durability seam (tests use durable.Inline).
	Step durable.Step

	// Streaming enables event emission for STANDALONE runs; ignored inside
	// a network (the network controls streaming).
	Streaming *StreamingConfig

	// streamingContext is set by the network to let the agent emit
	// part/text/tool events into the run's stream.
	streamingContext *StreamingContext
}

// Run runs the agent with the given user input, treated as a user message.
// An empty input executes only the system prompt.
func (a *Agent[T]) Run(ctx context.Context, input string, opts *RunOptions[T]) (res *AgentResult, err error) {
	if opts == nil {
		opts = &RunOptions[T]{}
	}

	if err := a.initMCP(ctx); err != nil {
		return nil, err
	}

	step := opts.Step
	if step == nil {
		step = durable.Inngest()
	}

	internalMaxIter := opts.MaxIterPerRun
	if internalMaxIter <= 0 {
		internalMaxIter = a.MaxIterPerRun
	}
	if internalMaxIter <= 0 {
		internalMaxIter = DefaultMaxIterPerRun
	}

	rawModel := opts.Model
	if rawModel == nil {
		rawModel = a.Model
	}
	modelOpts := append(append([]AgenticModelOption(nil), a.ModelOptions...), opts.ModelOptions...)
	if rawModel == nil && opts.Network != nil {
		rawModel = opts.Network.DefaultModel
		modelOpts = append(append([]AgenticModelOption(nil), opts.Network.DefaultModelOptions...), modelOpts...)
	}
	if rawModel == nil {
		return nil, fmt.Errorf("agentkit: no model provided to agent %q", a.Name)
	}
	model := NewAgenticModel(rawModel, append(modelOpts, WithStep(step))...)

	// Input state always overrides the network state.
	s := opts.State
	if s == nil && opts.Network != nil {
		s = opts.Network.State
	}
	if s == nil {
		s = NewState(StateConfig[T]{})
	}
	run := opts.Network
	if run == nil {
		run = newNetworkRun(NewNetwork(NetworkConfig[T]{Name: "default"}), s)
	}

	inputContent := input
	userSystemPrompt := ""
	if opts.UserMessage != nil {
		if opts.UserMessage.Content != "" {
			inputContent = opts.UserMessage.Content
		}
		userSystemPrompt = opts.UserMessage.SystemPrompt
	}

	// Standalone streaming (ignored inside a network — the network provides
	// its own context and emits the agent's run lifecycle itself).
	sc := opts.streamingContext
	standaloneStreaming := false
	if sc == nil && opts.Network == nil && opts.Streaming != nil && opts.Streaming.Publish != nil {
		ids, idErr := durable.Run(ctx, step, "generate-standalone-agent-ids",
			func(ctx context.Context) (agentIterationIDs, error) {
				return agentIterationIDs{RunID: generateRunID(), MessageID: uuid.NewString()}, nil
			})
		if idErr != nil {
			return nil, idErr
		}
		sc = streamingContextFromState(s, *opts.Streaming, ids.RunID, ids.MessageID, "agent")
		standaloneStreaming = true
		sc.PublishEvent(ctx, EventRunStarted, map[string]any{
			"runId": sc.RunID, "scope": "agent", "name": a.Name, "messageId": sc.MessageID,
		})
		// Single terminal emitter for every exit path, failure included, so
		// subscribers reliably unstick.
		defer func() {
			if err != nil {
				sc.PublishEvent(ctx, EventRunFailed, map[string]any{
					"runId": sc.RunID, "scope": "agent", "name": a.Name,
					"error": err.Error(), "recoverable": false,
				})
			}
			sc.PublishEvent(ctx, EventRunCompleted, map[string]any{
				"runId": sc.RunID, "scope": "agent", "name": a.Name,
			})
			sc.PublishEvent(ctx, EventStreamEnded, map[string]any{
				"scope": "agent", "messageId": sc.MessageID,
			})
		}()
	}

	topCfg := threadOpConfig[T]{State: s, History: a.History, Input: inputContent, Network: run, Step: step}
	if err := initializeThread(ctx, topCfg); err != nil {
		return nil, err
	}
	if err := loadThreadFromStorage(ctx, topCfg); err != nil {
		return nil, err
	}

	history := s.FormatHistory(nil)
	prompt, err := a.agentPrompt(ctx, inputContent, userSystemPrompt, run)
	if err != nil {
		return nil, err
	}

	result := NewAgentResult(a.Name, nil, nil, jsonutil.Now())
	result.Prompt = prompt
	result.History = history

	initialResultCount := len(s.results)

	hasMoreActions := true
	for iter := 0; hasMoreActions && iter < internalMaxIter; iter++ {
		if a.Lifecycle != nil && a.Lifecycle.OnStart != nil {
			modified, err := a.Lifecycle.OnStart(ctx, LifecycleBefore[T]{
				LifecycleBase: LifecycleBase[T]{Agent: a, Network: run},
				Input:         inputContent,
				Prompt:        prompt,
				History:       history,
			})
			if err != nil {
				return nil, err
			}
			if modified.Stop {
				// The user prevented calling the model.
				return result, nil
			}
			prompt = modified.Prompt
			history = modified.History
		}

		inference, err := a.performInference(ctx, model, iter, prompt, history, run, sc, step)
		if err != nil {
			return nil, err
		}

		// Filter reasoning before checking stop_reason: ReasoningMessage
		// may carry none and would otherwise loop forever.
		var lastActionable *Message
		for i := len(inference.Output) - 1; i >= 0; i-- {
			if inference.Output[i].Type != MessageReasoning {
				lastActionable = &inference.Output[i]
				break
			}
		}
		hasMoreActions = len(a.tools) > 0 && lastActionable != nil && lastActionable.StopReason != StopStop

		result = inference
		// The standalone streaming messageId becomes the persisted message id.
		if standaloneStreaming {
			result.ID = sc.MessageID
		}
		// Feed assistant output AND tool results back into history so a
		// multi-iteration internal loop sends a valid tool-call →
		// tool-result sequence (mirrors State.FormatHistory's order).
		history = append(append([]Message(nil), inference.Output...), inference.ToolCalls...)
	}

	if a.Lifecycle != nil && a.Lifecycle.OnFinish != nil {
		finished, err := a.Lifecycle.OnFinish(ctx, LifecycleResult[T]{
			LifecycleBase: LifecycleBase[T]{Agent: a, Network: run},
			Result:        result,
		})
		if err != nil {
			return nil, err
		}
		result = finished
	}

	// Routing lifecycles are not called by the agent — the network calls them.

	if err := saveThreadToStorage(ctx, topCfg, initialResultCount); err != nil {
		return nil, err
	}

	return result, nil
}

// performInference does one model call plus its tool invocations,
// streaming reasoning/text parts when a streaming context is present.
func (a *Agent[T]) performInference(
	ctx context.Context,
	model *AgenticModel,
	iter int,
	prompt, history []Message,
	run *NetworkRun[T],
	sc *StreamingContext,
	step durable.Step,
) (*AgentResult, error) {
	defs := make([]ToolDef, 0, len(a.tools))
	for _, t := range a.tools {
		defs = append(defs, t.Def())
	}
	toolChoice := a.ToolChoice
	if toolChoice == "" {
		toolChoice = "auto"
	}

	// Step id includes the iteration index: the TS version reuses the bare
	// agent name and relies on the runtime auto-suffixing duplicate ids;
	// an explicit index is deterministic without that behavior. (Step ids
	// only need consistency within a runtime — in-flight runs never
	// migrate between the TS and Go implementations.)
	stepID := fmt.Sprintf("%s/infer/%d", a.Name, iter)

	input := append(append([]Message(nil), prompt...), history...)
	var inferenceStream *inferencePartStream
	var resp *InferenceResponse
	var err error
	if sc != nil {
		inferenceStream = newInferencePartStream(ctx, sc, a.Name, iter)
		resp, err = model.inferStream(
			ctx, stepID, input, defs, toolChoice,
			inferenceStream.Handle,
			func(err error) {
				if err != nil {
					inferenceStream.Fail(err)
					return
				}
				inferenceStream.Complete()
			},
			inferenceStream.EventCount,
		)
	} else {
		resp, err = model.Infer(ctx, stepID, input, defs, toolChoice)
	}
	if err != nil {
		if inferenceStream != nil {
			inferenceStream.Fail(err)
		}
		return nil, err
	}
	if inferenceStream != nil && resp.trueStreaming {
		// A memoized inference does not execute its body or callbacks. Reserve
		// the same sequence range its already-published model parts occupied,
		// keeping every later durable publish id replay-stable.
		sc.seq.advance(resp.streamEventCount - inferenceStream.EventCount())
	}

	rawJSON, err := jsonutil.MarshalString(resp.Raw)
	if err != nil {
		return nil, fmt.Errorf("agentkit: serialize raw inference result: %w", err)
	}

	result := NewAgentResult(a.Name, resp.Output, nil, jsonutil.Now())
	result.Prompt = prompt
	result.History = history
	result.Raw = rawJSON

	if a.Lifecycle != nil && a.Lifecycle.OnResponse != nil {
		moderated, err := a.Lifecycle.OnResponse(ctx, LifecycleResult[T]{
			LifecycleBase: LifecycleBase[T]{Agent: a, Network: run},
			Result:        result,
		})
		if err != nil {
			return nil, err
		}
		result = moderated
	}

	// Backward-compatible replay of an inference cached by the pre-true-
	// streaming implementation. Its durable value has no streaming marker, so
	// reproduce the former post-completion events once. New streamed inferences
	// never enter this simulated fallback and therefore cannot publish twice.
	if sc != nil && !resp.trueStreaming {
		if err := a.streamLegacyCompletedInference(ctx, result, sc, step); err != nil {
			return nil, err
		}
	}

	toolCalls, err := a.invokeTools(ctx, result.Output, run, sc, step)
	if err != nil {
		return nil, err
	}
	result.ToolCalls = append(result.ToolCalls, toolCalls...)

	return result, nil
}

// inferencePartStream translates raw GoAI reasoning/text chunks into AgentKit
// parts. Only one provider content part is open at a time; a chunk-type switch
// completes the current part before creating the next. This handles providers
// that expose multiple reasoning/text blocks while keeping strict
// created -> delta(s) -> completed ordering for every stable part id.
type inferencePartStream struct {
	ctx       context.Context
	sc        *StreamingContext
	agentName string
	iter      int
	counts    map[string]int
	active    *inferenceStreamPart
	failed    bool
	events    int
}

type inferenceStreamPart struct {
	id      string
	kind    string
	content strings.Builder
}

func newInferencePartStream(ctx context.Context, sc *StreamingContext, agentName string, iter int) *inferencePartStream {
	return &inferencePartStream{
		ctx: ctx, sc: sc, agentName: agentName, iter: iter,
		counts: map[string]int{},
	}
}

func (s *inferencePartStream) Handle(chunk provider.StreamChunk) {
	if s == nil || s.failed {
		return
	}
	switch chunk.Type {
	case provider.ChunkReasoning:
		if s.sc.StreamReasoning && chunk.Text != "" {
			s.delta("reasoning", EventReasoningDelta, chunk.Text)
		}
	case provider.ChunkText:
		if chunk.Text != "" {
			s.delta("text", EventTextDelta, chunk.Text)
		}
	case provider.ChunkFinish:
		s.Complete()
	case provider.ChunkError:
		err := chunk.Error
		if err == nil {
			err = errors.New("agentkit: provider stream failed")
		}
		s.Fail(err)
	}
}

func (s *inferencePartStream) delta(kind, event, delta string) {
	if s.active == nil || s.active.kind != kind {
		s.Complete()
		index := s.counts[kind]
		s.counts[kind] = index + 1
		s.active = &inferenceStreamPart{
			id: s.sc.stablePartID(kind, s.iter, index), kind: kind,
		}
		s.sc.PublishEvent(s.ctx, EventPartCreated, map[string]any{
			"partId": s.active.id, "runId": s.sc.RunID, "messageId": s.sc.MessageID,
			"type": kind, "metadata": map[string]any{"agentName": s.agentName},
		})
		s.events++
	}
	s.active.content.WriteString(delta)
	s.sc.PublishEvent(s.ctx, event, map[string]any{
		"partId": s.active.id, "messageId": s.sc.MessageID, "delta": delta,
	})
	s.events++
}

func (s *inferencePartStream) Complete() {
	if s == nil || s.failed || s.active == nil {
		return
	}
	s.sc.PublishEvent(s.ctx, EventPartCompleted, map[string]any{
		"partId": s.active.id, "runId": s.sc.RunID, "messageId": s.sc.MessageID,
		"type": s.active.kind, "finalContent": s.active.content.String(),
	})
	s.events++
	s.active = nil
}

func (s *inferencePartStream) Fail(err error) {
	if s == nil || s.failed {
		return
	}
	s.failed = true
	if s.active == nil {
		return
	}
	s.sc.PublishEvent(s.ctx, EventPartFailed, map[string]any{
		"partId": s.active.id, "runId": s.sc.RunID, "messageId": s.sc.MessageID,
		"type": s.active.kind, "error": err.Error(),
	})
	s.events++
	s.active = nil
}

func (s *inferencePartStream) EventCount() int {
	if s == nil {
		return 0
	}
	return s.events
}

func (a *Agent[T]) streamLegacyCompletedInference(
	ctx context.Context,
	result *AgentResult,
	sc *StreamingContext,
	step durable.Step,
) error {
	if sc.StreamReasoning {
		reasoningIndex := 0
		for _, message := range result.Output {
			if message.Type != MessageReasoning {
				continue
			}
			content, _ := message.Content.AsString()
			partID, err := a.streamPartID(ctx, sc, step,
				fmt.Sprintf("generate-reasoning-part-id-%s-%d", sc.MessageID, reasoningIndex))
			if err != nil {
				return err
			}
			reasoningIndex++
			sc.PublishEvent(ctx, EventPartCreated, map[string]any{
				"partId": partID, "runId": sc.RunID, "messageId": sc.MessageID,
				"type": "reasoning", "metadata": map[string]any{"agentName": a.Name},
			})
			for _, delta := range sc.ChunkContent(content) {
				sc.PublishEvent(ctx, EventReasoningDelta, map[string]any{
					"partId": partID, "messageId": sc.MessageID, "delta": delta,
				})
			}
			sc.PublishEvent(ctx, EventPartCompleted, map[string]any{
				"partId": partID, "runId": sc.RunID, "messageId": sc.MessageID,
				"type": "reasoning", "finalContent": content,
			})
		}
	}

	content := ""
	for i := len(result.Output) - 1; i >= 0; i-- {
		message := result.Output[i]
		if message.Type == MessageText && message.Role == RoleAssistant {
			content = textOf(message.Content)
			break
		}
	}
	if content == "" {
		return nil
	}
	partID, err := a.streamPartID(ctx, sc, step, fmt.Sprintf("generate-text-part-id-%s", sc.MessageID))
	if err != nil {
		return err
	}
	sc.PublishEvent(ctx, EventPartCreated, map[string]any{
		"partId": partID, "runId": sc.RunID, "messageId": sc.MessageID,
		"type": "text", "metadata": map[string]any{"agentName": a.Name},
	})
	for _, delta := range sc.ChunkContent(content) {
		sc.PublishEvent(ctx, EventTextDelta, map[string]any{
			"partId": partID, "messageId": sc.MessageID, "delta": delta,
		})
	}
	sc.PublishEvent(ctx, EventPartCompleted, map[string]any{
		"partId": partID, "runId": sc.RunID, "messageId": sc.MessageID,
		"type": "text", "finalContent": content,
	})
	return nil
}

// streamPartID mints a streaming part id inside a durable step so it is
// replay-stable (a re-minted id would re-publish the part's deltas).
func (a *Agent[T]) streamPartID(ctx context.Context, sc *StreamingContext, step durable.Step, stepID string) (string, error) {
	return durable.Run(ctx, step, stepID, func(ctx context.Context) (string, error) {
		return sc.GeneratePartID(), nil
	})
}

// invokeTools executes every tool call in the inference output, in order,
// streaming tool-call arguments and outputs when a context is present.
func (a *Agent[T]) invokeTools(ctx context.Context, msgs []Message, run *NetworkRun[T], sc *StreamingContext, step durable.Step) ([]Message, error) {
	var output []Message

	for _, msg := range msgs {
		if msg.Type != MessageToolCall {
			continue
		}
		// callIdx MUST key the part-id step ids below: the model commonly
		// calls the same tool twice in one inference, so keying on the tool
		// NAME alone collides and the id drifts across replays, delivering
		// the part's deltas twice. The index is deterministic; the name is
		// kept for readability only.
		for callIdx, call := range msg.Tools {
			idx, ok := a.toolIndex[call.Name]
			if !ok {
				return nil, fmt.Errorf("agentkit: inference requested a non-existent tool: %s", call.Name)
			}
			tool := a.tools[idx]

			argsJSON := string(call.Input)
			if argsJSON == "" {
				argsJSON = "{}"
			}

			if sc != nil {
				partID, err := a.streamPartID(ctx, sc, step,
					fmt.Sprintf("generate-tool-part-id-%s-%s-%d", sc.MessageID, call.Name, callIdx))
				if err != nil {
					return nil, err
				}
				sc.PublishEvent(ctx, EventPartCreated, map[string]any{
					"partId": partID, "runId": sc.RunID, "messageId": sc.MessageID,
					"type": "tool-call", "metadata": map[string]any{"toolName": call.Name, "agentName": a.Name},
				})
				for i, delta := range sc.ChunkContent(argsJSON) {
					data := map[string]any{
						"partId": partID, "delta": delta, "messageId": sc.MessageID,
					}
					// toolName rides only the first delta (TS drops the
					// undefined key on later chunks).
					if i == 0 {
						data["toolName"] = call.Name
					}
					sc.PublishEvent(ctx, EventToolArgsDelta, data)
				}
				sc.PublishEvent(ctx, EventPartCompleted, map[string]any{
					"partId": partID, "runId": sc.RunID, "messageId": sc.MessageID,
					"type": "tool-call", "finalContent": json.RawMessage(argsJSON),
					"metadata": map[string]any{"toolName": call.Name, "agentName": a.Name},
				})
			}

			result := a.runToolHandler(ctx, tool, call, run, step)

			content, err := result.marshal()
			if err != nil {
				return nil, fmt.Errorf("agentkit: serialize result of tool %q: %w", call.Name, err)
			}

			if sc != nil {
				partID, err := a.streamPartID(ctx, sc, step,
					fmt.Sprintf("generate-output-part-id-%s-%s-%d", sc.MessageID, call.Name, callIdx))
				if err != nil {
					return nil, err
				}
				sc.PublishEvent(ctx, EventPartCreated, map[string]any{
					"partId": partID, "runId": sc.RunID, "messageId": sc.MessageID,
					"type": "tool-output", "metadata": map[string]any{"toolName": call.Name, "agentName": a.Name},
				})
				for _, delta := range sc.ChunkContent(string(content)) {
					sc.PublishEvent(ctx, EventToolOutDelta, map[string]any{
						"partId": partID, "delta": delta, "messageId": sc.MessageID,
					})
				}
				sc.PublishEvent(ctx, EventPartCompleted, map[string]any{
					"partId": partID, "runId": sc.RunID, "messageId": sc.MessageID,
					"type": "tool-output", "finalContent": json.RawMessage(content),
					"metadata": map[string]any{"toolName": call.Name, "agentName": a.Name},
				})
			}

			output = append(output, Message{
				Type: MessageToolResult, Role: RoleToolResult,
				Tool:       &ToolMessage{Type: "tool", ID: call.ID, Name: call.Name, Input: call.Input},
				Content:    RawContent(content),
				StopReason: StopTool,
			})
		}
	}
	return output, nil
}

// ToolHandlerResult normalizes a handler's return/throw into the shape fed
// back to the model: {data} on success, {error} on failure. Errors are
// captured, never propagated — the durable step records a SUCCESS returning
// the error, so the failing side effect is not retried and the model sees
// the same result on replay.
//
// Data is held as raw JSON from the moment of capture (parity decision 12):
// a map round-trip through the step would re-order keys.
type ToolHandlerResult struct {
	data json.RawMessage
	err  json.RawMessage
}

// IsError reports whether the handler failed.
func (r ToolHandlerResult) IsError() bool { return r.err != nil }

// Data returns the success payload as raw JSON (nil on error).
func (r ToolHandlerResult) Data() json.RawMessage { return r.data }

func (r ToolHandlerResult) marshal() (json.RawMessage, error) {
	if r.err != nil {
		return json.RawMessage(`{"error":` + string(r.err) + `}`), nil
	}
	data := r.data
	if data == nil {
		data = json.RawMessage("null")
	}
	return json.RawMessage(`{"data":` + string(data) + `}`), nil
}

func (r ToolHandlerResult) MarshalJSON() ([]byte, error) { return r.marshal() }

func (r *ToolHandlerResult) UnmarshalJSON(b []byte) error {
	var s struct {
		Data  json.RawMessage `json:"data"`
		Error json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	r.data, r.err = s.Data, s.Error
	return nil
}

// SerializedError is the {error} payload shape (mirrors the TS
// serializeError output that consumers read `.message` from).
type SerializedError struct {
	Name    string `json:"name"`
	Message string `json:"message"`
}

func dataResult(v any) ToolHandlerResult {
	b, err := jsonutil.Marshal(v)
	if err != nil {
		return errResult(fmt.Errorf("serialize tool result: %w", err))
	}
	return ToolHandlerResult{data: b}
}

func errResult(err error) ToolHandlerResult {
	b, merr := jsonutil.Marshal(SerializedError{Name: "Error", Message: err.Error()})
	if merr != nil {
		b = json.RawMessage(`{"name":"Error","message":"unserializable error"}`)
	}
	return ToolHandlerResult{err: b}
}

// runToolHandler invokes one tool-call handler — durable by default.
//
// Inngest re-executes the function body on every step boundary, so an
// unwrapped handler re-fires on each replay (edit_file double-applies, an
// image tool re-bills). The handler therefore runs inside ONE durable step
// under a deterministic id: memoized inferences replay the same tool calls
// in the same order, so the per-run counter assigns the Nth call the same
// id on every replay.
//
// State mutations: a handler may mutate State.Data INSIDE the step; on
// replay the memoized body is skipped and the mutation would be lost, so
// the post-handler snapshot is memoized in the step payload and re-applied
// OUTSIDE the step on every execution.
//
// ManualStep tools run inline and own their durability (WaitForEvent,
// Invoke, multi-checkpoint subagents). Outside Inngest the durable seam
// executes inline on its own — no caller-side detection needed.
func (a *Agent[T]) runToolHandler(ctx context.Context, tool Tool[T], call ToolMessage, run *NetworkRun[T], step durable.Step) ToolHandlerResult {
	invoke := func(ctx context.Context) ToolHandlerResult {
		r, err := tool.Handler(ctx, call.Input, ToolOptions[T]{Agent: a, Network: run, State: run.State, Step: step})
		if err != nil {
			return errResult(err)
		}
		if r == nil {
			return dataResult(fmt.Sprintf("%s successfully executed", call.Name))
		}
		return dataResult(r)
	}

	if tool.ManualStep {
		return invoke(ctx)
	}

	type memoized struct {
		Result ToolHandlerResult `json:"result"`
		// StatePatch is carried only when the tool actually mutated state —
		// every step's output counts against Inngest's ~4MB-per-run total.
		StatePatch json.RawMessage `json:"statePatch,omitempty"`
	}

	index := run.State.nextDurableToolCallIndex()
	stepID := fmt.Sprintf("%s/tool/%s/%d", a.Name, call.Name, index)

	m, err := durable.Run(ctx, step, stepID, func(ctx context.Context) (memoized, error) {
		before, beforeErr := jsonutil.Marshal(run.State.Data)
		result := invoke(ctx)
		after, afterErr := jsonutil.Marshal(run.State.Data)
		var patch json.RawMessage
		if afterErr == nil && (beforeErr != nil || !bytes.Equal(before, after)) {
			patch = after
		}
		return memoized{Result: result, StatePatch: patch}, nil
	})
	if err != nil {
		// Step-infrastructure failure (never a handler error — those are
		// captured as {error}).
		return errResult(err)
	}

	// Re-apply any state delta OUTSIDE the step on EVERY execution —
	// including replays, where the memoized body did not run.
	if m.StatePatch != nil {
		var data T
		if err := json.Unmarshal(m.StatePatch, &data); err != nil {
			return errResult(fmt.Errorf("re-apply state patch: %w", err))
		}
		run.State.ImportData(data)
	}

	return m.Result
}

// agentPrompt builds the agent's full prompt: system first (with the
// user's per-turn system prompt appended), then user input, then the
// assistant steer. Network state is NOT part of the prompt.
func (a *Agent[T]) agentPrompt(ctx context.Context, inputContent, userSystemPrompt string, run *NetworkRun[T]) ([]Message, error) {
	systemContent := a.System
	if a.SystemFn != nil {
		s, err := a.SystemFn(ctx, run)
		if err != nil {
			return nil, err
		}
		systemContent = s
	}
	if userSystemPrompt != "" {
		systemContent = systemContent + "\n\n" + userSystemPrompt
	}

	messages := []Message{{Type: MessageText, Role: RoleSystem, Content: TextContent(systemContent)}}
	if inputContent != "" {
		messages = append(messages, Message{Type: MessageText, Role: RoleUser, Content: TextContent(inputContent)})
	}
	if a.Assistant != "" {
		messages = append(messages, Message{Type: MessageText, Role: RoleAssistant, Content: TextContent(a.Assistant)})
	}
	return messages, nil
}

// --- MCP ---

// initMCP fetches tools from the agent's MCP servers on first run.
func (a *Agent[T]) initMCP(ctx context.Context) error {
	if len(a.MCPServers) == 0 {
		return nil
	}
	a.mcpMu.Lock()
	defer a.mcpMu.Unlock()
	if len(a.mcpClients) >= len(a.MCPServers) {
		return nil
	}
	for _, server := range a.MCPServers {
		if err := a.listMCPTools(ctx, server); err != nil {
			// Mirror TS: a broken MCP server degrades the tool list, it
			// does not fail the run.
			slog.WarnContext(ctx, "agentkit: error listing mcp tools",
				"server", server.Name, "error", err)
		}
	}
	return nil
}

// listMCPTools registers all tools advertised by one MCP server. Unlike TS
// — which converts each JSON Schema to Zod — the server's schema is passed
// through verbatim.
func (a *Agent[T]) listMCPTools(ctx context.Context, server MCPServer) error {
	client, err := mcpConnect(ctx, a.Name, server)
	if err != nil {
		return err
	}
	a.mcpClients = append(a.mcpClients, client)

	list, err := client.ListTools(ctx, nil)
	if err != nil {
		return err
	}
	for _, t := range list.Tools {
		name := server.Name + "-" + t.Name
		remoteName := t.Name
		a.addTool(Tool[T]{
			Name:        name,
			Description: t.Description,
			InputSchema: t.InputSchema,
			MCP: &MCPToolSource{
				Server: server,
				Tool:   MCPToolDef{Name: t.Name, Description: t.Description, InputSchema: t.InputSchema},
			},
			// No self-wrapping: runToolHandler wraps every tool in a durable
			// step with a replay-stable per-call id.
			Handler: func(ctx context.Context, input json.RawMessage, _ ToolOptions[T]) (any, error) {
				var args map[string]any
				if len(input) > 0 {
					if err := json.Unmarshal(input, &args); err != nil {
						return nil, fmt.Errorf("invalid input for MCP tool %q: %w", remoteName, err)
					}
				}
				result, err := client.CallTool(ctx, remoteName, args)
				if err != nil {
					return nil, err
				}
				// Content blocks are raw JSON — passed through verbatim.
				return result.Content, nil
			},
		})
	}
	return nil
}

// mcpConnect builds and connects a goai MCP client for a server config.
func mcpConnect(ctx context.Context, agentName string, server MCPServer) (*mcp.Client, error) {
	var transport mcp.Transport
	switch server.Transport.Type {
	case MCPTransportStreamableHTTP:
		var opts []mcp.HTTPTransportOption
		if len(server.Transport.Headers) > 0 {
			opts = append(opts, mcp.WithHTTPHeaders(server.Transport.Headers))
		}
		transport = mcp.NewHTTPTransport(server.Transport.URL, opts...)
	case MCPTransportSSE:
		var opts []mcp.SSETransportOption
		if len(server.Transport.Headers) > 0 {
			opts = append(opts, mcp.WithSSEHeaders(server.Transport.Headers))
		}
		transport = mcp.NewSSETransport(server.Transport.URL, opts...)
	case MCPTransportStdio:
		var opts []mcp.StdioOption
		if len(server.Transport.Env) > 0 {
			opts = append(opts, mcp.WithStdioEnv(server.Transport.Env))
		}
		transport = mcp.NewStdioTransport(server.Transport.Command, server.Transport.Args, opts...)
	case MCPTransportWS:
		return nil, fmt.Errorf("agentkit: websocket MCP transport is not supported by goai/mcp (server %q)", server.Name)
	default:
		return nil, fmt.Errorf("agentkit: unknown MCP transport type %q (server %q)", server.Transport.Type, server.Name)
	}

	client := mcp.NewClient(agentName, "1.0.0", mcp.WithTransport(transport))
	if err := client.Connect(ctx); err != nil {
		return nil, fmt.Errorf("connect to MCP server %q: %w", server.Name, err)
	}
	return client, nil
}
