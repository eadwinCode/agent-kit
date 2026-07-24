package agentkit

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/zendev-sh/goai/provider"

	"github.com/eadwinCode/agent-kit/go/durable"
)

func toolCallResult(toolName string, args string) *provider.GenerateResult {
	return &provider.GenerateResult{
		Text:         "calling " + toolName,
		FinishReason: provider.FinishToolCalls,
		ToolCalls:    []provider.ToolCall{{ID: "call_1", Name: toolName, Input: json.RawMessage(args)}},
		Usage:        provider.Usage{InputTokens: 10, OutputTokens: 5, TotalTokens: 15},
	}
}

func stopResult(text string) *provider.GenerateResult {
	return &provider.GenerateResult{
		Text:         text,
		FinishReason: provider.FinishStop,
		Usage:        provider.Usage{InputTokens: 10, OutputTokens: 5, TotalTokens: 15},
	}
}

func TestAgentRunPromptAssembly(t *testing.T) {
	fake := &fakeModel{id: "m", result: stopResult("hi there")}
	a := NewAgent(AgentConfig[shape]{
		Name:      "helper",
		System:    "You are helpful.",
		Assistant: "Understood:",
		Model:     fake,
	})

	res, err := a.Run(context.Background(), "help me", &RunOptions[shape]{
		UserMessage: &UserMessage{ID: "u1", Content: "help me", Role: RoleUser, SystemPrompt: "Be terse."},
	})
	if err != nil {
		t.Fatal(err)
	}

	// System prompt + per-turn system prompt merged, user, assistant steer.
	if fake.lastParams.System != "You are helpful.\n\nBe terse." {
		t.Errorf("system = %q", fake.lastParams.System)
	}
	if len(fake.lastParams.Messages) != 2 {
		t.Fatalf("want user+assistant messages, got %d", len(fake.lastParams.Messages))
	}
	if fake.lastParams.Messages[0].Role != provider.RoleUser ||
		fake.lastParams.Messages[1].Role != provider.RoleAssistant {
		t.Errorf("message roles wrong: %+v", fake.lastParams.Messages)
	}

	if res.AgentName != "helper" || len(res.Output) != 1 {
		t.Errorf("result wrong: %+v", res)
	}
	if s, _ := res.Output[0].Content.AsString(); s != "hi there" {
		t.Errorf("output = %q", s)
	}
	// Raw carries the serialized inference result for billing.
	if !strings.Contains(res.Raw, `"input_tokens":10`) {
		t.Errorf("raw missing usage: %s", res.Raw)
	}
}

func TestAgentRunInvokesToolAndRecordsResult(t *testing.T) {
	fake := &fakeModel{id: "m", result: toolCallResult("set_sku", `{"sku": 42}`)}
	a := NewAgent(AgentConfig[shape]{
		Name:   "agent",
		System: "sys",
		Model:  fake,
		Tools:  []Tool[shape]{newSKUTool()},
	})

	st := NewState(StateConfig[shape]{})
	res, err := a.Run(context.Background(), "set sku to 42", &RunOptions[shape]{State: st})
	if err != nil {
		t.Fatal(err)
	}

	// Tool executed: state mutated (via the memoized patch re-apply).
	if st.Data.SKU != 42 {
		t.Errorf("tool did not mutate state: %+v", st.Data)
	}

	// Tool result recorded with the {data: ...} payload shape.
	if len(res.ToolCalls) != 1 {
		t.Fatalf("want 1 tool result, got %d", len(res.ToolCalls))
	}
	tr := res.ToolCalls[0]
	if tr.Type != MessageToolResult || tr.Tool.Name != "set_sku" || tr.Tool.ID != "call_1" {
		t.Errorf("tool result wrong: %+v", tr)
	}
	if !strings.HasPrefix(string(tr.Content.Raw()), `{"data":`) {
		t.Errorf("content must be {data:...}-wrapped: %s", tr.Content.Raw())
	}
	// Default single iteration: one inference even though it returned tool calls.
	if fake.calls != 1 {
		t.Errorf("default MaxIterPerRun must be 1, made %d calls", fake.calls)
	}
}

func TestAgentRunMultiIteration(t *testing.T) {
	fake := &fakeModel{id: "m", queue: []*provider.GenerateResult{
		toolCallResult("set_sku", `{"sku": 1}`),
		stopResult("done"),
	}}
	a := NewAgent(AgentConfig[shape]{
		Name: "agent", System: "sys", Model: fake,
		Tools:         []Tool[shape]{newSKUTool()},
		MaxIterPerRun: 3,
	})

	res, err := a.Run(context.Background(), "go", nil)
	if err != nil {
		t.Fatal(err)
	}
	if fake.calls != 2 {
		t.Fatalf("want 2 inferences (tool round + stop), got %d", fake.calls)
	}
	// Second call's messages must include the tool round trip: assistant
	// tool_call followed by the tool result.
	second := fake.allParams[1]
	var haveCall, haveResult bool
	for _, m := range second.Messages {
		for _, p := range m.Content {
			switch p.Type {
			case provider.PartToolCall:
				haveCall = true
			case provider.PartToolResult:
				haveResult = true
			}
		}
	}
	if !haveCall || !haveResult {
		t.Errorf("iteration 2 must see the full tool round trip (call=%v result=%v)", haveCall, haveResult)
	}
	if s, _ := res.Output[0].Content.AsString(); s != "done" {
		t.Errorf("final output = %q", s)
	}
}

func TestAgentRunToolErrorsAreCaptured(t *testing.T) {
	failing := NewTool[shape]("boom", "always fails",
		func(ctx context.Context, in struct{}, opts ToolOptions[shape]) (any, error) {
			return nil, context.DeadlineExceeded
		})
	fake := &fakeModel{id: "m", result: toolCallResult("boom", `{}`)}
	a := NewAgent(AgentConfig[shape]{Name: "agent", System: "s", Model: fake, Tools: []Tool[shape]{failing}})

	res, err := a.Run(context.Background(), "go", nil)
	if err != nil {
		t.Fatalf("handler errors must be captured, not propagated: %v", err)
	}
	content := string(res.ToolCalls[0].Content.Raw())
	if !strings.HasPrefix(content, `{"error":`) || !strings.Contains(content, "deadline exceeded") {
		t.Errorf("error result shape wrong: %s", content)
	}
}

func TestAgentRunUnknownToolFails(t *testing.T) {
	fake := &fakeModel{id: "m", result: toolCallResult("nonexistent", `{}`)}
	a := NewAgent(AgentConfig[shape]{Name: "agent", System: "s", Model: fake, Tools: []Tool[shape]{newSKUTool()}})
	_, err := a.Run(context.Background(), "go", nil)
	if err == nil || !strings.Contains(err.Error(), "non-existent tool") {
		t.Fatalf("want non-existent tool error, got %v", err)
	}
}

func TestAgentLifecycleHooks(t *testing.T) {
	var order []string
	fake := &fakeModel{id: "m", result: stopResult("raw answer")}
	a := NewAgent(AgentConfig[shape]{
		Name: "agent", System: "sys", Model: fake,
		Lifecycle: &Lifecycle[shape]{
			OnStart: func(ctx context.Context, args LifecycleBefore[shape]) (LifecycleStartResult, error) {
				order = append(order, "onStart")
				// Adjust the prompt: swap the user message content.
				prompt := args.Prompt
				prompt[len(prompt)-1] = Message{Type: MessageText, Role: RoleUser, Content: TextContent("modified input")}
				return LifecycleStartResult{Prompt: prompt, History: args.History}, nil
			},
			OnResponse: func(ctx context.Context, args LifecycleResult[shape]) (*AgentResult, error) {
				order = append(order, "onResponse")
				args.Result.Output = []Message{{Type: MessageText, Role: RoleAssistant, Content: TextContent("moderated"), StopReason: StopStop}}
				return args.Result, nil
			},
			OnFinish: func(ctx context.Context, args LifecycleResult[shape]) (*AgentResult, error) {
				order = append(order, "onFinish")
				return args.Result, nil
			},
		},
	})

	res, err := a.Run(context.Background(), "original input", nil)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(order, ",") != "onStart,onResponse,onFinish" {
		t.Errorf("hook order = %v", order)
	}
	// OnStart's prompt modification reached the model.
	lastMsg := fake.lastParams.Messages[len(fake.lastParams.Messages)-1]
	if lastMsg.Content[0].Text != "modified input" {
		t.Errorf("onStart prompt modification lost: %+v", lastMsg)
	}
	if s, _ := res.Output[0].Content.AsString(); s != "moderated" {
		t.Errorf("onResponse moderation lost: %q", s)
	}
}

func TestAgentLifecycleOnStartStop(t *testing.T) {
	fake := &fakeModel{id: "m", result: stopResult("never")}
	a := NewAgent(AgentConfig[shape]{
		Name: "agent", System: "sys", Model: fake,
		Lifecycle: &Lifecycle[shape]{
			OnStart: func(ctx context.Context, args LifecycleBefore[shape]) (LifecycleStartResult, error) {
				return LifecycleStartResult{Prompt: args.Prompt, History: args.History, Stop: true}, nil
			},
		},
	})
	res, err := a.Run(context.Background(), "go", nil)
	if err != nil {
		t.Fatal(err)
	}
	if fake.calls != 0 {
		t.Error("Stop=true must prevent the model call")
	}
	if len(res.Output) != 0 {
		t.Errorf("stopped run must return the empty initial result: %+v", res)
	}
}

func TestAgentSystemFn(t *testing.T) {
	fake := &fakeModel{id: "m", result: stopResult("x")}
	a := NewAgent(AgentConfig[shape]{
		Name: "agent", Model: fake,
		SystemFn: func(ctx context.Context, run *NetworkRun[shape]) (string, error) {
			return "dynamic for " + run.Network.Name, nil
		},
	})
	if _, err := a.Run(context.Background(), "go", nil); err != nil {
		t.Fatal(err)
	}
	if fake.lastParams.System != "dynamic for default" {
		t.Errorf("system fn not applied: %q", fake.lastParams.System)
	}
}

func TestAgentNoModelFails(t *testing.T) {
	a := NewAgent(AgentConfig[shape]{Name: "agent", System: "s"})
	if _, err := a.Run(context.Background(), "go", nil); err == nil || !strings.Contains(err.Error(), "no model") {
		t.Fatalf("want no-model error, got %v", err)
	}
}

func TestAgentHistoryHooks(t *testing.T) {
	var calls []string
	loaded := resultWith("previous", "earlier answer")
	h := &HistoryConfig[shape]{
		CreateThread: func(ctx context.Context, hctx HistoryContext[shape]) (CreateThreadResult, error) {
			calls = append(calls, "createThread")
			return CreateThreadResult{ThreadID: "th_new"}, nil
		},
		Get: func(ctx context.Context, hctx HistoryContext[shape]) ([]*AgentResult, error) {
			calls = append(calls, "get:"+hctx.ThreadID)
			return []*AgentResult{loaded}, nil
		},
		AppendResults: func(ctx context.Context, hctx HistoryContext[shape], newResults []*AgentResult) error {
			calls = append(calls, "append")
			if len(newResults) != 0 {
				t.Errorf("standalone agent.Run does not append results to state; got %d new results", len(newResults))
			}
			return nil
		},
	}

	fake := &fakeModel{id: "m", result: stopResult("answer")}
	a := NewAgent(AgentConfig[shape]{Name: "agent", System: "sys", Model: fake, History: h})

	if _, err := a.Run(context.Background(), "hello", nil); err != nil {
		t.Fatal(err)
	}
	// createThread minted th_new; get loaded history for it; the loaded
	// result became model-visible history.
	if strings.Join(calls, ",") != "createThread,get:th_new" {
		t.Errorf("history hook calls = %v", calls)
	}
	foundHistory := false
	for _, m := range fake.lastParams.Messages {
		for _, p := range m.Content {
			if strings.Contains(p.Text, "earlier answer") {
				foundHistory = true
			}
		}
	}
	if !foundHistory {
		t.Error("loaded history did not reach the model")
	}
}

func TestAgentHistoryClientAuthoritativeSkipsGet(t *testing.T) {
	getCalled := false
	h := &HistoryConfig[shape]{
		Get: func(ctx context.Context, hctx HistoryContext[shape]) ([]*AgentResult, error) {
			getCalled = true
			return nil, nil
		},
	}
	fake := &fakeModel{id: "m", result: stopResult("x")}
	a := NewAgent(AgentConfig[shape]{Name: "agent", System: "s", Model: fake, History: h})

	// Client supplied results → Get must be skipped.
	st := NewState(StateConfig[shape]{ThreadID: "th_1", Results: []*AgentResult{resultWith("a", "client-held")}})
	if _, err := a.Run(context.Background(), "go", &RunOptions[shape]{State: st}); err != nil {
		t.Fatal(err)
	}
	if getCalled {
		t.Error("client-authoritative mode must skip History.Get")
	}
}

func TestPersistResultsWrapsInOneStep(t *testing.T) {
	appended := 0
	h := &HistoryConfig[shape]{
		AppendResults: func(ctx context.Context, hctx HistoryContext[shape], newResults []*AgentResult) error {
			appended += len(newResults)
			return nil
		},
	}
	st := NewState(StateConfig[shape]{ThreadID: "th"})
	cfg := PersistConfig[shape]{State: st, History: h, Input: "in", Step: durable.Inline{}}

	if err := PersistResults(context.Background(), cfg, []*AgentResult{resultWith("a", "one")}, IncrementalAppendStepID(0)); err != nil {
		t.Fatal(err)
	}
	if appended != 1 {
		t.Errorf("append not called: %d", appended)
	}
	if id := IncrementalAppendStepID(3); id != "agent-kit/history/append-results/3" {
		t.Errorf("incremental id format: %s", id)
	}
	// Empty results: no-op, no error.
	if err := PersistResults(context.Background(), cfg, nil, FinalAppendStepID); err != nil {
		t.Fatal(err)
	}
}

func TestRoutingAgent(t *testing.T) {
	fake := &fakeModel{id: "m", result: stopResult("route: helper")}
	ra := NewRoutingAgent(AgentConfig[shape]{Name: "router", System: "route requests", Model: fake},
		func(ctx context.Context, args LifecycleResult[shape]) []string {
			return []string{"helper"}
		})
	res, err := ra.Run(context.Background(), "pick an agent", nil)
	if err != nil {
		t.Fatal(err)
	}
	next := ra.OnRoute(context.Background(), LifecycleResult[shape]{Result: res})
	if len(next) != 1 || next[0] != "helper" {
		t.Errorf("OnRoute = %v", next)
	}
}
