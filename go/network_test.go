package agentkit

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/zendev-sh/goai/provider"

	"github.com/eadwinCode/agent-kit/go/internal/jsonutil"
)

func mkAgent(name, system string, model provider.LanguageModel, tools ...Tool[shape]) *Agent[shape] {
	return NewAgent(AgentConfig[shape]{Name: name, System: system, Model: model, Tools: tools})
}

// scriptRouter routes through the given agent lists one call at a time,
// then ends the run.
func scriptRouter(script ...*RouterResult[shape]) *Router[shape] {
	i := 0
	return &Router[shape]{Fn: func(ctx context.Context, args RouterArgs[shape]) (*RouterResult[shape], error) {
		if i >= len(script) {
			return nil, nil
		}
		r := script[i]
		i++
		return r, nil
	}}
}

func TestNetworkRunFnRouterLoop(t *testing.T) {
	fakeA := &fakeModel{id: "m", result: stopResult("from a")}
	fakeB := &fakeModel{id: "m", result: stopResult("from b")}
	a := mkAgent("a", "agent a", fakeA)
	b := mkAgent("b", "agent b", fakeB)

	n := NewNetwork(NetworkConfig[shape]{
		Name:   "net",
		Agents: []*Agent[shape]{a, b},
		Router: scriptRouter(RouteTo(a), RouteTo(b)), // a, then b, then done
	})

	run, err := n.Run(context.Background(), "do the thing", nil)
	if err != nil {
		t.Fatal(err)
	}

	results := run.State.Results()
	if len(results) != 2 || results[0].AgentName != "a" || results[1].AgentName != "b" {
		t.Fatalf("want results from a then b, got %+v", results)
	}
	if run.CallCount() != 2 {
		t.Errorf("CallCount = %d", run.CallCount())
	}
	// Durably-minted message ids were assigned (checksum depends on them).
	if results[0].ID == "" || results[0].ID == results[1].ID {
		t.Errorf("agent message ids missing or colliding: %q vs %q", results[0].ID, results[1].ID)
	}
	// Agent b's inference saw a's result as history.
	foundHistory := false
	for _, m := range fakeB.lastParams.Messages {
		for _, p := range m.Content {
			if strings.Contains(p.Text, "from a") {
				foundHistory = true
			}
		}
	}
	if !foundHistory {
		t.Error("network history did not flow between agents")
	}
	// Template state untouched; run state isolated.
	if len(n.defaultState.results) != 0 {
		t.Error("network template state was mutated by a run")
	}
}

func TestNetworkMaxIterCaps(t *testing.T) {
	fake := &fakeModel{id: "m", result: stopResult("x")}
	a := mkAgent("a", "sys", fake)
	// Router always schedules a again — MaxIter must break the loop.
	n := NewNetwork(NetworkConfig[shape]{
		Name:    "net",
		Agents:  []*Agent[shape]{a},
		MaxIter: 3,
		Router: &Router[shape]{Fn: func(ctx context.Context, args RouterArgs[shape]) (*RouterResult[shape], error) {
			return RouteTo(args.Network.agents[0]), nil
		}},
	})
	run, err := n.Run(context.Background(), "loop forever", nil)
	if err != nil {
		t.Fatal(err)
	}
	if run.CallCount() != 3 {
		t.Errorf("MaxIter=3 but CallCount = %d", run.CallCount())
	}
}

func TestNetworkStopWhen(t *testing.T) {
	fake := &fakeModel{id: "m", result: stopResult("x")}
	a := mkAgent("a", "sys", fake)
	n := NewNetwork(NetworkConfig[shape]{
		Name:   "net",
		Agents: []*Agent[shape]{a},
		Router: &Router[shape]{Fn: func(ctx context.Context, args RouterArgs[shape]) (*RouterResult[shape], error) {
			return RouteTo(args.Network.agents[0]), nil
		}},
		StopWhen: func(ctx context.Context, args StopWhenArgs[shape]) (*NetworkStop, error) {
			if args.CallCount >= 2 {
				return &NetworkStop{Reason: "budget", Metadata: map[string]any{"cap": 2}}, nil
			}
			return nil, nil
		},
	})
	run, err := n.Run(context.Background(), "go", nil)
	if err != nil {
		t.Fatal(err)
	}
	if run.CallCount() != 2 {
		t.Errorf("stopWhen at 2 but CallCount = %d", run.CallCount())
	}
	if run.StoppedBy == nil || run.StoppedBy.Reason != "budget" {
		t.Errorf("StoppedBy = %+v", run.StoppedBy)
	}
}

func TestNetworkAgenticRouting(t *testing.T) {
	// The DEFAULT router: its model first select_agent's the worker, then
	// done's the run. The worker answers in between.
	routerModel := &fakeModel{id: "claude-router", queue: []*provider.GenerateResult{
		toolCallResult("select_agent", `{"name":"worker"}`),
		toolCallResult("done", `{"summary":"all wrapped up"}`),
	}}
	workerModel := &fakeModel{id: "m", result: stopResult("worker did the job")}

	worker := mkAgent("worker", "does the actual work", workerModel)
	n := NewNetwork(NetworkConfig[shape]{
		Name:         "net",
		Agents:       []*Agent[shape]{worker},
		DefaultModel: routerModel, // default router runs on the network default model
	})

	run, err := n.Run(context.Background(), "please do the job", nil)
	if err != nil {
		t.Fatal(err)
	}

	results := run.State.Results()
	if len(results) != 1 || results[0].AgentName != "worker" {
		t.Fatalf("want exactly the worker's result, got %+v", results)
	}
	// The routing agent's system prompt advertised the worker.
	sys := routerModel.allParams[0].System
	if !strings.Contains(sys, "<agents>") || !strings.Contains(sys, "<name>worker</name>") {
		t.Errorf("router system prompt missing agent listing:\n%s", sys)
	}
	// Routing inferences forced a tool choice.
	if routerModel.allParams[0].ToolChoice != "required" {
		t.Errorf("router tool choice = %q, want required (mapped from \"any\")", routerModel.allParams[0].ToolChoice)
	}
	if routerModel.calls != 2 {
		t.Errorf("router model calls = %d, want 2 (select_agent, done)", routerModel.calls)
	}
}

func TestNetworkAgenticRoutingRejectsUnknownAgent(t *testing.T) {
	routerModel := &fakeModel{id: "claude-router", queue: []*provider.GenerateResult{
		toolCallResult("select_agent", `{"name":"ghost"}`),
	}}
	worker := mkAgent("worker", "w", &fakeModel{id: "m", result: stopResult("x")})
	n := NewNetwork(NetworkConfig[shape]{
		Name: "net", Agents: []*Agent[shape]{worker}, DefaultModel: routerModel,
	})

	run, err := n.Run(context.Background(), "go", nil)
	if err != nil {
		t.Fatal(err)
	}
	// select_agent("ghost") errors inside the tool; OnRoute finds no valid
	// data payload → run ends with zero agent calls.
	if run.CallCount() != 0 {
		t.Errorf("unknown agent must end the run, CallCount = %d", run.CallCount())
	}
}

func TestNetworkRouterIntroducedAgent(t *testing.T) {
	outsider := mkAgent("outsider", "not in the template", &fakeModel{id: "m", result: stopResult("outsider ran")})
	n := NewNetwork(NetworkConfig[shape]{
		Name:   "net",
		Agents: []*Agent[shape]{mkAgent("inside", "in", &fakeModel{id: "m", result: stopResult("x")})},
		Router: scriptRouter(RouteTo(outsider)),
	})
	run, err := n.Run(context.Background(), "go", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(run.State.Results()) != 1 || run.State.Results()[0].AgentName != "outsider" {
		t.Fatalf("router-introduced agent did not run: %+v", run.State.Results())
	}
	// Registered on the run, not the template.
	if _, ok := run.AgentByName("outsider"); !ok {
		t.Error("outsider not registered on the run")
	}
	if _, ok := n.AgentByName("outsider"); ok {
		t.Error("outsider leaked into the immutable template")
	}
}

func TestNetworkNoRouterNoModelFails(t *testing.T) {
	n := NewNetwork(NetworkConfig[shape]{
		Name:   "net",
		Agents: []*Agent[shape]{mkAgent("a", "s", &fakeModel{id: "m", result: stopResult("x")})},
	})
	_, err := n.Run(context.Background(), "go", nil)
	if err == nil || !strings.Contains(err.Error(), "no router or default model") {
		t.Fatalf("want router/model error, got %v", err)
	}
}

func TestNetworkHistoryFlow(t *testing.T) {
	var events []string
	var incrementalCounts, finalCount int
	h := &HistoryConfig[shape]{
		CreateThread: func(ctx context.Context, hctx HistoryContext[shape]) (CreateThreadResult, error) {
			events = append(events, "createThread")
			return CreateThreadResult{ThreadID: "th_net"}, nil
		},
		AppendUserMessage: func(ctx context.Context, hctx HistoryContext[shape], msg UserMessageRecord) error {
			events = append(events, "appendUser:"+msg.ID)
			return nil
		},
		AppendResults: func(ctx context.Context, hctx HistoryContext[shape], newResults []*AgentResult) error {
			events = append(events, "append")
			// Distinguish incremental (1 result) from final backstop (all new).
			if len(newResults) == 1 {
				incrementalCounts++
			} else {
				finalCount = len(newResults)
			}
			return nil
		},
	}

	a := mkAgent("a", "sys", &fakeModel{id: "m", result: stopResult("one")})
	b := mkAgent("b", "sys", &fakeModel{id: "m", result: stopResult("two")})
	n := NewNetwork(NetworkConfig[shape]{
		Name: "net", Agents: []*Agent[shape]{a, b},
		Router:  scriptRouter(RouteTo(a), RouteTo(b)),
		History: h,
	})

	run, err := n.Run(context.Background(), "hello", &NetworkRunOptions[shape]{
		UserMessage: &UserMessage{ID: "client_msg_1", Content: "hello", Role: RoleUser},
	})
	if err != nil {
		t.Fatal(err)
	}
	if run.State.ThreadID != "th_net" {
		t.Errorf("threadId = %q", run.State.ThreadID)
	}
	// User message persisted up front with the client's canonical id.
	if events[0] != "createThread" || events[1] != "appendUser:client_msg_1" {
		t.Errorf("event order wrong: %v", events)
	}
	// Two incremental appends (one per iteration) + one final backstop with
	// both results.
	if incrementalCounts != 2 {
		t.Errorf("incremental appends = %d, want 2", incrementalCounts)
	}
	if finalCount != 2 {
		t.Errorf("final backstop got %d results, want 2", finalCount)
	}
}

func TestNetworkEnabledLifecycleFiltersAgents(t *testing.T) {
	disabled := NewAgent(AgentConfig[shape]{
		Name: "disabled", System: "s", Model: &fakeModel{id: "m", result: stopResult("x")},
		Lifecycle: &Lifecycle[shape]{
			Enabled: func(ctx context.Context, args LifecycleBase[shape]) (bool, error) { return false, nil },
		},
	})
	n := NewNetwork(NetworkConfig[shape]{
		Name: "net", Agents: []*Agent[shape]{disabled},
		Router: scriptRouter(),
	})
	_, err := n.Run(context.Background(), "go", nil)
	if err == nil || !strings.Contains(err.Error(), "no agents enabled") {
		t.Fatalf("want no-agents-enabled error, got %v", err)
	}
}

func TestDefaultRoutingAgentOnRoute(t *testing.T) {
	ra := defaultRoutingAgent[shape]()

	route := func(toolName, content string) []string {
		res := NewAgentResult("router", nil, []Message{{
			Type: MessageToolResult, Role: RoleToolResult,
			Tool:    &ToolMessage{Type: "tool", ID: "t1", Name: toolName, Input: []byte(`{}`)},
			Content: RawContent(json.RawMessage(content)),
		}}, jsonutil.Now())
		return ra.OnRoute(context.Background(), LifecycleResult[shape]{Result: res})
	}

	if got := route("select_agent", `{"data":"worker"}`); len(got) != 1 || got[0] != "worker" {
		t.Errorf("select_agent route = %v", got)
	}
	if got := route("done", `{"data":"summary"}`); got != nil {
		t.Errorf("done must end the loop, got %v", got)
	}
	if got := route("select_agent", `{"error":{"name":"Error","message":"no such agent"}}`); got != nil {
		t.Errorf("tool error must end the loop, got %v", got)
	}
	// No tool calls at all.
	empty := NewAgentResult("router", nil, nil, jsonutil.Now())
	if got := ra.OnRoute(context.Background(), LifecycleResult[shape]{Result: empty}); got != nil {
		t.Errorf("no tool calls must end the loop, got %v", got)
	}
}
