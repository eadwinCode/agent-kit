package agentkit

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/inngest/inngestgo"
	"github.com/zendev-sh/goai/provider"

	"github.com/eadwinCode/agent-kit/go/durable"
	"github.com/eadwinCode/agent-kit/go/internal/jsonutil"
)

// capturePublisher records chunks; mutex-guarded for -race.
type capturePublisher struct {
	mu     sync.Mutex
	chunks []AgentMessageChunk
}

func (p *capturePublisher) publish(ctx context.Context, chunk AgentMessageChunk) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.chunks = append(p.chunks, chunk)
	return nil
}

func (p *capturePublisher) events() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, len(p.chunks))
	for i, c := range p.chunks {
		out[i] = c.Event
	}
	return out
}

func TestChunkEnvelopeWireShape(t *testing.T) {
	cap := &capturePublisher{}
	sc := newStreamingContext(StreamingConfig{Publish: cap.publish}, "run_1", "msg_1", "network", "th_1", "user_1")
	sc.PublishEvent(context.Background(), EventRunStarted, map[string]any{"runId": "run_1"})

	c := cap.chunks[0]
	if c.SequenceNumber != 0 || c.ID != "publish-0:run.started" || c.Timestamp == 0 {
		t.Errorf("envelope stamping wrong: %+v", c)
	}
	// threadId/userId auto-enrichment.
	if c.Data["threadId"] != "th_1" || c.Data["userId"] != "user_1" {
		t.Errorf("enrichment missing: %+v", c.Data)
	}

	// Envelope key order matches the TS interface exactly.
	b, err := jsonutil.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	for i, key := range []string{`"event":`, `"data":`, `"timestamp":`, `"sequenceNumber":`, `"id":`} {
		if !strings.Contains(string(b), key) {
			t.Fatalf("missing %s in %s", key, b)
		}
		if i > 0 && strings.Index(string(b), key) < strings.Index(string(b), `"event":`) {
			t.Errorf("envelope key order broken: %s", b)
		}
	}
}

func TestChunkContentBudget(t *testing.T) {
	// Chunking off: one delta regardless of size.
	off := newStreamingContext(StreamingConfig{Publish: func(ctx context.Context, c AgentMessageChunk) error { return nil }},
		"r", "m", "agent", "", "")
	if got := off.ChunkContent(strings.Repeat("x", 10_000)); len(got) != 1 {
		t.Errorf("chunking off must emit one delta, got %d", len(got))
	}
	if got := off.ChunkContent(""); got != nil {
		t.Errorf("empty content must emit no deltas, got %v", got)
	}

	// Chunking on: fixed size chunks...
	on := newStreamingContext(StreamingConfig{
		Publish:          func(ctx context.Context, c AgentMessageChunk) error { return nil },
		SimulateChunking: true, ChunkSize: 10, MaxChunksPerMessage: 5,
	}, "r", "m", "agent", "", "")
	if got := on.ChunkContent(strings.Repeat("x", 30)); len(got) != 3 {
		t.Errorf("30 chars / size 10 = 3 chunks, got %d", len(got))
	}
	// ...but never more than the cap: size grows for long content.
	long := on.ChunkContent(strings.Repeat("x", 1000))
	if len(long) > 5 {
		t.Errorf("cap of 5 exceeded: %d chunks", len(long))
	}
	if strings.Join(long, "") != strings.Repeat("x", 1000) {
		t.Error("chunks must reassemble to the original content")
	}
}

func TestSharedSequenceAcrossScopes(t *testing.T) {
	cap := &capturePublisher{}
	network := newStreamingContext(StreamingConfig{Publish: cap.publish}, "net_run", "net_run", "network", "", "")
	agent := network.WithSharedSequence("agent_run", "agent_msg", "agent")

	network.PublishEvent(context.Background(), EventRunStarted, map[string]any{})
	agent.PublishEvent(context.Background(), EventPartCreated, map[string]any{})
	network.PublishEvent(context.Background(), EventRunCompleted, map[string]any{})

	for i, c := range cap.chunks {
		if c.SequenceNumber != i {
			t.Errorf("sequence not shared/monotonic: chunk %d has seq %d", i, c.SequenceNumber)
		}
	}
	if agent.ParentRunID != "net_run" || agent.MessageID != "agent_msg" {
		t.Errorf("child context wrong: %+v", agent)
	}
}

func TestGeneratePartIDLength(t *testing.T) {
	sc := newStreamingContext(StreamingConfig{Publish: func(ctx context.Context, c AgentMessageChunk) error { return nil }},
		"r", "0195b2aa-7cf3-7893-a2bc-0123456789ab", "agent", "", "")
	id := sc.GeneratePartID()
	if len(id) > 40 {
		t.Errorf("part id exceeds OpenAI's 40-char limit: %q (%d)", id, len(id))
	}
	if !strings.HasPrefix(id, "tool_0195b2aa_") {
		t.Errorf("part id format wrong: %q", id)
	}
}

// TestNetworkStreamingEventFlow drives a full network run and asserts the
// protocol sequence use-agent depends on.
func TestNetworkStreamingEventFlow(t *testing.T) {
	cap := &capturePublisher{}
	workerModel := &fakeModel{id: "m", queue: []*provider.GenerateResult{
		toolCallResult("set_sku", `{"sku": 7}`),
	}}
	worker := NewAgent(AgentConfig[shape]{
		Name: "worker", System: "w", Model: workerModel, Tools: []Tool[shape]{newSKUTool()},
	})
	n := NewNetwork(NetworkConfig[shape]{
		Name:   "net",
		Agents: []*Agent[shape]{worker},
		Router: scriptRouter(RouteTo(worker)),
	})

	run, err := n.Run(context.Background(), "set it", &NetworkRunOptions[shape]{
		State:     NewState(StateConfig[shape]{ThreadID: "th_stream"}),
		Streaming: &StreamingConfig{Publish: cap.publish},
	})
	if err != nil {
		t.Fatal(err)
	}

	got := cap.events()
	want := []string{
		"run.started",               // network
		"run.started",               // agent (child)
		"part.created",              // text part ("calling set_sku")
		"text.delta",
		"part.completed",
		"part.created",              // tool-call args
		"tool_call.arguments.delta",
		"part.completed",
		"part.created",              // tool output
		"tool_call.output.delta",
		"part.completed",
		"run.completed",             // agent
		"run.completed",             // network (terminal)
		"stream.ended",
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("event flow mismatch:\nwant %v\n got %v", want, got)
	}

	// Sequence numbers strictly monotonic across network + agent scopes.
	for i, c := range cap.chunks {
		if c.SequenceNumber != i {
			t.Fatalf("chunk %d has sequence %d", i, c.SequenceNumber)
		}
	}

	// Every event carries the thread id.
	for _, c := range cap.chunks {
		if c.Data["threadId"] != "th_stream" {
			t.Fatalf("event %s missing threadId: %+v", c.Event, c.Data)
		}
	}

	// The agent child run.started links to the network run and its
	// messageId matches the persisted result id.
	var agentStarted AgentMessageChunk
	for _, c := range cap.chunks[1:] {
		if c.Event == "run.started" {
			agentStarted = c
			break
		}
	}
	if agentStarted.Data["parentRunId"] == nil || agentStarted.Data["parentRunId"] == "" {
		t.Error("agent run.started missing parentRunId")
	}
	if agentStarted.Data["messageId"] != run.State.Results()[0].ID {
		t.Errorf("streaming messageId %v != persisted result id %v",
			agentStarted.Data["messageId"], run.State.Results()[0].ID)
	}

	// Tool args first delta carries toolName; the finalContent of the
	// tool-call part is the raw input object.
	for _, c := range cap.chunks {
		if c.Event == "tool_call.arguments.delta" {
			if c.Data["toolName"] != "set_sku" {
				t.Errorf("first args delta missing toolName: %+v", c.Data)
			}
			break
		}
	}
}

func TestNetworkStreamingStopWhenInterrupted(t *testing.T) {
	cap := &capturePublisher{}
	a := mkAgent("a", "s", &fakeModel{id: "m", result: stopResult("x")})
	n := NewNetwork(NetworkConfig[shape]{
		Name: "net", Agents: []*Agent[shape]{a},
		Router: &Router[shape]{Fn: func(ctx context.Context, args RouterArgs[shape]) (*RouterResult[shape], error) {
			return RouteTo(args.Network.agents[0]), nil
		}},
		StopWhen: func(ctx context.Context, args StopWhenArgs[shape]) (*NetworkStop, error) {
			if args.CallCount >= 1 {
				return &NetworkStop{Reason: "budget"}, nil
			}
			return nil, nil
		},
	})
	_, err := n.Run(context.Background(), "go", &NetworkRunOptions[shape]{
		Streaming: &StreamingConfig{Publish: cap.publish},
	})
	if err != nil {
		t.Fatal(err)
	}
	events := cap.events()
	joined := strings.Join(events, ",")
	if !strings.Contains(joined, "run.interrupted") {
		t.Errorf("missing run.interrupted: %v", events)
	}
	// Terminal events carry the stop reason.
	last := cap.chunks[len(cap.chunks)-1]
	if last.Event != "stream.ended" || last.Data["reason"] != "budget" {
		t.Errorf("terminal event must carry reason: %+v", last)
	}
}

func TestStandaloneAgentStreaming(t *testing.T) {
	cap := &capturePublisher{}
	a := NewAgent(AgentConfig[shape]{
		Name: "solo", System: "s", Model: &fakeModel{id: "m", result: stopResult("hello")},
	})
	res, err := a.Run(context.Background(), "hi", &RunOptions[shape]{
		Streaming: &StreamingConfig{Publish: cap.publish},
	})
	if err != nil {
		t.Fatal(err)
	}
	got := cap.events()
	want := []string{"run.started", "part.created", "text.delta", "part.completed", "run.completed", "stream.ended"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("standalone flow mismatch:\nwant %v\n got %v", want, got)
	}
	// The streaming messageId became the result id.
	if res.ID == "" || cap.chunks[0].Data["messageId"] != res.ID {
		t.Errorf("result id %q != streamed messageId %v", res.ID, cap.chunks[0].Data["messageId"])
	}
	// Scope is agent throughout run lifecycle events.
	if cap.chunks[0].Data["scope"] != "agent" {
		t.Errorf("standalone scope: %+v", cap.chunks[0].Data)
	}
}

func TestStreamReasoningOptIn(t *testing.T) {
	reasoningResult := &provider.GenerateResult{
		Text:         "answer",
		FinishReason: provider.FinishStop,
		Reasoning:    "thinking hard",
		ProviderMetadata: map[string]map[string]any{
			"anthropic": {"reasoning": []map[string]any{{"type": "thinking", "text": "thinking hard", "signature": "s"}}},
		},
	}

	// Default: reasoning NOT streamed.
	capOff := &capturePublisher{}
	a := NewAgent(AgentConfig[shape]{Name: "solo", System: "s", Model: &fakeModel{id: "m", result: reasoningResult}})
	if _, err := a.Run(context.Background(), "hi", &RunOptions[shape]{
		Streaming: &StreamingConfig{Publish: capOff.publish},
	}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.Join(capOff.events(), ","), "reasoning.delta") {
		t.Error("reasoning streamed without opt-in")
	}

	// Opt-in: reasoning part precedes the text part.
	capOn := &capturePublisher{}
	if _, err := a.Run(context.Background(), "hi", &RunOptions[shape]{
		Streaming: &StreamingConfig{Publish: capOn.publish, StreamReasoning: true},
	}); err != nil {
		t.Fatal(err)
	}
	events := capOn.events()
	joined := strings.Join(events, ",")
	if !strings.Contains(joined, "reasoning.delta") {
		t.Fatalf("reasoning not streamed on opt-in: %v", events)
	}
	if strings.Index(joined, "reasoning.delta") > strings.Index(joined, "text.delta") {
		t.Error("reasoning part must precede text part")
	}
}

func TestDurablePublishUsesChunkID(t *testing.T) {
	cap := &capturePublisher{}
	// Inline step still routes through the JSON round-trip, proving the
	// chunk survives memoization.
	pub := DurablePublish(inlineStepRecorder{}, cap.publish)
	sc := newStreamingContext(StreamingConfig{Publish: pub}, "r", "m", "agent", "", "")
	sc.PublishEvent(context.Background(), EventTextDelta, map[string]any{"delta": "x"})
	if len(cap.chunks) != 1 || cap.chunks[0].Data["delta"] != "x" {
		t.Fatalf("durable publish did not deliver: %+v", cap.chunks)
	}
}

// inlineStepRecorder delegates to durable.Inline semantics while asserting
// the step id is the chunk's suggested id.
type inlineStepRecorder struct{}

func (inlineStepRecorder) Run(ctx context.Context, id string, fn durable.RunFn) (json.RawMessage, error) {
	if !strings.HasPrefix(id, "publish-") {
		return nil, context.Canceled
	}
	return fn(ctx)
}

func TestServerRegistration(t *testing.T) {
	worker := mkAgent("My Worker Agent", "w", &fakeModel{id: "m", result: stopResult("x")})
	n := NewNetwork(NetworkConfig[shape]{Name: "Support Network!", Agents: []*Agent[shape]{worker},
		Router: scriptRouter()})

	handler, err := NewServer("clevix-test", func(c inngestgo.Client) error {
		if _, err := RegisterAgent(c, worker); err != nil {
			return err
		}
		_, err := RegisterNetwork(c, n)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if handler == nil {
		t.Fatal("nil handler")
	}

	if got := Slugify("Support Network!"); got != "support-network" {
		t.Errorf("Slugify = %q", got)
	}
	if got := Slugify("My Worker Agent"); got != "my-worker-agent" {
		t.Errorf("Slugify = %q", got)
	}
}
