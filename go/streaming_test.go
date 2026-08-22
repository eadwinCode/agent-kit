package agentkit

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

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

type memoStep struct {
	mu    sync.Mutex
	cache map[string]json.RawMessage
}

func (s *memoStep) Run(ctx context.Context, id string, fn durable.RunFn) (json.RawMessage, error) {
	s.mu.Lock()
	raw, ok := s.cache[id]
	s.mu.Unlock()
	if ok {
		return append(json.RawMessage(nil), raw...), nil
	}
	raw, err := fn(ctx)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.cache[id] = append(json.RawMessage(nil), raw...)
	s.mu.Unlock()
	return raw, nil
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

func (p *capturePublisher) snapshot() []AgentMessageChunk {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]AgentMessageChunk(nil), p.chunks...)
}

// controlledStreamingModel exposes real provider chunk timing to tests. It
// sends all configured chunks immediately, then withholds the finish chunk
// until release is closed. Cancellation wakes the provider and surfaces as a
// ChunkError, matching GoAI provider contracts.
type controlledStreamingModel struct {
	id      string
	result  *provider.GenerateResult
	chunks  []provider.StreamChunk
	release <-chan struct{}
}

func (m *controlledStreamingModel) ModelID() string { return m.id }

func (m *controlledStreamingModel) DoGenerate(context.Context, provider.GenerateParams) (*provider.GenerateResult, error) {
	return m.result, nil
}

func (m *controlledStreamingModel) DoStream(ctx context.Context, _ provider.GenerateParams) (*provider.StreamResult, error) {
	out := make(chan provider.StreamChunk, len(m.chunks)+2)
	go func() {
		defer close(out)
		for _, chunk := range m.chunks {
			if !provider.TrySend(ctx, out, chunk) {
				return
			}
		}
		if m.release != nil {
			select {
			case <-m.release:
			case <-ctx.Done():
				// Send directly into the buffered provider channel: GoAI's
				// reducer records ChunkError before checking its own context.
				out <- provider.StreamChunk{Type: provider.ChunkError, Error: ctx.Err()}
				return
			}
		}
		out <- provider.StreamChunk{
			Type: provider.ChunkFinish, FinishReason: m.result.FinishReason,
			Usage: m.result.Usage, Response: m.result.Response,
			Metadata: map[string]any{"providerMetadata": m.result.ProviderMetadata},
		}
	}()
	return &provider.StreamResult{Stream: out}, nil
}

type deltaSignals struct {
	reasoning chan struct{}
	text      chan struct{}
	rOnce     sync.Once
	tOnce     sync.Once
}

func (s *deltaSignals) publish(cap *capturePublisher) PublishFn {
	return func(ctx context.Context, chunk AgentMessageChunk) error {
		if err := cap.publish(ctx, chunk); err != nil {
			return err
		}
		switch chunk.Event {
		case EventReasoningDelta:
			s.rOnce.Do(func() { close(s.reasoning) })
		case EventTextDelta:
			s.tOnce.Do(func() { close(s.text) })
		}
		return nil
	}
}

func waitSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func TestChunkEnvelopeWireShape(t *testing.T) {
	cap := &capturePublisher{}
	sc := newStreamingContext(StreamingConfig{Publish: cap.publish}, nil, "run_1", "msg_1", "network", "th_1", "user_1")
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
	off := newStreamingContext(StreamingConfig{Publish: func(ctx context.Context, c AgentMessageChunk) error { return nil }}, nil, "r", "m", "agent", "", "")
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
	}, nil, "r", "m", "agent", "", "")
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
	network := newStreamingContext(StreamingConfig{Publish: cap.publish}, nil, "net_run", "net_run", "network", "", "")
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
	sc := newStreamingContext(StreamingConfig{Publish: func(ctx context.Context, c AgentMessageChunk) error { return nil }}, nil, "r", "0195b2aa-7cf3-7893-a2bc-0123456789ab", "agent", "", "")
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
		"run.started",  // network
		"run.started",  // agent (child)
		"part.created", // text part ("calling set_sku")
		"text.delta",
		"part.completed",
		"part.created", // tool-call args
		"tool_call.arguments.delta",
		"part.completed",
		"part.created", // tool output
		"tool_call.output.delta",
		"part.completed",
		"run.completed", // agent
		"run.completed", // network (terminal)
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

func TestTrueModelStreamingPublishesBeforeInferenceCompletion(t *testing.T) {
	release := make(chan struct{})
	final := &provider.GenerateResult{
		Text:         "hello",
		Reasoning:    "think now",
		FinishReason: provider.FinishToolCalls,
		ToolCalls: []provider.ToolCall{{
			ID: "call_stream", Name: "set_sku", Input: json.RawMessage(`{"sku":7}`),
		}},
		Usage: provider.Usage{
			InputTokens: 11, OutputTokens: 6, TotalTokens: 17,
			CacheWriteTokens: 23, CacheReadTokens: 29,
		},
		ProviderMetadata: map[string]map[string]any{
			"anthropic": {"reasoning": []map[string]any{{
				"type": "thinking", "text": "think now", "signature": "sig-stream",
			}}},
		},
	}
	streamModel := &controlledStreamingModel{
		id: "claude-stream", result: final, release: release,
		chunks: []provider.StreamChunk{
			{Type: provider.ChunkReasoning, Text: "think "},
			{Type: provider.ChunkReasoning, Text: "now", Metadata: map[string]any{"signature": "sig-stream"}},
			{Type: provider.ChunkText, Text: "hel"},
			{Type: provider.ChunkText, Text: "lo"},
			{Type: provider.ChunkToolCall, ToolCallID: "call_stream", ToolName: "set_sku", ToolInput: `{"sku":7}`},
		},
	}

	seed := []Message{{Type: MessageText, Role: RoleAssistant, Content: TextContent("earlier"), StopReason: StopStop}}
	streamState := NewState(StateConfig[shape]{Messages: seed})
	cap := &capturePublisher{}
	signals := &deltaSignals{reasoning: make(chan struct{}), text: make(chan struct{})}
	streamAgent := NewAgent(AgentConfig[shape]{
		Name: "streamer", System: "sys", Model: streamModel, Tools: []Tool[shape]{newSKUTool()},
	})

	type runResult struct {
		result *AgentResult
		err    error
	}
	done := make(chan runResult, 1)
	go func() {
		result, err := streamAgent.Run(context.Background(), "set it", &RunOptions[shape]{
			State: streamState,
			Streaming: &StreamingConfig{
				Publish: signals.publish(cap), StreamReasoning: true,
				// Raw provider deltas must remain "hel"/"lo" and must not
				// be confused with simulated post-completion chunking.
				SimulateChunking: true, ChunkSize: 1,
			},
		})
		done <- runResult{result: result, err: err}
	}()

	waitSignal(t, signals.reasoning, "first reasoning.delta")
	select {
	case got := <-done:
		t.Fatalf("inference completed before reasoning delta assertion: %+v", got)
	default:
	}
	waitSignal(t, signals.text, "first text.delta")
	select {
	case got := <-done:
		t.Fatalf("inference completed before text delta assertion: %+v", got)
	default:
	}
	close(release)

	var streamed *AgentResult
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatal(got.err)
		}
		streamed = got.result
	case <-time.After(2 * time.Second):
		t.Fatal("streaming run did not complete after release")
	}

	// The same provider result through GenerateText is the compatibility
	// oracle for final output, history, tool calls, raw usage/cache tokens,
	// and AgentKit-owned tool execution.
	nonStreamState := NewState(StateConfig[shape]{Messages: seed})
	nonStreamAgent := NewAgent(AgentConfig[shape]{
		Name: "streamer", System: "sys", Model: &fakeModel{id: "claude-stream", result: final},
		Tools: []Tool[shape]{newSKUTool()},
	})
	nonStreamed, err := nonStreamAgent.Run(context.Background(), "set it", &RunOptions[shape]{State: nonStreamState})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(streamed.Output, nonStreamed.Output) {
		t.Errorf("streamed output differs from GenerateText:\nstreamed %#v\nregular  %#v", streamed.Output, nonStreamed.Output)
	}
	if !reflect.DeepEqual(streamed.History, nonStreamed.History) {
		t.Errorf("streamed history differs from GenerateText:\nstreamed %#v\nregular  %#v", streamed.History, nonStreamed.History)
	}
	if !reflect.DeepEqual(streamed.ToolCalls, nonStreamed.ToolCalls) {
		t.Errorf("streamed tool results differ from GenerateText:\nstreamed %#v\nregular  %#v", streamed.ToolCalls, nonStreamed.ToolCalls)
	}
	if streamed.Raw != nonStreamed.Raw {
		t.Errorf("streamed raw result differs from GenerateText:\nstreamed %s\nregular  %s", streamed.Raw, nonStreamed.Raw)
	}
	if streamState.Data.SKU != 7 || nonStreamState.Data.SKU != 7 {
		t.Errorf("AgentKit tool loop not preserved: streamed=%d regular=%d", streamState.Data.SKU, nonStreamState.Data.SKU)
	}

	assertProviderPartFlow(t, cap.snapshot(), "reasoning", EventReasoningDelta, []string{"think ", "now"})
	assertProviderPartFlow(t, cap.snapshot(), "text", EventTextDelta, []string{"hel", "lo"})
}

func assertProviderPartFlow(t *testing.T, chunks []AgentMessageChunk, kind, deltaEvent string, wantDeltas []string) {
	t.Helper()
	created, completed := 0, 0
	partID := ""
	var deltas []string
	createdAt, completedAt := -1, -1
	for i, chunk := range chunks {
		switch chunk.Event {
		case EventPartCreated:
			if chunk.Data["type"] == kind {
				created++
				partID, _ = chunk.Data["partId"].(string)
				createdAt = i
			}
		case deltaEvent:
			if chunk.Data["partId"] == partID {
				delta, _ := chunk.Data["delta"].(string)
				deltas = append(deltas, delta)
				if createdAt < 0 || completedAt >= 0 {
					t.Errorf("%s delta outside created/completed bounds: %+v", kind, chunk)
				}
			}
		case EventPartCompleted:
			if chunk.Data["type"] == kind {
				completed++
				completedAt = i
				if chunk.Data["partId"] != partID {
					t.Errorf("%s completion changed part id: created=%q completed=%v", kind, partID, chunk.Data["partId"])
				}
				if chunk.Data["finalContent"] != strings.Join(wantDeltas, "") {
					t.Errorf("%s final content = %v", kind, chunk.Data["finalContent"])
				}
			}
		}
	}
	if created != 1 || completed != 1 {
		t.Errorf("%s must have exactly one created/completed event, got %d/%d", kind, created, completed)
	}
	if createdAt < 0 || completedAt <= createdAt {
		t.Errorf("%s part ordering invalid: created=%d completed=%d", kind, createdAt, completedAt)
	}
	if !reflect.DeepEqual(deltas, wantDeltas) {
		t.Errorf("%s raw deltas were altered or duplicated: got %q want %q", kind, deltas, wantDeltas)
	}
}

func TestTrueModelStreamingCancellationAndErrors(t *testing.T) {
	t.Run("cancellation fails the open part", func(t *testing.T) {
		release := make(chan struct{})
		model := &controlledStreamingModel{
			id: "m", result: stopResult("partial"), release: release,
			chunks: []provider.StreamChunk{{Type: provider.ChunkText, Text: "partial"}},
		}
		cap := &capturePublisher{}
		signals := &deltaSignals{reasoning: make(chan struct{}), text: make(chan struct{})}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			a := NewAgent(AgentConfig[shape]{Name: "cancel", System: "s", Model: model})
			_, err := a.Run(ctx, "go", &RunOptions[shape]{Streaming: &StreamingConfig{Publish: signals.publish(cap)}})
			done <- err
		}()
		waitSignal(t, signals.text, "text.delta before cancellation")
		cancel()
		err := <-done
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation error = %v", err)
		}
		assertFailedOpenPart(t, cap.snapshot(), "The provider stream ended unexpectedly.")
	})

	t.Run("provider stream error fails the open part", func(t *testing.T) {
		boom := errors.New("stream exploded")
		model := &controlledStreamingModel{
			id: "m", result: stopResult("partial"),
			chunks: []provider.StreamChunk{
				{Type: provider.ChunkText, Text: "partial"},
				{Type: provider.ChunkError, Error: boom},
			},
		}
		cap := &capturePublisher{}
		a := NewAgent(AgentConfig[shape]{Name: "error", System: "s", Model: model})
		_, err := a.Run(context.Background(), "go", &RunOptions[shape]{Streaming: &StreamingConfig{Publish: cap.publish}})
		if !errors.Is(err, boom) {
			t.Fatalf("stream error = %v", err)
		}
		assertFailedOpenPart(t, cap.snapshot(), "The provider stream ended unexpectedly.")
	})
}

func assertFailedOpenPart(t *testing.T, chunks []AgentMessageChunk, errorText string) {
	t.Helper()
	var createdID string
	failed := 0
	completed := 0
	for _, chunk := range chunks {
		switch chunk.Event {
		case EventPartCreated:
			if chunk.Data["type"] == "text" {
				createdID, _ = chunk.Data["partId"].(string)
			}
		case EventPartFailed:
			if chunk.Data["type"] == "text" {
				failed++
				if chunk.Data["partId"] != createdID || chunk.Data["error"] != errorText {
					t.Errorf("failed part payload = %+v, created id %q", chunk.Data, createdID)
				}
			}
		case EventPartCompleted:
			if chunk.Data["type"] == "text" {
				completed++
			}
		}
	}
	if createdID == "" || failed != 1 || completed != 0 {
		t.Errorf("open text part must fail exactly once and never complete: id=%q failed=%d completed=%d events=%v",
			createdID, failed, completed, chunks)
	}
}

func TestStableProviderPartIdentity(t *testing.T) {
	a := newStreamingContext(StreamingConfig{}, nil, "run", "message", "agent", "", "")
	b := newStreamingContext(StreamingConfig{}, nil, "run", "message", "agent", "", "")
	if a.stablePartID("text", 2, 3) != b.stablePartID("text", 2, 3) {
		t.Fatal("identical inference coordinates must produce replay-stable part ids")
	}
	if a.stablePartID("text", 2, 3) == a.stablePartID("reasoning", 2, 3) {
		t.Fatal("part kind must participate in stable identity")
	}
	if len(a.stablePartID("reasoning", 2, 3)) > 40 {
		t.Fatal("stable part id exceeds provider limits")
	}
}

func TestTrueModelStreamingReplayDoesNotRepublish(t *testing.T) {
	step := &memoStep{cache: map[string]json.RawMessage{}}
	model := &fakeModel{id: "m", result: stopResult("hello")}
	a := NewAgent(AgentConfig[shape]{Name: "replay", System: "s", Model: model})
	cap := &capturePublisher{}
	streaming := &StreamingConfig{Publish: DurablePublish(step, cap.publish)}

	var firstPartID string
	for run := 0; run < 2; run++ {
		result, err := a.Run(context.Background(), "go", &RunOptions[shape]{Step: step, Streaming: streaming})
		if err != nil {
			t.Fatal(err)
		}
		if result.ID == "" {
			t.Fatal("replayed run lost its stable message id")
		}
		if run == 0 {
			for _, chunk := range cap.snapshot() {
				if chunk.Event == EventPartCreated && chunk.Data["type"] == "text" {
					firstPartID, _ = chunk.Data["partId"].(string)
				}
			}
			if firstPartID == "" {
				t.Fatal("first run did not publish a text part")
			}
		}
	}
	if model.calls != 1 {
		t.Fatalf("memoized replay called the streaming provider %d times", model.calls)
	}
	events := cap.snapshot()
	if len(events) != 6 {
		t.Fatalf("successful replay republished events: got %d events: %v", len(events), events)
	}
	createdID := ""
	for _, chunk := range events {
		if chunk.Event == EventPartCreated && chunk.Data["type"] == "text" {
			createdID, _ = chunk.Data["partId"].(string)
		}
	}
	if createdID != firstPartID {
		t.Fatalf("provider part identity drifted: first=%q final=%q", firstPartID, createdID)
	}
}

func TestTrueModelStreamingReplaysLegacyInferenceCacheCompatibly(t *testing.T) {
	legacy := SerializableResult{
		Text: "legacy answer", FinishReason: string(provider.FinishStop),
		Usage: &SerializableUsage{InputTokens: 3, OutputTokens: 2, TotalTokens: 5},
	}
	raw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	step := &memoStep{cache: map[string]json.RawMessage{"legacy/infer/0": raw}}
	model := &fakeModel{id: "m"}
	a := NewAgent(AgentConfig[shape]{Name: "legacy", System: "s", Model: model})
	cap := &capturePublisher{}

	result, err := a.Run(context.Background(), "go", &RunOptions[shape]{
		Step: step, Streaming: &StreamingConfig{Publish: cap.publish},
	})
	if err != nil {
		t.Fatal(err)
	}
	if model.calls != 0 {
		t.Fatalf("legacy cached inference unexpectedly called provider %d times", model.calls)
	}
	if len(result.Output) != 1 || textOf(result.Output[0].Content) != "legacy answer" {
		t.Fatalf("legacy cached output was not preserved: %+v", result.Output)
	}
	assertProviderPartFlow(t, cap.snapshot(), "text", EventTextDelta, []string{"legacy answer"})
}

func TestDurablePublishUsesChunkID(t *testing.T) {
	cap := &capturePublisher{}
	// Inline step still routes through the JSON round-trip, proving the
	// chunk survives memoization.
	pub := DurablePublish(inlineStepRecorder{}, cap.publish)
	sc := newStreamingContext(StreamingConfig{Publish: pub}, nil, "r", "m", "agent", "", "")
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
