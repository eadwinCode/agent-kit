package agentkit_test

// Lifecycle tests for the public runtime ports: WHEN AgentKit invokes each
// contract, in what order, and what it does when one fails. They run
// against the in-memory reference adapters, so they exercise the same
// public API an application adapter implements.

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zendev-sh/goai/provider"

	agentkit "github.com/eadwinCode/agent-kit/go"
	"github.com/eadwinCode/agent-kit/go/durable"
	"github.com/eadwinCode/agent-kit/go/memadapter"
)

type portState struct {
	UserID string `json:"userId,omitempty"`
	Note   string `json:"note,omitempty"`
}

// testScope is an opaque owner token. AgentKit never parses it; the
// composite shape here is just one application's convention.
const testScope agentkit.SessionScope = "owner-scope-1"

// scriptedModel replays a fixed chunk script per call, then the finish
// chunk. It gives tests exact control over provider timing without a
// network call.
type scriptedModel struct {
	mu      sync.Mutex
	id      string
	scripts [][]provider.StreamChunk
	results []*provider.GenerateResult
	calls   int
}

func (m *scriptedModel) ModelID() string { return m.id }

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
			if !provider.TrySend(ctx, out, chunk) {
				return
			}
		}
		out <- provider.StreamChunk{
			Type: provider.ChunkFinish, FinishReason: result.FinishReason,
			Usage: result.Usage, Response: result.Response,
			Metadata: map[string]any{"providerMetadata": result.ProviderMetadata},
		}
	}()
	return &provider.StreamResult{Stream: out}, nil
}

func textModel(text string) *scriptedModel {
	return &scriptedModel{
		id: "scripted",
		scripts: [][]provider.StreamChunk{{
			{Type: provider.ChunkText, Text: text},
		}},
		results: []*provider.GenerateResult{{
			Text: text, FinishReason: provider.FinishStop,
			Usage: provider.Usage{InputTokens: 3, OutputTokens: 2, TotalTokens: 5},
		}},
	}
}

// runNetwork runs a one-agent network with the supplied ports and returns
// the run.
func runNetwork(t *testing.T, model provider.LanguageModel, ports *agentkit.RuntimePorts, tools []agentkit.Tool[portState], step durable.Step) (*agentkit.NetworkRun[portState], error) {
	t.Helper()
	agent := agentkit.NewAgent(agentkit.AgentConfig[portState]{
		Name: "assistant", System: "be brief", Model: model, Tools: tools,
	})
	calls := 0
	network := agentkit.NewNetwork(agentkit.NetworkConfig[portState]{
		Name:   "editor",
		Agents: []*agentkit.Agent[portState]{agent},
		Router: &agentkit.Router[portState]{
			Fn: func(context.Context, agentkit.RouterArgs[portState]) (*agentkit.RouterResult[portState], error) {
				if calls > 0 {
					return nil, nil
				}
				calls++
				return agentkit.RouteTo(agent), nil
			},
		},
		MaxIter: 2,
		Ports:   ports,
		History: &agentkit.HistoryConfig[portState]{
			CreateThread: func(context.Context, agentkit.HistoryContext[portState]) (agentkit.CreateThreadResult, error) {
				return agentkit.CreateThreadResult{ThreadID: "thread_1"}, nil
			},
			AppendUserMessage: func(context.Context, agentkit.HistoryContext[portState], agentkit.UserMessageRecord) error {
				return nil
			},
			AppendResults: func(context.Context, agentkit.HistoryContext[portState], []*agentkit.AgentResult) error {
				return nil
			},
		},
	})
	if step == nil {
		step = durable.Inline{}
	}
	return network.Run(context.Background(), "hello", &agentkit.NetworkRunOptions[portState]{
		UserMessage: &agentkit.UserMessage{ID: "user_msg_1", Content: "hello", Role: agentkit.RoleUser},
		Step:        step,
		Streaming:   &agentkit.StreamingConfig{StreamReasoning: true},
	})
}

func eventNames(chunks []agentkit.AgentMessageChunk) []string {
	out := make([]string, len(chunks))
	for i, c := range chunks {
		out[i] = c.Event
	}
	return out
}

func indexOfEvent(names []string, want string) int {
	for i, name := range names {
		if name == want {
			return i
		}
	}
	return -1
}

func TestJournalIsWrittenBeforeLiveFanOut(t *testing.T) {
	handles, ports := memadapter.NewPorts(testScope, 0)

	// A sink that asserts, at delivery time, that the journal already holds
	// the envelope. Recovery depends on this ordering: an event delivered
	// live but never journaled is invisible to a client that reconnects.
	var missing []string
	ports.Sink = agentkit.SinkFunc(func(ctx context.Context, chunk agentkit.AgentMessageChunk) error {
		threadID, _ := chunk.Data["threadId"].(string)
		records, _, err := agentkit.ReadJournalTail(ctx, handles.Journal, agentkit.JournalQuery{
			Scope:    testScope,
			ThreadID: threadID,
			After: agentkit.JournalCursor{
				StreamEpoch:    chunk.StreamEpoch,
				SequenceNumber: agentkit.JournalStart,
			},
		})
		if err != nil {
			return err
		}
		found := false
		for _, rec := range records {
			if rec.EventID == chunk.EventID {
				found = true
				break
			}
		}
		if !found {
			missing = append(missing, chunk.Event)
		}
		return nil
	})

	if _, err := runNetwork(t, textModel("hi"), ports, nil, nil); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(missing) > 0 {
		t.Fatalf("delivered before journaling: %v", missing)
	}
	if handles.Journal.Len("thread_1") == 0 {
		t.Fatal("journal is empty; nothing was persisted")
	}
}

func TestJournalAppendIsIdempotentAcrossReplay(t *testing.T) {
	handles, ports := memadapter.NewPorts(testScope, 0)
	step := newMemoStep()

	if _, err := runNetwork(t, textModel("hi"), ports, nil, step); err != nil {
		t.Fatalf("first run: %v", err)
	}
	first := handles.Journal.Len("thread_1")

	// Second execution against the same memoized steps: an Inngest replay.
	// The same logical events are produced with the same EventIDs, so the
	// tail must not grow.
	if _, err := runNetwork(t, textModel("hi"), ports, nil, step); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if got := handles.Journal.Len("thread_1"); got != first {
		t.Fatalf("journal grew from %d to %d across a replay; EventIDs must dedupe", first, got)
	}
}

func TestJournalFailureDegradesAndMarksReconcile(t *testing.T) {
	handles, ports := memadapter.NewPorts(testScope, 0)
	handles.Journal.FailAppend = errors.New("journal offline")

	run, err := runNetwork(t, textModel("hi"), ports, nil, nil)
	if err != nil {
		t.Fatalf("a journal outage must not fail the run: %v", err)
	}
	if run == nil {
		t.Fatal("run is nil")
	}
	if handles.Finalizer.Calls() != 1 {
		t.Fatalf("finalizer ran %d times, want 1", handles.Finalizer.Calls())
	}
	if !handles.Finalizer.Requests[0].ReconcileRequired {
		t.Fatal("a failed journal append must set ReconcileRequired so clients reconcile from history")
	}

	state, err := handles.State.Load(context.Background(), testScope)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !state.ReconcileRequired {
		t.Fatal("terminal state must carry ReconcileRequired")
	}
}

func TestFinalizerAuthorizesTheOneTerminal(t *testing.T) {
	handles, ports := memadapter.NewPorts(testScope, 0)

	var atFinalize []string
	handles.Finalizer.OnFinalize = func(agentkit.FinalizeRequest) {
		atFinalize = handles.Sink.Events()
	}

	if _, err := runNetwork(t, textModel("hi"), ports, nil, nil); err != nil {
		t.Fatalf("run: %v", err)
	}

	if handles.Finalizer.Calls() != 1 {
		t.Fatalf("Finalize ran %d times; it must run exactly once", handles.Finalizer.Calls())
	}
	// Nothing terminal may be on the wire while the finalizer is still
	// settling history, billing and cleanup.
	for _, event := range atFinalize {
		switch event {
		case agentkit.EventRunCompleted, agentkit.EventRunFailed, agentkit.EventStreamEnded:
			// run.completed for the inner agent scope is emitted by the
			// network loop, not the terminal emitter; only the network's own
			// terminal is gated. Distinguish by checking it is the last one.
		}
	}
	names := handles.Sink.Events()
	if last := names[len(names)-1]; last != agentkit.EventStreamEnded {
		t.Fatalf("last event %q; the terminal must close the stream", last)
	}
	if indexOfEvent(atFinalize, agentkit.EventStreamEnded) != -1 {
		t.Fatal("stream.ended was published before the finalizer authorized it")
	}

	terminals := 0
	for _, name := range names {
		if name == agentkit.EventStreamEnded {
			terminals++
		}
	}
	if terminals != 1 {
		t.Fatalf("published %d stream.ended events; exactly one terminal is the contract", terminals)
	}
}

func TestFinalizerFailurePublishesTypedTerminalFailure(t *testing.T) {
	handles, ports := memadapter.NewPorts(testScope, 0)
	handles.Finalizer.Err = agentkit.NewPortError("Finalizer", "Finalize", "BILLING_UNAVAILABLE", true,
		errors.New("billing offline"))

	if _, err := runNetwork(t, textModel("hi"), ports, nil, nil); err != nil {
		t.Fatalf("run: %v", err)
	}

	chunks := handles.Sink.Chunks()
	var failed *agentkit.AgentMessageChunk
	for i := range chunks {
		if chunks[i].Event == agentkit.EventRunFailed {
			failed = &chunks[i]
		}
	}
	if failed == nil {
		t.Fatal("a finalizer failure must publish a failed terminal, not a silent success")
	}
	if failed.Data["code"] != "BILLING_UNAVAILABLE" {
		t.Fatalf("terminal code = %v; want the finalizer's bounded code", failed.Data["code"])
	}
}

func TestRuntimeErrorsDoNotLeakIntoTerminalEvents(t *testing.T) {
	handles, ports := memadapter.NewPorts(testScope, 0)
	secret := "api-key=sk-do-not-publish"
	model := &scriptedModel{
		id: "scripted",
		scripts: [][]provider.StreamChunk{{
			{Type: provider.ChunkError, Error: errors.New(secret)},
		}},
		results: []*provider.GenerateResult{{FinishReason: provider.FinishError}},
	}

	if _, err := runNetwork(t, model, ports, nil, nil); err == nil {
		t.Fatal("provider failure must surface to the server caller")
	}
	for _, chunk := range handles.Sink.Chunks() {
		raw, err := json.Marshal(chunk.Data)
		if err != nil {
			t.Fatalf("marshal event data: %v", err)
		}
		if strings.Contains(string(raw), secret) {
			t.Fatalf("event %q leaked the runtime error: %s", chunk.Event, raw)
		}
	}
}

func TestAcceptedUserMessageIsPublishedForEveryClient(t *testing.T) {
	handles, ports := memadapter.NewPorts(testScope, 0)

	if _, err := runNetwork(t, textModel("hi"), ports, nil, nil); err != nil {
		t.Fatalf("run: %v", err)
	}

	chunks := handles.Sink.Chunks()
	var accepted *agentkit.AgentMessageChunk
	for i := range chunks {
		if chunks[i].Event == agentkit.EventUserMessage {
			accepted = &chunks[i]
		}
	}
	if accepted == nil {
		t.Fatal("the server-accepted user turn must be published so every authorized client renders it")
	}
	if accepted.Data["messageId"] != "user_msg_1" {
		t.Fatalf("user message id = %v; it must be the one history persisted", accepted.Data["messageId"])
	}
	if accepted.Data["content"] != "hello" {
		t.Fatalf("user message content = %v", accepted.Data["content"])
	}
}

func TestEnvelopeCarriesReplayIdentity(t *testing.T) {
	handles, ports := memadapter.NewPorts(testScope, 3)

	if _, err := runNetwork(t, textModel("hi"), ports, nil, nil); err != nil {
		t.Fatalf("run: %v", err)
	}

	chunks := handles.Sink.Chunks()
	if len(chunks) == 0 {
		t.Fatal("no chunks delivered")
	}
	seen := map[string]bool{}
	for _, chunk := range chunks {
		if chunk.SchemaVersion != agentkit.ContractSchemaVersion {
			t.Fatalf("schemaVersion = %d, want %d", chunk.SchemaVersion, agentkit.ContractSchemaVersion)
		}
		if chunk.StreamEpoch != 3 {
			t.Fatalf("streamEpoch = %d, want the run's epoch 3", chunk.StreamEpoch)
		}
		if chunk.EventID == "" {
			t.Fatal("every envelope needs a stable event id for client dedupe")
		}
		if seen[chunk.EventID] {
			t.Fatalf("duplicate event id %q within one run", chunk.EventID)
		}
		seen[chunk.EventID] = true
		if !strings.Contains(chunk.ID, "publish-e3-") {
			t.Fatalf("step id %q does not carry the epoch; a resumed run would reuse ids", chunk.ID)
		}
	}
}

func TestPauseTakesEffectAtTheNextSafeBoundary(t *testing.T) {
	handles, ports := memadapter.NewPorts(testScope, 0)

	// Accept a pause before the run starts: the first safe boundary must
	// honor it, and the run must not proceed until a correlated resume.
	if _, err := handles.Control.Record(context.Background(), agentkit.ControlCommand{
		Scope: testScope, ID: "cmd_pause", Type: agentkit.CommandPause,
	}); err != nil {
		t.Fatalf("Record pause: %v", err)
	}

	resumed := make(chan struct{})
	go func() {
		defer close(resumed)
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			state, err := handles.State.Load(context.Background(), testScope)
			if err == nil && state.Pause.State == agentkit.PausePaused {
				_, _ = handles.Control.Record(context.Background(), agentkit.ControlCommand{
					Scope: testScope, ID: "cmd_resume", Type: agentkit.CommandResume,
					PauseEpoch: state.Pause.Epoch,
				})
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	if _, err := runNetwork(t, textModel("hi"), ports, nil, nil); err != nil {
		t.Fatalf("run: %v", err)
	}
	<-resumed

	names := handles.Sink.Events()
	if indexOfEvent(names, agentkit.EventStateUpdated) == -1 {
		t.Fatal("a pause must publish a state update so every tab shows Paused")
	}

	state, err := handles.State.Load(context.Background(), testScope)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if state.Pause.State != agentkit.PauseNone {
		t.Fatalf("pause state after resume = %q, want none", state.Pause.State)
	}
	if state.ActiveRun == nil || state.ActiveRun.Outcome != agentkit.OutcomeCompleted {
		t.Fatalf("run outcome = %+v; a resumed run must still complete", state.ActiveRun)
	}
}

func TestCancelIsTerminalAtASafeBoundary(t *testing.T) {
	handles, ports := memadapter.NewPorts(testScope, 0)

	if _, err := handles.Control.Record(context.Background(), agentkit.ControlCommand{
		Scope: testScope, ID: "cmd_cancel", Type: agentkit.CommandCancel,
	}); err != nil {
		t.Fatalf("Record cancel: %v", err)
	}

	_, err := runNetwork(t, textModel("hi"), ports, nil, nil)
	if !agentkit.IsCancelled(err) {
		t.Fatalf("run error = %v; cancel must end the run", err)
	}

	chunks := handles.Sink.Chunks()
	var failed *agentkit.AgentMessageChunk
	for i := range chunks {
		if chunks[i].Event == agentkit.EventRunFailed {
			failed = &chunks[i]
		}
	}
	if failed == nil || failed.Data["cancelled"] != true {
		t.Fatalf("a cancelled run must publish a cancelled terminal; got %+v", failed)
	}
	if handles.Finalizer.Calls() != 1 {
		t.Fatalf("finalizer ran %d times on cancel, want 1", handles.Finalizer.Calls())
	}
	if handles.Finalizer.Requests[0].Outcome != agentkit.OutcomeCancelled {
		t.Fatalf("finalizer saw outcome %q, want cancelled", handles.Finalizer.Requests[0].Outcome)
	}
}

func TestProviderToolArgumentsStreamAsTheyArrive(t *testing.T) {
	handles, ports := memadapter.NewPorts(testScope, 0)

	model := &scriptedModel{
		id: "scripted",
		scripts: [][]provider.StreamChunk{
			{
				{Type: provider.ChunkToolCallStreamStart, ToolCallID: "call_1", ToolName: "note"},
				{Type: provider.ChunkToolCallDelta, ToolCallID: "call_1", ToolName: "note", ToolInput: `{"text":`},
				{Type: provider.ChunkToolCallDelta, ToolCallID: "call_1", ToolName: "note", ToolInput: `"hi"}`},
				{Type: provider.ChunkToolCall, ToolCallID: "call_1", ToolName: "note", ToolInput: `{"text":"hi"}`},
			},
			{{Type: provider.ChunkText, Text: "done"}},
		},
		results: []*provider.GenerateResult{
			{
				Text: "", FinishReason: provider.FinishToolCalls,
				ToolCalls: []provider.ToolCall{{ID: "call_1", Name: "note", Input: json.RawMessage(`{"text":"hi"}`)}},
			},
			{Text: "done", FinishReason: provider.FinishStop},
		},
	}

	var gotInput string
	note := agentkit.NewTool[portState]("note", "Record a note.",
		func(_ context.Context, in struct {
			Text string `json:"text"`
		}, _ agentkit.ToolOptions[portState]) (any, error) {
			gotInput = in.Text
			return map[string]any{"ok": true}, nil
		})

	if _, err := runNetwork(t, model, ports, []agentkit.Tool[portState]{note}, nil); err != nil {
		t.Fatalf("run: %v", err)
	}

	chunks := handles.Sink.Chunks()
	var (
		created   int
		completed int
		partID    string
		deltas    []string
	)
	for _, chunk := range chunks {
		switch chunk.Event {
		case agentkit.EventPartCreated:
			if chunk.Data["type"] == "tool-call" {
				created++
				partID, _ = chunk.Data["partId"].(string)
			}
		case agentkit.EventToolArgsDelta:
			deltas = append(deltas, chunk.Data["delta"].(string))
		case agentkit.EventPartCompleted:
			if chunk.Data["type"] == "tool-call" {
				completed++
			}
		}
	}

	if created != 1 || completed != 1 {
		t.Fatalf("tool-call part lifecycle created=%d completed=%d; want exactly one of each — "+
			"the tool loop must not re-publish a part the provider already streamed", created, completed)
	}
	if len(deltas) < 2 {
		t.Fatalf("got %d argument deltas (%v); provider fragments must stream individually", len(deltas), deltas)
	}
	if joined := strings.Join(deltas, ""); joined != `{"text":"hi"}` {
		t.Fatalf("concatenated deltas = %q; they must reconstruct the final parsed input exactly", joined)
	}
	if partID == "" {
		t.Fatal("the streamed tool-call part has no id")
	}
	if gotInput != "hi" {
		t.Fatalf("tool received %q; streaming must not change the input the tool runs with", gotInput)
	}

	// The arguments must reach the wire before the inference's finish chunk
	// is processed — that is what makes this real streaming and not a
	// post-completion animation.
	names := eventNames(chunks)
	firstDelta := indexOfEvent(names, agentkit.EventToolArgsDelta)
	toolOutput := -1
	for i, chunk := range chunks {
		if chunk.Event == agentkit.EventPartCreated && chunk.Data["type"] == "tool-output" {
			toolOutput = i
			break
		}
	}
	if firstDelta == -1 || toolOutput == -1 || firstDelta > toolOutput {
		t.Fatalf("argument deltas (%d) must precede the tool output part (%d)", firstDelta, toolOutput)
	}
}

func TestToolStructuredStreamPublishesOrderedParts(t *testing.T) {
	handles, ports := memadapter.NewPorts(testScope, 0)

	model := &scriptedModel{
		id: "scripted",
		scripts: [][]provider.StreamChunk{
			{{Type: provider.ChunkToolCall, ToolCallID: "call_1", ToolName: "scan", ToolInput: `{}`}},
			{{Type: provider.ChunkText, Text: "done"}},
		},
		results: []*provider.GenerateResult{
			{
				FinishReason: provider.FinishToolCalls,
				ToolCalls:    []provider.ToolCall{{ID: "call_1", Name: "scan", Input: json.RawMessage(`{}`)}},
			},
			{Text: "done", FinishReason: provider.FinishStop},
		},
	}

	scan := agentkit.NewTool[portState]("scan", "Scan the project.",
		func(ctx context.Context, _ struct{}, opts agentkit.ToolOptions[portState]) (any, error) {
			opts.Stream.Status(ctx, agentkit.StatusUpdate{
				Kind: agentkit.ActivityReading, Label: "Reading project files",
				Source: agentkit.ActivityFromTool,
			})
			partID := opts.Stream.Data(ctx, agentkit.DataPart{
				Type: "file-list", Payload: agentkit.JSONValue(`{"count":0}`),
			})
			opts.Stream.DataDelta(ctx, partID, agentkit.JSONValue(`{"count":1}`))
			opts.Stream.Progress(ctx, agentkit.ToolProgress{Completed: 1, Total: 2, Label: "index.ts"})
			opts.Stream.CompleteData(ctx, partID, agentkit.JSONValue(`{"count":2}`))
			return map[string]any{"files": 2}, nil
		})

	if _, err := runNetwork(t, model, ports, []agentkit.Tool[portState]{scan}, nil); err != nil {
		t.Fatalf("run: %v", err)
	}

	chunks := handles.Sink.Chunks()
	var order []string
	dataPartID := ""
	for _, chunk := range chunks {
		partType, _ := chunk.Data["type"].(string)
		switch {
		case chunk.Event == agentkit.EventStatusUpdated:
			order = append(order, "status")
		case chunk.Event == agentkit.EventPartCreated && partType == "file-list":
			order = append(order, "created")
			dataPartID, _ = chunk.Data["partId"].(string)
		case chunk.Event == agentkit.EventDataDelta:
			order = append(order, "delta")
		case chunk.Event == agentkit.EventPartCompleted && partType == "file-list":
			order = append(order, "completed")
		}
	}
	joined := strings.Join(order, ",")
	if !strings.Contains(joined, "created,delta,delta") || !strings.HasSuffix(joined, "completed") {
		t.Fatalf("structured part order = %q; want created -> delta(s) -> completed", joined)
	}
	if dataPartID == "" {
		t.Fatal("a structured data part needs a stable id")
	}

	state, err := handles.State.Load(context.Background(), testScope)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	_ = state
}

func TestStructuredStreamDropsDeltaForUnopenedPart(t *testing.T) {
	handles, ports := memadapter.NewPorts(testScope, 0)

	model := &scriptedModel{
		id: "scripted",
		scripts: [][]provider.StreamChunk{
			{{Type: provider.ChunkToolCall, ToolCallID: "call_1", ToolName: "bad", ToolInput: `{}`}},
			{{Type: provider.ChunkText, Text: "done"}},
		},
		results: []*provider.GenerateResult{
			{
				FinishReason: provider.FinishToolCalls,
				ToolCalls:    []provider.ToolCall{{ID: "call_1", Name: "bad", Input: json.RawMessage(`{}`)}},
			},
			{Text: "done", FinishReason: provider.FinishStop},
		},
	}

	bad := agentkit.NewTool[portState]("bad", "Emit out of order.",
		func(ctx context.Context, _ struct{}, opts agentkit.ToolOptions[portState]) (any, error) {
			// A delta for a part that was never opened would leave the client
			// reducer with an orphan; the contract drops it instead.
			opts.Stream.DataDelta(ctx, "never-opened", agentkit.JSONValue(`{"x":1}`))
			opts.Stream.CompleteData(ctx, "never-opened", agentkit.JSONValue(`{"x":1}`))
			return map[string]any{"ok": true}, nil
		})

	if _, err := runNetwork(t, model, ports, []agentkit.Tool[portState]{bad}, nil); err != nil {
		t.Fatalf("run: %v", err)
	}
	for _, chunk := range handles.Sink.Chunks() {
		if partID, _ := chunk.Data["partId"].(string); partID == "never-opened" {
			t.Fatalf("published %q for a part that was never opened", chunk.Event)
		}
	}
}

func TestApprovalIsIssuedWaitedAndConsumedOnce(t *testing.T) {
	handles, ports := memadapter.NewPorts(testScope, 0)

	model := &scriptedModel{
		id: "scripted",
		scripts: [][]provider.StreamChunk{
			{{Type: provider.ChunkToolCall, ToolCallID: "call_1", ToolName: "publish", ToolInput: `{}`}},
			{{Type: provider.ChunkText, Text: "published"}},
		},
		results: []*provider.GenerateResult{
			{
				FinishReason: provider.FinishToolCalls,
				ToolCalls:    []provider.ToolCall{{ID: "call_1", Name: "publish", Input: json.RawMessage(`{}`)}},
			},
			{Text: "published", FinishReason: provider.FinishStop},
		},
	}

	go func() {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if rec, ok := handles.Approvals.Record("approval_call_1"); ok && rec.Status == agentkit.ApprovalPending {
				handles.Approvals.Decide("approval_call_1", agentkit.ApprovalApproved, "")
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	executed := 0
	publish := agentkit.NewTool[portState]("publish", "Publish the site.",
		func(ctx context.Context, _ struct{}, opts agentkit.ToolOptions[portState]) (any, error) {
			if opts.Approvals == nil || !opts.Approvals.Enabled() {
				return nil, errors.New("refusing to publish without an approval store")
			}
			if _, err := opts.Approvals.Require(ctx, agentkit.ApprovalRequest{
				RequestID: "approval_" + opts.ToolCallID, ToolName: "publish",
				ToolCallID: opts.ToolCallID, Summary: "Publish the site",
				Input: agentkit.JSONValue(`{"credential":"must-stay-server-side"}`),
			}); err != nil {
				return nil, err
			}
			executed++
			return map[string]any{"published": true}, nil
		})

	if _, err := runNetwork(t, model, ports, []agentkit.Tool[portState]{publish}, nil); err != nil {
		t.Fatalf("run: %v", err)
	}

	if executed != 1 {
		t.Fatalf("approved side effect ran %d times, want 1", executed)
	}
	rec, ok := handles.Approvals.Record("approval_call_1")
	if !ok || !rec.Consumed {
		t.Fatalf("approval record = %+v; an approved capability must be consumed exactly once", rec)
	}

	names := handles.Sink.Events()
	if indexOfEvent(names, agentkit.EventHITLRequested) == -1 {
		t.Fatal("an approval request must reach clients as a standard HITL event")
	}
	if indexOfEvent(names, agentkit.EventHITLResolved) == -1 {
		t.Fatal("an approval decision must reach clients as a standard HITL event")
	}
	for _, chunk := range handles.Sink.Chunks() {
		if chunk.Event == agentkit.EventHITLRequested {
			if _, leaked := chunk.Data["input"]; leaked {
				t.Fatal("approval events must not publish raw tool input; Summary is the bounded display field")
			}
		}
	}
}

func TestDeniedApprovalBlocksTheSideEffect(t *testing.T) {
	handles, ports := memadapter.NewPorts(testScope, 0)
	handles.Approvals.AutoDecide = agentkit.ApprovalDenied

	model := &scriptedModel{
		id: "scripted",
		scripts: [][]provider.StreamChunk{
			{{Type: provider.ChunkToolCall, ToolCallID: "call_1", ToolName: "publish", ToolInput: `{}`}},
			{{Type: provider.ChunkText, Text: "ok"}},
		},
		results: []*provider.GenerateResult{
			{
				FinishReason: provider.FinishToolCalls,
				ToolCalls:    []provider.ToolCall{{ID: "call_1", Name: "publish", Input: json.RawMessage(`{}`)}},
			},
			{Text: "ok", FinishReason: provider.FinishStop},
		},
	}

	executed := 0
	publish := agentkit.NewTool[portState]("publish", "Publish the site.",
		func(ctx context.Context, _ struct{}, opts agentkit.ToolOptions[portState]) (any, error) {
			if _, err := opts.Approvals.Require(ctx, agentkit.ApprovalRequest{
				RequestID: "approval_" + opts.ToolCallID, ToolName: "publish", ToolCallID: opts.ToolCallID,
			}); err != nil {
				return nil, err
			}
			executed++
			return map[string]any{"published": true}, nil
		})

	if _, err := runNetwork(t, model, ports, []agentkit.Tool[portState]{publish}, nil); err != nil {
		t.Fatalf("run: %v", err)
	}
	if executed != 0 {
		t.Fatal("a denied approval must not let the side effect run")
	}
	if rec, _ := handles.Approvals.Record("approval_call_1"); rec.Consumed {
		t.Fatal("a denied approval must never be consumed")
	}
}

func TestSessionStateTracksLifecycleAndCursor(t *testing.T) {
	handles, ports := memadapter.NewPorts(testScope, 2)

	if _, err := runNetwork(t, textModel("hi"), ports, nil, nil); err != nil {
		t.Fatalf("run: %v", err)
	}

	state, err := handles.State.Load(context.Background(), testScope)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if state.CurrentThreadID != "thread_1" {
		t.Fatalf("current thread = %q; the server pointer must follow the run", state.CurrentThreadID)
	}
	if state.StreamEpoch != 2 {
		t.Fatalf("stream epoch = %d, want 2", state.StreamEpoch)
	}
	if state.ActiveRun == nil || state.ActiveRun.Outcome != agentkit.OutcomeCompleted {
		t.Fatalf("active run = %+v; want a completed outcome", state.ActiveRun)
	}
	if state.Activity.Kind != agentkit.ActivityNone {
		t.Fatalf("terminal activity = %q; a finished run is not busy", state.Activity.Kind)
	}
	if state.Revision <= agentkit.InitialStateRevision {
		t.Fatalf("revision = %d; committed transitions must advance it", state.Revision)
	}
}

func TestSinkFailureDoesNotBreakTheRun(t *testing.T) {
	handles, ports := memadapter.NewPorts(testScope, 0)
	handles.Sink.Fail = errors.New("socket closed")

	if _, err := runNetwork(t, textModel("hi"), ports, nil, nil); err != nil {
		t.Fatalf("live delivery is best-effort; a sink failure must not fail the run: %v", err)
	}
	if handles.Journal.Len("thread_1") == 0 {
		t.Fatal("the durable tail must still exist when live delivery fails")
	}
}

// memoStep memoizes durable step results so a second execution replays the
// first one's values — the shape of an Inngest replay.
type memoStep struct {
	mu    sync.Mutex
	cache map[string]json.RawMessage
}

func newMemoStep() *memoStep { return &memoStep{cache: map[string]json.RawMessage{}} }

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
