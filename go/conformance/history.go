package conformance

// History adapter conformance.
//
// HistoryConfig is the oldest port and the one every application implements
// first, which makes it the one where "it worked in my test" diverges most
// from "it survives a replay". The suite below runs a real agent and a real
// network against an adapter and asserts the properties AgentKit's callers
// depend on: hooks invoked in the documented order, `Get` skipped in
// client-authoritative mode, `AppendResults` receiving only this run's
// results, and — the one that costs real money to get wrong — no duplicate
// thread, user turn, or result when the function body re-executes.

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/zendev-sh/goai/provider"

	agentkit "github.com/eadwinCode/agent-kit/go"
	"github.com/eadwinCode/agent-kit/go/durable"
)

// HistoryAdapter is the test-side view of a history adapter: the contract it
// supplies plus a way to read back what it stored. Implement it in your
// adapter's test package.
type HistoryAdapter[T any] interface {
	// Config returns the HistoryConfig handed to an agent or network.
	Config() *agentkit.HistoryConfig[T]
	// UserMessages returns the user turns persisted for a thread, in order.
	UserMessages(threadID string) []agentkit.UserMessageRecord
	// Results returns the results persisted for a thread, in order.
	Results(threadID string) []*agentkit.AgentResult
}

// recorder wraps an adapter's hooks to observe invocation order without
// requiring the adapter to expose one. The suite decorates rather than
// interrogates, so an adapter only implements storage.
type recorder[T any] struct {
	mu    sync.Mutex
	calls []string
}

func (r *recorder[T]) record(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, name)
}

func (r *recorder[T]) order() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

// wrap decorates every configured hook with a call recorder.
func (r *recorder[T]) wrap(cfg *agentkit.HistoryConfig[T]) *agentkit.HistoryConfig[T] {
	wrapped := &agentkit.HistoryConfig[T]{}
	if cfg.CreateThread != nil {
		wrapped.CreateThread = func(ctx context.Context, hctx agentkit.HistoryContext[T]) (agentkit.CreateThreadResult, error) {
			r.record("createThread")
			return cfg.CreateThread(ctx, hctx)
		}
	}
	if cfg.Get != nil {
		wrapped.Get = func(ctx context.Context, hctx agentkit.HistoryContext[T]) ([]*agentkit.AgentResult, error) {
			r.record("get")
			return cfg.Get(ctx, hctx)
		}
	}
	if cfg.AppendUserMessage != nil {
		wrapped.AppendUserMessage = func(ctx context.Context, hctx agentkit.HistoryContext[T], msg agentkit.UserMessageRecord) error {
			r.record("appendUserMessage")
			return cfg.AppendUserMessage(ctx, hctx, msg)
		}
	}
	if cfg.AppendResults != nil {
		wrapped.AppendResults = func(ctx context.Context, hctx agentkit.HistoryContext[T], newResults []*agentkit.AgentResult) error {
			r.record("appendResults")
			return cfg.AppendResults(ctx, hctx, newResults)
		}
	}
	return wrapped
}

// stubModel returns one canned response per call. History conformance is
// about persistence, not inference, so the model is deliberately inert.
type stubModel struct {
	mu    sync.Mutex
	text  string
	calls int
}

func (m *stubModel) ModelID() string { return "conformance-stub" }

func (m *stubModel) result() *provider.GenerateResult {
	m.mu.Lock()
	m.calls++
	m.mu.Unlock()
	return &provider.GenerateResult{
		Text: m.text, FinishReason: provider.FinishStop,
		Usage: provider.Usage{InputTokens: 4, OutputTokens: 2, TotalTokens: 6},
	}
}

func (m *stubModel) DoGenerate(context.Context, provider.GenerateParams) (*provider.GenerateResult, error) {
	return m.result(), nil
}

func (m *stubModel) DoStream(ctx context.Context, _ provider.GenerateParams) (*provider.StreamResult, error) {
	result := m.result()
	out := make(chan provider.StreamChunk, 2)
	go func() {
		defer close(out)
		provider.TrySend(ctx, out, provider.StreamChunk{Type: provider.ChunkText, Text: result.Text})
		out <- provider.StreamChunk{
			Type: provider.ChunkFinish, FinishReason: result.FinishReason, Usage: result.Usage,
		}
	}()
	return &provider.StreamResult{Stream: out}, nil
}

// memoStep memoizes step results, which is what an Inngest replay looks like
// from the runtime's point of view: the body re-executes, but every step that
// already completed returns its recorded value instead of running again.
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

// VerifyHistoryConfig runs an adapter through the documented history
// lifecycle. `newAdapter` must return a fresh, empty adapter each call.
//
//	func TestMyHistory(t *testing.T) {
//		conformance.VerifyHistoryConfig(t, func(t *testing.T) conformance.HistoryAdapter[MyState] {
//			return newMyHistory(t)
//		})
//	}
func VerifyHistoryConfig[T any](t *testing.T, newAdapter func(*testing.T) HistoryAdapter[T]) {
	t.Helper()

	// build wires one network run against a fresh adapter.
	build := func(t *testing.T) (
		*agentkit.Network[T], HistoryAdapter[T], *recorder[T],
	) {
		t.Helper()
		adapter := newAdapter(t)
		rec := &recorder[T]{}
		agent := agentkit.NewAgent(agentkit.AgentConfig[T]{
			Name: "assistant", System: "be brief", Model: &stubModel{text: "answer"},
		})
		routed := false
		network := agentkit.NewNetwork(agentkit.NetworkConfig[T]{
			Name:    "conformance",
			Agents:  []*agentkit.Agent[T]{agent},
			MaxIter: 2,
			History: rec.wrap(adapter.Config()),
			Router: &agentkit.Router[T]{
				Fn: func(context.Context, agentkit.RouterArgs[T]) (*agentkit.RouterResult[T], error) {
					if routed {
						return nil, nil
					}
					routed = true
					return agentkit.RouteTo(agent), nil
				},
			},
		})
		return network, adapter, rec
	}

	t.Run("creates a thread and runs against it", func(t *testing.T) {
		network, adapter, _ := build(t)
		run, err := network.Run(context.Background(), "hello", &agentkit.NetworkRunOptions[T]{
			Step: durable.Inline{},
		})
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		if run.State.ThreadID == "" {
			t.Fatal("CreateThread must give the run a thread id")
		}
		if len(adapter.Results(run.State.ThreadID)) == 0 {
			t.Fatal("the run's results were not persisted for its thread")
		}
	})

	t.Run("invokes hooks in the documented order", func(t *testing.T) {
		network, _, rec := build(t)
		if _, err := network.Run(context.Background(), "hello", &agentkit.NetworkRunOptions[T]{
			UserMessage: &agentkit.UserMessage{ID: "user_1", Content: "hello", Role: agentkit.RoleUser},
			Step:        durable.Inline{},
		}); err != nil {
			t.Fatalf("run: %v", err)
		}
		order := rec.order()
		if len(order) == 0 {
			t.Fatal("no history hooks were invoked")
		}
		if order[0] != "createThread" {
			t.Fatalf("first hook = %q; the thread must exist before anything is written to it", order[0])
		}
		// The user's turn is persisted BEFORE any agent runs, so a run that
		// fails mid-inference still leaves the intent on disk to regenerate from.
		userAt := indexOf(order, "appendUserMessage")
		resultsAt := indexOf(order, "appendResults")
		if userAt == -1 {
			t.Fatal("AppendUserMessage was never called")
		}
		if resultsAt != -1 && userAt > resultsAt {
			t.Fatalf("hook order %v: the user turn must be persisted before any result", order)
		}
	})

	t.Run("persists the client's canonical user message id", func(t *testing.T) {
		network, adapter, _ := build(t)
		run, err := network.Run(context.Background(), "hello", &agentkit.NetworkRunOptions[T]{
			UserMessage: &agentkit.UserMessage{ID: "user_1", Content: "hello", Role: agentkit.RoleUser},
			Step:        durable.Inline{},
		})
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		messages := adapter.UserMessages(run.State.ThreadID)
		if len(messages) != 1 {
			t.Fatalf("persisted %d user messages, want 1", len(messages))
		}
		if messages[0].ID != "user_1" {
			t.Fatalf("user message id = %q; the client's id must survive, or the "+
				"optimistic message on screen and the stored one are different messages",
				messages[0].ID)
		}
		if messages[0].Content != "hello" {
			t.Fatalf("user message content = %q", messages[0].Content)
		}
		if messages[0].Role != agentkit.RoleUser {
			t.Fatalf("user message role = %q", messages[0].Role)
		}
	})

	t.Run("skips Get in client-authoritative mode", func(t *testing.T) {
		// When the client supplied conversation state, the UI owns it. Loading
		// storage over the top would silently discard unsent context.
		network, _, rec := build(t)
		seeded := agentkit.NewAgentResult("assistant", []agentkit.Message{{
			Type: agentkit.MessageText, Role: agentkit.RoleAssistant,
			Content: agentkit.TextContent("earlier answer"),
		}}, nil, agentkit.Now())

		state := agentkit.NewState(agentkit.StateConfig[T]{
			ThreadID: "client_thread",
			Results:  []*agentkit.AgentResult{seeded},
		})
		if _, err := network.Run(context.Background(), "hello", &agentkit.NetworkRunOptions[T]{
			State: state, Step: durable.Inline{},
		}); err != nil {
			t.Fatalf("run: %v", err)
		}
		if indexOf(rec.order(), "get") != -1 {
			t.Fatalf("Get was called with client-supplied results: %v", rec.order())
		}
	})

	t.Run("hydrates from Get for an existing thread", func(t *testing.T) {
		network, _, rec := build(t)
		state := agentkit.NewState(agentkit.StateConfig[T]{ThreadID: "existing_thread"})
		if _, err := network.Run(context.Background(), "hello", &agentkit.NetworkRunOptions[T]{
			State: state, Step: durable.Inline{},
		}); err != nil {
			t.Fatalf("run: %v", err)
		}
		if adapterHasGet(network) && indexOf(rec.order(), "get") == -1 {
			t.Fatalf("Get was not called for an existing thread: %v", rec.order())
		}
	})

	t.Run("upserts a client-provided thread rather than replacing it", func(t *testing.T) {
		network, _, _ := build(t)
		state := agentkit.NewState(agentkit.StateConfig[T]{ThreadID: "client_thread"})
		run, err := network.Run(context.Background(), "hello", &agentkit.NetworkRunOptions[T]{
			State: state, Step: durable.Inline{},
		})
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		if run.State.ThreadID != "client_thread" {
			t.Fatalf("thread id = %q; a client-provided id is the source of truth "+
				"and must not be overwritten by CreateThread", run.State.ThreadID)
		}
	})

	t.Run("appends only the results this run produced", func(t *testing.T) {
		network, adapter, _ := build(t)
		seeded := agentkit.NewAgentResult("assistant", []agentkit.Message{{
			Type: agentkit.MessageText, Role: agentkit.RoleAssistant,
			Content: agentkit.TextContent("from a previous run"),
		}}, nil, agentkit.Now())
		state := agentkit.NewState(agentkit.StateConfig[T]{
			ThreadID: "client_thread",
			Results:  []*agentkit.AgentResult{seeded},
		})

		run, err := network.Run(context.Background(), "hello", &agentkit.NetworkRunOptions[T]{
			State: state, Step: durable.Inline{},
		})
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		for _, stored := range adapter.Results(run.State.ThreadID) {
			for _, message := range stored.Output {
				if text, ok := message.Content.AsString(); ok &&
					strings.Contains(text, "from a previous run") {
					t.Fatal("AppendResults received a result from a previous run; " +
						"re-persisting old results duplicates history on every turn")
				}
			}
		}
	})

	t.Run("a replay does not duplicate the thread, the user turn, or results", func(t *testing.T) {
		// Inngest re-executes the function body from the top on every step
		// boundary. Without memoized writes, one turn becomes N threads and N
		// copies of the same message.
		adapter := newAdapter(t)
		rec := &recorder[T]{}
		agent := agentkit.NewAgent(agentkit.AgentConfig[T]{
			Name: "assistant", System: "be brief", Model: &stubModel{text: "answer"},
		})
		wrapped := rec.wrap(adapter.Config())
		step := newMemoStep()

		runOnce := func() *agentkit.NetworkRun[T] {
			routed := false
			network := agentkit.NewNetwork(agentkit.NetworkConfig[T]{
				Name: "conformance", Agents: []*agentkit.Agent[T]{agent},
				MaxIter: 2, History: wrapped,
				Router: &agentkit.Router[T]{
					Fn: func(context.Context, agentkit.RouterArgs[T]) (*agentkit.RouterResult[T], error) {
						if routed {
							return nil, nil
						}
						routed = true
						return agentkit.RouteTo(agent), nil
					},
				},
			})
			run, err := network.Run(context.Background(), "hello", &agentkit.NetworkRunOptions[T]{
				UserMessage: &agentkit.UserMessage{ID: "user_1", Content: "hello", Role: agentkit.RoleUser},
				Step:        step,
			})
			if err != nil {
				t.Fatalf("run: %v", err)
			}
			return run
		}

		first := runOnce()
		firstResults := len(adapter.Results(first.State.ThreadID))
		second := runOnce()

		if second.State.ThreadID != first.State.ThreadID {
			t.Fatalf("replay produced thread %q, first run produced %q; thread creation "+
				"must be memoized or every retry starts a new conversation",
				second.State.ThreadID, first.State.ThreadID)
		}
		if messages := adapter.UserMessages(first.State.ThreadID); len(messages) != 1 {
			t.Fatalf("replay persisted %d user messages, want 1", len(messages))
		}
		if got := len(adapter.Results(first.State.ThreadID)); got != firstResults {
			t.Fatalf("replay grew stored results from %d to %d; result writes must be "+
				"idempotent under replay", firstResults, got)
		}

		// Count invocations, not just outcomes. Asserting only "no duplicate
		// rows" would pass for a runtime that called the write twice and an
		// adapter that happened to dedupe; the memoized step means the second
		// execution must not reach the adapter at all.
		order := rec.order()
		if creates := count(order, "createThread"); creates != 1 {
			t.Fatalf("CreateThread ran %d times across two executions, want 1; "+
				"the create step is no longer memoized", creates)
		}
		if appends := count(order, "appendUserMessage"); appends != 1 {
			t.Fatalf("AppendUserMessage ran %d times across two executions, want 1; "+
				"the user-turn write is no longer memoized", appends)
		}
	})

	t.Run("the adapter is idempotent on its own", func(t *testing.T) {
		// Step memoization only covers re-execution WITHIN one durable run. An
		// HTTP retry that re-enters the workflow starts a fresh run with fresh
		// step state, so the same client message id arrives again against a
		// storage layer with no memo to hide behind. The adapter itself has to
		// hold the line.
		adapter := newAdapter(t)
		cfg := adapter.Config()
		ctx := context.Background()

		created, err := cfg.CreateThread(ctx, agentkit.HistoryContext[T]{})
		if err != nil {
			t.Fatalf("CreateThread: %v", err)
		}
		threadID := created.ThreadID
		hctx := agentkit.HistoryContext[T]{ThreadID: threadID}

		if again, err := cfg.CreateThread(ctx, agentkit.HistoryContext[T]{ThreadID: threadID}); err != nil {
			t.Fatalf("re-CreateThread: %v", err)
		} else if again.ThreadID != threadID {
			t.Fatalf("re-creating thread %q returned %q; CreateThread must upsert, "+
				"so a retried run keeps its conversation", threadID, again.ThreadID)
		}

		if cfg.AppendUserMessage != nil {
			msg := agentkit.UserMessageRecord{
				ID: "user_1", Content: "hello", Role: agentkit.RoleUser, Timestamp: agentkit.Now(),
			}
			for i := 0; i < 2; i++ {
				if err := cfg.AppendUserMessage(ctx, hctx, msg); err != nil {
					t.Fatalf("AppendUserMessage attempt %d: %v", i+1, err)
				}
			}
			if got := len(adapter.UserMessages(threadID)); got != 1 {
				t.Fatalf("appending one message id twice stored %d messages; the adapter "+
					"must dedupe on the message id — a retried request is not a second message", got)
			}
		}

		if cfg.AppendResults != nil {
			result := agentkit.NewAgentResult("assistant", []agentkit.Message{{
				Type: agentkit.MessageText, Role: agentkit.RoleAssistant,
				Content: agentkit.TextContent("answer"),
			}}, nil, agentkit.Now())
			result.ID = "message_1"
			for i := 0; i < 2; i++ {
				if err := cfg.AppendResults(ctx, hctx, []*agentkit.AgentResult{result}); err != nil {
					t.Fatalf("AppendResults attempt %d: %v", i+1, err)
				}
			}
			if got := len(adapter.Results(threadID)); got != 1 {
				t.Fatalf("appending one result twice stored %d results; dedupe on the "+
					"result's replay-stable checksum or id", got)
			}
		}
	})

	t.Run("a standalone agent run persists through the same contract", func(t *testing.T) {
		adapter := newAdapter(t)
		rec := &recorder[T]{}
		agent := agentkit.NewAgent(agentkit.AgentConfig[T]{
			Name: "assistant", System: "be brief",
			Model:   &stubModel{text: "answer"},
			History: rec.wrap(adapter.Config()),
		})
		if _, err := agent.Run(context.Background(), "hello", &agentkit.RunOptions[T]{
			Step: durable.Inline{},
		}); err != nil {
			t.Fatalf("run: %v", err)
		}
		if indexOf(rec.order(), "createThread") == -1 {
			t.Fatalf("a standalone run did not create a thread: %v", rec.order())
		}
	})
}

// adapterHasGet reports whether the network's history config implements Get,
// so the suite does not demand a hook the adapter did not supply.
func adapterHasGet[T any](network *agentkit.Network[T]) bool {
	return network.History != nil && network.History.Get != nil
}

func count(values []string, want string) int {
	n := 0
	for _, value := range values {
		if value == want {
			n++
		}
	}
	return n
}

func indexOf(values []string, want string) int {
	for i, value := range values {
		if value == want {
			return i
		}
	}
	return -1
}
