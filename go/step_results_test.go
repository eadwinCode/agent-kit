package agentkit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/eadwinCode/agent-kit/go/durable"
)

type resultStoreFake struct {
	mu       sync.Mutex
	rows     map[string]StoredStepResult
	locators map[string]StepResultLocator
	lookups  int
	puts     int
	resolves int
}

func newResultStoreFake() *resultStoreFake {
	return &resultStoreFake{
		rows: map[string]StoredStepResult{}, locators: map[string]StepResultLocator{},
	}
}

func resultLocatorKey(locator StepResultLocator) string {
	return fmt.Sprintf("%s/%s/%s/%d", locator.Scope, locator.RunID, locator.StepID, locator.SchemaVersion)
}

func cloneStoredResult(stored StoredStepResult) StoredStepResult {
	stored.Payload = append(json.RawMessage(nil), stored.Payload...)
	return stored
}

func (s *resultStoreFake) Lookup(_ context.Context, locator StepResultLocator) (StoredStepResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lookups++
	stored, ok := s.rows[resultLocatorKey(locator)]
	if !ok {
		return StoredStepResult{}, ErrStepResultNotFound
	}
	return cloneStoredResult(stored), nil
}

func (s *resultStoreFake) Put(_ context.Context, locator StepResultLocator, payload json.RawMessage) (StoredStepResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.puts++
	key := resultLocatorKey(locator)
	checksum := checksumStepResult(payload)
	if existing, ok := s.rows[key]; ok {
		if existing.Ref.SHA256 != checksum {
			return StoredStepResult{}, ErrStepResultConflict
		}
		return cloneStoredResult(existing), nil
	}
	stored := StoredStepResult{
		Ref: StepResultRef{
			Ref: fmt.Sprintf("row-%d", len(s.rows)+1), SHA256: checksum,
			SizeBytes: int64(len(payload)), SchemaVersion: locator.SchemaVersion,
		},
		Payload: append(json.RawMessage(nil), payload...),
	}
	s.rows[key] = stored
	s.locators[key] = locator
	return cloneStoredResult(stored), nil
}

func (s *resultStoreFake) Resolve(_ context.Context, locator StepResultLocator, ref StepResultRef) (json.RawMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resolves++
	stored, ok := s.rows[resultLocatorKey(locator)]
	if !ok || stored.Ref.Ref != ref.Ref {
		return nil, ErrStepResultNotFound
	}
	return append(json.RawMessage(nil), stored.Payload...), nil
}

type batchResultStoreFake struct {
	*resultStoreFake
	loads int
}

func newBatchResultStoreFake() *batchResultStoreFake {
	return &batchResultStoreFake{resultStoreFake: newResultStoreFake()}
}

func (s *batchResultStoreFake) LoadRun(_ context.Context, query StepResultRunQuery) (map[string]StoredStepResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loads++
	rows := make(map[string]StoredStepResult)
	for key, locator := range s.locators {
		if locator.Scope != query.Scope || locator.RunID != query.RunID ||
			locator.SchemaVersion != query.SchemaVersion {
			continue
		}
		rows[locator.StepID] = cloneStoredResult(s.rows[key])
	}
	return rows, nil
}

type positionalResultMemoStep struct {
	cache       map[string]json.RawMessage
	occurrences map[string]int
}

func newPositionalResultMemoStep() *positionalResultMemoStep {
	return &positionalResultMemoStep{
		cache: map[string]json.RawMessage{}, occurrences: map[string]int{},
	}
}

func positionalStepKey(id string, occurrence int) string {
	return fmt.Sprintf("%s#%d", id, occurrence)
}

func (s *positionalResultMemoStep) Run(ctx context.Context, id string, fn durable.RunFn) (json.RawMessage, error) {
	key := positionalStepKey(id, s.occurrences[id])
	s.occurrences[id]++
	if raw, ok := s.cache[key]; ok {
		return append(json.RawMessage(nil), raw...), nil
	}
	raw, err := fn(ctx)
	if err == nil {
		s.cache[key] = append(json.RawMessage(nil), raw...)
	}
	return raw, err
}

func (s *positionalResultMemoStep) replay() {
	s.occurrences = map[string]int{}
}

type lostAcknowledgementStep struct{}

func (lostAcknowledgementStep) Run(ctx context.Context, _ string, fn durable.RunFn) (json.RawMessage, error) {
	return fn(ctx)
}

func newStoredTestStep(t *testing.T, inner durable.Step, store StepResultStore) durable.Step {
	t.Helper()
	step, err := NewStepResultStep(inner, store, StepResultStepConfig{
		Scope: "scope-1", RunID: "run-1", SchemaVersion: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return step
}

func TestStepResultStepMemoizesReferenceAndResolvesReplay(t *testing.T) {
	inner := newPositionalResultMemoStep()
	store := newResultStoreFake()
	step := newStoredTestStep(t, inner, store)
	work := 0

	got, err := durable.Run(t.Context(), step, "inference-1", func(context.Context) (map[string]string, error) {
		work++
		return map[string]string{"answer": "hello"}, nil
	})
	if err != nil || got["answer"] != "hello" {
		t.Fatalf("result=%v err=%v", got, err)
	}

	inner.replay()
	step = newStoredTestStep(t, inner, store)
	got, err = durable.Run(t.Context(), step, "inference-1", func(context.Context) (map[string]string, error) {
		t.Fatal("memoized inference body ran on replay")
		return nil, nil
	})
	if err != nil || got["answer"] != "hello" {
		t.Fatalf("replay result=%v err=%v", got, err)
	}

	if work != 1 || store.puts != 1 || store.resolves != 1 || len(store.rows) != 1 {
		t.Fatalf("work=%d puts=%d resolves=%d rows=%d", work, store.puts, store.resolves, len(store.rows))
	}
	cached := inner.cache[positionalStepKey("inference-1", 0)]
	if !strings.Contains(string(cached), `"_agentkitStepResult"`) || strings.Contains(string(cached), "hello") {
		t.Fatalf("Inngest cache contains the wrong value: %s", cached)
	}
}

func TestStepResultStepDisambiguatesRepeatedIDsAndReplaysPositionally(t *testing.T) {
	inner := newPositionalResultMemoStep()
	store := newResultStoreFake()
	step := newStoredTestStep(t, inner, store)
	wants := []string{"first", "second"}
	work := 0
	for _, want := range wants {
		got, err := durable.Run(t.Context(), step, "clevix-designer/infer/0", func(context.Context) (string, error) {
			work++
			return want, nil
		})
		if err != nil || got != want {
			t.Fatalf("initial occurrence result=%q want=%q err=%v", got, want, err)
		}
	}
	if len(store.rows) != 2 {
		t.Fatalf("stored rows=%d, want 2", len(store.rows))
	}

	inner.replay()
	step = newStoredTestStep(t, inner, store)
	for _, want := range wants {
		got, err := durable.Run(t.Context(), step, "clevix-designer/infer/0", func(context.Context) (string, error) {
			t.Fatal("provider body ran for a positionally memoized inference")
			return "", nil
		})
		if err != nil || got != want {
			t.Fatalf("replayed occurrence result=%q want=%q err=%v", got, want, err)
		}
	}
	if work != 2 || store.puts != 2 || store.resolves != 2 {
		t.Fatalf("work=%d puts=%d resolves=%d", work, store.puts, store.resolves)
	}
}

func TestStepResultStepBulkLoadsRunOncePerWorkflowCallback(t *testing.T) {
	inner := newPositionalResultMemoStep()
	store := newBatchResultStoreFake()
	step := newStoredTestStep(t, inner, store)
	wants := []string{"first", "second"}
	for _, want := range wants {
		got, err := durable.Run(t.Context(), step, "clevix-designer/infer/0", func(context.Context) (string, error) {
			return want, nil
		})
		if err != nil || got != want {
			t.Fatalf("initial result=%q want=%q err=%v", got, want, err)
		}
	}
	if store.loads != 1 || store.lookups != 2 || store.resolves != 0 || store.puts != 2 {
		t.Fatalf("initial loads=%d lookups=%d resolves=%d puts=%d", store.loads, store.lookups, store.resolves, store.puts)
	}

	inner.replay()
	step = newStoredTestStep(t, inner, store)
	for _, want := range wants {
		got, err := durable.Run(t.Context(), step, "clevix-designer/infer/0", func(context.Context) (string, error) {
			t.Fatal("provider body ran during bulk-loaded replay")
			return "", nil
		})
		if err != nil || got != want {
			t.Fatalf("replayed result=%q want=%q err=%v", got, want, err)
		}
	}
	if store.loads != 2 || store.lookups != 2 || store.resolves != 0 || store.puts != 2 {
		t.Fatalf("replay loads=%d lookups=%d resolves=%d puts=%d", store.loads, store.lookups, store.resolves, store.puts)
	}
}

func TestStepResultStepBulkSnapshotMissFallsBackToFreshLookup(t *testing.T) {
	inner := newPositionalResultMemoStep()
	store := newBatchResultStoreFake()
	step := newStoredTestStep(t, inner, store)
	wrapped := step.(*stepResultStep)
	if err := wrapped.loadRun(t.Context()); err != nil {
		t.Fatal(err)
	}
	locator := StepResultLocator{
		Scope: "scope-1", RunID: "run-1",
		StepID: qualifiedStepResultID("late", 0), SchemaVersion: 1,
	}
	if _, err := store.Put(t.Context(), locator, json.RawMessage(`"from-concurrent-attempt"`)); err != nil {
		t.Fatal(err)
	}

	got, err := durable.Run(t.Context(), step, "late", func(context.Context) (string, error) {
		t.Fatal("work ran despite a fresh point lookup finding the durable result")
		return "", nil
	})
	if err != nil || got != "from-concurrent-attempt" {
		t.Fatalf("result=%q err=%v", got, err)
	}
	if store.loads != 1 || store.lookups != 1 || store.puts != 1 {
		t.Fatalf("loads=%d lookups=%d puts=%d", store.loads, store.lookups, store.puts)
	}
}

func TestStepResultStepResolvesLegacyBareStepReference(t *testing.T) {
	inner := newPositionalResultMemoStep()
	store := newResultStoreFake()
	legacy := StepResultLocator{
		Scope: "scope-1", RunID: "run-1", StepID: "inference-1", SchemaVersion: 1,
	}
	stored, err := store.Put(t.Context(), legacy, json.RawMessage(`{"answer":"legacy"}`))
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := marshalStepResultEnvelope(stored.Ref)
	if err != nil {
		t.Fatal(err)
	}
	inner.cache[positionalStepKey("inference-1", 0)] = envelope

	step := newStoredTestStep(t, inner, store)
	got, err := durable.Run(t.Context(), step, "inference-1", func(context.Context) (map[string]string, error) {
		t.Fatal("legacy memoized inference body ran")
		return nil, nil
	})
	if err != nil || got["answer"] != "legacy" {
		t.Fatalf("result=%v err=%v", got, err)
	}
	if store.resolves != 2 {
		t.Fatalf("resolve calls=%d, want qualified miss plus legacy resolve", store.resolves)
	}
}

func TestStepResultStepLookupClosesLostAcknowledgementGap(t *testing.T) {
	store := newResultStoreFake()
	work := 0
	for range 2 {
		step := newStoredTestStep(t, lostAcknowledgementStep{}, store)
		got, err := durable.Run(t.Context(), step, "tool-1", func(context.Context) (string, error) {
			work++
			return "edited", nil
		})
		if err != nil || got != "edited" {
			t.Fatalf("got=%q err=%v", got, err)
		}
	}
	if work != 1 || store.lookups != 2 || store.puts != 1 {
		t.Fatalf("work=%d lookups=%d puts=%d", work, store.lookups, store.puts)
	}
}

func TestStepResultStepReadsLegacyInlineMemoizedValue(t *testing.T) {
	inner := newPositionalResultMemoStep()
	inner.cache[positionalStepKey("legacy", 0)] = json.RawMessage(`{"answer":"inline"}`)
	store := newResultStoreFake()
	step := newStoredTestStep(t, inner, store)
	got, err := durable.Run(t.Context(), step, "legacy", func(context.Context) (map[string]string, error) {
		t.Fatal("legacy memoized body ran")
		return nil, nil
	})
	if err != nil || got["answer"] != "inline" {
		t.Fatalf("got=%v err=%v", got, err)
	}
	if store.lookups != 0 || store.puts != 0 || store.resolves != 0 {
		t.Fatalf("legacy result touched store: %+v", store)
	}
}

func TestStepResultPayloadLimitIsExactAndFailClosed(t *testing.T) {
	atLimit := json.RawMessage(`"` + strings.Repeat("a", int(MaxDurableStepResultBytes)-2) + `"`)
	if err := validateStepResultPayload("limit", atLimit); err != nil {
		t.Fatalf("exact limit: %v", err)
	}
	over := append(append(json.RawMessage(nil), atLimit...), ' ')
	err := validateStepResultPayload("over", over)
	var typed *StepResultTooLargeError
	if !errors.Is(err, ErrStepResultTooLarge) || !errors.As(err, &typed) ||
		typed.SizeBytes != MaxDurableStepResultBytes+1 {
		t.Fatalf("oversize error=%#v", err)
	}
}

func TestStepResultStepAutomaticallyRecomputesOnlyOversizeResults(t *testing.T) {
	inner := newPositionalResultMemoStep()
	store := newResultStoreFake()
	step := newStoredTestStep(t, inner, store)

	smallRuns := 0
	got, err := durable.Run(t.Context(), step, "small-result", func(context.Context) (string, error) {
		smallRuns++
		return "small", nil
	})
	if err != nil || got != "small" {
		t.Fatalf("small result=%q err=%v", got, err)
	}

	largeRuns := 0
	got, err = durable.Run(t.Context(), step, "large-result", func(context.Context) (string, error) {
		largeRuns++
		return fmt.Sprintf("%d:%s", largeRuns, strings.Repeat("x", int(MaxDurableStepResultBytes))), nil
	})
	if err != nil || !strings.HasPrefix(got, "1:") {
		t.Fatalf("initial large result prefix=%q err=%v", got[:min(len(got), 8)], err)
	}

	inner.replay()
	step = newStoredTestStep(t, inner, store)
	got, err = durable.Run(t.Context(), step, "small-result", func(context.Context) (string, error) {
		t.Fatal("small memoized body ran on replay")
		return "", nil
	})
	if err != nil || got != "small" {
		t.Fatalf("replayed small result=%q err=%v", got, err)
	}
	got, err = durable.Run(t.Context(), step, "large-result", func(context.Context) (string, error) {
		largeRuns++
		return fmt.Sprintf("%d:%s", largeRuns, strings.Repeat("x", int(MaxDurableStepResultBytes))), nil
	})
	if err != nil || !strings.HasPrefix(got, "2:") {
		t.Fatalf("replayed large result prefix=%q err=%v", got[:min(len(got), 8)], err)
	}

	if smallRuns != 1 || largeRuns != 2 || store.puts != 1 || len(store.rows) != 1 {
		t.Fatalf("small=%d large=%d puts=%d rows=%d", smallRuns, largeRuns, store.puts, len(store.rows))
	}
	marker := inner.cache[positionalStepKey("large-result", 0)]
	if !strings.Contains(string(marker), `"mode":"recompute_oversize"`) ||
		len(marker) >= 512 || strings.Contains(string(marker), strings.Repeat("x", 32)) {
		t.Fatalf("oversize marker is not bounded: bytes=%d body=%s", len(marker), marker)
	}
}

func TestRecomputeToolBypassesDurableStep(t *testing.T) {
	state := NewState(StateConfig[map[string]int]{Data: map[string]int{}})
	network := newNetworkRun(NewNetwork(NetworkConfig[map[string]int]{Name: "test"}), state)
	agent := NewAgent(AgentConfig[map[string]int]{Name: "reader"})
	step := &countingAgentStep{}
	runs := 0
	tool := Tool[map[string]int]{
		Name: "read_file", ReplayPolicy: ReplayRecompute,
		Handler: func(context.Context, json.RawMessage, ToolOptions[map[string]int]) (any, error) {
			runs++
			return map[string]int{"run": runs}, nil
		},
	}
	for range 2 {
		result := agent.runToolHandler(t.Context(), tool, NewToolMessage("call", "read_file", nil), network, nil, step)
		if result.IsError() {
			t.Fatalf("tool result=%s", result.Data())
		}
	}
	if step.calls != 0 || runs != 2 {
		t.Fatalf("step calls=%d handler runs=%d", step.calls, runs)
	}
}

func TestRecomputeToolRejectsAndRestoresStateMutation(t *testing.T) {
	state := NewState(StateConfig[map[string]int]{Data: map[string]int{"count": 1}})
	network := newNetworkRun(NewNetwork(NetworkConfig[map[string]int]{Name: "test"}), state)
	agent := NewAgent(AgentConfig[map[string]int]{Name: "reader"})
	tool := Tool[map[string]int]{
		Name: "bad_read", ReplayPolicy: ReplayRecompute,
		Handler: func(_ context.Context, _ json.RawMessage, opts ToolOptions[map[string]int]) (any, error) {
			opts.State.Data["count"] = 2
			return map[string]bool{"ok": true}, nil
		},
	}
	result := agent.runToolHandler(t.Context(), tool, NewToolMessage("call", "bad_read", nil), network, nil, &countingAgentStep{})
	if !result.IsError() || state.Data["count"] != 1 {
		t.Fatalf("result error=%v state=%v", result.IsError(), state.Data)
	}
}

type countingAgentStep struct{ calls int }

func (s *countingAgentStep) Run(ctx context.Context, _ string, fn durable.RunFn) (json.RawMessage, error) {
	s.calls++
	return fn(ctx)
}
