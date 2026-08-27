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
	lookups  int
	puts     int
	resolves int
}

func newResultStoreFake() *resultStoreFake {
	return &resultStoreFake{rows: map[string]StoredStepResult{}}
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

type resultMemoStep struct {
	cache map[string]json.RawMessage
}

func (s *resultMemoStep) Run(ctx context.Context, id string, fn durable.RunFn) (json.RawMessage, error) {
	if raw, ok := s.cache[id]; ok {
		return append(json.RawMessage(nil), raw...), nil
	}
	raw, err := fn(ctx)
	if err == nil {
		s.cache[id] = append(json.RawMessage(nil), raw...)
	}
	return raw, err
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
	inner := &resultMemoStep{cache: map[string]json.RawMessage{}}
	store := newResultStoreFake()
	step := newStoredTestStep(t, inner, store)
	work := 0

	for range 2 {
		got, err := durable.Run(t.Context(), step, "inference-1", func(context.Context) (map[string]string, error) {
			work++
			return map[string]string{"answer": "hello"}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if got["answer"] != "hello" {
			t.Fatalf("result=%v", got)
		}
	}

	if work != 1 || store.puts != 1 || store.resolves != 1 || len(store.rows) != 1 {
		t.Fatalf("work=%d puts=%d resolves=%d rows=%d", work, store.puts, store.resolves, len(store.rows))
	}
	if !strings.Contains(string(inner.cache["inference-1"]), `"_agentkitStepResult"`) ||
		strings.Contains(string(inner.cache["inference-1"]), "hello") {
		t.Fatalf("Inngest cache contains the wrong value: %s", inner.cache["inference-1"])
	}
}

func TestStepResultStepLookupClosesLostAcknowledgementGap(t *testing.T) {
	store := newResultStoreFake()
	step := newStoredTestStep(t, lostAcknowledgementStep{}, store)
	work := 0
	for range 2 {
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
	inner := &resultMemoStep{cache: map[string]json.RawMessage{
		"legacy": json.RawMessage(`{"answer":"inline"}`),
	}}
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

func TestStepResultStepRejectsOversizeWithoutPersistingOrRecomputing(t *testing.T) {
	store := newResultStoreFake()
	step := newStoredTestStep(t, durable.Inline{}, store)
	work := 0
	_, err := durable.Run(t.Context(), step, "oversize", func(context.Context) (string, error) {
		work++
		return strings.Repeat("x", int(MaxDurableStepResultBytes)), nil
	})
	if !errors.Is(err, ErrStepResultTooLarge) || work != 1 || store.puts != 0 {
		t.Fatalf("err=%v work=%d puts=%d", err, work, store.puts)
	}
}

func TestStepResultStepRecomputesOnlyOversizeResults(t *testing.T) {
	inner := &resultMemoStep{cache: map[string]json.RawMessage{}}
	store := newResultStoreFake()
	step := newStoredTestStep(t, inner, store)

	smallRuns := 0
	for range 2 {
		got, err := durable.RunWithOptions(t.Context(), step, "small-read", durable.RunOptions{
			ReplayPolicy: ReplayRecomputeOversize,
		}, func(context.Context) (string, error) {
			smallRuns++
			return "small", nil
		})
		if err != nil || got != "small" {
			t.Fatalf("small result=%q err=%v", got, err)
		}
	}

	largeRuns := 0
	for wantRun := 1; wantRun <= 2; wantRun++ {
		got, err := durable.RunWithOptions(t.Context(), step, "large-read", durable.RunOptions{
			ReplayPolicy: ReplayRecomputeOversize,
		}, func(context.Context) (string, error) {
			largeRuns++
			return fmt.Sprintf("%d:%s", largeRuns, strings.Repeat("x", int(MaxDurableStepResultBytes))), nil
		})
		if err != nil || !strings.HasPrefix(got, fmt.Sprintf("%d:", wantRun)) {
			t.Fatalf("large result prefix=%q err=%v", got[:min(len(got), 8)], err)
		}
	}

	if smallRuns != 1 || largeRuns != 2 || store.puts != 1 || len(store.rows) != 1 {
		t.Fatalf("small=%d large=%d puts=%d rows=%d", smallRuns, largeRuns, store.puts, len(store.rows))
	}
	marker := inner.cache["large-read"]
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
	for _, policy := range []ReplayPolicy{ReplayRecompute, ReplayRecomputeOversize} {
		t.Run(string(policy), func(t *testing.T) {
			state := NewState(StateConfig[map[string]int]{Data: map[string]int{"count": 1}})
			network := newNetworkRun(NewNetwork(NetworkConfig[map[string]int]{Name: "test"}), state)
			agent := NewAgent(AgentConfig[map[string]int]{Name: "reader"})
			step := durable.Step(&countingAgentStep{})
			if policy == ReplayRecomputeOversize {
				step = newStoredTestStep(t, durable.Inline{}, newResultStoreFake())
			}
			tool := Tool[map[string]int]{
				Name: "bad_read", ReplayPolicy: policy,
				Handler: func(_ context.Context, _ json.RawMessage, opts ToolOptions[map[string]int]) (any, error) {
					opts.State.Data["count"] = 2
					return map[string]bool{"ok": true}, nil
				},
			}
			result := agent.runToolHandler(t.Context(), tool, NewToolMessage("call", "bad_read", nil), network, nil, step)
			if !result.IsError() || state.Data["count"] != 1 {
				t.Fatalf("result error=%v state=%v", result.IsError(), state.Data)
			}
		})
	}
}

type countingAgentStep struct{ calls int }

func (s *countingAgentStep) Run(ctx context.Context, _ string, fn durable.RunFn) (json.RawMessage, error) {
	s.calls++
	return fn(ctx)
}
