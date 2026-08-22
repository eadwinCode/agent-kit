package agentkit

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/eadwinCode/agent-kit/go/durable"
	"github.com/eadwinCode/agent-kit/go/internal/jsonutil"
)

// HistoryConfig manages conversation history for agents and networks:
// creating threads, loading prior results, and persisting new ones, so
// conversations span runs with full context.
//
// Hooks receive the run's durable.Step. Unlike TS — which passes
// `step: undefined` into hooks that are already inside a step — the Step
// here is always usable: nested durable.Run calls collapse to inline
// execution automatically, so a hook can wrap its DB work in durable.Run
// unconditionally — no manual nesting-avoidance is needed in the adapter.
type HistoryConfig[T any] struct {
	// CreateThread creates (or upserts) a conversation thread. Called
	// before any agents run when no ThreadID exists — or when one does,
	// so adapters can ensure it exists in storage.
	CreateThread func(ctx context.Context, hctx HistoryContext[T]) (CreateThreadResult, error)

	// Get loads initial conversation history. When it returns results,
	// they replace anything seeded into the state. Skipped entirely when
	// the client provided results or messages (client-authoritative mode).
	Get func(ctx context.Context, hctx HistoryContext[T]) ([]*AgentResult, error)

	// AppendUserMessage persists the user's message at the start of a run,
	// so intent is captured even if the run fails (enables "regenerate").
	AppendUserMessage func(ctx context.Context, hctx HistoryContext[T], msg UserMessageRecord) error

	// AppendResults saves new results generated during the current run.
	// AgentKit wraps the call in one durable step under a deterministic id
	// (see PersistResults) — the hook body runs inline within it.
	AppendResults func(ctx context.Context, hctx HistoryContext[T], newResults []*AgentResult) error
}

// HistoryContext carries run context into history hooks.
type HistoryContext[T any] struct {
	State   *State[T]
	Network *NetworkRun[T]
	Step    durable.Step
	// Input is the user's input for this conversation turn.
	Input string
	// ThreadID is set for Get/AppendResults; empty during CreateThread.
	ThreadID string
}

// CreateThreadResult returns the new thread's id.
type CreateThreadResult struct {
	ThreadID string `json:"threadId"`
}

// UserMessageRecord is the persisted shape of a user's message.
type UserMessageRecord struct {
	ID        string        `json:"id"`
	Content   string        `json:"content"`
	Role      Role          `json:"role"`
	Timestamp jsonutil.Time `json:"timestamp"`
}

// Deterministic step ids for the history lifecycle.
//
// Every write is memoized under one of these so an Inngest replay — which
// re-executes the function body from the top — cannot create a second thread
// or append the user's turn twice. Get is deliberately NOT memoized: its
// result is the whole conversation, and step outputs share a bounded
// per-run budget, so re-reading on replay is cheaper than carrying history
// through step state.
const (
	// FinalAppendStepID is the deterministic step id for the end-of-run save.
	FinalAppendStepID = "agent-kit/history/append-results/final"

	// CreateThreadStepID memoizes thread creation, so the thread id a run
	// commits to is the same one every replay sees.
	CreateThreadStepID = "agent-kit/history/create-thread"

	// AppendUserMessageStepID memoizes the user-turn write.
	AppendUserMessageStepID = "agent-kit/history/append-user-message"
)

// IncrementalAppendStepID builds the deterministic step id for an
// incremental (per-iteration) save. key MUST be replay-stable — an
// iteration counter, NEVER a checksum, timestamp or uuid, all of which
// change between Inngest re-executions and cause
// `Could not find step "<hash>" to run`.
func IncrementalAppendStepID(key any) string {
	return fmt.Sprintf("agent-kit/history/append-results/%v", key)
}

// threadOpConfig is the shared parameter set for the thread helpers.
type threadOpConfig[T any] struct {
	State   *State[T]
	History *HistoryConfig[T]
	Input   string
	Network *NetworkRun[T]
	Step    durable.Step
}

func (c threadOpConfig[T]) historyContext() HistoryContext[T] {
	return HistoryContext[T]{
		State:    c.State,
		Network:  c.Network,
		Step:     c.Step,
		Input:    c.Input,
		ThreadID: c.State.ThreadID,
	}
}

// initializeThread ensures a valid thread context: calls CreateThread when
// configured (upserting if the client provided a ThreadID), or
// auto-generates a ThreadID when only Get is configured.
//
// Divergence from TS (deliberate fix): the auto-generated ThreadID is
// minted inside a durable step — the TS version calls randomUUID inline,
// which re-mints a different id on every Inngest replay.
func initializeThread[T any](ctx context.Context, cfg threadOpConfig[T]) error {
	h := cfg.History
	if h == nil {
		return nil
	}

	// Client-provided ThreadID: ensure it exists in storage; the state's
	// ThreadID stays the source of truth.
	if cfg.State.ThreadID != "" {
		if h.CreateThread != nil {
			_, err := createThread(ctx, cfg)
			return err
		}
		return nil
	}

	switch {
	case h.CreateThread != nil:
		res, err := createThread(ctx, cfg)
		if err != nil {
			return err
		}
		cfg.State.ThreadID = res.ThreadID
	case h.Get != nil:
		id, err := durable.Run(ctx, cfg.Step, "agent-kit/history/generate-thread-id",
			func(ctx context.Context) (string, error) { return uuid.NewString(), nil })
		if err != nil {
			return err
		}
		cfg.State.ThreadID = id
	}
	return nil
}

// createThread runs the CreateThread hook inside ONE durable step under a
// deterministic id. The step is owned here, not by the adapter: an unmemoized
// creation would run again on every replay, and an adapter that inserts
// rather than upserts would leave a trail of orphan threads.
func createThread[T any](ctx context.Context, cfg threadOpConfig[T]) (CreateThreadResult, error) {
	step := cfg.Step
	if step == nil {
		step = durable.Inngest()
	}
	return durable.Run(ctx, step, CreateThreadStepID,
		func(ctx context.Context) (CreateThreadResult, error) {
			return cfg.History.CreateThread(ctx, cfg.historyContext())
		})
}

// appendUserMessage persists the user's turn inside ONE durable step, so a
// replay re-reads the memoized result instead of appending the message again.
// The record's id is already replay-stable; the step makes the write
// exactly-once even for an adapter that does not dedupe on it.
func appendUserMessage[T any](ctx context.Context, cfg threadOpConfig[T], msg UserMessageRecord) error {
	if cfg.History == nil || cfg.History.AppendUserMessage == nil {
		return nil
	}
	step := cfg.Step
	if step == nil {
		step = durable.Inngest()
	}
	_, err := durable.Run(ctx, step, AppendUserMessageStepID,
		func(ctx context.Context) (bool, error) {
			if err := cfg.History.AppendUserMessage(ctx, cfg.historyContext(), msg); err != nil {
				return false, err
			}
			return true, nil
		})
	return err
}

// loadThreadFromStorage hydrates the state from History.Get — unless the
// client already provided results or messages (client-authoritative mode,
// where the UI owns conversation state and Get is only a fallback for new
// threads or recovery).
func loadThreadFromStorage[T any](ctx context.Context, cfg threadOpConfig[T]) error {
	h := cfg.History
	if h == nil || h.Get == nil || cfg.State.ThreadID == "" ||
		len(cfg.State.results) > 0 || len(cfg.State.messages) > 0 {
		return nil
	}
	results, err := h.Get(ctx, cfg.historyContext())
	if err != nil {
		return err
	}
	cfg.State.SetResults(results)
	return nil
}

// saveThreadToStorage is the end-of-run backstop: persists only the
// results added after initialResultCount. When results were already
// persisted incrementally (see PersistResults), the consumer's
// idempotency — helped by the replay-stable AgentResult checksum — makes
// this a no-op.
func saveThreadToStorage[T any](ctx context.Context, cfg threadOpConfig[T], initialResultCount int) error {
	return PersistResults(ctx, PersistConfig[T]{
		State:   cfg.State,
		History: cfg.History,
		Input:   cfg.Input,
		Network: cfg.Network,
		Step:    cfg.Step,
	}, cfg.State.ResultsFrom(initialResultCount), FinalAppendStepID)
}

// PersistConfig parameterizes PersistResults.
type PersistConfig[T any] struct {
	State   *State[T]
	History *HistoryConfig[T]
	Input   string
	Network *NetworkRun[T]
	Step    durable.Step
}

// PersistResults persists newResults via History.AppendResults inside ONE
// durable step under the caller-provided deterministic stepID. The step is
// owned here, not by the consumer's hook: the persistence id stays
// replay-stable, and the write is memoized after it first succeeds, so
// re-executions never duplicate it. Safe to call repeatedly with the same
// stepID — the memoized step makes extra calls free.
func PersistResults[T any](ctx context.Context, cfg PersistConfig[T], newResults []*AgentResult, stepID string) error {
	if cfg.History == nil || cfg.History.AppendResults == nil || len(newResults) == 0 {
		return nil
	}
	step := cfg.Step
	if step == nil {
		step = durable.Inngest()
	}
	hctx := HistoryContext[T]{
		State:    cfg.State,
		Network:  cfg.Network,
		Step:     step,
		Input:    cfg.Input,
		ThreadID: cfg.State.ThreadID,
	}
	_, err := durable.Run(ctx, step, stepID, func(ctx context.Context) (bool, error) {
		if err := cfg.History.AppendResults(ctx, hctx, newResults); err != nil {
			return false, err
		}
		return true, nil
	})
	return err
}
