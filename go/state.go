package agentkit

// State stores a network run's state: the stack of AgentResult items each
// iteration appends, any seed messages, and strongly-typed data mutated by
// tool calls. Chat history for each agentic call is reconstructed from it.
//
// Unlike the TS version there is no Proxy around Data and no deprecated kv
// store — Data is a plain typed value (port decision 7). A State belongs to
// a single run's goroutine and is not synchronized (decision 11): share
// nothing, clone per run.
type State[T any] struct {
	// Data is the strongly-typed state mutated by tools and read by routers.
	Data T

	// ThreadID identifies the conversation thread, when history is enabled.
	ThreadID string

	// results is the stack of all agent results in the network loop.
	results []*AgentResult

	// messages is a linear seed history, always placed before any results
	// when formatting conversation history.
	messages []Message

	// durableToolCallIndex mints AgentKit's durable tool-step ids.
	//
	// It is intentionally NOT copied by Clone, so every network run (which
	// clones the template state once per Inngest execution) starts again at
	// 0. Memoized inferences replay the same tool calls in the same order,
	// so the Nth wrapped tool call always receives the same index across
	// replays — a deterministic, replay-stable step id with no checksum,
	// timestamp or uuid involved.
	durableToolCallIndex int
}

// StateConfig configures NewState. Results seeds conversation memory from
// prior runs; Messages are placed after the system and user prompt but
// before any results.
type StateConfig[T any] struct {
	Data     T
	Results  []*AgentResult
	Messages []Message
	ThreadID string
}

// NewState creates state for a network run. To create conversational
// memory, serialize each run's Results and seed them back here (or use a
// HistoryConfig to do it durably).
func NewState[T any](cfg StateConfig[T]) *State[T] {
	return &State[T]{
		Data:     cfg.Data,
		ThreadID: cfg.ThreadID,
		results:  append([]*AgentResult(nil), cfg.Results...),
		messages: append([]Message(nil), cfg.Messages...),
	}
}

// Results returns a copy of all past inference results. The slice is safe
// to modify; the pointed-to results are shared.
func (s *State[T]) Results() []*AgentResult {
	return append([]*AgentResult(nil), s.results...)
}

// SetResults replaces all results — used when loading initial history via
// HistoryConfig.Get.
func (s *State[T]) SetResults(results []*AgentResult) {
	s.results = results
}

// ResultsFrom returns the results from start onward — used when saving only
// a run's new results via HistoryConfig.AppendResults. Out-of-range starts
// clamp (mirroring JS Array.slice).
func (s *State[T]) ResultsFrom(start int) []*AgentResult {
	if start < 0 {
		start = 0
	}
	if start > len(s.results) {
		start = len(s.results)
	}
	return append([]*AgentResult(nil), s.results[start:]...)
}

// Messages returns a copy of the seed messages provided at construction.
func (s *State[T]) Messages() []Message {
	return append([]Message(nil), s.messages...)
}

// AppendResult appends a result to the state. Called by the network after
// each iteration.
func (s *State[T]) AppendResult(r *AgentResult) {
	s.results = append(s.results, r)
}

// FormatHistory renders the state as a conversation log for an agent call:
// seed messages first, then each result through formatter (nil = the
// default output+toolCalls concatenation).
func (s *State[T]) FormatHistory(formatter func(*AgentResult) []Message) []Message {
	if formatter == nil {
		formatter = defaultResultFormatter
	}
	out := append([]Message(nil), s.messages...)
	for _, r := range s.results {
		out = append(out, formatter(r)...)
	}
	return out
}

// nextDurableToolCallIndex returns the next replay-stable tool-call index
// for this run. Used only by the network to build durable tool-step ids;
// not part of the public data model.
func (s *State[T]) nextDurableToolCallIndex() int {
	i := s.durableToolCallIndex
	s.durableToolCallIndex++
	return i
}

// ImportData re-applies a typed-data snapshot captured INSIDE a durable
// tool step.
//
// A tool that mutates Data does so inside its memoized step; on replay that
// body is skipped, so the live mutation is absent. The network memoizes the
// post-handler snapshot and calls this OUTSIDE the step on every execution
// to restore it. Full replace — the TS version diff-mutates through its
// Proxy to keep references alive, which Go has no need for.
func (s *State[T]) ImportData(data T) {
	s.Data = data
}

// Clone copies the state for a new run: Data by value (reference types
// inside T are shared, same one-level-deep semantics as the TS spread),
// results and messages as fresh slices. The durable tool-call index
// deliberately resets — see its doc.
func (s *State[T]) Clone() *State[T] {
	return &State[T]{
		Data:     s.Data,
		ThreadID: s.ThreadID,
		results:  append([]*AgentResult(nil), s.results...),
		messages: append([]Message(nil), s.messages...),
	}
}

func defaultResultFormatter(r *AgentResult) []Message {
	out := make([]Message, 0, len(r.Output)+len(r.ToolCalls))
	out = append(out, r.Output...)
	out = append(out, r.ToolCalls...)
	return out
}
