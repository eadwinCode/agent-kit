package agentkit

// Streaming: the AgentKit event protocol consumed by use-agent and other
// clients. Wire shape is byte-compatible with the TS package: the
// AgentMessageChunk envelope {event, data, timestamp, sequenceNumber, id}
// with millisecond epoch timestamps and monotonic sequence numbers shared
// across a network run and its agents.
//
// Durability: PublishEvent hands the chunk to the consumer's Publish
// function directly — mirroring TS, where durability is the publisher's
// choice ("@inngest/realtime"'s in-function publish is itself a step).
// Chunk.ID carries the suggested step id (`publish-<seq>:<event>`);
// DurablePublish wraps any publisher in a durable step under that id so
// replays never re-emit chunks. Publish failures are logged and swallowed:
// streaming is best-effort and must never break execution.

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/eadwinCode/agent-kit/go/durable"
	"github.com/eadwinCode/agent-kit/go/internal/jsonutil"
)

// AgentMessageChunk is the streaming envelope. Field order mirrors the TS
// interface for byte parity.
type AgentMessageChunk struct {
	// Event is the event name (e.g. "run.started", "part.created").
	Event string `json:"event"`
	// Data is the event-specific payload.
	Data map[string]any `json:"data"`
	// Timestamp is when the event occurred (Unix epoch milliseconds).
	Timestamp int64 `json:"timestamp"`
	// SequenceNumber orders events; monotonic per run (shared between a
	// network and its agents).
	SequenceNumber int `json:"sequenceNumber"`
	// ID is the suggested Inngest step id for durable publishing.
	ID string `json:"id"`
}

// PublishFn delivers one chunk to the client transport.
type PublishFn func(ctx context.Context, chunk AgentMessageChunk) error

// Streaming event names. Data shapes follow the TS event interfaces; the
// emission sites in agent.go/network.go use the exact same keys.
const (
	EventRunStarted     = "run.started"
	EventRunCompleted   = "run.completed"
	EventRunFailed      = "run.failed"
	EventRunInterrupted = "run.interrupted"

	EventStepStarted   = "step.started"
	EventStepCompleted = "step.completed"
	EventStepFailed    = "step.failed"

	EventPartCreated   = "part.created"
	EventPartCompleted = "part.completed"
	EventPartFailed    = "part.failed"

	EventTextDelta      = "text.delta"
	EventToolArgsDelta  = "tool_call.arguments.delta"
	EventToolOutDelta   = "tool_call.output.delta"
	EventReasoningDelta = "reasoning.delta"
	EventDataDelta      = "data.delta"

	EventHITLRequested = "hitl.requested"
	EventHITLResolved  = "hitl.resolved"

	EventUsageUpdated    = "usage.updated"
	EventMetadataUpdated = "metadata.updated"
	EventStreamEnded     = "stream.ended"
	EventError           = "error"
)

// DefaultChunkSize is the simulated-chunking delta size (characters).
const DefaultChunkSize = 256

// DefaultMaxChunksPerMessage caps simulated deltas per part; the chunk
// size grows for very long content so publish volume (and, with a durable
// publisher, step usage) stays bounded regardless of output length.
const DefaultMaxChunksPerMessage = 24

// StreamingConfig enables streaming on a run.
type StreamingConfig struct {
	// Publish delivers chunks to the client. Wrap with DurablePublish to
	// make delivery exactly-once across Inngest replays.
	Publish PublishFn
	// SimulateChunking splits part content into multiple deltas; off emits
	// one delta per part.
	SimulateChunking bool
	// ChunkSize is the simulated delta size (0 = DefaultChunkSize). Larger
	// → fewer Publish calls, which matters when each publish is a step.
	ChunkSize int
	// MaxChunksPerMessage caps deltas per part (0 = default; negative
	// disables the cap).
	MaxChunksPerMessage int
	// StreamReasoning forwards the model's thinking to the client as
	// reasoning parts. Default false: thinking still runs on the model,
	// its chain-of-thought is just not streamed.
	StreamReasoning bool
}

// DurablePublish wraps a publisher so each chunk is delivered inside a
// durable step under the chunk's suggested id — exactly-once across
// replays. Outside Inngest it degrades to a direct call.
func DurablePublish(step durable.Step, publish PublishFn) PublishFn {
	return func(ctx context.Context, chunk AgentMessageChunk) error {
		_, err := durable.Run(ctx, step, chunk.ID, func(ctx context.Context) (bool, error) {
			if err := publish(ctx, chunk); err != nil {
				return false, err
			}
			return true, nil
		})
		return err
	}
}

// SequenceCounter issues monotonic sequence numbers. Atomic: a network and
// its agents share one counter, and Go serves runs concurrently.
type SequenceCounter struct {
	n atomic.Int64
}

// Next returns the next sequence number (starting at 0).
func (c *SequenceCounter) Next() int {
	return int(c.n.Add(1) - 1)
}

// StreamingContext manages event publishing for one run scope.
type StreamingContext struct {
	publish             PublishFn
	seq                 *SequenceCounter
	simulateChunking    bool
	chunkSize           int
	maxChunksPerMessage int

	// StreamReasoning mirrors StreamingConfig.StreamReasoning.
	StreamReasoning bool

	RunID       string
	ParentRunID string
	MessageID   string
	ThreadID    string
	UserID      string
	// Scope is "network" or "agent".
	Scope string
}

// newStreamingContext builds a context from config; threadID/userID are
// stamped onto every event's data.
func newStreamingContext(cfg StreamingConfig, runID, messageID, scope, threadID, userID string) *StreamingContext {
	chunkSize := cfg.ChunkSize
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	maxChunks := cfg.MaxChunksPerMessage
	if maxChunks == 0 {
		maxChunks = DefaultMaxChunksPerMessage
	}
	return &StreamingContext{
		publish:             cfg.Publish,
		seq:                 &SequenceCounter{},
		simulateChunking:    cfg.SimulateChunking,
		chunkSize:           chunkSize,
		maxChunksPerMessage: maxChunks,
		StreamReasoning:     cfg.StreamReasoning,
		RunID:               runID,
		MessageID:           messageID,
		Scope:               scope,
		ThreadID:            threadID,
		UserID:              userID,
	}
}

// streamingContextFromState extracts threadId (and a userId, when the
// typed state carries one under a `userId` JSON key) from the run state.
func streamingContextFromState[T any](s *State[T], cfg StreamingConfig, runID, messageID, scope string) *StreamingContext {
	userID := ""
	if b, err := jsonutil.Marshal(s.Data); err == nil {
		var probe struct {
			UserID string `json:"userId"`
		}
		_ = json.Unmarshal(b, &probe)
		userID = probe.UserID
	}
	return newStreamingContext(cfg, runID, messageID, scope, s.ThreadID, userID)
}

// WithSharedSequence derives a context for a child scope (an agent inside
// a network run): new run/message ids, same sequence counter, parent run
// recorded.
func (c *StreamingContext) WithSharedSequence(runID, messageID, scope string) *StreamingContext {
	child := *c
	child.RunID = runID
	child.MessageID = messageID
	child.Scope = scope
	child.ParentRunID = c.RunID
	return &child
}

// PublishEvent stamps sequence number, timestamp and the suggested step id
// onto the event and hands it to the publisher. ThreadID/UserID are
// auto-attached to data. Failures are logged and swallowed — streaming is
// best-effort by contract.
func (c *StreamingContext) PublishEvent(ctx context.Context, event string, data map[string]any) {
	if c == nil || c.publish == nil {
		return
	}
	seq := c.seq.Next()

	enriched := make(map[string]any, len(data)+2)
	for k, v := range data {
		enriched[k] = v
	}
	if c.ThreadID != "" {
		enriched["threadId"] = c.ThreadID
	}
	if c.UserID != "" {
		enriched["userId"] = c.UserID
	}

	chunk := AgentMessageChunk{
		Event:          event,
		Data:           enriched,
		Timestamp:      time.Now().UnixMilli(),
		SequenceNumber: seq,
		ID:             fmt.Sprintf("publish-%d:%s", seq, event),
	}
	if err := c.publish(ctx, chunk); err != nil {
		slog.WarnContext(ctx, "agentkit: failed to publish streaming event; continuing",
			"event", chunk.Event, "sequenceNumber", chunk.SequenceNumber, "error", err)
	}
}

// GeneratePartID mints a part id ≤ 40 chars (OpenAI tool-call id limit):
// tool_<msgid8>_<ts8>_<rand6>. Call it inside a durable step — the call
// sites in agent.go do — so the id is replay-stable.
func (c *StreamingContext) GeneratePartID() string {
	shortMsg := strings.ReplaceAll(c.MessageID, "-", "")
	if len(shortMsg) > 8 {
		shortMsg = shortMsg[:8]
	}
	ts := fmt.Sprintf("%d", time.Now().UnixMilli())
	if len(ts) > 8 {
		ts = ts[len(ts)-8:]
	}
	return fmt.Sprintf("tool_%s_%s_%s", shortMsg, ts, randBase36(6))
}

// ChunkContent splits content into the `*.delta` strings for one part:
// chunking off → one delta with the whole content; on → fixed ChunkSize
// chunks, never more than MaxChunksPerMessage (the size grows for long
// content). Empty content yields no deltas. Each element is one
// PublishEvent call, so this governs streaming's publish/step budget.
func (c *StreamingContext) ChunkContent(content string) []string {
	if content == "" {
		return nil
	}
	if !c.simulateChunking {
		return []string{content}
	}
	size := c.chunkSize
	if size < 1 {
		size = 1
	}
	if c.maxChunksPerMessage > 0 {
		if grown := (len(content) + c.maxChunksPerMessage - 1) / c.maxChunksPerMessage; grown > size {
			size = grown
		}
	}
	var chunks []string
	for i := 0; i < len(content); i += size {
		end := i + size
		if end > len(content) {
			end = len(content)
		}
		chunks = append(chunks, content[i:end])
	}
	return chunks
}

func randBase36(n int) string {
	const alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[rand.Intn(len(alphabet))]
	}
	return string(b)
}
