package main

import (
	"context"
	"sync"

	agentkit "github.com/eadwinCode/agent-kit/go"
)

// demoHistory is canonical conversation history. It deliberately stores user
// turns and assistant results in one ordered list so hydration never reconstructs
// a transcript from two independently ordered collections.
type demoHistory struct {
	mu      sync.Mutex
	threads map[string]bool
	entries map[string][]*agentkit.AgentResult
	seen    map[string]bool
}

func newDemoHistory() *demoHistory {
	return &demoHistory{
		threads: map[string]bool{},
		entries: map[string][]*agentkit.AgentResult{},
		seen:    map[string]bool{},
	}
}

func (h *demoHistory) config() *agentkit.HistoryConfig[demoState] {
	return &agentkit.HistoryConfig[demoState]{
		CreateThread: func(_ context.Context, hctx agentkit.HistoryContext[demoState]) (agentkit.CreateThreadResult, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.threads[hctx.ThreadID] = true
			return agentkit.CreateThreadResult{ThreadID: hctx.ThreadID}, nil
		},
		Get: func(_ context.Context, hctx agentkit.HistoryContext[demoState]) ([]*agentkit.AgentResult, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			return append([]*agentkit.AgentResult(nil), h.entries[hctx.ThreadID]...), nil
		},
		AppendUserMessage: func(_ context.Context, hctx agentkit.HistoryContext[demoState], msg agentkit.UserMessageRecord) error {
			h.mu.Lock()
			defer h.mu.Unlock()
			key := hctx.ThreadID + "\x00user\x00" + msg.ID
			if h.seen[key] {
				return nil
			}
			h.seen[key] = true
			entry := agentkit.NewAgentResult("user", []agentkit.Message{{
				Type: agentkit.MessageText, Role: agentkit.RoleUser,
				Content: agentkit.TextContent(msg.Content),
			}}, nil, msg.Timestamp)
			entry.ID = msg.ID
			h.entries[hctx.ThreadID] = append(h.entries[hctx.ThreadID], entry)
			return nil
		},
		AppendResults: func(_ context.Context, hctx agentkit.HistoryContext[demoState], results []*agentkit.AgentResult) error {
			h.mu.Lock()
			defer h.mu.Unlock()
			for _, result := range results {
				checksum, err := result.Checksum()
				if err != nil {
					checksum = result.ID
				}
				key := hctx.ThreadID + "\x00result\x00" + checksum
				if h.seen[key] {
					continue
				}
				h.seen[key] = true
				h.entries[hctx.ThreadID] = append(h.entries[hctx.ThreadID], result)
			}
			return nil
		},
	}
}

func (h *demoHistory) messages(threadID string) []agentkit.Message {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]agentkit.Message, 0)
	for _, entry := range h.entries[threadID] {
		out = append(out, entry.Output...)
		out = append(out, entry.ToolCalls...)
	}
	return out
}

func (h *demoHistory) count(threadID string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.entries[threadID])
}
