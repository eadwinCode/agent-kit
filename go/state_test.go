package agentkit

import (
	"testing"

	"github.com/eadwinCode/agent-kit/go/internal/jsonutil"
)

type shape struct {
	Category string `json:"category"`
	SKU      int    `json:"sku"`
}

func textMsg(role Role, s string) Message {
	return Message{Type: MessageText, Role: role, Content: TextContent(s), StopReason: StopStop}
}

func resultWith(name, text string) *AgentResult {
	return NewAgentResult(name, []Message{textMsg(RoleAssistant, text)}, nil, jsonutil.Now())
}

func TestStateTypedData(t *testing.T) {
	s := NewState(StateConfig[shape]{})
	s.Data.Category = "refund"
	s.Data.SKU = 123
	if s.Data.Category != "refund" || s.Data.SKU != 123 {
		t.Errorf("typed data mutation lost: %+v", s.Data)
	}
}

func TestStateResultsCopySafety(t *testing.T) {
	s := NewState(StateConfig[shape]{})
	s.AppendResult(resultWith("a", "one"))
	got := s.Results()
	got[0] = resultWith("evil", "mutated")
	if s.Results()[0].AgentName != "a" {
		t.Error("mutating the returned slice affected internal state")
	}
}

func TestStateResultsFromClamps(t *testing.T) {
	s := NewState(StateConfig[shape]{})
	s.AppendResult(resultWith("a", "one"))
	s.AppendResult(resultWith("b", "two"))
	if got := s.ResultsFrom(1); len(got) != 1 || got[0].AgentName != "b" {
		t.Errorf("ResultsFrom(1) = %v", got)
	}
	if got := s.ResultsFrom(99); len(got) != 0 {
		t.Errorf("ResultsFrom(99) should clamp to empty, got %d", len(got))
	}
	if got := s.ResultsFrom(-5); len(got) != 2 {
		t.Errorf("ResultsFrom(-5) should clamp to full, got %d", len(got))
	}
}

func TestStateFormatHistory(t *testing.T) {
	seed := textMsg(RoleUser, "earlier turn")
	s := NewState(StateConfig[shape]{Messages: []Message{seed}})
	r := NewAgentResult("a",
		[]Message{textMsg(RoleAssistant, "answer")},
		[]Message{{Type: MessageToolResult, Role: RoleToolResult, Tool: &ToolMessage{Type: "tool", ID: "t1", Name: "x", Input: []byte("{}")}, Content: RawContent([]byte(`{"data":1}`)), StopReason: StopTool}},
		jsonutil.Now())
	s.AppendResult(r)

	h := s.FormatHistory(nil)
	if len(h) != 3 {
		t.Fatalf("want 3 messages (seed + output + toolCall), got %d", len(h))
	}
	if c, _ := h[0].Content.AsString(); c != "earlier turn" {
		t.Error("seed messages must come first")
	}
	if h[2].Type != MessageToolResult {
		t.Error("tool results must follow output")
	}

	// Custom formatter.
	onlyOutput := s.FormatHistory(func(r *AgentResult) []Message { return r.Output })
	if len(onlyOutput) != 2 {
		t.Errorf("custom formatter: want 2, got %d", len(onlyOutput))
	}
}

func TestStateDurableToolCallIndex(t *testing.T) {
	s := NewState(StateConfig[shape]{})
	if s.nextDurableToolCallIndex() != 0 || s.nextDurableToolCallIndex() != 1 {
		t.Error("index must be monotonic from 0")
	}
	// Clone resets — each run's replayed tool calls must re-mint the same ids.
	c := s.Clone()
	if c.nextDurableToolCallIndex() != 0 {
		t.Error("Clone must reset the durable tool-call index")
	}
	// Original keeps counting.
	if s.nextDurableToolCallIndex() != 2 {
		t.Error("original index affected by clone")
	}
}

func TestStateCloneIndependence(t *testing.T) {
	s := NewState(StateConfig[shape]{Data: shape{Category: "refund", SKU: 1}, ThreadID: "th_1"})
	s.AppendResult(resultWith("a", "one"))

	c := s.Clone()
	c.Data.SKU = 999
	c.AppendResult(resultWith("b", "two"))

	if s.Data.SKU != 1 {
		t.Error("clone data mutation leaked into original")
	}
	if len(s.Results()) != 1 {
		t.Error("clone result append leaked into original")
	}
	if c.ThreadID != "th_1" || len(c.Results()) != 2 {
		t.Errorf("clone lost fields: %+v", c)
	}
}

func TestStateImportData(t *testing.T) {
	s := NewState(StateConfig[shape]{Data: shape{Category: "refund", SKU: 1}})
	s.ImportData(shape{SKU: 42}) // full replace: Category resets to zero value
	if s.Data.SKU != 42 || s.Data.Category != "" {
		t.Errorf("ImportData must fully replace: %+v", s.Data)
	}
}
