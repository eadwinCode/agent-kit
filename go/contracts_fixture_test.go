package agentkit_test

// A0 baseline: executable standard-event fixtures.
//
// The wire protocol is the contract between the Go runtime, the React
// package, and every application adapter. Prose drifts, so these fixtures
// freeze it: deterministic runs produce golden envelope sequences that both
// runtimes assert against. The TypeScript package reduces the SAME files
// (packages/use-agent/src/__tests__/contracts.test.ts), so a change to
// either side that breaks the other fails here first.
//
// Dynamic identity (run ids, message ids, part ids, timestamps) is
// normalized to placeholders. That keeps the fixtures stable while still
// proving the identity RELATIONSHIPS: the same part id must appear in
// created, its deltas, and completed.
//
// Regenerate after an intentional protocol change:
//
//	UPDATE_CONTRACT_FIXTURES=1 go test ./ -run TestStandardEventFixtures

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/zendev-sh/goai/provider"

	agentkit "github.com/eadwinCode/agent-kit/go"
	"github.com/eadwinCode/agent-kit/go/memadapter"
)

const fixtureDir = "../contracts/fixtures"

// fixtureFile is the on-disk shape both runtimes read.
type fixtureFile struct {
	Name          string                       `json:"name"`
	Description   string                       `json:"description"`
	SchemaVersion int                          `json:"schemaVersion"`
	Events        []agentkit.AgentMessageChunk `json:"events"`
}

// normalizer maps dynamic identifiers to stable placeholders, in first-seen
// order, so a golden file survives re-running the same scenario.
type normalizer struct {
	seen  map[string]string
	kinds map[string]int
}

func newNormalizer() *normalizer {
	return &normalizer{seen: map[string]string{}, kinds: map[string]int{}}
}

var (
	uuidRe     = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	runIDRe    = regexp.MustCompile(`^\d{10,}_[0-9a-z]{9}$`)
	partIDRe   = regexp.MustCompile(`^(part|tool|data)_[0-9a-z_]+$`)
	eventIDRe  = regexp.MustCompile(`^(.+):(\d+):(\d+)$`)
	isoTimeRe  = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T`)
	stepIDRe   = regexp.MustCompile(`^publish-(e\d+-)?\d+:`)
	sha16HexRe = regexp.MustCompile(`^part_[0-9a-f]{32}$`)
)

// placeholder returns a stable token for one dynamic value.
func (n *normalizer) placeholder(kind, value string) string {
	if token, ok := n.seen[value]; ok {
		return token
	}
	n.kinds[kind]++
	token := fmt.Sprintf("<%s:%d>", kind, n.kinds[kind])
	n.seen[value] = token
	return token
}

// normalizeString replaces one scalar when it looks like dynamic identity.
func (n *normalizer) normalizeString(key, value string) string {
	switch key {
	case "runId", "parentRunId":
		if runIDRe.MatchString(value) || uuidRe.MatchString(value) {
			return n.placeholder("run", value)
		}
	case "messageId":
		if uuidRe.MatchString(value) || runIDRe.MatchString(value) {
			return n.placeholder("msg", value)
		}
	case "partId":
		if partIDRe.MatchString(value) || sha16HexRe.MatchString(value) {
			return n.placeholder("part", value)
		}
	case "approvalId", "toolCallId":
		return value
	case "expiresAt", "timestamp":
		if isoTimeRe.MatchString(value) {
			return "<time>"
		}
	}
	return value
}

// wallClockKeys carry measured durations, which differ on every run. They
// are normalized to a token so the fixtures stay stable; the semantics are
// asserted separately in TestPausedStateFixture.
var wallClockKeys = map[string]bool{
	"accumulatedPausedMs": true,
	"pausedTotalMs":       true,
	"durationMs":          true,
}

func (n *normalizer) normalizeValue(key string, value any) any {
	if wallClockKeys[key] {
		return "<duration-ms>"
	}
	switch v := value.(type) {
	case string:
		return n.normalizeString(key, v)
	case map[string]any:
		// Sorted iteration: placeholder numbering must not depend on Go's
		// randomized map order, or the golden files would churn.
		out := make(map[string]any, len(v))
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			out[k] = n.normalizeValue(k, v[k])
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, inner := range v {
			out[i] = n.normalizeValue(key, inner)
		}
		return out
	default:
		return value
	}
}

// normalizeChunk produces the golden form of one envelope.
func (n *normalizer) normalizeChunk(chunk agentkit.AgentMessageChunk) agentkit.AgentMessageChunk {
	// Round-trip through JSON so json.RawMessage payloads normalize the same
	// way the wire sees them.
	raw, err := json.Marshal(chunk.Data)
	if err != nil {
		panic(err)
	}
	var data map[string]any
	if err := json.Unmarshal(raw, &data); err != nil {
		panic(err)
	}
	normalized := map[string]any{}
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		normalized[k] = n.normalizeValue(k, data[k])
	}

	out := chunk
	out.Data = normalized
	out.Timestamp = 0
	// Event ids embed the run id; normalize the run half and keep the
	// epoch/sequence half, which is what proves replay stability.
	if m := eventIDRe.FindStringSubmatch(chunk.EventID); m != nil {
		out.EventID = fmt.Sprintf("%s:%s:%s", n.placeholder("run", m[1]), m[2], m[3])
	}
	if stepIDRe.MatchString(chunk.ID) {
		out.ID = chunk.ID
	}
	return out
}

// scenario is one deterministic run whose envelopes are frozen.
type scenario struct {
	name        string
	description string
	run         func(t *testing.T, handles *memadapter.Ports, ports *agentkit.RuntimePorts)
}

func fixtureScenarios() []scenario {
	return []scenario{
		{
			name:        "text-turn",
			description: "Reasoning and text deltas for a single-agent turn, with the accepted user message and one terminal.",
			run: func(t *testing.T, _ *memadapter.Ports, ports *agentkit.RuntimePorts) {
				model := &scriptedModel{
					id: "scripted",
					scripts: [][]provider.StreamChunk{{
						{Type: provider.ChunkReasoning, Text: "The user greeted me."},
						{Type: provider.ChunkText, Text: "Hello"},
						{Type: provider.ChunkText, Text: " there."},
					}},
					results: []*provider.GenerateResult{{
						Reasoning: "The user greeted me.", Text: "Hello there.",
						FinishReason: provider.FinishStop,
						Usage:        provider.Usage{InputTokens: 9, OutputTokens: 4, TotalTokens: 13},
					}},
				}
				if _, err := runNetwork(t, model, ports, nil, nil); err != nil {
					t.Fatalf("run: %v", err)
				}
			},
		},
		{
			name:        "tool-turn",
			description: "Provider-streamed tool arguments, tool output, and the final text response.",
			run: func(t *testing.T, _ *memadapter.Ports, ports *agentkit.RuntimePorts) {
				model := &scriptedModel{
					id: "scripted",
					scripts: [][]provider.StreamChunk{
						{
							{Type: provider.ChunkToolCallStreamStart, ToolCallID: "call_1", ToolName: "note"},
							{Type: provider.ChunkToolCallDelta, ToolCallID: "call_1", ToolName: "note", ToolInput: `{"text":`},
							{Type: provider.ChunkToolCallDelta, ToolCallID: "call_1", ToolName: "note", ToolInput: `"ok"}`},
							{Type: provider.ChunkToolCall, ToolCallID: "call_1", ToolName: "note", ToolInput: `{"text":"ok"}`},
						},
						{{Type: provider.ChunkText, Text: "Noted."}},
					},
					results: []*provider.GenerateResult{
						{
							FinishReason: provider.FinishToolCalls,
							ToolCalls:    []provider.ToolCall{{ID: "call_1", Name: "note", Input: json.RawMessage(`{"text":"ok"}`)}},
						},
						{Text: "Noted.", FinishReason: provider.FinishStop},
					},
				}
				note := agentkit.NewTool[portState]("note", "Record a note.",
					func(_ context.Context, in struct {
						Text string `json:"text"`
					}, _ agentkit.ToolOptions[portState]) (any, error) {
						return map[string]any{"saved": in.Text}, nil
					})
				if _, err := runNetwork(t, model, ports, []agentkit.Tool[portState]{note}, nil); err != nil {
					t.Fatalf("run: %v", err)
				}
			},
		},
		{
			name:        "structured-turn",
			description: "Tool-emitted semantic status, a structured data part lifecycle, and progress.",
			run: func(t *testing.T, _ *memadapter.Ports, ports *agentkit.RuntimePorts) {
				model := &scriptedModel{
					id: "scripted",
					scripts: [][]provider.StreamChunk{
						{{Type: provider.ChunkToolCall, ToolCallID: "call_1", ToolName: "scan", ToolInput: `{}`}},
						{{Type: provider.ChunkText, Text: "Two files."}},
					},
					results: []*provider.GenerateResult{
						{
							FinishReason: provider.FinishToolCalls,
							ToolCalls:    []provider.ToolCall{{ID: "call_1", Name: "scan", Input: json.RawMessage(`{}`)}},
						},
						{Text: "Two files.", FinishReason: provider.FinishStop},
					},
				}
				scan := agentkit.NewTool[portState]("scan", "Scan the project.",
					func(ctx context.Context, _ struct{}, opts agentkit.ToolOptions[portState]) (any, error) {
						opts.Stream.Status(ctx, agentkit.StatusUpdate{
							Kind: agentkit.ActivityReading, Label: "Reading project files",
							Source: agentkit.ActivityFromTool,
						})
						partID := opts.Stream.Data(ctx, agentkit.DataPart{
							Type: "file-list", Payload: agentkit.JSONValue(`{"files":["a.ts"]}`),
						})
						opts.Stream.Progress(ctx, agentkit.ToolProgress{Completed: 1, Total: 2, Label: "a.ts"})
						opts.Stream.CompleteData(ctx, partID, agentkit.JSONValue(`{"files":["a.ts","b.ts"]}`))
						return map[string]any{"files": 2}, nil
					})
				if _, err := runNetwork(t, model, ports, []agentkit.Tool[portState]{scan}, nil); err != nil {
					t.Fatalf("run: %v", err)
				}
			},
		},
		{
			name:        "hitl-turn",
			description: "A tool that requires approval: hitl.requested, the decision, and the consumed capability.",
			run: func(t *testing.T, handles *memadapter.Ports, ports *agentkit.RuntimePorts) {
				handles.Approvals.AutoDecide = agentkit.ApprovalApproved
				model := &scriptedModel{
					id: "scripted",
					scripts: [][]provider.StreamChunk{
						{{Type: provider.ChunkToolCall, ToolCallID: "call_1", ToolName: "publish", ToolInput: `{}`}},
						{{Type: provider.ChunkText, Text: "Published."}},
					},
					results: []*provider.GenerateResult{
						{
							FinishReason: provider.FinishToolCalls,
							ToolCalls:    []provider.ToolCall{{ID: "call_1", Name: "publish", Input: json.RawMessage(`{}`)}},
						},
						{Text: "Published.", FinishReason: provider.FinishStop},
					},
				}
				publish := agentkit.NewTool[portState]("publish", "Publish the site.",
					func(ctx context.Context, _ struct{}, opts agentkit.ToolOptions[portState]) (any, error) {
						if _, err := opts.Approvals.Require(ctx, agentkit.ApprovalRequest{
							RequestID: "approval_1", ToolName: "publish",
							ToolCallID: opts.ToolCallID, Summary: "Publish the site",
						}); err != nil {
							return nil, err
						}
						return map[string]any{"published": true}, nil
					})
				if _, err := runNetwork(t, model, ports, []agentkit.Tool[portState]{publish}, nil); err != nil {
					t.Fatalf("run: %v", err)
				}
			},
		},
		{
			name:        "cancel-turn",
			description: "A cancel accepted before the first inference: one cancelled terminal, no partial output claimed as success.",
			run: func(t *testing.T, handles *memadapter.Ports, ports *agentkit.RuntimePorts) {
				if _, err := handles.Control.Record(context.Background(), agentkit.ControlCommand{
					Scope: testScope, ID: "cmd_cancel", Type: agentkit.CommandCancel,
				}); err != nil {
					t.Fatalf("Record cancel: %v", err)
				}
				if _, err := runNetwork(t, textModel("never"), ports, nil, nil); !agentkit.IsCancelled(err) {
					t.Fatalf("run error = %v; want cancelled", err)
				}
			},
		},
		{
			name:        "paused-turn",
			description: "A pause accepted before the run starts: it takes effect at the first safe boundary and a correlated resume continues the same run.",
			run: func(t *testing.T, handles *memadapter.Ports, ports *agentkit.RuntimePorts) {
				if _, err := handles.Control.Record(context.Background(), agentkit.ControlCommand{
					Scope: testScope, ID: "cmd_pause", Type: agentkit.CommandPause,
				}); err != nil {
					t.Fatalf("Record pause: %v", err)
				}
				done := make(chan struct{})
				go func() {
					defer close(done)
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
				if _, err := runNetwork(t, textModel("resumed"), ports, nil, nil); err != nil {
					t.Fatalf("run: %v", err)
				}
				<-done
			},
		},
		{
			name:        "error-turn",
			description: "A provider stream that fails mid-part: the part is failed, and one failed terminal follows.",
			run: func(t *testing.T, _ *memadapter.Ports, ports *agentkit.RuntimePorts) {
				model := &scriptedModel{
					id: "scripted",
					scripts: [][]provider.StreamChunk{{
						{Type: provider.ChunkText, Text: "Partial"},
						{Type: provider.ChunkError, Error: errors.New("provider stream failed")},
					}},
					results: []*provider.GenerateResult{{FinishReason: provider.FinishError}},
				}
				if _, err := runNetwork(t, model, ports, nil, nil); err == nil {
					t.Fatal("a failed provider stream must surface as a run error")
				}
			},
		},
	}
}

func TestStandardEventFixtures(t *testing.T) {
	update := os.Getenv("UPDATE_CONTRACT_FIXTURES") != ""

	for _, sc := range fixtureScenarios() {
		t.Run(sc.name, func(t *testing.T) {
			handles, ports := memadapter.NewPorts(testScope, 0)
			sc.run(t, handles, ports)

			n := newNormalizer()
			chunks := handles.Sink.Chunks()
			events := make([]agentkit.AgentMessageChunk, 0, len(chunks))
			for _, chunk := range chunks {
				events = append(events, n.normalizeChunk(chunk))
			}

			got := fixtureFile{
				Name:          sc.name,
				Description:   sc.description,
				SchemaVersion: agentkit.ContractSchemaVersion,
				Events:        events,
			}
			var buf bytes.Buffer
			enc := json.NewEncoder(&buf)
			enc.SetEscapeHTML(false)
			enc.SetIndent("", "  ")
			if err := enc.Encode(got); err != nil {
				t.Fatalf("marshal fixture: %v", err)
			}
			gotJSON := buf.Bytes()

			path := filepath.Join(fixtureDir, sc.name+".json")
			if update {
				if err := os.WriteFile(path, gotJSON, 0o644); err != nil {
					t.Fatalf("write fixture: %v", err)
				}
				return
			}

			wantJSON, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read fixture %s: %v\nrun UPDATE_CONTRACT_FIXTURES=1 go test ./ -run TestStandardEventFixtures", path, err)
			}
			if string(gotJSON) != string(wantJSON) {
				t.Fatalf("standard-event protocol changed for %q.\n"+
					"The React package reduces this same file, so an unintended change breaks it too.\n"+
					"If the change is intended, regenerate:\n"+
					"  UPDATE_CONTRACT_FIXTURES=1 go test ./ -run TestStandardEventFixtures\n\n%s",
					sc.name, firstDiff(string(wantJSON), string(gotJSON)))
			}
		})
	}
}

// firstDiff reports the first differing line, which is far more useful than
// two 300-line JSON blobs.
func firstDiff(want, got string) string {
	wantLines := strings.Split(want, "\n")
	gotLines := strings.Split(got, "\n")
	for i := 0; i < len(wantLines) || i < len(gotLines); i++ {
		w, g := "", ""
		if i < len(wantLines) {
			w = wantLines[i]
		}
		if i < len(gotLines) {
			g = gotLines[i]
		}
		if w != g {
			return fmt.Sprintf("first difference at line %d:\n  want: %s\n   got: %s", i+1, w, g)
		}
	}
	return "files differ only in trailing whitespace"
}

// TestFixturesCoverEveryEventName fails when a new standard event ships
// without a fixture. An event no client has ever seen in a golden file is an
// event whose shape nothing is holding still.
func TestFixturesCoverEveryEventName(t *testing.T) {
	declared := []string{
		agentkit.EventRunStarted,
		agentkit.EventRunCompleted,
		agentkit.EventRunFailed,
		agentkit.EventPartCreated,
		agentkit.EventPartCompleted,
		agentkit.EventPartFailed,
		agentkit.EventTextDelta,
		agentkit.EventToolArgsDelta,
		agentkit.EventToolOutDelta,
		agentkit.EventReasoningDelta,
		agentkit.EventDataDelta,
		agentkit.EventHITLRequested,
		agentkit.EventHITLResolved,
		agentkit.EventStreamEnded,
		agentkit.EventStateUpdated,
		agentkit.EventStatusUpdated,
		agentkit.EventUserMessage,
	}

	covered := map[string]bool{}
	entries, err := os.ReadDir(fixtureDir)
	if err != nil {
		t.Fatalf("read fixture dir: %v", err)
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(fixtureDir, entry.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", entry.Name(), err)
		}
		var file fixtureFile
		if err := json.Unmarshal(raw, &file); err != nil {
			t.Fatalf("parse %s: %v", entry.Name(), err)
		}
		for _, event := range file.Events {
			covered[event.Event] = true
		}
	}

	var missing []string
	for _, name := range declared {
		if !covered[name] {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("these standard events have no fixture: %v\n"+
			"Add a scenario in fixtureScenarios() so the wire shape is frozen for both runtimes.", missing)
	}
}

// TestPausedStateFixture freezes the pause/resume state envelopes
// separately: the scenario needs a concurrent decider, so it cannot share
// the table-driven generator's shape.
func TestPausedStateFixture(t *testing.T) {
	handles, ports := memadapter.NewPorts(testScope, 0)

	if _, err := handles.Control.Record(context.Background(), agentkit.ControlCommand{
		Scope: testScope, ID: "cmd_pause", Type: agentkit.CommandPause,
	}); err != nil {
		t.Fatalf("Record pause: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
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
	if _, err := runNetwork(t, textModel("resumed"), ports, nil, nil); err != nil {
		t.Fatalf("run: %v", err)
	}
	<-done

	var states []map[string]any
	for _, chunk := range handles.Sink.Chunks() {
		if chunk.Event != agentkit.EventStateUpdated {
			continue
		}
		states = append(states, chunk.Data)
	}
	if len(states) != 2 {
		t.Fatalf("got %d state updates, want paused then resumed: %+v", len(states), states)
	}
	if states[0]["pauseState"] != string(agentkit.PausePaused) {
		t.Fatalf("first state update = %v; want paused", states[0]["pauseState"])
	}
	if states[1]["pauseState"] != string(agentkit.PauseNone) {
		t.Fatalf("second state update = %v; want none after resume", states[1]["pauseState"])
	}
	if _, ok := states[1]["accumulatedPausedMs"]; !ok {
		t.Fatal("the resume state update must report accumulated paused time for the optional Active time display")
	}
}
