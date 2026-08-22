package agentkit_test

// A0 baseline: the frozen schemas in contracts/schemas are validated against
// what the runtime actually produces. A schema nothing checks is prose.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	agentkit "github.com/eadwinCode/agent-kit/go"
	"github.com/eadwinCode/agent-kit/go/internal/jsonschema"
	"github.com/eadwinCode/agent-kit/go/memadapter"
)

const schemaDir = "../contracts/schemas"

func loadSchema(t *testing.T, name string) *jsonschema.Schema {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(schemaDir, name))
	if err != nil {
		t.Fatalf("read schema %s: %v", name, err)
	}
	schema, err := jsonschema.Parse(raw)
	if err != nil {
		t.Fatalf("parse schema %s: %v", name, err)
	}
	return schema
}

func TestBaseSchemasDoNotDefineOrRequireApplicationTenancy(t *testing.T) {
	for _, name := range []string{"agent-state-snapshot.schema.json", "command-request.schema.json"} {
		raw, err := os.ReadFile(filepath.Join(schemaDir, name))
		if err != nil {
			t.Fatalf("read schema %s: %v", name, err)
		}
		var doc struct {
			Required   []string                   `json:"required"`
			Properties map[string]json.RawMessage `json:"properties"`
		}
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatalf("parse schema %s: %v", name, err)
		}
		for _, field := range []string{"projectId", "project_id", "teamId", "team_id", "tenantId", "workspaceId", "organizationId"} {
			if _, ok := doc.Properties[field]; ok {
				t.Errorf("%s defines application field %q", name, field)
			}
			for _, required := range doc.Required {
				if required == field {
					t.Errorf("%s requires application field %q", name, field)
				}
			}
		}
	}
}

func TestFixturesMatchTheStandardEventSchema(t *testing.T) {
	schema := loadSchema(t, "standard-event.schema.json")

	entries, err := os.ReadDir(fixtureDir)
	if err != nil {
		t.Fatalf("read fixtures: %v", err)
	}
	checked := 0
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(fixtureDir, entry.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", entry.Name(), err)
		}
		var file struct {
			Events []json.RawMessage `json:"events"`
		}
		if err := json.Unmarshal(raw, &file); err != nil {
			t.Fatalf("parse %s: %v", entry.Name(), err)
		}
		for i, event := range file.Events {
			if errs := schema.Validate(event); len(errs) > 0 {
				t.Errorf("%s event %d violates the frozen envelope schema:\n  %s\n  event: %s",
					entry.Name(), i, strings.Join(errs, "\n  "), event)
			}
			checked++
		}
	}
	if checked == 0 {
		t.Fatal("no fixture events were checked; the schema is not actually holding anything still")
	}
}

func TestLiveEnvelopesMatchTheStandardEventSchema(t *testing.T) {
	// The fixtures are normalized; this checks the raw, unnormalized
	// envelopes a real run puts on the wire.
	schema := loadSchema(t, "standard-event.schema.json")
	handles, ports := memadapter.NewPorts(testScope, 4)

	if _, err := runNetwork(t, textModel("hello"), ports, nil, nil); err != nil {
		t.Fatalf("run: %v", err)
	}
	chunks := handles.Sink.Chunks()
	if len(chunks) == 0 {
		t.Fatal("no envelopes produced")
	}
	for _, chunk := range chunks {
		raw, err := json.Marshal(chunk)
		if err != nil {
			t.Fatalf("marshal chunk: %v", err)
		}
		if errs := schema.Validate(raw); len(errs) > 0 {
			t.Errorf("live envelope %q violates the frozen schema:\n  %s\n  %s",
				chunk.Event, strings.Join(errs, "\n  "), raw)
		}
	}
}

func TestJournalRecordsMatchTheFrozenSchema(t *testing.T) {
	schema := loadSchema(t, "journal-record.schema.json")
	handles, ports := memadapter.NewPorts(testScope, 1)

	if _, err := runNetwork(t, textModel("hello"), ports, nil, nil); err != nil {
		t.Fatalf("run: %v", err)
	}
	records, _, err := agentkit.ReadJournalTail(context.Background(), handles.Journal, agentkit.JournalQuery{
		Scope: testScope, ThreadID: "thread_1",
		After: agentkit.JournalCursor{StreamEpoch: 1, SequenceNumber: agentkit.JournalStart},
	})
	if err != nil {
		t.Fatalf("ReadJournalTail: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("journal is empty")
	}
	for _, rec := range records {
		raw, err := json.Marshal(rec)
		if err != nil {
			t.Fatalf("marshal record: %v", err)
		}
		if errs := schema.Validate(raw); len(errs) > 0 {
			t.Errorf("journal record %q violates the frozen schema:\n  %s\n  %s",
				rec.Event, strings.Join(errs, "\n  "), raw)
		}
	}
}

// snapshotFromSessionState renders a SessionState in the wire shape the
// snapshot schema freezes. The Go type is the server's record; this is the
// projection clients receive.
func snapshotFromSessionState(state agentkit.SessionState) map[string]any {
	snapshot := map[string]any{
		"schemaVersion": state.SchemaVersion,
		// The session's own identity, opaque to clients as it is to AgentKit.
		// Application extensions, if any, are projected by the adapter.
		"sessionId":         string(state.Scope),
		"activeRun":         nil,
		"pause":             map[string]any{"state": string(state.Pause.State), "accumulatedPausedMs": state.Pause.AccumulatedPausedMs, "epoch": state.Pause.Epoch},
		"activity":          map[string]any{"kind": string(state.Activity.Kind)},
		"approval":          map[string]any{"status": string(state.Approval.Status)},
		"revision":          state.Revision,
		"cursor":            nil,
		"reconcileRequired": state.ReconcileRequired,
	}
	if state.CurrentThreadID != "" {
		snapshot["currentThreadId"] = state.CurrentThreadID
	}
	if state.Activity.Label != "" {
		snapshot["activity"].(map[string]any)["label"] = state.Activity.Label
	}
	if state.Activity.Source != "" {
		snapshot["activity"].(map[string]any)["source"] = string(state.Activity.Source)
	}
	if state.Approval.ApprovalID != "" {
		snapshot["approval"].(map[string]any)["approvalId"] = state.Approval.ApprovalID
	}
	if run := state.ActiveRun; run != nil && run.Outcome == agentkit.OutcomeNone {
		snapshot["activeRun"] = map[string]any{
			"runId":      run.RunID,
			"lifecycle":  string(run.Lifecycle),
			"acceptedAt": run.AcceptedAt.UTC().Format(time.RFC3339Nano),
		}
		cursor := state.Cursor()
		snapshot["cursor"] = map[string]any{
			"runId": cursor.RunID, "streamEpoch": cursor.StreamEpoch, "sequenceNumber": cursor.SequenceNumber,
		}
	}
	return snapshot
}

func TestSnapshotProjectionMatchesTheFrozenSchema(t *testing.T) {
	schema := loadSchema(t, "agent-state-snapshot.schema.json")
	handles, ports := memadapter.NewPorts(testScope, 0)

	// Mid-run: an executing run with a live cursor.
	captured := make(chan agentkit.SessionState, 1)
	handles.Finalizer.OnFinalize = func(agentkit.FinalizeRequest) {
		state, err := handles.State.Load(context.Background(), testScope)
		if err == nil {
			select {
			case captured <- state:
			default:
			}
		}
	}

	if _, err := runNetwork(t, textModel("hello"), ports, nil, nil); err != nil {
		t.Fatalf("run: %v", err)
	}

	states := []agentkit.SessionState{}
	select {
	case midRun := <-captured:
		states = append(states, midRun)
	default:
	}
	terminal, err := handles.State.Load(context.Background(), testScope)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	states = append(states, terminal)

	for i, state := range states {
		raw, err := json.Marshal(snapshotFromSessionState(state))
		if err != nil {
			t.Fatalf("marshal snapshot: %v", err)
		}
		if errs := schema.Validate(raw); len(errs) > 0 {
			t.Errorf("snapshot %d violates the frozen schema:\n  %s\n  %s",
				i, strings.Join(errs, "\n  "), raw)
		}
	}
}

func TestCommandAndErrorSchemasAcceptTheContractShapes(t *testing.T) {
	commandSchema := loadSchema(t, "command-request.schema.json")
	errorSchema := loadSchema(t, "error-envelope.schema.json")

	commands := []string{
		`{"commandId":"01J0","type":"send","threadId":"t1","payload":{"content":"hi"}}`,
		`{"commandId":"01J1","type":"pause","threadId":"t1","runId":"r1","expectedRevision":42}`,
		`{"commandId":"01J2","type":"resume","threadId":"t1","runId":"r1","pauseEpoch":2}`,
		`{"commandId":"01J3","type":"approve","threadId":"t1","runId":"r1","approvalId":"a1"}`,
		`{"commandId":"01J4","type":"new_chat"}`,
	}
	for _, cmd := range commands {
		if errs := commandSchema.Validate([]byte(cmd)); len(errs) > 0 {
			t.Errorf("command rejected by its own schema:\n  %s\n  %s", strings.Join(errs, "\n  "), cmd)
		}
	}

	// This is a base schema, not an application authorization policy. An
	// adapter may add fields without forcing every AgentKit consumer to carry
	// them. Servers still derive trusted identity from authenticated context;
	// accepting an extension never makes client input authoritative.
	if errs := commandSchema.Validate([]byte(`{"commandId":"01J5","type":"pause","adapterContext":{"owner":"opaque"}}`)); len(errs) > 0 {
		t.Errorf("the command base schema rejected an adapter extension:\n  %s", strings.Join(errs, "\n  "))
	}

	errorEnvelopes := []string{
		`{"error":{"code":"STATE_REVISION_MISMATCH","message":"The assistant state changed; it has been refreshed.","recoverable":true,"correlationId":"req-1","retryAfterMs":0,"details":{}},"snapshot":{}}`,
		`{"error":{"code":"IDEMPOTENCY_KEY_REUSED","message":"That request was already used with different content.","recoverable":false}}`,
		`{"error":{"code":"PROJECT_WRITER_BUSY","message":"Another change is being applied to this project.","recoverable":true,"retryAfterMs":2000}}`,
	}
	for _, envelope := range errorEnvelopes {
		if errs := errorSchema.Validate([]byte(envelope)); len(errs) > 0 {
			t.Errorf("error envelope rejected by its own schema:\n  %s\n  %s", strings.Join(errs, "\n  "), envelope)
		}
	}
	if errs := errorSchema.Validate([]byte(`{"error":{"code":"X","message":"m","recoverable":true,"prompt":"secret"}}`)); len(errs) == 0 {
		t.Error("the error schema accepted an extra field; error payloads are bounded so prompts cannot leak through them")
	}
}

// TestPortErrorCodesAreBounded proves the Go typed errors carry the same
// bounded codes the wire schema documents, so an adapter can map one to the
// other without inventing strings.
func TestPortErrorCodesAreBounded(t *testing.T) {
	err := agentkit.NewPortError("StateStore", "CompareAndSwap", "STATE_REVISION_MISMATCH", true,
		agentkit.ErrRevisionMismatch)

	if code := agentkit.ErrorCode(err); code != "STATE_REVISION_MISMATCH" {
		t.Fatalf("ErrorCode = %q", code)
	}
	if !agentkit.IsRecoverable(err) {
		t.Fatal("a revision mismatch is recoverable: the client reconciles and retries")
	}
	if got := agentkit.ErrorCode(context.Canceled); got != "" {
		t.Fatalf("ErrorCode of a plain error = %q, want empty", got)
	}
	// The message must stay free of user content — it is logged.
	if strings.Contains(err.Error(), "prompt") {
		t.Fatal("port error text must never carry user content")
	}
}
