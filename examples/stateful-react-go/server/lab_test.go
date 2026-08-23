package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	agentkit "github.com/eadwinCode/agent-kit/go"
)

func loadState(t *testing.T, session *demoSession) agentkit.SessionState {
	t.Helper()
	state, err := session.handles.State.Load(context.Background(), session.id)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func newTestDemoSession(id string) *demoSession {
	return newDemoSessionWithModel(id, testModelFor, false)
}

func newTestAPIServer() *apiServer { return newAPIServerWithModel(testModelFor) }

func waitState(t *testing.T, session *demoSession, timeout time.Duration, match func(agentkit.SessionState) bool) agentkit.SessionState {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		state := loadState(t, session)
		if match(state) {
			return state
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for state; last=%+v", state)
		}
		time.Sleep(15 * time.Millisecond)
	}
}

func sendCommand(t *testing.T, session *demoSession, id, scenario string) commandResponse {
	t.Helper()
	state := loadState(t, session)
	response, err := session.executeCommand(context.Background(), commandRequest{
		CommandID: id, Type: string(agentkit.CommandSend), ExpectedRevision: state.Revision,
		Payload: map[string]any{"message": "exercise " + scenario, "scenario": scenario},
	})
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func TestSlowRunPausesAndResumesAtSafeBoundary(t *testing.T) {
	session := newTestDemoSession("pause-session")
	started := sendCommand(t, session, "send-slow", "slow")
	if started.Snapshot.ActiveRun == nil {
		t.Fatal("send did not return an active run")
	}
	// Activity/status observations legitimately advance the state revision
	// while the same run identity remains active. A human pause click must not
	// lose that race to an observation-only update.
	waitState(t, session, 4*time.Second, func(state agentkit.SessionState) bool {
		return state.Revision > started.Snapshot.Revision && state.Activity.Kind != agentkit.ActivityNone
	})

	_, err := session.executeCommand(context.Background(), commandRequest{
		CommandID: "pause-slow", Type: string(agentkit.CommandPause),
		ThreadID: started.Snapshot.CurrentThreadID, RunID: started.Snapshot.ActiveRun.RunID,
		ExpectedRevision: started.Snapshot.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	paused := waitState(t, session, 4*time.Second, func(state agentkit.SessionState) bool { return state.Pause.State == agentkit.PausePaused })
	if paused.CheckpointKind == "" {
		t.Fatal("paused state omitted the safe checkpoint kind")
	}

	_, err = session.executeCommand(context.Background(), commandRequest{
		CommandID: "resume-slow", Type: string(agentkit.CommandResume),
		ThreadID: paused.CurrentThreadID, RunID: paused.ActiveRun.RunID,
		PauseEpoch: paused.Pause.Epoch, ExpectedRevision: paused.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	completed := waitState(t, session, 7*time.Second, func(state agentkit.SessionState) bool {
		return state.ActiveRun != nil && state.ActiveRun.Outcome == agentkit.OutcomeCompleted
	})
	if completed.Pause.State != agentkit.PauseNone {
		t.Fatalf("pause state not cleared: %s", completed.Pause.State)
	}
	if calls := session.handles.Finalizer.Calls(); calls != 1 {
		t.Fatalf("finalizer calls=%d, want 1", calls)
	}
}

func TestOpenAIModelConfiguration(t *testing.T) {
	model, options := modelFor("text")
	if model.ModelID() != openAIModelID {
		t.Fatalf("model id=%q, want %q", model.ModelID(), openAIModelID)
	}
	if len(options) != 1 {
		t.Fatalf("model options=%d, want 1 AgentKit option bundle", len(options))
	}
	providerOptions := openAIProviderOptions()
	if providerOptions["reasoning_effort"] != "high" {
		t.Fatalf("reasoning effort=%v, want high", providerOptions["reasoning_effort"])
	}
	if providerOptions["reasoning_summary"] != "auto" || providerOptions["store"] != false {
		t.Fatalf("unexpected OpenAI options: %+v", providerOptions)
	}
}

func TestOpenAIScenarioRequiresServerSideKey(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "")
	session := newDemoSession("openai-unconfigured")
	state := loadState(t, session)
	_, err := session.executeCommand(context.Background(), commandRequest{
		CommandID: "send-openai", Type: string(agentkit.CommandSend),
		ExpectedRevision: state.Revision,
		Payload:          map[string]any{"message": "hello", "scenario": "text"},
	})
	var failure *commandError
	if !errors.As(err, &failure) || failure.status != httpStatusServiceUnavailable || failure.code != "OPENAI_NOT_CONFIGURED" {
		t.Fatalf("missing-key error=%v", err)
	}
	if session.running {
		t.Fatal("missing key started a run")
	}
}

func TestApprovalCompletesAndIsConsumed(t *testing.T) {
	session := newTestDemoSession("approval-session")
	sendCommand(t, session, "send-approval", "approval")
	pending := waitState(t, session, 4*time.Second, func(state agentkit.SessionState) bool { return state.Approval.Status == agentkit.ApprovalPending })
	approvalID := pending.Approval.ApprovalID
	if approvalID == "" {
		t.Fatal("pending approval has no approvalId")
	}

	_, err := session.executeCommand(context.Background(), commandRequest{
		CommandID: "approve-release", Type: string(agentkit.CommandApprove),
		ThreadID: pending.CurrentThreadID, RunID: pending.ActiveRun.RunID,
		ApprovalID: approvalID, ExpectedRevision: pending.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitState(t, session, 5*time.Second, func(state agentkit.SessionState) bool {
		return state.ActiveRun != nil && state.ActiveRun.Outcome == agentkit.OutcomeCompleted
	})
	record, ok := session.handles.Approvals.RecordFor(session.id, approvalID)
	if !ok || !record.Consumed {
		t.Fatalf("approval was not consumed: %+v", record)
	}
	if calls := session.handles.Finalizer.Calls(); calls != 1 {
		t.Fatalf("finalizer calls=%d, want 1", calls)
	}
}

func TestCommandsAreIdempotentAndRejectStaleRevision(t *testing.T) {
	session := newTestDemoSession("command-session")
	state := loadState(t, session)
	command := commandRequest{CommandID: "new-chat-once", Type: string(agentkit.CommandNewChat), ExpectedRevision: state.Revision}
	first, err := session.executeCommand(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	second, err := session.executeCommand(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if first.Duplicate || !second.Duplicate {
		t.Fatalf("duplicate flags first=%v second=%v", first.Duplicate, second.Duplicate)
	}
	if first.Snapshot.CurrentThreadID != second.Snapshot.CurrentThreadID {
		t.Fatal("idempotent replay changed the thread")
	}

	changed := command
	changed.Type = string(agentkit.CommandSend)
	_, err = session.executeCommand(context.Background(), changed)
	var reuse *commandError
	if !errors.As(err, &reuse) || reuse.code != "IDEMPOTENCY_KEY_REUSED" {
		t.Fatalf("reuse error=%v", err)
	}

	_, err = session.executeCommand(context.Background(), commandRequest{
		CommandID: "stale-new-chat", Type: string(agentkit.CommandNewChat), ExpectedRevision: state.Revision,
	})
	var stale *commandError
	if !errors.As(err, &stale) || stale.code != "STATE_REVISION_MISMATCH" || stale.snapshot == nil {
		t.Fatalf("stale error=%v", err)
	}
}

func TestJournalReplayHasStableOrderedEnvelopes(t *testing.T) {
	session := newTestDemoSession("replay-session")
	started := sendCommand(t, session, "send-text", "text")
	terminal := waitState(t, session, 5*time.Second, func(state agentkit.SessionState) bool {
		return state.ActiveRun != nil && state.ActiveRun.Outcome == agentkit.OutcomeCompleted
	})
	page, err := session.handles.Journal.Read(context.Background(), agentkit.JournalQuery{
		Scope: session.id, ThreadID: terminal.CurrentThreadID, Limit: 500,
		After: agentkit.JournalCursor{RunID: started.Snapshot.ActiveRun.RunID, StreamEpoch: started.Snapshot.Cursor.StreamEpoch, SequenceNumber: agentkit.JournalStart},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Records) < 3 {
		t.Fatalf("journal records=%d, want a real run tail", len(page.Records))
	}
	seen := map[string]bool{}
	last := agentkit.JournalStart
	for _, record := range page.Records {
		if record.SequenceNumber <= last {
			t.Fatalf("out-of-order sequence %d after %d", record.SequenceNumber, last)
		}
		if record.EventID == "" || seen[record.EventID] {
			t.Fatalf("unstable/duplicate eventId %q", record.EventID)
		}
		seen[record.EventID] = true
		last = record.SequenceNumber
	}
	if session.history.count(terminal.CurrentThreadID) < 2 {
		t.Fatal("canonical history did not receive user and assistant entries")
	}
}

func TestStateWireContractIsNeutralAndUsesEmptyArrays(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/sessions/wire-session/state", nil)
	recorder := httptest.NewRecorder()
	newTestAPIServer().handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	if !strings.Contains(body, `"sessionId":"wire-session"`) || !strings.Contains(body, `"messages":[]`) {
		t.Fatalf("unexpected state response: %s", body)
	}
	for _, forbidden := range []string{`"scope"`, `projectId`, `project_id`, `teamId`, `team_id`} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("generic wire response leaked %q: %s", forbidden, body)
		}
	}
}

func TestHTTPReplayIncludesNestedAgentEvents(t *testing.T) {
	server := newTestAPIServer()
	session := server.lab.session("nested-replay")
	started := sendCommand(t, session, "send-structured", "structured")
	waitState(t, session, 5*time.Second, func(state agentkit.SessionState) bool {
		return state.ActiveRun != nil && state.ActiveRun.Outcome == agentkit.OutcomeCompleted
	})
	path := fmt.Sprintf(
		"/api/sessions/nested-replay/events?threadId=%s&runId=%s&streamEpoch=%d&after=-1&limit=500",
		started.Snapshot.CurrentThreadID,
		started.Snapshot.ActiveRun.RunID,
		started.Snapshot.Cursor.StreamEpoch,
	)
	request := httptest.NewRequest(http.MethodGet, path, nil)
	recorder := httptest.NewRecorder()
	server.handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	for _, required := range []string{`"event":"reasoning.delta"`, `"event":"status.updated"`, `"event":"data.delta"`} {
		if !strings.Contains(body, required) {
			t.Fatalf("nested agent event %s missing from replay: %s", required, body)
		}
	}
	if !strings.Contains(body, `"next":{"runId":"`+started.Snapshot.ActiveRun.RunID+`"`) {
		t.Fatalf("replay cursor did not preserve network run ID: %s", body)
	}
}
