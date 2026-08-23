package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	agentkit "github.com/eadwinCode/agent-kit/go"
)

type streamCursor struct {
	RunID          string `json:"runId"`
	StreamEpoch    int    `json:"streamEpoch"`
	SequenceNumber int    `json:"sequenceNumber"`
}

type stateSnapshot struct {
	SchemaVersion     int                   `json:"schemaVersion"`
	SessionID         string                `json:"sessionId"`
	CurrentThreadID   string                `json:"currentThreadId,omitempty"`
	ActiveRun         *agentkit.ActiveRun   `json:"activeRun"`
	Pause             agentkit.PauseInfo    `json:"pause"`
	Activity          agentkit.Activity     `json:"activity"`
	Approval          agentkit.ApprovalInfo `json:"approval"`
	Revision          int64                 `json:"revision"`
	Cursor            *streamCursor         `json:"cursor"`
	ReconcileRequired bool                  `json:"reconcileRequired"`
	LastErrorCode     string                `json:"lastErrorCode,omitempty"`
	CheckpointKind    string                `json:"checkpointKind,omitempty"`
}

func snapshotOf(state agentkit.SessionState) stateSnapshot {
	var active *agentkit.ActiveRun
	var cursor *streamCursor
	if state.ActiveRun != nil && state.ActiveRun.Outcome == agentkit.OutcomeNone {
		copy := *state.ActiveRun
		active = &copy
		cursor = &streamCursor{
			RunID: copy.RunID, StreamEpoch: state.StreamEpoch,
			SequenceNumber: state.LastSequenceNumber,
		}
	}
	return stateSnapshot{
		SchemaVersion: state.SchemaVersion, SessionID: string(state.Scope),
		CurrentThreadID: state.CurrentThreadID, ActiveRun: active,
		Pause: state.Pause, Activity: state.Activity, Approval: state.Approval,
		Revision: state.Revision, Cursor: cursor,
		ReconcileRequired: state.ReconcileRequired,
		LastErrorCode:     state.LastErrorCode, CheckpointKind: state.CheckpointKind,
	}
}

func hydrationCursor(state agentkit.SessionState) *streamCursor {
	if state.ActiveRun == nil || state.ActiveRun.Outcome != agentkit.OutcomeNone {
		return nil
	}
	return &streamCursor{
		RunID: state.ActiveRun.RunID, StreamEpoch: state.StreamEpoch,
		SequenceNumber: agentkit.JournalStart,
	}
}

type apiServer struct{ lab *lab }

func newAPIServer() *apiServer { return &apiServer{lab: newLab()} }

func newAPIServerWithModel(models modelFactory) *apiServer {
	return &apiServer{lab: newLabWithModel(models, false)}
}

func (a *apiServer) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/sessions/{session}/state", a.state)
	mux.HandleFunc("GET /api/sessions/{session}/events", a.events)
	mux.HandleFunc("GET /api/sessions/{session}/live", a.live)
	mux.HandleFunc("GET /api/sessions/{session}/diagnostics", a.diagnostics)
	mux.HandleFunc("POST /api/sessions/{session}/commands", a.command)
	mux.HandleFunc("POST /api/sessions/{session}/reset", a.reset)
	return withSecurityHeaders(mux)
}

func withSecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

func sessionID(r *http.Request) (string, error) {
	id := r.PathValue("session")
	if id == "" || len(id) > 80 {
		return "", apiFailure(httpStatusBadRequest, "INVALID_SESSION", "session must be 1 to 80 characters", false)
	}
	for _, c := range id {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' && c != '_' {
			return "", apiFailure(httpStatusBadRequest, "INVALID_SESSION", "session contains unsupported characters", false)
		}
	}
	return id, nil
}

func (a *apiServer) state(w http.ResponseWriter, r *http.Request) {
	session, err := a.requestSession(r)
	if err != nil {
		writeError(w, err)
		return
	}
	state, err := session.handles.State.Load(r.Context(), session.id)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"snapshot": snapshotOf(state),
		"messages": session.history.messages(state.CurrentThreadID),
		"cursor":   hydrationCursor(state),
	})
}

func (a *apiServer) events(w http.ResponseWriter, r *http.Request) {
	session, err := a.requestSession(r)
	if err != nil {
		writeError(w, err)
		return
	}
	query := r.URL.Query()
	if len(query.Get("threadId")) > 120 || len(query.Get("runId")) > 120 {
		writeError(w, apiFailure(httpStatusBadRequest, "INVALID_CURSOR", "cursor identity is too long", false))
		return
	}
	after, err := strconv.Atoi(query.Get("after"))
	if err != nil {
		writeError(w, apiFailure(httpStatusBadRequest, "INVALID_CURSOR", "after must be an integer", false))
		return
	}
	epoch, err := strconv.Atoi(query.Get("streamEpoch"))
	if err != nil {
		writeError(w, apiFailure(httpStatusBadRequest, "INVALID_CURSOR", "streamEpoch must be an integer", false))
		return
	}
	limit := 100
	if raw := query.Get("limit"); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > 500 {
			writeError(w, apiFailure(httpStatusBadRequest, "INVALID_LIMIT", "limit must be between 1 and 500", false))
			return
		}
	}
	page, err := session.handles.Journal.Read(r.Context(), agentkit.JournalQuery{
		Scope: session.id, ThreadID: query.Get("threadId"), Limit: limit,
		// Network and nested-agent records share one gapless epoch/sequence
		// stream but carry different record run IDs. Read the whole epoch and
		// preserve the client-facing network run ID on the returned cursor.
		After: agentkit.JournalCursor{StreamEpoch: epoch, SequenceNumber: after},
	})
	if err != nil {
		writeError(w, err)
		return
	}
	events := make([]agentkit.AgentMessageChunk, 0, len(page.Records))
	for _, record := range page.Records {
		var data map[string]any
		if err := json.Unmarshal(record.Data, &data); err != nil {
			writeError(w, err)
			return
		}
		events = append(events, agentkit.AgentMessageChunk{
			Event: record.Event, Data: data, Timestamp: record.Timestamp,
			SequenceNumber: record.SequenceNumber, ID: "replay-" + record.EventID,
			EventID: record.EventID, StreamEpoch: record.StreamEpoch,
			SchemaVersion: record.SchemaVersion,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"events":  events,
		"next":    streamCursor{RunID: query.Get("runId"), StreamEpoch: page.Next.StreamEpoch, SequenceNumber: page.Next.SequenceNumber},
		"hasMore": page.HasMore, "retentionGap": page.RetentionGap,
	})
}

func (a *apiServer) command(w http.ResponseWriter, r *http.Request) {
	session, err := a.requestSession(r)
	if err != nil {
		writeError(w, err)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	defer r.Body.Close()
	if mediaType := r.Header.Get("Content-Type"); !strings.HasPrefix(strings.ToLower(mediaType), "application/json") {
		writeError(w, apiFailure(http.StatusUnsupportedMediaType, "UNSUPPORTED_MEDIA_TYPE", "content-type must be application/json", false))
		return
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var command commandRequest
	if err := decoder.Decode(&command); err != nil {
		writeError(w, apiFailure(httpStatusBadRequest, "INVALID_JSON", "command body is not valid", false))
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(w, apiFailure(httpStatusBadRequest, "INVALID_JSON", "command body must contain one JSON object", false))
		return
	}
	response, err := session.executeCommand(r.Context(), command)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (a *apiServer) live(w http.ResponseWriter, r *http.Request) {
	session, err := a.requestSession(r)
	if err != nil {
		writeError(w, err)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, errorsNew("streaming is unsupported"))
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	stream, unsubscribe := session.broker.subscribe()
	defer unsubscribe()
	fmt.Fprint(w, ": connected\n\n")
	flusher.Flush()
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			fmt.Fprint(w, ": heartbeat\n\n")
			flusher.Flush()
		case event, ok := <-stream:
			if !ok {
				return
			}
			payload, _ := json.Marshal(event)
			fmt.Fprintf(w, "data: %s\n\n", payload)
			flusher.Flush()
		}
	}
}

func (a *apiServer) diagnostics(w http.ResponseWriter, r *http.Request) {
	session, err := a.requestSession(r)
	if err != nil {
		writeError(w, err)
		return
	}
	state, _ := session.handles.State.Load(r.Context(), session.id)
	session.mu.Lock()
	running, epoch := session.running, session.epoch
	session.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"journalRecords": session.handles.Journal.LenFor(session.id, state.CurrentThreadID),
		"historyEntries": session.history.count(state.CurrentThreadID),
		"finalizerCalls": session.handles.Finalizer.Calls(),
		"running":        running, "streamEpoch": epoch,
	})
}

func (a *apiServer) reset(w http.ResponseWriter, r *http.Request) {
	id, err := sessionID(r)
	if err != nil {
		writeError(w, err)
		return
	}
	current := a.lab.session(id)
	current.mu.Lock()
	running := current.running
	current.mu.Unlock()
	if running {
		writeError(w, apiFailure(httpStatusConflict, "ACTIVE_RUN_EXISTS", "finish or cancel the active run before reset", false))
		return
	}
	session := a.lab.reset(id)
	state, _ := session.handles.State.Load(context.Background(), session.id)
	writeJSON(w, http.StatusOK, map[string]any{"snapshot": snapshotOf(state)})
}

func (a *apiServer) requestSession(r *http.Request) (*demoSession, error) {
	id, err := sessionID(r)
	if err != nil {
		return nil, err
	}
	return a.lab.session(id), nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, err error) {
	failure := asCommandError(err)
	body := map[string]any{"error": map[string]any{
		"code": failure.code, "message": failure.message, "recoverable": failure.recoverable,
	}}
	if failure.snapshot != nil {
		body["snapshot"] = failure.snapshot
	}
	writeJSON(w, failure.status, body)
}

func errorsNew(message string) error {
	return &commandError{status: http.StatusInternalServerError, code: "STREAM_UNAVAILABLE", message: message, recoverable: true}
}
