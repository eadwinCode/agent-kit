package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	agentkit "github.com/eadwinCode/agent-kit/go"
	"github.com/eadwinCode/agent-kit/go/durable"
	"github.com/eadwinCode/agent-kit/go/memadapter"
)

type demoState struct {
	LastScenario string `json:"lastScenario,omitempty"`
}

type broker struct {
	mu          sync.Mutex
	subscribers map[chan agentkit.AgentMessageChunk]struct{}
}

func newBroker() *broker {
	return &broker{subscribers: map[chan agentkit.AgentMessageChunk]struct{}{}}
}

func (b *broker) subscribe() (<-chan agentkit.AgentMessageChunk, func()) {
	ch := make(chan agentkit.AgentMessageChunk, 256)
	b.mu.Lock()
	b.subscribers[ch] = struct{}{}
	b.mu.Unlock()
	return ch, func() {
		b.mu.Lock()
		if _, ok := b.subscribers[ch]; ok {
			delete(b.subscribers, ch)
			close(ch)
		}
		b.mu.Unlock()
	}
}

func (b *broker) publish(chunk agentkit.AgentMessageChunk) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subscribers {
		select {
		case ch <- chunk:
		default:
			// Live delivery is deliberately best-effort. The journal is the
			// recovery authority, and the client will backfill this gap.
		}
	}
}

type cachedCommand struct {
	hash     string
	response commandResponse
}

type demoSession struct {
	id                agentkit.SessionScope
	handles           *memadapter.Ports
	history           *demoHistory
	broker            *broker
	models            modelFactory
	requiresOpenAIKey bool

	commandMu    sync.Mutex
	mu           sync.Mutex
	epoch        int
	threadNumber int
	threadID     string
	running      bool
	lastPrompt   string
	lastScenario string
	commands     map[string]cachedCommand
}

func newDemoSession(id string) *demoSession {
	return newDemoSessionWithModel(id, modelFor, true)
}

func newDemoSessionWithModel(id string, models modelFactory, requiresOpenAIKey bool) *demoSession {
	handles, _ := memadapter.NewPorts(agentkit.SessionScope(id), 0)
	s := &demoSession{
		id:                agentkit.SessionScope(id),
		handles:           handles,
		history:           newDemoHistory(),
		broker:            newBroker(),
		models:            models,
		requiresOpenAIKey: requiresOpenAIKey,
		threadNumber:      1,
		threadID:          "thread-1",
		commands:          map[string]cachedCommand{},
	}
	handles.State.Set(agentkit.SessionState{
		SchemaVersion:      agentkit.ContractSchemaVersion,
		Scope:              s.id,
		CurrentThreadID:    s.threadID,
		Pause:              agentkit.PauseInfo{State: agentkit.PauseNone},
		Activity:           agentkit.Activity{Kind: agentkit.ActivityNone},
		Approval:           agentkit.ApprovalInfo{Status: agentkit.ApprovalNone},
		LastSequenceNumber: agentkit.JournalStart,
		Revision:           agentkit.InitialStateRevision,
		UpdatedAt:          time.Now().UTC(),
	})
	return s
}

func (s *demoSession) runtimePorts(epoch int) *agentkit.RuntimePorts {
	return &agentkit.RuntimePorts{
		Journal:   s.handles.Journal,
		State:     s.handles.State,
		Control:   s.handles.Control,
		Approvals: s.handles.Approvals,
		Finalizer: s.handles.Finalizer,
		Sink: agentkit.SinkFunc(func(ctx context.Context, chunk agentkit.AgentMessageChunk) error {
			if err := s.handles.Sink.Deliver(ctx, chunk); err != nil {
				return err
			}
			s.broker.publish(chunk)
			return nil
		}),
		Scope:       s.id,
		StreamEpoch: epoch,
	}
}

func buildNetwork(scenario string, history *agentkit.HistoryConfig[demoState], models modelFactory) *agentkit.Network[demoState] {
	itemDelay := 350 * time.Millisecond
	if scenario == "slow" {
		// Give a person enough time to observe activity labels and request a
		// pause before the next safe between-items checkpoint.
		itemDelay = 900 * time.Millisecond
	}
	scanWorkspace := agentkit.NewTool[demoState](
		"scan_workspace",
		"Scan a deterministic five-item workspace and report structured progress.",
		func(ctx context.Context, in struct {
			Depth int `json:"depth"`
		}, opts agentkit.ToolOptions[demoState]) (any, error) {
			opts.Stream.Status(ctx, agentkit.StatusUpdate{
				Kind: agentkit.ActivityReading, Label: "Scanning demo workspace",
				Source: agentkit.ActivityFromTool,
			})
			partID := opts.Stream.Data(ctx, agentkit.DataPart{
				Type: "workspace-scan", Payload: agentkit.JSONValue(`{"files":[]}`),
			})
			files := []string{"README.md", "main.go", "App.jsx", "session-transport.js", "lab_test.go"}
			for i, file := range files {
				if err := opts.Stream.Checkpoint(ctx, agentkit.CheckpointBetweenItems); err != nil {
					return nil, err
				}
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(itemDelay):
				}
				opts.Stream.Progress(ctx, agentkit.ToolProgress{
					ToolName: "scan_workspace", Completed: i + 1, Total: len(files), Label: file,
				})
				delta, _ := json.Marshal(map[string]any{"file": file, "index": i})
				opts.Stream.DataDelta(ctx, partID, agentkit.JSONValue(delta))
			}
			if err := opts.Stream.Checkpoint(ctx, agentkit.CheckpointBeforeSideEffect); err != nil {
				return nil, err
			}
			final, _ := json.Marshal(map[string]any{"files": files, "requestedDepth": in.Depth})
			opts.Stream.CompleteData(ctx, partID, agentkit.JSONValue(final))
			if err := opts.Stream.Checkpoint(ctx, agentkit.CheckpointAfterSideEffect); err != nil {
				return nil, err
			}
			return map[string]any{"files": files}, nil
		},
	)

	publishDemo := agentkit.NewTool[demoState](
		"publish_demo",
		"Publish the demo after an explicit human approval.",
		func(ctx context.Context, _ struct{}, opts agentkit.ToolOptions[demoState]) (any, error) {
			approvalID := "approval-" + opts.ToolCallID
			if _, err := opts.Approvals.Require(ctx, agentkit.ApprovalRequest{
				RequestID:  approvalID,
				ToolName:   "publish_demo",
				ToolCallID: opts.ToolCallID,
				Summary:    "Publish the deterministic demo release",
				ExpiresAt:  time.Now().UTC().Add(2 * time.Minute),
			}); err != nil {
				return nil, err
			}
			return map[string]any{"published": true, "release": "demo-v1"}, nil
		},
	)

	failDemo := agentkit.NewTool[demoState](
		"fail_demo",
		"Exercise AgentKit's typed terminal failure path.",
		func(ctx context.Context, _ struct{}, opts agentkit.ToolOptions[demoState]) (any, error) {
			opts.Stream.Status(ctx, agentkit.StatusUpdate{
				Kind: agentkit.ActivityTool, Label: "Exercising failure handling",
				Source: agentkit.ActivityFromTool,
			})
			return nil, errors.New("intentional demo tool failure")
		},
	)

	baseSystem := "You are the live GPT-5.6 Luna assistant inside a React and Go AgentKit session lab. " +
		"The lab demonstrates server-authoritative state, canonical history, durable event replay, " +
		"streamed reasoning and text, pause/resume/cancel, approvals, structured tool progress, " +
		"typed failures, and exactly-once finalization. "
	system := baseSystem + "Answer the user's message directly and concisely."
	var tools []agentkit.Tool[demoState]
	switch scenario {
	case "structured", "slow":
		system = baseSystem + "Call scan_workspace exactly once with depth 5. After its result, summarize the files and do not call it again."
		tools = []agentkit.Tool[demoState]{scanWorkspace}
	case "approval":
		system = baseSystem + "Call publish_demo exactly once. After the approved tool result, confirm the published release and do not call it again."
		tools = []agentkit.Tool[demoState]{publishDemo}
	case "error":
		system = baseSystem + "Call fail_demo exactly once to exercise the requested failure path."
		tools = []agentkit.Tool[demoState]{failDemo}
	}

	model, modelOptions := models(scenario)
	assistant := agentkit.NewAgent(agentkit.AgentConfig[demoState]{
		Name: "session-lab", System: system,
		Model: model, ModelOptions: modelOptions,
		Tools: tools,
	})
	return agentkit.NewNetwork(agentkit.NetworkConfig[demoState]{
		Name: "stateful-react-go", Agents: []*agentkit.Agent[demoState]{assistant},
		History: history, MaxIter: 4,
		Router: &agentkit.Router[demoState]{Fn: func(_ context.Context, args agentkit.RouterArgs[demoState]) (*agentkit.RouterResult[demoState], error) {
			if args.CallCount == 0 {
				return agentkit.RouteTo(assistant), nil
			}
			if args.LastResult != nil && len(args.LastResult.ToolCalls) > 0 {
				return agentkit.RouteTo(assistant), nil
			}
			return nil, nil
		}},
	})
}

func (s *demoSession) startRun(prompt, scenario string) error {
	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		return apiFailure(httpStatusBadRequest, "INVALID_COMMAND", "payload.message is required", false)
	}
	if scenario == "" {
		scenario = "text"
	}
	if s.models == nil {
		return apiFailure(httpStatusServiceUnavailable, "MODEL_NOT_CONFIGURED", "the server has no model factory", false)
	}
	if s.requiresOpenAIKey && strings.TrimSpace(os.Getenv("OPENAI_API_KEY")) == "" {
		return apiFailure(httpStatusServiceUnavailable, "OPENAI_NOT_CONFIGURED", "set OPENAI_API_KEY on the Go server to use GPT-5.6 Luna", false)
	}
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return apiFailure(httpStatusConflict, "ACTIVE_RUN_EXISTS", "a run is already active", false)
	}
	s.running = true
	s.epoch++
	epoch := s.epoch
	threadID := s.threadID
	s.lastPrompt = prompt
	s.lastScenario = scenario
	s.mu.Unlock()

	network := buildNetwork(scenario, s.history.config(), s.models)
	ports := s.runtimePorts(epoch)
	go func() {
		defer func() {
			s.mu.Lock()
			s.running = false
			s.mu.Unlock()
		}()
		state := agentkit.NewState(agentkit.StateConfig[demoState]{
			Data: demoState{LastScenario: scenario}, ThreadID: threadID,
		})
		_, err := network.Run(context.Background(), prompt, &agentkit.NetworkRunOptions[demoState]{
			State: state,
			UserMessage: &agentkit.UserMessage{
				ID: uuid.NewString(), Content: prompt, Role: agentkit.RoleUser,
			},
			Step: durable.Inline{},
			Streaming: &agentkit.StreamingConfig{
				StreamReasoning: true, SimulateChunking: true, ChunkSize: 18,
			},
			Ports: ports,
		})
		if err != nil && !agentkit.IsCancelled(err) {
			log.Printf("session %s scenario %s failed: %v", s.id, scenario, err)
		}
	}()
	return nil
}

func (s *demoSession) waitForRunState(ctx context.Context, previousRevision int64) agentkit.SessionState {
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		state, _ := s.handles.State.Load(ctx, s.id)
		if state.Revision > previousRevision && state.ActiveRun != nil {
			return state
		}
		select {
		case <-ctx.Done():
			return state
		case <-deadline.C:
			return state
		case <-ticker.C:
		}
	}
}

type commandRequest struct {
	CommandID        string         `json:"commandId"`
	Type             string         `json:"type"`
	ThreadID         string         `json:"threadId,omitempty"`
	RunID            string         `json:"runId,omitempty"`
	ApprovalID       string         `json:"approvalId,omitempty"`
	PauseEpoch       int            `json:"pauseEpoch,omitempty"`
	ExpectedRevision int64          `json:"expectedRevision,omitempty"`
	Payload          map[string]any `json:"payload,omitempty"`
}

type commandResponse struct {
	Snapshot    stateSnapshot `json:"snapshot"`
	Duplicate   bool          `json:"duplicate,omitempty"`
	OutcomeCode string        `json:"outcomeCode,omitempty"`
	Cursor      *streamCursor `json:"cursor,omitempty"`
}

func hashCommand(command commandRequest) string {
	raw, _ := json.Marshal(command)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func (s *demoSession) executeCommand(ctx context.Context, command commandRequest) (commandResponse, error) {
	// The production equivalent is a transaction around the command row and
	// state CAS. Serializing this in-memory adapter closes the same-command
	// race instead of pretending an ordinary map is an idempotency store.
	s.commandMu.Lock()
	defer s.commandMu.Unlock()

	if strings.TrimSpace(command.CommandID) == "" {
		return commandResponse{}, apiFailure(httpStatusBadRequest, "INVALID_COMMAND", "commandId is required", false)
	}
	if len(command.CommandID) > 120 {
		return commandResponse{}, apiFailure(httpStatusBadRequest, "INVALID_COMMAND", "commandId is too long", false)
	}
	hash := hashCommand(command)
	s.mu.Lock()
	if cached, ok := s.commands[command.CommandID]; ok {
		s.mu.Unlock()
		if cached.hash != hash {
			return commandResponse{}, apiFailure(httpStatusConflict, "IDEMPOTENCY_KEY_REUSED", "commandId was reused with a different payload", false)
		}
		response := cached.response
		response.Duplicate = true
		return response, nil
	}
	s.mu.Unlock()

	current, err := s.handles.State.Load(ctx, s.id)
	if err != nil {
		return commandResponse{}, err
	}
	commandType := agentkit.CommandType(command.Type)
	staleRevision := command.ExpectedRevision > 0 && command.ExpectedRevision != current.Revision
	controlsCurrentRun := (commandType == agentkit.CommandPause ||
		commandType == agentkit.CommandResume ||
		commandType == agentkit.CommandCancel ||
		commandType == agentkit.CommandApprove ||
		commandType == agentkit.CommandDeny) &&
		!current.IsTerminal() && current.ActiveRun != nil &&
		command.RunID == current.ActiveRun.RunID &&
		command.ThreadID == current.CurrentThreadID
	if staleRevision && !controlsCurrentRun {
		return commandResponse{}, &commandError{
			status: httpStatusConflict, code: "STATE_REVISION_MISMATCH",
			message:  "the session changed; reconcile from the returned snapshot",
			snapshot: ptrSnapshot(snapshotOf(current)),
		}
	}

	switch commandType {
	case agentkit.CommandSend, agentkit.CommandRetry, agentkit.CommandEdit:
		prompt, scenario := "", ""
		if commandType == agentkit.CommandRetry {
			s.mu.Lock()
			prompt, scenario = s.lastPrompt, s.lastScenario
			s.mu.Unlock()
		} else {
			prompt, _ = command.Payload["message"].(string)
			scenario, _ = command.Payload["scenario"].(string)
		}
		if !current.IsTerminal() {
			return commandResponse{}, apiFailure(httpStatusConflict, "ACTIVE_RUN_EXISTS", "wait for the active run to finish", false)
		}
		if err := s.startRun(prompt, scenario); err != nil {
			return commandResponse{}, err
		}
		current = s.waitForRunState(ctx, current.Revision)
	case agentkit.CommandPause, agentkit.CommandResume, agentkit.CommandCancel:
		if current.IsTerminal() {
			return commandResponse{}, apiFailure(httpStatusConflict, "RUN_TERMINAL", "there is no active run to control", false)
		}
		if command.RunID != current.ActiveRun.RunID || command.ThreadID != current.CurrentThreadID {
			return commandResponse{}, apiFailure(httpStatusConflict, "STALE_RUN", "the command does not target the active run", false)
		}
		if commandType == agentkit.CommandResume && command.PauseEpoch != current.Pause.Epoch {
			return commandResponse{}, apiFailure(httpStatusConflict, "STALE_PAUSE_EPOCH", "resume does not match the current pause epoch", false)
		}
		_, err = s.handles.Control.Record(ctx, agentkit.ControlCommand{
			Scope: s.id, ID: command.CommandID, Type: commandType,
			ThreadID: command.ThreadID, RunID: command.RunID,
			PauseEpoch: command.PauseEpoch, PayloadHash: hash,
			ExpectedRevision: command.ExpectedRevision, CreatedAt: time.Now().UTC(),
		})
		if err != nil {
			return commandResponse{}, err
		}
		current, _ = s.handles.State.Load(ctx, s.id)
	case agentkit.CommandApprove, agentkit.CommandDeny:
		if current.IsTerminal() || command.RunID != current.ActiveRun.RunID || command.ThreadID != current.CurrentThreadID {
			return commandResponse{}, apiFailure(httpStatusConflict, "STALE_RUN", "the command does not target the active run", false)
		}
		if command.ApprovalID == "" || current.Approval.ApprovalID != command.ApprovalID {
			return commandResponse{}, apiFailure(httpStatusConflict, "APPROVAL_NOT_FOUND", "approvalId is not pending for this session", false)
		}
		status := agentkit.ApprovalApproved
		reason := "approved in the React session lab"
		if commandType == agentkit.CommandDeny {
			status = agentkit.ApprovalDenied
			reason = "denied in the React session lab"
		}
		s.handles.Approvals.DecideFor(s.id, command.ApprovalID, status, reason)
		current, _ = s.handles.State.Load(ctx, s.id)
	case agentkit.CommandNewChat:
		if !current.IsTerminal() {
			return commandResponse{}, apiFailure(httpStatusConflict, "ACTIVE_RUN_EXISTS", "cancel or finish the active run first", false)
		}
		s.mu.Lock()
		nextThreadNumber := s.threadNumber + 1
		threadID := fmt.Sprintf("thread-%d", nextThreadNumber)
		s.mu.Unlock()
		current, err = s.handles.State.CompareAndSwap(ctx, agentkit.StateTransition{
			Scope: s.id, ExpectedRevision: current.Revision, Reason: "new_chat",
			Apply: func(state *agentkit.SessionState) {
				state.CurrentThreadID = threadID
				state.ActiveRun = nil
				state.Pause = agentkit.PauseInfo{State: agentkit.PauseNone}
				state.Activity = agentkit.Activity{Kind: agentkit.ActivityNone}
				state.Approval = agentkit.ApprovalInfo{Status: agentkit.ApprovalNone}
				state.StreamEpoch = 0
				state.LastSequenceNumber = agentkit.JournalStart
			},
		})
		if err != nil {
			return commandResponse{}, err
		}
		s.mu.Lock()
		s.threadNumber = nextThreadNumber
		s.threadID = threadID
		s.mu.Unlock()
	default:
		return commandResponse{}, apiFailure(httpStatusBadRequest, "UNSUPPORTED_COMMAND", "this demo does not implement that command", false)
	}

	response := commandResponse{Snapshot: snapshotOf(current), Cursor: hydrationCursor(current), OutcomeCode: "accepted"}
	s.mu.Lock()
	s.commands[command.CommandID] = cachedCommand{hash: hash, response: response}
	s.mu.Unlock()
	return response, nil
}

type lab struct {
	mu                sync.Mutex
	sessions          map[string]*demoSession
	models            modelFactory
	requiresOpenAIKey bool
}

func newLab() *lab { return newLabWithModel(modelFor, true) }

func newLabWithModel(models modelFactory, requiresOpenAIKey bool) *lab {
	return &lab{sessions: map[string]*demoSession{}, models: models, requiresOpenAIKey: requiresOpenAIKey}
}

func (l *lab) session(id string) *demoSession {
	l.mu.Lock()
	defer l.mu.Unlock()
	if session := l.sessions[id]; session != nil {
		return session
	}
	session := newDemoSessionWithModel(id, l.models, l.requiresOpenAIKey)
	l.sessions[id] = session
	return session
}

func (l *lab) reset(id string) *demoSession {
	l.mu.Lock()
	defer l.mu.Unlock()
	session := newDemoSessionWithModel(id, l.models, l.requiresOpenAIKey)
	l.sessions[id] = session
	return session
}

type commandError struct {
	status      int
	code        string
	message     string
	recoverable bool
	snapshot    *stateSnapshot
}

func (e *commandError) Error() string { return e.message }

const (
	httpStatusBadRequest         = 400
	httpStatusConflict           = 409
	httpStatusServiceUnavailable = 503
)

func apiFailure(status int, code, message string, recoverable bool) error {
	return &commandError{status: status, code: code, message: message, recoverable: recoverable}
}

func ptrSnapshot(snapshot stateSnapshot) *stateSnapshot { return &snapshot }

func asCommandError(err error) *commandError {
	var target *commandError
	if errors.As(err, &target) {
		return target
	}
	return &commandError{status: 500, code: "INTERNAL", message: "the demo server could not complete the request", recoverable: true}
}
