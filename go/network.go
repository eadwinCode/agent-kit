package agentkit

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/zendev-sh/goai/provider"

	"github.com/eadwinCode/agent-kit/go/durable"
	"github.com/eadwinCode/agent-kit/go/internal/jsonutil"
)

// Network is a network of agents. Immutable after construction (port
// decision 11): Run clones the template state into a per-run NetworkRun,
// and router-introduced agents live on the run, not the template.
type Network[T any] struct {
	// Name of the system of agents.
	Name string

	Description string

	// DefaultModel applies to agents without their own model.
	DefaultModel provider.LanguageModel

	// DefaultModelOptions accompany DefaultModel (cache control, call
	// options such as thinking budgets).
	DefaultModelOptions []AgenticModelOption

	// Router decides which agents run next. Nil uses the built-in agentic
	// router (requires DefaultModel).
	Router *Router[T]

	// StopWhen is a stop policy evaluated before each agent inference —
	// decoupled from routing, at the safe between-inference boundary. It
	// MUST be a pure function of the run state (no wall-clock/random): an
	// Inngest replay must stop at the same point.
	StopWhen StopWhen[T]

	// MaxIter caps agent calls per run (0 = unlimited).
	MaxIter int

	// History persists conversation state across runs.
	History *HistoryConfig[T]

	// Ports supplies the default runtime contracts (journal, state,
	// control, approvals, structured sink, finalizer). NetworkRunOptions.
	// Ports overrides it per run.
	Ports *RuntimePorts

	// agents in insertion order (order is prompt-visible in the default
	// router's system prompt and must be deterministic), with a name index.
	agents     []*Agent[T]
	agentIndex map[string]int

	// defaultState is the template state cloned for each run.
	defaultState *State[T]
}

// NetworkConfig configures NewNetwork.
type NetworkConfig[T any] struct {
	Name                string
	Description         string
	Agents              []*Agent[T]
	DefaultModel        provider.LanguageModel
	DefaultModelOptions []AgenticModelOption
	Router              *Router[T]
	StopWhen            StopWhen[T]
	MaxIter             int
	History             *HistoryConfig[T]
	Ports               *RuntimePorts
	// DefaultState seeds each run's state (optional).
	DefaultState *State[T]
}

// NewNetwork creates a network of agents.
func NewNetwork[T any](cfg NetworkConfig[T]) *Network[T] {
	n := &Network[T]{
		Name:                cfg.Name,
		Description:         cfg.Description,
		DefaultModel:        cfg.DefaultModel,
		DefaultModelOptions: cfg.DefaultModelOptions,
		Router:              cfg.Router,
		StopWhen:            cfg.StopWhen,
		MaxIter:             cfg.MaxIter,
		History:             cfg.History,
		Ports:               cfg.Ports,
		agentIndex:          map[string]int{},
		defaultState:        cfg.DefaultState,
	}
	if n.defaultState == nil {
		n.defaultState = NewState(StateConfig[T]{})
	}
	for _, a := range cfg.Agents {
		n.addAgent(a)
	}
	return n
}

func (n *Network[T]) addAgent(a *Agent[T]) {
	if _, exists := n.agentIndex[a.Name]; exists {
		return
	}
	n.agentIndex[a.Name] = len(n.agents)
	n.agents = append(n.agents, a)
}

// Agents returns the network's agents in insertion order.
func (n *Network[T]) Agents() []*Agent[T] {
	return append([]*Agent[T](nil), n.agents...)
}

// AgentByName looks up a template agent.
func (n *Network[T]) AgentByName(name string) (*Agent[T], bool) {
	i, ok := n.agentIndex[name]
	if !ok {
		return nil, false
	}
	return n.agents[i], true
}

// Router picks the next agents; exactly one field is set. A nil *Router on
// the network selects the built-in agentic router.
type Router[T any] struct {
	// Fn is a code router.
	Fn FnRouter[T]
	// Agent is an agentic router — an inference call decides.
	Agent *RoutingAgent[T]
}

// FnRouter decides the next agents in code. Return nil (or an empty
// result) to end the run; return Routing to delegate the decision to a
// routing agent.
type FnRouter[T any] func(ctx context.Context, args RouterArgs[T]) (*RouterResult[T], error)

// RouterResult is an FnRouter's verdict.
type RouterResult[T any] struct {
	// Agents are scheduled next, in order.
	Agents []*Agent[T]
	// Routing delegates to an agentic router instead.
	Routing *RoutingAgent[T]
}

// RouteTo schedules specific agents.
func RouteTo[T any](agents ...*Agent[T]) *RouterResult[T] {
	return &RouterResult[T]{Agents: agents}
}

// RouteVia delegates to a routing agent.
func RouteVia[T any](ra *RoutingAgent[T]) *RouterResult[T] {
	return &RouterResult[T]{Routing: ra}
}

// RouterArgs is the FnRouter payload.
type RouterArgs[T any] struct {
	// Input is the string content of the network's input.
	Input string
	// UserMessage is the rich input, when one was provided.
	UserMessage *UserMessage
	// Network is the current run; state lives on Network.State.
	Network *NetworkRun[T]
	// Stack is the ordered list of agents already scheduled.
	Stack []*Agent[T]
	// CallCount is the number of agent invocations made so far.
	CallCount int
	// LastResult is the network's most recent inference result.
	LastResult *AgentResult
	// Stream is the router's typed structured emitter: semantic status,
	// domain data parts, progress and safe boundaries, published into the
	// run's ordered stream with stable part identity. Never nil — a run
	// without streaming supplies a no-op.
	Stream StructuredStream
}

// NetworkStop is returned by StopWhen to end a run early.
type NetworkStop struct {
	// Reason is surfaced on run.interrupted and the terminal events
	// ("budget", "max_tokens", "timeout", "user_cancellation", ...).
	Reason string `json:"reason"`
	// Metadata is attached to the run.interrupted event.
	Metadata map[string]any `json:"metadata,omitempty"`
}

// StopWhen ends a run early at the safe between-inference boundary (every
// persisted AgentResult is complete: output + all toolCalls). Return nil
// to continue.
type StopWhen[T any] func(ctx context.Context, args StopWhenArgs[T]) (*NetworkStop, error)

// StopWhenArgs is the StopWhen payload.
type StopWhenArgs[T any] struct {
	Network    *NetworkRun[T]
	CallCount  int
	LastResult *AgentResult
}

// NetworkRunOptions configures one Network.Run.
type NetworkRunOptions[T any] struct {
	// UserMessage is the rich client input; its Content wins over the
	// plain input string when set.
	UserMessage *UserMessage
	// Router overrides the network's router for this run.
	Router *Router[T]
	// StopWhen overrides the network's stop policy for this run.
	StopWhen StopWhen[T]
	// State overrides the template state for this run.
	State *State[T]
	// Step overrides the durability seam (tests use durable.Inline).
	Step durable.Step
	// Streaming enables event emission for this run.
	Streaming *StreamingConfig
	// Ports overrides the network's runtime contracts for this run.
	Ports *RuntimePorts
}

// Run handles a request using the network of agents. The network template
// is untouched; all run state lives on the returned NetworkRun.
func (n *Network[T]) Run(ctx context.Context, input string, opts *NetworkRunOptions[T]) (*NetworkRun[T], error) {
	if opts == nil {
		opts = &NetworkRunOptions[T]{}
	}
	state := opts.State
	if state == nil {
		state = n.defaultState.Clone()
	}
	run := newNetworkRun(n, state)
	if err := run.execute(ctx, input, opts); err != nil {
		return run, err
	}
	return run, nil
}

// NetworkRun is a Network bound to one run's state. Agents receive it as
// their execution context.
type NetworkRun[T any] struct {
	*Network[T]

	// State is this run's state (typed data + result stack).
	State *State[T]

	// StoppedBy is set when a StopWhen policy ended the run early.
	StoppedBy *NetworkStop

	// stack holds the names of agents scheduled to run next.
	stack []string

	// counter is the number of agent invocations made.
	counter int

	// extraAgents are router-introduced agents not in the network template
	// (run-scoped: the template stays immutable).
	extraAgents map[string]*Agent[T]

	// ports, controller, approvals and stream are the run's wiring to the
	// public runtime contracts. All four are safe to use when nil-valued:
	// the controllers degrade to no-ops.
	ports      *RuntimePorts
	controller *RunController
	approvals  *ApprovalController
	stream     *StreamingContext
}

// newNetworkRun binds a network to a run state.
func newNetworkRun[T any](n *Network[T], s *State[T]) *NetworkRun[T] {
	return &NetworkRun[T]{Network: n, State: s, extraAgents: map[string]*Agent[T]{}}
}

// Schedule pushes an agent onto the run's stack.
func (r *NetworkRun[T]) Schedule(agentName string) {
	r.stack = append(r.stack, agentName)
}

// AgentByName looks up an agent — template agents plus any the router
// introduced during this run.
func (r *NetworkRun[T]) AgentByName(name string) (*Agent[T], bool) {
	if a, ok := r.extraAgents[name]; ok {
		return a, true
	}
	return r.Network.AgentByName(name)
}

// CallCount is the number of agent invocations made this run.
func (r *NetworkRun[T]) CallCount() int { return r.counter }

// AvailableAgents returns the template agents whose Enabled lifecycle (if
// any) accepts the current state.
func (r *NetworkRun[T]) AvailableAgents(ctx context.Context) ([]*Agent[T], error) {
	var available []*Agent[T]
	for _, a := range r.Network.agents {
		if a.Lifecycle != nil && a.Lifecycle.Enabled != nil {
			ok, err := a.Lifecycle.Enabled(ctx, LifecycleBase[T]{Agent: a, Network: r})
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
		}
		available = append(available, a)
	}
	return available, nil
}

// execute drives the network loop: route → schedule → run agent → persist
// → route again, until the stack drains, MaxIter is hit, or StopWhen says
// stop.
func (r *NetworkRun[T]) execute(ctx context.Context, input string, opts *NetworkRunOptions[T]) (err error) {
	step := opts.Step
	if step == nil {
		step = durable.Inngest()
	}

	// Minted inside a step for deterministic replay; the network's run id
	// doubles as the messageId for network-scoped streaming events.
	networkRunID, err := durable.Run(ctx, step, "generate-network-id", func(ctx context.Context) (string, error) {
		return uuid.NewString(), nil
	})
	if err != nil {
		return err
	}

	stopWhen := opts.StopWhen
	if stopWhen == nil {
		stopWhen = r.Network.StopWhen
	}

	ports := opts.Ports
	if ports == nil {
		ports = r.Network.Ports
	}
	r.ports = ports
	controller := newRunController(ports, nil, nil)
	approvals := newApprovalController(ports, nil)
	r.controller = controller
	r.approvals = approvals

	inputContent := input
	if opts.UserMessage != nil && opts.UserMessage.Content != "" {
		inputContent = opts.UserMessage.Content
	}

	topCfg := threadOpConfig[T]{State: r.State, History: r.History, Input: inputContent, Network: r, Step: step}

	// Capture BEFORE initialization: an absent ThreadID means a brand-new
	// thread, so the initial history load is a guaranteed no-op DB read.
	hadClientThreadID := r.State.ThreadID != ""
	if err := initializeThread(ctx, topCfg); err != nil {
		return err
	}

	// Persist the user's message up front for resilience (enables
	// "regenerate" even when the run fails).
	var acceptedUserMessage *UserMessageRecord
	if r.History != nil && r.History.AppendUserMessage != nil {
		record, err := r.buildUserMessageRecord(ctx, input, opts.UserMessage, step)
		if err != nil {
			return err
		}
		if err := appendUserMessage(ctx, topCfg, record); err != nil {
			return err
		}
		acceptedUserMessage = &record
	}

	if hadClientThreadID {
		if err := loadThreadFromStorage(ctx, topCfg); err != nil {
			return err
		}
	}

	// Streaming starts after thread initialization so events carry the
	// resolved threadId.
	var sc *StreamingContext
	if opts.Streaming != nil && (opts.Streaming.Publish != nil || ports.wantsStream()) {
		sc = streamingContextFromState(r.State, *opts.Streaming, ports, networkRunID, networkRunID, "network")
		r.stream = sc
		controller.stream = sc
		controller.journal = sc.journal
		approvals.stream = sc
		sc.PublishEvent(ctx, EventRunStarted, map[string]any{
			"runId": networkRunID, "scope": "network", "name": r.Name, "messageId": networkRunID,
		})
		r.markRunning(ctx, ports, networkRunID)

		// Publish the server-accepted user turn as a standard event so every
		// authorized client — not only the tab that sent it — renders it
		// immediately, under the one stable message ID history will use.
		if acceptedUserMessage != nil {
			sc.PublishEvent(ctx, EventUserMessage, map[string]any{
				"messageId": acceptedUserMessage.ID,
				"runId":     networkRunID,
				"role":      string(acceptedUserMessage.Role),
				"content":   acceptedUserMessage.Content,
				"timestamp": acceptedUserMessage.Timestamp,
			})
		}

		// ONE terminal emitter for EVERY exit path (normal drain, stopWhen,
		// unknown agent, cancel, even after a failure) so subscribers
		// reliably unstick and exactly-one-terminal holds structurally. The
		// Finalizer — when configured — settles history, billing, repository
		// state and live drain BEFORE the terminal is published.
		terminal := newTerminalEmitter(sc, ports, sc.journal, controller, "network", r.Name, networkRunID)
		defer func() {
			extra := map[string]any{"messageId": networkRunID}
			reason := ""
			if r.StoppedBy != nil {
				reason = r.StoppedBy.Reason
			}
			terminal.Emit(ctx, err, reason, extra)
		}()
	}

	available, err := r.AvailableAgents(ctx)
	if err != nil {
		return err
	}
	if len(available) == 0 {
		return fmt.Errorf("agentkit: no agents enabled in network %q", r.Name)
	}

	initialResultCount := len(r.State.results)

	// Run-start boundary: a pause or cancel accepted before any inference
	// began costs nothing to honor here.
	if err := controller.Checkpoint(ctx, Checkpoint{
		Kind: CheckpointRunStart, Resumable: true,
	}); err != nil {
		return err
	}

	next, err := r.getNextAgents(ctx, inputContent, opts.UserMessage, r.effectiveRouter(opts), step)
	if err != nil {
		return err
	}
	if len(next) == 0 {
		return nil
	}
	for _, a := range next {
		r.Schedule(a.Name)
	}

	for len(r.stack) > 0 && (r.MaxIter == 0 || r.counter < r.MaxIter) {
		// Safe boundary between agents: every persisted AgentResult is
		// complete here, so a pause parks with nothing half-done and a
		// cancel is terminal without abandoning work mid-inference.
		if err := controller.Checkpoint(ctx, Checkpoint{
			Kind: CheckpointNetworkIteration, Resumable: true,
		}); err != nil {
			return err
		}

		// Stop policy: before popping/inferring, at the safe boundary.
		if stopWhen != nil {
			decision, err := stopWhen(ctx, StopWhenArgs[T]{
				Network:    r,
				CallCount:  r.counter,
				LastResult: r.lastResult(),
			})
			if err != nil {
				return err
			}
			if decision != nil {
				r.StoppedBy = decision
				if sc != nil {
					sc.PublishEvent(ctx, EventRunInterrupted, map[string]any{
						"runId": networkRunID, "scope": "network", "name": r.Name,
						"reason": decision.Reason, "metadata": decision.Metadata,
					})
				}
				break
			}
		}

		agentName := r.stack[0]
		r.stack = r.stack[1:]
		agent, ok := r.AgentByName(agentName)
		if !ok {
			// Unknown scheduled agent — treat the stack as drained.
			break
		}

		// Mint this iteration's ids durably (replay-stable). The message id
		// becomes the persisted AgentResult.ID — and therefore part of the
		// replay-stable checksum.
		ids, err := durable.Run(ctx, step, fmt.Sprintf("generate-agent-ids-%d", r.counter),
			func(ctx context.Context) (agentIterationIDs, error) {
				return agentIterationIDs{RunID: generateRunID(), MessageID: uuid.NewString()}, nil
			})
		if err != nil {
			return err
		}

		// Child streaming context: agent-specific run/message ids, shared
		// sequence counter so ordering holds across the whole run.
		var agentSC *StreamingContext
		if sc != nil {
			agentSC = sc.WithSharedSequence(ids.RunID, ids.MessageID, "agent")
			sc.PublishEvent(ctx, EventRunStarted, map[string]any{
				"runId": ids.RunID, "parentRunId": networkRunID, "scope": "agent",
				"name": agent.Name, "messageId": ids.MessageID,
			})
		}

		// The network is the iteration authority: the agent does exactly ONE
		// inference per call (MaxIterPerRun 1) and the router decides whether
		// to call again — total inferences stay ≤ MaxIter, not MaxIter².
		call, err := agent.Run(ctx, inputContent, &RunOptions[T]{
			Network:          r,
			MaxIterPerRun:    1,
			Step:             step,
			streamingContext: agentSC,
		})
		if err != nil {
			return err
		}

		// The durably minted message id becomes the persisted message_id.
		call.ID = ids.MessageID

		if agentSC != nil {
			agentSC.PublishEvent(ctx, EventRunCompleted, map[string]any{
				"runId": ids.RunID, "scope": "agent", "name": agent.Name, "messageId": ids.MessageID,
			})
		}

		r.counter++
		r.State.AppendResult(call)

		// Persist the result the moment it's produced, so a mid-run failure
		// or hard abort still leaves every completed inference on disk. The
		// step id is the iteration counter — replay-stable, NEVER the
		// checksum (which the id assignment above just changed).
		if err := PersistResults(ctx, PersistConfig[T]{
			State: r.State, History: r.History, Input: inputContent, Network: r, Step: step,
		}, []*AgentResult{call}, IncrementalAppendStepID(r.counter)); err != nil {
			return err
		}

		next, err := r.getNextAgents(ctx, inputContent, opts.UserMessage, r.effectiveRouter(opts), step)
		if err != nil {
			return err
		}
		for _, a := range next {
			r.Schedule(a.Name)
		}
	}

	// End-of-run backstop save (idempotent against the incremental saves).
	return saveThreadToStorage(ctx, topCfg, initialResultCount)
}

// structuredStream returns the run's public typed emitter, scoped to a
// tool name when one applies. It is never nil, so callers need no guards.
func (r *NetworkRun[T]) structuredStream(toolName string) StructuredStream {
	if r == nil || r.stream == nil {
		return noopStream{}
	}
	return newRunStream(r.stream, r.controller, r.Name, toolName)
}

// markRunning records that the run is executing, together with the cursor
// a client should tail from. Observation writes degrade: a state store that
// is briefly unreachable must not fail a run.
func (r *NetworkRun[T]) markRunning(ctx context.Context, ports *RuntimePorts, runID string) {
	if ports == nil || ports.State == nil {
		return
	}
	now := time.Now().UTC()
	epoch := ports.epoch()
	_, _ = mutateState(ctx, ports.State, ports.Scope, "run.executing", func(s *SessionState) {
		s.SchemaVersion = ContractSchemaVersion
		s.Scope = ports.Scope
		if r.State.ThreadID != "" {
			s.CurrentThreadID = r.State.ThreadID
		}
		if s.ActiveRun == nil || s.ActiveRun.RunID != runID {
			s.ActiveRun = &ActiveRun{RunID: runID, AcceptedAt: now}
		}
		s.ActiveRun.Lifecycle = LifecycleExecuting
		s.ActiveRun.Outcome = OutcomeNone
		if s.StreamEpoch != epoch {
			// Epoch change resets the sequence cursor: a client must not
			// wait for a number the new epoch will never produce.
			s.StreamEpoch = epoch
			s.LastSequenceNumber = JournalStart
		}
		s.Activity = Activity{Kind: ActivityPreparing, Source: ActivityFromServer}
		s.UpdatedAt = now
	})
}

type agentIterationIDs struct {
	RunID     string `json:"agentRunId"`
	MessageID string `json:"agentMessageId"`
}

func (r *NetworkRun[T]) effectiveRouter(opts *NetworkRunOptions[T]) *Router[T] {
	if opts.Router != nil {
		return opts.Router
	}
	return r.Network.Router
}

func (r *NetworkRun[T]) lastResult() *AgentResult {
	if len(r.State.results) == 0 {
		return nil
	}
	return r.State.results[len(r.State.results)-1]
}

// buildUserMessageRecord shapes the user message for persistence. When the
// input is a plain string, the id is minted inside a durable step — the TS
// version calls randomUUID inline, which re-mints on every replay.
func (r *NetworkRun[T]) buildUserMessageRecord(ctx context.Context, input string, um *UserMessage, step durable.Step) (UserMessageRecord, error) {
	if um != nil && um.ID != "" {
		ts := jsonutil.Now()
		if um.ClientTimestamp != "" {
			var t jsonutil.Time
			if err := t.UnmarshalJSON([]byte(`"` + um.ClientTimestamp + `"`)); err == nil {
				ts = t
			}
		}
		return UserMessageRecord{ID: um.ID, Content: um.Content, Role: RoleUser, Timestamp: ts}, nil
	}
	id, err := durable.Run(ctx, step, "generate-user-message-id", func(ctx context.Context) (string, error) {
		return uuid.NewString(), nil
	})
	if err != nil {
		return UserMessageRecord{}, err
	}
	return UserMessageRecord{ID: id, Content: input, Role: RoleUser, Timestamp: jsonutil.Now()}, nil
}

// getNextAgents resolves the router's verdict into agents to schedule.
// Returning an empty slice means the run is done.
func (r *NetworkRun[T]) getNextAgents(ctx context.Context, inputContent string, um *UserMessage, router *Router[T], step durable.Step) ([]*Agent[T], error) {
	if router == nil {
		if r.DefaultModel == nil {
			return nil, fmt.Errorf("agentkit: no router or default model defined in network %q — pass a router or a default model to use the built-in agentic router", r.Name)
		}
		router = &Router[T]{Agent: defaultRoutingAgent[T]()}
	}

	if router.Agent != nil {
		return r.getNextAgentsViaRoutingAgent(ctx, router.Agent, inputContent, step)
	}

	stack := make([]*Agent[T], 0, len(r.stack))
	for _, name := range r.stack {
		a, ok := r.AgentByName(name)
		if !ok {
			return nil, fmt.Errorf("agentkit: unknown agent in the network stack: %s", name)
		}
		stack = append(stack, a)
	}

	res, err := router.Fn(ctx, RouterArgs[T]{
		Input:       inputContent,
		UserMessage: um,
		Network:     r,
		Stack:       stack,
		CallCount:   r.counter,
		LastResult:  r.lastResult(),
		Stream:      r.structuredStream(""),
	})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	if res.Routing != nil {
		return r.getNextAgentsViaRoutingAgent(ctx, res.Routing, inputContent, step)
	}

	// Router-introduced agents register on the run so scheduling and
	// lookups can find them (the template stays immutable).
	for _, a := range res.Agents {
		if _, known := r.AgentByName(a.Name); !known {
			r.extraAgents[a.Name] = a
		}
	}
	return res.Agents, nil
}

// getNextAgentsViaRoutingAgent runs the routing agent and interprets its
// OnRoute verdict.
func (r *NetworkRun[T]) getNextAgentsViaRoutingAgent(ctx context.Context, ra *RoutingAgent[T], input string, step durable.Step) ([]*Agent[T], error) {
	result, err := ra.Run(ctx, input, &RunOptions[T]{Network: r, Step: step})
	if err != nil {
		return nil, err
	}

	names := ra.OnRoute(ctx, LifecycleResult[T]{
		LifecycleBase: LifecycleBase[T]{Agent: &ra.Agent, Network: r},
		Result:        result,
	})

	var agents []*Agent[T]
	for _, name := range names {
		if a, ok := r.AgentByName(name); ok {
			agents = append(agents, a)
		}
	}
	return agents, nil
}

// generateRunID mirrors the TS generateId format: <epochMillis>_<rand36>.
// Always minted inside a durable step, so the wall clock and randomness are
// memoized and replay-stable.
func generateRunID() string {
	const alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
	b := make([]byte, 9)
	for i := range b {
		b[i] = alphabet[rand.Intn(len(alphabet))]
	}
	return fmt.Sprintf("%d_%s", time.Now().UnixMilli(), string(b))
}

// --- default agentic router ---

// defaultRoutingAgent builds the built-in orchestrator: an inference call
// that either select_agent's the next worker or done's the run. Fresh per
// call — unlike the TS module-level singleton — because Go generics
// instantiate per T (construction is cheap; no model is bound until run).
func defaultRoutingAgent[T any]() *RoutingAgent[T] {
	selectAgent := NewTool[T]("select_agent",
		"Select an agent to handle the next step of the conversation",
		func(ctx context.Context, in struct {
			Name   string  `json:"name" jsonschema:"description=The name of the agent that should handle the request"`
			Reason *string `json:"reason" jsonschema:"description=Brief explanation of why this agent was chosen"`
		}, opts ToolOptions[T]) (any, error) {
			if in.Name == "" {
				return nil, fmt.Errorf("the routing agent requested an invalid agent")
			}
			agent, ok := opts.Network.AgentByName(in.Name)
			if !ok {
				return nil, fmt.Errorf("the routing agent requested an agent that doesn't exist: %s", in.Name)
			}
			// The name is returned so OnRoute can schedule it from the tool
			// call output.
			return agent.Name, nil
		},
		// Pure routing primitive (no side effect, no state mutation) — skip
		// the automatic durable-step wrap so routing doesn't spend a step
		// per call.
		WithManualStep[T]())

	done := NewTool[T]("done",
		"Signal that the conversation is complete and no more agents need to be called",
		func(ctx context.Context, in struct {
			Summary *string `json:"summary" jsonschema:"description=Brief summary of what was accomplished"`
		}, opts ToolOptions[T]) (any, error) {
			if in.Summary != nil && *in.Summary != "" {
				return *in.Summary, nil
			}
			return "Conversation completed successfully", nil
		},
		WithManualStep[T]())

	return NewRoutingAgent(AgentConfig[T]{
		Name:        "Default routing agent",
		Description: "Selects which agents to work on based off of the current prompt and input.",
		Tools:       []Tool[T]{selectAgent, done},
		ToolChoice:  "any", // choose between select_agent or done
		SystemFn: func(ctx context.Context, run *NetworkRun[T]) (string, error) {
			if run == nil {
				return "", fmt.Errorf("the routing agent can only be used within a network of agents")
			}
			agents, err := run.AvailableAgents(ctx)
			if err != nil {
				return "", err
			}
			var b strings.Builder
			for _, a := range agents {
				tools, err := jsonutil.MarshalString(toolDefsOf(a))
				if err != nil {
					return "", err
				}
				fmt.Fprintf(&b, `
    <agent>
      <name>%s</name>
      <description>%s</description>
      <tools>%s</tools>
    </agent>`, a.Name, a.Description, tools)
			}
			return fmt.Sprintf(`You are the orchestrator between a group of agents. Each agent is suited for specific tasks and has a name, description, and tools.

The following agents are available:
<agents>
  %s
</agents>

Your responsibilities:
1. Analyze the conversation history and current state
2. Determine if the request has been completed or if more work is needed
3. Either:
   - Call select_agent to route to the appropriate agent for the next step
   - Call done if the conversation is complete or the user's request has been fulfilled

<instructions>
  - If the user's request has been addressed and no further action is needed, call the done tool
  - If more work is needed, select the most appropriate agent based on their capabilities
  - Consider the context and history when making routing decisions
  - Be efficient - don't route to agents unnecessarily if the task is complete
</instructions>`, b.String()), nil
		},
	}, func(ctx context.Context, args LifecycleResult[T]) []string {
		if len(args.Result.ToolCalls) == 0 {
			return nil
		}
		call := args.Result.ToolCalls[0]
		if call.Tool == nil {
			return nil
		}
		switch call.Tool.Name {
		case "done":
			return nil // exit the agent loop
		case "select_agent":
			var payload struct {
				Data string `json:"data"`
			}
			if err := json.Unmarshal(call.Content.Raw(), &payload); err == nil && payload.Data != "" {
				return []string{payload.Data}
			}
		}
		return nil
	})
}

// toolDefsOf serializes an agent's tools for the router's system prompt.
type promptToolDef struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

func toolDefsOf[T any](a *Agent[T]) []promptToolDef {
	defs := make([]promptToolDef, 0, len(a.tools))
	for _, t := range a.tools {
		defs = append(defs, promptToolDef{Name: t.Name, Description: t.Description, Parameters: t.InputSchema})
	}
	return defs
}
