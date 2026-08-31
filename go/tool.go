package agentkit

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/inngest/inngestgo/step"
	"github.com/zendev-sh/goai"

	"github.com/eadwinCode/agent-kit/go/durable"
	"github.com/eadwinCode/agent-kit/go/internal/jsonutil"
)

// Tool is a capability an agent can invoke. T is the network's state data
// type.
//
// The handler is stored input-erased (raw JSON in, any out) so tools with
// different input types fit in one []Tool[T]; build tools with NewTool to
// keep typed inputs and a schema derived from the input struct.
type Tool[T any] struct {
	Name        string
	Description string

	// InputSchema is the JSON Schema for the tool's parameters. NewTool
	// derives it from the input struct via goai.SchemaFrom (json +
	// jsonschema struct tags). Nil means the tool takes no parameters.
	InputSchema json.RawMessage

	// ReplayPolicy declares whether AgentKit memoizes the complete tool result
	// and state patch or may execute the handler again during driver replay.
	// Empty means ReplayMemoized. Recompute policies are restricted to reviewed,
	// side-effect-free operations whose reread semantics are acceptable.
	ReplayPolicy ReplayPolicy

	// ManualStep opts this tool OUT of AgentKit's automatic durable-step
	// wrapping.
	//
	// By default the handler runs inside a single durable step under a
	// deterministic id, so its side effect executes EXACTLY ONCE across
	// Inngest replays (an unwrapped handler would re-fire — re-applying an
	// edit, re-billing an image generation). Set ManualStep when the
	// handler must not be wrapped, namely:
	//
	//   - it drives step tooling itself — step.WaitForEvent
	//     (human-in-the-loop), step.Invoke (sub-functions), or its own
	//     multi-checkpoint durable.Run loop (a long subagent) — wrapping it
	//     would nest steps, which Inngest forbids.
	//
	// Tools whose primary effect is mutating State.Data do NOT need this:
	// AgentKit re-applies their state delta across replays automatically.
	// ManualStep handlers must honor the durable package's control-flow
	// contract (no recover around steps, no steps from goroutines).
	ManualStep bool

	// Strict requests strict schema validation from providers that
	// support it.
	Strict bool

	// MCP records provenance when this tool was listed from an MCP server.
	MCP *MCPToolSource

	// Handler executes the tool. Input is the model-provided arguments as
	// raw JSON; the returned value is serialized as the tool result.
	Handler ToolHandler[T]
}

// ToolHandler is the erased handler signature stored on Tool.
type ToolHandler[T any] func(ctx context.Context, input json.RawMessage, opts ToolOptions[T]) (any, error)

// ToolOptions carries the execution context into a tool handler.
//
// Unlike TS — where a wrapped handler receives step: undefined and only
// ManualStep handlers get live step tools — Step is always non-nil here:
// durable.Step auto-collapses inside an existing step, so a wrapped handler
// calling durable.Run simply executes inline. ManualStep handlers use Step
// (and the inngestgo step package) to own their durability.
type ToolOptions[T any] struct {
	// Agent is the agent that invoked the tool.
	Agent *Agent[T]

	// Network is the current network run (its State field is the same
	// state exposed below).
	Network *NetworkRun[T]

	// State is the run's state; mutate Data freely (AgentKit snapshots and
	// re-applies it across replays for wrapped tools).
	State *State[T]

	// Step is the run's durability seam.
	Step durable.Step

	// Stream is the typed structured emitter for this tool call: semantic
	// status, domain data parts, progress, and the tool's own declared
	// safe boundaries (before_side_effect, after_side_effect,
	// between_items). It is never nil — a run without streaming supplies a
	// no-op — so handlers may call it unconditionally.
	//
	// Use Stream.Checkpoint to make a long or iterative tool pausable:
	// completed items stay checkpointed and are not repeated on resume,
	// and an atomic side effect that has already begun finishes before the
	// pause takes effect.
	Stream StructuredStream

	// Approvals runs the human-in-the-loop lifecycle for a tool that must
	// not act without a decision. It is nil when no ApprovalStore is
	// configured; a tool that requires approval MUST refuse to act in that
	// case rather than proceeding unapproved.
	Approvals *ApprovalController

	// ToolCallID is the provider's id for this call, used to correlate
	// approvals and transcript parts.
	ToolCallID string
}

// NewTool builds a typed tool: the input schema is generated from In's
// struct tags (json + jsonschema), and the model-provided JSON is
// unmarshaled into In before the handler runs. A decode failure is returned
// as the tool's error, which flows back to the model as the tool result —
// mirroring how zod validation failures behave in TS.
//
// Schemas are OpenAI-strict-mode style (goai.SchemaFrom): declaring a
// parameter as a POINTER field makes it OPTIONAL — the sanitizer resolves
// goai's nullable type array to the base type and omits the field from
// required, which is the shape strict providers (Gemini) accept. `omitempty`
// affects only JSON encoding, not the schema.
//
//	agentkit.NewTool[MyState]("set_sku", "Sets the SKU.",
//		func(ctx context.Context, in struct {
//			SKU int `json:"sku" jsonschema:"description=The SKU to set"`
//		}, opts agentkit.ToolOptions[MyState]) (any, error) {
//			opts.State.Data.SKU = in.SKU
//			return map[string]any{"ok": true}, nil
//		})
func NewTool[T, In any](name, description string, handler func(ctx context.Context, input In, opts ToolOptions[T]) (any, error), opts ...ToolOption[T]) Tool[T] {
	t := Tool[T]{
		Name:        name,
		Description: description,
		InputSchema: sanitizeInputSchema(json.RawMessage(goai.SchemaFrom[In]())),
		Handler: func(ctx context.Context, input json.RawMessage, topts ToolOptions[T]) (any, error) {
			var in In
			if len(input) > 0 {
				if err := json.Unmarshal(input, &in); err != nil {
					return nil, fmt.Errorf("invalid input for tool %q: %w", name, err)
				}
			}
			return handler(ctx, in, topts)
		},
	}
	for _, o := range opts {
		o(&t)
	}
	return t
}

// ToolDef is a Tool's model-facing projection: what the provider needs to
// advertise the tool, with no state type and no handler. The model layer
// (AgenticModel.Infer) takes []ToolDef so it stays independent of T; the
// agent maps its tools down with Def before inference.
type ToolDef struct {
	Name        string
	Description string
	InputSchema json.RawMessage
	Strict      bool
}

// Def returns the tool's model-facing projection.
func (t Tool[T]) Def() ToolDef {
	return ToolDef{
		Name:        t.Name,
		Description: t.Description,
		InputSchema: t.InputSchema,
		Strict:      t.Strict,
	}
}

// ToolOption customizes a NewTool-built tool.
type ToolOption[T any] func(*Tool[T])

// WithManualStep opts the tool out of automatic durable-step wrapping. See
// Tool.ManualStep.
func WithManualStep[T any]() ToolOption[T] {
	return func(t *Tool[T]) { t.ManualStep = true }
}

// WithReplayPolicy explicitly classifies the tool's replay behavior. Most
// tools should omit it and retain ReplayMemoized. It cannot be combined with
// WithManualStep because a manual tool owns its own durability boundaries.
func WithReplayPolicy[T any](policy ReplayPolicy) ToolOption[T] {
	return func(t *Tool[T]) { t.ReplayPolicy = policy }
}

// WithStrict requests strict schema validation.
func WithStrict[T any]() ToolOption[T] {
	return func(t *Tool[T]) { t.Strict = true }
}

// WithInputSchema overrides the generated schema with an explicit one.
func WithInputSchema[T any](schema json.RawMessage) ToolOption[T] {
	return func(t *Tool[T]) { t.InputSchema = schema }
}

// WithoutParameters marks the tool as taking no input (clears the schema).
func WithoutParameters[T any]() ToolOption[T] {
	return func(t *Tool[T]) { t.InputSchema = nil }
}

// ToolResultPayload mirrors the UI package shape for structured tool
// outputs: handler results are wrapped as {data: ...} when recorded.
type ToolResultPayload struct {
	Data any `json:"data"`
}

// MarshalToolResult serializes a handler's return through the parity
// encoder for recording in a ToolResultMessage.
func MarshalToolResult(v any) (json.RawMessage, error) {
	return jsonutil.Marshal(ToolResultPayload{Data: v})
}

// InngestFnTool exposes another Inngest function as a tool: the handler
// delegates via step.Invoke and waits for the result. It is ManualStep by
// contract — step.Invoke is itself a step operation and cannot run inside
// a step.run wrap; it is already durable on its own.
//
// functionID must include the app id prefix ("<appID>-<fnID>", the
// inngestgo InvokeOpts convention). Unlike TS — which derives the input
// schema from the function's event types — the schema is caller-supplied.
func InngestFnTool[T any](functionID, description string, inputSchema json.RawMessage) Tool[T] {
	return Tool[T]{
		Name:        functionID,
		Description: description,
		InputSchema: inputSchema,
		ManualStep:  true,
		Handler: func(ctx context.Context, input json.RawMessage, opts ToolOptions[T]) (any, error) {
			var data map[string]any
			if len(input) > 0 {
				if err := json.Unmarshal(input, &data); err != nil {
					return nil, fmt.Errorf("invalid input for inngest tool %q: %w", functionID, err)
				}
			}
			return step.Invoke[any](ctx, "invoke/"+functionID, step.InvokeOpts{
				FunctionId: functionID,
				Data:       data,
			})
		},
	}
}

// --- MCP configuration ---

// MCPTransportType selects how to reach an MCP server.
type MCPTransportType string

const (
	MCPTransportStreamableHTTP MCPTransportType = "streamable-http"
	MCPTransportSSE            MCPTransportType = "sse"
	MCPTransportStdio          MCPTransportType = "stdio"
	// MCPTransportWS exists for TS config parity but is NOT supported by
	// the goai/mcp client; connecting returns an error (known gap).
	MCPTransportWS MCPTransportType = "ws"
)

// MCPServer configures an MCP server whose tools an agent can use. Name
// namespaces the tools ("github" → "github-create_issue").
type MCPServer struct {
	Name      string
	Transport MCPTransport
}

// MCPTransport is the flattened transport config; fields apply per Type.
type MCPTransport struct {
	Type MCPTransportType

	// URL for streamable-http, sse and ws.
	URL string
	// Headers adds HTTP headers (auth) for http-based transports.
	Headers map[string]string

	// Command, Args and Env spawn a stdio server.
	Command string
	Args    []string
	Env     map[string]string
}

// MCPToolDef is the server-advertised tool definition.
type MCPToolDef struct {
	Name        string
	Description string
	InputSchema json.RawMessage
}

// MCPToolSource records which server and definition a Tool came from.
type MCPToolSource struct {
	Server MCPServer
	Tool   MCPToolDef
}
