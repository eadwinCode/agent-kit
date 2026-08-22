# AgentKit for Go

A Go implementation of [`@inngest/agent-kit`](../packages/agent-kit): build
and orchestrate AI agents whose work survives restarts.

An agent is a model plus tools and a system prompt. A network calls agents in
a loop until a router says stop. Every side effect — inference, tool calls,
history writes, stream publishes — runs inside a memoized step, so an
interrupted run resumes without repeating work or re-billing a tool call.

Built on [goai](https://goai.sh) for inference and
[inngestgo](https://github.com/inngest/inngestgo) for durable execution.

```bash
go get github.com/eadwinCode/agent-kit/go
```

Requires Go 1.22+. API reference: [`doc.go`](doc.go) — or `go doc github.com/eadwinCode/agent-kit/go`.

## Quick start

```go
package main

import (
	"context"
	"fmt"

	agentkit "github.com/eadwinCode/agent-kit/go"
	"github.com/zendev-sh/goai/provider/anthropic"
)

// Typed state, shared across a run and mutated by tools.
type state struct {
	City string `json:"city,omitempty"`
}

func main() {
	weather := agentkit.NewTool[state]("get_weather",
		"Get the current weather for a city.",
		func(ctx context.Context, in struct {
			City string `json:"city" jsonschema:"description=The city to check"`
		}, opts agentkit.ToolOptions[state]) (any, error) {
			opts.State.Data.City = in.City
			return map[string]any{"city": in.City, "temperature_c": 21}, nil
		})

	assistant := agentkit.NewAgent(agentkit.AgentConfig[state]{
		Name:   "assistant",
		System: "You are concise. Use get_weather when asked about weather.",
		Tools:  []agentkit.Tool[state]{weather},
		Model:  anthropic.Chat("claude-sonnet-4-5"), // reads ANTHROPIC_API_KEY
	})

	res, err := assistant.Run(context.Background(), "Weather in Tokyo?", nil)
	if err != nil {
		panic(err)
	}
	for _, m := range res.Output {
		if text, ok := m.Content.AsString(); ok {
			fmt.Println(text)
		}
	}
}
```

That runs inline. Nothing changes when you move it inside an Inngest
function — the same code becomes durable.

## Networks

A network calls agents in a loop and the router decides what runs next. The
network is the iteration authority: each agent call performs exactly **one**
inference, so a tool round means routing back to the same agent rather than
letting it loop internally. This keeps total inferences bounded by `MaxIter`
instead of `MaxIter²`.

```go
net := agentkit.NewNetwork(agentkit.NetworkConfig[state]{
	Name:         "support",
	Agents:       []*agentkit.Agent[state]{assistant},
	DefaultModel: model,
	MaxIter:      5,
	Router: &agentkit.Router[state]{
		Fn: func(ctx context.Context, args agentkit.RouterArgs[state]) (*agentkit.RouterResult[state], error) {
			if args.CallCount == 0 {
				return agentkit.RouteTo(assistant), nil
			}
			// Keep going while the last turn called tools.
			if last := args.LastResult; last != nil && len(last.ToolCalls) > 0 {
				return agentkit.RouteTo(assistant), nil
			}
			return nil, nil // done
		},
	},
})

run, err := net.Run(ctx, "Weather in Tokyo?", nil)
```

Leave `Router` nil to use the built-in agentic router, which picks the next
agent with an inference call (needs `DefaultModel`). `RouteVia` hands off to
a `RoutingAgent` from inside a code router. `StopWhen` ends a run early at
the safe boundary between inferences — it must be a pure function of run
state so a replay stops at the same point.

## Tools

Input schemas come from struct tags. They are **OpenAI strict mode**: every
property is required, so make a parameter optional by declaring it a
**pointer**. `omitempty` affects JSON encoding only, not the schema.

```go
type editInput struct {
	Path   string  `json:"path" jsonschema:"description=File to edit"`
	Reason *string `json:"reason"` // pointer ⇒ optional / nullable
}
```

A handler that mutates `opts.State.Data` is safe across replays: the
post-handler snapshot is memoized and re-applied outside the step. Handler
errors are captured and returned to the model as the tool result rather than
failing the run, so a failed side effect is not retried.

Tools that drive step tooling themselves — waiting for an event, invoking
another function, running their own multi-step loop — must opt out of the
automatic wrap with `WithManualStep`, because nesting steps is illegal:

```go
agentkit.NewTool[state]("wait_for_approval", "Pause for a human.",
	handler, agentkit.WithManualStep[state]())
```

## Durability

`durable.Step` is the seam, and `durable.Inngest()` is correct in all three
contexts with no caller-side detection:

| Context | Behaviour |
|---|---|
| Inside an Inngest function | real memoized step |
| Inside an existing step body | collapses to inline (nesting is illegal) |
| Outside Inngest | runs directly |

Values round-trip through JSON on every path, so behaviour is identical in
and out of Inngest — a value that cannot survive serialization fails in
tests rather than first in a production replay.

> [!IMPORTANT]
> inngestgo suspends a function by **panicking** with an internal control
> value. So: never wrap a step call in `recover()` — not in a tool handler,
> a lifecycle hook, or a defer — and never call a step from a goroutine you
> spawned. A swallowed control panic makes the function silently complete
> instead of suspending, which is painful to debug.

## Streaming

Set `StreamingConfig` on a run to emit the AgentKit event protocol: run
lifecycle, part created/completed, and text, reasoning and tool deltas, with
monotonic sequence numbers shared across a network and its agents. Model text
and opted-in reasoning are forwarded from GoAI's raw provider chunks before
inference completes; `SimulateChunking` applies only to AgentKit-owned completed
content such as tool arguments and outputs.

```go
run, err := net.Run(ctx, input, &agentkit.NetworkRunOptions[state]{
	Streaming: &agentkit.StreamingConfig{
		Publish:         agentkit.DurablePublish(step, myPublish),
		StreamReasoning: true, // off by default
	},
})
```

Publishing is yours to define — `DurablePublish` wraps any publisher so
delivery is exactly-once across replays. `StreamReasoning` is off by
default: thinking still runs on the model, it is simply not forwarded.

## Persistence

`HistoryConfig` is the storage seam. Set it and AgentKit creates the thread,
records the user's turn before the run, hydrates prior context, and saves
results as they land — each inside its own durable step, with the
end-of-run save idempotent against the incremental ones.

```go
net := agentkit.NewNetwork(agentkit.NetworkConfig[state]{
	// …
	History: &agentkit.HistoryConfig[state]{
		CreateThread:      createThread,
		Get:               loadResults,
		AppendUserMessage: saveUserTurn,
		AppendResults:     saveResults,
	},
})
```

> [!NOTE]
> An `AgentResult` carries assistant output and tool results but **never the
> user turn that prompted it**. Storing only results leaves the model reading
> its own answers with the questions missing. Record user turns too and
> return them from `Get`. See `examples/go-chat/server/store.go` for a
> SQLite implementation.

## Runtime ports

`HistoryConfig` answers "where do messages live". The runtime ports answer the
questions that only matter once a run outlives the browser tab that started
it: what happens when the socket drops mid-turn, when a second tab joins, when
the user hits Pause, when a tool needs a human decision, and when the
application still has billing and cleanup to settle before anyone should see a
terminal.

Every port is an interface AgentKit publishes and invokes; the application
supplies an implementation backed by its own database, workflow engine and
policy. Each one is independently optional — a nil field just disables that
capability.

| Port               | AgentKit's lifecycle role                                                                            |
| ------------------ | ----------------------------------------------------------------------------------------------------- |
| `EventJournal`     | Every standard envelope is appended **before** live fan-out, so a reconnecting client can replay it.  |
| `StateStore`       | Versioned session state with compare-and-swap, so concurrent commands serialize instead of clobbering. |
| `ControlStore`     | Pause/resume/cancel intent, consulted at every safe boundary.                                          |
| `ApprovalStore`    | Issue → wait → resolve once → consume once, replay-safe at each step.                                  |
| `StreamSink`       | Outbound delivery, when the application owns transport and backpressure.                               |
| `Finalizer`        | Holds the terminal until the application's durable facts have settled.                                 |

```go
ports := &agentkit.RuntimePorts{
	Journal:   myJournal,   // agentkit.EventJournal
	State:     myState,     // agentkit.StateStore
	Control:   myControl,   // agentkit.ControlStore
	Approvals: myApprovals, // agentkit.ApprovalStore
	Finalizer: myFinalizer, // agentkit.Finalizer
	// Scope is an OPAQUE owner token. AgentKit never parses it and assumes
	// no tenancy model: compose whatever identifies the conversation owner
	// in yours — a composite key, a UUID, an opaque handle. Structured
	// context an adapter needs travels in context.Context, which every port
	// method already receives.
	Scope: agentkit.SessionScope(ownerKey),
	// Bump on a resumed or restarted run so clients discard a stale tail.
	StreamEpoch: epoch,
}

run, err := net.Run(ctx, input, &agentkit.NetworkRunOptions[state]{
	Ports:     ports,
	Streaming: &agentkit.StreamingConfig{Publish: publish},
})
```

Pause takes effect at the next **safe boundary** — after the active inference
records its complete result, before a returned tool executes, or between
network iterations. A provider request cannot be frozen mid-token, so v1 never
tries; a tool that has begun an atomic side effect finishes it first. Tools
declare their own boundaries:

```go
tool := agentkit.NewTool[state]("publish", "Publish the site.",
	func(ctx context.Context, in struct{}, opts agentkit.ToolOptions[state]) (any, error) {
		opts.Stream.Status(ctx, agentkit.StatusUpdate{
			Kind: agentkit.ActivityWriting, Label: "Publishing",
		})
		// A pause accepted here costs nothing: nothing irreversible has run.
		if err := opts.Stream.Checkpoint(ctx, agentkit.CheckpointBeforeSideEffect); err != nil {
			return nil, err
		}
		if _, err := opts.Approvals.Require(ctx, agentkit.ApprovalRequest{
			RequestID: "approval_" + opts.ToolCallID,
			ToolName:  "publish",
			Summary:   "Publish the site",
		}); err != nil {
			return nil, err // denied, expired, or already consumed
		}
		return publishSite(ctx)
	})
```

Testing an adapter: [`go/conformance`](./conformance) exports a suite for each
port, and [`go/memadapter`](./memadapter) is an in-memory reference that runs
it.

```go
func TestMyJournal(t *testing.T) {
	conformance.VerifyEventJournal(t, func() agentkit.EventJournal {
		return newMyJournal(t)
	})
}
```

## Serving on Inngest

```go
handler, err := agentkit.NewServer("my-app", func(c inngestgo.Client) error {
	_, err := agentkit.RegisterNetwork(c, net)
	return err
})
http.ListenAndServe(":3000", handler)
```

Or register onto an existing client with `RegisterAgent` / `RegisterNetwork`.

## Notes for TypeScript users

Behaviour matches the TS package, including quirks worth knowing. Message
and `AgentResult` JSON is **byte-identical** and checksums match, so both
implementations can share a database and streaming clients during a
migration.

The API differs where Go and goai demand it:

| Area | TypeScript | Go |
|---|---|---|
| Tool schemas | Zod, inferred handler args | struct tags; optional ⇒ pointer |
| Step tooling | `step` threaded through call sites | `durable.Step`, auto-collapsing |
| Tool opt-out | `manualStep: true` | `WithManualStep()` |
| Model settings | `wrapLanguageModel` + middleware | `WithCallOptions(goai.…)` |
| State access | `state.data` via Proxy | plain typed `State.Data` |

Provider details handled for you, listed because they surprise people:
the system prompt is passed via goai's `WithSystem` (its Anthropic provider
drops `RoleSystem` messages); Anthropic reasoning signatures are read from
provider metadata, since non-streaming responses omit them from the message
list; and `input_tokens` needs no cache-subtraction because goai already
reports it cache-exclusive.

Extended thinking is configured through goai, with AI-SDK camelCase keys —
and max output tokens must exceed the thinking budget:

```go
agentkit.WithCallOptions(
	goai.WithMaxOutputTokens(4096),
	goai.WithProviderOptions(map[string]any{
		"thinking": map[string]any{"type": "enabled", "budgetTokens": 1024},
	}),
)
```

## Status

All framework modules are ported with tests, including golden-file JSON
parity fixtures generated from the TypeScript package. Durable replay is
verified end-to-end: a single turn drove 47 function invocations while each
tool executed exactly once.

Known gaps: MCP supports stdio, streamable-HTTP and SSE — not websocket,
which goai's client does not implement. `State.kv` is deprecated in TS and
not ported. goai is pre-1.0 and pinned exactly.

## Example

[`examples/go-chat`](../examples/go-chat) is a full application: a Go server
with tools, streaming and SQLite persistence, and a React frontend on the
real `@inngest/use-agent` hook rendering thoughts, tool calls and token
usage live.
