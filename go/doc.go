// Package agentkit builds and orchestrates durable AI agents.
//
// It is a Go port of @inngest/agent-kit, running on goai
// (github.com/zendev-sh/goai) for inference and inngestgo for durable
// execution. An agent is a model plus tools and a system prompt; a network
// calls agents in a loop until a router says stop. Everything with a side
// effect — inference, tool calls, history writes, stream publishes — runs
// inside a memoized step, so an interrupted run resumes without repeating
// work.
//
// # Getting started
//
//	type state struct {
//		SKU int `json:"sku"`
//	}
//
//	setSKU := agentkit.NewTool[state]("set_sku", "Record the SKU.",
//		func(ctx context.Context, in struct {
//			SKU int `json:"sku" jsonschema:"description=The SKU to record"`
//		}, opts agentkit.ToolOptions[state]) (any, error) {
//			opts.State.Data.SKU = in.SKU
//			return map[string]any{"ok": true}, nil
//		})
//
//	assistant := agentkit.NewAgent(agentkit.AgentConfig[state]{
//		Name:   "assistant",
//		System: "You are a concise assistant.",
//		Tools:  []agentkit.Tool[state]{setSKU},
//		Model:  anthropic.Chat("claude-sonnet-4-5"),
//	})
//
//	result, err := assistant.Run(ctx, "Record SKU 42.", nil)
//
// Outside an Inngest function this executes inline; inside one, every step
// is memoized. The same code works in both places — see Durability.
//
// # Networks and routing
//
// A [Network] calls agents in a loop. The router decides what runs next and
// is the iteration authority: each agent call performs exactly one
// inference, so a tool round means routing back to the same agent rather
// than letting it loop internally.
//
//	net := agentkit.NewNetwork(agentkit.NetworkConfig[state]{
//		Name:         "support",
//		Agents:       []*agentkit.Agent[state]{assistant},
//		DefaultModel: model,
//		MaxIter:      5,
//		Router: &agentkit.Router[state]{
//			Fn: func(ctx context.Context, args agentkit.RouterArgs[state]) (*agentkit.RouterResult[state], error) {
//				if args.CallCount == 0 {
//					return agentkit.RouteTo(assistant), nil
//				}
//				// Keep going while the last turn called tools.
//				if last := args.LastResult; last != nil && len(last.ToolCalls) > 0 {
//					return agentkit.RouteTo(assistant), nil
//				}
//				return nil, nil // done
//			},
//		},
//	})
//	run, err := net.Run(ctx, "Record SKU 42.", nil)
//
// Leave Router nil to use the built-in agentic router, which picks the next
// agent with an inference call (it needs DefaultModel). Use [RouteVia] to
// hand off to a [RoutingAgent] from inside a code router. [StopWhen] ends a
// run early at the safe boundary between inferences; it must be a pure
// function of run state so a replay stops at the same point.
//
// # Tools
//
// [NewTool] derives the input schema from In's struct tags. Schemas are
// OpenAI-strict-mode: every property is required, so make a parameter
// optional by declaring it a pointer. Handlers receive decoded input and
// [ToolOptions], which carries the agent, the run, its state, and the
// durability seam.
//
// A handler that mutates opts.State.Data is safe across replays: AgentKit
// memoizes the post-handler snapshot and re-applies it outside the step.
// Handler errors are captured and returned to the model as the tool result
// rather than failing the run, so a failed side effect is not retried.
//
// # Durability
//
// [durable.Step] is the seam. [durable.Inngest] works in all three
// contexts without caller-side detection: inside a function it opens a real
// memoized step; inside an existing step body it collapses to inline
// execution rather than opening an illegal nested step; outside Inngest it
// simply runs. Values round-trip through JSON on every path, so behaviour
// is identical in and out of Inngest.
//
// Two rules are absolute, because inngestgo suspends a function by
// panicking with an internal control value:
//
//   - Never wrap a step call in recover(). Not in a tool handler, not in a
//     lifecycle hook, not in a defer. Re-panic anything you did not throw.
//   - Never call a step from a goroutine you spawned.
//
// Tools that drive step tooling themselves — waiting for an event, invoking
// another function, or running their own multi-step loop — must set
// [WithManualStep], since AgentKit's automatic wrap would nest steps.
// Large idempotent reads may also opt out to keep their output out of the
// per-run step budget.
//
// # Streaming
//
// Set [StreamingConfig] on a run to emit the AgentKit event protocol:
// run lifecycle, part created/completed, and text, reasoning and tool
// deltas. Model text and opted-in reasoning are forwarded from GoAI's raw
// provider chunks before inference completes; simulated chunking is reserved
// for AgentKit-owned completed content such as tool arguments and outputs.
// Chunks carry monotonic sequence numbers shared across a network and its
// agents. StreamReasoning is off by default — thinking still runs on the
// model, it is simply not forwarded.
//
// Publishing is the caller's concern: hand [StreamingConfig.Publish] any
// function, and wrap it in [DurablePublish] to make delivery exactly-once
// across replays.
//
// # Persistence
//
// [HistoryConfig] is the storage seam. Set it on a network or agent and
// AgentKit creates the thread, records the user's turn before the run,
// hydrates prior context, and saves results as they land — each inside its
// own durable step, with the end-of-run save idempotent against the
// incremental ones.
//
// Note that an [AgentResult] carries assistant output and tool results but
// never the user turn that prompted it. Storing only results leaves a model
// reading its own answers with the questions missing; record user turns
// too, and return them from Get.
//
// # Wire compatibility
//
// Message and AgentResult JSON is byte-identical to the TypeScript
// package's, and AgentResult.Checksum reproduces its xxhash output, so both
// implementations can share a database and streaming clients during a
// migration. Content the framework does not interpret — tool inputs and
// results — is carried as raw JSON end to end so key order survives.
//
// # Concurrency
//
// Agents and networks are immutable after construction and safe to share; a
// run clones state into its own [NetworkRun]. A [State] belongs to one run
// and is not synchronized.
package agentkit
