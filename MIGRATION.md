# Migration: consuming the Vercel-AI-SDK (v6) fork of `@inngest/agent-kit`

This fork (`@inngest/agent-kit@0.13.3-alpha.1`, branch `task/ai-sdk-refactor`) has
moved off `@inngest/ai` onto the **Vercel AI SDK v6** (`ai@^6`). Inference runs
`generateText()` inside `step.run()`, and `src/converters.ts` translates between
agent-kit `Message[]` and the AI SDK `ModelMessage[]` / `Tool` / `ToolResultOutput`
types.

It folds in everything the downstream **Clevix Studio** project carried as a
hand-applied bun patch (`patches/@inngest%2Fagent-kit@0.13.2.patch`), so Clevix can
consume the fork directly and **delete the patch**.

> **Already on v6.** A previous cut of this fork targeted `ai@4`
> (`LanguageModelV1`), which forced consumers onto the v4-compatible provider
> majors. This cut targets **`ai@6` / `LanguageModelV3`**, matching the versions
> Clevix already resolves (`ai@6`, `@ai-sdk/anthropic@3`, `@ai-sdk/openai@3`) — so
> there is **no AI SDK version pin to manage** anymore.

---

## 1. The model API changed (required)

Old (`@inngest/ai`): you built a provider adapter with `anthropic()` / `openai()`
and passed it as `model`.

New: you build a Vercel **`LanguageModel`** (an `@ai-sdk/*` model instance) and pass
it as `model`. The agent wraps it internally via
`createAgenticModelFromLanguageModel(model)`.

### Versions

- `ai@^6`, `@ai-sdk/anthropic@^3`, `@ai-sdk/openai@^3` — exactly what Clevix
  already has. No pinning, no dedupe needed.

### Before → after (Clevix `packages/ai/src/agentkit/model.ts`)

```ts
// BEFORE — @inngest/ai adapter; thinking/temperature/max_tokens via defaultParameters
import { anthropic, openai } from "@inngest/agent-kit";

return anthropic({
  model: config.model,
  defaultParameters: {
    max_tokens: 16384,
    thinking: { type: "adaptive" },
    effort: "high",
  },
});
```

```ts
// AFTER — Vercel LanguageModel; per-request settings baked in with middleware
import { createAnthropic } from "@ai-sdk/anthropic";
import { createOpenAI } from "@ai-sdk/openai";
import { wrapLanguageModel, defaultSettingsMiddleware } from "ai";

const anthropic = createAnthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

// max_tokens / temperature / thinking are now CALL settings (not constructor
// params). Bake them into the model instance with defaultSettingsMiddleware so
// every inference the agent runs uses them.
const model = wrapLanguageModel({
  model: anthropic(config.model),
  middleware: defaultSettingsMiddleware({
    settings: {
      maxOutputTokens: 16384, // NB: renamed from maxTokens in v5+
      // Anthropic extended thinking via provider options:
      providerOptions: {
        anthropic: { thinking: { type: "enabled", budgetTokens: 8000 } },
      },
      // temperature: config.temperature,   // omit when thinking is on
    },
  }),
});

// then, unchanged:
createAgent({ name: "clevix-designer", system, model, tools });
```

Notes:
- `wrapLanguageModel` preserves the wrapped model's `provider`/`modelId`, so the
  automatic Anthropic cache detection (below) still fires through the wrap.
- OpenAI reasoning effort → `settings.providerOptions.openai.reasoningEffort`.
- Anthropic **requires** a max-tokens value; if you don't set `maxOutputTokens` the
  provider falls back to its own default. Set it explicitly to keep 8k/16k.

`createAgenticModelFromLanguageModel(model, options?)` also accepts
`{ cacheControl?: boolean | "auto" }` if you construct the agentic model directly
(see §5).

---

## 2. `infer()` now carries usage, cache tokens, and reasoning (fixes zero-billing)

`AgentResult.raw` is a JSON string of a `SerializableResult`. It now includes a
`usage` block (previously absent — which is why billing read **zero**):

```jsonc
{
  "text": "...",
  "toolCalls": [...],
  "finishReason": "stop",
  "usage": {
    "input_tokens": 50,             // NON-cache prompt tokens
    "output_tokens": 12,
    "total_tokens": 892,            // provider total (includes cache)
    "cache_creation_input_tokens": 30,    // Anthropic, when caching is active
    "cache_read_input_tokens": 800
  },
  "reasoning": "…thinking text…",        // present only when the model reasons
  "reasoningDetails": [ { "type": "text", "text": "…", "signature": "…" } ]
}
```

**Billing works unchanged.** Clevix's `parseUsageFromRaw` already reads exactly
these snake_case keys and *adds* the cache buckets onto the input count:

```ts
const inputTokens =
  (u.input_tokens ?? u.prompt_tokens ?? 0) +
  (u.cache_read_input_tokens ?? 0) +
  (u.cache_creation_input_tokens ?? 0);
const outputTokens = u.output_tokens ?? u.completion_tokens ?? 0;
```

⚠️ **v6 usage subtlety (handled in the fork):** the AI SDK v6 `usage.inputTokens`
is the **cache-inclusive total**. To keep Clevix's "add the buckets" math correct,
the fork maps `input_tokens` from the **non-cache** breakdown
(`usage.inputTokenDetails.noCacheTokens`), not the total — so cache tokens are never
double-counted. No change to `billing.ts` is required. (`total_tokens` is the
cache-inclusive grand total and is informational; Clevix doesn't use it.)

---

## 3. Reasoning is a first-class message (render it; it round-trips)

`AgentResult.output` may now contain a `reasoning` message:

```ts
{ type: "reasoning", role: "assistant", content: "…", signature?: "…",
  details?: [ { type: "text", text, signature? } | { type: "redacted", data } ] }
```

- It is emitted **before** the text / tool-call messages within a turn. Anthropic
  requires the thinking block to precede the tool-use block, and the converter
  replays it (the signature travels in `providerOptions.anthropic.signature`) so
  multi-step tool use with extended thinking keeps working.
- Clevix's `history-prune.ts` already passes non-`tool_call` messages through
  untouched, so persistence/replay is unaffected. Render the `reasoning` message
  in the UI if you want to surface chain-of-thought.

This replaces the patch's "skip unhandled block types" crash fix — there is no
longer a hand-rolled response parser to crash on thinking blocks.

---

## 4. Vision / images in tool results (and user messages)

`messagesToCoreMessages` converts image content into AI SDK v6 parts:

- **User messages**: an image content part
  `{ type: "image", image: <url|base64|dataURL>, mimeType? }` becomes an AI SDK
  `ImagePart` (`mimeType` is mapped to v6's `mediaType`).
- **Tool results**: when `ToolResultMessage.content` is an array of blocks and at
  least one is an image, the converter emits a v6 `{ type: "content" }` tool-result
  output with `image-data` (base64) or `image-url` parts so a vision model can see
  it. It accepts the **Anthropic-native** shape your `imageToolResultContent()`
  already emits — `{ type: "image", source: { type: "base64"|"url", … } }` — as
  well as the AI SDK `{ type: "image", data, mediaType }` shape. (v6 supports image
  URLs in tool results directly, so URL images are no longer downgraded to text.)

So `vision.ts` / `imageToolResultContent()` keep working as-is; the bun patch's
"forward array content verbatim" hack is no longer needed.

---

## 5. Anthropic prompt caching (replaces the patch's cache_control edits)

Caching is **auto-enabled for Anthropic models** (detected from the model's
`provider` id, e.g. `"anthropic.messages"`) and **off for everyone else** — safe to
leave on:

- The **system message** is marked with `providerOptions.anthropic.cacheControl =
  { type: "ephemeral" }`. Anthropic orders a request as `[tools, system, messages]`,
  so this caches the tool definitions + system prompt prefix.
- The **last tool** is *also* marked. Unlike the old `ai@4` cut, **v6 tool
  definitions carry `providerOptions` through to the provider**, so this is a real
  second cache breakpoint (the static tool block caches independently of the
  per-project system tail).

Both breakpoints are verified end-to-end against the real `@ai-sdk/anthropic`
provider (`src/__tests__/anthropic-integration.test.ts`): the request body carries
`cache_control` on the system block and the last tool. Cache read/creation tokens
flow back through §2's `usage`.

Override if needed:

```ts
createAgenticModelFromLanguageModel(model, { cacheControl: true });   // force on
createAgenticModelFromLanguageModel(model, { cacheControl: false });  // force off
// default / "auto": on for Anthropic, off otherwise
```

---

## 6. OpenAI optional params (no forced strict mode)

The old code forced `strict: Boolean(parameters)`, which 400'd
(`invalid_function_parameters`) on tools with optional params (e.g.
`read_file.limit`). The Vercel path does **not** force strict mode: optional Zod
fields stay out of the JSON-schema `required` list and are sent as genuinely
optional. The patch's `strict: false` default is no longer needed.

---

## 7. Durability (`step.ai.infer` is intentionally not used)

`infer()` wraps the `generateText()` call in `step.run(stepID, …)`, so inference is
durable and replay-safe. `step.ai.infer` (from `@inngest/ai`) was removed and is
**not** reintroduced: it requires the provider-specific raw request/response shape
this fork no longer builds, and Clevix sets `retries: 0` and bills from the result
either way. No behavioral regression for Clevix.

---

## 8. Step budget: streaming publishes & iteration caps

A durable Inngest function has a **1000-step limit**. Two things in the agentic
loop consume steps; both are now bounded/configurable.

### Streaming publishes (each `publish()` is a durable step)

`@inngest/realtime`'s function-scoped `publish` wraps every call in a `step.run`
(you'll see `publish:<channel>:*` steps). AgentKit calls your `streaming.publish`
once per streaming event, so **publish volume = step cost**. Previously, with
`simulateChunking: true`, each inference's text was split into hardcoded ~50-char
`text.delta`s — a long turn emitted ~900 publish-steps and overflowed the limit.

Chunking is now configurable on the streaming options:

```ts
await network.run(msg, {
  state,
  streaming: {
    publish,
    simulateChunking: true,   // keep the typing animation…
    chunkSize: 256,           // …but ~5× fewer publishes than the old 50 (default 256)
    maxChunksPerMessage: 24,  // hard cap per part — bounds publishes regardless of
                              // output length (default 24; 0 = unlimited)
  },
});
```

- `chunkSize` (default **256**) — characters per `*.delta`. Bigger → fewer steps.
- `maxChunksPerMessage` (default **24**) — a part longer than
  `chunkSize × maxChunksPerMessage` is split into at most this many (coarser)
  chunks, so one part never emits more than N delta-steps.
- `simulateChunking: false` → **one** delta per part (minimum: `part.created` +
  one delta + `part.completed` ≈ 3 steps/part). The event shapes are unchanged in
  every mode — only the number of `*.delta` events differs.

Rough cost: `publishes ≈ Σ_parts (min(⌈len/chunkSize⌉, maxChunksPerMessage) + 2)`.
The defaults keep a typical multi-inference turn well under the step budget while
preserving incremental streaming.

**Removing streaming from the step budget entirely (recommended for long turns).**
Because these are fire-and-forget UI updates and the consumer runs with
`retries: 0`, durability buys little. If you pass a `publish` that does **not**
create steps — i.e. publish outside Inngest's durable graph rather than the
function-scoped `publish` from `realtimeMiddleware()` — streaming costs **zero**
steps regardless of chunking. AgentKit just calls whatever `publish` you give it;
it never wraps publishes in steps itself. (With a non-durable publish you can turn
`simulateChunking` back on for a smooth animation at no step cost.)

### Iteration caps (no `maxIter²`)

The network is the single iteration authority: it calls each agent once per loop
and the router decides whether to continue, so the agent does **one inference per
call** (`maxIterPerRun: 1`). Total inferences per `network.run` is therefore
**≤ `network.maxIter`**, never `maxIter²` — the network does **not** pass its
`maxIter` into the agent's internal tool-round loop.

`Agent` now exposes its own internal cap, decoupled from any network:

- `maxIterPerRun` (constructor or `agent.run(...)` option, default **1**) bounds
  inferences within a single `run()`. Raise it to let a **standalone** agent loop
  on its own tool calls without a network; it never affects the network bound.
- The legacy `agent.run({ maxIter })` option still works for standalone runs
  (back-compat) but is decoupled from `network.maxIter` and deprecated in favor of
  `maxIterPerRun`.

> Note: a prior dist led some consumers to believe the fork ran `maxIter²`
> inferences. It does not — the network passes `maxIterPerRun: 1` to the agent.
> You can size `network.maxIter` purely as the per-turn inference ceiling.

## 9. Durable persistence & deterministic step ids (Inngest replay safety)

Inngest re-executes the function body across step boundaries; completed `step.run`s
are returned from memory **by id**, so every step id must be identical on every
re-execution. A step id derived from `AgentResult.checksum` is **not** stable —
the checksum embeds `createdAt`, which is regenerated on each replay — and Inngest
fails with `Could not find step "<hash>" to run; timed out (foundSteps: [])`.

### Results are persisted incrementally (survives mid-run failure)

`network.run` now calls `history.appendResults` **after each agent result is
produced**, not only once at the end of the loop. So a mid-run failure — a thrown
error, or a hard abort (cancel via `cancelOn`, step-overflow, timeout/OOM) — leaves
**every completed inference on disk**. On reload the user sees the partial answer
they watched stream in, not just their own message.

Each incremental save is wrapped by the fork in a single `step.run` under a
**deterministic id** — the network iteration counter
(`agent-kit/history/append-results/<n>`), never a checksum/timestamp/UUID. Two
consequences for `HistoryConfig.appendResults`:

- It is invoked with **`step: undefined`** — the fork already owns the durable
  boundary, so the hook body runs inline (opening a nested `step.run` inside it
  would break Inngest). Do your DB write directly; do **not** wrap it in
  `step.run` and do **not** key anything on `result.checksum`.
- It may be called **more than once** for the same result (incrementally, then the
  end-of-run backstop, or on retry). Keep it **idempotent** (e.g. upsert / dedupe
  by a stable key). `checksum` is fine as a *content* dedupe key in your DB; just
  never use it as a *step* id.

Consumers can now delete their own rescue workarounds (a parent-side `catch` that
re-saves `state.results`, or an `onFinish` that persists per result) — the fork
covers the thrown-error case durably and the hard-abort case via the incremental
saves already on disk.

### Tool execution is durable by default (NEW in `0.13.3-alpha.4`)

> **Reverses the previous guidance in this section.** Earlier alphas ran every
> tool handler **inline** and told you to wrap your own side effects. The fork
> now wraps each tool call for you. If you added per-tool `step.run` wrapping in
> your adapter (or a `task`-tool wrap), **delete it** — see the migration below.

**The bug.** Inngest re-executes the function body on every step boundary;
completed steps replay from memory while inline code re-runs. A tool handler that
ran **inline** therefore re-fired its side effect **once per replay**:

- `edit_file` applied the edit on the first replay, then later replays failed
  "string not found" (the original text was already gone) — and that **failed**
  result was handed to the model, which then "saw" the edit as un-applied.
- `mv` / `rm` / `cp` succeeded once, then errored ("source not found").
- paid image/video tools **re-billed** and produced non-deterministic output on
  each replay.

(Idempotent reads — `read_file`, `grep`, `screenshot` — re-firing was harmless,
which is why this hid for so long.)

**The fix.** When a tool runs inside Inngest, AgentKit now wraps the handler in
**one durable `step.run`** under a deterministic, replay-stable id
(`<agent>/tool/<name>/<n>`, where `<n>` is a per-run counter in tool-call order —
never a checksum/timestamp/uuid). The side effect runs **exactly once**; every
replay returns the memoized result. In an Inngest trace each tool call is now a
single `tool` step span; a single-use tool stays at `…/0` across dozens of
replays.

Two things AgentKit handles **for you** so the wrap is transparent:

- **State mutations are preserved.** A tool whose primary effect is mutating
  `network.state.data` (design-questions, copy-suggestions, plan snapshots,
  `__stopReason`) keeps working with **no change**. The mutation happens inside
  the memoized step, so AgentKit snapshots the post-handler state into the step
  output and **re-applies it outside the step on every execution** (including
  replays, where the body is skipped). Your state delta is never lost to
  memoization. (Corollary: `state.data` you mutate from a tool must be
  JSON-serializable — it already had to be, to persist.)
- **In-tool live streaming still fires once.** Progress you `publish()` from
  inside a handler via `@inngest/realtime` still reaches the client exactly once.
  The realtime middleware publishes un-stepped while executing inside a step
  (`store.executingStep ? action() : step.run(...)`), so in-tool publishes fire
  during the real execution and do **not** re-fire on replay (the memoized body
  doesn't run). Bonus: they no longer each cost their own step.

#### Opt out with `manualStep: true` when the handler drives its own steps

A wrapped handler is a **leaf**: it receives `opts.step: undefined` (it is already
inside a step; a nested `step.run` would break Inngest). Set `manualStep: true` to
**skip the wrap** — the handler then runs inline with the **live** `opts.step` and
owns its own durability. Use it when the handler:

- uses step tooling itself — `step.waitForEvent` (HITL), `step.invoke`
  (sub-functions), or its own `step.run` checkpoints; **or**
- runs **long / multi-call** work (a subagent loop). A single `step.run` must
  finish within one request's execution window, so multi-minute work must
  checkpoint itself across requests, exactly as before; **or**
- is an idempotent **large-output read** (e.g. a base64 screenshot) you don't
  want occupying step state — see the budget note below.

```ts
createTool({
  name: "task",
  parameters: z.object({ goal: z.string() }),
  manualStep: true, // ← this handler opens its own steps; don't wrap it
  handler: async ({ goal }, { step }) => {
    let i = 0;
    for (const sub of plan(goal)) {
      await step!.run(`task/subcall/${i++}`, () => generateText({ /* … */ }));
    }
    // ✅ or hand the whole subagent off to its own Inngest function:
    // return step!.invoke("task/subagent", { function: subagentFn, data: { goal } });
  },
});
```

```ts
// ❌ a long multi-LLM-call loop WITHOUT manualStep is now wrapped in ONE step →
//    it runs inline within that single step and blocks the request past its
//    timeout ("Could not find step …; foundSteps: []"). Set manualStep: true.
createTool({ name: "task", handler: async ({ goal }) => runSubagentLoopInline(goal) });
```

AgentKit's built-in tools are already handled: `select_agent` / `done` (pure
routing primitives) and Inngest-function tools (`createTool` from an
`InngestFunction`, which call `step.invoke`) are marked `manualStep` internally;
**MCP tools are wrapped** by the framework (the old self-wrap keyed the step on
the tool name alone and collided when one MCP tool was called twice).

#### Migration: delete your manual tool/`task` step-wrapping

If your consuming app wrapped tool work itself — the common adapter pattern

```ts
// BEFORE (consumer adapter): a per-tool / task-tool durable wrap
let invocation = 0;
handler: async (input, { step }) => {
  const id = `tool/${name}/${invocation++}`;       // or `task/${name}/${n}`
  const run = async () => doTheToolWork(input);
  return step ? await step.run(id, run) : await run();
};
```

then, on `0.13.3-alpha.4`:

- **Self-contained tools (most of them):** **delete the wrap** and return the
  work directly. AgentKit now provides the durable boundary and the deterministic
  id. (If you leave the wrap in place it still *works* — AgentKit passes such a
  handler `step: undefined`, so it takes the `await run()` branch inside
  AgentKit's own step — but it's dead code; remove it.)
  ```ts
  // AFTER
  handler: async (input) => doTheToolWork(input),
  ```
- **`task` / subagent tools and HITL tools** (anything that calls
  `step.run` / `step.invoke` / `step.waitForEvent` itself): add
  `manualStep: true` and **keep** your `step.run` checkpoints. They now run inline
  with the live step, uncontested by an outer wrap.
- You can also drop any adapter-side `step` plumbing that existed only to feed
  that wrap.

Step ids you create inside a `manualStep` handler follow the same rule as
everywhere else: derive them from a **deterministic counter / index**, never from
`Date.now()`, a random id, a UUID, or `AgentResult.checksum`.

#### Step budget (the 4MB / 1000-step limits)

Each wrapped tool call is **+1 step** (against Inngest's 1000-step/function cap)
and its result lands in step state (all step outputs **share a ~4MB-per-run**
budget). Tool counts are minor next to streaming publishes (see §8), but two
things to know:

- AgentKit only stores a state snapshot in the step output **when the tool
  actually mutated `state.data`**, so non-state tools stay lean.
- A large base64 **screenshot/image read** stored in step state can eat a big
  slice of the 4MB run total. Mark such idempotent reads `manualStep: true`:
  re-running a read on replay is only wasted latency (not a correctness or
  billing bug), and its output never occupies step state.

## 10. Ending a run early with `stopWhen` (budget / iteration caps)

The router is the only place to stop a run, and it runs **between** inferences —
fine, because that's the only *safe* stop point (every persisted `AgentResult` is
complete: `output` + all its `toolCalls`; aborting mid-inference would orphan a
tool-call and break replay/history). `stopWhen` formalizes that boundary and makes
a budget-stop **legible on the stream**, instead of looking like a normal finish.

```ts
createNetwork({
  /* … */
  stopWhen: ({ network, callCount, lastResult }) =>
    regulator.budgetReached ? { reason: "budget", metadata: { … } } : undefined,
});
// or per-run: network.run(msg, { state, streaming, stopWhen })
```

- Evaluated **before each inference**, decoupled from routing (it composes with
  and overrides the router). Return a `{ reason, metadata? }` to stop, or
  `undefined`/`false` to continue. It can also stop **before the first** inference
  (already over budget at turn start → 0 inferences).
- On stop the network emits **`run.interrupted` `{ reason }`** and then the normal
  terminal `run.completed` + `stream.ended` — both now carry `reason?: StopReason`
  so a subscriber that only watches the terminal still learns why. Normal finishes
  have no `reason`. `StopReason` is `"budget" | "max_tokens" | "timeout" |
  "user_cancellation" | "cancelled" | "other" | (string & {})`.
- **Replay rule:** `stopWhen` MUST be a pure function of `network.state` (rebuilt
  deterministically from memoized steps). Reading `Date.now()`/random would make a
  replay stop at a different point — the same hazard as a non-deterministic step id.
  Reading accumulated token usage off the results is deterministic and fine.
- **Back-compat:** purely additive; `router → undefined` consumers are unchanged.
  (Also fixes a latent double-fire of the network terminal on the unknown-agent
  path — there is now a single terminal emitter.)

### Long-running tools: pull, don't push

`stopWhen` ticks **between** inferences, so it can't bound a single tool that does
unbounded work inside its one `step.run` (e.g. a subagent doing many model calls,
or a tool generating dozens of images). And the framework **cannot** push an
`AbortSignal` into a running tool — the handler runs *inside* a `step.run`, so the
framework isn't executing alongside it to fire one (and on an Inngest `cancelOn`
hard-kill the function is torn down without the fork getting to abort anything).

So a long tool must **cooperatively check budget itself** — it already receives
`network`, so it reads your budget off `network.state` and winds down, returning a
**valid** result (a partial output, or an error result) so the turn stays
consistent:

```ts
createTool({
  name: "subagent",
  handler: async (input, { network }) => {
    const out = [];
    for (const unit of plan(input)) {
      if ((network.state.data as ClevixState).budgetReached) break; // wind down
      out.push(await doUnit(unit));
    }
    return out; // always a valid tool_result
  },
});
```

This is app-side by necessity, not by omission: core's contribution is already
made by handing the tool `network`. (If you pass `opts.step` through, also remember
a multi-call tool should wrap each unit in its own `step.run` — see §9.)

## 11. Consuming the fork & dropping the patch

1. **Remove the patch** from Clevix:
   - delete `patches/@inngest%2Fagent-kit@0.13.2.patch`
   - delete the `patchedDependencies` entry (and the `@inngest/agent-kit@0.13.2`
     pin) from `package.json`.

2. **Point at the fork.** Pick one:

   - **Install from GitHub (recommended).** The repo publishes a prebuilt
     `release` branch carrying the package's `dist/` at the repo root, so it
     installs directly — no subdir path, no build-on-install:
     ```jsonc
     // Clevix package.json
     "dependencies": {
       "@inngest/agent-kit": "github:eadwinCode/agent-kit#release"
     }
     ```
     ```bash
     pnpm add github:eadwinCode/agent-kit#release
     ```
     For a reproducible/pinned install, use the release commit SHA instead of the
     moving branch name: `github:eadwinCode/agent-kit#<sha>`.

     > Why a branch and not plain `github:eadwinCode/agent-kit`? The package lives
     > in the `packages/agent-kit` subdir and gitignores `dist/`, so a root install
     > would get the workspace root with no build output. The `release` branch is
     > regenerated from `main` with `scripts/publish-github-release.sh` after any
     > change you want consumers to pick up.

   - **Pack a tarball** (good for air-gapped CI):
     ```bash
     cd packages/agent-kit && pnpm build && pnpm pack
     # -> inngest-agent-kit-0.13.3-alpha.1.tgz  →  "file:…/inngest-agent-kit-….tgz"
     ```

   - **Workspace link** (good for local iteration): add this repo's
     `packages/agent-kit` to the Clevix pnpm workspace, or
     `pnpm add @inngest/agent-kit@file:<path-to>/packages/agent-kit`.

3. Install & build — no AI SDK version changes are needed (Clevix is already on
   `ai@6`):
   ```bash
   pnpm install && pnpm -w build
   ```

4. Sanity-check one chat turn: (a) non-zero token usage with cache reads, (b)
   optional-param tools still work on OpenAI, (c) images from image tools are
   visible to the model, (d) reasoning is preserved across tool-use turns.

### Minor: a benign v6 warning

The agent-kit design sends the system prompt as a `role: "system"` message. AI SDK
v6 logs an advisory ("System messages in the prompt … use the system option")
on each call. It is harmless (the system block, with its cache_control, is still
sent correctly); suppress it with `allowSystemInMessages: true` on the
`generateText` call if the log noise is unwanted.
