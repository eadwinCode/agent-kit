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

## 8. Consuming the fork & dropping the patch

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
