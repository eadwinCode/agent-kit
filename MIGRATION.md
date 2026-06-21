# Migration: consuming the Vercel-AI-SDK fork of `@inngest/agent-kit`

This fork (`@inngest/agent-kit@0.13.3-alpha.1`, branch `task/ai-sdk-refactor`) has
moved off `@inngest/ai` onto the **Vercel AI SDK** (`ai` v4). Inference now runs
`generateText()` inside `step.run()`, and `src/converters.ts` translates between
agent-kit `Message[]` and Vercel `CoreMessage[]`.

It folds in everything the downstream **Clevix Studio** project was carrying as a
hand-applied bun patch (`patches/@inngest%2Fagent-kit@0.13.2.patch`), so Clevix can
consume the fork directly and **delete the patch**.

---

## 1. The model API changed (required)

Old (`@inngest/ai`): you built a provider adapter with `anthropic()` / `openai()`
and passed it as `model`.

New: you build a Vercel **`LanguageModelV1`** and pass it as `model`. The agent
wraps it internally via `createAgenticModelFromLanguageModel(model)`.

### ⚠️ AI SDK version compatibility

The fork is built against **`ai` v4** and expects a **`LanguageModelV1`**.

- Providers: use the v4-compatible majors — **`@ai-sdk/anthropic@^1`** and
  **`@ai-sdk/openai@^1`** (these export `LanguageModelV1`).
- `@ai-sdk/anthropic@^3` / `@ai-sdk/openai@^2` (the `ai` v5/v6 line) export
  `LanguageModelV2` and will **not** type-check or run against this fork.

Clevix currently resolves `ai@6` + `@ai-sdk/anthropic@3` for other code paths.
Pin the v4-compatible providers for the agent-kit path (a dedup/alias or a
separate dependency entry), or align the whole app to `ai@4`.

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
// AFTER — Vercel LanguageModelV1; per-request settings baked in with middleware
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
      maxTokens: 16384,
      // Anthropic extended thinking — providerMetadata at the V1 call layer:
      providerMetadata: {
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
- OpenAI reasoning effort → `settings.providerMetadata.openai.reasoningEffort`.
- Anthropic **requires** `max_tokens`; if you don't set it the provider falls back
  to its own default (4096). Set it explicitly to keep the 8k/16k behavior.

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
    "input_tokens": 1200,            // = AI SDK promptTokens; EXCLUDES cache tokens
    "output_tokens": 340,            // = completionTokens
    "total_tokens": 1540,
    "cache_creation_input_tokens": 800,   // present only for Anthropic w/ caching
    "cache_read_input_tokens": 16000      // present only for Anthropic w/ caching
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

`input_tokens` is the **non-cache** prompt count (matching the Anthropic API and
`ai`-v4 `promptTokens` semantics), so adding the cache buckets does **not**
double-count. No change to `billing.ts` is required.

---

## 3. Reasoning is a first-class message (render it; it round-trips)

`AgentResult.output` may now contain a `reasoning` message:

```ts
{ type: "reasoning", role: "assistant", content: "…", signature?: "…",
  details?: [ { type: "text", text, signature? } | { type: "redacted", data } ] }
```

- It is emitted **before** the text / tool-call messages within a turn. Anthropic
  requires the thinking block to precede the tool-use block, and the converter
  replays it (with signatures) so multi-step tool use with extended thinking keeps
  working.
- Clevix's `history-prune.ts` already passes non-`tool_call` messages through
  untouched, so persistence/replay is unaffected. Render the `reasoning` message
  in the UI if you want to surface chain-of-thought.

This replaces the patch's "skip unhandled block types" crash fix — there is no
longer a hand-rolled response parser to crash on thinking blocks.

---

## 4. Vision / images in tool results (and user messages)

`messagesToCoreMessages` converts image content into Vercel image parts:

- **User messages**: an image content part
  `{ type: "image", image: <url|base64|dataURL>, mimeType? }` becomes an AI SDK
  `ImagePart`.
- **Tool results**: when `ToolResultMessage.content` is an array of blocks and at
  least one is an image, the converter attaches AI SDK multi-part
  `experimental_content` so a vision model can see it. It accepts the
  **Anthropic-native** shape your `imageToolResultContent()` already emits —
  `{ type: "image", source: { type: "base64", media_type, data } }` — as well as
  the AI SDK shape `{ type: "image", data, mimeType }`. URL-only images are
  surfaced as text (the AI SDK tool-result image part is base64-only).

So `vision.ts` / `imageToolResultContent()` keep working as-is; the bun patch's
"forward array content verbatim" hack is no longer needed.

---

## 5. Anthropic prompt caching (replaces the patch's cache_control edits)

Caching is **auto-enabled for Anthropic models** (detected from the model's
`provider` id) and **off for everyone else** — safe to leave on:

- The **system message** is marked with `providerOptions.anthropic.cacheControl =
  { type: "ephemeral" }`. Because Anthropic orders a request as
  `[tools, system, messages]`, this caches the **tool definitions + system
  prompt** as one prefix.
- The **last tool** is also marked, best-effort. ⚠️ Caveat: the `ai` v4 `Tool`
  type has no `providerOptions`, so v4 cannot carry a tool-level breakpoint to the
  provider — the system-message breakpoint already caches the tool prefix, so this
  is effectively a single breakpoint on v4. (The marker is there for forward
  compatibility with providers/SDKs that read it.)

Cache read/creation tokens flow back through §2's `usage` for cache-aware billing.

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
either way. No behavioral regression for Clevix. If a future need for
`step.ai.infer` semantics appears (e.g. provider-native offloading), revisit then —
there is no current blocker.

---

## 8. Consuming the fork & dropping the patch

1. **Remove the patch** from Clevix:
   - delete `patches/@inngest%2Fagent-kit@0.13.2.patch`
   - delete the `patchedDependencies` entry (and the `@inngest/agent-kit@0.13.2`
     pin) from `package.json`.

2. **Point at the fork.** Two options:

   - **Pack a tarball** (good for CI / reproducible installs):
     ```bash
     # in this repo
     cd packages/agent-kit && pnpm build && pnpm pack
     # -> inngest-agent-kit-0.13.3-alpha.1.tgz
     ```
     Then in Clevix `package.json`:
     ```jsonc
     "dependencies": {
       "@inngest/agent-kit": "file:../agent-kit/packages/agent-kit/inngest-agent-kit-0.13.3-alpha.1.tgz"
     }
     ```

   - **Workspace link** (good for local iteration): add this repo's
     `packages/agent-kit` to the Clevix pnpm workspace (or
     `pnpm add @inngest/agent-kit@file:<path-to>/packages/agent-kit`) and let pnpm
     symlink it.

3. **Pin v4-compatible AI SDK providers** (see §1) and build:
   ```bash
   pnpm install && pnpm -w build
   ```

4. Sanity-check: one chat turn should now (a) record non-zero token usage with
   cache reads, (b) keep optional-param tools working on OpenAI, (c) show images
   from image tools, and (d) preserve reasoning across tool-use turns.
