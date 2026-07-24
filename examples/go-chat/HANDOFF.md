# go-chat — status

**2026-07-24 (later): reasoning stream + live updates verified in browser.**
The chat now runs INLINE in the Go server and publishes to Inngest realtime
on the EXISTING server at :8288 (works against `inngest start` with a
signing key — Bearer auth on /v1/realtime/publish and /v1/realtime/token).
Key fixes since the section below was written:

- **use-agent's reducer dropped ALL reasoning events.** `reasoning.delta` /
  `part.created(type reasoning)` had no handling — only the ReasoningUIPart
  type existed. Patched the fork (types + event-mapper + streaming-reducer,
  mirroring the text-part path); 81/81 tests pass. This affects Clevix too:
  before this patch NO consumer of use-agent could render a thought stream.
- **The browser's realtime host is effectively pinned to :8288.**
  @inngest/realtime's env lookup can't see import.meta.env inside Vite's
  pre-bundled deps, so every override silently falls through to
  localhost:8288. Don't fight it: run the Inngest server there.
- The earlier "Expected server kind cloud, got dev" came from a sloppy .env
  copy that included Clevix's INNGEST_SIGNING_KEY while pointing at a plain
  dev server. Signing key ⇒ `inngest start` semantics.
- After editing the file-linked use-agent, `npm install @inngest/use-agent`
  in web/ AND `rm -rf node_modules/.vite` — npm COPIES file: deps and Vite
  caches the pre-bundle; skipping either serves the stale build.

**SQL persistence (same day).** The in-memory store and the hand-rolled
message seed are gone; `server/store.go` implements agent-kit's
`HistoryConfig` over SQLite (pure-Go `modernc.org/sqlite`, no cgo) and is
handed to `NetworkConfig.History`. Verified: two turns persist, the server
restarts, and turn 3 answers *"We discussed **Tokyo**, 21°C"* purely from
disk. Notes:

- **Port API bug this surfaced and fixed:** `NewAgentResult` and
  `AgentResult.CreatedAt` took `internal/jsonutil.Time`, so nothing outside
  the module could construct or persist a result. `go/types.go` now exports
  `agentkit.Time` (alias) + `agentkit.Now()`.
- User turns are stored as results whose `Output` is one user-role text
  message — the framework flattens that into the transcript, so the whole
  conversation stays in the history layer (no parallel seed).
- `AppendResults` is called both incrementally and as an end-of-run
  backstop, so `insertMessage` is idempotent on the replay-stable result id.
- Replaying reasoning from storage worked here, but an earlier hand-built
  seed containing signed thinking blocks made Anthropic return an empty
  completion. If blank turns reappear, strip reasoning in `Get` first.

**Earlier pass: live chat UI + memory fixed.**

- **Multi-turn memory works.** Root cause: an `AgentResult` carries only
  assistant output + tool results, never the user turn that prompted it, so
  replaying results alone left the model answering blind. The server now
  rebuilds the interleaved conversation (`seedMessages`) and passes it as
  `StateConfig.Messages` with `Results` nil. Verified: turn 2 answers
  *"Yes, 21°C is warmer than 15°C by 6 degrees."*
- **Reasoning must be stripped from replayed history.** A signed thinking
  block is only valid inside the assistant turn that produced it; replaying
  it standalone makes Anthropic return an EMPTY completion (silent, no
  error). `seedMessages` drops reasoning — it is display-only, already
  streamed live.
- **UI**: collapsible thoughts (auto-open while streaming, fold when done,
  word count), tool cards with status glyph + expandable input/output,
  parallel calls grouped under "N tools in parallel", live activity line
  ("Thinking…" / "Running get_weather…" / "Writing…"), streaming caret,
  connection pill, auto-scroll. `refreshThreads()` on `onStreamEnded` so
  the tool-set title appears in the header.
- Removed the `/api/ping` diagnostic. The durable Inngest function remains
  as the worked example of agent-kit's Inngest integration, documented as
  requiring `inngest dev` (a signed `inngest start` won't sync a local app).

---

Test app for the Go agent-kit, driven by the real `@inngest/use-agent` hook.
**Working end-to-end in durable mode, verified in the browser.** This file
records what's proven, what was learned, and what's left.

See README.md for how to run it.

## Verified (2026-07-24, live Anthropic + Inngest dev server)

- **Full turn renders in the browser** via the real hook: user message →
  two tool-call parts showing inputs *and* outputs (the `ToolCallUIPart`
  state machine reaching `output-available`) → streamed assistant text.
- **Durable replay: exactly-once tools.** A single turn drove **47 function
  invocations** (step boundaries) while `get_weather` and
  `set_conversation_title` each logged `EXECUTED` exactly **once**. This
  was the port's last unverified claim — the memoized-step contract holds
  under real replay pressure.
- **Realtime streaming**: chunks published from Go reach `use-agent`'s
  `useInngestSubscription` and drive its reducer correctly (header shows
  `ready · live`).
- **State-mutating tool across replays**: `set_conversation_title` sets
  `chatState.Title` inside a memoized step; the title survives to the
  threads list ("Tokyo Weather Check").
- **Multi-turn memory** (curl, thread `th_mem`): turn 2 answered *"yes,
  21°C is warmer than 15°C — about 6 degrees warmer"* from turn 1's tool
  result. See the caveat below.
- `go build ./... && go vet ./...` clean.

## Findings worth keeping

1. **`realtime.Publish` hardcodes production.** `inngestgo`'s
   `realtime.Publish` always targets `https://api.inngest.com/v1/realtime/publish`
   and ignores dev mode — using it against the dev server **hangs the step
   silently** (the run sits in RUNNING forever, no error anywhere). Always
   use `realtime.PublishWithURL`. This cost the longest debugging detour
   here and will bite the Clevix migration; worth noting in the Go port docs.
2. **`use-agent`'s `connection` prop is currently a no-op.** Its React layer
   (`frameworks/react/hooks/use-connection.ts`) always uses
   `useInngestSubscription` from `@inngest/realtime`; the `IConnection` port
   exists in `core/` but React hasn't migrated to it (the file says so).
   A custom SSE connection is silently ignored — hence real Inngest realtime
   here. Revisit if that port lands.
3. **Realtime token shape.** `/api/realtime/token` must return
   `{channel, topics: [...], key: <jwt>}` — `@inngest/realtime`'s
   `TokenSubscription` reads `token.key` and `token.topics`. Returning
   `{token}` alone causes a tight token-refresh retry loop. Mint via
   `POST {inngest}/v1/realtime/token` with `[{channel,name,kind:"run"}]`;
   the dev server answers **201** (not 200) and needs no auth.
4. **History format for the hook.** `ThreadManager.formatRawHistoryMessages`
   wants rows of `{message_id, createdAt, type:"user"|…, content, data.output}`
   — not `AgentResult` exports. `ThreadsPage` needs
   `{threads:[{id,title,messageCount,lastMessageAt,createdAt,updatedAt}], hasMore, total}`.
5. **Go 1.22+ mux** rejects `OPTIONS /api/` alongside `/api/inngest`; CORS
   preflight lives in middleware instead.

## Known gaps / remaining work

1. **Prior user turns aren't in the model's history.** The `AgentResult`
   stack carries assistant output + tool results only, so the model sees
   answers without the questions that produced them. Sometimes it infers
   correctly (the `th_mem` run), sometimes it asks for context it should
   have ("which city?" in the browser run). **This is framework-faithful —
   the TS package behaves identically — so fix it in the example, not in
   `go/`.** The server already stores user turns in `thread.History`; seed
   them via `StateConfig.Messages` (note `FormatHistory` places seeded
   messages *before* results, so interleaving needs thought).
2. **Thread title in the header** shows "new conversation" until the threads
   list refreshes; call `refreshThreads()` after `stream.ended`.
3. **Diagnostics still in the code**: the `/api/ping` function and the
   `[result]` / `[tool] … EXECUTED` logging. Useful; remove if they become
   noise.
4. **Nothing is committed** — `examples/` and `go/` are untracked.
5. **Kill-mid-run test not done explicitly.** The 47-invocation
   exactly-once result proves memoization under normal step boundaries; a
   deliberate `kill -9` mid-run and resume would close the loop.

## Browser-automation gotcha (not an app bug)

Driving the React input with synthetic typing, or splitting "set value" and
"submit" across a `setTimeout` in separate `javascript_exec` calls, sends
stale/garbage content (observed: "hi", "hello"). Setting the value via the
native setter + `input` event and calling `requestSubmit()` **in one
evaluation** works — verified by intercepting `fetch`: the payload contained
exactly the probe string. Don't chase this as an app defect.
