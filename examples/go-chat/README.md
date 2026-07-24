# go-chat

End-to-end test app for the Go agent-kit (`go/` at the repo root), driven by
the **real `@inngest/use-agent` React hook** — the same client Clevix Studio
uses. That makes it a genuine wire-parity test of the Go streaming protocol,
not a bespoke demo client.

It runs in **durable mode**: the network executes inside an Inngest function,
so every inference and tool call is a memoized step, and stream chunks are
published to Inngest realtime (the transport `use-agent` actually subscribes
to).

```
                       POST /api/chat  ──send event──┐
  web (:5173) ──────►  POST /api/realtime/token      │
  use-agent hook       GET/POST /api/threads…        ▼
       ▲                                    Inngest dev (:8288)
       └──── realtime WS ◄── realtime.Publish ◄── fn on /api/inngest
```

## Run it

Three processes. The Anthropic key goes in `server/.env` (already gitignored)
or the environment:

```bash
npx inngest-cli@latest dev -u http://localhost:8484/api/inngest
```

```bash
cd examples/go-chat/server && ANTHROPIC_API_KEY=sk-ant-... go run .
```

```bash
cd examples/go-chat/web && npm install && npm run dev
```

Then open http://localhost:5173. The header shows `ready · live` once the
realtime socket is connected. `MODEL` overrides the default
`claude-sonnet-4-5`.

If the frontend can't reach the functions, re-register the app:
`curl -X PUT http://localhost:8484/api/inngest`.

## The UI

Thoughts stream live and collapse when finished (click to reopen); tool
calls show status inline and expand to full input/output; parallel calls
group together; an activity line tracks what the agent is doing right now.

## Persistence

Conversations live in SQLite (`server/go-chat.db`, override with `DB_PATH`)
behind agent-kit's own `HistoryConfig` — see `server/store.go`. Passing it to
`NetworkConfig.History` is the whole integration: the framework creates the
thread, writes the user's turn up front, hydrates prior context via `Get`,
and saves results as they land, each inside its own durable step. The HTTP
layer never assembles conversation context by hand.

One wrinkle worth knowing: an `AgentResult` carries assistant output and
tool results but never the user turn that prompted it, so results-only
history leaves the model reading its own answers with the questions
missing. The store therefore records each user turn as a result whose
`Output` is a single user-role text message — `State.FormatHistory`
flattens that into the transcript verbatim, keeping the whole conversation
inside the history layer.

## Token usage

Each stored result keeps the token counts agent-kit records in
`AgentResult.Raw`; the threads endpoint sums them per conversation and the
header shows `↑ prompt ↓ output` (hover for the cache breakdown). Prompt
total adds the cache buckets to `input_tokens`, which is cache-exclusive —
the same arithmetic the billing path uses.

## What it exercises

Typed network state, a state-mutating tool (`set_conversation_title`) through
the memoized state-patch path, the router-driven tool round, SQL-backed
conversation memory through `HistoryConfig`, the full 26-event streaming
protocol, and durable replay semantics.

See HANDOFF.md for verified results, known gaps, and remaining work.
