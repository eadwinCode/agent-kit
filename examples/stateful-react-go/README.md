# Stateful React + Go session lab

This is a runnable example of the new server-authoritative session API in `@inngest/use-agent` backed by the Go AgentKit runtime. Every browser scenario uses OpenAI's Responses API with `gpt-5.6-luna` and high reasoning effort; there is no mock provider in the runnable server.

The generic contract stays tenancy-neutral. `demo-session` is an opaque application-defined scope; there is no required `project_id`, `team_id`, `projectId`, or `teamId`. A real adapter can add its own authenticated ownership fields without changing AgentKit's base contract.

## Run it

Requirements: Go 1.26 or newer and Node.js 20 or newer.

Terminal 1:

```sh
cd examples/stateful-react-go/server
set -a
. ./.env
set +a
go run .
```

The `.env` file must define `OPENAI_API_KEY`. Alternatively, export that variable in your shell before running the server. The key remains in the Go process and is never sent to React.

Terminal 2:

```sh
cd examples/stateful-react-go
npm install
npm run dev
```

Open <http://localhost:5173>. The Vite development server proxies `/api` to the Go server at `127.0.0.1:8485`.

The provider uses model `gpt-5.6-luna`, `reasoning.effort: "high"`, an automatic reasoning summary for the status stream, a 4,096-token output cap, and `store: false`. If the key is absent, every send command returns the bounded `OPENAI_NOT_CONFIGURED` error instead of starting a doomed run.

## What each scenario proves

| Scenario                   | Exercise                                                                            |
| -------------------------- | ----------------------------------------------------------------------------------- |
| Text + reasoning           | Provider reasoning and text deltas, live tail, canonical history after finalization |
| Tool + pause checkpoints   | Pause intent, stopping only at a safe checkpoint, resume epoch correlation, cancel  |
| Structured data + progress | `status.updated`, data-part created/delta/completed events, tool progress           |
| Human approval             | Durable approval request, approve or deny, one-time approval consumption            |
| Typed terminal failure     | A real model tool call followed by a failed outcome and bounded server error code   |

The remaining buttons cover retry, edit-and-rerun, new chat, explicit hydration, and server reset. To verify recovery rather than just live streaming:

1. Choose **Tool + pause checkpoints** and send a turn.
2. Click **Disconnect live** while the tool is moving through its five items.
3. Wait, then click **Reconnect + replay**.
4. The hook hydrates state and fills the missed ordered range from the Go event journal. Event IDs prevent duplicates.

The diagnostics panel exposes the number of transient journal records, canonical history entries, and finalizer calls. A terminal run should add exactly one finalizer call.

The `AgentStatus` component at the top translates the orthogonal session dimensions into one accessible display. It shows starting, provider-confirmed thinking, responding, named tool work and progress, approval waits, pause/resume, reconnect recovery, finalization, and terminal outcomes. Its recent-activity trail uses actual `status.updated` labels; it never cycles through invented “thinking” text.

## Architecture

```text
React UI
  useAgentSession
    ├── DemoSessionTransport ── snapshot + canonical history
    ├── EventSource ─────────── best-effort live envelopes
    └── DemoSessionTransport ── ordered journal backfill + commands
                                      │
Go HTTP adapter                       │
  opaque /sessions/{session}          │
    ├── AgentKit Network + tools ─────┘
    ├── StateStore (CAS revision)
    ├── EventJournal (replay tail)
    ├── ControlStore (pause/resume/cancel)
    ├── ApprovalStore
    ├── canonical HistoryConfig
    └── Finalizer
```

The important split is intentional: state is a bounded control record, the journal is short-lived recovery transport, and `HistoryConfig` owns canonical conversation content. Do not reconstruct completed history from an event stream.

`memadapter` is a reference adapter for examples and tests. It is process-local and loses everything on restart. Production applications should implement the same Go ports with durable storage, authorize the opaque session before every state/tail/command operation, use secure same-origin cookies or another application-owned credential, add CSRF protection where applicable, enforce request limits, and apply retention only after finalization has made history canonical.

## Tests

Frontend transport contract and production bundle:

```sh
cd examples/stateful-react-go
npm test
npm run build
```

Go lifecycle, concurrency, replay, approval, and finalizer paths:

```sh
cd examples/stateful-react-go/server
go test -race ./...
go vet ./...
```

The Go tests cover safe-boundary pause/resume, approval consumption, idempotent command replay, idempotency-key misuse, stale-revision reconciliation, ordered stable event envelopes, canonical history, and exactly-once finalization. Their scripted language model is compiled only into `_test.go`; production code always constructs the OpenAI provider.

## Where to adapt it

- [`src/session-transport.js`](./src/session-transport.js) is the application-owned `IAgentSessionTransport` implementation.
- [`src/App.jsx`](./src/App.jsx) shows `useAgentSession`, live ingestion, reconnect hydration, and every command action.
- [`src/components/AgentStatus.jsx`](./src/components/AgentStatus.jsx) is a reusable, accessible agentic-status tracker with a pure derivation function for testing.
- [`server/http.go`](./server/http.go) maps the neutral wire contract to an opaque scope and structured error envelope.
- [`server/lab.go`](./server/lab.go) wires all Go runtime ports and scenario tools around the real model.
- [`server/history.go`](./server/history.go) keeps canonical user and assistant turns in one ordered history.
- [`server/lab_test.go`](./server/lab_test.go) is the executable lifecycle specification.
