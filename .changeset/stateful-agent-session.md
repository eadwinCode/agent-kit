---
"@inngest/use-agent": minor
---

Server-authoritative agent sessions: snapshot-plus-tail recovery, typed transport errors, and idempotent commands.

This is the maintained release that moves recovery and adaptation code out of applications and into the package, so the behavior is tested once instead of reimplemented per app.

**Typed transport errors.** `AgentTransportError` preserves the HTTP status, the server's bounded error code, whether the failure is recoverable, the correlation id, `retryAfterMs`, structured details, and — for a conflict — the authoritative snapshot the client should reconcile to. Applications no longer need a side variable next to the hook to keep a structured `409`.

**Snapshot-plus-tail hydration.** `hydrateAgentSession`, `LiveEventBuffer` and `SequenceGapTracker` implement the full recovery algorithm: buffer live envelopes before fetching, replace history from the snapshot, drain the durable tail in `(streamEpoch, sequenceNumber, eventId)` order, merge the buffer by stable event id, and go live. A live sequence gap triggers a bounded backfill instead of stalling; a gap that never fills becomes a re-snapshot rather than a permanently frozen transcript.

**Stream epochs.** Envelopes now carry `eventId`, `streamEpoch` and `schemaVersion`. A resumed or restarted run increments its epoch and resets sequence numbers, so a client discards a stale tail instead of waiting for numbers that will never arrive.

**Recoverable connection state.** `acquireRealtimeToken` retries transient token failures with full-jitter exponential backoff and cancellation, and fails fast on 401/403 where retrying cannot help. `ClientConnectionState` distinguishes a first connect from a reconnect, and is never written to the server.

**Idempotent commands.** `buildCommand` and `executeCommand` mint a command id, attach the snapshot's revision as a CAS precondition, retry recoverable failures under the same id, and refuse to retry an idempotency-key reuse, a stale revision, or a terminal run.

**`useAgentSession`.** A React hook that runs the whole algorithm and exposes `send`, `pause`, `resume`, `cancel`, `approve`, `deny`, `retry`, `edit` and `newChat` with stable identity across renders. It re-hydrates when the socket returns, because the run kept going while the client was away.

**Cross-tab user turns.** The reducer now renders the server-accepted `user.message` event, so a second tab no longer watches the assistant answer a question it never saw asked. The sending tab's optimistic message converges onto the server's id instead of duplicating beside it, and a redelivery through backfill is idempotent.

**Resume from what you applied.** Hydration takes an optional `from` cursor — the client's own last-applied position — so a reconnect continues the tail instead of replaying a turn already on screen. The snapshot's cursor is documented as where a *newcomer* starts: the beginning of the active epoch, not the newest event, because an in-flight message exists only in the tail.

**No tenancy in the contract.** `AgentStateSnapshot` carries an opaque `sessionId` instead of `agentId` + `projectId`, and `projectId` is gone from `AgentCommand`, `FetchAgentStateParams`, `BuildCommandParams` and `HydrationOptions`. Teams, projects and workspaces are the application's model: the transport it implements already authenticated against its own scope, so echoing those ids back through the contract forced consumers with a different model — or none — to carry fields nothing reads. Applications that want their own alongside intersect the type: `AgentStateSnapshot & { projectId: string }`. `useAgentSession` takes an opaque `scope` key used only to decide when to re-hydrate; it is never sent anywhere.

**`IAgentSessionTransport`.** The client half of the runtime contracts. The package knows the shape of a snapshot, an event tail and a command; it does not know which endpoints serve them or which tables back them.

Cross-runtime contract tests reduce the same golden fixtures the Go runtime generates (`contracts/fixtures`), and negative architecture tests assert what must stay absent: no `EventSource` for AI chat, no browser-owned current thread, no second reducer, no hard-coded application endpoints, no required polling loop.
