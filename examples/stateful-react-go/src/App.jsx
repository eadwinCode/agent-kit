import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { isAgentTransportError, useAgentSession } from "@inngest/use-agent";
import { AgentStatus, eventBelongsToRun } from "./components/AgentStatus.jsx";
import { DemoSessionTransport } from "./session-transport.js";

const SESSION_ID = "demo-session";

function eventLabel(event) {
  const label = event.data?.label ?? event.data?.kind ?? event.data?.toolName;
  return label ? `${event.event}: ${label}` : event.event;
}

function renderMessage(message, index) {
  const content =
    typeof message.content === "string"
      ? message.content
      : JSON.stringify(message.content ?? message.tools ?? message.tool);
  return (
    <article
      className={`message ${message.role}`}
      key={`${message.role}-${index}`}
    >
      <strong>{message.role}</strong>
      <span className="message-type">{message.type}</span>
      <pre>{content}</pre>
    </article>
  );
}

export function App() {
  const transport = useMemo(() => new DemoSessionTransport(SESSION_ID), []);
  const [messages, setMessages] = useState([]);
  const [events, setEvents] = useState([]);
  const [liveEnabled, setLiveEnabled] = useState(true);
  const [scenario, setScenario] = useState("text");
  const [prompt, setPrompt] = useState(
    "Explain what this AgentKit session example demonstrates."
  );
  const [notice, setNotice] = useState("Ready");
  const [diagnostics, setDiagnostics] = useState(null);
  const eventIDs = useRef(new Set());

  const onEvents = useCallback((incoming) => {
    const fresh = incoming.filter((event) => {
      const key =
        event.eventId ?? `${event.streamEpoch}:${event.sequenceNumber}`;
      if (eventIDs.current.has(key)) return false;
      eventIDs.current.add(key);
      return true;
    });
    if (fresh.length)
      setEvents((current) => [...current, ...fresh].slice(-240));
  }, []);

  const session = useAgentSession({
    transport,
    scope: SESSION_ID,
    gapTimeoutMs: 1200,
    onEvents,
    onMessages: setMessages,
    onError(error) {
      setNotice(error instanceof Error ? error.message : String(error));
    },
  });

  useEffect(() => {
    if (!liveEnabled) {
      session.reportConnectionState("disconnected");
      return undefined;
    }
    session.reportConnectionState("connecting");
    const source = new EventSource(transport.liveURL());
    source.onopen = () => session.reportConnectionState("connected");
    source.onmessage = (message) => {
      try {
        session.ingest(JSON.parse(message.data));
      } catch (error) {
        setNotice(`Bad live event: ${error.message}`);
      }
    };
    source.onerror = () => session.reportConnectionState("reconnecting");
    return () => source.close();
  }, [liveEnabled, session.ingest, session.reportConnectionState, transport]);

  const latestStateEventId = [...events]
    .reverse()
    .find((event) =>
      [
        "state.updated",
        "hitl.requested",
        "hitl.resolved",
        "run.completed",
        "run.failed",
        "run.interrupted",
      ].includes(event.event)
    )?.eventId;

  useEffect(() => {
    if (latestStateEventId) {
      void session.hydrate();
    }
  }, [latestStateEventId, session.hydrate]);

  const invoke = useCallback(async (label, action) => {
    try {
      const result = await action();
      setNotice(
        `${label}: ${result.outcomeCode ?? "ok"}${result.duplicate ? " (duplicate)" : ""}`
      );
      return result;
    } catch (error) {
      setNotice(
        isAgentTransportError(error)
          ? `${label}: ${error.code} — ${error.message}`
          : `${label}: ${error.message}`
      );
      return null;
    }
  }, []);

  const refreshDiagnostics = useCallback(async () => {
    try {
      setDiagnostics(await transport.diagnostics());
    } catch (error) {
      setNotice(error.message);
    }
  }, [transport]);

  const active = Boolean(session.snapshot?.activeRun);
  const paused = session.snapshot?.pause.state === "paused";
  const pendingApproval = session.snapshot?.approval.status === "pending";
  const liveText = events
    .filter(
      (event) =>
        event.event === "text.delta" &&
        eventBelongsToRun(event, session.snapshot)
    )
    .map((event) => event.data?.delta ?? event.data?.text ?? "")
    .join("");
  const progress = [...events]
    .reverse()
    .find(
      (event) =>
        event.event === "status.updated" &&
        eventBelongsToRun(event, session.snapshot) &&
        event.data?.completed !== undefined
    );

  return (
    <main>
      <header>
        <div>
          <p className="eyebrow">@inngest/use-agent + Go AgentKit</p>
          <h1>Stateful session test lab</h1>
          <p>
            Opaque session identity, canonical history, durable replay, live
            SSE, commands, approvals, and finalization.
          </p>
        </div>
        <div className="badges">
          <span>gpt-5.6-luna · high</span>
          <span>{session.status}</span>
          <span>{session.connectionState}</span>
          <span>rev {session.snapshot?.revision ?? "–"}</span>
        </div>
      </header>

      <AgentStatus
        snapshot={session.snapshot}
        sessionStatus={session.status}
        connectionState={session.connectionState}
        events={events}
        error={session.error}
      />

      <section className="composer panel">
        <label>
          Scenario
          <select
            value={scenario}
            onChange={(event) => setScenario(event.target.value)}
            disabled={active}
          >
            <option value="text">Text + reasoning</option>
            <option value="slow">Tool + pause checkpoints</option>
            <option value="structured">Structured data + progress</option>
            <option value="approval">Human approval</option>
            <option value="error">Typed terminal failure</option>
          </select>
        </label>
        <label className="grow">
          Message
          <input
            value={prompt}
            onChange={(event) => setPrompt(event.target.value)}
          />
        </label>
        <button
          disabled={active}
          onClick={() =>
            invoke("send", () => session.send({ message: prompt, scenario }))
          }
        >
          Send
        </button>
      </section>

      <section className="controls panel">
        <button
          disabled={!active || session.snapshot?.pause.state !== "none"}
          onClick={() => invoke("pause", session.pause)}
        >
          Pause
        </button>
        <button
          disabled={!paused}
          onClick={() => invoke("resume", session.resume)}
        >
          Resume
        </button>
        <button
          className="danger"
          disabled={!active}
          onClick={() => invoke("cancel", session.cancel)}
        >
          Cancel
        </button>
        <button
          disabled={active}
          onClick={() => invoke("retry", session.retry)}
        >
          Retry last
        </button>
        <button
          disabled={active}
          onClick={() =>
            invoke("edit", () =>
              session.edit({ message: `${prompt} (edited)`, scenario })
            )
          }
        >
          Edit + rerun
        </button>
        <button
          disabled={active}
          onClick={async () => {
            const result = await invoke("new chat", session.newChat);
            if (!result) return;
            eventIDs.current.clear();
            setEvents([]);
            await session.hydrate();
          }}
        >
          New chat
        </button>
        <button onClick={() => void session.hydrate()}>Hydrate now</button>
        <button onClick={() => setLiveEnabled((value) => !value)}>
          {liveEnabled ? "Disconnect live" : "Reconnect + replay"}
        </button>
      </section>

      {pendingApproval && (
        <section className="approval panel">
          <strong>
            Approval required: {session.snapshot.approval.approvalId}
          </strong>
          <button
            onClick={() =>
              invoke("approve", () =>
                session.approve(session.snapshot.approval.approvalId)
              )
            }
          >
            Approve
          </button>
          <button
            className="danger"
            onClick={() =>
              invoke("deny", () =>
                session.deny(session.snapshot.approval.approvalId, {
                  reason: "Test denial",
                })
              )
            }
          >
            Deny
          </button>
        </section>
      )}

      <p className="notice">{notice}</p>

      <div className="grid">
        <section className="panel transcript">
          <h2>Canonical history</h2>
          {messages.length ? (
            messages.map(renderMessage)
          ) : (
            <p className="muted">
              History is empty until the server finalizes a turn.
            </p>
          )}
          {active && liveText && (
            <article className="message assistant live">
              <strong>assistant · live tail</strong>
              <pre>{liveText}</pre>
            </article>
          )}
        </section>

        <section className="panel state">
          <h2>Authoritative state</h2>
          {progress && (
            <p className="progress">
              Progress: {progress.data?.completed}/{progress.data?.total}{" "}
              {progress.data?.label}
            </p>
          )}
          {session.reconcileRequired && (
            <p className="warning">
              Retention gap: canonical reconciliation required.
            </p>
          )}
          <pre>{JSON.stringify(session.snapshot, null, 2)}</pre>
          {session.error && <p className="warning">{session.error.message}</p>}
        </section>

        <section className="panel events">
          <h2>
            Ordered durable/live events <small>{events.length}</small>
          </h2>
          <ol>
            {[...events].reverse().map((event) => (
              <li
                key={
                  event.eventId ??
                  `${event.streamEpoch}-${event.sequenceNumber}`
                }
              >
                <code>
                  {event.streamEpoch}:{event.sequenceNumber}
                </code>{" "}
                {eventLabel(event)}
              </li>
            ))}
          </ol>
        </section>

        <section className="panel diagnostics">
          <h2>Adapter diagnostics</h2>
          <button onClick={refreshDiagnostics}>Refresh</button>
          <button
            disabled={active}
            onClick={async () => {
              const result = await invoke("reset", async () => {
                await transport.reset();
                return { outcomeCode: "reset" };
              });
              if (!result) return;
              eventIDs.current.clear();
              setEvents([]);
              setMessages([]);
              setLiveEnabled(false);
              setTimeout(() => setLiveEnabled(true), 0);
              await session.hydrate();
              setNotice("Server session reset");
            }}
          >
            Reset lab
          </button>
          <pre>{JSON.stringify(diagnostics, null, 2)}</pre>
          <p className="muted">
            Finalizer calls should increase exactly once per terminal run.
            Journal events are replay transport; history is canonical content.
          </p>
        </section>
      </div>
    </main>
  );
}
