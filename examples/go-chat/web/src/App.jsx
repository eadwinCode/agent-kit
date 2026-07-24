// go-chat frontend: the real @inngest/use-agent hook pointed at the Go
// agent-kit server — the strongest wire-parity test of the streaming
// protocol (this is the same hook Clevix Studio uses).
//
// Wiring: use-agent's DEFAULT transport (conventional /api/* endpoints,
// implemented by server/main.go) and its DEFAULT realtime connection.
//
// NOTE: the hook's `connection` prop is currently a no-op — its React
// layer (frameworks/react/hooks/use-connection.ts) always uses
// `useInngestSubscription` from @inngest/realtime, so a custom IConnection
// (e.g. plain SSE) is ignored. The Go server therefore publishes to real
// Inngest realtime and mints subscription tokens at /api/realtime/token.
import React, { useEffect, useRef, useState } from "react";
import { AgentProvider, useAgents } from "@inngest/use-agent";

export default function App() {
  return (
    <AgentProvider userId="demo-user" debug>
      <Chat />
    </AgentProvider>
  );
}

/* ---------------------------------------------------------------- parts */

/** Collapsible chain-of-thought. Auto-opens while streaming, folds when done. */
function ReasoningPart({ part }) {
  const streaming = part.status === "streaming";
  const [open, setOpen] = useState(true);
  const userToggled = useRef(false);

  // Fold once the model stops thinking — unless the user chose a state.
  useEffect(() => {
    if (!streaming && !userToggled.current) setOpen(false);
  }, [streaming]);

  const words = part.content.trim() ? part.content.trim().split(/\s+/).length : 0;

  return (
    <div className={`part reasoning ${streaming ? "is-streaming" : ""}`}>
      <button
        className="part-head"
        onClick={() => {
          userToggled.current = true;
          setOpen((v) => !v);
        }}
      >
        <span className={`chev ${open ? "open" : ""}`}>▸</span>
        <span className="glyph">✦</span>
        <span className="label">
          {streaming ? "Thinking" : "Thought"}
          {words > 0 && <span className="muted"> · {words} words</span>}
        </span>
        {streaming && <Dots />}
      </button>
      {open && <div className="part-body reasoning-body">{part.content}</div>}
    </div>
  );
}

const TOOL_STATE = {
  "input-streaming": { icon: "◌", text: "preparing", busy: true },
  "input-available": { icon: "◐", text: "queued", busy: true },
  "awaiting-approval": { icon: "⏸", text: "needs approval", busy: false },
  executing: { icon: "◑", text: "running", busy: true },
  "output-available": { icon: "✓", text: "done", busy: false },
};

/** One tool call: name + status always visible, args/result on demand. */
function ToolPart({ part }) {
  const [open, setOpen] = useState(false);
  const meta = TOOL_STATE[part.state] ?? TOOL_STATE["input-streaming"];
  const failed = part.error != null;

  return (
    <div className={`part tool ${meta.busy ? "is-streaming" : ""} ${failed ? "failed" : ""}`}>
      <button className="part-head" onClick={() => setOpen((v) => !v)}>
        <span className={`chev ${open ? "open" : ""}`}>▸</span>
        <span className="glyph">{failed ? "✕" : meta.icon}</span>
        <span className="label">
          <code>{part.toolName || "tool"}</code>
          <span className="muted"> · {failed ? "failed" : meta.text}</span>
        </span>
        {meta.busy && <Dots />}
      </button>
      {open && (
        <div className="part-body">
          <div className="kv">
            <span className="k">input</span>
            <pre>{fmt(part.input)}</pre>
          </div>
          {part.output !== undefined && (
            <div className="kv">
              <span className="k">output</span>
              <pre>{fmt(part.output)}</pre>
            </div>
          )}
          {failed && (
            <div className="kv">
              <span className="k">error</span>
              <pre>{fmt(part.error)}</pre>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function TextPart({ part }) {
  return (
    <div className="bubble assistant">
      {part.content}
      {part.status === "streaming" && <span className="caret" />}
    </div>
  );
}

/** Token totals for the current thread.
 *
 *  Fetched straight from the Go server rather than read off the hook's
 *  thread object: use-agent's ThreadManager rebuilds Thread rows with only
 *  its own known fields, so an extra `usage` key does not survive the merge.
 */
function useThreadUsage(threadId, refreshKey) {
  const [usage, setUsage] = useState(null);
  useEffect(() => {
    if (!threadId) {
      setUsage(null);
      return;
    }
    let cancelled = false;
    fetch("/api/threads")
      .then((r) => r.json())
      .then((d) => {
        if (cancelled) return;
        const t = (d.threads || []).find((x) => x.id === threadId);
        setUsage(t?.usage ?? null);
      })
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, [threadId, refreshKey]);
  return usage;
}

/** Thread token totals, summed from what agent-kit records in AgentResult.Raw. */
function Tokens({ usage }) {
  if (!usage) return null;
  const { inputTokens = 0, outputTokens = 0, cacheRead = 0, cacheWrite = 0 } = usage;
  if (!inputTokens && !outputTokens && !cacheRead && !cacheWrite) return null;

  // input_tokens is cache-exclusive, so the real prompt cost includes the
  // cache buckets — same arithmetic the billing path uses.
  const promptTotal = inputTokens + cacheRead + cacheWrite;
  const title =
    `input ${inputTokens.toLocaleString()} (+${cacheRead.toLocaleString()} cache read, ` +
    `+${cacheWrite.toLocaleString()} cache write) · output ${outputTokens.toLocaleString()}`;

  return (
    <span className="tokens" title={title}>
      <span className="tok">↑ {compact(promptTotal)}</span>
      <span className="tok">↓ {compact(outputTokens)}</span>
    </span>
  );
}

function compact(n) {
  if (n < 1000) return String(n);
  if (n < 1_000_000) return (n / 1000).toFixed(n < 10_000 ? 1 : 0) + "k";
  return (n / 1_000_000).toFixed(1) + "M";
}

function Dots() {
  return (
    <span className="dots" aria-hidden>
      <i />
      <i />
      <i />
    </span>
  );
}

function fmt(v) {
  if (v === undefined || v === null) return "—";
  if (typeof v === "string") {
    try {
      return JSON.stringify(JSON.parse(v), null, 2);
    } catch {
      return v;
    }
  }
  return JSON.stringify(v, null, 2);
}

/* -------------------------------------------------------------- message */

function AssistantMessage({ message }) {
  // Group consecutive tool calls so a parallel batch reads as one block.
  const groups = [];
  for (const part of message.parts) {
    const last = groups[groups.length - 1];
    if (part.type === "tool-call" && last?.kind === "tools") {
      last.parts.push(part);
    } else if (part.type === "tool-call") {
      groups.push({ kind: "tools", parts: [part] });
    } else {
      groups.push({ kind: part.type, parts: [part] });
    }
  }

  return (
    <div className="turn">
      {groups.map((g, i) => {
        if (g.kind === "tools") {
          return (
            <div className="tool-group" key={i}>
              {g.parts.length > 1 && (
                <div className="group-label">{g.parts.length} tools in parallel</div>
              )}
              {g.parts.map((p, j) => (
                <ToolPart key={p.toolCallId ?? j} part={p} />
              ))}
            </div>
          );
        }
        const p = g.parts[0];
        if (g.kind === "reasoning") return <ReasoningPart key={p.id ?? i} part={p} />;
        if (g.kind === "text") return <TextPart key={p.id ?? i} part={p} />;
        if (g.kind === "error")
          return (
            <div className="part tool failed" key={i}>
              <div className="part-head static">
                <span className="glyph">✕</span>
                <span className="label">{p.message ?? "error"}</span>
              </div>
            </div>
          );
        return null;
      })}
    </div>
  );
}

/* ----------------------------------------------------------------- chat */

/** What the agent is doing right now, derived from the live parts. */
function activityOf(messages, status) {
  if (status !== "streaming" && status !== "submitted") return null;
  const last = messages[messages.length - 1];
  if (!last || last.role !== "assistant") return "Working";

  const running = last.parts.filter(
    (p) => p.type === "tool-call" && TOOL_STATE[p.state]?.busy
  );
  if (running.length) {
    return running.length === 1
      ? `Running ${running[0].toolName}`
      : `Running ${running.length} tools`;
  }
  const openReasoning = last.parts.some(
    (p) => p.type === "reasoning" && p.status === "streaming"
  );
  if (openReasoning) return "Thinking";
  const openText = last.parts.some(
    (p) => p.type === "text" && p.status === "streaming"
  );
  if (openText) return "Writing";
  return "Working";
}

function Chat() {
  const {
    messages,
    status,
    sendMessage,
    currentThreadId,
    threads,
    isConnected,
    error,
    refreshThreads,
  } = useAgents({
    userId: "demo-user",
    debug: true,
    // The title is set by a tool DURING the run, so the thread list the
    // hook loaded at mount is stale by the time the run finishes.
    onStreamEnded: (args) => {
      refreshThreadsRef.current?.();
      // The hook does not always expose currentThreadId for a thread it
      // minted this session, so take the id the run reports and use it to
      // resolve the title and token totals.
      if (args?.threadId) setActiveThreadIdRef.current?.(args.threadId);
      setUsageTick((n) => n + 1);
    },
  });

  // Bumped when a run finishes, to refetch token totals.
  const [usageTick, setUsageTick] = useState(0);
  const [endedThreadId, setEndedThreadId] = useState(null);
  const setActiveThreadIdRef = useRef(null);
  setActiveThreadIdRef.current = setEndedThreadId;

  // onStreamEnded is captured once by the hook, so reach refreshThreads
  // through a ref rather than a stale closure.
  const refreshThreadsRef = useRef(null);
  refreshThreadsRef.current = refreshThreads;

  const [input, setInput] = useState("");
  const activeThreadId = currentThreadId ?? endedThreadId;
  const usage = useThreadUsage(activeThreadId, usageTick);
  const busy = status === "submitted" || status === "streaming";
  const activity = activityOf(messages, status);
  const thread = threads.find((t) => t.id === activeThreadId);

  // Keep the newest content in view while streaming.
  const logRef = useRef(null);
  useEffect(() => {
    const el = logRef.current;
    if (!el) return;
    const nearBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 120;
    if (nearBottom) el.scrollTop = el.scrollHeight;
  }, [messages, activity]);

  const onSubmit = async (e) => {
    e.preventDefault();
    const content = input.trim();
    if (!content || busy) return;
    setInput("");
    await sendMessage(content);
  };

  return (
    <>
      <header>
        <h1>go-chat</h1>
        <span className="title">{thread?.title ?? "New conversation"}</span>
        <span className="spacer" />
        <Tokens usage={usage} />
        <span className={`conn ${isConnected ? "on" : "off"}`}>
          <i /> {isConnected ? "live" : "offline"}
        </span>
      </header>

      <div className="log" ref={logRef}>
        {messages.length === 0 && (
          <div className="empty">
            Ask about the weather — the agent thinks out loud and calls tools.
          </div>
        )}
        {messages.map((m) =>
          m.role === "user" ? (
            <div key={m.id} className="bubble user">
              {m.parts
                .filter((p) => p.type === "text")
                .map((p) => p.content)
                .join("")}
            </div>
          ) : (
            <AssistantMessage key={m.id} message={m} />
          )
        )}
        {activity && (
          <div className="activity">
            <Dots />
            <span>{activity}…</span>
          </div>
        )}
        {error && <div className="part tool failed"><div className="part-head static">
          <span className="glyph">✕</span>
          <span className="label">{error.message ?? String(error)}</span>
        </div></div>}
      </div>

      <form onSubmit={onSubmit}>
        <input
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder={busy ? "Agent is working…" : "Ask about the weather…"}
          autoFocus
        />
        <button type="submit" disabled={busy || !input.trim()}>
          {busy ? "…" : "Send"}
        </button>
      </form>
    </>
  );
}
