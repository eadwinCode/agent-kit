import { describe, expect, it } from "vitest";
import { AgentResult, type Message } from "./types";

const textOutput = (content: string): Message[] => [
  { type: "text", role: "assistant", content, stop_reason: "stop" },
];

describe("AgentResult checksum", () => {
  it("is stable across re-creation with a different createdAt (Inngest replay)", () => {
    // Inngest re-runs the surrounding code on every step-boundary replay, so
    // the same logical result is re-constructed with a fresh `new Date()`.
    // The checksum must not change with it — otherwise history adapters that
    // dedupe by checksum persist the same result twice (incremental append vs
    // end-of-run backstop).
    const a = new AgentResult(
      "agent",
      textOutput("hi"),
      [],
      new Date("2026-01-01T00:00:00Z")
    );
    a.id = "msg-1";
    const b = new AgentResult(
      "agent",
      textOutput("hi"),
      [],
      new Date("2026-01-01T00:00:05Z")
    );
    b.id = "msg-1";
    expect(a.checksum).toBe(b.checksum);
  });

  it("differs for identical content under different result ids", () => {
    // Two genuinely different turns can produce byte-identical output (e.g.
    // the assistant replies "Of course!" twice in one thread) — the durable
    // id keeps their checksums distinct so dedup never drops a real message.
    const a = new AgentResult(
      "agent",
      textOutput("Of course!"),
      [],
      new Date()
    );
    a.id = "msg-1";
    const b = new AgentResult(
      "agent",
      textOutput("Of course!"),
      [],
      new Date()
    );
    b.id = "msg-2";
    expect(a.checksum).not.toBe(b.checksum);
  });

  it("re-memoizes when the id is assigned after a first read", () => {
    // network.run / standalone agent.run assign `result.id` AFTER construction;
    // a checksum read before that must not pin the id-less value forever.
    const r = new AgentResult("agent", textOutput("hi"), [], new Date());
    const before = r.checksum; // memoized without id
    r.id = "msg-1";
    expect(r.checksum).not.toBe(before);

    const s = new AgentResult("agent", textOutput("hi"), [], new Date());
    s.id = "msg-1";
    expect(r.checksum).toBe(s.checksum);
  });
});
