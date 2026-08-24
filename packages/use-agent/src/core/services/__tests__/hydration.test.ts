import { describe, it, expect, vi } from "vitest";
import {
  hydrateAgentSession,
  LiveEventBuffer,
  SequenceGapTracker,
  sortEnvelopes,
  envelopeKey,
} from "../hydration.js";
import { STREAM_START } from "../../ports/agent-session.js";
import type {
  IAgentSessionTransport,
  AgentStateResponse,
  EventTailPage,
  StreamCursor,
} from "../../ports/agent-session.js";
import type {
  AgentStateSnapshot,
  StandardEventEnvelope,
} from "../../../types/index.js";
import { AgentTransportError } from "../../errors/agent-transport-error.js";

function envelope(
  seq: number,
  event = "text.delta",
  epoch = 0,
  runId = "run_1"
): StandardEventEnvelope {
  return {
    event,
    data: { runId, threadId: "thread_1", delta: `d${seq}` },
    timestamp: 1_700_000_000_000 + seq,
    sequenceNumber: seq,
    id: `publish-${seq}:${event}`,
    eventId: `${runId}:${epoch}:${seq}`,
    streamEpoch: epoch,
    schemaVersion: 1,
  };
}

function snapshot(overrides: Partial<AgentStateSnapshot> = {}): AgentStateSnapshot {
  return {
    schemaVersion: 1,
    sessionId: "session_1",
    currentThreadId: "thread_1",
    activeRun: {
      runId: "run_1",
      lifecycle: "executing",
      acceptedAt: "2026-08-22T10:00:00.000Z",
    },
    pause: { state: "none", accumulatedPausedMs: 0 },
    activity: { kind: "responding", source: "provider" },
    approval: { status: "none" },
    revision: 7,
    cursor: { runId: "run_1", streamEpoch: 0, sequenceNumber: STREAM_START },
    reconcileRequired: false,
    ...overrides,
  };
}

/** A scripted session transport: pages are handed out in order. */
function makeTransport(config: {
  state: AgentStateResponse | AgentStateResponse[];
  pages?: EventTailPage[];
  onTail?: (after: StreamCursor) => void;
}): IAgentSessionTransport & { stateCalls: number; tailCalls: number } {
  const states = Array.isArray(config.state) ? [...config.state] : [config.state];
  const pages = [...(config.pages ?? [])];
  const transport = {
    stateCalls: 0,
    tailCalls: 0,
    async fetchAgentState() {
      transport.stateCalls++;
      return states.length > 1 ? states.shift()! : states[0];
    },
    async fetchEventTail({ after }: { after: StreamCursor }) {
      transport.tailCalls++;
      config.onTail?.(after);
      return (
        pages.shift() ?? {
          events: [],
          next: after,
          hasMore: false,
        }
      );
    },
    async executeCommand() {
      throw new Error("not used");
    },
  };
  return transport as IAgentSessionTransport & {
    stateCalls: number;
    tailCalls: number;
  };
}

describe("hydrateAgentSession", () => {
  it("replaces history from the snapshot and reduces the durable tail in order", async () => {
    const transport = makeTransport({
      state: {
        snapshot: snapshot(),
        messages: [{ role: "user", content: "hi" } as never],
        cursor: { runId: "run_1", streamEpoch: 0, sequenceNumber: STREAM_START },
      },
      pages: [
        {
          events: [envelope(2), envelope(0), envelope(1)],
          next: { runId: "run_1", streamEpoch: 0, sequenceNumber: 2 },
          hasMore: false,
        },
      ],
    });

    const buffer = new LiveEventBuffer();
    const result = await hydrateAgentSession({ transport }, buffer);

    expect(result.outcome).toBe("hydrated");
    expect(result.messages).toHaveLength(1);
    expect(result.events.map((e) => e.sequenceNumber)).toEqual([0, 1, 2]);
    expect(result.cursor?.sequenceNumber).toBe(2);
  });

  it("keeps live events that raced the fetch, without duplicating tail events", async () => {
    // The events produced while the snapshot was in flight are the ones a
    // naive implementation loses — and they belong to the turn on screen.
    const buffer = new LiveEventBuffer();
    buffer.push(envelope(1));
    buffer.push(envelope(2));
    buffer.push(envelope(3));

    const transport = makeTransport({
      state: {
        snapshot: snapshot(),
        messages: [],
        cursor: { runId: "run_1", streamEpoch: 0, sequenceNumber: STREAM_START },
      },
      pages: [
        {
          events: [envelope(0), envelope(1), envelope(2)],
          next: { runId: "run_1", streamEpoch: 0, sequenceNumber: 2 },
          hasMore: false,
        },
      ],
    });

    const result = await hydrateAgentSession({ transport }, buffer);

    expect(result.events.map((e) => e.sequenceNumber)).toEqual([0, 1, 2, 3]);
    const ids = result.events.map(envelopeKey);
    expect(new Set(ids).size).toBe(ids.length);
  });

  it("drops buffered events from a superseded epoch", async () => {
    const buffer = new LiveEventBuffer();
    buffer.push(envelope(5, "text.delta", 0));
    buffer.push(envelope(1, "text.delta", 2));

    const transport = makeTransport({
      state: {
        snapshot: snapshot({
          cursor: { runId: "run_1", streamEpoch: 2, sequenceNumber: 0 },
        }),
        messages: [],
        cursor: { runId: "run_1", streamEpoch: 2, sequenceNumber: 0 },
      },
      pages: [
        {
          events: [],
          next: { runId: "run_1", streamEpoch: 2, sequenceNumber: 0 },
          hasMore: false,
        },
      ],
    });

    const result = await hydrateAgentSession({ transport }, buffer);
    expect(result.events.map((e) => e.streamEpoch)).toEqual([2]);
  });

  it("does not reuse a cursor from a different run with the same epoch", async () => {
    const seen: StreamCursor[] = [];
    const transport = makeTransport({
      state: {
        snapshot: snapshot({
          activeRun: {
            runId: "run_2",
            lifecycle: "executing",
            acceptedAt: "2026-08-22T10:05:00.000Z",
          },
          cursor: {
            runId: "run_2",
            streamEpoch: 0,
            sequenceNumber: STREAM_START,
          },
        }),
        messages: [],
        cursor: {
          runId: "run_2",
          streamEpoch: 0,
          sequenceNumber: STREAM_START,
        },
      },
      onTail: (after) => seen.push(after),
    });

    await hydrateAgentSession(
      {
        transport,
        from: { runId: "run_1", streamEpoch: 0, sequenceNumber: 17 },
      },
      new LiveEventBuffer()
    );

    expect(seen[0]).toEqual({
      runId: "run_2",
      streamEpoch: 0,
      sequenceNumber: STREAM_START,
    });
  });

  it("keeps child-run events that share the parent stream sequence", async () => {
    const buffer = new LiveEventBuffer();
    buffer.push(envelope(9, "text.delta", 0, "run_1"));
    buffer.push(envelope(0, "run.started", 0, "run_2"));
    const run2 = snapshot({
      activeRun: {
        runId: "run_2",
        lifecycle: "executing",
        acceptedAt: "2026-08-22T10:05:00.000Z",
      },
      cursor: { runId: "run_2", streamEpoch: 0, sequenceNumber: STREAM_START },
    });
    const transport = makeTransport({
      state: {
        snapshot: run2,
        messages: [],
        cursor: {
          runId: "run_2",
          streamEpoch: 0,
          sequenceNumber: STREAM_START,
        },
      },
    });

    const result = await hydrateAgentSession({ transport }, buffer);
    expect(result.events.map((event) => event.data?.runId)).toEqual([
      "run_2",
      "run_1",
    ]);
    expect(result.cursor?.runId).toBe("run_2");
  });

  it("follows tail pages until the server reports no more", async () => {
    const seen: number[] = [];
    const transport = makeTransport({
      state: {
        snapshot: snapshot(),
        messages: [],
        cursor: { runId: "run_1", streamEpoch: 0, sequenceNumber: STREAM_START },
      },
      pages: [
        {
          events: [envelope(0), envelope(1)],
          next: { runId: "run_1", streamEpoch: 0, sequenceNumber: 1 },
          hasMore: true,
        },
        {
          events: [envelope(2)],
          next: { runId: "run_1", streamEpoch: 0, sequenceNumber: 2 },
          hasMore: false,
        },
      ],
      onTail: (after) => seen.push(after.sequenceNumber),
    });

    const result = await hydrateAgentSession({ transport }, new LiveEventBuffer());
    expect(seen).toEqual([STREAM_START, 1]);
    expect(result.events).toHaveLength(3);
  });

  it("reports reconcileRequired when the tail hit a retention gap", async () => {
    const transport = makeTransport({
      state: {
        snapshot: snapshot(),
        messages: [],
        cursor: { runId: "run_1", streamEpoch: 0, sequenceNumber: STREAM_START },
      },
      pages: [
        {
          events: [envelope(4)],
          next: { runId: "run_1", streamEpoch: 0, sequenceNumber: 4 },
          hasMore: false,
          retentionGap: true,
        },
      ],
    });

    const result = await hydrateAgentSession({ transport }, new LiveEventBuffer());
    // Completed content still came from canonical history, so this is a
    // degraded success — not a failure the UI should surface as broken.
    expect(result.outcome).toBe("hydrated");
    expect(result.reconcileRequired).toBe(true);
  });

  it("treats a RETENTION_GAP transport error the same way", async () => {
    const transport = makeTransport({
      state: {
        snapshot: snapshot(),
        messages: [],
        cursor: { runId: "run_1", streamEpoch: 0, sequenceNumber: STREAM_START },
      },
    });
    transport.fetchEventTail = async () => {
      throw new AgentTransportError({
        status: 410,
        code: "RETENTION_GAP",
        message: "gone",
        recoverable: false,
      });
    };

    const result = await hydrateAgentSession({ transport }, new LiveEventBuffer());
    expect(result.outcome).toBe("hydrated");
    expect(result.reconcileRequired).toBe(true);
  });

  it("skips the tail entirely when no run is active", async () => {
    const transport = makeTransport({
      state: {
        snapshot: snapshot({ activeRun: null, cursor: null }),
        messages: [],
        cursor: null,
      },
    });

    const result = await hydrateAgentSession({ transport }, new LiveEventBuffer());
    expect(transport.tailCalls).toBe(0);
    expect(result.events).toEqual([]);
  });

  it("aborts cleanly when the caller cancels", async () => {
    const controller = new AbortController();
    controller.abort();
    const transport = makeTransport({
      state: { snapshot: snapshot(), messages: [], cursor: null },
    });

    const result = await hydrateAgentSession(
      { transport, signal: controller.signal },
      new LiveEventBuffer()
    );
    expect(result.outcome).toBe("aborted");
    expect(transport.stateCalls).toBe(0);
  });

  it("surfaces a snapshot failure instead of pretending to be hydrated", async () => {
    const transport = makeTransport({
      state: { snapshot: snapshot(), messages: [], cursor: null },
    });
    transport.fetchAgentState = async () => {
      throw new AgentTransportError({
        status: 500,
        code: "HTTP_500",
        message: "boom",
        recoverable: true,
      });
    };

    const result = await hydrateAgentSession({ transport }, new LiveEventBuffer());
    expect(result.outcome).toBe("failed");
    expect(result.error).toBeInstanceOf(AgentTransportError);
  });
});

describe("SequenceGapTracker", () => {
  it("applies contiguous events immediately", () => {
    const tracker = new SequenceGapTracker();
    tracker.reset({ runId: "run_1", streamEpoch: 0, sequenceNumber: STREAM_START });

    expect(tracker.accept(envelope(0))).toEqual({
      type: "apply",
      events: [expect.objectContaining({ sequenceNumber: 0 })],
    });
    expect(tracker.accept(envelope(1))).toEqual({
      type: "apply",
      events: [expect.objectContaining({ sequenceNumber: 1 })],
    });
  });

  it("asks for a backfill on a gap and releases the buffer once filled", () => {
    const tracker = new SequenceGapTracker();
    tracker.reset({ runId: "run_1", streamEpoch: 0, sequenceNumber: 0 });

    const action = tracker.accept(envelope(3));
    expect(action.type).toBe("backfill");
    if (action.type === "backfill") {
      expect(action.gap.after).toBe(0);
      expect(action.gap.waitingOn).toBe(3);
    }

    const released = tracker.fill([envelope(1), envelope(2)]);
    expect(released.map((e) => e.sequenceNumber)).toEqual([1, 2, 3]);
    expect(tracker.currentGap).toBeNull();
  });

  it("gives up on a gap that never fills instead of freezing forever", () => {
    // The bug this replaces: strict contiguous buffering with no escape,
    // where one missing sequence number stalls every later event.
    let now = 0;
    const tracker = new SequenceGapTracker({ timeoutMs: 1000, now: () => now });
    tracker.reset({ runId: "run_1", streamEpoch: 0, sequenceNumber: 0 });

    expect(tracker.accept(envelope(5)).type).toBe("backfill");
    now = 1500;
    expect(tracker.accept(envelope(6))).toEqual({
      type: "resnapshot",
      reason: "gap-timeout",
    });
  });

  it("does not wait for sequence numbers a new epoch will never emit", () => {
    const tracker = new SequenceGapTracker();
    tracker.reset({ runId: "run_1", streamEpoch: 0, sequenceNumber: 4 });

    // Sequence 0 of epoch 1 is *lower* than the epoch-0 cursor. Treating it
    // as a duplicate would silently drop a whole new run.
    const action = tracker.accept(envelope(0, "run.started", 1));
    expect(action).toEqual({
      type: "apply",
      events: [expect.objectContaining({ streamEpoch: 1 })],
    });
  });

  it("suppresses duplicates and stale epochs", () => {
    const tracker = new SequenceGapTracker();
    tracker.reset({ runId: "run_1", streamEpoch: 1, sequenceNumber: 2 });

    expect(tracker.accept(envelope(2, "text.delta", 1))).toEqual({
      type: "apply",
      events: [],
    });
    expect(tracker.accept(envelope(9, "text.delta", 0))).toEqual({
      type: "apply",
      events: [],
    });
  });

  it("drops a replayed copy whose sequence number drifted", () => {
    // Durable executors re-publish journaled events on replay; when the
    // replay's allocation order differs, the copy arrives with a FRESH
    // sequence number and the sequence checks alone cannot recognize it.
    const tracker = new SequenceGapTracker();
    tracker.reset({ runId: "run_1", streamEpoch: 0, sequenceNumber: STREAM_START });

    expect(tracker.accept(envelope(0, "run.started")).type).toBe("apply");
    tracker.accept(envelope(1));
    tracker.accept(envelope(2, "run.completed"));
    tracker.accept(envelope(3, "stream.ended"));

    // The replay's copy of run.started: same event id, drifted sequence.
    const replayed = { ...envelope(7, "run.started"), eventId: "run_1:0:0" };
    expect(tracker.accept(replayed)).toEqual({ type: "apply", events: [] });
  });

  it("forgets applied ids when a new epoch supersedes the run", () => {
    const tracker = new SequenceGapTracker();
    tracker.reset({ runId: "run_1", streamEpoch: 0, sequenceNumber: STREAM_START });
    tracker.accept(envelope(0, "run.started"));

    // Epoch 1 restarts the sequence at 0; its events may share the same id
    // shape as epoch 0's and must still apply.
    const action = tracker.accept(envelope(0, "run.started", 1));
    expect(action).toEqual({
      type: "apply",
      events: [expect.objectContaining({ streamEpoch: 1 })],
    });
  });
});

describe("LiveEventBuffer", () => {
  it("stops buffering after a drain", () => {
    const buffer = new LiveEventBuffer();
    expect(buffer.push(envelope(0))).toBe(true);
    buffer.drain(0);
    expect(buffer.push(envelope(1))).toBe(false);
  });

  it("dedupes against events already applied", () => {
    const buffer = new LiveEventBuffer();
    buffer.push(envelope(0));
    buffer.push(envelope(1));
    buffer.markApplied([envelope(0)]);
    expect(buffer.drain(0).map((e) => e.sequenceNumber)).toEqual([1]);
  });
});

describe("sortEnvelopes", () => {
  it("orders by epoch, then sequence, then event id", () => {
    const ordered = sortEnvelopes([
      envelope(1, "text.delta", 1),
      envelope(5, "text.delta", 0),
      envelope(0, "text.delta", 1),
    ]);
    expect(ordered.map((e) => [e.streamEpoch, e.sequenceNumber])).toEqual([
      [0, 5],
      [1, 0],
      [1, 1],
    ]);
  });
});

describe("envelopeKey", () => {
  it("falls back to run/epoch/sequence when a runtime predates event ids", () => {
    const legacy = { ...envelope(3) } as StandardEventEnvelope;
    delete (legacy as { eventId?: string }).eventId;
    expect(envelopeKey(legacy)).toBe("run_1:0:3");
  });
});

describe("hydration restart", () => {
  it("restarts when the active run changed under the fetch", async () => {
    const first: AgentStateResponse = {
      snapshot: snapshot(),
      messages: [],
      cursor: { runId: "run_1", streamEpoch: 0, sequenceNumber: STREAM_START },
    };
    const second: AgentStateResponse = {
      snapshot: snapshot({
        activeRun: {
          runId: "run_2",
          lifecycle: "executing",
          acceptedAt: "2026-08-22T10:05:00.000Z",
        },
        cursor: { runId: "run_2", streamEpoch: 1, sequenceNumber: STREAM_START },
      }),
      messages: [],
      cursor: { runId: "run_2", streamEpoch: 1, sequenceNumber: STREAM_START },
    };

    const responses = [first, second, second, second];
    const fetchAgentState = vi.fn(async () => responses.shift() ?? second);
    const transport: IAgentSessionTransport = {
      fetchAgentState,
      async fetchEventTail({ after }) {
        return { events: [], next: after, hasMore: false };
      },
      async executeCommand() {
        throw new Error("not used");
      },
    };

    const result = await hydrateAgentSession({ transport }, new LiveEventBuffer());
    expect(result.outcome).toBe("restarted");
    expect(result.snapshot?.activeRun?.runId).toBe("run_2");
  });

  it("restarts when the active run completes while the tail is draining", async () => {
    const active: AgentStateResponse = {
      snapshot: snapshot(),
      messages: [],
      cursor: { runId: "run_1", streamEpoch: 0, sequenceNumber: STREAM_START },
    };
    const idle: AgentStateResponse = {
      snapshot: snapshot({ activeRun: null, cursor: null, revision: 8 }),
      messages: [{ role: "assistant", content: "done" } as never],
      cursor: null,
    };
    const responses = [active, idle, idle];
    const transport: IAgentSessionTransport = {
      async fetchAgentState() {
        return responses.shift() ?? idle;
      },
      async fetchEventTail({ after }) {
        return { events: [], next: after, hasMore: false };
      },
      async executeCommand() {
        throw new Error("not used");
      },
    };

    const result = await hydrateAgentSession({ transport }, new LiveEventBuffer());
    expect(result.outcome).toBe("restarted");
    expect(result.snapshot?.activeRun).toBeNull();
    expect(result.messages?.[0]).toMatchObject({ content: "done" });
  });
});
