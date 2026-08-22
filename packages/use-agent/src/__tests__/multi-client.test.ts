import { describe, it, expect } from "vitest";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { reduceStreamingState } from "../core/index.js";
import {
  hydrateAgentSession,
  LiveEventBuffer,
  SequenceGapTracker,
  sortEnvelopes,
} from "../core/services/hydration.js";
import { STREAM_START } from "../core/ports/agent-session.js";
import type {
  IAgentSessionTransport,
  StreamCursor,
  EventTailPage,
  AgentStateResponse,
} from "../core/ports/agent-session.js";
import type {
  AgentStateSnapshot,
  StandardEventEnvelope,
  StreamingState,
  StreamingAction,
  AgentKitEvent,
  ToolManifest,
} from "../types/index.js";

/**
 * Multi-client convergence.
 *
 * The promise this runtime makes is that the conversation belongs to the
 * user, not to the tab that started it. A second tab, a phone picked up
 * mid-turn, a laptop that slept through the interesting part, and the
 * original tab that never dropped a packet must all end up looking at the
 * same transcript.
 *
 * These tests drive several clients through the SAME event stream — the
 * golden fixtures the Go runtime generates — with different histories of
 * disconnection, and assert their reduced transcripts are identical. The
 * comparison is a projection rather than deep state equality: the reducer
 * keeps buffers and timestamps that legitimately differ between clients, and
 * asserting on those would test bookkeeping instead of what the user sees.
 */

const FIXTURES_DIR = join(__dirname, "../../../../contracts/fixtures");

interface FixtureFile {
  name: string;
  events: StandardEventEnvelope[];
}

function loadFixture(name: string): FixtureFile {
  return JSON.parse(
    readFileSync(join(FIXTURES_DIR, `${name}.json`), "utf8")
  ) as FixtureFile;
}

const fixtureNames = readdirSync(FIXTURES_DIR)
  .filter((f) => f.endsWith(".json"))
  .map((f) => f.replace(/\.json$/, ""))
  .sort();

type TestManifest = ToolManifest;

function emptyState(threadId: string): StreamingState<TestManifest> {
  return {
    threads: {},
    currentThreadId: threadId,
    lastProcessedIndex: 0,
    isConnected: true,
  } as StreamingState<TestManifest>;
}

function reduce(
  state: StreamingState<TestManifest>,
  events: StandardEventEnvelope[]
): StreamingState<TestManifest> {
  let next = state;
  for (const event of events) {
    next = reduceStreamingState(
      next,
      {
        type: "REALTIME_MESSAGES_RECEIVED",
        messages: [event as unknown as AgentKitEvent<TestManifest>],
      } as StreamingAction<TestManifest>,
      false
    );
  }
  return next;
}

/**
 * The user-visible projection of a reduced state: which messages exist, in
 * order, with which parts, content and status. Everything a second client
 * must agree with the first about.
 */
interface Transcript {
  messages: Array<{
    id: string;
    role: string;
    parts: Array<{ type: string; id: string; content: string; status?: string }>;
  }>;
}

function transcriptOf(state: StreamingState<TestManifest>): Transcript {
  const threadIds = Object.keys(state.threads).sort();
  const messages: Transcript["messages"] = [];
  for (const threadId of threadIds) {
    for (const message of state.threads[threadId].messages ?? []) {
      messages.push({
        id: message.id,
        role: message.role,
        parts: (message.parts ?? []).map((part) => {
          const anyPart = part as Record<string, unknown>;
          const content =
            typeof anyPart.content === "string"
              ? anyPart.content
              : anyPart.content !== undefined
                ? JSON.stringify(anyPart.content)
                : typeof anyPart.input === "string"
                  ? anyPart.input
                  : anyPart.input !== undefined
                    ? JSON.stringify(anyPart.input)
                    : "";
          return {
            type: part.type,
            id: (anyPart.id as string) ?? "",
            content,
            status: anyPart.status as string | undefined,
          };
        }),
      });
    }
  }
  return { messages };
}

/**
 * A session transport backed by a fixture's event stream, standing in for the
 * server's snapshot and tail endpoints.
 *
 * `availableUpTo` models how much of the run has happened when a client
 * arrives; `retainedFrom` models retention having dropped the earliest
 * events.
 */
function fixtureTransport(config: {
  events: StandardEventEnvelope[];
  threadId: string;
  snapshotCursorSequence?: number;
  availableUpTo?: number;
  retainedFrom?: number;
  pageSize?: number;
  terminal?: boolean;
}): IAgentSessionTransport & { tailCalls: number } {
  const {
    events,
    threadId,
    snapshotCursorSequence = STREAM_START,
    availableUpTo = Number.POSITIVE_INFINITY,
    retainedFrom = -1,
    pageSize = 4,
    terminal = false,
  } = config;

  const snapshot: AgentStateSnapshot = {
    schemaVersion: 1,
    sessionId: "session_1",
    currentThreadId: threadId,
    activeRun: terminal
      ? null
      : {
          runId: "run_1",
          lifecycle: "executing",
          acceptedAt: "2026-08-22T10:00:00.000Z",
        },
    pause: { state: "none", accumulatedPausedMs: 0 },
    activity: { kind: "responding" },
    approval: { status: "none" },
    revision: 4,
    cursor: terminal
      ? null
      : {
          runId: "run_1",
          streamEpoch: 0,
          sequenceNumber: snapshotCursorSequence,
        },
    reconcileRequired: false,
  };

  const transport = {
    tailCalls: 0,
    async fetchAgentState(): Promise<AgentStateResponse> {
      return {
        snapshot,
        // Canonical history is empty here on purpose: these fixtures are one
        // in-flight turn, so everything the client renders must come from the
        // durable tail. That makes the tail the thing under test.
        messages: [],
        cursor: snapshot.cursor,
      };
    },
    async fetchEventTail({
      after,
      limit,
    }: {
      after: StreamCursor;
      limit?: number;
    }): Promise<EventTailPage> {
      transport.tailCalls++;
      if (after.sequenceNumber < retainedFrom) {
        return { events: [], next: after, hasMore: false, retentionGap: true };
      }
      const eligible = events.filter(
        (event) =>
          event.sequenceNumber > after.sequenceNumber &&
          event.sequenceNumber <= availableUpTo
      );
      const size = limit ?? pageSize;
      const page = eligible.slice(0, size);
      const last = page[page.length - 1];
      return {
        events: page,
        next: last
          ? { runId: "run_1", streamEpoch: 0, sequenceNumber: last.sequenceNumber }
          : after,
        hasMore: eligible.length > page.length,
      };
    },
    async executeCommand() {
      throw new Error("not used");
    },
  };

  return transport as IAgentSessionTransport & { tailCalls: number };
}

function threadIdOf(events: StandardEventEnvelope[]): string {
  const withThread = events.find((e) => typeof e.data?.threadId === "string");
  return (withThread?.data.threadId as string) ?? "thread_1";
}

/** The baseline: a client that was connected for the whole run. */
function liveClient(events: StandardEventEnvelope[]): Transcript {
  const threadId = threadIdOf(events);
  return transcriptOf(reduce(emptyState(threadId), sortEnvelopes(events)));
}

/**
 * Guards against a vacuous comparison. Two clients that both rendered nothing
 * are trivially equal, which would make every convergence test below pass
 * while proving nothing.
 */
function assertRendersSomething(transcript: Transcript, name: string): void {
  const parts = transcript.messages.flatMap((m) => m.parts);
  expect(
    parts.length,
    `${name}: the baseline client rendered no parts, so convergence is vacuous`
  ).toBeGreaterThan(0);
}

describe("multi-client convergence", () => {
  for (const name of fixtureNames) {
    const fixture = loadFixture(name);
    const threadId = threadIdOf(fixture.events);

    describe(name, () => {
      it("a client that joins after the run converges with one that watched it live", async () => {
        // The initiating tab is gone. Nothing about recovery may depend on it.
        const expected = liveClient(fixture.events);
        assertRendersSomething(expected, name);

        const transport = fixtureTransport({
          events: fixture.events,
          threadId,
          pageSize: 3,
        });
        const buffer = new LiveEventBuffer();
        const result = await hydrateAgentSession({ transport }, buffer);

        expect(result.outcome).toBe("hydrated");
        const joined = transcriptOf(reduce(emptyState(threadId), result.events));
        expect(joined).toEqual(expected);
      });

      it("a client that drops mid-run and reconnects converges", async () => {
        const expected = liveClient(fixture.events);
        const ordered = sortEnvelopes(fixture.events);
        const cut = Math.floor(ordered.length / 2);
        const beforeDrop = ordered.slice(0, cut);
        const lastSeen = beforeDrop[beforeDrop.length - 1].sequenceNumber;

        // Live until the socket dies.
        let state = reduce(emptyState(threadId), beforeDrop);

        // Reconnect: snapshot from the last applied cursor, then tail.
        const transport = fixtureTransport({
          events: fixture.events,
          threadId,
          snapshotCursorSequence: lastSeen,
          pageSize: 2,
        });
        const buffer = new LiveEventBuffer();
        const result = await hydrateAgentSession({ transport }, buffer);
        state = reduce(state, result.events);

        expect(transcriptOf(state)).toEqual(expected);
      });

      it("a client whose live events race hydration applies each exactly once", async () => {
        const expected = liveClient(fixture.events);
        const ordered = sortEnvelopes(fixture.events);
        const overlapFrom = Math.floor(ordered.length / 3);

        const buffer = new LiveEventBuffer();
        // The socket is already delivering while the snapshot is in flight.
        for (const event of ordered.slice(overlapFrom)) buffer.push(event);

        const transport = fixtureTransport({
          events: fixture.events,
          threadId,
          pageSize: 5,
        });
        const result = await hydrateAgentSession({ transport }, buffer);

        // Duplicates would double every delta; drops would truncate the turn.
        const ids = result.events.map((e) => e.eventId);
        expect(new Set(ids).size).toBe(ids.length);
        expect(transcriptOf(reduce(emptyState(threadId), result.events))).toEqual(
          expected
        );
      });
    });
  }
});

describe("gap recovery converges", () => {
  const fixture = loadFixture("tool-turn");
  const threadId = threadIdOf(fixture.events);
  const ordered = sortEnvelopes(fixture.events);

  it("a client that misses events in the middle backfills to the same transcript", async () => {
    const expected = liveClient(fixture.events);

    const tracker = new SequenceGapTracker({ timeoutMs: 10_000 });
    tracker.reset({ runId: "run_1", streamEpoch: 0, sequenceNumber: STREAM_START });

    const transport = fixtureTransport({ events: fixture.events, threadId });
    let state = emptyState(threadId);

    // Deliver everything except a contiguous run in the middle — the shape a
    // dropped WebSocket frame leaves behind.
    const holeStart = 2;
    const holeEnd = 4;
    for (const event of ordered) {
      if (
        event.sequenceNumber >= holeStart &&
        event.sequenceNumber <= holeEnd
      ) {
        continue;
      }
      const action = tracker.accept(event);
      if (action.type === "apply") {
        state = reduce(state, action.events);
      } else if (action.type === "backfill") {
        const page = await transport.fetchEventTail({
          threadId,
          after: {
            runId: action.gap.runId,
            streamEpoch: action.gap.streamEpoch,
            sequenceNumber: action.gap.after,
          },
          limit: 50,
        });
        state = reduce(state, tracker.fill(page.events));
      }
    }

    expect(transcriptOf(state)).toEqual(expected);
  });

  it("a retention gap stops the drain instead of waiting forever", async () => {
    const transport = fixtureTransport({
      events: fixture.events,
      threadId,
      retainedFrom: 3,
    });
    const result = await hydrateAgentSession({ transport }, new LiveEventBuffer());

    // Waiting for events the server has already deleted is how a transcript
    // freezes permanently; the client is told to reconcile instead.
    expect(result.reconcileRequired).toBe(true);
    expect(result.outcome).toBe("hydrated");
  });
});

describe("terminal state", () => {
  it("a client arriving after the terminal reads no tail at all", async () => {
    const fixture = loadFixture("text-turn");
    const transport = fixtureTransport({
      events: fixture.events,
      threadId: threadIdOf(fixture.events),
      terminal: true,
    });

    const result = await hydrateAgentSession({ transport }, new LiveEventBuffer());

    // A finished run's content lives in canonical history. Replaying its
    // deltas would rebuild a message history already holds.
    expect(transport.tailCalls).toBe(0);
    expect(result.events).toEqual([]);
    expect(result.snapshot?.activeRun).toBeNull();
  });
});

describe("accepted user turn", () => {
  const fixture = loadFixture("text-turn");
  const threadId = threadIdOf(fixture.events);
  const runStarted = fixture.events.find((e) => e.event === "run.started")!;
  const accepted = fixture.events.find((e) => e.event === "user.message")!;
  // A tail always begins at the run's start, so the epoch is anchored before
  // any content event is reduced.
  const opening = [runStarted, accepted];

  it("renders on a client that never sent the message", () => {
    // Without this, a second tab watches the assistant answer a question it
    // never saw asked.
    const state = reduce(emptyState(threadId), opening);
    const transcript = transcriptOf(state);

    expect(transcript.messages).toHaveLength(1);
    expect(transcript.messages[0].role).toBe("user");
    expect(transcript.messages[0].id).toBe(accepted.data.messageId);
    expect(transcript.messages[0].parts[0].content).toBe("hello");
  });

  it("converges the sending tab's optimistic message instead of duplicating it", () => {
    const messageId = accepted.data.messageId as string;
    // The initiating tab rendered the message locally before the server
    // confirmed it, under the id the server was given.
    let state = reduceStreamingState(
      emptyState(threadId),
      {
        type: "MESSAGE_SENT",
        threadId,
        messageId,
        message: "hello",
      } as unknown as StreamingAction<TestManifest>,
      false
    );
    expect(transcriptOf(state).messages).toHaveLength(1);

    state = reduce(state, opening);

    const transcript = transcriptOf(state);
    expect(transcript.messages).toHaveLength(1);
    expect(transcript.messages[0].id).toBe(messageId);
  });

  it("is idempotent across a live delivery and a backfill of the same event", () => {
    // The same accepted turn commonly arrives twice: once live, once in the
    // tail a reconnect replays.
    const state = reduce(emptyState(threadId), [...opening, accepted]);
    expect(transcriptOf(state).messages).toHaveLength(1);
  });
});
