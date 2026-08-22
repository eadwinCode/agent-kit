/* @vitest-environment jsdom */
import { describe, it, expect, vi } from "vitest";
import { renderHook, act, waitFor } from "@testing-library/react";
import { useAgentSession } from "../use-agent-session.js";
import { STREAM_START } from "../../../../core/ports/agent-session.js";
import type {
  IAgentSessionTransport,
  AgentStateResponse,
  EventTailPage,
  StreamCursor,
} from "../../../../core/ports/agent-session.js";
import type {
  AgentStateSnapshot,
  StandardEventEnvelope,
} from "../../../../types/index.js";
import { AgentTransportError, AgentErrorCodes } from "../../../../core/errors/agent-transport-error.js";

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
    pause: { state: "none", accumulatedPausedMs: 0, epoch: 3 },
    activity: { kind: "responding" },
    approval: { status: "none" },
    revision: 5,
    cursor: { runId: "run_1", streamEpoch: 0, sequenceNumber: STREAM_START },
    reconcileRequired: false,
    ...overrides,
  };
}

function envelope(seq: number, epoch = 0): StandardEventEnvelope {
  return {
    event: "text.delta",
    data: { runId: "run_1", threadId: "thread_1", delta: `d${seq}` },
    timestamp: 1_700_000_000_000 + seq,
    sequenceNumber: seq,
    id: `publish-${seq}:text.delta`,
    eventId: `run_1:${epoch}:${seq}`,
    streamEpoch: epoch,
    schemaVersion: 1,
  };
}

interface HarnessConfig {
  state?: AgentStateResponse;
  pages?: EventTailPage[];
  onCommand?: (command: unknown) => unknown;
}

function harness(config: HarnessConfig = {}) {
  const pages = [...(config.pages ?? [])];
  const commands: unknown[] = [];
  const tailRequests: StreamCursor[] = [];

  const transport: IAgentSessionTransport = {
    async fetchAgentState() {
      return (
        config.state ?? {
          snapshot: snapshot(),
          messages: [],
          cursor: { runId: "run_1", streamEpoch: 0, sequenceNumber: STREAM_START },
        }
      );
    },
    async fetchEventTail({ after }) {
      tailRequests.push(after);
      return pages.shift() ?? { events: [], next: after, hasMore: false };
    },
    async executeCommand(command) {
      commands.push(command);
      const result = config.onCommand?.(command);
      if (result instanceof Error) throw result;
      return (result as never) ?? { snapshot: snapshot({ revision: 6 }) };
    },
  };

  return { transport, commands, tailRequests };
}

describe("useAgentSession", () => {
  it("hydrates from the snapshot and installs canonical history", async () => {
    const onMessages = vi.fn();
    const onEvents = vi.fn();
    const { transport } = harness({
      state: {
        snapshot: snapshot(),
        messages: [{ role: "user", content: "hi" } as never],
        cursor: { runId: "run_1", streamEpoch: 0, sequenceNumber: STREAM_START },
      },
      pages: [
        {
          events: [envelope(0), envelope(1)],
          next: { runId: "run_1", streamEpoch: 0, sequenceNumber: 1 },
          hasMore: false,
        },
      ],
    });

    const { result } = renderHook(() =>
      useAgentSession({ transport, onMessages, onEvents })
    );

    await waitFor(() => expect(result.current.status).toBe("ready"));
    expect(result.current.snapshot?.revision).toBe(5);
    expect(onMessages).toHaveBeenCalledTimes(1);
    expect(onEvents.mock.calls[0][0].map((e: StandardEventEnvelope) => e.sequenceNumber)).toEqual([
      0, 1,
    ]);
  });

  it("keeps action identity stable across renders", async () => {
    // Unstable callbacks re-trigger effects in consuming components, which is
    // how one extra render becomes one extra command.
    const { transport } = harness();
    const { result, rerender } = renderHook(() => useAgentSession({ transport }));

    await waitFor(() => expect(result.current.status).toBe("ready"));
    const before = {
      pause: result.current.pause,
      resume: result.current.resume,
      cancel: result.current.cancel,
      ingest: result.current.ingest,
      hydrate: result.current.hydrate,
    };

    rerender();
    rerender();

    expect(result.current.pause).toBe(before.pause);
    expect(result.current.resume).toBe(before.resume);
    expect(result.current.cancel).toBe(before.cancel);
    expect(result.current.ingest).toBe(before.ingest);
    expect(result.current.hydrate).toBe(before.hydrate);
  });

  it("buffers live envelopes that arrive during hydration and reduces them once", async () => {
    let releaseState: (value: AgentStateResponse) => void = () => {};
    const pending = new Promise<AgentStateResponse>((resolve) => {
      releaseState = resolve;
    });
    const onEvents = vi.fn();

    const transport: IAgentSessionTransport = {
      fetchAgentState: () => pending,
      async fetchEventTail({ after }) {
        return {
          events: [envelope(0)],
          next: { runId: "run_1", streamEpoch: 0, sequenceNumber: 0 },
          hasMore: false,
        };
      },
      async executeCommand() {
        throw new Error("not used");
      },
    };

    const { result } = renderHook(() => useAgentSession({ transport, onEvents }));

    act(() => {
      result.current.ingest(envelope(1));
    });
    // Nothing may reach the reducer before history is installed, or the
    // delta lands on a transcript that is about to be replaced.
    expect(onEvents).not.toHaveBeenCalled();

    await act(async () => {
      releaseState({
        snapshot: snapshot(),
        messages: [],
        cursor: { runId: "run_1", streamEpoch: 0, sequenceNumber: STREAM_START },
      });
      await pending;
    });

    await waitFor(() => expect(result.current.status).toBe("ready"));
    const delivered = onEvents.mock.calls.flatMap(
      (call) => call[0] as StandardEventEnvelope[]
    );
    expect(delivered.map((e) => e.sequenceNumber)).toEqual([0, 1]);
  });

  it("abandons an in-flight hydration when the transport and scope change", async () => {
    let releaseOld: (value: AgentStateResponse) => void = () => {};
    const oldState = new Promise<AgentStateResponse>((resolve) => {
      releaseOld = resolve;
    });
    const oldTransport: IAgentSessionTransport = {
      fetchAgentState: () => oldState,
      async fetchEventTail({ after }) {
        return { events: [], next: after, hasMore: false };
      },
      async executeCommand() {
        throw new Error("not used");
      },
    };
    const fresh = snapshot({
      sessionId: "session_2",
      currentThreadId: "thread_2",
      activeRun: null,
      cursor: null,
      revision: 11,
    });
    const newTransport: IAgentSessionTransport = {
      async fetchAgentState() {
        return { snapshot: fresh, messages: [], cursor: null };
      },
      async fetchEventTail({ after }) {
        return { events: [], next: after, hasMore: false };
      },
      async executeCommand() {
        throw new Error("not used");
      },
    };

    const { result, rerender } = renderHook(
      ({ transport, scope }) => useAgentSession({ transport, scope }),
      { initialProps: { transport: oldTransport, scope: "owner-a" } }
    );
    rerender({ transport: newTransport, scope: "owner-b" });
    await waitFor(() => expect(result.current.snapshot?.sessionId).toBe("session_2"));

    await act(async () => {
      releaseOld({ snapshot: snapshot(), messages: [], cursor: null });
      await oldState;
    });
    expect(result.current.snapshot?.sessionId).toBe("session_2");
  });

  it("does not carry a resume cursor across opaque scope changes", async () => {
    const asked: StreamCursor[] = [];
    const transport: IAgentSessionTransport = {
      async fetchAgentState() {
        return {
          snapshot: snapshot(),
          messages: [],
          cursor: {
            runId: "run_1",
            streamEpoch: 0,
            sequenceNumber: STREAM_START,
          },
        };
      },
      async fetchEventTail({ after }) {
        asked.push(after);
        return { events: [], next: after, hasMore: false };
      },
      async executeCommand() {
        throw new Error("not used");
      },
    };
    const { result, rerender } = renderHook(
      ({ scope }) => useAgentSession({ transport, scope }),
      { initialProps: { scope: "owner-a" } }
    );
    await waitFor(() => expect(result.current.status).toBe("ready"));
    act(() => result.current.ingest(envelope(0)));

    rerender({ scope: "owner-b" });
    await waitFor(() => expect(asked.length).toBeGreaterThan(1));
    expect(asked[asked.length - 1].sequenceNumber).toBe(STREAM_START);
  });

  it("cannot apply a hydration result after the hook is disabled", async () => {
    let release: (value: AgentStateResponse) => void = () => {};
    const pending = new Promise<AgentStateResponse>((resolve) => {
      release = resolve;
    });
    const transport: IAgentSessionTransport = {
      fetchAgentState: () => pending,
      async fetchEventTail({ after }) {
        return { events: [], next: after, hasMore: false };
      },
      async executeCommand() {
        throw new Error("not used");
      },
    };
    const { result, rerender } = renderHook(
      ({ enabled }) => useAgentSession({ transport, enabled }),
      { initialProps: { enabled: true } }
    );
    rerender({ enabled: false });
    await act(async () => {
      release({ snapshot: snapshot(), messages: [], cursor: null });
      await pending;
    });
    expect(result.current.status).toBe("idle");
    expect(result.current.snapshot).toBeNull();
  });

  it("backfills a live sequence gap instead of stalling", async () => {
    const { transport, tailRequests } = harness({
      pages: [
        // Initial hydration: nothing yet.
        { events: [], next: { runId: "run_1", streamEpoch: 0, sequenceNumber: STREAM_START }, hasMore: false },
        // Backfill for the hole.
        {
          events: [envelope(0), envelope(1)],
          next: { runId: "run_1", streamEpoch: 0, sequenceNumber: 1 },
          hasMore: false,
        },
      ],
    });
    const onEvents = vi.fn();
    const { result } = renderHook(() => useAgentSession({ transport, onEvents }));
    await waitFor(() => expect(result.current.status).toBe("ready"));

    act(() => {
      result.current.ingest(envelope(2));
    });

    await waitFor(() => expect(tailRequests.length).toBeGreaterThan(1));
    await waitFor(() => {
      const delivered = onEvents.mock.calls.flatMap(
        (call) => call[0] as StandardEventEnvelope[]
      );
      expect(delivered.map((e) => e.sequenceNumber)).toEqual([0, 1, 2]);
    });
  });

  it("re-snapshots when the tail reports a retention gap", async () => {
    let stateCalls = 0;
    const transport: IAgentSessionTransport = {
      async fetchAgentState() {
        stateCalls++;
        return {
          snapshot: snapshot(),
          messages: [],
          cursor: { runId: "run_1", streamEpoch: 0, sequenceNumber: STREAM_START },
        };
      },
      async fetchEventTail({ after }) {
        if (stateCalls === 1 && after.sequenceNumber === STREAM_START) {
          return { events: [], next: after, hasMore: false };
        }
        return { events: [], next: after, hasMore: false, retentionGap: true };
      },
      async executeCommand() {
        throw new Error("not used");
      },
    };

    const { result } = renderHook(() => useAgentSession({ transport }));
    await waitFor(() => expect(result.current.status).toBe("ready"));
    const before = stateCalls;

    act(() => {
      result.current.ingest(envelope(9));
    });

    await waitFor(() => expect(result.current.reconcileRequired).toBe(true));
    await waitFor(() => expect(stateCalls).toBeGreaterThan(before));
  });

  it("sends idempotent commands carrying the current revision", async () => {
    const { transport, commands } = harness();
    const { result } = renderHook(() => useAgentSession({ transport }));
    await waitFor(() => expect(result.current.status).toBe("ready"));

    await act(async () => {
      await result.current.pause();
    });

    expect(commands).toHaveLength(1);
    expect(commands[0]).toMatchObject({
      type: "pause",
      threadId: "thread_1",
      runId: "run_1",
      expectedRevision: 5,
    });
    expect((commands[0] as { commandId: string }).commandId).toBeTruthy();
  });

  it("correlates resume with the pause epoch so a stale resume cannot wake a later pause", async () => {
    const { transport, commands } = harness({
      state: {
        snapshot: snapshot({
          pause: { state: "paused", accumulatedPausedMs: 100, epoch: 7 },
        }),
        messages: [],
        cursor: { runId: "run_1", streamEpoch: 0, sequenceNumber: STREAM_START },
      },
    });
    const { result } = renderHook(() => useAgentSession({ transport }));
    await waitFor(() => expect(result.current.status).toBe("ready"));

    await act(async () => {
      await result.current.resume();
    });

    expect(commands[0]).toMatchObject({ type: "resume", pauseEpoch: 7 });
  });

  it("reconciles to the authoritative snapshot when a command loses a revision race", async () => {
    const authoritative = snapshot({ revision: 42 });
    const { transport } = harness({
      onCommand: () =>
        new AgentTransportError({
          status: 409,
          code: AgentErrorCodes.StateRevisionMismatch,
          message: "state changed",
          recoverable: true,
          snapshot: authoritative,
        }),
    });

    const { result } = renderHook(() => useAgentSession({ transport }));
    await waitFor(() => expect(result.current.status).toBe("ready"));

    await act(async () => {
      await expect(result.current.pause()).rejects.toMatchObject({
        code: AgentErrorCodes.StateRevisionMismatch,
      });
    });

    // The conflict handed back the truth; the hook adopted it rather than
    // leaving the UI on a revision the server has moved past.
    expect(result.current.snapshot?.revision).toBe(42);
  });

  it("re-hydrates when the socket comes back", async () => {
    let stateCalls = 0;
    const transport: IAgentSessionTransport = {
      async fetchAgentState() {
        stateCalls++;
        return {
          snapshot: snapshot(),
          messages: [],
          cursor: { runId: "run_1", streamEpoch: 0, sequenceNumber: STREAM_START },
        };
      },
      async fetchEventTail({ after }) {
        return { events: [], next: after, hasMore: false };
      },
      async executeCommand() {
        throw new Error("not used");
      },
    };

    const { result } = renderHook(() => useAgentSession({ transport }));
    await waitFor(() => expect(result.current.status).toBe("ready"));
    const before = stateCalls;

    act(() => {
      result.current.reportConnectionState("closed");
    });
    expect(result.current.connectionState).toBe("disconnected");

    act(() => {
      result.current.reportConnectionState("active");
    });
    // The run kept going while the socket was gone; the tail we missed is
    // exactly what the user cannot see.
    await waitFor(() => expect(stateCalls).toBeGreaterThan(before));
    expect(result.current.connectionState).toBe("connected");
  });

  it("surfaces a hydration failure as an error status", async () => {
    const transport: IAgentSessionTransport = {
      async fetchAgentState() {
        throw new AgentTransportError({
          status: 500,
          code: "HTTP_500",
          message: "boom",
          recoverable: true,
        });
      },
      async fetchEventTail({ after }) {
        return { events: [], next: after, hasMore: false };
      },
      async executeCommand() {
        throw new Error("not used");
      },
    };

    const onError = vi.fn();
    const { result } = renderHook(() => useAgentSession({ transport, onError }));
    await waitFor(() => expect(result.current.status).toBe("error"));
    expect(onError).toHaveBeenCalled();
  });

  it("stays dormant when disabled", async () => {
    let stateCalls = 0;
    const transport: IAgentSessionTransport = {
      async fetchAgentState() {
        stateCalls++;
        return { snapshot: snapshot(), messages: [], cursor: null };
      },
      async fetchEventTail({ after }) {
        return { events: [], next: after, hasMore: false };
      },
      async executeCommand() {
        throw new Error("not used");
      },
    };

    const { result } = renderHook(() =>
      useAgentSession({ transport, enabled: false })
    );
    await new Promise((r) => setTimeout(r, 10));
    expect(stateCalls).toBe(0);
    expect(result.current.status).toBe("idle");
  });
});

describe("useAgentSession resume cursor", () => {
  it("resumes from what it already applied instead of replaying the turn", async () => {
    const asked: StreamCursor[] = [];
    const transport: IAgentSessionTransport = {
      async fetchAgentState() {
        return {
          snapshot: snapshot(),
          messages: [],
          // The server always points a NEWCOMER at the start of the epoch.
          cursor: { runId: "run_1", streamEpoch: 0, sequenceNumber: STREAM_START },
        };
      },
      async fetchEventTail({ after }) {
        asked.push(after);
        if (after.sequenceNumber === STREAM_START) {
          return {
            events: [envelope(0), envelope(1)],
            next: { runId: "run_1", streamEpoch: 0, sequenceNumber: 1 },
            hasMore: false,
          };
        }
        return { events: [], next: after, hasMore: false };
      },
      async executeCommand() {
        throw new Error("not used");
      },
    };

    const { result } = renderHook(() => useAgentSession({ transport }));
    await waitFor(() => expect(result.current.status).toBe("ready"));
    expect(asked[0].sequenceNumber).toBe(STREAM_START);

    // Apply two more live events, then lose the socket and come back.
    act(() => {
      result.current.ingest(envelope(2));
      result.current.ingest(envelope(3));
    });
    act(() => {
      result.current.reportConnectionState("closed");
    });
    act(() => {
      result.current.reportConnectionState("active");
    });

    await waitFor(() => expect(asked.length).toBeGreaterThan(1));
    // Re-replaying from the epoch start would re-reduce a turn already on
    // screen; the client resumes from its own last-applied position.
    expect(asked[asked.length - 1].sequenceNumber).toBe(3);
  });
});
