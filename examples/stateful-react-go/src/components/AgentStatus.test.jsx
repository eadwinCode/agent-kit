import { describe, expect, it } from "vitest";
import { deriveAgentStatus } from "./AgentStatus.jsx";

const base = {
  sessionStatus: "ready",
  connectionState: "connected",
  events: [],
  error: null,
};

function snapshot(overrides = {}) {
  return {
    activeRun: {
      runId: "run-1",
      lifecycle: "executing",
      acceptedAt: "2026-08-23T00:00:00Z",
    },
    pause: { state: "none" },
    approval: { status: "none" },
    activity: { kind: "preparing", source: "server" },
    cursor: { runId: "run-1", streamEpoch: 1, sequenceNumber: 0 },
    ...overrides,
  };
}

describe("deriveAgentStatus", () => {
  it("calls preparation starting without pretending it is thinking", () => {
    expect(deriveAgentStatus({ ...base, snapshot: snapshot() })).toMatchObject({
      phase: "preparing",
      label: "Starting",
    });
  });

  it("only displays thinking when the runtime reports thinking", () => {
    expect(
      deriveAgentStatus({
        ...base,
        snapshot: snapshot({
          activity: { kind: "thinking", source: "provider" },
        }),
      })
    ).toMatchObject({ phase: "thinking", label: "Thinking" });
    expect(
      deriveAgentStatus({
        ...base,
        snapshot: snapshot(),
        events: [
          {
            event: "reasoning.delta",
            data: { delta: "provider reasoning" },
            streamEpoch: 1,
          },
        ],
      })
    ).toMatchObject({ phase: "thinking", label: "Thinking" });
    expect(
      deriveAgentStatus({
        ...base,
        snapshot: snapshot(),
        events: [
          {
            event: "reasoning.delta",
            data: { delta: "provider reasoning" },
            streamEpoch: 1,
          },
          {
            event: "text.delta",
            data: { delta: "answer" },
            streamEpoch: 1,
          },
        ],
      })
    ).toMatchObject({
      phase: "responding",
      label: "Responding",
      progress: null,
      transitions: ["Thinking", "Responding"],
    });
  });

  it("tracks actual tool labels and determinate progress", () => {
    const events = [
      {
        event: "status.updated",
        data: {
          runId: "agent-run-1",
          kind: "reading",
          label: "Scanning workspace",
        },
        streamEpoch: 1,
      },
      {
        event: "status.updated",
        data: {
          runId: "agent-run-1",
          toolName: "scan_workspace",
          label: "App.jsx",
          completed: 3,
          total: 5,
        },
        streamEpoch: 1,
      },
    ];
    expect(
      deriveAgentStatus({ ...base, snapshot: snapshot(), events })
    ).toMatchObject({
      label: "App.jsx",
      detail: "scan_workspace",
      progress: { completed: 3, total: 5, percent: 60 },
      transitions: ["Scanning workspace", "App.jsx (3/5)"],
    });
  });

  it("gives approval and pause dimensions priority over activity", () => {
    expect(
      deriveAgentStatus({
        ...base,
        snapshot: snapshot({ approval: { status: "pending" } }),
      }).label
    ).toBe("Waiting for your approval");
    expect(
      deriveAgentStatus({
        ...base,
        snapshot: snapshot({
          pause: { state: "paused" },
          checkpointKind: "between_items",
        }),
      })
    ).toMatchObject({ phase: "paused", label: "Paused" });
  });

  it("shows durable recovery and terminal outcomes", () => {
    expect(
      deriveAgentStatus({
        ...base,
        connectionState: "reconnecting",
        snapshot: snapshot(),
      }).label
    ).toBe("Reconnecting live updates");
    expect(
      deriveAgentStatus({
        ...base,
        snapshot: { activeRun: null },
        events: [{ event: "run.completed", data: { runId: "run-1" } }],
      })
    ).toMatchObject({ phase: "completed", label: "Completed" });
  });
});
