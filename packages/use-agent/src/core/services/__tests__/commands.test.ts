import { describe, it, expect, vi } from "vitest";
import { buildCommand, createCommandId, executeCommand } from "../commands.js";
import { AgentTransportError, AgentErrorCodes } from "../../errors/agent-transport-error.js";
import type { IAgentSessionTransport, AgentCommandResult } from "../../ports/agent-session.js";
import type { AgentStateSnapshot } from "../../../types/index.js";

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
    pause: { state: "none", accumulatedPausedMs: 0, epoch: 2 },
    activity: { kind: "responding" },
    approval: { status: "none" },
    revision: 9,
    cursor: { runId: "run_1", streamEpoch: 0, sequenceNumber: 3 },
    reconcileRequired: false,
    ...overrides,
  };
}

describe("buildCommand", () => {
  it("derives ids and the revision precondition from the snapshot", () => {
    const command = buildCommand({ type: "pause", snapshot: snapshot() });

    expect(command.type).toBe("pause");
    expect(command.commandId).toBeTruthy();
    expect(command.threadId).toBe("thread_1");
    // No tenancy: the transport already authenticated against its own scope,
    // so echoing a project id back would be untrusted input the server has to
    // overwrite anyway.
    expect("projectId" in command).toBe(false);
    expect(command.runId).toBe("run_1");
    // Without this precondition, two tabs racing a pause both "succeed" and
    // one silently overwrites the other.
    expect(command.expectedRevision).toBe(9);
  });

  it("lets explicit values win over the snapshot", () => {
    const command = buildCommand({
      type: "resume",
      snapshot: snapshot(),
      commandId: "fixed-id",
      runId: "run_override",
      pauseEpoch: 2,
      expectedRevision: 3,
    });
    expect(command.commandId).toBe("fixed-id");
    expect(command.runId).toBe("run_override");
    expect(command.pauseEpoch).toBe(2);
    expect(command.expectedRevision).toBe(3);
  });

  it("omits the precondition when there is no revision to assert", () => {
    const command = buildCommand({ type: "new_chat" });
    expect(command.expectedRevision).toBeUndefined();
    expect(command.threadId).toBeUndefined();
  });
});

describe("createCommandId", () => {
  it("mints unique ids", () => {
    const ids = new Set(Array.from({ length: 200 }, () => createCommandId()));
    expect(ids.size).toBe(200);
  });
});

function makeTransport(
  impl: (calls: number) => Promise<AgentCommandResult>
): IAgentSessionTransport & { calls: number } {
  const transport = {
    calls: 0,
    async fetchAgentState() {
      throw new Error("not used");
    },
    async fetchEventTail() {
      throw new Error("not used");
    },
    async executeCommand() {
      transport.calls++;
      return impl(transport.calls);
    },
  };
  return transport as IAgentSessionTransport & { calls: number };
}

describe("executeCommand", () => {
  it("retries a recoverable failure under the SAME command id", async () => {
    // Reusing the id is what makes the retry safe: the server records the
    // first result rather than starting a second run.
    const seen: string[] = [];
    const transport = makeTransport(async (calls) => {
      if (calls < 3) {
        throw new AgentTransportError({
          status: 503,
          code: "HTTP_503",
          message: "unavailable",
          recoverable: true,
          retryAfterMs: 0,
        });
      }
      return { snapshot: snapshot() };
    });
    const original = transport.executeCommand.bind(transport);
    transport.executeCommand = async (command, options) => {
      seen.push(command.commandId);
      return original(command, options);
    };

    const command = buildCommand({ type: "cancel", snapshot: snapshot() });
    const result = await executeCommand(transport, command, { sleep: async () => {} });

    expect(result.snapshot.revision).toBe(9);
    expect(new Set(seen).size).toBe(1);
    expect(seen).toHaveLength(3);
  });

  it("does not retry a stale revision; it hands back the authoritative snapshot", async () => {
    const authoritative = snapshot({ revision: 12 });
    const transport = makeTransport(async () => {
      throw new AgentTransportError({
        status: 409,
        code: AgentErrorCodes.StateRevisionMismatch,
        message: "state changed",
        recoverable: true,
        snapshot: authoritative,
      });
    });
    const onReconcile = vi.fn();

    await expect(
      executeCommand(transport, buildCommand({ type: "pause", snapshot: snapshot() }), {
        onReconcile,
        sleep: async () => {},
      })
    ).rejects.toMatchObject({ code: AgentErrorCodes.StateRevisionMismatch });

    expect(transport.calls).toBe(1);
    expect(onReconcile).toHaveBeenCalledWith(authoritative);
  });

  it("never retries an idempotency-key reuse", async () => {
    const transport = makeTransport(async () => {
      throw new AgentTransportError({
        status: 409,
        code: AgentErrorCodes.IdempotencyKeyReused,
        message: "reused",
        recoverable: false,
      });
    });

    await expect(
      executeCommand(transport, buildCommand({ type: "send" }), { sleep: async () => {} })
    ).rejects.toMatchObject({ code: AgentErrorCodes.IdempotencyKeyReused });
    expect(transport.calls).toBe(1);
  });

  it("never retries against a terminal run", async () => {
    const transport = makeTransport(async () => {
      throw new AgentTransportError({
        status: 409,
        code: AgentErrorCodes.RunTerminal,
        message: "already finished",
        recoverable: false,
        snapshot: snapshot({ activeRun: null }),
      });
    });

    await expect(
      executeCommand(transport, buildCommand({ type: "pause", snapshot: snapshot() }), {
        sleep: async () => {},
      })
    ).rejects.toMatchObject({ code: AgentErrorCodes.RunTerminal });
    expect(transport.calls).toBe(1);
  });

  it("stops after maxRetries", async () => {
    const transport = makeTransport(async () => {
      throw new AgentTransportError({
        status: 500,
        code: "HTTP_500",
        message: "boom",
        recoverable: true,
        retryAfterMs: 0,
      });
    });

    await expect(
      executeCommand(transport, buildCommand({ type: "cancel" }), {
        maxRetries: 1,
        sleep: async () => {},
      })
    ).rejects.toMatchObject({ status: 500 });
    expect(transport.calls).toBe(2);
  });

  it("rethrows non-transport errors untouched", async () => {
    const transport = makeTransport(async () => {
      throw new TypeError("programming error");
    });
    await expect(
      executeCommand(transport, buildCommand({ type: "cancel" }))
    ).rejects.toBeInstanceOf(TypeError);
    expect(transport.calls).toBe(1);
  });
});
