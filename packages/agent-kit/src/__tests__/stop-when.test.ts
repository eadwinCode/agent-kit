/* eslint-disable @typescript-eslint/require-await */
/**
 * Tests for the network `stopWhen` stop policy: it ends a run early at the safe
 * between-inference boundary, emits a typed `run.interrupted`, and annotates the
 * single terminal `run.completed`/`stream.ended` with the reason.
 */
import { describe, it, expect } from "vitest";
import { z } from "zod";
import { createAgent, createNetwork, createTool } from "../index";
import { type AgentMessageChunk, type StopReason } from "../streaming";
import { createMockModel } from "./test-helpers";

const noopTool = createTool({
  name: "noop",
  description: "no-op",
  parameters: z.object({}),
  handler: async () => "ok",
});

/** A network whose agent always calls a tool, so the router keeps looping until
 *  maxIter or stopWhen ends it. `onInfer` counts inferences. */
function makeNetwork(opts: {
  stopWhen?: Parameters<typeof createNetwork>[0]["stopWhen"];
  maxIter?: number;
  onInfer?: () => void;
}) {
  const model = createMockModel({
    toolCalls: [{ toolCallId: "c1", toolName: "noop", args: {} }],
    onGenerate: opts.onInfer,
  });
  const agent = createAgent({
    name: "a",
    system: "s",
    model,
    tools: [noopTool],
  });
  return createNetwork({
    name: "n",
    agents: [agent],
    defaultModel: model,
    maxIter: opts.maxIter ?? 10,
    router: () => agent,
    stopWhen: opts.stopWhen,
  });
}

function capture() {
  const events: AgentMessageChunk[] = [];
  const publish = async (c: AgentMessageChunk) => void events.push(c);
  const names = () => events.map((e) => e.event);
  const networkEvent = (name: string) =>
    events.find((e) => e.event === name && e.data.scope === "network");
  return { events, publish, names, networkEvent };
}

describe("network stopWhen", () => {
  it("stops at the configured iteration (between inferences)", async () => {
    let inferences = 0;
    const network = makeNetwork({
      onInfer: () => inferences++,
      stopWhen: ({ callCount }) =>
        callCount >= 2 ? { reason: "budget" } : undefined,
    });
    await network.run("hi");
    // callCount 0 → run, 1 → run, 2 → stop. Two completed inferences.
    expect(inferences).toBe(2);
  });

  it("can stop before any inference (already over budget at turn start)", async () => {
    let inferences = 0;
    const network = makeNetwork({
      onInfer: () => inferences++,
      stopWhen: () => ({ reason: "budget" }),
    });
    await network.run("hi");
    expect(inferences).toBe(0);
  });

  it("emits run.interrupted then a reason-annotated terminal", async () => {
    const cap = capture();
    const network = makeNetwork({
      stopWhen: ({ callCount }) =>
        callCount >= 1
          ? { reason: "budget", metadata: { hit: true } }
          : undefined,
    });
    await network.run("hi", { streaming: { publish: cap.publish } });

    const interrupted = cap.networkEvent("run.interrupted");
    expect(interrupted).toBeDefined();
    expect(interrupted!.data.reason).toBe<StopReason>("budget");
    expect(interrupted!.data.metadata).toEqual({ hit: true });

    // Terminal still fires (clients rely on it) AND carries the reason.
    expect(cap.networkEvent("run.completed")!.data.reason).toBe("budget");
    expect(cap.networkEvent("stream.ended")!.data.reason).toBe("budget");

    // Ordering: the network-scoped interrupted precedes the network terminal.
    // (filter by scope — an agent-scoped run.completed fires mid-run.)
    const idx = (name: string) =>
      cap.events.findIndex(
        (e) => e.event === name && e.data.scope === "network"
      );
    expect(idx("run.interrupted")).toBeGreaterThanOrEqual(0);
    expect(idx("run.interrupted")).toBeLessThan(idx("run.completed"));
    expect(idx("run.completed")).toBeLessThan(idx("stream.ended"));
  });

  it("a per-run stopWhen overrides the network default", async () => {
    let inferences = 0;
    const network = makeNetwork({
      onInfer: () => inferences++,
      stopWhen: () => undefined, // network default: never stop
    });
    await network.run("hi", {
      stopWhen: ({ callCount }) =>
        callCount >= 1 ? { reason: "limit" } : undefined,
    });
    expect(inferences).toBe(1);
  });

  it("is deterministic: identical stop across repeated runs", async () => {
    const counts: number[] = [];
    for (let i = 0; i < 2; i++) {
      let inferences = 0;
      const network = makeNetwork({
        onInfer: () => inferences++,
        stopWhen: ({ callCount }) =>
          callCount >= 3 ? { reason: "budget" } : undefined,
      });
      await network.run("hi");
      counts.push(inferences);
    }
    expect(counts).toEqual([3, 3]);
  });

  describe("backward compatibility / terminal hygiene", () => {
    it("no stopWhen → normal completion, no run.interrupted, no reason", async () => {
      const cap = capture();
      // maxIter bounds the otherwise-infinite tool loop.
      const network = makeNetwork({ maxIter: 2 });
      await network.run("hi", { streaming: { publish: cap.publish } });

      expect(cap.networkEvent("run.interrupted")).toBeUndefined();
      expect(cap.networkEvent("run.completed")!.data.reason).toBeUndefined();
      expect(cap.networkEvent("stream.ended")!.data.reason).toBeUndefined();
    });

    it("emits the network terminal exactly once (no double-fire)", async () => {
      const cap = capture();
      const network = makeNetwork({ maxIter: 2 });
      await network.run("hi", { streaming: { publish: cap.publish } });

      const completed = cap
        .names()
        .filter(
          (_, i) =>
            cap.events[i]!.event === "run.completed" &&
            cap.events[i]!.data.scope === "network"
        );
      const ended = cap.events.filter(
        (e) => e.event === "stream.ended" && e.data.scope === "network"
      );
      expect(completed).toHaveLength(1);
      expect(ended).toHaveLength(1);
    });
  });
});
