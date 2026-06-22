/* eslint-disable @typescript-eslint/require-await */
/**
 * Regression tests for the two step-budget problems:
 *   1. simulated streaming chunking flooding Inngest's step graph with publishes;
 *   2. the network's maxIter coupling to the agent's internal tool-loop.
 */
import { describe, it, expect } from "vitest";
import { z } from "zod";
import { createAgent, createNetwork, createTool } from "../index";
import {
  StreamingContext,
  type AgentMessageChunk,
  DEFAULT_CHUNK_SIZE,
} from "../streaming";
import { createMockModel } from "./test-helpers";

function makeContext(opts: {
  simulateChunking?: boolean;
  chunkSize?: number;
  maxChunksPerMessage?: number;
}): StreamingContext {
  return new StreamingContext({
    publish: async () => {},
    runId: "run-1",
    messageId: "msg-1",
    scope: "agent",
    ...opts,
  });
}

describe("StreamingContext.chunkContent", () => {
  it("returns a single delta when chunking is off (one publish/part)", () => {
    const ctx = makeContext({ simulateChunking: false });
    expect(ctx.chunkContent("x".repeat(1000))).toEqual(["x".repeat(1000)]);
  });

  it("returns no deltas for empty content", () => {
    expect(makeContext({ simulateChunking: true }).chunkContent("")).toEqual([]);
  });

  it("splits by chunkSize when chunking is on", () => {
    const ctx = makeContext({ simulateChunking: true, chunkSize: 100 });
    expect(ctx.chunkContent("x".repeat(1000))).toHaveLength(10);
  });

  it("defaults to a coarse chunk size (≥256), not the old hardcoded 50", () => {
    const ctx = makeContext({ simulateChunking: true });
    // 1000 chars / 256 ≈ 4 chunks (vs. 20 at the old size of 50).
    expect(ctx.chunkContent("x".repeat(1000)).length).toBeLessThanOrEqual(4);
    expect(DEFAULT_CHUNK_SIZE).toBeGreaterThanOrEqual(256);
  });

  it("caps chunks per part regardless of output length", () => {
    const ctx = makeContext({
      simulateChunking: true,
      chunkSize: 50,
      maxChunksPerMessage: 24,
    });
    // 100k chars at size 50 would be 2000 chunks; the cap keeps it ≤ 24.
    const chunks = ctx.chunkContent("x".repeat(100_000));
    expect(chunks.length).toBeLessThanOrEqual(24);
    // …and the chunks still reconstruct the original content losslessly.
    expect(chunks.join("")).toBe("x".repeat(100_000));
  });

  it("a 0 cap means unlimited chunks", () => {
    const ctx = makeContext({
      simulateChunking: true,
      chunkSize: 50,
      maxChunksPerMessage: 0,
    });
    expect(ctx.chunkContent("x".repeat(1000))).toHaveLength(20);
  });
});

describe("streaming publish volume scales with chunk config", () => {
  async function countTextDeltas(streaming: {
    simulateChunking?: boolean;
    chunkSize?: number;
    maxChunksPerMessage?: number;
  }): Promise<number> {
    const text = "x".repeat(1000);
    const model = createMockModel({ text });
    const agent = createAgent({ name: "a", system: "s", model });
    let textDeltas = 0;
    await agent.run("hi", {
      streaming: {
        publish: async (chunk: AgentMessageChunk) => {
          if (chunk.event === "text.delta") textDeltas++;
        },
        ...streaming,
      },
    });
    return textDeltas;
  }

  it("emits one text.delta per part when chunking is off", async () => {
    expect(await countTextDeltas({ simulateChunking: false })).toBe(1);
  });

  it("emits ~content/chunkSize deltas when chunking is on", async () => {
    expect(await countTextDeltas({ simulateChunking: true, chunkSize: 100 })).toBe(
      10
    );
    expect(
      await countTextDeltas({ simulateChunking: true, chunkSize: 250 })
    ).toBe(4);
  });

  it("does NOT emit ~1 publish per 50 chars (the old behavior)", async () => {
    // Old hardcoded chunkSize=50 → 20 deltas for 1000 chars. The configurable
    // default is far coarser.
    const deltas = await countTextDeltas({ simulateChunking: true });
    expect(deltas).toBeLessThan(20);
  });
});

describe("network bounds total inferences to maxIter (not maxIter²)", () => {
  function alwaysToolCallModel(onInfer: () => void) {
    return createMockModel({
      toolCalls: [{ toolCallId: "c1", toolName: "noop", args: {} }],
      onGenerate: onInfer,
    });
  }

  const noopTool = createTool({
    name: "noop",
    description: "no-op",
    parameters: z.object({}),
    handler: async () => "ok",
  });

  it("runs exactly maxIter inferences when the agent keeps calling tools", async () => {
    let inferences = 0;
    const model = alwaysToolCallModel(() => {
      inferences++;
    });
    const agent = createAgent({
      name: "a",
      system: "s",
      model,
      tools: [noopTool],
    });
    const maxIter = 4;
    const network = createNetwork({
      name: "n",
      agents: [agent],
      defaultModel: model,
      maxIter,
      // Keep routing back to the agent; the network's maxIter is the only bound.
      router: () => agent,
    });

    await network.run("hi");

    // Decoupled: maxIter network iterations × 1 inference each = maxIter.
    // If the agent's internal loop were driven by network.maxIter it would be 16.
    expect(inferences).toBe(maxIter);
  });
});

describe("agent internal tool-loop cap is its own (maxIterPerRun)", () => {
  const noopTool = createTool({
    name: "noop",
    description: "no-op",
    parameters: z.object({}),
    handler: async () => "ok",
  });

  async function countStandaloneInferences(
    opts: { maxIterPerRun?: number } = {}
  ): Promise<number> {
    let inferences = 0;
    const model = createMockModel({
      toolCalls: [{ toolCallId: "c1", toolName: "noop", args: {} }],
      onGenerate: () => {
        inferences++;
      },
    });
    const agent = createAgent({
      name: "a",
      system: "s",
      model,
      tools: [noopTool],
    });
    await agent.run("hi", opts);
    return inferences;
  }

  it("defaults to a single inference per run", async () => {
    expect(await countStandaloneInferences()).toBe(1);
  });

  it("loops up to maxIterPerRun when the model keeps calling tools", async () => {
    expect(await countStandaloneInferences({ maxIterPerRun: 3 })).toBe(3);
  });
});
