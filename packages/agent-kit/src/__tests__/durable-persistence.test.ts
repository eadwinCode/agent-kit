/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/require-await, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-assignment */
/**
 * Regression tests for Inngest determinism:
 *   (a) a mid-loop failure leaves completed results persisted (per-iteration save);
 *   (b) every fork-generated step id is replay-stable (no checksum/timestamp ids);
 *   (c) a tool handler that checkpoints async work via `step.run` replays without
 *       losing steps (no `foundSteps: []`).
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { z } from "zod";
import { createAgent, createNetwork, createTool } from "../index";
import {
  persistResults,
  incrementalAppendStepId,
  type HistoryConfig,
} from "../history";
import { createState } from "../state";
import { AgentResult, type Message } from "../types";
import { getStepTools } from "../util";
import { createMockModel } from "./test-helpers";

// Keep the real util (isInngestFn etc.); only stub getStepTools so tests can
// inject a deterministic, memoizing step (a stand-in for Inngest's step graph).
vi.mock("../util", async (importOriginal) => {
  const actual = await importOriginal<Record<string, unknown>>();
  return { ...actual, getStepTools: vi.fn() };
});
const mockedGetStepTools = vi.mocked(getStepTools);

/**
 * A step.run that mimics Inngest: results are memoized by id (so a second run is
 * a "replay" that returns cached values without re-executing), and duplicate ids
 * within a run get an occurrence suffix (`id`, `id:1`, …) — exactly Inngest's
 * behavior. `ids` records the id sequence; `executed` records cache MISSES (work
 * actually run). On a clean replay nothing should be executed.
 */
function makeReplayStep(cache = new Map<string, unknown>()) {
  const ids: string[] = [];
  const executed: string[] = [];
  const occ = new Map<string, number>();
  const step = {
    run: async (id: string, fn: () => unknown) => {
      const n = occ.get(id) ?? 0;
      occ.set(id, n + 1);
      const uid = n === 0 ? id : `${id}:${n}`;
      ids.push(uid);
      if (cache.has(uid)) return cache.get(uid);
      executed.push(uid);
      const r = await fn();
      cache.set(uid, r);
      return r;
    },
    invoke: async () => undefined,
    sendEvent: async () => undefined,
    waitForEvent: async () => undefined,
    ai: { infer: async () => undefined },
  };
  const reset = () => {
    ids.length = 0;
    executed.length = 0;
    occ.clear();
  };
  return { step, cache, ids, executed, reset };
}

const noopTool = createTool({
  name: "noop",
  description: "no-op",
  parameters: z.object({}),
  handler: async () => "ok",
});

beforeEach(() => {
  mockedGetStepTools.mockReset();
});

describe("Bug 1 — per-iteration persistence", () => {
  it("persists completed results when the run fails mid-loop", async () => {
    const rec = makeReplayStep();
    mockedGetStepTools.mockResolvedValue(rec.step as any);

    let calls = 0;
    const model = createMockModel({
      toolCalls: [{ toolCallId: "c1", toolName: "noop", args: {} }],
      onGenerate: () => {
        calls++;
        if (calls >= 3) throw new Error("mid-run boom");
      },
    });

    const persisted: AgentResult[] = [];
    const history: HistoryConfig<any> = {
      appendResults: async ({ newResults }) => {
        persisted.push(...newResults);
      },
    };

    const agent = createAgent({
      name: "a",
      system: "s",
      model,
      tools: [noopTool],
    });
    const network = createNetwork({
      name: "n",
      agents: [agent],
      defaultModel: model,
      history,
      maxIter: 5,
      router: () => agent,
    });

    await expect(network.run("hi", { state: createState({}) })).rejects.toThrow(
      "mid-run boom"
    );

    // Inferences 1 and 2 completed and were saved before #3 threw.
    expect(persisted).toHaveLength(2);
    // Each was saved under a deterministic counter id, never a checksum.
    expect(rec.ids).toContain(incrementalAppendStepId(1));
    expect(rec.ids).toContain(incrementalAppendStepId(2));
  });
});

describe("Bug 1/cross-cutting — persistence step ids are checksum-free", () => {
  it("uses the caller counter id, identical across results with different timestamps", async () => {
    const rec = makeReplayStep();
    mockedGetStepTools.mockResolvedValue(rec.step as any);

    const out: Message[] = [{ type: "text", role: "assistant", content: "hi" }];
    const r1 = new AgentResult("a", out, [], new Date(1000));
    const r2 = new AgentResult("a", out, [], new Date(2000)); // different createdAt → different checksum
    expect(r1.checksum).not.toBe(r2.checksum);

    const history: HistoryConfig<any> = { appendResults: async () => {} };
    const cfg = { state: createState({}), history, input: "x" } as any;

    // Two separate executions (original + replay), each with a different-timestamp
    // result. The persistence id must be identical and checksum-free both times.
    await persistResults(cfg, [r1], incrementalAppendStepId(7));
    const id1 = rec.ids.at(-1)!;
    rec.reset();
    await persistResults(cfg, [r2], incrementalAppendStepId(7));
    const id2 = rec.ids.at(-1)!;

    expect(id1).toBe(incrementalAppendStepId(7));
    expect(id2).toBe(id1);
    expect(id1).not.toContain(r1.checksum);
    expect(id2).not.toContain(r2.checksum);
  });

  it("passes step: undefined to the hook (fork owns the durable boundary)", async () => {
    const rec = makeReplayStep();
    mockedGetStepTools.mockResolvedValue(rec.step as any);
    let seenStep: unknown = "sentinel";
    const history: HistoryConfig<any> = {
      appendResults: async ({ step }) => {
        seenStep = step;
      },
    };
    const r = new AgentResult("a", [], [], new Date());
    await persistResults(
      { state: createState({}), history, input: "x" } as any,
      [r],
      incrementalAppendStepId(0)
    );
    expect(seenStep).toBeUndefined();
  });
});

describe("Bug 2 / determinism — step ids stable across a replay", () => {
  it("produces an identical step-id sequence on re-execution (no timestamp ids)", async () => {
    const cache = new Map<string, unknown>();
    const rec = makeReplayStep(cache);

    const makeRun = () => {
      const model = createMockModel({
        toolCalls: [{ toolCallId: "c1", toolName: "noop", args: {} }],
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
        history: { appendResults: async () => {} },
        maxIter: 2,
        router: () => agent,
      });
    };

    mockedGetStepTools.mockResolvedValue(rec.step as any);
    await makeRun().run("hi", { state: createState({}) });
    const idsRun1 = [...rec.ids];

    // Simulated replay: fresh run, SAME memoized cache, fresh AgentResults (new
    // timestamps). Any timestamp/checksum-derived id would now diverge.
    rec.reset();
    await makeRun().run("hi", { state: createState({}) });
    const idsRun2 = [...rec.ids];

    expect(idsRun2).toEqual(idsRun1);
    // On a clean replay every step is found in the cache — nothing re-executes.
    expect(rec.executed).toEqual([]);
    // Persistence ids are the counters.
    expect(idsRun1).toContain(incrementalAppendStepId(1));
    expect(idsRun1).toContain(incrementalAppendStepId(2));
  });
});

describe("Bug 2 — tool handler async work via step.run is replay-safe", () => {
  it("checkpoints handler work and replays without losing steps", async () => {
    const cache = new Map<string, unknown>();
    const rec = makeReplayStep(cache);

    const subagentTool = createTool({
      name: "subagent",
      description: "does multi-step async work durably",
      parameters: z.object({}),
      handler: async (_input, { step }) => {
        // Long/multi-call work: each unit is its own durable step with a
        // DETERMINISTIC id (the consumer contract). Replay finds each in cache.
        let acc = 0;
        for (let i = 0; i < 3; i++) {
          acc += await step!.run(`subagent/call/${i}`, async () => i + 1);
        }
        return acc; // 1 + 2 + 3 = 6
      },
    });

    const makeRun = () => {
      const model = createMockModel({
        toolCalls: [{ toolCallId: "c1", toolName: "subagent", args: {} }],
      });
      const agent = createAgent({
        name: "a",
        system: "s",
        model,
        tools: [subagentTool],
      });
      return createNetwork({
        name: "n",
        agents: [agent],
        defaultModel: model,
        maxIter: 1,
        router: () => agent,
      });
    };

    mockedGetStepTools.mockResolvedValue(rec.step as any);
    const r1 = await makeRun().run("hi", { state: createState({}) });
    const idsRun1 = [...rec.ids];

    // The handler's units are durable steps with stable ids.
    expect(idsRun1).toEqual(
      expect.arrayContaining([
        "subagent/call/0",
        "subagent/call/1",
        "subagent/call/2",
      ])
    );
    const toolResult = r1.state.results[0]?.toolCalls[0]?.content as {
      data?: unknown;
    };
    expect(toolResult?.data).toBe(6);

    // Replay: same cache → every step (incl. the handler's) is found; nothing
    // re-executes → no `foundSteps: []`.
    rec.reset();
    await makeRun().run("hi", { state: createState({}) });
    expect(rec.ids).toEqual(idsRun1);
    expect(rec.executed).toEqual([]);
  });
});
