/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/require-await, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-member-access */
/**
 * Tests for parallel agentic execution:
 *   (a) `parallelSafe` tools from one inference run concurrently, with results
 *       always fed back in model-call order (never completion order);
 *   (b) non-`parallelSafe` tools split execution segments and never overlap
 *       with anything;
 *   (c) durable step ids are pre-assigned in model-call order, so a concurrent
 *       batch replays with identical ids and zero duplicate side effects;
 *   (d) a failing tool is captured per-call and cannot fail its batch;
 *   (e) cooperative cancellation stops NOT-YET-STARTED segments while keeping
 *       every model tool_call paired with a tool_result;
 *   (f) a parallel batch measurably reduces wall-clock latency;
 *   (g) `parallelAgents` dispatches stacked subagents concurrently, finalizing
 *       results in stack order, persisting successes before rethrowing failures.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { z } from "zod";
import { createAgent, createNetwork, createTool, type Agent } from "../index";
import { createState } from "../state";
import { getStepTools } from "../util";
import { createMockModel } from "./test-helpers";

// Keep the real util (isInngestFn etc.); only stub getStepTools so tests can
// inject a deterministic, memoizing step (a stand-in for Inngest's step graph).
vi.mock("../util", async (importOriginal) => {
  const actual = await importOriginal<Record<string, unknown>>();
  return { ...actual, getStepTools: vi.fn() };
});
const mockedGetStepTools = vi.mocked(getStepTools);

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * A step.run that mimics Inngest: results are memoized by id (so a second run is
 * a "replay" that returns cached values without re-executing). `ids` records the
 * creation-order id sequence; `executed` records cache MISSES (work actually
 * run). On a clean replay nothing should be executed.
 */
function makeReplayStep(cache = new Map<string, unknown>()) {
  const ids: string[] = [];
  const executed: string[] = [];
  const step = {
    run: async (id: string, fn: () => unknown) => {
      ids.push(id);
      if (cache.has(id)) return cache.get(id);
      executed.push(id);
      const r = await fn();
      cache.set(id, r);
      return r;
    },
    invoke: async () => undefined,
    sendEvent: async () => undefined,
    waitForEvent: async () => undefined,
    ai: { infer: async () => undefined },
  };
  return { step, cache, ids, executed };
}

/**
 * Records the maximum number of simultaneously active entrants — a race probe:
 * serial execution never exceeds 1, a concurrent batch of N reaches N.
 */
function makeOverlapProbe() {
  let active = 0;
  let maxActive = 0;
  return {
    async hold(ms: number) {
      active++;
      maxActive = Math.max(maxActive, active);
      try {
        await sleep(ms);
      } finally {
        active--;
      }
    },
    maxActive: () => maxActive,
  };
}

type ToolSpec = {
  name: string;
  parallelSafe?: boolean;
  delay?: number;
  fail?: boolean;
  probe?: ReturnType<typeof makeOverlapProbe>;
  onCall?: () => void;
};

function probeTool(spec: ToolSpec) {
  return createTool({
    name: spec.name,
    description: spec.name,
    parameters: z.object({}),
    parallelSafe: spec.parallelSafe,
    handler: async () => {
      spec.onCall?.();
      if (spec.probe) {
        await spec.probe.hold(spec.delay ?? 20);
      } else if (spec.delay) {
        await sleep(spec.delay);
      }
      if (spec.fail) {
        throw new Error(`${spec.name} exploded`);
      }
      return `${spec.name}-result`;
    },
  });
}

/** An agent whose mock model calls the given tools once, in the listed order. */
function toolAgent(tools: ReturnType<typeof probeTool>[], name = "a") {
  const model = createMockModel({
    toolCalls: tools.map((t, i) => ({
      toolCallId: `c${i}`,
      toolName: t.name,
      args: {},
    })),
  });
  return createAgent({ name, system: "s", model, tools });
}

/** Extract (tool name, content) pairs from an agent run's tool results. */
function toolResults(result: Awaited<ReturnType<Agent<any>["run"]>>) {
  return result.toolCalls.map((r) => ({
    name: r.tool.name,
    content: r.content as any,
  }));
}

beforeEach(() => {
  mockedGetStepTools.mockReset();
});

describe("parallel tool execution", () => {
  it("runs parallelSafe tools from one inference concurrently", async () => {
    const probe = makeOverlapProbe();
    const tools = [
      probeTool({ name: "t1", parallelSafe: true, delay: 40, probe }),
      probeTool({ name: "t2", parallelSafe: true, delay: 40, probe }),
      probeTool({ name: "t3", parallelSafe: true, delay: 40, probe }),
    ];
    const result = await toolAgent(tools).run("hi");

    expect(probe.maxActive()).toBe(3);
    expect(toolResults(result).map((r) => r.name)).toEqual(["t1", "t2", "t3"]);
    expect(toolResults(result).map((r) => r.content)).toEqual([
      { data: "t1-result" },
      { data: "t2-result" },
      { data: "t3-result" },
    ]);
  });

  it("feeds results back in model-call order, not completion order", async () => {
    // The FIRST call is the slowest: completion order is t3, t2, t1 — the
    // recorded order must remain t1, t2, t3.
    const tools = [
      probeTool({ name: "t1", parallelSafe: true, delay: 60 }),
      probeTool({ name: "t2", parallelSafe: true, delay: 20 }),
      probeTool({ name: "t3", parallelSafe: true, delay: 40 }),
    ];
    const result = await toolAgent(tools).run("hi");

    expect(toolResults(result).map((r) => r.name)).toEqual(["t1", "t2", "t3"]);
    expect(toolResults(result).map((r) => r.content.data)).toEqual([
      "t1-result",
      "t2-result",
      "t3-result",
    ]);
  });

  it("serializes all tool calls by default (no parallelSafe)", async () => {
    const probe = makeOverlapProbe();
    const tools = [
      probeTool({ name: "t1", delay: 20, probe }),
      probeTool({ name: "t2", delay: 20, probe }),
      probeTool({ name: "t3", delay: 20, probe }),
    ];
    const result = await toolAgent(tools).run("hi");

    expect(probe.maxActive()).toBe(1);
    expect(toolResults(result).map((r) => r.name)).toEqual(["t1", "t2", "t3"]);
  });

  it("splits execution segments around non-parallelSafe tools", async () => {
    // [safe, safe, UNSAFE, safe, safe] → batch(1,2) → serial(3) → batch(4,5).
    // A single global probe proves the unsafe tool never overlaps ANY other
    // call while each batch still reaches 2.
    const probe = makeOverlapProbe();
    const tools = [
      probeTool({ name: "t1", parallelSafe: true, delay: 30, probe }),
      probeTool({ name: "t2", parallelSafe: true, delay: 30, probe }),
      probeTool({ name: "t3", delay: 30, probe }),
      probeTool({ name: "t4", parallelSafe: true, delay: 30, probe }),
      probeTool({ name: "t5", parallelSafe: true, delay: 30, probe }),
    ];
    const result = await toolAgent(tools).run("hi");

    expect(probe.maxActive()).toBe(2);
    expect(toolResults(result).map((r) => r.name)).toEqual([
      "t1",
      "t2",
      "t3",
      "t4",
      "t5",
    ]);
  });

  it("pre-assigns replay-stable durable step ids under concurrency", async () => {
    const replay = makeReplayStep();
    mockedGetStepTools.mockResolvedValue(replay.step as any);

    const make = () => [
      // The FIRST call is slower: completion order inverts, but step ids must
      // be created in model-call order (t1/0 before t2/1).
      probeTool({ name: "t1", parallelSafe: true, delay: 50 }),
      probeTool({ name: "t2", parallelSafe: true, delay: 5 }),
    ];

    const first = await toolAgent(make()).run("hi", { state: createState() });
    expect(first.toolCalls).toHaveLength(2);

    const toolStepIds = replay.ids.filter((id) => id.includes("/tool/"));
    expect(toolStepIds).toEqual(["a/tool/t1/0", "a/tool/t2/1"]);
    expect(replay.executed.filter((id) => id.includes("/tool/"))).toEqual(
      toolStepIds
    );

    // Replay with the same memoized cache and a FRESH state (as an Inngest
    // replay re-clones state): identical ids, zero re-executed side effects.
    const idsBefore = replay.ids.length;
    const second = await toolAgent(make()).run("hi", {
      state: createState(),
    });
    expect(second.toolCalls).toHaveLength(2);
    expect(replay.ids.slice(idsBefore)).toEqual(replay.ids.slice(0, idsBefore));
    expect(replay.executed).toHaveLength(idsBefore); // nothing new ran

    // Same results on replay, in the same order.
    expect(toolResults(second)).toEqual(toolResults(first));
  });

  it("captures a failing tool's error without failing its batch", async () => {
    const tools = [
      probeTool({ name: "t1", parallelSafe: true, delay: 10 }),
      probeTool({ name: "t2", parallelSafe: true, delay: 10, fail: true }),
      probeTool({ name: "t3", parallelSafe: true, delay: 10 }),
    ];
    const result = await toolAgent(tools).run("hi");

    const rs = toolResults(result);
    expect(rs.map((r) => r.name)).toEqual(["t1", "t2", "t3"]);
    expect(rs[0]!.content).toEqual({ data: "t1-result" });
    expect(rs[1]!.content.error.message).toContain("t2 exploded");
    expect(rs[2]!.content).toEqual({ data: "t3-result" });
  });

  it("stops dispatching later segments once cancelled, pairing every call with a result", async () => {
    const controller = new AbortController();
    const laterCalls: string[] = [];

    const canceller = createTool({
      name: "canceller",
      parameters: z.object({}),
      handler: async () => {
        controller.abort();
        return "done";
      },
    });
    const tools = [
      canceller,
      probeTool({
        name: "t2",
        parallelSafe: true,
        onCall: () => laterCalls.push("t2"),
      }),
      probeTool({
        name: "t3",
        parallelSafe: true,
        onCall: () => laterCalls.push("t3"),
      }),
    ];
    const result = await toolAgent(tools).run("hi", {
      signal: controller.signal,
    });

    // The cancelled batch never started…
    expect(laterCalls).toEqual([]);
    // …but every model tool_call is still paired with a tool_result.
    const rs = toolResults(result);
    expect(rs.map((r) => r.name)).toEqual(["canceller", "t2", "t3"]);
    expect(rs[0]!.content).toEqual({ data: "done" });
    expect(rs[1]!.content.error.message).toContain("cancelled");
    expect(rs[2]!.content.error.message).toContain("cancelled");
  });

  it("treats an already-aborted signal as fully cancelled without running handlers", async () => {
    const controller = new AbortController();
    controller.abort();
    const called: string[] = [];
    const tools = [
      probeTool({
        name: "t1",
        parallelSafe: true,
        onCall: () => called.push("t1"),
      }),
      probeTool({
        name: "t2",
        onCall: () => called.push("t2"),
      }),
    ];
    const result = await toolAgent(tools).run("hi", {
      signal: controller.signal,
    });

    expect(called).toEqual([]);
    const rs = toolResults(result);
    expect(rs).toHaveLength(2);
    expect(rs.every((r) => r.content.error)).toBe(true);
  });

  it("reduces wall-clock latency versus serial execution (benchmark)", async () => {
    const DELAY = 60;
    const N = 4;
    const spec = (parallelSafe: boolean) =>
      Array.from({ length: N }, (_, i) =>
        probeTool({ name: `t${i}`, parallelSafe, delay: DELAY })
      );

    const t0 = performance.now();
    await toolAgent(spec(true), "parallel").run("hi");
    const parallelMs = performance.now() - t0;

    const t1 = performance.now();
    await toolAgent(spec(false), "serial").run("hi");
    const serialMs = performance.now() - t1;

    // Serial must take ~N×DELAY; parallel must be far below it.
    expect(serialMs).toBeGreaterThanOrEqual(N * DELAY * 0.8);
    expect(parallelMs).toBeLessThan(serialMs * 0.6);
  });
});

describe("parallel subagent dispatch", () => {
  /** Two agents that overlap-probe their onStart; the router stacks both once. */
  function makeParallelNetwork(opts: {
    parallelAgents?: boolean;
    probe: ReturnType<typeof makeOverlapProbe>;
    failB?: boolean;
    state?: ReturnType<typeof createState>;
  }) {
    const mkAgent = (name: string, delay: number, fail = false) =>
      createAgent({
        name,
        system: "s",
        model: createMockModel(
          fail ? { error: new Error(`${name} exploded`) } : { text: name }
        ),
        lifecycle: {
          onStart: async ({ prompt, history }) => {
            await opts.probe.hold(delay);
            return {
              prompt: prompt ?? [],
              history: history ?? [],
              stop: false,
            };
          },
        },
      });
    // A is SLOWER: completion order would invert, but finalization must follow
    // stack order.
    const a = mkAgent("a", 50);
    const b = mkAgent("b", 10, opts.failB);
    const network = createNetwork({
      name: "n",
      agents: [a, b],
      maxIter: 10,
      parallelAgents: opts.parallelAgents,
      router: ({ callCount }) => (callCount === 0 ? [a, b] : undefined),
    });
    return { network, a, b };
  }

  it("dispatches stacked agents concurrently and finalizes in stack order", async () => {
    const probe = makeOverlapProbe();
    const { network } = makeParallelNetwork({ parallelAgents: true, probe });

    const run = await network.run("hi");

    expect(probe.maxActive()).toBe(2);
    // Stack order, not completion order (b finishes long before a).
    expect(run.state.results.map((r) => r.agentName)).toEqual(["a", "b"]);
  });

  it("runs stacked agents serially by default", async () => {
    const probe = makeOverlapProbe();
    const { network } = makeParallelNetwork({ probe });

    const run = await network.run("hi");

    expect(probe.maxActive()).toBe(1);
    expect(run.state.results.map((r) => r.agentName)).toEqual(["a", "b"]);
  });

  it("persists successful batch results in stack order before rethrowing a failure", async () => {
    const probe = makeOverlapProbe();
    const state = createState();
    const { network } = makeParallelNetwork({
      parallelAgents: true,
      probe,
      failB: true,
    });

    await expect(network.run("hi", { state })).rejects.toThrow("b exploded");

    // The successful sibling's result survived the failed batch.
    expect(state.results.map((r) => r.agentName)).toEqual(["a"]);
  });
});
