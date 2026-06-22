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
import {
  createAgent,
  createNetwork,
  createTool,
  type NetworkRun,
} from "../index";
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
      // Opt out of the automatic durable wrap: this handler opens its OWN steps,
      // which cannot be nested inside a framework `step.run`. `manualStep` keeps
      // it inline with the live `opts.step` so it owns its durability.
      manualStep: true,
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

describe("Bug 3 — tool side effects are durable (exactly-once across replays)", () => {
  // Typed view of the {data}/{error} the first tool produced (keeps lint happy).
  const toolOutput = (net: NetworkRun<any>) =>
    net.state.results[0]?.toolCalls[0]?.content as {
      data?: unknown;
      error?: unknown;
    };

  // A non-idempotent tool whose handler is registered with the agent named "a".
  const makeEditRun = (cache: Map<string, unknown>, onApply: () => string) => {
    const editTool = createTool({
      name: "edit_file",
      description: "applies an edit (non-idempotent)",
      parameters: z.object({}),
      handler: async () => onApply(),
    });
    const model = createMockModel({
      toolCalls: [{ toolCallId: "c1", toolName: "edit_file", args: {} }],
    });
    const agent = createAgent({
      name: "a",
      system: "s",
      model,
      tools: [editTool],
    });
    return createNetwork({
      name: "n",
      agents: [agent],
      defaultModel: model,
      maxIter: 1,
      router: () => agent,
    });
  };

  it("wraps each tool call in ONE deterministic step and runs the side effect once", async () => {
    const cache = new Map<string, unknown>();
    const rec = makeReplayStep(cache);
    let sideEffects = 0;

    mockedGetStepTools.mockResolvedValue(rec.step as any);
    const r1 = await makeEditRun(cache, () => {
      sideEffects++;
      return "applied";
    }).run("hi", { state: createState({}) });

    // The handler ran exactly once, inside its own replay-stable step span…
    expect(sideEffects).toBe(1);
    expect(rec.ids).toContain("a/tool/edit_file/0");
    // …and the model received the real result (not a re-fire error).
    expect(toolOutput(r1)?.data).toBe("applied");

    // Replay: same memoized cache, fresh state. The side effect must NOT re-fire.
    rec.reset();
    const r2 = await makeEditRun(cache, () => {
      sideEffects++;
      return "applied";
    }).run("hi", { state: createState({}) });

    expect(sideEffects).toBe(1); // exactly once across original + replay
    expect(rec.executed).toEqual([]); // clean replay: nothing re-executed
    expect(toolOutput(r2)?.data).toBe("applied");
  });

  it("returns the first result on replay even when a re-run would fail (the 'string not found' regression)", async () => {
    const cache = new Map<string, unknown>();
    const rec = makeReplayStep(cache);
    let calls = 0;
    const onApply = () => {
      calls++;
      if (calls > 1) {
        // The exact symptom from the bug report: the original text is gone, so a
        // second apply throws — and the *failed* result used to reach the model.
        throw new Error("Edit 1/1: string not found");
      }
      return "edit applied";
    };

    mockedGetStepTools.mockResolvedValue(rec.step as any);
    const r1 = await makeEditRun(cache, onApply).run("hi", {
      state: createState({}),
    });
    expect(toolOutput(r1)?.data).toBe("edit applied");

    // Replay: the memoized step returns the first success; the throwing body
    // never runs again, so the model never sees "string not found".
    rec.reset();
    const r2 = await makeEditRun(cache, onApply).run("hi", {
      state: createState({}),
    });
    expect(calls).toBe(1);
    const content = toolOutput(r2);
    expect(content?.data).toBe("edit applied");
    expect(content?.error).toBeUndefined();
  });

  it("re-applies a wrapped tool's network.state mutation on replay (body skipped)", async () => {
    const cache = new Map<string, unknown>();
    const rec = makeReplayStep(cache);
    let bodyRuns = 0;

    const makeRun = () => {
      const stateTool = createTool({
        name: "set_plan",
        description: "writes to network.state.data (primary effect)",
        parameters: z.object({}),
        handler: async (_input, { network }) => {
          bodyRuns++;
          network.state.data.plan = "step-1";
          return "ok";
        },
      });
      const model = createMockModel({
        toolCalls: [{ toolCallId: "c1", toolName: "set_plan", args: {} }],
      });
      const agent = createAgent({
        name: "a",
        system: "s",
        model,
        tools: [stateTool],
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
    expect(bodyRuns).toBe(1);
    expect(r1.state.data.plan).toBe("step-1");

    // Replay: fresh state + same cache. The body must NOT re-run, yet the state
    // delta is restored from the memoized step payload (req. #2 — mutations are
    // not lost to memoization).
    rec.reset();
    const r2 = await makeRun().run("hi", { state: createState({}) });
    expect(bodyRuns).toBe(1);
    expect(rec.executed).toEqual([]);
    expect(r2.state.data.plan).toBe("step-1");
  });

  it("runs a manualStep tool inline with the LIVE step (not framework-wrapped)", async () => {
    const rec = makeReplayStep();
    let seenStep: unknown = "sentinel";

    const manualTool = createTool({
      name: "hitl",
      description: "manages its own steps",
      parameters: z.object({}),
      manualStep: true,
      handler: async (_input, { step }) => {
        seenStep = step;
        // It opens its own checkpoint — only possible with a live step (would be
        // undefined if the framework had wrapped this handler).
        return await step!.run("hitl/inner", async () => "done");
      },
    });
    const model = createMockModel({
      toolCalls: [{ toolCallId: "c1", toolName: "hitl", args: {} }],
    });
    const agent = createAgent({
      name: "a",
      system: "s",
      model,
      tools: [manualTool],
    });
    const network = createNetwork({
      name: "n",
      agents: [agent],
      defaultModel: model,
      maxIter: 1,
      router: () => agent,
    });

    mockedGetStepTools.mockResolvedValue(rec.step as any);
    const r = await network.run("hi", { state: createState({}) });

    expect(seenStep).toBe(rec.step); // got the live step
    expect(rec.ids).toContain("hitl/inner"); // its own checkpoint ran
    expect(rec.ids).not.toContain("a/tool/hitl/0"); // NOT framework-wrapped
    expect(toolOutput(r)?.data).toBe("done");
  });

  it("falls back to inline execution when there is no step (non-Inngest) without crashing", async () => {
    mockedGetStepTools.mockResolvedValue(undefined as any);
    let ran = 0;
    const tool = createTool({
      name: "noop2",
      description: "x",
      parameters: z.object({}),
      handler: async () => {
        ran++;
        return "ok";
      },
    });
    const model = createMockModel({
      toolCalls: [{ toolCallId: "c1", toolName: "noop2", args: {} }],
    });
    const agent = createAgent({ name: "a", system: "s", model, tools: [tool] });
    const network = createNetwork({
      name: "n",
      agents: [agent],
      defaultModel: model,
      maxIter: 1,
      router: () => agent,
    });

    const r = await network.run("hi", { state: createState({}) });
    expect(ran).toBe(1);
    expect(toolOutput(r)?.data).toBe("ok");
  });
});
