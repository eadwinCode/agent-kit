/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/require-await, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-assignment */
/**
 * Regression test for streaming part-id step-id collisions.
 *
 * When one inference calls the same tool more than once (e.g. two read_files),
 * the streaming part-id generator steps must each get a DISTINCT, replay-stable
 * id. Keying them on the tool name alone collided → Inngest auto-suffixed the
 * duplicate → the id drifted across replays → the tool-call `*.delta` chunks were
 * re-published and the client rendered the input doubled.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { z } from "zod";
import { createAgent, createTool } from "../index";
import { getStepTools } from "../util";
import { createMockModel } from "./test-helpers";

vi.mock("../util", async (importOriginal) => {
  const actual = await importOriginal<Record<string, unknown>>();
  return { ...actual, getStepTools: vi.fn() };
});
const mockedGetStepTools = vi.mocked(getStepTools);

/** Memoizing step mock (mimics Inngest): caches by id; duplicate ids within a
 *  run get an occurrence suffix (`id`, `id:1`, …) exactly like Inngest. */
function makeReplayStep(cache = new Map<string, unknown>()) {
  const ids: string[] = [];
  const occ = new Map<string, number>();
  const step = {
    run: async (id: string, fn: () => unknown) => {
      const n = occ.get(id) ?? 0;
      occ.set(id, n + 1);
      const uid = n === 0 ? id : `${id}:${n}`;
      ids.push(uid);
      if (cache.has(uid)) return cache.get(uid);
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
    occ.clear();
  };
  return { step, cache, ids, reset };
}

const readFile = createTool({
  name: "read_file",
  description: "read a file",
  parameters: z.object({ path: z.string() }),
  handler: async () => "contents",
});

/** The same tool called twice in one inference. */
function twoReadFilesModel() {
  return createMockModel({
    toolCalls: [
      { toolCallId: "c1", toolName: "read_file", args: { path: "a.css" } },
      { toolCallId: "c2", toolName: "read_file", args: { path: "b.css" } },
    ],
  });
}

beforeEach(() => mockedGetStepTools.mockReset());

describe("streaming part-id step ids — same tool twice in one inference", () => {
  it("gives each call a distinct, index-suffixed part-id step (no collision)", async () => {
    const rec = makeReplayStep();
    mockedGetStepTools.mockResolvedValue(rec.step as any);

    const agent = createAgent({
      name: "reader",
      system: "s",
      model: twoReadFilesModel(),
      tools: [readFile],
    });
    await agent.run("hi", {
      streaming: { publish: async () => {}, simulateChunking: false },
    });

    const toolPartIds = rec.ids.filter((id) =>
      id.startsWith("generate-tool-part-id-")
    );
    const outputPartIds = rec.ids.filter((id) =>
      id.startsWith("generate-output-part-id-")
    );

    // Two calls → two distinct part-id steps each, suffixed -0 and -1 …
    expect(toolPartIds).toHaveLength(2);
    expect(new Set(toolPartIds).size).toBe(2);
    expect(toolPartIds.some((id) => id.endsWith("-read_file-0"))).toBe(true);
    expect(toolPartIds.some((id) => id.endsWith("-read_file-1"))).toBe(true);
    expect(outputPartIds).toHaveLength(2);
    expect(new Set(outputPartIds).size).toBe(2);

    // … and NONE of them was Inngest-auto-suffixed (a `:1` means a collision).
    expect(rec.ids.some((id) => /generate-(tool|output)-part-id-.*:\d+$/.test(id))).toBe(
      false
    );
  });

  it("produces byte-identical part-id step ids across a replay", async () => {
    const cache = new Map<string, unknown>();
    const rec = makeReplayStep(cache);
    mockedGetStepTools.mockResolvedValue(rec.step as any);

    const run = () =>
      createAgent({
        name: "reader",
        system: "s",
        model: twoReadFilesModel(),
        tools: [readFile],
      }).run("hi", {
        streaming: { publish: async () => {}, simulateChunking: false },
      });

    await run();
    const partIds1 = rec.ids.filter((id) => id.includes("-part-id-"));

    rec.reset(); // fresh execution, SAME cache → simulated replay
    await run();
    const partIds2 = rec.ids.filter((id) => id.includes("-part-id-"));

    expect(partIds2).toEqual(partIds1);
  });
});
