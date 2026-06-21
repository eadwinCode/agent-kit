/* eslint-disable @typescript-eslint/require-await */
/**
 * End-to-end check against the REAL `@ai-sdk/anthropic` provider (AI SDK v6).
 *
 * `fetch` is mocked, so no network call is made, but the full pipeline runs:
 * our converters build `ModelMessage`s / tools, the real Anthropic provider
 * serialises them to a request body, and parses a canned Messages API response.
 * This proves the v6 converter output is actually accepted by the provider and
 * that cache markers + usage flow through end-to-end.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { createAnthropic } from "@ai-sdk/anthropic";
import { AgenticModel } from "../model";
import type { SerializableResult } from "../converters";
import type { Message } from "../types";
import type { Tool } from "../tool";
import { z } from "zod";

/** Canned non-streaming Anthropic Messages API response. */
const ANTHROPIC_RESPONSE = {
  type: "message",
  id: "msg_test",
  model: "claude-opus-4-20250514",
  role: "assistant",
  content: [{ type: "text", text: "Hello from Anthropic" }],
  stop_reason: "end_turn",
  usage: {
    input_tokens: 50, // Anthropic's input_tokens EXCLUDES cache
    output_tokens: 12,
    cache_creation_input_tokens: 30,
    cache_read_input_tokens: 800,
  },
};

let lastRequestBody: Record<string, unknown> | undefined;

function mockFetch(response: unknown) {
  return vi.fn(async (_url: string | URL | Request, init?: RequestInit) => {
    lastRequestBody = init?.body
      ? (JSON.parse(init.body as string) as Record<string, unknown>)
      : undefined;
    return new Response(JSON.stringify(response), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  });
}

afterEach(() => {
  vi.unstubAllGlobals();
  lastRequestBody = undefined;
});

describe("AgenticModel × real @ai-sdk/anthropic (v6)", () => {
  it("sends cache_control on system + last tool and round-trips usage/cache", async () => {
    vi.stubGlobal("fetch", mockFetch(ANTHROPIC_RESPONSE));

    const anthropic = createAnthropic({ apiKey: "test-key" });
    const model = anthropic("claude-opus-4-20250514");
    const agentic = new AgenticModel(model); // auto cacheControl (anthropic provider)

    const input: Message[] = [
      { type: "text", role: "system", content: "You are a helpful designer." },
      { type: "text", role: "user", content: "Build me a hero section." },
    ];
    const tools: Tool.Any[] = [
      {
        name: "write_file",
        description: "Write a file",
        parameters: z.object({ path: z.string(), contents: z.string() }),
        handler: async () => "ok",
      },
      {
        name: "read_file",
        description: "Read a file",
        // optional param must not 400 under strict mode
        parameters: z.object({
          path: z.string(),
          limit: z.number().optional(),
        }),
        handler: async () => "ok",
      },
    ];

    const result = await agentic.infer("step", input, tools, "auto");
    const raw = result.raw as SerializableResult;

    // The real provider accepted our ModelMessages + tools and built a request.
    expect(lastRequestBody).toBeDefined();

    // System prompt is emitted as a cacheable content block.
    const system = lastRequestBody!.system as Array<Record<string, unknown>>;
    expect(Array.isArray(system)).toBe(true);
    expect(system[system.length - 1]!.cache_control).toEqual({
      type: "ephemeral",
    });

    // The last tool carries the cache breakpoint (v6 forwards tool providerOptions).
    const reqTools = lastRequestBody!.tools as Array<Record<string, unknown>>;
    expect(reqTools).toHaveLength(2);
    expect(reqTools[reqTools.length - 1]!.cache_control).toEqual({
      type: "ephemeral",
    });

    // read_file's optional `limit` is NOT required (would 400 under strict mode).
    const readFile = reqTools.find((t) => t.name === "read_file")!;
    const schema = readFile.input_schema as { required?: string[] };
    expect(schema.required).toEqual(["path"]);

    // Usage round-trips with cache buckets kept separate (no double count).
    expect(raw.usage).toEqual({
      input_tokens: 50,
      output_tokens: 12,
      total_tokens: 892, // 50 + 30 + 800 input (incl cache) + 12 output
      cache_creation_input_tokens: 30,
      cache_read_input_tokens: 800,
    });
  });

  it("extracts a thinking block as reasoning with its signature", async () => {
    vi.stubGlobal(
      "fetch",
      mockFetch({
        ...ANTHROPIC_RESPONSE,
        content: [
          {
            type: "thinking",
            thinking: "Let me reason about this.",
            signature: "sig-abc",
          },
          { type: "text", text: "Done." },
        ],
      })
    );

    const anthropic = createAnthropic({ apiKey: "test-key" });
    const agentic = new AgenticModel(anthropic("claude-opus-4-20250514"));

    const result = await agentic.infer(
      "step",
      [{ type: "text", role: "user", content: "Think." }],
      [],
      "auto"
    );
    const raw = result.raw as SerializableResult;

    expect(raw.reasoning).toBe("Let me reason about this.");
    expect(raw.reasoningDetails).toEqual([
      { type: "text", text: "Let me reason about this.", signature: "sig-abc" },
    ]);

    // Reasoning is first in the output messages, with its signature preserved.
    expect(result.output[0]!.type).toBe("reasoning");
    if (result.output[0]!.type === "reasoning") {
      expect(result.output[0]!.signature).toBe("sig-abc");
    }
  });
});
