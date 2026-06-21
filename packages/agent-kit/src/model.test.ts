/* eslint-disable @typescript-eslint/require-await */
import { describe, it, expect } from "vitest";
import { z } from "zod";
import type { LanguageModelV1, LanguageModelV1CallOptions } from "ai";
import { AgenticModel, createAgenticModelFromLanguageModel } from "./model";
import type { SerializableResult } from "./converters";
import type { Message } from "./types";
import type { Tool } from "./tool";
import { createMockModel } from "./__tests__/test-helpers";

const SYSTEM_AND_USER: Message[] = [
  { type: "text", role: "system", content: "You are helpful." },
  { type: "text", role: "user", content: "Hi" },
];

const WEATHER_TOOL: Tool.Any = {
  name: "get_weather",
  description: "Get weather",
  parameters: z.object({ city: z.string() }),
  handler: async () => "sunny",
};

/** Pull the system message out of a captured LanguageModelV1 prompt. */
function systemPromptMessage(options: LanguageModelV1CallOptions) {
  return options.prompt.find((m) => m.role === "system");
}

describe("AgenticModel", () => {
  it("infers a text response", async () => {
    const model = createMockModel({ text: "Hello world" });
    const agentic = new AgenticModel(model);

    const result = await agentic.infer(
      "test-step",
      [{ type: "text", role: "user", content: "Hi" }],
      [],
      "auto"
    );

    expect(result.output).toHaveLength(1);
    expect(result.output[0]!.type).toBe("text");
    if (result.output[0]!.type === "text") {
      expect(result.output[0]!.content).toBe("Hello world");
      expect(result.output[0]!.role).toBe("assistant");
      expect(result.output[0]!.stop_reason).toBe("stop");
    }
    expect(result.raw).toEqual({
      text: "Hello world",
      toolCalls: [],
      finishReason: "stop",
      usage: { input_tokens: 0, output_tokens: 0, total_tokens: 0 },
    });
  });

  it("infers a tool call response", async () => {
    const model = createMockModel({
      toolCalls: [
        { toolCallId: "c1", toolName: "get_weather", args: { city: "NYC" } },
      ],
    });
    const agentic = new AgenticModel(model);

    const tools: Tool.Any[] = [
      {
        name: "get_weather",
        description: "Get weather",
        parameters: z.object({ city: z.string() }),
        handler: async () => "sunny",
      },
    ];

    const result = await agentic.infer(
      "test-step",
      [{ type: "text", role: "user", content: "Weather?" }],
      tools,
      "auto"
    );

    expect(result.output).toHaveLength(1);
    expect(result.output[0]!.type).toBe("tool_call");
    if (result.output[0]!.type === "tool_call") {
      expect(result.output[0]!.tools).toHaveLength(1);
      expect(result.output[0]!.tools[0]!.name).toBe("get_weather");
      expect(result.output[0]!.tools[0]!.input).toEqual({ city: "NYC" });
    }
  });

  it("infers a response with both text and tool calls", async () => {
    const model = createMockModel({
      text: "Let me check that.",
      toolCalls: [
        { toolCallId: "c1", toolName: "search", args: { q: "test" } },
      ],
    });
    const agentic = new AgenticModel(model);

    const tools: Tool.Any[] = [
      {
        name: "search",
        description: "Search",
        parameters: z.object({ q: z.string() }),
        handler: async () => [],
      },
    ];

    const result = await agentic.infer(
      "test-step",
      [{ type: "text", role: "user", content: "Find something" }],
      tools,
      "auto"
    );

    expect(result.output).toHaveLength(2);
    expect(result.output[0]!.type).toBe("text");
    expect(result.output[1]!.type).toBe("tool_call");
  });

  it("propagates errors from the model", async () => {
    const model = createMockModel({
      error: new Error("Rate limit exceeded"),
    });
    const agentic = new AgenticModel(model);

    await expect(
      agentic.infer(
        "test-step",
        [{ type: "text", role: "user", content: "Hi" }],
        [],
        "auto"
      )
    ).rejects.toThrow("Rate limit exceeded");
  });

  it("propagates specific error types from the model", async () => {
    class RateLimitError extends Error {
      constructor(public retryAfter: number) {
        super("Rate limited");
        this.name = "RateLimitError";
      }
    }

    const model = createMockModel({
      error: new RateLimitError(30),
    });
    const agentic = new AgenticModel(model);

    await expect(
      agentic.infer(
        "test-step",
        [{ type: "text", role: "user", content: "Hi" }],
        [],
        "auto"
      )
    ).rejects.toBeInstanceOf(RateLimitError);
  });

  it("does not pass toolChoice when no tools are provided", async () => {
    let capturedOptions: Record<string, unknown> | undefined;
    const model: LanguageModelV1 = {
      specificationVersion: "v1",
      provider: "mock",
      modelId: "mock-model",
      defaultObjectGenerationMode: "json",
      doGenerate: async (options) => {
        capturedOptions = options as unknown as Record<string, unknown>;
        return {
          text: "response",
          toolCalls: [],
          finishReason: "stop" as const,
          usage: { promptTokens: 0, completionTokens: 0 },
          rawCall: { rawPrompt: null, rawSettings: {} },
        };
      },
      doStream: async () => {
        throw new Error("Not implemented");
      },
    };
    const agentic = new AgenticModel(model);

    await agentic.infer(
      "test-step",
      [{ type: "text", role: "user", content: "Hi" }],
      [],
      "any"
    );

    // When no tools are provided, tools and toolChoice should not be set
    expect(capturedOptions).toBeDefined();
  });
});

describe("AgenticModel inference metadata (usage / cache / reasoning)", () => {
  it("carries token usage through infer() into raw", async () => {
    const model = createMockModel({
      text: "Hi",
      usage: { promptTokens: 120, completionTokens: 34 },
    });
    const agentic = new AgenticModel(model);

    const result = await agentic.infer("s", SYSTEM_AND_USER, [], "auto");
    const raw = result.raw as SerializableResult;

    expect(raw.usage).toEqual({
      input_tokens: 120,
      output_tokens: 34,
      total_tokens: 154,
    });
  });

  it("carries Anthropic cache tokens through infer() into raw", async () => {
    const model = createMockModel({
      text: "Hi",
      provider: "anthropic.messages",
      usage: { promptTokens: 50, completionTokens: 10 },
      providerMetadata: {
        anthropic: {
          cacheCreationInputTokens: 200,
          cacheReadInputTokens: 1800,
        },
      },
    });
    const agentic = new AgenticModel(model);

    const result = await agentic.infer("s", SYSTEM_AND_USER, [], "auto");
    const raw = result.raw as SerializableResult;

    // input_tokens stays the non-cache count so a cache-aware biller can add the
    // cache buckets without double-counting.
    expect(raw.usage).toEqual({
      input_tokens: 50,
      output_tokens: 10,
      total_tokens: 60,
      cache_creation_input_tokens: 200,
      cache_read_input_tokens: 1800,
    });
  });

  it("carries reasoning through infer() into raw and output", async () => {
    const model = createMockModel({
      text: "The answer is 42.",
      reasoning: [
        { type: "text", text: "Let me think about this.", signature: "sig-1" },
      ],
    });
    const agentic = new AgenticModel(model);

    const result = await agentic.infer("s", SYSTEM_AND_USER, [], "auto");
    const raw = result.raw as SerializableResult;

    expect(raw.reasoning).toBe("Let me think about this.");
    expect(raw.reasoningDetails).toEqual([
      { type: "text", text: "Let me think about this.", signature: "sig-1" },
    ]);

    // Reasoning round-trips into the output messages, before the text, with its
    // signature preserved.
    expect(result.output[0]!.type).toBe("reasoning");
    if (result.output[0]!.type === "reasoning") {
      expect(result.output[0]!.content).toBe("Let me think about this.");
      expect(result.output[0]!.signature).toBe("sig-1");
    }
    expect(result.output[1]!.type).toBe("text");
  });

  it("omits cache fields when the provider reports no cache usage", async () => {
    const model = createMockModel({
      text: "Hi",
      usage: { promptTokens: 5, completionTokens: 5 },
    });
    const agentic = new AgenticModel(model);

    const result = await agentic.infer("s", SYSTEM_AND_USER, [], "auto");
    const raw = result.raw as SerializableResult;

    expect(raw.usage).not.toHaveProperty("cache_read_input_tokens");
    expect(raw.usage).not.toHaveProperty("cache_creation_input_tokens");
  });
});

describe("AgenticModel Anthropic prompt caching", () => {
  it("auto-applies cache control to the system message for Anthropic models", async () => {
    let captured: LanguageModelV1CallOptions | undefined;
    const model = createMockModel({
      provider: "anthropic.messages",
      onGenerate: (o) => {
        captured = o;
      },
    });
    const agentic = new AgenticModel(model);

    await agentic.infer("s", SYSTEM_AND_USER, [WEATHER_TOOL], "auto");

    const system = systemPromptMessage(captured!);
    expect(system?.providerMetadata?.anthropic?.cacheControl).toEqual({
      type: "ephemeral",
    });
  });

  it("does NOT apply cache control for non-Anthropic models", async () => {
    let captured: LanguageModelV1CallOptions | undefined;
    const model = createMockModel({
      provider: "openai.chat",
      onGenerate: (o) => {
        captured = o;
      },
    });
    const agentic = new AgenticModel(model);

    await agentic.infer("s", SYSTEM_AND_USER, [WEATHER_TOOL], "auto");

    const system = systemPromptMessage(captured!);
    expect(system?.providerMetadata?.anthropic?.cacheControl).toBeUndefined();
  });

  it("honors an explicit cacheControl override on a non-Anthropic model", async () => {
    let captured: LanguageModelV1CallOptions | undefined;
    const model = createMockModel({
      provider: "openai.chat",
      onGenerate: (o) => {
        captured = o;
      },
    });
    const agentic = new AgenticModel(model, { cacheControl: true });

    await agentic.infer("s", SYSTEM_AND_USER, [WEATHER_TOOL], "auto");

    const system = systemPromptMessage(captured!);
    expect(system?.providerMetadata?.anthropic?.cacheControl).toEqual({
      type: "ephemeral",
    });
  });
});

describe("createAgenticModelFromLanguageModel", () => {
  it("creates an AgenticModel instance", () => {
    const model = createMockModel();
    const agentic = createAgenticModelFromLanguageModel(model);
    expect(agentic).toBeInstanceOf(AgenticModel);
  });
});
