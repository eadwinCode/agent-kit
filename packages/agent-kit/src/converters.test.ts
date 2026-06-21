/* eslint-disable @typescript-eslint/require-await */
import { describe, it, expect } from "vitest";
import { z } from "zod";
import {
  messagesToCoreMessages,
  resultToMessages,
  toolsToAiTools,
  toSerializableResult,
  mapToolChoice,
  type SerializableResult,
} from "./converters";
import type { Message } from "./types";
import type { Tool } from "./tool";

const EPHEMERAL = { anthropic: { cacheControl: { type: "ephemeral" } } };

/** Extract the underlying JSON schema from an AI SDK `jsonSchema()` wrapper. */
function rawSchema(parameters: unknown): {
  required?: string[];
  properties?: Record<string, unknown>;
} {
  return (parameters as { jsonSchema: Record<string, unknown> }).jsonSchema;
}

/** First content part of a CoreMessage with array content (test-only cast). */
function firstPart(msg: unknown): Record<string, unknown> {
  return (msg as { content: Array<Record<string, unknown>> }).content[0]!;
}

describe("messagesToCoreMessages", () => {
  it("converts a system text message", () => {
    const messages: Message[] = [
      { type: "text", role: "system", content: "You are helpful." },
    ];
    const result = messagesToCoreMessages(messages);
    expect(result).toEqual([{ role: "system", content: "You are helpful." }]);
  });

  it("converts a user text message", () => {
    const messages: Message[] = [
      { type: "text", role: "user", content: "Hello" },
    ];
    const result = messagesToCoreMessages(messages);
    expect(result).toEqual([{ role: "user", content: "Hello" }]);
  });

  it("converts an assistant text message", () => {
    const messages: Message[] = [
      { type: "text", role: "assistant", content: "Hi there" },
    ];
    const result = messagesToCoreMessages(messages);
    expect(result).toEqual([{ role: "assistant", content: "Hi there" }]);
  });

  it("converts array content to joined string", () => {
    const messages: Message[] = [
      {
        type: "text",
        role: "user",
        content: [
          { type: "text", text: "Hello " },
          { type: "text", text: "world" },
        ],
      },
    ];
    const result = messagesToCoreMessages(messages);
    expect(result).toEqual([{ role: "user", content: "Hello world" }]);
  });

  it("handles empty array content", () => {
    const messages: Message[] = [{ type: "text", role: "user", content: [] }];
    const result = messagesToCoreMessages(messages);
    expect(result).toEqual([{ role: "user", content: "" }]);
  });

  it("converts a tool_call message to assistant with tool-call parts", () => {
    const messages: Message[] = [
      {
        type: "tool_call",
        role: "assistant",
        stop_reason: "tool",
        tools: [
          {
            type: "tool",
            id: "call_1",
            name: "get_weather",
            input: { city: "London" },
          },
        ],
      },
    ];
    const result = messagesToCoreMessages(messages);
    expect(result).toEqual([
      {
        role: "assistant",
        content: [
          {
            type: "tool-call",
            toolCallId: "call_1",
            toolName: "get_weather",
            args: { city: "London" },
          },
        ],
      },
    ]);
  });

  it("converts multiple tool calls in a single message", () => {
    const messages: Message[] = [
      {
        type: "tool_call",
        role: "assistant",
        stop_reason: "tool",
        tools: [
          { type: "tool", id: "call_1", name: "tool_a", input: { x: 1 } },
          { type: "tool", id: "call_2", name: "tool_b", input: { y: 2 } },
        ],
      },
    ];
    const result = messagesToCoreMessages(messages);
    expect(result).toHaveLength(1);
    expect(result[0]!.role).toBe("assistant");
    const content = (result[0] as { role: string; content: unknown[] }).content;
    expect(content).toHaveLength(2);
  });

  it("converts a tool_result message to tool role", () => {
    const messages: Message[] = [
      {
        type: "tool_result",
        role: "tool_result",
        tool: {
          type: "tool",
          id: "call_1",
          name: "get_weather",
          input: { city: "London" },
        },
        content: { temperature: 20 },
        stop_reason: "tool",
      },
    ];
    const result = messagesToCoreMessages(messages);
    expect(result).toEqual([
      {
        role: "tool",
        content: [
          {
            type: "tool-result",
            toolCallId: "call_1",
            toolName: "get_weather",
            result: { temperature: 20 },
          },
        ],
      },
    ]);
  });

  it("converts a mixed conversation", () => {
    const messages: Message[] = [
      { type: "text", role: "system", content: "System prompt" },
      { type: "text", role: "user", content: "What's the weather?" },
      {
        type: "tool_call",
        role: "assistant",
        stop_reason: "tool",
        tools: [
          { type: "tool", id: "c1", name: "weather", input: { city: "NYC" } },
        ],
      },
      {
        type: "tool_result",
        role: "tool_result",
        tool: {
          type: "tool",
          id: "c1",
          name: "weather",
          input: { city: "NYC" },
        },
        content: "Sunny, 75F",
        stop_reason: "tool",
      },
      {
        type: "text",
        role: "assistant",
        content: "It's sunny and 75F in NYC.",
      },
    ];
    const result = messagesToCoreMessages(messages);
    expect(result).toHaveLength(5);
    expect(result[0]!.role).toBe("system");
    expect(result[1]!.role).toBe("user");
    expect(result[2]!.role).toBe("assistant");
    expect(result[3]!.role).toBe("tool");
    expect(result[4]!.role).toBe("assistant");
  });

  it("returns empty array for empty input", () => {
    expect(messagesToCoreMessages([])).toEqual([]);
  });
});

describe("resultToMessages", () => {
  it("converts text-only response", () => {
    const result: SerializableResult = {
      text: "Hello world",
      toolCalls: [],
      finishReason: "stop",
    };
    const messages = resultToMessages(result);
    expect(messages).toHaveLength(1);
    expect(messages[0]).toEqual({
      type: "text",
      role: "assistant",
      content: "Hello world",
      stop_reason: "stop",
    });
  });

  it("converts tool-call-only response (no text)", () => {
    const result: SerializableResult = {
      text: "",
      toolCalls: [
        { toolCallId: "c1", toolName: "search", args: { q: "test" } },
      ],
      finishReason: "tool-calls",
    };
    const messages = resultToMessages(result);
    expect(messages).toHaveLength(1);
    expect(messages[0]!.type).toBe("tool_call");
    if (messages[0]!.type === "tool_call") {
      expect(messages[0]!.tools).toHaveLength(1);
      expect(messages[0]!.tools[0]!.id).toBe("c1");
      expect(messages[0]!.tools[0]!.name).toBe("search");
    }
  });

  it("converts response with both text and tool calls", () => {
    const result: SerializableResult = {
      text: "Let me search for that.",
      toolCalls: [
        { toolCallId: "c1", toolName: "search", args: { q: "test" } },
      ],
      finishReason: "tool-calls",
    };
    const messages = resultToMessages(result);
    expect(messages).toHaveLength(2);
    expect(messages[0]!.type).toBe("text");
    if (messages[0]!.type === "text") {
      expect(messages[0]!.stop_reason).toBe("tool");
    }
    expect(messages[1]!.type).toBe("tool_call");
  });

  it("returns empty text message when no text and no tool calls", () => {
    const result: SerializableResult = {
      text: "",
      toolCalls: [],
      finishReason: "stop",
    };
    const messages = resultToMessages(result);
    expect(messages).toHaveLength(1);
    expect(messages[0]).toEqual({
      type: "text",
      role: "assistant",
      content: "",
      stop_reason: "stop",
    });
  });

  it("treats whitespace-only text as empty", () => {
    const result: SerializableResult = {
      text: "   \n  ",
      toolCalls: [],
      finishReason: "stop",
    };
    const messages = resultToMessages(result);
    expect(messages).toHaveLength(1);
    expect(messages[0]!.type).toBe("text");
    if (messages[0]!.type === "text") {
      // Whitespace-only text is treated as empty, so fallback empty message
      expect(messages[0]!.content).toBe("");
    }
  });

  it("maps multiple tool calls", () => {
    const result: SerializableResult = {
      text: "",
      toolCalls: [
        { toolCallId: "c1", toolName: "tool_a", args: { x: 1 } },
        { toolCallId: "c2", toolName: "tool_b", args: { y: 2 } },
      ],
      finishReason: "tool-calls",
    };
    const messages = resultToMessages(result);
    expect(messages).toHaveLength(1);
    if (messages[0]!.type === "tool_call") {
      expect(messages[0]!.tools).toHaveLength(2);
      expect(messages[0]!.tools[0]!.name).toBe("tool_a");
      expect(messages[0]!.tools[1]!.name).toBe("tool_b");
    }
  });
});

describe("toolsToAiTools", () => {
  it("converts a tool with zod parameters", () => {
    const tools: Tool.Any[] = [
      {
        name: "get_weather",
        description: "Get weather for a city",
        parameters: z.object({ city: z.string() }),
        handler: async () => "sunny",
      },
    ];
    const result = toolsToAiTools(tools);
    expect(result).toHaveProperty("get_weather");
    expect(result["get_weather"]!.description).toBe("Get weather for a city");
    // The parameters should be a JSON schema wrapper
    expect(result["get_weather"]!.parameters).toBeDefined();
  });

  it("converts a tool without parameters to empty object schema", () => {
    const tools: Tool.Any[] = [
      {
        name: "ping",
        description: "Ping",
        handler: async () => "pong",
      },
    ];
    const result = toolsToAiTools(tools);
    expect(result).toHaveProperty("ping");
    expect(result["ping"]!.parameters).toBeDefined();
  });

  it("converts multiple tools", () => {
    const tools: Tool.Any[] = [
      {
        name: "tool_a",
        description: "A",
        parameters: z.object({ x: z.number() }),
        handler: async () => {},
      },
      {
        name: "tool_b",
        description: "B",
        parameters: z.object({ y: z.string() }),
        handler: async () => {},
      },
    ];
    const result = toolsToAiTools(tools);
    expect(Object.keys(result)).toEqual(["tool_a", "tool_b"]);
  });

  it("returns empty object for empty tools array", () => {
    const result = toolsToAiTools([]);
    expect(result).toEqual({});
  });
});

describe("mapToolChoice", () => {
  it("maps 'auto' to 'auto'", () => {
    expect(mapToolChoice("auto")).toBe("auto");
  });

  it("maps 'any' to 'required'", () => {
    expect(mapToolChoice("any")).toBe("required");
  });

  it("maps a specific tool name to tool object", () => {
    expect(mapToolChoice("get_weather")).toEqual({
      type: "tool",
      toolName: "get_weather",
    });
  });

  it("maps an arbitrary string to tool object", () => {
    expect(mapToolChoice("my_custom_tool")).toEqual({
      type: "tool",
      toolName: "my_custom_tool",
    });
  });
});

describe("messagesToCoreMessages — vision / images", () => {
  it("converts user image content parts into AI SDK image parts", () => {
    const messages: Message[] = [
      {
        type: "text",
        role: "user",
        content: [
          { type: "text", text: "What is this?" },
          { type: "image", image: "https://example.com/cat.png" },
        ],
      },
    ];
    const result = messagesToCoreMessages(messages);
    expect(result).toEqual([
      {
        role: "user",
        content: [
          { type: "text", text: "What is this?" },
          { type: "image", image: "https://example.com/cat.png" },
        ],
      },
    ]);
  });

  it("passes a user image mimeType through", () => {
    const messages: Message[] = [
      {
        type: "text",
        role: "user",
        content: [
          { type: "image", image: "BASE64DATA", mimeType: "image/jpeg" },
        ],
      },
    ];
    const result = messagesToCoreMessages(messages);
    expect(result[0]).toEqual({
      role: "user",
      content: [{ type: "image", image: "BASE64DATA", mimeType: "image/jpeg" }],
    });
  });

  it("converts an Anthropic-native base64 image in a tool result into multipart content", () => {
    const messages: Message[] = [
      {
        type: "tool_result",
        role: "tool_result",
        tool: { type: "tool", id: "c1", name: "screenshot", input: {} },
        content: [
          { type: "text", text: '{"ok":true}' },
          {
            type: "image",
            source: { type: "base64", media_type: "image/png", data: "AAAA" },
          },
        ],
        stop_reason: "tool",
      },
    ];
    const result = messagesToCoreMessages(messages);
    const part = firstPart(result[0]);
    expect(part.type).toBe("tool-result");
    expect(part.experimental_content).toEqual([
      { type: "text", text: '{"ok":true}' },
      { type: "image", data: "AAAA", mimeType: "image/png" },
    ]);
  });

  it("converts an AI SDK-shaped image in a tool result into multipart content", () => {
    const messages: Message[] = [
      {
        type: "tool_result",
        role: "tool_result",
        tool: { type: "tool", id: "c1", name: "screenshot", input: {} },
        content: [{ type: "image", data: "BBBB", mimeType: "image/jpeg" }],
        stop_reason: "tool",
      },
    ];
    const result = messagesToCoreMessages(messages);
    const part = firstPart(result[0]);
    expect(part.experimental_content).toEqual([
      { type: "image", data: "BBBB", mimeType: "image/jpeg" },
    ]);
  });

  it("surfaces a URL-only tool-result image as text (base64-only AI SDK part)", () => {
    const messages: Message[] = [
      {
        type: "tool_result",
        role: "tool_result",
        tool: { type: "tool", id: "c1", name: "screenshot", input: {} },
        content: [
          { type: "text", text: "see image" },
          {
            type: "image",
            source: { type: "url", url: "https://example.com/x.png" },
          },
        ],
        stop_reason: "tool",
      },
    ];
    const result = messagesToCoreMessages(messages);
    const part = firstPart(result[0]);
    // No embeddable image → no multipart content; URL is not silently dropped
    // because the original `result` still carries the blocks.
    expect(part.experimental_content).toBeUndefined();
    expect(part.result).toEqual(
      messages[0]!.type === "tool_result" ? messages[0]!.content : undefined
    );
  });

  it("leaves string tool-result content unchanged (no multipart)", () => {
    const messages: Message[] = [
      {
        type: "tool_result",
        role: "tool_result",
        tool: { type: "tool", id: "c1", name: "get_weather", input: {} },
        content: "Sunny, 75F",
        stop_reason: "tool",
      },
    ];
    const result = messagesToCoreMessages(messages);
    const part = firstPart(result[0]);
    expect(part.result).toBe("Sunny, 75F");
    expect(part.experimental_content).toBeUndefined();
  });
});

describe("messagesToCoreMessages — reasoning round-trip", () => {
  it("converts a reasoning message with details into assistant reasoning parts", () => {
    const messages: Message[] = [
      {
        type: "reasoning",
        role: "assistant",
        content: "thinking",
        signature: "sig",
        details: [{ type: "text", text: "thinking", signature: "sig" }],
      },
    ];
    const result = messagesToCoreMessages(messages);
    expect(result).toEqual([
      {
        role: "assistant",
        content: [{ type: "reasoning", text: "thinking", signature: "sig" }],
      },
    ]);
  });

  it("converts redacted reasoning details", () => {
    const messages: Message[] = [
      {
        type: "reasoning",
        role: "assistant",
        content: "",
        details: [{ type: "redacted", data: "opaque" }],
      },
    ];
    const result = messagesToCoreMessages(messages);
    expect(result).toEqual([
      {
        role: "assistant",
        content: [{ type: "redacted-reasoning", data: "opaque" }],
      },
    ]);
  });

  it("falls back to flat content + signature when there are no details", () => {
    const messages: Message[] = [
      {
        type: "reasoning",
        role: "assistant",
        content: "hmm",
        signature: "s2",
      },
    ];
    const result = messagesToCoreMessages(messages);
    expect(result).toEqual([
      {
        role: "assistant",
        content: [{ type: "reasoning", text: "hmm", signature: "s2" }],
      },
    ]);
  });

  it("drops an empty reasoning message", () => {
    const messages: Message[] = [
      { type: "reasoning", role: "assistant", content: "" },
    ];
    expect(messagesToCoreMessages(messages)).toEqual([]);
  });

  it("orders reasoning before tool-call within an assistant turn", () => {
    const messages: Message[] = [
      {
        type: "reasoning",
        role: "assistant",
        content: "plan",
        details: [{ type: "text", text: "plan", signature: "sig" }],
      },
      {
        type: "tool_call",
        role: "assistant",
        stop_reason: "tool",
        tools: [{ type: "tool", id: "c1", name: "search", input: { q: "x" } }],
      },
    ];
    const result = messagesToCoreMessages(messages);
    expect(result).toHaveLength(2);
    expect(firstPart(result[0]).type).toBe("reasoning");
    expect(firstPart(result[1]).type).toBe("tool-call");
  });
});

describe("resultToMessages — reasoning", () => {
  it("emits a reasoning message before the text message", () => {
    const result: SerializableResult = {
      text: "The answer is 42.",
      toolCalls: [],
      finishReason: "stop",
      reasoning: "because reasons",
      reasoningDetails: [
        { type: "text", text: "because reasons", signature: "sig" },
      ],
    };
    const messages = resultToMessages(result);
    expect(messages).toHaveLength(2);
    expect(messages[0]!.type).toBe("reasoning");
    if (messages[0]!.type === "reasoning") {
      expect(messages[0]!.content).toBe("because reasons");
      expect(messages[0]!.signature).toBe("sig");
      expect(messages[0]!.details).toEqual([
        { type: "text", text: "because reasons", signature: "sig" },
      ]);
    }
    expect(messages[1]!.type).toBe("text");
  });

  it("orders reasoning → text → tool_call", () => {
    const result: SerializableResult = {
      text: "Let me look.",
      toolCalls: [{ toolCallId: "c1", toolName: "search", args: { q: "x" } }],
      finishReason: "tool-calls",
      reasoning: "I should search",
    };
    const messages = resultToMessages(result);
    expect(messages.map((m) => m.type)).toEqual([
      "reasoning",
      "text",
      "tool_call",
    ]);
  });

  it("does not emit a reasoning message when there is no reasoning", () => {
    const result: SerializableResult = {
      text: "hi",
      toolCalls: [],
      finishReason: "stop",
    };
    const messages = resultToMessages(result);
    expect(messages).toHaveLength(1);
    expect(messages[0]!.type).toBe("text");
  });
});

describe("messagesToCoreMessages / toolsToAiTools — cache control", () => {
  it("marks the system message when cacheControl is enabled", () => {
    const messages: Message[] = [
      { type: "text", role: "system", content: "sys" },
      { type: "text", role: "user", content: "hi" },
    ];
    const result = messagesToCoreMessages(messages, { cacheControl: true });
    expect(
      (result[0] as { providerOptions?: unknown }).providerOptions
    ).toEqual(EPHEMERAL);
    expect(
      (result[1] as { providerOptions?: unknown }).providerOptions
    ).toBeUndefined();
  });

  it("does not mark the system message when cacheControl is off", () => {
    const messages: Message[] = [
      { type: "text", role: "system", content: "sys" },
    ];
    const result = messagesToCoreMessages(messages);
    expect(
      (result[0] as { providerOptions?: unknown }).providerOptions
    ).toBeUndefined();
  });

  it("marks the last tool when cacheControl is enabled", () => {
    const tools: Tool.Any[] = [
      {
        name: "tool_a",
        description: "A",
        parameters: z.object({ x: z.number() }),
        handler: async () => {},
      },
      {
        name: "tool_b",
        description: "B",
        parameters: z.object({ y: z.string() }),
        handler: async () => {},
      },
    ];
    const result = toolsToAiTools(tools, { cacheControl: true });
    expect(
      (result["tool_b"] as { providerOptions?: unknown }).providerOptions
    ).toEqual(EPHEMERAL);
    expect(
      (result["tool_a"] as { providerOptions?: unknown }).providerOptions
    ).toBeUndefined();
  });
});

describe("toolsToAiTools — OpenAI optional params (no forced strict)", () => {
  it("keeps optional parameters out of the JSON schema `required` list", () => {
    const tools: Tool.Any[] = [
      {
        name: "read_file",
        description: "Read a file",
        parameters: z.object({
          path: z.string(),
          limit: z.number().optional(),
        }),
        handler: async () => "",
      },
    ];
    const result = toolsToAiTools(tools);
    const schema = rawSchema(result["read_file"]!.parameters);
    // Optional `limit` must NOT be required — strict mode would 400 otherwise.
    expect(schema.required).toEqual(["path"]);
    expect(schema.properties).toHaveProperty("limit");
  });

  it("does not force `strict: true` on tools with parameters", () => {
    const tools: Tool.Any[] = [
      {
        name: "with_params",
        description: "x",
        parameters: z.object({ a: z.string() }),
        handler: async () => {},
      },
    ];
    const result = toolsToAiTools(tools);
    expect(
      (result["with_params"] as { strict?: unknown }).strict
    ).toBeUndefined();
  });
});

describe("toSerializableResult", () => {
  const base = {
    text: "hi",
    toolCalls: [],
    finishReason: "stop",
  };

  it("normalizes usage to snake_case input/output/total", () => {
    const out = toSerializableResult({
      ...base,
      usage: { promptTokens: 100, completionTokens: 20, totalTokens: 120 },
    });
    expect(out.usage).toEqual({
      input_tokens: 100,
      output_tokens: 20,
      total_tokens: 120,
    });
  });

  it("extracts Anthropic cache tokens separately (no double count)", () => {
    const out = toSerializableResult({
      ...base,
      usage: { promptTokens: 40, completionTokens: 5, totalTokens: 45 },
      providerMetadata: {
        anthropic: {
          cacheCreationInputTokens: 10,
          cacheReadInputTokens: 900,
        },
      },
    });
    expect(out.usage).toEqual({
      input_tokens: 40,
      output_tokens: 5,
      total_tokens: 45,
      cache_creation_input_tokens: 10,
      cache_read_input_tokens: 900,
    });
  });

  it("falls back to experimental_providerMetadata for cache tokens", () => {
    const out = toSerializableResult({
      ...base,
      usage: { promptTokens: 1, completionTokens: 1, totalTokens: 2 },
      experimental_providerMetadata: {
        anthropic: { cacheReadInputTokens: 7 },
      },
    });
    expect(out.usage?.cache_read_input_tokens).toBe(7);
  });

  it("carries reasoning and reasoningDetails", () => {
    const out = toSerializableResult({
      ...base,
      reasoning: "because",
      reasoningDetails: [{ type: "text", text: "because", signature: "s" }],
    });
    expect(out.reasoning).toBe("because");
    expect(out.reasoningDetails).toEqual([
      { type: "text", text: "because", signature: "s" },
    ]);
  });

  it("omits usage entirely when neither usage nor cache tokens are present", () => {
    const out = toSerializableResult({ ...base });
    expect(out.usage).toBeUndefined();
  });

  it("omits empty reasoning", () => {
    const out = toSerializableResult({ ...base, reasoning: "   " });
    expect(out.reasoning).toBeUndefined();
  });
});
