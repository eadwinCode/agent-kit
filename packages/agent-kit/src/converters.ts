/**
 * Converters between internal Message/Tool types and the Vercel AI SDK types.
 *
 * Targets the AI SDK v6 model interface: `ModelMessage` / `Tool` / `ToolResultOutput`.
 *
 * @module
 */
import {
  jsonSchema,
  type JSONValue,
  type ImagePart,
  type ModelMessage,
  type ProviderMetadata,
  type TextPart,
  type Tool,
  type ToolResultPart,
} from "ai";
import { z } from "zod";
import {
  type ImageContent,
  type Message,
  type ReasoningDetail,
  type ReasoningMessage,
  type TextMessage,
  type ToolCallMessage,
  type ToolMessage,
} from "./types";
import { type Tool as AgentKitTool } from "./tool";

/** Anthropic ephemeral cache breakpoint marker (provider-specific metadata). */
const ANTHROPIC_CACHE_CONTROL: ProviderMetadata = {
  anthropic: { cacheControl: { type: "ephemeral" } },
};

/** Tool-result output shape accepted by the AI SDK (text / json / content). */
type ToolResultOutput = ToolResultPart["output"];
/** The `value` of a multi-part (`type: "content"`) tool result. */
type ToolResultContentValue = Extract<
  ToolResultOutput,
  { type: "content" }
>["value"];

/** Assistant reasoning part as accepted by the AI SDK v6 message format. */
type ReasoningContentPart = {
  type: "reasoning";
  text: string;
  providerOptions?: ProviderMetadata;
};

/**
 * Options shared by the message/tool converters.
 */
export interface ConvertOptions {
  /**
   * When true, attach Anthropic ephemeral `cacheControl` breakpoints (the system
   * message via {@link messagesToCoreMessages}, the last tool via
   * {@link toolsToAiTools}). The markers live under the `anthropic` provider key,
   * so non-Anthropic providers simply ignore them — it is safe to leave on.
   */
  cacheControl?: boolean;
}

/**
 * Convert internal Message[] to AI SDK ModelMessage[].
 */
export function messagesToCoreMessages(
  messages: Message[],
  opts: ConvertOptions = {}
): ModelMessage[] {
  const result: ModelMessage[] = [];

  for (const msg of messages) {
    switch (msg.type) {
      case "text": {
        result.push(textMessageToModelMessage(msg));
        break;
      }
      case "reasoning": {
        const parts = reasoningMessageToParts(msg);
        // Drop empty reasoning turns rather than emit a contentless assistant
        // message (which some providers reject).
        if (parts.length > 0) {
          result.push({ role: "assistant", content: parts });
        }
        break;
      }
      case "tool_call": {
        // Convert to assistant message with tool-call parts
        result.push({
          role: "assistant",
          content: msg.tools.map((tool) => ({
            type: "tool-call" as const,
            toolCallId: tool.id,
            toolName: tool.name,
            input: tool.input,
          })),
        });
        break;
      }
      case "tool_result": {
        // Convert to tool message with tool-result part. When the result carries
        // image content (e.g. a screenshot tool), emit multi-part `content`
        // output so a vision-capable model can actually see the image.
        const part: ToolResultPart = {
          type: "tool-result",
          toolCallId: msg.tool.id,
          toolName: msg.tool.name,
          output: toToolResultOutput(msg.content),
        };
        result.push({ role: "tool", content: [part] });
        break;
      }
    }
  }

  if (opts.cacheControl) {
    applySystemCacheControl(result);
  }

  return result;
}

/**
 * Convert a single internal text message to a ModelMessage. User messages with
 * image parts emit structured `UserContent` (text + image parts); everything
 * else collapses to a joined string, preserving prior behaviour.
 */
function textMessageToModelMessage(msg: TextMessage): ModelMessage {
  if (
    typeof msg.content !== "string" &&
    msg.role === "user" &&
    msg.content.some((c) => c.type === "image")
  ) {
    const parts: Array<TextPart | ImagePart> = msg.content.map((c) =>
      c.type === "image"
        ? imageContentToImagePart(c)
        : { type: "text", text: c.text }
    );
    return { role: "user", content: parts };
  }

  const content =
    typeof msg.content === "string"
      ? msg.content
      : msg.content.map((c) => (c.type === "text" ? c.text : "")).join("");
  return { role: msg.role, content };
}

function imageContentToImagePart(c: ImageContent): ImagePart {
  return c.mimeType
    ? { type: "image", image: c.image, mediaType: c.mimeType }
    : { type: "image", image: c.image };
}

/**
 * Convert a reasoning message back into assistant reasoning parts. Prefers the
 * structured `details` (which preserve per-block signatures Anthropic needs to
 * replay a thinking block before a tool-use block); falls back to the flat text.
 * In v6 the signature travels in `providerOptions.anthropic.signature`.
 */
function reasoningMessageToParts(
  msg: ReasoningMessage
): ReasoningContentPart[] {
  if (msg.details && msg.details.length > 0) {
    return msg.details.map((d) =>
      d.type === "redacted"
        ? {
            type: "reasoning",
            text: "",
            providerOptions: { anthropic: { redactedData: d.data } },
          }
        : d.signature
          ? {
              type: "reasoning",
              text: d.text,
              providerOptions: { anthropic: { signature: d.signature } },
            }
          : { type: "reasoning", text: d.text }
    );
  }
  if (msg.content && msg.content.length > 0) {
    return [
      msg.signature
        ? {
            type: "reasoning",
            text: msg.content,
            providerOptions: { anthropic: { signature: msg.signature } },
          }
        : { type: "reasoning", text: msg.content },
    ];
  }
  return [];
}

/**
 * Build the AI SDK tool-result output from internal tool content:
 *  - a string  → `{ type: "text" }`
 *  - an array carrying image blocks → `{ type: "content" }` multi-part output so
 *    a vision model can see the image(s)
 *  - anything else → `{ type: "json" }`
 *
 * Recognises AI SDK image blocks (`{ data, mediaType }`), Anthropic-native blocks
 * (`{ source: { type, data/url, media_type } }`), and a generic
 * `{ image: <url|base64|dataURL> }` shape.
 */
function toToolResultOutput(content: unknown): ToolResultOutput {
  if (typeof content === "string") {
    return { type: "text", value: content };
  }

  const multipart = toToolResultContentValue(content);
  if (multipart) {
    return { type: "content", value: multipart };
  }

  return { type: "json", value: content as JSONValue };
}

function toToolResultContentValue(
  content: unknown
): ToolResultContentValue | undefined {
  if (!Array.isArray(content)) return undefined;

  const parts: ToolResultContentValue = [];
  let hasImage = false;

  for (const block of content) {
    if (!block || typeof block !== "object") {
      parts.push({ type: "text", text: String(block) });
      continue;
    }
    const b = block as Record<string, unknown>;

    if (b.type === "text" && typeof b.text === "string") {
      parts.push({ type: "text", text: b.text });
      continue;
    }

    if (b.type === "image") {
      const img = extractImage(b);
      if (img) {
        if (img.kind === "data") {
          parts.push({
            type: "image-data",
            data: img.data,
            mediaType: img.mediaType,
          });
        } else {
          parts.push({ type: "image-url", url: img.url });
        }
        hasImage = true;
        continue;
      }
    }

    // Unknown block — stringify so nothing is silently lost.
    parts.push({ type: "text", text: JSON.stringify(block) });
  }

  return hasImage ? parts : undefined;
}

type ExtractedImage =
  | { kind: "data"; data: string; mediaType: string }
  | { kind: "url"; url: string };

function extractImage(b: Record<string, unknown>): ExtractedImage | undefined {
  // Anthropic-native: { source: { type: "base64" | "url", ... } }
  const source = b.source as Record<string, unknown> | undefined;
  if (source && typeof source === "object") {
    if (source.type === "base64" && typeof source.data === "string") {
      return {
        kind: "data",
        data: source.data,
        mediaType:
          typeof source.media_type === "string"
            ? source.media_type
            : "image/png",
      };
    }
    if (source.type === "url" && typeof source.url === "string") {
      return { kind: "url", url: source.url };
    }
  }
  // AI SDK image block: { data, mediaType } (also tolerate legacy mimeType)
  if (typeof b.data === "string") {
    const mediaType = b.mediaType ?? b.mimeType;
    return {
      kind: "data",
      data: b.data,
      mediaType: typeof mediaType === "string" ? mediaType : "image/png",
    };
  }
  // Generic: { image: <url | base64 | dataURL>, mediaType? }
  if (typeof b.image === "string") {
    const mediaType = b.mediaType ?? b.mimeType;
    return parseImageString(
      b.image,
      typeof mediaType === "string" ? mediaType : undefined
    );
  }
  return undefined;
}

function parseImageString(s: string, mediaType?: string): ExtractedImage {
  const dataUrl = /^data:([^;]+);base64,([\s\S]*)$/.exec(s);
  if (dataUrl) {
    return { kind: "data", data: dataUrl[2]!, mediaType: dataUrl[1]! };
  }
  if (/^https?:\/\//i.test(s)) {
    return { kind: "url", url: s };
  }
  return { kind: "data", data: s, mediaType: mediaType ?? "image/png" };
}

/**
 * Mark the last system message as an Anthropic cache breakpoint. Anthropic orders
 * a request as [tools, system, messages], and a cache_control marker caches the
 * whole prefix up to that block — so marking the system message caches the tool
 * definitions + system prompt together.
 */
function applySystemCacheControl(messages: ModelMessage[]): void {
  for (let i = messages.length - 1; i >= 0; i--) {
    const m = messages[i]!;
    if (m.role === "system") {
      m.providerOptions = {
        ...(m.providerOptions ?? {}),
        ...ANTHROPIC_CACHE_CONTROL,
      };
      return;
    }
  }
}

/**
 * Serializable subset of generateText result for step.run() compatibility.
 */
export interface SerializableResult {
  text: string;
  toolCalls: Array<{
    toolCallId: string;
    toolName: string;
    args: unknown;
  }>;
  finishReason: string;
  /**
   * Token usage, normalised across providers and safe to serialise across
   * `step.run()`. Field names mirror the Anthropic API: `input_tokens` is the
   * NON-cache prompt count (the AI SDK v6 `usage.inputTokens` total includes
   * cache, so we read `inputTokenDetails.noCacheTokens`); the cache buckets are
   * kept separate so cache-aware billing can add them in without double-counting.
   */
  usage?: SerializableUsage;
  /** Concatenated reasoning text, when the model exposes chain-of-thought. */
  reasoning?: string;
  /** Structured reasoning blocks; preserves signatures for round-tripping. */
  reasoningDetails?: ReasoningDetail[];
}

export interface SerializableUsage {
  input_tokens: number;
  output_tokens: number;
  total_tokens: number;
  cache_creation_input_tokens?: number;
  cache_read_input_tokens?: number;
}

/**
 * Structural subset of the AI SDK v6 `generateText` result that we read from.
 * Declared locally so the converter is independent of the exact SDK result type.
 */
export interface AiResultLike {
  text: string;
  toolCalls: Array<{ toolCallId: string; toolName: string; input: unknown }>;
  finishReason: string;
  usage?: {
    inputTokens?: number;
    outputTokens?: number;
    totalTokens?: number;
    cachedInputTokens?: number;
    inputTokenDetails?: {
      noCacheTokens?: number;
      cacheReadTokens?: number;
      cacheWriteTokens?: number;
    };
  };
  providerMetadata?: ProviderMetadata;
  reasoning?: Array<{
    type: "reasoning";
    text: string;
    providerOptions?: ProviderMetadata;
    providerMetadata?: ProviderMetadata;
  }>;
  reasoningText?: string;
}

/**
 * Project an AI SDK `generateText` result down to the serializable subset that
 * survives `step.run()` and feeds `AgentResult.raw`. Carries token usage,
 * Anthropic cache tokens, and reasoning so downstream consumers (billing,
 * reasoning UIs) don't lose them.
 */
export function toSerializableResult(result: AiResultLike): SerializableResult {
  const out: SerializableResult = {
    text: result.text,
    toolCalls: result.toolCalls.map((tc) => ({
      toolCallId: tc.toolCallId,
      toolName: tc.toolName,
      args: tc.input as Record<string, unknown>,
    })),
    finishReason: result.finishReason,
  };

  const usage = extractUsage(result);
  if (usage) out.usage = usage;

  if (result.reasoningText && result.reasoningText.trim() !== "") {
    out.reasoning = result.reasoningText;
  }
  const details = reasoningPartsToDetails(result.reasoning);
  if (details.length > 0) {
    out.reasoningDetails = details;
  }

  return out;
}

function reasoningPartsToDetails(
  parts: AiResultLike["reasoning"]
): ReasoningDetail[] {
  if (!parts || parts.length === 0) return [];
  return parts.map((p): ReasoningDetail => {
    const meta = (p.providerOptions ?? p.providerMetadata)?.anthropic as
      | Record<string, unknown>
      | undefined;
    const signature = meta?.signature;
    return typeof signature === "string" && signature !== ""
      ? { type: "text", text: p.text, signature }
      : { type: "text", text: p.text };
  });
}

function extractUsage(result: AiResultLike): SerializableUsage | undefined {
  const u = result.usage;
  const details = u?.inputTokenDetails;
  const anthropic = result.providerMetadata?.anthropic as
    | Record<string, unknown>
    | undefined;

  const cacheRead = firstNumber(
    details?.cacheReadTokens,
    u?.cachedInputTokens,
    anthropic?.cacheReadInputTokens
  );
  const cacheWrite = firstNumber(
    details?.cacheWriteTokens,
    anthropic?.cacheCreationInputTokens
  );

  // `input_tokens` is the NON-cache prompt count. Prefer the standardised
  // breakdown; otherwise back it out of the (cache-inclusive) total.
  let inputNonCache = numberOrUndefined(details?.noCacheTokens);
  if (inputNonCache === undefined && u?.inputTokens !== undefined) {
    inputNonCache = u.inputTokens - (cacheRead ?? 0) - (cacheWrite ?? 0);
  }

  if (!u && cacheRead === undefined && cacheWrite === undefined) {
    return undefined;
  }

  const usage: SerializableUsage = {
    input_tokens: inputNonCache ?? 0,
    output_tokens: numberOrZero(u?.outputTokens),
    total_tokens: numberOrZero(u?.totalTokens),
  };
  if (cacheWrite !== undefined) {
    usage.cache_creation_input_tokens = cacheWrite;
  }
  if (cacheRead !== undefined) {
    usage.cache_read_input_tokens = cacheRead;
  }
  return usage;
}

function numberOrZero(v: unknown): number {
  return typeof v === "number" && Number.isFinite(v) ? v : 0;
}

function numberOrUndefined(v: unknown): number | undefined {
  return typeof v === "number" && Number.isFinite(v) ? v : undefined;
}

function firstNumber(...values: unknown[]): number | undefined {
  for (const v of values) {
    const n = numberOrUndefined(v);
    if (n !== undefined) return n;
  }
  return undefined;
}

/**
 * Convert AI SDK generateText result to internal Message[].
 */
export function resultToMessages(result: SerializableResult): Message[] {
  const messages: Message[] = [];

  const hasToolCalls = result.toolCalls && result.toolCalls.length > 0;

  // Reasoning comes first: Anthropic requires the thinking block to precede the
  // tool-use block within the assistant turn.
  const reasoning = reasoningResultToMessage(result, hasToolCalls);
  if (reasoning) {
    messages.push(reasoning);
  }

  // Add text message if present
  if (result.text && result.text.trim() !== "") {
    const msg: TextMessage = {
      type: "text",
      role: "assistant",
      content: result.text,
      stop_reason: hasToolCalls ? "tool" : "stop",
    };
    messages.push(msg);
  }

  // Add tool call message if present
  if (hasToolCalls) {
    const msg: ToolCallMessage = {
      type: "tool_call",
      role: "assistant",
      stop_reason: "tool",
      tools: result.toolCalls.map(
        (tc): ToolMessage => ({
          type: "tool",
          id: tc.toolCallId,
          name: tc.toolName,
          input: tc.args as Record<string, unknown>,
        })
      ),
    };
    messages.push(msg);
  }

  // If no text and no tool calls, add empty text message
  if (messages.length === 0) {
    const msg: TextMessage = {
      type: "text",
      role: "assistant",
      content: "",
      stop_reason: "stop",
    };
    messages.push(msg);
  }

  return messages;
}

function reasoningResultToMessage(
  result: SerializableResult,
  hasToolCalls: boolean
): ReasoningMessage | undefined {
  const details = result.reasoningDetails ?? [];
  const text =
    result.reasoning ??
    details
      .filter(
        (d): d is { type: "text"; text: string; signature?: string } =>
          d.type === "text"
      )
      .map((d) => d.text)
      .join("");

  if ((!text || text.trim() === "") && details.length === 0) {
    return undefined;
  }

  const signature = details.find(
    (d): d is { type: "text"; text: string; signature: string } =>
      d.type === "text" && typeof d.signature === "string" && d.signature !== ""
  )?.signature;

  const msg: ReasoningMessage = {
    type: "reasoning",
    role: "assistant",
    content: text || "",
    stop_reason: hasToolCalls ? "tool" : "stop",
  };
  if (details.length > 0) {
    msg.details = details;
  }
  if (signature) {
    msg.signature = signature;
  }
  return msg;
}

/**
 * Convert internal Tool.Any[] to AI SDK tool definitions.
 *
 * Note: We do NOT pass `execute` here — tool execution is handled by the
 * agent's own invokeTools method after inference.
 */
export function toolsToAiTools(
  tools: AgentKitTool.Any[],
  opts: ConvertOptions = {}
): Record<string, Tool> {
  const result: Record<string, Tool> = {};

  for (const tool of tools) {
    let inputSchema: Tool["inputSchema"];
    if (tool.parameters) {
      try {
        inputSchema = jsonSchema(
          z.toJSONSchema(tool.parameters, { target: "draft-7" }) as Parameters<
            typeof jsonSchema
          >[0]
        );
      } catch {
        // Fallback for schemas that z.toJSONSchema() cannot handle (e.g. Zod v3
        // schemas from MCP's JSON-Schema-to-Zod converter). Use an open object
        // schema so the tool is still callable.
        inputSchema = jsonSchema({ type: "object", properties: {} });
      }
    } else {
      inputSchema = jsonSchema({ type: "object", properties: {} });
    }

    result[tool.name] = {
      description: tool.description,
      inputSchema,
    };
  }

  if (opts.cacheControl) {
    // Mark the last tool as a cache breakpoint so the (static) tool-definition
    // block caches as a prefix (a separate breakpoint from the system message).
    // AI SDK v6 tools carry `providerOptions` through to the provider.
    const names = Object.keys(result);
    const last = names[names.length - 1];
    if (last) {
      result[last]!.providerOptions = ANTHROPIC_CACHE_CONTROL;
    }
  }

  return result;
}

/**
 * Map internal Tool.Choice to AI SDK toolChoice format.
 */
export function mapToolChoice(
  choice: AgentKitTool.Choice
): "auto" | "required" | { type: "tool"; toolName: string } {
  switch (choice) {
    case "auto":
      return "auto";
    case "any":
      return "required";
    default:
      return { type: "tool", toolName: choice };
  }
}
