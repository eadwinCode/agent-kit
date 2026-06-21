import { M as Message, T as Tool, R as ReasoningDetail } from './agent-Cb1-KuXd.cjs';
export { A as Agent, G as AgentMessageChunk, J as AgentMessageChunkSchema, y as AgentResult, n as AnyZodType, a2 as DataDeltaEvent, a8 as GenericErrorEvent, z as History, H as HistoryConfig, a3 as HitlRequestedEvent, a4 as HitlResolvedEvent, I as ImageContent, l as MCP, m as MaybePromise, a6 as MetadataUpdatedEvent, N as Network, e as NetworkRun, Y as PartCompletedEvent, X as PartCreatedEvent, Z as PartFailedEvent, a1 as ReasoningDeltaEvent, t as ReasoningMessage, b as RoutingAgent, L as RunCompletedEvent, O as RunFailedEvent, P as RunInterruptedEvent, K as RunStartedEvent, C as SaveThreadToStorageConfig, h as State, S as StateData, V as StepCompletedEvent, W as StepFailedEvent, Q as StepStartedEvent, a7 as StreamEndedEvent, a9 as StreamingConfig, aa as StreamingContext, ab as StreamingEvent, w as TextContent, _ as TextDeltaEvent, r as TextMessage, B as ThreadOperationConfig, $ as ToolCallArgumentsDeltaEvent, u as ToolCallMessage, a0 as ToolCallOutputDeltaEvent, x as ToolMessage, v as ToolResultMessage, i as ToolResultPayload, a5 as UsageUpdatedEvent, U as UserMessage, c as createAgent, d as createNetwork, a as createRoutingAgent, f as createState, ae as createStepWrapper, j as createTool, k as createToolManifest, ad as generateId, g as getDefaultRoutingAgent, q as getInngestFnInput, o as getStepTools, D as initializeThread, ac as isEventType, p as isInngestFn, E as loadThreadFromStorage, F as saveThreadToStorage, s as stringifyError } from './agent-Cb1-KuXd.cjs';
import { LanguageModel, ModelMessage, ProviderMetadata, Tool as Tool$1 } from 'ai';
import 'inngest';
import 'zod';
import 'inngest/experimental';
import '@modelcontextprotocol/sdk/client/streamableHttp.js';
import '@modelcontextprotocol/sdk/client/auth.js';

interface AgenticModelOptions {
    /**
     * Controls Anthropic prompt caching (ephemeral `cacheControl` breakpoints on
     * the system message and the last tool).
     *
     * - `undefined` / `"auto"` (default): enabled only for Anthropic models,
     *   detected from the model's `provider` id. Other providers are untouched.
     * - `true` / `false`: force caching on/off regardless of provider.
     */
    cacheControl?: boolean | "auto";
}
declare const createAgenticModelFromLanguageModel: (model: LanguageModel, options?: AgenticModelOptions) => AgenticModel;
declare class AgenticModel {
    #private;
    constructor(model: LanguageModel, options?: AgenticModelOptions);
    infer(stepID: string, input: Message[], tools: Tool.Any[], tool_choice: Tool.Choice): Promise<AgenticModel.InferenceResponse>;
}
declare namespace AgenticModel {
    type Any = AgenticModel;
    /**
     * InferenceResponse is the response from a model for an inference request.
     * This contains parsed messages and the raw result, with the type of the raw
     * result depending on the model's API response.
     */
    type InferenceResponse<T = unknown> = {
        output: Message[];
        raw: T;
    };
}

/**
 * Converters between internal Message/Tool types and the Vercel AI SDK types.
 *
 * Targets the AI SDK v6 model interface: `ModelMessage` / `Tool` / `ToolResultOutput`.
 *
 * @module
 */

/**
 * Options shared by the message/tool converters.
 */
interface ConvertOptions {
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
declare function messagesToCoreMessages(messages: Message[], opts?: ConvertOptions): ModelMessage[];
/**
 * Serializable subset of generateText result for step.run() compatibility.
 */
interface SerializableResult {
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
interface SerializableUsage {
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
interface AiResultLike {
    text: string;
    toolCalls: Array<{
        toolCallId: string;
        toolName: string;
        input: unknown;
    }>;
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
declare function toSerializableResult(result: AiResultLike): SerializableResult;
/**
 * Convert AI SDK generateText result to internal Message[].
 */
declare function resultToMessages(result: SerializableResult): Message[];
/**
 * Convert internal Tool.Any[] to AI SDK tool definitions.
 *
 * Note: We do NOT pass `execute` here — tool execution is handled by the
 * agent's own invokeTools method after inference.
 */
declare function toolsToAiTools(tools: Tool.Any[], opts?: ConvertOptions): Record<string, Tool$1>;
/**
 * Map internal Tool.Choice to AI SDK toolChoice format.
 */
declare function mapToolChoice(choice: Tool.Choice): "auto" | "required" | {
    type: "tool";
    toolName: string;
};

export { AgenticModel, type AgenticModelOptions, type AiResultLike, type ConvertOptions, Message, ReasoningDetail, type SerializableResult, type SerializableUsage, Tool, createAgenticModelFromLanguageModel, mapToolChoice, messagesToCoreMessages, resultToMessages, toSerializableResult, toolsToAiTools };
