/* eslint-disable @typescript-eslint/require-await */
import type {
  LanguageModelV1,
  LanguageModelV1CallOptions,
  LanguageModelV1ProviderMetadata,
} from "@ai-sdk/provider";

type MockReasoning =
  | string
  | Array<
      | { type: "text"; text: string; signature?: string }
      | { type: "redacted"; data: string }
    >;

export interface MockModelOptions {
  text?: string;
  toolCalls?: Array<{
    toolCallId: string;
    toolName: string;
    args: unknown;
  }>;
  error?: Error;
  /** Provider id (defaults to "mock"). Use e.g. "anthropic.messages" to exercise
   *  Anthropic-only behaviour such as auto cache control. */
  provider?: string;
  /** Token usage returned by the model (defaults to all-zero). */
  usage?: { promptTokens: number; completionTokens: number };
  /** Provider-specific metadata, e.g. `{ anthropic: { cacheReadInputTokens } }`. */
  providerMetadata?: LanguageModelV1ProviderMetadata;
  /** Reasoning the model "emitted" (string or structured blocks). */
  reasoning?: MockReasoning;
  /** Invoked with the raw call options on every doGenerate (test inspection). */
  onGenerate?: (options: LanguageModelV1CallOptions) => void;
  /** If provided, called with the prompt to decide the response dynamically. */
  handler?: (prompt: unknown) => {
    text?: string;
    toolCalls?: Array<{
      toolCallType: "function";
      toolCallId: string;
      toolName: string;
      args: string;
    }>;
    finishReason: "stop" | "tool-calls";
  };
}

/**
 * Create a mock LanguageModelV1 for testing.
 * By default returns a text response. Can return tool calls or throw errors.
 */
export function createMockModel(opts?: MockModelOptions): LanguageModelV1 {
  const usage = opts?.usage ?? { promptTokens: 0, completionTokens: 0 };
  return {
    specificationVersion: "v1",
    provider: opts?.provider ?? "mock",
    modelId: "mock-model",
    defaultObjectGenerationMode: "json",
    doGenerate: async (options) => {
      opts?.onGenerate?.(options);

      if (opts?.error) {
        throw opts.error;
      }

      if (opts?.handler) {
        const result = opts.handler(options.prompt);
        return {
          text: result.text ?? "",
          toolCalls: result.toolCalls ?? [],
          finishReason: result.finishReason,
          usage,
          rawCall: { rawPrompt: null, rawSettings: {} },
        };
      }

      const toolCalls = (opts?.toolCalls ?? []).map((tc) => ({
        toolCallType: "function" as const,
        toolCallId: tc.toolCallId,
        toolName: tc.toolName,
        args: JSON.stringify(tc.args),
      }));

      return {
        text: opts?.text ?? (toolCalls.length === 0 ? "Mock response" : ""),
        toolCalls,
        finishReason:
          toolCalls.length > 0 ? ("tool-calls" as const) : ("stop" as const),
        usage,
        reasoning: opts?.reasoning,
        providerMetadata: opts?.providerMetadata,
        rawCall: { rawPrompt: null, rawSettings: {} },
      };
    },
    doStream: async () => {
      throw new Error("Not implemented");
    },
  };
}
