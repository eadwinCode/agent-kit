/* eslint-disable @typescript-eslint/require-await */
import { MockLanguageModelV3 } from "ai/test";
import type {
  LanguageModelV3,
  LanguageModelV3CallOptions,
  LanguageModelV3Content,
  SharedV3ProviderMetadata,
} from "@ai-sdk/provider";

/** Simplified usage knobs; `inputTokens` is the NON-cache prompt count. */
export interface MockUsage {
  inputTokens?: number;
  outputTokens?: number;
  cacheReadTokens?: number;
  cacheWriteTokens?: number;
}

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
  usage?: MockUsage;
  /** Provider-specific metadata, e.g. `{ anthropic: { ... } }`. */
  providerMetadata?: SharedV3ProviderMetadata;
  /** Reasoning the model "emitted" — text, or blocks with optional signatures. */
  reasoning?: string | Array<{ text: string; signature?: string }>;
  /** Invoked with the raw call options on every doGenerate (test inspection). */
  onGenerate?: (options: LanguageModelV3CallOptions) => void;
}

/**
 * Create a mock LanguageModelV3 (AI SDK v6) for testing.
 * By default returns a text response. Can return tool calls, reasoning, usage,
 * provider metadata, or throw errors.
 */
export function createMockModel(opts?: MockModelOptions): LanguageModelV3 {
  const inputNonCache = opts?.usage?.inputTokens ?? 0;
  const cacheRead = opts?.usage?.cacheReadTokens;
  const cacheWrite = opts?.usage?.cacheWriteTokens;
  const inputTotal = inputNonCache + (cacheRead ?? 0) + (cacheWrite ?? 0);
  const outputTokens = opts?.usage?.outputTokens ?? 0;

  return new MockLanguageModelV3({
    provider: opts?.provider ?? "mock",
    modelId: "mock-model",
    doGenerate: async (options) => {
      opts?.onGenerate?.(options);

      if (opts?.error) {
        throw opts.error;
      }

      const toolCalls = opts?.toolCalls ?? [];
      const content: LanguageModelV3Content[] = [];

      // Reasoning first (mirrors a real thinking-then-answer turn).
      if (opts?.reasoning) {
        const blocks =
          typeof opts.reasoning === "string"
            ? [{ text: opts.reasoning }]
            : opts.reasoning;
        for (const b of blocks) {
          content.push({
            type: "reasoning",
            text: b.text,
            ...(b.signature
              ? { providerMetadata: { anthropic: { signature: b.signature } } }
              : {}),
          });
        }
      }

      const text =
        opts?.text ?? (toolCalls.length === 0 ? "Mock response" : "");
      if (text) {
        content.push({ type: "text", text });
      }

      for (const tc of toolCalls) {
        content.push({
          type: "tool-call",
          toolCallId: tc.toolCallId,
          toolName: tc.toolName,
          input: JSON.stringify(tc.args),
        });
      }

      const unified = toolCalls.length > 0 ? "tool-calls" : "stop";
      return {
        content,
        finishReason: { unified, raw: unified } as const,
        usage: {
          inputTokens: {
            total: inputTotal,
            noCache: inputNonCache,
            cacheRead,
            cacheWrite,
          },
          outputTokens: {
            total: outputTokens,
            text: outputTokens,
            reasoning: undefined,
          },
        },
        ...(opts?.providerMetadata
          ? { providerMetadata: opts.providerMetadata }
          : {}),
        warnings: [],
      };
    },
  });
}
