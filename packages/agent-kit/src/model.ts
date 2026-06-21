import { generateText, type LanguageModelV1 } from "ai";
import {
  messagesToCoreMessages,
  resultToMessages,
  toolsToAiTools,
  mapToolChoice,
  toSerializableResult,
  type SerializableResult,
} from "./converters";
import { type Message } from "./types";
import { type Tool } from "./tool";
import { getStepTools } from "./util";

export interface AgenticModelOptions {
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

export const createAgenticModelFromLanguageModel = (
  model: LanguageModelV1,
  options: AgenticModelOptions = {}
): AgenticModel => {
  return new AgenticModel(model, options);
};

export class AgenticModel {
  #model: LanguageModelV1;
  #cacheControl: boolean;

  constructor(model: LanguageModelV1, options: AgenticModelOptions = {}) {
    this.#model = model;
    this.#cacheControl = resolveCacheControl(model, options.cacheControl);
  }

  async infer(
    stepID: string,
    input: Message[],
    tools: Tool.Any[],
    tool_choice: Tool.Choice
  ): Promise<AgenticModel.InferenceResponse> {
    const convertOpts = { cacheControl: this.#cacheControl };
    const messages = messagesToCoreMessages(input, convertOpts);
    const aiTools =
      tools.length > 0 ? toolsToAiTools(tools, convertOpts) : undefined;

    const doInference = async (): Promise<SerializableResult> => {
      const result = await generateText({
        model: this.#model,
        messages,
        tools: aiTools,
        toolChoice: aiTools ? mapToolChoice(tool_choice) : undefined,
      });
      // Return only serializable fields for step.run() compatibility. This
      // carries usage, Anthropic cache tokens, and reasoning so the consumer
      // can bill and render them (see toSerializableResult).
      return toSerializableResult(result);
    };

    const step = await getStepTools();
    const result: SerializableResult = step
      ? await step.run(stepID, doInference)
      : await doInference();

    return { output: resultToMessages(result), raw: result };
  }
}

/**
 * Resolve whether to apply Anthropic prompt caching. `"auto"` (the default)
 * enables it only for Anthropic models so other providers never receive the
 * provider-specific markers.
 */
function resolveCacheControl(
  model: LanguageModelV1,
  setting?: boolean | "auto"
): boolean {
  if (setting === true || setting === false) {
    return setting;
  }
  return isAnthropicModel(model);
}

function isAnthropicModel(model: LanguageModelV1): boolean {
  return (model.provider ?? "").toLowerCase().includes("anthropic");
}

export namespace AgenticModel {
  export type Any = AgenticModel;

  /**
   * InferenceResponse is the response from a model for an inference request.
   * This contains parsed messages and the raw result, with the type of the raw
   * result depending on the model's API response.
   */
  export type InferenceResponse<T = unknown> = {
    output: Message[];
    raw: T;
  };
}
