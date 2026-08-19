import { type GetStepTools, type Inngest } from "inngest";
import { type output as ZodOutput } from "zod";
import { type Agent } from "./agent";
import { type StateData } from "./state";
import { type NetworkRun } from "./network";
import { type AnyZodType, type MaybePromise } from "./util";
import type { StreamableHTTPReconnectionOptions } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import type { OAuthClientProvider } from "@modelcontextprotocol/sdk/client/auth.js";

/**
 * ToolResultPayload mirrors the UI package shape for structured tool outputs.
 */
export type ToolResultPayload<T> = { data: T };

/**
 * createTool is a helper that properly types the input argument for a handler
 * based off of the Zod parameter types, and captures the handler output type.
 */
export function createTool<
  TName extends string,
  TInput extends Tool.Input,
  TOutput,
  TState extends StateData,
>({
  name,
  description,
  parameters,
  manualStep,
  parallelSafe,
  handler,
}: {
  name: TName;
  description?: string;
  parameters?: TInput;
  /**
   * Opt this tool OUT of AgentKit's automatic durable-step wrapping. See
   * {@link Tool.manualStep}. Defaults to `false` (the handler is wrapped in a
   * durable `step.run` when running inside Inngest).
   */
  manualStep?: boolean;
  /**
   * Mark this tool as safe to run CONCURRENTLY with other tools from the same
   * inference. See {@link Tool.parallelSafe}. Defaults to `false` (the tool is
   * executed serially, in model-call order).
   */
  parallelSafe?: boolean;
  handler: (
    input: ZodOutput<TInput>,
    opts: Tool.Options<TState>
  ) => MaybePromise<TOutput>;
}): Tool<TName, TInput, TOutput> {
  return {
    name,
    description,
    parameters,
    manualStep,
    parallelSafe,
    handler<TS extends StateData>(
      input: ZodOutput<TInput>,
      opts: Tool.Options<TS>
    ): MaybePromise<TOutput> {
      return handler(input, opts as unknown as Tool.Options<TState>);
    },
  };
}

export type Tool<TName extends string, TInput extends Tool.Input, TOutput> = {
  name: TName;
  description?: string;
  parameters?: TInput;

  // mcp lists the MCP details for this tool, if this tool is provided by an
  // MCP server.
  mcp?: {
    server: MCP.Server;
    tool: MCP.Tool;
  };

  strict?: boolean;

  /**
   * Opt out of AgentKit's automatic durable-step wrapping.
   *
   * By default, when a tool is invoked inside an Inngest run AgentKit wraps the
   * handler in a single `step.run(...)` under a deterministic id, so the tool's
   * side effect executes EXACTLY ONCE across Inngest's replays (Inngest
   * re-runs the function body on every step boundary; an unwrapped handler would
   * re-fire — re-applying an `edit_file`, re-billing an image generation, etc.).
   *
   * Set `manualStep: true` when the handler must NOT be wrapped, namely:
   *   - it drives step tooling itself — `step.waitForEvent` (human-in-the-loop),
   *     `step.invoke` (sub-functions), or its own `step.run` checkpoints (a long
   *     multi-call subagent) — since wrapping it would nest steps, which Inngest
   *     forbids; or
   *   - it is an idempotent large-output read (e.g. a screenshot) whose result
   *     you don't want occupying step state (all step outputs share a per-run
   *     ~4MB budget) — re-running a read on replay is only wasted latency, not a
   *     correctness or billing bug.
   *
   * A `manualStep` handler runs inline and receives the live `opts.step`, so it
   * owns its own durability. A wrapped handler instead receives `opts.step:
   * undefined` (it is already inside a step). Tools whose primary effect is
   * mutating `network.state.data` do NOT need this flag — AgentKit re-applies
   * their state delta across replays automatically.
   */
  manualStep?: boolean;

  /**
   * Mark this tool as safe to run CONCURRENTLY with other `parallelSafe` tools
   * from the same model inference.
   *
   * When a single inference emits multiple tool calls, AgentKit groups each
   * maximal run of consecutive `parallelSafe` calls into a batch and executes
   * the batch concurrently; every other call still executes serially, in
   * model-call order. Results are ALWAYS fed back to the model in the original
   * call order (never completion order), and durable step ids are pre-assigned
   * in call order, so batching changes ONLY wall-clock latency — never the
   * observable result sequence or Inngest replay behavior.
   *
   * Only set this for tools whose executions are mutually independent:
   *   - no reads or writes of `network.state` (a parallel batch shares the
   *     state object; concurrent mutation is a data race);
   *   - no dependence on a sibling call's output or side effect;
   *   - side effects (if any) touch disjoint resources (e.g. pure reads such
   *     as `read_file`/`grep`/`glob`, or writes to distinct, non-overlapping
   *     targets).
   *
   * Mutating tools (`edit_file`, `cp/mv/rm`, billing, publication) and any
   * tool that mutates `network.state` must keep the default `false`.
   */
  parallelSafe?: boolean;

  handler<TState extends StateData>(
    input: ZodOutput<TInput>,
    opts: Tool.Options<TState>
  ): MaybePromise<TOutput>;
};

export namespace Tool {
  export type Any = Tool<string, Tool.Input, unknown>;

  export type Options<T extends StateData> = {
    agent: Agent<T>;
    network: NetworkRun<T>;
    step?: GetStepTools<Inngest.Any>;
  };

  export type Input = AnyZodType;

  export type Choice = "auto" | "any" | (string & {});
}

/**
 * Helper to create a strongly-typed tool manifest from a list of tools.
 *
 * Returns a simple runtime object keyed by tool name. The primary value is the
 * compile-time type that captures each tool's input and output types.
 */
export function createToolManifest<
  TTools extends readonly Tool<string, Tool.Input, unknown>[],
>(tools: TTools) {
  const manifest: Record<string, { input: unknown; output: unknown }> = {};
  for (const t of tools) {
    // runtime structure is intentionally minimal; types carry the value
    manifest[t.name] = { input: {}, output: {} };
  }
  type Result = {
    [K in TTools[number] as K["name"] & string]: K extends Tool<
      string,
      infer In extends AnyZodType,
      infer Out
    >
      ? { input: ZodOutput<In>; output: ToolResultPayload<Out> }
      : never;
  };
  return manifest as Result;
}

export namespace MCP {
  export type Server = {
    // name is a short name for the MCP server, eg. "github".  This allows
    // us to namespace tools for each MCP server.
    name: string;
    transport:
      | TransportSSE
      | TransportWebsocket
      | TransportStreamableHttp
      | TransportStdio;
  };

  export type Transport =
    | TransportSSE
    | TransportWebsocket
    | TransportStreamableHttp
    | TransportStdio;

  export type TransportStreamableHttp = {
    type: "streamable-http";
    url: string;
    requestInit?: RequestInit;
    reconnectionOptions?: StreamableHTTPReconnectionOptions;
    sessionId?: string;
    authProvider?: OAuthClientProvider;
  };

  export type TransportStdio = {
    type: "stdio";
    command: string;
    args: string[];
    env?: Record<string, string>;
  };

  export type TransportSSE = {
    type: "sse";
    url: string;
    eventSourceInit?: EventSourceInit;
    requestInit?: RequestInit;
  };

  export type TransportWebsocket = {
    type: "ws";
    url: string;
  };

  export type Tool = {
    name: string;
    description?: string;
    inputSchema?: {
      type: "object";
      properties?: unknown;
    };
  };
}
