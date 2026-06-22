"use strict";
var __create = Object.create;
var __defProp = Object.defineProperty;
var __defProps = Object.defineProperties;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropDescs = Object.getOwnPropertyDescriptors;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getOwnPropSymbols = Object.getOwnPropertySymbols;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __propIsEnum = Object.prototype.propertyIsEnumerable;
var __typeError = (msg) => {
  throw TypeError(msg);
};
var __defNormalProp = (obj, key, value) => key in obj ? __defProp(obj, key, { enumerable: true, configurable: true, writable: true, value }) : obj[key] = value;
var __spreadValues = (a, b) => {
  for (var prop in b || (b = {}))
    if (__hasOwnProp.call(b, prop))
      __defNormalProp(a, prop, b[prop]);
  if (__getOwnPropSymbols)
    for (var prop of __getOwnPropSymbols(b)) {
      if (__propIsEnum.call(b, prop))
        __defNormalProp(a, prop, b[prop]);
    }
  return a;
};
var __spreadProps = (a, b) => __defProps(a, __getOwnPropDescs(b));
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);
var __accessCheck = (obj, member, msg) => member.has(obj) || __typeError("Cannot " + msg);
var __privateGet = (obj, member, getter) => (__accessCheck(obj, member, "read from private field"), getter ? getter.call(obj) : member.get(obj));
var __privateAdd = (obj, member, value) => member.has(obj) ? __typeError("Cannot add the same private member more than once") : member instanceof WeakSet ? member.add(obj) : member.set(obj, value);
var __privateSet = (obj, member, value, setter) => (__accessCheck(obj, member, "write to private field"), setter ? setter.call(obj, value) : member.set(obj, value), value);
var __privateWrapper = (obj, member, setter, getter) => ({
  set _(value) {
    __privateSet(obj, member, value, setter);
  },
  get _() {
    return __privateGet(obj, member, getter);
  }
});

// src/index.ts
var index_exports = {};
__export(index_exports, {
  Agent: () => Agent,
  AgentMessageChunkSchema: () => AgentMessageChunkSchema,
  AgentResult: () => AgentResult,
  AgenticModel: () => AgenticModel,
  DEFAULT_CHUNK_SIZE: () => DEFAULT_CHUNK_SIZE,
  DEFAULT_MAX_CHUNKS_PER_MESSAGE: () => DEFAULT_MAX_CHUNKS_PER_MESSAGE,
  DEFAULT_MAX_ITER_PER_RUN: () => DEFAULT_MAX_ITER_PER_RUN,
  FINAL_APPEND_STEP_ID: () => FINAL_APPEND_STEP_ID,
  Network: () => Network,
  NetworkRun: () => NetworkRun,
  RoutingAgent: () => RoutingAgent,
  State: () => State,
  StreamingContext: () => StreamingContext,
  createAgent: () => createAgent,
  createAgenticModelFromLanguageModel: () => createAgenticModelFromLanguageModel,
  createNetwork: () => createNetwork,
  createRoutingAgent: () => createRoutingAgent,
  createState: () => createState,
  createStepWrapper: () => createStepWrapper,
  createTool: () => createTool,
  createToolManifest: () => createToolManifest,
  generateId: () => generateId,
  getDefaultRoutingAgent: () => getDefaultRoutingAgent,
  getInngestFnInput: () => getInngestFnInput,
  getStepTools: () => getStepTools,
  incrementalAppendStepId: () => incrementalAppendStepId,
  initializeThread: () => initializeThread,
  isEventType: () => isEventType,
  isInngestFn: () => isInngestFn,
  loadThreadFromStorage: () => loadThreadFromStorage,
  mapToolChoice: () => mapToolChoice,
  messagesToCoreMessages: () => messagesToCoreMessages,
  persistResults: () => persistResults,
  resultToMessages: () => resultToMessages,
  saveThreadToStorage: () => saveThreadToStorage,
  stringifyError: () => stringifyError,
  toSerializableResult: () => toSerializableResult,
  toolsToAiTools: () => toolsToAiTools
});
module.exports = __toCommonJS(index_exports);

// src/agent.ts
var import_ai4 = require("ai");
var import_client = require("@modelcontextprotocol/sdk/client/index.js");
var import_streamableHttp = require("@modelcontextprotocol/sdk/client/streamableHttp.js");
var import_sse = require("@modelcontextprotocol/sdk/client/sse.js");
var import_websocket = require("@modelcontextprotocol/sdk/client/websocket.js");
var import_stdio = require("@modelcontextprotocol/sdk/client/stdio.js");
var import_transport = require("@modelcontextprotocol/sdk/shared/transport.js");
var import_types5 = require("@modelcontextprotocol/sdk/types.js");
var import_eventsource = require("eventsource");
var import_crypto2 = require("crypto");
var import_inngest6 = require("inngest");
var import_internals = require("inngest/internals");
var import_inngest7 = require("inngest");
var import_types6 = require("inngest/types");

// src/model.ts
var import_ai3 = require("ai");

// src/converters.ts
var import_ai2 = require("ai");
var import_zod5 = require("zod");

// src/types.ts
var import_xxhashjs = __toESM(require("xxhashjs"), 1);
var _checksum;
var AgentResult = class {
  constructor(agentName, output, toolCalls, createdAt, prompt, history, raw, id) {
    this.agentName = agentName;
    this.output = output;
    this.toolCalls = toolCalls;
    this.createdAt = createdAt;
    this.prompt = prompt;
    this.history = history;
    this.raw = raw;
    this.id = id;
    // checksum memoizes a checksum so that it doe snot have to be calculated many times.
    __privateAdd(this, _checksum);
  }
  /**
   * export returns all fields necessary to store the AgentResult for future use.
   */
  export() {
    return {
      agentName: this.agentName,
      output: this.output,
      toolCalls: this.toolCalls,
      createdAt: this.createdAt,
      checksum: this.checksum
    };
  }
  /**
   * checksum is a unique ID for this result.
   *
   * It is generated by taking a checksum of the message output and the created at date.
   * This allows you to dedupe items when saving conversation history.
   */
  get checksum() {
    if (__privateGet(this, _checksum) === void 0) {
      const input = JSON.stringify(this.output.concat(this.toolCalls)) + this.createdAt.toString();
      __privateSet(this, _checksum, import_xxhashjs.default.h64(input, 0).toString());
    }
    return __privateGet(this, _checksum);
  }
};
_checksum = new WeakMap();

// src/tool.ts
var import_inngest5 = require("inngest");
var import_zod4 = require("zod");

// src/state.ts
var createState = (initialState, opts) => {
  return new State(__spreadProps(__spreadValues({}, opts), { data: initialState }));
};
var _durableToolCallIndex, __kv;
var _State = class _State {
  constructor({
    data,
    messages,
    threadId,
    results
  } = {}) {
    /**
     * Monotonic counter for AgentKit's durable tool-step ids.
     *
     * It is intentionally NOT copied by {@link clone}, so every `network.run`
     * (which clones the template state once per Inngest execution) starts again at
     * 0. Because memoized inferences replay the same tool calls in the same order,
     * the Nth wrapped tool call in a run always receives the same index across
     * replays — giving each tool a deterministic, replay-stable `step.run` id
     * without resorting to a checksum/timestamp/uuid.
     */
    __privateAdd(this, _durableToolCallIndex, 0);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    __privateAdd(this, __kv);
    this._results = results || [];
    this._messages = messages || [];
    this._data = data ? __spreadValues({}, data) : {};
    this.threadId = threadId;
    this.data = new Proxy(this._data, {
      set: (target, prop, value) => {
        if (typeof prop === "string" && prop in target) {
          Reflect.set(target, prop, value);
          return true;
        }
        return Reflect.set(target, prop, value);
      }
    });
    __privateSet(this, __kv, new Map(Object.entries(this._data)));
    this.kv = {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      set: (key, value) => {
        __privateGet(this, __kv).set(key, value);
      },
      get: (key) => {
        return __privateGet(this, __kv).get(key);
      },
      delete: (key) => {
        return __privateGet(this, __kv).delete(key);
      },
      has: (key) => {
        return __privateGet(this, __kv).has(key);
      },
      all: () => {
        return Object.fromEntries(__privateGet(this, __kv));
      }
    };
  }
  /**
   * Results returns a new array containing all past inference results in the
   * network. This array is safe to modify.
   */
  get results() {
    return this._results.slice();
  }
  /**
   * Replaces all results with the provided array
   * used when loading initial results from history.get()
   */
  setResults(results) {
    this._results = results;
  }
  /**
   * Returns a slice of results from the given start index
   * used when saving results to a database via history.appendResults()
   */
  getResultsFrom(startIndex) {
    return this._results.slice(startIndex);
  }
  /**
   * Messages returns a new array containing all initial messages that were
   * provided to the constructor. This array is safe to modify.
   */
  get messages() {
    return this._messages.slice();
  }
  /**
   * formatHistory returns the memory used for agentic calls based off of prior
   * agentic calls.
   *
   * This is used to format the current State as a conversation log when
   * calling an individual agent.
   *
   */
  formatHistory(formatter) {
    if (!formatter) {
      formatter = defaultResultFormatter;
    }
    return this._messages.concat(
      this._results.map((result) => formatter(result)).flat()
    );
  }
  /**
   * appendResult appends a given result to the current state.  This
   * is called by the network after each iteration.
   */
  appendResult(call) {
    this._results.push(call);
  }
  /**
   * Returns the next durable tool-call index for this run (see
   * {@link #durableToolCallIndex}). Used only by the network to build
   * replay-stable tool-step ids; not part of the public data model.
   */
  nextDurableToolCallIndex() {
    return __privateWrapper(this, _durableToolCallIndex)._++;
  }
  /**
   * Re-applies a typed-data snapshot captured INSIDE a durable tool step.
   *
   * A tool that mutates `state.data` does so inside its memoized `step.run`; on
   * replay that body is skipped, so the live mutation is absent. The network
   * memoizes the post-handler data snapshot and calls this OUTSIDE the step on
   * every execution to restore it. Mutates the existing `data` object in place
   * (full replace: keys absent from `data` are removed) so references stay valid
   * and the proxy keeps intercepting writes.
   */
  importData(data) {
    const target = this.data;
    const next = data != null ? data : {};
    for (const key of Object.keys(target)) {
      if (!(key in next)) {
        delete target[key];
      }
    }
    Object.assign(target, next);
  }
  /**
   * clone allows you to safely clone the state.
   */
  clone() {
    const state = new _State({
      data: this.data,
      threadId: this.threadId,
      messages: this._messages.slice(),
      results: this._results.slice()
    });
    return state;
  }
};
_durableToolCallIndex = new WeakMap();
__kv = new WeakMap();
var State = _State;
var defaultResultFormatter = (r) => {
  return [].concat(r.output).concat(r.toolCalls);
};

// src/network.ts
var import_ai = require("ai");
var import_crypto = require("crypto");
var import_zod3 = require("zod");

// src/util.ts
var import_inngest = require("inngest");
var import_experimental = require("inngest/experimental");
var import_zod = require("zod");
var stringifyError = (e) => {
  if (e instanceof Error) {
    return e.message;
  }
  return String(e);
};
var getStepTools = async () => {
  var _a;
  const asyncCtx = await (0, import_experimental.getAsyncCtx)();
  const ctx = (asyncCtx == null ? void 0 : asyncCtx.ctx) || ((_a = asyncCtx == null ? void 0 : asyncCtx.execution) == null ? void 0 : _a.ctx);
  return ctx == null ? void 0 : ctx.step;
};
var isInngestFn = (fn) => {
  if ((0, import_inngest.isInngestFunction)(fn)) {
    return true;
  }
  if (typeof fn === "object" && fn !== null && "createExecution" in fn && typeof fn.createExecution === "function") {
    return true;
  }
  return false;
};
var getInngestFnInput = (fn) => {
  var _a, _b, _c;
  const runtimeSchemas = (_a = fn["client"]["schemas"]) == null ? void 0 : _a["runtimeSchemas"];
  if (!runtimeSchemas) {
    return;
  }
  const schemasToAttempt = new Set(
    (_c = (_b = fn["opts"].triggers) == null ? void 0 : _b.reduce((acc, trigger) => {
      if (trigger.event) {
        return [...acc, trigger.event];
      }
      return acc;
    }, [])) != null ? _c : []
  );
  if (!schemasToAttempt.size) {
    return;
  }
  let schema;
  for (const eventSchema of schemasToAttempt) {
    const runtimeSchema = runtimeSchemas[eventSchema];
    if (typeof runtimeSchema === "object" && runtimeSchema !== null && "data" in runtimeSchema && helpers.isZodObject(runtimeSchema.data)) {
      if (schema) {
        schema = schema.or(runtimeSchema.data);
      } else {
        schema = runtimeSchema.data;
      }
      continue;
    }
  }
  return schema;
};
var helpers = {
  isZodObject: (value) => {
    return value instanceof import_zod.ZodObject;
  },
  isObject: (value) => {
    return typeof value === "object" && value !== null && !Array.isArray(value);
  }
};

// src/history.ts
var import_inngest2 = require("inngest");
async function initializeThread(config) {
  const { state, history, input, network } = config;
  if (!history) return;
  const step = await getStepTools();
  if (state.threadId && history.createThread) {
    await history.createThread({
      state,
      network,
      input,
      step
    });
    return;
  }
  if (!state.threadId && history.createThread) {
    const { threadId } = await history.createThread({
      state,
      network,
      input,
      step
    });
    state.threadId = threadId;
  } else if (!state.threadId && history.get) {
    state.threadId = crypto.randomUUID();
    if (history.createThread) {
      await history.createThread({
        state,
        network,
        input,
        step
      });
    }
  }
}
async function loadThreadFromStorage(config) {
  const { state, history, input, network } = config;
  if (!(history == null ? void 0 : history.get) || !state.threadId || state.results.length > 0 || state.messages.length > 0) {
    return;
  }
  const step = await getStepTools();
  const historyResults = await history.get({
    state,
    network,
    input,
    step,
    threadId: state.threadId
  });
  state.setResults(historyResults);
}
async function saveThreadToStorage(config) {
  const { state, initialResultCount } = config;
  const newResults = state.getResultsFrom(initialResultCount);
  await persistResults(config, newResults, FINAL_APPEND_STEP_ID);
}
var FINAL_APPEND_STEP_ID = "agent-kit/history/append-results/final";
function incrementalAppendStepId(key) {
  return `agent-kit/history/append-results/${key}`;
}
async function persistResults(config, newResults, stepId) {
  const { state, history, network, input } = config;
  if (!(history == null ? void 0 : history.appendResults) || newResults.length === 0) return;
  const step = await getStepTools();
  const append = () => history.appendResults({
    state,
    network,
    // Intentionally undefined: this call is ALREADY inside a step.run, so the
    // hook must run inline (a nested step.run would break Inngest). The hook's
    // own durability/step id (if any) is therefore not used.
    step: void 0,
    newResults,
    input,
    threadId: state.threadId
  });
  await (step ? step.run(stepId, append) : append());
}

// src/streaming.ts
var import_inngest3 = require("inngest");
var import_inngest4 = require("inngest");
var import_zod2 = require("zod");
var AgentMessageChunkSchema = import_zod2.z.object({
  event: import_zod2.z.string(),
  data: import_zod2.z.record(import_zod2.z.string(), import_zod2.z.any()),
  timestamp: import_zod2.z.number(),
  sequenceNumber: import_zod2.z.number(),
  id: import_zod2.z.string()
});
var SequenceCounter = class {
  constructor() {
    this.value = 0;
  }
  getNext() {
    return this.value++;
  }
  current() {
    return this.value;
  }
};
var DEFAULT_CHUNK_SIZE = 256;
var DEFAULT_MAX_CHUNKS_PER_MESSAGE = 24;
var StreamingContext = class _StreamingContext {
  constructor(config) {
    var _a, _b, _c;
    this.publish = config.publish;
    this.runId = config.runId;
    this.parentRunId = config.parentRunId;
    this.messageId = config.messageId;
    this.threadId = config.threadId;
    this.userId = config.userId;
    this.scope = config.scope;
    this.sequenceCounter = config.sequenceCounter || new SequenceCounter();
    this.debug = (_a = config.debug) != null ? _a : process.env.NODE_ENV === "development";
    this.simulateChunking = (_b = config.simulateChunking) != null ? _b : false;
    this.chunkSize = config.chunkSize && config.chunkSize > 0 ? config.chunkSize : DEFAULT_CHUNK_SIZE;
    this.maxChunksPerMessage = (_c = config.maxChunksPerMessage) != null ? _c : DEFAULT_MAX_CHUNKS_PER_MESSAGE;
  }
  /**
   * Create a child streaming context for agent runs within network runs
   */
  createChildContext(agentRunId) {
    return new _StreamingContext({
      publish: this.publish,
      runId: agentRunId,
      parentRunId: this.runId,
      messageId: this.messageId,
      threadId: this.threadId,
      userId: this.userId,
      scope: "agent",
      sequenceCounter: this.sequenceCounter,
      // Share the same counter
      debug: this.debug,
      // Inherit debug setting
      simulateChunking: this.simulateChunking,
      chunkSize: this.chunkSize,
      maxChunksPerMessage: this.maxChunksPerMessage
    });
  }
  /**
   * Create a context with different messageId but shared sequence counter
   */
  createContextWithSharedSequence(config) {
    return new _StreamingContext({
      publish: this.publish,
      runId: config.runId,
      parentRunId: this.runId,
      messageId: config.messageId,
      threadId: this.threadId,
      userId: this.userId,
      scope: config.scope,
      sequenceCounter: this.sequenceCounter,
      // Share the same counter instance
      debug: this.debug,
      // Inherit debug setting
      simulateChunking: this.simulateChunking,
      chunkSize: this.chunkSize,
      maxChunksPerMessage: this.maxChunksPerMessage
    });
  }
  /**
   * Extract context information from network state
   */
  static fromNetworkState(networkState, config) {
    var _a, _b;
    const debug = (_a = config.debug) != null ? _a : process.env.NODE_ENV === "development";
    return new _StreamingContext({
      publish: config.publish,
      runId: config.runId,
      messageId: config.messageId,
      threadId: networkState.threadId,
      userId: typeof networkState.data.userId === "string" ? networkState.data.userId : void 0,
      scope: config.scope,
      debug,
      simulateChunking: (_b = config.simulateChunking) != null ? _b : false,
      chunkSize: config.chunkSize,
      maxChunksPerMessage: config.maxChunksPerMessage
    });
  }
  /**
   * Publish an event with automatic sequence numbering.
   * Provides a stepId in the chunk for optional Inngest step wrapping by the developer.
   */
  async publishEvent(event) {
    const sequenceNumber = this.sequenceCounter.getNext();
    const stepId = this.generateStreamingStepId(event, sequenceNumber);
    const enrichedData = __spreadValues({}, event.data);
    if (this.threadId) {
      enrichedData["threadId"] = this.threadId;
    }
    if (this.userId) {
      enrichedData["userId"] = this.userId;
    }
    const chunk = __spreadProps(__spreadValues({}, event), {
      data: enrichedData,
      timestamp: Date.now(),
      sequenceNumber,
      id: stepId
    });
    try {
      await this.publish(chunk);
    } catch (err) {
      console.warn(
        "[Streaming] Failed to publish event; continuing execution",
        {
          error: err instanceof Error ? err.message : String(err),
          event: chunk.event,
          sequenceNumber: chunk.sequenceNumber
        }
      );
    }
  }
  /**
   * Generate intelligent step IDs for streaming events
   */
  generateStreamingStepId(event, sequenceNumber) {
    return `publish-${sequenceNumber}:${event.event}`;
  }
  /**
   * Generate a unique part ID for this streaming context
   * OpenAI requires tool call IDs to be ≤ 40 characters
   */
  generatePartId() {
    const shortMessageId = this.messageId.replace(/-/g, "").substring(0, 8);
    const shortTimestamp = Date.now().toString().slice(-8);
    const randomSuffix = Math.random().toString(36).substr(2, 6);
    const partId = `tool_${shortMessageId}_${shortTimestamp}_${randomSuffix}`;
    return partId;
  }
  /**
   * Generate a unique step ID for this streaming context
   */
  generateStepId(baseName) {
    return `step_${baseName}_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }
  /** Returns whether simulated chunking is enabled for this context */
  isSimulatedChunking() {
    return this.simulateChunking;
  }
  /**
   * Split `content` into the list of `*.delta` strings to publish for one part.
   *
   * - chunking off → a single delta with the whole content (1 publish/part);
   * - chunking on  → fixed-size `chunkSize` chunks, but never more than
   *   `maxChunksPerMessage` (the chunk size grows for very long content so the
   *   publish/step count stays bounded regardless of output length).
   *
   * Empty content yields no deltas. Each returned element is one `publishEvent`
   * call by the caller, so this method governs streaming's step-budget cost.
   */
  chunkContent(content) {
    if (!content) return [];
    if (!this.simulateChunking) return [content];
    let size = Math.max(1, this.chunkSize);
    if (this.maxChunksPerMessage > 0) {
      size = Math.max(
        size,
        Math.ceil(content.length / this.maxChunksPerMessage)
      );
    }
    const chunks = [];
    for (let i = 0; i < content.length; i += size) {
      chunks.push(content.slice(i, i + size));
    }
    return chunks;
  }
};
function isEventType(event, eventType) {
  return event.event === eventType;
}
function generateId() {
  const id = `${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  return id;
}
function createStepWrapper(originalStep, context) {
  if (!context || !originalStep) {
    return originalStep;
  }
  return new Proxy(originalStep, {
    get(target, prop, receiver) {
      if (prop === "run") {
        return async (stepId, fn) => {
          const originalRun = Reflect.get(
            target,
            "run",
            receiver
          );
          return originalRun(stepId, fn);
        };
      }
      return Reflect.get(
        target,
        prop,
        receiver
      );
    }
  });
}

// src/network.ts
var createNetwork = (opts) => new Network(opts);
var Network = class {
  constructor({
    name,
    description,
    agents,
    defaultModel,
    maxIter,
    defaultState,
    router,
    defaultRouter,
    history
  }) {
    this._counter = 0;
    this.name = name;
    this.description = description;
    this.agents = /* @__PURE__ */ new Map();
    this._agents = /* @__PURE__ */ new Map();
    this.defaultModel = defaultModel;
    this.router = defaultRouter != null ? defaultRouter : router;
    this.maxIter = maxIter || 0;
    this._stack = [];
    this.history = history;
    if (defaultState) {
      this.state = defaultState;
    } else {
      this.state = createState();
    }
    for (const agent of agents) {
      this.agents.set(agent.name, agent);
      this._agents.set(agent.name, agent);
    }
  }
  async availableAgents(networkRun = new NetworkRun(this, new State())) {
    var _a;
    const available = [];
    const all = Array.from(this.agents.values());
    for (const a of all) {
      const enabled = (_a = a == null ? void 0 : a.lifecycles) == null ? void 0 : _a.enabled;
      if (!enabled || await enabled({ agent: a, network: networkRun })) {
        available.push(a);
      }
    }
    return available;
  }
  /**
   * addAgent adds a new agent to the network.
   */
  addAgent(agent) {
    this.agents.set(agent.name, agent);
  }
  /**
   * run handles a given request using the network of agents.  It is not
   * concurrency-safe; you can only call run on a network once, as networks are
   * stateful.
   *
   */
  run(...[input, overrides]) {
    var _a;
    if (typeof input === "object" && typeof input.clientTimestamp === "string") {
      input.clientTimestamp = new Date(input.clientTimestamp);
    }
    let state;
    if (overrides == null ? void 0 : overrides.state) {
      if (overrides.state instanceof State) {
        state = overrides.state;
      } else {
        const stateObj = overrides.state;
        state = new State({
          data: stateObj.data || {},
          messages: stateObj._messages || [],
          results: stateObj._results || []
        });
      }
    } else {
      state = ((_a = this.state) == null ? void 0 : _a.clone()) || new State();
    }
    return new NetworkRun(this, state)["execute"](input, overrides);
  }
};
var defaultRoutingAgent;
var getDefaultRoutingAgent = () => {
  defaultRoutingAgent != null ? defaultRoutingAgent : defaultRoutingAgent = createRoutingAgent({
    name: "Default routing agent",
    description: "Selects which agents to work on based off of the current prompt and input.",
    lifecycle: {
      onRoute: ({ result }) => {
        const tool = result.toolCalls[0];
        if (!tool) {
          return;
        }
        if (tool.tool.name === "done") {
          return void 0;
        }
        if (tool.tool.name === "select_agent") {
          if (typeof tool.content === "object" && tool.content !== null && "data" in tool.content && typeof tool.content.data === "string") {
            return [tool.content.data];
          }
        }
        return;
      }
    },
    tools: [
      createTool({
        name: "select_agent",
        description: "Select an agent to handle the next step of the conversation",
        // Pure routing primitive (no side effect, no state mutation) — skip the
        // automatic durable-step wrap so routing doesn't spend a step per call.
        manualStep: true,
        parameters: import_zod3.z.object({
          name: import_zod3.z.string().describe("The name of the agent that should handle the request"),
          reason: import_zod3.z.string().optional().describe("Brief explanation of why this agent was chosen")
        }).strict(),
        handler: ({ name }, { network }) => {
          if (typeof name !== "string") {
            throw new Error("The routing agent requested an invalid agent");
          }
          const agent = network.agents.get(name);
          if (agent === void 0) {
            throw new Error(
              `The routing agent requested an agent that doesn't exist: ${name}`
            );
          }
          return agent.name;
        }
      }),
      createTool({
        name: "done",
        description: "Signal that the conversation is complete and no more agents need to be called",
        // Pure routing primitive — skip the automatic durable-step wrap.
        manualStep: true,
        parameters: import_zod3.z.object({
          summary: import_zod3.z.string().optional().describe("Brief summary of what was accomplished")
        }).strict(),
        handler: ({ summary }) => {
          return summary || "Conversation completed successfully";
        }
      })
    ],
    tool_choice: "any",
    // Allow the model to choose between select_agent or done
    system: async ({ network }) => {
      if (!network) {
        throw new Error(
          "The routing agent can only be used within a network of agents"
        );
      }
      const agents = await (network == null ? void 0 : network.availableAgents());
      return `You are the orchestrator between a group of agents. Each agent is suited for specific tasks and has a name, description, and tools.

The following agents are available:
<agents>
  ${agents.map((a) => {
        return `
    <agent>
      <name>${a.name}</name>
      <description>${a.description}</description>
      <tools>${JSON.stringify(Array.from(a.tools.values()))}</tools>
    </agent>`;
      }).join("\n")}
</agents>

Your responsibilities:
1. Analyze the conversation history and current state
2. Determine if the request has been completed or if more work is needed
3. Either:
   - Call select_agent to route to the appropriate agent for the next step
   - Call done if the conversation is complete or the user's request has been fulfilled

<instructions>
  - If the user's request has been addressed and no further action is needed, call the done tool
  - If more work is needed, select the most appropriate agent based on their capabilities
  - Consider the context and history when making routing decisions
  - Be efficient - don't route to agents unnecessarily if the task is complete
</instructions>`;
    }
  });
  return defaultRoutingAgent;
};
var NetworkRun = class extends Network {
  constructor(network, state) {
    super({
      name: network.name,
      description: network.description,
      agents: Array.from(network.agents.values()),
      defaultModel: network.defaultModel,
      defaultState: network.state,
      router: network.router,
      maxIter: network.maxIter,
      history: network.history
    });
    this.state = state;
  }
  run() {
    throw new Error("NetworkRun does not support run");
  }
  async availableAgents() {
    return super.availableAgents(this);
  }
  /**
   * Schedule is used to push an agent's run function onto the stack.
   */
  schedule(agentName) {
    this["_stack"].push(agentName);
  }
  async execute(...[input, overrides]) {
    var _a, _b, _c, _d, _e;
    const stepTools = await getStepTools();
    let networkRunId;
    if (stepTools) {
      networkRunId = await stepTools.run("generate-network-id", () => {
        return (0, import_crypto.randomUUID)();
      });
    } else {
      networkRunId = (0, import_crypto.randomUUID)();
    }
    const streamingPublish = (_a = overrides == null ? void 0 : overrides.streaming) == null ? void 0 : _a.publish;
    let streamingContext;
    const inputContent = typeof input === "object" && input !== null && "content" in input ? input.content : input;
    const hadClientThreadId = Boolean(this.state.threadId);
    await initializeThread({
      state: this.state,
      history: this.history,
      input: inputContent,
      network: this
    });
    if ((_b = this.history) == null ? void 0 : _b.appendUserMessage) {
      let userMessage;
      if (typeof input === "object" && input !== null && "id" in input) {
        const userInput = input;
        const timestamp = userInput.clientTimestamp instanceof Date ? userInput.clientTimestamp : userInput.clientTimestamp ? new Date(userInput.clientTimestamp) : /* @__PURE__ */ new Date();
        userMessage = {
          id: userInput.id,
          content: userInput.content,
          role: "user",
          timestamp
        };
      } else {
        userMessage = {
          id: (0, import_crypto.randomUUID)(),
          content: input,
          role: "user",
          timestamp: /* @__PURE__ */ new Date()
        };
      }
      await this.history.appendUserMessage({
        state: this.state,
        network: this,
        input: inputContent,
        threadId: this.state.threadId,
        userMessage,
        step: stepTools || void 0
      });
    }
    if (hadClientThreadId) {
      await loadThreadFromStorage({
        state: this.state,
        history: this.history,
        input: inputContent,
        network: this
      });
    }
    if (streamingPublish) {
      streamingContext = StreamingContext.fromNetworkState(this.state, {
        publish: streamingPublish,
        runId: networkRunId,
        messageId: networkRunId,
        // Use networkRunId as messageId for network-level events
        scope: "network",
        simulateChunking: (_c = overrides == null ? void 0 : overrides.streaming) == null ? void 0 : _c.simulateChunking,
        chunkSize: (_d = overrides == null ? void 0 : overrides.streaming) == null ? void 0 : _d.chunkSize,
        maxChunksPerMessage: (_e = overrides == null ? void 0 : overrides.streaming) == null ? void 0 : _e.maxChunksPerMessage
      });
      await streamingContext.publishEvent({
        event: "run.started",
        data: {
          runId: networkRunId,
          scope: "network",
          name: this.name,
          messageId: networkRunId,
          // Network events use networkRunId as messageId
          threadId: this.state.threadId
        }
      });
    }
    const step = await getStepTools();
    const wrappedStep = createStepWrapper(step, streamingContext);
    const available = await this.availableAgents();
    if (available.length === 0) {
      throw new Error("no agents enabled in network");
    }
    const initialResultCount = this.state.results.length;
    try {
      const next = await this.getNextAgents(
        input,
        // Pass full UserMessage object, not extracted content
        (overrides == null ? void 0 : overrides.router) || (overrides == null ? void 0 : overrides.defaultRouter) || this.router
      );
      if (!(next == null ? void 0 : next.length)) {
        return this;
      }
      for (const agent of next) {
        this.schedule(agent.name);
      }
      while (this._stack.length > 0 && (this.maxIter === 0 || this._counter < this.maxIter)) {
        const agentName = this._stack.shift();
        const agent = agentName && this._agents.get(agentName);
        if (!agent) {
          if (streamingContext) {
            await streamingContext.publishEvent({
              event: "run.completed",
              data: {
                runId: networkRunId,
                scope: "network",
                name: this.name,
                messageId: networkRunId
                // Use networkRunId for network completion
              }
            });
            await streamingContext.publishEvent({
              event: "stream.ended",
              data: {
                scope: "network",
                messageId: networkRunId
              }
            });
          }
          return this;
        }
        let agentRunId;
        let agentMessageId;
        if (stepTools) {
          const agentIds = await stepTools.run(
            `generate-agent-ids-${this._counter}`,
            () => {
              return {
                agentRunId: generateId(),
                agentMessageId: (0, import_crypto.randomUUID)()
              };
            }
          );
          agentRunId = agentIds.agentRunId;
          agentMessageId = agentIds.agentMessageId;
        } else {
          agentRunId = generateId();
          agentMessageId = (0, import_crypto.randomUUID)();
        }
        let agentStreamingContext;
        if (streamingContext) {
          agentStreamingContext = streamingContext.createContextWithSharedSequence({
            runId: agentRunId,
            messageId: agentMessageId,
            scope: "agent"
          });
          await streamingContext.publishEvent({
            event: "run.started",
            data: {
              runId: agentRunId,
              parentRunId: networkRunId,
              scope: "agent",
              name: agent.name,
              messageId: agentMessageId
              // Use agent-specific messageId
            }
          });
        }
        const call = await agent.run(inputContent, {
          network: this,
          // One inference per network iteration; decoupled from network.maxIter.
          maxIterPerRun: 1,
          // Provide streaming context so the agent can emit part/text/tool events
          streamingContext: agentStreamingContext,
          // Provide wrapped step tools for automatic step lifecycle events
          step: wrappedStep
        });
        call.id = agentMessageId;
        if (agentStreamingContext) {
          await agentStreamingContext.publishEvent({
            event: "run.completed",
            data: {
              runId: agentRunId,
              scope: "agent",
              name: agent.name,
              messageId: agentMessageId
              // Include agent-specific messageId in completion event
            }
          });
        }
        this._counter += 1;
        this.state.appendResult(call);
        await persistResults(
          {
            state: this.state,
            history: this.history,
            input: inputContent,
            network: this
          },
          [call],
          incrementalAppendStepId(this._counter)
        );
        const next2 = await this.getNextAgents(
          input,
          // Pass full UserMessage object, not extracted content
          (overrides == null ? void 0 : overrides.router) || (overrides == null ? void 0 : overrides.defaultRouter) || this.router
        );
        for (const a of next2 || []) {
          this.schedule(a.name);
        }
      }
      await saveThreadToStorage({
        state: this.state,
        history: this.history,
        input: inputContent,
        initialResultCount,
        network: this
      });
    } catch (error) {
      if (streamingContext) {
        try {
          await streamingContext.publishEvent({
            event: "run.failed",
            data: {
              runId: networkRunId,
              scope: "network",
              name: this.name,
              messageId: networkRunId,
              // Use networkRunId for network error events
              error: error instanceof Error ? error.message : String(error),
              recoverable: false
            }
          });
        } catch (streamingError) {
          console.warn("Failed to publish run.failed event:", streamingError);
        }
      }
      throw error;
    } finally {
      if (streamingContext) {
        try {
          await streamingContext.publishEvent({
            event: "run.completed",
            data: {
              runId: networkRunId,
              scope: "network",
              name: this.name,
              messageId: networkRunId
              // Use networkRunId for network completion in finally block
            }
          });
          await streamingContext.publishEvent({
            event: "stream.ended",
            data: {
              scope: "network",
              messageId: networkRunId
            }
          });
        } catch (streamingError) {
          console.warn("Failed to publish completion events:", streamingError);
        }
      }
    }
    return this;
  }
  async getNextAgents(input, router) {
    if (!router && !this.defaultModel) {
      throw new Error(
        "No router or model defined in network.  You must pass a router or a default model to use the built-in agentic router."
      );
    }
    if (!router) {
      router = getDefaultRoutingAgent();
    }
    if (router instanceof RoutingAgent) {
      const inputContent = typeof input === "object" && input !== null && "content" in input ? input.content : input;
      return await this.getNextAgentsViaRoutingAgent(router, inputContent);
    }
    const stack = this._stack.map((name) => {
      const agent2 = this._agents.get(name);
      if (!agent2) {
        throw new Error(`unknown agent in the network stack: ${name}`);
      }
      return agent2;
    });
    const routerInputContent = typeof input === "object" && input !== null && "content" in input ? input.content : input;
    const agent = await router({
      input: routerInputContent,
      // Always pass string content for backwards compatibility
      userMessage: typeof input === "object" && input !== null && "content" in input ? input : void 0,
      network: this,
      stack,
      lastResult: this.state.results[this.state.results.length - 1],
      callCount: this._counter
    });
    if (!agent) {
      return;
    }
    if (agent instanceof RoutingAgent) {
      const inputContent = typeof input === "object" && input !== null && "content" in input ? input.content : input;
      return await this.getNextAgentsViaRoutingAgent(agent, inputContent);
    }
    for (const a of Array.isArray(agent) ? agent : [agent]) {
      if (!this._agents.has(a.name)) {
        this._agents.set(a.name, a);
      }
    }
    return Array.isArray(agent) ? agent : [agent];
  }
  async getNextAgentsViaRoutingAgent(routingAgent, input) {
    const result = await routingAgent.run(input, {
      network: this,
      model: routingAgent.model || this.defaultModel
    });
    const agentNames = routingAgent.lifecycles.onRoute({
      result,
      agent: routingAgent,
      network: this
    });
    return (agentNames || []).map((name) => this.agents.get(name)).filter(Boolean);
  }
};

// src/tool.ts
function createTool({
  name,
  description,
  parameters,
  manualStep,
  handler
}) {
  return {
    name,
    description,
    parameters,
    manualStep,
    handler(input, opts) {
      return handler(input, opts);
    }
  };
}
function createToolManifest(tools) {
  const manifest = {};
  for (const t of tools) {
    manifest[t.name] = { input: {}, output: {} };
  }
  return manifest;
}

// src/converters.ts
var ANTHROPIC_CACHE_CONTROL = {
  anthropic: { cacheControl: { type: "ephemeral" } }
};
function messagesToCoreMessages(messages, opts = {}) {
  const result = [];
  for (const msg of messages) {
    switch (msg.type) {
      case "text": {
        result.push(textMessageToModelMessage(msg));
        break;
      }
      case "reasoning": {
        const parts = reasoningMessageToParts(msg);
        if (parts.length > 0) {
          result.push({ role: "assistant", content: parts });
        }
        break;
      }
      case "tool_call": {
        result.push({
          role: "assistant",
          content: msg.tools.map((tool) => ({
            type: "tool-call",
            toolCallId: tool.id,
            toolName: tool.name,
            input: tool.input
          }))
        });
        break;
      }
      case "tool_result": {
        const part = {
          type: "tool-result",
          toolCallId: msg.tool.id,
          toolName: msg.tool.name,
          output: toToolResultOutput(msg.content)
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
function textMessageToModelMessage(msg) {
  if (typeof msg.content !== "string" && msg.role === "user" && msg.content.some((c) => c.type === "image")) {
    const parts = msg.content.map(
      (c) => c.type === "image" ? imageContentToImagePart(c) : { type: "text", text: c.text }
    );
    return { role: "user", content: parts };
  }
  const content = typeof msg.content === "string" ? msg.content : msg.content.map((c) => c.type === "text" ? c.text : "").join("");
  return { role: msg.role, content };
}
function imageContentToImagePart(c) {
  return c.mimeType ? { type: "image", image: c.image, mediaType: c.mimeType } : { type: "image", image: c.image };
}
function reasoningMessageToParts(msg) {
  if (msg.details && msg.details.length > 0) {
    return msg.details.map(
      (d) => d.type === "redacted" ? {
        type: "reasoning",
        text: "",
        providerOptions: { anthropic: { redactedData: d.data } }
      } : d.signature ? {
        type: "reasoning",
        text: d.text,
        providerOptions: { anthropic: { signature: d.signature } }
      } : { type: "reasoning", text: d.text }
    );
  }
  if (msg.content && msg.content.length > 0) {
    return [
      msg.signature ? {
        type: "reasoning",
        text: msg.content,
        providerOptions: { anthropic: { signature: msg.signature } }
      } : { type: "reasoning", text: msg.content }
    ];
  }
  return [];
}
function toToolResultOutput(content) {
  if (typeof content === "string") {
    return { type: "text", value: content };
  }
  const multipart = toToolResultContentValue(content);
  if (multipart) {
    return { type: "content", value: multipart };
  }
  return { type: "json", value: content };
}
function toToolResultContentValue(content) {
  if (!Array.isArray(content)) return void 0;
  const parts = [];
  let hasImage = false;
  for (const block of content) {
    if (!block || typeof block !== "object") {
      parts.push({ type: "text", text: String(block) });
      continue;
    }
    const b = block;
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
            mediaType: img.mediaType
          });
        } else {
          parts.push({ type: "image-url", url: img.url });
        }
        hasImage = true;
        continue;
      }
    }
    parts.push({ type: "text", text: JSON.stringify(block) });
  }
  return hasImage ? parts : void 0;
}
function extractImage(b) {
  var _a, _b;
  const source = b.source;
  if (source && typeof source === "object") {
    if (source.type === "base64" && typeof source.data === "string") {
      return {
        kind: "data",
        data: source.data,
        mediaType: typeof source.media_type === "string" ? source.media_type : "image/png"
      };
    }
    if (source.type === "url" && typeof source.url === "string") {
      return { kind: "url", url: source.url };
    }
  }
  if (typeof b.data === "string") {
    const mediaType = (_a = b.mediaType) != null ? _a : b.mimeType;
    return {
      kind: "data",
      data: b.data,
      mediaType: typeof mediaType === "string" ? mediaType : "image/png"
    };
  }
  if (typeof b.image === "string") {
    const mediaType = (_b = b.mediaType) != null ? _b : b.mimeType;
    return parseImageString(
      b.image,
      typeof mediaType === "string" ? mediaType : void 0
    );
  }
  return void 0;
}
function parseImageString(s, mediaType) {
  const dataUrl = /^data:([^;]+);base64,([\s\S]*)$/.exec(s);
  if (dataUrl) {
    return { kind: "data", data: dataUrl[2], mediaType: dataUrl[1] };
  }
  if (/^https?:\/\//i.test(s)) {
    return { kind: "url", url: s };
  }
  return { kind: "data", data: s, mediaType: mediaType != null ? mediaType : "image/png" };
}
function applySystemCacheControl(messages) {
  var _a;
  for (let i = messages.length - 1; i >= 0; i--) {
    const m = messages[i];
    if (m.role === "system") {
      m.providerOptions = __spreadValues(__spreadValues({}, (_a = m.providerOptions) != null ? _a : {}), ANTHROPIC_CACHE_CONTROL);
      return;
    }
  }
}
function toSerializableResult(result) {
  const out = {
    text: result.text,
    toolCalls: result.toolCalls.map((tc) => ({
      toolCallId: tc.toolCallId,
      toolName: tc.toolName,
      args: tc.input
    })),
    finishReason: result.finishReason
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
function reasoningPartsToDetails(parts) {
  if (!parts || parts.length === 0) return [];
  return parts.map((p) => {
    var _a, _b;
    const meta = (_b = (_a = p.providerOptions) != null ? _a : p.providerMetadata) == null ? void 0 : _b.anthropic;
    const signature = meta == null ? void 0 : meta.signature;
    return typeof signature === "string" && signature !== "" ? { type: "text", text: p.text, signature } : { type: "text", text: p.text };
  });
}
function extractUsage(result) {
  var _a;
  const u = result.usage;
  const details = u == null ? void 0 : u.inputTokenDetails;
  const anthropic = (_a = result.providerMetadata) == null ? void 0 : _a.anthropic;
  const cacheRead = firstNumber(
    details == null ? void 0 : details.cacheReadTokens,
    u == null ? void 0 : u.cachedInputTokens,
    anthropic == null ? void 0 : anthropic.cacheReadInputTokens
  );
  const cacheWrite = firstNumber(
    details == null ? void 0 : details.cacheWriteTokens,
    anthropic == null ? void 0 : anthropic.cacheCreationInputTokens
  );
  let inputNonCache = numberOrUndefined(details == null ? void 0 : details.noCacheTokens);
  if (inputNonCache === void 0 && (u == null ? void 0 : u.inputTokens) !== void 0) {
    inputNonCache = u.inputTokens - (cacheRead != null ? cacheRead : 0) - (cacheWrite != null ? cacheWrite : 0);
  }
  if (!u && cacheRead === void 0 && cacheWrite === void 0) {
    return void 0;
  }
  const usage = {
    input_tokens: inputNonCache != null ? inputNonCache : 0,
    output_tokens: numberOrZero(u == null ? void 0 : u.outputTokens),
    total_tokens: numberOrZero(u == null ? void 0 : u.totalTokens)
  };
  if (cacheWrite !== void 0) {
    usage.cache_creation_input_tokens = cacheWrite;
  }
  if (cacheRead !== void 0) {
    usage.cache_read_input_tokens = cacheRead;
  }
  return usage;
}
function numberOrZero(v) {
  return typeof v === "number" && Number.isFinite(v) ? v : 0;
}
function numberOrUndefined(v) {
  return typeof v === "number" && Number.isFinite(v) ? v : void 0;
}
function firstNumber(...values) {
  for (const v of values) {
    const n = numberOrUndefined(v);
    if (n !== void 0) return n;
  }
  return void 0;
}
function resultToMessages(result) {
  const messages = [];
  const hasToolCalls = result.toolCalls && result.toolCalls.length > 0;
  const reasoning = reasoningResultToMessage(result, hasToolCalls);
  if (reasoning) {
    messages.push(reasoning);
  }
  if (result.text && result.text.trim() !== "") {
    const msg = {
      type: "text",
      role: "assistant",
      content: result.text,
      stop_reason: hasToolCalls ? "tool" : "stop"
    };
    messages.push(msg);
  }
  if (hasToolCalls) {
    const msg = {
      type: "tool_call",
      role: "assistant",
      stop_reason: "tool",
      tools: result.toolCalls.map(
        (tc) => ({
          type: "tool",
          id: tc.toolCallId,
          name: tc.toolName,
          input: tc.args
        })
      )
    };
    messages.push(msg);
  }
  if (messages.length === 0) {
    const msg = {
      type: "text",
      role: "assistant",
      content: "",
      stop_reason: "stop"
    };
    messages.push(msg);
  }
  return messages;
}
function reasoningResultToMessage(result, hasToolCalls) {
  var _a, _b, _c;
  const details = (_a = result.reasoningDetails) != null ? _a : [];
  const text = (_b = result.reasoning) != null ? _b : details.filter(
    (d) => d.type === "text"
  ).map((d) => d.text).join("");
  if ((!text || text.trim() === "") && details.length === 0) {
    return void 0;
  }
  const signature = (_c = details.find(
    (d) => d.type === "text" && typeof d.signature === "string" && d.signature !== ""
  )) == null ? void 0 : _c.signature;
  const msg = {
    type: "reasoning",
    role: "assistant",
    content: text || "",
    stop_reason: hasToolCalls ? "tool" : "stop"
  };
  if (details.length > 0) {
    msg.details = details;
  }
  if (signature) {
    msg.signature = signature;
  }
  return msg;
}
function toolsToAiTools(tools, opts = {}) {
  const result = {};
  for (const tool of tools) {
    let inputSchema;
    if (tool.parameters) {
      try {
        inputSchema = (0, import_ai2.jsonSchema)(
          import_zod5.z.toJSONSchema(tool.parameters, { target: "draft-7" })
        );
      } catch (e) {
        inputSchema = (0, import_ai2.jsonSchema)({ type: "object", properties: {} });
      }
    } else {
      inputSchema = (0, import_ai2.jsonSchema)({ type: "object", properties: {} });
    }
    result[tool.name] = {
      description: tool.description,
      inputSchema
    };
  }
  if (opts.cacheControl) {
    const names = Object.keys(result);
    const last = names[names.length - 1];
    if (last) {
      result[last].providerOptions = ANTHROPIC_CACHE_CONTROL;
    }
  }
  return result;
}
function mapToolChoice(choice) {
  switch (choice) {
    case "auto":
      return "auto";
    case "any":
      return "required";
    default:
      return { type: "tool", toolName: choice };
  }
}

// src/model.ts
var createAgenticModelFromLanguageModel = (model, options = {}) => {
  return new AgenticModel(model, options);
};
var _model, _cacheControl;
var AgenticModel = class {
  constructor(model, options = {}) {
    __privateAdd(this, _model);
    __privateAdd(this, _cacheControl);
    __privateSet(this, _model, model);
    __privateSet(this, _cacheControl, resolveCacheControl(model, options.cacheControl));
  }
  async infer(stepID, input, tools, tool_choice) {
    const convertOpts = { cacheControl: __privateGet(this, _cacheControl) };
    const messages = messagesToCoreMessages(input, convertOpts);
    const aiTools = tools.length > 0 ? toolsToAiTools(tools, convertOpts) : void 0;
    const doInference = async () => {
      const result2 = await (0, import_ai3.generateText)({
        model: __privateGet(this, _model),
        messages,
        tools: aiTools,
        toolChoice: aiTools ? mapToolChoice(tool_choice) : void 0
      });
      return toSerializableResult(result2);
    };
    const step = await getStepTools();
    const result = step ? await step.run(stepID, doInference) : await doInference();
    return { output: resultToMessages(result), raw: result };
  }
};
_model = new WeakMap();
_cacheControl = new WeakMap();
function resolveCacheControl(model, setting) {
  if (setting === true || setting === false) {
    return setting;
  }
  return isAnthropicModel(model);
}
function isAnthropicModel(model) {
  var _a;
  const provider = typeof model === "string" ? model : (_a = model.provider) != null ? _a : "";
  return provider.toLowerCase().includes("anthropic");
}

// src/agent.ts
var safeSerialize = (value) => {
  try {
    return JSON.stringify(value);
  } catch (e) {
    return void 0;
  }
};
var DEFAULT_MAX_ITER_PER_RUN = 1;
var createAgent = (opts) => new Agent(opts);
var createRoutingAgent = (opts) => new RoutingAgent(opts);
var Agent = class _Agent {
  constructor(opts) {
    this.name = opts.name;
    this.description = opts.description || "";
    this.system = opts.system;
    this.assistant = opts.assistant || "";
    this.tools = /* @__PURE__ */ new Map();
    this.tool_choice = opts.tool_choice;
    this.lifecycles = opts.lifecycle;
    this.model = opts.model;
    this.maxIterPerRun = opts.maxIterPerRun;
    this.history = opts.history;
    this.setTools(opts.tools);
    this.mcpServers = opts.mcpServers;
    this._mcpClients = [];
  }
  setTools(tools) {
    for (const tool of tools || []) {
      if (isInngestFn(tool)) {
        this.tools.set(tool["absoluteId"], {
          name: tool["absoluteId"],
          description: tool.description,
          // TODO Should we error here if we can't find an input schema?
          parameters: getInngestFnInput(tool),
          // This handler calls `step.invoke`, which is itself a step operation
          // and CANNOT run inside a `step.run`. Opt out of the automatic durable
          // wrap so it executes inline with the live step (step.invoke is
          // already durable on its own).
          manualStep: true,
          handler: async (input, opts) => {
            const step = await getStepTools();
            if (!step) {
              throw new Error("Inngest tool called outside of Inngest context");
            }
            const stepId = `${opts.agent.name}/tools/${tool["absoluteId"]}`;
            return step.invoke(stepId, {
              function: (0, import_inngest6.referenceFunction)({
                appId: tool["client"]["id"],
                functionId: tool.id()
              }),
              data: input
            });
          }
        });
      } else {
        this.tools.set(tool.name, tool);
      }
    }
  }
  withModel(model) {
    return new _Agent({
      name: this.name,
      description: this.description,
      system: this.system,
      assistant: this.assistant,
      tools: Array.from(this.tools.values()),
      lifecycle: this.lifecycles,
      model,
      maxIterPerRun: this.maxIterPerRun
    });
  }
  /**
   * Run runs an agent with the given user input, treated as a user message.  If
   * the input is an empty string, only the system prompt will execute.
   */
  async run(input, {
    model,
    network,
    state,
    maxIter = 0,
    maxIterPerRun,
    streaming,
    streamingContext,
    step
  } = {}) {
    var _a, _b, _c;
    await this.initMCP();
    const internalMaxIter = (_a = maxIterPerRun != null ? maxIterPerRun : this.maxIterPerRun) != null ? _a : maxIter && maxIter > 0 ? maxIter : DEFAULT_MAX_ITER_PER_RUN;
    const rawModel = model || this.model || (network == null ? void 0 : network.defaultModel);
    if (!rawModel) {
      throw new Error("No model provided to agent");
    }
    const p = createAgenticModelFromLanguageModel(rawModel);
    const s = state || (network == null ? void 0 : network.state) || new State();
    const run = new NetworkRun(
      network || createNetwork({ name: "default", agents: [] }),
      s
    );
    let standaloneStreamingContext;
    let standaloneWrappedStep;
    if (!network && (streaming == null ? void 0 : streaming.publish)) {
      const stepTools = await getStepTools();
      let agentRunId;
      let messageId;
      if (stepTools) {
        const ids = await stepTools.run("generate-standalone-agent-ids", () => {
          return {
            agentRunId: generateId(),
            messageId: (0, import_crypto2.randomUUID)()
          };
        });
        agentRunId = ids.agentRunId;
        messageId = ids.messageId;
      } else {
        agentRunId = generateId();
        messageId = (0, import_crypto2.randomUUID)();
      }
      standaloneStreamingContext = StreamingContext.fromNetworkState(s, {
        publish: streaming.publish,
        runId: agentRunId,
        messageId,
        scope: "agent",
        simulateChunking: streaming.simulateChunking,
        chunkSize: streaming.chunkSize,
        maxChunksPerMessage: streaming.maxChunksPerMessage
      });
      standaloneWrappedStep = createStepWrapper(
        stepTools,
        standaloneStreamingContext
      );
      await standaloneStreamingContext.publishEvent({
        event: "run.started",
        data: {
          runId: agentRunId,
          scope: "agent",
          name: this.name,
          messageId,
          threadId: s.threadId
        }
      });
    }
    const effectiveStreamingContext = streamingContext || standaloneStreamingContext;
    const effectiveStep = step || standaloneWrappedStep;
    const inputContent = typeof input === "object" && input !== null && "content" in input ? input.content : input;
    await initializeThread({
      state: s,
      history: this.history,
      input: inputContent,
      network: run
    });
    await loadThreadFromStorage({
      state: s,
      history: this.history,
      input: inputContent,
      network: run
    });
    let history = s ? s.formatHistory() : [];
    let prompt = await this.agentPrompt(input, run);
    let result = new AgentResult(
      this.name,
      [],
      [],
      /* @__PURE__ */ new Date(),
      prompt,
      history,
      ""
    );
    let hasMoreActions = true;
    let iter = 0;
    const initialResultCount = s.results.length;
    try {
      do {
        if ((_b = this.lifecycles) == null ? void 0 : _b.onStart) {
          const modified = await this.lifecycles.onStart({
            agent: this,
            network: run,
            input: inputContent,
            prompt,
            history
          });
          if (modified.stop) {
            return result;
          }
          prompt = modified.prompt;
          history = modified.history;
        }
        const inference = await this.performInference(
          p,
          prompt,
          history,
          run,
          effectiveStreamingContext,
          effectiveStep
        );
        const lastActionableMessage = inference.output.filter((m) => m.type !== "reasoning").pop();
        hasMoreActions = Boolean(
          this.tools.size > 0 && lastActionableMessage && lastActionableMessage.stop_reason !== "stop"
        );
        result = inference;
        if (standaloneStreamingContext) {
          result.id = standaloneStreamingContext.messageId;
        }
        history = [...inference.output, ...inference.toolCalls];
        iter++;
      } while (hasMoreActions && iter < internalMaxIter);
      if ((_c = this.lifecycles) == null ? void 0 : _c.onFinish) {
        result = await this.lifecycles.onFinish({
          agent: this,
          network: run,
          result
        });
      }
      await saveThreadToStorage({
        state: s,
        history: this.history,
        input: inputContent,
        initialResultCount,
        network: run
      });
    } catch (error) {
      if (standaloneStreamingContext) {
        try {
          await standaloneStreamingContext.publishEvent({
            event: "run.failed",
            data: {
              runId: standaloneStreamingContext.runId,
              scope: "agent",
              name: this.name,
              error: error instanceof Error ? error.message : String(error),
              recoverable: false
            }
          });
        } catch (streamingError) {
          console.warn("Failed to publish run.failed event:", streamingError);
        }
      }
      throw error;
    } finally {
      if (standaloneStreamingContext) {
        try {
          await standaloneStreamingContext.publishEvent({
            event: "run.completed",
            data: {
              runId: standaloneStreamingContext.runId,
              scope: "agent",
              name: this.name
            }
          });
          await standaloneStreamingContext.publishEvent({
            event: "stream.ended",
            data: {
              scope: "agent",
              messageId: standaloneStreamingContext.messageId
            }
          });
        } catch (streamingError) {
          console.warn("Failed to publish completion events:", streamingError);
        }
      }
    }
    return result;
  }
  async performInference(p, prompt, history, network, streamingContext, step) {
    var _a;
    const { output, raw } = await p.infer(
      this.name,
      prompt.concat(history),
      Array.from(this.tools.values()),
      this.tool_choice || "auto"
    );
    let result = new AgentResult(
      this.name,
      output,
      [],
      /* @__PURE__ */ new Date(),
      prompt,
      history,
      typeof raw === "string" ? raw : JSON.stringify(raw)
    );
    if ((_a = this.lifecycles) == null ? void 0 : _a.onResponse) {
      result = await this.lifecycles.onResponse({
        agent: this,
        network,
        result
      });
    }
    if (streamingContext) {
      const reasoningMsgs = result.output.filter((m) => m.type === "reasoning");
      for (const msg of reasoningMsgs) {
        if (msg.type !== "reasoning") continue;
        const stepTools = step || await getStepTools();
        const partId = stepTools ? await stepTools.run(
          `generate-reasoning-part-id-${streamingContext.messageId}`,
          () => {
            return streamingContext.generatePartId();
          }
        ) : streamingContext.generatePartId();
        await streamingContext.publishEvent({
          event: "part.created",
          data: {
            partId,
            runId: streamingContext.runId,
            messageId: streamingContext.messageId,
            type: "reasoning",
            metadata: { agentName: this.name }
          }
        });
        for (const delta of streamingContext.chunkContent(msg.content)) {
          await streamingContext.publishEvent({
            event: "reasoning.delta",
            data: {
              partId,
              messageId: streamingContext.messageId,
              delta
            }
          });
        }
        await streamingContext.publishEvent({
          event: "part.completed",
          data: {
            partId,
            runId: streamingContext.runId,
            messageId: streamingContext.messageId,
            type: "reasoning",
            finalContent: msg.content
          }
        });
      }
    }
    if (streamingContext) {
      const lastTextMsg = [...result.output].reverse().find((m) => m.type === "text" && m.role === "assistant");
      let content = "";
      if (lastTextMsg && lastTextMsg.type === "text") {
        const anyMsg = lastTextMsg;
        if (typeof anyMsg.content === "string") {
          content = anyMsg.content;
        } else if (Array.isArray(anyMsg.content)) {
          content = anyMsg.content.map((c) => c.text).join("");
        }
      }
      if (content && content.length > 0) {
        const stepTools = step || await getStepTools();
        const partId = stepTools ? await stepTools.run(
          `generate-text-part-id-${streamingContext.messageId}`,
          () => {
            return streamingContext.generatePartId();
          }
        ) : streamingContext.generatePartId();
        await streamingContext.publishEvent({
          event: "part.created",
          data: {
            partId,
            runId: streamingContext.runId,
            messageId: streamingContext.messageId,
            type: "text",
            metadata: { agentName: this.name }
          }
        });
        for (const delta of streamingContext.chunkContent(content)) {
          await streamingContext.publishEvent({
            event: "text.delta",
            data: {
              partId,
              messageId: streamingContext.messageId,
              delta
            }
          });
        }
        await streamingContext.publishEvent({
          event: "part.completed",
          data: {
            partId,
            runId: streamingContext.runId,
            messageId: streamingContext.messageId,
            type: "text",
            finalContent: content
          }
        });
      }
    }
    const toolCallOutput = await this.invokeTools(
      result.output,
      network,
      streamingContext,
      step
    );
    if (toolCallOutput.length > 0) {
      result.toolCalls = result.toolCalls.concat(toolCallOutput);
    }
    return result;
  }
  /**
   * invokeTools takes output messages from an inference call then invokes any tools
   * in the message responses.
   */
  async invokeTools(msgs, network, streamingContext, step) {
    var _a, _b;
    const output = [];
    for (const msg of msgs) {
      if (msg.type !== "tool_call") {
        continue;
      }
      if (!Array.isArray(msg.tools)) {
        continue;
      }
      for (const tool of msg.tools) {
        const found = this.tools.get(tool.name);
        if (!found) {
          throw new Error(
            `Inference requested a non-existent tool: ${tool.name}`
          );
        }
        const toolArgsJson = JSON.stringify((_a = tool.input) != null ? _a : {});
        if (streamingContext) {
          const stepTools = step || await getStepTools();
          const toolCallPartId = stepTools ? await stepTools.run(
            `generate-tool-part-id-${streamingContext.messageId}-${tool.name}`,
            () => {
              return streamingContext.generatePartId();
            }
          ) : streamingContext.generatePartId();
          await streamingContext.publishEvent({
            event: "part.created",
            data: {
              partId: toolCallPartId,
              runId: streamingContext.runId,
              messageId: streamingContext.messageId,
              type: "tool-call",
              metadata: { toolName: tool.name, agentName: this.name }
            }
          });
          const argChunks = streamingContext.chunkContent(toolArgsJson);
          for (let i = 0; i < argChunks.length; i++) {
            await streamingContext.publishEvent({
              event: "tool_call.arguments.delta",
              data: {
                partId: toolCallPartId,
                delta: argChunks[i],
                // toolName is included only on the first delta.
                toolName: i === 0 ? tool.name : void 0,
                messageId: streamingContext.messageId
              }
            });
          }
          await streamingContext.publishEvent({
            event: "part.completed",
            data: {
              partId: toolCallPartId,
              runId: streamingContext.runId,
              messageId: streamingContext.messageId,
              type: "tool-call",
              finalContent: (_b = tool.input) != null ? _b : {},
              metadata: { toolName: tool.name, agentName: this.name }
            }
          });
        }
        const result = await this.runToolHandler(
          found,
          tool,
          network,
          step
        );
        if (streamingContext) {
          const stepTools = step || await getStepTools();
          const outputPartId = stepTools ? await stepTools.run(
            `generate-output-part-id-${streamingContext.messageId}-${tool.name}`,
            () => {
              return streamingContext.generatePartId();
            }
          ) : streamingContext.generatePartId();
          await streamingContext.publishEvent({
            event: "part.created",
            data: {
              partId: outputPartId,
              runId: streamingContext.runId,
              messageId: streamingContext.messageId,
              type: "tool-output",
              metadata: { toolName: tool.name, agentName: this.name }
            }
          });
          const resultJson = JSON.stringify(result);
          for (const delta of streamingContext.chunkContent(resultJson)) {
            await streamingContext.publishEvent({
              event: "tool_call.output.delta",
              data: {
                partId: outputPartId,
                delta,
                messageId: streamingContext.messageId
              }
            });
          }
          await streamingContext.publishEvent({
            event: "part.completed",
            data: {
              partId: outputPartId,
              runId: streamingContext.runId,
              messageId: streamingContext.messageId,
              type: "tool-output",
              finalContent: result,
              metadata: { toolName: tool.name, agentName: this.name }
            }
          });
        }
        output.push({
          role: "tool_result",
          type: "tool_result",
          tool: {
            type: "tool",
            id: tool.id,
            name: tool.name,
            input: tool.input.arguments
          },
          content: result,
          stop_reason: "tool"
        });
      }
    }
    return output;
  }
  /**
   * Invoke one tool-call handler, normalizing its return/throw into the
   * {@link ToolHandlerResult} shape fed back to the model.
   *
   * Durable-by-default: when an Inngest step is available AND the tool has not
   * set `manualStep`, the handler runs inside ONE `step.run` under a
   * deterministic id, so its side effect fires EXACTLY ONCE across Inngest's
   * re-executions (no `edit_file` double-apply, no image-tool re-bill). Memoized
   * inferences replay the same tool calls in the same order, so the per-run
   * counter (`network.state.nextDurableToolCallIndex()`) assigns the Nth tool
   * call the same id on every replay — never a checksum/timestamp/uuid.
   *
   * Two subtleties handled here:
   *   - State mutations: a tool may mutate `network.state.data` (design
   *     questions / plan / `__stopReason`). That mutation happens INSIDE the
   *     step and is lost on replay (the memoized body is skipped), so we memoize
   *     the post-handler data snapshot in the step payload and RE-APPLY it
   *     outside the step on every execution. State-mutating tools keep working
   *     with no code change.
   *   - Errors: captured and returned as `{ error }` (never thrown) so the step
   *     is recorded as a success returning the error — the failing side effect
   *     is not retried, and the model sees the same result on replay.
   *
   * Inline fallback: no step in context (non-Inngest run / tests) OR the tool
   * opted out via `manualStep`. The handler then receives the live `step` and
   * owns its own durability. A wrapped handler instead receives `step:
   * undefined` (it is already inside a step; a nested `step.run` would break
   * Inngest).
   */
  async runToolHandler(toolDef, call, network, step) {
    const invoke = async (handlerStep) => {
      try {
        const r = await toolDef.handler(call.input, {
          agent: this,
          network,
          step: handlerStep
        });
        return {
          data: typeof r === "undefined" ? `${call.name} successfully executed` : r
        };
      } catch (err) {
        return { error: import_internals.errors.serializeError(err) };
      }
    };
    const durableStep = step != null ? step : await getStepTools();
    if (!durableStep || toolDef.manualStep) {
      return invoke(step);
    }
    const index = network.state.nextDurableToolCallIndex();
    const stepId = `${this.name}/tool/${call.name}/${index}`;
    const memoized = await durableStep.run(stepId, async () => {
      const before = safeSerialize(network.state.data);
      const result = await invoke(void 0);
      const after = network.state.data;
      const stateChanged = before === void 0 || safeSerialize(after) !== before;
      return {
        result,
        // Only carry a patch when the tool actually mutated state — every step's
        // output counts against Inngest's ~4MB-per-run total.
        statePatch: stateChanged ? after : void 0
      };
    });
    if (memoized.statePatch !== void 0) {
      network.state.importData(memoized.statePatch);
    }
    return memoized.result;
  }
  async agentPrompt(input, network) {
    const systemContent = typeof this.system === "string" ? this.system : await this.system({ network });
    const inputContent = typeof input === "object" && input !== null && "content" in input ? input.content : input;
    const userSystemPrompt = typeof input === "object" && input !== null && "systemPrompt" in input ? input.systemPrompt : void 0;
    const messages = [
      {
        type: "text",
        role: "system",
        content: userSystemPrompt ? `${systemContent}

${userSystemPrompt}` : systemContent
      }
    ];
    if (inputContent.length > 0) {
      messages.push({ type: "text", role: "user", content: inputContent });
    }
    if (this.assistant.length > 0) {
      messages.push({
        type: "text",
        role: "assistant",
        content: this.assistant
      });
    }
    return messages;
  }
  // initMCP fetches all tools from the agent's MCP servers, adding them to the tool list.
  // This is all that's necessary in order to enable MCP tool use within agents
  async initMCP() {
    if (!this.mcpServers || this._mcpClients.length >= this.mcpServers.length) {
      return;
    }
    const promises = [];
    for (const server of this.mcpServers) {
      promises.push(this.listMCPTools(server));
    }
    await Promise.all(promises);
  }
  /**
   * listMCPTools lists all available tools for a given MCP server
   */
  async listMCPTools(server) {
    const { JSONSchemaToZod } = await import("@dmitryrechkin/json-schema-to-zod");
    const client = await this.mcpClient(server);
    this._mcpClients.push(client);
    try {
      const results = await client.request(
        { method: "tools/list" },
        import_types5.ListToolsResultSchema
      );
      results.tools.forEach((t) => {
        const name = `${server.name}-${t.name}`;
        let zschema;
        try {
          zschema = JSONSchemaToZod.convert(
            t.inputSchema
          );
        } catch (e) {
          zschema = void 0;
        }
        this.tools.set(name, {
          name,
          description: t.description,
          parameters: zschema,
          mcp: {
            server,
            tool: t
          },
          handler: async (input) => {
            const result = await client.callTool({
              name: t.name,
              arguments: input
            });
            return result.content;
          }
        });
      });
    } catch (e) {
      console.warn("error listing mcp tools", e);
    }
  }
  /**
   * mcpClient creates a new MCP client for the given server.
   */
  async mcpClient(server) {
    const transport = (() => {
      switch (server.transport.type) {
        case "streamable-http":
          return new import_streamableHttp.StreamableHTTPClientTransport(
            new URL(server.transport.url),
            {
              requestInit: server.transport.requestInit,
              authProvider: server.transport.authProvider,
              reconnectionOptions: server.transport.reconnectionOptions,
              sessionId: server.transport.sessionId
            }
          );
        case "sse":
          if (global.EventSource === void 0) {
            global.EventSource = import_eventsource.EventSource;
          }
          return new import_sse.SSEClientTransport(new URL(server.transport.url), {
            eventSourceInit: server.transport.eventSourceInit,
            requestInit: server.transport.requestInit
          });
        case "ws":
          return new import_websocket.WebSocketClientTransport(new URL(server.transport.url));
        case "stdio": {
          const { command, args, env } = server.transport;
          const safeProcessEnv = Object.fromEntries(
            Object.entries(process.env).filter(([, v]) => v !== void 0)
          );
          const finalEnv = __spreadValues(__spreadValues({}, safeProcessEnv), env);
          return new import_stdio.StdioClientTransport({
            command,
            args,
            env: finalEnv
          });
        }
      }
    })();
    const client = new import_client.Client(
      {
        name: this.name,
        // XXX: This version should change.
        version: "1.0.0"
      },
      {
        capabilities: {}
      }
    );
    try {
      await client.connect(transport);
    } catch (e) {
      console.warn("mcp server disconnected", server, e);
    }
    return client;
  }
};
var RoutingAgent = class _RoutingAgent extends Agent {
  constructor(opts) {
    super(opts);
    this.type = "routing";
    this.lifecycles = opts.lifecycle;
  }
  withModel(model) {
    return new _RoutingAgent({
      name: this.name,
      description: this.description,
      system: this.system,
      assistant: this.assistant,
      tools: Array.from(this.tools.values()),
      lifecycle: this.lifecycles,
      model,
      maxIterPerRun: this.maxIterPerRun
    });
  }
};
// Annotate the CommonJS export names for ESM import in node:
0 && (module.exports = {
  Agent,
  AgentMessageChunkSchema,
  AgentResult,
  AgenticModel,
  DEFAULT_CHUNK_SIZE,
  DEFAULT_MAX_CHUNKS_PER_MESSAGE,
  DEFAULT_MAX_ITER_PER_RUN,
  FINAL_APPEND_STEP_ID,
  Network,
  NetworkRun,
  RoutingAgent,
  State,
  StreamingContext,
  createAgent,
  createAgenticModelFromLanguageModel,
  createNetwork,
  createRoutingAgent,
  createState,
  createStepWrapper,
  createTool,
  createToolManifest,
  generateId,
  getDefaultRoutingAgent,
  getInngestFnInput,
  getStepTools,
  incrementalAppendStepId,
  initializeThread,
  isEventType,
  isInngestFn,
  loadThreadFromStorage,
  mapToolChoice,
  messagesToCoreMessages,
  persistResults,
  resultToMessages,
  saveThreadToStorage,
  stringifyError,
  toSerializableResult,
  toolsToAiTools
});
//# sourceMappingURL=index.cjs.map