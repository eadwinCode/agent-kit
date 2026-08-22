import type {
  StreamingAction,
  StreamingState,
  ThreadState,
  ConversationMessage,
  TextUIPart,
  ToolCallUIPart,
  RunStarted,
  PartCreated,
  TextDelta,
  ToolArgsDelta,
  ToolOutputDelta,
  PartCompleted,
  AgentKitEvent,
  ToolManifest,
  ToolResultPayload,
  ReasoningDelta,
  ReasoningUIPart,
  DataDelta,
  DataUIPart,
  ErrorEvent,
  HitlRequested,
  HitlResolved,
  HitlUIPart,
  MessagePart,
  StructuredPartType,
} from "../../types/index.js";

// Safe accessor for typed event.data
function getEventData<T extends Record<string, unknown>>(
  evt: AgentKitEvent
): T | undefined {
  const anyEvt = evt as unknown as { data?: unknown };
  if (!anyEvt || typeof anyEvt !== "object") return undefined;
  const data = (anyEvt as { data?: unknown }).data;
  if (data && typeof data === "object") return data as T;
  return undefined;
}

// Minimal pure reducer wrapper – delegates to current logic via a simple switch
// We start with no-op transitions to establish the hexagonal seam without behavior changes.

export function reduceStreamingState<
  TManifest extends ToolManifest = ToolManifest,
  TState = Record<string, unknown>,
>(
  state: StreamingState<TManifest, TState>,
  action: StreamingAction<TManifest, TState>,
  _debug?: boolean
): StreamingState<TManifest, TState> {
  void _debug;
  switch (action.type) {
    case "CONNECTION_STATE_CHANGED": {
      // Treat known values from the realtime hook (e.g. 'active', 'connecting', numbers) robustly
      const raw = action.state as unknown;
      const text = String(raw).toLowerCase();
      const isConnected =
        text === "active" ||
        text === "open" ||
        text === "connected" ||
        text === "ready" ||
        raw === 1;
      return { ...state, isConnected };
    }

    case "SET_CURRENT_THREAD": {
      if (!action.threadId || action.threadId === state.currentThreadId)
        return state;
      return {
        ...state,
        currentThreadId: action.threadId,
      };
    }

    case "REALTIME_MESSAGES_RECEIVED": {
      if (!Array.isArray(action.messages) || action.messages.length === 0)
        return state;
      let next: StreamingState<TManifest, TState> = state;
      for (const evt of action.messages) {
        const eventName = evt.event;
        const dataForThread = getEventData<{ threadId?: string }>(evt);
        const threadId =
          (dataForThread && typeof dataForThread.threadId === "string"
            ? dataForThread.threadId
            : undefined) || next.currentThreadId;
        if (!threadId) continue;

        // Ensure thread exists
        const thread = ensureThread<TManifest, TState>(next, threadId);

        // Deduplicate exact realtime deliveries even after they have drained from
        // the sequence buffer. Some transports replay the last item on reconnect.
        const eventIdentity =
          typeof evt.id === "string" && evt.id
            ? evt.id
            : `${evt.sequenceNumber}:${evt.event}`;
        thread.processedEventIds ??= new Set<string>();
        if (thread.processedEventIds.has(eventIdentity)) {
          if (_debug)
            console.log("[ordering] dedup: already processed", {
              threadId,
              eventIdentity,
              event: evt.event,
            });
          continue;
        }
        thread.processedEventIds.add(eventIdentity);

        // Per-thread dedup by sequenceNumber
        if (thread.eventBuffer.has(evt.sequenceNumber)) {
          if (_debug)
            console.log("[ordering] dedup: already buffered", {
              threadId,
              seq: evt.sequenceNumber,
              event: evt.event,
            });
          continue;
        }

        // Opportunistic immediate apply for pre-epoch or duplicate/lower seq events
        if (
          eventName !== "run.started" &&
          typeof evt.sequenceNumber === "number" &&
          typeof thread.nextExpectedSequence === "number" &&
          evt.sequenceNumber <= thread.nextExpectedSequence - 1
        ) {
          const updatedThread = applyEvent<TManifest, TState>(thread, evt);
          next = writeThread<TManifest, TState>(next, threadId, updatedThread);
          // Mark background flag if needed
          if (threadId !== next.currentThreadId) {
            const t2 = ensureThread<TManifest, TState>(next, threadId);
            if (!t2.hasNewMessages) {
              next = writeThread<TManifest, TState>(next, threadId, {
                ...t2,
                hasNewMessages: true,
              } as ThreadState<TManifest, TState>);
            }
          }
          continue;
        }

        // Buffer the event (ordered path)
        thread.eventBuffer.set(evt.sequenceNumber, evt);

        // Epoch handling for run.started
        if (eventName === "run.started") {
          const d =
            getEventData<{
              scope?: string;
              parentRunId?: string;
              name?: string;
            }>(evt) || {};
          const scope = typeof d.scope === "string" ? d.scope : undefined;
          const parentRunId =
            typeof d.parentRunId === "string" ? d.parentRunId : undefined;

          // Epoch boundary only for network runs or standalone agent runs (no parentRunId)
          const isEpochStart =
            scope === "network" || (scope === "agent" && !parentRunId);

          if (isEpochStart) {
            // Reset epoch: set next expected to s + 1 and purge older entries
            const s = evt.sequenceNumber;
            thread.nextExpectedSequence = s + 1;
            thread.runActive = true;
            thread.agentStatus = "submitted";
            thread.currentAgent =
              typeof d.name === "string" ? d.name : thread.currentAgent;
            thread.lastActivity = new Date();
            for (const key of Array.from(thread.eventBuffer.keys())) {
              if (key <= s) thread.eventBuffer.delete(key);
            }
            if (_debug)
              console.log("[ordering] epoch-reset", {
                threadId,
                scope,
                parentRunId,
                seq: s,
                nextExpected: thread.nextExpectedSequence,
              });
          } else {
            // Agent sub-run within a network epoch: update thinking status only
            thread.runActive = true;
            thread.agentStatus = "submitted";
            thread.currentAgent =
              typeof d.name === "string" ? d.name : thread.currentAgent;
            thread.lastActivity = new Date();
          }

          // run.started can establish a new epoch without leaving an event for
          // drainBuffer to apply. Persist the lifecycle mutation immutably so
          // StreamingEngine observes a new root state and notifies React. The
          // sending tab already has an optimistic submitted state, but sibling
          // tabs depend on this notification to render the same live status.
          next = writeThread<TManifest, TState>(next, threadId, {
            ...thread,
          } as ThreadState<TManifest, TState>);
        }

        // Drain any consecutive events now available
        next = drainBuffer(next, threadId, _debug);

        // Mark non-current thread as having unseen messages when updated in background
        try {
          if (threadId !== next.currentThreadId) {
            const t = ensureThread<TManifest, TState>(next, threadId);
            if (!t.hasNewMessages) {
              next = writeThread<TManifest, TState>(next, threadId, {
                ...t,
                hasNewMessages: true,
              } as ThreadState<TManifest, TState>);
            }
          }
        } catch {
          /* noop */
        }
      }
      return next;
    }

    // Optimistic user message added before send
    case "MESSAGE_SENT": {
      const threadId = action.threadId;
      const messageId = action.messageId;
      const message = action.message;
      const clientState = action.clientState;
      if (!threadId || !messageId || typeof message !== "string") return state;

      const existing = state.threads[threadId]?.messages || [];
      const already = existing.some((m) => m.id === messageId);
      if (already) return state;

      const userMessage: ConversationMessage<TManifest, TState> = {
        id: messageId,
        role: "user",
        parts: [
          {
            type: "text",
            id: `text-${messageId}`,
            content: message,
            status: "complete",
          } as TextUIPart,
        ],
        timestamp: new Date(),
        status: "sending",
        clientState,
      } as ConversationMessage<TManifest, TState>;

      const base = ensureThread<TManifest, TState>(state, threadId);
      const updated: ThreadState<TManifest, TState> = {
        ...base,
        messages: [...existing, userMessage],
        agentStatus: "submitted",
        lastActivity: new Date(),
      } as ThreadState<TManifest, TState>;
      return writeThread<TManifest, TState>(state, threadId, updated);
    }

    case "MESSAGE_SEND_SUCCESS": {
      const threadId = action.threadId;
      const messageId = action.messageId;
      if (!threadId || !messageId) return state;
      const thread = ensureThread<TManifest, TState>(state, threadId);
      const updated: ThreadState<TManifest, TState> = {
        ...thread,
        messages: (thread.messages || []).map((m) =>
          m.id === messageId
            ? ({ ...m, status: "sent" } as ConversationMessage<
                TManifest,
                TState
              >)
            : m
        ),
      } as ThreadState<TManifest, TState>;
      return writeThread<TManifest, TState>(state, threadId, updated);
    }

    case "MESSAGE_SEND_FAILED": {
      const threadId = action.threadId;
      const messageId = action.messageId;
      const error = action.error as string | undefined;
      if (!threadId || !messageId) return state;
      const thread = ensureThread<TManifest, TState>(state, threadId);
      const updated: ThreadState<TManifest, TState> = {
        ...thread,
        messages: (thread.messages || []).map((m) =>
          m.id === messageId
            ? ({ ...m, status: "failed" } as ConversationMessage<
                TManifest,
                TState
              >)
            : m
        ),
        agentStatus: "error",
        error: error
          ? { message: error, timestamp: new Date(), recoverable: true }
          : thread.error,
      } as ThreadState<TManifest, TState>;
      return writeThread<TManifest, TState>(state, threadId, updated);
    }

    case "REPLACE_THREAD_MESSAGES": {
      const threadId = action.threadId;
      const messages = action.messages;
      if (!threadId || !Array.isArray(messages)) return state;
      const thread = ensureThread<TManifest, TState>(state, threadId);
      const updated: ThreadState<TManifest, TState> = {
        ...thread,
        messages,
        agentStatus: "ready",
        lastActivity: new Date(),
        error: undefined,
        historyLoaded: true,
      } as ThreadState<TManifest, TState>;
      return writeThread<TManifest, TState>(state, threadId, updated);
    }

    case "CLEAR_THREAD_MESSAGES": {
      const threadId = action.threadId;
      if (!threadId) return state;
      const thread = ensureThread<TManifest, TState>(state, threadId);
      const updated: ThreadState<TManifest, TState> = {
        ...thread,
        messages: [],
        eventBuffer: new Map(),
        processedEventIds: new Set(),
        nextExpectedSequence: 0,
        agentStatus: "ready",
        error: undefined,
      } as ThreadState<TManifest, TState>;
      return writeThread<TManifest, TState>(state, threadId, updated);
    }

    case "CLEAR_THREAD_ERROR": {
      const threadId = action.threadId;
      if (!threadId) return state;
      const thread = ensureThread<TManifest, TState>(state, threadId);
      const updated: ThreadState<TManifest, TState> = {
        ...thread,
        error: undefined,
      } as ThreadState<TManifest, TState>;
      return writeThread<TManifest, TState>(state, threadId, updated);
    }

    case "MARK_THREAD_VIEWED": {
      const threadId = action.threadId;
      if (!threadId) return state;
      const thread = ensureThread<TManifest, TState>(state, threadId);
      if (!thread.hasNewMessages) return state;
      const updated: ThreadState<TManifest, TState> = {
        ...thread,
        hasNewMessages: false,
      } as ThreadState<TManifest, TState>;
      return writeThread<TManifest, TState>(state, threadId, updated);
    }

    case "CREATE_THREAD": {
      const threadId = action.threadId;
      if (!threadId) return state;
      if (state.threads[threadId]) return state;
      const created = ensureThread<TManifest, TState>(state, threadId);
      return writeThread<TManifest, TState>(state, threadId, created);
    }

    case "REMOVE_THREAD": {
      const threadId = action.threadId;
      if (!threadId) return state;
      if (!state.threads[threadId]) return state;
      const rest = { ...state.threads } as Record<
        string,
        ThreadState<TManifest, TState>
      >;
      delete rest[threadId];
      return {
        ...state,
        threads: rest,
        currentThreadId:
          state.currentThreadId === threadId
            ? Object.keys(rest)[0] || ""
            : state.currentThreadId,
      } as StreamingState<TManifest, TState>;
    }

    default:
      return state;
  }
}

// === Buffer draining and ordered application ===

function drainBuffer<
  TManifest extends ToolManifest = ToolManifest,
  TState = Record<string, unknown>,
>(
  state: StreamingState<TManifest, TState>,
  threadId: string,
  debug?: boolean
): StreamingState<TManifest, TState> {
  let next = state;
  let thread = ensureThread<TManifest, TState>(next, threadId);

  // Guard: Avoid infinite loops on malformed state
  if (typeof thread.nextExpectedSequence !== "number") {
    thread.nextExpectedSequence = 0;
  }

  // Back-compat fallback: if no event for current expectation and no epoch set,
  // align to the smallest buffered sequence to allow processing to begin.
  // Do NOT align if the smallest buffered event is an agent sub-run run.started
  // (scope === 'agent' with parentRunId present). That would jump past gaps.
  if (
    !thread.eventBuffer.has(thread.nextExpectedSequence) &&
    thread.eventBuffer.size > 0 &&
    thread.runActive !== true
  ) {
    try {
      const keys = Array.from(thread.eventBuffer.keys());
      const minSeq = Math.min(...keys);
      const minEvt = thread.eventBuffer.get(minSeq);
      const isAgentSubRun = Boolean(
        minEvt &&
          minEvt.event === "run.started" &&
          (() => {
            const data = getEventData<{ scope?: string; parentRunId?: string }>(
              minEvt as AgentKitEvent<TManifest>
            );
            return (
              data?.scope === "agent" && typeof data?.parentRunId === "string"
            );
          })()
      );
      // Only run.started and part.created may anchor a thread that has not
      // started yet. A bare delta arriving first is a PRE-EPOCH straggler
      // from a run this client is about to be told about, and aligning to it
      // would skip the beginning of the real run.
      //
      // A client joining mid-run does not rely on this path: hydration tails
      // from the start of the active epoch, so its replay always begins with
      // run.started. See hydrateAgentSession.
      const isAlignableEvent = Boolean(
        minEvt &&
          (minEvt.event === "run.started" || minEvt.event === "part.created")
      );
      if (
        thread.nextExpectedSequence < minSeq &&
        !isAgentSubRun &&
        isAlignableEvent
      ) {
        thread.nextExpectedSequence = minSeq;
        if (debug)
          console.log("[ordering] align-nextExpected", {
            threadId,
            alignedTo: minSeq,
          });
      }
    } catch {
      /* noop */
    }
  } else if (
    // Log gaps during active runs for diagnostics
    !thread.eventBuffer.has(thread.nextExpectedSequence) &&
    thread.eventBuffer.size > 0 &&
    thread.runActive === true
  ) {
    try {
      const keys = Array.from(thread.eventBuffer.keys()).sort((a, b) => a - b);
      const minSeq = keys[0];
      const maxSeq = keys[keys.length - 1];
      if (debug)
        console.warn("[UA-DIAG] reducer-gap-stalled", {
          threadId,
          nextExpected: thread.nextExpectedSequence,
          minBuffered: minSeq,
          maxBuffered: maxSeq,
          size: thread.eventBuffer.size,
        });
    } catch {
      /* noop */
    }
  }

  let progressed = false;
  while (thread.eventBuffer.has(thread.nextExpectedSequence)) {
    const evt = thread.eventBuffer.get(thread.nextExpectedSequence)!;
    thread.eventBuffer.delete(thread.nextExpectedSequence);
    if (debug)
      console.log("[UA-DIAG] reducer-apply", {
        threadId,
        event: evt.event,
        seq: thread.nextExpectedSequence,
      });
    const updatedThread = applyEvent<TManifest, TState>(thread, evt);
    // Write thread updates after each event application
    next = writeThread<TManifest, TState>(next, threadId, updatedThread);
    thread = ensureThread<TManifest, TState>(next, threadId);
    thread.nextExpectedSequence += 1;
    progressed = true;
    if (
      debug &&
      (evt.event === "run.completed" || evt.event === "stream.ended")
    ) {
      console.log("[UA-DIAG] reducer-terminal-applied", {
        threadId,
        event: evt.event,
        appliedSeq: thread.nextExpectedSequence - 1,
      });
    }
    if (debug)
      console.log("[ordering] drain", {
        threadId,
        appliedSeq: thread.nextExpectedSequence - 1,
      });
  }

  // If no progress, return unchanged reference
  return progressed ? next : state;
}

function applyEvent<
  TManifest extends ToolManifest = ToolManifest,
  TState = Record<string, unknown>,
>(
  thread: ThreadState<TManifest, TState>,
  evt: AgentKitEvent<TManifest>
): ThreadState<TManifest, TState> {
  switch (evt.event) {
    case "user.message": {
      // The server-accepted user turn.
      //
      // The tab that sent the message already rendered it optimistically, but
      // every OTHER authorized client — a second tab, a phone, a browser that
      // reconnected mid-turn — has never seen it. Without this the assistant
      // appears to answer a question nobody asked.
      //
      // The server's id is the same one canonical history persists, so the
      // initiating tab's optimistic message converges onto it rather than
      // being duplicated beside it.
      const d =
        getEventData<{
          messageId?: string;
          content?: string;
          role?: string;
          timestamp?: string;
        }>(evt) || {};
      const messageId = typeof d.messageId === "string" ? d.messageId : "";
      const content = typeof d.content === "string" ? d.content : "";
      if (!messageId) return thread;

      const existing = thread.messages || [];
      const alreadyThere = existing.some((m) => m.id === messageId);
      if (alreadyThere) {
        return {
          ...thread,
          messages: existing.map((m) =>
            m.id === messageId
              ? ({ ...m, status: "sent" } as ConversationMessage<
                  TManifest,
                  TState
                >)
              : m
          ),
        } as ThreadState<TManifest, TState>;
      }

      const timestamp =
        typeof d.timestamp === "string" &&
        !Number.isNaN(Date.parse(d.timestamp))
          ? new Date(d.timestamp)
          : new Date();

      const userMessage: ConversationMessage<TManifest, TState> = {
        id: messageId,
        role: "user",
        parts: [
          {
            type: "text",
            id: `text-${messageId}`,
            content,
            status: "complete",
          } as TextUIPart,
        ],
        timestamp,
        status: "sent",
      } as ConversationMessage<TManifest, TState>;

      return {
        ...thread,
        messages: [...existing, userMessage],
        lastActivity: new Date(),
      } as ThreadState<TManifest, TState>;
    }
    case "run.started": {
      const d =
        getEventData<RunStarted["data"]>(evt) || ({} as RunStarted["data"]);
      return {
        ...thread,
        agentStatus: "submitted",
        runActive: true,
        currentAgent:
          typeof d?.name === "string" ? d.name : thread.currentAgent,
        lastActivity: new Date(),
      } as ThreadState<TManifest, TState>;
    }
    case "part.created": {
      return applyPartCreated<TManifest, TState>(
        thread,
        (evt as PartCreated).data
      );
    }
    case "text.delta": {
      return applyTextDelta<TManifest, TState>(thread, (evt as TextDelta).data);
    }
    case "reasoning.delta": {
      return applyReasoningDelta<TManifest, TState>(
        thread,
        (evt as ReasoningDelta).data
      );
    }
    case "data.delta": {
      return applyDataDelta<TManifest, TState>(thread, (evt as DataDelta).data);
    }
    case "tool_call.arguments.delta": {
      return applyToolArgumentsDelta<TManifest, TState>(
        thread,
        (evt as ToolArgsDelta).data
      );
    }
    case "tool_call.output.delta": {
      return applyToolOutputDelta<TManifest, TState>(
        thread,
        (evt as ToolOutputDelta).data
      );
    }
    case "part.completed": {
      return applyPartCompleted<TManifest, TState>(
        thread,
        (evt as PartCompleted).data
      );
    }
    case "error": {
      return applyErrorEvent<TManifest, TState>(
        thread,
        (evt as ErrorEvent).data,
        evt.id
      );
    }
    case "hitl.requested": {
      return applyHitlRequested<TManifest, TState>(
        thread,
        (evt as HitlRequested).data,
        evt.id
      );
    }
    case "hitl.resolved": {
      return applyHitlResolved<TManifest, TState>(
        thread,
        (evt as HitlResolved).data
      );
    }
    case "run.completed": {
      // Do not mark ready on run.completed; only finalize tool outputs.
      // We will transition to "ready" exclusively on stream.ended.
      const finalized = finalizeToolsWithOutput<TManifest, TState>(thread);
      return {
        ...finalized,
        lastActivity: new Date(),
      } as ThreadState<TManifest, TState>;
    }
    case "stream.ended": {
      const finalized = finalizeToolsWithOutput<TManifest, TState>(thread);
      return {
        ...finalized,
        agentStatus: finalized.error ? "error" : "ready",
        runActive: false,
        lastActivity: new Date(),
      } as ThreadState<TManifest, TState>;
    }
    default:
      return thread;
  }
}

function ensureThread<
  TManifest extends ToolManifest = ToolManifest,
  TState = Record<string, unknown>,
>(
  state: StreamingState<TManifest, TState>,
  threadId: string
): ThreadState<TManifest, TState> {
  const existing = state.threads[threadId];
  if (existing) return existing;
  const created: ThreadState<TManifest, TState> = {
    messages: [],
    eventBuffer: new Map(),
    nextExpectedSequence: 0,
    agentStatus: "ready",
    hasNewMessages: false,
    lastActivity: new Date(),
    historyLoaded: false,
    runActive: false, // Default to inactive
    processedEventIds: new Set(),
  } as ThreadState<TManifest, TState>;
  state.threads[threadId] = created;
  return created;
}

function writeThread<
  TManifest extends ToolManifest = ToolManifest,
  TState = Record<string, unknown>,
>(
  state: StreamingState<TManifest, TState>,
  threadId: string,
  updated: ThreadState<TManifest, TState>
): StreamingState<TManifest, TState> {
  if (state.threads[threadId] === updated) return state;
  return {
    ...state,
    threads: {
      ...state.threads,
      [threadId]: updated,
    },
  };
}

// === Message assembly helpers (minimal incremental support) ===

function getOrCreateAssistantMessage<
  TManifest extends ToolManifest = ToolManifest,
  TState = Record<string, unknown>,
>(
  messages: ConversationMessage<TManifest, TState>[],
  data: { messageId?: string }
): {
  list: ConversationMessage<TManifest, TState>[];
  msg: ConversationMessage<TManifest, TState>;
} {
  const messageIdRaw = data?.messageId;
  const messageId: string = messageIdRaw || `msg-${Date.now()}`;
  let msg = messages.find((m) => m.id === messageId && m.role === "assistant");
  if (msg) return { list: messages, msg };
  msg = {
    id: messageId,
    role: "assistant",
    parts: [],
    timestamp: new Date(),
    status: "sent",
  } as ConversationMessage<TManifest, TState>;
  return { list: [...messages, msg], msg };
}

function ensureTextPart<
  TManifest extends ToolManifest = ToolManifest,
  TState = Record<string, unknown>,
>(message: ConversationMessage<TManifest, TState>, partId: string): TextUIPart {
  let part = message.parts.find((p) => p.type === "text" && p.id === partId) as
    | TextUIPart
    | undefined;
  if (!part) {
    part = {
      type: "text",
      id: partId,
      content: "",
      status: "streaming",
    } as TextUIPart;
    message.parts = [...message.parts, part];
  }
  return part;
}

function ensureReasoningPart<
  TManifest extends ToolManifest = ToolManifest,
  TState = Record<string, unknown>,
>(
  message: ConversationMessage<TManifest, TState>,
  partId: string,
  metadata?: Record<string, unknown>
): ReasoningUIPart {
  let part = message.parts.find(
    (p) => p.type === "reasoning" && p.id === partId
  ) as ReasoningUIPart | undefined;
  if (!part) {
    part = {
      type: "reasoning",
      id: partId,
      agentName: stringValue(metadata?.agentName),
      content: "",
      status: "streaming",
    } as ReasoningUIPart;
    message.parts = [...message.parts, part];
  }
  return part;
}

function asRecord(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function stringValue(value: unknown, fallback = ""): string {
  return typeof value === "string" ? value : fallback;
}

function statusValue(
  value: unknown,
  fallback:
    | "started"
    | "thinking"
    | "calling-tool"
    | "responding"
    | "completed"
    | "error" = "started"
) {
  switch (value) {
    case "started":
    case "thinking":
    case "calling-tool":
    case "responding":
    case "completed":
    case "error":
      return value;
    default:
      return fallback;
  }
}

function hitlStatusValue(
  value: unknown,
  fallback: HitlUIPart["status"] = "pending"
): HitlUIPart["status"] {
  switch (value) {
    case "pending":
    case "approved":
    case "denied":
    case "expired":
      return value;
    default:
      return fallback;
  }
}

function ensureDataPart<
  TManifest extends ToolManifest = ToolManifest,
  TState = Record<string, unknown>,
>(
  message: ConversationMessage<TManifest, TState>,
  partId: string,
  metadata?: Record<string, unknown>
): DataUIPart {
  let part = message.parts.find(
    (candidate) => candidate.type === "data" && candidate.id === partId
  ) as DataUIPart | undefined;
  if (!part) {
    part = {
      type: "data",
      id: partId,
      name: stringValue(
        metadata?.dataType,
        stringValue(metadata?.name, partId)
      ),
      data: undefined,
      ui: metadata?.ui,
    };
    message.parts = [...message.parts, part];
  }
  return part;
}

function structuredPart<TManifest extends ToolManifest = ToolManifest>(
  type: StructuredPartType,
  partId: string,
  value: unknown,
  metadata?: Record<string, unknown>
): MessagePart<TManifest> | undefined {
  const content = asRecord(value);
  switch (type) {
    case "data":
      return {
        type,
        id: partId,
        name: stringValue(
          metadata?.dataType,
          stringValue(metadata?.name, partId)
        ),
        data: value,
        ui: metadata?.ui,
      };
    case "status":
      return {
        type,
        id: partId,
        status: statusValue(content.status ?? metadata?.status),
        agentId: stringValue(content.agentId ?? metadata?.agentId) || undefined,
        message: stringValue(content.message ?? metadata?.message) || undefined,
      };
    case "file":
      return {
        type,
        id: partId,
        url: stringValue(content.url),
        mediaType: stringValue(content.mediaType, "application/octet-stream"),
        title: stringValue(content.title) || undefined,
        size: typeof content.size === "number" ? content.size : undefined,
      };
    case "source":
      return {
        type,
        id: partId,
        subtype: content.subtype === "document" ? "document" : "url",
        url: stringValue(content.url) || undefined,
        title: stringValue(content.title, "Source"),
        mediaType: stringValue(content.mediaType) || undefined,
        excerpt: stringValue(content.excerpt) || undefined,
      };
    case "error":
      return {
        type,
        id: partId,
        error: stringValue(
          content.error,
          stringValue(content.message, "Agent run failed")
        ),
        agentId: stringValue(content.agentId) || undefined,
        recoverable:
          typeof content.recoverable === "boolean"
            ? content.recoverable
            : undefined,
      };
    case "hitl": {
      const rawMetadata = asRecord(content.metadata);
      const riskLevel = rawMetadata.riskLevel;
      return {
        type,
        id: partId,
        toolCalls: Array.isArray(content.toolCalls)
          ? (content.toolCalls as HitlUIPart["toolCalls"])
          : [],
        status: hitlStatusValue(content.status),
        expiresAt: stringValue(content.expiresAt) || undefined,
        resolvedBy: stringValue(content.resolvedBy) || undefined,
        resolvedAt: stringValue(content.resolvedAt) || undefined,
        metadata: {
          reason: stringValue(rawMetadata.reason) || undefined,
          riskLevel:
            riskLevel === "low" ||
            riskLevel === "medium" ||
            riskLevel === "high"
              ? riskLevel
              : undefined,
        },
      };
    }
    default:
      return undefined;
  }
}

function applyPartCreated<
  TManifest extends ToolManifest = ToolManifest,
  TState = Record<string, unknown>,
>(
  thread: ThreadState<TManifest, TState>,
  data: PartCreated["data"]
): ThreadState<TManifest, TState> {
  if (!data) return thread;
  const type = data?.type;
  const messageId = data?.messageId;
  const partId = data?.partId;
  if (!type || !messageId || !partId) return { ...thread };
  const { list, msg } = getOrCreateAssistantMessage<TManifest, TState>(
    thread.messages,
    data
  );
  if (type === "text") {
    ensureTextPart<TManifest, TState>(msg, partId);
  } else if (type === "reasoning") {
    ensureReasoningPart<TManifest, TState>(msg, partId, data.metadata);
  } else if (type === "tool-call") {
    const tool: ToolCallUIPart<TManifest> = {
      type: "tool-call",
      toolCallId: partId,
      toolName: (data?.metadata?.toolName || "") as keyof TManifest & string,
      state: "input-streaming",
      input: {} as ToolCallUIPart<TManifest>["input"],
      output: undefined,
    } as ToolCallUIPart<TManifest>;
    msg.parts = [...msg.parts, tool];
  } else if (type === "data") {
    const part = ensureDataPart<TManifest, TState>(msg, partId, data.metadata);
    if (data.content !== undefined) part.data = data.content;
  } else {
    const part = structuredPart<TManifest>(
      type,
      partId,
      data.content,
      data.metadata
    );
    if (part) msg.parts = [...msg.parts, part];
  }
  return {
    ...thread,
    messages: list,
    agentStatus: "streaming",
    lastActivity: new Date(),
  };
}

function applyReasoningDelta<
  TManifest extends ToolManifest = ToolManifest,
  TState = Record<string, unknown>,
>(
  thread: ThreadState<TManifest, TState>,
  data: ReasoningDelta["data"]
): ThreadState<TManifest, TState> {
  if (!data) return thread;
  const partId = data?.partId;
  const messageId = data?.messageId;
  const delta = data?.delta;
  if (!partId || !messageId || typeof delta !== "string") return { ...thread };
  const { list, msg } = getOrCreateAssistantMessage<TManifest, TState>(
    thread.messages,
    data
  );
  const part = ensureReasoningPart<TManifest, TState>(
    msg,
    partId,
    data.metadata
  );
  part.content = (part.content || "") + delta;
  part.status = "streaming";
  return {
    ...thread,
    messages: list,
    agentStatus: "streaming",
    lastActivity: new Date(),
  };
}

function applyDataDelta<
  TManifest extends ToolManifest = ToolManifest,
  TState = Record<string, unknown>,
>(
  thread: ThreadState<TManifest, TState>,
  data: DataDelta["data"]
): ThreadState<TManifest, TState> {
  if (!data) return thread;
  const partId = data.partId;
  const messageId = data.messageId;
  if (
    !partId ||
    !messageId ||
    !Object.prototype.hasOwnProperty.call(data, "delta")
  ) {
    return { ...thread };
  }
  const { list, msg } = getOrCreateAssistantMessage<TManifest, TState>(
    thread.messages,
    data
  );
  const part = ensureDataPart<TManifest, TState>(msg, partId, data.metadata);
  part.data = data.delta;
  return {
    ...thread,
    messages: list,
    agentStatus: "streaming",
    lastActivity: new Date(),
  };
}

function applyTextDelta<
  TManifest extends ToolManifest = ToolManifest,
  TState = Record<string, unknown>,
>(
  thread: ThreadState<TManifest, TState>,
  data: TextDelta["data"]
): ThreadState<TManifest, TState> {
  if (!data) return thread;
  const partId = data?.partId;
  const messageId = data?.messageId;
  const delta = data?.delta;
  if (!partId || !messageId || typeof delta !== "string") return { ...thread };
  const { list, msg } = getOrCreateAssistantMessage<TManifest, TState>(
    thread.messages,
    data
  );
  const part = ensureTextPart<TManifest, TState>(msg, partId);
  part.content = (part.content || "") + delta;
  part.status = "streaming";
  return {
    ...thread,
    messages: list,
    agentStatus: "streaming",
    lastActivity: new Date(),
  };
}

function applyPartCompleted<
  TManifest extends ToolManifest = ToolManifest,
  TState = Record<string, unknown>,
>(
  thread: ThreadState<TManifest, TState>,
  data: PartCompleted["data"]
): ThreadState<TManifest, TState> {
  if (!data) return thread;
  const messageId = data?.messageId;
  const partId = data?.partId;
  const type = data?.type;
  const finalContent = data?.finalContent;
  if (!messageId || !partId) return { ...thread };
  const { list, msg } = getOrCreateAssistantMessage<TManifest, TState>(
    thread.messages,
    data
  );

  if (type === "text") {
    const text = ensureTextPart<TManifest, TState>(msg, partId);
    if (typeof finalContent === "string") text.content = finalContent;
    text.status = "complete";
    return { ...thread, messages: list, lastActivity: new Date() };
  }

  if (type === "reasoning") {
    const part = ensureReasoningPart<TManifest, TState>(
      msg,
      partId,
      data.metadata
    );
    if (typeof finalContent === "string") part.content = finalContent;
    part.status = "complete";
    return { ...thread, messages: list, lastActivity: new Date() };
  }

  if (type === "data") {
    const part = ensureDataPart<TManifest, TState>(msg, partId, data.metadata);
    if (finalContent !== undefined) part.data = finalContent;
    return { ...thread, messages: list, lastActivity: new Date() };
  }

  if (
    type === "status" ||
    type === "file" ||
    type === "source" ||
    type === "error" ||
    type === "hitl"
  ) {
    const part = structuredPart<TManifest>(
      type,
      partId,
      finalContent,
      data.metadata
    );
    if (part) {
      const existing = msg.parts.findIndex(
        (candidate) => candidate.type === type && candidate.id === partId
      );
      msg.parts =
        existing >= 0
          ? msg.parts.map((candidate, index) =>
              index === existing ? part : candidate
            )
          : [...msg.parts, part];
    }
    return { ...thread, messages: list, lastActivity: new Date() };
  }

  if (type === "tool-call") {
    let tool = msg.parts.find(
      (p) => p.type === "tool-call" && p.toolCallId === partId
    ) as ToolCallUIPart<TManifest> | undefined;
    if (!tool) {
      const rawData = asRecord(data);
      const rawMetadata = asRecord(rawData.metadata);
      tool = {
        type: "tool-call",
        toolCallId: partId,
        toolName: stringValue(
          rawData.toolName,
          stringValue(rawMetadata.toolName)
        ) as keyof TManifest & string,
        state: "input-available",
        input: {} as ToolCallUIPart<TManifest>["input"],
      } as ToolCallUIPart<TManifest>;
      msg.parts = [...msg.parts, tool];
    }
    // Prefer structured final input if provided
    if (finalContent !== undefined) {
      try {
        const parsed =
          typeof finalContent === "string"
            ? (JSON.parse(finalContent) as unknown)
            : (finalContent as unknown);
        tool.input = parsed as ToolCallUIPart<TManifest>["input"];
      } catch {
        tool.input = finalContent as ToolCallUIPart<TManifest>["input"];
      }
    }
    tool.state = "input-available";
    return {
      ...thread,
      messages: list,
      agentStatus: "streaming",
      lastActivity: new Date(),
    };
  }

  if (type === "tool-output") {
    // Prefer id match; if not, try a reasonable fallback to the latest in-flight tool
    let tool = msg.parts.find(
      (p) => p.type === "tool-call" && p.toolCallId === partId
    ) as ToolCallUIPart<TManifest> | undefined;
    if (!tool) {
      tool = findFallbackToolPartForCompletion(msg);
    }
    if (tool) {
      // Finalize output
      tool.output = normalizeToolResultPayload(
        tool.toolName,
        finalContent
      ) as ToolCallUIPart<TManifest>["output"];
      tool.state = "output-available";
      return {
        ...thread,
        messages: list,
        agentStatus: "streaming",
        lastActivity: new Date(),
      };
    }
  }

  // Fallback: if unknown type, keep thread unchanged except activity
  return { ...thread, messages: list, lastActivity: new Date() };
}

function applyErrorEvent<
  TManifest extends ToolManifest = ToolManifest,
  TState = Record<string, unknown>,
>(
  thread: ThreadState<TManifest, TState>,
  data: ErrorEvent["data"],
  eventId?: string
): ThreadState<TManifest, TState> {
  const payload = asRecord(data);
  const message = stringValue(
    payload.error,
    stringValue(payload.message, "Agent run failed")
  );
  let messages = thread.messages;
  const messageId = stringValue(payload.messageId);

  if (messageId) {
    const result = getOrCreateAssistantMessage<TManifest, TState>(
      messages,
      payload
    );
    const partId = stringValue(
      payload.partId,
      stringValue(eventId, `error-${messageId}`)
    );
    const part = structuredPart<TManifest>(
      "error",
      partId,
      { ...payload, error: message },
      asRecord(payload.metadata)
    );
    const existing = result.msg.parts.findIndex(
      (candidate) => candidate.type === "error" && candidate.id === partId
    );
    if (part) {
      result.msg.parts =
        existing >= 0
          ? result.msg.parts.map((candidate, index) =>
              index === existing ? part : candidate
            )
          : [...result.msg.parts, part];
    }
    messages = result.list;
  }

  return {
    ...thread,
    messages,
    agentStatus: "error",
    error: {
      message,
      recoverable:
        typeof payload.recoverable === "boolean" ? payload.recoverable : false,
      timestamp: new Date(),
    },
    lastActivity: new Date(),
  };
}

function applyHitlRequested<
  TManifest extends ToolManifest = ToolManifest,
  TState = Record<string, unknown>,
>(
  thread: ThreadState<TManifest, TState>,
  data: HitlRequested["data"],
  eventId?: string
): ThreadState<TManifest, TState> {
  const payload = asRecord(data);
  const messageId = stringValue(payload.messageId);
  if (!messageId) return thread;
  const { list, msg } = getOrCreateAssistantMessage<TManifest, TState>(
    thread.messages,
    payload
  );
  const partId = stringValue(
    payload.partId,
    stringValue(payload.requestId, stringValue(eventId))
  );
  if (!partId) return thread;
  const part = structuredPart<TManifest>(
    "hitl",
    partId,
    { ...payload, status: "pending" },
    asRecord(payload.metadata)
  );
  if (part) {
    msg.parts = [
      ...msg.parts.filter(
        (candidate) => !(candidate.type === "hitl" && candidate.id === partId)
      ),
      part,
    ];
  }
  return {
    ...thread,
    messages: list,
    agentStatus: "streaming",
    lastActivity: new Date(),
  };
}

function applyHitlResolved<
  TManifest extends ToolManifest = ToolManifest,
  TState = Record<string, unknown>,
>(
  thread: ThreadState<TManifest, TState>,
  data: HitlResolved["data"]
): ThreadState<TManifest, TState> {
  const payload = asRecord(data);
  const partId = stringValue(payload.partId, stringValue(payload.requestId));
  if (!partId) return thread;

  let changed = false;
  const messages = thread.messages.map((message) => {
    let messageChanged = false;
    const parts = message.parts.map((part) => {
      if (part.type !== "hitl" || part.id !== partId) return part;
      changed = true;
      messageChanged = true;
      return {
        ...part,
        status: hitlStatusValue(
          payload.status,
          payload.approved === true
            ? "approved"
            : payload.approved === false
              ? "denied"
              : part.status
        ),
        resolvedBy: stringValue(payload.resolvedBy) || part.resolvedBy,
        resolvedAt: stringValue(payload.resolvedAt) || part.resolvedAt,
      } satisfies HitlUIPart;
    });
    return messageChanged ? { ...message, parts } : message;
  });

  return changed
    ? {
        ...thread,
        messages,
        agentStatus: "streaming",
        runActive: true,
        lastActivity: new Date(),
      }
    : thread;
}

function applyToolArgumentsDelta<
  TManifest extends ToolManifest = ToolManifest,
  TState = Record<string, unknown>,
>(
  thread: ThreadState<TManifest, TState>,
  data: ToolArgsDelta["data"]
): ThreadState<TManifest, TState> {
  if (!data) return thread;
  const partId = data?.partId;
  const messageId = data?.messageId;
  const delta = data?.delta;
  if (!partId || !messageId || typeof delta !== "string") return { ...thread };
  const { list, msg } = getOrCreateAssistantMessage<TManifest, TState>(
    thread.messages,
    data
  );
  let tool = msg.parts.find(
    (p): p is ToolCallUIPart<TManifest> =>
      p.type === "tool-call" &&
      (p as { toolCallId?: unknown }).toolCallId === partId
  );
  if (!tool) {
    // Fallback: attach to the most recent tool that is awaiting/streaming input
    tool = findFallbackToolPartForArgs(msg);
    if (!tool) {
      tool = {
        type: "tool-call",
        toolCallId: partId,
        toolName: (data?.toolName ||
          data?.metadata?.toolName ||
          "") as keyof TManifest & string,
        state: "input-streaming",
        input: {} as ToolCallUIPart<TManifest>["input"],
        output: undefined,
      } as ToolCallUIPart<TManifest>;
      msg.parts = [...msg.parts, tool];
    }
  }
  try {
    // Deltas are JSON string chunks; accumulate into object
    const chunk = delta.trim();
    if (!chunk) return { ...thread };
    const parsedUnknown: unknown = JSON.parse(chunk);
    if (
      parsedUnknown &&
      typeof parsedUnknown === "object" &&
      !Array.isArray(parsedUnknown)
    ) {
      const parsedRecord = parsedUnknown as Record<string, unknown>;
      const prevObject: Record<string, unknown> =
        tool.input &&
        typeof tool.input === "object" &&
        !Array.isArray(tool.input)
          ? (tool.input as Record<string, unknown>)
          : {};
      tool.input = { ...prevObject, ...parsedRecord } as unknown;
    }
  } catch {
    // If not valid standalone JSON, concatenate into a string buffer
    const prev =
      typeof tool.input === "string"
        ? tool.input
        : JSON.stringify(tool.input || {});
    tool.input = prev + delta;
  }
  tool.state = "input-streaming";
  return {
    ...thread,
    messages: list,
    agentStatus: "streaming",
    lastActivity: new Date(),
  };
}

function applyToolOutputDelta<
  TManifest extends ToolManifest = ToolManifest,
  TState = Record<string, unknown>,
>(
  thread: ThreadState<TManifest, TState>,
  data: ToolOutputDelta["data"]
): ThreadState<TManifest, TState> {
  if (!data) return thread;
  const partId = data?.partId;
  const messageId = data?.messageId;
  const delta = data?.delta;
  if (!partId || !messageId || typeof delta !== "string") return { ...thread };
  const { list, msg } = getOrCreateAssistantMessage<TManifest, TState>(
    thread.messages,
    data
  );
  let tool = msg.parts.find(
    (p): p is ToolCallUIPart<TManifest> =>
      p.type === "tool-call" &&
      (p as { toolCallId?: unknown }).toolCallId === partId
  );
  if (!tool) {
    // Fallback: attach to the most recent tool that is executing or has input available
    tool = findFallbackToolPartForOutput(msg);
    if (!tool) return { ...thread };
  }
  const previous =
    tool.output &&
    typeof tool.output === "object" &&
    tool.output !== null &&
    "data" in tool.output
      ? (tool.output as { data: unknown }).data
      : undefined;

  const previousString = (() => {
    if (previous === undefined) return "";
    if (typeof previous === "string") return previous;
    if (typeof previous === "number" || typeof previous === "boolean") {
      return String(previous);
    }
    try {
      return JSON.stringify(previous);
    } catch {
      return "";
    }
  })();

  const nextData = `${previousString}${delta}`;

  tool.output = {
    data: nextData,
  } as ToolCallUIPart<TManifest>["output"];
  tool.state = "executing";
  return {
    ...thread,
    messages: list,
    agentStatus: "streaming",
    lastActivity: new Date(),
  };
}

// === Helper utilities for robust tool matching ===

function findFallbackToolPartForCompletion<
  TManifest extends ToolManifest = ToolManifest,
  TState = Record<string, unknown>,
>(
  message: ConversationMessage<TManifest, TState>
): ToolCallUIPart<TManifest> | undefined {
  // Prefer most recent tool without finalized output
  const tools = message.parts.filter((p) => p.type === "tool-call");
  for (let i = tools.length - 1; i >= 0; i--) {
    const t = tools[i];
    if (t.state !== "output-available") return t;
  }
  return undefined;
}

function findFallbackToolPartForArgs<
  TManifest extends ToolManifest = ToolManifest,
  TState = Record<string, unknown>,
>(
  message: ConversationMessage<TManifest, TState>
): ToolCallUIPart<TManifest> | undefined {
  const tools = message.parts.filter((p) => p.type === "tool-call");
  for (let i = tools.length - 1; i >= 0; i--) {
    const t = tools[i];
    if (t.state === "input-streaming" || t.state === "input-available")
      return t;
  }
  return undefined;
}

function findFallbackToolPartForOutput<
  TManifest extends ToolManifest = ToolManifest,
  TState = Record<string, unknown>,
>(
  message: ConversationMessage<TManifest, TState>
): ToolCallUIPart<TManifest> | undefined {
  const tools = message.parts.filter((p) => p.type === "tool-call");
  for (let i = tools.length - 1; i >= 0; i--) {
    const t = tools[i];
    if (t.state === "executing" || t.state === "input-available") return t;
  }
  return undefined;
}

function finalizeToolsWithOutput<
  TManifest extends ToolManifest = ToolManifest,
  TState = Record<string, unknown>,
>(thread: ThreadState<TManifest, TState>): ThreadState<TManifest, TState> {
  let changed = false;
  const updatedMessages = (thread.messages || []).map((m) => {
    if (m.role !== "assistant") return m;
    const parts = m.parts.map((p) => {
      if (p.type !== "tool-call") return p;
      const tool = p; // narrowed to ToolCallUIPart by discriminant
      if (tool.state === "executing" && tool.output !== undefined) {
        const wrappedOutput =
          tool.output &&
          typeof tool.output === "object" &&
          tool.output !== null &&
          "data" in tool.output
            ? (tool.output as ToolResultPayload<unknown>)
            : ({ data: tool.output } satisfies ToolResultPayload<unknown>);
        changed = true;
        return {
          ...tool,
          state: "output-available",
          output: wrappedOutput as ToolCallUIPart<TManifest>["output"],
        } as ToolCallUIPart<TManifest>;
      }
      return p;
    });
    if (changed) {
      return { ...m, parts } as ConversationMessage<TManifest, TState>;
    }
    return m;
  });
  if (!changed) return thread;
  return { ...thread, messages: updatedMessages } as ThreadState<
    TManifest,
    TState
  >;
}

function normalizeToolResultPayload(
  _toolName: string,
  finalContent: unknown
): ToolResultPayload<unknown> {
  if (
    finalContent &&
    typeof finalContent === "object" &&
    finalContent !== null &&
    "data" in (finalContent as Record<string, unknown>)
  ) {
    return finalContent as ToolResultPayload<unknown>;
  }
  return { data: finalContent } satisfies ToolResultPayload<unknown>;
}
