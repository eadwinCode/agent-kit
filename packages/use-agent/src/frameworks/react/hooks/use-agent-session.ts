"use client";

/**
 * useAgentSession — the package-owned server-authoritative session hook.
 *
 * It runs the snapshot-plus-tail algorithm, keeps live delivery honest with
 * gap detection and backfill, and issues idempotent commands. Applications
 * used to reimplement all of that around the hook: a five-second polling
 * loop for active runs, a side variable holding a structured 409, a set of
 * approval tombstones, refs to keep callbacks from changing identity every
 * render. Each of those is a contract here instead.
 *
 * It does not reduce events itself. `onEvents` hands ordered, deduplicated
 * envelopes to the package's own reducer, so there is still exactly one
 * transcript owner.
 *
 * Every returned action has stable identity across renders. That is not
 * cosmetic: unstable callbacks re-trigger effects in consuming components,
 * which is how a "harmless" render turns into a duplicate command.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type {
  AgentStateSnapshot,
  StandardEventEnvelope,
  AgentKitMessage,
  ClientConnectionState,
} from "../../../types/index.js";
import type {
  IAgentSessionTransport,
  AgentCommand,
  AgentCommandResult,
  StreamCursor,
} from "../../../core/ports/agent-session.js";
import {
  hydrateAgentSession,
  LiveEventBuffer,
  SequenceGapTracker,
  sortEnvelopes,
} from "../../../core/services/hydration.js";
import {
  buildCommand,
  executeCommand,
} from "../../../core/services/commands.js";
import { normalizeConnectionState } from "../../../core/services/token-refresh.js";
import {
  isAgentTransportError,
  type AgentTransportError,
} from "../../../core/errors/agent-transport-error.js";

export type AgentSessionStatus = "idle" | "hydrating" | "ready" | "error";

export interface UseAgentSessionConfig {
  /** The application's authenticated session transport. */
  transport: IAgentSessionTransport | null | undefined;
  /**
   * An opaque key identifying which conversation this hook is watching.
   *
   * It is NOT sent anywhere: the transport the application supplies already
   * knows its own scope. The hook only compares it, and re-hydrates when it
   * changes. Its meaning remains entirely application-defined.
   */
  scope?: string;
  /** Set false to keep the hook dormant (e.g. before auth resolves). */
  enabled?: boolean;
  /**
   * How long a live sequence gap may stay unfilled before the hook
   * re-snapshots instead of waiting. Without a bound, one missing sequence
   * number freezes the transcript.
   */
  gapTimeoutMs?: number;
  /** Ordered, deduplicated envelopes to reduce. */
  onEvents?: (events: StandardEventEnvelope[]) => void;
  /** Canonical history to install, REPLACING what the client held. */
  onMessages?: (messages: AgentKitMessage[]) => void;
  onSnapshot?: (snapshot: AgentStateSnapshot) => void;
  onError?: (error: unknown) => void;
}

export interface UseAgentSessionReturn {
  snapshot: AgentStateSnapshot | null;
  status: AgentSessionStatus;
  error: AgentTransportError | Error | null;
  connectionState: ClientConnectionState;
  /** The durable tail has holes; completed content still comes from history. */
  reconcileRequired: boolean;

  /** Feed one live envelope. Ordering, dedupe and backfill are handled here. */
  ingest: (envelope: StandardEventEnvelope) => void;
  /** Re-run snapshot-plus-tail. Safe to call on reconnect or on demand. */
  hydrate: () => Promise<void>;
  /** Report the realtime provider's own state; mapped to one vocabulary. */
  reportConnectionState: (raw: unknown) => void;

  send: (
    payload: Record<string, unknown>,
    options?: { commandId?: string }
  ) => Promise<AgentCommandResult>;
  pause: (options?: { commandId?: string }) => Promise<AgentCommandResult>;
  resume: (options?: { commandId?: string }) => Promise<AgentCommandResult>;
  cancel: (options?: { commandId?: string }) => Promise<AgentCommandResult>;
  approve: (
    approvalId: string,
    options?: { commandId?: string }
  ) => Promise<AgentCommandResult>;
  deny: (
    approvalId: string,
    options?: { commandId?: string; reason?: string }
  ) => Promise<AgentCommandResult>;
  retry: (options?: { commandId?: string }) => Promise<AgentCommandResult>;
  edit: (
    payload: Record<string, unknown>,
    options?: { commandId?: string }
  ) => Promise<AgentCommandResult>;
  newChat: (options?: { commandId?: string }) => Promise<AgentCommandResult>;
}

export function useAgentSession(
  config: UseAgentSessionConfig
): UseAgentSessionReturn {
  const {
    transport,
    scope,
    enabled = true,
    gapTimeoutMs = 5000,
    onEvents,
    onMessages,
    onSnapshot,
    onError,
  } = config;

  const [snapshot, setSnapshot] = useState<AgentStateSnapshot | null>(null);
  const [status, setStatus] = useState<AgentSessionStatus>("idle");
  const [error, setError] = useState<AgentTransportError | Error | null>(null);
  const [connectionState, setConnectionState] =
    useState<ClientConnectionState>("connecting");
  const [reconcileRequired, setReconcileRequired] = useState(false);

  // Everything the actions read lives in a ref, so the actions themselves
  // never change identity.
  const transportRef = useRef(transport);
  const snapshotRef = useRef<AgentStateSnapshot | null>(null);
  const onEventsRef = useRef(onEvents);
  const onMessagesRef = useRef(onMessages);
  const onSnapshotRef = useRef(onSnapshot);
  const onErrorRef = useRef(onError);
  const scopeRef = useRef(scope);

  transportRef.current = transport;
  onEventsRef.current = onEvents;
  onMessagesRef.current = onMessages;
  onSnapshotRef.current = onSnapshot;
  onErrorRef.current = onError;
  scopeRef.current = scope;

  const bufferRef = useRef<LiveEventBuffer>(new LiveEventBuffer());
  const trackerRef = useRef<SequenceGapTracker>(
    new SequenceGapTracker({ timeoutMs: gapTimeoutMs })
  );
  // The last position this client actually applied. A reconnect resumes from
  // here rather than replaying the turn it already rendered.
  const appliedCursorRef = useRef<StreamCursor | null>(null);
  const abortRef = useRef<AbortController | null>(null);
  const mountedRef = useRef(true);
  // Invalidates hydration, backfill and command responses that belong to a
  // superseded transport/scope. Without this, a slow response from the old
  // owner can overwrite the new owner's snapshot after a switch.
  const syncGenerationRef = useRef(0);
  const targetRef = useRef<{
    transport: IAgentSessionTransport;
    scope: string | undefined;
  } | null>(null);

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
      abortRef.current?.abort();
    };
  }, []);

  const applySnapshot = useCallback((next: AgentStateSnapshot) => {
    snapshotRef.current = next;
    if (mountedRef.current) setSnapshot(next);
    onSnapshotRef.current?.(next);
  }, []);

  const reportError = useCallback((err: unknown) => {
    const normalized =
      err instanceof Error
        ? err
        : new Error(typeof err === "string" ? err : "Unknown session error");
    if (mountedRef.current) {
      setError(normalized as AgentTransportError | Error);
      setStatus("error");
    }
    onErrorRef.current?.(err);
  }, []);

  const hydrate = useCallback(async () => {
    const activeTransport = transportRef.current;
    if (!activeTransport) return;

    const targetChanged =
      targetRef.current?.transport !== activeTransport ||
      targetRef.current?.scope !== scopeRef.current;
    if (targetChanged) {
      targetRef.current = {
        transport: activeTransport,
        scope: scopeRef.current,
      };
      appliedCursorRef.current = null;
      trackerRef.current.reset(null);
      snapshotRef.current = null;
      if (mountedRef.current) {
        setSnapshot(null);
        setReconcileRequired(false);
        setError(null);
      }
    }

    const generation = ++syncGenerationRef.current;
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    // Buffering must be live BEFORE the fetch: events produced during
    // hydration belong to the turn the user is watching.
    const buffer = new LiveEventBuffer();
    bufferRef.current = buffer;
    if (mountedRef.current) setStatus("hydrating");

    try {
      const result = await hydrateAgentSession(
        {
          transport: activeTransport,
          from: appliedCursorRef.current,
          signal: controller.signal,
        },
        buffer
      );

      if (
        controller.signal.aborted ||
        generation !== syncGenerationRef.current ||
        result.outcome === "aborted"
      ) {
        return;
      }
      if (result.outcome === "failed") {
        reportError(result.error);
        return;
      }

      if (result.snapshot) applySnapshot(result.snapshot);
      if (result.messages) onMessagesRef.current?.(result.messages);
      if (result.events.length > 0) onEventsRef.current?.(result.events);

      trackerRef.current.reset(result.cursor);
      appliedCursorRef.current = result.cursor;
      if (mountedRef.current) {
        setReconcileRequired(result.reconcileRequired);
        setError(null);
        setStatus("ready");
      }
    } catch (err) {
      if (
        !controller.signal.aborted &&
        generation === syncGenerationRef.current
      ) {
        reportError(err);
      }
    } finally {
      if (abortRef.current === controller) abortRef.current = null;
    }
  }, [applySnapshot, reportError]);

  // Hydrate on mount and whenever the transport or opaque scope changes. A
  // reconnect calls hydrate() again from the consumer's connection handler.
  useEffect(() => {
    if (!enabled || !transport) {
      ++syncGenerationRef.current;
      abortRef.current?.abort();
      abortRef.current = null;
      targetRef.current = null;
      appliedCursorRef.current = null;
      trackerRef.current.reset(null);
      bufferRef.current = new LiveEventBuffer();
      snapshotRef.current = null;
      setSnapshot(null);
      setReconcileRequired(false);
      setError(null);
      setStatus("idle");
      return;
    }
    void hydrate();
  }, [enabled, transport, scope, hydrate]);

  useEffect(() => {
    const tracker = new SequenceGapTracker({ timeoutMs: gapTimeoutMs });
    tracker.reset(appliedCursorRef.current);
    trackerRef.current = tracker;
  }, [gapTimeoutMs]);

  const backfill = useCallback(
    async (after: StreamCursor, threadId: string) => {
      const activeTransport = transportRef.current;
      if (!activeTransport) return;
      const generation = syncGenerationRef.current;
      try {
        const page = await activeTransport.fetchEventTail({ threadId, after });
        if (
          activeTransport !== transportRef.current ||
          generation !== syncGenerationRef.current
        ) {
          return;
        }
        if (page.retentionGap) {
          // The hole can never be filled. A fresh snapshot always can.
          setReconcileRequired(true);
          void hydrate();
          return;
        }
        const released = trackerRef.current.fill(page.events);
        if (released.length > 0) {
          onEventsRef.current?.(released);
          const last = released[released.length - 1];
          appliedCursorRef.current = {
            runId: after.runId,
            streamEpoch: last.streamEpoch ?? after.streamEpoch,
            sequenceNumber: last.sequenceNumber,
          };
        }
      } catch (err) {
        if (
          activeTransport !== transportRef.current ||
          generation !== syncGenerationRef.current
        ) {
          return;
        }
        if (isAgentTransportError(err) && err.code === "RETENTION_GAP") {
          setReconcileRequired(true);
          void hydrate();
          return;
        }
        // A failed backfill is not fatal: the gap timeout will re-snapshot.
        onErrorRef.current?.(err);
      }
    },
    [hydrate]
  );

  const ingest = useCallback(
    (envelope: StandardEventEnvelope) => {
      // Still hydrating: buffer rather than reduce out of order.
      if (bufferRef.current.push(envelope)) return;

      const action = trackerRef.current.accept(envelope);
      if (action.type === "apply") {
        if (action.events.length > 0) {
          const ordered = sortEnvelopes(action.events);
          onEventsRef.current?.(ordered);
          const last = ordered[ordered.length - 1];
          appliedCursorRef.current = {
            runId:
              appliedCursorRef.current?.runId ??
              (typeof last.data?.runId === "string" ? last.data.runId : ""),
            streamEpoch: last.streamEpoch ?? 0,
            sequenceNumber: last.sequenceNumber,
          };
        }
        return;
      }
      if (action.type === "resnapshot") {
        void hydrate();
        return;
      }
      const threadId =
        (typeof envelope.data?.threadId === "string"
          ? envelope.data.threadId
          : snapshotRef.current?.currentThreadId) ?? "";
      void backfill(
        {
          runId: action.gap.runId,
          streamEpoch: action.gap.streamEpoch,
          sequenceNumber: action.gap.after,
        },
        threadId
      );
    },
    [backfill, hydrate]
  );

  const connectionStateRef = useRef<ClientConnectionState>("connecting");
  const reportConnectionState = useCallback((raw: unknown) => {
    const previous = connectionStateRef.current;
    const next = normalizeConnectionState(raw, previous);
    connectionStateRef.current = next;
    setConnectionState(next);
    // A recovered socket must re-sync: the run kept going while we were
    // away, and the tail we missed is exactly what the user cannot see.
    if (previous !== "connected" && next === "connected") {
      void hydrateRef.current?.();
    }
  }, []);

  // The stable connection callback reaches the latest hydration function
  // through a ref.
  const hydrateRef = useRef<(() => Promise<void>) | null>(null);
  hydrateRef.current = hydrate;

  const run = useCallback(
    async (command: AgentCommand): Promise<AgentCommandResult> => {
      const activeTransport = transportRef.current;
      if (!activeTransport) {
        throw new Error(
          "useAgentSession: no session transport is configured; commands need one."
        );
      }
      const generation = syncGenerationRef.current;
      const result = await executeCommand(activeTransport, command, {
        onReconcile: (authoritative) => {
          // A conflict hands back the truth. Reconciling to it is the
          // recovery — refetching blind would race the same way again.
          if (
            generation === syncGenerationRef.current &&
            activeTransport === transportRef.current
          ) {
            applySnapshot(authoritative);
          }
        },
      });
      if (
        generation === syncGenerationRef.current &&
        activeTransport === transportRef.current
      ) {
        applySnapshot(result.snapshot);
        if (result.cursor !== undefined) {
          trackerRef.current.reset(result.cursor ?? null);
          appliedCursorRef.current = result.cursor ?? null;
        }
      }
      return result;
    },
    [applySnapshot]
  );

  const command = useCallback(
    (
      type: AgentCommand["type"],
      extra: Partial<AgentCommand> = {},
      commandId?: string
    ) =>
      run(
        buildCommand({
          type,
          commandId,
          snapshot: snapshotRef.current,
          ...extra,
        })
      ),
    [run]
  );

  const send = useCallback(
    (payload: Record<string, unknown>, options?: { commandId?: string }) =>
      command("send", { payload }, options?.commandId),
    [command]
  );
  const pause = useCallback(
    (options?: { commandId?: string }) =>
      command("pause", {}, options?.commandId),
    [command]
  );
  const resume = useCallback(
    (options?: { commandId?: string }) =>
      command(
        "resume",
        { pauseEpoch: snapshotRef.current?.pause.epoch },
        options?.commandId
      ),
    [command]
  );
  const cancel = useCallback(
    (options?: { commandId?: string }) =>
      command("cancel", {}, options?.commandId),
    [command]
  );
  const approve = useCallback(
    (approvalId: string, options?: { commandId?: string }) =>
      command("approve", { approvalId }, options?.commandId),
    [command]
  );
  const deny = useCallback(
    (approvalId: string, options?: { commandId?: string; reason?: string }) =>
      command(
        "deny",
        {
          approvalId,
          payload: options?.reason ? { reason: options.reason } : undefined,
        },
        options?.commandId
      ),
    [command]
  );
  const retry = useCallback(
    (options?: { commandId?: string }) =>
      command("retry", {}, options?.commandId),
    [command]
  );
  const edit = useCallback(
    (payload: Record<string, unknown>, options?: { commandId?: string }) =>
      command("edit", { payload }, options?.commandId),
    [command]
  );
  const newChat = useCallback(
    (options?: { commandId?: string }) =>
      command("new_chat", {}, options?.commandId),
    [command]
  );

  return useMemo(
    () => ({
      snapshot,
      status,
      error,
      connectionState,
      reconcileRequired,
      ingest,
      hydrate,
      reportConnectionState,
      send,
      pause,
      resume,
      cancel,
      approve,
      deny,
      retry,
      edit,
      newChat,
    }),
    [
      snapshot,
      status,
      error,
      connectionState,
      reconcileRequired,
      ingest,
      hydrate,
      reportConnectionState,
      send,
      pause,
      resume,
      cancel,
      approve,
      deny,
      retry,
      edit,
      newChat,
    ]
  );
}
