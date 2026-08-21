"use client";

import { useEffect, useRef } from "react";
import type { IConnection } from "../../../core/ports/connection.js";
import { useInngestSubscription } from "@inngest/realtime/hooks";

/**
 * NOTE (2025-09): Realtime subscriptions require a token.
 * We currently rely on the official `useInngestSubscription` path when a
 * `refreshToken` handler is provided. The previous fallback path using
 * `ConnectionManager` has been disabled to make token usage explicit.
 *
 * Plan: We'll replace the React hook with a framework-agnostic connection
 * adapter (`InngestConnection`) managed via `ConnectionManager`, and use
 * `useSyncExternalStore` to bridge into React.
 */
export function useConnectionSubscription(params: {
  connection: IConnection | null;
  channel: string | null;
  onMessage: (chunk: unknown) => void;
  onStateChange?: (state: unknown) => void;
  debug?: boolean;
  /** Optional: direct token fetcher; when provided, we use the official hook */
  refreshToken?: () => Promise<unknown>;
}) {
  const { connection, channel, onMessage, onStateChange, debug, refreshToken } =
    params;
  const onMessageRef = useRef(onMessage);
  const onStateChangeRef = useRef(onStateChange);
  useEffect(() => {
    onMessageRef.current = onMessage;
    onStateChangeRef.current = onStateChange;
  }, [onMessage, onStateChange]);

  const customConnectionEnabled = Boolean(channel && connection);
  const realtimeHookEnabled = Boolean(channel && refreshToken && !connection);
  type SubscriptionOptions = {
    key?: string;
    enabled?: boolean;
    refreshToken: () => Promise<unknown>;
  };
  const subOptions: SubscriptionOptions = {
    key: channel || undefined,
    enabled: realtimeHookEnabled,
    refreshToken: async () => {
      if (!refreshToken) {
        throw new Error("Realtime token provider is unavailable");
      }
      return await refreshToken();
    },
  };

  const { data, state, error } = useInngestSubscription(
    subOptions as unknown as Parameters<typeof useInngestSubscription>[0]
  );

  const lastLenRef = useRef(0);
  useEffect(() => {
    if (!realtimeHookEnabled) return;
    try {
      onStateChange?.(state);
    } catch (err) {
      if (debug)
        console.warn("[useConnectionSubscription] state handler error", err);
    }
  }, [realtimeHookEnabled, state, onStateChange]);

  useEffect(() => {
    if (!realtimeHookEnabled) return;
    if (!Array.isArray(data)) return;
    for (let i = lastLenRef.current; i < data.length; i++) {
      try {
        onMessage(data[i]);
      } catch (err) {
        if (debug)
          console.warn(
            "[useConnectionSubscription] message handler error",
            err
          );
      }
    }
    lastLenRef.current = data.length;
  }, [realtimeHookEnabled, data, onMessage]);

  useEffect(() => {
    if (!customConnectionEnabled || !connection || !channel) return;
    let disposed = false;
    let activeSubscription: { unsubscribe(): void } | undefined;

    Promise.resolve(
      connection.subscribe({
        channel,
        onMessage: (chunk) => onMessageRef.current(chunk),
        onStateChange: (nextState) => onStateChangeRef.current?.(nextState),
        debug,
      })
    )
      .then((subscription) => {
        if (disposed) subscription.unsubscribe();
        else activeSubscription = subscription;
      })
      .catch((err: unknown) => {
        if (debug)
          console.warn(
            "[useConnectionSubscription] custom connection error",
            err
          );
        try {
          onStateChangeRef.current?.("Error");
        } catch {
          // Ignore consumer callback failures.
        }
      });

    return () => {
      disposed = true;
      activeSubscription?.unsubscribe();
    };
  }, [customConnectionEnabled, connection, channel, debug]);

  // Minimal error logging / diagnostics
  useEffect(() => {
    if (realtimeHookEnabled || customConnectionEnabled || !channel) return;
    if (debug)
      console.warn(
        "[useConnectionSubscription] Token is required; realtime disabled (channel=",
        channel,
        ")"
      );
  }, [realtimeHookEnabled, customConnectionEnabled, channel, debug]);

  useEffect(() => {
    if (!realtimeHookEnabled || !error) return;
    if (debug)
      console.warn("[useConnectionSubscription] realtime error", error);
  }, [realtimeHookEnabled, error, debug]);
}
