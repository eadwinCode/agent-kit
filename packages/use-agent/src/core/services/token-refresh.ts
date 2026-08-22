/**
 * Recoverable realtime-token acquisition.
 *
 * A single failed token fetch used to put the connection into a terminal
 * error state, so a two-second network blip meant a dead socket until the
 * component remounted. Applications papered over it by retrying inside their
 * own token getter — an infinite loop with no backoff, no jitter and no
 * cancellation, which turns one outage into a thundering herd.
 *
 * The distinction that makes this tractable: an authentication failure and a
 * transient failure need opposite responses. 401/403 means retrying with the
 * same credentials cannot work, so fail fast and ask the user to
 * reauthenticate. Anything else — 5xx, a timeout, a dropped connection — is
 * worth retrying with exponential backoff and jitter.
 *
 * Connection state is client-local throughout. The server run keeps going
 * whether or not this socket ever reconnects.
 */

import type { ClientConnectionState } from "../../types/index.js";
import {
  isAgentTransportError,
  isRecoverableStatus,
} from "../errors/agent-transport-error.js";

export interface TokenRefreshOptions {
  /** First retry delay. */
  baseDelayMs?: number;
  /** Upper bound on any single delay. */
  maxDelayMs?: number;
  /** Attempts before giving up; Infinity keeps trying while mounted. */
  maxAttempts?: number;
  /** Jitter fraction of the computed delay, 0..1. */
  jitter?: number;
  signal?: AbortSignal;
  /** Injectable for deterministic tests. */
  sleep?: (ms: number, signal?: AbortSignal) => Promise<void>;
  random?: () => number;
  /** Reports each state change so the UI can show "Reconnecting…". */
  onStateChange?: (state: ClientConnectionState) => void;
  /** Reports a failed attempt, for metrics. */
  onAttemptFailed?: (info: {
    attempt: number;
    delayMs: number;
    recoverable: boolean;
    error: unknown;
  }) => void;
}

/** Thrown when acquisition gave up. `recoverable` says whether it could work later. */
export class TokenRefreshError extends Error {
  readonly name = "TokenRefreshError";
  readonly recoverable: boolean;
  readonly attempts: number;
  readonly lastError: unknown;

  constructor(init: {
    message: string;
    recoverable: boolean;
    attempts: number;
    lastError: unknown;
  }) {
    super(init.message);
    this.recoverable = init.recoverable;
    this.attempts = init.attempts;
    this.lastError = init.lastError;
  }
}

/**
 * Classifies a failure as an auth problem (fail fast) or transient (retry).
 *
 * Unknown failures are treated as transient. Getting this backwards in the
 * safe direction costs one retry; getting it backwards the other way strands
 * a working session on a blip.
 */
export function isAuthFailure(error: unknown): boolean {
  if (isAgentTransportError(error)) return error.isAuthFailure;
  const status = (error as { status?: unknown })?.status;
  if (typeof status === "number") return status === 401 || status === 403;
  const raw = (error as { message?: unknown })?.message;
  const message = typeof raw === "string" ? raw : "";
  return /\b(401|403|unauthori[sz]ed|forbidden)\b/i.test(message);
}

/** True when retrying this failure could plausibly succeed. */
export function isTransientFailure(error: unknown): boolean {
  if (isAuthFailure(error)) return false;
  if (isAgentTransportError(error)) return error.recoverable;
  const status = (error as { status?: unknown })?.status;
  if (typeof status === "number") return isRecoverableStatus(status);
  return true;
}

const defaultSleep = (ms: number, signal?: AbortSignal): Promise<void> =>
  new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(new DOMException("Aborted", "AbortError"));
      return;
    }
    const timer = setTimeout(() => {
      signal?.removeEventListener?.("abort", onAbort);
      resolve();
    }, ms);
    function onAbort() {
      clearTimeout(timer);
      reject(new DOMException("Aborted", "AbortError"));
    }
    signal?.addEventListener?.("abort", onAbort, { once: true });
  });

/**
 * Full-jitter exponential backoff: `random() * min(max, base * 2^attempt)`,
 * blended with the jitter fraction. Full jitter is what keeps many clients
 * from reconnecting in lockstep after a shared outage.
 */
export function backoffDelay(
  attempt: number,
  options: {
    baseDelayMs?: number;
    maxDelayMs?: number;
    jitter?: number;
    random?: () => number;
  } = {}
): number {
  const base = options.baseDelayMs ?? 500;
  const max = options.maxDelayMs ?? 30_000;
  const jitter = Math.min(Math.max(options.jitter ?? 1, 0), 1);
  const random = options.random ?? Math.random;
  const ceiling = Math.min(max, base * Math.pow(2, Math.max(0, attempt)));
  const fixed = ceiling * (1 - jitter);
  return Math.round(fixed + random() * (ceiling - fixed));
}

/**
 * Acquires a realtime token, retrying transient failures with bounded
 * exponential backoff and jitter, and failing fast on auth failures.
 */
export async function acquireRealtimeToken<T>(
  fetchToken: (signal?: AbortSignal) => Promise<T>,
  options: TokenRefreshOptions = {}
): Promise<T> {
  const {
    baseDelayMs = 500,
    maxDelayMs = 30_000,
    maxAttempts = 8,
    jitter = 1,
    signal,
    sleep = defaultSleep,
    random = Math.random,
    onStateChange,
    onAttemptFailed,
  } = options;

  let attempt = 0;

  for (;;) {
    if (signal?.aborted) {
      throw new DOMException("Aborted", "AbortError");
    }
    onStateChange?.(attempt === 0 ? "connecting" : "reconnecting");
    try {
      const token = await fetchToken(signal);
      onStateChange?.("connected");
      return token;
    } catch (error) {
      if (signal?.aborted) throw error;

      if (isAuthFailure(error)) {
        // Retrying with the same credentials cannot help; the user has to
        // reauthenticate. Burning the backoff budget here only delays that.
        onStateChange?.("error");
        throw new TokenRefreshError({
          message:
            "Realtime authentication failed. Sign in again to reconnect.",
          recoverable: false,
          attempts: attempt + 1,
          lastError: error,
        });
      }

      attempt++;
      if (attempt >= maxAttempts) {
        onStateChange?.("error");
        throw new TokenRefreshError({
          message: `Realtime token could not be refreshed after ${attempt} attempts.`,
          recoverable: true,
          attempts: attempt,
          lastError: error,
        });
      }

      const delayMs = backoffDelay(attempt - 1, {
        baseDelayMs,
        maxDelayMs,
        jitter,
        random,
      });
      onAttemptFailed?.({
        attempt,
        delayMs,
        recoverable: isTransientFailure(error),
        error,
      });
      onStateChange?.("reconnecting");
      await sleep(delayMs, signal);
    }
  }
}

/**
 * Maps a realtime provider's own state value onto the package's connection
 * vocabulary. Providers report differently-shaped values; the UI needs one.
 */
export function normalizeConnectionState(
  raw: unknown,
  previous?: ClientConnectionState
): ClientConnectionState {
  const text = String(raw).toLowerCase();
  if (["active", "open", "connected", "ready"].includes(text) || raw === 1) {
    return "connected";
  }
  if (["connecting", "opening"].includes(text)) {
    // A reconnect and a first connect look the same to most providers; the
    // difference is whether we have ever been connected.
    return previous === "connected" || previous === "reconnecting"
      ? "reconnecting"
      : "connecting";
  }
  if (["reconnecting", "retrying"].includes(text)) return "reconnecting";
  if (["error", "failed"].includes(text)) return "error";
  if (["closed", "closing", "disconnected", "inactive"].includes(text)) {
    return "disconnected";
  }
  return previous ?? "connecting";
}
