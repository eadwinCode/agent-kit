/**
 * Typed transport errors.
 *
 * A default transport reduces every failure to `Error.message`, which throws
 * away everything a client needs to react: the status, the server's error
 * code, whether retrying could work, the correlation id for support, and —
 * for a conflict — the authoritative state the client should reconcile to.
 * Applications work around that by stashing the structured body in a side
 * variable next to the hook, which is not a contract and does not survive a
 * refactor.
 *
 * `AgentTransportError` carries all of it, so no side channel is needed.
 * The wire shape is frozen in contracts/schemas/error-envelope.schema.json.
 */

import type { AgentStateSnapshot } from "../../types/index.js";

/**
 * Bounded, machine-readable server error codes. The list is open — a server
 * may send a code this package has never heard of, and `code` stays a string
 * — but these are the ones with defined client behavior.
 */
export const AgentErrorCodes = {
  /** The client's expectedRevision was stale; reconcile from `snapshot`. */
  StateRevisionMismatch: "STATE_REVISION_MISMATCH",
  /** The same command id arrived with a different payload. Never retry as-is. */
  IdempotencyKeyReused: "IDEMPOTENCY_KEY_REUSED",
  /** Another writer holds the project lease. Retry after `retryAfterMs`. */
  ProjectWriterBusy: "PROJECT_WRITER_BUSY",
  /** The run already reached a terminal outcome and cannot be reopened. */
  RunTerminal: "RUN_TERMINAL",
  /** An active run already exists for this session. */
  ActiveRunExists: "ACTIVE_RUN_EXISTS",
  /** The durable tail no longer holds the requested events. Re-snapshot. */
  RetentionGap: "RETENTION_GAP",
  /** Authentication failed. Retrying without new credentials cannot help. */
  Unauthorized: "UNAUTHORIZED",
  /** Authorization failed for this project or session. */
  Forbidden: "FORBIDDEN",
} as const;

export type AgentErrorCode =
  (typeof AgentErrorCodes)[keyof typeof AgentErrorCodes];

/** The frozen error envelope every assistant endpoint returns. */
export interface AgentErrorEnvelope {
  error: {
    code: string;
    message: string;
    recoverable: boolean;
    correlationId?: string;
    retryAfterMs?: number;
    details?: Record<string, unknown>;
  };
  /** The authoritative state, returned with a conflict. */
  snapshot?: AgentStateSnapshot;
}

export interface AgentTransportErrorInit {
  status: number;
  code: string;
  message: string;
  recoverable: boolean;
  correlationId?: string;
  retryAfterMs?: number;
  details?: Record<string, unknown>;
  snapshot?: AgentStateSnapshot;
  /** The operation that failed, e.g. "sendMessage". */
  operation?: string;
  cause?: unknown;
}

/**
 * A transport failure with everything the caller needs to decide what to do.
 */
export class AgentTransportError extends Error {
  readonly name = "AgentTransportError";
  /** HTTP status, or 0 for a network-level failure with no response. */
  readonly status: number;
  /** Bounded server code; see AgentErrorCodes. */
  readonly code: string;
  /** Whether retrying could plausibly succeed. */
  readonly recoverable: boolean;
  readonly correlationId?: string;
  readonly retryAfterMs?: number;
  readonly details?: Record<string, unknown>;
  /**
   * The authoritative state a conflict returned. Present on
   * STATE_REVISION_MISMATCH and other conflicts; reconciling from it is the
   * intended recovery, not a refetch.
   */
  readonly snapshot?: AgentStateSnapshot;
  readonly operation?: string;

  constructor(init: AgentTransportErrorInit) {
    super(init.message);
    if (init.cause !== undefined) {
      // `cause` in the Error constructor needs a lib target this package does
      // not require; assigning keeps the chain without raising the baseline.
      (this as { cause?: unknown }).cause = init.cause;
    }
    this.status = init.status;
    this.code = init.code;
    this.recoverable = init.recoverable;
    this.correlationId = init.correlationId;
    this.retryAfterMs = init.retryAfterMs;
    this.details = init.details;
    this.snapshot = init.snapshot;
    this.operation = init.operation;
  }

  /** True when the failure is an authentication or authorization problem. */
  get isAuthFailure(): boolean {
    return (
      this.status === 401 ||
      this.status === 403 ||
      this.code === AgentErrorCodes.Unauthorized ||
      this.code === AgentErrorCodes.Forbidden
    );
  }

  /** True when the client should reconcile from `snapshot` rather than retry. */
  get requiresReconcile(): boolean {
    return (
      this.code === AgentErrorCodes.StateRevisionMismatch ||
      this.code === AgentErrorCodes.RetentionGap ||
      Boolean(this.snapshot)
    );
  }
}

/** Narrowing helper that survives bundling across module realms. */
export function isAgentTransportError(
  value: unknown
): value is AgentTransportError {
  return (
    value instanceof AgentTransportError ||
    (typeof value === "object" &&
      value !== null &&
      (value as { name?: unknown }).name === "AgentTransportError" &&
      typeof (value as { code?: unknown }).code === "string")
  );
}

/**
 * Default recoverability for a status, used when the server did not say.
 * 408/429 and 5xx are worth retrying; 4xx generally is not, because the
 * request itself is what the server objected to.
 */
export function isRecoverableStatus(status: number): boolean {
  if (status === 408 || status === 429) return true;
  if (status >= 500) return true;
  return false;
}

function readRetryAfter(headers: Headers | undefined): number | undefined {
  const raw = headers?.get?.("retry-after");
  if (!raw) return undefined;
  const seconds = Number(raw);
  if (Number.isFinite(seconds)) return Math.max(0, seconds * 1000);
  const date = Date.parse(raw);
  if (Number.isFinite(date)) return Math.max(0, date - Date.now());
  return undefined;
}

/**
 * Builds an AgentTransportError from a failed response, preferring the
 * server's structured envelope and falling back to status-derived defaults
 * when the body is missing or not JSON.
 */
export async function agentTransportErrorFromResponse(
  response: Response,
  operation?: string
): Promise<AgentTransportError> {
  let envelope: Partial<AgentErrorEnvelope> | undefined;
  try {
    envelope = (await response.json()) as Partial<AgentErrorEnvelope>;
  } catch {
    // A non-JSON body is normal for proxies and gateways; fall through.
  }

  const error = envelope?.error;
  const headerRetry = readRetryAfter(response.headers);

  return new AgentTransportError({
    status: response.status,
    code: error?.code || `HTTP_${response.status}`,
    message:
      error?.message ||
      `HTTP ${response.status}${response.statusText ? `: ${response.statusText}` : ""}`,
    recoverable:
      typeof error?.recoverable === "boolean"
        ? error.recoverable
        : isRecoverableStatus(response.status),
    correlationId:
      error?.correlationId ||
      response.headers?.get?.("x-request-id") ||
      undefined,
    retryAfterMs: error?.retryAfterMs ?? headerRetry,
    details: error?.details,
    snapshot: envelope?.snapshot,
    operation,
  });
}

/**
 * Wraps a network-level failure (no response at all) as a transport error.
 * These are always recoverable: nothing about the request was rejected.
 */
export function agentTransportErrorFromNetwork(
  cause: unknown,
  operation?: string
): AgentTransportError {
  const message =
    cause instanceof Error ? cause.message : "Network request failed";
  return new AgentTransportError({
    status: 0,
    code: "NETWORK_ERROR",
    message,
    recoverable: true,
    operation,
    cause,
  });
}
