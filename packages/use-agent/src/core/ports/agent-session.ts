/**
 * Server-authoritative session ports.
 *
 * These are the client half of the AgentKit runtime contracts. The package
 * knows the *shape* of a snapshot, an event tail and a command; it does not
 * know which endpoints serve them, which tables back them, or how the
 * application authorizes them. The application implements these interfaces
 * against its own authenticated API — the same dependency-inversion boundary
 * the Go runtime uses for storage.
 *
 * Nothing here may be satisfied by browser storage. "Which thread am I in"
 * and "is a run active" are server facts: a new device, cleared storage, or a
 * closed originating tab must not change the answer.
 *
 * Nor does anything here prescribe an ownership model. The transport already
 * authenticated against its application-defined scope. Applications may
 * extend these base records and requests without making their fields part of
 * AgentKit's required contract.
 */

import type {
  AgentStateSnapshot,
  StandardEventEnvelope,
  AgentKitMessage,
} from "../../types/index.js";
import type { RequestOptions } from "./transport.js";

/** A position in one run's ordered event tail. */
export interface StreamCursor {
  runId: string;
  streamEpoch: number;
  /** Exclusive. -1 means "from the first event of this epoch". */
  sequenceNumber: number;
}

/** The cursor value meaning "everything from the beginning". */
export const STREAM_START = -1;

export interface FetchAgentStateParams {
  /** Revision the client already holds, for a conditional fetch. */
  knownRevision?: number;
}

/** What a snapshot fetch returns: state, canonical history, and where to tail from. */
export interface AgentStateResponse {
  snapshot: AgentStateSnapshot;
  /**
   * Canonical conversation history for the current thread, in package
   * message form. It replaces whatever the client held — it does not merge.
   */
  messages: AgentKitMessage[];
  /**
   * Where a client with NO prior state should begin tailing: the position
   * BEFORE the first event of the active run's current epoch — normally
   * `sequenceNumber: STREAM_START`.
   *
   * It is deliberately not "the latest sequence the server has". A client
   * that joins mid-turn needs the whole in-flight message, and an in-flight
   * message exists only in the tail: canonical history has not absorbed it
   * yet. Pointing this at the newest event would leave a new tab watching an
   * assistant that never says anything.
   *
   * A client that already applied events overrides this with its own
   * last-applied cursor (see HydrationOptions.from).
   *
   * Null when no run is active.
   */
  cursor: StreamCursor | null;
}

export interface FetchEventTailParams {
  threadId: string;
  after: StreamCursor;
  limit?: number;
}

export interface EventTailPage {
  /** Ordered by (streamEpoch, sequenceNumber, eventId). */
  events: StandardEventEnvelope[];
  next: StreamCursor;
  hasMore: boolean;
  /**
   * The server can no longer serve events at or after the requested cursor.
   * The client must re-snapshot: waiting for a backfill that will never
   * arrive is how a transcript freezes forever.
   */
  retentionGap?: boolean;
}

/** The base set of commands the package-owned session hook can issue. */
export type AgentCommandType =
  | "send"
  | "pause"
  | "resume"
  | "cancel"
  | "approve"
  | "deny"
  | "retry"
  | "edit"
  | "new_chat";

/**
 * One idempotent command. `commandId` is the idempotency key: replaying it
 * with the same payload returns the recorded result instead of applying the
 * command twice, and reusing it with a different payload is rejected.
 */
export interface AgentCommand<TPayload = Record<string, unknown>> {
  commandId: string;
  type: AgentCommandType;
  threadId?: string;
  runId?: string;
  approvalId?: string;
  /** Correlates a resume with the pause it answers. */
  pauseEpoch?: number;
  /** Optional CAS precondition. Omit or 0 for none. */
  expectedRevision?: number;
  payload?: TPayload;
}

export interface AgentCommandResult {
  /** The authoritative state after the command. */
  snapshot: AgentStateSnapshot;
  /** True when the server returned a previously recorded result. */
  duplicate?: boolean;
  /** Bounded structured result code. */
  outcomeCode?: string;
  /** Where to tail from after this command, when the server supplies one. */
  cursor?: StreamCursor | null;
}

/**
 * The server-authoritative session transport. Implement it against your own
 * authenticated API; the package never constructs URLs of its own.
 */
export interface IAgentSessionTransport {
  /**
   * Loads the authoritative session state plus canonical history. This is
   * step one of every hydration, reconnect and reconcile.
   */
  fetchAgentState(
    params: FetchAgentStateParams,
    options?: RequestOptions
  ): Promise<AgentStateResponse>;

  /**
   * Reads one ordered page of the durable event tail. Called after the
   * snapshot and whenever a live sequence gap needs backfilling.
   */
  fetchEventTail(
    params: FetchEventTailParams,
    options?: RequestOptions
  ): Promise<EventTailPage>;

  /**
   * Executes one idempotent command. Failures must reject with an
   * AgentTransportError so the caller sees the code, recoverability and any
   * authoritative snapshot.
   */
  executeCommand(
    command: AgentCommand,
    options?: RequestOptions
  ): Promise<AgentCommandResult>;
}

/** Narrowing helper for transports that implement the session contract. */
export function supportsAgentSession(
  transport: unknown
): transport is IAgentSessionTransport {
  const candidate = transport as Partial<IAgentSessionTransport> | null;
  return (
    typeof candidate?.fetchAgentState === "function" &&
    typeof candidate?.fetchEventTail === "function" &&
    typeof candidate?.executeCommand === "function"
  );
}
