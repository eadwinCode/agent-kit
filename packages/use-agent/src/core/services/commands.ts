/**
 * Idempotent command construction.
 *
 * Every mutating assistant action is a command with a client-minted id. That
 * id is what makes a retry safe: the server records the first result and
 * replays it, rather than starting a second run or applying a second pause.
 * Reusing an id with different content is the one case that must fail —
 * silently applying different content under an old key is worse than an error.
 *
 * `expectedRevision` is the other half. Two tabs can both send a command; the
 * revision precondition serializes them, and the loser gets the authoritative
 * snapshot back instead of overwriting the winner.
 */

import type {
  AgentCommand,
  AgentCommandType,
  IAgentSessionTransport,
  AgentCommandResult,
} from "../ports/agent-session.js";
import type { RequestOptions } from "../ports/transport.js";
import type { AgentStateSnapshot } from "../../types/index.js";
import {
  AgentErrorCodes,
  isAgentTransportError,
} from "../errors/agent-transport-error.js";

/** Mints a command id. Prefers crypto.randomUUID where available. */
export function createCommandId(): string {
  const crypto = (globalThis as { crypto?: Crypto }).crypto;
  if (crypto?.randomUUID) return crypto.randomUUID();
  if (crypto?.getRandomValues) {
    const bytes = crypto.getRandomValues(new Uint8Array(16));
    return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
  }
  // Non-cryptographic last resort. Command ids need uniqueness, not secrecy:
  // the server authorizes the request, it does not trust this value.
  return `cmd_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
}

export interface BuildCommandParams<TPayload = Record<string, unknown>> {
  type: AgentCommandType;
  commandId?: string;
  threadId?: string;
  runId?: string;
  approvalId?: string;
  pauseEpoch?: number;
  /**
   * The snapshot the client is acting on. Its revision becomes the CAS
   * precondition, so a command built from stale state is rejected rather
   * than applied over someone else's.
   */
  snapshot?: AgentStateSnapshot | null;
  expectedRevision?: number;
  payload?: TPayload;
}

/** Builds a command, deriving ids and the revision precondition. */
export function buildCommand<TPayload = Record<string, unknown>>(
  params: BuildCommandParams<TPayload>
): AgentCommand<TPayload> {
  const snapshot = params.snapshot ?? undefined;
  const command: AgentCommand<TPayload> = {
    commandId: params.commandId ?? createCommandId(),
    type: params.type,
  };
  const threadId = params.threadId ?? snapshot?.currentThreadId;
  const runId = params.runId ?? snapshot?.activeRun?.runId;
  const expectedRevision = params.expectedRevision ?? snapshot?.revision;

  if (threadId) command.threadId = threadId;
  if (runId) command.runId = runId;
  if (params.approvalId) command.approvalId = params.approvalId;
  if (typeof params.pauseEpoch === "number") {
    command.pauseEpoch = params.pauseEpoch;
  }
  if (typeof expectedRevision === "number" && expectedRevision > 0) {
    command.expectedRevision = expectedRevision;
  }
  if (params.payload !== undefined) command.payload = params.payload;
  return command;
}

export interface ExecuteCommandOptions extends RequestOptions {
  /**
   * Called when the server rejects the command's revision precondition and
   * returns the authoritative snapshot. Reconciling to it is the recovery;
   * blind retrying is not.
   */
  onReconcile?: (snapshot: AgentStateSnapshot) => void;
  /**
   * How many times to retry a *recoverable* failure with the SAME command id.
   * Reusing the id is what makes the retry safe — the server will not apply
   * the command twice.
   */
  maxRetries?: number;
  sleep?: (ms: number) => Promise<void>;
}

const wait = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/**
 * Sends one command, retrying recoverable failures under the same id and
 * surfacing a conflict's authoritative snapshot instead of swallowing it.
 */
export async function executeCommand(
  transport: IAgentSessionTransport,
  command: AgentCommand,
  options: ExecuteCommandOptions = {}
): Promise<AgentCommandResult> {
  const {
    onReconcile,
    maxRetries = 2,
    sleep = wait,
    ...requestOptions
  } = options;

  let attempt = 0;
  for (;;) {
    try {
      return await transport.executeCommand(command, requestOptions);
    } catch (error) {
      if (!isAgentTransportError(error)) throw error;

      if (error.snapshot && onReconcile) onReconcile(error.snapshot);

      // A stale revision is not a transport problem: the state moved, and
      // the caller must decide what the command means against the new state.
      if (error.code === AgentErrorCodes.StateRevisionMismatch) throw error;
      // Reusing an id with different content is never retryable.
      if (error.code === AgentErrorCodes.IdempotencyKeyReused) throw error;
      // A terminal run cannot be reopened by any number of retries.
      if (error.code === AgentErrorCodes.RunTerminal) throw error;

      attempt++;
      if (!error.recoverable || attempt > maxRetries) throw error;
      await sleep(
        error.retryAfterMs ?? Math.min(2000, 250 * 2 ** (attempt - 1))
      );
    }
  }
}
