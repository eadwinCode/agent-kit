/**
 * Snapshot-plus-tail hydration.
 *
 * The problem this solves: live delivery is best-effort. A socket drops, a
 * tab is opened mid-turn, a phone comes back from sleep, the tab that started
 * the run was closed an hour ago. In all of those cases the client has to
 * rebuild the transcript from the server without the user seeing a frozen
 * assistant or a duplicated message.
 *
 * The algorithm, in order:
 *
 *  1. Start buffering live envelopes BEFORE fetching anything. Events that
 *     arrive during hydration are the ones most easily lost.
 *  2. Fetch the snapshot: authoritative state, canonical history, and the
 *     cursor to tail from.
 *  3. Replace history from the snapshot. This is a replace, not a merge —
 *     canonical history is the truth about completed content.
 *  4. Drain the durable tail from the cursor, reducing in
 *     (streamEpoch, sequenceNumber, eventId) order.
 *  5. Merge the buffer by stable event id. Drop exact duplicates and stale
 *     epochs; never drop a *newer* event just because it raced hydration.
 *  6. Go live. On a sequence gap, buffer what is ahead and backfill the hole.
 *     A bounded timer means a gap that never fills becomes a re-snapshot
 *     instead of a permanently stuck UI.
 *
 * This module is transport-agnostic and framework-agnostic on purpose: it is
 * the package-owned recovery path, so applications never write a second
 * reducer to do it.
 */

import type {
  AgentStateSnapshot,
  StandardEventEnvelope,
  AgentKitMessage,
} from "../../types/index.js";
import type {
  IAgentSessionTransport,
  StreamCursor,
  EventTailPage,
} from "../ports/agent-session.js";
import { STREAM_START } from "../ports/agent-session.js";
import {
  AgentErrorCodes,
  isAgentTransportError,
} from "../errors/agent-transport-error.js";

/** Why a hydration attempt ended. */
export type HydrationOutcome = "hydrated" | "restarted" | "aborted" | "failed";

export interface HydrationResult {
  outcome: HydrationOutcome;
  snapshot?: AgentStateSnapshot;
  /** Canonical history the caller should install, replacing what it had. */
  messages?: AgentKitMessage[];
  /** Events to reduce, already ordered and deduplicated. */
  events: StandardEventEnvelope[];
  /** Where live delivery should continue from. */
  cursor: StreamCursor | null;
  error?: unknown;
  /**
   * The server reported its durable tail has holes. Completed content is
   * still correct — it comes from canonical history — but backfill cannot be
   * trusted to fill everything.
   */
  reconcileRequired: boolean;
}

export interface HydrationOptions {
  transport: IAgentSessionTransport;
  /**
   * The client's own last-applied cursor, when it has one.
   *
   * A reconnecting client must resume from what IT already applied, not from
   * where the server thinks a newcomer should start — otherwise it replays
   * the whole turn and re-reduces events it already has. A fresh client
   * passes nothing and takes the snapshot's cursor, which points at the start
   * of the active epoch so the replay begins with run.started.
   */
  from?: StreamCursor | null;
  /** Abort signal; hydration checks it between every step. */
  signal?: AbortSignal;
  /** Page size for tail reads. */
  tailLimit?: number;
  /**
   * Maximum snapshot restarts before giving up. A run whose active run keeps
   * changing under us would otherwise loop.
   */
  maxRestarts?: number;
}

/** Ordering key for an envelope: epoch first, then sequence, then id. */
export function envelopeOrder(
  envelope: StandardEventEnvelope
): [number, number, string] {
  return [
    envelope.streamEpoch ?? 0,
    envelope.sequenceNumber ?? 0,
    envelope.eventId ?? envelope.id ?? "",
  ];
}

/** Sorts envelopes into the canonical reduce order. */
export function sortEnvelopes(
  envelopes: StandardEventEnvelope[]
): StandardEventEnvelope[] {
  return [...envelopes].sort((a, b) => {
    const [ae, as, ai] = envelopeOrder(a);
    const [be, bs, bi] = envelopeOrder(b);
    if (ae !== be) return ae - be;
    if (as !== bs) return as - bs;
    return ai < bi ? -1 : ai > bi ? 1 : 0;
  });
}

/** The dedupe identity of an envelope. */
export function envelopeKey(envelope: StandardEventEnvelope): string {
  if (envelope.eventId) return envelope.eventId;
  // Pre-epoch runtimes have no event id; the epoch/sequence pair is still
  // unique within a run, and the run id lives in the payload.
  const runId =
    typeof envelope.data?.runId === "string" ? envelope.data.runId : "";
  return `${runId}:${envelope.streamEpoch ?? 0}:${envelope.sequenceNumber}`;
}

/**
 * Buffers live envelopes while hydration runs and deduplicates them against
 * everything already applied.
 *
 * The buffer exists because the alternative — subscribing only after the
 * snapshot returns — loses every event produced during the fetch, and those
 * are exactly the events of the turn the user is watching.
 */
export class LiveEventBuffer {
  private buffered: StandardEventEnvelope[] = [];
  private applied = new Set<string>();
  private buffering = true;

  /** Accepts a live envelope. Returns true when it was buffered. */
  push(envelope: StandardEventEnvelope): boolean {
    if (!this.buffering) return false;
    this.buffered.push(envelope);
    return true;
  }

  /** Records envelopes that have been reduced, so duplicates are dropped. */
  markApplied(envelopes: StandardEventEnvelope[]): void {
    for (const envelope of envelopes) this.applied.add(envelopeKey(envelope));
  }

  /** True when this envelope was already reduced. */
  isApplied(envelope: StandardEventEnvelope): boolean {
    return this.applied.has(envelopeKey(envelope));
  }

  /**
   * Ends buffering and returns the envelopes that still need reducing:
   * ordered, deduplicated against what was applied, and with events from
   * superseded epochs dropped.
   */
  drain(currentEpoch: number): StandardEventEnvelope[] {
    this.buffering = false;
    const pending = sortEnvelopes(this.buffered).filter((envelope) => {
      if (this.isApplied(envelope)) return false;
      // A stale epoch belongs to a run this client already replaced. Applying
      // it would resurrect content the server has moved past.
      if ((envelope.streamEpoch ?? 0) < currentEpoch) return false;
      return true;
    });
    this.buffered = [];
    this.markApplied(pending);
    return pending;
  }

  /** Restarts buffering for another hydration attempt. */
  reset(): void {
    this.buffering = true;
    this.buffered = [];
    this.applied.clear();
  }

  get size(): number {
    return this.buffered.length;
  }
}

function aborted(signal?: AbortSignal): boolean {
  return Boolean(signal?.aborted);
}

/**
 * Runs the snapshot-plus-tail algorithm once, restarting when the server's
 * active run changes underneath it.
 *
 * `buffer` must already be collecting live envelopes when this is called.
 */
export async function hydrateAgentSession(
  options: HydrationOptions,
  buffer: LiveEventBuffer
): Promise<HydrationResult> {
  const { transport, signal, from, tailLimit = 200, maxRestarts = 3 } = options;

  let restarts = 0;

  for (;;) {
    if (aborted(signal)) {
      return {
        outcome: "aborted",
        events: [],
        cursor: null,
        reconcileRequired: false,
      };
    }

    let state;
    try {
      state = await transport.fetchAgentState({}, { signal });
    } catch (error) {
      return {
        outcome: "failed",
        events: [],
        cursor: null,
        reconcileRequired: false,
        error,
      };
    }

    if (aborted(signal)) {
      return {
        outcome: "aborted",
        events: [],
        cursor: null,
        reconcileRequired: false,
      };
    }

    const snapshot = state.snapshot;
    const serverCursor = state.cursor ?? snapshot.cursor ?? null;
    // The client's own position wins, but only within the same run and epoch.
    // Sequence numbers and epochs are local to a run, so comparing the epoch
    // alone can accidentally reuse a cursor from a completed run when its
    // successor also starts at epoch zero.
    const startCursor =
      from &&
      serverCursor &&
      from.runId === serverCursor.runId &&
      from.streamEpoch === serverCursor.streamEpoch
        ? from
        : serverCursor;
    const epoch = startCursor?.streamEpoch ?? 0;

    const tail: StandardEventEnvelope[] = [];
    let reconcileRequired = Boolean(snapshot.reconcileRequired);
    let cursor: StreamCursor | null = startCursor;

    if (startCursor && snapshot.activeRun) {
      const drained = await drainTail(
        transport,
        snapshot.currentThreadId ?? "",
        startCursor,
        tailLimit,
        signal
      );
      if (drained.aborted) {
        return {
          outcome: "aborted",
          events: [],
          cursor: null,
          reconcileRequired,
        };
      }
      if (drained.retentionGap) {
        // The tail cannot be completed. Canonical history from the snapshot
        // still holds every completed message, so this is a degraded success,
        // not a failure — but the caller must not trust backfill afterwards.
        reconcileRequired = true;
      }
      if (drained.error && !drained.retentionGap) {
        return {
          outcome: "failed",
          events: [],
          cursor: null,
          reconcileRequired,
          error: drained.error,
        };
      }
      tail.push(...drained.events);
      cursor = drained.cursor;
    }

    // If the active run changed while we were fetching, the snapshot and the
    // tail describe two different runs. Restart rather than stitch them.
    if (
      restarts < maxRestarts &&
      (await activeRunChanged(transport, snapshot, signal))
    ) {
      restarts++;
      buffer.reset();
      continue;
    }

    const ordered = sortEnvelopes(tail).filter((envelope) => {
      if (buffer.isApplied(envelope)) return false;
      return (envelope.streamEpoch ?? 0) >= epoch;
    });
    buffer.markApplied(ordered);

    // Merge whatever arrived live during the fetch. A newer event that raced
    // hydration must be kept: dropping it is how a transcript ends up missing
    // the last few deltas of a turn.
    const buffered = buffer.drain(epoch);
    const events = sortEnvelopes([...ordered, ...buffered]);
    const last = events[events.length - 1];
    if (last) {
      cursor = {
        runId:
          cursor?.runId ??
          (typeof last.data?.runId === "string" ? last.data.runId : ""),
        streamEpoch: last.streamEpoch ?? epoch,
        sequenceNumber: last.sequenceNumber,
      };
    }

    return {
      outcome: restarts > 0 ? "restarted" : "hydrated",
      snapshot,
      messages: state.messages,
      events,
      cursor,
      reconcileRequired,
    };
  }
}

interface DrainResult {
  events: StandardEventEnvelope[];
  cursor: StreamCursor;
  retentionGap: boolean;
  aborted: boolean;
  error?: unknown;
}

/** Reads tail pages until the server reports no more. */
async function drainTail(
  transport: IAgentSessionTransport,
  threadId: string,
  from: StreamCursor,
  limit: number,
  signal?: AbortSignal
): Promise<DrainResult> {
  const events: StandardEventEnvelope[] = [];
  let cursor = from;

  for (;;) {
    if (aborted(signal)) {
      return { events, cursor, retentionGap: false, aborted: true };
    }
    let page: EventTailPage;
    try {
      page = await transport.fetchEventTail(
        { threadId, after: cursor, limit },
        { signal }
      );
    } catch (error) {
      if (
        isAgentTransportError(error) &&
        error.code === AgentErrorCodes.RetentionGap
      ) {
        return { events, cursor, retentionGap: true, aborted: false, error };
      }
      return { events, cursor, retentionGap: false, aborted: false, error };
    }

    events.push(...page.events);
    if (page.retentionGap) {
      return {
        events,
        cursor: page.next ?? cursor,
        retentionGap: true,
        aborted: false,
      };
    }
    const advanced = page.next && !sameCursor(page.next, cursor);
    cursor = page.next ?? cursor;
    if (!page.hasMore || page.events.length === 0 || !advanced) {
      return { events, cursor, retentionGap: false, aborted: false };
    }
  }
}

function sameCursor(a: StreamCursor, b: StreamCursor): boolean {
  return (
    a.runId === b.runId &&
    a.streamEpoch === b.streamEpoch &&
    a.sequenceNumber === b.sequenceNumber
  );
}

/**
 * Re-reads the snapshot to see whether the active run moved while we were
 * draining. Cheap compared to reducing a tail that belongs to a dead run.
 */
async function activeRunChanged(
  transport: IAgentSessionTransport,
  before: AgentStateSnapshot,
  signal?: AbortSignal
): Promise<boolean> {
  if (!before.activeRun) return false;
  try {
    const after = await transport.fetchAgentState(
      { knownRevision: before.revision },
      { signal }
    );
    const beforeRun = before.activeRun?.runId;
    const afterRun = after.snapshot.activeRun?.runId;
    return beforeRun !== afterRun;
  } catch {
    // A failed re-check is not evidence of a change; proceed with what we
    // have rather than restarting into the same failure.
    return false;
  }
}

/** A gap in the live sequence that is waiting on a backfill. */
export interface SequenceGap {
  runId: string;
  streamEpoch: number;
  /** Last sequence number successfully applied. */
  after: number;
  /** Lowest sequence number waiting behind the gap. */
  waitingOn: number;
  detectedAt: number;
}

export interface GapTrackerOptions {
  /**
   * How long a gap may stay unfilled before the tracker gives up and asks
   * for a full re-snapshot. Without this bound, one missing sequence number
   * freezes the transcript forever — which is the failure this whole module
   * exists to prevent.
   */
  timeoutMs?: number;
  now?: () => number;
  /** Upper bound on remembered event ids used for replay dedupe. */
  maxAppliedIds?: number;
}

/** What the caller should do about the current live stream. */
export type GapAction =
  | { type: "apply"; events: StandardEventEnvelope[] }
  | { type: "backfill"; gap: SequenceGap }
  | { type: "resnapshot"; reason: "gap-timeout" | "epoch-changed" };

/**
 * Tracks live sequence continuity and decides between applying, backfilling
 * and re-snapshotting.
 *
 * Strict contiguous buffering with no escape hatch is the bug this replaces:
 * one dropped sequence number and every later event waits forever.
 */
export class SequenceGapTracker {
  private expected: number | null = null;
  private epoch = 0;
  private runId = "";
  private pending = new Map<number, StandardEventEnvelope>();
  private gap: SequenceGap | null = null;
  // Durable executors re-publish already-journaled events when a replay's
  // allocation order drifts; the replayed copy then carries a FRESH sequence
  // number and slips past the sequence checks below. The event id is the only
  // replay-stable identity the envelope has, so steady-state dedupe keys on
  // it. Bounded FIFO: the set only has to outlive a replay window, not the run.
  private appliedIds = new Set<string>();
  private appliedOrder: string[] = [];
  private readonly maxAppliedIds: number;
  private readonly timeoutMs: number;
  private readonly now: () => number;

  constructor(options: GapTrackerOptions = {}) {
    this.timeoutMs = options.timeoutMs ?? 5000;
    this.now = options.now ?? (() => Date.now());
    this.maxAppliedIds = options.maxAppliedIds ?? 2000;
  }

  /** Positions the tracker after hydration. */
  reset(cursor: StreamCursor | null, applied?: StandardEventEnvelope[]): void {
    this.pending.clear();
    this.gap = null;
    this.appliedIds.clear();
    this.appliedOrder = [];
    // Seed the replay-dedupe set with everything hydration just reduced.
    // Without this, a durable-executor replay whose drifted allocation gives
    // the already-journaled prefix FRESH sequence numbers slips past the
    // sequence checks — the event id is the only replay-stable identity, and
    // a refreshed tab has no other memory of what the tail installed.
    if (applied) this.markApplied(applied);
    if (!cursor) {
      this.expected = null;
      this.epoch = 0;
      this.runId = "";
      return;
    }
    this.epoch = cursor.streamEpoch;
    this.runId = cursor.runId;
    this.expected =
      cursor.sequenceNumber === STREAM_START ? 0 : cursor.sequenceNumber + 1;
  }

  /** The gap currently awaiting backfill, if any. */
  get currentGap(): SequenceGap | null {
    return this.gap;
  }

  private alreadyApplied(envelope: StandardEventEnvelope): boolean {
    const id = envelope.eventId ?? envelope.id;
    return typeof id === "string" && id !== "" && this.appliedIds.has(id);
  }

  private markApplied(events: StandardEventEnvelope[]): void {
    for (const envelope of events) {
      const id = envelope.eventId ?? envelope.id;
      if (typeof id !== "string" || id === "" || this.appliedIds.has(id)) {
        continue;
      }
      this.appliedIds.add(id);
      this.appliedOrder.push(id);
    }
    while (this.appliedOrder.length > this.maxAppliedIds) {
      const evicted = this.appliedOrder.shift();
      if (evicted !== undefined) this.appliedIds.delete(evicted);
    }
  }

  /**
   * Offers one envelope. Returns what the caller should do next.
   */
  accept(envelope: StandardEventEnvelope): GapAction {
    const epoch = envelope.streamEpoch ?? 0;

    if (this.alreadyApplied(envelope)) {
      // A durable-executor replay re-published an event this client already
      // reduced; drifted numbering means the sequence checks below cannot
      // recognize it, but the replay-stable event id can.
      return { type: "apply", events: [] };
    }

    if (this.expected === null) {
      this.epoch = epoch;
      this.runId =
        typeof envelope.data?.runId === "string" ? envelope.data.runId : "";
      this.expected = envelope.sequenceNumber + 1;
      this.markApplied([envelope]);
      return { type: "apply", events: [envelope] };
    }

    if (epoch > this.epoch) {
      // A new epoch supersedes everything buffered: the old sequence numbers
      // will never arrive, so waiting for them is waiting forever.
      this.epoch = epoch;
      this.pending.clear();
      this.gap = null;
      this.appliedIds.clear();
      this.appliedOrder = [];
      this.expected = envelope.sequenceNumber + 1;
      this.markApplied([envelope]);
      return { type: "apply", events: [envelope] };
    }

    if (epoch < this.epoch) {
      // Stale epoch: the run it belongs to has been replaced.
      return { type: "apply", events: [] };
    }

    if (envelope.sequenceNumber < this.expected) {
      // Already applied, or a duplicate delivery. Reducers are idempotent on
      // part ids, but suppressing here keeps the ordering invariant honest.
      return { type: "apply", events: [] };
    }

    if (envelope.sequenceNumber === this.expected) {
      const ready = [envelope];
      this.expected++;
      // Drain anything that was waiting behind this one.
      while (this.pending.has(this.expected)) {
        ready.push(this.pending.get(this.expected)!);
        this.pending.delete(this.expected);
        this.expected++;
      }
      if (this.gap && this.expected > this.gap.waitingOn) this.gap = null;
      this.markApplied(ready);
      return { type: "apply", events: ready };
    }

    // Ahead of what we expect: hold it and ask for the missing range.
    this.pending.set(envelope.sequenceNumber, envelope);
    const waitingOn = Math.min(...this.pending.keys());
    if (!this.gap) {
      this.gap = {
        runId: this.runId,
        streamEpoch: this.epoch,
        after: this.expected - 1,
        waitingOn,
        detectedAt: this.now(),
      };
      return { type: "backfill", gap: this.gap };
    }

    if (this.now() - this.gap.detectedAt >= this.timeoutMs) {
      // The backfill never closed the hole. A fresh snapshot always can.
      this.pending.clear();
      this.gap = null;
      return { type: "resnapshot", reason: "gap-timeout" };
    }
    return { type: "backfill", gap: this.gap };
  }

  /**
   * Applies backfilled events, then returns everything now contiguous.
   */
  fill(events: StandardEventEnvelope[]): StandardEventEnvelope[] {
    for (const envelope of sortEnvelopes(events)) {
      if ((envelope.streamEpoch ?? 0) !== this.epoch) continue;
      if (this.expected !== null && envelope.sequenceNumber < this.expected) {
        continue;
      }
      this.pending.set(envelope.sequenceNumber, envelope);
    }
    if (this.expected === null) return [];

    const ready: StandardEventEnvelope[] = [];
    while (this.pending.has(this.expected)) {
      ready.push(this.pending.get(this.expected)!);
      this.pending.delete(this.expected);
      this.expected++;
    }
    if (this.gap && this.expected > this.gap.waitingOn) this.gap = null;
    this.markApplied(ready);
    return ready;
  }

  /** True when a gap has outlived its timeout. */
  isStale(): boolean {
    return Boolean(
      this.gap && this.now() - this.gap.detectedAt >= this.timeoutMs
    );
  }
}
