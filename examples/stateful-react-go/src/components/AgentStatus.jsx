const ACTIVE_CONNECTION_STATES = new Set(["connecting", "reconnecting"]);

const ACTIVITY_COPY = {
  none: "Working",
  preparing: "Starting",
  thinking: "Thinking",
  responding: "Responding",
  reading: "Reading",
  writing: "Writing",
  tool: "Using a tool",
  waiting_external: "Waiting for an external result",
  finalizing: "Finalizing",
};

const TERMINAL_COPY = {
  "run.completed": { phase: "completed", label: "Completed" },
  "run.failed": { phase: "failed", label: "Run failed" },
  "run.interrupted": { phase: "cancelled", label: "Cancelled" },
};

export function eventBelongsToRun(event, snapshot) {
  const runId = snapshot?.activeRun?.runId;
  if (!runId) return false;
  if (event.data?.runId === runId || event.data?.parentRunId === runId) {
    return true;
  }
  // Agent sub-runs have their own data.runId, but they are still members of
  // the active network production stream. Epoch is the shared identity that
  // safely correlates their gapless sequence numbers.
  return (
    typeof event.streamEpoch === "number" &&
    event.streamEpoch === snapshot.cursor?.streamEpoch
  );
}

function statusEventsForRun(events, snapshot) {
  return events.filter(
    (event) =>
      event.event === "status.updated" && eventBelongsToRun(event, snapshot)
  );
}

function activityEventsForRun(events, snapshot) {
  return events.filter(
    (event) =>
      eventBelongsToRun(event, snapshot) &&
      ["status.updated", "reasoning.delta", "text.delta"].includes(event.event)
  );
}

function eventDescription(event) {
  if (event.event === "reasoning.delta") return ACTIVITY_COPY.thinking;
  if (event.event === "text.delta") return ACTIVITY_COPY.responding;
  const { kind, label, toolName, completed, total } = event.data ?? {};
  if (completed !== undefined) {
    const item = label || toolName || "Working";
    return total ? `${item} (${completed}/${total})` : item;
  }
  return label || ACTIVITY_COPY[kind] || toolName || null;
}

function recentTransitions(statusEvents) {
  const descriptions = statusEvents.map(eventDescription).filter(Boolean);
  const distinct = descriptions.filter(
    (description, index) => description !== descriptions[index - 1]
  );
  return distinct.slice(-4);
}

function latestTerminal(events) {
  return [...events].reverse().find((event) => TERMINAL_COPY[event.event]);
}

export function deriveAgentStatus({
  snapshot,
  sessionStatus,
  connectionState,
  events = [],
  error,
}) {
  const activeRun = snapshot?.activeRun;
  const statusEvents = statusEventsForRun(events, snapshot);
  const activityEvents = activityEventsForRun(events, snapshot);
  const latestActivityEvent = activityEvents.at(-1);
  const transitions = recentTransitions(activityEvents);

  if (error) {
    return {
      phase: "failed",
      label: "Needs attention",
      detail: error.message || "The session reported an error",
      transitions,
    };
  }
  if (sessionStatus === "hydrating") {
    return {
      phase: "starting",
      label: "Restoring session",
      detail: "Loading authoritative state and missed events",
      transitions,
    };
  }
  if (activeRun && ACTIVE_CONNECTION_STATES.has(connectionState)) {
    return {
      phase: "waiting",
      label: "Reconnecting live updates",
      detail: "The durable journal will fill anything missed",
      transitions,
    };
  }
  if (activeRun && connectionState === "disconnected") {
    return {
      phase: "waiting",
      label: "Live updates disconnected",
      detail: "The run continues safely on the server",
      transitions,
    };
  }
  if (activeRun && snapshot.approval?.status === "pending") {
    return {
      phase: "waiting",
      label: "Waiting for your approval",
      detail: "The run is parked until you approve or deny",
      transitions,
    };
  }
  if (activeRun && snapshot.pause?.state === "requested") {
    return {
      phase: "pausing",
      label: "Pausing at the next safe point",
      detail: snapshot.checkpointKind
        ? `Last checkpoint: ${snapshot.checkpointKind}`
        : "Current work will not be interrupted halfway through",
      transitions,
    };
  }
  if (activeRun && snapshot.pause?.state === "paused") {
    return {
      phase: "paused",
      label: "Paused",
      detail: snapshot.checkpointKind
        ? `Safe checkpoint: ${snapshot.checkpointKind}`
        : "The run is safely parked",
      transitions,
    };
  }
  if (activeRun && snapshot.pause?.state === "resuming") {
    return {
      phase: "starting",
      label: "Resuming",
      detail: "Restoring the saved checkpoint",
      transitions,
    };
  }
  if (activeRun?.lifecycle === "terminalizing") {
    return {
      phase: "finalizing",
      label: "Finalizing",
      detail: "Saving canonical history and settling the run",
      transitions,
    };
  }

  if (activeRun) {
    const activity = snapshot.activity ?? { kind: "preparing" };
    const progress =
      latestActivityEvent?.event === "status.updated" &&
      latestActivityEvent.data?.completed !== undefined
        ? {
            completed: latestActivityEvent.data.completed,
            total: latestActivityEvent.data.total,
            percent: latestActivityEvent.data.total
              ? Math.min(
                  100,
                  Math.round(
                    (latestActivityEvent.data.completed /
                      latestActivityEvent.data.total) *
                      100
                  )
                )
              : null,
          }
        : null;
    const mostRecentStatusKind = [...statusEvents]
      .reverse()
      .find((event) => event.data?.kind)?.data?.kind;
    let kind = activity.kind || "preparing";
    let liveLabel;
    let liveDetail;
    if (latestActivityEvent?.event === "reasoning.delta") {
      // A provider-returned reasoning delta is direct evidence of thinking.
      kind = "thinking";
      liveLabel = ACTIVITY_COPY.thinking;
      liveDetail = "Reported by provider";
    } else if (latestActivityEvent?.event === "text.delta") {
      kind = "responding";
      liveLabel = ACTIVITY_COPY.responding;
      liveDetail = "Streaming a response";
    } else if (latestActivityEvent?.event === "status.updated") {
      kind =
        latestActivityEvent.data?.kind ||
        mostRecentStatusKind ||
        (latestActivityEvent.data?.toolName ? "tool" : kind);
      liveLabel = latestActivityEvent.data?.label;
      liveDetail = latestActivityEvent.data?.toolName;
    }
    return {
      phase: kind,
      label: liveLabel || activity.label || ACTIVITY_COPY[kind],
      detail:
        liveDetail ||
        (activity.source
          ? `Reported by ${activity.source}`
          : "Run in progress"),
      progress,
      transitions,
    };
  }

  const terminal = latestTerminal(events);
  if (terminal) {
    return { ...TERMINAL_COPY[terminal.event], transitions: [] };
  }
  return {
    phase: connectionState === "disconnected" ? "offline" : "idle",
    label: connectionState === "disconnected" ? "Live updates off" : "Ready",
    detail: "Send a message to start a run",
    transitions: [],
  };
}

export function AgentStatus({
  snapshot,
  sessionStatus,
  connectionState,
  events,
  error,
}) {
  const status = deriveAgentStatus({
    snapshot,
    sessionStatus,
    connectionState,
    events,
    error,
  });
  const active = Boolean(snapshot?.activeRun);

  return (
    <section className={`agent-status phase-${status.phase}`}>
      <div
        className="agent-status-main"
        role="status"
        aria-live="polite"
        aria-atomic="true"
      >
        <span className="agent-status-indicator" aria-hidden="true">
          <span />
        </span>
        <div>
          <p className="agent-status-kicker">Agent status</p>
          <strong>{status.label}</strong>
          {status.detail && <p>{status.detail}</p>}
        </div>
        {active && snapshot?.activeRun?.acceptedAt && (
          <time dateTime={snapshot.activeRun.acceptedAt}>
            {snapshot.activeRun.lifecycle}
          </time>
        )}
      </div>

      {status.progress && (
        <div className="agent-progress">
          <div>
            <span>Progress</span>
            <span>
              {status.progress.completed}
              {status.progress.total ? ` / ${status.progress.total}` : ""}
            </span>
          </div>
          {status.progress.percent !== null && (
            <progress max="100" value={status.progress.percent}>
              {status.progress.percent}%
            </progress>
          )}
        </div>
      )}

      {status.transitions.length > 0 && (
        <ol className="agent-transitions" aria-label="Recent agent activity">
          {status.transitions.map((transition, index) => (
            <li
              className={
                index === status.transitions.length - 1 ? "current" : "done"
              }
              key={`${transition}-${index}`}
            >
              <span aria-hidden="true" />
              {transition}
            </li>
          ))}
        </ol>
      )}
    </section>
  );
}
