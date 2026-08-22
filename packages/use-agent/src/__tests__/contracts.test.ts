import { describe, it, expect } from "vitest";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { reduceStreamingState } from "../core/index.js";
import { sortEnvelopes } from "../core/services/hydration.js";
import type {
  StreamingState,
  StreamingAction,
  ToolManifest,
  AgentKitEvent,
  StandardEventEnvelope,
  MessagePart,
} from "../types/index.js";

/**
 * Cross-runtime contract tests.
 *
 * These reduce the SAME golden files the Go runtime generates
 * (contracts/fixtures, produced by go/contracts_fixture_test.go). That is the
 * point: the wire protocol is a contract between two independently-versioned
 * runtimes, and the only way to keep them honest is to have both assert
 * against one artifact. If the Go side changes an event's shape without
 * regenerating, its own test fails; if it regenerates without the client
 * agreeing, this one does.
 *
 * The schemas in contracts/schemas are checked here too, with a small
 * validator rather than a dependency — a schema nothing validates is prose.
 */

const CONTRACTS_DIR = join(__dirname, "../../../../contracts");
const FIXTURES_DIR = join(CONTRACTS_DIR, "fixtures");
const SCHEMAS_DIR = join(CONTRACTS_DIR, "schemas");

interface FixtureFile {
  name: string;
  description: string;
  schemaVersion: number;
  events: StandardEventEnvelope[];
}

function loadFixtures(): FixtureFile[] {
  return readdirSync(FIXTURES_DIR)
    .filter((f) => f.endsWith(".json"))
    .sort()
    .map((f) => JSON.parse(readFileSync(join(FIXTURES_DIR, f), "utf8")) as FixtureFile);
}

// ---------------------------------------------------------------------------
// Minimal JSON Schema validator covering the subset the frozen schemas use.
// ---------------------------------------------------------------------------

type Json = unknown;
interface SchemaNode {
  type?: string;
  const?: Json;
  enum?: Json[];
  required?: string[];
  properties?: Record<string, SchemaNode>;
  additionalProperties?: boolean;
  items?: SchemaNode;
  oneOf?: SchemaNode[];
  minimum?: number;
  minLength?: number;
}

function typeOf(value: Json): string {
  if (value === null) return "null";
  if (Array.isArray(value)) return "array";
  if (typeof value === "number") {
    return Number.isInteger(value) ? "integer" : "number";
  }
  return typeof value;
}

function typeMatches(want: string, value: Json): boolean {
  const actual = typeOf(value);
  if (want === "number") return actual === "number" || actual === "integer";
  return actual === want;
}

function validate(schema: SchemaNode, value: Json, path = "$"): string[] {
  const errors: string[] = [];

  if (schema.oneOf) {
    const matched = schema.oneOf.filter((alt) => validate(alt, value, path).length === 0);
    if (matched.length !== 1) {
      errors.push(`${path}: matched ${matched.length} oneOf alternatives, want 1`);
      return errors;
    }
  }

  if (schema.type && !typeMatches(schema.type, value)) {
    errors.push(`${path}: type is ${typeOf(value)}, want ${schema.type}`);
    return errors;
  }

  if (schema.const !== undefined && value !== schema.const) {
    errors.push(`${path}: ${String(value)} !== const ${String(schema.const)}`);
  }

  if (schema.enum && !schema.enum.includes(value as never)) {
    errors.push(`${path}: ${String(value)} is not one of the allowed values`);
  }

  if (typeOf(value) === "object") {
    const object = value as Record<string, Json>;
    for (const key of schema.required ?? []) {
      if (!(key in object)) errors.push(`${path}: missing required property "${key}"`);
    }
    for (const [key, child] of Object.entries(object)) {
      const childSchema = schema.properties?.[key];
      if (childSchema) {
        errors.push(...validate(childSchema, child, `${path}.${key}`));
      } else if (schema.additionalProperties === false) {
        errors.push(`${path}: unexpected property "${key}"`);
      }
    }
  }

  if (Array.isArray(value) && schema.items) {
    value.forEach((item, i) => {
      errors.push(...validate(schema.items!, item, `${path}[${i}]`));
    });
  }

  if (typeof value === "number" && schema.minimum !== undefined && value < schema.minimum) {
    errors.push(`${path}: ${value} < minimum ${schema.minimum}`);
  }
  if (typeof value === "string" && schema.minLength !== undefined && value.length < schema.minLength) {
    errors.push(`${path}: length ${value.length} < ${schema.minLength}`);
  }

  return errors;
}

function loadSchema(name: string): SchemaNode {
  return JSON.parse(readFileSync(join(SCHEMAS_DIR, name), "utf8")) as SchemaNode;
}

// ---------------------------------------------------------------------------

type TestManifest = ToolManifest;

function emptyState(threadId: string): StreamingState<TestManifest> {
  return {
    threads: {},
    currentThreadId: threadId,
    lastProcessedIndex: 0,
    isConnected: true,
  } as StreamingState<TestManifest>;
}

/** Reduces a fixture's events exactly as the live path would. */
function reduceFixture(fixture: FixtureFile): StreamingState<TestManifest> {
  const threadId =
    (fixture.events.find((e) => typeof e.data?.threadId === "string")?.data
      .threadId as string) ?? "thread_1";
  let state = emptyState(threadId);
  for (const event of sortEnvelopes(fixture.events)) {
    state = reduceStreamingState(
      state,
      {
        type: "REALTIME_MESSAGES_RECEIVED",
        messages: [event as unknown as AgentKitEvent<TestManifest>],
      } as StreamingAction<TestManifest>,
      false
    );
  }
  return state;
}

function partsOf(state: StreamingState<TestManifest>, threadId: string): MessagePart[] {
  const thread = state.threads[threadId];
  if (!thread) return [];
  return thread.messages.flatMap((m) => m.parts ?? []);
}

const fixtures = loadFixtures();

describe("frozen standard-event fixtures", () => {
  it("exist", () => {
    expect(fixtures.length).toBeGreaterThan(0);
  });

  const eventSchema = loadSchema("standard-event.schema.json");

  for (const fixture of fixtures) {
    describe(fixture.name, () => {
      it("matches the frozen envelope schema", () => {
        const problems = fixture.events.flatMap((event, i) =>
          validate(eventSchema, event, `$.events[${i}]`)
        );
        expect(problems).toEqual([]);
      });

      it("carries replay identity on every envelope", () => {
        const ids = new Set<string>();
        for (const event of fixture.events) {
          expect(event.eventId, `${fixture.name}: ${event.event} has no eventId`).toBeTruthy();
          expect(ids.has(event.eventId!)).toBe(false);
          ids.add(event.eventId!);
          expect(event.schemaVersion).toBe(1);
          expect(typeof event.streamEpoch).toBe("number");
        }
      });

      it("has gapless sequence numbers within its epoch", () => {
        // A gap here would mean the runtime emitted an event the client can
        // never account for, and the gap tracker would stall waiting for it.
        const byEpoch = new Map<number, number[]>();
        for (const event of fixture.events) {
          const epoch = event.streamEpoch ?? 0;
          byEpoch.set(epoch, [...(byEpoch.get(epoch) ?? []), event.sequenceNumber]);
        }
        for (const [epoch, sequences] of byEpoch) {
          const sorted = [...sequences].sort((a, b) => a - b);
          expect(sorted[0], `epoch ${epoch} does not start at 0`).toBe(0);
          for (let i = 1; i < sorted.length; i++) {
            expect(sorted[i], `epoch ${epoch} has a gap before ${sorted[i]}`).toBe(
              sorted[i - 1] + 1
            );
          }
        }
      });

      it("closes or fails every part it opens", () => {
        // created -> delta(s) -> completed is what lets the client render a
        // part progressively without ever leaving one spinning forever.
        const open = new Set<string>();
        for (const event of fixture.events) {
          const partId = event.data?.partId as string | undefined;
          if (!partId) continue;
          if (event.event === "part.created") {
            expect(open.has(partId), `${partId} opened twice`).toBe(false);
            open.add(partId);
          } else if (event.event === "part.completed" || event.event === "part.failed") {
            expect(open.has(partId), `${partId} closed without being opened`).toBe(true);
            open.delete(partId);
          } else if (event.event.endsWith(".delta")) {
            expect(open.has(partId), `delta for unopened part ${partId}`).toBe(true);
          }
        }
        expect([...open], "parts left open at the end of the turn").toEqual([]);
      });

      it("emits exactly one terminal", () => {
        const terminals = fixture.events.filter((e) => e.event === "stream.ended");
        expect(terminals).toHaveLength(1);
        expect(fixture.events[fixture.events.length - 1].event).toBe("stream.ended");
      });

      it("reduces without throwing and produces a transcript", () => {
        const state = reduceFixture(fixture);
        const threadId = Object.keys(state.threads)[0];
        expect(threadId).toBeTruthy();
        expect(state.threads[threadId]).toBeDefined();
      });
    });
  }
});

describe("live-to-client equivalence for a text turn", () => {
  const fixture = fixtures.find((f) => f.name === "text-turn")!;

  it("renders the provider's reasoning and text as separate completed parts", () => {
    const state = reduceFixture(fixture);
    const parts = partsOf(state, "thread_1");

    const text = parts.filter((p) => p.type === "text");
    const reasoning = parts.filter((p) => p.type === "reasoning");

    expect(reasoning.length).toBeGreaterThan(0);
    expect(text.length).toBeGreaterThan(0);
    // The deltas must have been concatenated, not replaced by the last one.
    const joined = text.map((p) => (p as { content?: string }).content ?? "").join("");
    expect(joined).toContain("Hello there.");
  });

  it("marks the thread idle once the terminal arrives", () => {
    const state = reduceFixture(fixture);
    const thread = state.threads["thread_1"];
    expect(thread.agentStatus).not.toBe("streaming");
  });
});

describe("live-to-client equivalence for a tool turn", () => {
  const fixture = fixtures.find((f) => f.name === "tool-turn")!;

  it("assembles streamed tool arguments into the final parsed input", () => {
    const deltas = fixture.events
      .filter((e) => e.event === "tool_call.arguments.delta")
      .map((e) => e.data.delta as string);
    const completed = fixture.events.find(
      (e) => e.event === "part.completed" && e.data.type === "tool-call"
    );

    expect(deltas.length).toBeGreaterThan(1);
    // Concatenated deltas must equal the input the tool actually ran with,
    // or the UI shows something the model never sent.
    expect(deltas.join("")).toBe(JSON.stringify(completed!.data.finalContent));
  });

  it("opens exactly one tool-call part for one provider tool call", () => {
    const created = fixture.events.filter(
      (e) => e.event === "part.created" && e.data.type === "tool-call"
    );
    expect(created).toHaveLength(1);
  });

  it("renders a tool-call part in the transcript", () => {
    const state = reduceFixture(fixture);
    const parts = partsOf(state, "thread_1");
    expect(parts.some((p) => p.type === "tool-call")).toBe(true);
  });
});

describe("structured turn", () => {
  const fixture = fixtures.find((f) => f.name === "structured-turn")!;

  it("carries a truthful semantic activity label, not a claim of thinking", () => {
    const status = fixture.events.find((e) => e.event === "status.updated");
    expect(status).toBeDefined();
    expect(status!.data.kind).toBe("reading");
    expect(status!.data.label).toBe("Reading project files");
    // "thinking" is reserved for provider-returned reasoning; a file scan is
    // not the model thinking, however slow it is.
    expect(status!.data.kind).not.toBe("thinking");
  });

  it("gives the structured data part its own created/delta/completed lifecycle", () => {
    const dataParts = fixture.events.filter(
      (e) => e.data?.type === "file-list" || e.event === "data.delta"
    );
    expect(dataParts.length).toBeGreaterThanOrEqual(3);
  });
});

describe("hitl turn", () => {
  const fixture = fixtures.find((f) => f.name === "hitl-turn")!;

  it("publishes the request and the decision as standard events", () => {
    const requested = fixture.events.find((e) => e.event === "hitl.requested");
    const resolved = fixture.events.find((e) => e.event === "hitl.resolved");

    expect(requested?.data.approvalId).toBe("approval_1");
    expect(requested?.data.status).toBe("pending");
    expect(resolved?.data.approvalId).toBe("approval_1");
    expect(resolved?.data.approved).toBe(true);
    // The request must precede the decision, so a client that joins mid-turn
    // can rebuild the approval card before it sees the answer.
    expect(fixture.events.indexOf(requested!)).toBeLessThan(
      fixture.events.indexOf(resolved!)
    );
  });
});

describe("cancel and error turns", () => {
  it("reports a cancelled run as cancelled, never as a success", () => {
    const fixture = fixtures.find((f) => f.name === "cancel-turn")!;
    const failed = fixture.events.find((e) => e.event === "run.failed");
    const ended = fixture.events.find((e) => e.event === "stream.ended");
    expect(failed?.data.cancelled).toBe(true);
    expect(ended?.data.outcome).toBe("cancelled");
  });

  it("fails the open part when a provider stream dies mid-part", () => {
    const fixture = fixtures.find((f) => f.name === "error-turn")!;
    const failedPart = fixture.events.find((e) => e.event === "part.failed");
    const ended = fixture.events.find((e) => e.event === "stream.ended");
    // Presenting an incomplete part as complete is the failure mode this
    // prevents: the user would read a truncated answer as a finished one.
    expect(failedPart).toBeDefined();
    expect(ended?.data.outcome).toBe("failed");
  });
});

describe("paused turn", () => {
  const fixture = fixtures.find((f) => f.name === "paused-turn")!;

  it("publishes paused then resumed as state updates carrying no transcript", () => {
    const states = fixture.events.filter((e) => e.event === "state.updated");
    expect(states).toHaveLength(2);
    expect(states[0].data.pauseState).toBe("paused");
    expect(states[1].data.pauseState).toBe("none");
    for (const state of states) {
      // A state event that duplicated transcript content would give clients
      // two sources of truth for the same message.
      expect(state.data.delta).toBeUndefined();
      expect(state.data.finalContent).toBeUndefined();
    }
  });

  it("still completes the run after the resume", () => {
    const ended = fixture.events.find((e) => e.event === "stream.ended");
    expect(ended?.data.outcome).toBe("completed");
  });
});

describe("frozen schemas", () => {
  it("does not define or require application tenancy fields", () => {
    for (const name of [
      "agent-state-snapshot.schema.json",
      "command-request.schema.json",
    ]) {
      const schema = loadSchema(name);
      for (const field of [
        "projectId",
        "project_id",
        "teamId",
        "team_id",
        "tenantId",
        "workspaceId",
        "organizationId",
      ]) {
        expect(schema.properties, `${name} defines ${field}`).not.toHaveProperty(
          field
        );
        expect(schema.required ?? [], `${name} requires ${field}`).not.toContain(
          field
        );
      }
    }
  });

  it("accept the documented snapshot shape", () => {
    const schema = loadSchema("agent-state-snapshot.schema.json");
    const snapshot = {
      schemaVersion: 1,
      sessionId: "session_1",
      currentThreadId: "thread_1",
      activeRun: {
        runId: "run_1",
        lifecycle: "executing",
        acceptedAt: "2026-08-22T10:00:00.000Z",
      },
      pause: { state: "paused", accumulatedPausedMs: 1200, epoch: 1 },
      activity: { kind: "reading", label: "Reading project files", source: "tool" },
      approval: { status: "pending", approvalId: "approval_1" },
      revision: 12,
      cursor: { runId: "run_1", streamEpoch: 0, sequenceNumber: 42 },
      reconcileRequired: false,
    };
    expect(validate(schema, snapshot)).toEqual([]);
  });

  it("reject a snapshot that invents an activity kind", () => {
    const schema = loadSchema("agent-state-snapshot.schema.json");
    const snapshot = {
      schemaVersion: 1,
      sessionId: "s",
      activeRun: null,
      pause: { state: "none", accumulatedPausedMs: 0 },
      activity: { kind: "vibing" },
      approval: { status: "none" },
      revision: 1,
      cursor: null,
      reconcileRequired: false,
    };
    expect(validate(schema, snapshot).length).toBeGreaterThan(0);
  });

  it("lets adapters extend command requests without changing the base contract", () => {
    const schema = loadSchema("command-request.schema.json");
    expect(
      validate(schema, {
        commandId: "01J5",
        type: "pause",
        adapterContext: { owner: "opaque" },
      })
    ).toEqual([]);
  });

  it("accept the documented error envelope and reject extra fields", () => {
    const schema = loadSchema("error-envelope.schema.json");
    expect(
      validate(schema, {
        error: {
          code: "STATE_REVISION_MISMATCH",
          message: "The assistant state changed; it has been refreshed.",
          recoverable: true,
        },
      })
    ).toEqual([]);
    // Bounded payloads are how prompts and tool output stay out of errors.
    expect(
      validate(schema, {
        error: { code: "X", message: "m", recoverable: true, prompt: "secret" },
      }).length
    ).toBeGreaterThan(0);
  });
});
