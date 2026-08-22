import { describe, it, expect } from "vitest";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";

/**
 * Negative architecture checks.
 *
 * These assert the absence of things, which ordinary tests never do. Each one
 * guards a boundary that is cheap to cross by accident and expensive to
 * uncross once an application depends on it.
 */

const SRC = join(__dirname, "..");

function sourceFiles(dir = SRC, out: string[] = []): string[] {
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry);
    if (statSync(full).isDirectory()) {
      if (entry === "__tests__" || entry === "node_modules" || entry === "dist") continue;
      sourceFiles(full, out);
      continue;
    }
    if (/\.(ts|tsx)$/.test(entry)) out.push(full);
  }
  return out;
}

const files = sourceFiles();

/**
 * Strips comments before scanning. Doc examples legitimately show endpoint
 * strings and the package's own name; only real code should trip a check.
 */
function stripComments(source: string): string {
  return source
    .replace(/\/\*[\s\S]*?\*\//g, "")
    .replace(/(^|[^:])\/\/.*$/gm, "$1");
}

const read = (file: string) => stripComments(readFileSync(file, "utf8"));
const readRaw = (file: string) => readFileSync(file, "utf8");
const rel = (file: string) => relative(SRC, file);

describe("no server-sent events for AI chat", () => {
  it("never constructs an EventSource", () => {
    // AI chat is WebSocket plus HTTP snapshot/command APIs. An EventSource
    // here would be a second, unordered transport with no replay contract.
    const offenders = files.filter((f) => /\bnew EventSource\b|EventSource\(/.test(read(f)));
    expect(offenders.map(rel)).toEqual([]);
  });
});

describe("no browser-owned current thread", () => {
  it("never reads or writes storage for thread or run identity", () => {
    // Which thread the user is in is a server fact. A browser-local pointer
    // means a new device, cleared storage, or a closed tab silently changes
    // the agent's identity.
    const offenders: string[] = [];
    for (const file of files) {
      const source = read(file);
      // Only the KEY matters, not the storage object's own name — matching
      // "sessionStorage" itself would flag the guest-identity fallback, which
      // is an application-supplied user id, not a thread or run pointer.
      for (const match of source.matchAll(
        /(?:localStorage|sessionStorage)\.[a-zA-Z]+\(([^)]*)\)/g
      )) {
        const args = match[1] ?? "";
        if (/thread|\brun\b|current|revision|cursor|active/i.test(args)) {
          offenders.push(`${rel(file)}: ${match[0]}`);
        }
      }
    }
    expect(offenders).toEqual([]);
  });
});

describe("one reducer", () => {
  it("exports exactly one standard-event reducer", () => {
    // A second reducer means two transcripts that drift. The package owns
    // reduction; applications render projections of it.
    const exported = files.flatMap((file) => {
      const matches = read(file).match(/export\s+function\s+(\w*[Rr]educe\w*)/g) ?? [];
      return matches.map((m) => `${rel(file)}: ${m}`);
    });
    expect(exported).toHaveLength(1);
    expect(exported[0]).toContain("reduceStreamingState");
  });
});

describe("transport neutrality", () => {
  it("hard-codes no application endpoints outside the optional default transport", () => {
    // The package must not know which endpoints an application serves; the
    // default HTTP transport is the one place conventional paths live, and
    // every one of them is overridable.
    const offenders: string[] = [];
    for (const file of files) {
      if (rel(file) === "core/adapters/http-transport.ts") continue;
      const urls = read(file).match(/["'`]\/api\/[^"'`]*["'`]/g);
      if (urls) offenders.push(`${rel(file)}: ${urls.join(", ")}`);
    }
    expect(offenders).toEqual([]);
  });

  it("imports nothing from an application package", () => {
    const allowed = /^(react|react-dom|@inngest\/realtime|@tanstack\/react-query|uuid|node:)/;
    const offenders: string[] = [];
    for (const file of files) {
      const imports = read(file).match(/from\s+["']([^"']+)["']/g) ?? [];
      for (const statement of imports) {
        const spec = statement.replace(/from\s+["']/, "").replace(/["']$/, "");
        if (spec.startsWith(".")) continue;
        if (allowed.test(spec)) continue;
        offenders.push(`${rel(file)} imports ${spec}`);
      }
    }
    expect(offenders).toEqual([]);
  });
});

describe("no required application polling", () => {
  it("starts no timer-driven recovery loop", () => {
    // Applications used to poll every five seconds for active-run state.
    // Snapshot-plus-tail replaces that: recovery is event- and
    // reconnect-driven, so an idle tab costs nothing.
    const offenders = files.filter((f) => /setInterval\s*\(/.test(read(f)));
    expect(offenders.map(rel)).toEqual([]);
  });
});

describe("no tenancy model in the contracts", () => {
  // This is the check that was missing when the ports shipped with
  // `projectId` on the snapshot, the command and the fetch params. Banning
  // one company's name caught the obvious leak and missed the structural
  // one: a contract that names a tenancy model forces every consumer without
  // that model to satisfy a shape the package never reads, and hands the
  // application back an id its own transport supplied a moment earlier.
  const tenancyFields = [
    "projectId",
    "project_id",
    "teamId",
    "team_id",
    "orgId",
    "organizationId",
    "workspaceId",
    "tenantId",
    "accountId",
  ];

  const contractFiles = [
    "core/ports/agent-session.ts",
    "core/services/commands.ts",
    "core/services/hydration.ts",
    "core/errors/agent-transport-error.ts",
  ];

  it("keeps tenancy out of the port and service contracts", () => {
    const offenders: string[] = [];
    for (const relative of contractFiles) {
      const source = read(join(SRC, relative));
      for (const field of tenancyFields) {
        // Prose in a doc comment explaining WHY tenancy is absent is fine;
        // the stripped source is what must not name one.
        if (new RegExp(`\\b${field}\\b`).test(source)) {
          offenders.push(`${relative}: ${field}`);
        }
      }
    }
    expect(offenders).toEqual([]);
  });

  it("keeps tenancy out of AgentStateSnapshot", () => {
    const types = readRaw(join(SRC, "types/index.ts"));
    const start = types.indexOf("export interface AgentStateSnapshot");
    const body = types.slice(
      start,
      types.indexOf("}", types.indexOf("reconcileRequired", start))
    );
    for (const field of tenancyFields) {
      // Allowed inside the doc comment that tells applications how to add
      // their own; not allowed as a declared property.
      expect(
        new RegExp(`^\\s*${field}\\??:`, "m").test(body),
        `AgentStateSnapshot declares ${field}`
      ).toBe(false);
    }
  });
});

describe("state carries no transcript", () => {
  it("keeps messages out of AgentStateSnapshot", () => {
    // The snapshot is a bounded control record. Duplicating messages into it
    // would create a second source of truth for content canonical history
    // already owns.
    const types = readRaw(join(SRC, "types/index.ts"));
    const start = types.indexOf("export interface AgentStateSnapshot");
    expect(start).toBeGreaterThan(-1);
    const body = types.slice(start, types.indexOf("}", types.indexOf("reconcileRequired", start)));
    expect(body).not.toMatch(/\bmessages\b/);
    expect(body).not.toMatch(/\bparts\b/);
    expect(body).not.toMatch(/\btranscript\b/);
  });
});

describe("public surface", () => {
  it("exports the recovery, error and command contracts applications need", () => {
    // If these are not exported, applications rebuild them by hand — which is
    // exactly the duplicated recovery code this package is replacing.
    const index = readRaw(join(SRC, "index.ts"));
    for (const symbol of [
      "AgentTransportError",
      "hydrateAgentSession",
      "LiveEventBuffer",
      "SequenceGapTracker",
      "acquireRealtimeToken",
      "buildCommand",
      "executeCommand",
      "supportsAgentSession",
      "AgentStateSnapshot",
      "IAgentSessionTransport",
      "ClientConnectionState",
    ]) {
      expect(index, `index.ts does not export ${symbol}`).toContain(symbol);
    }
  });
});
