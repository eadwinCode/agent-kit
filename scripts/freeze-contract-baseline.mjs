#!/usr/bin/env node
/**
 * Freezes the A0 dependency and artifact baseline into contracts/VERSIONS.json.
 *
 * The point is reproducibility, not bookkeeping. A runtime split across a Go
 * module, an npm package and an application adapter drifts silently unless
 * the exact versions and artifact checksums are recorded somewhere a test can
 * check. `--check` fails when the recorded baseline no longer matches the
 * tree, which is what makes it a gate rather than a note.
 *
 *   node scripts/freeze-contract-baseline.mjs          # write
 *   node scripts/freeze-contract-baseline.mjs --check  # verify
 */

import { createHash } from "node:crypto";
import {
  readFileSync,
  writeFileSync,
  existsSync,
  readdirSync,
  statSync,
} from "node:fs";
import { join, dirname, relative } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const OUT = join(ROOT, "contracts", "VERSIONS.json");

const sha256 = (buffer) => createHash("sha256").update(buffer).digest("hex");

function hashFile(path) {
  return existsSync(path) ? `sha256:${sha256(readFileSync(path))}` : null;
}

/** Hashes a directory's files by relative path, so order never matters. */
function hashTree(dir) {
  const entries = {};
  const walk = (current) => {
    for (const name of readdirSync(current).sort()) {
      const full = join(current, name);
      if (statSync(full).isDirectory()) {
        walk(full);
        continue;
      }
      entries[relative(dir, full)] = `sha256:${sha256(readFileSync(full))}`;
    }
  };
  walk(dir);
  return entries;
}

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function goModuleVersions() {
  const goMod = readFileSync(join(ROOT, "go", "go.mod"), "utf8");
  const versions = {};
  for (const line of goMod.split("\n")) {
    const match = line.match(/^\s*([\w.\-/]+\.[\w.\-/]+)\s+(v[\w.\-+]+)/);
    if (match && !line.includes("// indirect")) versions[match[1]] = match[2];
  }
  const goVersion = goMod.match(/^go\s+([\d.]+)/m)?.[1] ?? null;
  return { goVersion, require: versions };
}

const useAgentPkg = readJson(
  join(ROOT, "packages", "use-agent", "package.json")
);
const agentKitPkg = readJson(
  join(ROOT, "packages", "agent-kit", "package.json")
);

const baseline = {
  $comment:
    "A0 baseline. Regenerate with `node scripts/freeze-contract-baseline.mjs` after an intentional " +
    "dependency, schema or fixture change; CI runs it with --check.",
  contractSchemaVersion: 1,
  packages: {
    "@inngest/use-agent": {
      version: useAgentPkg.version,
      dependencies: useAgentPkg.dependencies ?? {},
      peerDependencies: useAgentPkg.peerDependencies ?? {},
      artifact: {
        "dist/index.js": hashFile(
          join(ROOT, "packages", "use-agent", "dist", "index.js")
        ),
        "dist/index.d.ts": hashFile(
          join(ROOT, "packages", "use-agent", "dist", "index.d.ts")
        ),
      },
    },
    "@inngest/agent-kit": { version: agentKitPkg.version },
  },
  goModule: {
    module: "github.com/eadwinCode/agent-kit/go",
    ...goModuleVersions(),
  },
  schemas: hashTree(join(ROOT, "contracts", "schemas")),
  fixtures: hashTree(join(ROOT, "contracts", "fixtures")),
};

const serialized = `${JSON.stringify(baseline, null, 2)}\n`;

if (process.argv.includes("--check")) {
  const current = existsSync(OUT) ? readFileSync(OUT, "utf8") : "";
  // Artifact hashes are only meaningful when dist/ was built; a check on a
  // clean tree compares everything else.
  const strip = (text) =>
    JSON.stringify(
      (() => {
        const parsed = JSON.parse(text || "{}");
        if (parsed?.packages?.["@inngest/use-agent"]) {
          delete parsed.packages["@inngest/use-agent"].artifact;
        }
        return parsed;
      })(),
      null,
      2
    );
  if (strip(current) !== strip(serialized)) {
    console.error(
      "contracts/VERSIONS.json is stale.\n" +
        "Run: node scripts/freeze-contract-baseline.mjs"
    );
    process.exit(1);
  }
  console.log("contracts/VERSIONS.json is current.");
} else {
  writeFileSync(OUT, serialized);
  console.log(`Wrote ${relative(ROOT, OUT)}`);
}
