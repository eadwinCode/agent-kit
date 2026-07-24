// Emits golden JSON fixtures for the Go port's wire-parity tests.
//
// Each case is a Message constructed EXACTLY like the TS construction sites
// (converters.ts resultToMessages / reasoningResultToMessage, agent.ts
// invokeTools / agentPrompt) and serialized with JSON.stringify — so the Go
// side is tested against real TS bytes, not a reading of them.
//
// Regenerate with:
//   node scripts/emit-fixtures.mjs > ../../go/internal/testdata/fixtures.json
//
// Content deliberately includes the parity traps: HTML chars (Go escapes by
// default), emoji/unicode, nested non-alphabetical keys (Go maps would sort),
// null content, empty strings. U+2028/U+2029 get semanticOnly: encoding/json
// always escapes them while JSON.stringify does not — both parse identically,
// bytes differ by design.
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const xxh = require("xxhashjs");

const cases = [];
const add = (name, msg, opts = {}) =>
  cases.push({ name, json: JSON.stringify(msg), ...opts });

// --- text variants (converters.ts resultToMessages / agent.ts agentPrompt) ---
add("text_assistant_stop", {
  type: "text",
  role: "assistant",
  content: "Here's the fix: use `a < b && c > d` — not \"a <= b\".\n\tDone ✅",
  stop_reason: "stop",
});
add("text_assistant_tool", {
  type: "text",
  role: "assistant",
  content: "Let me check the weather 🌍 in São Paulo…",
  stop_reason: "tool",
});
add("text_empty", {
  type: "text",
  role: "assistant",
  content: "",
  stop_reason: "stop",
});
// agentPrompt system/user messages carry no stop_reason.
add("text_system", {
  type: "text",
  role: "system",
  content: "You are a <helpful> assistant & code reviewer.",
});
add("text_user", {
  type: "text",
  role: "user",
  content: "Fix this: if (x & 0xFF) { emit('<done>') }",
});
// Vision: content as parts array.
add("text_user_parts", {
  type: "text",
  role: "user",
  content: [
    { type: "text", text: "What's in this image? <analyze & describe>" },
    { type: "image", image: "data:image/png;base64,iVBORw0KGgoAAAANSUhEUg==" },
    { type: "image", image: "aGVsbG8=", mimeType: "image/jpeg" },
  ],
});
// U+2028/U+2029: JSON.stringify emits them raw; encoding/json always escapes.
add(
  "text_line_separators_semantic_only",
  {
    type: "text",
    role: "assistant",
    content: "line one line two end",
    stop_reason: "stop",
  },
  { semanticOnly: true }
);

// --- reasoning (converters.ts reasoningResultToMessage) ---
{
  const details = [
    {
      type: "text",
      text: "The user wants weather data — I should call the tool. Note: 1 < 2.",
      signature: "EqQBCkYIBRgCIkDBaK3clevixSig+/f8wPCJ7wE=",
    },
  ];
  const msg = {
    type: "reasoning",
    role: "assistant",
    content: details[0].text,
    stop_reason: "tool",
  };
  msg.details = details;
  msg.signature = details[0].signature;
  add("reasoning_signed", msg);
}
{
  // Redacted-only: no text details -> content "", no signature.
  const msg = {
    type: "reasoning",
    role: "assistant",
    content: "",
    stop_reason: "stop",
  };
  msg.details = [{ type: "redacted", data: "EncRyPtEdOpaqueData==" }];
  add("reasoning_redacted", msg);
}

// --- tool_call (converters.ts: stop_reason inserted BEFORE tools) ---
const toolCallMsg = {
  type: "tool_call",
  role: "assistant",
  stop_reason: "tool",
  tools: [
    {
      type: "tool",
      id: "toolu_01ABC",
      name: "edit_file",
      // Non-alphabetical keys + nasty values: Go must preserve order (no map round-trip).
      input: {
        path: "src/<main>.ts",
        find: "a & b",
        replace: "a && b",
        allOccurrences: true,
        limit: 3,
      },
    },
  ],
};
add("tool_call", toolCallMsg);

// --- tool_result (agent.ts invokeTools: role precedes type) ---
add("tool_result", {
  role: "tool_result",
  type: "tool_result",
  tool: {
    type: "tool",
    id: "toolu_01ABC",
    name: "edit_file",
    input: { path: "src/<main>.ts", find: "a & b", replace: "a && b" },
  },
  content: {
    data: {
      zulu: "last-alphabetically, first-inserted",
      applied: true,
      hunks: [{ line: 42, before: "a & b", after: "a && b" }, null],
      stats: { edits: 1, elapsedMs: 12.5 },
    },
  },
  stop_reason: "tool",
});
add("tool_result_null_content", {
  role: "tool_result",
  type: "tool_result",
  tool: { type: "tool", id: "toolu_02", name: "noop", input: {} },
  content: null,
  stop_reason: "tool",
});

// --- UserMessage ---
const userMessage = {
  id: "cm_user_msg_01",
  content: "Deploy the <staging> build & notify #eng",
  role: "user",
  state: { selection: { from: 10, to: 42 }, dirty: true },
  clientTimestamp: new Date("2026-07-24T09:15:30.500Z").toISOString(),
  systemPrompt: "Be terse.",
};

// --- AgentResult: full object, export() shape, and checksum ---
// Mirrors the class: constructor-order fields, undefined omitted, checksum
// input = JSON.stringify(output.concat(toolCalls)) + id (createdAt excluded).
const output = [
  cases.find((c) => c.name === "reasoning_signed"),
  cases.find((c) => c.name === "text_assistant_tool"),
  cases.find((c) => c.name === "tool_call"),
].map((c) => JSON.parse(c.json));
const toolCalls = [JSON.parse(cases.find((c) => c.name === "tool_result").json)];
const createdAt = new Date("2026-07-24T10:30:00.123Z");
const id = "msg_durable_01";
const raw = JSON.stringify({
  text: "Let me check the weather 🌍 in São Paulo…",
  usage: { input_tokens: 517, output_tokens: 115, cache_read_input_tokens: 0, cache_creation_input_tokens: 3850 },
});

const checksumInput = JSON.stringify(output.concat(toolCalls)) + id;
const checksum = xxh.h64(checksumInput, 0).toString();

const agentResult = {
  json: JSON.stringify({ agentName: "weather-agent", output, toolCalls, createdAt, raw, id }),
  checksum,
  exportJson: JSON.stringify({ agentName: "weather-agent", output, toolCalls, createdAt, checksum }),
};

process.stdout.write(
  JSON.stringify(
    {
      comment: "GENERATED by packages/agent-kit/scripts/emit-fixtures.mjs — do not edit by hand.",
      cases,
      userMessage: { json: JSON.stringify(userMessage) },
      agentResult,
    },
    null,
    2
  ) + "\n"
);
