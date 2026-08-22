import { describe, it, expect } from "vitest";
import { ThreadManager } from "../thread-manager.js";
import { makeThread } from "./test-utils.js";

const tm = new ThreadManager();

const mk = (id: string, title = "New conversation") => makeThread(id, title);

describe("ThreadManager", () => {
  it("dedupes by id", () => {
    const a = mk("a");
    const b = mk("a");
    const out = tm.dedupeThreadsById([a, b]);
    expect(out.length).toBe(1);
  });

  it("merges preserving local order", () => {
    const local = [mk("a", "T1"), mk("b", "T2")];
    const server = [mk("a", "Server T1"), mk("c", "T3")];
    const out = tm.mergeThreadsPreserveOrder(local as any, server as any);
    expect(out.map((t: any) => t.id)).toEqual(["a", "b", "c"]);
  });

  it("prefers non-generic local title when longer", () => {
    const tm = new ThreadManager();
    const local = [mk("a", "Custom Analysis Title")];
    const server = [mk("a", "New conversation")];
    const out = tm.mergeThreadsPreserveOrder(local as any, server as any);
    expect(out[0].title).toBe("Custom Analysis Title");
  });

  it("parseCachedThreads handles invalid shapes and revives dates", () => {
    const tm = new ThreadManager();
    const now = new Date().toISOString();
    const raw = [
      {
        id: "a",
        title: "X",
        messageCount: "3",
        lastMessageAt: now,
        createdAt: now,
        updatedAt: now,
      },
      { id: "a", title: "Duplicate", messageCount: 1 }, // duplicate id
      { bogus: true },
    ];
    const out = tm.parseCachedThreads(raw as any);
    expect(out.length).toBe(1);
    expect(out[0].messageCount).toBe(3);
    expect(out[0].lastMessageAt instanceof Date).toBe(true);
  });

  it("preserves already structured history messages", () => {
    const timestamp = new Date().toISOString();
    const parts = [
      {
        type: "reasoning",
        id: "r1",
        agentName: "planner",
        content: "Think",
        status: "complete",
      },
      { type: "text", id: "t1", content: "Answer", status: "complete" },
    ];
    const [message] = tm.formatRawHistoryMessages([
      { id: "m1", role: "assistant", parts, created_at: timestamp },
    ]);

    expect(message.id).toBe("m1");
    expect(message.parts).toBe(parts);
    expect(message.timestamp).toBeInstanceOf(Date);
  });

  it("hydrates all text, reasoning, tool calls, and paired results", () => {
    const [message] = tm.formatRawHistoryMessages([
      {
        message_id: "m-rich",
        type: "assistant",
        data: {
          agentName: "researcher",
          output: [
            {
              type: "reasoning",
              content: [{ type: "text", text: "First, inspect." }],
            },
            {
              type: "text",
              content: [
                { type: "text", text: "One" },
                { type: "text", text: " two" },
              ],
            },
            {
              type: "tool_call",
              tools: [
                { id: "call-1", name: "search", input: { query: "agent kit" } },
                {
                  id: "call-2",
                  name: "fetch",
                  input: { url: "https://example.com" },
                },
              ],
            },
          ],
          toolCalls: [
            {
              tool: { id: "call-1", name: "search" },
              content: { data: { matches: 2 } },
            },
            {
              tool: { id: "call-2", name: "fetch" },
              content: "page body",
            },
          ],
        },
      },
    ]);

    expect(message.parts).toHaveLength(4);
    expect(message.parts[0]).toMatchObject({
      type: "reasoning",
      agentName: "researcher",
      content: "First, inspect.",
      status: "complete",
    });
    expect(message.parts[1]).toMatchObject({
      type: "text",
      content: "One two",
      status: "complete",
    });
    expect(message.parts[2]).toMatchObject({
      type: "tool-call",
      toolCallId: "call-1",
      toolName: "search",
      state: "output-available",
      input: { query: "agent kit" },
      output: { data: { matches: 2 } },
    });
    expect(message.parts[3]).toMatchObject({
      type: "tool-call",
      toolCallId: "call-2",
      state: "output-available",
      output: { data: "page body" },
    });
  });
});
