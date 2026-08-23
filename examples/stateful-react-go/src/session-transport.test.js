import { afterEach, describe, expect, it, vi } from "vitest";
import { DemoSessionTransport } from "./session-transport.js";

afterEach(() => vi.unstubAllGlobals());

describe("DemoSessionTransport", () => {
  it("encodes the opaque session and replay cursor", async () => {
    const fetch = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          events: [],
          next: { runId: "r", streamEpoch: 2, sequenceNumber: 4 },
          hasMore: false,
        }),
        { status: 200 }
      )
    );
    vi.stubGlobal("fetch", fetch);
    const transport = new DemoSessionTransport("session/with spaces", "/api/");
    await transport.fetchEventTail({
      threadId: "thread-1",
      after: { runId: "r", streamEpoch: 2, sequenceNumber: 4 },
    });
    expect(fetch.mock.calls[0][0]).toContain(
      "/sessions/session%2Fwith%20spaces/events?"
    );
    expect(fetch.mock.calls[0][0]).toContain("after=4");
  });

  it("preserves the server's structured conflict", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            error: {
              code: "STATE_REVISION_MISMATCH",
              message: "reconcile",
              recoverable: false,
            },
            snapshot: { revision: 9 },
          }),
          { status: 409, headers: { "content-type": "application/json" } }
        )
      )
    );
    const transport = new DemoSessionTransport("demo");
    await expect(
      transport.executeCommand({ commandId: "one", type: "pause" })
    ).rejects.toMatchObject({
      code: "STATE_REVISION_MISMATCH",
      status: 409,
      snapshot: { revision: 9 },
    });
  });

  it("maps a network failure to a recoverable transport error", async () => {
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new TypeError("offline")));
    await expect(
      new DemoSessionTransport("demo").fetchAgentState()
    ).rejects.toMatchObject({
      code: "NETWORK_ERROR",
      recoverable: true,
      operation: "fetchAgentState",
    });
  });
});
