import { describe, it, expect } from "vitest";
import {
  AgentTransportError,
  AgentErrorCodes,
  agentTransportErrorFromNetwork,
  agentTransportErrorFromResponse,
  isAgentTransportError,
  isRecoverableStatus,
} from "../agent-transport-error.js";

function jsonResponse(status: number, body: unknown, headers: Record<string, string> = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", ...headers },
  });
}

describe("agentTransportErrorFromResponse", () => {
  it("preserves the server's structured envelope, including the snapshot", async () => {
    // This is the whole point: applications used to keep the 409 body in a
    // side variable next to the hook because the transport threw it away.
    const snapshot = {
      schemaVersion: 1 as const,
      sessionId: "session_1",
      activeRun: null,
      pause: { state: "none" as const, accumulatedPausedMs: 0 },
      activity: { kind: "none" as const },
      approval: { status: "none" as const },
      revision: 12,
      cursor: null,
      reconcileRequired: false,
    };
    const response = jsonResponse(409, {
      error: {
        code: AgentErrorCodes.StateRevisionMismatch,
        message: "The assistant state changed; it has been refreshed.",
        recoverable: true,
        correlationId: "req-42",
        retryAfterMs: 0,
        details: { expected: 11, actual: 12 },
      },
      snapshot,
    });

    const error = await agentTransportErrorFromResponse(response, "pause");

    expect(error.status).toBe(409);
    expect(error.code).toBe(AgentErrorCodes.StateRevisionMismatch);
    expect(error.recoverable).toBe(true);
    expect(error.correlationId).toBe("req-42");
    expect(error.details).toEqual({ expected: 11, actual: 12 });
    expect(error.snapshot?.revision).toBe(12);
    expect(error.operation).toBe("pause");
    expect(error.requiresReconcile).toBe(true);
  });

  it("falls back to status-derived values when the body is not JSON", async () => {
    const response = new Response("<html>gateway timeout</html>", {
      status: 504,
      statusText: "Gateway Timeout",
    });
    const error = await agentTransportErrorFromResponse(response);

    expect(error.code).toBe("HTTP_504");
    expect(error.recoverable).toBe(true);
    expect(error.message).toContain("504");
  });

  it("reads Retry-After when the envelope does not carry one", async () => {
    const response = jsonResponse(
      429,
      { error: { code: "RATE_LIMITED", message: "slow down", recoverable: true } },
      { "retry-after": "3" }
    );
    const error = await agentTransportErrorFromResponse(response);
    expect(error.retryAfterMs).toBe(3000);
  });

  it("classifies auth failures so callers stop retrying", async () => {
    const response = jsonResponse(401, {
      error: { code: "UNAUTHORIZED", message: "sign in", recoverable: false },
    });
    const error = await agentTransportErrorFromResponse(response);
    expect(error.isAuthFailure).toBe(true);
    expect(error.recoverable).toBe(false);
  });

  it("honors an explicit recoverable:false on a 5xx", async () => {
    const response = jsonResponse(500, {
      error: { code: "PERMANENT", message: "no", recoverable: false },
    });
    const error = await agentTransportErrorFromResponse(response);
    expect(error.recoverable).toBe(false);
  });
});

describe("agentTransportErrorFromNetwork", () => {
  it("marks a transport-level failure recoverable", () => {
    const error = agentTransportErrorFromNetwork(new Error("offline"), "sendMessage");
    expect(error.status).toBe(0);
    expect(error.code).toBe("NETWORK_ERROR");
    expect(error.recoverable).toBe(true);
    expect(error.operation).toBe("sendMessage");
  });
});

describe("isAgentTransportError", () => {
  it("recognizes instances and structurally compatible values", () => {
    const real = new AgentTransportError({
      status: 400,
      code: "BAD",
      message: "bad",
      recoverable: false,
    });
    expect(isAgentTransportError(real)).toBe(true);
    // Bundlers can produce two copies of a class; the structural check keeps
    // narrowing working across module realms.
    expect(
      isAgentTransportError({ name: "AgentTransportError", code: "BAD" })
    ).toBe(true);
    expect(isAgentTransportError(new Error("plain"))).toBe(false);
  });
});

describe("isRecoverableStatus", () => {
  it("retries timeouts, rate limits and server errors only", () => {
    expect(isRecoverableStatus(408)).toBe(true);
    expect(isRecoverableStatus(429)).toBe(true);
    expect(isRecoverableStatus(503)).toBe(true);
    expect(isRecoverableStatus(400)).toBe(false);
    expect(isRecoverableStatus(404)).toBe(false);
    expect(isRecoverableStatus(409)).toBe(false);
  });
});
