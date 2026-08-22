import { describe, it, expect, vi } from "vitest";
import {
  acquireRealtimeToken,
  backoffDelay,
  isAuthFailure,
  isTransientFailure,
  normalizeConnectionState,
  TokenRefreshError,
} from "../token-refresh.js";
import { AgentTransportError } from "../../errors/agent-transport-error.js";
import type { ClientConnectionState } from "../../../types/index.js";

const noSleep = async () => {};

describe("acquireRealtimeToken", () => {
  it("returns the token on the first success", async () => {
    const fetchToken = vi.fn(async () => ({ token: "t1" }));
    const states: ClientConnectionState[] = [];

    const token = await acquireRealtimeToken(fetchToken, {
      sleep: noSleep,
      onStateChange: (s) => states.push(s),
    });

    expect(token).toEqual({ token: "t1" });
    expect(fetchToken).toHaveBeenCalledTimes(1);
    expect(states).toEqual(["connecting", "connected"]);
  });

  it("recovers from a transient failure instead of going terminal", async () => {
    // One 503 used to leave the connection permanently errored; the run was
    // fine the whole time.
    let calls = 0;
    const fetchToken = vi.fn(async () => {
      calls++;
      if (calls < 3) {
        throw new AgentTransportError({
          status: 503,
          code: "HTTP_503",
          message: "unavailable",
          recoverable: true,
        });
      }
      return { token: "t1" };
    });
    const states: ClientConnectionState[] = [];

    const token = await acquireRealtimeToken(fetchToken, {
      sleep: noSleep,
      random: () => 0.5,
      onStateChange: (s) => states.push(s),
    });

    expect(token).toEqual({ token: "t1" });
    expect(fetchToken).toHaveBeenCalledTimes(3);
    expect(states).toContain("reconnecting");
    expect(states[states.length - 1]).toBe("connected");
  });

  it("fails fast on an auth failure rather than burning the backoff budget", async () => {
    const fetchToken = vi.fn(async () => {
      throw new AgentTransportError({
        status: 401,
        code: "UNAUTHORIZED",
        message: "sign in",
        recoverable: false,
      });
    });

    await expect(
      acquireRealtimeToken(fetchToken, { sleep: noSleep })
    ).rejects.toMatchObject({
      name: "TokenRefreshError",
      recoverable: false,
    });
    expect(fetchToken).toHaveBeenCalledTimes(1);
  });

  it("gives up after maxAttempts but reports the failure as recoverable", async () => {
    const fetchToken = vi.fn(async () => {
      throw new Error("network down");
    });

    const error = await acquireRealtimeToken(fetchToken, {
      sleep: noSleep,
      maxAttempts: 3,
    }).catch((e) => e as TokenRefreshError);

    expect(error).toBeInstanceOf(TokenRefreshError);
    expect(error.recoverable).toBe(true);
    expect(error.attempts).toBe(3);
    expect(fetchToken).toHaveBeenCalledTimes(3);
  });

  it("stops immediately when the caller aborts", async () => {
    const controller = new AbortController();
    const fetchToken = vi.fn(async () => {
      controller.abort();
      throw new Error("boom");
    });

    await expect(
      acquireRealtimeToken(fetchToken, {
        signal: controller.signal,
        sleep: noSleep,
      })
    ).rejects.toThrow("boom");
    expect(fetchToken).toHaveBeenCalledTimes(1);
  });

  it("waits the delays it computes, growing them between attempts", async () => {
    const delays: number[] = [];
    let calls = 0;
    const fetchToken = async () => {
      calls++;
      if (calls < 4) throw new Error("transient");
      return "ok";
    };

    await acquireRealtimeToken(fetchToken, {
      sleep: async (ms) => {
        delays.push(ms);
      },
      random: () => 1,
      baseDelayMs: 100,
      jitter: 0,
    });

    expect(delays).toEqual([100, 200, 400]);
  });
});

describe("backoffDelay", () => {
  it("caps at maxDelayMs", () => {
    expect(backoffDelay(20, { baseDelayMs: 500, maxDelayMs: 30_000, jitter: 0 })).toBe(30_000);
  });

  it("applies full jitter across the whole window", () => {
    const low = backoffDelay(3, { baseDelayMs: 100, jitter: 1, random: () => 0 });
    const high = backoffDelay(3, { baseDelayMs: 100, jitter: 1, random: () => 1 });
    // Full jitter is what stops every client reconnecting in lockstep after
    // a shared outage.
    expect(low).toBe(0);
    expect(high).toBe(800);
  });
});

describe("failure classification", () => {
  it("treats 401/403 as auth and everything unknown as transient", () => {
    expect(isAuthFailure({ status: 401 })).toBe(true);
    expect(isAuthFailure(new Error("403 Forbidden"))).toBe(true);
    expect(isAuthFailure(new Error("socket hang up"))).toBe(false);

    expect(isTransientFailure(new Error("socket hang up"))).toBe(true);
    expect(isTransientFailure({ status: 500 })).toBe(true);
    expect(isTransientFailure({ status: 400 })).toBe(false);
    expect(isTransientFailure({ status: 401 })).toBe(false);
  });
});

describe("normalizeConnectionState", () => {
  it("maps provider vocabularies onto the package's states", () => {
    expect(normalizeConnectionState("active")).toBe("connected");
    expect(normalizeConnectionState(1)).toBe("connected");
    expect(normalizeConnectionState("closed")).toBe("disconnected");
    expect(normalizeConnectionState("error")).toBe("error");
  });

  it("distinguishes a first connect from a reconnect", () => {
    // The UI says "Reconnecting…" only when there was something to lose.
    expect(normalizeConnectionState("connecting")).toBe("connecting");
    expect(normalizeConnectionState("connecting", "connected")).toBe("reconnecting");
  });

  it("keeps the previous state for values it does not recognize", () => {
    expect(normalizeConnectionState({ weird: true }, "connected")).toBe("connected");
  });
});
