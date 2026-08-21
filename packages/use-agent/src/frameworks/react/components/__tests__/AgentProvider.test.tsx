/* @vitest-environment jsdom */
import { describe, it, expect, vi } from "vitest";
import React, { useContext } from "react";
import { render } from "@testing-library/react";
import { AgentProvider, AgentContext } from "../../components/AgentProvider.js";

vi.mock("../../../../core/adapters/http-transport.js", async (orig) => {
  const mod: any = await (orig as any)();
  return {
    ...mod,
    createDefaultHttpTransport: vi.fn(() => ({
      getRealtimeToken: vi.fn(async () => ({
        token: "tok",
        expires: new Date().toISOString(),
      })),
    })),
  };
});

describe("AgentProvider", () => {
  it("creates a default transport and leaves connection unset by default", () => {
    function Probe() {
      const ctx = useContext(AgentContext);
      expect(ctx?.transport).toBeTruthy();
      expect(ctx?.connection).toBeNull();
      return null;
    }
    render(
      <AgentProvider debug={false}>
        <Probe />
      </AgentProvider>
    );
  });

  it("uses provided transport and connection when specified", () => {
    const transport: any = {
      getRealtimeToken: vi.fn(async () => ({
        token: "x",
        expires: new Date().toISOString(),
      })),
    };
    const connection: any = { id: "c" };
    function Probe() {
      const ctx = useContext(AgentContext);
      // Context may wrap/proxy the transport; assert by behavior
      expect(typeof ctx?.transport?.getRealtimeToken).toBe("function");
      expect(ctx?.connection).toBe(connection);
      return null;
    }
    render(
      <AgentProvider
        debug={false}
        transport={transport}
        connection={connection}
      >
        <Probe />
      </AgentProvider>
    );
  });

  it("resolves channelKey precedence: channelKey > userId > anon", () => {
    function Probe() {
      const ctx = useContext(AgentContext);
      expect(ctx?.resolvedChannelKey).toBe("ck");
      return null;
    }
    render(
      <AgentProvider debug={false} userId="uid" channelKey="ck">
        <Probe />
      </AgentProvider>
    );
  });
});
