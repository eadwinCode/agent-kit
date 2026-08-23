import {
  agentTransportErrorFromNetwork,
  agentTransportErrorFromResponse,
} from "@inngest/use-agent";

/**
 * Application-owned HTTP adapter for the generic IAgentSessionTransport.
 * The opaque session is captured here; useAgentSession never prescribes an
 * ownership URL, authentication strategy, project, or team field.
 */
export class DemoSessionTransport {
  constructor(sessionId, baseUrl = "/api") {
    this.sessionId = sessionId;
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  async fetchAgentState(params = {}, options = {}) {
    const query = new URLSearchParams();
    if (params.knownRevision !== undefined) {
      query.set("knownRevision", String(params.knownRevision));
    }
    return this.#request(
      `/state${query.size ? `?${query}` : ""}`,
      { signal: options.signal },
      "fetchAgentState"
    );
  }

  async fetchEventTail(params, options = {}) {
    const query = new URLSearchParams({
      threadId: params.threadId,
      runId: params.after.runId,
      streamEpoch: String(params.after.streamEpoch),
      after: String(params.after.sequenceNumber),
      limit: String(params.limit ?? 100),
    });
    return this.#request(
      `/events?${query}`,
      { signal: options.signal },
      "fetchEventTail"
    );
  }

  executeCommand(command, options = {}) {
    return this.#request(
      "/commands",
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(command),
        signal: options.signal,
      },
      "executeCommand"
    );
  }

  liveURL() {
    return `${this.#sessionURL()}/live`;
  }

  diagnostics(options = {}) {
    return this.#request(
      "/diagnostics",
      { signal: options.signal },
      "diagnostics"
    );
  }

  reset() {
    return this.#request("/reset", { method: "POST" }, "reset");
  }

  #sessionURL() {
    return `${this.baseUrl}/sessions/${encodeURIComponent(this.sessionId)}`;
  }

  async #request(path, init, operation) {
    let response;
    try {
      response = await fetch(`${this.#sessionURL()}${path}`, init);
    } catch (error) {
      throw agentTransportErrorFromNetwork(error, operation);
    }
    if (!response.ok) {
      throw await agentTransportErrorFromResponse(response, operation);
    }
    try {
      return await response.json();
    } catch (error) {
      throw agentTransportErrorFromNetwork(error, operation);
    }
  }
}
