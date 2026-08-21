import type { ConversationMessage, Thread } from "../../types/index.js";

/**
 * Framework-agnostic thread utilities and manager.
 */
export class ThreadManager {
  mergeThreadsPreserveOrder(
    localThreads: Thread[],
    serverThreads: Thread[]
  ): Thread[] {
    const serverById = new Map(serverThreads.map((t) => [t.id, t] as const));

    const updatedLocal = localThreads.map((local) => {
      const server = serverById.get(local.id);
      if (!server) return local;

      const preferredTitle =
        !isGenericTitle(local.title) &&
        (isGenericTitle(server.title) ||
          (local.title?.length || 0) >= (server.title?.length || 0))
          ? local.title
          : server.title;

      return {
        id: server.id,
        title: preferredTitle,
        messageCount: Math.max(
          local.messageCount || 0,
          server.messageCount || 0
        ),
        lastMessageAt: new Date(
          Math.max(
            new Date(local.lastMessageAt).getTime(),
            new Date(server.lastMessageAt).getTime()
          )
        ),
        createdAt: new Date(
          Math.min(
            new Date(local.createdAt).getTime(),
            new Date(server.createdAt).getTime()
          )
        ),
        updatedAt: new Date(
          Math.max(
            new Date(local.updatedAt).getTime(),
            new Date(server.updatedAt).getTime()
          )
        ),
        hasNewMessages: Boolean(local.hasNewMessages || server.hasNewMessages),
      } as Thread;
    });

    const localIds = new Set(localThreads.map((t) => t.id));
    const newFromServer = serverThreads.filter((t) => !localIds.has(t.id));

    const merged = [...updatedLocal, ...newFromServer];
    return this.dedupeThreadsById(merged);
  }

  dedupeThreadsById(threads: Thread[]): Thread[] {
    const seen = new Set<string>();
    const out: Thread[] = [];
    for (const t of threads) {
      if (seen.has(t.id)) continue;
      seen.add(t.id);
      out.push(t);
    }
    return out;
  }

  reviveThreadDates<T extends Thread>(thread: T): T {
    return {
      ...thread,
      lastMessageAt: new Date(thread.lastMessageAt),
      createdAt: new Date(thread.createdAt),
      updatedAt: new Date(thread.updatedAt),
    } as T;
  }

  parseCachedThreads(raw: unknown): Thread[] {
    if (!Array.isArray(raw)) return [];
    const input = raw as unknown[];
    const seen = new Set<string>();
    const out: Thread[] = [];
    for (const item of input) {
      if (!isRecord(item)) continue;
      const id = typeof item.id === "string" ? item.id : undefined;
      if (!id || seen.has(id)) continue;
      seen.add(id);

      const title =
        typeof item.title === "string" ? item.title : "New conversation";

      let messageCount = 0;
      const mc = item["messageCount"];
      if (typeof mc === "number") messageCount = mc;
      else if (typeof mc === "string") {
        const n = Number.parseInt(mc, 10);
        if (!Number.isNaN(n)) messageCount = n;
      }

      const lastMessageAt = toDate(item["lastMessageAt"]);
      const createdAt = toDate(item["createdAt"]);
      const updatedAt = toDate(item["updatedAt"]);
      const hasNewMessages = item["hasNewMessages"] === true;

      out.push(
        this.reviveThreadDates({
          id,
          title,
          messageCount,
          lastMessageAt,
          createdAt,
          updatedAt,
          hasNewMessages,
        } as Thread)
      );
    }
    return out;
  }

  buildThreadsCacheKey(userId: string): string {
    return `threads_${userId}`;
  }

  formatRawHistoryMessages(rawMessages: unknown[]): ConversationMessage[] {
    if (!Array.isArray(rawMessages)) return [];
    return rawMessages.map((raw, messageIndex) => {
      const msg = isRecord(raw) ? raw : {};
      const id =
        typeof msg["message_id"] === "string"
          ? msg["message_id"]
          : typeof msg["id"] === "string"
            ? msg["id"]
            : `msg-${Date.now()}-${messageIndex}`;
      const baseTimestamp = msg["createdAt"] ?? msg["created_at"] ?? Date.now();
      const timestamp = toDate(baseTimestamp);

      if (
        (msg["role"] === "user" || msg["role"] === "assistant") &&
        Array.isArray(msg["parts"])
      ) {
        return {
          ...msg,
          id,
          timestamp,
          status: typeof msg["status"] === "string" ? msg["status"] : "sent",
        } as ConversationMessage;
      }

      const kind = msg["kind"] ?? msg["type"];
      if (kind === "user") {
        const content = historyContentText(msg["content"]);
        const clientState = isRecord(msg["clientState"])
          ? msg["clientState"]
          : undefined;
        return {
          id,
          role: "user",
          parts: [
            { type: "text", id: `text-${id}`, content, status: "complete" },
          ],
          timestamp,
          status: "sent",
          clientState,
        } as ConversationMessage;
      }

      const data = isRecord(msg["data"]) ? msg["data"] : {};
      const output = asUnknownArray(msg["output"]);
      const nestedOutput = asUnknownArray(data["output"]);
      const resolvedOutput = output.length > 0 ? output : nestedOutput;
      const toolResults = asUnknownArray(msg["toolCalls"]);
      const nestedToolResults = asUnknownArray(data["toolCalls"]);
      const resolvedToolResults =
        toolResults.length > 0 ? toolResults : nestedToolResults;
      const agentName =
        typeof msg["agentName"] === "string"
          ? msg["agentName"]
          : typeof data["agentName"] === "string"
            ? data["agentName"]
            : "";
      const parts: ConversationMessage["parts"] = [];
      let toolResultIndex = 0;

      for (
        let outputIndex = 0;
        outputIndex < resolvedOutput.length;
        outputIndex++
      ) {
        const item = resolvedOutput[outputIndex];
        if (!isRecord(item)) continue;

        if (item["type"] === "text") {
          parts.push({
            type: "text",
            id: `text-${id}-${outputIndex}`,
            content: historyContentText(item["content"]),
            status: "complete",
          });
          continue;
        }

        if (item["type"] === "reasoning") {
          parts.push({
            type: "reasoning",
            id: `reasoning-${id}-${outputIndex}`,
            agentName,
            content: historyContentText(item["content"]),
            status: "complete",
          });
          continue;
        }

        const rawTools = asUnknownArray(item["tools"]);
        if (item["type"] === "tool_call" && rawTools.length > 0) {
          for (const rawTool of rawTools) {
            if (!isRecord(rawTool)) continue;
            const result = resolvedToolResults[toolResultIndex++];
            const resultRecord = isRecord(result) ? result : undefined;
            const tool =
              resultRecord && isRecord(resultRecord["tool"])
                ? resultRecord["tool"]
                : {};
            const toolCallId =
              typeof rawTool["id"] === "string"
                ? rawTool["id"]
                : typeof tool["id"] === "string"
                  ? tool["id"]
                  : `tool-${id}-${outputIndex}-${toolResultIndex}`;
            const rawResult = resultRecord?.["content"];
            const normalizedResult =
              isRecord(rawResult) && "data" in rawResult
                ? rawResult
                : { data: rawResult };
            const toolName =
              typeof rawTool["name"] === "string"
                ? rawTool["name"]
                : typeof tool["name"] === "string"
                  ? tool["name"]
                  : "tool";
            if (resultRecord) {
              parts.push({
                type: "tool-call",
                toolCallId,
                toolName,
                state: "output-available",
                input: rawTool["input"] ?? {},
                output: normalizedResult,
              });
            } else {
              parts.push({
                type: "tool-call",
                toolCallId,
                toolName,
                state: "input-available",
                input: rawTool["input"] ?? {},
              });
            }
          }
        }
      }

      if (parts.length === 0) {
        const legacyText = (() => {
          for (const item of resolvedOutput) {
            if (isRecord(item) && item["type"] === "text") {
              return historyContentText(item["content"]);
            }
          }
          return "";
        })();
        parts.push({
          type: "text",
          id: `text-${id}`,
          content: legacyText,
          status: "complete",
        });
      }

      return {
        id,
        role: "assistant",
        parts,
        timestamp,
        status: "sent",
      } as ConversationMessage;
    });
  }
}

export function isGenericTitle(title?: string | null): boolean {
  if (!title) return true;
  const t = String(title).trim().toLowerCase();
  return t.length === 0 || t === "new conversation" || t === "new query";
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function asUnknownArray(value: unknown): unknown[] {
  return Array.isArray(value) ? (value as unknown[]) : [];
}

function historyContentText(value: unknown): string {
  if (typeof value === "string") return value;
  if (Array.isArray(value)) {
    return value
      .map((part) =>
        isRecord(part) &&
        part["type"] === "text" &&
        typeof part["text"] === "string"
          ? part["text"]
          : ""
      )
      .join("");
  }
  if (value === null || value === undefined) return "";
  try {
    return JSON.stringify(value) ?? "";
  } catch {
    return "[unserializable value]";
  }
}

function toDate(value: unknown): Date {
  if (value instanceof Date) return value;
  if (typeof value === "number") return new Date(value);
  if (typeof value === "string") {
    const t = Date.parse(value);
    if (!Number.isNaN(t)) return new Date(t);
  }
  return new Date();
}
