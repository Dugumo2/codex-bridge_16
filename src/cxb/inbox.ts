import { sanitizeForTerminal } from "./terminal-text";
import { CxbError, EXIT, type InboxItem, type ThreadSummary } from "./contracts";
import { commandSession } from "./command-session";
import type { CommandHandler } from "./main";
import { redactParams, renderPending } from "./server-requests";

export function object(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

export function safeLine(value: unknown): string {
  return sanitizeForTerminal(String(value ?? "")).replace(/[\r\n\t\u0080-\u009f\u202a-\u202e\u2066-\u2069]/g, " ");
}

function hasRedaction(raw: unknown, clean: unknown): boolean {
  if (typeof clean === "string") return clean.includes("***") && clean !== raw;
  if (Array.isArray(clean)) return clean.some((v, i) => hasRedaction(Array.isArray(raw) ? raw[i] : undefined, v));
  return Object.entries(object(clean)).some(([key, value]) => key !== "isSecret" && hasRedaction(object(raw)[key], value));
}

/** 摘要与回答提示复用交接渲染器；原始参数绝不进入命令输出。 */
export function presentItem(item: InboxItem, shortId: string) {
  const params = object(redactParams(item.params));
  const questions = Array.isArray(params.questions) ? params.questions.map(object) : [];
  const rendered = renderPending([{ ...item, params }], { threadId: item.threadId, shortId });
  const action = rendered.text.split("\n").slice(0, -1).join(" ");
  const summary = action.slice(safeLine(item.id).length + 1).split("；回答：")[0];
  return {
    action: safeLine(action),
    json: {
      id: item.id, kind: item.kind, status: item.status, threadId: item.threadId, shortId,
      turnId: item.turnId, createdAt: item.createdAt, expiresAt: item.expiresAt, summary,
      ...(item.kind === "question" ? { questions: questions.map((q) => ({ id: q.id, text: q.question,
        options: Array.isArray(q.options) ? q.options.map((o) => object(o).label) : [] })) } : {}),
      sensitive: (Array.isArray(object(item.params).questions) && (object(item.params).questions as unknown[]).some((q) => object(q).isSecret === true))
        || hasRedaction(item.params, params),
    },
  };
}

export const inbox: CommandHandler = async (ctx) => {
  if (ctx.parsed.options.all) throw new CxbError("BROKER_UNAVAILABLE", "broker 不支持跨工作区列出事项，请用 -d 指定目录");
  const scope = commandSession(ctx);
  try {
    const rpc = await scope.connect();
    const { kind, limit } = ctx.parsed.options;
    const list = await rpc.request<{ items: InboxItem[] }>("broker/inbox/list", { includeClosed: true, ...(kind ? { kind } : {}) });
    const { threads } = await rpc.request<{ threads: ThreadSummary[] }>("broker/threads/list", {});
    const shorts = new Map(threads.map((t) => [t.threadId, t.shortId]));
    const items = list.items.filter((i) => ["pending", "resolving", "expired"].includes(i.status) && (!kind || i.kind === kind))
      .sort((a, b) => a.createdAt - b.createdAt).slice(0, limit);
    const rows = items.map((item) => {
      const shortId = shorts.get(item.threadId) ?? item.threadId.slice(0, 8);
      const view = presentItem(item, shortId);
      if (ctx.parsed.globals.verbose) ctx.out.progress(`事项参数摘要：${safeLine(JSON.stringify(redactParams(item.params)))}`);
      return { item, shortId, view };
    });
    ctx.out.result(["ID TYPE THREAD AGE ACTION", ...rows.map(({ item, shortId, view }) =>
      `${safeLine(item.id)} ${item.kind} ${safeLine(shortId)} ${Math.max(0, Math.floor((Date.now() - item.createdAt) / 1000))}秒 ${item.status === "expired" ? "已过期" : view.action}`)].join("\n"));
    ctx.out.resultJson({ ok: true, command: "inbox", items: rows.map((r) => r.view.json), count: rows.length });
    return EXIT.OK;
  } finally { scope.dispose(); }
};
