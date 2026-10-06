import { sanitizeForTerminal } from "../questions";
import { CxbError, type ThreadSummary } from "./contracts";
import { readRunRecord, readThreadIndex } from "./ledger";
import type { CommandContext, CommandHandler } from "./main";
import { commandSession } from "./command-session";

interface ListItem {
  threadId: string; shortId: string | null; status: string; lastRunId: string | null;
  updatedAt: number; model: string | null; preview: string | null; goal: string | null;
}
interface ServerThread {
  id: string; updatedAt: number; model?: string | null; preview?: string;
  status?: { type: string }; goal?: string | null;
}
const preview = (text: string | null | undefined) => text == null ? null
  : [...sanitizeForTerminal(text).replace(/\s+/gu, " ")].slice(0, 120).join("");
const sorted = (items: Iterable<ListItem>) => [...items].sort((a, b) => b.updatedAt - a.updatedAt || a.threadId.localeCompare(b.threadId));

function localItems(ctx: CommandContext, threads: Array<Pick<ThreadSummary, "threadId" | "shortId" | "lastRunId" | "updatedAt" | "model" | "preview" | "meta"> & { state?: ThreadSummary["state"] }>): ListItem[] {
  return threads.map((thread) => {
    const run = (!thread.state || thread.state === "idle") && thread.lastRunId
      ? readRunRecord(ctx.workspace.workspaceDir, thread.lastRunId) : null;
    const status = thread.state && thread.state !== "idle" ? thread.state
      : run?.state === "lost" ? "lost" : run?.status ?? "idle";
    return { ...thread, status, preview: preview(thread.preview), goal: thread.meta?.goal ?? null };
  });
}

export const ls: CommandHandler = async (ctx) => {
  const session = commandSession(ctx);
  try {
    let source: "local" | "merged" = "local";
    let items: ListItem[];
    let more = false;
    const limit = Number(ctx.parsed.options.limit);
    if (ctx.parsed.options.local) {
      items = localItems(ctx, readThreadIndex(ctx.workspace.workspaceDir));
      ctx.out.progress("已跳过服务器查询：指定了 --local");
    } else {
      let rpc;
      let summaries: ThreadSummary[] | undefined;
      try {
        rpc = await session.connect();
        summaries = (await rpc.request<{ threads: ThreadSummary[] }>("broker/threads/list", {})).threads;
      } catch (error) {
        if (session.signal.aborted || error instanceof CxbError && error.code === "LOCAL_ERROR") throw error;
        ctx.out.notice("无法连接或读取 broker，改为显示本地账本");
      }
      items = localItems(ctx, summaries ?? readThreadIndex(ctx.workspace.workspaceDir));
      if (rpc && summaries) {
        const started = Date.now();
        const merged = new Map(items.map((item) => [item.threadId, item]));
        const seen = new Set<string>();
        let cursor: string | null = null;
        let count = 0;
        try {
          do {
            const page: { data: ServerThread[]; nextCursor: string | null } = await rpc.request("thread/list", {
              cwd: ctx.workspace.root, cursor, limit: 50, sortKey: "updated_at", sortDirection: "desc",
              sourceKinds: ["cli", "vscode", "exec", "appServer"],
            });
            if (!Array.isArray(page.data) || !(page.nextCursor === null || typeof page.nextCursor === "string")
              || page.data.some((thread) => typeof thread.id !== "string" || !Number.isFinite(thread.updatedAt))) {
              throw new CxbError("CODEX_FAILED", "服务器线程列表回包损坏");
            }
            count += page.data.length;
            for (const thread of page.data) {
              if (merged.has(thread.id)) { ctx.out.progress(`跳过重复线程 ${thread.id}：保留本地摘要`); continue; }
              merged.set(thread.id, { threadId: thread.id, shortId: null, status: `server:${thread.status?.type ?? "notLoaded"}`,
                lastRunId: null, updatedAt: thread.updatedAt * 1000, model: thread.model ?? null, preview: preview(thread.preview), goal: thread.goal ?? null });
            }
            cursor = page.nextCursor;
            more = cursor !== null;
            if (cursor !== null && (!cursor || seen.has(cursor))) throw new CxbError("CODEX_FAILED", "服务器线程分页游标没有前进");
            if (cursor !== null) seen.add(cursor);
            const ranked = sorted(merged.values());
            const last = page.data.at(-1);
            // 相等时仍须继续：后页可能含相同时间戳但更靠前的 ID。
            if (!ctx.parsed.options.all && ranked.length >= limit && last && last.updatedAt * 1000 < ranked[limit - 1]!.updatedAt) break;
          } while (cursor !== null);
          items = [...merged.values()];
          source = "merged";
        } catch (error) {
          if (session.signal.aborted) throw error;
          more = false;
          ctx.out.notice("服务器线程查询失败，改为显示本地账本");
        } finally { ctx.out.progress(`服务器发现 ${count} 条线程，耗时 ${Date.now() - started} 毫秒`); }
      }
    }
    const ranked = sorted(items);
    const truncated = more || ranked.length > limit;
    items = ctx.parsed.options.all ? ranked : ranked.slice(0, limit);
    ctx.out.progress(`共发现 ${ranked.length} 条线程，显示 ${items.length} 条`);
    ctx.out.result("SHORT_ID STATUS LAST_RUN UPDATED MODEL PREVIEW\n");
    for (const item of items) ctx.out.result(sanitizeForTerminal(`${item.shortId ?? item.threadId.slice(0, 13)} ${item.status} ${item.lastRunId ?? "-"} ${new Date(item.updatedAt).toISOString()} ${item.model ?? "-"} ${item.preview ?? ""}\n`));
    ctx.out.resultJson({ ok: true, command: "ls", workspace: ctx.workspace.root, source, items, truncated });
    return 0;
  } finally { session.dispose(); }
};
