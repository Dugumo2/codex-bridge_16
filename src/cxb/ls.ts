import { sanitizeForTerminal } from "../questions";
import { CxbError, type BrokerRpc, type LifecycleInfo, type RunResult, type RunModelSummary, type ThreadSummary } from "./contracts";
import { readAllRuns, readLifecycle, readPendingInbox, readThreadIndex, type ThreadIndexEntry } from "./ledger";
import { modelLabel, threadModelDisplay, type ModelDisplay } from "./show-model";
import { backgroundLines, derivedOnly, stopReportLines } from "./subagents";
import { lifecycleLabel } from "./lifecycle";
import type { CommandContext, CommandHandler } from "./main";
import { commandSession } from "./command-session";
import { convergenceFields, resultReady, resultTurn, selectedResultFields } from "./run-result";
import { convergenceLines } from "./convergence";
import { upstreamFields } from "./upstream-errors";
import type { ConvergenceSummary } from "./sa1b-contracts";

interface ListItem extends ModelDisplay, ConvergenceSummary {
  upstreamError?: import("./contracts").UpstreamError;
  threadId: string; shortId: string | null; status: string; lastRunId: string | null;
  updatedAt: number; model: string | null; preview: string | null; goal: string | null;
  /** 待核实的归档或删除（`--json` 原样带出）。 */
  lifecycle?: LifecycleInfo;
  /** 默认隐藏的、只作为派生成员的子代理线程（父行显示数量，`--json` 带出 ID）。 */
  subagents?: string[];
  backgroundCleanup?: import("./contracts").BackgroundCleanup;
  childrenStop?: import("./contracts").ChildrenStopReport;
  result?: string;
  ready?: "pending" | "ready" | "unavailable";
  origin?: RunResult["turns"][number]["origin"];
  stageResult?: boolean;
  pendingCount?: number;
}
interface ServerThread {
  id: string; updatedAt: number; model?: string | null; preview?: string;
  status?: { type: string }; goal?: string | null;
}
const preview = (text: string | null | undefined) => text == null ? null
  : [...sanitizeForTerminal(text).replace(/\s+/gu, " ")].slice(0, 120).join("");
const sorted = (items: Iterable<ListItem>) => [...items].sort((a, b) => b.updatedAt - a.updatedAt || a.threadId.localeCompare(b.threadId));

type LocalThread = Pick<ThreadSummary, "threadId" | "shortId" | "lastRunId" | "updatedAt" | "model" | "preview" | "meta">
  & { state?: ThreadSummary["state"]; lifecycle?: LifecycleInfo };

function localItems(threads: LocalThread[], runs: RunResult[]): ListItem[] {
  const byId = new Map(runs.map((run) => [run.runId, run]));
  return threads.map((thread) => {
    // 待核实的归档或删除优先显示（门禁挡着，线程不能照常使用）；只剩记录的线程没有运行可读。
    const run = thread.lastRunId ? byId.get(thread.lastRunId) : null;
    const status = thread.lifecycle ? lifecycleLabel(thread.lifecycle) : thread.state && thread.state !== "idle" ? thread.state
      : run?.state === "running" || run?.state === "stopping" || run?.state === "lost" ? run.state : run?.status ?? "idle";
    const selected = run ? resultTurn(run) : undefined;
    return { ...thread, ...threadModelDisplay(thread.threadId, thread.model, []),
      ...(run ? { ...convergenceFields(run), ...selectedResultFields(run), ...upstreamFields(run) } : {}),
      ...(run?.finalTurnId !== undefined || run?.subagentJoin ? { result: selected?.ready === "ready" ? selected.output : "", ready: resultReady(run),
        ...(run.state === "running" || run.state === "stopping" ? { stageResult: true } : {}) } : {}),
      ...(run?.backgroundCleanup ? { backgroundCleanup: run.backgroundCleanup } : {}), ...(run?.childrenStop ? { childrenStop: run.childrenStop } : {}), status, preview: preview(thread.preview), goal: thread.meta?.goal ?? null };
  });
}

/** 本地索引并上 lifecycle.json：索引项带上记录；purge 已删索引、只剩记录的线程按记录补一项。 */
function localThreads(workspaceDir: string): LocalThread[] {
  const records = new Map(readLifecycle(workspaceDir).map((record) => [record.threadId, record]));
  const threads: LocalThread[] = readThreadIndex(workspaceDir).map((entry) => {
    const record = records.get(entry.threadId);
    records.delete(entry.threadId);
    return record ? { ...entry, lifecycle: record } : entry;
  });
  for (const record of records.values()) {
    threads.push({ threadId: record.threadId, shortId: record.shortId ?? record.threadId, lastRunId: null, updatedAt: record.updatedAt,
      model: null, preview: record.preview, meta: null, lifecycle: record });
  }
  return threads;
}

/** 隐藏只作为派生成员的子线程，并把它们记到父线程那一行（父线程不在列表里时照常列出子线程）。 */
function foldSubagents(items: ListItem[], index: ThreadIndexEntry[]): ListItem[] {
  const derived = new Map(index.filter((entry) => derivedOnly(entry)).map((entry) => [entry.threadId, entry.parentThreadId!]));
  const present = new Set(items.map((item) => item.threadId));
  const counts = new Map<string, string[]>();
  const kept = items.filter((item) => {
    const parent = derived.get(item.threadId);
    if (!parent || !present.has(parent)) return true;
    counts.set(parent, [...counts.get(parent) ?? [], item.threadId]);
    return false;
  });
  return kept.map((item) => counts.has(item.threadId) ? { ...item, subagents: counts.get(item.threadId)!.sort() } : item);
}

export const ls: CommandHandler = async (ctx) => {
  const session = commandSession(ctx);
  try {
    let source: "local" | "merged" = "local";
    let items: ListItem[];
    let more = false;
    // 全部线程共享一次磁盘扫描，不能只看索引的 lastRunId。
    const diskRuns = readAllRuns(ctx.workspace.workspaceDir);
    let onlineRpc: BrokerRpc | undefined;
    const limit = Number(ctx.parsed.options.limit);
    if (ctx.parsed.options.local) {
      items = localItems(localThreads(ctx.workspace.workspaceDir), diskRuns);
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
      items = localItems(summaries ?? localThreads(ctx.workspace.workspaceDir), diskRuns);
      if (rpc && summaries) {
        onlineRpc = rpc;
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
                lastRunId: null, updatedAt: thread.updatedAt * 1000, ...threadModelDisplay(thread.id, thread.model ?? null, []), preview: preview(thread.preview), goal: thread.goal ?? null });
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
    // SA1 第 11 节：只作为派生成员的子线程默认隐藏，父行显示数量；有独立 cxb 运行的子线程照常列出。
    let index: ThreadIndexEntry[] = [];
    try { index = readThreadIndex(ctx.workspace.workspaceDir); } catch { /* 索引不可读时不折叠，照常列出。 */ }
    items = foldSubagents(items, index);
    const ranked = sorted(items);
    const truncated = more || ranked.length > limit;
    items = ctx.parsed.options.all ? ranked : ranked.slice(0, limit);
    let online: RunModelSummary[] = [];
    if (onlineRpc && items.length) {
      try {
        online = (await onlineRpc.request<{ runs: RunModelSummary[] }>("broker/runs/modelSummaries",
          { threadIds: [...new Set(items.map((item) => item.threadId))] })).runs;
      } catch (error) {
        if (session.signal.aborted) throw error;
        ctx.out.notice("无法读取在线运行摘要，模型来源按本地账本显示");
      }
    }
    for (const item of items) {
      Object.assign(item, threadModelDisplay(item.threadId, item.model, diskRuns, online));
      // 诊断严格按列表项 lastRunId 关联；review／无模型／未确认启动仍是有效诊断来源。
      const run = online?.find((candidate) => candidate.runId === item.lastRunId)
        ?? diskRuns.find((candidate) => candidate.runId === item.lastRunId);
      if (run?.upstreamError) Object.assign(item, upstreamFields(run));
      else delete item.upstreamError;
    }
    const pending = readPendingInbox(ctx.workspace.workspaceDir);
    for (const item of items) if (item.subagentJoin && pending && item.pendingCount === undefined) {
      item.pendingCount = pending.filter((entry) => entry.runId === item.lastRunId && entry.kind !== "guardian").length;
    }
    ctx.out.progress(`共发现 ${ranked.length} 条线程，显示 ${items.length} 条`);
    ctx.out.result("SHORT_ID STATUS LAST_RUN UPDATED MODEL REASONING PREVIEW\n");
    for (const item of items) {
      const error = item.upstreamError;
      const status = `${item.status}${error ? `（上游 ${error.httpStatusCode ?? error.category}×${error.count}）` : ""}`;
      ctx.out.result(sanitizeForTerminal(`${item.shortId ?? item.threadId.slice(0, 13)} ${status} ${item.lastRunId ?? "-"} ${new Date(item.updatedAt).toISOString()} ${modelLabel(item)} ${item.reasoning ?? "-"} ${item.subagents?.length ? `[子代理 ${item.subagents.length}] ` : ""}${item.preview ?? ""}\n`));
    }
    for (const item of items) if (item.childrenStop) for (const line of stopReportLines(item.childrenStop)) ctx.out.notice(`${item.shortId ?? item.threadId} ${line}`);
    for (const item of items) if (item.backgroundCleanup && !item.childrenStop) for (const line of backgroundLines(item.threadId, item.backgroundCleanup)) ctx.out.notice(line);
    for (const item of items) {
      for (const line of convergenceLines(item, item.pendingCount ?? 0)) ctx.out.notice(`${item.shortId ?? item.threadId} ${line}`);
      if (item.result) ctx.out.result(`${item.stageResult ? "阶段结果" : "最终结果"}：${preview(item.result)}\n`);
      if (item.ready === "pending") ctx.out.notice(`${item.shortId ?? item.threadId} 结果尚未就绪，请用 cxb wait 核实`);
      if (item.ready === "unavailable") ctx.out.notice(`${item.shortId ?? item.threadId} 最终结果不可用（unavailable）`);
    }
    ctx.out.resultJson({ ok: true, command: "ls", workspace: ctx.workspace.root, source, items, truncated });
    return 0;
  } finally { session.dispose(); }
};
