import { CxbError, type RunResult } from "./contracts";
import { listThreadRuns, readRunLogSnapshot, readThreadIndex, type LogLine } from "./ledger";
import type { CommandContext } from "./main";
import { threadLabel } from "./subagents";
import { sanitizeForTerminal } from "./terminal-text";
import type { ThreadRef } from "./thread-ref";
import type { LogCoverage, LogCoverageIssue, TreeLogEvent } from "./tree-log-types";

const clean = (text: string) => sanitizeForTerminal(text).replace(/[\r\n\u2028\u2029]/g, " ");
const itemKinds = new Set(["条目开始", "条目完成"]);

/** 仅使用事件自身的根身份，绝不按当前血缘把独立运行的日志并进来。 */
export function treeTimeline(rootThreadId: string, lines: LogLine[], records: RunResult[],
  readIssues: LogCoverageIssue[] = []): { items: TreeLogEvent[]; coverage: LogCoverage } {
  const issues: LogCoverageIssue[] = [];
  const keys = new Set<string>();
  const add = (issue: LogCoverageIssue) => {
    const key = `${issue.code}\0${issue.runId ?? ""}\0${issue.threadId ?? ""}`;
    if (!keys.has(key)) { keys.add(key); issues.push(issue); }
  };
  readIssues.forEach(add);
  const runIds = new Set(records.map((run) => run.runId));
  const seen = new Set<string>();
  const items: TreeLogEvent[] = [];
  for (const line of lines) {
    const source = line.source;
    if (source && source.rootThreadId !== rootThreadId) continue;
    if (source && seen.has(source.eventId)) continue;
    if (source) seen.add(source.eventId);
    const scope = { ...(line.runId ? { runId: line.runId } : {}), ...(source ? { threadId: source.threadId } : {}) };
    if (line.event === "丢行") add({ code: "droppedLines", ...scope });
    if (!source && line.runId !== null) add({ code: "legacySourceUnknown", ...scope });
    if (line.runId && !runIds.has(line.runId)) add({ code: "metadataUnavailable", runId: line.runId });
    if (source?.coverageIssue) add({ code: source.coverageIssue, ...scope });
    items.push({ eventId: source?.eventId ?? null, ts: line.ts, runId: line.runId,
      rootThreadId: source?.rootThreadId ?? null, threadId: source?.threadId ?? null,
      parentThreadId: source?.parentThreadId ?? null, shortId: source?.shortId ?? null,
      sourceRole: source?.sourceRole ?? (line.runId === null ? "log" : "legacy"),
      turnId: source?.turnId ?? null, itemId: source?.itemId ?? null,
      kind: clean(line.event), text: clean(line.text), summary: clean(line.content), truncated: source?.truncated ?? false,
      ...(source?.late !== undefined ? { late: source.late } : {}) });
  }
  for (const run of records) {
    if (run.childrenStop?.discovery.complete === false || run.childrenStop?.conflicts.length
      || run.interruptReason === "discoveryIncomplete" || run.interruptReason === "ownershipConflict") {
      add({ code: "discoveryIncomplete", runId: run.runId });
    }
  }
  // 稳定排序保留同毫秒的落盘追加顺序；eventId 只用于去重，不用它重新排序。
  items.sort((a, b) => a.ts.localeCompare(b.ts));
  return { items, coverage: { scope: "retained", issues } };
}

export function treeLogLine(item: TreeLogEvent, labels: ReadonlyMap<string, string>): string {
  const label = (id: string, shortId?: string | null) => threadLabel(id, shortId ?? labels.get(id));
  const source = item.sourceRole === "run" ? `根 ${label(item.threadId!, item.shortId)}`
    : item.sourceRole === "execution" ? `执行 ${label(item.threadId!, item.shortId)}`
    : item.sourceRole === "subagent" ? `子 ${label(item.threadId!, item.shortId)}${item.parentThreadId ? ` ← ${label(item.parentThreadId)}` : ""}`
    : item.sourceRole === "log" ? "日志" : "旧执行源未记录";
  return `${item.ts} [${clean(item.runId ?? "文件")}][${source}] ${item.text}\n`;
}

/** 静态树日志全程读本地文件；不查模型在线摘要，不订阅或恢复子线程。 */
export function showTreeLog(ctx: CommandContext, ref: ThreadRef): 0 {
  const index = readThreadIndex(ctx.workspace.workspaceDir);
  const records = listThreadRuns(ctx.workspace.workspaceDir, ref.threadId).filter((run) => run.threadId === ref.threadId);
  const entry = index.find((item) => item.threadId === ref.threadId);
  if (entry?.parentThreadId && !entry.lastRunId && records.length === 0) {
    const parent = index.find((item) => item.threadId === entry.parentThreadId);
    throw new CxbError("USAGE", `该线程是派生子线程，请使用根线程查看树日志（父线程 ${threadLabel(entry.parentThreadId, parent?.shortId)}）`);
  }
  const snapshot = ref.shortId ? readRunLogSnapshot(ctx.workspace.workspaceDir, ref.shortId)
    : { lines: null, issues: [{ code: "missingLog" as const }] };
  const timeline = treeTimeline(ref.threadId, snapshot.lines ?? [], records, snapshot.issues);
  const tail = !!ctx.parsed.options.tail;
  const items = tail ? timeline.items.slice(-Number(ctx.parsed.options.limit))
    : timeline.items.filter((item) => ctx.parsed.options.full || !itemKinds.has(item.kind));
  const labels = new Map(index.map((item) => [item.threadId, item.shortId]));
  // 历史事件身份优先；索引只给直接父线程提供展示标签，不影响事件归属。
  for (const item of timeline.items) if (item.threadId && item.shortId) labels.set(item.threadId, item.shortId);
  for (const item of items) ctx.out.result(treeLogLine(item, labels));
  if (timeline.coverage.issues.length) ctx.out.notice(`日志仅覆盖保留窗口：${[...new Set(timeline.coverage.issues.map((issue) => issue.code))].join("、")}`);
  ctx.out.resultJson({ ok: true, command: "show", ...ref, tree: true, mode: tail ? "tail" : "log", order: "timestamp",
    items, coverage: timeline.coverage,
    runs: records.map((run) => ({ runId: run.runId, state: run.state, status: run.status, startedAt: run.startedAt })),
    ...(tail ? { next: null } : {}) });
  return 0;
}
