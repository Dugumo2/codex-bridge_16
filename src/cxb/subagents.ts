// 子代理的 CLI 展示与错误归类（SA1 接口第 8.2、10、11 节，SA-b）。
//
// broker 写的运行记录带 `children`（受管子线程）与 `childrenStop`（整树停止报告）；这里只读、只展示，
// 不改已有 JSON 字段。文案按报告生成：未确认或有冲突时不能写「已停止它们」，屏障未解除时提示先 wait/stop 核实。
import { sanitizeForTerminal } from "./terminal-text";
import { CxbError, rpcData, rpcFailure, type BackgroundCleanup, type ChildrenStopReport, type ExitCode, type InboxItem, type RunResult } from "./contracts";
import { readPendingInbox, readThreadIndex, type ThreadIndexEntry } from "./ledger";
import type { CommandContext } from "./main";
import { convergenceLines } from "./convergence";
import { resultReady } from "./run-result";

const safe = (text: string) => sanitizeForTerminal(text).replace(/[\r\n\u2028\u2029]/g, " ");

/** \u7ebf\u7a0b\u7684\u5c55\u793a\u6807\u7b7e\uff08wait --tree\u3001show --tree \u5171\u7528\uff09\uff1a\u77ed ID \u4f18\u5148\uff1b\u6ca1\u6709\u77ed ID \u65f6\u7528\u5b8c\u6574\u7ebf\u7a0b ID\uff0c\u4e0d\u622a\u65ad\uff0c\u907f\u514d\u9020\u51fa\u4e0d\u53ef\u89e3\u6790\u6216\u4f1a\u51b2\u7a81\u7684\u6807\u8bc6\u3002 */
export function threadLabel(threadId: string, shortId?: string | null): string {
  return safe(shortId || threadId);
}

/** 运行记录里的子代理线程（去重，fork 与 review 执行线程不计）。 */
export function subagentThreads(run: RunResult): string[] {
  return [...new Set((run.children ?? []).filter((child) => child.relationKind === "subagent").map((child) => child.threadId))];
}

/** API 只证明受管列表状态，不扩大为操作系统进程退出保证。 */
export function backgroundLines(thread: string, report: BackgroundCleanup): string[] {
  const states: Record<string, string> = { pending: "待核验", listing: "正在列举", terminating: "正在终止受管命令", verifying: "后置核验",
    confirmed: "已核验为空", capabilityLimited: "能力受限", notLoaded: "线程未加载，无法核验", unknown: "未知", conflict: "归属冲突", unconfirmed: "尚未确认" };
  const basis = report.verificationBasis;
  if (basis && report.state === "notLoaded") states.notLoaded = basis.source === "thread/closed"
    ? "运行时已关闭，受管后台命令无存活登记" : "当前连接未加载运行时，无存活受管后台命令登记";
  return [`  ${safe(thread)}：${safe(report.scope)}，${states[report.state] ?? "未知"}（${safe(report.state)}）；发现 ${report.discovered}，剩余 ${report.remaining.map(safe).join("、") || "无"}`,
    `    终止回包：${safe(JSON.stringify(report.terminationResults))}；最后核验：${report.lastVerifiedAt == null ? "尚未核验" : new Date(report.lastVerifiedAt).toISOString()}${basis ? `；依据 ${safe(basis.source)}（连接代际 ${basis.connectionEpoch}，观察于 ${new Date(basis.observedAt).toISOString()}）` : ""}${report.failureReason ? `；${safe(report.failureReason)}` : ""}`];
}

/** 停止报告的一行摘要：「父线程已停；3 个子代理已停；1 个尚未确认」，冲突、失败与能力限制另起一行列出。 */
export function stopReportLines(report: ChildrenStopReport): string[] {
  const parts = [report.root.phase === "confirmed" ? "父线程已停" : "父线程尚未确认停下"];
  if (report.stopped.length) parts.push(`${report.stopped.length} 个子代理已停`);
  if (report.unconfirmed.length) parts.push(`${report.unconfirmed.length} 个尚未确认`);
  if (report.failed.length) parts.push(`${report.failed.length} 个核实失败`);
  const lines = [parts.join("；")];
  if (report.cleanupState === "released") lines.push("  历史清理核验已释放：未加载成员缺少完整停止证据；本次不再占用线程或周期重试。");
  if (report.cleanupState === "abandoned") lines.push(`  清理已放弃：${safe(report.abandonReason ?? "显式放弃")}；可能仍有子代理或命令在运行，请换新线程，必要时换隔离工作目录。`);
  if (report.cleanupState === "abandoned" && report.rootRelease) lines.push(`  本次放弃时根线程 ${safe(report.rootRelease.thread)}${report.rootRelease.released ? "已释放，可在同线程发起新运行" : "仍被占用"}：${safe(report.rootRelease.reason)}。`);
  for (const member of [report.root, ...Object.values(report.members ?? {})]) {
    if (member.step || member.lastVerifiedAt != null || member.error) lines.push(`  成员 ${safe(member.thread)}：步骤 ${safe(member.step ?? "未知")}；检查 ${safe(JSON.stringify(member.check))}；最近核实 ${member.lastVerifiedAt == null ? "无" : new Date(member.lastVerifiedAt).toISOString()}${member.error ? `；错误 ${safe(member.error)}` : ""}`);
    if (member.backgroundCleanup) lines.push(...backgroundLines(member.thread, member.backgroundCleanup));
  }
  for (const [thread, missing] of Object.entries(report.discovery.missing ?? {})) lines.push(`  缺证据：${safe(thread)}（${missing.map(safe).join("、")}）`);
  for (const conflict of report.conflicts) lines.push(`  冲突：子线程 ${safe(conflict.thread)}（${safe(conflict.reason)}），未中断`);
  for (const thread of report.capabilityLimited) {
    lines.push(`  能力限制：子线程 ${safe(thread)} 存在能力限制，目标或受管后台命令尚未核验${report.root.clearRequested ? "，未能清除" : ""}`);
  }
  for (const thread of report.unconfirmed) lines.push(`  未确认：子线程 ${safe(thread)}`);
  for (const thread of report.failed) lines.push(`  核实失败：子线程 ${safe(thread)}`);
  if (!report.discovery.complete) {
    lines.push(`  子代理发现不完整${report.discovery.incompleteReasons.length ? `：${safe(report.discovery.incompleteReasons.join("；"))}` : ""}`);
  }
  if (report.unsettledSpawns) lines.push(`  还有 ${report.unsettledSpawns} 个派生尚未落定`);
  return lines;
}

/** 报告是否允许说「已停止它们」：确认、没有冲突、没有能力限制。 */
function fullyStopped(report: ChildrenStopReport): boolean {
  return report.phase === "confirmed" && !report.conflicts.length && !report.capabilityLimited.length && !report.unconfirmed.length;
}

/** 因子代理未完成而中断时的说明（第 8.2 节），按报告分别显示已确认、未确认、冲突与能力限制。 */
export function unfinishedNotice(run: RunResult, label: string): string[] {
  const report = run.childrenStop;
  const lines = ["父线程在子代理完成之前结束了本轮，运行已标为中断（子代理未完成），父轮输出已保留。"];
  if (!report) return [...lines, `子代理的停止报告不可用，请先 cxb wait ${label} 或 cxb stop ${label} 核实。`];
  if (fullyStopped(report)) {
    lines.push(report.stopped.length ? `仍在运行的 ${report.stopped.length} 个子代理已停止。` : "子代理已全部停下。");
  } else {
    lines.push("部分子代理尚未确认停下，不能当作已经停下：");
  }
  lines.push(...stopReportLines(report));
  if (report.phase !== "confirmed" && report.cleanupState !== "abandoned" && report.cleanupState !== "released") lines.push(`子代理树的停止尚未确认，请先 cxb wait ${label} 或 cxb stop ${label} 核实，再决定是否继续。`);
  return lines;
}

/** `show P`、`wait`：子代理数量、待答数、未确认的停止明细；因子代理未完成而中断时显示说明。 */
export function showSubagents(ctx: CommandContext, run: RunResult, options?: { tree?: boolean }): void {
  if (run.finalTurnId !== undefined && resultReady(run) === "unavailable") ctx.out.notice("最终结果不可用（unavailable）");
  for (const line of convergenceLines(run, pendingCount(ctx, run) ?? 0)) ctx.out.notice(line);
  if (run.backgroundCleanup && !run.childrenStop) for (const line of backgroundLines(run.threadId, run.backgroundCleanup)) ctx.out.notice(line);
  const children = subagentThreads(run);
  const report = run.childrenStop;
  if (!children.length && !report && run.interruptReason !== "subagentsUnfinished") return;
  const label = safe(run.shortId || run.threadId);
  if (children.length && !options?.tree) {
    const pending = pendingCount(ctx, run);
    ctx.out.notice(`子代理 ${children.length} 个${pending === null ? "" : `，待答 ${pending} 项`}`);
  }
  if (run.interruptReason === "subagentsUnfinished") {
    for (const line of unfinishedNotice(run, label)) ctx.out.notice(line);
  } else if (report && (report.phase !== "confirmed" || !fullyStopped(report))) {
    for (const line of stopReportLines(report)) ctx.out.notice(line);
  }
}

/** 运行名下（父线程与子线程）仍未决的事项数；本地收件箱不可读时为 null。 */
function pendingCount(ctx: CommandContext, run: RunResult): number | null {
  let items: InboxItem[] | null = null;
  try { items = readPendingInbox(ctx.workspace.workspaceDir); } catch { return null; }
  return items === null ? null : items.filter((item) => item.runId === run.runId && item.kind !== "guardian").length;
}

/** 只作为子代理派生成员的子线程：索引带父线程、关系是 subagent、自己没有 cxb 运行。 */
export function derivedOnly(entry: Pick<ThreadIndexEntry, "parentThreadId" | "relationKind" | "lastRunId">): boolean {
  return typeof entry.parentThreadId === "string" && (entry.relationKind ?? "subagent") === "subagent" && !entry.lastRunId;
}

/** 本地索引里的子线程条目（没有返回 null）。 */
export function childEntry(workspaceDir: string, threadId: string): ThreadIndexEntry | null {
  const entry = readThreadIndex(workspaceDir).find((item) => item.threadId === threadId);
  return entry && typeof entry.parentThreadId === "string" ? entry : null;
}

/**
 * `show C`：没有 RunRecord 的子线程。先说明血缘与能力，再交给服务器读取它最近一轮的结果（读取函数由调用方给出）；
 * 服务器不可用时只输出本地能确定的信息。
 */
export async function showChildThread(ctx: CommandContext, entry: ThreadIndexEntry,
  readServer: (() => Promise<ExitCode>) | null): Promise<ExitCode> {
  const parent = safe(entry.parentThreadId ?? "");
  const capability = entry.canAcceptDirectInput === true ? "可以" : entry.canAcceptDirectInput === false ? "不能" : "未知是否能";
  ctx.out.notice(`子代理线程（父线程 ${parent}，${entry.agentVersion ?? "版本未知"}），${capability}直接输入；它没有自己的 cxb 运行记录，`
    + `受管运行与停止报告见 cxb show ${parent}`);
  if (entry.canAcceptDirectInput === false) ctx.out.notice(`继续工作请从父线程发起：cxb run --resume ${parent} <要求>`);
  if (readServer) {
    try { return await readServer(); } catch (error) {
      const { code, reason } = rpcFailure(error);
      if (!(error instanceof CxbError && error.code === "BROKER_UNAVAILABLE"
        && (code === null || reason === "shuttingDown"))) throw error;
      ctx.out.notice("无法从服务器读取子线程的结果");
    }
  }
  ctx.out.resultJson({ ok: true, command: "show", threadId: entry.threadId, shortId: entry.shortId, mode: "child",
    parentThreadId: entry.parentThreadId, relationKind: entry.relationKind ?? "subagent",
    canAcceptDirectInput: entry.canAcceptDirectInput ?? null, agentVersion: entry.agentVersion ?? null, runId: null });
  return 0;
}

/**
 * broker 对子线程直接输入与树停止屏障的拒绝 → CxbError（SA1 接口第 10 节）：
 * 不接受直接输入退出 1（从父线程继续）；仍在活动、属于未提交结束的运行或树停止未确认退出 8（先 wait 或 stop）。
 */
export function subagentRefusal(error: unknown, label: string): CxbError | null {
  const { code, reason } = rpcFailure(error);
  if (code === null || reason === null) return null;
  const data = rpcData(error);
  const parent = typeof data.parentThreadId === "string" ? safe(data.parentThreadId) : null;
  const opts = { id: label, cause: error };
  switch (reason) {
    case "subagentNoDirectInput":
      return new CxbError("LOCAL_ERROR", `子代理线程 ${label} 不接受直接输入，请从父线程继续`
        + `${parent ? `：cxb run --resume ${parent} <要求>` : ""}。`, opts);
    case "subagentBusy":
      return new CxbError("THREAD_HELD", `子代理线程 ${label} 仍在活动或属于尚未结束的运行，请先 cxb wait ${parent ?? label}`
        + ` 或 cxb stop ${parent ?? label}。`, opts);
    case "treeStopping":
      return new CxbError("THREAD_HELD", `子代理树的停止尚未确认，暂不接受新工作；请先 cxb wait ${label} 或 cxb stop ${label} 核实。`, opts);
    default:
      return null;
  }
}
