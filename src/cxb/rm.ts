import { CxbError, EXIT, rpcData, rpcFailure, type ArchiveReply, type BrokerRpc, type LifecycleBlocker, type LifecycleInfo,
  type PurgeReply, type RunResult, type ThreadSummary } from "./contracts";
import { safeLine } from "./inbox";
import { readRunRecord } from "./ledger";
import { lifecycleHint } from "./lifecycle";
import type { CommandContext, CommandHandler } from "./main";
import { commandSession } from "./command-session";
import { redactParams } from "./server-requests";
import { stopThread } from "./stop-thread";
import { resolveThreadViaBroker } from "./thread-ref";

/** 客户端期限覆盖 broker 的整体预算加余量（任务说明 N7-A 2.3.7 节）：归档 70 秒、purge（含服务器删除）100 秒。 */
const ARCHIVE_LIMIT_MS = 90_000;
const PURGE_LIMIT_MS = 120_000;

/** 重试等待可取消，定时器在取消或正常唤醒后都回收。 */
function pause(signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const abort = () => { clearTimeout(timer); signal.removeEventListener("abort", abort); reject(new CxbError("INTERRUPTED", "删除已取消")); };
    const timer = setTimeout(() => { signal.removeEventListener("abort", abort); resolve(); }, 500);
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) abort();
  });
}

const BLOCKER_REASONS: Record<string, string> = {
  running: "运行中", stopping: "停止中", lostUnconfirmed: "失联且未确认停下", settling: "正在判定归属", stopInProgress: "停止操作进行中",
  joined: "属于其他客户端", reading: "正在补读结果", purgePending: "永久删除进行中", guardianPending: "有未处理的 gd_ 事项",
  externalTurn: "共享服务器上有别的客户端开始了新一轮",
  // SA1 第 10 节：子代理树的安全检查集合。
  hasSubtree: "有子代理子树（删除或归档会连带子树）", subagentInUse: "是子代理且正被运行使用",
  discoveryIncomplete: "子代理发现不完整", treeStopUnconfirmed: "子代理树的停止尚未确认",
  childRequestsPending: "子代理有待答事项", relationsChanged: "检查期间子代理关系变化",
};

/** 阻塞项的下一步：失联或停止中的历史运行按 runId 核实，运行中的先停，gd_ 事项先处理。 */
function blockerHint(blocker: LifecycleBlocker): string {
  const thread = safeLine(blocker.threadId);
  if ((blocker.reason === "lostUnconfirmed" || blocker.reason === "stopping") && blocker.runId) return `cxb stop ${thread} --run ${safeLine(blocker.runId)}`;
  if (blocker.reason === "running") return `cxb stop ${thread}`;
  if (blocker.reason === "guardianPending") return "cxb inbox 处理 gd_ 事项";
  if (blocker.reason === "hasSubtree") return "子代理线程随父线程保留，不能单独删除父线程";
  if (blocker.reason === "subagentInUse" || blocker.reason === "treeStopUnconfirmed") return `cxb wait ${thread} 或 cxb stop ${thread}`;
  if (blocker.reason === "childRequestsPending") return "cxb inbox 处理待答事项";
  return "稍后重试";
}

function blockersOf(error: unknown): LifecycleBlocker[] {
  const list = rpcData(error).blockers;
  return Array.isArray(list) ? list.filter((item): item is LifecycleBlocker => !!item && typeof item === "object"
    && typeof (item as LifecycleBlocker).threadId === "string" && typeof (item as LifecycleBlocker).reason === "string")
    .map((item) => ({ threadId: item.threadId, runId: typeof item.runId === "string" ? item.runId : null, reason: item.reason })) : [];
}

/** 有阻塞项时如实列出并退出 8；不自动停止任何运行（方案 ②：忙则拒绝）。 */
function reportBlockers(ctx: CommandContext, id: string, threadId: string, mode: "archive" | "purge", blockers: LifecycleBlocker[]) {
  ctx.out.notice(`线程仍有未结束的运行，未${mode === "archive" ? "归档" : "删除"}：`);
  for (const blocker of blockers) {
    ctx.out.notice(`  线程 ${safeLine(blocker.threadId)}${blocker.runId ? ` 运行 ${safeLine(blocker.runId)}` : ""}：${BLOCKER_REASONS[blocker.reason] ?? safeLine(blocker.reason)}；${blockerHint(blocker)}`);
  }
  ctx.out.resultJson({ ok: false, command: "rm", id, threadId, mode, blockers });
  return EXIT.THREAD_HELD;
}

/** 生命周期操作的线上错误统一归类（2.3.2 节）；阻塞项由调用方单独展示。 */
function lifecycleFailure(error: unknown, kind: LifecycleInfo["kind"], id: string): CxbError {
  if (error instanceof CxbError && error.code === "INTERRUPTED") return error;
  const { reason } = rpcFailure(error);
  const cause = error instanceof CxbError ? error.cause : error;
  if (reason === "outcomeUnknown") return new CxbError("BROKER_UNAVAILABLE", `Codex 未按期回复，${lifecycleHint(kind, "outcomeUnknown", id)}`, { cause, retryable: true, id });
  if (reason === "upstreamRejected") {
    const upstream = rpcData(error).upstream as { code?: unknown; message?: unknown } | undefined;
    return new CxbError("CODEX_FAILED", `Codex 拒绝${kind === "delete" ? "删除" : "归档"}（错误码 ${String(upstream?.code ?? "未知")}）：`
      + `${String(redactParams(typeof upstream?.message === "string" ? upstream.message : "")).slice(0, 400)}`, { cause, id });
  }
  if (reason === "storageError" || reason === "storageTimeout") {
    const done = rpcData(error).upstreamDone === true;
    return new CxbError("BROKER_UNAVAILABLE", done ? "服务器已完成，本地索引尚未提交，重试同一命令即可"
      : "broker 存储操作失败或超过期限，未完成；稍后重试", { cause, retryable: true, id });
  }
  return error instanceof CxbError ? error : new CxbError("BROKER_UNAVAILABLE", "broker 请求失败", { cause, retryable: true, id });
}

async function purgeOnce(rpc: BrokerRpc, threadId: string, signal: AbortSignal): Promise<PurgeReply> {
  const deadline = Date.now() + 10_000;
  for (;;) {
    try { return await rpc.request<PurgeReply>("broker/threads/purge", { threadId, serverDelete: true }, { timeoutMs: PURGE_LIMIT_MS }); }
    catch (error) {
      const { reason } = rpcFailure(error);
      if (reason === "settling" && Date.now() + 500 < deadline) { await pause(signal); continue; }
      if (reason === "settling") throw new CxbError("THREAD_HELD", "线程仍在判定归属，请稍后重试", { retryable: true });
      if (reason === "stopping") throw new CxbError("THREAD_HELD", `线程仍在停止中，请先 cxb stop ${threadId}`);
      if (reason === "purgePending") throw new CxbError("THREAD_HELD", "请先处理 cxb inbox 中该线程的 gd_ 事项，或稍后重试", { retryable: true });
      throw error;
    }
  }
}

async function purge(ctx: CommandContext, rpc: BrokerRpc, id: string, threadId: string, signal: AbortSignal) {
  let reply: PurgeReply;
  try { reply = await purgeOnce(rpc, threadId, signal); }
  catch (error) {
    if (rpcFailure(error).reason === "threadBusy") return reportBlockers(ctx, id, threadId, "purge", blockersOf(error));
    if (error instanceof CxbError && error.code === "THREAD_HELD") throw error;
    throw lifecycleFailure(error, "delete", id);
  }
  const failed = reply.failed.map((failure) => ({ path: String(redactParams(failure.path)), reason: String(redactParams(failure.reason)) }));
  ctx.out.resultJson({ ok: !failed.length, command: "rm", id, threadId, mode: "purge", deleted: reply.deleted,
    alreadyDeleted: reply.alreadyDeleted, failed, operationId: reply.operationId ?? null, recoverable: false });
  if (failed.length) {
    for (const failure of failed) {
      ctx.out.notice(failure.reason === "outcomeUnknown"
        ? `删除失败：${failure.path}（Codex 未按期回复，${lifecycleHint("delete", "outcomeUnknown", id)}）`
        : `删除失败：${failure.path}（${failure.reason}）`);
    }
    return EXIT.USAGE_OR_LOCAL_ERROR;
  }
  ctx.out.result(`已永久删除线程 ${id}。\n`);
  return EXIT.OK;
}

export const rm: CommandHandler = async (ctx) => {
  const session = commandSession(ctx);
  try {
    const rpc = await session.connect();
    const id = ctx.parsed.positional[0]!;
    const ref = await resolveThreadViaBroker(rpc, id, { passThrough: true });
    if (ctx.parsed.options.purge) return await purge(ctx, rpc, id, ref.threadId, session.signal);
    let snapshot: { thread: ThreadSummary; run: RunResult | null; lifecycle?: LifecycleInfo } | null = null;
    try { snapshot = await rpc.request("broker/threads/watch", { threadId: ref.threadId }); }
    catch (error) {
      // 无本地摘要且 broker 明确不认识时，交给 broker 的归档操作判定；其他错误不能绕过检查。
      if (ref.summary !== null || rpcFailure(error).reason !== "unknownThread") throw error;
    }
    const lifecycle = snapshot?.lifecycle ?? snapshot?.thread.lifecycle ?? ref.summary?.lifecycle;
    if (lifecycle?.kind === "delete") throw new CxbError("THREAD_HELD", lifecycleHint("delete", lifecycle.phase, id), { retryable: true, id });
    let stopped = false;
    // 有待核实的归档时不做便利停止：门禁会拒绝停止，直接进入归档完成核实（2.3.1 节）。
    if (!lifecycle) {
      const run = snapshot?.run ?? (snapshot?.thread.lastRunId ? readRunRecord(ctx.workspace.workspaceDir, snapshot.thread.lastRunId) : null);
      if (snapshot && (snapshot.thread.state !== "idle" || run?.state === "running" || run?.state === "stopping"
        || (run?.state === "lost" && run.stopOutcome !== "confirmed"))) {
        const stop = await stopThread(rpc, { threadId: ref.threadId }, { signal: session.signal });
        if (stop.stopOutcome !== "confirmed") {
          ctx.out.notice(`停止中（未确认），未归档；请先 cxb stop ${id} 核实`);
          return EXIT.TIMEOUT;
        }
        stopped = true;
      }
    }
    let reply: ArchiveReply;
    try { reply = await rpc.request<ArchiveReply>("broker/threads/archive", { threadId: ref.threadId }, { timeoutMs: ARCHIVE_LIMIT_MS }); }
    catch (error) {
      if (rpcFailure(error).reason === "threadBusy") return reportBlockers(ctx, id, ref.threadId, "archive", blockersOf(error));
      if (error instanceof CxbError && error.code === "THREAD_HELD") throw error;
      throw lifecycleFailure(error, "archive", id);
    }
    if (reply.archived === "alreadyDone") ctx.out.notice("服务器上已不存在或已归档，本地索引已清理");
    ctx.out.result(`已归档线程 ${ref.shortId ?? ref.threadId}。\n`);
    ctx.out.resultJson({ ok: true, command: "rm", id, threadId: ref.threadId, mode: "archive", stopped, archived: reply.archived,
      localIndex: reply.localIndex, operationId: reply.operationId, recoverable: true });
    return EXIT.OK;
  } finally { session.dispose(); }
};
