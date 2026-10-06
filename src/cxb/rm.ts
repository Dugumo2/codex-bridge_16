import { CxbError, rpcFailure, type BrokerRpc, type PurgeReply, type RunResult, type ThreadSummary } from "./contracts";
import { readRunRecord } from "./ledger";
import type { CommandContext, CommandHandler } from "./main";
import { commandSession, missingThread, wireMessage } from "./command-session";
import { redactParams } from "./server-requests";
import { stopThread } from "./stop-thread";
import { resolveThreadViaBroker } from "./thread-ref";

/** 重试等待可取消，定时器在取消或正常唤醒后都回收。 */
function pause(signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const abort = () => { clearTimeout(timer); signal.removeEventListener("abort", abort); reject(new CxbError("INTERRUPTED", "删除已取消")); };
    const timer = setTimeout(() => { signal.removeEventListener("abort", abort); resolve(); }, 500);
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) abort();
  });
}

async function purgeLocal(rpc: BrokerRpc, threadId: string, signal: AbortSignal): Promise<PurgeReply> {
  const deadline = Date.now() + 10_000;
  for (;;) {
    try { return await rpc.request<PurgeReply>("broker/threads/purge", { threadId }); }
    catch (error) {
      const { reason } = rpcFailure(error);
      if (reason === "settling" && Date.now() + 500 < deadline) { await pause(signal); continue; }
      if (reason === "settling") throw new CxbError("THREAD_HELD", "线程仍在判定归属，请稍后重试", { retryable: true });
      if (reason === "threadBusy" || reason === "stopping") throw new CxbError("THREAD_HELD", `线程仍在运行或停止中，请先 cxb stop ${threadId}`);
      if (reason === "purgePending") throw new CxbError("THREAD_HELD", "请先处理 cxb inbox 中该线程的 gd_ 事项，或稍后重试", { retryable: true });
      throw error;
    }
  }
}

async function purge(ctx: CommandContext, rpc: BrokerRpc, id: string, threadId: string, signal: AbortSignal) {
  const local = await purgeLocal(rpc, threadId, signal);
  const deleted = [...local.deleted];
  const alreadyDeleted = [...local.alreadyDeleted];
  const failed = local.failed.map((failure) => ({ path: String(redactParams(failure.path)), reason: String(redactParams(failure.reason)) }));
  if (!failed.length) {
    try { await rpc.request("thread/delete", { threadId }); deleted.push("server-thread"); }
    catch (error) {
      if (signal.aborted) throw error;
      if (missingThread(error)) alreadyDeleted.push("server-thread");
      else failed.push({ path: "server-thread", reason: rpcFailure(error).code === -32601
        ? "此 Codex 版本不支持 thread/delete，服务器线程保留"
        : error instanceof CxbError ? error.message : "服务器线程删除失败" });
    }
  }
  ctx.out.resultJson({ ok: !failed.length, command: "rm", id, threadId, mode: "purge", deleted, alreadyDeleted, failed, recoverable: false });
  if (failed.length) {
    for (const failure of failed) ctx.out.notice(`删除失败：${failure.path}（${failure.reason}）`);
    return 1 as const;
  }
  ctx.out.result(`已永久删除线程 ${id}。\n`);
  return 0 as const;
}

export const rm: CommandHandler = async (ctx) => {
  const session = commandSession(ctx);
  try {
    const rpc = await session.connect();
    const id = ctx.parsed.positional[0]!;
    const ref = await resolveThreadViaBroker(rpc, id, { passThrough: true });
    if (ctx.parsed.options.purge) return await purge(ctx, rpc, id, ref.threadId, session.signal);
    let snapshot: { thread: ThreadSummary; run: RunResult | null } | null = null;
    try { snapshot = await rpc.request("broker/threads/watch", { threadId: ref.threadId }); }
    catch (error) {
      // 无本地摘要且 broker 明确不认识时，交给服务器判定；其他错误不能绕过停止检查。
      if (ref.summary !== null || rpcFailure(error).reason !== "unknownThread") throw error;
    }
    const run = snapshot?.run ?? (snapshot?.thread.lastRunId ? readRunRecord(ctx.workspace.workspaceDir, snapshot.thread.lastRunId) : null);
    let stopped = false;
    if (snapshot && (snapshot.thread.state !== "idle" || run?.state === "running" || run?.state === "stopping"
      || (run?.state === "lost" && run.stopOutcome !== "confirmed"))) {
      const stop = await stopThread(rpc, { threadId: ref.threadId }, { signal: session.signal });
      if (stop.stopOutcome !== "confirmed") {
        ctx.out.notice(`停止中（未确认），未归档；请先 cxb stop ${id} 核实`);
        return 3;
      }
      stopped = true;
    }
    let archived: true | "alreadyDone" = true;
    try { await rpc.request("thread/archive", { threadId: ref.threadId }); }
    catch (error) {
      if (!missingThread(error) && !(rpcFailure(error).code === -32600 && /^thread \S+ is archived$/.test(wireMessage(error)))) throw error;
      archived = "alreadyDone";
      ctx.out.notice("服务器上已不存在或已归档，本地索引仍保留");
    }
    ctx.out.result(`已归档线程 ${ref.shortId ?? ref.threadId}。\n`);
    ctx.out.resultJson({ ok: true, command: "rm", id, threadId: ref.threadId, mode: "archive", stopped, archived,
      deleted: archived === true ? ["local-index"] : [], recoverable: true });
    return 0;
  } finally { session.dispose(); }
};
