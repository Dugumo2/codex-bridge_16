// src/cxb/stop-thread.ts — `broker/threads/stop` 的调用与错误归类（S4 N4：stop 命令与 rm 的隐式停止共用；
// 命令面规格 3.8 节、详细设计 2.3 节 stop 的约定）。run 的信号清理有自己的写法（只带 runId 停本次运行），不走这里。

import { CxbError, rpcFailure, type BrokerRpc, type StopReply } from "./contracts";
import { bounded } from "./wait-run";

/** broker 确认窗口 40 秒，客户端至少等 45 秒（S3-计划 8.1、详细设计 2.3 节）。 */
export const STOP_TIMEOUT_MS = 45_000;
/** 共享服务器归属判定中（settling）的重试：间隔与总时长。 */
export const STOP_SETTLING_RETRY_MS = 500;
export const STOP_SETTLING_LIMIT_MS = 10_000;

/** 可取消的等待：取消时同时清掉定时器，不留到点才释放的资源。 */
function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const abort = () => { clearTimeout(timer); reject(new CxbError("INTERRUPTED", "停止已取消")); };
    const timer = setTimeout(() => { signal?.removeEventListener("abort", abort); resolve(); }, ms);
    if (signal?.aborted) { abort(); return; }
    signal?.addEventListener("abort", abort, { once: true });
  });
}

/**
 * 发停止并等 broker 回包；每次请求期限 45 秒，加上 settling 重试，总时长最多约 55 秒。
 * joined（线程上的 turn 属于别的客户端）、stopping（失联 review 的实际执行线程被占）→ THREAD_HELD；
 * settling（共享服务器归属判定中）按间隔重试，超过总时长仍 settling → THREAD_HELD；已取消不再发送；
 * runMismatch（带 runId 且不是这次运行）→ NOT_FOUND；shuttingDown 与连接问题 → BROKER_UNAVAILABLE。
 * `stopOutcome:"unconfirmed"` 不算错误，原样返回，由调用方决定退出码（stop 命令退出 3）。
 */
export async function stopThread(rpc: BrokerRpc, args: { threadId: string; clear?: boolean; runId?: string; abandon?: boolean },
  opts: { signal?: AbortSignal } = {}): Promise<StopReply> {
  const deadline = Date.now() + STOP_SETTLING_LIMIT_MS;
  const params = { ...(args.abandon ? { abandon: true } : {}), threadId: args.threadId, ...(args.clear ? { clear: true } : {}), ...(args.runId ? { runId: args.runId } : {}) };
  for (;;) {
    // 已取消就不再发：停止有副作用，bounded 只能拒绝等待，挡不住已发出的请求。
    if (opts.signal?.aborted) throw new CxbError("INTERRUPTED", "停止已取消", { id: args.threadId });
    try {
      return await bounded(rpc.request<StopReply>("broker/threads/stop", params, { timeoutMs: STOP_TIMEOUT_MS }),
        STOP_TIMEOUT_MS, opts.signal);
    } catch (error) {
      if (error instanceof CxbError && error.code === "INTERRUPTED") throw error;
      // broker 在 40 秒窗口内必回（未确认也回），45 秒没回包是 broker 的问题，不是「停止中」。
      if (error instanceof CxbError && error.code === "TIMEOUT") {
        throw new CxbError("BROKER_UNAVAILABLE", "broker 未在 45 秒内回复停止请求", { id: args.threadId, cause: error, retryable: true });
      }
      const { reason } = rpcFailure(error);
      if (reason === "settling" && Date.now() + STOP_SETTLING_RETRY_MS < deadline) {
        await sleep(STOP_SETTLING_RETRY_MS, opts.signal);
        continue;
      }
      if (reason === "joined" || reason === "settling" || reason === "stopping") {
        throw new CxbError("THREAD_HELD", reason === "joined" ? "线程上的运行属于其他客户端，cxb 不会中断它"
          : reason === "settling" ? "线程正在判定归属，稍后再试"
          // 失联 review 的实际执行线程上已有别的运行或停止操作（stop/mod.rs 的 lost 核实）。
          : "实际执行线程上已有其他运行或停止操作，稍后再试", { id: args.threadId, cause: error, retryable: reason !== "joined" });
      }
      if (reason === "runMismatch") throw new CxbError("NOT_FOUND", "指定的运行不是该线程当前的运行", { id: args.threadId, cause: error });
      if (reason === "shuttingDown") throw new CxbError("BROKER_UNAVAILABLE", "broker 正在关闭", { id: args.threadId, cause: error, retryable: true });
      if (error instanceof CxbError) throw error;
      throw new CxbError("BROKER_UNAVAILABLE", "停止请求失败", { id: args.threadId, cause: error, retryable: true });
    }
  }
}
