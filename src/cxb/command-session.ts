// src/cxb/command-session.ts — show、ls、rm、inbox、reply、wait、stop 共用的命令会话（S4 N4）：
// 连接与每个 broker 请求都有期限并接命令的取消；线上错误按原因归到统一退出码（命令面规格第 4 节）。
// run、review 有自己的期限模型（整条命令的剩余时间），不走这里。

import { CxbError, rpcFailure, type BrokerRpc } from "./contracts";
import type { CommandContext } from "./main";
import { redactParams } from "./server-requests";
import { bounded } from "./wait-run";

/** 连接 broker 的期限（含拉起），与入口的 CONNECT_TIMEOUT_MS 一致。 */
const CONNECT_LIMIT_MS = 30_000;
/** 普通请求的期限；停止、长等待由调用方显式放宽。 */
const REQUEST_LIMIT_MS = 30_000;

/** 线上错误的原文（broker 转发的上游消息不带前缀，见 RpcError.detail）。 */
export function wireMessage(error: unknown): string {
  const wire = (error instanceof CxbError ? error.cause : error) as { detail?: unknown; message?: unknown } | null;
  return typeof wire?.detail === "string" ? wire.detail : typeof wire?.message === "string" ? wire.message : "";
}

/** 只认上游源码里「线程不存在」的错误形式，不把其他资源的 not found 当作幂等成功
 *  （0.160.0 local/read_thread.rs:94–115、thread_processor.rs:5902、thread_delete.rs）。 */
export function missingThread(error: unknown): boolean {
  const { code } = rpcFailure(error);
  return (code === -32600 && /^(thread not found: \S+|no rollout found for (thread|conversation) id \S+)$/i.test(wireMessage(error)))
    || (code === -32603 && /^failed to archive session: thread \S+ not found$/i.test(wireMessage(error)));
}

export interface CommandSession {
  signal: AbortSignal;
  /** 连接并返回带期限、可取消、错误已归类的 BrokerRpc。 */
  connect(): Promise<BrokerRpc>;
  /** 取消未完成的请求并撤销清理登记；幂等。 */
  dispose(): void;
}

export function commandSession(ctx: CommandContext): CommandSession {
  const controller = new AbortController();
  const unregister = ctx.registerCleanup(() => controller.abort());
  return {
    signal: controller.signal,
    dispose() { controller.abort(); unregister(); },
    async connect(): Promise<BrokerRpc> {
      let rpc: BrokerRpc;
      try { ({ rpc } = await bounded(ctx.connect(), CONNECT_LIMIT_MS, controller.signal)); }
      catch (cause) {
        // 连接层已定好的退出码（版本不符 12、拉起失败 6、取消 4）原样保留，只有超时改报 broker 不可用。
        if (cause instanceof CxbError && cause.code !== "TIMEOUT") throw cause;
        throw new CxbError("BROKER_UNAVAILABLE", "连接 broker 失败或超过期限", { cause, retryable: true });
      }
      ctx.out.progress("已连接工作区 broker");
      return { ...rpc, async request<T>(method: string, params?: unknown, options?: { timeoutMs?: number }): Promise<T> {
        if (controller.signal.aborted) throw new CxbError("INTERRUPTED", "命令已取消");
        const timeoutMs = options?.timeoutMs ?? REQUEST_LIMIT_MS;
        try { return await bounded(rpc.request<T>(method, params, { timeoutMs }), timeoutMs, controller.signal); }
        catch (error) {
          if (error instanceof CxbError && error.code === "INTERRUPTED") throw error;
          const { code, reason } = rpcFailure(error);
          // 归类后的错误仍以线上错误为 cause，调用方可以继续用 rpcFailure 读原因。
          const cause = error instanceof CxbError ? error.cause : error;
          if (reason === "unknownThread" || reason === "notFound" || missingThread(error)) {
            throw new CxbError("NOT_FOUND", "找不到指定线程或事项", { cause });
          }
          // 永久删除进行中，broker 对该线程的请求（watch、purge 除外）一律回 -32001 purgePending（claim/mod.rs）：线程被占，不是 broker 或 Codex 失败。
          if (reason === "purgePending") throw new CxbError("THREAD_HELD", "线程正在永久删除，稍后再试", { cause, retryable: true });
          if (reason === "storageError" || reason === "storageTimeout") throw new CxbError("LOCAL_ERROR", "broker 存储操作失败", { cause });
          const unavailable = code === null || reason === "shuttingDown" || method.startsWith("broker/");
          throw new CxbError(unavailable ? "BROKER_UNAVAILABLE" : "CODEX_FAILED",
            unavailable ? "broker 请求失败或未按期回复，请稍后重试"
              : `Codex 请求失败（${method}，错误码 ${code}）：${String(redactParams(wireMessage(error))).slice(0, 400)}`,
            { cause, retryable: unavailable });
        }
      } };
    },
  };
}
