// src/cxb/lifecycle.ts — 线程归档、永久删除的待核实状态在 CLI 上的展示与提示（任务说明 N7-A 2.3、5.5 节）。
// broker 是唯一判定者：这里只把它给出的记录和错误原因翻成用户看得懂的状态与下一步命令。

import { sanitizeForTerminal } from "../questions";
import { CxbError, rpcData, type LifecycleInfo } from "./contracts";

const COMMITTING = new Set(["committing", "upstreamDone", "draining"]);

/** 状态栏文字：归档待核实、删除待核实，或上游已完成、本地仍在提交时的「归档提交中」「删除提交中」。 */
export function lifecycleLabel(info: Pick<LifecycleInfo, "kind" | "phase">): string {
  return `${info.kind === "delete" ? "删除" : "归档"}${COMMITTING.has(info.phase) ? "提交中" : "待核实"}`;
}

/** 完成核实要重新执行的命令；kind 不同的命令会被 broker 一直拒绝，必须给对。 */
export function retryCommand(kind: LifecycleInfo["kind"], id: string): string {
  return kind === "delete" ? `cxb rm ${id} --purge` : `cxb rm ${id}`;
}

/** 共享服务器上无法证明旧请求已结束时线程会一直待核实（2.3.8 节），提示里不暗示重试一定能解除。 */
export function lifecycleHint(kind: LifecycleInfo["kind"], phase: string, id: string): string {
  if (phase === "draining") {
    return `线程已${kind === "delete" ? "删除" : "归档"}，仍在等此前发出的请求结束；共享服务器上的旧请求无法确认已结束时，线程会保持待核实`;
  }
  return `线程正在${kind === "delete" ? "永久删除" : "归档"}，或上次结果待核实；重新执行 ${retryCommand(kind, id)} 完成核实`
    + "（共享服务器上的旧请求无法确认已结束时，线程会保持待核实）";
}

/**
 * broker 回 -32001 lifecyclePending 时的统一错误（退出 8，可重试）。
 * 提示里的线程取回包的 `data.threadId`（被占的可能是关联子线程，不是调用方给的线程）；缺失时才用调用方的 ID。
 */
export function lifecyclePendingError(error: unknown, fallbackId: string): CxbError {
  const data = rpcData(error);
  const kind = data.kind === "delete" ? "delete" : "archive";
  const phase = typeof data.phase === "string" ? data.phase : "outcomeUnknown";
  const id = typeof data.threadId === "string" && data.threadId
    ? sanitizeForTerminal(data.threadId).replace(/\s+/gu, " ") : fallbackId;
  const cause = error instanceof CxbError ? error.cause : error;
  return new CxbError("THREAD_HELD", lifecycleHint(kind, phase, id), { cause, retryable: true, id });
}
