// src/cxb/thread-ref.ts — 线程 ID 解析（S4 N4 起 run、show、stop、rm、wait 共用；命令面规格 3.3 节：
// 短 ID、唯一前缀、服务器 UUID）。规则：先精确匹配 threadId 或 shortId，再按前缀匹配两者；多于一个报歧义。

import { CxbError, type BrokerRpc, type ThreadSummary } from "./contracts";
import { readLifecycle, readThreadIndex, type LifecycleRecord } from "./ledger";

export interface ThreadRef {
  threadId: string;
  /** broker 索引里的短 ID；索引里没有（只在服务器上、或原样交给服务器判定）时为 null。 */
  shortId: string | null;
}

interface Candidate { threadId: string; shortId: string }

/** 精确优先，其次唯一前缀；多个前缀匹配抛 AMBIGUOUS（退出 2），零个返回 null。 */
export function matchThread<T extends Candidate>(ref: string, candidates: readonly T[]): T | null {
  const exact = candidates.find((c) => c.threadId === ref || c.shortId === ref);
  if (exact) return exact;
  // 每个候选至多出现一次：同一线程的两个字段都命中前缀也只算一个。
  const matches = candidates.filter((c) => c.threadId.startsWith(ref) || c.shortId.startsWith(ref));
  if (matches.length > 1) {
    throw new CxbError("AMBIGUOUS", `线程 ID 前缀 ${ref} 不唯一：${matches.slice(0, 5).map((c) => c.shortId).join("、")}${matches.length > 5 ? " 等" : ""}`, { id: ref });
  }
  return matches[0] ?? null;
}

function notFound(ref: string): CxbError {
  return new CxbError("NOT_FOUND", "找不到指定线程，请检查 ID 与工作区（-d）", { id: ref });
}

/**
 * 用 broker 的线程列表（`broker/threads/list`，含索引与当前认领）解析。
 * 找不到时：`passThrough` 为 true 则原样返回（shortId 为 null，由服务器判定，例如只在服务器上的线程），否则 NOT_FOUND。
 * 返回值附带命中的 ThreadSummary（找不到时为 null），调用方不必再查一次列表。
 */
export async function resolveThreadViaBroker(rpc: Pick<BrokerRpc, "request">, ref: string, opts: { passThrough?: boolean } = {}):
  Promise<ThreadRef & { summary: ThreadSummary | null }> {
  const list = await rpc.request<{ threads: ThreadSummary[] }>("broker/threads/list", {});
  const hit = matchThread(ref, list.threads);
  if (hit) return { threadId: hit.threadId, shortId: hit.shortId, summary: hit };
  if (opts.passThrough) return { threadId: ref, shortId: null, summary: null };
  throw notFound(ref);
}

/**
 * 用本地 `threads.json` 与 `lifecycle.json` 解析（不连 broker）；找不到抛 NOT_FOUND，任一文件损坏抛 LOCAL_ERROR。
 * purge 已删索引、删除结果待核实的线程只剩生命周期记录，仍能用原短 ID 找到（任务说明 N7-A 5.5 节），这时附带记录。
 */
export function resolveThreadLocal(workspaceDir: string, ref: string): ThreadRef & { lifecycle?: LifecycleRecord } {
  const records = readLifecycle(workspaceDir);
  const index = readThreadIndex(workspaceDir);
  const indexed = new Set(index.map((entry) => entry.threadId));
  const candidates = [...index, ...records.filter((record) => !indexed.has(record.threadId))
    .map((record) => ({ threadId: record.threadId, shortId: record.shortId ?? record.threadId }))];
  const hit = matchThread(ref, candidates);
  if (!hit) throw notFound(ref);
  const lifecycle = records.find((record) => record.threadId === hit.threadId);
  return { threadId: hit.threadId, shortId: indexed.has(hit.threadId) ? hit.shortId : lifecycle?.shortId ?? null, ...(lifecycle ? { lifecycle } : {}) };
}
