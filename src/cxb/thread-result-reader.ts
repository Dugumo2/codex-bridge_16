import type { BrokerRpc } from "./contracts";
import { serverOutput } from "./show";
import type { TreeNode, TreeNodeResult } from "./tree-contracts";

const TURN_STATUSES = new Set(["inProgress", "completed", "failed", "interrupted"]);
const THREAD_BYTES = 8 * 1024;
const TOTAL_BYTES = 64 * 1024;
type Item = Parameters<typeof serverOutput>[0][number];
type Turn = { id: string; status: NonNullable<TreeNodeResult["status"]> };

class ReadFailure extends Error {}
function unavailable(reason: string, turn?: Turn, availability: "unavailable" | "omitted" = "unavailable"): TreeNodeResult {
  return { turnId: turn?.id ?? null, status: turn?.status ?? null, source: turn ? "server" : null,
    availability, output: "", outputSource: "none", truncated: false, reason };
}

/** 按 UTF-8 字节裁剪，不能切断 Unicode 码点。 */
function clip(text: string, bytes: number): string {
  let used = 0;
  let result = "";
  for (const char of text) {
    const size = Buffer.byteLength(char, "utf8");
    if (used + size > bytes) break;
    used += size;
    result += char;
  }
  return result;
}

/** 传输只有请求期限；本地取消解除等待，迟到回包不得再写结果。 */
async function boundedRequest(rpc: BrokerRpc, method: string, params: unknown, deadline: number, signal?: AbortSignal): Promise<unknown> {
  if (signal?.aborted) throw new ReadFailure("结果补读已取消");
  const remaining = deadline - performance.now();
  if (remaining <= 0) throw new ReadFailure("结果补读期限已到");
  let timer: ReturnType<typeof setTimeout> | undefined;
  let abort: (() => void) | undefined;
  try {
    const boundary = new Promise<never>((_, reject) => {
      const fail = (reason: string) => { reject(new ReadFailure(reason)); };
      abort = () => fail("结果补读已取消");
      signal?.addEventListener("abort", abort, { once: true });
      // 计时器可能比 performance.now 算出的时刻早几毫秒触发（Windows 实测最多约 15 毫秒）。
      // 总期限要等真正到点才算到期：既不提前放弃，排队的子线程也不会看到「还剩几毫秒」又发一轮注定超时的请求。
      const expire = () => {
        const left = deadline - performance.now();
        if (remaining <= 2000 && left > 0) { timer = setTimeout(expire, Math.ceil(left)); return; }
        fail(remaining <= 2000 ? "结果补读期限已到" : "结果读取请求超时");
      };
      timer = setTimeout(expire, Math.min(2000, remaining));
    });
    const result = await Promise.race([rpc.request(method, params, { timeoutMs: Math.max(1, Math.ceil(Math.min(2000, remaining))) }), boundary]);
    if (signal?.aborted || performance.now() >= deadline) throw new ReadFailure("结果补读期限已到或已取消");
    return result;
  } finally {
    clearTimeout(timer);
    if (abort) signal?.removeEventListener("abort", abort);
  }
}

function object(value: unknown): value is Record<string, any> { return value !== null && typeof value === "object" && !Array.isArray(value); }
function page(value: unknown, threadId: string, seen: Set<string>): { data: unknown[]; nextCursor: string | null } {
  if (!object(value) || !Array.isArray(value.data) || !(value.nextCursor === null || typeof value.nextCursor === "string")
    || (value.threadId !== undefined && value.threadId !== threadId)) throw new ReadFailure("服务器分页回包损坏或线程归属不符");
  const next = value.nextCursor;
  if (next !== null) {
    if (!next || !value.data.length || seen.has(next)) throw new ReadFailure("服务器分页游标没有前进");
    seen.add(next);
  }
  return { data: value.data, nextCursor: next };
}

async function readOne(rpc: BrokerRpc, node: TreeNode, deadline: number, signal?: AbortSignal): Promise<TreeNodeResult> {
  const bound = new Set(node.turnIds);
  if (!bound.size) return unavailable("没有可定位的受管轮次");
  let turn: Turn | undefined;
  let pages = 0;
  const request = async (method: string, params: unknown) => {
    if (pages >= 64) throw new ReadFailure("每线程分页上限为 64 页，读取未完成");
    pages++;
    return boundedRequest(rpc, method, params, deadline, signal);
  };
  try {
    let cursor: string | null = null;
    const turnCursors = new Set<string>();
    do {
      const current = page(await request("thread/turns/list", { threadId: node.threadId, cursor, limit: 64,
        sortDirection: "desc", itemsView: "notLoaded" }), node.threadId, turnCursors);
      // 必须校验整页，不能在首个匹配项处提前忽略同页损坏的数据。
      for (const value of current.data) {
        if (!object(value) || typeof value.id !== "string" || !value.id || !TURN_STATUSES.has(value.status)
          || (value.threadId !== undefined && value.threadId !== node.threadId)) throw new ReadFailure("服务器轮次分页回包损坏或线程归属不符");
      }
      turn = (current.data as Turn[]).find((value) => bound.has(value.id));
      cursor = current.nextCursor;
    } while (!turn && cursor !== null);
    if (!turn) return unavailable("服务器分页中未找到本次运行的绑定轮次");

    const items: Item[] = [];
    const itemCursors = new Set<string>();
    cursor = null;
    do {
      const current = page(await request("thread/items/list", { threadId: node.threadId, turnId: turn.id,
        cursor, limit: 64, sortDirection: "asc" }), node.threadId, itemCursors);
      for (const value of current.data) {
        if (!object(value) || value.turnId !== turn.id || (value.threadId !== undefined && value.threadId !== node.threadId)
          || !object(value.item) || typeof value.item.type !== "string" || !value.item.type
          || ["text", "phase", "review"].some((key) => value.item[key] !== undefined && value.item[key] !== null && typeof value.item[key] !== "string")) {
          throw new ReadFailure("服务器条目分页回包损坏或轮次归属不符");
        }
        items.push(value.item as Item);
      }
      cursor = current.nextCursor;
    } while (cursor !== null);
    const extracted = serverOutput(items);
    const output = clip(extracted.output, THREAD_BYTES);
    const truncated = output !== extracted.output;
    return { turnId: turn.id, status: turn.status, source: "server", availability: truncated ? "truncated" : "available",
      output, outputSource: extracted.outputSource, truncated, reason: truncated ? "每线程正文上限为 8 KiB" : null };
  } catch (error) {
    // 不展示上游异常原文，避免凭据或任意服务端文本进入结果。
    return unavailable(error instanceof ReadFailure ? error.message : "服务器结果读取失败或不支持分页", turn);
  }
}

/** 只读本运行绑定轮次；子结果失败不能影响父运行已经选定的出口。 */
export async function readThreadResults(rpc: BrokerRpc, nodes: readonly TreeNode[], options: { deadline: number; signal?: AbortSignal }): Promise<Map<string, TreeNodeResult>> {
  const deadline = Math.min(options.deadline, performance.now() + 5000);
  const children = [...new Map(nodes.filter((node) => node.relationKind === "subagent").map((node) => [node.threadId, node])).values()];
  const results = new Map<string, TreeNodeResult>();
  const selected = children.slice(0, 32);
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(4, selected.length) }, async () => {
    while (next < selected.length) {
      const node = selected[next++]!;
      results.set(node.threadId, await readOne(rpc, node, deadline, options.signal));
    }
  }));
  let remainingBytes = TOTAL_BYTES;
  // 总正文配额按成员顺序分配，不受异步回包先后影响。
  for (const node of children) {
    const result = results.get(node.threadId);
    if (!result) { results.set(node.threadId, unavailable("本次最多补读 32 个子线程", undefined, "omitted")); continue; }
    if (result.availability !== "available" && result.availability !== "truncated") continue;
    if (result.output && remainingBytes === 0) {
      results.set(node.threadId, { ...result, availability: "omitted", output: "", truncated: false, reason: "总正文上限为 64 KiB" });
      continue;
    }
    const output = clip(result.output, remainingBytes);
    remainingBytes -= Buffer.byteLength(output, "utf8");
    if (output !== result.output) results.set(node.threadId, { ...result, output, availability: "truncated", truncated: true, reason: "总正文上限为 64 KiB" });
  }
  return results;
}
