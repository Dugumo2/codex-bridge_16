import { CxbError, type BrokerRpc, type RunResult, type RunModelSummary } from "../contracts";
import { NO_RESPONSE } from "../../rpc";
import { watchdog } from "./test-watchdog";
import { resultTurn } from "../run-result";

/** 单元测试用可控传输：显式扣住与放行消息，不用固定延迟制造竞态。 */
export class FakeRpc {
  handlers = new Map<string, Set<(params: any) => void>>();
  closed = new Set<() => void>();
  private anyRequests: Parameters<BrokerRpc["onAnyRequest"]>[0][] = [];
  private requests = new Map<string, Parameters<BrokerRpc["onRequest"]>[1]>();
  private anyNotifications = new Set<Parameters<BrokerRpc["onAny"]>[0]>();
  replies: Array<{ id: string | number; result?: unknown; error?: { code: number; message: string } }> = [];
  inFlightServerRequests = 0;
  /** 监听器异常与真实 RPC 一样逐个隔离；用例收尾必须检查此列表，不能把异常当成功。 */
  notificationErrors: Array<{ method: string; listener: "通配" | "具体"; error: unknown }> = [];
  calls: Array<{ method: string; params: any; options: any }> = [];
  handle: (method: string, params: any) => unknown = () => ({});
  rpc: BrokerRpc = {
    request: async <T>(method: string, params?: unknown, options?: unknown): Promise<T> => {
      this.calls.push({ method, params, options });
      return await this.handle(method, params) as T;
    },
    on: (method, handler) => {
      const set = this.handlers.get(method) ?? new Set();
      this.handlers.set(method, set);
      set.add(handler);
      return () => { set.delete(handler); };
    },
    onClose: (handler) => { this.closed.add(handler); return () => { this.closed.delete(handler); }; },
    onAny: (handler) => { this.anyNotifications.add(handler); return () => { this.anyNotifications.delete(handler); }; },
    onRequest: (method, handler) => {
      this.requests.set(method, handler);
      return () => { if (this.requests.get(method) === handler) this.requests.delete(method); };
    },
    onAnyRequest: (handler) => {
      this.anyRequests.push(handler);
      return () => { this.anyRequests = this.anyRequests.filter((h) => h !== handler); };
    },
    notify() {}, respond: (id, result) => { this.replies.push({ id, result }); },
  };
  emit(method: string, params: unknown) {
    for (const handler of this.anyNotifications) {
      try { handler(method, params); }
      catch (error) { this.notificationErrors.push({ method, listener: "通配", error }); }
    }
    for (const handler of this.handlers.get(method) ?? []) {
      try { handler(params); }
      catch (error) { this.notificationErrors.push({ method, listener: "具体", error }); }
    }
  }
  /** 按真实 RPC 的优先级投递请求，等待处理完成并记录实际回包；NO_RESPONSE 保持静默。 */
  async serverRequest(method: string, params: unknown, id: string | number) {
    const specific = this.requests.get(method);
    const fallback = this.anyRequests.at(-1);
    this.inFlightServerRequests++;
    try {
      if (!specific && !fallback) { this.replies.push({ id, error: { code: -32601, message: "方法不存在" } }); return; }
      const result = await watchdog(Promise.resolve().then(() => specific ? specific(params, id) : fallback!(method, params, id)));
      if (result !== NO_RESPONSE) this.rpc.respond(id, result);
    } finally { this.inFlightServerRequests--; }
  }
  disconnect() { for (const handler of this.closed) handler(); }
  get subscriptions() { return [...this.handlers.values()].reduce((n, set) => n + set.size,
    this.closed.size + this.anyRequests.length + this.requests.size + this.anyNotifications.size); }
}

export function runResult(patch: Partial<RunResult> = {}): RunResult {
  return { runId: "run-test", threadId: "thread-test", shortId: "abcd1234", joined: false, state: "ended", status: "completed",
    startedAt: 1000, endedAt: 2000, meta: null, reviewThreadId: null, input: "测试要求", continuedFrom: null,
    interruptReason: null, stopOutcome: null, progress: { done: [], doneTotal: 0, unconfirmed: [], observedFiles: [] },
    turns: [{ turnId: "turn-test", ready: "ready", status: "completed", output: "中文结果", outputSource: "finalAnswer", error: null, completedAt: 2000 }],
    turnsTotal: 1, ...patch };
}

export function wireError(code: number, message: string) {
  return new CxbError("BROKER_UNAVAILABLE", "远端请求失败", { cause: { rpcCode: code, detail: message } });
}

/**
 * 严格按公共摘要接口构造回包，避免测试意外依赖完整记录的正文或 turns。与 broker `model_summary` 一致：
 * 带最终指针，结果轮按 `selected_parent_result`（即 CLI 的 resultTurn）选择，不取数组尾项。
 */
export function modelSummary(run: RunResult): RunModelSummary {
  const last = resultTurn(run);
  return { ...(run.finalTurnId !== undefined ? { finalTurnId: run.finalTurnId } : {}), runId: run.runId, threadId: run.threadId, reviewThreadId: run.reviewThreadId, state: run.state,
    startedAt: run.startedAt, creationOrder: run.creationOrder ?? null,
    startConfirmation: run.startConfirmation ?? { confirmed: false, owned: null, threadId: null, turnId: null },
    meta: run.meta ? { source: run.meta.source, model: run.meta.model ?? null, effort: run.meta.effort ?? null } : null,
    currentTurnId: run.currentTurnId ?? null, lastTurn: last ? { turnId: last.turnId, reroute: last.reroute ?? null } : null,
    pendingReroute: run.pendingReroute ?? null, ...(run.upstreamError ? { upstreamError: run.upstreamError } : {}) };
}
