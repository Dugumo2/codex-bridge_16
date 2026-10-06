import { CxbError, type BrokerRpc, type RunResult } from "../contracts";

/** 单元测试用可控传输：显式扣住与放行消息，不用固定延迟制造竞态。 */
export class FakeRpc {
  handlers = new Map<string, Set<(params: any) => void>>();
  closed = new Set<() => void>();
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
    onAny: () => () => {}, onRequest: () => () => {}, onAnyRequest: () => () => {}, notify() {}, respond() {},
  };
  emit(method: string, params: unknown) { for (const handler of this.handlers.get(method) ?? []) handler(params); }
  disconnect() { for (const handler of this.closed) handler(); }
  get subscriptions() { return [...this.handlers.values()].reduce((n, set) => n + set.size, this.closed.size); }
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
