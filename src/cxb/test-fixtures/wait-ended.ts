import type { BrokerConnection, RunResult, TurnResult } from "../contracts";
import { watchdog } from "./test-watchdog";

/** 独立的端到端观察器：先订阅，再读快照，同时接收晚到的结果补齐。 */
export async function waitEnded(conn: BrokerConnection, threadId: string, runId?: string,
  onPending?: () => Promise<unknown>): Promise<RunResult> {
  let current: RunResult | undefined;
  let expected = runId;
  let released = false;
  const results = new Map<string, { turn: number; result: TurnResult }>();
  const off: Array<() => void> = [];
  let resolve!: (run: RunResult) => void;
  let reject!: (error: unknown) => void;
  const done = new Promise<RunResult>((yes, no) => { resolve = yes; reject = no; });
  const check = () => {
    if (!current || !["ended", "lost"].includes(current.state)) return;
    const last = current.turns.at(-1);
    const extra = results.get(current.runId);
    if (last?.ready === "pending" && extra?.turn === current.turnsTotal - 1 && extra.result.turnId === last.turnId) {
      current = { ...current, turns: [...current.turns.slice(0, -1), extra.result] };
    }
    if (current.turns.at(-1)?.ready !== "pending") resolve(current);
    else if (onPending && !released) {
      released = true;
      void watchdog(onPending(), 5000, "放行结果补读").catch(reject);
    }
  };
  const accept = (run: RunResult | null | undefined) => {
    if (!run || (run.threadId !== threadId && run.reviewThreadId !== threadId) || (expected && run.runId !== expected)) return;
    expected ??= run.runId;
    if (!current || !["ended", "lost"].includes(current.state)) current = run;
    check();
  };
  try {
    off.push(conn.rpc.on("broker/run/ended", (raw) => accept((raw as { run?: RunResult }).run)));
    off.push(conn.rpc.on("broker/thread/result", (raw) => {
      const event = raw as { runId: string; turn: number; result: TurnResult };
      if (expected && event.runId !== expected) return;
      results.set(event.runId, { turn: event.turn, result: event.result });
      check();
    }));
    off.push(conn.rpc.onClose(() => reject(new Error("等待终态时测试连接断开"))));
    void conn.rpc.request<{ run: RunResult | null }>("broker/threads/watch", { threadId, ...(runId ? { runId } : {}) }, { timeoutMs: 5000 })
      .then((snapshot) => accept(snapshot.run), reject);
    return await watchdog(done, 20_000, "运行终态与结果补齐");
  } finally { for (const dispose of off) dispose(); results.clear(); }
}
