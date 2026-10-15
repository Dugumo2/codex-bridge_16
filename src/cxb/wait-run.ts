import { CxbError, EXIT, type ExitCodeForRun, type RunResult, type TurnResult, type WaitRun } from "./contracts";
import { readRunRecord } from "./ledger";
import { resultPending, resultTurn } from "./run-result";
import { createDiagnosticStore, isUnconfirmedRun, type DiagnosticView } from "./run-diagnostics";

/** 即使被等待方不遵守期限，也必须拒绝等待；迟到拒绝由 race 接住。 */
export async function bounded<T>(work: Promise<T>, timeoutMs: number | null, signal?: AbortSignal): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  let abort: (() => void) | undefined;
  try {
    return await Promise.race([work, new Promise<never>((_, reject) => {
      abort = () => reject(new CxbError("INTERRUPTED", "等待已取消"));
      if (signal?.aborted) { abort(); return; }
      signal?.addEventListener("abort", abort, { once: true });
      if (timeoutMs !== null) timer = setTimeout(() => reject(new CxbError("TIMEOUT", "等待运行超时")), Math.max(0, timeoutMs));
    })]);
  } finally {
    clearTimeout(timer);
    if (abort) signal?.removeEventListener("abort", abort);
  }
}

export const exitCodeForRun: ExitCodeForRun = (run) => {
  if (run.interruptReason === "stallTimeout" || run.interruptReason === "upstreamAccessError") {
    if (run.status === "failed") return EXIT.CODEX_FAILED;
    if (run.stopOutcome === "unconfirmed") return EXIT.TIMEOUT;
    if (run.interruptReason === "stallTimeout") return EXIT.TIMEOUT;
    if (run.interruptReason === "upstreamAccessError") return EXIT.INTERRUPTED;
  }
  if (run.subagentJoin) {
    if (run.state === "lost") return EXIT.INTERRUPTED;
    if (run.status === "failed") return EXIT.CODEX_FAILED;
  }
  if (run.loopGuard || run.interruptReason === "goalLoop" || run.status === "goalBlocked") return EXIT.GOAL_BLOCKED_OR_LIMITED;
  if (run.status === "completed") return EXIT.OK;
  if (run.status === "failed") return EXIT.CODEX_FAILED;
  return EXIT.INTERRUPTED;
};

export const waitRun: WaitRun = async (rpc, opts) => {
  if (opts.signal?.aborted) throw new CxbError("INTERRUPTED", "等待已取消");
  let current: RunResult | undefined;
  const diagnostics = createDiagnosticStore();
  const recoveryController = new AbortController();
  let recovering = false;
  // 补读可能早于 ended 或 watch 回包；仅缓存目标运行最近 100 轮，与账本保留量一致。
  const results = new Map<number, TurnResult>();
  const disposers: Array<() => void> = [];
  let finish!: (run: RunResult) => void;
  let fail!: (error: unknown) => void;
  let settled = false;
  const done = new Promise<RunResult>((resolve, reject) => {
    finish = (run) => { if (!settled) { settled = true; resolve(run); } };
    fail = (error) => { if (!settled) { settled = true; reject(error); } };
  });
  const check = () => {
    if (settled) return;
    if (current) current = diagnostics.merge(current) as RunResult;
    // 新原因停止未确认可以早于 ended；此时不等最终轮补读，也不把停止未确认当成执行完成。
    if (current && isUnconfirmedRun(current)) { finish(current); return; }
    if (!current || (current.state !== "ended" && current.state !== "lost")) return;
    const last = resultTurn(current);
    // 新记录按稳定 turnId 接补读；旧记录仍核对零基序号，兼容裁剪后的账本。
    const replacement = current.finalTurnId !== undefined
      ? [...results.values()].find((turn) => turn.turnId === current!.finalTurnId)
      : results.get(current.turnsTotal - 1);
    if ((resultPending(current) || !last) && replacement && replacement.turnId === (current.finalTurnId ?? last?.turnId)) {
      current = { ...current, turns: last ? current.turns.map((turn) => turn.turnId === replacement.turnId ? replacement : turn)
        : [...current.turns, replacement] };
    }
    if (!resultPending(current)) finish(current);
  };
  const recoverUnconfirmed = () => {
    if (settled || recovering) return;
    recovering = true;
    const timeoutMs = Math.max(1, Math.min(5000, opts.timeoutMs ?? 5000));
    void bounded(rpc.request<{ run?: RunResult | null }>("broker/threads/watch", { threadId: opts.threadId, runId: opts.runId },
      { timeoutMs }), timeoutMs, opts.signal ? AbortSignal.any([opts.signal, recoveryController.signal]) : recoveryController.signal).then((snapshot) => {
      if (snapshot.run) accept(snapshot.run);
    }, () => undefined).finally(() => {
      if (settled) return;
      // 补取失败时只用最后完整记录和已接纳的诊断。没有记录时明确构造空结果，绝不把通知强转成运行记录。
      if (!current && opts.workspaceDir) {
        try {
          const disk = readRunRecord(opts.workspaceDir, opts.runId);
          if (disk && (disk.threadId === opts.threadId || disk.reviewThreadId === opts.threadId)) accept(disk);
        } catch { /* 诊断本身仍可交付。 */ }
      }
      if (!current) current = { runId: opts.runId, threadId: opts.threadId, shortId: "", joined: false,
        state: "stopping", status: null, startedAt: 0, endedAt: null, meta: null, reviewThreadId: null,
        input: "", continuedFrom: null, interruptReason: null, stopOutcome: null,
        progress: { done: [], doneTotal: 0, unconfirmed: [], observedFiles: [] }, turns: [], turnsTotal: 0 };
      check();
    });
  };
  const watched = new Set<string>();
  const watch = (threadId: string) => {
    if (watched.has(threadId)) return;
    watched.add(threadId);
    // 请求错误接入同一个等待，避免连接断开时卡在快照 RPC。快照按 runId 取这次运行，不受线程上更新的运行影响。
    void rpc.request<{ run: RunResult | null }>("broker/threads/watch", { threadId, runId: opts.runId },
      { timeoutMs: Math.max(1, Math.min(30_000, opts.timeoutMs ?? 30_000)) }).then((snapshot) => {
      try {
        if (settled) return;
        if (snapshot.run) { accept(snapshot.run); return; }
        // broker 内存里没有这次运行（每个线程只留最近一次、全局最多 MAX_HISTORY 次，或是 broker 重启前的运行），
        // 之后也不会再有它的通知。手上的结果（例如先到的 ended 带着 pending 的最后一轮）不可交付时，以磁盘记录补全。
        const disk = opts.workspaceDir ? readRunRecord(opts.workspaceDir, opts.runId) : null;
        if (disk && disk.threadId !== threadId && disk.reviewThreadId !== threadId) {
          fail(new CxbError("NOT_FOUND", `运行 ${opts.runId} 不属于该线程`, { id: opts.runId }));
          return;
        }
        if (disk) accept(disk);
        if (settled) return;
        if (isUnconfirmedRun(diagnostics.get(opts.runId) ?? {})) { recoverUnconfirmed(); return; }
        if (!disk && !current) { fail(new CxbError("NOT_FOUND", `找不到运行 ${opts.runId}`, { id: opts.runId })); return; }
        // broker 只淘汰已结束且结果就绪的运行，磁盘上仍是运行中或结果未就绪说明写入未落盘或记录未完整加载。
        fail(new CxbError("BROKER_UNAVAILABLE", `运行 ${opts.runId} 当前无法取得可靠的终态，请稍后重试`, { id: opts.runId, retryable: true }));
      } catch (error) { fail(error); }
    }, (error) => {
      if (isUnconfirmedRun(diagnostics.get(opts.runId) ?? {})) recoverUnconfirmed();
      else fail(error);
    });
  };
  const accept = (run: RunResult | null | undefined) => {
    if (settled || run?.runId !== opts.runId) return;
    diagnostics.accept(run, true);
    // 旧快照不能把已经收到的终态降回 running，也不能覆盖已就绪的结果。
    if (!current || (current.state !== "ended" && current.state !== "lost")) current = run;
    else if ((run.state === "ended" || run.state === "lost") && run.turnsTotal >= current.turnsTotal
      && (current.finalTurnId === undefined || run.finalTurnId === current.finalTurnId)
      && !resultPending(run)) current = run;
    check();
  };
  try {
    disposers.push(rpc.on("broker/run/updated", (raw) => {
      if (settled || !raw || typeof raw !== "object") return;
      const view = raw as DiagnosticView;
      if (view.runId !== opts.runId) return;
      diagnostics.accept(view, ["stall", "stopDiagnostics", "childUpstreamErrors", "stopOutcome", "stopCheck"].every((key) => Object.hasOwn(view, key)));
      check();
      if (!current && isUnconfirmedRun(diagnostics.get(opts.runId) ?? {})) recoverUnconfirmed();
    }));
    disposers.push(rpc.on("broker/run/ended", (raw) => accept((raw as { run?: RunResult })?.run)));
    disposers.push(rpc.on("broker/thread/result", (raw) => {
      const event = raw as { runId?: string; turn?: number; result?: TurnResult };
      if (event?.runId !== opts.runId || !Number.isSafeInteger(event.turn) || !event.result) return;
      results.set(event.turn!, event.result);
      if (results.size > 100) results.delete(Math.min(...results.keys()));
      check();
    }));
    disposers.push(rpc.onClose(() => fail(new CxbError("BROKER_UNAVAILABLE", "等待期间 broker 连接断开", { retryable: true }))));
    watch(opts.threadId);
    return await bounded(done, opts.timeoutMs, opts.signal);
  } catch (error) {
    if (error instanceof CxbError) throw error;
    throw new CxbError("BROKER_UNAVAILABLE", "无法读取运行结果", { cause: error, retryable: true });
  } finally {
    settled = true;
    recoveryController.abort();
    for (const dispose of disposers) dispose();
    results.clear();
    diagnostics.clear();
  }
};
