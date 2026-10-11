import type { BrokerRpc, RunResult } from "./contracts";
import type { ConvergenceSummary, JoinPhase, TerminalReason } from "./sa1b-contracts";
import { convergenceFields } from "./run-result";

const phases: Record<JoinPhase, string> = { running: "运行中", waitingSubtree: "等待子代理", preparingReconcile: "准备汇总",
  reconciling: "汇总中", finalCheck: "最终核实", released: "已释放", stopping: "异常收尾", abnormal: "异常收尾" };
const reasons: Record<TerminalReason, string> = {
  parentFailed: "父轮失败", parentInterrupted: "父轮中断", userStop: "用户停止", ownerGone: "客户端断开",
  budgetExhausted: "原预算耗尽", budgetIncomplete: "预算证据不完整", convergeDeadline: "收敛期限已到",
  wakeLimit: "汇总次数用尽", autoTurnLimit: "自动父轮次数用尽", noAction: "连续无动作",
  discoveryIncomplete: "子树发现不完整", ownershipConflict: "轮次归属冲突", wakeUnknown: "汇总启动结果不明",
  wakeFailed: "汇总启动失败", goalPaused: "目标已暂停", goalBlocked: "目标受阻", goalBudgetLimited: "目标预算受限",
  goalUsageLimited: "目标用量受限", rootNoDirectInput: "根线程不能直接输入", abandoned: "已放弃清理责任", brokerShutdown: "broker 关闭或失联",
};

/** 协调状态是运行的附加说明，不另造生命周期或停止成功结论。 */
export function convergenceLines(run: ConvergenceSummary & Pick<Partial<RunResult>, "convergenceConfig">, pending = 0): string[] {
  const lines: string[] = [];
  const join = run.subagentJoin;
  if (join) {
    lines.push(`运行协调：${phases[join.phase]}${pending > 0 ? `；等待回答（${pending} 项）` : ""}；唤醒 ${join.wakeupsStarted}/${run.convergenceConfig?.maxWakeups ?? 5} 次，提交 ${join.wakeupAttempts} 次，重试 ${join.startRetriesUsed} 次`);
    lines.push(`收敛期限：${new Date(join.deadlineAt).toISOString()}；工作批次 ${join.workEpoch}，已投递 ${join.deliveredEpoch}，已汇总 ${join.reconciledEpoch}`);
    if (join.resultCoverage) {
      const c = join.resultCoverage;
      lines.push(`结果覆盖：批次 ${c.epoch}，共 ${c.total} 项，可用 ${c.available}，截断 ${c.truncated}，不可用 ${c.unavailable}，省略 ${c.omitted}`);
    }
    if (join.terminalReason) lines.push(`终态原因：${reasons[join.terminalReason]}（${join.terminalReason}）`);
    if (join.automaticControlRevoked) lines.push("自动汇总权限已撤销");
  }
  if (run.autoTurnsStarted !== undefined) lines.push(`自动父轮：${run.autoTurnsStarted}/${run.convergenceConfig?.maxAutoTurns ?? 50}`);
  if (run.runBudget) {
    const b = run.runBudget;
    const coverage = { complete: "完整", incomplete: "不完整", unknown: "未知" };
    lines.push(`预算：原额度 ${b.tokenBudget}，起始已用 ${b.usedAtStart}，本运行新增 ${b.tokensCharged}；证据${coverage[b.coverage]}（${b.coverage}），来源 ${b.source}${b.exhausted ? "；已耗尽" : ""}`);
  }
  return lines;
}

/** 先订阅再取快照；同运行 revision 只前进，防止快照与迟到通知倒退阶段。 */
export function observeConvergence(rpc: BrokerRpc, runId: () => string | null | undefined, notice: (line: string) => void) {
  const views = new Map<string, { revision: number; fields: ConvergenceSummary }>();
  let printed = "";
  let active = true;
  const display = () => {
    const view = views.get(runId() ?? "");
    if (!view) return;
    const text = convergenceLines(view.fields).join("\n");
    if (text && text !== printed) { printed = text; for (const line of text.split("\n")) notice(line); }
  };
  const accept = (view: { runId?: string; revision?: number } & ConvergenceSummary) => {
    if (!active || !view.runId || !Number.isSafeInteger(view.revision)) return;
    const previous = views.get(view.runId);
    if (!previous || view.revision! > previous.revision) {
      // childChanged 可不带协调摘要；缺省字段不能抹掉已知的同运行事实。
      views.set(view.runId, { revision: view.revision!, fields: { ...previous?.fields, ...convergenceFields(view) } });
      if (views.size > 16) views.delete(views.keys().next().value!);
    }
    display();
  };
  const offs = ["broker/run/convergenceChanged", "broker/run/childChanged"].map((method) => rpc.on(method, (raw) => {
    if (raw && typeof raw === "object") accept(raw as Parameters<typeof accept>[0]);
  }));
  const watchedRpc: BrokerRpc = { ...rpc, async request<T>(method: string, params?: unknown, options?: Parameters<BrokerRpc["request"]>[2]): Promise<T> {
    const result = await rpc.request<T>(method, params, options);
    if (method === "broker/threads/watch" && result && typeof result === "object") {
      const view = result as { run?: RunResult; revision?: number } & ConvergenceSummary;
      if (view.run) accept({ ...view.run, ...view, runId: view.run.runId });
    }
    return result;
  } };
  return { accept, rpc: watchedRpc, fields: () => views.get(runId() ?? "")?.fields ?? {}, dispose: () => { active = false; for (const off of offs) off(); views.clear(); } };
}
