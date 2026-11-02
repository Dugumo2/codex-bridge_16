import type { BrokerRpc, RunResult } from "./contracts";
import type { ConvergenceSummary, JoinPhase, TerminalReason } from "./sa1b-contracts";
import { convergenceFields } from "./run-result";

const phases: Record<JoinPhase, string> = { running: "运行中", waitingSubtree: "等待子代理", preparingReconcile: "准备汇总",
  reconciling: "汇总中", finalCheck: "最终核实", released: "已释放", stopping: "异常收尾", abnormal: "异常收尾" };
const gates: Record<string, string> = {
  automaticControlRevoked: "自动汇总权限核实", connectionOrAuthorityChanged: "连接与管理权核实", parentNotSettled: "父轮落定",
  conflicts: "归属冲突核实", reconcileOutcomeUnknown: "汇总提交结果核实", rootUnsettled: "根线程落定",
  requestInFlight: "请求处理", stopChangeInFlight: "停止变更处理", discoveryInFlight: "子树发现回包",
  resultReadInFlight: "结果读取回包", retryScheduled: "重试调度", reconcileSubmissionInFlight: "汇总提交回包",
  discoveryEpochUncovered: "发现覆盖当前工作批次", discoveryIncomplete: "子树发现核实", pendingOperations: "派生操作落定",
  pendingRequests: "待答事项", unresolvedLookups: "关系查询落定", unresolvedCandidates: "候选归属核实",
  activityUnverified: "活动核实", activityUnsettled: "活动落定", activityResultUnknown: "活动结果核实",
  activityGoalActive: "活动目标结束", activityGoalUnknown: "活动目标状态核实", activityPendingRequests: "活动待答事项",
  rootGoalTurnInFlight: "根目标轮落定", readyToRelease: "最终释放", awaitingReconcile: "已投递汇总完成",
  rootCapabilityUnknown: "根线程能力核实", rootNoDirectInput: "根线程输入能力核实", budgetExhausted: "预算收尾",
  budgetVerification: "预算核实", budgetUnavailable: "预算证据核实", preparingReconcile: "汇总准备",
  goalTerminal: "目标终态收尾", budgetVerificationExpired: "预算核实期限收尾", convergenceDeadline: "收敛期限收尾",
  wakeLimit: "汇总次数收尾", autoTurnLimit: "自动父轮次数收尾",
};
const reasons: Record<TerminalReason, string> = {
  parentFailed: "父轮失败", parentInterrupted: "父轮中断", userStop: "用户停止", ownerGone: "客户端断开",
  budgetExhausted: "原预算耗尽", budgetIncomplete: "预算证据不完整", convergeDeadline: "收敛期限已到",
  wakeLimit: "汇总次数用尽", autoTurnLimit: "自动父轮次数用尽", noAction: "连续无动作",
  discoveryIncomplete: "子树发现不完整", ownershipConflict: "轮次归属冲突", wakeUnknown: "汇总启动结果不明",
  wakeFailed: "汇总启动失败", goalPaused: "目标已暂停", goalBlocked: "目标受阻", goalBudgetLimited: "目标预算受限",
  goalUsageLimited: "目标用量受限", rootNoDirectInput: "根线程不能直接输入", abandoned: "已放弃清理责任", brokerShutdown: "broker 关闭或失联",
  stallTimeout: "无进展超时", upstreamAccessError: "上游访问报错即停",
};

/** 协调状态是运行的附加说明，不另造生命周期或停止成功结论。 */
export function convergenceLines(run: ConvergenceSummary & Pick<Partial<RunResult>, "convergenceConfig" | "children">, pending = 0): string[] {
  const lines: string[] = [];
  // broker 只在本运行确有子代理派生／跟进时按需启用协调并写 subagentJoin；没有时普通运行输出保持原样（用户 2026-10-11 定）。
  const join = run.subagentJoin;
  if (!join) return lines;
  lines.push(`运行协调：${phases[join.phase]}${pending > 0 ? `；等待回答（${pending} 项）` : ""}；唤醒 ${join.wakeupsStarted}/${run.convergenceConfig?.maxWakeups ?? 5} 次，提交 ${join.wakeupAttempts} 次，重试 ${join.startRetriesUsed} 次`);
  if (join.phase === "waitingSubtree" && run.convergenceDiagnostic) {
    lines.push(`正在等待：${gates[run.convergenceDiagnostic.gate] ?? "收敛条件核实"}`);
  }
  lines.push(`收敛期限：${join.deadlineAt === 0 ? "尚未开始收敛计时" : new Date(join.deadlineAt).toISOString()}；工作批次 ${join.workEpoch}，已投递 ${join.deliveredEpoch}，已汇总 ${join.reconciledEpoch}`);
  if (join.resultCoverage) {
    const c = join.resultCoverage;
    lines.push(`结果覆盖：批次 ${c.epoch}，共 ${c.total} 项，可用 ${c.available}，截断 ${c.truncated}，不可用 ${c.unavailable}，省略 ${c.omitted}`);
  }
  if (join.terminalReason) lines.push(`终态原因：${reasons[join.terminalReason]}（${join.terminalReason}）`);
  if (join.automaticControlRevoked) lines.push("自动汇总权限已撤销");
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
  type Fields = ConvergenceSummary & Pick<Partial<RunResult>, "convergenceConfig" | "executionConfig">;
  const views = new Map<string, { revision: number; fields: Fields }>();
  let printed = "";
  let active = true;
  const display = () => {
    const view = views.get(runId() ?? "");
    if (!view) return;
    const text = convergenceLines(view.fields).join("\n");
    if (text && text !== printed) { printed = text; for (const line of text.split("\n")) notice(line); }
  };
  const accept = (view: { runId?: string; revision?: number } & Fields) => {
    if (!active || !view.runId || !Number.isSafeInteger(view.revision)) return;
    const previous = views.get(view.runId);
    if (!previous || view.revision! > previous.revision) {
      // childChanged 可不带协调摘要；缺省字段不能抹掉已知的同运行事实。
      views.set(view.runId, { revision: view.revision!, fields: { ...previous?.fields, ...convergenceFields(view),
        convergenceConfig: previous?.fields.convergenceConfig ?? view.convergenceConfig,
        executionConfig: previous?.fields.executionConfig ?? view.executionConfig } });
      if (views.size > 16) views.delete(views.keys().next().value!);
    } else {
      // 配置在同一运行内不可变；迟到／同 revision 快照可补齐配置，但不能倒退动态摘要。
      previous.fields.convergenceConfig ??= view.convergenceConfig;
      previous.fields.executionConfig ??= view.executionConfig;
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
  return { accept, rpc: watchedRpc, fields: () => convergenceFields(views.get(runId() ?? "")?.fields ?? {}), dispose: () => { active = false; for (const off of offs) off(); views.clear(); } };
}
