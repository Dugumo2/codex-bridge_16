import type { RunResult } from "./contracts";

/** 已启用子代理协调的运行已结束（或失联）却没有最终指针：没有可交付的最终结果。 */
function finalMissing(run: RunResult): boolean {
  return run.finalTurnId === undefined && !!run.subagentJoin && (run.state === "ended" || run.state === "lost");
}
function finalUnavailable(run: RunResult): boolean {
  return (run.finalTurnId !== undefined
    && (run.state === "ended" || run.state === "lost") && !run.turns.some(turn => turn.turnId === run.finalTurnId));
}

/**
 * 与 broker `selected_parent_result` 一致：最终指针优先，存在指针但结果缺失时绝不能借旧轮冒充最终答复；
 * 启用协调的记录没有指针时，运行中取已确认当前父轮，已结束或失联则不可用；只有旧记录（无协调字段）回退尾项。
 */
export function resultTurn(run: RunResult) {
  if (run.finalTurnId !== undefined) return run.turns.find((turn) => turn.turnId === run.finalTurnId);
  if (run.subagentJoin) {
    if (finalMissing(run) || run.currentTurnId == null) return undefined;
    return run.turns.find((turn) => turn.turnId === run.currentTurnId);
  }
  return run.turns.at(-1);
}

export function resultPending(run: RunResult): boolean {
  const turn = resultTurn(run);
  return turn?.ready === "pending" || (run.finalTurnId !== undefined && !turn && !finalUnavailable(run));
}

/** 选定结果的就绪状态：缺最终指针的已结束协调运行明确为 unavailable，不让 wait 无限等待。 */
export function resultReady(run: RunResult) {
  return resultTurn(run)?.ready ?? (finalMissing(run) || finalUnavailable(run) ? "unavailable" : "pending");
}

/** 新运行输出的就绪状态与来源；旧 CLI JSON 不增添推造字段。 */
export function selectedResultFields(run: RunResult) {
  if (run.finalTurnId === undefined && !run.subagentJoin) return {};
  const turn = resultTurn(run);
  return { ready: resultReady(run), ...(turn?.origin !== undefined ? { origin: turn.origin } : {}) };
}

/** 模型改道跟随已确认当前父轮；历史新记录跟随最终轮，旧记录保留原回退。 */
export function modelTurn(run: RunResult) {
  if (!run.subagentJoin && run.finalTurnId === undefined) return run.turns.at(-1);
  const id = run.finalTurnId ?? run.currentTurnId;
  return id != null ? run.turns.find((turn) => turn.turnId === id) : resultTurn(run);
}

/**
 * JSON 增量字段原样透传；缺省不补默认值，不伪造已启用事实。broker 为每次运行保存的收敛与执行配置
 * 只在已启用子代理协调（有 subagentJoin）时输出，未启用运行的 JSON 与 s4-cli 一致。
 */
export function convergenceFields(run: Partial<RunResult>) {
  const active = run.subagentJoin !== undefined;
  return {
    ...(run.finalTurnId !== undefined ? { finalTurnId: run.finalTurnId } : {}),
    ...(active ? { subagentJoin: run.subagentJoin } : {}),
    ...(run.runBudget !== undefined ? { runBudget: run.runBudget } : {}),
    ...(run.autoTurnsStarted !== undefined ? { autoTurnsStarted: run.autoTurnsStarted } : {}),
    ...(run.convergenceDiagnostic !== undefined ? {
      convergenceDiagnostic: run.state === "ended" || run.state === "lost" ? null : run.convergenceDiagnostic,
    } : {}),
    ...(active && run.convergenceConfig !== undefined ? { convergenceConfig: run.convergenceConfig } : {}),
    ...(active && run.executionConfig !== undefined ? { executionConfig: run.executionConfig } : {}),
  };
}
