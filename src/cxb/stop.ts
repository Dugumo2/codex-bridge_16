import { EXIT } from "./contracts";
import { safeLine } from "./inbox";
import { commandSession } from "./command-session";
import type { CommandHandler } from "./main";
import { stopThread } from "./stop-thread";
import { backgroundLines, stopReportLines } from "./subagents";
import { resolveThreadViaBroker } from "./thread-ref";

export const stop: CommandHandler = async (ctx) => {
  const scope = commandSession(ctx);
  try {
    const rpc = await scope.connect();
    const ref = await resolveThreadViaBroker(rpc, ctx.parsed.positional[0]!, { passThrough: true });
    const runId = ctx.parsed.options.run;
    const result = await stopThread(rpc, { threadId: ref.threadId, clear: ctx.parsed.options.clear, abandon: ctx.parsed.options.abandon, ...(runId ? { runId } : {}) },
      { signal: scope.signal });
    const ok = result.stopOutcome === "confirmed";
    const label = safeLine(ref.shortId ?? ref.threadId);
    const tree = result.childrenStop;
    if (tree) {
      // 整树停止：父线程与子代理分别报告，冲突与能力限制另列；未确认时不说「已停止」。
      const [summary, ...details] = stopReportLines(tree);
      if (ok) ctx.out.result(`${summary}。`);
      else ctx.out.notice(`停止尚未确认：${summary}；请先查看下列诊断`);
      for (const line of details) ctx.out.notice(line);
    } else if (ok) ctx.out.result(result.alreadyStopped ? `线程 ${label} 没有运行中的运行。` : `已停止线程 ${label}。`);
    else ctx.out.notice(`已发出停止，Codex 尚未确认停下；请先查看清理报告，临时故障可等待后台核实`);
    if (result.backgroundCleanup) for (const line of backgroundLines(label, result.backgroundCleanup)) ctx.out.notice(line);
    if (ctx.parsed.options.abandon) ctx.out.notice(result.actionApplied ? "已放弃本次树清理责任；可能仍有子代理或命令在运行。请换新线程，必要时换隔离工作目录。"
      : result.reason === "alreadyAbandoned" ? "本次树清理此前已放弃，未重新启动清理。"
      : result.reason === "alreadyConfirmed" ? "该运行的树停止已经确认，无需放弃清理。" : "不存在该运行的活动树停止，未应用放弃操作。");
    ctx.out.resultJson({ ok, command: "stop", threadId: ref.threadId, shortId: ref.shortId, runId: result.runId,
      interruptSent: result.interruptSent, stopOutcome: result.stopOutcome, goal: result.goal, alreadyStopped: result.alreadyStopped,
      ...(result.backgroundCleanup ? { backgroundCleanup: result.backgroundCleanup } : {}),
      ...(result.actionApplied !== undefined ? { actionApplied: result.actionApplied, reason: result.reason } : {}),
      ...(tree ? { childrenStop: tree } : {}) });
    return ok ? EXIT.OK : EXIT.TIMEOUT;
  } finally { scope.dispose(); }
};
