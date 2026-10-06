import { EXIT } from "./contracts";
import { safeLine } from "./inbox";
import { commandSession } from "./command-session";
import type { CommandHandler } from "./main";
import { stopThread } from "./stop-thread";
import { resolveThreadViaBroker } from "./thread-ref";

export const stop: CommandHandler = async (ctx) => {
  const scope = commandSession(ctx);
  try {
    const rpc = await scope.connect();
    const ref = await resolveThreadViaBroker(rpc, ctx.parsed.positional[0]!, { passThrough: true });
    const result = await stopThread(rpc, { threadId: ref.threadId, clear: ctx.parsed.options.clear }, { signal: scope.signal });
    const ok = result.stopOutcome === "confirmed";
    const label = safeLine(ref.shortId ?? ref.threadId);
    if (ok) ctx.out.result(result.alreadyStopped ? `线程 ${label} 没有运行中的运行。` : `已停止线程 ${label}。`);
    else ctx.out.notice(`已发出停止，Codex 尚未确认停下，状态为停止中；可再次 cxb stop ${label} 重试`);
    ctx.out.resultJson({ ok, command: "stop", threadId: ref.threadId, shortId: ref.shortId, runId: result.runId,
      interruptSent: result.interruptSent, stopOutcome: result.stopOutcome, goal: result.goal, alreadyStopped: result.alreadyStopped });
    return ok ? EXIT.OK : EXIT.TIMEOUT;
  } finally { scope.dispose(); }
};
