// 真 broker 的确定性交错入口：只扣住交接，直到同一运行已经结束。
// 不伪造 RPC 回包，不更改生产入口；其余命令仍使用主入口的处理器。
import { HANDLERS, runCli } from "../main";
import { createRunHandler } from "../run";
import { attachServerRequests, handover, renderPending } from "../server-requests";
import type { RunResult } from "../contracts";

await runCli(process.argv.slice(2), {
  ...HANDLERS,
  run: createRunHandler({
    attachServerRequests, renderPending,
    async handover(rpc, args, opts) {
      const deadline = Date.now() + 10000;
      for (;;) {
        const snapshot = await rpc.request<{ run: RunResult | null }>("broker/threads/watch",
          { threadId: args.threadId, runId: args.runId }, { timeoutMs: 2000 });
        if (snapshot.run?.runId === args.runId && snapshot.run.state === "ended") break;
        if (Date.now() >= deadline) throw new Error("确定性交错未等到运行结束");
        await Bun.sleep(10);
      }
      const result = await handover(rpc, args, opts);
      if (result.outcome !== "runEnded") throw new Error(`确定性交错失效：${result.outcome}`);
      process.stderr.write("N4B 确定性交错：交接前已结束，真实 broker 返回 runEnded\n");
      return result;
    },
  }),
});
