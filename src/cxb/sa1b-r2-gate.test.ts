/** G 路二审黑盒：初始输入身份与真实插话分开，启用前插话仍交 SA1a。 */
import { expect, test } from "bun:test";
import { Sa1bHarness, SA1B_ROOT as P } from "../test-support/sa1b-harness";

for (const automatic of [false, true]) {
  test(`G 路：${automatic ? "自动轮" : "首轮"}插话后派生不启用，初始输入标识不算汇总请求`, async () => {
    const h = new Sa1bHarness();
    try {
      await h.open({ pageSize: 16 });
      await h.start({ cxbConvergence: { timeoutMs: 20000, discoveryTimeoutMs: 400, wakeVerifyTimeoutMs: 600 } });
      const initial = await h.starts();
      expect(initial).toHaveLength(1);
      expect(typeof initial[0].params.clientUserMessageId).toBe("string");
      let turn = h.firstTurn;
      if (automatic) {
        await h.control("goal", { threadId: P, status: "active" });
        await h.finish(P);
        turn = (await h.control("turn", { threadId: P, action: "start", turnId: "goal-auto" })).turnId;
      }
      await h.control("item", { threadId: P, turnId: turn, item: {
        type: "userMessage", id: "真实插话", content: [{ type: "text", text: "用户补充任务" }],
      } });
      await h.observe();
      await h.spawn("C", P, turn);
      await h.observe();
      expect((await h.record()).subagentJoin).toBeUndefined();
      if (automatic) await h.control("goal", { threadId: P, status: "complete" });
      await h.finish(P, "父先结束", "completed", turn);
      await h.until(async () => h.ended().length > 0, 8000, "SA1a 未在期限内收尾");
      const run = h.ended().at(-1);
      expect(run.subagentJoin).toBeUndefined();
      expect(run.interruptReason).toBe("subagentsUnfinished");
      // 初始请求本身也有 clientUserMessageId；直接检查没有新增 start/steer，避免误判汇总。
      expect(await h.starts()).toEqual(initial);
    } finally {
      await h.close();
    }
  }, 30000);
}
