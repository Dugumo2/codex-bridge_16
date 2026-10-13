/** 二审 R 路：真实 broker 验证跟进活动乱序、并入已有轮与最终汇总释放。 */
import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { Sa1bHarness, SA1B_ROOT as P } from "../test-support/sa1b-harness";

const config = { timeoutMs: 20000, discoveryTimeoutMs: 400, wakeVerifyTimeoutMs: 600 };

test("二审 R1 未知接收方不入成员表，也不阻塞本运行结束", async () => {
  const h = new Sa1bHarness();
  try {
    await h.open();
    await h.start({ cxbConvergence: config });
    await h.control("reuse", { parentThreadId: P, parentTurnId: h.firstTurn, childThreadId: "unknown", open: false });
    await h.observe();
    expect((await h.record()).children ?? []).toHaveLength(0);
    await h.finish(P);
    await h.until(async () => h.ended().length > 0 || (await h.starts()).some(r => r.params.clientUserMessageId), 6000, "被拒绝候选仍阻塞结束");
    if (!h.ended().length) {
      const turn = (await h.state()).threads.root.turns.at(-1);
      await h.finish(P, "无需等待未知接收方", "completed", turn.id);
    }
    await h.until(async () => h.ended().length > 0, 8000, "父运行未结束");
    expect(h.ended().at(-1).status).toBe("completed");
    expect(h.ended().at(-1).children ?? []).toHaveLength(0);
  } finally { await h.close(); }
}, 25000);

async function finishSummary(h: Sa1bHarness, skip: number, marker: string) {
  let request: any;
  await h.until(async () => {
    request = (await h.starts()).slice(skip).find(x => x.params.clientUserMessageId);
    return !!request || h.ended().length > 0;
  }, 6000, "跟进活动结束后 pending 未清除，未发出汇总");
  expect(request).toBeDefined();
  const text = request.params.input.filter((i: any) => i.type === "text").map((i: any) => i.text).join("\n");
  expect(text).toContain(marker);
  const turn = (await h.state()).threads.root.turns.at(-1);
  expect(turn.items.some((i: any) => i.type === "userMessage" && i.clientId === request.params.clientUserMessageId)).toBe(true);
  await h.finish(P, "已汇总当前子活动", "completed", turn.id);
  await h.until(async () => h.ended().length > 0, 8000, "汇总后运行未结束");
  const record = h.ended().at(-1);
  expect(record.status).toBe("completed");
  expect(record.runId).toBe(h.runId);
  expect(record.finalTurnId).toBe(turn.id);
  expect(record.subagentJoin.reconciledEpoch).toBe(record.subagentJoin.workEpoch);
}

for (const mode of ["新轮先到interacted后到", "sendInput并入旧线程活动轮", "sendInput并入本运行已绑定轮"] as const) {
  test(`二审 R2 ${mode}：绑定成功、汇总后结束`, async () => {
    const h = new Sa1bHarness();
    try {
      await h.open();
      await h.start({ cxbConvergence: config });
      const version = mode === "新轮先到interacted后到" ? "v2" : "v1";
      let childTurn = await h.spawn("C", P, h.firstTurn, { version });
      if (mode !== "sendInput并入本运行已绑定轮") {
        await h.finish("C", "历史输出");
        await h.finish(P);
        await finishSummary(h, 1, "历史输出");
        const response: any = await h.owner.request("turn/start", {
          threadId: P, input: [{ type: "text", text: "跟进旧子线程" }], model: "original-model", effort: "high", cxbConvergence: config,
        });
        h.runId = response.runId;
        await h.until(async () => {
          h.firstTurn = (await h.state()).threads.root.turns.at(-1).id;
          return (await h.watch()).thread.state === "running";
        }, 8000, "第二次运行未开始");
        if (mode === "sendInput并入旧线程活动轮") {
          childTurn = (await h.control("turn", { threadId: "C", action: "start" })).turnId;
          await h.observe();
        }
      }
      const count = (await h.state()).threads.C.turns.length;
      const reused = await h.control("reuse", {
        parentThreadId: P, parentTurnId: h.firstTurn, childThreadId: "C",
        order: "turnFirst", open: mode === "新轮先到interacted后到",
      });
      if (mode !== "新轮先到interacted后到") {
        expect(reused.turnId).toBe(childTurn);
        expect((await h.state()).threads.C.turns).toHaveLength(count);
      }
      await h.until(async () => (await h.record()).children.some((c: any) => c.threadId === "C" && c.turnIds.includes(reused.turnId)),
        3000, "实际承载轮未绑定到本次运行");
      const before = (await h.starts()).length;
      const marker = `本轮结果_${randomUUID()}`;
      await h.finish("C", marker, "completed", reused.turnId);
      await h.finish(P);
      await finishSummary(h, before, marker);
    } finally {
      await h.close();
    }
  }, 35000);
}
