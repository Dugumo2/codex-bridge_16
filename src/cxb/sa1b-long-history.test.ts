/**
 * 子代理历史超过 broker 单条消息上限（10 MiB）时，收敛不能把 broker 撑退出。
 * 历史分散在多轮、不发通知，所以只有全量读才会碰到整段历史。
 * 这里不调用夹具的 state：它的回包走同一条通道，历史一大自己就会超限，造成假象。
 */
import { expect, test } from "bun:test";
import { Sa1bHarness, SA1B_ROOT as P } from "../test-support/sa1b-harness";

const TURN_KB = 100;

async function converge(historyMb: number) {
  const h = new Sa1bHarness();
  try {
    await h.open({ pageSize: 16 });
    await h.start({ cxbConvergence: { timeoutMs: 30_000 } });
    await h.spawn("C0");
    const body = "x".repeat(TURN_KB * 1024);
    await h.finish("C0", `第 0 轮 ${body}`);
    const turns = Math.ceil((historyMb * 1024) / TURN_KB);
    for (let n = 1; n < turns; n++) {
      const started = await h.control("turn", { threadId: "C0", action: "start", announce: false });
      await h.control("turn", { threadId: "C0", turnId: started.turnId, action: "complete", output: `第 ${n} 轮 ${body}`, status: "completed", announce: false });
    }
    await h.finish(P);
    const broker = h.spawnedProcesses[0]!;
    await h.until(async () => {
      expect(broker.exitCode, "broker 不能退出").toBeNull();
      return ((await h.record())?.subagentJoin?.wakeupsStarted ?? 0) >= 1;
    }, 25_000, "汇总没有发出");
    await h.control("turn", { threadId: P, action: "complete", output: "已核实", status: "completed" });
    await h.until(async () => h.ended().length > 0, 10_000, "运行没有结束");
    const run = h.ended().at(-1);
    expect(run.status).toBe("completed");
    expect(run.subagentJoin.terminalReason ?? null).toBeNull();
    expect(broker.exitCode, "broker 不能退出").toBeNull();
  } finally {
    await h.close();
  }
}

test("子代理历史超过单条消息上限时收敛正常完成", () => converge(11), 60_000);
test("子代理历史远超单条消息上限时收敛正常完成", () => converge(40), 60_000);
test("子代理历史低于单条消息上限时不受影响", () => converge(9), 60_000);
