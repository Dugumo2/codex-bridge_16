/**
 * 子线程订阅释放黑盒：Codex app-server 会把子代理线程自动挂到 broker 的连接上（0.160.0 实测），
 * broker 释放根线程时要把已无活动的子线程一并退订，否则子线程连同它的整套 MCP 服务一直不卸载。
 * 走 SA1b 夹具（生产默认启用收敛）：
 * - 子线程先结束、父轮再结束：运行结束前不退订子线程，结束后根与子线程各退订一次；
 * - 用户停止整树：子线程被中断，运行收尾后子线程退订一次；
 * - 父线程不等子代理就结束、后代完成后汇总：汇总期间不退订子线程，运行结束后退订一次。
 * 未启用 SA1b 的整树停止与清理结束再扫由 Rust 单测 `claim::tests::child_unsub` 覆盖。
 * broker 只支持 Windows：其他平台整组跳过（不加载夹具）。
 */
import { describe, expect, test } from "bun:test";

const RUNNABLE = process.platform === "win32";
const sa1b = RUNNABLE ? await import("./test-support/sa1b-harness") : null;
const CHILD = "child-1";

const unsubscribes = (state: any): string[] =>
  state.calls.filter((c: any) => c.method === "thread/unsubscribe").map((c: any) => c.params?.threadId);
const count = (list: string[], thread: string) => list.filter((t) => t === thread).length;

describe.skipIf(!RUNNABLE)("broker-ext 子线程订阅随根释放", () => {
  test("子线程先结束、父轮再结束：运行结束前不退订子线程，结束后根与子线程各退订一次", async () => {
    const s = new sa1b!.Sa1bHarness();
    try {
      const root = sa1b!.SA1B_ROOT;
      await s.open(); await s.start(); await s.spawn(CHILD);
      await s.finish(CHILD, "子代理最终结果"); await s.observe();
      expect(unsubscribes(await s.state())).not.toContain(CHILD);
      await s.finish(root);
      // 子结果尚未交付时收敛会开汇总轮；汇总轮结束后运行才结束。
      if (await s.reconciliation()) await s.finish(root, "已核实子代理最终结果");
      await s.until(async () => s.ended().length === 1, 8_000, "运行结束");
      await s.until(async () => unsubscribes(await s.state()).includes(CHILD), 8_000, "子线程退订");
      await s.observe(300);
      const list = unsubscribes(await s.state());
      expect(count(list, root)).toBe(1);
      expect(count(list, CHILD)).toBe(1);
    } finally {
      await s.close();
    }
  }, 60_000);

  test("用户停止整树：子线程被中断，运行收尾后子线程退订一次", async () => {
    const s = new sa1b!.Sa1bHarness();
    try {
      await s.open(); await s.start(); await s.spawn(CHILD);
      const observer = await s.connect();
      await observer.request("broker/threads/stop", { threadId: sa1b!.SA1B_ROOT }, 45_000);
      await s.until(async () => (await s.state()).threads[CHILD].interrupts.length > 0, 8_000, "子线程被中断");
      await s.until(async () => s.ended().length === 1, 15_000, "运行结束");
      await s.until(async () => unsubscribes(await s.state()).includes(CHILD), 8_000, "子线程退订");
      await s.observe(300);
      expect(count(unsubscribes(await s.state()), CHILD)).toBe(1);
    } finally {
      await s.close();
    }
  }, 60_000);

  test("退订后在根上再开运行：开工前重新订阅子线程，复用子线程的第二次运行正常结束且子线程再次退订", async () => {
    const s = new sa1b!.Sa1bHarness();
    try {
      const root = sa1b!.SA1B_ROOT;
      await s.open(); await s.start(); await s.spawn(CHILD);
      await s.finish(CHILD, "第一次结果"); await s.observe();
      await s.finish(root);
      if (await s.reconciliation()) await s.finish(root, "已核实第一次结果");
      await s.until(async () => s.ended().length === 1, 8_000, "第一次运行结束");
      await s.until(async () => unsubscribes(await s.state()).includes(CHILD), 8_000, "子线程退订");
      const second: any = await s.owner.request("turn/start", { threadId: root, input: [{ type: "text", text: "复用子代理继续" }], model: "original-model", effort: "high" });
      expect(second.runId).not.toBe(s.runId);
      const calls = async () => (await s.state()).calls as any[];
      await s.until(async () => (await calls()).some((c) => c.method === "thread/resume" && c.params?.threadId === CHILD), 8_000, "子线程重新订阅");
      const list = await calls();
      const reattach = list.findIndex((c) => c.method === "thread/resume" && c.params?.threadId === CHILD);
      const rootStarts = list.map((c, i) => [c, i] as const).filter(([c]) => c.method === "turn/start" && c.params?.threadId === root).map(([, i]) => i);
      expect(rootStarts.length).toBeGreaterThanOrEqual(2);
      expect(reattach).toBeLessThan(rootStarts.at(-1)!);
      let parentTurn = "";
      await s.until(async () => {
        const turn = (await s.state()).threads[root].turns.at(-1);
        parentTurn = turn?.status === "inProgress" ? turn.id : "";
        return parentTurn !== "";
      }, 8_000, "第二次父轮开始");
      await s.control("reuse", { parentThreadId: root, parentTurnId: parentTurn, childThreadId: CHILD });
      await s.finish(CHILD, "第二次结果"); await s.observe();
      const endedSecond = () => s.notes("broker/run/ended").some((n: any) => n.run?.runId === second.runId);
      // reconciliation() 会匹配到第一次运行的汇总请求：这里按根上开轮次数判断第二次是否开了汇总轮。
      const startsBefore = (await s.starts()).length;
      await s.finish(root);
      await s.until(async () => endedSecond() || (await s.starts()).length > startsBefore, 10_000, "第二次汇总或结束");
      if (!endedSecond()) await s.finish(root, "已核实第二次结果");
      await s.until(async () => endedSecond(), 10_000, "第二次运行结束");
      await s.until(async () => count(unsubscribes(await s.state()), CHILD) === 2, 8_000, "子线程再次退订");
    } finally {
      await s.close();
    }
  }, 60_000);

  test("父线程不等子代理就结束：汇总期间不退订子线程，运行结束后退订一次", async () => {
    const s = new sa1b!.Sa1bHarness();
    try {
      await s.open(); await s.start(); await s.spawn(CHILD);
      await s.finish(sa1b!.SA1B_ROOT); await s.observe();
      await s.finish(CHILD, "子代理最终结果");
      const request = await s.reconciliation();
      expect(request?.params.input[0].text).toContain("子代理最终结果");
      expect(unsubscribes(await s.state())).not.toContain(CHILD);
      await s.finish(sa1b!.SA1B_ROOT, "已核实子代理最终结果");
      await s.until(async () => s.ended().length === 1, 8_000, "运行结束");
      await s.until(async () => unsubscribes(await s.state()).includes(CHILD), 8_000, "子线程退订");
      await s.observe(300);
      expect(count(unsubscribes(await s.state()), CHILD)).toBe(1);
    } finally {
      await s.close();
    }
  }, 60_000);
});
