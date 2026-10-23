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
import { createSa1bMock, type RpcMessage } from "./test-support/sa1b-mock";

const RUNNABLE = process.platform === "win32";
const sa1b = RUNNABLE ? await import("./test-support/sa1b-harness") : null;
const CHILD = "child-1";

const unsubscribes = (state: any): string[] =>
  state.calls.filter((c: any) => c.method === "thread/unsubscribe").map((c: any) => c.params?.threadId);
const count = (list: string[], thread: string) => list.filter((t) => t === thread).length;
const APPROVAL = "item/commandExecution/requestApproval";

/** 每个客户端请求都保留拒绝结果，避免测试等待期间产生未处理的 Promise 拒绝。 */
const begin = (s: InstanceType<NonNullable<typeof sa1b>["Sa1bHarness"]>, threadId = "root") =>
  s.owner.request("turn/start", { threadId, input: [{ type: "text", text: "复用子代理继续" }], model: "original-model", effort: "high" }, 25_000)
    .then(value => ({ value: value as any, error: undefined as unknown }), error => ({ value: undefined, error }));

async function retired(s: InstanceType<NonNullable<typeof sa1b>["Sa1bHarness"]>) {
  await s.open(); await s.start(); await s.spawn(CHILD);
  await s.finish(CHILD, "第一次结果"); await s.observe(); await s.finish(sa1b!.SA1B_ROOT);
  if (await s.reconciliation()) await s.finish(sa1b!.SA1B_ROOT, "已核实第一次结果");
  await s.until(async () => s.ended().length === 1, 8_000, "第一次运行结束");
  await s.until(async () => unsubscribes(await s.state()).includes(CHILD), 8_000, "子线程退订");
  await s.until(async () => !(await s.state()).subscriptions.broker.includes(CHILD), 3_000, "退订实际生效");
}

async function approvalAndFinish(s: InstanceType<NonNullable<typeof sa1b>["Sa1bHarness"]>, runId: string) {
  const root = sa1b!.SA1B_ROOT;
  const parentTurn = (await s.state()).threads[root].turns.at(-1).id;
  const reused = await s.control("reuse", { parentThreadId: root, parentTurnId: parentTurn, childThreadId: CHILD });
  const requestId = `审批-${runId}`;
  await s.control("inject", { message: { id: requestId, method: APPROVAL,
    params: { threadId: CHILD, turnId: reused.turnId, itemId: requestId, command: "echo 子线程", cwd: process.env.TEMP! } } });
  const forwarded = () => s.owner.messages.find((m: any) => m.method === APPROVAL && m.params?.itemId === requestId);
  await s.until(async () => !!forwarded(), 5_000, "挂回后子线程审批实际送达 owner");
  expect((forwarded() as any).params.cxbOrigin.runId).toBe(runId);
  s.owner.send({ id: forwarded()!.id as string, result: { decision: "accept" } });
  await s.until(async () => (await s.state()).threads[CHILD].answers.some((a: any) => a.id === requestId), 5_000, "审批答复回到子线程");
  await s.finish(CHILD, "第二次结果"); await s.observe();
  const ended = () => s.notes("broker/run/ended").some((n: any) => n.run?.runId === runId);
  const startsBefore = (await s.starts()).length;
  await s.finish(root);
  await s.until(async () => ended() || (await s.starts()).length > startsBefore, 10_000, "第二次汇总或结束");
  if (!ended()) await s.finish(root, "已核实第二次结果");
  await s.until(async () => ended(), 10_000, "第二次运行结束");
  await s.until(async () => !(await s.state()).subscriptions.broker.includes(CHILD), 8_000, "子线程最终再次撒手");
}

describe("子线程释放夹具：四环节独立控制", () => {
  function fixture() {
    const output: { message: RpcMessage; connectionId: string | undefined }[] = [];
    const mock = createSa1bMock((message, connectionId) => output.push({ message, connectionId }));
    let id = 0;
    const call = (method: string, params: any = {}, connectionId = "broker") => {
      const requestId = ++id;
      mock.handle({ id: requestId, method, params }, true, connectionId);
      return { id: requestId, reply: () => output.find(x => x.message.id === requestId && !x.message.method)?.message };
    };
    const control = (name: string, params: any = {}) => call(`fixture/sa1b/${name}`, params);
    call("thread/resume", { threadId: CHILD });
    return { mock, output, call, control };
  }

  test("按连接摘除订阅；广播仍送达；三种退订成功状态分别保留", () => {
    const f = fixture();
    f.control("connection", { connectionId: "其他连接", threadIds: [CHILD] });
    expect(f.call("thread/unsubscribe", { threadId: CHILD }).reply()?.result.status).toBe("unsubscribed");
    expect(f.call("thread/unsubscribe", { threadId: CHILD }).reply()?.result.status).toBe("notSubscribed");
    f.control("inject", { message: { method: "item/agentMessage/delta", params: { threadId: CHILD, itemId: "仅订阅者" } } });
    expect(f.output.filter(x => x.message.params?.itemId === "仅订阅者").map(x => x.connectionId)).toEqual(["其他连接"]);
    f.control("inject", { message: { method: "thread/goal/cleared", params: { threadId: CHILD } } });
    expect(f.output.filter(x => x.message.method === "thread/goal/cleared").map(x => x.connectionId)).toEqual(["broker", "其他连接"]);
    f.control("unload", { threadId: CHILD, closed: false });
    expect(f.call("thread/unsubscribe", { threadId: CHILD }).reply()?.result.status).toBe("notLoaded");
    expect(f.output.some(x => x.message.method === "thread/closed")).toBe(false);
  });

  test("取得旧快照后退订成功，旧通知与旧服务端请求仍可以迟到", () => {
    const f = fixture();
    f.control("fence", { name: "旧通知", method: "turn/started", threadId: CHILD });
    f.control("fence", { name: "旧审批", method: APPROVAL, threadId: CHILD });
    f.control("inject", { message: { method: "turn/started", params: { threadId: CHILD, turn: { id: "旧轮" } } } });
    f.control("inject", { message: { id: "旧审批", method: APPROVAL, params: { threadId: CHILD, turnId: "旧轮" } } });
    expect(f.call("thread/unsubscribe", { threadId: CHILD }).reply()?.result.status).toBe("unsubscribed");
    f.control("release", { name: "旧通知" }); f.control("release", { name: "旧审批" });
    expect(f.output.some(x => x.connectionId === "broker" && x.message.method === "turn/started")).toBe(true);
    expect(f.output.some(x => x.connectionId === "broker" && x.message.id === "旧审批")).toBe(true);
    expect(f.mock.state().subscriptions.broker).not.toContain(CHILD);
  });

  test("快照前栅栏读取变更后的订阅集合，送达栅栏不重取快照", () => {
    const f = fixture();
    f.control("fence", { name: "尚未快照", direction: "snapshot", method: "turn/started", threadId: CHILD });
    f.control("inject", { message: { method: "turn/started", params: { threadId: CHILD, turn: { id: "不应送达" } } } });
    f.call("thread/unsubscribe", { threadId: CHILD });
    f.control("release", { name: "尚未快照" });
    expect(f.output.some(x => x.message.method === "turn/started")).toBe(false);
  });

  test("同线程执行串行，但响应送达独立；迟到旧 resume 回包不改变后来退订的真实状态", () => {
    const f = fixture();
    f.call("thread/unsubscribe", { threadId: CHILD });
    f.control("fence", { name: "执行", direction: "request", method: "thread/resume", threadId: CHILD });
    f.control("fence", { name: "响应", direction: "response", method: "thread/resume", threadId: CHILD });
    const resume = f.call("thread/resume", { threadId: CHILD });
    const unsubscribe = f.call("thread/unsubscribe", { threadId: CHILD });
    expect(unsubscribe.reply()).toBeUndefined();
    expect(f.mock.state().subscriptions.broker).not.toContain(CHILD);
    f.control("release", { name: "执行" });
    expect(unsubscribe.reply()?.result.status).toBe("unsubscribed");
    expect(resume.reply()).toBeUndefined();
    f.control("release", { name: "响应" });
    expect(resume.reply()?.result.thread.id).toBe(CHILD);
    expect(f.mock.state().subscriptions.broker).not.toContain(CHILD);
  });

  test("closed 后在途 resume 能冷恢复；只读不加载；closing、存储不存在与附着后报错分别可控", () => {
    const f = fixture();
    f.control("fence", { name: "冷恢复", direction: "request", method: "thread/resume", threadId: CHILD });
    const resumed = f.call("thread/resume", { threadId: CHILD });
    f.control("unload", { threadId: CHILD });
    expect(f.mock.state().threads[CHILD]!.loaded).toBe(false);
    f.control("release", { name: "冷恢复" });
    expect(resumed.reply()?.result.thread.status.type).toBe("idle");
    expect(f.mock.state().subscriptions.broker).toContain(CHILD);
    f.control("unload", { threadId: CHILD, closed: false });
    expect(f.call("thread/read", { threadId: CHILD }).reply()?.result.thread.status.type).toBe("notLoaded");
    expect(f.mock.state().threads[CHILD]!.loaded).toBe(false);
    f.control("metadata", { threadId: CHILD, closing: true });
    expect(f.call("thread/resume", { threadId: CHILD }).reply()?.error.message).toContain("is closing");
    f.control("metadata", { threadId: CHILD, closing: false, stored: false });
    expect(f.call("thread/resume", { threadId: CHILD }).reply()?.error.message).toContain("no rollout found for thread id");
    expect(f.call("thread/read", { threadId: CHILD }).reply()?.error.message).toBe(`thread not loaded: ${CHILD}`);
    f.control("metadata", { threadId: CHILD, loaded: true });
    expect(f.call("thread/read", { threadId: CHILD }).reply()?.result.thread.status.type).toBe("idle");
    f.control("metadata", { threadId: CHILD, stored: true });
    f.control("fault", { method: "thread/resume", threadId: CHILD, mode: "attachedError", count: 1 });
    expect(f.call("thread/resume", { threadId: CHILD }).reply()?.error.message).toContain("已订阅后");
    expect(f.mock.state().subscriptions.broker).toContain(CHILD);
  });
});

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

  test("复用已撒手子线程：订阅生效与回包分别等待，挂回前事件不可达，挂回后审批可达并再次撒手", async () => {
    const s = new sa1b!.Sa1bHarness();
    try {
      await retired(s);
      await s.control("fence", { name: "挂回执行", direction: "request", method: "thread/resume", threadId: CHILD });
      await s.control("fence", { name: "挂回回包", direction: "response", method: "thread/resume", threadId: CHILD });
      const startsBefore = (await s.starts()).length;
      const pending = begin(s);
      await s.until(async () => ((await s.state()).requestHeld["挂回执行"] ?? []).length > 0, 5_000, "挂回请求进入执行栅栏");
      await s.control("inject", { message: { method: "item/agentMessage/delta", params: { threadId: CHILD, turnId: "旧轮", itemId: "不可达", delta: "挂回前不应收到" } } });
      await s.observe();
      expect((await s.state()).subscriptions.broker).not.toContain(CHILD);
      expect((await s.state()).events.some((e: any) => e.params?.itemId === "不可达")).toBe(false);
      expect(s.owner.messages.some((e: any) => e.params?.itemId === "不可达")).toBe(false);
      expect((await s.starts()).length).toBe(startsBefore);
      await s.control("release", { name: "挂回执行" });
      await s.until(async () => ((await s.state()).held["挂回回包"] ?? []).length > 0, 5_000, "挂回已生效但回包未送达");
      expect((await s.state()).subscriptions.broker).toContain(CHILD);
      expect((await s.starts()).length).toBe(startsBefore);
      await s.control("release", { name: "挂回回包" });
      const second = await pending;
      expect(second.error).toBeUndefined();
      await approvalAndFinish(s, second.value.runId);
    } finally { await s.close(); }
  }, 60_000);

  test("挂回先订阅后报错：本次开工失败、不发父轮，下一次重试收到审批并完成收敛", async () => {
    const s = new sa1b!.Sa1bHarness();
    try {
      await retired(s);
      const startsBefore = (await s.starts()).length;
      await s.control("fault", { method: "thread/resume", threadId: CHILD, mode: "attachedError" });
      const failed = await begin(s);
      expect(String(failed.error)).toContain(`子代理线程 ${CHILD} 恢复订阅失败`);
      expect((await s.starts()).length).toBe(startsBefore);
      await s.control("fault", { method: "thread/resume", threadId: CHILD, mode: "attachedError", count: 0 });
      const resumed = await begin(s);
      expect(resumed.error).toBeUndefined();
      await approvalAndFinish(s, resumed.value.runId);
    } finally { await s.close(); }
  }, 70_000);

  test("挂回回包迟到超过总期限：开工失败，迟到成功后最终再次撒手", async () => {
    const s = new sa1b!.Sa1bHarness();
    try {
      await retired(s);
      const startsBefore = (await s.starts()).length;
      const unsubBefore = count(unsubscribes(await s.state()), CHILD);
      await s.control("fence", { name: "迟到成功", direction: "response", method: "thread/resume", threadId: CHILD });
      const pending = begin(s);
      await s.until(async () => ((await s.state()).held["迟到成功"] ?? []).length > 0, 5_000, "挂回成功回包被延迟");
      const failed = await pending;
      expect(String(failed.error)).toContain(`子代理线程 ${CHILD} 恢复订阅失败`);
      expect((await s.starts()).length).toBe(startsBefore);
      await s.control("release", { name: "迟到成功" });
      await s.until(async () => {
        const state = await s.state();
        return count(unsubscribes(state), CHILD) > unsubBefore && !state.subscriptions.broker.includes(CHILD);
      }, 8_000, "迟到成功后的补偿退订生效");
    } finally { await s.close(); }
  }, 60_000);

  test("未发送 closed 的卸载：只读核实未加载，下一次父运行不冷恢复该子线程", async () => {
    const s = new sa1b!.Sa1bHarness();
    try {
      await retired(s);
      await s.control("unload", { threadId: CHILD, closed: false });
      const resumesBefore = (await s.state()).calls.filter((c: any) => c.method === "thread/resume" && c.params?.threadId === CHILD).length;
      const resumed = await begin(s);
      expect(resumed.error).toBeUndefined();
      const state = await s.state();
      expect(state.threads[CHILD].loaded).toBe(false);
      expect(state.calls.filter((c: any) => c.method === "thread/resume" && c.params?.threadId === CHILD)).toHaveLength(resumesBefore);
      await s.finish(sa1b!.SA1B_ROOT, "已卸载子线程无需复用");
      await s.until(async () => s.notes("broker/run/ended").some((n: any) => n.run?.runId === resumed.value.runId), 8_000, "父运行结束");
    } finally { await s.close(); }
  }, 60_000);

  test("退订返回 NotLoaded：确认未加载后父运行不再恢复已卸载子线程", async () => {
    const s = new sa1b!.Sa1bHarness();
    try {
      await s.open(); await s.start(); await s.spawn(CHILD);
      await s.control("fence", { name: "卸载先行", direction: "request", method: "thread/unsubscribe", threadId: CHILD });
      await s.finish(CHILD); await s.observe(); await s.finish(sa1b!.SA1B_ROOT);
      if (await s.reconciliation()) await s.finish(sa1b!.SA1B_ROOT, "已核实子代理结果");
      await s.until(async () => ((await s.state()).requestHeld["卸载先行"] ?? []).length > 0, 8_000, "退订等待执行");
      await s.control("unload", { threadId: CHILD, closed: false });
      const requestId = (await s.state()).requestHeld["卸载先行"][0].id;
      await s.control("release", { name: "卸载先行" });
      expect((await s.state()).events.find((e: any) => e.id === requestId)?.result.status).toBe("notLoaded");
      const resumed = await begin(s);
      expect(resumed.error).toBeUndefined();
      expect((await s.state()).calls.some((c: any) => c.method === "thread/resume" && c.params?.threadId === CHILD)).toBe(false);
      await s.finish(sa1b!.SA1B_ROOT, "无需冷恢复");
      await s.until(async () => s.notes("broker/run/ended").some((n: any) => n.run?.runId === resumed.value.runId), 8_000, "父运行结束");
    } finally { await s.close(); }
  }, 60_000);

  test("子线程独立运行结束后再被原父代理复用：普通退订仍能挂回并收到审批", async () => {
    const s = new sa1b!.Sa1bHarness();
    try {
      await retired(s);
      const independent = await begin(s, CHILD);
      expect(independent.error).toBeUndefined();
      const unsubBefore = count(unsubscribes(await s.state()), CHILD);
      await s.finish(CHILD, "独立运行完成");
      await s.until(async () => s.notes("broker/run/ended").some((n: any) => n.run?.runId === independent.value.runId), 8_000, "子线程独立运行结束");
      await s.until(async () => count(unsubscribes(await s.state()), CHILD) > unsubBefore, 8_000, "独立根普通退订");
      const resumed = await begin(s);
      expect(resumed.error).toBeUndefined();
      await approvalAndFinish(s, resumed.value.runId);
    } finally { await s.close(); }
  }, 70_000);

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
