/** 真实 broker 跨进程回归：失败跟进撤销占位，但保留本运行受管轮与最终释放门禁。 */
import { expect, test } from "bun:test";
import { Sa1bHarness, SA1B_ROOT as ROOT } from "../test-support/sa1b-harness";
import { bounded } from "./wait-run";
import { followup } from "./test-fixtures/v2-followup";

const CHILD = "placeholder-child";
const HISTORY = "historical-child";
const CURRENT_OUTPUT = "本运行受管轮结果-占位回归";
const HISTORY_OUTPUT = "历史运行结果-不得进入本次汇总";
const convergence = { timeoutMs: 20_000, discoveryTimeoutMs: 1200, wakeVerifyTimeoutMs: 1200 };

/** 夹具负责杀超时进程；外层期限同时约束进程退出与输出管道 EOF。 */
function cli(h: Sa1bHarness, args: string[], timeoutMs: number) {
  return bounded(h.cli(args, timeoutMs), timeoutMs + 2000);
}

async function openAndStart(h: Sa1bHarness) {
  await bounded(h.open({ pageSize: 1 }), 20_000);
  await h.owner.request("thread/start", {}, 6000);
  await bounded(h.start({ cxbConvergence: convergence }), 20_000);
}

async function diagnostic(h: Sa1bHarness): Promise<any> {
  const response: any = await h.owner.request("broker/runs/convergenceDiagnostics", { runId: h.runId }, 3000);
  return response.diagnostic;
}

async function ended(h: Sa1bHarness): Promise<any> {
  let run: any;
  await bounded(h.until(async () => {
    run = await h.record();
    return run?.runId === h.runId && run.state === "ended";
  }, 10_000, "失败跟进撤销后，运行未在期限内最终收敛"), 12_000);
  return run;
}

async function finishReconciliation(h: Sa1bHarness, expectedOutput: string, after = 1) {
  let request: any;
  await bounded(h.until(async () => {
    request = (await h.starts()).slice(after).find((entry: any) => entry.params.clientUserMessageId);
    return !!request || h.ended().length > 0;
  }, 8000, "当前运行未进入汇总"), 10_000);
  expect(request, "撤销失败占位后必须实际进入汇总轮").toBeDefined();
  const input = request.params.input.map((item: any) => item.text ?? "").join("\n");
  expect(input).toContain(expectedOutput);
  const turn = (await h.state()).threads.root.turns.at(-1);
  await h.finish(ROOT, "已核实本运行受管轮", "completed", turn.id);
  const run = await ended(h);
  expect(run, "必须经过汇总完成和最终核实才能释放").toMatchObject({
    state: "ended", status: "completed", finalTurnId: turn.id,
    subagentJoin: { phase: "released", wakeupsStarted: 1 },
  });
  expect(run.subagentJoin.reconciledEpoch).toBe(run.subagentJoin.workEpoch);
  return { run, input };
}

async function prepareResumedHistory(h: Sa1bHarness) {
  await openAndStart(h);
  const historicalTurn = await h.spawn(HISTORY);
  await h.finish(HISTORY, HISTORY_OUTPUT, "completed", historicalTurn);
  await h.finish(ROOT, "历史父轮完成");
  await finishReconciliation(h, HISTORY_OUTPUT);
  const oldRunId = h.runId;
  const snapshot = await h.control("snapshot");
  snapshot.threads[HISTORY].loaded = false;
  // 保留服务器历史与完整 broker 记录，重启后不重放历史派生通知。
  await bounded(h.restart({ snapshot, persisted: { state: "ended", status: "completed", children: [HISTORY] } }), 20_000);
  const resumed = await cli(h, ["run", "--resume", ROOT, "恢复后核实失败跟进", "--detach", "--json", "--converge-timeout", "20"], 12_000);
  expect(resumed.code, resumed.stderr).toBe(0);
  h.runId = JSON.parse(resumed.stdout).runId;
  expect(h.runId).not.toBe(oldRunId);
  h.firstTurn = (await h.state()).threads.root.turns.at(-1).id;
}

async function assertCleanFinalViews(h: Sa1bHarness, turnId: string) {
  await h.persisted({ state: "ended", status: "completed", children: [CHILD] });
  const logged = await cli(h, ["show", ROOT, "--log", "--tree", "--json"], 12_000);
  expect(logged.code, logged.stderr).toBe(0);
  const log = JSON.parse(logged.stdout);
  expect(log.items.length, "必须实际读到日志，不能用空日志掩盖缺口").toBeGreaterThan(0);
  expect(log.coverage.issues, "完整成功路径不得残留覆盖误报").toEqual([]);
  const waiting = await cli(h, ["wait", ROOT, "--tree", "--timeout", "10"], 15_000);
  expect(waiting.code, waiting.stderr).toBe(0);
  expect(waiting.stdout + waiting.stderr).not.toMatch(/陈旧|发现不完整|记录缺口|卡住/);
  const tree = await cli(h, ["wait", ROOT, "--tree", "--json", "--timeout", "10"], 15_000);
  expect(tree.code, tree.stderr).toBe(0);
  const result = JSON.parse(tree.stdout);
  expect(result.tree.stale).not.toBe(true);
  expect(result.tree.nodes.map((node: any) => node.threadId)).toEqual([CHILD]);
  expect(result.tree.nodes[0].result).toMatchObject({ turnId, output: CURRENT_OUTPUT });
  expect(JSON.stringify(result.tree)).not.toContain(HISTORY_OUTPUT);
  expect(await diagnostic(h), "终态后不能继续返回旧阻塞门").toBeNull();
}

for (const resumed of [false, true]) test(`失败跟进跨进程收敛：${resumed ? "含历史子代理、broker 重启和 --resume" : "新父线程"}`, async () => {
  const h = new Sa1bHarness();
  try {
    if (resumed) await prepareResumedHistory(h);
    else await openAndStart(h);
    const t1 = await h.spawn(CHILD);
    await h.finish(CHILD, CURRENT_OUTPUT, "completed", t1);
    const item = {
      id: "failed-followup-without-new-turn", type: "collabAgentToolCall", tool: "sendInput",
      status: "inProgress", senderThreadId: ROOT, receiverThreadIds: [CHILD], agentsStates: {},
    };
    // 先保存 started 条目，再单独投递通知；明确失败由测试下一步扣放，不依赖固定睡眠。
    await h.control("item", { threadId: ROOT, turnId: h.firstTurn, item, announce: false });
    await h.control("inject", { message: { method: "item/started", params: { threadId: ROOT, turnId: h.firstTurn, item } } });
    let before: any;
    await bounded(h.until(async () => {
      before = await diagnostic(h);
      return before?.activities.some((activity: any) => activity.threadId === CHILD && activity.turnId === null);
    }, 6000, "夹具前置：跟进必须实际建立 turnId=None 占位"), 8000);
    const original = before.activities.find((activity: any) => activity.threadId === CHILD && activity.turnId === t1);
    expect(original, "建立占位不能替换已完成 t1 的活动").toBeDefined();
    expect(before.pendingOperations.some((operation: any) => operation.receivers.includes(CHILD))).toBe(true);
    expect(h.ended()).toHaveLength(0);
    const online = await cli(h, ["show", ROOT, "--json"], 12_000);
    expect(online.code).toBe(5);
    expect(JSON.parse(online.stdout).convergenceDiagnostic.activities.some((activity: any) => activity.turnId === null)).toBe(true);

    await h.control("item", { threadId: ROOT, turnId: h.firstTurn, item: { ...item, status: "failed" } });
    const after = await diagnostic(h);
    expect(after.activities.find((activity: any) => activity.threadId === CHILD && activity.turnId === t1).activitySeq).toBe(original.activitySeq);
    expect((await h.state()).threads[CHILD].turns.map((turn: any) => turn.id)).toEqual([t1]);
    await h.finish(ROOT, "父工作轮已交结论");
    const { run, input } = await finishReconciliation(h, CURRENT_OUTPUT);
    expect(input).not.toContain(HISTORY_OUTPUT);
    expect(run.children.map((child: any) => child.threadId)).toEqual([CHILD]);
    expect(run.children[0].turnIds).toEqual([t1]);
    expect(run.subagentJoin.resultCoverage.total).toBe(1);
    expect(await h.starts()).toHaveLength(2);
    await assertCleanFinalViews(h, t1);
  } finally {
    await bounded(h.close(), 12_000);
  }
}, 90_000);

test("真实 raw 开关下退役旧轮迟到不能核销新跟进", async () => {
  const h = new Sa1bHarness();
  try {
    await openAndStart(h);
    const previous = await h.spawn(CHILD, ROOT, h.firstTurn, { version: "v2" });
    await h.finish(CHILD, "历史结果", "completed", previous);
    await h.finish(ROOT, "历史父完成");
    await finishReconciliation(h, "历史结果");
    const response: any = await h.owner.request("turn/start", { threadId: ROOT, input: [{ type: "text", text: "下一运行" }], cxbConvergence: convergence }, 6000);
    h.runId = response.runId;
    h.firstTurn = (await h.state()).threads.root.turns.at(-1).id;
    const before = (await h.starts()).length;
    await followup(h, CHILD, { open: false, consumeOnNextTurn: true, message: "新运行任务" });
    for (const status of ["inProgress", "completed"]) await h.control("inject", { message: {
      method: status === "inProgress" ? "turn/started" : "turn/completed",
      params: { threadId: CHILD, turn: { id: previous, status, items: [] } },
    } });
    expect((await diagnostic(h)).pendingOperations).toHaveLength(1);
    const { turnId } = await h.control("turn", { threadId: CHILD, action: "start" });
    await h.finish(CHILD, CURRENT_OUTPUT, "completed", turnId);
    await h.finish(ROOT, "本次父完成");
    await finishReconciliation(h, CURRENT_OUTPUT, before);
    await assertCleanFinalViews(h, turnId);
  } finally { await bounded(h.close(), 12_000); }
}, 90_000);

test("冷恢复父线程没有 raw 时宽松回退并正常收尾", async () => {
  const h = new Sa1bHarness();
  try {
    await bounded(h.open(), 20_000);
    await h.owner.request("thread/start", {}, 6000);
    await h.control("unload", { threadId: ROOT });
    await h.owner.request("thread/resume", { threadId: ROOT }, 6000);
    await bounded(h.start({ cxbConvergence: convergence }), 20_000);
    expect((await h.state()).threads.root.experimentalRawEvents).toBe(false);
    const old = await h.spawn(CHILD, ROOT, h.firstTurn, { version: "v2" });
    await h.finish(CHILD, "旧结果", "completed", old);
    const reused = await followup(h, CHILD, { message: "新工作" });
    expect((await h.state()).threads[CHILD].experimentalRawEvents).toBe(false);
    expect((await h.state()).events.filter((e: any) => e.method === "rawResponseItem/completed")).toHaveLength(0);
    await h.finish(CHILD, CURRENT_OUTPUT, "completed", reused.turnId);
    await h.finish(ROOT, "父轮结束");
    await finishReconciliation(h, CURRENT_OUTPUT);
    await assertCleanFinalViews(h, reused.turnId);
  } finally { await bounded(h.close(), 12_000); }
}, 90_000);

test("父 raw 监听迟到：子轮和消费先送达仍严格关联并收敛", async () => {
  const h = new Sa1bHarness();
  try {
    await openAndStart(h);
    const old = await h.spawn(CHILD, ROOT, h.firstTurn, { version: "v2" });
    await h.finish(CHILD, "旧结果", "completed", old);
    for (const [name, method] of [["raw-parent", "rawResponseItem/completed"], ["start-parent", "item/started"], ["done-parent", "item/completed"]]) {
      await h.control("fence", { name, method, threadId: ROOT });
    }
    const reused = await followup(h, CHILD, { message: "先到子线程的输入", open: true });
    await h.finish(CHILD, CURRENT_OUTPUT, "completed", reused.turnId);
    // 按父线程自己的顺序释放，保留父子监听器交错。
    for (const name of ["raw-parent", "start-parent", "done-parent"]) await h.control("release", { name });
    await h.finish(ROOT, "父完成");
    await finishReconciliation(h, CURRENT_OUTPUT);
    await assertCleanFinalViews(h, reused.turnId);
  } finally { await bounded(h.close(), 12_000); }
}, 90_000);

test("真实 raw 开关下旧轮跨基线和重叠操作仍阻塞", async () => {
  const h = new Sa1bHarness();
  try {
    await openAndStart(h);
    const old = await h.spawn(CHILD, ROOT, h.firstTurn, { version: "v2" });
    await followup(h, CHILD, { open: false, consumeOnNextTurn: true, message: "第一跟进" });
    await h.finish(CHILD, "旧轮完但跟进未消费", "completed", old);
    expect((await diagnostic(h)).pendingOperations).toHaveLength(1);
    await followup(h, CHILD, { open: false, consumeOnNextTurn: true, message: "第二跟进" });
    await h.control("fence", { name: "inputs", method: "rawResponseItem/completed", threadId: CHILD });
    const { turnId } = await h.control("turn", { threadId: CHILD, action: "start" });
    expect((await diagnostic(h)).pendingOperations).toHaveLength(2);
    await h.control("release", { name: "inputs" });
    expect((await diagnostic(h)).pendingOperations).toHaveLength(0);
    await followup(h, CHILD, { open: false, consumeOnNextTurn: true, message: "尚未消费的第三跟进" });
    await h.finish(CHILD, "前两个跟进完成", "completed", turnId);
    expect((await diagnostic(h)).pendingOperations).toHaveLength(1);
    const finalTurn = (await h.control("turn", { threadId: CHILD, action: "start" })).turnId;
    await h.finish(CHILD, CURRENT_OUTPUT, "completed", finalTurn);
    await h.finish(ROOT, "父完成");
    await finishReconciliation(h, CURRENT_OUTPUT);
    await assertCleanFinalViews(h, finalTurn);
  } finally { await bounded(h.close(), 12_000); }
}, 90_000);

for (const sameTurn of [false, true]) test(`v2 跟进忙碌子代理：${sameTurn ? "旧轮核实输入后吸收" : "旧轮结束后新轮承载"}正常收敛`, async () => {
  const h = new Sa1bHarness();
  try {
    await openAndStart(h);
    const previous = await h.spawn(CHILD, ROOT, h.firstTurn, { version: "v2" });
    await h.control("fence", { name: "consume", method: "rawResponseItem/completed", threadId: CHILD });
    await followup(h, CHILD, { open: false, consumeOnNextTurn: !sameTurn, message: "额外的跟进工作" });
    const followups = (await diagnostic(h)).pendingOperations.filter((op: any) => op.kind === "v2Interacted");
    expect(followups).toHaveLength(1);
    expect(followups[0].pairingMode, "broker 新开的线程必须走严格配对").toBe("strict");
    expect(followups[0].fallbackReason, "严格配对不带回退原因").toBeNull();
    await h.finish(ROOT, "父轮先结束");
    let carryingTurn = previous;
    if (sameTurn) {
      await h.control("release", { name: "consume" });
      expect((await diagnostic(h)).pendingOperations).toHaveLength(0);
      expect(h.ended()).toHaveLength(0);
    } else {
      await h.finish(CHILD, "原任务完成，但还没处理跟进", "completed", previous);
      expect((await diagnostic(h)).pendingOperations).toHaveLength(1);
      expect(h.ended()).toHaveLength(0);
      await h.control("release", { name: "consume" });
      carryingTurn = (await h.control("turn", { threadId: CHILD, action: "start" })).turnId;
    }
    await h.finish(CHILD, CURRENT_OUTPUT, "completed", carryingTurn);
    const { run } = await finishReconciliation(h, CURRENT_OUTPUT);
    expect(run.subagentJoin.resultCoverage.total).toBe(sameTurn ? 1 : 2);
    await assertCleanFinalViews(h, carryingTurn);
  } finally { await bounded(h.close(), 12_000); }
}, 90_000);

/** 冷恢复：broker 重开线程后没有 raw，只能靠会话文件。 */
async function openWithoutRaw(h: Sa1bHarness) {
  await bounded(h.open(), 20_000);
  await h.owner.request("thread/start", {}, 6000);
  await h.control("unload", { threadId: ROOT });
  await h.owner.request("thread/resume", { threadId: ROOT }, 6000);
  await bounded(h.start({ cxbConvergence: convergence }), 20_000);
  expect((await h.state()).threads.root.experimentalRawEvents).toBe(false);
}

async function strictFollowup(h: Sa1bHarness, source: "raw" | "rolloutFile"): Promise<any> {
  let op: any;
  await bounded(h.until(async () => {
    op = (await diagnostic(h)).pendingOperations.find((entry: any) => entry.kind === "v2Interacted");
    return !op || op.pairingMode !== "awaitingRollout";
  }, 6000, "等会话文件定案"), 8000);
  if (op) {
    expect(op.pairingMode, "这次有证据，必须走严格配对").toBe("strict");
    expect(op.pairingSource).toBe(source);
    expect(op.fallbackReason).toBeNull();
  }
  return op;
}

for (const source of ["raw", "rolloutFile"] as const) {
  test(`v2 跟进已答完的子代理（${source}）：旧轮结束不算读到，自动开的下一轮承载后才收敛`, async () => {
    const h = new Sa1bHarness();
    try {
      if (source === "raw") await openAndStart(h); else await openWithoutRaw(h);
      const old = await h.spawn(CHILD, ROOT, h.firstTurn, { version: "v2" });
      await h.control("item", { threadId: CHILD, turnId: old, item: { id: "早答", type: "agentMessage", text: "旧轮答完了", phase: "final_answer" } });
      await followup(h, CHILD, { message: "答完之后才到的跟进" });
      expect((await h.state()).threads[CHILD].turns).toHaveLength(1);
      expect(await strictFollowup(h, source), "跟进还没被读到，操作必须待定").toBeDefined();
      await h.finish(ROOT, "父轮先交结论");
      await h.finish(CHILD, "旧轮结果", "completed", old);
      const turns = (await h.state()).threads[CHILD].turns;
      expect(turns, "旧轮结束后自动开下一轮").toHaveLength(2);
      const next = turns[1].id;
      expect(h.ended(), "下一轮还没结束，不能收尾").toHaveLength(0);
      await bounded(h.until(async () => {
        const left = await diagnostic(h);
        return !left || left.pendingOperations.filter((entry: any) => entry.kind === "v2Interacted").length === 0;
      }, 6000, "下一轮的消费记录必须配上跟进"), 8000);
      await h.finish(CHILD, CURRENT_OUTPUT, "completed", next);
      const { run } = await finishReconciliation(h, CURRENT_OUTPUT);
      expect(run.children[0].turnIds).toEqual([old, next]);
      await assertCleanFinalViews(h, next);
    } finally { await bounded(h.close(), 12_000); }
  }, 90_000);

  test(`v2 跟进没答完的子代理（${source}）：被当前轮吸收，不开新轮也能收敛`, async () => {
    const h = new Sa1bHarness();
    try {
      if (source === "raw") await openAndStart(h); else await openWithoutRaw(h);
      const busy = await h.spawn(CHILD, ROOT, h.firstTurn, { version: "v2" });
      await followup(h, CHILD, { message: "忙的时候到的跟进" });
      expect((await h.state()).threads[CHILD].turns, "不开新轮").toHaveLength(1);
      await bounded(h.until(async () => {
        const left = await diagnostic(h);
        return !left || left.pendingOperations.filter((entry: any) => entry.kind === "v2Interacted").length === 0;
      }, 6000, "当前轮的消费记录必须配上跟进"), 8000);
      await h.finish(ROOT, "父轮先交结论");
      await h.finish(CHILD, CURRENT_OUTPUT, "completed", busy);
      await finishReconciliation(h, CURRENT_OUTPUT);
      await assertCleanFinalViews(h, busy);
    } finally { await bounded(h.close(), 12_000); }
  }, 90_000);
}

test("v2 只入队消息给空闲子代理：不占待定操作、不开新轮也能收敛", async () => {
  const h = new Sa1bHarness();
  try {
    await openAndStart(h);
    const turn = await h.spawn(CHILD, ROOT, h.firstTurn, { version: "v2" });
    await h.finish(CHILD, CURRENT_OUTPUT, "completed", turn);
    await followup(h, CHILD, { tool: "send_message", message: "顺带告知，不需要处理" });
    expect((await diagnostic(h)).pendingOperations).toHaveLength(0);
    await h.finish(ROOT, "父完成");
    await finishReconciliation(h, CURRENT_OUTPUT);
    await assertCleanFinalViews(h, turn);
  } finally { await bounded(h.close(), 12_000); }
}, 90_000);

test("v2 三个已完成子代理跟进：父轮完成后子轮才开始并最终收敛", async () => {
  const h = new Sa1bHarness();
  const children = ["v2-a", "v2-b", "v2-c"];
  try {
    await openAndStart(h);
    for (const child of children) {
      const previous = await h.spawn(child, ROOT, h.firstTurn, { version: "v2" });
      await h.finish(child, `前轮:${child}`, "completed", previous);
      await followup(h, child, { open: false, consumeOnNextTurn: true, message: `工作:${child}` });
    }
    const before = await diagnostic(h);
    expect(before.pendingOperations.filter((op: any) => op.kind === "v2Interacted")).toHaveLength(3);
    await h.finish(ROOT, "父轮先交结论");
    expect(h.ended()).toHaveLength(0);
    for (const child of children) {
      const { turnId } = await h.control("turn", { threadId: child, action: "start" });
      await h.finish(child, `跟进结果:${child}`, "completed", turnId);
    }
    const { run, input } = await finishReconciliation(h, "跟进结果:v2-a");
    for (const child of children) expect(input).toContain(`跟进结果:${child}`);
    expect(run.children).toHaveLength(3);
    expect(run.children.every((child: any) => child.turnIds.length === 2)).toBe(true);
    expect(run.subagentJoin.resultCoverage.total).toBe(6);
    await h.persisted({ state: "ended", status: "completed", children });
    const log = await cli(h, ["show", ROOT, "--log", "--tree", "--json"], 12_000);
    expect(log.code, log.stderr).toBe(0);
    expect(JSON.parse(log.stdout).coverage.issues).toEqual([]);
    const wait = await cli(h, ["wait", ROOT, "--tree", "--timeout", "10"], 15_000);
    expect(wait.code, wait.stderr).toBe(0);
    expect(wait.stdout + wait.stderr).not.toMatch(/陈旧|记录缺口|发现不完整/);
    expect(await diagnostic(h)).toBeNull();
  } finally { await bounded(h.close(), 12_000); }
}, 90_000);
