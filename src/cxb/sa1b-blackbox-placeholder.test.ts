/** 真实 broker 跨进程回归：失败跟进撤销占位，但保留本运行受管轮与最终释放门禁。 */
import { expect, test } from "bun:test";
import { Sa1bHarness, SA1B_ROOT as ROOT } from "../test-support/sa1b-harness";
import { bounded } from "./wait-run";

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

async function finishReconciliation(h: Sa1bHarness, expectedOutput: string) {
  const request = await bounded(h.reconciliation(8000), 10_000);
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

test("v2 三个已完成子代理跟进：父轮完成后子轮才开始并最终收敛", async () => {
  const h = new Sa1bHarness();
  const children = ["v2-a", "v2-b", "v2-c"];
  try {
    await openAndStart(h);
    for (const child of children) {
      const previous = await h.spawn(child, ROOT, h.firstTurn, { version: "v2" });
      await h.finish(child, `前轮:${child}`, "completed", previous);
      const item = { id: `v2-followup-${child}`, type: "subAgentActivity", kind: "interacted", agentThreadId: child, agentPath: `/root/${child}` };
      await h.control("inject", { message: { method: "rawResponseItem/completed", params: {
        threadId: ROOT, turnId: h.firstTurn, item: { type: "function_call", name: "followup_task", call_id: item.id },
      } } });
      await h.control("inject", { message: { method: "item/started", params: { threadId: ROOT, turnId: h.firstTurn, item } } });
      await h.control("item", { threadId: ROOT, turnId: h.firstTurn, item });
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
