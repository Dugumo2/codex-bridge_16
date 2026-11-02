/** 真实 broker 与模拟上游：验证 CLI 树投影、只读补读与既有等待出口。 */
import { expect, test } from "bun:test";
import { Sa1bHarness, SA1B_ROOT as ROOT } from "../test-support/sa1b-harness";
import type { TreeView } from "./tree-contracts";
import { bounded } from "./wait-run";

const convergence = { timeoutMs: 20_000, discoveryTimeoutMs: 400, wakeVerifyTimeoutMs: 600 };

function scenario(name: string, body: (h: Sa1bHarness) => Promise<void>) {
  test(`wait --tree 真实 broker：${name}`, async () => {
    const h = new Sa1bHarness();
    try {
      await h.open({ pageSize: 1 });
      await h.start({ cxbConvergence: convergence });
      await body(h);
    } finally { await bounded(h.close(), 12_000); }
  }, 45_000);
}

/** 子任务完成之后显式完成 broker 发起的汇总轮；不以睡眠猜测收敛时机。 */
async function completeRoot(h: Sa1bHarness, output = "父运行确认完成") {
  await h.finish(ROOT, "父初轮完成");
  const request = await h.reconciliation(6000);
  expect(request, "测试前置：broker 应发起子结果汇总轮").toBeDefined();
  const turn = (await h.state()).threads.root.turns.at(-1);
  await h.finish(ROOT, output, "completed", turn.id);
  await h.until(async () => h.ended().length > 0, 8000, "父汇总轮未结束");
  expect(h.ended().at(-1).status).toBe("completed");
}

function json(result: { stdout: string; stderr: string }) {
  // JSON.parse 会同时拒绝第二个对象以及混入 stdout 的文本树。
  expect(result.stdout.trim(), "JSON 模式必须交付一个结果对象").not.toBe("");
  const value = JSON.parse(result.stdout);
  expect(value.tree).toBeDefined();
  return value as { tree: TreeView; [key: string]: any };
}

async function terminalTree(h: Sa1bHarness) {
  const before = (await h.state()).calls.length;
  const result = await h.cli(["wait", ROOT, "--tree", "--json", "--timeout", "10"], 15_000);
  expect(result.code).toBe(0);
  const value = json(result);
  expect(value).toMatchObject({ ok: true, command: "wait", runId: h.runId, status: "completed", alreadyDone: true });
  expect(value.tree).toMatchObject({ rootThreadId: ROOT, view: "terminal", resultScope: "latestBoundTurnPerThread" });
  const calls = (await h.state()).calls.slice(before);
  expect(calls.filter((call: any) => ["thread/resume", "turn/start", "turn/steer"].includes(call.method))).toEqual([]);
  return { value, calls, result };
}

for (const version of ["v1", "v2"] as const) {
  scenario(`${version} 多层树保留失败子轮而父运行成功`, async h => {
    const childTurn = await h.spawn("child", ROOT, h.firstTurn, { version });
    const grandchildTurn = await h.spawn("grandchild", "child", childTurn, { version });
    await h.finish("grandchild", "孙代理发现断言失败", "failed", grandchildTurn);
    await h.finish("child", "子代理已报告失败详情", "completed", childTurn);
    await completeRoot(h);
    const { value, calls } = await terminalTree(h);
    expect(value.result).toBe("父运行确认完成");
    expect(value.tree.nodes.map(node => node.threadId).sort()).toEqual(["child", "grandchild"]);
    expect(value.tree.nodes.find(node => node.threadId === "child")).toMatchObject({
      parentThreadId: ROOT, activity: "settled", result: { turnId: childTurn, status: "completed", availability: "available", output: "子代理已报告失败详情" },
    });
    expect(value.tree.nodes.find(node => node.threadId === "grandchild")).toMatchObject({
      parentThreadId: "child", activity: "settled", result: { turnId: grandchildTurn, status: "failed", availability: "available", output: "孙代理发现断言失败" },
    });
    expect(value.tree.outputCoverage).toMatchObject({ total: 2, available: 2, unavailable: 0 });
    expect(calls.filter((call: any) => call.method === "thread/turns/list").length).toBeGreaterThanOrEqual(2);
    expect(calls.filter((call: any) => call.method === "thread/items/list").every((call: any) =>
      call.params.threadId === "child" ? call.params.turnId === childTurn : call.params.threadId !== "grandchild" || call.params.turnId === grandchildTurn)).toBe(true);
  });
}

scenario("后续未绑定轮不能冒充本运行结果，受管轮在后续页仍能读取", async h => {
  const bound = await h.spawn("child");
  await h.finish("child", "本运行的绑定结果", "completed", bound);
  await completeRoot(h);
  // 已结束运行的绑定集合固定；模拟子线程后来被其他运行复用的服务器历史。
  for (const id of ["zz-other-run", "aa-newest-other-run"]) {
    await h.control("turn", { threadId: "child", action: "start", turnId: id, announce: false });
    await h.control("turn", { threadId: "child", action: "complete", turnId: id, output: `不属于本运行：${id}`, announce: false });
  }
  const { value, calls } = await terminalTree(h);
  expect(value.tree.nodes).toHaveLength(1);
  expect(value.tree.nodes[0]!.result).toMatchObject({ turnId: bound, output: "本运行的绑定结果", availability: "available" });
  expect(JSON.stringify(value.tree)).not.toContain("不属于本运行");
  const pages = calls.filter((call: any) => call.method === "thread/turns/list" && call.params.threadId === "child");
  expect(pages.length).toBeGreaterThanOrEqual(3);
  expect(pages.every((call: any) => call.params.sortDirection === "desc" && call.params.itemsView === "notLoaded")).toBe(true);
  expect(calls.filter((call: any) => call.method === "thread/items/list" && call.params.threadId === "child")
    .every((call: any) => call.params.turnId === bound)).toBe(true);
});

scenario("正文接口失败仅标子结果不可用", async h => {
  const childTurn = await h.spawn("child");
  await h.finish("child", "服务器原始正文", "completed", childTurn);
  await completeRoot(h);
  await h.control("fault", { method: "thread/items/list", threadId: "child", mode: "error" });
  const { value } = await terminalTree(h);
  expect(value.result).toBe("父运行确认完成");
  expect(value.tree.nodes[0]!.result).toMatchObject({ turnId: childTurn, status: "completed", availability: "unavailable", output: "" });
  expect(value.tree.outputCoverage).toMatchObject({ total: 1, unavailable: 1, available: 0 });
});

scenario("broker 重启后空在线快照仍保留运行记录中的成员", async h => {
  const childTurn = await h.spawn("child");
  await h.finish("child", "重启前结果", "completed", childTurn);
  await completeRoot(h);
  await h.restart();
  const { value } = await terminalTree(h);
  expect(value.result).toBe("父运行确认完成");
  expect(value.tree.nodes).toHaveLength(1);
  expect(value.tree.nodes[0]).toMatchObject({ threadId: "child", parentThreadId: ROOT, turnIds: [childTurn], result: { availability: "unavailable" } });
});

scenario("子提问退出 5 后可 reply，完成后仍交付同一运行", async h => {
  const childTurn = await h.spawn("child");
  await h.owner.request("broker/threads/detach", { threadId: ROOT, runId: h.runId, policy: "on-request" }, 6000);
  await h.control("inject", { message: { id: "tree-question", method: "item/tool/requestUserInput", params: {
    threadId: "child", turnId: childTurn, itemId: "question-item", questions: [{ id: "choice", header: "选择", question: "是否继续？", options: [] }],
  } } });
  let pending: any;
  await h.until(async () => {
    pending = (await h.watch()).pending?.find((item: any) => item.kind === "question" && item.runId === h.runId);
    return !!pending;
  }, 6000, "子提问未进入本运行待答快照");
  const before = (await h.state()).calls.length;
  const waiting = await h.cli(["wait", ROOT, "--tree", "--json", "--timeout", "10"], 15_000);
  expect(waiting.code).toBe(5);
  expect(json(waiting).tree).toMatchObject({ view: "partial", rootThreadId: ROOT });
  expect((await h.state()).calls.slice(before).filter((call: any) => call.method === "thread/items/list" && call.params.threadId === "child")).toEqual([]);
  const replied = await h.cli(["reply", pending.id, "继续", "--json"]);
  expect(replied.code).toBe(0);
  expect(JSON.parse(replied.stdout)).toMatchObject({ accepted: true, kind: "question" });
  await h.until(async () => (await h.state()).threads.child.answers.length === 1, 4000, "回复未交付上游");
  await h.finish("child", "提问后完成", "completed", childTurn);
  await completeRoot(h);
  const { value } = await terminalTree(h);
  expect(value.tree.nodes[0]!.result).toMatchObject({ output: "提问后完成", status: "completed" });
});

scenario("活动子树到期退出 3，沿用 stderr 错误对象且 stdout 留空", async h => {
  await h.spawn("child");
  const waiting = await h.cli(["wait", ROOT, "--tree", "--json", "--timeout", "1"], 6000);
  expect(waiting.code).toBe(3);
  expect(waiting.stdout).toBe("");
  expect(JSON.parse(waiting.stderr)).toMatchObject({ ok: false, error: { code: "TIMEOUT" } });
});
