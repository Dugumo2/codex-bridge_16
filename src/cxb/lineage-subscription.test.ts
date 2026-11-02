/** C 路订阅行为：核实中保留监听，历史候选定案并释放后真正退订，再开工等待重挂确认。 */
import { expect, test } from "bun:test";
import { Sa1bHarness, SA1B_ROOT as ROOT } from "../test-support/sa1b-harness";
import { bounded } from "./wait-run";

const CHILD = "subscription-history-child";
const NEW_CHILD = "subscription-current-child";
const convergence = { timeoutMs: 30_000, discoveryTimeoutMs: 8000, wakeVerifyTimeoutMs: 2000 };

async function ended(h: Sa1bHarness) {
  await h.until(async () => (await h.record())?.state === "ended", 10_000, "运行没有完成释放");
  expect(await h.record()).toMatchObject({ runId: h.runId, status: "completed" });
}

async function unsubscribed(h: Sa1bHarness, since: number) {
  await h.until(async () => {
    const state = await h.state();
    return state.calls.slice(since).some((call: any) => call.method === "thread/unsubscribe" && call.params.threadId === CHILD)
      && !state.subscriptions.broker.includes(CHILD);
  }, 12_000, "历史候选定案、父运行释放后必须实际退订子线程");
}

function start(h: Sa1bHarness, text: string) {
  return h.owner.request("turn/start", { threadId: ROOT, input: [{ type: "text", text }], cxbConvergence: convergence }, 15_000);
}

test("C 历史候选核实期间保留订阅，定案释放后实际退订，再次开工经过重挂屏障", async () => {
  const h = new Sa1bHarness();
  try {
    await h.open();
    await h.start({ cxbConvergence: convergence });
    const oldTurn = await h.spawn(CHILD);
    await h.finish(CHILD, "历史子结果", "completed", oldTurn);
    await h.finish(ROOT, "历史父工作完成");
    expect(await h.reconciliation(8000)).toBeDefined();
    await h.finish(ROOT, "历史汇总完成");
    await ended(h);
    await unsubscribed(h, 0);

    // 同进程新运行仅继承血缘；不重放旧派生、不人为创建本次成员。
    const next: any = await start(h, "核实旧子，本次只派一个新子任务");
    h.runId = next.runId;
    expect(h.runId).toBeTruthy();
    const current = await h.state();
    expect(current.subscriptions.broker).toContain(CHILD);
    h.firstTurn = current.threads.root.turns.at(-1).id;
    const currentTurn = await h.spawn(NEW_CHILD);
    await h.finish(NEW_CHILD, "R2新子结果", "completed", currentTurn);
    await h.control("fence", { name: "candidate-goal", method: "thread/goal/get", threadId: CHILD, direction: "response" });
    const secondStarts = (await h.starts()).length;
    await h.finish(ROOT, "本次新子已完成，等待历史候选定案");
    await h.until(async () => ((await h.state()).held["candidate-goal"] ?? []).length > 0,
      8000, "必须扣住历史候选真实核实请求");
    const checking = await h.state();
    expect(checking.subscriptions.broker, "核实未完成时不得提前退订").toContain(CHILD);
    expect((await h.record()).state).not.toBe("ended");
    expect((await h.watch()).children.members.map((child: any) => child.threadId), "历史候选不进入本次活动账").not.toContain(CHILD);
    const releaseOffset = checking.calls.length;
    await h.control("release", { name: "candidate-goal" });
    await h.until(async () => (await h.starts()).length > secondStarts || (await h.record()).state === "ended",
      10_000, "候选定案后未提交本次新子汇总");
    const secondSummary = JSON.stringify((await h.starts()).at(-1)?.params.input);
    expect(secondSummary).toContain("R2新子结果");
    expect(secondSummary).not.toContain("历史子结果");
    await h.finish(ROOT, "R2新子汇总完成");
    await ended(h);
    expect((await h.record()).children.map((child: any) => child.threadId)).toEqual([NEW_CHILD]);
    await unsubscribed(h, releaseOffset);

    // 扣住重挂响应，确认父 turn/start 在收到确认前没有越过屏障。
    const beforeStart = (await h.state()).calls.length;
    await h.control("fence", { name: "child-reattach", method: "thread/resume", threadId: CHILD, direction: "response" });
    const pending = start(h, "再次开工并合法复用旧子").then(value => ({ value }), error => ({ error }));
    await h.until(async () => ((await h.state()).held["child-reattach"] ?? []).length > 0,
      8000, "再次开工必须向已退订子线程发送真实重挂请求");
    await h.control("barrier");
    const blocked = await h.state();
    expect(blocked.calls.slice(beforeStart).some((call: any) => call.method === "turn/start" && call.params.threadId === ROOT),
      "子线程重挂确认前不得把父工作提交给上游").toBe(false);
    await h.control("release", { name: "child-reattach" });
    const outcome = await pending;
    if ("error" in outcome) throw outcome.error;
    const started = outcome.value as any;
    h.runId = started.runId;
    const resumed = await h.state();
    expect(resumed.subscriptions.broker).toContain(CHILD);
    h.firstTurn = resumed.threads.root.turns.at(-1).id;
    const reused = await h.control("reuse", { parentThreadId: ROOT, parentTurnId: h.firstTurn, childThreadId: CHILD });
    await h.until(async () => (await h.watch()).children.members.some((child: any) => child.threadId === CHILD
      && child.turnIds.includes(reused.turnId)), 8000, "重挂后复用首轮必须能送达并绑定本次运行");
    await h.finish(CHILD, "重挂后的本次结果", "completed", reused.turnId);
    const starts = await h.starts();
    await h.finish(ROOT, "再次父工作完成");
    await h.until(async () => (await h.starts()).length > starts.length || (await h.record()).state === "ended",
      10_000, "重挂后的本次结果没有触发汇总");
    const summary = (await h.starts()).at(-1);
    const summaryText = JSON.stringify(summary?.params.input);
    expect(summaryText).toContain("重挂后的本次结果");
    expect(summaryText).not.toContain("历史子结果");
    await h.finish(ROOT, "再次汇总完成");
    await ended(h);
    const child = (await h.record()).children.find((value: any) => value.threadId === CHILD);
    expect(child.turnIds).toEqual([reused.turnId]);
  } finally {
    await bounded(h.close(), 12_000);
  }
}, 100_000);
