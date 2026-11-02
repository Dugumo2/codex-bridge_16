/** C 路跨进程契约：服务器历史与 broker 存储分别恢复，不重放旧派生通知。 */
import { expect, test } from "bun:test";
import { appendFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { Sa1bHarness, SA1B_ROOT as ROOT } from "../test-support/sa1b-harness";
import { BROKER_BIN } from "../test-support/broker-harness";
import type { Sa1bSnapshot } from "../test-support/sa1b-mock";
import { bounded } from "./wait-run";

const OLD = ["old-a", "old-b", "old-archived"];
const NEW = ["new-a", "new-b"];
const convergence = { timeoutMs: 20_000, discoveryTimeoutMs: 1200, wakeVerifyTimeoutMs: 1200 };
const ids = (children: any[] | undefined) => (children ?? []).map(child => child.threadId).sort();
const inputText = (request: any) => (request.params.input ?? []).map((item: any) => item.text ?? "").join("\n");

async function ended(h: Sa1bHarness) {
  let run: any;
  await h.until(async () => { run = await h.record(); return run?.runId === h.runId && run.state === "ended"; }, 10_000, "本运行没有完成释放");
  return run;
}

async function completeWithSummary(h: Sa1bHarness, active: string[]) {
  await h.finish(ROOT, "父工作轮结束");
  const summary = await h.reconciliation(8000);
  expect(summary, "旧线程被误作活动会卡住 waitingSubtree；必须收到本次汇总").toBeDefined();
  for (const child of active) expect(inputText(summary)).toContain(`R2结果:${child}`);
  expect(inputText(summary)).not.toContain("R1历史结果:");
  const turn = (await h.state()).threads.root.turns.at(-1);
  await h.finish(ROOT, "R2汇总完成", "completed", turn.id);
  const run = await ended(h);
  expect(run).toMatchObject({ status: "completed", subagentJoin: { phase: "released", wakeupsStarted: 1 } });
  expect(run.subagentJoin.resultCoverage.total).toBe(active.length);
  expect((await h.starts()).length).toBe(2);
  return run;
}

async function history(h: Sa1bHarness, interrupted: boolean, loaded: boolean, gap = false) {
  await h.open({ pageSize: 1, duplicateListIds: [OLD[0]!, NEW[0]!] });
  await h.start({ cxbConvergence: convergence });
  for (const child of OLD) { const turn = await h.spawn(child); await h.finish(child, `R1历史结果:${child}`, "completed", turn); }
  if (interrupted) {
    const stopped: any = await h.owner.request("broker/threads/stop", { threadId: ROOT, runId: h.runId }, 45_000);
    expect(stopped.stopOutcome).toBe("confirmed");
    expect(await ended(h)).toMatchObject({ interruptReason: "stop", status: "interrupted" });
  } else {
    await h.finish(ROOT, "R1父工作完成");
    expect(await h.reconciliation(8000)).toBeDefined();
    await h.finish(ROOT, "R1汇总完成");
    expect(await ended(h)).toMatchObject({ status: "completed", interruptReason: null });
    const refusal = await h.cli(["run", "--resume", ROOT, "--continue", "--detach", "--json"]);
    expect(refusal.code, "正常完成的 R1 不得为了测试放宽 continue 准入").toBe(1);
    expect((await h.record()).runId).toBe(h.runId);
  }
  let expected = { state: "ended", status: interrupted ? "interrupted" : "completed", children: OLD };
  const persisted = await h.persisted(expected);
  expect(persisted.children).toHaveLength(3);
  expect(persisted.children.every((child: any) => child.turnIds.length === 1)).toBe(true);
  if (gap) {
    const next = await h.cli(["run", "--resume", ROOT, "R1本次不使用子代理", "--detach", "--json"]);
    expect(next.code, next.stderr).toBe(0);
    h.runId = JSON.parse(next.stdout).runId;
    await h.finish(ROOT, "R1无子任务完成");
    const noChildren = await ended(h);
    expect(noChildren.status).toBe("completed");
    expect(ids(noChildren.children)).toEqual([]);
    expected = { state: "ended", status: "completed", children: [] };
    await h.persisted(expected);
  }
  const snapshot = await h.control("snapshot") as Sa1bSnapshot;
  for (const child of OLD) { snapshot.threads[child]!.loaded = loaded; snapshot.threads[child]!.goal = null; }
  snapshot.threads[OLD[2]!]!.archived = true;
  const oldRun = h.runId;
  await h.restart({ snapshot, persisted: expected });
  const restored = await h.state();
  expect(restored.events.some((event: any) => event.method?.startsWith("item/") && event.params?.item?.tool === "spawnAgent")).toBe(false);
  expect(restored.threads[OLD[0]!].turns[0].status).toBe("completed");
  return oldRun;
}

for (const scenario of [
  { name: "--resume 三个 notLoaded 历史成员、两个新成员", mode: "new", loaded: false, continuing: false, gap: false },
  { name: "--resume 已加载 completed 历史成员", mode: "new", loaded: true, continuing: false, gap: false },
  { name: "--resume 合法复用历史成员", mode: "reuse", loaded: false, continuing: false, gap: false },
  { name: "--resume 本次完全不用子代理", mode: "none", loaded: false, continuing: false, gap: false },
  { name: "--resume R0 有子而 R1 无子，历史证据穿过启动筛选", mode: "new", loaded: false, continuing: false, gap: true },
  { name: "--continue 真实停止后的符合准入运行", mode: "new", loaded: false, continuing: true, gap: false },
] as const) test(`C 真实恢复：${scenario.name}`, async () => {
  const h = new Sa1bHarness();
  const report: Record<string, any> = { name: scenario.name, broker: BROKER_BIN,
    commit: execFileSync("git", ["rev-parse", "HEAD"], { cwd: import.meta.dir, encoding: "utf8", windowsHide: true }).trim() };
  try {
    report.previousRun = await history(h, scenario.continuing, scenario.loaded, scenario.gap);
    const started = await h.cli(["run", "--resume", ROOT, ...(scenario.continuing ? ["--continue"] : []),
      "R2接着完成", "--detach", "--json", "--converge-timeout", "20"]);
    report.startExitCode = started.code;
    expect(started.code, started.stderr).toBe(0);
    h.runId = JSON.parse(started.stdout).runId;
    expect(h.runId).not.toBe(report.previousRun);
    h.firstTurn = (await h.state()).threads.root.turns.at(-1).id;
    expect((await h.state()).subscriptions.broker).toContain(ROOT);
    expect((await h.record()).continuedFrom).toBe(scenario.continuing ? report.previousRun : null);
    const managed = new Map<string, string>();
    if (scenario.mode !== "none") for (const child of NEW) managed.set(child, await h.spawn(child));
    if (scenario.mode === "reuse") {
      const reused = await h.control("reuse", { parentThreadId: ROOT, parentTurnId: h.firstTurn, childThreadId: OLD[0] });
      managed.set(OLD[0]!, reused.turnId);
    }
    const active = [...managed.keys()].sort();
    await h.until(async () => JSON.stringify(ids((await h.watch()).children?.members)) === JSON.stringify(active), 8000,
      "在线成员必须恰好包含本运行有效派生与复用成员").catch(error => {
        if (!String(error).includes("在线成员必须恰好包含")) throw error;
      });
    expect(ids((await h.watch()).children?.members), "在线成员不能包含历史伪成员，也不能漏掉本次有效复用").toEqual(active);
    for (const [child, turn] of managed) await h.finish(child, `R2结果:${child}`, "completed", turn);
    let run: any;
    if (active.length) run = await completeWithSummary(h, active);
    else { await h.finish(ROOT, "R2无子任务完成"); run = await ended(h); expect(run.status).toBe("completed"); expect(await h.starts()).toHaveLength(1); }
    expect(ids(run.children)).toEqual(active);
    for (const child of run.children ?? []) expect(child.turnIds).toEqual([managed.get(child.threadId)]);
    // 通知即契约；即使最后撤回历史伪成员，也不能让 wait --tree 曾经看见它。
    const changes = h.notes("broker/run/childChanged").filter(event => event.runId === h.runId && event.threadId);
    if (active.length) expect(changes.length).toBeGreaterThan(0);
    for (const event of changes) {
      expect(active).toContain(event.threadId);
    }
    // 模拟服务器后来新增其他运行的完成轮，结果仍必须命中本运行受管轮。
    for (const child of active) {
      const later = await h.control("turn", { threadId: child, action: "start", announce: false });
      await h.control("turn", { threadId: child, turnId: later.turnId, action: "complete", announce: false, output: "其他运行的最新结果" });
    }
    const waiting = await h.cli(["wait", ROOT, "--tree", "--json", "--timeout", "10"], 15_000);
    report.waitExitCode = waiting.code;
    expect(waiting.code, waiting.stderr).toBe(0);
    const result = JSON.parse(waiting.stdout);
    expect(ids(result.tree.nodes)).toEqual(active);
    expect(JSON.stringify(result.tree)).not.toContain("其他运行的最新结果");
    for (const child of result.tree.nodes) expect(child.result).toMatchObject({ turnId: managed.get(child.threadId), output: `R2结果:${child.threadId}` });
    report.runId = h.runId; report.members = ids(run.children); report.summaryCount = (await h.starts()).length - 1;
    report.status = run.status; report.result = "pass";
  } catch (error) {
    report.result = "fail"; report.error = String(error);
    try { report.watch = await h.watch(); report.upstream = await h.state(); } catch { report.observationUnavailable = true; }
    throw error;
  }
  finally {
    if (process.env.CXB_LINEAGE_REPORT) appendFileSync(process.env.CXB_LINEAGE_REPORT, JSON.stringify(report) + "\n");
    await bounded(h.close(), 12_000);
  }
}, 90_000);
