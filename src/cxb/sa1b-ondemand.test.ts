/**
 * SA1b 按需启用的事件顺序验收（三路审查 P1）：只有本运行确有子代理派生／跟进时才启用协调；
 * 启用前的 detach、插话、预算、历史子线程都不能让运行卡住或误停；未启用运行的 CLI 输出与 s4-cli 逐字一致。
 * 每个场景独立真 broker + 模拟上游。
 */
import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { join } from "node:path";
import { envWithPathPrefix } from "../test-support/broker-harness";
import { Sa1bHarness, SA1B_ROOT as P } from "../test-support/sa1b-harness";

type H = Sa1bHarness;
const mark = () => `SA1B_${randomUUID().replaceAll("-", "")}`;
const text = (request: any) => (request?.params?.input ?? []).filter((x: any) => x.type === "text").map((x: any) => x.text).join("\n");
/** s4-cli 基准 CLI（未合入 SA1b 的正式分支工作树）。 */
const BASELINE_CLI = process.env.SA1B_BASELINE_CLI ?? "D:/codex-bridge/src/cxb/main.ts";

const terminal = async (h: H, timeout = 8000) => {
  await h.until(async () => !!h.ended().length, timeout, "运行未在期限内结束"); return h.ended().at(-1);
};
function scenario(name: string, body: (h: H) => Promise<void>, timeout = 30000, start = true) {
  test(name, async () => {
    const h = new Sa1bHarness();
    try {
      await h.open({ pageSize: 16 });
      if (start) await h.start({ cxbConvergence: { timeoutMs: 20000, discoveryTimeoutMs: 400, wakeVerifyTimeoutMs: 600 } });
      await body(h);
    } finally { await h.close(); }
  }, timeout);
}
/** 汇总请求必须带随机标记；完成汇总轮后同一运行正常结束。skip 为本运行汇总之前已有的开轮请求数。 */
async function finishSummary(h: H, markers: string[], skip = 1) {
  let request: any;
  const ended = h.ended().length;
  await h.until(async () => { request = (await h.starts()).slice(skip).find(x => x.params.clientUserMessageId); return !!request || h.ended().length > ended; }, 4000, "汇总未发出");
  expect(request, `汇总未发出；结束记录=${JSON.stringify(h.ended().at(-1))}`).toBeDefined();
  for (const marker of markers) expect(text(request)).toContain(marker);
  const turn = (await h.state()).threads.root.turns.at(-1);
  await h.finish(P, "已核实子结果", "completed", turn.id);
  await h.until(async () => h.ended().length > ended, 8000, "运行未在期限内结束");
  const run = h.ended().at(-1);
  expect(run.status).toBe("completed"); expect(run.runId).toBe(h.runId); expect(run.finalTurnId).toBe(turn.id);
  return run;
}
/** 第二次运行：沿用同一父线程开新运行，等它的首轮开始。 */
async function nextRun(h: H) {
  const response: any = await h.owner.request("turn/start", { threadId: P, input: [{ type: "text", text: "第二次运行" }], model: "original-model", effort: "high",
    cxbConvergence: { timeoutMs: 20000, discoveryTimeoutMs: 400, wakeVerifyTimeoutMs: 600 } });
  h.runId = response.runId;
  await h.until(async () => {
    const turns = (await h.state()).threads.root.turns; h.firstTurn = turns.at(-1)?.id ?? "";
    return turns.length >= 2 && (await h.watch()).thread?.state === "running";
  }, 8000, "第二次运行未开始");
}
async function cli(h: H, main: string, args: string[], timeout = 10000) {
  const proc = Bun.spawn([process.execPath, main, ...args, "--dir", h.tempDir], {
    cwd: h.tempDir, stdin: "ignore", stdout: "pipe", stderr: "pipe",
    env: { ...envWithPathPrefix(h.tempDir), CXB_HOME: h.home, CODEX_HOME: join(h.tempDir, "cli-codex-home"), USERPROFILE: h.tempDir, HOME: h.tempDir },
  });
  h.spawnedProcesses.push(proc);
  const timer = setTimeout(() => proc.kill(), timeout);
  try {
    const [stdout, stderr, code] = await Promise.all([new Response(proc.stdout).text(), new Response(proc.stderr).text(), proc.exited]);
    return { code, stdout, stderr };
  } finally { clearTimeout(timer); }
}

scenario("启动→无子代理 detach→派生→父子完成：授权代际有效，汇总后同一运行结束", async h => {
  const detached: any = await h.owner.request("broker/threads/detach", { threadId: P, runId: h.runId, policy: "on-request" });
  expect(detached.outcome).toBe("detached");
  await h.observe();
  expect((await h.record()).subagentJoin, "未派生前不启用协调").toBeUndefined();
  const marker = mark();
  await h.spawn("C"); await h.finish("C", marker); await h.finish(P);
  const run = await finishSummary(h, [marker]);
  expect(run.subagentJoin.reconciledEpoch).toBe(run.subagentJoin.workEpoch);
});

scenario("无子代理时插话撤权→派生→父完成：交 SA1a 停树收尾，不卡在收敛", async h => {
  await h.control("goal", { threadId: P, status: "active" });
  await h.finish(P);
  const auto = (await h.control("turn", { threadId: P, action: "start", turnId: "goal-auto" })).turnId;
  await h.control("item", { threadId: P, turnId: auto, item: { type: "userMessage", id: "插话", content: [{ type: "text", text: "用户插话" }] } });
  await h.observe();
  await h.spawn("C", P, auto);
  await h.observe();
  expect((await h.record()).subagentJoin, "插话撤权后不再启用协调").toBeUndefined();
  await h.control("goal", { threadId: P, status: "complete" });
  await h.finish(P, "父先结束", "completed", auto);
  const run = await terminal(h);
  expect(run.subagentJoin).toBeUndefined();
  expect(run.interruptReason).toBe("subagentsUnfinished");
  expect((await h.starts()).filter(x => x.params.clientUserMessageId)).toHaveLength(0);
});

scenario("无子代理带 tokenBudget 运行超过 20 秒：不被预算核实误停，行为同 s4-cli", async h => {
  await h.control("goal", { threadId: P, status: "active", tokenBudget: 100000, tokensUsed: 10 });
  await Bun.sleep(21000);
  expect(h.ended(), "预算核实不能停掉无子代理运行").toHaveLength(0);
  const record = await h.record();
  expect(record.state).toBe("running"); expect(record.runBudget).toBeUndefined(); expect(record.subagentJoin).toBeUndefined();
  await h.control("goal", { threadId: P, status: "complete", tokenBudget: 100000, tokensUsed: 20 });
  await h.finish(P);
  const run = await terminal(h);
  expect(run.status).toBe("completed"); expect(run.interruptReason).toBeNull();
}, 45000);

scenario("同一父线程 R1 派生、R2 不派生：R2 不启用协调，按原流程结束", async h => {
  const marker = mark();
  await h.spawn("C"); await h.finish("C", marker); await h.finish(P);
  await finishSummary(h, [marker]);
  await nextRun(h);
  await h.finish(P, "第二次运行结果");
  const run = await terminal(h);
  expect(run.status).toBe("completed"); expect(run.subagentJoin).toBeUndefined(); expect(run.finalTurnId).toBeUndefined();
  expect(run.turns.at(-1).output).toBe("第二次运行结果");
});

scenario("新运行跟进旧子线程：按本次跟进绑定，汇总后结束", async h => {
  await h.spawn("C"); await h.finish("C", "旧子结果"); await h.finish(P);
  await finishSummary(h, ["旧子结果"]);
  await nextRun(h);
  const marker = mark();
  await h.control("reuse", { parentThreadId: P, parentTurnId: h.firstTurn, childThreadId: "C" });
  const before = (await h.starts()).length;
  await h.finish("C", marker); await h.finish(P);
  const run = await finishSummary(h, [marker], before);
  expect(run.subagentJoin).toBeDefined();
});

for (const where of ["核实分页", "快轮完成全文"] as const) scenario(`${where}才发现真实输入：明确按归属冲突收尾，wait 能结束`, async h => {
  await h.spawn(); await h.finish("child", mark());
  // 回包丢失、条目通知扣住：汇总已发出但结果未知，真实输入只能从核实读取或完成全文里看到。
  await h.control("startBehavior", { threadId: P, mode: "replyLost" });
  await h.control("fence", { name: "input", method: "item/completed", threadId: P });
  await h.control("fence", { name: "started", method: "turn/started", threadId: P });
  if (where === "核实分页") await h.control("fence", { name: "items", direction: "request", method: "thread/items/list", threadId: P });
  await h.finish(P);
  let request: any;
  await h.until(async () => { request = (await h.starts()).slice(1).find(x => x.params.clientUserMessageId); return !!request; }, 4000, "汇总未发出");
  const carrier = (await h.state()).threads.root.turns.at(-1).id;
  await h.control("item", { threadId: P, turnId: carrier, announce: false, item: { id: "external-item", type: "userMessage", content: [{ type: "text", text: "真实用户修改了任务要求" }] } });
  if (where === "核实分页") {
    await h.control("release", { name: "items" });
    // 撤权后不能借它中断未证明自有的承载轮；该轮自行结束后停止才能确认、运行收尾。
    await h.until(async () => (await h.record()).subagentJoin?.automaticControlRevoked === true, 6000, "分页核实未撤权");
    await h.finish(P, "承载轮结束", "completed", carrier);
  } else {
    await h.control("turn", { threadId: P, turnId: carrier, action: "complete", announce: false });
    const turn = (await h.state()).threads.root.turns.find((x: any) => x.id === carrier);
    await h.control("inject", { message: { method: "turn/completed", params: { threadId: P, turn: { ...turn, itemsView: "full" } } } });
    await h.until(async () => (await h.record()).subagentJoin?.automaticControlRevoked === true, 6000, "完成全文未撤权");
  }
  // 通知只是延迟而非丢失：撤权定案后放行，停止据此确认承载轮已结束。
  await h.control("release", { name: "started" }); await h.control("release", { name: "input" });
  // 结果不明的已发提交保留只读核实直到 20 秒总窗，之后才能确认根线程收尾。
  const run = await terminal(h, 30000).catch(async error => {
    const watch = await h.watch();
    throw new Error(`${error}；记录=${JSON.stringify(watch.run)}；线程=${JSON.stringify(watch.thread)}；调用=${JSON.stringify((await h.state()).calls.slice(-10).map((c: any) => c.method))}`);
  });
  expect(run.subagentJoin.automaticControlRevoked).toBe(true);
  expect(run.subagentJoin.terminalReason).toBe("ownershipConflict");
  expect((await h.starts()).length, "已发提交不重发").toBe(2);
  const waited = await h.cli(["wait", P, "--json"], 15000);
  expect(typeof waited.code).toBe("number");
}, 60000);

scenario("detach 时已发汇总仍在核实：迁到新授权只读核实，不重发、不卡住", async h => {
  await h.control("goal", { threadId: P, status: "active" });
  await h.spawn(); await h.finish(P);
  const goalTurn = (await h.control("turn", { threadId: P, action: "start", turnId: "goal-auto" })).turnId;
  // 扣住回包、条目通知与条目分页：提交已发出但核实无法在 detach 前完成。
  await h.control("startBehavior", { threadId: P, mode: "replyLost" });
  await h.control("fence", { name: "input", method: "item/completed", threadId: P });
  await h.control("fence", { name: "items", direction: "request", method: "thread/items/list", threadId: P });
  const marker = mark();
  await h.finish("child", marker);
  let request: any;
  await h.until(async () => { request = (await h.starts()).slice(1).find(x => x.params.clientUserMessageId); return !!request; }, 4000, "汇总未发出");
  expect(request.method).toBe("turn/steer"); expect(request.params.expectedTurnId).toBe(goalTurn);
  // 目标轮先结束（不再是当前轮），随后 detach：原提交不能再过新提交准入。
  await h.finish(P, `已核实 ${marker}`, "completed", goalTurn);
  await h.observe();
  const detached: any = await h.owner.request("broker/threads/detach", { threadId: P, runId: h.runId, policy: "on-request" });
  expect(detached.outcome).toBe("detached");
  await h.control("release", { name: "items" }); await h.control("release", { name: "input" });
  await h.control("goal", { threadId: P, status: "complete" });
  const run = await terminal(h, 15000);
  expect(run.status).toBe("completed"); expect(run.interruptReason).toBeNull();
  expect((await h.starts()).length, "已发提交只核实不重发").toBe(2);
  expect(run.finalTurnId).toBe(goalTurn);
});

test("未启用协调的普通运行：run/show/ls/wait/show --follow 文本与 s4-cli 逐字一致", async () => {
  const h = new Sa1bHarness();
  try {
    await h.open({ pageSize: 16 });
    const running = cli(h, join(import.meta.dir, "main.ts"), ["run", "普通任务", "--model", "original-model", "--reasoning", "high"], 20000);
    void running.catch(() => {});
    await h.until(async () => { const run = await h.record().catch(() => undefined); if (!run) return false; h.runId = run.runId; h.firstTurn = run.currentTurnId; return !!h.firstTurn; }, 8000, "CLI 初始运行没有开始");
    await h.finish(P, "普通运行结果");
    const run = await running;
    expect(run.code).toBe(0);
    expect(run.stdout).toContain("普通运行结果");
    for (const line of ["运行协调", "收敛期限", "自动父轮", "预算：", "最终结果：", "阶段结果"]) expect(run.stdout + run.stderr).not.toContain(line);
    const record = await h.record();
    for (const key of ["subagentJoin", "runBudget", "autoTurnsStarted", "finalTurnId"]) expect(record).not.toHaveProperty(key);
    for (const args of [["show", P], ["show", P, "--follow"], ["wait", P], ["ls"], ["show", P, "--json"], ["wait", P, "--json"]]) {
      const ours = await cli(h, join(import.meta.dir, "main.ts"), args);
      const base = await cli(h, BASELINE_CLI, args);
      expect({ args, code: ours.code }).toEqual({ args, code: 0 });
      expect({ args, code: ours.code, stdout: ours.stdout }).toEqual({ args, code: base.code, stdout: base.stdout });
    }
  } finally { await h.close(); }
}, 60000);
