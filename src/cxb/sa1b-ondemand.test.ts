/**
 * SA1b 按需启用的事件顺序验收（三路审查 P1）：只有本运行确有子代理派生／跟进时才启用协调；
 * 启用前的 detach、插话、预算、历史子线程都不能让运行卡住或误停；未启用运行的 CLI 输出与 s4-cli 逐字一致。
 * 每个场景独立真 broker + 模拟上游。
 */
import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { envWithPathPrefix, writeMockCodexScript } from "../test-support/broker-harness";
import { Sa1bHarness, SA1B_ROOT as P } from "../test-support/sa1b-harness";
import { readRunLog } from "./ledger";
import { resolveWorkspace } from "./workspace";

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

/** 发现真实输入后必须当场锁定归属冲突并收尾，不能只撤权、拖到 20 秒核实窗到期才由别的路径结束。 */
async function locked(h: H, where: string) {
  await h.until(async () => (await h.record()).subagentJoin?.terminalReason === "ownershipConflict", 5000, `${where}发现真实输入后未立即锁定归属冲突`);
}
for (const where of ["核实分页", "快轮完成全文"] as const) for (const real of [true, false]) scenario(`${where}${real ? "才发现真实输入" : "删除真实插话"}：路径与终态可核验`, async h => {
  await h.start({ cxbConvergence: { timeoutMs: 30000, discoveryTimeoutMs: 400, wakeVerifyTimeoutMs: 10000 } });
  await h.spawn(); await h.finish("child", mark());
  await h.control("startBehavior", { threadId: P, mode: "replyLost" });
  // 先扣提交请求，保证父轮的结果补读不受后面的分页隔离影响。
  await h.control("fence", { name: "submit", direction: "request", method: "turn/start", threadId: P });
  await h.finish(P);
  await h.until(async () => !!(await h.state()).requestHeld.submit?.some((x: any) => x.params.clientUserMessageId), 4000, "汇总请求未进入栅栏");
  await h.control("fence", { name: "input", method: "item/completed", threadId: P });
  await h.control("fence", { name: "items", direction: "request", method: "thread/items/list", threadId: P });
  await h.control("release", { name: "submit" });
  const carrier = (await h.state()).threads.root.turns.at(-1).id;
  await h.until(async () => h.notes("turn/started").some(x => x.threadId === P && x.turn.id === carrier), 4000, "承载轮 started 未被 broker 转发");
  await h.observe();
  expect(carrier).not.toBe(h.firstTurn);
  expect((await h.record()).subagentJoin?.terminalReason).toBeUndefined();
  const waiting = h.cli(["wait", P, "--json"], 35000);
  void waiting.catch(() => {});
  await h.observe(200);
  expect(h.ended()).toHaveLength(0);
  // 负向对照只删去这一条真实插话，合成输入、started 和两条读取路径均保持相同。
  if (real) await h.control("item", { threadId: P, turnId: carrier, announce: false,
    item: { id: "external-item", type: "userMessage", content: [{ type: "text", text: "真实用户修改了任务要求" }] } });
  if (where === "核实分页") {
    const released = await h.control("release", { name: "items" });
    expect(released.released).toBeGreaterThan(0);
    if (real) await locked(h, "分页核实");
    else await h.until(async () => (await h.record()).currentTurnId === carrier, 4000, "无插话的承载轮未确认为自有");
    expect((await h.state()).threads.root.turns.at(-1).status).toBe("inProgress");
    await h.finish(P, "承载轮结束", "completed", carrier);
  } else {
    await h.control("turn", { threadId: P, turnId: carrier, action: "complete", output: "承载轮结束", announce: false });
    const turn = (await h.state()).threads.root.turns.find((x: any) => x.id === carrier);
    await h.control("inject", { message: { method: "turn/completed", params: { threadId: P, turn: { ...turn, itemsView: "full" } } } });
    if (real) await locked(h, "完成全文");
    else {
      await h.observe();
      expect((await h.record()).subagentJoin?.automaticControlRevoked).not.toBe(true);
      expect((await h.record()).subagentJoin?.terminalReason).toBeUndefined();
    }
    // 归属冲突已经断言之后才能释放分页，防止分页抢先给全文项兜底。
    await h.control("release", { name: "items" });
  }
  const run = await terminal(h, 30000);
  expect(run.status).toBe(real ? "interrupted" : "completed");
  expect(run.subagentJoin.terminalReason).toBe(real ? "ownershipConflict" : undefined);
  expect(run.subagentJoin.automaticControlRevoked ?? false).toBe(real);
  if (real) expect(run.childrenStop.trigger).toBe("ownershipConflict");
  expect((await h.starts()).length, "已发提交不重发").toBe(2);
  const waited = await waiting;
  expect(waited.code).toBe(real ? 4 : 0);
  expect(JSON.parse(waited.stdout)).toMatchObject({ runId: h.runId, status: real ? "interrupted" : "completed", alreadyDone: false });
}, 60000, false);

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

/** 只归一化两个独立运行必然不同的标识、工作区路径、时间；其余字节（含字段及提示）原样比较。 */
function normalizeCli(value: string, h: H, run: any) {
  for (const [from, to] of [[h.tempDir, "<工作区>"], [h.tempDir.replaceAll("\\", "/"), "<工作区>"],
    [JSON.stringify(h.tempDir).slice(1, -1), "<工作区>"], [run.runId, "<运行>"], [run.shortId, "<短ID>"]]) {
    if (from) value = value.replaceAll(from, to);
  }
  return value.replace(/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z/g, "<时间>")
    .replace(/("(?:updatedAt|startedAt|endedAt|lastVerifiedAt)":\s*)\d+/g, '$1"<时间>"')
    .replace(/(耗时 |等待耗时 )\d+ 毫秒/g, "$1<耗时> 毫秒");
}
/** 仅为内联审查补上协议回包，复用原模拟器的轮次、停止及分页，不修改 R 路夹具。 */
class BaselineHarness extends Sa1bHarness {
  override async open(options: { pageSize?: number } = {}) {
    const module = pathToFileURL(join(import.meta.dir, "../test-support/sa1b-mock.ts")).href;
    writeMockCodexScript(this.tempDir, `import { createSa1bMock } from ${JSON.stringify(module)};
const reviews = new Map();
const mock = createSa1bMock(message => {
  if (message.id !== undefined && reviews.has(message.id)) {
    if (message.result) message.result.reviewThreadId = reviews.get(message.id);
    reviews.delete(message.id);
  }
  process.stdout.write(JSON.stringify(message) + "\\n");
}, ${JSON.stringify(options)});
let buffer = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", chunk => { buffer += chunk; let end; while ((end = buffer.indexOf("\\n")) >= 0) {
  const line = buffer.slice(0,end); buffer = buffer.slice(end+1); if (!line.trim()) continue;
  const message = JSON.parse(line);
  if (message.method === "review/start") { reviews.set(message.id, message.params.threadId); message.method = "turn/start"; }
  mock.handle(message);
} });
process.stdin.on("end", () => process.exit(0));`);
    await this.launch(); this.owner = await this.connect(); return this;
  }
}
async function baselineSession(main: string, status: "completed" | "interrupted", json: boolean, command: "run" | "review") {
  const h = new BaselineHarness();
  try {
    await h.open({ pageSize: 16 });
    const running = cli(h, main, [command, "普通任务", "--model", "original-model", "--reasoning", "high", ...(json ? ["--json"] : [])], 20000);
    void running.catch(() => {});
    await h.until(async () => { const run = await h.record().catch(() => undefined); if (!run) return false; h.runId = run.runId; h.firstTurn = run.currentTurnId; return !!h.firstTurn; }, 8000, "CLI 初始运行没有开始");
    const workspace = resolveWorkspace(h.tempDir, { ...process.env, CXB_HOME: h.home });
    await h.until(async () => {
      const run = await h.record();
      return !!readRunLog(workspace.workspaceDir, run.shortId)?.some(line => line.runId === h.runId && line.event === "轮次开始");
    }, 5000, "开始日志未落盘，不能比较跟随输出");
    const following = cli(h, main, ["show", P, "--follow", "--json"], 20000);
    const waiting = cli(h, main, ["wait", P, "--json"], 20000);
    await h.observe(350);
    expect(h.ended()).toHaveLength(0);
    if (status === "interrupted") await h.owner.request("broker/threads/stop", { threadId: P, runId: h.runId }, 15000);
    else await h.finish(P, "普通运行结果");
    const run = await h.record();
    expect(run.status).toBe(status);
    expect(run.meta.source).toBe(command);
    if (status === "interrupted") { expect(run.interruptReason).toBe("stop"); expect(run.childrenStop).toBeUndefined(); }
    for (const key of ["subagentJoin", "runBudget", "autoTurnsStarted", "finalTurnId"]) expect(run).not.toHaveProperty(key);
    const outputs = [{ args: [command, ...(json ? ["--json"] : [])], ...await running },
      { args: ["show", "--follow", "--json", "运行中"], ...await following },
      { args: ["wait", "--json", "运行中"], ...await waiting }];
    expect(outputs[0]!.code).toBe(status === "completed" ? 0 : 4);
    expect(JSON.parse(outputs[2]!.stdout)).toMatchObject({ runId: h.runId, status, alreadyDone: false });
    for (const args of [["show", P], ["show", P, "--follow"], ["wait", P], ["ls"], ["show", P, "--json"],
      ["wait", P, "--json"], ["ls", "--json"], ["show", P, "--follow", "--json"]]) {
      outputs.push({ args, ...await cli(h, main, args) });
    }
    return outputs.map(row => ({ ...row, stdout: normalizeCli(row.stdout, h, run), stderr: normalizeCli(row.stderr, h, run) }));
  } finally { await h.close(); }
}
for (const command of ["run", "review"] as const) for (const status of ["completed", "interrupted"] as const) for (const json of [false, true]) test(`未启用协调的普通${status}运行：${command}${json ? " --json" : ""}/show/ls/wait 双流与 s4-cli 逐字一致`, async () => {
  const base = await baselineSession(BASELINE_CLI, status, json, command);
  const ours = await baselineSession(join(import.meta.dir, "main.ts"), status, json, command);
  expect(ours).toEqual(base);
}, 90000);
