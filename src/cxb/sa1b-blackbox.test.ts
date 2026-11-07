/**
 * 接口第 11 节逐行组合验收。业务断言保持为普通 test；骨架不启用 SA1b，红色结果是交付证据。
 * 不使用 test.failing/skip/接受旧行为来伪造通过。每个场景独立真 broker + 模拟上游。
 */
import { expect, test } from "bun:test";
import { appendFileSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { Sa1bHarness, SA1B_ROOT as P } from "../test-support/sa1b-harness";

type H = Sa1bHarness;
const mark = () => `SA1B_${randomUUID().replaceAll("-", "")}`;
const text = (request: any) => (request?.params?.input ?? []).filter((x: any) => x.type === "text").map((x: any) => x.text).join("\n");
const terminal = async (h: H, timeout = 6000) => {
  await h.until(async () => !!h.ended().length, timeout, "运行未在期限内结束"); return h.ended().at(-1);
};
function scenario(id: string, name: string, body: (h: H) => Promise<void>, timeout = 25000) {
  test(`${id} ${name}`, async () => {
    const h = new Sa1bHarness(); let setup = false, error: unknown;
    try {
      await h.open({ pageSize: 16 });
      await h.start({ cxbConvergence: { timeoutMs: ["E05", "E06", "E19"].includes(id) ? 1500 : id === "E09" ? 180000 : 20000, discoveryTimeoutMs: id === "E09" ? 20000 : 400, wakeVerifyTimeoutMs: name.startsWith("超大批次") ? 20000 : 600 } });
      setup = true; await body(h);
    }
    catch (e) { error = e; throw e; }
    finally {
      if (process.env.SA1B_REPORT) {
        let observation: any;
        try { observation = { run: await h.record(), starts: (await h.starts()).length }; } catch { observation = { unavailable: true }; }
        appendFileSync(process.env.SA1B_REPORT, JSON.stringify({ id, name, setup, result: error ? "fail" : "pass", error: error ? String(error) : undefined, observation }) + "\n");
      }
      await h.close();
    }
  }, timeout);
}
async function held(h: H) {
  await h.observe();
  expect(h.ended(), "父结束不能提前 ended").toHaveLength(0);
  const run = await h.record(); expect(run.state).toBe("running"); expect(run.subagentJoin, "A 收敛门禁应已接线").toBeDefined();
}
async function summary(h: H, markers: string[] = [], timeout = 2000) {
  const request = await h.reconciliation(timeout);
  expect(request, "B 汇总请求应含唯一 clientUserMessageId；骨架 Disabled 不发请求").toBeDefined();
  expect(request.params.clientUserMessageId).toBeString();
  for (const marker of markers) expect(text(request)).toContain(marker);
  const state = await h.state(), turn = state.threads.root.turns.at(-1);
  const item = turn.items.find((x: any) => x.type === "userMessage" && x.clientId === request.params.clientUserMessageId);
  expect(item).toBeDefined(); expect(item.id).not.toBe(item.clientId);
  return { request, turn };
}
async function finishSummary(h: H, markers: string[] = []) {
  const { request, turn } = await summary(h, markers);
  await h.finish(P, markers.join(" ") || "已核实子结果", "completed", turn.id);
  const run = await terminal(h); expect(run.status).toBe("completed"); expect(run.runId).toBe(h.runId);
  expect(run.finalTurnId).toBe(turn.id); expect(run.subagentJoin.reconciledEpoch).toBe(run.subagentJoin.workEpoch);
  expect(run.subagentJoin.deliveredEpoch).toBeGreaterThanOrEqual(run.subagentJoin.reconciledEpoch);
  expect(h.ended()).toHaveLength(1);
  return { run, request, turn };
}

for (const version of ["v1", "v2"] as const) {
  for (const order of ["父完全不等", "父只等部分", "子先完成"] as const) scenario("E01", `${version} ${order} 同运行交付`, async h => {
    const marker = mark(); await h.spawn("C", P, h.firstTurn, { version });
    if (order === "父只等部分") { await h.spawn("D", P, h.firstTurn, { version }); await h.finish("D", "快子结果"); }
    if (order === "子先完成") await h.finish("C", marker);
    await h.finish(P);
    if (order !== "子先完成") { await held(h); await h.finish("C", marker); }
    await finishSummary(h, [marker]); expect((await h.starts()).length).toBe(2);
  });
  for (const shape of ["孙代理晚完成", "线程复用", "新派生"] as const) scenario("E02", `${version} ${shape} 不抵消新工作`, async h => {
    const marker = mark(); const c = await h.spawn("C", P, h.firstTurn, { version });
    if (shape === "孙代理晚完成") await h.spawn("G", "C", c, { version });
    await h.finish("C", "旧子输出");
    if (shape === "线程复用") await h.control("reuse", { parentThreadId: P, parentTurnId: h.firstTurn, childThreadId: "C" });
    if (shape === "新派生") await h.spawn("N", P, h.firstTurn, { version });
    await h.finish(P); await held(h);
    const target = shape === "孙代理晚完成" ? "G" : shape === "线程复用" ? "C" : "N";
    await h.finish(target, marker); const { request } = await finishSummary(h, [marker]); expect(text(request)).toContain(target);
  });
}
scenario("E03", "重复事件、发现及重复游标不重复开轮计费", async h => {
  await h.spawn(); const marker = mark(); await h.finish("child", marker);
  await h.control("replay", { method: "turn/completed", threadId: "child", count: 1 });
  await h.control("usage", { threadId: "child", inputTokens: 100, cachedInputTokens: 40, outputTokens: 10 });
  await h.control("replay", { method: "thread/tokenUsage/updated", threadId: "child" });
  await h.finish(P); await summary(h, [marker]);
  const before = await h.record();
  await h.control("replay", { method: "turn/completed", threadId: "child" });
  const state = await h.state();
  for (const method of ["thread/list", "thread/items/list"]) {
    const call = state.calls.findLast((x: any) => x.method === method);
    const response = call && state.events.findLast((x: any) => x.id === call.id && x.result?.data);
    if (response) { await h.control("inject", { message: response }); await h.control("inject", { message: response }); }
  }
  await h.observe(); expect((await h.record()).subagentJoin.workEpoch).toBe(before.subagentJoin.workEpoch);
  const { run } = await finishSummary(h, [marker]);
  expect(run.subagentJoin.wakeupsStarted).toBe(1); expect(run.subagentJoin.wakeupAttempts).toBe(1);
  expect(run.autoTurnsStarted).toBe(1);
  expect((await h.starts()).length).toBe(2);
});
for (const status of ["interrupted", "failed", "errored", "shutdown"]) scenario("E04", `子 ${status} 带真实状态汇总`, async h => {
  await h.spawn(); await h.control("delivery", { childThreadId: "child", mode: "dropped" });
  await h.finish("child", "失败或中断的部分输出", status);
  if (status === "shutdown") await h.control("metadata", { threadId: "child", loaded: false });
  await h.finish(P); const { request } = await summary(h);
  expect(text(request)).toContain(status === "errored" ? "failed" : status);
  await finishSummary(h);
});
for (const initial of ["pendingInit", "首轮未出现"]) scenario("E05", `${initial} 有限期限异常而非空树成功`, async h => {
  await h.spawn("C", P, h.firstTurn, initial === "pendingInit" ? { pendingInit: true } : { firstTurn: false });
  await h.finish(P); await h.observe(); expect((await h.record()).subagentJoin, "未落定派生必须进入SA1b协调器").toBeDefined();
  const run = await terminal(h, 48000);
  expect(run.status).not.toBe("completed"); expect(["convergeDeadline", "discoveryIncomplete"]).toContain(run.interruptReason);
  expect(run.childrenStop).toBeDefined(); expect((await h.starts()).length).toBe(1);
}, 60000);
for (const mode of ["error", "duplicateCursor", "notFound", "notLoaded"]) scenario("E06", `${mode} 发现缺口到期如实停止`, async h => {
  await h.spawn();
  await h.control("fault", { method: mode === "error" || mode === "duplicateCursor" ? "thread/list" : "thread/read", threadId: mode.startsWith("not") ? "child" : undefined, mode });
  await h.finish(P); await h.observe(); expect((await h.record()).subagentJoin, "发现失败应由有总期限的SA1b协调器接管").toBeDefined();
  const run = await terminal(h, 48000);
  expect(run.status).not.toBe("completed"); expect(run.interruptReason).toBe("discoveryIncomplete");
  expect(run.childrenStop.discovery).toBeDefined(); expect((await h.starts()).length).toBe(1);
}, 60000);
for (const method of ["item/commandExecution/requestApproval", "item/tool/requestUserInput", "item/tool/call"]) scenario("E07", `${method} 加 detach 不代答不换运行`, async h => {
  const c = await h.spawn();
  await h.control("inject", { message: { id: "pending-question", method, params: { threadId: "child", turnId: c, itemId: "request-item", command: "echo test", cwd: h.tempDir, questions: [{ id: "q", header: "问题", question: "请选择", options: [] }], callId: "call", tool: "test", arguments: {} } } });
  await h.observe(); const before = await h.watch();
  const detached: any = await h.owner.request("broker/threads/detach", { threadId: P, runId: h.runId, policy: "on-request" });
  expect(detached.outcome).toBe("detached"); await h.finish(P); await h.observe();
  const state = await h.state(); expect(state.threads.child.answers).toHaveLength(0);
  expect((await h.record()).runId).toBe(h.runId); expect(h.ended()).toHaveLength(0);
  expect((await h.record()).subagentJoin?.deadlineAt).toBe(before.run?.subagentJoin?.deadlineAt);
  expect((await h.record()).subagentJoin).toBeDefined();
});
for (const mode of ["normal", "delayed", "dropped"]) scenario("E08", `模拟投递 ${mode} 首采样 final 显式含随机标记`, async h => {
  const marker = mark(); await h.spawn(); await h.control("delivery", { childThreadId: "child", mode });
  await h.finish(P); await held(h); await h.finish("child", marker);
  const { run, request } = await finishSummary(h, [marker]);
  expect(text((await h.starts())[0])).not.toContain(marker); expect(text(request)).toContain(marker);
  expect(run.turns.find((t: any) => t.turnId === run.finalTurnId).output).toContain(marker);
});
for (const size of ["多字节8KiB", "总64KiB", "超大批次", "输出不可读"]) scenario("E09", `${size} 有界证据包且不为分页多开轮`, async h => {
  const count = size === "超大批次" ? 180 : size === "总64KiB" ? 12 : 1;
  for (let i = 0; i < count; i++) {
    const turnId = await h.spawn(`C${i}`);
    if (size === "输出不可读") {
      await h.control("turn", { threadId: `C${i}`, action: "complete", announce: false });
      await h.control("inject", { message: { method: "turn/completed", params: { threadId: `C${i}`, turn: { id: turnId, status: "completed", items: [], itemsView: "notLoaded", error: null } } } });
    } else await h.finish(`C${i}`, `${i}：` + "汉字😀".repeat(size === "超大批次" ? 100 : 3000));
  }
  if (size === "输出不可读") await h.control("fault", { method: "thread/items/list", threadId: "C0", mode: "error" });
  // 180 个子代理逐个读结果再打包，很吃时间：调试版 broker 单独跑约 45 秒，3 份并行时更久。这里只给足时间，判定的是证据包的行为，不是速度。
  await h.finish(P); const { request, turn } = await summary(h, [], 120000); expect(Buffer.byteLength(text(request))).toBeLessThanOrEqual(65536);
  expect(text(request)).not.toContain("\uFFFD");
  const before = await h.record(); const coverage = before.subagentJoin.resultCoverage;
  expect(coverage.available + coverage.unavailable + coverage.omitted).toBe(coverage.total);
  if (size === "多字节8KiB") expect(coverage.truncated).toBeGreaterThan(0);
  if (size === "超大批次") expect(coverage.omitted).toBeGreaterThan(0);
  if (size === "输出不可读") expect(coverage.unavailable).toBe(1);
  await h.finish(P, "已核实缺失与截断", "completed", turn.id); const run = await terminal(h, 120000); expect((await h.starts()).length).toBe(2);
  // 期限太短时偶尔确认不了唤醒，运行会走异常收尾、照样结束；这里用产品默认的 20 秒，并确认真的走到了正常结束。
  if (size === "超大批次") expect(run.status).toBe("completed");
}, 300000);
for (const mode of ["replyLost", "transportError", "通知先到"] as const) scenario("E10", `${mode} clientId 核实不重发`, async h => {
  await h.spawn(); await h.finish("child", mark());
  await h.control("startBehavior", { threadId: P, mode: mode === "通知先到" ? "started" : mode });
  await h.finish(P); const { turn } = await summary(h); await h.observe(300);
  expect((await h.starts()).length).toBe(2);
  await h.finish(P, "核实后的答案", "completed", turn.id); const run = await terminal(h);
  expect(run.status).toBe("completed"); expect(run.subagentJoin.startRetriesUsed).toBe(0);
});
scenario("E10u", "已发未知且无法回查到期 wakeUnknown，绝不重发", async h => {
  await h.spawn(); await h.finish("child"); await h.control("startBehavior", { threadId: P, mode: "replyLost" });
  await h.control("fence", { name: "started", method: "turn/started", threadId: P });
  await h.control("fence", { name: "input", method: "item/completed", threadId: P });
  await h.control("fault", { method: "thread/items/list", threadId: P, mode: "error" });
  await h.finish(P); const request = await h.reconciliation(); expect(request).toBeDefined();
  const run = await terminal(h, 65000); expect(run.interruptReason).toBe("wakeUnknown"); expect((await h.starts()).length).toBe(2);
}, 75000);
for (const visibility of ["外部", "未知"]) scenario("E11", `start 并入${visibility}轮不能取得所有权`, async h => {
  await h.spawn(); await h.finish("child");
  // 请求已经交给传输，但上游尚未执行；在该栅栏内让另一客户端开轮。
  await h.control("fence", { name: "start-race", direction: "request", method: "turn/start", threadId: P });
  await h.finish(P); expect(await h.reconciliation()).toBeDefined();
  expect((await h.state()).requestHeld["start-race"]).toHaveLength(1);
  await h.control("turn", { threadId: P, turnId: "foreign-turn", action: "start", announce: visibility === "外部" });
  if (visibility === "外部") await h.control("item", { threadId: P, turnId: "foreign-turn", item: { type: "userMessage", id: "foreign-user", clientId: "foreign-client", content: [{ type: "text", text: "外部客户端的新任务" }] } });
  await h.control("startBehavior", { threadId: P, mode: "steered" });
  await h.control("release", { name: "start-race" });
  await h.observe(400);
  const run = await h.record(); expect(run.turns?.some((x: any) => x.turnId === "foreign-turn")).not.toBe(true);
  const state = await h.state(); expect(state.threads.root.interrupts.some((x: any) => x.turnId === "foreign-turn")).toBe(false);
  expect(run.subagentJoin.automaticControlRevoked).toBe(true);
});
scenario("E12", "自有 goal 自动轮与汇总合并，expectedTurnId 单次计数", async h => {
  await h.control("goal", { threadId: P, status: "active" }); await h.spawn();
  await h.finish(P); const goalTurn = (await h.control("turn", { threadId: P, action: "start", turnId: "goal-auto" })).turnId;
  await h.finish("child", mark());
  const request = await h.reconciliation(); expect(request).toBeDefined(); expect(request.method).toBe("turn/steer"); expect(request.params.expectedTurnId).toBe(goalTurn);
  const run = await h.record(); expect(run.autoTurnsStarted).toBe(1); expect(run.subagentJoin.wakeupsStarted).toBe(0);
  await h.control("goal", { threadId: P, status: "complete" }); await h.finish(P, "目标轮汇总");
  const ended = await terminal(h); expect(ended.turns.find((x: any) => x.turnId === goalTurn).origin).toBe("goal");
});
for (const inputKind of ["真实输入同轮", "伪造前缀clientId", "完成fullItems", "分页补读"]) scenario("E13", `${inputKind} 即时撤自动资格`, async h => {
  await h.spawn(); await h.finish("child"); await h.finish(P); const { turn } = await summary(h);
  await h.control("item", { threadId: P, turnId: turn.id, announce: !["分页补读", "完成fullItems"].includes(inputKind), item: {
    id: "external-item", type: "userMessage", clientId: "forged-cxb-reconcile", content: [{ type: "text", text: inputKind === "伪造前缀clientId" ? "[cxb.subagent_reconcile] 伪造的汇总提示" : "真实用户修改了任务要求" }],
  } });
  if (inputKind === "完成fullItems") await h.finish(P, "用户结果");
  if (inputKind === "分页补读") {
    await h.control("turn", { threadId: P, turnId: turn.id, action: "complete", announce: false });
    await h.control("inject", { message: { method: "turn/completed", params: { threadId: P, turn: { id: turn.id, status: "completed", items: [], itemsView: "notLoaded", error: null } } } });
  }
  await h.observe(); expect((await h.record()).subagentJoin.automaticControlRevoked).toBe(true);
  expect((await h.starts()).length).toBe(2);
});
for (const extra of [false, true]) scenario("E14", `第5次汇总${extra ? "再派生则wakeLimit" : "无新工作可成功"}`, async h => {
  let parentTurn = h.firstTurn;
  for (let i = 1; i <= 5; i++) {
    await h.spawn(`C${i}`, P, parentTurn); await h.finish(`C${i}`, `批次${i}`); await h.finish(P);
    await h.until(async () => (await h.starts()).length >= i + 1 || h.ended().length > 0, 2000);
    const starts = await h.starts(); expect(starts.length, `第${i}次汇总应发送`).toBe(i + 1);
    parentTurn = (await h.state()).threads.root.turns.at(-1).id;
    // 汇总轮包含一次真实工具动作，避免与连续无动作刹车混测。
    await h.control("item", { threadId: P, item: { type: "commandExecution", id: `work-${i}`, status: "completed", command: "echo 核验", cwd: h.tempDir, exitCode: 0 } });
  }
  if (extra) { await h.spawn("C6", P, parentTurn); await h.finish("C6", "新工作"); }
  await h.finish(P, "最终答案"); const run = await terminal(h);
  expect(run.subagentJoin.wakeupsStarted).toBe(5); expect((await h.starts()).length).toBe(6);
  expect(extra ? run.interruptReason : run.status).toBe(extra ? "wakeLimit" : "completed");
}, 45000);
scenario("E15", "50轮总数不因goal变更或裁剪历史恢复重置，无免费汇总", async h => {
  await h.control("goal", { threadId: P, status: "active" });
  await h.finish(P);
  for (let i = 1; i <= 50; i++) {
    await h.control("turn", { threadId: P, action: "start", turnId: `auto-${i}` });
    await h.control("item", { threadId: P, item: { type: "commandExecution", id: `action-${i}`, status: "completed", command: "echo 核验", cwd: h.tempDir, exitCode: 0 } });
    if (i % 10 === 0) await h.control("goal", { threadId: P, status: "active", objective: `目标修订${i}` });
    if (i === 50) { await h.spawn("late", P, `auto-${i}`); await h.finish("late"); }
    await h.finish(P, `${i}`);
  }
  await h.control("goal", { threadId: P, status: "complete" }); const run = await terminal(h);
  expect(run.autoTurnsStarted).toBe(50); expect(run.interruptReason).toBe("autoTurnLimit");
  expect((await h.starts()).length).toBe(1);
  await h.restart({ trimTurns: 1 });
  const restored = await h.record(); expect(restored.autoTurnsStarted).toBe(50); expect(restored.turns).toHaveLength(1);
}, 45000);
scenario("E16", "连续无动作3轮保留刹车及真实停止报告", async h => {
  await h.control("goal", { threadId: P, status: "active" }); await h.finish(P);
  for (let i = 1; i <= 3; i++) { await h.control("turn", { threadId: P, action: "start", turnId: `idle-${i}` }); await h.finish(P, "没有动作"); }
  // 无子代理运行不启用 SA1b（按需启用），保留 s4-cli 的旧刹车与记录形状，不写独立自动轮计数。
  await h.observe(); expect((await h.record()).autoTurnsStarted, "未启用协调不写独立计数").toBeUndefined();
  const run = await terminal(h, 45000); expect(run.interruptReason).toBe("goalLoop"); expect(run.loopGuard.kind).toBe("noAction");
  expect(run.stopOutcome).toBe("confirmed"); expect(run.autoTurnsStarted).toBeUndefined(); expect(run.subagentJoin).toBeUndefined();
  expect((await h.cli(["wait", P, "--json"])).code).toBe(9);
}, 60000);
for (const state of ["active", "complete"]) scenario("E17", `goal ${state} 已含后代用量不双加不重获预算`, async h => {
  await h.control("goal", { threadId: P, status: "active", tokenBudget: 100, tokensUsed: 0 });
  const childTurn = await h.spawn(); await h.control("usage", { threadId: "child", turnId: childTurn, inputTokens: 100, cachedInputTokens: 40, outputTokens: 10 });
  await h.control("goal", { threadId: P, status: state, tokenBudget: 100, tokensUsed: 70 });
  await h.finish("child"); await h.finish(P); const run = await h.record();
  expect(run.runBudget).toBeDefined(); expect(run.runBudget.tokenBudget).toBe(100);
  // 若无法证明覆盖，可保守 incomplete；不得简单相加140或未核实放行。
  if (run.runBudget.coverage === "complete") expect(run.runBudget.tokensCharged + run.runBudget.usedAtStart).toBe(70);
  else { expect(run.runBudget.coverage).toBe("incomplete"); expect((await h.starts()).length).toBe(1); }
  await h.control("goal", { threadId: P, status: "complete", tokenBudget: 100, tokensUsed: 100 });
  await h.observe(); expect((await h.record()).runBudget.tokenBudget).toBe(100); expect((await h.starts()).length).toBeLessThanOrEqual(2);
});
scenario("E18", "预算证据缺失且核实失败，budgetIncomplete且无新汇总", async h => {
  await h.control("goal", { threadId: P, status: "active", tokenBudget: 100 }); await h.spawn();
  await h.finish("child"); await h.control("goal", { threadId: P, status: "complete", tokenBudget: 100 });
  await h.control("fault", { method: "thread/goal/get", mode: "error" }); await h.finish(P);
  await h.observe(); expect((await h.record()).runBudget, "预算缺失不能被省略为无预算运行").toBeDefined();
  const run = await terminal(h, 65000); expect(run.interruptReason).toBe("budgetIncomplete"); expect((await h.starts()).length).toBe(1);
}, 75000);
for (const phase of ["未发", "已发未知"]) for (const reason of ["stop", "ownerGone", "期限", "预算"]) scenario("E19", `${reason} 与${phase}交错不复活`, async h => {
  await h.spawn();
  await h.owner.request("fixture/backgroundTerminals/set", { threadId: "child", scenario: { sessions: [{ processId: "123", itemId: "background-child" }] } });
  await h.control("item", { threadId: "child", item: { type: "commandExecution", id: "background-child", status: "inProgress", command: "后台核验", cwd: h.tempDir } });
  if (phase === "已发未知") {
    await h.finish("child"); await h.control("startBehavior", { threadId: P, mode: "replyLost" });
    await h.control("fence", { name: "unknown-start", method: "turn/started", threadId: P });
    await h.control("fence", { name: "unknown-input", method: "item/completed", threadId: P });
    await h.control("fault", { method: "thread/items/list", threadId: P, mode: "error" });
  }
  await h.finish(P);
  if (phase === "已发未知") await summary(h); else await held(h);
  const observer = await h.connect();
  if (reason === "stop") await observer.request("broker/threads/stop", { threadId: P }, 45000);
  if (reason === "ownerGone") { await h.owner.close(); h.owner = observer; }
  if (reason === "预算") await h.control("goal", { threadId: P, status: "budgetLimited", tokenBudget: 100, tokensUsed: 100 });
  if (reason === "期限") await h.observe(1800);
  await h.control("release", { name: "reply:root" }); await h.observe(200);
  await h.control("release", { name: "unknown-start" }); await h.control("release", { name: "unknown-input" });
  await h.control("goal", { threadId: P, status: "active" }); await h.observe(200);
  expect((await h.starts()).length).toBe(phase === "已发未知" ? 2 : 1); expect((await h.record()).subagentJoin.automaticControlRevoked).toBe(true);
  // 根输入回查仍被持续故障阻断，迟到 started/成功回包不能单独授予自有停止权。
  if (phase === "已发未知") expect((await h.state()).threads.root.interrupts).toHaveLength(0);
  await h.until(async () => ((await h.owner.request("fixture/backgroundTerminals/state", {}) as any).threads.child ?? []).length === 0, 45000, "异常收尾未完成子后台命令清理");
  const background: any = await h.owner.request("fixture/backgroundTerminals/state", {});
  expect(background.threads.child).toEqual([]);
  expect(background.calls.some((x: any) => x.method === "thread/backgroundTerminals/terminate" && x.params.processId === "123")).toBe(true);
}, 65000);
for (const submitted of [false, true]) scenario("E20", `abandon ${submitted ? "已发未知" : "未发"} 不清掉未决启动且迟到goal不复活`, async h => {
  await h.spawn();
  if (submitted) {
    await h.finish("child"); await h.control("startBehavior", { threadId: P, mode: "replyLost" });
    await h.control("fence", { name: "unknown-start", method: "turn/started", threadId: P });
    await h.control("fence", { name: "unknown-input", method: "item/completed", threadId: P });
    await h.control("fault", { method: "thread/items/list", threadId: P, mode: "error" });
  }
  await h.finish(P);
  if (submitted) await summary(h); else await held(h);
  await h.control("fence", { name: "keep-stop", direction: "request", method: "turn/interrupt" });
  const stopper = await h.connect();
  const stopping = stopper.request("broker/threads/stop", { threadId: P, runId: h.runId }, 45000).catch(() => undefined);
  await h.until(async () => (await h.record()).state === "stopping", 3000, "abandon需要已有停止协调器");
  const outcome: any = await h.owner.request("broker/threads/stop", { threadId: P, runId: h.runId, abandon: true }, 8000);
  const before = (await h.starts()).length;
  if (submitted) expect(outcome.stopCheck?.settled ?? outcome.settled).not.toBe(true);
  await h.control("release", { name: "reply:root" }); await h.control("goal", { threadId: P, status: "active" }); await h.observe(250);
  expect((await h.starts()).length).toBe(before); expect((await h.record()).subagentJoin.automaticControlRevoked).toBe(true);
  await h.control("release", { name: "keep-stop" });
  await Promise.race([stopping, Bun.sleep(100)]);
});
scenario("E21", "断线重启只恢复lost历史，不恢复自动权", async h => {
  await h.spawn(); await h.owner.request("broker/threads/detach", { threadId: P, runId: h.runId, policy: "on-request" });
  await h.restart(); const run = await h.record(); expect(run.runId).toBe(h.runId); expect(run.state).toBe("lost");
  await h.control("goal", { threadId: P, status: "active" }); await h.observe(150);
  expect((await h.starts()).length).toBe(0); expect((await h.state()).threads.root.interrupts).toHaveLength(0);
});
scenario("E22", "旧补读/完成晚于新汇总不能覆盖finalTurnId与modelSummary", async h => {
  const marker = mark(); await h.spawn(); await h.finish("child", marker); await h.finish(P);
  const { run, turn } = await finishSummary(h, [marker]);
  await h.control("replay", { method: "turn/completed", threadId: P, count: 2 });
  await h.control("item", { threadId: P, turnId: h.firstTurn, item: { type: "agentMessage", id: "late-old", text: "旧输出", phase: "final_answer" } });
  await h.observe(); expect((await h.record()).finalTurnId).toBe(turn.id);
  const response: any = await h.owner.request("broker/runs/modelSummaries", { threadIds: [P] });
  expect(response.runs.find((x: any) => x.runId === run.runId).finalTurnId).toBe(turn.id);
  for (const args of [["wait", P, "--json"], ["show", P, "--json"], ["ls", "--json"]]) {
    const result = await h.cli(args); expect(result.code).toBe(0); expect(result.stdout).toContain(turn.id);
    if (args[0] !== "ls") expect(result.stdout).toContain(marker);
  }
});
test("E22run CLI run 交付同运行最后父轮", async () => {
  const h = new Sa1bHarness(); const marker = mark();
  let running: Promise<{ code: number; stdout: string; stderr: string }> | undefined;
  let earlyExit: { code: number; stdout: string; stderr: string } | undefined;
  let failure: unknown;
  try {
    await h.open(); running = h.cli(["run", "测试派生后最终汇总", "--json", "--model", "original-model", "--reasoning", "high"], 15000);
    // 提前挂拒绝处理，确保前置断言失败后也能回收 CLI。
    void running.catch(() => {});
    void running.then(result => { earlyExit = result; }, () => {});
    await h.until(async () => { if (earlyExit) throw new Error(`CLI提前退出：${JSON.stringify(earlyExit)}；调用=${JSON.stringify((await h.state()).calls.slice(-6))}`); const run = await h.record().catch(() => undefined); if (!run) return false; h.runId = run.runId; h.firstTurn = run.currentTurnId; return !!h.firstTurn; }, 7000, "CLI初始运行没有开始");
    await h.spawn(); await h.finish("child", marker); await h.finish(P); await finishSummary(h, [marker]);
    const result = await running; expect(result.code).toBe(0); expect(result.stdout).toContain(marker);
    expect(result.stdout).toContain(h.runId);
  } catch (error) { failure = error; throw error; }
  finally {
    if (process.env.SA1B_REPORT) appendFileSync(process.env.SA1B_REPORT, JSON.stringify({ id: "E22run", name: "CLI run 交付同运行最后父轮",
      setup: !!h.firstTurn, result: failure ? "fail" : "pass", error: failure ? String(failure) : undefined,
      observation: { run: await h.record().catch(() => null), starts: (await h.starts().catch(() => [])).length } }) + "\n");
    await h.close(); await running?.catch(() => {});
  }
}, 22000);
scenario("E23", "模型强度、creationOrder、startConfirmation沿用原运行", async h => {
  const initial = await h.record(); await h.spawn(); await h.finish("child"); await h.finish(P);
  const { request, run } = await finishSummary(h);
  expect(request.params.model).toBe("original-model"); expect(request.params.effort).toBe("high");
  for (const key of ["runId", "creationOrder", "startedAt", "startConfirmation"]) expect(run[key]).toEqual(initial[key]);
  expect(h.notes("broker/run/started")).toHaveLength(1);
});
for (const withChild of [false, true]) for (const parentStatus of ["failed", "interrupted"]) scenario("E24", `${withChild ? "树提交" : "普通释放"} 父${parentStatus}不被goalBlocked覆盖`, async h => {
  if (withChild) await h.spawn(); await h.finish(P, "失败细节", parentStatus); await h.control("goal", { threadId: P, status: "blocked" });
  const run = await terminal(h); expect(run.status).toBe(parentStatus); expect((await h.starts()).length).toBe(1);
  if (withChild) expect(run.childrenStop).toBeDefined();
});
for (const withChild of [false, true]) for (const [goalStatus, status, reason, terminalReason, code] of [
  ["paused", "interrupted", "goalInactive", "goalPaused", 4],
  ["blocked", "goalBlocked", "goalInactive", "goalBlocked", 9],
  ["budgetLimited", "goalBlocked", "budgetExhausted", "goalBudgetLimited", 9],
  ["usageLimited", "goalBlocked", "goalInactive", "goalUsageLimited", 9],
] as const) scenario("E24g", `${withChild ? "树提交" : "普通释放"} ${goalStatus} 不沿用父completed`, async h => {
  if (withChild) await h.spawn();
  await h.control("goal", { threadId: P, status: goalStatus, tokenBudget: 100, tokensUsed: goalStatus === "budgetLimited" ? 100 : 0 });
  await h.finish(P); const run = await terminal(h);
  if (!withChild) {
    // 用户 2026-10-11 定：没有子代理的运行保持原语义——暂停算正常完成，其余限制状态记 goalBlocked。
    const legacy = goalStatus === "paused" ? "completed" : "goalBlocked";
    expect(run.status).toBe(legacy); expect((await h.starts()).length).toBe(1);
    expect((await h.cli(["wait", P, "--json"])).code).toBe(goalStatus === "paused" ? 0 : 9);
    return;
  }
  expect(run.status).toBe(status); expect(run.interruptReason).toBe(reason);
  expect(run.subagentJoin.terminalReason).toBe(terminalReason); expect((await h.starts()).length).toBe(1);
  expect(run.childrenStop.trigger).toBe(terminalReason);
  expect((await h.cli(["wait", P, "--json"])).code).toBe(code);
});
