/**
 * 契约第 8 节黑盒 A。骨架没有 C1/C2/C3，完整行为仅 CXB_UE2_MERGED=1 时启用。
 * 未确认用例真实等待 40 秒；状态读取始终保持 inProgress，不借假结束证据通过。
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { createStallMock } from "./test-fixtures/stall-upstream";
import { StallHarness, type StallCliResult } from "./test-fixtures/stall-harness";
import { within, type ManagedProcess } from "../test-support/upstream-isolation";
import { startStallModel } from "../test-support/upstream-model-server-stall";
import { FAKE_KEY } from "../test-support/upstream-model-server";
import { request as httpRequest } from "node:http";
import { assertHangRequestBeforeExpiry } from "../test-support/upstream-e2e-stall";

test("四轮挂起证据：请求先到模型服务才认可", () => {
  expect(() => assertHangRequestBeforeExpiry([{ at: 10 }], 20)).not.toThrow();
});
test("四轮挂起证据：没有模型请求时拒绝即使停止已确认", () => {
  expect(() => assertHangRequestBeforeExpiry([], 20)).toThrow("必须已接受请求");
});
test("四轮挂起证据：到期时或到期后才到达的请求不能冒充挂起", () => {
  for (const at of [20, 21]) expect(() => assertHangRequestBeforeExpiry([{ at }], 20)).toThrow("到期之前");
});

const MERGED = process.env.CXB_UE2_MERGED === "1";
const lines = (text: string) => text.split(/\r?\n/u).filter(Boolean);
const json = (result: StallCliResult) => {
  expect(result.stdout.trim().startsWith("{")).toBe(true);
  const value = JSON.parse(result.stdout.trim());
  expect(value && typeof value === "object" && !Array.isArray(value)).toBe(true);
  expect(value.runPolicy).toBeUndefined(); return value;
};
const exact = (result: StallCliResult, line: string) => expect(lines(result.stderr)).toContain(line);
async function done(proc: ManagedProcess, timeout = 20_000) {
  const code = await within(proc.closed, timeout, "黑盒 CLI 总期限"); return { code, ...proc.output() };
}
function scenario(id: string, title: string, body: (h: StallHarness) => Promise<void>, timeout = 35_000, merged = true, shared = false) {
  describe(`${id} ${title}${merged && !MERGED ? "（待合并 C1/C2/C3；设置 CXB_UE2_MERGED=1）" : ""}`, () => {
    let h: StallHarness;
    beforeAll(async () => { if (merged && !MERGED) return; h = new StallHarness(); await h.open(shared); }, 20_000);
    afterAll(async () => { if (h) await h.close(); }, 15_000);
    test.skipIf(merged && !MERGED)("屏幕、退出码、独立 watcher 与落盘契约", async () => body(h), timeout);
  });
}
function snapshots(h: StallHarness) {
  const updates = h.updated(); expect(updates.length).toBeGreaterThan(0);
  const versions = updates.map((event) => event.params.diagnosticRevision);
  expect(versions.every((version, i) => i === 0 || version > versions[i - 1])).toBe(true);
  for (const { params } of updates) {
    for (const key of ["upstreamError", "lastProgressAt", "state", "status", "interruptReason", "stopOutcome", "stopCheck", "stall", "stopDiagnostics", "childUpstreamErrors", "childUpstreamError"]) expect(Object.hasOwn(params, key)).toBe(true);
    expect(Array.isArray(params.childUpstreamErrors)).toBe(true);
  }
  return updates;
}
async function stallResult(h: StallHarness, proc: ManagedProcess, code = 3, verify = "inProgress", timeout = 20_000) {
  const result = await done(proc, timeout); expect(result.code).toBe(code);
  const final = json(result), record = await h.persisted();
  expect(final.interruptReason).toBe("stallTimeout"); expect(record.interruptReason).toBe("stallTimeout");
  expect(record.stall.verify).toBe(verify); expect(record.stopDiagnostics.interruptSent).toBe(true);
  expect(final.stall).toEqual(record.stall); expect(final.stopDiagnostics).toEqual(record.stopDiagnostics);
  const updates = snapshots(h);
  const verifyIndex = updates.findIndex((event) => event.params.stall?.phase === "verifying");
  const stopIndex = updates.findIndex((event) => event.params.stall?.phase === "stopping");
  expect(verifyIndex).toBeGreaterThanOrEqual(0); expect(stopIndex).toBeGreaterThan(verifyIndex);
  const expired = updates[verifyIndex].params.stall;
  exact(result, `[cxb] 已 ${Math.floor(expired.idleMs / 1000)} 秒未观察到进展（无进展期限 3 秒），正在核实；仍无进展将停止本次运行。`);
  const translated: Record<string, string> = { inProgress: "轮次仍在进行", absent: "没有进行中的父轮", unknown: "状态读取失败或超时" };
  exact(result, `[cxb] 无进展核实结束（${translated[verify]}），正在停止本次运行。`);
  if (record.stopOutcome === "confirmed") {
    exact(result, "[cxb] 已因无进展停止本次运行（3 秒未观察到进展）。");
    expect(record.stopDiagnostics.confirmedAt).toBeNumber();
  }
  // 同一阶段不因后续完整快照反复打印。
  for (const prefix of ["[cxb] 已 3 秒未观察到进展", "[cxb] 无进展核实结束", "[cxb] 已因无进展停止本次运行"]) {
    expect(lines(result.stderr).filter((line) => line.startsWith(prefix)).length).toBeLessThanOrEqual(1);
  }
  return { result, final, record, updates };
}

describe("独占模拟上游的证据真实性自测（不依赖 C1/C2/C3）", () => {
  for (const mode of ["replyOnly", "silent"] as const) test(`${mode}：所有读取口保持活动且没有完成通知`, () => {
    const messages: any[] = []; const mock = createStallMock((message) => messages.push(message));
    try {
      mock.handle({ id: 1, method: "turn/start", params: { threadId: "root", input: [] } });
      const turnId = mock.state().threads.root.turns[0].id;
      mock.handle({ id: 2, method: "fixture/stall/mode", params: { mode } });
      mock.handle({ id: 3, method: "turn/interrupt", params: { threadId: "root", turnId } });
      mock.handle({ id: 4, method: "thread/read", params: { threadId: "root", includeTurns: true } });
      mock.handle({ id: 5, method: "thread/turns/list", params: { threadId: "root", itemsView: "full" } });
      mock.handle({ id: 6, method: "thread/items/list", params: { threadId: "root", turnId } });
      expect(messages.find((message) => message.id === 4).result.thread.turns[0].status).toBe("inProgress");
      expect(messages.find((message) => message.id === 5).result.data[0].status).toBe("inProgress");
      expect(messages.find((message) => message.id === 6).result.data.every((item: any) => item.item.type !== "agentMessage")).toBe(true);
      expect(messages.some((message) => message.method === "turn/completed")).toBe(false);
      expect(messages.some((message) => message.id === 3)).toBe(mode === "replyOnly");
    } finally { mock.close(); }
  });
  test("扣住和放行读取不改变真实轮态", () => {
    const messages: any[] = []; const mock = createStallMock((message) => messages.push(message));
    try {
      mock.handle({ id: 1, method: "turn/start", params: { threadId: "root", input: [] } });
      mock.handle({ id: 2, method: "fixture/stall/holdReads" });
      mock.handle({ id: 3, method: "thread/read", params: { threadId: "root", includeTurns: true } });
      expect(messages.some((message) => message.id === 3)).toBe(false); expect(mock.state().heldReads).toHaveLength(1);
      mock.handle({ id: 4, method: "fixture/stall/releaseReads" });
      expect(messages.find((message) => message.id === 3).result.thread.turns[0].status).toBe("inProgress");
    } finally { mock.close(); }
  });
});

describe("第二批本地挂起模型的传输自测（不拉起 Codex）", () => {
  let model: Awaited<ReturnType<typeof startStallModel>>;
  beforeAll(async () => { model = await startStallModel("hang"); }, 10_000);
  afterAll(async () => { await model?.close(); }, 5000);
  test("接受合法本地请求后不发响应头或首字节", async () => {
    let responseSeen = false;
    const req = httpRequest(`${model.baseUrl}/responses`, { method: "POST", headers: { authorization: `Bearer ${FAKE_KEY}` } }, () => { responseSeen = true; });
    req.on("error", () => {}); req.end("{}");
    try {
      const end = Date.now() + 2000;
      while (model.requests.length === 0 && Date.now() < end) await Bun.sleep(10);
      expect(model.requests).toHaveLength(1); await Bun.sleep(200);
      expect(responseSeen).toBe(false); expect(model.requests[0].status).toBeNull(); expect(model.violations).toEqual([]);
    } finally { req.destroy(); }
  }, 5000);
  test("错误凭证立即拒绝，不能把隔离错误伪装成挂起", async () => {
    const response = await fetch(`${model.baseUrl}/responses`, { method: "POST", headers: { authorization: "Bearer wrong-local-key" }, body: "{}", signal: AbortSignal.timeout(2000) });
    expect(response.status).toBe(400); expect(model.violations).toHaveLength(1);
  }, 5000);
});

scenario("S01", "重复重试报错不续命，核实后正常 interrupt", async (h) => {
  const proc = h.run(["--json"]); await h.started(); const started = Date.now();
  await h.control("errors"); const { record } = await stallResult(h, proc);
  expect(record.stopOutcome).toBe("confirmed"); expect(record.upstreamError.count).toBeGreaterThanOrEqual(3);
  expect(record.stall.expiredAt - started).toBeLessThan(6500);
});
for (const mode of ["replyOnly", "silent"] as const) scenario(mode === "replyOnly" ? "S02" : "S03", `${mode}：40 秒后如实未确认`, async (h) => {
  await h.control("mode", { mode }); const proc = h.run(["--json"]); await h.started(); await h.control("errors");
  const { result, final, record } = await stallResult(h, proc, 3, "inProgress", 75_000);
  expect(record.state).toBe("stopping"); expect(record.stopOutcome).toBe("unconfirmed"); expect(final.ok).toBe(false);
  expect(record.stopDiagnostics.unconfirmedAt - record.stopDiagnostics.startedAt).toBeGreaterThanOrEqual(39_000);
  expect(record.stopCheck.turnEnded).toBe(false); expect(record.stopDiagnostics.confirmedAt).toBeNull();
  const detail = mode === "replyOnly" ? "已发出 interrupt，尚未收到轮次结束证据" : "interrupt 请求失败或超时";
  exact(result, `[cxb] 停止尚未确认：${detail}。`);
  expect(result.stderr).not.toContain("运行已结束"); expect(result.stderr).not.toContain("已因无进展停止本次运行");
  const state = await h.state(); expect(state.threads.root.turns.at(-1).status).toBe("inProgress");
}, 85_000);

scenario("S04", "六秒有效增量延长期限，之后静默才停止", async (h) => {
  const proc = h.run(["--json"]); await h.started(); const started = Date.now();
  await h.control("progress", { count: 6, everyMs: 1000 });
  await h.until(async () => (await h.state()).emitted.filter((event: any) => event.method === "item/agentMessage/delta").length === 6, 8000);
  expect((await h.state()).calls.filter((call: any) => call.method === "turn/interrupt")).toHaveLength(0);
  const { record } = await stallResult(h, proc); expect(record.stall.expiredAt - started).toBeGreaterThanOrEqual(8500);
});
scenario("S05", "只有 error 的停止时点不随报错推后", async (h) => {
  const proc = h.run(["--json"]); await h.started(); const started = Date.now(); await h.control("errors", { everyMs: 500 });
  const { record } = await stallResult(h, proc); expect(record.stall.expiredAt - started).toBeLessThan(6000);
  expect(record.upstreamError.count).toBeGreaterThanOrEqual(4);
});
scenario("S06", "审批待答六秒不触发，真实回答重置基准", async (h) => {
  const detached = await done(h.run(["--detach", "--json"])); expect(detached.code).toBe(0); await h.started();
  await h.control("approval");
  let item: any;
  await h.until(async () => { item = (await h.owner.request<any>("broker/inbox/list", {})).items.find((entry: any) => entry.status === "pending"); return !!item; });
  await Bun.sleep(6100); expect((await h.state()).calls.some((call: any) => call.method === "turn/interrupt")).toBe(false);
  const answered = Date.now(); await h.owner.request("broker/inbox/resolve", { id: item.id, answer: { decision: "accept" } });
  const waited = await h.cli(["wait", "root", "--json", "--timeout", "20"], 25_000); expect(waited.code).toBe(3); json(waited);
  const record = await h.persisted(); expect(record.stall.expiredAt - answered).toBeGreaterThanOrEqual(2700); snapshots(h);
}, 40_000);
scenario("S07", "核实读取扣住后出现待答，取消在途核实", async (h) => {
  const detached = await done(h.run(["--detach", "--json"])); expect(detached.code).toBe(0); await h.started();
  await h.control("holdReads"); await h.until(async () => (await h.state()).heldReads.length > 0, 10_000);
  await h.control("approval"); await h.control("releaseReads");
  await Bun.sleep(4000); const record = await h.record();
  expect(record.state).toBe("running"); expect(record.interruptReason).toBeNull();
  expect((await h.state()).calls.some((call: any) => call.method === "turn/interrupt")).toBe(false);
  expect(h.updated().some((event) => event.params.stall?.phase === "resumed" && event.params.stall?.verify === "progressed")).toBe(true);
  await h.control("finish"); const waited = await h.cli(["wait", "root", "--json", "--timeout", "10"]); expect(waited.code).toBe(0); json(waited);
}, 35_000);
scenario("S08", "显式 0 关闭，无输出八秒后正常完成", async (h) => {
  const proc = h.run(["--stall-timeout", "0"]); await h.started(); await h.control("finishAfter", { ms: 8000 });
  const result = await done(proc); expect(result.code).toBe(0); expect(result.stdout.trim()).toBe("本地脚本结果");
  expect(result.stderr).not.toContain("未观察到进展"); const record = await h.persisted();
  expect(record.runPolicy).toEqual({ stallTimeoutMs: 0, upstreamErrorPolicy: "observe" }); expect(record.stall).toBeUndefined();
  expect(record.stopDiagnostics).toBeUndefined(); expect((await h.state()).calls.some((call: any) => call.method === "turn/interrupt")).toBe(false);
}, 25_000, false);
scenario("S09", "detach 后 broker 继续计时，wait 返回 3", async (h) => {
  const detached = await done(h.run(["--detach", "--json"])); expect(detached.code).toBe(0); const initial = json(detached); await h.started();
  const waited = await h.cli(["wait", "root", "--json", "--timeout", "20"], 25_000); expect(waited.code).toBe(3);
  expect(json(waited).runId).toBe(initial.runId); const record = await h.persisted();
  expect(record.interruptReason).toBe("stallTimeout"); expect(record.stopOutcome).toBe("confirmed"); snapshots(h);
});
scenario("S10a", "结构化 401 重试触发 stop-access，退出 4", async (h) => {
  const proc = h.run(["--json", "--upstream-error-policy", "stop-access"]); await h.started(); await h.control("errors");
  const result = await done(proc); expect(result.code).toBe(4); const final = json(result), record = await h.persisted();
  expect(final.interruptReason).toBe("upstreamAccessError"); expect(record.interruptReason).toBe("upstreamAccessError");
  expect(record.upstreamError.willRetry).toBe(true); expect(record.stopOutcome).toBe("confirmed");
  exact(result, "[cxb] 已按 --upstream-error-policy stop-access 停止本次运行：上游认证失败（HTTP 401）。"); snapshots(h);
});
scenario("S10b", "observe 加期限 0 的对照持续重试后仍可完成", async (h) => {
  const proc = h.run(["--json", "--stall-timeout", "0", "--upstream-error-policy", "observe"]); await h.started();
  await h.control("errors"); await h.until(async () => (await h.record()).upstreamError?.count >= 3, 5000);
  expect((await h.state()).calls.some((call: any) => call.method === "turn/interrupt")).toBe(false); await h.control("finish");
  const result = await done(proc); expect(result.code).toBe(0); expect(json(result).result).toBe("本地脚本结果");
  const record = await h.persisted(); expect(record.interruptReason).toBeNull(); expect(record.stall).toBeUndefined();
  expect(result.stderr).not.toContain("已按 --upstream-error-policy"); snapshots(h);
}, 25_000, false);

for (const [mode, code] of [["failed", 7], ["completed", 0], ["interrupted", 3]] as const) scenario("S11", `SA1b 未启用，停止竞态 ${mode} → ${code}`, async (h) => {
  await h.control("mode", { mode }); const proc = h.run(["--json"]); await h.started();
  const result = await done(proc); expect(result.code).toBe(code); const final = json(result), record = await h.persisted();
  expect(final.status).toBe(mode); expect(record.status).toBe(mode);
  expect(record.interruptReason).toBe(mode === "completed" ? null : "stallTimeout");
  if (mode === "failed") expect(final.error.message).toBe("测试失败"); snapshots(h);
});
for (const mode of ["failed", "interrupted"] as const) scenario("S11-SA1b", `受管子树停止竞态 ${mode}`, async (h) => {
  await h.control("mode", { mode }); const proc = h.run(["--json"]); await h.started();
  const turnId = (await h.state()).threads.root.turns[0].id;
  await h.sa1b("spawn", { parentThreadId: "root", parentTurnId: turnId, childThreadId: "child" });
  const result = await done(proc, 35_000); expect(result.code).toBe(mode === "failed" ? 7 : 3);
  const final = json(result), record = await h.persisted(); expect(final.interruptReason).toBe("stallTimeout");
  expect(record.subagentJoin).toBeDefined(); expect(record.childrenStop).toBeDefined(); snapshots(h);
}, 50_000);
scenario("S12", "Converging 的父轮已结束，不读历史父轮并以 absent 停树", async (h) => {
  const proc = h.run(["--json"]); await h.started(); const turnId = (await h.state()).threads.root.turns[0].id;
  await h.sa1b("spawn", { parentThreadId: "root", parentTurnId: turnId, childThreadId: "silent-child" });
  await h.control("finish");
  await h.until(async () => (await h.record()).subagentJoin?.phase === "waitingSubtree", 5000, "父完成后未进入 Converging");
  const result = await done(proc, 35_000); expect(result.code).toBe(3);
  const final = json(result), record = await h.persisted();
  expect(final.status).toBe("interrupted"); expect(record.interruptReason).toBe("stallTimeout"); expect(record.stall.verify).toBe("absent");
  expect(final.stall).toEqual(record.stall); expect(final.stopDiagnostics).toEqual(record.stopDiagnostics);
  // 无活动父轮会在同一输入中直接进入 stopping；根已结束也不要求给根再发 interrupt。
  expect(snapshots(h).some((event) => event.params.stall?.phase === "stopping" && event.params.stall?.verify === "absent")).toBe(true);
  exact(result, "[cxb] 无进展核实结束（没有进行中的父轮），正在停止本次运行。");
  expect(record.childrenStop).toBeDefined(); expect(record.subagentJoin.terminalReason).toBe("stallTimeout");
  expect((await h.state()).threads["silent-child"].turns.at(-1).status).toBe("interrupted");
}, 50_000);
scenario("S13", "共享服务器 joined 运行不取得停止权", async (h) => {
  const proc = h.run(["--json"]); await h.started();
  await h.until(async () => (await h.record()).joined === true, 8000, "共享场景没有 joined 定案");
  await Bun.sleep(6500); const record = await h.record();
  expect(record.stall).toBeUndefined(); expect(record.stopDiagnostics).toBeUndefined();
  expect((await h.state()).calls.some((call: any) => call.method === "turn/interrupt")).toBe(false);
  await h.control("finish"); const result = await done(proc); expect(result.code).toBe(0); expect(json(result).status).toBe("completed"); await h.persisted();
}, 35_000, true, true);
