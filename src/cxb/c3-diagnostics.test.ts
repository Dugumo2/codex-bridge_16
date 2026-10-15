import { expect, test } from "bun:test";
import type { RunResult } from "./contracts";
import { createDiagnosticStore, diagnosticFields } from "./run-diagnostics";
import { observeUpstreamErrors, upstreamResultFields } from "./upstream-errors";
import { FakeRpc, runResult } from "./test-fixtures/run-fakes";
import { childError, stall, stopDiagnostics } from "./test-fixtures/c3-diagnostics";
import { upstreamError } from "./test-fixtures/upstream-errors";

function observer(initial: string | null = "run-test") {
  const fake = new FakeRpc();
  const lines: string[] = [];
  let id = initial, time = 0;
  const observed = observeUpstreamErrors(fake.rpc, () => id, (line) => lines.push(line), () => time);
  const emit = (diagnosticRevision: number, patch: Partial<RunResult> = {}) => fake.emit("broker/run/updated", {
    runId: "run-test", state: "running", status: null, diagnosticRevision, upstreamError: null, lastProgressAt: null,
    interruptReason: null, stopOutcome: null, stopCheck: null, stall: null, stopDiagnostics: null, childUpstreamErrors: [], ...patch,
  });
  return { fake, lines, observed, emit, setId() { id = "run-test"; observed.refresh(); }, clock(n: number) { time = n; },
    close() { observed.dispose(); expect(fake.notificationErrors).toEqual([]); expect(fake.subscriptions).toBe(0); } };
}

test("无进展三个阶段和结束提示逐字，每阶段只提示一次", () => {
  const o = observer();
  try {
    o.emit(1, { stall: stall() });
    o.emit(2, { stall: stall({ idleMs: 650000 }) });
    o.emit(3, { stall: stall({ phase: "resumed", verify: "progressed" }) });
    o.emit(4, { stall: stall({ phase: "resumed", verify: "turnEnded" }) });
    o.emit(5, { stall: stall({ phase: "deferred" }) });
    o.emit(6, { interruptReason: "stallTimeout", stall: stall({ phase: "stopping", verify: "inProgress" }) });
    o.emit(7, { interruptReason: "stallTimeout", stall: stall({ phase: "stopping", verify: "unknown" }) });
    const ended = runResult({ diagnosticRevision: 7, interruptReason: "stallTimeout", status: "interrupted", stall: stall({ phase: "stopping" }) });
    o.observed.apply(ended); o.observed.apply(ended);
    expect(o.lines).toEqual([
      "[cxb] 已 601 秒未观察到进展（无进展期限 600 秒），正在核实；仍无进展将停止本次运行。",
      "[cxb] 核实时发现新进展或轮次已结束，继续运行。",
      "[cxb] 无进展核实结束（轮次仍在进行），正在停止本次运行。",
      "[cxb] 已因无进展停止本次运行（600 秒未观察到进展）。",
    ]);
  } finally { o.close(); }
});

for (const [verify, text] of [["absent", "没有进行中的父轮"], ["unknown", "状态读取失败或超时"]] as const) test(`停止核实提示逐字：${verify}`, () => {
  const o = observer();
  try { o.emit(1, { stall: stall({ phase: "stopping", verify }) }); expect(o.lines).toEqual([`[cxb] 无进展核实结束（${text}），正在停止本次运行。`]); }
  finally { o.close(); }
});

test("访问即停从父或子取结构化类别，子报错不成为父失败原因", () => {
  const o = observer();
  try {
    o.emit(1, { interruptReason: "upstreamAccessError", childUpstreamErrors: [childError()] });
    o.emit(2, { interruptReason: "upstreamAccessError", childUpstreamErrors: [childError()] });
    expect(o.lines).toEqual([
      "[cxb] 已按 --upstream-error-policy stop-access 停止本次运行：上游认证失败（HTTP 401）。",
      "[cxb] 子代理 019a3f2c 上游认证失败（HTTP 401），Codex 正在重试；已观察到 3 次。",
      "[cxb] 原因：认证已过期",
    ]);
    o.observed.failure(runResult({ status: "failed", childUpstreamErrors: [childError()], turns: [] }));
    expect(o.lines.join("\n")).not.toContain("失败原因");
    expect(upstreamResultFields(runResult({ childUpstreamErrors: [childError()], turns: [] }))).not.toHaveProperty("error");
  } finally { o.close(); }
});

test("停止未确认四种阶段按优先级逐字，阶段说明相同不重复", () => {
  const o = observer();
  const base = { state: "stopping" as const, stopOutcome: "unconfirmed" as const, interruptReason: "stallTimeout" as const };
  try {
    o.emit(1, { ...base, stopDiagnostics: stopDiagnostics({ interruptReply: "error" }), stopCheck: { turnEnded: false, goalBraked: false } });
    o.emit(2, { ...base, stopDiagnostics: stopDiagnostics(), stopCheck: { turnEnded: false, goalBraked: false } });
    o.emit(3, { ...base, stopDiagnostics: stopDiagnostics({ interruptSent: true, interruptReply: "timeout" }), stopCheck: { turnEnded: false } });
    o.emit(4, { ...base, stopDiagnostics: stopDiagnostics({ interruptSent: true, interruptReply: "ok" }), stopCheck: { turnEnded: false, settled: false } });
    o.emit(5, { ...base, stopDiagnostics: stopDiagnostics({ interruptSent: true }), stopCheck: { turnEnded: true, ownership: false, verifyDone: false } });
    expect(o.lines).toEqual([
      "[cxb] 停止尚未确认：尚未发出 interrupt（未满足：turnEnded、goalBraked）。",
      "[cxb] 停止尚未确认：interrupt 请求失败或超时。",
      "[cxb] 停止尚未确认：已发出 interrupt，尚未收到轮次结束证据。",
      "[cxb] 停止尚未确认：未满足：ownership、verifyDone。",
    ]);
  } finally { o.close(); }
});

test("晚接入快照已有 stall 与子报错时各提示一次，通知早于runId且旧快照不倒退", async () => {
  const o = observer(null);
  try {
    o.emit(5, { childUpstreamErrors: [childError("aaaaaaaa-A"), childError("bbbbbbbb-B")], stall: stall() });
    o.setId();
    o.fake.handle = () => ({ run: runResult({ state: "running", status: null, diagnosticRevision: 3, childUpstreamErrors: [childError("aaaaaaaa-A")] }) });
    const snapshot = await o.observed.rpc.request<{ run: RunResult }>("broker/threads/watch");
    expect(snapshot.run.childUpstreamErrors?.map((x) => x.threadId)).toEqual(["aaaaaaaa-A", "bbbbbbbb-B"]);
    expect(o.lines.filter((line) => line.includes("子代理"))).toHaveLength(2);
    expect(o.lines.filter((line) => line.includes("正在核实"))).toHaveLength(1);
  } finally { o.close(); }
  const late = observer();
  try {
    late.fake.handle = () => ({ run: runResult({ state: "running", status: null, diagnosticRevision: 1, stall: stall(), childUpstreamErrors: [childError()] }) });
    await late.observed.rpc.request("broker/threads/watch");
    await late.observed.rpc.request("broker/threads/watch");
    expect(late.lines).toHaveLength(3);
  } finally { late.close(); }
});

test("子线程限频分别计算，完整新数组替换而不保留已淘汰线程", () => {
  const o = observer();
  try {
    o.emit(1, { childUpstreamErrors: [childError("aaaaaaaa-A")] });
    o.emit(2, { childUpstreamErrors: [childError("aaaaaaaa-A", 4), childError("bbbbbbbb-B")] });
    o.clock(30000);
    o.emit(3, { childUpstreamErrors: [childError("bbbbbbbb-B", 5)] });
    expect(o.lines.filter((line) => line.includes("子代理"))).toEqual([
      "[cxb] 子代理 aaaaaaaa 上游认证失败（HTTP 401），Codex 正在重试；已观察到 3 次。",
      "[cxb] 子代理 bbbbbbbb 上游认证失败（HTTP 401），Codex 正在重试；已观察到 3 次。",
      "[cxb] 子代理 bbbbbbbb 上游认证失败（HTTP 401），Codex 正在重试；已观察到 5 次。",
    ]);
    const merged = o.observed.apply(runResult({ diagnosticRevision: 1, state: "running", status: null }));
    expect(merged.childUpstreamErrors).toEqual([childError("bbbbbbbb-B", 5)]);
  } finally { o.close(); }
});

test("完整诊断中的null和空数组清除旧字段，终态独立于诊断去重", () => {
  const store = createDiagnosticStore();
  store.accept({ runId: "run-test", diagnosticRevision: 3, stall: stall(), childUpstreamErrors: [childError()], upstreamError: upstreamError() });
  store.accept({ runId: "run-test", diagnosticRevision: 4, stall: null, childUpstreamErrors: [], upstreamError: null }, true);
  store.accept({ runId: "run-test", diagnosticRevision: 1, state: "ended", status: "failed" });
  const run = store.merge(runResult({ state: "running", status: null, diagnosticRevision: 2, upstreamError: upstreamError() }));
  expect(run.state).toBe("ended"); expect(run.status).toBe("failed"); expect(run.diagnosticRevision).toBe(4);
  expect(run).not.toHaveProperty("stall"); expect(run).not.toHaveProperty("upstreamError"); expect(run.childUpstreamErrors).toEqual([]);
});

test("JSON只增存在的新诊断，runPolicy与空子数组不输出，嵌套再次脱敏", () => {
  expect(diagnosticFields(runResult())).toEqual({});
  expect(upstreamResultFields(runResult({ childUpstreamErrors: [], runPolicy: { stallTimeoutMs: 600000, upstreamErrorPolicy: "observe" } }))).toEqual({});
  const fields = diagnosticFields({ stall: stall(), stopDiagnostics: stopDiagnostics({ rpcIssues: [{ method: "turn/interrupt", kind: "error", at: 1, message: "Bearer secret-value" }] }), childUpstreamErrors: [childError()] });
  expect(Object.keys(fields)).toEqual(["stall", "stopDiagnostics", "childUpstreamErrors"]);
  expect(JSON.stringify(fields)).not.toContain("secret-value");
});
