import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChildrenStopReport, RunResult, StallReport, StopDiagnostics, StopReply } from "./contracts";
import { buildContinuePrompt } from "./continue-prompt";
import { ls } from "./ls";
import { show } from "./show";
import { stop } from "./stop";
import { stopStageDescription } from "./stop-diagnostics";
import { bounded, removeTemp } from "./test-fixtures/main-test-support";
import { context, ledger, summary } from "./test-fixtures/n4a-unit";
import { runResult } from "./test-fixtures/run-fakes";
import { upstreamError } from "./test-fixtures/upstream-errors";

let dir: string;
beforeEach(() => { dir = mkdtempSync(join(tmpdir(), "cxb-ue2-presentation-")); });
afterEach(async () => { await removeTemp(dir); }, 10_000);

function stall(patch: Partial<StallReport> = {}): StallReport {
  return { timeoutMs: 600_000, phase: "verifying", expiredAt: 2000, idleMs: 601_500,
    lastProgressAt: 1000, verify: null, expiries: 1, ...patch };
}
function diagnostics(patch: Partial<StopDiagnostics> = {}): StopDiagnostics {
  return { reason: "stallTimeout", startedAt: 2000, goalStartedAt: null, goalSettledAt: null,
    interruptSent: false, interruptSentAt: null, interruptReply: null, interruptRepliedAt: null,
    turnEndedAt: null, unconfirmedAt: 3000, confirmedAt: null, rpcIssues: [], ...patch };
}
const check = { ownership: true, turnEnded: false, goalBraked: false, settled: false, verifyDone: false };
function treeReport(): ChildrenStopReport {
  return { trigger: "userStop", phase: "unconfirmed", version: 1,
    root: { thread: "thread-test", phase: "unconfirmed", check, clearRequested: false, clearConfirmed: false, error: null },
    stopped: [], unconfirmed: ["child-one"], failed: [], conflicts: [], capabilityLimited: [],
    discovery: { complete: true, version: 1, incompleteReasons: [] }, unsettledSpawns: 0 };
}
function stopReply(patch: Partial<StopReply> = {}): StopReply {
  return { threadId: "thread-test", runId: "run-test", interruptSent: false, stopOutcome: "unconfirmed",
    goal: "none", alreadyStopped: false, run: null, ...patch };
}
function stopContext(reply: StopReply, json = false) {
  const t = context(dir, ["stop", "abcd", ...(json ? ["--json"] : [])]);
  t.fake.handle = (method) => method === "broker/threads/list" ? { threads: [summary()] } : reply;
  return t;
}

describe("二批停止阶段说明", () => {
  test("尚未发送优先于失败回包与轮次结束，false 项固定顺序且忽略非布尔值", () => {
    expect(stopStageDescription(diagnostics({ interruptReply: "error" }), {
      verifyDone: false, extra: false, settled: false, goalBraked: false, turnEnded: false, ownership: false,
    })).toBe("尚未发出 interrupt（未满足：ownership、turnEnded、goalBraked、settled、verifyDone）");
    expect(stopStageDescription(diagnostics(), { ownership: null, turnEnded: 0, settled: "false" })).toBe("尚未发出 interrupt");
    expect(stopStageDescription(diagnostics(), null)).toBe("尚未发出 interrupt");
  });
  for (const reply of ["error", "timeout"] as const) test(`${reply} 回包优先于缺少轮次结束证据`, () => {
    expect(stopStageDescription(diagnostics({ interruptSent: true, interruptReply: reply }), check))
      .toBe("interrupt 请求失败或超时");
  });
  test("已发送但无轮次结束证据与其余未满足检查项", () => {
    expect(stopStageDescription(diagnostics({ interruptSent: true, interruptReply: "ok" }), check))
      .toBe("已发出 interrupt，尚未收到轮次结束证据");
    expect(stopStageDescription(diagnostics({ interruptSent: true, interruptReply: "ok" }), { ...check, turnEnded: true }))
      .toBe("未满足：goalBraked、settled、verifyDone");
  });
});

describe("二批 show 展示", () => {
  for (const [phase, label] of [["verifying", "核实中"], ["resumed", "已恢复"], ["stopping", "停止中"],
    ["deferred", "等待轮次归属落定"]] as const) test(`无进展 ${phase} 逐字显示期限与到期时长`, async () => {
    ledger(dir, [runResult({ stall: stall({ phase }) })]);
    const t = context(dir, ["show", "abcd"]);
    expect(await show(t.ctx)).toBe(0);
    expect(t.texts.join("")).toBe(`中文结果\n无进展：期限 600 秒；最近到期 1970-01-01T00:00:02.000Z（已 601 秒无进展）；${label}\n`);
  });
  test("停止诊断未发送时附上未满足项，运行中结果仍返回 5", async () => {
    ledger(dir, [runResult({ state: "stopping", status: null, stopCheck: check, stopDiagnostics: diagnostics() })]);
    const t = context(dir, ["show", "abcd"]);
    expect(await show(t.ctx)).toBe(5);
    expect(t.texts).toEqual(["停止诊断：尚未发出 interrupt（未满足：turnEnded、goalBraked、settled、verifyDone）；开始 1970-01-01T00:00:02.000Z；interrupt 未发出\n"]);
  });
  for (const [reply, label] of [[null, "等待回包"], ["ok", "成功"], ["error", "失败"], ["timeout", "超时"]] as const) {
    test(`停止已发送回包 ${reply} 文本完整，ended 加未确认也显示`, async () => {
      ledger(dir, [runResult({ stopOutcome: "unconfirmed", stopCheck: check,
        stopDiagnostics: diagnostics({ interruptSent: true, interruptSentAt: 2500, interruptReply: reply }) })]);
      const t = context(dir, ["show", "abcd"]);
      expect(await show(t.ctx)).toBe(0);
      const stage = reply === "error" || reply === "timeout" ? "interrupt 请求失败或超时" : "已发出 interrupt，尚未收到轮次结束证据";
      expect(t.texts.join("")).toBe(`中文结果\n停止诊断：${stage}；开始 1970-01-01T00:00:02.000Z；interrupt 已发出 1970-01-01T00:00:02.500Z（${label}）\n`);
    });
  }
  test("已确认停止诊断不加文本行，JSON 仍带完整诊断", async () => {
    const run = runResult({ stopOutcome: "confirmed", stopDiagnostics: diagnostics() });
    ledger(dir, [run]);
    const text = context(dir, ["show", "abcd"]);
    await show(text.ctx);
    expect(text.texts).toEqual(["中文结果"]);
    const json = context(dir, ["show", "abcd", "--json"]);
    await show(json.ctx);
    expect(json.json[0].stopDiagnostics).toEqual(run.stopDiagnostics);
  });
  test("子代理最多列五项，状态复用上游诊断规则且不替代父失败原因", async () => {
    const children = Array.from({ length: 7 }, (_, i) => ({ threadId: `child-00${i}-long`, parentThreadId: "thread-test",
      upstreamError: upstreamError({ threadId: `child-00${i}-long`, willRetry: i === 0 ? true : i === 1 ? false : null }) }));
    const run = runResult({ state: "running", status: null, childUpstreamErrors: children });
    ledger(dir, [run]);
    const t = context(dir, ["show", "abcd"]);
    expect(await show(t.ctx)).toBe(5);
    expect(t.texts.join("").split("\n")).toEqual([
      "子代理 child-00 最近上游报错：上游认证失败（HTTP 401）；3 次；仍在重试",
      "子代理 child-00 最近上游报错：上游认证失败（HTTP 401）；3 次；已报告终止错误",
      ...Array(3).fill("子代理 child-00 最近上游报错：上游认证失败（HTTP 401）；3 次；重试状态未知"),
      "另有 2 个子代理有报错", "",
    ]);
    expect(t.notices.join("")).not.toContain("失败原因");
    ledger(dir, [runResult({ childUpstreamErrors: children.slice(0, 1) })]);
    const ended = context(dir, ["show", "abcd"]);
    await show(ended.ctx);
    expect(ended.texts.join("")).toContain("3 次；运行已结束");
  });
  for (const mode of [[], ["--log"], ["--log", "--full"]]) test(`JSON ${mode.join(" ") || "结果"} 追加全部诊断并省略运行策略`, async () => {
    const run = runResult({ state: "stopping", status: null, interruptReason: "stallTimeout", stall: stall(), stopDiagnostics: diagnostics(),
      childUpstreamErrors: [{ threadId: "child-test", parentThreadId: "thread-test", upstreamError: upstreamError() }],
      runPolicy: { stallTimeoutMs: 600_000, upstreamErrorPolicy: "observe" } });
    ledger(dir, [run]);
    const t = context(dir, ["show", "abcd", ...mode, "--json"]);
    expect(await show(t.ctx)).toBe(mode.length ? 0 : 5);
    const value = mode.length ? t.json[0].runs[0] : t.json[0];
    expect(value).toMatchObject({ stall: run.stall, stopDiagnostics: run.stopDiagnostics, childUpstreamErrors: run.childUpstreamErrors,
      interruptReason: "stallTimeout" });
    expect(value).not.toHaveProperty("runPolicy");
  });
  for (const [reason, label] of [["stallTimeout", "无进展超时"], ["upstreamAccessError", "上游访问报错即停"]] as const) {
    test(`show 与续跑提示的 ${reason} 标签`, async () => {
      const from = runResult({ status: "interrupted", interruptReason: reason });
      ledger(dir, [from]);
      const t = context(dir, ["show", "abcd"]);
      await show(t.ctx);
      expect(t.notices.join("\n")).toContain(`已中断（${label}），可继续：cxb run --resume abcd1234 --continue`);
      expect(buildContinuePrompt({ from, chain: { original: from, broken: false, missingRunId: null }, goal: null, supplement: null }))
        .toContain(`上次中断原因：${label}。`);
    });
  }
});

describe("二批 ls 展示", () => {
  for (const [patch, label] of [
    [{ status: "interrupted", interruptReason: "stallTimeout" }, "interrupted（无进展超时；上游 401×3）"],
    [{ status: "interrupted", interruptReason: "upstreamAccessError" }, "interrupted（访问报错即停；上游 401×3）"],
    [{ state: "running", status: null, stall: stall() }, "running（无进展核实中；上游 401×3）"],
  ] as const) test(`STATUS 合并 ${label}`, async () => {
    ledger(dir, [runResult({ ...patch, upstreamError: upstreamError() })]);
    const t = context(dir, ["ls", "--local"]);
    expect(await ls(t.ctx)).toBe(0);
    expect(t.texts.join("")).toContain(`abcd1234 ${label} run-test`);
    expect(t.texts.join("")).not.toContain("）（");
  });
  test("JSON 只新增 stall 和新原因，保留原始 status，省略其他运行诊断", async () => {
    const run = runResult({ status: "interrupted", interruptReason: "stallTimeout", stall: stall(), stopDiagnostics: diagnostics(),
      childUpstreamErrors: [{ threadId: "child", parentThreadId: "thread-test", upstreamError: upstreamError() }] });
    ledger(dir, [run]);
    const t = context(dir, ["ls", "--local", "--json"]);
    await ls(t.ctx);
    expect(t.json[0].items[0]).toMatchObject({ status: "interrupted", interruptReason: "stallTimeout", stall: run.stall });
    for (const key of ["stopDiagnostics", "childUpstreamErrors", "runPolicy"]) expect(t.json[0].items[0]).not.toHaveProperty(key);
  });
  test("旧原因仍不新增 interruptReason，非核实阶段运行中没有新括注", async () => {
    ledger(dir, [runResult({ state: "running", status: null, interruptReason: "stop", stall: stall({ phase: "resumed" }) })]);
    const text = context(dir, ["ls", "--local"]);
    await ls(text.ctx);
    expect(text.texts.join("")).toContain("abcd1234 running run-test");
    const json = context(dir, ["ls", "--local", "--json"]);
    await ls(json.ctx);
    expect(json.json[0].items[0]).not.toHaveProperty("interruptReason");
  });
});

describe("二批 stop 展示", () => {
  for (const tree of [false, true]) test(`${tree ? "整树" : "单线程"} 未确认加诊断行并保留退出 3`, async () => {
    const reply = stopReply({ stopDiagnostics: diagnostics(), run: runResult({ stopCheck: check }), ...(tree ? { childrenStop: treeReport() } : {}) });
    const t = stopContext(reply);
    expect(await stop(t.ctx)).toBe(3);
    expect(t.notices).toContain("停止诊断：尚未发出 interrupt（未满足：turnEnded、goalBraked、settled、verifyDone）");
    expect(t.notices[0]).toContain("尚未确认");
    const json = stopContext(reply, true);
    expect(await stop(json.ctx)).toBe(3);
    expect(json.json[0].stopDiagnostics).toEqual(reply.stopDiagnostics);
    if (tree) expect(json.json[0].childrenStop).toEqual(reply.childrenStop);
  });
  test("整树优先采用根检查项，回包根诊断可独立于 run 出现", async () => {
    const report = treeReport();
    report.root.check = { ...check, turnEnded: true };
    const t = stopContext(stopReply({ stopDiagnostics: diagnostics({ interruptSent: true, interruptReply: "ok" }), childrenStop: report }));
    expect(await stop(t.ctx)).toBe(3);
    expect(t.notices).toContain("停止诊断：未满足：goalBraked、settled、verifyDone");
  });
  test("已确认不显示诊断行，但 JSON 追加且脱敏 RPC 报错", async () => {
    const reply = stopReply({ stopOutcome: "confirmed", stopDiagnostics: diagnostics({ rpcIssues: [
      { method: "thread/goal/get", kind: "error", at: 2000, message: "token=不能泄露" },
    ] }) });
    const text = stopContext(reply);
    expect(await stop(text.ctx)).toBe(0);
    expect(text.notices.join("")).not.toContain("停止诊断");
    const json = stopContext(reply, true);
    await stop(json.ctx);
    expect(json.json[0]).toHaveProperty("stopDiagnostics");
    expect(JSON.stringify(json.json[0])).not.toContain("不能泄露");
  });
});

describe("二批旧输出兼容", () => {
  test("show 无新字段时正文与 JSON 逐字保持旧夹具", async () => {
    ledger(dir);
    const text = context(dir, ["show", "abcd"]);
    expect(await show(text.ctx)).toBe(0);
    expect(text.texts).toEqual(["中文结果"]);
    expect(text.notices).toEqual(["模型：模型（线程索引）；请求强度：-"]);
    const json = context(dir, ["show", "abcd", "--json"]);
    await show(json.ctx);
    expect(JSON.stringify(json.json[0])).toBe(JSON.stringify({ ok: true, command: "show", threadId: "thread-test", shortId: "abcd1234",
      model: "模型", reasoning: null, modelSource: "index", reroute: null, mode: "result", runId: "run-test", state: "ended",
      status: "completed", ready: "ready", result: "中文结果", outputSource: "finalAnswer", interruptReason: null,
      stopOutcome: null, continuedFrom: null, updatedAt: 2000 }));
  });
  for (const mode of [[], ["--log"]]) test(`show ${mode.join(" ") || "未就绪结果"} 旧原因不扩展 interruptReason 条件`, async () => {
    ledger(dir, [runResult({ state: "stopping", status: null, interruptReason: "stop" })]);
    const t = context(dir, ["show", "abcd", ...mode, "--json"]);
    await show(t.ctx);
    const value = mode.length ? t.json[0].runs[0] : t.json[0];
    for (const field of ["stall", "stopDiagnostics", "childUpstreamErrors", "interruptReason", "runPolicy"]) expect(value).not.toHaveProperty(field);
  });
  test("ls 无新字段时文本状态保持旧格式且 JSON 无新增键", async () => {
    ledger(dir);
    const text = context(dir, ["ls", "--local"]);
    expect(await ls(text.ctx)).toBe(0);
    expect(text.texts).toEqual(["SHORT_ID STATUS LAST_RUN UPDATED MODEL REASONING PREVIEW\n",
      "abcd1234 completed run-test 1970-01-01T00:00:02.000Z 模型（线程索引） - 预览\n"]);
    const json = context(dir, ["ls", "--local", "--json"]);
    await ls(json.ctx);
    for (const field of ["stall", "stopDiagnostics", "childUpstreamErrors", "interruptReason", "runPolicy"]) expect(json.json[0].items[0]).not.toHaveProperty(field);
  });
  for (const confirmed of [false, true]) test(`stop 无诊断时${confirmed ? "已确认" : "未确认"}输出逐字保持旧夹具`, async () => {
    const reply = stopReply({ stopOutcome: confirmed ? "confirmed" : "unconfirmed", interruptSent: true });
    const text = stopContext(reply);
    expect(await stop(text.ctx)).toBe(confirmed ? 0 : 3);
    expect(text.texts).toEqual(confirmed ? ["已停止线程 abcd1234。"] : []);
    expect(text.notices).toEqual(confirmed ? [] : ["已发出停止，Codex 尚未确认停下；请先查看清理报告，临时故障可等待后台核实"]);
    const json = stopContext(reply, true);
    await stop(json.ctx);
    expect(JSON.stringify(json.json[0])).toBe(JSON.stringify({ ok: confirmed, command: "stop", threadId: "thread-test", shortId: "abcd1234",
      runId: "run-test", interruptSent: true, stopOutcome: confirmed ? "confirmed" : "unconfirmed", goal: "none", alreadyStopped: false }));
  });
});

describe("二批 show --follow", () => {
  for (const json of [false, true]) test(`stall 与子代理提示只走 stderr，逐条格式保持${json ? " JSON" : "文本"}`, async () => {
    ledger(dir);
    const t = context(dir, ["show", "abcd", "--follow", ...(json ? ["--json"] : [])]);
    let watches = 0;
    const extra = { diagnosticRevision: 1, stall: stall(), childUpstreamErrors: [
      { threadId: "child-12345", parentThreadId: "thread-test", upstreamError: upstreamError({ threadId: "child-12345" }) },
    ] };
    t.fake.handle = (method) => {
      if (method === "broker/threads/list") return { threads: [summary()] };
      if (method === "broker/runs/modelSummaries") return { runs: [] };
      if (method === "broker/threads/watch") {
        if (++watches === 2) {
          t.emit("broker/run/updated", { runId: "run-test", threadId: "thread-test", state: "running", status: null, ...extra });
          t.emit("item/started", { threadId: "thread-test", item: { type: "commandExecution", id: "item-new" } });
          t.emit("broker/run/ended", { run: runResult(extra) });
        }
        return { thread: summary(), run: runResult({ state: "running", status: null }), pending: [] };
      }
      throw new Error(`未预期的方法 ${method}`);
    };
    expect(await bounded(show(t.ctx), 3000, "follow 未结束")).toBe(0);
    expect(t.notices).toContain("[cxb] 已 601 秒未观察到进展（无进展期限 600 秒），正在核实；仍无进展将停止本次运行。");
    expect(t.notices).toContain("[cxb] 子代理 child-12 上游认证失败（HTTP 401），Codex 正在重试；已观察到 3 次。");
    expect(t.texts.join("") + JSON.stringify(t.json)).not.toContain("[cxb]");
    if (json) expect(t.json.at(-1)).toEqual({ terminal: true, status: "completed", runId: "run-test", model: "模型", reasoning: null, modelSource: "index", reroute: null });
    expect(t.fake.subscriptions + t.any.size + t.cleanup.size).toBe(0);
  });
  for (const json of [false, true]) test(`新原因未确认不声称运行结束${json ? " JSON" : "文本"}`, async () => {
    const run = runResult({ state: "stopping", status: null, interruptReason: "stallTimeout", stopOutcome: "unconfirmed", stopDiagnostics: diagnostics() });
    ledger(dir, [run]);
    const t = context(dir, ["show", "abcd", "--follow", ...(json ? ["--json"] : [])]);
    t.fake.handle = (method) => method === "broker/threads/list" ? { threads: [summary()] }
      : method === "broker/runs/modelSummaries" ? { runs: [] } : { thread: summary(), run, pending: [] };
    expect(await bounded(show(t.ctx), 3000, "follow 应立即返回")).toBe(0);
    expect(t.texts.join("") + t.notices.join("\n")).not.toContain("运行结束");
    if (json) expect(t.json.at(-1)).toMatchObject({ terminal: false, status: null, runId: "run-test" });
    expect(t.fake.subscriptions + t.any.size + t.cleanup.size).toBe(0);
  });
});
