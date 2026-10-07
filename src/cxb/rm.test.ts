import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { rm } from "./rm";
import { removeTemp } from "./test-fixtures/main-test-support";
import { context, ledger, reasonError, summary } from "./test-fixtures/n4a-unit";
import { runResult } from "./test-fixtures/run-fakes";
import { N4Broker } from "./test-fixtures/n4a-broker";
import { readThreadIndex } from "./ledger";
import { until } from "./test-fixtures/main-test-support";

let dir: string;
beforeEach(() => { dir = mkdtempSync(join(tmpdir(), "cxb-n7b-rm-")); });
afterEach(async () => { await removeTemp(dir); }, 10_000);
const stop = (confirmed = true) => ({ threadId: "thread-test", runId: "run-test", interruptSent: true,
  alreadyStopped: false, stopOutcome: confirmed ? "confirmed" : "unconfirmed", goal: "paused", run: null });
const archived = (patch: Record<string, unknown> = {}) => ({ threadId: "thread-test", operationId: "op-1", archived: true, localIndex: "removed", ...patch });
const purged = (patch: Record<string, unknown> = {}) => ({ threadId: "thread-test", operationId: "op-2",
  deleted: ["local-index", "server-thread"], alreadyDeleted: ["local-log"], failed: [], ...patch });
const methods = (t: ReturnType<typeof context>) => t.fake.calls.map((c) => c.method);
const blocked = reasonError("threadBusy", -32001, { blockers: [
  { threadId: "review-child", runId: "run-old", reason: "lostUnconfirmed" },
  { threadId: "thread-test", runId: "run-new", reason: "running" },
  { threadId: "thread-test", runId: null, reason: "guardianPending" },
] });

describe("rm 归档（broker/threads/archive，任务说明 N7-A 2.3、5.5 节）", () => {
  for (const state of ["running", "stopping", "lost"] as const) test(`${state} 先停止确认再归档（lost 从磁盘补读）`, async () => {
    const run = runResult({ state, stopOutcome: "unconfirmed" });
    ledger(dir, [run]);
    const t = context(dir, ["rm", "abcd", "--json"]);
    t.fake.handle = (m) => {
      if (m === "broker/threads/list") return { threads: [summary()] };
      if (m === "broker/threads/watch") return { thread: summary(), run: state === "lost" ? null : run };
      if (m === "broker/threads/stop") return stop();
      return archived();
    };
    expect(await rm(t.ctx)).toBe(0);
    expect(methods(t)).toEqual(["broker/threads/list", "broker/threads/watch", "broker/threads/stop", "broker/threads/archive"]);
    expect(t.fake.calls[3]!.params).toEqual({ threadId: "thread-test" });
    expect(t.fake.calls[3]!.options.timeoutMs).toBe(90_000);
    expect(t.json[0]).toEqual({ ok: true, command: "rm", id: "abcd", threadId: "thread-test", mode: "archive", stopped: true,
      archived: true, localIndex: "removed", operationId: "op-1", recoverable: true });
  });
  test("线程忙即使无运行也先停止；停止未确认不归档并退出 3", async () => {
    const t = context(dir, ["rm", "abcd"]);
    t.fake.handle = (m) => m === "broker/threads/list" ? { threads: [summary()] }
      : m === "broker/threads/watch" ? { thread: summary({ state: "starting", lastRunId: null }), run: null } : stop(false);
    expect(await rm(t.ctx)).toBe(3);
    expect(methods(t)).not.toContain("broker/threads/archive");
    expect(t.notices.join()).toContain("未归档");
  });
  test("已确认 lost 不重复停止；上游已不存在或已归档时如实报告本地索引已清理", async () => {
    const t = context(dir, ["rm", "abcd", "--json"]);
    t.fake.handle = (m) => {
      if (m === "broker/threads/list") return { threads: [summary()] };
      if (m === "broker/threads/watch") return { thread: summary(), run: runResult({ state: "lost", stopOutcome: "confirmed" }) };
      return archived({ archived: "alreadyDone", localIndex: "alreadyAbsent" });
    };
    expect(await rm(t.ctx)).toBe(0);
    expect(methods(t)).not.toContain("broker/threads/stop");
    expect(methods(t)).not.toContain("thread/archive");
    expect(t.json[0]).toMatchObject({ archived: "alreadyDone", localIndex: "alreadyAbsent" });
    expect(t.notices.join()).toContain("本地索引已清理");
  });
  test("仅服务器线程 unknownThread 直接交给 broker 归档，不误停止", async () => {
    const t = context(dir, ["rm", "server-only", "--json"]);
    t.fake.handle = (m) => {
      if (m === "broker/threads/list") return { threads: [] };
      if (m === "broker/threads/watch") throw reasonError("unknownThread");
      return archived({ threadId: "server-only", localIndex: "alreadyAbsent" });
    };
    expect(await rm(t.ctx)).toBe(0);
    expect(methods(t)).toEqual(["broker/threads/list", "broker/threads/watch", "broker/threads/archive"]);
  });
  test("无记录只放行 unknownThread，其他错误不能绕过停止检查", async () => {
    const t = context(dir, ["rm", "server-only"]);
    t.fake.handle = (m) => { if (m === "broker/threads/list") return { threads: [] }; throw reasonError("shuttingDown"); };
    await expect(rm(t.ctx)).rejects.toMatchObject({ exitCode: 6 });
    expect(methods(t)).not.toContain("broker/threads/archive");
  });
  test("有阻塞项：逐条列出真实执行线程、运行与下一步命令，退出 8，不自动停止", async () => {
    const t = context(dir, ["rm", "abcd", "--json"]);
    t.fake.handle = (m) => {
      if (m === "broker/threads/list") return { threads: [summary()] };
      if (m === "broker/threads/watch") return { thread: summary(), run: runResult() };
      throw blocked;
    };
    expect(await rm(t.ctx)).toBe(8);
    const notices = t.notices.join("\n");
    expect(notices).toContain("review-child 运行 run-old：失联且未确认停下；cxb stop review-child --run run-old");
    expect(notices).toContain("运行 run-new：运行中；cxb stop thread-test");
    expect(notices).toContain("有未处理的 gd_ 事项；cxb inbox");
    expect(t.json[0]).toMatchObject({ ok: false, mode: "archive", blockers: [{ threadId: "review-child", runId: "run-old" }, {}, { runId: null }] });
    expect(methods(t).filter((m) => m === "broker/threads/stop")).toHaveLength(0);
  });
  for (const kind of ["archive", "delete"] as const) test(`归档时线程上有${kind === "archive" ? "归档" : "删除"}待核实：退出 8 并提示对应命令`, async () => {
    const t = context(dir, ["rm", "abcd"]);
    t.fake.handle = (m) => {
      if (m === "broker/threads/list") return { threads: [summary()] };
      if (m === "broker/threads/watch") return { thread: summary(), run: runResult() };
      throw reasonError("lifecyclePending", -32001, { kind, phase: "outcomeUnknown", operationId: "op-9", threadId: "thread-test" });
    };
    const error = await rm(t.ctx).then(() => null, (error) => error);
    expect(error).toMatchObject({ exitCode: 8 });
    expect(error.message).toContain(kind === "delete" ? "cxb rm thread-test --purge" : "cxb rm thread-test");
    if (kind === "archive") expect(error.message).not.toContain("--purge");
  });
  test("快照带归档待核实：跳过便利停止，直接进入归档完成核实", async () => {
    const t = context(dir, ["rm", "abcd", "--json"]);
    const lifecycle = { operationId: "op-1", kind: "archive", phase: "outcomeUnknown", shortId: "abcd1234", preview: null };
    t.fake.handle = (m) => {
      if (m === "broker/threads/list") return { threads: [summary({ lifecycle } as any)] };
      if (m === "broker/threads/watch") return { thread: summary({ state: "running", lifecycle } as any), run: runResult({ state: "lost" }) };
      return archived();
    };
    expect(await rm(t.ctx)).toBe(0);
    expect(methods(t)).toEqual(["broker/threads/list", "broker/threads/watch", "broker/threads/archive"]);
    expect(t.json[0]).toMatchObject({ stopped: false, archived: true });
  });
  test("快照带删除待核实：不发归档，退出 8 并提示 --purge", async () => {
    const t = context(dir, ["rm", "abcd"]);
    const lifecycle = { operationId: "op-2", kind: "delete", phase: "localPurging", shortId: "abcd1234", preview: null };
    t.fake.handle = (m) => m === "broker/threads/list" ? { threads: [summary({ lifecycle } as any)] }
      : { thread: summary({ lifecycle } as any), run: null };
    await expect(rm(t.ctx)).rejects.toMatchObject({ exitCode: 8, message: expect.stringContaining("cxb rm abcd --purge") });
    expect(methods(t)).not.toContain("broker/threads/archive");
    expect(methods(t)).not.toContain("broker/threads/stop");
  });
  for (const [label, error, exitCode, text] of [
    ["结果未知可重试", reasonError("outcomeUnknown", -32000, { operationId: "op-1" }), 6, "cxb rm abcd"],
    ["上游明确拒绝附脱敏消息", reasonError("upstreamRejected", -32600, { upstream: { code: -32600, message: "拒绝 token=秘密值" } }), 7, "错误码 -32600"],
    ["上游已完成、本地索引未提交", reasonError("storageTimeout", -32000, { upstreamDone: true }), 6, "服务器已完成"],
    ["存储失败、上游未执行", reasonError("storageError", -32000, { upstreamDone: false }), 6, "未完成"],
  ] as const) test(`归档失败：${label}（退出 ${exitCode}）`, async () => {
    const t = context(dir, ["rm", "abcd"]);
    t.fake.handle = (m) => {
      if (m === "broker/threads/list") return { threads: [summary()] };
      if (m === "broker/threads/watch") return { thread: summary(), run: runResult() };
      throw error;
    };
    const failure = await rm(t.ctx).then(() => null, (error) => error);
    expect(failure).toMatchObject({ exitCode });
    expect(failure.message).toContain(text);
    expect(failure.message).not.toContain("秘密值");
  });
});

// 依赖新 broker（丙路的 broker/threads/archive）：合并前用起点 broker 跑会失败，合并后经真 broker 验证。
describe("rm 真 broker", () => {
  let broker: N4Broker;
  beforeEach(async () => { broker = new N4Broker(); await broker.setup(); }, 25_000);
  afterEach(async () => { await broker.cleanup(); }, 40_000);
  test("归档成功后 broker 删除索引，ls --local 看不到", async () => {
    const created = await broker.request("thread/start", { cwd: broker.workspace.root });
    await until(() => readThreadIndex(broker.workspace.workspaceDir).length > 0, 5000, "初始索引没有落盘");
    const result = await broker.cli(["rm", created.thread.id, "--json"]);
    expect(result.code).toBe(0);
    expect(JSON.parse(result.stdout)).toMatchObject({ archived: true, localIndex: "removed" });
    await until(() => readThreadIndex(broker.workspace.workspaceDir).length === 0, 5000, "归档索引未更新");
    const listed = await broker.cli(["ls", "--local", "--json"]);
    expect(listed.code).toBe(0);
    expect(JSON.parse(listed.stdout).items).toEqual([]);
  }, 30_000);
});

describe("rm 永久删除（purge{serverDelete:true}）", () => {
  for (const [reason, exitCode] of [["stopping", 8], ["purgePending", 8], ["shuttingDown", 6], ["storageError", 6], ["storageTimeout", 6]] as const) {
    test(`${reason} 映射退出 ${exitCode}，只发一次 purge`, async () => {
      const t = context(dir, ["rm", "abcd", "--purge"]);
      t.fake.handle = (m) => {
        if (m === "broker/threads/list") return { threads: [summary()] };
        throw reasonError(reason, reason === "stopping" || reason === "purgePending" ? -32001 : -32000);
      };
      await expect(rm(t.ctx)).rejects.toMatchObject({ exitCode });
      expect(methods(t)).toEqual(["broker/threads/list", "broker/threads/purge"]);
    });
  }
  test("一次请求完成本地与服务器删除，参数带 serverDelete、期限 120 秒，不再单独发 thread/delete", async () => {
    const t = context(dir, ["rm", "abcd", "--purge", "--json"]);
    t.fake.handle = (m) => m === "broker/threads/list" ? { threads: [summary()] } : purged();
    expect(await rm(t.ctx)).toBe(0);
    expect(methods(t)).toEqual(["broker/threads/list", "broker/threads/purge"]);
    expect(t.fake.calls[1]!.params).toEqual({ threadId: "thread-test", serverDelete: true });
    expect(t.fake.calls[1]!.options.timeoutMs).toBe(120_000);
    expect(t.json[0]).toMatchObject({ ok: true, deleted: ["local-index", "server-thread"], alreadyDeleted: ["local-log"], failed: [],
      operationId: "op-2", recoverable: false });
  });
  test("有阻塞项时列出并退出 8", async () => {
    const t = context(dir, ["rm", "abcd", "--purge", "--json"]);
    t.fake.handle = (m) => { if (m === "broker/threads/list") return { threads: [summary()] }; throw blocked; };
    expect(await rm(t.ctx)).toBe(8);
    expect(t.json[0]).toMatchObject({ ok: false, mode: "purge" });
    expect(t.notices.join()).toContain("cxb stop review-child --run run-old");
  });
  test("settling 以 500 毫秒重试成功", async () => {
    const t = context(dir, ["rm", "abcd", "--purge", "--json"]);
    let count = 0;
    const stamps: number[] = [];
    t.fake.handle = (m) => {
      if (m === "broker/threads/list") return { threads: [summary()] };
      stamps.push(Date.now()); if (++count === 1) throw reasonError("settling", -32001); return purged();
    };
    expect(await rm(t.ctx)).toBe(0);
    expect(stamps[1]! - stamps[0]!).toBeGreaterThanOrEqual(490);
  });
  test("settling 十秒预算耗尽退出 8", async () => {
    const t = context(dir, ["rm", "abcd", "--purge"]);
    let count = 0;
    t.fake.handle = (m) => { if (m === "broker/threads/list") return { threads: [summary()] }; count++; throw reasonError("settling", -32001); };
    await expect(rm(t.ctx)).rejects.toMatchObject({ exitCode: 8 });
    expect(count).toBeGreaterThan(1);
  }, 15_000);
  test("部分失败（含服务器删除结果未知）如实报告并退出 1，结果未知提示按 --purge 重试", async () => {
    const t = context(dir, ["rm", "abcd", "--purge", "--json"]);
    t.fake.handle = (m) => m === "broker/threads/list" ? { threads: [summary()] }
      : purged({ deleted: ["local-index"], failed: [{ path: "runs/记录", reason: "拒绝访问" }, { path: "server-thread", reason: "outcomeUnknown" }] });
    expect(await rm(t.ctx)).toBe(1);
    expect(t.json[0]).toMatchObject({ ok: false, failed: [{ path: "runs/记录", reason: "拒绝访问" }, { path: "server-thread", reason: "outcomeUnknown" }] });
    expect(t.notices.join("\n")).toContain("cxb rm abcd --purge");
  });
  test("删除时线程上有归档待核实：提示普通 rm 而不是 --purge", async () => {
    const t = context(dir, ["rm", "abcd", "--purge"]);
    t.fake.handle = (m) => {
      if (m === "broker/threads/list") return { threads: [summary()] };
      throw reasonError("lifecyclePending", -32001, { kind: "archive", phase: "outcomeUnknown", threadId: "thread-test" });
    };
    const error = await rm(t.ctx).then(() => null, (error) => error);
    expect(error).toMatchObject({ exitCode: 8 });
    expect(error.message).toContain("cxb rm thread-test");
    expect(error.message).not.toContain("--purge");
  });
});
