import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { rm } from "./rm";
import { removeTemp } from "./test-fixtures/main-test-support";
import { context, ledger, reasonError, summary } from "./test-fixtures/n4a-unit";
import { runResult, wireError } from "./test-fixtures/run-fakes";
import { N4Broker } from "./test-fixtures/n4a-broker";
import { readThreadIndex } from "./ledger";
import { until } from "./test-fixtures/main-test-support";

let dir: string;
beforeEach(() => { dir = mkdtempSync(join(tmpdir(), "cxb-n4a-rm-")); });
afterEach(async () => { await removeTemp(dir); }, 10_000);
const stop = (confirmed = true) => ({ threadId: "thread-test", runId: "run-test", interruptSent: true,
  alreadyStopped: false, stopOutcome: confirmed ? "confirmed" : "unconfirmed", goal: "paused", run: null });
const purged = () => ({ threadId: "thread-test", deleted: ["local-index"], alreadyDeleted: ["local-log"], failed: [] });

describe("rm 归档", () => {
  for (const state of ["running", "stopping", "lost"] as const) test(`${state} 先停止确认再归档（lost 从磁盘补读）`, async () => {
    const run = runResult({ state, stopOutcome: "unconfirmed" });
    ledger(dir, [run]);
    const t = context(dir, ["rm", "abcd", "--json"]);
    t.fake.handle = (m) => {
      if (m === "broker/threads/list") return { threads: [summary()] };
      if (m === "broker/threads/watch") return { thread: summary(), run: state === "lost" ? null : run };
      if (m === "broker/threads/stop") return stop();
      return {};
    };
    expect(await rm(t.ctx)).toBe(0);
    expect(t.fake.calls.map((c) => c.method)).toEqual(["broker/threads/list", "broker/threads/watch", "broker/threads/stop", "thread/archive"]);
    expect(t.fake.calls[2]!.params).toEqual({ threadId: "thread-test" });
    expect(t.fake.calls[2]!.options.timeoutMs).toBe(45_000);
    expect(t.json[0]).toMatchObject({ stopped: true, archived: true, deleted: ["local-index"], recoverable: true });
  });
  test("线程忙即使无运行也先停止；停止未确认不归档并退出 3", async () => {
    const t = context(dir, ["rm", "abcd"]);
    t.fake.handle = (m) => m === "broker/threads/list" ? { threads: [summary()] }
      : m === "broker/threads/watch" ? { thread: summary({ state: "starting", lastRunId: null }), run: null } : stop(false);
    expect(await rm(t.ctx)).toBe(3);
    expect(t.fake.calls.some((c) => c.method === "thread/archive")).toBe(false);
    expect(t.notices.join()).toContain("未归档");
  });
  test("已确认 lost 不重复停止；已归档错误如实报告本地仍保留", async () => {
    const t = context(dir, ["rm", "abcd", "--json"]);
    t.fake.handle = (m) => {
      if (m === "broker/threads/list") return { threads: [summary()] };
      if (m === "broker/threads/watch") return { thread: summary(), run: runResult({ state: "lost", stopOutcome: "confirmed" }) };
      throw wireError(-32603, "failed to archive session: thread thread-test not found");
    };
    expect(await rm(t.ctx)).toBe(0);
    expect(t.fake.calls.some((c) => c.method === "broker/threads/stop")).toBe(false);
    expect(t.json[0]).toMatchObject({ archived: "alreadyDone", deleted: [] });
    expect(t.notices.join()).toContain("本地索引仍保留");
  });
  test("仅服务器线程 unknownThread 直接归档，不误停止", async () => {
    const t = context(dir, ["rm", "server-only", "--json"]);
    t.fake.handle = (m) => {
      if (m === "broker/threads/list") return { threads: [] };
      if (m === "broker/threads/watch") throw reasonError("unknownThread");
      return {};
    };
    expect(await rm(t.ctx)).toBe(0);
    expect(t.fake.calls.map((c) => c.method)).toEqual(["broker/threads/list", "broker/threads/watch", "thread/archive"]);
  });
  test("无记录只放行 unknownThread，其他错误不能绕过停止检查", async () => {
    const t = context(dir, ["rm", "server-only"]);
    t.fake.handle = (m) => { if (m === "broker/threads/list") return { threads: [] }; throw reasonError("shuttingDown"); };
    await expect(rm(t.ctx)).rejects.toMatchObject({ exitCode: 6 });
    expect(t.fake.calls.some((c) => c.method === "thread/archive")).toBe(false);
  });
  for (const message of ["no rollout found for thread id thread-test", "thread thread-test is archived"]) test(`上游幂等归档：${message}`, async () => {
    const t = context(dir, ["rm", "abcd", "--json"]);
    t.fake.handle = (m) => {
      if (m === "broker/threads/list") return { threads: [summary()] };
      if (m === "broker/threads/watch") return { thread: summary(), run: runResult() };
      throw wireError(-32600, message);
    };
    expect(await rm(t.ctx)).toBe(0);
    expect(t.json[0]).toMatchObject({ archived: "alreadyDone", deleted: [] });
  });
  test("其他资源缺失不冒充归档已完成", async () => {
    const t = context(dir, ["rm", "abcd", "--json"]);
    t.fake.handle = (m) => {
      if (m === "broker/threads/list") return { threads: [summary()] };
      if (m === "broker/threads/watch") return { thread: summary(), run: runResult() };
      throw wireError(-32600, "model not found: missing-model");
    };
    await expect(rm(t.ctx)).rejects.toMatchObject({ exitCode: 7 });
    expect(t.json).toEqual([]);
  });
});

describe("rm 真 broker", () => {
  let broker: N4Broker;
  beforeEach(async () => { broker = new N4Broker(); await broker.setup(); }, 25_000);
  afterEach(async () => { await broker.cleanup(); }, 40_000);
  test("归档成功后 broker 删除索引，ls --local 看不到", async () => {
    const created = await broker.request("thread/start", { cwd: broker.workspace.root });
    await until(() => readThreadIndex(broker.workspace.workspaceDir).length > 0, 5000, "初始索引没有落盘");
    const archived = await broker.cli(["rm", created.thread.id, "--json"]);
    expect(archived.code).toBe(0);
    expect(JSON.parse(archived.stdout)).toMatchObject({ archived: true, deleted: ["local-index"] });
    await until(() => readThreadIndex(broker.workspace.workspaceDir).length === 0, 5000, "归档索引未更新");
    const listed = await broker.cli(["ls", "--local", "--json"]);
    expect(listed.code).toBe(0);
    expect(JSON.parse(listed.stdout).items).toEqual([]);
  }, 30_000);
});

describe("rm 永久删除", () => {
  for (const [reason, exitCode] of [["threadBusy", 8], ["stopping", 8], ["purgePending", 8], ["shuttingDown", 6], ["storageError", 1], ["storageTimeout", 1]] as const) {
    test(`${reason} 映射退出 ${exitCode} 且不删除服务器`, async () => {
      const t = context(dir, ["rm", "abcd", "--purge"]);
      t.fake.handle = (m) => { if (m === "broker/threads/list") return { threads: [summary()] }; throw reasonError(reason); };
      await expect(rm(t.ctx)).rejects.toMatchObject({ exitCode });
      expect(t.fake.calls.map((c) => c.method)).toEqual(["broker/threads/list", "broker/threads/purge"]);
    });
  }
  test("settling 以 500 毫秒重试成功，本地在服务器之前且保留原数组", async () => {
    const t = context(dir, ["rm", "abcd", "--purge", "--json"]);
    let count = 0;
    const stamps: number[] = [];
    t.fake.handle = (m) => {
      if (m === "broker/threads/list") return { threads: [summary()] };
      if (m === "broker/threads/purge") { stamps.push(Date.now()); if (++count === 1) throw reasonError("settling"); return purged(); }
      return {};
    };
    expect(await rm(t.ctx)).toBe(0);
    expect(stamps[1]! - stamps[0]!).toBeGreaterThanOrEqual(490);
    expect(t.json[0]).toMatchObject({ deleted: ["local-index", "server-thread"], alreadyDeleted: ["local-log"], failed: [], recoverable: false });
    expect(t.fake.calls.at(-1)?.method).toBe("thread/delete");
  });
  test("settling 十秒预算耗尽退出 8", async () => {
    const t = context(dir, ["rm", "abcd", "--purge"]);
    let count = 0;
    t.fake.handle = (m) => { if (m === "broker/threads/list") return { threads: [summary()] }; count++; throw reasonError("settling"); };
    await expect(rm(t.ctx)).rejects.toMatchObject({ exitCode: 8 });
    expect(count).toBeGreaterThan(1);
    expect(t.fake.calls.some((c) => c.method === "thread/delete")).toBe(false);
  }, 15_000);
  test("本地部分失败不删服务器，报告三个数组并退出 1", async () => {
    const t = context(dir, ["rm", "abcd", "--purge", "--json"]);
    t.fake.handle = (m) => m === "broker/threads/list" ? { threads: [summary()] }
      : { ...purged(), failed: [{ path: "runs/记录", reason: "拒绝访问" }] };
    expect(await rm(t.ctx)).toBe(1);
    expect(t.json[0]).toMatchObject({ ok: false, deleted: ["local-index"], alreadyDeleted: ["local-log"], failed: [{ path: "runs/记录", reason: "拒绝访问" }] });
    expect(t.fake.calls.some((c) => c.method === "thread/delete")).toBe(false);
  });
  for (const code of [-32601, -32600, -32603]) test(`服务器删除错误 ${code} 分别处理不支持、不存在与失败`, async () => {
    const t = context(dir, ["rm", "abcd", "--purge", "--json"]);
    t.fake.handle = (m) => {
      if (m === "broker/threads/list") return { threads: [summary()] };
      if (m === "broker/threads/purge") return purged();
      throw wireError(code, code === -32600 ? "thread not found: thread-test" : "内部失败");
    };
    expect(await rm(t.ctx)).toBe(code === -32600 ? 0 : 1);
    if (code === -32600) expect(t.json[0].alreadyDeleted).toEqual(["local-log", "server-thread"]);
    else expect(t.json[0].failed[0].reason).toContain(code === -32601 ? "不支持 thread/delete" : "Codex 请求失败");
  });
});
