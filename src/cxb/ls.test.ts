import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { CxbError } from "./contracts";
import { ls } from "./ls";
import { show } from "./show";
import { removeTemp } from "./test-fixtures/main-test-support";
import { context, ledger, summary } from "./test-fixtures/n4a-unit";
import { runResult, wireError } from "./test-fixtures/run-fakes";
import { N4Broker } from "./test-fixtures/n4a-broker";

let dir: string;
beforeEach(() => { dir = mkdtempSync(join(tmpdir(), "cxb-n4a-ls-")); });
afterEach(async () => { await removeTemp(dir); }, 10_000);

describe("ls", () => {
  test("local 从不连接 broker，按最近运行显示状态、清洗预览并截断 Unicode 字符", async () => {
    ledger(dir, [runResult({ status: "interrupted" })], [summary({ preview: "\u001b\n" + "😀".repeat(130) })]);
    const t = context(dir, ["ls", "--local", "--json"]);
    let connects = 0;
    t.ctx.connect = async () => { connects++; throw new Error("禁止拉起 broker"); };
    expect(await ls(t.ctx)).toBe(0);
    expect(connects).toBe(0);
    expect(t.json[0]).toMatchObject({ source: "local", truncated: false });
    expect(t.json[0].items[0].status).toBe("interrupted");
    expect([...t.json[0].items[0].preview]).toHaveLength(120);
    expect(t.json[0].items[0].preview).not.toMatch(/[\n\u001b]/);
  });
  test("合并按本地优先去重，秒转毫秒排序与仅服务器短 ID 为 null", async () => {
    ledger(dir);
    const t = context(dir, ["ls", "--json"]);
    t.fake.handle = (method, p) => {
      if (method === "broker/threads/list") return { threads: [summary({ state: "awaiting" })] };
      expect(p).toEqual({ cwd: dir, cursor: null, limit: 50, sortKey: "updated_at", sortDirection: "desc", sourceKinds: ["cli", "vscode", "exec", "appServer"] });
      return { data: [{ id: "server", updatedAt: 3, status: { type: "notLoaded" } }, { id: "thread-test", updatedAt: 2, preview: "不采用" }], nextCursor: null };
    };
    expect(await ls(t.ctx)).toBe(0);
    expect(t.json[0].items.map((i: any) => [i.threadId, i.updatedAt, i.status, i.shortId])).toEqual([
      ["server", 3000, "server:notLoaded", null], ["thread-test", 2000, "awaiting", "abcd1234"],
    ]);
    expect(t.json[0].items[1].preview).toBe("预览");
  });
  for (const disconnected of [true, false]) test(`${disconnected ? "broker 不可用" : "服务器失败"} 降级 local 并退出 0`, async () => {
    ledger(dir);
    const t = context(dir, ["ls", "--json"]);
    if (disconnected) t.ctx.connect = async () => { throw new CxbError("BROKER_UNAVAILABLE", "离线"); };
    t.fake.handle = (m) => { if (m === "broker/threads/list") return { threads: [summary()] }; throw wireError(-32603, "服务器错误"); };
    expect(await ls(t.ctx)).toBe(0);
    expect(t.json[0]).toMatchObject({ source: "local", truncated: false });
    expect(t.json[0].items).toHaveLength(1);
    expect(t.notices).toHaveLength(1);
  });
  test("分页相等边界继续翻，老线程后页仍进入前 N", async () => {
    ledger(dir, [], [summary({ lastRunId: null, updatedAt: 1000 })]);
    const t = context(dir, ["ls", "--limit", "2", "--json"]);
    t.fake.handle = (m, p) => {
      if (m === "broker/threads/list") return { threads: [summary({ lastRunId: null, updatedAt: 1000 })] };
      return p.cursor === null ? { data: [{ id: "z", updatedAt: 9 }, { id: "y", updatedAt: 8 }], nextCursor: "下一页" }
        : { data: [{ id: "a", updatedAt: 8 }, { id: "b", updatedAt: 7 }], nextCursor: "更老" };
    };
    expect(await ls(t.ctx)).toBe(0);
    expect(t.fake.calls.filter((c) => c.method === "thread/list")).toHaveLength(2);
    expect(t.json[0].items.map((i: any) => i.threadId)).toEqual(["z", "a"]);
    expect(t.json[0].truncated).toBe(true);
  });
  test("all 不截断且不设页数上限", async () => {
    const t = context(dir, ["ls", "--all", "--json"]);
    let pages = 0;
    t.fake.handle = (m) => m === "broker/threads/list" ? { threads: [] }
      : { data: [{ id: `线程-${++pages}`, updatedAt: 100 - pages }], nextCursor: pages < 30 ? String(pages) : null };
    expect(await ls(t.ctx)).toBe(0);
    expect(pages).toBe(30);
    expect(t.json[0].items).toHaveLength(30);
    expect(t.json[0].truncated).toBe(false);
  });
  test("本地 limit 与空列表、索引损坏", async () => {
    const empty = context(dir, ["ls", "--local", "--json"]);
    expect(await ls(empty.ctx)).toBe(0);
    expect(empty.json[0].items).toEqual([]);
    ledger(dir, [runResult({ state: "lost", status: null })], [summary(), summary({ threadId: "other", shortId: "other123", lastRunId: null, updatedAt: 1000 })]);
    const limited = context(dir, ["ls", "--local", "--limit", "1", "--json"]);
    await ls(limited.ctx);
    expect(limited.json[0].items).toHaveLength(1);
    expect(limited.json[0].items[0].status).toBe("lost");
    expect(limited.json[0].truncated).toBe(true);
    writeFileSync(join(dir, "threads.json"), "{}");
    await expect(ls(context(dir, ["ls", "--local"]).ctx)).rejects.toMatchObject({ exitCode: 1 });
  });
  test("后页失败丢弃不完整服务器合并，所有终态按记录显示", async () => {
    const states = ["completed", "interrupted", "failed", "goalBlocked"] as const;
    const threads = states.map((status, i) => summary({ threadId: status, shortId: `id${i}`, lastRunId: `run${i}`, updatedAt: 0 }));
    ledger(dir, states.map((status, i) => runResult({ runId: `run${i}`, threadId: status, status })), threads);
    const t = context(dir, ["ls", "--all", "--json"]);
    t.fake.handle = (m, p) => {
      if (m === "broker/threads/list") return { threads };
      if (!p.cursor) return { data: [{ id: "半截服务器", updatedAt: 10 }], nextCursor: "下一页" };
      throw wireError(-32603, "后页失败");
    };
    expect(await ls(t.ctx)).toBe(0);
    expect(t.json[0].source).toBe("local");
    expect(t.json[0].items.map((item: any) => item.status).sort()).toEqual([...states].sort());
    expect(t.json[0].truncated).toBe(false);
  });
});

describe("ls、show 的归档与删除待核实（任务说明 N7-A 5.5 节）", () => {
  const lifecycleFile = (records: Record<string, unknown>) =>
    writeFileSync(join(dir, "lifecycle.json"), JSON.stringify({ epoch: 1, payload: { records } }));
  const record = (threadId: string, patch: Record<string, unknown> = {}) => ({ threadId, operationId: "op-1", kind: "archive",
    phase: "forwarded", shortId: "abcd1234", preview: "预览", updatedAt: 5000, ...patch });
  test("local：索引项带记录显示归档待核实；purge 已删索引、只剩记录的线程按记录列出", async () => {
    ledger(dir);
    lifecycleFile({ "thread-test": record("thread-test"), "thread-gone": record("thread-gone", { kind: "delete", phase: "upstreamDone", shortId: "dead0001", preview: null }) });
    const t = context(dir, ["ls", "--local", "--json"]);
    expect(await ls(t.ctx)).toBe(0);
    const items = Object.fromEntries(t.json[0].items.map((item: any) => [item.threadId, item]));
    expect(items["thread-test"]).toMatchObject({ status: "归档待核实", lifecycle: { kind: "archive" } });
    expect(items["thread-gone"]).toMatchObject({ shortId: "dead0001", status: "删除提交中", lastRunId: null, lifecycle: { kind: "delete" } });
  });
  test("broker 列表带 lifecycle 时显示状态，不读运行记录", async () => {
    ledger(dir, []);
    const t = context(dir, ["ls", "--json"]);
    const lifecycle = { operationId: "op-2", kind: "delete", phase: "outcomeUnknown", shortId: "dead0001", preview: null };
    t.fake.handle = (m) => m === "broker/threads/list"
      ? { threads: [summary({ threadId: "thread-gone", shortId: "dead0001", lastRunId: "run-已删除", lifecycle } as any)] }
      : { data: [], nextCursor: null };
    expect(await ls(t.ctx)).toBe(0);
    expect(t.json[0].items[0]).toMatchObject({ status: "删除待核实", lifecycle });
  });
  test("lifecycle.json 损坏时 ls --local 报本地错误（退出 1），不当作没有待核实", async () => {
    ledger(dir);
    writeFileSync(join(dir, "lifecycle.json"), JSON.stringify({ payload: { records: { "thread-test": { kind: "未知" } } } }));
    await expect(ls(context(dir, ["ls", "--local"]).ctx)).rejects.toMatchObject({ exitCode: 1 });
    writeFileSync(join(dir, "lifecycle.json"), JSON.stringify({ payload: {} }));
    await expect(ls(context(dir, ["ls", "--local"]).ctx)).rejects.toMatchObject({ exitCode: 1 });
  });
  test("show：只剩记录的线程能用原短 ID 找到，提示按 kind 完成核实；有索引的照常显示结果并提示", async () => {
    ledger(dir);
    lifecycleFile({ "thread-gone": record("thread-gone", { kind: "delete", phase: "outcomeUnknown", shortId: "dead0001" }), "thread-test": record("thread-test") });
    const gone = context(dir, ["show", "dead", "--json"]);
    expect(await show(gone.ctx)).toBe(0);
    expect(gone.notices.join()).toContain("删除待核实");
    expect(gone.notices.join()).toContain("cxb rm dead0001 --purge");
    expect(gone.json[0]).toMatchObject({ threadId: "thread-gone", shortId: "dead0001", runId: null, lifecycle: { kind: "delete" } });
    const kept = context(dir, ["show", "abcd"]);
    expect(await show(kept.ctx)).toBe(0);
    expect(kept.notices.join()).toContain("归档待核实");
    expect(kept.texts.join("")).not.toBe("");
  });
});

describe("ls 真 broker", () => {
  let broker: N4Broker;
  beforeEach(async () => { broker = new N4Broker(); await broker.setup(); }, 25_000);
  afterEach(async () => { await broker.cleanup(); }, 40_000);
  test("合并去重与排序经真实服务器列表，保留本地模型", async () => {
    const created = await broker.request("thread/start", { cwd: broker.workspace.root });
    const result = await broker.cli(["ls", "--json"]);
    expect(result.code).toBe(0);
    const data = JSON.parse(result.stdout);
    expect(data.source).toBe("merged");
    expect(data.items.map((item: any) => item.threadId)).toEqual(["server-only-thread", created.thread.id]);
    expect(data.items[1].model).toBe("本地模型");
    expect(data.items[0].shortId).toBeNull();
  }, 30_000);
});
