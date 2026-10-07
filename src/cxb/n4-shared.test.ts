// N4 共用模块的单测：账本只读访问（ledger.ts）、线程 ID 解析（thread-ref.ts）、停止调用与错误归类（stop-thread.ts）。
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { RpcError } from "../types";
import { commandSession } from "./command-session";
import { CxbError, rpcFailure, type StopReply, type ThreadSummary } from "./contracts";
import { listThreadRuns, readRunLog, readRunRecord, readThreadIndex } from "./ledger";
import { STOP_SETTLING_LIMIT_MS, stopThread } from "./stop-thread";
import { context, reasonError } from "./test-fixtures/n4a-unit";
import { FakeRpc, runResult } from "./test-fixtures/run-fakes";
import { matchThread, resolveThreadLocal, resolveThreadViaBroker } from "./thread-ref";
import { watchdog } from "./test-fixtures/test-watchdog";

let dir: string;
const sessionCleanups: Array<() => Promise<void>> = [];
function trackedSession(c: ReturnType<typeof context>) {
  const session = commandSession(c.ctx);
  let closed = false;
  const connect = c.ctx.connect;
  c.ctx.connect = async () => {
    const connection = await connect();
    sessionCleanups.push(async () => { await watchdog(connection.close()); closed = true; });
    return connection;
  };
  sessionCleanups.push(async () => {
    session.dispose();
    expect(session.signal.aborted).toBe(true);
    expect(c.cleanup.size).toBe(0);
    expect(c.fake.subscriptions).toBe(0);
    expect(c.fake.notificationErrors).toEqual([]);
    if (c.connects) expect(closed).toBe(true);
  });
  return session;
}
beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "cxb-n4-shared-"));
  mkdirSync(join(dir, "runs"));
});
afterEach(async () => {
  const errors: unknown[] = [];
  for (const cleanup of sessionCleanups.splice(0).reverse()) {
    try { await cleanup(); } catch (error) { errors.push(error); }
  }
  rmSync(dir, { recursive: true, force: true });
  expect(existsSync(dir)).toBe(false);
  if (errors.length) throw new AggregateError(errors, "会话收尾失败");
});

function writeIndex(threads: Record<string, unknown>) {
  writeFileSync(join(dir, "threads.json"), JSON.stringify({ epoch: 1, revision: 3, payload: { threads, removed: {} } }));
}
function entry(threadId: string, shortId: string, updatedAt: number) {
  return { threadId, shortId, lastRunId: null, updatedAt, model: null, preview: null, cwd: null, meta: null };
}
function writeRun(run: ReturnType<typeof runResult>) {
  writeFileSync(join(dir, "runs", `${run.runId}.json`), JSON.stringify({ epoch: 1, revision: 1, payload: run }));
}

describe("ledger：只读 broker 账本", () => {
  test("线程索引：不存在为空、按 updatedAt 倒序、损坏抛 LOCAL_ERROR", () => {
    expect(readThreadIndex(dir)).toEqual([]);
    writeIndex({ aaaa1111: entry("t-1", "aaaa1111", 1), bbbb2222: entry("t-2", "bbbb2222", 5) });
    expect(readThreadIndex(dir).map((e) => e.shortId)).toEqual(["bbbb2222", "aaaa1111"]);
    writeFileSync(join(dir, "threads.json"), "{坏");
    expect(() => readThreadIndex(dir)).toThrow(CxbError);
    writeFileSync(join(dir, "threads.json"), JSON.stringify({ payload: {} }));
    expect(() => readThreadIndex(dir)).toThrow(CxbError);
    // 坏条目不静默跳过：数组形式、缺 threadId、键与 shortId 不符都报损坏。
    for (const threads of [[entry("t-1", "aaaa1111", 1)], { aaaa1111: { shortId: "aaaa1111", updatedAt: 1 } },
      { aaaa1111: entry("t-1", "bbbb2222", 1) }]) {
      writeIndex(threads as Record<string, unknown>);
      expect(() => readThreadIndex(dir)).toThrow(expect.objectContaining({ code: "LOCAL_ERROR" }));
    }
  });

  test("运行记录缺少调用方依赖的字段（turns、state 等）报损坏", () => {
    const { turns: _, ...noTurns } = runResult({ runId: "run-a" });
    const { status: __, ...noStatus } = runResult({ runId: "run-a" });
    const base = runResult({ runId: "run-a" });
    for (const payload of [noTurns, noStatus, { ...base, state: "奇怪" }, { ...base, status: "奇怪" }, { ...base, turns: [{}] },
      { ...base, turns: [{ ...base.turns[0], ready: "broken" }] }, { ...base, turns: [{ ...base.turns[0], status: "奇怪" }] },
      { ...base, progress: {} }, { ...base, progress: { ...base.progress, done: [{ itemId: 1 }] } }]) {
      writeFileSync(join(dir, "runs", "run-a.json"), JSON.stringify({ epoch: 1, revision: 1, payload }));
      expect(() => readRunRecord(dir, "run-a")).toThrow(expect.objectContaining({ code: "LOCAL_ERROR" }));
    }
  });

  test("运行记录：不存在为 null、runId 不符或损坏抛 LOCAL_ERROR；按线程列出含 review 子线程并按开始时间排序", () => {
    expect(readRunRecord(dir, "run-none")).toBeNull();
    writeRun(runResult({ runId: "run-b", threadId: "t-1", startedAt: 20 }));
    writeRun(runResult({ runId: "run-a", threadId: "t-1", startedAt: 10 }));
    writeRun(runResult({ runId: "run-r", threadId: "t-parent", reviewThreadId: "t-1", startedAt: 30 }));
    writeRun(runResult({ runId: "run-x", threadId: "t-2", startedAt: 5 }));
    expect(listThreadRuns(dir, "t-1").map((r) => r.runId)).toEqual(["run-a", "run-b", "run-r"]);
    writeFileSync(join(dir, "runs", "run-c.json"), JSON.stringify({ payload: runResult({ runId: "别的" }) }));
    expect(() => readRunRecord(dir, "run-c")).toThrow(CxbError);
    expect(() => listThreadRuns(dir, "t-1")).toThrow(CxbError);
  });

  test("日志：先轮转文件再当前文件，丢弃没有换行结尾的半行，按前三个空格切出 runId、事件、内容", () => {
    expect(readRunLog(dir, "aaaa1111")).toBeNull();
    writeFileSync(join(dir, "runs", "aaaa1111.log.1"), "2026-10-06T00:00:00.000Z run-1 运行开始 run-1\n");
    writeFileSync(join(dir, "runs", "aaaa1111.log"), "2026-10-06T00:00:01.000Z - 丢行 丢了 2 行\r\n"
      + "2026-10-06T00:00:01.000Z run-1 空转刹车暂停目标 内容 含 空格\n2026-10-06T00:00:02.000Z run-1 运行结束\n2026-10-06T00:00:03.000Z run-1 写到一半");
    expect(readRunLog(dir, "aaaa1111")).toEqual([
      { ts: "2026-10-06T00:00:00.000Z", runId: "run-1", event: "运行开始", content: "run-1", text: "运行开始 run-1" },
      { ts: "2026-10-06T00:00:01.000Z", runId: null, event: "丢行", content: "丢了 2 行", text: "丢行 丢了 2 行" },
      { ts: "2026-10-06T00:00:01.000Z", runId: "run-1", event: "空转刹车暂停目标", content: "内容 含 空格", text: "空转刹车暂停目标 内容 含 空格" },
      { ts: "2026-10-06T00:00:02.000Z", runId: "run-1", event: "运行结束", content: "", text: "运行结束" },
    ]);
    // 完整的行格式不对就是损坏：没有时间戳、旧格式（第二段是事件名）、缺事件、旧的无时间戳丢行标记。
    for (const bad of ["乱码一行", "2026-10-06T00:00:00.000Z 运行开始 run-1", "2026-10-06T00:00:00.000Z run-1", "丢了 3 行"]) {
      writeFileSync(join(dir, "runs", "aaaa1111.log"), `${bad}\n`);
      expect(() => readRunLog(dir, "aaaa1111")).toThrow(expect.objectContaining({ code: "LOCAL_ERROR" }));
    }
  });
});

describe("thread-ref：短 ID、前缀与服务器 ID", () => {
  const candidates = [
    { threadId: "0199aaaa-1111", shortId: "ab12cd34" },
    { threadId: "0199aaaa-2222", shortId: "ab99ef00" },
    { threadId: "0199bbbb-3333", shortId: "ffff0000" },
  ];

  test("精确优先于前缀；唯一前缀命中；多个前缀为 AMBIGUOUS；零个为 null", () => {
    expect(matchThread("ab12cd34", candidates)?.threadId).toBe("0199aaaa-1111");
    expect(matchThread("0199bbbb-3333", candidates)?.shortId).toBe("ffff0000");
    expect(matchThread("ab12", candidates)?.threadId).toBe("0199aaaa-1111");
    expect(matchThread("0199b", candidates)?.shortId).toBe("ffff0000");
    expect(() => matchThread("ab", candidates)).toThrow(expect.objectContaining({ code: "AMBIGUOUS" }));
    expect(() => matchThread("0199aaaa", candidates)).toThrow(expect.objectContaining({ code: "AMBIGUOUS" }));
    expect(matchThread("zz", candidates)).toBeNull();
    // 只有生命周期记录的服务器线程可能没有短 ID：只按 threadId 匹配，不影响其他候选。
    const withNull = [...candidates, { threadId: "0199cccc-4444", shortId: null as unknown as string }];
    expect(matchThread("ab12", withNull)?.threadId).toBe("0199aaaa-1111");
    expect(matchThread("0199cccc", withNull)?.threadId).toBe("0199cccc-4444");
    expect(matchThread("server-uuid", withNull)).toBeNull();
    // 同一线程的 threadId 与短 ID 都以该前缀开头：只算一个，不报歧义。
    expect(matchThread("abc", [{ threadId: "abc-1", shortId: "abc12345" }, { threadId: "t-9", shortId: "ffff" }])?.threadId).toBe("abc-1");
    // 精确匹配某线程的短 ID，同时是另一线程 threadId 的前缀时，不报歧义。
    expect(matchThread("abc", [{ threadId: "abcdef", shortId: "x1" }, { threadId: "t-9", shortId: "abc" }])?.threadId).toBe("t-9");
  });

  test("经 broker 解析：命中带摘要；找不到时 passThrough 原样返回，否则 NOT_FOUND", async () => {
    const fake = new FakeRpc();
    const summary = { threadId: "0199aaaa-1111", shortId: "ab12cd34" } as ThreadSummary;
    fake.handle = (method) => (method === "broker/threads/list" ? { threads: [summary] } : {});
    expect(await resolveThreadViaBroker(fake.rpc, "ab12")).toEqual({ threadId: "0199aaaa-1111", shortId: "ab12cd34", summary });
    expect(await resolveThreadViaBroker(fake.rpc, "服务器上的线程", { passThrough: true }))
      .toEqual({ threadId: "服务器上的线程", shortId: null, summary: null });
    await expect(resolveThreadViaBroker(fake.rpc, "zz")).rejects.toMatchObject({ code: "NOT_FOUND" });
  });

  test("本地解析：读 threads.json，找不到 NOT_FOUND", () => {
    writeIndex({ ab12cd34: entry("0199aaaa-1111", "ab12cd34", 1) });
    expect(resolveThreadLocal(dir, "ab1")).toEqual({ threadId: "0199aaaa-1111", shortId: "ab12cd34" });
    expect(() => resolveThreadLocal(dir, "zz")).toThrow(expect.objectContaining({ code: "NOT_FOUND" }));
  });
});

describe("command-session：连接与请求的错误归类", () => {
  test("连接层已定的退出码原样保留（版本不符 12、broker 不可用 6），超时改报 6", async () => {
    for (const code of ["PROTOCOL_MISMATCH", "BROKER_UNAVAILABLE", "INTERRUPTED"] as const) {
      const c = context(dir, ["ls"]);
      c.ctx.connect = async () => { throw new CxbError(code, "连接失败"); };
      await expect(trackedSession(c).connect()).rejects.toMatchObject({ code });
    }
  });

  test("请求错误：线程不存在 2、存储失败 1、broker 私有方法 6、上游方法 7，原因仍可从 cause 读出", async () => {
    const c = context(dir, ["ls"]);
    const { fake } = c;
    const failures: Record<string, unknown> = {
      "thread/archive": new CxbError("BROKER_UNAVAILABLE", "线上", { cause: new RpcError("x", -32600, "no rollout found for thread id t-1") }),
      "broker/threads/purge": reasonError("storageError"),
      "broker/threads/watch": reasonError("tooManyWatchers"),
      "thread/delete": new CxbError("BROKER_UNAVAILABLE", "线上", { cause: new RpcError("x", -32603, "内部错误") }),
      "broker/inbox/resolve": reasonError("notFound"),
      // 删除进行中 broker 拦下线程请求：私有方法与转发的上游方法都归为线程被占 8。
      "broker/threads/stop": reasonError("purgePending", -32001),
      "thread/name/set": reasonError("purgePending", -32001),
    };
    fake.handle = (method) => { throw failures[method]; };
    const rpc = await trackedSession(c).connect();
    const codes = Object.fromEntries(await Promise.all(Object.keys(failures).map(async (method) =>
      [method, await rpc.request(method, {}).then(() => "成功", (error: CxbError) => error.code)])));
    expect(codes).toEqual({ "thread/archive": "NOT_FOUND", "broker/threads/purge": "LOCAL_ERROR",
      "broker/threads/watch": "BROKER_UNAVAILABLE", "thread/delete": "CODEX_FAILED", "broker/inbox/resolve": "NOT_FOUND",
      "broker/threads/stop": "THREAD_HELD", "thread/name/set": "THREAD_HELD" });
    const wrapped = await rpc.request("broker/threads/watch", {}).catch((error) => error);
    expect(rpcFailure(wrapped).reason).toBe("tooManyWatchers");
  });

  test("已取消时不再发请求", async () => {
    const c = context(dir, ["ls"]);
    const { fake } = c;
    const session = trackedSession(c);
    const rpc = await session.connect();
    session.dispose();
    await expect(rpc.request("broker/threads/list", {})).rejects.toMatchObject({ code: "INTERRUPTED" });
    expect(fake.calls).toHaveLength(0);
  });
});

describe("stop-thread：期限与错误归类", () => {
  const reply: StopReply = { threadId: "t-1", runId: "run-1", interruptSent: true, goal: "paused", alreadyStopped: false,
    stopOutcome: "unconfirmed", run: null };
  const wire = (reason: string) => new CxbError("BROKER_UNAVAILABLE", "线上错误",
    { cause: new RpcError("拒绝", -32000, undefined, { reason }) });

  test("参数只带给定的字段，客户端期限 45 秒，未确认原样返回", async () => {
    const fake = new FakeRpc();
    fake.handle = () => reply;
    expect(await stopThread(fake.rpc, { threadId: "t-1" })).toEqual(reply);
    expect(await stopThread(fake.rpc, { threadId: "t-1", clear: true, runId: "run-1" })).toEqual(reply);
    expect(fake.calls.map((c) => c.params)).toEqual([{ threadId: "t-1" }, { threadId: "t-1", clear: true, runId: "run-1" }]);
    expect(fake.calls.every((c) => c.options.timeoutMs === 45_000)).toBe(true);
  });

  test("joined 为 8、runMismatch 为 2、shuttingDown 为 6；settling 重试后成功", async () => {
    for (const [reason, code] of [["joined", "THREAD_HELD"], ["stopping", "THREAD_HELD"], ["runMismatch", "NOT_FOUND"],
      ["shuttingDown", "BROKER_UNAVAILABLE"]] as const) {
      const fake = new FakeRpc();
      fake.handle = () => { throw wire(reason); };
      await expect(stopThread(fake.rpc, { threadId: "t-1" })).rejects.toMatchObject({ code });
    }
    const fake = new FakeRpc();
    let calls = 0;
    fake.handle = () => { if (calls++ < 2) throw wire("settling"); return reply; };
    expect(await stopThread(fake.rpc, { threadId: "t-1" })).toEqual(reply);
    expect(calls).toBe(3);
  });

  test("settling 持续超过重试总时长为 8；取消立即拒绝", async () => {
    const fake = new FakeRpc();
    fake.handle = () => { throw wire("settling"); };
    const started = Date.now();
    await expect(stopThread(fake.rpc, { threadId: "t-1" })).rejects.toMatchObject({ code: "THREAD_HELD", retryable: true });
    expect(Date.now() - started).toBeGreaterThanOrEqual(STOP_SETTLING_LIMIT_MS - 1000);
    const controller = new AbortController();
    const pending = new FakeRpc();
    pending.handle = () => new Promise(() => {});
    const work = stopThread(pending.rpc, { threadId: "t-1" }, { signal: controller.signal });
    controller.abort();
    await expect(work).rejects.toMatchObject({ code: "INTERRUPTED" });
    // 预先取消：一条停止请求都不发。
    const idle = new FakeRpc();
    await expect(stopThread(idle.rpc, { threadId: "t-1" }, { signal: AbortSignal.abort() })).rejects.toMatchObject({ code: "INTERRUPTED" });
    expect(idle.calls).toHaveLength(0);
  }, 20_000);
});
