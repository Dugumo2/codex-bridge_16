import { expect } from "bun:test";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { parseArgv } from "../args";
import { CxbError, type ExitCode, type InboxItem, type ThreadSummary } from "../contracts";
import type { CommandContext } from "../main";
import type { RegisterCleanup } from "../output";
import { watchdog } from "./test-watchdog";
import { FakeRpc } from "./run-fakes";

const cleanups: Array<() => Promise<void>> = [];

/** 即使用例中途断言失败，也取消命令，并给异步 finally 一次事件循环收束机会。 */
export async function cleanupContexts(): Promise<void> {
  const results = await Promise.allSettled(cleanups.splice(0).map((fn) => fn()));
  const errors = results.filter((r) => r.status === "rejected").map((r) => r.reason);
  if (errors.length) throw new AggregateError(errors, "乙组单测资源清理失败");
}

export function item(patch: Partial<InboxItem> = {}): InboxItem {
  return { id: "qu_test1234", kind: "question", threadId: "thread-test", runId: "run-test", turnId: "turn-test",
    method: "item/tool/requestUserInput", params: { questions: [{ id: "q1", question: "请选择文件", isSecret: false,
      options: [{ label: "甲", description: "甲文件" }] }] }, createdAt: 1000, expiresAt: 5000, status: "pending", ...patch };
}

export function thread(patch: Partial<ThreadSummary> = {}): ThreadSummary {
  return { threadId: "thread-test", shortId: "abcd1234", state: "running", owner: "broker", turnId: "turn-test",
    goalActive: false, pendingCount: 0, lastRunId: "run-test", updatedAt: 2000, model: null, preview: null, cwd: null, meta: null, ...patch };
}

export function context(args: string[], fake = new FakeRpc()) {
  const parsed = parseArgv(args);
  const text: string[] = [], json: any[] = [], notices: string[] = [], progress: string[] = [];
  const cleanup = new Set<Parameters<RegisterCleanup>[0]>();
  const dir = mkdtempSync(join(tmpdir(), "cxb-n4b-unit-"));
  const connections: Array<{ closed: boolean; close(): Promise<void> }> = [];
  let connects = 0;
  const ctx: CommandContext = {
    parsed,
    workspace: { dir, root: dir, key: "测试", dataRoot: dir, workspaceDir: dir, pipePath: "", endpoint: "" },
    out: { result(s) { if (!parsed.globals.json) text.push(s); }, resultJson(o) { if (parsed.globals.json) json.push(o); },
      notice(s) { notices.push(s); }, progress(s) { if (parsed.globals.verbose) progress.push(s); },
      error() {}, async flush() {}, async finish() {} },
    registerCleanup(fn) { cleanup.add(fn); return () => { cleanup.delete(fn); }; },
    async connect() {
      connects++;
      const connection = { rpc: fake.rpc, handshake: {} as any, launched: false, closed: false,
        async close() { this.closed = true; } };
      connections.push(connection);
      return connection;
    },
  };
  cleanups.push(async () => {
    const errors: unknown[] = [];
    const clean = async (work: () => unknown) => { try { await watchdog(Promise.resolve().then(work), 1000); } catch (error) { errors.push(error); } };
    for (const fn of cleanup) await clean(fn);
    await clean(() => new Promise<void>((done) => setImmediate(done)));
    for (const connection of connections) await clean(() => connection.close());
    await clean(() => {
      expect(cleanup.size).toBe(0);
      expect(fake.subscriptions).toBe(0);
      expect(fake.inFlightServerRequests).toBe(0);
      expect(fake.notificationErrors).toEqual([]);
      expect(connections.every((c) => c.closed)).toBe(true);
    });
    await clean(() => { rmSync(dir, { recursive: true, force: true }); expect(existsSync(dir)).toBe(false); });
    if (errors.length) throw new AggregateError(errors, "单测会话收尾失败");
  });
  return { ctx, fake, text, json, notices, progress, get connects() { return connects; },
    async cancel() {
      // 同步抛错也转成各自的拒绝；单个挂起或失败不能阻止其他回调执行。
      const results = await Promise.allSettled([...cleanup].map((fn) =>
        watchdog(Promise.resolve().then(() => fn()), 1000, "取消清理回调")));
      const errors = results.filter((result) => result.status === "rejected").map((result) => result.reason);
      if (errors.length) throw new AggregateError(errors, "取消清理失败");
    },
    checkClean() { expect(cleanup.size).toBe(0); expect(fake.subscriptions).toBe(0); expect(fake.notificationErrors).toEqual([]); } };
}

export function failure(reason: string, rpcCode = -32000) {
  return new CxbError("BROKER_UNAVAILABLE", "模拟线上错误", { cause: { rpcCode, data: { reason } } });
}

export async function errorCode(work: Promise<unknown>, code: ExitCode) {
  try { await watchdog(work, 3000, "命令应先于看门狗结束"); throw new Error("预期命令失败却成功"); }
  catch (error) { expect(error).toBeInstanceOf(CxbError); expect((error as CxbError).exitCode).toBe(code); }
}

export function gate<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => { resolve = done; });
  return { promise, resolve, wait: () => watchdog(promise, 5000, "测试控制点") };
}
