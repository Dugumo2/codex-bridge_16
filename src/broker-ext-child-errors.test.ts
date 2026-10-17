/**
 * 上游报错第二批 C2 黑盒：子线程（子代理）上的报错与进展按活动绑定归入父运行（契约第 5、6 节）。
 * 用子代理模拟（src/test-support/subagent-mock.ts）派生子线程，再在子线程上注入通知驱动真实 broker：
 * - 子线程连发 3 条 401：owner 收到带本次子线程项与完整数组的 broker/run/updated（count 1→3），
 *   watch 快照、落盘记录与重启后快照一致，父运行自己的 upstreamError 不变；
 * - 另一运行的子线程报错只记到那个运行；
 * - review 未归属期间到达的子线程事件只记账一次；
 * - 子线程持续进展时父运行不判无进展（依赖 C1 计时，待合并后运行：CXB_UE2_MERGED=1）。
 * broker 只支持 Windows：其他平台整组跳过（不加载夹具）。
 */
import { describe, expect, test, beforeEach, afterEach } from "bun:test";
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import type { Subprocess } from "bun";
import type { TestClient as Client } from "./test-support/broker-harness";

const RUNNABLE = process.platform === "win32";
const MERGED = process.env.CXB_UE2_MERGED === "1";
const harness = RUNNABLE ? await import("./test-support/broker-harness") : null;

type Diagnostic = { count: number; willRetry: boolean | null; category: string; httpStatusCode: number | null; turnId: string | null };
type ChildError = { threadId: string; parentThreadId: string; upstreamError: Diagnostic };
type Run = {
  runId: string; state: string; upstreamError?: Diagnostic | null; diagnosticRevision?: number;
  childUpstreamErrors?: ChildError[]; stall?: unknown; interruptReason?: string | null;
};
type Update = { runId: string; diagnosticRevision: number; upstreamError: Diagnostic | null; childUpstreamErrors: ChildError[]; childUpstreamError: ChildError | null; stall: unknown };

let tempDir: string;
let home: string;
const spawnedProcesses: Subprocess[] = [];
const context = { get tempDir() { return tempDir; }, spawnedProcesses };
const mockPids = new Set<number>();

beforeEach(() => {
  tempDir = mkdtempSync(join(process.env.TEMP ?? tmpdir(), "cxb-child-errors-"));
  home = join(tempDir, "data");
  mkdirSync(home);
});

function alive(pid: number): boolean {
  try { process.kill(pid, 0); return true; } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
    throw error;
  }
}

function registerMockPids(): void {
  const path = join(tempDir, "mock-pids.txt");
  if (existsSync(path)) {
    for (const pid of readFileSync(path, "utf8").trim().split(/\s+/).filter(Boolean).map(Number)) mockPids.add(pid);
  }
}

afterEach(async () => {
  const failures: unknown[] = [];
  registerMockPids();
  for (const proc of spawnedProcesses) {
    try { proc.kill(); } catch { /* 进程可能已正常退出。 */ }
    try { expect(await harness!.exitsWithin(proc, 8_000)).toBe(true); } catch (error) { failures.push(error); }
  }
  spawnedProcesses.length = 0;
  for (const pid of mockPids) {
    try {
      if (alive(pid)) process.kill(pid, "SIGKILL");
      await harness!.waitFor(() => !alive(pid), 5_000);
    } catch (error) { failures.push(error); }
  }
  mockPids.clear();
  if (harness) await harness.removeDirWithRetry(tempDir);
  expect(failures).toEqual([]);
  expect(existsSync(tempDir)).toBe(false);
}, 30_000);

const PARENT = "thread-001";
const UNAUTHORIZED = { message: "unauthorized", codexErrorInfo: { httpConnectionFailed: { httpStatusCode: 401 } } };

describe.skipIf(!RUNNABLE)("broker-ext 子线程报错归入父运行（C2）", () => {
  const h = harness!;
  const clients: Client[] = [];

  afterEach(async () => {
    for (const client of clients.splice(0)) await client.close().catch(() => undefined);
  });

  async function startBroker(options: { reviewDelay?: number } = {}) {
    const sockPath = h.testSocketPath(tempDir);
    const mockDir = h.createMockCodex(tempDir, {
      subagents: { enabled: true }, sendTurnCompleted: false,
      ...(options.reviewDelay === undefined ? {} : { legacyReview: true, reviewDelay: options.reviewDelay }),
    });
    const proc = h.spawnBroker(context, h.endpointFor(sockPath), mockDir, { env: { CXB_HOME: home } });
    await h.waitForSocket(sockPath);
    return { sockPath, proc };
  }
  async function connect(sockPath: string): Promise<Client> {
    const client = await h.TestClient.connectAndInit(sockPath, { cxb: true });
    clients.push(client);
    return client;
  }
  const notes = (client: Client, method: string) =>
    client.messages.filter((m) => m.method === method && m.id === undefined).map((m) => m.params as any);
  const updates = (client: Client, runId: string) =>
    (notes(client, "broker/run/updated") as Update[]).filter((u) => u.runId === runId);
  const startRun = async (client: Client, threadId = PARENT, extra: Record<string, unknown> = {}) =>
    await client.request("turn/start", { threadId, input: [{ type: "text", text: "派生子代理" }], ...extra }) as { runId: string; turn: { id: string } };
  /** 派生并等到子线程首轮在父运行下标为运行中（绑定已建立）。 */
  async function spawnChild(client: Client, parent: string, parentTurn: string, child: string, item: string) {
    const spawned = await client.request("fixture/spawnChild", {
      parentThreadId: parent, parentTurnId: parentTurn, itemId: item, childThreadId: child, version: "v1", started: "never",
    }) as { turnId: string };
    await h.waitFor(() => notes(client, "broker/run/childChanged").some((n) => n.threadId === child && n.activity === "running"), 10_000);
    return spawned.turnId;
  }
  const injectError = (client: Client, thread: string, turnId: string, error = UNAUTHORIZED) =>
    client.request("fixture/inject", { threadId: thread, kind: "notification", payload: { method: "error", params: { turnId, willRetry: true, error } } });
  const injectDelta = (client: Client, thread: string, turnId: string, index: number) =>
    client.request("fixture/inject", { threadId: thread, kind: "notification", payload: { method: "item/agentMessage/delta", params: { turnId, itemId: `子消息-${index}`, delta: `子输出 ${index}` } } });
  /** 有界地确认一段时间内某条件始终不成立。 */
  const staysFalse = async (cond: () => boolean, ms: number) => {
    const until = Date.now() + ms;
    while (Date.now() < until) {
      if (cond()) return false;
      await Bun.sleep(25);
    }
    return !cond();
  };
  function records(): Run[] {
    const workspaces = join(home, "workspaces");
    if (!existsSync(workspaces)) return [];
    return readdirSync(workspaces).flatMap((workspace) => {
      const runs = join(workspaces, workspace, "runs");
      if (!existsSync(runs)) return [];
      return readdirSync(runs).filter((name) => name.endsWith(".json"))
        .map((name) => JSON.parse(readFileSync(join(runs, name), "utf8")).payload as Run);
    });
  }

  test("子线程连发 3 条 401：owner 通知计数 1→3 带完整数组，watch 快照、落盘与重启后一致，父报错不变", async () => {
    const first = await startBroker();
    const owner = await connect(first.sockPath);
    const run = await startRun(owner);
    const childTurn = await spawnChild(owner, PARENT, run.turn.id, "child-1", "spawn-1");
    for (let i = 0; i < 3; i++) await injectError(owner, "child-1", childTurn);
    await h.waitFor(() => updates(owner, run.runId).some((u) => u.childUpstreamError?.upstreamError.count === 3), 10_000);
    const seen = updates(owner, run.runId).filter((u) => u.childUpstreamError !== null);
    expect(seen.map((u) => u.childUpstreamError!.upstreamError.count)).toEqual([1, 2, 3]);
    for (const u of seen) {
      expect(u.upstreamError).toBeNull();
      expect(u.childUpstreamErrors).toHaveLength(1);
      expect(u.childUpstreamErrors[0]).toEqual(u.childUpstreamError!);
      expect(u.childUpstreamError).toMatchObject({ threadId: "child-1", parentThreadId: PARENT });
    }
    const revisions = seen.map((u) => u.diagnosticRevision);
    for (let i = 1; i < revisions.length; i++) expect(revisions[i]).toBeGreaterThan(revisions[i - 1]!);
    const snapshot = await owner.request("broker/threads/watch", { threadId: PARENT }) as { run: Run };
    expect(snapshot.run.runId).toBe(run.runId);
    expect(snapshot.run.upstreamError ?? null).toBeNull();
    expect(snapshot.run.childUpstreamErrors).toEqual([seen.at(-1)!.childUpstreamError!]);
    expect(snapshot.run.childUpstreamErrors![0]!.upstreamError).toMatchObject({
      count: 3, willRetry: true, category: "authentication", httpStatusCode: 401, turnId: childTurn,
    });
    await h.waitFor(() => records().some((r) => r.runId === run.runId && r.childUpstreamErrors?.[0]?.upstreamError.count === 3), 10_000);
    expect(records().find((r) => r.runId === run.runId)!.childUpstreamErrors).toEqual(snapshot.run.childUpstreamErrors);
    registerMockPids();
    for (const client of clients.splice(0)) await client.close();
    first.proc.kill();
    expect(await h.exitsWithin(first.proc, 8_000)).toBe(true);
    const second = await startBroker();
    const reader = await connect(second.sockPath);
    const restored = await reader.request("broker/threads/watch", { threadId: PARENT }) as { run: Run };
    expect(restored.run.runId).toBe(run.runId);
    expect(restored.run.childUpstreamErrors).toEqual(snapshot.run.childUpstreamErrors);
  }, 60_000);

  test("另一运行的子线程报错只记到那个运行", async () => {
    const { sockPath } = await startBroker();
    const ownerA = await connect(sockPath);
    const ownerB = await connect(sockPath);
    const runA = await startRun(ownerA, PARENT);
    const runB = await startRun(ownerB, "thread-002");
    expect(runB.runId).not.toBe(runA.runId);
    const turnA = await spawnChild(ownerA, PARENT, runA.turn.id, "child-a", "spawn-a");
    const turnB = await spawnChild(ownerB, "thread-002", runB.turn.id, "child-b", "spawn-b");
    await injectError(ownerB, "child-b", turnB);
    await h.waitFor(() => updates(ownerB, runB.runId).some((u) => u.childUpstreamError?.threadId === "child-b"), 10_000);
    await injectError(ownerA, "child-a", turnA);
    await h.waitFor(() => updates(ownerA, runA.runId).some((u) => u.childUpstreamError?.threadId === "child-a"), 10_000);
    const allA = updates(ownerA, runA.runId);
    expect(allA.flatMap((u) => u.childUpstreamErrors.map((e) => e.threadId))).not.toContain("child-b");
    expect(updates(ownerA, runB.runId)).toEqual([]);
    const snapshotA = await ownerA.request("broker/threads/watch", { threadId: PARENT }) as { run: Run };
    const snapshotB = await ownerB.request("broker/threads/watch", { threadId: "thread-002" }) as { run: Run };
    expect(snapshotA.run.childUpstreamErrors!.map((e) => e.threadId)).toEqual(["child-a"]);
    expect(snapshotB.run.childUpstreamErrors!.map((e) => e.threadId)).toEqual(["child-b"]);
    expect(snapshotB.run.childUpstreamErrors![0]!.parentThreadId).toBe("thread-002");
  }, 40_000);

  test("review 未归属期间到达的子线程报错只记账一次", async () => {
    const { sockPath } = await startBroker({ reviewDelay: 1_500 });
    const owner = await connect(sockPath);
    const reviewer = await connect(sockPath);
    const run = await startRun(owner);
    const childTurn = await spawnChild(owner, PARENT, run.turn.id, "child-1", "spawn-1");
    // review 回包延迟期间 broker 处于「有未归属 review」状态，非认领线程事件会进入未归属缓冲。
    const review = reviewer.request("review/start", { delivery: "detached", threadId: "thread-002", target: { type: "uncommittedChanges" } });
    await Bun.sleep(200);
    await injectError(owner, "child-1", childTurn);
    await h.waitFor(() => updates(owner, run.runId).some((u) => u.childUpstreamError?.threadId === "child-1"), 10_000);
    await review;
    // 归属定下后的回放不应再把同一事件计入一次。
    expect(await staysFalse(() => updates(owner, run.runId).filter((u) => u.childUpstreamError !== null).length > 1, 1_500)).toBe(true);
    const snapshot = await owner.request("broker/threads/watch", { threadId: PARENT }) as { run: Run };
    expect(snapshot.run.childUpstreamErrors!.map((e) => [e.threadId, e.upstreamError.count])).toEqual([["child-1", 1]]);
  }, 40_000);

  // 进展归入后由 C1 的计时重排；本路 broker 的计时是空钩子，合并后运行。
  test.skipIf(!MERGED)("子线程持续进展时父运行不判无进展；进展停止后才到期（待合并后运行）", async () => {
    const { sockPath } = await startBroker();
    const owner = await connect(sockPath);
    const run = await startRun(owner, PARENT, { cxbRunPolicy: { stallTimeoutMs: 1_500, upstreamErrorPolicy: "observe" } });
    const childTurn = await spawnChild(owner, PARENT, run.turn.id, "child-1", "spawn-1");
    const stalled = () => updates(owner, run.runId).some((u) => u.stall !== null && u.stall !== undefined);
    const until = Date.now() + 4_000;
    for (let i = 0; Date.now() < until; i++) {
      await injectDelta(owner, "child-1", childTurn, i);
      expect(stalled()).toBe(false);
      await Bun.sleep(400);
    }
    expect(stalled()).toBe(false);
    await h.waitFor(stalled, 10_000);
  }, 40_000);
});
