/**
 * SA-a 黑盒：用子代理模拟（src/test-support/subagent-mock.ts）驱动真实 broker。
 * - 子线程审批先于派生证据与 thread/started 到达，最终转给父运行 owner；
 * - cxbOrigin 只发给 cxb 连接；
 * - detach 后子事项进收件箱，保留来源和原期限；
 * - watch 快照与 broker/run/childChanged 的 revision 规则；
 * - 重启后不恢复回答权。
 * broker 只支持 Windows：其他平台没有可运行的被测 broker，整组跳过（不加载夹具）。
 */
import { describe, expect, test, beforeEach, afterEach } from "bun:test";
import { mkdtempSync, readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import type { Subprocess } from "bun";
import type { TestClient as Client } from "./test-support/broker-harness";

const RUNNABLE = process.platform === "win32";
const harness = RUNNABLE ? await import("./test-support/broker-harness") : null;

let tempDir: string;
const spawnedProcesses: Subprocess[] = [];
const context = { get tempDir() { return tempDir; }, spawnedProcesses };

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), "broker-ext-subagent-"));
});
afterEach(async () => {
  for (const proc of spawnedProcesses) { try { proc.kill(); } catch {} }
  await Promise.all(spawnedProcesses.map((p) => p.exited.catch(() => undefined)));
  spawnedProcesses.length = 0;
  if (harness) await harness.removeDirWithRetry(tempDir);
});

const APPROVAL = "item/commandExecution/requestApproval";
const PARENT = "thread-001";

describe.skipIf(!RUNNABLE)("broker-ext 子代理（SA-a）", () => {
  const h = harness!;

  async function startBroker(home = join(tempDir, "cxb-home")) {
    const sockPath = h.testSocketPath(tempDir);
    const mockDir = h.createMockCodex(tempDir, { subagents: { enabled: true }, sendTurnCompleted: false });
    const proc = h.spawnBroker(context, h.endpointFor(sockPath), mockDir, { env: { CXB_HOME: home } });
    await h.waitForSocket(sockPath);
    return { sockPath, proc, home };
  }
  const serverRequests = (client: Client) =>
    client.messages.filter((m) => m.id !== undefined && "method" in m) as Array<{ id: unknown; method: string; params: any }>;
  const notes = (client: Client, method: string) =>
    client.messages.filter((m) => m.method === method && m.id === undefined) as Array<{ params: any }>;
  const startRun = async (client: Client) =>
    await client.request("turn/start", { threadId: PARENT, input: [{ type: "text", text: "派生子代理" }] }) as { runId?: string; turn: { id: string } };
  const spawnChild = (client: Client, started: "now" | "held" | "never", child = "child-1", item = "spawn-1") =>
    client.request("fixture/spawnChild", { parentThreadId: PARENT, parentTurnId: "turn-001", itemId: item, childThreadId: child, version: "v1", started }) as
      Promise<{ childThreadId: string; turnId: string; holdId: string | null }>;
  const inject = (client: Client, threadId: string, turnId: string, requestId: string) =>
    client.request("fixture/inject", { threadId, kind: "request", requestId, payload: { method: APPROVAL, params: { turnId, itemId: "item-c", command: "echo 子线程", cwd: "/tmp" } } });
  const mockState = async (client: Client) => await client.request("fixture/state", {}) as Record<string, { answers: Array<{ requestId: unknown; count: number }> }>;
  /** 有界地确认一段时间内某条件始终不成立（用于「不转发」类断言）。 */
  const staysFalse = async (cond: () => boolean, ms: number) => {
    const until = Date.now() + ms;
    while (Date.now() < until) {
      if (cond()) return false;
      await new Promise((r) => setTimeout(r, 25));
    }
    return !cond();
  };

  test("子线程审批先于派生证据和 thread/started 到达：回查等证据，最终带 cxbOrigin 转给父运行 owner", async () => {
    const { sockPath } = await startBroker();
    const owner = await h.TestClient.connectAndInit(sockPath, { cxb: true });
    try {
      const run = await startRun(owner);
      expect(run.runId).toMatch(/^run-/);
      const spawned = await spawnChild(owner, "held");
      expect(spawned.holdId).toBe("spawn:child-1");
      await inject(owner, "child-1", spawned.turnId, "sa-early");
      // 只有 spawnAgent 的 started（接收方为空）：证据不足，请求留在回查，不转发、不回答。
      expect(await staysFalse(() => serverRequests(owner).some((r) => r.params?.threadId === "child-1"), 500)).toBe(true);
      await owner.request("fixture/release", { holdId: "spawn:child-1" });
      await h.waitFor(() => serverRequests(owner).some((r) => r.params?.threadId === "child-1"));
      const forwarded = serverRequests(owner).find((r) => r.params?.threadId === "child-1")!;
      expect(forwarded.method).toBe(APPROVAL);
      expect(forwarded.params.cxbOrigin).toEqual({ rootThreadId: PARENT, runId: run.runId, parentThreadId: PARENT, relationKind: "subagent", sourceThreadId: "child-1" });
      owner.send({ id: forwarded.id as string, result: { decision: "accept" } });
      let answers: Array<{ requestId: unknown; count: number }> = [];
      for (let i = 0; i < 100 && answers.length === 0; i++) {
        answers = (await mockState(owner))["child-1"]?.answers ?? [];
        if (!answers.length) await new Promise((r) => setTimeout(r, 25));
      }
      expect(answers).toEqual([{ requestId: "sa-early", count: 1, result: { decision: "accept" } } as any]);
    } finally {
      await owner.close();
    }
  }, 30_000);

  test("cxbOrigin 只发给 cxb 连接", async () => {
    const { sockPath } = await startBroker();
    const plain = await h.TestClient.connectAndInit(sockPath);
    try {
      await startRun(plain);
      const spawned = await spawnChild(plain, "never");
      await inject(plain, "child-1", spawned.turnId, "sa-plain");
      await h.waitFor(() => serverRequests(plain).some((r) => r.params?.threadId === "child-1"));
      const forwarded = serverRequests(plain).find((r) => r.params?.threadId === "child-1")!;
      expect(forwarded.params).not.toHaveProperty("cxbOrigin");
      expect(plain.messages.some((m) => typeof m.method === "string" && (m.method as string).startsWith("broker/"))).toBe(false);
    } finally {
      await plain.close();
    }
  }, 30_000);

  test("detach 原子接管已转发的子请求：进收件箱，保留来源线程与原期限，另一连接回答后上游收到一次", async () => {
    const { sockPath } = await startBroker();
    const owner = await h.TestClient.connectAndInit(sockPath, { cxb: true });
    const other = await h.TestClient.connectAndInit(sockPath, { cxb: true });
    try {
      const run = await startRun(owner);
      const spawned = await spawnChild(owner, "never");
      const injectedAt = Date.now();
      await inject(owner, "child-1", spawned.turnId, "sa-detach");
      await h.waitFor(() => serverRequests(owner).some((r) => r.params?.threadId === "child-1"));
      await new Promise((r) => setTimeout(r, 200));
      const detached = await owner.request("broker/threads/detach", { threadId: PARENT, runId: run.runId, policy: "on-request" }) as { outcome: string; pending: any[] };
      expect(detached.outcome).toBe("detached");
      expect(detached.pending).toHaveLength(1);
      const item = detached.pending[0];
      expect(item).toMatchObject({ kind: "approval", threadId: "child-1", rootThreadId: PARENT, parentThreadId: PARENT, runId: run.runId, turnId: spawned.turnId, status: "pending" });
      // 原期限从首次收到算起，不因转入收件箱重新计时。
      expect(item.expiresAt - item.createdAt).toBeLessThan(3_600_000);
      expect(item.expiresAt).toBeLessThanOrEqual(Date.now() + 3_600_000);
      expect(item.expiresAt).toBeGreaterThanOrEqual(injectedAt + 3_600_000 - 5_000);
      expect(await other.request("broker/inbox/resolve", { id: item.id, answer: { decision: "accept" } })).toEqual({ status: "resolved", duplicate: false });
      let answers: Array<{ requestId: unknown; count: number }> = [];
      for (let i = 0; i < 200 && answers.length === 0; i++) {
        answers = (await mockState(other))["child-1"]?.answers ?? [];
        if (!answers.length) await new Promise((r) => setTimeout(r, 25));
      }
      expect(answers).toEqual([{ requestId: "sa-detach", count: 1, result: { decision: "accept" } } as any]);
    } finally {
      await owner.close();
      await other.close();
    }
  }, 30_000);

  test("watch 先登记再给快照；childChanged 的 revision 单调递增，快照 revision 不小于已收到的通知", async () => {
    const { sockPath } = await startBroker();
    const owner = await h.TestClient.connectAndInit(sockPath, { cxb: true });
    const watcher = await h.TestClient.connectAndInit(sockPath, { cxb: true });
    try {
      const run = await startRun(owner);
      const before = await watcher.request("broker/threads/watch", { threadId: PARENT }) as { revision: number; children: { members: unknown[] } };
      expect(before.children.members).toEqual([]);
      await spawnChild(owner, "never");
      await h.waitFor(() => notes(watcher, "broker/run/childChanged").some((n) => n.params.threadId === "child-1" && n.params.activity === "running"));
      const changes = notes(watcher, "broker/run/childChanged").map((n) => n.params);
      const revisions = changes.map((c) => c.revision as number);
      expect(revisions.every((r, i) => i === 0 || r > revisions[i - 1])).toBe(true);
      expect(revisions[0]).toBeGreaterThan(before.revision);
      expect(changes.at(-1)).toMatchObject({ runId: run.runId, rootThreadId: PARENT, threadId: "child-1", parentThreadId: PARENT, relationKind: "subagent" });
      const after = await watcher.request("broker/threads/watch", { threadId: PARENT }) as { revision: number; children: { members: any[] } };
      expect(after.revision).toBeGreaterThanOrEqual(revisions.at(-1)!);
      expect(after.children.members.map((m) => m.threadId)).toEqual(["child-1"]);
    } finally {
      await owner.close();
      await watcher.close();
    }
  }, 30_000);

  test("重启后只恢复关系历史，不恢复回答权", async () => {
    const home = join(tempDir, "cxb-home");
    const first = await startBroker(home);
    const owner = await h.TestClient.connectAndInit(first.sockPath, { cxb: true });
    const spawned = await (async () => {
      await startRun(owner);
      const spawned = await spawnChild(owner, "never");
      await h.waitFor(() => readIndex(home).some((e) => e.threadId === "child-1" && e.parentThreadId === PARENT));
      return spawned;
    })();
    await owner.close();
    first.proc.kill();
    await first.proc.exited;
    const second = await startBroker(home);
    const owner2 = await h.TestClient.connectAndInit(second.sockPath, { cxb: true });
    try {
      // 新运行在同一父线程上开始；旧子线程旧轮次的请求不能凭重启前的绑定转给新 owner。
      await startRun(owner2);
      await inject(owner2, "child-1", spawned.turnId, "sa-restart");
      expect(await staysFalse(() => serverRequests(owner2).some((r) => r.params?.threadId === "child-1"), 1500)).toBe(true);
      const answers = (await mockState(owner2))["child-1"]?.answers ?? [];
      expect(answers).toEqual([]);
      expect(readIndex(home).find((e) => e.threadId === "child-1")).toMatchObject({ parentThreadId: PARENT, relationKind: "subagent", agentVersion: "v1" });
    } finally {
      await owner2.close();
    }
  }, 40_000);

  function readIndex(home: string): Array<Record<string, unknown>> {
    const out: Array<Record<string, unknown>> = [];
    let workspaces: string[] = [];
    try { workspaces = readdirSync(join(home, "workspaces")); } catch { return out; }
    for (const ws of workspaces) {
      try {
        const payload = JSON.parse(readFileSync(join(home, "workspaces", ws, "threads.json"), "utf8")).payload;
        out.push(...Object.values(payload.threads as Record<string, Record<string, unknown>>));
      } catch { /* 尚未写出 */ }
    }
    return out;
  }
});
