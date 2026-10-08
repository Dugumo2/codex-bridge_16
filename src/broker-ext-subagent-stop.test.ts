/**
 * SA-b 黑盒：用子代理模拟（subagent-mock.ts）与状态读取模拟（status-history.ts，二者同时启用）驱动真实 broker。
 * - 父线程不等子代理就结束：门禁暂缓释放、停树，运行以 interrupted/subagentsUnfinished 结束且报告先于 ended；
 * - 停止父线程：整树停止，回包带 childrenStop；
 * - 不接受直接输入的子线程拒绝开工（subagentNoDirectInput）；
 * - 有子树的父线程拒绝归档（threadBusy + hasSubtree）。
 * 两个模拟接管的请求不重叠：子线程的 thread/read 由子代理模拟先接管（状态字段按子线程轮次给出），
 * 父线程的 thread/read 与所有 thread/turns/list 由状态读取模拟接管。
 * broker 只支持 Windows：其他平台没有可运行的被测 broker，整组跳过（不加载夹具）。
 */
import { describe, expect, test, beforeEach, afterEach } from "bun:test";
import { mkdtempSync } from "node:fs";
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
  tempDir = mkdtempSync(join(tmpdir(), "broker-ext-subagent-stop-"));
});
afterEach(async () => {
  for (const proc of spawnedProcesses) { try { proc.kill(); } catch {} }
  await Promise.all(spawnedProcesses.map((p) => p.exited.catch(() => undefined)));
  spawnedProcesses.length = 0;
  if (harness) await harness.removeDirWithRetry(tempDir);
});

const PARENT = "thread-001";
const PARENT_TURN = "turn-001";

describe.skipIf(!RUNNABLE)("broker-ext 子代理停止与门禁（SA-b）", () => {
  const h = harness!;

  async function startBroker() {
    const sockPath = h.testSocketPath(tempDir);
    const mockDir = h.createMockCodex(tempDir, { subagents: { enabled: true }, statusRead: { historyMode: "paged" }, sendTurnCompleted: false });
    const proc = h.spawnBroker(context, h.endpointFor(sockPath), mockDir, { env: { CXB_HOME: join(tempDir, "cxb-home") } });
    await h.waitForSocket(sockPath);
    return { sockPath, proc };
  }
  const notes = (client: Client, method: string) =>
    client.messages.filter((m) => m.method === method && m.id === undefined) as Array<{ params: any }>;
  const startRun = async (client: Client) =>
    await client.request("turn/start", { threadId: PARENT, input: [{ type: "text", text: "派生子代理" }] }) as { runId: string };
  const spawnChild = (client: Client, child: string, version: "v1" | "v2" = "v1") =>
    client.request("fixture/spawnChild", { parentThreadId: PARENT, parentTurnId: PARENT_TURN, itemId: `spawn-${child}`,
      childThreadId: child, version, started: "now" }) as Promise<{ turnId: string }>;
  const finishParent = (client: Client, status: "completed" | "interrupted" | "failed" = "completed") =>
    client.request("fixture/inject", { threadId: PARENT, kind: "notification",
      payload: { method: "turn/completed", params: { turn: { id: PARENT_TURN, items: [], status, error: null } } } });
  const mockState = async (client: Client) =>
    await client.request("fixture/state", {}) as Record<string, { interrupts: unknown[]; goalWrites: unknown[] }>;
  /** 测试客户端的错误只带 code 与 message；data 从收到的原始回包里取。 */
  const errorData = (client: Client) =>
    (client.messages.filter((m) => m.id !== undefined && !("method" in m) && "error" in m).at(-1)?.error as { data?: any } | undefined)?.data;
  const until = async (cond: () => Promise<boolean>, ms: number) => {
    const deadline = Date.now() + ms;
    while (Date.now() < deadline) {
      if (await cond()) return;
      await new Promise((r) => setTimeout(r, 50));
    }
    throw new Error("等待条件超时");
  };

  test("父线程不等子代理就结束：暂缓释放并停树，运行标子代理未完成，报告与 ended 一起交付", async () => {
    const { sockPath } = await startBroker();
    const owner = await h.TestClient.connectAndInit(sockPath, { cxb: true });
    try {
      const run = await startRun(owner);
      await spawnChild(owner, "child-1");
      await finishParent(owner);
      await h.waitFor(() => notes(owner, "broker/run/ended").some((n) => n.params?.run?.runId === run.runId), 30_000);
      const ended = notes(owner, "broker/run/ended").find((n) => n.params?.run?.runId === run.runId)!.params.run;
      expect(ended.status).toBe("interrupted");
      expect(ended.interruptReason).toBe("subagentsUnfinished");
      expect(ended.childrenStop.trigger).toBe("parentFinishedEarly");
      expect(ended.childrenStop.stopped).toContain("child-1");
      expect((await mockState(owner))["child-1"]!.interrupts.length).toBeGreaterThanOrEqual(1);
      expect(notes(owner, "broker/run/ended").filter((n) => n.params?.run?.runId === run.runId)).toHaveLength(1);
    } finally {
      await owner.close();
    }
  }, 60_000);

  test("停止父线程：整树停止，回包带 childrenStop", async () => {
    const { sockPath } = await startBroker();
    const owner = await h.TestClient.connectAndInit(sockPath, { cxb: true });
    const other = await h.TestClient.connectAndInit(sockPath, { cxb: true });
    try {
      const run = await startRun(owner);
      await spawnChild(owner, "child-1");
      const stopping = other.request("broker/threads/stop", { threadId: PARENT }) as Promise<any>;
      // 父线程的模拟中断不自带完成通知：等子线程被中断后补上父轮的结束。
      await until(async () => (await mockState(owner))["child-1"]!.interrupts.length > 0, 8_000);
      await finishParent(owner, "interrupted");
      const reply = await stopping;
      expect(reply.runId).toBe(run.runId);
      expect(reply.alreadyStopped).toBe(false);
      expect(reply.childrenStop.trigger).toBe("userStop");
      expect(reply.childrenStop.stopped).toContain("child-1");
      expect(reply.stopOutcome).toBe("confirmed");
    } finally {
      await owner.close();
      await other.close();
    }
  }, 90_000);

  test("不接受直接输入的子线程拒绝开工，不改投到父线程", async () => {
    const { sockPath } = await startBroker();
    const owner = await h.TestClient.connectAndInit(sockPath, { cxb: true });
    try {
      await startRun(owner);
      await spawnChild(owner, "child-v2", "v2");
      await owner.request("fixture/turn", { threadId: "child-v2", action: "complete" });
      const error = await owner.request("turn/start", { threadId: "child-v2", input: [{ type: "text", text: "直接输入" }] })
        .then(() => null, (e: any) => e);
      expect(error?.code).toBe(-32000);
      expect(errorData(owner)?.reason).toBe("subagentNoDirectInput");
      expect(errorData(owner)?.parentThreadId).toBe(PARENT);
    } finally {
      await owner.close();
    }
  }, 30_000);

  test("有子树的父线程拒绝归档并列出阻塞项", async () => {
    const { sockPath } = await startBroker();
    const owner = await h.TestClient.connectAndInit(sockPath, { cxb: true });
    try {
      const run = await startRun(owner);
      await spawnChild(owner, "child-1");
      await owner.request("fixture/turn", { threadId: "child-1", action: "complete" });
      await finishParent(owner);
      await h.waitFor(() => notes(owner, "broker/run/ended").some((n) => n.params?.run?.runId === run.runId), 30_000);
      const error = await owner.request("broker/threads/archive", { threadId: PARENT }).then(() => null, (e: any) => e);
      expect(error?.code).toBe(-32001);
      const data = errorData(owner);
      expect(data?.reason).toBe("threadBusy");
      expect(data?.blockers.some((b: any) => b.reason === "hasSubtree")).toBe(true);
    } finally {
      await owner.close();
    }
  }, 60_000);
});
