/**
 * SA-b 黑盒：用子代理模拟（subagent-mock.ts）与状态读取模拟（status-history.ts，二者同时启用）驱动真实 broker。
 * - 父线程不等子代理就结束：门禁暂缓释放、等待后代，再在同运行汇总；
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
import { Sa1bHarness } from "./test-support/sa1b-harness";

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
    const mockDir = h.createMockCodex(tempDir, { backgroundTerminals: { enabled: true }, subagents: { enabled: true }, statusRead: { historyMode: "paged" }, sendTurnCompleted: false });
    const proc = h.spawnBroker(context, h.endpointFor(sockPath), mockDir, { env: { CXB_HOME: join(tempDir, "cxb-home") } });
    await h.waitForSocket(sockPath);
    return { sockPath, proc };
  }
  const notes = (client: Client, method: string) =>
    client.messages.filter((m) => m.method === method && m.id === undefined) as Array<{ params: any }>;
  const startRun = async (client: Client) => {
    const run = await client.request("turn/start", { threadId: PARENT, input: [{ type: "text", text: "派生子代理" }] }) as { runId: string };
    // cxb 能力的 turn/start 先回运行句柄；本组场景需要真正开工后再派生和停止。
    await until(async () => (await client.request("broker/threads/watch", { threadId: PARENT }) as any).thread.state === "running", 8_000);
    return run;
  };
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

  test("父线程不等子代理就结束：保持占用，后代完成后同运行汇总且只交付一次", async () => {
    const integrated = new Sa1bHarness();
    try {
      await integrated.open(); await integrated.start(); await integrated.spawn("child-1");
      await integrated.finish("root"); await integrated.observe();
      expect(integrated.ended()).toHaveLength(0);
      expect((await integrated.record()).state).toBe("running");
      expect((await integrated.state()).threads["child-1"].interrupts).toHaveLength(0);
      await integrated.finish("child-1", "子代理最终结果");
      const request = await integrated.reconciliation();
      expect(request.params.input[0].text).toContain("子代理最终结果");
      await integrated.finish("root", "已核实子代理最终结果");
      await integrated.until(async () => integrated.ended().length === 1);
      const ended = integrated.ended()[0];
      expect(ended.status).toBe("completed");
      expect(ended.runId).toBe(integrated.runId);
      expect(ended.childrenStop).toBeUndefined();
      expect(ended.subagentJoin.reconciledEpoch).toBe(ended.subagentJoin.workEpoch);
    } finally {
      await integrated.close();
    }
  }, 60_000);

  test("停止父线程：整树停止，回包带 childrenStop", async () => {
    const { sockPath } = await startBroker();
    const owner = await h.TestClient.connectAndInit(sockPath, { cxb: true });
    const other = await h.TestClient.connectAndInit(sockPath, { cxb: true });
    try {
      const run = await startRun(owner);
      await spawnChild(owner, "child-1");
      const stopping = other.request("broker/threads/stop", { threadId: PARENT }, 45_000) as Promise<any>;
      // 父线程的模拟中断不自带完成通知：等子线程被中断后补上父轮的结束。
      await until(async () => (await mockState(owner))["child-1"]!.interrupts.length > 0, 8_000);
      await finishParent(owner, "interrupted");
      const reply = await stopping.catch(async (error) => {
        const snapshot = await owner.request("broker/threads/watch", { threadId: PARENT });
        const background = await owner.request("fixture/backgroundTerminals/state", {});
        const calls = await owner.request("mock/state", {});
        throw new Error(`整树停止未回复：${String(error)}；运行=${JSON.stringify(snapshot)}；后台=${JSON.stringify(background)}；调用=${JSON.stringify(calls)}`);
      });
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
