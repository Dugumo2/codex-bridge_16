import { afterEach, beforeEach, expect, test } from "bun:test";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import type { Subprocess } from "bun";
import {
  createMockCodex, endpointFor, exitsWithin, spawnBroker, TestClient, testSocketPath,
  waitFor, waitForSocket,
} from "./test-support/broker-harness";

type Diagnostic = {
  count: number; willRetry: boolean | null; category: string; httpStatusCode: number | null;
  message: string; additionalDetails: string | null; source: string;
};
type Run = {
  runId: string; threadId: string; state: string; status: string | null;
  upstreamError?: Diagnostic; diagnosticRevision?: number; lastProgressAt?: number;
};
type Notice = { method?: string; params?: { runId?: string; run?: Run; upstreamError?: Diagnostic; diagnosticRevision?: number } };

let tempDir: string;
let home: string;
let spawnedProcesses: Subprocess[];
let clients: TestClient[];
let mockPids: Set<number>;

beforeEach(() => {
  tempDir = mkdtempSync(join(process.env.TEMP ?? tmpdir(), "cxb-upstream-error-"));
  home = join(tempDir, "data");
  mkdirSync(home);
  spawnedProcesses = [];
  clients = [];
  mockPids = new Set();
});

function alive(pid: number): boolean {
  try { process.kill(pid, 0); return true; } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
    throw error;
  }
}

afterEach(async () => {
  const failures: unknown[] = [];
  for (const client of clients) {
    try { await client.close(); } catch (error) { failures.push(error); }
  }
  for (const proc of spawnedProcesses) {
    try { proc.kill(); } catch { /* 进程可能已正常退出。 */ }
    try { expect(await exitsWithin(proc, 8_000)).toBe(true); } catch (error) { failures.push(error); }
  }
  for (const pid of mockPids) {
    try {
      if (alive(pid)) process.kill(pid, "SIGKILL");
      await waitFor(() => !alive(pid), 5_000);
    } catch (error) { failures.push(error); }
  }
  const deadline = performance.now() + 5_000;
  for (;;) {
    try { rmSync(tempDir, { recursive: true, force: true }); break; } catch (error) {
      if (performance.now() >= deadline) { failures.push(error); break; }
      await Bun.sleep(50);
    }
  }
  expect(failures).toEqual([]);
  expect(existsSync(tempDir)).toBe(false);
}, 30_000);

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

async function connect(socket: string, cxb = true): Promise<TestClient> {
  const client = await TestClient.connectAndInit(socket, { cxb });
  clients.push(client);
  return client;
}

async function startBroker(mock: string): Promise<{ socket: string; proc: Subprocess }> {
  const socket = testSocketPath(tempDir);
  const profile = join(tempDir, "profile");
  mkdirSync(profile, { recursive: true });
  const proc = spawnBroker({ tempDir, spawnedProcesses }, endpointFor(socket), mock, {
    cwd: tempDir, idleTimeout: 60_000,
    env: { CXB_HOME: home, USERPROFILE: profile, HOME: profile, TEMP: process.env.TEMP ?? tmpdir(), TMP: process.env.TMP ?? tmpdir() },
  });
  await waitForSocket(socket, 15_000);
  return { socket, proc };
}

function mockWithErrors(): string {
  const mock = createMockCodex(tempDir, { sendTurnCompleted: false });
  const script = join(mock, process.platform === "win32" ? "codex-mock.ts" : "codex");
  const original = readFileSync(script, "utf8");
  const marker = 'case "turn/start": {\n        const threadId = msg.params?.threadId || "thread-001";';
  expect(original.split(marker)).toHaveLength(2);
  // 只扩充本例的临时模拟脚本，复用现有握手与分页夹具。
  const diagnostic = `
        const shouldFail = msg.params?.input?.[0]?.text !== "正常";
        const error = { message: "认证失败", additionalDetails: "凭证失效 Bearer fake-test-secret",
          codexErrorInfo: {responseStreamDisconnected: {httpStatusCode: 401}} };
        if (shouldFail) for (let count = 1; count <= 3; count++) {
          setTimeout(() => respond({method:"error",params:{threadId,turnId:"turn-001",willRetry:true,
            error:{...error,message:"Reconnecting... " + count + "/5"}}}), 100 + count * 25);
        }
        setTimeout(() => respond({method:"turn/completed",params:{threadId,
          turn:{id:"turn-001",status:shouldFail ? "failed" : "completed",items:[],itemsView:"full",
            error:shouldFail ? error : null}}}), 250);
  `;
  const pidFile = join(tempDir, "mock-pids.txt");
  writeFileSync(script, `import {appendFileSync} from "node:fs"; appendFileSync(${JSON.stringify(pidFile)}, String(process.pid) + "\\n");\n`
    + original.replace(/^#![^\n]*\n/, "").replace(marker, marker + diagnostic));
  return mock;
}

function collect(client: TestClient): Notice[] {
  const notices: Notice[] = [];
  client.onNotification((message) => notices.push(message as Notice));
  return notices;
}

function registerMockPids(): void {
  const path = join(tempDir, "mock-pids.txt");
  if (existsSync(path)) {
    for (const pid of readFileSync(path, "utf8").trim().split(/\s+/).map(Number)) mockPids.add(pid);
  }
}

test("上游报错：真实 broker 连续诊断通知、快照、投影、落盘及重启一致", async () => {
  const mock = mockWithErrors();
  const { socket, proc } = await startBroker(mock);
  const owner = await connect(socket);
  const watcher = await connect(socket);
  const ordinary = await connect(socket, false);
  const ownerNotices = collect(owner);
  const watcherNotices = collect(watcher);
  const ordinaryNotices = collect(ordinary);
  const { thread } = await owner.request("thread/start", {}) as { thread: { id: string } };
  await watcher.request("broker/threads/watch", { threadId: thread.id });
  await ordinary.request("broker/threads/watch", { threadId: thread.id });
  const started = await owner.request("turn/start", { threadId: thread.id, input: [{ type: "text", text: "报错" }] }) as { runId: string };
  await waitFor(() => ownerNotices.some((notice) => notice.method === "broker/run/ended" && notice.params?.run?.runId === started.runId), 15_000);
  const updates = ownerNotices.filter((notice) => notice.method === "broker/run/updated" && notice.params?.runId === started.runId);
  expect(updates.map((notice) => notice.params?.upstreamError?.count)).toEqual([1, 2, 3, 3]);
  expect(updates.map((notice) => notice.params?.upstreamError?.willRetry)).toEqual([true, true, true, false]);
  const revisions = updates.map((notice) => notice.params!.diagnosticRevision!);
  for (let index = 1; index < revisions.length; index++) expect(revisions[index]).toBeGreaterThan(revisions[index - 1]!);
  expect(watcherNotices.filter((notice) => notice.method === "broker/run/updated")).toHaveLength(4);
  expect(ordinaryNotices.filter((notice) => notice.method === "broker/run/updated")).toHaveLength(0);
  expect(ordinaryNotices.filter((notice) => notice.method === "error")).toHaveLength(3);
  const snapshot = await owner.request("broker/threads/watch", { threadId: thread.id }) as { run: Run };
  expect(snapshot.run.upstreamError).toMatchObject({ count: 3, willRetry: false, category: "authentication", httpStatusCode: 401, source: "errorNotification" });
  expect(snapshot.run.upstreamError?.additionalDetails).toBe("凭证失效 [已隐藏]");
  const summaries = await owner.request("broker/runs/modelSummaries", { threadIds: [thread.id] }) as { runs: Run[] };
  expect(summaries.runs.find((run) => run.runId === started.runId)?.upstreamError).toEqual(snapshot.run.upstreamError);
  await waitFor(() => records().some((run) => run.runId === started.runId && run.state === "ended" && run.upstreamError?.count === 3), 10_000);
  expect(records().find((run) => run.runId === started.runId)?.upstreamError).toEqual(snapshot.run.upstreamError);
  registerMockPids();
  for (const client of [owner, watcher, ordinary]) await client.close();
  proc.kill();
  expect(await exitsWithin(proc, 8_000)).toBe(true);
  const restarted = await startBroker(mock);
  const client = await connect(restarted.socket);
  const restored = await client.request("broker/threads/watch", { threadId: thread.id }) as { run: Run };
  expect(restored.run.upstreamError).toEqual(snapshot.run.upstreamError);
  expect(restored.run.diagnosticRevision).toBe(snapshot.run.diagnosticRevision);
  registerMockPids();
}, 45_000);

test("上游报错：无错误运行不新增报错字段，进展诊断依冻结契约保存", async () => {
  const mock = mockWithErrors();
  const { socket } = await startBroker(mock);
  const client = await connect(socket);
  const notices = collect(client);
  const { thread } = await client.request("thread/start", {}) as { thread: { id: string } };
  const started = await client.request("turn/start", { threadId: thread.id, input: [{ type: "text", text: "正常" }] }) as { runId: string };
  await waitFor(() => notices.some((notice) => notice.method === "broker/run/ended" && notice.params?.run?.runId === started.runId));
  expect(notices.filter((notice) => notice.method === "broker/run/updated")).toHaveLength(0);
  const snapshot = await client.request("broker/threads/watch", { threadId: thread.id }) as { run: Run };
  expect(snapshot.run).not.toHaveProperty("upstreamError");
  expect(snapshot.run.diagnosticRevision).toBeGreaterThan(0);
  expect(snapshot.run.lastProgressAt).toBeGreaterThan(0);
  await waitFor(() => records().some((run) => run.runId === started.runId && run.state === "ended"));
  expect(records().find((run) => run.runId === started.runId)).not.toHaveProperty("upstreamError");
  const summaries = await client.request("broker/runs/modelSummaries", { threadIds: [thread.id] }) as { runs: Run[] };
  expect(summaries.runs.find((run) => run.runId === started.runId)).not.toHaveProperty("upstreamError");
  registerMockPids();
}, 30_000);
