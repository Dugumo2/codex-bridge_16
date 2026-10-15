import { afterEach, beforeEach, expect, test } from "bun:test";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import type { Subprocess } from "bun";
import {
  createMockCodex, endpointFor, exitsWithin, spawnBroker, TestClient, testSocketPath,
  waitFor, waitForSocket,
} from "./test-support/broker-harness";

// 上游报错第二批 C1：真实 broker 的无进展期限、访问报错即停与停止诊断（契约第 2、3、4.3、6 节）。

type Stall = { timeoutMs: number; phase: string; idleMs: number; verify: string | null; expiries: number };
type StopDiagnostics = {
  reason: string; interruptSent: boolean; interruptSentAt: number | null; interruptReply: string | null;
  turnEndedAt: number | null; unconfirmedAt: number | null; confirmedAt: number | null;
};
type Run = {
  runId: string; state: string; status: string | null; interruptReason: string | null;
  stopOutcome?: string | null; stall?: Stall; stopDiagnostics?: StopDiagnostics; lastProgressAt?: number;
  upstreamError?: { category: string; willRetry: boolean | null; count: number } | null;
  diagnosticRevision?: number;
};
type Update = Run & { diagnosticRevision: number; childUpstreamErrors: unknown[] };
type Notice = { method?: string; params?: Update & { run?: Run } };
type Timed = { at: number; notice: Notice };

const STALL_MS = 1_500;

let tempDir: string;
let home: string;
let spawnedProcesses: Subprocess[];
let clients: TestClient[];
let mockPids: Set<number>;

beforeEach(() => {
  tempDir = mkdtempSync(join(process.env.TEMP ?? tmpdir(), "cxb-stall-"));
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
  registerMockPids();
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

function registerMockPids(): void {
  const path = join(tempDir, "mock-pids.txt");
  if (existsSync(path)) {
    for (const pid of readFileSync(path, "utf8").trim().split(/\s+/).filter(Boolean).map(Number)) mockPids.add(pid);
  }
}

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

/**
 * 在临时模拟脚本里按输入文字「无进展:<场景>」切换上游行为（只改本例副本，复用握手与状态读取夹具）：
 * - 重试／认证：每 300 毫秒一条 error（willRetry:true；认证为 401）；
 * - 输出：每 500 毫秒一条非空 agentMessage 增量，共 6 条后静默；
 * - 审批：开轮后发一条命令审批请求；
 * - 不确认：同重试，但 interrupt 只回包、不发 turn/completed（状态读取仍报进行中）。
 * 除「不确认」外，interrupt 回包后立即发 turn/completed(interrupted)。
 */
function stallMock(): string {
  const mock = createMockCodex(tempDir, { backgroundTerminals: { enabled: true }, statusRead: { historyMode: "paged" }, sendTurnCompleted: false });
  const script = join(mock, process.platform === "win32" ? "codex-mock.ts" : "codex");
  const original = readFileSync(script, "utf8");
  const startMarker = 'case "turn/start": {\n        const threadId = msg.params?.threadId || "thread-001";';
  const interruptMarker = 'case "turn/interrupt":\n        appendFileSync(';
  expect(original.split(startMarker)).toHaveLength(2);
  expect(original.split(interruptMarker)).toHaveLength(2);
  const start = `
        const scenario = msg.params?.input?.[0]?.text ?? "";
        if (scenario.startsWith("无进展:")) {
          const mode = scenario.slice("无进展:".length);
          const timers = [];
          globalThis.stallScenario = { mode, timers };
          respond({ id: msg.id, result: { turn: { id: "turn-001", items: [], status: "inProgress", error: null } } });
          respond({ method: "turn/started", params: { threadId, turn: { id: "turn-001", items: [], status: "inProgress", error: null } } });
          if (mode === "重试" || mode === "认证" || mode === "不确认") {
            const info = { responseStreamDisconnected: { httpStatusCode: mode === "认证" ? 401 : 503 } };
            timers.push(setInterval(() => respond({ method: "error", params: { threadId, turnId: "turn-001", willRetry: true,
              error: { message: "Reconnecting...", codexErrorInfo: info } } }), 300));
          }
          if (mode === "输出") {
            let count = 0;
            const timer = setInterval(() => {
              count++;
              respond({ method: "item/agentMessage/delta", params: { threadId, turnId: "turn-001", itemId: "msg-1", delta: "片段" + count } });
              if (count >= 6) clearInterval(timer);
            }, 500);
            timers.push(timer);
          }
          if (mode === "审批") {
            setTimeout(() => respond({ id: "stall-approval", method: "item/commandExecution/requestApproval",
              params: { threadId, turnId: "turn-001", itemId: "cmd-1", command: "echo hi", cwd: "/tmp" } }), 100);
          }
          break;
        }
  `;
  const interrupt = `
        if (globalThis.stallScenario) {
          for (const timer of globalThis.stallScenario.timers) clearInterval(timer);
          respond({ id: msg.id, result: {} });
          if (globalThis.stallScenario.mode !== "不确认") {
            respond({ method: "turn/completed", params: { threadId: msg.params?.threadId,
              turn: { id: "turn-001", items: [], status: "interrupted", error: null } } });
          }
          break;
        }
  `;
  const pidFile = join(tempDir, "mock-pids.txt");
  writeFileSync(script, `import {appendFileSync} from "node:fs"; appendFileSync(${JSON.stringify(pidFile)}, String(process.pid) + "\\n");\n`
    + original.replace(/^#![^\n]*\n/, "").replace(startMarker, startMarker + start).replace(interruptMarker, 'case "turn/interrupt":' + interrupt + "\n        appendFileSync("));
  return mock;
}

async function startBroker(): Promise<string> {
  const socket = testSocketPath(tempDir);
  const profile = join(tempDir, "profile");
  mkdirSync(profile, { recursive: true });
  spawnBroker({ tempDir, spawnedProcesses }, endpointFor(socket), stallMock(), {
    cwd: tempDir, idleTimeout: 120_000,
    env: { CXB_HOME: home, USERPROFILE: profile, HOME: profile, TEMP: process.env.TEMP ?? tmpdir(), TMP: process.env.TMP ?? tmpdir() },
  });
  await waitForSocket(socket, 15_000);
  return socket;
}

async function connect(socket: string): Promise<{ client: TestClient; notices: Timed[] }> {
  const client = await TestClient.connectAndInit(socket, { cxb: true });
  clients.push(client);
  const notices: Timed[] = [];
  client.onNotification((message) => notices.push({ at: performance.now(), notice: message as Notice }));
  return { client, notices };
}

type Scenario = { client: TestClient; notices: Timed[]; runId: string; startedAt: number };

async function startScenario(mode: string, policy: { stallTimeoutMs: number; upstreamErrorPolicy: string }): Promise<Scenario> {
  const socket = await startBroker();
  const { client, notices } = await connect(socket);
  const { thread } = await client.request("thread/start", {}) as { thread: { id: string } };
  const startedAt = performance.now();
  const started = await client.request("turn/start", {
    threadId: thread.id, input: [{ type: "text", text: `无进展:${mode}` }], cxbRunPolicy: policy,
  }) as { runId: string };
  return { client, notices, runId: started.runId, startedAt };
}

function updates(s: Scenario): Array<{ at: number; params: Update }> {
  return s.notices
    .filter(({ notice }) => notice.method === "broker/run/updated" && notice.params?.runId === s.runId)
    .map(({ at, notice }) => ({ at, params: notice.params! }));
}

function firstPhase(s: Scenario, phase: string): { at: number; params: Update } | undefined {
  return updates(s).find((u) => u.params.stall?.phase === phase);
}

async function ended(s: Scenario, timeoutMs = 20_000): Promise<Run> {
  try {
    await waitFor(() => s.notices.some(({ notice }) => notice.method === "broker/run/ended" && notice.params?.run?.runId === s.runId), timeoutMs);
  } catch (error) {
    console.error("调试通知", JSON.stringify(s.notices.map(({ at, notice }) => ({ t: Math.round(at - s.startedAt), m: notice.method, p: notice.method === "broker/run/updated" ? notice.params : undefined }))));
    throw error;
  }
  return s.notices.find(({ notice }) => notice.method === "broker/run/ended" && notice.params?.run?.runId === s.runId)!.notice.params!.run!;
}

function expectRevisionsIncrease(s: Scenario): void {
  const revisions = updates(s).map((u) => u.params.diagnosticRevision);
  expect(revisions.length).toBeGreaterThan(0);
  for (let index = 1; index < revisions.length; index++) expect(revisions[index]).toBeGreaterThan(revisions[index - 1]!);
}

test("无进展：持续重试报错不算进展，到期核实后停止并记录诊断", async () => {
  const s = await startScenario("重试", { stallTimeoutMs: STALL_MS, upstreamErrorPolicy: "observe" });
  const run = await ended(s);
  expect(run).toMatchObject({ status: "interrupted", interruptReason: "stallTimeout" });
  const verifying = firstPhase(s, "verifying")!;
  // 重试报错持续到达，期限不被推后：约 1.5 秒到期。
  expect(verifying.at - s.startedAt).toBeGreaterThanOrEqual(STALL_MS - 100);
  expect(verifying.at - s.startedAt).toBeLessThan(STALL_MS + 2_500);
  expect(verifying.params.stall).toMatchObject({ timeoutMs: STALL_MS, verify: null, expiries: 1 });
  const stopping = firstPhase(s, "stopping")!;
  expect(stopping.params.stall?.verify).toBe("inProgress");
  expect(stopping.params.upstreamError).toMatchObject({ category: "server", willRetry: true });
  const last = updates(s).at(-1)!.params;
  expect(last.stopDiagnostics).toMatchObject({ reason: "stallTimeout", interruptSent: true, interruptReply: "ok" });
  expect(last.stopDiagnostics?.confirmedAt).toBeGreaterThan(0);
  expect(last.childUpstreamErrors).toEqual([]);
  expectRevisionsIncrease(s);
  await waitFor(() => records().some((r) => r.runId === s.runId && r.state === "ended"), 10_000);
  const record = records().find((r) => r.runId === s.runId)!;
  expect(record.stall).toMatchObject({ phase: "stopping", verify: "inProgress" });
  expect(record.stopDiagnostics?.reason).toBe("stallTimeout");
}, 40_000);

test("无进展：非空输出增量延长期限，静默后才到期", async () => {
  const s = await startScenario("输出", { stallTimeoutMs: STALL_MS, upstreamErrorPolicy: "observe" });
  const run = await ended(s);
  expect(run.interruptReason).toBe("stallTimeout");
  const verifying = firstPhase(s, "verifying")!;
  // 最后一条增量约在第 3 秒，之后再静默 1.5 秒才到期。
  expect(verifying.at - s.startedAt).toBeGreaterThanOrEqual(3_000 + STALL_MS - 300);
  expect(updates(s).filter((u) => u.params.stall?.phase === "verifying")).toHaveLength(1);
}, 40_000);

test("无进展：等待审批时不计时，回答后从回答时刻重新计", async () => {
  const s = await startScenario("审批", { stallTimeoutMs: STALL_MS, upstreamErrorPolicy: "observe" });
  let approval: Record<string, unknown> | undefined;
  s.client.onRequest((message) => { approval = message; });
  await waitFor(() => approval !== undefined, 10_000);
  await Bun.sleep(STALL_MS * 2);
  expect(updates(s).filter((u) => u.params.stall)).toHaveLength(0);
  const answeredAt = performance.now();
  s.client.send({ id: approval!.id, result: { decision: "accept" } });
  const run = await ended(s);
  expect(run.interruptReason).toBe("stallTimeout");
  const verifying = firstPhase(s, "verifying")!;
  expect(verifying.at - answeredAt).toBeGreaterThanOrEqual(STALL_MS - 100);
}, 40_000);

test("访问报错即停：stopAccess 遇 401 停止本次运行", async () => {
  const s = await startScenario("认证", { stallTimeoutMs: 0, upstreamErrorPolicy: "stopAccess" });
  const run = await ended(s, 10_000);
  expect(run).toMatchObject({ status: "interrupted", interruptReason: "upstreamAccessError" });
  expect(updates(s).some((u) => u.params.stall)).toBe(false);
  const last = updates(s).at(-1)!.params;
  expect(last.stopDiagnostics?.reason).toBe("upstreamAccessError");
}, 40_000);

test("访问报错即停：observe（默认）只提示不停", async () => {
  const observe = await startScenario("认证", { stallTimeoutMs: 0, upstreamErrorPolicy: "observe" });
  await waitFor(() => updates(observe).some((u) => u.params.upstreamError?.count === 3), 10_000);
  expect(observe.notices.some(({ notice }) => notice.method === "broker/run/ended")).toBe(false);
  expect(updates(observe).every((u) => u.params.state === "running" && !u.params.stopDiagnostics)).toBe(true);
}, 40_000);

test("无进展：interrupt 只回包不完成时停止未确认，停止诊断可区分阶段", async () => {
  const s = await startScenario("不确认", { stallTimeoutMs: STALL_MS, upstreamErrorPolicy: "observe" });
  await waitFor(() => updates(s).some((u) => u.params.stopOutcome === "unconfirmed"), 60_000);
  const unconfirmed = updates(s).find((u) => u.params.stopOutcome === "unconfirmed")!.params;
  expect(unconfirmed).toMatchObject({ state: "stopping", interruptReason: "stallTimeout" });
  expect(unconfirmed.stopDiagnostics).toMatchObject({ reason: "stallTimeout", interruptSent: true, interruptReply: "ok", turnEndedAt: null, confirmedAt: null });
  expect(unconfirmed.stopDiagnostics?.unconfirmedAt).toBeGreaterThan(0);
  expect(s.notices.some(({ notice }) => notice.method === "broker/run/ended")).toBe(false);
  expectRevisionsIncrease(s);
}, 90_000);
