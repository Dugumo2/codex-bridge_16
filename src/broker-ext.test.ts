/**
 * 这里放 Rust 扩展的用例：M1–M5 的线路、认领、路由、停止、存储，M6 的托管交接、inbox、watch、续跑等场景。
 * TS 分支精确断言上游行为，Rust 分支精确断言扩展协议。
 */

import { describe, expect, test, beforeAll, beforeEach, afterEach } from "bun:test";
import net from "node:net";
import { mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import type { Subprocess } from "bun";
import {
  BROKER_BIN,
  createMockCodex, endpointFor, exitsWithin, removeDirWithRetry, TestClient, testSocketPath, writeMockCodexScript,
  waitFor, waitForSocket, spawnBroker as spawnBrokerWithContext, serveMockOverSocket as serveMockOverSocketWithContext,
} from "./test-support/broker-harness";
import { waitForProcessGone } from "./test-support/process-identity";

console.log(`[broker 扩展测试] 被测可执行文件=${BROKER_BIN}`);

let tempDir: string;

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), "broker-ext-test-"));
});

afterEach(async () => {
  // kill 仅发送信号；等待全部进程退出后再清理临时目录。
  for (const proc of spawnedProcesses) {
    try { proc.kill(); } catch {}
  }
  await Promise.all(spawnedProcesses.map((p) => p.exited.catch(() => undefined)));
  spawnedProcesses.length = 0;
  await removeDirWithRetry(tempDir);
});

const spawnedProcesses: Subprocess[] = [];
const harnessContext = {
  get tempDir() { return tempDir; },
  spawnedProcesses,
};
const spawnBroker = spawnBrokerWithContext.bind(null, harnessContext);
const serveMockOverSocket = serveMockOverSocketWithContext.bind(null, harnessContext);

// 采用与 broker-server 测试相同的绑定探测和显式跳过规则。
const IS_UNIX = process.platform !== "win32";
const ALLOW_SOCKET_SKIP = process.env.CXB_ALLOW_SOCKET_SKIP === "1";
let socketBindError: unknown;
const SOCKETS_AVAILABLE = await (async () => {
  const checkDir = mkdtempSync(join(tmpdir(), "broker-ext-sock-check-"));
  const testSock = testSocketPath(checkDir);
  try {
    const srv = net.createServer();
    await new Promise<void>((resolve, reject) => {
      srv.on("error", reject);
      srv.listen(testSock, () => { srv.close(); resolve(); });
    });
    return true;
  } catch (error) {
    socketBindError = error;
    return false;
  } finally {
    try { rmSync(checkDir, { recursive: true, force: true }); } catch {}
  }
})();

if (!SOCKETS_AVAILABLE && ALLOW_SOCKET_SKIP) {
  console.warn(
    `\n⚠️  broker-ext 测试组已跳过：当前环境无法绑定${IS_UNIX ? " Unix 套接字" : " Windows 命名管道"}（沙箱或文件系统限制）。` +
    `\n   本次运行没有覆盖被测 broker；请在允许绑定的环境中重新运行后再确认结果。\n`,
  );
}

describe.skipIf(!SOCKETS_AVAILABLE && ALLOW_SOCKET_SKIP)("broker-ext", () => {
  beforeAll(() => {
    if (!SOCKETS_AVAILABLE) {
      throw new Error(
        `当前环境无法绑定${IS_UNIX ? " Unix 套接字" : " Windows 命名管道"}，broker 扩展测试无法运行（沙箱或文件系统限制）。如需显式允许跳过，请设置 CXB_ALLOW_SOCKET_SKIP=1。`,
        { cause: socketBindError },
      );
    }
  });

  // 假 app-server：initialize 延迟 delayMs 才回包；其余请求回空结果。
  const slowHandshakeMock = (delayMs: number) => `
const decoder = new TextDecoder();
let buf = "";
const send = (m: unknown) => process.stdout.write(JSON.stringify(m) + "\\n");
for await (const chunk of Bun.stdin.stream()) {
  buf += decoder.decode(chunk);
  let i: number;
  while ((i = buf.indexOf("\\n")) !== -1) {
    const line = buf.slice(0, i).trim();
    buf = buf.slice(i + 1);
    if (!line) continue;
    const msg = JSON.parse(line);
    if (msg.id === undefined) continue;
    if (msg.method === "initialize") {
      await new Promise((r) => setTimeout(r, ${delayMs}));
      send({ id: msg.id, result: { userAgent: "slow-mock" } });
    } else {
      send({ id: msg.id, result: {} });
    }
  }
}
`;

  test("M1：app-server 握手慢时，早连上的客户端的 initialize 排队到握手完成后才回包", async () => {
    const sockPath = testSocketPath(tempDir);
    const endpoint = endpointFor(sockPath);
    writeMockCodexScript(tempDir, slowHandshakeMock(1500));
    const started = Date.now();
    const proc = spawnBroker(endpoint, tempDir);
    await waitForSocket(sockPath);
    const socketReadyAfter = Date.now() - started;
    try {
      const client = await TestClient.connect(sockPath);
      try {
        const result = await client.request("initialize", {
          clientInfo: { name: "test", title: null, version: "0.0.1" },
          capabilities: {},
        }) as { userAgent: string };
        expect(result.userAgent).toBe("cxb-broker");
        // 要等握手完成后才回 initialize。
        expect(Date.now() - started).toBeGreaterThanOrEqual(1400);
        // 先抢管道名再握手（详细设计 0.1、2.1 节）：管道立刻可连。
        expect(socketReadyAfter).toBeLessThan(1400);
      } finally {
        await client.close();
      }
    } finally {
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  test("M1：app-server 握手失败时 broker 以非零码退出，早连上的客户端被断开", async () => {
    const sockPath = testSocketPath(tempDir);
    const endpoint = endpointFor(sockPath);
    writeMockCodexScript(tempDir, `process.stderr.write("mock app-server 启动失败\\n"); process.exit(3);\n`);
    const proc = spawnBroker(endpoint, tempDir);

    // 先占管道名：握手失败之前客户端就能连上，broker 退出时连接随之断开。
    await waitForSocket(sockPath);
    const earlyClient: TestClient | null = await TestClient.connect(sockPath);
    const outcome = await Promise.race([
      proc.exited.then((code) => ({ code })),
      new Promise<null>((r) => setTimeout(() => r(null), 15_000)),
    ]);
    expect(outcome).not.toBeNull();
    expect(outcome!.code).not.toBe(0);
    if (earlyClient) {
      const deadline = Date.now() + 3000;
      while (!earlyClient.destroyed && Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, 20));
      }
      expect(earlyClient.destroyed).toBe(true);
      const stderr = await new Response(proc.stderr as ReadableStream).text();
      expect(stderr).toContain("连接 Codex app-server 失败");
    }
  }, 30_000);

  // ─── M2：同一连接上的请求流水化（详细设计 1.2 节，有意偏离上游） ───
  // 上游每个连接一个 Promise 队列，前一个请求等到 app-server 回包才处理下一个；
  // Rust 按顺序处理、但不等回包。下面三个用例分别精确断言两种实现，记录这处语义变化。

  type Settled =
    | { ok: true; value: unknown }
    | { ok: false; error: Error & { code?: number } };

  /** 记下各请求的完成先后，成功和失败都算完成。 */
  const settleInto = (order: string[], name: string, p: Promise<unknown>): Promise<Settled> =>
    p.then(
      (value) => { order.push(name); return { ok: true as const, value }; },
      (error: Error & { code?: number }) => { order.push(name); return { ok: false as const, error }; },
    );

  // 假 app-server：thread/resume 延迟 resumeDelayMs 才回包（用定时器，不阻塞后续消息的读取）；
  // 按收到、回复的先后记事件，供 mock/state 查询。
  const delayedResumeMock = (resumeDelayMs: number) => `
const events = [];
let buf = "";
const send = (m) => process.stdout.write(JSON.stringify(m) + "\\n");
process.stdin.setEncoding("utf-8");
process.stdin.on("data", (chunk) => {
  buf += chunk;
  let i;
  while ((i = buf.indexOf("\\n")) !== -1) {
    const line = buf.slice(0, i).trim();
    buf = buf.slice(i + 1);
    if (!line) continue;
    let msg;
    try { msg = JSON.parse(line); } catch { continue; }
    if (msg.id === undefined || msg.method === undefined) continue;
    if (msg.method !== "mock/state") events.push("recv:" + msg.method);
    switch (msg.method) {
      case "initialize":
        send({ id: msg.id, result: { userAgent: "delayed-resume-mock" } });
        break;
      case "thread/resume":
        setTimeout(() => {
          events.push("reply:thread/resume");
          send({ id: msg.id, result: {
            thread: { id: msg.params.threadId, preview: "", cwd: "/tmp", turns: [] },
            model: "mock-model", cwd: "/tmp",
          }});
        }, ${resumeDelayMs});
        break;
      case "turn/start": {
        const threadId = msg.params.threadId;
        send({ id: msg.id, result: { turn: { id: "turn-b1", items: [], status: "inProgress", error: null } } });
        setTimeout(() => {
          send({ method: "turn/completed", params: { threadId, turn: { id: "turn-b1", items: [], status: "completed", error: null } } });
        }, 20);
        break;
      }
      case "mock/state":
        send({ id: msg.id, result: { events } });
        break;
      default:
        send({ id: msg.id, result: {} });
    }
  }
});
process.stdin.on("end", () => process.exit(0));
`;

  test("M2 流水化：同一连接连发两个同线程 turn/start，第一个 turn 在回包前已结束", async () => {
    const sockPath = testSocketPath(tempDir);
    const endpoint = endpointFor(sockPath);
    // turn/completed 先于 turn/start 回包写出，回包再晚 300 ms；每个 turn 只有这一条完成通知。
    const mockDir = createMockCodex(tempDir, {
      completeBeforeResponse: true,
      sendTurnCompleted: false,
      turnDelay: 300,
    });
    const proc = spawnBroker(endpoint, mockDir);
    await waitForSocket(sockPath);
    try {
      const client = await TestClient.connectAndInit(sockPath);
      try {
        const params = { threadId: "thread-pipe-a", input: [{ type: "text", text: "go" }] };
        const order: string[] = [];
        // 不等回包，两条请求连着写出。
        const first = settleInto(order, "first", client.request("turn/start", params));
        const second = settleInto(order, "second", client.request("turn/start", params));
        const [r1, r2] = await Promise.all([first, second]);
        expect(r1.ok).toBe(true);
        // 第二个到达时线程仍被第一个占着，立即回 -32001，先于第一个的回包。
        expect(order).toEqual(["second", "first"]);
        expect(r2.ok).toBe(false);
        const error = (r2 as { ok: false; error: Error & { code?: number } }).error;
        expect(error.code).toBe(-32001);
        expect(error.message).toBe("A turn is already running on this thread.");
        // 不再追加第三轮：这个 mock 每轮都用 turn-001，后续轮次的完成通知会被当作迟到的重复通知丢弃（#13）。
      } finally {
        await client.close();
      }
    } finally {
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  test("M2 流水化：resume 回包延迟时 turn/start 不会先发出，同连接的其他请求不被堵住", async () => {
    const sockPath = testSocketPath(tempDir);
    const endpoint = endpointFor(sockPath);
    writeMockCodexScript(tempDir, delayedResumeMock(400));
    const proc = spawnBroker(endpoint, tempDir);
    await waitForSocket(sockPath);
    try {
      const client = await TestClient.connectAndInit(sockPath);
      try {
        const order: string[] = [];
        const start = settleInto(order, "turn/start", client.request("turn/start", {
          threadId: "thread-pipe-b",
          input: [{ type: "text", text: "go" }],
        }));
        const list = settleInto(order, "model/list", client.request("model/list", {}));
        const [rs, rl] = await Promise.all([start, list]);
        expect(rs.ok).toBe(true);
        expect(rl.ok).toBe(true);

        const { events } = await client.request("mock/state", {}) as { events: string[] };
        // 两种实现都有依赖屏障：未订阅的线程先 resume，resume 回包之后才发 turn/start（#2）。
        const recvResume = events.indexOf("recv:thread/resume");
        const replyResume = events.indexOf("reply:thread/resume");
        const recvStart = events.indexOf("recv:turn/start");
        expect(recvResume).toBeGreaterThanOrEqual(0);
        expect(replyResume).toBeGreaterThan(recvResume);
        expect(recvStart).toBeGreaterThan(replyResume);

        const recvList = events.indexOf("recv:model/list");
        // 屏障只挂在这个认领上，model/list 在 resume 等待期间就已转发并回包。
        expect(order).toEqual(["model/list", "turn/start"]);
        expect(recvList).toBeGreaterThan(recvResume);
        expect(recvList).toBeLessThan(replyResume);
      } finally {
        await client.close();
      }
    } finally {
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  test("M2 流水化：同一连接 turn/start 未回包时发 broker/shutdown{ifIdle}", async () => {
    const sockPath = testSocketPath(tempDir);
    const endpoint = endpointFor(sockPath);
    // turn/start 500 ms 后才回包，之后不发完成通知：turn 一直在跑。
    const mockDir = createMockCodex(tempDir, { turnDelay: 500, sendTurnCompleted: false });
    const proc = spawnBroker(endpoint, mockDir);
    await waitForSocket(sockPath);
    try {
      const client = await TestClient.connectAndInit(sockPath);
      try {
        const order: string[] = [];
        const sentAt = Date.now();
        let shutdownSettledAfter = -1;
        const start = settleInto(order, "turn/start", client.request("turn/start", {
          threadId: "thread-pipe-c",
          input: [{ type: "text", text: "go" }],
        }));
        const shutdown = settleInto(order, "shutdown", client.request("broker/shutdown", { ifIdle: true }))
          .then((r) => { shutdownSettledAfter = Date.now() - sentAt; return r; });
        const [rs, rd] = await Promise.all([start, shutdown]);
        expect(rs.ok).toBe(true);
        // 拒绝：线程有认领，broker 不空闲。
        expect(rd.ok).toBe(false);
        const error = (rd as { ok: false; error: Error & { code?: number } }).error;
        expect(error.code).toBe(-32001);
        expect(error.message).toBe("A turn is running on this broker.");
        // shutdown 不等 turn/start 回包，先于它被拒。只断言先后，不设耗时上限，免得机器繁忙时误报。
        expect(order).toEqual(["shutdown", "turn/start"]);
        // 被拒后 broker 继续运行。
        expect(await exitsWithin(proc, 300)).toBe(false);
      } finally {
        await client.close();
      }
    } finally {
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  // ─── M3：服务端请求路由（详细设计 4.2 节） ───
  // 假 app-server：turn/start 开一个一直跑的 turn；mock/ask{threadId, method} 让它向 broker 发一个服务端请求；
  // broker 回给它的每一条回包（或拒绝）都按 JSON 行追加到 answers.log，同一请求回了几次就记几条，
  // 断言按请求 id 计数，才能查出重复回包。收到的 turn/interrupt 记到 answers.log.events。
  // 用文件而不是 mock/state，是因为关闭用例里 app-server 会被 broker 关掉，事后只能读文件。
  const serverRequestMock = (logPath: string) => `
import { appendFileSync } from "node:fs";
let buf = "";
let next = 1;
const asked = new Map();
const send = (m) => process.stdout.write(JSON.stringify(m) + "\\n");
process.stdin.setEncoding("utf-8");
process.stdin.on("data", (chunk) => {
  buf += chunk;
  let i;
  while ((i = buf.indexOf("\\n")) !== -1) {
    const line = buf.slice(0, i).trim();
    buf = buf.slice(i + 1);
    if (!line) continue;
    let msg;
    try { msg = JSON.parse(line); } catch { continue; }
    if (msg.id === undefined) continue;
    if (msg.method === undefined) {
      const method = asked.get(String(msg.id));
      if (method !== undefined) {
        appendFileSync(${JSON.stringify(logPath)}, JSON.stringify({ id: msg.id, method, result: msg.result ?? null, error: msg.error ?? null }) + "\\n");
      }
      continue;
    }
    switch (msg.method) {
      case "initialize":
        send({ id: msg.id, result: { userAgent: "server-request-mock" } });
        break;
      case "thread/resume":
        send({ id: msg.id, result: { thread: { id: msg.params.threadId, preview: "", cwd: "/tmp", turns: [] }, model: "mock-model", cwd: "/tmp" } });
        break;
      case "turn/start": {
        const threadId = msg.params.threadId;
        send({ id: msg.id, result: { turn: { id: "turn-1", items: [], status: "inProgress", error: null } } });
        send({ method: "turn/started", params: { threadId, turn: { id: "turn-1", items: [], status: "inProgress", error: null } } });
        break;
      }
      case "turn/interrupt":
        appendFileSync(${JSON.stringify(logPath + ".events")}, JSON.stringify({ method: msg.method, params: msg.params }) + "\\n");
        send({ id: msg.id, result: {} });
        break;
      case "mock/ask": {
        const id = "srv-" + (next++);
        const method = msg.params.method;
        asked.set(id, method);
        send({ id, method, params: { threadId: msg.params.threadId, turnId: "turn-1", itemId: "item-" + id, command: "echo hi", cwd: "/tmp" } });
        send({ id: msg.id, result: { asked: id } });
        break;
      }
      default:
        send({ id: msg.id, result: {} });
    }
  }
});
process.stdin.on("end", () => process.exit(0));
`;

  type Answer = { id: string; method: string; result: unknown; error: { code: number; message: string } | null };
  const readAnswers = (logPath: string): Answer[] => {
    try {
      return readFileSync(logPath, "utf-8").split("\n").filter(Boolean).map((l) => JSON.parse(l) as Answer);
    } catch {
      return [];
    }
  };
  const APPROVAL = "item/commandExecution/requestApproval";
  const countFor = (answers: Answer[], id: string) => answers.filter((a) => a.id === id).length;

  test("M3 路由：只有原目标连接的回包有效，别的连接冒用转发 id 的回包被丢弃", async () => {
    const sockPath = testSocketPath(tempDir);
    const logPath = join(tempDir, "answers.log");
    writeMockCodexScript(tempDir, serverRequestMock(logPath));
    const proc = spawnBroker(endpointFor(sockPath), tempDir);
    await waitForSocket(sockPath);
    try {
      const owner = await TestClient.connectAndInit(sockPath);
      const other = await TestClient.connectAndInit(sockPath);
      try {
        const forwarded: Array<Record<string, unknown>> = [];
        owner.onRequest((msg) => forwarded.push(msg));
        await owner.request("turn/start", { threadId: "thread-m3a", input: [{ type: "text", text: "go" }] });
        await other.request("mock/ask", { threadId: "thread-m3a", method: APPROVAL });
        await waitFor(() => forwarded.length === 1);
        const forwardedId = forwarded[0].id;
        expect(forwarded[0].method).toBe(APPROVAL);
        expect(String(forwardedId)).toMatch(/^broker-\d+-[0-9a-z]+$/);

        // 别的连接拿到转发 id 也不能替 owner 回答。
        other.send({ id: forwardedId, result: { decision: "decline" } });
        await new Promise((r) => setTimeout(r, 300));
        expect(readAnswers(logPath)).toEqual([]);

        owner.send({ id: forwardedId, result: { decision: "accept" } });
        // owner 重复回答同一个转发：第二次已无挂起项，只记告警，不能再回给 app-server。
        owner.send({ id: forwardedId, result: { decision: "accept" } });
        await waitFor(() => readAnswers(logPath).length >= 1);
        await new Promise((r) => setTimeout(r, 300));
        const answers = readAnswers(logPath);
        expect(countFor(answers, "srv-1")).toBe(1);
        expect(answers).toHaveLength(1);
        expect(answers[0].method).toBe(APPROVAL);
        expect(answers[0].result).toEqual({ decision: "accept" });
      } finally {
        await owner.close();
        await other.close();
      }
    } finally {
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  test("M3 路由：broker 关闭时，挂起的转发以 -32603「Broker shutting down」回给 app-server", async () => {
    const sockPath = testSocketPath(tempDir);
    const logPath = join(tempDir, "answers.log");
    writeMockCodexScript(tempDir, serverRequestMock(logPath));
    const proc = spawnBroker(endpointFor(sockPath), tempDir);
    await waitForSocket(sockPath);
    const owner = await TestClient.connectAndInit(sockPath);
    try {
      const forwarded: Array<Record<string, unknown>> = [];
      owner.onRequest((msg) => forwarded.push(msg));
      await owner.request("turn/start", { threadId: "thread-m3b", input: [{ type: "text", text: "go" }] });
      await owner.request("mock/ask", { threadId: "thread-m3b", method: APPROVAL });
      await waitFor(() => forwarded.length === 1);

      // owner 不回答，直接让 broker 关闭（不带 ifIdle）。
      await owner.request("broker/shutdown", {});
      expect(await exitsWithin(proc, 10_000)).toBe(true);
      const answers = readAnswers(logPath);
      // 关闭时先写完已排队的出站消息再关 stdin（M1 rpc 的正常关闭），拒绝一定送达。
      // 文本照上游：处理函数抛错的 -32603 带「Handler error: 」前缀（rpc.ts 296–297 行）。
      expect(countFor(answers, "srv-1")).toBe(1);
      expect(answers).toHaveLength(1);
      expect(answers[0].result).toBeNull();
      expect(answers[0].error).toEqual({ code: -32603, message: "Handler error: Broker shutting down" });
    } finally {
      await owner.close();
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  // 独立的 owner 进程：连上 broker、initialize、开一轮 turn，收到 turn/start 回包后打印 READY，然后一直挂着。
  // 用单独进程是为了能可靠地「断开」：测试结束这个进程，操作系统一定会关闭它持有的管道句柄，
  // broker 一定读到断开。同进程里的 bun 客户端 destroy() 在 Windows 命名管道上不一定真正关掉句柄
  // （bun 修过同类问题，oven-sh/bun#30150），用它制造断开时 broker 有时察觉不到。
  // 第三个参数为 sent 时，turn/start 一写出就打印 READY，不等回包（用于「start 还没发出就断开」）。
  const ownerScript = `
import net from "node:net";
const [pipe, threadId, mode] = process.argv.slice(2);
const sock = net.connect({ path: pipe });
let buf = "";
const send = (m) => sock.write(JSON.stringify(m) + "\\n");
sock.setEncoding("utf8");
sock.on("data", (chunk) => {
  buf += chunk;
  let i;
  while ((i = buf.indexOf("\\n")) !== -1) {
    const line = buf.slice(0, i).trim();
    buf = buf.slice(i + 1);
    if (!line) continue;
    const msg = JSON.parse(line);
    if ((msg.id === 1 || msg.id === 2) && msg.error !== undefined) {
      // 握手或开工失败：把原因交给测试，直接退出，不打印 READY。
      process.stderr.write("owner 请求失败：" + JSON.stringify(msg) + "\\n");
      process.exit(1);
    }
    if (msg.id === 1) {
      send({ method: "initialized" });
      send({ id: 2, method: "turn/start", params: { threadId, input: [{ type: "text", text: "go" }] } });
      if (mode === "sent") process.stdout.write("READY\\n");
    } else if (msg.id === 2 && mode !== "sent") {
      process.stdout.write("READY\\n");
    }
  }
});
sock.on("connect", () => send({ id: 1, method: "initialize", params: { clientInfo: { name: "owner", title: null, version: "0.0.1" }, capabilities: {} } }));
setInterval(() => {}, 1000);
`;

  /** 启动 owner 进程，等到它的 turn 已经开工（打印 READY）；最多等 10 秒，超时或提前退出就结束它并带上 stderr 报错。 */
  const spawnOwner = async (
    dir: string, sockPath: string, threadId: string, mode: "replied" | "sent" = "replied",
  ): Promise<Subprocess> => {
    const ownerPath = join(dir, "owner.ts");
    writeFileSync(ownerPath, ownerScript);
    const owner = Bun.spawn(["bun", "run", ownerPath, sockPath, threadId, mode], { stdout: "pipe", stderr: "pipe", windowsHide: true });
    spawnedProcesses.push(owner);
    const decoder = new TextDecoder();
    const readyOrEnd = (async () => {
      let out = "";
      for await (const chunk of owner.stdout as ReadableStream<Uint8Array>) {
        out += decoder.decode(chunk);
        if (out.includes("READY")) return true;
      }
      return false;
    })();
    const timedOut = new Promise<false>((resolve) => setTimeout(() => resolve(false), 10_000));
    if (!(await Promise.race([readyOrEnd, timedOut]))) {
      owner.kill();
      await owner.exited;
      const stderr = await new Response(owner.stderr as ReadableStream).text();
      throw new Error(`owner 进程没有就绪（超时或提前退出）：${stderr}`);
    }
    return owner;
  };

  test("M3 路由：owner 断开后才到的服务端请求，审批以「No active client…」拒绝，其他请求回 -32601", async () => {
    const sockPath = testSocketPath(tempDir);
    const logPath = join(tempDir, "answers.log");
    writeMockCodexScript(tempDir, serverRequestMock(logPath));
    const proc = spawnBroker(endpointFor(sockPath), tempDir);
    await waitForSocket(sockPath);
    try {
      const owner = await spawnOwner(tempDir, sockPath, "thread-m3c");
      const other = await TestClient.connectAndInit(sockPath);
      try {
        // owner 断开：结束它的进程。turn 还在跑（mock 不发完成通知），线程仍被占着，但已没人接收。
        owner.kill();
        await owner.exited;
        // 等 broker 处理完断开再发请求（这是就绪等待，不是重试）：停止操作先读 goal，再对正在跑的
        // turn 发 turn/interrupt（mock 记到 .events），中断只可能由断开处理触发。
        await waitFor(() => {
          try { return readFileSync(logPath + ".events", "utf-8").includes("turn/interrupt"); } catch { return false; }
        }, 5000);

        await other.request("mock/ask", { threadId: "thread-m3c", method: APPROVAL });
        await other.request("mock/ask", { threadId: "thread-m3c", method: "item/tool/requestUserInput" });
        await waitFor(() => readAnswers(logPath).length >= 2, 8000).catch(() => undefined);
        await new Promise((r) => setTimeout(r, 300));
        const answers = readAnswers(logPath);
        // 每个请求恰好回一次。
        expect(countFor(answers, "srv-1")).toBe(1);
        expect(countFor(answers, "srv-2")).toBe(1);
        const byMethod = new Map(answers.map((a) => [a.method, a]));
        // 审批 fail-closed（4.2 节第 5 条，上游 1234–1246 行）；-32603 带「Handler error: 」前缀（rpc.ts 296–297 行）。
        expect(byMethod.get(APPROVAL)?.error).toEqual({
          code: -32603,
          message: "Handler error: No active client to forward approval request",
        });
        // 其他类回 -32601（上游 onAnyRequest，758–760 行）。
        expect(byMethod.get("item/tool/requestUserInput")?.error).toEqual({
          code: -32601,
          message: "Method not found: item/tool/requestUserInput",
        });
      } finally {
        await other.close();
      }
    } finally {
      proc.kill();
      await proc.exited;
    }
  }, 30_000);

  // ─── M4：停止操作与断开即停（详细设计 3.3 节「停止操作」、迁移表 #11、#19、#20、#28 系列） ───
  // 假 app-server：可配置 goal 状态、暂停或清除的延迟与失败、中断后是否报完成、goal 活跃时的自动续跑。
  // 收到的每个请求、goal 变更的回包、turn 的开始和结束都按 JSON 行记到 logPath，断言按记录的先后。
  // turn/start 先回包、后发 turn/started，与真实 Codex 一致（openai/codex#41888 实测）。
  // 需要制造时序交错的地方不用定时器：hold 里列出的动作先扣住，测试发 mock/release{what} 才放行，
  // mock 先写出被扣住的消息、再回 release 的回包，所以 release 返回时 broker 已经按顺序读到了那条消息。
  // 放行之后，同类动作不再扣住（例如之后的 thread/resume 立即回包）。
  type HoldPoint = "pause" | "clear" | "resume" | "continuation";
  interface StopMockOptions {
    backgroundTerminals?: import("./test-support/background-terminals-mock").BackgroundTerminalsMockOptions;
    goal?: { status: "active" | "paused" | "blocked" } | null;
    goalFeatureDisabled?: boolean;
    pauseFails?: boolean;
    clearFails?: boolean;
    /** 收到针对当前 turn 的中断后是否报 interrupted 完成，默认是。 */
    interruptCompletes?: boolean;
    /** turn 结束时 goal 仍活跃，最多自动续跑几轮，默认 0。 */
    continuations?: number;
    /** 扣住这些动作，等 mock/release 放行：pause、clear 是 goal 变更的回包，resume 是 thread/resume 的回包，continuation 是自动续跑。 */
    hold?: HoldPoint[];
    /** turn/start 先宣布 turn，再回错误；turn 继续跑（#11）。 */
    startedThenError?: boolean;
  }
  const stopMock = (logPath: string, opts: StopMockOptions = {}) => `
import { appendFileSync } from "node:fs";
// CXB_INLINE_BACKGROUND_TERMINALS_BEGIN
import { createBackgroundTerminalsMock } from "./background-terminals-mock";
// CXB_INLINE_BACKGROUND_TERMINALS_END
const O = ${JSON.stringify(opts)};
const LOG = ${JSON.stringify(logPath)};
const log = (e) => appendFileSync(LOG, JSON.stringify(e) + "\\n");
const send = (m) => process.stdout.write(JSON.stringify(m) + "\\n");
const background = createBackgroundTerminalsMock(send, O.backgroundTerminals);
const heldActions = {};
const releasedPoints = new Set();
const gate = (what, f) => {
  if ((O.hold ?? []).includes(what) && !releasedPoints.has(what)) {
    (heldActions[what] ??= []).push(f);
    log({ ev: "held", what });
  } else {
    f();
  }
};
let goal = O.goal ?? null;
let turnSeq = 0;
let continuationsLeft = O.continuations ?? 0;
const running = new Map();
const asked = new Map();
let nextAsk = 1;
const goalObj = (threadId) => goal ? {
  threadId, objective: "mock objective", status: goal.status, tokenBudget: null,
  tokensUsed: 0, timeUsedSeconds: 0, createdAt: 1, updatedAt: 2,
} : null;
const turnObj = (id, status) => ({ id, items: [], status, error: null });
function announce(threadId, id) {
  send({ method: "turn/started", params: { threadId, turn: turnObj(id, "inProgress") } });
  log({ ev: "started", turnId: id });
}
function complete(threadId, status) {
  const id = running.get(threadId);
  if (!id) return;
  running.delete(threadId);
  send({ method: "turn/completed", params: { threadId, turn: turnObj(id, status) } });
  log({ ev: "completed", turnId: id, status });
  if (goal && goal.status === "active" && continuationsLeft > 0) {
    continuationsLeft--;
    gate("continuation", () => {
      if (!goal || goal.status !== "active" || running.has(threadId)) return;
      const next = "turn-" + (++turnSeq);
      running.set(threadId, next);
      announce(threadId, next);
    });
  }
}
let buf = "";
process.stdin.setEncoding("utf-8");
process.stdin.on("data", (chunk) => {
  buf += chunk;
  let i;
  while ((i = buf.indexOf("\\n")) !== -1) {
    const line = buf.slice(0, i).trim();
    buf = buf.slice(i + 1);
    if (!line) continue;
    let msg;
    try { msg = JSON.parse(line); } catch { continue; }
    if (msg.id === undefined) continue;
    if (msg.method === undefined) {
      const method = asked.get(String(msg.id));
      if (method !== undefined) log({ ev: "answer", id: msg.id, method, result: msg.result ?? null, error: msg.error ?? null });
      continue;
    }
    const p = msg.params ?? {};
    if (msg.method !== "initialize") log({ ev: "recv", method: msg.method, params: p });
    if (background.handle(msg)) continue;
    switch (msg.method) {
      case "initialize":
        send({ id: msg.id, result: { userAgent: "stop-mock" } });
        break;
      case "thread/resume":
        gate("resume", () => {
          log({ ev: "reply", method: "thread/resume" });
          send({ id: msg.id, result: { thread: { id: p.threadId, preview: "", cwd: "/tmp", turns: [] }, model: "mock-model", cwd: "/tmp" } });
        });
        break;
      case "turn/start": {
        const id = "turn-" + (++turnSeq);
        running.set(p.threadId, id);
        if (O.startedThenError) {
          announce(p.threadId, id);
          send({ id: msg.id, error: { code: -32000, message: "mock start failed after announcing the turn" } });
        } else {
          send({ id: msg.id, result: { turn: turnObj(id, "inProgress") } });
          announce(p.threadId, id);
        }
        break;
      }
      case "turn/interrupt":
        send({ id: msg.id, result: {} });
        if (O.interruptCompletes !== false && running.get(p.threadId) === p.turnId) {
          setTimeout(() => { if (running.get(p.threadId) === p.turnId) complete(p.threadId, "interrupted"); }, 20);
        }
        break;
      case "thread/goal/get":
        if (O.goalFeatureDisabled) send({ id: msg.id, error: { code: -32600, message: "goals feature is disabled" } });
        else send({ id: msg.id, result: { goal: goalObj(p.threadId) } });
        break;
      case "thread/goal/set":
        gate("pause", () => {
          if (O.pauseFails) {
            log({ ev: "reply", method: "thread/goal/set", ok: false });
            send({ id: msg.id, error: { code: -32000, message: "mock pause failed" } });
            return;
          }
          if (goal && p.status) goal.status = p.status;
          log({ ev: "reply", method: "thread/goal/set", ok: true });
          send({ id: msg.id, result: { goal: goalObj(p.threadId) } });
        });
        break;
      case "thread/goal/clear":
        gate("clear", () => {
          if (O.clearFails) {
            log({ ev: "reply", method: "thread/goal/clear", ok: false });
            send({ id: msg.id, error: { code: -32000, message: "mock clear failed" } });
            return;
          }
          goal = null;
          log({ ev: "reply", method: "thread/goal/clear", ok: true });
          send({ id: msg.id, result: {} });
        });
        break;
      case "thread/turns/list": {
        const id = running.get(p.threadId);
        send({ id: msg.id, result: { data: id ? [turnObj(id, "inProgress")] : [], nextCursor: null, backwardsCursor: null } });
        break;
      }
      case "thread/read": {
        const id = running.get(p.threadId);
        send({ id: msg.id, result: { thread: {
          id: p.threadId, preview: "", cwd: "/tmp", status: { type: id ? "active" : "idle" },
          turns: id ? [turnObj(id, "inProgress")] : [],
        } } });
        break;
      }
      case "mock/release": {
        const actions = heldActions[p.what] ?? [];
        delete heldActions[p.what];
        releasedPoints.add(p.what);
        for (const f of actions) f();
        log({ ev: "released", what: p.what, count: actions.length });
        send({ id: msg.id, result: { released: actions.length } });
        break;
      }
      case "mock/finish":
        complete(p.threadId, "completed");
        send({ id: msg.id, result: {} });
        break;
      case "mock/ask": {
        const id = "srv-" + (nextAsk++);
        asked.set(id, p.method);
        send({ id, method: p.method, params: { threadId: p.threadId, turnId: running.get(p.threadId) ?? null, itemId: "item-" + id, command: "echo hi", cwd: "/tmp" } });
        send({ id: msg.id, result: { asked: id } });
        break;
      }
      default:
        send({ id: msg.id, result: {} });
    }
  }
});
process.stdin.on("end", () => process.exit(0));
`;

  type StopLog = { ev: string; what?: string; method?: string; params?: Record<string, unknown>; turnId?: string; status?: string; ok?: boolean; id?: string; result?: unknown; error?: { code: number; message: string } | null };
  const readStopLog = (logPath: string): StopLog[] => {
    try {
      return readFileSync(logPath, "utf-8").split("\n").filter(Boolean).map((l) => JSON.parse(l) as StopLog);
    } catch {
      return [];
    }
  };
  const recvs = (log: StopLog[], method: string) => log.filter((e) => e.ev === "recv" && e.method === method);
  const indexOfRecv = (log: StopLog[], method: string) => log.findIndex((e) => e.ev === "recv" && e.method === method);

  /** 启动带停止 mock 的 broker，返回连接地址和日志路径。 */
  const startStopBroker = async (opts: StopMockOptions = {}, dir = tempDir) => {
    const sockPath = testSocketPath(dir);
    const logPath = join(dir, "mock.log");
    writeMockCodexScript(dir, stopMock(logPath, { backgroundTerminals: { enabled: true }, ...opts }));
    const proc = spawnBroker(endpointFor(sockPath), dir);
    await waitForSocket(sockPath);
    return { sockPath, logPath, proc };
  };

  /** 不经 TestClient 的 10 秒超时直接发请求，返回请求 id；用 replyFor 查有没有回包。stop 最多要等 40 秒才回。 */
  let rawId = 10_000;
  const sendRaw = (client: TestClient, method: string, params: unknown): number => {
    const id = rawId++;
    client.send({ id, method, params });
    return id;
  };
  const replyFor = (client: TestClient, id: number) =>
    client.messages.find((m) => m.id === id && !("method" in m)) as
      | { id: number; result?: Record<string, unknown>; error?: { code: number; message: string; data?: Record<string, unknown> } }
      | undefined;

  const expectBusy = async (client: TestClient, threadId: string) => {
    const error = await client.request("turn/start", { threadId, input: [{ type: "text", text: "next" }] })
      .catch((e: unknown) => e) as Error & { code?: number };
    expect(error.code).toBe(-32001);
  };

  test("M4 stop：运行中的 turn 先读 goal 再中断，Codex 报完成后回 confirmed 并释放", async () => {
    const { sockPath, logPath, proc } = await startStopBroker();
    const client = await TestClient.connectAndInit(sockPath);
    try {
      await client.request("turn/start", { threadId: "thread-s1", input: [{ type: "text", text: "go" }] });
      await waitFor(() => readStopLog(logPath).some((e) => e.ev === "started"));
      const reply = await client.request("broker/threads/stop", { threadId: "thread-s1" }) as Record<string, unknown>;
      expect(reply).toMatchObject({
        threadId: "thread-s1", interruptSent: true, goal: "none", alreadyStopped: false, stopOutcome: "confirmed",
      });
      expect(String(reply.runId)).toMatch(/^run-/);
      const log = readStopLog(logPath);
      // 先读 goal、再中断（kill.ts 191 行）；中断带 threadId 和 turnId。
      expect(indexOfRecv(log, "thread/goal/get")).toBeGreaterThanOrEqual(0);
      expect(indexOfRecv(log, "thread/goal/get")).toBeLessThan(indexOfRecv(log, "turn/interrupt"));
      expect(recvs(log, "turn/interrupt").map((e) => e.params)).toEqual([{ threadId: "thread-s1", turnId: "turn-1" }]);
      expect(recvs(log, "thread/goal/set")).toEqual([]);
      // 已确认停下：线程空出，可以开新一轮。
      await client.request("turn/start", { threadId: "thread-s1", input: [{ type: "text", text: "again" }] });
    } finally {
      await client.close();
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  test("M4 stop：goal 活跃时先暂停、再中断，回 goal paused", async () => {
    const { sockPath, logPath, proc } = await startStopBroker({ goal: { status: "active" } });
    const client = await TestClient.connectAndInit(sockPath);
    try {
      await client.request("turn/start", { threadId: "thread-s2", input: [{ type: "text", text: "go" }] });
      await waitFor(() => readStopLog(logPath).some((e) => e.ev === "started"));
      const reply = await client.request("broker/threads/stop", { threadId: "thread-s2" }) as Record<string, unknown>;
      expect(reply).toMatchObject({ interruptSent: true, goal: "paused", alreadyStopped: false, stopOutcome: "confirmed" });
      const log = readStopLog(logPath);
      const get = indexOfRecv(log, "thread/goal/get");
      const set = indexOfRecv(log, "thread/goal/set");
      const interrupt = indexOfRecv(log, "turn/interrupt");
      expect(get).toBeGreaterThanOrEqual(0);
      expect(set).toBeGreaterThan(get);
      expect(interrupt).toBeGreaterThan(set);
      expect(recvs(log, "thread/goal/set")[0].params).toEqual({ threadId: "thread-s2", status: "paused" });
    } finally {
      await client.close();
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  test("M4 stop：goal 功能不可用按没有 goal 处理，照常中断并确认", async () => {
    const { sockPath, logPath, proc } = await startStopBroker({ goalFeatureDisabled: true });
    const client = await TestClient.connectAndInit(sockPath);
    try {
      await client.request("turn/start", { threadId: "thread-s3", input: [{ type: "text", text: "go" }] });
      await waitFor(() => readStopLog(logPath).some((e) => e.ev === "started"));
      const reply = await client.request("broker/threads/stop", { threadId: "thread-s3" }) as Record<string, unknown>;
      expect(reply).toMatchObject({ interruptSent: true, goal: "none", stopOutcome: "confirmed" });
    } finally {
      await client.close();
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  test("M4 stop：turn 在暂停回包之前结束时不释放，暂停回来才确认，也不再中断已结束的 turn", async () => {
    const { sockPath, logPath, proc } = await startStopBroker({ goal: { status: "active" }, hold: ["pause"] });
    const client = await TestClient.connectAndInit(sockPath);
    const other = await TestClient.connectAndInit(sockPath);
    try {
      await client.request("turn/start", { threadId: "thread-s4", input: [{ type: "text", text: "go" }] });
      await waitFor(() => readStopLog(logPath).some((e) => e.ev === "started"));
      const stopId = sendRaw(client, "broker/threads/stop", { threadId: "thread-s4" });
      await waitFor(() => readStopLog(logPath).some((e) => e.ev === "held" && e.what === "pause"));
      // 暂停被扣住，turn 自己结束了。mock/finish 的回包排在 completed 之后，回来时 broker 已读到 completed。
      await other.request("mock/finish", { threadId: "thread-s4" });
      await waitFor(() => readStopLog(logPath).some((e) => e.ev === "completed"));
      // goal 制动还没确认（判定第 3、4 项），线程仍被占着，stop 也还没回。
      await expectBusy(other, "thread-s4");
      expect(replyFor(client, stopId)).toBeUndefined();
      expect(await other.request("mock/release", { what: "pause" })).toEqual({ released: 1 });
      await waitFor(() => replyFor(client, stopId) !== undefined, 8000);
      expect(replyFor(client, stopId)!.result).toMatchObject({
        interruptSent: false, goal: "paused", alreadyStopped: false, stopOutcome: "confirmed",
      });
      // 规则 f：已结束的 turn 不再发中断。
      expect(recvs(readStopLog(logPath), "turn/interrupt")).toEqual([]);
      await other.request("turn/start", { threadId: "thread-s4", input: [{ type: "text", text: "again" }] });
    } finally {
      await client.close();
      await other.close();
      proc.kill();
      await proc.exited;
    }
  }, 30_000);

  test("M4 stop：暂停 goal 失败时，Codex 自动续跑的下一轮也被中断，线程不释放", async () => {
    const { sockPath, logPath, proc } = await startStopBroker({
      goal: { status: "active" }, pauseFails: true, continuations: 1,
    });
    const client = await TestClient.connectAndInit(sockPath);
    const other = await TestClient.connectAndInit(sockPath);
    try {
      await client.request("turn/start", { threadId: "thread-s5", input: [{ type: "text", text: "go" }] });
      await waitFor(() => readStopLog(logPath).some((e) => e.ev === "started"));
      const stopId = sendRaw(client, "broker/threads/stop", { threadId: "thread-s5" });
      // turn-1 被中断后 goal 仍活跃，Codex 续跑 turn-2；broker 对新 turn 再中断一次（换代，步骤 3）。
      await waitFor(() => {
        const ids = recvs(readStopLog(logPath), "turn/interrupt").map((e) => e.params?.turnId);
        return ids.includes("turn-1") && ids.includes("turn-2");
      }, 8000);
      await waitFor(() => readStopLog(logPath).filter((e) => e.ev === "completed").length === 2);
      // goal 一直没制住：不算停下，线程保持占用，stop 不回 confirmed。
      await expectBusy(other, "thread-s5");
      expect(replyFor(client, stopId)).toBeUndefined();
    } finally {
      await client.close();
      await other.close();
      proc.kill();
      await proc.exited;
    }
  }, 30_000);

  test("M4 stop：Codex 一直不报完成时，重复 stop 真正重发中断，期间 ifIdle 关闭被拒", async () => {
    const { sockPath, logPath, proc } = await startStopBroker({ interruptCompletes: false });
    const client = await TestClient.connectAndInit(sockPath);
    try {
      await client.request("turn/start", { threadId: "thread-s6", input: [{ type: "text", text: "go" }] });
      await waitFor(() => readStopLog(logPath).some((e) => e.ev === "started"));
      const first = sendRaw(client, "broker/threads/stop", { threadId: "thread-s6" });
      await waitFor(() => recvs(readStopLog(logPath), "turn/interrupt").length === 1);
      const second = sendRaw(client, "broker/threads/stop", { threadId: "thread-s6" });
      // 中断回包成功不等于停下，第二次 stop 要真正再发一次。
      await waitFor(() => recvs(readStopLog(logPath), "turn/interrupt").length === 2);
      expect(recvs(readStopLog(logPath), "turn/interrupt").map((e) => e.params)).toEqual([
        { threadId: "thread-s6", turnId: "turn-1" },
        { threadId: "thread-s6", turnId: "turn-1" },
      ]);
      expect(replyFor(client, first)).toBeUndefined();
      expect(replyFor(client, second)).toBeUndefined();
      const error = await client.request("broker/shutdown", { ifIdle: true }).catch((e: unknown) => e) as Error & { code?: number };
      expect(error.code).toBe(-32001);
      expect(error.message).toBe("A turn is running on this broker.");
    } finally {
      await client.close();
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  test("M4 stop：线程空闲时带 clear，paused、blocked 的 goal 都清除；清除失败报 unchanged", async () => {
    const cases: Array<{ opts: StopMockOptions; goal: string; clears: number }> = [
      { opts: { goal: { status: "paused" } }, goal: "cleared", clears: 1 },
      { opts: { goal: { status: "blocked" } }, goal: "cleared", clears: 1 },
      { opts: { goal: { status: "paused" }, clearFails: true }, goal: "unchanged", clears: 1 },
      { opts: { goal: null }, goal: "none", clears: 0 },
    ];
    for (const [n, c] of cases.entries()) {
      const dir = mkdtempSync(join(tempDir, `case-${n}-`));
      const { sockPath, logPath, proc } = await startStopBroker(c.opts, dir);
      const client = await TestClient.connectAndInit(sockPath);
      try {
        const reply = await client.request("broker/threads/stop", { threadId: "thread-s7", clear: true }) as Record<string, unknown>;
        expect(reply).toMatchObject({
          threadId: "thread-s7", runId: null, interruptSent: false, goal: c.goal, alreadyStopped: true, stopOutcome: "confirmed",
        });
        const clears = recvs(readStopLog(logPath), "thread/goal/clear");
        if (c.opts.clearFails) expect(clears.length).toBeGreaterThanOrEqual(c.clears);
        else expect(clears).toHaveLength(c.clears);
        expect(recvs(readStopLog(logPath), "turn/interrupt")).toEqual([]);
      } finally {
        await client.close();
        proc.kill();
        await proc.exited;
      }
    }
  }, 40_000);

  test("M4 stop：只剩空闲线程上的清除在途时，ifIdle 关闭被拒；清除回来后才放行", async () => {
    const { sockPath, logPath, proc } = await startStopBroker({ goal: { status: "paused" }, hold: ["clear"] });
    const client = await TestClient.connectAndInit(sockPath);
    try {
      const stopId = sendRaw(client, "broker/threads/stop", { threadId: "thread-s8", clear: true });
      await waitFor(() => readStopLog(logPath).some((e) => e.ev === "held" && e.what === "clear"));
      // 没有任何认领，只有一个未完成的停止操作：仍算忙（第 5 节、第 8 节空闲判定）。
      const error = await client.request("broker/shutdown", { ifIdle: true }).catch((e: unknown) => e) as Error & { code?: number };
      expect(error.code).toBe(-32001);
      expect(await client.request("mock/release", { what: "clear" })).toEqual({ released: 1 });
      await waitFor(() => replyFor(client, stopId) !== undefined, 8000);
      expect(replyFor(client, stopId)!.result).toMatchObject({ goal: "cleared", alreadyStopped: true, stopOutcome: "confirmed" });
      await client.request("broker/shutdown", { ifIdle: true });
      expect(await exitsWithin(proc, 10_000)).toBe(true);
    } finally {
      await client.close();
      proc.kill();
      await proc.exited;
    }
  }, 30_000);

  test("M4 stop：joined 的线程拒绝停止，回 -32000 reason joined，不中断", async () => {
    const { sockPath, logPath, proc } = await startStopBroker();
    const client = await TestClient.connectAndInit(sockPath);
    try {
      await client.request("turn/start", { threadId: "thread-s9", input: [{ type: "text", text: "go" }] });
      await waitFor(() => readStopLog(logPath).some((e) => e.ev === "started"));
      await client.request("broker/joined", { threadId: "thread-s9", turnId: "turn-1" });
      const stopId = sendRaw(client, "broker/threads/stop", { threadId: "thread-s9" });
      await waitFor(() => replyFor(client, stopId) !== undefined);
      const reply = replyFor(client, stopId)!;
      expect(reply.error?.code).toBe(-32000);
      expect(reply.error?.data?.reason).toBe("joined");
      expect(recvs(readStopLog(logPath), "turn/interrupt")).toEqual([]);
      expect(recvs(readStopLog(logPath), "thread/goal/set")).toEqual([]);
    } finally {
      await client.close();
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  test("M4 断开即停：两轮之间断开不空等，暂停 goal 后立即释放，Codex 不再自动续跑", async () => {
    const { sockPath, logPath, proc } = await startStopBroker({
      goal: { status: "active" }, continuations: 1, hold: ["pause", "continuation"],
    });
    try {
      const owner = await spawnOwner(tempDir, sockPath, "thread-d1");
      const other = await TestClient.connectAndInit(sockPath);
      try {
        // broker 从 goal/get 的回包学到 goal 活跃（线程级事实，任何连接的流量都算，测试 25 同理）。
        await other.request("thread/goal/get", { threadId: "thread-d1" });
        // 第一轮自然结束，goal 仍活跃、owner 在线：进入两轮之间（#14）。自动续跑被扣住，由测试放行。
        await other.request("mock/finish", { threadId: "thread-d1" });
        await waitFor(() => readStopLog(logPath).some((e) => e.ev === "completed"));
        owner.kill();
        await owner.exited;
        // 断开处理的可观察信号：broker 发出暂停，暂停回包被扣住（就绪等待，不是重试）。
        await waitFor(() => readStopLog(logPath).some((e) => e.ev === "held" && e.what === "pause"), 5000);
        // 放行暂停回包：release 的回包排在它后面，回来时 broker 已处理完暂停回包、按判定释放了线程。
        expect(await other.request("mock/release", { what: "pause" })).toEqual({ released: 1 });
        expect(recvs(readStopLog(logPath), "thread/goal/set")[0].params).toEqual({ threadId: "thread-d1", status: "paused" });
        // 放行自动续跑：goal 已暂停，Codex 不再开下一轮。此时线程上没有任何 turn，mock 不会因「已有 turn」而跳过续跑，
        // 所以没有新 started 才能证明是暂停起了作用。
        expect(await other.request("mock/release", { what: "continuation" })).toEqual({ released: 1 });
        expect(readStopLog(logPath).filter((e) => e.ev === "started").map((e) => e.turnId)).toEqual(["turn-1"]);
        // 不空等 40 秒：暂停确认后线程马上可用；两轮之间没有 turn，不发中断。
        await other.request("turn/start", { threadId: "thread-d1", input: [{ type: "text", text: "next" }] });
        // mock 先回 turn/start、再宣布 turn-2，等日志里出现 turn-2 再断言完整列表。
        await waitFor(() => readStopLog(logPath).some((e) => e.ev === "started" && e.turnId === "turn-2"));
        expect(recvs(readStopLog(logPath), "turn/interrupt")).toEqual([]);
        expect(readStopLog(logPath).filter((e) => e.ev === "started").map((e) => e.turnId)).toEqual(["turn-1", "turn-2"]);
      } finally {
        await other.close();
      }
    } finally {
      proc.kill();
      await proc.exited;
    }
  }, 30_000);

  test("M4 断开即停：start 还在等订阅恢复时断开，start 不再发出，线程随后可用", async () => {
    const { sockPath, logPath, proc } = await startStopBroker({ hold: ["resume"] });
    try {
      const owner = await spawnOwner(tempDir, sockPath, "thread-d2", "sent");
      await waitFor(() => recvs(readStopLog(logPath), "thread/resume").length === 1);
      owner.kill();
      await owner.exited;
      // 断开处理的可观察信号：停止操作先读 goal（第 3 节 b）。
      await waitFor(() => recvs(readStopLog(logPath), "thread/goal/get").length >= 1, 5000);
      const other = await TestClient.connectAndInit(sockPath);
      try {
        // 放行订阅恢复的回包；release 的回包排在它后面，回来时 broker 已读到 resume 回包。
        expect(await other.request("mock/release", { what: "resume" })).toEqual({ released: 1 });
        // 订阅恢复回来之后也不补发：被取消的 start 不会到 Codex。若 broker 错发，它会排在下面这个
        // turn/start 之前写给 mock，所以只数到一个 turn/start、且参数是 fresh，就证明没有补发。
        await other.request("turn/start", { threadId: "thread-d2", input: [{ type: "text", text: "fresh" }] });
        const starts = recvs(readStopLog(logPath), "turn/start");
        expect(starts).toHaveLength(1);
        expect(starts[0].params?.input).toEqual([{ type: "text", text: "fresh" }]);
      } finally {
        await other.close();
      }
    } finally {
      proc.kill();
      await proc.exited;
    }
  }, 30_000);

  test("M4 #11：已宣布 turn 的 start 失败后，broker 停掉这一轮，审批不再转给仍在线的原 owner", async () => {
    const { sockPath, logPath, proc } = await startStopBroker({ startedThenError: true });
    const owner = await TestClient.connectAndInit(sockPath);
    try {
      const forwarded: Array<Record<string, unknown>> = [];
      owner.onRequest((msg) => forwarded.push(msg));
      const error = await owner.request("turn/start", { threadId: "thread-e1", input: [{ type: "text", text: "go" }] })
        .catch((e: unknown) => e) as Error & { code?: number };
      expect(error.message).toBe("mock start failed after announcing the turn");
      // 有 turn 在跑却没人接收：发起停止，按 turn_id 中断。
      await waitFor(() => recvs(readStopLog(logPath), "turn/interrupt").length === 1, 5000);
      expect(recvs(readStopLog(logPath), "turn/interrupt")[0].params).toEqual({ threadId: "thread-e1", turnId: "turn-1" });
    } finally {
      await owner.close();
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  test("M4 #11：停止期间来的审批 fail-closed，不转给原 owner", async () => {
    // 中断后 Codex 不报完成：停止一直未确认，这期间的审批要 fail-closed（14.2 第 2 条）。
    const { sockPath, logPath, proc } = await startStopBroker({ startedThenError: true, interruptCompletes: false });
    const owner = await TestClient.connectAndInit(sockPath);
    try {
      const forwarded: Array<Record<string, unknown>> = [];
      owner.onRequest((msg) => forwarded.push(msg));
      await owner.request("turn/start", { threadId: "thread-e2", input: [{ type: "text", text: "go" }] }).catch(() => undefined);
      await waitFor(() => recvs(readStopLog(logPath), "turn/interrupt").length === 1, 5000);
      await owner.request("mock/ask", { threadId: "thread-e2", method: APPROVAL });
      await waitFor(() => readStopLog(logPath).some((e) => e.ev === "answer"), 5000);
      const answers = readStopLog(logPath).filter((e) => e.ev === "answer");
      expect(answers).toHaveLength(1);
      expect(answers[0].error).toEqual({ code: -32603, message: "Handler error: No active client to forward approval request" });
      expect(forwarded).toEqual([]);
    } finally {
      await owner.close();
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  // ─── M5：存储、结果与运行记录（详细设计 3.6 节、第 7 节、第 5 节第 4、5 步；S1-评审第 20 节） ───
  // 假 app-server：turn/start 先回包、后宣布；mock/finish 用给定的 items 和 status 结束 turn（readItems 是之后条目分页返回的 items；
  // 给 itemsView 时 completed 带该标记，模拟上游摘要完成通知）；mock/item 发条目通知；
  // thread/items/list 按 turnId 分页补读；thread/read 保留给停止核实。readMode 控制完整条目、空历史或失败。
  // broker 的状态文件写在每个用例自己的 CXB_HOME 里，断言直接读 runs/*.json 的 payload。
  /** 两个模拟器共用 0.160.0 条目页形状；可扣住回包以确定性验证“结束先于结果”。 */
  const resultPagingMock = `
const heldItemReads = [];
function listItems(msg, turns) {
  const p = msg.params;
  const prefix = "items:" + p.turnId + ":";
  const offset = p.cursor == null ? 0 : String(p.cursor).startsWith(prefix) ? Number(p.cursor.slice(prefix.length)) : NaN;
  if (!Number.isSafeInteger(offset) || offset < 0 || p.sortDirection !== "asc") {
    send({ id: msg.id, error: { code: -32600, message: "条目分页参数无效" } });
    return;
  }
  const items = turns.find((turn) => turn.id === p.turnId)?.items ?? [];
  const end = Math.min(offset + Math.min(p.limit ?? 64, O.itemsPageSize ?? 64), items.length);
  const response = { id: msg.id, result: {
    data: items.slice(offset, end).map((item) => ({ turnId: p.turnId, item, startedAtMs: null, completedAtMs: null })),
    nextCursor: end < items.length ? prefix + end : null,
    backwardsCursor: end > offset ? prefix + offset : null,
  } };
  if (O.holdItemReads) heldItemReads.push(response);
  else send(response);
}
function releaseItems(msg) {
  O.holdItemReads = false;
  for (const response of heldItemReads.splice(0)) send(response);
  send({ id: msg.id, result: {} });
}
`;
  interface RunsMockOptions {
    backgroundTerminals?: import("./test-support/background-terminals-mock").BackgroundTerminalsMockOptions;
    itemsPageSize?: number;
    holdItemReads?: boolean;
    readMode?: "items" | "empty" | "fail";
    /** 新进程不知道上一个进程的 turn，重启用例用它让分页补读返回指定轮次的条目。 */
    restoredTurns?: { id: string; items: unknown[] }[];
    /** 延迟回 thread/unsubscribe（毫秒），让认领释放后的自动退订保持在途。 */
    unsubscribeDelayMs?: number;
  }
  const runsMock = (logPath: string, opts: RunsMockOptions = {}) => `
import { appendFileSync } from "node:fs";
// CXB_INLINE_BACKGROUND_TERMINALS_BEGIN
import { createBackgroundTerminalsMock } from "./background-terminals-mock";
// CXB_INLINE_BACKGROUND_TERMINALS_END
const O = ${JSON.stringify(opts)};
const LOG = ${JSON.stringify(logPath)};
const log = (e) => appendFileSync(LOG, JSON.stringify(e) + "\\n");
const send = (m) => process.stdout.write(JSON.stringify(m) + "\\n");
const background = createBackgroundTerminalsMock(send, O.backgroundTerminals);
let turnSeq = 0;
${resultPagingMock}
const running = new Map();
const finished = new Map();
const turnObj = (id, status, items) => ({ id, items: items ?? [], status, error: null });
function finish(threadId, status, items, readItems, itemsView) {
  const id = running.get(threadId);
  if (!id) return;
  running.delete(threadId);
  (finished.get(threadId) ?? finished.set(threadId, []).get(threadId)).push({ id, items: readItems ?? items ?? [], view: itemsView });
  send({ method: "turn/completed", params: { threadId, turn: { ...turnObj(id, status, items), ...(itemsView ? { itemsView } : {}) } } });
  log({ ev: "completed", turnId: id, status });
}
let buf = "";
process.stdin.setEncoding("utf-8");
process.stdin.on("data", (chunk) => {
  buf += chunk;
  let i;
  while ((i = buf.indexOf("\\n")) !== -1) {
    const line = buf.slice(0, i).trim();
    buf = buf.slice(i + 1);
    if (!line) continue;
    let msg;
    try { msg = JSON.parse(line); } catch { continue; }
    if (msg.id === undefined || msg.method === undefined) continue;
    const p = msg.params ?? {};
    if (msg.method !== "initialize") log({ ev: "recv", method: msg.method, params: p });
    if (background.handle(msg)) continue;
    switch (msg.method) {
      case "initialize":
        send({ id: msg.id, result: { userAgent: "runs-mock" } });
        break;
      case "thread/start":
        send({ id: msg.id, result: { thread: { id: "thread-new", preview: "hello", cwd: "/work", turns: [] }, model: "mock-model", cwd: "/work" } });
        break;
      case "thread/resume":
        send({ id: msg.id, result: { thread: { id: p.threadId, preview: "", cwd: "/work", turns: [] }, model: "mock-model", cwd: "/work" } });
        break;
      case "turn/start": {
        const id = "turn-" + (++turnSeq);
        running.set(p.threadId, id);
        send({ id: msg.id, result: { turn: turnObj(id, "inProgress") } });
        send({ method: "turn/started", params: { threadId: p.threadId, turn: turnObj(id, "inProgress") } });
        log({ ev: "started", turnId: id });
        break;
      }
      case "turn/interrupt":
        send({ id: msg.id, result: {} });
        if (running.get(p.threadId) === p.turnId) setTimeout(() => finish(p.threadId, "interrupted"), 20);
        break;
      case "thread/goal/get":
        send({ id: msg.id, result: { goal: null } });
        break;
      case "thread/items/list":
        if (O.readMode === "fail") send({ id: msg.id, error: { code: -32603, message: "模拟分页读取失败" } });
        else listItems(msg, O.readMode === "empty" ? [] : [...(O.restoredTurns ?? []), ...(finished.get(p.threadId) ?? [])]);
        break;
      case "mock/releaseItems":
        releaseItems(msg);
        break;
      case "thread/turns/list": {
        const id = running.get(p.threadId);
        send({ id: msg.id, result: { data: id ? [turnObj(id, "inProgress")] : [], nextCursor: null, backwardsCursor: null } });
        break;
      }
      case "thread/read": {
        if (O.readMode === "fail") {
          send({ id: msg.id, error: { code: -32603, message: "mock read failed" } });
          break;
        }
        const id = running.get(p.threadId);
        const turns = O.readMode === "empty" ? [] : [
          ...(O.restoredTurns ?? []).map((t) => turnObj(t.id, "completed", t.items)),
          ...(finished.get(p.threadId) ?? []).map((t) => ({ ...turnObj(t.id, "completed", t.items), ...(t.view ? { itemsView: "full" } : {}) })),
          ...(id ? [turnObj(id, "inProgress")] : []),
        ];
        send({ id: msg.id, result: { thread: { id: p.threadId, preview: "", cwd: "/work", status: { type: id ? "active" : "idle" }, turns } } });
        break;
      }
      case "mock/item":
        send({ method: p.method, params: { threadId: p.threadId, turnId: running.get(p.threadId) ?? null, item: p.item } });
        send({ id: msg.id, result: {} });
        break;
      case "thread/unsubscribe":
        setTimeout(() => send({ id: msg.id, result: {} }), O.unsubscribeDelayMs ?? 0);
        break;
      case "mock/finish":
        finish(p.threadId, p.status ?? "completed", p.items, p.readItems, p.itemsView);
        send({ id: msg.id, result: {} });
        break;
      default:
        send({ id: msg.id, result: {} });
    }
  }
});
process.stdin.on("end", () => process.exit(0));
`;

  type RunFile = {
    runId: string; threadId: string; shortId: string; state: string; status: string | null; input: string;
    interruptReason: string | null; stopOutcome: string | null; endedAt: number | null;
    continuedFrom?: string | null; meta?: unknown;
    progress: { done: { itemId: string; type: string; summary: string }[]; doneTotal: number; unconfirmed: { itemId: string }[]; observedFiles: string[] };
    turns: { turnId: string; ready: string; status: string; output: string; outputSource: string }[];
  };
  /** 读 CXB_HOME 下全部运行记录的 payload；读不到或解析失败的文件直接让断言失败（原子写保证不会读到半个文件）。 */
  const readRuns = (home: string): RunFile[] => {
    const out: RunFile[] = [];
    let workspaces: string[] = [];
    try { workspaces = readdirSync(join(home, "workspaces")); } catch { return out; }
    for (const ws of workspaces) {
      let names: string[] = [];
      try { names = readdirSync(join(home, "workspaces", ws, "runs")); } catch { continue; }
      for (const name of names.filter((n) => n.endsWith(".json"))) {
        const envelope = JSON.parse(readFileSync(join(home, "workspaces", ws, "runs", name), "utf-8"));
        expect(typeof envelope.epoch).toBe("number");
        expect(typeof envelope.revision).toBe("number");
        out.push(envelope.payload as RunFile);
      }
    }
    return out;
  };
  const runOf = (home: string, threadId: string) => readRuns(home).find((r) => r.threadId === threadId);

  /** 启动带运行记录 mock 的 broker；home 可传入同一个目录来测重启。 */
  const startRunsBroker = async (opts: RunsMockOptions = {}, home = join(tempDir, "cxb-home"), logName = "runs-mock.log") => {
    const sockPath = testSocketPath(tempDir);
    const logPath = join(tempDir, logName);
    writeMockCodexScript(tempDir, runsMock(logPath, { backgroundTerminals: { enabled: true }, ...opts }));
    const proc = spawnBroker(endpointFor(sockPath), tempDir, { env: { CXB_HOME: home } });
    await waitForSocket(sockPath);
    return { sockPath, logPath, proc, home };
  };
  const startTurn = (client: TestClient, threadId: string, text = "do the task") =>
    client.request("turn/start", { threadId, input: [{ type: "text", text }] });

  const agent = (id: string, text: string, phase?: string) => ({ type: "agentMessage", id, text, ...(phase ? { phase } : {}) });

  test("M5 结果：completed 自带 items 时按 review → finalAnswer → accumulated 的优先级生成，立即就绪", async () => {
    const { sockPath, proc, home } = await startRunsBroker();
    const client = await TestClient.connectAndInit(sockPath);
    try {
      const cases: [string, unknown[], string, string][] = [
        ["thread-review", [agent("a1", "看一下", "commentary"), { type: "exitedReviewMode", id: "r1", review: "正式审查意见" }, agent("a2", "完成", "final_answer")], "正式审查意见", "review"],
        ["thread-final", [agent("a1", "中间说明"), agent("a2", "答案一", "final_answer"), agent("a3", "答案二", "final_answer")], "答案一\n答案二", "finalAnswer"],
        ["thread-acc", [agent("a1", "第一段"), agent("a2", "第二段")], "第一段\n第二段", "accumulated"],
      ];
      for (const [threadId, items] of cases) {
        await startTurn(client, threadId, "请做 " + threadId);
        await client.request("mock/finish", { threadId, items });
      }
      for (const [threadId, , output, source] of cases) {
        await waitFor(() => runOf(home, threadId)?.state === "ended");
        const run = runOf(home, threadId)!;
        expect(run.runId).toMatch(/^run-/);
        expect(run.status).toBe("completed");
        expect(run.input).toBe("请做 " + threadId);
        expect(run.interruptReason).toBeNull();
        expect(run.turns).toHaveLength(1);
        expect(run.turns[0]).toMatchObject({ turnId: "turn-" + (cases.findIndex((c) => c[0] === threadId) + 1), ready: "ready", status: "completed", output, outputSource: source });
      }
    } finally {
      await client.close();
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  test("M5 结果：completed 的 items 为空时补读 thread/items/list，读到后就绪；认领不等补读", async () => {
    const { sockPath, logPath, proc, home } = await startRunsBroker({ readMode: "items", itemsPageSize: 1, holdItemReads: true });
    const client = await TestClient.connectAndInit(sockPath);
    try {
      await startTurn(client, "thread-r1");
      await client.request("mock/finish", { threadId: "thread-r1", items: [], readItems: [agent("b0", "中间说明"), agent("b1", "补读到的答案", "final_answer")] });
      await waitFor(() => readLog(logPath).some((e) => e.method === "thread/items/list"));
      await waitFor(() => runOf(home, "thread-r1")?.turns[0]?.ready === "pending");
      // 首页回包已扣住：下一轮成功开工才能证明认领不等待补读。
      await startTurn(client, "thread-r1", "下一轮");
      await client.request("mock/finish", { threadId: "thread-r1", items: [agent("b2", "第二轮", "final_answer")] });
      await waitFor(() => readRuns(home).filter((r) => r.threadId === "thread-r1" && r.state === "ended").length === 2);
      expect(readRuns(home).find((r) => r.turns[0]?.turnId === "turn-1")!.turns[0].ready).toBe("pending");
      await client.request("mock/releaseItems");
      await waitFor(() => readRuns(home).every((r) => r.turns.every((t) => t.ready === "ready")));
      const byTurn = (id: string) => readRuns(home).find((r) => r.turns[0]?.turnId === id)!.turns[0];
      expect(byTurn("turn-1")).toMatchObject({ ready: "ready", status: "completed", output: "补读到的答案", outputSource: "finalAnswer" });
      expect(byTurn("turn-2")).toMatchObject({ ready: "ready", output: "第二轮", outputSource: "finalAnswer" });
      const reads = readFileSync(logPath, "utf-8").split("\n").filter(Boolean).map((l) => JSON.parse(l)).filter((e) => e.ev === "recv" && e.method === "thread/items/list");
      expect(readLog(logPath).filter((e) => e.ev === "recv" && e.method === "thread/read")).toEqual([]);
      expect(reads.map((e) => e.params)).toEqual([null, "items:turn-1:1"].map((cursor) => ({ threadId: "thread-r1", turnId: "turn-1", cursor, limit: 64, sortDirection: "asc" })));
    } finally {
      await client.close();
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  test("M9 结果：completed 标 itemsView summary 时不采用自带的最后一条，按 turnId 补读完整条目", async () => {
    const { sockPath, logPath, proc, home } = await startRunsBroker({ readMode: "items", itemsPageSize: 1 });
    const client = await TestClient.connectAndInit(sockPath);
    try {
      await startTurn(client, "thread-s1");
      await client.request("mock/finish", {
        threadId: "thread-s1",
        itemsView: "summary",
        items: [agent("s3", "答案二", "final_answer")],
        readItems: [agent("s1", "中间说明"), agent("s2", "答案一", "final_answer"), agent("s3", "答案二", "final_answer")],
      });
      await waitFor(() => runOf(home, "thread-s1")?.turns[0]?.ready === "ready");
      expect(runOf(home, "thread-s1")!.turns[0]).toMatchObject({ turnId: "turn-1", status: "completed", output: "答案一\n答案二", outputSource: "finalAnswer" });
      const reads = readFileSync(logPath, "utf-8").split("\n").filter(Boolean).map((l) => JSON.parse(l)).filter((e) => e.ev === "recv" && e.method === "thread/items/list");
      expect(readLog(logPath).filter((e) => e.ev === "recv" && e.method === "thread/read")).toEqual([]);
      expect(reads.map((e) => e.params)).toEqual([null, "items:turn-1:1", "items:turn-1:2"].map((cursor) => ({ threadId: "thread-s1", turnId: "turn-1", cursor, limit: 64, sortDirection: "asc" })));
    } finally {
      await client.close();
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  test("M5 结果：补读一直失败时重试 3 次后标 unavailable，输出用降级快照", async () => {
    const { sockPath, logPath, proc, home } = await startRunsBroker({ readMode: "fail" });
    const client = await TestClient.connectAndInit(sockPath);
    try {
      await startTurn(client, "thread-f1");
      await client.request("mock/item", { threadId: "thread-f1", method: "item/completed", item: agent("c1", "快照里的答案", "final_answer") });
      await client.request("mock/finish", { threadId: "thread-f1", items: [] });
      await waitFor(() => runOf(home, "thread-f1")?.turns[0]?.ready === "pending");
      await waitFor(() => runOf(home, "thread-f1")?.turns[0]?.ready === "unavailable", 15_000);
      expect(runOf(home, "thread-f1")!.turns[0]).toMatchObject({ status: "completed", output: "快照里的答案", outputSource: "finalAnswer" });
      const reads = readFileSync(logPath, "utf-8").split("\n").filter(Boolean).map((l) => JSON.parse(l)).filter((e) => e.ev === "recv" && e.method === "thread/items/list");
      // 首次 + 3 次重试（1、2、4 秒）。
      expect(reads).toHaveLength(4);
      expect(readLog(logPath).filter((e) => e.ev === "recv" && e.method === "thread/read")).toEqual([]);
      expect(reads.map((e) => e.params)).toEqual([null, null, null, null].map((cursor) => ({ threadId: "thread-f1", turnId: "turn-1", cursor, limit: 64, sortDirection: "asc" })));
    } finally {
      await client.close();
      proc.kill();
      await proc.exited;
    }
  }, 25_000);

  test("M5 结果：补读在途不挡 ifIdle 关闭，关闭时没读完的标 unavailable 后才退出", async () => {
    const { sockPath, proc, home } = await startRunsBroker({ readMode: "fail" });
    const client = await TestClient.connectAndInit(sockPath);
    try {
      await startTurn(client, "thread-i1");
      await client.request("mock/item", { threadId: "thread-i1", method: "item/completed", item: agent("d1", "兜底文本", "final_answer") });
      await client.request("mock/finish", { threadId: "thread-i1", items: [] });
      await waitFor(() => runOf(home, "thread-i1")?.state === "ended");
      await client.request("broker/shutdown", { ifIdle: true });
      expect(await exitsWithin(proc, 8_000)).toBe(true);
      expect(runOf(home, "thread-i1")!.turns[0]).toMatchObject({ ready: "unavailable", output: "兜底文本", outputSource: "finalAnswer" });
    } finally {
      await client.close();
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  test("M9 关闭：认领释放后的自动退订在途时，ifIdle 关闭不被当作有 turn 在跑", async () => {
    // M9 扩展连跑偶发「A turn is running」的复现：退订回包被扣 3 秒，关闭请求必然落在退订在途的窗口里。
    const { sockPath, logPath, proc, home } = await startRunsBroker({ unsubscribeDelayMs: 3_000 });
    const client = await TestClient.connectAndInit(sockPath);
    try {
      await startTurn(client, "thread-u1");
      await client.request("mock/finish", { threadId: "thread-u1", items: [agent("u1", "完成", "final_answer")] });
      await waitFor(() => runOf(home, "thread-u1")?.state === "ended");
      const unsubscribes = () => readFileSync(logPath, "utf-8").split("\n").filter(Boolean).map((l) => JSON.parse(l))
        .filter((e) => e.ev === "recv" && e.method === "thread/unsubscribe");
      await waitFor(() => unsubscribes().length === 1);
      await client.request("broker/shutdown", { ifIdle: true });
      expect(await exitsWithin(proc, 2_500)).toBe(true);
    } finally {
      await client.close().catch(() => undefined);
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  test("M5 进度：完成的条目进 done，改动的文件进 observedFiles，开始未完成的留在 unconfirmed", async () => {
    const { sockPath, proc, home } = await startRunsBroker();
    const client = await TestClient.connectAndInit(sockPath);
    try {
      await startTurn(client, "thread-p1");
      const cmd = { type: "commandExecution", id: "cmd-1", command: "cargo test", status: "inProgress" };
      await client.request("mock/item", { threadId: "thread-p1", method: "item/started", item: cmd });
      await client.request("mock/item", { threadId: "thread-p1", method: "item/completed", item: { ...cmd, status: "completed", exitCode: 0 } });
      await client.request("mock/item", { threadId: "thread-p1", method: "item/completed", item: { type: "fileChange", id: "fc-1", status: "completed", changes: [{ path: "src/a.rs", kind: { type: "update" }, diff: "" }] } });
      await client.request("mock/item", { threadId: "thread-p1", method: "item/started", item: { type: "commandExecution", id: "cmd-2", command: "bun test", status: "inProgress" } });
      await client.request("mock/finish", { threadId: "thread-p1", items: [agent("e1", "好了", "final_answer")] });
      await waitFor(() => runOf(home, "thread-p1")?.state === "ended");
      const progress = runOf(home, "thread-p1")!.progress;
      expect(progress.done.map((d) => d.itemId)).toEqual(expect.arrayContaining(["cmd-1", "fc-1"]));
      expect(progress.doneTotal).toBeGreaterThanOrEqual(2);
      expect(progress.observedFiles).toEqual(["src/a.rs"]);
      expect(progress.unconfirmed.map((u) => u.itemId)).toEqual(["cmd-2"]);
    } finally {
      await client.close();
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  test("M5 停止：stop 回包带 run，运行记录记 interruptReason stop 与 stopOutcome confirmed", async () => {
    const { sockPath, proc, home } = await startRunsBroker();
    const client = await TestClient.connectAndInit(sockPath);
    try {
      await startTurn(client, "thread-st");
      await waitFor(() => runOf(home, "thread-st")?.state === "running");
      const reply = await client.request("broker/threads/stop", { threadId: "thread-st" }) as Record<string, unknown>;
      expect(reply).toMatchObject({ threadId: "thread-st", stopOutcome: "confirmed" });
      const run = reply.run as RunFile;
      expect(run.runId).toBe(reply.runId as string);
      await waitFor(() => runOf(home, "thread-st")?.state === "ended");
      expect(runOf(home, "thread-st")).toMatchObject({ status: "interrupted", interruptReason: "stop", stopOutcome: "confirmed" });
      expect(runOf(home, "thread-st")!.turns[0]).toMatchObject({ status: "interrupted" });
    } finally {
      await client.close();
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  test("M5 重启：运行中杀掉 broker，重启后标 lost；stop 按唯一判定重新核实后记 confirmed", async () => {
    const home = join(tempDir, "cxb-home");
    const first = await startRunsBroker({}, home, "first.log");
    const client = await TestClient.connectAndInit(first.sockPath);
    await startTurn(client, "thread-k1");
    await waitFor(() => runOf(home, "thread-k1")?.state === "running");
    // 写到一半杀进程：之后所有状态文件都必须能完整解析（readRuns 里逐个 JSON.parse）。
    first.proc.kill();
    await first.proc.exited;
    await client.close().catch(() => undefined);
    readRuns(home);

    const second = await startRunsBroker({}, home, "second.log");
    const client2 = await TestClient.connectAndInit(second.sockPath);
    try {
      await waitFor(() => runOf(home, "thread-k1")?.state === "lost");
      expect(runOf(home, "thread-k1")).toMatchObject({ interruptReason: "brokerLost", stopOutcome: "unconfirmed" });
      const reply = await client2.request("broker/threads/stop", { threadId: "thread-k1" }) as Record<string, unknown>;
      expect(reply).toMatchObject({ threadId: "thread-k1", stopOutcome: "confirmed" });
      await waitFor(() => runOf(home, "thread-k1")?.stopOutcome === "confirmed");
      expect(runOf(home, "thread-k1")!.state).toBe("lost");
      // 新 broker 确实去服务器核实过，而不是凭本地记录直接回已停。
      const log = readFileSync(second.logPath, "utf-8");
      expect(log).toContain("\"thread/read\"");
    } finally {
      await client2.close();
      second.proc.kill();
      await second.proc.exited;
    }
  }, 25_000);

  test("M5 重启：结束时结果还在补读，重启后只遍历一次分页并写入", async () => {
    const home = join(tempDir, "cxb-home");
    const first = await startRunsBroker({ readMode: "items", holdItemReads: true }, home, "first.log");
    const client = await TestClient.connectAndInit(first.sockPath);
    await startTurn(client, "thread-q1");
    await client.request("mock/finish", { threadId: "thread-q1", items: [] });
    await waitFor(() => runOf(home, "thread-q1")?.state === "ended" && runOf(home, "thread-q1")?.turns[0]?.ready === "pending");
    await waitFor(() => readLog(first.logPath).filter((e) => e.method === "thread/items/list").length === 1);
    first.proc.kill();
    await first.proc.exited;
    await client.close().catch(() => undefined);

    const second = await startRunsBroker({ readMode: "items", itemsPageSize: 1, restoredTurns: [{ id: "turn-1", items: [agent("q0", "恢复说明"), agent("q1", "重启后读到的答案", "final_answer")] }] }, home, "second.log");
    try {
      await waitFor(() => runOf(home, "thread-q1")?.turns[0]?.ready === "ready", 8_000);
      expect(runOf(home, "thread-q1")!.turns[0]).toMatchObject({ output: "重启后读到的答案", outputSource: "finalAnswer" });
      const reads = readFileSync(second.logPath, "utf-8").split("\n").filter(Boolean).map((l) => JSON.parse(l)).filter((e) => e.ev === "recv" && e.method === "thread/items/list");
      // 一次完整遍历：两页、首页恰好一次，没有重新开始或额外重试。
      expect(readLog(second.logPath).filter((e) => e.ev === "recv" && e.method === "thread/read")).toEqual([]);
      expect(reads.map((e) => e.params)).toEqual([null, "items:turn-1:1"].map((cursor) => ({ threadId: "thread-q1", turnId: "turn-1", cursor, limit: 64, sortDirection: "asc" })));
    } finally {
      second.proc.kill();
      await second.proc.exited;
    }
  }, 25_000);

  // ─── M6：扩展方法、托管与 inbox（详细设计 2.3 节、3.6 节、迁移表 #2c–#2e、#26 系列、4.2、4.3 节） ───
  // 假 app-server：turn/start 回包后宣布 turn；askBeforeReply 时先发一个服务端请求再回包（审批比回包先到）；
  // holdStartReply 时扣住 turn/start 的回包与 turn/started，直到 mock/release 放行；
  // mock/ask 发服务端请求、mock/resolved 发 serverRequest/resolved、mock/notify 发任意通知、mock/finish 结束 turn；
  // goal 按线程保存，get、set、clear 照常读写，mock/setGoal 只改服务器侧 goal、不发通知；resume 可失败；
  // autoGoalOnResume 时模拟上游冷 resume 的副作用：resume 完成后线程空闲且 goal 为 active，服务器自己开一轮
  // （Codex ff6aec9：thread_processor 发 idle 生命周期 → goal 扩展 continue_if_idle → start_turn_if_idle）。
  // 每条日志带 t（毫秒时间戳），broker 回答服务端请求时记 answer。
  // M7（共享接入时经 serveMockOverSocket 挂在 AF_UNIX socket 上）：holdStarted 时 turn/start 照常回包、
  // 但扣住 turn/started 直到 mock/announce；readFail 时 thread/read 一律失败（吸收判定进入 undecided）；
  // mock/flood 连发通知、mock/askMany 连发服务端请求；mock/absorbNext 让该线程下一个 turn/start 被外部 turn
  // 「theirs-1」吸收（回包给出自己的 turn id，但从不宣布，thread/read 报 theirs-1 在跑）。
  // M9（空转刹车）：autoLoop 时模拟 goal 扩展的自动续跑——任何一轮结束后，goal 仍为 active 且线程空闲，
  // 就自开一轮（输入是内部指引，没有 userMessage），隔 delayMs 后以 items 完成；completed 带 view 作为
  // itemsView（summary 时通过 thread/items/list 补全条目），最多自开 limit 轮防止失控。日志记 autoStarted。
  interface HostMockOptions {
    backgroundTerminals?: import("./test-support/background-terminals-mock").BackgroundTerminalsMockOptions;
    itemsPageSize?: number;
    holdItemReads?: boolean;
    holdStarted?: boolean;
    readFail?: boolean;
    askBeforeReply?: { method: string; params?: Record<string, unknown> };
    holdStartReply?: boolean;
    resumeFail?: boolean;
    autoGoalOnResume?: boolean;
    interruptCompletes?: boolean;
    goals?: Record<string, string>;
    autoLoop?: { limit: number; delayMs?: number; items: unknown[]; view?: string };
  }
  const hostMock = (logPath: string, opts: HostMockOptions = {}) => `
import { appendFileSync } from "node:fs";
// CXB_INLINE_BACKGROUND_TERMINALS_BEGIN
import { createBackgroundTerminalsMock } from "./background-terminals-mock";
// CXB_INLINE_BACKGROUND_TERMINALS_END
const O = ${JSON.stringify(opts)};
const LOG = ${JSON.stringify(logPath)};
const log = (e) => appendFileSync(LOG, JSON.stringify({ t: Date.now(), ...e }) + "\\n");
const send = (m) => process.stdout.write(JSON.stringify(m) + "\\n");
const background = createBackgroundTerminalsMock(send, O.backgroundTerminals);
let turnSeq = 0;
let nextAsk = 100;
${resultPagingMock}
const running = new Map();
const finished = new Map();
const goals = new Map(Object.entries(O.goals ?? {}));
const asked = new Map();
const heldStarts = new Map();
const pendingAnnounce = new Map();
const absorbNext = new Set();
const turnObj = (id, status, items, view) => ({ id, items: items ?? [], status, error: null, ...(view ? { itemsView: view } : {}) });
const goalObj = (threadId) => goals.has(threadId) ? {
  threadId, objective: "mock objective", status: goals.get(threadId), tokenBudget: null,
  tokensUsed: 0, timeUsedSeconds: 0, createdAt: 1, updatedAt: 2,
} : null;
let autoCount = 0;
function finish(threadId, status, items, readItems, view) {
  const id = running.get(threadId);
  if (!id) return;
  running.delete(threadId);
  (finished.get(threadId) ?? finished.set(threadId, []).get(threadId)).push({ id, items: readItems ?? items ?? [] });
  send({ method: "turn/completed", params: { threadId, turn: turnObj(id, status, items, view) } });
  log({ ev: "completed", turnId: id, status });
  if (O.autoLoop) setTimeout(() => autoContinue(threadId), 10);
}
function autoContinue(threadId) {
  const L = O.autoLoop;
  if (autoCount >= L.limit || goals.get(threadId) !== "active" || running.has(threadId)) return;
  autoCount++;
  const id = "turn-" + (++turnSeq);
  running.set(threadId, id);
  send({ method: "turn/started", params: { threadId, turn: turnObj(id, "inProgress") } });
  log({ ev: "autoStarted", turnId: id });
  setTimeout(() => {
    if (running.get(threadId) !== id) return;
    const summary = L.view === "summary";
    finish(threadId, "completed", summary ? L.items.slice(-1) : L.items, L.items, L.view);
  }, L.delayMs ?? 30);
}
function ask(threadId, method, params) {
  const id = nextAsk++;
  asked.set(String(id), method);
  send({ id, method, params: { threadId, turnId: running.get(threadId) ?? null, ...params } });
  log({ ev: "ask", id, method });
  return id;
}
let buf = "";
process.stdin.setEncoding("utf-8");
process.stdin.on("data", (chunk) => {
  buf += chunk;
  let i;
  while ((i = buf.indexOf("\\n")) !== -1) {
    const line = buf.slice(0, i).trim();
    buf = buf.slice(i + 1);
    if (!line) continue;
    let msg;
    try { msg = JSON.parse(line); } catch { continue; }
    if (msg.id === undefined) continue;
    if (msg.method === undefined) {
      const method = asked.get(String(msg.id));
      if (method !== undefined) log({ ev: "answer", id: msg.id, method, result: msg.result ?? null, error: msg.error ?? null });
      continue;
    }
    const p = msg.params ?? {};
    if (msg.method !== "initialize") log({ ev: "recv", method: msg.method, params: p });
    if (background.handle(msg)) continue;
    switch (msg.method) {
      case "initialize":
        send({ id: msg.id, result: { userAgent: "host-mock" } });
        break;
      case "thread/start":
        send({ id: msg.id, result: { thread: { id: "thread-new", preview: "hello", cwd: "/work", turns: [] }, model: "mock-model", cwd: "/work" } });
        break;
      case "thread/resume":
        if (O.resumeFail) {
          send({ id: msg.id, error: { code: -32600, message: "mock resume failed" } });
          break;
        }
        send({ id: msg.id, result: { thread: { id: p.threadId, preview: "", cwd: "/work", turns: [] }, model: "mock-model", cwd: "/work" } });
        if (O.autoGoalOnResume && goals.get(p.threadId) === "active" && !running.has(p.threadId)) {
          const id = "turn-" + (++turnSeq);
          running.set(p.threadId, id);
          send({ method: "turn/started", params: { threadId: p.threadId, turn: turnObj(id, "inProgress") } });
          log({ ev: "autoStarted", turnId: id });
        }
        break;
      case "turn/start": {
        const id = "turn-" + (++turnSeq);
        if (absorbNext.has(p.threadId)) {
          absorbNext.delete(p.threadId);
          running.set(p.threadId, "theirs-1");
          send({ method: "turn/started", params: { threadId: p.threadId, turn: turnObj("theirs-1", "inProgress") } });
          send({ id: msg.id, result: { turn: turnObj(id, "inProgress") } });
          log({ ev: "absorbed", turnId: id });
          break;
        }
        running.set(p.threadId, id);
        if (O.askBeforeReply) ask(p.threadId, O.askBeforeReply.method, O.askBeforeReply.params ?? {});
        const reply = () => {
          send({ id: msg.id, result: { turn: turnObj(id, "inProgress") } });
          if (O.holdStarted) {
            pendingAnnounce.set(p.threadId, id);
            log({ ev: "replied", turnId: id });
            return;
          }
          send({ method: "turn/started", params: { threadId: p.threadId, turn: turnObj(id, "inProgress") } });
          log({ ev: "started", turnId: id });
        };
        if (O.holdStartReply) heldStarts.set(p.threadId, reply);
        else reply();
        break;
      }
      case "mock/release": {
        const reply = heldStarts.get(p.threadId);
        heldStarts.delete(p.threadId);
        if (reply) reply();
        send({ id: msg.id, result: {} });
        break;
      }
      case "turn/interrupt":
        send({ id: msg.id, result: {} });
        if (O.interruptCompletes !== false && running.get(p.threadId) === p.turnId) {
          setTimeout(() => { if (running.get(p.threadId) === p.turnId) finish(p.threadId, "interrupted"); }, 20);
        }
        break;
      case "thread/goal/get":
        send({ id: msg.id, result: { goal: goalObj(p.threadId) } });
        break;
      case "thread/goal/set":
        if (p.status) goals.set(p.threadId, p.status);
        send({ id: msg.id, result: { goal: goalObj(p.threadId) } });
        break;
      case "thread/goal/clear":
        goals.delete(p.threadId);
        send({ id: msg.id, result: {} });
        break;
      case "thread/items/list":
        if (O.readFail) send({ id: msg.id, error: { code: -32603, message: "模拟分页读取失败" } });
        else listItems(msg, finished.get(p.threadId) ?? []);
        break;
      case "mock/releaseItems":
        releaseItems(msg);
        break;
      case "thread/turns/list": {
        const id = running.get(p.threadId);
        send({ id: msg.id, result: { data: id ? [turnObj(id, "inProgress")] : [], nextCursor: null, backwardsCursor: null } });
        break;
      }
      case "thread/read": {
        if (O.readFail) {
          send({ id: msg.id, error: { code: -32603, message: "mock read failed" } });
          break;
        }
        const id = running.get(p.threadId);
        const turns = [
          ...(finished.get(p.threadId) ?? []).map((t) => turnObj(t.id, "completed", t.items)),
          ...(id ? [turnObj(id, "inProgress")] : []),
        ];
        send({ id: msg.id, result: { thread: { id: p.threadId, preview: "", cwd: "/work", status: { type: id ? "active" : "idle" }, turns } } });
        break;
      }
      case "thread/turns/list": {
        // H2 状态读取层的轮次分页：与 thread/read 一致，倒序（最新在前）一页给完；notLoaded 时不带条目。
        if (O.readFail) {
          send({ id: msg.id, error: { code: -32603, message: "mock read failed" } });
          break;
        }
        const id = running.get(p.threadId);
        const view = p.itemsView === "notLoaded" ? "notLoaded" : undefined;
        const data = [
          ...(id ? [turnObj(id, "inProgress", [], view)] : []),
          ...[...(finished.get(p.threadId) ?? [])].reverse().map((t) => turnObj(t.id, "completed", view ? [] : t.items, view)),
        ];
        send({ id: msg.id, result: { data, nextCursor: null, backwardsCursor: null } });
        break;
      }
      case "mock/ask":
        send({ id: msg.id, result: { id: ask(p.threadId, p.method, p.params ?? {}) } });
        break;
      case "mock/resolved":
        send({ method: "serverRequest/resolved", params: { threadId: p.threadId, requestId: p.requestId } });
        send({ id: msg.id, result: {} });
        break;
      case "mock/notify":
        send({ method: p.method, params: p.params });
        send({ id: msg.id, result: {} });
        break;
      case "mock/setGoal":
        goals.set(p.threadId, p.status);
        send({ id: msg.id, result: {} });
        break;
      case "mock/finish":
        finish(p.threadId, p.status ?? "completed", p.items, p.readItems);
        send({ id: msg.id, result: {} });
        break;
      case "mock/announce": {
        const id = pendingAnnounce.get(p.threadId);
        pendingAnnounce.delete(p.threadId);
        if (id) {
          send({ method: "turn/started", params: { threadId: p.threadId, turn: turnObj(id, "inProgress") } });
          log({ ev: "started", turnId: id });
        }
        send({ id: msg.id, result: {} });
        break;
      }
      case "mock/flood": {
        const pad = "x".repeat(p.size ?? 0);
        const turnId = running.get(p.threadId) ?? null;
        for (let k = 0; k < p.count; k++) {
          send({ method: p.method, params: { threadId: p.threadId, turnId, item: { id: "f" + k, type: "agentMessage", text: pad } } });
        }
        log({ ev: "flood", method: p.method, count: p.count });
        send({ id: msg.id, result: {} });
        break;
      }
      case "mock/askMany": {
        for (let k = 0; k < p.count; k++) ask(p.threadId, p.method, { itemId: "a" + k, command: "ls" });
        send({ id: msg.id, result: {} });
        break;
      }
      case "mock/absorbNext":
        absorbNext.add(p.threadId);
        send({ id: msg.id, result: {} });
        break;
      default:
        send({ id: msg.id, result: {} });
    }
  }
});
process.stdin.on("end", () => process.exit(0));
`;

  type MockLogEntry = { t: number; ev: string; method?: string; params?: Record<string, unknown>; id?: number; result?: unknown; error?: unknown; turnId?: string };
  const readLog = (logPath: string): MockLogEntry[] => {
    try {
      return readFileSync(logPath, "utf-8").split("\n").filter(Boolean).map((l) => JSON.parse(l));
    } catch {
      return [];
    }
  };
  const startHostBroker = async (opts: HostMockOptions = {}, home = join(tempDir, "cxb-home"), env: Record<string, string> = {}, dir = tempDir) => {
    const sockPath = testSocketPath(dir);
    const logPath = join(dir, "host-mock.log");
    writeMockCodexScript(dir, hostMock(logPath, { backgroundTerminals: { enabled: true }, ...opts }));
    const proc = spawnBroker(endpointFor(sockPath), dir, { env: { CXB_HOME: home, ...env } });
    await waitForSocket(sockPath);
    return { sockPath, logPath, proc, home };
  };
  type RawReply = { id: number; result?: any; error?: { code: number; message: string; data?: any } };
  /** 用 sendRaw 发请求并取完整回包（TestClient.request 的错误不带 data，取不到 data.reason）。 */
  const rawRequest = async (client: TestClient, method: string, params: unknown, timeoutMs = 10_000): Promise<RawReply> => {
    const id = sendRaw(client, method, params);
    await waitFor(() => replyFor(client, id) !== undefined, timeoutMs);
    return replyFor(client, id) as RawReply;
  };
  const waitForAsync = async (cond: () => Promise<boolean>, timeoutMs = 5_000) => {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if (await cond()) return;
      await Bun.sleep(30);
    }
    throw new Error("waitForAsync timed out");
  };
  /** 读 CXB_HOME 下 inbox.json 的事项（没有文件时为空）。 */
  const readInboxFile = (home: string): any[] => {
    try {
      return readdirSync(join(home, "workspaces")).flatMap((ws) => {
        try {
          return JSON.parse(readFileSync(join(home, "workspaces", ws, "inbox.json"), "utf-8")).payload.items;
        } catch {
          return [];
        }
      });
    } catch {
      return [];
    }
  };
  const notes = (client: TestClient, method: string) =>
    client.messages.filter((m) => m.method === method && m.id === undefined) as { method: string; params: any }[];
  const serverRequests = (client: TestClient) => client.messages.filter((m) => m.id !== undefined && "method" in m);
  const reasonOf = (reply: RawReply) => reply.error?.data?.reason;
  const cxbClient = (sockPath: string) => TestClient.connectAndInit(sockPath, { cxb: true });
  /** 开一轮、停下来，返回被中断的运行编号（续跑的前提）。 */
  const startAndStop = async (client: TestClient, home: string, threadId: string) => {
    const started = await startTurn(client, threadId, "原始要求：整理文档") as { runId: string };
    const stopped = await client.request("broker/threads/stop", { threadId }) as { stopOutcome: string };
    expect(stopped.stopOutcome).toBe("confirmed");
    await waitFor(() => readRuns(home).some((r) => r.runId === started.runId && r.state === "ended"));
    expect(readRuns(home).find((r) => r.runId === started.runId)!.interruptReason).toBe("stop");
    return started.runId;
  };
  const continueTurn = (client: TestClient, threadId: string, fromRunId: string, extra: Record<string, unknown> = {}) =>
    rawRequest(client, "turn/start", { threadId, input: [{ type: "text", text: "接着干" }], cxbContinue: { fromRunId, ...extra } });

  test("M6 门控：cxb 连接先收到 run/started 再收到转发请求，回包附 runId、shortId；非 cxb 连接都看不到", async () => {
    const { sockPath, proc } = await startHostBroker({ askBeforeReply: { method: "item/commandExecution/requestApproval", params: { itemId: "i1", command: "ls" } } });
    const plain = await TestClient.connect(sockPath);
    const cxbRaw = await TestClient.connect(sockPath);
    try {
      const plainInit = await plain.request("initialize", { clientInfo: { name: "t", title: null, version: "0" }, capabilities: {} }) as Record<string, unknown>;
      expect(plainInit).not.toHaveProperty("brokerVersion");
      expect(plainInit).not.toHaveProperty("epoch");
      const cxbInit = await cxbRaw.request("initialize", { clientInfo: { name: "t", title: null, version: "0" }, capabilities: { cxb: { protocolVersion: 1 } } }) as Record<string, unknown>;
      expect(typeof cxbInit.brokerVersion).toBe("string");
      expect(cxbInit.protocolVersion).toBe(1);
      expect(typeof cxbInit.epoch).toBe("number");

      const reply = await startTurn(cxbRaw, "thread-a") as { runId: string; shortId: string };
      expect(reply.runId).toMatch(/^run-/);
      expect(typeof reply.shortId).toBe("string");
      await waitFor(() => serverRequests(cxbRaw).length === 1);
      const started = cxbRaw.messages.findIndex((m) => m.method === "broker/run/started");
      const forwarded = cxbRaw.messages.findIndex((m) => m.id !== undefined && m.method === "item/commandExecution/requestApproval");
      expect(started).toBeGreaterThanOrEqual(0);
      expect(started).toBeLessThan(forwarded);
      expect(notes(cxbRaw, "broker/run/started")[0].params).toMatchObject({ threadId: "thread-a", runId: reply.runId, shortId: reply.shortId });
      expect(typeof notes(cxbRaw, "broker/run/started")[0].params.requestId).toBe("number");

      const plainReply = await startTurn(plain, "thread-b") as Record<string, unknown>;
      expect(plainReply).not.toHaveProperty("runId");
      expect(plainReply).not.toHaveProperty("shortId");
      await waitFor(() => serverRequests(plain).length === 1);
      expect(plain.messages.some((m) => typeof m.method === "string" && (m.method as string).startsWith("broker/"))).toBe(false);
    } finally {
      await plain.close();
      await cxbRaw.close();
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  test("M6 托管：交接后转发中的审批进 inbox，另一连接回答后 app-server 收到回包；重复回答 duplicate、不同答案 alreadyResolved", async () => {
    const { sockPath, logPath, proc } = await startHostBroker({ askBeforeReply: { method: "item/commandExecution/requestApproval", params: { itemId: "i1", command: "ls" } } });
    const owner = await cxbClient(sockPath);
    const other = await cxbClient(sockPath);
    try {
      const { runId } = await startTurn(owner, "thread-h") as { runId: string };
      await waitFor(() => serverRequests(owner).length === 1);
      const detached = await owner.request("broker/threads/detach", { threadId: "thread-h", runId, policy: "on-request" }) as { outcome: string; turnId: string; pending: any[] };
      expect(detached.outcome).toBe("detached");
      expect(detached.turnId).toBe("turn-1");
      expect(detached.pending).toHaveLength(1);
      const item = detached.pending[0];
      expect(item).toMatchObject({ kind: "approval", threadId: "thread-h", runId, turnId: "turn-1", method: "item/commandExecution/requestApproval", status: "pending" });
      expect(item.id).toMatch(/^ap_[a-z0-9_-]{8,64}$/);
      expect(item.params).toMatchObject({ command: "ls" });

      const again = await owner.request("broker/threads/detach", { threadId: "thread-h", runId, policy: "on-request" }) as { outcome: string };
      expect(again.outcome).toBe("alreadyDetached");

      const listed = await other.request("broker/inbox/list", {}) as { items: any[] };
      expect(listed.items.map((i) => i.id)).toEqual([item.id]);

      const first = await other.request("broker/inbox/resolve", { id: item.id, answer: { decision: "accept" } });
      expect(first).toEqual({ status: "resolved", duplicate: false });
      await waitFor(() => readLog(logPath).some((e) => e.ev === "answer"));
      expect(readLog(logPath).filter((e) => e.ev === "answer").map((e) => e.result)).toEqual([{ decision: "accept" }]);
      expect(await other.request("broker/inbox/resolve", { id: item.id, answer: { decision: "accept" } })).toEqual({ status: "resolved", duplicate: true });
      const conflict = await rawRequest(other, "broker/inbox/resolve", { id: item.id, answer: { decision: "decline" } });
      expect(conflict.error?.code).toBe(-32000);
      expect(reasonOf(conflict)).toBe("alreadyResolved");
      expect(reasonOf(await rawRequest(other, "broker/inbox/resolve", { id: "ap_doesnotexist", answer: { decision: "accept" } }))).toBe("notFound");
      const closed = await other.request("broker/inbox/list", { includeClosed: true }) as { items: any[] };
      expect(closed.items.find((i) => i.id === item.id)?.status).toBe("resolved");
      expect((await other.request("broker/inbox/list", {}) as { items: any[] }).items).toEqual([]);
    } finally {
      await owner.close();
      await other.close();
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  test("M6 detach：turn/start 回包未到时即可交接，转发中的审批进 inbox，之后回包照常送达原连接", async () => {
    const { sockPath, proc } = await startHostBroker({ holdStartReply: true, askBeforeReply: { method: "item/commandExecution/requestApproval", params: { itemId: "i1", command: "ls" } } });
    const owner = await cxbClient(sockPath);
    try {
      const startId = sendRaw(owner, "turn/start", { threadId: "thread-hd", input: [{ type: "text", text: "do the task" }] });
      await waitFor(() => notes(owner, "broker/run/started").length === 1 && serverRequests(owner).length === 1);
      const runId = notes(owner, "broker/run/started")[0].params.runId as string;
      expect(replyFor(owner, startId)).toBeUndefined();
      const detached = await owner.request("broker/threads/detach", { threadId: "thread-hd", runId, policy: "on-request" }) as { outcome: string; pending: any[] };
      expect(detached.outcome).toBe("detached");
      expect(detached.pending).toHaveLength(1);
      expect(detached.pending[0]).toMatchObject({ kind: "approval", threadId: "thread-hd", runId, status: "pending" });
      expect(replyFor(owner, startId)).toBeUndefined();
      await owner.request("mock/release", { threadId: "thread-hd" });
      await waitFor(() => replyFor(owner, startId) !== undefined);
      expect(replyFor(owner, startId)!.result).toMatchObject({ runId });
    } finally {
      await owner.close();
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  describe("乙路拍板 L 的托管回归", () => {
    let host: import("./cxb/test-fixtures/sr-host").SrHost | undefined;
    beforeEach(async () => {
      const { SrHost } = await import("./cxb/test-fixtures/sr-host");
      host = new SrHost(tempDir);
      await host.start();
    }, 20_000);
    afterEach(async () => {
      await host?.cleanup();
      host = undefined;
    }, 60_000);

    test("M6 托管：never 策略下交接后到的提问进 inbox 成为 qu_，resolve 前不回包、之后恰好一次；审批仍自动接受", async () => {
      const owner = host!.rpc;
      const threadId = "thread-n";
      await owner.request("thread/resume", { threadId });
      const { runId } = await owner.request<{ runId: string }>("turn/start", { threadId, input: [{ type: "text", text: "执行任务" }] });
      expect(await owner.request("broker/threads/detach", { threadId, runId, policy: "never" })).toMatchObject({ outcome: "detached", pending: [] });
      const question = await owner.request<{ id: number }>("mock/ask", { threadId, method: "item/tool/requestUserInput", params: { itemId: "q", isBlocking: false, questions: [{ id: "q1", question: "选哪个？", options: [{ label: "甲" }, { label: "乙" }] }] } });
      const approval = await owner.request<{ id: number }>("mock/ask", { threadId, method: "item/commandExecution/requestApproval", params: { itemId: "c", command: "列出文件" } });
      const inbox = await owner.request<{ items: Array<{ id: string; kind: string }> }>("broker/inbox/list", {});
      expect(inbox.items).toHaveLength(1);
      expect(inbox.items[0]).toMatchObject({ kind: "question", threadId, runId, status: "pending" });
      expect(inbox.items[0]!.id).toMatch(/^qu_/);
      // app-server 的同通道屏障：前面的自动应答如果存在，此时必已被记录。
      type State = { answers: Array<{ id: number; method: string; result?: unknown; error?: unknown }> };
      const before = await owner.request<State>("mock/state");
      expect(before.answers.filter((answer) => answer.id === question.id)).toEqual([]);
      expect(before.answers.filter((answer) => answer.id === approval.id)).toEqual([{ id: approval.id, method: "item/commandExecution/requestApproval", result: { decision: "accept" } }]);
      const answer = { answers: { q1: { answers: ["乙"] } } };
      expect(await owner.request<unknown>("broker/inbox/resolve", { id: inbox.items[0]!.id, answer })).toEqual({ status: "resolved", duplicate: false });
      expect(await owner.request<unknown>("broker/inbox/resolve", { id: inbox.items[0]!.id, answer })).toEqual({ status: "resolved", duplicate: true });
      const after = await owner.request<State>("mock/state");
      expect(after.answers.filter((value) => value.id === question.id)).toEqual([{ id: question.id, method: "item/tool/requestUserInput", result: answer }]);
      expect(after.answers.filter((value) => value.id === approval.id)).toHaveLength(1);
      expect(host!.requests).toEqual([]);
      expect(await owner.request<unknown>("broker/inbox/list", {})).toEqual({ items: [] });
      await owner.request("mock/finish", { threadId });
    }, 20_000);
  });

  test("M6 detach：运行结束后重试交接回 runEnded 并附结果；没有这个运行回 unknownRun，运行不属于该线程回 runMismatch", async () => {
    const { sockPath, proc } = await startHostBroker();
    const owner = await cxbClient(sockPath);
    try {
      const { runId } = await startTurn(owner, "thread-e") as { runId: string };
      await owner.request("mock/finish", { threadId: "thread-e", items: [agent("x1", "做完了", "final_answer")] });
      await waitFor(() => notes(owner, "broker/run/ended").length === 1);
      const ended = await owner.request("broker/threads/detach", { threadId: "thread-e", runId, policy: "on-request" }) as { outcome: string; result: any; pending: unknown[] };
      expect(ended.outcome).toBe("runEnded");
      expect(ended.pending).toEqual([]);
      expect(ended.result).toMatchObject({ runId, threadId: "thread-e", state: "ended", status: "completed" });
      expect(ended.result.turns[0]).toMatchObject({ ready: "ready", output: "做完了" });
      expect(reasonOf(await rawRequest(owner, "broker/threads/detach", { threadId: "thread-e", runId: "run-nope", policy: "never" }))).toBe("unknownRun");
      await startTurn(owner, "thread-f");
      expect(reasonOf(await rawRequest(owner, "broker/threads/detach", { threadId: "thread-f", runId, policy: "never" }))).toBe("runMismatch");
      const badPolicy = await rawRequest(owner, "broker/threads/detach", { threadId: "thread-f", runId, policy: "sometimes" });
      expect(badPolicy.error?.code).toBe(-32602);
    } finally {
      await owner.close();
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  test("M6 watch：watcher 收到线程通知、run/ended 与补读后的 thread/result；不认识的线程回 unknownThread", async () => {
    const { sockPath, logPath, proc } = await startHostBroker({ itemsPageSize: 1, holdItemReads: true });
    const owner = await cxbClient(sockPath);
    const watcher = await cxbClient(sockPath);
    try {
      expect(reasonOf(await rawRequest(watcher, "broker/threads/watch", { threadId: "thread-unknown" }))).toBe("unknownThread");
      const { runId, shortId } = await startTurn(owner, "thread-w") as { runId: string; shortId: string };
      const snap = await watcher.request("broker/threads/watch", { threadId: "thread-w" }) as { thread: any; run: any; pending: unknown[] };
      expect(snap.thread).toMatchObject({ threadId: "thread-w", shortId, state: "running", owner: "client", turnId: "turn-1", pendingCount: 0, lastRunId: runId });
      expect(snap.run).toMatchObject({ runId, state: "running" });
      expect(snap.pending).toEqual([]);
      await owner.request("mock/notify", { method: "item/started", params: { threadId: "thread-w", turnId: "turn-1", item: { type: "commandExecution", id: "c1", command: "ls" } } });
      await waitFor(() => notes(watcher, "item/started").length === 1);
      expect(notes(owner, "item/started")).toHaveLength(1);
      // completed 不带 items，补读才拿到结果：认领先释放，watcher 之后仍收到 thread/result。
      await owner.request("mock/finish", { threadId: "thread-w", items: [], readItems: [agent("w0", "补读说明"), agent("w1", "补读结果", "final_answer")] });
      await waitFor(() => notes(watcher, "broker/run/ended").length === 1);
      expect(notes(watcher, "broker/run/ended")[0].params).toMatchObject({ threadId: "thread-w", run: { runId, state: "ended" } });
      await waitFor(() => readLog(logPath).some((e) => e.method === "thread/items/list"));
      expect(notes(watcher, "broker/thread/result")).toHaveLength(0);
      await owner.request("mock/releaseItems");
      await waitFor(() => notes(watcher, "broker/thread/result").length === 1);
      expect(notes(watcher, "broker/thread/result")[0].params).toMatchObject({ threadId: "thread-w", runId, turn: 0, result: { turnId: "turn-1", ready: "ready", output: "补读结果" } });
      // owner 不是 watcher：结果补全通知只发给 watchers（2.3 节）。
      expect(notes(owner, "broker/thread/result")).toHaveLength(0);
      expect(notes(owner, "broker/run/ended")).toHaveLength(1);
      const reads = readLog(logPath).filter((e) => e.ev === "recv" && e.method === "thread/items/list");
      expect(readLog(logPath).filter((e) => e.ev === "recv" && e.method === "thread/read")).toEqual([]);
      expect(reads.map((e) => e.params)).toEqual([null, "items:turn-1:1"].map((cursor) => ({ threadId: "thread-w", turnId: "turn-1", cursor, limit: 64, sortDirection: "asc" })));
    } finally {
      await owner.close();
      await watcher.close();
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  test("M6 inbox/wait：按线程等到新事项；按 id 等超时回 timeout；运行结束回 runEnded；没有认领和事项的线程立即回 idle", async () => {
    const { sockPath, proc } = await startHostBroker();
    const owner = await cxbClient(sockPath);
    const waiter = await cxbClient(sockPath);
    try {
      expect(await waiter.request("broker/inbox/wait", { threadId: "thread-quiet", timeoutMs: 3_000 })).toMatchObject({ event: "idle" });
      const { runId } = await startTurn(owner, "thread-q") as { runId: string };
      await owner.request("broker/threads/detach", { threadId: "thread-q", runId, policy: "on-request" });
      const added = waiter.request("broker/inbox/wait", { threadId: "thread-q", timeoutMs: 5_000 }) as Promise<{ event: string; item: any }>;
      // 同一连接上的后续请求作屏障：它的回包到达时，前面的 wait 已在 core 里建立订阅。
      await waiter.request("broker/inbox/list", {});
      await owner.request("mock/ask", { threadId: "thread-q", method: "item/tool/requestUserInput", params: { itemId: "q", questions: [{ id: "q1", question: "?", options: [{ label: "a" }] }] } });
      const got = await added;
      expect(got.event).toBe("added");
      expect(got.item).toMatchObject({ kind: "question", threadId: "thread-q", status: "pending" });
      expect(got.item.id).toMatch(/^qu_/);
      const started = Date.now();
      expect(await waiter.request("broker/inbox/wait", { id: got.item.id, timeoutMs: 300 })).toMatchObject({ event: "timeout" });
      expect(Date.now() - started).toBeGreaterThanOrEqual(250);
      // 订阅时仍 pending 的事项会被 wait 立即看到（2.3 节），先答掉提问，再单独测运行结束。
      expect(await waiter.request("broker/inbox/wait", { threadId: "thread-q", timeoutMs: 5_000 })).toMatchObject({ event: "added", item: { id: got.item.id } });
      expect(await owner.request("broker/inbox/resolve", { id: got.item.id, answer: { answers: { q1: { answers: ["a"] } } } })).toEqual({ status: "resolved", duplicate: false });
      const ended = waiter.request("broker/inbox/wait", { threadId: "thread-q", timeoutMs: 5_000 }) as Promise<{ event: string; run: any }>;
      await waiter.request("broker/inbox/list", {});
      await owner.request("mock/finish", { threadId: "thread-q", items: [agent("q2", "结束", "final_answer")] });
      const done = await ended;
      expect(done.event).toBe("runEnded");
      expect(done.run).toMatchObject({ runId, state: "ended" });
    } finally {
      await owner.close();
      await waiter.close();
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  test("M6 inbox：别人先答（serverRequest/resolved）时事项变 cancelled，之后回答回 cancelled", async () => {
    const { sockPath, logPath, proc } = await startHostBroker({ askBeforeReply: { method: "item/fileChange/requestApproval", params: { itemId: "f1" } } });
    const owner = await cxbClient(sockPath);
    try {
      const { runId } = await startTurn(owner, "thread-r") as { runId: string };
      await waitFor(() => serverRequests(owner).length === 1);
      const { pending } = await owner.request("broker/threads/detach", { threadId: "thread-r", runId, policy: "on-request" }) as { pending: any[] };
      const askId = readLog(logPath).find((e) => e.ev === "ask")!.id;
      await owner.request("mock/resolved", { threadId: "thread-r", requestId: askId });
      await waitForAsync(async () => ((await owner.request("broker/inbox/list", { includeClosed: true })) as { items: any[] }).items[0]?.status === "cancelled");
      expect(reasonOf(await rawRequest(owner, "broker/inbox/resolve", { id: pending[0].id, answer: { decision: "accept" } }))).toBe("cancelled");
      expect(readLog(logPath).filter((e) => e.ev === "answer")).toHaveLength(0);
    } finally {
      await owner.close();
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  test("M6 Guardian：自有认领上被拒的审查生成 gd_，同一 reviewId 只生成一次；override 先 resume 再调用 approveGuardianDeniedAction", async () => {
    const { sockPath, logPath, proc } = await startHostBroker();
    const owner = await cxbClient(sockPath);
    try {
      const { runId } = await startTurn(owner, "thread-g") as { runId: string };
      const review = { threadId: "thread-g", turnId: "turn-1", reviewId: "rev-1", review: { status: { type: "denied" }, riskLevel: "high" }, action: { type: "command", command: "rm -rf build" } };
      await owner.request("mock/notify", { method: "item/autoApprovalReview/completed", params: review });
      await owner.request("mock/notify", { method: "item/autoApprovalReview/completed", params: review });
      await owner.request("mock/notify", { method: "item/autoApprovalReview/completed", params: { ...review, reviewId: "rev-2", action: undefined } });
      await owner.request("mock/finish", { threadId: "thread-g", items: [] });
      await waitFor(() => notes(owner, "broker/run/ended").length === 1);
      // 释放认领不撤销 gd_。
      const { items } = await owner.request("broker/inbox/list", {}) as { items: any[] };
      expect(items).toHaveLength(1);
      expect(items[0]).toMatchObject({ kind: "guardian", threadId: "thread-g", runId, turnId: "turn-1", method: "item/autoApprovalReview/completed", status: "pending" });
      expect(items[0].id).toMatch(/^gd_/);
      expect(items[0].expiresAt - items[0].createdAt).toBe(86_400_000);
      const event = { id: "rev-1", turn_id: "turn-1", status: "denied", action: { type: "command", command: "rm -rf build" } };
      expect(await owner.request("broker/inbox/resolve", { id: items[0].id, answer: { override: true, event } })).toEqual({ status: "resolved", duplicate: false });
      const calls = readLog(logPath).filter((e) => e.ev === "recv" && (e.method === "thread/resume" || e.method === "thread/approveGuardianDeniedAction"));
      expect(calls.slice(-2).map((e) => e.method)).toEqual(["thread/resume", "thread/approveGuardianDeniedAction"]);
      expect(calls.at(-1)!.params).toEqual({ threadId: "thread-g", event });
    } finally {
      await owner.close();
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  test("M6 inbox：broker 重启后旧事项全部 expired，回答回 expired", async () => {
    const home = join(tempDir, "cxb-home");
    const first = await startHostBroker({ askBeforeReply: { method: "item/commandExecution/requestApproval", params: { itemId: "i1", command: "ls" } } }, home);
    let itemId: string;
    const owner = await cxbClient(first.sockPath);
    try {
      const { runId } = await startTurn(owner, "thread-x") as { runId: string };
      await waitFor(() => serverRequests(owner).length === 1);
      const { pending } = await owner.request("broker/threads/detach", { threadId: "thread-x", runId, policy: "on-request" }) as { pending: any[] };
      itemId = pending[0].id;
      // 写入任务异步落盘：确认 inbox.json 已写出再杀进程。
      await waitFor(() => readInboxFile(home).some((i) => i.id === itemId));
    } finally {
      await owner.close();
      first.proc.kill();
      await first.proc.exited;
    }
    const second = await startHostBroker({}, home);
    const client = await cxbClient(second.sockPath);
    try {
      const { items } = await client.request("broker/inbox/list", { includeClosed: true }) as { items: any[] };
      expect(items.map((i) => [i.id, i.status])).toEqual([[itemId!, "expired"]]);
      expect(reasonOf(await rawRequest(client, "broker/inbox/resolve", { id: itemId!, answer: { decision: "accept" } }))).toBe("expired");
    } finally {
      await client.close();
      second.proc.kill();
      await second.proc.exited;
    }
  }, 25_000);

  test("M6 annotate 与 list：登记的 meta 绑定到该连接下一次运行；list 按更新时间倒序返回 ThreadSummary", async () => {
    const { sockPath, proc, home } = await startHostBroker();
    const client = await cxbClient(sockPath);
    try {
      expect(reasonOf(await rawRequest(client, "broker/threads/annotate", { threadId: "thread-none", meta: { source: "run" } }))).toBe("unknownThread");
      await client.request("thread/start", {});
      const bad = await rawRequest(client, "broker/threads/annotate", { threadId: "thread-new", meta: { approval: "never" } });
      expect(bad.error?.code).toBe(-32602);
      const meta = { approval: "never", template: "doc", source: "run" };
      const annotated = await client.request("broker/threads/annotate", { threadId: "thread-new", meta }) as { threadId: string; shortId: string };
      expect(annotated.threadId).toBe("thread-new");
      const { runId } = await startTurn(client, "thread-new") as { runId: string };
      await waitFor(() => readRuns(home).some((r) => r.runId === runId));
      expect((readRuns(home).find((r) => r.runId === runId) as any).meta).toEqual(meta);
      await startTurn(client, "thread-other");
      const { threads } = await client.request("broker/threads/list", {}) as { threads: any[] };
      expect(threads.map((t) => t.threadId).slice(0, 2)).toEqual(["thread-other", "thread-new"]);
      expect(threads.find((t) => t.threadId === "thread-new")).toMatchObject({ shortId: annotated.shortId, state: "running", owner: "client", model: "mock-model", cwd: "/work", meta });
      expect((await client.request("broker/threads/list", { limit: 1 }) as { threads: unknown[] }).threads).toHaveLength(1);
    } finally {
      await client.close();
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  test("M6 ThreadSummary：owner 在线、停止未确认时显示 stopping 与 client", async () => {
    const { sockPath, proc } = await startHostBroker({ interruptCompletes: false });
    const owner = await cxbClient(sockPath);
    const other = await cxbClient(sockPath);
    try {
      await startTurn(owner, "thread-s");
      owner.send({ id: 70_000, method: "broker/threads/stop", params: { threadId: "thread-s" } });
      await waitForAsync(async () => ((await other.request("broker/threads/watch", { threadId: "thread-s" })) as { thread: any }).thread.state === "stopping");
      const { thread } = await other.request("broker/threads/watch", { threadId: "thread-s" }) as { thread: any };
      expect(thread).toMatchObject({ state: "stopping", owner: "client", turnId: "turn-1" });
    } finally {
      await owner.close();
      await other.close();
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  test("M6 续跑：停下的运行用 cxbContinue 接着干，去掉扩展字段再转发，记 continuedFrom；再续原运行回 notLatestRun", async () => {
    const { sockPath, logPath, proc, home } = await startHostBroker();
    const client = await cxbClient(sockPath);
    try {
      const run1 = await startAndStop(client, home, "thread-c");
      expect(reasonOf(await continueTurn(client, "thread-c", "run-nope"))).toBe("unknownRun");
      const reply = await continueTurn(client, "thread-c", run1);
      expect(reply.error).toBeUndefined();
      const run2 = reply.result.runId as string;
      expect(run2).not.toBe(run1);
      expect(notes(client, "broker/run/started").map((n) => n.params.runId)).toContain(run2);
      const starts = readLog(logPath).filter((e) => e.ev === "recv" && e.method === "turn/start");
      expect(starts).toHaveLength(2);
      expect(starts[1].params).not.toHaveProperty("cxbContinue");
      expect(starts[1].params).toMatchObject({ threadId: "thread-c", input: [{ type: "text", text: "接着干" }] });
      await waitFor(() => readRuns(home).some((r) => r.runId === run2));
      expect(readRuns(home).find((r) => r.runId === run2)).toMatchObject({ continuedFrom: run1, input: "接着干" });
      // 正在跑：线程忙。
      const busy = await continueTurn(client, "thread-c", run2);
      expect(busy.error?.code).toBe(-32001);
      expect(reasonOf(busy)).toBe("threadBusy");
      await client.request("mock/finish", { threadId: "thread-c", items: [agent("c9", "完成", "final_answer")] });
      await waitFor(() => readRuns(home).find((r) => r.runId === run2)?.state === "ended");
      expect(reasonOf(await continueTurn(client, "thread-c", run1))).toBe("notLatestRun");
      expect(reasonOf(await continueTurn(client, "thread-c", run2))).toBe("notResumable");
      expect(readRuns(home).filter((r) => r.threadId === "thread-c")).toHaveLength(2);
    } finally {
      await client.close();
      proc.kill();
      await proc.exited;
    }
  }, 25_000);

  test("M6 续跑：goal 被暂停的运行续跑后，在新 turn 开始之后才设回 active", async () => {
    const { sockPath, logPath, proc, home } = await startHostBroker({ goals: { "thread-gc": "active" } });
    const client = await cxbClient(sockPath);
    try {
      const run1 = await startAndStop(client, home, "thread-gc");
      const sets = () => readLog(logPath).filter((e) => e.ev === "recv" && e.method === "thread/goal/set").map((e) => e.params!.status);
      expect(sets()).toEqual(["paused"]);
      const reply = await continueTurn(client, "thread-gc", run1);
      expect(reply.error).toBeUndefined();
      await waitFor(() => sets().length === 2);
      expect(sets()).toEqual(["paused", "active"]);
      const log = readLog(logPath);
      const startAt = log.map((e, i) => (e.ev === "recv" && e.method === "turn/start" ? i : -1)).filter((i) => i >= 0)[1];
      const startedAt = log.map((e, i) => (e.ev === "started" ? i : -1)).filter((i) => i >= 0)[1];
      const activeAt = log.findIndex((e) => e.ev === "recv" && e.method === "thread/goal/set" && e.params!.status === "active");
      expect(startAt).toBeGreaterThan(0);
      expect(startedAt).toBeGreaterThan(startAt);
      // mock 先写日志再发 turn/started 之后才可能收到 set；以 turn/started 发出为界。
      expect(activeAt).toBeGreaterThan(startedAt);
    } finally {
      await client.close();
      proc.kill();
      await proc.exited;
    }
  }, 25_000);

  test("M6 续跑：恢复订阅失败时以 resumeCheckFailed 拒绝，不留新运行记录", async () => {
    const { sockPath, logPath, proc, home } = await startHostBroker({ resumeFail: true });
    const client = await cxbClient(sockPath);
    try {
      const run1 = await startAndStop(client, home, "thread-rf");
      const reply = await continueTurn(client, "thread-rf", run1);
      expect(reply.error?.code).toBe(-32000);
      expect(reasonOf(reply)).toBe("resumeCheckFailed");
      // 屏障：同一连接的 watch 回包到达时，续跑请求已完全结算；内存账本的最近运行仍是原运行。
      const watched = await client.request("broker/threads/watch", { threadId: "thread-rf" }) as { run: any };
      expect(watched.run.runId).toBe(run1);
      expect(readLog(logPath).filter((e) => e.ev === "recv" && e.method === "turn/start")).toHaveLength(1);
      expect(notes(client, "broker/run/started")).toHaveLength(1);
      expect(readRuns(home).filter((r) => r.threadId === "thread-rf").map((r) => r.runId)).toEqual([run1]);
    } finally {
      await client.close();
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  test("M6 续跑：未订阅时先核实、再 resume、resume 后复核，之后才开工", async () => {
    const { sockPath, logPath, proc, home } = await startHostBroker();
    const client = await cxbClient(sockPath);
    try {
      const run1 = await startAndStop(client, home, "thread-o");
      // 等退订落定，续跑时一定要 resume。
      await waitFor(() => readLog(logPath).some((e) => e.method === "thread/unsubscribe"));
      const before = readLog(logPath).length;
      const reply = await continueTurn(client, "thread-o", run1);
      expect(reply.error).toBeUndefined();
      const after = readLog(logPath).slice(before);
      const forwardedAt = after.findIndex((e) => e.ev === "recv" && e.method === "turn/start");
      expect(forwardedAt).toBeGreaterThan(0);
      // 开工之前的控制请求序列：读服务器（thread/read、goal/get）记为 R，恢复订阅记为 resume。
      const steps = after.slice(0, forwardedAt)
        .filter((e) => e.ev === "recv" && ["thread/resume", "thread/read", "thread/goal/get"].includes(e.method!))
        .map((e) => (e.method === "thread/resume" ? "resume" : "R"));
      expect(steps).toEqual(["R", "R", "resume", "R", "R"]);
    } finally {
      await client.close();
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  test("M6 续跑：服务器侧 goal 仍为 active 时在 resume 之前就以 stopping 拒绝，不触发冷 resume 的自动续跑", async () => {
    const { sockPath, logPath, proc, home } = await startHostBroker({ autoGoalOnResume: true });
    const client = await cxbClient(sockPath);
    try {
      const run1 = await startAndStop(client, home, "thread-ga");
      await waitFor(() => readLog(logPath).some((e) => e.method === "thread/unsubscribe"));
      await client.request("mock/setGoal", { threadId: "thread-ga", status: "active" });
      const before = readLog(logPath).length;
      const reply = await continueTurn(client, "thread-ga", run1);
      expect(reply.error?.code).toBe(-32001);
      expect(reasonOf(reply)).toBe("stopping");
      // 屏障：同一连接的 watch 回包到达时，续跑请求已完全结算。
      const watched = await client.request("broker/threads/watch", { threadId: "thread-ga" }) as { run: any };
      expect(watched.run.runId).toBe(run1);
      const after = readLog(logPath).slice(before);
      expect(after.filter((e) => e.ev === "recv" && e.method === "thread/resume")).toHaveLength(0);
      expect(after.filter((e) => e.ev === "autoStarted")).toHaveLength(0);
      expect(after.filter((e) => e.ev === "recv" && e.method === "turn/start")).toHaveLength(0);
    } finally {
      await client.close();
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  // ─── M9：自动续跑空转刹车（S1-评审第 26 节，私有模式） ───
  for (const view of ["full", "summary"] as const) {
    test(`M9 空转刹车：自动续跑连续 3 轮只说话不干活（completed 条目 ${view}）时暂停 goal、中断并记 goalLoop`, async () => {
      const idleItems = [{ type: "agentMessage", id: "m", text: "我再想想" }];
      const { sockPath, logPath, proc, home } = await startHostBroker({ autoLoop: { limit: 8, items: idleItems, view } });
      const client = await cxbClient(sockPath);
      try {
        await client.request("thread/goal/set", { threadId: "thread-loop", objective: "mock objective", status: "active" });
        const { runId } = await startTurn(client, "thread-loop") as { runId: string };
        await client.request("mock/finish", { threadId: "thread-loop", items: [{ type: "agentMessage", id: "a", text: "首轮" }] });
        await waitFor(() => runOf(home, "thread-loop")?.state === "ended", 10_000);
        const run = runOf(home, "thread-loop") as RunFile & { loopGuard?: unknown };
        expect(run.runId).toBe(runId);
        expect(run.status).toBe("interrupted");
        expect(run.interruptReason).toBe("goalLoop");
        expect(run.stopOutcome).toBe("confirmed");
        expect(run.loopGuard).toEqual({ kind: "noAction", count: 3, threshold: 3, turnIds: ["turn-2", "turn-3", "turn-4"], action: "stop" });
        const log = readLog(logPath);
        const paused = log.filter((e) => e.ev === "recv" && e.method === "thread/goal/set" && e.params?.status === "paused");
        expect(paused).toHaveLength(1);
        const evidenceReads = log.filter((e) => e.ev === "recv" && e.method === "thread/items/list"
          && ["turn-2", "turn-3", "turn-4"].includes(String(e.params?.turnId)));
        expect(evidenceReads.map((e) => e.params)).toEqual(view === "summary"
          ? ["turn-2", "turn-3", "turn-4"].map((turnId) => ({ threadId: "thread-loop", turnId, cursor: null, limit: 64, sortDirection: "asc" }))
          : []);
        // 刹车后 goal 已暂停：再等几个续跑间隔，暂停写入之后服务器不再自开新轮，也没跑满 mock 上限。
        await Bun.sleep(300);
        const after = readLog(logPath);
        const pausedAt = after.findIndex((e) => e.ev === "recv" && e.method === "thread/goal/set" && e.params?.status === "paused");
        expect(after.slice(pausedAt).some((e) => e.ev === "autoStarted")).toBe(false);
        const auto = after.filter((e) => e.ev === "autoStarted").map((e) => e.turnId);
        expect(auto.slice(0, 3)).toEqual(["turn-2", "turn-3", "turn-4"]);
        expect(auto.length).toBeLessThan(8);
      } finally {
        await client.close();
        proc.kill();
        await proc.exited;
      }
    }, 20_000);
  }

  test("M9 空转刹车：自动续跑轮有命令执行时不刹车，goal 由服务器完成后运行正常结束", async () => {
    const busyItems = [
      { type: "commandExecution", id: "c", command: "ls", status: "completed" },
      { type: "agentMessage", id: "m", text: "做了一步" },
    ];
    const { sockPath, logPath, proc, home } = await startHostBroker({ autoLoop: { limit: 5, items: busyItems } });
    const client = await cxbClient(sockPath);
    try {
      await client.request("thread/goal/set", { threadId: "thread-busy", objective: "mock objective", status: "active" });
      await startTurn(client, "thread-busy");
      await client.request("mock/finish", { threadId: "thread-busy", items: busyItems });
      await waitFor(() => readLog(logPath).filter((e) => e.ev === "completed").length === 6, 10_000);
      // 续跑额度用完后由「服务器」把 goal 标为完成，认领在等待续跑时释放。
      await client.request("mock/setGoal", { threadId: "thread-busy", status: "complete" });
      await client.request("mock/notify", { method: "thread/goal/updated", params: { threadId: "thread-busy", goal: { threadId: "thread-busy", objective: "mock objective", status: "complete", createdAt: 1 } } });
      await waitFor(() => runOf(home, "thread-busy")?.state === "ended", 10_000);
      const run = runOf(home, "thread-busy") as RunFile & { loopGuard?: unknown };
      expect(run.status).toBe("completed");
      expect(run.interruptReason).toBeNull();
      expect(run.loopGuard).toBeUndefined();
      expect(readLog(logPath).some((e) => e.ev === "recv" && e.method === "thread/goal/set" && e.params?.status === "paused")).toBe(false);
    } finally {
      await client.close();
      proc.kill();
      await proc.exited;
    }
  }, 20_000);

  // ─── M7：共享接入（详细设计 0.1 节、3.4 节、第 5 节第 2–3 步、第 6 节；S1-评审第 22 节缓冲预算） ───
  // 共享服务器用 serveMockOverSocket 在 AF_UNIX socket 上挂 hostMock（每个 WebSocket 连接一个 mock 进程）；
  // 第 2 档的 `codex app-server proxy` 由 harness 的 MOCK_PROXY_SCRIPT 模拟（只搬字节）。
  const startSharedBroker = async (opts: HostMockOptions = {}, env: Record<string, string> = {}) => {
    const sockPath = testSocketPath(tempDir);
    const logPath = join(tempDir, "host-mock.log");
    writeMockCodexScript(tempDir, hostMock(logPath, { backgroundTerminals: { enabled: true }, ...opts }));
    const codexSock = join(tempDir, "cx.sock");
    const fake = serveMockOverSocket(tempDir, codexSock);
    const home = join(tempDir, "cxb-home");
    const proc = spawnBroker(endpointFor(sockPath), tempDir, {
      env: { CXB_HOME: home, CXB_SERVER: "shared", CXB_SERVER_SOCKET: codexSock, ...env },
    });
    await waitForSocket(sockPath);
    return { sockPath, logPath, proc, home, fake, codexSock };
  };
  /** 声明 cxb 的连接看到完整的 server（含 M7 新增的 via）。 */
  const initServer = async (sockPath: string) => {
    const client = await TestClient.connect(sockPath);
    const init = await client.request("initialize", { clientInfo: { name: "t", title: null, version: "0" }, capabilities: { cxb: { protocolVersion: 1 } } }) as { server: Record<string, unknown> };
    return { client, server: init.server };
  };
  const recvMethods = (logPath: string, threadId?: string) =>
    readLog(logPath).filter((e) => e.ev === "recv" && (threadId === undefined || e.params?.threadId === threadId));
  /** 共享 settling 卡在 undecided：thread/read 失败、turn/started 扣住；返回回包（不带 absorbedBy）。 */
  const startUndecided = async (client: TestClient, threadId: string) => {
    const reply = await startTurn(client, threadId) as Record<string, unknown>;
    expect("absorbedBy" in reply).toBe(false);
    return reply;
  };

  // ─── M9：共享模式的空转刹车（S1-评审第 26 节） ───
  // 多连接共享状态的假 app-server：在测试进程里用一个 Bun.serve 挂在 AF_UNIX socket 上，所有 WebSocket 连接共用
  // 同一份线程、turn、goal 状态，通知广播给每个连接（真实共享服务器上 broker 与桌面端看的是同一个线程）。
  // 「桌面端」是不经 broker 的伪连接 desktop：请求走同一个处理函数，回包留在 desktopReplies。
  // 自动续跑由测试用 autoTurn 逐轮驱动：goal 为 active 且线程空闲时开一轮（输入是内部指引，没有 userMessage），
  // 隔 20 毫秒以给定条目完成；经 turn/start 开的轮完成时条目里带 userMessage。goal/set 先广播 updated 再回包。
  type SharedConn = { name: string; send: (m: unknown) => void };
  type SharedThread = { running?: string; user?: boolean; goal?: any; finished: { id: string; items: unknown[] }[] };
  const serveSharedState = (socketPath: string) => {
    let turnSeq = 0;
    let goalSeq = 0;
    let desktopSeq = 0;
    const log: { conn: string; method: string; params: any }[] = [];
    const threads = new Map<string, SharedThread>();
    const thread = (id: string) => threads.get(id) ?? threads.set(id, { finished: [] }).get(id)!;
    const sockets = new Set<{ send: (s: string) => unknown }>();
    const desktopReplies: any[] = [];
    const desktopConn: SharedConn = { name: "desktop", send: (m) => desktopReplies.push(m) };
    const broadcast = (method: string, params: unknown) => {
      const line = JSON.stringify({ method, params });
      for (const ws of sockets) { try { ws.send(line); } catch { /* 连接已断开 */ } }
    };
    const turnObj = (id: string, status: string, items: unknown[] = [], view?: string) =>
      ({ id, items, status, error: null, ...(view ? { itemsView: view } : {}) });
    const begin = (threadId: string, user: boolean) => {
      const t = thread(threadId);
      const id = "turn-" + (++turnSeq);
      t.running = id;
      t.user = user;
      return id;
    };
    const finish = (threadId: string, status: string, items: unknown[], view?: string) => {
      const t = thread(threadId);
      const id = t.running;
      if (!id) return;
      const full = [...(t.user ? [{ type: "userMessage", id: id + "-u", content: [{ type: "text", text: "输入" }] }] : []), ...items];
      t.running = undefined;
      t.user = false;
      t.finished.push({ id, items: full });
      broadcast("turn/completed", { threadId, turn: turnObj(id, status, view === "summary" ? full.slice(-1) : full, view) });
    };
    const handle = (conn: SharedConn, msg: any) => {
      if (msg.id === undefined || msg.method === undefined) return;
      const p = msg.params ?? {};
      if (msg.method !== "initialize") log.push({ conn: conn.name, method: msg.method, params: p });
      const reply = (result: unknown) => conn.send({ id: msg.id, result });
      switch (msg.method) {
        case "initialize":
          reply({ userAgent: "shared-state-mock" });
          break;
        case "thread/resume":
          reply({ thread: { id: p.threadId, preview: "", cwd: "/work", turns: [] }, model: "mock-model", cwd: "/work" });
          break;
        case "turn/start": {
          const id = begin(p.threadId, true);
          reply({ turn: turnObj(id, "inProgress") });
          broadcast("turn/started", { threadId: p.threadId, turn: turnObj(id, "inProgress") });
          break;
        }
        case "turn/interrupt":
          reply({});
          if (thread(p.threadId).running === p.turnId) setTimeout(() => finish(p.threadId, "interrupted", []), 20);
          break;
        case "thread/goal/get":
          reply({ goal: thread(p.threadId).goal ?? null });
          break;
        case "thread/list":
        case "thread/loaded/list":
        case "thread/backgroundTerminals/list":
          reply({ data: [], nextCursor: null });
          break;
        case "thread/goal/set": {
          const t = thread(p.threadId);
          if (!t.goal || (p.objective !== undefined && p.objective !== t.goal.objective)) {
            t.goal = { threadId: p.threadId, objective: p.objective ?? "", status: "active", createdAt: ++goalSeq, tokensUsed: 0 };
          }
          if (p.status) t.goal = { ...t.goal, status: p.status };
          broadcast("thread/goal/updated", { threadId: p.threadId, goal: t.goal });
          reply({ goal: t.goal });
          break;
        }
        case "thread/read": {
          const t = thread(p.threadId);
          reply({ thread: { id: p.threadId, preview: "", cwd: "/work", status: { type: t.running ? "active" : "idle" }, turns: [
            ...t.finished.map((f) => turnObj(f.id, "completed", f.items)),
            ...(t.running ? [turnObj(t.running, "inProgress")] : []),
          ] } });
          break;
        }
        default:
          reply({});
      }
    };
    const server = Bun.serve({
      unix: socketPath,
      fetch(req, srv) {
        if (srv.upgrade(req)) return undefined as unknown as Response;
        return new Response("not a websocket", { status: 400 });
      },
      websocket: {
        open(ws) { sockets.add(ws); },
        message(ws, raw) {
          const conn: SharedConn = { name: "broker", send: (m) => ws.send(JSON.stringify(m)) };
          for (const line of String(raw).split("\n")) {
            if (!line.trim()) continue;
            let msg: unknown;
            try { msg = JSON.parse(line); } catch { continue; }
            handle(conn, msg);
          }
        },
        close(ws) { sockets.delete(ws); },
      },
    });
    return {
      log,
      desktopReplies,
      desktop: (method: string, params: Record<string, unknown>) => handle(desktopConn, { id: ++desktopSeq, method, params }),
      running: (threadId: string) => thread(threadId).running,
      goal: (threadId: string) => thread(threadId).goal,
      finish: (threadId: string, items: unknown[]) => finish(threadId, "completed", items),
      /** 服务器自开一轮（不等完成）；goal 不是 active 或线程忙时返回 undefined。 */
      startAuto: (threadId: string) => {
        const t = thread(threadId);
        if (t.goal?.status !== "active" || t.running) return undefined;
        const id = begin(threadId, false);
        broadcast("turn/started", { threadId, turn: turnObj(id, "inProgress") });
        return id;
      },
      stop: () => server.stop(true),
    };
  };
  type SharedState = ReturnType<typeof serveSharedState>;
  /** 自动续跑一轮：开轮、20 毫秒后以 items 完成；返回 turn id（不能开时 undefined）。 */
  const autoTurn = async (mock: SharedState, threadId: string, items: unknown[]) => {
    const t = mock.goal(threadId);
    if (t?.status !== "active" || mock.running(threadId)) return undefined;
    const id = mock.startAuto(threadId);
    await Bun.sleep(20);
    mock.finish(threadId, items);
    return id;
  };
  const startSharedStateBroker = async () => {
    const codexSock = join(tempDir, "cx-state.sock");
    const mock = serveSharedState(codexSock);
    // 共享档不会拉起 codex；PATH 上照常放 mock，与 startSharedBroker 一致。
    writeMockCodexScript(tempDir, hostMock(join(tempDir, "host-mock.log")));
    const sockPath = testSocketPath(tempDir);
    const home = join(tempDir, "cxb-home");
    const proc = spawnBroker(endpointFor(sockPath), tempDir, {
      env: { CXB_HOME: home, CXB_SERVER: "shared", CXB_SERVER_SOCKET: codexSock },
    });
    await waitForSocket(sockPath);
    return { sockPath, proc, home, mock };
  };
  const idleItem = { type: "agentMessage", id: "m", text: "我再想想" };
  /** 等 broker 把这条运行的第 n 轮写进运行记录（说明它已处理完那条 turn/completed，completed 自带完整条目时
   *  刹车判定在同一步完成）。之后再经 broker 往返一次请求作屏障：在它之前发出的暂停一定已先到 mock。 */
  const turnsWritten = (home: string, threadId: string, n: number) =>
    waitFor(() => (runOf(home, threadId)?.turns.length ?? 0) >= n, 10_000);
  const brokerPaused = (mock: SharedState) =>
    mock.log.some((e) => e.conn === "broker" && e.method === "thread/goal/set" && e.params?.status === "paused");

  test("M9 共享空转刹车：本 broker 设置的 goal 连续 3 轮只说话时只暂停 goal，已开出的下一轮不中断、照常跑完", async () => {
    const { sockPath, proc, home, mock } = await startSharedStateBroker();
    const client = await cxbClient(sockPath);
    try {
      await client.request("thread/goal/set", { threadId: "thread-sh", objective: "我的目标", status: "active" });
      const { runId } = await startTurn(client, "thread-sh") as { runId: string };
      await waitFor(() => mock.running("thread-sh") !== undefined);
      mock.finish("thread-sh", [agent("a1", "首轮")]);
      await turnsWritten(home, "thread-sh", 1);
      for (let n = 2; n <= 3; n++) {
        await autoTurn(mock, "thread-sh", [idleItem]);
        await turnsWritten(home, "thread-sh", n);
      }
      await autoTurn(mock, "thread-sh", [idleItem]);
      // 服务器紧接着又开了一轮：刹车只能暂停 goal，不能误伤正在跑的轮（拍板 K；没有子代理时 SA1b 不改变）。
      const next = mock.startAuto("thread-sh");
      expect(next).toBe("turn-5");
      await waitFor(() => brokerPaused(mock), 10_000);
      expect(mock.goal("thread-sh").status).toBe("paused");
      // 屏障：经 broker 往返一次；在它之前发出的请求都已先到 mock。
      await client.request("thread/goal/get", { threadId: "thread-sh" });
      expect(mock.log.some((e) => e.method === "turn/interrupt")).toBe(false);
      expect(mock.running("thread-sh")).toBe("turn-5");
      mock.finish("thread-sh", [agent("a5", "收尾")]);
      await waitFor(() => runOf(home, "thread-sh")?.state === "ended", 10_000);
      const run = runOf(home, "thread-sh") as RunFile & { loopGuard?: unknown };
      expect(run.runId).toBe(runId);
      expect(run.status).toBe("completed");
      expect(run.interruptReason).toBeNull();
      expect(run.loopGuard).toEqual({ kind: "noAction", count: 3, threshold: 3, turnIds: ["turn-2", "turn-3", "turn-4"], action: "pauseGoal", pause: "paused" });
      expect(mock.log.some((e) => e.method === "turn/interrupt")).toBe(false);
      expect(await autoTurn(mock, "thread-sh", [idleItem])).toBeUndefined();
    } finally {
      await client.close();
      proc.kill();
      await proc.exited;
      mock.stop();
    }
  }, 30_000);

  test("M9 共享空转刹车：桌面端（另一连接）设置的 goal 空转时 broker 不碰", async () => {
    const { sockPath, proc, home, mock } = await startSharedStateBroker();
    const client = await cxbClient(sockPath);
    try {
      mock.desktop("thread/goal/set", { threadId: "thread-dk", objective: "桌面的目标", status: "active" });
      expect(mock.desktopReplies.at(-1)?.result?.goal?.status).toBe("active");
      await client.request("thread/goal/get", { threadId: "thread-dk" });
      await startTurn(client, "thread-dk");
      await waitFor(() => mock.running("thread-dk") !== undefined);
      mock.finish("thread-dk", [agent("a1", "首轮")]);
      await turnsWritten(home, "thread-dk", 1);
      for (let n = 2; n <= 5; n++) {
        await autoTurn(mock, "thread-dk", [idleItem]);
        await turnsWritten(home, "thread-dk", n);
      }
      await client.request("thread/goal/get", { threadId: "thread-dk" });
      expect(brokerPaused(mock)).toBe(false);
      expect(mock.goal("thread-dk").status).toBe("active");
      // 桌面端自己暂停：没有子代理时运行正常结束、没有刹车记录（用户 2026-10-11 定）。
      mock.desktop("thread/goal/set", { threadId: "thread-dk", status: "paused" });
      await waitFor(() => runOf(home, "thread-dk")?.state === "ended", 10_000);
      const run = runOf(home, "thread-dk") as RunFile & { loopGuard?: unknown };
      expect(run.status).toBe("completed");
      expect(run.loopGuard).toBeUndefined();
      expect(mock.log.some((e) => e.conn === "broker" && e.method === "turn/interrupt")).toBe(false);
    } finally {
      await client.close();
      proc.kill();
      await proc.exited;
      mock.stop();
    }
  }, 30_000);

  test("M9 共享空转刹车：桌面端在续跑间隙插话后，broker 不再替这个 goal 刹车", async () => {
    const { sockPath, proc, home, mock } = await startSharedStateBroker();
    const client = await cxbClient(sockPath);
    try {
      await client.request("thread/goal/set", { threadId: "thread-us", objective: "我的目标", status: "active" });
      await startTurn(client, "thread-us");
      await waitFor(() => mock.running("thread-us") !== undefined);
      mock.finish("thread-us", [agent("a1", "首轮")]);
      await turnsWritten(home, "thread-us", 1);
      await autoTurn(mock, "thread-us", [idleItem]);
      await turnsWritten(home, "thread-us", 2);
      // 桌面端在同一线程上开一轮（带 userMessage）：broker 认领正等续跑，这一轮接进同一运行。
      mock.desktop("turn/start", { threadId: "thread-us", input: [{ type: "text", text: "我来插一句" }] });
      mock.finish("thread-us", [agent("d", "回答桌面端")]);
      await turnsWritten(home, "thread-us", 3);
      for (let n = 4; n <= 6; n++) {
        await autoTurn(mock, "thread-us", [idleItem]);
        await turnsWritten(home, "thread-us", n);
      }
      await client.request("thread/goal/get", { threadId: "thread-us" });
      expect(brokerPaused(mock)).toBe(false);
      expect(mock.goal("thread-us").status).toBe("active");
      mock.desktop("thread/goal/set", { threadId: "thread-us", status: "paused" });
      await waitFor(() => runOf(home, "thread-us")?.state === "ended", 10_000);
      expect((runOf(home, "thread-us") as RunFile & { loopGuard?: unknown }).loopGuard).toBeUndefined();
    } finally {
      await client.close();
      proc.kill();
      await proc.exited;
      mock.stop();
    }
  }, 30_000);

  test("M7 接入：shared 直连第 1 档，initialize 的 server 为 shared/socket 并带 socketPath", async () => {
    const { sockPath, proc, fake, codexSock } = await startSharedBroker();
    const { client, server } = await initServer(sockPath);
    try {
      expect(server).toMatchObject({ kind: "shared", via: "socket" });
      expect(String(server.socketPath).toLowerCase()).toBe(codexSock.toLowerCase());
      // 未声明 cxb 的连接只看到上游形状（3.6 节门控）。
      const plain = await TestClient.connect(sockPath);
      const plainInit = await plain.request("initialize", { clientInfo: { name: "t", title: null, version: "0" }, capabilities: {} }) as { server: Record<string, unknown> };
      await plain.close();
      expect(Object.keys(plainInit.server).sort()).toEqual(["kind", "socketPath"]);
    } finally {
      await client.close();
      proc.kill();
      await proc.exited;
      fake.stop();
    }
  }, 20_000);

  test("M7 接入：auto 时第 1 档连不上，回退到第 2 档 proxy", async () => {
    const proxyLog = join(tempDir, "proxy.log");
    const missing = join(tempDir, "missing.sock");
    const { sockPath, proc, fake, codexSock } = await startSharedBroker({}, {
      CXB_SERVER: "auto", CXB_SERVER_SOCKET: missing,
      MOCK_PROXY_TARGET: join(tempDir, "cx.sock"), MOCK_PROXY_LOG: proxyLog,
    });
    const { client, server } = await initServer(sockPath);
    try {
      expect(codexSock).toBe(join(tempDir, "cx.sock"));
      expect(server).toMatchObject({ kind: "shared", via: "proxy" });
      const lines = readFileSync(proxyLog, "utf-8").split("\n").filter(Boolean);
      expect(lines).toHaveLength(1);
      expect(lines[0]).toContain("proxy");
      expect(lines[0]).toContain("--sock");
    } finally {
      await client.close();
      proc.kill();
      await proc.exited;
      fake.stop();
    }
  }, 20_000);

  test("M7 接入：auto 时两档都不可用回退私有；第 2 档挂住时最多等 3 秒，挂住的 proxy 随后被回收", async () => {
    const proxyLog = join(tempDir, "proxy.log");
    const sockPath = testSocketPath(tempDir);
    writeMockCodexScript(tempDir, hostMock(join(tempDir, "host-mock.log")));
    const began = Date.now();
    const proc = spawnBroker(endpointFor(sockPath), tempDir, {
      env: { CXB_SERVER: "auto", CXB_SERVER_SOCKET: join(tempDir, "missing.sock"), MOCK_PROXY_HANG: "1", MOCK_PROXY_LOG: proxyLog },
    });
    await waitForSocket(sockPath);
    const { client, server } = await initServer(sockPath);
    const elapsed = Date.now() - began;
    try {
      expect(server.kind).toBe("private");
      const lines = readFileSync(proxyLog, "utf-8").split("\n").filter(Boolean);
      expect(lines).toHaveLength(1);
      expect(elapsed).toBeGreaterThanOrEqual(2_800);
      expect(elapsed).toBeLessThan(9_000);
      // 回退后 broker 继续运行，挂住的 proxy 进程树仍要在有界时间内被回收（任务说明 7.3 第 2 条）。
      const pid = Number(/pid=(\d+)/.exec(lines[0])![1]);
      await waitForProcessGone(pid, 12_000, "挂住的 proxy 进程树没有被回收");
      expect(proc.exitCode).toBeNull();
    } finally {
      await client.close();
      proc.kill();
      await proc.exited;
    }
  }, 25_000);

  test("M7 接入：shared 时两档都失败，broker 以非零码退出并写明原因", async () => {
    const sockPath = testSocketPath(tempDir);
    writeMockCodexScript(tempDir, hostMock(join(tempDir, "host-mock.log")));
    const proc = spawnBroker(endpointFor(sockPath), tempDir, {
      env: { CXB_SERVER: "shared", CXB_SERVER_SOCKET: join(tempDir, "missing.sock") },
    });
    expect(await exitsWithin(proc, 15_000)).toBe(true);
    expect(proc.exitCode).not.toBe(0);
    const stderr = await new Response(proc.stderr as ReadableStream).text();
    expect(stderr).toContain("连接 Codex 共享 app-server 失败");
    expect(stderr).toContain("CXB_SERVER=shared");
  }, 25_000);

  test("M7 互斥量：同一 CODEX_HOME 的两个私有 broker 初始化串行，不同 CODEX_HOME 互不等待", async () => {
    // 假 app-server：启动即记 spawn，initialize 延迟 800 ms 才回包。
    const probeMock = (log: string) => `
import { appendFileSync } from "node:fs";
appendFileSync(${JSON.stringify(log)}, JSON.stringify({ ev: "spawn", t: Date.now() }) + "\\n");
let buf = "";
process.stdin.setEncoding("utf-8");
process.stdin.on("data", (chunk) => {
  buf += chunk;
  let i;
  while ((i = buf.indexOf("\\n")) !== -1) {
    const line = buf.slice(0, i).trim();
    buf = buf.slice(i + 1);
    if (!line) continue;
    const msg = JSON.parse(line);
    if (msg.id === undefined) continue;
    const reply = () => process.stdout.write(JSON.stringify({ id: msg.id, result: { userAgent: "probe" } }) + "\\n");
    if (msg.method === "initialize") setTimeout(reply, 800); else reply();
  }
});
process.stdin.on("end", () => process.exit(0));
`;
    const run = async (codexHome: (i: number) => string) => {
      const log = join(mkdtempSync(join(tempDir, "probe-")), "spawn.log");
      const procs = [0, 1].map((i) => {
        const dir = mkdtempSync(join(tempDir, `ws${i}-`));
        writeMockCodexScript(dir, probeMock(log));
        const sockPath = testSocketPath(dir);
        return { sockPath, proc: spawnBroker(endpointFor(sockPath), dir, { cwd: dir, env: { CODEX_HOME: codexHome(i) } }) };
      });
      const clients = await Promise.all(procs.map(async ({ sockPath }) => {
        await waitForSocket(sockPath);
        return TestClient.connectAndInit(sockPath);
      }));
      const spawns = readFileSync(log, "utf-8").split("\n").filter(Boolean).map((l) => JSON.parse(l).t as number).sort((a, b) => a - b);
      for (const c of clients) await c.close();
      for (const { proc } of procs) { proc.kill(); await proc.exited; }
      expect(spawns).toHaveLength(2);
      return spawns[1] - spawns[0];
    };
    const sharedHome = mkdtempSync(join(tempDir, "codex-home-shared-"));
    expect(await run(() => sharedHome)).toBeGreaterThanOrEqual(700);
    expect(await run((i) => mkdtempSync(join(tempDir, `codex-home-${i}-`)))).toBeLessThan(700);
  }, 40_000);

  test("M7 关闭：共享服务器上先暂停自己线程的 goal 再中断 turn，joined 的线程不碰", async () => {
    const { sockPath, logPath, proc, fake } = await startSharedBroker({ goals: { "thread-a": "active" } });
    const client = await TestClient.connectAndInit(sockPath);
    const other = await TestClient.connectAndInit(sockPath);
    try {
      await startTurn(client, "thread-a");
      // broker 从 goal/updated 学到 goal 为 active（与上游 goalActiveThreads 一致）。
      await client.request("mock/notify", { method: "thread/goal/updated", params: { threadId: "thread-a", goal: { threadId: "thread-a", objective: "o", status: "active" } } });
      await startTurn(other, "thread-b");
      await other.request("broker/joined", { threadId: "thread-b", turnId: "turn-2" });
      const closer = await TestClient.connectAndInit(sockPath);
      await closer.request("broker/shutdown", {}).catch(() => undefined);
      expect(await exitsWithin(proc, 15_000)).toBe(true);
      const a = recvMethods(logPath, "thread-a").map((e) => e.method);
      const pause = a.lastIndexOf("thread/goal/set");
      const interrupt = a.lastIndexOf("turn/interrupt");
      expect(pause).toBeGreaterThanOrEqual(0);
      expect(interrupt).toBeGreaterThan(pause);
      expect(recvMethods(logPath, "thread-a").find((e) => e.method === "thread/goal/set")!.params).toMatchObject({ status: "paused" });
      expect(recvMethods(logPath, "thread-a").find((e) => e.method === "turn/interrupt")!.params).toMatchObject({ turnId: "turn-1" });
      expect(recvMethods(logPath, "thread-b").some((e) => e.method === "turn/interrupt" || e.method === "thread/goal/set")).toBe(false);
    } finally {
      await client.close().catch(() => undefined);
      await other.close().catch(() => undefined);
      proc.kill();
      await proc.exited;
      fake.stop();
    }
  }, 30_000);

  test("M7 停止：共享 settling 期间主动 stop 回 settling；owner 断开引起的停止等定案为自己的之后才暂停并中断", async () => {
    const { sockPath, logPath, proc, fake } = await startSharedBroker({ goals: { "thread-a": "active" }, holdStarted: true, readFail: true });
    const client = await cxbClient(sockPath);
    try {
      // owner 在独立进程里开工（回包已到、读服务器失败 → undecided），结束进程才能可靠断开。
      const owner = await spawnOwner(tempDir, sockPath, "thread-a");
      await client.request("mock/notify", { method: "thread/goal/updated", params: { threadId: "thread-a", goal: { threadId: "thread-a", objective: "o", status: "active" } } });
      // 详细设计 2.3 节：settling 期间主动 stop 回 -32000 settling，什么都不发。
      const refused = await rawRequest(client, "broker/threads/stop", { threadId: "thread-a" });
      expect(reasonOf(refused)).toBe("settling");
      // 3.3 节「Starting{settling}：归属未定，不猜」：owner 断开发起的停止等定案，期间不先改 goal。
      owner.kill();
      await owner.exited;
      await Bun.sleep(600);
      expect(recvMethods(logPath, "thread-a").some((e) => e.method === "thread/goal/set" || e.method === "turn/interrupt")).toBe(false);
      await client.request("mock/announce", { threadId: "thread-a" });
      await waitFor(() => recvMethods(logPath, "thread-a").some((e) => e.method === "turn/interrupt"), 10_000);
      const a = recvMethods(logPath, "thread-a").map((e) => e.method);
      expect(a.indexOf("thread/goal/set")).toBeGreaterThanOrEqual(0);
      expect(a.indexOf("turn/interrupt")).toBeGreaterThan(a.indexOf("thread/goal/set"));
      expect(recvMethods(logPath, "thread-a").find((e) => e.method === "thread/goal/set")!.params).toMatchObject({ status: "paused" });
    } finally {
      await client.close();
      proc.kill();
      await proc.exited;
      fake.stop();
    }
  }, 30_000);

  test("M7 续跑：共享服务器上续跑被外部 turn 吸收时回包 absorbedBy 为对方 turn，运行记 joined；再续原运行回 notLatestRun", async () => {
    const { sockPath, proc, fake, home } = await startSharedBroker();
    const client = await cxbClient(sockPath);
    try {
      const runId = await startAndStop(client, home, "thread-a");
      await client.request("mock/absorbNext", { threadId: "thread-a" });
      const reply = await continueTurn(client, "thread-a", runId);
      expect(reply.error).toBeUndefined();
      expect(reply.result.absorbedBy).toBe("theirs-1");
      await waitFor(() => notes(client, "broker/run/settled").some((n) => n.params.absorbedBy === "theirs-1"), 5_000);
      await waitFor(() => readRuns(home).some((r) => r.continuedFrom === runId && (r as any).joined === true), 5_000);
      // 外部 turn 还在跑时线程忙（threadBusy）；它结束、joined 认领释放后，原运行已不是最近一次。
      await client.request("mock/finish", { threadId: "thread-a" });
      await waitFor(() => notes(client, "broker/run/ended").length >= 2, 5_000);
      const again = await continueTurn(client, "thread-a", runId);
      expect(reasonOf(again)).toBe("notLatestRun");
    } finally {
      await client.close();
      proc.kill();
      await proc.exited;
      fake.stop();
    }
  }, 30_000);

  test("M7 预算：settling 超软上限后丢 item 与 delta、照收其余通知，定案为自己的后运行标进度不完整", async () => {
    const { sockPath, proc, fake, home } = await startSharedBroker({ holdStarted: true, readFail: true });
    const client = await cxbClient(sockPath);
    try {
      await startUndecided(client, "thread-a");
      await client.request("mock/flood", { threadId: "thread-a", method: "item/completed", count: 1200 });
      await client.request("mock/flood", { threadId: "thread-a", method: "item/agentMessage/delta", count: 50 });
      await client.request("mock/flood", { threadId: "thread-a", method: "custom/keep", count: 3 });
      await client.request("mock/announce", { threadId: "thread-a" });
      await waitFor(() => notes(client, "turn/started").length === 1, 10_000);
      await waitFor(() => notes(client, "custom/keep").length === 3, 10_000);
      expect(notes(client, "item/completed")).toHaveLength(1000);
      expect(notes(client, "item/agentMessage/delta")).toHaveLength(0);
      await waitFor(() => (runOf(home, "thread-a")?.progress as any)?.incomplete === true, 10_000);
    } finally {
      await client.close();
      proc.kill();
      await proc.exited;
      fake.stop();
    }
  }, 40_000);

  test("M7 预算：settling 超硬上限按 joined 定案，run/settled 带 absorbedBy，运行不标进度不完整", async () => {
    const { sockPath, proc, fake, home } = await startSharedBroker({ holdStarted: true, readFail: true });
    const client = await cxbClient(sockPath);
    try {
      const reply = await startUndecided(client, "thread-a") as { runId: string };
      await client.request("mock/flood", { threadId: "thread-a", method: "custom/keep", count: 4001 });
      await waitFor(() => notes(client, "broker/run/settled").length === 1, 10_000);
      expect(notes(client, "broker/run/settled")[0].params).toMatchObject({ runId: reply.runId, absorbedBy: "turn-1" });
      await waitFor(() => (runOf(home, "thread-a") as any)?.joined === true, 10_000);
      expect((runOf(home, "thread-a")!.progress as any).incomplete ?? false).toBe(false);
    } finally {
      await client.close();
      proc.kill();
      await proc.exited;
      fake.stop();
    }
  }, 40_000);

  test("M7 预算：settling 期间挂起的服务端请求超过 64 个时多出的不答，定案后 64 个转给 owner", async () => {
    const { sockPath, logPath, proc, fake } = await startSharedBroker({ holdStarted: true, readFail: true });
    const client = await cxbClient(sockPath);
    client.onRequest((msg) => client.send({ id: msg.id, result: { decision: "accept" } }));
    try {
      await startUndecided(client, "thread-a");
      await client.request("mock/askMany", { threadId: "thread-a", method: "item/commandExecution/requestApproval", count: 70 });
      await client.request("mock/announce", { threadId: "thread-a" });
      await waitFor(() => readLog(logPath).filter((e) => e.ev === "answer").length === 64, 10_000);
      await Bun.sleep(500);
      const answers = readLog(logPath).filter((e) => e.ev === "answer");
      expect(answers).toHaveLength(64);
      expect(answers.every((e) => (e.result as { decision?: string } | null)?.decision === "accept")).toBe(true);
    } finally {
      await client.close();
      proc.kill();
      await proc.exited;
      fake.stop();
    }
  }, 40_000);

  test("broker/health：声明 cxb 能力后精确断言", async () => {
    const sockPath = testSocketPath(tempDir);
    const endpoint = endpointFor(sockPath);
    const mockDir = createMockCodex(tempDir);
    const proc = spawnBroker(endpoint, mockDir);
    await waitForSocket(sockPath);
    try {
      const client = await TestClient.connectAndInit(sockPath, { cxb: true });
      try {
        const health = await client.request("broker/health", {});
        for (const field of [
          "brokerVersion", "protocolVersion", "epoch", "server", "claims",
          "pending", "uptimeMs", "survivesParentJob", "parentJob", "storage",
        ]) {
          expect(health).toHaveProperty(field);
        }
        // 测试工具层直接启动 serve，没有经过 launch：实际脱离结果为 null；只查直接所属的 Job（M8）。
        const parentJob = (health as { parentJob: Record<string, unknown> }).parentJob;
        expect(parentJob).toMatchObject({ breakaway: null, queryScope: "immediateJob" });
        for (const field of ["inJob", "killOnClose", "breakawayAllowed"]) {
          expect(typeof parentJob[field]).toBe("boolean");
        }
        expect(health).toMatchObject({ protocolVersion: 1, claims: 0, pending: 0, storage: "ok" });
        expect(typeof (health as { survivesParentJob: unknown }).survivesParentJob).toBe("boolean");
        expect(typeof (health as { uptimeMs: unknown }).uptimeMs).toBe("number");
      } finally {
        await client.close();
      }
    } finally {
      proc.kill();
      await proc.exited;
    }
  }, 15_000);
});
