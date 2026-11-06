import { afterEach, beforeEach, expect, spyOn, test } from "bun:test";
import childProcess from "node:child_process";
import { randomBytes } from "node:crypto";
import fs from "node:fs";
import net from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { config } from "../config";
import {
  BROKER_BIN, createMockCodex, envWithPathPrefix, removeDirWithRetry,
} from "../test-support/broker-harness";
import { reapTestProcess } from "../test-support/process-identity";
import { brokerVersionOf, connectBroker, resolveBrokerBin } from "./broker-conn";
import { CxbError, type BrokerConnection, type ConnectOptions, type WorkspaceInfo } from "./contracts";
import { createOutput } from "./output";

let tempDir: string;
let workspace: WorkspaceInfo;
let options: ConnectOptions;
let lines: string[];
let pids: Set<number>;
let connections: BrokerConnection[];
let servers: net.Server[];
let sockets: Set<net.Socket>;
let processSpy: ReturnType<typeof spyOn<typeof childProcess, "execFile">>;
let timers: Set<ReturnType<typeof setTimeout>>;
let children: childProcess.ChildProcess[];
let clientStops: Array<() => Promise<void>>;
let outputStreams: fs.WriteStream[];

const CLIENT_SETTLE_MS = 12_000;
const CONNECTION_CLOSE_MS = 2000;
const SERVER_CLOSE_MS = 5000;
const PID_GRACE_MS = 8000;
const PID_KILL_MS = 5000;
const DIRECTORY_RETRIES = 40;
// 各阶段内部并行，阶段之间有依赖：12 + 2 + 5 + 8 + 5 + 2 秒，另留 10 秒本机 I/O 与调度余量。
const CLEANUP_TIMEOUT_MS = CLIENT_SETTLE_MS + CONNECTION_CLOSE_MS + SERVER_CLOSE_MS
  + PID_GRACE_MS + PID_KILL_MS + DIRECTORY_RETRIES * 50 + 10_000;

/** harness 中的同名助手尚未导出，本路局部隔离所有大小写写法。 */
function privateEnv(env: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  const dropped = new Set(["CXB_SERVER", "CXB_SERVER_SOCKET", "MOCK_PROXY_TARGET", "MOCK_PROXY_HANG", "MOCK_PROXY_LOG"]);
  return {
    ...Object.fromEntries(Object.entries(env).filter(([key]) => !dropped.has(key.toUpperCase()))),
    CXB_SERVER: "private",
  };
}

function later(callback: () => void, ms: number): void {
  const timer = setTimeout(() => { timers.delete(timer); callback(); }, ms);
  timers.add(timer);
}

function alive(pid: number): boolean {
  try { process.kill(pid, 0); return true; } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
    throw error;
  }
}

async function until(check: () => boolean, timeoutMs = 5000): Promise<void> {
  const deadline = performance.now() + timeoutMs;
  while (!check()) {
    if (performance.now() >= deadline) throw new Error("等待测试条件超时");
    await Bun.sleep(20);
  }
}

async function bounded<T>(promise: Promise<T>, timeoutMs = 5000): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error("测试操作超时")), timeoutMs); }),
    ]);
  } finally { clearTimeout(timer); }
}

beforeEach(() => {
  tempDir = fs.mkdtempSync(join(tmpdir(), "cxb-n1a-"));
  const root = join(tempDir, "工作区 有空格");
  const dataRoot = join(tempDir, "data");
  const codexHome = join(tempDir, "codex-home");
  for (const dir of [root, dataRoot, codexHome]) fs.mkdirSync(dir);
  const key = randomBytes(8).toString("hex");
  const pipePath = `\\\\.\\pipe\\cxb-${key}`;
  workspace = { dir: root, root, key, dataRoot, workspaceDir: join(dataRoot, "workspaces", key), pipePath, endpoint: `pipe:${pipePath}` };
  lines = [];
  pids = new Set();
  connections = [];
  servers = [];
  sockets = new Set();
  timers = new Set();
  children = [];
  clientStops = [];
  outputStreams = [];
  const execute = childProcess.execFile;
  processSpy = spyOn(childProcess, "execFile").mockImplementation(((...args: Parameters<typeof execute>) => {
    const child = execute(...args);
    if (child.pid) pids.add(child.pid);
    return child;
  }) as typeof execute);
  const mockDir = createMockCodex(tempDir);
  // 模拟 app-server 也记录实际 PID；broker 的 Job 回收后逐一核实。
  const mockScript = join(mockDir, "codex-mock.ts");
  fs.appendFileSync(mockScript, `\nimport { appendFileSync as recordPid } from "node:fs";\nrecordPid(${JSON.stringify(join(tempDir, "mock-pids"))}, String(process.pid) + "\\n");\n`);
  options = {
    workspace,
    brokerBin: BROKER_BIN,
    expectedBrokerVersion: "0.1.0",
    timeoutMs: 15_000,
    idleTimeoutMs: 30_000,
    env: { ...privateEnv(envWithPathPrefix(mockDir)), CXB_HOME: dataRoot, CODEX_HOME: codexHome },
    log(line) {
      lines.push(line);
      const pid = /候选进程已拉起：pid=(\d+)/.exec(line);
      if (pid) pids.add(Number(pid[1]));
    },
  };
});

afterEach(async () => {
  const failures: string[] = [];
  const clean = async (label: string, action: () => unknown | Promise<unknown>) => {
    try { await action(); } catch (error) { failures.push(`${label}：${String(error)}`); }
  };
  await clean("恢复进程监测", () => processSpy.mockRestore());
  for (const timer of timers) clearTimeout(timer);
  // 先让独立客户端收束，等 stdout 中的候选 PID 全部到齐，再遍历进程登记表。
  await Promise.all(clientStops.map((stopClient) => clean("收束独立客户端", stopClient)));
  // 正常用例先自行 shutdown；失败用例也必须清理自己创建的连接和进程。
  await Promise.all(connections.map((connection) => clean("关闭客户端", () => bounded(connection.close(), CONNECTION_CLOSE_MS))));
  for (const socket of sockets) await clean("销毁测试管道", () => socket.destroy());
  await Promise.all(servers.map((server) => clean("关闭模拟服务", async () => {
      if (!server.listening) return;
      await bounded(new Promise<void>((done, reject) => server.close((error) => error ? reject(error) : done())), SERVER_CLOSE_MS);
  })));
  await clean("登记模拟子进程", () => {
    const file = join(tempDir, "mock-pids");
    if (fs.existsSync(file)) for (const pid of fs.readFileSync(file, "utf8").trim().split("\n")) pids.add(Number(pid));
  });
  await Promise.all([...pids].map((pid) => clean(`回收进程 ${pid}`, async () => {
    try { await until(() => !alive(pid), PID_GRACE_MS); } catch {
      if (await reapTestProcess(pid, PID_KILL_MS) !== "reused") failures.push(`进程 ${pid} 未按时退出，需强制结束`);
    }
  })));
  for (const child of children) {
    await clean("关闭子进程输入流", () => child.stdin?.destroy());
    await clean("关闭子进程输出流", () => child.stdout?.destroy());
    await clean("关闭子进程错误流", () => child.stderr?.destroy());
  }
  for (const stream of outputStreams) await clean("关闭测试输出流", () => stream.destroy());
  await clean("删除临时目录", async () => {
    await removeDirWithRetry(tempDir, DIRECTORY_RETRIES);
    expect(fs.existsSync(tempDir)).toBe(false);
  });
  expect(failures).toEqual([]);
}, CLEANUP_TIMEOUT_MS);

function startClient(mode: string, overrides: Partial<ConnectOptions> = {}, gate = "") {
  const { log: _, env, ...input } = { ...options, ...overrides };
  const child = childProcess.spawn(process.execPath, [join(import.meta.dir, "test-fixtures/conn-client.ts"), mode, JSON.stringify(input), gate], {
    env, windowsHide: true, stdio: ["pipe", "pipe", "pipe"],
  });
  children.push(child);
  if (child.pid) pids.add(child.pid);
  let output = "";
  let stderr = "";
  const events: Array<{ result?: string; retryable?: boolean; exitCode?: number; launched?: boolean; log?: string; process?: number; message?: string }> = [];
  child.stdout!.setEncoding("utf8");
  child.stdout!.on("data", (chunk: string) => {
    output += chunk;
    let end: number;
    while ((end = output.indexOf("\n")) >= 0) {
      const event = JSON.parse(output.slice(0, end));
      output = output.slice(end + 1);
      events.push(event);
      if (event.process) pids.add(event.process);
      if (event.log) options.log?.(event.log);
    }
  });
  child.stderr!.setEncoding("utf8");
  child.stderr!.on("data", (chunk: string) => { stderr += chunk; });
  child.stdin!.on("error", () => {});
  const exited = new Promise<void>((done, reject) => {
    child.once("error", reject);
    child.once("close", (code) => {
      if (code !== 0) reject(new Error(`独立客户端异常退出 ${code}：${stderr}`)); else done();
    });
  });
  // 立即接拒绝处理，避免服务端等待控制点期间出现未处理拒绝；最终仍由 finish 验证结果。
  void exited.catch(() => {});
  clientStops.push(async () => {
    if (child.exitCode === null && child.signalCode === null) {
      child.stdin!.write("取消\n");
      if (gate) fs.writeFileSync(gate, "继续");
    }
    await bounded(exited, CLIENT_SETTLE_MS);
  });
  return {
    child, events,
    async finish() {
      await bounded(exited, 15_000);
      expect(alive(child.pid!)).toBe(false);
      const result = events.find((event) => event.result);
      expect(result).toBeDefined();
      return result!;
    },
  };
}

async function connect(overrides: Partial<ConnectOptions> = {}): Promise<BrokerConnection> {
  const connection = await connectBroker({ ...options, ...overrides });
  connections.push(connection);
  return connection;
}

async function stop(connection: BrokerConnection): Promise<void> {
  await bounded(connection.rpc.request("broker/shutdown", { ifIdle: true }));
  await connection.close();
  for (const pid of pids) await until(() => !alive(pid), 8000);
}

type Message = { id?: number | string; method: string; params?: any; result?: unknown };
const handshake = { userAgent: "模拟服务器", brokerVersion: "0.1.0", protocolVersion: 1, epoch: 1, server: { kind: "private" } };

function reply(socket: net.Socket, message: Message, result: unknown): void {
  socket.write(JSON.stringify({ id: message.id, result }) + "\n");
}

async function simulate(
  handler: (socket: net.Socket, message: Message) => void,
  onConnect?: (socket: net.Socket) => void,
): Promise<net.Server> {
  const server = net.createServer((socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
    socket.on("error", () => {});
    socket.setEncoding("utf8");
    let buffer = "";
    socket.on("data", (data) => {
      buffer += data;
      let end: number;
      while ((end = buffer.indexOf("\n")) >= 0) {
        const line = buffer.slice(0, end);
        buffer = buffer.slice(end + 1);
        handler(socket, JSON.parse(line));
      }
    });
    onConnect?.(socket);
  });
  servers.push(server);
  await bounded(new Promise<void>((done, reject) => {
    server.once("error", reject);
    server.listen(workspace.pipePath, () => { server.off("error", reject); done(); });
  }));
  return server;
}

async function expectError(promise: Promise<unknown>, code: "BROKER_UNAVAILABLE" | "PROTOCOL_MISMATCH"): Promise<CxbError> {
  try { await promise; } catch (error) {
    expect(error).toBeInstanceOf(CxbError);
    expect((error as CxbError).code).toBe(code);
    expect((error as CxbError).exitCode).toBe(code === "BROKER_UNAVAILABLE" ? 6 : 12);
    return error as CxbError;
  }
  throw new Error("预期失败，实际成功");
}

test("缺失管道实测为 ENOENT；拉起真 broker、握手、复用后关闭并确认进程退出", async () => {
  const code = await bounded(new Promise<string | undefined>((done) => {
    const socket = net.connect(workspace.pipePath);
    socket.on("error", (error: NodeJS.ErrnoException) => { socket.destroy(); done(error.code); });
  }));
  expect(code).toBe("ENOENT");
  const first = await connect();
  expect(first.launched).toBe(true);
  expect(first.handshake).toMatchObject({ brokerVersion: "0.1.0", protocolVersion: 1, server: { kind: "private" } });
  expect(Number.isSafeInteger(first.handshake.epoch)).toBe(true);
  const second = await connect();
  expect(second.launched).toBe(false);
  expect(second.handshake.epoch).toBe(first.handshake.epoch);
  expect(lines.some((line) => line.includes("候选进程已拉起：pid="))).toBe(true);
  await second.close();
  await stop(first);
}, 30_000);

test("空闲旧版本确认关闭并释放管道后替换为真 broker", async () => {
  let shutdownParams: unknown;
  const server = await simulate((socket, message) => {
    if (message.method === "initialize") reply(socket, message, { ...handshake, brokerVersion: "0.0.0" });
    if (message.method === "broker/shutdown") {
      shutdownParams = message.params;
      reply(socket, message, {});
      server.close();
      socket.end();
    }
  });
  const connection = await connect();
  expect(shutdownParams).toEqual({ ifIdle: true });
  expect(connection.launched).toBe(true);
  expect(connection.handshake.brokerVersion).toBe("0.1.0");
  await stop(connection);
}, 30_000);

test.each([
  ["broker 版本不符", { ...handshake, brokerVersion: "0.0.0" }],
  ["协议版本不符", { ...handshake, protocolVersion: 2 }],
  ["缺少 cxb 握手字段", { userAgent: "旧服务器", server: { kind: "private" } }],
])("%s 且正忙：退出码 12，保留旧服务且不拉起", async (_, result) => {
  await simulate((socket, message) => {
    if (message.method === "initialize") reply(socket, message, result);
    if (message.method === "broker/shutdown") socket.write(JSON.stringify({ id: message.id, error: { code: -32001, message: "模拟 broker 正忙" } }) + "\n");
  });
  const independent = startClient("失败");
  const error = await independent.finish();
  expect(error.result).toBe("PROTOCOL_MISMATCH");
  expect(error.exitCode).toBe(12);
  expect(error.message).toContain("broker 正忙，等任务结束再试");
  expect(error.message).toContain("CLI 期望 broker 0.1.0、协议 1");
  expect(error.message).toContain("当前 broker");
  expect(independent.events.some((event) => event.process)).toBe(false);
  await until(() => sockets.size === 0);
});

test("启动程序以 1 退出：返回退出码 6 并保留 stderr 摘要", async () => {
  // bun 把 launch 当作不存在的脚本名，立即以 1 退出，不创建后台 broker。
  const error = await expectError(connect({ brokerBin: process.execPath }), "BROKER_UNAVAILABLE");
  expect(error.message).toContain("launch");
  expect(error.message).toContain("失败");
  expect((error.cause as { code: number }).code).toBe(1);
  expect(pids.size).toBe(1);
});

test("前三次连接即断开，第四次完整握手；退避期间不拉起", async () => {
  let count = 0;
  const accepted: number[] = [];
  let initialize: unknown;
  let initialized = false;
  await simulate((socket, message) => {
    if (message.method === "initialize") {
      initialize = message.params;
      reply(socket, message, handshake);
    }
    if (message.method === "initialized") initialized = true;
  }, (socket) => {
    accepted.push(performance.now());
    if (++count <= 3) socket.destroy();
  });
  const connection = await connect();
  expect(connection.launched).toBe(false);
  expect(count).toBe(4);
  // 只断言设计规定的退避下限，允许计时器的毫秒取整误差。
  expect(accepted[3] - accepted[0]).toBeGreaterThanOrEqual(690);
  expect(initialize).toEqual({
    clientInfo: { name: "cxb", version: config.clientVersion },
    capabilities: { cxb: { protocolVersion: 1 }, experimentalApi: true, optOutNotificationMethods: ["item/reasoning/textDelta"] },
  });
  await until(() => initialized);
  expect(processSpy).not.toHaveBeenCalled();
});

test("initialize 永不回包：总期限到返回可重试错误并关闭连接", async () => {
  await simulate(() => {});
  const result = await startClient("失败", { timeoutMs: 250 }).finish();
  expect(result.result).toBe("BROKER_UNAVAILABLE");
  expect(result.retryable).toBe(true);
  expect(processSpy).not.toHaveBeenCalled();
  await until(() => sockets.size === 0);
}, 5000);

test("旧 broker 接受关闭但未释放管道：等待总期限且不拉起", async () => {
  let shutdowns = 0;
  await simulate((socket, message) => {
    if (message.method === "initialize") reply(socket, message, { ...handshake, brokerVersion: "0.0.0" });
    if (message.method === "broker/shutdown") { shutdowns++; reply(socket, message, {}); }
  });
  const result = await startClient("失败", { timeoutMs: 500 }).finish();
  expect(result.result).toBe("BROKER_UNAVAILABLE");
  expect(result.retryable).toBe(true);
  expect(shutdowns).toBe(1);
  expect(processSpy).not.toHaveBeenCalled();
  await until(() => sockets.size === 0);
}, 5000);

test("替换后仍不匹配：返回退出码 12，不反复关闭重启", async () => {
  const server = await simulate((socket, message) => {
    if (message.method === "initialize") reply(socket, message, { ...handshake, brokerVersion: "0.0.0" });
    if (message.method === "broker/shutdown") { reply(socket, message, {}); server.close(); socket.end(); }
  });
  await expectError(connect({ expectedBrokerVersion: "9.9.9" }), "PROTOCOL_MISMATCH");
  // 故意制造的第二次版本不符必须先失败，再以正确版本连接并关闭测试进程。
  const connection = await connect();
  expect(connection.launched).toBe(false);
  await stop(connection);
}, 30_000);

test("意外断开立即拒绝 pending 并触发一次 onClose；close 可重复调用", async () => {
  let closes = 0;
  await simulate((socket, message) => {
    if (message.method === "initialize") reply(socket, message, handshake);
    if (message.method === "测试挂起") socket.destroy();
  });
  const connection = await connect();
  connection.rpc.onClose(() => closes++);
  const error = await bounded(expectError(connection.rpc.request("测试挂起"), "BROKER_UNAVAILABLE"));
  expect(error.message).toContain("连接断开");
  expect(closes).toBe(1);
  await Promise.all([connection.close(), connection.close()]);
  await expectError(connection.rpc.request("关闭后请求"), "BROKER_UNAVAILABLE");
});

test("主动关闭拒绝 pending 且不触发意外断开回调", async () => {
  await simulate((socket, message) => {
    if (message.method === "initialize") reply(socket, message, handshake);
  });
  const connection = await connect();
  let closed = false;
  connection.rpc.onClose(() => { closed = true; });
  const pending = expectError(connection.rpc.request("测试挂起"), "BROKER_UNAVAILABLE");
  await connection.close();
  await bounded(pending);
  expect(closed).toBe(false);
});

test("查找顺序：显式路径、入口同目录、仓库开发产物；显式缺失不回退", () => {
  const originalArgv = process.argv[1];
  process.argv[1] = join(tempDir, "安装目录", "cxb.js");
  const explicit = join(tempDir, "显式.exe");
  const installed = join(dirname(process.argv[1]), "cxb-broker.exe");
  const development = resolve(import.meta.dir, "../../broker-rs/target/debug/cxb-broker.exe");
  const found = new Set([explicit, installed, development]);
  const stat = spyOn(fs, "statSync").mockImplementation(((path: fs.PathLike) => {
    if (found.has(String(path))) return { isFile: () => true };
    throw Object.assign(new Error("文件不存在"), { code: "ENOENT" });
  }) as typeof fs.statSync);
  try {
    expect(resolveBrokerBin({ CXB_BROKER_BIN: explicit })).toBe(explicit);
    expect(resolveBrokerBin({})).toBe(installed);
    found.delete(installed);
    expect(resolveBrokerBin({})).toBe(development);
    const missing = join(tempDir, "缺失.exe");
    expect(() => resolveBrokerBin({ CXB_BROKER_BIN: missing })).toThrow(missing);
    expect(() => resolveBrokerBin({ CXB_BROKER_BIN: "" })).toThrow("空路径");
    found.clear();
    try { resolveBrokerBin({}); throw new Error("预期查找失败"); } catch (error) {
      expect(error).toBeInstanceOf(CxbError);
      expect((error as CxbError).exitCode).toBe(6);
      expect((error as Error).message).toContain(installed);
      expect((error as Error).message).toContain(development);
    }
  } finally {
    stat.mockRestore();
    process.argv[1] = originalArgv;
  }
});

test("显式路径必须是文件；真 exe 版本为 0.1.0", async () => {
  expect(() => resolveBrokerBin({ CXB_BROKER_BIN: tempDir })).toThrow(CxbError);
  expect(resolveBrokerBin({ CXB_BROKER_BIN: BROKER_BIN })).toBe(resolve(BROKER_BIN));
  expect(await brokerVersionOf(BROKER_BIN)).toBe("0.1.0");
  expect(pids.size).toBe(1);
});

test("版本命令启动失败或输出不是 broker 版本时统一报退出码 6", async () => {
  await expectError(brokerVersionOf(join(tempDir, "不存在.exe")), "BROKER_UNAVAILABLE");
  await expectError(brokerVersionOf(process.execPath), "BROKER_UNAVAILABLE");
});

test("逐请求期限允许超过默认 30 秒的停止确认，短期限请求互不影响", async () => {
  let delayed: { socket: net.Socket; message: Message } | undefined;
  await simulate((socket, message) => {
    if (message.method === "initialize") reply(socket, message, handshake);
    if (message.method === "broker/threads/stop") {
      delayed = { socket, message };
      later(() => reply(socket, message, { stopOutcome: "confirmed" }), 31_000);
    }
    if (message.method === "立即回应") reply(socket, message, { ok: true });
  });
  const connection = await connect();
  const long = connection.rpc.request("broker/threads/stop", {}, { timeoutMs: 45_000 });
  const started = performance.now();
  const defaultTimeout = expectError(connection.rpc.request("不回应"), "BROKER_UNAVAILABLE").then(() => performance.now() - started);
  // 同时运行短期限请求；超时与迟到回包都不能取消长请求或串错 ID。
  await expectError(connection.rpc.request("不回应", {}, { timeoutMs: 50 }), "BROKER_UNAVAILABLE");
  expect(await connection.rpc.request<{ ok: boolean }>("立即回应")).toEqual({ ok: true });
  expect(delayed).toBeDefined();
  expect(await bounded(long, 45_000)).toEqual({ stopOutcome: "confirmed" });
  expect(await defaultTimeout).toBeGreaterThanOrEqual(29_990);
}, 50_000);

test("短期限超时后迟到回包不串入下一请求，通知与服务端请求仍只分派一次", async () => {
  let late: { socket: net.Socket; message: Message } | undefined;
  let serverAnswer: unknown;
  await simulate((socket, message) => {
    if (message.method === "initialize") reply(socket, message, handshake);
    if (message.method === "短请求") late = { socket, message };
    if (message.method === "后续请求") {
      reply(late!.socket, late!.message, "过期结果");
      socket.write(JSON.stringify({ method: "测试通知", params: {} }) + "\n");
      socket.write(JSON.stringify({ id: "服务器请求", method: "测试提问", params: {} }) + "\n");
      reply(socket, message, "正确结果");
    }
    if (message.id === "服务器请求") serverAnswer = message.result;
  });
  const connection = await connect();
  let notices = 0;
  let questions = 0;
  connection.rpc.on("测试通知", () => notices++);
  connection.rpc.onRequest("测试提问", () => { questions++; return "测试回答"; });
  await expectError(connection.rpc.request("短请求", {}, { timeoutMs: 50 }), "BROKER_UNAVAILABLE");
  expect(await connection.rpc.request<string>("后续请求", {}, { timeoutMs: 1000 })).toBe("正确结果");
  await until(() => serverAnswer !== undefined);
  expect(serverAnswer).toBe("测试回答");
  expect(notices).toBe(1);
  expect(questions).toBe(1);
});

test("另一 CLI 在重连暂停期间拉起新版：错过 ENOENT 的客户端仍成功复用", async () => {
  let shutdowns = 0;
  const server = await simulate((socket, message) => {
    if (message.method === "initialize") reply(socket, message, { ...handshake, brokerVersion: "0.0.0" });
    if (message.method === "broker/shutdown") {
      shutdowns++;
      reply(socket, message, {});
      server.close();
      socket.end();
    }
  });
  const gate = join(tempDir, "放行重连");
  const first = startClient("错开重连", {}, gate);
  await until(() => first.events.some((event) => event.log === "重连已暂停"));
  const second = await connect();
  try {
    expect(second.launched).toBe(true);
    fs.writeFileSync(gate, "继续");
    const result = await first.finish();
    expect(result.result).toBe("已连接");
    expect(result.launched).toBe(false);
    expect(first.events.some((event) => event.process)).toBe(false);
    expect(shutdowns).toBe(1);
  } finally {
    fs.writeFileSync(gate, "继续");
    await stop(second);
  }
}, 30_000);

test("替换期间仍是同 epoch 的旧实例则继续握手退避，仅关闭一次", async () => {
  let shutdowns = 0;
  let initializes = 0;
  await simulate((socket, message) => {
    if (message.method === "initialize") {
      initializes++;
      reply(socket, message, initializes < 3 ? { ...handshake, brokerVersion: "0.0.0" } : { ...handshake, epoch: 2 });
    }
    if (message.method === "broker/shutdown") { shutdowns++; reply(socket, message, {}); }
  });
  const connection = await connect();
  expect(connection.handshake.epoch).toBe(2);
  expect(connection.launched).toBe(false);
  expect(shutdowns).toBe(1);
  expect(processSpy).not.toHaveBeenCalled();
});

test("拉起中取消：登记候选 PID，等待 launch 结束后返回中断且不再重连", async () => {
  const client = startClient("取消拉起");
  const result = await client.finish();
  expect(result.result).toBe("INTERRUPTED");
  expect(result.exitCode).toBe(4);
  expect(client.events.filter((event) => event.log?.startsWith("连接 broker：")).length).toBe(1);
  expect(client.events.some((event) => event.log?.includes("候选进程已拉起：pid="))).toBe(true);
  // 取消连接不关闭共享 broker；测试自己接管已拉起的实例并明确 shutdown。
  const connection = await connect();
  await stop(connection);
}, 30_000);

test("退避中取消：独立客户端自然退出，没有继续重连或遗留定时器", async () => {
  await simulate(() => {}, (socket) => socket.destroy());
  const client = startClient("取消退避");
  const result = await client.finish();
  expect(result.result).toBe("INTERRUPTED");
  expect(result.exitCode).toBe(4);
  expect(client.events.filter((event) => event.log?.startsWith("连接 broker：")).length).toBe(1);
  expect(client.events.some((event) => event.process)).toBe(false);
  await until(() => sockets.size === 0);
});

test("握手中取消：关闭管道，独立客户端自然退出并移除请求期限", async () => {
  let initializing = false;
  await simulate((_, message) => { if (message.method === "initialize") initializing = true; });
  const client = startClient("取消握手");
  await until(() => initializing);
  client.child.stdin!.write("取消\n");
  const result = await client.finish();
  expect(result.result).toBe("INTERRUPTED");
  expect(result.exitCode).toBe(4);
  expect(client.events.some((event) => event.process)).toBe(false);
  await until(() => sockets.size === 0);
});

test("预先取消不连接也不拉起；连接成功后 signal 不影响句柄", async () => {
  const cancelled = AbortSignal.abort();
  await expect(connect({ signal: cancelled })).rejects.toMatchObject({ code: "INTERRUPTED", exitCode: 4 });
  expect(processSpy).not.toHaveBeenCalled();
  await simulate((socket, message) => {
    if (message.id !== undefined) reply(socket, message, message.method === "initialize" ? handshake : "正常回应");
  });
  const controller = new AbortController();
  const connection = await connect({ signal: controller.signal });
  controller.abort();
  expect(await connection.rpc.request<string>("正常请求")).toBe("正常回应");
});

test("带大小写混合的服务选择污染环境仍只连接临时私有模拟服务器", async () => {
  const env = privateEnv({
    ...options.env,
    cXb_SeRvEr: "shared", CXB_SERVER_SOCKET: "测试无效路径", cxb_server: "shared", cxb_SeRvEr_SoCkEt: "测试无效路径",
  });
  expect(Object.keys(env).filter((key) => /^cxb_server(_socket)?$/i.test(key))).toEqual(["CXB_SERVER"]);
  const connection = await connect({ env });
  expect(connection.handshake.server.kind).toBe("private");
  await stop(connection);
}, 30_000);

test.each([true, false])("RPC 诊断只使用连接日志回调，重定向顺序正确（启用日志：%s）", async (enabled) => {
  const stderr = spyOn(console, "error").mockImplementation(() => {});
  try {
    await simulate((socket, message) => {
      if (message.method === "initialize") reply(socket, message, handshake);
      if (message.method === "触发诊断") {
        socket.write("不是 JSON\n");
        reply(socket, message, "已回应");
      }
    });
    const destination = join(tempDir, "诊断顺序.log");
    const stream = fs.createWriteStream(destination);
    outputStreams.push(stream);
    const out = createOutput({ command: "doctor", json: false, verbose: true, stderr: stream });
    const connection = await connect({ log: enabled ? (line) => out.progress(line) : undefined });
    out.progress("业务输出之前");
    await connection.rpc.request("触发诊断");
    out.progress("业务输出之后");
    await out.flush();
    const content = fs.readFileSync(destination, "utf8");
    const diagnostic = content.indexOf("不是 JSON");
    if (enabled) {
      expect(diagnostic).toBeGreaterThan(content.indexOf("业务输出之前"));
      expect(diagnostic).toBeLessThan(content.indexOf("业务输出之后"));
    } else {
      expect(diagnostic).toBe(-1);
    }
    expect(stderr).not.toHaveBeenCalled();
  } finally { stderr.mockRestore(); }
}, 10_000);
