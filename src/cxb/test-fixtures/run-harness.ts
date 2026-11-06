import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, watch, type FSWatcher } from "node:fs";
import net from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Writable } from "node:stream";
import { BROKER_BIN, envWithPathPrefix, testSocketPath, endpointFor, writeMockCodexScript } from "../../test-support/broker-harness";
import { reapTestProcess } from "../../test-support/process-identity";
import { createRpcEndpoint, NO_RESPONSE } from "../../rpc";
import { CxbError, type BrokerConnection, type BrokerRpc, type ExitCode, type ServerRequestDeps, type WorkspaceInfo } from "../contracts";
import { parseArgv } from "../args";
import { createOutput, type Cleanup } from "../output";
import type { CommandHandler } from "../main";
import { bounded } from "../wait-run";

export const TEST_BUDGET = 30_000;
export const CLEANUP_BUDGET = 70_000;
export function alive(pid: number): boolean {
  try { process.kill(pid, 0); return true; } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
    throw error;
  }
}
export async function until(check: () => boolean | Promise<boolean>, ms = 10_000): Promise<void> {
  const end = performance.now() + ms;
  while (!await check()) {
    if (performance.now() >= end) throw new Error("测试控制点等待超时");
    await Bun.sleep(10);
  }
}

export class RunHarness {
  dir: string;
  workspace: WorkspaceInfo;
  env: NodeJS.ProcessEnv;
  connections: BrokerConnection[] = [];
  sockets = new Set<net.Socket>();
  children: Array<{ child: ChildProcess; closed: Promise<number | null> }> = [];
  pids = new Set<number>();
  streams: Writable[] = [];
  disposers: Array<() => unknown | Promise<unknown>> = [];
  watcher: FSWatcher;
  broker?: ChildProcess;
  constructor() {
    this.dir = mkdtempSync(join(tmpdir(), "cxb-n2a-run-"));
    const root = join(this.dir, "工作区");
    const dataRoot = join(this.dir, "data");
    const codexHome = join(this.dir, "codex-home");
    const profile = join(this.dir, "profile");
    for (const dir of [root, dataRoot, codexHome, profile]) mkdirSync(dir);
    const pipePath = testSocketPath(this.dir);
    this.workspace = { root, dir: root, key: "测试", dataRoot, workspaceDir: dataRoot, pipePath, endpoint: endpointFor(pipePath) };
    this.env = Object.fromEntries(Object.entries(envWithPathPrefix(this.dir)).filter(([key]) =>
      !/^(?:CXB_HOME|CODEX_HOME|USERPROFILE|HOME|CXB_SERVER.*|.*SOCKET.*|.*ENDPOINT.*|MOCK_PROXY_.*)$/i.test(key)));
    Object.assign(this.env, { CXB_HOME: dataRoot, CODEX_HOME: codexHome, CXB_SERVER: "private",
      USERPROFILE: profile, HOME: profile, TEMP: tmpdir(), TMP: tmpdir(), RUN_FIXTURE_PID: join(this.dir, "mock.pid") });
    // 创建模拟进程时即登记 PID；清理阶段不依赖 broker.json 或临时 PID 文件补找资源。
    this.watcher = watch(this.dir, () => {
      const file = join(this.dir, "mock.pid");
      if (existsSync(file)) this.pids.add(Number(readFileSync(file, "utf8")));
    });
  }
  spawn(args: string[], env = this.env) {
    const child = spawn(args[0], args.slice(1), { env, cwd: this.workspace.root, windowsHide: true, stdio: ["pipe", "pipe", "pipe"] });
    if (child.pid) this.pids.add(child.pid);
    const closed = new Promise<number | null>((resolve, reject) => { child.once("error", reject); child.once("close", resolve); });
    void closed.catch(() => {});
    this.children.push({ child, closed });
    child.stdin!.on("error", () => {});
    let stdout = "";
    let stderr = "";
    child.stdout!.setEncoding("utf8");
    child.stderr!.setEncoding("utf8");
    child.stdout!.on("data", (chunk) => { stdout += chunk; });
    child.stderr!.on("data", (chunk) => { stderr += chunk; });
    return { child, closed, output: () => ({ stdout, stderr }) };
  }
  async start(scenario: object = {}): Promise<BrokerConnection> {
    const script = join(import.meta.dir, "run-app-server.ts").replace(/\\/g, "/");
    writeMockCodexScript(this.dir, `import ${JSON.stringify(script)};\n`);
    this.env.RUN_FIXTURE_OPTIONS = JSON.stringify(scenario);
    this.broker = this.spawn([BROKER_BIN, "serve", "--endpoint", this.workspace.endpoint, "--cwd", this.workspace.root, "--idle-timeout", "60000"]).child;
    const connection = await this.connect();
    const state = await connection.rpc.request<{ pid: number }>("fixture/state");
    this.pids.add(state.pid);
    return connection;
  }
  async connect(): Promise<BrokerConnection> {
    const connection = await openConnection(this.workspace, (socket) => this.sockets.add(socket));
    this.connections.push(connection);
    return connection;
  }
  capture() {
    let stdout = "";
    let stderr = "";
    const output = new Writable({ write(chunk, _, done) { stdout += chunk; done(); } });
    this.streams.push(output);
    const diagnostic = new Writable({ write(chunk, _, done) { stderr += chunk; done(); } });
    this.streams.push(diagnostic);
    return { output, diagnostic, value: () => ({ stdout, stderr }) };
  }
  async invoke(handler: CommandHandler, argv: string[], connection?: BrokerConnection) {
    const parsed = parseArgv(argv);
    const capture = this.capture();
    const out = createOutput({ command: parsed.command, json: parsed.globals.json, verbose: parsed.globals.verbose,
      stdout: capture.output, stderr: capture.diagnostic });
    const callbacks = new Set<Cleanup>();
    const stopInvocation = async () => {
      await Promise.allSettled([...callbacks].map((callback) => bounded(Promise.resolve(callback()), 5000)));
    };
    this.disposers.push(stopInvocation);
    let connectCount = 0;
    let code: ExitCode;
    try {
      code = await bounded(handler({ parsed, out, workspace: this.workspace,
        registerCleanup(callback) { callbacks.add(callback); return () => { callbacks.delete(callback); }; },
        connect: async () => { connectCount++; return connection ?? await this.connect(); } }), TEST_BUDGET);
    } catch (error) {
      out.error(error);
      code = error instanceof CxbError ? error.exitCode : 1;
    } finally {
      const results = await Promise.allSettled([...callbacks].map((callback) => bounded(Promise.resolve(callback()), 5000)));
      this.disposers = this.disposers.filter((dispose) => dispose !== stopInvocation);
      if (results.some((value) => value.status === "rejected")) throw new Error("处理器清理失败");
    }
    await out.flush();
    return { ...capture.value(), code, connectCount };
  }
  async cleanup() {
    const errors: unknown[] = [];
    const clean = async (work: () => unknown | Promise<unknown>) => { try { await work(); } catch (error) { errors.push(error); } };
    for (const dispose of this.disposers) await clean(dispose);
    await clean(() => this.watcher.close());
    await Promise.all(this.connections.map((conn) => clean(() => conn.close())));
    for (const socket of this.sockets) await clean(() => socket.destroy());
    // 测试拥有 serve 句柄，直接收束它，Windows Job 会回收模拟 codex。
    if (this.broker) await clean(() => { if (this.broker!.exitCode === null) this.broker!.kill("SIGKILL"); });
    await Promise.all(this.children.map(({ child, closed }) => clean(async () => {
      if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      await bounded(closed, 8000);
    })));
    await Promise.all([...this.pids].map((pid) => clean(async () => {
      try { await until(() => !alive(pid), 3000); } catch { await reapTestProcess(pid); }
    })));
    for (const { child } of this.children) for (const stream of [child.stdin, child.stdout, child.stderr]) await clean(() => stream?.destroy());
    for (const stream of this.streams) await clean(() => stream.destroy());
    await clean(async () => {
      await until(() => {
        try { rmSync(this.dir, { recursive: true, force: true }); return !existsSync(this.dir); } catch { return false; }
      }, 5000);
      if (existsSync(this.dir)) throw new Error("测试目录仍残留");
    });
    if (errors.length) throw new AggregateError(errors, "测试资源回收失败");
  }
}

export async function openConnection(workspace: WorkspaceInfo, track: (socket: net.Socket) => void = () => {}): Promise<BrokerConnection> {
  let socket: net.Socket | undefined;
  await until(async () => {
    const candidate = net.createConnection(workspace.pipePath);
    track(candidate);
    candidate.on("error", () => {});
    try {
      await bounded(new Promise<void>((resolve, reject) => { candidate.once("connect", resolve); candidate.once("error", reject); }), 500);
      socket = candidate;
      return true;
    } catch { candidate.destroy(); return false; }
  });
  const transport = socket!;
  const endpoint = createRpcEndpoint({ label: "甲路测试", requestTimeout: 5000, send: (data) => { transport.write(data); },
    downReason: () => transport.destroyed ? "测试连接已关闭" : null, overflowReason: "测试回包超限", onOverflow: () => transport.destroy(), writeFailedPrefix: "测试写入失败", log() {} });
  transport.setEncoding("utf8");
  transport.on("data", (chunk: string) => endpoint.feed(chunk));
  transport.on("close", () => endpoint.fail("测试对端已关闭"));
  transport.on("error", () => endpoint.fail("测试管道错误"));
  const rpc: BrokerRpc = { ...endpoint, async request<T>(method: string, params?: unknown, opts?: { timeoutMs?: number }): Promise<T> {
    try {
      // 丁路接口尚未合并；仅此方法换为可记录的桩，其他方法均穿过真实 Rust broker。
      return await endpoint.request<T>(method === "broker/threads/resumeOverrides" ? "fixture/resumeOverrides" : method, params, opts);
    } catch (error) { throw new CxbError("BROKER_UNAVAILABLE", "测试 RPC 失败", { cause: error }); }
  } };
  const close = async () => {
    endpoint.markClosed();
    if (transport.closed) return;
    const closed = new Promise<void>((done) => transport.once("close", done));
    transport.destroy();
    await bounded(closed, 2000);
  };
  try {
    const handshake = await rpc.request<any>("initialize", { clientInfo: { name: "甲路测试", version: "0.1.0" }, capabilities: { experimentalApi: true, cxb: { protocolVersion: 1 } } });
    return { rpc, handshake, launched: false, close };
  } catch (error) { await close(); throw error; }
}

export function fakeServerRequests(mode: "normal" | "alreadyDetached" | "runEnded" = "normal"): ServerRequestDeps {
  return {
    attachServerRequests(rpc, opts) {
      let called = false;
      return rpc.onAnyRequest((method, params) => {
        if ((params as any)?.threadId !== opts.threadId) return NO_RESPONSE;
        if (!called) { called = true; opts.onNeedsHuman(method); }
        return NO_RESPONSE;
      });
    },
    async handover(rpc, args, opts) {
      if (mode === "runEnded") {
        await rpc.request("fixture/control", { action: "finish", threadId: args.threadId });
        await until(async () => (await rpc.request<any>("broker/threads/watch", { threadId: args.threadId })).run?.state === "ended");
      }
      const result = await rpc.request<any>("broker/threads/detach", args, opts);
      return mode === "alreadyDetached" ? rpc.request("broker/threads/detach", args, opts) : result;
    },
    renderPending(items, ctx) {
      const pending = items.map(({ params: _, ...item }) => ({ ...item, params: "测试摘要" }));
      return { text: `等待用户回应\ncxb wait ${ctx.shortId}\n`, json: { ok: false, status: "waiting", threadId: ctx.threadId, pending } };
    },
  };
}
