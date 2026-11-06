// 只供乙路 M6 用例使用；句柄立即登记，所有等待有期限，清理失败必须报错。
import { tmpdir } from "node:os";
import { spawn, type ChildProcess } from "node:child_process";
import { appendFileSync, existsSync, readFileSync, rmSync, watch, type FSWatcher } from "node:fs";
import net from "node:net";
import { join, resolve, sep } from "node:path";
import { createRpcEndpoint, NO_RESPONSE, type RpcEndpoint } from "../../rpc";
import { BROKER_BIN, createMockCodex, envWithPathPrefix, endpointFor, testSocketPath, writeMockCodexScript } from "../../test-support/broker-harness";

async function bounded<T>(work: Promise<T>, ms: number, description: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([work, new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error(`${description}超时`)), ms); })]);
  } finally { clearTimeout(timer); }
}

function alive(pid: number): boolean {
  try { process.kill(pid, 0); return true; } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
    throw error;
  }
}

async function until(check: () => boolean, ms: number): Promise<void> {
  const deadline = performance.now() + ms;
  while (!check()) {
    if (performance.now() >= deadline) throw new Error("等待资源收束超时");
    await Bun.sleep(20);
  }
}

export class SrHost {
  rpc!: RpcEndpoint;
  readonly requests: unknown[] = [];
  private sockets = new Set<net.Socket>();
  private pids = new Set<number>();
  private children: Array<{ child: ChildProcess; exited: Promise<void> }> = [];
  private disposers: Array<() => void> = [];
  private watcher?: FSWatcher;
  private pidError?: unknown;
  private broker?: ChildProcess;

  constructor(readonly dir: string) {
    this.audit({ dir });
  }

  private audit(value: { dir?: string; pid?: number }): void {
    if (process.env.SR_RESOURCE_LOG) appendFileSync(process.env.SR_RESOURCE_LOG, JSON.stringify(value) + "\n");
  }

  private registerPid(pid: number): void {
    if (!this.pids.has(pid)) {
      this.pids.add(pid);
      this.audit({ pid });
    }
  }

  private child(command: string, args: string[], env?: NodeJS.ProcessEnv): { child: ChildProcess; exited: Promise<void> } {
    const child = spawn(command, args, { cwd: this.dir, env, stdio: "ignore", windowsHide: true });
    if (child.pid) this.registerPid(child.pid);
    const exited = new Promise<void>((resolve, reject) => {
      child.once("exit", () => resolve());
      child.once("error", reject);
    });
    void exited.catch(() => undefined);
    const entry = { child, exited };
    this.children.push(entry);
    return entry;
  }

  async start(): Promise<void> {
    const pidPath = join(this.dir, "sr-mock.pid");
    this.watcher = watch(this.dir, (_event, name) => {
      if (String(name) !== "sr-mock.pid" || !existsSync(pidPath)) return;
      try {
        const pid = Number(readFileSync(pidPath, "utf8"));
        if (Number.isInteger(pid) && pid > 0) this.registerPid(pid);
      } catch (error) { this.pidError = error; }
    });
    this.watcher.on("error", (error) => { this.pidError = error; });
    createMockCodex(this.dir, { sendTurnCompleted: false });
    const mockPath = join(import.meta.dir, "sr-mock.ts");
    writeMockCodexScript(this.dir, `import ${JSON.stringify(mockPath)};\n`);
    const inherited = envWithPathPrefix(this.dir);
    const env = Object.fromEntries(Object.entries(inherited).filter(([key]) => !/^(CXB_|CODEX_|MOCK_)/i.test(key) && !/SOCKET|ENDPOINT/i.test(key)));
    Object.assign(env, { CXB_HOME: join(this.dir, "cxb-home"), CODEX_HOME: join(this.dir, "codex-home"), CXB_SERVER: "private", SR_MOCK_PID: pidPath });
    const socketPath = testSocketPath(this.dir);
    this.broker = this.child(BROKER_BIN, ["serve", "--endpoint", endpointFor(socketPath), "--cwd", this.dir, "--idle-timeout", "60000"], env).child;
    const deadline = performance.now() + 10_000;
    let connected: net.Socket | undefined;
    while (!connected && performance.now() < deadline) {
      const socket = new net.Socket();
      this.sockets.add(socket);
      socket.on("error", () => {});
      try {
        await bounded(new Promise<void>((resolve, reject) => {
          socket.once("connect", resolve);
          socket.once("error", reject);
          socket.connect(socketPath);
        }), Math.max(1, Math.min(500, deadline - performance.now())), "连接命名管道");
        connected = socket;
      } catch {
        socket.destroy();
        await bounded(new Promise<void>((resolve) => {
          if (socket.closed) resolve(); else socket.once("close", () => resolve());
        }), 1000, "关闭失败的连接");
        this.sockets.delete(socket);
        await Bun.sleep(20);
      }
    }
    if (!connected) throw new Error("测试 broker 未能启动");
    const socket = connected;
    this.rpc = createRpcEndpoint({ label: "乙路扩展", requestTimeout: 5000,
      send: (line) => { socket.write(line); }, downReason: () => socket.destroyed ? "连接已关闭" : null,
      overflowReason: "消息过大", onOverflow: () => socket.destroy(), writeFailedPrefix: "写入失败", log() {} });
    socket.setEncoding("utf8");
    socket.on("data", (chunk: string) => this.rpc.feed(chunk));
    socket.on("close", () => this.rpc.fail("连接已关闭"));
    this.disposers.push(this.rpc.onAnyRequest((method, params) => { this.requests.push({ method, params }); return NO_RESPONSE; }));
    await this.rpc.request("initialize", { clientInfo: { name: "乙路测试", version: "1" }, capabilities: { cxb: { protocolVersion: 1 } } });
    this.rpc.notify("initialized");
    // 在任何业务断言前登记 mock 句柄；文件监视覆盖初始化失败的路径。
    const state = await this.rpc.request<{ pid: number }>("mock/state");
    this.registerPid(state.pid);
  }

  async cleanup(): Promise<void> {
    const errors: unknown[] = [];
    const attempt = async (work: () => unknown | Promise<unknown>) => { try { await work(); } catch (error) { errors.push(error); } };
    if (this.rpc) await attempt(() => this.rpc.request("broker/shutdown", {}, { timeoutMs: 5000 }));
    for (const dispose of this.disposers) await attempt(dispose);
    await attempt(() => this.rpc?.markClosed());
    await attempt(() => this.watcher?.close());
    if (this.pidError) errors.push(this.pidError);
    for (const socket of this.sockets) await attempt(async () => {
      socket.destroy();
      await bounded(new Promise<void>((resolve) => { if (socket.closed) resolve(); else socket.once("close", () => resolve()); }), 1000, "关闭连接");
    });
    // 先给 broker 的 Job 清理留出窗口，失败才强杀进程树。
    if (this.broker?.pid && alive(this.broker.pid)) {
      try { await until(() => !alive(this.broker!.pid!), 5000); } catch {
        await attempt(async () => {
          if (process.platform === "win32") {
            const killer = this.child("taskkill", ["/PID", String(this.broker!.pid), "/T", "/F"]);
            try { await bounded(killer.exited, 5000, "强杀 broker 进程树"); } finally { if (killer.child.exitCode === null) killer.child.kill("SIGKILL"); }
          } else this.broker!.kill("SIGKILL");
        });
      }
    }
    for (const pid of this.pids) await attempt(async () => {
      if (alive(pid)) process.kill(pid, "SIGKILL");
      await until(() => !alive(pid), 3000);
    });
    for (const { exited } of this.children) await attempt(() => bounded(exited, 1000, "确认子进程退出"));
    await attempt(async () => {
      const dir = resolve(this.dir);
      const root = resolve(process.env.TEMP ?? process.env.TMP ?? tmpdir());
      if (!dir.startsWith(root + sep)) throw new Error("拒绝删除测试临时根目录以外的路径");
      for (let i = 0; i < 40 && existsSync(dir); i++) {
        try { rmSync(dir, { recursive: true, force: true }); } catch (error) { if (i === 39) throw error; await Bun.sleep(50); }
      }
      if (existsSync(dir)) throw new Error("测试临时目录仍然存在");
    });
    if (errors.length) throw new AggregateError(errors, "乙路扩展资源清理失败");
  }
}
