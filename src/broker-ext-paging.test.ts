/** N6：真实 Rust broker 的分页补读；所有资源由钩子按创建句柄收束。 */
import { afterEach, beforeEach, expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, watch, writeFileSync, type FSWatcher } from "node:fs";
import net from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { BROKER_BIN, envWithPathPrefix, testSocketPath, endpointFor, writeMockCodexScript } from "./test-support/broker-harness";

let dir: string;
let children: Array<{ child: ChildProcess; exited: Promise<number | null> }>;
let sockets: Set<net.Socket>;
let pids: Set<number>;
let watchers: FSWatcher[];
let timers: Set<ReturnType<typeof setTimeout>>;
let stderr: string;

function bounded<T>(promise: Promise<T>, ms: number, description: string): Promise<T> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => { timers.delete(timer); reject(new Error(`${description}超时`)); }, ms);
    timers.add(timer);
    promise.then(resolve, reject).finally(() => { clearTimeout(timer); timers.delete(timer); });
  });
}
async function until(check: () => boolean | Promise<boolean>, ms: number, description: string) {
  const deadline = performance.now() + ms;
  while (!(await check())) {
    if (performance.now() >= deadline) throw new Error(`${description}超时：${stderr}`);
    await Bun.sleep(20);
  }
}
function alive(pid: number) {
  try { process.kill(pid, 0); return true; } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
    throw error;
  }
}

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "cxb-n2c-paging-"));
  children = [];
  sockets = new Set();
  pids = new Set();
  watchers = [];
  timers = new Set();
  stderr = "";
});

afterEach(async () => {
  const failures: string[] = [];
  const clean = async (label: string, action: () => unknown | Promise<unknown>) => {
    try { await action(); } catch (error) { failures.push(`${label}：${String(error)}`); }
  };
  for (const socket of sockets) await clean("关闭连接", () => socket.destroy());
  for (const { child } of children) await clean("结束 broker", () => {
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
  });
  await Promise.all(children.map(({ exited }) => clean("确认 broker 退出", () => bounded(exited, 8000, "broker 退出"))));
  await Promise.all([...pids].map((pid) => clean(`回收进程 ${pid}`, async () => {
    try { await until(() => !alive(pid), 3000, "子进程退出"); }
    catch {
      if (alive(pid)) process.kill(pid, "SIGKILL");
      await until(() => !alive(pid), 5000, "强杀后确认退出");
    }
    expect(alive(pid)).toBe(false);
  })));
  for (const watcher of watchers) await clean("关闭目录监测", () => watcher.close());
  for (const { child } of children) {
    await clean("关闭标准输入", () => child.stdin?.destroy());
    await clean("关闭标准输出", () => child.stdout?.destroy());
    await clean("关闭标准错误", () => child.stderr?.destroy());
  }
  for (const timer of timers) clearTimeout(timer);
  await clean("删除临时目录", async () => {
    let error: unknown;
    for (let i = 0; i < 40; i++) {
      try { rmSync(dir, { recursive: true, force: true }); error = undefined; break; }
      catch (caught) { error = caught; await Bun.sleep(50); }
    }
    if (error) throw error;
    expect(existsSync(dir)).toBe(false);
  });
  expect(failures).toEqual([]);
}, 35_000);

class Client {
  private pending = new Map<number, { resolve(value: any): void; reject(error: Error): void }>();
  private id = 0;
  private buffer = "";
  constructor(readonly socket: net.Socket) {
    socket.setEncoding("utf8");
    socket.on("data", (chunk: string) => {
      this.buffer += chunk;
      let end: number;
      while ((end = this.buffer.indexOf("\n")) >= 0) {
        const message = JSON.parse(this.buffer.slice(0, end));
        this.buffer = this.buffer.slice(end + 1);
        const pending = this.pending.get(message.id);
        if (!pending || message.method) continue;
        if (message.error) pending.reject(new Error(JSON.stringify(message.error)));
        else pending.resolve(message.result);
      }
    });
    const fail = () => { for (const p of this.pending.values()) p.reject(new Error("测试连接已关闭")); };
    socket.on("error", fail);
    socket.on("close", fail);
  }
  async request(method: string, params: unknown = {}) {
    const id = ++this.id;
    try {
      return await bounded(new Promise<any>((resolve, reject) => {
        this.pending.set(id, { resolve, reject });
        this.socket.write(JSON.stringify({ id, method, params }) + "\n");
      }), 5000, method);
    } finally { this.pending.delete(id); }
  }
}

async function start(mode: string) {
  const dataRoot = join(dir, "data");
  const codexHome = join(dir, "codex-home");
  mkdirSync(dataRoot);
  mkdirSync(codexHome);
  const pidFile = join(dir, "attempts.log");
  writeFileSync(pidFile, "");
  // 模拟程序创建时即写 PID，目录监测随事件登记，不依赖 broker.json。
  const register = () => {
    for (const line of readFileSync(pidFile, "utf8").trim().split("\n")) {
      if (/^\d+$/.test(line)) pids.add(Number(line));
    }
  };
  watchers.push(watch(dir, (_event, name) => { if (name === "attempts.log") register(); }));
  const fixture = resolve(import.meta.dir, "../broker-rs/tests/fixtures/app-server.ts");
  writeMockCodexScript(dir, `import ${JSON.stringify(fixture)};\n`);
  const env = Object.fromEntries(Object.entries(envWithPathPrefix(dir)).filter(([key]) =>
    !/^(CXB_HOME|CODEX_HOME|CXB_SERVER.*|.*SOCKET.*|.*ENDPOINT.*|MOCK_PROXY_.*|MOCK_PAGING_MODE)$/i.test(key)));
  Object.assign(env, { CXB_HOME: dataRoot, CODEX_HOME: codexHome, CXB_SERVER: "private", MOCK_PAGING_MODE: mode });
  const socketPath = testSocketPath(dir);
  const child = spawn(BROKER_BIN, ["serve", "--endpoint", endpointFor(socketPath), "--cwd", dir, "--idle-timeout", "60000"], {
    cwd: dir, env, windowsHide: true, stdio: ["pipe", "pipe", "pipe"],
  });
  const exited = new Promise<number | null>((resolve, reject) => { child.once("error", reject); child.once("close", resolve); });
  void exited.catch(() => {});
  children.push({ child, exited });
  if (child.pid) pids.add(child.pid);
  child.stdout!.on("data", () => {});
  child.stderr!.setEncoding("utf8");
  child.stderr!.on("data", (chunk: string) => { stderr += chunk; });
  await until(() => { register(); return pids.size >= 2; }, 12_000, "模拟进程启动");
  const socket = new net.Socket();
  sockets.add(socket);
  const client = new Client(socket);
  try {
    await bounded(new Promise<void>((resolve, reject) => {
      socket.once("connect", resolve);
      socket.once("error", reject);
      socket.connect(socketPath);
    }), 5000, "管道连接");
    await client.request("initialize", { clientInfo: { name: "分页测试", version: "1" }, capabilities: { cxb: { protocolVersion: 1 } } });
  } catch (error) { socket.destroy(); throw error; }
  await client.request("thread/resume", { threadId: "分页线程" });
  return { client, exited, dataRoot };
}

async function run(client: Client) {
  const started = await client.request("turn/start", { threadId: "分页线程", input: [{ type: "text", text: "分页测试" }] });
  let result: any;
  await until(async () => {
    const snapshot = await client.request("broker/threads/watch", { threadId: "分页线程" });
    result = snapshot.run;
    return result?.runId === started.runId && ["ready", "unavailable"].includes(result.turns.at(-1)?.ready);
  }, 20_000, "补读结果落定");
  return result;
}

test("多页补读完整且按已知轮次定位，不读取长线程全历史", async () => {
  const { client } = await start("paging-multi");
  for (let turn = 1; turn <= 3; turn++) {
    const result = await run(client);
    expect(result.turns.at(-1).ready).toBe("ready");
    expect(result.turns.at(-1).output).toBe("分页一 中文\n分页二 中文");
  }
  const { calls } = await client.request("fixture/state");
  const pages = calls.filter((call: any) => call.method === "thread/items/list");
  expect(pages.length).toBe(9);
  expect(new Set(pages.map((call: any) => call.params.turnId)).size).toBe(3);
  expect(calls.filter((call: any) => call.method === "thread/read")).toEqual([]);
  expect(pages.every((call: any) => call.params.sortDirection === "asc")).toBe(true);
  await client.request("broker/shutdown");
}, 35_000);

test("不支持条目分页时不回退全历史：降级为摘要快照并标 unavailable，整个 broker 只告警一次", async () => {
  const { client, exited } = await start("paging-unsupported");
  for (let turn = 0; turn < 2; turn++) {
    const result = await run(client);
    expect(result.turns.at(-1).ready).toBe("unavailable");
    expect(result.turns.at(-1).output).toBe("分页二 中文");
  }
  const { calls } = await client.request("fixture/state");
  // 每轮只试一次条目分页，不重试、不发整线程读取。
  expect(calls.filter((call: any) => call.method === "thread/items/list").length).toBe(2);
  expect(calls.filter((call: any) => call.method === "thread/read")).toEqual([]);
  await client.request("broker/shutdown");
  await bounded(exited, 10_000, "正常退出");
  expect(stderr.match(/上游不支持条目分页/g)?.length).toBe(1);
}, 35_000);

test("分页中途失败按一二四秒退避三次后可辨认地降级", async () => {
  const { client } = await start("paging-fail-middle");
  const started = performance.now();
  const result = await run(client);
  expect(performance.now() - started).toBeGreaterThanOrEqual(7000);
  expect(result.turns.at(-1).ready).toBe("unavailable");
  expect(result.turns.at(-1).output).toBe("分页二 中文");
  const { calls } = await client.request("fixture/state");
  const pages = calls.filter((call: any) => call.method === "thread/items/list");
  expect(pages.length).toBe(8);
  expect(pages.filter((call: any) => call.params.cursor === null).length).toBe(4);
  expect(calls.filter((call: any) => call.method === "thread/read")).toEqual([]);
  await client.request("broker/shutdown");
}, 35_000);

test("单条超过十兆使传输关闭，封口结果使用摘要且标记 unavailable", async () => {
  const { client, exited, dataRoot } = await start("paging-huge");
  await client.request("turn/start", { threadId: "分页线程", input: [{ type: "text", text: "巨大条目测试" }] });
  await bounded(exited, 20_000, "超大条目后的 broker 收束");
  const files = readdirSync(dataRoot, { recursive: true }) as string[];
  const runs = files.filter((file) => /[/\\]runs[/\\].+\.json$/.test(file)).map((file) => JSON.parse(readFileSync(join(dataRoot, file), "utf8")).payload);
  expect(runs.length).toBe(1);
  expect(runs[0].turns.at(-1).ready).toBe("unavailable");
  expect(runs[0].turns.at(-1).output).toBe("分页二 中文");
}, 35_000);
