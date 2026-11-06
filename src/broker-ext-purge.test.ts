import { afterEach, beforeEach, expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, watch, writeFileSync, type FSWatcher } from "node:fs";
import net from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { BROKER_BIN, createMockCodex, endpointFor, envWithPathPrefix, testSocketPath } from "./test-support/broker-harness";
import { processGone, reapTestProcess } from "./test-support/process-identity";

type Frame = { id?: number; method?: string; params?: Record<string, unknown>; result?: any; error?: { code: number; message: string; data?: { reason?: string } } };
let dir: string;
let sockets: Set<net.Socket>;
let clients: Rpc[];
let children: Array<{ child: ChildProcess; exited: Promise<void> }>;
let pids: Set<number>;
let watcher: FSWatcher;
let observed: Frame[];
let observerErrors: unknown[];
let traceOffset: number;

function alive(pid: number): boolean {
  try { process.kill(pid, 0); return true; } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
    throw error;
  }
}
async function bounded<T>(promise: Promise<T>, ms = 10_000, cancel: () => void = () => {}): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_, reject) => {
      timer = setTimeout(() => { reject(new Error("测试操作超过期限")); cancel(); }, ms);
    })]);
  } finally { clearTimeout(timer); }
}
async function until(check: () => boolean, ms = 10_000): Promise<void> {
  const deadline = performance.now() + ms;
  while (!check()) {
    if (performance.now() >= deadline) throw new Error("测试条件未在期限内成立");
    await Bun.sleep(20);
  }
}
function readTrace(): void {
  const path = join(dir, "observed.jsonl");
  if (!existsSync(path)) return;
  const data = readFileSync(path, "utf8");
  const end = data.lastIndexOf("\n") + 1;
  for (const line of data.slice(traceOffset, end).split("\n").filter(Boolean)) {
    const entry = JSON.parse(line);
    if (entry.pid) pids.add(entry.pid);
    if (entry.message) observed.push(entry.message);
  }
  traceOffset = end;
}
class Rpc {
  private next = 1;
  private buffer = "";
  private pending = new Map<number, { resolve: (v: any) => void; reject: (e: unknown) => void; timer: ReturnType<typeof setTimeout> }>();
  constructor(readonly socket: net.Socket) {
    socket.setEncoding("utf8");
    socket.on("data", (chunk: string) => {
      this.buffer += chunk;
      for (let end; (end = this.buffer.indexOf("\n")) >= 0;) {
        const line = this.buffer.slice(0, end); this.buffer = this.buffer.slice(end + 1);
        const frame: Frame = JSON.parse(line);
        if (frame.id === undefined || frame.method) continue;
        const pending = this.pending.get(frame.id);
        if (!pending) continue;
        clearTimeout(pending.timer); this.pending.delete(frame.id);
        if (frame.error) pending.reject(Object.assign(new Error(frame.error.message), frame.error));
        else pending.resolve(frame.result);
      }
    });
    socket.on("close", () => this.abort());
    socket.on("error", () => this.abort());
  }
  abort(): void {
    for (const pending of this.pending.values()) { clearTimeout(pending.timer); pending.reject(new Error("测试连接已关闭")); }
    this.pending.clear(); this.socket.destroy();
  }
  request(method: string, params: unknown = {}): Promise<any> {
    return new Promise((resolve, reject) => {
      const id = this.next++;
      const timer = setTimeout(() => { this.pending.delete(id); reject(new Error(`请求超过期限：${method}`)); this.abort(); }, 45_000);
      this.pending.set(id, { resolve, reject, timer });
      this.socket.write(JSON.stringify({ id, method, params }) + "\n");
    });
  }
}
beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "cxb-n2d-"));
  sockets = new Set(); clients = []; children = []; pids = new Set(); observed = []; observerErrors = []; traceOffset = 0;
  for (const name of ["data", "codex-home"]) mkdirSync(join(dir, name));
  watcher = watch(dir, () => { try { readTrace(); } catch (error) { observerErrors.push(error); } });
  // 透明观测器只记录现有 mock 的输入与进程号，不改应答或时序。
  writeFileSync(join(dir, "observe.ts"), `import { appendFileSync } from "node:fs";
const record = (value: unknown) => appendFileSync(${JSON.stringify(join(dir, "observed.jsonl"))}, JSON.stringify(value) + "\\n");
record({pid:process.pid});
let pending = "";
process.stdin.on("data", (chunk) => {
  pending += String(chunk);
  for (let end; (end = pending.indexOf("\\n")) >= 0;) {
    const line = pending.slice(0,end); pending = pending.slice(end + 1);
    if (line.trim()) {
      const message=JSON.parse(line); record({message});
      if(message.method==="n2/finish") process.stdout.write(JSON.stringify({method:"turn/completed",params:{threadId:"thread-001",turn:{id:message.params.turnId,status:"completed",items:[],error:null}}})+"\\n");
    }
  }
});
`);
});
afterEach(async () => {
  const failures: string[] = [];
  const clean = async (label: string, action: () => unknown | Promise<unknown>) => {
    try { await action(); } catch (error) { failures.push(`${label}：${String(error)}`); }
  };
  for (const client of clients) await clean("关闭请求与定时器", () => client.abort());
  for (const socket of sockets) await clean("关闭连接", () => socket.destroy());
  await clean("收取进程登记", readTrace);
  await clean("关闭观测器", () => watcher.close());
  await Promise.all(children.map(({ child, exited }) => clean("回收 broker", async () => {
    if (child.pid && alive(child.pid)) child.kill("SIGKILL");
    await bounded(exited, 8000);
    if (child.pid) expect(await processGone(child.pid)).toBe(true);
  })));
  await Promise.all([...pids].map(pid => clean("核实子进程退出", async () => {
    await reapTestProcess(pid);
  })));
  await clean("删除测试目录", async () => {
    const deadline = performance.now() + 5000;
    for (;;) {
      try { rmSync(dir, { recursive: true, force: true }); break; } catch (error) {
        if (performance.now() >= deadline) throw error;
        await Bun.sleep(50);
      }
    }
    expect(existsSync(dir)).toBe(false);
  });
  expect(observerErrors).toEqual([]);
  expect(failures).toEqual([]);
}, 30_000);
async function rpcError(promise: Promise<unknown>): Promise<unknown> {
  try { await promise; } catch (error) { return error; }
  throw new Error("应收到 RPC 错误");
}
async function start(): Promise<{ client: Rpc; exited: Promise<void> }> {
  const mock = createMockCodex(dir, { sendTurnCompleted: false });
  // 只调整临时启动器，mock 源文件保持 createMockCodex 生成的原样。
  writeFileSync(join(mock, "codex.cmd"), `@echo off\r\n"${process.execPath}" --preload "%~dp0observe.ts" "%~dp0codex-mock.ts" %*\r\n`);
  const env = Object.fromEntries(Object.entries(envWithPathPrefix(mock)).filter(([key]) => {
    const name = key.toUpperCase();
    return !["CXB_HOME", "CODEX_HOME", "CXB_SERVER"].includes(name)
      && !/SOCKET|ENDPOINT/.test(name) && !name.startsWith("MOCK_");
  }));
  Object.assign(env, { CXB_HOME: join(dir, "data"), CODEX_HOME: join(dir, "codex-home"), CXB_SERVER: "private" });
  const path = testSocketPath(dir);
  const child = spawn(BROKER_BIN, ["serve", "--cwd", dir, "--endpoint", endpointFor(path), "--idle-timeout", "60000"], {
    env, cwd: dir, windowsHide: true, stdio: "ignore",
  });
  if (child.pid) pids.add(child.pid);
  const exited = new Promise<void>((resolve, reject) => { child.once("exit", () => resolve()); child.once("error", reject); });
  void exited.catch(() => {});
  children.push({ child, exited });
  let socket: net.Socket | undefined;
  const deadline = performance.now() + 15000;
  while (!socket) {
    const candidate = new net.Socket(); sockets.add(candidate);
    try {
      await bounded(new Promise<void>((resolve, reject) => {
        candidate.once("connect", resolve); candidate.once("error", reject); candidate.connect(path);
      }), 1000, () => candidate.destroy());
      socket = candidate;
    } catch (error) {
      candidate.destroy();
      if (performance.now() >= deadline) throw error;
      await Bun.sleep(20);
    }
  }
  const client = new Rpc(socket); clients.push(client);
  await client.request("initialize", { clientInfo: { name: "丁路测试", version: "1" }, capabilities: { cxb: { protocolVersion: 1 } } });
  socket.write(JSON.stringify({ method: "initialized" }) + "\n");
  readTrace();
  return { client, exited };
}
function workspaceDir(): string {
  const base = join(dir, "data", "workspaces");
  const entries = readdirSync(base); expect(entries).toHaveLength(1);
  return join(base, entries[0]!);
}
async function shutdown(instance: { client: Rpc; exited: Promise<void> }): Promise<void> {
  await instance.client.request("broker/shutdown");
  await bounded(instance.exited, 20_000);
  instance.client.abort();
}

test("purge 拒绝运行中线程，终态后删净文件且重启后重复调用幂等", async () => {
  let instance = await start();
  await instance.client.request("thread/start");
  const run = await instance.client.request("turn/start", { threadId: "thread-001", input: [{ type: "text", text: "删除测试" }] });
  expect(await rpcError(instance.client.request("broker/threads/purge", { threadId: "thread-001" }))).toMatchObject({ code: -32001, data: { reason: "threadBusy" } });
  // 通过观测器放行完成通知，先取得真实终态，再验证跨重启删除。
  await rpcError(instance.client.request("n2/finish", {turnId:run.turn.id}));
  const settled=await instance.client.request("broker/threads/watch",{threadId:"thread-001"});
  expect(settled.run.state).toBe("ended");
  await shutdown(instance);
  instance = await start();
  const path = join(workspaceDir(), "runs");
  const before = observed.length;
  const first = await instance.client.request("broker/threads/purge", { threadId: "thread-001" });
  expect(first.threadId).toBe("thread-001");
  expect(first.failed).toEqual([]);
  expect(first.deleted).toContain(`runs/${run.runId}.json`);
  expect(first.deleted.some((file: string) => file.endsWith(".log"))).toBe(true);
  expect(readdirSync(path)).toEqual([]);
  const second = await instance.client.request("broker/threads/purge", { threadId: "thread-001" });
  expect(second.failed).toEqual([]);
  expect(second.deleted).toEqual([]);
  expect([...second.alreadyDeleted].sort()).toEqual([...first.deleted, ...first.alreadyDeleted].sort());
  readTrace();
  expect(observed.slice(before).some(frame => frame.method === "thread/delete")).toBe(false);
  await shutdown(instance);
  instance = await start();
  const third = await instance.client.request("broker/threads/purge", { threadId: "thread-001" });
  expect(third).toEqual(second);
  expect(readdirSync(path)).toEqual([]);
  await shutdown(instance);
}, 90_000);

test("purge 参数必须是非空线程标识，未知线程按空清单幂等", async () => {
  const instance = await start();
  for (const params of [{}, { threadId: "" }, { threadId: 42 }, []]) {
    expect(await rpcError(instance.client.request("broker/threads/purge", params))).toMatchObject({ code: -32602 });
  }
  expect(await instance.client.request("broker/threads/purge", { threadId: "unknown" })).toEqual({ threadId: "unknown", deleted: [], alreadyDeleted: [], failed: [] });
  await shutdown(instance);
}, 60_000);
