import { afterEach, beforeEach, expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, watch, writeFileSync, type FSWatcher } from "node:fs";
import net from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { BROKER_BIN, createMockCodex, endpointFor, envWithPathPrefix, testSocketPath } from "./test-support/broker-harness";

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
let baseObserver: string;

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
async function until(check: () => boolean | Promise<boolean>, ms = 10_000): Promise<void> {
  const deadline = performance.now() + ms;
  while (!(await check())) {
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
  notifications: Frame[] = [];
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
        if (frame.id === undefined || frame.method) { this.notifications.push(frame); continue; }
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
  dir = mkdtempSync(join(tmpdir(), "cxb-n2d-fix-"));
  sockets = new Set(); clients = []; children = []; pids = new Set(); observed = []; observerErrors = []; traceOffset = 0; baseObserver = "";
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
    if (line.trim()) record({message:JSON.parse(line)});
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
  // broker 子进程按句柄判定：exit 事件就是已退出的证据，不再按 PID 回查（PID 可能已被新进程复用，回查与强杀都会误伤；
  // N7-A 证伪实验 30 遍未复现「已退出的 PID 仍存活」，按更稳的写法收尾）。
  await Promise.all(children.map(({ child, exited }) => clean("回收 broker", async () => {
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
    await bounded(exited, 8000);
  })));
  const brokerPids = new Set(children.map(({ child }) => child.pid));
  // 其余登记的是 broker 拉起的模拟 codex（与 broker 同一 Job，broker 退出即随之结束），没有句柄只能按 PID 核实。
  await Promise.all([...pids].filter((pid) => !brokerPids.has(pid)).map(pid => clean("核实子进程退出", async () => {
    if (alive(pid)) process.kill(pid, "SIGKILL");
    await until(() => !alive(pid), 5000);
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
  const mock = createMockCodex(dir, { backgroundTerminals: { enabled: true }, sendTurnCompleted: false, legacyReview: true });
  baseObserver ||= readFileSync(join(dir, "observe.ts"), "utf8");
  writeFileSync(join(dir, "observe.ts"), baseObserver + transportObserver);
  // 只调整临时启动器，mock 源文件保持 createMockCodex 生成的原样。
  writeFileSync(join(mock, "codex.cmd"), `@echo off\r\n"${process.execPath}" --preload "%~dp0observe.ts" "%~dp0codex-mock.ts" %*\r\n`);
  const env = Object.fromEntries(Object.entries(envWithPathPrefix(mock)).filter(([key]) => {
    const name = key.toUpperCase();
    return !["CXB_HOME", "CODEX_HOME", "USERPROFILE", "HOME", "CXB_SERVER"].includes(name)
      && !/SOCKET|ENDPOINT/.test(name) && !name.startsWith("MOCK_");
  }));
  Object.assign(env, { CXB_HOME: join(dir, "data"), CODEX_HOME: join(dir, "codex-home"), USERPROFILE: join(dir, "profile"), HOME: join(dir, "profile"), CXB_SERVER: "private" });
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

// 测试局部传输观测器：使用既有 mock 的应答，只注入准确错误、通知与扣住/放行控制点。
const transportObserver = `
const n2Requests = [];
const n2ById = new Map();
const n2Held = [];
let n2Fault = null;
let n2WireBuffer = "";
process.stdin.on("data", chunk => {
  n2WireBuffer += String(chunk);
  for (let end; (end = n2WireBuffer.indexOf("\\n")) >= 0;) {
    const line = n2WireBuffer.slice(0,end); n2WireBuffer = n2WireBuffer.slice(end+1);
    if (!line.trim()) continue;
    const message = JSON.parse(line);
    if (message.method && message.id !== undefined) { n2Requests.push(message); n2ById.set(String(message.id),message); }
  }
});
const n2Write = process.stdout.write.bind(process.stdout);
const n2Send = message => n2Write(JSON.stringify(message) + "\\n");
process.stdout.write = (chunk, ...args) => {
  let message;
  try { message = JSON.parse(String(chunk).trim()); } catch { return n2Write(chunk,...args); }
  const request = n2ById.get(String(message.id));
  if (!request || message.method) return n2Write(chunk,...args);
  if (request.method === "n2/config") { n2Fault = request.params; return n2Send({id:message.id,result:{}}); }
  if (request.method === "n2/state") return n2Send({id:message.id,result:{requests:n2Requests,held:n2Held.length}});
  if (request.method === "n2/notify") {
    n2Send(request.params);
    return n2Send({id:message.id,result:{}});
  }
  if (request.method === "n2/release") {
    for (const held of n2Held.splice(0)) n2Send(held);
    return n2Send({id:message.id,result:{}});
  }
  if (request.params?.n2Hold) { n2Held.push(message); return true; }
  if (request.method === "thread/resume" && n2Fault?.holdResume) { n2Held.push(message); return true; }
  if (n2Fault?.stopControl && request.method === "thread/goal/get") return n2Send({id:message.id,result:{goal:null}});
  if (n2Fault?.stopControl && request.method === "turn/interrupt") {
    n2Send({method:"turn/completed",params:{threadId:request.params.threadId,turn:{id:request.params.turnId,status:"interrupted",items:[],error:null}}});
    return n2Send({id:message.id,result:{}});
  }

  if (request.method === "thread/resume" && n2Fault) {
    const key = (n2Fault.keys || []).find(key => Object.hasOwn(request.params?.config || {},key));
    if (key || n2Fault.message) {
      return n2Send({id:message.id,error:{code:n2Fault.code ?? -32600,message:n2Fault.message || ("failed to load configuration: unknown configuration field "+String.fromCharCode(96)+key+String.fromCharCode(96)+" in -c/--config override")}});
    }
  }
  return n2Write(chunk,...args);
};
`;

const overrides = { config: { "bad.key": true, "keep.key": 7 }, developerInstructions: "需要人类决定时提问" };
async function begin(client: Rpc) {
  await client.request("thread/start");
  await client.request("broker/threads/resumeOverrides", { threadId: "thread-001", overrides });
}
async function finish(client: Rpc, threadId: string, turnId: string) {
  await client.request("n2/notify", { method: "turn/completed", params: { threadId, turn: { id: turnId, status: "completed", error: null, items: [{ id: "answer", type: "agentMessage", phase: "final_answer", text: "已完成本轮" }] } } });
  await until(async () => (await client.request("broker/threads/watch", { threadId })).thread.state === "idle");
}

for (const status of ["blocked", "budgetLimited", "usageLimited", "complete"]) {
  test(`目标 ${status} 的运行终态不会误报成功`, async () => {
    const instance = await start(); const client = instance.client;
    await begin(client);
    const opened = await client.request("turn/start", { threadId: "thread-001", input: [{ type: "text", text: "目标测试" }] });
    await client.request("n2/notify", { method: "thread/goal/updated", params: { threadId: "thread-001", goal: { objective: "目标", createdAt: 1, status } } });
    await finish(client, "thread-001", opened.turn.id);
    const snapshot = await client.request("broker/threads/watch", { threadId: "thread-001" });
    expect(snapshot.run.status).toBe(status === "complete" ? "completed" : "goalBlocked");
    expect(snapshot.run.state).toBe("ended");
    await shutdown(instance);
    const saved = JSON.parse(readFileSync(join(workspaceDir(), "runs", `${opened.runId}.json`), "utf8"));
    expect(saved.payload.status).toBe(snapshot.run.status);
  }, 60_000);
}

for (const mode of ["barrier", "continue", "guardian"] as const) {
  test(`内部恢复 ${mode} 未知键只删除命中项并重试一次`, async () => {
    let instance = await start(); await begin(instance.client);
    let runId: string | undefined;
    if (mode !== "barrier") {
      const run = await instance.client.request("turn/start", { threadId: "thread-001", input: [{ type: "text", text: "恢复测试" }] });
      runId = run.runId;
    }
    if (mode !== "guardian") { await shutdown(instance); instance = await start(); }
    await instance.client.request("n2/config", { keys: ["bad.key"] });
    if (mode === "guardian") {
      await instance.client.request("n2/notify", { method: "item/autoApprovalReview/completed", params: { threadId: "thread-001", turnId: "turn-001", reviewId: "guardian-n2", review: { status: "denied" }, action: { type: "command" } } });
      let inbox: any;
      await until(async () => {
        inbox = await instance.client.request("broker/inbox/list", { threadId: "thread-001" });
        return inbox.items.some((item: any) => item.kind === "guardian");
      });
      const item = inbox.items.find((item: any) => item.kind === "guardian");
      // 既有 mock 不实现最终 override；此处关注它之前实际发生的两次恢复。
      await rpcError(instance.client.request("broker/inbox/resolve", { id: item.id, answer: { override: true, event: { review_id: "guardian-n2" } } }));
    } else {
      await instance.client.request("turn/start", { threadId: "thread-001", input: [{ type: "text", text: "恢复后的工作" }], ...(mode === "continue" ? { cxbContinue: { fromRunId: runId } } : {}) });
    }
    const state = await instance.client.request("n2/state");
    const resumes = state.requests.filter((r: any) => r.method === "thread/resume");
    expect(resumes).toHaveLength(2);
    expect(resumes[0].params.config).toEqual(overrides.config);
    expect(resumes[1].params.config).toEqual({ "keep.key": 7 });
    expect(resumes[1].params.developerInstructions).toBe(overrides.developerInstructions);
    if (mode === "guardian") expect(resumes[1].params.persistExtendedHistory).toBe(false);
    await shutdown(instance);
    const saved = JSON.parse(readFileSync(join(workspaceDir(), "threads.json"), "utf8"));
    expect(Object.values(saved.payload.threads).map((entry: any) => entry.resumeOverrides?.config)).toContainEqual({ "keep.key": 7 });
  }, 90_000);
}

test("内部恢复非未知键错误不重试，第二个未知键也不重试", async () => {
  let instance = await start(); await begin(instance.client); await shutdown(instance);
  instance = await start();
  await instance.client.request("n2/config", { message: "其他无效请求", code: -32600 });
  await instance.client.request("turn/start", { threadId: "thread-001", input: [{ type: "text", text: "普通错误" }] });
  let state = await instance.client.request("n2/state");
  expect(state.requests.filter((r: any) => r.method === "thread/resume")).toHaveLength(1);
  await shutdown(instance); instance = await start();
  await instance.client.request("n2/config", { keys: ["bad.key", "keep.key"] });
  await instance.client.request("turn/start", { threadId: "thread-001", input: [{ type: "text", text: "两个未知键" }] });
  state = await instance.client.request("n2/state");
  expect(state.requests.filter((r: any) => r.method === "thread/resume")).toHaveLength(2);
  await shutdown(instance);
}, 90_000);

test("purge 整体拒绝旧审查活动及子线程在途写入，删除后迟到通知不复活", async () => {
  const instance = await start(); const client = instance.client;
  await begin(client);
  const review = await client.request("review/start", { threadId: "thread-001", target: { type: "uncommittedChanges" }, delivery: "detached" });
  const parent = await client.request("turn/start", { threadId: "thread-001", input: [{ type: "text", text: "父线程新运行" }] });
  await finish(client, "thread-001", parent.turn.id);
  expect(await rpcError(client.request("broker/threads/purge", { threadId: "thread-001" }))).toMatchObject({ code: -32001 });
  await finish(client, review.reviewThreadId, review.turn.id);
  const held = client.request("thread/resume", { threadId: review.reviewThreadId, n2Hold: true });
  void held.catch(() => {});
  await until(async () => (await client.request("n2/state")).held === 1);
  expect(await rpcError(client.request("broker/threads/purge", { threadId: "thread-001" }))).toMatchObject({ code: -32001, data: { reason: "settling" } });
  await client.request("n2/release"); await held;
  let result: any;
  await until(async () => {
    try { result = await client.request("broker/threads/purge", { threadId: "thread-001" }); return true; }
    catch (error) { if ((error as any).code === -32001) return false; throw error; }
  });
  expect(result.failed).toEqual([]);
  expect(result.deleted).toContain(`runs/${review.runId}.json`);
  expect(result.deleted).toContain(`runs/${parent.runId}.json`);
  await client.request("n2/notify", { method: "turn/completed", params: { threadId: review.reviewThreadId, turn: { id: review.turn.id, status: "completed", items: [], error: null } } });
  await shutdown(instance);
  expect(readdirSync(join(workspaceDir(), "runs"))).toEqual([]);
}, 90_000);


test("按运行 stop 不停尚未认领的运行，旧运行结束后也不停后继", async () => {
  const instance=await start();const client=instance.client;await begin(client);
  const held=client.request("thread/resume",{threadId:"thread-001",n2Hold:true});void held.catch(()=>{});
  await until(async()=>(await client.request("n2/state")).held===1);
  expect(await rpcError(client.request("broker/threads/stop",{threadId:"thread-001",runId:"not-owned"}))).toMatchObject({code:-32000,data:{reason:"runMismatch"}});
  await client.request("n2/release");await held;
  const old=await client.request("turn/start",{threadId:"thread-001",input:[{type:"text",text:"旧运行"}]});
  await client.request("n2/config",{stopControl:true});
  expect(await rpcError(client.request("broker/threads/stop",{threadId:"thread-001",runId:"wrong-active"}))).toMatchObject({code:-32000,data:{reason:"runMismatch"}});
  await finish(client,"thread-001",old.turn.id);
  const next=await client.request("review/start",{threadId:"thread-001",target:{type:"uncommittedChanges"},delivery:"inline"});
  const before=await client.request("n2/state");
  // 后继已占用线程：旧 runId 不能取得本线程后台命令的清理授权，也不能快捷确认。
  expect(await rpcError(client.request("broker/threads/stop",{threadId:"thread-001",runId:old.runId,clear:true}))).toMatchObject({code:-32000,data:{reason:"runMismatch"}});
  expect(await rpcError(client.request("broker/threads/stop",{threadId:"thread-001",runId:"wrong"}))).toMatchObject({code:-32000,data:{reason:"runMismatch"}});
  const after=await client.request("n2/state");
  expect(after.requests.slice(before.requests.length).some((r:any)=>["turn/interrupt","thread/goal/set","thread/goal/clear"].includes(r.method))).toBe(false);
  expect((await client.request("broker/threads/watch",{threadId:next.reviewThreadId})).run.runId).toBe(next.runId);
  await shutdown(instance);
},60000);

test("按运行 stop 从父线程定位 review 子线程且不触碰父线程新运行",async()=>{
  const instance=await start();const client=instance.client;await begin(client);
  const review=await client.request("review/start",{threadId:"thread-001",target:{type:"uncommittedChanges"},delivery:"detached"});
  const parent=await client.request("turn/start",{threadId:"thread-001",input:[{type:"text",text:"父线程新任务"}]});
  await client.request("n2/config",{stopControl:true});
  const before=await client.request("n2/state");
  const stopped=await client.request("broker/threads/stop",{threadId:"thread-001",runId:review.runId});
  expect(stopped).toMatchObject({runId:review.runId,stopOutcome:"confirmed"});
  const after=await client.request("n2/state");
  const controls=after.requests.slice(before.requests.length).filter((r:any)=>["turn/interrupt","thread/goal/set","thread/goal/clear"].includes(r.method));
  expect(controls.length).toBeGreaterThan(0);expect(controls.every((r:any)=>r.params.threadId===review.reviewThreadId)).toBe(true);
  expect((await client.request("broker/threads/watch",{threadId:"thread-001"})).run.runId).toBe(parent.runId);
  await shutdown(instance);
},60000);

test("Guardian 回复与 purge 以事项线程互锁，扣住的恢复放行后才能删除",async()=>{
  const instance=await start();const client=instance.client;await begin(client);
  const run=await client.request("turn/start",{threadId:"thread-001",input:[{type:"text",text:"守护测试"}]});
  await client.request("n2/notify",{method:"item/autoApprovalReview/completed",params:{threadId:"thread-001",turnId:run.turn.id,reviewId:"purge-guardian",review:{status:"denied"},action:{type:"command"}}});
  let item:any;await until(async()=>{const inbox=await client.request("broker/inbox/list",{threadId:"thread-001"});item=inbox.items.find((i:any)=>i.kind==="guardian");return !!item;});
  await finish(client,"thread-001",run.turn.id);
  expect(await rpcError(client.request("broker/threads/purge",{threadId:"thread-001"}))).toMatchObject({code:-32001,data:{reason:"purgePending"}});
  await client.request("n2/config",{holdResume:true});
  const resolving=rpcError(client.request("broker/inbox/resolve",{id:item.id,answer:{override:true,event:{review_id:"purge-guardian"}}}));
  await until(async()=>(await client.request("n2/state")).held===1);
  expect(await rpcError(client.request("broker/threads/purge",{threadId:"thread-001"}))).toMatchObject({code:-32001});
  await client.request("n2/config",{});await client.request("n2/release");await resolving;
  await client.request("broker/inbox/resolve",{id:item.id,answer:{override:false}});
  // 同一管道批次有序入箱，删除准备的计时回执前投递无 threadId 的重复回答。
  // 刚结束那一轮的读仍在途时 purge 按设计临时回 settling、不挂删除锁（purge.rs 的拒绝顺序），
  // 同批重复回答按幂等成功；重试到受理为止，受理那一批的重复回答必须被删除锁拒绝。
  const outcome=(p:Promise<any>)=>p.then((ok:any)=>({ok,err:undefined}),(err:any)=>({ok:undefined,err}));
  let deleted:any;
  await until(async()=>{
    client.socket.cork();
    const purging=outcome(client.request("broker/threads/purge",{threadId:"thread-001"}));
    const duplicate=outcome(client.request("broker/inbox/resolve",{id:item.id,answer:{override:false}}));
    client.socket.uncork();
    const [d,p]=await Promise.all([duplicate,purging]);
    if(p.err){
      expect(p.err).toMatchObject({code:-32001,data:{reason:"settling"}});
      expect(d.ok).toMatchObject({status:"resolved",duplicate:true});
      return false;
    }
    expect(d.err).toMatchObject({code:-32001,data:{reason:"purgePending"}});
    deleted=p.ok;return true;
  });
  expect(deleted.failed).toEqual([]);
  await shutdown(instance);
  const index=JSON.parse(readFileSync(join(workspaceDir(),"threads.json"),"utf8")).payload;
  expect(Object.values(index.threads).some((e:any)=>e.threadId==="thread-001")).toBe(false);
  expect(index.removed["thread-001"]).toBeNumber();
},60000);

test("重启失联运行未经 stop 确认不得 purge",async()=>{
  let instance=await start();await begin(instance.client);
  await instance.client.request("turn/start",{threadId:"thread-001",input:[{type:"text",text:"失联测试"}]});
  await shutdown(instance);instance=await start();
  expect(await rpcError(instance.client.request("broker/threads/purge",{threadId:"thread-001"}))).toMatchObject({code:-32001,data:{reason:"stopping"}});
  await instance.client.request("n2/config",{stopControl:true});
  expect(await instance.client.request("broker/threads/stop",{threadId:"thread-001"})).toMatchObject({stopOutcome:"confirmed"});
  let result:any;await until(async()=>{try{result=await instance.client.request("broker/threads/purge",{threadId:"thread-001"});return true;}catch(error){if((error as any).code===-32001)return false;throw error;}});
  expect(result.failed).toEqual([]);await shutdown(instance);
},60000);

test("watch 带 runId 给指定的旧运行，最近一次运行不受影响（S4 N4）",async()=>{
  const instance=await start();const client=instance.client;await begin(client);
  const first=await client.request("turn/start",{threadId:"thread-001",input:[{type:"text",text:"第一次"}]});
  await finish(client,"thread-001",first.turn.id);
  const second=await client.request("turn/start",{threadId:"thread-001",input:[{type:"text",text:"第二次"}]});
  await client.request("thread/resume",{threadId:"thread-002"});
  const other=await client.request("turn/start",{threadId:"thread-002",input:[{type:"text",text:"别的线程"}]});
  // 内存账本每个线程只留最近一次运行（runs/state.rs evict）：已结束的旧运行给 null，不能错给更新的那次，CLI 改读磁盘。
  const old=await client.request("broker/threads/watch",{threadId:"thread-001",runId:first.runId});
  expect(old.run).toBeNull();
  expect(old.thread.lastRunId).toBe(second.runId);
  expect((await client.request("broker/threads/watch",{threadId:"thread-001"})).run.runId).toBe(second.runId);
  expect((await client.request("broker/threads/watch",{threadId:"thread-001",runId:second.runId})).run).toMatchObject({runId:second.runId,state:"running"});
  expect((await client.request("broker/threads/watch",{threadId:"thread-001",runId:other.runId})).run).toBeNull();
  expect((await client.request("broker/threads/watch",{threadId:"thread-001",runId:"run-不存在"})).run).toBeNull();
  for(const runId of [7,"",null]) expect(await rpcError(client.request("broker/threads/watch",{threadId:"thread-001",runId}))).toMatchObject({code:-32602});
  await shutdown(instance);
},60000);
