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
  onNotification?: (frame: Frame) => void;
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
        if (frame.id === undefined || frame.method) { this.notifications.push(frame); this.onNotification?.(frame); continue; }
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
  dir = mkdtempSync(join(tmpdir(), "cxb-n4-review-"));
  sockets = new Set(); clients = []; children = []; pids = new Set(); observed = []; observerErrors = []; traceOffset = 0; baseObserver = "";
  for (const name of ["data", "codex-home", "profile"]) mkdirSync(join(dir, name));
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
  await Promise.all(children.map(({ child, exited }) => clean("回收 broker", async () => {
    if (child.pid && alive(child.pid)) child.kill("SIGKILL");
    await bounded(exited, 8000);
    if (child.pid) expect(alive(child.pid)).toBe(false);
  })));
  await Promise.all([...pids].map(pid => clean("核实子进程退出", async () => {
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
  Object.assign(env, { CXB_HOME: join(dir, "data"), CODEX_HOME: join(dir, "codex-home"),
    USERPROFILE: join(dir, "profile"), HOME: join(dir, "profile"), CXB_SERVER: "private" });
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
  await client.request("initialize", { clientInfo: { name: "丙路审查测试", version: "1" }, capabilities: { cxb: { protocolVersion: 1 } } });
  socket.write(JSON.stringify({ method: "initialized" }) + "\n");
  readTrace();
  return { client, exited };
}

// 传输层控制点只替换指定回包、注入通知；既有 mock 文件保持原样。
const transportObserver = `
const requests = new Map();
let input = "";
const held = [];
let holdReview = false;
let goalActive = false;
process.stdin.on("data", chunk => {
  input += String(chunk);
  for (let end; (end = input.indexOf("\\n")) >= 0;) {
    const line = input.slice(0,end); input = input.slice(end+1);
    if (line.trim()) { const message = JSON.parse(line); if (message.method && message.id !== undefined) requests.set(String(message.id),message); }
  }
});
const write = process.stdout.write.bind(process.stdout);
const send = message => write(JSON.stringify(message)+"\\n");
process.stdout.write = (chunk,...args) => {
  let message;
  try { message = JSON.parse(String(chunk).trim()); } catch { return write(chunk,...args); }
  const request = requests.get(String(message.id));
  if (!request || message.method) return write(chunk,...args);
  if (request.method === "n4/hold") { holdReview=true; return send({id:message.id,result:{}}); }
  if (request.method === "n4/goal") { goalActive=true; return send({id:message.id,result:{}}); }
  if (request.method === "thread/goal/get") return send({id:message.id,result:{goal:goalActive ? {threadId:request.params.threadId,status:"active",objective:"审查目标"} : null}});
  if (request.method === "thread/goal/clear" || request.method === "thread/goal/set") { goalActive=false; return send({id:message.id,result:{}}); }
  if (request.method === "n4/state") return send({id:message.id,result:{held:held.length}});
  if (request.method === "n4/release") { for (const item of held.splice(0)) send(item); return send({id:message.id,result:{}}); }
  if (request.method === "n4/emit") { send(request.params); return send({id:message.id,result:{}}); }
  if (request.method === "review/start" && holdReview) { held.push(message); return true; }
  return write(chunk,...args);
};
`;

async function connectClient(cxb: boolean): Promise<Rpc> {
  const socket = new net.Socket(); sockets.add(socket);
  await bounded(new Promise<void>((resolve,reject) => {
    socket.once("connect",resolve); socket.once("error",reject); socket.connect(testSocketPath(dir));
  }),5000,()=>socket.destroy());
  const client = new Rpc(socket); clients.push(client);
  await client.request("initialize", {clientInfo:{name:"审查观察者",version:"1"}, capabilities:cxb ? {cxb:{protocolVersion:1}} : {}});
  socket.write(JSON.stringify({method:"initialized"})+"\n");
  return client;
}
const parent = "thread-001";
const child = "review-thread-001";
const reviewTurn = "review-turn-001";
function movedNotices(client: Rpc): Frame[] { return client.notifications.filter(n=>n.method==="broker/run/moved"); }
async function complete(client: Rpc): Promise<void> {
  await client.request("n4/emit",{method:"turn/completed",params:{threadId:child,turn:{id:reviewTurn,status:"interrupted",items:[],error:null}}});
}

test("迁移仅通知父扩展所有者与观察者，子事件和事项仍独立，结束后可订阅子线程", async () => {
  const {client:owner} = await start();
  await owner.request("thread/start");
  const watcher = await connectClient(true);
  const legacy = await connectClient(false);
  await owner.request("broker/threads/watch",{threadId:parent});
  await watcher.request("broker/threads/watch",{threadId:parent});
  await legacy.request("broker/threads/watch",{threadId:parent});
  const review = await owner.request("review/start",{ delivery: "detached",threadId:parent,target:{type:"uncommittedChanges"}});
  const snapshot = await watcher.request("broker/threads/watch",{threadId:parent});
  const runId = snapshot.run.runId;
  await until(()=>movedNotices(owner).length>0 && movedNotices(watcher).length>0);
  const expected={threadId:parent,runId,reviewThreadId:child,reviewShortId:snapshot.run.reviewShortId};
  expect(movedNotices(owner).map(n=>n.params)).toEqual([expected]);
  expect(movedNotices(watcher).map(n=>n.params)).toEqual([expected]);
  expect(movedNotices(legacy)).toEqual([]);
  expect(review.reviewThreadId).toBe(child);
  const childOnly = await connectClient(true);
  const childSnapshot = await childOnly.request("broker/threads/watch",{threadId:child});
  expect(childSnapshot.run.runId).toBe(runId);
  expect(childSnapshot.thread.lastRunId).toBe(runId);
  await owner.request("broker/threads/detach",{threadId:child,runId,policy:"auto"});
  await owner.request("n4/emit",{id:700,method:"item/tool/requestUserInput",params:{threadId:child,turnId:reviewTurn,itemId:"问题",questions:[{id:"选择",header:"选择",question:"请选择",isOther:false,isSecret:false,options:[{label:"继续",description:"继续审查"}]}]}});
  let pending: any;
  await until(async()=> { pending=await childOnly.request("broker/threads/watch",{threadId:child}); return pending.pending.length===1; });
  expect(pending.pending[0].threadId).toBe(child);
  expect(pending.pending[0].runId).toBe(runId);
  expect((await watcher.request("broker/threads/watch",{threadId:parent})).pending).toEqual([]);
  await owner.request("n4/emit",{method:"item/started",params:{threadId:child,turnId:reviewTurn,item:{id:"子标记",type:"agentMessage",text:"审查进行中"}}});
  await until(()=>childOnly.notifications.some(n=>n.method==="item/started"));
  await watcher.request("broker/threads/watch",{threadId:parent});
  expect(watcher.notifications.filter(n=>n.method==="item/started")).toEqual([]);
  expect(watcher.notifications.filter(n=>n.method==="broker/inbox/added")).toEqual([]);
  expect(movedNotices(childOnly)).toEqual([]);
  await owner.request("broker/inbox/resolve",{id:pending.pending[0].id,answer:{answers:{选择:{answers:["继续"]}}}});
  await complete(owner);
  await until(async()=> (await childOnly.request("broker/threads/watch",{threadId:child})).run.state==="ended");
  const ended = await childOnly.request("broker/threads/watch",{threadId:child});
  expect(ended.run.runId).toBe(runId);
  expect(ended.thread.lastRunId).toBe(runId);
  expect(ended.pending).toEqual([]);
},30_000);

for (const managed of [false,true]) {
  test(`父线程停止${managed ? "托管" : "客户端"}审查只中断子线程并等待完成证据`,async()=> {
    const {client}=await start();
    await client.request("thread/start");
    await client.request("review/start",{ delivery: "detached",threadId:parent,target:{type:"uncommittedChanges"}});
    const runId=(await client.request("broker/threads/watch",{threadId:parent})).run.runId;
    if (managed) await client.request("broker/threads/detach",{threadId:child,runId,policy:"auto"});
    await client.request("n4/goal");
    let settled=false;
    const stopping=client.request("broker/threads/stop",{threadId:parent,clear:true}).then(r=>{settled=true;return r;});
    void stopping.catch(()=>{});
    await until(()=>{readTrace();return observed.some(r=>r.method==="turn/interrupt");});
    expect(settled).toBe(false);
    const operations=observed.filter(r=>r.method?.startsWith("thread/goal/") || r.method==="turn/interrupt");
    expect(operations.length).toBeGreaterThan(0);
    expect(operations.every(r=>r.params?.threadId===child)).toBe(true);
    await complete(client);
    const result=await stopping;
    expect(result.stopOutcome).toBe("confirmed");
    expect(result.alreadyStopped).toBe(false);
    expect(result.runId).toBe(runId);
    expect(result.goal).toBe("cleared");
  },30_000);
}

test("停止先于迁移回包时不操作父线程，迁移后合并同一运行",async()=> {
  const {client}=await start();
  await client.request("thread/start");
  await client.request("n4/hold");
  const reviewing=client.request("review/start",{ delivery: "detached",threadId:parent,target:{type:"uncommittedChanges"}});
  void reviewing.catch(()=>{});
  await until(async()=> (await client.request("n4/state")).held===1);
  const stopping=client.request("broker/threads/stop",{threadId:parent});
  void stopping.catch(()=>{});
  const snapshot=await client.request("broker/threads/watch",{threadId:parent});
  expect(snapshot.run.state).toBe("stopping");
  readTrace();
  expect(observed.filter(r=>r.method?.startsWith("thread/goal/") || r.method==="turn/interrupt")).toEqual([]);
  await client.request("n4/release");
  await reviewing;
  await until(()=>{readTrace();return observed.some(r=>r.method==="turn/interrupt");});
  expect(observed.filter(r=>r.method==="turn/interrupt").map(r=>r.params)).toEqual([{threadId:child,turnId:reviewTurn}]);
  await complete(client);
  expect((await stopping).stopOutcome).toBe("confirmed");
},30_000);
for (const detachFirst of [true, false]) test(`同批交接与停止：${detachFirst ? "交接先到" : "停止先到"}仍只停止活动 review 子线程`, async () => {
  const { client } = await start();
  await client.request("thread/start");
  await client.request("review/start", { delivery: "detached", threadId: parent, target: { type: "uncommittedChanges" } });
  const runId = (await client.request("broker/threads/watch", { threadId: parent })).run.runId;
  let transferred!: Promise<any>;
  let stopping!: Promise<any>;
  client.socket.cork();
  try {
    const detach = () => { transferred = client.request("broker/threads/detach", { threadId: parent, runId, policy: "auto" }); };
    const stop = () => { stopping = client.request("broker/threads/stop", { threadId: parent }); };
    if (detachFirst) { detach(); stop(); } else { stop(); detach(); }
  } finally { client.socket.uncork(); }
  void transferred.catch(() => {}); void stopping.catch(() => {});
  await until(() => { readTrace(); return observed.some((r) => r.method === "turn/interrupt"); });
  expect(observed.filter((r) => r.method === "turn/interrupt").map((r) => r.params)).toEqual([{ threadId: child, turnId: reviewTurn }]);
  expect((await client.request("broker/threads/watch", { threadId: parent })).run.state).toBe("stopping");
  await complete(client);
  expect(await stopping).toMatchObject({ runId, alreadyStopped: false, stopOutcome: "confirmed" });
  expect(["detached", "alreadyDetached", "runEnded"]).toContain((await transferred).outcome);
  const ended = await client.request("broker/threads/watch", { threadId: child });
  expect(ended.run).toMatchObject({ runId, state: "ended", reviewThreadId: child });
}, 60_000);

for (const firstTarget of [parent, child]) test(`迁移通知与父子停止交错：先停${firstTarget === parent ? "父" : "子"}线程，两次停止汇合到同一 review`, async () => {
  const { client } = await start();
  await client.request("thread/start");
  await client.request("n4/hold");
  const reviewing = client.request("review/start", { delivery: "detached", threadId: parent, target: { type: "uncommittedChanges" } });
  void reviewing.catch(() => {});
  await until(async () => (await client.request("n4/state")).held === 1);
  let stops: Promise<any>[] = [];
  let movedRun = "";
  client.onNotification = (frame) => {
    if (frame.method !== "broker/run/moved") return;
    movedRun = frame.params!.runId as string;
    stops = [firstTarget, firstTarget === parent ? child : parent].map((threadId) => {
      const work = client.request("broker/threads/stop", { threadId });
      void work.catch(() => {});
      return work;
    });
  };
  try {
    await client.request("n4/release");
    await reviewing;
    await until(() => { readTrace(); return stops.length === 2 && observed.some((r) => r.method === "turn/interrupt"); });
    // 夹具不自动完成；手工注入完成通知前，父线程关联的运行仍在停止中。
    expect((await client.request("broker/threads/watch", { threadId: parent })).run.state).toBe("stopping");
    await complete(client);
    const replies = await bounded(Promise.all(stops), 50_000);
    expect(movedRun).not.toBe("");
    for (const reply of replies) expect(reply).toMatchObject({
      runId: movedRun, stopOutcome: "confirmed", alreadyStopped: false, interruptSent: true,
    });
    await client.request("n4/state");
    readTrace();
    // 详细设计第 384 行允许显式 stop 重发中断；同步夹具排空此前出站消息后再计数。
    const interrupts = observed.filter((r) => r.method === "turn/interrupt");
    expect(interrupts.length).toBeGreaterThanOrEqual(1);
    expect(interrupts.length).toBeLessThanOrEqual(2);
    for (const request of interrupts) expect(request.params).toEqual({ threadId: child, turnId: reviewTurn });
    expect((await client.request("broker/threads/watch", { threadId: child })).run.state).toBe("ended");
  } finally { client.onNotification = undefined; }
}, 60_000);
