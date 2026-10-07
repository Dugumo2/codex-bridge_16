/** N7 丙路：经真实 broker 验证生命周期屏障、持久化和新日志格式。 */
import { afterEach, beforeEach, expect, test } from "bun:test";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import type { Subprocess } from "bun";
import { endpointFor, exitsWithin, spawnBroker, TestClient, testSocketPath, waitForSocket, writeMockCodexScript } from "./test-support/broker-harness";

let dir: string;
let home: string;
const processes: Subprocess[] = [];
const clients: TestClient[] = [];
const readers: Promise<unknown>[] = [];
beforeEach(() => { dir = mkdtempSync(join(tmpdir(), "n7c-")); home = join(dir, "home"); mkdirSync(home); });
afterEach(async () => {
  const errors: unknown[] = [];
  for (const client of clients.splice(0)) { try { await client.close(); } catch (e) { errors.push(e); } }
  for (const proc of processes.splice(0)) {
    try { if (proc.exitCode === null) proc.kill(); if (!await exitsWithin(proc, 5000)) throw new Error("测试进程未按期退出"); } catch (e) { errors.push(e); }
  }
  for (const reader of readers.splice(0)) { try { const text=await reader; if (typeof text==="string" && /panicked/.test(text)) errors.push(new Error("被测进程发生异常："+text)); } catch (e) { errors.push(e); } }
  try { rmSync(dir, { recursive: true, force: true }); expect(existsSync(dir)).toBe(false); } catch (e) { errors.push(e); }
  if (errors.length) throw new AggregateError(errors, "测试资源清理失败");
}, 25000);

const mock = `
let buffer = ""; const decoder = new TextDecoder(); let count=0; const held=[];
const send=(m)=>process.stdout.write(JSON.stringify(m)+"\\n");
for await (const chunk of Bun.stdin.stream()) {
 buffer+=decoder.decode(chunk); let at;
 while((at=buffer.indexOf("\\n"))>=0){const line=buffer.slice(0,at);buffer=buffer.slice(at+1);if(!line.trim())continue;const m=JSON.parse(line);if(m.id===undefined)continue;
 const p=m.params||{};
 if(m.method==="initialize")send({id:m.id,result:{userAgent:"模拟服务器"}});
 else if(m.method==="thread/start"||m.method==="thread/resume")send({id:m.id,result:{thread:{id:p.threadId||"P",preview:"测试线程"}}});
 else if(m.method==="thread/archive"||m.method==="thread/delete"){
  count++; if(p.threadId==="hold")held.push(m); else if(p.threadId==="missing")send({id:m.id,error:{code:-32600,message:"thread not found: missing"}});
  else if(p.threadId==="archived")send({id:m.id,error:{code:-32600,message:"thread archived is archived"}});
  else send({id:m.id,result:{}});
 }else if(m.method==="测试/计数")send({id:m.id,result:{count,held:held.length}});
 else if(m.method==="测试/放行"){for(const old of held.splice(0))send({id:old.id,result:{}});send({id:m.id,result:{}});}
 else send({id:m.id,result:{}});
 }
}
`;

async function start(): Promise<TestClient> {
  writeMockCodexScript(dir, mock);
  const sock = testSocketPath(dir);
  const env: Record<string, string> = { CXB_HOME: home, HOME: home, USERPROFILE: home, CODEX_HOME: join(home, "codex"), CXB_SERVER: "private" };
  const proc = spawnBroker({ tempDir: dir, spawnedProcesses: processes }, endpointFor(sock), dir, { cwd: dir, env });
  for (const stream of [proc.stdout, proc.stderr]) {
    if (stream && typeof stream !== "number") {
      let timer: ReturnType<typeof setTimeout>;
      const reading = Promise.race([
        new Response(stream).text(), new Promise((_, reject) => { timer = setTimeout(() => reject(new Error("测试输出读取超时")), 20000); }),
      ]).finally(() => clearTimeout(timer));
      reading.catch(() => {}); readers.push(reading);
    }
  }
  await waitForSocket(sock);
  const client = await TestClient.connect(sock); clients.push(client);
  const result = await client.request("initialize", { capabilities: { cxb: { protocolVersion: 1 } }, clientInfo: { name: "测试" } }) as { userAgent: string };
  expect(result.userAgent).toBe("cxb-broker"); client.send({ method: "initialized" }); return client;
}
function workspace(): string { return join(home, "workspaces", readdirSync(join(home, "workspaces"))[0]!); }
function payload(name: string): any { return JSON.parse(readFileSync(join(workspace(), name), "utf8")).payload; }
async function rejects(client:TestClient,method:string,params:unknown,message:string):Promise<void> {
  const result=await client.request(method,params).then(value=>({value,error:null}),error=>({value:null,error}));
  expect(result.error).toBeInstanceOf(Error);expect(String(result.error.message)).toContain(message);
}

test("归档成功回包之前索引删除已落盘，原始方法形状保留", async () => {
  const client = await start(); await client.request("thread/start", { threadId: "P" });
  const reply = await client.request("broker/threads/archive", { threadId: "P" }) as any;
  expect(reply.archived).toBe(true); expect(reply.localIndex).toBe("removed"); expect(reply.operationId).toBeString();
  expect(Object.values(payload("threads.json").threads)).toHaveLength(0); expect(payload("threads.json").removed.P).toBeNumber();
  expect(await client.request("thread/delete", { threadId: "Q" })).toEqual({});
}, 15000);

async function shutdown(client: TestClient): Promise<void> {
  await client.request("broker/shutdown"); await client.close();
  const proc = processes[processes.length - 1]!;
  if (!await exitsWithin(proc, 5000)) throw new Error("broker 关闭超过期限");
}

test("真实重启后损坏生命周期拒绝修改并在健康检查中降级", async () => {
  const first = await start(); const path = join(workspace(), "lifecycle.json");
  await shutdown(first); writeFileSync(path, "{", "utf8");
  const client = await start();
  const health = await client.request("broker/health") as any;
  expect(health.storage).toBe("degraded"); expect(health.storageReason).toContain("生命周期文件无法读取");
  await rejects(client, "broker/threads/archive", { threadId: "P" }, "生命周期存储操作失败");
  await rejects(client, "turn/start", { threadId: "P" }, "生命周期存储操作失败");
  expect(await client.request("thread/read", { threadId: "P" })).toEqual({});
  expect((await client.request("broker/threads/list") as any).threads).toEqual([]);
}, 15000);

test("真实重启后仅有本地删除意图仍可列表观察并重试完成", async () => {
  const first = await start(); const path = join(workspace(), "lifecycle.json"); await shutdown(first);
  writeFileSync(path, JSON.stringify({ epoch: 1, revision: 1, payload: { records: { P: {
    operationId: "恢复删除", kind: "delete", threadId: "P", shortId: "1234abcd", preview: "恢复线程",
    phase: "localPurging", attempts: [], createdAt: 1, updatedAt: 2,
  } } } }), "utf8");
  const client = await start(); const list = await client.request("broker/threads/list") as any;
  const thread = list.threads.find((t: any) => t.threadId === "P");
  expect(thread.shortId).toBe("1234abcd"); expect(thread.preview).toBe("恢复线程"); expect(thread.state).toBe("idle");
  expect(thread.lifecycle.phase).toBe("localPurging"); expect(Object.values(payload("threads.json").threads)).toHaveLength(0);
  const watch = await client.request("broker/threads/watch", { threadId: "P" }) as any; expect(watch.run).toBeNull();
  const reply = await client.request("broker/threads/purge", { threadId: "P", serverDelete: true }) as any;
  expect(reply.operationId).toBe("恢复删除"); expect(reply.deleted).toContain("server-thread"); expect(reply.failed).toEqual([]);
}, 15000);

test("确切不存在才作为已达目标，已归档不能当成已删除", async () => {
  const client = await start();
  const reply = await client.request("broker/threads/archive", { threadId: "missing" }) as any;
  expect(reply.archived).toBe("alreadyDone"); expect(reply.localIndex).toBe("alreadyAbsent");
  await rejects(client,"thread/delete",{threadId:"archived"},"thread archived is archived");
}, 15000);

test("在途归档阻止写入口、查询显示生命周期且断言未转发冲突请求", async () => {
  const client = await start(); const pending = client.request("broker/threads/archive", { threadId: "hold" }); pending.catch(()=>{});
  let seen = false;
  for (let i = 0; i < 100; i++) {
    const count = await client.request("测试/计数") as any; if (count.held === 1) { seen = true; break; }
    await new Promise(r => setTimeout(r, 10));
  }
  expect(seen).toBe(true);
  const watch = await client.request("broker/threads/watch", { threadId: "hold" }) as any;
  expect(watch.lifecycle.kind).toBe("archive"); expect(watch.thread.state).toBe("idle"); expect(watch.run).toBeNull();
  for (const method of ["turn/start", "turn/steer", "review/start", "thread/resume", "thread/goal/set", "thread/unarchive", "broker/threads/stop"]) {
    await rejects(client,method,{threadId:"hold"},"线程生命周期操作尚未完成");
  }
  expect((await client.request("测试/计数") as any).count).toBe(1);
  await client.request("测试/放行"); expect((await pending as any).archived).toBe(true);
}, 15000);

test("purge 一次完成本地删除和服务器删除并保留操作编号", async () => {
  const client = await start(); await client.request("thread/start", { threadId: "P" });
  const result = await client.request("broker/threads/purge", { threadId: "P", serverDelete: true }) as any;
  expect(result.operationId).toBeString(); expect(result.deleted).toContain("server-thread"); expect(result.failed).toEqual([]);
  expect((await client.request("测试/计数") as any).count).toBe(1);
}, 15000);
