/** 真实 broker 与真实 CLI 的独占等待；SA1b 轮次只由测试显式完成。 */
import { afterAll, describe, expect, test } from "bun:test";
import { appendFileSync, existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { bounded } from "./cxb/wait-run";

const RUNNABLE = process.platform === "win32";
const sa1b = RUNNABLE ? await import("./test-support/sa1b-harness") : null;
const brokerHarness = RUNNABLE ? await import("./test-support/broker-harness") : null;
type Harness = InstanceType<NonNullable<typeof sa1b>["Sa1bHarness"]>;
const harnesses: Harness[] = [];
const tasks: Promise<unknown>[] = [];
const notice = "[cxb] 本次等待已被同一线程的新等待接替，已退出。\n";

// 被动旁读真实管道：记录请求与成功回包的对应关系，不合成回包、不改变 CLI 输出。
// 时间戳写在首次独占 watch 回包进入 CLI 的 data 处理器之前，作为登记完成同步点。
const observer = String.raw`
import net from "node:net";
import { appendFileSync } from "node:fs";
const states = new WeakMap();
const write = net.Socket.prototype.write, emit = net.Socket.prototype.emit;
function state(socket) {
  if (!states.has(socket)) states.set(socket, { input: "", output: "", watches: new Map() });
  return states.get(socket);
}
function record(value) { appendFileSync(process.env.CXB_WAIT_TEST_EVENTS, JSON.stringify({ ...value, at: Date.now() }) + "\n"); }
function feed(socket, direction, data) {
  const s = state(socket); s[direction] += data.toString();
  let end;
  while ((end = s[direction].indexOf("\n")) >= 0) {
    const line = s[direction].slice(0, end); s[direction] = s[direction].slice(end + 1);
    let message; try { message = JSON.parse(line); } catch { continue; }
    if (direction === "output" && message.method === "broker/threads/watch") s.watches.set(message.id, message.params);
    if (direction === "input" && message.result && s.watches.has(message.id)) {
      record({ event: "registered", ...s.watches.get(message.id) }); s.watches.delete(message.id);
    }
    if (direction === "input" && message.method === "broker/wait/superseded") record({ event: "superseded", ...message.params });
  }
}
net.Socket.prototype.write = function(chunk, ...args) { feed(this, "output", chunk); return write.call(this, chunk, ...args); };
net.Socket.prototype.emit = function(event, ...args) { if (event === "data") feed(this, "input", args[0]); return emit.call(this, event, ...args); };
`;

function fixture() {
  const s = new sa1b!.Sa1bHarness(); harnesses.push(s); return s;
}

function cli(s: Harness, args: string[]) {
  const index = s.spawnedProcesses.length;
  const preload = join(s.tempDir, `旁读-${index}.mjs`), events = join(s.tempDir, `管道-${index}.jsonl`);
  writeFileSync(preload, observer);
  const proc = Bun.spawn([process.execPath, "--preload", preload, join(import.meta.dir, "cxb/main.ts"), ...args, "--dir", s.tempDir], {
    cwd: s.tempDir, stdin: "ignore", stdout: "pipe", stderr: "pipe", windowsHide: true,
    env: { ...brokerHarness!.envWithPathPrefix(s.tempDir), CXB_HOME: s.home, CODEX_HOME: join(s.tempDir, "cli-codex-home"),
      USERPROFILE: s.tempDir, HOME: s.tempDir, CXB_WAIT_TEST_EVENTS: events },
  });
  s.spawnedProcesses.push(proc);
  const exited = proc.exited.then(code => ({ code, at: Date.now() }));
  const result = Promise.all([new Response(proc.stdout).text(), new Response(proc.stderr).text(), exited])
    .then(([stdout, stderr, exit]) => ({ stdout, stderr, ...exit }));
  // 失败路径也登记收束；不给裸 Promise 留未处理拒绝。
  tasks.push(result.catch(() => undefined));
  const records = (): any[] => existsSync(events) ? readFileSync(events, "utf8").split("\n").slice(0, -1).filter(Boolean).map(line => JSON.parse(line)) : [];
  return { proc, result, records, async registered(exclusive = true) {
    const matching = () => records().find(record => record.event === "registered" && (exclusive ? record.exclusiveWaiter === true : !record.exclusiveWaiter));
    await s.until(async () => !!matching(), 10_000, `CLI ${args[0]} 首次 watch 未成功登记`);
    return matching() as { threadId: string; at: number };
  } };
}

afterAll(async () => {
  for (const s of harnesses) {
    await bounded(s.close(), 12_000);
    for (const proc of s.spawnedProcesses) {
      await bounded(proc.exited, 1000);
      expect(proc.exitCode !== null || proc.signalCode !== null).toBe(true);
    }
    if (process.env.CXB_WAIT_PROCESS_LOG) appendFileSync(process.env.CXB_WAIT_PROCESS_LOG,
      JSON.stringify({ tempDir: s.tempDir, pids: s.spawnedProcesses.map(proc => proc.pid), allExited: true }) + "\n");
  }
  await bounded(Promise.all(tasks), 5000);
}, 60_000);

describe.skipIf(!RUNNABLE)("broker-ext 独占线程等待", () => {
  for (const json of [false, true]) test(`后等待登记后一秒内旧等待退出，新等待正常完成：${json ? "JSON" : "文本"}`, async () => {
    const s = fixture();
    try {
      await s.open(); await s.start();
      const view = await s.watch();
      const a = cli(s, ["wait", sa1b!.SA1B_ROOT, ...(json ? ["--json"] : [])]);
      await a.registered();
      expect(a.proc.exitCode).toBeNull();
      // 新等待使用短 ID，登记与通知仍应采用同一个完整 threadId。
      const b = cli(s, ["wait", view.thread.shortId, "--json"]);
      const registered = await b.registered();
      const old = await bounded(a.result, Math.max(1, 1000 - (Date.now() - registered.at)));
      expect(old.at - registered.at).toBeLessThanOrEqual(1000);
      expect(old.code).toBe(10);
      expect(old.stderr).toEqual(notice);
      expect(old.stdout).toEqual(json ? JSON.stringify({ ok: false, command: "wait", waitFor: "thread", threadId: sa1b!.SA1B_ROOT,
        shortId: view.thread.shortId, event: "superseded", superseded: true }) + "\n" : "");
      expect(a.records().filter(record => record.event === "superseded")).toHaveLength(1);
      expect(b.records().filter(record => record.event === "superseded")).toEqual([]);
      expect(b.proc.exitCode).toBeNull();
      await s.finish(sa1b!.SA1B_ROOT, "独占等待正常结果");
      const next = await bounded(b.result, 10_000);
      expect(next.code).toBe(0);
      expect(JSON.parse(next.stdout)).toMatchObject({ ok: true, command: "wait", result: "独占等待正常结果" });
      expect(next.stderr).toEqual("");
      console.info(`接替时延（${json ? "JSON" : "文本"}）：${old.at - registered.at} 毫秒`);
    } finally { await bounded(s.close(), 12_000); }
  }, 45_000);

  // 已结束的 A 收不到任何消息，「不向断开连接发通知」无法从进程外观察；断开即清字段由 Rust
  // wait_superseded 用例 3 直接断言。这里只证明可观察部分：B 不被误通知、独占登记确实转到 B（C 能接替 B）。
  test("强行结束旧等待后新等待接手独占登记并正常完成", async () => {
    const s = fixture();
    try {
      await s.open(); await s.start();
      const a = cli(s, ["wait", sa1b!.SA1B_ROOT, "--json"]);
      await a.registered(); a.proc.kill(); await bounded(a.result, 5000);
      const b = cli(s, ["wait", sa1b!.SA1B_ROOT, "--json"]);
      await b.registered();
      expect(b.records().filter(record => record.event === "superseded")).toEqual([]);
      expect(b.proc.exitCode).toBeNull();
      const c = cli(s, ["wait", sa1b!.SA1B_ROOT, "--json"]);
      const registered = await c.registered();
      const old = await bounded(b.result, Math.max(1, 1000 - (Date.now() - registered.at)));
      expect(old.at - registered.at).toBeLessThanOrEqual(1000);
      expect(old.code).toBe(10);
      expect(JSON.parse(old.stdout)).toMatchObject({ ok: false, event: "superseded", superseded: true });
      expect(b.records().filter(record => record.event === "superseded")).toHaveLength(1);
      await s.finish(sa1b!.SA1B_ROOT, "强制结束后正常结果");
      const next = await bounded(c.result, 10_000);
      expect(next.code).toBe(0);
      expect(JSON.parse(next.stdout)).toMatchObject({ ok: true, result: "强制结束后正常结果" });
      expect(next.stderr).toEqual("");
      expect(c.records().filter(record => record.event === "superseded")).toEqual([]);
    } finally { await bounded(s.close(), 12_000); }
  }, 45_000);

  test("前台 run 的拥有者及内部 watch 不被线程等待接替", async () => {
    const s = fixture();
    try {
      await s.open();
      const owner = cli(s, ["run", "请完成前台测试任务", "--json"]);
      const registered = await owner.registered(false);
      const waiter = cli(s, ["wait", registered.threadId, "--json"]);
      await waiter.registered();
      expect(owner.records().every(record => !record.exclusiveWaiter && record.event !== "superseded")).toBe(true);
      expect(owner.proc.exitCode).toBeNull();
      await s.finish(registered.threadId, "前台和等待均正常完成");
      const [run, wait] = await bounded(Promise.all([owner.result, waiter.result]), 10_000);
      for (const result of [run, wait]) {
        expect(result.code).toBe(0);
        expect(JSON.parse(result.stdout)).toMatchObject({ ok: true, result: "前台和等待均正常完成" });
        expect(result.stderr).not.toContain("接替");
      }
      expect(owner.records().some(record => record.event === "superseded")).toBe(false);
    } finally { await bounded(s.close(), 12_000); }
  }, 45_000);
});
