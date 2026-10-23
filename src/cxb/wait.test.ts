import { afterEach, beforeEach, describe, expect, spyOn, test } from "bun:test";
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { COMMANDS, parseArgv } from "./args";
import { wait } from "./wait";
import { bounded, waitRun } from "./wait-run";
import { cleanupContexts, context, errorCode, failure, gate, item, thread } from "./test-fixtures/n4b-support";
import { runResult } from "./test-fixtures/run-fakes";
import { removeTemp } from "./test-fixtures/main-test-support";
import { upstreamError } from "./test-fixtures/upstream-errors";
import { Writable } from "node:stream";
import { createOutput } from "./output";

/** 使用真实输出层检查完整字节，覆盖补换行与 JSON 序列化。 */
function wireOutput(c: ReturnType<typeof context>) {
  let stdout = "", stderr = "";
  c.ctx.out = createOutput({ command: "wait", json: c.ctx.parsed.globals.json, verbose: false,
    stdout: new Writable({ write(chunk, _encoding, done) { stdout += chunk.toString(); done(); } }),
    stderr: new Writable({ write(chunk, _encoding, done) { stderr += chunk.toString(); done(); } }),
  });
  return { get stdout() { return stdout; }, get stderr() { return stderr; } };
}

describe("单等待逐字基线 6619d6d", () => {
  for (const scenario of ["正常完成", "待答事项", "目标受阻"] as const) {
    for (const json of [false, true]) test(`${scenario}：${json ? "JSON" : "文本"}及标准错误与退出码`, async () => {
      const c = context(["wait", "abcd1234", ...(json ? ["--json"] : [])]);
      const output = wireOutput(c);
      const status = scenario === "目标受阻" ? "goalBlocked" : "completed";
      c.fake.handle = (method) => method === "broker/threads/list" ? { threads: [thread()] }
        : { thread: thread(), run: runResult({ status }), pending: scenario === "待答事项" ? [item()] : [] };
      expect(await bounded(wait(c.ctx), 2000)).toBe(scenario === "待答事项" ? 5 : scenario === "目标受阻" ? 9 : 0);
      await c.ctx.out.flush();
      const expected = scenario === "待答事项"
        ? { ok: false, status: "waiting", threadId: "thread-test", pending: [{ ...item(), params: {
          questions: [{ id: "q1", question: "请选择文件", isSecret: "***", options: [{ label: "甲", description: "甲文件" }] }],
        } }] }
        : { ok: scenario === "正常完成", command: "wait", waitFor: "thread", threadId: "thread-test", shortId: "abcd1234",
          runId: "run-test", status, result: "中文结果", alreadyDone: true };
      expect(output.stdout).toEqual(json ? JSON.stringify(expected) + "\n" : scenario === "待答事项"
        ? 'qu_test1234 提问：请选择文件（选项：甲）；回答：cxb reply qu_test1234 "你的回答"\ncxb wait abcd1234' : "中文结果");
      if (json) expect(JSON.parse(output.stdout)).toEqual(expected);
      expect(output.stderr).toEqual("");
      c.checkClean();
    });
  }
});

const supersededNotice = "[cxb] 本次等待已被同一线程的新等待接替，已退出。\n";
const supersededJson = { ok: false, command: "wait", waitFor: "thread", threadId: "thread-test", shortId: "abcd1234",
  event: "superseded", superseded: true };
const replaceWait = (c: ReturnType<typeof context>, threadId = "thread-test") =>
  c.fake.emit("broker/wait/superseded", { threadId });

describe("独占线程等待", () => {
  test("仅首次线程快照带标记，运行补读与 review 子线程观察不带标记", async () => {
    const c = context(["wait", "abcd1234"]);
    c.fake.handle = (method, params) => method === "broker/threads/list" ? { threads: [thread()] }
      : { thread: thread(), run: runResult({ reviewThreadId: "review-child" }), pending: [] };
    expect(await bounded(wait(c.ctx), 2000)).toBe(0);
    expect(c.fake.calls.filter(call => call.method === "broker/threads/watch").map(call => call.params)).toEqual([
      { threadId: "thread-test", exclusiveWaiter: true }, { threadId: "review-child" }, { threadId: "thread-test", runId: "run-test" },
    ]);
    c.checkClean();
  });

  test("waitRun 初始 watch 与未确认诊断补读均不带独占标记", async () => {
    const c = context(["wait", "abcd1234"]);
    c.fake.handle = () => new Promise(() => {});
    const controller = new AbortController();
    const work = waitRun(c.fake.rpc, { threadId: "thread-test", runId: "run-test", timeoutMs: 2000, signal: controller.signal });
    try {
      c.fake.emit("broker/run/updated", { runId: "run-test", diagnosticRevision: 3, state: "stopping", status: null,
        interruptReason: "stallTimeout", stopOutcome: "unconfirmed" });
      expect(c.fake.calls.map(call => call.params)).toEqual([
        { threadId: "thread-test", runId: "run-test" }, { threadId: "thread-test", runId: "run-test" },
      ]);
    } finally { controller.abort(); await work.catch(() => undefined); }
    c.checkClean();
  });

  for (const id of [undefined, "ap_test1234", "qu_test1234", "mc_test1234", "gd_test1234"]) test(`事项或工作区等待不登记独占：${id ?? "工作区"}`, async () => {
    const c = context(["wait", ...(id ? [id] : [])]);
    c.fake.handle = () => ({ event: id ? "resolved" : "idle", item: item() });
    expect(await bounded(wait(c.ctx), 2000)).toBe(id ? 0 : 10);
    expect(c.fake.calls.map(call => call.method)).toEqual(["broker/inbox/wait"]);
    c.checkClean();
  });

  for (const scenario of ["待答事项", "没有运行", "快照失败"] as const) {
    for (const json of [false, true]) test(`首次快照同批接替优先于${scenario}：${json ? "JSON" : "文本"}`, async () => {
      const c = context(["wait", "abcd1234", ...(json ? ["--json"] : [])]);
      const output = wireOutput(c);
      const watching = gate<void>();
      let resolve!: (value: unknown) => void, reject!: (error: Error) => void;
      const reply = new Promise((yes, no) => { resolve = yes; reject = no; });
      c.fake.handle = () => ({ threads: [thread()] });
      const request = c.fake.rpc.request;
      // 直接返回可控传输 Promise，避免 FakeRpc 的 async 适配额外插入一次微任务，遮住真实竞态。
      c.fake.rpc.request = ((method, params, options) => {
        if (method !== "broker/threads/watch") return request(method, params, options);
        c.fake.calls.push({ method, params, options });
        expect(c.fake.handlers.get("broker/wait/superseded")?.size).toBe(1);
        watching.resolve();
        return reply;
      }) as typeof request;
      const work = wait(c.ctx);
      await watching.wait();
      // 先完成回包，再在同批同步派发通知；await 续体必须检查标志。
      if (scenario === "快照失败") reject(new Error("首次快照失败"));
      else resolve({ thread: thread({ lastRunId: null }), run: scenario === "没有运行" ? null : runResult(), pending: [item()] });
      replaceWait(c);
      expect(await bounded(work, 2000)).toBe(10);
      await c.ctx.out.flush();
      expect(output.stdout).toEqual(json ? JSON.stringify(supersededJson) + "\n" : "");
      expect(output.stderr).toEqual(supersededNotice);
      c.checkClean();
    });
  }

  for (const competitor of ["待答事项", "正常结果", "失败结果", "请求失败", "子线程订阅失败", "无竞争"] as const) {
    for (const first of [true, false]) test(`等待中接替与${competitor}竞争，接替${first ? "先" : "后"}派发，资源收束`, async () => {
      const c = context(["wait", "abcd1234", "--json"]);
      const output = wireOutput(c);
      const watching = gate<void>();
      let rejectWatch!: (error: Error) => void;
      c.fake.handle = (method, params) => {
        if (method === "broker/threads/list") return { threads: [thread()] };
        if (params.threadId === "review-child") return new Promise((_, reject) => { rejectWatch = reject; });
        if (!params.exclusiveWaiter) {
          watching.resolve();
          if (competitor === "请求失败") return new Promise((_, reject) => { rejectWatch = reject; });
        }
        return { thread: thread(), run: runResult({ state: "running", status: null,
          reviewThreadId: competitor === "子线程订阅失败" ? "review-child" : null }), pending: [] };
      };
      // 记录命令实际申请的计时器；必须取消或已触发，不以等待一段时间推测清理。
      const active = new Set<ReturnType<typeof setTimeout>>();
      const originalSet = globalThis.setTimeout, originalClear = globalThis.clearTimeout;
      const set = spyOn(globalThis, "setTimeout").mockImplementation(((callback: (...args: any[]) => void, ms?: number, ...args: any[]) => {
        const timer = originalSet(() => { active.delete(timer); callback(...args); }, ms);
        active.add(timer); return timer;
      }) as typeof setTimeout);
      const clear = spyOn(globalThis, "clearTimeout").mockImplementation(((timer: ReturnType<typeof setTimeout>) => {
        active.delete(timer); originalClear(timer);
      }) as typeof clearTimeout);
      const work = wait(c.ctx);
      try {
        await watching.wait();
        if (first) replaceWait(c);
        if (competitor === "待答事项") c.fake.emit("broker/inbox/added", { item: item() });
        if (competitor === "正常结果" || competitor === "失败结果") c.fake.emit("broker/run/ended", {
          run: runResult({ status: competitor === "正常结果" ? "completed" : "failed" }),
        });
        if (competitor === "请求失败" || competitor === "子线程订阅失败") rejectWatch(new Error("补读失败"));
        if (!first) replaceWait(c);
        replaceWait(c);
        expect(await bounded(work, 2000)).toBe(10);
        await c.ctx.out.flush();
        expect(output.stdout).toEqual(JSON.stringify(supersededJson) + "\n");
        expect(output.stderr).toEqual(supersededNotice);
        c.checkClean();
        expect(active.size).toBe(0);
      } finally {
        await c.cancel(); await work.catch(() => undefined);
        set.mockRestore(); clear.mockRestore();
        for (const timer of active) originalClear(timer);
      }
    });
  }

  test("其他线程通知被忽略；开始输出及输出后的接替不改变已选定结果", async () => {
    const c = context(["wait", "abcd1234", "--json"]);
    c.fake.handle = (method) => {
      if (method === "broker/threads/list") return { threads: [thread()] };
      replaceWait(c, "其他线程");
      return { thread: thread(), run: runResult(), pending: [] };
    };
    const original = c.ctx.out.resultJson;
    c.ctx.out.resultJson = (value) => { replaceWait(c); original(value); replaceWait(c); };
    expect(await bounded(wait(c.ctx), 2000)).toBe(0);
    replaceWait(c);
    expect(c.json).toEqual([{ ok: true, command: "wait", waitFor: "thread", threadId: "thread-test", shortId: "abcd1234",
      runId: "run-test", status: "completed", result: "中文结果", alreadyDone: true }]);
    expect(c.notices).toEqual([]);
    c.checkClean();
  });

  test("接替保留已输出上游诊断并只追加一行提示", async () => {
    const c = context(["wait", "abcd1234"]);
    const output = wireOutput(c), watching = gate<void>();
    c.fake.handle = (method, params) => {
      if (method === "broker/threads/list") return { threads: [thread()] };
      if (!params.exclusiveWaiter) watching.resolve();
      return { thread: thread(), run: runResult({ state: "running", status: null }), pending: [] };
    };
    const work = wait(c.ctx);
    await watching.wait();
    c.fake.emit("broker/run/updated", { runId: "run-test", diagnosticRevision: 1, upstreamError: upstreamError() });
    await c.ctx.out.flush();
    const prior = output.stderr;
    expect(prior.length).toBeGreaterThan(0);
    replaceWait(c);
    expect(await bounded(work, 2000)).toBe(10);
    await c.ctx.out.flush();
    expect(output.stdout).toEqual("");
    expect(output.stderr).toEqual(prior + supersededNotice);
    c.checkClean();
  });
});

const dirs: string[] = [];
beforeEach(() => { dirs.push(mkdtempSync(join(tmpdir(), "cxb-n4b-disk-"))); });
afterEach(async () => {
  const errors: unknown[] = [];
  try { await cleanupContexts(); } catch (e) { errors.push(e); }
  for (const dir of dirs.splice(0)) { try { await removeTemp(dir); } catch (e) { errors.push(e); } }
  if (errors.length) throw new AggregateError(errors, "乙组磁盘夹具清理失败");
}, 15000);

describe("乙组 wait 事项", () => {
  test("解析层拒绝零超时", () => {
    expect(() => parseArgv(["wait", "--timeout", "0"])).toThrow();
    const parsed = parseArgv(["wait"]);
    parsed.globals.timeout = 0; parsed.explicit = new Set(["timeout"]);
    expect(() => COMMANDS.wait.validate(parsed)).toThrow();
  });
  test("连接期间也受 wait 总期限约束", async () => {
    const c = context(["wait"]);
    c.ctx.parsed.explicit = new Set(["timeout"]); c.ctx.parsed.globals.timeout = 0.02;
    c.ctx.connect = () => new Promise(() => {});
    await errorCode(wait(c.ctx), 3);
    c.checkClean();
  });
  test("无 ID 的 added 脱敏输出与默认长等待期限", async () => {
    const c = context(["wait", "--json"]);
    c.fake.handle = (m) => m === "broker/threads/list" ? { threads: [thread()] } : { event: "added", item: item({ params: { token: "秘密", questions: [{ id: "q", question: "Bearer 凭证" }] } }) };
    expect(await wait(c.ctx)).toBe(0);
    expect(c.json[0]).toMatchObject({ waitFor: "inbox", event: { type: "added", item: { shortId: "abcd1234" } } });
    expect(JSON.stringify(c.json)).not.toContain("凭证");
    const call = c.fake.calls[0]!;
    expect(call.params.timeoutMs).toBeGreaterThan(3500000);
    expect(call.options.timeoutMs).toBe(call.params.timeoutMs + 5000);
    c.checkClean();
  });
  test("文本 added 给事项和回答方法；idle 退出 10；timeout 退出 3", async () => {
    for (const event of ["added", "idle", "timeout"]) {
      const c = context(["wait"]);
      c.fake.handle = (m) => m === "broker/threads/list" ? { threads: [] } : { event, item: item() };
      if (event === "timeout") await errorCode(wait(c.ctx), 3);
      else { expect(await wait(c.ctx)).toBe(event === "idle" ? 10 : 0); if (event === "added") expect(c.text.join("")).toContain("回答：cxb reply"); }
      c.checkClean();
    }
  });
  test("无 ID 被运行结束、已答、过期和撤销唤醒后按原期限重等", async () => {
    const c = context(["wait", "--timeout", "60"]);
    const events = ["runEnded", "resolved", "expired", "cancelled", "idle"];
    c.fake.handle = () => ({ event: events.shift() });
    expect(await wait(c.ctx)).toBe(10);
    expect(c.fake.calls).toHaveLength(5);
    const timeouts = c.fake.calls.map((call) => call.params.timeoutMs);
    expect(timeouts.every((v) => v > 50000 && v <= 60000)).toBe(true);
    expect(timeouts.at(-1)).toBeLessThanOrEqual(timeouts[0]!);
    c.checkClean();
  });
  test("超过 24 小时分段请求，分段超时继续等待", async () => {
    const c = context(["wait"]);
    c.ctx.parsed.explicit = new Set(["timeout"]); c.ctx.parsed.globals.timeout = 172800;
    let count = 0;
    c.fake.handle = () => ({ event: ++count === 1 ? "timeout" : "idle" });
    expect(await wait(c.ctx)).toBe(10);
    expect(c.fake.calls).toHaveLength(2);
    expect(c.fake.calls[0]).toMatchObject({ params: { timeoutMs: 86400000 }, options: { timeoutMs: 86405000 } });
    c.checkClean();
  });
  test("事项 resolved 返回类型，expired、cancelled、notFound 为 2", async () => {
    for (const event of ["resolved", "expired", "cancelled", "notFound"]) {
      const c = context(["wait", "qu_test1234", "--json"]);
      c.fake.handle = () => { if (event === "notFound") throw failure(event); return { event, item: item() }; };
      if (event === "resolved") { expect(await wait(c.ctx)).toBe(0); expect(c.json[0].resolvedKind).toBe("question"); }
      else await errorCode(wait(c.ctx), 2);
      expect(c.fake.calls[0]!.params.id).toBe("qu_test1234");
      c.checkClean();
    }
  });
});

describe("乙组 wait 线程", () => {
  test("快照已有托管提问立即交接；Guardian 不阻止取终态", async () => {
    for (const guardian of [false, true]) {
      const c = context(["wait", "abcd1234", "--json"]);
      c.fake.handle = (m) => m === "broker/threads/list" ? { threads: [thread()] } : {
        thread: thread(), run: runResult(), pending: [item(guardian ? { kind: "guardian" } : {})],
      };
      expect(await wait(c.ctx)).toBe(guardian ? 0 : 5);
      expect(c.json[0]).toMatchObject(guardian ? { alreadyDone: true, result: "中文结果" } : { status: "waiting" });
      c.checkClean();
    }
  });
  test("快照回包前到的事项不能丢，必须先登记监听", async () => {
    const c = context(["wait", "abcd1234"]);
    let watches = 0;
    c.fake.handle = (m) => {
      if (m === "broker/threads/list") return { threads: [thread()] };
      expect(c.fake.handlers.get("broker/inbox/added")?.size).toBe(1);
      if (++watches === 1) c.fake.emit("broker/inbox/added", { item: item() });
      return { thread: thread(), run: runResult(watches === 1 ? { state: "running", status: null } : {}), pending: [] };
    };
    expect(await bounded(wait(c.ctx), 2000)).toBe(5);
    expect(c.text.join("")).toContain("cxb reply");
    c.checkClean();
  });
  test("等待期间只接受目标运行的非 Guardian 事项", async () => {
    const c = context(["wait", "abcd1234"]);
    const watching = gate<void>();
    let watches = 0;
    c.fake.handle = (m) => {
      if (m === "broker/threads/list") return { threads: [thread()] };
      if (++watches === 2) watching.resolve();
      return { thread: thread(), run: runResult({ state: "running", status: null }), pending: [] };
    };
    const work = wait(c.ctx);
    await watching.wait();
    c.fake.emit("broker/inbox/added", { item: item({ id: "qu_别的线程", threadId: "other", runId: "run-其他" }) });
    c.fake.emit("broker/inbox/added", { item: item({ id: "qu_别的运行", runId: "other" }) });
    c.fake.emit("broker/inbox/added", { item: item({ id: "gd_拒绝", kind: "guardian" }) });
    c.fake.emit("broker/inbox/added", { item: item() });
    expect(await bounded(work, 2000)).toBe(5);
    expect(c.text.join("")).toContain("qu_test1234");
    expect(c.text.join("")).not.toContain("qu_别的");
    c.checkClean();
  });
  test("按父线程等 review：子线程上目标运行的事项也接住（任务说明 5.2）", async () => {
    const c = context(["wait", "abcd1234"]);
    const watching = gate<void>();
    let watches = 0;
    c.fake.handle = (m) => {
      if (m === "broker/threads/list") return { threads: [thread()] };
      if (++watches === 2) watching.resolve();
      return { thread: thread(), run: runResult({ state: "running", status: null, reviewThreadId: "review-child" }), pending: [] };
    };
    const work = wait(c.ctx);
    await watching.wait();
    c.fake.emit("broker/inbox/added", { item: item({ id: "qu_子线程", threadId: "review-child" }) });
    expect(await bounded(work, 2000)).toBe(5);
    expect(c.text.join("")).toContain("qu_子线程");
    c.checkClean();
  });
  test("快照显示 review 已在子线程上：订阅子线程，只存在于子线程快照里的未决事项也退出 5（任务说明 5.4）", async () => {
    const c = context(["wait", "abcd1234"]);
    c.fake.handle = (m, p) => {
      if (m === "broker/threads/list") return { threads: [thread()] };
      if (p?.threadId === "review-child") return { thread: thread(), run: runResult({ state: "running", status: null }), pending: [item({ id: "qu_子线程", threadId: "review-child" })] };
      return { thread: thread(), run: runResult({ state: "running", status: null, reviewThreadId: "review-child" }), pending: [] };
    };
    expect(await bounded(wait(c.ctx), 2000)).toBe(5);
    expect(c.text.join("")).toContain("qu_子线程");
    c.checkClean();
  });
  test("收到 broker/run/moved 后订阅子线程，子线程快照里目标运行的未决事项退出 5；别的运行的迁移不理会（任务说明 5.4）", async () => {
    const c = context(["wait", "abcd1234"]);
    const watching = gate<void>();
    let watches = 0;
    c.fake.handle = (m, p) => {
      if (m === "broker/threads/list") return { threads: [thread()] };
      if (p?.threadId === "review-child") return { thread: thread(), run: runResult({ state: "running", status: null }), pending: [item({ id: "qu_子线程", threadId: "review-child" })] };
      if (++watches === 2) watching.resolve();
      return { thread: thread(), run: runResult({ state: "running", status: null }), pending: [item({ id: "qu_别的运行", runId: "other" })] };
    };
    const work = wait(c.ctx);
    await watching.wait();
    c.fake.emit("broker/run/moved", { threadId: "thread-test", runId: "other", reviewThreadId: "别的子线程" });
    c.fake.emit("broker/run/moved", { threadId: "thread-test", runId: "run-test", reviewThreadId: "review-child" });
    expect(await bounded(work, 2000)).toBe(5);
    expect(c.text.join("")).toContain("qu_子线程");
    expect(c.text.join("")).not.toContain("qu_别的运行");
    expect(c.fake.calls.filter((r) => r.method === "broker/threads/watch").map((r) => r.params.threadId)).not.toContain("别的子线程");
    c.checkClean();
  });
  test("快照时由别的客户端持有、等待途中交接给 broker 后出现的事项：退出 5，且不答任何服务端请求", async () => {
    // 非 guardian 事项只产生在 broker 托管的线程上（详细设计 4.3 节），不能按快照时的 owner 过滤（N4 终审第 3 条）。
    const c = context(["wait", "abcd1234", "--json"]);
    const watching = gate<void>();
    let watches = 0;
    c.fake.handle = (m) => {
      if (m === "broker/threads/list") return { threads: [thread({ owner: "client" })] };
      if (++watches === 2) watching.resolve();
      return { thread: thread({ owner: "client" }), run: runResult({ state: "running", status: null }), pending: [] };
    };
    const work = wait(c.ctx);
    await watching.wait();
    c.fake.emit("broker/inbox/added", { item: item() });
    expect(await bounded(work, 2000)).toBe(5);
    expect(c.json[0]).toMatchObject({ ok: false, status: "waiting" });
    expect(c.fake.calls.every((r) => ["broker/threads/list", "broker/threads/watch"].includes(r.method))).toBe(true);
    c.checkClean();
  });

  test("别的客户端持有时只观察：运行结束照常输出结果，不答服务端请求", async () => {
    const c = context(["wait", "abcd1234", "--json"]);
    const watching = gate<void>();
    let watches = 0;
    c.fake.handle = (m) => {
      if (m === "broker/threads/list") return { threads: [thread({ owner: "client" })] };
      if (++watches === 2) watching.resolve();
      return { thread: thread({ owner: "client" }), run: runResult({ state: "running", status: null }), pending: [] };
    };
    const work = wait(c.ctx);
    await watching.wait();
    c.fake.emit("broker/inbox/added", { item: item({ id: "gd_拒绝", kind: "guardian" }) });
    c.fake.emit("broker/run/ended", { run: runResult() });
    expect(await bounded(work, 2000)).toBe(0);
    expect(c.json[0].alreadyDone).toBe(false);
    expect(c.fake.calls.every((r) => ["broker/threads/list", "broker/threads/watch"].includes(r.method))).toBe(true);
    c.checkClean();
  });
  test("已结束但 pending 继续等待结果补全", async () => {
    const c = context(["wait", "abcd1234", "--json"]);
    const watching = gate<void>();
    let watches = 0;
    const run = runResult(); run.turns[0]!.ready = "pending";
    c.fake.handle = (m) => { if (m === "broker/threads/list") return { threads: [thread()] }; if (++watches === 2) watching.resolve(); return { thread: thread(), run, pending: [] }; };
    const work = wait(c.ctx);
    await watching.wait();
    expect(c.json).toHaveLength(0);
    c.fake.emit("broker/thread/result", { runId: "run-test", turn: 0, result: runResult().turns[0] });
    expect(await bounded(work, 2000)).toBe(0);
    expect(c.json[0]).toMatchObject({ alreadyDone: true, result: "中文结果" });
    c.checkClean();
  });
  test("快照 run 为空仍按 lastRunId 读取磁盘，结果与 alreadyDone 正确", async () => {
    const dir = dirs.at(-1)!;
    mkdirSync(join(dir, "runs"));
    writeFileSync(join(dir, "runs", "run-test.json"), JSON.stringify({ epoch: 1, revision: 1, payload: runResult() }));
    const c = context(["wait", "abcd1234", "--json"]); c.ctx.workspace.workspaceDir = dir;
    c.fake.handle = (m) => m === "broker/threads/list" ? { threads: [thread()] } : { thread: thread(), run: null, pending: [] };
    expect(await wait(c.ctx)).toBe(0);
    expect(c.json[0]).toMatchObject({ runId: "run-test", result: "中文结果", alreadyDone: true });
    expect(c.fake.calls.at(-1)!.params.runId).toBe("run-test");
    c.checkClean();
  });
  test("没有运行返回 2，goalBlocked 与刹车 9，lost 4，失败 7", async () => {
    for (const status of ["无运行", "goalBlocked", "lost", "failed", "刹车"] as const) {
      const c = context(["wait", "abcd1234", "--json"]);
      const run = runResult(status === "lost" ? { state: "lost", status: null } : status === "刹车"
        ? { loopGuard: { kind: "noAction", count: 3, threshold: 3, turnIds: [], action: "stop" } }
        : { status: status === "无运行" ? null : status });
      c.fake.handle = (m) => m === "broker/threads/list" ? { threads: [thread()] }
        : { thread: thread({ lastRunId: status === "无运行" ? null : "run-test" }), run: status === "无运行" ? null : run, pending: [] };
      if (status === "无运行") await errorCode(wait(c.ctx), 2);
      else expect(await wait(c.ctx)).toBe(status === "lost" ? 4 : status === "failed" ? 7 : 9);
      c.checkClean();
    }
  });
  test("连接断开为 6，取消为 4，超时为 3 且订阅全部收束", async () => {
    for (const mode of ["断线", "取消", "超时"]) {
      const c = context(["wait", "abcd1234"]);
      if (mode === "超时") { c.ctx.parsed.explicit = new Set(["timeout"]); c.ctx.parsed.globals.timeout = 0.02; }
      const watching = gate<void>(); let watches = 0;
      c.fake.handle = (m) => { if (m === "broker/threads/list") return { threads: [thread()] }; if (++watches === 2) watching.resolve(); return { thread: thread(), run: runResult({ state: "running", status: null }), pending: [] }; };
      const work = wait(c.ctx);
      await watching.wait();
      if (mode === "断线") c.fake.disconnect();
      if (mode === "取消") await c.cancel();
      await errorCode(work, mode === "断线" ? 6 : mode === "取消" ? 4 : 3);
      // 外层期限取消内部等待，等待其微任务完成，不依赖固定延迟。
      await Promise.resolve(); await Promise.resolve();
      c.checkClean();
    }
  });
});

for (const json of [false, true]) test(`上游诊断 wait${json ? ' --json' : ''}：晚接入快照提示一次，最终错误退出七`, async () => {
  const c = context(['wait', 'abcd1234', ...(json ? ['--json'] : [])]);
  const run = runResult({ status: 'failed', upstreamError: upstreamError(), diagnosticRevision: 4,
    turns: [{ ...runResult().turns[0]!, status: 'failed', error: { message: 'Bearer privateWaitKey', codexErrorInfo: 'unauthorized' } }] });
  c.fake.handle = (method) => method === 'broker/threads/list' ? { threads: [thread()] }
    : { thread: thread(), run, pending: [] };
  expect(await wait(c.ctx)).toBe(7);
  // 最终轮有错误时只输出一行「失败原因」，不再补「最近上游报错（运行已结束）」摘要。
  expect(c.notices.filter(line => line.startsWith('[cxb] 最近上游报错：'))).toEqual([]);
  expect(c.notices.filter(line => line.startsWith('[cxb] 失败原因：'))).toEqual(['[cxb] 失败原因：Bearer ***（unauthorized）']);
  expect(c.text.join('') + JSON.stringify(c.json)).not.toContain('[cxb]');
  expect(c.notices.join('') + JSON.stringify(c.json)).not.toContain('privateWaitKey');
  if (json) {
    expect(c.json).toHaveLength(1);
    expect(c.json[0]).toMatchObject({ status: 'failed', upstreamError: run.upstreamError, error: { message: 'Bearer ***', codexErrorInfo: 'unauthorized' } });
  }
  c.checkClean();
});

test('上游诊断 wait：通知先于快照，旧版本快照不倒退诊断，最终摘要独立于版本', async () => {
  const c = context(['wait', 'abcd1234', '--json']);
  let watches = 0;
  c.fake.handle = (method) => {
    if (method === 'broker/threads/list') return { threads: [thread()] };
    if (++watches === 1) c.fake.emit('broker/run/updated', { runId: 'run-test', threadId: 'thread-test', diagnosticRevision: 8,
      upstreamError: upstreamError({ count: 8 }), lastProgressAt: null, state: 'running', status: null });
    return { thread: thread(), run: runResult({ status: 'failed', diagnosticRevision: 1, upstreamError: upstreamError({ count: 1 }),
      turns: [{ ...runResult().turns[0]!, error: { message: '终态原因' }, status: 'failed' }] }), pending: [] };
  };
  expect(await wait(c.ctx)).toBe(7);
  expect(c.json[0].upstreamError.count).toBe(8);
  expect(c.notices.filter(line => line.includes('失败原因：'))).toEqual(['[cxb] 失败原因：终态原因']);
  c.checkClean();
});

test('上游诊断 wait SA1b：finalTurnId 缺结果时不回退阶段错误', async () => {
  const c = context(['wait', 'abcd1234', '--json']);
  const run = runResult({ status: 'failed', finalTurnId: 'final-missing', upstreamError: upstreamError(), diagnosticRevision: 1,
    turns: [{ ...runResult().turns[0]!, error: { message: '阶段原因不能冒充' }, status: 'failed' }] });
  c.fake.handle = (method) => method === 'broker/threads/list' ? { threads: [thread()] } : { thread: thread(), run, pending: [] };
  expect(await wait(c.ctx)).toBe(7);
  expect(c.json[0]).not.toHaveProperty('error');
  expect(c.notices.join()).not.toContain('失败原因');
  expect(c.notices.join()).not.toContain('阶段原因不能冒充');
  c.checkClean();
});
