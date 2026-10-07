import { afterEach, expect, test } from "bun:test";
import { NO_RESPONSE } from "../rpc";
import { CxbError, type BrokerConnection } from "./contracts";
import { FakeRpc, runResult } from "./test-fixtures/run-fakes";
import { cleanupContexts, context, errorCode } from "./test-fixtures/n4b-support";
import { deferred, messageBarrier, TestWatchdogError, watchdog } from "./test-fixtures/test-watchdog";
import { waitEnded } from "./test-fixtures/wait-ended";

afterEach(cleanupContexts, 5000);

test("FakeRpc 通知先通配后具体，单个监听器异常不阻断后续监听且可检查", () => {
  const c = context(["wait", "abcd1234"]);
  const events: string[] = [];
  const wildcardError = new Error("通配监听失败");
  const specificError = new Error("具体监听失败");
  // 先登记具体监听，顺序仍应由协议约定决定。
  const off = [
    c.fake.rpc.on("测试通知", () => { events.push("具体失败"); throw specificError; }),
    c.fake.rpc.on("测试通知", () => { events.push("具体成功"); }),
    c.fake.rpc.onAny(() => { events.push("通配失败"); throw wildcardError; }),
    c.fake.rpc.onAny((method) => { events.push(`通配成功：${method}`); }),
  ];
  try {
    expect(() => c.fake.emit("测试通知", {})).not.toThrow();
    expect(events).toEqual(["通配失败", "通配成功：测试通知", "具体失败", "具体成功"]);
    expect(c.fake.notificationErrors).toEqual([
      { method: "测试通知", listener: "通配", error: wildcardError },
      { method: "测试通知", listener: "具体", error: specificError },
    ]);
    // 仅显式核对过的故意错误可移出列表；其他错误由 afterEach 报告。
    c.fake.notificationErrors.splice(0);
  } finally { for (const dispose of off) dispose(); }
  c.checkClean();
});

test("消息屏障等待嵌套事件循环中的调用与服务端回包稳定", async () => {
  const c = context(["wait", "abcd1234"]);
  const answer = deferred<object>();
  const off = c.fake.rpc.onAnyRequest(() => answer.promise);
  const immediates = new Set<ReturnType<typeof setImmediate>>();
  const next = (callback: () => void) => {
    const handle = setImmediate(() => { immediates.delete(handle); callback(); });
    immediates.add(handle);
  };
  const request = c.fake.serverRequest("测试请求", {}, "request-1");
  void request.catch(() => {});
  next(() => next(() => {
    c.fake.calls.push({ method: "延迟调用", params: {}, options: {} });
    answer.resolve({ done: true });
  }));
  try {
    await messageBarrier(() => [c.fake.calls.length, c.fake.replies.length, c.fake.inFlightServerRequests]);
    expect(c.fake.calls.map((call) => call.method)).toEqual(["延迟调用"]);
    expect(c.fake.replies).toEqual([{ id: "request-1", result: { done: true } }]);
    expect(c.fake.inFlightServerRequests).toBe(0);
  } finally {
    for (const handle of immediates) clearImmediate(handle);
    answer.resolve({ done: true });
    await watchdog(request);
    off();
  }
  c.checkClean();
});

test("消息状态一直变化时屏障以独立看门狗拒绝并停止采样", async () => {
  let samples = 0;
  await expect(messageBarrier(() => [++samples], 20)).rejects.toBeInstanceOf(TestWatchdogError);
  const stoppedAt = samples;
  await watchdog(new Promise<void>((resolve) => setImmediate(resolve)));
  expect(samples).toBe(stoppedAt);
});

test("取消入口等待每个正常清理回调完成", async () => {
  const c = context(["wait", "abcd1234"]);
  const events: string[] = [];
  const off = [
    c.ctx.registerCleanup(async () => { await Promise.resolve(); events.push("第一项完成"); }),
    c.ctx.registerCleanup(() => { events.push("第二项完成"); }),
  ];
  try {
    await watchdog(c.cancel(), 3000, "取消入口未按期完成");
    expect(events.sort()).toEqual(["第一项完成", "第二项完成"]);
  } finally { for (const dispose of off) dispose(); }
  c.checkClean();
});

test("取消入口隔离同步异常与永不完成的回调，仍执行其余回调后汇总错误", async () => {
  const c = context(["wait", "abcd1234"]);
  const held = deferred<void>();
  const failure = new Error("故意抛错的清理回调");
  const events: string[] = [];
  const off = [
    c.ctx.registerCleanup(() => { throw failure; }),
    c.ctx.registerCleanup(() => held.promise),
    c.ctx.registerCleanup(() => { events.push("其余清理已执行"); }),
  ];
  try {
    const error = await watchdog(c.cancel(), 3000, "取消入口未按期拒绝").catch((e) => e);
    expect(error).toBeInstanceOf(AggregateError);
    expect(error.errors).toHaveLength(2);
    expect(error.errors[0]).toBe(failure);
    expect(error.errors[1]).toBeInstanceOf(TestWatchdogError);
    expect(events).toEqual(["其余清理已执行"]);
  } finally { held.resolve(); for (const dispose of off) dispose(); }
  c.checkClean();
});

test("测试看门狗与产品期限分离，防挂失败不能冒充产品退出码 3", async () => {
  const waiting = deferred<void>();
  try {
    const error = await watchdog(waiting.promise, 20, "故意不回包").catch((e) => e);
    expect(error).toBeInstanceOf(TestWatchdogError);
    expect(error).not.toBeInstanceOf(CxbError);
    await expect(errorCode(Promise.reject(error), 3)).rejects.toThrow();
    await errorCode(Promise.reject(new CxbError("TIMEOUT", "产品期限已到")), 3);
  } finally { waiting.resolve(); }
});

test("FakeRpc 真正投递请求，专用处理器优先，NO_RESPONSE 不产生回包，撤销后不再调用", async () => {
  const fake = new FakeRpc();
  const off = [fake.rpc.onAnyRequest(() => NO_RESPONSE), fake.rpc.onRequest("审批", () => ({ decision: "accept" }))];
  try {
    await fake.serverRequest("审批", {}, 1);
    await fake.serverRequest("提问", {}, 2);
    expect(fake.replies).toEqual([{ id: 1, result: { decision: "accept" } }]);
    expect(fake.inFlightServerRequests).toBe(0);
  } finally { for (const dispose of off) dispose(); }
  await fake.serverRequest("审批", {}, 3);
  expect(fake.replies.at(-1)).toMatchObject({ id: 3, error: { code: -32601 } });
  expect(fake.subscriptions).toBe(0);
});

for (const early of [false, true]) test(`waitEnded 结果${early ? "先于终态" : "晚于终态"}到达，按运行与轮次补齐并撤销监听`, async () => {
  const fake = new FakeRpc();
  const snapshot = deferred<any>();
  const pending = runResult();
  pending.turns[0] = { ...pending.turns[0]!, ready: "pending", output: "" };
  fake.handle = () => snapshot.promise;
  const conn = { rpc: fake.rpc } as BrokerConnection;
  const work = waitEnded(conn, "thread-test", "run-test");
  try {
    const result = { runId: "run-test", turn: 0, result: runResult().turns[0] };
    if (early) fake.emit("broker/thread/result", result);
    fake.emit("broker/run/ended", { run: pending });
    fake.emit("broker/thread/result", { ...result, runId: "run-other" });
    snapshot.resolve({ run: runResult({ state: "running", status: null }) });
    if (!early) fake.emit("broker/thread/result", result);
    expect((await watchdog(work)).turns[0]).toEqual(runResult().turns[0]);
  } finally { snapshot.resolve({ run: pending }); fake.disconnect(); await Promise.allSettled([work]); }
  expect(fake.subscriptions).toBe(0);
});
