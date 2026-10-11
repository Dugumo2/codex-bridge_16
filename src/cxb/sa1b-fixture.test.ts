import { describe, expect, test } from "bun:test";
import { createSa1bMock, type RpcMessage } from "../test-support/sa1b-mock";

function fixture() {
  const out: RpcMessage[] = [];
  const mock = createSa1bMock(x => out.push(x), { pageSize: 2 });
  let id = 0;
  const call = (method: string, params: any = {}) => {
    const requestId = ++id; mock.handle({ id: requestId, method, params });
    return out.find(x => x.id === requestId);
  };
  const ctl = (name: string, params: any = {}) => call(`fixture/sa1b/${name}`, params)?.result;
  const root = call("turn/start", { threadId: "P", input: [{ type: "text", text: "初始任务" }] })!.result.turn.id;
  const spawn = (child = "C", parent = "P", turn = root, extra = {}) => ctl("spawn", { parentThreadId: parent, parentTurnId: turn, childThreadId: child, ...extra });
  return { mock, out, call, ctl, root, spawn };
}

describe("SA1b 夹具自身契约", () => {
  test("反复读取state不把观测响应递归塞回事件日志", () => {
    const f = fixture(), before = f.mock.state().events.length;
    for (let i = 0; i < 100; i++) f.ctl("state");
    expect(f.mock.state().events).toHaveLength(before);
  });
  for (const version of ["v1", "v2"] as const) test(`${version} 派生、孙代理与复用保存独立活动`, () => {
    const f = fixture(); const c = f.spawn("C", "P", f.root, { version }); f.spawn("G", "C", c.turnId, { version });
    f.ctl("turn", { threadId: "C", action: "complete", output: "旧结果" });
    const reused = f.ctl("reuse", { parentThreadId: "P", parentTurnId: f.root, childThreadId: "C" });
    expect(reused.turnId).not.toBe(c.turnId); expect(f.mock.state().threads.C!.turns).toHaveLength(2);
    const data = f.call("thread/list", { ancestorThreadId: "P" })!.result.data;
    expect(data.map((x: any) => x.id)).toEqual(["C", "G"]);
    expect(data[1].source.subAgent.thread_spawn.depth).toBe(2);
  });
  for (const mode of ["normal", "delayed", "dropped"] as const) test(`完成消息 ${mode} 与可读结果独立`, () => {
    const f = fixture(); const c = f.spawn(); f.ctl("delivery", { childThreadId: "C", mode });
    f.ctl("turn", { threadId: "C", action: "complete", output: "随机结果" });
    expect(f.call("thread/items/list", { threadId: "C", turnId: c.turnId })!.result.data[0].text).toBe("随机结果");
    expect(f.out.filter(x => x.method === "turn/completed")).toHaveLength(1);
    expect(f.out.filter(x => x.method === "fixture/sa1b/completionMessage")).toHaveLength(mode === "normal" ? 1 : 0);
    const released = f.ctl("release", { name: "completion:C" }); expect(released.released).toBe(mode === "delayed" ? 1 : 0);
  });
  for (const mode of ["started", "steered", "replyLost", "transportError"] as const) test(`start ${mode} 真实保存 clientId 且条目 id 不同`, () => {
    const f = fixture(); f.ctl("startBehavior", { threadId: "P", mode });
    if (mode !== "steered") f.ctl("turn", { threadId: "P", action: "complete" });
    const response = f.call("turn/start", { threadId: "P", input: [{ type: "text", text: "证据" }], clientUserMessageId: "unique-client" });
    const t = f.mock.state().threads.P!; const value = t.turns.at(-1)!.items.at(-1);
    expect(value.clientId).toBe("unique-client"); expect(value.id).not.toBe(value.clientId);
    expect(t.turns).toHaveLength(mode === "steered" ? 1 : 2);
    if (mode === "replyLost") { expect(response).toBeUndefined(); expect(f.ctl("release", { name: "reply:P" }).released).toBe(1); }
    else if (mode === "transportError") expect(response!.error).toBeDefined();
    else expect(response!.result.turn.id).toBe(t.turns.at(-1)!.id);
  });
  test("steer 必须匹配 expectedTurnId，错误不写入", () => {
    const f = fixture(); expect(f.call("turn/steer", { threadId: "P", expectedTurnId: "foreign", clientUserMessageId: "bad" })!.error).toBeDefined();
    expect(f.mock.state().threads.P!.inputs).toHaveLength(1);
    expect(f.call("turn/steer", { threadId: "P", expectedTurnId: f.root, clientUserMessageId: "good" })!.result.turnId).toBe(f.root);
  });
  test("请求栅栏在执行前扣住，放行时可被竞争轮吸收且调用只记一次", () => {
    const f = fixture(); f.ctl("turn", { threadId: "P", action: "complete" });
    f.ctl("fence", { name: "start", direction: "request", method: "turn/start", threadId: "P" });
    expect(f.call("turn/start", { threadId: "P", clientUserMessageId: "race" })).toBeUndefined();
    expect(f.mock.state().threads.P!.turns).toHaveLength(1);
    f.ctl("turn", { threadId: "P", action: "start", turnId: "foreign", announce: false });
    f.ctl("release", { name: "start" });
    expect(f.mock.state().threads.P!.turns.at(-1)!.items[0].clientId).toBe("race");
    expect(f.mock.state().calls.filter(x => x.method === "turn/start")).toHaveLength(2);
  });
  test("pendingInit 与首轮缺失不伪造已完成轮", () => {
    const f = fixture(); expect(f.spawn("C", "P", f.root, { pendingInit: true }).turnId).toBeNull();
    expect(f.spawn("D", "P", f.root, { firstTurn: false }).turnId).toBeNull();
    expect(f.mock.state().threads.C!.turns).toEqual([]); expect(f.mock.state().threads.C!.pendingInit).toBe(true);
    expect(f.out.some(x => x.method === "turn/completed")).toBe(false);
  });
  test("分页真实跨页，clientId 不被 summary 伪造", () => {
    const f = fixture();
    for (let i = 0; i < 5; i++) f.ctl("item", { threadId: "P", item: { id: `f-${i}`, type: "reasoning", summary: [] } });
    f.call("turn/steer", { threadId: "P", expectedTurnId: f.root, clientUserMessageId: "last-page" });
    let cursor = null, items: any[] = [], pages = 0;
    do { const page: any = f.call("thread/items/list", { threadId: "P", turnId: f.root, cursor })!.result; items.push(...page.data); cursor = page.nextCursor; pages++; } while (cursor);
    expect(pages).toBe(4); expect(items.filter(x => x.clientId === "last-page")).toHaveLength(1);
  });
  for (const mode of ["error", "notFound", "notLoaded", "duplicateCursor", "dropped"]) test(`读取故障 ${mode} 有界次数与复原`, () => {
    const f = fixture(); f.ctl("fault", { method: "thread/read", threadId: "P", mode, count: 1 });
    const reply = f.call("thread/read", { threadId: "P" });
    if (mode === "dropped") expect(reply).toBeUndefined();
    else if (mode === "duplicateCursor") expect(reply!.result.nextCursor).toBe("duplicate");
    else if (mode === "notLoaded") expect(reply!.result.thread.status.type).toBe("notLoaded");
    else expect(reply!.error).toBeDefined();
    expect(f.call("thread/read", { threadId: "P" })!.result.thread.status.type).toBe("active");
  });
  test("栅栏先扣住后放行，持久读取不随通知扣住而消失", () => {
    const f = fixture(); f.ctl("fence", { name: "end", method: "turn/completed", threadId: "P" });
    f.ctl("turn", { threadId: "P", action: "complete", output: "结果" });
    expect(f.out.some(x => x.method === "turn/completed")).toBe(false);
    expect(f.mock.state().threads.P!.turns[0]!.status).toBe("completed");
    expect(f.ctl("release", { name: "end" }).released).toBe(1);
    expect(f.out.filter(x => x.method === "turn/completed")).toHaveLength(1);
    expect(f.ctl("release", { name: "end" }).released).toBe(0);
  });
  test("goal 与净用量原值、请求实际回答次数按线程隔离", () => {
    const f = fixture(); f.spawn(); f.ctl("goal", { threadId: "P", status: "active", tokenBudget: 200, tokensUsed: 70 });
    f.ctl("usage", { threadId: "C", inputTokens: 100, cachedInputTokens: 40, outputTokens: 10 });
    expect(f.call("thread/goal/get", { threadId: "P" })!.result.goal.tokensUsed).toBe(70);
    expect(f.mock.state().threads.C!.usage.total).toMatchObject({ inputTokens: 100, cachedInputTokens: 40, outputTokens: 10 });
    f.ctl("inject", { message: { id: "ask", method: "item/tool/requestUserInput", params: { threadId: "C" } } });
    expect(f.mock.state().threads.C!.answers).toEqual([]); f.mock.handle({ id: "ask", result: { answers: {} } });
    expect(f.mock.state().threads.C!.answers).toHaveLength(1); expect(f.mock.state().threads.P!.answers).toHaveLength(0);
  });
  test("v2 子线程拒绝直接输入和 goal 写入但可中断", () => {
    const f = fixture(); const c = f.spawn("C", "P", f.root, { version: "v2" });
    expect(f.call("turn/start", { threadId: "C" })!.error).toBeDefined();
    expect(f.call("thread/goal/set", { threadId: "C" })!.error).toBeDefined();
    f.call("turn/interrupt", { threadId: "C", turnId: c.turnId });
    expect(f.mock.state().threads.C!.turns[0]!.status).toBe("interrupted");
  });
  for (const status of ["errored", "shutdown"]) test(`${status} 用代理状态上报，轮次只发有效TurnStatus`, () => {
    const f = fixture(); f.spawn(); f.ctl("turn", { threadId: "C", action: "complete", status });
    expect(f.mock.state().threads.C!.turns[0]!.status).toBe(status === "errored" ? "failed" : "interrupted");
    const result = f.out.find(x => x.method === "item/completed" && x.params?.item?.agentsStates?.C?.status === status);
    expect(result).toBeDefined();
    if (status === "shutdown") expect(f.mock.state().threads.C!.loaded).toBe(false);
  });
});
