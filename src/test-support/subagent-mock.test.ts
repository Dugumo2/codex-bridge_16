/** 子代理模拟本身的行为：事件按真实时序、每线程独立状态、扣放、故障与观测。纯内存，不起进程。 */
import { describe, expect, test } from "bun:test";
import { createSubagentMock, type SubagentMockOptions } from "./subagent-mock";

function mock(options: SubagentMockOptions = { enabled: true }) {
  const sent: any[] = [];
  const m = createSubagentMock((message) => { sent.push(message); }, options);
  let id = 0;
  const call = (method: string, params?: any) => {
    const before = sent.length;
    const handled = m.handle({ id: `c${++id}`, method, params });
    const out = sent.slice(before);
    const reply = out.find((x) => x.id === `c${id}` && x.method === undefined);
    return { handled, out, reply, events: out.filter((x) => x !== reply) };
  };
  return { m, sent, call };
}

const spawn = (version: "v1" | "v2", started: "now" | "held" | "never", child = "子线程") => ({
  parentThreadId: "父线程", parentTurnId: "父轮", itemId: "派生1", childThreadId: child, version, started,
});

describe("子代理模拟", () => {
  test("未启用时不消费任何消息", () => {
    const { m, sent } = mock({});
    expect(m.handle({ id: 1, method: "fixture/state" })).toBe(false);
    expect(m.handle({ id: 2, method: "thread/loaded/list" })).toBe(false);
    expect(sent).toEqual([]);
    expect(m.state()).toEqual({});
  });

  test("v1 派生：started 接收方为空，completed 才带 C；不发子线程 thread/started", () => {
    const { call } = mock();
    const { events, reply } = call("fixture/spawnChild", spawn("v1", "never"));
    expect(events.map((e) => e.method)).toEqual(["item/started", "thread/status/changed", "item/completed", "turn/started"]);
    expect(events[0].params.item).toMatchObject({ type: "collabAgentToolCall", tool: "spawnAgent", receiverThreadIds: [], status: "inProgress" });
    expect(events[2].params.item).toMatchObject({ id: "派生1", receiverThreadIds: ["子线程"], status: "completed" });
    expect(events[3].params).toMatchObject({ threadId: "子线程", turn: { id: reply.result.turnId } });
    expect(events.some((e) => e.method === "thread/started")).toBe(false);
  });

  test("v2 派生：同一 itemId 的两条活动内容相同且已带接收方，不造 collabAgentToolCall", () => {
    const { call } = mock();
    const { events } = call("fixture/spawnChild", spawn("v2", "never"));
    const items = events.filter((e) => e.method?.startsWith("item/"));
    expect(items.map((e) => e.method)).toEqual(["item/started", "item/completed"]);
    expect(items[0].params.item).toEqual(items[1].params.item);
    expect(items[0].params.item).toMatchObject({ type: "subAgentActivity", kind: "started", agentThreadId: "子线程" });
    expect(events.some((e) => e.params?.item?.type === "collabAgentToolCall")).toBe(false);
  });

  test("held：派生证据、thread/started、首轮一起扣住，放行后按序发出", () => {
    const { call } = mock();
    const first = call("fixture/spawnChild", spawn("v1", "held"));
    expect(first.events.map((e) => e.method)).toEqual(["item/started"]);
    expect(first.reply.result.holdId).toBe("spawn:子线程");
    const released = call("fixture/release", { holdId: "spawn:子线程" });
    expect(released.events.map((e) => e.method)).toEqual(["thread/started", "thread/status/changed", "item/completed", "turn/started"]);
    expect(released.events[0].params.thread).toMatchObject({ id: "子线程", parentThreadId: "父线程", canAcceptDirectInput: true });
  });

  test("复用：v1 发送输入与恢复在 started 已带接收方；只有 v1Input、v2Followup 开新轮", () => {
    const { call } = mock();
    call("fixture/spawnChild", spawn("v1", "never"));
    const cases: Array<[string, boolean]> = [["v1Input", true], ["v1Steer", false], ["v1ResumeNoop", false], ["v2Message", false], ["v2Followup", true]];
    for (const [mode, opens] of cases) {
      const { events, reply } = call("fixture/reuseChild", { parentThreadId: "父线程", parentTurnId: "父轮", itemId: mode, childThreadId: "子线程", version: mode.slice(0, 2), mode });
      const started = events.find((e) => e.method === "item/started");
      if (mode.startsWith("v1")) {
        expect(started.params.item.receiverThreadIds).toEqual(["子线程"]);
        expect(started.params.item.tool).toBe(mode === "v1ResumeNoop" ? "resumeAgent" : "sendInput");
      } else {
        expect(started.params.item).toMatchObject({ type: "subAgentActivity", kind: "interacted" });
      }
      expect(events.some((e) => e.method === "turn/started")).toBe(opens);
      expect(reply.result.turnId !== null).toBe(opens);
    }
    const failed = call("fixture/reuseChild", { parentThreadId: "父线程", parentTurnId: "父轮", itemId: "失败", childThreadId: "子线程", version: "v1", mode: "v1Input", fail: true });
    expect(failed.events.at(-1).params.item.status).toBe("failed");
    expect(failed.events.some((e) => e.method === "turn/started")).toBe(false);
  });

  test("注入请求：可指定 requestId 与重复发送；按线程记录实际回答与次数", () => {
    const { m, call } = mock();
    call("fixture/spawnChild", spawn("v1", "never"));
    const injected = call("fixture/inject", { threadId: "子线程", kind: "request", requestId: 7, payload: { method: "item/commandExecution/requestApproval", params: { turnId: "子线程-turn-1" } } });
    expect(injected.events[0]).toMatchObject({ id: 7, method: "item/commandExecution/requestApproval", params: { threadId: "子线程", turnId: "子线程-turn-1" } });
    call("fixture/inject", { threadId: "子线程", kind: "request", requestId: 7, payload: {} });
    expect(m.handle({ id: 7, result: { decision: "accept" } })).toBe(true);
    expect(m.handle({ id: 7, result: { decision: "accept" } })).toBe(true);
    expect(m.state()["子线程"].answers).toEqual([{ requestId: 7, count: 2, result: { decision: "accept" } }]);
    expect(m.handle({ id: "未知", result: {} })).toBe(false);
  });

  test("每线程独立：中断子线程不影响父线程；never 不完成；v2 拒绝 start、steer 与 goal 写入但允许中断", () => {
    const { m, call } = mock();
    call("fixture/spawnChild", spawn("v1", "never"));
    call("fixture/spawnChild", { ...spawn("v2", "never", "子二"), itemId: "派生2" });
    const parent = call("fixture/turn", { threadId: "父线程", action: "start" });
    const interrupted = call("turn/interrupt", { threadId: "子线程", turnId: "子线程-turn-1" });
    expect(interrupted.handled).toBe(true);
    expect(interrupted.events.map((e) => e.params.turn.status)).toEqual(["interrupted"]);
    expect(m.state()["父线程"].turns).toEqual([{ id: parent.reply.result.turnId, status: "inProgress" }]);
    call("fixture/interruptBehavior", { threadId: "子二", mode: "never" });
    const never = call("turn/interrupt", { threadId: "子二", turnId: "子二-turn-1" });
    expect(never.reply.result).toEqual({});
    expect(never.events).toEqual([]);
    expect(m.state()["子二"].interrupts).toHaveLength(1);
    expect(call("turn/start", { threadId: "子二" }).reply.error.code).toBe(-32600);
    expect(call("turn/steer", { threadId: "子二" }).reply.error.code).toBe(-32600);
    expect(call("thread/goal/set", { threadId: "子二", status: "paused" }).reply.error.code).toBe(-32600);
    expect(call("thread/goal/set", { threadId: "子线程", status: "paused" }).reply.result.goal.status).toBe("paused");
    expect(m.state()["子线程"].goalWrites).toHaveLength(1);
    // 父线程不是子线程：中断交还所在夹具。
    expect(call("turn/interrupt", { threadId: "父线程" }).handled).toBe(false);
  });

  test("thread/read 给出 parentThreadId、source、canAcceptDirectInput；父线程交还夹具", () => {
    const { call } = mock();
    call("fixture/spawnChild", spawn("v2", "never"));
    const read = call("thread/read", { threadId: "子线程", includeTurns: false });
    expect(read.reply.result.thread).toMatchObject({
      id: "子线程", parentThreadId: "父线程", canAcceptDirectInput: false,
      source: { subAgent: { thread_spawn: { parent_thread_id: "父线程", depth: 1, agent_path: "/root/子线程" } } },
    });
    expect(call("thread/read", { threadId: "父线程" }).handled).toBe(false);
  });

  test("ancestor 分页区分归档；分页失败、重复游标、慢回包可控", () => {
    const { call } = mock();
    for (let i = 0; i < 3; i++) call("fixture/spawnChild", { ...spawn("v1", "never", `子${i}`), itemId: `派${i}` });
    call("fixture/spawnChild", { ...spawn("v1", "never", "旧子"), itemId: "派旧", archived: true });
    call("fixture/spawnChild", { parentThreadId: "子0", parentTurnId: "子0-turn-1", itemId: "孙派", childThreadId: "孙", version: "v1", started: "never" });
    const page1 = call("thread/list", { ancestorThreadId: "父线程", archived: false, limit: 2 }).reply.result;
    expect(page1.data.map((t: any) => t.id)).toEqual(["子0", "子1"]);
    const page2 = call("thread/list", { ancestorThreadId: "父线程", archived: false, limit: 2, cursor: page1.nextCursor }).reply.result;
    expect(page2.data.map((t: any) => t.id)).toEqual(["子2", "孙"]);
    expect(page2.nextCursor).toBeNull();
    expect(call("thread/list", { ancestorThreadId: "父线程", archived: true }).reply.result.data.map((t: any) => t.id)).toEqual(["旧子"]);
    expect(call("thread/loaded/list", {}).reply.result.data).toEqual(["子0", "子1", "子2", "孙"]);
    call("fixture/listFault", { mode: "pageError" });
    expect(call("thread/list", { ancestorThreadId: "父线程" }).reply.error.code).toBe(-32603);
    expect(call("thread/list", { ancestorThreadId: "父线程" }).reply.result).toBeDefined();
    call("fixture/listFault", { mode: "duplicateCursor" });
    expect(call("thread/list", { ancestorThreadId: "父线程", cursor: "dup" }).reply.result.nextCursor).toBe("dup");
    call("fixture/listFault", { mode: "slow" });
    const slow = call("thread/list", { ancestorThreadId: "父线程" });
    expect(slow.handled).toBe(true);
    expect(slow.reply).toBeUndefined();
    const released = call("fixture/release", { holdId: "list" });
    expect(released.out.some((x) => x.result?.data)).toBe(true);
  });

  test("autoAttach=missing：子线程消息不发给本连接，thread/resume 附着后补发并记录附着请求", () => {
    const { m, call } = mock({ enabled: true, autoAttach: "missing" });
    const spawned = call("fixture/spawnChild", spawn("v1", "never"));
    // 父线程上的条目照常发出，子线程自己的状态与轮次被压住。
    expect(spawned.events.map((e) => e.method)).toEqual(["item/started", "item/completed"]);
    const injected = call("fixture/inject", { threadId: "子线程", kind: "request", payload: {} });
    expect(injected.events).toEqual([]);
    const resumed = call("thread/resume", { threadId: "子线程", excludeTurns: true });
    expect(resumed.events.map((e) => e.method)).toEqual(["thread/status/changed", "turn/started", "item/commandExecution/requestApproval"]);
    expect(m.state()["子线程"].attachments).toHaveLength(1);
  });
});
