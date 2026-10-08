import { describe, expect, test } from "bun:test";
import { createSubagentMock } from "./subagent-mock";
import type { BackgroundTerminalsMockOptions } from "./background-terminals-mock";

function fixture(options: BackgroundTerminalsMockOptions = { enabled: true }, subagents = true) {
  const sent: any[] = [];
  const mock = createSubagentMock((message) => sent.push(message), { enabled: subagents }, options);
  let id = 0;
  return (method: string, params: object = {}) => {
    const requestId = ++id;
    const handled = mock.handle({ id: requestId, method, params });
    return { handled, ...sent.find((m) => m.id === requestId) };
  };
}
const prefix = "thread/backgroundTerminals/";
const seeds = [{ itemId: "命令10", processId: "10" }, { itemId: "命令2", processId: "2" }, { itemId: "命令100", processId: "100" }];

describe("后台命令夹具：0.160.0 协议", () => {
  test.each([true, false])("子代理开关 %s：后台能力默认关闭，三个接口均报 -32601", (subagents) => {
    const call = fixture({}, subagents);
    for (const method of ["list", "terminate", "clean"]) {
      expect(call(prefix + method, { threadId: "父", processId: "2" })).toMatchObject({ handled: true, error: { code: -32601 } });
    }
  });
  test("完整字段、内部编号数值排序与游标分页，缺省读取全部", () => {
    const call = fixture({ enabled: true, threads: { 父: { sessions: seeds } } });
    const first = call(prefix + "list", { threadId: "父", limit: 1 }).result;
    expect(first).toEqual({ data: [{ itemId: "命令2", processId: "2", command: "模拟命令 2", cwd: "D:\\", osPid: null, cpuPercent: null, rssKb: null }], nextCursor: "2" });
    expect(call(prefix + "list", { threadId: "父", cursor: first.nextCursor, limit: 1 }).result.nextCursor).toBe("10");
    expect(call(prefix + "list", { threadId: "父", cursor: "10" }).result).toMatchObject({ data: [{ processId: "100" }], nextCursor: null });
    expect(call(prefix + "list", { threadId: "父" }).result.data).toHaveLength(3);
    expect(call(prefix + "list", { threadId: "父", cursor: "0", limit: 0 }).result.data).toHaveLength(1);
    expect(call(prefix + "list", { threadId: "父", cursor: "错误" }).error.code).toBe(-32600);
  });
  test("camelCase 与字符串 i32 编号；错误参数不删除会话", () => {
    const call = fixture({ enabled: true, threads: { 父: { sessions: seeds } } });
    for (const params of [{ thread_id: "父", process_id: "2" }, { threadId: "父", processId: 2 }, { threadId: "父", processId: "2147483648" }]) {
      expect(call(prefix + "terminate", params).error.code).toBe(-32600);
    }
    expect(call(prefix + "list", { threadId: "父" }).result.data).toHaveLength(3);
  });
  test.each(["success", "exited", "remaining"] as const)("terminate %s：布尔返回值与复查列表分别观测", (termination) => {
    const call = fixture({ enabled: true, threads: { 父: { sessions: [{ ...seeds[0]!, termination }] } } });
    expect(call(prefix + "terminate", { threadId: "父", processId: "10" }).result).toEqual({ terminated: termination === "success" });
    expect(call(prefix + "list", { threadId: "父" }).result.data).toHaveLength(termination === "remaining" ? 1 : 0);
    expect(call(prefix + "terminate", { threadId: "父", processId: "999" }).result).toEqual({ terminated: false });
  });
  test.each(["v1", "v2"] as const)("%s 子线程：中断不清后台命令，clean 只清本线程并返回空对象", (version) => {
    const call = fixture({ enabled: true, threads: { 父: { sessions: seeds }, 子: { sessions: seeds } } });
    call("fixture/spawnChild", { parentThreadId: "父", parentTurnId: "父轮", itemId: "派生", childThreadId: "子", version, started: "now" });
    call("turn/interrupt", { threadId: "子", turnId: "子-turn-1" });
    expect(call(prefix + "list", { threadId: "子" }).result.data).toHaveLength(3);
    expect(call(prefix + "clean", { threadId: "父" }).result).toEqual({});
    expect(call(prefix + "list", { threadId: "父" }).result.data).toEqual([]);
    expect(call(prefix + "list", { threadId: "子" }).result.data).toHaveLength(3);
  });
  test.each(["unsupported", "hang", "notFound"] as const)("故障 %s：三个接口分别支持，不伪造成功回包", (fault) => {
    const call = fixture({ enabled: true, threads: { 父: { sessions: seeds, fault } } });
    for (const method of ["list", "terminate", "clean"]) {
      const reply = call(prefix + method, { threadId: "父", processId: "10" });
      expect(reply.handled).toBe(true);
      expect(reply.result).toBeUndefined();
      if (fault === "hang") expect(reply.id).toBeUndefined();
      else expect(reply.error).toMatchObject({ code: fault === "unsupported" ? -32601 : -32600,
        message: fault === "unsupported" ? `Method not found: ${prefix + method}` : "thread not found: 父" });
    }
  });
  test("独立故障步骤、动态新会话与状态快照可供重复 stop 观测", () => {
    const call = fixture({ enabled: true });
    call("fixture/backgroundTerminals/set", { threadId: "父", scenario: { sessions: seeds, fault: "hang", faultMethod: "terminate" } });
    expect(call(prefix + "list", { threadId: "父" }).result.data).toHaveLength(3);
    expect(call(prefix + "terminate", { threadId: "父", processId: "2" }).id).toBeUndefined();
    const state = call("fixture/backgroundTerminals/state").result;
    expect(state.threads.父).toHaveLength(3);
    expect(state.calls.map((c: any) => c.method)).toEqual([prefix + "list", prefix + "terminate"]);
    state.threads.父.length = 0;
    expect(call("fixture/backgroundTerminals/state").result.threads.父).toHaveLength(3);
  });
});
