import { expect, test } from "bun:test";
import { createSa1bMock, type RpcMessage } from "../test-support/sa1b-mock";
import { readThreadResults } from "./thread-result-reader";
import { FakeRpc } from "./test-fixtures/run-fakes";

test("C 历史快照保存血缘、归档、轮次、goal 与编号，不恢复订阅或重放 spawn", () => {
  const outgoing: RpcMessage[] = [];
  const first = createSa1bMock(message => outgoing.push(message));
  let serial = 0;
  const call = (method: string, params: any = {}) => {
    const id = ++serial; first.handle({ id, method, params }); return outgoing.find(message => message.id === id)?.result;
  };
  const parent = call("turn/start", { threadId: "root" }).turn.id;
  const child = call("fixture/sa1b/spawn", { parentThreadId: "root", parentTurnId: parent, childThreadId: "old" }).turnId;
  call("fixture/sa1b/turn", { threadId: "old", turnId: child, action: "complete", output: "历史结果" });
  call("fixture/sa1b/goal", { threadId: "old", status: "complete", objective: "历史目标" });
  call("fixture/sa1b/metadata", { threadId: "old", archived: true, loaded: false });
  call("fixture/sa1b/turn", { threadId: "root", action: "complete" });
  const saved = first.snapshot(), messages: RpcMessage[] = [];
  const restored = createSa1bMock(message => messages.push(message), { snapshot: saved, pageSize: 1, duplicateListIds: ["old"] });
  expect(messages).toEqual([]);
  expect(restored.state().subscriptions.broker).toEqual([]);
  expect(restored.state().calls).toEqual([]);
  expect(restored.snapshot()).toEqual(saved);
  restored.handle({ id: "list", method: "thread/list", params: { ancestorThreadId: "root", archived: true } });
  const page = messages.find(message => message.id === "list")!.result;
  expect(page.data[0]).toMatchObject({ id: "old", parentThreadId: "root", status: { type: "notLoaded" } });
  expect(page.nextCursor).toBe("1");
  restored.handle({ id: "page2", method: "thread/list", params: { ancestorThreadId: "root", archived: true, cursor: page.nextCursor } });
  expect(messages.find(message => message.id === "page2")!.result).toMatchObject({ data: [{ id: "old" }], nextCursor: null });
  restored.handle({ id: "resume", method: "thread/resume", params: { threadId: "root" } });
  expect(restored.state().subscriptions.broker).toEqual(["root"]);
  restored.handle({ id: "start", method: "turn/start", params: { threadId: "root" } });
  expect(messages.find(message => message.id === "start")!.result.turn.id).toBe(`root-turn-${saved.serial + 1}`);
  expect(messages.filter(message => message.method === "item/completed" && message.params.item.tool === "spawnAgent")).toEqual([]);
  expect(restored.state().threads.old).toMatchObject({ archived: true, loaded: false, parentThreadId: "root",
    goal: { status: "complete", objective: "历史目标" }, turns: [{ id: child, status: "completed" }] });
  // 调用方修改导出快照不会反向改写运行中的模拟服务器。
  saved.threads.old!.goal.status = "active";
  expect(restored.state().threads.old!.goal.status).toBe("complete");
  restored.handle({ id: "reuse", method: "fixture/sa1b/reuse", params: {
    parentThreadId: "root", parentTurnId: `root-turn-${saved.serial + 1}`, childThreadId: "old",
  } });
  const reused = messages.find(message => message.id === "reuse")!.result.turnId;
  expect(reused).not.toBe(child);
  expect(restored.state().subscriptions.broker).toContain("old");
  expect(messages.some(message => message.method === "turn/started" && message.params.turn.id === reused)).toBe(true);
  expect(messages.some(message => message.method === "turn/started" && message.params.turn.id === child)).toBe(false);
  expect(messages.some(message => message.method === "item/completed" && message.params.item.tool === "spawnAgent")).toBe(false);
});

test("C 历史快照拒绝损坏的编号状态和线程索引", () => {
  expect(() => createSa1bMock(() => {}, { snapshot: { version: 1, serial: -1, threads: {} } })).toThrow("编号状态");
  const mock = createSa1bMock(() => {});
  mock.handle({ id: 1, method: "thread/read", params: { threadId: "child" } });
  const snapshot = mock.snapshot(); snapshot.threads.child!.id = "different";
  expect(() => createSa1bMock(() => {}, { snapshot })).toThrow("线程键");
});

test("C 恢复快照结果补读只取所选运行的受管轮，不取线程最新轮", async () => {
  const replies: RpcMessage[] = [];
  const source = createSa1bMock(message => replies.push(message));
  let id = 0;
  const call = (method: string, params: any) => {
    const request = ++id; source.handle({ id: request, method, params }); return replies.find(message => message.id === request)?.result;
  };
  for (const [turnId, output] of [["R1-turn", "R1历史结果"], ["R2-turn", "R2受管结果"], ["R3-turn", "其他运行最新结果"]]) {
    call("fixture/sa1b/turn", { threadId: "child", turnId, action: "start", announce: false });
    call("fixture/sa1b/turn", { threadId: "child", turnId, action: "complete", output, announce: false });
  }
  let response: RpcMessage | undefined;
  const restored = createSa1bMock(message => { response = message; }, { snapshot: source.snapshot(), pageSize: 1 });
  const fake = new FakeRpc();
  fake.handle = (method, params) => {
    response = undefined;
    restored.handle({ id: ++id, method, params });
    if (!response || (response as RpcMessage).error) throw new Error("恢复快照只读结果失败");
    return (response as RpcMessage).result;
  };
  const result = await readThreadResults(fake.rpc, [{ threadId: "child", parentThreadId: "root", relationKind: "subagent",
    activity: "settled", pendingCount: 0, conflict: null, turnIds: ["R2-turn"], shortId: null, stop: null, result: null }],
  { deadline: performance.now() + 4000 });
  expect(result.get("child")).toMatchObject({ turnId: "R2-turn", output: "R2受管结果", availability: "available" });
  expect(fake.calls.filter(call => call.method === "thread/turns/list")).toHaveLength(2);
  expect(fake.calls.filter(call => call.method === "thread/items/list").map(call => call.params.turnId)).toEqual(["R2-turn"]);
});
