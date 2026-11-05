import { expect, test } from "bun:test";
import { createSa1bMock, type RpcMessage } from "../test-support/sa1b-mock";

function rawHarness() {
  const sent: RpcMessage[] = [];
  const mock = createSa1bMock(m => sent.push(m));
  let serial = 0;
  const call = (method: string, params: any = {}) => {
    const id = ++serial; mock.handle({ id, method, params });
    const response = sent.find(m => m.id === id);
    expect(response?.error, method).toBeUndefined(); return response?.result;
  };
  call("thread/start", { experimentalRawEvents: true });
  const t: string = call("turn/start", { threadId: "root", input: [] }).turn.id;
  const raw = (type: string, thread: string) => sent.filter(m => m.method === "rawResponseItem/completed" && m.params.item.type === type && m.params.threadId === thread);
  const turns = (thread: string): any[] => call("fixture/sa1b/state").threads[thread].turns;
  return { call, t, raw, turns };
}

test("模拟上游：同一明文每次调用密文不同；不加密的模型正文相同", () => {
  const { call, t, raw } = rawHarness();
  call("fixture/sa1b/spawn", { parentThreadId: "root", parentTurnId: t, childThreadId: "child", version: "v2" });
  for (const _ of [1, 2]) call("fixture/sa1b/reuse", { parentThreadId: "root", parentTurnId: t, childThreadId: "child", message: "同一句话" });
  const bodies = raw("function_call", "root").map(m => JSON.parse(m.params.item.arguments).message);
  expect(bodies[0]).not.toBe(bodies[1]);
  expect(raw("agent_message", "child").map(m => m.params.item.content[1].encrypted_content), "接收方拿到的就是这次调用的密文").toEqual(bodies);
  for (const _ of [1, 2]) call("fixture/sa1b/reuse", { parentThreadId: "root", parentTurnId: t, childThreadId: "child", message: "同一句话", plaintext: true });
  const plain = raw("function_call", "root").slice(2).map(m => JSON.parse(m.params.item.arguments).message);
  expect(plain).toEqual(["同一句话", "同一句话"]);
  const delivered = raw("agent_message", "child").slice(2).map(m => m.params.item.content);
  expect(delivered[0]).toEqual(delivered[1]);
  expect(delivered[0][0].text.endsWith("同一句话")).toBe(true);
});

test("模拟上游：跟进怎么落由子代理状态决定，与探针二一致", () => {
  const { call, t, raw, turns } = rawHarness();
  const spawned = call("fixture/sa1b/spawn", { parentThreadId: "root", parentTurnId: t, childThreadId: "child", version: "v2" }).turnId;
  // 忙且没答完：并入当前轮，不开新轮。
  const absorbed = call("fixture/sa1b/reuse", { parentThreadId: "root", parentTurnId: t, childThreadId: "child", message: "并入" }).turnId;
  expect(absorbed).toBe(spawned);
  expect(turns("child")).toHaveLength(1);
  expect(raw("agent_message", "child").at(-1)?.params.turnId).toBe(spawned);
  // 已答完、当前轮还没结束：排到下一轮，当前轮结束前既不开轮也没有消费记录。
  call("fixture/sa1b/item", { threadId: "child", turnId: spawned, item: { id: "早答", type: "agentMessage", text: "答完了", phase: "final_answer" } });
  const count = raw("agent_message", "child").length;
  call("fixture/sa1b/reuse", { parentThreadId: "root", parentTurnId: t, childThreadId: "child", message: "顺延" });
  expect(turns("child")).toHaveLength(1);
  expect(raw("agent_message", "child")).toHaveLength(count);
  call("fixture/sa1b/turn", { threadId: "child", turnId: spawned, action: "complete", status: "completed" });
  const after = turns("child");
  expect(after, "当前轮结束后自动开下一轮").toHaveLength(2);
  expect(after[1].status).toBe("inProgress");
  expect(raw("agent_message", "child").at(-1)?.params.turnId, "消费记录记在下一轮").toBe(after[1].id);
  // 空闲：开新轮。自动开轮只发生一次。
  call("fixture/sa1b/turn", { threadId: "child", turnId: after[1].id, action: "complete", status: "completed" });
  expect(turns("child")).toHaveLength(2);
  const opened = call("fixture/sa1b/reuse", { parentThreadId: "root", parentTurnId: t, childThreadId: "child", message: "新工作" }).turnId;
  expect(opened).not.toBe(after[1].id);
  expect(turns("child")).toHaveLength(3);
  // 显式指定时序的选项不受影响。
  const before = turns("child").length;
  call("fixture/sa1b/reuse", { parentThreadId: "root", parentTurnId: t, childThreadId: "child", open: false, consumeOnNextTurn: true, message: "拉开时序" });
  expect(turns("child")).toHaveLength(before);
});

test("模拟上游 raw 默认关闭、start打开、子继承、热恢复保留、冷恢复关闭", () => {
  const sent: RpcMessage[] = [];
  const mock = createSa1bMock(m => sent.push(m));
  let serial = 0;
  const call = (method: string, params: any = {}) => {
    const id = ++serial; mock.handle({ id, method, params });
    const response = sent.find(m => m.id === id);
    expect(response, `同步模拟回包缺失:${method}`).toBeDefined();
    expect(response?.error).toBeUndefined(); return response?.result;
  };
  const raw = () => sent.filter(m => m.method === "rawResponseItem/completed");
  call("thread/start", {});
  const t = call("turn/start", { threadId: "root", input: [] }).turn.id;
  call("fixture/sa1b/spawn", { parentThreadId: "root", parentTurnId: t, childThreadId: "cold", version: "v2" });
  call("fixture/sa1b/reuse", { parentThreadId: "root", parentTurnId: t, childThreadId: "cold" });
  expect(raw()).toHaveLength(0);
  call("thread/start", { experimentalRawEvents: true });
  call("thread/start", { experimentalRawEvents: false });
  call("thread/resume", { threadId: "root" });
  expect(call("fixture/sa1b/state").threads.root.experimentalRawEvents).toBe(true);
  call("fixture/sa1b/spawn", { parentThreadId: "root", parentTurnId: t, childThreadId: "child", version: "v2" });
  call("fixture/sa1b/reuse", { parentThreadId: "root", parentTurnId: t, childThreadId: "child", open: false, message: "并入旧轮" });
  expect(raw()).toHaveLength(2);
  expect(raw()[0]?.params.item.type).toBe("function_call");
  expect(raw()[1]?.params.item.type).toBe("agent_message");
  // 与探针记录的真实格式对齐：短名 target、密文正文，工具参数与接收方输入是同一段密文。
  const [tool, input] = [raw()[0]?.params.item, raw()[1]?.params.item];
  const args = JSON.parse(tool.arguments);
  expect(args.target).toBe("agent_child");
  expect(args.message).not.toContain("并入旧轮");
  expect(input.recipient).toBe("/root/agent_child");
  expect(input.content).toEqual([
    { type: "input_text", text: "Message Type: NEW_TASK\nTask name: /root/agent_child\nSender: /root\nPayload:\n" },
    { type: "encrypted_content", encrypted_content: args.message },
  ]);
  const before = raw().length, turns = call("fixture/sa1b/state").threads.child.turns.length;
  call("fixture/sa1b/turn", { threadId: "child", action: "complete", status: "completed" });
  call("fixture/sa1b/reuse", { parentThreadId: "root", parentTurnId: t, childThreadId: "child", tool: "send_message", message: "只入队" });
  expect(call("fixture/sa1b/state").threads.child.turns.length, "send_message 不开新轮").toBe(turns);
  expect(raw().slice(before).map(m => [m.params.item.type, m.params.item.name])).toEqual([["function_call", "send_message"]]);
  call("fixture/sa1b/turn", { threadId: "child", action: "start" });
  const queued = raw().at(-1)?.params.item;
  expect(queued.type, "入队的消息等下一轮才被读到").toBe("agent_message");
  expect(queued.content[0].text.startsWith("Message Type: MESSAGE\n")).toBe(true);
  call("fixture/sa1b/unload", { threadId: "root" });
  call("thread/resume", { threadId: "root" });
  expect(call("fixture/sa1b/state").threads.root.experimentalRawEvents).toBe(false);
  call("fixture/sa1b/spawn", { parentThreadId: "root", parentTurnId: t, childThreadId: "next", version: "v2" });
  const n = raw().length;
  call("fixture/sa1b/reuse", { parentThreadId: "root", parentTurnId: t, childThreadId: "next" });
  expect(raw(), "冷恢复关闭 raw，不能由 mock 始终发送掩盖缺证据").toHaveLength(n);
});
