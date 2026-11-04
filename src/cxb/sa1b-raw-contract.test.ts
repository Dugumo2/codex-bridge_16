import { expect, test } from "bun:test";
import { createSa1bMock, type RpcMessage } from "../test-support/sa1b-mock";

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
