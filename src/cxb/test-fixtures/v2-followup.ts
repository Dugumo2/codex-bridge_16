import type { Sa1bHarness } from "../../test-support/sa1b-harness";

/** 真实顺序：先收到模型工具调用，再收到发送成功的双 Interacted；不改公共 mock。 */
export async function beginV2Followup(h: Sa1bHarness, child: string, id: string, message: string) {
  await h.control("inject", { message: { method: "rawResponseItem/completed", params: {
    threadId: "root", turnId: h.firstTurn,
    item: { type: "function_call", name: "followup_task", call_id: id, arguments: JSON.stringify({ target: child, message }) },
  } } });
  const item = { id, type: "subAgentActivity", kind: "interacted", agentThreadId: child, agentPath: `/root/${child}` };
  await h.control("inject", { message: { method: "item/started", params: { threadId: "root", turnId: h.firstTurn, item } } });
  await h.control("item", { threadId: "root", turnId: h.firstTurn, item });
}

/** 输入已记录到指定接收轮；仅 send 返回没有这项证据。 */
export async function consumeV2Input(h: Sa1bHarness, child: string, turnId: string, id: string, text: string) {
  await h.control("inject", { message: { method: "rawResponseItem/completed", params: {
    threadId: child, turnId, item: {
      type: "agent_message", id, author: "/root", recipient: `/root/${child}`,
      content: [{ type: "input_text", text: `Message Type: NEW_TASK\nTask name: /root/${child}\nSender: /root\nPayload:\n${text}` }],
    },
  } } });
}
