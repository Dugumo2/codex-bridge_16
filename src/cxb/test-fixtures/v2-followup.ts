import type { Sa1bHarness } from "../../test-support/sa1b-harness";

/** 消费模型工具调用和输入回执均由模拟上游按线程 raw 开关生成。 */
export function followup(h: Sa1bHarness, child: string, options: { open?: boolean; consumeOnNextTurn?: boolean; message?: string; tool?: "followup_task" | "send_message" } = {}) {
  return h.control("reuse", { parentThreadId: "root", parentTurnId: h.firstTurn, childThreadId: child, ...options });
}
