import { expect, test } from "bun:test";
import { ASK_FEATURE_KEY } from "./contracts";
import { ASK_INSTRUCTIONS, askOverrides } from "./ask-instructions";

test("提问覆盖使用点号键、开发者指令与独立对象", () => {
  expect(askOverrides()).toEqual({ config: { [ASK_FEATURE_KEY]: true }, developerInstructions: ASK_INSTRUCTIONS });
  const changed = askOverrides();
  changed.config![ASK_FEATURE_KEY] = false;
  expect(askOverrides().config![ASK_FEATURE_KEY]).toBe(true);
  for (const text of ["自行确认", "request_user_input", "等待用户回答", "unavailable", "一句文字", "结束本轮", "update_goal", "blocked"]) expect(ASK_INSTRUCTIONS).toContain(text);
  expect(ASK_INSTRUCTIONS).not.toContain("禁止文字提问");
});
