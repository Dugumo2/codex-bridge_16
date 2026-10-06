import { afterEach, describe, expect, test } from "bun:test";
import { PassThrough } from "node:stream";
import { CxbError } from "./contracts";
import { encodeAnswer, readReply, reply } from "./reply";
import { cleanupContexts, context, errorCode, failure, item, thread } from "./test-fixtures/n4b-support";

afterEach(cleanupContexts, 5000);

const encode = (kind: Parameters<typeof item>[0], text: string) => encodeAnswer(item(kind), text, () => {});
describe("乙组 reply 编码", () => {
  test("审批的四种同意词和四种拒绝词，拒绝任意文本", () => {
    for (const word of ["YES", "y", "approve", "accept"]) expect(encode({ kind: "approval" }, word)).toEqual({ decision: "accept" });
    for (const word of ["NO", "n", "decline", "reject"]) expect(encode({ kind: "approval" }, word)).toEqual({ decision: "decline" });
    expect(() => encode({ kind: "approval" }, "随便")).toThrow(CxbError);
  });
  test("提问保留纯文本原文、多题告警、JSON 字符串与数组映射", () => {
    const questions = { questions: [{ id: "q1" }, { id: "q2" }] };
    const warnings: string[] = [];
    expect(encodeAnswer(item({ params: questions }), " 第一行\n第二行\n", (s) => warnings.push(s)))
      .toEqual({ answers: { q1: { answers: [" 第一行\n第二行\n"] } } });
    expect(warnings).toHaveLength(1);
    expect(encode({ params: questions }, '{"q1":"甲","q2":["乙","丙"]}'))
      .toEqual({ answers: { q1: { answers: ["甲"] }, q2: { answers: ["乙", "丙"] } } });
    expect(encode({}, "单题")).toEqual({ answers: { q1: { answers: ["单题"] } } });
  });
  test("未知问题、错误值、空问题和无效 JSON 拒绝", () => {
    for (const text of ['{"q2":"甲"}', '{"q1":2}', '{"q1":["甲",2]}', '{坏', "  "]) expect(() => encode({}, text)).toThrow(CxbError);
    expect(() => encode({ params: { questions: [] } }, "甲")).toThrow(CxbError);
  });
  test("MCP URL 仅接受同意或拒绝", () => {
    const mc = { kind: "mcp" as const, params: { mode: "url" } };
    expect(encode(mc, "yes")).toEqual({ action: "accept", content: null, _meta: null });
    expect(encode(mc, "no")).toEqual({ action: "decline", content: null, _meta: null });
    expect(() => encode(mc, "文字")).toThrow(CxbError);
    expect(() => encode(mc, "{}")).toThrow(CxbError);
  });
  test("MCP 表单校验未知与必填字段，拒绝无内容同意", () => {
    const mc = { kind: "mcp" as const, params: { mode: "form", requestedSchema: { type: "object", properties: { name: { type: "string" } }, required: ["name"] } } };
    expect(encode(mc, '{"name":"甲"}')).toEqual({ action: "accept", content: { name: "甲" }, _meta: null });
    expect(encode(mc, "no")).toEqual({ action: "decline", content: null, _meta: null });
    for (const text of ["yes", "{}", '{"name":"甲","other":1}']) expect(() => encode(mc, text)).toThrow(CxbError);
  });
  test("上游其余 mode 全部拒绝，不能当普通表单", () => {
    for (const mode of ["openai/form", "openaiForm", "openai/userVerification", "future", undefined]) {
      for (const text of ["{}", "yes", "no"]) expect(() => encode({ kind: "mcp", params: { mode } }, text)).toThrow("暂不支持");
    }
  });
  test("Guardian 适配记录后生成 snake_case，拒绝不构建事件", () => {
    const gd = { kind: "guardian" as const, params: { reviewId: "review-1", turnId: "turn-1", targetItemId: "item-1",
      startedAtMs: 10, completedAtMs: 20, decisionSource: "autoReview", review: { status: { type: "denied" }, riskLevel: "high" },
      action: { type: "mcpTool", toolName: "工具", connectorId: "连接", source: "unifiedExec" } } };
    expect(encode(gd, "yes")).toEqual({ override: true, event: { id: "review-1", turn_id: "turn-1", target_item_id: "item-1",
      started_at_ms: 10, completed_at_ms: 20, decision_source: "auto_review", status: "denied", risk_level: "high",
      action: { type: "mcp_tool", tool_name: "工具", connector_id: "连接", source: "unified_exec" } } });
    expect(encode({ kind: "guardian", params: {} }, "no")).toEqual({ override: false });
    expect(() => encode({ kind: "guardian", params: {} }, "yes")).toThrow("缺少有效");
  });
  test("标准输入读 UTF-8 原文，取消后移除监听", async () => {
    const stream = new PassThrough();
    try {
      const work = readReply(stream, new AbortController().signal);
      const bytes = Buffer.from("中文\n原文\n");
      stream.write(bytes.subarray(0, 2)); stream.end(bytes.subarray(2));
      expect(await work).toBe("中文\n原文\n");
      expect(stream.listenerCount("data")).toBe(0);
    } finally { stream.destroy(); }
    const waiting = new PassThrough();
    const controller = new AbortController();
    try { const work = readReply(waiting, controller.signal); controller.abort(); await errorCode(work, 4); expect(waiting.listenerCount("data")).toBe(0); }
    finally { waiting.destroy(); }
  });
});

describe("乙组 reply 命令", () => {
  test("精确查找旧 ID；重复提交成功且只发一次 resolve", async () => {
    const c = context(["reply", "legacy1234", "回答", "--json"]);
    c.fake.handle = (m) => m === "broker/inbox/list" ? { items: [item({ id: "legacy1234", status: "resolved" })] } : { status: "resolved", duplicate: true };
    expect(await reply(c.ctx)).toBe(0);
    expect(c.json[0]).toMatchObject({ accepted: true, duplicate: true, id: "legacy1234" });
    expect(c.fake.calls[1]).toMatchObject({ method: "broker/inbox/resolve", params: { id: "legacy1234", answer: { answers: { q1: { answers: ["回答"] } } } }, options: { timeoutMs: 30000 } });
    c.checkClean();
  });
  test("不存在、仅前缀匹配、ID 类型不符均不发 resolve", async () => {
    for (const entries of [[], [item({ id: "qu_test12345" })], [item({ kind: "approval" })]]) {
      const c = context(["reply", "qu_test1234", "yes"]);
      c.fake.handle = () => ({ items: entries });
      await errorCode(reply(c.ctx), 2);
      expect(c.fake.calls.map((r) => r.method)).toEqual(["broker/inbox/list"]);
      c.checkClean();
    }
  });
  for (const [reason, code] of [["alreadyResolved", 1], ["bad", 1], ["expired", 2], ["cancelled", 2], ["notFound", 2],
    ["purgePending", 8], ["rpcFailed", 7], ["writeFailed", 6], ["capacityFull", 6], ["tooManyWaiters", 6], ["unknown", 6]] as const) {
    test(`线上错误 ${reason} 退出 ${code}`, async () => {
      const c = context(["reply", "qu_test1234", "回答"]);
      c.fake.handle = (m) => { if (m === "broker/inbox/list") return { items: [item()] }; throw failure(reason, reason === "bad" ? -32602 : -32000); };
      await errorCode(reply(c.ctx), code);
      c.checkClean();
    });
  }
  test("Guardian 成功提示下一轮且不提示 wait，拒绝不提示续跑", async () => {
    for (const word of ["yes", "no"]) {
      const c = context(["reply", "gd_test1234", word]);
      c.fake.handle = (m) => m === "broker/inbox/list" ? { items: [item({ id: "gd_test1234", kind: "guardian",
        params: { reviewId: "review-1", review: { status: "denied" }, action: { type: "command" } } })] }
        : m === "broker/threads/list" ? { threads: [thread()] } : { status: "resolved", duplicate: false };
      expect(await reply(c.ctx)).toBe(0);
      expect(c.text[0]).toContain("已回复");
      if (word === "yes") expect(c.notices.join("")).toContain('cxb run --resume abcd1234 "继续"');
      else expect(c.notices).toHaveLength(0);
      expect(c.notices.join("")).not.toContain("cxb wait");
      c.checkClean();
    }
  });
});
