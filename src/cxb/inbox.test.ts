import { afterEach, describe, expect, test } from "bun:test";
import { CxbError } from "./contracts";
import { inbox } from "./inbox";
import { cleanupContexts, context, errorCode, item, thread } from "./test-fixtures/n4b-support";

afterEach(cleanupContexts, 5000);

describe("乙组 inbox", () => {
  test("过滤终态、保留过期、按创建时间升序截断并映射短 ID", async () => {
    const c = context(["inbox", "--json", "--limit", "3"]);
    c.fake.handle = (m) => m === "broker/threads/list" ? { threads: [thread()] } : { items: [
      item({ id: "qu_后", createdAt: 3000 }), item({ id: "qu_已答", status: "resolved" }),
      item({ id: "qu_撤销", status: "cancelled" }), item({ id: "qu_过期", status: "expired", createdAt: 500 }),
      item({ id: "qu_处理中", status: "resolving", createdAt: 2000, threadId: "other-thread" }), item({ id: "qu_末", createdAt: 4000 }),
    ] };
    expect(await inbox(c.ctx)).toBe(0);
    expect(c.json[0].items.map((i: any) => i.id)).toEqual(["qu_过期", "qu_处理中", "qu_后"]);
    expect(c.json[0].items.map((i: any) => i.shortId)).toEqual(["abcd1234", "other-th", "abcd1234"]);
    expect(c.json[0].count).toBe(3);
    expect(c.fake.calls[0]).toMatchObject({ params: { includeClosed: true }, options: { timeoutMs: 30000 } });
    c.checkClean();
  });
  test("文本显示已过期、Guardian 续跑提示；类型筛选下发 broker", async () => {
    const c = context(["inbox", "--kind", "guardian"]);
    c.fake.handle = (m) => m === "broker/threads/list" ? { threads: [] } : { items: [
      item({ id: "gd_过期", kind: "guardian", status: "expired" }), item({ id: "gd_待答", kind: "guardian" }), item(),
    ] };
    expect(await inbox(c.ctx)).toBe(0);
    expect(c.text.join("")).toContain("已过期");
    expect(c.text.join("")).toContain("cxb run --resume");
    expect(c.text.join("")).not.toContain("qu_test1234");
    expect(c.fake.calls[0]!.params.kind).toBe("guardian");
    c.checkClean();
  });
  test("JSON、文本和详细输出均脱敏并清洗控制字符", async () => {
    for (const mode of [[], ["--json"]]) {
      const c = context(["inbox", "-v", ...mode]);
      c.fake.handle = (m) => m === "broker/threads/list" ? { threads: [] } : { items: [item({ params: {
        secret: "秘密甲", nested: { Cookie: "秘密乙" },
        questions: [{ id: "q1", question: "\u001b[31m秘密甲 Bearer 隐藏丙", isSecret: true, options: [{ label: "秘密乙" }] }],
      } })] };
      expect(await inbox(c.ctx)).toBe(0);
      const all = JSON.stringify([c.json, c.text, c.progress]);
      for (const secret of ["秘密甲", "秘密乙", "隐藏丙", "\\u001b"]) expect(all).not.toContain(secret);
      if (mode.length) { expect(c.json[0].items[0].sensitive).toBe(true); expect(c.json[0].items[0]).not.toHaveProperty("params"); }
      c.checkClean();
    }
  });
  test("仅 isSecret 也标敏感，普通问题不误标；空列表成功", async () => {
    for (const secret of [true, false]) {
      const c = context(["inbox", "--json"]);
      c.fake.handle = (m) => m === "broker/threads/list" ? { threads: [] } : { items: [item({ params: { questions: [{ id: "q", question: "问题", isSecret: secret }] } })] };
      await inbox(c.ctx);
      expect(c.json[0].items[0].sensitive).toBe(secret);
      c.checkClean();
    }
    const c = context(["inbox", "--json"]);
    c.fake.handle = (m) => m === "broker/threads/list" ? { threads: [] } : { items: [] };
    expect(await inbox(c.ctx)).toBe(0);
    expect(c.json[0]).toMatchObject({ items: [], count: 0 });
    c.checkClean();
  });
  test("没有秘密问题标记时，secret 字段与 Bearer 脱敏也标敏感", async () => {
    for (const params of [{ secret: "秘密字段" }, { message: "Bearer 秘密凭据" }]) {
      const c = context(["inbox", "--json"]);
      c.fake.handle = (m) => m === "broker/threads/list" ? { threads: [] } : { items: [item({ kind: "mcp", params })] };
      await inbox(c.ctx);
      expect(c.json[0].items[0].sensitive).toBe(true);
      c.checkClean();
    }
  });
  test("跨工作区返回 6 且不连接", async () => {
    const c = context(["inbox", "--all"]);
    await errorCode(inbox(c.ctx), 6);
    expect(c.connects).toBe(0);
    c.checkClean();
  });
  test("连接超时和普通连接异常都归为 broker 不可用", async () => {
    for (const error of [new CxbError("TIMEOUT", "连接超时"), new Error("连接失败")]) {
      const c = context(["inbox"]);
      c.ctx.connect = async () => { throw error; };
      await errorCode(inbox(c.ctx), 6);
      c.checkClean();
    }
  });
});
