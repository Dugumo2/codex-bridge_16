// Tests for rpc.ts — the JSON-RPC endpoint shared by every transport.
import { afterEach, describe, expect, spyOn, test } from "bun:test";
import { createRpcEndpoint, MAX_BUFFER_SIZE, NO_RESPONSE, parseMessage, RpcTimeoutError, type RpcEndpoint, type RpcEndpointOptions } from "./rpc";

const endpoints = new Set<RpcEndpoint>();
afterEach(() => {
  for (const ep of endpoints) ep.markClosed();
  endpoints.clear();
});

function endpoint(overrides: Partial<RpcEndpointOptions> = {}) {
  const writes: Array<Record<string, unknown>> = [];
  const ep = createRpcEndpoint({
    label: "test",
    requestTimeout: 1000,
    send: (data) => { for (const line of data.split("\n")) if (line.trim()) writes.push(JSON.parse(line)); },
    downReason: () => null,
    overflowReason: "overflow",
    onOverflow: () => {},
    writeFailedPrefix: "write failed: ",
    ...overrides,
  });
  endpoints.add(ep);
  return { ep, writes };
}

const tick = () => new Promise((r) => setTimeout(r, 10));

describe("server-sent requests", () => {
  test("an unregistered method is answered method-not-found by default", async () => {
    const { ep, writes } = endpoint();
    ep.feed(JSON.stringify({ id: 1, method: "item/tool/requestUserInput", params: { threadId: "t" } }) + "\n");
    await tick();
    expect(writes).toEqual([{ id: 1, error: { code: -32601, message: "Method not found: item/tool/requestUserInput" } }]);
  });

  test("the catch-all handler decides for unregistered methods, and may stay silent", async () => {
    const { ep, writes } = endpoint();
    const seen: string[] = [];
    ep.onAnyRequest((method, params) => {
      seen.push(method);
      return (params as { threadId: string }).threadId === "mine" ? { ok: true } : NO_RESPONSE;
    });
    ep.feed(JSON.stringify({ id: 1, method: "mcpServer/elicitation/request", params: { threadId: "theirs" } }) + "\n");
    ep.feed(JSON.stringify({ id: 2, method: "item/permissions/requestApproval", params: { threadId: "mine" } }) + "\n");
    await tick();
    expect(seen).toEqual(["mcpServer/elicitation/request", "item/permissions/requestApproval"]);
    expect(writes).toEqual([{ id: 2, result: { ok: true } }]);
  });

  test("a specific handler wins over the catch-all, and NO_RESPONSE from it writes nothing", async () => {
    const { ep, writes } = endpoint();
    ep.onAnyRequest(() => ({ fallback: true }));
    ep.onRequest("item/commandExecution/requestApproval", () => NO_RESPONSE);
    ep.feed(JSON.stringify({ id: 3, method: "item/commandExecution/requestApproval", params: {} }) + "\n");
    await tick();
    expect(writes).toEqual([]);
  });

  test("a request re-sent while its first delivery is still being handled is not handled again", async () => {
    const { ep, writes } = endpoint();
    let handled = 0;
    let answer!: (v: unknown) => void;
    ep.onRequest("item/tool/call", () => { handled++; return new Promise((r) => { answer = r; }); });
    const req = JSON.stringify({ id: "call-7", method: "item/tool/call", params: { threadId: "t" } }) + "\n";
    ep.feed(req);
    await tick();
    ep.feed(req); // the server re-sends its pending request (a rejoin of the thread, say)
    await tick();
    expect(handled).toBe(1);
    answer({ done: true });
    await tick();
    expect(writes).toEqual([{ id: "call-7", result: { done: true } }]);
  });

  test("disposing catch-alls out of order never resurrects a released one", async () => {
    const { ep, writes } = endpoint();
    ep.onAnyRequest(() => ({ guard: true }));
    const disposeB = ep.onAnyRequest(() => ({ b: true }));
    const disposeC = ep.onAnyRequest(() => ({ c: true }));
    disposeB(); // released first, while C still stands
    ep.feed(JSON.stringify({ id: 7, method: "x/y", params: {} }) + "\n");
    await tick();
    disposeC();
    ep.feed(JSON.stringify({ id: 8, method: "x/y", params: {} }) + "\n");
    await tick();
    expect(writes).toEqual([{ id: 7, result: { c: true } }, { id: 8, result: { guard: true } }]);
  });

  test("a handler is told the server's id for the request", async () => {
    const { ep } = endpoint();
    const ids: unknown[] = [];
    ep.onRequest("item/tool/call", (_params, id) => { ids.push(id); return NO_RESPONSE; });
    ep.onAnyRequest((_m, _p, id) => { ids.push(id); return NO_RESPONSE; });
    ep.feed(JSON.stringify({ id: "call-1", method: "item/tool/call", params: {} }) + "\n");
    ep.feed(JSON.stringify({ id: 9, method: "x/y", params: {} }) + "\n");
    await tick();
    expect(ids).toEqual(["call-1", 9]);
  });

  test("disposing a catch-all restores the one that stood before it", async () => {
    const { ep, writes } = endpoint();
    ep.onAnyRequest(() => NO_RESPONSE); // the connection's own guard
    const dispose = ep.onAnyRequest(() => ({ turn: true }));
    ep.feed(JSON.stringify({ id: 5, method: "x/y", params: {} }) + "\n");
    await tick();
    dispose();
    ep.feed(JSON.stringify({ id: 6, method: "x/y", params: {} }) + "\n");
    await tick();
    // The second request meets the guard again, not method-not-found.
    expect(writes).toEqual([{ id: 5, result: { turn: true } }]);
  });

  test("an error carrying a code is forwarded with that code", async () => {
    const { ep, writes } = endpoint();
    ep.onAnyRequest((method) => { const e = new Error(`Method not found: ${method}`) as Error & { code: number }; e.code = -32601; throw e; });
    ep.feed(JSON.stringify({ id: 4, method: "x/y", params: {} }) + "\n");
    await tick();
    expect(writes).toEqual([{ id: 4, error: { code: -32601, message: "Method not found: x/y" } }]);
  });
});

describe("底层逐请求期限校验", () => {
  test.each([NaN, Infinity, -Infinity, 0, -1, 2_147_483_648, Number.MAX_VALUE])("拒绝非法期限 %s，且不发送请求", async (timeoutMs) => {
    const { ep, writes } = endpoint();
    await expect(ep.request("测试请求", {}, { timeoutMs })).rejects.toBeInstanceOf(RangeError);
    expect(writes).toEqual([]);
  });

  test("允许最大期限，回包仍会清除请求定时器", async () => {
    const { ep, writes } = endpoint();
    const pending = ep.request("测试请求", {}, { timeoutMs: 2_147_483_647 });
    ep.feed(JSON.stringify({ id: writes[0].id, result: "完成" }) + "\n");
    expect(await pending).toBe("完成");
  });

  test("请求超时抛 RpcTimeoutError，调用方能和服务器报错区分开，错误文字不变", async () => {
    const { ep, writes } = endpoint();
    const error = await ep.request("测试请求", {}, { timeoutMs: 1 }).catch((reason: unknown) => reason);
    expect(error).toBeInstanceOf(RpcTimeoutError);
    expect((error as Error).message).toBe(`Request 测试请求 (id=${writes[0].id}) timed out after 1ms`);

    const answered = ep.request("报错请求", {}, { timeoutMs: 1000 }).catch((reason: unknown) => reason);
    ep.feed(JSON.stringify({ id: writes[1].id, error: { code: -1, message: "服务器报错" } }) + "\n");
    expect(await answered).not.toBeInstanceOf(RpcTimeoutError);
  });

  test("默认期限也校验，合法的逐请求值可覆盖非法默认值", async () => {
    const { ep, writes } = endpoint({ requestTimeout: NaN });
    await expect(ep.request("默认期限")).rejects.toBeInstanceOf(RangeError);
    const pending = ep.request("覆盖期限", {}, { timeoutMs: 1000 });
    ep.feed(JSON.stringify({ id: writes[0].id, result: "完成" }) + "\n");
    expect(await pending).toBe("完成");
  });
});

describe("可注入 RPC 诊断", () => {
  test.each([
    "不是 JSON", "{}", '{"id":1}', '{"id":1,"error":{}}',
    '{"id":1,"method":"测试","result":{}}', '{"method":"测试","result":{}}',
  ])("解析异常只进入指定回调：%s", (line) => {
    const diagnostics: string[] = [];
    const stderr = spyOn(console, "error").mockImplementation(() => {});
    try {
      const { ep } = endpoint({ log: (message) => diagnostics.push(message) });
      ep.feed(line + "\n");
      expect(diagnostics).toHaveLength(1);
      expect(stderr).not.toHaveBeenCalled();
    } finally { stderr.mockRestore(); }
  });

  test("未传回调时 endpoint 和独立解析均保留 console.error", () => {
    const stderr = spyOn(console, "error").mockImplementation(() => {});
    try {
      endpoint().ep.feed("不是 JSON\n");
      parseMessage("不是 JSON");
      expect(stderr).toHaveBeenCalledTimes(2);
    } finally { stderr.mockRestore(); }
  });

  test("处理器异常、重复注册、写入失败、关闭及溢出均走诊断回调", async () => {
    const diagnostics: string[] = [];
    const log = (line: string) => { diagnostics.push(line); };
    const stderr = spyOn(console, "error").mockImplementation(() => {});
    try {
      const { ep, writes } = endpoint({ log });
      const broken = () => { throw new Error("模拟处理失败"); };
      ep.onAny(broken);
      ep.on("通知", broken);
      ep.onRequest("提问", broken);
      ep.onRequest("提问", broken);
      ep.onClose(broken);
      ep.feed('{"method":"通知"}\n{"id":7,"method":"提问"}\n');
      await tick();
      expect(writes[0]).toMatchObject({ id: 7, error: { code: -32603 } });
      ep.fail("模拟断开");
      const brokenWrite = endpoint({ log, send: broken }).ep;
      await expect(brokenWrite.request("发送")).rejects.toThrow("模拟处理失败");
      endpoint({ log }).ep.feed("x".repeat(MAX_BUFFER_SIZE + 1));
      expect(diagnostics).toHaveLength(7);
      expect(stderr).not.toHaveBeenCalled();
    } finally { stderr.mockRestore(); }
  });

  test("诊断回调抛错也不阻止 pending 拒绝和服务端错误回包", async () => {
    const { ep, writes } = endpoint({ log: () => { throw new Error("模拟输出故障"); } });
    ep.onRequest("提问", () => { throw new Error("模拟处理失败"); });
    ep.feed("不是 JSON\n" + '{"id":"服务端请求","method":"提问"}\n');
    await tick();
    expect(writes[0]).toMatchObject({ id: "服务端请求", error: { code: -32603 } });
    const pending = ep.request("等待");
    ep.onClose(() => { throw new Error("模拟清理失败"); });
    ep.fail("模拟断开");
    await expect(pending).rejects.toThrow("模拟断开");
  });
});
