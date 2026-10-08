import { afterEach, beforeEach, expect, test } from "bun:test";
import { createRpcEndpoint, NO_RESPONSE, type RpcEndpoint } from "../rpc";
import { RpcError } from "../types";
import { CxbError, type ApprovalPolicyName, type BrokerRpc, type HandoverResult, type InboxItem, type RunResult } from "./contracts";
import { attachServerRequests, decidePolicy, handover, redactParams, renderPending } from "./server-requests";

let rpc: RpcEndpoint;
let messages: Array<{ id?: number; result?: unknown; error?: { code: number } }>;
let disposers: Array<() => void>;
let incoming: number;

beforeEach(() => {
  messages = [];
  disposers = [];
  incoming = 0;
  rpc = createRpcEndpoint({ label: "服务端请求测试", requestTimeout: 500, send: (line) => messages.push(JSON.parse(line)),
    downReason: () => null, overflowReason: "缓冲区超限", onOverflow() {}, writeFailedPrefix: "写入失败", log() {} });
});

afterEach(() => {
  const errors: unknown[] = [];
  for (const dispose of disposers.reverse()) { try { dispose(); } catch (error) { errors.push(error); } }
  try { rpc.markClosed(); } catch (error) { errors.push(error); }
  if (errors.length) throw new AggregateError(errors, "清理服务端请求测试失败");
});

/** 用同一 RPC 的探针请求等前面的处理微任务排空，不依赖墙钟延迟。 */
async function drain(): Promise<void> {
  let finish!: () => void;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const done = new Promise<void>((resolve) => { finish = resolve; });
  const off = rpc.onRequest("测试/屏障", () => { finish(); return NO_RESPONSE; });
  disposers.push(off);
  rpc.feed(JSON.stringify({ id: ++incoming, method: "测试/屏障" }) + "\n");
  try {
    await Promise.race([done, new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error("RPC 屏障超时")), 1000); })]);
    // 屏障 handler 之后还有 finally 与写回链，微任务屏障覆盖完整分派。
    for (let i = 0; i < 8; i++) await Promise.resolve();
  } finally { clearTimeout(timer); off(); }
}

function request(method: string, params: unknown): number {
  const id = ++incoming;
  rpc.feed(JSON.stringify({ id, method, params }) + "\n");
  return id;
}

const policies: ApprovalPolicyName[] = ["never", "auto", "on-request", "on-failure", "untrusted"];
const methods = ["item/commandExecution/requestApproval", "item/fileChange/requestApproval", "item/permissions/requestApproval",
  "item/tool/requestUserInput", "mcpServer/elicitation/request", "item/tool/call", "item/未来请求"];

for (const policy of policies) {
  for (const method of methods) {
    test(`路由：${policy} 的 ${method}`, async () => {
      const needs: string[] = [];
      disposers.push(attachServerRequests(rpc, { threadId: "线程", policy, onNeedsHuman: (value) => needs.push(value) }));
      const params = { threadId: "线程", isBlocking: false, permissions: { fileSystem: { write: ["D:/工作区"] } } };
      const id = request(method, params);
      await drain();
      const answer = messages.find((value) => value.id === id);
      if (method === "item/tool/call") {
        expect(answer?.error?.code).toBe(-32601);
        expect(needs).toEqual([]);
      } else if (policy === "never" && !["item/tool/requestUserInput", "item/未来请求"].includes(method)) {
        const result = method === "item/permissions/requestApproval" ? { scope: "session", permissions: params.permissions }
          : method === "mcpServer/elicitation/request" ? { action: "accept", content: {} } : { decision: "accept" };
        expect(answer).toEqual({ id, result });
        expect(needs).toEqual([]);
      } else {
        expect(answer).toBeUndefined();
        expect(needs).toEqual([method]);
      }
    });
  }
}

test("连续需人答请求只触发一次回调且全部不回包", async () => {
  const needs: string[] = [];
  disposers.push(attachServerRequests(rpc, { threadId: "线程", policy: "auto", onNeedsHuman: (method) => needs.push(method) }));
  for (const method of methods.filter((method) => method !== "item/tool/call")) request(method, { threadId: "线程", isBlocking: false });
  await drain();
  expect(needs).toEqual([methods[0]!]);
  expect(messages).toEqual([]);
});

test("其他线程和缺失归属静默忽略，日志不回显秘密", async () => {
  const logs: string[] = [];
  let needs = 0;
  disposers.push(attachServerRequests(rpc, { threadId: "线程", policy: "never", onNeedsHuman: () => { needs++; }, log: (line) => logs.push(line) }));
  for (const params of [{ threadId: "其他线程", token: "保密值" }, {}, null, []]) request("秘密方法\u001b\r", params);
  await drain();
  expect(messages).toEqual([]);
  expect(needs).toBe(0);
  expect(logs).toHaveLength(4);
  expect(logs.join("\n")).not.toMatch(/保密值|秘密方法|\u001b|\r/);
});

test("撤销幂等，排队中的请求不回答，之后恢复原处理器", async () => {
  let fallback = 0;
  disposers.push(rpc.onAnyRequest(() => { fallback++; return NO_RESPONSE; }));
  let needs = 0;
  const off = attachServerRequests(rpc, { threadId: "线程", policy: "never", onNeedsHuman: () => { needs++; } });
  disposers.push(off);
  request("item/commandExecution/requestApproval", { threadId: "线程" });
  off(); off();
  await drain();
  request("item/tool/requestUserInput", { threadId: "线程" });
  await drain();
  expect(needs).toBe(0);
  expect(messages).toEqual([]);
  expect(fallback).toBe(1);
});

test("日志和人工回调抛错也不能误答提问", async () => {
  let needs = 0;
  disposers.push(attachServerRequests(rpc, { threadId: "线程", policy: "never", log: () => { throw new Error("日志错误"); }, onNeedsHuman: () => { needs++; throw new Error("回调错误"); } }));
  request("item/tool/requestUserInput", { threadId: "线程" });
  request("item/tool/requestUserInput", { threadId: "线程" });
  await drain();
  expect(messages).toEqual([]);
  expect(needs).toBe(1);
});

test("权限缺省与空值都授予空对象，纯函数不修改参数", () => {
  for (const params of [undefined, null, {}, { permissions: null }]) {
    expect(decidePolicy("never", "item/permissions/requestApproval", params)).toEqual({ kind: "answer", result: { scope: "session", permissions: {} } });
  }
  const params = Object.freeze({ questions: Object.freeze([{ id: "问题", options: [{ label: "甲" }] }]) });
  expect(decidePolicy("never", "item/tool/requestUserInput", params)).toEqual({ kind: "inbox" });
});

const args = { threadId: "线程", runId: "运行", policy: "never" as const, approvalsReviewer: "guardian" };
function fakeRequest(handler: (method: string, params: unknown, opts?: { timeoutMs?: number }) => Promise<unknown>): BrokerRpc {
  return { ...rpc, request: handler as BrokerRpc["request"] };
}

test("交接：连接断开后立即报告结果未知，不空转重试到期限", async () => {
  let closeHandler: (() => void) | undefined;
  let calls = 0;
  const transport = fakeRequest(async () => {
    calls++;
    closeHandler?.();
    throw new CxbError("BROKER_UNAVAILABLE", "连接断开");
  });
  transport.onClose = (handler) => { closeHandler = handler; return () => { closeHandler = undefined; }; };
  const result = await handover(transport, args, { timeoutMs: 10_000 }).then(() => null, (error: unknown) => error);
  expect(result).toMatchObject({ code: "BROKER_UNAVAILABLE", retryable: true });
  expect(calls).toBe(1);
});

test("交接：上一次 detach 仍在排队（detachPending）时在期限内重试，最终拿到结果", async () => {
  const result = { outcome: "alreadyDetached" as const, turnId: "轮次", pending: [] };
  let calls = 0;
  const transport = fakeRequest(async () => {
    if (++calls === 1) {
      throw new CxbError("BROKER_UNAVAILABLE", "远端错误", {
        cause: new RpcError("JSON-RPC error -32000: 扩展请求未完成：detachPending", -32000, "扩展请求未完成", { reason: "detachPending" }),
      });
    }
    return result;
  });
  expect(await handover(transport, args, { timeoutMs: 5_000 })).toBe(result);
  expect(calls).toBe(2);
});

for (const [reason, code] of [["notOwner", "THREAD_HELD"], ["joined", "THREAD_HELD"], ["unknownRun", "NOT_FOUND"], ["runMismatch", "NOT_FOUND"]] as const) {
  for (const structured of [true, false]) {
    test(`交接错误映射：${reason}，${structured ? "结构化原因" : "RpcError 携带 data"}`, async () => {
      const cause = structured ? Object.assign(new Error("拒绝"), { rpcCode: -32000, data: { reason } })
        : new RpcError(`JSON-RPC error -32000: 拒绝 ({"reason":"${reason}","token":"保密值"})`, -32000, "拒绝",
          { reason, token: "保密值" });
      let calls = 0;
      const transport = fakeRequest(async () => { calls++; throw new CxbError("BROKER_UNAVAILABLE", "远端错误", { cause }); });
      let result: unknown;
      try { await handover(transport, args, { timeoutMs: 1000 }); } catch (error) { result = error; }
      expect(result).toBeInstanceOf(CxbError);
      expect(result).toMatchObject({ code, exitCode: code === "THREAD_HELD" ? 8 : 2, id: "线程" });
      expect((result as Error).message).not.toContain("保密值");
      expect(calls).toBe(1);
    });
  }
}

for (const failure of ["超时"] as const) {
  test(`交接${failure}后原样重试，alreadyDetached 成功`, async () => {
    const sent: unknown[] = [];
    const timeouts: number[] = [];
    const result: HandoverResult = { outcome: "alreadyDetached", turnId: "轮次", pending: [] };
    const changed = { ...args };
    const transport = fakeRequest(async (method, params, opts) => {
      sent.push({ method, params: structuredClone(params) });
      timeouts.push(opts!.timeoutMs!);
      if (sent.length === 1) {
        changed.runId = "另一次运行";
        throw new CxbError("BROKER_UNAVAILABLE", failure);
      }
      return result;
    });
    expect(await handover(transport, changed, { timeoutMs: 1000 })).toBe(result);
    expect(sent).toEqual([{ method: "broker/threads/detach", params: args }, { method: "broker/threads/detach", params: args }]);
    expect(timeouts.every((value) => value > 0 && value < 1000)).toBe(true);
  });
}

test("交接 runEnded 原样透传尚未补全的结果", async () => {
  const result: HandoverResult = { outcome: "runEnded", turnId: null, pending: [], result: { runId: "运行", turns: [{ ready: "pending" }] } as RunResult };
  expect(await handover(fakeRequest(async () => result), args)).toBe(result);
});

test("真实 RPC 首次丢包超时后重新发送，迟到旧回包不覆盖 alreadyDetached", async () => {
  rpc.markClosed();
  const sent: Array<{ id: number; method: string; params: unknown }> = [];
  rpc = createRpcEndpoint({ label: "丢包测试", requestTimeout: 1000, downReason: () => null,
    overflowReason: "超限", onOverflow() {}, writeFailedPrefix: "写入失败", log() {},
    send: (line) => {
      const message = JSON.parse(line);
      sent.push(message);
      if (sent.length === 2) queueMicrotask(() => {
        rpc.feed(JSON.stringify({ id: sent[0]!.id, result: { outcome: "detached", pending: [], turnId: "旧轮次" } }) + "\n");
        rpc.feed(JSON.stringify({ id: message.id, result: { outcome: "alreadyDetached", pending: [], turnId: "轮次" } }) + "\n");
      });
    } });
  expect(await handover(rpc, args, { timeoutMs: 1500 })).toEqual({ outcome: "alreadyDetached", pending: [], turnId: "轮次" });
  expect(sent).toHaveLength(2);
  expect(sent.map(({ method, params }) => ({ method, params }))).toEqual([
    { method: "broker/threads/detach", params: args }, { method: "broker/threads/detach", params: args },
  ]);
});

test("交接 detached 原样透传待答快照", async () => {
  const result: HandoverResult = { outcome: "detached", turnId: "轮次", pending: [item("question", {})] };
  expect(await handover(fakeRequest(async () => result), args)).toBe(result);
});

test("未知远端错误不重试，也不按消息中的原因词误映射", async () => {
  let calls = 0;
  const transport = fakeRequest(async () => { calls++; throw new RpcError("notOwner token=保密值", -32602, "notOwner"); });
  await expect(handover(transport, args)).rejects.toMatchObject({ code: "BROKER_UNAVAILABLE", message: "broker 拒绝交接请求" });
  expect(calls).toBe(1);
});

test("总期限拒绝永久无响应并撤销订阅", async () => {
  let subscriptions = 0;
  const transport = fakeRequest(() => new Promise(() => {}));
  transport.onClose = () => { subscriptions++; return () => { subscriptions--; }; };
  await expect(handover(transport, args, { timeoutMs: 25 })).rejects.toMatchObject({ code: "TIMEOUT" });
  expect(subscriptions).toBe(0);
});

test("永久断开在期限内收束为 broker 不可用", async () => {
  const transport = fakeRequest(async () => { rpc.fail("断开"); throw new CxbError("BROKER_UNAVAILABLE", "断开"); });
  await expect(handover(transport, args, { timeoutMs: 25 })).rejects.toMatchObject({ code: "BROKER_UNAVAILABLE", retryable: true });
});

test("取消进行中的交接立即拒绝，预先取消不发请求", async () => {
  const controller = new AbortController();
  let calls = 0;
  const transport = fakeRequest(async () => { calls++; controller.abort(); return new Promise(() => {}); });
  await expect(handover(transport, args, { signal: controller.signal, timeoutMs: 1000 })).rejects.toMatchObject({ code: "INTERRUPTED" });
  await expect(handover(transport, args, { signal: controller.signal, timeoutMs: 1000 })).rejects.toMatchObject({ code: "INTERRUPTED" });
  expect(calls).toBe(1);
});

test("非法交接期限不发请求", async () => {
  const transport = fakeRequest(async () => { throw new Error("不应调用"); });
  for (const timeoutMs of [0, -1, NaN, Infinity, 2_147_483_648]) {
    await expect(handover(transport, args, { timeoutMs })).rejects.toMatchObject({ code: "USAGE" });
  }
});

function item(kind: InboxItem["kind"], params: unknown): InboxItem {
  const prefixes = { approval: "ap_", question: "qu_", mcp: "mc_", guardian: "gd_" };
  return { id: prefixes[kind] + "123", kind, threadId: "线程", runId: "运行", turnId: "轮次", method: "请求", params, createdAt: 1, expiresAt: 2, status: "pending" };
}

test("四类摘要、回答方式、问题截断与最终等待命令", () => {
  const items = [item("approval", { command: "列出文件" }), item("approval", { changes: { "src/文件.ts": {} } }),
    item("question", { questions: [{ question: "选".repeat(200), options: [{ label: "甲" }, { label: "乙" }] }, { question: "名字？" }] }),
    item("mcp", { serverName: "搜索", mode: "url", url: "https://example.test/path" }),
    item("mcp", { serverName: "表单", mode: "form", requestedSchema: { properties: { 城市: {}, 日期: {} } } }),
    item("guardian", { action: { type: "command", command: "发布" } })];
  const output = renderPending(items, { threadId: "线程", shortId: "abcd1234" });
  expect(output.text.split("\n")).toHaveLength(7);
  for (const text of ["审批：列出文件", "src/文件.ts", "…", "甲 / 乙", "名字？", "搜索 https://example.test/path", "表单字段：城市、日期", "Guardian 拒绝：命令： 发布", "cxb run --resume abcd1234", "cxb reply qu_123", "cxb reply mc_123 -"]) expect(output.text).toContain(text);
  expect(output.text).not.toContain("选".repeat(200));
  expect(output.text.split("\n").at(-1)).toBe("cxb wait abcd1234");
  expect(output.json).toEqual({ ok: false, status: "waiting", threadId: "线程", pending: items });
});

test("文本与 JSON 递归脱敏，不修改原始事项，不保留控制字符", () => {
  const secrets = ["令牌原值", "饼干原值", "嵌套原值", "密码原值", "接口原值", "授权原值"];
  const params = { ToKeN: secrets[0], Cookie: secrets[1], nested: [{ client_SECRET: secrets[2], Password: secrets[3], apiKey: secrets[4], Authorization: secrets[5] }],
    command: secrets.join(" ") + "\u001b\r\n危险", questions: [{ question: secrets.join(" "), options: [{ label: secrets[2] }] }],
    serverName: "服务器", mode: "url", url: "https://example.test/?token=网址原值", action: { type: "command", command: secrets[0] } };
  const items = (["approval", "question", "mcp", "guardian"] as const).map((kind) => item(kind, params));
  const before = JSON.stringify(items);
  const output = renderPending(items, { threadId: "线程", shortId: "abcd1234" });
  for (const text of [output.text, JSON.stringify(output.json)]) {
    for (const secret of [...secrets, "网址原值"]) expect(text).not.toContain(secret);
    expect(text).toContain("***");
    expect(text).not.toMatch(/\u001b|\r/);
  }
  expect(output.text.split("\n")).toHaveLength(5);
  expect(JSON.stringify(output.json)).not.toContain("\\u001b");
  expect(JSON.stringify(items)).toBe(before);
  expect(redactParams(params)).toMatchObject({ ToKeN: "***", Cookie: "***", nested: [{ client_SECRET: "***", Password: "***", apiKey: "***", Authorization: "***" }] });
});

test("畸形或空详情仍给出可用摘要，Guardian 数组畸形不抛错", () => {
  const items = [item("approval", null), item("question", { questions: [null] }), item("mcp", null), item("guardian", { action: { type: "execve", argv: 3 } })];
  expect(renderPending(items, { threadId: "线程", shortId: "abcd1234" }).text).toContain("动作详情不可用");
  expect(renderPending([], { threadId: "线程", shortId: "abcd1234" })).toEqual({ text: "cxb wait abcd1234", json: { ok: false, status: "waiting", threadId: "线程", pending: [] } });
});

test("自由文本中的授权头和 URL 用户口令也被脱敏", () => {
  const params = { command: "Authorization: Bearer 头部原值 https://用户名:网址口令@example.test/path?api_key=查询原值" };
  const output = renderPending([item("approval", params)], { threadId: "线程", shortId: "abcd1234" });
  for (const text of [output.text, JSON.stringify(output.json)]) {
    for (const secret of ["头部原值", "网址口令", "用户名", "查询原值"]) expect(text).not.toContain(secret);
  }
});

test("文件审批兼容实际 grantRoot 和缺路径请求，条目数组只展示路径", () => {
  const make = (params: unknown): InboxItem => ({ ...item("approval", params), method: "item/fileChange/requestApproval" });
  const output = renderPending([
    make({ grantRoot: "D:/工作区", reason: "允许修改" }),
    make({ grantRoot: null, reason: "需要额外写权限" }),
    make({ changes: [{ path: "src/文件.ts", diff: "大段补丁正文" }] }),
  ], { threadId: "线程", shortId: "abcd1234" });
  expect(output.text).toContain("审批：D:/工作区");
  expect(output.text).toContain("审批：文件修改（需要额外写权限）");
  expect(output.text).toContain("src/文件.ts");
  expect(output.text).not.toContain("大段补丁正文");
});

// SA1 接口第 5 节：路由器保存逻辑根与执行线程，按 cxbOrigin 接受本次运行的子线程请求。
const origin = (patch: Record<string, unknown> = {}) => ({ rootThreadId: "根线程", runId: "run-1", parentThreadId: "根线程",
  relationKind: "subagent", sourceThreadId: "子线程", ...patch });

test("子线程请求：cxbOrigin 的逻辑根与运行一致才接受，按策略回答", async () => {
  let runId: string | undefined;
  const needs: string[] = [];
  disposers.push(attachServerRequests(rpc, { threadId: "根线程", logicalRootThreadId: "根线程", getRunId: () => runId,
    policy: "never", onNeedsHuman: (method) => needs.push(method) }));
  // 早于启动回包（runId 未知）到达的子请求照收。
  const early = request("item/commandExecution/requestApproval", { threadId: "子线程", cxbOrigin: origin() });
  await drain();
  expect(messages.find((m) => m.id === early)?.result).toEqual({ decision: "accept" });
  runId = "run-1";
  const accepted = request("item/fileChange/requestApproval", { threadId: "子线程", cxbOrigin: origin() });
  // 别的运行、别的逻辑根、来源线程不符、没有 cxbOrigin：都不答。
  const otherRun = request("item/commandExecution/requestApproval", { threadId: "子线程", cxbOrigin: origin({ runId: "run-2" }) });
  const otherRoot = request("item/commandExecution/requestApproval", { threadId: "子线程", cxbOrigin: origin({ rootThreadId: "别的根" }) });
  const mismatch = request("item/commandExecution/requestApproval", { threadId: "别的线程", cxbOrigin: origin() });
  const bare = request("item/commandExecution/requestApproval", { threadId: "子线程" });
  await drain();
  expect(messages.find((m) => m.id === accepted)?.result).toEqual({ decision: "accept" });
  for (const id of [otherRun, otherRoot, mismatch, bare]) expect(messages.find((m) => m.id === id)).toBeUndefined();
  // 需要人答的子请求同样触发交接。
  request("item/tool/requestUserInput", { threadId: "子线程", cxbOrigin: origin() });
  await drain();
  expect(needs).toEqual(["item/tool/requestUserInput"]);
});

test("review 迁移只更新执行线程：逻辑根不变，孙代理的请求仍能到达", async () => {
  // run.ts 在迁移时以新的执行线程重新登记，逻辑根仍是原线程。
  disposers.push(attachServerRequests(rpc, { threadId: "审查线程", logicalRootThreadId: "根线程", getRunId: () => "run-1",
    policy: "never", onNeedsHuman() {} }));
  const own = request("item/commandExecution/requestApproval", { threadId: "审查线程" });
  const grandchild = request("item/commandExecution/requestApproval", { threadId: "孙线程",
    cxbOrigin: origin({ parentThreadId: "审查线程", sourceThreadId: "孙线程" }) });
  const oldRoot = request("item/commandExecution/requestApproval", { threadId: "根线程" });
  await drain();
  expect(messages.find((m) => m.id === own)?.result).toEqual({ decision: "accept" });
  expect(messages.find((m) => m.id === grandchild)?.result).toEqual({ decision: "accept" });
  expect(messages.find((m) => m.id === oldRoot)).toBeUndefined();
});
