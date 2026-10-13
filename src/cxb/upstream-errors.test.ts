import { expect, test } from "bun:test";
import { FakeRpc, runResult } from "./test-fixtures/run-fakes";
import { upstreamError } from "./test-fixtures/upstream-errors";
import { diagnosticLine, observeUpstreamErrors, upstreamGroupKey, upstreamNoticeLines, upstreamResultFields, upstreamShowLines } from "./upstream-errors";
import type { UpstreamErrorCategory } from "./contracts";
import { redactParams } from "./server-requests";

const categories: Record<UpstreamErrorCategory, string> = {
  authentication: "上游认证失败", paymentRequired: "上游要求付费", permissionDenied: "上游拒绝访问",
  usageLimit: "上游用量已达上限", rateLimit: "上游限流", server: "上游服务器错误", network: "上游连接失败", unknown: "上游报错",
};
for (const [category, label] of Object.entries(categories)) test(`提示逐字：${category}`, () => {
  expect(upstreamNoticeLines(upstreamError({ category: category as UpstreamErrorCategory }))).toEqual([
    `[cxb] ${label}（HTTP 401），Codex 正在重试；已观察到 3 次。`, "[cxb] 原因：认证已过期",
  ]);
});
test("HTTP、变体名与无括注，true／false／null 及已结束措辞逐字", () => {
  expect(upstreamNoticeLines(upstreamError({ willRetry: false }))[0]).toBe("[cxb] 上游认证失败（HTTP 401），上游已报告终止错误，等待轮次结束确认。");
  expect(upstreamNoticeLines(upstreamError({ httpStatusCode: null, codexErrorInfo: "unauthorized", willRetry: null }))[0]).toBe("[cxb] 上游认证失败（unauthorized）。");
  expect(upstreamNoticeLines(upstreamError({ httpStatusCode: null, codexErrorInfo: { badRequest: {} }, willRetry: null }))[0]).toBe("[cxb] 上游认证失败（badRequest）。");
  expect(upstreamNoticeLines(upstreamError({ httpStatusCode: null, codexErrorInfo: null, willRetry: null }))[0]).toBe("[cxb] 上游认证失败。");
  for (const willRetry of [true, false, null]) expect(upstreamNoticeLines(upstreamError({ willRetry }), true)[0])
    .toBe("[cxb] 最近上游报错：上游认证失败（HTTP 401），共观察到 3 次（运行已结束）。");
});
test("原因再次脱敏：Bearer、裸 key、JWT、查询参数、头部与长串", () => {
  const jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjMifQ.signature";
  const values = ["Bearer very-private", "sk-testPrivateValue", "sk-proj-projectPrivateValue", jwt,
    "https://local/v1?APIKEY=keyPrivate&Auth=authPrivate&signature=sigPrivate&x=保留", "Authorization: authHeader", "Cookie: cookiePrivate", "Set-Cookie: session=privateCookie", "a".repeat(40)];
  const line = diagnosticLine(values.join("\n"));
  for (const secret of ["very-private", "testPrivateValue", "projectPrivateValue", jwt, "keyPrivate", "authPrivate", "sigPrivate", "authHeader", "cookiePrivate", "privateCookie", "a".repeat(40)]) expect(line).not.toContain(secret);
  expect(line).toContain("x=保留");
  expect(line).not.toMatch(/[\r\n]/);
});
test("原因移除控制与 ESC 序列，合并单行，Unicode 超 300 字符截断", () => {
  expect(diagnosticLine("\u001b[31m红字\u001b[0m\u001b]0;秘密标题\u0007\u0000\u0008\n\t下一行"))
    .toBe("红字 下一行");
  expect(diagnosticLine("😀".repeat(301))).toBe("😀".repeat(300) + "…");
  expect(diagnosticLine("中".repeat(300))).toBe("中".repeat(300));
  expect(upstreamNoticeLines(upstreamError({ additionalDetails: "  ", message: "实际原因" }))[1]).toBe("[cxb] 原因：实际原因");
});
test("先去控制序列再脱敏：颜色不能遮住 key、JWT 与敏感键名", () => {
  expect(diagnosticLine("\u001b[31msk-testColorKey\u001b[0m")).toBe("***");
  expect(diagnosticLine("s\u0008k-proj-testControlKey")).toBe("***");
  expect(diagnosticLine("\u001b[31meyJhbGci.aPayload.aSignature\u001b[0m")).toBe("***");
  expect(redactParams({ "to\u0008ken": "短秘密", text: "短秘密" })).toEqual({ token: "***", text: "***" });
});
test("默认 show 两行逐字与已结束／失联不宣称仍在重试", () => {
  expect(upstreamShowLines(runResult({ upstreamError: upstreamError(), state: "running" }))).toEqual([
    "最近上游报错：上游认证失败（HTTP 401）；3 次；首次 1970-01-01T00:00:01.000Z，最近 1970-01-01T00:00:02.000Z；仍在重试", "原因：认证已过期",
  ]);
  for (const state of ["ended", "lost"] as const) expect(upstreamShowLines(runResult({ upstreamError: upstreamError(), state }))[0]).toEndWith("；运行已结束");
  expect(upstreamShowLines(runResult())).toEqual([]);
});
test("分组键逐项一致：只归一整段重连提示，保留其他 n/m 与正文", () => {
  const error = upstreamError({ additionalDetails: null });
  expect(upstreamGroupKey(error)).toBe(upstreamGroupKey({ ...error, message: "  Reconnecting... 5/5  " }));
  const key = upstreamGroupKey(error);
  for (const patch of [{ threadId: "另线程" }, { turnId: "另轮" }, { category: "unknown" as const }, { httpStatusCode: 403 },
    { codexErrorInfo: "unauthorized" }, { additionalDetails: "payload 1/5" }, { message: "Reconnecting...  1/5" }, { message: "Reconnecting... 1/5 failed" }]) expect(upstreamGroupKey({ ...error, ...patch })).not.toBe(key);
  expect(upstreamGroupKey({ ...error, additionalDetails: "实际 1/5", message: "忽略" })).not.toBe(upstreamGroupKey({ ...error, additionalDetails: "实际 2/5" }));
  // A 路显式采用 JS trim 的空白集合：移除 BOM，但保留 U+0085，跨语言分组才一致。
  expect(upstreamGroupKey({ ...error, additionalDetails: "\uFEFF正文\uFEFF" })).toBe(upstreamGroupKey({ ...error, additionalDetails: "正文" }));
  expect(upstreamGroupKey({ ...error, additionalDetails: "\u0085正文\u0085" })).not.toBe(upstreamGroupKey({ ...error, additionalDetails: "正文" }));
});

function observer(initial: string | null = "run-test") {
  const fake = new FakeRpc();
  let id = initial, time = 0;
  const lines: string[] = [];
  const observed = observeUpstreamErrors(fake.rpc, () => id, line => lines.push(line), () => time);
  const emit = (revision: number, error = upstreamError(), runId = "run-test") => fake.emit("broker/run/updated", {
    runId, threadId: "thread-test", diagnosticRevision: revision, upstreamError: error, lastProgressAt: null, state: "running", status: null,
  });
  return { fake, observed, lines, emit, setId(value: string) { id = value; observed.refresh(); }, clock(value: number) { time = value; },
    dispose() { observed.dispose(); expect(fake.subscriptions).toBe(0); expect(fake.notificationErrors).toEqual([]); } };
}
test("同组 30 秒限频：29 秒静默、30 秒带最新次数，重连计数提示不另分组", () => {
  const o = observer();
  try {
    o.emit(1); o.clock(29_000); o.emit(2, upstreamError({ count: 4, message: "Reconnecting... 2/5" }));
    expect(o.lines).toHaveLength(2);
    o.clock(30_000); o.emit(3, upstreamError({ count: 5 }));
    expect(o.lines).toHaveLength(4);
    expect(o.lines[2]).toBe("[cxb] 上游认证失败（HTTP 401），Codex 正在重试；已观察到 5 次。");
  } finally { o.dispose(); }
});
test("新组、正文变化与 willRetry 变化立即输出", () => {
  const o = observer();
  try {
    o.emit(1); o.emit(2, upstreamError({ willRetry: false })); o.emit(3, upstreamError({ willRetry: null }));
    o.emit(4, upstreamError({ additionalDetails: "另一个原因" })); o.emit(5, upstreamError({ category: "unknown" }));
    expect(o.lines).toHaveLength(10);
    expect(o.lines[2]).toContain("上游已报告终止错误，等待轮次结束确认");
  } finally { o.dispose(); }
});
test("相同与较小 diagnosticRevision 即使跨过限频也不重复", () => {
  const o = observer();
  try {
    o.emit(5); o.clock(30_000); o.emit(5, upstreamError({ count: 100 })); o.emit(4, upstreamError({ additionalDetails: "倒退" }));
    expect(o.lines).toHaveLength(2);
    expect(o.observed.fields().upstreamError?.count).toBe(3);
  } finally { o.dispose(); }
});
test("runId 确认前最多缓冲 64 条，确定后只回放本运行", () => {
  const o = observer(null);
  try {
    for (let i = 1; i <= 65; i++) o.emit(i, upstreamError({ additionalDetails: `原因${i}` }));
    o.emit(99, upstreamError({ additionalDetails: "别的运行" }), "run-other");
    expect(o.lines).toEqual([]);
    o.setId("run-test");
    expect(o.lines).toHaveLength(126);
    expect(o.lines[1]).toBe("[cxb] 原因：原因3");
    expect(o.lines.join()).not.toContain("别的运行");
    o.emit(100, upstreamError(), "run-other"); expect(o.lines).toHaveLength(126);
  } finally { o.dispose(); }
});
test("先订阅后快照，迟到旧快照不覆盖新通知与最终失败摘要", async () => {
  const o = observer();
  try {
    const terminal = runResult({ status: "failed", diagnosticRevision: 1, upstreamError: upstreamError({ count: 1 }),
      turns: [{ ...runResult().turns[0]!, status: "failed", error: { message: "最终失败", codexErrorInfo: "unauthorized" } }] });
    o.fake.handle = () => { o.emit(2, upstreamError({ count: 9 })); return { run: terminal }; };
    const snapshot = await o.observed.rpc.request<{ run: typeof terminal }>("broker/threads/watch");
    expect(snapshot.run.upstreamError?.count).toBe(9);
    expect(snapshot.run.diagnosticRevision).toBe(2);
    expect(snapshot.run.state).toBe("ended");
    o.observed.failure(snapshot.run); o.observed.failure(snapshot.run);
    expect(o.lines.filter(line => line.startsWith("[cxb] 失败原因"))).toEqual(["[cxb] 失败原因：最终失败（unauthorized）"]);
    expect(o.lines.filter(line => line.includes("运行已结束"))).toHaveLength(1);
  } finally { o.dispose(); }
});
test("失败没有最终轮 error 时只称最近上游报错，历史错误与终态同 revision 仍收束", () => {
  const o = observer();
  try {
    o.emit(1);
    const run = o.observed.apply(runResult({ status: "failed", diagnosticRevision: 1, upstreamError: upstreamError() }));
    o.observed.failure(run); o.observed.failure(run);
    expect(o.lines.filter(line => line.includes("运行已结束"))).toHaveLength(1);
    expect(o.lines.join()).not.toContain("失败原因");
  } finally { o.dispose(); }
});
test("最终 JSON 只增实际存在字段，SA1b 最终轮错误不能取数组尾项", () => {
  const base = runResult();
  const stage = { ...base.turns[0]!, turnId: "stage", error: { message: "阶段失败" } };
  const final = { ...stage, turnId: "final", error: { message: "Bearer final-secret", codexErrorInfo: "unauthorized" } };
  expect(upstreamResultFields(base)).toEqual({});
  expect(upstreamResultFields({ ...base, diagnosticRevision: 10, lastProgressAt: 1234 })).toEqual({});
  expect(upstreamResultFields({ ...base, finalTurnId: "final", turns: [final, stage] })).toEqual({ error: { message: "Bearer ***", codexErrorInfo: "unauthorized" } });
  expect(upstreamResultFields({ ...base, finalTurnId: "missing", turns: [stage] })).toEqual({});
  expect(upstreamResultFields({ ...base, finalTurnId: "final", turns: [stage, { ...final, error: null }] })).toEqual({});
});

test("runId 未确认的终态快照也必须缓冲，销毁后保持静默", async () => {
  const o = observer(null);
  o.fake.handle = () => ({ run: runResult({ upstreamError: upstreamError(), diagnosticRevision: 1 }) });
  await o.observed.rpc.request("broker/threads/watch");
  expect(o.lines).toEqual([]);
  o.setId("run-test");
  expect(o.lines[0]).toBe("[cxb] 最近上游报错：上游认证失败（HTTP 401），共观察到 3 次（运行已结束）。");
  o.dispose();
  const before = [...o.lines];
  o.emit(2); o.observed.apply(runResult({ upstreamError: upstreamError(), diagnosticRevision: 3 }));
  expect(o.lines).toEqual(before);
});
