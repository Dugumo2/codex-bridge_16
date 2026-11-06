import { describe, expect, test } from "bun:test";
import { createStatusHistory, type StatusHistoryTurn } from "../test-support/status-history";
import { readThreadResults } from "./thread-result-reader";
import type { TreeNode } from "./tree-contracts";
import { FakeRpc } from "./test-fixtures/run-fakes";
import { deferred, messageBarrier, watchdog } from "./test-fixtures/test-watchdog";

function node(threadId = "child", turnIds = ["bound"]): TreeNode {
  return { threadId, parentThreadId: "root", relationKind: "subagent", activity: "settled", pendingCount: 0,
    conflict: null, turnIds, shortId: null, stop: null, result: null };
}
function turn(id: string, text: string, status = "completed"): StatusHistoryTurn {
  return { id, status, items: [{ type: "agentMessage", phase: "final_answer", text }] };
}
function fixture(turns: StatusHistoryTurn[], mode: "paged" | "legacy" | "unsupported" = "paged") {
  const fake = new FakeRpc();
  let reply: any;
  const history = createStatusHistory({ strictPagination: true, longHistory: true, historyMode: mode }, (message) => { reply = message; }, () => turns);
  fake.handle = (method, params) => {
    reply = undefined;
    if (!history.handle({ id: 1, method, params })) throw new Error(`未预期的写操作：${method}`);
    if (reply.error) throw new Error(reply.error.message);
    return reply.result;
  };
  return fake;
}
async function read(fake: FakeRpc, nodes = [node()], deadline = performance.now() + 10_000, signal?: AbortSignal) {
  return watchdog(readThreadResults(fake.rpc, nodes, { deadline, signal }), 8000, "子结果补读未结束");
}

describe("本运行绑定轮次归属", () => {
  test("跨运行复用：服务器最新轮不属于本运行时跳过它，绝不按线程最新结果冒充", async () => {
    const fake = fixture([turn("bound", "本运行结果"), turn("new-run", "其他运行结果", "failed")]);
    const result = (await read(fake)).get("child")!;
    expect(result).toMatchObject({ turnId: "bound", status: "completed", availability: "available", output: "本运行结果" });
    expect(fake.calls.filter((c) => c.method === "thread/items/list").every((c) => c.params.turnId === "bound")).toBe(true);
  });

  test("跨运行复用：最近绑定轮在后续页，绑定 ID 字典序不等于时间序", async () => {
    const fake = fixture([turn("z-old", "旧绑定结果"), turn("a-recent", "最近绑定结果"),
      ...Array.from({ length: 70 }, (_, i) => turn(`foreign-${i}`, "其他运行结果"))]);
    const result = (await read(fake, [node("child", ["a-recent", "z-old"])] )).get("child")!;
    expect(result).toMatchObject({ turnId: "a-recent", output: "最近绑定结果", availability: "available" });
    expect(fake.calls.filter((c) => c.method === "thread/turns/list")).toHaveLength(2);
    expect(fake.calls.filter((c) => c.method === "thread/items/list")).toHaveLength(2);
    expect(fake.calls.filter((c) => c.method === "thread/turns/list").every((c) => c.params.itemsView === "notLoaded" && c.params.sortDirection === "desc")).toBe(true);
  });

  test("没有绑定集合或服务器无匹配轮时不可用，不读取外部轮次", async () => {
    const fake = fixture([turn("foreign", "不可交付")]);
    const result = await read(fake, [node("empty", []), node()]);
    expect(result.get("empty")?.availability).toBe("unavailable");
    expect(result.get("child")?.availability).toBe("unavailable");
    expect(fake.calls).toHaveLength(1);
  });

  test("只补读 subagent，不扩张 fork 或 reviewExecution", async () => {
    const fake = fixture([turn("bound", "可用")]);
    const results = await read(fake, [node(), { ...node("fork"), relationKind: "fork" }, { ...node("review"), relationKind: "reviewExecution" }]);
    expect([...results.keys()]).toEqual(["child"]);
    expect(fake.calls.every((c) => c.params.threadId === "child")).toBe(true);
  });
});

describe("完整分页与正文语义", () => {
  test("正文跨页且复用 review → finalAnswer → accumulated 提取优先级", async () => {
    for (const [items, expected, source] of [
      [[{ type: "agentMessage", text: "过程" }, { type: "agentMessage", text: "最终", phase: "final_answer" }, { type: "exitedReviewMode", review: "审核" }], "审核", "review"],
      [[{ type: "agentMessage", text: "过程" }, { type: "agentMessage", text: "甲", phase: "final_answer" }, { type: "agentMessage", text: "乙", phase: "final_answer" }], "甲\n乙", "finalAnswer"],
      [[{ type: "agentMessage", text: "甲" }, { type: "agentMessage", text: "乙" }], "甲\n乙", "accumulated"],
      [[], "", "none"],
    ] as const) {
      const fake = fixture([{ id: "bound", status: "interrupted", items: [...items] }]);
      expect((await read(fake)).get("child")).toMatchObject({ output: expected, outputSource: source, status: "interrupted", availability: "available" });
    }
  });

  test("匹配轮后仍校验同页其余轮次，损坏不能被首项遮蔽", async () => {
    const fake = fixture([turn("bound", "不可交付")]);
    fake.handle = () => ({ data: [{ id: "bound", status: "completed" }, { id: "bad", status: "unknown" }], nextCursor: null });
    expect((await read(fake)).get("child")).toMatchObject({ availability: "unavailable", output: "", status: null });
  });

  test("条目后页读取失败不可交付部分正文，但保留独立核实的轮次状态", async () => {
    const fake = fixture([turn("bound", "结果", "failed")]);
    const original = fake.handle;
    fake.handle = (method, params) => {
      if (method !== "thread/items/list") return original(method, params);
      if (params.cursor) throw new Error("password=不允许显示");
      return { data: [{ turnId: "bound", item: { type: "agentMessage", text: "半截正文" } }], nextCursor: "next" };
    };
    const result = (await read(fake)).get("child")!;
    expect(result).toMatchObject({ turnId: "bound", status: "failed", source: "server", availability: "unavailable", output: "", truncated: false });
    expect(result.reason).not.toContain("password");
  });

  test("条目接口不支持时不可用，不 resume，也不请求 full 回退", async () => {
    const fake = fixture([turn("bound", "结果")], "legacy");
    expect((await read(fake)).get("child")).toMatchObject({ availability: "unavailable", status: "completed" });
    expect(fake.calls.map((c) => c.method)).toEqual(["thread/turns/list", "thread/items/list"]);
    expect(fake.calls[0]!.params.itemsView).toBe("notLoaded");
  });

  test("空页续游标、空游标、游标循环都明确不可用", async () => {
    for (const method of ["thread/turns/list", "thread/items/list"]) {
      for (const kind of ["empty-page", "empty-cursor", "cycle"]) {
        const fake = fixture([turn("bound", "结果")]);
        const original = fake.handle;
        fake.handle = (name, params) => {
          if (name !== method) return original(name, params);
          const data = name === "thread/turns/list" ? [{ id: "foreign", status: "completed" }] : [{ turnId: "bound", item: { type: "agentMessage", text: "半截" } }];
          return { data: kind === "empty-page" ? [] : data, nextCursor: kind === "empty-cursor" ? "" : "repeat" };
        };
        expect((await read(fake)).get("child")).toMatchObject({ availability: "unavailable", output: "", truncated: false });
        expect(fake.calls.length).toBeLessThanOrEqual(3);
      }
    }
  });

  test("条目绑定错误、线程归属错误与畸形分页都不可用", async () => {
    for (const bad of [null, { data: [], nextCursor: undefined },
      { data: [{ turnId: "other", item: { type: "agentMessage", text: "错误结果" } }], nextCursor: null },
      { data: [{ turnId: "bound", threadId: "other-thread", item: { type: "agentMessage", text: "错误线程" } }], nextCursor: null },
      { data: [{ turnId: "bound", item: { type: "agentMessage", text: 123 } }], nextCursor: null },
      { threadId: "other-thread", data: [], nextCursor: null }]) {
      const fake = fixture([turn("bound", "结果")]);
      const original = fake.handle;
      fake.handle = (method, params) => method === "thread/items/list" ? bad : original(method, params);
      expect((await read(fake)).get("child")).toMatchObject({ status: "completed", availability: "unavailable", output: "" });
    }
  });

  test("每线程轮次页与条目页合计最多 64 页，未读完不是展示截断", async () => {
    const fake = fixture([turn("bound", "结果")]);
    const original = fake.handle;
    let count = 0;
    fake.handle = (method, params) => method === "thread/turns/list" ? original(method, params)
      : { data: [{ turnId: "bound", item: { type: "agentMessage", text: "半截" } }], nextCursor: `page-${++count}` };
    expect((await read(fake)).get("child")).toMatchObject({ status: "completed", availability: "unavailable", output: "", truncated: false });
    expect(fake.calls).toHaveLength(64);
  });
});

describe("补读限额、期限与资源", () => {
  test("完整结果超过 8 KiB 才标 truncated，UTF-8 不切断 Unicode", async () => {
    const fake = fixture([turn("bound", "🙂".repeat(3000))]);
    const result = (await read(fake)).get("child")!;
    expect(result).toMatchObject({ availability: "truncated", truncated: true });
    expect(result.output).toBe("🙂".repeat(2048));
    expect(Buffer.byteLength(result.output)).toBe(8192);
  });

  test("总正文 64 KiB；超额正文 omitted，节点不丢失", async () => {
    const fake = fixture([turn("bound", "x".repeat(8192))]);
    const results = await read(fake, Array.from({ length: 10 }, (_, i) => node(`child-${i}`)));
    expect(results.size).toBe(10);
    expect([...results.values()].reduce((sum, r) => sum + Buffer.byteLength(r.output), 0)).toBe(65536);
    expect(results.get("child-7")?.availability).toBe("available");
    expect(results.get("child-8")).toMatchObject({ availability: "omitted", status: "completed", output: "", truncated: false });
  });

  test("总配额剩余不足时裁剪，线程数超过 32 时 omitted", async () => {
    const fake = fixture([turn("bound", "🙂".repeat(2000))]);
    const results = await read(fake, Array.from({ length: 33 }, (_, i) => node(`child-${i}`)));
    expect(results.size).toBe(33);
    expect(results.get("child-8")).toMatchObject({ availability: "truncated", output: "🙂".repeat(384), truncated: true });
    expect(results.get("child-32")).toMatchObject({ availability: "omitted", status: null });
    expect(new Set(fake.calls.map((c) => c.params.threadId)).size).toBe(32);
  });

  test("最多四个 RPC 并发；取消后不再读取排队节点，迟到回包不能改写交付结果", async () => {
    const fake = new FakeRpc();
    const gate = deferred<unknown>();
    const controller = new AbortController();
    fake.handle = () => gate.promise;
    const pending = read(fake, Array.from({ length: 6 }, (_, i) => node(`child-${i}`)), performance.now() + 10_000, controller.signal);
    await messageBarrier(() => [fake.calls.length]);
    expect(fake.calls).toHaveLength(4);
    controller.abort();
    const results = await pending;
    expect([...results.values()].every((r) => r.availability === "unavailable")).toBe(true);
    expect(fake.calls).toHaveLength(4);
    gate.resolve({ data: [{ id: "bound", status: "completed" }], nextCursor: null });
    await messageBarrier(() => [fake.calls.length]);
    expect([...results.values()].every((r) => r.availability === "unavailable")).toBe(true);
    expect(fake.calls).toHaveLength(4);
  });

  test("命令剩余期限优先于五秒窗口；挂起请求到期转 unavailable", async () => {
    const fake = new FakeRpc();
    const gate = deferred<unknown>();
    fake.handle = () => gate.promise;
    const started = performance.now();
    const results = await read(fake, [node()], started + 40);
    expect(results.get("child")).toMatchObject({ availability: "unavailable", output: "" });
    expect(fake.calls[0]!.options.timeoutMs).toBeLessThanOrEqual(40);
    expect(performance.now() - started).toBeLessThan(1500);
    gate.resolve({ data: [], nextCursor: null });
  });

  test("单 RPC 最多两秒，已到期或预先取消不发送请求", async () => {
    const fake = new FakeRpc();
    const gate = deferred<unknown>();
    fake.handle = () => gate.promise;
    const result = await read(fake);
    expect(result.get("child")?.availability).toBe("unavailable");
    expect(fake.calls[0]!.options.timeoutMs).toBe(2000);
    gate.resolve({ data: [], nextCursor: null });
    const before = fake.calls.length;
    expect((await read(fake, [node()], performance.now() - 1)).get("child")?.availability).toBe("unavailable");
    const controller = new AbortController(); controller.abort();
    expect((await read(fake, [node()], performance.now() + 1000, controller.signal)).get("child")?.availability).toBe("unavailable");
    expect(fake.calls).toHaveLength(before);
  });

  test("无命令期限压力时总窗口仍为五秒，排队子线程不能延长窗口", async () => {
    const fake = new FakeRpc();
    const gate = deferred<unknown>();
    fake.handle = () => gate.promise;
    const started = performance.now();
    const results = await read(fake, Array.from({ length: 16 }, (_, i) => node(`child-${i}`)), started + 60_000);
    const elapsed = performance.now() - started;
    expect(results.size).toBe(16);
    expect([...results.values()].every((r) => r.availability === "unavailable")).toBe(true);
    expect(elapsed).toBeGreaterThanOrEqual(4900);
    expect(elapsed).toBeLessThan(6500);
    expect(fake.calls.length).toBeLessThanOrEqual(12);
    expect(fake.calls[8]!.options.timeoutMs).toBeLessThan(1500);
    gate.resolve({ data: [], nextCursor: null });
  });

  test("计时器比总期限早触发时，排队子线程不再发注定超时的请求", async () => {
    const fake = new FakeRpc();
    const gate = deferred<unknown>();
    fake.handle = () => gate.promise;
    // Windows 上实测计时器会比 performance.now 算出的时刻早 1～15 毫秒；固定早 30 毫秒，让偶发变成必现。
    const realSetTimeout = globalThis.setTimeout;
    let shortened = 0;
    globalThis.setTimeout = ((callback: (...args: unknown[]) => void, delay?: number, ...args: unknown[]) => {
      if (typeof delay === "number" && delay > 30 && delay <= 2000) { shortened++; delay -= 30; }
      return realSetTimeout(callback, delay, ...args);
    }) as typeof setTimeout;
    let results: Awaited<ReturnType<typeof read>>;
    try {
      results = await read(fake, Array.from({ length: 16 }, (_, i) => node(`child-${i}`)), performance.now() + 60_000);
    } finally {
      globalThis.setTimeout = realSetTimeout;
      gate.resolve({ data: [], nextCursor: null });
    }
    // 前置条件：三轮请求的计时器都确实被提前了，第三轮结束时总期限还没到。
    expect(shortened).toBeGreaterThanOrEqual(12);
    expect(results.size).toBe(16);
    expect([...results.values()].every((r) => r.availability === "unavailable")).toBe(true);
    expect(fake.calls).toHaveLength(12);
  });
});
