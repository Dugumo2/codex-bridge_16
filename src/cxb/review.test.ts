import { afterEach, beforeEach, expect, test } from "bun:test";
import { join } from "node:path";
import { createReviewHandler } from "./review";
import { bounded } from "./wait-run";
import { RunHarness, fakeServerRequests, CLEANUP_BUDGET, TEST_BUDGET, alive } from "./test-fixtures/run-harness";

let h: RunHarness;
beforeEach(() => { h = new RunHarness(); });
afterEach(async () => { await h.cleanup(); }, CLEANUP_BUDGET);

test("四种审查目标、默认 main/HEAD、强制只读与 never、运行保留父线程并转子线程", async () => {
  const observer = await h.start();
  const handler = createReviewHandler(fakeServerRequests());
  for (const [args, target] of [
    [[], { type: "baseBranch", branch: "main" }],
    [["--mode", "uncommitted"], { type: "uncommittedChanges" }],
    [["--mode", "commit"], { type: "commit", sha: "HEAD" }],
    [["检查", "安全边界"], { type: "custom", instructions: "检查 安全边界" }],
  ] as Array<[string[], object]>) {
    const result = await h.invoke((ctx) => {
      // 显式移除解析层补上的缺省值，独立验证执行层的 HEAD 回退。
      if (ctx.parsed.options.mode === "commit") delete ctx.parsed.options.ref;
      return handler(ctx);
    }, ["review", ...args, "--json", "-s", "danger-full-access", "--approval", "auto"]);
    expect(result.code).toBe(0);
    const output = JSON.parse(result.stdout);
    expect(output.command).toBe("review");
    expect(output.result).toBe("完整中文审查");
    const snapshot = await observer.rpc.request<any>("broker/threads/watch", { threadId: output.thread.id });
    expect(snapshot.run.reviewThreadId).toMatch(/^review-/);
    expect(snapshot.run.threadId).toBe(output.thread.id);
    const { calls } = await observer.rpc.request<any>("fixture/state");
    expect(calls.filter((call: any) => call.method === "review/start").at(-1).params).toMatchObject({ target, delivery: "detached" });
    expect(calls.filter((call: any) => call.method === "thread/start").at(-1).params).toMatchObject({ sandbox: "read-only", approvalPolicy: "never" });
    // 告警由 args/main 负责，执行层不得重复。
    expect(result.stderr).toBe("");
  }
}, TEST_BUDGET);

test("review 子线程提问也能交接退出五", async () => {
  const observer = await h.start({ reviewQuestion: true });
  const result = await h.invoke(createReviewHandler(fakeServerRequests()), ["review", "--mode", "uncommitted", "--json"]);
  expect(result.code).toBe(5);
  const output = JSON.parse(result.stdout);
  expect(output.pending[0].kind).toBe("question");
  const snapshot = await observer.rpc.request<any>("broker/threads/watch", { threadId: output.threadId });
  expect(snapshot.run.state).toBe("running");
}, TEST_BUDGET);

test("review 大中文 JSON 管道完整且自然退出", async () => {
  const output = "审查报告中文。\n".repeat(16000);
  await h.start({ output });
  const child = h.spawn([process.execPath, join(import.meta.dir, "test-fixtures/run-client.ts"), "review", "--mode", "uncommitted", "--json"],
    { ...h.env, RUN_FIXTURE_WORKSPACE: JSON.stringify(h.workspace) });
  child.child.stdin!.end();
  expect(await bounded(child.closed, 20_000)).toBe(0);
  expect(alive(child.child.pid!)).toBe(false);
  expect(JSON.parse(child.output().stdout).result).toBe(output);
}, TEST_BUDGET);

test("review 失败七与超时三", async () => {
  await h.start({ fail: true });
  const result = await h.invoke(createReviewHandler(fakeServerRequests()), ["review", "--mode", "uncommitted", "--json"]);
  expect(result.code).toBe(7);
  expect(JSON.parse(result.stdout).status).toBe("failed");
}, TEST_BUDGET);

test("review 超过期限退出三", async () => {
  await h.start({ hold: true });
  const result = await h.invoke(createReviewHandler(fakeServerRequests()), ["review", "--mode", "uncommitted", "--timeout", "1"]);
  expect(result.code).toBe(3);
}, TEST_BUDGET);

test("执行层独立强制审批 never，不依赖解析层改写", async () => {
  const observer = await h.start();
  const handler = createReviewHandler(fakeServerRequests());
  const result = await h.invoke((ctx) => {
    ctx.parsed.globals.approval = "auto";
    ctx.parsed.globals.sandbox = "danger-full-access";
    return handler(ctx);
  }, ["review", "--mode", "uncommitted"]);
  expect(result.code).toBe(0);
  const { calls } = await observer.rpc.request<any>("fixture/state");
  expect(calls.find((call: any) => call.method === "thread/start").params).toMatchObject({ approvalPolicy: "never", sandbox: "read-only" });
}, TEST_BUDGET);
