import { afterEach, beforeEach, expect, test } from "bun:test";
import { join } from "node:path";
import { createReviewHandler } from "./review";
import { createRunHandler, rpcDetail } from "./run";
import type { BrokerConnection } from "./contracts";
import type { Cleanup } from "./output";
import { listThreadRuns } from "./ledger";
import { resolveWorkspace } from "./workspace";
import { bounded } from "./wait-run";
import { RunHarness, fakeServerRequests, CLEANUP_BUDGET, TEST_BUDGET, alive, until } from "./test-fixtures/run-harness";

let h: RunHarness;
beforeEach(() => { h = new RunHarness(); });
afterEach(async () => { await h.cleanup(); }, CLEANUP_BUDGET);

/** 仅在测试连接边界改写请求；服务器仍按实际 delivery 校验。 */
function detachedRequest(connection: BrokerConnection): BrokerConnection {
  const request = connection.rpc.request.bind(connection.rpc);
  return { ...connection, rpc: { ...connection.rpc,
    request: (method, params, options) => request(method,
      method === "review/start" ? { ...(params as object), delivery: "detached" } : params, options),
  } };
}

test("协议反例：Paginated 服务器精确拒绝 detached 审查", async () => {
  const observer = await h.start();
  const { thread } = await observer.rpc.request<any>("thread/start", {});
  const error = await observer.rpc.request("review/start", { threadId: thread.id, delivery: "detached",
    target: { type: "uncommittedChanges" } }).then(() => null, (error) => error);
  expect(rpcDetail(error)).toEqual({ code: -32600, message: "paginated threads do not support detached review" });
}, TEST_BUDGET);

test("CLI 连接边界改成 detached 时审查退出七", async () => {
  await h.start();
  const result = await h.invoke(createReviewHandler(fakeServerRequests()), ["review", "--mode", "uncommitted"],
    detachedRequest(await h.connect()));
  expect(result.code).toBe(7);
  expect(result.stderr).toContain("Codex");
  expect(result.stderr).toContain("失败");
  // 上游报错可见化（契约 3.4）：启动失败的脱敏原因要展示，且同一段原因在 stderr 里只出现一次。
  expect(result.stderr).toContain("[cxb] 原因：paginated threads do not support detached review");
  expect(result.stderr).toContain("[cxb] 失败原因：见上方上游报错。");
  expect(result.stderr.split("paginated threads").length - 1).toBe(1);
}, TEST_BUDGET);

for (const legacyReview of [false, true]) test(`${legacyReview ? "legacy detached 迁移才重新挂接" : "inline 同线程回包不重新挂接"}服务端请求`, async () => {
  const observer = await h.start({ legacyReview });
  const attachments: string[] = [];
  const deps = fakeServerRequests();
  const result = await h.invoke(createReviewHandler({ ...deps, attachServerRequests(rpc, opts) {
    attachments.push(opts.threadId);
    return deps.attachServerRequests(rpc, opts);
  } }), ["review", "--mode", "uncommitted", "--json"], legacyReview ? detachedRequest(await h.connect()) : undefined);
  expect(result.code).toBe(0);
  const parent = JSON.parse(result.stdout).thread.id;
  const { run } = await observer.rpc.request<any>("broker/threads/watch", { threadId: parent });
  expect(attachments).toEqual(legacyReview ? [parent, run.reviewThreadId] : [parent]);
  expect(legacyReview ? run.reviewThreadId !== parent : run.reviewThreadId === null).toBe(true);
}, TEST_BUDGET);

test("inline 审查运行中信号清理只停止父线程和本次运行", async () => {
  const observer = await h.start({ hold: true, backgroundTerminals: { enabled: true } });
  const connection = await h.connect();
  const request = connection.rpc.request.bind(connection.rpc);
  const stops: unknown[] = [];
  let started: { threadId: string; runId: string } | undefined;
  let cleanup: Cleanup | undefined;
  const unsubscribe = connection.rpc.on("broker/run/started", (params: any) => { started = params; });
  h.disposers.push(unsubscribe);
  connection.rpc.request = (method, params, options) => {
    if (method === "broker/threads/stop") stops.push(params);
    return request(method, params, options);
  };
  const handler = createReviewHandler(fakeServerRequests());
  const work = h.invoke((ctx) => {
    const register = ctx.registerCleanup;
    ctx.registerCleanup = (callback) => { cleanup = callback; return register(callback); };
    return handler(ctx);
  }, ["review", "--mode", "uncommitted"], connection);
  void work.catch(() => {});
  await until(async () => !!started && !!cleanup
    && (await observer.rpc.request<any>("broker/threads/watch", { threadId: started.threadId })).thread.state === "running");
  await bounded(Promise.resolve(cleanup!("SIGINT")), 10_000);
  await bounded(work, 10_000);
  expect(stops).toEqual([{ threadId: started!.threadId, runId: started!.runId }]);
  expect(started!.threadId).toMatch(/^thread-/);
}, TEST_BUDGET);

test("review resume 在原线程追加新运行并保留前次历史结果", async () => {
  const observer = await h.start();
  const first = await h.invoke(createRunHandler(fakeServerRequests()), ["run", "初次运行", "--json"]);
  expect(first.code).toBe(0);
  const before = JSON.parse(first.stdout);
  const review = await h.invoke(createReviewHandler(fakeServerRequests()),
    ["review", "--resume", before.thread.id, "--mode", "uncommitted", "--json"]);
  expect(review.code).toBe(0);
  const after = JSON.parse(review.stdout);
  expect(after.thread.id).toBe(before.thread.id);
  expect(after.runId).not.toBe(before.runId);
  expect(after.result).toBe("完整中文审查");
  const workspaceDir = resolveWorkspace(h.workspace.root, h.env).workspaceDir;
  await until(() => listThreadRuns(workspaceDir, before.thread.id).length === 2);
  const runs = listThreadRuns(workspaceDir, before.thread.id);
  expect(runs.find((run) => run.runId === before.runId)?.turns[0]?.output).toBe("完整中文结果");
  const { run } = await observer.rpc.request<any>("broker/threads/watch", { threadId: before.thread.id });
  expect(run.reviewThreadId).toBeNull();
}, TEST_BUDGET);

test("四种审查目标、默认 main/HEAD、强制只读与 never、运行留在审查线程", async () => {
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
    expect(snapshot.run.reviewThreadId).toBeNull();
    expect(snapshot.thread.threadId).toBe(output.thread.id);
    expect(output.reviewThreadId).toBeUndefined();
    expect(snapshot.run.threadId).toBe(output.thread.id);
    const { calls } = await observer.rpc.request<any>("fixture/state");
    expect(calls.filter((call: any) => call.method === "review/start").at(-1).params).toMatchObject({ target, delivery: "inline" });
    expect(calls.filter((call: any) => call.method === "thread/start").at(-1).params).toMatchObject({ sandbox: "read-only", approvalPolicy: "never" });
    // 告警由 args/main 负责，执行层不得重复。
    expect(result.stderr).toBe("");
  }
}, TEST_BUDGET);

test("review 原线程提问能交接退出五", async () => {
  const observer = await h.start({ reviewQuestion: true });
  const result = await h.invoke(createReviewHandler(fakeServerRequests()), ["review", "--mode", "uncommitted", "--json"]);
  expect(result.code).toBe(5);
  const output = JSON.parse(result.stdout);
  expect(output.pending[0].kind).toBe("question");
  expect(output.pending[0].threadId).toBe(output.threadId);
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
