import { afterEach, beforeEach, expect, test } from "bun:test";
import { join } from "node:path";
import { readFileSync } from "node:fs";
import { createRunHandler } from "./run";
import { createReviewHandler } from "./review";
import { ASK_FEATURE_KEY } from "./contracts";
import { bounded } from "./wait-run";
import { RunHarness, fakeServerRequests, until, alive, TEST_BUDGET, CLEANUP_BUDGET } from "./test-fixtures/run-harness";
import { createMockCodex, writeMockCodexScript } from "../test-support/broker-harness";

let h: RunHarness;
let oldHome: string | undefined;
beforeEach(() => {
  h = new RunHarness();
  oldHome = process.env.CODEX_HOME;
  process.env.CODEX_HOME = h.env.CODEX_HOME;
});
afterEach(async () => {
  try { await h.cleanup(); } finally {
    if (oldHome === undefined) delete process.env.CODEX_HOME; else process.env.CODEX_HOME = oldHome;
  }
}, CLEANUP_BUDGET);

for (const review of [false, true]) for (const rejectAsk of [false, true]) {
  test(`长历史恢复：${review ? "审查" : "普通运行"}，${rejectAsk ? "去键重试" : "首次成功"}`, async () => {
    const observer = await h.start({ longHistory: true, rejectAsk });
    const first = await h.invoke(createRunHandler(fakeServerRequests()), ["run", "初始任务", "--json"]);
    expect(first.code).toBe(0);
    const id = JSON.parse(first.stdout).thread.id;
    const result = await h.invoke(review ? createReviewHandler(fakeServerRequests()) : createRunHandler(fakeServerRequests()),
      review ? ["review", "--resume", id, "--mode", "uncommitted", "--json"] : ["run", "接着做", "--resume", id, "--json"]);
    expect(result.code).toBe(0);
    expect(JSON.parse(result.stdout)).toMatchObject({ thread: { id }, ...(!review ? { model: "mock-model", reasoning: "high" } : {}),
      result: review ? "完整中文审查" : "完整中文结果" });
    const { calls } = await observer.rpc.request<any>("fixture/state");
    const resumes = calls.filter((c: any) => c.method === "thread/resume");
    expect(resumes).toHaveLength(rejectAsk ? 2 : 1);
    for (const call of resumes) expect(call.params.excludeTurns).toBe(true);
    for (const call of calls.filter((c: any) => c.method === "thread/start")) expect(call.params).not.toHaveProperty("excludeTurns");
    if (rejectAsk) {
      expect(resumes[0].params.config[ASK_FEATURE_KEY]).toBe(true);
      expect(resumes[1].params.config).not.toHaveProperty(ASK_FEATURE_KEY);
    }
  }, TEST_BUDGET);
}

test("长历史黑盒：B 先运行，恢复 A 完成后 B 与原 broker 连接仍存活", async () => {
  const observer = await h.start({ longHistory: true, hold: true });
  const pid = h.broker!.pid!;
  const env = { ...h.env, RUN_FIXTURE_WORKSPACE: JSON.stringify(h.workspace) };
  const script = join(import.meta.dir, "test-fixtures/run-client.ts");
  const b = h.spawn([process.execPath, script, "run", "B 任务", "--json"], env);
  b.child.stdin!.end();
  await until(async () => (await observer.rpc.request<any>("fixture/state")).calls.some((c: any) => c.method === "turn/start" && c.params.threadId === "thread-1"));
  expect((await observer.rpc.request<any>("broker/threads/watch", { threadId: "thread-1" })).run.state).toBe("running");
  const before = await observer.rpc.request<any>("broker/health");
  const a = h.spawn([process.execPath, script, "run", "A 任务", "--resume", "thread-A", "--json"], env);
  a.child.stdin!.end();
  await until(async () => (await observer.rpc.request<any>("fixture/state")).calls.some((c: any) => c.method === "turn/start" && c.params.threadId === "thread-A"));
  expect((await observer.rpc.request<any>("broker/threads/watch", { threadId: "thread-A" })).run.state).toBe("running");
  await observer.rpc.request("fixture/control", { action: "finish", threadId: "thread-A" });
  expect(await bounded(a.closed, 10_000)).toBe(0);
  expect(JSON.parse(a.output().stdout).result).toBe("完整中文结果");
  expect((await observer.rpc.request<any>("broker/threads/watch", { threadId: "thread-1" })).run.state).toBe("running");
  expect(h.broker!.pid).toBe(pid);
  expect(alive(pid)).toBe(true);
  expect(alive(b.child.pid!)).toBe(true);
  expect((await observer.rpc.request<any>("broker/health")).epoch).toBe(before.epoch);
  const { calls } = await observer.rpc.request<any>("fixture/state");
  expect(calls.find((c: any) => c.method === "thread/resume" && c.params.threadId === "thread-A").params.excludeTurns).toBe(true);
  await observer.rpc.request("fixture/control", { action: "finish", threadId: "thread-1" });
  expect(await bounded(b.closed, 10_000)).toBe(0);
  expect(JSON.parse(b.output().stdout).result).toBe("完整中文结果");
}, TEST_BUDGET);

for (const fixture of ["run", "e2e", "broker"] as const) test(`长历史夹具 ${fixture}：合法大回包与随后小消息不交错，start 不携带合成历史`, async () => {
  if (fixture === "broker") createMockCodex(h.dir, { longHistory: true });
  if (fixture === "e2e") writeMockCodexScript(h.dir, readFileSync(join(import.meta.dir, "test-fixtures/e2e-codex.ts"), "utf8"));
  const script = fixture !== "run" ? join(h.dir, "codex-mock.ts")
    : join(import.meta.dir, "test-fixtures", fixture === "run" ? "run-app-server.ts" : "e2e-codex.ts");
  const child = h.spawn([process.execPath, script, "app-server"], { ...h.env,
    RUN_FIXTURE_OPTIONS: JSON.stringify({ longHistory: true }), MOCK_E2E_LONG_HISTORY: "1", MOCK_E2E_SCENARIO: "complete" });
  const messages = [
    { id: 1, method: "thread/resume", params: { threadId: "history" } },
    { id: 2, method: "thread/resume", params: { threadId: "history", excludeTurns: false } },
    { id: 3, method: "thread/resume", params: { threadId: "history", excludeTurns: true } },
    { id: 4, method: "thread/start", params: {} },
    { id: 5, method: fixture === "run" ? "fixture/state" : fixture === "broker" ? "mock/state" : "thread/read", params: { threadId: "history" } },
  ];
  let lines = 0;
  const received = new Promise<void>((resolve) => child.child.stdout!.on("data", (chunk: string) => {
    for (const char of chunk) if (char === "\n") lines++;
    if (lines >= messages.length) resolve();
  }));
  child.child.stdin!.write(messages.map(message => JSON.stringify(message) + "\n").join(""));
  await bounded(received, 15_000);
  const frames = child.output().stdout.trim().split("\n").map(line => ({ line, message: JSON.parse(line) }));
  for (const id of [1, 2]) {
    const frame = frames.find(f => f.message.id === id)!;
    expect(Buffer.byteLength(frame.line)).toBeGreaterThan(12 * 1024 * 1024);
    expect(Buffer.byteLength(frame.line)).toBeLessThan(13 * 1024 * 1024);
    expect(frame.message.result.thread.turns).toHaveLength(3072);
    expect(frame.message.result.thread.turns.every((t: any) => t.items[0].text.length === 4096)).toBe(true);
  }
  for (const id of [3, 4, 5]) expect(Buffer.byteLength(frames.find(f => f.message.id === id)!.line)).toBeLessThan(4096);
}, TEST_BUDGET);
