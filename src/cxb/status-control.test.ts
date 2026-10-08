// H2-b 黑盒：长历史（全量读超过 10 MiB）与严格分页、活动轮次不进存储页的模拟服务器下，
// 经真 broker 的 `cxb stop`、`run --resume --continue` 正常工作；broker 不因超限退出，
// 同工作区另一条线程的运行不受影响。broker 的控制类读取（续跑预检、停止核实、对账、迟到归属）
// 只读元数据与分页，不再发 includeTurns:true。
import { afterEach, beforeEach, expect, test } from "bun:test";
import { join } from "node:path";
import { stop } from "./stop";
import { bounded } from "./wait-run";
import { RunHarness, until, alive, TEST_BUDGET, CLEANUP_BUDGET } from "./test-fixtures/run-harness";

const STATUS_READ = { longHistory: true, strictPagination: true, omitActiveTurns: true };

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

test("长历史与严格分页：cxb stop、--continue 正常，broker 不退出，另一条线程不受影响", async () => {
  const observer = await h.start({ hold: true, statusRead: STATUS_READ, backgroundTerminals: { enabled: true } });
  const pid = h.broker!.pid!;
  const before = await observer.rpc.request<any>("broker/health");
  const env = { ...h.env, RUN_FIXTURE_WORKSPACE: JSON.stringify(h.workspace) };
  const script = join(import.meta.dir, "test-fixtures/run-client.ts");
  const state = async () => (await observer.rpc.request<any>("fixture/state")).calls as Array<{ method: string; params: any }>;
  const starts = async (threadId: string) => (await state()).filter((c) => c.method === "turn/start" && c.params.threadId === threadId);

  // B：同工作区另一条线程一直在跑。
  const b = h.spawn([process.execPath, script, "run", "B 任务", "--json"], env);
  b.child.stdin!.end();
  await until(async () => (await starts("thread-1")).length === 1);

  // A：长历史线程上运行，再用 cxb stop 停下。
  const a = h.spawn([process.execPath, script, "run", "A 任务", "--resume", "thread-A", "--json"], env);
  a.child.stdin!.end();
  await until(async () => (await starts("thread-A")).length === 1);
  await until(async () => (await observer.rpc.request<any>("broker/threads/watch", { threadId: "thread-A" })).run?.state === "running");
  const runA = (await observer.rpc.request<any>("broker/threads/watch", { threadId: "thread-A" })).run.runId as string;
  const stopped = await h.invoke(stop, ["stop", "thread-A", "--json"]);
  expect(stopped.code).toBe(0);
  expect(JSON.parse(stopped.stdout)).toMatchObject({ stopOutcome: "confirmed", runId: runA });
  expect(await bounded(a.closed, 10_000)).toBe(4);

  // 续跑：恢复前后两次预检只读元数据（Busy），核实通过后开工。
  const readsBefore = (await state()).filter((c) => c.method === "thread/read").length;
  const continued = h.spawn([process.execPath, script, "run", "--resume", "thread-A", "--continue", "--json", "补充要求"], env);
  continued.child.stdin!.end();
  await until(async () => (await starts("thread-A")).length === 2);
  const second = (await starts("thread-A"))[1]!;
  expect(second.params).not.toHaveProperty("cxbContinue");
  const prechecks = (await state()).filter((c) => c.method === "thread/read").slice(readsBefore);
  expect(prechecks.length).toBeGreaterThanOrEqual(1);
  for (const read of prechecks) expect(read.params).toEqual({ threadId: "thread-A" });
  await observer.rpc.request("fixture/control", { action: "finish", threadId: "thread-A" });
  expect(await bounded(continued.closed, 10_000)).toBe(0);
  expect(JSON.parse(continued.output().stdout)).toMatchObject({ continuedFrom: runA, result: "完整中文结果" });

  // 整个过程没有任何全量读；broker 与另一条线程都还在。
  const calls = await state();
  expect(calls.filter((c) => c.method === "thread/read" && c.params.includeTurns)).toEqual([]);
  expect(h.broker!.pid).toBe(pid);
  expect(alive(pid)).toBe(true);
  expect((await observer.rpc.request<any>("broker/health")).epoch).toBe(before.epoch);
  expect((await observer.rpc.request<any>("broker/threads/watch", { threadId: "thread-1" })).run.state).toBe("running");
  expect(alive(b.child.pid!)).toBe(true);
  await observer.rpc.request("fixture/control", { action: "finish", threadId: "thread-1" });
  expect(await bounded(b.closed, 10_000)).toBe(0);
  expect(JSON.parse(b.output().stdout).result).toBe("完整中文结果");
}, TEST_BUDGET);
