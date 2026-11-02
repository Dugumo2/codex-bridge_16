import { afterEach, expect, spyOn, test } from "bun:test";
import { observeWaitTree } from "./wait-tree";
import type { ChildChanged, ChildrenStopReport, RunResult } from "./contracts";
import { treeResultText } from "./subagent-tree";
import type { TreeSnapshot, WatchTreeMember } from "./tree-contracts";
import { FakeRpc, runResult } from "./test-fixtures/run-fakes";
import { gate } from "./test-fixtures/n4b-support";
import { messageBarrier, watchdog } from "./test-fixtures/test-watchdog";

const resources: Array<() => void> = [];
afterEach(() => { for (const close of resources.splice(0).reverse()) close(); });
const member = (patch: Partial<WatchTreeMember> = {}): WatchTreeMember => ({ threadId: "child", parentThreadId: "thread-test",
  relationKind: "subagent", activity: "starting", pendingCount: 0, conflict: null, turnIds: ["t"], ...patch });
const snapshot = (revision = 1, members = [member()]): TreeSnapshot => ({ run: runResult({ state: "running", status: null }), revision,
  children: { members, discovery: { complete: true, version: 1, incompleteReasons: [] }, inFlightSpawns: 0, stop: null } });
const event = (patch: Partial<ChildChanged> = {}): ChildChanged => ({ runId: "run-test", rootThreadId: "thread-test", threadId: "child",
  parentThreadId: "thread-test", relationKind: "subagent", activity: "running", pendingCount: 0, revision: 2, ...patch });

/** 只扣住产品的合并窗口；用显式放行推进补快照，不用睡眠猜竞态。 */
function setup() {
  const timers: Array<() => void> = [];
  const original = globalThis.setTimeout;
  const timerSpy = spyOn(globalThis, "setTimeout");
  // 被扣住的原生 timer 仍由产品 clearTimeout 清理；手动触发时同步清理。
  const handles = new Set<ReturnType<typeof setTimeout>>();
  timerSpy.mockImplementation(((fn: () => void, ms: number, ...args: any[]) => {
    if (ms !== 200) return original(fn, ms, ...args);
    const handle = original(() => {}, 100_000);
    handles.add(handle);
    timers.push(() => { clearTimeout(handle); handles.delete(handle); fn(); });
    return handle;
  }) as typeof setTimeout);
  resources.push(() => { timerSpy.mockRestore(); for (const handle of handles) clearTimeout(handle); });
  const fake = new FakeRpc(), lines: string[] = [];
  const observer = observeWaitTree(fake.rpc, { threadId: "thread-test", deadline: performance.now() + 10_000, notice: (line) => lines.push(line) });
  resources.push(observer.dispose);
  return { fake, observer, lines, timers, fire: () => { const fn = timers.shift(); expect(fn).toBeDefined(); fn!(); } };
}

test("先订阅缓冲；固定运行过滤，重复成员版本不重复显示", () => {
  const c = setup();
  c.fake.emit("broker/run/childChanged", event());
  c.fake.emit("broker/run/childChanged", event({ runId: "other", revision: 900, threadId: "foreign" }));
  c.observer.initialize("run-test", snapshot());
  expect(c.observer.current().nodes.map((node) => node.threadId)).toEqual(["child"]);
  expect(c.observer.current().nodes[0]!.activity).toBe("running");
  const count = c.lines.length;
  c.fake.emit("broker/run/childChanged", event());
  c.fake.emit("broker/run/childChanged", event({ runId: "other", revision: 901, threadId: "foreign" }));
  expect(c.lines).toHaveLength(count);
  expect(c.observer.current().revision).toBe(2);
  expect(c.timers).toHaveLength(1);
});

test("旧快照回包重放新成员通知，合并且最多一个在途，固定 runId 且不抢独占", async () => {
  const c = setup(), reply = gate<TreeSnapshot>();
  c.fake.handle = () => reply.promise;
  c.observer.initialize("run-test", snapshot());
  c.fake.emit("broker/run/childChanged", event());
  c.fake.emit("broker/run/childChanged", event({ revision: 3 }));
  expect(c.timers).toHaveLength(1);
  c.fire();
  expect(c.fake.calls).toHaveLength(1);
  expect(c.fake.calls[0]!.params).toEqual({ threadId: "thread-test", runId: "run-test" });
  c.fake.emit("broker/run/childChanged", event({ revision: 4, activity: "settled", pendingCount: 2 }));
  expect(c.timers).toHaveLength(0);
  reply.resolve(snapshot(2));
  await messageBarrier(() => [c.timers.length, c.lines.length]);
  expect(c.observer.current().nodes[0]).toMatchObject({ activity: "settled", pendingCount: 2 });
  expect(c.observer.current().revision).toBe(4);
  expect(c.observer.current()).toMatchObject({ stale: true, discovery: null, inFlightSpawns: null });
  expect(c.fake.calls).toHaveLength(1);
  expect(c.timers).toHaveLength(1);
});

test("新快照覆盖全部已知变化后恢复全局字段，不永久标记陈旧", async () => {
  const c = setup(), reply = gate<TreeSnapshot>();
  const initial = snapshot(); initial.children!.inFlightSpawns = 1;
  c.observer.initialize("run-test", initial);
  c.fake.handle = () => reply.promise;
  c.fake.emit("broker/run/childChanged", event({ threadId: null, revision: 2 }));
  expect(c.observer.current()).toMatchObject({ stale: true, discovery: null, inFlightSpawns: null });
  c.fire();
  expect(c.observer.current().stale).toBe(true);
  reply.resolve(snapshot(2));
  await messageBarrier(() => [c.lines.length, c.observer.current().stale]);
  expect(c.observer.current().stale).toBeUndefined();
  expect(c.observer.current()).toMatchObject({ discovery: { complete: true }, inFlightSpawns: 0 });
});

test("同 revision 的 stopVersion 单独推进并补取停止报告", async () => {
  const c = setup(), done = gate<void>();
  const report: ChildrenStopReport = { trigger: "userStop", phase: "unconfirmed", version: 8,
    root: { thread: "thread-test", phase: "confirmed", check: { ownership: true, turnEnded: true, goalBraked: true, settled: true, verifyDone: true }, clearRequested: false, clearConfirmed: false, error: null },
    stopped: [], unconfirmed: ["child"], failed: ["child"], conflicts: [], capabilityLimited: ["child"],
    discovery: { complete: false, version: 2, incompleteReasons: ["缺证据"] }, unsettledSpawns: 1 };
  c.fake.handle = () => {
    const next = snapshot();
    next.children!.stop = { phase: report.phase, version: report.version, unconfirmed: report.unconfirmed.length, conflicts: report.conflicts.length };
    next.run = { ...next.run!, childrenStop: report };
    done.resolve(); return next;
  };
  c.observer.initialize("run-test", snapshot());
  c.fake.emit("broker/run/childChanged", event({ revision: 1, threadId: null, stopVersion: 8, stopPhase: "unconfirmed" }));
  expect(c.timers).toHaveLength(1);
  c.fire(); await done.wait();
  await messageBarrier(() => [c.lines.length]);
  expect(c.observer.current().stopVersion).toBe(8);
  expect(c.observer.current().stopSummary).toEqual({ phase: "unconfirmed", version: 8, unconfirmed: 1, conflicts: 0 });
  expect(c.observer.current().nodes[0]!.stop?.issues).toContain("核实失败");
  expect(c.observer.current(runResult({ childrenStop: { ...report, version: 10 } }), "terminal").stopVersion).toBe(10);
  const count = c.lines.length;
  c.fake.emit("broker/run/childChanged", event({ revision: 1, threadId: null, stopVersion: 8 }));
  expect(c.lines).toHaveLength(count);
  expect(c.timers).toHaveLength(0);
});

test("补快照失败保留树并标陈旧，释放后迟到回包与通知不再输出", async () => {
  const c = setup();
  c.fake.handle = () => { throw new Error("模拟失联"); };
  c.observer.initialize("run-test", snapshot());
  c.fake.emit("broker/run/childChanged", event()); c.fire();
  await messageBarrier(() => [c.lines.length]);
  expect(c.observer.current().stale).toBe(true);
  expect(c.observer.current().nodes).toHaveLength(1);
  const reply = gate<TreeSnapshot>();
  c.fake.handle = () => reply.promise;
  c.fake.emit("broker/run/childChanged", event({ revision: 3 })); c.fire();
  c.observer.dispose();
  const before = JSON.stringify(c.observer.current()), printed = c.lines.length;
  reply.resolve(snapshot(90, []));
  c.fake.emit("broker/run/childChanged", event({ revision: 91 }));
  await messageBarrier(() => [c.lines.length]);
  expect(JSON.stringify(c.observer.current())).toBe(before);
  expect(c.lines).toHaveLength(printed);
  expect(c.fake.subscriptions).toBe(0);
});

test("取消信号释放监听和计时器；无变化不轮询；早已完成不演示历史", async () => {
  const fake = new FakeRpc(), lines: string[] = [], controller = new AbortController();
  const observer = observeWaitTree(fake.rpc, { threadId: "thread-test", signal: controller.signal,
    deadline: performance.now() + 1000, notice: (line) => lines.push(line) });
  resources.push(observer.dispose);
  observer.initialize("run-test", snapshot(), true);
  await watchdog(messageBarrier(() => [fake.calls.length]));
  expect(lines).toEqual([]); expect(fake.calls).toEqual([]);
  controller.abort(); expect(fake.subscriptions).toBe(0);
});

const terminal = (patch: Partial<RunResult> = {}) => runResult({ finalTurnId: "turn-test", children: [{ threadId: "child", parentThreadId: "thread-test",
  relationKind: "subagent", turnIds: ["t"], evidence: "v2Started", finalState: "settled" }],
  subagentJoin: { phase: "released", workEpoch: 2, deliveredEpoch: 2, reconciledEpoch: 2, wakeupsStarted: 1,
    wakeupAttempts: 1, startRetriesUsed: 0, deadlineAt: 1000 }, ...patch });
const stopReport = (): ChildrenStopReport => ({ trigger: "userStop", phase: "unconfirmed", version: 5,
  root: { thread: "thread-test", phase: "confirmed", check: { ownership: true, turnEnded: true, goalBraked: true, settled: true, verifyDone: true },
    clearRequested: false, clearConfirmed: false, error: null }, stopped: [], unconfirmed: ["child"], failed: [], conflicts: [], capabilityLimited: [],
  discovery: { complete: true, version: 2, incompleteReasons: [] }, unsettledSpawns: 0 });

test("partial仍标陈旧；完整释放记录只在terminal投影时取代在线缓存", () => {
  const c = setup(); c.observer.initialize("run-test", snapshot());
  c.fake.emit("broker/run/childChanged", event({ revision: 8, pendingCount: 3 }));
  expect(c.observer.current(terminal(), "partial")).toMatchObject({ stale: true, discovery: null, inFlightSpawns: null });
  const tree = c.observer.current(terminal(), "terminal");
  expect(tree.stale).toBeUndefined();
  expect(tree).toMatchObject({ source: "runRecord", revision: null, discovery: null, inFlightSpawns: null, nodes: [{ pendingCount: null, activity: "settled" }] });
  // 终态投影不能改写观察器，之后的 partial 仍保留版本落后的事实。
  expect(c.observer.current().stale).toBe(true);
});

test("停止报告必须追上已见版本；完整报告可以保留未确认风险而不误报缓存陈旧", () => {
  const c = setup(); c.observer.initialize("run-test", snapshot());
  c.fake.emit("broker/run/childChanged", event({ revision: 8, threadId: null, stopVersion: 5, stopPhase: "unconfirmed" }));
  expect(c.observer.current(terminal(), "terminal").stale).toBe(true);
  expect(c.observer.current(terminal({ childrenStop: { ...stopReport(), version: 4 } }), "terminal").stale).toBe(true);
  const tree = c.observer.current(terminal({ status: "interrupted", subagentJoin: undefined,
    stopOutcome: "unconfirmed", childrenStop: stopReport() }), "terminal");
  expect(tree.stale).toBeUndefined();
  expect(tree.stopVersion).toBe(5);
  expect(treeResultText(tree)).toContain("尚未确认停下");
});

test("alreadyDone仅有停止摘要而缺完整报告，仍明确标记陈旧", () => {
  const c = setup(), initial = snapshot(0);
  initial.run = terminal();
  initial.children!.stop = { phase: "unconfirmed", version: 5, unconfirmed: 1, conflicts: 0 };
  c.observer.initialize("run-test", initial, true);
  expect(c.observer.current(terminal(), "terminal").stale).toBe(true);
});

test("终态发现不完整或缺少已观察成员，不能借released隐藏缺失事实", () => {
  const c = setup(); c.observer.initialize("run-test", snapshot());
  c.fake.emit("broker/run/childChanged", event({ revision: 8 }));
  const tree = c.observer.current(terminal({ childrenStop: { ...stopReport(),
    discovery: { complete: false, version: 9, incompleteReasons: ["仍缺成员证据"] } } }), "terminal");
  expect(tree.stale).toBe(true);
  expect(tree.discovery?.complete).toBe(false);
  expect(treeResultText(tree)).toContain("子代理发现不完整：仍缺成员证据");
  expect(c.observer.current(terminal({ children: [] }), "terminal").stale).toBe(true);
  expect(c.observer.current(terminal({ state: "running" }), "terminal").stale).toBe(true);
  expect(c.observer.current(terminal({ subagentJoin: undefined }), "terminal").stale).toBe(true);
});
