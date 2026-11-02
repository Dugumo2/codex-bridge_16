import { describe, expect, test } from "bun:test";
import type { ChildRecord, ChildrenStopReport, RootStopReport } from "./contracts";
import { buildTree, treeChanges, treeLines, treeResultText } from "./subagent-tree";
import { runResult } from "./test-fixtures/run-fakes";
import type { TreeNodeResult, TreeSnapshot, WatchTreeMember } from "./tree-contracts";

const child = (id: string, parent = "root", patch: Partial<ChildRecord> = {}): ChildRecord => ({
  threadId: id, parentThreadId: parent, relationKind: "subagent", turnIds: ["t1"], evidence: "v1Spawn", finalState: "settled", ...patch,
});
const member = (id: string, patch: Partial<WatchTreeMember> = {}): WatchTreeMember => ({
  threadId: id, parentThreadId: "root", relationKind: "subagent", activity: "running", pendingCount: 0, conflict: null, turnIds: ["t2"], ...patch,
});
const snapshot = (members: WatchTreeMember[], patch: Partial<NonNullable<TreeSnapshot["children"]>> = {}): TreeSnapshot => ({
  revision: 4, children: { members, discovery: null, inFlightSpawns: null, stop: null, ...patch },
});
const result = (patch: Partial<TreeNodeResult> = {}): TreeNodeResult => ({
  turnId: "t1", status: "completed", source: "server", availability: "available", output: "完成正文", outputSource: "finalAnswer", truncated: false, reason: null, ...patch,
});
const report = (patch: Partial<ChildrenStopReport> = {}): ChildrenStopReport => ({
  trigger: "userStop", phase: "confirmed", version: 3,
  root: { thread: "root", phase: "confirmed", check: { ownership: true, turnEnded: true, goalBraked: true, settled: true, verifyDone: true },
    clearRequested: false, clearConfirmed: false, error: null },
  stopped: [], unconfirmed: [], failed: [], conflicts: [], capabilityLimited: [],
  discovery: { complete: true, version: 2, incompleteReasons: [] }, unsettledSpawns: 0, ...patch,
});

describe("树成员与层级纯投影", () => {
  test("空在线快照保留历史成员，不从短 ID 索引扩张成员", () => {
    const tree = buildTree({ rootThreadId: "root", run: runResult({ children: [child("history-child")] }),
      snapshot: snapshot([]), shortIds: new Map([["history-child", "12345678"], ["old-unmanaged", "87654321"]]) });
    expect(tree.nodes.map((node) => node.threadId)).toEqual(["history-child"]);
    expect(tree.nodes[0]!.shortId).toBe("12345678");
    expect(tree.nodes[0]!.activity).toBe("settled");
    expect(tree.source).toBe("mixed");
    expect(tree.discovery).toBeNull();
    expect(tree.inFlightSpawns).toBeNull();
  });

  test("合并成员与绑定集合，在线活动独立覆盖历史；不修改输入", () => {
    const run = runResult({ children: [child("a", "root", { turnIds: ["z", "a"] }), child("b")] });
    const online = snapshot([member("a", { turnIds: ["b", "z"], pendingCount: 2 })]);
    const before = JSON.stringify({ run, online });
    const tree = buildTree({ rootThreadId: "root", run, snapshot: online });
    expect(tree.nodes.map((node) => node.threadId)).toEqual(["a", "b"]);
    expect(tree.nodes[0]!.turnIds).toEqual(["a", "b", "z"]);
    expect(tree.nodes[0]!.activity).toBe("running");
    expect(tree.nodes[0]!.pendingCount).toBe(2);
    expect(JSON.stringify({ run, online })).toBe(before);
  });

  test("同层稳定排序、后代紧随父节点、缺父另组", () => {
    const tree = buildTree({ rootThreadId: "root", run: runResult({ children: [child("z"), child("b", "a"), child("c", "missing"), child("a")] }) });
    expect(tree.nodes.map((node) => node.threadId)).toEqual(["a", "b", "z", "c"]);
    expect(tree.nodes[3]!.hierarchyIssue).toBe("missingParent");
    const lines = treeLines(tree, "rootshort");
    expect(lines[0]).toBe("rootshort");
    expect(lines[2]).toStartWith("    └─ b");
    expect(lines).toContain("  父节点信息缺失：");
    expect(tree.nodes[3]!.parentThreadId).toBe("missing");
  });

  test("终态记录绑定集覆盖在线缓存，正文定位不混入其他轮次", () => {
    const tree = buildTree({ rootThreadId: "root", view: "terminal", run: runResult({ children: [child("a", "root", { turnIds: ["bound-final"] })] }),
      snapshot: snapshot([member("a", { turnIds: ["unverified-cache"] })]) });
    expect(tree.nodes[0]!.turnIds).toEqual(["bound-final"]);
    expect(tree.nodes[0]!.activity).toBe("settled");
  });

  test("循环与自环只切投影，每成员恰好一次且稳定", () => {
    const children = [child("b", "a"), child("a", "b"), child("c", "b"), child("self", "self")];
    const tree = buildTree({ rootThreadId: "root", run: runResult({ children }) });
    const reversed = buildTree({ rootThreadId: "root", run: runResult({ children: [...children].reverse() }) });
    expect(tree.nodes).toEqual(reversed.nodes);
    expect(tree.nodes.map((node) => node.threadId)).toEqual(["a", "b", "c", "self"]);
    expect(tree.nodes.filter((node) => node.hierarchyIssue === "cycle").map((node) => node.threadId)).toEqual(["a", "b", "self"]);
    expect(tree.nodes[0]!.parentThreadId).toBe("b");
    expect(treeLines(tree).join("\n")).toContain("父子关系异常");
  });

  test("fork 排除、review 保留为结构节点且不计结果覆盖", () => {
    const tree = buildTree({ rootThreadId: "root", run: runResult({ reviewThreadId: "review", reviewShortId: "aabbccdd",
      children: [child("fork", "root", { relationKind: "fork" }), child("agent", "review")] }),
      results: new Map([["agent", result()], ["review", result()]]) });
    expect(tree.nodes.map((node) => node.threadId)).toEqual(["review", "agent"]);
    expect(tree.nodes[0]!.relationKind).toBe("reviewExecution");
    expect(tree.nodes[0]!.result).toBeNull();
    expect(tree.outputCoverage.total).toBe(1);
    expect(treeResultText(tree)).toContain("aabbccdd  review 执行线程（结构节点）");
  });

  test("缺短 ID 必须使用完整 threadId", () => {
    const id = "long-thread-identifier-with-no-short-id";
    const tree = buildTree({ rootThreadId: "root", snapshot: snapshot([member(id)]) });
    expect(tree.nodes[0]!.shortId).toBeNull();
    expect(treeLines(tree).join("\n")).toContain(id);
    expect(treeResultText(tree)).toContain(id);
  });
});

describe("活动、结果与停止证据保持独立", () => {
  test("settled 只说明活动落定，不伪造任务完成", () => {
    const tree = buildTree({ rootThreadId: "root", run: runResult({ children: [child("a")] }) });
    expect(tree.nodes[0]!.result).toBeNull();
    expect(treeLines(tree).join("\n")).toContain("活动已落定");
    expect(treeLines(tree).join("\n")).not.toContain("完成");
    expect(treeResultText(tree)).toContain("任务结果未知");
  });

  test("核实失败与能力受限不能解释为任务失败，结果完成可同时存在", () => {
    const stop = report({ failed: ["a"], unconfirmed: ["a"], capabilityLimited: ["a"], conflicts: [{ thread: "a", reason: "未知归属" }] });
    const tree = buildTree({ rootThreadId: "root", run: runResult({ children: [child("a")], childrenStop: stop }), results: new Map([["a", result()]]) });
    const text = treeResultText(tree);
    expect(text).toContain("a  完成");
    expect(text).toContain("核实失败");
    expect(text).toContain("尚未确认停下");
    expect(text).toContain("能力受限");
    expect(text).not.toContain("任务失败");
    expect(tree.nodes[0]!.result!.status).toBe("completed");
  });

  test("选择较新停止版本，状态独立保留未知", () => {
    const tree = buildTree({ rootThreadId: "root", run: runResult({ children: [child("a")], childrenStop: report({ version: 8, stopped: ["a"] }) }),
      snapshot: snapshot([member("a")], { stop: report({ version: 7, unconfirmed: ["a"] }) }) });
    expect(tree.stopVersion).toBe(8);
    expect(tree.nodes[0]!.stop).toMatchObject({ phase: "confirmed", source: "runRecord" });
    expect(tree.nodes[0]!.result).toBeNull();
    expect(treeResultText(tree)).toContain("已确认停下");
  });

  test("后台核验与最近受管轮完成并存", () => {
    const base = report();
    const memberReport: RootStopReport = { ...base.root, thread: "a", phase: "unconfirmed",
      backgroundCleanup: { scope: "managed", state: "unconfirmed", discovered: 1, remaining: ["command-1"], terminationResults: {}, lastVerifiedAt: null, failureReason: null } };
    const tree = buildTree({ rootThreadId: "root", run: runResult({ children: [child("a")], childrenStop: report({ members: { a: memberReport } }) }),
      results: new Map([["a", result()]]) });
    expect(treeResultText(tree)).toContain("a  完成");
    expect(treeResultText(tree)).toContain("尚未确认");
    expect(treeResultText(tree)).toContain("command-1");
  });

  test("发现状态按独立 version 选较新，不被旧在线快照覆盖", () => {
    const newer = { complete: false, version: 9, incompleteReasons: ["停止时发现缺证据"] };
    const tree = buildTree({ rootThreadId: "root", view: "terminal",
      run: runResult({ children: [child("a")], childrenStop: report({ version: 2, discovery: newer }) }),
      snapshot: snapshot([member("a")], { discovery: { complete: true, version: 4, incompleteReasons: [] },
        stop: report({ version: 7, discovery: { complete: true, version: 3, incompleteReasons: [] } }) }) });
    expect(tree.stopVersion).toBe(7);
    expect(tree.discovery).toEqual(newer);
    expect(treeLines(tree).join("\n")).toContain("停止时发现缺证据");
  });

  test("覆盖四类互斥，状态已知但正文不可用仍保留状态", () => {
    const tree = buildTree({ rootThreadId: "root", run: runResult({ children: ["a", "b", "c", "d"].map((id) => child(id)) }), results: new Map([
      ["a", result()], ["b", result({ availability: "truncated", truncated: true })],
      ["c", result({ availability: "unavailable", output: "", reason: "分页失败" })],
      ["d", result({ availability: "omitted", status: null, source: null, output: "", reason: "线程限额" })],
    ]) });
    expect(tree.outputCoverage).toEqual({ total: 4, available: 1, truncated: 1, unavailable: 1, omitted: 1 });
    expect(tree.nodes.find((node) => node.threadId === "c")!.result!.status).toBe("completed");
    const text = treeResultText(tree);
    for (const phrase of ["正文已截断", "正文不可用：分页失败", "正文已省略：线程限额"]) expect(text).toContain(phrase);
  });
});

describe("树文本与变化", () => {
  test("增量只显示变化节点，附直接父标识；待处理不写需要用户回答", () => {
    const previous = buildTree({ rootThreadId: "root", snapshot: snapshot([member("a"), member("b", { parentThreadId: "a" })]), shortIds: new Map([["a", "short-a"]]) });
    const next = buildTree({ rootThreadId: "root", snapshot: snapshot([member("a"), member("b", { parentThreadId: "a", activity: "settled", pendingCount: 2 })]), shortIds: new Map([["a", "short-a"]]) });
    expect(treeChanges(previous, previous)).toEqual([]);
    const lines = treeChanges(previous, next);
    expect(lines).toHaveLength(1);
    expect(lines[0]).toStartWith("    b（父 short-a）");
    expect(lines[0]).toContain("活动已落定");
    expect(lines[0]).toContain("待处理 2 项");
    expect(lines[0]).not.toContain("回答");
  });

  test("全局发现与在途数变化可见，未知不伪装完整", () => {
    const unknown = buildTree({ rootThreadId: "root" });
    expect(treeLines(unknown).join("\n")).toContain("发现完整性未知");
    const incomplete = buildTree({ rootThreadId: "root", snapshot: snapshot([], { discovery: { complete: false, version: 1, incompleteReasons: ["缺证据"] }, inFlightSpawns: 1 }) });
    expect(treeChanges(unknown, incomplete).join("\n")).toContain("子代理发现不完整：缺证据");
    expect(treeChanges(unknown, incomplete).join("\n")).toContain("在途派生 1 个");
    const complete = buildTree({ rootThreadId: "root", snapshot: snapshot([], { discovery: { complete: true, version: 2, incompleteReasons: [] }, inFlightSpawns: 0 }) });
    expect(treeLines(complete).join("\n")).toContain("本次运行没有子代理");
    expect(treeChanges(incomplete, complete).join("\n")).toContain("子代理发现已完整");
  });

  test("300 Unicode 字符摘要不拆代理对，JSON 正文保持原文", () => {
    const output = "😀".repeat(301);
    const tree = buildTree({ rootThreadId: "root", run: runResult({ children: [child("a")] }), results: new Map([["a", result({ output })]]) });
    const body = treeResultText(tree).split("\n").find((line) => line.includes("😀"))!.trim();
    expect(Array.from(body)).toHaveLength(300);
    expect(body).toBe("😀".repeat(299) + "…");
    expect(body).not.toMatch(/[\ud800-\udfff]/u);
    expect(tree.nodes[0]!.result!.output).toBe(output);
  });

  test("终端控制字符与注入换行标签清理；正文最多保留少量换行", () => {
    const id = "child\u001b[2J\u009b2J\r\nforged";
    const output = "第一行\u001b[31m\u009b31m\r第二行\n第三行\n第四行\n第五行";
    const tree = buildTree({ rootThreadId: "root", snapshot: snapshot([member(id, { conflict: "冲突\n伪造\u001b" })]), results: new Map([[id, result({ output })]]) });
    const lines = treeLines(tree);
    expect(lines).toHaveLength(2);
    expect(lines.join("\n")).not.toMatch(/[\x00-\x09\x0b-\x1f\x7f-\x9f]/);
    expect(lines[1]).not.toContain("\n");
    const text = treeResultText(tree);
    expect(text).not.toMatch(/[\x00-\x09\x0b-\x1f\x7f-\x9f]/);
    expect(text).not.toContain("第五行");
    expect(text).toContain("第四行…");
    expect(tree.nodes[0]!.threadId).toBe(id);
    expect(tree.nodes[0]!.result!.output).toBe(output);
  });
});
