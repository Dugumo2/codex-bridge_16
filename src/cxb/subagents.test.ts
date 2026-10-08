import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ASK_INSTRUCTIONS, ASK_RULES, SUBAGENT_RULES, SUBAGENT_RULES_VERSION, askOverrides } from "./ask-instructions";
import { continueRefusal } from "./continue-prompt";
import { EXIT, type ChildrenStopReport, type RunResult } from "./contracts";
import { ls } from "./ls";
import { rm } from "./rm";
import { show } from "./show";
import { stop } from "./stop";
import { stopReportLines, subagentRefusal, unfinishedNotice } from "./subagents";
import { wait } from "./wait";
import { removeTemp } from "./test-fixtures/main-test-support";
import { context, ledger, reasonError, summary } from "./test-fixtures/n4a-unit";
import { runResult } from "./test-fixtures/run-fakes";

let dir: string;
beforeEach(() => { dir = mkdtempSync(join(tmpdir(), "cxb-sab-")); });
afterEach(async () => { await removeTemp(dir); }, 10_000);

function report(patch: Partial<ChildrenStopReport> = {}): ChildrenStopReport {
  return {
    trigger: "userStop", phase: "confirmed", version: 3,
    root: { thread: "thread-test", phase: "confirmed", check: { ownership: true, turnEnded: true, goalBraked: true, settled: true, verifyDone: true },
      clearRequested: false, clearConfirmed: false, error: null },
    stopped: ["child-a", "child-b"], unconfirmed: [], failed: [], conflicts: [], capabilityLimited: [],
    discovery: { complete: true, version: 2, incompleteReasons: [] }, unsettledSpawns: 0, ...patch,
  };
}
const children = (...ids: string[]) => ids.map((threadId) => ({ threadId, parentThreadId: "thread-test", relationKind: "subagent" as const,
  turnIds: ["t1"], evidence: "v1Spawn" as const, finalState: "settled" as const }));

describe("甲：开发者指令", () => {
  test("带版本号的条件段追加在原有提问规则之后，随 askOverrides 登记", () => {
    expect(ASK_INSTRUCTIONS.startsWith(ASK_RULES)).toBe(true);
    expect(ASK_INSTRUCTIONS.endsWith(SUBAGENT_RULES)).toBe(true);
    expect(SUBAGENT_RULES).toContain(`v${SUBAGENT_RULES_VERSION}`);
    for (const text of ["如果本次任务实际使用了子代理", "完成、失败或被明确取消", "等待超时或部分代理完成", "不得猜测答案",
      "不要仅因等待超时而重新派生", "不要求你主动使用子代理"]) expect(SUBAGENT_RULES).toContain(text);
    expect(askOverrides().developerInstructions).toBe(ASK_INSTRUCTIONS);
    // 原有提问规则保留。
    for (const text of ["request_user_input", "update_goal", "blocked"]) expect(ASK_INSTRUCTIONS).toContain(text);
  });
});

describe("停止报告与中断说明", () => {
  test("确认：父线程与子代理分别报告", () => {
    expect(stopReportLines(report())).toEqual(["父线程已停；2 个子代理已停"]);
  });
  test("未确认、冲突与能力限制另列，不写已停止它们", () => {
    const lines = stopReportLines(report({ phase: "unconfirmed", stopped: ["child-a"], unconfirmed: ["child-b"],
      conflicts: [{ thread: "child-c", reason: "属于其他运行" }], capabilityLimited: ["child-d"],
      root: { ...report().root, clearRequested: true } }));
    expect(lines[0]).toBe("父线程已停；1 个子代理已停；1 个尚未确认");
    expect(lines.join("\n")).toContain("冲突：子线程 child-c（属于其他运行），未中断");
    expect(lines.join("\n")).toContain("能力限制：子线程 child-d 不接受 goal 写入，目标仍可能 active，未能清除");
    const notice = unfinishedNotice(runResult({ status: "interrupted", interruptReason: "subagentsUnfinished",
      childrenStop: report({ trigger: "parentFinishedEarly", phase: "unconfirmed", unconfirmed: ["child-b"], stopped: [] }) }), "abcd1234");
    expect(notice.join("\n")).not.toContain("已停止");
    expect(notice.join("\n")).toContain("cxb wait abcd1234");
  });
  test("确认的子代理未完成：说明已停止仍在运行的子代理并保留父轮输出", () => {
    const notice = unfinishedNotice(runResult({ status: "interrupted", interruptReason: "subagentsUnfinished",
      childrenStop: report({ trigger: "parentFinishedEarly", stopped: ["child-a"] }) }), "abcd1234");
    expect(notice[0]).toContain("父轮输出已保留");
    expect(notice[1]).toBe("仍在运行的 1 个子代理已停止。");
  });
});

describe("子线程门禁的退出码", () => {
  test("不接受直接输入退出 1；占用与树停止未确认退出 8；续跑同样映射", () => {
    const refusal = subagentRefusal(reasonError("subagentNoDirectInput", -32000, { parentThreadId: "parent-1" }), "child");
    expect(refusal?.exitCode).toBe(EXIT.USAGE_OR_LOCAL_ERROR);
    expect(refusal?.message).toContain("cxb run --resume parent-1");
    expect(subagentRefusal(reasonError("subagentBusy", -32001), "child")?.exitCode).toBe(EXIT.THREAD_HELD);
    expect(subagentRefusal(reasonError("treeStopping", -32001), "child")?.exitCode).toBe(EXIT.THREAD_HELD);
    expect(subagentRefusal(reasonError("threadBusy", -32001), "child")).toBeNull();
    expect(continueRefusal(reasonError("subagentNoDirectInput"), "child")?.exitCode).toBe(EXIT.USAGE_OR_LOCAL_ERROR);
    expect(continueRefusal(reasonError("treeStopping", -32001), "child")?.exitCode).toBe(EXIT.THREAD_HELD);
  });
});

describe("命令展示", () => {
  test("stop 整树确认：输出「父线程已停；N 个子代理已停」，JSON 带 childrenStop，退出 0", async () => {
    ledger(dir);
    const t = context(dir, ["stop", "abcd1234"]);
    t.fake.handle = (m) => m === "broker/threads/list" ? { threads: [summary()] } : { threadId: "thread-test", runId: "run-test",
      interruptSent: true, goal: "paused", alreadyStopped: false, stopOutcome: "confirmed", run: null, childrenStop: report() };
    expect(await stop(t.ctx)).toBe(EXIT.OK);
    expect(t.texts.join("")).toBe("父线程已停；2 个子代理已停。");
  });
  test("stop 整树未确认：不说已停止，冲突另列，退出 3", async () => {
    ledger(dir);
    const t = context(dir, ["stop", "abcd1234", "--json"]);
    const tree = report({ phase: "unconfirmed", stopped: ["child-a"], unconfirmed: ["child-b"], conflicts: [{ thread: "child-c", reason: "未绑定" }] });
    t.fake.handle = (m) => m === "broker/threads/list" ? { threads: [summary()] } : { threadId: "thread-test", runId: "run-test",
      interruptSent: true, goal: "paused", alreadyStopped: false, stopOutcome: "unconfirmed", run: null, childrenStop: tree };
    expect(await stop(t.ctx)).toBe(EXIT.TIMEOUT);
    expect(t.notices[0]).toBe("已发出停止，尚未全部确认：父线程已停；1 个子代理已停；1 个尚未确认；可再次 cxb stop abcd1234 重试");
    expect(t.notices.join("\n")).toContain("冲突：子线程 child-c");
    expect(t.json[0].childrenStop).toEqual(tree);
    expect(t.json[0].ok).toBe(false);
  });
  test("stop 无子代理时输出与 JSON 不变", async () => {
    ledger(dir);
    const t = context(dir, ["stop", "abcd1234", "--json"]);
    t.fake.handle = (m) => m === "broker/threads/list" ? { threads: [summary()] } : { threadId: "thread-test", runId: "run-test",
      interruptSent: true, goal: "paused", alreadyStopped: false, stopOutcome: "confirmed", run: null };
    expect(await stop(t.ctx)).toBe(EXIT.OK);
    expect(t.json[0]).not.toHaveProperty("childrenStop");
  });
  test("show P：子代理数量、待答数与子代理未完成说明", async () => {
    const run = runResult({ status: "interrupted", interruptReason: "subagentsUnfinished", stopOutcome: "confirmed",
      children: children("child-a", "child-b"), childrenStop: report({ trigger: "parentFinishedEarly", stopped: ["child-a"] }) });
    ledger(dir, [run]);
    writeFileSync(join(dir, "inbox.json"), JSON.stringify({ payload: { items: [
      { id: "ap_1", kind: "approval", threadId: "child-b", runId: "run-test", rootThreadId: "thread-test", status: "pending" },
      { id: "ap_2", kind: "approval", threadId: "other", runId: "run-other", status: "pending" },
    ] } }));
    const t = context(dir, ["show", "abcd1234"]);
    expect(await show(t.ctx)).toBe(0);
    const notices = t.notices.join("\n");
    expect(notices).toContain("子代理 2 个，待答 1 项");
    expect(notices).toContain("运行已标为中断（子代理未完成）");
    expect(notices).toContain("已中断（子代理未完成）");
    expect(t.texts.join("")).toBe("中文结果");
  });
  test("show C：没有运行记录的子线程说明血缘，并从服务器读结果", async () => {
    ledger(dir, [], [summary(), summary({ threadId: "child-a", shortId: "c0ffee01", lastRunId: null, preview: "子任务",
      ...{ parentThreadId: "thread-test", relationKind: "subagent", canAcceptDirectInput: false, agentVersion: "v2" } as object })]);
    const t = context(dir, ["show", "c0ffee01", "--json"]);
    t.fake.handle = (m, p) => {
      if (m === "thread/turns/list") {
        expect(p.threadId).toBe("child-a");
        return { data: [{ id: "turn-c", status: "completed", items: [] }], nextCursor: null, backwardsCursor: null };
      }
      if (m === "thread/items/list") {
        return { data: [{ turnId: "turn-c", item: { type: "agentMessage", text: "子代理结果", phase: "final_answer" } }], nextCursor: null };
      }
      return {};
    };
    expect(await show(t.ctx)).toBe(0);
    expect(t.notices.join("\n")).toContain("子代理线程（父线程 thread-test，v2），不能直接输入");
    expect(t.notices.join("\n")).toContain("cxb run --resume thread-test");
    expect(t.json.at(-1)).toMatchObject({ result: "子代理结果" });
  });
  test("show C：服务器不可用时只输出本地能确定的信息", async () => {
    ledger(dir, [], [summary({ threadId: "child-a", shortId: "c0ffee01", lastRunId: null,
      ...{ parentThreadId: "thread-test", relationKind: "subagent" } as object })]);
    const t = context(dir, ["show", "c0ffee01", "--json"]);
    t.fake.handle = () => { throw reasonError("shuttingDown"); };
    expect(await show(t.ctx)).toBe(0);
    expect(t.json.at(-1)).toMatchObject({ mode: "child", threadId: "child-a", parentThreadId: "thread-test", runId: null });
  });
  test("ls：只作为派生成员的子线程默认隐藏，父行显示数量；有独立运行的子线程照常列出", async () => {
    const child = (id: string, short: string, lastRunId: string | null) => summary({ threadId: id, shortId: short, lastRunId, updatedAt: 1000,
      ...{ parentThreadId: "thread-test", relationKind: "subagent" } as object });
    ledger(dir, [runResult(), runResult({ runId: "run-own", threadId: "child-own", shortId: "c0ffee03" })],
      [summary(), child("child-a", "c0ffee01", null), child("child-b", "c0ffee02", null), child("child-own", "c0ffee03", "run-own")]);
    const t = context(dir, ["ls", "--local", "--json"]);
    expect(await ls(t.ctx)).toBe(0);
    const items = t.json[0].items as Array<{ threadId: string; subagents?: string[] }>;
    expect(items.map((i) => i.threadId).sort()).toEqual(["child-own", "thread-test"]);
    expect(items.find((i) => i.threadId === "thread-test")!.subagents).toEqual(["child-a", "child-b"]);
    const text = context(dir, ["ls", "--local"]);
    expect(await ls(text.ctx)).toBe(0);
    expect(text.texts.join("")).toContain("[子代理 2]");
  });
  test("rm：有子树或子代理在用时逐条列原因并退出 8", async () => {
    ledger(dir);
    const t = context(dir, ["rm", "abcd1234"]);
    t.fake.handle = (m) => {
      if (m === "broker/threads/list") return { threads: [summary()] };
      if (m === "broker/threads/watch") return { thread: summary(), run: runResult() };
      throw reasonError("threadBusy", -32001, { blockers: [
        { threadId: "thread-test", runId: null, reason: "hasSubtree", children: ["child-a"] },
        { threadId: "thread-test", runId: "run-test", reason: "discoveryIncomplete" },
        { threadId: "child-a", runId: "run-test", reason: "subagentInUse" },
      ] });
    };
    expect(await rm(t.ctx)).toBe(EXIT.THREAD_HELD);
    const notices = t.notices.join("\n");
    expect(notices).toContain("有子代理子树");
    expect(notices).toContain("子代理发现不完整");
    expect(notices).toContain("child-a 运行 run-test：是子代理且正被运行使用；cxb wait child-a 或 cxb stop child-a");
  });
  test("wait：带出子代理停止报告与 subagentsUnfinished，退出 4", async () => {
    const run: RunResult = runResult({ status: "interrupted", interruptReason: "subagentsUnfinished", stopOutcome: "confirmed",
      children: children("child-a"), childrenStop: report({ trigger: "parentFinishedEarly", stopped: ["child-a"] }) });
    ledger(dir, [run]);
    const t = context(dir, ["wait", "abcd1234", "--json"]);
    t.fake.handle = (m) => {
      if (m === "broker/threads/list") return { threads: [summary()] };
      if (m === "broker/threads/watch") return { thread: summary(), run, pending: [] };
      return {};
    };
    expect(await wait(t.ctx)).toBe(EXIT.INTERRUPTED);
    expect(t.json[0]).toMatchObject({ status: "interrupted", interruptReason: "subagentsUnfinished", childrenStop: { phase: "confirmed" } });
    expect(t.notices.join("\n")).toContain("运行已标为中断（子代理未完成）");
  });
});
