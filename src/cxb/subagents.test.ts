import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ASK_INSTRUCTIONS, ASK_RULES, SUBAGENT_RULES, SUBAGENT_RULES_VERSION, askOverrides } from "./ask-instructions";
import { continueRefusal } from "./continue-prompt";
import { EXIT, type ChildrenStopReport, type ExitCode, type RunResult, type ServerRequestDeps } from "./contracts";
import { executeRun } from "./run";
import { ls } from "./ls";
import { rm } from "./rm";
import { show } from "./show";
import { stop } from "./stop";
import { stopReportLines, subagentRefusal, unfinishedNotice } from "./subagents";
import { wait } from "./wait";
import { removeTemp } from "./test-fixtures/main-test-support";
import { context, ledger, reasonError, summary } from "./test-fixtures/n4a-unit";
import { modelSummary, runResult } from "./test-fixtures/run-fakes";
import { convergenceLines, observeConvergence } from "./convergence";
import { convergenceFields, resultTurn } from "./run-result";
import { exitCodeForRun, waitRun } from "./wait-run";
import { latestModelRun, readModelRuns } from "./ledger";
import { runModelDisplay } from "./show-model";
import { parseArgv } from "./args";
import { applyUserConfig } from "./user-config";
import { renderHelp } from "./help";
import type { SubagentJoin, TerminalReason } from "./sa1b-contracts";
import { watchdog } from "./test-fixtures/test-watchdog";
import { item } from "./test-fixtures/n4b-support";

let dir: string;
const fakeServerRequests = (): ServerRequestDeps => ({
  attachServerRequests: () => () => {},
  handover: async () => { throw new Error("本用例不应交接"); },
  renderPending: () => { throw new Error("本用例不应等待人工回应"); },
});
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
    expect(lines.join("\n")).toContain("能力限制：子线程 child-d 存在能力限制，目标或受管后台命令尚未核验，未能清除");
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
  test.each(["已知", "未知", "空值"])("run 冷恢复 v2 子线程：%s能力在 resume 前拒绝并退出 1", async (known) => {
    if (known !== "未知") ledger(dir, [], [summary({ threadId: "child-a", shortId: "c0ffee01", lastRunId: null,
      ...{ parentThreadId: "parent-a", canAcceptDirectInput: known === "已知" ? false : null } as object })]);
    const t = context(dir, ["run", "继续", "--resume", "child-a"]);
    t.fake.handle = (m, p) => {
      if (m === "broker/threads/list") return { threads: [] };
      if (m === "thread/read") {
        expect(p).toEqual({ threadId: "child-a" });
        return { thread: { id: "child-a", canAcceptDirectInput: false,
          source: { subAgent: { thread_spawn: { parent_thread_id: "parent-a" } } } } };
      }
      if (m === "thread/resume") throw reasonError("invalidRequest", -32600);
      return {};
    };
    const error = await executeRun(t.ctx, fakeServerRequests(), { command: "run", sandbox: "workspace-write", prompt: "继续" }).catch(e => e);
    expect(error.exitCode).toBe(1);
    expect(error.message).toContain("cxb run --resume parent-a");
    expect(t.fake.calls.some(c => c.method === "thread/read")).toBe(known !== "已知");
    expect(t.fake.calls.some(c => ["thread/resume", "turn/start", "thread/goal/set"].includes(c.method))).toBe(false);
  });
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
  test.each(["畸形轮次", "重复游标", "不支持分页", "条目不完整", "协议错误"])("show C：%s必须失败，不能伪装成功", async (fault) => {
    ledger(dir, [], [summary({ threadId: "child-a", shortId: "c0ffee01", lastRunId: null,
      ...{ parentThreadId: "thread-test" } as object })]);
    const t = context(dir, ["show", "c0ffee01", "--json"]);
    t.fake.handle = (m) => {
      if (m === "thread/turns/list") {
        if (fault === "不支持分页") throw reasonError("unsupported", -32601);
        if (fault === "协议错误") throw reasonError("invalidRequest", -32600);
        const data = [{ id: "turn-c", status: "completed", items: [] }];
        return fault === "畸形轮次" ? { data } : { data, nextCursor: null, backwardsCursor: null };
      }
      return { data: [{ turnId: fault === "条目不完整" ? "别的轮次" : "turn-c", item: { type: "agentMessage", text: "部分结果" } }], nextCursor: "重复" };
    };
    await expect(show(t.ctx)).rejects.toMatchObject({ code: "CODEX_FAILED", exitCode: 7 });
    expect(t.json).toHaveLength(0);
    expect(t.texts).toHaveLength(0);
  });

  test.each(["confirmed", "unconfirmed", "missing"])("run 子代理未完成退出 4：%s报告在普通输出和 JSON 中可见", async (phase) => {
    for (const json of [false, true]) {
      const t = context(dir, ["run", "任务", ...(json ? ["--json"] : [])]);
      const tree = phase === "missing" ? undefined : report({ trigger: "parentFinishedEarly", phase: phase as "confirmed" | "unconfirmed",
        ...(phase === "unconfirmed" ? { stopped: [], unconfirmed: ["child-a"], conflicts: [{ thread: "child-b", reason: "未绑定" }] } : {}) });
      const run = runResult({ status: "interrupted", interruptReason: "subagentsUnfinished", childrenStop: tree,
        stopOutcome: phase === "missing" ? null : phase as "confirmed" | "unconfirmed" });
      t.fake.handle = m => {
        if (m === "thread/start") return { thread: { id: "thread-test" } };
        if (m === "broker/threads/annotate") return { shortId: "abcd1234" };
        if (m === "turn/start") return { runId: "run-test" };
        if (m === "broker/threads/watch") return { thread: summary(), run, pending: [] };
        return {};
      };
      expect(await executeRun(t.ctx, fakeServerRequests(), { command: "run", sandbox: "workspace-write", prompt: "任务" })).toBe(4);
      const notice = t.notices.join("\n");
      expect(notice).toContain("运行已标为中断（子代理未完成）");
      if (phase === "unconfirmed") {
        expect(notice).toContain("尚未确认");
        expect(notice).toContain("冲突");
        expect(notice).not.toContain("已停止。");
      } else if (phase === "confirmed") expect(notice).toContain("父线程已停；2 个子代理已停");
      else expect(notice).toContain("停止报告不可用");
      if (json) {
        expect(t.json.at(-1)).toMatchObject({ interruptReason: "subagentsUnfinished", result: "中文结果" });
        if (tree) expect(t.json.at(-1)).toMatchObject({ childrenStop: tree, stopOutcome: phase });
      } else expect(t.texts.join("")).toBe("中文结果");
    }
  });
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
    expect(t.notices[0]).toBe("停止尚未确认：父线程已停；1 个子代理已停；1 个尚未确认；请先查看下列诊断");
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

const joinState = (patch: Partial<SubagentJoin> = {}): SubagentJoin => ({ phase: "released", wakeupsStarted: 2,
  wakeupAttempts: 3, startRetriesUsed: 1, workEpoch: 4, deliveredEpoch: 4, reconciledEpoch: 4, deadlineAt: 1791702000000,
  resultCoverage: { epoch: 4, total: 3, available: 1, truncated: 1, unavailable: 1, omitted: 1 }, ...patch });
function converged(patch: Partial<RunResult> = {}): RunResult {
  const old = runResult().turns[0]!;
  return runResult({ finalTurnId: "new-final", currentTurnId: "new-final", turnsTotal: 2,
    turns: [{ ...old, turnId: "new-final", output: "新最终父轮", origin: "subagentReconcile",
      reroute: { fromModel: "请求模型", toModel: "新改道", reason: "容量" } }, { ...old, output: "迟到旧父轮" }],
    subagentJoin: joinState(), autoTurnsStarted: 3,
    runBudget: { tokenBudget: 100, usedAtStart: 10, tokensCharged: 42, coverage: "complete", exhausted: false, source: "activityLedger", goalAggregateWatermark: 10 },
    meta: { source: "run", model: "请求模型", effort: "high" }, creationOrder: { epoch: 1, sequence: 1 },
    startConfirmation: { confirmed: true, owned: true, threadId: "thread-test", turnId: "turn-test" }, ...patch });
}
function serveRun(t: ReturnType<typeof context>, run: RunResult, pending: ReturnType<typeof item>[] = []) {
  const connect = t.ctx.connect;
  t.ctx.connect = async () => {
    const connection = await connect();
    return { ...connection, handshake: { userAgent: "模拟 broker", brokerVersion: "0.1.0", protocolVersion: 1, epoch: 1, server: { kind: "private" } } };
  };
  t.fake.handle = (m) => {
    if (m === "thread/start" || m === "thread/resume") return { thread: { id: "thread-test" } };
    if (m === "broker/threads/annotate") return { shortId: "abcd1234" };
    if (m === "turn/start" || m === "review/start") return { runId: "run-test" };
    if (m === "broker/threads/list") return { threads: [summary({ pendingCount: pending.length })] };
    if (m === "broker/threads/watch") return { thread: summary(), run, revision: 5, ...convergenceFields(run), pending };
    if (m === "broker/runs/modelSummaries") return { runs: [modelSummary(run)] };
    return {};
  };
}

describe("SA1b D：最终交付与协调展示", () => {
  for (const command of ["run", "wait", "show", "ls"] as const) for (const json of [false, true]) {
    test(`${command} ${json ? "JSON" : "文本"} 旧补读晚到仍交付新最终父轮`, async () => {
      const run = converged();
      ledger(dir, [run]);
      const args = command === "run" ? ["run", "任务"] : command === "ls" ? ["ls", "--local"] : [command, "abcd1234"];
      const t = context(dir, [...args, ...(json ? ["--json"] : [])]);
      serveRun(t, run);
      const code = command === "run" ? await executeRun(t.ctx, fakeServerRequests(), { command, sandbox: "workspace-write", prompt: "任务" })
        : await ({ wait, show, ls })[command](t.ctx);
      expect(code).toBe(0);
      const output = json ? command === "ls" ? t.json.at(-1).items[0] : t.json.at(-1) : null;
      if (json) {
        expect(output).toMatchObject({ result: "新最终父轮", ...convergenceFields(run) });
        expect(JSON.stringify(output)).not.toContain("迟到旧父轮");
      } else {
        expect(t.texts.join("")).toContain("新最终父轮");
        expect(t.texts.join("")).not.toContain("迟到旧父轮");
      }
      expect(t.notices.join("\n")).toContain("结果覆盖：批次 4，共 3 项，可用 1，截断 1，不可用 1，省略 1");
      expect(t.notices.join("\n")).toContain("本运行新增 42");
      expect(t.fake.subscriptions).toBe(0);
    }, 10000);
  }
  test("show 等待子树只交付标明阶段的结果；最终指针缺失目标时不交付旧结果", async () => {
    for (const missing of [false, true]) {
      const run = converged(missing ? { finalTurnId: "未读到" } : { state: "running", status: null, finalTurnId: undefined, subagentJoin: joinState({ phase: "waitingSubtree" }) });
      ledger(dir, [run]);
      const t = context(dir, ["show", "abcd1234", "--json"]);
      serveRun(t, run);
      expect(await show(t.ctx)).toBe(5);
      if (missing) expect(t.json[0]).not.toHaveProperty("result");
      else {
        // 与 broker selected_parent_result 一致：阶段结果取已确认当前父轮，不取数组尾部的迟到旧轮。
        expect(t.json[0]).toMatchObject({ stageResult: true, result: "新最终父轮" });
        expect(t.notices.join()).toContain("阶段结果");
        expect(t.notices.join()).toContain("等待子代理");
      }
    }
  });
  test("wait 待答保留来源、同运行期限与覆盖，不代答", async () => {
    const run = converged({ state: "running", status: null, finalTurnId: undefined, subagentJoin: joinState({ phase: "waitingSubtree" }) });
    ledger(dir, [run]);
    const t = context(dir, ["wait", "abcd1234", "--json"]);
    const pending = item({ threadId: "孙代理", rootThreadId: "thread-test" });
    serveRun(t, run, [pending]);
    expect(await wait(t.ctx)).toBe(5);
    expect(t.json[0]).toMatchObject(convergenceFields(run));
    expect(JSON.stringify(t.json[0])).toContain("孙代理");
    expect(t.notices.join()).toContain("等待回答（1 项）");
    expect(t.fake.calls.some(c => /reply|resolve/.test(c.method))).toBe(false);
  });
  test("run detach 保留原 deadline 与汇总计数，传独立配置", async () => {
    const run = converged({ state: "running", status: null, finalTurnId: undefined, subagentJoin: joinState({ phase: "waitingSubtree" }) });
    const t = context(dir, ["run", "任务", "--detach", "--json", "--timeout", "30", "--converge-timeout", "77"]);
    serveRun(t, run);
    const deps = { ...fakeServerRequests(), handover: async () => ({ outcome: "detached" as const, turnId: null, pending: [] }) };
    expect(await executeRun(t.ctx, deps, { command: "run", sandbox: "workspace-write", prompt: "任务" })).toBe(0);
    expect(t.json[0]).toMatchObject({ runId: run.runId, ...convergenceFields(run) });
    expect(t.fake.calls.find(c => c.method === "turn/start")!.params.cxbConvergence).toEqual({ timeoutMs: 77000 });
    expect(t.fake.subscriptions).toBe(0);
  });
  test("协调通知先于快照、重复 revision、旧通知和其他运行均不倒退展示", () => {
    const t = context(dir, ["wait", "abcd1234"]);
    let id: string | undefined;
    const seen: string[] = [];
    const observer = observeConvergence(t.fake.rpc, () => id, s => seen.push(s));
    try {
      t.fake.emit("broker/run/convergenceChanged", { runId: "run-test", revision: 9, subagentJoin: joinState({ phase: "reconciling" }) });
      id = "run-test";
      observer.accept({ runId: id, revision: 8, subagentJoin: joinState({ phase: "waitingSubtree" }) });
      for (const revision of [7, 9]) t.fake.emit("broker/run/childChanged", { runId: id, revision, subagentJoin: joinState({ phase: "waitingSubtree" }) });
      t.fake.emit("broker/run/convergenceChanged", { runId: "别的运行", revision: 30, subagentJoin: joinState({ phase: "abnormal" }) });
      expect(seen.join()).toContain("汇总中");
      expect(seen.join()).not.toContain("等待子代理");
      expect(seen.join()).not.toContain("异常收尾");
      t.fake.emit("broker/run/convergenceChanged", { runId: id, revision: 10, subagentJoin: joinState({ phase: "abnormal", terminalReason: "wakeUnknown" }) });
      expect(seen.join()).toContain("汇总启动结果不明（wakeUnknown）");
      t.fake.emit("broker/run/childChanged", { runId: id, revision: 11 });
      expect(observer.fields().subagentJoin?.terminalReason).toBe("wakeUnknown");
    } finally { observer.dispose(); }
    expect(t.fake.subscriptions).toBe(0);
  });
  test("wait 最终目标在数组中段或缺失时只接受对应 turnId 的补读", async () => {
    for (const missing of [false, true]) {
      const t = context(dir, ["wait", "abcd1234"]);
      const run = converged();
      const ready = run.turns[0]!;
      run.turns = missing ? [run.turns[1]!] : [{ ...ready, ready: "pending", output: "" }, run.turns[1]!];
      serveRun(t, run);
      let done = false;
      const work = waitRun(t.fake.rpc, { threadId: run.threadId, runId: run.runId, timeoutMs: 1000 }).then(r => { done = true; return r; });
      t.fake.emit("broker/thread/result", { runId: run.runId, turn: 1, result: run.turns.at(-1) });
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(done).toBe(false);
      t.fake.emit("broker/thread/result", { runId: "其他运行", turn: 0, result: ready });
      expect(done).toBe(false);
      t.fake.emit("broker/thread/result", { runId: run.runId, turn: 0, result: ready });
      expect(resultTurn(await watchdog(work))?.output).toBe("新最终父轮");
      expect(t.fake.subscriptions).toBe(0);
    }
  });
  test("模型沿用不随汇总轮改变原运行创建顺序、模型与强度；账本摘要认最终指针", () => {
    const run = converged();
    const newer = converged({ runId: "run-newer", creationOrder: { epoch: 1, sequence: 2 }, meta: { source: "run", model: "后开模型", effort: "low" } });
    ledger(dir, [run, newer]);
    const stored = readModelRuns(dir, () => {}).runs;
    expect(stored.find(r => r.runId === run.runId)).toMatchObject({ finalTurnId: "new-final", lastTurn: { turnId: "new-final" } });
    expect(latestModelRun(run.threadId, stored)?.runId).toBe(newer.runId);
    expect(runModelDisplay(run)).toMatchObject({ model: "请求模型", reasoning: "high", reroute: { toModel: "新改道" } });
    expect(runModelDisplay(stored.find(r => r.runId === run.runId)!)).toEqual(runModelDisplay(run));
  });
});

const terminals: Array<[TerminalReason | undefined, RunResult["status"], RunResult["interruptReason"], ExitCode]> = [
  ["parentFailed", "failed", null, 7], ["parentInterrupted", "interrupted", null, 4], ["userStop", "interrupted", "stop", 4],
  ["ownerGone", "interrupted", "clientDisconnected", 4], ["budgetExhausted", "goalBlocked", "budgetExhausted", 9],
  ["budgetIncomplete", "goalBlocked", "budgetIncomplete", 9], ["convergeDeadline", "interrupted", "convergeDeadline", 4],
  ["wakeLimit", "goalBlocked", "wakeLimit", 9], ["autoTurnLimit", "goalBlocked", "autoTurnLimit", 9],
  ["noAction", "interrupted", "goalLoop", 9], ["discoveryIncomplete", "interrupted", "discoveryIncomplete", 4],
  ["ownershipConflict", "interrupted", "ownershipConflict", 4], ["wakeUnknown", "interrupted", "wakeUnknown", 4],
  ["wakeFailed", "failed", "wakeFailed", 7], ["goalPaused", "interrupted", "goalInactive", 4],
  ["goalBlocked", "goalBlocked", "goalInactive", 9], ["goalBudgetLimited", "goalBlocked", "budgetExhausted", 9],
  ["goalUsageLimited", "goalBlocked", "goalInactive", 9], ["rootNoDirectInput", "interrupted", "rootNoDirectInput", 4],
  [undefined, "completed", null, 0], ["abandoned", "interrupted", "abandoned", 4], ["brokerShutdown", null, "brokerLost", 4],
];
for (const [terminalReason, status, interruptReason, code] of terminals) test(`SA1b 普通释放与树提交：${terminalReason ?? "正常完成"} 退出 ${code}`, async () => {
  for (const tree of [false, true]) {
    const run = converged({ status, interruptReason, state: terminalReason === "brokerShutdown" ? "lost" : "ended",
      subagentJoin: joinState({ phase: terminalReason ? "abnormal" : "released", terminalReason }),
      ...(tree ? { childrenStop: report({ phase: "unconfirmed", unconfirmed: ["child-a"], stopped: [] }) } : {}) });
    expect(exitCodeForRun(run)).toBe(code);
    ledger(dir, [run]);
    const t = context(dir, ["wait", "abcd1234", "--json"]);
    serveRun(t, run);
    expect(await wait(t.ctx)).toBe(code);
    expect(t.json[0]).toMatchObject({ status, ...convergenceFields(run) });
    if (terminalReason) expect(t.notices.join()).toContain(`（${terminalReason}）`);
  }
});
test("SA1b 父 failed 优先于附带空转报告；lost 不继承旧 completed 成功", () => {
  const loopGuard = { kind: "noAction" as const, count: 3, threshold: 3, turnIds: ["a", "b", "c"], action: "stop" as const, pause: "paused" as const };
  expect(exitCodeForRun(converged({ status: "failed", interruptReason: "goalLoop", loopGuard }))).toBe(7);
  expect(exitCodeForRun(converged({ state: "lost" }))).toBe(4);
});
test("SA1b 收敛期限 CLI/config 独立、优先级与帮助一致", () => {
  const stored = { path: "测试配置", values: { "converge-timeout": 83, timeout: 9 }, unknownKeys: [] };
  for (const command of ["run", "review"]) {
    const parsed = parseArgv([command, "任务"]);
    applyUserConfig(parsed, stored);
    expect(parsed.options["converge-timeout"]).toBe(83);
    expect(parsed.globals.timeout).toBe(9);
    const explicit = parseArgv([command, "任务", "--converge-timeout", "99"]);
    applyUserConfig(explicit, stored);
    expect(explicit.options["converge-timeout"]).toBe(99);
    const help = renderHelp(command as "run" | "review");
    expect(help).toContain("--converge-timeout");
    expect(help).toContain("自动汇总");
    for (const bad of ["0", "-1", "1.5", "1e3", "9007199254741"]) expect(() => parseArgv([command, "任务", "--converge-timeout", bad])).toThrow();
  }
  expect(() => parseArgv(["wait", "--converge-timeout", "1"])).toThrow();
});

test.each(["冷恢复", "已加载恢复"])("SA1b 甲 v1：%s 始终提交并登记同一完整指令，不强制关闭线程", async (mode) => {
  const run = converged();
  ledger(dir, [run]);
  const t = context(dir, ["run", "继续任务", "--resume", "abcd1234", "--json"]);
  serveRun(t, run);
  const original = t.fake.handle;
  t.fake.handle = (method, params) => method === "thread/read"
    ? { thread: { id: run.threadId, canAcceptDirectInput: true, status: { type: mode === "冷恢复" ? "notLoaded" : "idle" } } }
    : original(method, params);
  expect(await executeRun(t.ctx, fakeServerRequests(), { command: "run", sandbox: "workspace-write", prompt: "继续任务" })).toBe(0);
  const resume = t.fake.calls.find(c => c.method === "thread/resume")!.params;
  const registered = t.fake.calls.find(c => c.method === "broker/threads/resumeOverrides")!.params.overrides;
  expect(resume.developerInstructions).toBe(ASK_INSTRUCTIONS);
  expect(registered.developerInstructions).toBe(ASK_INSTRUCTIONS);
  expect(SUBAGENT_RULES_VERSION).toBe(1);
  expect(ASK_INSTRUCTIONS.startsWith(ASK_RULES)).toBe(true);
  expect(ASK_INSTRUCTIONS).toContain("包括仍在执行的后代任务");
  expect(ASK_INSTRUCTIONS).toContain("待答的审批和提问仍属于未完成的工作");
  expect(t.fake.calls.some(c => /close|unload|shutdown/.test(c.method))).toBe(false);
  expect(t.fake.calls.find(c => c.method === "turn/start")!.params).not.toHaveProperty("developerInstructions");
});
test("SA1b review 同样传独立收敛期限；汇总仍属原运行", async () => {
  const t = context(dir, ["review", "检查", "--converge-timeout", "72", "--json"]);
  const run = converged();
  serveRun(t, run);
  expect(await executeRun(t.ctx, fakeServerRequests(), { command: "review", sandbox: "read-only",
    review: { mode: "custom", ref: null, base: null, target: { type: "custom", instructions: "检查" } } })).toBe(0);
  expect(t.fake.calls.find(c => c.method === "review/start")!.params.cxbConvergence).toEqual({ timeoutMs: 72000 });
  expect(t.json[0]).toMatchObject({ runId: "run-test", finalTurnId: "new-final", result: "新最终父轮" });
});
test("SA1b show follow 等最终指向轮补齐后再交付，旧轮不抢终态", async () => {
  const run = converged();
  const ready = run.turns[0]!;
  run.turns[0] = { ...ready, ready: "pending", output: "" };
  ledger(dir, [run]);
  const t = context(dir, ["show", "abcd1234", "--follow", "--json"]);
  serveRun(t, run);
  const original = t.fake.handle;
  t.fake.handle = (m, p) => {
    if (m === "broker/threads/watch" && p.runId) {
      t.emit("broker/thread/result", { runId: run.runId, turn: 1, result: run.turns[1] });
      t.emit("broker/thread/result", { runId: run.runId, turn: 0, result: ready });
    }
    return original(m, p);
  };
  expect(await watchdog(show(t.ctx))).toBe(0);
  expect(t.json.at(-1)).toMatchObject({ terminal: true, result: "新最终父轮", finalTurnId: "new-final" });
  expect(t.fake.subscriptions + t.any.size).toBe(0);
});
test("SA1b run 待答交接输出等待回答与同一协调摘要", async () => {
  const run = converged({ state: "running", status: null, finalTurnId: undefined, subagentJoin: joinState({ phase: "waitingSubtree" }) });
  const t = context(dir, ["run", "任务", "--json"]);
  serveRun(t, run);
  const deps: ServerRequestDeps = { ...fakeServerRequests(),
    attachServerRequests: (_rpc, options) => { options.onNeedsHuman("item/tool/requestUserInput"); return () => {}; },
    handover: async () => ({ outcome: "detached", turnId: "new-final", pending: [item({ threadId: "子代理" })] }),
    renderPending: () => ({ text: "待答事项", json: { pending: true } }),
  };
  expect(await watchdog(executeRun(t.ctx, deps, { command: "run", sandbox: "workspace-write", prompt: "任务" }))).toBe(5);
  expect(t.json[0]).toMatchObject({ pending: true, ...convergenceFields(run) });
  expect(t.notices.join()).toContain("等待回答（1 项）");
  expect(t.fake.subscriptions).toBe(0);
});

test("SA1b ls 明确区分最终结果待补读、不可用及待回答阶段", async () => {
  for (const mode of ["pending", "unavailable", "waiting"] as const) {
    const run = converged(mode === "waiting" ? { state: "running", status: null, finalTurnId: undefined, subagentJoin: joinState({ phase: "waitingSubtree" }) } : {});
    if (mode === "pending") run.finalTurnId = "尚未读到";
    if (mode === "unavailable") run.turns[0] = { ...run.turns[0]!, ready: "unavailable", output: "" };
    ledger(dir, [run]);
    writeFileSync(join(dir, "inbox.json"), JSON.stringify({ payload: { items: [item({ threadId: "子代理" })] } }));
    const t = context(dir, ["ls", "--local", "--json"]);
    expect(await ls(t.ctx)).toBe(0);
    const row = t.json[0].items[0];
    if (mode === "waiting") {
      expect(row).toMatchObject({ stageResult: true, pendingCount: 1 });
      expect(t.notices.join()).toContain("等待回答（1 项）");
    } else {
      expect(row).toMatchObject({ ready: mode, result: "" });
      expect(t.notices.join()).toContain(mode === "pending" ? "结果尚未就绪" : "最终结果不可用");
    }
  }
});

for (const command of ["run", "wait", "show"] as const) test(`SA1b ${command} 最终父输出 unavailable 显式披露并透传 origin`, async () => {
  const run = converged();
  run.turns[0] = { ...run.turns[0]!, ready: "unavailable", output: "" };
  ledger(dir, [run]);
  const t = context(dir, [...command === "run" ? ["run", "任务"] : [command, "abcd1234"], "--json"]);
  serveRun(t, run);
  const code = command === "run" ? await executeRun(t.ctx, fakeServerRequests(), { command, sandbox: "workspace-write", prompt: "任务" })
    : await ({ wait, show })[command](t.ctx);
  expect(code).toBe(0);
  expect(t.json.at(-1)).toMatchObject({ result: "", ready: "unavailable", origin: "subagentReconcile", finalTurnId: "new-final" });
  expect(t.notices.join()).toContain("最终结果不可用（unavailable）");
});
for (const order of ["启用前快照", "通知先于同 revision 快照", "通知先于旧 revision 快照"] as const) test(`SA1b 观察器保留配置：${order}`, async () => {
  const t = context(dir, ["wait", "abcd1234"]);
  const config = { maxWakeups: 8 } as NonNullable<RunResult["convergenceConfig"]>;
  const execution = { model: "原模型", effort: "high" };
  const observer = observeConvergence(t.fake.rpc, () => "run-test", () => {});
  try {
    t.fake.handle = () => ({ run: runResult({ convergenceConfig: config, executionConfig: execution }), revision: order.includes("同 revision") ? 5 : 4 });
    if (order === "启用前快照") {
      await observer.rpc.request("broker/threads/watch", {});
      expect(observer.fields()).not.toHaveProperty("convergenceConfig");
      expect(observer.fields()).not.toHaveProperty("executionConfig");
    }
    t.fake.emit("broker/run/convergenceChanged", { runId: "run-test", revision: 5, subagentJoin: joinState({ phase: "reconciling" }) });
    if (order !== "启用前快照") await observer.rpc.request("broker/threads/watch", {});
    expect(observer.fields()).toMatchObject({ convergenceConfig: config, executionConfig: execution, subagentJoin: { phase: "reconciling" } });
    observer.accept({ runId: "别的运行", revision: 99, executionConfig: { model: "错模型" } });
    observer.accept({ runId: "run-test", revision: 3, subagentJoin: joinState({ phase: "running" }) });
    expect(observer.fields().executionConfig).toEqual(execution);
    expect(observer.fields().subagentJoin?.phase).toBe("reconciling");
  } finally { observer.dispose(); }
});

test("SA1b 等待结束后的迟到快照不再输出或复活协调摘要", async () => {
  const t = context(dir, ["wait", "abcd1234"]);
  const lines: string[] = [];
  let release!: (value: unknown) => void;
  t.fake.handle = () => new Promise(resolve => { release = resolve; });
  const observer = observeConvergence(t.fake.rpc, () => "run-test", line => lines.push(line));
  const request = observer.rpc.request("broker/threads/watch", {});
  observer.dispose();
  release({ run: converged(), revision: 5 });
  await watchdog(request);
  expect(lines).toEqual([]);
  expect(observer.fields()).toEqual({});
  expect(t.fake.subscriptions).toBe(0);
});
