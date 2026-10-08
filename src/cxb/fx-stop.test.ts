import { afterEach, beforeEach, expect, test } from "bun:test";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { parseArgv } from "./args";
import type { BackgroundCleanup, ChildrenStopReport } from "./contracts";
import { stop } from "./stop";
import { show } from "./show";
import { ls } from "./ls";
import { wait } from "./wait";
import { context, ledger, summary } from "./test-fixtures/n4a-unit";
import { runResult } from "./test-fixtures/run-fakes";
import { removeTemp } from "./test-fixtures/main-test-support";

let dir: string;
beforeEach(() => { dir = mkdtempSync(join(tmpdir(), "cxb-fx-stop-")); });
afterEach(async () => { await removeTemp(dir); }, 10_000);
const background: BackgroundCleanup = { scope: "Codex 受管的后台命令", state: "capabilityLimited", discovered: 2,
  terminationResults: { "11": true, "12": null }, remaining: ["12"], lastVerifiedAt: 1_700_000_000_000, failureReason: "experimental 能力受限" };
function report(): ChildrenStopReport {
  const root = { thread: "thread-test", phase: "unconfirmed" as const, step: "Background", error: "读取失败", lastVerifiedAt: 1_700_000_000_000,
    check: { ownership: true, turnEnded: true, goalBraked: true, settled: false, verifyDone: false }, clearRequested: false, clearConfirmed: false, backgroundCleanup: background };
  return { trigger: "userStop", phase: "unconfirmed", version: 2, root, stopped: [], unconfirmed: ["child-x"], failed: [], conflicts: [],
    capabilityLimited: ["child-x"], discovery: { complete: false, version: 3, incompleteReasons: ["attachmentMissing"], missing: { "child-x": ["attachmentMissing"] } },
    unsettledSpawns: 0, cleanupState: "active", members: { "child-x": { ...root, thread: "child-x" } } };
}

test("fx：abandon 必须绑定运行且不能与 clear 同用", () => {
  expect(() => parseArgv(["stop", "thread-test", "--abandon"])).toThrow("必须带 --run");
  expect(() => parseArgv(["stop", "thread-test", "--run", "r", "--abandon", "--clear"])).toThrow("不能与 --clear");
});

test.each(["confirmed", "unconfirmed"] as const)("fx：单线程清理报告 JSON 与文本完整且退出码为 %s 对应值", async outcome => {
  ledger(dir); const t = context(dir, ["stop", "abcd1234", "--json"]);
  t.fake.handle = m => m === "broker/threads/list" ? { threads: [summary()] } : { threadId: "thread-test", runId: "run-test",
    interruptSent: true, goal: "none", alreadyStopped: false, stopOutcome: outcome, run: null, backgroundCleanup: background };
  expect(await stop(t.ctx)).toBe(outcome === "confirmed" ? 0 : 3);
  expect(t.json[0].backgroundCleanup).toEqual(background);
  for (const value of ["Codex 受管的后台命令", "capabilityLimited", "发现 2", "剩余 12", "experimental", '"11":true', '"12":null']) expect(t.notices.join("\n")).toContain(value);
});

test.each(["abandoned", "alreadyAbandoned", "alreadyConfirmed", "treeStopNotFound"])("fx：abandon %s 返回 3 并区分是否应用", async reason => {
  ledger(dir); const tree = report(); tree.cleanupState = "abandoned"; tree.abandonReason = "显式放弃";
  const t = context(dir, ["stop", "abcd1234", "--run", "run-test", "--abandon", "--json"]);
  t.fake.handle = m => m === "broker/threads/list" ? { threads: [summary()] } : { threadId: "thread-test", runId: "run-test",
    interruptSent: false, goal: "unchanged", alreadyStopped: false, stopOutcome: "unconfirmed", actionApplied: reason === "abandoned", reason,
    run: null, childrenStop: tree };
  expect(await stop(t.ctx)).toBe(3);
  expect(t.fake.calls[1]?.params).toEqual({ threadId: "thread-test", runId: "run-test", abandon: true });
  expect(t.json[0]).toMatchObject({ ok: false, actionApplied: reason === "abandoned", reason, stopOutcome: "unconfirmed" });
  expect(t.notices.join("\n")).not.toContain("可再次 cxb stop");
});

test.each(["show", "ls", "stop", "wait"] as const)("fx：%s 文本和 JSON 保留缺证据成员与核实细节", async command => {
  const tree = report();
  const run = runResult({ status: "interrupted", stopOutcome: "unconfirmed", childrenStop: tree });
  ledger(dir, [run]);
  const t = context(dir, [command, ...(command === "ls" ? ["--local"] : ["abcd1234"]), "--json"]);
  t.fake.handle = m => {
    if (m === "broker/threads/list") return { threads: [summary()] };
    if (m === "broker/threads/watch") return { thread: summary(), run, pending: [] };
    if (m === "broker/threads/stop") return { threadId: "thread-test", runId: "run-test", interruptSent: true,
      goal: "none", alreadyStopped: false, stopOutcome: "unconfirmed", run, childrenStop: tree };
    return {};
  };
  await ({ show, ls, stop, wait })[command](t.ctx);
  const json = command === "ls" ? t.json[0].items[0].childrenStop : t.json.at(-1).childrenStop;
  expect(json).toEqual(tree);
  expect(t.notices.join("\n")).toContain("缺证据：child-x（attachmentMissing）");
  for (const value of ["child-x", "attachmentMissing", "Background", "读取失败", "Codex 受管的后台命令"]) expect(t.notices.join("\n")).toContain(value);
});
