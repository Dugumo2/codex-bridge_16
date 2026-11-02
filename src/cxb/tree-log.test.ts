import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ownLogLines, readRunLog, readRunLogSnapshot, type LogReadIO } from "./ledger";
import { show } from "./show";
import { treeLogLine, treeTimeline } from "./tree-log";
import type { TreeLogPayload } from "./tree-log-types";
import { context, ledger, summary } from "./test-fixtures/n4a-unit";
import { runResult } from "./test-fixtures/run-fakes";
import { removeTemp } from "./test-fixtures/main-test-support";

let dir: string;
beforeEach(() => { dir = mkdtempSync(join(tmpdir(), "cxb-tree-log-")); mkdirSync(join(dir, "runs")); });
afterEach(async () => { await removeTemp(dir); }, 10_000);
const ts = "2026-11-02T00:00:00.000Z";
const payload = (patch: Partial<TreeLogPayload> = {}): TreeLogPayload => ({
  eventId: "42:1", rootThreadId: "thread-test", threadId: "thread-test", parentThreadId: null,
  shortId: "abcd1234", sourceRole: "run", turnId: null, itemId: null, summary: "摘要", truncated: false, ...patch,
});
const line = (patch: Partial<TreeLogPayload> = {}, event = "轮次开始", stamp = ts, runId = "run-test") =>
  `${stamp} ${runId} ${event} @cxb2 ${JSON.stringify(payload(patch))}\n`;
const write = (text: string, suffix = "") => writeFileSync(join(dir, "runs", `abcd1234.log${suffix}`), text);

describe("树日志解析与静态保留窗口", () => {
  test("新旧混合还原正文并严格校验结构，长 JSON 不截断", () => {
    const long = "中文\\\"".repeat(300);
    write(`${ts} run-test 运行开始 旧摘要\n` + line({ summary: long, truncated: true }));
    const rows = readRunLog(dir, "abcd1234")!;
    expect(rows[0]!.source).toBeUndefined();
    expect(rows[1]!.content).toBe(long);
    expect(rows[1]!.source?.truncated).toBe(true);
    for (const bad of ["@cxb2", "@cxb2 {", "@cxb2\t{", `@cxb2 ${JSON.stringify({ ...payload(), truncated: "false" })}`,
      `@cxb2 ${JSON.stringify({ ...payload(), threadId: "" })}`]) {
      write(`${ts} run-test 轮次开始 ${bad}\n`);
      expect(() => readRunLog(dir, "abcd1234")).toThrow("损坏的 @cxb2 行");
    }
  });
  test("轮转备份先于活动文件，同事件 ID 去重但同毫秒同正文的不同事件保留", () => {
    write(line({ eventId: "42:0", summary: "仅备份保留" }) + line({ eventId: "42:1" }), ".1");
    write(line({ eventId: "42:1" }) + line({ eventId: "42:2" }));
    const read = readRunLogSnapshot(dir, "abcd1234");
    expect(read.lines!.map((row) => row.source!.eventId)).toEqual(["42:0", "42:1", "42:2"]);
    expect(read.issues).toEqual([{ code: "rotationWindow" }]);
    const timeline = treeTimeline("thread-test", read.lines!, [runResult()], read.issues);
    expect(timeline.items.map((item) => item.eventId)).toEqual(["42:0", "42:1", "42:2"]);
  });
  test("轮转中有限重试，不把旧活动文件和新备份拼在一起", () => {
    let changed = false, reads = 0;
    const io: LogReadIO = {
      fingerprint: (path) => path.endsWith(".1") ? changed ? "旧活动" : null : changed ? "新活动" : "旧活动",
      read(path) { reads++; if (!changed) { changed = true; return line({ eventId: "42:1" }); }
        return line({ eventId: path.endsWith(".1") ? "42:1" : "42:2" }); },
    };
    const result = readRunLogSnapshot(dir, "abcd1234", io);
    expect(reads).toBe(3);
    expect(result.lines!.map((row) => row.source!.eventId)).toEqual(["42:1", "42:2"]);
    expect(result.issues).toContainEqual({ code: "rotationWindow" });
  });
  test("轮转期间 ENOENT 可重试，持续变化三次后退出 1", () => {
    let missing = true;
    const io: LogReadIO = { fingerprint: () => "存在", read() {
      if (missing) { missing = false; throw Object.assign(new Error("轮转"), { code: "ENOENT" }); }
      return line();
    } };
    expect(readRunLogSnapshot(dir, "abcd1234", io).lines).toHaveLength(1);
    let count = 0;
    try {
      readRunLogSnapshot(dir, "abcd1234", { fingerprint: () => String(count++), read: () => line() });
      throw new Error("应拒绝不稳定快照");
    } catch (error) { expect(error).toMatchObject({ exitCode: 1 }); }
    expect(count).toBe(12);
  });
  test("稳定尾半行有覆盖提示，完整坏行报错，读取失败不会当作日志缺失", () => {
    write(line() + `${ts} run-test 条目完成 @cxb2 {`);
    expect(readRunLogSnapshot(dir, "abcd1234")).toMatchObject({ lines: [{ source: { eventId: "42:1" } }], issues: [{ code: "trailingPartialLine" }] });
    write("坏行\n");
    expect(() => readRunLog(dir, "abcd1234")).toThrow("损坏的行");
    expect(() => readRunLogSnapshot(dir, "abcd1234", {
      fingerprint: () => "存在", read: () => { throw Object.assign(new Error("禁止"), { code: "EACCES" }); },
    })).toThrow("无法读取");
  });
  test("文件级丢行标记始终保留，包括运行首尾之外", () => {
    write(`${ts} - 丢行 丢了 3 行\n` + line() + `${ts} - 丢行 丢了 4 行\n`);
    const result = treeTimeline("thread-test", readRunLog(dir, "abcd1234")!, [runResult()]);
    expect(result.items.map((row) => row.kind)).toEqual(["丢行", "轮次开始", "丢行"]);
    expect(result.coverage.issues).toEqual([{ code: "droppedLines" }]);
    expect(result.items[0]).toMatchObject({ sourceRole: "log", runId: null, threadId: null });
  });
  test("按时间排序、同毫秒按追加顺序；跨根事件不接纳；缺运行记录仍保留", () => {
    write(line({ eventId: "42:3" }, "轮次完成", "2026-11-02T00:00:02.000Z")
      + line({ eventId: "42:1", summary: "相同正文" }) + line({ eventId: "42:2", summary: "相同正文" })
      + line({ eventId: "42:4", rootThreadId: "其他根" }));
    const result = treeTimeline("thread-test", readRunLog(dir, "abcd1234")!, []);
    expect(result.items.map((row) => row.eventId)).toEqual(["42:1", "42:2", "42:3"]);
    expect(result.coverage.issues).toEqual([{ code: "metadataUnavailable", runId: "run-test" }]);
  });
  test("旧来源未知与候选暂存缺口、发现不完整保持稳定代码", () => {
    write(`${ts} run-test 运行开始 旧\n` + line({ eventId: "42:2", coverageIssue: "beforeBindingUnknown", late: true })
      + line({ eventId: "42:3", coverageIssue: "discoveryIncomplete" }));
    const result = treeTimeline("thread-test", readRunLog(dir, "abcd1234")!, [runResult()]);
    expect(result.coverage.issues.map((issue) => issue.code)).toEqual(["legacySourceUnknown", "beforeBindingUnknown", "discoveryIncomplete"]);
    expect(result.items[0]!.threadId).toBeNull();
    expect(treeLogLine(result.items[0]!, new Map())).toContain("旧执行源未记录");
    expect(result.items[1]!.late).toBe(true);
  });
  test("来源过滤保留旧行、根与 review，并恢复正文", () => {
    write(`${ts} run-test 运行开始 旧\n` + line({ eventId: "42:1" })
      + line({ eventId: "42:2", sourceRole: "execution", threadId: "review" })
      + line({ eventId: "42:3", sourceRole: "subagent", threadId: "child" })
      + line({ eventId: "42:4", coverageIssue: "discoveryIncomplete" }, "记录缺口"));
    expect(ownLogLines(readRunLog(dir, "abcd1234")!).map((row) => row.source?.sourceRole ?? "legacy")).toEqual(["legacy", "run", "execution"]);
  });
});

describe("show 静态树日志离线接口", () => {
  test("JSON 是单对象、完整身份、条目默认隐藏，失败运行仍退出 0", async () => {
    ledger(dir, [runResult({ status: "failed" })]);
    write(line() + line({ eventId: "42:2", threadId: "child-long-id", parentThreadId: "thread-test", shortId: "kid", sourceRole: "subagent" }, "条目完成"));
    for (const full of [false, true]) {
      const t = context(dir, ["show", "abcd", "--log", "--tree", "--json", ...(full ? ["--full"] : [])]);
      expect(await show(t.ctx)).toBe(0);
      expect(t.connects).toBe(0);
      expect(t.cleanup.size).toBe(0);
      expect(t.json).toHaveLength(1);
      expect(t.json[0]).toMatchObject({ tree: true, order: "timestamp", coverage: { scope: "retained", issues: [] } });
      expect(t.json[0].items).toHaveLength(full ? 2 : 1);
      if (full) expect(t.json[0].items[1].threadId).toBe("child-long-id");
    }
  });
  test("tail 含条目，先合并排序后 limit；文本有根、直接父短标签和 review 来源", async () => {
    ledger(dir);
    write(line() + line({ eventId: "42:2", threadId: "child-long-id", parentThreadId: "thread-test", shortId: "kid", sourceRole: "subagent" }, "条目完成")
      + line({ eventId: "42:3", threadId: "grandchild", parentThreadId: "child-long-id", shortId: null, sourceRole: "subagent" }, "轮次完成")
      + line({ eventId: "42:4", threadId: "review", sourceRole: "execution", shortId: "rev" }, "轮次完成"));
    const all = context(dir, ["show", "abcd", "--log", "--tree", "--full"]);
    expect(await show(all.ctx)).toBe(0);
    const text = all.texts.join("");
    expect(text).toContain("[run-test][根 abcd1234]");
    expect(text).toContain("[子 kid ← abcd1234]");
    expect(text).toContain("[子 grandchild ← kid]");
    expect(text).toContain("[执行 rev]");
    const t = context(dir, ["show", "abcd", "--tail", "--tree", "--limit", "3", "--json"]);
    expect(await show(t.ctx)).toBe(0);
    expect(t.json[0].items.map((item: any) => item.eventId)).toEqual(["42:2", "42:3", "42:4"]);
    expect(t.connects).toBe(0);
  });
  test("缺日志、只有旧日志、缺 RunRecord 都返回保留范围提示", async () => {
    ledger(dir, [], [summary()]);
    let t = context(dir, ["show", "abcd", "--log", "--tree", "--json"]);
    expect(await show(t.ctx)).toBe(0);
    expect(t.json[0].coverage.issues).toEqual([{ code: "missingLog" }]);
    write(`${ts} run-test 运行开始 旧\n`);
    t = context(dir, ["show", "abcd", "--log", "--tree", "--json"]);
    expect(await show(t.ctx)).toBe(0);
    expect(t.json[0].runs).toEqual([]);
    expect(t.json[0].items).toHaveLength(1);
    expect(t.json[0].coverage.issues.map((issue: any) => issue.code)).toEqual(["legacySourceUnknown", "metadataUnavailable"]);
    expect(t.connects).toBe(0);
  });
  test("纯派生线程退出 1 并提示根线程；不存在或歧义退出 2；损坏退出 1", async () => {
    ledger(dir, [], [summary(), { ...summary({ threadId: "child", shortId: "cdef1234", lastRunId: null }), parentThreadId: "thread-test" } as ReturnType<typeof summary>]);
    await expect(show(context(dir, ["show", "cdef", "--log", "--tree"]).ctx)).rejects.toMatchObject({ exitCode: 1, message: expect.stringContaining("根线程") });
    await expect(show(context(dir, ["show", "none", "--log", "--tree"]).ctx)).rejects.toMatchObject({ exitCode: 2 });
    write(`${ts} run-test 轮次开始 @cxb2 {\n`);
    await expect(show(context(dir, ["show", "abcd", "--log", "--tree"]).ctx)).rejects.toMatchObject({ exitCode: 1 });
  });
  test("旧 tail 在 limit 之前去掉普通子来源，根和 review 正文与旧格式逐字一致", async () => {
    ledger(dir);
    const content = `${ts} run-test 轮次开始 根内容\n${ts} run-test 条目完成 review内容\n`;
    write(content);
    const before = context(dir, ["show", "abcd", "--tail", "--limit", "2", "--json"]);
    await show(before.ctx);
    write(line({ summary: "根内容" }) + line({ eventId: "42:2", sourceRole: "execution", summary: "review内容", threadId: "review" }, "条目完成")
      + line({ eventId: "42:3", sourceRole: "subagent", summary: "不可泄漏", threadId: "child" })
      + line({ eventId: "42:4", summary: "树发现缺口", coverageIssue: "discoveryIncomplete" }, "记录缺口"));
    const after = context(dir, ["show", "abcd", "--tail", "--limit", "2", "--json"]);
    await show(after.ctx);
    expect(after.json).toEqual(before.json);
  });
  test("旧 log 的摘要筛选与 full 均不泄漏子来源", async () => {
    ledger(dir);
    write(line({ summary: "根" }) + line({ eventId: "42:2", sourceRole: "subagent", summary: "不可泄漏", threadId: "child" }));
    for (const full of [false, true]) {
      const t = context(dir, ["show", "abcd", "--log", "--json", ...(full ? ["--full"] : [])]);
      await show(t.ctx);
      expect(t.json[0].runs[0].items).toEqual([{ ts, kind: "轮次开始", text: "轮次开始 根" }]);
    }
  });
});
