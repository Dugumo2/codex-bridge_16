import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { appendFileSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { show, serverOutput } from "./show";
import { CxbError } from "./contracts";
import { bounded, removeTemp } from "./test-fixtures/main-test-support";
import { context, ledger, logs, summary } from "./test-fixtures/n4a-unit";
import { runResult, wireError } from "./test-fixtures/run-fakes";
import { N4Broker } from "./test-fixtures/n4a-broker";
import { readRunLog } from "./ledger";
import { until } from "./test-fixtures/main-test-support";

let dir: string;
beforeEach(() => { dir = mkdtempSync(join(tmpdir(), "cxb-n4a-show-")); });
afterEach(async () => { await removeTemp(dir); }, 10_000);

describe("show 本地读取", () => {
  test("就绪结果与完整 JSON 字段，完全不连接 broker", async () => {
    ledger(dir);
    const t = context(dir, ["show", "abcd", "--json"]);
    expect(await show(t.ctx)).toBe(0);
    expect(t.connects).toBe(0);
    expect(t.json[0]).toEqual({ ok: true, command: "show", threadId: "thread-test", shortId: "abcd1234", mode: "result", runId: "run-test",
      state: "ended", status: "completed", ready: "ready", result: "中文结果", outputSource: "finalAnswer", interruptReason: null,
      stopOutcome: null, continuedFrom: null, updatedAt: 2000 });
    expect(t.cleanup.size).toBe(0);
  });
  for (const state of ["running", "stopping", "pending"] as const) test(`${state} 提示等待并退出 5`, async () => {
    const run = runResult(state === "pending" ? {} : { state });
    if (state === "pending") run.turns[0]!.ready = "pending";
    ledger(dir, [run]);
    const t = context(dir, ["show", "abcd"]);
    expect(await show(t.ctx)).toBe(5);
    expect(t.texts).toEqual([]);
    expect(t.notices.join()).toContain("cxb wait abcd1234");
  });
  for (const lost of [true, false]) test(`${lost ? "失联" : "已中断"} 给出对应恢复提示且退出 0`, async () => {
    ledger(dir, [runResult({ state: lost ? "lost" : "ended", status: "interrupted", interruptReason: "stop", stopOutcome: lost ? "unconfirmed" : "confirmed" })]);
    const t = context(dir, ["show", "abcd"]);
    expect(await show(t.ctx)).toBe(0);
    expect(t.notices.join()).toContain(lost ? "停止中（未确认）" : "已中断（主动停止）");
    expect(t.notices.join()).toContain(lost ? "cxb stop" : "--continue");
  });
  test("无轮次的启动失败为空结果并提示摘要", async () => {
    ledger(dir, [runResult({ turns: [], turnsTotal: 0, status: "failed" })]);
    logs(dir, ["运行开始 run-test", "启动失败 权限不足 token=隐藏", "运行结束 run-test"]);
    const t = context(dir, ["show", "abcd"]);
    expect(await show(t.ctx)).toBe(0);
    expect(t.texts.join("")).toBe("");
    expect(t.notices.join()).toContain("启动失败");
    expect(t.notices.join()).toContain("权限不足");
    expect(t.notices.join()).not.toContain("隐藏");
  });
  test("无运行记录、歧义分别退出 2；损坏记录和日志退出 1", async () => {
    ledger(dir, [], [summary({ lastRunId: null })]);
    await expect(show(context(dir, ["show", "abcd"]).ctx)).rejects.toMatchObject({ exitCode: 2 });
    ledger(dir, [], [summary(), summary({ shortId: "abcd5678", threadId: "thread-other" })]);
    await expect(show(context(dir, ["show", "abcd"]).ctx)).rejects.toMatchObject({ exitCode: 2 });
    ledger(dir);
    writeFileSync(join(dir, "runs", "run-test.json"), "{}");
    await expect(show(context(dir, ["show", "abcd"]).ctx)).rejects.toMatchObject({ exitCode: 1 });
    writeFileSync(join(dir, "runs", "abcd1234.log"), "损坏\n");
    await expect(show(context(dir, ["show", "abcd", "--tail"]).ctx)).rejects.toMatchObject({ exitCode: 1 });
  });
  test("tail 限制行数、清洗与 JSON；缺日志为空", async () => {
    ledger(dir);
    let t = context(dir, ["show", "abcd", "--tail", "--json"]);
    expect(await show(t.ctx)).toBe(0);
    expect(t.json[0].items).toEqual([]);
    logs(dir, ["运行开始 run-test", "条目完成 \u001b危险", "轮次完成 turn-test"]);
    t = context(dir, ["show", "abcd", "--tail", "--limit", "2", "--json"]);
    expect(await show(t.ctx)).toBe(0);
    expect(t.json[0]).toMatchObject({ mode: "tail", next: null, command: "show" });
    expect(t.json[0].items.map((l: any) => l.text)).toEqual(["条目完成 危险", "轮次完成 turn-test"]);
    expect(t.json[0].items[0].kind).toBe("条目完成");
    expect(t.connects).toBe(0);
  });
  test("log 按运行分段、保留状态，full 加条目及脱敏元数据", async () => {
    ledger(dir, [runResult({ meta: { source: "run", token: "不可见" } }), runResult({ runId: "run-two", startedAt: 3000, input: "第二次要求" })]);
    logs(dir, ["运行开始 run-test", "条目完成 第一次", "轮次完成 t1", "运行结束 run-test", "运行开始 run-two", "条目完成 第二次", "运行结束 run-two"]);
    const normal = context(dir, ["show", "abcd", "--log", "--json"]);
    expect(await show(normal.ctx)).toBe(0);
    expect(normal.json[0].runs.map((r: any) => r.runId)).toEqual(["run-test", "run-two"]);
    expect(normal.json[0].runs[0].items.map((l: any) => l.kind)).toEqual(["运行开始", "轮次完成", "运行结束"]);
    const full = context(dir, ["show", "abcd", "--log", "--full", "--json"]);
    await show(full.ctx);
    expect(full.json[0].runs[0].items).toHaveLength(4);
    expect(full.json[0].runs[1].items).toHaveLength(3);
    expect(JSON.stringify(full.json)).not.toContain("不可见");
    expect(full.json[0].runs[0].progress).toBeDefined();
  });
  test("log 按行内 runId 拆开交错写入的两个运行，丢行标记归入覆盖它的运行，旧格式日志报损坏", async () => {
    ledger(dir, [runResult(), runResult({ runId: "run-two", startedAt: 3000, input: "第二次要求" })]);
    logs(dir, ["运行开始 run-test", "run-two 运行开始 run-two", "条目完成 R甲", "run-two 条目完成 S甲",
      "- 丢行 丢了 3 行", "空转刹车暂停目标 Paused", "运行结束 run-test", "run-two 运行结束 run-two"]);
    const full = context(dir, ["show", "abcd", "--log", "--full", "--json"]);
    expect(await show(full.ctx)).toBe(0);
    const [first, second] = full.json[0].runs.map((run: any) => run.items.map((line: any) => line.text));
    expect(first).toEqual(["运行开始 run-test", "条目完成 R甲", "丢行 丢了 3 行", "空转刹车暂停目标 Paused", "运行结束 run-test"]);
    expect(second).toEqual(["运行开始 run-two", "条目完成 S甲", "丢行 丢了 3 行", "运行结束 run-two"]);
    const normal = context(dir, ["show", "abcd", "--log", "--json"]);
    await show(normal.ctx);
    expect(normal.json[0].runs[0].items.map((line: any) => line.kind)).toEqual(["运行开始", "丢行", "空转刹车暂停目标", "运行结束"]);
    writeFileSync(join(dir, "runs", "abcd1234.log"), "2026-10-06T00:00:00.000Z 运行开始 run-test\n");
    await expect(show(context(dir, ["show", "abcd", "--log"]).ctx)).rejects.toMatchObject({ exitCode: 1 });
  });
  test("启动失败摘要只取该运行的行", async () => {
    ledger(dir, [runResult({ turns: [], turnsTotal: 0, status: "failed" })]);
    logs(dir, ["run-other 启动失败 别的运行的原因", "运行开始 run-test", "启动失败 本运行的原因", "run-other 启动失败 之后别的运行"]);
    const t = context(dir, ["show", "abcd"]);
    expect(await show(t.ctx)).toBe(0);
    expect(t.notices.join()).toContain("本运行的原因");
    expect(t.notices.join()).not.toContain("别的运行");
  });
});

describe("show 真 broker", () => {
  let broker: N4Broker;
  beforeEach(async () => { broker = new N4Broker(); await broker.setup(); }, 25_000);
  afterEach(async () => { await broker.cleanup(); }, 40_000);
  test("服务器最新轮次经真实转发返回", async () => {
    const result = await broker.cli(["show", "server-only-thread", "--server", "--json"]);
    expect(result.code).toBe(0);
    expect(JSON.parse(result.stdout)).toMatchObject({ result: "服务器末轮结果", outputSource: "finalAnswer" });
  }, 30_000);
  test("follow 输出初始日志后等真实运行完成，终态为最后一行", async () => {
    const created = await broker.request("thread/start", { cwd: broker.workspace.root });
    const threadId = created.thread.id;
    const started = await broker.request("turn/start", { threadId, input: [{ type: "text", text: "跟随测试" }] });
    const threads = await broker.request("broker/threads/list");
    const shortId = threads.threads.find((thread: any) => thread.threadId === threadId).shortId;
    await until(() => (readRunLog(broker.workspace.workspaceDir, shortId)?.length ?? 0) > 0, 5000, "日志没有落盘");
    const t = broker.command(["show", shortId, "--follow", "--json"]);
    let initial!: () => void;
    const initialTail = new Promise<void>((resolve) => { initial = resolve; });
    const output = t.ctx.out.resultJson;
    t.ctx.out.resultJson = (value) => { output(value); initial(); };
    const work = show(t.ctx);
    void work.catch(() => undefined);
    try {
      await bounded(initialTail, 5000, "初始 tail 未输出");
      expect(t.json[0].terminal).toBeUndefined();
      await broker.request("n4a/release");
      expect(await bounded(work, 10_000, "真实跟随未结束")).toBe(0);
      expect(t.json.at(-1)).toEqual({ terminal: true, status: "completed", runId: started.runId });
      expect(t.json.some((line) => line.kind === "item/completed")).toBe(true);
    } finally { for (const cleanup of t.cleanup) await cleanup(); await bounded(work.catch(() => undefined), 2000, "跟随取消后未结束"); }
  }, 30_000);
});

describe("show 服务器与跟随", () => {
  test("服务器最近轮次按 turnId 分页，精确载荷与输出优先级", async () => {
    const t = context(dir, ["show", "server-only", "--server", "--json"]);
    t.fake.handle = (method, p) => {
      if (method === "broker/threads/list") return { threads: [] };
      if (method === "thread/turns/list") { expect(p).toEqual({ threadId: "server-only", cursor: null, limit: 1, sortDirection: "desc", itemsView: "summary" }); return { data: [{ id: "last", status: "completed" }] }; }
      expect(p).toEqual({ threadId: "server-only", turnId: "last", cursor: p.cursor, limit: 64, sortDirection: "asc" });
      return { data: [{ turnId: "last", item: p.cursor ? { type: "exitedReviewMode", review: "审查结果" } : { type: "agentMessage", text: "最终回答", phase: "final_answer" } }], nextCursor: p.cursor ? null : "第二页" };
    };
    expect(await show(t.ctx)).toBe(0);
    expect(t.json[0]).toMatchObject({ result: "审查结果", outputSource: "review", shortId: null });
    expect(t.fake.calls).toHaveLength(4);
    expect(t.fake.calls.every((c) => c.options.timeoutMs === 30_000)).toBe(true);
    expect(serverOutput([{ type: "agentMessage", text: "甲" }, { type: "agentMessage", text: "乙", phase: "final_answer" }]).output).toBe("乙");
    expect(serverOutput([{ type: "agentMessage", text: "甲" }, { type: "agentMessage", text: "乙" }]).output).toBe("甲\n乙");
    expect(serverOutput([]).outputSource).toBe("none");
  });
  test("分页不支持才回退完整读取最后轮次；普通错误不回退", async () => {
    for (const code of [-32601, -32603]) {
      const t = context(dir, ["show", "abcd", "--server", "--json"]);
      t.fake.handle = (method) => {
        if (method === "broker/threads/list") return { threads: [summary()] };
        if (method === "thread/turns/list") throw wireError(code, "失败");
        return { thread: { turns: [{ items: [{ type: "agentMessage", text: "旧轮" }] }, { items: [{ type: "agentMessage", text: "末轮" }] }] } };
      };
      if (code === -32601) { expect(await show(t.ctx)).toBe(0); expect(t.json[0].result).toBe("末轮"); }
      else { await expect(show(t.ctx)).rejects.toMatchObject({ exitCode: 7 }); expect(t.fake.calls.some((c) => c.method === "thread/read")).toBe(false); }
    }
  });
  test("服务器输出采用最后一次非空审查，空 final 不遮蔽累积内容", () => {
    expect(serverOutput([{ type: "exitedReviewMode", review: "旧审查" }, { type: "exitedReviewMode", review: "新审查" }]).output).toBe("新审查");
    expect(serverOutput([{ type: "exitedReviewMode", review: "" }, { type: "agentMessage", text: "累积" }, { type: "agentMessage", phase: "final_answer", text: "" }])).toEqual({ output: "累积", outputSource: "accumulated" });
  });
  test("服务器条目错轮、畸形分页或重复游标不得交付部分结果", async () => {
    for (const mode of ["错轮", "畸形", "重复游标"]) {
      const t = context(dir, ["show", "abcd", "--server", "--json"]);
      t.fake.handle = (m) => {
        if (m === "broker/threads/list") return { threads: [summary()] };
        if (m === "thread/turns/list") return { data: [{ id: "last" }] };
        if (mode === "畸形") return { data: [], nextCursor: 17 };
        return { data: [{ turnId: mode === "错轮" ? "other" : "last", item: { type: "agentMessage", text: "不完整" } }], nextCursor: "相同游标" };
      };
      await expect(show(t.ctx)).rejects.toMatchObject({ exitCode: 7 });
      expect(t.json).toEqual([]);
    }
  });
  test("follow 同批完整终态不丢失，旧 running 快照不能覆盖早到结果", async () => {
    ledger(dir);
    const t = context(dir, ["show", "abcd", "--follow", "--json"]);
    let watches = 0;
    const pending = runResult(); pending.turns[0]!.ready = "pending";
    t.fake.handle = (m) => {
      if (m === "broker/threads/list") return { threads: [summary()] };
      if (++watches === 1) {
        t.emit("broker/run/ended", { run: pending });
        t.emit("broker/thread/result", { runId: "run-test", turn: 0, result: runResult().turns[0] });
      }
      return { thread: summary(), run: runResult({ state: "running" }) };
    };
    const work = show(t.ctx);
    try {
      expect(await bounded(work, 1000, "早到终态丢失")).toBe(0);
      expect(t.json.at(-1)).toEqual({ terminal: true, status: "completed", runId: "run-test" });
    } finally { for (const cleanup of t.cleanup) await cleanup(); await bounded(work.catch(() => undefined), 1000, "取消失败"); }
  });
  test("follow 内存快照为空时用 lastRunId 从磁盘取得终态", async () => {
    ledger(dir);
    const t = context(dir, ["show", "abcd", "--follow", "--json"]);
    t.fake.handle = (m) => m === "broker/threads/list" ? { threads: [summary()] } : { thread: summary(), run: null };
    expect(await show(t.ctx)).toBe(0);
    expect(t.json.at(-1)).toMatchObject({ status: "completed", runId: "run-test" });
    expect(t.fake.calls.at(-1)?.params).toEqual({ threadId: "thread-test", runId: "run-test" });
  });
  test("follow 缓冲同批早到通知，初始 tail 在前，pending 补全后最后一行才是终态", async () => {
    ledger(dir);
    logs(dir, ["运行开始 run-test"]);
    const t = context(dir, ["show", "abcd", "--follow", "--json"]);
    const pending = runResult(); pending.turns[0]!.ready = "pending";
    let release!: () => void;
    const barrier = new Promise<void>((resolve) => { release = resolve; });
    let watches = 0;
    t.fake.handle = (method) => {
      if (method === "broker/threads/list") return { threads: [summary()] };
      watches++;
      if (watches === 1) {
        t.emit("turn/started", { threadId: "thread-test", turn: { id: "早到" } });
        t.emit("broker/run/ended", { run: pending });
      } else release();
      return { thread: summary(), run: pending };
    };
    const running = show(t.ctx);
    await bounded(barrier, 1000, "未建立等待");
    expect(t.json[0].text).toBe("运行开始 run-test");
    expect(t.json.some((line) => line.text?.includes("早到"))).toBe(true);
    expect(t.json.some((line) => line.terminal)).toBe(false);
    // R 已结束、只等结果补齐：之后的原始进展即使来自本线程也可能属于后继运行，一律不显示。
    t.emit("item/completed", { threadId: "thread-test", item: { type: "agentMessage", text: "结束后本线程" } });
    t.emit("item/completed", { threadId: "别线程", item: { type: "agentMessage", text: "不应显示" } });
    t.emit("item/completed", { item: { type: "agentMessage", text: "不应显示" } });
    t.emit("broker/thread/result", { runId: "run-test", turn: 0, result: runResult().turns[0] });
    expect(await bounded(running, 1000, "跟随未结束")).toBe(0);
    expect(t.json.at(-1)).toEqual({ terminal: true, status: "completed", runId: "run-test" });
    expect(JSON.stringify(t.json)).not.toContain("结束后本线程");
    expect(JSON.stringify(t.json)).not.toContain("不应显示");
    expect(t.fake.subscriptions + t.any.size + t.cleanup.size).toBe(0);
  });
  test("follow 跟到已迁到子线程的 review：订阅子线程、从日志补迁移空档、只显示子线程进展（任务说明 5.4）", async () => {
    ledger(dir);
    logs(dir, ["运行开始 run-test", "条目开始 读文件"]);
    const t = context(dir, ["show", "abcd", "--follow", "--json"]);
    const reviewRun = runResult({ state: "running", status: null, reviewThreadId: "review-child" });
    let release!: () => void;
    const childWatched = new Promise<void>((resolve) => { release = resolve; });
    t.fake.handle = (method, params) => {
      if (method === "broker/threads/list") return { threads: [summary()] };
      if (params?.threadId === "review-child") {
        // 迁移到订阅之间发生、已写进父线程日志的一行，follow 要补出来。
        appendFileSync(join(dir, "runs", "abcd1234.log"), "2026-10-06T00:00:05.000Z run-test 条目完成 改了文件\n");
        release();
        return { thread: summary({ threadId: "review-child", shortId: "rev12345" }), run: reviewRun, pending: [] };
      }
      return { thread: summary(), run: reviewRun };
    };
    const running = show(t.ctx);
    await bounded(childWatched, 1000, "未订阅子线程");
    await new Promise<void>((resolve) => setImmediate(resolve));
    t.emit("item/completed", { threadId: "thread-test", item: { type: "agentMessage", text: "父线程新运行" } });
    t.emit("item/completed", { threadId: "review-child", item: { type: "agentMessage", text: "子线程进展" } });
    t.emit("broker/run/ended", { run: runResult({ reviewThreadId: "review-child" }) });
    expect(await bounded(running, 1000, "跟随未结束")).toBe(0);
    const text = JSON.stringify(t.json);
    expect(text).toContain("改了文件");
    expect(text).toContain("子线程进展");
    expect(text).not.toContain("父线程新运行");
    expect(t.json.findIndex((l) => l.text?.includes("改了文件"))).toBeLessThan(t.json.findIndex((l) => l.text?.includes("子线程进展")));
    expect(t.json.at(-1)).toMatchObject({ terminal: true, status: "completed" });
  });

  test("follow 收到 broker/run/moved 后才切到子线程：之前认父线程，之后只认子线程", async () => {
    ledger(dir);
    logs(dir, ["运行开始 run-test"]);
    const t = context(dir, ["show", "abcd", "--follow", "--json"]);
    const running0 = runResult({ state: "running", status: null });
    let release!: () => void;
    const childWatched = new Promise<void>((resolve) => { release = resolve; });
    let watches = 0;
    let started!: () => void;
    const parentWatched = new Promise<void>((resolve) => { started = resolve; });
    t.fake.handle = (method, params) => {
      if (method === "broker/threads/list") return { threads: [summary()] };
      if (params?.threadId === "review-child") { release(); return { thread: summary({ threadId: "review-child" }), run: running0, pending: [] }; }
      if (++watches === 2) started();
      return { thread: summary(), run: running0 };
    };
    const runningShow = show(t.ctx);
    await bounded(parentWatched, 1000, "未建立等待");
    t.emit("item/started", { threadId: "thread-test", item: { type: "agentMessage", text: "迁移前父线程进展" } });
    t.emit("broker/run/moved", { threadId: "thread-test", runId: "别的运行", reviewThreadId: "别的子线程" });
    t.emit("broker/run/moved", { threadId: "thread-test", runId: "run-test", reviewThreadId: "review-child" });
    await bounded(childWatched, 1000, "未订阅子线程");
    await new Promise<void>((resolve) => setImmediate(resolve));
    t.emit("item/completed", { threadId: "thread-test", item: { type: "agentMessage", text: "迁移后父线程" } });
    t.emit("item/completed", { threadId: "review-child", item: { type: "agentMessage", text: "子线程进展" } });
    t.emit("broker/run/ended", { run: runResult() });
    expect(await bounded(runningShow, 1000, "跟随未结束")).toBe(0);
    const text = JSON.stringify(t.json);
    expect(text).toContain("迁移前父线程进展");
    expect(text).toContain("子线程进展");
    expect(text).not.toContain("迁移后父线程");
    expect(t.fake.calls.filter((c) => c.method === "broker/threads/watch").map((c) => c.params.threadId)).not.toContain("别的子线程");
  });

  /** 目标运行迁到子线程的跟随：子线程订阅时执行 onChildWatch（可追加日志、扣住回包），返回值为子线程快照的运行。 */
  function followMoved(onChildWatch: () => unknown, initial = ["运行开始 run-test"]) {
    ledger(dir);
    logs(dir, initial);
    const t = context(dir, ["show", "abcd", "--follow", "--json"]);
    const reviewRun = runResult({ state: "running", status: null, reviewThreadId: "review-child" });
    let childWatched!: () => void;
    const watched = new Promise<void>((resolve) => { childWatched = resolve; });
    t.fake.handle = (method, params) => {
      if (method === "broker/threads/list") return { threads: [summary()] };
      if (params?.threadId === "review-child") { childWatched(); return onChildWatch(); }
      return { thread: summary(), run: reviewRun };
    };
    return { t, watched, reviewRun, log: (line: string) => appendFileSync(join(dir, "runs", "abcd1234.log"), `${line}\n`) };
  }
  const texts = (json: any[]) => json.filter((l) => typeof l.text === "string").map((l) => l.text as string);

  test("follow 补发迁移空档时，与父线程新运行交错写入的行按 runId 拆开，只补目标运行的", async () => {
    const f = followMoved(() => {
      // 父线程上的新运行 S 与 R 写同一份日志、交错写入。
      f.log("2026-10-06T00:00:05.000Z run-test 条目完成 R迁移前");
      f.log("2026-10-06T00:00:06.000Z run-s 运行开始 run-s");
      f.log("2026-10-06T00:00:07.000Z run-s 条目完成 S的进展");
      f.log("2026-10-06T00:00:07.000Z run-test 条目完成 R在S之后");
      f.log("2026-10-06T00:00:08.000Z run-s 运行结束 run-s");
      return { thread: summary({ threadId: "review-child" }), run: f.reviewRun, pending: [] };
    });
    const running = show(f.t.ctx);
    await bounded(f.watched, 1000, "未订阅子线程");
    await new Promise<void>((resolve) => setImmediate(resolve));
    f.t.emit("broker/run/ended", { run: runResult({ reviewThreadId: "review-child" }) });
    expect(await bounded(running, 1000, "跟随未结束")).toBe(0);
    const shown = texts(f.t.json);
    expect(shown).toContain("条目完成 R迁移前");
    expect(shown).toContain("条目完成 R在S之后");
    expect(shown.some((text) => text.includes("run-s") || text.includes("S的进展"))).toBe(false);
  });

  for (const [label, initial, appended] of [
    ["目标运行开始时别的运行还没结束", ["运行开始 run-s", "运行开始 run-test"],
      ["2026-10-06T00:00:05.000Z run-s 条目完成 S的进展", "2026-10-06T00:00:06.000Z run-test 条目完成 R的补发",
        "2026-10-06T00:00:07.000Z run-s 运行结束 run-s"]],
    ["段内出现起点已轮转出日志的别的运行的结束", ["运行开始 run-test"],
      ["2026-10-06T00:00:05.000Z run-s 条目完成 S的进展", "2026-10-06T00:00:06.000Z run-test 条目完成 R的补发",
        "2026-10-06T00:00:07.000Z run-s 运行结束 run-s"]],
  ] as const) test(`follow 补发时${label}，仍按 runId 只补目标运行的行`, async () => {
    const f = followMoved(() => {
      for (const line of appended) f.log(line);
      return { thread: summary({ threadId: "review-child" }), run: f.reviewRun, pending: [] };
    }, [...initial]);
    const running = show(f.t.ctx);
    await bounded(f.watched, 1000, "未订阅子线程");
    await new Promise<void>((resolve) => setImmediate(resolve));
    f.t.emit("broker/run/ended", { run: runResult({ reviewThreadId: "review-child" }) });
    expect(await bounded(running, 1000, "跟随未结束")).toBe(0);
    const shown = texts(f.t.json);
    expect(shown).toContain("条目完成 R的补发");
    expect(shown.some((text) => text.includes("S的进展") || text === "运行结束 run-s")).toBe(false);
  });

  test("follow 补发时之前的运行已完整结束（有开始有结束），照常补出目标运行的行", async () => {
    const f = followMoved(() => {
      f.log("2026-10-06T00:00:05.000Z run-test 条目完成 R的补发");
      return { thread: summary({ threadId: "review-child" }), run: f.reviewRun, pending: [] };
    }, ["运行开始 run-old", "运行结束 run-old", "运行开始 run-test"]);
    const running = show(f.t.ctx);
    await bounded(f.watched, 1000, "未订阅子线程");
    await new Promise<void>((resolve) => setImmediate(resolve));
    f.t.emit("broker/run/ended", { run: runResult({ reviewThreadId: "review-child" }) });
    expect(await bounded(running, 1000, "跟随未结束")).toBe(0);
    expect(texts(f.t.json)).toContain("条目完成 R的补发");
  });

  test("follow 子线程订阅失败与终态接近到达时，报错而不是正常结束", async () => {
    ledger(dir);
    logs(dir, ["运行开始 run-test"]);
    const t = context(dir, ["show", "abcd", "--follow", "--json"]);
    const running0 = runResult({ state: "running", status: null });
    let parentReady!: () => void;
    const parentWatched = new Promise<void>((resolve) => { parentReady = resolve; });
    let watches = 0;
    t.fake.handle = (method, params) => {
      if (method === "broker/threads/list") return { threads: [summary()] };
      if (params?.threadId === "review-child") throw wireError(-32603, "子线程订阅失败");
      if (++watches === 2) parentReady();
      return { thread: summary(), run: running0 };
    };
    const runningShow = show(t.ctx);
    await bounded(parentWatched, 1000, "未建立等待");
    t.emit("broker/run/moved", { threadId: "thread-test", runId: "run-test", reviewThreadId: "review-child" });
    t.emit("broker/run/ended", { run: runResult({ reviewThreadId: "review-child" }) });
    const outcome = await bounded(runningShow.then(() => "正常结束", (error) => error), 1000, "跟随未结束");
    expect(outcome).toBeInstanceOf(CxbError);
    expect(t.json.some((line) => line.terminal)).toBe(false);
  });

  test("follow 补发时同一毫秒的多行（含内容相同的行）都补出，已显示过的行不重复", async () => {
    const f = followMoved(() => {
      for (const name of ["甲", "甲", "乙", "丙"]) f.log(`2026-10-06T00:00:05.000Z run-test 条目完成 同毫秒${name}`);
      return { thread: summary({ threadId: "review-child" }), run: f.reviewRun, pending: [] };
    });
    const running = show(f.t.ctx);
    await bounded(f.watched, 1000, "未订阅子线程");
    await new Promise<void>((resolve) => setImmediate(resolve));
    f.t.emit("broker/run/ended", { run: runResult({ reviewThreadId: "review-child" }) });
    expect(await bounded(running, 1000, "跟随未结束")).toBe(0);
    expect(texts(f.t.json).filter((text) => text.startsWith("条目完成 同毫秒"))).toEqual(
      ["条目完成 同毫秒甲", "条目完成 同毫秒甲", "条目完成 同毫秒乙", "条目完成 同毫秒丙"]);
    // 初始 tail 已显示「运行开始」，与补发水位同一时刻，不能再补一遍。
    expect(texts(f.t.json).filter((text) => text === "运行开始 run-test")).toHaveLength(1);
  });

  test("follow 快照里运行在订阅前已结束、只等结果补齐时，之后的原始进展不显示", async () => {
    ledger(dir);
    const t = context(dir, ["show", "abcd", "--follow", "--json"]);
    const pending = runResult(); pending.turns[0]!.ready = "pending";
    let parentReady!: () => void;
    const parentWatched = new Promise<void>((resolve) => { parentReady = resolve; });
    let watches = 0;
    t.fake.handle = (method) => {
      if (method === "broker/threads/list") return { threads: [summary()] };
      if (++watches === 2) parentReady();
      return { thread: summary(), run: pending };
    };
    const running = show(t.ctx);
    await bounded(parentWatched, 1000, "未建立等待");
    t.emit("item/completed", { threadId: "thread-test", item: { type: "agentMessage", text: "后继运行进展" } });
    t.emit("broker/thread/result", { runId: "run-test", turn: 0, result: runResult().turns[0] });
    expect(await bounded(running, 1000, "跟随未结束")).toBe(0);
    expect(JSON.stringify(t.json)).not.toContain("后继运行进展");
    expect(t.json.at(-1)).toEqual({ terminal: true, status: "completed", runId: "run-test" });
  });

  test("follow 在目标运行结束、等结果补齐期间不显示子线程上后继运行的进展", async () => {
    const f = followMoved(() => ({ thread: summary({ threadId: "review-child" }), run: f.reviewRun, pending: [] }));
    const running = show(f.t.ctx);
    await bounded(f.watched, 1000, "未订阅子线程");
    await new Promise<void>((resolve) => setImmediate(resolve));
    f.t.emit("item/completed", { threadId: "review-child", item: { type: "agentMessage", text: "R的进展" } });
    const pending = runResult({ reviewThreadId: "review-child" }); pending.turns[0]!.ready = "pending";
    f.t.emit("broker/run/ended", { run: pending });
    // R 释放子线程后，别的运行 S 接着用它。
    f.t.emit("turn/started", { threadId: "review-child", turn: { id: "S的轮次" } });
    f.t.emit("item/completed", { threadId: "review-child", item: { type: "agentMessage", text: "S的进展" } });
    f.t.emit("broker/thread/result", { runId: "run-test", turn: 0, result: runResult().turns[0] });
    expect(await bounded(running, 1000, "跟随未结束")).toBe(0);
    const text = JSON.stringify(f.t.json);
    expect(text).toContain("R的进展");
    expect(text).not.toContain("S的");
    expect(f.t.json.at(-1)).toMatchObject({ terminal: true, status: "completed" });
  });

  test("follow 的终态等子线程订阅与补发做完再输出，不跳过迁移空档", async () => {
    ledger(dir);
    logs(dir, ["运行开始 run-test"]);
    const t = context(dir, ["show", "abcd", "--follow", "--json"]);
    const running0 = runResult({ state: "running", status: null });
    let releaseChild!: (value: unknown) => void;
    let childWatched!: () => void;
    const watched = new Promise<void>((resolve) => { childWatched = resolve; });
    let parentReady!: () => void;
    const parentWatched = new Promise<void>((resolve) => { parentReady = resolve; });
    let watches = 0;
    t.fake.handle = (method, params) => {
      if (method === "broker/threads/list") return { threads: [summary()] };
      if (params?.threadId === "review-child") {
        childWatched();
        return new Promise((resolve) => { releaseChild = resolve; });
      }
      if (++watches === 2) parentReady();
      return { thread: summary(), run: running0 };
    };
    const runningShow = show(t.ctx);
    await bounded(parentWatched, 1000, "未建立等待");
    // 短 review：迁移通知与完整终态同批到达，子线程订阅还没回包。
    t.emit("broker/run/moved", { threadId: "thread-test", runId: "run-test", reviewThreadId: "review-child" });
    t.emit("broker/run/ended", { run: runResult({ reviewThreadId: "review-child" }) });
    await bounded(watched, 1000, "未订阅子线程");
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(t.json.some((line) => line.terminal)).toBe(false);
    appendFileSync(join(dir, "runs", "abcd1234.log"), "2026-10-06T00:00:05.000Z run-test 条目完成 迁移空档\n");
    releaseChild({ thread: summary({ threadId: "review-child" }), run: runResult({ reviewThreadId: "review-child" }), pending: [] });
    expect(await bounded(runningShow, 1000, "跟随未结束")).toBe(0);
    const gap = t.json.findIndex((line) => line.text === "条目完成 迁移空档");
    expect(gap).toBeGreaterThan(-1);
    expect(gap).toBeLessThan(t.json.findIndex((line) => line.terminal));
    expect(t.json.at(-1)).toMatchObject({ terminal: true, status: "completed" });
  });

  for (const noRun of [false, true]) test(`follow ${noRun ? "从未运行" : "快照已就绪"} 输出初始 tail 后直接终态`, async () => {
    ledger(dir);
    const t = context(dir, ["show", "abcd", "--follow", "--json"]);
    t.fake.handle = (m) => m === "broker/threads/list" ? { threads: [summary()] }
      : { thread: summary({ lastRunId: noRun ? null : "run-test" }), run: noRun ? null : runResult() };
    expect(await show(t.ctx)).toBe(0);
    expect(t.json.at(-1)).toEqual({ terminal: true, status: noRun ? null : "completed", runId: noRun ? null : "run-test" });
    expect(t.fake.calls.filter((c) => c.method === "broker/threads/watch")).toHaveLength(1);
  });
  test("follow 断线退出 6 并撤销全部订阅", async () => {
    ledger(dir);
    const t = context(dir, ["show", "abcd", "--follow"]);
    t.fake.handle = (m) => {
      if (m === "broker/threads/list") return { threads: [summary()] };
      t.fake.disconnect();
      return { thread: summary(), run: runResult({ state: "running" }) };
    };
    await expect(show(t.ctx)).rejects.toMatchObject({ exitCode: 6 });
    expect(t.fake.subscriptions + t.any.size).toBe(0);
  });
  test("取消挂起请求及时退出并清理", async () => {
    const t = context(dir, ["show", "abcd", "--server"]);
    let release!: () => void;
    const sent = new Promise<void>((resolve) => { release = resolve; });
    t.fake.handle = () => { release(); return new Promise(() => {}); };
    const work = show(t.ctx);
    await bounded(sent, 1000, "请求未发出");
    for (const cleanup of t.cleanup) await cleanup();
    await expect(bounded(work, 1000, "取消未生效")).rejects.toBeInstanceOf(CxbError);
    expect(t.cleanup.size).toBe(0);
  });
});
