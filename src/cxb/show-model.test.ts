import { afterEach, beforeEach, describe, expect, spyOn, test } from "bun:test";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { RunResult } from "./contracts";
import * as ledgerModule from "./ledger";
import { ls } from "./ls";
import { show } from "./show";
import { context, ledger, summary } from "./test-fixtures/n4a-unit";
import { removeTemp } from "./test-fixtures/main-test-support";
import { runResult } from "./test-fixtures/run-fakes";

let dir: string;
beforeEach(() => { dir = mkdtempSync(join(tmpdir(), "cxb-show-model-")); });
afterEach(async () => { await removeTemp(dir); }, 10_000);
const reroute = { fromModel: "请求-A", toModel: "服务器-B", reason: "容量调度" };
const qualified = (patch: Partial<RunResult> = {}) => runResult({
  meta: { source: "run", model: "请求-A", effort: "high" },
  startConfirmation: { confirmed: true, owned: true, threadId: "thread-test", turnId: "turn-test" },
  creationOrder: { epoch: 1, sequence: 1 }, ...patch,
});

for (const command of ["ls", "show"] as const) describe(`${command} 模型显示`, () => {
  async function display(json = true) {
    const t = context(dir, [...(command === "ls" ? ["ls", "--local"] : ["show", "abcd"]), ...json ? ["--json"] : []]);
    expect(await (command === "ls" ? ls : show)(t.ctx)).toBe(0);
    return { ...t, item: command === "ls" ? t.json[0]?.items[0] : t.json[0], text: [...t.texts, ...t.notices].join("\n") };
  }

  test("合格运行优先于索引，JSON 保留请求值和来源", async () => {
    ledger(dir, [qualified()], [summary({ model: "索引-C" })]);
    const t = await display();
    expect(t.item).toMatchObject({ model: "请求-A", reasoning: "high", modelSource: "run", reroute: null, threadId: "thread-test" });
    const human = await display(false);
    expect(human.text).toContain("请求-A（请求值）");
    expect(human.text).toContain("high");
    expect(human.text).not.toContain("索引-C");
    if (command === "show") expect(human.texts.join("")).toBe("中文结果");
  });

  const invalid: Array<[string, Partial<RunResult>]> = [
    ["joined", { joined: true, startConfirmation: { confirmed: false, owned: false, threadId: null, turnId: null } }],
    ["未确认", { startConfirmation: { confirmed: false, owned: null, threadId: null, turnId: null } }],
    ["旧记录没有确认", { startConfirmation: undefined }],
    ["source=review", { meta: { source: "review", model: "不采用", effort: "low" } }],
    ["无 model", { meta: { source: "run", effort: "low" } }],
    ["确认前失联", { state: "lost", startConfirmation: undefined }],
    ["开轮失败", { status: "failed", turns: [], startConfirmation: undefined }],
  ];
  for (const [name, patch] of invalid) test(`最近运行${name}，回溯更早合格运行`, async () => {
    ledger(dir, [qualified({ runId: "run-earlier" }), qualified({ creationOrder: { epoch: 1, sequence: 2 },
      meta: { source: "run", model: "不采用", effort: "low" }, ...patch })]);
    expect((await display()).item).toMatchObject({ model: "请求-A", reasoning: "high", modelSource: "run" });
  });

  for (const state of ["lost", "ended"] as const) test(`确认后${state}仍合格`, async () => {
    ledger(dir, [qualified({ state, status: "interrupted", stopOutcome: "confirmed" })]);
    expect((await display()).item.modelSource).toBe("run");
  });

  test("无合格运行明确标注线程索引，不取索引元数据的强度", async () => {
    ledger(dir, [runResult()], [summary({ model: "索引-C", meta: { source: "run", effort: "low" } })]);
    expect((await display()).item).toMatchObject({ model: "索引-C", reasoning: null, modelSource: "index", reroute: null });
    expect((await display(false)).text).toContain("索引-C（线程索引）");
  });

  test("review 关联及父子线程不串用请求模型", async () => {
    ledger(dir, [runResult(), qualified({ runId: "run-parent", threadId: "parent", reviewThreadId: "thread-test" }),
      qualified({ runId: "run-child", threadId: "child" })]);
    expect((await display()).item).toMatchObject({ model: "模型", modelSource: "index" });
  });

  test("旧记录与新记录混排，顺序键优先于墙钟和随机 ID", async () => {
    ledger(dir, [qualified({ runId: "run-z-old", creationOrder: undefined, startedAt: 999999,
      meta: { source: "run", model: "旧格式" } }),
      qualified({ runId: "run-z-seq", startedAt: 9999, creationOrder: { epoch: 1, sequence: 99 }, meta: { source: "run", model: "旧进程" } }),
      qualified({ runId: "run-z-first", startedAt: 9999, creationOrder: { epoch: 2, sequence: 1 }, meta: { source: "run", model: "旧序号" } }),
      qualified({ startedAt: 1, creationOrder: { epoch: 2, sequence: 2 } })]);
    expect((await display()).item.model).toBe("请求-A");
  });

  test("全为旧记录时按 startedAt 再按 runId 排序", async () => {
    ledger(dir, [qualified({ creationOrder: undefined, startedAt: 20 }),
      qualified({ runId: "run-z", creationOrder: undefined, startedAt: 20, meta: { source: "run", model: "同刻后者" } }),
      qualified({ runId: "run-zz", creationOrder: undefined, startedAt: 10, meta: { source: "run", model: "更早" } })]);
    expect((await display()).item.model).toBe("同刻后者");
  });

  test("最近轮改道注明，下一轮无改道时不残留", async () => {
    const run = qualified();
    run.turns[0]!.reroute = reroute;
    ledger(dir, [run]);
    expect((await display()).item).toMatchObject({ model: "请求-A", reroute });
    expect((await display(false)).text).toContain("（服务器改道为 服务器-B）");
    run.turns.push({ ...run.turns[0]!, turnId: "下一轮", reroute: null });
    run.turnsTotal++;
    ledger(dir, [run]);
    expect((await display()).item.reroute).toBeNull();
    expect((await display(false)).text).not.toContain("服务器改道");
  });
});

describe("ls 批量与在线快照", () => {
  test("多线程只扫描一次运行目录，索引缺失的服务器线程也能回溯磁盘", async () => {
    ledger(dir, [qualified(), qualified({ runId: "run-other", threadId: "other", meta: { source: "run", model: "另一模型" } })]);
    const t = context(dir, ["ls", "--json"]);
    t.fake.handle = (method) => method === "broker/threads/list" ? { threads: [summary()] }
      : method === "broker/threads/watch" ? { runs: [] }
      : { data: [{ id: "other", updatedAt: 1, model: "错误索引" }], nextCursor: null };
    const scan = spyOn(ledgerModule, "readAllRuns");
    try {
      expect(await ls(t.ctx)).toBe(0);
      expect(scan).toHaveBeenCalledTimes(1);
      expect(t.json[0].items.map((item: any) => item.model)).toEqual(["请求-A", "另一模型"]);
    } finally { scan.mockRestore(); }
  });

  test("在线快照覆盖磁盘同一运行，且从全部内存记录选择未落盘的合格运行", async () => {
    ledger(dir, [qualified({ meta: { source: "run", model: "过期磁盘" } })]);
    const t = context(dir, ["ls", "--json"]);
    t.fake.handle = (method, params) => {
      if (method === "broker/threads/list") return { threads: [summary()] };
      if (method === "thread/list") return { data: [], nextCursor: null };
      expect(params).toEqual({ threadId: "thread-test", includeRuns: true });
      const invalid = qualified({ startConfirmation: undefined });
      return { thread: summary(), run: invalid, runs: [invalid,
        qualified({ runId: "run-memory", creationOrder: { epoch: 1, sequence: 2 } })] };
    };
    expect(await ls(t.ctx)).toBe(0);
    expect(t.json[0].items[0]).toMatchObject({ model: "请求-A", modelSource: "run" });
  });

  test("在线撤销合格资格后退回真实索引，不能误标磁盘请求模型为索引", async () => {
    ledger(dir, [qualified()]);
    const t = context(dir, ["ls", "--json"]);
    t.fake.handle = (method) => method === "broker/threads/list" ? { threads: [summary()] }
      : method === "thread/list" ? { data: [], nextCursor: null }
      : { runs: [qualified({ startConfirmation: undefined })] };
    expect(await ls(t.ctx)).toBe(0);
    expect(t.json[0].items[0]).toMatchObject({ model: "模型", modelSource: "index" });
  });

  test("在线最近运行不合格，仍选磁盘旧记录；下一轮进行中不残留上轮改道", async () => {
    const run = qualified({ state: "running", status: null });
    run.turns[0]!.reroute = reroute;
    ledger(dir, [run]);
    const t = context(dir, ["ls", "--json"]);
    t.fake.handle = (method) => method === "broker/threads/list" ? { threads: [summary()] }
      : method === "thread/list" ? { data: [], nextCursor: null }
      : { thread: summary({ turnId: "新轮" }), runs: [qualified({ runId: "run-failed", startConfirmation: undefined })] };
    expect(await ls(t.ctx)).toBe(0);
    expect(t.json[0].items[0]).toMatchObject({ model: "请求-A", modelSource: "run", reroute: null });
  });

  test("当前轮暂存改道覆盖上一轮结果，但不改变请求模型", async () => {
    ledger(dir, [qualified()]);
    const t = context(dir, ["ls", "--json"]);
    t.fake.handle = (method) => method === "broker/threads/list" ? { threads: [summary()] }
      : method === "thread/list" ? { data: [], nextCursor: null }
      : { thread: summary({ turnId: "当前轮" }), run: qualified({ state: "running", status: null,
        pendingReroute: { ...reroute, turnId: "当前轮" } }) };
    expect(await ls(t.ctx)).toBe(0);
    expect(t.json[0].items[0]).toMatchObject({ model: "请求-A", reasoning: "high", reroute });
  });
});

for (const mode of ["--tail", "--log", "--server", "--follow"]) test(`show ${mode} 线程级 JSON 与提示使用共同来源`, async () => {
  ledger(dir, [qualified()]);
  const t = context(dir, ["show", "abcd", mode, "--json"]);
  t.fake.handle = (method, params) => {
    if (method === "broker/threads/list") return { threads: [summary()] };
    if (method === "broker/threads/watch") {
      expect(params).toEqual({ threadId: "thread-test", includeRuns: true });
      return { thread: summary(), run: runResult(), runs: [qualified({ runId: "run-online", creationOrder: { epoch: 2, sequence: 1 },
        meta: { source: "run", model: "在线-A", effort: "xhigh" } })] };
    }
    return { data: [], nextCursor: null };
  };
  expect(await show(t.ctx)).toBe(0);
  const online = mode === "--server" || mode === "--follow";
  expect(t.json.at(-1)).toMatchObject({ model: online ? "在线-A" : "请求-A", reasoning: online ? "xhigh" : "high", modelSource: "run", reroute: null });
  expect(t.notices.join()).toContain(online ? "在线-A（请求值）" : "请求-A（请求值）");
});

test("show 运行中结果带来源与暂存改道，仍保留退出码 5 和空正文", async () => {
  ledger(dir, [qualified({ state: "running", status: null, pendingReroute: { ...reroute, turnId: "当前轮" } })]);
  const t = context(dir, ["show", "abcd", "--json"]);
  expect(await show(t.ctx)).toBe(5);
  expect(t.json[0]).toMatchObject({ model: "请求-A", reasoning: "high", modelSource: "run", reroute, state: "running" });
  const human = context(dir, ["show", "abcd"]);
  expect(await show(human.ctx)).toBe(5);
  expect(human.texts).toEqual([]);
  expect(human.notices.join()).toContain("（服务器改道为 服务器-B）");
});

test("show 运行明细始终展示该次请求模型强度及各轮和暂存改道", async () => {
  const run = qualified({ state: "running", status: null, startConfirmation: undefined,
    pendingReroute: { ...reroute, turnId: "进行中", toModel: "服务器-C" } });
  run.turns[0]!.reroute = reroute;
  run.turns.push({ ...run.turns[0]!, turnId: "未改道轮", reroute: null });
  ledger(dir, [run]);
  const t = context(dir, ["show", "abcd", "--log", "--json"]);
  expect(await show(t.ctx)).toBe(0);
  expect(t.json[0].runs[0]).toMatchObject({ model: "请求-A", reasoning: "high", modelSource: "run",
    pendingReroute: run.pendingReroute, turns: [{ reroute }, { reroute: null }] });
  const human = context(dir, ["show", "abcd", "--log"]);
  expect(await show(human.ctx)).toBe(0);
  expect(human.texts.join("")).toContain("请求模型：请求-A；请求强度：high");
  expect(human.texts.join("")).toContain("轮次 turn-test（服务器改道为 服务器-B）");
  expect(human.texts.join("")).toContain("运行中轮次 进行中（服务器改道为 服务器-C）");
  expect(human.texts.join("")).toContain("原因：容量调度");
  expect(human.texts.join("")).not.toContain("轮次 未改道轮（服务器改道");
});
