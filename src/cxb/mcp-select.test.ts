// 按线程关闭 MCP（命令面规格 3.1.4 节）：名单解析与选择的纯函数，以及 run、review 实际下发与登记的覆盖。
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { parseArgv } from "./args";
import { brokerVersionOf, connectBroker } from "./broker-conn";
import { BROKER_BIN } from "../test-support/broker-harness";
import { readThreadIndex } from "./ledger";
import { resolveWorkspace } from "./workspace";
import { CONFIG_SCHEMA } from "./config-schema";
import { ASK_FEATURE_KEY, CxbError } from "./contracts";
import type { CommandHandler } from "./main";
import { enabledMcpServers, mcpOffConfig, parseMcpNames, selectMcp } from "./mcp-select";
import { createReviewHandler } from "./review";
import { createRunHandler } from "./run";
import { CLEANUP_BUDGET, TEST_BUDGET, RunHarness, fakeServerRequests, until } from "./test-fixtures/run-harness";
import { applyUserConfig } from "./user-config";

const off = (name: string) => `mcp_servers.${name}.enabled`;

describe("名单解析与选择", () => {
  test("逗号名单去空白、去重并保持顺序；非法名字整体无效", () => {
    expect(parseMcpNames("backlog, serena,backlog")).toEqual(["backlog", "serena"]);
    expect(parseMcpNames("grok-search,node_repl,a.b")).toEqual(["grok-search", "node_repl", "a.b"]);
    for (const bad of ["", "backlog,", "back log", "a/b", "x".repeat(129), "名字"]) expect(parseMcpNames(bad)).toBeNull();
  });

  test("持久配置键：存储值必须是规范化后的名单", () => {
    const spec = CONFIG_SCHEMA["mcp-disabled"];
    expect(spec.parse(" serena ,backlog,serena")).toBe("serena,backlog");
    expect(spec.isValid("serena,backlog")).toBe(true);
    expect(spec.isValid("serena, backlog")).toBe(false);
    expect(() => spec.parse("a b")).toThrow(CxbError);
    expect(CONFIG_SCHEMA["mcp-when-never"].isValid("off")).toBe(true);
    expect(CONFIG_SCHEMA["mcp-when-never"].isValid("all")).toBe(false);
  });

  const base = { configured: ["backlog", "serena", "context7"], disabled: [], extraOff: [], on: [], policy: "auto", whenNever: "keep" as const };

  test("名单与 --no-mcp 合并，--mcp 优先；按 Codex 配置顺序输出", () => {
    expect(selectMcp({ ...base, disabled: ["serena", "backlog"] }).off).toEqual(["backlog", "serena"]);
    expect(selectMcp({ ...base, disabled: ["backlog", "serena"], extraOff: ["context7"], on: ["serena"] }).off).toEqual(["backlog", "context7"]);
    expect(selectMcp(base)).toEqual({ off: [], dotted: [], unknown: [], unverified: false });
  });

  test("审批 never 且 mcp-when-never=off 时全关，--mcp 可单独豁免；其他审批不受影响", () => {
    expect(selectMcp({ ...base, policy: "never", whenNever: "off" }).off).toEqual(["backlog", "serena", "context7"]);
    expect(selectMcp({ ...base, policy: "never", whenNever: "off", on: ["context7"] }).off).toEqual(["backlog", "serena"]);
    expect(selectMcp({ ...base, policy: "auto", whenNever: "off" }).off).toEqual([]);
    expect(selectMcp({ ...base, policy: "never", whenNever: "keep" }).off).toEqual([]);
  });

  test("Codex 配置里没有的名字只提示、不下发；读不到配置时什么都不下发", () => {
    expect(selectMcp({ ...base, disabled: ["backlog", "ghost"], on: ["ghost2"] })).toEqual({ off: ["backlog"], dotted: [], unknown: ["ghost", "ghost2"], unverified: false });
    expect(selectMcp({ ...base, configured: null, disabled: ["backlog"] })).toEqual({ off: [], dotted: [], unknown: [], unverified: true });
    expect(selectMcp({ ...base, configured: null, policy: "never", whenNever: "off" }).unverified).toBe(true);
    expect(selectMcp({ ...base, configured: null }).unverified).toBe(false);
  });

  test("下发的配置键", () => {
    expect(mcpOffConfig(["backlog", "serena"])).toEqual({ [off("backlog")]: false, [off("serena")]: false });
  });

  test("带点服务名在名单和 never 全关两条路径均跳过下发", () => {
    for (const policy of [{ disabled: ["a.b", "backlog"] }, { policy: "never", whenNever: "off" as const }]) {
      const selection = selectMcp({ ...base, configured: ["a.b", "backlog"], ...policy });
      expect(selection.off).toEqual(["backlog"]);
      expect(selection.dotted).toEqual(["a.b"]);
      expect(mcpOffConfig(selection.off)).not.toHaveProperty(off("a.b"));
    }
    expect(selectMcp({ ...base, configured: ["a.b"], disabled: ["a.b"], on: ["a.b"] }).dotted).toEqual([]);
    expect(selectMcp({ ...base, disabled: ["a.b"] }).dotted).toEqual([]);
  });

  test("有效配置服务按回包顺序返回，排除明确停用的服务", () => {
    expect(enabledMcpServers({ config: { mcp_servers: { project_only: { enabled: true }, retired: { enabled: false }, user_on: {} } } }))
      .toEqual(["project_only", "user_on"]);
    expect(enabledMcpServers({ config: { mcp_servers: {} } })).toEqual([]);
    expect(enabledMcpServers({ config: {} })).toEqual([]);
  });

  test("有效配置回包形状异常时不猜测服务名单", () => {
    for (const value of [null, [], {}, { config: null }, { config: [] }, { config: { mcp_servers: [] } },
      { config: { mcp_servers: null } }, { config: { mcp_servers: { bad: false } } }]) expect(enabledMcpServers(value)).toBeNull();
  });

  test("参数解析：--mcp、--no-mcp 规范化；同一服务两边都列出是用法错误", () => {
    const parsed = parseArgv(["run", "任务", "--mcp", "serena, serena", "--no-mcp", "context7"]);
    expect(parsed.options.mcp).toBe("serena");
    expect(parsed.options["no-mcp"]).toBe("context7");
    expect(() => parseArgv(["run", "任务", "--mcp", "serena", "--no-mcp", "serena,backlog"])).toThrow("--mcp 与 --no-mcp 不能同时列出：serena");
    expect(() => parseArgv(["review", "--mcp", "a b"])).toThrow(CxbError);
  });

  test("持久配置只作用于 run、review", () => {
    const stored = { path: "", values: { "mcp-disabled": "backlog", "mcp-when-never": "off" }, unknownKeys: [] };
    for (const argv of [["run", "任务"], ["review", "--mode", "uncommitted"]]) {
      const parsed = parseArgv(argv);
      applyUserConfig(parsed, stored);
      expect(parsed.options).toMatchObject({ "mcp-disabled": "backlog", "mcp-when-never": "off" });
    }
    const other = parseArgv(["ls"]);
    applyUserConfig(other, stored);
    expect(other.options).not.toHaveProperty("mcp-disabled");
  });
});

describe("run、review 下发与登记", () => {
  let h: RunHarness;
  let oldHome: string | undefined;
  beforeEach(() => { h = new RunHarness(); oldHome = process.env.CODEX_HOME; process.env.CODEX_HOME = h.env.CODEX_HOME; });
  afterEach(async () => {
    try { await h.cleanup(); } finally {
      if (oldHome === undefined) delete process.env.CODEX_HOME; else process.env.CODEX_HOME = oldHome;
    }
  }, CLEANUP_BUDGET);

  const mcpServers = { backlog: {}, serena: {}, context7: {}, retired: { enabled: false } };
  /** 本地配置只用于既有审批风险计算；有效服务名单由模拟服务器独立返回。 */
  const codexConfig = () => writeFileSync(join(h.env.CODEX_HOME!, "config.toml"),
    "[mcp_servers.backlog]\ncommand = \"b\"\n[mcp_servers.serena]\ncommand = \"s\"\n[mcp_servers.context7]\ncommand = \"c\"\n"
    + "[mcp_servers.retired]\ncommand = \"r\"\nenabled = false\n");
  /** 先套持久配置再交给正式处理器，与 main.ts 的顺序一致。 */
  const withConfig = (inner: CommandHandler, values: Record<string, unknown>): CommandHandler =>
    (ctx) => { applyUserConfig(ctx.parsed, { path: "", values, unknownKeys: [] }); return inner(ctx); };
  const run = () => createRunHandler(fakeServerRequests());
  const calls = async (observer: Awaited<ReturnType<RunHarness["start"]>>) =>
    (await observer.rpc.request<any>("fixture/state")).calls as Array<{ method: string; params: any }>;

  test("持久名单：新开、恢复与登记都带关闭键，未关闭的服务不受影响", async () => {
    codexConfig();
    const observer = await h.start({ mcpServers });
    const handler = withConfig(run(), { "mcp-disabled": "backlog,serena" });
    const first = await h.invoke(handler, ["run", "中文", "--json", "-v"]);
    expect(first.code).toBe(0);
    expect(first.stderr).toContain("已请求关闭 MCP：backlog、serena");
    const id = JSON.parse(first.stdout).thread.id;
    const resumed = await h.invoke(handler, ["run", "继续", "--resume", id]);
    expect(resumed.code).toBe(0);
    expect(resumed.stderr.match(/本次 MCP 开关未确认生效/g)).toHaveLength(1);
    expect(resumed.stderr).toContain("MCP 服务器 backlog、serena、context7 的工具请求可能被 Codex 拒绝");
    const all = await calls(observer);
    const start = all.find((c) => c.method === "thread/start")!.params.config;
    const resume = all.find((c) => c.method === "thread/resume")!.params.config;
    for (const config of [start, resume]) {
      expect(config).toMatchObject({ [ASK_FEATURE_KEY]: true, [off("backlog")]: false, [off("serena")]: false });
      expect(config).not.toHaveProperty(off("context7"));
    }
    const registered = all.filter((c) => c.method === "fixture/resumeOverrides").map((c) => c.params.overrides.config);
    expect(registered).toHaveLength(2);
    for (const config of registered) expect(config).toMatchObject({ [off("backlog")]: false, [off("serena")]: false });
  }, TEST_BUDGET);

  test("--mcp 豁免名单里的服务，--no-mcp 额外关闭；未知名字提示一次且不下发", async () => {
    codexConfig();
    const observer = await h.start({ mcpServers });
    const handler = withConfig(run(), { "mcp-disabled": "backlog,serena,ghost" });
    const result = await h.invoke(handler, ["run", "中文", "--mcp", "serena", "--no-mcp", "context7,retired"]);
    expect(result.code).toBe(0);
    expect(result.stderr.match(/不在 Codex 已启用的配置里/g)).toHaveLength(1);
    expect(result.stderr).toContain("ghost、retired");
    const config = (await calls(observer)).find((c) => c.method === "thread/start")!.params.config;
    expect(Object.keys(config).filter((key) => key.startsWith("mcp_servers.")).sort()).toEqual([off("backlog"), off("context7")]);
  }, TEST_BUDGET);

  test("审批 never 且 mcp-when-never=off：全关并且不再提示这些服务可能被拒；改用 auto 则不全关", async () => {
    codexConfig();
    const observer = await h.start({ mcpServers });
    const handler = withConfig(run(), { "mcp-when-never": "off" });
    const never = await h.invoke(handler, ["run", "中文"]);
    expect(never.code).toBe(0);
    expect(never.stderr).not.toContain("当前审批为 never");
    const keep = await h.invoke(withConfig(run(), {}), ["run", "中文"]);
    expect(keep.stderr).toContain("MCP 服务器 backlog、serena、context7 的工具请求可能被 Codex 拒绝");
    const partial = await h.invoke(withConfig(run(), { "mcp-disabled": "backlog" }), ["run", "中文"]);
    expect(partial.stderr).toContain("MCP 服务器 serena、context7 的工具请求可能被 Codex 拒绝");
    expect((await h.invoke(handler, ["run", "中文", "--approval", "auto"])).code).toBe(0);
    // 键的先后对 Codex 没有意义，按集合比较。
    const starts = (await calls(observer)).filter((c) => c.method === "thread/start").map((c) =>
      Object.keys(c.params.config).filter((key) => key.startsWith("mcp_servers.")).sort());
    expect(starts).toEqual([[off("backlog"), off("context7"), off("serena")], [], [off("backlog")], []]);
  }, TEST_BUDGET);

  test("读不到 Codex 配置：提示且不下发任何关闭键；名单为空时不提示", async () => {
    const observer = await h.start({ configReadError: -32601 });
    const named = await h.invoke(withConfig(run(), { "mcp-disabled": "backlog" }), ["run", "中文"]);
    expect(named.code).toBe(0);
    expect(named.stderr).toContain("读不到 Codex 的 MCP 配置，本次未关闭任何 MCP 服务");
    const empty = await h.invoke(withConfig(run(), {}), ["run", "中文"]);
    expect(empty.stderr).not.toContain("读不到 Codex 的 MCP 配置");
    for (const start of (await calls(observer)).filter((c) => c.method === "thread/start")) {
      expect(Object.keys(start.params.config).some((key) => key.startsWith("mcp_servers."))).toBe(false);
    }
  }, TEST_BUDGET);

  test("提问开关被拒去键重试时保留关闭键，登记也保留", async () => {
    codexConfig();
    const observer = await h.start({ rejectAsk: true, mcpServers });
    expect((await h.invoke(withConfig(run(), { "mcp-disabled": "backlog" }), ["run", "中文"])).code).toBe(0);
    const all = await calls(observer);
    const starts = all.filter((c) => c.method === "thread/start");
    expect(starts).toHaveLength(2);
    expect(starts[1]!.params.config).not.toHaveProperty(ASK_FEATURE_KEY);
    expect(starts[1]!.params.config[off("backlog")]).toBe(false);
    expect(all.find((c) => c.method === "fixture/resumeOverrides")!.params.overrides.config[off("backlog")]).toBe(false);
  }, TEST_BUDGET);

  test("review 按 never 处理 mcp-when-never，且不输出审批风险提醒", async () => {
    codexConfig();
    const observer = await h.start({ mcpServers });
    const review = await h.invoke(withConfig(createReviewHandler(fakeServerRequests()), { "mcp-when-never": "off" }),
      ["review", "--mode", "uncommitted"]);
    expect(review.code).toBe(0);
    expect(review.stderr).not.toContain("当前审批为 never");
    const start = (await calls(observer)).find((c) => c.method === "thread/start")!.params.config;
    expect(start).toMatchObject({ [off("backlog")]: false, [off("serena")]: false, [off("context7")]: false });
  }, TEST_BUDGET);

  test("有效配置包含项目独有和项目重新启用的服务时均关闭，用户级文件不参与名单选择", async () => {
    codexConfig();
    const observer = await h.start({ mcpServers: { project_only: { enabled: true }, retired: { enabled: true } } });
    const result = await h.invoke(run(), ["run", "项目配置", "--no-mcp", "project_only,retired"]);
    expect(result.code).toBe(0);
    const all = await calls(observer);
    expect(all.find((c) => c.method === "config/read")!.params).toEqual({ cwd: h.workspace.root });
    expect(all.find((c) => c.method === "thread/start")!.params.config)
      .toMatchObject({ [off("project_only")]: false, [off("retired")]: false });
  }, TEST_BUDGET);

  test("同一线程三次恢复按关、开、再关替换登记，名单清空时不残留关闭键", async () => {
    const observer = await h.start({ mcpServers });
    const first = await h.invoke(run(), ["run", "创建", "--json"]);
    const id = JSON.parse(first.stdout).thread.id;
    for (const disabled of ["backlog", "", "serena"]) {
      const result = await h.invoke(withConfig(run(), disabled ? { "mcp-disabled": disabled } : {}), ["run", "恢复", "--resume", id]);
      expect(result.code).toBe(0);
    }
    const all = await calls(observer);
    const keys = (config: Record<string, unknown>) => Object.keys(config).filter((key) => key.startsWith("mcp_servers."));
    expect(all.filter((c) => c.method === "thread/resume").map((c) => keys(c.params.config)))
      .toEqual([[off("backlog")], [], [off("serena")]]);
    expect(all.filter((c) => c.method === "fixture/resumeOverrides").slice(1).map((c) => keys(c.params.overrides.config)))
      .toEqual([[off("backlog")], [], [off("serena")]]);
  }, TEST_BUDGET);

  for (const command of ["run", "review"] as const) test(`${command} 对比真实旧登记：清空名单提醒，保持不变不提醒，再关闭提醒`, async () => {
    h.workspace.workspaceDir = resolveWorkspace(h.workspace.root, h.env).workspaceDir;
    await h.start({ mcpServers });
    const invoke = async (disabled: string, id?: string) => {
      // 此处走真实恢复登记，避免 RunHarness 的登记记录桩掩盖持久化行为。
      const connection = await connectBroker({ workspace: h.workspace, brokerBin: BROKER_BIN,
        expectedBrokerVersion: await brokerVersionOf(BROKER_BIN), probe: true, timeoutMs: 5000 });
      h.connections.push(connection);
      const inner = command === "run" ? run() : createReviewHandler(fakeServerRequests());
      const args = command === "run" ? ["run", "配置切换", "--json"] : ["review", "--mode", "uncommitted", "--json"];
      if (id) args.push("--resume", id);
      const result = await h.invoke(withConfig(inner, disabled ? { "mcp-disabled": disabled } : {}), args, connection);
      expect(result.code).toBe(0);
      const threadId = JSON.parse(result.stdout).thread.id;
      const expected = mcpOffConfig(disabled ? disabled.split(",") : []);
      await until(() => {
        const stored = readThreadIndex(h.workspace.workspaceDir).find((entry) => entry.threadId === threadId)?.resumeOverrides as any;
        return stored?.config && JSON.stringify(Object.fromEntries(Object.entries(stored.config).filter(([key]) => key.startsWith("mcp_servers.")))) === JSON.stringify(expected);
      });
      return { ...result, id: threadId };
    };
    const first = await invoke("backlog");
    expect(first.stderr).not.toContain("本次 MCP 开关未确认生效");
    const raw = JSON.parse(readFileSync(join(h.workspace.workspaceDir, "threads.json"), "utf8"));
    expect(Object.values(raw.payload.threads).find((entry: any) => entry.threadId === first.id))
      .toMatchObject({ resumeOverrides: { config: { [off("backlog")]: false } } });
    expect((await invoke("backlog", first.id)).stderr).not.toContain("本次 MCP 开关未确认生效");
    expect((await invoke("", first.id)).stderr.match(/本次 MCP 开关未确认生效/g)).toHaveLength(1);
    expect((await invoke("backlog", first.id)).stderr.match(/本次 MCP 开关未确认生效/g)).toHaveLength(1);
  }, TEST_BUDGET);

  test("带点服务名在名单与 never 全关时提示并跳过，恢复只提示一次未确认生效", async () => {
    const observer = await h.start({ mcpServers: { "a.b": {}, backlog: {} } });
    const first = await h.invoke(run(), ["run", "创建", "--json"]);
    const id = JSON.parse(first.stdout).thread.id;
    for (const values of [{ "mcp-disabled": "a.b" }, { "mcp-when-never": "off" }]) {
      const result = await h.invoke(withConfig(run(), values), ["run", "恢复", "--resume", id]);
      expect(result.code).toBe(0);
      expect(result.stderr.match(/名字含「.」/g)).toHaveLength(1);
      expect(result.stderr.match(/本次 MCP 开关未确认生效/g)).toHaveLength(1);
    }
    const all = await calls(observer);
    for (const call of all.filter((c) => c.method === "thread/resume" || c.method === "fixture/resumeOverrides")) {
      const config = call.params.overrides?.config ?? call.params.config;
      expect(Object.keys(config)).not.toContain(off("a.b"));
    }
  }, TEST_BUDGET);

  test("没有 MCP 策略时不读有效配置；只给 --mcp ghost 仍读取并提示不存在", async () => {
    const observer = await h.start({ mcpServers });
    expect((await h.invoke(run(), ["run", "普通运行"])).code).toBe(0);
    expect((await calls(observer)).some((c) => c.method === "config/read")).toBe(false);
    const result = await h.invoke(run(), ["run", "豁免", "--mcp", "ghost"]);
    expect(result.code).toBe(0);
    expect(result.stderr).toContain("MCP 服务 ghost 不在 Codex 已启用的配置里");
    expect((await calls(observer)).filter((c) => c.method === "config/read")).toHaveLength(1);
  }, TEST_BUDGET);

  test("恢复时只给 --mcp 豁免也提示未确认生效，Codex 已停用服务不被启用", async () => {
    const observer = await h.start({ mcpServers });
    const first = await h.invoke(run(), ["run", "创建", "--json"]);
    const id = JSON.parse(first.stdout).thread.id;
    const result = await h.invoke(run(), ["run", "豁免", "--resume", id, "--mcp", "retired"]);
    expect(result.code).toBe(0);
    expect(result.stderr).toContain("本次 MCP 开关未确认生效");
    expect(result.stderr).toContain("MCP 服务 retired 不在 Codex 已启用的配置里");
    const resume = (await calls(observer)).find((c) => c.method === "thread/resume")!;
    expect(Object.keys(resume.params.config).some((key) => key.startsWith("mcp_servers."))).toBe(false);
  }, TEST_BUDGET);

  test("有效配置回包形状异常时提示且不下发关闭键", async () => {
    const observer = await h.start({ configRead: { unexpected: {} } });
    const result = await h.invoke(run(), ["run", "配置异常", "--no-mcp", "backlog"]);
    expect(result.code).toBe(0);
    expect(result.stderr).toContain("读不到 Codex 的 MCP 配置，本次未关闭任何 MCP 服务");
    const all = await calls(observer);
    expect(Object.keys(all.find((c) => c.method === "thread/start")!.params.config).some((key) => key.startsWith("mcp_servers."))).toBe(false);
    expect(Object.keys(all.find((c) => c.method === "fixture/resumeOverrides")!.params.overrides.config).some((key) => key.startsWith("mcp_servers."))).toBe(false);
  }, TEST_BUDGET);
});
