import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { existsSync, mkdirSync, readFileSync, readdirSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { CONFIG_KEYS, CONFIG_SCHEMA } from "./config-schema";
import { CxbError, type MigrationReport } from "./contracts";
import { migrateLegacyData } from "./workspace";
import { bounded, CLEANUP_HOOK_TIMEOUT_MS, cleanupTestResources, eventually, makeTemp, privateTestEnv, ProcessTracker } from "./test-fixtures/ws-test-support";

let temporary: string;
let legacyRoot: string;
let dataRoot: string;
let processes: ProcessTracker;
const marker = "migrated-from-cxb.json";

beforeEach(() => {
  temporary = makeTemp("cxb-n1b-migrate-");
  legacyRoot = join(temporary, "old");
  dataRoot = join(temporary, "new");
  mkdirSync(legacyRoot);
  processes = new ProcessTracker();
});

afterEach(async () => {
  await cleanupTestResources(processes, temporary);
}, CLEANUP_HOOK_TIMEOUT_MS);

function json(path: string, value: unknown): void {
  writeFileSync(path, JSON.stringify(value));
}

function migrate(): MigrationReport { return migrateLegacyData({ dataRoot, legacyRoot }); }

function startTemplateChild(mode: string) {
  const ready = join(temporary, `template-${mode}-ready.json`);
  const release = join(temporary, `template-${mode}-release`);
  const proc = processes.add(Bun.spawn([
    process.execPath, join(import.meta.dir, "test-fixtures", "ws-template-child.ts"),
    mode, dataRoot, legacyRoot, ready, release,
  ], { stdin: "ignore", stdout: "pipe", stderr: "pipe", env: privateTestEnv() }));
  return { proc, ready, release, stdout: new Response(proc.stdout).text(), stderr: new Response(proc.stderr).text() };
}

async function templateChildReport(child: ReturnType<typeof startTemplateChild>): Promise<MigrationReport> {
  expect(await processes.exited(child.proc)).toBe(0);
  expect(await bounded(child.stderr, 1000, "读取模板夹具错误输出超时")).toBe("");
  return JSON.parse(await bounded(child.stdout, 1000, "读取模板夹具报告超时")) as MigrationReport;
}

function snapshot(root: string, prefix = ""): Record<string, string> {
  const result: Record<string, string> = {};
  for (const entry of readdirSync(join(root, prefix), { withFileTypes: true })) {
    const name = join(prefix, entry.name);
    if (entry.isDirectory()) Object.assign(result, snapshot(root, name));
    else result[name] = readFileSync(join(root, name)).toString("base64");
  }
  return result;
}

describe("配置键表", () => {
  test("包含运行策略的十三个键，字符串转换为各自的存储类型", () => {
    expect(CONFIG_KEYS).toEqual(["model", "reasoning", "sandbox", "approval", "timeout", "converge-timeout", "stall-timeout", "upstream-error-policy", "memory", "server", "broker-idle-timeout", "mcp-disabled", "mcp-when-never"]);
    const values = { model: "模型", reasoning: "ultra", sandbox: "read-only", approval: "auto", timeout: "3600", memory: "false", server: "private", "converge-timeout": "3600", "stall-timeout": "0", "upstream-error-policy": "observe", "broker-idle-timeout": "300", "mcp-disabled": "backlog, serena,backlog", "mcp-when-never": "keep" };
    values.model = "provider/model:[v1]";
    for (const key of CONFIG_KEYS) {
      const spec = CONFIG_SCHEMA[key];
      expect(spec.validate(values[key])).toBe(true);
      expect(spec.isValid(spec.parse(values[key]))).toBe(true);
      expect(spec.hint.length).toBeGreaterThan(0);
    }
    expect(CONFIG_SCHEMA.timeout.parse("3600")).toBe(3600);
    expect(CONFIG_SCHEMA.memory.parse("false")).toBe(false);
    expect(CONFIG_SCHEMA.memory.parse("true")).toBe(true);
    expect(CONFIG_SCHEMA["broker-idle-timeout"].parse("300")).toBe(300);
  });

  test("严格拒绝非法枚举、错误存储类型和不精确的秒数", () => {
    for (const value of ["", "0", "-1", "1.5", "Infinity", "1e3", " 1", "2147484"]) {
      expect(CONFIG_SCHEMA.timeout.validate(value)).toBe(false);
      expect(() => CONFIG_SCHEMA.timeout.parse(value)).toThrow(CxbError);
    }
    expect(CONFIG_SCHEMA.timeout.parse("2147483")).toBe(2_147_483);
    expect(CONFIG_SCHEMA.timeout.isValid("300")).toBe(false);
    expect(CONFIG_SCHEMA.memory.isValid("true")).toBe(false);
    expect(CONFIG_SCHEMA.memory.validate("TRUE")).toBe(false);
    for (const key of ["reasoning", "sandbox", "approval", "server"] as const) {
      expect(CONFIG_SCHEMA[key].validate("不存在")).toBe(false);
    }
    for (const value of ["", "有空格 model", "name;command", "name\n"]) expect(CONFIG_SCHEMA.model.validate(value)).toBe(false);
    expect(CONFIG_SCHEMA["broker-idle-timeout"].validate("9007199254740992")).toBe(false);
    expect(CONFIG_SCHEMA["broker-idle-timeout"].parse("2147484")).toBe(2_147_484);
  });
});

test("只迁合法配置与顶层 Markdown，保留新值、既有模板和全部旧文件", () => {
  json(join(legacyRoot, "config.json"), {
    model: "old-model", reasoning: "ultra", sandbox: "wrong", approval: "auto", timeout: 90,
    memory: true, server: "private", "broker-idle-timeout": 301, peer: "旧对端", peers: [], bad: 1,
  });
  mkdirSync(join(legacyRoot, "templates", "nested"), { recursive: true });
  writeFileSync(join(legacyRoot, "templates", "keep.md"), "旧模板\r\n");
  writeFileSync(join(legacyRoot, "templates", "中文.md"), "中文与 emoji 😀\r\n");
  writeFileSync(join(legacyRoot, "templates", "ignore.txt"), "忽略");
  writeFileSync(join(legacyRoot, "templates", "nested", "ignore.md"), "不递归");
  writeFileSync(join(legacyRoot, "threads.json"), "不迁线程");
  mkdirSync(join(dataRoot, "templates"), { recursive: true });
  json(join(dataRoot, "config.json"), { model: "new-model", memory: false, custom: "保留未知的新键" });
  writeFileSync(join(dataRoot, "templates", "keep.md"), "新模板");
  const before = snapshot(legacyRoot);
  const report = migrate();
  expect(report.migrated).toBe(true);
  expect(report.warnings).toEqual([]);
  expect(report.configKeys.sort()).toEqual(["reasoning", "approval", "timeout", "server", "broker-idle-timeout"].sort());
  expect(report.droppedKeys.sort()).toEqual(["sandbox", "peer", "peers", "bad"].sort());
  expect(report.templates).toEqual(["中文.md"]);
  expect(JSON.parse(readFileSync(join(dataRoot, "config.json"), "utf8"))).toEqual({
    model: "new-model", memory: false, custom: "保留未知的新键", reasoning: "ultra", approval: "auto", timeout: 90, server: "private", "broker-idle-timeout": 301,
  });
  expect(readFileSync(join(dataRoot, "templates", "keep.md"), "utf8")).toBe("新模板");
  expect(readFileSync(join(dataRoot, "templates", "中文.md"))).toEqual(readFileSync(join(legacyRoot, "templates", "中文.md")));
  expect(readdirSync(join(dataRoot, "templates")).sort()).toEqual(["keep.md", "中文.md"].sort());
  expect(existsSync(join(dataRoot, "threads.json"))).toBe(false);
  expect(snapshot(legacyRoot)).toEqual(before);
  expect(existsSync(join(dataRoot, marker))).toBe(true);
  expect(readdirSync(dataRoot).sort()).toEqual(["config.json", "templates", marker].sort());
});

test("旧目录不存在仍写标记，后来出现旧数据也不再迁移", () => {
  const missing = join(temporary, "missing");
  const first = migrateLegacyData({ dataRoot, legacyRoot: missing });
  expect(first).toEqual({ migrated: false, configKeys: [], droppedKeys: [], templates: [], warnings: [] });
  expect(existsSync(join(dataRoot, marker))).toBe(true);
  mkdirSync(missing);
  json(join(missing, "config.json"), { model: "later" });
  expect(migrateLegacyData({ dataRoot, legacyRoot: missing }).migrated).toBe(false);
  expect(existsSync(join(dataRoot, "config.json"))).toBe(false);
});

test("已有标记时不读旧目录也不触碰新目录内容", () => {
  mkdirSync(dataRoot);
  writeFileSync(join(dataRoot, marker), "保留原始标记，即使不是 JSON");
  writeFileSync(join(legacyRoot, "config.json"), "损坏");
  const before = snapshot(dataRoot);
  expect(migrate()).toEqual({ migrated: false, configKeys: [], droppedKeys: [], templates: [], warnings: [] });
  expect(snapshot(dataRoot)).toEqual(before);
});

test("默认旧目录来自 USERPROFILE", () => {
  const previous = process.env.USERPROFILE;
  process.env.USERPROFILE = temporary;
  try {
    mkdirSync(join(temporary, ".cxb-legacy"));
    json(join(temporary, ".cxb-legacy", "config.json"), { timeout: 20 });
    expect(migrateLegacyData({ dataRoot }).configKeys).toEqual(["timeout"]);
  } finally {
    if (previous === undefined) delete process.env.USERPROFILE;
    else process.env.USERPROFILE = previous;
  }
});

test("缺少 USERPROFILE 时仅告警且不写标记", () => {
  const previous = process.env.USERPROFILE;
  delete process.env.USERPROFILE;
  try {
    const report = migrateLegacyData({ dataRoot });
    expect(report.migrated).toBe(false);
    expect(report.warnings.length).toBeGreaterThan(0);
    expect(existsSync(join(dataRoot, marker))).toBe(false);
  } finally { if (previous !== undefined) process.env.USERPROFILE = previous; }
});

test("连续迁移失败不累积文件，修复源配置后仍可成功迁移", () => {
  const source = join(legacyRoot, "config.json");
  writeFileSync(source, "{损坏配置");
  for (let attempt = 0; attempt < 32; attempt++) {
    const report = migrate();
    expect(report.migrated).toBe(false);
    expect(report.warnings.length).toBeGreaterThan(0);
    expect(readdirSync(dataRoot)).toEqual([]);
    expect(readFileSync(source, "utf8")).toBe("{损坏配置");
  }
  json(source, { timeout: 30 });
  expect(migrate().configKeys).toEqual(["timeout"]);
  expect(readdirSync(dataRoot).sort()).toEqual(["config.json", marker].sort());
});

test.each(["旧", "新"])("%s配置损坏时不覆盖、不写标记，修复后可重试", (which) => {
  mkdirSync(dataRoot);
  const broken = join(which === "旧" ? legacyRoot : dataRoot, "config.json");
  json(join(legacyRoot, "config.json"), { timeout: 20 });
  writeFileSync(broken, "{损坏内容");
  const report = migrate();
  expect(report.migrated).toBe(false);
  expect(report.warnings.length).toBeGreaterThan(0);
  expect(readFileSync(broken, "utf8")).toBe("{损坏内容");
  expect(existsSync(join(dataRoot, marker))).toBe(false);
  json(broken, which === "旧" ? { timeout: 20 } : {});
  expect(migrate().warnings).toEqual([]);
  expect(existsSync(join(dataRoot, marker))).toBe(true);
});

test.each([{ value: null }, { value: [] }, { value: 42 }])("非对象配置 %j 不得当成空配置覆盖", ({ value }) => {
  json(join(legacyRoot, "config.json"), value);
  expect(migrate().warnings.length).toBeGreaterThan(0);
  expect(existsSync(join(dataRoot, marker))).toBe(false);
});

test("非法存储类型进入丢弃列表且原型名称不属于配置键", () => {
  writeFileSync(join(legacyRoot, "config.json"), '{"timeout":"30","memory":"true","model":42,"__proto__":{},"constructor":"x"}');
  const result = migrate();
  expect(result.droppedKeys.sort()).toEqual(["timeout", "memory", "model", "__proto__", "constructor"].sort());
  expect(result.configKeys).toEqual([]);
  expect(result.warnings).toEqual([]);
});

test("模板目录失败不写标记，已迁配置可在重试时保留", () => {
  json(join(legacyRoot, "config.json"), { model: "old" });
  mkdirSync(join(legacyRoot, "templates"));
  writeFileSync(join(legacyRoot, "templates", "a.md"), "模板");
  mkdirSync(dataRoot);
  writeFileSync(join(dataRoot, "templates"), "这里是文件");
  expect(migrate().warnings.length).toBeGreaterThan(0);
  expect(existsSync(join(dataRoot, marker))).toBe(false);
  json(join(dataRoot, "config.json"), { model: "new" });
  rmSync(join(dataRoot, "templates"));
  expect(migrate().warnings).toEqual([]);
  expect(JSON.parse(readFileSync(join(dataRoot, "config.json"), "utf8")).model).toBe("new");
  expect(readFileSync(join(dataRoot, "templates", "a.md"), "utf8")).toBe("模板");
});

test.each(["B 成功发布", "B 发布失败"])("A 的半份模板不可见：%s 后 A 复制失败，最终完整或可重试", async (scenario) => {
  const source = join(legacyRoot, "templates");
  mkdirSync(source);
  const content = Buffer.from("完整模板，含中文与 emoji 😀\r\n".repeat(4096));
  writeFileSync(join(source, "a.md"), content);
  const before = snapshot(legacyRoot);
  const target = join(dataRoot, "templates", "a.md");
  const a = startTemplateChild("partial-failure");
  try {
    await eventually(() => existsSync(a.ready));
    const staged = JSON.parse(readFileSync(a.ready, "utf8")).temporary as string;
    expect(staged.startsWith(join(dataRoot, "templates") + "\\")).toBe(true);
    expect(staged.endsWith(".tmp")).toBe(true);
    expect(readFileSync(staged)).toEqual(content.subarray(0, Math.floor(content.length / 2)));
    expect(existsSync(target)).toBe(false);
    expect(existsSync(join(dataRoot, marker))).toBe(false);

    // 由文件控制点保证 A 仍停在复制中途，再让独立进程 B 迁移。
    const b = startTemplateChild(scenario === "B 成功发布" ? "normal" : "EPERM");
    const report = await templateChildReport(b);
    if (scenario === "B 成功发布") {
      expect(report.migrated).toBe(true);
      expect(readFileSync(target)).toEqual(content);
      expect(existsSync(join(dataRoot, marker))).toBe(true);
    } else {
      expect(report.warnings.some((warning) => warning.includes("EPERM"))).toBe(true);
      expect(existsSync(target)).toBe(false);
      expect(existsSync(join(dataRoot, marker))).toBe(false);
    }
  } finally { writeFileSync(a.release, "让 A 复制失败"); }

  const failed = await templateChildReport(a);
  expect(failed.migrated).toBe(false);
  expect(failed.warnings.some((warning) => warning.includes("EIO"))).toBe(true);
  expect(existsSync(join(dataRoot, marker)) && !existsSync(target)).toBe(false);
  expect(readdirSync(join(dataRoot, "templates"))).toEqual(scenario === "B 成功发布" ? ["a.md"] : []);

  const retried = migrate();
  expect(retried.warnings).toEqual([]);
  expect(retried.migrated).toBe(scenario === "B 发布失败");
  expect(readFileSync(target)).toEqual(content);
  expect(existsSync(join(dataRoot, marker))).toBe(true);
  expect(readdirSync(join(dataRoot, "templates"))).toEqual(["a.md"]);
  expect(snapshot(legacyRoot)).toEqual(before);
}, 30000);

test.each(["EPERM", "ENOTSUP"])("硬链接发布返回 %s 时告警并清理临时文件，修复后可补迁", async (code) => {
  mkdirSync(join(legacyRoot, "templates"));
  writeFileSync(join(legacyRoot, "templates", "a.md"), "完整模板");
  const report = await templateChildReport(startTemplateChild(code));
  expect(report.migrated).toBe(false);
  expect(report.templates).toEqual([]);
  expect(report.warnings.some((warning) => warning.includes(code) && warning.includes("硬链接"))).toBe(true);
  expect(existsSync(join(dataRoot, marker))).toBe(false);
  expect(readdirSync(join(dataRoot, "templates"))).toEqual([]);
  expect(migrate().templates).toEqual(["a.md"]);
  expect(readFileSync(join(dataRoot, "templates", "a.md"), "utf8")).toBe("完整模板");
}, 15000);

test("发布瞬间另一个进程已写入完整模板时不覆盖，并清理本进程临时文件", async () => {
  mkdirSync(join(legacyRoot, "templates"));
  writeFileSync(join(legacyRoot, "templates", "a.md"), "旧模板");
  const report = await templateChildReport(startTemplateChild("existing-at-publish"));
  expect(report.warnings).toEqual([]);
  expect(report.templates).toEqual([]);
  expect(readFileSync(join(dataRoot, "templates", "a.md"), "utf8")).toBe("并发进程已发布的完整模板");
  expect(readdirSync(join(dataRoot, "templates"))).toEqual(["a.md"]);
  expect(existsSync(join(dataRoot, marker))).toBe(true);
}, 15000);

test("数据根不可写时仅返回告警", () => {
  writeFileSync(dataRoot, "不是目录");
  expect(() => migrate()).not.toThrow();
  expect(migrate().warnings.length).toBeGreaterThan(0);
});

test("新旧目录重叠时保护旧文件", () => {
  json(join(legacyRoot, "config.json"), { model: "old" });
  const before = snapshot(legacyRoot);
  for (const target of [legacyRoot, join(legacyRoot, "new")]) {
    expect(migrateLegacyData({ dataRoot: target, legacyRoot }).warnings.length).toBeGreaterThan(0);
  }
  expect(snapshot(legacyRoot)).toEqual(before);
});

test("目标模板 junction 指向旧目录时也不写旧目录", () => {
  mkdirSync(join(legacyRoot, "templates"));
  writeFileSync(join(legacyRoot, "templates", "a.md"), "模板");
  mkdirSync(dataRoot);
  symlinkSync(legacyRoot, join(dataRoot, "templates"), "junction");
  const before = snapshot(legacyRoot);
  expect(migrate().warnings.length).toBeGreaterThan(0);
  expect(snapshot(legacyRoot)).toEqual(before);
  expect(existsSync(join(dataRoot, marker))).toBe(false);
});

test.each(["已有新配置", "没有新配置"])("两个独立 CLI 并发迁移（%s），最终状态完整且可幂等重试", async (state) => {
  json(join(legacyRoot, "config.json"), { model: "old", timeout: 45, memory: true });
  mkdirSync(join(legacyRoot, "templates"));
  for (let i = 0; i < 32; i++) writeFileSync(join(legacyRoot, "templates", `${i}.md`), `模板 ${i}\n`);
  mkdirSync(dataRoot);
  if (state === "已有新配置") json(join(dataRoot, "config.json"), { model: "new", extra: false });
  const before = snapshot(legacyRoot);
  const start = join(temporary, "start");
  const children = [0, 1].map((i) => {
    const ready = join(temporary, `ready-${i}`);
    const proc = processes.add(Bun.spawn([process.execPath, join(import.meta.dir, "test-fixtures", "ws-migrate-child.ts"), dataRoot, legacyRoot, ready, start], {
      stdin: "ignore", stdout: "pipe", stderr: "pipe", env: privateTestEnv(),
    }));
    return { proc, ready, stdout: new Response(proc.stdout).text(), stderr: new Response(proc.stderr).text() };
  });
  await eventually(() => children.every((child) => existsSync(child.ready)));
  writeFileSync(start, "开始");
  const results = await Promise.allSettled(children.map(async (child) => {
    expect(await processes.exited(child.proc)).toBe(0);
    expect(await bounded(child.stderr, 1000, "读取迁移错误输出超时")).toBe("");
    return JSON.parse(await bounded(child.stdout, 1000, "读取迁移结果超时")) as MigrationReport;
  }));
  const failures = results.filter((result) => result.status === "rejected");
  if (failures.length) throw new AggregateError(failures.map((result) => result.reason), "并发迁移子进程失败");
  // 不约束竞争者的告警或执行速度；两边均收束后检查最终状态及重复调用的幂等性。
  expect(JSON.parse(readFileSync(join(dataRoot, "config.json"), "utf8"))).toEqual(state === "已有新配置"
    ? { model: "new", extra: false, timeout: 45, memory: true }
    : { model: "old", timeout: 45, memory: true });
  expect(snapshot(join(dataRoot, "templates"))).toEqual(snapshot(join(legacyRoot, "templates")));
  expect(snapshot(legacyRoot)).toEqual(before);
  expect(() => JSON.parse(readFileSync(join(dataRoot, marker), "utf8"))).not.toThrow();
  expect(readdirSync(dataRoot).sort()).toEqual(["config.json", "templates", marker].sort());
  const completed = snapshot(dataRoot);
  expect(migrate().migrated).toBe(false);
  expect(snapshot(dataRoot)).toEqual(completed);
  expect(snapshot(legacyRoot)).toEqual(before);
}, 15000);
