// cxb 命令行的黑盒验收测试（S4 N1）：只依据命令面规格（第 1、2、4 节），从外部启动 `bun src/cxb/main.ts`，
// 看退出码、stdout、stderr，不依赖各模块的内部接口。
// 每个子进程都在临时的 USERPROFILE、HOME、CXB_HOME 下运行，绝不读写开发者真实的 ~/.cxb-legacy 与 ~/.codex-bridge。

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Processes, alive, privateEnv, removeTemp, until, type Ran } from "./test-fixtures/main-test-support";

const MAIN = join(import.meta.dir, "main.ts");
/** 单个子进程的期限：到期结束它并让用例失败。 */
const RUN_LIMIT_MS = 20_000;
const TEST_LIMIT_MS = 40_000;

let sandbox = "";
let processes: Processes;
beforeEach(() => {
  sandbox = mkdtempSync(join(tmpdir(), "cxb-accept-"));
  processes = new Processes();
  for (const sub of ["profile", "cxb-home"]) mkdirSync(join(sandbox, sub));
});
afterEach(async () => {
  const errors: unknown[] = [];
  try { await processes.cleanup(); } catch (error) { errors.push(error); }
  try {
    if (!processes.allExited()) throw new Error("仍有存活进程，保留临时目录供排查");
    await removeTemp(sandbox);
  } catch (error) { errors.push(error); }
  if (errors.length) throw new AggregateError(errors, "验收测试清理失败");
}, 25_000);

async function cxb(...args: string[]): Promise<Ran> {
  return processes.run(process.execPath, [MAIN, ...args], {
    cwd: sandbox,
    limitMs: RUN_LIMIT_MS,
    env: {
      ...privateEnv(process.env),
      USERPROFILE: join(sandbox, "profile"),
      HOME: join(sandbox, "profile"),
      CXB_HOME: join(sandbox, "cxb-home"),
    },
  });
}

const COMMANDS = ["run", "review", "show", "ls", "inbox", "reply", "wait", "stop", "rm", "config", "doctor"];

describe("cxb 黑盒验收（N1）", () => {
  test("--version：stdout 一行 `cxb <版本>`，退出 0，stderr 为空", async () => {
    const r = await cxb("--version");
    expect(r.code).toBe(0);
    expect(r.stdout).toMatch(/^cxb \S+\r?\n$/);
    expect(r.stderr).toBe("");
  }, TEST_LIMIT_MS);

  test("--help：列出 11 个命令、不含已取消的 peer 系，退出 0", async () => {
    const r = await cxb("--help");
    expect(r.code).toBe(0);
    for (const name of COMMANDS) expect(r.stdout).toContain(name);
    for (const gone of ["peer", "send", "update"]) expect(r.stdout).not.toMatch(new RegExp(`^\\s*${gone}\\b`, "m"));
  }, TEST_LIMIT_MS);

  test.each(COMMANDS)("%s 的 --help 退出 0 并写到 stdout", async (name) => {
    const r = await cxb(name, "--help");
    expect(r.code).toBe(0);
    expect(r.stdout).toContain(name);
  }, TEST_LIMIT_MS);

  test("未知命令：退出 1，stdout 为空，stderr 以 `cxb: error:` 开头", async () => {
    const r = await cxb("frobnicate");
    expect(r.code).toBe(1);
    expect(r.stdout).toBe("");
    expect(r.stderr.startsWith("cxb: error:")).toBe(true);
  }, TEST_LIMIT_MS);

  const replacements: Array<[string, string]> = [
    ["threads", "ls"], ["jobs", "ls"], ["kill", "stop"], ["follow", "show"], ["output", "show"],
    ["progress", "show"], ["peek", "show"], ["approve", "reply"], ["decline", "reply"], ["answer", "reply"],
    ["questions", "inbox"], ["next", "wait"], ["delete", "rm"], ["health", "doctor"], ["models", "doctor"],
    ["templates", "config templates"], ["version", "--version"],
  ];
  test.each(replacements)("旧名 %s 退出 1，并提示 %s", async (old, replacement) => {
    const r = await cxb(old);
    expect(r.code).toBe(1);
    expect(r.stdout).toBe("");
    expect(r.stderr).toContain(replacement);
  }, TEST_LIMIT_MS);
  test.each(["update", "skill", "clean", "ask", "peer", "peers", "send", "task", "tasks"])("已删除命令 %s 退出 1", async (gone) => {
    const r = await cxb(gone);
    expect(r.code).toBe(1);
    expect(r.stdout).toBe("");
    expect(r.stderr.startsWith("cxb: error:")).toBe(true);
  }, TEST_LIMIT_MS);

  test("--json 下的用法错误：stderr 是一个 JSON 错误对象，stdout 为空", async () => {
    const r = await cxb("--json", "frobnicate");
    expect(r.code).toBe(1);
    expect(r.stdout).toBe("");
    const obj = JSON.parse(r.stderr.trim());
    expect(obj.ok).toBe(false);
    expect(obj.error.code).toBe("USAGE");
    expect(typeof obj.error.message).toBe("string");
    expect(obj.error.retryable).toBe(false);
    expect(obj.error.id).toBeNull();
    expect(obj.error.command).toBeNull();
  }, TEST_LIMIT_MS);

  test.each([
    ["ls", "--bogus"],
    ["ls", "--timeout"],
    ["ls", "--timeout", "0"],
    ["ls", "--timeout", "2147484"],
    ["ls", "--timeout", "60", "--timeout", "60"],
    ["run", "--sandbox", "everything", "hi"],
    ["run", "--approval", "sometimes", "hi"],
    ["run", "-r", "extreme", "hi"],
  ].map((args) => ({ args })))("非法参数退出 1：%j", async ({ args }) => {
    const r = await cxb(...args);
    expect(r.code).toBe(1);
    expect(r.stdout).toBe("");
    expect(r.stderr.startsWith("cxb: error:")).toBe(true);
  }, TEST_LIMIT_MS);

  test("-d 指向不存在的目录：退出 1", async () => {
    const r = await cxb("ls", "-d", join(sandbox, "no-such-dir"));
    expect(r.code).toBe(1);
    expect(r.stdout).toBe("");
  }, TEST_LIMIT_MS);

  test.each([
    { args: ["-d", "目录", "run", "--timeout", "0"], command: "run" },
    { args: ["--dir=目录", "run", "--timeout=0"], command: "run" },
    { args: ["--model", "x", "run", "--timeout=0"], command: "run" },
    { args: ["--model", "x"], command: null },
    { args: ["--model", "run"], command: null },
    { args: ["--timeout=5", "run", "--timeout=0"], command: "run" },
    { args: ["--timeout", "5", "run", "--timeout=0"], command: "run" },
    { args: ["-d", "目录", "threads"], command: "threads" },
    { args: ["完全无法识别"], command: null },
    { args: ["run", "--timeout", "--json"], command: "run" },
  ])("参数错误预扫描只报告已知命令：%j", async ({ args, command }) => {
    const r = await cxb(...args, "--json");
    expect(r.code).toBe(1);
    expect(r.stdout).toBe("");
    expect(JSON.parse(r.stderr).error).toMatchObject({ code: "USAGE", command });
  }, TEST_LIMIT_MS);

  test("终止符之后的 --json 不启用 JSON 错误模式", async () => {
    const r = await cxb("run", "--timeout=0", "--", "--json");
    expect(r.code).toBe(1);
    expect(r.stdout).toBe("");
    expect(r.stderr.startsWith("cxb: error:")).toBe(true);
  }, TEST_LIMIT_MS);

  test("终止符之后的命令词不进入预扫描错误对象", async () => {
    const r = await cxb("--json", "--timeout=0", "--", "run");
    expect(r.code).toBe(1);
    expect(JSON.parse(r.stderr).error.command).toBeNull();
  }, TEST_LIMIT_MS);

  test("子进程助手超时会拒绝、强制回收并确认 PID 已退出", async () => {
    await expect(processes.run(process.execPath, ["-e", "setInterval(() => {}, 1000)"], {
      cwd: sandbox, env: privateEnv(process.env), limitMs: 500,
    })).rejects.toThrow("子进程执行失败");
    expect(processes.allExited()).toBe(true);
  }, TEST_LIMIT_MS);

  test("进程已退出但后代保持输出写端时，助手的 EOF 等待也有期限", async () => {
    const registry = join(sandbox, "pids.txt");
    const release = join(sandbox, "release");
    const running = processes.run(process.execPath, [join(import.meta.dir, "test-fixtures/main-held-pipe.ts"), "parent", registry, release], {
      cwd: sandbox, env: privateEnv(process.env), limitMs: 2000,
    });
    // 立即挂接拒绝处理，避免等待就绪期间出现未处理拒绝。
    const result = running.then(() => null, (error: unknown) => error);
    try {
      await until(() => existsSync(registry), 5000, "输出持有者没有登记 PID");
      processes.readRegistry(registry);
      const [parent, holder] = readFileSync(registry, "utf8").trim().split(/\r?\n/).map(Number);
      await until(() => !alive(parent), 5000, "夹具父进程没有自然退出");
      expect(alive(holder)).toBe(true);
      expect(await result).toBeInstanceOf(AggregateError);
    } finally {
      processes.readRegistry(registry);
      writeFileSync(release, "允许后代退出");
      await result;
    }
  }, TEST_LIMIT_MS);
});
