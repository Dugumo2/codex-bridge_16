import { existsSync, mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve, sep } from "node:path";
import { Writable } from "node:stream";
import { parseArgv } from "../args";
import { CxbError, type ExitCode } from "../contracts";
import { execute, HANDLERS, type CommandHandler } from "../main";
import { createOutput, type Cleanup } from "../output";
import { bounded } from "../wait-run";
import { privateEnv } from "./main-test-support";

export class N5Harness {
  dir = mkdtempSync(join(tmpdir(), "cxb-n5a-"));
  root = join(this.dir, "ws");
  home = join(this.dir, "home");
  original = { ...process.env };
  cleanups = new Set<Cleanup>();
  constructor() {
    mkdirSync(this.root); mkdirSync(this.home);
    const env = privateEnv(process.env);
    for (const key of Object.keys(process.env)) delete process.env[key];
    for (const [key, value] of Object.entries(env)) {
      if (!["USERPROFILE", "HOME", "CODEX_HOME", "CXB_HOME"].includes(key.toUpperCase())) process.env[key] = value;
    }
    for (const key of ["USERPROFILE", "HOME", "CODEX_HOME", "CXB_HOME"]) process.env[key] = this.home;
  }
  async invoke(args: string[], handlers: Partial<typeof HANDLERS> = HANDLERS, deps?: Parameters<typeof execute>[4]) {
    let stdout = ""; let stderr = "";
    const capture = (write: (value: string) => void) => new Writable({ write(chunk, _encoding, done) { write(chunk.toString()); done(); } });
    const json = args.includes("--json");
    const out = createOutput({ command: args[0], json, verbose: args.includes("-v"), stdout: capture((value) => stdout += value), stderr: capture((value) => stderr += value) });
    let code: ExitCode;
    let error: unknown;
    try {
      const parsed = parseArgv([...args, "-d", this.root]);
      code = await execute(parsed, out, (cleanup) => { this.cleanups.add(cleanup); return () => { this.cleanups.delete(cleanup); }; }, handlers, deps);
    } catch (cause) {
      error = cause; code = cause instanceof CxbError ? cause.exitCode : 1; out.error(cause);
    }
    await out.flush();
    return { code, stdout, stderr, error };
  }
  async cleanup() {
    const errors: unknown[] = [];
    for (const cleanup of this.cleanups) {
      try { await bounded(Promise.resolve(cleanup()), 3000); } catch (error) { errors.push(error); }
    }
    this.cleanups.clear();
    for (const key of Object.keys(process.env)) delete process.env[key];
    Object.assign(process.env, this.original);
    try {
      const allowed = resolve(process.env.TEMP!) + sep;
      if (!resolve(this.dir).toLowerCase().startsWith(allowed.toLowerCase())) throw new Error("拒绝清理指定临时根以外的目录");
      rmSync(this.dir, { recursive: true, force: true });
      if (existsSync(this.dir)) throw new Error("临时目录未清除");
    } catch (error) { errors.push(error); }
    if (errors.length) throw new AggregateError(errors, "N5 测试清理失败");
  }
}

export const captureHandler = (inspect: Parameters<CommandHandler>[0] extends infer T ? (ctx: T) => void : never): CommandHandler => async (ctx) => { inspect(ctx); return 0; };
