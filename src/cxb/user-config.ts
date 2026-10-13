import fs, { closeSync, fsyncSync, mkdirSync, openSync, readFileSync, unlinkSync, writeFileSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { join } from "node:path";
import { config } from "../config";
import type { ParsedCommand } from "./args";
import { CONFIG_KEYS, CONFIG_SCHEMA, isConfigKey, type ConfigKey } from "./config-schema";
import { CxbError } from "./contracts";

export interface UserConfig {
  path: string;
  values: Record<string, unknown>;
  unknownKeys: string[];
}

export const CONFIG_DEFAULTS: Readonly<Record<ConfigKey, string | number | boolean | null>> = Object.freeze({
  model: null, reasoning: null, sandbox: config.defaultSandbox, approval: config.defaultApprovalPolicy,
  timeout: config.defaultTimeout, "converge-timeout": 3600, memory: false, server: "auto", "broker-idle-timeout": 300,
});

function invalid(path: string): CxbError {
  return new CxbError("LOCAL_ERROR", `配置文件不可用：${path}；请备份后修复或删除该文件。`);
}

function validate(path: string, value: unknown): asserts value is Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw invalid(path);
  for (const [key, entry] of Object.entries(value)) {
    if (isConfigKey(key) && !CONFIG_SCHEMA[key].isValid(entry)) throw invalid(path);
  }
}

export function readUserConfig(dataRoot: string): UserConfig {
  const path = join(dataRoot, "config.json");
  let raw: string;
  try { raw = readFileSync(path, "utf8"); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return { path, values: {}, unknownKeys: [] };
    throw invalid(path);
  }
  let values: unknown;
  try { values = JSON.parse(raw); } catch { throw invalid(path); }
  validate(path, values);
  return { path, values, unknownKeys: Object.keys(values).filter((key) => !isConfigKey(key)) };
}

/** 无锁读改写：另一进程在读取与改名之间的修改可能被覆盖；调用方保留未知键，全部清除除外。 */
export function writeUserConfig(dataRoot: string, value: Record<string, unknown>): void {
  const path = join(dataRoot, "config.json");
  validate(path, value);
  // 即使调用方漏读，写入也不能覆盖损坏的目标文件。
  readUserConfig(dataRoot);
  const temporary = `${path}.${process.pid}.${randomUUID()}.tmp`;
  let fd: number | undefined;
  let owned = false;
  try {
    mkdirSync(dataRoot, { recursive: true });
    fd = openSync(temporary, "wx");
    owned = true;
    writeFileSync(fd, `${JSON.stringify(value, null, 2)}\n`, "utf8");
    fsyncSync(fd);
    closeSync(fd);
    fd = undefined;
    fs.renameSync(temporary, path);
  } catch (cause) {
    throw new CxbError("LOCAL_ERROR", `无法原子保存配置：${path}`, { cause });
  } finally {
    try { if (fd !== undefined) closeSync(fd); }
    finally {
      if (owned) {
        try { unlinkSync(temporary); }
        catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw new CxbError("LOCAL_ERROR", "清理配置临时文件失败", { cause: error }); }
      }
    }
  }
}

/** globals/options 保存有效选项；explicit 始终只代表真实命令行参数。 */
export function applyUserConfig(parsed: ParsedCommand, stored: UserConfig): void {
  for (const key of CONFIG_KEYS) {
    if (parsed.explicit.has(key) || !Object.hasOwn(stored.values, key)) continue;
    if (parsed.command === "review" && (key === "sandbox" || key === "approval")) continue;
    const value = stored.values[key];
    switch (key) {
      case "model": parsed.globals.model = value as string; break;
      case "reasoning": parsed.globals.reasoning = value as typeof parsed.globals.reasoning; break;
      case "sandbox": parsed.globals.sandbox = value as typeof parsed.globals.sandbox; break;
      case "approval": parsed.globals.approval = value as typeof parsed.globals.approval; break;
      case "timeout": parsed.globals.timeout = value as number; break;
      case "converge-timeout": if (parsed.command === "run" || parsed.command === "review") parsed.options["converge-timeout"] = value as number; break;
      case "memory": if (parsed.command === "run") parsed.options.memory = value as boolean; break;
    }
  }
}

/** 只保留有界单行摘要，常见凭据形式不进入错误与过程日志。 */
export function safeSummary(text: string): string {
  return text
    .replace(/\b(Bearer)\s+\S+/gi, "$1 [已隐藏]")
    .replace(/\b([\w-]*(?:token|password|secret|api[_ -]?key|cookie|authorization)[\w-]*)["']?\s*[:=]\s*[^\r\n]+/gi, "$1=[已隐藏]")
    .replace(/[\u0000-\u001f\u007f-\u009f\u2028\u2029]+/g, " ")
    .slice(0, 300).trim();
}
