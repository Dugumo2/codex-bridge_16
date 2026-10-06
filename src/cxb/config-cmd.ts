import { readFileSync, readdirSync } from "node:fs";
import { join, resolve } from "node:path";
import { parseTemplateFrontmatter } from "../config";
import { CONFIG_KEYS, CONFIG_SCHEMA, isConfigKey } from "./config-schema";
import { CxbError, EXIT } from "./contracts";
import { safeSummary } from "./user-config";
import type { CommandHandler } from "./main";
import { CONFIG_DEFAULTS, writeUserConfig } from "./user-config";

/** 源码 src/cxb 与安装 scripts 都从所在目录向上一级找 prompts。 */
export function builtinTemplatesDir(moduleDir = import.meta.dir): string {
  return resolve(moduleDir, "..", "prompts");
}

export function listTemplates(dataRoot: string, builtinDir = builtinTemplatesDir()) {
  const items = new Map<string, { name: string; sandbox: string | null; description: string; source: "builtin" | "user" }>();
  const warnings: string[] = [];
  for (const [directory, source] of [[builtinDir, "builtin"], [join(dataRoot, "templates"), "user"]] as const) {
    let files: string[];
    try { files = readdirSync(directory).filter((name) => name.endsWith(".md")).sort(); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") warnings.push(safeSummary(`无法读取模板目录：${directory}`));
      continue;
    }
    for (const file of files) {
      const name = file.slice(0, -3);
      // 用户的同名坏模板也遮蔽内置模板，与 run 实际选取路径一致。
      items.delete(name);
      try {
        const raw = readFileSync(join(directory, file), "utf8").replace(/\r\n/g, "\n");
        if (raw.split("\n")[0]?.trim() === "---") {
          const lines = raw.split("\n");
          const end = lines.indexOf("---", 1);
          if (end < 0 || lines.slice(1, end).some((line) => line.trim() && !/^\s*#/.test(line) && !/^\w+\s*:\s*.+$/.test(line))) {
            throw new CxbError("LOCAL_ERROR", "模板元数据格式损坏");
          }
        }
        const { meta } = parseTemplateFrontmatter(raw);
        if (meta.sandbox && !CONFIG_SCHEMA.sandbox.isValid(meta.sandbox)) throw new CxbError("LOCAL_ERROR", "模板沙箱无效");
        items.set(name, { name, sandbox: meta.sandbox ?? null, description: meta.description, source });
      } catch { warnings.push(safeSummary(`已跳过无法读取或 frontmatter 损坏的模板：${join(directory, file)}`)); }
    }
  }
  return { items: [...items.values()].sort((a, b) => a.name.localeCompare(b.name)), warnings };
}

export const configCommand: CommandHandler = async (ctx) => {
  if (ctx.userConfigError) throw ctx.userConfigError;
  const stored = ctx.userConfig;
  if (!stored) throw new CxbError("LOCAL_ERROR", "配置尚未由命令入口读取");
  const [key, raw] = ctx.parsed.positional;
  const emit = (value: object, text: string) => { ctx.out.resultJson({ ok: true, command: "config", ...value }); ctx.out.result(`${text}\n`); };
  if (key === "templates") {
    const result = listTemplates(ctx.workspace.dataRoot);
    for (const warning of result.warnings) ctx.out.notice(`[cxb] ${warning}`);
    emit({ subcommand: "templates", ...result }, result.items.map((item) => `${item.name}（${item.source === "user" ? "用户" : "内置"}，${item.sandbox ?? "未建议沙箱"}）：${item.description}`).join("\n") || "没有可用模板");
    return EXIT.OK;
  }
  if (key !== undefined && !isConfigKey(key)) throw new CxbError("USAGE", "不支持的配置键");
  if (ctx.parsed.options.unset) {
    const values = key === undefined ? {} : { ...stored.values };
    if (key !== undefined) delete values[key];
    writeUserConfig(ctx.workspace.dataRoot, values);
    emit(key === undefined ? { action: "unsetAll" } : { action: "unset", key }, key === undefined ? "已清除全部配置（包括未知键）" : `已清除 ${key}，恢复内置默认值`);
  } else if (key !== undefined && raw !== undefined) {
    const value = CONFIG_SCHEMA[key].parse(raw);
    writeUserConfig(ctx.workspace.dataRoot, { ...stored.values, [key]: value });
    const appliesTo = ["server", "broker-idle-timeout"].includes(key) ? "brokerRestart" : "newRuns";
    emit({ action: "set", key, value, appliesTo }, `已设置 ${key} = ${value}`);
  } else if (key !== undefined) {
    const source = Object.hasOwn(stored.values, key) ? "config" : "default";
    const value = source === "config" ? stored.values[key] : CONFIG_DEFAULTS[key];
    emit({ key, value, source }, `${key} = ${value ?? "自动"}（${source === "config" ? "配置" : "默认"}）`);
  } else {
    const values = Object.fromEntries(CONFIG_KEYS.map((name) => [name, { value: stored.values[name] ?? CONFIG_DEFAULTS[name], source: Object.hasOwn(stored.values, name) ? "config" : "default" }]));
    emit({ path: stored.path, values, unknownKeys: stored.unknownKeys }, [`配置文件：${stored.path}`, ...Object.entries(values).map(([name, entry]) => `${name} = ${entry.value ?? "自动"}（${entry.source === "config" ? "配置" : "默认"}）`), ...(stored.unknownKeys.length ? [`未知键：${stored.unknownKeys.map(safeSummary).join("、")}`] : [])].join("\n"));
  }
  if (ctx.parsed.options.unset || raw !== undefined) {
    ctx.out.progress(key === undefined || ["server", "broker-idle-timeout"].includes(key)
      ? "broker 重启后生效（当前 broker 空闲退出或被替换后）；运行选项只影响之后新开的运行"
      : "只影响之后新开的运行");
  }
  return EXIT.OK;
};
