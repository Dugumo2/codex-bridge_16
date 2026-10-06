// src/cxb/help.ts — 由 args.ts 的选项表生成中文帮助，保证帮助与解析规则一致；每行不超过 100 显示列。

import { COMMANDS, GLOBAL_OPTIONS, type CommandName, type OptionSpec } from "./args";
import { CxbError } from "./contracts";

/** 中文按两列保守折行，保持帮助在 100 列终端中可读。 */
function wrap(line: string): string[] {
  const lines: string[] = [];
  let current = "";
  let columns = 0;
  for (const char of line) {
    const width = char.codePointAt(0)! > 0xff ? 2 : 1;
    if (columns + width > 100) {
      lines.push(current);
      current = "    ";
      columns = 4;
    }
    current += char;
    columns += width;
  }
  lines.push(current);
  return lines;
}

function optionLines(options: readonly OptionSpec[]): string[] {
  return options.flatMap((option) => [
    `  ${option.flags.join(", ")}${option.valueName ? ` <${option.valueName}>` : ""}`,
    `    ${option.description}`,
  ]);
}

/** 返回文本，由入口决定写出；不连接 broker，也不改变进程退出状态。 */
export function renderHelp(command: CommandName | null = null): string {
  const lines: string[] = [];
  if (command === null) {
    lines.push("cxb：Codex 协作命令行", "", "用法：cxb <命令> [选项] [--] [位置参数 ...]", "", "命令：");
    for (const [name, spec] of Object.entries(COMMANDS)) lines.push(`  ${name}  ${spec.description}`);
    lines.push("", "全局选项（适用命令见各命令帮助）：", ...optionLines(GLOBAL_OPTIONS));
  } else {
    if (!Object.hasOwn(COMMANDS, command)) throw new CxbError("USAGE", "未知命令；请用 cxb --help 查看可用命令。");
    const spec = COMMANDS[command];
    lines.push(`cxb ${command}：${spec.description}`, "", "用法：", ...spec.usage.map((line) => `  ${line}`));
    lines.push("", "通用选项：", ...optionLines(GLOBAL_OPTIONS.filter((o) => spec.globals.some((key) => key === o.key))));
    if (spec.options.length) lines.push("", "命令选项：", ...optionLines(spec.options));
    lines.push("", "规则：", ...spec.rules.map((line) => `  ${line}`));
  }
  lines.push("", "全局选项可放在命令前或后；命令专属选项放在命令后。",
    "--名称=值 与 --名称 值 等价；-- 后的所有内容作为位置参数。",
    "--timeout 不可重复；model/reasoning/sandbox/approval 后值覆盖，并在 -v 下告警。",
    "其余带值选项和命令开关不可重复；通用布尔开关可重复。",
    "默认结果写标准输出；过程与告警写标准错误。-v 表示详细输出，不表示版本。",
    "--help 与 --version 跳过执行参数的必填与组合校验；未知选项或非法值仍报错。",
    "使用 cxb <命令> --help 查看详细帮助。");
  return lines.flatMap(wrap).join("\n") + "\n";
}
