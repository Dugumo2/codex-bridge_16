// src/cxb/args.ts — 参数解析与 11 个命令的选项表（命令面规格 2.1、2.2 节与第 3 节；改规格处以 S4-计划第 2 节、
// S1-评审第 27 节为准）。
//
// 只做语法与取值校验：不读文件、标准输入或 broker，不写输出、不改退出状态；用法错误抛 CxbError("USAGE")，
// 消息不带 `cxb: error:` 前缀（由 output.ts 统一加）。被删命令与旧名报错并给替代命令，不接受为隐藏别名。

import { resolve } from "node:path";
import { config, validateId, type ApprovalMode, type ReasoningEffort, type SandboxMode } from "../config";
import { CONFIG_SCHEMA, isConfigKey } from "./config-schema";
import { CxbError } from "./contracts";

export type CommandName = "run" | "review" | "show" | "ls" | "inbox" | "reply" | "wait" | "stop" | "rm" | "config" | "doctor";
export type OptionValue = string | number | boolean;
type ValueParser = (value: string, label: string) => OptionValue;

export interface GlobalOptions {
  help: boolean;
  version: boolean;
  verbose: boolean;
  json: boolean;
  dir: string;
  timeout: number;
  model?: string;
  reasoning?: ReasoningEffort;
  sandbox: SandboxMode;
  approval: ApprovalMode;
}

/** 只含当前命令的选项；未指定且没有静态默认值的字段不出现。 */
export interface CommandOptions {
  "converge-timeout"?: number;
  file?: string;
  resume?: string;
  continue?: boolean;
  run?: string;
  template?: string;
  goal?: string;
  budget?: number;
  memory?: boolean;
  detach?: boolean;
  mode?: "pr" | "uncommitted" | "commit" | "custom";
  ref?: string;
  base?: string;
  tail?: boolean;
  log?: boolean;
  follow?: boolean;
  server?: boolean;
  limit?: number;
  full?: boolean;
  all?: boolean;
  local?: boolean;
  kind?: "approval" | "question" | "mcp" | "guardian";
  clear?: boolean;
  abandon?: boolean;
  purge?: boolean;
  unset?: boolean;
}

export interface ParseWarning {
  message: string;
  /** 覆盖告警只在 verbose 下展示；review 忽略选项的告警始终展示。 */
  verboseOnly: boolean;
}

export interface ParsedCommand {
  /** 总帮助与总版本没有命令名。 */
  command: CommandName | null;
  globals: GlobalOptions;
  options: CommandOptions;
  /** 原样保留位置参数；提示词/回复由命令层按一个空格连接。 */
  positional: string[];
  /** 规范化后的选项键，不含短横线；供命令层合并配置时区分显式值。 */
  explicit: ReadonlySet<string>;
  /** 解析器只收集告警，不写 stdout/stderr。 */
  warnings: ParseWarning[];
}

export interface OptionSpec {
  key: keyof GlobalOptions | keyof CommandOptions;
  flags: readonly string[];
  description: string;
  valueName?: string;
  parse?: ValueParser;
  defaultValue?: OptionValue;
  repeat?: "overwrite" | "idempotent";
}

export interface CommandSpec {
  description: string;
  usage: readonly string[];
  globals: readonly (keyof GlobalOptions)[];
  options: readonly OptionSpec[];
  rules: readonly string[];
  validate: (parsed: ParsedCommand) => void;
}

function usage(message: string): never {
  throw new CxbError("USAGE", message);
}

function nonempty(value: string, label: string): string {
  if (!value.trim() || value.includes("\0")) usage(`${label} 需要非空且不含空字符的值。`);
  return value;
}

function choice(values: readonly string[]): ValueParser {
  return (value, label) => {
    if (!values.includes(value)) usage(`${label} 必须是：${values.join("、")}。`);
    return value;
  };
}

function integer(max = Number.MAX_SAFE_INTEGER): ValueParser {
  return (value, label) => {
    const number = Number(value);
    if (!/^[0-9]+$/.test(value) || !Number.isSafeInteger(number) || number < 1 || number > max) {
      usage(`${label} 必须是 1 至 ${max} 的十进制整数。`);
    }
    return number;
  };
}

function model(value: string, label: string): string {
  if (!/^[a-zA-Z0-9._\-/:\[\]]+$/.test(value)) usage(`${label} 含有非法模型名字符。`);
  return value;
}

function id(value: string, label: string): string {
  try {
    return validateId(value);
  } catch {
    usage(`${label} 只允许字母、数字、下划线和短横线。`);
  }
}

/** 沿用旧模板加载器的基名规则，并排除 Windows 非法字符和设备保留名。 */
function templateName(value: string, label: string): string {
  nonempty(value, label);
  if (value.includes("..") || /[<>:"/\\|?*\x00-\x1f]/.test(value)) {
    usage(`${label} 不能包含路径分隔符、连续两个点或 Windows 非法文件名字符。`);
  }
  if (/^(?:con|prn|aux|nul|com[1-9¹²³]|lpt[1-9¹²³]|conin\$|conout\$)(?: *\.| *$)/i.test(value)) {
    usage(`${label} 不能使用 Windows 设备保留名。`);
  }
  return value;
}

function gitRef(value: string, label: string): string {
  if (!value || value.startsWith("-") || /[;|&`$()<>\\'"\s\x00-\x1f\x7f]/.test(value)) {
    usage(`${label} 不能以短横线开头，也不能包含空白、控制字符或 shell 元字符。`);
  }
  return value;
}

function flag(key: OptionSpec["key"], flags: string[], description: string): OptionSpec {
  return { key, flags, description, defaultValue: false };
}

function value(key: OptionSpec["key"], flags: string[], valueName: string, description: string,
  parse: ValueParser = nonempty, defaultValue?: OptionValue): OptionSpec {
  return { key, flags, valueName, description, parse, defaultValue };
}

export const GLOBAL_OPTIONS: readonly OptionSpec[] = [
  { ...flag("help", ["-h", "--help"], "显示总帮助或当前命令帮助。"), repeat: "idempotent" },
  { ...flag("version", ["--version"], "显示 cxb 版本。"), repeat: "idempotent" },
  { ...flag("verbose", ["-v", "--verbose"], "将过程和覆盖告警写入标准错误。"), repeat: "idempotent" },
  { ...flag("json", ["--json"], "输出机器可读 JSON；可与 verbose 同用。"), repeat: "idempotent" },
  value("dir", ["-d", "--dir"], "目录", "工作区目录，默认当前目录。", (v, l) => resolve(nonempty(v, l))),
  value("timeout", ["--timeout"], "秒", `等待期限，默认 ${config.defaultTimeout} 秒；最大 ${config.maxTimeoutSeconds} 秒。`, integer(config.maxTimeoutSeconds), config.defaultTimeout),
  { ...value("model", ["-m", "--model"], "名称", "模型名；run 续跑默认沿用线程原模型，新开按 config 或自动选择；允许字母、数字及 . _ - / : [ ]。", model), repeat: "overwrite" },
  { ...value("reasoning", ["-r", "--reasoning"], "级别", `推理级别：${config.reasoningEfforts.join("|")}；run 续跑默认沿用原强度；换模型不带 -r 时按 config 或自动选择（最高 xhigh），无法确定时要求 -r。`, choice(config.reasoningEfforts)), repeat: "overwrite" },
  { ...value("sandbox", ["-s", "--sandbox"], "模式", `沙箱：${config.sandboxModes.join("|")}；默认 workspace-write。`, choice(config.sandboxModes), config.defaultSandbox), repeat: "overwrite" },
  { ...value("approval", ["--approval"], "策略", `审批：${config.approvalModes.join("|")}；默认 never。`, choice(config.approvalModes), config.defaultApprovalPolicy), repeat: "overwrite" },
];

const COMMON: readonly (keyof GlobalOptions)[] = ["help", "version", "verbose", "json", "dir"];
const EXECUTION: readonly (keyof GlobalOptions)[] = [...COMMON, "timeout", "model", "reasoning", "sandbox", "approval"];
const resume = value("resume", ["--resume"], "线程ID", "恢复已有线程。", id);
const convergeTimeout = value("converge-timeout", ["--converge-timeout"], "秒",
  "子代理收敛总期限，默认 3600 秒；首次收敛起算，跨汇总、detach、待答不重置，独立于 --timeout。",
  (value) => CONFIG_SCHEMA["converge-timeout"].parse(value), 3600);
const convergenceRules = ["子代理运行会等待受管后代收敛，再自动汇总到同一运行；期间上一轮输出只是阶段结果。",
  "最多新开 5 次汇总轮、50 个自动父轮；连续 3 轮无动作会刹车。达到最后一次额度仍允许当前轮完成核实。",
  "期限、预算、次数或归属核实异常会停止已确认自有范围并报告原因；未确认停止不等于已清理。",
  "--timeout 沿用命令等待期限；wait 超时只结束等待，未 detach 的前台 run 断开可能触发停止。"];
const REVIEW_MODES = ["pr", "uncommitted", "commit", "custom"] as const;

export const COMMANDS: Readonly<Record<CommandName, CommandSpec>> = {
  run: {
    description: "发送提示词并等待结果",
    usage: ["cxb run [提示词 ...] [选项]", "cxb run - | --file <文件> [选项]", "cxb run --resume <线程ID> --continue [补充说明 ...] [选项]"],
    globals: EXECUTION,
    options: [
      convergeTimeout,
      value("file", ["-f", "--file"], "文件", "从 UTF-8 文件读取提示词。"), resume,
      flag("continue", ["--continue"], "按原运行续跑，位置参数作为本次补充。"),
      value("run", ["--run"], "运行ID", "指定续跑来源，默认最近一次运行。", id),
      value("template", ["--template"], "名称", "套用模板；名称支持中文、空格和单点，禁止路径、连续双点及 Windows 非法名。", templateName),
      value("goal", ["--goal"], "目标", "创建或替换目标。"),
      value("budget", ["--budget"], "令牌数", "目标令牌预算，必须是正的安全整数。", integer()),
      flag("memory", ["--memory"], "允许 Codex 记忆新线程，默认关闭。"),
      flag("detach", ["--detach"], "交给 broker 托管后返回运行句柄。"),
    ],
    rules: ["提示词按空格连接；位置提示词、单独的 -（标准输入）、--file 三选一。",
      "--resume 需要新提示词或 --continue；--continue 必须配合 --resume。",
      "--continue 仅接受位置补充，不接受标准输入或文件；补充不替换原始要求；不能与 --goal、--template 同用。",
      "--run 必须配合 --continue；--budget 必须配合 --goal。",
      "空白提示词无效；文件与标准输入的内容由执行层读取并校验。", ...convergenceRules],
    validate: validateRun,
  },
  review: {
    description: "执行一次只读代码审查",
    usage: ["cxb review [审查说明 ...] [选项]", "cxb review --mode <模式> [--ref <引用>] [--base <分支>]", "cxb review --resume <线程ID> [审查说明 ...] [选项]"],
    globals: EXECUTION,
    options: [convergeTimeout, value("mode", ["--mode"], "模式", `审查模式：${REVIEW_MODES.join("|")}；默认 pr。`, choice(REVIEW_MODES), "pr"),
      value("ref", ["--ref"], "引用", "仅 commit 模式可用，默认 HEAD。", gitRef),
      value("base", ["--base"], "分支", "仅 pr 模式可用，默认自动检测，失败回退 main。", gitRef), resume],
    rules: ["未显式设置模式时，提供审查说明会选择 custom；custom 必须有说明。",
      "显式 pr、uncommitted、commit 均不接受位置说明。",
      "显式 sandbox 或 approval 先校验，再始终告警并忽略；固定 read-only 和 never。",
      "不接受 --goal 或 --budget；引用不能包含 shell 元字符。", ...convergenceRules],
    validate: validateReview,
  },
  show: {
    description: "查看最近结果、日志或进展",
    usage: ["cxb show <线程ID> [--tail|--log|--follow] [选项]"], globals: COMMON,
    options: [flag("tail", ["--tail"], "显示最近进展，默认 50 行。"), flag("log", ["--log"], "显示完整本地日志。"),
      flag("follow", ["--follow"], "跟随至结束，隐含 --tail。"), flag("server", ["--server"], "强制从服务器读取。"),
      value("limit", ["--limit"], "数量", "最近进展行数，必须是正的安全整数。", integer(), 50),
      flag("full", ["--full"], "日志包含请求/响应元数据和所有事件类型。")],
    rules: ["必须提供一个线程 ID；默认显示最近一轮结果。", "--log 与 --tail、--follow 互斥；--tail 与 --follow 可以同用。",
      "--follow --json 输出逐行 JSON（NDJSON），最后一行是终态。"],
    validate: (p) => {
      positionCount(p, 1, 1);
      if (p.options.log && (p.options.tail || p.options.follow)) usage("--log 不能与 --tail 或 --follow 同用。");
      if (p.options.follow) p.options.tail = true;
    },
  },
  ls: {
    description: "列出当前工作区线程", usage: ["cxb ls [--all] [--limit <数量>] [--local] [选项]"], globals: COMMON,
    options: [flag("all", ["--all"], "列出全部线程，不限制数量。"),
      value("limit", ["--limit"], "数量", "线程数，1 至 10000，默认 20。", integer(10000), 20), flag("local", ["--local"], "只读取本地账本。")],
    rules: ["默认从服务器合并线程；--local 可与 --all 同用。", "--all 与显式 --limit 互斥；infinity 请写成 --all。"],
    validate: (p) => {
      positionCount(p, 0, 0);
      if (p.options.all && p.explicit.has("limit")) usage("--all 不能与 --limit 同用。");
      if (p.options.all) p.options.limit = Infinity;
    },
  },
  inbox: {
    description: "列出待回应事项", usage: ["cxb inbox [--kind <类型>] [--all] [--limit <数量>] [选项]"], globals: COMMON,
    options: [value("kind", ["--kind"], "类型", "筛选：approval|question|mcp|guardian。", choice(["approval", "question", "mcp", "guardian"])),
      flag("all", ["--all"], "扫描所有工作区，需要 broker 支持。"),
      value("limit", ["--limit"], "数量", "最多显示的事项数，正的安全整数；默认不限制。", integer())],
    rules: ["默认列出本工作区所有未完成事项，最早创建的在前。", "--all 只扩大工作区范围，可与 --limit 同用。"],
    validate: (p) => positionCount(p, 0, 0),
  },
  reply: {
    description: "回答提问、审批或 MCP 表单", usage: ["cxb reply <事项ID> <回复内容 ...> [选项]", "cxb reply <事项ID> - [选项]"], globals: COMMON, options: [],
    rules: ["内容按空格连接；单独的 - 从标准输入读取，与位置内容互斥。",
      "接受：yes/y/approve/accept；拒绝：no/n/decline/reject；不区分大小写。",
      "ap_ 和 gd_ 只接受同意/拒绝；qu_ 的 JSON 值须为字符串或字符串数组。",
      "以 { 开头的内容须是 JSON 对象；MCP 字段与无前缀 ID 留给 broker 校验。"],
    validate: validateReply,
  },
  wait: {
    description: "等待待答事项或整次运行结束", usage: ["cxb wait [线程ID] [--timeout <秒>] [选项]"], globals: [...COMMON, "timeout"], options: [],
    rules: ["不带 ID 等待本工作区新事项；带 ID 等待整次运行结束。", "默认等待 3600 秒，不允许 --timeout 0。"],
    validate: (p) => {
      positionCount(p, 0, 1);
      if (p.globals.timeout === 0 && p.explicit.has("timeout")) usage("wait 不允许 --timeout 0。");
    },
  },
  stop: {
    description: "停止运行并暂停或清除目标", usage: ["cxb stop <线程ID> [--clear] [--run <运行ID>] [--abandon] [选项]"], globals: COMMON,
    options: [flag("abandon", ["--abandon"], "放弃指定运行的树清理责任，必须带 --run；不表示已经停止。"), flag("clear", ["--clear"], "同时放弃目标；默认仅暂停目标。"),
      value("run", ["--run"], "运行ID", "停止或核实指定运行（含已失联、未确认停下的历史运行）；默认只看最近一次运行。", id)],
    rules: ["必须提供一个线程 ID；停止确认由 broker 处理。",
      "归档被历史运行挡住时，按 cxb rm 列出的运行 ID 用 --run 核实。"], validate: (p) => {
      positionCount(p, 1, 1);
      if (p.options.abandon && !p.options.run) usage("--abandon 必须带 --run <运行ID>。");
      if (p.options.abandon && p.options.clear) usage("--abandon 不能与 --clear 同时使用。");
    },
  },
  rm: {
    description: "归档或永久删除线程", usage: ["cxb rm <线程ID> [--purge] [选项]"], globals: COMMON,
    options: [flag("purge", ["--purge"], "永久删除；默认只归档。")],
    rules: ["必须提供一个线程 ID；运行中不能直接永久删除，由 broker 检查。"], validate: (p) => positionCount(p, 1, 1),
  },
  config: {
    description: "查看或修改配置，列出模板", usage: ["cxb config [选项]", "cxb config <键> [值] [--unset] [选项]", "cxb config templates [选项]"], globals: COMMON,
    options: [flag("unset", ["--unset"], "删除指定键；无键时清空全部配置。")],
    rules: ["无参数显示配置；仅给键显示有效值；键和值用于设置。", "--unset 与值互斥；templates 不接受值或 --unset，只读列出模板。",
      ...Object.entries(CONFIG_SCHEMA).map(([key, spec]) => `${key}：${spec.hint}。`)], validate: validateConfig,
  },
  doctor: {
    description: "诊断环境、broker、模型与 MCP", usage: ["cxb doctor [选项]"], globals: COMMON, options: [],
    rules: ["不接受位置参数；检查版本、连接、模型、目录权限及 MCP 审批风险。"], validate: (p) => positionCount(p, 0, 0),
  },
};

export const REMOVED_COMMANDS: Readonly<Record<string, string>> = {
  update: "更新请使用安装流程；环境检查请用 cxb doctor。",
  skill: "请用 cxb config templates 查看模板；技能同步已取消。",
  clean: "过期文件由 broker 回收；归档线程请用 cxb rm <线程ID>。",
  jobs: "请用 cxb ls 或 cxb show <线程ID>。",
  templates: "请用 cxb config templates。", threads: "请用 cxb ls。", kill: "请用 cxb stop <线程ID>。",
  follow: "请用 cxb show <线程ID> --follow。", output: "请用 cxb show <线程ID>。",
  progress: "请用 cxb show <线程ID> --tail。", peek: "请用 cxb show <线程ID> --server。",
  approve: "请用 cxb inbox 查看事项，再用 cxb reply <事项ID> yes。",
  decline: "请用 cxb inbox 查看事项，再用 cxb reply <事项ID> no。",
  answer: "请用 cxb inbox 和 cxb reply <事项ID> <回复>。", questions: "请用 cxb inbox --kind question，再用 cxb reply 回答。",
  next: "请用 cxb wait。", delete: "请用 cxb rm <线程ID>。",
  health: "请用 cxb doctor。", models: "请用 cxb doctor。", version: "请用 cxb --version。",
  ask: "ask 通道已取消；请用 cxb inbox、cxb reply 和 cxb run --resume <线程ID> <回复>。",
  peer: "peer 通道已取消；请用 cxb inbox、cxb reply 和 cxb wait。",
  peers: "peer 通道已取消；请用 cxb inbox、cxb reply 和 cxb wait。",
  send: "peer 通道已取消；新任务请用 cxb run，回答请用 cxb reply。",
  task: "peer 任务已取消；请用 cxb run。", tasks: "peer 任务已取消；请用 cxb ls。",
};

function positionCount(p: ParsedCommand, min: number, max: number): void {
  if (p.positional.length < min || p.positional.length > max) usage(`${p.command} 需要 ${min === max ? min : `${min} 至 ${max}`} 个位置参数。`);
  if (max === 1 && p.positional[0] !== undefined) id(p.positional[0], "线程 ID");
}

function inputKind(tokens: string[], label: string): "empty" | "stdin" | "text" {
  if (tokens.includes("-")) {
    if (tokens.length !== 1) usage(`${label} 的标准输入标记 - 不能与其他内容同用。`);
    return "stdin";
  }
  if (!tokens.length) return "empty";
  nonempty(tokens.join(" "), label);
  return "text";
}

function validateRun(p: ParsedCommand): void {
  const o = p.options;
  const input = inputKind(p.positional, "提示词");
  if (o.file && input !== "empty") usage("--file 不能与位置提示词或标准输入同用。");
  if (o.continue && !o.resume) usage("--continue 必须与 --resume 同用。");
  if (o.continue && (o.file || input === "stdin")) usage("--continue 仅接受位置补充说明，不能与 --file 或标准输入同用。");
  if (o.run && !o.continue) usage("--run 必须与 --continue 同用。");
  // 续跑沿用原运行的要求与目标：开工前设 goal 会让 broker 按「goal 仍 active」拒绝（详细设计 3.6 节）。
  if (o.continue && (o.goal !== undefined || o.template !== undefined)) usage("--continue 不能与 --goal 或 --template 同用：续跑沿用原运行的要求与目标。");
  if (o.budget !== undefined && !o.goal) usage("--budget 必须与 --goal 同用。");
  if (input === "empty" && !o.file && !o.continue) usage("run 需要提示词、标准输入或文件；续跑请加 --resume 和 --continue。");
}

function validateReview(p: ParsedCommand): void {
  const o = p.options;
  if (p.positional.length) {
    nonempty(p.positional.join(" "), "审查说明");
    if (p.explicit.has("mode") && o.mode !== "custom") usage("显式 pr、uncommitted、commit 模式不接受位置说明。");
    o.mode = "custom";
  }
  if (o.mode === "custom" && !p.positional.length) usage("custom 模式需要审查说明。");
  if (o.ref !== undefined && o.mode !== "commit") usage("--ref 仅适用于 commit 模式。");
  if (o.base !== undefined && o.mode !== "pr") usage("--base 仅适用于 pr 模式。");
  if (o.mode === "commit" && o.ref === undefined) o.ref = "HEAD";
}

const ACCEPT = ["yes", "y", "approve", "accept"];
const DECLINE = ["no", "n", "decline", "reject"];

function validateReply(p: ParsedCommand): void {
  if (p.positional.length < 2) usage("reply 需要事项 ID 和回复内容。");
  const requestId = id(p.positional[0], "事项 ID");
  if (inputKind(p.positional.slice(1), "回复") === "stdin") return;
  const content = p.positional.slice(1).join(" ");
  const decision = [...ACCEPT, ...DECLINE].includes(content.toLowerCase());
  if (/^(ap_|gd_)/.test(requestId) && !decision) usage("审批和 Guardian 事项只接受同意或拒绝。");
  if (!content.startsWith("{")) return;
  let answer: unknown;
  try {
    answer = JSON.parse(content);
  } catch {
    usage("回复内容不是合法的 JSON 对象。");
  }
  if (answer === null || typeof answer !== "object" || Array.isArray(answer)) usage("回复内容必须是 JSON 对象。");
  if (requestId.startsWith("qu_") && Object.values(answer).some((v) =>
    typeof v !== "string" && !(Array.isArray(v) && v.every((item) => typeof item === "string")))) {
    usage("提问回复的 JSON 值必须是字符串或字符串数组。");
  }
}

function validateConfig(p: ParsedCommand): void {
  positionCount(p, 0, 2);
  const [key, raw] = p.positional;
  if (key === "templates") {
    if (raw !== undefined || p.options.unset) usage("config templates 只允许列出模板，不接受值或 --unset。");
    return;
  }
  if (key === undefined) return;
  if (!isConfigKey(key)) usage("不支持的配置键；请用 cxb config --help 查看可用键。");
  if (raw !== undefined) {
    if (p.options.unset) usage("--unset 不能与配置值同用。");
    CONFIG_SCHEMA[key].parse(raw);
  }
}

function defaults(specs: readonly OptionSpec[]): Record<string, OptionValue> {
  return Object.fromEntries(specs.filter((s) => s.defaultValue !== undefined).map((s) => [s.key, s.defaultValue!]));
}

/**
 * 参数报错时恢复输出模式与命令上下文；不校验值、不抛用法错误。
 * 选项值不能成为命令名；首个位置词未知时保持 null，避免把后续内容猜成命令。
 * 遇到 -- 就停止。缺值后的另一选项仍可识别，与 parseArgv 的缺值边界一致。
 */
export function prescanArgv(argv: string[]): { json: boolean; verbose: boolean; command: string | null } {
  let json = false;
  let verbose = false;
  let command: string | null = null;
  let sawCommandPosition = false;
  for (let i = 0; i < argv.length; i++) {
    const token = argv[i];
    if (token === "--") break;
    if (token !== "-" && token.startsWith("-")) {
      const eq = token.startsWith("--") ? token.indexOf("=") : -1;
      const name = eq < 0 ? token : token.slice(0, eq);
      const spec = GLOBAL_OPTIONS.find((s) => s.flags.includes(name))
        ?? (command && Object.hasOwn(COMMANDS, command)
          ? COMMANDS[command as CommandName].options.find((s) => s.flags.includes(name))
          : undefined);
      if (spec?.parse) {
        const next = argv[i + 1];
        if (eq < 0 && next !== undefined && (!next.startsWith("-") || next === "-")) i++;
      } else if (eq < 0) {
        if (spec?.key === "json") json = true;
        if (spec?.key === "verbose") verbose = true;
      }
      continue;
    }
    if (!sawCommandPosition) {
      sawCommandPosition = true;
      if (Object.hasOwn(COMMANDS, token) || Object.hasOwn(REMOVED_COMMANDS, token)) command = token;
    }
  }
  return { json, verbose, command };
}

/** argv 不含运行时与脚本名；不读文件、标准输入或 broker，不改变进程退出状态。 */
export function parseArgv(argv: string[]): ParsedCommand {
  const globals: Record<string, OptionValue> = { ...defaults(GLOBAL_OPTIONS), dir: resolve(".") };
  const options: Record<string, OptionValue> = {};
  const explicit = new Set<string>();
  const warnings: ParseWarning[] = [];
  const positional: string[] = [];
  let command: CommandName | null = null;
  let terminated = false;
  for (let i = 0; i < argv.length; i++) {
    const token = argv[i];
    if (!terminated && token === "--") {
      terminated = true;
      continue;
    }
    if (!terminated && token !== "-" && token.startsWith("-")) {
      const eq = token.startsWith("--") ? token.indexOf("=") : -1;
      const name = eq < 0 ? token : token.slice(0, eq);
      const global = GLOBAL_OPTIONS.find((s) => s.flags.includes(name));
      const spec = global ?? (command ? COMMANDS[command].options.find((s) => s.flags.includes(name)) : undefined);
      if (!spec) usage("未知选项；请用 cxb --help 或 cxb <命令> --help 查看用法。");
      let parsed: OptionValue = true;
      if (spec.parse) {
        const raw = eq < 0 ? argv[++i] : token.slice(eq + 1);
        if (raw === undefined || (eq < 0 && raw.startsWith("-") && raw !== "-")) usage(`${spec.flags.at(-1)} 缺少值。`);
        parsed = spec.parse(raw, spec.flags.at(-1)!);
      } else if (eq >= 0) {
        usage(`${spec.flags.at(-1)} 是开关，不接受值。`);
      }
      if (explicit.has(spec.key)) {
        if (!spec.repeat) usage(`${spec.flags.at(-1)} 不能重复指定。`);
        if (spec.repeat === "overwrite") warnings.push({ message: `${spec.flags.at(-1)} 重复指定，后者覆盖前者。`, verboseOnly: true });
      }
      explicit.add(spec.key);
      (global ? globals : options)[spec.key] = parsed;
      continue;
    }
    if (!command) {
      if (Object.hasOwn(REMOVED_COMMANDS, token)) usage(`命令 ${token} 已删除。${REMOVED_COMMANDS[token]}`);
      if (!Object.hasOwn(COMMANDS, token)) usage("未知命令；请用 cxb --help 查看可用命令。");
      command = token as CommandName;
      Object.assign(options, defaults(COMMANDS[command].options));
    } else {
      positional.push(token);
    }
  }
  // 无参调用显示总帮助；其余缺命令的调用必须明确要求帮助或版本。
  if (!argv.length) globals.help = true;
  if (!command && !globals.help && !globals.version) usage("缺少命令；请用 cxb --help 查看用法。");
  const parsed: ParsedCommand = { command, globals: globals as unknown as GlobalOptions, options, positional, explicit, warnings };
  if (command) {
    const spec = COMMANDS[command];
    for (const global of GLOBAL_OPTIONS) {
      if (explicit.has(global.key) && !spec.globals.includes(global.key as keyof GlobalOptions)) usage(`${command} 不支持 ${global.flags.at(-1)}。`);
    }
    if (command === "review") {
      for (const key of ["sandbox", "approval"] as const) {
        if (explicit.has(key)) warnings.push({ message: `review 忽略显式 --${key}；审查固定使用 read-only 沙箱和 never 审批。`, verboseOnly: false });
      }
      parsed.globals.sandbox = "read-only";
      parsed.globals.approval = "never";
    }
    // 帮助和版本不要求完整的执行参数，但选项名称、值与适用命令仍须合法。
    if (!parsed.globals.help && !parsed.globals.version) spec.validate(parsed);
  }
  return parsed;
}
