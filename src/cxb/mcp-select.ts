// src/cxb/mcp-select.ts — cxb 开线程时按线程关闭的 MCP 服务（命令面规格 3.1.4 节）。
//
// Codex 0.160.0 的 MCP 服务按线程（会话）各起一套，默认一套约 491 MB（backlog 约 253、serena 约 96）。
// thread/start、thread/resume 的 config 传 `mcp_servers.<名>.enabled=false` 只关本线程，子代理线程继承；
// 冷恢复不带这一覆盖会重新起（2026-10-18 实测 E7），所以关闭键随恢复覆盖登记给 broker，内部恢复与续跑都补传。
// 只对 Codex 配置里存在且启用的服务下发：给不存在的名字写 enabled 等于凭空造出一个不完整的服务条目。

/** 服务名：与 Codex 配置表键一致的字母、数字、下划线、点、短横线。 */
const NAME = /^[A-Za-z0-9_.-]{1,128}$/;

/** 逗号分隔的服务名单：去空白、去重、保持首次出现的顺序；任一名字不合法返回 null。 */
export function parseMcpNames(value: string): string[] | null {
  const names: string[] = [];
  for (const raw of value.split(",")) {
    const name = raw.trim();
    if (!NAME.test(name)) return null;
    if (!names.includes(name)) names.push(name);
  }
  return names;
}

export type McpWhenNever = "keep" | "off";

export interface McpSelectionInput {
  /** Codex 配置里启用的 MCP 服务名；读不到配置时为 null。 */
  configured: readonly string[] | null;
  /** 持久配置 `mcp-disabled`。 */
  disabled: readonly string[];
  /** 本次 `--no-mcp`。 */
  extraOff: readonly string[];
  /** 本次 `--mcp`：优先于关闭名单与「审批 never 时全关」。 */
  on: readonly string[];
  /** 本次有效审批策略。 */
  policy: string;
  /** 持久配置 `mcp-when-never`。 */
  whenNever: McpWhenNever;
}

export interface McpSelection {
  /** 本次下发关闭的服务，按 Codex 配置中的顺序。 */
  off: string[];
  /** 名单或参数里出现、但 Codex 配置里没有（或已停用）的名字。 */
  unknown: string[];
  /** 读不到 Codex 配置而无法下发的关闭请求（名单非空时才有意义）。 */
  unverified: boolean;
}

export function selectMcp(input: McpSelectionInput): McpSelection {
  const requested = [...input.disabled, ...input.extraOff];
  const allOff = input.policy === "never" && input.whenNever === "off";
  if (input.configured === null) {
    return { off: [], unknown: [], unverified: requested.length > 0 || allOff };
  }
  const configured = input.configured;
  const named = new Set(requested);
  const off = configured.filter((name) => (allOff || named.has(name)) && !input.on.includes(name));
  const mentioned = [...requested, ...input.on];
  const unknown = mentioned.filter((name, index) => !configured.includes(name) && mentioned.indexOf(name) === index);
  return { off, unknown, unverified: false };
}

/** 写进 thread/start、thread/resume 与恢复覆盖登记的配置键。 */
export function mcpOffConfig(off: readonly string[]): Record<string, false> {
  return Object.fromEntries(off.map((name) => [`mcp_servers.${name}.enabled`, false]));
}
