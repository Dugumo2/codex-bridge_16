import { readFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import { CxbError, type ApprovalPolicyName } from "./contracts";
import type { Output } from "./output";
import { bounded } from "./wait-run";

export interface McpApprovalRisk {
  name: string;
  source: string;
  effectiveMode: string;
  risk: boolean;
  suggestion: string;
}

/** doctor 可直接展示；不包含原始配置、环境变量或凭据。 */
export interface ApprovalPreflightResult {
  check: "mcpApprovalRisk";
  status: "ok" | "warn" | "unavailable";
  servers: McpApprovalRisk[];
}

function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new CxbError("LOCAL_ERROR", "配置必须为对象");
  return value as Record<string, unknown>;
}

/** Codex 0.160.0 的 MCP 配置位于 config.toml 的 mcp_servers 表，包含 tools 覆盖。 */
export async function approvalPreflight(policy: ApprovalPolicyName, env: NodeJS.ProcessEnv = process.env): Promise<ApprovalPreflightResult> {
  try {
    const getEnv = (name: string) => Object.entries(env).find(([key]) => key.toUpperCase() === name)?.[1];
    const source = join(getEnv("CODEX_HOME") || join(getEnv("USERPROFILE") || homedir(), ".codex"), "config.toml");
    let raw: string;
    try { raw = await bounded(readFile(source, "utf8"), 30_000); } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return { check: "mcpApprovalRisk", status: "ok", servers: [] };
      throw error;
    }
    const parsed = object(Bun.TOML.parse(raw));
    const servers: McpApprovalRisk[] = [];
    for (const [name, value] of Object.entries(object(parsed.mcp_servers ?? {}))) {
      const server = object(value);
      if (server.enabled === false) continue;
      const mode = server.default_tools_approval_mode;
      const toolOverrides = object(server.tools ?? {});
      const enabled = Array.isArray(server.enabled_tools) ? server.enabled_tools : undefined;
      const disabled = Array.isArray(server.disabled_tools) ? server.disabled_tools : [];
      const tools = Object.entries(toolOverrides).filter(([tool, settings]) =>
        (!enabled || enabled.includes(tool)) && !disabled.includes(tool) && object(settings).enabled !== false);
      const explicitRisk = tools.some(([, settings]) => {
        const value = object(settings).approval_mode;
        return value !== undefined && value !== "approve";
      });
      const risk = policy === "never" && (mode !== "approve" || explicitRisk);
      const knownMode = typeof mode === "string" && ["approve", "auto", "prompt", "writes"].includes(mode);
      servers.push({ name, source, effectiveMode: explicitRisk ? "工具覆盖需要审批" : knownMode ? mode as string : mode === undefined ? "未配置" : "未知模式",
        risk, suggestion: risk ? "需要搜索/MCP 时请加 --approval auto" : "无需调整" });
    }
    return { check: "mcpApprovalRisk", status: servers.some((server) => server.risk) ? "warn" : "ok", servers };
  } catch {
    return { check: "mcpApprovalRisk", status: "unavailable", servers: [] };
  }
}

export function reportApprovalPreflight(result: ApprovalPreflightResult, out: Output): void {
  if (result.status === "unavailable") { out.progress("approval preflight: unavailable"); return; }
  const names = result.servers.filter((server) => server.risk).map((server) =>
    server.name.replace(/[\u0000-\u001f\u007f-\u009f\u2028\u2029]/g, " "));
  if (names.length) out.notice(`[cxb] 提醒：当前审批为 never，MCP 服务器 ${names.join("、")} 的工具请求可能被 Codex 拒绝；需要搜索/MCP 时请加 --approval auto。`);
}
