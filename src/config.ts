// src/config.ts — cxb 协议配置、参数校验与模板解析。

import { join } from "path";
import { existsSync, readFileSync } from "fs";
import pkg from "../package.json";

// ─── Model aliases ──────────────────────────────────────────────────────────

const MODEL_ALIASES: Record<string, string> = {
  spark: "gpt-5.3-codex-spark",
};

// ─── Effort levels ──────────────────────────────────────────────────────────

// The full effort enum the app server accepts, ascending. Not every model
// advertises every level in its `supportedReasoningEfforts`.
const VALID_EFFORTS = ["none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"] as const;

// ─── Config object ──────────────────────────────────────────────────────────

export const config = {
  // Reasoning effort levels
  reasoningEfforts: VALID_EFFORTS,
  // Highest level auto-selection will settle on. Levels above it stay reachable
  // through an explicit -r flag or user config, but are never chosen for you.
  autoEffortCeiling: "xhigh" as const,

  // Sandbox modes
  sandboxModes: ["read-only", "workspace-write", "danger-full-access"] as const,
  defaultSandbox: "workspace-write" as const,

  // Approval policies accepted by the Codex app server
  // CLI/config approval modes: server policies plus "auto", which maps to
  // approvalPolicy "on-request" reviewed by Codex's Guardian subagent
  // (approvalsReviewer "auto_review") with the interactive flow as the
  // escalation path.
  approvalModes: ["never", "on-request", "on-failure", "untrusted", "auto"] as const,
  defaultApprovalPolicy: "never" as const,

  // Timeouts
  defaultTimeout: 3600, // seconds — turn completion (1 hour)
  // Largest turn timeout accepted anywhere (setTimeout's 32-bit ceiling, in
  // seconds). The CLI flag, `config timeout`, and the peer's `timeout:`
  // header all validate against this one number.
  maxTimeoutSeconds: 2_147_483,
  requestTimeout: 30_000, // milliseconds — individual protocol requests (30s)
  clientVersion: pkg.version,
};

Object.freeze(config);

export type ReasoningEffort = (typeof config.reasoningEfforts)[number];
export type SandboxMode = (typeof config.sandboxModes)[number];

/** Wire shape of a sandbox mode: thread/start's `sandbox` accepts the
 *  kebab-case string, turn/start's `sandboxPolicy` override wants this. */
export function sandboxPolicyFor(mode: SandboxMode): { type: string } {
  switch (mode) {
    case "read-only": return { type: "readOnly" };
    case "workspace-write": return { type: "workspaceWrite" };
    case "danger-full-access": return { type: "dangerFullAccess" };
  }
}

/** The mode a `sandboxPolicy` (or a kebab-case string) names; undefined
 *  for a shape this build does not know, so protocol drift is never read
 *  as one of the three modes. */
export function sandboxModeOf(policy: unknown): SandboxMode | undefined {
  if (typeof policy === "string") {
    return (config.sandboxModes as readonly string[]).includes(policy) ? policy as SandboxMode : undefined;
  }
  switch ((policy as { type?: unknown } | null)?.type) {
    case "readOnly": return "read-only";
    case "workspaceWrite": return "workspace-write";
    case "dangerFullAccess": return "danger-full-access";
    default: return undefined;
  }
}
export type ApprovalMode = (typeof config.approvalModes)[number];

// ─── Pure utility functions ─────────────────────────────────────────────────

/** Validate that an ID contains only safe characters for file paths. */
export function validateId(id: string): string {
  if (!/^[a-zA-Z0-9_-]+$/.test(id)) {
    throw new Error(`Invalid ID: "${id}"`);
  }
  return id;
}

/**
 * Resolve model aliases. Currently: `spark → gpt-5.3-codex-spark`.
 * Passes through unknown names. Returns undefined for undefined input.
 */
export function resolveModel(model: string | undefined): string | undefined {
  if (model === undefined) return undefined;
  return MODEL_ALIASES[model] ?? model;
}

/**
 * Validate reasoning effort against known levels.
 * Throws on invalid. Returns undefined for undefined input.
 */
export function validateEffort(effort: string | undefined): ReasoningEffort | undefined {
  if (effort === undefined) return undefined;
  if (!(VALID_EFFORTS as readonly string[]).includes(effort)) {
    throw new Error(
      `Invalid effort level "${effort}". Valid levels: ${VALID_EFFORTS.join(", ")}`,
    );
  }
  return effort as ReasoningEffort;
}

// ─── Template metadata ─────────────────────────────────────────────────────

export interface TemplateMeta {
  name: string;
  description: string;
  sandbox?: string;
}

/**
 * Parse YAML frontmatter from a template string.
 * Returns the metadata fields and the body (content after frontmatter).
 */
export function parseTemplateFrontmatter(raw: string): { meta: TemplateMeta; body: string } {
  // Normalize CRLF to LF so Windows-edited templates parse correctly
  const normalized = raw.replace(/\r\n/g, "\n");
  const lines = normalized.split("\n");
  if (lines[0]?.trim() !== "---") {
    return { meta: { name: "", description: "" }, body: normalized };
  }
  const endIdx = lines.indexOf("---", 1);
  if (endIdx === -1) {
    return { meta: { name: "", description: "" }, body: normalized };
  }

  const meta: Record<string, string> = {};
  for (let i = 1; i < endIdx; i++) {
    const match = lines[i].match(/^(\w+)\s*:\s*(.+)$/);
    if (match) meta[match[1]] = match[2].trim();
  }

  const body = lines.slice(endIdx + 1).join("\n").replace(/^\n+/, "");
  return {
    meta: {
      name: meta.name ?? "",
      description: meta.description ?? "",
      sandbox: meta.sandbox,
    },
    body,
  };
}

/** 从指定目录加载模板，返回元数据和正文。 */
export function loadTemplateWithMeta(name: string, promptsDir: string): { meta: TemplateMeta; body: string } {
  return parseTemplateFrontmatter(loadTemplateRaw(name, promptsDir));
}

/** Load the raw template content (including frontmatter). */
function loadTemplateRaw(name: string, promptsDir: string): string {
  if (name.includes("/") || name.includes("\\") || name.includes("..")) {
    throw new Error(`Invalid template name: "${name}"`);
  }

  const filePath = join(promptsDir, `${name}.md`);
  if (!existsSync(filePath)) {
    throw new Error(`Template not found: ${filePath}`);
  }
  return readFileSync(filePath, "utf-8");
}

/**
 * Replace `{{VAR}}` placeholders in a template string.
 * Unknown variables are left as-is.
 */
export function interpolateTemplate(
  template: string,
  vars: Record<string, string>,
): string {
  return template.replace(/\{\{(\w+)\}\}/g, (match, key) => {
    return key in vars ? vars[key] : match;
  });
}
