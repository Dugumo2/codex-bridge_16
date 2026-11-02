// src/guardian.ts — Guardian 拒绝事件的协议映射与动作摘要，供 cxb 回复和展示使用。

import type { GuardianDenialRecord } from "./types";

// ---------------------------------------------------------------------------
// Wire mapping: v2 notification (camelCase) → core GuardianAssessmentEvent
// (snake_case). Verified against codex-rs 0.142: the core struct has no
// serde rename_all, its enums are snake_case/lowercase, and the v2 schema
// camelCases both keys and enum values (e.g. "applyPatch", "unifiedExec",
// "socks5Tcp"). `permissions` (RequestPermissionProfile) is the core type
// embedded directly in the v2 schema — already snake_case, pass verbatim
// (it is deny_unknown_fields on the server, so do NOT transform it).
// ---------------------------------------------------------------------------

const ACTION_KEY_MAP: Record<string, string> = {
  toolName: "tool_name",
  connectorId: "connector_id",
  connectorName: "connector_name",
  toolTitle: "tool_title",
  // writeStdin（0.160.0 v2/item.rs:667、core approvals.rs:152）：上游必填，缺了反序列化失败。
  approvalId: "approval_id",
  processId: "process_id",
};

function camelToSnake(value: string): string {
  // serde snake_case splits on uppercase only: "socks5Tcp" → "socks5_tcp".
  return value.replace(/[A-Z]/g, (c) => `_${c.toLowerCase()}`);
}

/** Enum-valued action fields whose camelCase wire values must be converted
 *  (e.g. source "unifiedExec" → "unified_exec", protocol "socks5Tcp" →
 *  "socks5_tcp"). Data fields (command, cwd, argv, ...) pass through. */
const ACTION_ENUM_VALUE_KEYS = new Set(["type", "source", "protocol"]);

/** Map the notification's action payload to the core snake_case shape. */
export function mapGuardianAction(action: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(action)) {
    const mappedKey = ACTION_KEY_MAP[key] ?? key;
    out[mappedKey] =
      ACTION_ENUM_VALUE_KEYS.has(key) && typeof value === "string" ? camelToSnake(value) : value;
  }
  return out;
}

/** Build the `event` param for thread/approveGuardianDeniedAction from a
 *  persisted denial. Throws when the stored notification is missing the
 *  pieces the server requires (id, denied status, action). */
export function buildGuardianOverrideEvent(record: GuardianDenialRecord): Record<string, unknown> {
  const n = record.notification;
  const review = (n.review ?? {}) as Record<string, unknown>;
  // The dispatcher tolerates enum-shaped values like { type: "denied" }
  // (UNSTABLE protocol); accept the same shapes here so any denial it
  // persisted can actually be overridden.
  const rawStatus = review.status;
  const status =
    typeof rawStatus === "string"
      ? rawStatus
      : rawStatus !== null && typeof rawStatus === "object" &&
          typeof (rawStatus as { type?: unknown }).type === "string"
        ? (rawStatus as { type: string }).type
        : null;
  if (status !== "denied") {
    throw new Error(`Review ${record.reviewId} is not a denial (status: ${status ?? "unknown"})`);
  }
  const action = n.action;
  if (action === null || typeof action !== "object" || typeof (action as { type?: unknown }).type !== "string") {
    throw new Error(`Review ${record.reviewId} has no action payload to approve`);
  }

  const event: Record<string, unknown> = {
    id: record.reviewId,
    turn_id: typeof n.turnId === "string" ? n.turnId : "",
    status: "denied",
    action: mapGuardianAction(action as Record<string, unknown>),
  };
  if (typeof n.targetItemId === "string") event.target_item_id = n.targetItemId;
  if (typeof n.startedAtMs === "number") event.started_at_ms = n.startedAtMs;
  if (typeof n.completedAtMs === "number") event.completed_at_ms = n.completedAtMs;
  if (typeof review.riskLevel === "string") event.risk_level = review.riskLevel;
  if (typeof review.userAuthorization === "string") event.user_authorization = review.userAuthorization;
  if (typeof review.rationale === "string") event.rationale = review.rationale;
  if (typeof n.decisionSource === "string") event.decision_source = camelToSnake(n.decisionSource);
  return event;
}

/** One-line summary of a denied action for listings and progress lines. */
export function describeGuardianAction(record: GuardianDenialRecord): string {
  const action = (record.notification.action ?? {}) as Record<string, unknown>;
  const type = typeof action.type === "string" ? action.type : "action";
  switch (type) {
    case "command":
      return `command: ${action.command}`;
    case "execve":
      return `execve: ${[action.program, ...((action.argv as string[]) ?? [])].join(" ")}`;
    case "applyPatch":
      return `patch: ${((action.files as string[]) ?? []).join(", ")}`;
    case "networkAccess":
      return `network: ${action.host}:${action.port}`;
    case "mcpToolCall":
      return `mcp tool: ${action.server}/${action.toolName}`;
    case "requestPermissions":
      return `permissions request${action.reason ? `: ${action.reason}` : ""}`;
    default:
      return type;
  }
}
