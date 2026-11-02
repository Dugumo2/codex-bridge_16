// src/guardian.test.ts — Guardian 事件映射与动作摘要测试。

import { describe, test, expect } from "bun:test";
import {
  buildGuardianOverrideEvent,
  describeGuardianAction,
  mapGuardianAction,
} from "./guardian";
import type { AutoApprovalReviewParams, GuardianDenialRecord } from "./types";

function deniedNotification(overrides: Partial<AutoApprovalReviewParams> = {}): AutoApprovalReviewParams {
  return {
    threadId: "thread-1",
    turnId: "turn-1",
    reviewId: "rev-abc123",
    targetItemId: "call_1",
    startedAtMs: 1000,
    completedAtMs: 2000,
    decisionSource: "agent",
    review: { status: "denied", riskLevel: "medium", userAuthorization: "low", rationale: "not asked for" },
    action: { type: "command", source: "unifiedExec", command: "touch /tmp/x", cwd: "/tmp" },
    ...overrides,
  };
}

function record(overrides: Partial<AutoApprovalReviewParams> = {}): GuardianDenialRecord {
  return {
    reviewId: "rev-abc123",
    threadId: "thread-1",
    receivedAt: "2026-07-04T00:00:00.000Z",
    notification: deniedNotification(overrides),
  };
}

describe("mapGuardianAction", () => {
  // Casings verified against codex-rs 0.142 source: the core
  // GuardianAssessmentAction is snake_case-tagged with snake_case enum
  // values, the v2 notification camelCases both.
  test("command / execve pass through with source value converted", () => {
    expect(mapGuardianAction({ type: "command", source: "unifiedExec", command: "ls", cwd: "/" }))
      .toEqual({ type: "command", source: "unified_exec", command: "ls", cwd: "/" });
    expect(mapGuardianAction({ type: "execve", source: "shell", program: "ls", argv: ["-l"], cwd: "/" }))
      .toEqual({ type: "execve", source: "shell", program: "ls", argv: ["-l"], cwd: "/" });
  });

  test("tag values convert camelCase → snake_case", () => {
    expect(mapGuardianAction({ type: "applyPatch", cwd: "/", files: ["/a"] }).type).toBe("apply_patch");
    expect(mapGuardianAction({ type: "networkAccess", target: "t", host: "h", protocol: "socks5Tcp", port: 80 }))
      .toEqual({ type: "network_access", target: "t", host: "h", protocol: "socks5_tcp", port: 80 });
    expect(mapGuardianAction({ type: "requestPermissions", reason: null, permissions: { network: null, file_system: null } }))
      .toEqual({ type: "request_permissions", reason: null, permissions: { network: null, file_system: null } });
  });

  test("mcpToolCall keys convert to snake_case", () => {
    expect(mapGuardianAction({
      type: "mcpToolCall", server: "s", toolName: "t", connectorId: null, connectorName: null, toolTitle: "T",
    })).toEqual({
      type: "mcp_tool_call", server: "s", tool_name: "t", connector_id: null, connector_name: null, tool_title: "T",
    });
  });

  test("writeStdin 的 approvalId、processId 转成上游必填的 snake_case（0.160.0 v2/item.rs:667）", () => {
    expect(mapGuardianAction({ type: "writeStdin", approvalId: "a-1", processId: "p-1", stdin: "y\n", cwd: "/" }))
      .toEqual({ type: "write_stdin", approval_id: "a-1", process_id: "p-1", stdin: "y\n", cwd: "/" });
  });
});

describe("buildGuardianOverrideEvent", () => {
  test("maps the full notification to the core snake_case event", () => {
    expect(buildGuardianOverrideEvent(record())).toEqual({
      id: "rev-abc123",
      turn_id: "turn-1",
      status: "denied",
      target_item_id: "call_1",
      started_at_ms: 1000,
      completed_at_ms: 2000,
      risk_level: "medium",
      user_authorization: "low",
      rationale: "not asked for",
      decision_source: "agent",
      action: { type: "command", source: "unified_exec", command: "touch /tmp/x", cwd: "/tmp" },
    });
  });

  test("omits optional fields the notification lacks", () => {
    const event = buildGuardianOverrideEvent(record({
      targetItemId: null,
      startedAtMs: undefined,
      completedAtMs: undefined,
      decisionSource: undefined,
      review: { status: "denied", riskLevel: null, userAuthorization: null, rationale: null },
    }));
    expect(event).toEqual({
      id: "rev-abc123",
      turn_id: "turn-1",
      status: "denied",
      action: { type: "command", source: "unified_exec", command: "touch /tmp/x", cwd: "/tmp" },
    });
  });

  test("accepts the enum-shaped status the dispatcher tolerates", () => {
    const event = buildGuardianOverrideEvent(record({
      review: { status: { type: "denied" }, riskLevel: null, userAuthorization: null, rationale: null },
    }));
    expect(event.status).toBe("denied");
  });

  test("rejects non-denials and missing actions", () => {
    expect(() => buildGuardianOverrideEvent(record({
      review: { status: "approved", riskLevel: "low", userAuthorization: "high", rationale: null },
    }))).toThrow(/not a denial/);
    expect(() => buildGuardianOverrideEvent(record({ action: undefined }))).toThrow(/no action/);
  });
});

describe("describeGuardianAction", () => {
  test("summarizes each action type", () => {
    expect(describeGuardianAction(record())).toBe("command: touch /tmp/x");
    expect(describeGuardianAction(record({ action: { type: "networkAccess", target: "t", host: "example.com", protocol: "https", port: 443 } })))
      .toBe("network: example.com:443");
    expect(describeGuardianAction(record({ action: { type: "mcpToolCall", server: "srv", toolName: "fetch" } })))
      .toBe("mcp tool: srv/fetch");
  });
});
