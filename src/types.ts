// src/types.ts — Protocol types for Codex app server (JSON-RPC over stdio)

import type { SandboxMode, ReasoningEffort } from "./config";

// --- JSON-RPC primitives ---

export type RequestId = string | number;

export interface JsonRpcRequest {
  id: RequestId;
  method: string;
  params?: unknown;
}

export interface JsonRpcNotification {
  method: string;
  params?: unknown;
}

export interface JsonRpcResponse {
  id: RequestId;
  result: unknown;
}

export interface JsonRpcError {
  id: RequestId;
  error: {
    code: number;
    message: string;
    data?: unknown;
  };
}

export type JsonRpcMessage =
  | JsonRpcRequest
  | JsonRpcNotification
  | JsonRpcResponse
  | JsonRpcError;

// --- Threads ---

export type { SandboxMode, ReasoningEffort } from "./config";

/** Error carrying a JSON-RPC error code for protocol-level error forwarding. */
export class RpcError extends Error {
  /**
   * @param message  Display form, "JSON-RPC error <code>: <detail>".
   * @param rpcCode  The wire code.
   * @param detail   The server's own message, un-prefixed — what a relay
   *                 (the broker) forwards, so the prefix is applied once.
   */
  constructor(message: string, public readonly rpcCode: number, public readonly detail?: string,
    /** 线上错误的 data 原样保留（broker 私有方法的 data.reason 等）。 */
    public readonly data?: unknown) {
    super(message);
    this.name = "RpcError";
  }
}

/** `account/read`. `type` is open-ended ("chatgpt", "apiKey", …) — treat an
 *  unrecognized value as inconclusive rather than as logged out. */
export interface CodexAccount {
  type?: string | null;
  email?: string | null;
  planType?: string | null;
}

export interface AccountRead {
  account?: CodexAccount | null;
  /** False when the configured provider needs no OpenAI credentials at all
   *  (third-party base URL) — such a setup is authenticated by definition. */
  requiresOpenaiAuth?: boolean | null;
}

/** item/autoApprovalReview/started|completed notification payloads (Guardian
 *  reviewing an approval request). Marked [UNSTABLE] in Codex 0.142 and
 *  "expected to change soon" — every field is optional and consumers must
 *  parse defensively, degrading to a generic progress line. */
export interface AutoApprovalReviewParams {
  threadId?: string;
  turnId?: string;
  itemId?: string;
  /** Stable identifier for the review lifecycle — the handle
   *  thread/approveGuardianDeniedAction overrides are keyed by. */
  reviewId?: string;
  targetItemId?: string | null;
  startedAtMs?: number;
  completedAtMs?: number;
  decisionSource?: string;
  /** Verdict: { status, riskLevel, userAuthorization, rationale }. */
  review?: unknown;
  /** Reviewed action, tagged by `type` (command, execve, applyPatch,
   *  networkAccess, mcpToolCall, requestPermissions). */
  action?: unknown;
  [key: string]: unknown;
}

/** A Guardian denial captured from item/autoApprovalReview/completed and
 *  persisted under the workspace's guardian/ dir so the user can override
 *  it later with `approve --guardian <review-id>`. */
export interface GuardianDenialRecord {
  reviewId: string;
  threadId: string;
  receivedAt: string;
  /** Set once the override RPC has been sent; hides it from the pending list. */
  overriddenAt?: string;
  /** Raw notification payload (camelCase v2 wire shape). */
  notification: AutoApprovalReviewParams;
}

// --- Model list ---

export interface Model {
  id: string;
  model: string;
  upgrade: string | null;
  displayName: string;
  description: string;
  hidden: boolean;
  supportedReasoningEfforts: Array<{ reasoningEffort: string; description: string }>;
  defaultReasoningEffort: string;
  inputModalities: string[];
  supportsPersonality: boolean;
  isDefault: boolean;
}

// --- Ask channel (Codex asks mid-turn, anyone answers) ---

/** On-disk shape of a mailbox question file (`{id}.json`). Written by
 *  `cxb ask` from INSIDE Codex's sandbox — which is why the mailbox
 *  lives in temp space (resolveMailboxDir), not the workspace state dir. */
export interface QuestionRecord {
  id: string;
  question: string;
  askedAt: string;
  expiresAt: string;
  workspaceDir: string;
  /** PID of the asking process (orphan detection for sweeps). */
  pid: number;
  /** Set when the deadline lapsed unanswered; the file is kept for the
   *  audit trail until `clean` sweeps it. */
  expired?: boolean;
}
