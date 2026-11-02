/** broker @cxb2 内容段；身份字段始终使用完整线程 ID。 */
export interface TreeLogPayload {
  eventId: string;
  rootThreadId: string;
  threadId: string;
  parentThreadId: string | null;
  shortId: string | null;
  sourceRole: "run" | "execution" | "subagent";
  turnId: string | null;
  itemId: string | null;
  summary: string;
  truncated: boolean;
  late?: boolean;
  coverageIssue?: "beforeBindingUnknown" | "discoveryIncomplete";
}

/** 静态时间线项；旧行及文件级标记缺少的身份保持未知。 */
export interface TreeLogEvent {
  eventId: string | null;
  ts: string;
  runId: string | null;
  rootThreadId: string | null;
  threadId: string | null;
  parentThreadId: string | null;
  shortId: string | null;
  sourceRole: TreeLogPayload["sourceRole"] | "legacy" | "log";
  turnId: string | null;
  itemId: string | null;
  kind: string;
  text: string;
  summary: string;
  truncated: boolean;
  late?: boolean;
}

export interface LogCoverageIssue {
  code: "legacySourceUnknown" | "rotationWindow" | "droppedLines" | "beforeBindingUnknown"
    | "discoveryIncomplete" | "metadataUnavailable" | "missingLog" | "trailingPartialLine";
  runId?: string;
  threadId?: string;
}

export interface LogCoverage {
  scope: "retained";
  issues: LogCoverageIssue[];
}
