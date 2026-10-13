import type { UpstreamError } from "../contracts";

/** 完整遵循冻结契约的 broker 诊断夹具，不依赖 A 路实现是否已经合并。 */
export function upstreamError(patch: Partial<UpstreamError> = {}): UpstreamError {
  return { threadId: "thread-test", turnId: "turn-test", source: "errorNotification", category: "authentication",
    httpStatusCode: 401, codexErrorInfo: { responseStreamDisconnected: { httpStatusCode: 401 } },
    message: "Reconnecting... 1/5", additionalDetails: "认证已过期", truncated: false, willRetry: true,
    firstSeenAt: 1000, lastSeenAt: 2000, count: 3, ...patch };
}
