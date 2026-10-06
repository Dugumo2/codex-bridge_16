import type { Readable } from "node:stream";
import { buildGuardianOverrideEvent } from "../guardian";
import { CxbError, EXIT, rpcFailure, type InboxItem, type InboxResolveReply, type ThreadSummary } from "./contracts";
import { object, safeLine } from "./inbox";
import { commandSession } from "./command-session";
import type { CommandHandler } from "./main";
import { bounded } from "./wait-run";

const PREFIX_KIND = { ap_: "approval", qu_: "question", mc_: "mcp", gd_: "guardian" } as const;

export async function readReply(stream: Readable, signal: AbortSignal): Promise<string> {
  let data!: (chunk: string) => void;
  let end!: () => void;
  let error!: (cause: unknown) => void;
  try {
    return await bounded(new Promise<string>((resolve, reject) => {
      let text = "";
      data = (chunk) => { text += chunk; };
      end = () => resolve(text);
      error = (cause) => reject(new CxbError("LOCAL_ERROR", "读取回复标准输入失败", { cause }));
      stream.setEncoding("utf8");
      stream.on("data", data); stream.once("end", end); stream.once("error", error);
      if (stream.readableEnded) end();
    }), 30_000, signal);
  } finally {
    stream.removeListener("data", data); stream.removeListener("end", end); stream.removeListener("error", error); stream.pause();
  }
}

export function encodeAnswer(item: InboxItem, content: string, warn: (text: string) => void): object {
  if (!content.trim()) throw new CxbError("USAGE", "回复不能为空或只有空白");
  const word = content.trim().toLowerCase();
  const accept = ["yes", "y", "approve", "accept"].includes(word);
  const decline = ["no", "n", "decline", "reject"].includes(word);
  let json: Record<string, unknown> | undefined;
  if (content.trimStart().startsWith("{")) {
    try { json = JSON.parse(content); } catch { throw new CxbError("USAGE", "回复内容不是合法的 JSON 对象"); }
    if (json === null || typeof json !== "object" || Array.isArray(json)) throw new CxbError("USAGE", "回复内容必须是 JSON 对象");
  }
  const params = object(item.params);
  if (item.kind === "approval" || item.kind === "guardian") {
    if (!accept && !decline) throw new CxbError("USAGE", "审批和 Guardian 事项只接受同意或拒绝");
    if (item.kind === "approval") return { decision: accept ? "accept" : "decline" };
    if (decline) return { override: false };
    try {
      if (typeof params.reviewId !== "string" || !params.reviewId) throw new Error();
      return { override: true, event: buildGuardianOverrideEvent({ reviewId: params.reviewId, threadId: item.threadId,
        receivedAt: new Date(item.createdAt).toISOString(), notification: params }) };
    } catch (cause) { throw new CxbError("LOCAL_ERROR", "Guardian 事项缺少有效的拒绝动作", { cause }); }
  }
  if (item.kind === "question") {
    const questions = Array.isArray(params.questions) ? params.questions.map(object) : [];
    const ids = new Set(questions.map((q) => q.id).filter((id): id is string => typeof id === "string"));
    if (!ids.size) throw new CxbError("LOCAL_ERROR", "事项没有可回答的问题");
    if (!json) {
      if (questions.length > 1) warn("纯文本只回答第一题，其余问题未作答；可用 JSON 按问题 ID 回答");
      return { answers: { [String(questions[0]!.id)]: { answers: [content] } } };
    }
    if (Object.keys(json).some((key) => !ids.has(key))) throw new CxbError("USAGE", "提问回复包含未知问题 ID");
    if (Object.values(json).some((v) => typeof v !== "string" && !(Array.isArray(v) && v.every((s) => typeof s === "string")))) {
      throw new CxbError("USAGE", "提问回复的 JSON 值必须是字符串或字符串数组");
    }
    return { answers: Object.fromEntries(Object.entries(json).map(([id, value]) => [id, { answers: typeof value === "string" ? [value] : value }])) };
  }
  if (params.mode !== "url" && params.mode !== "form") throw new CxbError("USAGE", "该 MCP 请求类型 cxb 暂不支持");
  if (decline) return { action: "decline", content: null, _meta: null };
  if (params.mode === "url") {
    if (!accept) throw new CxbError("USAGE", "MCP URL 请求只接受同意或拒绝");
    return { action: "accept", content: null, _meta: null };
  }
  if (!json) throw new CxbError("USAGE", "MCP 表单需要 JSON 对象，不能只回复同意");
  const schema = object(params.requestedSchema);
  const properties = object(schema.properties);
  if (Object.keys(json).some((key) => !Object.hasOwn(properties, key))) throw new CxbError("USAGE", "MCP 表单包含未知字段");
  if (Array.isArray(schema.required) && schema.required.some((key) => typeof key !== "string" || !Object.hasOwn(json!, key))) {
    throw new CxbError("USAGE", "MCP 表单缺少必填字段");
  }
  return { action: "accept", content: json, _meta: null };
}

export const reply: CommandHandler = async (ctx) => {
  const scope = commandSession(ctx);
  const [id, ...tokens] = ctx.parsed.positional;
  try {
    const content = tokens.length === 1 && tokens[0] === "-" ? await readReply(process.stdin, scope.signal) : tokens.join(" ");
    const rpc = await scope.connect();
    const { items } = await rpc.request<{ items: InboxItem[] }>("broker/inbox/list", { includeClosed: true });
    const matches = items.filter((item) => item.id === id);
    const item = matches[0];
    const prefix = Object.entries(PREFIX_KIND).find(([p]) => id.startsWith(p));
    if (matches.length !== 1 || !item || (prefix && prefix[1] !== item.kind)) throw new CxbError("NOT_FOUND", "找不到类型匹配的事项", { id });
    const answer = encodeAnswer(item, content, (text) => ctx.out.progress(text));
    let result: InboxResolveReply;
    try { result = await rpc.request<InboxResolveReply>("broker/inbox/resolve", { id, answer }); }
    catch (cause) {
      const { code, reason } = rpcFailure(cause);
      if (["expired", "cancelled", "notFound"].includes(reason ?? "") || (cause instanceof CxbError && cause.code === "NOT_FOUND")) {
        throw new CxbError("NOT_FOUND", "事项已过期、撤销或不存在", { id, cause });
      }
      if (reason === "alreadyResolved" || code === -32602) throw new CxbError("USAGE", "事项已用其他答案处理，或 broker 拒绝答案格式", { id, cause });
      if (reason === "purgePending") throw new CxbError("THREAD_HELD", "线程正在删除，请稍后重试", { id, cause });
      if (reason === "rpcFailed") throw new CxbError("CODEX_FAILED", "Guardian 恢复线程或覆盖拒绝失败", { id, cause });
      if (cause instanceof CxbError && cause.code === "INTERRUPTED") throw cause;
      throw new CxbError("BROKER_UNAVAILABLE", "broker 未能交付回复，请稍后重试", { id, cause, retryable: true });
    }
    if (item.kind === "guardian" && object(answer).override === true) {
      let shortId = item.threadId.slice(0, 8);
      try {
        const { threads } = await rpc.request<{ threads: ThreadSummary[] }>("broker/threads/list", {});
        shortId = threads.find((t) => t.threadId === item.threadId)?.shortId ?? item.threadId;
      } catch { /* 回复已提交，短 ID 查询失败不改变交付结果。 */ }
      ctx.out.notice(`用 cxb run --resume ${safeLine(shortId)} "继续" 开下一轮`);
    }
    ctx.out.result(`已回复 ${safeLine(id)}（${item.kind}）。`);
    ctx.out.resultJson({ ok: true, command: "reply", id, kind: item.kind, accepted: true,
      duplicate: result.duplicate, submittedAt: new Date().toISOString() });
    return EXIT.OK;
  } finally { scope.dispose(); }
};
