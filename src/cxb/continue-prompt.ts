// src/cxb/continue-prompt.ts — `cxb run --resume <id> --continue [补充说明]` 的纯逻辑部分（S4 N3）：
// 沿 continuedFrom 追原始要求、拼「丁＋」续跑提示（S1-评审第 25 节）、本地拒绝与 broker 拒绝原因到退出码的映射
// （命令面规格 3.1 节 `--continue`、详细设计 2.3 节「续跑的条件请求」）。
// 运行记录的读取在 ledger.ts（只读 broker 写的文件）；本文件不读写任何文件。

import { CxbError, rpcFailure, type ProgressItem, type RunResult } from "./contracts";
import { subagentRefusal } from "./subagents";

/** 追溯原始要求的最大层数：防止损坏的记录成环或过长。 */
export const MAX_CHAIN_DEPTH = 100;
/** 提示里最多列出的已完成步骤与观察到的文件数（其余只给总数）。 */
export const MAX_LISTED_DONE = 20;
export const MAX_LISTED_FILES = 30;

export interface RunChain {
  /** 沿 continuedFrom 能追到的最早一次运行（原始要求取自它）。 */
  original: RunResult;
  /** 链在某处断开（记录缺失、损坏或成环）时为 true：原始要求可能不是真正最初的那一次（S3-计划 8.2）。 */
  broken: boolean;
  /** 断开处缺的运行 ID（broken 时有值）。 */
  missingRunId: string | null;
}

/** 沿 continuedFrom 追到最初那次运行；`read` 默认读工作区的运行记录，测试可注入。 */
export function resolveRunChain(from: RunResult, read: (runId: string) => RunResult | null): RunChain {
  const seen = new Set<string>([from.runId]);
  let current = from;
  for (let depth = 0; current.continuedFrom !== null; depth++) {
    const next = current.continuedFrom;
    if (depth >= MAX_CHAIN_DEPTH || seen.has(next)) return { original: current, broken: true, missingRunId: next };
    let record: RunResult | null;
    try { record = read(next); } catch { record = null; }
    if (!record) return { original: current, broken: true, missingRunId: next };
    seen.add(next);
    current = record;
  }
  return { original: current, broken: false, missingRunId: null };
}

/**
 * 本地就能判定不可续跑的情形（不连 broker 也成立，详细设计 3.6 节「CLI 在本地就拒绝」）：
 * review 线程、正常完成的运行、旁观别人的 joined 运行。其余由 broker 在开工那一刻核实。
 */
export function localContinueRefusal(run: RunResult, threadLabel: string): CxbError | null {
  if (run.reviewThreadId !== null || run.meta?.source === "review") {
    return new CxbError("LOCAL_ERROR", `运行 ${run.runId} 是代码审查，不能续跑；请重新执行 cxb review。`, { id: threadLabel });
  }
  if (run.joined) {
    return new CxbError("LOCAL_ERROR", `运行 ${run.runId} 是旁观别的客户端的 turn，不能续跑；请用 cxb show ${threadLabel} 查看线程。`, { id: threadLabel });
  }
  if (run.state === "ended" && run.interruptReason === null) {
    return new CxbError("LOCAL_ERROR", `运行 ${run.runId} 已正常结束，不需要续跑；请直接写新要求：cxb run --resume ${threadLabel} "<新要求>"。`, { id: threadLabel });
  }
  return null;
}

/** broker 对带 cxbContinue 的 turn/start 的拒绝 → CxbError（命令面规格 3.1 节的退出码表）。不是续跑拒绝时返回 null。 */
export function continueRefusal(error: unknown, threadLabel: string): CxbError | null {
  const { code, reason } = rpcFailure(error);
  if (code === null || reason === null) return null;
  const opts = { id: threadLabel, cause: error };
  switch (reason) {
    case "stopping":
      return new CxbError("THREAD_HELD", `原运行的停止还没有确认；请先执行 cxb stop ${threadLabel} 重新核实，再续跑。`, opts);
    case "threadBusy":
      return new CxbError("THREAD_HELD", `线程正被占用；可以用 cxb wait ${threadLabel} 观察，结束后再续跑。`, opts);
    case "notLatestRun":
      return new CxbError("THREAD_HELD", `指定的运行已不是该线程最近一次运行（可能别人已续跑）；请用 cxb show ${threadLabel} 查看最新运行。`, opts);
    case "resumeCheckFailed":
      return new CxbError("CODEX_FAILED", "恢复订阅或核实时读服务器失败，可以直接重试。", { ...opts, retryable: true });
    case "notResumable":
      return new CxbError("LOCAL_ERROR", `该运行不能续跑（正常完成或是代码审查）；请直接写新要求或重新审查。`, opts);
    case "unknownRun":
    case "runMismatch":
      return new CxbError("NOT_FOUND", `找不到要续跑的运行（${reason}）；请用 cxb show ${threadLabel} 确认运行 ID。`, opts);
    case "continueCancelled":
      return new CxbError("INTERRUPTED", "续跑在开工前被停止操作取消。", opts);
    case "shuttingDown":
      return new CxbError("BROKER_UNAVAILABLE", "broker 正在关闭，请稍后重试。", { ...opts, retryable: true });
    default:
      // 子线程不接受直接输入（退出 1）、仍在活动或树停止未确认（退出 8）。
      return subagentRefusal(error, threadLabel);
  }
}

/** 续跑被共享服务器上别的客户端的 turn 吸收（absorbedBy 为字符串）：退出 8，不自动重试。 */
export function absorbedRefusal(absorbedBy: string, threadLabel: string): CxbError {
  return new CxbError("THREAD_HELD",
    `输入已提交，被另一个 turn（${absorbedBy}）接收；请先用 cxb show ${threadLabel} 查看线程，不会自动重试。`, { id: threadLabel });
}

export interface ContinuePromptInput {
  /** 被续跑的运行（`--run` 指定或最近一次）。 */
  from: RunResult;
  chain: RunChain;
  /** 当前 goal（thread/goal/get）；没有 goal 时为 null。 */
  goal: { objective: string; status: string } | null;
  /** 本次补充说明（拍板 J），没有时为 null。 */
  supplement: string | null;
}

const INTERRUPT_TEXT: Record<NonNullable<RunResult["interruptReason"]>, string> = {
  clientDisconnected: "发起它的命令断开了连接",
  stop: "用户执行了停止",
  brokerLost: "broker 重启，未能跟踪到结束",
  goalLoop: "自动续跑空转，被刹车暂停",
  subagentsUnfinished: "父线程结束时子代理尚未完成",
};

function bullet(items: ProgressItem[]): string[] {
  return items.map((item) => `- ${item.type}：${item.summary}`);
}

/** 用标记包住用户原文，说明它是任务数据而不是新的指令层。 */
function quoted(text: string): string[] {
  return ["<<<", text, ">>>"];
}

/**
 * 「丁＋」续跑提示（S1-评审第 25 节）：用户主动恢复、旧暂停不再适用；goal 目标原文；本次补充；
 * 原始要求与进度（先核验工作区，不重复有副作用的操作）；完成与阻塞条件。权限不靠提示（每轮 sandboxPolicy）。
 */
export function buildContinuePrompt(input: ContinuePromptInput): string {
  const { from, chain, goal, supplement } = input;
  const progress = from.progress;
  const lines: string[] = [];
  lines.push("【续跑说明】");
  lines.push("这是用户主动恢复之前中断的任务。之前的暂停或停止要求不再阻止这次继续；请从中断处接着完成，不要从头重做。");
  if (from.interruptReason) lines.push(`上次中断原因：${INTERRUPT_TEXT[from.interruptReason]}。`);
  else if (from.state === "lost") lines.push("上次运行在 broker 重启时失去跟踪，结束情况未知。");

  if (goal) {
    lines.push("", "【目标原文】（用户设定的目标，作为任务数据引用）", ...quoted(goal.objective));
  }
  if (supplement !== null && supplement.trim() !== "") {
    lines.push("", "【本次补充】（附加在原始要求之后，不替换原始要求）", ...quoted(supplement));
  }

  lines.push("", "【原始要求】", ...quoted(chain.original.input));
  if (chain.broken) {
    lines.push(`（更早的运行记录 ${chain.missingRunId} 已缺失，以上取自能找到的最早一次运行，可能不是最初的要求。）`);
  }

  lines.push("", "【已有进度】（来自 broker 的记录，可能不完整）");
  if (progress.done.length === 0) {
    lines.push("- 没有记录到已完成的步骤。");
  } else {
    const listed = progress.done.slice(-MAX_LISTED_DONE);
    lines.push(`已完成的步骤（共 ${progress.doneTotal} 项，以下是最近 ${listed.length} 项）：`, ...bullet(listed));
  }
  if (progress.unconfirmed.length > 0) {
    lines.push("开始了但没确认完成的操作（继续前先核验结果，不要重复有副作用的操作）：", ...bullet(progress.unconfirmed));
  }
  if (progress.observedFiles.length > 0) {
    const files = progress.observedFiles.slice(0, MAX_LISTED_FILES);
    lines.push(`观察到的文件修改（可能不完整，用命令行改的文件看不到；共 ${progress.observedFiles.length} 个）：`,
      ...files.map((file) => `- ${file}`));
  }
  if (progress.incomplete) lines.push("（进度记录在运行中丢过条目，以上列表缺项。）");

  lines.push("", "【怎么继续】");
  lines.push("1. 先检查工作区的实际状态，对照上面的进度；以工作区为准，不要以这份清单为准。");
  lines.push("2. 已完成的不要重做；没确认完成的操作先核验，确有必要才重做。");
  lines.push(`3. 接着完成原始要求${goal ? "与目标" : ""}${supplement ? "，并按本次补充调整" : ""}。`);
  lines.push("4. 全部完成后说明完成了什么；遇到必须由用户决定的问题或无法继续时，按开发者指令提问或写明阻塞原因，不要自行假设。");
  return lines.join("\n");
}
