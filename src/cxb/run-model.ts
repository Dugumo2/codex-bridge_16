// 普通 run 的模型/强度解析；所有值表示开轮请求值，不声称是服务器最终推理参数。
import { resolveModel, validateEffort } from "../config";
import { fetchModels, pickAutoEffort, pickBestModel } from "../models";
import type { Model } from "../types";
import { CxbError, rpcFailure, type BrokerRpc, type RunResult } from "./contracts";
import { latestModelRun, readAllRuns } from "./ledger";
import type { Output } from "./output";

export interface ThreadModel { model?: string | null; reasoningEffort?: string | null }
export interface InheritedModel { model: string; effort?: string | null; source: "沿用线程" | "沿用上游记录" }
export interface ModelSelection { model?: string; reasoning?: string; modelSource: string; reasoningSource: string }

/** 纯决策表；目录未知时不以省略 effort 的方式把旧模型强度带给新模型。 */
export function selectRunModel(options: {
  model?: string; reasoning?: string; explicit: ReadonlySet<string>; resume: boolean;
  inherited?: InheritedModel; models?: Model[]; out: Pick<Output, "notice">;
}): ModelSelection {
  const { explicit, inherited, models, out, resume } = options;
  const configuredModel = resolveModel(options.model);
  let configuredEffort: string | undefined;
  try { configuredEffort = validateEffort(options.reasoning); } catch (cause) {
    throw new CxbError("USAGE", "推理强度无效", { cause });
  }
  const model = (explicit.has("model") ? configuredModel : undefined) ?? inherited?.model ?? configuredModel ?? pickBestModel(models ?? []);
  const modelSource = explicit.has("model") ? "-m 指定" : inherited ? inherited.source : configuredModel ? "config 默认" : "自动选择";
  const entry = models?.find((item) => item.id === model);
  const supported = entry?.supportedReasoningEfforts;
  const known = !!supported?.length;
  const includes = (effort: string) => supported?.some((item) => item.reasoningEffort === effort);
  if (model && !entry) out.notice(`无法从模型目录确认 ${model}，按原样提交`);
  let reasoning: string | undefined;
  let reasoningSource = "自动选择";
  if (explicit.has("reasoning")) {
    reasoning = configuredEffort;
    reasoningSource = "-r 指定";
    if (reasoning && known && !includes(reasoning)) out.notice(`${model} 目录未列出强度 ${reasoning}，按原样提交`);
  } else {
    if (model === inherited?.model && inherited?.effort) {
      if (!known || includes(inherited.effort)) {
        reasoning = inherited.effort;
        reasoningSource = inherited.source;
        if (!known) out.notice(`无法从目录核实强度，沿用 ${reasoning}`);
      } else out.notice(`${model} 目录未列出原强度 ${inherited.effort}，不沿用，改按 config 或自动选择`);
    }
    if (!reasoning && configuredEffort) {
      if (!known || includes(configuredEffort)) {
        reasoning = configuredEffort;
        reasoningSource = "config 默认";
        if (!known) out.notice(`无法从目录核实强度，使用 config 默认 ${reasoning}`);
      } else out.notice(`${model} 目录未列出 config 强度 ${configuredEffort}，改为自动选择`);
    }
    if (!reasoning && known) {
      reasoning = pickAutoEffort(supported!);
      // 新目录可能只列出高于自动上限或尚不认识的值，不能误报为已经自动选定。
      if (!reasoning) out.notice(`${model} 目录中没有可自动选择的推理强度（最高 xhigh）`);
    }
    if (!reasoning) {
      if (!resume) {
        reasoningSource = "服务器默认";
        out.notice("推理强度由服务器默认决定");
      } else if (model && model === inherited?.model) {
        reasoningSource = "上游当前设置";
        out.notice("推理强度沿用上游当前设置（未核实）");
      } else throw new CxbError("LOCAL_ERROR", `无法确定 ${model ?? "所选模型"} 支持的推理强度，请用 -r 指定`);
    }
  }
  return { model, reasoning, modelSource, reasoningSource };
}

export async function resolveRunModel(options: {
  request: BrokerRpc["request"]; workspaceDir: string; threadId?: string; shared: boolean;
  model?: string; reasoning?: string; explicit: ReadonlySet<string>; out: Pick<Output, "notice" | "progress">;
  readThread: () => Promise<ThreadModel | undefined>;
  rethrow: (error: unknown) => void;
}): Promise<ModelSelection> {
  const { request, threadId, out } = options;
  let inherited: InheritedModel | undefined;
  if (threadId) {
    const disk = readAllRuns(options.workspaceDir);
    let online: RunResult[] = [];
    try {
      const snapshot = await request<{ run?: RunResult | null; runs?: RunResult[] | null }>("broker/threads/watch", { threadId, includeRuns: true });
      online = snapshot.runs ?? (snapshot.run ? [snapshot.run] : []);
    } catch (error) {
      options.rethrow(error);
      // 索引缺失或仅在上游存在的线程还不能 watch，仍可读磁盘与 thread/read。
      if (rpcFailure(error).reason !== "unknownThread") throw error;
    }
    const run = latestModelRun(threadId, disk, online);
    if (run) inherited = { model: run.meta!.model!, effort: run.meta!.effort, source: "沿用线程" };
    if (!inherited) {
      let upstream: ThreadModel | undefined;
      try { upstream = await options.readThread(); } catch (error) { options.rethrow(error); }
      if (upstream?.model) {
        inherited = { model: upstream.model, effort: upstream.reasoningEffort, source: "沿用上游记录" };
        out.notice("没有合格的 cxb 模型记录，沿用上游记录");
      } else out.notice("无法确定线程原模型，退回显式参数、config 默认或自动选择");
    }
  }
  let models: Model[] | undefined;
  try { models = await fetchModels({ request }); } catch (error) {
    options.rethrow(error);
    out.notice("无法读取模型目录，无法核实模型与推理强度");
  }
  const selection = selectRunModel({ ...options, resume: !!threadId, inherited, models });
  // 只有本次确实沿用 cxb 值才核对；显式换模型不能提示成「沿用」旧模型。
  if (options.shared && inherited?.source === "沿用线程"
    && (selection.modelSource === "沿用线程" || selection.reasoningSource === "沿用线程")) {
    let upstream: ThreadModel | undefined;
    try { upstream = await options.readThread(); } catch (error) { options.rethrow(error); }
    if (!upstream?.model) out.notice("未能与上游核对线程当前模型与强度");
    else if (inherited.model !== upstream.model || (inherited.effort ?? null) !== (upstream.reasoningEffort ?? null)) {
      out.notice(`沿用 cxb 最近记录 ${inherited.model}/${inherited.effort ?? "未登记"}，上游当前配置为 ${upstream.model}/${upstream.reasoningEffort ?? "未登记"}`);
    }
  }
  out.progress(`模型：${selection.model ?? "服务器自动选择"}（${selection.modelSource}）；推理强度：${selection.reasoning ?? "未发送"}（${selection.reasoningSource}）`);
  return selection;
}
