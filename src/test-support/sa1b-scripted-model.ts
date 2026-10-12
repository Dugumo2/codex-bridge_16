/** 本地 Responses SSE 脚本模型；真实 core 决定工具执行、线程事件和邮箱取用。 */
import { randomUUID } from "node:crypto";

export class ProtocolBarrier {
  private resolve!: () => void;
  private readonly promise = new Promise<void>(resolve => { this.resolve = resolve; });
  opened = false;
  open() { this.opened = true; this.resolve(); }
  async wait(timeoutMs = 15000) {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try { await Promise.race([this.promise, new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error("协议事件栅栏超时")), timeoutMs); })]); }
    finally { clearTimeout(timer); }
  }
}
export interface ScriptedOptions { version: "v1" | "v2"; grandchild?: boolean; contextOnly?: boolean }

/** broker 汇总证据包的固定开头（broker-rs/src/claim/subagent_reconcile/evidence.rs 的 PROMPT）。 */
export const EVIDENCE_HEAD = "请继续完成原任务，核实下列子代理结果并汇总最终答复。";

/** 模型请求里的用户消息文本（字符串或 input_text 分段）。 */
function userTexts(body: any): string[] {
  return (body?.input ?? []).filter((item: any) => item?.role === "user").flatMap((item: any) =>
    typeof item.content === "string" ? [item.content] : (item.content ?? []).map((part: any) => part?.text ?? ""));
}

/** 标记必须出现在本次汇总证据包内；子代理完成通知等其他位置的同一标记不算证据包送达。 */
export function markerInEvidence(body: any, marker: string): boolean {
  return userTexts(body).some((text) => text.includes(EVIDENCE_HEAD) && text.slice(text.indexOf(EVIDENCE_HEAD)).includes(marker));
}

export function createScriptedModel(options: ScriptedOptions) {
  const marker = `SA1B_${randomUUID().replaceAll("-", "")}`;
  const parentFinal = new ProtocolBarrier();
  const descendantSpawned = new ProtocolBarrier();
  const leafSampling = new ProtocolBarrier();
  const requests: Array<{ role: string; body: any }> = [];
  let rootSamples = 0, childSamples = 0;
  const answer = (value: string) => ({ type: "message", role: "assistant", id: randomUUID(), phase: "final_answer", content: [{ type: "output_text", text: value }] });
  const spawn = (callId: string, task: string) => ({ type: "function_call", call_id: callId,
    namespace: options.version === "v2" ? "collaboration" : "multi_agent_v1", name: "spawn_agent",
    arguments: JSON.stringify(options.version === "v2" ? { task_name: task.toLowerCase(), message: task, fork_turns: "none" } : { message: task }) });
  const sse = (item: any) => {
    const id = randomUUID();
    return [ { type: "response.created", response: { id } }, { type: "response.output_item.done", item },
      { type: "response.completed", response: { id, usage: { input_tokens: 10, input_tokens_details: { cached_tokens: 0 }, output_tokens: 5, output_tokens_details: null, total_tokens: 15 } } },
    ].map(event => `event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`).join("");
  };
  async function respond(body: any): Promise<string> {
    const input = body.input ?? [];
    const hasOutput = (id: string) => input.some((x: any) => x.type === "function_call_output" && x.call_id === id);
    const bodyText = JSON.stringify(input);
    const role = options.contextOnly ? "context" : hasOutput("spawn-root") ? "root" : hasOutput("spawn-child") ? "child"
      : bodyText.includes("SA1B_GRAND_TASK") ? "grandchild" : bodyText.includes("SA1B_CHILD_TASK") ? "child" : "root";
    requests.push({ role, body: structuredClone(body) });
    if (role === "context") return sse(answer("上下文已核对"));
    if (role === "root") {
      rootSamples++;
      if (rootSamples === 1) {
        if (JSON.stringify(body).includes(marker)) throw new Error("随机标记意外出现在初始模型请求");
        return sse(spawn("spawn-root", "SA1B_CHILD_TASK"));
      }
      if (rootSamples === 2) {
        if (options.grandchild) { await descendantSpawned.wait(); await leafSampling.wait(); }
        return sse(answer("父线程阶段完成"));
      }
      // 汇总第一次采样直接 final；只有实际模型输入含标记才返回标记。
      if (!markerInEvidence(body, marker)) throw new Error("汇总首采样没有随机子结果标记（须位于汇总证据包内）");
      return sse(answer(`已核实 ${marker}`));
    }
    if (role === "child" && options.grandchild) {
      childSamples++;
      if (childSamples === 1) return sse(spawn("spawn-child", "SA1B_GRAND_TASK"));
      // v1 的子完成通知可能让父模型继续采样；父final前连中间子代理也不能先完成。
      await parentFinal.wait();
      return sse(answer("子线程阶段完成，孙任务稍后完成"));
    }
    leafSampling.open();
    await parentFinal.wait();
    return sse(answer(marker));
  }
  return { marker, parentFinal, descendantSpawned, leafSampling, requests, respond };
}
