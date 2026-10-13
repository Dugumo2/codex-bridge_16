import { expect, test } from "bun:test";
import { createScriptedModel, EVIDENCE_HEAD, markerInEvidence, ProtocolBarrier } from "../test-support/sa1b-scripted-model";

/** 按真实模型请求形状构造汇总证据包消息。 */
const evidence = (text: string) => ({ type: "message", role: "user", content: [{ type: "input_text", text: `${EVIDENCE_HEAD}\n${text}` }] });
const activity = (output: string) => JSON.stringify({ key: { threadId: "child", turnId: "child-turn", activitySeq: 1 },
  locator: { threadId: "child", turnId: "child-turn", resultVersion: 1 }, output });

for (const version of ["v1", "v2"] as const) test(`脚本模型 ${version} 必须父final栅栏后返回随机子结果`, async () => {
  const m = createScriptedModel({ version });
  const initial = { input: [{ role: "user", content: "普通初始任务" }] };
  expect(JSON.stringify(initial)).not.toContain(m.marker);
  expect(await m.respond(initial)).toContain("spawn_agent");
  const parent = { input: [{ type: "function_call_output", call_id: "spawn-root", output: "子已派生" }] };
  expect(await m.respond(parent)).toContain("父线程阶段完成");
  let delivered = false;
  const child = m.respond({ input: [{ role: "user", content: "SA1B_CHILD_TASK" }] }).then(x => { delivered = true; return x; });
  await Promise.resolve(); expect(delivered).toBe(false); m.parentFinal.open(); expect(await child).toContain(m.marker);
  const summary = { input: [...parent.input, evidence(activity(m.marker))] };
  expect(await m.respond(summary)).toContain(m.marker);
  expect(m.requests.filter(x => x.role === "root")).toHaveLength(3);
});
test("汇总首采样缺随机标记时拒绝伪造答案", async () => {
  const m = createScriptedModel({ version: "v2" }); const p = { input: [{ type: "function_call_output", call_id: "spawn-root" }] };
  await m.respond({ input: [] }); await m.respond(p);
  await expect(m.respond(p)).rejects.toThrow("汇总首采样没有随机子结果标记");
});
test("只在通知里出现标记、证据包缺子结果时汇总断言必须失败", async () => {
  const m = createScriptedModel({ version: "v1" }); const p = { input: [{ type: "function_call_output", call_id: "spawn-root" }] };
  await m.respond({ input: [] }); await m.respond(p);
  const notice = { type: "message", role: "user", content: [{ type: "input_text", text: `<subagent_notification>${m.marker}</subagent_notification>` }] };
  const body = { input: [...p.input, notice, evidence('{"unavailableReason":"活动没有输出"}')] };
  expect(markerInEvidence(body, m.marker)).toBe(false);
  await expect(m.respond(body)).rejects.toThrow("汇总首采样没有随机子结果标记");
  expect(markerInEvidence({ input: [notice, evidence(activity(m.marker))] }, m.marker)).toBe(true);
});
for (const mode of ["历史证据包有标记、本次缺", "提示头后只有通知含标记", "提示头后只有缺失说明含标记"] as const) test(mode, async () => {
  const m = createScriptedModel({ version: "v2" });
  const p = { input: [{ type: "function_call_output", call_id: "spawn-root" }] };
  await m.respond({ input: [] }); await m.respond(p);
  const latest = mode === "提示头后只有通知含标记" ? `<subagent_notification>${m.marker}</subagent_notification>`
    : JSON.stringify({ ...JSON.parse(activity("")), unavailableReason: `缺失说明 ${mode.startsWith("历史") ? "" : m.marker}` });
  const body = { input: [...p.input, evidence(activity(m.marker)), evidence(latest)] };
  expect(markerInEvidence(body, m.marker)).toBe(false);
  await expect(m.respond(body)).rejects.toThrow("汇总首采样没有随机子结果标记");
});
test("证据活动定位必须一致，并可核对真实子轮", () => {
  const body = { input: [evidence(activity("随机标记"))] };
  expect(markerInEvidence(body, "随机标记", { threadId: "child", turnId: "child-turn" })).toBe(true);
  expect(markerInEvidence(body, "随机标记", { threadId: "child", turnId: "旧轮" })).toBe(false);
  const invalid = JSON.parse(activity("随机标记")); invalid.locator.turnId = "别轮";
  expect(markerInEvidence({ input: [evidence(JSON.stringify(invalid))] }, "随机标记")).toBe(false);
});
test("事件栅栏有超时而非永久等待", async () => {
  await expect(new ProtocolBarrier().wait(10)).rejects.toThrow("协议事件栅栏超时");
});
for (const version of ["v1", "v2"] as const) test(`脚本孙代理 ${version} 中间子代理也须等父final，避免通知抢先续采样`, async () => {
  const m = createScriptedModel({ version, grandchild: true });
  await m.respond({ input: [{ role: "user", content: "初始任务" }] });
  const parentBody = { input: [{ type: "function_call_output", call_id: "spawn-root" }] };
  const parent = m.respond(parentBody);
  expect(await m.respond({ input: [{ role: "user", content: "SA1B_CHILD_TASK" }] })).toContain("spawn_agent");
  let childFinished = false;
  const child = m.respond({ input: [{ type: "function_call_output", call_id: "spawn-child" }] }).then(x => { childFinished = true; return x; });
  const grand = m.respond({ input: [{ role: "user", content: "SA1B_GRAND_TASK" }] });
  m.descendantSpawned.open();
  expect(await parent).toContain("父线程阶段完成"); expect(childFinished).toBe(false);
  m.parentFinal.open(); await child; expect(await grand).toContain(m.marker);
  expect(await m.respond({ input: [...parentBody.input, evidence(activity(m.marker))] })).toContain(m.marker);
});
