/**
 * 真实 0.160.0 app-server/core + 真 broker + 本地脚本模型。
 * 用法：bun src/test-support/sa1b-real-protocol.ts --app-server <本机构建产物> --report <JSON路径>
 * 缺产物/版本不符/协议失败均写「未覆盖」且退出2。仅模拟模型决定，不伪造线程或完成事件。
 * 延迟 core 邮箱投递需要上游测试钩子；外部协议无法设置，该项单独记未覆盖。
 */
import { existsSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { TestClient, endpointFor, testSocketPath, writeMockCodexScript, spawnBroker, waitForSocket, removeDirWithRetry } from "./broker-harness";
import { createScriptedModel, markerInEvidence, type ScriptedOptions } from "./sa1b-scripted-model";
import { ASK_INSTRUCTIONS, ASK_RULES, SUBAGENT_RULES } from "../cxb/ask-instructions";
import type { Subprocess } from "bun";

const args = process.argv.slice(2);
const arg = (name: string) => args[args.indexOf(name) + 1];
const binary = args.includes("--app-server") ? arg("--app-server") : "codex-app-server.exe";
const codexCli = args.includes("--codex-cli");
const reportFile = args.includes("--report") ? arg("--report") : (process.env.TEMP! + "\\sa1b-e-real-protocol.json");
const results: any[] = [];

async function run(name: string, options: ScriptedOptions, resume?: "cold" | "loaded") {
  const tempDir = mkdtempSync(join(tmpdir(), "sa1b-real-"));
  const codeHome = join(tempDir, "codex-home"), brokerHome = join(tempDir, "cxb-home");
  mkdirSync(codeHome); mkdirSync(brokerHome);
  const spawnedProcesses: Subprocess[] = [];
  const script = createScriptedModel(options);
  const model = options.version === "v2" ? "gpt-6.1-sol" : "gpt-5.6-luna";
  const upstreamTrace = `${reportFile}.${name}.upstream.jsonl`;
  writeFileSync(upstreamTrace, "");
  const events: any[] = [], modelErrors: string[] = [];
  let root = "", initialTurn = "", rootFinalSeen = false;
  let orderingProved = false;
  const readUpstream = (): any[] => readFileSync(upstreamTrace, "utf8").split(/\r?\n/).filter(Boolean).flatMap(line => {
    try { return [JSON.parse(line)]; } catch { return []; }
  });
  // 从真实传输记录等待孙派生成功；不以模型返回tool call当作执行成功。
  const monitor = setInterval(() => {
    if (!options.grandchild || !root) return;
    const spawned = readUpstream().some(x => x.method === "item/completed" && x.params.threadId !== root &&
      ((x.params.item?.type === "collabAgentToolCall" && x.params.item.tool === "spawnAgent" && x.params.item.status === "completed") ||
       (x.params.item?.type === "subAgentActivity" && x.params.item.kind === "started")));
    if (spawned) script.descendantSpawned.open();
  }, 20);
  const server = Bun.serve({ hostname: "127.0.0.1", port: 0, idleTimeout: 30,
    async fetch(request) {
      if (!new URL(request.url).pathname.endsWith("/responses")) return new Response("未实现端点", { status: 404 });
      try { return new Response(await script.respond(await request.json()), { headers: { "Content-Type": "text/event-stream" } }); }
      catch (error) { modelErrors.push(String(error)); return new Response(String(error), { status: 500 }); }
    },
  });
  let client: TestClient | undefined;
  const drains: Promise<string>[] = [];
  const deadline = Date.now() + 40000;
  const until = async (condition: () => boolean | Promise<boolean>) => {
    while (!await condition()) { if (Date.now() >= deadline) throw new Error("真实协议场景40秒截止"); if (modelErrors.length) throw new Error(modelErrors.join("；")); await Bun.sleep(20); }
  };
  try {
    // 使用指定源码目录中声明支持v2的模型元数据；所有模型响应仍来自本地脚本。
    const overrides = [`model=${JSON.stringify(model)}`, "model_provider=\"sa1b_script\"", "approval_policy=\"never\"", "sandbox_mode=\"danger-full-access\"",
      "features.multi_agent=true", `features.multi_agent_v2=${options.version === "v2"}`, "features.default_mode_request_user_input=true",
      "agents.max_depth=3", "agents.max_threads=6",
      "model_providers.sa1b_script.name=\"SA1b本地脚本模型\"", `model_providers.sa1b_script.base_url=\"http://127.0.0.1:${server.port}/v1\"`,
      "model_providers.sa1b_script.wire_api=\"responses\"", "model_providers.sa1b_script.requires_openai_auth=false",
      "model_providers.sa1b_script.request_max_retries=0", "model_providers.sa1b_script.stream_max_retries=0"];
    const command = [binary, ...(codexCli ? ["app-server"] : []), ...overrides.flatMap(x => ["-c", x])];
    // 旧broker会在父final后立即interrupt子线程。传输栅栏只扣这些真实请求，直到对应真实
    // turn/completed（最多5秒），确保本场景能观察随机子结果；不伪造任何回包或core事件。
    // 这不是core完成消息投递延迟测试，后者仍单列未覆盖。
    writeMockCodexScript(tempDir, `import { appendFileSync } from "node:fs";
const child = Bun.spawn(${JSON.stringify(command)}, { stdin: "pipe", stdout: "pipe", stderr: "inherit", env: process.env });
const completed = new Set(), holds = new Map(); let input = "", observed = "";
function release(thread) { const pending = holds.get(thread); if (!pending) return; holds.delete(thread); for (const entry of pending) { clearTimeout(entry.timer); child.stdin.write(entry.line + "\\n"); } }
process.stdin.setEncoding("utf8");
process.stdin.on("data", chunk => { input += chunk; let index; while ((index = input.indexOf("\\n")) >= 0) {
 const line = input.slice(0,index); input = input.slice(index+1); if (!line.trim()) continue; const msg = JSON.parse(line), thread = msg.params?.threadId;
 if (msg.method === "turn/interrupt" && !completed.has(thread)) { const queue = holds.get(thread) ?? []; queue.push({line, timer:setTimeout(() => release(thread),5000)}); holds.set(thread,queue); }
 else child.stdin.write(line + "\\n");
} });
process.stdin.on("end", () => { for (const thread of holds.keys()) release(thread); child.stdin.end(); });
process.on("exit", () => child.kill());
const decoder = new TextDecoder();
const drain = (async () => { for await (const chunk of child.stdout) {
 appendFileSync(${JSON.stringify(upstreamTrace)}, chunk); process.stdout.write(chunk); observed += decoder.decode(chunk,{stream:true}); let index;
 while ((index = observed.indexOf("\\n")) >= 0) { const line = observed.slice(0,index); observed = observed.slice(index+1); if (!line.trim()) continue; const msg = JSON.parse(line);
 if (msg.method === "turn/started") completed.delete(msg.params.threadId);
 if (msg.method === "turn/completed") { completed.add(msg.params.threadId); release(msg.params.threadId); }
 }
} })();
const code = await child.exited; await drain; process.exit(code);`);
    const socket = testSocketPath(tempDir);
    const onNotification = (message: Record<string, unknown>) => {
      events.push(structuredClone(message));
      const p: any = message.params;
      if (message.method === "turn/started" && p.threadId === root && !initialTurn) initialTurn = p.turn.id;
      if (message.method === "turn/completed" && p.threadId === root && p.turn.id === initialTurn) {
        rootFinalSeen = true; script.parentFinal.open();
      }
    };
    const launch = async () => {
      const proc = spawnBroker({ tempDir, spawnedProcesses }, endpointFor(socket), tempDir, { env: { CODEX_HOME: codeHome, CXB_HOME: brokerHome } });
      drains.push(new Response(proc.stdout as ReadableStream).text(), new Response(proc.stderr as ReadableStream).text());
      await waitForSocket(socket); client = await TestClient.connectAndInit(socket, { cxb: true }); client.onNotification(onNotification);
    };
    await launch();
    const created: any = await client!.request("thread/start", { cwd: tempDir, model, developerInstructions: ASK_INSTRUCTIONS, approvalPolicy: "never", sandbox: "danger-full-access", experimentalRawEvents: false, persistExtendedHistory: true });
    root = created.thread.id;
    if (resume) {
      // 0.160.0 只有发生首轮后才有可恢复 rollout；先真实执行一轮建立历史。
      const seeded: any = await client!.request("turn/start", { threadId: root, input: [{ type: "text", text: "建立恢复测试历史" }] });
      await until(() => events.some(x => x.method === "broker/run/ended" && x.params.run?.runId === seeded.runId));
    }
    if (resume === "cold") {
      // unsubscribe不保证卸载session；重启隔离app-server后核对loaded列表，才算冷恢复。
      await client!.close();
      for (const proc of spawnedProcesses) { proc.kill(); await proc.exited; }
      await launch();
      await until(async () => !((await client!.request("thread/loaded/list", {}) as any).data ?? []).includes(root));
      await client!.request("thread/resume", { threadId: root, developerInstructions: ASK_INSTRUCTIONS });
    } else if (resume === "loaded") {
      if (!((await client!.request("thread/loaded/list", {}) as any).data ?? []).includes(root)) throw new Error("已加载恢复的前置状态未成立");
      await client!.request("thread/resume", { threadId: root, developerInstructions: ASK_INSTRUCTIONS });
    }
    const started: any = await client!.request("turn/start", { threadId: root, input: [{ type: "text", text: "执行本地确定性测试任务" }], cxbConvergence: { timeoutMs: 25000 } });
    await until(() => events.some(x => x.method === "broker/run/ended" && x.params.run?.runId === started.runId));
    const ended = events.find(x => x.method === "broker/run/ended" && x.params.run?.runId === started.runId).params.run;
    const inspected = resume ? script.requests.at(-1)?.body : script.requests[0]?.body;
    const initialBody = JSON.stringify(inspected);
    const instructionText = [inspected?.instructions ?? "", ...(inspected?.input ?? [])
      .filter((item: any) => item.role === "developer" || item.role === "system")
      .flatMap((item: any) => typeof item.content === "string" ? [item.content] : (item.content ?? []).map((part: any) => part.text ?? ""))].join("\n");
    if (!instructionText.includes(ASK_RULES) || !instructionText.includes(SUBAGENT_RULES)) throw new Error("甲指令没有出现在真实模型请求中");
    if (options.contextOnly) {
      if (!script.requests.length) throw new Error("没有实际采样，不能以RPC参数代替上下文验证");
    } else {
      // owner默认只订阅父线程；从透明传输记录核实子事件，不能把owner未收到当作core没产生。
      const upstreamEvents = readUpstream();
      const children = upstreamEvents.filter(x => x.method === "turn/completed" && x.params.threadId !== root);
      if (!rootFinalSeen || !children.length) throw new Error("未触发父final→子完成场景");
      const parentIndex = upstreamEvents.findIndex(x => x.method === "turn/completed" && x.params.threadId === root && x.params.turn.id === initialTurn);
      const markerChild = upstreamEvents.findIndex(x => ["item/completed", "turn/completed"].includes(x.method) && x.params.threadId !== root && JSON.stringify(x).includes(script.marker));
      if (markerChild <= parentIndex) throw new Error("子随机结果没有发生在父final之后");
      orderingProved = true;
      if (children.some(x => x.params.turn.status !== "completed")) throw new Error("父final后已有随机子结果，但broker停树中断子活动，未进入汇总");
      const rootBodies = script.requests.filter(x => x.role === "root");
      const markerEvent = upstreamEvents[markerChild].params;
      const markerActivity = { threadId: markerEvent.threadId, turnId: markerEvent.turnId ?? markerEvent.turn?.id };
      if (!markerActivity.turnId || rootBodies.length !== 3 || !markerInEvidence(rootBodies[2]!.body, script.marker, markerActivity)) throw new Error("汇总第一次采样的本次活动 output 内没有随机标记或活动定位不符");
      if (initialBody.includes(script.marker)) throw new Error("初始提示泄露随机标记");
      const final = ended.turns.find((x: any) => x.turnId === ended.finalTurnId);
      if (!final?.output?.includes(script.marker) || ended.status !== "completed") throw new Error("最终父轮未交付随机标记");
      if (options.grandchild && new Set(children.map(x => x.params.threadId)).size < 2) throw new Error("孙代理场景未触发");
    }
    results.push({ name, status: "通过", requests: script.requests.length, events: events.length });
  } catch (error) {
    results.push({ name, status: orderingProved ? "失败" : "未覆盖", orderingProved, blocker: String(error), modelErrors, requests: script.requests.length, events: events.length });
  } finally {
    clearInterval(monitor); script.parentFinal.open(); script.descendantSpawned.open(); script.leafSampling.open(); await client?.close();
    for (const proc of spawnedProcesses) { try { proc.kill(); } catch {} }
    await Promise.race([Promise.all(spawnedProcesses.map(x => x.exited)), Bun.sleep(5000)]);
    await server.stop(true);
    await Promise.race([Promise.all(drains), Bun.sleep(1000)]);
    // 只输出测试生成的协议记录，不读取或复制开发者凭据。
    writeFileSync(`${reportFile}.${name}.trace.json`, JSON.stringify({ events, modelRequests: script.requests }, null, 2));
    await removeDirWithRetry(tempDir);
  }
}

const cases: Array<[string, ScriptedOptions, ("cold" | "loaded")?]> = [
  ["v1", { version: "v1" }], ["v2", { version: "v2" }],
  ["v1-grandchild", { version: "v1", grandchild: true }], ["v2-grandchild", { version: "v2", grandchild: true }],
  ["E25-cold", { version: "v1", contextOnly: true }, "cold"], ["E25-loaded", { version: "v2", contextOnly: true }, "loaded"],
];
if (!existsSync(binary)) {
  for (const [name] of cases) results.push({ name, status: "未覆盖", blocker: `指定0.160.0 app-server产物不存在：${binary}` });
} else {
  const proc = Bun.spawn([binary, "--version"], { stdout: "pipe", stderr: "pipe" });
  const timer = setTimeout(() => proc.kill(), 5000);
  const version = await new Response(proc.stdout).text(); await proc.exited; clearTimeout(timer);
  if (!version.includes("0.160.0")) for (const [name] of cases) results.push({ name, status: "未覆盖", blocker: `版本核实失败：${version.trim()}` });
  else for (const [name, options, resume] of cases) await run(name, options, resume);
}
results.push({ name: "delayed-core-delivery", status: "未覆盖", blocker: "公开JSON-RPC不暴露core邮箱投递栅栏。须在指定源码 core/src/agent/control/completion.rs::notify_parent_of_terminal_turn 的 send_inter_agent_communication 前增加仅测试钩子；外部扣通知不能替代此验证。" });
writeFileSync(reportFile, JSON.stringify({ date: new Date().toISOString(), binary,
  transportFence: "turn/interrupt扣到对应真实turn/completed后放行，最多5秒；不等同于core完成邮箱延迟", results }, null, 2));
console.log(JSON.stringify(results, null, 2));
process.exitCode = results.every(x => x.status === "通过") ? 0 : 2;
