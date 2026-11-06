/** 隔离副本变异：不修改产品或并行测试正在读取的主文件。恢复副本后必须再次通过。 */
import { copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join, resolve, sep } from "node:path";
import { tmpdir } from "node:os";

const base = mkdtempSync(join(tmpdir(), "sa1b-mutation-"));
const support = join(base, "src", "test-support"), tests = join(base, "src", "cxb");
mkdirSync(support, { recursive: true }); mkdirSync(tests, { recursive: true });
for (const file of ["sa1b-mock.ts", "sa1b-scripted-model.ts", "background-terminals-mock.ts"]) copyFileSync(join(import.meta.dir, file), join(support, file));
for (const file of ["sa1b-fixture.test.ts", "sa1b-scripted-model.test.ts"]) copyFileSync(join(import.meta.dir, "../cxb", file), join(tests, file));
const output = process.argv[2] ?? join(tmpdir(), "sa1b-e-mutation.json");
const mutations = [
  { name: "历史证据包补齐本次缺项", file: "sa1b-scripted-model.ts", from: "findLast(text => text.includes(EVIDENCE_HEAD))", to: "find(text => text.includes(EVIDENCE_HEAD))", test: "历史证据包有标记、本次缺" },
  ...["提示头后只有通知含标记", "提示头后只有缺失说明含标记"].map(test => ({ name: test, file: "sa1b-scripted-model.ts", from: "if (!current) return false;", to: "if (!current) return false; return current.includes(marker);", test })),
  { name: "clientId误读条目id", file: "sa1b-mock.ts", from: "clientId: p.clientUserMessageId ?? null", to: "clientId: null", test: "真实保存 clientId" },
  { name: "忽略expectedTurnId", file: "sa1b-mock.ts", from: "(!turn || turn.id !== p.expectedTurnId)", to: "(!turn)", test: "steer 必须匹配" },
  { name: "丢失投递变正常投递", file: "sa1b-mock.ts", from: "!t.parentThreadId || t.delivery === \"dropped\"", to: "!t.parentThreadId", test: "完成消息 dropped" },
  { name: "pendingInit伪造首轮", file: "sa1b-mock.ts", from: "if (child.pendingInit) {", to: "if (false) {", test: "pendingInit 与首轮缺失" },
  { name: "撤掉通知栅栏", file: "sa1b-mock.ts", from: "if (fence) { held.set", to: "if (false) { held.set", test: "栅栏先扣住后放行" },
  { name: "撤掉请求栅栏", file: "sa1b-mock.ts", from: "if (requestFence) { requestHeld.set", to: "if (false) { requestHeld.set", test: "请求栅栏在执行前" },
  { name: "伪造缺标记汇总成功", file: "sa1b-scripted-model.ts", from: "if (!markerInEvidence(body, marker)) throw", to: "if (false) throw", test: "汇总首采样缺随机标记" },
  { name: "通知补齐证据包缺失", file: "sa1b-scripted-model.ts", from: "if (!markerInEvidence(body, marker)) throw", to: "if (!JSON.stringify(body).includes(marker)) throw", test: "只在通知里出现标记" },
  { name: "撤掉父final栅栏", file: "sa1b-scripted-model.ts", from: "leafSampling.open();\n    await parentFinal.wait();", to: "leafSampling.open();", test: "必须父final栅栏后" },
  { name: "中间子代理抢先完成", file: "sa1b-scripted-model.ts", from: "await parentFinal.wait();\n      return sse(answer(\"子线程", to: "return sse(answer(\"子线程", test: "中间子代理也须等父final" },
];
async function run(pattern?: string) {
  const proc = Bun.spawn([process.execPath, "test", tests, ...(pattern ? ["-t", pattern] : [])], { stdout: "pipe", stderr: "pipe", cwd: base, windowsHide: true });
  const timer = setTimeout(() => proc.kill(), 15000);
  try {
    const [stdout, stderr, code] = await Promise.all([new Response(proc.stdout).text(), new Response(proc.stderr).text(), proc.exited]);
    return { code, log: stdout + stderr };
  } finally { clearTimeout(timer); }
}
const results: any[] = [];
try {
  const baseline = await run(); if (baseline.code !== 0) throw new Error(`变异前夹具失败：${baseline.log}`);
  for (const mutation of mutations) {
    const path = join(support, mutation.file), original = readFileSync(path, "utf8");
    if (original.split(mutation.from).length !== 2) throw new Error(`变异定位不唯一：${mutation.name}`);
    let changed: Awaited<ReturnType<typeof run>>;
    try { writeFileSync(path, original.replace(mutation.from, mutation.to)); changed = await run(mutation.test); }
    finally { writeFileSync(path, original); }
    const restored = await run(mutation.test);
    results.push({ name: mutation.name, test: mutation.test, mutatedExit: changed.code, restoredExit: restored.code,
      detected: changed.code !== 0 && changed.log.includes("(fail)") && restored.code === 0,
      failureNames: changed.log.split(/\r?\n/).filter(x => x.includes("(fail)")) });
  }
  writeFileSync(output, JSON.stringify({ baseline: baseline.code, results }, null, 2));
  console.log(JSON.stringify(results, null, 2));
  process.exitCode = results.every(x => x.detected) ? 0 : 1;
} finally {
  // 删除前核实绝对路径限定在本次临时工作区。
  const target = resolve(base), parent = resolve(tmpdir());
  if (!target.startsWith(parent + sep) || !target.includes("sa1b-mutation-")) throw new Error("拒绝删除越界路径");
  rmSync(target, { recursive: true, force: true });
}
