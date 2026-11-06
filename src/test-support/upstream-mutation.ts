/** 可回滚的本路关键变异检查；每次失败后还原并重跑对应真实／安全用例。 */
import { tmpdir } from "node:os";
import assert from "node:assert/strict";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { TRACE_ROOT, isolatedEnvironment, startManaged, stopManaged, within } from "./upstream-isolation";

if (import.meta.main) {
  const worktree = join(import.meta.dir, "../..");
  const env = isolatedEnvironment(join(tmpdir(), "ue-mutation-only"));
  env.CXB_UPSTREAM_REAL = "1";
  const cases = [
    { name: "凭证白名单误放行", file: "upstream-isolation.ts", before: 'new Set(["PATH",', after: 'new Set(["OPENAI_API_KEY", "PATH",',
      filter: "环境采用白名单" },
    { name: "WebSocket 安全核对遗漏", file: "upstream-isolation.ts", before: "provider?.supports_websockets !== false", after: "false",
      filter: "有效配置被更改" },
    { name: "本机代理占位遗漏", file: "upstream-isolation.ts", before: 'Object.assign(env, { HTTP_PROXY: new URL(baseUrl).origin, HTTPS_PROXY: new URL(baseUrl).origin, ALL_PROXY: new URL(baseUrl).origin });',
      after: "// 变异：遗漏本机代理占位", filter: "系统代理占位仅" },
    { name: "401 场景误返回 402", file: "upstream-model-server.ts", before: ": Number(selected);", after: ': (selected === "401" ? 402 : Number(selected));',
      filter: "401：五次重试" },
  ];
  const results = [];
  for (const entry of cases) {
    const file = join(import.meta.dir, entry.file), original = readFileSync(file, "utf8");
    assert.equal(original.split(entry.before).length, 2, "变异目标必须唯一");
    const run = async () => {
      const proc = startManaged([process.execPath, "test", "src/cxb/upstream-real.test.ts", "--timeout", "45000", "-t", entry.filter], { cwd: worktree, env });
      try { return { code: await within(proc.closed, 60_000, entry.name), ...proc.output() }; }
      finally { await stopManaged(proc); }
    };
    let broken: Awaited<ReturnType<typeof run>>;
    try { writeFileSync(file, original.replace(entry.before, entry.after)); broken = await run(); }
    finally { writeFileSync(file, original); }
    assert.notEqual(broken!.code, 0, `${entry.name} 未被用例发现`);
    const restored = await run(); assert.equal(restored.code, 0, `${entry.name} 还原后未通过`);
    results.push({ name: entry.name, filter: entry.filter, mutationExitCode: broken!.code, restoredExitCode: restored.code,
      failure: broken!.stderr.split(/\r?\n/).filter((line) => line.includes("(fail)") || line.includes("error:")).join("\n") });
  }
  mkdirSync(TRACE_ROOT, { recursive: true });
  writeFileSync(join(TRACE_ROOT, "mutation.json"), JSON.stringify({ at: new Date().toISOString(), results }, null, 2));
  console.log(JSON.stringify(results, null, 2));
}
