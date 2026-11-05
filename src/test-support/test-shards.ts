/** 按文件分片运行 CLI 测试；同一文件内仍由 Bun 顺序执行。 */
import { spawn, type ChildProcess } from "node:child_process";
import { closeSync, existsSync, mkdirSync, mkdtempSync, openSync, readdirSync, readFileSync, realpathSync, renameSync, statSync, writeFileSync } from "node:fs";
import { freemem, tmpdir } from "node:os";
import { isAbsolute, join, relative, resolve, sep } from "node:path";
import { DescendantWatcher } from "./process-tree";

const REPO = resolve(import.meta.dir, "../..");
const TIMINGS = join(import.meta.dir, "test-shards.timings.json");
// 与 Bun 1.3.12 实测一致：四种后缀、八种扩展名，不分大小写；`.test.d.ts` 不算。
const TEST_FILE = /(?:\.test|_test|\.spec|_spec)\.(?:ts|tsx|js|jsx|mts|cts|mjs|cjs)$/i;
const SAMPLE_INTERVAL_MS = 1000;
const keyOf = (file: string) => relative(REPO, resolve(file)).split(sep).join("/");
type Timings = Record<string, number>;
interface Group { files: string[]; seconds: number }
interface Counts { passed: number; failed: number; skipped: number }
interface Report extends Counts { failures: string[]; files: string[] }
interface Options { target: string; shards: number; logDir?: string; deadlineMs: number; minMemoryMb: number; leftoverGraceMs: number; timingsFile: string; updateTimings: boolean; help: boolean }

export function parseArgs(args: string[]): Options {
  const options: Options = { target: join(REPO, "src/cxb"), shards: 3, deadlineMs: 30 * 60_000, minMemoryMb: 1500, leftoverGraceMs: 15_000, timingsFile: TIMINGS, updateTimings: false, help: false };
  let targetSeen = false;
  for (let i = 0; i < args.length; i++) {
    const [flag, inline] = args[i].split(/=(.*)/s);
    if (flag === "--help" || flag === "-h") { options.help = true; continue; }
    if (flag === "--update-timings") { options.updateTimings = true; continue; }
    if (["--shards", "--log-dir", "--timings-file", "--deadline-ms", "--min-memory-mb", "--leftover-grace-ms"].includes(flag)) {
      const value = inline ?? args[++i];
      if (!value || value.startsWith("--")) throw new Error(`${flag} 缺少参数`);
      if (flag === "--log-dir") { options.logDir = resolve(value); continue; }
      if (flag === "--timings-file") { options.timingsFile = resolve(value); continue; }
      const number = Number(value);
      if (!Number.isSafeInteger(number) || number <= 0 || (flag === "--deadline-ms" && number > 2_147_483_647)) throw new Error(`${flag} 必须是有效的正整数`);
      if (flag === "--shards") options.shards = number;
      if (flag === "--deadline-ms") options.deadlineMs = number;
      if (flag === "--min-memory-mb") options.minMemoryMb = number;
      if (flag === "--leftover-grace-ms") options.leftoverGraceMs = number;
      continue;
    }
    if (flag.startsWith("-") || targetSeen) throw new Error(`无法识别参数：${args[i]}`);
    options.target = resolve(args[i]); targetSeen = true;
  }
  return options;
}

export function collectTests(target: string): string[] {
  const files = new Set<string>(), visited = new Set<string>();
  const visit = (directory: string) => {
    directory = realpathSync(directory);
    if (visited.has(directory)) return;
    visited.add(directory);
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const path = join(directory, entry.name);
      const info = entry.isSymbolicLink() ? statSync(path) : entry;
      // Bun 1.3.12 跟随目录联接，排除隐藏目录与 node_modules（不分大小写）；隐藏测试文件仍可收集。
      if (info.isDirectory() && !entry.name.startsWith(".") && entry.name.toLowerCase() !== "node_modules") visit(path);
      else if (info.isFile() && TEST_FILE.test(entry.name)) files.add(realpathSync(path));
    }
  };
  visit(resolve(target));
  return [...files].sort();
}

export function groupTests(files: string[], timings: Timings, count: number): Group[] {
  const values = Object.values(timings);
  const average = values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 1;
  const weight = (file: string) => timings[keyOf(file)] ?? average;
  const groups: Group[] = Array.from({ length: Math.min(count, files.length) }, () => ({ files: [], seconds: 0 }));
  for (const file of [...files].sort((a, b) => weight(b) - weight(a) || a.localeCompare(b, "en"))) {
    const group = groups.reduce((best, next) => next.seconds < best.seconds || (next.seconds === best.seconds && next.files.length < best.files.length) ? next : best);
    group.files.push(file); group.seconds += weight(file);
  }
  return groups;
}

function readTimings(path: string): Timings {
  if (!existsSync(path)) return {};
  const value = JSON.parse(readFileSync(path, "utf8"));
  if (!value || typeof value !== "object" || Array.isArray(value) || Object.values(value).some(seconds => typeof seconds !== "number" || !Number.isFinite(seconds) || seconds < 0)) {
    throw new Error(`耗时记录必须是文件路径到非负秒数的映射：${path}`);
  }
  return value;
}

function decodeXml(value: string): string {
  return value.replace(/&(#x[\da-f]+|#\d+|amp|lt|gt|quot|apos);/gi, (_, entity: string) => {
    if (entity.startsWith("#")) return String.fromCodePoint(parseInt(entity.slice(entity[1] === "x" ? 2 : 1), entity[1] === "x" ? 16 : 10));
    return ({ amp: "&", lt: "<", gt: ">", quot: '"', apos: "'" } as Record<string, string>)[entity];
  });
}

/**
 * 仅解析 Bun 的 JUnit 标签，不解析 DTD／外部实体，也不累计嵌套套件的重复计数。
 * 逐个标签走一遍并核对配对：报告残缺时计数可能仍然对得上，只查根结束标签会放过去。
 */
export function parseJunit(xml: string): Report {
  const incomplete = (reason: string) => new Error(`JUnit 报告缺失或不完整：${reason}`);
  const source = xml.replace(/^﻿?\s*<\?xml[^>]*\?>/, "").replace(/<!\[CDATA\[[\s\S]*?\]\]>|<!--[\s\S]*?-->/g, "");
  const attrs = (text: string) => Object.fromEntries([...text.matchAll(/([\w:.-]+)\s*=\s*(?:"([^"]*)"|'([^']*)')/g)].map(match => [match[1], decodeXml(match[2] ?? match[3])]));
  const fileKey = (file: string) => keyOf(isAbsolute(file) ? file : resolve(REPO, file));
  const tag = /<(\/?)([A-Za-z_][\w:.-]*)((?:\s+[\w:.-]+\s*=\s*(?:"[^"]*"|'[^']*'))*)\s*(\/?)>/y;
  const result: Report = { passed: 0, failed: 0, skipped: 0, failures: [], files: [] };
  const files = new Set<string>(), stack: string[] = [];
  let root: Record<string, string> | undefined, testcase: { attributes: Record<string, string>; state: "passed" | "failed" | "skipped" } | undefined;
  const finishCase = () => {
    const { attributes, state } = testcase!; testcase = undefined;
    if (!attributes.file) throw new Error("JUnit 用例缺少文件路径");
    const key = fileKey(attributes.file);
    files.add(key);
    if (state === "failed") { result.failed++; result.failures.push(`${key}：${attributes.classname ? `${attributes.classname} > ` : ""}${attributes.name}`); }
    else if (state === "skipped") result.skipped++;
    else result.passed++;
  };
  for (let position = 0;;) {
    const open = source.indexOf("<", position);
    if (!stack.length && source.slice(position, open < 0 ? undefined : open).trim()) throw incomplete("根节点之外有内容");
    if (open < 0) break;
    tag.lastIndex = open;
    const match = tag.exec(source);
    if (!match) throw incomplete("标签不合法或被截断");
    position = tag.lastIndex;
    const [, closing, name, text, selfClosing] = match;
    if (closing) {
      if (stack.pop() !== name) throw incomplete(`结束标签 ${name} 没有配对`);
      if (name === "testcase") finishCase();
      continue;
    }
    if (!stack.length) {
      if (root || name !== "testsuites") throw incomplete(`意外的根节点 ${name}`);
      root = attrs(text);
    } else if (name === "testsuite") {
      const file = attrs(text).file;
      if (file) files.add(fileKey(file));
    } else if (name === "testcase") {
      if (testcase) throw incomplete("用例嵌套");
      testcase = { attributes: attrs(text), state: "passed" };
      if (selfClosing) finishCase();
    } else if (testcase) {
      if (name === "failure" || name === "error") testcase.state = "failed";
      else if (name === "skipped" && testcase.state === "passed") testcase.state = "skipped";
    }
    if (!selfClosing) stack.push(name);
  }
  if (!root) throw incomplete("没有根节点");
  if (stack.length) throw incomplete(`标签 ${stack.at(-1)} 没有结束`);
  const total = result.passed + result.failed + result.skipped;
  if (Number(root.tests) !== total || Number(root.failures) + Number(root.errors ?? 0) !== result.failed || Number(root.skipped ?? 0) !== result.skipped) {
    throw new Error(`JUnit 汇总与用例节点不一致：报告 ${root.tests}，节点 ${total}`);
  }
  result.files = [...files].sort();
  return result;
}

function alive(pid: number): boolean {
  try { process.kill(pid, 0); return true; }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ESRCH") return false; throw error; }
}

function bounded<T>(promise: Promise<T>, ms: number, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  return Promise.race([promise, new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error(`${label}超时`)), ms); })])
    .finally(() => clearTimeout(timer));
}

interface Running { child: ChildProcess; done: Promise<number>; index: number; files: string[]; log: string; xml: string; started: number; seconds: number; code: number | null }

async function stopTree(run: Running): Promise<void> {
  const pid = run.child.pid;
  // 已收到退出事件就不再操作旧 PID，避免长分片期间 PID 被其他进程复用。
  if (run.code !== null || run.child.exitCode !== null || run.child.signalCode !== null) { await bounded(run.done, 5000, "分片退出确认"); return; }
  if (!pid || !alive(pid)) { await bounded(run.done, 5000, "分片退出确认"); return; }
  if (process.platform === "win32") {
    const killer = spawn("taskkill.exe", ["/PID", String(pid), "/T", "/F"], { windowsHide: true, stdio: "ignore", timeout: 10_000 });
    const code = await new Promise<number | null>((done, reject) => { killer.once("error", reject); killer.once("exit", done); });
    if (code !== 0 && alive(pid)) throw new Error(`分片 ${run.index} 进程树回收失败，taskkill 退出码 ${code}`);
  } else {
    // detached 建立本次分片专用进程组；负 PID 只指向这组后代。
    try { process.kill(-pid, "SIGKILL"); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error; }
  }
  await bounded(run.done, 5000, "进程树退出确认");
}

export async function main(args = process.argv.slice(2)): Promise<number> {
  const options = parseArgs(args);
  if (options.help) {
    console.log("用法：bun run test:shards [目录，默认 src/cxb] [--shards 3] [--log-dir 日志父目录] [--deadline-ms 1800000] [--min-memory-mb 1500] [--leftover-grace-ms 15000] [--update-timings] [--timings-file 路径]\n日志父目录下每次新建唯一目录；耗时记录以仓库相对路径保存，单位为秒。分片数超过文件数时只启动非空分片。\n--update-timings 让每个文件单独一个进程、最多同时 --shards 个，量到的是含钩子和加载的实际用时；全部通过才写回，并清掉已删文件的记录。\n分片结束后等待 --leftover-grace-ms，还活着的后代进程只报告，不结束，也不影响退出码。");
    return 0;
  }
  const files = collectTests(options.target);
  if (!files.length) throw new Error(`目录下没有测试文件：${options.target}`);
  const timings = readTimings(options.timingsFile), groups = groupTests(files, timings, options.updateTimings ? files.length : options.shards);
  const concurrency = options.updateTimings ? options.shards : groups.length;
  const freeMb = Math.floor(freemem() / 1024 / 1024);
  console.log(`可用内存：${freeMb} MB；警告阈值：${options.minMemoryMb} MB。`);
  if (freeMb < options.minMemoryMb) console.warn(`警告：可用内存低于阈值，建议减少份数（例如 --shards ${Math.max(1, options.shards - 1)}）。继续运行。`);
  const parent = options.logDir ?? tmpdir(); mkdirSync(parent, { recursive: true });
  const logDir = mkdtempSync(join(parent, "cxb-test-shards-"));
  const started = Date.now(), running: Running[] = [];
  let interrupted = 0, cleanup: Promise<void> | undefined;
  let cancelled!: () => void;
  const cancellation = new Promise<void>(done => { cancelled = done; });
  const cleanupErrors: string[] = [];
  const cancel = (code: number, reason: string) => {
    if (cleanup) return cleanup;
    interrupted = code; console.error(`${reason}；正在结束本次全部分片进程树。日志：${logDir}`);
    cleanup = Promise.allSettled(running.map(stopTree)).then(results => {
      for (const result of results) if (result.status === "rejected") cleanupErrors.push(String(result.reason));
      cancelled();
    });
    return cleanup;
  };
  const onInt = () => { void cancel(130, "收到 Ctrl-C／SIGINT"); };
  const onTerm = () => { void cancel(143, "收到 SIGTERM"); };
  process.on("SIGINT", onInt); process.on("SIGTERM", onTerm);
  const timer = setTimeout(() => { void cancel(124, `超过总期限 ${options.deadlineMs} 毫秒`); }, options.deadlineMs);
  console.log(`收集 ${files.length} 个文件，共 ${groups.length} 份，同时最多 ${concurrency} 份；日志：${logDir}`);
  const watcher = new DescendantWatcher();
  let watchError: string | undefined;
  const sample = (action: () => void = () => watcher.sample()) => {
    if (watchError) return;
    try { action(); } catch (error) { watchError = String(error); }
  };
  const sampler = setInterval(sample, SAMPLE_INTERVAL_MS);
  const planFile = (index: number, extension: string) => join(logDir, `shard-${index + 1}.${extension}`);
  writeFileSync(join(logDir, "manifest.json"), JSON.stringify({ startedAt: new Date(started).toISOString(), freeMb, deadlineMs: options.deadlineMs, concurrency,
    shards: groups.map((group, index) => ({ index: index + 1, files: group.files.map(keyOf), log: planFile(index, "log"), xml: planFile(index, "xml") })) }, null, 2) + "\n");
  const start = (i: number): Running => {
    const group = groups[i], log = planFile(i, "log"), xml = planFile(i, "xml");
    const fd = openSync(log, "w");
    let child: ChildProcess;
    try {
      // 绝对路径是明确的文件列表，避免 Bun 把相对文件名当成模糊过滤器。
      // Windows 不能 detached：那会让分片没有控制台，用例里未隐藏的子进程各开一个可见窗口；进程树回收靠 taskkill /T。
      child = spawn(process.execPath, ["test", ...group.files, "--timeout", "90000", "--reporter=junit", `--reporter-outfile=${xml}`],
        { cwd: REPO, env: { ...process.env }, detached: process.platform !== "win32", windowsHide: true, stdio: ["ignore", fd, fd] });
    } finally { closeSync(fd); }
    const run: Running = { child, done: undefined!, index: i + 1, files: group.files, log, xml, started: Date.now(), seconds: 0, code: null };
    run.done = new Promise<number>(done => {
      const finish = (code: number) => { run.code = code; run.seconds = (Date.now() - run.started) / 1000; done(code); };
      child.once("error", error => { console.error(`分片 ${run.index} 启动失败：${error.message}`); finish(1); });
      child.once("exit", code => finish(code ?? 1));
    });
    running.push(run);
    if (child.pid) { const pid = child.pid; sample(() => { watcher.addRoot(pid, "bun"); watcher.sample(); }); }
    console.log(`分片 ${run.index}：${group.files.length} 个文件，预计 ${group.seconds.toFixed(1)} 秒，PID ${child.pid ?? "启动失败"}。`);
    return run;
  };
  try {
    let next = 0;
    const worker = async () => { while (next < groups.length && !cleanup) await start(next++).done; };
    await Promise.race([Promise.all(Array.from({ length: Math.min(concurrency, groups.length) }, worker)), cancellation]);
    if (cleanup) await cleanup;
  } catch (error) {
    await cancel(1, `启动或执行出错：${String(error)}`);
  } finally {
    clearTimeout(timer); clearInterval(sampler); process.off("SIGINT", onInt); process.off("SIGTERM", onTerm);
  }
  const graceEnd = Date.now() + options.leftoverGraceMs;
  for (sample(); !watchError && watcher.tracked.size && Date.now() < graceEnd; sample()) await Bun.sleep(250);
  const leftovers = watchError ? null : [...watcher.tracked].map(([pid, info]) => ({ pid, name: info.name }));
  const total: Counts = { passed: 0, failed: 0, skipped: 0 };
  const results: Array<Record<string, unknown>> = [];
  let failed = interrupted !== 0 || cleanupErrors.length > 0 || (!interrupted && running.length !== groups.length);
  for (const run of running) {
    let report: Report | undefined, problem: string | undefined;
    try {
      report = parseJunit(readFileSync(run.xml, "utf8"));
      const expected = run.files.map(keyOf).sort();
      if (JSON.stringify(report.files) !== JSON.stringify(expected)) throw new Error("JUnit 文件集合与本份清单不一致");
    } catch (error) { problem = String(error); }
    const counts = report ?? { passed: 0, failed: 0, skipped: 0 };
    total.passed += counts.passed; total.failed += counts.failed; total.skipped += counts.skipped;
    if (run.code === null) { problem = `分片 PID ${run.child.pid} 尚未确认退出`; await stopTree(run).catch(error => cleanupErrors.push(String(error))); }
    const bad = run.code !== 0 || counts.failed > 0 || !!problem;
    failed ||= bad;
    console.log(`分片 ${run.index}：文件 ${run.files.length}，用时 ${run.seconds.toFixed(2)} 秒，通过 ${counts.passed}，失败 ${counts.failed}，跳过 ${counts.skipped}，退出码 ${run.code}${problem ? "（报告不完整）" : ""}。`);
    if (bad) {
      console.error(`失败分片 ${run.index}；日志：${run.log}；JUnit：${run.xml}`);
      for (const name of report?.failures ?? []) console.error(`  失败用例：${name}`);
      if (problem) console.error(`  无法取得完整结果：${problem}`);
      if (!report?.failures.length) {
        const names = readFileSync(run.log, "utf8").split(/\r?\n/).filter(line => line.includes("(fail)"));
        for (const name of names) console.error(`  失败用例（控制台）：${name}`);
        if (!names.length) console.error("  无失败用例节点，请查看日志中的启动／钩子／进程错误。");
      }
    }
    results.push({ index: run.index, files: run.files.length, seconds: run.seconds, passed: counts.passed, failed: counts.failed, skipped: counts.skipped,
      failures: report?.failures ?? [], code: run.code, problem, log: run.log, xml: run.xml });
  }
  for (const error of cleanupErrors) console.error(`进程回收错误：${error}`);
  if (options.updateTimings && !failed) {
    // 中断、缺报告或失败时保留上一份完整记录；原子替换避免留下半份 JSON。
    const updated: Timings = Object.fromEntries(Object.entries(timings).filter(([key]) => existsSync(resolve(REPO, key))));
    for (const run of running) updated[keyOf(run.files[0])] = Math.round(run.seconds * 100) / 100;
    const temporary = `${options.timingsFile}.${process.pid}.tmp`;
    writeFileSync(temporary, JSON.stringify(Object.fromEntries(Object.entries(updated).sort(([a], [b]) => a.localeCompare(b, "en"))), null, 2) + "\n");
    renameSync(temporary, options.timingsFile); console.log(`已更新每文件耗时：${options.timingsFile}`);
  } else if (options.updateTimings) console.warn("本次结果不完整或失败，未覆盖耗时记录。");
  if (watchError) console.warn(`无法检查残留进程：${watchError}`);
  else if (leftovers?.length) console.warn(`残留后代进程 ${leftovers.length} 个（只报告，未结束）：${leftovers.map(item => `${item.name}(${item.pid})`).join("、")}`);
  const seconds = (Date.now() - started) / 1000, code = interrupted || (failed ? 1 : 0);
  console.log(`总计：${files.length} 个文件，${running.length} 份，用时 ${seconds.toFixed(2)} 秒，通过 ${total.passed}，失败 ${total.failed}，跳过 ${total.skipped}，退出码 ${code}。${failed ? "失败或不完整的结果请查看日志。" : "全部分片子进程已退出。"}`);
  writeFileSync(join(logDir, "summary.json"), JSON.stringify({ seconds, ...total, code, interrupted, cleanupErrors, leftovers, shards: results }, null, 2) + "\n");
  return code;
}

if (import.meta.main) {
  try { process.exitCode = await main(); }
  catch (error) { console.error(`分片执行失败：${error instanceof Error ? error.message : String(error)}`); process.exitCode = 1; }
}
