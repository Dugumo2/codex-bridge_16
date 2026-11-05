import { afterEach, describe, expect, spyOn, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { existsSync, mkdtempSync, mkdirSync, readdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve, sep } from "node:path";
import { processCreated, snapshotProcesses, updateDescendants } from "./process-tree";
import { collectTests, groupTests, main, parseArgs, parseJunit } from "./test-shards";

const REPO = resolve(import.meta.dir, "../..");
const created: string[] = [];
const sleepers: number[] = [];

function tempDir(prefix: string): string {
  const root = mkdtempSync(join(tmpdir(), prefix));
  created.push(root);
  return root;
}

function writeFiles(root: string, files: Record<string, string>): void {
  for (const [name, body] of Object.entries(files)) {
    const file = join(root, name);
    mkdirSync(dirname(file), { recursive: true });
    writeFileSync(file, body);
  }
}

function isAlive(pid: number): boolean {
  try { process.kill(pid, 0); return true; }
  catch { return false; }
}

afterEach(() => {
  for (const pid of sleepers.splice(0)) if (isAlive(pid)) process.kill(pid);
  for (const root of created.splice(0)) {
    if (!resolve(root).startsWith(resolve(tmpdir()) + sep)) throw new Error("拒绝删除越界目录");
    rmSync(root, { recursive: true, force: true });
  }
});

/** 真实运行分片主流程；耗时记录指向临时文件，不碰仓库里的记录。 */
async function runMain(target: string, ...args: string[]) {
  const logDir = tempDir("cxb-shards-log-");
  const timings = args.includes("--timings-file") ? [] : ["--timings-file", join(logDir, "timings.json")];
  const code = await main([target, "--log-dir", logDir, ...timings, ...args]);
  const runDir = readdirSync(logDir, { withFileTypes: true }).find(entry => entry.isDirectory());
  if (!runDir) throw new Error("主流程没有创建日志目录");
  return { code, summary: JSON.parse(readFileSync(join(logDir, runDir.name, "summary.json"), "utf8")) };
}

/** 用真实 Bun 跑目录，取它实际收集到的文件。 */
function realBunReport(root: string) {
  const xml = join(tempDir("cxb-shards-xml-"), "out.xml");
  const result = spawnSync(process.execPath, ["test", root, "--reporter=junit", `--reporter-outfile=${xml}`], { cwd: REPO, encoding: "utf8", windowsHide: true, timeout: 60_000 });
  if (result.error) throw result.error;
  return { status: result.status, xml: readFileSync(xml, "utf8") };
}

const PASSING = 'import { test } from "bun:test"; test("通过", () => {});\n';

describe("测试分片：收集", () => {
  test("收集结果与真实 Bun 一致，含各种命名、大小写、隐藏项与依赖目录", () => {
    const root = tempDir("cxb-shards-collect-");
    const body = 'test("x", () => {});\n';
    writeFiles(root, Object.fromEntries([
      "a.test.ts", "b_test.ts", "c.spec.ts", "d_spec.ts", "e.test.js", "f.test.tsx", "g.test.jsx", "h.test.mts", "i.test.cts", "j.test.mjs", "k.test.cjs",
      "l.TEST.TS", "m.Test.ts", ".r.test.ts", "sub/t.test.ts", "x.spec.mjs", "y_spec.cjs",
      "NODE_MODULES/o.test.ts", "node_modules/p.test.ts", ".hidden/q.test.ts", "helper.ts", "test.ts", "s.test.d.ts", "v.tests.ts", "wtest.ts",
    ].map(name => [name, body])));
    const real = realBunReport(root);
    // 前置条件：真实 Bun 确实收到了非 .test.ts 的命名，否则这条用例证明不了什么。
    expect(real.status).toBe(0);
    const bunFiles = parseJunit(real.xml).files.map(key => resolve(REPO, key)).sort();
    expect(bunFiles).toContain(join(root, "b_test.ts"));
    expect(bunFiles).toContain(join(root, "l.TEST.TS"));
    expect(collectTests(root)).toEqual(bunFiles);
  });

  test("跟随目录联接，重复目标及环不重复收集", () => {
    const root = tempDir("cxb-shards-unit-");
    writeFiles(root, { "a.test.ts": "", ".hidden.test.ts": "", "test-fixtures/b.test.ts": "", ".hidden/c.test.ts": "", "helper.ts": "" });
    const expected = [".hidden.test.ts", "a.test.ts", "test-fixtures/b.test.ts"].map(name => join(root, name)).sort();
    expect(collectTests(root)).toEqual(expected);
    // 目录联接不需要 Windows 开发者模式。
    symlinkSync(join(root, "test-fixtures"), join(root, "linked"), process.platform === "win32" ? "junction" : "dir");
    symlinkSync(root, join(root, "test-fixtures", "cycle"), process.platform === "win32" ? "junction" : "dir");
    expect(collectTests(root)).toEqual(expected);
  });
});

describe("测试分片：分组与参数", () => {
  test("优先分配慢文件，新文件取历史均值，同权重退化为按文件数均分", () => {
    const files = ["a", "b", "c", "d"].map(name => resolve(`src/cxb/${name}.test.ts`));
    const groups = groupTests(files, { "src/cxb/a.test.ts": 9, "src/cxb/b.test.ts": 3, "src/cxb/c.test.ts": 6 }, 2);
    expect(groups.map(group => group.seconds)).toEqual([12, 12]);
    expect(groups[0].files).toEqual([files[0], files[1]]);
    expect(groupTests(files, {}, 3).map(group => group.files.length)).toEqual([2, 1, 1]);
    expect(groupTests(files, {}, 9)).toHaveLength(4);
    const zeros = Object.fromEntries(["a", "b", "c", "d"].map(name => [`src/cxb/${name}.test.ts`, 0]));
    expect(groupTests(files, zeros, 3).map(group => group.files.length)).toEqual([2, 1, 1]);
  });

  test("参数按值解析，拒绝零、负数、未知选项与定时器溢出", () => {
    expect(parseArgs([]).shards).toBe(3);
    const parsed = parseArgs(["--shards=2", "--deadline-ms", "3000", "--update-timings", "--leftover-grace-ms", "500", "--timings-file", "t.json"]);
    expect([parsed.shards, parsed.deadlineMs, parsed.updateTimings, parsed.leftoverGraceMs, parsed.timingsFile]).toEqual([2, 3000, true, 500, resolve("t.json")]);
    for (const args of [["--shards", "0"], ["--shards", "-1"], ["--shards", "1.5"], ["--deadline-ms", "2147483648"], ["--concurrent"], ["--log-dir"]]) {
      expect(() => parseArgs(args)).toThrow();
    }
  });
});

describe("测试分片：JUnit 判定", () => {
  const XML = `<?xml version="1.0" encoding="UTF-8"?>
<testsuites tests="4" failures="1" skipped="2"><testsuite file="src/cxb/a.test.ts" time="0"><testsuite file="src/cxb/a.test.ts" time="2">
  <testcase file="src/cxb/a.test.ts" name="通过 a &gt; b" time="1.25" />
  <testcase file="src/cxb/a.test.ts" name="失败 &amp; &lt; &#20013;" classname="套件" time="0.75"><failure><![CDATA[<testcase name="假节点"/>]]></failure></testcase>
  <testcase file="src/cxb/a.test.ts" name="跳过" time="0"><skipped /></testcase>
  <testcase file="src/cxb/a.test.ts" name="待办" time="0"><skipped message="TODO" /></testcase>
  </testsuite></testsuite></testsuites>`;

  test("不重复累加嵌套套件，处理失败、跳过、待办、实体及干扰文本", () => {
    const report = parseJunit(XML);
    expect([report.passed, report.failed, report.skipped]).toEqual([1, 1, 2]);
    expect(report.files).toEqual(["src/cxb/a.test.ts"]);
    expect(report.failures).toEqual(["src/cxb/a.test.ts：套件 > 失败 & < 中"]);
    expect(() => parseJunit(XML.replace('tests="4"', 'tests="5"'))).toThrow("不一致");
  });

  test("结构残缺的报告一律拒绝，即使根结束标签和计数都对", () => {
    const damaged: Array<[string, string]> = [
      ["缺根结束标签", XML.replace("</testsuites>", "")],
      ["缺套件结束标签", XML.replace("</testsuite>", "")],
      ["缺用例结束标签", XML.replace("<skipped /></testcase>", "<skipped />")],
      ["标签交错", XML.replace("</testsuite></testsuite></testsuites>", "</testsuites></testsuite></testsuite>")],
      ["末尾截断在标签中间", XML.slice(0, XML.indexOf('name="待办"'))],
      ["CDATA 未结束", XML.replace("]]>", "")],
      ["根后还有内容", `${XML}<testsuites tests="0" failures="0" skipped="0"></testsuites>`],
      ["根后有文本", `${XML} 垃圾`],
      ["空文件", ""],
    ];
    expect(() => parseJunit(XML)).not.toThrow();
    for (const [label, xml] of damaged) {
      expect(() => parseJunit(xml), label).toThrow("不完整");
    }
  });

  test("真实 Bun 报告：通过、跳过、待办、失败、钩子失败都能正确计数，加载失败的文件不在报告里", () => {
    const root = tempDir("cxb-shards-real-");
    writeFiles(root, {
      "mix.test.ts": 'import { expect, test } from "bun:test"; test("过", () => {}); test.skip("跳", () => {}); test.todo("办"); test("挂 <a> & \\"q\\"", () => { expect(1).toBe(2); });\n',
      "hook.test.ts": 'import { beforeAll, test } from "bun:test"; beforeAll(() => { throw new Error("钩子挂了"); }); test("被拖累", () => {});\n',
      "load.test.ts": 'throw new Error("加载挂了");\n',
    });
    const real = realBunReport(root);
    expect(real.status).not.toBe(0);
    const report = parseJunit(real.xml);
    expect([report.passed, report.skipped]).toEqual([1, 2]);
    expect(report.failed).toBe(2);
    expect(report.failures.some(name => name.includes('挂 <a> & "q"'))).toBe(true);
    const files = report.files.map(key => resolve(REPO, key)).sort();
    expect(files).toEqual([join(root, "hook.test.ts"), join(root, "mix.test.ts")].sort());
  });
});

describe("测试分片：进程树跟踪", () => {
  test("孙进程按快照顺序无关地纳入；父进程退出后孙进程仍跟踪；PID 复用不误纳", () => {
    const createdAt = new Map<number, bigint>([[10, 100n], [20, 110n], [30, 120n], [40, 50n], [50, 130n]]);
    const createdOf = (pid: number) => createdAt.get(pid);
    const tracked = new Map<number, { created: bigint; name: string }>([[10, { created: 100n, name: "bun.exe" }]]);
    // 孙（30）排在子（20）前面；40 的父 PID 恰好是 10，但比 10 更早创建，只能是 PID 复用留下的旧记录。
    updateDescendants(tracked, [
      { pid: 30, parent: 20, name: "leaf.exe" }, { pid: 20, parent: 10, name: "mid.exe" },
      { pid: 10, parent: 1, name: "bun.exe" }, { pid: 40, parent: 10, name: "old.exe" }, { pid: 99, parent: 1, name: "other.exe" },
    ], createdOf);
    expect([...tracked.keys()].sort((a, b) => a - b)).toEqual([10, 20, 30]);
    // 10、20 都退出后 30 仍在跟踪；父进程已退出的新进程不会再被纳入（20 不在了）。
    updateDescendants(tracked, [{ pid: 30, parent: 20, name: "leaf.exe" }, { pid: 50, parent: 20, name: "new.exe" }], createdOf);
    expect([...tracked.keys()]).toEqual([30]);
    // 同一 PID 换了进程（创建时间变了）视为原进程已退出。
    createdAt.set(30, 999n);
    updateDescendants(tracked, [{ pid: 30, parent: 1, name: "reused.exe" }], createdOf);
    expect(tracked.size).toBe(0);
  });

  test("真实快照能看到本进程及其父进程", () => {
    const self = snapshotProcesses().find(entry => entry.pid === process.pid);
    expect(self?.parent).toBe(process.ppid);
    expect(self?.name.toLowerCase()).toContain("bun");
    expect(processCreated(process.pid)).toBeGreaterThan(0n);
  });
});

describe("测试分片：主流程", () => {
  test("正常路径零误报：含短命子进程、_spec 命名，退出 0，无残留、无任何警告", async () => {
    const root = tempDir("cxb-shards-ok-");
    writeFiles(root, {
      "a.test.ts": PASSING,
      "b_spec.ts": PASSING,
      "child.test.ts": 'import { test } from "bun:test"; test("短命子进程", async () => { const p = Bun.spawn([process.execPath, "-e", "setTimeout(()=>{},1500)"], { stdio: ["ignore", "ignore", "ignore"], windowsHide: true }); await p.exited; }, 20000);\n',
    });
    const warn = spyOn(console, "warn"), error = spyOn(console, "error");
    try {
      // 内存阈值放到最低：这条只验证残留与结果判定，本机内存紧张时的提示不算误报。
      const { code, summary } = await runMain(root, "--shards", "2", "--min-memory-mb", "1");
      expect(code).toBe(0);
      expect([summary.passed, summary.failed, summary.skipped]).toEqual([3, 0, 0]);
      expect(summary.leftovers).toEqual([]);
      expect(warn).not.toHaveBeenCalled();
      expect(error).not.toHaveBeenCalled();
    } finally { warn.mockRestore(); error.mockRestore(); }
  }, 60_000);

  test("分片退出后孙进程还活着：报告 PID，不结束它，退出码不变", async () => {
    const root = tempDir("cxb-shards-left-");
    const pidFile = join(root, "leaf.pid");
    writeFiles(root, {
      "leaf.js": "setTimeout(() => {}, 30000);\n",
      "mid.js": `const { spawn } = require("node:child_process"); const leaf = spawn(process.execPath, [${JSON.stringify(join(root, "leaf.js"))}], { detached: true, stdio: "ignore", windowsHide: true }); leaf.unref(); require("node:fs").writeFileSync(${JSON.stringify(pidFile)}, String(leaf.pid)); setTimeout(() => {}, 2500);\n`,
      "left.test.ts": `import { test } from "bun:test"; test("留下孙进程", async () => { const p = Bun.spawn([process.execPath, ${JSON.stringify(join(root, "mid.js"))}], { stdio: ["ignore", "ignore", "ignore"], windowsHide: true }); await p.exited; }, 20000);\n`,
    });
    const { code, summary } = await runMain(root, "--leftover-grace-ms", "500");
    // 前置条件：孙进程确实启动过且活到现在，否则「报告了残留」证明不了什么。
    expect(existsSync(pidFile)).toBe(true);
    const leaf = Number(readFileSync(pidFile, "utf8"));
    sleepers.push(leaf);
    expect(isAlive(leaf)).toBe(true);
    expect(code).toBe(0);
    expect(summary.leftovers.map((item: { pid: number }) => item.pid)).toEqual([leaf]);
  }, 60_000);

  test("失败用例和加载失败都让整体退出 1", async () => {
    const root = tempDir("cxb-shards-bad-");
    writeFiles(root, { "ok.test.ts": PASSING, "bad.test.ts": 'import { expect, test } from "bun:test"; test("必挂", () => { expect(1).toBe(2); });\n' });
    const failed = await runMain(root, "--shards", "2");
    expect(failed.code).toBe(1);
    expect(failed.summary.failed).toBe(1);
    const root2 = tempDir("cxb-shards-load-");
    writeFiles(root2, { "ok.test.ts": PASSING, "load.test.ts": 'throw new Error("加载挂了");\n' });
    const load = await runMain(root2, "--shards", "2");
    expect(load.code).toBe(1);
  }, 60_000);

  test("总期限到达：退出 124，进程树已结束", async () => {
    const root = tempDir("cxb-shards-deadline-");
    const pidFile = join(root, "bun.pid");
    writeFiles(root, { "slow.test.ts": `import { test } from "bun:test"; import { writeFileSync } from "node:fs"; test("睡", async () => { writeFileSync(${JSON.stringify(pidFile)}, String(process.pid)); await Bun.sleep(60000); }, 90000);\n` });
    const { code, summary } = await runMain(root, "--deadline-ms", "2500");
    expect(code).toBe(124);
    expect(summary.interrupted).toBe(124);
    expect(existsSync(pidFile)).toBe(true);
    expect(isAlive(Number(readFileSync(pidFile, "utf8")))).toBe(false);
  }, 60_000);

  test("刷新耗时：每文件单独量墙钟（含钩子），限制同时份数，清理已删文件的记录", async () => {
    const root = tempDir("cxb-shards-timing-");
    const hooked = 'import { beforeAll, test } from "bun:test"; beforeAll(() => Bun.sleep(1200)); test("断言", () => {});\n';
    writeFiles(root, { "a.test.ts": hooked, "b.test.ts": hooked, "c.test.ts": hooked });
    const file = join(root, "timings.json");
    const gone = "src/cxb/已删除的文件.test.ts";
    writeFileSync(file, JSON.stringify({ [gone]: 99 }));
    const { code, summary } = await runMain(root, "--shards", "2", "--update-timings", "--timings-file", file);
    expect(code).toBe(0);
    expect(summary.passed).toBe(3);
    const timings: Record<string, number> = JSON.parse(readFileSync(file, "utf8"));
    expect(timings[gone]).toBeUndefined();
    const values = Object.values(timings);
    expect(values).toHaveLength(3);
    // beforeAll 的 1.2 秒要算进去；只统计用例耗时的话这里接近 0。
    for (const seconds of values) expect(seconds).toBeGreaterThanOrEqual(1.1);
    // 3 个文件、同时 2 份，至少排成两批。
    expect(summary.seconds).toBeGreaterThanOrEqual(2.2);
  }, 60_000);

  test("刷新耗时被期限打断：不再启动后面的文件，也不覆盖原记录", async () => {
    const root = tempDir("cxb-shards-timing-stop-");
    const slow = 'import { test } from "bun:test"; test("睡", async () => { await Bun.sleep(60000); }, 90000);\n';
    writeFiles(root, { "a.test.ts": slow, "b.test.ts": slow });
    const file = join(root, "timings.json");
    writeFileSync(file, "{}");
    const { code, summary } = await runMain(root, "--shards", "1", "--update-timings", "--timings-file", file, "--deadline-ms", "2000");
    expect(code).toBe(124);
    expect(summary.shards).toHaveLength(1);
    expect(readFileSync(file, "utf8")).toBe("{}");
  }, 60_000);
});
