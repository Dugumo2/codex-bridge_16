import { afterEach, describe, expect, spyOn, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { existsSync, mkdtempSync, mkdirSync, readdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve, sep } from "node:path";
import { checkSnapshot, processCreated, snapshotProcesses, updateDescendants } from "./process-tree";
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
      ["结束标签带属性", XML.replace("</testsuite></testsuite>", '</testsuite broken="1"></testsuite>')],
      ["结束标签自闭合", XML.replace("</testsuite></testsuite>", "</testsuite/></testsuite>")],
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
  test("孙进程按快照顺序无关地纳入；PID 复用留下的旧父 PID 不误纳", () => {
    const createdAt = new Map<number, bigint>([[10, 100n], [20, 110n], [30, 120n], [40, 50n]]);
    const tracked = new Map<number, { created: bigint; name: string }>([[10, { created: 100n, name: "bun.exe" }]]);
    // 孙（30）排在子（20）前面；40 的父 PID 恰好是 10，但比 10 更早创建，只能是 PID 复用留下的旧记录。
    updateDescendants(tracked, [
      { pid: 30, parent: 20, name: "leaf.exe" }, { pid: 20, parent: 10, name: "mid.exe" },
      { pid: 10, parent: 1, name: "bun.exe" }, { pid: 40, parent: 10, name: "old.exe" }, { pid: 99, parent: 1, name: "other.exe" },
    ], (pid) => createdAt.get(pid));
    expect([...tracked.keys()].sort((a, b) => a - b)).toEqual([10, 20, 30]);
  });

  test("已跟踪的父进程在两次采样之间退出：它新起的子进程仍纳入，比它早的不纳入，随后父进程才移除", () => {
    const createdAt = new Map<number, bigint>([[60, 130n], [70, 50n]]);
    const tracked = new Map<number, { created: bigint; name: string }>([[10, { created: 100n, name: "bun.exe" }]]);
    updateDescendants(tracked, [{ pid: 60, parent: 10, name: "leaf.exe" }, { pid: 70, parent: 10, name: "old.exe" }], (pid) => createdAt.get(pid));
    expect([...tracked.keys()]).toEqual([60]);
  });

  test("同一 PID 换了进程视为原进程已退出，新进程的子进程不算后代", () => {
    const createdAt = new Map<number, bigint>([[10, 999n], [80, 1000n]]);
    const tracked = new Map<number, { created: bigint; name: string }>([[10, { created: 100n, name: "bun.exe" }]]);
    updateDescendants(tracked, [{ pid: 10, parent: 1, name: "reused.exe" }, { pid: 80, parent: 10, name: "child.exe" }], (pid) => createdAt.get(pid));
    expect(tracked.size).toBe(0);
  });

  test("还在快照里但查询失败的进程继续跟踪，不当成已退出；真正消失后才移除", () => {
    const tracked = new Map<number, { created: bigint; name: string }>([[30, { created: 120n, name: "leaf.exe" }]]);
    updateDescendants(tracked, [{ pid: 30, parent: 1, name: "leaf.exe" }], () => undefined);
    expect([...tracked.keys()]).toEqual([30]);
    updateDescendants(tracked, [], () => undefined);
    expect(tracked.size).toBe(0);
  });

  test("快照里没有本进程说明枚举不完整，报错而不是当成进程都退出了", () => {
    const entries = [{ pid: 5, parent: 1, name: "a.exe" }];
    expect(checkSnapshot(entries, 5)).toEqual(entries);
    expect(() => checkSnapshot(entries, 6)).toThrow("不完整");
    expect(() => checkSnapshot([], 6)).toThrow("不完整");
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
      "mid.js": `const { spawn } = require("node:child_process"); const leaf = spawn(process.execPath, [${JSON.stringify(join(root, "leaf.js"))}], { detached: true, stdio: "ignore", windowsHide: true }); leaf.unref(); require("node:fs").writeFileSync(${JSON.stringify(pidFile)}, String(leaf.pid)); setTimeout(() => {}, 4000);\n`,
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

  test("分片起了子进程就立刻退出，两次采样之间来不及看到父子同在：子进程仍要报告", async () => {
    const root = tempDir("cxb-shards-fast-");
    const pidFile = join(root, "leaf.pid");
    writeFiles(root, {
      "leaf.js": "setTimeout(() => {}, 30000);\n",
      "fast.test.ts": `import { test } from "bun:test"; import { spawn } from "node:child_process"; import { writeFileSync } from "node:fs"; test("起了就走", () => { const leaf = spawn(process.execPath, [${JSON.stringify(join(root, "leaf.js"))}], { detached: true, stdio: "ignore", windowsHide: true }); leaf.unref(); writeFileSync(${JSON.stringify(pidFile)}, String(leaf.pid)); });\n`,
    });
    const { code, summary } = await runMain(root, "--leftover-grace-ms", "500");
    expect(existsSync(pidFile)).toBe(true);
    const leaf = Number(readFileSync(pidFile, "utf8"));
    sleepers.push(leaf);
    expect(isAlive(leaf)).toBe(true);
    expect(code).toBe(0);
    expect(summary.leftovers.map((item: { pid: number }) => item.pid)).toEqual([leaf]);
  }, 60_000);

  test("进程快照失败：明确提示无法检查，summary 里是 null 而不是残留 0，不影响退出码", async () => {
    const root = tempDir("cxb-shards-nowatch-");
    writeFiles(root, { "ok.test.ts": PASSING });
    const logDir = tempDir("cxb-shards-log-");
    const warn = spyOn(console, "warn");
    try {
      const code = await main([root, "--log-dir", logDir, "--timings-file", join(logDir, "timings.json"), "--min-memory-mb", "1"], {
        writeManifest: (path, text) => writeFileSync(path, text),
        createWatcher: () => ({ tracked: new Map(), addRoot() {}, sample() { throw new Error("快照失败"); } }),
      });
      expect(code).toBe(0);
      const runDir = readdirSync(logDir, { withFileTypes: true }).find(entry => entry.isDirectory())!;
      expect(JSON.parse(readFileSync(join(logDir, runDir.name, "summary.json"), "utf8")).leftovers).toBeNull();
      expect(warn.mock.calls.some(call => String(call[0]).includes("无法检查残留进程"))).toBe(true);
    } finally { warn.mockRestore(); }
  }, 60_000);

  test("写清单失败：不抛出、不挂住，定时器和信号监听都已清理，退出 1", async () => {
    const root = tempDir("cxb-shards-manifest-");
    writeFiles(root, { "ok.test.ts": PASSING });
    const logDir = tempDir("cxb-shards-log-");
    const intervals = spyOn(globalThis, "clearInterval");
    const listeners = [process.listenerCount("SIGINT"), process.listenerCount("SIGTERM")];
    try {
      const code = await main([root, "--log-dir", logDir, "--timings-file", join(logDir, "timings.json")], {
        writeManifest: () => { throw new Error("磁盘已满"); },
        createWatcher: () => ({ tracked: new Map(), addRoot() {}, sample() {} }),
      });
      expect(code).toBe(1);
      expect(intervals).toHaveBeenCalled();
    } finally {
      intervals.mockRestore();
      expect([process.listenerCount("SIGINT"), process.listenerCount("SIGTERM")]).toEqual(listeners);
    }
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

  test("总期限到达：退出 124，每份分片和它起的子进程都已结束", async () => {
    const root = tempDir("cxb-shards-deadline-");
    const leafScript = JSON.stringify(join(root, "leaf.js"));
    // 子进程必须 detached：普通子进程会随父进程一起退出，测不出 taskkill /T 是否结束了整棵树。
    const slow = (name: string) => `import { test } from "bun:test"; import { spawn } from "node:child_process"; import { writeFileSync } from "node:fs"; test("睡", async () => { const leaf = spawn(process.execPath, [${leafScript}], { detached: true, stdio: "ignore", windowsHide: true }); leaf.unref(); writeFileSync(${JSON.stringify(join(root, `${name}.pids`))}, process.pid + "," + leaf.pid); await Bun.sleep(60000); }, 90000);\n`;
    writeFiles(root, { "leaf.js": "setTimeout(() => {}, 60000);\n", "a.test.ts": slow("a"), "b.test.ts": slow("b") });
    const { code, summary } = await runMain(root, "--shards", "2", "--deadline-ms", "8000", "--leftover-grace-ms", "3000");
    expect(code).toBe(124);
    expect(summary.interrupted).toBe(124);
    // 前置条件：两份分片都起了子进程才到期，否则证明不了「整棵树」。
    const pids = ["a", "b"].flatMap(name => readFileSync(join(root, `${name}.pids`), "utf8").split(",").map(Number));
    expect(pids).toHaveLength(4);
    sleepers.push(...pids);
    expect(pids.filter(isAlive)).toEqual([]);
    expect(summary.leftovers).toEqual([]);
  }, 60_000);

  test("刷新耗时：每文件单独量墙钟（含钩子），同时运行正好 2 份，清理已删文件的记录", async () => {
    const root = tempDir("cxb-shards-timing-");
    const spans = join(root, "spans");
    mkdirSync(spans);
    // 屏障：至少两个文件都开始后才继续，串行实现会在这里超时失败；钩子再睡 1.2 秒，并发超过 2 时区间必然重叠。
    const hooked = (name: string) => `import { beforeAll, test } from "bun:test"; import { readdirSync, writeFileSync } from "node:fs"; import { join } from "node:path";
const dir = ${JSON.stringify(spans)};
beforeAll(async () => {
  writeFileSync(join(dir, "${name}.start"), String(Date.now()));
  const until = Date.now() + 30000;
  while (readdirSync(dir).filter(file => file.endsWith(".start")).length < 2) { if (Date.now() > until) throw new Error("同时运行的文件不足 2 个"); await Bun.sleep(20); }
  await Bun.sleep(1200);
}, 60000);
test("断言", () => { writeFileSync(join(dir, "${name}.end"), String(Date.now())); });\n`;
    writeFiles(root, { "a.test.ts": hooked("a"), "b.test.ts": hooked("b"), "c.test.ts": hooked("c") });
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
    const read = (name: string) => Number(readFileSync(join(spans, name), "utf8"));
    const events = ["a", "b", "c"].flatMap(name => [{ at: read(`${name}.start`), delta: 1 }, { at: read(`${name}.end`), delta: -1 }]).sort((x, y) => x.at - y.at || x.delta - y.delta);
    let running = 0, peak = 0;
    for (const event of events) { running += event.delta; peak = Math.max(peak, running); }
    expect(peak).toBe(2);
  }, 90_000);

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
