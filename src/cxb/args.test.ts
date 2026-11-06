import { describe, expect, test } from "bun:test";
import { resolve } from "node:path";
import { config } from "../config";
import { COMMANDS, GLOBAL_OPTIONS, parseArgv, prescanArgv, type CommandName } from "./args";
import { CxbError } from "./contracts";
import { renderHelp } from "./help";

function argvCases(cases: string[][]): { argv: string[] }[] {
  return cases.map((argv) => ({ argv }));
}

function expectUsage(argv: string[], fragment?: string): CxbError {
  let caught: unknown;
  try {
    parseArgv(argv);
  } catch (error) {
    caught = error;
  }
  expect(caught).toBeInstanceOf(CxbError);
  const error = caught as CxbError;
  expect(error.code).toBe("USAGE");
  expect(error.exitCode).toBe(1);
  expect(error.message).not.toContain("cxb: error:");
  if (fragment) expect(error.message).toContain(fragment);
  return error;
}

describe("参数边界与全局选项", () => {
  test("默认值与显式选项分离，重复调用互不污染", () => {
    const result = parseArgv(["run", "中文提示"]);
    expect(result.command).toBe("run");
    expect(result.globals).toEqual({ help: false, version: false, verbose: false, json: false,
      dir: resolve("."), timeout: config.defaultTimeout, sandbox: config.defaultSandbox, approval: config.defaultApprovalPolicy });
    expect(result.options).toEqual({ "converge-timeout": 3600, "stall-timeout": 600, "upstream-error-policy": "observe", continue: false, memory: false, detach: false });
    expect(result.explicit.size).toBe(0);
    parseArgv(["run", "--memory", "--json", "提示"]);
    expect(parseArgv(["run", "提示"]).options.memory).toBe(false);
  });

  test.each(argvCases([
    ["--help"], ["-h"], ["--version"], ["--json", "--help"], [],
  ]))("无命令时帮助和版本可用：%j", ({ argv }) => {
    const result = parseArgv(argv);
    expect(result.command).toBeNull();
    expect(result.globals.help || result.globals.version).toBe(true);
  });

  test("短选项 v 表示详细输出，不是版本", () => {
    const result = parseArgv(["-v", "run", "--json", "提示"]);
    expect(result.globals.verbose).toBe(true);
    expect(result.globals.version).toBe(false);
    expect(result.globals.json).toBe(true);
    expectUsage(["-v"]);
  });

  const pairs: [string, string, string][] = [
    ["dir", "--dir", "带 空格的目录"], ["timeout", "--timeout", "42"], ["model", "--model", "vendor/model:v1[1m]"],
    ["reasoning", "--reasoning", "ultra"], ["sandbox", "--sandbox", "read-only"], ["approval", "--approval", "auto"],
  ];
  test.each(pairs)("全局值 %s 在命令前后与等号写法等价", (key, name, raw) => {
    const before = parseArgv([name, raw, "run", "提示"]);
    const after = parseArgv(["run", name, raw, "提示"]);
    const equals = parseArgv(["run", `${name}=${raw}`, "提示"]);
    expect(before).toEqual(after);
    expect(equals).toEqual(after);
    expect(after.explicit.has(key)).toBe(true);
  });

  test.each([["-m", "--model", "gpt-5"], ["-r", "--reasoning", "high"],
    ["-s", "--sandbox", "workspace-write"], ["-d", "--dir", "."]])("短选项 %s 与长选项等价", (short, long, raw) => {
    expect(parseArgv([short, raw, "run", "提示"])).toEqual(parseArgv(["run", long, raw, "提示"]));
  });

  test("位置参数可与选项交错，保留空白和 Unicode", () => {
    const argv = ["run", " 首段 ", "--json", "中文 😀", "--verbose"];
    const snapshot = [...argv];
    expect(parseArgv(argv).positional).toEqual([" 首段 ", "中文 😀"]);
    expect(argv).toEqual(snapshot);
  });

  test("终止符后不解析选项或等号，第二个终止符也是原文", () => {
    const result = parseArgv(["run", "--", "--help", "--model=秘密", "--", "-v"]);
    expect(result.positional).toEqual(["--help", "--model=秘密", "--", "-v"]);
    expect(result.globals.help).toBe(false);
    expect(result.explicit.size).toBe(0);
    expect(parseArgv(["--", "run", "--json"]).positional).toEqual(["--json"]);
  });

  test("长选项仅在第一个等号处分割", () => {
    expect(parseArgv(["run", "--goal=a=b", "提示"]).options.goal).toBe("a=b");
    expect(parseArgv(["run", "--file=-文件.md"]).options.file).toBe("-文件.md");
  });

  test.each(argvCases([
    ["run", "--不认识", "提示"], ["run", "-vm", "提示"], ["--detach", "run", "提示"],
    ["run", "--timeout"], ["run", "--timeout", "--json", "提示"], ["run", "--timeout", "--", "提示"],
    ["run", "--model="], ["run", "--json=true", "提示"], ["run", "--help=false"],
    ["run", "--dir", ""], ["run", "--file", "\0"], ["run", "--goal", "  ", "提示"],
    ["run", "--timeout", "1", "--timeout=2", "提示"], ["run", "--dir=.", "-d", ".", "提示"],
    ["run", "--memory", "--memory", "提示"], ["review", "--mode=pr", "--mode=custom", "提示"],
    ["run", "--effort", "high", "提示"], ["run", "--content-only", "提示"],
    ["不存在的命令"], ["constructor"], ["__proto__"], ["--"], ["--json"],
  ]))("非法词法或重复选项：%j", ({ argv }) => { expectUsage(argv); });

  test.each(["0", "-1", "1.5", "1e3", "0x10", "NaN", "Infinity", " 1", "1 ", "+1", "", String(config.maxTimeoutSeconds + 1)])(
    "拒绝非法超时 %s", (raw) => { expectUsage(["wait", `--timeout=${raw}`]); },
  );

  test.each([1, config.maxTimeoutSeconds])("接受超时边界 %s", (n) => {
    expect(parseArgv(["wait", `--timeout=${n}`]).globals.timeout).toBe(n);
  });

  test.each([
    ["model", "--model", "gpt-5", "gpt-6"], ["reasoning", "--reasoning", "low", "high"],
    ["sandbox", "--sandbox", "read-only", "workspace-write"], ["approval", "--approval", "never", "auto"],
  ])("重复 %s 后值覆盖并保留详细模式告警", (key, flag, first, last) => {
    const result = parseArgv([flag, first, "run", flag, last, "提示", "-v"]);
    expect(result.globals[key as keyof typeof result.globals]).toBe(last);
    expect(result.warnings).toHaveLength(1);
    expect(result.warnings[0].verboseOnly).toBe(true);
    expect(result.warnings[0].message).toContain(flag);
    expect(result.explicit.has(key)).toBe(true);
    expect(parseArgv(["run", flag, first, flag, last, "提示"]).warnings).toEqual(result.warnings);
  });

  test("通用布尔开关重复是幂等的", () => {
    const result = parseArgv(["-v", "run", "--verbose", "--json", "--json", "提示"]);
    expect(result.warnings).toEqual([]);
    expect(result.globals.verbose).toBe(true);
  });

  for (const [name, values] of [["reasoning", config.reasoningEfforts], ["sandbox", config.sandboxModes], ["approval", config.approvalModes]] as const) {
    test.each([...values])(`${name} 接受配置中的枚举 %s`, (raw) => {
      expect(parseArgv(["run", `--${name}`, raw, "提示"]).globals[name]).toBe(raw);
    });
    test(`${name} 拒绝未知枚举`, () => { expectUsage(["run", `--${name}=不支持`, "提示"]); });
  }

  test("错误消息不回显提示词、模型值或配置值", () => {
    for (const argv of [["run", "--model=令牌秘密"], ["config", "timeout", "令牌秘密"], ["reply", "qu_id", "{令牌秘密"]]) {
      expect(expectUsage(argv).message).not.toContain("令牌秘密");
    }
  });
});

describe("模板名兼容旧加载器", () => {
  test.each(["中文", "review.v2", "代码 审查", "collab", "review-v2_中文", ".", ".review", " 模板 "])(
    "迁移模板的合法基名可以解析且保持原文：%s", (name) => {
      expect(parseArgv(["run", "--template", name, "提示"]).options.template).toBe(name);
      expect(parseArgv(["run", `--template=${name}`, "提示"]).options.template).toBe(name);
    },
  );

  test.each(["", " ", "../中文", "目录/中文", "目录\\中文", "review..v2", "..", "模板\0", "模板\n",
    "模板<", "模板>", "模板:", "模板\"", "模板|", "模板?", "模板*",
    "CON", "con.txt", "PrN", "AUX", "nul", "COM1", "com9.v2", "LPT1", "lpt9", "COM¹", "LPT²",
    "CONIN$", "CONOUT$", "con ", "con .v2"])("拒绝路径、非法字符及保留名：%j", (name) => {
    expectUsage(["run", `--template=${name}`, "提示"]);
  });

  test.each(["console", "auxiliary", "COM0", "COM10", "LPT0", "LPT10", "中文.CON"])(
    "保留名校验不误拒绝其他名称：%s", (name) => {
      expect(parseArgv(["run", "--template", name, "提示"]).options.template).toBe(name);
    },
  );

  test("帮助由选项表同步说明模板名称规则", () => {
    expect(renderHelp("run")).toContain("名称支持中文、空格和单点");
  });
});

describe("参数错误上下文预扫描", () => {
  test.each([
    { argv: ["-d", "D:\\项目", "run", "--timeout", "0", "--json"], command: "run", json: true, verbose: false },
    { argv: ["--model", "x", "run", "--timeout", "0", "--json", "-v"], command: "run", json: true, verbose: true },
    { argv: ["--timeout=5", "run", "--不认识", "--json", "--verbose"], command: "run", json: true, verbose: true },
    { argv: ["--model", "x"], command: null, json: false, verbose: false },
    { argv: ["--timeout=5"], command: null, json: false, verbose: false },
    { argv: ["run", "--", "--json", "-v"], command: "run", json: false, verbose: false },
    { argv: ["--", "run", "--json", "--verbose"], command: null, json: false, verbose: false },
    { argv: ["--json", "run", "-v", "--", "--verbose"], command: "run", json: true, verbose: true },
    { argv: ["--dir", "--", "run", "--json"], command: null, json: false, verbose: false },
    { argv: ["--dir", "--json", "run"], command: "run", json: true, verbose: false },
    { argv: ["run", "--goal=--json", "--file=--verbose"], command: "run", json: false, verbose: false },
    { argv: ["--dir=--json", "--model=--verbose", "run"], command: "run", json: false, verbose: false },
    { argv: ["--model", "run", "review", "--json"], command: "review", json: true, verbose: false },
    { argv: ["--model", "run"], command: null, json: false, verbose: false },
    { argv: ["--model", "-", "run", "--json"], command: "run", json: true, verbose: false },
    { argv: ["未知命令", "run", "--json"], command: null, json: true, verbose: false },
    { argv: ["未知命令", "未知参数"], command: null, json: false, verbose: false },
    { argv: ["--未知=秘密", "--json"], command: null, json: true, verbose: false },
    { argv: ["constructor", "--json"], command: null, json: true, verbose: false },
    { argv: ["__proto__", "--verbose"], command: null, json: false, verbose: true },
    { argv: ["run", "--json=false", "--verbose=true"], command: "run", json: false, verbose: false },
    { argv: ["run", "review", "--json"], command: "run", json: true, verbose: false },
    { argv: ["--json", "--dir"], command: null, json: true, verbose: false },
    { argv: [], command: null, json: false, verbose: false },
  ])("保留参数错误的输出上下文：%j", ({ argv, ...expected }) => {
    const input: string[] = [...argv];
    const original = [...input];
    expect(prescanArgv(input)).toEqual(expected);
    expect(input).toEqual(original);
  });

  test.each(Object.keys(COMMANDS))("只从已知表识别命令：%s", (command) => {
    expect(prescanArgv(["--json", "-d", "目录", command])).toEqual({ command, json: true, verbose: false });
  });

  test.each(GLOBAL_OPTIONS.filter((s) => s.parse).flatMap((s) => s.flags))(
    "全局带值选项 %s 的值即使命中命令表也不作命令", (flag) => {
      expect(prescanArgv([flag, "doctor", "run", "--json"])).toEqual({ command: "run", json: true, verbose: false });
      if (flag.startsWith("--")) {
        expect(prescanArgv([`${flag}=doctor`, "run", "--json"])).toEqual({ command: "run", json: true, verbose: false });
      }
    },
  );

  test("完整解析失败后仍可识别正确命令与 JSON 模式", () => {
    const argv = ["-d", "D:\\项目", "run", "--timeout", "0", "--json"];
    expectUsage(argv);
    expect(prescanArgv(argv)).toEqual({ command: "run", json: true, verbose: false });
  });
});

describe("十一条命令的语法与取值", () => {
  const legal: [CommandName, string[]][] = [
    ["run", ["--resume", "a1b2", "--continue", "本次", "补充"]], ["review", ["--mode", "commit"]],
    ["show", ["a1b2", "--tail", "--follow", "--json"]], ["ls", ["--all", "--local"]],
    ["inbox", ["--kind", "question", "--all", "--limit", "2"]], ["reply", ["qu_123", "回答"]],
    ["wait", []], ["stop", ["a1b2", "--clear"]], ["rm", ["a1b2", "--purge"]],
    ["config", ["timeout", "42"]], ["doctor", ["--json", "-v"]],
  ];
  test.each(legal)("%s 的合法组合", (command, args) => expect(parseArgv([command, ...args]).command).toBe(command));
  test.each(argvCases([
    ["run"], ["review", "--mode", "custom"], ["show"], ["ls", "--limit=10001"],
    ["inbox", "--kind=ask"], ["reply", "qu_123"], ["wait", "--timeout=0"], ["stop"], ["rm"],
    ["config", "未知键"], ["doctor", "多余参数"],
  ]))("命令的非法组合：%j", ({ argv }) => { expectUsage(argv); });

  test.each(argvCases([
    ["run", "--continue", "提示"], ["run", "--resume", "a1b2"], ["run", "--run", "run_123", "提示"],
    ["run", "--budget=1", "提示"], ["run", "--goal=目标", "--budget=0", "提示"],
    ["run", "--goal=目标", "--budget=9007199254740992", "提示"], ["run", "--file=文件", "提示"],
    ["run", "--file=文件", "-"], ["run", "-", "提示"], ["run", "-", "-"], ["run", "  "],
    ["run", "--resume=a1b2", "--continue", "-"], ["run", "--resume=a1b2", "--continue", "--file=文件"],
    ["run", "--resume=../id", "提示"], ["run", "--template=../模板", "提示"],
    ["run", "--resume=a1b2", "--continue", "--run=../运行"],
    ["run", "--resume=a1b2", "--continue", "--goal=目标"], ["run", "--resume=a1b2", "--continue", "--template=审阅"],
    ["review", "--mode=pr", "说明"], ["review", "--mode=uncommitted", "说明"], ["review", "--mode=commit", "说明"],
    ["review", "--mode=uncommitted", "--ref=HEAD"], ["review", "--ref=HEAD"], ["review", "--mode=commit", "--base=main"],
    ["review", "--base=main", "说明"], ["review", "--goal=目标"], ["review", "--budget=2"],
    ["review", "--ref=-分支", "--mode=commit"], ["review", "--base=main;命令"], ["review", "--base=a b"],
    ["review", "--mode=custom", " "], ["review", "--mode=未知"],
    ["show", "id", "--tail", "--log"], ["show", "id", "--follow", "--log"], ["show", "id", "--limit=0"],
    ["ls", "--all", "--limit=2"], ["ls", "--limit=infinity"], ["ls", "id"], ["inbox", "--limit=1.5"], ["inbox", "id"],
    ["reply", "ap_123", "文字"], ["reply", "gd_123", "文字"], ["reply", "qu_123", "{不合法"],
    ["reply", "qu_123", "{\"问题\":1}"], ["reply", "qu_123", "{\"问题\":[1]}"], ["reply", "qu_123", " "],
    ["reply", "qu_123", "-", "说明"], ["reply", "../id", "回答"], ["wait", "a", "b"], ["stop", "a", "b"],
    ["rm", "a", "b"], ["config", "model", "gpt-5", "--unset"], ["config", "templates", "--unset"],
    ["config", "templates", "collab"], ["config", "memory", "1"], ["config", "server", "其他"],
    ["config", "timeout", "0"], ["config", "broker-idle-timeout", "0"], ["config", "__proto__"],
    ["config", "constructor"], ["config", "model", "gpt", "多余"],
  ]))("拒绝互斥、依赖或值错误：%j", ({ argv }) => { expectUsage(argv); });

  test.each(argvCases([
    ["run", "-"], ["run", "--file=不存在但不在解析层读取.md"],
    ["run", "--resume=a1b2", "--continue", "--run=run_123"], ["run", "--resume=a1b2", "新提示"],
    ["run", "--resume=a1b2", "--file=文件.md"], ["run", "--resume=a1b2", "-"],
    ["run", "提示", "--goal=目标", "--budget=9007199254740991", "--memory", "--detach", "--template=collab"],
    ["review", "--mode=pr", "--base=main"], ["review", "--mode=uncommitted"],
    ["review", "--mode=commit", "--ref=HEAD@{1}"], ["review", "--mode=custom", "说明"],
    ["reply", "ap_123", "YES"], ["reply", "ap_123", "Accept"], ["reply", "gd_123", "Reject"], ["reply", "old_id", "回答"],
    ["reply", "qu_123", "{\"问题\":[\"一\",\"二\"]}"], ["reply", "mc_123", "{\"字段\":1}"], ["reply", "qu_123", "-"],
    ["config"], ["config", "--unset"], ["config", "model"], ["config", "model", "--unset"], ["config", "templates"],
    ["config", "model", "provider/gpt:v1[1m]"], ["config", "reasoning", "max"], ["config", "sandbox", "read-only"],
    ["config", "approval", "auto"], ["config", "memory", "true"], ["config", "memory", "false"],
    ["config", "server", "private"], ["config", "broker-idle-timeout", "300"],
  ]))("接受边界组合：%j", ({ argv }) => { expect(parseArgv(argv).command).toBe(argv[0] as CommandName); });

  test("review 默认 pr，位置说明选 custom，commit 缺省 HEAD", () => {
    expect(parseArgv(["review"]).options.mode).toBe("pr");
    expect(parseArgv(["review", "审查说明"]).options.mode).toBe("custom");
    expect(parseArgv(["review", "--mode=commit"]).options.ref).toBe("HEAD");
    expect(parseArgv(["review"]).options.base).toBeUndefined();
  });

  test("review 忽略沙箱和审批，并返回始终需要显示的告警", () => {
    const result = parseArgv(["-s", "danger-full-access", "--approval=auto", "review"]);
    expect(result.globals.sandbox).toBe("read-only");
    expect(result.globals.approval).toBe("never");
    expect(result.warnings).toHaveLength(2);
    expect(result.warnings.every((w) => !w.verboseOnly)).toBe(true);
    expect(parseArgv(["review"]).warnings).toEqual([]);
    expectUsage(["review", "--sandbox=未知"]);
    expectUsage(["review", "--approval=未知"]);
  });

  test("show 跟随隐含 tail，ls 全部不限制数量，inbox 全部只扩大范围", () => {
    expect(parseArgv(["show", "id", "--follow"]).options).toMatchObject({ follow: true, tail: true, limit: 50 });
    expect(parseArgv(["ls"]).options.limit).toBe(20);
    expect(parseArgv(["ls", "--all"]).options.limit).toBe(Infinity);
    expect(parseArgv(["inbox", "--all", "--limit=2"]).options.limit).toBe(2);
    expect(parseArgv(["inbox"]).options.limit).toBeUndefined();
  });

  for (const command of ["show", "ls", "inbox", "reply", "stop", "rm", "config", "doctor"] as const) {
    test(`${command} 拒绝不适用的执行选项，放在命令前也不能绕过`, () => {
      expectUsage(["--model=gpt-5", command, "--help"], "不支持");
      expectUsage([command, "--timeout=1", "--help"], "不支持");
    });
  }

  test.each(["show", "wait", "stop", "rm"])("%s 接受 UUID 并拒绝路径穿越", (command) => {
    const uuid = "12345678-1234-1234-1234-123456789abc";
    expect(parseArgv([command, uuid]).positional).toEqual([uuid]);
    for (const bad of ["../id", "..\\id", "a/b", "a:b", "", "a\0b"]) expectUsage([command, bad]);
  });
});

describe("子代理树选项（第二期）", () => {
  test("wait --tree 只接受线程 ID", () => {
    expect(parseArgv(["wait", "t1", "--tree"]).options.tree).toBe(true);
    expect(parseArgv(["wait", "t1"]).options.tree).toBe(false);
    expectUsage(["wait", "--tree"], "--tree 须带线程 ID");
    for (const item of ["ap_1", "qu_1", "mc_1", "gd_1"]) expectUsage(["wait", item, "--tree"], "--tree 须带线程 ID");
  });

  test("show --tree 须与 --log 或 --tail 同用，不能与 --follow、--server 同用", () => {
    expect(parseArgv(["show", "t1", "--log", "--tree"]).options.tree).toBe(true);
    expect(parseArgv(["show", "t1", "--tail", "--tree"]).options.tree).toBe(true);
    expectUsage(["show", "t1", "--tree"], "须与 --log 或 --tail 同用");
    expectUsage(["show", "t1", "--follow", "--tree"], "不能与 --follow 同用");
    expectUsage(["show", "t1", "--tail", "--follow", "--tree"], "不能与 --follow 同用");
    expectUsage(["show", "t1", "--log", "--server", "--tree"], "不能与 --server 同用");
  });
});

describe("帮助与选项表", () => {
  const names: CommandName[] = ["run", "review", "show", "ls", "inbox", "reply", "wait", "stop", "rm", "config", "doctor"];
  const expectedFlags: Record<CommandName, string[]> = {
    run: ["--converge-timeout", "--stall-timeout", "--upstream-error-policy", "--mcp", "--no-mcp", "--file", "--resume", "--continue", "--run", "--template", "--goal", "--budget", "--memory", "--detach"],
    review: ["--converge-timeout", "--stall-timeout", "--upstream-error-policy", "--mcp", "--no-mcp", "--mode", "--ref", "--base", "--resume"], show: ["--tail", "--log", "--follow", "--server", "--limit", "--full", "--tree"],
    ls: ["--all", "--limit", "--local"], inbox: ["--kind", "--all", "--limit"], reply: [], wait: ["--tree"],
    stop: ["--abandon", "--clear", "--run"], rm: ["--purge"], config: ["--unset"], doctor: [],
  };
  test("总帮助恰好列出十一条命令和全局选项", () => {
    expect(Object.keys(COMMANDS)).toEqual(names);
    const help = renderHelp();
    for (const name of names) expect(help).toContain(`  ${name}  `);
    for (const spec of GLOBAL_OPTIONS) for (const flag of spec.flags) expect(help).toContain(flag);
    expect(GLOBAL_OPTIONS.map((spec) => spec.flags.at(-1))).toEqual([
      "--help", "--version", "--verbose", "--json", "--dir", "--timeout", "--model", "--reasoning", "--sandbox", "--approval",
    ]);
  });

  test.each(names)("%s 帮助可以单独解析，包含全部适用选项", (name) => {
    const result = parseArgv([name, "--help"]);
    expect(result.command).toBe(name);
    expect(result.globals.help).toBe(true);
    expect(parseArgv(["--help", name]).globals.help).toBe(true);
    expect(COMMANDS[name].options.map((o) => o.flags.at(-1))).toEqual(expectedFlags[name]);
    const help = renderHelp(name);
    const specs = [...GLOBAL_OPTIONS.filter((o) => COMMANDS[name].globals.some((key) => key === o.key)), ...COMMANDS[name].options];
    for (const spec of specs) for (const flag of spec.flags) expect(help).toContain(flag);
    // 选项定义必须实际被解析器接受；关系校验另有独立命令用例覆盖。
    const samples: Record<string, string> = { dir: ".", timeout: "1", "converge-timeout": "1", "stall-timeout": "0", "upstream-error-policy": "observe", model: "gpt-5", reasoning: "high", sandbox: "read-only",
      approval: "auto", file: "文件.md", resume: "a1b2", run: "run_123", template: "collab", goal: "目标", budget: "1",
      mode: "pr", ref: "HEAD", base: "main", limit: "1", kind: "approval", mcp: "serena", "no-mcp": "backlog" };
    for (const spec of specs) {
      for (const flag of spec.flags) {
        const args = spec.parse ? [flag, samples[spec.key]] : [flag];
        expect(parseArgv([name, ...args, "--help"]).explicit.has(spec.key)).toBe(true);
        if (spec.parse && flag.startsWith("--")) {
          expect(parseArgv([name, `${flag}=${samples[spec.key]}`, "--help"])).toEqual(parseArgv([name, ...args, "--help"]));
        }
      }
    }
  });

  test.each([null, ...names])("帮助 %s 的每行不超过一百显示列且以换行结束", (name) => {
    const help = renderHelp(name);
    for (const line of help.split("\n")) {
      const columns = Array.from(line).reduce((sum, c) => sum + (/^[\u0000-\u00ff]$/.test(c) ? 1 : 2), 0);
      expect(columns).toBeLessThanOrEqual(100);
    }
    expect(help.endsWith("\n")).toBe(true);
  });

  test("帮助体现续跑新规则与 NDJSON 例外", () => {
    expect(renderHelp("run")).toContain("本次补充");
    expect(renderHelp("show")).toContain("NDJSON");
    expect(renderHelp("review")).toContain("告警并忽略");
    expectUsage(["run", "--help", "--不存在"]);
    expectUsage(["run", "--help", "--timeout=0"]);
    expect(() => renderHelp("frobnicate" as CommandName)).toThrow(CxbError);
  });
});
