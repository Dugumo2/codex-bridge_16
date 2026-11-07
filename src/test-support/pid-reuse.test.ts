// 进程号被无关程序占用时，测试清理不能把那个程序结束掉；真正遗留的测试进程仍要回收并上报。
import { describe, expect, spyOn, test } from "bun:test";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { Processes } from "../cxb/test-fixtures/main-test-support";
import { SrHost } from "../cxb/test-fixtures/sr-host";
import { ProcessTracker } from "../cxb/test-fixtures/ws-test-support";
import { isTestProcess, processGone, reapTestProcess, readProcessIdentity, waitForProcessGone, type ProcessIdentity } from "./process-identity";
import { startManaged, stopManaged, type ManagedProcess } from "./upstream-isolation";

/** 模拟占用了旧进程号的无关程序：用 ping 是因为它和测试自己拉起的程序都不同名。 */
function spawnBystander() {
  return Bun.spawn(["ping.exe", "-n", "120", "127.0.0.1"], { stdin: "ignore", stdout: "ignore", stderr: "ignore", windowsHide: true });
}

/** 模拟遗留的测试进程。 */
function spawnSleeper() {
  return Bun.spawn([process.execPath, "-e", "setTimeout(() => {}, 120000)"], { stdin: "ignore", stdout: "ignore", stderr: "ignore", windowsHide: true });
}

type Spawned = ReturnType<typeof spawnBystander>;

async function withProcess(child: Spawned, work: () => Promise<void>): Promise<void> {
  try {
    expect(child.exitCode).toBeNull();
    await work();
  } finally {
    child.kill();
    await child.exited;
  }
}

async function expectStillRunning(child: Spawned): Promise<void> {
  await Bun.sleep(300);
  expect(child.exitCode).toBeNull();
}

/** 让对这个进程号的第一次判活看到「已不存在」，模拟 stopManaged 走到「关 stdin 等退出」那条路。 */
function firstProbeSeesNothing(pid: number) {
  const realKill = process.kill.bind(process);
  let first = true;
  return spyOn(process, "kill").mockImplementation(((target: number, signal?: string | number) => {
    if (target === pid && signal === 0 && first) {
      first = false;
      throw Object.assign(new Error("没有这个进程"), { code: "ESRCH" });
    }
    return realKill(target, signal);
  }) as typeof process.kill);
}

describe.skipIf(process.platform !== "win32")("按进程号回收前核实身份", () => {
  test("工作区测试的后代登记：进程号已被无关程序占用时不结束它，也不算遗留", async () => {
    const bystander = spawnBystander();
    await withProcess(bystander, async () => {
      const processes = new ProcessTracker();
      processes.descendants.add(bystander.pid);
      await processes.cleanup();
      await expectStillRunning(bystander);
    });
  }, 40_000);

  test("端到端测试的进程登记：进程号已被无关程序占用时不结束它，也不算遗留", async () => {
    const bystander = spawnBystander();
    await withProcess(bystander, async () => {
      const processes = new Processes();
      processes.add(bystander.pid);
      await processes.cleanup();
      await expectStillRunning(bystander);
      expect(processes.allExited()).toBe(true);
    });
  }, 50_000);

  test("工作区测试的后代登记：真正遗留的测试进程仍被结束并上报", async () => {
    const sleeper = spawnSleeper();
    await withProcess(sleeper, async () => {
      const processes = new ProcessTracker();
      processes.descendants.add(sleeper.pid);
      const failure = await processes.cleanup().then(() => null, (error) => error);
      expect(JSON.stringify(failure?.errors?.map((error: Error) => error.message))).toContain(`测试遗留进程已强制结束：${sleeper.pid}`);
      expect(await sleeper.exited).toBeDefined();
    });
  }, 40_000);

  test("端到端测试的进程登记：真正遗留的测试进程仍被结束并上报", async () => {
    const sleeper = spawnSleeper();
    await withProcess(sleeper, async () => {
      const processes = new Processes();
      processes.add(sleeper.pid);
      await expect(processes.cleanup()).rejects.toThrow("进程清理失败");
      expect(await sleeper.exited).toBeDefined();
    });
  }, 50_000);

  test("串行测试的宿主：broker 已退出、进程号被无关程序占用时不用 taskkill 结束它", async () => {
    const bystander = spawnBystander();
    await withProcess(bystander, async () => {
      const host = new SrHost(mkdtempSync(join(tmpdir(), "cxb-pid-reuse-")));
      (host as unknown as { broker: unknown }).broker = { pid: bystander.pid, exitCode: 0, signalCode: null };
      await host.cleanup();
      await expectStillRunning(bystander);
    });
  }, 30_000);

  test("真实上游的回收：进程已退出、进程号被无关程序占用时不用 taskkill 结束它", async () => {
    const bystander = spawnBystander();
    await withProcess(bystander, async () => {
      // 关 stdin 后进程退出，但后代还占着管道，closed 超过 2 秒才到；这时句柄已显示退出，而这个号已被 ping 占用。
      const child = { pid: bystander.pid, exitCode: null as number | null, signalCode: null, stdin: { end() { child.exitCode = 0; } } };
      const closed = Bun.sleep(2500).then(() => 0);
      const spy = firstProbeSeesNothing(bystander.pid);
      try {
        await stopManaged({ child, closed } as unknown as ManagedProcess);
      } finally {
        spy.mockRestore();
      }
      await expectStillRunning(bystander);
    });
  }, 40_000);

  test("真实上游的回收：等 2 秒后进程还在运行时仍用 taskkill 回收", async () => {
    const managed = startManaged([process.execPath, "-e", "setTimeout(() => {}, 120000)"], { cwd: tmpdir(), env: process.env });
    const pid = managed.child.pid;
    const spy = firstProbeSeesNothing(pid);
    try {
      await stopManaged(managed);
    } finally {
      spy.mockRestore();
      managed.child.kill();
    }
    expect(managed.child.exitCode !== null || managed.child.signalCode !== null).toBe(true);
    expect(await processGone(pid)).toBe(true);
  }, 40_000);

  test("真实上游的回收：还在运行的进程连同后代一起回收", async () => {
    // 后代要 detached：普通子进程会随父进程一起结束，那样不用 /T 也看不出区别。
    const script = `const c = require("node:child_process").spawn(process.execPath, ["-e", "setTimeout(() => {}, 120000)"], { detached: true, stdio: "ignore", windowsHide: true }); c.unref(); console.log("后代=" + c.pid); setTimeout(() => {}, 120000);`;
    const managed = startManaged([process.execPath, "-e", script], { cwd: tmpdir(), env: process.env });
    let descendant = 0;
    try {
      const deadline = Date.now() + 15_000;
      while (!descendant && Date.now() < deadline) {
        descendant = Number(/后代=(\d+)/.exec(managed.output().stdout)?.[1] ?? 0);
        await Bun.sleep(25);
      }
      expect(descendant).toBeGreaterThan(0);
      await stopManaged(managed);
      expect(managed.child.exitCode !== null || managed.child.signalCode !== null).toBe(true);
      expect(await processGone(descendant)).toBe(true);
    } finally {
      managed.child.kill();
      if (descendant) await reapTestProcess(descendant);
    }
  }, 40_000);

  test("端到端测试的进程登记：运行超时并确认退出后不再留登记，进程号之后被占用也不算存活", async () => {
    const processes = new Processes();
    const spawned: number[] = [];
    const realAdd = processes.add.bind(processes);
    processes.add = (pid: number) => { spawned.push(pid); realAdd(pid); };
    await expect(processes.run(process.execPath, ["-e", "setTimeout(() => {}, 120000)"], { cwd: tmpdir(), env: process.env, limitMs: 500 })).rejects.toThrow("子进程执行失败");
    expect(spawned).toHaveLength(1);
    // 进程已确认退出；此后这个号被别的程序占用，判活会看到它「还活着」。
    const realKill = process.kill.bind(process);
    const spy = spyOn(process, "kill").mockImplementation(((target: number, signal?: string | number) => {
      if (target === spawned[0] && signal === 0) return true;
      return realKill(target, signal);
    }) as typeof process.kill);
    try {
      expect(processes.allExited()).toBe(true);
    } finally {
      spy.mockRestore();
    }
  }, 40_000);

  test("读到真实身份：这次测试拉起的 bun 算测试进程，ping 不算", async () => {
    const sleeper = spawnSleeper();
    const bystander = spawnBystander();
    await withProcess(sleeper, () => withProcess(bystander, async () => {
      const own = await readProcessIdentity(sleeper.pid);
      expect(own?.name.toLowerCase()).toBe("bun.exe");
      expect(own?.exePath.toLowerCase()).toBe(process.execPath.toLowerCase());
      expect(own?.commandLine).toContain(" -e ");
      expect(isTestProcess(own!)).toBe(true);
      const other = await readProcessIdentity(bystander.pid);
      expect(other?.name.toLowerCase()).toBe("ping.exe");
      expect(isTestProcess(other!)).toBe(false);
    }));
  }, 40_000);

  test("查询不存在的进程号返回空", async () => {
    // 不用刚退出的进程：它的号几秒内就可能被别的程序占用，并行时甚至可能是别的测试进程，回收会误杀它。
    const missing = 0x7ffffffc;
    expect(await readProcessIdentity(missing)).toBeNull();
    expect(await reapTestProcess(missing)).toBe("exited");
    expect(await processGone(missing)).toBe(true);
  }, 40_000);

  test("同名但比测试开始得早的程序不结束", async () => {
    const sleeper = spawnSleeper();
    await withProcess(sleeper, async () => {
      expect(await reapTestProcess(sleeper.pid, 5000, { runStartedAtMs: Date.now() + 60_000 })).toBe("reused");
      await expectStillRunning(sleeper);
    });
  }, 40_000);

  test("确认是测试进程时强杀并确认退出", async () => {
    const sleeper = spawnSleeper();
    await withProcess(sleeper, async () => {
      expect(await reapTestProcess(sleeper.pid)).toBe("killed");
      expect(await sleeper.exited).toBeDefined();
    });
  }, 40_000);

  test("身份查询失败时报错，不强杀", async () => {
    const sleeper = spawnSleeper();
    await withProcess(sleeper, async () => {
      await expect(reapTestProcess(sleeper.pid, 5000, { readIdentity: async () => { throw new Error("查询失败"); } })).rejects.toThrow("查询失败");
      await expectStillRunning(sleeper);
    });
  }, 40_000);

  test("强杀后进程号立刻被无关程序占用时，不误报仍存活", async () => {
    // 第一次查询看到测试进程，强杀后第二次查询看到的已是别的程序；ping 活着，正好模拟「等不到退出」。
    const bystander = spawnBystander();
    await withProcess(bystander, async () => {
      let queries = 0;
      const killed: number[] = [];
      const outcome = await reapTestProcess(bystander.pid, 300, {
        kill: (pid) => { killed.push(pid); },
        readIdentity: async () => (queries++ === 0
          ? { name: "bun.exe", exePath: process.execPath, commandLine: "bun -e x", startedAtMs: Date.now() }
          : { name: "ping.exe", exePath: "", commandLine: "", startedAtMs: Date.now() }),
      });
      expect(outcome).toBe("killed");
      expect(queries).toBe(2);
      expect(killed).toEqual([bystander.pid]);
    });
  }, 40_000);

  test("强杀后还是同一个测试进程时报仍存活", async () => {
    const sleeper = spawnSleeper();
    await withProcess(sleeper, async () => {
      await expect(reapTestProcess(sleeper.pid, 300, { kill: () => undefined })).rejects.toThrow("强杀后仍存活");
    });
  }, 40_000);

  test("等待退出：进程号被无关程序占用算已退出，测试进程还在就报错", async () => {
    const sleeper = spawnSleeper();
    const bystander = spawnBystander();
    await withProcess(sleeper, () => withProcess(bystander, async () => {
      expect(await processGone(bystander.pid)).toBe(true);
      await waitForProcessGone(bystander.pid, 100, "不该报错");
      expect(await processGone(sleeper.pid)).toBe(false);
      await expect(waitForProcessGone(sleeper.pid, 100, "测试进程没有退出")).rejects.toThrow("测试进程没有退出");
    }));
  }, 40_000);

  test("身份判断：时间、程序名、路径都要符合", () => {
    const later = Date.now() + 1000;
    const repo = resolve(import.meta.dir, "..", "..");
    const bun = (commandLine: string, startedAtMs = later): ProcessIdentity => ({ name: "bun.exe", exePath: process.execPath, commandLine, startedAtMs });
    const broker = (exePath: string): ProcessIdentity => ({ name: "cxb-broker.exe", exePath, commandLine: "cxb-broker serve", startedAtMs: later });

    expect(isTestProcess(bun(`bun "${join(repo, "src", "fixture.ts")}"`))).toBe(true);
    expect(isTestProcess(bun(`bun ${join(tmpdir(), "x", "codex-mock.ts").toUpperCase()}`))).toBe(true);
    expect(isTestProcess(bun("bun -e code"))).toBe(true);
    expect(isTestProcess(bun("bun Z:\\installed\\scripts\\cxb.js run"))).toBe(false);
    expect(isTestProcess(bun(`bun "${join(repo, "src", "fixture.ts")}"`, 0))).toBe(false);
    expect(isTestProcess(bun(""))).toBe(false);

    expect(isTestProcess(broker(join(repo, "broker-rs", "target", "debug", "cxb-broker.exe")))).toBe(true);
    expect(isTestProcess(broker(join(tmpdir(), "bin", "cxb-broker.exe")))).toBe(true);
    expect(isTestProcess(broker("Z:\\installed\\bin\\cxb-broker.exe"))).toBe(false);
    expect(isTestProcess(broker(""))).toBe(false);

    expect(isTestProcess({ name: "QQ.exe", exePath: join(repo, "QQ.exe"), commandLine: `QQ -e ${repo}`, startedAtMs: later })).toBe(false);
  });

  test("身份判断：CXB_BROKER_BIN 指定的 broker 算测试进程", () => {
    const saved = process.env.CXB_BROKER_BIN;
    const identity: ProcessIdentity = { name: "cxb-broker.exe", exePath: "Z:\\build\\cxb-broker.exe", commandLine: "", startedAtMs: Date.now() + 1000 };
    try {
      process.env.CXB_BROKER_BIN = "Z:\\Build\\CXB-Broker.exe";
      expect(isTestProcess(identity)).toBe(true);
      process.env.CXB_BROKER_BIN = "Z:\\other\\cxb-broker.exe";
      expect(isTestProcess(identity)).toBe(false);
    } finally {
      if (saved === undefined) delete process.env.CXB_BROKER_BIN; else process.env.CXB_BROKER_BIN = saved;
    }
  });
});
