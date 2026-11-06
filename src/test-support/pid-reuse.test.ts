// 进程号被无关程序占用时，测试清理不能把那个程序结束掉；真正遗留的测试进程仍要回收并上报。
import { describe, expect, test } from "bun:test";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { Processes } from "../cxb/test-fixtures/main-test-support";
import { SrHost } from "../cxb/test-fixtures/sr-host";
import { ProcessTracker } from "../cxb/test-fixtures/ws-test-support";
import { isTestProcess, processGone, reapTestProcess, readProcessIdentity, waitForProcessGone, type ProcessIdentity } from "./process-identity";

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

  test("查询已退出的进程号返回空", async () => {
    const child = spawnSleeper();
    child.kill();
    await child.exited;
    expect(await readProcessIdentity(child.pid)).toBeNull();
    expect(await reapTestProcess(child.pid)).toBe("exited");
    expect(await processGone(child.pid)).toBe(true);
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
