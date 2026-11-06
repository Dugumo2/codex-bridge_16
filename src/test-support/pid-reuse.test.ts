// 进程号被无关程序占用时，测试清理不能把那个程序结束掉；真正遗留的测试进程仍要回收并上报。
import { describe, expect, test } from "bun:test";
import { Processes } from "../cxb/test-fixtures/main-test-support";
import { ProcessTracker } from "../cxb/test-fixtures/ws-test-support";
import { isTestProcess, reapTestProcess, readProcessIdentity } from "./process-identity";

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
  }, 30_000);

  test("端到端测试的进程登记：进程号已被无关程序占用时不结束它，也不算遗留", async () => {
    const bystander = spawnBystander();
    await withProcess(bystander, async () => {
      const processes = new Processes();
      processes.add(bystander.pid);
      await processes.cleanup();
      await expectStillRunning(bystander);
      expect(processes.allExited()).toBe(true);
    });
  }, 40_000);

  test("工作区测试的后代登记：真正遗留的测试进程仍被结束并上报", async () => {
    const sleeper = spawnSleeper();
    await withProcess(sleeper, async () => {
      const processes = new ProcessTracker();
      processes.descendants.add(sleeper.pid);
      const failure = await processes.cleanup().then(() => null, (error) => error);
      expect(JSON.stringify(failure?.errors?.map((error: Error) => error.message))).toContain(`测试遗留进程已强制结束：${sleeper.pid}`);
      expect(await sleeper.exited).toBeDefined();
    });
  }, 30_000);

  test("端到端测试的进程登记：真正遗留的测试进程仍被结束并上报", async () => {
    const sleeper = spawnSleeper();
    await withProcess(sleeper, async () => {
      const processes = new Processes();
      processes.add(sleeper.pid);
      await expect(processes.cleanup()).rejects.toThrow("进程清理失败");
      expect(await sleeper.exited).toBeDefined();
    });
  }, 40_000);

  test("读到真实身份：这次测试拉起的 bun 算测试进程，ping 不算", async () => {
    const sleeper = spawnSleeper();
    const bystander = spawnBystander();
    await withProcess(sleeper, () => withProcess(bystander, async () => {
      const own = await readProcessIdentity(sleeper.pid);
      expect(own?.name.toLowerCase()).toBe("bun");
      expect(isTestProcess(own!)).toBe(true);
      const other = await readProcessIdentity(bystander.pid);
      expect(other?.name.toLowerCase()).toBe("ping");
      expect(isTestProcess(other!)).toBe(false);
    }));
  }, 30_000);

  test("查询已退出的进程号返回空", async () => {
    const child = spawnSleeper();
    child.kill();
    await child.exited;
    expect(await readProcessIdentity(child.pid)).toBeNull();
    expect(await reapTestProcess(child.pid)).toBe("exited");
  }, 30_000);

  test("同名但比测试开始得早的程序不结束", async () => {
    const sleeper = spawnSleeper();
    await withProcess(sleeper, async () => {
      expect(await reapTestProcess(sleeper.pid, 5000, { runStartedAtMs: Date.now() + 60_000 })).toBe("reused");
      await expectStillRunning(sleeper);
    });
  }, 30_000);

  test("确认是测试进程时强杀并确认退出", async () => {
    const sleeper = spawnSleeper();
    await withProcess(sleeper, async () => {
      expect(await reapTestProcess(sleeper.pid)).toBe("killed");
      expect(await sleeper.exited).toBeDefined();
    });
  }, 30_000);

  test("身份查询失败时报错，不强杀", async () => {
    const sleeper = spawnSleeper();
    await withProcess(sleeper, async () => {
      await expect(reapTestProcess(sleeper.pid, 5000, { readIdentity: async () => { throw new Error("查询失败"); } })).rejects.toThrow("查询失败");
      await expectStillRunning(sleeper);
    });
  }, 30_000);

  test("读不到启动时间的程序按比测试早处理", () => {
    expect(isTestProcess({ name: "bun", startedAtMs: 0 })).toBe(false);
    expect(isTestProcess({ name: "BUN", startedAtMs: Date.now() + 1000 })).toBe(true);
    expect(isTestProcess({ name: "cxb-broker", startedAtMs: Date.now() + 1000 })).toBe(true);
    expect(isTestProcess({ name: "QQ", startedAtMs: Date.now() + 1000 })).toBe(false);
  });
});
