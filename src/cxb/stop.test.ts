import { afterEach, describe, expect, test } from "bun:test";
import { CxbError } from "./contracts";
import { stop } from "./stop";
import { cleanupContexts, context, errorCode, failure, thread } from "./test-fixtures/n4b-support";

afterEach(cleanupContexts, 5000);

describe("乙组 stop", () => {
  test("确认与未确认的输出、退出码及 clear 传参", async () => {
    for (const confirmed of [true, false]) {
      const c = context(["stop", "abcd1234", "--clear", "--json"]);
      c.fake.handle = (m) => m === "broker/threads/list" ? { threads: [thread()] } : {
        threadId: "thread-test", runId: "run-test", interruptSent: true, stopOutcome: confirmed ? "confirmed" : "unconfirmed",
        goal: "cleared", alreadyStopped: false, run: null,
      };
      expect(await stop(c.ctx)).toBe(confirmed ? 0 : 3);
      expect(c.fake.calls[1]).toMatchObject({ method: "broker/threads/stop", params: { threadId: "thread-test", clear: true }, options: { timeoutMs: 45000 } });
      expect(c.json[0]).toMatchObject({ ok: confirmed, shortId: "abcd1234", goal: "cleared", interruptSent: true });
      expect(c.json[0]).not.toHaveProperty("run");
      if (!confirmed) expect(c.notices.join("")).toContain("尚未确认");
      c.checkClean();
    }
  });
  test("无运行的提示与服务器独有 ID 原样透传", async () => {
    for (const alreadyStopped of [true, false]) {
      const c = context(["stop", "server-only"]);
      c.fake.handle = (m) => m === "broker/threads/list" ? { threads: [] } : {
        threadId: "server-only", runId: null, interruptSent: false, stopOutcome: "confirmed", goal: "none", alreadyStopped, run: null,
      };
      expect(await stop(c.ctx)).toBe(0);
      expect(c.fake.calls[1]!.params).toEqual({ threadId: "server-only" });
      expect(c.text.join("")).toContain(alreadyStopped ? "没有运行中的运行" : "已停止线程");
      c.checkClean();
    }
  });
  for (const [reason, code] of [["joined", 8], ["stopping", 8], ["shuttingDown", 6]] as const) {
    test(`停止错误 ${reason} 退出 ${code}`, async () => {
      const c = context(["stop", "abcd1234"]);
      c.fake.handle = (m) => { if (m === "broker/threads/list") return { threads: [thread()] }; throw failure(reason); };
      await errorCode(stop(c.ctx), code);
      c.checkClean();
    });
  }
  test("--run 把运行 ID 传给 broker，用来核实已失联的历史运行（任务说明 N7-A 2.3.1、5.5 节）", async () => {
    const c = context(["stop", "abcd1234", "--run", "run-old", "--json"]);
    c.fake.handle = (m) => m === "broker/threads/list" ? { threads: [thread()] } : {
      threadId: "thread-test", runId: "run-old", interruptSent: false, stopOutcome: "confirmed", goal: "unchanged", alreadyStopped: false, run: null,
    };
    expect(await stop(c.ctx)).toBe(0);
    expect(c.fake.calls[1]).toMatchObject({ method: "broker/threads/stop", params: { threadId: "thread-test", runId: "run-old" } });
    expect(c.json[0]).toMatchObject({ ok: true, runId: "run-old" });
    c.checkClean();
  });
  for (const kind of ["archive", "delete"] as const) test(`线程上有${kind === "archive" ? "归档" : "删除"}待核实时退出 8 并按 kind 提示核实命令`, async () => {
    const c = context(["stop", "abcd1234"]);
    c.fake.handle = (m) => {
      if (m === "broker/threads/list") return { threads: [thread()] };
      throw new CxbError("BROKER_UNAVAILABLE", "模拟线上错误",
        { cause: { rpcCode: -32001, data: { reason: "lifecyclePending", kind, phase: "outcomeUnknown", threadId: "thread-test" } } });
    };
    const error = await stop(c.ctx).then(() => null, (error) => error);
    expect(error).toBeInstanceOf(CxbError);
    expect(error.exitCode).toBe(8);
    expect(error.message).toContain(kind === "delete" ? "cxb rm thread-test --purge" : "cxb rm thread-test");
    c.checkClean();
  });
  test("被占的是关联子线程时，提示里的核实命令指向回包给出的那个线程", async () => {
    const c = context(["stop", "abcd1234"]);
    c.fake.handle = (m) => {
      if (m === "broker/threads/list") return { threads: [thread()] };
      throw new CxbError("BROKER_UNAVAILABLE", "模拟线上错误",
        { cause: { rpcCode: -32001, data: { reason: "lifecyclePending", kind: "archive", phase: "outcomeUnknown", threadId: "review-child" } } });
    };
    const error = await stop(c.ctx).then(() => null, (error) => error);
    expect(error.exitCode).toBe(8);
    expect(error.message).toContain("cxb rm review-child");
    expect(error.message).not.toContain("cxb rm thread-test");
    c.checkClean();
  });
});
