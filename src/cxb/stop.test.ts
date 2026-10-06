import { afterEach, describe, expect, test } from "bun:test";
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
});
