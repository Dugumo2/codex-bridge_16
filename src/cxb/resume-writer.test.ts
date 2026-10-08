import { expect, test } from "bun:test";
import { Writable } from "node:stream";
import { createJsonLineWriter } from "../test-support/resume-history";
import { bounded } from "./wait-run";

for (const failure of ["close", "error", "timeout"] as const) test(`模拟输出背压遇到 ${failure} 后收束队列`, async () => {
  let started!: () => void;
  const writing = new Promise<void>(resolve => { started = resolve; });
  const output = new Writable({ highWaterMark: 1, write(_chunk, _encoding, _done) { started(); } });
  output.on("error", () => {});
  const errors: Error[] = [];
  const send = createJsonLineWriter(output, error => errors.push(error), 100);
  try {
    const first = send({ text: "x".repeat(20_000) });
    const second = send({ id: 2 });
    await bounded(writing, 1000);
    if (failure === "close") output.destroy();
    if (failure === "error") output.destroy(new Error("模拟管道错误"));
    await bounded(Promise.all([first, second]), 1000);
    expect(errors).toHaveLength(1);
    expect(errors[0]!.message).toContain(failure === "timeout" ? "超时" : failure === "close" ? "关闭" : "管道错误");
    expect(output.listenerCount("close")).toBe(0);
  } finally { output.destroy(); }
});
