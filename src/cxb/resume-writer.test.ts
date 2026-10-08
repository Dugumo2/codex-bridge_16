import { expect, test } from "bun:test";
import { Writable } from "node:stream";
import { createJsonLineWriter } from "../test-support/resume-history";
import { bounded } from "./wait-run";

function listenerCounts(output: Writable) {
  return Object.fromEntries(["close", "error", "drain"].map(event => [event, output.listenerCount(event)]));
}

/** 写回调由测试放行，使每次背压与 drain 都有明确控制点。 */
function controlledOutput() {
  const writes: Array<{ bytes: Buffer; done: () => void }> = [];
  const waiting = new Map<number, () => void>();
  const output = new Writable({ highWaterMark: 1, write(chunk, _encoding, done) {
    writes.push({ bytes: Buffer.from(chunk), done });
    waiting.get(writes.length - 1)?.();
    waiting.delete(writes.length - 1);
  } });
  // 保留原有监听器，既检查临时监听器清理，也检查原有监听器没有被移除。
  for (const event of ["close", "error", "drain"]) output.on(event, () => {});
  const originalListeners = listenerCounts(output);
  const nextWrite = async (index: number) => {
    if (!writes[index]) await bounded(new Promise<void>(resolve => waiting.set(index, resolve)), 1000);
    return writes[index]!;
  };
  const drainAll = async (expected: Buffer) => {
    let length = 0;
    for (let index = 0; length < expected.length; index++) {
      const write = await nextWrite(index);
      length += write.bytes.length;
      expect(Buffer.concat(writes.map(value => value.bytes))).toEqual(Buffer.from(expected.subarray(0, length)));
      write.done();
    }
  };
  return { output, writes, originalListeners, nextWrite, drainAll };
}

test("模拟输出受控 drain 后继续写完当前消息与排队消息", async () => {
  const h = controlledOutput();
  const errors: Error[] = [];
  const send = createJsonLineWriter(h.output, error => errors.push(error));
  const messages = [{ id: 1, result: { text: "x".repeat(20_000) } }, { id: 2, result: "后续响应" }];
  const expected = Buffer.from(messages.map(value => JSON.stringify(value) + "\n").join(""));
  try {
    let completed = false;
    const pending = Promise.all(messages.map(send)).then(() => { completed = true; });
    const first = await h.nextWrite(0);
    expect(h.writes).toHaveLength(1);
    expect(first.bytes.length).toBeLessThan(expected.length);
    expect(completed).toBe(false);
    expect(listenerCounts(h.output)).toEqual({ close: 2, error: 2, drain: 2 });
    first.done();
    await h.nextWrite(1);
    expect(h.writes).toHaveLength(2);
    expect(completed).toBe(false);
    // 第一块已放行；继续逐块释放剩余背压。
    for (let index = 1, length = first.bytes.length; length < expected.length; index++) {
      const write = await h.nextWrite(index);
      length += write.bytes.length;
      write.done();
    }
    await bounded(pending, 1000);
    expect(Buffer.concat(h.writes.map(value => value.bytes))).toEqual(expected);
    expect(errors).toEqual([]);
    expect(listenerCounts(h.output)).toEqual(h.originalListeners);
  } finally { h.output.destroy(); }
});

test("模拟输出响应与通知混排时按完整消息串行", async () => {
  const h = controlledOutput();
  const errors: Error[] = [];
  const send = createJsonLineWriter(h.output, error => errors.push(error));
  const messages = [
    { id: 1, result: { text: "响应".repeat(5000) } },
    { method: "turn/started", params: { text: "通知".repeat(5000) } },
    { id: 2, result: { status: "completed" } },
    { method: "turn/completed", params: { status: "completed" } },
  ];
  const expected = Buffer.from(messages.map(value => JSON.stringify(value) + "\n").join(""));
  try {
    const pending = Promise.all(messages.map(send));
    await h.drainAll(expected);
    await bounded(pending, 1000);
    const lines = Buffer.concat(h.writes.map(value => value.bytes)).toString("utf8").split("\n");
    expect(lines.pop()).toBe("");
    expect(lines.map(line => JSON.parse(line))).toEqual(messages);
    expect(errors).toEqual([]);
    expect(listenerCounts(h.output)).toEqual(h.originalListeners);
  } finally { h.output.destroy(); }
});

test("模拟输出入队后修改原对象不影响已排队内容", async () => {
  const h = controlledOutput();
  const errors: Error[] = [];
  const send = createJsonLineWriter(h.output, error => errors.push(error));
  const blocker = { id: 1, result: { text: "x".repeat(20_000) } };
  const queued = { id: 2, result: { turn: { status: "inProgress", items: [{ text: "原内容" }] } } };
  const expected = Buffer.from([blocker, queued].map(value => JSON.stringify(value) + "\n").join(""));
  try {
    const first = send(blocker);
    await h.nextWrite(0);
    const second = send(queued);
    queued.id = 99;
    queued.result.turn.status = "completed";
    queued.result.turn.items[0]!.text = "改写内容";
    queued.result.turn.items.push({ text: "新增内容" });
    await h.drainAll(expected);
    await bounded(Promise.all([first, second]), 1000);
    expect(Buffer.concat(h.writes.map(value => value.bytes))).toEqual(expected);
    expect(errors).toEqual([]);
    expect(listenerCounts(h.output)).toEqual(h.originalListeners);
  } finally { h.output.destroy(); }
});

for (const failure of ["close", "error", "timeout"] as const) test(`模拟输出背压遇到 ${failure} 后收束队列`, async () => {
  let started!: () => void;
  const writing = new Promise<void>(resolve => { started = resolve; });
  const output = new Writable({ highWaterMark: 1, write(_chunk, _encoding, _done) { started(); } });
  output.on("error", () => {});
  const originalListeners = listenerCounts(output);
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
    expect(listenerCounts(output)).toEqual(originalListeners);
  } finally { output.destroy(); }
});
