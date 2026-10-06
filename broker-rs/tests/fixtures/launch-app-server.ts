// M8 进程级测试的替身 app-server：按行记下 pid、ppid、cwd、argv 与收到 stdin EOF，只应答 initialize。
// 环境变量：
// - FIXTURE_PID_LOG：记录文件，每行一个完整 JSON；
// - FIXTURE_GATE：闸门文件路径。测试创建它表示「放行」，替身在下面两处停住等它出现：
//   - FIXTURE_INIT_ERROR=1：收到 initialize 先记一行，等放行后才回错误（让测试先拿到 serve 的句柄）；
//   - FIXTURE_HOLD_EXIT=1：收到 stdin EOF 先记一行，等放行后才退出（让 broker 停在关闭第 6 步）。
import { appendFileSync, existsSync } from "node:fs";

const log = process.env.FIXTURE_PID_LOG;
const gate = process.env.FIXTURE_GATE;
const record = (entry: Record<string, unknown>) => {
  if (log) appendFileSync(log, JSON.stringify(entry) + "\n");
};
const send = (message: unknown) => process.stdout.write(JSON.stringify(message) + "\n");
const released = async () => {
  while (gate && !existsSync(gate)) await new Promise((resolve) => setTimeout(resolve, 20));
};

record({ event: "start", pid: process.pid, ppid: process.ppid, cwd: process.cwd(), argv: process.argv.slice(2) });

let pending = "";
const decoder = new TextDecoder();
for await (const chunk of process.stdin) {
  pending += decoder.decode(chunk, { stream: true });
  let newline: number;
  while ((newline = pending.indexOf("\n")) >= 0) {
    const line = pending.slice(0, newline).trim();
    pending = pending.slice(newline + 1);
    if (!line) continue;
    const message = JSON.parse(line);
    if (message.method === "initialize") {
      if (process.env.FIXTURE_INIT_ERROR === "1") {
        record({ event: "initialize", pid: process.pid });
        await released();
        send({ id: message.id, error: { code: -32000, message: "拒绝握手" } });
      } else {
        send({ id: message.id, result: { userAgent: "launch-fixture" } });
      }
    } else if (message.id !== undefined && message.method !== undefined) {
      send({ id: message.id, result: null });
    }
  }
}

record({ event: "eof", pid: process.pid });
if (process.env.FIXTURE_HOLD_EXIT === "1") await released();
process.exit(0);
