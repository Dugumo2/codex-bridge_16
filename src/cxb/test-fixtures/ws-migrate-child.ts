import { existsSync, writeFileSync } from "node:fs";
import { migrateLegacyData } from "../workspace";

const [dataRoot, legacyRoot, ready, start] = process.argv.slice(2);
writeFileSync(ready, String(process.pid));
const deadline = Date.now() + 5000;
while (!existsSync(start)) {
  if (Date.now() >= deadline) throw new Error("等待并发迁移起跑信号超时");
  await Bun.sleep(10);
}
process.stdout.write(JSON.stringify(migrateLegacyData({ dataRoot, legacyRoot })) + "\n");
