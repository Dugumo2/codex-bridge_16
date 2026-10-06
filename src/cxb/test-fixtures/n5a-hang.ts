import { writeFileSync } from "node:fs";
writeFileSync(process.argv[2], String(process.pid));
const timer = setInterval(() => {}, 1000);
process.on("SIGTERM", () => { clearInterval(timer); });
