import { parseArgv } from "../args";
import { runMain } from "../output";
import { createRunHandler } from "../run";
import { createReviewHandler } from "../review";
import { fakeServerRequests, openConnection } from "./run-harness";
import type { WorkspaceInfo } from "../contracts";

const workspace: WorkspaceInfo = JSON.parse(process.env.RUN_FIXTURE_WORKSPACE!);
const parsed = parseArgv(process.argv.slice(2));
const handler = parsed.command === "review" ? createReviewHandler(fakeServerRequests()) : createRunHandler(fakeServerRequests());
await runMain((out, registerCleanup) => handler({ parsed, out, registerCleanup, workspace, connect: async () => {
  const connection = await openConnection(workspace);
  const signal = process.env.RUN_FIXTURE_SIGNAL;
  if (signal === "SIGINT" || signal === "SIGTERM") {
    const dispose = connection.rpc.on("turn/started", () => { process.emit(signal); });
    registerCleanup(dispose);
  }
  return connection;
} }),
  { command: parsed.command, json: parsed.globals.json, verbose: parsed.globals.verbose, cleanupTimeoutMs: 50_000 });
