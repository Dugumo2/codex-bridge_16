import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import type { ApprovalPolicyName, PolicyDecision } from "./contracts";
import { decidePolicy } from "./server-requests";

// Rust include_str! 指向同一文件；不维护第二份自动应答契约。
const fixture = JSON.parse(readFileSync(new URL("../../broker-rs/tests/fixtures/policy.json", import.meta.url), "utf8")) as {
  cases: Array<{ name: string; policy: ApprovalPolicyName; method: string; params?: unknown; expect: PolicyDecision }>;
};

for (const entry of fixture.cases) {
  test(`策略一致性：${entry.name}`, () => {
    expect(decidePolicy(entry.policy, entry.method, entry.params ?? {})).toEqual(entry.expect);
  });
}
