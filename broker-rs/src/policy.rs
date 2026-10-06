//! 托管线程的自动应答策略（详细设计 4.2 节第 4 条、命令面规格 6.1 节）。【甲路实现】
//!
//! 纯函数：输入审批策略与一条服务端请求，给出「直接回答（附回包 result）」「进 inbox」或「回 -32601」。
//! 规则与 S4 的 TS 实现共用一份表驱动用例 `broker-rs/tests/fixtures/policy.json`（第 9 节第 8 条），
//! 两边都必须逐条通过，防止行为分叉。fixture 是契约，实现与它不一致时改实现，不改 fixture。

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// `broker/threads/detach` 的 `policy` 参数（2.3 节），与 codex 的 approvalPolicy 取值一致。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Policy {
    #[serde(rename = "never")]
    Never,
    #[serde(rename = "auto")]
    Auto,
    #[serde(rename = "on-request")]
    OnRequest,
    #[serde(rename = "on-failure")]
    OnFailure,
    #[serde(rename = "untrusted")]
    Untrusted,
}

impl Policy {
    /// 线上字符串到策略；不认识的取值返回 None（detach 据此回 -32602）。
    pub fn parse(text: &str) -> Option<Self> {
        serde_json::from_value(Value::String(text.to_owned())).ok()
    }
}

/// 一条服务端请求在托管线程上的处置。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// 直接以这个 result 回给 app-server。
    Answer(Value),
    /// 登记进 inbox，等 `cxb reply`。
    Inbox,
    /// 以 -32601 `Method not found: <method>` 回给 app-server（托管线程上的动态工具，4.1 节）。
    MethodNotFound,
}

/// 按策略决定托管线程上一条服务端请求的处置。`params` 是请求原样的 params（缺省按 `{}`）。
pub fn decide(policy: Policy, method: &str, params: &Value) -> Decision {
    use serde_json::json;
    if method == "item/tool/call" {
        return Decision::MethodNotFound;
    }
    // 拍板 L、28：提问总是交给人，Default 的 isBlocking:false 也不例外。
    if method == "item/tool/requestUserInput" || policy != Policy::Never {
        return Decision::Inbox;
    }
    Decision::Answer(match method {
        "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => {
            json!({"decision":"accept"})
        }
        "item/permissions/requestApproval" => {
            json!({"scope":"session", "permissions": params.get("permissions").filter(|v| !v.is_null()).cloned().unwrap_or(json!({}))})
        }
        "mcpServer/elicitation/request" => json!({"action":"accept", "content":{}}),
        _ => return Decision::Inbox,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn 所有策略下非阻塞提问也必须交给人() {
        for policy in [
            Policy::Never,
            Policy::Auto,
            Policy::OnRequest,
            Policy::OnFailure,
            Policy::Untrusted,
        ] {
            for params in [
                json!({"isBlocking":false,"questions":[{"id":"q","options":[{"label":"甲"}]}]}),
                json!({}),
                Value::Null,
            ] {
                assert_eq!(
                    decide(policy, "item/tool/requestUserInput", &params),
                    Decision::Inbox
                );
            }
        }
    }

    #[test]
    fn 策略契约逐条断言() {
        let fixture: Value =
            serde_json::from_str(include_str!("../tests/fixtures/policy.json")).unwrap();
        for case in fixture["cases"].as_array().unwrap() {
            let policy = Policy::parse(case["policy"].as_str().unwrap()).unwrap();
            let got = decide(
                policy,
                case["method"].as_str().unwrap(),
                case.get("params").unwrap_or(&json!({})),
            );
            let expected = match case["expect"]["kind"].as_str().unwrap() {
                "answer" => Decision::Answer(case["expect"]["result"].clone()),
                "inbox" => Decision::Inbox,
                "methodNotFound" => Decision::MethodNotFound,
                _ => panic!("未知契约类型"),
            };
            assert_eq!(got, expected, "{}", case["name"]);
        }
        assert!(Policy::parse("invalid").is_none());
    }
}
