//! 已知文件输出的嵌套结构检查；不透明 JSON 字段不推测内部语义。
use serde_json::Value;

pub(super) fn optional(value: &Value, key: &str, valid: impl Fn(&Value) -> bool) -> bool {
    value.get(key).is_none_or(|v| v.is_null() || valid(v))
}

fn string(value: &Value, key: &str) -> bool {
    value.get(key).is_some_and(Value::is_string)
}

fn array(value: &Value, key: &str, valid: impl Fn(&Value) -> bool) -> bool {
    value
        .get(key)
        .and_then(Value::as_array)
        .is_some_and(|a| a.iter().all(valid))
}

pub(super) fn image(value: &Value) -> bool {
    (string(value, "image_url") || string(value, "file_id"))
        && optional(value, "detail", |v| {
            matches!(v.as_str(), Some("auto" | "low" | "high" | "original"))
        })
}

pub(super) fn reasoning(value: &Value) -> bool {
    array(value, "summary", |v| {
        v["type"] == "summary_text" && string(v, "text")
    }) && optional(value, "content", |v| {
        v.as_array().is_some_and(|parts| {
            parts.iter().all(|part| {
                matches!(part["type"].as_str(), Some("reasoning_text" | "text"))
                    && string(part, "text")
            })
        })
    }) && optional(value, "encrypted_content", Value::is_string)
}

pub(super) fn tool_output(value: &Value) -> bool {
    value.is_string()
        || value.as_array().is_some_and(|parts| {
            parts.iter().all(|part| match part["type"].as_str() {
                Some("input_text") => string(part, "text"),
                Some("input_audio") => string(part, "audio_url"),
                Some("input_image") => image(part),
                Some("encrypted_content") => string(part, "encrypted_content"),
                _ => false,
            })
        })
}

pub(super) fn shell_action(value: &Value) -> bool {
    value["type"] == "exec"
        && array(value, "command", Value::is_string)
        && optional(value, "timeout_ms", Value::is_u64)
        && optional(value, "working_directory", Value::is_string)
        && optional(value, "user", Value::is_string)
        && optional(value, "env", |v| {
            v.as_object()
                .is_some_and(|m| m.values().all(Value::is_string))
        })
}

pub(super) fn web_action(value: &Value) -> bool {
    match value["type"].as_str() {
        Some("search") => {
            optional(value, "query", Value::is_string)
                && optional(value, "queries", |v| {
                    v.as_array().is_some_and(|a| a.iter().all(Value::is_string))
                })
        }
        Some("open_page") => optional(value, "url", Value::is_string),
        Some("find_in_page") => {
            optional(value, "url", Value::is_string) && optional(value, "pattern", Value::is_string)
        }
        // 上游 Other 接住未知变体，但不能证明未知动作与覆盖无关。
        _ => false,
    }
}

pub(super) fn plan(value: &Value) -> bool {
    value.as_array().is_some_and(|items| {
        items.iter().all(|item| {
            string(item, "step")
                && item.as_object().is_some_and(|m| {
                    m.keys()
                        .all(|key| matches!(key.as_str(), "step" | "status"))
                })
                && matches!(
                    item["status"].as_str(),
                    Some("pending" | "in_progress" | "completed")
                )
        })
    })
}

pub(super) fn parsed_commands(value: &Value) -> bool {
    value.as_array().is_some_and(|items| {
        items.iter().all(|item| {
            string(item, "cmd")
                && match item["type"].as_str() {
                    Some("read") => string(item, "name") && string(item, "path"),
                    Some("list_files") => optional(item, "path", Value::is_string),
                    Some("search") => {
                        optional(item, "path", Value::is_string)
                            && optional(item, "query", Value::is_string)
                    }
                    Some("unknown") => true,
                    _ => false,
                }
        })
    })
}

pub(super) fn changes(value: &Value) -> bool {
    value.as_object().is_some_and(|items| {
        items.values().all(|item| match item["type"].as_str() {
            Some("add" | "delete") => string(item, "content"),
            Some("update") => {
                string(item, "unified_diff") && optional(item, "move_path", Value::is_string)
            }
            _ => false,
        })
    })
}

pub(super) fn duration(value: &Value) -> bool {
    value.get("secs").is_some_and(Value::is_u64)
        && value
            .get("nanos")
            .and_then(Value::as_u64)
            .is_some_and(|n| n < 1_000_000_000)
}

pub(super) fn mcp_result(value: &Value) -> bool {
    value.as_object().is_some_and(|m| {
        m.len() == 1
            && (value.get("Err").is_some_and(Value::is_string)
                || value.get("Ok").is_some_and(|v| {
                    v.get("content").is_some_and(Value::is_array)
                        && optional(v, "is_error", Value::is_boolean)
                }))
    })
}

pub(super) fn usage(value: &Value) -> bool {
    [
        "input_tokens",
        "cached_input_tokens",
        "output_tokens",
        "reasoning_output_tokens",
        "total_tokens",
    ]
    .iter()
    .all(|key| value.get(key).is_some_and(Value::is_i64))
        && defaulted(value, "cache_write_input_tokens", Value::is_i64)
}

pub(super) fn usage_info(value: &Value) -> bool {
    value.get("total_token_usage").is_some_and(usage)
        && value.get("last_token_usage").is_some_and(usage)
        && optional(value, "model_context_window", Value::is_i64)
}

pub(super) fn rate_limits(value: &Value) -> bool {
    let window = |v: &Value| {
        v.get("used_percent").is_some_and(Value::is_number)
            && optional(v, "window_minutes", Value::is_i64)
            && optional(v, "resets_at", Value::is_i64)
    };
    value.is_object()
        && ["limit_id", "limit_name", "normal_model_slug", "plan_type"]
            .iter()
            .all(|key| optional(value, key, Value::is_string))
        && optional(value, "rate_limit_reached_type", rate_limit_reached)
        && optional(value, "primary", window)
        && optional(value, "secondary", window)
        && optional(value, "credits", |v| {
            v.get("has_credits").is_some_and(Value::is_boolean)
                && v.get("unlimited").is_some_and(Value::is_boolean)
                && optional(v, "balance", Value::is_string)
        })
        && optional(value, "spend_control_reached", Value::is_boolean)
        && optional(value, "individual_limit", |v| {
            string(v, "limit")
                && string(v, "used")
                && v.get("remaining_percent").is_some_and(i32_value)
                && v.get("resets_at").is_some_and(Value::is_i64)
        })
}

/// serde(default) 允许缺字段，但非 Option 字段不能用 null 冒充默认值。
pub(super) fn defaulted(value: &Value, key: &str, valid: impl Fn(&Value) -> bool) -> bool {
    value.get(key).is_none_or(valid)
}

pub(super) fn terminal_status(value: &Value) -> bool {
    matches!(value.as_str(), Some("completed" | "failed" | "declined"))
}

pub(super) fn command_source(value: &Value) -> bool {
    matches!(
        value.as_str(),
        Some("agent" | "user_shell" | "unified_exec_startup" | "unified_exec_interaction")
    )
}

pub(super) fn abort_reason(value: &Value) -> bool {
    matches!(
        value.as_str(),
        Some("interrupted" | "replaced" | "review_ended" | "budget_limited")
    )
}

pub(super) fn mode_kind(value: &Value) -> bool {
    matches!(
        value.as_str(),
        Some("plan" | "default" | "code" | "pair_programming" | "execute" | "custom")
    )
}

pub(super) fn message_phase(value: &Value) -> bool {
    matches!(value.as_str(), Some("commentary" | "final_answer"))
}

pub(super) fn i32_value(value: &Value) -> bool {
    value.as_i64().is_some_and(|n| i32::try_from(n).is_ok())
}

/// 对 Vec<u8> 的标准 Base64 持久化编码做结构校验，不读取或输出正文。
pub(super) fn base64_bytes(value: &Value) -> bool {
    let Some(text) = value.as_str() else {
        return false;
    };
    let bytes = text.as_bytes();
    if bytes.is_empty() {
        return true;
    }
    let padding = bytes.iter().rev().take_while(|&&b| b == b'=').count();
    if padding > 2 {
        return false;
    }
    if padding > 0 && !bytes.len().is_multiple_of(4) {
        return false;
    }
    let digits = &bytes[..bytes.len() - padding];
    let remainder = digits.len() % 4;
    if remainder == 1 {
        return false;
    }
    let sextet = |b: u8| -> Option<u8> {
        match b {
            b'A'..=b'Z' => Some(b - b'A'),
            b'a'..=b'z' => Some(b - b'a' + 26),
            b'0'..=b'9' => Some(b - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    };
    digits.iter().all(|&b| sextet(b).is_some())
        && match remainder {
            3 => sextet(digits[digits.len() - 1]).is_some_and(|v| v & 3 == 0),
            2 => sextet(digits[digits.len() - 1]).is_some_and(|v| v & 15 == 0),
            _ => true,
        }
}

pub(super) fn rate_limit_reached(value: &Value) -> bool {
    matches!(
        value.as_str(),
        Some(
            "rate_limit_reached"
                | "workspace_owner_credits_depleted"
                | "workspace_member_credits_depleted"
                | "workspace_owner_usage_limit_reached"
                | "workspace_member_usage_limit_reached"
        )
    )
}

/// CodexErrorInfo 的未知变体按上游自定义反序列化回退 Other；已知载荷仍严格校验。
pub(super) fn error_info(value: &Value) -> bool {
    let structured = |kind: &str| {
        matches!(
            kind,
            "http_connection_failed"
                | "response_stream_connection_failed"
                | "response_stream_disconnected"
                | "response_too_many_failed_attempts"
                | "active_turn_not_steerable"
        )
    };
    if let Some(kind) = value.as_str() {
        return !structured(kind);
    }
    let Some(fields) = value.as_object().filter(|m| m.len() == 1) else {
        return false;
    };
    let (kind, payload) = fields.iter().next().expect("单字段枚举");
    match kind.as_str() {
        "http_connection_failed"
        | "response_stream_connection_failed"
        | "response_stream_disconnected"
        | "response_too_many_failed_attempts" => {
            payload.is_object()
                && optional(payload, "http_status_code", |v| {
                    v.as_u64().is_some_and(|n| u16::try_from(n).is_ok())
                })
        }
        "active_turn_not_steerable" => matches!(
            payload.get("turn_kind").and_then(Value::as_str),
            Some("review" | "compact")
        ),
        "context_window_exceeded"
        | "session_budget_exceeded"
        | "usage_limit_exceeded"
        | "rate_limit_exceeded"
        | "flex_unavailable"
        | "server_overloaded"
        | "cyber_policy"
        | "bio_policy"
        | "misalignment_policy_violation"
        | "too_many_denials"
        | "internal_server_error"
        | "unauthorized"
        | "bad_request"
        | "invalid_prompt"
        | "sandbox_error"
        | "thread_rollback_failed"
        | "other" => payload.is_null(),
        _ => true,
    }
}

pub(super) fn error_event(value: &Value) -> bool {
    string(value, "message") && optional(value, "codex_error_info", error_info)
}

/// EventMsg 的显示/工具元数据仍有固定类型；内部 Value 字段继续原样视为不透明。
pub(super) fn event_options(kind: &str, value: &Value) -> bool {
    let texts = |fields: &[&str]| {
        fields
            .iter()
            .all(|key| optional(value, key, Value::is_string))
    };
    match kind {
        "agent_message" => {
            optional(value, "memory_citation", |citation| {
                array(citation, "rolloutIds", Value::is_string)
                    && array(citation, "entries", |entry| {
                        string(entry, "path")
                            && string(entry, "note")
                            && ["lineStart", "lineEnd"].iter().all(|key| {
                                entry
                                    .get(key)
                                    .and_then(Value::as_u64)
                                    .is_some_and(|n| u32::try_from(n).is_ok())
                            })
                    })
            }) && optional(value, "delivery", |v| v == "async")
                && optional(value, "questions", |v| {
                    v.as_array().is_some_and(|questions| {
                        questions.iter().all(|question| {
                            string(question, "title")
                                && optional(question, "options", |v| {
                                    v.as_array().is_some_and(|a| a.iter().all(Value::is_string))
                                })
                                && question.as_object().is_some_and(|m| {
                                    m.keys()
                                        .all(|key| matches!(key.as_str(), "title" | "options"))
                                })
                        })
                    })
                })
        }
        "exec_command_begin" | "exec_command_end" => {
            texts(&[
                "plugin_id",
                "script_path",
                "process_id",
                "interaction_input",
            ]) && (kind != "exec_command_end"
                || defaulted(value, "aggregated_output", Value::is_string))
        }
        "mcp_tool_call_begin" | "mcp_tool_call_end" => {
            defaulted(value, "turn_id", Value::is_string)
                && texts(&[
                    "connector_id",
                    "mcp_app_resource_uri",
                    "link_id",
                    "app_name",
                    "action_name",
                    "plugin_id",
                ])
                && optional(value, "read_only_hint", Value::is_boolean)
                && optional(value, "mcp_app_ui", |v| {
                    string(v, "resourceUri")
                        && matches!(
                            v["preferredModelDisplayMode"].as_str(),
                            Some("inline" | "fullscreen")
                        )
                })
        }
        _ => true,
    }
}
