//! Codex 0.160.0 已接纳条目的有限结构检查；不透明 JSON 与未知附加字段保持开放。
use serde_json::Value;

fn text(v: &Value) -> bool {
    v.is_string()
}
fn nonempty_text(v: &Value) -> bool {
    v.as_str().is_some_and(|s| !s.is_empty())
}
fn u32_value(v: &Value) -> bool {
    v.as_u64().is_some_and(|n| u32::try_from(n).is_ok())
}
fn i32_value(v: &Value) -> bool {
    v.as_i64().is_some_and(|n| i32::try_from(n).is_ok())
}
fn one_of(v: &Value, choices: &[&str]) -> bool {
    v.as_str().is_some_and(|s| choices.contains(&s))
}
fn array(v: &Value, check: fn(&Value) -> bool) -> bool {
    v.as_array().is_some_and(|items| items.iter().all(check))
}
fn strings(v: &Value) -> bool {
    array(v, text)
}
fn required(v: &Value, key: &str, check: fn(&Value) -> bool) -> bool {
    v.get(key).is_some_and(check)
}
fn optional(v: &Value, key: &str, check: fn(&Value) -> bool) -> bool {
    v.get(key)
        .is_none_or(|value| value.is_null() || check(value))
}
fn optional_texts(v: &Value, fields: &[&str]) -> bool {
    fields.iter().all(|key| optional(v, key, text))
}
fn phase(v: &Value) -> bool {
    one_of(v, &["commentary", "final_answer"])
}
fn image_detail(v: &Value) -> bool {
    one_of(v, &["auto", "low", "high", "original"])
}

/// 主内容校验仍由原解析器负责；这里只验证已声明的图像可选参数，避免重复覆盖主分支。
fn content_options(v: &Value) -> bool {
    v.get("type").and_then(Value::as_str) != Some("input_image")
        || optional(v, "detail", image_detail)
}
fn array_options(v: Option<&Value>) -> bool {
    v.and_then(Value::as_array)
        .is_none_or(|items| items.iter().all(content_options))
}
fn metadata(v: &Value) -> bool {
    // create_time、content_item_kinds 和宿主调用清单不在公开 schema 中，不解释这些额外字段。
    v.is_object() && optional(v, "turn_id", text)
}
fn string_map(v: &Value) -> bool {
    v.as_object().is_some_and(|map| map.values().all(text))
}
fn shell_options(v: &Value) -> bool {
    optional(v, "timeout_ms", Value::is_u64)
        && optional_texts(v, &["working_directory", "user"])
        && optional(v, "env", string_map)
}

/// raw 与文件共同校验已声明 Option 字段；缺失/null 合法，主类型及必要字段不在此重复检查。
pub(crate) fn response_optional_fields(item: &Value) -> bool {
    let Some(kind) = item.get("type").and_then(Value::as_str) else {
        return false;
    };
    if !matches!(kind, "configuration_update" | "compaction_trigger") && !optional(item, "id", text)
    {
        return false;
    }
    if !matches!(
        kind,
        "additional_tools" | "configuration_update" | "compaction_trigger"
    ) && !optional(item, "internal_chat_message_metadata_passthrough", metadata)
    {
        return false;
    }
    match kind {
        "message" => optional(item, "phase", phase) && array_options(item.get("content")),
        "function_call" => {
            optional(item, "namespace", text) && optional(item, "encrypted_function_args", strings)
        }
        "function_call_output" => {
            optional_texts(item, &["call_id", "name", "namespace"])
                && array_options(item.get("output"))
        }
        "custom_tool_call" => optional_texts(item, &["status", "namespace"]),
        "custom_tool_call_output" => {
            optional(item, "name", text) && array_options(item.get("output"))
        }
        "local_shell_call" => {
            optional(item, "call_id", text) && item.get("action").is_none_or(shell_options)
        }
        "image_generation_call" => optional(item, "revised_prompt", text),
        "tool_search_call" => optional_texts(item, &["call_id", "status"]),
        "tool_search_output" => optional(item, "call_id", text),
        "web_search_call" => optional(item, "status", text),
        "agent_message"
        | "reasoning"
        | "compaction"
        | "compaction_summary"
        | "context_compaction"
        | "additional_tools"
        | "configuration_update"
        | "compaction_trigger" => true,
        _ => false,
    }
}

fn command_action(v: &Value) -> bool {
    if !required(v, "command", text) {
        return false;
    }
    match v.get("type").and_then(Value::as_str) {
        Some("read") => required(v, "name", text) && required(v, "path", text),
        Some("listFiles") => optional(v, "path", text),
        Some("search") => optional_texts(v, &["query", "path"]),
        Some("unknown") => true,
        _ => false,
    }
}
pub(super) fn command_actions(v: &Value) -> bool {
    array(v, command_action)
}
fn patch_kind(v: &Value) -> bool {
    match v.get("type").and_then(Value::as_str) {
        Some("add" | "delete") => true,
        // 该 enum 只重命名变体；字段使用实际 schema 的 move_path。
        Some("update") => optional(v, "move_path", text),
        _ => false,
    }
}
fn file_update(v: &Value) -> bool {
    required(v, "path", text) && required(v, "diff", text) && required(v, "kind", patch_kind)
}
pub(super) fn file_changes(v: &Value) -> bool {
    array(v, file_update)
}
fn agent_state(v: &Value) -> bool {
    v.get("status").is_some_and(|status| {
        one_of(
            status,
            &[
                "pendingInit",
                "running",
                "interrupted",
                "completed",
                "errored",
                "shutdown",
                "notFound",
            ],
        )
    }) && optional(v, "message", text)
}
pub(super) fn agent_states(v: &Value) -> bool {
    v.as_object()
        .is_some_and(|map| map.values().all(agent_state))
}

fn citation_entry(v: &Value) -> bool {
    required(v, "path", text)
        && required(v, "note", text)
        && required(v, "lineStart", u32_value)
        && required(v, "lineEnd", u32_value)
}
fn citation_entries(v: &Value) -> bool {
    array(v, citation_entry)
}
fn memory_citation(v: &Value) -> bool {
    required(v, "entries", citation_entries) && required(v, "threadIds", strings)
}
fn question(v: &Value) -> bool {
    // AsyncUserInputQuestion 是明确声明 deny_unknown_fields 的唯一此类对象。
    required(v, "title", text)
        && optional(v, "options", strings)
        && v.as_object().is_some_and(|map| {
            map.keys()
                .all(|key| matches!(key.as_str(), "title" | "options"))
        })
}
fn questions(v: &Value) -> bool {
    array(v, question)
}
fn delivery(v: &Value) -> bool {
    one_of(v, &["async"])
}
fn command_source(v: &Value) -> bool {
    one_of(
        v,
        &[
            "agent",
            "userShell",
            "unifiedExecStartup",
            "unifiedExecInteraction",
        ],
    )
}
fn mcp_context(v: &Value) -> bool {
    required(v, "connectorId", text)
        && optional_texts(v, &["linkId", "resourceUri", "appName", "actionName"])
}
fn mcp_ui(v: &Value) -> bool {
    required(v, "resourceUri", text)
        && v.get("preferredModelDisplayMode")
            .is_some_and(|mode| one_of(mode, &["inline", "fullscreen"]))
}
fn mcp_result(v: &Value) -> bool {
    // 内容元素、structuredContent 与 _meta 是显式 Value，不能递归猜测其语义。
    required(v, "content", Value::is_array)
}
fn mcp_error(v: &Value) -> bool {
    required(v, "message", text)
}
fn dynamic_content(v: &Value) -> bool {
    match v.get("type").and_then(Value::as_str) {
        Some("inputText") => required(v, "text", text),
        Some("inputImage") => required(v, "imageUrl", text),
        Some("inputAudio") => required(v, "audioUrl", text),
        _ => false,
    }
}
fn dynamic_contents(v: &Value) -> bool {
    array(v, dynamic_content)
}
fn image_failure(v: &Value) -> bool {
    v.get("type").and_then(Value::as_str) == Some("usageLimitExceeded")
        && required(v, "limitId", text)
        && optional(v, "resetsAt", Value::is_i64)
}

/// 公开 ThreadItem 的已声明可选字段与 default 枚举；基本必需字段仍由调用方按类型校验。
pub(super) fn public_optional_fields(item: &Value) -> bool {
    match item.get("type").and_then(Value::as_str) {
        Some("agentMessage") => {
            optional(item, "phase", phase)
                && optional(item, "memoryCitation", memory_citation)
                && optional(item, "delivery", delivery)
                && optional(item, "questions", questions)
        }
        Some("functionCallOutput") => {
            optional(item, "namespace", text) && array_options(item.get("output"))
        }
        Some("commandExecution") => {
            optional_texts(
                item,
                &["pluginId", "scriptPath", "processId", "aggregatedOutput"],
            ) && optional(item, "exitCode", i32_value)
                && optional(item, "durationMs", Value::is_i64)
                && item.get("source").is_none_or(command_source)
        }
        Some("mcpToolCall") => {
            optional(item, "appContext", mcp_context)
                && optional(item, "mcpAppUi", mcp_ui)
                && optional_texts(item, &["mcpAppResourceUri", "pluginId"])
                && optional(item, "readOnlyHint", Value::is_boolean)
                && optional(item, "result", mcp_result)
                && optional(item, "error", mcp_error)
                && optional(item, "durationMs", Value::is_i64)
        }
        Some("dynamicToolCall") => {
            optional(item, "namespace", text)
                && optional(item, "contentItems", dynamic_contents)
                && optional(item, "success", Value::is_boolean)
                && optional(item, "durationMs", Value::is_i64)
        }
        Some("collabAgentToolCall") => {
            optional_texts(item, &["prompt", "model"])
                && optional(item, "reasoningEffort", nonempty_text)
        }
        Some("imageGeneration") => {
            optional_texts(item, &["revisedPrompt", "savedPath"])
                && optional(item, "transparentBackground", Value::is_boolean)
                && optional(item, "failure", image_failure)
        }
        Some(
            "reasoning" | "plan" | "fileChange" | "subAgentActivity" | "imageView"
            | "enteredReviewMode" | "exitedReviewMode" | "contextCompaction" | "webSearch"
            | "sleep",
        ) => true,
        _ => false,
    }
}
