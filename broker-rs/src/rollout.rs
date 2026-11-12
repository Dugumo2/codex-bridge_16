//! 会话文件（Codex 的 rollout jsonl）的有界只读跟随，只服务于没有 raw 事件的 v2 跟进配对。
//!
//! - 只读协议（thread/read）返回的那一个路径，不遍历目录；路径须是绝对路径、以 `.jsonl` 结尾、
//!   文件名含该线程 ID（[`validate_path`]）。
//! - 只读打开，每次读完即关闭句柄；Windows 上 std 默认共享读写删，不影响 Codex 追加写入。
//! - 首次只读末尾窗口，之后按字节偏移只读新增内容；只处理到最后一个换行符，半行留到下次。
//!   单次读取与单行都有上限，超长行整行跳过；窗口里没有轮次标记、或窗口起点落在轮次中间时向前多读，
//!   直到能确定当前轮，也有上限。到上限仍定位不到时，之后对不上轮次的消息标为「轮次丢失」交给状态机明确降级，
//!   不静默丢弃。
//! - 只挑出协作工具调用（`function_call` 的 followup_task／send_message）和收到的消息（`agent_message`），
//!   原样交给状态机按 raw 的同一套规则解析；其余行只用来跟踪当前轮。这里不保存、不记录任何正文。
//!
//! 每行的形状（上游 `RolloutLine`）：`{"timestamp":"YYYY-MM-DDTHH:MM:SS.mmmZ","type":…,"payload":…}`，
//! 时间戳是写入时刻的 UTC 毫秒；行所在的轮由它之前最近的 `event_msg`（`task_started`）或 `turn_context` 决定。
use serde_json::Value;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Component, Path};

mod validation;

/// 首次读取的末尾窗口。
pub const TAIL_BYTES: u64 = 256 * 1024;
/// 为确定窗口起点所在的轮向前多读的总上限（含首个窗口）。
pub const MAX_TAIL_BYTES: u64 = 4 * 1024 * 1024;
/// 跟随时单次读取的上限，也是单行上限：更长的行整行跳过。
pub const MAX_READ_BYTES: u64 = 4 * 1024 * 1024;

/// 读到哪里：`offset` 总在行首（跳过超长行时在行中间，`skipping` 为真）；`turn` 是 `offset` 处所在的轮。
/// `lost`：末尾窗口向前找到上限仍没找到轮次标记，`offset` 处所在的轮未知（不是「还没开始任何轮」）。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RolloutCursor {
    pub offset: u64,
    pub turn: Option<String>,
    pub skipping: bool,
    pub lost: bool,
}

/// 状态机交给 IO 层的一次读取。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RolloutReadRequest {
    pub id: u64,
    pub thread: String,
    pub conn_epoch: u64,
    pub observation_gen: u64,
    pub issued_seq: u64,
    pub path: String,
    /// None 表示首次读取（末尾窗口）。
    pub cursor: Option<RolloutCursor>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RolloutRecord {
    /// 协作工具调用；`item` 是 payload 原样（`type`、`name`、`call_id`、`arguments`）。
    Call { item: Value },
    /// 收到的消息与它所在的轮；轮次不明时为 None。
    Message { turn: Option<String>, item: Value },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RolloutEntry {
    pub record: RolloutRecord,
    /// 行的写入时间（UTC 毫秒）。
    pub wall_ms: u64,
    /// 消息的轮次为 None 是因为读取起点所在的轮定位不到（窗口没到文件头），而不是它前面确实没有任何轮。
    pub turn_lost: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RolloutChunk {
    pub entries: Vec<RolloutEntry>,
    pub cursor: RolloutCursor,
    /// 本次实际从文件读出的字节数。
    pub bytes_read: u64,
    /// 解析不了的行（不是 JSON、需要的字段缺失或类型不对）。
    pub bad_lines: u32,
    /// 超过单行上限而整行跳过的行。
    pub skipped_lines: u32,
    /// 读完后文件里还有没读的完整内容，应尽快再读。
    pub more: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RolloutError {
    /// 文件不存在（含 app-server 在别的机器上）。
    Missing,
    /// 打不开或读失败。
    Unreadable,
    /// 文件比上次读到的位置还短：被截断或替换，偏移不再可信。
    Shrunk,
}

/// 协议给出的路径是否可读：绝对路径、无 `..`、以 `.jsonl` 结尾、文件名含线程 ID。
pub fn validate_path(path: &str, thread: &str) -> bool {
    if thread.is_empty() || path.len() > 4096 {
        return false;
    }
    let p = Path::new(path);
    if !p.is_absolute() || p.components().any(|c| matches!(c, Component::ParentDir)) {
        return false;
    }
    let Some(name) = p.file_name().and_then(|n| n.to_str()) else {
        return false;
    };
    name.len() > ".jsonl".len()
        && name.to_ascii_lowercase().ends_with(".jsonl")
        && name.contains(thread)
}

/// 解析 `YYYY-MM-DDTHH:MM:SS(.fff)Z` 为 UTC 毫秒；其他形式返回 None。
pub fn parse_timestamp(text: &str) -> Option<u64> {
    let b = text.as_bytes();
    if b.len() < 20 || b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' {
        return None;
    }
    if b[16] != b':' || *b.last()? != b'Z' {
        return None;
    }
    let num = |s: &str| -> Option<u64> {
        (!s.is_empty() && s.bytes().all(|c| c.is_ascii_digit()))
            .then(|| s.parse().ok())
            .flatten()
    };
    let year = num(text.get(0..4)?)? as i64;
    let month = num(text.get(5..7)?)?;
    let day = num(text.get(8..10)?)?;
    let hour = num(text.get(11..13)?)?;
    let minute = num(text.get(14..16)?)?;
    let second = num(text.get(17..19)?)?;
    let rest = text.get(19..text.len() - 1)?;
    let millis = if rest.is_empty() {
        0
    } else {
        let frac = rest.strip_prefix('.')?;
        let digits = num(frac)?;
        match frac.len() {
            1 => digits * 100,
            2 => digits * 10,
            3 => digits,
            n if n > 3 && n <= 9 => digits / 10u64.pow(n as u32 - 3),
            _ => return None,
        }
    };
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) || hour > 23 || minute > 59 {
        return None;
    }
    if second > 60 {
        return None;
    }
    // 公历日期换算到 1970-01-01 起的天数（Howard Hinnant 的 days_from_civil）。
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let m = month as i64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + day as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    if days < 0 {
        return None;
    }
    Some(((days as u64 * 24 + hour) * 60 + minute) * 60_000 + second * 1000 + millis)
}

fn is_collab_call(payload: &Value) -> bool {
    payload.get("type").and_then(Value::as_str) == Some("function_call")
        && payload
            .get("name")
            .and_then(Value::as_str)
            .and_then(|n| n.rsplit('.').next())
            .is_some_and(|n| matches!(n, "followup_task" | "send_message"))
}

/// 只接受当前协议能解释的消息片段；未知片段不能借外层已知类型被吞掉。
fn message_content_valid(payload: &Value, agent: bool) -> bool {
    payload
        .get("content")
        .and_then(Value::as_array)
        .is_some_and(|parts| {
            parts
                .iter()
                .all(|part| match part.get("type").and_then(Value::as_str) {
                    Some("input_text") => part.get("text").is_some_and(Value::is_string),
                    Some("encrypted_content") if agent => {
                        part.get("encrypted_content").is_some_and(Value::is_string)
                    }
                    Some("output_text") if !agent => part.get("text").is_some_and(Value::is_string),
                    Some("input_audio") if !agent => {
                        part.get("audio_url").is_some_and(Value::is_string)
                    }
                    Some("input_image") if !agent => validation::image(part),
                    _ => false,
                })
        })
}

/// 文件事件与公开通知字段不同，按 0.160.0 持久化 EventMsg 的最小结构检查。
fn irrelevant_event_valid(kind: &str, payload: &Value) -> bool {
    if !validation::event_options(kind, payload) {
        return false;
    }
    let string = |field: &str| payload.get(field).is_some_and(Value::is_string);
    match kind {
        "context_compacted" => true,
        "token_count" => {
            validation::optional(payload, "info", validation::usage_info)
                && validation::optional(payload, "rate_limits", validation::rate_limits)
        }
        "agent_message" => {
            string("message") && validation::optional(payload, "phase", validation::message_phase)
        }
        "warning" => string("message"),
        "error" => validation::error_event(payload),
        "agent_reasoning" | "agent_reasoning_raw_content" => string("text"),
        "agent_message_content_delta" | "plan_delta" => {
            string("thread_id") && string("turn_id") && string("item_id") && string("delta")
        }
        "reasoning_content_delta" | "reasoning_raw_content_delta" => {
            string("thread_id")
                && string("turn_id")
                && string("item_id")
                && string("delta")
                && validation::defaulted(
                    payload,
                    if kind == "reasoning_content_delta" {
                        "summary_index"
                    } else {
                        "content_index"
                    },
                    Value::is_i64,
                )
        }
        "exec_command_begin" | "exec_command_end" => {
            let command = payload
                .get("command")
                .and_then(Value::as_array)
                .is_some_and(|a| a.iter().all(Value::is_string));
            string("call_id")
                && string("turn_id")
                && command
                && string("cwd")
                && payload
                    .get("parsed_cmd")
                    .is_some_and(validation::parsed_commands)
                && validation::defaulted(payload, "source", validation::command_source)
                && validation::defaulted(
                    payload,
                    if kind == "exec_command_begin" {
                        "started_at_ms"
                    } else {
                        "completed_at_ms"
                    },
                    Value::is_i64,
                )
                && (kind == "exec_command_begin"
                    || (string("stdout")
                        && string("stderr")
                        && payload.get("exit_code").is_some_and(validation::i32_value)
                        && string("formatted_output")
                        && payload
                            .get("status")
                            .is_some_and(validation::terminal_status)
                        && payload.get("duration").is_some_and(validation::duration)))
        }
        "exec_command_output_delta" => {
            string("call_id")
                && payload.get("chunk").is_some_and(validation::base64_bytes)
                && matches!(
                    payload.get("stream").and_then(Value::as_str),
                    Some("stdout" | "stderr")
                )
        }
        "patch_apply_begin" => {
            string("call_id")
                && validation::defaulted(payload, "turn_id", Value::is_string)
                && payload.get("changes").is_some_and(validation::changes)
                && payload.get("auto_approved").is_some_and(Value::is_boolean)
        }
        "patch_apply_end" => {
            string("call_id")
                && string("stdout")
                && string("stderr")
                && payload
                    .get("status")
                    .is_some_and(validation::terminal_status)
                && validation::defaulted(payload, "turn_id", Value::is_string)
                && payload.get("success").is_some_and(Value::is_boolean)
                && validation::defaulted(payload, "changes", validation::changes)
        }
        "mcp_tool_call_begin" | "mcp_tool_call_end" => {
            let invocation = payload.get("invocation").is_some_and(|v| {
                v.get("server").is_some_and(Value::is_string)
                    && v.get("tool").is_some_and(Value::is_string)
            });
            string("call_id")
                && invocation
                && (kind == "mcp_tool_call_begin"
                    || (payload.get("result").is_some_and(validation::mcp_result)
                        && payload.get("duration").is_some_and(validation::duration)))
        }
        "web_search_begin" => string("call_id"),
        "web_search_end" => {
            string("call_id")
                && string("query")
                && payload.get("action").is_some_and(validation::web_action)
                && validation::optional(payload, "results", Value::is_array)
        }
        "view_image_tool_call" => string("call_id") && string("path"),
        "plan_update" => {
            payload.get("plan").is_some_and(validation::plan)
                && validation::optional(payload, "explanation", Value::is_string)
                && payload.as_object().is_some_and(|m| {
                    m.keys()
                        .all(|key| matches!(key.as_str(), "type" | "plan" | "explanation"))
                })
        }
        _ => false,
    }
}

/// 一行完整内容（不含换行符）。只有能解释为无关的记录返回 Ok(None)；Err 表示无法解释。
/// `lost`：读取起点所在的轮未知；遇到轮次标记后清除。
fn parse_line(
    line: &[u8],
    turn: &mut Option<String>,
    lost: &mut bool,
) -> Result<Option<RolloutEntry>, ()> {
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    if line.iter().all(u8::is_ascii_whitespace) {
        return Ok(None);
    }
    let value: Value = serde_json::from_slice(line).map_err(|_| ())?;
    let kind = value.get("type").and_then(Value::as_str).ok_or(())?;
    let payload = value.get("payload").filter(|p| p.is_object()).ok_or(())?;
    match kind {
        "event_msg" => {
            match payload.get("type").and_then(Value::as_str).ok_or(())? {
                "task_started" | "turn_started" => {
                    if payload
                        .get("root_turn_id")
                        .is_some_and(|v| !v.is_null() && !v.is_string())
                        || payload
                            .get("model_context_window")
                            .is_some_and(|v| !v.is_null() && !v.is_i64())
                    {
                        return Err(());
                    }
                    if !validation::defaulted(
                        payload,
                        "collaboration_mode_kind",
                        validation::mode_kind,
                    ) || !validation::optional(payload, "started_at", Value::is_i64)
                        || !validation::optional(payload, "trace_id", Value::is_string)
                    {
                        return Err(());
                    }
                    let id = payload
                        .get("turn_id")
                        .and_then(Value::as_str)
                        .filter(|s| !s.is_empty())
                        .ok_or(())?;
                    *turn = Some(id.to_owned());
                    *lost = false;
                }
                "task_complete" | "turn_complete" | "turn_aborted" => {
                    let aborted = payload["type"] == "turn_aborted";
                    if !validation::optional(payload, "error", validation::error_event)
                        || (!aborted
                            && !validation::optional(
                                payload,
                                "last_agent_message",
                                Value::is_string,
                            ))
                        || (aborted && !payload.get("reason").is_some_and(validation::abort_reason))
                    {
                        return Err(());
                    }
                    for field in ["started_at", "completed_at", "duration_ms"] {
                        if !validation::optional(payload, field, Value::is_i64) {
                            return Err(());
                        }
                    }
                    if !aborted
                        && !validation::optional(payload, "time_to_first_token_ms", Value::is_i64)
                    {
                        return Err(());
                    }
                    payload
                        .get("turn_id")
                        .and_then(Value::as_str)
                        .filter(|s| !s.is_empty())
                        .ok_or(())?;
                    *turn = None;
                    *lost = false;
                }
                known if irrelevant_event_valid(known, payload) => {}
                _ => return Err(()),
            }
            Ok(None)
        }
        "turn_context" => {
            if payload.get("cwd").is_some_and(|v| !v.is_string()) {
                return Err(());
            }
            let id = payload
                .get("turn_id")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .ok_or(())?;
            *turn = Some(id.to_owned());
            *lost = false;
            Ok(None)
        }
        "response_item" => {
            let kind = payload.get("type").and_then(Value::as_str).ok_or(())?;
            if !crate::relations::coverage_observation::validation::response_optional_fields(
                payload,
            ) {
                return Err(());
            }
            let message = match kind {
                "agent_message" => {
                    for field in ["author", "recipient"] {
                        payload
                            .get(field)
                            .and_then(Value::as_str)
                            .filter(|s| !s.is_empty())
                            .ok_or(())?;
                    }
                    if !message_content_valid(payload, true) {
                        return Err(());
                    }
                    true
                }
                "message" => {
                    let role = payload.get("role").and_then(Value::as_str).ok_or(())?;
                    if !message_content_valid(payload, false) {
                        return Err(());
                    }
                    match role {
                        "user" | "developer" | "system" => true,
                        "assistant" => return Ok(None),
                        _ => return Err(()),
                    }
                }
                "function_call" => {
                    for field in ["name", "call_id", "arguments"] {
                        payload.get(field).and_then(Value::as_str).ok_or(())?;
                    }
                    if !validation::optional(payload, "encrypted_function_args", |v| {
                        v.as_array().is_some_and(|a| a.iter().all(Value::is_string))
                    }) {
                        return Err(());
                    }
                    if !is_collab_call(payload) {
                        return Ok(None);
                    }
                    false
                }
                "reasoning" => {
                    if !validation::reasoning(payload) {
                        return Err(());
                    }
                    return Ok(None);
                }
                "function_call_output" | "custom_tool_call_output" => {
                    if kind == "custom_tool_call_output" {
                        payload.get("call_id").and_then(Value::as_str).ok_or(())?;
                    } else if payload
                        .get("call_id")
                        .is_some_and(|v| !v.is_null() && !v.is_string())
                    {
                        return Err(());
                    }
                    let output = payload.get("output").ok_or(())?;
                    if !validation::tool_output(output) {
                        return Err(());
                    }
                    return Ok(None);
                }
                "custom_tool_call" => {
                    for field in ["name", "call_id", "input"] {
                        payload.get(field).and_then(Value::as_str).ok_or(())?;
                    }
                    if !validation::optional(payload, "status", Value::is_string) {
                        return Err(());
                    }
                    return Ok(None);
                }
                "local_shell_call" => {
                    if !matches!(
                        payload["status"].as_str(),
                        Some("completed" | "in_progress" | "incomplete")
                    ) || !payload.get("action").is_some_and(validation::shell_action)
                    {
                        return Err(());
                    }
                    return Ok(None);
                }
                "image_generation_call" => {
                    payload.get("status").and_then(Value::as_str).ok_or(())?;
                    payload.get("result").and_then(Value::as_str).ok_or(())?;
                    return Ok(None);
                }
                "web_search_call" => {
                    if payload
                        .get("status")
                        .is_some_and(|v| !v.is_null() && !v.is_string())
                        || !validation::optional(payload, "action", validation::web_action)
                    {
                        return Err(());
                    }
                    return Ok(None);
                }
                "tool_search_call" => {
                    payload.get("execution").and_then(Value::as_str).ok_or(())?;
                    payload.get("arguments").ok_or(())?;
                    if !validation::optional(payload, "status", Value::is_string) {
                        return Err(());
                    }
                    return Ok(None);
                }
                "tool_search_output" => {
                    for field in ["execution", "status"] {
                        payload.get(field).and_then(Value::as_str).ok_or(())?;
                    }
                    payload.get("tools").and_then(Value::as_array).ok_or(())?;
                    return Ok(None);
                }
                "additional_tools" => {
                    payload.get("role").and_then(Value::as_str).ok_or(())?;
                    payload.get("tools").and_then(Value::as_array).ok_or(())?;
                    return Ok(None);
                }
                "configuration_update" => {
                    if !payload
                        .get("reasoning")
                        .is_some_and(|v| v.get("effort").is_some_and(Value::is_string))
                    {
                        return Err(());
                    }
                    return Ok(None);
                }
                "compaction_trigger" => return Ok(None),
                "context_compaction" => {
                    if payload
                        .get("encrypted_content")
                        .is_some_and(|v| !v.is_null() && !v.is_string())
                    {
                        return Err(());
                    }
                    return Ok(None);
                }
                "compaction" | "compaction_summary" => {
                    payload
                        .get("encrypted_content")
                        .and_then(Value::as_str)
                        .ok_or(())?;
                    return Ok(None);
                }
                _ => return Err(()),
            };
            let wall_ms = value
                .get("timestamp")
                .and_then(Value::as_str)
                .and_then(parse_timestamp)
                .ok_or(())?;
            let record = if message {
                RolloutRecord::Message {
                    turn: turn.clone(),
                    item: payload.clone(),
                }
            } else {
                RolloutRecord::Call {
                    item: payload.clone(),
                }
            };
            Ok(Some(RolloutEntry {
                record,
                wall_ms,
                turn_lost: message && turn.is_none() && *lost,
            }))
        }
        "session_meta" => {
            payload
                .get("id")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .ok_or(())?;
            for field in ["timestamp", "cwd", "originator", "cli_version"] {
                if payload.get(field).is_some_and(|v| !v.is_string()) {
                    return Err(());
                }
            }
            Ok(None)
        }
        "compacted" => {
            payload.get("message").and_then(Value::as_str).ok_or(())?;
            if !validation::optional(payload, "replacement_history", replacement_history_valid) {
                return Err(());
            }
            Ok(None)
        }
        "inter_agent_communication_metadata" => {
            payload
                .get("trigger_turn")
                .and_then(Value::as_bool)
                .ok_or(())?;
            Ok(None)
        }
        "world_state" => {
            payload.get("full").and_then(Value::as_bool).ok_or(())?;
            payload.get("state").and_then(Value::as_object).ok_or(())?;
            Ok(None)
        }
        "security_risk_score" => {
            let scores = payload.get("scores").and_then(Value::as_object).ok_or(())?;
            if !scores.values().all(Value::is_number) {
                return Err(());
            }
            Ok(None)
        }
        "token_usage_record" => {
            for field in [
                "thread_id",
                "turn_id",
                "session_id",
                "root_turn_id",
                "response_id",
            ] {
                payload.get(field).and_then(Value::as_str).ok_or(())?;
            }
            for field in ["usage", "turn_token_usage", "thread_token_usage"] {
                if !payload.get(field).is_some_and(validation::usage) {
                    return Err(());
                }
            }
            Ok(None)
        }
        _ => Err(()),
    }
}

/// 压缩替换历史是结构化 ResponseItem 列表；校验其形状但不把历史消息登记为当前消费。
fn replacement_history_valid(value: &Value) -> bool {
    value.as_array().is_some_and(|items| {
        items.iter().all(|item| {
            let line = serde_json::json!({"type":"response_item", "payload":item,
            "timestamp":"1970-01-01T00:00:00.000Z"});
            let bytes = serde_json::to_vec(&line).expect("JSON可序列化");
            parse_line(&bytes, &mut None, &mut false).is_ok()
        })
    })
}

/// 只处理到最后一个换行符：返回解析出的条目与已消费的字节数（含最后的换行符）。
fn parse_lines(
    buf: &[u8],
    turn: &mut Option<String>,
    lost: &mut bool,
    chunk: &mut RolloutChunk,
) -> usize {
    let Some(last) = buf.iter().rposition(|&b| b == b'\n') else {
        return 0;
    };
    for line in buf[..last].split(|&b| b == b'\n') {
        match parse_line(line, turn, lost) {
            Ok(Some(entry)) => chunk.entries.push(entry),
            Ok(None) => {}
            Err(()) => {
                chunk.bad_lines = chunk.bad_lines.saturating_add(1);
                *turn = None;
                *lost = true;
            }
        }
    }
    last + 1
}

fn read_range(file: &mut std::fs::File, start: u64, len: u64) -> Result<Vec<u8>, RolloutError> {
    file.seek(SeekFrom::Start(start))
        .map_err(|_| RolloutError::Unreadable)?;
    let mut buf = Vec::with_capacity(len as usize);
    file.take(len)
        .read_to_end(&mut buf)
        .map_err(|_| RolloutError::Unreadable)?;
    Ok(buf)
}

/// 首次读取：窗口里没有轮次标记、或第一条消息之前没有轮次标记，而窗口又没到文件头时，加倍向前多读，
/// 总量有上限。只有工具输出、没有消息的窗口也要找：之后吸收跟进的消息要靠它带上轮次。
/// 到上限仍找不到：游标标为 `lost`，消息标为 `turn_lost`，由状态机明确降级。
fn read_tail(file: &mut std::fs::File, len: u64) -> Result<RolloutChunk, RolloutError> {
    let mut window = TAIL_BYTES.min(len);
    let mut bytes_read = 0u64;
    loop {
        let start = len - window;
        // 从前一个字节读起，便于判断窗口起点是否恰在行首。
        let from = start.saturating_sub(1);
        let buf = read_range(file, from, len - from)?;
        bytes_read += buf.len() as u64;
        let skip = if start == 0 {
            0
        } else {
            match buf.iter().position(|&b| b == b'\n') {
                Some(i) => i + 1,
                None => buf.len(),
            }
        };
        let body = &buf[skip..];
        let mut turn = None;
        let mut lost = start > 0;
        let mut chunk = RolloutChunk::default();
        let consumed = parse_lines(body, &mut turn, &mut lost, &mut chunk);
        let unanchored = turn.is_none()
            || chunk
                .entries
                .iter()
                .any(|e| matches!(&e.record, RolloutRecord::Message { turn: None, .. }));
        if unanchored && start > 0 && window < MAX_TAIL_BYTES {
            window = (window * 2).min(MAX_TAIL_BYTES).min(len);
            continue;
        }
        let offset = from + skip as u64 + consumed as u64;
        // 末尾不完整的半行留到下次读。
        chunk.cursor = RolloutCursor {
            offset,
            turn,
            skipping: false,
            lost,
        };
        chunk.bytes_read = bytes_read;
        return Ok(chunk);
    }
}

fn read_follow(
    file: &mut std::fs::File,
    len: u64,
    cursor: &RolloutCursor,
) -> Result<RolloutChunk, RolloutError> {
    if len < cursor.offset {
        return Err(RolloutError::Shrunk);
    }
    let want = (len - cursor.offset).min(MAX_READ_BYTES);
    let mut chunk = RolloutChunk {
        cursor: cursor.clone(),
        ..Default::default()
    };
    if want == 0 {
        return Ok(chunk);
    }
    let buf = read_range(file, cursor.offset, want)?;
    chunk.bytes_read = buf.len() as u64;
    let mut pos = 0usize;
    let mut turn = cursor.turn.clone();
    let mut lost = cursor.lost;
    if cursor.skipping {
        match buf.iter().position(|&b| b == b'\n') {
            Some(i) => {
                pos = i + 1;
                chunk.skipped_lines += 1;
                chunk.cursor.skipping = false;
                turn = None;
                lost = true;
            }
            None => {
                chunk.cursor.offset += buf.len() as u64;
                chunk.more = chunk.cursor.offset < len;
                return Ok(chunk);
            }
        }
    }
    let consumed = parse_lines(&buf[pos..], &mut turn, &mut lost, &mut chunk);
    if consumed == 0 && pos == 0 && buf.len() as u64 >= MAX_READ_BYTES {
        // 一整次读取都没有换行：这一行超过上限，跳过到下一个换行符。
        chunk.cursor.offset += buf.len() as u64;
        chunk.cursor.skipping = true;
        turn = None;
        lost = true;
    } else {
        chunk.cursor.offset += (pos + consumed) as u64;
    }
    chunk.cursor.turn = turn;
    chunk.cursor.lost = lost;
    // 还有完整的行没读到（本次读满了上限）时请调用方尽快再读；只剩半行时等下次轮询。
    chunk.more = buf.len() as u64 == want && want == MAX_READ_BYTES && chunk.cursor.offset < len;
    Ok(chunk)
}

/// 读一次：首次读末尾窗口，之后从游标读新增内容。只读打开，返回前关闭句柄。
pub fn read_chunk(
    path: &str,
    cursor: Option<&RolloutCursor>,
) -> Result<RolloutChunk, RolloutError> {
    let mut file = match std::fs::OpenOptions::new().read(true).open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Err(RolloutError::Missing),
        Err(_) => return Err(RolloutError::Unreadable),
    };
    let meta = file.metadata().map_err(|_| RolloutError::Unreadable)?;
    if !meta.is_file() {
        return Err(RolloutError::Unreadable);
    }
    let len = meta.len();
    match cursor {
        None => read_tail(&mut file, len),
        Some(cursor) => read_follow(&mut file, len, cursor),
    }
}

#[cfg(test)]
mod tests;
