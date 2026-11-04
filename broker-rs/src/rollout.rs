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

/// 一行完整内容（不含换行符）。返回 Ok(None) 表示与配对无关的行；Err 表示坏行。
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
    let payload = value.get("payload");
    match kind {
        "event_msg" => {
            let payload = payload.ok_or(())?;
            if matches!(
                payload.get("type").and_then(Value::as_str),
                Some("task_started" | "turn_started")
            ) {
                let id = payload.get("turn_id").and_then(Value::as_str).ok_or(())?;
                *turn = Some(id.to_owned());
                *lost = false;
            }
            Ok(None)
        }
        "turn_context" => {
            if let Some(id) = payload
                .and_then(|p| p.get("turn_id"))
                .and_then(Value::as_str)
            {
                *turn = Some(id.to_owned());
                *lost = false;
            }
            Ok(None)
        }
        "response_item" => {
            let payload = payload.ok_or(())?;
            let message = payload.get("type").and_then(Value::as_str) == Some("agent_message");
            if !message && !is_collab_call(payload) {
                return Ok(None);
            }
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
        _ => Ok(None),
    }
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
            Err(()) => chunk.bad_lines = chunk.bad_lines.saturating_add(1),
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
