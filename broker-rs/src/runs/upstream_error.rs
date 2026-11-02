//! 上游诊断的分类、脱敏、分组与合并；不参与运行状态或协调判定。

use super::{Phase, RunState};
use crate::claim::{Action, Machine};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const MAX_ERROR_BYTES: usize = 4096;
const REDACTED: &str = "[已隐藏]";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum UpstreamErrorSource {
    ErrorNotification,
    TurnCompleted,
    RpcResponse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum UpstreamErrorCategory {
    Authentication,
    PaymentRequired,
    PermissionDenied,
    UsageLimit,
    RateLimit,
    Server,
    Network,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpstreamError {
    pub thread_id: String,
    pub turn_id: Option<String>,
    pub source: UpstreamErrorSource,
    pub category: UpstreamErrorCategory,
    pub http_status_code: Option<u16>,
    pub codex_error_info: Option<Value>,
    pub message: String,
    pub additional_details: Option<String>,
    pub truncated: bool,
    pub will_retry: Option<bool>,
    pub first_seen_at: u64,
    pub last_seen_at: u64,
    pub count: u32,
}

pub fn variant_name(info: Option<&Value>) -> &str {
    match info {
        Some(Value::String(name)) => name,
        Some(Value::Object(fields)) if fields.len() == 1 => {
            fields.keys().next().map(String::as_str).unwrap_or_default()
        }
        _ => "",
    }
}

pub fn http_status_code(info: Option<&Value>) -> Option<u16> {
    let fields = info?.as_object()?;
    if fields.len() != 1 {
        return None;
    }
    fields
        .values()
        .next()?
        .get("httpStatusCode")?
        .as_u64()
        .and_then(|status| u16::try_from(status).ok())
}

pub fn classify(info: Option<&Value>) -> UpstreamErrorCategory {
    use UpstreamErrorCategory::*;
    match variant_name(info) {
        "unauthorized" => return Authentication,
        "usageLimitExceeded" | "sessionBudgetExceeded" => return UsageLimit,
        "rateLimitExceeded" => return RateLimit,
        "serverOverloaded" | "internalServerError" => return Server,
        _ => {}
    }
    match http_status_code(info) {
        Some(401) => Authentication,
        Some(402) => PaymentRequired,
        Some(403) => PermissionDenied,
        Some(429) => RateLimit,
        Some(500..=599) => Server,
        None if matches!(
            variant_name(info),
            "httpConnectionFailed" | "responseStreamConnectionFailed"
        ) && info
            .and_then(Value::as_object)
            .and_then(|fields| fields.values().next())
            .and_then(|fields| fields.get("httpStatusCode"))
            .is_some_and(Value::is_null) =>
        {
            Network
        }
        _ => Unknown,
    }
}

/// 与 CLI 共用的规则：只消去整段重连提示，不能抹掉正文中的其他分数。
pub fn normalized_body(error: &UpstreamError) -> &str {
    let body = error
        .additional_details
        .as_deref()
        .filter(|details| !trim_body(details).is_empty())
        .map_or_else(|| trim_body(&error.message), trim_body);
    if body
        .strip_prefix("Reconnecting... ")
        .and_then(|suffix| suffix.split_once('/'))
        .is_some_and(|(left, right)| {
            !left.is_empty()
                && !right.is_empty()
                && left.bytes().all(|c| c.is_ascii_digit())
                && right.bytes().all(|c| c.is_ascii_digit())
        })
    {
        ""
    } else {
        body
    }
}

/// 与 JavaScript String.trim 一致，避免 BOM 与 U+0085 导致两端分组不同。
fn trim_body(value: &str) -> &str {
    value.trim_matches(|c| {
        matches!(c,
        '\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{00a0}' | '\u{1680}'
        | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}'
        | '\u{205f}' | '\u{3000}' | '\u{feff}')
    })
}

pub fn same_group(left: &UpstreamError, right: &UpstreamError) -> bool {
    left.thread_id == right.thread_id
        && left.turn_id == right.turn_id
        && variant_name(left.codex_error_info.as_ref())
            == variant_name(right.codex_error_info.as_ref())
        && left.category == right.category
        && left.http_status_code == right.http_status_code
        && normalized_body(left) == normalized_body(right)
}

impl UpstreamError {
    pub fn from_error(
        thread: &str,
        turn: Option<&str>,
        source: UpstreamErrorSource,
        error: &Value,
        will_retry: Option<bool>,
        now: u64,
    ) -> Self {
        let info = if source == UpstreamErrorSource::RpcResponse {
            error
                .get("data")
                .and_then(|data| data.get("codexErrorInfo"))
        } else {
            error.get("codexErrorInfo")
        }
        .filter(|info| !info.is_null());
        let (message, message_truncated) = sanitize(
            error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or_default(),
        );
        let details = error.get("additionalDetails").and_then(Value::as_str);
        let (additional_details, details_truncated) = match details {
            Some(details) => {
                let (details, truncated) = sanitize(details);
                (Some(details), truncated)
            }
            None => (None, false),
        };
        Self {
            thread_id: thread.into(),
            turn_id: turn.map(str::to_owned),
            source,
            category: classify(info),
            http_status_code: http_status_code(info),
            codex_error_info: info.cloned(),
            message,
            additional_details,
            truncated: message_truncated || details_truncated,
            will_retry,
            first_seen_at: now,
            last_seen_at: now,
            count: 1,
        }
    }
}

/// 返回字段是否改变；完成兜底只终止同组重试，不重复计入通知次数。
pub fn merge(current: &mut Option<UpstreamError>, next: UpstreamError) -> bool {
    if next.source != UpstreamErrorSource::RpcResponse {
        if let Some(previous) = current.as_mut().filter(|old| same_group(old, &next)) {
            let before = previous.clone();
            if next.source == UpstreamErrorSource::ErrorNotification {
                previous.count = previous.count.saturating_add(1);
                previous.message = next.message;
                previous.additional_details = next.additional_details;
                previous.truncated = next.truncated;
            }
            previous.last_seen_at = next.last_seen_at;
            previous.will_retry = next.will_retry;
            return *previous != before;
        }
    }
    let changed = current.as_ref() != Some(&next);
    *current = Some(next);
    changed
}

/// 清除终端控制序列，先脱敏再按 UTF-8 字符边界截断。
pub fn sanitize(text: &str) -> (String, bool) {
    let text = strip_controls(text);
    let text = redact_headers(&text);
    let text = redact_query_values(&text);
    let text = redact_tokens(&text);
    if text.len() <= MAX_ERROR_BYTES {
        return (text, false);
    }
    let mut end = MAX_ERROR_BYTES;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    (text[..end].into(), true)
}

fn strip_controls(text: &str) -> String {
    let mut result = String::new();
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            match chars.next() {
                Some('[') => {
                    for next in chars.by_ref() {
                        if ('@'..='~').contains(&next) {
                            break;
                        }
                    }
                }
                Some(']' | 'P' | 'X' | '^' | '_') => {
                    while let Some(next) = chars.next() {
                        if next == '\u{7}' || next == '\u{1b}' && chars.peek() == Some(&'\\') {
                            if next == '\u{1b}' {
                                chars.next();
                            }
                            break;
                        }
                    }
                }
                Some(' '..='/') => {
                    for next in chars.by_ref() {
                        if ('0'..='~').contains(&next) {
                            break;
                        }
                    }
                }
                _ => {}
            }
        } else if !(c <= '\u{1f}' && !matches!(c, '\n' | '\t')) {
            result.push(c);
        }
    }
    result
}

fn redact_headers(text: &str) -> String {
    text.split('\n')
        .map(|line| {
            // 上游可能在头部前附加错误说明；头名必须完整匹配，不能误认 X-Authorization。
            let lower = line.to_ascii_lowercase();
            for (colon, _) in line.match_indices(':') {
                let name = lower[..colon].trim_end();
                for header in ["authorization", "cookie", "set-cookie"] {
                    if let Some(prefix) = name.strip_suffix(header) {
                        if prefix
                            .chars()
                            .next_back()
                            .is_none_or(|c| !c.is_ascii_alphanumeric() && !matches!(c, '-' | '_'))
                        {
                            return format!("{}: {REDACTED}", &line[..colon]);
                        }
                    }
                }
            }
            line.into()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn redact_query_values(text: &str) -> String {
    let mut spans = Vec::new();
    for (start, c) in text.char_indices() {
        if !matches!(c, '?' | '&') {
            continue;
        }
        let rest = &text[start + 1..];
        let Some((equal, '=')) = rest
            .char_indices()
            .find(|(_, c)| c.is_whitespace() || matches!(c, '=' | '&' | '?' | '#'))
        else {
            continue;
        };
        let name = &rest[..equal];
        if name
            .chars()
            .any(|c| c.is_whitespace() || matches!(c, '&' | '?' | '#'))
        {
            continue;
        }
        let name = name.to_ascii_lowercase();
        if !["key", "token", "secret", "password", "auth", "signature"]
            .iter()
            .any(|part| name.contains(part))
        {
            continue;
        }
        let value_start = start + 1 + equal + 1;
        let end = text[value_start..]
            .char_indices()
            .find(|(_, c)| c.is_whitespace() || matches!(c, '&' | '#' | '"' | '\'' | '<' | '>'))
            .map_or(text.len(), |(offset, _)| value_start + offset);
        spans.push((value_start, end));
    }
    replace_spans(text, spans)
}

fn token_byte(c: u8) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-' | b'+' | b'/' | b'=')
}

fn redact_tokens(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut spans = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index..].len() >= 7
            && bytes[index..index + 6].eq_ignore_ascii_case(b"bearer")
            && matches!(bytes[index + 6], b' ' | b'\t')
        {
            let mut token_start = index + 6;
            while token_start < bytes.len() && matches!(bytes[token_start], b' ' | b'\t') {
                token_start += 1;
            }
            let mut end = token_start;
            while end < bytes.len()
                && !bytes[end].is_ascii_whitespace()
                && !matches!(bytes[end], b'"' | b'\'' | b',' | b';')
            {
                end += 1;
            }
            if end > token_start && text.is_char_boundary(end) {
                spans.push((index, end));
                index = end;
                continue;
            }
        }
        if bytes[index..].starts_with(b"sk-") {
            let mut end = index + 3;
            while end < bytes.len() && token_byte(bytes[end]) {
                end += 1;
            }
            if end > index + 3 {
                spans.push((index, end));
                index = end;
                continue;
            }
        }
        if bytes[index..].starts_with(b"eyJ") {
            let mut end = index + 3;
            while end < bytes.len() && (token_byte(bytes[end]) || bytes[end] == b'.') {
                end += 1;
            }
            while end > index && bytes[end - 1] == b'.' {
                end -= 1;
            }
            let jwt = &text[index..end];
            let parts: Vec<_> = jwt.split('.').collect();
            if parts.len() == 3 && parts.iter().all(|part| !part.is_empty()) {
                spans.push((index, end));
                index = end;
                continue;
            }
        }
        if token_byte(bytes[index]) {
            let start = index;
            while index < bytes.len() && token_byte(bytes[index]) {
                index += 1;
            }
            if index - start >= 32 {
                spans.push((start, index));
            }
        } else {
            index += 1;
        }
    }
    replace_spans(text, spans)
}

fn replace_spans(text: &str, spans: Vec<(usize, usize)>) -> String {
    let mut output = String::new();
    let mut cursor = 0;
    for (start, end) in spans {
        if start < cursor || start == end {
            continue;
        }
        output.push_str(&text[cursor..start]);
        output.push_str(REDACTED);
        cursor = end;
    }
    output.push_str(&text[cursor..]);
    output
}

impl Machine {
    /// 自报 turnId 不授予归属；已确认的 review 回放只忽略到达阶段的 ended 标记。
    pub(crate) fn diagnostic_run(&self, thread: &str, turn: &str, replay: bool) -> Option<String> {
        if self.runs.phase != Phase::Open || turn.is_empty() {
            return None;
        }
        let claim = self.claims.get(thread)?;
        let run = claim.run_id.as_deref()?;
        let record = self.runs.runs.get(run)?;
        let execution = record
            .review_thread_id
            .as_deref()
            .unwrap_or(&record.thread_id);
        if execution != thread
            || claim.settling()
            || claim.pending_start.is_some()
            || claim.turn_id() != Some(turn)
            || !matches!(record.state, RunState::Running | RunState::Stopping)
            || record.current_turn_id.as_deref() != Some(turn)
            || self.runs.find_run(thread, turn, None).as_deref() != Some(run)
            || !self.run_parent_is_owned(run, thread, turn)
            || !self.runs.active.get(run).is_some_and(|active| {
                active
                    .current_turn
                    .as_ref()
                    .is_some_and(|key| key.0 == thread && key.1 == turn)
            })
            || self.records.get(thread).is_some_and(|record| {
                record.released_turns.iter().any(|old| old == turn)
                    || !replay && record.ended_turns.iter().any(|old| old == turn)
            })
            || self
                .runs
                .turn_runs
                .get(thread)
                .is_some_and(|links| links.iter().any(|link| link.turn == turn && link.completed))
        {
            return None;
        }
        Some(run.into())
    }

    pub(crate) fn run_diagnostic_event(
        &mut self,
        thread: &str,
        turn: &str,
        replay: bool,
        error: Option<UpstreamError>,
        progress: bool,
        actions: &mut Vec<Action>,
    ) {
        let Some(run) = self.diagnostic_run(thread, turn, replay) else {
            return;
        };
        let record = self.runs.runs.get_mut(&run).expect("诊断归属已确认");
        let error_changed = error.is_some_and(|next| merge(&mut record.upstream_error, next));
        // 旧客户端沿用第一批进展语义；显式策略的进展由 C1 去重后统一更新。
        let progress_changed = progress
            && record.run_policy.is_none()
            && record.last_progress_at != Some(self.clock.now_ms);
        if progress_changed {
            record.last_progress_at = Some(self.clock.now_ms);
        }
        if !error_changed && !progress_changed {
            return;
        }
        if error_changed {
            let error = record.upstream_error.clone().expect("报错已记录");
            self.mark_run_diagnostic(&run, super::DiagnosticField::UpstreamError, None);
            self.upstream_access_check(&run, &error, actions);
        }
        if progress_changed {
            self.mark_run_diagnostic(&run, super::DiagnosticField::LastProgressAt, None);
        }
        if !self.diagnostic_batch_active {
            self.flush_run_diagnostics(actions);
        }
    }

    /// 调用方已核对原请求身份；此钩子只改诊断，绝不新增轮次或驱动协调。
    pub(crate) fn run_rpc_error(
        &mut self,
        run: &str,
        thread: &str,
        error: &Value,
        actions: &mut Vec<Action>,
    ) {
        if self.runs.phase != Phase::Open || !self.runs.active.contains_key(run) {
            return;
        }
        let Some(record) = self.runs.runs.get_mut(run) else {
            return;
        };
        if record
            .review_thread_id
            .as_deref()
            .unwrap_or(&record.thread_id)
            != thread
            || !matches!(record.state, RunState::Running | RunState::Stopping)
        {
            return;
        }
        let next = UpstreamError::from_error(
            thread,
            None,
            UpstreamErrorSource::RpcResponse,
            error,
            None,
            self.clock.now_ms,
        );
        if !merge(&mut record.upstream_error, next) {
            return;
        }
        self.mark_run_diagnostic(run, super::DiagnosticField::UpstreamError, None);
        if !self.diagnostic_batch_active {
            self.flush_run_diagnostics(actions);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn error(info: Value, message: &str, source: UpstreamErrorSource, now: u64) -> UpstreamError {
        UpstreamError::from_error(
            "线程",
            Some("本轮"),
            source,
            &json!({"message":message,"codexErrorInfo":info}),
            Some(source != UpstreamErrorSource::TurnCompleted),
            now,
        )
    }

    #[test]
    fn 分类按语义优先级并只使用结构化状态() {
        use UpstreamErrorCategory::*;
        for (info, expected) in [
            (json!("unauthorized"), Authentication),
            (json!("usageLimitExceeded"), UsageLimit),
            (json!("sessionBudgetExceeded"), UsageLimit),
            (
                json!({"usageLimitExceeded":{"httpStatusCode":429}}),
                UsageLimit,
            ),
            (json!("rateLimitExceeded"), RateLimit),
            (json!("serverOverloaded"), Server),
            (json!("internalServerError"), Server),
            (
                json!({"httpConnectionFailed":{"httpStatusCode":401}}),
                Authentication,
            ),
            (
                json!({"responseStreamConnectionFailed":{"httpStatusCode":402}}),
                PaymentRequired,
            ),
            (
                json!({"responseStreamDisconnected":{"httpStatusCode":403}}),
                PermissionDenied,
            ),
            (
                json!({"responseTooManyFailedAttempts":{"httpStatusCode":429}}),
                RateLimit,
            ),
            (
                json!({"responseStreamDisconnected":{"httpStatusCode":500}}),
                Server,
            ),
            (
                json!({"responseStreamDisconnected":{"httpStatusCode":599}}),
                Server,
            ),
            (
                json!({"httpConnectionFailed":{"httpStatusCode":null}}),
                Network,
            ),
            (
                json!({"responseStreamConnectionFailed":{"httpStatusCode":null}}),
                Network,
            ),
            (
                json!({"responseStreamDisconnected":{"httpStatusCode":null}}),
                Unknown,
            ),
            (
                json!({"responseStreamDisconnected":{"httpStatusCode":400}}),
                Unknown,
            ),
            (json!({"httpConnectionFailed":{}}), Unknown),
            (
                json!({"httpConnectionFailed":{"httpStatusCode":-1}}),
                Unknown,
            ),
            (json!("cyberPolicy"), Unknown),
            (json!("misalignmentPolicyViolation"), Unknown),
            (json!("flexUnavailable"), Unknown),
            (json!("contextWindowExceeded"), Unknown),
            (json!("badRequest"), Unknown),
            (Value::Null, Unknown),
        ] {
            assert_eq!(classify(Some(&info)), expected, "{info}");
        }
        let diagnostic = UpstreamError::from_error(
            "线程",
            None,
            UpstreamErrorSource::ErrorNotification,
            &json!({"message":"HTTP 401","httpStatusCode":401}),
            Some(true),
            1,
        );
        assert_eq!(diagnostic.category, Unknown);
        assert_eq!(diagnostic.http_status_code, None);
    }

    #[test]
    fn 完整重连提示同组累加且类别和变体变化另起一组() {
        let info = json!({"responseStreamDisconnected":{"httpStatusCode":401}});
        let mut current = None;
        merge(
            &mut current,
            error(
                info.clone(),
                " Reconnecting... 2/5 ",
                UpstreamErrorSource::ErrorNotification,
                1,
            ),
        );
        merge(
            &mut current,
            error(
                info.clone(),
                "Reconnecting... 3/5",
                UpstreamErrorSource::ErrorNotification,
                2,
            ),
        );
        assert_eq!(current.as_ref().unwrap().count, 2);
        assert_eq!(current.as_ref().unwrap().first_seen_at, 1);
        assert_eq!(current.as_ref().unwrap().last_seen_at, 2);
        for text in [
            "原因 2/5",
            "Reconnecting... 2/5 后续",
            "Reconnecting...  2/5",
        ] {
            let next = error(
                info.clone(),
                text,
                UpstreamErrorSource::ErrorNotification,
                3,
            );
            assert_eq!(normalized_body(&next), text);
            assert!(!same_group(current.as_ref().unwrap(), &next));
        }
        merge(
            &mut current,
            error(
                json!({"responseStreamDisconnected":{"httpStatusCode":429}}),
                "Reconnecting... 4/5",
                UpstreamErrorSource::ErrorNotification,
                3,
            ),
        );
        assert_eq!(current.as_ref().unwrap().count, 1);
        merge(
            &mut current,
            error(
                json!({"httpConnectionFailed":{"httpStatusCode":429}}),
                "Reconnecting... 4/5",
                UpstreamErrorSource::ErrorNotification,
                4,
            ),
        );
        assert_eq!(current.as_ref().unwrap().first_seen_at, 4);
    }

    #[test]
    fn 完成同组不重复计数不同组兜底写入() {
        let info = json!("unauthorized");
        let mut current = None;
        merge(
            &mut current,
            error(
                info.clone(),
                "认证失败",
                UpstreamErrorSource::ErrorNotification,
                1,
            ),
        );
        merge(
            &mut current,
            error(info, "认证失败", UpstreamErrorSource::TurnCompleted, 2),
        );
        let saved = current.as_ref().unwrap();
        assert_eq!(saved.count, 1);
        assert_eq!(saved.will_retry, Some(false));
        assert_eq!(saved.source, UpstreamErrorSource::ErrorNotification);
        assert_eq!(saved.last_seen_at, 2);
        merge(
            &mut current,
            error(
                json!("usageLimitExceeded"),
                "额度耗尽",
                UpstreamErrorSource::TurnCompleted,
                3,
            ),
        );
        assert_eq!(
            current.as_ref().unwrap().source,
            UpstreamErrorSource::TurnCompleted
        );
        assert_eq!(current.as_ref().unwrap().first_seen_at, 3);
    }

    #[test]
    fn 启动失败从数据读取语义且总是新组() {
        let input = json!({"message":"启动失败","codexErrorInfo":"unauthorized",
            "data":{"codexErrorInfo":{"httpConnectionFailed":{"httpStatusCode":402}}}});
        let diagnostic = UpstreamError::from_error(
            "线程",
            None,
            UpstreamErrorSource::RpcResponse,
            &input,
            None,
            3,
        );
        assert_eq!(diagnostic.category, UpstreamErrorCategory::PaymentRequired);
        assert_eq!(diagnostic.will_retry, None);
        assert_eq!(diagnostic.turn_id, None);
        let mut current = Some(diagnostic.clone());
        let mut next = diagnostic;
        next.last_seen_at = 4;
        next.first_seen_at = 4;
        merge(&mut current, next);
        assert_eq!(current.unwrap().count, 1);
    }

    #[test]
    fn 脱敏覆盖认证密钥令牌查询参数头与长串() {
        for input in [
            "Bearer abcdef",
            "Bearer\tabcdef",
            "bEaReR abcdef",
            "sk-abcdef",
            "s\u{1b}[31mk-abcdef\u{1b}[0m",
            "sk-proj-abcdef",
            "eyJheader.payload.signature",
            "eyJheader.payload.signature.",
            "https://host/path?apiKEY=abcdef&normal=visible",
            "https://host?token=abcdef&secret=abcdef&password=abcdef&auth=abcdef&signature=abcdef",
            "Authorization: abcdef",
            "请求失败 Authorization: abcdef",
            "HTTP error: Authorization: abcdef",
            "[upstream] COOKIE: abcdef",
            " Cookie: abcdef",
            "Set-Cookie: abcdef",
            "abcdef0123456789abcdef0123456789abcd",
            "Aq9+Q3/aAq9+Q3/aAq9+Q3/aAq9+Q3/a",
        ] {
            let (redacted, truncated) = sanitize(input);
            assert!(redacted.contains(REDACTED), "{input} => {redacted}");
            assert!(!redacted.contains("abcdef"), "{input} => {redacted}");
            assert!(!truncated);
        }
        assert_eq!(
            sanitize("https://host?token=abcdef&normal=visible").0,
            "https://host?token=[已隐藏]&normal=visible"
        );
        assert_eq!(
            sanitize("X-Authorization: visible").0,
            "X-Authorization: visible"
        );
    }

    #[test]
    fn 截断遵守字节预算不切断中文且清除控制字符() {
        let (text, truncated) = sanitize(&"甲".repeat(1500));
        assert!(truncated);
        assert_eq!(text.len(), 4095);
        assert_eq!(text.chars().count(), 1365);
        assert_eq!(
            sanitize("甲\0\r\u{7}\u{1b}[31m乙\u{1b}[0m\n\t丙\u{1b}]0;标题\u{7}丁").0,
            "甲乙\n\t丙丁"
        );
        let diagnostic = UpstreamError::from_error(
            "线程",
            Some("轮"),
            UpstreamErrorSource::ErrorNotification,
            &json!({"message":"短消息","additionalDetails":"乙".repeat(1500)}),
            Some(true),
            1,
        );
        assert!(diagnostic.truncated);
        assert_eq!(diagnostic.additional_details.unwrap().len(), 4095);
    }

    #[test]
    fn 内部可选字段保留为空并饱和计数() {
        let mut diagnostic = error(
            Value::Null,
            "未知",
            UpstreamErrorSource::ErrorNotification,
            1,
        );
        let value = serde_json::to_value(&diagnostic).unwrap();
        assert!(value.get("httpStatusCode").unwrap().is_null());
        assert!(value.get("codexErrorInfo").unwrap().is_null());
        assert!(value.get("additionalDetails").unwrap().is_null());
        diagnostic.count = u32::MAX;
        let mut current = Some(diagnostic);
        merge(
            &mut current,
            error(
                Value::Null,
                "未知",
                UpstreamErrorSource::ErrorNotification,
                2,
            ),
        );
        assert_eq!(current.unwrap().count, u32::MAX);
    }

    #[test]
    fn 分组裁剪与客户端一致且空详情回退消息() {
        let mut diagnostic = error(
            Value::Null,
            "\u{feff}Reconnecting... 2/5\u{feff}",
            UpstreamErrorSource::ErrorNotification,
            1,
        );
        diagnostic.additional_details = Some("\u{feff} \t\u{feff}".into());
        assert_eq!(normalized_body(&diagnostic), "");
        diagnostic.additional_details = Some("\u{85}原因\u{85}".into());
        assert_eq!(normalized_body(&diagnostic), "\u{85}原因\u{85}");
        diagnostic.additional_details = Some("  原因 3/5  ".into());
        assert_eq!(normalized_body(&diagnostic), "原因 3/5");
        assert_eq!(
            sanitize("甲\u{1b}(0乙\u{1b}P终端指令\u{1b}\\丙").0,
            "甲乙丙"
        );
    }
}
