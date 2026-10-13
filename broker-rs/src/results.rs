//! 从条目生成输出、降级快照与条目摘要；纯函数，不读取时钟或随机源。
//! 输出优先级沿用 src/events.ts 146–155、210–213、567–575 行。

use crate::runs::types::{OutputSource, ProgressItem};
use serde_json::Value;

/// 输入、单轮输出及每份降级文本的 UTF-8 字节上限（含截断提示）。
pub const MAX_TEXT_BYTES: usize = 1024 * 1024;
const TRUNCATED: &str = "（输出过长，已截断）";

pub(crate) fn limit_text(text: &str) -> String {
    if text.len() <= MAX_TEXT_BYTES {
        return text.to_owned();
    }
    let mut end = MAX_TEXT_BYTES - TRUNCATED.len();
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{TRUNCATED}", &text[..end])
}

// 只暂存上限多一个完整字符，既能判断截断，也避免先拼接整个超长输出。
fn append_text(target: &mut String, text: &str) {
    if text.is_empty() || target.len() > MAX_TEXT_BYTES {
        return;
    }
    if !target.is_empty() {
        target.push('\n');
    }
    let mut end = text.len().min(MAX_TEXT_BYTES + 4 - target.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    target.push_str(&text[..end]);
}

/// 空数组或输出字段格式错误为未知；合法的纯工具条目可以明确生成空输出。
pub fn output_from_items(items: &[Value]) -> Option<(String, OutputSource)> {
    let mut output = ItemOutput::default();
    output.extend(items);
    output.finish()
}

/// 分页只保留有界文本和活动摘要，不把整个轮次的 JSON 留在内存中。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ItemOutput {
    review: Option<String>,
    final_answer: String,
    accumulated: String,
    seen: bool,
    invalid: bool,
    pub activity: Activity,
}

impl ItemOutput {
    pub fn extend(&mut self, items: &[Value]) {
        self.extend_with_classifier(items, classify_untrusted_input);
    }

    /// 分页调用方在接受当前页前核实运行身份，来源不能事后从布尔摘要中抹除。
    pub fn extend_with_classifier(
        &mut self,
        items: &[Value],
        mut classify: impl FnMut(&Value) -> crate::sa1b::InputSource,
    ) {
        for item in items {
            self.seen = true;
            self.activity.observe_with_source(item, classify(item));
            if self.observe(item).is_none() {
                self.invalid = true;
            }
        }
    }

    fn observe(&mut self, item: &Value) -> Option<()> {
        match item.get("type")?.as_str()? {
            "exitedReviewMode" => {
                self.review = Some(limit_text(item.get("review")?.as_str()?));
            }
            "agentMessage" => {
                let text = item.get("text")?.as_str()?;
                if item
                    .get("phase")
                    .is_some_and(|p| !p.is_null() && !p.is_string())
                {
                    return None;
                }
                append_text(&mut self.accumulated, text);
                if item.get("phase").and_then(Value::as_str) == Some("final_answer") {
                    append_text(&mut self.final_answer, text);
                }
            }
            _ => {}
        }
        Some(())
    }

    pub fn finish(&self) -> Option<(String, OutputSource)> {
        if !self.seen || self.invalid {
            None
        } else if let Some(text) = self.review.as_ref().filter(|text| !text.is_empty()) {
            Some((text.clone(), OutputSource::Review))
        } else if !self.final_answer.is_empty() {
            Some((limit_text(&self.final_answer), OutputSource::FinalAnswer))
        } else if !self.accumulated.is_empty() {
            Some((limit_text(&self.accumulated), OutputSource::Accumulated))
        } else {
            Some((String::new(), OutputSource::None))
        }
    }
}

/// 只有不带 itemsView（mock、旧版本）或标为 full 的 items 才是该轮完整条目。上游 0.159
/// 的 turn/completed 只放最后一条 agentMessage 并标 summary，空时标 notLoaded；
/// 这两种和不认识的取值都要按 turnId 补读，不能据此生成结果。
pub fn full_items(turn: &Value) -> Option<&[Value]> {
    if let Some(view) = turn.get("itemsView") {
        if view.as_str() != Some("full") {
            return None;
        }
    }
    turn.get("items")?.as_array().map(Vec::as_slice)
}

/// 一轮条目里与空转刹车有关的事实（S1-评审第 26 节）。只做归类，判定在 claim::loop_guard。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Activity {
    /// 有对外动作：命令、文件改动、各类工具调用、网页搜索、看图等；失败或被拒也算尝试过。
    pub action: bool,
    /// 有用户输入条目（userMessage）。
    pub user_input: bool,
    /// 有 review 或上下文压缩这类维护性条目：打断无动作计数，但不算任务进展。
    pub special: bool,
    /// 有不认识的类型或缺少类型字段：不能当作「没有动作」的证据。
    pub unknown: bool,
}

pub fn classify_untrusted_input(item: &Value) -> crate::sa1b::InputSource {
    if item.get("type").and_then(Value::as_str) == Some("userMessage") {
        crate::sa1b::InputSource::ExternalUser
    } else {
        crate::sa1b::InputSource::NotUserMessage
    }
}

impl Activity {
    pub fn observe(&mut self, item: &Value) {
        self.observe_with_source(item, classify_untrusted_input(item));
    }

    pub fn observe_with_source(&mut self, item: &Value, source: crate::sa1b::InputSource) {
        match item.get("type").and_then(Value::as_str) {
            Some(
                "commandExecution" | "fileChange" | "mcpToolCall" | "dynamicToolCall" | "webSearch"
                | "imageView" | "imageGeneration",
            ) => self.action = true,
            Some(t) if t.ends_with("ToolCall") => self.action = true,
            Some("userMessage") => {
                if source != crate::sa1b::InputSource::TrustedReconcile {
                    self.user_input = true;
                }
            }
            Some("agentMessage" | "reasoning" | "plan") => {}
            Some("enteredReviewMode" | "exitedReviewMode" | "contextCompaction") => {
                self.special = true
            }
            _ => self.unknown = true,
        }
    }

    pub fn of(items: &[Value]) -> Self {
        Self::of_with_classifier(items, classify_untrusted_input)
    }

    /// full items 与通知使用相同分类器；逐条合并，真实输入具有单调否决权。
    pub fn of_with_classifier(
        items: &[Value],
        mut classify: impl FnMut(&Value) -> crate::sa1b::InputSource,
    ) -> Self {
        let mut activity = Self::default();
        for item in items {
            activity.observe_with_source(item, classify(item));
        }
        activity
    }
}

/// 当前轮只保存两份最后文本；转为 pending 时整份移动到补读上下文中。
/// summary 是完成通知自带的摘要输出，只在补读失败降级时排在最后使用。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Snapshot {
    pub review: Option<String>,
    pub final_answer: Option<String>,
    pub summary: Option<(String, OutputSource)>,
}

impl Snapshot {
    pub fn observe(&mut self, item: &Value) {
        match item.get("type").and_then(Value::as_str) {
            Some("exitedReviewMode") => {
                if let Some(text) = item.get("review").and_then(Value::as_str) {
                    self.review = Some(limit_text(text));
                }
            }
            Some("agentMessage")
                if item.get("phase").and_then(Value::as_str) == Some("final_answer") =>
            {
                if let Some(text) = item.get("text").and_then(Value::as_str) {
                    self.final_answer = Some(limit_text(text));
                }
            }
            _ => {}
        }
    }

    /// 摘要只含最后一条 agentMessage，生成的输出可能不完整，所以只作降级兜底。
    pub fn observe_summary(&mut self, items: &[Value]) {
        if let Some(output) = output_from_items(items).filter(|(text, _)| !text.is_empty()) {
            self.summary = Some(output);
        }
    }

    pub fn output(&self) -> (String, OutputSource) {
        if let Some(text) = self.review.as_ref().filter(|text| !text.is_empty()) {
            (limit_text(text), OutputSource::Review)
        } else if let Some(text) = self.final_answer.as_ref().filter(|text| !text.is_empty()) {
            (limit_text(text), OutputSource::FinalAnswer)
        } else if let Some((text, source)) = &self.summary {
            (text.clone(), *source)
        } else {
            (String::new(), OutputSource::None)
        }
    }
}

/// 摘要稳定取前 80 个 Unicode 字符；换行折为空格，日志始终一事件一行。
pub fn summarize_item(item: &Value) -> Option<ProgressItem> {
    let item_type = item.get("type")?.as_str()?;
    let item_id = item.get("id")?.as_str()?;
    let summary = match item_type {
        "commandExecution" => item.get("command")?.as_str()?.chars().take(80).collect(),
        "agentMessage" | "plan" => item.get("text")?.as_str()?.chars().take(80).collect(),
        "fileChange" => item
            .get("changes")?
            .as_array()?
            .iter()
            .filter_map(|change| change.get("path").and_then(Value::as_str))
            .take(8)
            .map(|path| path.chars().take(80).collect::<String>())
            .collect::<Vec<_>>()
            .join("、"),
        "mcpToolCall" => format!(
            "{} / {}",
            item.get("server")?
                .as_str()?
                .chars()
                .take(40)
                .collect::<String>(),
            item.get("tool")?
                .as_str()?
                .chars()
                .take(40)
                .collect::<String>()
        ),
        "dynamicToolCall" => item
            .get("tool")
            .or_else(|| item.get("name"))?
            .as_str()?
            .chars()
            .take(80)
            .collect(),
        "webSearch" => item.get("query")?.as_str()?.chars().take(80).collect(),
        "imageView" => item.get("path")?.as_str()?.chars().take(80).collect(),
        "enteredReviewMode" => "开始审查".into(),
        "exitedReviewMode" => "审查完成".into(),
        "contextCompaction" => "压缩上下文".into(),
        _ if item_type.ends_with("ToolCall") => item
            .get("tool")
            .and_then(Value::as_str)
            .unwrap_or("工具调用")
            .chars()
            .take(80)
            .collect(),
        _ => return None,
    };
    Some(ProgressItem {
        item_id: item_id.into(),
        item_type: item_type.into(),
        summary: summary
            .chars()
            .take(80)
            .map(|c| if c.is_control() { ' ' } else { c })
            .collect(),
    })
}

#[cfg(test)]
mod paging_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn 多页中文输出按字节上限截断且后页活动不丢失() {
        let mut output = ItemOutput::default();
        for _ in 0..100 {
            output.extend(&[
                json!({"type":"agentMessage","phase":"final_answer","text":"中文".repeat(4000)}),
            ]);
        }
        output.extend(&[json!({"type":"commandExecution"})]);
        let (text, source) = output.finish().unwrap();
        assert!(text.len() <= MAX_TEXT_BYTES);
        assert!(text.ends_with(TRUNCATED));
        assert_eq!(source, OutputSource::FinalAnswer);
        assert!(output.activity.action);
        assert!(output.final_answer.len() <= MAX_TEXT_BYTES + 4);
        assert!(output.accumulated.len() <= MAX_TEXT_BYTES + 4);
    }

    #[test]
    fn 跨页优先级仍为最后审查再最终回答再全部回答() {
        let mut output = ItemOutput::default();
        output.extend(&[json!({"type":"agentMessage","text":"说明"})]);
        assert_eq!(
            output.finish(),
            Some(("说明".into(), OutputSource::Accumulated))
        );
        output.extend(&[json!({"type":"agentMessage","phase":"final_answer","text":"结论"})]);
        assert_eq!(
            output.finish(),
            Some(("结论".into(), OutputSource::FinalAnswer))
        );
        output.extend(&[json!({"type":"exitedReviewMode","review":"正式审查"})]);
        output.extend(&[json!({"type":"agentMessage","phase":"final_answer","text":"收尾"})]);
        assert_eq!(
            output.finish(),
            Some(("正式审查".into(), OutputSource::Review))
        );
        output.extend(&[json!({"type":"exitedReviewMode","review":"更新审查"})]);
        assert_eq!(
            output.finish(),
            Some(("更新审查".into(), OutputSource::Review))
        );
    }

    #[test]
    fn 单条大于十兆但已被调用方交付时文本有截断标记() {
        let mut output = ItemOutput::default();
        output.extend(&[json!({"type":"agentMessage","text":"大".repeat(4*1024*1024)})]);
        let (text, _) = output.finish().unwrap();
        assert!(text.len() <= MAX_TEXT_BYTES);
        assert!(text.ends_with(TRUNCATED));
    }
}
