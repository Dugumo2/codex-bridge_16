//! 会话文件读取：真实文件、真实偏移，断言读取量有界、半行与坏行的处理、路径校验。
use super::*;
use serde_json::json;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};

const THREAD: &str = "019a0000-0000-7000-8000-00000000abcd";

/// 临时会话文件：值被丢弃时连目录一起删，断言失败展开时也一样。
struct TempRollout {
    dir: std::path::PathBuf,
    path: std::path::PathBuf,
}

impl std::ops::Deref for TempRollout {
    type Target = std::path::Path;

    fn deref(&self) -> &std::path::Path {
        &self.path
    }
}

impl AsRef<std::path::Path> for TempRollout {
    fn as_ref(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for TempRollout {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn temp_file(tag: &str) -> TempRollout {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "cxb-rollout-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("rollout-2026-11-05T01-02-03-{tag}-{THREAD}.jsonl"));
    TempRollout { dir, path }
}

#[test]
fn 临时文件在值丢弃和断言失败时都连目录删除() {
    let dropped = {
        let file = temp_file("drop");
        append(&file, "内容\n");
        file.dir.clone()
    };
    assert!(!dropped.exists(), "值丢弃后目录还在：{dropped:?}");

    let (tx, rx) = std::sync::mpsc::channel();
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let file = temp_file("panic");
        append(&file, "内容\n");
        tx.send(file.dir.clone()).unwrap();
        panic!("模拟断言失败");
    }));
    assert!(unwound.is_err());
    let dir = rx.recv().unwrap();
    assert!(!dir.exists(), "断言失败后目录还在：{dir:?}");
}

fn line(ts: &str, kind: &str, payload: serde_json::Value) -> String {
    format!(
        "{}\n",
        json!({"timestamp": ts, "type": kind, "payload": payload})
    )
}

fn task_started(ts: &str, turn: &str) -> String {
    line(
        ts,
        "event_msg",
        json!({"type":"task_started","turn_id":turn,"model_context_window":1}),
    )
}

fn message(ts: &str, id: &str, text: &str) -> String {
    line(
        ts,
        "response_item",
        json!({"type":"agent_message","id":id,"author":"/root","recipient":"/root/子",
            "content":[{"type":"input_text","text":text}]}),
    )
}

fn call(ts: &str, id: &str, name: &str) -> String {
    line(
        ts,
        "response_item",
        json!({"type":"function_call","name":name,"namespace":"collaboration","call_id":id,
            "arguments":json!({"target":"子","message":"正文"}).to_string()}),
    )
}

fn append(path: &std::path::Path, text: &str) {
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    f.write_all(text.as_bytes()).unwrap();
}

fn path_str(path: &std::path::Path) -> String {
    path.to_str().unwrap().to_owned()
}

#[test]
fn 路径须为绝对路径_jsonl_结尾_文件名含线程编号且无上级目录() {
    let good = path_str(&temp_file("ok"));
    assert!(validate_path(&good, THREAD));
    assert!(!validate_path(&good, "别的线程"));
    assert!(!validate_path(&good, ""));
    assert!(!validate_path(&format!("rollout-{THREAD}.jsonl"), THREAD));
    assert!(!validate_path(&good.replace(".jsonl", ".json"), THREAD));
    let dir = std::env::temp_dir();
    let up = dir.join("..").join(format!("rollout-{THREAD}.jsonl"));
    assert!(!validate_path(&path_str(&up), THREAD));
    assert!(!validate_path(&path_str(&dir.join(THREAD)), THREAD));
}

#[test]
fn 时间戳按_utc_毫秒解析且拒绝异形() {
    assert_eq!(parse_timestamp("1970-01-01T00:00:00.000Z"), Some(0));
    assert_eq!(
        parse_timestamp("2026-11-05T01:02:03.456Z"),
        Some(1_793_840_523_456)
    );
    assert_eq!(
        parse_timestamp("2000-03-01T00:00:00Z"),
        Some(951_868_800_000)
    );
    assert_eq!(
        parse_timestamp("2026-11-05T01:02:03.4Z"),
        Some(1_793_840_523_400)
    );
    for bad in [
        "",
        "2026-11-05 01:02:03.456Z",
        "2026-13-05T01:02:03.456Z",
        "2026-11-05T01:02:03.456",
        "2026-11-05T01:02:03.4x6Z",
        "１０２６-11-05T01:02:03.456Z",
    ] {
        assert_eq!(parse_timestamp(bad), None, "{bad}");
    }
}

#[test]
fn 首次读末尾窗口_按轮标记归属消息_半行留到下次() {
    let path = temp_file("tail");
    let mut text = task_started("2026-11-05T01:00:00.000Z", "旧轮");
    text += &message("2026-11-05T01:00:01.000Z", "m1", "第一条");
    text += &line(
        "2026-11-05T01:00:02.000Z",
        "turn_context",
        json!({"turn_id":"新轮","cwd":"C:\\"}),
    );
    text += &call("2026-11-05T01:00:03.000Z", "调用", "followup_task");
    text += &message("2026-11-05T01:00:04.000Z", "m2", "第二条");
    append(&path, &text);
    let half = message("2026-11-05T01:00:05.000Z", "m3", "第三条");
    let (head, tail) = half.split_at(half.len() / 2);
    append(&path, head);
    let chunk = read_chunk(&path_str(&path), None).unwrap();
    assert_eq!(chunk.bad_lines, 0);
    let summary: Vec<_> = chunk
        .entries
        .iter()
        .map(|e| match &e.record {
            RolloutRecord::Call { item } => format!("调用:{}", item["call_id"]),
            RolloutRecord::Message { turn, item } => {
                format!("{}:{}", turn.as_deref().unwrap_or("?"), item["id"])
            }
        })
        .collect();
    assert_eq!(summary, ["旧轮:\"m1\"", "调用:\"调用\"", "新轮:\"m2\""]);
    assert_eq!(
        chunk.entries[0].wall_ms,
        parse_timestamp("2026-11-05T01:00:01.000Z").unwrap()
    );
    assert_eq!(chunk.cursor.offset, text.len() as u64, "半行不消费");
    assert_eq!(chunk.cursor.turn.as_deref(), Some("新轮"));
    // 没有新增内容：不读出条目，游标不动。
    let again = read_chunk(&path_str(&path), Some(&chunk.cursor)).unwrap();
    assert!(again.entries.is_empty());
    assert_eq!(again.cursor, chunk.cursor);
    // 半行补全后读到，且归属仍是新轮。
    append(&path, tail);
    let next = read_chunk(&path_str(&path), Some(&chunk.cursor)).unwrap();
    assert_eq!(next.entries.len(), 1);
    assert!(matches!(&next.entries[0].record,
        RolloutRecord::Message { turn: Some(t), item } if t == "新轮" && item["id"] == "m3"));
    assert_eq!(next.bytes_read, tail.len() as u64 + head.len() as u64);
}

#[test]
fn 坏行_缺字段_未知类型与未知字段_不崩溃且只计坏行() {
    let path = temp_file("bad");
    let mut text = task_started("2026-11-05T01:00:00.000Z", "轮");
    text += "这不是 JSON\n";
    text += "{\"type\":\"response_item\"}\n";
    // 缺时间戳和未知顶层类型都是坏行；已知记录的扩展字段可忽略。
    text += &format!(
        "{}\n",
        json!({"type":"response_item","payload":{"type":"agent_message","id":"无时间"}})
    );
    text += &line("2026-11-05T01:00:01.000Z", "world_state", json!({"x":1}));
    text += &line(
        "2026-11-05T01:00:01.000Z",
        "inter_agent_communication_metadata",
        json!({"trigger_turn":true}),
    );
    text += &format!(
        "{}\n",
        json!({"timestamp":"2026-11-05T01:00:02.000Z","ordinal":7,"type":"response_item","未知":1,
            "payload":{"type":"agent_message","id":"m","author":"/root","recipient":"/root/子",
            "content":[{"type":"input_text","text":"x"}],"新字段":true}})
    );
    text += &call("2026-11-05T01:00:03.000Z", "无关", "spawn_agent");
    text += &line(
        "2026-11-05T01:00:03.000Z",
        "response_item",
        json!({"type":"message","role":"user","content":[]}),
    );
    append(&path, &text);
    let chunk = read_chunk(&path_str(&path), None).unwrap();
    assert_eq!(chunk.bad_lines, 4);
    assert_eq!(chunk.entries.len(), 2, "用户输入也作为文件消费反证交付");
    assert_eq!(chunk.cursor.offset, text.len() as u64);
}

#[test]
fn 文件缺失_截断与目录均报错误不崩溃() {
    let path = temp_file("missing");
    assert_eq!(
        read_chunk(&path_str(&path), None),
        Err(RolloutError::Missing)
    );
    append(&path, &task_started("2026-11-05T01:00:00.000Z", "轮"));
    let chunk = read_chunk(&path_str(&path), None).unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(3)
        .unwrap();
    assert_eq!(
        read_chunk(&path_str(&path), Some(&chunk.cursor)),
        Err(RolloutError::Shrunk)
    );
    let dir = path.parent().unwrap();
    assert_eq!(
        read_chunk(&path_str(dir), None),
        Err(RolloutError::Unreadable)
    );
}

#[test]
fn 超长行整行跳过_之后的行照常读到() {
    let path = temp_file("long");
    append(&path, &task_started("2026-11-05T01:00:00.000Z", "轮"));
    let first = read_chunk(&path_str(&path), None).unwrap();
    let long = "x".repeat(MAX_READ_BYTES as usize + 10);
    append(&path, &format!("{long}\n"));
    append(&path, &message("2026-11-05T01:00:01.000Z", "之后", "正文"));
    let mut cursor = first.cursor.clone();
    let mut entries = vec![];
    let mut skipped = 0;
    for _ in 0..4 {
        let chunk = read_chunk(&path_str(&path), Some(&cursor)).unwrap();
        assert!(chunk.bytes_read <= MAX_READ_BYTES, "单次读取有上限");
        skipped += chunk.skipped_lines;
        entries.extend(chunk.entries);
        cursor = chunk.cursor;
    }
    assert_eq!(skipped, 1);
    assert_eq!(entries.len(), 1);
    assert!(matches!(
        &entries[0].record,
        RolloutRecord::Message { turn: None, .. }
    ));
    assert!(entries[0].turn_lost, "超长行可能隐藏边界，旧锚点必须失效");
}

/// 前面是一大段全零，末尾才有内容；首次只读末尾窗口，跟随只读新增。
/// `set_len` 在 NTFS 上真占磁盘，所以不造 GiB 级文件：3 倍读取上限已经足够，整个读一遍就会超过下面的断言。
#[test]
fn 大文件只读末尾窗口与新增内容_读取字节有界() {
    let path = temp_file("sparse");
    let hole = 3 * MAX_TAIL_BYTES;
    {
        let f = std::fs::File::create(&path).unwrap();
        f.set_len(hole).unwrap();
    }
    let mut text = "\n".to_owned();
    text += &task_started("2026-11-05T01:00:00.000Z", "当前轮");
    text += &message("2026-11-05T01:00:01.000Z", "m", "正文");
    append(&path, &text);
    let chunk = read_chunk(&path_str(&path), None).unwrap();
    assert!(chunk.bytes_read <= TAIL_BYTES + 1, "{}", chunk.bytes_read);
    assert_eq!(chunk.entries.len(), 1);
    assert_eq!(chunk.cursor.offset, hole + text.len() as u64);
    append(&path, &message("2026-11-05T01:00:02.000Z", "m2", "正文"));
    let next = read_chunk(&path_str(&path), Some(&chunk.cursor)).unwrap();
    assert_eq!(next.entries.len(), 1);
    assert!(next.bytes_read < 1024);

    // 窗口里没有轮次标记：向前加倍多读，总量不超过上限；仍找不到时消息的轮次为 None。
    let bare = temp_file("sparse-bare");
    {
        let f = std::fs::File::create(&bare).unwrap();
        f.set_len(hole).unwrap();
    }
    append(
        &bare,
        &format!("\n{}", message("2026-11-05T01:00:01.000Z", "m", "正文")),
    );
    let chunk = read_chunk(&path_str(&bare), None).unwrap();
    assert!(
        chunk.bytes_read <= 2 * MAX_TAIL_BYTES,
        "{}",
        chunk.bytes_read
    );
    assert!(matches!(
        &chunk.entries[..],
        [RolloutEntry {
            record: RolloutRecord::Message { turn: None, .. },
            turn_lost: true,
            ..
        }]
    ));
    assert!(
        chunk.cursor.lost,
        "到上限仍找不到：起点所在的轮未知，交给状态机降级"
    );

    // 从文件头跟随一个没有换行的巨大空洞：每次只读上限，按超长行跳过。
    let chunk = read_chunk(&path_str(&bare), Some(&RolloutCursor::default())).unwrap();
    assert_eq!(chunk.bytes_read, MAX_READ_BYTES);
    assert!(chunk.cursor.skipping && chunk.more);
}

#[test]
fn 窗口起点落在轮次中间时向前多读到轮次标记() {
    let path = temp_file("rewind");
    let mut text = task_started("2026-11-05T01:00:00.000Z", "本轮");
    // 填充让标记落在首个窗口之外。
    let filler = line(
        "2026-11-05T01:00:00.500Z",
        "response_item",
        json!({"type":"reasoning","summary":[],"encrypted_content":"y".repeat(1000)}),
    );
    while text.len() < TAIL_BYTES as usize + 4096 {
        text += &filler;
    }
    text += &message("2026-11-05T01:00:01.000Z", "m", "正文");
    append(&path, &text);
    let chunk = read_chunk(&path_str(&path), None).unwrap();
    assert!(
        matches!(&chunk.entries[..], [RolloutEntry { record: RolloutRecord::Message { turn: Some(t), .. }, .. }] if t == "本轮")
    );
    assert!(chunk.bytes_read <= 3 * TAIL_BYTES + 2);
}

#[test]
fn 读取期间文件可被追加写入() {
    let path = temp_file("share");
    append(&path, &task_started("2026-11-05T01:00:00.000Z", "轮"));
    // 写端一直开着句柄，读端只读打开、读完关闭，两边互不阻塞。
    let mut writer = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    let chunk = read_chunk(&path_str(&path), None).unwrap();
    writer
        .write_all(message("2026-11-05T01:00:01.000Z", "m", "正文").as_bytes())
        .unwrap();
    writer.flush().unwrap();
    let next = read_chunk(&path_str(&path), Some(&chunk.cursor)).unwrap();
    assert_eq!(next.entries.len(), 1);
    drop(writer);
    std::fs::remove_file(&path).expect("读端不残留句柄，文件可以删除");
}

#[test]
fn 窗口里只有工具输出没有消息时也向前找到当前轮() {
    let path = temp_file("bare-output");
    let mut text = task_started("2026-11-05T01:00:00.000Z", "忙碌轮");
    let filler = line(
        "2026-11-05T01:00:00.500Z",
        "response_item",
        json!({"type":"function_call_output","call_id":"工具","output":"z".repeat(1000)}),
    );
    while text.len() < TAIL_BYTES as usize + 4096 {
        text += &filler;
    }
    append(&path, &text);
    let chunk = read_chunk(&path_str(&path), None).unwrap();
    assert!(chunk.entries.is_empty());
    assert_eq!(chunk.cursor.turn.as_deref(), Some("忙碌轮"));
    assert!(!chunk.cursor.lost);
    assert!(chunk.bytes_read <= 3 * TAIL_BYTES + 2);
    // 之后吸收的消息带上当前轮。
    append(&path, &message("2026-11-05T01:00:01.000Z", "m", "正文"));
    let next = read_chunk(&path_str(&path), Some(&chunk.cursor)).unwrap();
    assert!(matches!(
        &next.entries[..],
        [RolloutEntry { record: RolloutRecord::Message { turn: Some(t), .. }, turn_lost: false, .. }] if t == "忙碌轮"
    ));
}

/// 合法 JSON 仍可能无法解释；真实读取器必须把这一点带到覆盖判定入口。
#[test]
fn gap_b_red_合法json但无法解释的记录必须报告坏行并失去轮锚点() {
    for (tag, kind, payload) in [
        ("缺类型", "response_item", json!({"id":"未知"})),
        ("数字类型", "response_item", json!({"type":7,"id":"未知"})),
        (
            "未知条目",
            "response_item",
            json!({"type":"future_consumed_input","id":"未知"}),
        ),
        (
            "坏角色",
            "response_item",
            json!({"type":"message","role":7,"content":[]}),
        ),
        (
            "未知外层",
            "future_rollout_record",
            json!({"type":"agent_message"}),
        ),
        (
            "坏边界",
            "event_msg",
            json!({"type":"task_started","turn_id":7}),
        ),
        (
            "未知事件",
            "event_msg",
            json!({"type":"future_runtime_unloaded"}),
        ),
    ] {
        let path = temp_file(tag);
        let mut text = task_started("2026-11-05T01:00:00.000Z", "原轮");
        text += &line("2026-11-05T01:00:01.000Z", kind, payload);
        text += &message("2026-11-05T01:00:02.000Z", "后续消费", "不能沿用原轮");
        append(&path, &text);
        let chunk = read_chunk(&path_str(&path), None).unwrap();
        assert_eq!(
            chunk.cursor.offset,
            text.len() as u64,
            "{tag}：完整行确已读完"
        );
        assert_eq!(chunk.entries.len(), 1, "{tag}：读到后续消费，不是空文件");
        assert!(chunk.bad_lines > 0, "{tag}：不能静默当作无关记录");
        assert!(
            matches!(
                &chunk.entries[0].record,
                RolloutRecord::Message { turn: None, .. }
            ),
            "{tag}：无法解释记录之后不能继续沿用旧轮锚点"
        );
    }
}

#[test]
fn gap_b_red_确认无关记录与尚未补齐的半行不制造坏行() {
    let path = temp_file("known-and-partial");
    let mut text = task_started("2026-11-05T01:00:00.000Z", "轮");
    text += &line(
        "2026-11-05T01:00:01.000Z",
        "response_item",
        json!({"type":"reasoning","summary":[]}),
    );
    append(&path, &text);
    let unknown = line(
        "2026-11-05T01:00:02.000Z",
        "response_item",
        json!({"type":"future_consumed_input"}),
    );
    let (head, tail) = unknown.split_at(unknown.len() / 2);
    append(&path, head);
    let first = read_chunk(&path_str(&path), None).unwrap();
    assert_eq!(first.bad_lines, 0, "确认无关记录和半行都不能误报");
    assert_eq!(first.cursor.offset, text.len() as u64, "半行未被消费");
    append(&path, tail);
    let second = read_chunk(&path_str(&path), Some(&first.cursor)).unwrap();
    assert_eq!(second.bad_lines, 1, "补全后未知记录才进入覆盖缺口");
}

#[test]
fn gap_b_合法输出可选字段与明确元数据不误报坏行() {
    let path = temp_file("known-output");
    let mut text = task_started("2026-11-05T01:00:00.000Z", "轮");
    for payload in [
        json!({"type":"function_call_output","output":"工具结果"}),
        json!({"type":"web_search_call"}),
        json!({"type":"tool_search_call","execution":"client","arguments":{}}),
        json!({"type":"tool_search_output","execution":"client","status":"completed","tools":[]}),
        json!({"type":"additional_tools","role":"developer","tools":[]}),
        json!({"type":"context_compaction"}),
        json!({"type":"compaction_trigger"}),
        json!({"type":"configuration_update","reasoning":{"effort":"medium"}}),
        json!({"type":"compaction_summary","encrypted_content":"摘要"}),
        json!({"type":"message","role":"assistant","content":[]}),
    ] {
        text += &line("2026-11-05T01:00:01.000Z", "response_item", payload);
    }
    text += &line(
        "2026-11-05T01:00:01.000Z",
        "world_state",
        json!({"full":true,"state":{}}),
    );
    text += &line(
        "2026-11-05T01:00:01.000Z",
        "inter_agent_communication_metadata",
        json!({"trigger_turn":true}),
    );
    text += &message("2026-11-05T01:00:02.000Z", "消费", "消息");
    append(&path, &text);
    let chunk = read_chunk(&path_str(&path), None).unwrap();
    assert_eq!(chunk.bad_lines, 0);
    assert_eq!(chunk.entries.len(), 1);
    assert!(
        matches!(&chunk.entries[0].record, RolloutRecord::Message { turn:Some(t), .. } if t == "轮")
    );
}

#[test]
fn gap_b_已知事件坏结构不能伪装为无关记录() {
    for (kind, payload) in [
        ("event_msg", json!({"type":"agent_message","message":7})),
        ("event_msg", json!({"type":"agent_reasoning","text":null})),
        (
            "event_msg",
            json!({"type":"exec_command_begin","call_id":"x"}),
        ),
        (
            "event_msg",
            json!({"type":"task_started","turn_id":"新轮","model_context_window":"坏字段"}),
        ),
        (
            "event_msg",
            json!({"type":"task_complete","turn_id":"旧轮","error":{"message":7}}),
        ),
        ("turn_context", json!({"turn_id":"新轮","cwd":7})),
        ("session_meta", json!({"id":"线程","cwd":7})),
        (
            "response_item",
            json!({"type":"message","role":"assistant","content":[{"type":"未知内容"}]}),
        ),
    ] {
        let path = temp_file("known-malformed");
        let mut text = task_started("2026-11-05T01:00:00.000Z", "旧轮");
        text += &line("2026-11-05T01:00:01.000Z", kind, payload);
        text += &message("2026-11-05T01:00:02.000Z", "后续", "消费");
        append(&path, &text);
        let chunk = read_chunk(&path_str(&path), None).unwrap();
        assert_eq!(chunk.bad_lines, 1);
        assert!(matches!(
            &chunk.entries[0].record,
            RolloutRecord::Message { turn: None, .. }
        ));
    }
}

/// 审查复现：已知外层类型不能掩盖无法解释的嵌套结构。
#[test]
fn gap_b_review1_已知输出嵌套坏结构真实读取必须否决并清轮锚() {
    let mut swallowed = Vec::new();
    for (tag, kind, payload) in [
        (
            "推理摘要标量",
            "response_item",
            json!({"type":"reasoning","summary":[7]}),
        ),
        (
            "推理摘要未知类型",
            "response_item",
            json!({"type":"reasoning","summary":[{"type":"future_summary","text":"x"}]}),
        ),
        (
            "推理摘要坏文本",
            "response_item",
            json!({"type":"reasoning","summary":[{"type":"summary_text","text":7}]}),
        ),
        (
            "推理内容未知类型",
            "response_item",
            json!({"type":"reasoning","summary":[],"content":[{"type":"future_input"}]}),
        ),
        (
            "推理密文坏类型",
            "response_item",
            json!({"type":"reasoning","summary":[],"encrypted_content":7}),
        ),
        (
            "工具输出未知片段",
            "response_item",
            json!({"type":"function_call_output","output":[{"type":"future_input"}]}),
        ),
        (
            "工具输出标量片段",
            "response_item",
            json!({"type":"function_call_output","output":[7]}),
        ),
        (
            "工具输出坏文本",
            "response_item",
            json!({"type":"function_call_output","output":[{"type":"input_text","text":7}]}),
        ),
        (
            "工具输出坏密文",
            "response_item",
            json!({"type":"function_call_output","output":[{"type":"encrypted_content","encrypted_content":7}]}),
        ),
        (
            "工具输出坏图片",
            "response_item",
            json!({"type":"function_call_output","output":[{"type":"input_image","image_url":7}]}),
        ),
        (
            "工具输出坏图片精度",
            "response_item",
            json!({"type":"function_call_output","output":[{"type":"input_image","file_id":"f","detail":"future"}]}),
        ),
        (
            "自定义工具输出未知片段",
            "response_item",
            json!({"type":"custom_tool_call_output","call_id":"c","output":[{"type":"future_input"}]}),
        ),
        (
            "本地命令缺动作类型",
            "response_item",
            json!({"type":"local_shell_call","status":"completed","action":{}}),
        ),
        (
            "本地命令未知动作",
            "response_item",
            json!({"type":"local_shell_call","status":"completed","action":{"type":"future_action"}}),
        ),
        (
            "本地命令坏参数",
            "response_item",
            json!({"type":"local_shell_call","status":"completed","action":{"type":"exec","command":[7]}}),
        ),
        (
            "本地命令坏环境",
            "response_item",
            json!({"type":"local_shell_call","status":"completed","action":{"type":"exec","command":["echo"],"env":{"A":7}}}),
        ),
        (
            "本地命令坏状态",
            "response_item",
            json!({"type":"local_shell_call","status":"future","action":{"type":"exec","command":["echo"]}}),
        ),
        (
            "搜索缺动作类型",
            "response_item",
            json!({"type":"web_search_call","action":{}}),
        ),
        (
            "搜索未知动作",
            "response_item",
            json!({"type":"web_search_call","action":{"type":"future_action"}}),
        ),
        (
            "搜索坏queries",
            "response_item",
            json!({"type":"web_search_call","action":{"type":"search","queries":[7]}}),
        ),
        (
            "配置缺推理等级",
            "response_item",
            json!({"type":"configuration_update","reasoning":{}}),
        ),
        (
            "配置坏推理等级",
            "response_item",
            json!({"type":"configuration_update","reasoning":{"effort":7}}),
        ),
        (
            "函数调用坏密文参数",
            "response_item",
            json!({"type":"function_call","name":"exec","call_id":"c","arguments":"{}","encrypted_function_args":[7]}),
        ),
        (
            "消息图片坏精度",
            "response_item",
            json!({"type":"message","role":"assistant","content":[{"type":"input_image","file_id":"f","detail":7}]}),
        ),
        (
            "计划坏条目",
            "event_msg",
            json!({"type":"plan_update","plan":[7]}),
        ),
        (
            "计划坏状态",
            "event_msg",
            json!({"type":"plan_update","plan":[{"step":"x","status":"future"}]}),
        ),
        (
            "命令解释坏条目",
            "event_msg",
            json!({"type":"exec_command_begin","call_id":"c","turn_id":"轮","command":["echo"],"cwd":"D:\\w","parsed_cmd":[7]}),
        ),
        (
            "补丁坏变化",
            "event_msg",
            json!({"type":"patch_apply_begin","call_id":"c","auto_approved":true,"changes":{"D:\\x":7}}),
        ),
        (
            "搜索事件坏动作",
            "event_msg",
            json!({"type":"web_search_end","call_id":"c","query":"x","action":{}}),
        ),
        (
            "统计坏用量",
            "event_msg",
            json!({"type":"token_count","info":{"total_token_usage":7,"last_token_usage":{}}}),
        ),
    ] {
        let path = temp_file(tag);
        let mut text = task_started("2026-11-05T01:00:00.000Z", "原轮");
        text += &line("2026-11-05T01:00:01.000Z", kind, payload);
        text += &message("2026-11-05T01:00:02.000Z", "后续消费", "必须失去原轮锚点");
        append(&path, &text);
        let chunk = read_chunk(&path_str(&path), None).unwrap();
        assert_eq!(
            chunk.cursor.offset,
            text.len() as u64,
            "{tag}：完整行确已读完"
        );
        assert_eq!(chunk.entries.len(), 1, "{tag}：确实读到后续消费");
        let lost = matches!(
            &chunk.entries[0].record,
            RolloutRecord::Message { turn: None, .. }
        ) && chunk.entries[0].turn_lost
            && chunk.cursor.lost;
        if chunk.bad_lines != 1 || !lost {
            swallowed.push(format!("{tag}: bad_lines={}, lost={lost}", chunk.bad_lines));
        }
    }
    assert!(swallowed.is_empty(), "嵌套坏结构被吞掉：{swallowed:?}");
}

#[test]
fn gap_b_review1_合法嵌套输出含密文工具输出不失去轮锚() {
    for (tag, kind, payload) in [
        (
            "推理结构",
            "response_item",
            json!({"type":"reasoning","summary":[{"type":"summary_text","text":"摘要"}],"content":[{"type":"reasoning_text","text":"内容"},{"type":"text","text":"旧结构"}],"encrypted_content":"密文"}),
        ),
        (
            "混合工具输出",
            "response_item",
            json!({"type":"function_call_output","output":[{"type":"input_text","text":"结果"},{"type":"input_image","file_id":"f","detail":"original"},{"type":"input_audio","audio_url":"data:audio/wav;base64,A"},{"type":"encrypted_content","encrypted_content":"不透明结果"}]}),
        ),
        (
            "密文自定义工具输出",
            "response_item",
            json!({"type":"custom_tool_call_output","call_id":"c","output":[{"type":"encrypted_content","encrypted_content":"不透明结果"}]}),
        ),
        (
            "本地命令完整结构",
            "response_item",
            json!({"type":"local_shell_call","status":"completed","action":{"type":"exec","command":["echo","x"],"timeout_ms":100,"working_directory":"D:\\w","env":{"A":"B"},"user":null}}),
        ),
        (
            "搜索完整结构",
            "response_item",
            json!({"type":"web_search_call","action":{"type":"search","query":"x","queries":["x","y"]}}),
        ),
        (
            "打开页面可选字段",
            "response_item",
            json!({"type":"web_search_call","action":{"type":"open_page"}}),
        ),
        (
            "自定义推理等级",
            "response_item",
            json!({"type":"configuration_update","reasoning":{"effort":"custom_future_effort"}}),
        ),
        (
            "工具定义任意json",
            "response_item",
            json!({"type":"additional_tools","role":"developer","tools":[7,{"future_tool_schema":true}]}),
        ),
        (
            "工具搜索任意json",
            "response_item",
            json!({"type":"tool_search_call","execution":"client","arguments":[7,{"future_argument":true}]}),
        ),
        (
            "工具搜索输出任意json",
            "response_item",
            json!({"type":"tool_search_output","status":"completed","execution":"client","tools":[7,{"future_tool_schema":true}]}),
        ),
        (
            "计划结构",
            "event_msg",
            json!({"type":"plan_update","plan":[{"step":"x","status":"in_progress"}]}),
        ),
    ] {
        let path = temp_file(tag);
        let mut text = task_started("2026-11-05T01:00:00.000Z", "原轮");
        text += &line("2026-11-05T01:00:01.000Z", kind, payload);
        text += &message("2026-11-05T01:00:02.000Z", "后续消费", "仍属于原轮");
        append(&path, &text);
        let chunk = read_chunk(&path_str(&path), None).unwrap();
        assert_eq!(chunk.cursor.offset, text.len() as u64, "{tag}");
        assert_eq!(chunk.bad_lines, 0, "{tag}：合法嵌套字段不能误报坏行");
        assert_eq!(chunk.entries.len(), 1, "{tag}");
        assert!(
            matches!(&chunk.entries[0].record, RolloutRecord::Message { turn:Some(t), .. } if t == "原轮"),
            "{tag}"
        );
        assert!(!chunk.cursor.lost, "{tag}");
    }
}

fn gap_b_review2_exec(status: &str) -> Value {
    json!({"type":"exec_command_end","call_id":"c","turn_id":"原轮","command":["echo"],
        "cwd":"D:\\w","parsed_cmd":[],"stdout":"","stderr":"","exit_code":0,
        "formatted_output":"","duration":{"secs":0,"nanos":1},"status":status})
}

fn gap_b_review2_patch(status: &str) -> Value {
    json!({"type":"patch_apply_end","call_id":"c","stdout":"","stderr":"","success":true,"status":status})
}

#[test]
fn gap_b_review2_闭合枚举和强类型字段坏值真实读取不得静默吞掉() {
    let mut cases = vec![
        ("命令未知终态", "event_msg", gap_b_review2_exec("future")),
        ("补丁未知终态", "event_msg", gap_b_review2_patch("future")),
        (
            "中止缺原因",
            "event_msg",
            json!({"type":"turn_aborted","turn_id":"原轮"}),
        ),
        (
            "中止未知原因",
            "event_msg",
            json!({"type":"turn_aborted","turn_id":"原轮","reason":"future"}),
        ),
        (
            "开始未知协作模式",
            "event_msg",
            json!({"type":"task_started","turn_id":"新轮","collaboration_mode_kind":"future"}),
        ),
        (
            "开始默认枚举不能null",
            "event_msg",
            json!({"type":"task_started","turn_id":"新轮","collaboration_mode_kind":null}),
        ),
        (
            "输出未知阶段",
            "response_item",
            json!({"type":"message","role":"assistant","content":[],"phase":"future"}),
        ),
        (
            "输出事件未知阶段",
            "event_msg",
            json!({"type":"agent_message","message":"x","phase":"future"}),
        ),
        (
            "增量缺线程轮身份",
            "event_msg",
            json!({"type":"agent_message_content_delta","item_id":"i","delta":"x"}),
        ),
        (
            "搜索结果容器坏类型",
            "event_msg",
            json!({"type":"web_search_end","call_id":"c","query":"x","action":{"type":"search"},"results":7}),
        ),
        (
            "错误枚举坏类型",
            "event_msg",
            json!({"type":"error","message":"x","codex_error_info":7}),
        ),
        (
            "错误枚举载荷坏类型",
            "event_msg",
            json!({"type":"error","message":"x","codex_error_info":{"http_connection_failed":{"http_status_code":"坏值"}}}),
        ),
        (
            "额度未知触顶枚举",
            "event_msg",
            json!({"type":"token_count","rate_limits":{"rate_limit_reached_type":"future"}}),
        ),
        (
            "完成显示消息坏类型",
            "event_msg",
            json!({"type":"task_complete","turn_id":"原轮","last_agent_message":7}),
        ),
        (
            "压缩替换历史非数组",
            "compacted",
            json!({"message":"摘要","replacement_history":7}),
        ),
        (
            "压缩替换历史坏条目",
            "compacted",
            json!({"message":"摘要","replacement_history":[7]}),
        ),
    ];
    let mut exec_source = gap_b_review2_exec("completed");
    exec_source["source"] = json!("future");
    cases.push(("命令未知来源", "event_msg", exec_source));
    let mut exec_source_null = gap_b_review2_exec("completed");
    exec_source_null["source"] = Value::Null;
    cases.push(("命令默认来源不能null", "event_msg", exec_source_null));
    let mut wide_exit = gap_b_review2_exec("completed");
    wide_exit["exit_code"] = json!(i64::MAX);
    cases.push(("退出码超i32", "event_msg", wide_exit));
    let mut null_changes = gap_b_review2_patch("completed");
    null_changes["changes"] = Value::Null;
    cases.push(("默认变化映射不能null", "event_msg", null_changes));
    cases.push(("推理增量默认索引坏类型", "event_msg", json!({"type":"reasoning_content_delta","thread_id":"线程","turn_id":"原轮","item_id":"i","delta":"x","summary_index":"坏值"})));
    cases.push((
        "命令字节流不是Base64",
        "event_msg",
        json!({"type":"exec_command_output_delta","call_id":"c","stream":"stdout","chunk":"%%%"}),
    ));
    let mut bad_time = gap_b_review2_exec("completed");
    bad_time["completed_at_ms"] = Value::Null;
    cases.push(("命令默认时间不能null", "event_msg", bad_time));
    let mut bad_patch_turn = gap_b_review2_patch("completed");
    bad_patch_turn["turn_id"] = json!(7);
    cases.push(("补丁默认轮号坏类型", "event_msg", bad_patch_turn));
    let mut swallowed = vec![];
    for (tag, kind, payload) in cases {
        let path = temp_file(tag);
        let mut text = task_started("2026-11-05T01:00:00.000Z", "原轮");
        text += &line("2026-11-05T01:00:01.000Z", kind, payload);
        text += &message("2026-11-05T01:00:02.000Z", "后续", "不能沿用锚点");
        append(&path, &text);
        let chunk = read_chunk(&path_str(&path), None).unwrap();
        assert_eq!(
            chunk.cursor.offset,
            text.len() as u64,
            "{tag}：完整内容确已消费"
        );
        assert_eq!(chunk.entries.len(), 1, "{tag}：确有后续消费");
        let lost = matches!(
            &chunk.entries[0].record,
            RolloutRecord::Message { turn: None, .. }
        ) && chunk.entries[0].turn_lost
            && chunk.cursor.lost;
        if chunk.bad_lines != 1 || !lost {
            swallowed.push(format!("{tag}: bad={},lost={lost}", chunk.bad_lines));
        }
    }
    assert!(
        swallowed.is_empty(),
        "强类型坏字段被当无关吞掉：{swallowed:?}"
    );
}

#[test]
fn gap_b_review2_全部合法终态及闭合枚举与不透明json对照() {
    let mut cases = vec![];
    for status in ["completed", "failed", "declined"] {
        cases.push(("命令终态", "event_msg", gap_b_review2_exec(status), false));
        cases.push(("补丁终态", "event_msg", gap_b_review2_patch(status), false));
    }
    for source in [
        "agent",
        "user_shell",
        "unified_exec_startup",
        "unified_exec_interaction",
    ] {
        let mut payload = gap_b_review2_exec("completed");
        payload["source"] = json!(source);
        cases.push(("命令来源", "event_msg", payload, false));
    }
    for reason in ["interrupted", "replaced", "review_ended", "budget_limited"] {
        cases.push((
            "中止原因",
            "event_msg",
            json!({"type":"turn_aborted","turn_id":"原轮","reason":reason}),
            true,
        ));
    }
    for mode in [
        "plan",
        "default",
        "code",
        "pair_programming",
        "execute",
        "custom",
    ] {
        cases.push((
            "协作模式及历史别名",
            "event_msg",
            json!({"type":"task_started","turn_id":"原轮","collaboration_mode_kind":mode}),
            false,
        ));
    }
    for phase in ["commentary", "final_answer"] {
        cases.push((
            "输出阶段",
            "response_item",
            json!({"type":"message","role":"assistant","content":[],"phase":phase}),
            false,
        ));
        cases.push((
            "输出事件阶段",
            "event_msg",
            json!({"type":"agent_message","message":"x","phase":phase}),
            false,
        ));
    }
    for reached in [
        "rate_limit_reached",
        "workspace_owner_credits_depleted",
        "workspace_member_credits_depleted",
        "workspace_owner_usage_limit_reached",
        "workspace_member_usage_limit_reached",
    ] {
        cases.push((
            "额度枚举",
            "event_msg",
            json!({"type":"token_count","rate_limits":{"rate_limit_reached_type":reached}}),
            false,
        ));
    }
    cases.extend([
        ("命令字节流Base64", "event_msg", json!({"type":"exec_command_output_delta","call_id":"c","stream":"stdout","chunk":"eA=="}), false),
        ("推理增量默认索引", "event_msg", json!({"type":"reasoning_content_delta","thread_id":"线程","turn_id":"原轮","item_id":"i","delta":"x","summary_index":0}), false),
        ("合法增量", "event_msg", json!({"type":"agent_message_content_delta","thread_id":"线程","turn_id":"原轮","item_id":"i","delta":"x"}), false),
        ("搜索结果任意json", "event_msg", json!({"type":"web_search_end","call_id":"c","query":"x","action":{"type":"search"},"results":[7,{"future":true}]}), false),
        ("合法错误枚举", "event_msg", json!({"type":"error","message":"x","codex_error_info":"context_window_exceeded"}), false),
        ("错误未知变体有上游Other兜底", "event_msg", json!({"type":"error","message":"x","codex_error_info":{"future_error":{"arbitrary":7}}}), false),
        ("合法错误结构载荷", "event_msg", json!({"type":"error","message":"x","codex_error_info":{"http_connection_failed":{"http_status_code":503}}}), false),
        ("压缩替换历史", "compacted", json!({"message":"摘要","replacement_history":[{"type":"reasoning","summary":[]},{"type":"message","role":"assistant","content":[]}]}), false),
        ("世界状态任意json", "world_state", json!({"full":true,"state":{"future":7,"another":[null]}}), false),
        ("工具状态为开放字符串", "response_item", json!({"type":"image_generation_call","status":"future_provider_status","result":"x"}), false),
    ]);
    for (tag, kind, payload, boundary) in cases {
        let path = temp_file(tag);
        let mut text = task_started("2026-11-05T01:00:00.000Z", "原轮");
        text += &line("2026-11-05T01:00:01.000Z", kind, payload);
        text += &message("2026-11-05T01:00:02.000Z", "后续", "合法记录后的消费");
        append(&path, &text);
        let chunk = read_chunk(&path_str(&path), None).unwrap();
        assert_eq!(chunk.cursor.offset, text.len() as u64, "{tag}");
        assert_eq!(chunk.bad_lines, 0, "{tag}：全部合法变体均不能误报");
        assert_eq!(chunk.entries.len(), 1, "{tag}");
        assert!(!chunk.cursor.lost, "{tag}");
        match &chunk.entries[0].record {
            RolloutRecord::Message { turn, .. } if boundary => assert!(turn.is_none(), "{tag}"),
            RolloutRecord::Message { turn, .. } => {
                assert_eq!(turn.as_deref(), Some("原轮"), "{tag}")
            }
            _ => panic!("{tag}：应读到消费"),
        }
    }
}

#[test]
fn gap_b_review2_文件与raw共用已声明可选字段校验() {
    for (tag, payload) in [
        (
            "元数据非对象",
            json!({"type":"reasoning","summary":[],"internal_chat_message_metadata_passthrough":7}),
        ),
        (
            "元数据轮身份",
            json!({"type":"reasoning","summary":[],"internal_chat_message_metadata_passthrough":{"turn_id":7}}),
        ),
        (
            "普通输出阶段",
            json!({"type":"message","role":"assistant","content":[],"phase":7}),
        ),
        (
            "工具命名空间",
            json!({"type":"function_call","name":"exec","call_id":"c","arguments":"{}","namespace":7}),
        ),
        (
            "工具密文参数",
            json!({"type":"function_call","name":"exec","call_id":"c","arguments":"{}","encrypted_function_args":[7]}),
        ),
        (
            "输出名字",
            json!({"type":"function_call_output","output":"x","name":7}),
        ),
        (
            "输出命名空间",
            json!({"type":"function_call_output","output":"x","namespace":7}),
        ),
        (
            "自定义输出名字",
            json!({"type":"custom_tool_call_output","call_id":"c","output":"x","name":7}),
        ),
        (
            "自定义调用状态",
            json!({"type":"custom_tool_call","call_id":"c","name":"tool","input":"x","status":7}),
        ),
        (
            "生成图像提示词",
            json!({"type":"image_generation_call","status":"completed","result":"x","revised_prompt":7}),
        ),
    ] {
        let path = temp_file(tag);
        let mut text = task_started("2026-11-05T01:00:00.000Z", "原轮");
        text += &line("2026-11-05T01:00:01.000Z", "response_item", payload);
        text += &message("2026-11-05T01:00:02.000Z", "后续", "不能沿用锚点");
        append(&path, &text);
        let chunk = read_chunk(&path_str(&path), None).unwrap();
        assert_eq!(chunk.cursor.offset, text.len() as u64, "{tag}");
        assert_eq!(chunk.bad_lines, 1, "{tag}");
        assert!(
            matches!(
                &chunk.entries[0].record,
                RolloutRecord::Message { turn: None, .. }
            ),
            "{tag}"
        );
        assert!(chunk.cursor.lost, "{tag}");
    }
    for payload in [
        json!({"type":"reasoning","summary":[],"internal_chat_message_metadata_passthrough":{"turn_id":null,"create_time":"私有字段不验证","content_item_kinds":7}}),
        json!({"type":"function_call_output","output":[{"type":"encrypted_content","encrypted_content":"x"}],"name":null,"namespace":"n","internal_chat_message_metadata_passthrough":null}),
        json!({"type":"image_generation_call","status":"future","result":"x","revised_prompt":null}),
    ] {
        let path = temp_file("optional-good");
        let mut text = task_started("2026-11-05T01:00:00.000Z", "原轮");
        text += &line("2026-11-05T01:00:01.000Z", "response_item", payload);
        text += &message("2026-11-05T01:00:02.000Z", "后续", "仍有锚点");
        append(&path, &text);
        let chunk = read_chunk(&path_str(&path), None).unwrap();
        assert_eq!(chunk.bad_lines, 0);
        assert_eq!(chunk.cursor.turn.as_deref(), Some("原轮"));
    }
}

#[test]
fn gap_b_review2_base64带填充与无填充同样可解释且拒绝坏尾位() {
    for (encoded, valid) in [
        ("", true),
        ("eA", true),
        ("eA==", true),
        ("eHk", true),
        ("eHk=", true),
        ("eHl6", true),
        ("e", false),
        ("eA=", false),
        ("eA===", false),
        ("=eA=", false),
        ("%%%", false),
        ("eB", false),
        ("eB==", false),
        ("eHl", false),
        ("eHl=", false),
    ] {
        let path = temp_file("base64-padding");
        let mut text = task_started("2026-11-05T01:00:00.000Z", "原轮");
        text += &line(
            "2026-11-05T01:00:01.000Z",
            "event_msg",
            json!({"type":"exec_command_output_delta",
            "call_id":"c","stream":"stdout","chunk":encoded}),
        );
        text += &message("2026-11-05T01:00:02.000Z", "后续", "记录");
        append(&path, &text);
        let chunk = read_chunk(&path_str(&path), None).unwrap();
        assert_eq!(
            chunk.cursor.offset,
            text.len() as u64,
            "{encoded:?}：完整行确实被读取"
        );
        assert_eq!(chunk.entries.len(), 1, "{encoded:?}");
        assert_eq!(chunk.bad_lines, u32::from(!valid), "{encoded:?}");
        assert_eq!(chunk.cursor.lost, !valid, "{encoded:?}");
        match &chunk.entries[0].record {
            RolloutRecord::Message { turn, .. } => {
                assert_eq!(turn.as_deref(), valid.then_some("原轮"), "{encoded:?}")
            }
            _ => panic!("预期后续消费"),
        }
    }
}
