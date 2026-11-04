//! 会话文件读取：真实文件、真实偏移，断言读取量有界、半行与坏行的处理、路径校验。
use super::*;
use serde_json::json;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};

const THREAD: &str = "019a0000-0000-7000-8000-00000000abcd";

fn temp_file(tag: &str) -> std::path::PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "cxb-rollout-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(format!("rollout-2026-11-05T01-02-03-{tag}-{THREAD}.jsonl"))
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
    // 缺时间戳的消息是坏行；未知顶层类型与未知字段照常忽略。
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
    assert_eq!(chunk.bad_lines, 3);
    assert_eq!(chunk.entries.len(), 1);
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
    assert!(
        matches!(&entries[0].record, RolloutRecord::Message { turn: Some(t), .. } if t == "轮")
    );
}

/// 大文件用稀疏文件：前面是大段空洞，末尾才有内容；首次只读末尾窗口，跟随只读新增。
#[test]
fn 大文件只读末尾窗口与新增内容_读取字节有界() {
    let path = temp_file("sparse");
    let hole = 1u64 << 30;
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
