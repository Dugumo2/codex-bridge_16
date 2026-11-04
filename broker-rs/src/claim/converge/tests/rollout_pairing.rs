//! 没有 raw 的 v2 跟进按会话文件配对：真实 step、真实临时文件、生产同一个读取函数（`test_support::rollout`）。
//!
//! 父线程从不送 raw（跨重启 `--resume` 的线程）。文件内容按上游实测形状写：
//! 父线程文件在工具执行前写入 `function_call`，子线程文件在消费时写入 `agent_message`，
//! 行所在的轮由之前最近的 `task_started` 决定。断言只写设计保证的行为。
use super::causal_pairing::{
    assert_no_alarm, base, delivery, drive_run, finish, op, pending, raw, C,
};
use super::placeholder::replies;
use super::*;
use crate::relations::rollout_pairing::{ROLLOUT_POLL_MS, ROLLOUT_WAIT_MS};
use crate::test_support::rollout as fx;

/// 安装夹具并建好父、子线程的会话文件：父线程当前轮已开始，子线程「旧轮」已开始。
fn files(m: &Machine) {
    fx::install();
    fx::create(P);
    fx::create(C);
    let at = m.clock.now_ms - 5_000;
    fx::append_line(P, at, "session_meta", json!({"id":P,"cwd":"C:\\w"}));
    fx::append_line(
        P,
        at,
        "event_msg",
        json!({"type":"task_started","turn_id":T}),
    );
    fx::append_line(C, at, "session_meta", json!({"id":C}));
    turn_marker(C, "旧轮", at + 1);
}

fn turn_marker(thread: &str, turn: &str, at: u64) {
    fx::append_line(
        thread,
        at,
        "turn_context",
        json!({"turn_id":turn,"cwd":"C:\\w"}),
    );
    fx::append_line(
        thread,
        at,
        "event_msg",
        json!({"type":"task_started","turn_id":turn}),
    );
}

/// 父线程文件里的协作工具调用（工具执行前写入）。
fn call_line(m: &Machine, id: &str, name: &str, target: &str, message: &str) {
    fx::append_line(
        P,
        m.clock.now_ms,
        "response_item",
        json!({"type":"function_call","name":name,"namespace":"collaboration","call_id":id,
            "arguments":json!({"target":target,"message":message}).to_string()}),
    );
}

/// 子线程文件里收到的消息（与 raw 的 agent_message 同形）。
fn message_line(m: &Machine, child: &str, id: &str, kind: &str, message: &str) {
    let text = format!(
        "Message Type: {kind}\nTask name: /root/{child}\nSender: /root\nPayload:\n{message}"
    );
    fx::append_line(
        child,
        m.clock.now_ms,
        "response_item",
        json!({"type":"agent_message","id":id,"author":"/root","recipient":format!("/root/{child}"),
            "content":[{"type":"input_text","text":text}]}),
    );
}

fn interacted_to(m: &mut Machine, id: &str, child: &str) -> Vec<Action> {
    let (a, b) = SubagentMock::v2_activity("interacted", P, T, id, child);
    let mut out = apply(m, a);
    out.extend(apply(m, b));
    out
}

/// 推进时钟走完轮询，直到没有在等的读取或超过给定时长。
fn poll_for(m: &mut Machine, mock: &SubagentMock, ms: u64) -> Vec<Action> {
    let mut all = vec![];
    let mut spent = 0;
    while spent < ms {
        let out = fx::poll(m);
        all.extend(replies(m, mock, out));
        spent += ROLLOUT_POLL_MS;
    }
    all
}

fn awaiting(m: &Machine, id: &str) -> bool {
    m.relations
        .aux
        .rollout
        .waits
        .keys()
        .any(|k| k.item_id == id)
}

fn no_lingering_poll(m: &mut Machine, mock: &SubagentMock) {
    assert!(m.relations.aux.rollout.waits.is_empty(), "不留等文件的操作");
    assert!(m.relations.aux.rollout.timer.is_none(), "不留轮询计时");
    let reads = fx::with(|f| f.reads);
    let out = poll_for(m, mock, 2 * ROLLOUT_POLL_MS);
    assert!(!fx::touches_rollout(&out), "不再读文件");
    assert_eq!(fx::with(|f| f.reads), reads);
}

// ─── 1. followup_task 给空闲子代理 ─────────────────────────────────

#[test]
fn 无raw_跟进空闲子代理按文件配到新轮并零误报收尾() {
    let (mut m, mut mock) = base();
    files(&m);
    let mut all = complete(&mut m, &mut mock, C, "旧轮", "completed");
    call_line(&m, "跟进", "followup_task", C, "新工作");
    all.extend(interacted_to(&mut m, "跟进", C));
    // 父线程文件已有调用、子线程文件已读通：当场定为严格，来源为会话文件。
    let o = op(&m, "跟进");
    assert!(o.v2_strict && !o.v2_queue_only);
    assert_eq!(o.v2_source, V2EvidenceSource::RolloutFile);
    assert_eq!(o.v2_fallback, None);
    pending(&m, 1);
    // 子代理读到跟进开新轮：消费记录记在新轮。
    m.clock.now_ms += 20;
    turn_marker(C, "新轮", m.clock.now_ms);
    message_line(&m, C, "入站", "NEW_TASK", "新工作");
    let actions = apply(&mut m, mock.turn_started(C, "新轮"));
    all.extend(replies(&mut m, &mock, actions));
    all.extend(poll_for(&mut m, &mock, ROLLOUT_POLL_MS));
    pending(&m, 0);
    assert_eq!(op(&m, "跟进").proven_turns[C], ["新轮".to_owned()].into());
    let actions = complete(&mut m, &mut mock, C, "新轮", "completed");
    all.extend(replies(&mut m, &mock, actions));
    all.extend(finish(&mut m, &mut mock));
    assert_no_alarm(&all);
    no_lingering_poll(&mut m, &mock);
}

// ─── 2. send_message 给空闲子代理 ──────────────────────────────────

#[test]
fn 无raw_留言给空闲子代理只入队不占账本且运行不卡() {
    let (mut m, mut mock) = base();
    files(&m);
    let mut all = complete(&mut m, &mut mock, C, "旧轮", "completed");
    call_line(&m, "留言", "send_message", C, "顺便说一句");
    all.extend(interacted_to(&mut m, "留言", C));
    let o = op(&m, "留言");
    assert!(o.v2_strict && o.v2_queue_only, "认出 send_message");
    assert!(o.receivers.is_empty(), "只入队不授予管理权");
    pending(&m, 0);
    // 子代理空闲、文件里也没有记录：运行照常收尾，不等收敛期限。
    all.extend(finish(&mut m, &mut mock));
    assert_no_alarm(&all);
    no_lingering_poll(&mut m, &mock);
}

// ─── 3. 被忙碌轮吸收 ───────────────────────────────────────────────

#[test]
fn 无raw_跟进被忙碌轮吸收时配到当前轮() {
    let (mut m, mut mock) = base();
    files(&m);
    let original = m.binding(C, "旧轮").unwrap();
    call_line(&m, "跟进", "followup_task", C, "补充要求");
    let mut all = interacted_to(&mut m, "跟进", C);
    assert!(op(&m, "跟进").v2_strict);
    pending(&m, 1);
    // 消费记录晚些才记到同一轮（实测 11.5 秒后）。
    m.clock.now_ms += 11_500;
    message_line(&m, C, "入站", "NEW_TASK", "补充要求");
    all.extend(poll_for(&mut m, &mock, ROLLOUT_POLL_MS));
    pending(&m, 0);
    assert_eq!(op(&m, "跟进").proven_turns[C], ["旧轮".to_owned()].into());
    assert_eq!(m.binding(C, "旧轮").unwrap(), original, "不改写原绑定");
    all.extend(complete(&mut m, &mut mock, C, "旧轮", "completed"));
    all.extend(finish(&mut m, &mut mock));
    assert_no_alarm(&all);
}

// ─── 4. 答完后跟进、顺延到下一轮 ───────────────────────────────────

#[test]
fn 无raw_子代理答完后跟进顺延到下一轮_不配上一轮且上一轮结束后继续等() {
    let (mut m, mut mock) = base();
    files(&m);
    // 子代理已给出最终回答，旧轮尚未结束：宽松模式此刻会认旧轮。
    call_line(&m, "跟进", "followup_task", C, "再做一件");
    let mut all = interacted_to(&mut m, "跟进", C);
    assert!(op(&m, "跟进").v2_strict);
    assert!(op(&m, "跟进").proven_turns.is_empty(), "不认旧轮");
    let actions = complete(&mut m, &mut mock, C, "旧轮", "completed");
    all.extend(replies(&mut m, &mock, actions));
    all.extend(poll_for(&mut m, &mock, ROLLOUT_POLL_MS));
    pending(&m, 1);
    // 父轮也结束：运行进入收敛但必须继续等这次跟进。
    let actions = complete(&mut m, &mut mock, P, T, "completed");
    all.extend(replies(&mut m, &mock, actions));
    all.extend(drive_run(&mut m, &mock, R));
    assert_eq!(m.runs.runs[R].state, RunState::Running);
    assert!(m.convergences.progress[R].reading.is_none(), "不能收尾");
    pending(&m, 1);
    // 上一轮结束后开下一轮，消费记录记在下一轮。
    m.clock.now_ms += 50;
    turn_marker(C, "下一轮", m.clock.now_ms);
    message_line(&m, C, "入站", "NEW_TASK", "再做一件");
    let actions = apply(&mut m, mock.turn_started(C, "下一轮"));
    all.extend(replies(&mut m, &mock, actions));
    all.extend(poll_for(&mut m, &mock, ROLLOUT_POLL_MS));
    pending(&m, 0);
    assert_eq!(op(&m, "跟进").proven_turns[C], ["下一轮".to_owned()].into());
    let actions = complete(&mut m, &mut mock, C, "下一轮", "completed");
    all.extend(replies(&mut m, &mock, actions));
    all.extend(finish(&mut m, &mut mock));
    assert_no_alarm(&all);
}

// ─── 5. 相同正文两次吸收到同一轮 ───────────────────────────────────

#[test]
fn 无raw_同正文两次被同一忙碌轮吸收时一一核销() {
    let (mut m, mut mock) = base();
    files(&m);
    call_line(&m, "A", "followup_task", C, "同一正文");
    let mut all = interacted_to(&mut m, "A", C);
    m.clock.now_ms += 10;
    m.clock.mono_ms += 10;
    call_line(&m, "B", "followup_task", C, "同一正文");
    all.extend(interacted_to(&mut m, "B", C));
    pending(&m, 2);
    m.clock.now_ms += 1_000;
    message_line(&m, C, "入站1", "NEW_TASK", "同一正文");
    all.extend(poll_for(&mut m, &mock, ROLLOUT_POLL_MS));
    pending(&m, 1);
    m.clock.now_ms += 1_000;
    message_line(&m, C, "入站2", "NEW_TASK", "同一正文");
    all.extend(poll_for(&mut m, &mock, ROLLOUT_POLL_MS));
    pending(&m, 0);
    all.extend(complete(&mut m, &mut mock, C, "旧轮", "completed"));
    all.extend(finish(&mut m, &mut mock));
    assert_no_alarm(&all);
}

// ─── 6. 恢复后新派子代理：文件开头抄有父线程历史 ───────────────────

#[test]
fn 无raw_新派子代理文件开头的父线程历史不当作消费记录() {
    const N: &str = "新子";
    let (mut m, mut mock) = base();
    files(&m);
    let mut all = complete(&mut m, &mut mock, C, "旧轮", "completed");
    spawn(&mut m, &mut mock, P, T, N, "派生新子", "新子首轮");
    fx::create(N);
    // 抄来的父线程历史：别的轮次的开始与结束、父线程自己收到的消息、一条同正文的旧消息。
    let copied = m.clock.now_ms - 2_000;
    fx::append_line(N, copied, "session_meta", json!({"id":N}));
    for turn in ["父历史一", "父历史二"] {
        fx::append_line(
            N,
            copied,
            "event_msg",
            json!({"type":"task_started","turn_id":turn}),
        );
        fx::append_line(
            N,
            copied,
            "event_msg",
            json!({"type":"task_complete","turn_id":turn}),
        );
    }
    let old = "Message Type: NEW_TASK\nTask name: /root/新子\nSender: /root\nPayload:\n同一正文";
    fx::append_line(
        N,
        copied,
        "response_item",
        json!({"type":"agent_message","id":"抄来的","author":"/root","recipient":"/root/新子",
            "content":[{"type":"input_text","text":old}]}),
    );
    fx::append_line(
        N,
        copied,
        "response_item",
        json!({"type":"agent_message","id":"父收到的","author":"/root/子","recipient":"/root",
            "content":[{"type":"input_text","text":"Message Type: MESSAGE\nTask name: /root\nSender: /root/子\nPayload:\n同一正文"}]}),
    );
    turn_marker(N, "新子首轮", copied + 1);
    all.extend(complete(&mut m, &mut mock, N, "新子首轮", "completed"));
    // 跟进新子代理：抄来的同正文旧消息早于这次调用，不能被核销，也不能让整组作废。
    call_line(&m, "跟进新子", "followup_task", N, "同一正文");
    all.extend(interacted_to(&mut m, "跟进新子", N));
    assert!(op(&m, "跟进新子").v2_strict);
    assert!(op(&m, "跟进新子").proven_turns.is_empty());
    m.clock.now_ms += 30;
    turn_marker(N, "新子二轮", m.clock.now_ms);
    message_line(&m, N, "真正入站", "NEW_TASK", "同一正文");
    let actions = apply(&mut m, mock.turn_started(N, "新子二轮"));
    all.extend(replies(&mut m, &mock, actions));
    all.extend(poll_for(&mut m, &mock, ROLLOUT_POLL_MS));
    pending(&m, 0);
    assert_eq!(
        op(&m, "跟进新子").proven_turns[N],
        ["新子二轮".to_owned()].into()
    );
    let actions = complete(&mut m, &mut mock, N, "新子二轮", "completed");
    all.extend(replies(&mut m, &mock, actions));
    all.extend(finish(&mut m, &mut mock));
    assert_no_alarm(&all);
}

// ─── 7. 文件不可用：退回宽松，结果与原来相同 ───────────────────────

/// 宽松回退下空闲子代理的跟进：配到之后的唯一新轮，正常收尾（与改动前的行为相同）。
fn fallback_completes(m: &mut Machine, mock: &mut SubagentMock, id: &str) -> Vec<Action> {
    let mut all = vec![];
    let actions = apply(m, mock.turn_started(C, "新轮"));
    all.extend(replies(m, mock, actions));
    pending(m, 0);
    assert_eq!(op(m, id).proven_turns[C], ["新轮".to_owned()].into());
    let actions = complete(m, mock, C, "新轮", "completed");
    all.extend(replies(m, mock, actions));
    all.extend(finish(m, mock));
    all
}

#[test]
fn 无raw_路径拿不到_不合规_文件缺失时退回宽松且有原因() {
    for case in [
        "无path",
        "请求失败",
        "相对路径",
        "别的线程",
        "不是jsonl",
        "文件缺失",
        "未装夹具",
    ] {
        fx::uninstall();
        let (mut m, mut mock) = base();
        let mut all = complete(&mut m, &mut mock, C, "旧轮", "completed");
        if case != "未装夹具" {
            files(&m);
            call_line(&m, "跟进", "followup_task", C, "新工作");
            let path = fx::path(P);
            let reply = match case {
                "无path" => Some(Ok(json!({"thread":{"id":P}}))),
                "请求失败" => Some(Err(json!({"code":-32600,"message":"thread not found"}))),
                "相对路径" => Some(Ok(
                    json!({"thread":{"id":P,"path":format!("rollout-{P}.jsonl")}}),
                )),
                "别的线程" => Some(Ok(
                    json!({"thread":{"id":P,"path":path.replace(P, "别人")}}),
                )),
                "不是jsonl" => Some(Ok(json!({"thread":{"id":P,"path":format!("{path}.bak")}}))),
                "文件缺失" => {
                    std::fs::remove_file(&path).unwrap();
                    None
                }
                _ => unreachable!(),
            };
            if let Some(reply) = reply {
                fx::with(|f| f.replies.insert(P.into(), reply));
            }
        }
        all.extend(interacted_to(&mut m, "跟进", C));
        let o = op(&m, "跟进");
        assert!(!awaiting(&m, "跟进"), "{case}：文件不可用时当场定案");
        assert!(!o.v2_strict, "{case}");
        assert_eq!(o.v2_source, V2EvidenceSource::None, "{case}");
        let expected = if case == "文件缺失" {
            V2FallbackReason::RolloutUnreadable
        } else {
            V2FallbackReason::RolloutPathUnavailable
        };
        assert_eq!(o.v2_fallback, Some(expected), "{case}");
        pending(&m, 1);
        all.extend(fallback_completes(&mut m, &mut mock, "跟进"));
        assert_no_alarm(&all);
    }
}

#[test]
fn 无raw_半行等下次读_坏行与未知字段不崩溃() {
    let (mut m, mut mock) = base();
    files(&m);
    let mut all = complete(&mut m, &mut mock, C, "旧轮", "completed");
    // 坏行、未知类型、带未知字段的无关行先写进去；调用行只写了一半。
    fx::append_raw(P, "这不是 JSON\n");
    fx::append_line(P, m.clock.now_ms, "未知类型", json!({"x":1}));
    let line = json!({"timestamp": fx::timestamp(m.clock.now_ms), "type":"response_item","新字段":1,
        "payload":{"type":"function_call","name":"followup_task","namespace":"collaboration",
        "call_id":"跟进","arguments":json!({"target":C,"message":"新工作"}).to_string(),"新字段":2}})
    .to_string();
    let (head, tail) = line.split_at(line.len() / 2);
    fx::append_raw(P, head);
    all.extend(interacted_to(&mut m, "跟进", C));
    assert!(awaiting(&m, "跟进"), "半行不算调用，等下一次读");
    // 等文件期间：诊断标明中间态，不给回退原因，不按终态报警。
    let row = m.convergence_diagnostics(R).unwrap()["pendingOperations"][0].clone();
    assert_eq!(row["pairingMode"], "awaitingRollout");
    assert!(row["fallbackReason"].is_null() && row["pairingSource"].is_null());
    assert!(!crate::relations::convergence::settled(
        &m.convergence_facts(R).unwrap()
    ));
    fx::append_raw(P, &format!("{tail}\n"));
    all.extend(poll_for(&mut m, &mock, ROLLOUT_POLL_MS));
    assert!(!awaiting(&m, "跟进"));
    assert!(op(&m, "跟进").v2_strict);
    m.clock.now_ms += 20;
    turn_marker(C, "新轮", m.clock.now_ms);
    message_line(&m, C, "入站", "NEW_TASK", "新工作");
    let actions = apply(&mut m, mock.turn_started(C, "新轮"));
    all.extend(replies(&mut m, &mock, actions));
    all.extend(poll_for(&mut m, &mock, ROLLOUT_POLL_MS));
    pending(&m, 0);
    let actions = complete(&mut m, &mut mock, C, "新轮", "completed");
    all.extend(replies(&mut m, &mock, actions));
    all.extend(finish(&mut m, &mut mock));
    assert_no_alarm(&all);
}

#[test]
fn 无raw_调用缺字段时不按严格配对_退回宽松() {
    let (mut m, mut mock) = base();
    files(&m);
    let mut all = complete(&mut m, &mut mock, C, "旧轮", "completed");
    // arguments 缺 message：没有可比的摘要，按严格配对只会卡住。
    fx::append_line(
        P,
        m.clock.now_ms,
        "response_item",
        json!({"type":"function_call","name":"followup_task","call_id":"跟进",
            "arguments":json!({"target":C}).to_string()}),
    );
    all.extend(interacted_to(&mut m, "跟进", C));
    let o = op(&m, "跟进");
    assert!(!o.v2_strict);
    assert_eq!(o.v2_fallback, Some(V2FallbackReason::RolloutCallMissing));
    all.extend(fallback_completes(&mut m, &mut mock, "跟进"));
    assert_no_alarm(&all);
}

#[test]
fn 无raw_子线程文件读不了时退回宽松() {
    let (mut m, mut mock) = base();
    files(&m);
    let mut all = complete(&mut m, &mut mock, C, "旧轮", "completed");
    std::fs::remove_file(fx::path(C)).unwrap();
    call_line(&m, "跟进", "followup_task", C, "新工作");
    all.extend(interacted_to(&mut m, "跟进", C));
    let o = op(&m, "跟进");
    assert!(!o.v2_strict);
    assert_eq!(o.v2_fallback, Some(V2FallbackReason::RolloutUnreadable));
    all.extend(fallback_completes(&mut m, &mut mock, "跟进"));
    assert_no_alarm(&all);
}

#[test]
fn 无raw_严格定案后子线程文件被截断时降为宽松不卡() {
    let (mut m, mut mock) = base();
    files(&m);
    let mut all = complete(&mut m, &mut mock, C, "旧轮", "completed");
    call_line(&m, "跟进", "followup_task", C, "新工作");
    all.extend(interacted_to(&mut m, "跟进", C));
    assert!(op(&m, "跟进").v2_strict);
    std::fs::OpenOptions::new()
        .write(true)
        .open(fx::path(C))
        .unwrap()
        .set_len(1)
        .unwrap();
    all.extend(poll_for(&mut m, &mock, ROLLOUT_POLL_MS));
    let o = op(&m, "跟进");
    assert!(!o.v2_strict);
    assert_eq!(o.v2_fallback, Some(V2FallbackReason::RolloutLost));
    all.extend(fallback_completes(&mut m, &mut mock, "跟进"));
    assert_no_alarm(&all);
}

// ─── 8. 窗口内找不到调用 ───────────────────────────────────────────

#[test]
fn 无raw_窗口内找不到调用时定为宽松_迟到的行不改写模式() {
    let (mut m, mut mock) = base();
    files(&m);
    let mut all = complete(&mut m, &mut mock, C, "旧轮", "completed");
    all.extend(interacted_to(&mut m, "跟进", C));
    assert!(awaiting(&m, "跟进"));
    // 窗口期间照常阻止收敛：父轮结束也不能收尾。
    let actions = complete(&mut m, &mut mock, P, T, "completed");
    all.extend(replies(&mut m, &mock, actions));
    assert!(m.convergences.progress[R].reading.is_none());
    all.extend(poll_for(&mut m, &mock, ROLLOUT_WAIT_MS));
    assert!(!awaiting(&m, "跟进"));
    let o = op(&m, "跟进");
    assert!(!o.v2_strict);
    assert_eq!(o.v2_fallback, Some(V2FallbackReason::RolloutCallMissing));
    // 迟到的调用行：已定的模式不变，也不再读文件。
    call_line(&m, "跟进", "followup_task", C, "新工作");
    let reads = fx::with(|f| f.reads);
    all.extend(poll_for(&mut m, &mock, 2 * ROLLOUT_POLL_MS));
    assert!(!op(&m, "跟进").v2_strict);
    assert_eq!(fx::with(|f| f.reads), reads);
    pending(&m, 1);
    let actions = apply(&mut m, mock.turn_started(C, "新轮"));
    all.extend(replies(&mut m, &mock, actions));
    let actions = complete(&mut m, &mut mock, C, "新轮", "completed");
    all.extend(replies(&mut m, &mock, actions));
    all.extend(finish(&mut m, &mut mock));
    assert_no_alarm(&all);
}

// ─── 9. 消费记录先于开始记录被读到 ─────────────────────────────────

#[test]
fn 无raw_消费记录先于开始记录被读到仍能配对() {
    let (mut m, mut mock) = base();
    files(&m);
    let mut all = complete(&mut m, &mut mock, C, "旧轮", "completed");
    // Interacted 先到，父线程文件里还没有调用；子线程已经开新轮并记下消费记录。
    let call_wall = m.clock.now_ms;
    m.clock.now_ms += 5;
    turn_marker(C, "新轮", m.clock.now_ms);
    message_line(&m, C, "入站", "NEW_TASK", "新工作");
    all.extend(interacted_to(&mut m, "跟进", C));
    let actions = apply(&mut m, mock.turn_started(C, "新轮"));
    all.extend(replies(&mut m, &mock, actions));
    assert!(awaiting(&m, "跟进"));
    assert!(
        m.relations.aux.v2_deliveries.iter().any(|d| d.id == "入站"),
        "消费记录已先读到"
    );
    // 调用行写入时间在消费之前，只是读得晚。
    fx::append_line(
        P,
        call_wall,
        "response_item",
        json!({"type":"function_call","name":"followup_task","call_id":"跟进",
            "arguments":json!({"target":C,"message":"新工作"}).to_string()}),
    );
    all.extend(poll_for(&mut m, &mock, ROLLOUT_POLL_MS));
    assert!(op(&m, "跟进").v2_strict);
    pending(&m, 0);
    assert_eq!(op(&m, "跟进").proven_turns[C], ["新轮".to_owned()].into());
    let actions = complete(&mut m, &mut mock, C, "新轮", "completed");
    all.extend(replies(&mut m, &mock, actions));
    all.extend(finish(&mut m, &mut mock));
    assert_no_alarm(&all);
}

// ─── 10. 运行结束、停止、连接或授权换代 ────────────────────────────

#[test]
fn 无raw_授权或连接换代与停止后不再读文件且无残留轮询() {
    for case in ["授权换代", "连接换代", "停止", "读取在途时换代"] {
        let (mut m, mut mock) = base();
        files(&m);
        complete(&mut m, &mut mock, C, "旧轮", "completed");
        if case == "读取在途时换代" {
            fx::with(|f| f.hold = true);
        }
        interacted_to(&mut m, "跟进", C);
        assert!(awaiting(&m, "跟进"), "{case}");
        match case {
            "授权换代" | "读取在途时换代" => {
                m.relations.contexts.get_mut(R).unwrap().gen += 1
            }
            "连接换代" => m.relations.conn_epoch += 1,
            "停止" => {
                apply(
                    &mut m,
                    Event::ClientRequest {
                        request: ClientRequest {
                            conn: 1,
                            id: json!(99),
                            method: "broker/threads/stop".into(),
                            params: Some(json!({"threadId":P,"clear":false})),
                        },
                        run_id: None,
                    },
                );
            }
            _ => unreachable!(),
        }
        let reads = fx::with(|f| f.reads);
        let held = fx::with(|f| f.held.len()) as u64;
        let out = fx::poll(&mut m);
        assert!(!awaiting(&m, "跟进"), "{case}");
        assert!(
            !fx::touches_rollout(&out)
                || case == "停止" && !out.iter().any(|a| matches!(a, Action::ReadRollout(_))),
            "{case}"
        );
        if case == "读取在途时换代" {
            let late = fx::release_held(&mut m);
            assert!(!late.iter().any(|a| matches!(a, Action::ReadRollout(_))));
        }
        // 在途的读取照常完成，结果被丢弃；此后不再发新的读取。
        assert_eq!(fx::with(|f| f.reads), reads + held, "{case}");
        if case != "连接换代" {
            assert_eq!(
                op(&m, "跟进").v2_fallback,
                Some(V2FallbackReason::RolloutCancelled),
                "{case}"
            );
        }
        no_lingering_poll(&mut m, &mock);
    }
}

#[test]
fn 无raw_配对后与运行结束后都停止跟随子线程文件() {
    let (mut m, mut mock) = base();
    files(&m);
    complete(&mut m, &mut mock, C, "旧轮", "completed");
    call_line(&m, "跟进", "followup_task", C, "新工作");
    interacted_to(&mut m, "跟进", C);
    assert!(
        m.relations.aux.rollout.timer.is_some(),
        "等消费记录时轮询子线程文件"
    );
    m.clock.now_ms += 20;
    turn_marker(C, "新轮", m.clock.now_ms);
    message_line(&m, C, "入站", "NEW_TASK", "新工作");
    let actions = apply(&mut m, mock.turn_started(C, "新轮"));
    replies(&mut m, &mock, actions);
    poll_for(&mut m, &mock, ROLLOUT_POLL_MS);
    pending(&m, 0);
    // 配对后不再需要文件：计时撤销，之后不再读。
    no_lingering_poll(&mut m, &mock);
    let actions = complete(&mut m, &mut mock, C, "新轮", "completed");
    replies(&mut m, &mock, actions);
    finish(&mut m, &mut mock);
    no_lingering_poll(&mut m, &mock);
}

// ─── 11. 大文件 ────────────────────────────────────────────────────

#[test]
fn 无raw_大文件只读末尾窗口与新增内容() {
    let (mut m, mut mock) = base();
    files(&m);
    complete(&mut m, &mut mock, C, "旧轮", "completed");
    // 父线程文件前面是 1 GiB 的空洞（稀疏文件），末尾才是本轮内容。
    for thread in [P, C] {
        let path = fx::path(thread);
        let len = std::fs::metadata(&path).unwrap().len();
        let content = std::fs::read(&path).unwrap();
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(0).unwrap();
        f.set_len(1 << 30).unwrap();
        drop(f);
        fx::append_raw(thread, "\n");
        fx::append_raw(thread, std::str::from_utf8(&content).unwrap());
        assert!(len < 4096);
    }
    call_line(&m, "跟进", "followup_task", C, "新工作");
    interacted_to(&mut m, "跟进", C);
    assert!(op(&m, "跟进").v2_strict);
    m.clock.now_ms += 20;
    turn_marker(C, "新轮", m.clock.now_ms);
    message_line(&m, C, "入站", "NEW_TASK", "新工作");
    let actions = apply(&mut m, mock.turn_started(C, "新轮"));
    replies(&mut m, &mock, actions);
    poll_for(&mut m, &mock, ROLLOUT_POLL_MS);
    pending(&m, 0);
    let (reads, bytes) = fx::with(|f| (f.reads, f.bytes));
    assert!(reads >= 3);
    assert!(
        bytes <= 2 * (crate::rollout::TAIL_BYTES + 1) + 64 * 1024,
        "读取字节有界：{bytes}"
    );
}

// ─── 12. 有 raw 的线程一次都不读 ───────────────────────────────────

#[test]
fn 有raw的线程零次文件读取_行为不变() {
    let (mut m, mut mock) = base();
    files(&m);
    let mut all = complete(&mut m, &mut mock, C, "旧轮", "completed");
    raw(&mut m, "跟进", "新工作");
    all.extend(interacted_to(&mut m, "跟进", C));
    assert_eq!(op(&m, "跟进").v2_source, V2EvidenceSource::Raw);
    let actions = apply(&mut m, mock.turn_started(C, "新轮"));
    all.extend(replies(&mut m, &mock, actions));
    delivery(&mut m, "新轮", "入站", "新工作", false);
    pending(&m, 0);
    let actions = complete(&mut m, &mut mock, C, "新轮", "completed");
    all.extend(replies(&mut m, &mock, actions));
    all.extend(finish(&mut m, &mut mock));
    assert!(
        !fx::touches_rollout(&all),
        "有 raw 时不发路径请求、不读文件、不排轮询"
    );
    assert_eq!(fx::with(|f| (f.path_reads, f.reads)), (0, 0));
    assert_eq!(m.relations.aux.rollout, Default::default());
    assert_no_alarm(&all);
}

#[test]
fn 父线程无raw而子线程有raw时只读父线程文件() {
    let (mut m, mut mock) = base();
    files(&m);
    let mut all = complete(&mut m, &mut mock, C, "旧轮", "completed");
    // 子线程送过 raw：它的消费记录只认 raw。
    notify(
        &mut m,
        "rawResponseItem/completed",
        json!({"threadId":C,"turnId":"旧轮","item":{"type":"reasoning","summary":[]}}),
    );
    call_line(&m, "跟进", "followup_task", C, "新工作");
    all.extend(interacted_to(&mut m, "跟进", C));
    assert!(op(&m, "跟进").v2_strict);
    let actions = apply(&mut m, mock.turn_started(C, "新轮"));
    all.extend(replies(&mut m, &mock, actions));
    delivery(&mut m, "新轮", "入站", "新工作", false);
    pending(&m, 0);
    assert!(
        fx::with(|f| f.log.iter().all(|(t, _)| t == P)),
        "子线程文件一次都不读"
    );
    let actions = complete(&mut m, &mut mock, C, "新轮", "completed");
    all.extend(replies(&mut m, &mock, actions));
    all.extend(finish(&mut m, &mut mock));
    assert_no_alarm(&all);
}

#[test]
fn 有raw的父线程缺开始记录时也不读文件_照原样宽松() {
    let (mut m, mut mock) = base();
    files(&m);
    let mut all = complete(&mut m, &mut mock, C, "旧轮", "completed");
    // 父线程送过 raw，这次调用的开始记录没赶上：仍是原来的 startMissing 宽松，不改用会话文件。
    raw(&mut m, "B", "第二任务");
    all.extend(interacted_to(&mut m, "没有开始的A", C));
    assert_eq!(
        op(&m, "没有开始的A").v2_fallback,
        Some(V2FallbackReason::StartMissing)
    );
    all.extend(interacted_to(&mut m, "B", C));
    let actions = apply(&mut m, mock.turn_started(C, "不明承载轮"));
    all.extend(replies(&mut m, &mock, actions));
    delivery(&mut m, "不明承载轮", "B输入", "第二任务", false);
    pending(&m, 0);
    all.extend(complete(&mut m, &mut mock, C, "不明承载轮", "completed"));
    all.extend(finish(&mut m, &mut mock));
    assert!(!fx::touches_rollout(&all));
    assert_eq!(fx::with(|f| (f.path_reads, f.reads)), (0, 0));
    assert_eq!(m.relations.aux.rollout, Default::default());
}
