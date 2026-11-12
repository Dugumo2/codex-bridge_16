//! 通过真实通知入口验证连续观察，测试不直接填入 raw 见证。
use super::*;
use crate::runs::event_log::CoverageDisposition;

const SEED: &str = "01900000-0000-7000-8000-000000000001";
const FIRST: &str = "01900000-0001-7000-8000-000000000001";
const SECOND: &str = "01900000-0002-7000-8000-000000000001";

fn notify(m: &mut Machine, method: &str, params: Value) {
    let (next, _) = crate::claim::step(
        std::mem::take(m),
        Event::Notification {
            method: method.into(),
            params: Some(params),
        },
    );
    *m = next;
}

fn start(m: &mut Machine, turn: &str) {
    notify(
        m,
        "turn/started",
        json!({"threadId":C,"turn":{"id":turn,"status":"inProgress"}}),
    );
}

fn complete(m: &mut Machine, turn: &str) {
    notify(
        m,
        "turn/completed",
        json!({"threadId":C,"turn":{"id":turn,"status":"completed","error":null}}),
    );
}

fn raw(m: &mut Machine, turn: &str, item: Value) {
    notify(
        m,
        "rawResponseItem/completed",
        json!({"threadId":C,"turnId":turn,"item":item}),
    );
}

fn reasoning(m: &mut Machine, turn: &str) {
    raw(m, turn, json!({"type":"reasoning","summary":[]}));
}

fn delivery(id: &str, body: &str) -> Value {
    json!({"type":"agent_message","id":id,"author":"/root/甲","recipient":"/root/乙",
        "content":[{"type":"input_text","text":format!("Message Type: MESSAGE\nTask name: /root/乙\nSender: /root/甲\nPayload:\n{body}")}]})
}

fn seeded() -> Machine {
    let mut m = Machine::new(false);
    start(&mut m, SEED);
    assert!(
        m.relations
            .aux
            .live_turn_starts
            .contains_key(&(C.into(), SEED.into())),
        "前置：真实开始进入原始锚点"
    );
    reasoning(&mut m, SEED);
    complete(&mut m, SEED);
    assert_eq!(
        m.coverage_observation_gen(C),
        Some(0),
        "前置：完整种子轮没有断档"
    );
    m
}

fn open(m: &mut Machine) {
    start(m, FIRST);
    assert!(
        m.coverage_open_proof(C, FIRST).is_some(),
        "前置：先验 raw 建立开放证明"
    );
    raw(m, FIRST, delivery("协作一", "内容"));
    assert_eq!(m.coverage_disposition(C, FIRST), CoverageDisposition::Wait);
}

#[test]
fn gap_b_observation_无断档真实通知连续两轮可免报() {
    let mut m = seeded();
    for turn in [FIRST, SECOND] {
        start(&mut m, turn);
        assert!(m.coverage_open_proof(C, turn).is_some());
        raw(&mut m, turn, delivery("同id跨轮", "内容"));
        notify(
            &mut m,
            "rawResponse/completed",
            json!({"threadId":C,"turnId":turn}),
        );
        assert_eq!(
            m.coverage_disposition(C, turn),
            CoverageDisposition::Wait,
            "rawResponse 不能冒充轮结束"
        );
        complete(&mut m, turn);
        assert_eq!(
            m.coverage_disposition(C, turn),
            CoverageDisposition::AllowCollaborationExclusion
        );
    }
}

#[test]
fn gap_b_observation_无开始或发现开始的raw不能授信() {
    for discovered in [false, true] {
        let mut m = Machine::new(false);
        if discovered {
            m.observe_turn(C, SEED, true, false, &mut Vec::new());
        }
        reasoning(&mut m, SEED);
        assert!(!m
            .relations
            .aux
            .live_turn_starts
            .contains_key(&(C.into(), SEED.into())));
        start(&mut m, FIRST);
        assert!(m.coverage_open_proof(C, FIRST).is_none());
        reasoning(&mut m, FIRST);
        start(&mut m, FIRST);
        assert!(
            m.coverage_open_proof(C, FIRST).is_none(),
            "本轮raw及重复started不得追认"
        );
    }
}

#[test]
fn gap_b_observation_无活动轮的未知缺口永久封锁未来() {
    for scoped in [false, true] {
        let mut m = seeded();
        let params = if scoped {
            json!({"threadId":C})
        } else {
            json!({})
        };
        notify(&mut m, "unknown/lifecycle", params);
        assert_eq!(m.coverage_observation_gen(C), None);
        start(&mut m, FIRST);
        reasoning(&mut m, FIRST);
        complete(&mut m, FIRST);
        start(&mut m, SECOND);
        assert!(m.coverage_open_proof(C, SECOND).is_none());
    }
}

#[test]
fn gap_b_observation_合法全局通知不撤销未知或坏生命周期会撤销() {
    let mut m = seeded();
    open(&mut m);
    for method in [
        "account/rateLimits/updated",
        "account/updated",
        "item/agentMessage/delta",
        "thread/name/updated",
    ] {
        notify(&mut m, method, json!({}));
        assert!(
            m.coverage_open_proof(C, FIRST).is_some(),
            "合法无关通知：{method}"
        );
    }
    notify(&mut m, "thread/status/changed", json!({"threadId":C}));
    assert!(m.coverage_open_proof(C, FIRST).is_none());
    assert_eq!(m.coverage_observation_gen(C), None);
}

#[test]
fn gap_b_observation_raw坏身份和内容均撤销但已知无关条目不撤销() {
    let invalid = [
        json!({"turnId":FIRST,"item":{"type":"reasoning","summary":[]}}),
        json!({"threadId":C,"item":{"type":"reasoning","summary":[]}}),
        json!({"threadId":C,"turnId":FIRST}),
        json!({"threadId":C,"turnId":FIRST,"item":{"type":"future_input"}}),
        json!({"threadId":C,"turnId":FIRST,"item":{"type":"agent_message","id":"坏头","author":"/root/甲","recipient":"/root/乙","content":[{"type":"input_text","text":"坏消息头"}]}}),
        json!({"threadId":"x".repeat(257),"turnId":FIRST,"item":{"type":"reasoning","summary":[]}}),
    ];
    for params in invalid {
        let mut m = seeded();
        open(&mut m);
        reasoning(&mut m, FIRST);
        assert!(m.coverage_open_proof(C, FIRST).is_some());
        notify(&mut m, "rawResponseItem/completed", params);
        assert!(m.coverage_open_proof(C, FIRST).is_none());
        assert_eq!(m.coverage_observation_gen(C), None);
    }
}

#[test]
fn gap_b_observation_退订重附着和旧raw均不能恢复但封存结果保留() {
    for finished in [false, true] {
        let mut m = seeded();
        open(&mut m);
        if finished {
            complete(&mut m, FIRST);
        }
        m.detach_evidence(C);
        m.attach(C, AttachmentSource::ExplicitResume);
        assert!(m.relations.attachments[C].valid, "前置：确实重新附着");
        reasoning(&mut m, SEED);
        start(&mut m, FIRST);
        if finished {
            complete(&mut m, FIRST);
            raw(&mut m, FIRST, delivery("协作一", "内容"));
            assert_eq!(
                m.coverage_disposition(C, FIRST),
                CoverageDisposition::AllowCollaborationExclusion,
                "一致重复完成和精确重复消费保持封存"
            );
        } else {
            assert_eq!(
                m.coverage_disposition(C, FIRST),
                CoverageDisposition::OrdinaryGap
            );
        }
        start(&mut m, SECOND);
        assert!(m.coverage_open_proof(C, SECOND).is_none());
    }
}

#[test]
fn gap_b_observation_完成后新增消费或矛盾完成否决并封锁() {
    for conflict in [false, true] {
        let mut m = seeded();
        open(&mut m);
        complete(&mut m, FIRST);
        assert_eq!(
            m.coverage_disposition(C, FIRST),
            CoverageDisposition::AllowCollaborationExclusion
        );
        if conflict {
            notify(
                &mut m,
                "turn/completed",
                json!({"threadId":C,"turn":{"id":FIRST,"status":"failed"}}),
            );
        } else {
            raw(&mut m, FIRST, delivery("协作一", "冲突内容"));
        }
        assert_eq!(
            m.coverage_disposition(C, FIRST),
            CoverageDisposition::OrdinaryGap
        );
        assert_eq!(m.coverage_observation_gen(C), None);
    }
}

#[test]
fn gap_b_observation_逆序完成及完成后的raw不制造资格() {
    let mut m = Machine::new(false);
    complete(&mut m, SEED);
    start(&mut m, SEED);
    reasoning(&mut m, SEED);
    start(&mut m, FIRST);
    assert!(m.coverage_open_proof(C, FIRST).is_none());
    let mut m = Machine::new(false);
    start(&mut m, SEED);
    complete(&mut m, SEED);
    reasoning(&mut m, SEED);
    start(&mut m, FIRST);
    assert!(m.coverage_open_proof(C, FIRST).is_none());
}

#[test]
fn gap_b_observation_raw表满不淘汰封锁标记且既有干净线程继续() {
    let mut m = seeded();
    notify(&mut m, "unknown/lifecycle", json!({"threadId":"永久封锁"}));
    for i in 0..MAX_OPERATIONS - 2 {
        m.clock.mono_ms += 1;
        notify(
            &mut m,
            "thread/name/updated",
            json!({"threadId":C,"threadName":"保留观察"}),
        );
        notify(
            &mut m,
            "turn/started",
            json!({"threadId":format!("容量{i}"),"turn":{"id":SEED,"status":"inProgress"}}),
        );
    }
    assert_eq!(m.relations.aux.raw_observations.len(), MAX_OPERATIONS);
    m.clock.mono_ms += 1;
    notify(
        &mut m,
        "thread/name/updated",
        json!({"threadId":C,"threadName":"保留观察"}),
    );
    // 大量轮锚点可能触发自己的上限；只验证未淘汰线程的先验段。
    assert!(matches!(
        m.relations.aux.raw_observations.get("永久封锁"),
        Some(crate::relations::coverage_observation::RawObservation::Blocked { .. })
    ));
    notify(
        &mut m,
        "turn/started",
        json!({"threadId":"满额新线程","turn":{"id":FIRST,"status":"inProgress"}}),
    );
    assert!(!m.relations.aux.raw_observations.contains_key("满额新线程"));
    assert_eq!(m.relations.aux.raw_observations.len(), MAX_OPERATIONS);
    assert!(
        m.relations.attachments[C].valid,
        "前置：目标附着凭据没有被其他容量挤出"
    );
    assert!(
        m.relations
            .aux
            .live_turn_starts
            .contains_key(&(C.into(), SEED.into())),
        "前置：目标真实开始仍在"
    );
    assert_eq!(
        m.coverage_observation_gen(C),
        Some(0),
        "前置：目标观察段仍干净"
    );
    start(&mut m, FIRST);
    assert!(
        m.coverage_open_proof(C, FIRST).is_some(),
        "已登记干净线程不因raw表满失去先验资格"
    );
}

#[test]
fn gap_b_observation_已知条目必须结构完整且合法工具输出不误撤销() {
    let mut m = seeded();
    open(&mut m);
    for item in [
        json!({"type":"function_call_output","output":"完成"}),
        json!({"type":"custom_tool_call","call_id":"调用","name":"工具","input":"参数"}),
        json!({"type":"web_search_call","status":"completed","action":{"type":"search","query":"内容"}}),
        json!({"type":"context_compaction"}),
        json!({"type":"tool_search_output","execution":"client","status":"completed","tools":[]}),
    ] {
        raw(&mut m, FIRST, item);
        assert!(
            m.coverage_open_proof(C, FIRST).is_some(),
            "已核对协议的无关输出不撤销"
        );
    }
    notify(
        &mut m,
        "item/completed",
        json!({"threadId":C,"turnId":FIRST,"item":{"type":"agentMessage","id":"缺正文"}}),
    );
    assert!(
        m.coverage_open_proof(C, FIRST).is_none(),
        "只认type/id会吞掉不完整结构"
    );
    assert_eq!(m.coverage_observation_gen(C), None);
}

#[test]
fn gap_b_observation_冷恢复无raw和热恢复均不得授信() {
    for hot in [false, true] {
        let mut m = if hot { seeded() } else { Machine::new(false) };
        assert!(
            m.thread_relation(C).is_none(),
            "前置：覆盖没有血缘的订阅路径"
        );
        // 真实目标通知维持观察责任，避免成功恢复立即被空闲补偿退订遮盖。
        notify(
            &mut m,
            "thread/goal/updated",
            json!({"threadId":C,"goal":{"status":"active","objective":"持续观察"}}),
        );
        assert!(m.records[C].goal_active, "前置：确有持续观察责任");
        let context = RequestContext {
            resume_retried: false,
            stream_id: None,
            review_id: None,
            client: None,
            token: None,
            operation: Operation::ChildSubscription {
                thread: C.into(),
                op: 77,
            },
            method: "thread/resume".into(),
            params: Some(json!({"threadId":C,"excludeTurns":true})),
        };
        let mut actions = vec![Action::Request(context.clone())];
        m.child_subscriptions_after_step(&mut actions);
        assert_eq!(m.coverage_observation_gen(C), None, "发出resume前即封锁");
        m.child_register_subscription(&context, &mut actions);
        let (next, _) = crate::claim::step(
            m,
            Event::Reply {
                context,
                result: Ok(json!({"thread":{"id":C,"status":{"type":"idle"},"turns":[]}})),
            },
        );
        m = next;
        assert!(
            m.relations.attachments[C].valid,
            "前置：真实订阅回包恢复附着"
        );
        start(&mut m, FIRST);
        reasoning(&mut m, FIRST);
        complete(&mut m, FIRST);
        start(&mut m, SECOND);
        assert!(m.coverage_open_proof(C, SECOND).is_none());
    }
}

#[test]
fn gap_b_observation_开放轮开始锚点淘汰永久封锁而已完成轮保留() {
    let mut m = seeded();
    open(&mut m);
    for i in 3..=MAX_OBSERVED_TURNS + 3 {
        let turn = format!("01900000-{i:04x}-7000-8000-000000000001");
        start(&mut m, &turn);
        complete(&mut m, &turn);
    }
    assert!(
        !m.relations
            .aux
            .live_turn_starts
            .contains_key(&(C.into(), FIRST.into())),
        "前置：真实开始锚点被观察表挤出"
    );
    assert_eq!(m.coverage_observation_gen(C), None);
    assert!(m.coverage_open_proof(C, FIRST).is_none());
}

#[test]
fn gap_b_observation_无在途票据的模拟换代也永久封锁() {
    let mut m = seeded();
    assert!(
        m.relations.aux.rollout.coverage_reads.is_empty(),
        "前置：不能靠票据退休检测换代"
    );
    m.relations.conn_epoch += 1;
    start(&mut m, FIRST);
    reasoning(&mut m, FIRST);
    complete(&mut m, FIRST);
    start(&mut m, SECOND);
    assert!(m.relations.aux.raw_globally_blocked);
    assert!(m.coverage_open_proof(C, SECOND).is_none());
}

#[test]
fn gap_b_observation_种子轮已否决仍须识别畸形完成并封锁() {
    let mut m = Machine::new(false);
    start(&mut m, SEED);
    reasoning(&mut m, SEED);
    assert!(
        m.coverage_open_proof(C, SEED).is_none(),
        "前置：种子轮无先验见证而否决"
    );
    notify(
        &mut m,
        "turn/completed",
        json!({"threadId":C,"turn":{"id":SEED,"status":"futureStatus"}}),
    );
    assert_eq!(m.coverage_observation_gen(C), None);
    start(&mut m, FIRST);
    assert!(m.coverage_open_proof(C, FIRST).is_none());
}

#[test]
fn gap_b_observation_换代后仅计时器也先撤销封存证明() {
    let mut m = seeded();
    open(&mut m);
    complete(&mut m, FIRST);
    assert_eq!(
        m.coverage_disposition(C, FIRST),
        CoverageDisposition::AllowCollaborationExclusion
    );
    m.relations.conn_epoch += 1;
    let (next, _) = crate::claim::step(
        m,
        Event::Timer {
            token: Machine::relation_token(),
            kind: TimerKind::Run(crate::runs::RunTimer::EventLogSweep { generation: 999 }),
        },
    );
    assert!(next.relations.aux.raw_globally_blocked);
    assert_eq!(
        next.coverage_disposition(C, FIRST),
        CoverageDisposition::OrdinaryGap
    );
}

#[test]
fn gap_b_observation_合法活动标记保留覆盖而未知或坏状态撤销() {
    for flags in [
        json!([]),
        json!(["waitingOnApproval", "waitingOnUserInput"]),
    ] {
        let mut m = seeded();
        open(&mut m);
        notify(
            &mut m,
            "thread/status/changed",
            json!({"threadId":C,"status":{"type":"active","activeFlags":flags}}),
        );
        assert!(m.coverage_open_proof(C, FIRST).is_some());
    }
    for status in [
        json!({"type":"pendingInit"}),
        json!({"type":"active"}),
        json!({"type":"active","activeFlags":[7]}),
        json!({"type":"active","activeFlags":["futureState"]}),
    ] {
        let mut m = seeded();
        open(&mut m);
        notify(
            &mut m,
            "thread/status/changed",
            json!({"threadId":C,"status":status}),
        );
        assert!(m.coverage_open_proof(C, FIRST).is_none());
        assert_eq!(m.coverage_observation_gen(C), None);
    }
}

// 实现审查第一轮复现：仅通过真实通知入口穿插合法输出，不手工改覆盖状态。
fn gap_b_review1_output_sequence(method: &str, item: Value) {
    let mut m = seeded();
    open(&mut m);
    let start = m
        .coverage_open_proof(C, FIRST)
        .cloned()
        .expect("前置：先验raw、真实开始和纯协作消费已开放");
    assert_eq!(m.coverage_observation_gen(C), Some(0));
    assert_eq!(m.coverage_disposition(C, FIRST), CoverageDisposition::Wait);
    notify(
        &mut m,
        method,
        json!({"threadId":C,"turnId":FIRST,"item":item}),
    );
    assert_eq!(
        m.coverage_open_proof(C, FIRST),
        Some(&start),
        "合法输出不得改变开放证明：{method}"
    );
    assert_eq!(
        m.coverage_observation_gen(C),
        Some(0),
        "合法输出不得封锁未来授信：{method}"
    );
    complete(&mut m, FIRST);
    assert_eq!(
        m.coverage_disposition(C, FIRST),
        CoverageDisposition::AllowCollaborationExclusion,
        "合法完成仍可协作免报"
    );
    assert_eq!(m.coverage_observation_gen(C), Some(0));
}

#[test]
fn gap_b_review1_合法协作工具全部枚举不封锁() {
    for tool in [
        "spawnAgent",
        "sendInput",
        "resumeAgent",
        "wait",
        "closeAgent",
        "sendMessage",
        "followupTask",
        "interruptAgent",
        "listAgents",
    ] {
        for method in ["item/started", "item/completed"] {
            gap_b_review1_output_sequence(
                method,
                json!({"type":"collabAgentToolCall","id":format!("合法工具{tool}"),
                "tool":tool,"status":if method=="item/started" {"inProgress"} else {"completed"},"senderThreadId":C,"receiverThreadIds":[],
                "prompt":null,"model":null,"reasoningEffort":null,"agentsStates":{}}),
            );
        }
    }
}

#[test]
fn gap_b_review1_合法协作工具中断状态不封锁() {
    for status in ["inProgress", "completed", "failed", "interrupted"] {
        gap_b_review1_output_sequence(
            if status == "inProgress" {
                "item/started"
            } else {
                "item/completed"
            },
            json!({"type":"collabAgentToolCall","id":"工具状态",
            "tool":"wait","status":status,"senderThreadId":C,"receiverThreadIds":[],
            "prompt":null,"model":null,"reasoningEffort":null,"agentsStates":{}}),
        );
    }
}

#[test]
fn gap_b_review1_合法web_search没有status也保留覆盖() {
    for action in [
        Value::Null,
        json!({"type":"search","query":"问题","queries":null}),
        json!({"type":"openPage","url":"https://example.invalid/"}),
        json!({"type":"findInPage","url":null,"pattern":"关键字"}),
        json!({"type":"other"}),
    ] {
        for results in [
            Value::Null,
            json!([]),
            json!([{"providerSpecific":"不透明结果"}]),
        ] {
            gap_b_review1_output_sequence(
                "item/completed",
                json!({"type":"webSearch","id":"搜索",
                "query":"问题","action":action,"results":results}),
            );
        }
    }
    gap_b_review1_output_sequence(
        "item/started",
        json!({"type":"webSearch","id":"搜索开始","query":"问题"}),
    );
}

#[test]
fn gap_b_review1_合法sleep只需duration_ms不要求status() {
    for duration in [0_u64, 125, 60_000, u64::MAX] {
        for method in ["item/started", "item/completed"] {
            gap_b_review1_output_sequence(
                method,
                json!({"type":"sleep","id":"等待","durationMs":duration}),
            );
        }
    }
}

#[test]
fn gap_b_review1_合法加密工具输出不当成未知输入() {
    for output in [
        json!([{"type":"encrypted_content","encrypted_content":"密文"}]),
        json!([{"type":"input_text","text":"结果"},{"type":"input_image","file_id":"图片"},
            {"type":"input_audio","audio_url":"data:audio/wav;base64,AA=="},
            {"type":"encrypted_content","encrypted_content":"密文"}]),
    ] {
        for item_type in ["function_call_output", "custom_tool_call_output"] {
            gap_b_review1_output_sequence(
                "rawResponseItem/completed",
                json!({"type":item_type,
                "call_id":"工具调用","output":output}),
            );
        }
        gap_b_review1_output_sequence(
            "item/completed",
            json!({"type":"functionCallOutput","id":"展示工具输出",
            "name":"工具","namespace":null,"output":output}),
        );
    }
}

#[test]
fn gap_b_review1_合法raw搜索other动作保留覆盖() {
    for action in [
        Value::Null,
        json!({"type":"search","query":"问题"}),
        json!({"type":"other"}),
    ] {
        gap_b_review1_output_sequence(
            "rawResponseItem/completed",
            json!({"type":"web_search_call","action":action}),
        );
    }
}

#[test]
fn gap_b_review1_现有合法输出对照仍保留完整覆盖() {
    for item in [
        json!({"type":"agentMessage","id":"输出","text":"正文"}),
        json!({"type":"reasoning","id":"推理","summary":["摘要"],"content":[]}),
        json!({"type":"plan","id":"计划","text":"安排"}),
        json!({"type":"imageGeneration","id":"图片","status":"completed","result":"图片结果","revisedPrompt":null}),
        json!({"type":"functionCallOutput","id":"明文工具","name":"工具","output":[{"type":"input_text","text":"结果"}]}),
    ] {
        gap_b_review1_output_sequence("item/completed", item);
    }
    gap_b_review1_output_sequence(
        "rawResponseItem/completed",
        json!({"type":"reasoning","summary":[{"type":"summary_text","text":"摘要"}]}),
    );
}
#[test]
fn gap_b_review1_raw系统和开发者输入不是无关输出() {
    for role in ["system", "developer"] {
        let mut m = seeded();
        open(&mut m);
        raw(
            &mut m,
            FIRST,
            json!({"type":"message","role":role,"content":[{"type":"input_text","text":"新增输入约束"}]}),
        );
        assert!(
            m.coverage_open_proof(C, FIRST).is_none(),
            "输入角色{role}必须否决纯协作结论"
        );
        assert_eq!(m.coverage_observation_gen(C), None, "保守缺口封锁未来授信");
        complete(&mut m, FIRST);
        assert_eq!(
            m.coverage_disposition(C, FIRST),
            CoverageDisposition::OrdinaryGap
        );
    }
    gap_b_review1_output_sequence(
        "rawResponseItem/completed",
        json!({"type":"message","role":"assistant","content":[{"type":"output_text","text":"合法输出"}]}),
    );
}
#[test]
fn gap_b_review1_attachment真实淘汰撤销开放轮并永久封锁() {
    let mut m = seeded();
    open(&mut m);
    assert!(m.relations.attachments[C].valid);
    assert_eq!(m.coverage_observation_gen(C), Some(0));
    let observed = m.relations.aux.observed.len();
    let starts = m.relations.aux.coverage_starts.len();
    let raw_threads = m.relations.aux.raw_observations.len();
    assert!(
        observed < MAX_OBSERVED_THREADS && starts < MAX_OPERATIONS && raw_threads < MAX_OPERATIONS
    );
    // 只用明确无关的真实名称通知创建附着，不登记其他线程的开始/raw/覆盖条目。
    for index in 0..MAX_ATTACHMENTS {
        m.clock.mono_ms += 1;
        notify(
            &mut m,
            "thread/name/updated",
            json!({"threadId":format!("附着容量{index}"),"threadName":"已核对名称"}),
        );
    }
    assert_eq!(
        m.relations.attachments.len(),
        MAX_ATTACHMENTS,
        "前置：确实触达附着容量上限"
    );
    assert!(
        !m.relations.attachments.contains_key(C),
        "前置：目标附着确实被生产淘汰路径删除"
    );
    assert_eq!(
        m.relations.aux.observed.len(),
        observed,
        "没有触发观察表淘汰"
    );
    assert_eq!(
        m.relations.aux.coverage_starts.len(),
        starts,
        "没有触发开始锚点淘汰"
    );
    assert_eq!(
        m.relations.aux.raw_observations.len(),
        raw_threads,
        "没有触发raw表上限"
    );
    assert!(
        m.relations
            .aux
            .live_turn_starts
            .contains_key(&(C.into(), FIRST.into())),
        "真实开始仍存在，独立锁定附着丢失分支"
    );
    assert!(
        m.coverage_open_proof(C, FIRST).is_none(),
        "开放证明必须已经转否决"
    );
    assert_eq!(
        m.coverage_disposition(C, FIRST),
        CoverageDisposition::OrdinaryGap
    );
    assert_eq!(m.coverage_observation_gen(C), None, "连续观察永久封锁");
    m.clock.mono_ms += 1;
    notify(
        &mut m,
        "thread/name/updated",
        json!({"threadId":C,"threadName":"重新附着"}),
    );
    assert!(
        m.relations.attachments[C].valid,
        "前置：之后真实通知确实恢复附着"
    );
    reasoning(&mut m, FIRST);
    complete(&mut m, FIRST);
    start(&mut m, SECOND);
    assert!(
        m.coverage_open_proof(C, SECOND).is_none(),
        "重新附着与raw不能恢复授信"
    );
    assert_eq!(m.coverage_observation_gen(C), None);
}

#[test]
fn gap_b_review1_新增合法展示分支的缺字段和错误类型仍撤销() {
    let invalid = [
        json!({"type":"collabAgentToolCall","id":"工具","status":"completed","senderThreadId":C,"receiverThreadIds":[],"agentsStates":{}}),
        json!({"type":"collabAgentToolCall","id":"工具","tool":7,"status":"completed","senderThreadId":C,"receiverThreadIds":[],"agentsStates":{}}),
        json!({"type":"collabAgentToolCall","id":"工具","tool":"sendMessage","status":"declined","senderThreadId":C,"receiverThreadIds":[],"agentsStates":{}}),
        json!({"type":"collabAgentToolCall","id":"工具","tool":"followupTask","status":"interrupted","senderThreadId":C,"receiverThreadIds":[7],"agentsStates":{}}),
        json!({"type":"collabAgentToolCall","id":"工具","tool":"interruptAgent","status":"completed","senderThreadId":C,"receiverThreadIds":[],"agentsStates":[]}),
        json!({"type":"webSearch","id":"搜索"}),
        json!({"type":"webSearch","id":"搜索","query":7}),
        json!({"type":"webSearch","id":"搜索","query":"问题","action":{}}),
        json!({"type":"webSearch","id":"搜索","query":"问题","action":{"type":"search","queries":[7]}}),
        json!({"type":"webSearch","id":"搜索","query":"问题","action":{"type":"openPage","url":7}}),
        json!({"type":"webSearch","id":"搜索","query":"问题","action":{"type":"findInPage","pattern":{}}}),
        json!({"type":"webSearch","id":"搜索","query":"问题","action":{"type":"unknown"}}),
        json!({"type":"webSearch","id":"搜索","query":"问题","results":{}}),
        json!({"type":"sleep","id":"等待"}),
        json!({"type":"sleep","id":"等待","durationMs":null}),
        json!({"type":"sleep","id":"等待","durationMs":-1}),
        json!({"type":"sleep","id":"等待","durationMs":0.5}),
        json!({"type":"sleep","id":"等待","durationMs":"1"}),
        json!({"type":"imageGeneration","id":"图片","status":"completed"}),
        json!({"type":"imageGeneration","id":"图片","status":"completed","result":7}),
        json!({"type":"mcpToolCall","id":"工具","server":"服务","tool":"调用","status":"declined","arguments":{}}),
        json!({"type":"dynamicToolCall","id":"工具","tool":"调用","status":"declined","arguments":{}}),
    ];
    for item in invalid {
        let mut m = seeded();
        open(&mut m);
        let detail = item.to_string();
        notify(
            &mut m,
            "item/completed",
            json!({"threadId":C,"turnId":FIRST,"item":item}),
        );
        assert!(
            m.coverage_open_proof(C, FIRST).is_none(),
            "无法解释的条目必须撤销：{detail}"
        );
        assert_eq!(m.coverage_observation_gen(C), None);
        complete(&mut m, FIRST);
        assert_eq!(
            m.coverage_disposition(C, FIRST),
            CoverageDisposition::OrdinaryGap
        );
    }
}

#[test]
fn gap_b_review1_工具加密结果严格校验且不扩展普通消息内容() {
    for part in [
        json!({"type":"encrypted_content"}),
        json!({"type":"encrypted_content","encrypted_content":7}),
        json!({"type":"future_input"}),
        json!({"type":"input_text"}),
        json!({"type":"output_text","text":"不是工具输出枚举"}),
    ] {
        for item_type in [
            "function_call_output",
            "custom_tool_call_output",
            "functionCallOutput",
        ] {
            let mut m = seeded();
            open(&mut m);
            let method = if item_type == "functionCallOutput" {
                "item/completed"
            } else {
                "rawResponseItem/completed"
            };
            notify(
                &mut m,
                method,
                json!({"threadId":C,"turnId":FIRST,"item":{"type":item_type,"id":"坏输出","call_id":"调用","name":"工具","output":[part]}}),
            );
            assert!(
                m.coverage_open_proof(C, FIRST).is_none(),
                "{item_type}坏结果必须撤销"
            );
            assert_eq!(m.coverage_observation_gen(C), None);
        }
    }
    let mut m = seeded();
    open(&mut m);
    raw(
        &mut m,
        FIRST,
        json!({"type":"message","role":"assistant","content":[{"type":"encrypted_content","encrypted_content":"不属于普通消息枚举"}]}),
    );
    assert!(
        m.coverage_open_proof(C, FIRST).is_none(),
        "工具输出扩展不能放宽普通消息"
    );
}

#[test]
fn gap_b_review1_raw搜索动作参数错误仍然撤销() {
    for action in [
        json!({}),
        json!({"type":"search","queries":[7]}),
        json!({"type":"open_page","url":7}),
        json!({"type":"find_in_page","pattern":[]}),
        json!({"type":"future_action"}),
    ] {
        let mut m = seeded();
        open(&mut m);
        raw(
            &mut m,
            FIRST,
            json!({"type":"web_search_call","action":action}),
        );
        assert!(m.coverage_open_proof(C, FIRST).is_none());
        assert_eq!(m.coverage_observation_gen(C), None);
    }
}

#[test]
fn gap_b_review1_普通函数调用参数字符串不要求工具执行成功() {
    for arguments in [json!("{broken"), json!(""), json!("{\"command\":\"echo\"}")] {
        gap_b_review1_output_sequence(
            "rawResponseItem/completed",
            json!({"type":"function_call","call_id":"普通调用","name":"shell","arguments":arguments}),
        );
    }
    for item in [
        json!({"type":"function_call","call_id":"普通调用","name":"shell"}),
        json!({"type":"function_call","call_id":"普通调用","name":"shell","arguments":7}),
    ] {
        let mut m = seeded();
        open(&mut m);
        raw(&mut m, FIRST, item);
        assert!(
            m.coverage_open_proof(C, FIRST).is_none(),
            "缺字段或非字符串仍是结构缺口"
        );
        assert_eq!(m.coverage_observation_gen(C), None);
    }
}

// 第二轮审查：已知结构中的强类型字段必须解释完整；Value 不透明内容只校验其容器。
fn gap_b_review2_reject(method: &str, item: Value) {
    let mut m = seeded();
    open(&mut m);
    assert!(m.relations.attachments[C].valid);
    assert!(
        m.coverage_open_proof(C, FIRST).is_some(),
        "前置：坏条目前真实覆盖为Open"
    );
    assert_eq!(m.coverage_observation_gen(C), Some(0));
    let detail = item.to_string();
    notify(
        &mut m,
        method,
        json!({"threadId":C,"turnId":FIRST,"item":item}),
    );
    assert!(
        m.coverage_open_proof(C, FIRST).is_none(),
        "畸形强类型结构应否决：{detail}"
    );
    assert_eq!(
        m.coverage_observation_gen(C),
        None,
        "畸形结构永久封锁未来授信"
    );
    complete(&mut m, FIRST);
    assert_eq!(
        m.coverage_disposition(C, FIRST),
        CoverageDisposition::OrdinaryGap
    );
}
fn gap_b_review2_command(actions: Value) -> Value {
    json!({"type":"commandExecution","id":"命令","command":"echo x","cwd":"D:/work",
        "status":"completed","commandActions":actions})
}
fn gap_b_review2_changes(changes: Value) -> Value {
    json!({"type":"fileChange","id":"文件变化","status":"completed","changes":changes})
}
fn gap_b_review2_agents(states: Value) -> Value {
    json!({"type":"collabAgentToolCall","id":"代理工具","tool":"listAgents","status":"completed",
        "senderThreadId":C,"receiverThreadIds":[],"agentsStates":states})
}
#[test]
fn gap_b_review2_command_actions畸形元素与必需字段必须否决() {
    for action in [
        json!(7),
        json!({}),
        json!({"type":"read","command":"cat","name":"文件"}),
        json!({"type":"read","command":7,"name":"文件","path":"D:/file"}),
        json!({"type":"listFiles","path":null}),
        json!({"type":"listFiles","command":"ls","path":7}),
        json!({"type":"search","command":"rg","query":7,"path":null}),
        json!({"type":"unknown","command":7}),
        json!({"type":"futureAction","command":"x"}),
    ] {
        gap_b_review2_reject("item/completed", gap_b_review2_command(json!([action])));
    }
}
#[test]
fn gap_b_review2_command_actions全部合法枚举保留覆盖() {
    for action in [
        json!({"type":"read","command":"cat","name":"文件","path":"D:/file"}),
        json!({"type":"listFiles","command":"ls","path":null}),
        json!({"type":"listFiles","command":"ls"}),
        json!({"type":"search","command":"rg","query":"词","path":"D:/work"}),
        json!({"type":"search","command":"rg","query":null,"path":null}),
        json!({"type":"unknown","command":"echo x"}),
    ] {
        gap_b_review1_output_sequence("item/completed", gap_b_review2_command(json!([action])));
    }
}
#[test]
fn gap_b_review2_file_change畸形元素与修改种类必须否决() {
    for change in [
        json!(7),
        json!({"path":"文件","diff":"补丁"}),
        json!({"kind":{"type":"add"},"diff":"补丁"}),
        json!({"path":"文件","kind":{"type":"add"}}),
        json!({"path":7,"kind":{"type":"add"},"diff":"补丁"}),
        json!({"path":"文件","kind":{"type":"futureKind"},"diff":"补丁"}),
        json!({"path":"文件","kind":{"type":"update","move_path":7},"diff":"补丁"}),
        json!({"path":"文件","kind":{"type":"delete"},"diff":7}),
    ] {
        gap_b_review2_reject("item/completed", gap_b_review2_changes(json!([change])));
    }
}
#[test]
fn gap_b_review2_file_change全部合法修改种类保留覆盖() {
    for kind in [
        json!({"type":"add"}),
        json!({"type":"delete"}),
        json!({"type":"update"}),
        json!({"type":"update","move_path":null}),
        json!({"type":"update","move_path":"D:/new"}),
    ] {
        gap_b_review1_output_sequence(
            "item/completed",
            gap_b_review2_changes(json!([{"path":"D:/file","kind":kind,"diff":"补丁"}])),
        );
    }
}
#[test]
fn gap_b_review2_agents_states畸形值与状态必须否决() {
    for state in [
        json!(7),
        json!({}),
        json!({"status":7}),
        json!({"status":"futureState"}),
        json!({"status":"running","message":7}),
    ] {
        gap_b_review2_reject(
            "item/completed",
            gap_b_review2_agents(json!({"child":state})),
        );
    }
}
#[test]
fn gap_b_review2_agents_states全部合法状态保留覆盖() {
    for status in [
        "pendingInit",
        "running",
        "interrupted",
        "completed",
        "errored",
        "shutdown",
        "notFound",
    ] {
        for message in [Value::Null, json!("状态消息")] {
            gap_b_review1_output_sequence(
                "item/completed",
                gap_b_review2_agents(json!({"child":{"status":status,"message":message}})),
            );
        }
    }
    gap_b_review1_output_sequence(
        "item/completed",
        gap_b_review2_agents(json!({"child":{"status":"running"}})),
    );
}
#[test]
fn gap_b_review2_mcp结果错误与应用上下文强类型仍须校验() {
    for (field, value) in [
        ("result", json!({})),
        ("result", json!({"content":7})),
        ("error", json!({})),
        ("error", json!({"message":7})),
        ("appContext", json!({})),
        ("appContext", json!({"connectorId":7})),
        (
            "mcpAppUi",
            json!({"resourceUri":"ui://x","preferredModelDisplayMode":"unknown"}),
        ),
    ] {
        let mut item = json!({"type":"mcpToolCall","id":"mcp","server":"服务","tool":"工具","arguments":null,"status":"completed"});
        item[field] = value;
        gap_b_review2_reject("item/completed", item);
    }
}
#[test]
fn gap_b_review2_dynamic输出内容枚举必须校验() {
    for content in [
        json!(7),
        json!({"type":"inputText"}),
        json!({"type":"inputImage","imageUrl":7}),
        json!({"type":"inputAudio","audioUrl":7}),
        json!({"type":"futureInput"}),
    ] {
        gap_b_review2_reject(
            "item/completed",
            json!({"type":"dynamicToolCall","id":"dynamic","tool":"工具",
            "arguments":{},"status":"completed","contentItems":[content]}),
        );
    }
}
#[test]
fn gap_b_review2_agent_message结构化元数据必须校验() {
    for (field, value) in [
        ("phase", json!("unknown")),
        ("delivery", json!("unknown")),
        ("questions", json!([7])),
        ("questions", json!([{"title":7}])),
        ("questions", json!([{"title":"问题","options":[7]}])),
        ("memoryCitation", json!({"entries":[7],"threadIds":[]})),
        (
            "memoryCitation",
            json!({"entries":[{"path":"文件","lineStart":0,"lineEnd":1}],"threadIds":[]}),
        ),
        ("memoryCitation", json!({"entries":[],"threadIds":[7]})),
    ] {
        let mut item = json!({"type":"agentMessage","id":"消息","text":"正文"});
        item[field] = value;
        gap_b_review2_reject("item/completed", item);
    }
}
#[test]
fn gap_b_review2_image_failure结构与必需字段必须校验() {
    for failure in [
        json!(7),
        json!({}),
        json!({"type":"unknown"}),
        json!({"type":"usageLimitExceeded"}),
        json!({"type":"usageLimitExceeded","limitId":7}),
        json!({"type":"usageLimitExceeded","limitId":"限制","resetsAt":"明天"}),
    ] {
        gap_b_review2_reject(
            "item/completed",
            json!({"type":"imageGeneration","id":"图片","status":"failed","result":"","failure":failure}),
        );
    }
}
#[test]
fn gap_b_review2_raw强类型可选容器与枚举不能吞错误() {
    let items = [
        json!({"type":"function_call","call_id":"调用","name":"工具","arguments":"{}","encrypted_function_args":[7]}),
        json!({"type":"message","role":"assistant","content":[],"phase":"futurePhase"}),
        json!({"type":"message","role":"assistant","content":[{"type":"input_image","file_id":"图片","detail":"futureDetail"}]}),
        json!({"type":"local_shell_call","status":"completed","action":{"type":"exec","command":["echo"],"env":{"X":7}}}),
        json!({"type":"local_shell_call","status":"completed","action":{"type":"exec","command":["echo"],"timeout_ms":-1}}),
        json!({"type":"function_call","call_id":"调用","name":"工具","arguments":"{}","namespace":7}),
        json!({"type":"reasoning","summary":[],"internal_chat_message_metadata_passthrough":{"turn_id":7}}),
    ];
    for item in items {
        gap_b_review2_reject("rawResponseItem/completed", item);
    }
}
#[test]
fn gap_b_review2_合法强类型扩展与不透明值不误封锁() {
    for item in [
        json!({"type":"mcpToolCall","id":"mcp","server":"服务","tool":"工具","arguments":[7],"status":"completed",
            "result":{"content":[7,{"futureContent":"不透明"}],"structuredContent":7,"_meta":false},
            "appContext":{"connectorId":"连接"},"mcpAppUi":{"resourceUri":"ui://x","preferredModelDisplayMode":"inline"},"error":null}),
        json!({"type":"dynamicToolCall","id":"dynamic","tool":"工具","arguments":7,"status":"completed",
            "contentItems":[{"type":"inputText","text":"正文"},{"type":"inputImage","imageUrl":"图片"},{"type":"inputAudio","audioUrl":"音频"}],"success":true}),
        json!({"type":"agentMessage","id":"消息","text":"正文","phase":"commentary","delivery":"async",
            "questions":[{"title":"问题","options":["选项"]}],
            "memoryCitation":{"entries":[{"path":"文件","lineStart":0,"lineEnd":1,"note":"说明"}],"threadIds":["线程"]}}),
        json!({"type":"imageGeneration","id":"图片","status":"failed","result":"","failure":{"type":"usageLimitExceeded","limitId":"限制","resetsAt":null}}),
    ] {
        gap_b_review1_output_sequence("item/completed", item);
    }
    gap_b_review1_output_sequence(
        "rawResponseItem/completed",
        json!({"type":"tool_search_output","execution":"client","status":"completed","tools":[7,{"futureTool":"不透明"}]}),
    );
    gap_b_review1_output_sequence(
        "rawResponseItem/completed",
        json!({"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"input_image","file_id":"图片","detail":"original"}]}),
    );
    gap_b_review1_output_sequence(
        "rawResponseItem/completed",
        json!({"type":"local_shell_call","status":"completed","action":{"type":"exec","command":["echo"],"timeout_ms":1,"env":{"X":"值"}}}),
    );
}
#[test]
fn gap_b_review2_公开条目已知可选字段出现时仍须符合类型() {
    let bases = [
        (
            gap_b_review2_command(json!([])),
            vec![
                ("source", json!("unknown")),
                ("processId", json!(7)),
                ("exitCode", json!(2147483648_i64)),
                ("durationMs", json!("慢")),
                ("aggregatedOutput", json!(7)),
            ],
        ),
        (
            gap_b_review2_agents(json!({})),
            vec![
                ("prompt", json!(7)),
                ("model", json!(7)),
                ("reasoningEffort", json!(7)),
            ],
        ),
        (
            json!({"type":"dynamicToolCall","id":"dynamic","tool":"工具","arguments":{},"status":"completed"}),
            vec![
                ("success", json!("是")),
                ("namespace", json!(7)),
                ("durationMs", json!("慢")),
            ],
        ),
        (
            json!({"type":"imageGeneration","id":"图片","status":"completed","result":"结果"}),
            vec![
                ("transparentBackground", json!("是")),
                ("savedPath", json!(7)),
            ],
        ),
        (
            json!({"type":"functionCallOutput","id":"输出","name":"工具","output":"结果"}),
            vec![("namespace", json!(7))],
        ),
    ];
    for (base, fields) in bases {
        for (field, value) in fields {
            let mut item = base.clone();
            item[field] = value;
            gap_b_review2_reject("item/completed", item);
        }
    }
}

#[test]
fn gap_b_review2_公开字段的缺省null范围和扩展兼容() {
    for item in [
        json!({"type":"agentMessage","id":"默认消息","text":"正文","phase":null,"delivery":null,"questions":null,"memoryCitation":null,"extra":7}),
        json!({"type":"agentMessage","id":"问题","text":"正文","questions":[{"title":"问题"},{"title":"问题","options":null}],"memoryCitation":{"entries":[{"path":"文件","lineStart":0,"lineEnd":4294967295_u64,"note":"说明","extra":7}],"threadIds":[]}}),
        json!({"type":"reasoning","id":"缺省数组"}),
        json!({"type":"functionCallOutput","id":"输出","name":"工具","namespace":null,"output":"结果"}),
        json!({"type":"commandExecution","id":"命令","command":"echo","cwd":"D:/work","status":"completed","commandActions":[],"pluginId":null,"scriptPath":null,"processId":null,"aggregatedOutput":null,"exitCode":null,"durationMs":null}),
        json!({"type":"mcpToolCall","id":"mcp","server":"服务","tool":"工具","arguments":null,"status":"completed","appContext":null,"mcpAppUi":null,"mcpAppResourceUri":null,"pluginId":null,"readOnlyHint":null,"result":null,"error":null,"durationMs":null}),
        json!({"type":"mcpToolCall","id":"mcp","server":"服务","tool":"工具","arguments":7,"status":"completed","appContext":{"connectorId":"连接","linkId":null,"resourceUri":null,"appName":null,"actionName":null,"extra":7},"mcpAppUi":{"resourceUri":"ui://x","preferredModelDisplayMode":"fullscreen"},"readOnlyHint":false,"error":{"message":"错误"},"result":{"content":[7],"structuredContent":false,"_meta":"不透明"}}),
        json!({"type":"dynamicToolCall","id":"动态","tool":"工具","arguments":false,"status":"completed","namespace":null,"contentItems":null,"success":null,"durationMs":null}),
        json!({"type":"collabAgentToolCall","id":"代理","tool":"listAgents","status":"completed","senderThreadId":C,"receiverThreadIds":[],"agentsStates":{},"prompt":null,"model":null,"reasoningEffort":"model-custom-effort"}),
        json!({"type":"collabAgentToolCall","id":"代理空值","tool":"listAgents","status":"completed","senderThreadId":C,"receiverThreadIds":[],"agentsStates":{},"reasoningEffort":null}),
        json!({"type":"imageGeneration","id":"图片","status":"free-form-status","result":"结果","revisedPrompt":null,"savedPath":null,"transparentBackground":null,"failure":null}),
        json!({"type":"fileChange","id":"兼容额外字段","status":"completed","changes":[{"path":"文件","diff":"补丁","kind":{"type":"update","move_path":null,"movePath":7}}]}),
    ] {
        gap_b_review1_output_sequence("item/completed", item);
    }
    for source in [
        "agent",
        "userShell",
        "unifiedExecStartup",
        "unifiedExecInteraction",
    ] {
        let mut item = gap_b_review2_command(json!([]));
        item["source"] = json!(source);
        for (exit, duration) in [(i32::MIN, i64::MIN), (i32::MAX, i64::MAX)] {
            item["exitCode"] = json!(exit);
            item["durationMs"] = json!(duration);
            gap_b_review1_output_sequence("item/completed", item.clone());
        }
    }
    for reset in [i64::MIN, i64::MAX] {
        gap_b_review1_output_sequence(
            "item/completed",
            json!({"type":"imageGeneration","id":"图片","status":"failed","result":"","transparentBackground":true,"savedPath":"D:/image.png","failure":{"type":"usageLimitExceeded","limitId":"限制","resetsAt":reset}}),
        );
    }
}

#[test]
fn gap_b_review2_default空值与闭合范围不能混入option语义() {
    let mut bad_source = gap_b_review2_command(json!([]));
    bad_source["source"] = Value::Null;
    let mut bad_exit = gap_b_review2_command(json!([]));
    bad_exit["exitCode"] = json!(-2147483649_i64);
    let mut bad_duration = gap_b_review2_command(json!([]));
    bad_duration["durationMs"] = json!(9223372036854775808_u64);
    let mut empty_effort = gap_b_review2_agents(json!({}));
    empty_effort["reasoningEffort"] = json!("");
    for item in [
        bad_source,
        bad_exit,
        bad_duration,
        empty_effort,
        json!({"type":"reasoning","id":"坏默认数组","summary":null}),
        json!({"type":"reasoning","id":"坏默认数组","content":null}),
        json!({"type":"agentMessage","id":"问题额外字段","text":"正文","questions":[{"title":"问题","unexpected":7}]}),
        json!({"type":"agentMessage","id":"坏范围","text":"正文","memoryCitation":{"entries":[{"path":"文件","note":"说明","lineStart":-1,"lineEnd":1}],"threadIds":[]}}),
        json!({"type":"agentMessage","id":"坏范围","text":"正文","memoryCitation":{"entries":[{"path":"文件","note":"说明","lineStart":0,"lineEnd":4294967296_u64}],"threadIds":[]}}),
    ] {
        gap_b_review2_reject("item/completed", item);
    }
}

#[test]
fn gap_b_review2_raw已声明option空值及私有扩展不误封锁() {
    for mut item in [
        json!({"type":"message","role":"assistant","content":[],"phase":null}),
        json!({"type":"function_call","call_id":"调用","name":"shell","arguments":"非JSON","namespace":null,"encrypted_function_args":null}),
        json!({"type":"function_call","call_id":"调用","name":"shell","arguments":"","encrypted_function_args":["密文"]}),
        json!({"type":"function_call_output","output":"结果","call_id":null,"name":null,"namespace":null}),
        json!({"type":"custom_tool_call","call_id":"调用","name":"工具","input":"输入","status":null,"namespace":null}),
        json!({"type":"custom_tool_call_output","call_id":"调用","name":null,"output":"结果"}),
        json!({"type":"local_shell_call","call_id":null,"status":"completed","action":{"type":"exec","command":[],"timeout_ms":18446744073709551615_u64,"working_directory":null,"user":null,"env":null}}),
        json!({"type":"image_generation_call","status":"自由状态","result":"结果","revised_prompt":null}),
        json!({"type":"reasoning","summary":[],"content":null,"encrypted_content":null}),
        json!({"type":"context_compaction","encrypted_content":null}),
    ] {
        item["id"] = Value::Null;
        item["internal_chat_message_metadata_passthrough"] = json!({"turn_id":null,"create_time":"非公开字段","content_item_kinds":[7],"executed_tool_calls":7,"cell_id":7,"tool_calls_complete":"忽略"});
        gap_b_review1_output_sequence("rawResponseItem/completed", item.clone());
        item["internal_chat_message_metadata_passthrough"] = Value::Null;
        gap_b_review1_output_sequence("rawResponseItem/completed", item);
    }
    // 无此声明的compaction_trigger附加字段不作已声明字段限制。
    gap_b_review1_output_sequence(
        "rawResponseItem/completed",
        json!({"type":"compaction_trigger","id":7,"internal_chat_message_metadata_passthrough":7}),
    );
    for detail in [
        Value::Null,
        json!("auto"),
        json!("low"),
        json!("high"),
        json!("original"),
    ] {
        // ImageReference是anyOf；只要file_id分支成立，另一分支名称为兼容额外字段。
        let image = json!({"type":"input_image","file_id":"图片","image_url":7,"detail":detail});
        gap_b_review1_output_sequence(
            "rawResponseItem/completed",
            json!({"type":"message","role":"assistant","content":[image]}),
        );
    }
}
