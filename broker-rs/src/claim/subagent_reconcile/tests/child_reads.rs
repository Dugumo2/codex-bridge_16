use super::*;
use crate::test_support::status_read::items_page;
fn child_requests(actions: &[Action]) -> Vec<RequestContext> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Request(r) if matches!(r.operation, Operation::ChildResultRead { .. }) => {
                Some(r.clone())
            }
            _ => None,
        })
        .collect()
}
fn answer(
    m: &mut Machine,
    request: &RequestContext,
    text: &str,
    cursor: Option<&str>,
) -> Vec<Action> {
    let turn = request.params.as_ref().unwrap()["turnId"].as_str().unwrap();
    let mut actions = vec![];
    m.child_result_reply(
        request,
        Ok(items_page(
            turn,
            &[json!({"type":"agentMessage","phase":"final_answer","text":text})],
            cursor,
        )),
        &mut actions,
    );
    actions
}
#[test]
fn 四路并发分页独立代号且同批只完成一次() {
    let (mut m, token) = fixture(9);
    let mut actions = vec![];
    let id = m.read_child_results(token.clone(), 1, ResultLimits::default(), &mut actions);
    assert_ne!(id, BatchReadId::DISABLED);
    let mut pending = child_requests(&actions);
    assert_eq!(pending.len(), 4);
    let first = pending.remove(0);
    let next = answer(&mut m, &first, "前页", Some("下一页"));
    let page = child_requests(&next);
    assert_eq!(page.len(), 1);
    assert!(answer(&mut m, &first, "重复页不应进入", Some("下一页")).is_empty());
    pending.extend(page);
    let mut maximum = pending.len();
    while let Some(request) = pending.pop() {
        let next = answer(&mut m, &request, "最终证据🔎", None);
        pending.extend(child_requests(&next));
        maximum = maximum.max(pending.len());
    }
    assert_eq!(maximum, 4);
    let batch = m.take_child_results(&token).unwrap();
    assert_eq!(batch.coverage.total, 9);
    assert_eq!(batch.coverage.available, 9);
    assert!(batch.input.contains("前页"));
    assert!(!batch.input.contains("重复页不应进入"));
    assert_eq!(batch.items[1].parent_thread_id, "子0");
    assert!(m.runs.runs[R].turns.is_empty());
    assert_eq!(
        m.runs.runs[R]
            .subagent_join
            .as_ref()
            .unwrap()
            .wakeup_attempts,
        0
    );
    m.child_results_done(token.clone(), id, batch, &mut vec![]);
    m.drive_child_results(&mut vec![]);
    assert!(m.take_child_results(&token).is_none());
}
#[test]
fn 二十秒批次期限与总期限取较早且迟到页丢弃() {
    for remaining in [7_000, 50_000] {
        let (mut m, token) = fixture(6);
        m.convergences.runs.get_mut(R).unwrap().deadline_mono_ms = m.clock.mono_ms + remaining;
        let mut actions = vec![];
        m.read_child_results(token.clone(), 1, ResultLimits::default(), &mut actions);
        assert!(actions.iter().any(
            |a| matches!(a, Action::Schedule { delay_ms, .. } if *delay_ms == remaining.min(20_000))
        ));
        let pending = child_requests(&actions);
        m.clock.mono_ms += remaining.min(20_000);
        m.drive_child_results(&mut vec![]);
        let batch = m.take_child_results(&token).unwrap();
        assert_eq!(batch.coverage.unavailable, 6);
        assert_eq!(batch.items[0].status, ActivityResultStatus::Completed);
        assert!(batch.input.contains("批次读取期限已到"));
        for request in pending {
            assert!(answer(&mut m, &request, "迟到结果", None).is_empty());
        }
        assert!(m.take_child_results(&token).is_none());
    }
}
#[test]
fn 无输出异步返回unavailable而活动未知拒绝批次() {
    let (mut m, token) = fixture(1);
    m.runs
        .reconciliation
        .harness
        .as_mut()
        .unwrap()
        .facts
        .get_mut(R)
        .unwrap()
        .result_refs[0]
        .available = false;
    let mut actions = vec![];
    let id = m.read_child_results(token.clone(), 1, ResultLimits::default(), &mut actions);
    assert_ne!(id, BatchReadId::DISABLED);
    assert!(child_requests(&actions).is_empty());
    assert!(m.take_child_results(&token).is_none());
    m.drive_child_results(&mut actions);
    assert_eq!(
        m.take_child_results(&token).unwrap().coverage.unavailable,
        1
    );
    m.runs
        .reconciliation
        .harness
        .as_mut()
        .unwrap()
        .facts
        .get_mut(R)
        .unwrap()
        .activities[0]
        .activity = MemberActivity::Unknown;
    assert_eq!(
        m.read_child_results(token, 1, ResultLimits::default(), &mut vec![]),
        BatchReadId::DISABLED
    );
}
#[test]
fn 畸形页错误和重复游标不保留假完整前缀() {
    for kind in 0..4 {
        let (mut m, token) = fixture(1);
        let mut a = vec![];
        m.read_child_results(token.clone(), 1, ResultLimits::default(), &mut a);
        let first = child_requests(&a).pop().unwrap();
        let next = answer(&mut m, &first, "不可冒充完整", Some("游标"));
        let request = child_requests(&next).pop().unwrap();
        let value = match kind {
            0 => Err(json!({"code":-32603})),
            1 => Ok(items_page(
                "错误轮",
                &[json!({"type":"agentMessage","text":"错误轮文本"})],
                None,
            )),
            2 => Ok(items_page(
                "子轮0",
                &[json!({"type":"agentMessage","text":"后页"})],
                Some("游标"),
            )),
            _ => Ok(items_page("子轮0", &[], Some("空续页"))),
        };
        m.child_result_reply(&request, value, &mut vec![]);
        let b = m.take_child_results(&token).unwrap();
        assert_eq!(b.coverage.unavailable, 1);
        assert!(!b.input.contains("不可冒充完整"));
    }
}
#[test]
fn 五项token身份变化和取消均静默退役() {
    for field in 0..7 {
        let (mut m, token) = fixture(1);
        let mut a = vec![];
        m.read_child_results(token.clone(), 1, ResultLimits::default(), &mut a);
        let first = child_requests(&a).pop().unwrap();
        match field {
            0 => m.convergences.runs.get_mut(R).unwrap().token.run_id = "新运行".into(),
            1 => m.claims.get_mut(T).unwrap().claim_id += 1,
            2 => m.relations.contexts.get_mut(R).unwrap().gen += 1,
            3 => m.relations.conn_epoch += 1,
            4 => m.convergences.runs.get_mut(R).unwrap().token.generation += 1,
            5 => m.cancel_child_results(&token, &mut vec![]),
            _ => m.closing = true,
        }
        answer(&mut m, &first, "旧结果", None);
        m.drive_child_results(&mut vec![]);
        assert!(!m.runs.reconciliation.ready.contains_key(R));
    }
}
#[test]
fn 超大批次有界清单与遗漏总数且不另开轮() {
    let (mut m, token) = fixture(10_000);
    let mut a = vec![];
    m.read_child_results(token.clone(), 1, ResultLimits::default(), &mut a);
    let mut pending = child_requests(&a);
    let mut reads = 0;
    while let Some(request) = pending.pop() {
        pending.extend(child_requests(&answer(
            &mut m,
            &request,
            &"多字节🙂\"\n".repeat(3_000),
            None,
        )));
        reads += 1;
    }
    assert!(reads <= 256);
    let b = m.take_child_results(&token).unwrap();
    assert!(b.input.len() <= 65536);
    assert_eq!(b.total_bytes, b.input.len());
    assert_eq!(b.coverage.total, 10_000);
    assert_eq!(
        b.coverage.available + b.coverage.unavailable + b.omitted,
        10_000
    );
    assert!(b.omitted > 9_700);
    assert_eq!(b.coverage.omitted, b.omitted);
    for item in &b.items {
        assert!(serde_json::to_vec(item).unwrap().len() < 8192);
        assert!(item.truncated);
    }
    assert_eq!(
        m.runs.runs[R]
            .subagent_join
            .as_ref()
            .unwrap()
            .wakeup_attempts,
        0
    );
}
#[test]
fn 证据按转义后完整序列化字节计且utf8边界正确() {
    let (m, _) = fixture(2);
    let mut item = batch(&m, "").items[0].clone();
    item.output = "中文🙂\"\\\n\u{0001}".repeat(10_000);
    let b = evidence::build_batch(1, 2, [item.clone(), item], &ResultLimits::default()).unwrap();
    assert_eq!(b.total_bytes, b.input.len());
    assert!(b.input.len() <= 65536);
    for item in b.items {
        let encoded = serde_json::to_vec(&item).unwrap();
        assert!(encoded.len() < 8192);
        assert!(item.truncated);
        assert!(!item.output.contains('\u{fffd}'));
        assert!(std::str::from_utf8(&encoded).is_ok());
    }
}
#[test]
fn 工作版本变化回包只能丢弃不能推进新覆盖() {
    let (mut m, token) = fixture(1);
    let mut a = vec![];
    m.read_child_results(token.clone(), 1, ResultLimits::default(), &mut a);
    let request = child_requests(&a).pop().unwrap();
    m.runs
        .reconciliation
        .harness
        .as_mut()
        .unwrap()
        .facts
        .get_mut(R)
        .unwrap()
        .work_epoch = 2;
    answer(&mut m, &request, "旧批次", None);
    assert!(m.take_child_results(&token).is_none());
    assert_eq!(
        m.runs.runs[R]
            .subagent_join
            .as_ref()
            .unwrap()
            .delivered_epoch,
        0
    );
}

#[test]
fn 子失败中断关闭缺失均保留状态且不依赖完成消息() {
    let (mut m, token) = fixture(4);
    let statuses = [
        ActivityResultStatus::Failed,
        ActivityResultStatus::Interrupted,
        ActivityResultStatus::Shutdown,
        ActivityResultStatus::Unavailable,
    ];
    let facts = m
        .runs
        .reconciliation
        .harness
        .as_mut()
        .unwrap()
        .facts
        .get_mut(R)
        .unwrap();
    for (fact, status) in facts.activities.iter_mut().zip(statuses) {
        fact.result_status = Some(status);
    }
    for result in &mut facts.result_refs {
        result.available = false;
    }
    let mut a = vec![];
    assert_ne!(
        m.read_child_results(token.clone(), 1, ResultLimits::default(), &mut a),
        BatchReadId::DISABLED
    );
    assert!(child_requests(&a).is_empty());
    m.drive_child_results(&mut a);
    let batch = m.take_child_results(&token).unwrap();
    assert_eq!(batch.coverage.unavailable, 4);
    assert_eq!(
        batch.items.iter().map(|i| i.status).collect::<Vec<_>>(),
        statuses
    );
    for name in ["failed", "interrupted", "shutdown", "unavailable"] {
        assert!(batch.input.contains(name));
    }
}
#[test]
fn 读完但没有任何输出条目时标unavailable并保留真实终态() {
    for items in [
        vec![],
        vec![json!({"type":"commandExecution","id":"命令","status":"completed"})],
    ] {
        let (mut m, token) = fixture(1);
        let mut actions = vec![];
        let id = m.read_child_results(token.clone(), 1, ResultLimits::default(), &mut actions);
        assert_ne!(id, BatchReadId::DISABLED);
        let request = child_requests(&actions).remove(0);
        let turn = request.params.as_ref().unwrap()["turnId"].as_str().unwrap();
        let status = m.runs.reconciliation.harness.as_ref().unwrap().facts[R].activities[0]
            .result_status
            .unwrap();
        let mut next = vec![];
        m.child_result_reply(&request, Ok(items_page(turn, &items, None)), &mut next);
        m.drive_child_results(&mut next);
        let batch = m.take_child_results(&token).unwrap();
        assert_eq!(batch.coverage.available, 0, "空输出不能计为可用");
        assert_eq!(batch.coverage.unavailable, 1);
        assert_eq!(
            batch.items[0].unavailable_reason.as_deref(),
            Some("活动没有输出")
        );
        assert_eq!(batch.items[0].status, status, "活动终态保持不变");
    }
}
