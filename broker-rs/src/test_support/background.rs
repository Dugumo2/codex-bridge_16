//! 旧停止用例的空后台环境：只回答新增清理阶段及其后置核实，不改变旧用例手工控制的读取。
use crate::claim::{self, Action, Event, Machine, Operation};
use serde_json::json;
use std::collections::VecDeque;

pub(crate) fn empty_background(machine: &mut Machine, actions: Vec<Action>) -> Vec<Action> {
    let mut queue: VecDeque<_> = actions.into();
    let mut all = Vec::new();
    while let Some(action) = queue.pop_front() {
        if let Action::Request(context) = &action {
            let thread = context
                .params
                .as_ref()
                .and_then(|p| p["threadId"].as_str())
                .unwrap_or("");
            let postcheck = machine
                .stops
                .operations
                .values()
                .any(|op| op.thread == thread && op.background.verifying);
            let preflight = machine
                .stops
                .operations
                .values()
                .any(|op| op.thread == thread && op.historical_probe);
            let result = match context.method.as_str() {
                "thread/backgroundTerminals/list" => Some(json!({"data":[],"nextCursor":null})),
                "thread/goal/get" if postcheck => Some(json!({"goal":null})),
                "thread/read"
                    if (postcheck || preflight)
                        && matches!(context.operation, Operation::StatusRead { .. }) =>
                {
                    Some(crate::test_support::status_read::meta(thread, "idle"))
                }
                _ => None,
            };
            if let Some(result) = result {
                let (next, follow) = claim::step(
                    std::mem::take(machine),
                    Event::Reply {
                        context: context.clone(),
                        result: Ok(result),
                    },
                );
                *machine = next;
                queue.extend(follow);
            }
        }
        all.push(action);
    }
    all
}
