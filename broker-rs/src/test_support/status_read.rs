//! H2 状态读取层的单步驱动（接口第 7 节）：取出本层发出的请求、逐个回包、按脚本驱动。
//! 保留请求快照可以重放迟到或重复的回包；只回答本层的请求，其他请求原样留在累计动作里。
use crate::claim::status_read::{timer_token, StatusDone};
use crate::claim::{step, Action, Event, Machine, Operation, RequestContext, TimerKind};
use serde_json::{json, Value};
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StatusPhase {
    Metadata,
    Turns,
    Items,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StatusRequest {
    pub read_id: u64,
    pub generation: u64,
    pub phase: StatusPhase,
    pub candidate: Option<String>,
    pub method: String,
    pub params: Option<Value>,
    pub context: RequestContext,
}
pub(crate) enum StatusStep {
    /// 回包。`generation` 非零时原样回给这个请求（可以是旧请求，用于重复、迟到回包）；
    /// 为零时是「下一个请求」的模板：回给编号对应（`read_id` 为零则唯一在途）读取的当前请求，
    /// 并核对阶段与候选（`candidate` 为 Some 时）。模板用 [`expect`]、[`expect_items`] 构造。
    Reply(StatusRequest, Result<Value, Value>),
    /// 插入任意事件（通知、计时、超时等），直接走现有入口。
    Event(Event),
}

fn request_of(context: &RequestContext) -> Option<StatusRequest> {
    let Operation::StatusRead {
        read_id,
        generation,
    } = context.operation
    else {
        return None;
    };
    let phase = match context.method.as_str() {
        "thread/read" => StatusPhase::Metadata,
        "thread/turns/list" => StatusPhase::Turns,
        "thread/items/list" => StatusPhase::Items,
        other => panic!("状态读取发出了意外的方法 {other}"),
    };
    let candidate = (phase == StatusPhase::Items)
        .then(|| {
            context
                .params
                .as_ref()
                .and_then(|p| p.get("turnId"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .flatten();
    Some(StatusRequest {
        read_id,
        generation,
        phase,
        candidate,
        method: context.method.clone(),
        params: context.params.clone(),
        context: context.clone(),
    })
}

pub(crate) fn status_requests(actions: &[Action]) -> Vec<StatusRequest> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::Request(context) => request_of(context),
            _ => None,
        })
        .collect()
}

pub(crate) fn reply_status(
    machine: &mut Machine,
    request: &StatusRequest,
    result: Result<Value, Value>,
) -> Vec<Action> {
    apply(
        machine,
        Event::Reply {
            context: request.context.clone(),
            result,
        },
    )
}

pub(crate) fn drive_status(
    machine: &mut Machine,
    script: Vec<StatusStep>,
) -> (Vec<Action>, Vec<StatusDone>) {
    let observed = machine.status_reads.observed.len();
    let mut actions = Vec::new();
    for step in script {
        match step {
            StatusStep::Reply(template, result) => {
                let request = if template.generation != 0 {
                    template
                } else {
                    resolve(machine, &template)
                };
                actions.extend(reply_status(machine, &request, result));
            }
            StatusStep::Event(event) => actions.extend(apply(machine, event)),
        }
    }
    let done = machine.status_reads.observed[observed..].to_vec();
    (actions, done)
}

fn apply(machine: &mut Machine, event: Event) -> Vec<Action> {
    let (next, actions) = step(std::mem::take(machine), event);
    *machine = next;
    actions
}

/// 把模板换成在途读取的当前请求；找不到或阶段不符说明脚本写错，直接 panic。
fn resolve(machine: &Machine, template: &StatusRequest) -> StatusRequest {
    let live = live_status_requests(machine);
    let matching: Vec<_> = live
        .into_iter()
        .filter(|r| template.read_id == 0 || r.read_id == template.read_id)
        .collect();
    assert_eq!(
        matching.len(),
        1,
        "脚本回包找不到唯一的在途状态读取：模板 {template:?}"
    );
    let request = matching.into_iter().next().unwrap();
    assert_eq!(
        request.phase, template.phase,
        "在途请求的阶段与脚本不符：{request:?}"
    );
    if template.candidate.is_some() {
        assert_eq!(
            request.candidate, template.candidate,
            "在途条目请求的候选与脚本不符"
        );
    }
    request
}

/// 各在途读取的当前请求（已被回包消费、还没发出下一页的读取不在其中）。
pub(crate) fn live_status_requests(machine: &Machine) -> Vec<StatusRequest> {
    machine
        .status_reads
        .reads
        .iter()
        .filter(|(_, read)| read.generation != 0)
        .filter_map(|(id, _)| machine.status_request_context(*id))
        .filter_map(|context| request_of(&context))
        .collect()
}

/// 「下一个请求」模板：不限读取编号，只核对阶段。
pub(crate) fn expect(phase: StatusPhase) -> StatusRequest {
    StatusRequest {
        read_id: 0,
        generation: 0,
        phase,
        candidate: None,
        method: String::new(),
        params: None,
        context: RequestContext {
            resume_retried: false,
            stream_id: None,
            review_id: None,
            client: None,
            token: None,
            operation: Operation::Forward,
            method: String::new(),
            params: None,
        },
    }
}

/// 「下一个条目请求」模板，核对候选轮次。
pub(crate) fn expect_items(candidate: &str) -> StatusRequest {
    StatusRequest {
        candidate: Some(candidate.into()),
        ..expect(StatusPhase::Items)
    }
}

impl StatusRequest {
    /// 限定模板只匹配指定读取。
    pub(crate) fn of(mut self, read_id: u64) -> Self {
        self.read_id = read_id;
        self
    }
}

/// 读取总期限到期的计时事件。
pub(crate) fn deadline_event(machine: &Machine, read_id: u64) -> Event {
    let read = &machine.status_reads.reads[&read_id];
    Event::Timer {
        token: timer_token(&read.thread, &None),
        kind: TimerKind::StatusReadDeadline { read_id },
    }
}

/// 元数据回包：`{"thread":{"id","status":{"type"}}}`。
pub(crate) fn meta(thread: &str, status: &str) -> Value {
    let status = if status == "active" {
        json!({"type": "active", "activeFlags": []})
    } else {
        json!({"type": status})
    };
    json!({"thread": {"id": thread, "status": status, "turns": []}})
}

/// 轮次页：`turns` 为 (id, status)，按服务器给出的顺序（倒序）排列。
pub(crate) fn turns_page(turns: &[(&str, &str)], next: Option<&str>) -> Value {
    let data: Vec<Value> = turns
        .iter()
        .map(|(id, status)| {
            json!({"id": id, "status": status, "items": [], "itemsView": "notLoaded", "error": null})
        })
        .collect();
    json!({"data": data, "nextCursor": next, "backwardsCursor": null})
}

/// 条目页：`items` 是完整的条目对象，按 turnId 包装。
pub(crate) fn items_page(turn: &str, items: &[Value], next: Option<&str>) -> Value {
    let data: Vec<Value> = items
        .iter()
        .map(|item| json!({"turnId": turn, "item": item, "startedAtMs": 1, "completedAtMs": 2}))
        .collect();
    json!({"data": data, "nextCursor": next, "backwardsCursor": null})
}

pub(crate) fn user_message(text: &str) -> Value {
    json!({"type": "userMessage", "id": format!("u-{text}"), "content": [{"type": "text", "text": text, "text_elements": []}]})
}

pub(crate) fn agent_message(text: &str) -> Value {
    json!({"type": "agentMessage", "id": format!("a-{text}"), "text": text})
}

/// 线程通知（`turn/started`、`turn/completed`）。
pub(crate) fn turn_event(method: &str, thread: &str, turn: &str) -> Event {
    let status = if method == "turn/started" {
        "inProgress"
    } else {
        "completed"
    };
    Event::Notification {
        method: method.into(),
        params: Some(
            json!({"threadId": thread, "turn": {"id": turn, "status": status, "items": [], "error": null}}),
        ),
    }
}

pub(crate) fn error_reply(code: i64, message: &str) -> Value {
    json!({"code": code, "message": message})
}
