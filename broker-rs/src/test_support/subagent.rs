//! SA-a 共享模拟驱动，按来源线程隔离，默认不处理。
//!
//! 事件按真实时序构造（`S4-子代理事件实测`、上游 0.160.0）：
//! - v1 spawnAgent：item/started 的 receiverThreadIds 为空，成功后的 item/completed 才带 C；失败没有 C。
//! - v1 sendInput/resumeAgent：item/started 已带接收方。
//! - v2：操作成功后连续发出同一 itemId、同一内容的 subAgentActivity started/completed。
//! - 子线程的 thread/started 默认不发（实测两轮都没有收到）。
//!
//! `inject` 单步驱动状态机，并按线程记录实际收到的回答与次数、中断、goal 写入和附着请求；
//! `answer_reads` 用登记的线程元数据回答关系回查与发现请求。
use crate::claim::{Action, Event, Machine, Operation};
use serde_json::{json, Value};
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub(crate) struct SubagentMock {
    pub threads: std::collections::BTreeMap<String, Value>,
    /// 注入过的服务端请求：requestId → 来源线程。
    requests: BTreeMap<String, String>,
    next_request: u64,
}

fn key(id: &Value) -> String {
    crate::proto::request_id_key(id)
}

impl SubagentMock {
    pub(crate) fn inject(&mut self, machine: &mut Machine, event: Event) -> Vec<Action> {
        let (next, actions) = crate::claim::step(std::mem::take(machine), event);
        *machine = next;
        self.observe(&actions);
        actions
    }

    pub(crate) fn state(&self) -> Value {
        json!(self.threads)
    }

    fn thread(&mut self, thread: &str) -> &mut Value {
        self.threads.entry(thread.into()).or_insert_with(|| {
            json!({"turns": [], "pendingRequests": [], "answers": [], "interrupts": [], "goalWrites": [], "attachments": [], "meta": null})
        })
    }

    /// 记录状态机对注入请求的实际回答（同一 requestId 的每次回答都计数）、中断、goal 写入与附着请求。
    fn observe(&mut self, actions: &[Action]) {
        for action in actions {
            match action {
                Action::ServerResult { id, result } => self.answer(id, json!({"result": result})),
                Action::ServerError { id, error } => self.answer(id, json!({"error": error})),
                Action::AppReplyAcked { id, payload, .. } => self.answer(
                    id,
                    match payload {
                        Ok(r) => json!({"result": r, "acked": true}),
                        Err(e) => json!({"error": e, "acked": true}),
                    },
                ),
                Action::Dismiss { id } => self.answer(id, json!({"dismiss": true})),
                Action::Request(context) => {
                    let thread = context
                        .params
                        .as_ref()
                        .and_then(|p| p.get("threadId"))
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                    let Some(thread) = thread.filter(|t| self.threads.contains_key(t)) else {
                        continue;
                    };
                    let field = match context.method.as_str() {
                        "turn/interrupt" => "interrupts",
                        m if m.starts_with("thread/goal/") && m != "thread/goal/get" => {
                            "goalWrites"
                        }
                        "thread/resume" => "attachments",
                        _ => continue,
                    };
                    let params = context.params.clone().unwrap_or(Value::Null);
                    self.thread(&thread)[field]
                        .as_array_mut()
                        .expect("列表")
                        .push(params);
                }
                _ => {}
            }
        }
    }

    fn answer(&mut self, id: &Value, outcome: Value) {
        let Some(thread) = self.requests.get(&key(id)).cloned() else {
            return;
        };
        let answers = self.thread(&thread)["answers"]
            .as_array_mut()
            .expect("列表");
        if let Some(entry) = answers.iter_mut().find(|a| a["requestId"] == *id) {
            entry["count"] = json!(entry["count"].as_u64().unwrap_or(0) + 1);
            entry["last"] = outcome;
        } else {
            answers.push(json!({"requestId": id, "count": 1, "last": outcome}));
        }
    }

    /// 某个注入请求实际收到的回答次数。
    pub(crate) fn answer_count(&self, thread: &str, id: &Value) -> u64 {
        self.threads
            .get(thread)
            .and_then(|t| t["answers"].as_array())
            .and_then(|a| a.iter().find(|x| x["requestId"] == *id))
            .and_then(|x| x["count"].as_u64())
            .unwrap_or(0)
    }

    /// 登记线程元数据，供 thread/read、thread/list 回答。
    pub(crate) fn register(
        &mut self,
        thread: &str,
        parent: Option<&str>,
        version: &str,
        archived: bool,
        loaded: bool,
    ) {
        let meta = json!({
            "id": thread,
            "parentThreadId": parent,
            "source": match parent {
                Some(p) => json!({"subAgent": {"thread_spawn": {"parent_thread_id": p, "depth": 1,
                    "agent_path": if version == "v2" { json!(format!("/root/{thread}")) } else { Value::Null },
                    "agent_nickname": "代理", "agent_role": if version == "v1" { json!("default") } else { Value::Null }}}}),
                None => json!("vscode"),
            },
            "canAcceptDirectInput": version != "v2",
            "status": {"type": "idle"},
            "archived": archived,
            "loaded": loaded,
        });
        self.thread(thread)["meta"] = meta;
    }

    pub(crate) fn thread_info(&self, thread: &str) -> Option<Value> {
        self.threads
            .get(thread)
            .map(|t| t["meta"].clone())
            .filter(|m| !m.is_null())
    }

    // ─── 事件构造 ─────────────────────────────────────────────────────

    fn item(method: &str, thread: &str, turn: &str, item: Value) -> Event {
        Event::Notification {
            method: method.into(),
            params: Some(json!({"threadId": thread, "turnId": turn, "item": item})),
        }
    }

    fn collab(item_id: &str, tool: &str, status: &str, parent: &str, receivers: &[&str]) -> Value {
        json!({"type": "collabAgentToolCall", "id": item_id, "tool": tool, "status": status,
            "senderThreadId": parent, "receiverThreadIds": receivers, "prompt": null, "model": null,
            "reasoningEffort": null, "agentsStates": {}})
    }

    /// v1 spawnAgent：started 不带接收方；成功的 completed 才带 C；失败时 completed 也没有 C。
    pub(crate) fn v1_spawn(
        parent: &str,
        turn: &str,
        item_id: &str,
        child: &str,
        fail: bool,
    ) -> (Event, Event) {
        let started = Self::item(
            "item/started",
            parent,
            turn,
            Self::collab(item_id, "spawnAgent", "inProgress", parent, &[]),
        );
        let completed = if fail {
            Self::item(
                "item/completed",
                parent,
                turn,
                Self::collab(item_id, "spawnAgent", "failed", parent, &[]),
            )
        } else {
            Self::item(
                "item/completed",
                parent,
                turn,
                Self::collab(item_id, "spawnAgent", "completed", parent, &[child]),
            )
        };
        (started, completed)
    }

    /// v1 sendInput/resumeAgent：started 已带接收方。
    pub(crate) fn v1_message(
        tool: &str,
        parent: &str,
        turn: &str,
        item_id: &str,
        child: &str,
        fail: bool,
    ) -> (Event, Event) {
        (
            Self::item(
                "item/started",
                parent,
                turn,
                Self::collab(item_id, tool, "inProgress", parent, &[child]),
            ),
            Self::item(
                "item/completed",
                parent,
                turn,
                Self::collab(
                    item_id,
                    tool,
                    if fail { "failed" } else { "completed" },
                    parent,
                    &[child],
                ),
            ),
        )
    }

    /// v2 subAgentActivity：成功后连续发出，两条通知 itemId 与内容相同。
    pub(crate) fn v2_activity(
        kind: &str,
        parent: &str,
        turn: &str,
        item_id: &str,
        child: &str,
    ) -> (Event, Event) {
        let item = json!({"type": "subAgentActivity", "id": item_id, "kind": kind,
            "agentThreadId": child, "agentPath": format!("/root/{child}")});
        (
            Self::item("item/started", parent, turn, item.clone()),
            Self::item("item/completed", parent, turn, item),
        )
    }

    pub(crate) fn turn_started(&mut self, thread: &str, turn: &str) -> Event {
        self.thread(thread)["turns"]
            .as_array_mut()
            .expect("列表")
            .push(json!({"id": turn, "status": "inProgress"}));
        Event::Notification {
            method: "turn/started".into(),
            params: Some(
                json!({"threadId": thread, "turn": {"id": turn, "items": [], "status": "inProgress", "error": null}}),
            ),
        }
    }

    pub(crate) fn turn_completed(&mut self, thread: &str, turn: &str, status: &str) -> Event {
        if let Some(t) = self.thread(thread)["turns"]
            .as_array_mut()
            .expect("列表")
            .iter_mut()
            .find(|t| t["id"] == turn)
        {
            t["status"] = json!(status);
        }
        Event::Notification {
            method: "turn/completed".into(),
            params: Some(
                json!({"threadId": thread, "turn": {"id": turn, "items": [], "status": status, "error": null}}),
            ),
        }
    }

    /// 子线程的 thread/started（实测未出现；只在需要验证「若到达」时使用）。
    pub(crate) fn thread_started(&self, thread: &str) -> Event {
        Event::Notification {
            method: "thread/started".into(),
            params: Some(
                json!({"thread": self.thread_info(thread).unwrap_or(json!({"id": thread}))}),
            ),
        }
    }

    /// 子线程上的服务端请求；不给 id 时自动编号。可重复注入同一 id。
    pub(crate) fn request(
        &mut self,
        thread: &str,
        turn: Option<&str>,
        id: Option<Value>,
        method: &str,
    ) -> (Value, Event) {
        let id = id.unwrap_or_else(|| {
            self.next_request += 1;
            json!(self.next_request)
        });
        self.requests.insert(key(&id), thread.into());
        let mut params =
            json!({"threadId": thread, "itemId": "条目", "command": "echo", "cwd": "/tmp"});
        if let Some(turn) = turn {
            params["turnId"] = json!(turn);
        }
        self.thread(thread)["pendingRequests"]
            .as_array_mut()
            .expect("列表")
            .push(json!({"id": id, "method": method}));
        let forward_id = format!(
            "broker-1-sa{:06}",
            self.next_request + 1000 * self.requests.len() as u64
        );
        (
            id.clone(),
            Event::ServerRequest {
                id,
                forward_id,
                method: method.into(),
                params: Some(params),
            },
        )
    }

    pub(crate) fn resolved(thread: &str, id: &Value) -> Event {
        Event::Notification {
            method: "serverRequest/resolved".into(),
            params: Some(json!({"threadId": thread, "requestId": id})),
        }
    }

    /// 用登记的元数据回答动作里的关系回查（thread/read）；返回回答后产生的动作。
    pub(crate) fn answer_reads(
        &mut self,
        machine: &mut Machine,
        actions: &[Action],
    ) -> Vec<Action> {
        let mut out = Vec::new();
        for action in actions {
            let Action::Request(context) = action else {
                continue;
            };
            if !matches!(context.operation, Operation::RelationLookup { .. })
                || context.method != "thread/read"
            {
                continue;
            }
            let thread = context
                .params
                .as_ref()
                .and_then(|p| p["threadId"].as_str())
                .unwrap_or_default();
            let result = match self.thread_info(thread) {
                Some(info) => Ok(json!({"thread": info})),
                None => Err(json!({"code": -32600, "message": "thread not found"})),
            };
            out.extend(self.inject(
                machine,
                Event::Reply {
                    context: context.clone(),
                    result,
                },
            ));
        }
        out
    }
}
