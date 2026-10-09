//! SA-b 单测：派生停止、整树停止、结束门禁与专用提交、能力与生命周期门禁。
//!
//! `Sim` 是一个纯状态机外的小模拟世界：按线程保存轮次与 goal，自动回答树停止、读取层、发现与关系回查
//! 发出的请求（可按方法或线程扣住），中断按线程配置立即结束或永不结束；计时按单调毫秒登记，
//! `advance` 推进时钟并按到期顺序触发。不用固定延迟，交错都用「扣住、放行」表达。
use crate::claim::{Action, ClaimToken, ClientRequest, Event, Machine, Operation, RequestContext};
use crate::claim::{State, TimerKind};
use crate::relations::BindingStatus;
use crate::test_support::status_read::{meta, turns_page};
use crate::test_support::subagent::SubagentMock;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

mod derived;
mod fx_stop;
mod gate;
mod lifecycle;
mod tree;

pub(super) const P: &str = "父线程";
pub(super) const T: &str = "父轮1";
pub(super) const R: &str = "运行甲";
pub(super) const OWNER: u64 = 1;

/// 登记中的计时：(到期单调毫秒, 序号, token, kind)。
type Timer = (u64, u64, ClaimToken, TimerKind);

pub(super) struct Sim {
    pub m: Machine,
    pub mock: SubagentMock,
    /// 每线程的轮次 (id, status)，按开始顺序。
    pub turns: BTreeMap<String, Vec<(String, String)>>,
    pub goals: BTreeMap<String, Value>,
    pub background: BTreeMap<String, Vec<Value>>,
    pub background_supported: bool,
    pub background_missing: BTreeSet<String>,
    pub metadata_unloaded: BTreeSet<String>,
    pub terminate_false: bool,
    /// 中断永不生效的线程。
    pub never: BTreeSet<String>,
    /// 不自动回答的 (方法, 线程)；线程为空表示所有线程。
    pub hold: Vec<(String, String)>,
    pub held: Vec<RequestContext>,
    /// 发现时的已加载线程与父 → 子。
    pub children: BTreeMap<String, Vec<String>>,
    /// 发现失败（thread/list 回错误）。
    pub list_fault: bool,
    pub timers: Vec<Timer>,
    seq: u64,
    pub log: Vec<Action>,
    next_id: u64,
}

impl Sim {
    /// 私有模式，cxb 连接 1 在父线程 P 上开运行 R、首轮 T 已开始。
    pub fn new() -> Self {
        let mut sim = Sim {
            m: Machine::new(false),
            mock: SubagentMock::default(),
            turns: BTreeMap::new(),
            goals: BTreeMap::new(),
            background: BTreeMap::new(),
            background_supported: true,
            background_missing: BTreeSet::new(),
            metadata_unloaded: BTreeSet::new(),
            terminate_false: false,
            never: BTreeSet::new(),
            hold: Vec::new(),
            held: Vec::new(),
            children: BTreeMap::new(),
            list_fault: false,
            timers: Vec::new(),
            seq: 0,
            log: Vec::new(),
            next_id: 100,
        };
        sim.m.clock.mono_ms = 1_000;
        sim.m.clock.now_ms = 1_700_000_000_000;
        sim.mock.register(P, None, "v1", false, true);
        sim.apply(Event::Connected { conn: OWNER });
        sim.apply(Event::Initialized {
            conn: OWNER,
            cxb: true,
        });
        sim.apply(Event::Connected { conn: 2 });
        sim.apply(Event::Initialized { conn: 2, cxb: true });
        sim.m.records.entry(P.into()).or_default().subscribed = true;
        let actions = sim.client(
            OWNER,
            "turn/start",
            json!({"threadId": P, "input": [{"type": "text", "text": "父任务"}]}),
            Some(R),
        );
        let start = actions
            .iter()
            .find_map(|a| match a {
                Action::Request(c) if c.method == "turn/start" => Some(c.clone()),
                _ => None,
            })
            .expect("turn/start 已转发");
        sim.turns
            .entry(P.into())
            .or_default()
            .push((T.into(), "inProgress".into()));
        sim.apply(Event::Reply {
            context: start,
            result: Ok(json!({"turn": {"id": T}})),
        });
        sim.notify(json!({"method": "turn/started", "params": {"threadId": P,
            "turn": {"id": T, "status": "inProgress", "items": [], "error": null}}}));
        sim
    }

    /// 注入一个事件并自动回答它产生的请求（直到没有可答的）。
    pub fn apply(&mut self, event: Event) -> Vec<Action> {
        let actions = self.mock.inject(&mut self.m, event);
        self.absorb(actions)
    }

    pub fn absorb(&mut self, actions: Vec<Action>) -> Vec<Action> {
        let mut all = Vec::new();
        let mut queue: VecDeque<Action> = actions.into_iter().collect();
        let mut guard = 0;
        while let Some(action) = queue.pop_front() {
            guard += 1;
            if guard > 20_000 {
                let tail: Vec<String> = all
                    .iter()
                    .rev()
                    .take(40)
                    .map(|a: &Action| match a {
                        Action::Request(c) => {
                            format!("请求 {} {:?} {:?}", c.method, c.params, c.operation)
                        }
                        other => format!("{other:?}").chars().take(200).collect(),
                    })
                    .collect();
                panic!("模拟世界未收敛：{tail:#?}");
            }
            self.track_timer(&action);
            all.push(action.clone());
            self.log.push(action.clone());
            let Action::Request(context) = action else {
                continue;
            };
            let Some(events) = self.answer(&context) else {
                self.held.push(context);
                continue;
            };
            for event in events {
                let next = self.mock.inject(&mut self.m, event);
                queue.extend(next);
            }
        }
        all
    }

    fn track_timer(&mut self, action: &Action) {
        match action {
            Action::Schedule {
                token,
                kind,
                delay_ms,
            } => {
                if let Some(key) = kind.key(token) {
                    self.timers
                        .retain(|(_, _, t, k)| k.key(t).as_ref() != Some(&key));
                }
                self.seq += 1;
                self.timers.push((
                    self.m.clock.mono_ms + delay_ms,
                    self.seq,
                    token.clone(),
                    kind.clone(),
                ));
            }
            Action::Unschedule { key } => {
                self.timers
                    .retain(|(_, _, t, k)| k.key(t).as_ref() != Some(key));
            }
            _ => {}
        }
    }

    /// 推进时钟并按到期顺序触发计时。
    pub fn advance(&mut self, ms: u64) -> Vec<Action> {
        let target = self.m.clock.mono_ms + ms;
        let mut all = Vec::new();
        loop {
            self.timers.sort_by_key(|(at, seq, _, _)| (*at, *seq));
            let Some(index) = self.timers.iter().position(|(at, ..)| *at <= target) else {
                break;
            };
            let (at, _, token, kind) = self.timers.remove(index);
            let delta = at.saturating_sub(self.m.clock.mono_ms);
            self.m.clock.mono_ms += delta;
            self.m.clock.now_ms += delta;
            all.extend(self.apply(Event::Timer { token, kind }));
        }
        let delta = target - self.m.clock.mono_ms;
        self.m.clock.mono_ms += delta;
        self.m.clock.now_ms += delta;
        all
    }

    pub fn client(
        &mut self,
        conn: u64,
        method: &str,
        params: Value,
        run: Option<&str>,
    ) -> Vec<Action> {
        self.next_id += 1;
        let request = ClientRequest {
            conn,
            id: json!(self.next_id),
            method: method.into(),
            params: Some(params),
        };
        self.apply(Event::ClientRequest {
            request,
            run_id: run.map(str::to_owned),
        })
    }

    /// 客户端请求并返回回包（结果或错误对象）。
    pub fn call(&mut self, conn: u64, method: &str, params: Value) -> (u64, Vec<Action>) {
        let actions = self.client(conn, method, params, None);
        (self.next_id, actions)
    }

    pub fn notify(&mut self, message: Value) -> Vec<Action> {
        self.apply(Event::Notification {
            method: message["method"].as_str().unwrap().into(),
            params: Some(message["params"].clone()),
        })
    }

    pub fn turn_started(&mut self, thread: &str, turn: &str) -> Vec<Action> {
        self.turns
            .entry(thread.into())
            .or_default()
            .push((turn.into(), "inProgress".into()));
        let event = self.mock.turn_started(thread, turn);
        self.apply(event)
    }

    pub fn turn_completed(&mut self, thread: &str, turn: &str, status: &str) -> Vec<Action> {
        if let Some(t) = self
            .turns
            .entry(thread.into())
            .or_default()
            .iter_mut()
            .find(|(id, _)| id == turn)
        {
            t.1 = status.into();
        }
        let event = self.mock.turn_completed(thread, turn, status);
        self.apply(event)
    }

    /// v1 派生子线程 C 并开始首轮（绑定到父轮 T 所在的运行）。
    pub fn spawn_v1(&mut self, child: &str, item: &str, child_turn: &str) -> Vec<Action> {
        self.spawn(child, item, child_turn, "v1")
    }

    pub fn spawn(
        &mut self,
        child: &str,
        item: &str,
        child_turn: &str,
        version: &str,
    ) -> Vec<Action> {
        self.mock.register(child, Some(P), version, false, true);
        self.children
            .entry(P.into())
            .or_default()
            .push(child.into());
        let mut all = Vec::new();
        if version == "v2" {
            let (started, completed) = SubagentMock::v2_activity("started", P, T, item, child);
            all.extend(self.apply(started));
            all.extend(self.apply(completed));
        } else {
            let (started, completed) = SubagentMock::v1_spawn(P, T, item, child, false);
            all.extend(self.apply(started));
            all.extend(self.apply(completed));
        }
        all.extend(self.turn_started(child, child_turn));
        all
    }

    pub fn bound(&self, thread: &str, turn: &str) -> bool {
        self.m
            .binding(thread, turn)
            .is_some_and(|b| b.status == BindingStatus::Bound && b.run_id == R)
    }

    fn held_by(&self, context: &RequestContext) -> bool {
        let thread = context
            .params
            .as_ref()
            .and_then(|p| p.get("threadId"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        self.hold
            .iter()
            .any(|(m, t)| *m == context.method && (t.is_empty() || t == thread))
    }

    /// 放行扣住的请求（满足条件的），按原样回答。
    pub fn release(&mut self, method: &str, thread: &str) -> Vec<Action> {
        self.hold
            .retain(|(m, t)| !(m == method && (thread.is_empty() || t == thread)));
        let held = std::mem::take(&mut self.held);
        let mut all = Vec::new();
        for context in held {
            if self.held_by(&context) || self.answer_kind(&context).is_none() {
                self.held.push(context);
                continue;
            }
            let ctx_thread = context
                .params
                .as_ref()
                .and_then(|p| p.get("threadId"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            if context.method == method && (thread.is_empty() || ctx_thread == thread) {
                let events = self.answer(&context).unwrap_or_default();
                for event in events {
                    let next = self.mock.inject(&mut self.m, event);
                    all.extend(self.absorb(next));
                }
            } else {
                self.held.push(context);
            }
        }
        all
    }

    /// 回答一个扣住的读取层请求（`Operation::StatusRead`）。
    pub fn reply_status_read(
        &mut self,
        method: &str,
        thread: &str,
        result: Result<Value, Value>,
    ) -> Vec<Action> {
        let index = self
            .held
            .iter()
            .position(|c| {
                c.method == method
                    && matches!(c.operation, Operation::StatusRead { .. })
                    && c.params.as_ref().and_then(|p| p["threadId"].as_str()) == Some(thread)
            })
            .unwrap_or_else(|| panic!("没有扣住的读取 {method} {thread}"));
        let context = self.held.remove(index);
        self.apply(Event::Reply { context, result })
    }

    /// 让子线程的 thread/started 到达，SA-a 由此学到能力值（canAcceptDirectInput）。
    pub fn learn_thread(&mut self, thread: &str) -> Vec<Action> {
        let event = self.mock.thread_started(thread);
        self.apply(event)
    }

    fn active(&self, thread: &str) -> Vec<String> {
        self.turns
            .get(thread)
            .map(|t| {
                t.iter()
                    .filter(|(_, s)| s == "inProgress")
                    .map(|(id, _)| id.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// 请求是否属于模拟世界会回答的类别。
    fn answer_kind(&self, context: &RequestContext) -> Option<()> {
        let inner = unwrap_operation(&context.operation);
        match inner {
            Operation::Stop(..)
            | Operation::DerivedStop { .. }
            | Operation::StatusRead { .. }
            | Operation::Discovery { .. }
            | Operation::RelationLookup { .. } => Some(()),
            _ => None,
        }
    }

    /// 自动回答；扣住或不认识的请求返回 None。
    fn answer(&mut self, context: &RequestContext) -> Option<Vec<Event>> {
        self.answer_kind(context)?;
        if self.held_by(context) {
            return None;
        }
        let params = context.params.clone().unwrap_or(Value::Null);
        let thread = params["threadId"].as_str().unwrap_or_default().to_owned();
        let reply = |result: Result<Value, Value>| Event::Reply {
            context: context.clone(),
            result,
        };
        let inner = unwrap_operation(&context.operation);
        let mut events = Vec::new();
        match (context.method.as_str(), inner) {
            ("thread/backgroundTerminals/list", _) => events.push(reply(if self.background_missing.contains(&thread) {
                    Err(json!({"code":-32600,"message":"thread not found"}))
                } else if self.background_supported {
                Ok(json!({"data":self.background.get(&thread).cloned().unwrap_or_default(),"nextCursor":null}))
            } else { Err(json!({"code":-32601,"message":"方法不存在"})) })),
            ("thread/backgroundTerminals/terminate", _) => {
                self.background.entry(thread).or_default().retain(|v| v["processId"] != params["processId"]);
                events.push(reply(Ok(json!({"terminated":!self.terminate_false}))));
            }

            ("thread/goal/get", _) => {
                let goal = self.goals.get(&thread).cloned().unwrap_or(Value::Null);
                events.push(reply(Ok(json!({"goal": goal}))));
            }
            ("thread/goal/set", _) => {
                if let Some(goal) = self.goals.get_mut(&thread) {
                    if !goal.is_null() {
                        goal["status"] = params["status"].clone();
                    }
                }
                let goal = self.goals.get(&thread).cloned().unwrap_or(Value::Null);
                events.push(reply(Ok(json!({"goal": goal}))));
            }
            ("thread/goal/clear", _) => {
                self.goals.insert(thread.clone(), Value::Null);
                events.push(reply(Ok(json!({}))));
            }
            ("turn/interrupt", _) => {
                events.push(reply(Ok(json!({}))));
                let turn = params["turnId"].as_str().unwrap_or_default().to_owned();
                if !self.never.contains(&thread) && self.active(&thread).contains(&turn) {
                    if let Some(t) = self
                        .turns
                        .entry(thread.clone())
                        .or_default()
                        .iter_mut()
                        .find(|(id, _)| *id == turn)
                    {
                        t.1 = "interrupted".into();
                    }
                    events.push(self.mock.turn_completed(&thread, &turn, "interrupted"));
                }
            }
            ("thread/read", Operation::StatusRead { .. }) => {
                let status = if self.metadata_unloaded.contains(&thread) { "notLoaded" } else if self.active(&thread).is_empty() {
                    "idle"
                } else {
                    "active"
                };
                events.push(reply(Ok(meta(&thread, status))));
            }
            ("thread/turns/list", Operation::StatusRead { .. }) => {
                let active = self.active(&thread);
                let page: Vec<(&str, &str)> =
                    active.iter().map(|t| (t.as_str(), "inProgress")).collect();
                events.push(reply(Ok(turns_page(&page, None))));
            }
            ("thread/read", Operation::Stop(..)) => {
                let turns: Vec<Value> = self
                    .turns
                    .get(&thread)
                    .map(|t| {
                        t.iter()
                            .map(|(id, s)| json!({"id": id, "status": s}))
                            .collect()
                    })
                    .unwrap_or_default();
                events.push(reply(Ok(json!({"thread": {"id": thread, "turns": turns}}))));
            }
            ("thread/read", _) => {
                let result = match self.mock.thread_info(&thread) {
                    Some(info) => Ok(json!({"thread": info})),
                    None => Err(json!({"code": -32600, "message": "thread not found"})),
                };
                events.push(reply(result));
            }
            ("thread/list", _) => {
                if self.list_fault {
                    events.push(reply(Err(json!({"code": -32000, "message": "列表失败"}))));
                } else {
                    let ancestor = params["ancestorThreadId"].as_str().unwrap_or_default();
                    let archived = params["archived"] == true;
                    let data: Vec<Value> = if archived {
                        Vec::new()
                    } else {
                        self.descendants(ancestor)
                            .iter()
                            .filter_map(|t| self.mock.thread_info(t))
                            .collect()
                    };
                    events.push(reply(Ok(json!({"data": data, "nextCursor": null}))));
                }
            }
            ("thread/loaded/list", _) => {
                let data: Vec<String> = self.mock.threads.keys().filter(|t| !self.metadata_unloaded.contains(*t)).cloned().collect();
                events.push(reply(Ok(json!({"data": data, "nextCursor": null}))));
            }
            _ => return None,
        }
        Some(events)
    }

    fn descendants(&self, root: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut stack = vec![root.to_owned()];
        while let Some(t) = stack.pop() {
            for c in self.children.get(&t).into_iter().flatten() {
                out.push(c.clone());
                stack.push(c.clone());
            }
        }
        out
    }

    /// 发给连接的某个请求编号的回包（result 或 error）。
    pub fn response(&self, actions: &[Action], id: u64) -> Option<Value> {
        actions.iter().find_map(|a| match a {
            Action::Send { message, .. }
                if message["id"] == id && message.get("method").is_none() =>
            {
                Some(message.clone())
            }
            _ => None,
        })
    }

    pub fn requests<'a>(
        &self,
        actions: &'a [Action],
        method: &str,
        thread: &str,
    ) -> Vec<&'a RequestContext> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Request(c)
                    if c.method == method
                        && c.params.as_ref().and_then(|p| p["threadId"].as_str())
                            == Some(thread) =>
                {
                    Some(c)
                }
                _ => None,
            })
            .collect()
    }

    pub fn interrupts(&self, actions: &[Action], thread: &str) -> usize {
        self.requests(actions, "turn/interrupt", thread).len()
    }

    pub fn record(&self) -> &crate::runs::RunRecord {
        &self.m.runs.runs[R]
    }

    pub fn tree_op(&self) -> Option<u64> {
        self.m.tree_for_run(R)
    }

    pub fn finishing(&self) -> bool {
        self.m
            .claims
            .get(P)
            .is_some_and(|c| matches!(c.state, State::FinishingTree { .. }))
    }

    /// 发给 owner 连接的 broker/run/ended 通知数。
    pub fn run_ended(&self, actions: &[Action]) -> usize {
        actions
            .iter()
            .filter(|a| {
                matches!(a, Action::Send { conn, message }
                    if *conn == OWNER && message["method"] == "broker/run/ended")
            })
            .count()
    }

    /// 运行记录的全部写盘快照。
    pub fn run_writes(&self, actions: &[Action]) -> Vec<crate::runs::RunRecord> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Store(crate::store::StoreOp::PutRun(r)) if r.run_id == R => {
                    Some((**r).clone())
                }
                _ => None,
            })
            .collect()
    }
}

pub(super) fn unwrap_operation(operation: &Operation) -> &Operation {
    match operation {
        Operation::GoalChange { inner, .. } | Operation::GoalRead { inner, .. } => {
            unwrap_operation(inner)
        }
        other => other,
    }
}

pub(super) fn goal(thread: &str, status: &str) -> Value {
    json!({"threadId": thread, "objective": "目标", "status": status, "createdAt": 1})
}
