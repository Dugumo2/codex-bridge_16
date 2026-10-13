//! 线程状态读取层（H2 接口第 2–5 节）：用元数据读与分页读取代 `includeTurns:true` 的全量读，
//! 把结果收成明确的几种状态。
//!
//! - 每次读取有唯一的非零编号；同一时刻最多一个在途请求，请求带「读取编号 + 页代号」，
//!   代号不符的回包（重复、迟到、超时后才到）一律丢弃，照 `runs/reads.rs` 的 `next_page` 换代。
//! - 活动版本号按线程维护，只在该线程有在途读取时登记；`turn/started`、`turn/completed` 首次到达
//!   （进入 held 之前）时推进，settling 回放不再推进。每个回包都与读取开始时的版本比对。
//! - 总期限 20 秒从首次发起算起，重读不续期；页数（含元数据读）与重读次数都不随重读清零。
//! - 每次读取的成功、失败、超时只回调一次，完成时先移除登记、撤销计时，再分派；关闭时静默取消。
use super::{Action, ClaimToken, Machine, Operation, RequestContext, TimerKey, TimerKind};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, HashSet};

/// 每次读取的总期限（毫秒），从首次发起算起。
pub const STATUS_READ_DEADLINE_MS: u64 = 20_000;
/// 一次读取（含重读）最多发出的请求数；元数据读也计入。
pub const STATUS_MAX_PAGES: usize = 1000;
/// 版本变化后最多从第一步重读的次数（Settlement 不重读）。
pub const STATUS_MAX_REREADS: u8 = 3;
/// Settlement 最多逐个核对的候选轮次数；超过时不截取，直接返回 `Unknown(PageBudget)`。
pub const STATUS_MAX_CANDIDATES: usize = 16;
/// `thread/turns/list` 每页条数（上游上限 100）。
pub const STATUS_TURNS_LIMIT: usize = 100;
/// `thread/items/list` 每页条数，与结果补读一致。
pub const STATUS_ITEMS_LIMIT: usize = 64;
const MAX_CURSOR_BYTES: usize = 4096;
/// 只保留对账和迟到归属的在途超时证据，容量覆盖两类登记的上限。
const MAX_DEADLINE_RPCS: usize = crate::stop::MAX_UNRESOLVED_STARTS + crate::stop::MAX_LATE;
/// 上游把「第一轮前」等不支持情形映射成方法不存在。
const METHOD_NOT_FOUND: i64 = -32601;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatusQuery {
    ClientMessage {
        client_user_message_id: String,
        expected_previous_turn_id: Option<String>,
    },
    Busy,
    Activity,
    Turn {
        target: String,
    },
    Settlement {
        candidates: Vec<String>,
        input: Vec<String>,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusUnknown {
    Unsupported,
    Malformed,
    ErrorReply,
    PageBudget,
    DuplicateCursor,
    Changed,
    NotVisible,
    Timeout,
    Unresponsive,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusTurn {
    InProgress,
    Completed,
    Failed,
    Interrupted,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatusResult {
    NotBusy,
    Busy,
    Idle,
    Active(Vec<String>),
    Found { status: StatusTurn, shell: Value },
    Absent,
    FirstActive(String),
    Carrier(String),
    NoCarrier,
    Unknown(StatusUnknown),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatusCaller {
    SubagentReconcile {
        token: crate::sa1b::ConvergeToken,
        op_id: crate::sa1b::ReconcileOpId,
    },
    ContinueCheck {
        round: u64,
    },
    StopVerify {
        tag: crate::stop::StopTag,
    },
    Reconcile {
        generation: u64,
        round: u64,
    },
    LateSettlement {
        generation: u64,
        round: u64,
    },
    Settlement {
        attempt: u8,
    },
    JoinedCheck {
        attempt: u8,
    },
    DerivedStopVerify {
        tree_op: u64,
        thread: String,
        round: u64,
    },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusDone {
    pub read_id: u64,
    pub thread: String,
    pub token: Option<ClaimToken>,
    pub caller: StatusCaller,
    pub result: StatusResult,
}

/// 读取所处的步骤；请求的方法与参数完全由它和游标决定。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReadStage {
    /// Busy、Activity、Settlement 的第一步：只读元数据。
    Metadata,
    /// 分页读轮次（Activity 收集活动轮次，Turn 查找目标）。
    Turns,
    /// Turn 遍历完未找到后的元数据读。
    TurnMetadata,
    /// Settlement 第二步：逐个候选分页读条目。
    Items { index: usize },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadState {
    connection_epoch: u64,
    pub(crate) thread: String,
    token: Option<ClaimToken>,
    pub(crate) query: StatusQuery,
    pub(crate) caller: StatusCaller,
    /// 本轮（首读或最近一次重读）开始时的活动版本号。
    version: u64,
    /// 在途请求的页代号；0 表示没有在途请求（回包已消费）。
    pub(crate) generation: u64,
    pub(crate) stage: ReadStage,
    rereads: u8,
    /// 已发出的请求数，含元数据读与重读，不清零。
    requests: usize,
    cursor: Option<String>,
    /// 当前分页流内见过的游标；换候选或重读时重置。
    cursors: HashSet<String>,
    /// Activity 按倒序收集到的活动轮次。
    active: Vec<String>,
    /// clientId 查询的有界候选，前轮本身也必须检查（start 可能被 steer）。
    client_candidates: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StatusReads {
    next_read_id: u64,
    next_generation: u64,
    /// 按编号有序，关闭清理与测试观测的顺序稳定。
    pub(crate) reads: BTreeMap<u64, ReadState>,
    /// 总期限已回调、RPC 尚未结算；调用方轮次指向期限结束后安排的退避轮次。
    deadline_rpcs: BTreeMap<(u64, u64), StatusCaller>,
    /// 活动版本号；只为有在途读取的线程登记，最后一个读取结束时移除，不随线程数增长。
    versions: HashMap<String, u64>,
    /// 单步驱动观测到的完成（按完成顺序），只在测试中记录。
    #[cfg(test)]
    pub(crate) observed: Vec<StatusDone>,
}

impl StatusReads {
    fn allocate_read(&mut self) -> u64 {
        self.next_read_id = self
            .next_read_id
            .checked_add(1)
            .expect("状态读取编号已耗尽");
        self.next_read_id
    }
    fn allocate_generation(&mut self) -> u64 {
        self.next_generation = self
            .next_generation
            .checked_add(1)
            .expect("状态读取页代号已耗尽");
        self.next_generation
    }
    fn version(&self, thread: &str) -> u64 {
        self.versions.get(thread).copied().unwrap_or(0)
    }
    /// 线程的活动版本号是否已登记（即该线程有在途读取）。
    #[cfg(test)]
    pub(crate) fn tracks(&self, thread: &str) -> bool {
        self.versions.contains_key(thread)
    }
}

/// 计时用的认领标识：有调用方 token 就用它，否则用 claim_id 为 0 的占位（同结果补读）。
pub(crate) fn timer_token(thread: &str, token: &Option<ClaimToken>) -> ClaimToken {
    token.clone().unwrap_or(ClaimToken {
        thread_id: thread.to_owned(),
        claim_id: 0,
    })
}

impl Machine {
    /// 发起一次读取，返回唯一的非零编号。总是先发出第一个请求，从不同步回调。
    /// 关闭中发起的读取不发请求、不回调（与关闭时的静默取消一致）。
    pub fn begin_status_read(
        &mut self,
        thread: &str,
        token: Option<ClaimToken>,
        query: StatusQuery,
        caller: StatusCaller,
        actions: &mut Vec<Action>,
    ) -> u64 {
        let read_id = self.status_reads.allocate_read();
        if self.closing {
            actions.push(Action::Debug(format!(
                "关闭中不发起线程 {thread} 的状态读取 {read_id}"
            )));
            return read_id;
        }
        // 真正开始下一轮读取时，上一轮的无响应证据失效；仅安排退避不算被新读取取代。
        self.status_reads
            .deadline_rpcs
            .retain(|_, waiting| waiting != &caller);
        let version = *self
            .status_reads
            .versions
            .entry(thread.to_owned())
            .or_insert(0);
        let stage = match query {
            StatusQuery::Turn { .. } | StatusQuery::ClientMessage { .. } => ReadStage::Turns,
            _ => ReadStage::Metadata,
        };
        actions.push(Action::Schedule {
            token: timer_token(thread, &token),
            kind: TimerKind::StatusReadDeadline { read_id },
            delay_ms: STATUS_READ_DEADLINE_MS,
        });
        self.status_reads.reads.insert(
            read_id,
            ReadState {
                connection_epoch: self.relations.conn_epoch,
                client_candidates: Vec::new(),
                thread: thread.to_owned(),
                token,
                query,
                caller,
                version,
                generation: 0,
                stage,
                rereads: 0,
                requests: 0,
                cursor: None,
                cursors: HashSet::new(),
                active: Vec::new(),
            },
        );
        self.issue_status_page(read_id, actions);
        read_id
    }

    /// 按调用方分派完成结果；有效性（round、generation、token 是否当前）由各处理函数自己判断。
    pub fn status_read_done(&mut self, done: StatusDone, actions: &mut Vec<Action>) {
        #[cfg(test)]
        self.status_reads.observed.push(done.clone());
        match &done.caller {
            StatusCaller::SubagentReconcile { .. } => {
                self.subagent_reconcile_status_done(done, actions)
            }
            StatusCaller::ContinueCheck { .. } => self.continue_check_done(done, actions),
            StatusCaller::StopVerify { .. } => self.stop_verify_done(done, actions),
            StatusCaller::Reconcile { .. } => self.reconcile_status_done(done, actions),
            StatusCaller::LateSettlement { .. } => self.late_settlement_status_done(done, actions),
            StatusCaller::Settlement { .. } => self.settlement_status_done(done, actions),
            StatusCaller::JoinedCheck { .. } => self.joined_check_done(done, actions),
            StatusCaller::DerivedStopVerify { .. } => self.derived_stop_verify_done(done, actions),
        }
    }

    /// 读取的当前请求（不改状态）；单步驱动用它还原在途请求。
    pub(crate) fn status_request_context(&self, read_id: u64) -> Option<RequestContext> {
        let read = self.status_reads.reads.get(&read_id)?;
        let (method, params) = match &read.stage {
            ReadStage::Metadata | ReadStage::TurnMetadata => {
                ("thread/read", json!({"threadId": read.thread}))
            }
            ReadStage::Turns => (
                "thread/turns/list",
                json!({"threadId": read.thread, "cursor": read.cursor,
                    "limit": STATUS_TURNS_LIMIT, "sortDirection": "desc", "itemsView": "notLoaded"}),
            ),
            ReadStage::Items { index } => {
                let candidates = match &read.query {
                    StatusQuery::Settlement { candidates, .. } => candidates,
                    StatusQuery::ClientMessage { .. } => &read.client_candidates,
                    _ => return None,
                };
                (
                    "thread/items/list",
                    json!({"threadId": read.thread, "turnId": candidates.get(*index)?,
                        "cursor": read.cursor, "limit": STATUS_ITEMS_LIMIT, "sortDirection": "asc"}),
                )
            }
        };
        Some(RequestContext {
            resume_retried: false,
            stream_id: None,
            review_id: None,
            client: None,
            // 不带认领：读取不属于任何认领的启动或停止登记，迟到丢弃时不应触及它们。
            token: None,
            operation: Operation::StatusRead {
                read_id,
                generation: read.generation,
            },
            method: method.into(),
            params: Some(params),
        })
    }

    /// 换一个页代号发出当前步骤的请求；超过页数预算则以 `PageBudget` 完成。
    fn issue_status_page(&mut self, read_id: u64, actions: &mut Vec<Action>) {
        let generation = self.status_reads.allocate_generation();
        let Some(read) = self.status_reads.reads.get_mut(&read_id) else {
            return;
        };
        if read.requests >= STATUS_MAX_PAGES {
            self.finish_status_read(
                read_id,
                StatusResult::Unknown(StatusUnknown::PageBudget),
                actions,
            );
            return;
        }
        read.requests += 1;
        read.generation = generation;
        if let Some(context) = self.status_request_context(read_id) {
            actions.push(Action::Request(context));
        }
    }

    /// 完成一次读取：先移除登记与计时，再回调，保证只完成一次。
    fn finish_status_read(
        &mut self,
        read_id: u64,
        result: StatusResult,
        actions: &mut Vec<Action>,
    ) {
        let Some(read) = self.status_reads.reads.remove(&read_id) else {
            return;
        };
        actions.push(Action::Unschedule {
            key: TimerKey::StatusRead(read_id),
        });
        self.forget_status_version(&read.thread);
        self.status_read_done(
            StatusDone {
                read_id,
                thread: read.thread,
                token: read.token,
                caller: read.caller,
                result,
            },
            actions,
        );
    }

    fn forget_status_version(&mut self, thread: &str) {
        if !self.status_reads.reads.values().any(|r| r.thread == thread) {
            self.status_reads.versions.remove(thread);
        }
    }

    /// 活动版本号推进：只在 `arrive_notification` 进入 held 之前调用一次，回放不调用。
    pub(crate) fn status_activity_changed(&mut self, thread: &str) {
        if let Some(version) = self.status_reads.versions.get_mut(thread) {
            *version = version.wrapping_add(1);
        }
    }

    /// 总期限到期。
    pub(crate) fn status_read_deadline(&mut self, read_id: u64, actions: &mut Vec<Action>) {
        let pending = self.status_reads.reads.get(&read_id).and_then(|read| {
            if read.generation == 0 || !self.status_caller_reconciling(&read.caller) {
                return None;
            }
            let mut caller = read.caller.clone();
            match &mut caller {
                StatusCaller::Reconcile { round, .. }
                | StatusCaller::LateSettlement { round, .. } => *round += 1,
                _ => return None,
            }
            Some(((read_id, read.generation), caller))
        });
        if self.status_reads.reads.contains_key(&read_id) {
            self.finish_status_read(
                read_id,
                StatusResult::Unknown(StatusUnknown::Timeout),
                actions,
            );
        }
        // 完成回调会推进退避轮次，也可能结束登记；只留下仍有效的 RPC 证据。
        if let Some((key, caller)) = pending.filter(|(_, c)| self.status_caller_reconciling(c)) {
            if self.status_reads.deadline_rpcs.len() >= MAX_DEADLINE_RPCS {
                self.status_reads.deadline_rpcs.pop_first();
            }
            self.status_reads.deadline_rpcs.insert(key, caller);
        }
    }

    /// 在途 RPC 的调用方；总期限已结束的读取仍可提供熔断证据，但不得再次回调。
    pub(crate) fn status_read_caller(
        &self,
        read_id: u64,
        generation: u64,
    ) -> Option<&StatusCaller> {
        self.status_reads
            .reads
            .get(&read_id)
            .filter(|read| read.generation != 0 && read.generation == generation)
            .map(|read| &read.caller)
            .or_else(|| self.status_reads.deadline_rpcs.get(&(read_id, generation)))
    }

    /// 单次请求无响应（上游 RPC 超时）：只对仍在途的那一页生效，读取以 `Unresponsive` 完成。
    pub(crate) fn status_read_unresponsive(
        &mut self,
        read_id: u64,
        generation: u64,
        actions: &mut Vec<Action>,
    ) {
        if self
            .status_reads
            .deadline_rpcs
            .remove(&(read_id, generation))
            .is_some()
        {
            return;
        }
        if self.status_read_caller(read_id, generation).is_none() {
            actions.push(Action::Debug(format!(
                "忽略已完成或旧代号的状态读取超时 {read_id}/{generation}"
            )));
            return;
        }
        self.finish_status_read(
            read_id,
            StatusResult::Unknown(StatusUnknown::Unresponsive),
            actions,
        );
    }

    pub(crate) fn cancel_status_read(&mut self, id: u64, actions: &mut Vec<Action>) {
        if let Some(read) = self.status_reads.reads.remove(&id) {
            self.forget_status_version(&read.thread);
            actions.push(Action::Unschedule {
                key: TimerKey::StatusRead(id),
            });
        }
        self.status_reads
            .deadline_rpcs
            .retain(|(read, _), _| *read != id);
    }

    pub(crate) fn cancel_stop_reads(&mut self, generation: u64, actions: &mut Vec<Action>) {
        let ids: Vec<_> = self.status_reads.reads.iter().filter(|(_, read)|
            matches!(&read.caller, StatusCaller::StopVerify { tag } if tag.generation == generation))
            .map(|(id, _)| *id).collect();
        for id in ids {
            self.cancel_status_read(id, actions);
        }
        self.status_reads.deadline_rpcs.retain(|_, caller|
            !matches!(caller, StatusCaller::StopVerify { tag } if tag.generation == generation));
    }

    pub(crate) fn cancel_tree_reads(
        &mut self,
        tree_op: u64,
        root_generation: Option<u64>,
        actions: &mut Vec<Action>,
    ) {
        let matches = |caller: &StatusCaller| match caller {
            StatusCaller::DerivedStopVerify { tree_op: id, .. } => *id == tree_op,
            StatusCaller::StopVerify { tag } => root_generation == Some(tag.generation),
            _ => false,
        };
        let ids: Vec<_> = self
            .status_reads
            .reads
            .iter()
            .filter(|(_, r)| matches(&r.caller))
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            if let Some(read) = self.status_reads.reads.remove(&id) {
                self.forget_status_version(&read.thread);
                actions.push(Action::Unschedule {
                    key: TimerKey::StatusRead(id),
                });
            }
        }
        self.status_reads
            .deadline_rpcs
            .retain(|_, caller| !matches(caller));
    }

    /// 关闭：在途读取全部清理，不回调；由现有关闭流程收尾。
    pub(crate) fn status_reads_shutdown(&mut self, actions: &mut Vec<Action>) {
        let reads = std::mem::take(&mut self.status_reads.reads);
        for read_id in reads.keys() {
            actions.push(Action::Unschedule {
                key: TimerKey::StatusRead(*read_id),
            });
        }
        self.status_reads.versions.clear();
        self.status_reads.deadline_rpcs.clear();
    }

    /// 回包入口（`Operation::StatusRead`）。
    pub(crate) fn status_read_reply(
        &mut self,
        read_id: u64,
        generation: u64,
        result: Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) {
        self.status_reads
            .deadline_rpcs
            .remove(&(read_id, generation));
        let Some(read) = self.status_reads.reads.get_mut(&read_id) else {
            actions.push(Action::Debug(format!(
                "丢弃已完成或已取消的状态读取回包 {read_id}"
            )));
            return;
        };
        if read.generation == 0 || read.generation != generation {
            actions.push(Action::Debug(format!(
                "丢弃代号不符的状态读取回包 {read_id}/{generation}（当前 {}）",
                read.generation
            )));
            return;
        }
        // 回包只消费一次；重复的同代号回包随后按代号不符丢弃。
        read.generation = 0;
        if read.connection_epoch != self.relations.conn_epoch {
            self.finish_status_read(
                read_id,
                StatusResult::Unknown(StatusUnknown::Changed),
                actions,
            );
            return;
        }
        let (thread, version, stage) = (read.thread.clone(), read.version, read.stage.clone());
        if self.status_reads.version(&thread) != version {
            self.status_version_changed(read_id, actions);
            return;
        }
        let value = match result {
            Ok(value) => value,
            Err(error) => {
                let reason = if error.get("code").and_then(Value::as_i64) == Some(METHOD_NOT_FOUND)
                {
                    StatusUnknown::Unsupported
                } else {
                    StatusUnknown::ErrorReply
                };
                // 不支持只作用于本次读取，不缓存成「服务器不支持分页」：第一轮前的 -32601 之后可以恢复。
                self.finish_status_read(read_id, StatusResult::Unknown(reason), actions);
                return;
            }
        };
        // 只接纳身份、请求代号及活动版本都通过校验的元数据；notLoaded 不等同一般 Idle。
        if matches!(stage, ReadStage::Metadata | ReadStage::TurnMetadata)
            && thread_active(&value, &thread).is_some()
        {
            if value["thread"]["status"]["type"] == "notLoaded" {
                if value["thread"]["id"].as_str() == Some(thread.as_str()) {
                    self.record_closed_runtime(&thread, "thread/read:notLoaded");
                    match self.status_reads.reads[&read_id].caller.clone() {
                        StatusCaller::StopVerify { tag } => {
                            if let Some(op) = self.stops.operations.get_mut(&tag.id).filter(|op| {
                                op.generation == tag.generation && op.round == tag.round
                            }) {
                                op.unloaded_read = true;
                            }
                        }
                        StatusCaller::DerivedStopVerify {
                            tree_op,
                            thread,
                            round,
                        } => {
                            if let Some(op) = self
                                .trees
                                .aux
                                .derived
                                .get_mut(&(tree_op, thread))
                                .filter(|op| op.request_round == round)
                            {
                                op.unloaded_read = true;
                            }
                        }
                        _ => {}
                    }
                }
            } else {
                self.forget_closed_runtime(&thread);
            }
        }
        let outcome = match stage {
            ReadStage::Metadata => self.status_metadata(read_id, &value),
            ReadStage::TurnMetadata => match thread_active(&value, &thread) {
                None => Step::Done(StatusResult::Unknown(StatusUnknown::Malformed)),
                Some(false) => Step::Done(StatusResult::Absent),
                Some(true) => Step::Done(StatusResult::Unknown(StatusUnknown::NotVisible)),
            },
            ReadStage::Turns => self.status_turns(read_id, &value),
            ReadStage::Items { index } => self.status_items(read_id, index, &value, actions),
        };
        match outcome {
            Step::Done(result) => self.finish_status_read(read_id, result, actions),
            Step::Next => self.issue_status_page(read_id, actions),
        }
    }

    /// 版本变化：Settlement 直接 `Changed`；其余从第一步重读，最多 3 次，不续期、不清页数。
    fn status_version_changed(&mut self, read_id: u64, actions: &mut Vec<Action>) {
        let read = &self.status_reads.reads[&read_id];
        let current = self.status_reads.version(&read.thread);
        if matches!(read.query, StatusQuery::Settlement { .. })
            || read.rereads >= STATUS_MAX_REREADS
        {
            self.finish_status_read(
                read_id,
                StatusResult::Unknown(StatusUnknown::Changed),
                actions,
            );
            return;
        }
        let read = self
            .status_reads
            .reads
            .get_mut(&read_id)
            .expect("读取登记存在");
        read.rereads += 1;
        read.version = current;
        read.stage = match read.query {
            StatusQuery::Turn { .. } | StatusQuery::ClientMessage { .. } => ReadStage::Turns,
            _ => ReadStage::Metadata,
        };
        read.cursor = None;
        read.cursors.clear();
        read.active.clear();
        read.client_candidates.clear();
        actions.push(Action::Debug(format!(
            "线程 {} 活动版本变化，状态读取 {read_id} 第 {} 次重读",
            read.thread, read.rereads
        )));
        self.issue_status_page(read_id, actions);
    }

    fn status_metadata(&mut self, read_id: u64, value: &Value) -> Step {
        let read = self
            .status_reads
            .reads
            .get_mut(&read_id)
            .expect("读取登记存在");
        let Some(active) = thread_active(value, &read.thread) else {
            return Step::Done(StatusResult::Unknown(StatusUnknown::Malformed));
        };
        match (&read.query, active) {
            (StatusQuery::ClientMessage { .. }, _) => {
                Step::Done(StatusResult::Unknown(StatusUnknown::Unsupported))
            }
            (StatusQuery::Busy, true) => Step::Done(StatusResult::Busy),
            (StatusQuery::Busy, false) => Step::Done(StatusResult::NotBusy),
            (StatusQuery::Activity, false) => Step::Done(StatusResult::Idle),
            (StatusQuery::Settlement { candidates, .. }, false) => {
                if candidates.len() > STATUS_MAX_CANDIDATES {
                    return Step::Done(StatusResult::Unknown(StatusUnknown::PageBudget));
                }
                if candidates.is_empty() {
                    return Step::Done(StatusResult::NoCarrier);
                }
                read.stage = ReadStage::Items { index: 0 };
                read.cursor = None;
                read.cursors.clear();
                Step::Next
            }
            (StatusQuery::Activity | StatusQuery::Settlement { .. }, true) => {
                read.stage = ReadStage::Turns;
                read.cursor = None;
                read.cursors.clear();
                read.active.clear();
                Step::Next
            }
            // Turn 不从元数据起步，第一步的元数据读不会落在这里。
            (StatusQuery::Turn { .. }, _) => {
                Step::Done(StatusResult::Unknown(StatusUnknown::Malformed))
            }
        }
    }

    fn status_turns(&mut self, read_id: u64, value: &Value) -> Step {
        let read = self
            .status_reads
            .reads
            .get_mut(&read_id)
            .expect("读取登记存在");
        let page = match turns_page(value, &mut read.cursors) {
            Ok(page) => page,
            Err(reason) => return Step::Done(StatusResult::Unknown(reason)),
        };
        if let StatusQuery::ClientMessage {
            expected_previous_turn_id,
            ..
        } = &read.query
        {
            let mut reached_previous = false;
            for (id, _, _) in &page.turns {
                if read.client_candidates.contains(id) {
                    return Step::Done(StatusResult::Unknown(StatusUnknown::Changed));
                }
                if read.client_candidates.len() >= STATUS_MAX_CANDIDATES {
                    return Step::Done(StatusResult::Unknown(StatusUnknown::PageBudget));
                }
                read.client_candidates.push(id.clone());
                if expected_previous_turn_id.as_ref() == Some(id) {
                    reached_previous = true;
                    break;
                }
            }
            if !reached_previous {
                if let Some(cursor) = page.next {
                    read.cursor = Some(cursor);
                    return Step::Next;
                }
            }
            if read.client_candidates.is_empty() {
                return Step::Done(StatusResult::NoCarrier);
            }
            read.stage = ReadStage::Items { index: 0 };
            read.cursor = None;
            read.cursors.clear();
            return Step::Next;
        }
        if let StatusQuery::Turn { target } = &read.query {
            if let Some((_, status, turn)) = page.turns.iter().find(|(id, _, _)| id == target) {
                return Step::Done(StatusResult::Found {
                    status: *status,
                    shell: shell(turn),
                });
            }
            return match page.next {
                Some(cursor) => {
                    read.cursor = Some(cursor);
                    Step::Next
                }
                None => {
                    // 存储页里没有不能证明不存在：分页线程的活动轮次可能还未持久化，再读一次元数据。
                    read.stage = ReadStage::TurnMetadata;
                    read.cursor = None;
                    Step::Next
                }
            };
        }
        read.active.extend(
            page.turns
                .iter()
                .filter(|(_, status, _)| *status == StatusTurn::InProgress)
                .map(|(id, _, _)| id.clone()),
        );
        if let Some(cursor) = page.next {
            read.cursor = Some(cursor);
            return Step::Next;
        }
        // 倒序收集，反转成升序（最早的在前）。
        let mut ids = std::mem::take(&mut read.active);
        ids.reverse();
        match &read.query {
            StatusQuery::Settlement { .. } => match ids.into_iter().next() {
                Some(first) => Step::Done(StatusResult::FirstActive(first)),
                None => Step::Done(StatusResult::Unknown(StatusUnknown::NotVisible)),
            },
            // 可能为空：状态是 active，但轮次还没出现在存储页里；不是 Idle。
            _ => Step::Done(StatusResult::Active(ids)),
        }
    }

    fn status_items(
        &mut self,
        read_id: u64,
        index: usize,
        value: &Value,
        actions: &mut Vec<Action>,
    ) -> Step {
        let reconcile = self.status_reads.reads.get(&read_id).and_then(|read| {
            let StatusCaller::SubagentReconcile { token, op_id } = &read.caller else {
                return None;
            };
            let turn = read.client_candidates.get(index)?;
            let pending = self.runs.reconciliation.operations.get(&op_id.0)?;
            // 查询可能回看原用户轮；历史输入不构成本次提交期间的新 veto。
            if !pending.observed_starts.contains(turn) && !pending.started.contains_key(turn) {
                return None;
            }
            Some((token.clone(), turn.clone()))
        });
        let outcome = self.status_items_page(read_id, index, value);
        if !matches!(outcome, Step::Done(StatusResult::Unknown(_))) {
            if let Some((token, turn)) = reconcile {
                // 承载核实的分页也可能同时含真实输入，必须在授予归属前走 C 分类器。
                if value
                    .get("data")
                    .and_then(Value::as_array)
                    .is_some_and(|items| {
                        items.iter().any(|entry| {
                            self.classify_result_input(
                                &token.run_id,
                                &token.claim.thread_id,
                                &turn,
                                &entry["item"],
                            ) == crate::sa1b::InputSource::ExternalUser
                        })
                    })
                {
                    self.reconcile_user_input(&token, actions);
                }
            }
        }
        outcome
    }

    fn status_items_page(&mut self, read_id: u64, index: usize, value: &Value) -> Step {
        let read = self
            .status_reads
            .reads
            .get_mut(&read_id)
            .expect("读取登记存在");
        let (candidates, input, client_id) = match &read.query {
            StatusQuery::Settlement { candidates, input } => (candidates, Some(input), None),
            StatusQuery::ClientMessage {
                client_user_message_id,
                ..
            } => (&read.client_candidates, None, Some(client_user_message_id)),
            _ => return Step::Done(StatusResult::Unknown(StatusUnknown::Malformed)),
        };
        let Some(candidate) = candidates.get(index) else {
            return Step::Done(StatusResult::Unknown(StatusUnknown::Malformed));
        };
        let page = match items_page(
            value,
            candidate,
            &mut read.cursors,
            client_id.map(String::as_str),
        ) {
            Ok(page) => page,
            Err(reason) => return Step::Done(StatusResult::Unknown(reason)),
        };
        if page.client_match
            || input.is_some_and(|input| page.texts.iter().any(|text| carries(text, input)))
        {
            return Step::Done(StatusResult::Carrier(candidate.clone()));
        }
        if let Some(cursor) = page.next {
            read.cursor = Some(cursor);
            return Step::Next;
        }
        if index + 1 >= candidates.len() {
            return Step::Done(StatusResult::NoCarrier);
        }
        // 换候选：新的分页流，游标集合重置；总期限、页数不清零。
        read.stage = ReadStage::Items { index: index + 1 };
        read.cursor = None;
        read.cursors.clear();
        Step::Next
    }
}

enum Step {
    Done(StatusResult),
    Next,
}

/// 元数据里的线程状态：`Some(true)` 为 active；`idle`、`notLoaded`、`systemError` 为非 active；
/// 缺字段、未知值或线程身份不符为 None。
fn thread_active(value: &Value, thread: &str) -> Option<bool> {
    let object = value.get("thread")?.as_object()?;
    if let Some(id) = object.get("id") {
        if id.as_str()? != thread {
            return None;
        }
    }
    match object.get("status")?.as_object()?.get("type")?.as_str()? {
        "active" => Some(true),
        "idle" | "notLoaded" | "systemError" => Some(false),
        _ => None,
    }
}

/// 下一页游标：null 为流结束；非空字符串、不超过 4096 字节且在本流内未出现过才接受。
/// 空页带游标为畸形；重复游标为 `DuplicateCursor`。
fn next_cursor(
    value: &Value,
    empty: bool,
    seen: &mut HashSet<String>,
) -> Result<Option<String>, StatusUnknown> {
    match value.get("nextCursor") {
        Some(Value::Null) => Ok(None),
        Some(Value::String(cursor))
            if !cursor.is_empty() && cursor.len() <= MAX_CURSOR_BYTES && !empty =>
        {
            if seen.insert(cursor.clone()) {
                Ok(Some(cursor.clone()))
            } else {
                Err(StatusUnknown::DuplicateCursor)
            }
        }
        _ => Err(StatusUnknown::Malformed),
    }
}

struct TurnsPage<'a> {
    turns: Vec<(String, StatusTurn, &'a Value)>,
    next: Option<String>,
}

/// 校验整页后再使用：任一轮次缺 id、id 为空或状态不在四种之内，整页按畸形处理。
fn turns_page<'a>(
    value: &'a Value,
    seen: &mut HashSet<String>,
) -> Result<TurnsPage<'a>, StatusUnknown> {
    let data = value
        .get("data")
        .and_then(Value::as_array)
        .ok_or(StatusUnknown::Malformed)?;
    if data.len() > STATUS_TURNS_LIMIT {
        return Err(StatusUnknown::Malformed);
    }
    let mut turns = Vec::with_capacity(data.len());
    for turn in data {
        let id = turn
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or(StatusUnknown::Malformed)?;
        let status = match turn.get("status").and_then(Value::as_str) {
            Some("inProgress") => StatusTurn::InProgress,
            Some("completed") => StatusTurn::Completed,
            Some("failed") => StatusTurn::Failed,
            Some("interrupted") => StatusTurn::Interrupted,
            _ => return Err(StatusUnknown::Malformed),
        };
        turns.push((id.to_owned(), status, turn));
    }
    let next = next_cursor(value, data.is_empty(), seen)?;
    Ok(TurnsPage { turns, next })
}

/// notLoaded 形式的轮次壳：条目清空、`itemsView` 置为 notLoaded，其余字段（如 error）原样保留。
fn shell(turn: &Value) -> Value {
    let mut shell = turn.clone();
    if let Some(object) = shell.as_object_mut() {
        object.insert("items".into(), Value::Array(Vec::new()));
        object.insert("itemsView".into(), Value::String("notLoaded".into()));
    }
    shell
}

struct ItemsPage {
    client_match: bool,
    /// 本页全部 userMessage 的文本部分。
    texts: Vec<String>,
    next: Option<String>,
}

/// 条目是 `{turnId, item, ...}` 包装：turnId 必须等于候选，item 必须是带字符串 type 的对象；
/// userMessage 的 content 必须是数组，每部分是带字符串 type 的对象，文本部分必须带字符串 text。
/// 畸形条目整页按畸形处理，不当作「不匹配」。
fn items_page(
    value: &Value,
    candidate: &str,
    seen: &mut HashSet<String>,
    client_id: Option<&str>,
) -> Result<ItemsPage, StatusUnknown> {
    let malformed = StatusUnknown::Malformed;
    let data = value
        .get("data")
        .and_then(Value::as_array)
        .ok_or(malformed)?;
    if data.len() > STATUS_ITEMS_LIMIT {
        return Err(malformed);
    }
    let mut texts = Vec::new();
    let mut client_match = false;
    for entry in data {
        if entry.get("turnId").and_then(Value::as_str) != Some(candidate) {
            return Err(malformed);
        }
        let item = entry
            .get("item")
            .filter(|item| item.is_object())
            .ok_or(malformed)?;
        let kind = item.get("type").and_then(Value::as_str).ok_or(malformed)?;
        if kind != "userMessage" {
            continue;
        }
        client_match |= client_id.is_some_and(|id| {
            !id.is_empty() && item.get("clientId").and_then(Value::as_str) == Some(id)
        });
        let content = item
            .get("content")
            .and_then(Value::as_array)
            .ok_or(malformed)?;
        for part in content {
            let part_kind = part.get("type").and_then(Value::as_str).ok_or(malformed)?;
            if part_kind == "text" {
                let text = part.get("text").and_then(Value::as_str).ok_or(malformed)?;
                if client_id.is_none() {
                    texts.push(text.to_owned());
                }
            }
        }
    }
    let next = next_cursor(value, data.is_empty(), seen)?;
    Ok(ItemsPage {
        texts,
        client_match,
        next,
    })
}

/// 与 `claim/settling.rs` 的 `carries_input` 同一口径：去首尾空白后与某条非空输入完全相等。
fn carries(text: &str, input: &[String]) -> bool {
    input
        .iter()
        .any(|wanted| !wanted.trim().is_empty() && text.trim() == wanted.trim())
}

#[cfg(test)]
#[path = "tests/status_read.rs"]
mod tests;
