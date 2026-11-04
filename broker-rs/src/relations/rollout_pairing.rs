//! 没有 raw 的 v2 跟进：从会话文件（rollout jsonl）补开始记录与消费记录。
//!
//! 上游只有 thread/start 能打开 raw，跨重启 `--resume` 的线程没有 raw，原来只能宽松配对。
//! Codex 把每个线程写进会话文件，路径在 thread/read 回包的 `thread.path`：
//! - 父线程文件里有协作工具调用（`function_call`，`call_id` 等于 Interacted 的条目 id），
//!   由此知道是 followup_task 还是 send_message，以及目标与正文摘要；
//! - 子线程文件里有收到的消息（`agent_message`），与 raw 的同形，所在的轮就是消费它的轮。
//!
//! 流程：Interacted 到达、父线程没有 raw、也没有开始记录时，不立刻定模式，进入「等文件」：
//! 这期间不配对，操作照常阻止收敛。在 [`ROLLOUT_WAIT_MS`] 内从父线程文件按 call_id 找到调用、
//! 且（followup_task 时）子线程文件已读通一次，就按严格模式走；找不到、读不了就和原来一样定为宽松。
//! 定案后按原样重放暂存的 Interacted 通知，此后一切沿用 raw 的规则（`v2_pairing`）：
//! send_message 走只入队，消费记录进 [`DeliveryFact`](super::v2_pairing::DeliveryFact) 一一配对。
//!
//! 状态机不碰文件：读取经 [`Action::ReadRollout`] 交给 core 的 IO 层，结果以
//! [`Event::RolloutRead`](crate::claim::Event::RolloutRead) 回来；路径用 thread/read 请求拿。
//! 只在有操作等文件或等文件里的消费记录时才轮询，操作有结果、运行结束、停止、连接或授权换代后立即停止。
//! 有 raw 的线程一次都不读。不存、不记任何正文，只用摘要与字段名。
use super::v2_pairing::{parse_call, parse_delivery, EvidenceSource, ParsedCall};
use super::*;
use crate::rollout::{
    RolloutChunk, RolloutCursor, RolloutError, RolloutReadRequest, RolloutRecord,
};
use serde_json::json;

/// 等父线程文件里的工具调用（以及子线程文件首次读通）的窗口。
pub const ROLLOUT_WAIT_MS: u64 = 2_000;
/// 有操作等文件时的轮询间隔。
pub const ROLLOUT_POLL_MS: u64 = 150;
/// 文件末尾窗口里的消息比等它的最早操作还早这么多，就不可能是它们的，不登记。
pub const ROLLOUT_FLOOR_SLACK_MS: u64 = 10_000;
/// 同时跟随的文件数上限。
pub const MAX_ROLLOUT_FILES: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RolloutPathState {
    /// thread/read 在途（请求代号）。
    Requested(u64),
    Ready(String),
    /// 本连接内不再尝试；原因进诊断。
    Failed(V2FallbackReason),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RolloutFile {
    pub epoch: u64,
    pub path: RolloutPathState,
    /// None：下次读末尾窗口。停止跟随时清空，重新需要时从末尾重来。
    pub cursor: Option<RolloutCursor>,
    /// 在途读取的编号。
    pub reading: Option<u64>,
    /// 当前这段跟随里成功读过几次。
    pub reads: u32,
    pub next_at: u64,
}

/// 等文件的操作：暂存的 Interacted 通知，定案后按原样重放。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RolloutWait {
    pub deadline_ms: u64,
    /// Interacted 到达时的墙钟毫秒，用来给末尾窗口里的旧消息定下限。
    pub wall_ms: u64,
    pub child: String,
    /// Interacted 到达时接收方已观察到的轮次：退回宽松时据此还原旧版当时会配的那一轮。
    pub child_turns: Vec<String>,
    pub events: Vec<(Value, bool)>,
}

/// 父线程文件里读到的协作工具调用；只留身份、类型与摘要。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RolloutCall {
    pub epoch: u64,
    pub at: u64,
    pub wall_ms: u64,
    pub trigger: bool,
    pub target: Option<String>,
    pub message: Option<[u8; 32]>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RolloutState {
    pub files: BTreeMap<String, RolloutFile>,
    pub waits: BTreeMap<EvidenceKey, RolloutWait>,
    /// (父线程, call_id) → 调用。读到时对应的 Interacted 可能还没来。
    pub calls: BTreeMap<(String, String), RolloutCall>,
    /// 轮询计时：(到期单调毫秒, 代号)。
    pub timer: Option<(u64, u64)>,
    pub next_id: u64,
    /// 测试用：发出过的读取次数。
    pub reads_issued: u64,
}

enum Decision {
    Strict(RolloutCall),
    Fallback(V2FallbackReason),
}

impl RolloutState {
    fn next(&mut self) -> u64 {
        self.next_id = self.next_id.saturating_add(1);
        self.next_id
    }
}

fn read_failure(error: RolloutError) -> V2FallbackReason {
    match error {
        RolloutError::Missing | RolloutError::Unreadable => V2FallbackReason::RolloutUnreadable,
        RolloutError::Shrunk => V2FallbackReason::RolloutLost,
    }
}

impl Machine {
    fn raw_thread(&self, thread: &str) -> bool {
        self.relations.aux.v2_raw_threads.get(thread) == Some(&self.relations.conn_epoch)
    }

    fn rollout_file_failed(&self, thread: &str) -> Option<V2FallbackReason> {
        self.relations
            .aux
            .rollout
            .files
            .get(thread)
            .filter(|f| f.epoch == self.relations.conn_epoch)
            .and_then(|f| match f.path {
                RolloutPathState::Failed(reason) => Some(reason),
                _ => None,
            })
    }

    /// Interacted 首次到达、父线程没送过 raw、也没有开始记录：能用会话文件时进入「等文件」。
    /// 返回 Err(原因) 表示文件已知不可用，按宽松回退并记下原因；Ok(false) 表示不适用。
    pub(super) fn rollout_try_wait(
        &mut self,
        key: &EvidenceKey,
        item: &Value,
        completed: bool,
    ) -> Result<bool, V2FallbackReason> {
        let Some(op) = self.relations.operations.get(key) else {
            return Ok(false);
        };
        // 发起方这一轮的归属还没定（等绑定）时同样等文件：调用证据与归属分开保存，
        // 定案时登记的开始记录归属留空，绑定后补上（`adopt_pending_starts`）。
        if !self.convergence_enabled()
            || op.kind != EvidenceKind::V2Interacted
            || (op.run_id.is_empty() && !self.awaiting_parent_binding(key))
            || op.outcome != EvidenceOutcome::Pending
        {
            return Ok(false);
        }
        let Some(child) = item
            .get("agentThreadId")
            .and_then(Value::as_str)
            .filter(|t| !t.is_empty() && *t != key.parent_thread_id)
        else {
            return Ok(false);
        };
        // 向上、平级、越级等注定被拒的接收方不需要证据，照原样处理。
        if self.followup_rejection(key, child, &op.run_id).is_some() {
            return Ok(false);
        }
        if let Some(reason) = self.rollout_file_failed(&key.parent_thread_id) {
            return Err(reason);
        }
        if !self.raw_thread(child) {
            if let Some(reason) = self.rollout_file_failed(child) {
                return Err(reason);
            }
        }
        if self.relations.aux.rollout.waits.len() >= MAX_OPERATIONS {
            return Ok(false);
        }
        let child_turns = self
            .relations
            .aux
            .observed
            .get(child)
            .into_iter()
            .flatten()
            .map(|o| o.turn.clone())
            .collect();
        let wait = RolloutWait {
            deadline_ms: self.clock.mono_ms.saturating_add(ROLLOUT_WAIT_MS),
            wall_ms: self.clock.now_ms,
            child: child.into(),
            child_turns,
            events: vec![(item.clone(), completed)],
        };
        self.relations.aux.rollout.waits.insert(key.clone(), wait);
        // 调用刚写进父线程文件：不等下一个轮询间隔，尽快读一次。
        let epoch = self.relations.conn_epoch;
        let now = self.clock.mono_ms;
        if let Some(file) = self
            .relations
            .aux
            .rollout
            .files
            .get_mut(&key.parent_thread_id)
            .filter(|f| f.epoch == epoch)
        {
            file.next_at = file.next_at.min(now);
        }
        Ok(true)
    }

    /// 等文件期间同键的另一条 Interacted 通知只暂存，定案后一并重放。
    pub(super) fn rollout_hold(
        &mut self,
        key: &EvidenceKey,
        item: &Value,
        completed: bool,
    ) -> bool {
        let Some(wait) = self.relations.aux.rollout.waits.get_mut(key) else {
            return false;
        };
        if !wait.events.iter().any(|(_, c)| *c == completed) {
            wait.events.push((item.clone(), completed));
        }
        true
    }

    pub(crate) fn rollout_awaiting(&self, key: &EvidenceKey) -> bool {
        self.relations.aux.rollout.waits.contains_key(key)
    }

    /// 运行名下还在等文件的操作数；它们照常阻止收敛。
    pub(crate) fn rollout_awaiting_for_run(&self, run: &str) -> usize {
        self.relations
            .aux
            .rollout
            .waits
            .keys()
            .filter(|key| {
                self.relations
                    .operations
                    .get(*key)
                    .is_some_and(|op| op.run_id == run)
            })
            .count()
    }

    /// 已按文件证据定为严格、还在等消费记录的操作：它的接收方文件要继续跟随。
    fn rollout_strict_waiting(&self, op: &EvidenceOperation) -> bool {
        op.v2_source == V2EvidenceSource::RolloutFile
            && self.v2_awaits_pairing(op)
            && self.rollout_run_live(op)
    }

    fn rollout_run_live(&self, op: &EvidenceOperation) -> bool {
        self.runs
            .runs
            .get(&op.run_id)
            .is_some_and(|r| r.state == crate::runs::RunState::Running)
            && self
                .relations
                .contexts
                .get(&op.run_id)
                .is_some_and(|c| c.authority != Authority::Revoked)
    }

    fn rollout_wait_live(&self, op: &EvidenceOperation) -> bool {
        op.outcome == EvidenceOutcome::Pending
            && op.conn_epoch == self.relations.conn_epoch
            && if op.run_id.is_empty() {
                // 运行还不知道：只要还在等发起方这一轮的绑定就继续等。
                self.awaiting_parent_binding(&op.key)
            } else {
                (!self.op_confirmed(&op.key) || self.operation_generation_current(op))
                    && self.rollout_run_live(op)
            }
    }

    /// 操作在等发起方这一轮的绑定（发起方是刚被跟进、新轮尚未绑定的子代理）。
    pub(super) fn awaiting_parent_binding(&self, key: &EvidenceKey) -> bool {
        matches!(
            self.relations.aux.parent_waits.get(key),
            Some(ParentWait::Binding { .. })
        )
    }

    fn rollout_decision(&self, key: &EvidenceKey, wait: &RolloutWait) -> Option<Decision> {
        let Some(op) = self.relations.operations.get(key) else {
            return Some(Decision::Fallback(V2FallbackReason::RolloutCancelled));
        };
        if !self.rollout_wait_live(op) {
            return Some(Decision::Fallback(V2FallbackReason::RolloutCancelled));
        }
        let expired = self.clock.mono_ms >= wait.deadline_ms;
        let call = self
            .relations
            .aux
            .rollout
            .calls
            .get(&(key.parent_thread_id.clone(), key.item_id.clone()))
            .filter(|c| c.epoch == self.relations.conn_epoch);
        if let Some(call) = call {
            // 没有可比的正文摘要：严格配对只会等到收敛期限，不如照原样宽松。
            if call.trigger && call.message.is_none() {
                return Some(Decision::Fallback(V2FallbackReason::RolloutCallMissing));
            }
            // 文件证据只按消费记录配对，消费记录按发起方的代理路径比对；路径未知时同样只会卡到期限。
            if call.trigger && self.v2_sender_path(op).is_none() {
                return Some(Decision::Fallback(V2FallbackReason::RolloutSenderUnknown));
            }
            if !call.trigger || self.raw_thread(&wait.child) {
                return Some(Decision::Strict(call.clone()));
            }
            if let Some(reason) = self.rollout_file_failed(&wait.child) {
                return Some(Decision::Fallback(reason));
            }
            let ready = self
                .relations
                .aux
                .rollout
                .files
                .get(&wait.child)
                .is_some_and(|f| f.epoch == self.relations.conn_epoch && f.reads > 0);
            if ready {
                return Some(Decision::Strict(call.clone()));
            }
            return expired.then_some(Decision::Fallback(V2FallbackReason::RolloutUnreadable));
        }
        if let Some(reason) = self.rollout_file_failed(&key.parent_thread_id) {
            return Some(Decision::Fallback(reason));
        }
        expired.then_some(Decision::Fallback(V2FallbackReason::RolloutCallMissing))
    }

    /// 定案：固定模式与来源，按原样重放暂存的 Interacted 通知；之后迟到的行不改写已定模式。
    fn rollout_decide(&mut self, key: &EvidenceKey, decision: Decision, actions: &mut Vec<Action>) {
        let Some(wait) = self.relations.aux.rollout.waits.remove(key) else {
            return;
        };
        let Some(op) = self.relations.operations.get(key).cloned() else {
            return;
        };
        let decision = match decision {
            Decision::Strict(call) => {
                let recorded = self.v2_record_start(
                    key.clone(),
                    op.run_id.clone(),
                    op.authority_gen,
                    ParsedCall {
                        call_id: key.item_id.clone(),
                        trigger: call.trigger,
                        target: call.target.clone(),
                        message: call.message,
                    },
                    EvidenceSource::RolloutFile,
                    call.wall_ms,
                );
                if recorded && self.v2_start(&op).is_some() {
                    Decision::Strict(call)
                } else {
                    Decision::Fallback(V2FallbackReason::StartMissing)
                }
            }
            other => other,
        };
        if let Some(op) = self.relations.operations.get_mut(key) {
            match &decision {
                Decision::Strict(call) => {
                    op.v2_strict = true;
                    op.v2_queue_only = !call.trigger;
                    op.v2_source = V2EvidenceSource::RolloutFile;
                    op.v2_fallback = None;
                }
                Decision::Fallback(reason) => {
                    op.v2_fallback = Some(*reason);
                }
            }
        }
        if matches!(decision, Decision::Fallback(_)) {
            self.rollout_fallback_baseline(key, &wait);
        }
        for (item, completed) in &wait.events {
            self.item_evidence_replay(key, item, *completed, actions);
        }
        if !op.run_id.is_empty() {
            self.touch_context(&op.run_id, MemberChange::Activity, actions);
        }
    }

    /// 等文件期间接收方的轮次可能已经变了：退回宽松时只留旧版宽松当时会配的那一轮，其余并入基线。
    ///
    /// 旧版在 Interacted 到达时就按唯一候选配对：那时恰有一个未完成的候选就配它；一个都没有就配之后出现的
    /// 第一轮；那时就有多个候选的不猜（首轮身份不明）。等待期间新出现的轮次按观察先后排在后面。
    /// 到达时就有多个候选的不改动，仍按原规则判为身份不明。
    fn rollout_fallback_baseline(&mut self, key: &EvidenceKey, wait: &RolloutWait) {
        let Some(op) = self.relations.operations.get(key) else {
            return;
        };
        let child = &wait.child;
        let baseline = op.baseline_turns.get(child);
        let candidates: Vec<String> = self
            .relations
            .aux
            .observed
            .get(child)
            .into_iter()
            .flatten()
            .filter(|o| !baseline.is_some_and(|old| old.contains(&o.turn)))
            .map(|o| o.turn.clone())
            .collect();
        let (before, after): (Vec<String>, Vec<String>) = candidates
            .into_iter()
            .partition(|turn| wait.child_turns.contains(turn));
        let keep = match before.len() {
            0 => after.first().cloned(),
            1 => before.first().cloned(),
            _ => return,
        };
        let excluded: BTreeSet<String> = before
            .into_iter()
            .chain(after)
            .filter(|turn| Some(turn) != keep.as_ref())
            .collect();
        if excluded.is_empty() {
            return;
        }
        if let Some(op) = self.relations.operations.get_mut(key) {
            op.baseline_turns
                .entry(child.clone())
                .or_default()
                .extend(excluded);
        }
    }

    /// 文件证据定为严格后接收方的文件读不了或定位不到当前轮：降为宽松并立即按宽松规则重试配对，
    /// 不让它卡到收敛期限。
    fn rollout_downgrade(
        &mut self,
        key: &EvidenceKey,
        reason: V2FallbackReason,
        actions: &mut Vec<Action>,
    ) {
        let Some(op) = self.relations.operations.get_mut(key) else {
            return;
        };
        op.v2_strict = false;
        op.v2_source = V2EvidenceSource::None;
        op.v2_fallback = Some(match reason {
            V2FallbackReason::RolloutTurnUnknown => reason,
            _ => V2FallbackReason::RolloutLost,
        });
        let receivers: Vec<String> = op.receivers.iter().cloned().collect();
        let run = op.run_id.clone();
        for child in receivers {
            self.pair_first_turn(key, &child, actions);
        }
        if self.op_confirmed(key) {
            self.materialize(key, actions);
        }
        self.touch_context(&run, MemberChange::Activity, actions);
    }

    /// 每步末尾：定案到期或有结果的等待、补发路径与读取、维护轮询计时。没有文件模式的操作时什么都不做。
    pub(crate) fn drive_rollout(&mut self, actions: &mut Vec<Action>) {
        if self.closing {
            self.rollout_stop(actions);
            return;
        }
        let state = &self.relations.aux.rollout;
        if state.waits.is_empty() && state.files.is_empty() && state.timer.is_none() {
            return;
        }
        let epoch = self.relations.conn_epoch;
        let now = self.clock.mono_ms;
        self.relations
            .aux
            .rollout
            .calls
            .retain(|_, c| c.epoch == epoch && now < c.at.saturating_add(EVIDENCE_TTL_MS));
        let keys: Vec<EvidenceKey> = self.relations.aux.rollout.waits.keys().cloned().collect();
        for key in keys {
            let Some(wait) = self.relations.aux.rollout.waits.get(&key) else {
                continue;
            };
            if let Some(decision) = self.rollout_decision(&key, wait) {
                self.rollout_decide(&key, decision, actions);
            }
        }
        let lost: Vec<(EvidenceKey, V2FallbackReason)> = self
            .relations
            .operations
            .values()
            .filter(|op| self.rollout_strict_waiting(op))
            .filter_map(|op| {
                op.receivers
                    .iter()
                    .filter(|r| !op.rejected_receivers.contains_key(*r) && !self.raw_thread(r))
                    .find_map(|r| self.rollout_file_failed(r))
                    .map(|reason| (op.key.clone(), reason))
            })
            .collect();
        for (key, reason) in lost {
            self.rollout_downgrade(&key, reason, actions);
        }
        let needed = self.rollout_needed();
        let mut due: Option<u64> = self
            .relations
            .aux
            .rollout
            .waits
            .values()
            .map(|w| w.deadline_ms)
            .min();
        // 不再需要的文件停止跟随：清掉游标（重新需要时从末尾重来），只留路径与失败结论。
        let state = &mut self.relations.aux.rollout;
        state.files.retain(|thread, f| {
            if f.epoch != epoch {
                return false;
            }
            if !needed.contains(thread) && f.reading.is_none() {
                f.cursor = None;
                f.reads = 0;
            }
            true
        });
        for thread in &needed {
            let file = self.relations.aux.rollout.files.get(thread);
            match file.map(|f| &f.path) {
                None => {
                    if self.relations.aux.rollout.files.len() >= MAX_ROLLOUT_FILES {
                        let idle = self
                            .relations
                            .aux
                            .rollout
                            .files
                            .iter()
                            .find(|(t, f)| !needed.contains(*t) && f.reading.is_none())
                            .map(|(t, _)| t.clone());
                        match idle {
                            Some(t) => {
                                self.relations.aux.rollout.files.remove(&t);
                            }
                            None => continue,
                        }
                    }
                    let generation = self.relations.aux.rollout.next();
                    self.relations.aux.rollout.files.insert(
                        thread.clone(),
                        RolloutFile {
                            epoch,
                            path: RolloutPathState::Requested(generation),
                            cursor: None,
                            reading: None,
                            reads: 0,
                            next_at: now,
                        },
                    );
                    actions.push(Action::Request(RequestContext {
                        resume_retried: false,
                        stream_id: None,
                        review_id: None,
                        client: None,
                        token: None,
                        operation: Operation::RolloutPath {
                            thread: thread.clone(),
                            generation,
                        },
                        method: "thread/read".into(),
                        params: Some(json!({"threadId": thread, "includeTurns": false})),
                    }));
                }
                Some(RolloutPathState::Ready(path)) => {
                    let f = file.expect("文件登记存在");
                    if f.reading.is_some() {
                        continue;
                    }
                    if f.next_at > now {
                        due = Some(due.map_or(f.next_at, |d| d.min(f.next_at)));
                        continue;
                    }
                    let request = RolloutReadRequest {
                        id: 0,
                        thread: thread.clone(),
                        path: path.clone(),
                        cursor: f.cursor.clone(),
                    };
                    let id = self.relations.aux.rollout.next();
                    let state = &mut self.relations.aux.rollout;
                    state.reads_issued = state.reads_issued.saturating_add(1);
                    state.files.get_mut(thread).expect("文件登记存在").reading = Some(id);
                    actions.push(Action::ReadRollout(RolloutReadRequest { id, ..request }));
                }
                Some(RolloutPathState::Requested(_) | RolloutPathState::Failed(_)) => {}
            }
        }
        self.rollout_schedule(due, actions);
    }

    /// 需要跟随的文件：等文件的操作的父线程与接收方，以及按文件证据定为严格、还在等消费记录的接收方。
    /// 有 raw 的线程不读。
    fn rollout_needed(&self) -> BTreeSet<String> {
        let mut needed = BTreeSet::new();
        for (key, wait) in &self.relations.aux.rollout.waits {
            needed.insert(key.parent_thread_id.clone());
            if !self.raw_thread(&wait.child) {
                needed.insert(wait.child.clone());
            }
        }
        for op in self.relations.operations.values() {
            if !self.rollout_strict_waiting(op) {
                continue;
            }
            for r in &op.receivers {
                if !op.rejected_receivers.contains_key(r) && !self.raw_thread(r) {
                    needed.insert(r.clone());
                }
            }
        }
        needed.retain(|t| self.rollout_file_failed(t).is_none());
        needed
    }

    fn rollout_schedule(&mut self, due: Option<u64>, actions: &mut Vec<Action>) {
        let now = self.clock.mono_ms;
        let Some(at) = due else {
            if self.relations.aux.rollout.timer.take().is_some() {
                actions.push(Action::Unschedule {
                    key: crate::claim::TimerKey::RolloutPoll,
                });
            }
            return;
        };
        if self
            .relations
            .aux
            .rollout
            .timer
            .is_some_and(|(d, _)| d == at)
        {
            return;
        }
        let generation = self.relations.aux.rollout.next();
        self.relations.aux.rollout.timer = Some((at, generation));
        actions.push(Action::Schedule {
            token: Self::relation_token(),
            kind: TimerKind::RolloutPoll { generation },
            delay_ms: at.saturating_sub(now),
        });
    }

    fn rollout_stop(&mut self, actions: &mut Vec<Action>) {
        let state = &mut self.relations.aux.rollout;
        state.waits.clear();
        state.files.clear();
        state.calls.clear();
        if state.timer.take().is_some() {
            actions.push(Action::Unschedule {
                key: crate::claim::TimerKey::RolloutPoll,
            });
        }
    }

    pub(crate) fn rollout_timer(&mut self, generation: u64) {
        if self
            .relations
            .aux
            .rollout
            .timer
            .is_some_and(|(_, g)| g == generation)
        {
            self.relations.aux.rollout.timer = None;
        }
    }

    /// thread/read 回包：只认协议返回的、且通过校验的那一个路径。
    pub(crate) fn rollout_path_reply(
        &mut self,
        thread: &str,
        generation: u64,
        result: &Result<Value, Value>,
    ) {
        let epoch = self.relations.conn_epoch;
        let Some(file) = self.relations.aux.rollout.files.get_mut(thread) else {
            return;
        };
        if file.epoch != epoch || file.path != RolloutPathState::Requested(generation) {
            return;
        }
        let path = result
            .as_ref()
            .ok()
            .and_then(|v| v.get("thread"))
            .filter(|t| t.get("id").and_then(Value::as_str) == Some(thread))
            .and_then(|t| t.get("path"))
            .and_then(Value::as_str)
            .filter(|p| crate::rollout::validate_path(p, thread));
        file.path = match path {
            Some(path) => RolloutPathState::Ready(path.into()),
            None => RolloutPathState::Failed(V2FallbackReason::RolloutPathUnavailable),
        };
        file.next_at = self.clock.mono_ms;
    }

    /// 一次读取的结果：调用进缓存，消息按 raw 的规则登记消费记录；读不了就记失败原因。
    pub(crate) fn rollout_read_done(
        &mut self,
        id: u64,
        thread: &str,
        result: Result<RolloutChunk, RolloutError>,
        actions: &mut Vec<Action>,
    ) {
        let now = self.clock.mono_ms;
        let epoch = self.relations.conn_epoch;
        let Some(file) = self.relations.aux.rollout.files.get_mut(thread) else {
            return;
        };
        if file.reading != Some(id) || file.epoch != epoch {
            return;
        }
        file.reading = None;
        let chunk = match result {
            Ok(chunk) => chunk,
            Err(error) => {
                file.path = RolloutPathState::Failed(read_failure(error));
                file.cursor = None;
                return;
            }
        };
        // 停止跟随后才回来的结果不推进游标，下次重新需要时从末尾重来。
        if !self.rollout_needed().contains(thread) {
            return;
        }
        let file = self
            .relations
            .aux
            .rollout
            .files
            .get_mut(thread)
            .expect("文件登记存在");
        file.cursor = Some(chunk.cursor.clone());
        file.reads = file.reads.saturating_add(1);
        file.next_at = if chunk.more {
            now
        } else {
            now.saturating_add(ROLLOUT_POLL_MS)
        };
        let raw = self.raw_thread(thread);
        let floor = self.rollout_floor(thread);
        let mut turn_lost = false;
        for entry in chunk.entries {
            match entry.record {
                RolloutRecord::Call { item } => {
                    let Some(call) = parse_call(&item) else {
                        continue;
                    };
                    let calls = &mut self.relations.aux.rollout.calls;
                    let key = (thread.to_owned(), call.call_id.clone());
                    if calls.contains_key(&key) || calls.len() >= MAX_OPERATIONS {
                        continue;
                    }
                    calls.insert(
                        key,
                        RolloutCall {
                            epoch,
                            at: now,
                            wall_ms: entry.wall_ms,
                            trigger: call.trigger,
                            target: call.target,
                            message: call.message,
                        },
                    );
                }
                RolloutRecord::Message { turn, item } => {
                    // 有 raw 的线程只认 raw。
                    let (false, Some(floor)) = (raw, floor) else {
                        continue;
                    };
                    if entry.wall_ms < floor {
                        continue;
                    }
                    let Some(delivery) = parse_delivery(&item) else {
                        continue;
                    };
                    // 轮次不明的记录对应不到任何轮。起点所在的轮定位不到（末尾窗口找到上限仍没有标记）时，
                    // 它可能正是要等的消费记录：不能静默丢掉，这个文件明确降级，等它的操作按宽松规则配对。
                    let Some(turn) = turn else {
                        turn_lost |= entry.turn_lost;
                        continue;
                    };
                    self.prune_v2_deliveries();
                    self.v2_record_delivery(
                        thread,
                        &turn,
                        delivery,
                        EvidenceSource::RolloutFile,
                        entry.wall_ms,
                        actions,
                    );
                }
            }
        }
        if turn_lost {
            if let Some(file) = self.relations.aux.rollout.files.get_mut(thread) {
                file.path = RolloutPathState::Failed(V2FallbackReason::RolloutTurnUnknown);
                file.cursor = None;
            }
        }
    }

    /// 接收方文件里的消息至少要晚到这里才可能属于等它的操作。
    fn rollout_floor(&self, thread: &str) -> Option<u64> {
        let waits = self
            .relations
            .aux
            .rollout
            .waits
            .values()
            .filter(|w| w.child == thread)
            .map(|w| w.wall_ms);
        let strict = self
            .relations
            .operations
            .values()
            .filter(|op| self.rollout_strict_waiting(op) && op.receivers.contains(thread))
            .filter_map(|op| self.v2_start(op).map(|s| s.wall_ms));
        waits
            .chain(strict)
            .min()
            .map(|w| w.saturating_sub(ROLLOUT_FLOOR_SLACK_MS))
    }
}
