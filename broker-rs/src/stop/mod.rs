//! 停止操作与启动超时对账：与认领、服务端请求共用一次纯函数迁移。
//! 只有 confirmed 判断函数可以确认停止；中断回包从不提供完成证据。

use crate::claim::{
    Action, ClaimToken, ClientRequest, ConnId, Machine, Operation, RequestContext, State,
    StopReason, TimerKind,
};
use crate::proto::{self, code};
use crate::runs::{ReleaseKind, RunState, StopCheck, StopOutcome};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet, VecDeque};

#[cfg(test)]
mod tests;

pub const CONFIRM_MS: u64 = 40_000;
pub const VERIFY_MS: u64 = 15_000;
pub const LATE_MS: u64 = 600_000;
pub const MAX_LATE: usize = 64;
pub const MAX_UNRESOLVED_STARTS: usize = 64;
pub const MAX_STOP_WAITERS: usize = 64;
pub const MAX_RECONCILE_DELAY_MS: u64 = 300_000;
const STOP_NOT_OWNED: &str = "线程归属未确定或属于其他客户端，不能停止";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StopTag {
    pub id: String,
    pub generation: u64,
    pub round: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopRequest {
    GoalGet { version: u64 },
    GoalWrite { version: u64, clear: bool },
    VerifyTurn,
    VerifyGoal { version: u64 },
    Interrupt { turn: String },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopTimer {
    Deadline,
    Waiter(u64),
    Verify,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Pending,
    Unconfirmed,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Waiter {
    pub request: ClientRequest,
    pub id: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StopOp {
    pub thread: String,
    pub claim_id: Option<u64>,
    pub foreign_claim: bool,
    pub run_id: Option<String>,
    pub generation: u64,
    pub reason: StopReason,
    pub clear: bool,
    pub goal: String,
    pub brake_version: Option<u64>,
    pub goal_round: u64,
    pub goal_busy: bool,
    pub goal_attempted: bool,
    pub goal_done: bool,
    pub clear_done: bool,
    pub target: Option<String>,
    pub turn_absent: bool,
    pub snapshot_absent: Option<u64>,
    pub ended: HashSet<String>,
    pub owned_turns: HashSet<String>,
    pub interrupted: HashSet<String>,
    pub interrupt_sent: bool,
    pub explicit_interrupts: bool,
    pub round: u64,
    pub verifying: bool,
    pub verify_timer: u64,
    pub verification_pending: bool,
    pub read_done: bool,
    pub goal_read_done: bool,
    pub outcome: Outcome,
    pub waiters: Vec<Waiter>,
    pub already_stopped: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalChange {
    pub thread: String,
    pub unknown: bool,
    pub wire_tag: Option<u64>,
    /// 纯迟到制动产生的写入独立保存准入屏障，不随迟到表清理而解除。
    pub late_brake: bool,
    /// 可能授予空转刹车控制资格的写入（本 broker 客户端设 active、#2e 恢复设 active）在发出时
    /// 记下的资格代号；回包只有代号未变才授予（`loop_guard::grant_goal_control`）。
    pub control_epoch: Option<u64>,
    /// 同一写入请求的目标原文（有资格代号时才记）：它自己引起的 updated 通知不算外部替换。
    pub grant_objective: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LateStart {
    pub stream_id: Option<u64>,
    pub review_id: Option<u64>,
    pub token: ClaimToken,
    pub method: String,
    pub generation: u64,
    pub round: u64,
    pub reconciling: bool,
    pub turn: Option<String>,
    pub target_thread: String,
    pub interrupting: bool,
    pub wire_tag: u64,
    pub wire_available: bool,
    pub deadline: std::time::Instant,
    pub review_pending: bool,
    pub ownership_proven: bool,
    pub late_settling: bool,
    /// 回包消费后由本地截止负责定案收尾；线上等待的截止仅由 RPC 决定。
    pub reply_received: bool,
    pub goal_round: u64,
    pub goal_busy: bool,
    pub goal_done: bool,
    pub goal_version: u64,
    pub goal_waiting_for_owner: bool,
    pub goal_change_id: Option<u64>,
    pub reconcile_delay_ms: u64,
    pub cleanup_due: bool,
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BreakerProbe {
    pub generation: u64,
    pub scheduled: bool,
    pub pending: Option<u64>,
    pub delay_ms: u64,
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Stops {
    pub probe: BreakerProbe,
    pub operations: HashMap<String, StopOp>,
    pub versions: HashMap<String, u64>,
    pub changes: HashMap<u64, GoalChange>,
    pub late: VecDeque<LateStart>,
    /// 已有对账不被淘汰；达到 64 项后拒绝新流式请求，保留未知启动的占用与核实依据。
    pub reconciliations: HashMap<u64, LateStart>,
    pub retired_late_turns: VecDeque<(String, String)>,
    next: u64,
    next_version: u64,
}
impl Stops {
    fn allocate(&mut self) -> u64 {
        self.next = self.next.checked_add(1).expect("停止操作代号已耗尽");
        self.next
    }
    fn version(&self, thread: &str) -> u64 {
        *self.versions.get(thread).unwrap_or(&0)
    }
    fn bump(&mut self, thread: &str) -> u64 {
        self.next_version = self
            .next_version
            .checked_add(1)
            .expect("goal 事实版本已耗尽");
        self.versions.insert(thread.into(), self.next_version);
        self.next_version
    }
    fn ensure_version(&mut self, thread: &str) -> u64 {
        self.versions
            .get(thread)
            .copied()
            .unwrap_or_else(|| self.bump(thread))
    }
}
impl StopOp {
    fn invalidate_round(&mut self) {
        if self.verifying {
            self.verification_pending = true;
        }
        self.round += 1;
        self.verifying = false;
        if self.snapshot_absent.take().is_some() {
            self.turn_absent = false;
        }
    }

    /// 报告值说明本次停止对 goal 做成了什么，与制动证据分开：成功的清除不再改变，
    /// 成功的暂停只会升级为清除；后续只读核实（none、unchanged）与写失败只在尚无成功动作时生效。
    fn report_goal(&mut self, value: &str) {
        let keep = match self.goal.as_str() {
            "cleared" => true,
            "paused" => value != "cleared",
            _ => false,
        };
        if !keep {
            self.goal = value.into();
        }
    }

    fn tag(&self, id: &str, round: u64) -> StopTag {
        StopTag {
            id: id.into(),
            generation: self.generation,
            round,
        }
    }
    fn token(&self) -> ClaimToken {
        ClaimToken {
            thread_id: self.thread.clone(),
            claim_id: self.claim_id.unwrap_or(0),
        }
    }
    fn result(&self, outcome: &str) -> Value {
        json!({"threadId":self.thread,"runId":self.run_id,"interruptSent":self.interrupt_sent,
            "goal":self.goal,"alreadyStopped":self.already_stopped,"stopOutcome":outcome,"run":null})
    }
}

impl Machine {
    fn cancel_breaker_probe(&mut self, actions: &mut Vec<Action>) {
        let probe = &mut self.stops.probe;
        if probe.scheduled {
            actions.push(Action::CancelBreakerProbe {
                generation: probe.generation,
            });
            probe.scheduled = false;
            probe.generation = probe.generation.checked_add(1).expect("熔断试探代号已耗尽");
        }
    }

    /// 熔断断开时始终保留对账试探，或唯一的独立试探/后继计时器。
    pub(crate) fn ensure_breaker_probe(&mut self, actions: &mut Vec<Action>) {
        if self.closing || !self.app_server_unresponsive {
            return;
        }
        let reconciling = !self.stops.reconciliations.is_empty()
            || self.stops.late.iter().any(|reg| reg.late_settling);
        if reconciling {
            self.cancel_breaker_probe(actions);
            return;
        }
        let probe = &mut self.stops.probe;
        if probe.scheduled || probe.pending.is_some() {
            return;
        }
        probe.generation = probe.generation.checked_add(1).expect("熔断试探代号已耗尽");
        probe.scheduled = true;
        let delay_ms = reconcile_delay(&mut probe.delay_ms, true);
        actions.push(Action::ScheduleBreakerProbe {
            generation: probe.generation,
            delay_ms,
        });
    }

    pub(crate) fn breaker_probe_timer(&mut self, generation: u64, actions: &mut Vec<Action>) {
        if self.closing
            || !self.app_server_unresponsive
            || !self.stops.probe.scheduled
            || self.stops.probe.generation != generation
        {
            return;
        }
        self.stops.probe.scheduled = false;
        if self.stops.probe.pending.is_some()
            || !self.stops.reconciliations.is_empty()
            || self.stops.late.iter().any(|reg| reg.late_settling)
        {
            return;
        }
        self.stops.probe.pending = Some(generation);
        actions.push(Action::Request(RequestContext {
            resume_retried: false,
            stream_id: None,
            review_id: None,
            client: None,
            token: None,
            operation: Operation::BreakerProbe { generation },
            method: "thread/list".into(),
            params: Some(json!({"limit":1})),
        }));
    }

    pub(crate) fn breaker_probe_reply(&mut self, generation: u64, _actions: &mut Vec<Action>) {
        if self.closing {
            return;
        }
        if self.stops.probe.pending == Some(generation) {
            self.stops.probe.pending = None;
        }
    }

    pub(crate) fn observe_app_response(&mut self, actions: &mut Vec<Action>) {
        if self.closing {
            return;
        }
        if self.app_server_unresponsive {
            self.app_server_unresponsive = false;
            actions.push(Action::Debug(
                "Codex 已恢复回应，继续接收新的流式请求".into(),
            ));
            self.cancel_breaker_probe(actions);
            self.stops.probe.delay_ms = 0;
        }
    }

    pub(crate) fn reject_stream_admission(
        &self,
        request: &ClientRequest,
        actions: &mut Vec<Action>,
    ) -> bool {
        if request
            .params
            .as_ref()
            .and_then(|params| params.get("threadId"))
            .and_then(Value::as_str)
            .is_some_and(|thread| {
                self.stops
                    .changes
                    .values()
                    .any(|change| change.thread == thread && change.late_brake)
            })
        {
            // 线上线程忙文本逐字沿用 broker-server.ts:1767；阻止旧暂停晚到后影响新开工。
            stop_error(
                request.clone(),
                code::THREAD_BUSY,
                "A turn is already running on this thread.",
                None,
                actions,
            );
            return true;
        }
        let rejection = if self.app_server_unresponsive {
            Some((
                "appServerUnresponsive",
                "Codex 无响应，已暂停接收新的流式请求，等待服务器恢复回应",
            ))
        } else if self.stream_slots.len() >= MAX_UNRESOLVED_STARTS {
            Some((
                "tooManyUnresolvedStarts",
                "尚未落定的启动请求及对账已达到 64 项，暂停接收新的流式请求",
            ))
        } else {
            None
        };
        if let Some((reason, message)) = rejection {
            stop_error(
                request.clone(),
                code::GENERAL_ERROR,
                message,
                Some(json!({"reason":reason})),
                actions,
            );
            true
        } else {
            false
        }
    }
    fn stop_key(&self, thread: &str) -> Option<String> {
        self.stops
            .operations
            .iter()
            .find(|(_, op)| op.thread == thread && !op.foreign_claim)
            .map(|(id, _)| id.clone())
    }
    pub(crate) fn stop_busy(&self, thread: &str) -> bool {
        self.stop_key(thread).is_some()
            || self.latest_run(thread).is_some_and(|run| {
                self.stops
                    .operations
                    .values()
                    .any(|op| op.run_id.as_deref() == Some(&run.run_id))
            })
    }
    pub(crate) fn stops_busy(&self) -> bool {
        self.stops.operations.values().any(|op| !op.foreign_claim)
    }

    pub(crate) fn begin_stop(
        &mut self,
        thread: &str,
        reason: StopReason,
        clear: bool,
        actions: &mut Vec<Action>,
    ) -> Option<String> {
        if self.closing {
            return None;
        }
        if let Some(id) = self.stop_key(thread) {
            let op = self.stops.operations.get_mut(&id).unwrap();
            if clear && !op.clear {
                op.clear = true;
                op.goal_attempted = false;
                op.clear_done = false;
                op.invalidate_round();
            }
            return Some(id);
        }
        let claim = self.claims.get(thread);
        if claim.is_some_and(|c| c.joined()) {
            return None;
        }
        self.stops.ensure_version(thread);
        let generation = self.stops.allocate();
        let run_id = claim.and_then(|c| c.run_id.clone());
        let id = run_id
            .clone()
            .unwrap_or_else(|| format!("停止临时-{generation}"));
        let target = claim
            .and_then(|c| c.turn_id())
            .filter(|t| !t.is_empty())
            .map(str::to_owned);
        let mut owned_turns = claim
            .map(|claim| claim.owned_turns.clone())
            .unwrap_or_default();
        if let Some(target) = &target {
            owned_turns.insert(target.clone());
        }
        let ended = self
            .records
            .get(thread)
            .map(|r| {
                r.ended_turns
                    .iter()
                    .filter(|turn| owned_turns.contains(*turn))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        let turn_absent = claim.is_none_or(|c| {
            c.pending_start.is_some() || matches!(c.state, State::AwaitingContinuation { .. })
        });
        let op = StopOp {
            thread: thread.into(),
            claim_id: claim.map(|c| c.claim_id),
            foreign_claim: false,
            run_id: run_id.clone().or_else(|| {
                claim
                    .is_none()
                    .then(|| self.records.get(thread).and_then(|r| r.last_run.clone()))
                    .flatten()
            }),
            generation,
            reason,
            clear,
            goal: "unchanged".into(),
            brake_version: None,
            goal_round: 0,
            goal_busy: false,
            goal_attempted: false,
            goal_done: false,
            clear_done: !clear,
            target,
            turn_absent,
            snapshot_absent: None,
            ended,
            owned_turns,
            interrupted: HashSet::new(),
            interrupt_sent: false,
            explicit_interrupts: false,
            round: 0,
            verifying: false,
            verify_timer: 0,
            verification_pending: false,
            read_done: false,
            goal_read_done: false,
            outcome: Outcome::Pending,
            waiters: Vec::new(),
            already_stopped: claim.is_none(),
        };
        actions.push(Action::Schedule {
            token: op.token(),
            kind: TimerKind::Stop(op.tag(&id, 0), StopTimer::Deadline),
            delay_ms: CONFIRM_MS,
        });
        if let Some(run) = &run_id {
            self.run_stop_began(run, reason, actions);
        }
        self.stops.operations.insert(id.clone(), op);
        if let Some(claim) = self.claims.get_mut(thread) {
            if let Some(request) = claim.pending_start.take() {
                claim.request_pending = false;
                if let Some(id) = claim.stream_id.take() {
                    self.stream_slots.remove(&id);
                }
                if self.live_conns.contains(&request.conn) && !claim.owner_gone {
                    actions.push(Action::Send {
                        conn: request.conn,
                        message: proto::response_err(
                            request.id,
                            proto::error_object(
                                code::GENERAL_ERROR,
                                "启动请求已被停止操作取消",
                                None,
                            ),
                        ),
                    });
                }
                // 待发请求已经取消，共享归属也不会再产生不确定性。
                if let State::Starting { settling, .. } = &mut claim.state {
                    *settling = false;
                }
            }
        }
        Some(id)
    }

    pub(crate) fn client_stop(&mut self, request: ClientRequest, actions: &mut Vec<Action>) {
        let thread = request
            .params
            .as_ref()
            .and_then(|p| p.get("threadId"))
            .and_then(Value::as_str);
        let clear = request.params.as_ref().and_then(|p| p.get("clear"));
        let expected = request.params.as_ref().and_then(|p| p.get("runId"));
        if thread.is_none()
            || clear.is_some_and(|v| !v.is_boolean())
            || expected.is_some_and(|v| v.as_str().is_none_or(str::is_empty))
        {
            stop_error(
                request,
                code::INVALID_PARAMS,
                "停止请求需要字符串 threadId、可选布尔值 clear 和可选非空字符串 runId",
                None,
                actions,
            );
            return;
        }
        if self.closing {
            stop_error(
                request,
                code::GENERAL_ERROR,
                crate::claim::SHUTTING_DOWN,
                Some(json!({"reason":"shuttingDown"})),
                actions,
            );
            return;
        }
        let mut thread = thread.unwrap().to_owned();
        if self.lifecycle_gate(&request, &thread, actions) {
            return;
        }
        if let Some(run) = expected.and_then(Value::as_str) {
            if !self.runs.runs.contains_key(run) {
                self.lifecycle_history_stop(request.clone(), run.to_owned(), actions);
                return;
            }
        }
        if let Some(expected) = expected.and_then(Value::as_str) {
            let record = self.runs.runs.get(expected);
            let belongs = record.is_some_and(|run| {
                run.thread_id == thread || run.review_thread_id.as_deref() == Some(&thread)
            });
            let ended = belongs
                && record.is_some_and(|run| {
                    run.state == RunState::Ended
                        || run.state == RunState::Lost
                            && run.stop_outcome == Some(StopOutcome::Confirmed)
                });
            if ended {
                actions.push(Action::Send {conn: request.conn, message: proto::response_ok(request.id.clone(),
                    json!({"threadId":thread,"runId":expected,"interruptSent":false,"goal":"unchanged",
                        "alreadyStopped":true,"stopOutcome":"confirmed","run":self.run_snapshot(expected)}))});
                return;
            }
            // 原子核对运行身份后，review 按稳定 runId 定位实际认领；绝不借父线程去停新运行。
            let actual = self
                .claims
                .iter()
                .find(|(id, claim)| {
                    claim.run_id.as_deref() == Some(expected) && (belongs || id.as_str() == thread)
                })
                .map(|(id, _)| id.clone())
                .or_else(|| {
                    record
                        .filter(|r| {
                            belongs
                                && !r.joined
                                && (r.state == RunState::Stopping
                                    || r.state == RunState::Lost
                                        && r.stop_outcome != Some(StopOutcome::Confirmed))
                        })
                        .map(|r| {
                            r.review_thread_id
                                .clone()
                                .unwrap_or_else(|| r.thread_id.clone())
                        })
                });
            let Some(actual) = actual else {
                stop_error(
                    request,
                    code::GENERAL_ERROR,
                    "线程当前运行与预期 runId 不一致",
                    Some(json!({"reason":"runMismatch"})),
                    actions,
                );
                return;
            };
            thread = actual;
        }
        // 无 runId 时只从空闲父线程的最近运行定位，不能借历史关系取得其他运行的权限。
        let review = if expected.is_none() && !self.claims.contains_key(&thread) {
            self.latest_run(&thread)
                .filter(|run| {
                    !run.joined
                        && run.state != RunState::Ended
                        && !(run.state == RunState::Lost
                            && run.stop_outcome == Some(StopOutcome::Confirmed))
                })
                .and_then(|run| {
                    run.review_thread_id
                        .as_ref()
                        .filter(|child| *child != &thread)
                        .map(|child| (run.run_id.clone(), child.clone(), run.state))
                })
        } else {
            None
        };
        if let Some((run, child, state)) = &review {
            let claim = self.claims.get(child);
            let conflict = claim.is_some_and(|claim| claim.run_id.as_deref() != Some(run))
                || self.stops.operations.values().any(|op| {
                    op.thread == *child && (op.run_id.as_deref() != Some(run) || op.foreign_claim)
                });
            let missing =
                *state == RunState::Running && claim.is_none() && self.stop_key(child).is_none();
            if conflict || missing {
                stop_error(
                    request,
                    code::THREAD_BUSY,
                    "实际执行线程已有其他运行、停止操作或缺少本次运行的认领",
                    Some(json!({"reason":"stopping"})),
                    actions,
                );
                return;
            }
            thread = child.clone();
        }
        if self.lifecycle_gate(&request, &thread, actions) {
            return;
        }
        if self
            .claims
            .get(&thread)
            .is_some_and(crate::claim::Claim::precheck)
        {
            // #28d：先取消预检（原 turn/start 回 continueCancelled），再按 #28c 对原运行走停止核实（乙路）。
            self.continue_cancel(&thread, crate::claim::resume::CancelCause::Stop, actions);
        }
        if let Some(claim) = self.claims.get(&thread) {
            let reason = if claim.settling() {
                Some("settling")
            } else if claim.joined() {
                Some("joined")
            } else {
                None
            };
            if let Some(reason) = reason {
                stop_error(
                    request,
                    code::GENERAL_ERROR,
                    STOP_NOT_OWNED,
                    Some(json!({"reason":reason})),
                    actions,
                );
                return;
            }
        }
        let lost = review
            .as_ref()
            .and_then(|(run, _, _)| self.runs.runs.get(run))
            .or_else(|| {
                expected
                    .and_then(Value::as_str)
                    .and_then(|id| self.runs.runs.get(id))
            })
            .or_else(|| self.latest_run(&thread))
            .filter(|r| {
                (r.state == RunState::Stopping
                    || r.state == RunState::Lost
                        && r.stop_outcome == Some(StopOutcome::Unconfirmed))
                    && !r.joined
            })
            .map(|r| {
                (
                    r.run_id.clone(),
                    r.review_thread_id
                        .clone()
                        .unwrap_or_else(|| r.thread_id.clone()),
                )
            });
        let lost = lost.filter(|_| !self.claims.contains_key(&thread));
        let id = if let Some((run, actual)) = lost {
            if self.claims.contains_key(&actual)
                || self.stop_key(&actual).is_some_and(|id| {
                    self.stops.operations[&id].run_id.as_deref() != Some(run.as_str())
                })
            {
                stop_error(
                    request,
                    code::THREAD_BUSY,
                    "实际执行线程已有其他运行或停止操作",
                    Some(json!({"reason":"stopping"})),
                    actions,
                );
                return;
            }
            self.begin_lost_stop(&actual, &run, clear == Some(&Value::Bool(true)), actions)
        } else {
            self.begin_stop(
                &thread,
                StopReason::Stop,
                clear == Some(&Value::Bool(true)),
                actions,
            )
            .unwrap()
        };
        let op = self.stops.operations.get_mut(&id).unwrap();
        op.goal_attempted = false;
        if op.clear {
            op.clear_done = false;
        }
        op.explicit_interrupts = true;
        if op.waiters.len() >= MAX_STOP_WAITERS {
            let mut result = op.result("unconfirmed");
            let run = op.run_id.clone();
            result["threadId"] = json!(thread);
            result["run"] = run
                .as_deref()
                .and_then(|run| self.run_snapshot(run))
                .unwrap_or(Value::Null);
            actions.push(Action::Send {
                conn: request.conn,
                message: proto::response_ok(request.id.clone(), result),
            });
            return;
        }
        let waiter_id = self.stops.allocate();
        let op = self.stops.operations.get_mut(&id).unwrap();
        op.waiters.push(Waiter {
            request,
            id: waiter_id,
        });
        actions.push(Action::Schedule {
            token: op.token(),
            kind: TimerKind::Stop(op.tag(&id, 0), StopTimer::Waiter(waiter_id)),
            delay_ms: CONFIRM_MS,
        });
    }

    /// 展示唯一判定的五项事实，不修改停止状态。
    pub(crate) fn stop_check(&self, op: &StopOp) -> StopCheck {
        let claim = op
            .claim_id
            .and_then(|id| self.claims.get(&op.thread).filter(|c| c.claim_id == id));
        let ownership = !op.foreign_claim && claim.is_none_or(|c| !c.settling() && !c.joined());
        let turn = op.turn_absent || op.target.as_ref().is_some_and(|t| op.ended.contains(t));
        let brake = op.brake_version == Some(self.stops.version(&op.thread));
        let settled = claim.is_none_or(|c| !c.request_pending)
            && !self.stops.changes.values().any(|c| c.thread == op.thread);
        StopCheck {
            ownership,
            turn_ended: turn,
            goal_braked: brake,
            settled,
            verify_done: !op.verifying || (op.read_done && op.goal_read_done),
        }
    }

    /// 唯一停止判定；外层 goal_busy、clear_done 门控保持不变。
    pub(crate) fn confirmed(&self, op: &StopOp) -> bool {
        let check = self.stop_check(op);
        check.ownership
            && check.turn_ended
            && check.goal_braked
            && check.settled
            && check.verify_done
    }

    /// 重启后的运行不沿用 Idle 的缺轮证据，立即读取实际线程的当前状态。
    fn begin_lost_stop(
        &mut self,
        thread: &str,
        run: &str,
        clear: bool,
        actions: &mut Vec<Action>,
    ) -> String {
        if self.stop_key(thread).is_some() {
            return self
                .begin_stop(thread, StopReason::Stop, clear, actions)
                .expect("合并已有停止核实");
        }
        let id = self
            .begin_stop(thread, StopReason::Stop, clear, actions)
            .expect("丢失运行可建立停止核实");
        let mut op = self.stops.operations.remove(&id).unwrap();
        op.run_id = Some(run.into());
        self.runs.verifying_history.insert(run.into());
        op.target = None;
        op.turn_absent = false;
        op.already_stopped = false;
        op.ended.clear();
        self.start_verification(&id, &mut op, actions);
        self.stops.operations.insert(id.clone(), op);
        id
    }

    fn start_verification(&self, id: &str, op: &mut StopOp, actions: &mut Vec<Action>) {
        op.invalidate_round();
        op.verifying = true;
        op.verification_pending = true;
        op.read_done = false;
        op.goal_read_done = false;
        op.goal_attempted = true;
        stop_request(
            id,
            op,
            op.round,
            StopRequest::VerifyTurn,
            "thread/read",
            json!({"threadId":op.thread,"includeTurns":true}),
            actions,
        );
        stop_request(
            id,
            op,
            op.round,
            StopRequest::VerifyGoal {
                version: self.stops.version(&op.thread),
            },
            "thread/goal/get",
            json!({"threadId":op.thread}),
            actions,
        );
    }

    pub(crate) fn drive_stops(&mut self, actions: &mut Vec<Action>) {
        if self.closing {
            return;
        }
        let ids: Vec<_> = self.stops.operations.keys().cloned().collect();
        for id in ids {
            let mut op = self.stops.operations.remove(&id).unwrap();
            // review 迁移后的子线程也必须先分配版本，再发出目标读取。
            self.stops.ensure_version(&op.thread);
            let claim = op
                .claim_id
                .and_then(|cid| self.claims.get(&op.thread).filter(|c| c.claim_id == cid));
            if op.foreign_claim {
                if !op.waiters.is_empty() {
                    self.stops.operations.insert(id, op);
                }
                continue;
            }
            if claim.is_some_and(|c| c.joined()) {
                self.reject_joined_waiters(&mut op, actions);
                continue;
            }
            if let Some(turn) = claim.and_then(|c| c.turn_id()).filter(|t| !t.is_empty()) {
                if op.target.as_deref() != Some(turn) {
                    op.target = Some(turn.into());
                    op.turn_absent = false;
                    op.invalidate_round();
                }
            }
            let own = !op.foreign_claim
                && claim.is_none_or(|c| {
                    !c.settling()
                        && !(c.request_pending
                            && matches!(
                                c.state,
                                State::Starting {
                                    req: crate::claim::ReqKind::ReviewStart,
                                    ..
                                }
                            ))
                });

            let pending_changes = self
                .stops
                .changes
                .values()
                .any(|c| c.thread == op.thread && !c.unknown);
            if own && !pending_changes {
                if claim.is_some() {
                    if let Some(turn) = op.target.as_ref() {
                        op.owned_turns.insert(turn.clone());
                        if self
                            .records
                            .get(&op.thread)
                            .is_some_and(|record| record.ended_turns.contains(turn))
                        {
                            op.ended.insert(turn.clone());
                        }
                    }
                }
                if !op.verifying
                    && !op.goal_busy
                    && !op.goal_attempted
                    && (!op.goal_done
                        || (op.clear && !op.clear_done)
                        || op.brake_version != Some(self.stops.version(&op.thread)))
                {
                    op.goal_round += 1;
                    op.goal_busy = true;
                    op.goal_attempted = true;
                    stop_request(
                        &id,
                        &op,
                        op.goal_round,
                        StopRequest::GoalGet {
                            version: self.stops.version(&op.thread),
                        },
                        "thread/goal/get",
                        json!({"threadId":op.thread}),
                        actions,
                    );
                }
                if op.goal_done
                    && !op.goal_busy
                    && !self.stops.changes.values().any(|c| c.thread == op.thread)
                {
                    let explicit = std::mem::take(&mut op.explicit_interrupts);
                    self.stop_interrupt(&id, &mut op, explicit, actions);
                }
                // clear 的执行结果要先回来，报告值与停止证据分别保存。
                if !op.goal_busy && (!op.clear || op.clear_done) && self.confirmed(&op) {
                    if !op.already_stopped {
                        if let Some(run) = &op.run_id {
                            self.run_stop_confirmed(run, actions);
                        }
                    }
                    if op.claim_id.is_some_and(|cid| {
                        self.claims
                            .get(&op.thread)
                            .is_some_and(|c| c.claim_id == cid)
                    }) {
                        self.release_with_kind(&op.thread, ReleaseKind::StopConfirmed, actions);
                    }
                    self.answer_waiters(&mut op, None, "confirmed", actions);
                    // 表中只保留未确认操作，迟到回调找不到操作就忽略。
                    continue;
                }
            }
            self.stops.operations.insert(id, op);
        }
    }

    fn stop_interrupt(&self, id: &str, op: &mut StopOp, explicit: bool, actions: &mut Vec<Action>) {
        if op.goal_busy
            || !op.goal_done
            || self.stops.changes.values().any(|c| c.thread == op.thread)
        {
            return;
        }
        let Some(turn) = op.target.clone() else {
            return;
        };
        if op.turn_absent
            || op.ended.contains(&turn)
            || self
                .records
                .get(&op.thread)
                .is_some_and(|r| r.ended_turns.contains(&turn))
        {
            return;
        }
        if !explicit && op.interrupted.contains(&turn) {
            return;
        }
        op.interrupted.insert(turn.clone());
        op.interrupt_sent = true;
        stop_request(
            id,
            op,
            op.round,
            StopRequest::Interrupt { turn: turn.clone() },
            "turn/interrupt",
            json!({"threadId":op.thread,"turnId":turn}),
            actions,
        );
    }

    pub(crate) fn stop_started(&mut self, thread: &str, turn: &str) {
        if let Some(id) = self.stop_key(thread) {
            let op = self.stops.operations.get_mut(&id).unwrap();
            op.owned_turns.insert(turn.into());
            if op.target.as_deref() != Some(turn) {
                op.target = Some(turn.into());
                op.turn_absent = false;
                op.invalidate_round();
            }
        }
    }
    pub(crate) fn sync_stop_owned_turn(
        &mut self,
        thread: &str,
        claim_id: u64,
        turn: &str,
        ended: bool,
    ) {
        if let Some(id) = self.stop_key(thread) {
            let op = self.stops.operations.get_mut(&id).unwrap();
            if op.claim_id != Some(claim_id) {
                return;
            }
            op.owned_turns.insert(turn.into());
            if ended {
                op.ended.insert(turn.into());
            }
        }
    }
    pub(crate) fn stop_completed(&mut self, thread: &str, _actions: &mut Vec<Action>) {
        let target = self
            .claims
            .get(thread)
            .and_then(|c| c.turn_id())
            .map(str::to_owned);
        let version = self.stops.version(thread);
        if let Some(id) = self.stop_key(thread) {
            let op = self.stops.operations.get_mut(&id).unwrap();
            if let Some(target) = target {
                op.ended.insert(target);
            }
            if op.brake_version != Some(version) {
                op.goal_attempted = false;
            }
            // 完整结果已在 claim 提交通知、完成去重前交给账本；此处仅保留停止证据。
        }
    }
    pub(crate) fn stop_completed_turn(
        &mut self,
        thread: &str,
        turn: &str,
        actions: &mut Vec<Action>,
    ) {
        // 完整结果由 claim 提交与 review 回放入口交给账本，此处不重复结算。
        let version = self.stops.version(thread);
        if let Some(id) = self.stop_key(thread) {
            let op = self.stops.operations.get_mut(&id).unwrap();
            if op.target.as_deref() == Some(turn) || op.owned_turns.contains(turn) {
                op.ended.insert(turn.into());
                if op.target.as_deref() == Some(turn) {
                    op.invalidate_round();
                }
                if op.brake_version != Some(version) {
                    op.goal_attempted = false;
                }
            }
        }
        let ids: Vec<_> = self
            .stops
            .late
            .iter()
            .filter(|l| l.target_thread == thread && l.turn.as_deref() == Some(turn))
            .map(|l| l.generation)
            .collect();
        for id in ids {
            self.remove_late(id, actions);
        }
    }
    pub(crate) fn stop_no_turn(&mut self, thread: &str) {
        if let Some(id) = self.stop_key(thread) {
            self.stops.operations.get_mut(&id).unwrap().turn_absent = true;
            if let Some(c) = self.claims.get_mut(thread) {
                c.request_pending = false;
                if let State::Starting { settling, .. } = &mut c.state {
                    *settling = false;
                }
            }
        }
    }
    fn reject_joined_waiters(&self, op: &mut StopOp, actions: &mut Vec<Action>) {
        for waiter in op.waiters.drain(..) {
            if self.live_conns.contains(&waiter.request.conn) {
                stop_error(
                    waiter.request,
                    code::GENERAL_ERROR,
                    STOP_NOT_OWNED,
                    Some(json!({"reason":"joined"})),
                    actions,
                );
            }
        }
    }
    pub(crate) fn finish_joined_stop(
        &mut self,
        thread: &str,
        claim_id: u64,
        actions: &mut Vec<Action>,
    ) {
        let ids: Vec<_> = self
            .stops
            .operations
            .iter()
            .filter(|(_, op)| op.thread == thread && op.claim_id == Some(claim_id))
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            let mut op = self.stops.operations.remove(&id).unwrap();
            self.reject_joined_waiters(&mut op, actions);
        }
        self.retire_joined_tracking(thread, claim_id, actions);
    }
    pub(crate) fn schedule_next_verifications(&mut self, actions: &mut Vec<Action>) {
        for (id, op) in &mut self.stops.operations {
            if !op.foreign_claim
                && op.outcome == Outcome::Unconfirmed
                && !op.verifying
                && op.verification_pending
            {
                op.verification_pending = false;
                actions.push(Action::Schedule {
                    token: op.token(),
                    kind: TimerKind::Stop(op.tag(id, op.verify_timer), StopTimer::Verify),
                    delay_ms: VERIFY_MS,
                });
            }
        }
    }
    pub(crate) fn take_moving_stop(&mut self, parent: &str) -> Option<(String, StopOp)> {
        let id = self.stop_key(parent)?;
        Some((id.clone(), self.stops.operations.remove(&id)?))
    }
    pub(crate) fn restore_moving_stop(
        &mut self,
        moving: Option<(String, StopOp)>,
        child: &str,
        claim_id: u64,
    ) {
        if let Some((id, mut op)) = moving {
            op.thread = child.into();
            op.claim_id = Some(claim_id);
            op.target = self
                .claims
                .get(child)
                .and_then(|c| c.turn_id())
                .filter(|s| !s.is_empty())
                .map(str::to_owned);
            op.turn_absent = false;
            op.owned_turns = self
                .claims
                .get(child)
                .map(|claim| claim.owned_turns.clone())
                .unwrap_or_default();
            if let Some(target) = &op.target {
                op.owned_turns.insert(target.clone());
            }
            op.ended.retain(|turn| op.owned_turns.contains(turn));
            op.brake_version = None;
            op.goal_done = false;
            op.goal_busy = false;
            op.goal_attempted = false;
            op.invalidate_round();
            op.goal_round += 1;
            self.stops.operations.insert(id, op);
        }
    }
    pub(crate) fn preserve_review_collision(
        &mut self,
        moving: Option<(String, StopOp)>,
        child: &str,
        turn: Option<&str>,
    ) {
        if let Some((id, mut op)) = moving {
            // 子线程已经由其他认领占用，保留原等待期限，但不动其 goal 或 turn。
            op.thread = child.into();
            op.claim_id = None;
            op.foreign_claim = true;
            op.target = turn.map(str::to_owned);
            op.turn_absent = false;
            op.goal_busy = false;
            op.goal_round += 1;
            op.brake_version = None;
            op.invalidate_round();
            self.stops.operations.insert(id, op);
        }
    }
    pub(crate) fn observe_goal(&mut self, thread: &str, active: bool) {
        self.stops.bump(thread);
        if let Some(id) = self.stop_key(thread) {
            let op = self.stops.operations.get_mut(&id).unwrap();
            op.invalidate_round();
            if active {
                op.brake_version = None;
                op.goal_done = false;
                op.goal_attempted = false;
            }
        }
    }
}

impl Machine {
    pub(crate) fn stop_reply(
        &mut self,
        tag: StopTag,
        request: StopRequest,
        result: Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) {
        if self.closing {
            return;
        }
        let Some(mut op) = self.stops.operations.remove(&tag.id) else {
            return;
        };
        if op.generation != tag.generation {
            self.stops.operations.insert(tag.id, op);
            return;
        }
        match request {
            StopRequest::Interrupt { turn } => {
                if let Err(error) = result {
                    actions.push(Action::Warn(format!(
                        "线程 {} 的中断请求失败：{}",
                        op.thread, error
                    )));
                    if let Some(found) = retarget(&error) {
                        if found != turn
                            && op.target.as_deref() == Some(found)
                            && self.claims.get(&op.thread).is_some_and(|c| {
                                c.claim_id == op.claim_id.unwrap_or(0)
                                    && !c.joined()
                                    && !c.settling()
                                    && c.turn_id() == Some(found)
                            })
                        {
                            self.stop_interrupt(&tag.id, &mut op, false, actions);
                        }
                    }
                }
            }
            StopRequest::GoalGet { version } | StopRequest::VerifyGoal { version } => {
                let verify = matches!(request, StopRequest::VerifyGoal { .. });
                let valid_round = if verify {
                    op.verifying && op.round == tag.round
                } else {
                    op.goal_round == tag.round
                };
                if !valid_round {
                    self.stops.operations.insert(tag.id, op);
                    return;
                }
                if !verify {
                    op.goal_busy = false;
                }
                if version != self.stops.version(&op.thread)
                    || self
                        .stops
                        .changes
                        .values()
                        .any(|c| c.thread == op.thread && !c.unknown)
                {
                    op.goal_attempted = false;
                    if verify {
                        op.invalidate_round();
                    }
                } else {
                    match read_goal(&result) {
                        Some(goal) => {
                            let active = goal.as_ref().is_some_and(|g| {
                                g.get("status").and_then(Value::as_str) == Some("active")
                            });
                            self.run_goal_status(
                                &op.thread,
                                goal.as_ref()
                                    .and_then(|g| g.get("status"))
                                    .and_then(Value::as_str),
                            );
                            self.record(&op.thread).goal_active = active;
                            if !active {
                                op.brake_version = Some(version);
                            }
                            op.goal_done = true;
                            if verify {
                                op.goal_read_done = true;
                            }
                            if goal.is_none() {
                                op.report_goal("none");
                                op.clear_done = true;
                            } else if op.clear || active {
                                op.brake_version = if active { None } else { op.brake_version };
                                op.goal_busy = true;
                                op.goal_round += 1;
                                let clear = op.clear;
                                let method = if clear {
                                    "thread/goal/clear"
                                } else {
                                    "thread/goal/set"
                                };
                                let params = if clear {
                                    json!({"threadId":op.thread})
                                } else {
                                    json!({"threadId":op.thread,"status":"paused"})
                                };
                                stop_request(
                                    &tag.id,
                                    &op,
                                    op.goal_round,
                                    StopRequest::GoalWrite { version, clear },
                                    method,
                                    params,
                                    actions,
                                );
                            } else {
                                op.report_goal("unchanged");
                            }
                        }
                        None => {
                            op.goal_done = true;
                            if verify {
                                op.goal_read_done = true;
                            }
                            if op.clear {
                                op.goal_busy = true;
                                op.goal_round += 1;
                                stop_request(
                                    &tag.id,
                                    &op,
                                    op.goal_round,
                                    StopRequest::GoalWrite {
                                        version,
                                        clear: true,
                                    },
                                    "thread/goal/clear",
                                    json!({"threadId":op.thread}),
                                    actions,
                                );
                            }
                            actions.push(Action::Warn(format!(
                                "线程 {} 的 goal 读取失败，停止尚未确认",
                                op.thread
                            )));
                        }
                    }
                }
            }
            StopRequest::GoalWrite { version, clear } => {
                if op.goal_round != tag.round {
                    self.stops.operations.insert(tag.id, op);
                    return;
                }
                op.goal_busy = false;
                op.goal_done = true;
                if clear {
                    op.clear_done = true;
                }
                if result.is_ok() {
                    op.report_goal(if clear { "cleared" } else { "paused" });
                    if version == self.stops.version(&op.thread)
                        && !self.stops.changes.values().any(|c| c.thread == op.thread)
                    {
                        self.run_goal_status(&op.thread, if clear { None } else { Some("paused") });
                        self.record(&op.thread).goal_active = false;
                        op.brake_version = Some(self.stops.bump(&op.thread));
                    } else {
                        op.brake_version = None;
                        op.goal_attempted = false;
                    }
                } else {
                    op.report_goal("unchanged");
                    actions.push(Action::Warn(format!(
                        "线程 {} 的 goal 制动失败，继续中断与核实",
                        op.thread
                    )));
                }
                if op.clear && !clear {
                    op.goal_attempted = false;
                }
            }
            StopRequest::VerifyTurn => {
                if !op.verifying || op.round != tag.round {
                    self.stops.operations.insert(tag.id, op);
                    return;
                }
                op.read_done = true;
                if let Some(active) = strict_active(&result) {
                    let active: Vec<_> = active
                        .into_iter()
                        .filter(|t| !op.ended.contains(t))
                        .collect();
                    // 不把共享服务器上未观察到的其他 turn 认成自己的。
                    op.turn_absent = active.is_empty();
                    op.snapshot_absent = op.turn_absent.then_some(op.round);
                    if !self.shared || (op.claim_id.is_none() && op.run_id.is_some()) {
                        if let Some(turn) = active.first() {
                            if op.target.as_ref() != Some(turn) {
                                op.target = Some(turn.clone());
                                if let Some(claim) = self.claims.get_mut(&op.thread) {
                                    match &mut claim.state {
                                        State::Running { turn_id, .. } => *turn_id = turn.clone(),
                                        State::Starting { turn_id, .. } => {
                                            *turn_id = Some(turn.clone())
                                        }
                                        _ => {}
                                    }
                                }
                                self.record_run_turn_started(&op.thread, turn, actions);
                            }
                        }
                    }
                } else {
                    actions.push(Action::Warn(format!(
                        "线程 {} 的停止核实读取未知，保留占用",
                        op.thread
                    )));
                }
            }
        }
        if op.verifying && op.read_done && op.goal_read_done {
            if (!self.confirmed(&op) || op.goal_busy || (op.clear && !op.clear_done))
                && !op.already_stopped
            {
                if let Some(run) = &op.run_id {
                    self.run_stop_check(run, self.stop_check(&op), actions);
                }
            }
            op.verifying = false;
            op.verification_pending = false;
            actions.push(Action::Schedule {
                token: op.token(),
                kind: TimerKind::Stop(op.tag(&tag.id, op.verify_timer), StopTimer::Verify),
                delay_ms: VERIFY_MS,
            });
        }
        self.stops.operations.insert(tag.id, op);
    }

    pub(crate) fn stop_timer(&mut self, tag: StopTag, kind: StopTimer, actions: &mut Vec<Action>) {
        if self.closing {
            return;
        }
        let Some(mut op) = self.stops.operations.remove(&tag.id) else {
            return;
        };
        if op.generation != tag.generation {
            self.stops.operations.insert(tag.id, op);
            return;
        }
        if op.foreign_claim {
            match kind {
                StopTimer::Waiter(id) => {
                    self.answer_waiters(&mut op, Some(id), "unconfirmed", actions)
                }
                StopTimer::Deadline => self.answer_waiters(&mut op, None, "unconfirmed", actions),
                StopTimer::Verify => {}
            }
            if !op.waiters.is_empty() {
                self.stops.operations.insert(tag.id, op);
            }
            return;
        }
        match kind {
            StopTimer::Deadline => {
                if op.outcome != Outcome::Pending {
                    self.stops.operations.insert(tag.id, op);
                    return;
                }
                op.outcome = Outcome::Unconfirmed;
                if !op.already_stopped {
                    if let Some(run) = &op.run_id {
                        self.run_stop_check(run, self.stop_check(&op), actions);
                    }
                }
                actions.push(Action::Schedule {
                    token: op.token(),
                    kind: TimerKind::Stop(op.tag(&tag.id, op.verify_timer), StopTimer::Verify),
                    delay_ms: VERIFY_MS,
                });
            }
            StopTimer::Waiter(id) => self.answer_waiters(&mut op, Some(id), "unconfirmed", actions),
            StopTimer::Verify => {
                if tag.round != op.verify_timer || op.outcome != Outcome::Unconfirmed {
                    self.stops.operations.insert(tag.id, op);
                    return;
                }
                op.verify_timer += 1;
                if op.verifying {
                    self.stops.operations.insert(tag.id, op);
                    return;
                }
                let ownership_pending = op.foreign_claim
                    || self.claims.get(&op.thread).is_some_and(|c| {
                        c.settling()
                            || (c.request_pending
                                && matches!(
                                    c.state,
                                    State::Starting {
                                        req: crate::claim::ReqKind::ReviewStart,
                                        ..
                                    }
                                ))
                    });
                if ownership_pending
                    || op.goal_busy
                    || self
                        .stops
                        .changes
                        .values()
                        .any(|c| c.thread == op.thread && !c.unknown)
                {
                    actions.push(Action::Schedule {
                        token: op.token(),
                        kind: TimerKind::Stop(op.tag(&tag.id, op.verify_timer), StopTimer::Verify),
                        delay_ms: VERIFY_MS,
                    });
                    self.stops.operations.insert(tag.id, op);
                    return;
                }
                self.start_verification(&tag.id, &mut op, actions);
            }
        }
        self.stops.operations.insert(tag.id, op);
    }
    fn answer_waiters(
        &self,
        op: &mut StopOp,
        only: Option<u64>,
        outcome: &str,
        actions: &mut Vec<Action>,
    ) {
        let mut result = op.result(outcome);
        result["run"] = op
            .run_id
            .as_deref()
            .and_then(|run| self.run_snapshot(run))
            .unwrap_or(Value::Null);
        op.waiters.retain(|waiter| {
            if only.is_none_or(|id| id == waiter.id) {
                if self.live_conns.contains(&waiter.request.conn) {
                    actions.push(Action::Send {
                        conn: waiter.request.conn,
                        message: proto::response_ok(waiter.request.id.clone(), {
                            let mut reply = result.clone();
                            if let Some(thread) = waiter
                                .request
                                .params
                                .as_ref()
                                .and_then(|p| p.get("threadId"))
                            {
                                reply["threadId"] = thread.clone();
                            }
                            reply
                        }),
                    });
                }
                false
            } else {
                true
            }
        });
    }
    pub(crate) fn disconnect_stops(&mut self, conn: ConnId) {
        for op in self.stops.operations.values_mut() {
            op.waiters.retain(|w| w.request.conn != conn);
        }
    }
    pub(crate) fn shutdown_stops(&mut self, actions: &mut Vec<Action>) {
        self.cancel_breaker_probe(actions);
        self.stops.probe.pending = None;
        for (_, mut op) in std::mem::take(&mut self.stops.operations) {
            self.answer_waiters(&mut op, None, "unconfirmed", actions);
        }
        let registrations: Vec<_> = self
            .stops
            .late
            .drain(..)
            .chain(self.stops.reconciliations.drain().map(|(_, l)| l))
            .collect();
        for reg in registrations {
            self.finish_tracking(reg, actions);
        }
        self.stops.retired_late_turns.clear();
        self.stops.changes.clear();
    }

    /// 变更结果未知最多保留十分钟；过期不是制动证据，必须重新读目标状态。
    pub(crate) fn expire_goal_change(&mut self, id: u64, actions: &mut Vec<Action>) {
        if self.closing
            || !self
                .stops
                .changes
                .get(&id)
                .is_some_and(|change| change.unknown)
        {
            return;
        }
        let change = self
            .stops
            .changes
            .remove(&id)
            .expect("已核对未知的目标变更");
        actions.push(Action::Warn(format!(
            "线程 {} 的未知目标变更 {id} 的迟到登记已结束，清理变更并重新核实",
            change.thread
        )));
        if let Some(tag) = change.wire_tag {
            actions.push(Action::ForgetLate { tag });
        }
        self.stops.bump(&change.thread);
        if let Some(key) = self.stop_key(&change.thread) {
            let op = self.stops.operations.get_mut(&key).unwrap();
            op.invalidate_round();
            op.brake_version = None;
            op.goal_done = false;
            // 沿用十五秒核实计划，不能把删除未知登记当作停止成功。
            op.goal_attempted = true;
        }
    }

    /// 只保留仍有线程记录、停止操作或目标变更的版本；全局计数器不随淘汰回退。
    pub(crate) fn prune_goal_versions(&mut self) {
        let retained: HashSet<_> = self
            .records
            .keys()
            .map(String::as_str)
            .chain(self.stops.operations.values().map(|op| op.thread.as_str()))
            .chain(
                self.stops
                    .changes
                    .values()
                    .map(|change| change.thread.as_str()),
            )
            .collect();
        self.stops
            .versions
            .retain(|thread, _| retained.contains(thread.as_str()));
    }

    /// 线程当前的 goal 事实版本（没有时分配一个），供续跑预检与 goal 恢复核对读数是否过期。
    pub(crate) fn goal_fact_version(&mut self, thread: &str) -> u64 {
        self.stops.ensure_version(thread)
    }

    /// goal 变更在真正发出时统一登记，客户端、停止操作、恢复操作走同一路径。
    pub(crate) fn track_goal_requests(&mut self, actions: &mut [Action]) {
        for action in actions {
            let Action::Request(context) = action else {
                continue;
            };
            let Some(thread) = context
                .params
                .as_ref()
                .and_then(|p| p.get("threadId"))
                .and_then(Value::as_str)
                .map(str::to_owned)
            else {
                continue;
            };
            if context.method.starts_with("thread/goal/") {
                // 请求本身也是线程流量；记录仍在时保留版本，淘汰后旧读数不能复用。
                self.record(&thread);
                self.stops.ensure_version(&thread);
            }
            if matches!(
                context.method.as_str(),
                "thread/goal/set" | "thread/goal/clear"
            ) {
                let id = self.stops.allocate();
                let previous_version = self.stops.version(&thread);
                let version = self.stops.bump(&thread);
                if let Operation::Stop(_, StopRequest::GoalWrite { version: v, .. }) =
                    &mut context.operation
                {
                    *v = version;
                }
                if let Operation::LateGoal {
                    version: v,
                    write: true,
                    ..
                } = &mut context.operation
                {
                    *v = version;
                }
                if let Operation::LoopGuardPause(target) = &mut context.operation {
                    target.version = version;
                }
                let control_epoch = self.control_epoch_for(&thread, context);
                if let Operation::LateGoal {
                    generation,
                    write: true,
                    ..
                } = &context.operation
                {
                    if let Some(reg) = self
                        .stops
                        .late
                        .iter_mut()
                        .find(|reg| reg.generation == *generation)
                    {
                        reg.goal_change_id = Some(id);
                    }
                }
                if let Some(key) = self.stop_key(&thread) {
                    let op = self.stops.operations.get_mut(&key).unwrap();
                    if op.verifying {
                        op.invalidate_round();
                    }
                    if op.brake_version == Some(previous_version) {
                        op.brake_version = Some(version);
                    }
                }
                self.stops.changes.insert(
                    id,
                    GoalChange {
                        thread,
                        unknown: false,
                        wire_tag: None,
                        late_brake: matches!(
                            context.operation,
                            Operation::LateGoal { write: true, .. }
                                | Operation::GoalRestore(
                                    crate::claim::resume::GoalRestoreStep::SetPaused { .. }
                                )
                        ),
                        control_epoch,
                        grant_objective: control_epoch.and_then(|_| {
                            context
                                .params
                                .as_ref()
                                .and_then(|p| p.get("objective"))
                                .and_then(Value::as_str)
                                .map(str::to_owned)
                        }),
                    },
                );
                if let Operation::GoalRestore(step) = &context.operation {
                    self.goal_restore_tracked(step, id, version);
                }
                context.operation = Operation::GoalChange {
                    id,
                    version,
                    inner: Box::new(context.operation.clone()),
                };
            } else if context.method == "thread/goal/get"
                && !matches!(
                    context.operation,
                    Operation::Stop(..)
                        | Operation::LateGoal { .. }
                        | Operation::ContinueRead(_)
                        | Operation::GoalRestore(_)
                )
            {
                // 续跑核实读与 goal 恢复读由乙路自己持有版本并核对，公共过滤不得吞掉它们的回包。
                context.operation = Operation::GoalRead {
                    version: self.stops.version(&thread),
                    inner: Box::new(context.operation.clone()),
                };
            }
        }
    }
    pub(crate) fn unwrap_goal_reply(
        &mut self,
        context: &mut RequestContext,
        result: &Result<Value, Value>,
        timeout: bool,
        actions: &mut Vec<Action>,
    ) -> bool {
        loop {
            match context.operation.clone() {
                Operation::GoalChange { id, version, inner } => {
                    if timeout {
                        let thread = self.stops.changes.get_mut(&id).and_then(|change| {
                            if change.unknown {
                                return None;
                            }
                            change.unknown = true;
                            Some(change.thread.clone())
                        });
                        if let Some(thread_id) = thread {
                            let token = self.token(&thread_id).unwrap_or(ClaimToken {
                                thread_id,
                                claim_id: 0,
                            });
                            actions.push(Action::Schedule {
                                token,
                                kind: TimerKind::GoalChangeExpiry { id },
                                delay_ms: LATE_MS,
                            });
                        }
                    } else {
                        // 只在真正解开写入包装时设置；同一回包再次进入解包（迟到回包转 apply_reply）不会清掉它。
                        self.grant_intent = self.stops.changes.remove(&id).and_then(|c| {
                            c.control_epoch
                                .map(|epoch| crate::claim::loop_guard::GrantIntent {
                                    thread: c.thread,
                                    epoch,
                                    objective: c.grant_objective,
                                })
                        });
                    }
                    context.operation = *inner;
                    let thread = context
                        .params
                        .as_ref()
                        .and_then(|p| p.get("threadId"))
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    if !timeout
                        && version != self.stops.version(thread)
                        && context.operation == Operation::Forward
                    {
                        // 迟到的 active 变更保守地撤销制动；旧 inactive 结果不能覆盖新事实。
                        if result
                            .as_ref()
                            .ok()
                            .and_then(|v| v.get("goal"))
                            .and_then(|g| g.get("status"))
                            .and_then(Value::as_str)
                            == Some("active")
                        {
                            self.run_goal_status(thread, Some("active"));
                            self.learn_goal(thread, true, actions);
                        }
                        // 版本常因本次写入自己引起的 updated 通知先到而推进；回包身份与当前事实一致才授予控制资格。
                        if context.method == "thread/goal/set" && context.client.is_some() {
                            if let Some(goal) = result.as_ref().ok().and_then(|v| v.get("goal")) {
                                self.grant_goal_control(thread, goal);
                            }
                        }
                        if let Some(c) = &context.client {
                            actions_deliver(c, result.clone(), actions);
                        }
                        return true;
                    }
                }
                Operation::GoalRead { version, inner } => {
                    context.operation = *inner;
                    let thread = context
                        .params
                        .as_ref()
                        .and_then(|p| p.get("threadId"))
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    if version != self.stops.version(thread) {
                        if let Some(c) = &context.client {
                            actions_deliver(c, result.clone(), actions);
                        }
                        return true;
                    }
                }
                _ => return false,
            }
        }
    }
}

impl Machine {
    pub(crate) fn timeout_reply(
        &mut self,
        mut context: RequestContext,
        error: Value,
        wire_tag: u64,
        deadline: std::time::Instant,
        actions: &mut Vec<Action>,
    ) {
        if let Operation::Lifecycle {
            thread,
            operation,
            attempt,
        } = &context.operation
        {
            self.lifecycle_reply(thread, operation, *attempt, Err(error), false, actions);
            return;
        }
        if self.closing {
            self.apply_reply(context, Err(error), actions);
            return;
        }
        if let Operation::GoalChange { id, .. } = &context.operation {
            if let Some(change) = self.stops.changes.get_mut(id) {
                change.wire_tag = Some(wire_tag);
            }
        }
        let before = actions.len();
        self.unwrap_goal_reply(&mut context, &Err(error.clone()), true, actions);
        let current_reconcile = match context.operation {
            Operation::Reconcile { generation, round } => self
                .stops
                .reconciliations
                .get(&generation)
                .is_some_and(|reg| reg.round == round),
            Operation::LateSettlement { generation, round } => {
                self.stops.late.iter().any(|reg| {
                    reg.generation == generation && reg.round == round && reg.late_settling
                })
            }
            _ => false,
        };
        if current_reconcile && !self.app_server_unresponsive {
            self.app_server_unresponsive = true;
            actions.push(Action::Warn(
                "流式启动和后续对账读取均超时，Codex 无响应，暂停接收新的流式请求".into(),
            ));
        }
        // 等待线上 goal 回包时不另开截止竞争入口，RPC 的有序丢弃事件负责过期。
        let mut index = before;
        while index < actions.len() {
            if matches!(
                actions[index],
                Action::Schedule {
                    kind: TimerKind::GoalChangeExpiry { .. },
                    ..
                }
            ) {
                actions.remove(index);
            } else {
                index += 1;
            }
        }
        if !matches!(context.operation, Operation::Stream { .. }) {
            if context.operation == Operation::Forward {
                context.stream_id = None;
            }
            self.apply_reply(context, Err(error), actions);
            return;
        }
        let Some(token) = context.token.clone() else {
            return;
        };
        // 上游 broker-server.ts:1430：steer 加入的是别人的轮次，失败只释放，永不中断。
        if context.method == "turn/steer" {
            self.release_stream_slot(context.stream_id);
            if let Some(client) = &context.client {
                actions_deliver(client, Err(error), actions);
            }
            if self.current(&token) {
                self.finish_joined_stop(&token.thread_id, token.claim_id, actions);
                self.release(&token.thread_id, actions);
            }
            actions.push(Action::ForgetLate { tag: wire_tag });
            return;
        }
        if !self.current(&token) {
            self.release_stream_slot(context.stream_id);
            if let Some(client) = &context.client {
                actions_deliver(client, Err(error), actions);
            }
            if context.method == "review/start" {
                self.finish_timed_out_review(context.review_id);
            }
            actions.push(Action::ForgetLate { tag: wire_tag });
            return;
        }
        self.claims.get_mut(&token.thread_id).unwrap().owner_gone = true;
        let generation = self.stops.allocate();
        let target = self.claims[&token.thread_id]
            .turn_id()
            .filter(|id| !id.is_empty())
            .map(str::to_owned);
        let review = context.method == "review/start";
        let reconciling = review || target.is_none() || self.claims[&token.thread_id].settling();
        let registration = LateStart {
            stream_id: context.stream_id,
            review_id: context.review_id,
            token: token.clone(),
            method: context.method.clone(),
            generation,
            round: 0,
            reconciling,
            turn: if review { None } else { target },
            target_thread: token.thread_id.clone(),
            interrupting: false,
            wire_tag,
            wire_available: true,
            deadline,
            review_pending: review,
            ownership_proven: !review && !self.shared,
            late_settling: false,
            reply_received: false,
            goal_round: 0,
            goal_busy: false,
            goal_done: false,
            goal_version: 0,
            goal_waiting_for_owner: false,
            goal_change_id: None,
            reconcile_delay_ms: 0,
            cleanup_due: false,
        };
        if reconciling {
            self.stops.reconciliations.insert(generation, registration);
            if let Some(client) = &context.client {
                actions_deliver(client, Err(error), actions);
            }
            self.begin_reconcile(token, generation, 0, actions);
        } else {
            self.push_late(registration, actions);
            self.apply_reply(context, Err(error), actions);
        }
    }
    fn begin_reconcile(
        &mut self,
        token: ClaimToken,
        generation: u64,
        round: u64,
        actions: &mut Vec<Action>,
    ) {
        actions.push(Action::Request(RequestContext {
            resume_retried: false,
            stream_id: None,
            review_id: None,
            client: None,
            params: Some(json!({"threadId":token.thread_id,"includeTurns":true})),
            token: Some(token),
            operation: Operation::Reconcile { generation, round },
            method: "thread/read".into(),
        }));
    }
    fn retry_reconcile(
        &mut self,
        mut registration: LateStart,
        failed: bool,
        actions: &mut Vec<Action>,
    ) {
        registration.round += 1;
        let delay_ms = reconcile_delay(&mut registration.reconcile_delay_ms, failed);
        actions.push(Action::Schedule {
            token: registration.token.clone(),
            kind: TimerKind::Reconcile {
                generation: registration.generation,
                round: registration.round,
            },
            delay_ms,
        });
        self.stops
            .reconciliations
            .insert(registration.generation, registration);
    }
    pub(crate) fn reconcile_reply(
        &mut self,
        _context: &RequestContext,
        generation: u64,
        round: u64,
        result: Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) {
        if self.closing {
            return;
        }
        let Some(mut registration) = self
            .stops
            .reconciliations
            .get(&generation)
            .filter(|l| l.round == round)
            .cloned()
        else {
            return;
        };
        let token = registration.token.clone();
        if !self.current(&token) {
            self.stops.reconciliations.remove(&generation);
            registration.reconciling = false;
            self.push_late(registration, actions);
            return;
        }
        let active = strict_active(&result);
        if active.is_some() {
            registration.reconcile_delay_ms = 0;
        }
        // 子线程未知时，父线程快照不能授予 review 停止权限；等待权威回包。
        if registration.method == "review/start" {
            self.retry_reconcile(registration, active.is_none(), actions);
            return;
        }
        match active {
            Some(active) if active.is_empty() => {
                self.release_stream_slot(registration.stream_id);
                self.stops.reconciliations.remove(&generation);
                registration.reconciling = false;
                self.push_late(registration, actions);
                self.claims
                    .get_mut(&token.thread_id)
                    .unwrap()
                    .request_pending = false;
                if self.stop_busy(&token.thread_id) {
                    self.stop_no_turn(&token.thread_id);
                } else {
                    self.release(&token.thread_id, actions);
                }
            }
            Some(active) => {
                let observed = self.claims[&token.thread_id].turn_id().map(str::to_owned);
                let owned = if self.shared {
                    observed.filter(|id|active.contains(id)).filter(|id| matches!(&self.claims[&token.thread_id].state,
                        State::Starting {held,submitted_at,..} if held.iter().skip(*submitted_at).any(|n|n.method=="turn/started" && response_turn(n.params.as_ref().unwrap_or(&Value::Null))==Some(id))))
                } else {
                    active.first().cloned()
                };
                let Some(turn) = owned else {
                    self.retry_reconcile(registration, false, actions);
                    return;
                };
                self.stops.reconciliations.remove(&generation);
                registration.turn = Some(turn.clone());
                self.release_stream_slot(registration.stream_id);
                registration.reconciling = false;
                registration.ownership_proven = true;
                self.push_late(registration, actions);
                let claim = self.claims.get_mut(&token.thread_id).unwrap();
                claim.request_pending = false;
                if let State::Starting { turn_id, .. } = &mut claim.state {
                    *turn_id = Some(turn.clone());
                }
                self.begin_stop(&token.thread_id, StopReason::Stop, false, actions);
                if claim_settling(self, &token) {
                    self.begin_settlement(&token.thread_id, 0, actions);
                } else {
                    let claim = self.claims.get_mut(&token.thread_id).unwrap();
                    let owner = claim.owner().unwrap().clone();
                    claim.state = State::Running {
                        owner,
                        turn_id: turn.clone(),
                        joined: false,
                    };
                    self.record_run_turn_started(&token.thread_id, &turn, actions);
                    self.remember_owned_turn(&token.thread_id, &turn);
                }
            }
            None => self.retry_reconcile(registration, true, actions),
        }
    }
    pub(crate) fn reconcile_timer(
        &mut self,
        token: ClaimToken,
        generation: u64,
        round: u64,
        actions: &mut Vec<Action>,
    ) {
        if self.closing {
            return;
        }
        if self
            .stops
            .late
            .iter()
            .any(|l| l.generation == generation && l.round == round && l.late_settling)
        {
            self.begin_late_settlement(generation, actions);
            return;
        }
        if !self.current(&token) {
            self.detach_reconciliations(&token.thread_id, token.claim_id, actions);
            return;
        }
        if self
            .stops
            .reconciliations
            .get(&generation)
            .is_some_and(|l| l.round == round)
        {
            self.begin_reconcile(token, generation, round, actions);
        }
    }
    fn settle_review_tracking(&mut self, registration: &mut LateStart) {
        if registration.review_pending {
            registration.review_pending = false;
            self.finish_timed_out_review(registration.review_id);
        }
    }
    fn finish_tracking(&mut self, mut registration: LateStart, actions: &mut Vec<Action>) {
        self.release_stream_slot(registration.stream_id);
        if registration.ownership_proven && registration.reply_received {
            if let Some(turn) = &registration.turn {
                let fact = (registration.target_thread.clone(), turn.clone());
                if !self.stops.retired_late_turns.contains(&fact) {
                    self.stops.retired_late_turns.push_back(fact);
                }
                if self.stops.retired_late_turns.len() > MAX_LATE {
                    self.stops.retired_late_turns.pop_front();
                }
            }
        }
        self.settle_review_tracking(&mut registration);
        if registration.wire_available {
            actions.push(Action::ForgetLate {
                tag: registration.wire_tag,
            });
        }
    }
    fn push_late(&mut self, registration: LateStart, actions: &mut Vec<Action>) {
        if !registration.wire_available && !registration.reply_received {
            self.finish_tracking(registration, actions);
            return;
        }
        self.stops.late.push_back(registration);
        self.trim_late(actions);
    }
    pub(crate) fn detach_reconciliations(
        &mut self,
        thread: &str,
        claim_id: u64,
        actions: &mut Vec<Action>,
    ) {
        let ids: Vec<_> = self
            .stops
            .reconciliations
            .iter()
            .filter(|(_, l)| l.token.thread_id == thread && l.token.claim_id == claim_id)
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            let mut reg = self.stops.reconciliations.remove(&id).unwrap();
            reg.reconciling = false;
            self.push_late(reg, actions);
        }
    }
    fn retire_joined_tracking(&mut self, thread: &str, claim_id: u64, actions: &mut Vec<Action>) {
        let ids: Vec<_> = self
            .stops
            .reconciliations
            .iter()
            .filter(|(_, l)| l.token.thread_id == thread && l.token.claim_id == claim_id)
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            let reg = self.stops.reconciliations.remove(&id).unwrap();
            self.finish_tracking(reg, actions);
        }
        let mut retained = VecDeque::new();
        while let Some(reg) = self.stops.late.pop_front() {
            if reg.token.thread_id == thread && reg.token.claim_id == claim_id {
                self.finish_tracking(reg, actions);
            } else {
                retained.push_back(reg);
            }
        }
        self.stops.late = retained;
    }
    /// 容量淘汰只结束线上等待：结果未知的 goal 写入（及其准入屏障）保留到原截止时刻，
    /// 由本地计时器到期清理；其余登记与到期丢弃同样收尾。
    pub(crate) fn evict_late_context(
        &mut self,
        context: &RequestContext,
        deadline: std::time::Instant,
        actions: &mut Vec<Action>,
    ) {
        if self.closing {
            return;
        }
        if let Operation::GoalChange { id, .. } = &context.operation {
            let id = *id;
            let kept = self.stops.changes.get_mut(&id).and_then(|change| {
                if !change.unknown {
                    return None;
                }
                change.wire_tag = None;
                Some(change.thread.clone())
            });
            if let Some(thread_id) = kept {
                actions.push(Action::Warn(format!(
                    "线程 {thread_id} 的未知目标变更 {id} 被迟到登记上限挤出，保留到原截止再清理"
                )));
                let token = self.token(&thread_id).unwrap_or(ClaimToken {
                    thread_id,
                    claim_id: 0,
                });
                actions.push(Action::ScheduleAt {
                    token,
                    kind: TimerKind::GoalChangeExpiry { id },
                    deadline,
                });
                return;
            }
        }
        self.discard_late_context(context, actions);
    }

    pub(crate) fn discard_late_context(
        &mut self,
        context: &RequestContext,
        actions: &mut Vec<Action>,
    ) {
        if self.closing {
            return;
        }
        if let Operation::GoalChange { id, .. } = &context.operation {
            self.expire_goal_change(*id, actions);
            return;
        }
        let Some(token) = &context.token else {
            self.release_stream_slot(context.stream_id);
            return;
        };
        let id = self
            .stops
            .reconciliations
            .iter()
            .find(|(_, l)| l.token == *token && l.method == context.method)
            .map(|(id, _)| *id);
        if let Some(id) = id {
            let mut reg = self.stops.reconciliations.remove(&id).unwrap();
            reg.wire_available = false;
            self.settle_review_tracking(&mut reg);
            if reg.method == "review/start" {
                if self.current(token) {
                    if let Some(key) = self.stop_key(&token.thread_id) {
                        let op = self.stops.operations.get_mut(&key).unwrap();
                        op.foreign_claim = true;
                        op.claim_id = None;
                    }
                    self.release(&token.thread_id, actions);
                }
            } else if self.current(token) {
                self.stops.reconciliations.insert(id, reg);
            }
        }
        let index = self
            .stops
            .late
            .iter()
            .position(|l| l.token == *token && l.method == context.method);
        if let Some(index) = index {
            let mut reg = self.stops.late.remove(index).unwrap();
            reg.wire_available = false;
            self.finish_tracking(reg, actions);
        } else if !self
            .stops
            .reconciliations
            .values()
            .any(|reg| reg.token == *token && reg.method == context.method)
        {
            self.release_stream_slot(context.stream_id);
        }
    }
    pub(crate) fn late_reply(
        &mut self,
        mut context: RequestContext,
        result: Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) {
        if self.closing {
            return;
        }
        self.release_stream_slot(context.stream_id);
        context.client = None;
        if self.unwrap_goal_reply(&mut context, &result, false, actions) {
            return;
        }
        if !matches!(context.operation, Operation::Stream { .. }) {
            if context.operation == Operation::Forward {
                // 普通转发已在超时出口结算；迟到回包只补服务器事实，不再交付或扣减 review 计数。
                self.learn_traffic(&context, &result, actions);
            } else {
                self.apply_reply(context, result, actions);
            }
            return;
        }
        let Some(token) = context.token.clone() else {
            return;
        };
        let found = self
            .stops
            .reconciliations
            .iter()
            .find(|(_, l)| l.token == token && l.method == context.method)
            .map(|(id, _)| *id);
        let registration = if let Some(id) = found {
            self.stops.reconciliations.remove(&id)
        } else {
            self.stops
                .late
                .iter()
                .position(|l| l.token == token && l.method == context.method)
                .and_then(|i| self.stops.late.remove(i))
        };
        let Some(mut reg) = registration else {
            return;
        };
        // RPC 已消费线上回包，此后只有本地定案/制动需要跟踪。
        reg.wire_available = false;
        reg.reply_received = true;
        if context.method == "turn/steer" {
            self.finish_tracking(reg, actions);
            return;
        }
        if self.current(&token)
            && (self.claims[&token.thread_id].request_pending || context.method == "review/start")
        {
            // 只有同代认领可走正常回包/settling；绝不按 threadId 停止后来认领。
            reg.review_pending = false; // 正常 review 回包出口负责本次计数。
            self.apply_reply(context, result, actions);
            if self.current(&token) && !self.claims[&token.thread_id].joined() {
                self.begin_stop(&token.thread_id, StopReason::Stop, false, actions);
            }
            if reg.wire_available {
                actions.push(Action::ForgetLate { tag: reg.wire_tag });
            }
            return;
        }
        if self.current(&token) {
            // 对账已落定但目标制动仍在途时，迟到回包只收尾登记，继续原停止顺序。
            self.finish_tracking(reg, actions);
            if !self.claims[&token.thread_id].joined() {
                self.begin_stop(&token.thread_id, StopReason::Stop, false, actions);
            }
            return;
        }
        self.settle_review_tracking(&mut reg);
        let Some(value) = result.as_ref().ok() else {
            self.finish_tracking(reg, actions);
            return;
        };
        let Some(turn) = response_turn(value).map(str::to_owned) else {
            self.finish_tracking(reg, actions);
            return;
        };
        if value.get("absorbedBy").is_some_and(Value::is_string) {
            self.finish_tracking(reg, actions);
            return;
        }
        reg.turn = Some(turn);
        reg.reconciling = false;
        if reg.method == "review/start" {
            let Some(child) = value.get("reviewThreadId").and_then(Value::as_str) else {
                self.finish_tracking(reg, actions);
                return;
            };
            reg.target_thread = child.into();
            reg.ownership_proven = true;
        }
        let generation = reg.generation;
        // 从处理回包这一刻开始新的有界收尾期，不能复用已经过去的线上截止。
        actions.push(Action::Schedule {
            token: reg.token.clone(),
            kind: TimerKind::LateExpiry { generation },
            delay_ms: LATE_MS,
        });
        if self.shared && !reg.ownership_proven {
            reg.late_settling = true;
            reg.round += 1;
            self.push_late(reg, actions);
            self.begin_late_settlement(generation, actions);
        } else {
            self.push_late(reg, actions);
            self.interrupt_late(generation, actions);
        }
    }
    fn begin_late_settlement(&mut self, generation: u64, actions: &mut Vec<Action>) {
        let Some(reg) = self.stops.late.iter().find(|l| l.generation == generation) else {
            return;
        };
        actions.push(Action::Request(RequestContext {
            resume_retried: false,
            stream_id: None,
            review_id: None,
            client: None,
            token: Some(reg.token.clone()),
            operation: Operation::LateSettlement {
                generation,
                round: reg.round,
            },
            method: "thread/read".into(),
            params: Some(json!({"threadId":reg.target_thread,"includeTurns":true})),
        }));
    }
    pub(crate) fn late_settlement_reply(
        &mut self,
        generation: u64,
        round: u64,
        result: Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) {
        let Some(index) = self
            .stops
            .late
            .iter()
            .position(|l| l.generation == generation && l.round == round && l.late_settling)
        else {
            return;
        };
        let Some(active) = strict_active(&result) else {
            let reg = &mut self.stops.late[index];
            reg.round += 1;
            let delay_ms = reconcile_delay(&mut reg.reconcile_delay_ms, true);
            actions.push(Action::Schedule {
                token: reg.token.clone(),
                kind: TimerKind::Reconcile {
                    generation,
                    round: reg.round,
                },
                delay_ms,
            });
            return;
        };
        let mut reg = self.stops.late.remove(index).unwrap();
        reg.reconcile_delay_ms = 0;
        // 与 settling 相同：成功回包编号与活动编号一致才取得自有证据。
        if !active.is_empty() && reg.turn.as_ref().is_some_and(|turn| active.contains(turn)) {
            reg.ownership_proven = true;
            reg.late_settling = false;
            self.push_late(reg, actions);
            self.interrupt_late(generation, actions);
        } else {
            self.finish_tracking(reg, actions);
        }
    }
    pub(crate) fn handle_late_notification(
        &mut self,
        thread: &str,
        method: &str,
        turn: &str,
        actions: &mut Vec<Action>,
    ) -> bool {
        let registered = self.stops.late.iter().any(|l| {
            l.target_thread == thread && l.turn.as_deref() == Some(turn) && !l.reconciling
        });
        if self.stops.late.iter().any(|l| {
            l.target_thread == thread && l.turn.as_deref() == Some(turn) && self.current(&l.token)
        }) {
            // 同代通知必须走认领的生命周期，交给已有 StopOp 制动和去重。
            return false;
        }
        let current = self.claims.get(thread).and_then(|c| c.turn_id()) == Some(turn);
        let retired = self
            .stops
            .retired_late_turns
            .iter()
            .any(|(t, id)| t == thread && id == turn);
        if current {
            return false;
        }
        if retired && matches!(method, "turn/started" | "turn/completed") {
            return true;
        }
        if !registered {
            return false;
        }
        if method == "turn/started" {
            self.late_started(thread, turn, actions);
        } else if method == "turn/completed" {
            let ids: Vec<_> = self
                .stops
                .late
                .iter()
                .filter(|l| l.target_thread == thread && l.turn.as_deref() == Some(turn))
                .map(|l| l.generation)
                .collect();
            for id in ids {
                self.remove_late(id, actions);
            }
        } else {
            return false;
        }
        true
    }
    pub(crate) fn late_started(&mut self, thread: &str, turn: &str, actions: &mut Vec<Action>) {
        let generations: Vec<_> = self
            .stops
            .late
            .iter()
            .filter(|l| l.target_thread == thread && l.turn.as_deref() == Some(turn))
            .map(|l| l.generation)
            .collect();
        for generation in generations {
            self.interrupt_late(generation, actions);
        }
    }

    pub(crate) fn local_goal_change_expiry(&mut self, id: u64, actions: &mut Vec<Action>) {
        // 有 wire 登记时，唯一过期入口是 RPC 的有序 LateDiscarded。
        if self
            .stops
            .changes
            .get(&id)
            .is_some_and(|change| change.wire_tag.is_none())
        {
            self.expire_goal_change(id, actions);
        }
    }

    pub(crate) fn drive_late_goals(&mut self, actions: &mut Vec<Action>) {
        self.drive_goal_restores(actions);
        if self.closing {
            return;
        }
        let ids: Vec<_> = self
            .stops
            .late
            .iter()
            .filter(|reg| !reg.late_settling)
            .map(|reg| reg.generation)
            .collect();
        for id in ids {
            self.interrupt_late(id, actions);
        }
    }

    fn begin_late_goal(&mut self, generation: u64, actions: &mut Vec<Action>) {
        let Some(index) = self
            .stops
            .late
            .iter()
            .position(|reg| reg.generation == generation)
        else {
            return;
        };
        let thread = self.stops.late[index].target_thread.clone();
        let version = self.stops.ensure_version(&thread);
        let reg = &mut self.stops.late[index];
        reg.goal_round += 1;
        reg.goal_busy = true;
        reg.goal_done = false;
        reg.goal_waiting_for_owner = false;
        actions.push(Action::Request(RequestContext {
            resume_retried: false,
            stream_id: None,
            review_id: None,
            client: None,
            token: Some(reg.token.clone()),
            operation: Operation::LateGoal {
                generation,
                round: reg.goal_round,
                version,
                write: false,
            },
            method: "thread/goal/get".into(),
            params: Some(json!({"threadId": thread})),
        }));
    }

    pub(crate) fn late_goal_reply(
        &mut self,
        generation: u64,
        round: u64,
        version: u64,
        write: bool,
        result: Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) {
        if self.closing {
            return;
        }
        let Some(index) = self
            .stops
            .late
            .iter()
            .position(|reg| reg.generation == generation && reg.goal_round == round)
        else {
            return;
        };
        let mut reg = self.stops.late.remove(index).unwrap();
        reg.goal_busy = false;
        if self.current(&reg.token) {
            let thread = reg.token.thread_id.clone();
            self.push_late(reg, actions);
            self.begin_stop(&thread, StopReason::Stop, false, actions);
            return;
        }
        if version != self.stops.version(&reg.target_thread)
            || self
                .stops
                .changes
                .values()
                .any(|change| change.thread == reg.target_thread)
        {
            if write
                && reg
                    .goal_change_id
                    .is_some_and(|id| self.stops.changes.contains_key(&id))
            {
                // 本次暂停刚超时且结果未知，已到收尾截止也必须等它落定或 RPC 迟到期限结束。
                reg.goal_done = true;
                reg.goal_version = self.stops.version(&reg.target_thread);
                self.push_late(reg, actions);
                return;
            }
            if reg.cleanup_due {
                self.finish_tracking(reg, actions);
                return;
            }
            reg.goal_done = false;
            self.push_late(reg, actions);
            return;
        }
        if write {
            if result.is_ok() {
                self.run_goal_status(&reg.target_thread, Some("paused"));
                self.record(&reg.target_thread).goal_active = false;
            } else {
                actions.push(Action::Warn(format!(
                    "迟到轮次所在的线程 {} 暂停目标失败，继续按编号中断",
                    reg.target_thread
                )));
            }
            reg.goal_version = self.stops.bump(&reg.target_thread);
            reg.goal_done = true;
        } else {
            let goal = read_goal(&result);
            if let Some(goal) = &goal {
                self.run_goal_status(
                    &reg.target_thread,
                    goal.as_ref()
                        .and_then(|g| g.get("status"))
                        .and_then(Value::as_str),
                );
            }
            let active = goal
                .as_ref()
                .and_then(Option::as_ref)
                .is_some_and(|goal| goal.get("status").and_then(Value::as_str) == Some("active"));
            if active && self.claims.contains_key(&reg.target_thread) && reg.cleanup_due {
                // 收尾期已到，不能为了旧轮次暂停后来认领的目标，也不能无限等它释放：结束旧跟踪。
                actions.push(Action::Warn(format!(
                    "迟到轮次的收尾期已到，线程 {} 已由后来认领占用且目标活跃，结束旧跟踪",
                    reg.target_thread
                )));
                self.finish_tracking(reg, actions);
                return;
            } else if active && self.claims.contains_key(&reg.target_thread) {
                // 线程已有后来认领，不能为了旧轮次暂停它的目标；待该认领释放后再处理。
                reg.goal_waiting_for_owner = true;
            } else if active {
                reg.goal_busy = true;
                reg.goal_round += 1;
                actions.push(Action::Request(RequestContext {
                    resume_retried: false,
                    stream_id: None,
                    review_id: None,
                    client: None,
                    token: Some(reg.token.clone()),
                    operation: Operation::LateGoal {
                        generation,
                        round: reg.goal_round,
                        version,
                        write: true,
                    },
                    method: "thread/goal/set".into(),
                    params: Some(json!({"threadId": reg.target_thread, "status": "paused"})),
                }));
            } else {
                if goal.is_none() {
                    actions.push(Action::Warn(format!(
                        "迟到轮次所在的线程 {} 读取目标失败，继续按编号中断",
                        reg.target_thread
                    )));
                } else {
                    self.record(&reg.target_thread).goal_active = false;
                }
                reg.goal_done = true;
                reg.goal_version = version;
            }
        }
        self.push_late(reg, actions);
    }
    fn interrupt_late(&mut self, generation: u64, actions: &mut Vec<Action>) {
        let Some(index) = self
            .stops
            .late
            .iter()
            .position(|l| l.generation == generation)
        else {
            return;
        };
        let late = &self.stops.late[index];
        if self.current(&late.token) {
            let thread = late.token.thread_id.clone();
            if !self.claims[&thread].joined() {
                self.begin_stop(&thread, StopReason::Stop, false, actions);
            }
            return;
        }
        let Some(turn) = late.turn.clone() else {
            return;
        };
        if self
            .records
            .get(&late.target_thread)
            .is_some_and(|r| r.ended_turns.contains(&turn))
        {
            let reg = self.stops.late.remove(index).unwrap();
            self.finish_tracking(reg, actions);
            return;
        }
        if !late.ownership_proven || late.method == "turn/steer" {
            return;
        }
        if self.shared
            && self.claims.get(&late.target_thread).is_some_and(|claim| {
                claim.claim_id != late.token.claim_id && claim.turn_id() == Some(&turn)
            })
        {
            // 新认领正在接收同一编号时，旧请求可能被吸收；不得把它的编号当作旧请求的停止权限。
            let reg = self.stops.late.remove(index).unwrap();
            self.finish_tracking(reg, actions);
            actions.push(Action::Warn(
                "迟到请求轮次与新的共享认领重合，保留新认领且不中断".into(),
            ));
            return;
        }

        if late.interrupting {
            return;
        }
        let thread = late.target_thread.clone();
        if late.goal_waiting_for_owner && self.claims.contains_key(&thread) {
            if late.cleanup_due && !late.goal_busy {
                // 收尾期已到仍要等后来认领释放：不再无限等待，结束旧跟踪。
                let reg = self.stops.late.remove(index).unwrap();
                actions.push(Action::Warn(format!(
                    "迟到轮次的收尾期已到，线程 {} 仍由后来认领占用，结束旧跟踪",
                    reg.target_thread
                )));
                self.finish_tracking(reg, actions);
            }
            return;
        }
        if self
            .stops
            .changes
            .values()
            .any(|change| change.thread == thread)
        {
            if late.cleanup_due
                && !late.goal_busy
                && !late
                    .goal_change_id
                    .is_some_and(|id| self.stops.changes.contains_key(&id))
            {
                let reg = self.stops.late.remove(index).unwrap();
                self.finish_tracking(reg, actions);
            }
            return;
        }
        if late.goal_busy {
            return;
        }
        if !late.goal_done
            || (!late.cleanup_due && late.goal_version != self.stops.version(&thread))
        {
            self.begin_late_goal(generation, actions);
            return;
        }
        actions.push(Action::Warn(format!(
            "超时请求的迟到轮次 {} 按编号中断，不修改线程 {} 的当前认领",
            turn, late.target_thread
        )));
        actions.push(Action::Request(RequestContext {
            resume_retried: false,
            stream_id: None,
            review_id: None,
            client: None,
            token: Some(late.token.clone()),
            operation: Operation::LateInterrupt { generation },
            method: "turn/interrupt".into(),
            params: Some(json!({"threadId":late.target_thread,"turnId":turn})),
        }));
        self.stops.late[index].interrupting = true;
        if self.stops.late[index].cleanup_due {
            let reg = self.stops.late.remove(index).unwrap();
            self.finish_tracking(reg, actions);
        }
    }
    pub(crate) fn late_interrupt_reply(
        &mut self,
        generation: u64,
        result: Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) {
        if self.closing {
            return;
        }
        if result.is_err() {
            actions.push(Action::Warn(
                "迟到轮次的按编号中断失败，保留登记至完成或过期".into(),
            ));
        }
        if result.is_ok() {
            if let Some(index) = self
                .stops
                .late
                .iter()
                .position(|l| l.generation == generation)
            {
                let retired = self.stops.late.remove(index).unwrap();
                self.finish_tracking(retired.clone(), actions);
            }
        }
    }
    fn remove_late(&mut self, generation: u64, actions: &mut Vec<Action>) {
        if let Some(index) = self
            .stops
            .late
            .iter()
            .position(|l| l.generation == generation)
        {
            let reg = self.stops.late.remove(index).unwrap();
            self.finish_tracking(reg, actions);
        }
    }
    pub(crate) fn expire_late(&mut self, generation: u64, actions: &mut Vec<Action>) {
        if self.closing {
            return;
        }
        if let Some(reg) = self
            .stops
            .late
            .iter()
            .find(|l| l.generation == generation)
            .cloned()
        {
            // 旧的独立计时器不能覆盖 RPC 在截止前已接收、尚未转交 core 的回包。
            if !reg.reply_received {
                return;
            }
            let goal_pending = reg.goal_busy
                || reg
                    .goal_change_id
                    .is_some_and(|id| self.stops.changes.contains_key(&id));
            if goal_pending {
                if let Some(reg) = self
                    .stops
                    .late
                    .iter_mut()
                    .find(|reg| reg.generation == generation)
                {
                    reg.cleanup_due = true;
                }
                // 收尾期限已到，但先让受 30 秒 RPC/10 分钟 unknown 限制的制动落定，随后只尝试一次中断。
                return;
            }
            self.remove_late(generation, actions);
            actions.push(Action::Warn(format!(
                "迟到回包登记 {} 已满十分钟，丢弃",
                reg.generation
            )));
        }
    }
    fn trim_late(&mut self, actions: &mut Vec<Action>) {
        while self.stops.late.len() > MAX_LATE {
            let reg = self.stops.late.pop_front().unwrap();
            self.finish_tracking(reg, actions);
            actions.push(Action::Warn("迟到回包登记超过 64 条，丢弃最旧登记".into()));
        }
    }
}

fn stop_request(
    id: &str,
    op: &StopOp,
    round: u64,
    kind: StopRequest,
    method: &str,
    params: Value,
    actions: &mut Vec<Action>,
) {
    actions.push(Action::Request(RequestContext {
        resume_retried: false,
        stream_id: None,
        review_id: None,
        client: None,
        token: Some(op.token()),
        operation: Operation::Stop(op.tag(id, round), kind),
        method: method.into(),
        params: Some(params),
    }));
}
fn stop_error(
    request: ClientRequest,
    code: i64,
    message: &str,
    data: Option<Value>,
    actions: &mut Vec<Action>,
) {
    actions.push(Action::Send {
        conn: request.conn,
        message: proto::response_err(request.id, proto::error_object(code, message, data)),
    });
}
fn actions_deliver(c: &ClientRequest, result: Result<Value, Value>, actions: &mut Vec<Action>) {
    let message = match result {
        Ok(v) => proto::response_ok(c.id.clone(), v),
        Err(e) => proto::response_err(c.id.clone(), e),
    };
    actions.push(Action::Send {
        conn: c.conn,
        message,
    });
}
fn response_turn(value: &Value) -> Option<&str> {
    value
        .get("turn")
        .and_then(|t| t.get("id"))
        .and_then(Value::as_str)
        .or_else(|| value.get("turnId").and_then(Value::as_str))
}
fn retarget(error: &Value) -> Option<&str> {
    let words: Vec<_> = error.get("message")?.as_str()?.split_whitespace().collect();
    words.windows(8).find_map(|w| {
        (w[..4] == ["expected", "active", "turn", "id"] && w[5..7] == ["but", "found"])
            .then_some(w[7])
    })
}
/// 上游 goals.ts 38–45 行：仅这四种错误视为无 goal，其余错误保持未知。
fn read_goal(result: &Result<Value, Value>) -> Option<Option<Value>> {
    match result {
        Ok(v) => match v.get("goal") {
            Some(Value::Null) | None => Some(None),
            Some(g) if g.get("status").is_some_and(Value::is_string) => Some(Some(g.clone())),
            _ => None,
        },
        Err(e) => {
            let m = e
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_lowercase();
            [
                "goals feature is disabled",
                "method not found",
                "unknown method",
                "sqlite state db unavailable",
            ]
            .iter()
            .any(|p| m.contains(p))
            .then_some(None)
        }
    }
}
/// 核实与超时对账采用从严解析，字段缺失或任意 turn 形状错误均为未知。
pub(crate) fn strict_active(result: &Result<Value, Value>) -> Option<Vec<String>> {
    let turns = result
        .as_ref()
        .ok()?
        .get("thread")?
        .get("turns")?
        .as_array()?;
    let mut active = Vec::new();
    for turn in turns {
        let id = turn.get("id")?.as_str()?;
        let status = turn.get("status")?.as_str()?;
        if id.is_empty() {
            return None;
        }
        if status == "inProgress" {
            active.push(id.into());
        }
    }
    Some(active)
}

fn claim_settling(machine: &Machine, token: &ClaimToken) -> bool {
    machine
        .claims
        .get(&token.thread_id)
        .is_some_and(|c| c.claim_id == token.claim_id && c.settling())
}

/// 对账失败按逐项指数退避，读到完整结果后清零；固定的停止核实不调用此函数。
fn reconcile_delay(previous: &mut u64, failed: bool) -> u64 {
    if failed {
        *previous = if *previous == 0 {
            VERIFY_MS
        } else {
            previous.saturating_mul(2).min(MAX_RECONCILE_DELAY_MS)
        };
        *previous
    } else {
        *previous = 0;
        VERIFY_MS
    }
}
