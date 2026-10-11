//! 续跑的条件请求（`turn/start` 带 `cxbContinue`）、goal 恢复操作与定案通知（详细设计 2.3 节「续跑的条件请求」、
//! 迁移表 #2c、#2d、#2d'、#2e、#20 的预检分支、#28d，3.6 节「中断后接着干」，S1-评审第 16、17 节，
//! S3-计划第 6 节）。【乙路实现】
//!
//! 调用点已由骨架插好（乙路实现函数体，不改调用点；确需改动在交付说明里列出）：
//! - [`Machine::continue_request`]：claim::client_request 收到带 `cxbContinue` 的 `turn/start`（任何状态，含关闭中）。
//! - [`Machine::continue_reply`]：`Operation::ContinueResume`、`Operation::ContinueRead` 的回包（apply_reply）。
//! - [`Machine::continue_precheck_driver`]：commit_notification 在驱动 started、completed 之前；返回 true 表示
//!   该通知已按预检规则处理（只转发、不驱动 #4–#17），调用方不再驱动认领。
//! - [`Machine::continue_cancel`]：预检中的 owner 断开（#20）、关闭第 4 步、预检中收到 stop（#28d，stop/mod.rs）。
//! - [`Machine::maybe_restore_goal`]：本认领观察到 turn/started 之后（claim::started），共享服务器仅由回放里的真实 started 触发；自己判断 #2e 的全部条件，重复调用必须无副作用。
//! - [`Machine::goal_restore_reply`]：`Operation::GoalRestore` 的回包（goal 写入经 stop 模块统一登记、解包后到这里）。
//! - [`Machine::notify_run_settled`]：所有认领定案的那一刻（settle、`broker/joined`），先于回放 held。
//!
//! 检查顺序固定为：纯内存前提 → 服务器核实 → 必要时恢复订阅 → 再核实 → 开工前复查。
//! 冷 resume 可能触发 active goal 自动开工，因此必须先核实服务器已停。

use super::status_read::{StatusCaller, StatusQuery, StatusResult};
use super::*;
use crate::runs::{RunLookup, RunState};

/// 续跑失败时 `data.reason` 的取值（2.3 节）。-32000：
pub const UNKNOWN_RUN: &str = "unknownRun";
pub const RUN_MISMATCH: &str = "runMismatch";
pub const NOT_LATEST_RUN: &str = "notLatestRun";
pub const NOT_RESUMABLE: &str = "notResumable";
pub const RESUME_CHECK_FAILED: &str = "resumeCheckFailed";
pub const CONTINUE_CANCELLED: &str = "continueCancelled";
pub const SHUTTING_DOWN: &str = "shuttingDown";
/// -32001：
pub const THREAD_BUSY: &str = "threadBusy";
pub const STOPPING: &str = "stopping";

/// 预检被取消的原因（#2d'）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelCause {
    /// owner 断开（#20）：什么都不发。
    Disconnected,
    /// 预检中收到 stop（#28d）：原 turn/start 回 `continueCancelled`。
    Stop,
    /// 关闭（第 5 节第 4 步）：原 turn/start 回 `shuttingDown`。
    Shutdown,
    /// 预检期间外部 turn 开始（#2c）：原 turn/start 回 -32001 `threadBusy`。
    ExternalTurn,
}

/// 预检的 goal 核实读（`thread/goal/get`）；回包按认领 token 与 round 核对。
/// 线程是否在活动改由状态读取层的 Busy 查询核实（`StatusCaller::ContinueCheck`），回到
/// [`Machine::continue_check_done`]。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContinueRead {
    Goal { round: u64 },
}

/// goal 恢复操作（#2e）发出的请求。乙路可增加字段或变体（只增不改）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GoalRestoreStep {
    /// 先读当前 goal。
    Get { run_id: RunId },
    /// 版本变化后的重读，轮次隔离迟到回包。
    GetAgain { run_id: RunId, round: u64 },
    /// 五项核对通过后设回 active。
    SetActive { run_id: RunId },
    /// set active 回来时线程上已没有认领，补发暂停。
    SetPaused { run_id: RunId },
}

/// 每轮最多重读两次；所有请求仍由 RPC 层的期限保证后继事件。
const MAX_REREADS: u8 = 2;
const MAX_GOAL_RESTORES: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
enum CheckPhase {
    Resume,
    Read,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CheckProgress {
    claim_id: u64,
    phase: CheckPhase,
    round: u64,
    retries: u8,
    version: u64,
    thread_done: bool,
    goal_done: bool,
    resumed: bool,
}

/// 每个预检一项，随 #2d 或 #2d′ 删除，受开工名额的 64 项上限约束。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Continues {
    pending: HashMap<String, CheckProgress>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RestorePhase {
    Reading,
    Writing,
    Unknown,
    Pausing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RestoreProgress {
    token: ClaimToken,
    turn: String,
    phase: RestorePhase,
    round: u64,
    retries: u8,
    version: u64,
    change_id: Option<u64>,
    active_change: Option<u64>,
    /// 原 active 写入的版本，不随未知阶段观察到的新事实或补暂停改变。
    active_version: Option<u64>,
    pause_again: bool,
}

/// 恢复操作最多 64 项，结束或关闭删除；定案标记仅随仍在表中的认领保留。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GoalRestores {
    pending: HashMap<RunId, RestoreProgress>,
    settled: HashMap<String, u64>,
}

pub(crate) fn continue_field(params: &Option<Value>) -> Option<&Value> {
    params.as_ref().and_then(|p| p.get("cxbContinue"))
}

fn fail(request: &ClientRequest, reason: &str, actions: &mut Vec<Action>) {
    let code = if matches!(reason, THREAD_BUSY | STOPPING | "lifecyclePending") {
        code::THREAD_BUSY
    } else {
        code::GENERAL_ERROR
    };
    actions.push(Action::Send {
        conn: request.conn,
        message: proto::response_err(
            request.id.clone(),
            proto::error_object(code, "续跑条件未满足", Some(json!({"reason": reason}))),
        ),
    });
}

/// 缺字段与未知状态均不当作已停止；功能不可用沿用设计指定的上游四种错误。
fn goal_status(result: &Result<Value, Value>) -> Option<Option<&str>> {
    match result {
        Ok(value) => match value.get("goal")? {
            Value::Null => Some(None),
            goal => match goal.get("status")?.as_str()? {
                status @ ("active" | "paused" | "blocked" | "complete" | "usageLimited"
                | "budgetLimited") => Some(Some(status)),
                _ => None,
            },
        },
        Err(error) => {
            let message = error.get("message")?.as_str()?.to_lowercase();
            [
                "goals feature is disabled",
                "method not found",
                "unknown method",
                "sqlite state db unavailable",
            ]
            .iter()
            .any(|text| message.contains(text))
            .then_some(None)
        }
    }
}

/// 墓碑只证明所属线程，不把已淘汰记录当成可续跑运行。
pub(super) fn lookup_error(lookup: &RunLookup, thread: &str) -> Option<&'static str> {
    match lookup {
        RunLookup::Live => None,
        RunLookup::Retired { thread_id } if thread_id == thread => Some(NOT_LATEST_RUN),
        RunLookup::Retired { .. } => Some(RUN_MISMATCH),
        RunLookup::Unknown => Some(UNKNOWN_RUN),
    }
}

impl Machine {
    pub(crate) fn goal_restoration_pending(&self, thread: &str) -> bool {
        self.claims.get(thread).is_some_and(|c| {
            c.resume_goal
                || c.run_id
                    .as_ref()
                    .is_some_and(|run| self.goal_restores.pending.contains_key(run))
        })
    }
    pub(crate) fn continue_request(
        &mut self,
        request: ClientRequest,
        run_id: Option<RunId>,
        actions: &mut Vec<Action>,
    ) {
        let parsed = (|| {
            let thread = thread_id(&request.params)?.to_owned();
            let value = continue_field(&request.params)?.as_object()?;
            let from_run = value.get("fromRunId")?.as_str()?.to_owned();
            let resume_goal = match value.get("resumeGoal") {
                None => true,
                Some(value) => value.as_bool()?,
            };
            Some((
                thread,
                ContinueCheck {
                    from_run,
                    resume_goal,
                    run_id: run_id?,
                },
            ))
        })();
        let Some((thread, check)) = parsed else {
            send_error(
                &request,
                code::INVALID_PARAMS,
                "续跑需要字符串 threadId、fromRunId 和可选布尔值 resumeGoal",
                actions,
            );
            return;
        };
        if self.closing || self.quiescing {
            fail(&request, SHUTTING_DOWN, actions);
            return;
        }
        if self.reject_stream_admission(&request, actions) {
            return;
        }
        if self.claims.contains_key(&thread) || self.stop_busy(&thread) {
            let reason = if self
                .stops
                .operations
                .values()
                .any(|op| op.run_id.as_deref() == Some(&check.from_run))
            {
                STOPPING
            } else {
                THREAD_BUSY
            };
            fail(&request, reason, actions);
            return;
        }
        let stream_id = self.reserve_stream_slot();
        let claim_id = self.allocate_claim_id();
        self.record(&thread).last_claim_id = claim_id;
        let subscribed = self.records[&thread].subscribed;
        self.claims.insert(
            thread.clone(),
            Claim {
                stream_id: Some(stream_id),
                owned_ended: Default::default(),
                owned_turns: Default::default(),
                claim_id,
                state: State::Starting {
                    owner: Owner::Client(request.conn),
                    req: ReqKind::TurnStart,
                    turn_id: None,
                    cont: Some(check),
                    settling: false,
                    undecided: None,
                    submitted_at: 0,
                    held: Vec::new(),
                },
                run_id: None,
                resume_goal: false,
                request_pending: true,
                owner_gone: false,
                submitted_input: input_texts(&request.params),
                pending_start: Some(request),
                held_requests: Vec::new(),
                held_deltas: 0,
                held_bytes: 0,
                held_dropped_items: 0,
                overflowed: false,
                settlement_reply: None,
                pending_detach: None,
            },
        );
        self.continues.pending.insert(
            thread.clone(),
            CheckProgress {
                claim_id,
                phase: CheckPhase::Read,
                round: 0,
                retries: 0,
                version: 0,
                thread_done: false,
                goal_done: false,
                resumed: subscribed,
            },
        );
        // continue_read 先检查纯内存前提，通过后才发核实读；resume 只能由核实成功触发。
        self.continue_read(&thread, false, actions);
    }

    fn continue_condition(&self, thread: &str, from: &str) -> Result<(), &'static str> {
        if let Some(reason) = lookup_error(&self.run_lookup(from), thread) {
            return Err(reason);
        }
        let run = self.runs.runs.get(from).ok_or(UNKNOWN_RUN)?;
        if run.thread_id != thread {
            return Err(RUN_MISMATCH);
        }
        if self.latest_run(thread).is_none_or(|run| run.run_id != from) {
            return Err(NOT_LATEST_RUN);
        }
        if self.stop_busy(thread)
            || run.state == RunState::Stopping
            || self
                .stops
                .changes
                .values()
                .any(|change| change.thread == thread)
        {
            return Err(STOPPING);
        }
        if run.joined
            || run.review_thread_id.is_some()
            || run
                .meta
                .as_ref()
                .and_then(|meta| meta.get("source"))
                .and_then(Value::as_str)
                == Some("review")
            || !(run.state == RunState::Lost
                || run.state == RunState::Ended && run.interrupt_reason.is_some())
        {
            return Err(NOT_RESUMABLE);
        }
        Ok(())
    }

    fn continue_resume(&mut self, thread: &str, actions: &mut Vec<Action>) {
        if self.closing || self.quiescing {
            self.continue_cancel(thread, CancelCause::Shutdown, actions);
            return;
        }
        self.continues
            .pending
            .get_mut(thread)
            .expect("预检已登记")
            .phase = CheckPhase::Resume;
        actions.push(Action::Request(RequestContext {
            resume_retried: false,
            stream_id: None,
            review_id: None,
            client: None,
            token: self.token(thread),
            operation: Operation::ContinueResume,
            method: "thread/resume".into(),
            params: Some(self.resume_params(thread, json!({"threadId":thread}))),
        }));
    }

    fn continue_read(&mut self, thread: &str, retry: bool, actions: &mut Vec<Action>) {
        if self.closing || self.quiescing {
            self.continue_cancel(thread, CancelCause::Shutdown, actions);
            return;
        }
        let Some(check) = self
            .claims
            .get(thread)
            .and_then(|claim| match &claim.state {
                State::Starting { cont, .. } => cont.clone(),
                _ => None,
            })
        else {
            return;
        };
        if let Err(reason) = self.continue_condition(thread, &check.from_run) {
            self.continue_fail(thread, Some(reason), actions);
            return;
        }
        let version = self.goal_fact_version(thread);
        let progress = self.continues.pending.get_mut(thread).expect("预检已登记");
        if retry && progress.retries >= MAX_REREADS {
            self.continue_fail(thread, Some(RESUME_CHECK_FAILED), actions);
            return;
        }
        progress.retries = if retry { progress.retries + 1 } else { 0 };
        progress.round += 1;
        progress.version = version;
        progress.phase = CheckPhase::Read;
        progress.thread_done = false;
        progress.goal_done = false;
        let round = progress.round;
        // H2：线程是否在活动只读元数据（Busy），不再发 `includeTurns:true` 的全量读。
        let token = self.token(thread);
        self.begin_status_read(
            thread,
            token.clone(),
            StatusQuery::Busy,
            StatusCaller::ContinueCheck { round },
            actions,
        );
        actions.push(Action::Request(RequestContext {
            resume_retried: false,
            stream_id: None,
            review_id: None,
            client: None,
            token,
            operation: Operation::ContinueRead(ContinueRead::Goal { round }),
            method: "thread/goal/get".into(),
            params: Some(json!({"threadId":thread})),
        }));
    }

    pub(crate) fn continue_resume_current(&self, context: &RequestContext) -> bool {
        context.token.as_ref().is_some_and(|token| {
            self.current(token)
                && self
                    .claims
                    .get(&token.thread_id)
                    .is_some_and(Claim::precheck)
                && self
                    .continues
                    .pending
                    .get(&token.thread_id)
                    .is_some_and(|progress| {
                        progress.claim_id == token.claim_id && progress.phase == CheckPhase::Resume
                    })
        })
    }

    pub(crate) fn continue_reply(
        &mut self,
        context: &RequestContext,
        result: Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) {
        let Some(token) = &context.token else {
            return;
        };
        let thread = token.thread_id.as_str();
        let current = self.current(token) && self.claims.get(thread).is_some_and(Claim::precheck);
        let Some(progress) = self
            .continues
            .pending
            .get(thread)
            .filter(|p| current && p.claim_id == token.claim_id)
            .cloned()
        else {
            if matches!(context.operation, Operation::ContinueResume)
                && result.is_ok()
                && !self.claims.contains_key(thread)
            {
                if let Some(record) = self.records.get(thread) {
                    self.schedule_unsubscribe(
                        ClaimToken {
                            thread_id: thread.into(),
                            claim_id: record.last_claim_id,
                        },
                        actions,
                    );
                }
            }
            return;
        };
        if self.closing || self.quiescing {
            self.continue_cancel(thread, CancelCause::Shutdown, actions);
            return;
        }
        match &context.operation {
            Operation::ContinueResume if progress.phase == CheckPhase::Resume => {
                if result.is_err() {
                    self.continue_fail(thread, Some(RESUME_CHECK_FAILED), actions);
                    return;
                }
                self.continues.pending.get_mut(thread).unwrap().resumed = true;
                self.continue_read(thread, false, actions);
            }
            Operation::ContinueRead(ContinueRead::Goal { round })
                if progress.phase == CheckPhase::Read =>
            {
                if *round != progress.round || progress.goal_done {
                    return;
                }
                if self.stops.versions.get(thread).copied().unwrap_or(0) != progress.version {
                    self.continue_read(thread, true, actions);
                    return;
                }
                let reason = match goal_status(&result) {
                    Some(Some("active")) => Some(STOPPING),
                    Some(_) => None,
                    None => Some(RESUME_CHECK_FAILED),
                };
                self.learn_traffic(context, &result, actions);
                self.continue_checked(thread, true, reason, actions);
            }
            _ => {}
        }
    }

    /// 一份核实读（线程 Busy 或 goal）已按本轮版本得出结论：失败即定案，否则两份都齐后
    /// 恢复订阅（恢复前预检）或开工（恢复后预检）。
    fn continue_checked(
        &mut self,
        thread: &str,
        goal: bool,
        reason: Option<&str>,
        actions: &mut Vec<Action>,
    ) {
        if let Some(reason) = reason {
            self.continue_fail(thread, Some(reason), actions);
            return;
        }
        let version = self.stops.versions.get(thread).copied().unwrap_or(0);
        let progress = self.continues.pending.get_mut(thread).unwrap();
        // 自己刚学习的 goal 事实不使同一轮另一份快照失效。
        progress.version = version;
        if goal {
            progress.goal_done = true;
        } else {
            progress.thread_done = true;
        }
        if progress.thread_done && progress.goal_done {
            if !progress.resumed {
                self.continue_resume(thread, actions);
            } else {
                self.continue_commit(thread, actions);
            }
        }
    }

    fn continue_commit(&mut self, thread: &str, actions: &mut Vec<Action>) {
        if let Some(error) = self.lifecycle_error(thread) {
            self.continue_fail_error(thread, error, actions);
            return;
        }
        let Some(progress) = self.continues.pending.get(thread) else {
            return;
        };
        let Some(claim) = self
            .claims
            .get(thread)
            .filter(|claim| claim.claim_id == progress.claim_id && !claim.owner_gone)
        else {
            return;
        };
        let State::Starting {
            cont: Some(check), ..
        } = &claim.state
        else {
            return;
        };
        let check = check.clone();
        let reason = if self.closing || self.quiescing {
            Some(SHUTTING_DOWN)
        } else {
            self.continue_condition(thread, &check.from_run).err()
        };
        if let Some(reason) = reason {
            self.continue_fail(thread, Some(reason), actions);
            return;
        }
        self.continues.pending.remove(thread);
        let claim = self.claims.get_mut(thread).unwrap();
        claim.run_id = Some(check.run_id.clone());
        claim.resume_goal = check.resume_goal;
        if let State::Starting { cont, settling, .. } = &mut claim.state {
            *cont = None;
            *settling = self.shared;
        }
        let request = claim.pending_start.as_mut().expect("预检保留原请求");
        if let Some(object) = request.params.as_mut().and_then(Value::as_object_mut) {
            object.remove("cxbContinue");
        }
        let request = request.clone();
        let input = claim.submitted_input.join("\n");
        self.run_opened(
            RunOpen {
                thread_id: thread.into(),
                run_id: check.run_id.clone(),
                conn: Some(request.conn),
                kind: ReqKind::TurnStart,
                joined: false,
                input,
                continued_from: Some(check.from_run),
            },
            actions,
        );
        self.send_run_started(thread, &check.run_id, &request, actions);
        self.submit_stream(thread, actions);
    }

    fn continue_fail(&mut self, thread: &str, reason: Option<&str>, actions: &mut Vec<Action>) {
        let error = reason.map(|reason| {
            proto::error_object(
                if matches!(reason, THREAD_BUSY | STOPPING | "lifecyclePending") {
                    code::THREAD_BUSY
                } else {
                    code::GENERAL_ERROR
                },
                "续跑条件未满足",
                Some(json!({"reason": reason})),
            )
        });
        self.continue_fail_value(thread, error, actions);
    }

    fn continue_fail_error(&mut self, thread: &str, error: Value, actions: &mut Vec<Action>) {
        self.continue_fail_value(thread, Some(error), actions);
    }

    fn continue_fail_value(
        &mut self,
        thread: &str,
        error: Option<Value>,
        actions: &mut Vec<Action>,
    ) {
        let Some(progress) = self.continues.pending.remove(thread) else {
            return;
        };
        let Some(claim) = self
            .claims
            .get(thread)
            .filter(|claim| claim.claim_id == progress.claim_id && claim.run_id.is_none())
        else {
            return;
        };
        if let (Some(error), Some(request)) = (error, &claim.pending_start) {
            actions.push(Action::Send {
                conn: request.conn,
                message: proto::response_err(request.id.clone(), error),
            });
        }
        // 保留 pending_start 到统一释放出口，名额只由该出口结算；恢复旧 last_run，预检不覆盖历史。
        let last_run = self
            .records
            .get(thread)
            .and_then(|record| record.last_run.clone());
        self.release_with_kind(thread, ReleaseKind::StartFailed, actions);
        self.record(thread).last_run = last_run;
    }

    pub(crate) fn continue_precheck_driver(
        &mut self,
        thread_id: &str,
        _event: &HeldNotification,
        _actions: &mut Vec<Action>,
    ) -> bool {
        self.claims.get(thread_id).is_some_and(Claim::precheck)
    }

    pub(crate) fn continue_cancel(
        &mut self,
        thread_id: &str,
        cause: CancelCause,
        actions: &mut Vec<Action>,
    ) {
        let reason = match cause {
            CancelCause::Disconnected => None,
            CancelCause::Stop => Some(CONTINUE_CANCELLED),
            CancelCause::Shutdown => Some(SHUTTING_DOWN),
            CancelCause::ExternalTurn => Some(THREAD_BUSY),
        };
        self.continue_fail(thread_id, reason, actions);
    }

    pub(crate) fn maybe_restore_goal(&mut self, thread: &str, actions: &mut Vec<Action>) {
        let Some(claim) = self.claims.get(thread) else {
            return;
        };
        if self.closing
            || self.lifecycle_error(thread).is_some()
            || self.quiescing
            || !claim.resume_goal
            || claim.settling()
            || claim.joined()
            || claim.precheck()
            || claim.owner_gone
            || self.stop_busy(thread)
        {
            return;
        }
        let Some(turn) = claim
            .turn_id()
            .filter(|turn| claim.owned_turns.contains(*turn))
            .map(str::to_owned)
        else {
            return;
        };
        let Some(run) = claim.run_id.clone() else {
            return;
        };
        if self.goal_restores.pending.contains_key(&run) {
            return;
        }
        let token = self.token(thread).unwrap();
        // 首次真实 started 即消费恢复意图，后续 started 不重复恢复。
        self.claims.get_mut(thread).unwrap().resume_goal = false;
        if self.goal_restores.pending.len() >= MAX_GOAL_RESTORES {
            actions.push(Action::Warn("目标恢复登记已达 64 项，放弃本次恢复".into()));
            return;
        }
        let version = self.goal_fact_version(thread);
        self.goal_restores.pending.insert(
            run.clone(),
            RestoreProgress {
                token,
                turn,
                phase: RestorePhase::Reading,
                round: 0,
                retries: 0,
                version,
                change_id: None,
                active_change: None,
                active_version: None,
                pause_again: false,
            },
        );
        self.restore_request(
            &run,
            GoalRestoreStep::Get {
                run_id: run.clone(),
            },
            actions,
        );
    }

    /// 已发出的 active 写入（含结果未知）也需要由关闭第 3 步暂停。
    pub(super) fn restore_active_pending(&self, thread: &str) -> bool {
        self.goal_restores.pending.values().any(|progress| {
            progress.token.thread_id == thread
                && progress
                    .active_change
                    .is_some_and(|id| self.stops.changes.contains_key(&id))
        })
    }

    fn restore_owned(&self, progress: &RestoreProgress) -> bool {
        let thread = &progress.token.thread_id;
        !self.closing
            && self.lifecycle_error(thread).is_none()
            && !self.quiescing
            && self.current(&progress.token)
            && self.claims.get(thread).is_some_and(|claim| {
                !claim.joined()
                    && !claim.settling()
                    && !claim.precheck()
                    && !claim.owner_gone
                    && claim.turn_id() == Some(progress.turn.as_str())
                    && !self
                        .records
                        .get(thread)
                        .is_some_and(|record| record.ended_turns.contains(&progress.turn))
            })
            && !self.stop_busy(thread)
    }

    fn restore_request(&self, run: &str, step: GoalRestoreStep, actions: &mut Vec<Action>) {
        if (self.closing || self.quiescing) && !matches!(step, GoalRestoreStep::SetPaused { .. }) {
            return;
        }
        let progress = &self.goal_restores.pending[run];
        let thread = &progress.token.thread_id;
        let status = match step {
            GoalRestoreStep::SetActive { .. } => Some("active"),
            GoalRestoreStep::SetPaused { .. } => Some("paused"),
            _ => None,
        };
        actions.push(Action::Request(RequestContext {
            resume_retried: false,
            stream_id: None,
            review_id: None,
            client: None,
            token: Some(progress.token.clone()),
            operation: Operation::GoalRestore(step),
            method: if status.is_some() {
                "thread/goal/set"
            } else {
                "thread/goal/get"
            }
            .into(),
            params: Some(match status {
                Some(status) => json!({"threadId":thread,"status":status}),
                None => json!({"threadId":thread}),
            }),
        }));
    }

    pub(crate) fn goal_restore_reply(
        &mut self,
        context: &RequestContext,
        step: GoalRestoreStep,
        result: Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) {
        let (run, round) = match &step {
            GoalRestoreStep::Get { run_id } => (run_id.clone(), 0),
            GoalRestoreStep::GetAgain { run_id, round } => (run_id.clone(), *round),
            GoalRestoreStep::SetActive { run_id } | GoalRestoreStep::SetPaused { run_id } => {
                (run_id.clone(), 0)
            }
        };
        let Some(progress) = self
            .goal_restores
            .pending
            .get(&run)
            .filter(|p| context.token.as_ref() == Some(&p.token))
            .cloned()
        else {
            return;
        };
        let thread = progress.token.thread_id.as_str();
        match step {
            GoalRestoreStep::Get { .. } | GoalRestoreStep::GetAgain { .. } => {
                if progress.phase != RestorePhase::Reading || round != progress.round {
                    return;
                }
                if !self.restore_owned(&progress) {
                    self.goal_restores.pending.remove(&run);
                    return;
                }
                if self.stops.versions.get(thread).copied().unwrap_or(0) != progress.version {
                    if progress.retries >= MAX_REREADS {
                        self.goal_restores.pending.remove(&run);
                        actions.push(Action::Warn("目标事实持续变化，放弃本次恢复".into()));
                        return;
                    }
                    let version = self.goal_fact_version(thread);
                    let progress = self.goal_restores.pending.get_mut(&run).unwrap();
                    progress.retries += 1;
                    progress.round += 1;
                    progress.version = version;
                    let round = progress.round;
                    self.restore_request(
                        &run,
                        GoalRestoreStep::GetAgain {
                            run_id: run.clone(),
                            round,
                        },
                        actions,
                    );
                    return;
                }
                let paused = goal_status(&result) == Some(Some("paused"));
                self.learn_traffic(context, &result, actions);
                if !paused
                    || !self.restore_owned(&progress)
                    || self
                        .stops
                        .changes
                        .values()
                        .any(|change| change.thread == thread)
                {
                    self.goal_restores.pending.remove(&run);
                    return;
                }
                self.goal_restores.pending.get_mut(&run).unwrap().phase = RestorePhase::Writing;
                self.restore_request(
                    &run,
                    GoalRestoreStep::SetActive {
                        run_id: run.clone(),
                    },
                    actions,
                );
            }
            GoalRestoreStep::SetActive { .. } => {
                if !matches!(
                    progress.phase,
                    RestorePhase::Writing | RestorePhase::Unknown | RestorePhase::Pausing
                ) {
                    return;
                }
                if progress
                    .active_change
                    .and_then(|id| self.stops.changes.get(&id))
                    .is_some_and(|change| change.unknown)
                {
                    self.goal_restores.pending.get_mut(&run).unwrap().phase = RestorePhase::Unknown;
                    return;
                }
                if self.claims.contains_key(thread) && !self.current(&progress.token) {
                    // 与 M4 的 GoalChange 规则一致：旧 active 仍撤销制动，旧 inactive 不覆盖新事实。
                    let version = self.stops.versions.get(thread).copied().unwrap_or(0);
                    if progress.active_version == Some(version)
                        || goal_status(&result) == Some(Some("active"))
                    {
                        self.learn_traffic(context, &result, actions);
                    }
                    self.goal_restores.pending.remove(&run);
                    actions.push(Action::Warn(
                        "目标恢复回包到达时认领已换代，按版本更新线程事实，不替旧运行暂停新认领"
                            .into(),
                    ));
                    return;
                }
                self.learn_traffic(context, &result, actions);
                if progress.phase == RestorePhase::Pausing {
                    if result.is_ok() {
                        self.goal_restores
                            .pending
                            .get_mut(&run)
                            .unwrap()
                            .pause_again = true;
                    }
                    return;
                }
                if result.is_ok() && !self.claims.contains_key(thread) {
                    self.restore_pause(&run, actions);
                } else {
                    self.goal_restores.pending.remove(&run);
                }
            }
            GoalRestoreStep::SetPaused { .. } => {
                if progress.phase != RestorePhase::Pausing {
                    return;
                }
                if progress
                    .change_id
                    .and_then(|id| self.stops.changes.get(&id))
                    .is_some_and(|change| change.unknown)
                {
                    return;
                }
                if !self.claims.contains_key(thread) || self.current(&progress.token) {
                    self.learn_traffic(context, &result, actions);
                }
                if result.is_err() {
                    actions.push(Action::Warn("目标恢复后的补暂停失败".into()));
                }
                self.finish_restore_pause(&run, actions);
            }
        }
    }

    fn finish_restore_pause(&mut self, run: &str, actions: &mut Vec<Action>) {
        let Some(progress) = self.goal_restores.pending.get(run).cloned() else {
            return;
        };
        if progress.pause_again && !self.claims.contains_key(&progress.token.thread_id) {
            self.goal_restores.pending.get_mut(run).unwrap().pause_again = false;
            self.restore_pause(run, actions);
        } else if progress
            .active_change
            .is_some_and(|id| self.stops.changes.contains_key(&id))
        {
            let state = self.goal_restores.pending.get_mut(run).unwrap();
            state.phase = RestorePhase::Unknown;
            state.change_id = state.active_change;
            state.version = self
                .stops
                .versions
                .get(&state.token.thread_id)
                .copied()
                .unwrap_or(0);
        } else {
            self.goal_restores.pending.remove(run);
        }
    }

    fn restore_pause(&mut self, run: &str, actions: &mut Vec<Action>) {
        let progress = self.goal_restores.pending.get_mut(run).unwrap();
        progress.phase = RestorePhase::Pausing;
        progress.change_id = None;
        self.restore_request(
            run,
            GoalRestoreStep::SetPaused { run_id: run.into() },
            actions,
        );
    }

    /// 在 stop 的统一 goal 写入登记点绑定变更代号，超时、迟到与到期共用同一身份。
    pub(crate) fn goal_restore_tracked(&mut self, step: &GoalRestoreStep, id: u64, version: u64) {
        let run = match step {
            GoalRestoreStep::SetActive { run_id } | GoalRestoreStep::SetPaused { run_id } => run_id,
            _ => return,
        };
        if let Some(progress) = self.goal_restores.pending.get_mut(run) {
            progress.change_id = Some(id);
            if matches!(step, GoalRestoreStep::SetActive { .. }) {
                progress.active_change = Some(id);
                progress.active_version = Some(version);
            }
            progress.version = version;
        }
    }

    /// 仅供调试断言和测试核对；生产构建不扫描或修复预检登记。
    pub(super) fn continue_prechecks_valid(&self) -> bool {
        self.continues.pending.iter().all(|(thread, progress)| {
            self.claims.get(thread).is_some_and(|claim| {
                claim.claim_id == progress.claim_id
                    && claim.precheck()
                    && claim.run_id.is_none()
                    && claim.pending_start.is_some()
                    && claim
                        .stream_id
                        .is_some_and(|id| self.stream_slots.contains(&id))
            })
        }) && self
            .claims
            .iter()
            .filter(|(_, claim)| claim.precheck())
            .all(|(thread, claim)| {
                self.continues
                    .pending
                    .get(thread)
                    .is_some_and(|progress| progress.claim_id == claim.claim_id)
            })
    }

    /// 每步沿现有晚到 goal 驱动入口执行：关闭清表、释放清定案标记、未知写入按事实或到期收尾。
    pub(crate) fn drive_goal_restores(&mut self, actions: &mut Vec<Action>) {
        if self.closing {
            self.goal_restores = GoalRestores::default();
            return;
        }
        debug_assert!(
            self.continue_prechecks_valid(),
            "预检登记必须与认领及开工名额一致"
        );
        self.goal_restores.settled.retain(|thread, id| {
            self.claims
                .get(thread)
                .is_some_and(|claim| claim.claim_id == *id)
        });
        let pending: Vec<_> = self
            .goal_restores
            .pending
            .iter()
            .map(|(run, p)| (run.clone(), p.clone()))
            .collect();
        for (run, progress) in pending {
            let thread = &progress.token.thread_id;
            if progress.phase == RestorePhase::Reading && !self.restore_owned(&progress) {
                self.goal_restores.pending.remove(&run);
            } else if progress.phase == RestorePhase::Pausing
                && progress
                    .change_id
                    .is_some_and(|id| !self.stops.changes.contains_key(&id))
            {
                self.finish_restore_pause(&run, actions);
            } else if progress.phase == RestorePhase::Unknown {
                let expired = progress
                    .change_id
                    .is_none_or(|id| !self.stops.changes.contains_key(&id));
                let changed =
                    self.stops.versions.get(thread).copied().unwrap_or(0) != progress.version;
                if !expired && !changed {
                    continue;
                }
                if !self.claims.contains_key(thread)
                    && (expired
                        || self
                            .records
                            .get(thread)
                            .is_some_and(|record| record.goal_active))
                {
                    self.restore_pause(&run, actions);
                } else if expired {
                    self.goal_restores.pending.remove(&run);
                    if !self.current(&progress.token) {
                        actions.push(Action::Warn(
                            "未知目标恢复到期时认领已换代，不补暂停".into(),
                        ));
                    }
                } else {
                    // 非 active 通知不是未知写入已落定的证据，继续等待其迟到回包或到期。
                    self.goal_restores.pending.get_mut(&run).unwrap().version =
                        self.stops.versions.get(thread).copied().unwrap_or(0);
                }
            }
        }
    }

    pub(crate) fn notify_run_settled(
        &mut self,
        thread_id: &str,
        run_id: &str,
        absorbed_by: Option<String>,
        actions: &mut Vec<Action>,
    ) {
        let Some(claim) = self
            .claims
            .get(thread_id)
            .filter(|claim| claim.run_id.as_deref() == Some(run_id) && !claim.precheck())
        else {
            return;
        };
        if self.goal_restores.settled.get(thread_id) == Some(&claim.claim_id) {
            return;
        }
        self.goal_restores
            .settled
            .insert(thread_id.into(), claim.claim_id);
        self.notify_thread_ext(
            thread_id,
            self.owner_conn(thread_id),
            "broker/run/settled",
            json!({"threadId":thread_id,"runId":run_id,"absorbedBy":absorbed_by}),
            actions,
        );
    }
}

impl Machine {
    /// 续跑预检的线程核实（H2 Busy 查询）完成。有效性沿用原核实读的口径：认领仍是当前的预检、
    /// 阶段是核实读、round 是当前轮且本轮线程核实尚未完成；否则是旧轮或已定案的迟到结果，丢弃。
    ///
    /// - `NotBusy`：线程不在活动，本份核实通过；
    /// - `Busy`：最新运行已丢失（Lost）报 `stopping`，否则报 `threadBusy`。元数据为 active 即拒绝，
    ///   比旧的「历史里有 inProgress 轮次」更保守（active 但轮次尚未投影时不会误放行）；
    /// - 其余（含 `Unknown` 的任何原因）报 `resumeCheckFailed`。
    pub fn continue_check_done(
        &mut self,
        done: crate::claim::status_read::StatusDone,
        actions: &mut Vec<Action>,
    ) {
        let StatusCaller::ContinueCheck { round } = done.caller else {
            return;
        };
        let Some(token) = done.token else {
            return;
        };
        let thread = token.thread_id.as_str();
        let current = done.thread == thread
            && self.current(&token)
            && self.claims.get(thread).is_some_and(Claim::precheck);
        let Some(version) = self
            .continues
            .pending
            .get(thread)
            .filter(|p| current && p.claim_id == token.claim_id)
            .filter(|p| p.phase == CheckPhase::Read && p.round == round && !p.thread_done)
            .map(|p| p.version)
        else {
            return;
        };
        if self.closing || self.quiescing {
            self.continue_cancel(thread, CancelCause::Shutdown, actions);
            return;
        }
        if self.stops.versions.get(thread).copied().unwrap_or(0) != version {
            self.continue_read(thread, true, actions);
            return;
        }
        let reason = match done.result {
            StatusResult::NotBusy => None,
            StatusResult::Busy
                if self
                    .latest_run(thread)
                    .is_some_and(|r| r.state == RunState::Lost) =>
            {
                Some(STOPPING)
            }
            StatusResult::Busy => Some(THREAD_BUSY),
            _ => Some(RESUME_CHECK_FAILED),
        };
        self.continue_checked(thread, false, reason, actions);
    }
}
