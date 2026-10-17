//! 无进展期限（契约第 2 节）与访问报错即停（第 3 节）。
//!
//! - 进展只认已确认归属本运行的事件（本运行执行线程上的自有轮、按活动绑定归入的子线程），
//!   按类别去重：轮与条目各计一次、增量仅非空、快照仅内容变化、hook 按实例、token 按高水位。
//! - 惰性单调计时：进展只前移内存里的基准，不重排计时器；到点时基准已前移就按新基准重排。
//!   每个运行最多一个带键计时器（`TimerKey::Stall`），代际不符的到点与核实回包一律丢弃。
//! - 本运行名下有待答（inbox 待答项、已转发未回包的请求）时不判到期；待答真实登记或解除时基准取当时，
//!   并撤销在途核实。settling 中推迟 5 秒再判；已有停止在途时不发起、不重排。
//! - 到期：先写诊断，再对当前有效认领上未结束的自有轮做一次有界状态读取（没有就直接 absent），
//!   复核待答、认领身份、停止在途与关闭状态后才发停止；私有、共享模式都只停当前 owned run。
use crate::claim::status_read::{StatusCaller, StatusDone, StatusQuery, StatusResult, StatusTurn};
use crate::claim::{Action, ClaimToken, Machine, StopReason, TimerKey, TimerKind};
use crate::runs::upstream_error::{UpstreamError, UpstreamErrorCategory, UpstreamErrorSource};
use crate::runs::{
    DiagnosticField, RunState, StallPhase, StallReport, StallState, StallVerify, StallVerifyState,
    UpstreamErrorPolicy,
};
use crate::sa1b::TerminalReason;
use serde_json::Value;
use std::collections::VecDeque;
use std::hash::{Hash, Hasher};

/// settling 中到点时推迟再判的间隔。
pub const STALL_DEFER_MS: u64 = 5_000;
/// 每运行每类进展去重状态的上限，超出淘汰最旧项。
pub const PROGRESS_DEDUP_LIMIT: usize = 1024;
/// 单次挂出的计时上限；期限更长时到点后按惰性规则续排，避免极大延迟。
pub const STALL_MAX_TIMER_MS: u64 = 24 * 60 * 60 * 1000;

/// 有界去重：已见过返回 false，否则记下（超限淘汰最旧）并返回 true。
fn remember<T: PartialEq>(seen: &mut VecDeque<T>, key: T) -> bool {
    if seen.contains(&key) {
        return false;
    }
    if seen.len() >= PROGRESS_DEDUP_LIMIT {
        seen.pop_front();
    }
    seen.push_back(key);
    true
}

fn content_hash(value: Option<&Value>) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.map(Value::to_string).hash(&mut hasher);
    hasher.finish()
}

impl Machine {
    /// 运行当前执行线程上属于该运行的认领。
    fn stall_claim(&self, run: &str) -> Option<(String, ClaimToken)> {
        let record = self.runs.runs.get(run)?;
        let thread = record
            .review_thread_id
            .as_deref()
            .unwrap_or(&record.thread_id);
        let claim = self
            .claims
            .get(thread)
            .filter(|c| c.run_id.as_deref() == Some(run))?;
        Some((
            thread.to_owned(),
            ClaimToken {
                thread_id: thread.to_owned(),
                claim_id: claim.claim_id,
            },
        ))
    }

    /// 本运行名下的未决服务端请求：inbox 待答项（含子线程事项）与已转发给 owner 尚未回包的请求。
    fn stall_pending(&self, run: &str) -> bool {
        !self.inbox_pending_run_items(run).is_empty()
            || self
                .requests
                .pending
                .values()
                .any(|p| p.run_id.as_deref() == Some(run))
    }

    fn stall_settling(&self, run: &str) -> bool {
        self.stall_claim(run)
            .is_some_and(|(thread, _)| self.claims[&thread].settling())
    }

    fn stall_schedule(&mut self, run: &str, delay_ms: u64, actions: &mut Vec<Action>) {
        let token = self
            .stall_claim(run)
            .map(|(_, t)| t)
            .unwrap_or_else(|| ClaimToken {
                thread_id: self
                    .runs
                    .runs
                    .get(run)
                    .map(|r| r.thread_id.clone())
                    .unwrap_or_default(),
                claim_id: 0,
            });
        let Some(state) = self.runs.stall_states.get_mut(run) else {
            return;
        };
        state.generation = state.generation.checked_add(1).expect("无进展代际已耗尽");
        state.timer_scheduled = true;
        actions.push(Action::Schedule {
            token,
            kind: TimerKind::Stall {
                run_id: run.into(),
                generation: state.generation,
            },
            delay_ms: delay_ms.min(STALL_MAX_TIMER_MS),
        });
    }

    /// 按当前基准重排：未到期排剩余时间，已到期立即到点。
    fn stall_schedule_remaining(&mut self, run: &str, actions: &mut Vec<Action>) {
        let Some(state) = self.runs.stall_states.get(run) else {
            return;
        };
        let due = state.baseline_mono.saturating_add(state.timeout_ms);
        let delay = due.saturating_sub(self.clock.mono_ms);
        self.stall_schedule(run, delay, actions);
    }

    /// 撤销在途核实：撤销读取与计时、代际 +1，迟到回包因此被丢弃。返回是否确有在途核实。
    fn stall_cancel_verify(&mut self, run: &str, actions: &mut Vec<Action>) -> bool {
        let Some(state) = self.runs.stall_states.get_mut(run) else {
            return false;
        };
        let Some(verify) = state.verify.take() else {
            return false;
        };
        state.generation = state.generation.checked_add(1).expect("无进展代际已耗尽");
        self.cancel_status_read(verify.read_id, actions);
        actions.push(Action::Unschedule {
            key: TimerKey::Stall(run.into()),
        });
        true
    }

    /// 撤销计时与核实并清除本运行全部计时、去重状态（运行诊断记录保留）。
    fn stall_clear(&mut self, run: &str, actions: &mut Vec<Action>) {
        if let Some(state) = self.runs.stall_states.remove(run) {
            if let Some(verify) = state.verify {
                self.cancel_status_read(verify.read_id, actions);
            }
            actions.push(Action::Unschedule {
                key: TimerKey::Stall(run.into()),
            });
        }
        self.runs.progress_dedup.remove(run);
    }

    /// 改写已有诊断的阶段与核实结果；诊断只在第一次到期后出现。
    fn stall_set(&mut self, run: &str, phase: StallPhase, verify: Option<Option<StallVerify>>) {
        if let Some(state) = self.runs.stall_states.get_mut(run) {
            state.phase = Some(phase);
        }
        let Some(report) = self.runs.runs.get_mut(run).and_then(|r| r.stall.as_mut()) else {
            return;
        };
        let before = report.clone();
        report.phase = phase;
        if let Some(verify) = verify {
            report.verify = verify;
        }
        if *report != before {
            self.runs.updated.insert(run.into(), self.clock.now_ms);
            self.mark_run_diagnostic(run, DiagnosticField::Stall, None);
        }
    }

    /// 到期诊断：写到期时间、距基准毫秒与最近进展；推迟后的同一次到期不重复计数。
    fn stall_expiry_report(&mut self, run: &str, phase: StallPhase) {
        let Some(state) = self.runs.stall_states.get_mut(run) else {
            return;
        };
        let repeat = state.phase == Some(StallPhase::Deferred);
        state.phase = Some(phase);
        let (timeout_ms, idle_ms) = (
            state.timeout_ms,
            self.clock.mono_ms.saturating_sub(state.baseline_mono),
        );
        let now = self.clock.now_ms;
        let Some(record) = self.runs.runs.get_mut(run) else {
            return;
        };
        let expiries = record.stall.as_ref().map_or(0, |s| s.expiries);
        record.stall = Some(StallReport {
            timeout_ms,
            phase,
            expired_at: now,
            idle_ms,
            last_progress_at: record.last_progress_at,
            verify: None,
            expiries: if repeat {
                expiries.max(1)
            } else {
                expiries.saturating_add(1)
            },
        });
        self.runs.updated.insert(run.into(), now);
        self.mark_run_diagnostic(run, DiagnosticField::Stall, None);
    }

    /// 基准取当时（进展、待答变化）：撤销在途核实并按新基准重排；停止阶段只记基准。
    fn stall_rebase(&mut self, run: &str, actions: &mut Vec<Action>) {
        let mono = self.clock.mono_ms;
        let Some(state) = self.runs.stall_states.get_mut(run) else {
            return;
        };
        state.baseline_mono = mono;
        if state.phase == Some(StallPhase::Stopping) {
            return;
        }
        let scheduled = state.timer_scheduled;
        if self.stall_cancel_verify(run, actions) {
            self.stall_set(
                run,
                StallPhase::Resumed,
                Some(Some(StallVerify::Progressed)),
            );
            self.stall_schedule_remaining(run, actions);
        } else if !scheduled {
            self.stall_schedule_remaining(run, actions);
        }
    }

    fn stall_defer(&mut self, run: &str, actions: &mut Vec<Action>) {
        if self.runs.runs.get(run).is_some_and(|r| r.stall.is_some()) {
            self.stall_set(run, StallPhase::Deferred, None);
        } else {
            self.stall_expiry_report(run, StallPhase::Deferred);
        }
        self.stall_schedule(run, STALL_DEFER_MS, actions);
    }

    /// 核实目标：当前有效认领的执行线程上尚未结束的自有轮（不用历史 currentTurnId）。
    fn stall_verify_target(&self, run: &str) -> Option<(String, ClaimToken, String)> {
        let (thread, token) = self.stall_claim(run)?;
        let claim = &self.claims[&thread];
        if claim.joined() || claim.settling() {
            return None;
        }
        let turn = claim.turn_id().filter(|t| !t.is_empty())?.to_owned();
        (self.diagnostic_run(&thread, &turn, false).as_deref() == Some(run))
            .then_some((thread, token, turn))
    }

    /// 本次进展事件是否计数（按运行去重）。
    fn stall_counts(
        &mut self,
        run: &str,
        thread: &str,
        turn: &str,
        method: &str,
        params: &Option<Value>,
    ) -> bool {
        let params = params.as_ref();
        let text = |key: &str| params.and_then(|p| p.get(key)).and_then(Value::as_str);
        let dedup = self.runs.progress_dedup.entry(run.into()).or_default();
        let key = |instance: &str| {
            (
                thread.to_owned(),
                turn.to_owned(),
                instance.to_owned(),
                method.to_owned(),
            )
        };
        match method {
            "turn/started" | "turn/completed" => remember(&mut dedup.items, key("")),
            "item/started" | "item/completed" => params
                .and_then(|p| p.get("item"))
                .and_then(|i| i.get("id"))
                .and_then(Value::as_str)
                .is_some_and(|id| remember(&mut dedup.items, key(id))),
            "item/agentMessage/delta"
            | "item/plan/delta"
            | "item/reasoning/summaryTextDelta"
            | "item/commandExecution/outputDelta"
            | "item/fileChange/outputDelta" => text("delta").is_some_and(|d| !d.is_empty()),
            "item/reasoning/summaryPartAdded"
            | "item/commandExecution/terminalInteraction"
            | "item/mcpToolCall/progress" => true,
            "turn/diff/updated" | "turn/plan/updated" | "item/fileChange/patchUpdated" => {
                let content = match method {
                    "turn/diff/updated" => params.and_then(|p| p.get("diff")).cloned(),
                    "turn/plan/updated" => {
                        params.map(|p| serde_json::json!([p.get("explanation"), p.get("plan")]))
                    }
                    _ => params.and_then(|p| p.get("changes")).cloned(),
                };
                let hash = content_hash(content.as_ref());
                let item = text("itemId").unwrap_or_default();
                let found = dedup
                    .snapshots
                    .iter_mut()
                    .find(|(t, u, i, m, _)| t == thread && u == turn && i == item && m == method);
                match found {
                    Some(entry) if entry.4 == hash => false,
                    Some(entry) => {
                        entry.4 = hash;
                        true
                    }
                    None => {
                        if dedup.snapshots.len() >= PROGRESS_DEDUP_LIMIT {
                            dedup.snapshots.pop_front();
                        }
                        dedup.snapshots.push_back((
                            thread.into(),
                            turn.into(),
                            item.into(),
                            method.into(),
                            hash,
                        ));
                        true
                    }
                }
            }
            "hook/started" | "hook/completed" => params
                .and_then(|p| p.get("run"))
                .and_then(|r| r.get("id"))
                .and_then(Value::as_str)
                .is_some_and(|id| remember(&mut dedup.hooks, key(id))),
            "thread/tokenUsage/updated" => {
                let Some(total) = params
                    .and_then(|p| p.pointer("/tokenUsage/total/totalTokens"))
                    .and_then(Value::as_u64)
                else {
                    return false;
                };
                match dedup.token_high_water.iter_mut().find(|(t, _)| t == thread) {
                    Some(entry) if total > entry.1 => {
                        entry.1 = total;
                        true
                    }
                    Some(_) => false,
                    None => {
                        if dedup.token_high_water.len() >= PROGRESS_DEDUP_LIMIT {
                            dedup.token_high_water.pop_front();
                        }
                        dedup.token_high_water.push_back((thread.into(), total));
                        true
                    }
                }
            }
            _ => false,
        }
    }

    pub(crate) fn stall_arm(&mut self, run: &str, actions: &mut Vec<Action>) {
        if self.closing || self.quiescing {
            return;
        }
        let Some(record) = self.runs.runs.get(run) else {
            return;
        };
        let timeout_ms = record.run_policy.as_ref().map_or(0, |p| p.stall_timeout_ms);
        let live = !record.joined && matches!(record.state, RunState::Running | RunState::Stopping);
        self.stall_clear(run, actions);
        if timeout_ms == 0 || !live {
            return;
        }
        self.runs.stall_states.insert(
            run.into(),
            StallState {
                baseline_mono: self.clock.mono_ms,
                timeout_ms,
                generation: 0,
                timer_scheduled: false,
                verify: None,
                phase: None,
            },
        );
        self.stall_schedule(run, timeout_ms, actions);
    }

    /// 显式策略的运行由这里去重并更新 lastProgressAt（调用方比较前后值标脏）；旧客户端不经此处。
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn stall_progress(
        &mut self,
        run: &str,
        thread: &str,
        turn: &str,
        method: &str,
        params: &Option<Value>,
        _replay: bool,
        actions: &mut Vec<Action>,
    ) {
        if turn.is_empty()
            || !self
                .runs
                .runs
                .get(run)
                .is_some_and(|r| r.run_policy.is_some())
            || !self.stall_counts(run, thread, turn, method, params)
        {
            return;
        }
        let now = self.clock.now_ms;
        if let Some(record) = self.runs.runs.get_mut(run) {
            record.last_progress_at = Some(now);
        }
        self.stall_rebase(run, actions);
    }

    pub(crate) fn stall_timer(&mut self, run: &str, generation: u64, actions: &mut Vec<Action>) {
        let Some(state) = self.runs.stall_states.get_mut(run) else {
            return;
        };
        if state.generation != generation || !state.timer_scheduled {
            actions.push(Action::Debug(format!(
                "忽略运行 {run} 过期代际 {generation} 的无进展计时"
            )));
            return;
        }
        state.timer_scheduled = false;
        if state.verify.is_some() || state.phase == Some(StallPhase::Stopping) {
            return;
        }
        let due = state.baseline_mono.saturating_add(state.timeout_ms);
        if self.clock.mono_ms < due {
            self.stall_schedule_remaining(run, actions);
            return;
        }
        self.stall_expired(run, actions);
    }

    fn stall_expired(&mut self, run: &str, actions: &mut Vec<Action>) {
        if self.closing || self.quiescing {
            return;
        }
        let Some(record) = self.runs.runs.get(run) else {
            return;
        };
        if record.joined || !matches!(record.state, RunState::Running | RunState::Stopping) {
            self.stall_clear(run, actions);
            return;
        }
        let stopping = record.state == RunState::Stopping;
        if stopping
            || self
                .stall_claim(run)
                .is_some_and(|(thread, _)| self.stop_busy(&thread))
        {
            // 停止流程接管，不发起、不重排。
            return;
        }
        if self.stall_pending(run) {
            if let Some(state) = self.runs.stall_states.get_mut(run) {
                state.baseline_mono = self.clock.mono_ms;
            }
            self.stall_schedule_remaining(run, actions);
            return;
        }
        if self.stall_settling(run) {
            self.stall_defer(run, actions);
            return;
        }
        self.stall_expiry_report(run, StallPhase::Verifying);
        let Some((thread, token, turn)) = self.stall_verify_target(run) else {
            self.stall_conclude(run, StallVerify::Absent, None, actions);
            return;
        };
        let state = self.runs.stall_states.get_mut(run).expect("计时状态存在");
        state.generation = state.generation.checked_add(1).expect("无进展代际已耗尽");
        let generation = state.generation;
        let claim_id = token.claim_id;
        let read_id = self.begin_status_read(
            &thread,
            Some(token),
            StatusQuery::Turn {
                target: turn.clone(),
            },
            StatusCaller::StallVerify {
                run_id: run.into(),
                generation,
            },
            actions,
        );
        let conn_epoch = self.relations.conn_epoch;
        if let Some(state) = self.runs.stall_states.get_mut(run) {
            state.verify = Some(StallVerifyState {
                read_id,
                generation,
                claim_id,
                conn_epoch,
                turn_id: turn,
            });
        }
    }

    pub(crate) fn stall_verify_done(&mut self, done: StatusDone, actions: &mut Vec<Action>) {
        let StatusCaller::StallVerify { run_id, generation } = &done.caller else {
            return;
        };
        let run = run_id.clone();
        let Some(state) = self.runs.stall_states.get_mut(&run) else {
            return;
        };
        let verify = match state.verify.take() {
            Some(v)
                if v.read_id == done.read_id
                    && v.generation == *generation
                    && state.generation == *generation =>
            {
                v
            }
            other => {
                state.verify = other;
                actions.push(Action::Debug(format!(
                    "丢弃运行 {run} 过期的无进展核实结果 {}",
                    done.read_id
                )));
                return;
            }
        };
        if self.closing || self.quiescing {
            return;
        }
        if self.stall_settling(&run) {
            self.stall_defer(&run, actions);
            return;
        }
        let token = done
            .token
            .clone()
            .filter(|t| self.current(t) && t.claim_id == verify.claim_id)
            .filter(|_| self.relations.conn_epoch == verify.conn_epoch);
        let outcome = match (&token, &done.result) {
            (None, _) => StallVerify::Unknown,
            (Some(token), StatusResult::Found { status, shell })
                if *status != StatusTurn::InProgress
                    && shell.get("id").and_then(Value::as_str) == Some(&verify.turn_id) =>
            {
                // 提交终态可能移除 owner；先保存本事件的诊断接收者，最终仍只统一提交一次。
                self.mark_run_diagnostic(&run, DiagnosticField::Stall, None);
                if !self.commit_verified_turn_end(&run, token, *status, shell, actions) {
                    // 同一结束证据已处理过或身份不符：不再次重置基准，按读取失败复核。
                    StallVerify::Unknown
                } else {
                    // 收尾可能已释放计时状态；核实结论仍须写入保留的运行记录。
                    self.stall_set(
                        &run,
                        StallPhase::Resumed,
                        Some(Some(StallVerify::TurnEnded)),
                    );
                    // 新发现的终态已交既有收尾；运行仍在计时就以当时为基准重排。
                    if let Some(state) = self.runs.stall_states.get_mut(&run) {
                        state.baseline_mono = self.clock.mono_ms;
                        self.stall_schedule_remaining(&run, actions);
                    }
                    return;
                }
            }
            (Some(_), StatusResult::Found { .. }) => StallVerify::InProgress,
            (Some(_), StatusResult::Absent) => StallVerify::Absent,
            (Some(_), _) => StallVerify::Unknown,
        };
        self.stall_conclude(&run, outcome, Some(verify.claim_id), actions);
    }

    /// 停止提交前复核（2.3 第 4、5 步）。
    fn stall_conclude(
        &mut self,
        run: &str,
        verify: StallVerify,
        claim_id: Option<u64>,
        actions: &mut Vec<Action>,
    ) {
        if let Some(state) = self.runs.stall_states.get(run) {
            let phase = state.phase.unwrap_or(StallPhase::Verifying);
            self.stall_set(run, phase, Some(Some(verify)));
        }
        if self.closing || self.quiescing {
            actions.push(Action::Debug(format!(
                "运行 {run} 无进展核实结束，但正在关闭，不发停止"
            )));
            return;
        }
        if self.stall_pending(run) {
            if let Some(state) = self.runs.stall_states.get_mut(run) {
                state.baseline_mono = self.clock.mono_ms;
            }
            self.stall_set(run, StallPhase::Resumed, None);
            self.stall_schedule_remaining(run, actions);
            return;
        }
        if self.stall_settling(run) {
            self.stall_defer(run, actions);
            return;
        }
        let Some((thread, token)) = self.stall_claim(run) else {
            actions.push(Action::Debug(format!(
                "运行 {run} 无进展核实结束，但已无有效认领，不发停止"
            )));
            return;
        };
        let claim = &self.claims[&thread];
        let record = &self.runs.runs[run];
        if claim.joined()
            || record.joined
            || record.state != RunState::Running
            || claim_id.is_some_and(|id| id != token.claim_id)
            || self.stop_busy(&thread)
        {
            actions.push(Action::Debug(format!(
                "运行 {run} 无进展核实结束，但认领已变化、已在停止或已结束，不发停止"
            )));
            return;
        }
        self.stall_set(run, StallPhase::Stopping, None);
        let timeout_s = self
            .runs
            .stall_states
            .get(run)
            .map_or(0, |s| s.timeout_ms / 1000);
        actions.push(Action::Warn(format!(
            "运行 {run} 已 {timeout_s} 秒未观察到进展（核实：{verify:?}），停止本次运行"
        )));
        self.stall_stop(
            run,
            &thread,
            TerminalReason::StallTimeout,
            StopReason::StallTimeout,
            actions,
        );
    }

    /// 只复用空转刹车的身份核对与调用组织：SA1b 已启用走收敛失败，否则普通停止（受管子树由停止入口转整树）。
    fn stall_stop(
        &mut self,
        run: &str,
        thread: &str,
        terminal: TerminalReason,
        reason: StopReason,
        actions: &mut Vec<Action>,
    ) {
        if self.sa1b_active(run) && self.convergence_enabled() {
            self.convergence_fail(run, terminal, actions);
        } else {
            self.begin_stop(thread, reason, false, actions);
        }
    }

    pub(crate) fn stall_pending_changed(
        &mut self,
        run: &str,
        real: bool,
        actions: &mut Vec<Action>,
    ) {
        if real {
            self.stall_rebase(run, actions);
        }
    }

    pub(crate) fn stall_owned_settled(&mut self, run: &str, actions: &mut Vec<Action>) {
        if !self.runs.stall_states.contains_key(run) {
            return;
        }
        self.stall_cancel_verify(run, actions);
        if let Some(state) = self.runs.stall_states.get_mut(run) {
            state.baseline_mono = self.clock.mono_ms;
        }
        self.stall_schedule_remaining(run, actions);
    }

    pub(crate) fn stall_joined(&mut self, run: &str, actions: &mut Vec<Action>) {
        self.stall_clear(run, actions);
    }

    pub(crate) fn stall_released(&mut self, run: &str, actions: &mut Vec<Action>) {
        self.stall_clear(run, actions);
        self.runs.access_stopped.remove(run);
    }

    /// review 迁移保留基准，使在途核实失效并按新执行线程继续（已超期则立即重新到点）。
    pub(crate) fn stall_review_moved(&mut self, run: &str, actions: &mut Vec<Action>) {
        let Some(state) = self.runs.stall_states.get(run) else {
            return;
        };
        if state.phase == Some(StallPhase::Stopping) {
            return;
        }
        self.stall_cancel_verify(run, actions);
        self.stall_schedule_remaining(run, actions);
    }

    pub(crate) fn stall_shutdown(&mut self, actions: &mut Vec<Action>) {
        let runs: Vec<_> = self.runs.stall_states.keys().cloned().collect();
        for run in runs {
            self.stall_clear(&run, actions);
        }
        self.runs.progress_dedup.clear();
    }

    pub(crate) fn upstream_access_check(
        &mut self,
        run: &str,
        error: &UpstreamError,
        actions: &mut Vec<Action>,
    ) {
        let stop_access = self
            .runs
            .runs
            .get(run)
            .and_then(|r| r.run_policy.as_ref())
            .is_some_and(|p| p.upstream_error_policy == UpstreamErrorPolicy::StopAccess);
        if !stop_access
            || error.source == UpstreamErrorSource::RpcResponse
            || !matches!(
                error.category,
                UpstreamErrorCategory::Authentication
                    | UpstreamErrorCategory::PaymentRequired
                    | UpstreamErrorCategory::PermissionDenied
            )
            || self.runs.access_stopped.contains(run)
            || self.closing
            || self.quiescing
        {
            return;
        }
        // settling、身份不符时本次不触发，下一条同类报错再判；已在停止则以先发起者为准。
        let Some((thread, _)) = self.stall_claim(run) else {
            return;
        };
        let claim = &self.claims[&thread];
        let record = &self.runs.runs[run];
        if claim.settling() || claim.joined() || record.joined || self.stall_pending(run) {
            return;
        }
        self.runs.access_stopped.insert(run.into());
        if record.state != RunState::Running || self.stop_busy(&thread) {
            return;
        }
        actions.push(Action::Warn(format!(
            "运行 {run} 出现上游访问报错（{:?}），按 stop-access 停止本次运行",
            error.category
        )));
        self.stall_cancel_verify(run, actions);
        self.stall_stop(
            run,
            &thread,
            TerminalReason::UpstreamAccessError,
            StopReason::UpstreamAccessError,
            actions,
        );
    }
}
