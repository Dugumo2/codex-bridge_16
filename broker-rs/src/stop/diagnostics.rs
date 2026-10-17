//! 停止诊断（契约 4.3）：记录本运行根停止各阶段的时间、interrupt 发送与回包、RPC 超时／失败摘要。
//!
//! - 只读诊断，不参与停止判定；所有写入只标脏，由 `flush_run_diagnostics` 在事件末尾统一增版提交。
//! - 诊断只由 `stop_diag_began` 新建：历史停止（运行已结束、`already_stopped`、回退到旧运行）
//!   拿不到发送身份，也就不会新建或改写。
//! - 每次写入按发送身份核对：停止代际须与本运行当前诊断代际一致，目标线程须是运行当前的执行线程，
//!   review 迁移前发出的请求（线程是旧执行线程）的回包因此不会覆盖迁移后的字段。
//! - `unconfirmedAt`／`confirmedAt` 描述整个停止范围：调用方只在单线程停止或整树结论处调用。
use crate::claim::Machine;
use crate::runs::upstream_error::sanitize;
use crate::runs::{
    DiagnosticField, InterruptReason, RunState, StopDiagSend, StopDiagnostics, StopRpcIssue,
    StopRpcOutcome,
};

/// `rpcIssues` 最多保留的条数（只留最近的）。
pub const MAX_STOP_RPC_ISSUES: usize = 8;
/// 每条 RPC 问题摘要的字节上限。
pub const MAX_STOP_RPC_MESSAGE_BYTES: usize = 300;

/// 脱敏、压成单行并按 UTF-8 字符边界截断到上限。
pub(crate) fn issue_message(message: &str) -> String {
    let (text, _) = sanitize(message);
    let line = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if line.len() <= MAX_STOP_RPC_MESSAGE_BYTES {
        return line;
    }
    let mut end = MAX_STOP_RPC_MESSAGE_BYTES;
    while !line.is_char_boundary(end) {
        end -= 1;
    }
    line[..end].to_owned()
}

impl Machine {
    /// 目标身份切换时丢弃旧目标证据；运行级首次时间、原因和最近问题列表保持不变。
    fn stop_diag_select_target(&mut self, run: &str, send: &StopDiagSend) {
        let changed = self
            .runs
            .stop_diag_sends
            .get(run)
            .is_some_and(|old| old != send);
        self.runs.stop_diag_sends.insert(run.into(), send.clone());
        if changed {
            self.stop_diag_update(run, |d, _| {
                let changed = d.turn_ended_at.is_some()
                    || d.interrupt_reply.is_some()
                    || d.interrupt_replied_at.is_some();
                d.turn_ended_at = None;
                d.interrupt_reply = None;
                d.interrupt_replied_at = None;
                changed
            });
        }
    }

    /// 新轮可能在旧 goal RPC 尚未回包时接管目标，事件提交前即同步，不能等下一次 interrupt。
    pub(crate) fn sync_stop_diagnostic_targets(&mut self) {
        let targets: Vec<_> = self
            .stops
            .operations
            .values()
            .filter_map(|op| op.diagnostic_send(op.target.as_deref()))
            .collect();
        for (run, send) in targets {
            if self.stop_diag_current(&run, &send) {
                self.stop_diag_select_target(&run, &send);
            }
        }
    }

    /// 运行当前的执行线程（review 迁移后为 review 线程）。
    fn stop_diag_execution(&self, run: &str) -> Option<String> {
        let record = self.runs.runs.get(run)?;
        Some(
            record
                .review_thread_id
                .clone()
                .unwrap_or_else(|| record.thread_id.clone()),
        )
    }

    /// 发送身份是否属于本运行当前的根停止：诊断已存在、停止代际一致、目标线程是当前执行线程。
    fn stop_diag_current(&self, run: &str, send: &StopDiagSend) -> bool {
        self.runs
            .runs
            .get(run)
            .is_some_and(|r| r.stop_diagnostics.is_some() && !r.joined)
            && self
                .runs
                .stop_diag_sends
                .get(run)
                .is_some_and(|current| current.stop_generation == send.stop_generation)
            && self.stop_diag_execution(run).as_deref() == Some(send.thread.as_str())
    }

    /// 改写已有诊断并在确有变化时标脏。
    fn stop_diag_update(
        &mut self,
        run: &str,
        update: impl FnOnce(&mut StopDiagnostics, u64) -> bool,
    ) {
        let now = self.clock.now_ms;
        let Some(diagnostics) = self
            .runs
            .runs
            .get_mut(run)
            .and_then(|r| r.stop_diagnostics.as_mut())
        else {
            return;
        };
        if update(diagnostics, now) {
            self.mark_run_diagnostic(run, DiagnosticField::StopDiagnostics, None);
        }
    }

    pub(crate) fn stop_diag_began(
        &mut self,
        run: &str,
        reason: InterruptReason,
        stop_generation: u64,
    ) {
        let Some(thread) = self.stop_diag_execution(run) else {
            return;
        };
        let record = &self.runs.runs[run];
        if record.joined || matches!(record.state, RunState::Ended | RunState::Lost) {
            return;
        }
        // 已退役运行的发送身份不再需要；表随运行表有界。
        let runs = &self.runs.runs;
        self.runs
            .stop_diag_sends
            .retain(|id, _| runs.contains_key(id));
        if self
            .runs
            .stop_diag_sends
            .get(run)
            .is_none_or(|send| send.stop_generation != stop_generation || send.thread != thread)
        {
            self.stop_diag_select_target(
                run,
                &StopDiagSend {
                    stop_generation,
                    turn: None,
                    thread,
                },
            );
        }
        let now = self.clock.now_ms;
        let record = self.runs.runs.get_mut(run).expect("运行存在");
        if record.stop_diagnostics.is_some() {
            // 先发起者为准：原因与开始时间保留首次，后续停止只换发送身份。
            return;
        }
        record.stop_diagnostics = Some(StopDiagnostics {
            reason,
            started_at: now,
            goal_started_at: None,
            goal_settled_at: None,
            interrupt_sent: false,
            interrupt_sent_at: None,
            interrupt_reply: None,
            interrupt_replied_at: None,
            turn_ended_at: None,
            unconfirmed_at: None,
            confirmed_at: None,
            rpc_issues: Vec::new(),
        });
        self.mark_run_diagnostic(run, DiagnosticField::StopDiagnostics, None);
    }

    pub(crate) fn stop_diag_goal_started(&mut self, run: &str, send: &StopDiagSend) {
        if self.stop_diag_current(run, send) {
            self.stop_diag_update(run, |d, now| set_once(&mut d.goal_started_at, now));
        }
    }

    pub(crate) fn stop_diag_goal_settled(&mut self, run: &str, send: &StopDiagSend) {
        if self.stop_diag_current(run, send) {
            self.stop_diag_update(run, |d, now| set_once(&mut d.goal_settled_at, now));
        }
    }

    /// 最近一次发送：新发送先清空上一次的回包字段，之后的回包只认这次发送的目标轮。
    pub(crate) fn stop_diag_interrupt_sent(&mut self, run: &str, send: &StopDiagSend) {
        if !self.stop_diag_current(run, send) {
            return;
        }
        self.stop_diag_select_target(run, send);
        self.stop_diag_update(run, |d, now| {
            d.interrupt_sent = true;
            d.interrupt_sent_at = Some(now);
            d.interrupt_reply = None;
            d.interrupt_replied_at = None;
            true
        });
    }

    pub(crate) fn stop_diag_interrupt_reply(
        &mut self,
        run: &str,
        send: &StopDiagSend,
        outcome: StopRpcOutcome,
    ) {
        if !self.stop_diag_current(run, send)
            || self.runs.stop_diag_sends.get(run).map(|s| &s.turn) != Some(&send.turn)
        {
            return;
        }
        self.stop_diag_update(run, |d, now| {
            if !d.interrupt_sent || d.interrupt_reply.is_some() {
                return false;
            }
            d.interrupt_reply = Some(outcome);
            d.interrupt_replied_at = Some(now);
            true
        });
    }

    pub(crate) fn stop_diag_turn_ended(&mut self, run: &str, send: &StopDiagSend) {
        self.sync_stop_diagnostic_targets();
        if self.stop_diag_current(run, send)
            && self
                .runs
                .stop_diag_sends
                .get(run)
                .is_some_and(|current| current.turn.is_none() || current.turn == send.turn)
        {
            self.stop_diag_select_target(run, send);
            self.stop_diag_update(run, |d, now| set_once(&mut d.turn_ended_at, now));
        }
    }

    pub(crate) fn stop_diag_rpc_issue(
        &mut self,
        run: &str,
        send: &StopDiagSend,
        method: &str,
        kind: StopRpcOutcome,
        message: &str,
    ) {
        if kind == StopRpcOutcome::Ok || !self.stop_diag_current(run, send) {
            return;
        }
        let issue = StopRpcIssue {
            method: method.into(),
            kind,
            at: self.clock.now_ms,
            message: issue_message(message),
        };
        self.stop_diag_update(run, |d, _| {
            if d.rpc_issues.len() >= MAX_STOP_RPC_ISSUES {
                d.rpc_issues.remove(0);
            }
            d.rpc_issues.push(issue);
            true
        });
    }

    pub(crate) fn stop_diag_unconfirmed(&mut self, run: &str) {
        self.stop_diag_update(run, |d, now| {
            d.confirmed_at.is_none() && set_once(&mut d.unconfirmed_at, now)
        });
    }

    pub(crate) fn stop_diag_confirmed(&mut self, run: &str) {
        self.stop_diag_update(run, |d, now| set_once(&mut d.confirmed_at, now));
    }
}

/// 只在首次写入；返回是否写了。
fn set_once(slot: &mut Option<u64>, now: u64) -> bool {
    if slot.is_some() {
        return false;
    }
    *slot = Some(now);
    true
}
