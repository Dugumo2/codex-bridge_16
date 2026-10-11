//! 运行记录账本：运行怎么建、结果怎么生成、进度与日志、关闭与重启（详细设计 3.6、7.2 节，第 5 节第 4、5 步）。
//! 【乙路实现函数体；丙路在 claim、stop、core 里按下列钩子的约定调用】
//!
//! 与 claim、requests、stop 一样挂在 [`Machine`] 上，共用一次 `claim::step`，纯逻辑：
//! 不读时钟（时间与随机数取 `Machine.clock`，由 core 在每次 step 前填写），不做 I/O，
//! 写盘以 [`Action::Store`]、补读以 [`Action::Request`]（`Operation::ResultRead`）、计时以
//! [`Action::Schedule`]（`TimerKind::Run`）表达。
//!
//! 钩子约定（谁在什么时刻调用）：
//! - [`Machine::run_opened`]：#2 建 Claim 的那一刻（含 turn/steer 的 joined 运行、review 的父线程运行）。
//! - [`Machine::run_settled`]：共享服务器上认领定案（含 `broker/joined`、undecided 之后的定案），在回放缓冲之前。
//! - [`Machine::run_turn_started`]：本运行的认领观察到 turn/started 或从回包学到 turn_id；登记「实际线程 + turnId → 运行」。
//! - [`Machine::run_item`]：线程上提交的每条 item/started、item/completed（含 review 子线程缓冲回放的那一批）。
//! - [`Machine::run_turn_completed`]：线程上每条 turn/completed，**在 claim 的完成去重之前**调用（含缓冲回放、
//!   停止期间、认领已释放后迟到的）；按「实际线程 + turnId」找运行，找不到再用 `fallback_run`，账本自己去重。
//! - [`Machine::run_start_failed`]：#10，启动回包确定失败、没产生 turn。
//! - [`Machine::run_review_moved`]：3.5 节 review 运行从父线程转到子线程（运行不结束）。
//! - [`Machine::run_goal_status`]：观察到线程 goal 的状态（通知或 goal 读写回包）。
//! - [`Machine::run_stop_began`] / [`Machine::run_stop_check`] / [`Machine::run_stop_confirmed`]：停止操作发起、
//!   未确认时的判定快照（40 秒到期与每轮核实）、唯一判定 `confirmed()` 通过（在释放之前调用）。
//! - [`Machine::run_loop_guard`]：空转刹车触发，在它发起停止或暂停之前调用；
//!   [`Machine::run_loop_guard_pause`]：共享模式暂停 goal 的回包（运行可能已结束）。
//! - 空转刹车的条目证据：completed 自带完整条目时当场、补读落定时放进 `Runs.guard_evidence`，
//!   由 claim 的 `drive_loop_guards` 在每次 step 末尾取空。
//! - [`Machine::run_released`]：认领释放，一次运行到此结束（3.6「一次运行在认领释放时才算结束」），带释放原因。
//! - [`Machine::run_learn_thread`] / [`Machine::run_forget_thread`]：线程索引的增删。
//! - [`Machine::restore_runs`]、[`Machine::shutdown_runs`]、[`Machine::finalize_runs`]：启动、关闭第 4 步、第 5 步期限到。
//!
//! 关闭阶段的写盘权限（预审结论）：`shutdown_runs` 之后，只有已在途的结果补读可以更新输出并写盘；
//! `finalize_runs` 之后账本封口，任何钩子都不再产生 [`Action::Store`] 或补读请求。
//!
//! 测试观测：每个钩子入口调用 [`Runs::trace`] 记下钩子名（仅测试构建记录），丙路的单测据此断言调用时机与次数；
//! 乙路实现函数体时保留这一行。

use crate::claim::{Action, ConnId, Machine, ReqKind, StopReason};
use serde_json::Value;
use std::collections::HashMap;

#[cfg(test)]
mod tests;
pub mod types;

pub use types::*;

/// 结果补读退避：1、2、4 秒，共 3 次（第 8 节）。
pub const RESULT_RETRY_MS: [u64; 3] = [1_000, 2_000, 4_000];
/// 进度落盘：每完成 10 个条目或每 5 秒一次（第 8 节）。
pub const PROGRESS_FLUSH_ITEMS: u64 = 10;
pub const PROGRESS_FLUSH_MS: u64 = 5_000;
/// 进度列表上限（S1-评审.md 第 20 节）。
pub const MAX_DONE_ITEMS: usize = 200;
pub const MAX_OBSERVED_FILES: usize = 500;
pub const MAX_UNCONFIRMED_ITEMS: usize = 64;
/// 已结束、可淘汰的运行在内存中最多保留的线程数（与线程记录上限一致）。
pub const MAX_RECENT_RUNS: usize = crate::claim::MAX_HISTORY;
/// 「实际线程 + turnId → 运行」关联：每线程最多保留的 turn 数（与 ended_turns 的上限一致）；线程数随运行淘汰。
pub const TURN_RUNS_PER_THREAD: usize = crate::claim::ENDED_PER_THREAD;
/// 每日清理计时。
pub const CLEANUP_INTERVAL_MS: u64 = 24 * 3600 * 1000;

/// 一次补读请求的身份：回包按它找回运行与轮次；generation 与 attempt 一起核对，旧的、已作废的回包丢弃。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultRead {
    pub run_id: RunId,
    /// 实际要读的线程：review 运行转到子线程后是子线程，不是运行记录里保留的父线程。
    pub thread_id: String,
    pub turn_id: String,
    /// 从零开始的绝对轮次序号；保留窗口截掉旧轮后不重编号。
    pub turn_index: usize,
    /// 0 为首次，1–3 为重试；重启后的单次补读也从 0 开始、不再重试（7.2 节「读一次」）。
    pub attempt: u8,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunTimer {
    Purge { generation: u64 },
    ResultRetry(ResultRead),
    ProgressFlush { run_id: RunId, generation: u64 },
    DailyCleanup,
}

/// 建运行所需的信息，由丙路在 #2 处填写。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunOpen {
    pub thread_id: String,
    pub run_id: RunId,
    /// 发起连接；用来取出该连接 annotate 过的 pending_meta（M6 填入，M5 时一般为空）。
    pub conn: Option<ConnId>,
    pub kind: ReqKind,
    pub joined: bool,
    /// 本次流式请求的输入文本（Claim.submitted_input 用换行连接）。
    pub input: String,
    /// `--continue` 续跑时为原运行（#2d，乙路填）；普通运行为 None。
    pub continued_from: Option<RunId>,
}

/// 账本产生、需要发给客户端的事件（丙路在 [`crate::claim::step`] 末尾用 `flush_run_notices` 取走并转成通知）。
/// 账本不知道连接，只记事实；收件人（owner、watchers）与 capabilities.cxb 门控由丙路决定。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunNotice {
    /// 某一轮结果就绪或确定读不到（ready 由 pending 变为 ready、unavailable，或完成时就已就绪），
    /// 对应 `broker/thread/result{threadId, runId, turn, result}`。`thread_id` 是运行记录的线程。
    TurnResult {
        thread_id: String,
        run_id: RunId,
        turn_index: u64,
        result: TurnResult,
    },
    /// 运行结束（认领释放），对应 `broker/run/ended{threadId, run}`。`owner` 是释放前捕获的
    /// 在线 owner 连接（step 末尾 flush 时认领已删除，不能再查）。
    RunEnded {
        thread_id: String,
        run_id: RunId,
        owner: Option<crate::claim::ConnId>,
    },
}

/// 已淘汰运行的墓碑上限（丙路）：内存账本每线程只留最近一次运行，更早的运行在这里记所属线程，
/// 让续跑回 `notLatestRun` 而不是 `unknownRun`、让交接回 `runEnded`；超出淘汰最旧的。
pub const MAX_RETIRED_RUNS: usize = 4096;

/// 运行编号查询结果（[`crate::claim::Machine::run_lookup`]）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunLookup {
    /// 在内存账本里，可以取快照。
    Live,
    /// 已从内存淘汰，只知道所属线程（墓碑）。
    Retired { thread_id: String },
    /// 不认识（从未有过，或墓碑也已淘汰）。
    Unknown,
}

/// 认领为什么释放；决定运行终态与 interruptReason 是否保留（预审结论第 5 条）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseKind {
    /// 没有停止操作，turn 结束（#15）或两轮之间 goal 不再活跃（#17）。
    Natural,
    /// 唯一判定通过后释放（含屏障中取消、#20）。
    StopConfirmed,
    /// #10：启动确定失败、没产生 turn。
    StartFailed,
    /// joined 运行随别人的 turn 结束或被拒而退出。
    Joined,
    /// 关闭第 4 步释放全部认领。
    Shutdown,
}

/// 内存账本。活动记录随认领，历史最多 200 个线程；内部登记的清理随结果、释放和淘汰。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Runs {
    pub(crate) epoch: u64,
    next_creation_sequence: u64,
    pub(crate) verifying_history: std::collections::HashSet<RunId>,
    pub(crate) lifecycle_gates: std::collections::BTreeMap<String, String>,
    purge: Option<purge::PendingPurge>,
    pub runs: HashMap<RunId, RunRecord>,
    pub index: ThreadIndexFile,
    /// 待发出的客户端事件；每次 step 末尾由丙路取空，天然有界。
    pub notices: Vec<RunNotice>,
    /// 交给空转刹车的条目证据（runId、turnId；None 表示确定读不到完整条目），每轮最多一条；
    /// 由 `Machine::drive_loop_guards` 在每次 step 末尾取空，天然有界。
    pub(crate) guard_evidence: Vec<(RunId, String, Option<crate::results::Activity>)>,
    retired: std::collections::VecDeque<(RunId, String)>,
    #[cfg(test)]
    pub hook_log: Vec<String>,
    active: HashMap<RunId, ActiveRun>,
    updated: HashMap<RunId, u64>,
    turn_runs: HashMap<String, std::collections::VecDeque<state::TurnLink>>,
    goals: HashMap<String, String>,
    reads: HashMap<u64, reads::PendingRead>,
    read_queue: std::collections::VecDeque<u64>,
    /// 不同线程的历史存储可能不同，只抑制重复告警，不全局禁用分页。
    paging_fallback_logged: bool,
    /// SA-b：树级停止未确认（或仍有清理屏障）的运行，内存淘汰时保留。
    pub(crate) tree_retained: std::collections::HashSet<RunId>,
    phase: Phase,
    next_generation: u64,
    cleanup_scheduled: bool,
    restored: bool,
}

mod purge;
mod reads;
mod state;
use crate::results::{
    full_items, limit_text, output_from_items, summarize_item, Activity, Snapshot,
};
pub(crate) use state::utc_timestamp;
use state::{schedule, ActiveRun, Phase};
pub use state::{MAX_RESULT_QUEUE, MAX_RESULT_READS, MAX_TURNS};

impl Runs {
    pub(crate) fn lookup(&self, run: &str) -> RunLookup {
        if self.runs.contains_key(run) {
            RunLookup::Live
        } else if let Some((_, thread)) = self.retired.iter().find(|(id, _)| id == run) {
            RunLookup::Retired {
                thread_id: thread.clone(),
            }
        } else {
            RunLookup::Unknown
        }
    }
    pub(crate) fn retire(&mut self, run: String, thread: String) {
        self.retired.retain(|(id, _)| id != &run);
        self.retired.push_back((run, thread));
        while self.retired.len() > MAX_RETIRED_RUNS {
            self.retired.pop_front();
        }
    }
    pub(crate) fn thread_reads_pending(&self, thread: &str) -> bool {
        self.reads.values().any(|pending| {
            pending.phase != reads::ReadPhase::Done
                && (pending.read.thread_id == thread
                    || self.runs.get(&pending.read.run_id).is_some_and(|r| {
                        r.thread_id == thread || r.review_thread_id.as_deref() == Some(thread)
                    }))
        })
    }
    fn notice_turn(&mut self, run: &str, turn_index: u64) {
        if self.phase == Phase::Finalized {
            return;
        }
        let Some(record) = self.runs.get(run) else {
            return;
        };
        let base = record.turns_total.saturating_sub(record.turns.len() as u64);
        let Some(turn) = turn_index
            .checked_sub(base)
            .and_then(|i| record.turns.get(i as usize))
        else {
            return;
        };
        if turn.ready == Ready::Pending {
            return;
        }
        self.notices.push(RunNotice::TurnResult {
            thread_id: record.thread_id.clone(),
            run_id: run.into(),
            turn_index,
            result: turn.clone(),
        });
    }
    /// 取走待发出的客户端事件（丙路在 step 末尾调用）。
    pub fn take_notices(&mut self) -> Vec<RunNotice> {
        std::mem::take(&mut self.notices)
    }

    /// 每个钩子入口调用一次；测试观测也限制为最近 4096 条，避免长压测无限累积。
    #[allow(unused_variables)]
    pub fn trace(&mut self, hook: &str) {
        #[cfg(test)]
        {
            if self.hook_log.len() == 4096 {
                self.hook_log.remove(0);
            }
            self.hook_log.push(hook.to_owned());
        }
    }
}

impl Machine {
    pub(crate) fn run_opened(&mut self, open: RunOpen, actions: &mut Vec<Action>) {
        self.runs.trace("run_opened");
        if self.runs.phase != Phase::Open || self.runs.runs.contains_key(&open.run_id) {
            return;
        }
        let short_id =
            self.runs
                .ensure_thread(&open.thread_id, self.clock.now_ms, self.clock.entropy);
        let meta = open.conn.and_then(|conn| {
            self.records
                .get_mut(&open.thread_id)?
                .pending_meta
                .remove(&conn)
        });
        let run_id = open.run_id.clone();
        self.runs.next_creation_sequence = self
            .runs
            .next_creation_sequence
            .checked_add(1)
            .expect("运行创建序号已耗尽");
        let entry = self
            .runs
            .index
            .threads
            .get_mut(&short_id)
            .expect("线程索引存在");
        entry.last_run_id = Some(run_id.clone());
        entry.updated_at = self.clock.now_ms;
        if meta.is_some() {
            entry.meta = state::index_meta(meta.as_ref(), actions);
        }
        self.runs.runs.insert(
            run_id.clone(),
            RunRecord {
                pending_reroute: None,
                creation_order: Some(CreationOrder {
                    epoch: self.runs.epoch,
                    sequence: self.runs.next_creation_sequence,
                }),
                start_confirmation: StartConfirmation {
                    owned: if open.joined {
                        Some(false)
                    } else if !self.shared {
                        Some(true)
                    } else {
                        None
                    },
                    ..Default::default()
                },
                children: Vec::new(),
                children_stop: None,
                background_cleanup: None,

                run_id: run_id.clone(),
                thread_id: open.thread_id,
                short_id,
                joined: open.joined,
                state: RunState::Running,
                status: None,
                started_at: self.clock.now_ms,
                ended_at: None,
                meta,
                review_thread_id: None,
                review_short_id: None,
                input: limit_text(&open.input),
                continued_from: open.continued_from,
                interrupt_reason: None,
                stop_outcome: None,
                stop_check: None,
                loop_guard: None,
                progress: Progress::default(),
                turns: Vec::new(),
                turns_total: 0,
            },
        );
        self.runs
            .active
            .insert(run_id.clone(), ActiveRun::default());
        self.runs.updated.insert(run_id.clone(), self.clock.now_ms);
        self.runs.put(&run_id, actions);
        self.runs.put_threads(actions);
        self.runs
            .log(&run_id, self.clock.now_ms, "运行开始", &run_id, actions);
        self.runs.evict();
    }

    /// 运行的进度因 settling 缓冲超限而缺项（M7）：置 `progress.incomplete` 并按进度落盘规则写盘。
    pub(crate) fn mark_progress_incomplete(&mut self, run: &str, actions: &mut Vec<Action>) {
        if self.runs.phase != Phase::Open {
            return;
        }
        let Some(record) = self.runs.runs.get_mut(run) else {
            return;
        };
        if std::mem::replace(&mut record.progress.incomplete, true) {
            return;
        }
        self.runs.updated.insert(run.into(), self.clock.now_ms);
        self.runs.progress_changed(run, false, actions);
    }

    pub(crate) fn run_settled(&mut self, run: &str, joined: bool, actions: &mut Vec<Action>) {
        self.runs.trace("run_settled");
        if self.runs.phase != Phase::Open {
            return;
        }
        if let Some(record) = self.runs.runs.get_mut(run) {
            if matches!(record.state, RunState::Running | RunState::Stopping) {
                record.joined = joined;
                record.start_confirmation.owned = Some(!joined);
                record.start_confirmation.confirmed = !joined
                    && record.start_confirmation.thread_id.as_deref() == Some(&record.thread_id)
                    && record.start_confirmation.turn_id.is_some();
                self.runs.updated.insert(run.into(), self.clock.now_ms);
                self.runs.put(run, actions);
            }
        }
    }

    /// 只接收成功 turn/start 回包或 turn/started；请求回包必须使用原请求保存的 runId。
    pub(crate) fn run_start_evidence(
        &mut self,
        run: &str,
        thread: &str,
        turn: &str,
        actions: &mut Vec<Action>,
    ) {
        if self.runs.phase != Phase::Open || turn.is_empty() {
            return;
        }
        let Some(record) = self.runs.runs.get_mut(run) else {
            return;
        };
        if record.state == RunState::Lost
            || record.thread_id != thread
            || record.start_confirmation.confirmed
        {
            return;
        }
        let evidence = &mut record.start_confirmation;
        evidence.thread_id = Some(thread.into());
        evidence.turn_id = Some(turn.into());
        evidence.confirmed = evidence.owned == Some(true);
        self.runs.put(run, actions);
    }

    pub(crate) fn run_turn_started(
        &mut self,
        run: &str,
        thread: &str,
        turn: &str,
        actions: &mut Vec<Action>,
    ) {
        self.runs.trace("run_turn_started");
        if self.runs.phase != Phase::Open || turn.is_empty() || !self.runs.active.contains_key(run)
        {
            return;
        }
        if self.runs.find_run(thread, turn, Some(run)).as_deref() != Some(run) {
            return;
        }
        if self.runs.runs[run].turns.iter().any(|t| t.turn_id == turn) {
            return;
        }
        self.runs.link_turn(run, thread, turn, false);
        let active = self.runs.active.get_mut(run).expect("活动运行存在");
        let key = (thread.to_owned(), turn.to_owned());
        if active.current_turn.as_ref() != Some(&key) {
            // 新轮开始时淘汰旧轮暂存，哪怕旧轮完成通知缺失也不串轮。
            if self
                .runs
                .runs
                .get_mut(run)
                .expect("运行存在")
                .pending_reroute
                .take()
                .is_some()
            {
                self.runs.put(run, actions);
            }
            let active = self.runs.active.get_mut(run).expect("活动运行存在");
            active.current_turn = Some(key);
            active.snapshot = Snapshot::default();
            self.runs
                .log(run, self.clock.now_ms, "轮次开始", turn, actions);
        }
    }

    pub(crate) fn run_rerouted(&mut self, params: &Option<Value>, actions: &mut Vec<Action>) {
        if self.runs.phase != Phase::Open {
            return;
        }
        let Some(params) = params.as_ref() else {
            return;
        };
        let (Some(thread), Some(turn)) = (
            params.get("threadId").and_then(Value::as_str),
            params.get("turnId").and_then(Value::as_str),
        ) else {
            return;
        };
        // 必须已有当前轮关联；不能借当前认领或通知自报的轮次建立归属。
        let Some(run) = self.runs.find_run(thread, turn, None) else {
            return;
        };
        if !self.runs.active.get(&run).is_some_and(|active| {
            active
                .current_turn
                .as_ref()
                .is_some_and(|key| key.0 == thread && key.1 == turn)
        }) || !matches!(
            self.runs.runs[&run].state,
            RunState::Running | RunState::Stopping
        ) {
            return;
        }
        let Ok(reroute) = serde_json::from_value::<Reroute>(params.clone()) else {
            return;
        };
        let pending = PendingReroute {
            turn_id: turn.into(),
            reroute,
        };
        let record = self.runs.runs.get_mut(&run).expect("运行存在");
        if record.pending_reroute.as_ref() != Some(&pending) {
            record.pending_reroute = Some(pending);
            self.runs.put(&run, actions);
        }
    }

    pub(crate) fn run_item(
        &mut self,
        thread: &str,
        fallback_run: Option<&str>,
        method: &str,
        params: &Option<Value>,
        actions: &mut Vec<Action>,
    ) {
        self.runs.trace("run_item");
        if self.runs.phase != Phase::Open || !matches!(method, "item/started" | "item/completed") {
            return;
        }
        let Some(params) = params.as_ref() else {
            return;
        };
        let Some(turn) = params.get("turnId").and_then(Value::as_str) else {
            return;
        };
        let Some(item) = params.get("item") else {
            return;
        };
        let Some(run) = self.runs.find_run(thread, turn, fallback_run) else {
            return;
        };
        if !self.runs.active.contains_key(&run)
            || self.runs.runs[&run].turns.iter().any(|t| t.turn_id == turn)
        {
            return;
        }
        self.runs.link_turn(&run, thread, turn, false);
        let completed = method == "item/completed";
        let summary = summarize_item(item);
        let active = self.runs.active.get_mut(&run).expect("活动运行存在");
        let key = (thread.to_owned(), turn.to_owned());
        if active.current_turn.is_none() {
            active.current_turn = Some(key.clone());
        }
        // 迟到的旧轮条目不污染当前轮降级快照。
        if completed && active.current_turn.as_ref() == Some(&key) {
            active.snapshot.observe(item);
        }
        let record = self.runs.runs.get_mut(&run).expect("运行存在");
        let progress = &mut record.progress;
        let mut changed = false;
        // 完成通知可能省略命令行或工具名，不能因此遗留未确认登记。
        if completed {
            if let Some(id) = item.get("id").and_then(Value::as_str) {
                let count = progress.unconfirmed.len();
                progress.unconfirmed.retain(|entry| entry.item_id != id);
                changed = progress.unconfirmed.len() != count;
            }
        }
        if let Some(summary) = summary {
            if completed {
                // 有界的重复通知去重窗口与 done 保留窗口相同。
                if progress
                    .done
                    .iter()
                    .any(|done| done.item_id == summary.item_id)
                {
                    return;
                }
                progress
                    .unconfirmed
                    .retain(|item| item.item_id != summary.item_id);
                progress.done.push(summary.clone());
                progress.done_total = progress.done_total.saturating_add(1);
                if progress.done.len() > MAX_DONE_ITEMS {
                    progress.done.remove(0);
                }
                changed = true;
            } else if (summary.item_type == "commandExecution"
                || summary.item_type.ends_with("ToolCall"))
                && !progress
                    .unconfirmed
                    .iter()
                    .any(|item| item.item_id == summary.item_id)
                && !progress
                    .done
                    .iter()
                    .any(|item| item.item_id == summary.item_id)
            {
                if progress.unconfirmed.len() == MAX_UNCONFIRMED_ITEMS {
                    progress.unconfirmed.remove(0);
                    actions.push(Action::Warn("未确认条目超过 64 条，已丢弃最旧条目".into()));
                }
                progress.unconfirmed.push(summary.clone());
                changed = true;
            }
            let event = if completed {
                "条目完成"
            } else {
                "条目开始"
            };
            self.runs
                .log(&run, self.clock.now_ms, event, &summary.summary, actions);
        }
        if completed
            && item.get("type").and_then(Value::as_str) == Some("fileChange")
            && item.get("status").and_then(Value::as_str) == Some("completed")
        {
            let files = &mut self
                .runs
                .runs
                .get_mut(&run)
                .expect("运行存在")
                .progress
                .observed_files;
            if let Some(changes) = item.get("changes").and_then(Value::as_array) {
                for path in changes
                    .iter()
                    .filter_map(|c| c.get("path").and_then(Value::as_str))
                {
                    if files.len() >= MAX_OBSERVED_FILES {
                        break;
                    }
                    if !files.iter().any(|file| file == path) {
                        files.push(path.into());
                        changed = true;
                    }
                }
            }
        }
        if changed {
            self.runs.updated.insert(run.clone(), self.clock.now_ms);
            self.runs.progress_changed(&run, completed, actions);
        }
    }

    pub(crate) fn run_turn_completed(
        &mut self,
        thread: &str,
        fallback_run: Option<&str>,
        turn: &Value,
        actions: &mut Vec<Action>,
    ) {
        self.runs.trace("run_turn_completed");
        if self.runs.phase != Phase::Open {
            return;
        }
        let Some(id) = turn
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
        else {
            return;
        };
        let Some(run) = self.runs.find_run(thread, id, fallback_run) else {
            return;
        };
        if self.runs.runs[&run].turns.iter().any(|t| t.turn_id == id)
            || self.runs.turn_runs.get(thread).is_some_and(|links| {
                links
                    .iter()
                    .any(|l| l.turn == id && l.run == run && l.completed)
            })
        {
            return;
        }
        let Some(status) = turn
            .get("status")
            .and_then(Value::as_str)
            .and_then(turn_status)
        else {
            actions.push(Action::Warn(
                "轮次完成通知缺少合法终态，保留未知状态".into(),
            ));
            self.runs.guard_evidence.push((run, id.into(), None));
            return;
        };
        self.runs.link_turn(&run, thread, id, true);
        let mut snapshot = if let Some(active) = self.runs.active.get_mut(&run) {
            active.completed_after_stop = active.stop_started;
            if active
                .current_turn
                .as_ref()
                .is_none_or(|key| key.0 == thread && key.1 == id)
            {
                active.current_turn = None;
                std::mem::take(&mut active.snapshot)
            } else {
                Snapshot::default()
            }
        } else {
            Snapshot::default()
        };
        let full = full_items(turn);
        if let Some(items) = full {
            self.runs
                .guard_evidence
                .push((run.clone(), id.into(), Some(Activity::of(items))));
        }
        let output = full.and_then(output_from_items);
        let pending = output.is_none();
        if full.is_none() {
            if let Some(items) = turn.get("items").and_then(Value::as_array) {
                snapshot.observe_summary(items);
            }
        }
        let (output, output_source) = output.unwrap_or_else(|| (String::new(), OutputSource::None));
        let record = self.runs.runs.get_mut(&run).expect("运行存在");
        let turn_index = usize::try_from(record.turns_total).expect("轮次总数超出本机容量");
        record.turns_total = record.turns_total.checked_add(1).expect("轮次总数已耗尽");
        // 迟到的旧轮完成不消费当前轮改道；对应轮完成则转入结果并清空暂存。
        let reroute = if record
            .pending_reroute
            .as_ref()
            .is_some_and(|p| p.turn_id == id)
        {
            record.pending_reroute.take().map(|p| p.reroute)
        } else {
            None
        };
        record.turns.push(TurnResult {
            reroute,
            turn_id: id.into(),
            ready: if pending {
                Ready::Pending
            } else {
                Ready::Ready
            },
            status,
            output,
            output_source,
            error: turn.get("error").filter(|e| !e.is_null()).cloned(),
            completed_at: self.clock.now_ms,
        });
        self.runs.trim_turns(&run);
        if !pending {
            self.runs.notice_turn(&run, turn_index as u64);
        }
        if pending {
            self.runs.queue_read(
                ResultRead {
                    run_id: run.clone(),
                    thread_id: thread.into(),
                    turn_id: id.into(),
                    turn_index,
                    attempt: 0,
                    generation: 0,
                },
                snapshot,
                false,
                // completed 自带完整条目时证据已交出，补读只为取输出。
                full.is_some(),
                actions,
            );
        }
        self.runs.put(&run, actions);
        self.runs.touch(&run, self.clock.now_ms, actions);
        self.runs
            .log(&run, self.clock.now_ms, "轮次完成", id, actions);
        self.runs.evict();
    }

    pub(crate) fn run_start_failed(&mut self, run: &str, error: &Value, actions: &mut Vec<Action>) {
        self.runs.trace("run_start_failed");
        if self.runs.phase != Phase::Open || !self.runs.active.contains_key(run) {
            return;
        }
        let record = self.runs.runs.get_mut(run).expect("运行存在");
        if record.turns.iter().any(|t| t.turn_id.is_empty()) {
            return;
        }
        record.turns_total = record.turns_total.checked_add(1).expect("轮次总数已耗尽");
        record.turns.push(TurnResult {
            reroute: None,
            turn_id: String::new(),
            ready: Ready::Ready,
            status: TurnStatus::Failed,
            output: String::new(),
            output_source: OutputSource::None,
            error: Some(error.clone()),
            completed_at: self.clock.now_ms,
        });
        let turn_index = record.turns_total - 1;
        self.runs.trim_turns(run);
        self.runs.notice_turn(run, turn_index);
        self.runs.put(run, actions);
        self.runs.touch(run, self.clock.now_ms, actions);
        self.runs.log(
            run,
            self.clock.now_ms,
            "启动失败",
            error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("启动请求失败"),
            actions,
        );
    }

    pub(crate) fn run_review_moved(
        &mut self,
        run: &str,
        child: &str,
        owner: Option<ConnId>,
        actions: &mut Vec<Action>,
    ) {
        self.runs.trace("run_review_moved");
        if self.runs.phase != Phase::Open
            || !self.runs.active.contains_key(run)
            || self.runs.runs[run].thread_id == child
        {
            return;
        }
        if let Some(previous) = self.runs.runs[run].review_thread_id.as_deref() {
            if previous != child {
                actions.push(Action::Warn(
                    "审查运行已有子线程，忽略冲突的转移回包".into(),
                ));
            }
            return;
        }
        let inherited = self
            .runs
            .index
            .threads
            .values()
            .find(|entry| entry.thread_id == self.runs.runs[run].thread_id)
            .and_then(|entry| entry.resume_overrides.clone());
        let short = self
            .runs
            .ensure_thread(child, self.clock.now_ms, self.clock.entropy);
        let child_entry = self
            .runs
            .index
            .threads
            .get_mut(&short)
            .expect("子线程索引存在");
        if child_entry.resume_overrides.is_none() {
            child_entry.resume_overrides = inherited;
        }
        let record = self.runs.runs.get_mut(run).expect("运行存在");
        record.review_thread_id = Some(child.into());
        record.review_short_id = Some(short.clone());
        self.runs
            .index
            .threads
            .get_mut(&short)
            .expect("子线程索引存在")
            .last_run_id = Some(run.into());
        self.runs.put(run, actions);
        self.runs.touch(run, self.clock.now_ms, actions);
        let parent = &self.runs.runs[run].thread_id;
        if child != parent {
            self.notify_thread_ext(parent, owner, "broker/run/moved",
                serde_json::json!({"threadId":parent,"runId":run,"reviewThreadId":child,"reviewShortId":short}), actions);
        }
    }

    pub(crate) fn run_goal_status(&mut self, thread: &str, status: Option<&str>) {
        self.runs.trace("run_goal_status");
        if self.runs.phase != Phase::Open {
            return;
        }
        let known = self
            .runs
            .index
            .threads
            .values()
            .any(|e| e.thread_id == thread)
            || self.runs.active.keys().any(|id| {
                self.runs.runs.get(id).is_some_and(|r| {
                    r.thread_id == thread || r.review_thread_id.as_deref() == Some(thread)
                })
            });
        if !known {
            return;
        }
        if let Some(status) = status {
            self.runs.goals.insert(thread.into(), status.into());
        } else {
            self.runs.goals.remove(thread);
        }
    }

    pub(crate) fn run_stop_began(
        &mut self,
        run: &str,
        reason: StopReason,
        actions: &mut Vec<Action>,
    ) {
        self.runs.trace("run_stop_began");
        if self.runs.phase != Phase::Open {
            return;
        }
        let Some(record) = self.runs.runs.get_mut(run) else {
            return;
        };
        if record.state != RunState::Running {
            return;
        }
        let thread = record
            .review_thread_id
            .as_ref()
            .unwrap_or(&record.thread_id);
        let goal_active = self
            .runs
            .goals
            .get(thread)
            .map(|status| status == "active")
            .unwrap_or_else(|| self.records.get(thread).is_some_and(|r| r.goal_active));
        record.state = RunState::Stopping;
        record.interrupt_reason = Some(match reason {
            StopReason::ClientDisconnected => InterruptReason::ClientDisconnected,
            StopReason::Stop => InterruptReason::Stop,
            StopReason::GoalLoop => InterruptReason::GoalLoop,
        });
        if let Some(active) = self.runs.active.get_mut(run) {
            active.stop_started = true;
            active.goal_active_at_stop = goal_active;
            active.completed_after_stop = false;
        }
        self.runs.updated.insert(run.into(), self.clock.now_ms);
        self.runs.put(run, actions);
        self.runs
            .log(run, self.clock.now_ms, "停止发起", "等待停止证据", actions);
    }

    /// 空转刹车触发（在发起停止之前调用）：记下触发详情，同一运行只记第一次。
    pub(crate) fn run_loop_guard(
        &mut self,
        run: &str,
        report: LoopGuardReport,
        actions: &mut Vec<Action>,
    ) {
        self.runs.trace("run_loop_guard");
        if self.runs.phase != Phase::Open {
            return;
        }
        let Some(record) = self.runs.runs.get_mut(run) else {
            return;
        };
        if record.loop_guard.is_some() {
            return;
        }
        let detail = format!(
            "{:?}：{} 轮（阈值 {}）",
            report.kind, report.count, report.threshold
        );
        record.loop_guard = Some(report);
        self.runs.updated.insert(run.into(), self.clock.now_ms);
        self.runs.put(run, actions);
        self.runs
            .log(run, self.clock.now_ms, "空转刹车", &detail, actions);
    }

    /// 共享模式刹车的暂停回包：运行可能已经结束，照样更新记录；已确认暂停的不再被迟到失败改写。
    pub(crate) fn run_loop_guard_pause(
        &mut self,
        run: &str,
        outcome: PauseOutcome,
        actions: &mut Vec<Action>,
    ) {
        self.runs.trace("run_loop_guard_pause");
        if self.runs.phase != Phase::Open {
            return;
        }
        let Some(report) = self
            .runs
            .runs
            .get_mut(run)
            .and_then(|record| record.loop_guard.as_mut())
        else {
            return;
        };
        if report.pause == Some(PauseOutcome::Paused) || report.pause == Some(outcome) {
            return;
        }
        report.pause = Some(outcome);
        self.runs.updated.insert(run.into(), self.clock.now_ms);
        self.runs.put(run, actions);
        self.runs.log(
            run,
            self.clock.now_ms,
            "空转刹车暂停目标",
            &format!("{outcome:?}"),
            actions,
        );
    }

    pub(crate) fn run_stop_check(
        &mut self,
        run: &str,
        check: StopCheck,
        actions: &mut Vec<Action>,
    ) {
        self.runs.trace("run_stop_check");
        if self.runs.phase != Phase::Open {
            return;
        }
        let Some(record) = self.runs.runs.get_mut(run) else {
            return;
        };
        if record.state == RunState::Ended || record.stop_outcome == Some(StopOutcome::Confirmed) {
            return;
        }
        if record.stop_outcome != Some(StopOutcome::Unconfirmed) || record.stop_check != Some(check)
        {
            record.stop_outcome = Some(StopOutcome::Unconfirmed);
            record.stop_check = Some(check);
            self.runs.updated.insert(run.into(), self.clock.now_ms);
            self.runs.put(run, actions);
        }
    }

    pub(crate) fn run_stop_confirmed(&mut self, run: &str, actions: &mut Vec<Action>) {
        self.runs.trace("run_stop_confirmed");
        if self.runs.phase != Phase::Open {
            return;
        }
        let Some(record) = self.runs.runs.get_mut(run) else {
            return;
        };
        if record.stop_outcome == Some(StopOutcome::Confirmed) && record.stop_check.is_none() {
            return;
        }
        record.stop_outcome = Some(StopOutcome::Confirmed);
        record.stop_check = None;
        self.runs.verifying_history.remove(run);
        self.runs.updated.insert(run.into(), self.clock.now_ms);
        self.runs.put(run, actions);
        self.runs.log(
            run,
            self.clock.now_ms,
            "停止确认",
            "唯一停止判定通过",
            actions,
        );
        self.runs.evict();
    }

    pub(crate) fn run_released_owned(
        &mut self,
        run: &str,
        kind: ReleaseKind,
        owner: Option<ConnId>,
        actions: &mut Vec<Action>,
    ) {
        let before = self.runs.notices.len();
        self.run_released(run, kind, actions);
        for notice in &mut self.runs.notices[before..] {
            if let RunNotice::RunEnded {
                owner: recipient, ..
            } = notice
            {
                *recipient = owner;
            }
        }
    }
    pub(crate) fn run_released(&mut self, run: &str, kind: ReleaseKind, actions: &mut Vec<Action>) {
        self.runs.trace("run_released");
        if self.runs.phase != Phase::Open {
            return;
        }
        let Some(record) = self.runs.runs.get_mut(run) else {
            return;
        };
        if matches!(record.state, RunState::Lost | RunState::Ended) || kind == ReleaseKind::Shutdown
        {
            return;
        }
        let active = self.runs.active.remove(run).unwrap_or_default();
        let last = record.turns.last().map(|t| t.status);
        let thread = record
            .review_thread_id
            .as_ref()
            .unwrap_or(&record.thread_id);
        let status = if self.runs.goals.get(thread).is_some_and(|status| {
            matches!(
                status.as_str(),
                "blocked" | "budgetLimited" | "usageLimited"
            )
        }) {
            RunStatus::GoalBlocked
        } else {
            match kind {
                ReleaseKind::Natural => last.map(run_status).unwrap_or(RunStatus::Failed),
                ReleaseKind::Joined => last.map(run_status).unwrap_or(RunStatus::Interrupted),
                ReleaseKind::StartFailed => RunStatus::Failed,
                ReleaseKind::StopConfirmed
                    if active.completed_after_stop
                        && !active.goal_active_at_stop
                        && last == Some(TurnStatus::Completed) =>
                {
                    record.interrupt_reason = None;
                    RunStatus::Completed
                }
                ReleaseKind::StopConfirmed | ReleaseKind::Shutdown => RunStatus::Interrupted,
            }
        };
        record.state = RunState::Ended;
        record.status = Some(status);
        record.ended_at = Some(self.clock.now_ms);
        self.runs.notices.push(RunNotice::RunEnded {
            thread_id: record.thread_id.clone(),
            run_id: run.into(),
            owner: None,
        });
        // 已发补读和迟到轮次的归属必须跨释放保留；当前快照、进度代号已随 active 删除。
        self.runs.put(run, actions);
        self.runs.touch(run, self.clock.now_ms, actions);
        self.runs
            .log(run, self.clock.now_ms, "运行结束", run, actions);
        self.runs.evict();
    }

    /// SA-a：运行记录的 children（子线程、直接父线程、已绑定轮次、证据与最终活动）变化时写盘。
    pub(crate) fn run_set_children(
        &mut self,
        run: &str,
        children: Vec<crate::relations::ChildRecord>,
        actions: &mut Vec<Action>,
    ) {
        let Some(record) = self.runs.runs.get_mut(run) else {
            return;
        };
        if record.children == children {
            return;
        }
        record.children = children;
        self.runs.put(run, actions);
    }

    pub(crate) fn run_learn_thread(
        &mut self,
        thread: &str,
        response: &Value,
        learn_model: bool,
        actions: &mut Vec<Action>,
    ) {
        self.runs.trace("run_learn_thread");
        if self.runs.phase != Phase::Open {
            return;
        }
        let Some(info) = response.get("thread").filter(|t| t.is_object()) else {
            return;
        };
        let Some(id) = info
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
        else {
            return;
        };
        let _ = thread;
        let short = self
            .runs
            .ensure_thread(id, self.clock.now_ms, self.clock.entropy);
        let entry = self
            .runs
            .index
            .threads
            .get_mut(&short)
            .expect("线程索引存在");
        if learn_model {
            entry.model = response
                .get("model")
                .and_then(Value::as_str)
                .map(str::to_owned);
        }
        entry.preview = info
            .get("preview")
            .and_then(Value::as_str)
            .map(state::index_preview);
        entry.cwd = info.get("cwd").and_then(Value::as_str).map(str::to_owned);
        entry.updated_at = self.clock.now_ms;
        self.runs.put_threads(actions);
    }

    pub(crate) fn run_forget_thread(&mut self, thread: &str, actions: &mut Vec<Action>) {
        self.runs.trace("run_forget_thread");
        if self.runs.phase != Phase::Open {
            return;
        }
        self.runs
            .index
            .threads
            .retain(|_, entry| entry.thread_id != thread);
        self.runs
            .index
            .removed
            .insert(thread.into(), self.clock.now_ms);
        self.runs.put_threads(actions);
        self.runs.evict();
    }

    pub(crate) fn run_result_reply(
        &mut self,
        read: ResultRead,
        result: Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) {
        self.runs.trace("run_result_reply");
        self.runs
            .read_reply(read, result, self.clock.now_ms, actions);
    }

    pub(crate) fn run_timer(&mut self, timer: RunTimer, actions: &mut Vec<Action>) {
        self.runs.trace("run_timer");
        if let RunTimer::Purge { generation } = timer {
            self.run_purge_poll(generation, actions);
            return;
        }
        if self.runs.phase != Phase::Open {
            return;
        }
        match timer {
            RunTimer::Purge { .. } => unreachable!("删除回包已单独处理"),
            RunTimer::ResultRetry(read) => self.runs.retry_read(read, actions),
            RunTimer::ProgressFlush { run_id, generation } => {
                if let Some(active) = self.runs.active.get_mut(&run_id) {
                    if active.progress_generation == Some(generation) {
                        active.progress_generation = None;
                        active.completed_since_flush = 0;
                        self.runs.put(&run_id, actions);
                    }
                }
            }
            RunTimer::DailyCleanup if self.runs.cleanup_scheduled => {
                self.runs.cleanup_index(self.clock.now_ms, actions);
                self.runs.evict();
                schedule("", RunTimer::DailyCleanup, CLEANUP_INTERVAL_MS, actions);
            }
            RunTimer::DailyCleanup => {}
        }
    }

    pub(crate) fn restore_runs(
        &mut self,
        index: ThreadIndexFile,
        runs: Vec<RunRecord>,
        actions: &mut Vec<Action>,
    ) {
        self.runs.trace("restore_runs");
        if self.runs.phase != Phase::Open || self.runs.restored {
            return;
        }
        self.runs.restored = true;
        self.runs.index = index;
        // 旧索引及 store 重建的条目也遵守字段预算，先规范化再产生任何写盘动作。
        for entry in self.runs.index.threads.values_mut() {
            entry.preview = entry.preview.as_deref().map(state::index_preview);
            entry.meta = state::index_meta(entry.meta.as_ref(), actions);
        }
        self.runs.cleanup_index(self.clock.now_ms, actions);
        // store 可能已用运行记录重建了索引（7.2「跨文件一致性」），重建结果只在内存里：
        // 启动时无论是否清理过都写回一次，让 threads.json 与 runs 对齐。
        if !actions
            .iter()
            .any(|a| matches!(a, Action::Store(crate::store::StoreOp::PutThreads(_))))
        {
            self.runs.put_threads(actions);
        }
        let mut ids = Vec::new();
        let mut runs = runs;
        runs.sort_by(|a, b| (a.started_at, &a.run_id).cmp(&(b.started_at, &b.run_id)));
        let loaded_ids: std::collections::HashSet<_> =
            runs.iter().map(|r| r.run_id.as_str()).collect();
        // 索引可能仍引用已不在加载结果中的运行，保留其所属线程。
        let mut indexed: Vec<_> = self
            .runs
            .index
            .threads
            .values()
            .filter_map(|e| {
                e.last_run_id
                    .as_ref()
                    .filter(|id| !loaded_ids.contains(id.as_str()))
                    .map(|id| (e.updated_at, id.clone(), e.thread_id.clone()))
            })
            .collect();
        indexed.sort();
        for (_, id, thread) in indexed {
            self.runs.retire(id, thread);
        }
        for mut record in runs {
            let before = record.clone();
            if record.state == RunState::Running {
                record.state = RunState::Lost;
                record.interrupt_reason = Some(InterruptReason::BrokerLost);
                record.stop_outcome = Some(StopOutcome::Unconfirmed);
            } else if record.state == RunState::Stopping {
                record.state = RunState::Lost;
                record.stop_outcome = Some(StopOutcome::Unconfirmed);
            }
            record.input = limit_text(&record.input);
            for turn in &mut record.turns {
                turn.output = limit_text(&turn.output);
            }
            record.turns_total = record.turns_total.max(record.turns.len() as u64);
            record.progress.done_total = record
                .progress
                .done_total
                .max(record.progress.done.len() as u64);
            let excess = record.progress.done.len().saturating_sub(MAX_DONE_ITEMS);
            record.progress.done.drain(..excess);
            let excess = record
                .progress
                .unconfirmed
                .len()
                .saturating_sub(MAX_UNCONFIRMED_ITEMS);
            if excess > 0 {
                actions.push(Action::Warn(
                    "恢复的未确认条目超过 64 条，已丢弃最旧条目".into(),
                ));
            }
            record.progress.unconfirmed.drain(..excess);
            let mut files = std::collections::HashSet::new();
            record
                .progress
                .observed_files
                .retain(|path| files.len() < MAX_OBSERVED_FILES && files.insert(path.clone()));
            let id = record.run_id.clone();
            self.runs.updated.insert(
                id.clone(),
                record
                    .ended_at
                    .unwrap_or(record.started_at)
                    .max(record.turns.last().map_or(0, |t| t.completed_at)),
            );
            let changed = record != before;
            self.runs.runs.insert(id.clone(), record);
            self.runs.trim_turns(&id);
            if changed || self.runs.runs[&id].turns.len() != before.turns.len() {
                self.runs.put(&id, actions);
            }
            ids.push(id);
        }
        // 所有记录先入账，再逐项提交；排队期间不会被淘汰。
        ids.sort();
        for id in ids {
            let record = &self.runs.runs[&id];
            let thread = record
                .review_thread_id
                .as_ref()
                .unwrap_or(&record.thread_id)
                .clone();
            let base = record.turns_total.saturating_sub(record.turns.len() as u64);
            let turns: Vec<_> = record
                .turns
                .iter()
                .enumerate()
                .map(|(i, t)| (base + i as u64, t.turn_id.clone(), t.ready))
                .collect();
            for (index, turn_id, ready) in turns {
                self.runs.link_turn(&id, &thread, &turn_id, true);
                if ready == Ready::Pending {
                    self.runs.queue_read(
                        ResultRead {
                            run_id: id.clone(),
                            thread_id: thread.clone(),
                            turn_id,
                            turn_index: usize::try_from(index).expect("轮次索引超出本机容量"),
                            attempt: 0,
                            generation: 0,
                        },
                        Snapshot::default(),
                        true,
                        // 重启前的运行没有刹车状态。
                        true,
                        actions,
                    );
                }
            }
            // 队列满时可能立即 unavailable，必须投递最新状态。
            if self.runs.runs[&id]
                .turns
                .iter()
                .any(|t| t.ready == Ready::Unavailable)
            {
                self.runs.put(&id, actions);
            }
        }
        self.runs.cleanup_scheduled = true;
        schedule("", RunTimer::DailyCleanup, CLEANUP_INTERVAL_MS, actions);
        self.runs.evict();
        self.lifecycle_recompute_gates();
    }

    pub(crate) fn shutdown_runs(&mut self, actions: &mut Vec<Action>) {
        self.runs.trace("shutdown_runs");
        if self.runs.phase != Phase::Open {
            return;
        }
        self.runs.phase = Phase::Draining;
        self.runs.active.clear();
        self.runs.cleanup_scheduled = false;
        let mut changed = Vec::new();
        for record in self.runs.runs.values_mut() {
            if matches!(record.state, RunState::Running | RunState::Stopping) {
                record.state = RunState::Lost;
                record
                    .interrupt_reason
                    .get_or_insert(InterruptReason::BrokerLost);
                record.stop_outcome = Some(StopOutcome::Unconfirmed);
                changed.push(record.run_id.clone());
            }
        }
        for run in changed {
            self.runs.put(&run, actions);
        }
    }

    pub(crate) fn finalize_runs(&mut self, actions: &mut Vec<Action>) {
        self.runs.trace("finalize_runs");
        if self.runs.phase == Phase::Finalized {
            return;
        }
        self.runs.finalize_pending(actions);
        self.runs.phase = Phase::Finalized;
        self.runs.active.clear();
        self.runs.turn_runs.clear();
        self.runs.goals.clear();
        self.runs.cleanup_scheduled = false;
        self.runs.evict();
    }

    pub(crate) fn result_reads_pending(&self) -> bool {
        !self.runs.reads.is_empty()
    }
    pub(crate) fn run_snapshot(&self, run: &str) -> Option<Value> {
        self.runs
            .runs
            .get(run)
            .and_then(|record| serde_json::to_value(record).ok())
    }
    pub(crate) fn latest_run(&self, thread: &str) -> Option<&RunRecord> {
        self.runs
            .index
            .threads
            .values()
            .find(|entry| entry.thread_id == thread)
            .and_then(|entry| entry.last_run_id.as_ref())
            .and_then(|run| self.runs.runs.get(run))
    }

    /// 内存账本里属于该线程（或以它为父线程的 review）的指定运行；已淘汰或不属于时为 None。
    pub(crate) fn thread_run(&self, thread: &str, run: &str) -> Option<&RunRecord> {
        self.runs
            .runs
            .get(run)
            .filter(|r| r.thread_id == thread || r.review_thread_id.as_deref() == Some(thread))
    }
}

fn turn_status(status: &str) -> Option<TurnStatus> {
    match status {
        "completed" => Some(TurnStatus::Completed),
        "interrupted" => Some(TurnStatus::Interrupted),
        "failed" => Some(TurnStatus::Failed),
        _ => None,
    }
}
fn run_status(status: TurnStatus) -> RunStatus {
    match status {
        TurnStatus::Completed => RunStatus::Completed,
        TurnStatus::Interrupted => RunStatus::Interrupted,
        TurnStatus::Failed => RunStatus::Failed,
    }
}

impl Machine {
    /// SA-b：原子写报告和聚合停止账本，再唯一提交 ended；严禁调用普通终态映射。
    /// 终态与原因由树协调器按第 8.4 节优先级算好（见 `stop::tree::commit_status`），这里只落账：
    /// 同一次写盘里 childrenStop、stopOutcome/stopCheck 与 ended 一起出现，不存在「已 ended 而无报告」的记录。
    pub fn commit_tree_finish(
        &mut self,
        commit: crate::stop::tree::TreeFinishCommit,
        actions: &mut Vec<Action>,
    ) {
        self.runs.trace("commit_tree_finish");
        if self.runs.phase != Phase::Open {
            return;
        }
        let run = commit.snapshot.identity.run_id.clone();
        let check = self.tree_check(commit.report.tree_op);
        let owner = match &commit.snapshot.owner {
            Some(crate::claim::Owner::Client(conn)) => {
                let thread = &commit.snapshot.identity.execution_thread_id;
                let gone = self.claims.get(thread).is_some_and(|c| c.owner_gone);
                (!gone && self.live_conns.contains(conn)).then_some(*conn)
            }
            _ => None,
        };
        let Some(record) = self.runs.runs.get_mut(&run) else {
            return;
        };
        if matches!(record.state, RunState::Ended | RunState::Lost) {
            return;
        }
        let confirmed = commit.report.children.phase == crate::stop::tree::TreePhase::Confirmed;
        self.runs.active.remove(&run);
        record.children_stop = Some(commit.report.children.clone());
        record.stop_outcome = Some(if confirmed {
            StopOutcome::Confirmed
        } else {
            StopOutcome::Unconfirmed
        });
        record.stop_check = (!confirmed).then_some(check);
        record.state = RunState::Ended;
        record.status = Some(commit.status);
        record.interrupt_reason = commit.interrupt_reason;
        record.ended_at = Some(self.clock.now_ms);
        let thread = record.thread_id.clone();
        if confirmed {
            self.runs.verifying_history.remove(&run);
        }
        self.runs.notices.push(RunNotice::RunEnded {
            thread_id: thread,
            run_id: run.clone(),
            owner,
        });
        self.runs.put(&run, actions);
        self.runs.touch(&run, self.clock.now_ms, actions);
        let detail = format!(
            "{run}（子代理树 {:?}，停止 {:?}）",
            commit.report.children.trigger, commit.report.children.phase
        );
        self.runs
            .log(&run, self.clock.now_ms, "运行结束", &detail, actions);
        self.runs.evict();
    }

    /// SA-b：树停止报告更新（提交前与提交后的核实都走这里，不改 ended、不发 RunEnded）。
    /// confirmed 时 stopOutcome=confirmed、stopCheck 清空；首次期限后未确认写 unconfirmed 与当前聚合检查；
    /// pending 只保留当前检查，不宣告结论。
    pub(crate) fn run_tree_report(
        &mut self,
        run: &str,
        report: crate::stop::tree::ChildrenStopReport,
        check: StopCheck,
        actions: &mut Vec<Action>,
    ) {
        self.runs.trace("run_tree_report");
        if self.runs.phase != Phase::Open {
            return;
        }
        let Some(record) = self.runs.runs.get_mut(run) else {
            return;
        };
        use crate::stop::tree::TreePhase;
        let (outcome, stop_check) = match report.phase {
            TreePhase::Confirmed => (Some(StopOutcome::Confirmed), None),
            TreePhase::Unconfirmed => (Some(StopOutcome::Unconfirmed), Some(check)),
            TreePhase::Pending => (record.stop_outcome, Some(check)),
        };
        if record.children_stop.as_ref() == Some(&report)
            && record.stop_outcome == outcome
            && record.stop_check == stop_check
        {
            return;
        }
        record.children_stop = Some(report);
        record.stop_outcome = outcome;
        record.stop_check = stop_check;
        if outcome == Some(StopOutcome::Confirmed) {
            self.runs.verifying_history.remove(run);
        }
        self.runs.updated.insert(run.into(), self.clock.now_ms);
        self.runs.put(run, actions);
    }
}

impl Machine {
    pub(crate) fn publish_background(
        &mut self,
        op: &crate::stop::StopOp,
        actions: &mut Vec<Action>,
    ) {
        let Some(id) = &op.run_id else {
            return;
        };
        let Some(run) = self.runs.runs.get_mut(id) else {
            return;
        };
        if run.background_cleanup.as_ref() == Some(&op.background.report) {
            return;
        }
        run.background_cleanup = Some(op.background.report.clone());
        if run.stop_outcome == Some(StopOutcome::Confirmed) && !op.background.confirmed {
            run.stop_outcome = Some(StopOutcome::Unconfirmed);
        }
        if op.already_stopped && op.background.confirmed {
            run.stop_outcome = Some(StopOutcome::Confirmed);
            self.runs.verifying_history.remove(id);
        }
        self.runs.put(id, actions);
    }
}
