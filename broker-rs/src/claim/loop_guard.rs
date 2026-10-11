//! 自动续跑空转刹车（S1-评审第 26 节）。
//!
//! 兼容旧的 AwaitingContinuation 自动候选路径；SA1b 启用后另纳入已确认自有汇总轮：
//! - 刹车一：连续 [`NO_ACTION_LIMIT`] 个证据完整的自动轮都没有外部动作；
//! - 刹车二：本次运行合格的自动轮（结算时不含用户输入；共享模式还要求证据完整）达到 [`CONTINUATION_LIMIT`]。
//!
//! 证据只认完整条目（completed 自带 full，或结果补读读回的 full 轮次，经 `Runs.guard_evidence` 交回）；
//! 条目通知只能证明「有」动作或用户输入，不能证明「没有」。按开始顺序结算，前面的轮证据未到时后面的排队。
//! 用户输入是立即生效的否决：一看到（条目通知或完整证据）就清空无动作计数、丢掉排在它之前的候选，
//! 共享模式还当场撤销控制资格，不等按序结算。goal 身份或状态真正变化只清除旧结算计数及连续无动作计数；
//! SA1b 的 autoTurnsStarted、输入能力和预算独立保留，最后获准的第 50 轮可完成最终核实。
//!
//! 触发后：
//! - 私有模式及已启用、授权仍有效的 SA1b 运行走现有停止流程（`StopReason::GoalLoop`）；
//! - 旧共享模式只对本次运行控制的 goal（`ThreadRecord.goal_controlled`）发 `thread/goal/set paused`，
//!   不中断任何轮、不重试：正在跑的轮可能是别的客户端的。控制资格只来自本 broker 的客户端把 goal
//!   设成 active、#2e 恢复成功（回包身份与当前事实一致，且发出后资格代号未变），或已知 goal 在自有的
//!   客户端轮内换成新的（模型自己建 goal）；goal 变为非 active、自动轮出现用户输入时撤销并推进代号，
//!   身份被外部替换时撤销。暂停回包按版本与身份核对：发出后 goal 事实已变的旧回包不学习。
//!   已发出的暂停无法阻止它影响服务器上的并发接管，只保证本地不采纳旧证据、不再发后续动作。

use super::*;
use crate::results::Activity;
use crate::runs::{LoopGuardAction, LoopGuardKind, LoopGuardReport, PauseOutcome};

/// 刹车一：连续几个无动作的自动轮。
pub const NO_ACTION_LIMIT: usize = 3;
/// 刹车二：每次运行合格自动轮的上限。
pub const CONTINUATION_LIMIT: u32 = 50;
/// 等证据的自动轮最多排几轮；超出时最早的一轮按「不计入」结算并告警。
pub const MAX_PENDING_TURNS: usize = 8;
/// 每次运行记住的最近自动候选轮次（判断「正在跑的是不是客户端轮」、认出被挤出队列的轮）。
/// 证据还没到就被挤出的轮无法再认出，按「可能有用户输入」保守处理。
pub const MAX_AUTO_TURNS: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingTurn {
    turn: String,
    /// 开始序号（本运行内递增），用户输入否决按它划分前后。
    seq: u64,
    /// 条目通知里见到的事实，只取「有」的一面。
    seen: Activity,
    /// 完整条目的归类：外层 None 表示证据还没到，内层 None 表示确定拿不到完整条目。
    evidence: Option<Option<Activity>>,
}

/// 一次运行的刹车状态；认领不再属于该运行时删除。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Guard {
    /// SA1b 的运行级能力与水位；goal 变化不能清除，历史恢复不能重建。
    pub(crate) safeguards: Option<super::loop_guard_input::Safeguards>,
    pub thread: String,
    /// 本次运行合格的自动轮数（刹车二），结算时累计。
    pub continuations: u32,
    /// 按结算顺序连续无动作的自动轮（刹车一），带开始序号。
    idle: Vec<(u64, String)>,
    pending: VecDeque<PendingTurn>,
    /// 最近的自动候选轮次；goal 变化清零计数时保留，轮次来源不从等证据队列反推。
    auto_turns: VecDeque<AutoTurn>,
    next_seq: u64,
    /// 已触发过；同一运行只刹一次。
    pub tripped: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AutoTurn {
    turn: String,
    seq: u64,
    /// 已因用户输入否决过：同一轮的通知与补读不重复否决、不重复撤销资格。
    vetoed: bool,
    /// 完整证据或「确定拿不到」已经交回过。
    evidence_seen: bool,
}

/// 正在处理的那次授予写入回包的意图（解包时取出，同一次回包处理里用完，step 末尾清空）：
/// 线程、写入发出时的资格代号、写入请求的目标原文。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantIntent {
    pub thread: String,
    pub epoch: u64,
    pub objective: Option<String>,
}

/// 共享模式暂停请求所针对的 goal：刹车时受控的那个；`version` 是请求发出时的 goal 事实版本
/// （由 `track_goal_requests` 填写），回包按它判断事实是否已变。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PauseTarget {
    pub run_id: RunId,
    pub thread: String,
    pub objective: Option<String>,
    pub created: Option<String>,
    pub version: u64,
}

impl Guard {
    pub(crate) fn new(thread: &str) -> Self {
        Self {
            safeguards: None,
            thread: thread.into(),
            continuations: 0,
            idle: Vec::new(),
            pending: VecDeque::new(),
            auto_turns: VecDeque::new(),
            next_seq: 0,
            tripped: false,
        }
    }

    /// goal 真正变化：计数整体清零，自动候选的来源记录与序号保留。
    fn reset(&mut self) {
        let auto_turns = std::mem::take(&mut self.auto_turns);
        let next_seq = self.next_seq;
        let safeguards = self.safeguards.take();
        *self = Self::new(&self.thread);
        self.safeguards = safeguards;
        self.auto_turns = auto_turns;
        self.next_seq = next_seq;
    }

    fn is_auto(&self, turn: &str) -> bool {
        self.auto_turns.iter().any(|t| t.turn == turn)
    }

    /// 用户输入否决（同一轮只生效一次，返回是否这次才生效）：只清掉开始早于该轮（含该轮）的
    /// 无动作计数与等证据候选，之后开始的轮照常计数。
    fn veto(&mut self, turn: &str) -> bool {
        let Some(entry) = self
            .auto_turns
            .iter_mut()
            .find(|t| t.turn == turn && !t.vetoed)
        else {
            return false;
        };
        entry.vetoed = true;
        let seq = entry.seq;
        self.idle.retain(|(s, _)| *s > seq);
        while self.pending.front().is_some_and(|p| p.seq <= seq) {
            self.pending.pop_front();
        }
        true
    }

    /// 按开始顺序结算证据已到的轮，给出应触发的刹车（动作由触发方填写）。
    fn settle(&mut self, shared: bool) -> Option<LoopGuardReport> {
        while self.pending.front().is_some_and(|p| p.evidence.is_some()) {
            let turn = self.pending.pop_front().expect("队首存在");
            let seen = turn.seen;
            let (activity, complete) = match turn.evidence.expect("证据已到") {
                Some(mut full) => {
                    full.action |= seen.action;
                    full.user_input |= seen.user_input;
                    full.special |= seen.special;
                    (full, true)
                }
                // 拿不到完整条目：通知证明有的照用，否则既不加一也不清零。
                None => (
                    Activity {
                        unknown: true,
                        ..seen
                    },
                    false,
                ),
            };
            if activity.user_input {
                // 否决通常已在看到用户输入时生效（幂等）；这里只保证不把它算作自动轮。
                self.veto(&turn.turn);
                continue;
            }
            if activity.action || activity.special {
                self.idle.clear();
            } else if !activity.unknown {
                self.idle.push((turn.seq, turn.turn.clone()));
                if self.idle.len() >= NO_ACTION_LIMIT {
                    return Some(report(
                        LoopGuardKind::NoAction,
                        self.idle.len() as u32,
                        NO_ACTION_LIMIT as u32,
                        self.idle.iter().map(|(_, t)| t.clone()).collect(),
                    ));
                }
            }
            // 共享模式拿不到完整证据就无法排除别的客户端的输入，不计入上限。
            if complete || !shared {
                self.continuations = self.continuations.saturating_add(1);
                // 新路径由开始时的持久化总数准入；第 50 轮仍可完成最终核实。
                if self.safeguards.is_none() && self.continuations >= CONTINUATION_LIMIT {
                    return Some(report(
                        LoopGuardKind::ContinuationLimit,
                        self.continuations,
                        CONTINUATION_LIMIT,
                        vec![turn.turn],
                    ));
                }
            }
        }
        None
    }
}

fn report(
    kind: LoopGuardKind,
    count: u32,
    threshold: u32,
    turn_ids: Vec<String>,
) -> LoopGuardReport {
    LoopGuardReport {
        kind,
        count,
        threshold,
        turn_ids,
        action: LoopGuardAction::Stop,
        pause: None,
    }
}

/// goal 事实从哪里来：只有服务器的通知能说明「模型在自有轮里自建了 goal」；RPC 回包（读、写的结果）
/// 不走这条授予，写入回包的授予一律经有效的 [`GrantIntent`]。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GoalSource {
    Notification,
    Reply,
}

/// goal 对象的身份：目标原文与 createdAt（JSON 文本）。
fn identity(goal: Option<&Value>) -> (Option<String>, Option<String>) {
    let field = |name: &str| goal.and_then(|g| g.get(name)).filter(|v| !v.is_null());
    (
        field("objective")
            .and_then(Value::as_str)
            .map(str::to_owned),
        field("createdAt").map(Value::to_string),
    )
}

impl Machine {
    /// `started()` 里由 AwaitingContinuation 转入运行的轮：登记为本运行的自动续跑候选。
    pub(super) fn guard_auto_started(
        &mut self,
        run: &str,
        thread: &str,
        turn: &str,
        actions: &mut Vec<Action>,
    ) {
        if turn.is_empty() {
            return;
        }
        // 新运行的 goal 自动轮与汇总轮共享开始计数；旧路径仍保留原结算语义。
        if let Some(token) = self.convergences.runs.get(run).map(|r| r.token.clone()) {
            if self
                .safeguards(&token)
                .is_some_and(|s| !s.counted.contains_key(turn))
            {
                self.register_auto_parent_turn(
                    &token,
                    turn,
                    crate::sa1b::TurnOrigin::Goal,
                    actions,
                );
                return;
            }
        }
        let guard = self
            .loop_guards
            .entry(run.into())
            .or_insert_with(|| Guard::new(thread));
        if guard.is_auto(turn) {
            return;
        }
        let seq = guard.next_seq;
        guard.next_seq += 1;
        guard.auto_turns.push_back(AutoTurn {
            turn: turn.into(),
            seq,
            vetoed: false,
            evidence_seen: false,
        });
        let mut lost = None;
        while guard.auto_turns.len() > MAX_AUTO_TURNS {
            let old = guard.auto_turns.pop_front().expect("超限时非空");
            if !old.evidence_seen && !old.vetoed {
                lost = Some(old.turn);
            }
        }
        if let Some(old) = lost {
            // 这一轮的证据以后到了也认不出来：不能排除其中有用户输入，按介入处理。
            guard.idle.clear();
            actions.push(Action::Warn(format!(
                "运行 {run} 的自动续跑轮 {old} 证据未到就超出记录上限，按可能有用户输入处理"
            )));
            if self.shared {
                self.revoke_goal_control(thread);
            }
        }
        let Some(guard) = self.loop_guards.get_mut(run) else {
            return;
        };
        if guard.tripped {
            return;
        }
        guard.pending.push_back(PendingTurn {
            turn: turn.into(),
            seq,
            seen: Activity::default(),
            evidence: None,
        });
        if guard.pending.len() > MAX_PENDING_TURNS {
            let dropped = guard.pending.pop_front().expect("队列超限时非空");
            actions.push(Action::Warn(format!(
                "运行 {run} 等证据的自动续跑轮超过 {MAX_PENDING_TURNS} 轮，轮次 {} 不计入空转判断",
                dropped.turn
            )));
        }
    }

    /// 线程上提交的条目通知：只记「有」动作或用户输入；自动候选里出现用户输入立即否决。
    pub(super) fn guard_item(&mut self, thread: &str, turn: &str, item: &Value) {
        let sources: Vec<_> = self
            .loop_guards
            .keys()
            .map(|run| {
                (
                    run.clone(),
                    self.classify_result_input(run, thread, turn, item),
                )
            })
            .collect();
        for (run, source) in sources {
            let mut activity = Activity::default();
            activity.observe_with_source(item, source);
            self.guard_activity(&run, thread, turn, activity);
        }
    }

    /// 已通过分页身份校验且逐条分类后的正面证据；前页可立即否决，但不代表完整轮次。
    pub(crate) fn guard_activity(
        &mut self,
        run: &str,
        thread: &str,
        turn: &str,
        activity: Activity,
    ) {
        let original_user_turn = self.runs.runs.get(run).is_some_and(|r| {
            r.start_confirmation.turn_id.as_deref() == Some(turn) && r.start_confirmation.confirmed
        });
        let Some(guard) = self.loop_guards.get_mut(run).filter(|g| g.thread == thread) else {
            return;
        };
        if activity.user_input && !original_user_turn {
            if let Some(safeguards) = &mut guard.safeguards {
                safeguards.revoked = true;
                if let Some(runtime) = self.convergences.runs.get_mut(run) {
                    runtime.automatic_control_revoked = true;
                }
            }
        }
        if !guard.is_auto(turn) {
            return;
        }
        if let Some(pending) = guard.pending.iter_mut().find(|p| p.turn == turn) {
            pending.seen.action |= activity.action;
            pending.seen.user_input |= activity.user_input;
            pending.seen.special |= activity.special;
            pending.seen.unknown |= activity.unknown;
        }
        if activity.user_input && guard.veto(turn) && self.shared {
            self.revoke_goal_control(thread);
        }
    }

    /// 有人在这个线程上输入：共享服务器上它可能来自别的客户端，不再替用户刹车；推进代号，
    /// 让此前发出、尚未回包的授予写入失效。
    fn revoke_goal_control(&mut self, thread: &str) {
        let record = self.record(thread);
        record.goal_controlled = false;
        record.goal_control_epoch = record.goal_control_epoch.wrapping_add(1);
    }

    /// 本线程正在跑的是自有认领里客户端开的轮（不是自动候选，也不是 joined、settling）。
    fn own_client_turn(&self, thread: &str) -> bool {
        let Some(claim) = self.claims.get(thread) else {
            return false;
        };
        let State::Running {
            turn_id,
            joined: false,
            ..
        } = &claim.state
        else {
            return false;
        };
        !self
            .loop_guards
            .values()
            .any(|guard| guard.thread == thread && guard.is_auto(turn_id))
    }

    /// 观察到 goal（通知或读写回包；`None` 为已清除）。
    /// - 刹车计数：身份或状态真正变化时清零本线程的计数；只更新用量的通知不变，不清零；第一次观察只记下。
    /// - 共享控制资格：goal 不是 active 即撤销并推进代号；已知 goal 换成新身份时，若是**通知**且发生在自有的
    ///   客户端轮里就授予（模型自己建 goal；迟到的旧写入回包不算），是本 broker 未回包的授予写入请求的目标就等它的回包，否则视为外部替换，
    ///   撤销并推进代号（外部改走再改回也不能让旧回包恢复资格）。客户端设置与 #2e 恢复的授予见 [`Self::grant_goal_control`]。
    pub(super) fn guard_goal(&mut self, thread: &str, goal: Option<&Value>, source: GoalSource) {
        if let Some(goal) = goal.filter(|g| g.is_object()) {
            self.convergences
                .goal_evidence
                .insert(thread.into(), goal.clone());
        }
        let goal = goal.filter(|g| g.is_object());
        let (objective, created) = identity(goal);
        let status = goal
            .and_then(|g| g.get("status"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let own_turn = source == GoalSource::Notification && self.own_client_turn(thread);
        // 新身份是本 broker 的授予写入请求的目标：正在处理的这次回包（解包时已从登记删除），或结果已知、
        // 发出后代号仍有效的未回包登记；是它自己引起的更新，不算外部替换。超时结果未知或代号已失效的旧登记不算。
        let epoch = self.records.get(thread).map_or(0, |r| r.goal_control_epoch);
        let expected = objective.as_deref().is_some_and(|wanted| {
            self.grant_intent.as_ref().is_some_and(|i| {
                i.thread == thread && i.epoch == epoch && i.objective.as_deref() == Some(wanted)
            }) || self.stops.changes.values().any(|c| {
                c.thread == thread
                    && !c.unknown
                    && c.control_epoch == Some(epoch)
                    && c.grant_objective.as_deref() == Some(wanted)
            })
        });
        let record = self.record(thread);
        // 缺字段的通知不当作变化，沿用已知值；清除则全部没有。
        let objective = goal.and(objective.or_else(|| record.goal_objective.clone()));
        let created = goal.and(created.or_else(|| record.goal_created.clone()));
        let status = goal.and(status.or_else(|| record.goal_status.clone()));
        let renewed = objective != record.goal_objective || created != record.goal_created;
        let changed = record.goal_seen && (renewed || status != record.goal_status);
        let deactivated = status.as_deref() != Some("active");
        let mut replaced = false;
        if !deactivated && renewed && record.goal_seen {
            if own_turn {
                record.goal_controlled = true;
            } else if expected {
                // 等这次写入自己的回包授予。
                record.goal_controlled = false;
            } else {
                replaced = true;
            }
        }
        record.goal_seen = true;
        record.goal_objective = objective;
        record.goal_created = created;
        record.goal_status = status;
        if deactivated || replaced {
            self.revoke_goal_control(thread);
        }
        if changed {
            for guard in self.loop_guards.values_mut() {
                if guard.thread == thread && !guard.tripped {
                    guard.reset();
                }
            }
        }
    }

    /// 可能授予控制资格的 goal 写入在发出时的资格代号（`track_goal_requests` 调用，写入还没包装）。
    pub(crate) fn control_epoch_for(&self, thread: &str, context: &RequestContext) -> Option<u64> {
        let activating =
            context.method == "thread/goal/set" && shutdown::goal_set_can_activate(&context.params);
        let source = (context.operation == Operation::Forward && context.client.is_some())
            || matches!(
                context.operation,
                Operation::GoalRestore(resume::GoalRestoreStep::SetActive { .. })
            );
        (activating && source).then(|| self.records.get(thread).map_or(0, |r| r.goal_control_epoch))
    }

    /// 本 broker 的客户端把 goal 设成 active（或 #2e 恢复成功）的回包：回包里的 goal 是 active、身份
    /// 与当前已知事实一致（`guard_goal` 已学过这次写入，或它自己引起的 updated 通知先到），且写入发出后
    /// 资格代号未变（解包时取出的 `grant_intent`）才授予。中间被换掉或被暂停过都不授予。
    pub(crate) fn grant_goal_control(&mut self, thread: &str, goal: &Value) {
        let Some(intent) = self.grant_intent.take() else {
            return;
        };
        if intent.thread != thread || goal.get("status").and_then(Value::as_str) != Some("active") {
            return;
        }
        let epoch = intent.epoch;
        let (objective, created) = identity(Some(goal));
        let record = self.record(thread);
        if record.goal_control_epoch == epoch
            && record.goal_status.as_deref() == Some("active")
            && record.goal_objective == objective
            && record.goal_created == created
        {
            record.goal_controlled = true;
        }
    }

    /// 每次 step 处理完事件后：收下补读交回的证据，按运行编号顺序结算并触发。
    pub(super) fn drive_loop_guards(&mut self, actions: &mut Vec<Action>) {
        let evidence = std::mem::take(&mut self.runs.guard_evidence);
        // 认领释放、换代或转到 review 子线程后，运行不再由这里刹车（认领有多个删除出口，统一在此清理）。
        self.loop_guards.retain(|run, guard| {
            self.claims
                .get(&guard.thread)
                .is_some_and(|c| c.run_id.as_deref() == Some(run.as_str()))
        });
        if self.closing {
            return;
        }
        for (run, turn, activity) in evidence {
            let Some(guard) = self.loop_guards.get_mut(&run) else {
                continue;
            };
            if let Some(entry) = guard.auto_turns.iter_mut().find(|a| a.turn == turn) {
                entry.evidence_seen = true;
            }
            if activity.is_some_and(|a| a.user_input) && guard.is_auto(&turn) {
                // 被挤出队列或已排到后面的轮也照样否决；通知已否决过的不重复。
                let newly = guard.veto(&turn);
                if let Some(safeguards) = &mut guard.safeguards {
                    safeguards.revoked = true;
                }
                if let Some(runtime) = self.convergences.runs.get_mut(&run) {
                    runtime.automatic_control_revoked = true;
                }
                let thread = guard.thread.clone();
                if newly && self.shared {
                    self.revoke_goal_control(&thread);
                }
                continue;
            }
            if let Some(pending) = guard
                .pending
                .iter_mut()
                .find(|p| p.turn == turn && p.evidence.is_none())
            {
                pending.evidence = Some(activity);
            }
        }
        let mut runs: Vec<RunId> = self.loop_guards.keys().cloned().collect();
        runs.sort();
        let shared = self.shared;
        for run in runs {
            let revoked = self.loop_guards[&run]
                .safeguards
                .as_ref()
                .is_some_and(|s| s.revoked);
            if revoked {
                let join = self
                    .runs
                    .runs
                    .get_mut(&run)
                    .and_then(|r| r.subagent_join.as_mut());
                if let Some(join) = join.filter(|j| !j.automatic_control_revoked) {
                    join.automatic_control_revoked = true;
                    self.persist_safeguards(&run, actions);
                }
            }
            // 迟到的开始事实可以记账，但撤权或授权换代后不能再替用户发起刹车。
            if self.loop_guards[&run].safeguards.is_some()
                && !self.convergences.runs.get(&run).is_some_and(|runtime| {
                    !runtime.automatic_control_revoked
                        && self.safeguards(&runtime.token).is_some_and(|s| !s.revoked)
                })
            {
                continue;
            }
            let guard = self.loop_guards.get_mut(&run).expect("刚取出的运行");
            if guard.tripped {
                continue;
            }
            if let Some(report) = guard.settle(shared) {
                guard.tripped = true;
                let thread = guard.thread.clone();
                self.trip_loop_guard(&run, &thread, report, actions);
            }
        }
    }

    /// 触发刹车：运行已不是这个线程上的自有认领、已有停止操作或正在关闭时只记日志。
    fn trip_loop_guard(
        &mut self,
        run: &str,
        thread: &str,
        mut report: LoopGuardReport,
        actions: &mut Vec<Action>,
    ) {
        let own = self
            .claims
            .get(thread)
            .is_some_and(|c| c.run_id.as_deref() == Some(run) && !c.joined() && !c.settling());
        if !own || self.stop_busy(thread) || self.quiescing {
            actions.push(Action::Debug(format!(
                "线程 {thread} 满足空转刹车条件（{:?}），但运行已结束、正在停止或正在关闭，不再处理",
                report.kind
            )));
            return;
        }
        let managed = self
            .loop_guards
            .get(run)
            .is_some_and(|g| g.safeguards.is_some());
        if !self.shared || managed {
            actions.push(Action::Warn(format!(
                "线程 {thread} 自动续跑空转（{:?}，{} 轮），暂停 goal 并停止本次运行",
                report.kind, report.count
            )));
            self.run_loop_guard(run, report, actions);
            self.begin_stop(thread, StopReason::GoalLoop, false, actions);
            return;
        }
        let record = self.records.get(thread);
        let controlled =
            record.is_some_and(|r| r.goal_controlled && r.goal_status.as_deref() == Some("active"));
        if !controlled {
            // 不归本次运行控制的 goal 不碰；从头计数，资格之后恢复时要重新攒证据。
            actions.push(Action::Debug(format!(
                "线程 {thread} 满足空转刹车条件（{:?}），但 goal 不归本次运行控制，不暂停",
                report.kind
            )));
            if let Some(guard) = self.loop_guards.get_mut(run) {
                guard.reset();
            }
            return;
        }
        let target = PauseTarget {
            run_id: run.into(),
            thread: thread.into(),
            objective: record.and_then(|r| r.goal_objective.clone()),
            created: record.and_then(|r| r.goal_created.clone()),
            version: 0,
        };
        actions.push(Action::Warn(format!(
            "线程 {thread} 自动续跑空转（{:?}，{} 轮），共享服务器上只暂停 goal、不中断正在跑的轮",
            report.kind, report.count
        )));
        report.action = LoopGuardAction::PauseGoal;
        report.pause = Some(PauseOutcome::Pending);
        self.run_loop_guard(run, report, actions);
        let claim_id = self.claims[thread].claim_id;
        actions.push(Action::Request(RequestContext {
            resume_retried: false,
            stream_id: None,
            review_id: None,
            client: None,
            token: Some(ClaimToken {
                thread_id: thread.into(),
                claim_id,
            }),
            operation: Operation::LoopGuardPause(target),
            method: "thread/goal/set".into(),
            params: Some(json!({"threadId": thread, "status": "paused"})),
        }));
    }

    /// 共享模式暂停请求的回包（通用事实学习已跳过）：
    /// - 发出后 goal 事实没变：照常学习回包，再按身份核对；
    /// - 事实已变（常见的是这次写入自己引起的 updated 通知先到）：不学习旧回包；当前已知事实正是
    ///   同一个 goal 且已暂停时报 paused，否则报 superseded。
    pub(super) fn loop_guard_pause_reply(
        &mut self,
        target: PauseTarget,
        context: &RequestContext,
        result: &Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) {
        let fresh = self.stops.versions.get(&target.thread).copied() == Some(target.version);
        let outcome = match result {
            Err(error) => {
                actions.push(Action::Warn(format!(
                    "运行 {} 的空转刹车暂停 goal 失败：{error}",
                    target.run_id
                )));
                PauseOutcome::Failed
            }
            Ok(value) if fresh => {
                self.learn_traffic(context, result, actions);
                let goal = value.get("goal").filter(|g| g.is_object());
                if identity(goal) != (target.objective.clone(), target.created.clone()) {
                    PauseOutcome::Superseded
                } else if goal.and_then(|g| g.get("status")).and_then(Value::as_str)
                    == Some("paused")
                {
                    PauseOutcome::Paused
                } else {
                    PauseOutcome::Failed
                }
            }
            Ok(_) => {
                let record = self.records.get(&target.thread);
                let same = record.is_some_and(|r| {
                    r.goal_objective == target.objective
                        && r.goal_created == target.created
                        && r.goal_status.as_deref() == Some("paused")
                });
                if same {
                    PauseOutcome::Paused
                } else {
                    PauseOutcome::Superseded
                }
            }
        };
        self.run_loop_guard_pause(&target.run_id, outcome, actions);
    }
}
