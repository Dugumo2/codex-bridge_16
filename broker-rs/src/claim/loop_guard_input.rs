//! C 路输入能力与自动父轮账本。仅由集成后的协调器显式初始化。

use super::{loop_guard::Guard, Action, Machine};
use crate::relations::Authority;
use crate::runs::RunState;
use crate::sa1b::*;
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Safeguards {
    pub token: Option<ConvergeToken>,
    pub trusted: Vec<TrustedInputRegistration>,
    /// 不受结果窗口裁剪影响，重复确认不重复计数。
    pub counted: BTreeMap<String, TurnOrigin>,
    pub revoked: bool,
    pub budget: crate::runs::budget::BudgetTracker,
}

impl Machine {
    /// B 创建 TurnResult 时保留开始来源；不从输入文本或已裁剪的 turns 推算。
    pub fn auto_parent_turn_origin(&self, run: &str, turn: &str) -> Option<TurnOrigin> {
        self.loop_guards
            .get(run)?
            .safeguards
            .as_ref()?
            .counted
            .get(turn)
            .copied()
    }
    /// 撤销自动权后仍可记录原操作用量；停止和关闭不能授予新能力。
    pub(crate) fn safeguards_token_current(&self, token: &ConvergeToken) -> bool {
        !self.closing
            && token.conn_epoch == self.relations.conn_epoch
            && self
                .convergences
                .runs
                .get(&token.run_id)
                .is_some_and(|r| r.token == *token)
            && self.claims.get(&token.claim.thread_id).is_some_and(|c| {
                c.claim_id == token.claim.claim_id
                    && c.run_id.as_deref() == Some(&token.run_id)
                    && !c.joined()
                    && !c.settling()
            })
            && self.manage_context(&token.run_id).is_some_and(|c| {
                c.gen == token.manage_gen
                    && c.identity.execution_thread_id == token.claim.thread_id
                    && matches!(c.authority, Authority::Owner(_) | Authority::Broker { .. })
            })
            && self
                .runs
                .runs
                .get(&token.run_id)
                .is_some_and(|r| matches!(r.state, RunState::Running | RunState::Stopping))
    }

    pub(crate) fn safeguards(&self, token: &ConvergeToken) -> Option<&Safeguards> {
        self.safeguards_token_current(token).then_some(())?;
        self.loop_guards
            .get(&token.run_id)?
            .safeguards
            .as_ref()
            .filter(|s| s.token.as_ref() == Some(token))
    }

    pub(crate) fn safeguards_mut(&mut self, token: &ConvergeToken) -> Option<&mut Safeguards> {
        self.safeguards_token_current(token).then_some(())?;
        self.loop_guards
            .get_mut(&token.run_id)?
            .safeguards
            .as_mut()
            .filter(|s| s.token.as_ref() == Some(token))
    }

    /// A 在新运行建立协调上下文后调用。None 必须代表已核实无预算，不能代表漏读预算。
    /// 生产协调器建立运行身份后调用；旧记录或重启摘要不能重新构造能力。
    pub fn initialize_run_safeguards(
        &mut self,
        token: &ConvergeToken,
        mut budget: Option<RunBudget>,
        actions: &mut Vec<Action>,
    ) -> bool {
        if !self.safeguards_token_current(token)
            || self.runs.runs[&token.run_id].auto_turns_started.is_some()
            || self.runs.runs[&token.run_id].state != RunState::Running
            || self.convergences.runs[&token.run_id].automatic_control_revoked
        {
            return false;
        }
        let mut safeguards = Safeguards {
            token: Some(token.clone()),
            ..Safeguards::default()
        };
        if let Some(budget) = &mut budget {
            safeguards.budget.initialize(budget);
            safeguards.budget.validate_initial(budget);
        }
        let guard = self
            .loop_guards
            .entry(token.run_id.clone())
            .or_insert_with(|| Guard::new(&token.claim.thread_id));
        // 按需启用：接上启用前已开始的 goal 自动轮（总数不重置，仍可追溯的轮次按 goal 来源登记）。
        let (started, turns) = guard.started_auto_turns();
        for turn in &turns {
            safeguards.counted.insert(turn.clone(), TurnOrigin::Goal);
        }
        guard.safeguards = Some(safeguards);
        let record = self.runs.runs.get_mut(&token.run_id).expect("已核实运行");
        record.auto_turns_started = Some(started);
        for result in record
            .turns
            .iter_mut()
            .filter(|r| turns.contains(&r.turn_id))
        {
            result.origin = Some(TurnOrigin::Goal);
        }
        record.run_budget = budget;
        self.persist_safeguards(&token.run_id, actions);
        true
    }

    /// A 完成 detach 授权换代后迁移同一运行能力；不允许新认领、重连或新协调器继承。
    pub fn rebind_run_safeguards(&mut self, old: &ConvergeToken, new: &ConvergeToken) -> bool {
        if old.run_id != new.run_id
            || old.claim != new.claim
            || old.conn_epoch != new.conn_epoch
            || old.generation != new.generation
            || !self.safeguards_token_current(new)
        {
            return false;
        }
        let Some(s) = self
            .loop_guards
            .get_mut(&new.run_id)
            .and_then(|g| g.safeguards.as_mut())
        else {
            return false;
        };
        if s.token.as_ref() != Some(old) {
            return false;
        }
        s.token = Some(new.clone());
        for registration in &mut s.trusted {
            registration.token = new.clone();
        }
        s.budget.rebind(new);
        true
    }

    /// 第 50 轮可完成最终核实；后续启动由 A/B 检查此准入。
    pub fn auto_parent_turn_admission(
        &self,
        token: &ConvergeToken,
    ) -> Result<(), ReconcileRefusal> {
        let Some(guard) = self.safeguards(token) else {
            return Err(ReconcileRefusal::Disabled);
        };
        if guard.revoked || self.convergences.runs[&token.run_id].automatic_control_revoked {
            return Err(ReconcileRefusal::OwnershipConflict);
        }
        let record = &self.runs.runs[&token.run_id];
        if record.state != RunState::Running
            || self.quiescing
            || self.stop_busy(&token.claim.thread_id)
        {
            return Err(ReconcileRefusal::Stopping);
        }
        let limit = record
            .convergence_config
            .as_ref()
            .map_or(50, |c| c.max_auto_turns);
        if record.auto_turns_started.is_none_or(|n| n >= limit) {
            return Err(ReconcileRefusal::AutoTurnLimit);
        }
        Ok(())
    }

    /// 调用方须先证明为本运行自有父轮。计数只记事实，不据此授予归属。
    pub fn register_auto_parent_turn(
        &mut self,
        token: &ConvergeToken,
        turn: &str,
        origin: TurnOrigin,
        actions: &mut Vec<Action>,
    ) -> bool {
        if turn.is_empty() || origin == TurnOrigin::User || self.safeguards(token).is_none() {
            return false;
        }
        let safeguards = self.safeguards_mut(token).expect("已核实能力");
        if safeguards.counted.contains_key(turn) {
            return true;
        }
        safeguards.counted.insert(turn.into(), origin);
        let record = self.runs.runs.get_mut(&token.run_id).expect("已核实运行");
        record.auto_turns_started = record.auto_turns_started.map(|n| n.saturating_add(1));
        if let Some(result) = record.turns.iter_mut().find(|r| r.turn_id == turn) {
            result.origin = Some(origin);
        }
        self.guard_auto_started(&token.run_id, &token.claim.thread_id, turn, actions);
        self.persist_safeguards(&token.run_id, actions);
        true
    }

    /// B 先生成不可预测的 clientId 并登记，再发请求；文本、条目 id 均不授信。
    pub fn register_reconcile_input(
        &mut self,
        registration: TrustedInputRegistration,
        _actions: &mut Vec<Action>,
    ) -> bool {
        let token = &registration.token;
        let merging_own_turn = registration.expected_turn_id.as_ref().is_some_and(|turn| {
            self.safeguards(token)
                .is_some_and(|g| g.counted.get(turn) == Some(&TurnOrigin::Goal))
                && self
                    .claims
                    .get(&token.claim.thread_id)
                    .and_then(|c| c.turn_id())
                    == Some(turn.as_str())
        });
        let admission = self.auto_parent_turn_admission(token);
        if (admission.is_err()
            && !(admission == Err(ReconcileRefusal::AutoTurnLimit) && merging_own_turn))
            || self.reconcile_budget_admission(token) != BudgetAdmission::Allowed
            || (registration.expected_turn_id.is_some() && !merging_own_turn)
            || registration.op_id.0 == 0
            || registration.client_user_message_id.is_empty()
            || registration.client_user_message_id.len() > 256
            || registration.work_epoch == 0
        {
            return false;
        }
        let guard = self.safeguards_mut(token).expect("准入已核实");
        if guard.trusted.iter().any(|r| {
            r.op_id == registration.op_id
                || r.client_user_message_id == registration.client_user_message_id
        }) {
            return guard.trusted.contains(&registration);
        }
        // 合并既有 goal 轮也会生成输入能力，不把它误算成 5 次新开汇总。
        // 有界历史保留至运行释放，旧 clientId 不得挪给另一轮。
        if guard.trusted.len() >= 4096 {
            return false;
        }
        guard.trusted.push(registration);
        true
    }

    /// 授权换代（`rebind_run_safeguards`）已把同一笔提交的登记迁到新身份。
    pub(crate) fn reconcile_input_migrated(&self, registration: &TrustedInputRegistration) -> bool {
        self.safeguards(&registration.token).is_some_and(|s| {
            s.trusted.iter().any(|r| {
                r.token == registration.token
                    && r.op_id == registration.op_id
                    && r.client_user_message_id == registration.client_user_message_id
                    && r.work_epoch == registration.work_epoch
            })
        })
    }

    /// B 确认承载及自有归属后、回放条目前绑定实际轮；clientId 本身不授予自有权。
    pub fn bind_reconcile_input(
        &mut self,
        token: &ConvergeToken,
        op: ReconcileOpId,
        turn: &str,
    ) -> bool {
        let Some(guard) = self.safeguards_mut(token) else {
            return false;
        };
        if !guard.counted.contains_key(turn) {
            return false;
        }
        let Some(reg) = guard
            .trusted
            .iter_mut()
            .find(|r| r.token == *token && r.op_id == op)
        else {
            return false;
        };
        if reg.expected_turn_id.as_deref().is_some_and(|t| t != turn) {
            return false;
        }
        reg.expected_turn_id = Some(turn.into());
        true
    }

    /// 通知、full items、分页共用，必须在压缩成布尔值以前调用。
    pub fn classify_run_input(
        &self,
        run: &str,
        thread: &str,
        turn: &str,
        item: &Value,
    ) -> InputSource {
        if initial_input_matches(self.initial_input_id(run, thread, turn), item) {
            // 与可信汇总共用“已核实、非插话”的分类；这里只豁免原请求的精确 clientId。
            return InputSource::TrustedReconcile;
        }
        let Some(runtime) = self.convergences.runs.get(run) else {
            return crate::results::classify_untrusted_input(item);
        };
        let Some(guard) = self.safeguards(&runtime.token) else {
            return crate::results::classify_untrusted_input(item);
        };
        classify_registered(&guard.trusted, &runtime.token, thread, turn, item)
    }

    fn initial_input_id(&self, run: &str, thread: &str, turn: &str) -> Option<&str> {
        let record = self.runs.runs.get(run)?;
        let guard = self.loop_guards.get(run)?;
        (guard.thread == thread
            && record.start_confirmation.confirmed
            && record.start_confirmation.owned == Some(true)
            && record.start_confirmation.turn_id.as_deref() == Some(turn))
        .then_some(guard.initial_client_id.as_deref())
        .flatten()
    }

    /// B 借用 Runs 前取得快照，每次接受页面前重新取得，不跨异步等待保存。
    pub fn run_input_classifier(&self, run: &str, thread: &str, turn: &str) -> RunInputClassifier {
        let initial_client_id = self.initial_input_id(run, thread, turn).map(str::to_owned);
        let Some(runtime) = self.convergences.runs.get(run) else {
            return RunInputClassifier {
                initial_client_id,
                ..Default::default()
            };
        };
        let Some(guard) = self.safeguards(&runtime.token) else {
            return RunInputClassifier::default();
        };
        RunInputClassifier {
            initial_client_id,
            context: Some((runtime.token.clone(), thread.into(), turn.into())),
            registrations: guard.trusted.clone(),
        }
    }
}

fn classify_registered(
    registrations: &[TrustedInputRegistration],
    token: &ConvergeToken,
    thread: &str,
    turn: &str,
    item: &Value,
) -> InputSource {
    let source = crate::results::classify_untrusted_input(item);
    if source != InputSource::ExternalUser || thread != token.claim.thread_id {
        return source;
    }
    let Some(id) = item.get("clientId").and_then(Value::as_str) else {
        return source;
    };
    if registrations.iter().any(|r| {
        r.token == *token
            && r.client_user_message_id == id
            && r.expected_turn_id.as_deref() == Some(turn)
    }) {
        InputSource::TrustedReconcile
    } else {
        source
    }
}

#[derive(Debug, Clone, Default)]
pub struct RunInputClassifier {
    initial_client_id: Option<String>,
    context: Option<(ConvergeToken, String, String)>,
    registrations: Vec<TrustedInputRegistration>,
}
impl RunInputClassifier {
    pub fn classify(&self, item: &Value) -> InputSource {
        if initial_input_matches(self.initial_client_id.as_deref(), item) {
            return InputSource::TrustedReconcile;
        }
        if let Some((token, thread, turn)) = &self.context {
            classify_registered(&self.registrations, token, thread, turn, item)
        } else {
            crate::results::classify_untrusted_input(item)
        }
    }
}

fn initial_input_matches(id: Option<&str>, item: &Value) -> bool {
    item.get("type").and_then(Value::as_str) == Some("userMessage")
        && id.is_some_and(|id| item.get("clientId").and_then(Value::as_str) == Some(id))
}
