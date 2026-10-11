//! C 路预算准入与用量核实入口。骨架不扣费、不改变现有 goal 刹车。

use crate::claim::{Action, Machine};
use crate::sa1b::{BudgetAdmission, BudgetEvidence, ConvergeToken, GoalBudgetEvidence};

impl Machine {
    pub fn observe_goal_budget(
        &mut self,
        _token: &ConvergeToken,
        _evidence: GoalBudgetEvidence,
        actions: &mut Vec<Action>,
    ) {
        actions.push(Action::Warn("SA1b goal 汇总预算核实尚未接线".into()));
    }

    pub fn verify_run_budget(&mut self, _token: &ConvergeToken, actions: &mut Vec<Action>) {
        actions.push(Action::Warn("SA1b 预算补核实尚未接线".into()));
    }

    pub fn reconcile_budget_admission(&self, _token: &ConvergeToken) -> BudgetAdmission {
        BudgetAdmission::Disabled
    }

    pub fn observe_run_budget(
        &mut self,
        _token: &ConvergeToken,
        _evidence: BudgetEvidence,
        actions: &mut Vec<Action>,
    ) {
        actions.push(Action::Warn("SA1b 运行预算记账尚未接线".into()));
    }
}
