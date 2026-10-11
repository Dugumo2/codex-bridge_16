//! B 路汇总启动入口。骨架拒绝启动，不产生上游请求。

use super::{Action, Machine, RequestContext};
use crate::sa1b::*;
use serde_json::Value;

impl Machine {
    /// 传输失败不等于明确未提交；B 必须保留未知启动的核实责任。
    pub fn subagent_reconcile_transport(
        &mut self,
        _context: &RequestContext,
        event: ReconcileTransportEvent,
        actions: &mut Vec<Action>,
    ) {
        actions.push(Action::Warn(format!(
            "SA1b 汇总传输事件尚未接线：{event:?}"
        )));
    }

    pub fn begin_subagent_reconcile(
        &mut self,
        _token: ConvergeToken,
        _batch: ReconcileBatch,
        actions: &mut Vec<Action>,
    ) -> Result<ReconcileOpId, ReconcileRefusal> {
        actions.push(Action::Warn("SA1b 汇总启动尚未接线".into()));
        Err(ReconcileRefusal::Disabled)
    }

    pub fn cancel_subagent_reconcile(
        &mut self,
        _token: &ConvergeToken,
        reason: ReconcileCancel,
        actions: &mut Vec<Action>,
    ) {
        actions.push(Action::Warn(format!("SA1b 汇总取消尚未接线：{reason:?}")));
    }

    pub fn subagent_reconcile_reply(
        &mut self,
        _context: &RequestContext,
        _result: Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) {
        actions.push(Action::Warn("SA1b 汇总回包尚未接线，未学习归属".into()));
    }

    pub fn subagent_reconcile_status_done(
        &mut self,
        _done: super::status_read::StatusDone,
        actions: &mut Vec<Action>,
    ) {
        actions.push(Action::Warn("SA1b 输入标识核实尚未接线".into()));
    }

    pub fn child_results_done(
        &mut self,
        _token: ConvergeToken,
        _read_id: BatchReadId,
        _batch: ReconcileBatch,
        actions: &mut Vec<Action>,
    ) {
        actions.push(Action::Warn("SA1b 子结果完成回调尚未接线".into()));
    }

    /// B 确认自有后才能推进 currentTurnId；不得改写 creationOrder 与首次启动证明。
    pub fn reconcile_turn_confirmed(
        &mut self,
        _token: &ConvergeToken,
        _op_id: ReconcileOpId,
        _turn: &str,
        _origin: TurnOrigin,
        actions: &mut Vec<Action>,
    ) {
        actions.push(Action::Warn("SA1b 自有父轮登记尚未接线".into()));
    }
}
