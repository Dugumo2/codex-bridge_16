//! B 实现独立于父 turns 的异步子结果读取。

use crate::claim::{Action, Machine, RequestContext};
use crate::sa1b::{BatchReadId, ConvergeToken, ResultLimits};
use serde_json::Value;

impl Machine {
    /// 非零读号只由实际登记的异步操作产生；骨架返回禁用哨兵。
    pub fn read_child_results(
        &mut self,
        _token: ConvergeToken,
        _work_epoch: u64,
        _limits: ResultLimits,
        actions: &mut Vec<Action>,
    ) -> BatchReadId {
        actions.push(Action::Warn("SA1b 子结果读取尚未接线".into()));
        BatchReadId::DISABLED
    }

    pub fn child_result_reply(
        &mut self,
        _context: &RequestContext,
        _result: Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) {
        actions.push(Action::Warn("SA1b 子结果读取回包尚未接线".into()));
    }
}
