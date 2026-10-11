//! C 路可信输入接口。登记必须早于发请求；骨架不授信任何输入。

use super::{Action, Machine};
use crate::sa1b::{InputSource, TrustedInputRegistration};
use serde_json::Value;

impl Machine {
    pub fn register_auto_parent_turn(
        &mut self,
        _token: &crate::sa1b::ConvergeToken,
        _turn: &str,
        _origin: crate::sa1b::TurnOrigin,
        actions: &mut Vec<Action>,
    ) -> bool {
        actions.push(Action::Warn("SA1b 自动父轮计数尚未接线".into()));
        false
    }

    pub fn register_reconcile_input(
        &mut self,
        _registration: TrustedInputRegistration,
        actions: &mut Vec<Action>,
    ) -> bool {
        actions.push(Action::Warn("SA1b 可信输入登记尚未接线".into()));
        false
    }

    /// 通知、full items 与分页读取必须在压缩成布尔值前调用同一分类器。
    pub fn classify_run_input(
        &self,
        _run: &str,
        _thread: &str,
        _turn: &str,
        item: &Value,
    ) -> InputSource {
        crate::results::classify_untrusted_input(item)
    }
}
