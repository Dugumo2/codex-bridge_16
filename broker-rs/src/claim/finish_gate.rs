//! SA-b 自然结束门禁。占位始终 Release，父异常优先判定的调用点先于 goal active。
use super::{Action, Machine};
use crate::runs::TurnStatus;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseDecision {
    Release,
    Hold,
}
impl Machine {
    pub fn natural_release_gate(
        &mut self,
        _thread: &str,
        _goal_status: Option<&str>,
        _turn_status: Option<TurnStatus>,
        _actions: &mut Vec<Action>,
    ) -> ReleaseDecision {
        ReleaseDecision::Release
    }
    pub fn finishing_tree_turn(
        &mut self,
        _thread: &str,
        _turn: &str,
        _started: bool,
        _actions: &mut Vec<Action>,
    ) {
    }
    pub fn finishing_tree_goal(
        &mut self,
        _thread: &str,
        _active: bool,
        _actions: &mut Vec<Action>,
    ) {
    }
}

impl Machine {
    pub fn subagent_work_gate(
        &mut self,
        _request: &super::ClientRequest,
        _actions: &mut Vec<Action>,
    ) -> bool {
        false
    }
    pub fn subagent_lifecycle_gate(
        &mut self,
        _request: &super::ClientRequest,
        _actions: &mut Vec<Action>,
    ) -> bool {
        false
    }
}
