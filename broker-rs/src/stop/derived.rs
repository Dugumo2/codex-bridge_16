//! SA-b 派生停止；不冒充父认领，不直接改父运行终态。
use super::tree::{RootStopReport, TreeTrigger};
use crate::claim::status_read::StatusDone;
use crate::claim::{Action, Machine};
use crate::relations::AgentVersion;
use std::collections::BTreeSet;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedTarget {
    pub thread: String,
    pub run_id: String,
    pub proven_turns: BTreeSet<String>,
    pub capability: Option<bool>,
    pub agent_version: AgentVersion,
    pub tree_op: u64,
    pub generation: u64,
    pub round: u64,
    pub authority_gen: u64,
    pub reason: TreeTrigger,
    pub clear: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedStopDone {
    pub tree_op: u64,
    pub run_id: String,
    pub thread: String,
    pub generation: u64,
    pub round: u64,
    pub report: RootStopReport,
    pub conflicts: Vec<String>,
    pub capability_limited: bool,
    pub cancelled: bool,
}
impl Machine {
    pub fn begin_derived_stop(&mut self, _target: DerivedTarget, actions: &mut Vec<Action>) {
        actions.push(Action::Warn("SA 派生停止尚未启用".into()));
    }
    pub fn derived_stop_done(&mut self, _done: DerivedStopDone, _actions: &mut Vec<Action>) {}
    pub fn derived_stop_verify_done(&mut self, _done: StatusDone, actions: &mut Vec<Action>) {
        actions.push(Action::Warn("SA 派生核实回调尚未启用".into()));
    }
}
