//! SA-a 填充事实与路由；骨架的查询保守返回未知，所有钩子无副作用。
pub mod types;
use crate::claim::{Action, HeldNotification, HeldRequest, Machine, RequestContext, TimerKind};
use serde_json::Value;
use std::collections::BTreeMap;
pub use types::*;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Relations {
    pub conn_epoch: u64,
    pub edges: BTreeMap<String, RelationEdge>,
    pub bindings: BTreeMap<(String, String), ActivityBinding>,
    pub contexts: BTreeMap<String, ManageContext>,
    pub operations: BTreeMap<EvidenceKey, EvidenceOperation>,
    pub discoveries: BTreeMap<u64, DiscoveryOperation>,
    pub requests: BTreeMap<(u64, String), ChildRequest>,
    pub tombstones: BTreeMap<(u64, String), u64>,
    pub attachments: BTreeMap<String, AttachmentEvidence>,
}
impl Machine {
    pub fn run_subtree(&self, _run_id: &str) -> Option<SubtreeView> {
        None
    }
    pub fn binding(&self, _thread_id: &str, _turn_id: &str) -> Option<ActivityBinding> {
        None
    }
    pub fn manage_context(&self, _run_id: &str) -> Option<&ManageContext> {
        None
    }
    pub fn thread_relation(&self, _thread_id: &str) -> Option<RelationEdge> {
        None
    }
    pub fn thread_capability(&self, _thread_id: &str) -> Option<bool> {
        None
    }
    pub fn has_managed_activity(&self, _run_id: &str) -> Activity {
        Activity::Unknown
    }
    /// 0 为未登记的占位编号；正式操作编号从 1 开始。
    pub fn begin_discovery(
        &mut self,
        _root: &str,
        _scope: DiscoveryScope,
        _caller: DiscoveryCaller,
        actions: &mut Vec<Action>,
    ) -> u64 {
        actions.push(Action::Warn("SA 发现尚未启用".into()));
        0
    }
    pub fn discovery_done(
        &mut self,
        _op_id: u64,
        _caller: DiscoveryCaller,
        _result: DiscoveryResult,
        _actions: &mut Vec<Action>,
    ) {
    }
    pub fn on_claim_settled(
        &mut self,
        _thread: &str,
        _claim_id: u64,
        _own: bool,
        _actions: &mut Vec<Action>,
    ) {
    }
    pub fn on_handoff(&mut self, _run_id: &str, _kind: HandoffKind, _actions: &mut Vec<Action>) {}
    pub fn on_unclaimed_turn(
        &mut self,
        _thread: &str,
        _turn: &str,
        _started: bool,
        _actions: &mut Vec<Action>,
    ) {
    }
    pub fn on_relation_notification(
        &mut self,
        _event: &HeldNotification,
        _actions: &mut Vec<Action>,
    ) {
    }
    /// false 表示交回原路由；正式实现先排除来源线程自己的认领，再接管子请求。
    pub fn route_child_request(
        &mut self,
        _request: &HeldRequest,
        _actions: &mut Vec<Action>,
    ) -> bool {
        false
    }
    pub fn relation_reply(
        &mut self,
        _context: &RequestContext,
        _result: Result<Value, Value>,
        _actions: &mut Vec<Action>,
    ) {
    }
    pub fn relation_timer(&mut self, _kind: TimerKind, _actions: &mut Vec<Action>) {}
    pub fn relations_shutdown(&mut self, _actions: &mut Vec<Action>) {}
    pub fn child_origin(&self, _thread: &str, _turn: Option<&str>) -> Option<CxbOrigin> {
        None
    }
    pub fn inbox_pending_run(&self, _run_id: &str) -> u64 {
        0
    }
    pub fn revoke_child_requests(&mut self, _run_id: &str, _actions: &mut Vec<Action>) {}
}

impl Machine {
    /// SA-a：仅对 cxb 目标在转发副本覆盖 cxbOrigin；内部参数保持原样。
    pub fn child_forward_params(
        &self,
        _target: crate::claim::ConnId,
        _thread: &str,
        params: &Option<Value>,
    ) -> Option<Value> {
        params.clone()
    }
}

impl Machine {
    pub fn child_reply_written(
        &mut self,
        _op: u64,
        _outcome: &crate::inbox::ReplyOutcome,
        _actions: &mut Vec<Action>,
    ) {
    }
    /// SA-a：已登记 watcher 后扩充 children/revision；骨架保持原快照。
    pub fn child_watch_snapshot(
        &self,
        _thread: &str,
        _run_id: Option<&str>,
        snapshot: Value,
    ) -> Value {
        snapshot
    }
}
