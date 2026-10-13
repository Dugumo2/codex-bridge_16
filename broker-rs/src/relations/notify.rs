//! 按运行聚合的通知与 watch 快照（接口第 6 节）。
//!
//! `broker/run/childChanged` 只发 cxb 连接：运行的在线 owner、逻辑根与执行线程的 watcher。
//! watch 回包加 `children` 快照与 `revision`；watcher 先登记、再取快照，客户端丢弃 revision 不大于快照的通知。
//! 子线程原始的 turn/completed 不转发给父运行的 watcher（转发仍按来源线程）。
use super::*;
use crate::claim::ConnId;
use serde_json::json;

impl Machine {
    /// 运行的聚合通知对象：在线 Client owner、逻辑根与执行线程的 watcher，去重。
    pub(crate) fn run_recipients(&self, run: &str) -> Vec<ConnId> {
        let Some(context) = self.relations.contexts.get(run) else {
            return Vec::new();
        };
        let execution = &context.identity.execution_thread_id;
        let root = &context.identity.logical_root_thread_id;
        let owner = self
            .claims
            .get(execution)
            .filter(|c| c.run_id.as_deref() == Some(run))
            .and_then(|_| self.owner_conn(execution));
        let mut recipients: Vec<ConnId> = Vec::new();
        for conn in owner.into_iter().chain(
            [root, execution]
                .into_iter()
                .filter_map(|t| self.records.get(t))
                .flat_map(|r| r.watchers.iter().copied()),
        ) {
            if !recipients.contains(&conn) {
                recipients.push(conn);
            }
        }
        recipients
    }

    /// `broker/run/childChanged{runId, rootThreadId, threadId, parentThreadId, relationKind, activity, pendingCount, revision}`。
    /// 只影响运行整体的变化（在途派生、发现）threadId 为 null。
    pub(crate) fn notify_child_changed(&self, run: &str, thread: &str, actions: &mut Vec<Action>) {
        let Some(context) = self.relations.contexts.get(run) else {
            return;
        };
        let member = context.members.get(thread);
        let mut params = json!({
            "runId": run,
            "rootThreadId": context.identity.logical_root_thread_id,
            "threadId": if thread.is_empty() { Value::Null } else { json!(thread) },
            "parentThreadId": member.map(|m| m.parent.clone()),
            "relationKind": member.map(|m| m.kind),
            "activity": member.map(|m| m.activity),
            "pendingCount": member.map(|m| m.pending_requests),
            "revision": context.revision,
        });
        if let Some(summary) = self.convergence_summary(run) {
            if let (Some(target), Ok(Value::Object(fields))) =
                (params.as_object_mut(), serde_json::to_value(summary))
            {
                target.extend(fields);
            }
        }
        let message = crate::proto::notification("broker/run/childChanged", Some(params));
        for conn in self.run_recipients(run) {
            self.send_ext(conn, message.clone(), actions);
        }
    }

    pub(super) fn notify_watch_snapshot(
        &self,
        thread: &str,
        run_id: Option<&str>,
        mut snapshot: Value,
    ) -> Value {
        let run = run_id
            .map(str::to_owned)
            .or_else(|| {
                snapshot
                    .get("run")
                    .and_then(|r| r.get("runId"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .or_else(|| self.claims.get(thread).and_then(|c| c.run_id.clone()));
        let context = run.as_deref().and_then(|r| self.relations.contexts.get(r));
        let members: Vec<Value> = context
            .map(|c| {
                c.members
                    .iter()
                    .map(|(t, m)| {
                        json!({
                            "threadId": t,
                            "parentThreadId": m.parent,
                            "relationKind": m.kind,
                            "activity": m.activity,
                            "pendingCount": m.pending_requests,
                            "conflict": m.conflict,
                            "canAcceptDirectInput": m.capability,
                            "agentVersion": m.agent_version,
                            "turnIds": m.bindings,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        let stop = run
            .as_deref()
            .and_then(|r| self.runs.runs.get(r))
            .and_then(|r| r.children_stop.as_ref())
            .map(|s| json!({"phase": s.phase, "version": s.version, "unconfirmed": s.unconfirmed.len(), "conflicts": s.conflicts.len()}));
        let pending_count = run.as_deref().map_or(0, |r| self.inbox_pending_run(r));
        let children = json!({
            "runId": run,
            "members": members,
            "pendingCount": pending_count,
            "discovery": context.map(|c| json!({
                "complete": c.discovery.complete,
                "version": c.discovery.version,
                "incompleteReasons": c.discovery.incomplete_reasons,
            })),
            "inFlightSpawns": context.map_or(0, |c| c.in_flight_spawns.len()),
            "stop": stop,
        });
        if let Some(object) = snapshot.as_object_mut() {
            // 父线程的 pending 按聚合值：本线程事项加上以它为逻辑根的子线程事项。
            if object.get("pending").is_some_and(Value::is_array) {
                object.insert(
                    "pending".into(),
                    json!(self.inbox_pending_aggregate(thread)),
                );
            }
            if let Some(summary) = run.as_deref().and_then(|run| self.convergence_summary(run)) {
                if let Ok(Value::Object(fields)) = serde_json::to_value(summary) {
                    object.extend(fields);
                }
            }
            object.insert("children".into(), children);
            object.insert("revision".into(), json!(context.map_or(0, |c| c.revision)));
        }
        snapshot
    }
}
