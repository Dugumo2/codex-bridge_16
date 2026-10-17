//! 子线程（子代理）上的上游报错与进展归入父运行（契约第 5 节）。
//!
//! 归属只认活动绑定：绑定上的 runId 与父线程、父运行仍在管理且在跑、子轮尚未结束。
//! 记账按子线程各自合并，只标脏，由 `flush_run_diagnostics` 统一增版与通知。
use crate::claim::{Action, Machine};
use crate::relations::BindingStatus;
use crate::runs::{
    upstream_error::{merge, UpstreamError},
    ChildRunRef, ChildUpstreamError, DiagnosticField, RunState,
};

/// 每个运行最多记录的子线程项；满了淘汰 `lastSeenAt` 最旧的一项。
pub const MAX_CHILD_UPSTREAM_ERRORS: usize = 32;

impl Machine {
    /// 子线程通知归属：满足契约 5.1 时返回父运行 runId 与父线程。纯查询，无副作用。
    ///
    /// `completion` 只用于 `turn/completed` 的首次终态：关系层先于诊断处理同一完成事件，
    /// 此时观察记录已标完成仍放行；首次接纳由运行级绑定标记判断，不依赖线程完成缓存。
    pub(crate) fn child_activity_run(
        &self,
        thread: &str,
        turn: &str,
        completion: bool,
    ) -> Option<ChildRunRef> {
        #[cfg(test)]
        if let Some(child) = &self.runs.child_activity_override {
            return Some(child.clone());
        }
        if turn.is_empty() || self.is_execution_thread(thread) {
            return None;
        }
        let binding = self.binding(thread, turn)?;
        if binding.status != BindingStatus::Bound {
            return None;
        }
        self.manage_context(&binding.run_id)?;
        let record = self.runs.runs.get(&binding.run_id)?;
        if !matches!(record.state, RunState::Running | RunState::Stopping) {
            return None;
        }
        // 观察记录缺失（已被淘汰）时无法证明子轮仍在进行，按不归属处理。
        let observation = self
            .relations
            .aux
            .observed
            .get(thread)?
            .iter()
            .find(|o| o.turn == turn)?;
        if observation.completed && !completion {
            return None;
        }
        Some(ChildRunRef {
            run_id: binding.run_id,
            parent_thread_id: binding.parent_thread_id,
        })
    }

    /// 与有效绑定同寿命的完成接纳标记；有界于现存绑定，不能用提前淘汰的 LRU 代替。
    pub(crate) fn accept_child_completion(
        &mut self,
        child: &ChildRunRef,
        thread: &str,
        turn: &str,
    ) -> bool {
        let Some(active) = self.runs.active.get_mut(&child.run_id) else {
            return false;
        };
        let bindings = &self.relations.bindings;
        active.child_completions.retain(|key| {
            bindings.get(key).is_some_and(|binding| {
                binding.run_id == child.run_id && binding.status == BindingStatus::Bound
            })
        });
        active
            .child_completions
            .insert((thread.into(), turn.into()))
    }

    /// 子线程 error／turn/completed(failed) 的诊断记账（契约 5.2）。
    /// 只改本运行的 `childUpstreamErrors`：父 `upstreamError`、轮次结果、SA1b 证据与预算都不碰。
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn child_diagnostic_event(
        &mut self,
        child: &ChildRunRef,
        thread: &str,
        _turn: &str,
        error: UpstreamError,
        _completion: bool,
        actions: &mut Vec<Action>,
    ) {
        let Some(record) = self.runs.runs.get_mut(&child.run_id) else {
            return;
        };
        let errors = &mut record.child_upstream_errors;
        let item = match errors.iter_mut().find(|e| e.thread_id == thread) {
            Some(entry) => {
                let mut current = Some(entry.upstream_error.clone());
                let mut changed = merge(&mut current, error);
                entry.upstream_error = current.expect("合并后必有报错");
                if entry.parent_thread_id != child.parent_thread_id {
                    entry.parent_thread_id = child.parent_thread_id.clone();
                    changed = true;
                }
                if !changed {
                    return;
                }
                entry.clone()
            }
            None => {
                if errors.len() >= MAX_CHILD_UPSTREAM_ERRORS {
                    if let Some(oldest) = errors
                        .iter()
                        .enumerate()
                        .min_by_key(|(_, e)| e.upstream_error.last_seen_at)
                        .map(|(i, _)| i)
                    {
                        errors.remove(oldest);
                    }
                }
                let entry = ChildUpstreamError {
                    thread_id: thread.into(),
                    parent_thread_id: child.parent_thread_id.clone(),
                    upstream_error: error,
                };
                errors.push(entry.clone());
                entry
            }
        };
        self.mark_run_diagnostic(
            &child.run_id,
            DiagnosticField::ChildUpstreamErrors,
            Some(item),
        );
        // 与 `run_diagnostic_event` 一致：批处理外的直接调用立即提交。
        if !self.diagnostic_batch_active {
            self.flush_run_diagnostics(actions);
        }
    }

    /// 认领线程与活动运行的执行线程（含 review 执行线程）不是子线程。
    fn is_execution_thread(&self, thread: &str) -> bool {
        self.claims.contains_key(thread)
            || self.runs.active.keys().any(|id| {
                self.runs.runs.get(id).is_some_and(|r| {
                    r.thread_id == thread || r.review_thread_id.as_deref() == Some(thread)
                })
            })
    }
}
