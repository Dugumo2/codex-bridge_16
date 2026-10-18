//! 子线程订阅释放：Codex app-server 会把新建线程（含子代理线程）自动挂到当时所有已初始化的连接上
//! （0.160.0 `app-server/src/lib.rs` 的 thread_created 分支），broker 从未主动订阅它们，也就不会随根线程退订。
//! 线程只要还有订阅者就不会卸载，它的整套 MCP 服务一直常驻（实测每个子线程约 300–370 MB）。
//!
//! 这里只在「祖先里由 broker 跑过运行的那个根已释放、自己也已无活动」时退订子线程：
//! - 根线程的退订真正发出时，扫一遍所有附着证据有效的子线程；
//! - 根释放后才结束的子线程，在它的轮次完成时补退；
//! - 整树清理结束时再扫一遍（清理范围内的子线程此前被 `relation_needs_subscription` 留住）。
//!
//! 退订只摘掉本连接，不影响别的客户端；判据从严，宁可多留不误退。
//!
//! 退订后线程要空闲 60 秒才卸载。这期间根线程若再开运行、父代理用 send_input 等给这个子线程派活，
//! Codex 不会把本连接重新挂上（只有新建或重新加载子线程才挂，`core/src/agent/control/spawn.rs`），
//! broker 收不到子线程的轮次与输出（E6 实测），收敛会一直等。所以退订过、尚未见到卸载的子线程记入
//! `released_children`，根线程开新运行时先 `thread/resume` 挂回；对已加载线程这只是重新订阅，不重载、不多起 MCP。
use super::*;
use serde_json::json;

impl Machine {
    /// 退订所有可释放的子线程；`only` 给出时只看这一个线程。
    pub(crate) fn release_idle_children(&mut self, only: Option<&str>, actions: &mut Vec<Action>) {
        if self.closing {
            return;
        }
        let epoch = self.relations.conn_epoch;
        let candidates: Vec<String> = match only {
            Some(thread) => vec![thread.to_owned()],
            None => self.relations.attachments.keys().cloned().collect(),
        };
        for thread in candidates {
            let attached = self
                .relations
                .attachments
                .get(&thread)
                .is_some_and(|a| a.valid && a.conn_epoch == epoch);
            if !attached
                || !self.child_subscription_idle(&thread)
                || !self.run_root_released(&thread)
            {
                continue;
            }
            self.detach_evidence(&thread);
            let released = &mut self.relations.aux.released_children;
            if released.len() >= MAX_ATTACHMENTS {
                released.pop_first();
            }
            released.insert(thread.clone());
            actions.push(Action::Request(RequestContext {
                resume_retried: false,
                stream_id: None,
                review_id: None,
                client: None,
                token: None,
                operation: Operation::Unsubscribe,
                method: "thread/unsubscribe".into(),
                params: Some(json!({ "threadId": thread })),
            }));
        }
    }

    /// 运行在 `root` 上开启：把本连接退订过、尚未卸载的后代子线程重新订阅回来。
    pub(crate) fn reattach_released_children(&mut self, root: &str, actions: &mut Vec<Action>) {
        if self.closing {
            return;
        }
        let threads: Vec<String> = self
            .relations
            .aux
            .released_children
            .iter()
            .filter(|t| !self.claims.contains_key(*t) && self.descends_from(t, root))
            .cloned()
            .collect();
        for thread in threads {
            // 发出即移出名单：回包前的重复开运行不重发；失败回包只告警，不再重试。
            self.relations.aux.released_children.remove(&thread);
            actions.push(Action::Request(RequestContext {
                resume_retried: false,
                stream_id: None,
                review_id: None,
                client: None,
                token: None,
                operation: Operation::ChildReattach {
                    thread: thread.clone(),
                },
                method: "thread/resume".into(),
                params: Some(json!({ "threadId": thread, "excludeTurns": true })),
            }));
        }
    }

    /// 重新订阅的回包：成功即恢复附着证据（之后可再随根退订）；失败只告警。
    pub(crate) fn child_reattach_reply(
        &mut self,
        thread: &str,
        ok: bool,
        actions: &mut Vec<Action>,
    ) {
        if ok {
            self.attach(thread, AttachmentSource::ExplicitResume);
        } else {
            actions.push(Action::Warn(format!("子线程 {thread} 重新订阅失败")));
        }
    }

    /// `thread` 沿血缘向上能到达 `root`（不含自身）。
    fn descends_from(&self, thread: &str, root: &str) -> bool {
        let mut current = thread.to_owned();
        for _ in 0..=MAX_DEPTH {
            let Some(edge) = self.thread_relation(&current) else {
                return false;
            };
            if edge.parent_thread_id == root {
                return true;
            }
            current = edge.parent_thread_id;
        }
        false
    }

    /// 子线程本身已无活动：未被认领、不在停止树里、关系层不再需要、没有活跃 goal、没有未完成的轮次。
    fn child_subscription_idle(&self, thread: &str) -> bool {
        !self.claims.contains_key(thread)
            && !self.tree_busy(Some(thread))
            && !self.relation_needs_subscription(thread)
            && !self
                .records
                .get(thread)
                .is_some_and(|r| r.goal_active || r.subscribed)
            && !self
                .relations
                .aux
                .observed
                .get(thread)
                .is_some_and(|turns| turns.iter().any(|t| t.started && !t.completed))
    }

    /// 沿血缘向上：必须是子线程（有父边），祖先里有 broker 跑过运行的线程，且没有任何祖先仍被认领或订阅。
    /// 不属于 broker 运行的线程（例如共享模式下别的客户端开的线程）一律不动。
    fn run_root_released(&self, thread: &str) -> bool {
        let mut current = thread.to_owned();
        let mut run_ancestor = false;
        for _ in 0..=MAX_DEPTH {
            let Some(edge) = self.thread_relation(&current) else {
                break;
            };
            current = edge.parent_thread_id;
            if self.claims.contains_key(&current) {
                return false;
            }
            if let Some(record) = self.records.get(&current) {
                if record.subscribed {
                    return false;
                }
                run_ancestor |= record.last_run.is_some();
            }
        }
        run_ancestor
    }
}
