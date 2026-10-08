//! 待回应事项：纯状态迁移、提交确认与有界等待。

use crate::claim::{
    Action, ClaimToken, ClientRequest, ConnId, HeldRequest, Machine, Operation, RequestContext,
    TimerKey, TimerKind,
};
use crate::{proto, requests, store::StoreOp};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};

mod lifecycle;
mod resolve;
mod wait;
use crate::runs::RunId;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[cfg(test)]
mod tests;

/// 服务端请求项从登记起 1 小时过期，与转发超时一致（第 8 节，评审 B2）。
pub const REQUEST_EXPIRY_MS: u64 = 3_600_000;
/// gd_ 项 24 小时过期（4.3 节：override 要等下一次运行）。
pub const GUARDIAN_EXPIRY_MS: u64 = 86_400_000;
/// 已关闭（resolved、expired、cancelled）的事项保留 24 小时供 `inbox/list{includeClosed}` 查看。
pub const CLOSED_RETENTION_MS: u64 = 86_400_000;
/// 全部事项（含已关闭的）上限；超出先淘汰最旧的已关闭项，仍超出时新的 gd_ 不生成、新的服务端请求按
/// 「无人可转」拒绝（用户 2026-10-04 同意的默认值）。
pub const MAX_ITEMS: usize = 1000;
/// `broker/inbox/wait` 等待者的全局上限（用户 2026-10-04 同意的默认值）。
pub const MAX_WAITERS: usize = 256;
/// 全部事项（params、答案、内部字段）序列化后的总字节上限；超出按「容量已满」处理（预审补充）。
pub const MAX_INBOX_BYTES: usize = 64 * 1024 * 1024;
/// `inbox/wait` 的 timeoutMs 上限（整数、0 到此值；超出或类型不对回 -32602）。
pub const MAX_WAIT_MS: u64 = 86_400_000;

/// 2.3 节 InboxItem 的 kind。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum InboxKind {
    Approval,
    Question,
    Mcp,
    Guardian,
}

/// 状态单向前进：pending → resolving → resolved | expired | cancelled（2.3 节）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum InboxStatus {
    Pending,
    Resolving,
    Resolved,
    Expired,
    Cancelled,
}

/// 2.3 节 InboxItem：对外返回、写进 inbox.json 的字段。内部字段（appRequestId、claimId、responseKind）
/// 由甲路另存，不出现在这里。`id` 是命令面规格 7.1 节的 typed ID（ap_、qu_、mc_、gd_）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InboxItem {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_thread_id: Option<String>,

    pub id: String,
    pub kind: InboxKind,
    pub thread_id: String,
    pub run_id: Option<RunId>,
    pub turn_id: Option<String>,
    pub method: String,
    /// 原样保存，由 CLI 脱敏显示。
    pub params: Value,
    /// 毫秒时间戳。
    pub created_at: u64,
    pub expires_at: u64,
    pub status: InboxStatus,
}

/// inbox.json 的 payload。不跨重启：启动时读出的事项一律标 expired，只供展示（4.3 节，评审 A5）。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InboxFile {
    pub items: Vec<InboxItem>,
}

/// 服务端请求项的来源：哪个线程、哪次认领、哪次运行、哪一轮。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxOrigin {
    pub thread_id: String,
    pub claim_id: u64,
    pub run_id: Option<RunId>,
    pub turn_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboxTimer {
    /// 事项过期；generation 防止旧计时器作用于已经换代的登记。
    Expiry { id: String, generation: u64 },
    /// `broker/inbox/wait` 的 timeoutMs 到了。
    WaitTimeout { waiter: u64 },
}

/// inbox 自己发给 app-server 的请求（Guardian override：先 resume 再调用，4.3 节）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboxRpc {
    GuardianResume {
        op: u64,
    },
    /// 随 RPC 上下文保留线程，不依赖业务操作登记的寿命。
    GuardianResumeFor {
        op: u64,
        thread_id: String,
    },
    GuardianOverride {
        op: u64,
    },
}

/// 带确认的回包最终怎样了（[`Action::AppReplyAcked`] 的结果）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplyOutcome {
    /// 已完整写进 app-server 连接（提交边界，2.3 节「提交边界分两种」）。
    Written,
    /// 写入失败或连接已关闭。
    Failed(String),
    /// 写出之前被 [`Action::CancelAppReply`] 撤回（B2：`serverRequest/resolved` 先到）。
    Cancelled,
}

const MAX_RESOLVERS: usize = 64;
// 上游 src/broker-server.ts:1200 的线上文本，与转发超时共用同一常量。
pub(crate) use crate::requests::TIMEOUT;
// 上游 src/broker-server.ts:1316。
const SHUTDOWN: &str = "Broker shutting down";

type Recipient = (ConnId, Value);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
enum Phase {
    Reply,
    Resume,
    Override,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct Entry {
    item: InboxItem,
    app_id: Option<Value>,
    claim_id: u64,
    generation: u64,
    closed_at: Option<u64>,
    answer: Option<Value>,
    op: Option<u64>,
    phase: Option<Phase>,
    upstream_closed: bool,
    expiry_requested: bool,
    shutdown_requested: bool,
    #[serde(skip)]
    byte_size: usize,
    waiters: Vec<Recipient>,
    first_alive: bool,
}

impl Entry {
    fn active(&self) -> bool {
        matches!(
            self.item.status,
            InboxStatus::Pending | InboxStatus::Resolving
        )
    }
    fn bytes(&self) -> usize {
        serde_json::to_vec(self).expect("事项可以序列化").len() + 256
    }
    fn token(&self) -> ClaimToken {
        ClaimToken {
            thread_id: self.item.thread_id.clone(),
            claim_id: self.claim_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Waiter {
    recipient: Recipient,
    id: Option<String>,
    thread: Option<String>,
}

/// 所有索引随事项删除；操作最多每事项一个，等待者分别受 64 与 256 限制。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Inbox {
    entries: BTreeMap<String, Entry>,
    app_ids: HashMap<(String, String), String>,
    operations: HashMap<u64, String>,
    waiters: BTreeMap<u64, Waiter>,
    sequence: u64,
    entry_bytes: usize,
    #[cfg(test)]
    save_count: usize,
    // 断开入口没有 actions 参数；保留等待者槽位直至运行时取出撤销动作，仍受 MAX_WAITERS 限制。
    disconnected_timers: Vec<u64>,
    #[cfg(test)]
    pub hook_log: Vec<String>,
}

impl Inbox {
    #[allow(unused_variables)]
    pub fn trace(&mut self, hook: &str) {
        #[cfg(test)]
        {
            if self.hook_log.len() == 4096 {
                self.hook_log.remove(0);
            }
            self.hook_log.push(hook.to_owned());
        }
    }
    fn next(&mut self) -> u64 {
        self.sequence = self.sequence.checked_add(1).expect("事项代号已耗尽");
        self.sequence
    }
    fn bytes(&self) -> usize {
        32 + self.entry_bytes
    }
    /// 仅重算发生变化的事项；容量判断和逐项淘汰不再序列化整个 inbox。
    fn refresh_entry(&mut self, id: &str) {
        let entry = self.entries.get_mut(id).expect("待更新事项存在");
        let size = entry.bytes();
        self.entry_bytes = self.entry_bytes - entry.byte_size + size;
        entry.byte_size = size;
    }
    fn insert_entry(&mut self, mut entry: Entry) {
        entry.byte_size = entry.bytes();
        self.entry_bytes += entry.byte_size;
        if let Some(old) = self.entries.insert(entry.item.id.clone(), entry) {
            self.entry_bytes -= old.byte_size;
        }
    }
}

fn digest(text: &str) -> String {
    format!("{:x}", Sha256::digest(text.as_bytes()))[..16].into()
}
fn status_event(status: InboxStatus) -> &'static str {
    match status {
        InboxStatus::Pending | InboxStatus::Resolving => "added",
        InboxStatus::Resolved => "resolved",
        InboxStatus::Expired => "expired",
        InboxStatus::Cancelled => "cancelled",
    }
}
fn error(reason: &str) -> Value {
    let message = match reason {
        "notFound" => "事项不存在",
        "alreadyResolved" => "事项已提交其他答案",
        "expired" => "事项已过期",
        "cancelled" => "事项已撤销",
        "tooManyWaiters" => "等待者数量已达上限",
        "shuttingDown" => "服务正在关闭",
        "capacityFull" => "待回应事项容量已满",
        _ => "提交回答失败",
    };
    proto::error_object(-32000, message, Some(json!({"reason":reason})))
}
fn reply(to: Recipient, result: Result<Value, Value>, actions: &mut Vec<Action>) {
    actions.push(Action::Send {
        conn: to.0,
        message: match result {
            Ok(value) => proto::response_ok(to.1, value),
            Err(error) => proto::response_err(to.1, error),
        },
    });
}
fn invalid(to: Recipient, actions: &mut Vec<Action>) {
    reply(
        to,
        Err(proto::error_object(-32602, "请求参数不合法", None)),
        actions,
    );
}

impl Machine {
    pub(crate) fn inbox_request(&mut self, request: ClientRequest, actions: &mut Vec<Action>) {
        self.inbox.trace("inbox_request");
        self.inbox_flush_actions(actions);
        let to = (request.conn, request.id);
        let params = request.params.unwrap_or(json!({}));
        if self.closing
            && matches!(
                request.method.as_str(),
                "broker/inbox/resolve" | "broker/inbox/wait"
            )
        {
            reply(to, Err(error("shuttingDown")), actions);
            return;
        }
        if !params.is_object() {
            invalid(to, actions);
            return;
        }
        match request.method.as_str() {
            "broker/inbox/resolve" => self.resolve_inbox(to, params, actions),
            "broker/inbox/wait" => self.wait_inbox(to, params, actions),
            "broker/inbox/list" => {
                let thread = params.get("threadId");
                let kind = params
                    .get("kind")
                    .map(|v| serde_json::from_value::<InboxKind>(v.clone()));
                if thread.is_some_and(|v| !v.is_string())
                    || kind.as_ref().is_some_and(Result::is_err)
                    || params.get("includeClosed").is_some_and(|v| !v.is_boolean())
                {
                    invalid(to, actions);
                    return;
                }
                let kind = kind.and_then(Result::ok);
                let closed = params["includeClosed"].as_bool().unwrap_or(false);
                let mut items: Vec<_> = self
                    .inbox
                    .entries
                    .values()
                    .filter(|e| {
                        (closed || e.active())
                            && thread.is_none_or(|v| v.as_str() == Some(e.item.thread_id.as_str()))
                            && kind.is_none_or(|k| k == e.item.kind)
                    })
                    .map(|e| e.item.clone())
                    .collect();
                items.sort_by(|a, b| (a.created_at, &a.id).cmp(&(b.created_at, &b.id)));
                reply(to, Ok(json!({"items":items})), actions);
            }
            _ => reply(
                to,
                Err(proto::error_object(-32601, "未知的待回应事项方法", None)),
                actions,
            ),
        }
    }

    pub(crate) fn inbox_busy(&self) -> bool {
        self.tree_busy(None)
            || self.inbox.entries.values().any(Entry::active)
            || !self.inbox.waiters.is_empty()
    }
    pub(crate) fn inbox_pending(&self, thread_id: &str) -> Vec<InboxItem> {
        let mut items: Vec<_> = self
            .inbox
            .entries
            .values()
            .filter(|e| e.active() && e.item.thread_id == thread_id)
            .map(|e| e.item.clone())
            .collect();
        items.sort_by(|a, b| (a.created_at, &a.id).cmp(&(b.created_at, &b.id)));
        items
    }
    /// 全工作区 pending 与 resolving 事项数（health 的 pending 用；含索引已删除线程上的 gd_）。
    pub(crate) fn inbox_pending_total(&self) -> usize {
        self.inbox.entries.values().filter(|e| e.active()).count()
    }
    pub(crate) fn inbox_pending_count(&self, thread_id: &str) -> u64 {
        self.inbox
            .entries
            .values()
            .filter(|e| e.active() && e.item.thread_id == thread_id)
            .count() as u64
    }
    fn save_inbox(&mut self, actions: &mut Vec<Action>) {
        #[cfg(test)]
        {
            self.inbox.save_count += 1;
        }
        actions.push(Action::Store(StoreOp::PutInbox(InboxFile {
            items: self
                .inbox
                .entries
                .values()
                .map(|e| e.item.clone())
                .collect(),
        })));
    }
    fn schedule_inbox(&self, id: &str, delay_ms: u64, actions: &mut Vec<Action>) {
        let entry = &self.inbox.entries[id];
        actions.push(Action::Schedule {
            token: entry.token(),
            kind: TimerKind::Inbox(InboxTimer::Expiry {
                id: id.into(),
                generation: entry.generation,
            }),
            delay_ms,
        });
    }
    fn remove_inbox(&mut self, id: &str, actions: &mut Vec<Action>) {
        if let Some(e) = self.inbox.entries.remove(id) {
            self.inbox.entry_bytes -= e.byte_size;
            if let Some(app) = &e.app_id {
                self.inbox
                    .app_ids
                    .remove(&(e.item.thread_id, proto::request_id_key(app)));
            }
            if let Some(op) = e.op {
                self.inbox.operations.remove(&op);
            }
            actions.push(Action::Unschedule {
                key: TimerKey::InboxItem(id.into()),
            });
        }
    }
    /// 保留活跃项；容量不足先淘汰最早关闭的事项，gd_ 去重也随之清除。
    fn room_inbox(&mut self, extra: usize, slots: usize, actions: &mut Vec<Action>) -> bool {
        while self.inbox.entries.len() + slots > MAX_ITEMS
            || self.inbox.bytes().saturating_add(extra) > MAX_INBOX_BYTES
        {
            let oldest = self
                .inbox
                .entries
                .iter()
                .filter(|(_, e)| !e.active())
                .min_by_key(|(_, e)| (e.closed_at, e.item.created_at))
                .map(|(id, _)| id.clone());
            let Some(oldest) = oldest else {
                return false;
            };
            self.remove_inbox(&oldest, actions);
        }
        true
    }
    fn add_inbox(&mut self, entry: Entry, actions: &mut Vec<Action>) -> bool {
        if !self.room_inbox(entry.bytes(), 1, actions) {
            actions.push(Action::Warn("待回应事项容量已满，拒绝新增事项".into()));
            self.save_inbox(actions);
            return false;
        }
        let item = entry.item.clone();
        if let Some(app) = &entry.app_id {
            self.inbox.app_ids.insert(
                (item.thread_id.clone(), proto::request_id_key(app)),
                item.id.clone(),
            );
        }
        self.inbox.insert_entry(entry);
        self.schedule_inbox(
            &item.id,
            item.expires_at.saturating_sub(self.clock.now_ms),
            actions,
        );
        self.notify_thread_ext(
            &item.thread_id,
            self.owner_conn(&item.thread_id),
            "broker/inbox/added",
            json!({"item":item}),
            actions,
        );
        self.wake_inbox(
            Some(&item),
            &item.thread_id,
            json!({"event":"added", "item":item}),
            actions,
        );
        self.save_inbox(actions);
        true
    }
    fn random_inbox_id(&mut self, prefix: &str) -> String {
        loop {
            let sequence = self.inbox.next();
            let id = format!(
                "{prefix}_{}",
                digest(&format!("{}:{sequence}", self.clock.entropy))
            );
            if !self.inbox.entries.contains_key(&id) {
                return id;
            }
        }
    }
    pub(crate) fn inbox_register_request(
        &mut self,
        origin: InboxOrigin,
        request: HeldRequest,
        expires_at_ms: Option<u64>,
        actions: &mut Vec<Action>,
    ) {
        self.inbox.trace("inbox_register_request");
        if self.closing {
            actions.push(Action::ServerError {
                id: request.id,
                error: requests::handler_error(SHUTDOWN),
            });
            return;
        }
        let key = (origin.thread_id.clone(), proto::request_id_key(&request.id));
        if self.inbox.app_ids.contains_key(&key) {
            return;
        }
        let mut request = request;
        let params = request.params.take().unwrap_or(json!({}));
        let (kind, prefix) = match request.method.as_str() {
            "item/commandExecution/requestApproval"
            | "item/fileChange/requestApproval"
            | "item/permissions/requestApproval" => (InboxKind::Approval, "ap"),
            "mcpServer/elicitation/request" => (InboxKind::Mcp, "mc"),
            _ => (InboxKind::Question, "qu"),
        };
        let id = if kind == InboxKind::Mcp && params["mode"] == "url" {
            params["elicitationId"]
                .as_str()
                .map(|s| format!("mc_{}", digest(s)))
                .filter(|id| !self.inbox.entries.contains_key(id))
                .unwrap_or_else(|| self.random_inbox_id(prefix))
        } else {
            self.random_inbox_id(prefix)
        };
        let entry = Entry {
            item: InboxItem {
                root_thread_id: None,
                parent_thread_id: None,

                id,
                kind,
                thread_id: origin.thread_id,
                run_id: origin.run_id,
                turn_id: origin.turn_id,
                method: request.method.clone(),
                params,
                created_at: self.clock.now_ms,
                expires_at: expires_at_ms
                    .unwrap_or_else(|| self.clock.now_ms.saturating_add(REQUEST_EXPIRY_MS)),
                status: InboxStatus::Pending,
            },
            app_id: Some(request.id.clone()),
            claim_id: origin.claim_id,
            generation: self.inbox.next(),
            closed_at: None,
            answer: None,
            op: None,
            phase: None,
            upstream_closed: false,
            expiry_requested: false,
            shutdown_requested: false,
            byte_size: 0,
            waiters: Vec::new(),
            first_alive: false,
        };
        if !self.add_inbox(entry, actions) {
            requests::reject_no_client(request, actions);
        }
    }
    pub(crate) fn inbox_guardian_review(
        &mut self,
        thread_id: &str,
        params: &Option<Value>,
        actions: &mut Vec<Action>,
    ) {
        self.inbox.trace("inbox_guardian_review");
        if self.closing {
            return;
        }
        let Some(claim) = self
            .claims
            .get(thread_id)
            .filter(|c| c.owner().is_some() && !c.joined() && !c.settling() && !c.precheck())
        else {
            return;
        };
        let Some(params) = params else {
            return;
        };
        // 上游 events.ts 519–537：优先取 review.status，兼容顶层状态字段。
        let status = params
            .pointer("/review/status")
            .or_else(|| params.get("decision"))
            .or_else(|| params.get("verdict"))
            .or_else(|| params.get("outcome"))
            .or_else(|| params.get("status"));
        if !status.is_some_and(|s| s == "denied" || s.get("type").is_some_and(|t| t == "denied")) {
            return;
        }
        let Some(review) = params["reviewId"].as_str().filter(|s| !s.is_empty()) else {
            actions.push(Action::Warn("Guardian 拒绝通知缺少 reviewId".into()));
            return;
        };
        if params
            .pointer("/action/type")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
        {
            actions.push(Action::Warn(
                "Guardian 拒绝通知缺少 action 或 action.type".into(),
            ));
            return;
        }
        let id = format!(
            "gd_{}",
            digest(&format!("{}{}", self.workspace_key, review))
        );
        if self.inbox.entries.contains_key(&id) {
            return;
        }
        let entry = Entry {
            item: InboxItem {
                root_thread_id: None,
                parent_thread_id: None,

                id,
                kind: InboxKind::Guardian,
                thread_id: thread_id.into(),
                run_id: claim.run_id.clone(),
                turn_id: claim.turn_id().map(str::to_owned),
                method: "item/autoApprovalReview/completed".into(),
                params: params.clone(),
                created_at: self.clock.now_ms,
                expires_at: self.clock.now_ms.saturating_add(GUARDIAN_EXPIRY_MS),
                status: InboxStatus::Pending,
            },
            app_id: None,
            claim_id: claim.claim_id,
            generation: self.inbox.next(),
            closed_at: None,
            answer: None,
            op: None,
            phase: None,
            upstream_closed: false,
            expiry_requested: false,
            shutdown_requested: false,
            byte_size: 0,
            waiters: Vec::new(),
            first_alive: false,
        };
        self.add_inbox(entry, actions);
    }
}
