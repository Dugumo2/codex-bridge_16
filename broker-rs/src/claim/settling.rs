//! 共享服务器吸收判定与回放（详细设计 3.1、3.4 节）。
//!
//! 先核对提交后的 started，再读服务器；读期间的证据也会重新核对。
//! held 的完成通知仅在定案后的提交阶段记账。
//!
//! 服务器读取经 H2 状态读取层（`status_read.rs`）：吸收判定用 Settlement 组合查询，
//! joined 核实用 Turn 查询；都不发 `includeTurns:true` 的全量读。

use super::status_read::{
    StatusCaller, StatusDone, StatusQuery, StatusResult, StatusTurn, StatusUnknown,
};
use super::*;

/// 与 held 顺序一致的通知字节、按转发编号登记的请求字节，以及本次认领的告警状态。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct HeldAccounting {
    notifications: Vec<usize>,
    requests: HashMap<String, usize>,
    soft_warned: bool,
    request_warned: bool,
    /// 只转发过未保存的 turn 生命周期，定案后必须核实目标的存活状态。
    lifecycle_incomplete: bool,
    /// 本次 settling 当前有效的吸收判定读取编号；只认这一次读取的完成，被取代的读取结果丢弃。
    settlement_read: u64,
    /// 本次 settling 因活动版本变化（`Unknown(Changed)`）立即重发的次数，不计入 attempt。
    settlement_changed: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct JoinedCheck {
    claim_id: u64,
    turn_id: String,
    attempt: u8,
    in_flight: bool,
    /// 在途核实读取的编号；只认这一次读取的完成。
    read_id: u64,
}

const JOINED_CHECK_RETRIES: u8 = 10;
/// 吸收判定的普通失败重试上限：每秒一次，用尽按 joined 保守定案。
const SETTLEMENT_RETRIES: u8 = 10;
/// 同一次 settling 中，活动版本变化后立即重算候选并重发的上限；超过按普通失败计。
const SETTLEMENT_CHANGED_REISSUES: u8 = 3;

/// 序列化到计数器，避免为估算再分配一份完整 JSON；每条收下的消息只计算一次。
fn message_bytes(method: &str, params: &Option<Value>) -> usize {
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(method.len());
    if let Some(params) = params {
        serde_json::to_writer(&mut counter, params).expect("JSON 值可序列化");
    }
    counter.0
}

impl Machine {
    pub(crate) fn begin_settlement(
        &mut self,
        thread_id: &str,
        attempt: u8,
        actions: &mut Vec<Action>,
    ) {
        let Some(claim) = self.claims.get(thread_id) else {
            return;
        };
        if !claim.settling() {
            actions.push(Action::Debug("吸收判定已经定案，忽略后续重试".into()));
            return;
        }
        let turn_id = claim.turn_id().map(str::to_owned);
        if claim.overflowed {
            if claim.request_pending {
                return;
            }
            if turn_id.as_ref().is_some_and(|id| !id.is_empty()) {
                self.settle(thread_id, true, turn_id, actions);
            } else {
                actions.push(Action::Warn(format!(
                    "线程 {thread_id} 溢出后的启动回包没有 turn id，放弃认领"
                )));
                let reply = self
                    .claims
                    .get_mut(thread_id)
                    .and_then(|c| c.settlement_reply.take());
                self.fail_settlement(thread_id, actions);
                if let Some((context, value)) = reply {
                    self.deliver(&context, Ok(value), actions);
                }
            }
            return;
        }
        if turn_id.is_none() || own_started(claim) {
            self.settle(thread_id, false, turn_id, actions);
            return;
        }
        // 候选每次发起时都用最新的 held 重算（版本变化后的重发也走这里）。
        let query = StatusQuery::Settlement {
            candidates: settlement_candidates(claim),
            input: claim.submitted_input.clone(),
        };
        let token = self.token(thread_id);
        let read_id = self.begin_status_read(
            thread_id,
            token,
            query,
            StatusCaller::Settlement { attempt },
            actions,
        );
        // 读取层从不同步回调，登记编号之后才可能收到完成。
        self.settling_accounting
            .entry(thread_id.into())
            .or_default()
            .settlement_read = read_id;
    }

    /// 吸收判定的普通失败出口（含 NotVisible、超时、无响应、版本变化重发用尽）：
    /// 第一次失败后先把回包交给客户端（undecided），每秒重试一次；第十次仍失败按 joined 保守定案。
    fn settlement_failed(&mut self, token: &ClaimToken, attempt: u8, actions: &mut Vec<Action>) {
        let thread_id = &token.thread_id;
        if attempt >= SETTLEMENT_RETRIES {
            actions.push(Action::Warn(
                "吸收判定重试十次仍失败，按 joined 定案".into(),
            ));
            let turn_id = self
                .claims
                .get(thread_id)
                .and_then(|c| c.turn_id().map(str::to_owned));
            self.settle(thread_id, true, turn_id, actions);
            return;
        }
        if let Some(claim) = self.claims.get_mut(thread_id) {
            if let State::Starting { undecided, .. } = &mut claim.state {
                *undecided = Some(attempt);
            }
            if let Some((context, result)) = claim.settlement_reply.take() {
                self.deliver(&context, Ok(result), actions);
            }
        }
        actions.push(Action::Schedule {
            token: token.clone(),
            kind: TimerKind::SettlementRetry {
                attempt: attempt + 1,
            },
            delay_ms: 1000,
        });
    }

    /// #8、#25：先完成归属更新，再按 ownStart 或 joined 的规则逐条回放。
    pub(super) fn settle(
        &mut self,
        thread_id: &str,
        joined: bool,
        target: Option<String>,
        actions: &mut Vec<Action>,
    ) {
        let Some(mut claim) = self.claims.remove(thread_id) else {
            return;
        };
        let (owner, original, submitted_at, held) = match &mut claim.state {
            State::Starting {
                settling: true,
                owner,
                turn_id,
                submitted_at,
                held,
                ..
            } => (
                owner.clone(),
                turn_id.clone(),
                *submitted_at,
                std::mem::take(held),
            ),
            _ => {
                self.claims.insert(thread_id.into(), claim);

                actions.push(Action::Warn("定案事件与认领阶段不匹配".into()));
                return;
            }
        };
        let target = target.or(original);
        if !joined {
            if let Some(turn) = target.as_ref().filter(|id| !id.is_empty()) {
                claim.owned_turns.insert(turn.clone());
            }
        }
        let reply = claim.settlement_reply.take();
        let own_start = (!joined)
            .then(|| {
                target.as_deref().and_then(|id| {
                    held.iter()
                        .enumerate()
                        .skip(submitted_at)
                        .find(|(_, n)| notification_turn_id(&n.params) == Some(id))
                        .map(|(index, _)| index)
                })
            })
            .flatten();
        let gone = claim.owner_gone;
        claim.held_deltas = 0;
        let lifecycle_incomplete = self
            .clear_held_accounting(thread_id, &mut claim)
            .lifecycle_incomplete;
        if !joined && claim.held_dropped_items > 0 {
            if let Some(run) = &claim.run_id {
                self.mark_progress_incomplete(run, actions);
            }
        }
        claim.state = State::Running {
            owner: owner.clone(),
            turn_id: target.clone().unwrap_or_default(),
            joined,
        };
        if joined {
            claim.resume_goal = false;
        }
        if joined {
            self.retire_thread_forwards(thread_id, claim.claim_id, actions);
        }
        if let Some(run) = &claim.run_id {
            self.run_settled(run, joined, actions);
            if let Some(turn) = target.as_ref().filter(|t| !t.is_empty()) {
                self.run_turn_started(run, thread_id, turn, actions);
            }
        }
        let claim_id = claim.claim_id;
        let settled_run = claim.run_id.clone();
        self.claims.insert(thread_id.into(), claim);
        // C1 需要有效认领的 token；定案钩子在放回之后、回放之前执行。
        if !joined {
            if let Some(run) = &settled_run {
                self.stall_owned_settled(run, actions);
            }
        }
        self.on_claim_settled(thread_id, claim_id, !joined, actions);
        if joined {
            if let Some(run) = &settled_run {
                self.on_handoff(run, crate::relations::HandoffKind::Joined, actions);
            }
        }
        // 认领放回之后再发定案通知（owner_conn 要从表里查），并先于回放。
        if let Some(run) = &settled_run {
            let absorbed_by = if joined { target.clone() } else { None };
            self.notify_run_settled(thread_id, run, absorbed_by, actions);
        }
        if joined {
            self.finish_joined_stop(thread_id, claim_id, actions);
        } else if let Some(turn) = &target {
            self.remember_owned_turn(thread_id, turn);
        }
        for (index, event) in held.into_iter().enumerate() {
            let id = notification_turn_id(&event.params);
            let owned = own_start.is_some_and(|start| index >= start);
            let commit = owned || target.is_none() || id.is_none() || id == target.as_deref();
            if commit {
                self.commit_notification(thread_id, event, actions);
            } else {
                self.forward_notification(thread_id, Some(&owner), gone, &event, actions);
            }
        }
        // #26a'：排队的交接在通知回放之后、挂起请求处理之前定案：定案为自己的就执行交接，
        // 之后的挂起请求按托管走 inbox；joined 回 `joined`；回放中已释放的由释放出口结算。
        // goal 恢复只由回放中真正的 turn/started（`started`）触发，这里不再调用。
        self.detach_settled(thread_id, claim_id, joined, actions);
        // 3.4：先回放通知，再处理仍属于本认领的请求。回放 completed 可能
        // 已由统一释放出口清空 held，不能向客户端发送已经过时的对话。
        let (requests, request_turn) = self
            .claims
            .get_mut(thread_id)
            .filter(|c| c.claim_id == claim_id)
            .map(|c| {
                let turn = c.turn_id().filter(|id| !id.is_empty()).map(str::to_owned);
                (std::mem::take(&mut c.held_requests), turn)
            })
            .unwrap_or_default();
        for request in requests {
            let turn_id = request
                .params
                .as_ref()
                .and_then(|p| p.get("turnId"))
                .and_then(Value::as_str);
            if !joined
                && (turn_id.is_none()
                    || request_turn.is_none()
                    || turn_id == request_turn.as_deref())
            {
                self.server_request(request, true, actions);
            } else {
                actions.push(Action::Dismiss { id: request.id });
            }
        }
        if let Some((context, mut value)) = reply {
            if let Some(object) = value.as_object_mut() {
                object.insert(
                    "absorbedBy".into(),
                    if joined {
                        target.clone().map(Value::String).unwrap_or(Value::Null)
                    } else {
                        Value::Null
                    },
                );
            }
            self.deliver(&context, Ok(value), actions);
        }

        if gone && self.claims.contains_key(thread_id) {
            if joined {
                self.release(thread_id, actions);
            } else {
                self.begin_stop(thread_id, StopReason::ClientDisconnected, false, actions);
            }
        }
        if joined
            && lifecycle_incomplete
            && self
                .claims
                .get(thread_id)
                .is_some_and(|c| c.claim_id == claim_id)
        {
            self.begin_joined_check(thread_id, actions);
        }
    }

    /// #10：失败不能把 held 当作本认领的生命周期，只把原通知交给原 owner。
    pub(super) fn fail_settlement(&mut self, thread_id: &str, actions: &mut Vec<Action>) {
        if let Some(mut claim) = self.claims.remove(thread_id) {
            self.clear_held_accounting(thread_id, &mut claim);
            let owner = claim.owner().cloned();
            let gone = claim.owner_gone;
            let held = match &mut claim.state {
                State::Starting { held, .. } => std::mem::take(held),
                _ => Vec::new(),
            };
            for request in std::mem::take(&mut claim.held_requests) {
                actions.push(Action::Dismiss { id: request.id });
            }
            let overflowed = claim.overflowed;
            self.claims.insert(thread_id.into(), claim);
            for event in held {
                self.forward_notification(thread_id, owner.as_ref(), gone, &event, actions);
            }
            if overflowed {
                let claim_id = self.claims[thread_id].claim_id;
                self.finish_joined_stop(thread_id, claim_id, actions);
                self.release_with_kind(thread_id, ReleaseKind::StartFailed, actions);
                return;
            }
        }
        if self.stop_busy(thread_id) {
            self.stop_no_turn(thread_id);
        } else {
            let kind = if self.claims.get(thread_id).is_some_and(|c| {
                matches!(
                    c.state,
                    State::Starting {
                        req: ReqKind::TurnSteer,
                        ..
                    }
                )
            }) {
                ReleaseKind::Joined
            } else {
                ReleaseKind::StartFailed
            };
            self.release_with_kind(thread_id, kind, actions);
        }
    }
}

impl Machine {
    /// settling 期间到达的通知进缓冲（M7 预算，S1-评审第 22 节方案 A）。返回 false 表示本认领已不在 settling
    /// （这条通知触发硬上限且回包已到，已按 joined 定案），调用方不再做 own 判断。
    ///
    /// 规则（预审后修订，任务说明第 7 节优先）：
    /// 0. 认领已溢出（`overflowed`，回包还没到）：只有方法名以 `turn/` 开头的生命周期通知继续缓冲
    ///    （另限 [`OVERFLOW_LIFECYCLE_ITEMS`] 条，超过只转发），其余只转发给 owner、不驱动状态；
    /// 1. 先估算字节（方法名长度 + params 序列化长度）；
    /// 2. 可丢通知：本认领已达软上限（条数 [`SETTLING_SOFT_ITEMS`] 或字节 [`SETTLING_SOFT_BYTES`]），
    ///    或是 delta 且 `held_deltas` 已到 [`MAX_HELD_DELTAS`]：不缓冲；丢的是 item 通知时 `held_dropped_items += 1`；
    ///    软上限引起的首次丢弃记一条告警（之后不逐条告警）；
    /// 3. 其余照收，`held_bytes`、`Machine.settling_bytes` 增量累加；
    /// 4. 收下后本认领超过硬上限（[`SETTLING_HARD_ITEMS`] 或 [`SETTLING_HARD_BYTES`]）→ 对本认领调用
    ///    [`Self::settling_overflow`]；全局超过 [`SETTLING_GLOBAL_BYTES`] → 对占用最多的、尚未溢出的 settling 认领调用
    ///    （可能不是本认领）。
    pub(crate) fn hold_notification(
        &mut self,
        thread_id: &str,
        event: HeldNotification,
        actions: &mut Vec<Action>,
    ) -> bool {
        let Some(claim) = self.claims.get_mut(thread_id).filter(|c| c.settling()) else {
            return false;
        };
        let State::Starting { held, .. } = &mut claim.state else {
            return false;
        };
        let delta = event.method.ends_with("/delta");
        let soft = held.len() >= SETTLING_SOFT_ITEMS || claim.held_bytes >= SETTLING_SOFT_BYTES;
        if !claim.overflowed
            && droppable_while_settling(&event.method)
            && (soft || delta && claim.held_deltas >= MAX_HELD_DELTAS)
        {
            if matches!(event.method.as_str(), "item/started" | "item/completed") {
                claim.held_dropped_items += 1;
            }
            let accounting = self
                .settling_accounting
                .entry(thread_id.into())
                .or_default();
            if soft && !std::mem::replace(&mut accounting.soft_warned, true) {
                actions.push(Action::Warn(format!(
                    "线程 {thread_id} 的 settling 缓冲达到软上限，省略后续可丢通知"
                )));
            }
            return true;
        }
        let bytes = message_bytes(&event.method, &event.params);
        if claim.overflowed
            && (!event.method.starts_with("turn/")
                || held.len() >= OVERFLOW_LIFECYCLE_ITEMS
                || claim.held_bytes + bytes > SETTLING_HARD_BYTES
                || self.settling_bytes + bytes > SETTLING_GLOBAL_BYTES)
        {
            if event.method.starts_with("turn/") {
                self.settling_accounting
                    .entry(thread_id.into())
                    .or_default()
                    .lifecycle_incomplete = true;
            }
            let owner = claim.owner().cloned();
            let gone = claim.owner_gone;
            self.forward_notification(thread_id, owner.as_ref(), gone, &event, actions);
            return true;
        }
        held.push(event);
        claim.held_deltas += usize::from(delta);
        claim.held_bytes += bytes;
        self.settling_bytes += bytes;
        self.settling_accounting
            .entry(thread_id.into())
            .or_default()
            .notifications
            .push(bytes);
        if !claim.overflowed
            && (held.len() > SETTLING_HARD_ITEMS || claim.held_bytes > SETTLING_HARD_BYTES)
        {
            self.settling_overflow(thread_id, actions);
        }
        while self.settling_bytes > SETTLING_GLOBAL_BYTES {
            let largest = self
                .claims
                .iter()
                .filter(|(_, c)| c.settling() && !c.overflowed)
                .max_by(|(a_id, a), (b_id, b)| {
                    a.held_bytes.cmp(&b.held_bytes).then_with(|| b_id.cmp(a_id))
                })
                .map(|(id, _)| id.clone());
            let Some(largest) = largest else { break };
            self.settling_overflow(&largest, actions);
        }
        self.claims.get(thread_id).is_some_and(Claim::settling)
    }

    /// settling 期间到达的服务端请求进 `held_requests`。请求的字节同样计入 `held_bytes` 与 `settling_bytes`
    /// （活性预审第 5 条）。以下情况不答（`Action::Dismiss`，同时清 rpc 层的去重登记，同一请求重发时会再交付一次），
    /// 每个认领只告警一次：已有 [`MAX_HELD_REQUESTS`] 个；收下后本认领字节超过 [`SETTLING_HARD_BYTES`] 或全局超过
    /// [`SETTLING_GLOBAL_BYTES`]；认领已溢出。请求本身不触发认领溢出。
    /// 不回错误：共享服务器上回错误就是替别人拒绝（4.2 节第 1 条、3.4 节「宁可漏答」）。
    pub(crate) fn hold_request(
        &mut self,
        thread_id: &str,
        request: HeldRequest,
        actions: &mut Vec<Action>,
    ) {
        let Some(claim) = self.claims.get_mut(thread_id).filter(|c| c.settling()) else {
            actions.push(Action::Dismiss { id: request.id });
            return;
        };
        let bytes = message_bytes(&request.method, &request.params);
        let accounting = self
            .settling_accounting
            .entry(thread_id.into())
            .or_default();
        if claim.overflowed
            || claim.held_requests.len() >= MAX_HELD_REQUESTS
            || claim.held_bytes + bytes > SETTLING_HARD_BYTES
            || self.settling_bytes + bytes > SETTLING_GLOBAL_BYTES
        {
            if !std::mem::replace(&mut accounting.request_warned, true) {
                actions.push(Action::Warn(format!(
                    "线程 {thread_id} 的 settling 挂起请求达到预算，后续超限请求交还共享服务器"
                )));
            }
            actions.push(Action::Dismiss { id: request.id });
            return;
        }
        accounting
            .requests
            .insert(request.forward_id.clone(), bytes);
        claim.held_bytes += bytes;
        self.settling_bytes += bytes;
        claim.held_requests.push(request);
    }

    /// 硬上限触发（每个认领最多一次）：记告警（线程、条数、字节），然后
    /// - **start 回包已到**（`turn_id` 已知）：`settle(thread, true, turn_id)`，与 undecided 十次后的出口相同；
    ///   held 按正常 joined 回放规则处理（3.4 节：属于该 turn 和不带 turn id 的照样重新提交，目标轮的 completed
    ///   会按 #15 释放认领），挂起请求按 joined 不答；
    /// - **回包还没到**（`turn_id` 为 None，含 resume 屏障尚未放行）：不离开 Starting，置 `overflowed`；held 里
    ///   不以 `turn/` 开头的通知立刻按顺序只转发给 owner 并移出、扣回字节；挂起请求全部 Dismiss。回包到达时：
    ///   成功且带 turn id → `settle(thread, true, 该 turn id)`（absorbedBy 为该字符串）；成功但没有 turn id →
    ///   按 #10 定案失败处理（`fail_settlement`）并告警；失败、超时 → 走原有的失败出口。定案通知只发一次。
    pub(crate) fn settling_overflow(&mut self, thread_id: &str, actions: &mut Vec<Action>) {
        let Some(mut claim) = self.claims.remove(thread_id) else {
            return;
        };
        if !claim.settling() || claim.overflowed {
            self.claims.insert(thread_id.into(), claim);
            return;
        }
        claim.overflowed = true;
        let State::Starting { held, .. } = &claim.state else {
            unreachable!()
        };
        actions.push(Action::Warn(format!(
            "线程 {thread_id} 的 settling 缓冲溢出：{} 条通知、{} 字节，按 joined 保守处理",
            held.len(),
            claim.held_bytes
        )));
        if let Some(turn) = claim
            .turn_id()
            .filter(|id| !id.is_empty())
            .map(str::to_owned)
        {
            self.claims.insert(thread_id.into(), claim);
            self.settle(thread_id, true, Some(turn), actions);
            return;
        }
        let sizes = self
            .settling_accounting
            .get_mut(thread_id)
            .map(|a| std::mem::take(&mut a.notifications))
            .unwrap_or_default();
        // 取走并清零后，只把有界生命周期重新登记；通知大小沿用首次入队的缓存。
        let mut accounting = self.clear_held_accounting(thread_id, &mut claim);
        let owner = claim.owner().cloned();
        let gone = claim.owner_gone;
        let State::Starting {
            held, submitted_at, ..
        } = &mut claim.state
        else {
            unreachable!()
        };
        let old_submitted = *submitted_at;
        *submitted_at = 0;
        for (index, (event, bytes)) in std::mem::take(held).into_iter().zip(sizes).enumerate() {
            if event.method.starts_with("turn/")
                && held.len() < OVERFLOW_LIFECYCLE_ITEMS
                && claim.held_bytes + bytes <= SETTLING_HARD_BYTES
                && self.settling_bytes + bytes <= SETTLING_GLOBAL_BYTES
            {
                *submitted_at += usize::from(index < old_submitted);
                claim.held_bytes += bytes;
                self.settling_bytes += bytes;
                accounting.notifications.push(bytes);
                held.push(event);
            } else {
                accounting.lifecycle_incomplete |= event.method.starts_with("turn/");
                self.forward_notification(thread_id, owner.as_ref(), gone, &event, actions);
            }
        }
        for request in std::mem::take(&mut claim.held_requests) {
            actions.push(Action::Dismiss { id: request.id });
        }
        self.settling_accounting
            .insert(thread_id.into(), accounting);
        self.claims.insert(thread_id.into(), claim);
    }

    /// 所有离开 settling 的出口共用，重复清理只能取到零，不能重复扣款。
    pub(super) fn clear_held_accounting(
        &mut self,
        thread: &str,
        claim: &mut Claim,
    ) -> HeldAccounting {
        self.settling_bytes -= std::mem::take(&mut claim.held_bytes);
        claim.held_deltas = 0;
        let mut accounting = self.settling_accounting.remove(thread).unwrap_or_default();
        accounting.notifications.clear();
        accounting.requests.clear();
        accounting
    }

    /// resolved 在通知入队前撤销挂起请求；大小来自入队缓存，不再序列化。
    pub(crate) fn remove_held_request(&mut self, thread: &str, id: &Value) {
        let Some(claim) = self.claims.get_mut(thread) else {
            return;
        };
        let key = proto::request_id_key(id);
        let accounting = self.settling_accounting.get_mut(thread);
        let mut removed = 0;
        let mut accounting = accounting;
        claim.held_requests.retain(|request| {
            if proto::request_id_key(&request.id) != key {
                return true;
            }
            if let Some(accounting) = accounting.as_mut() {
                removed += accounting.requests.remove(&request.forward_id).unwrap_or(0);
            }
            false
        });
        claim.held_bytes -= removed;
        self.settling_bytes -= removed;
    }

    pub(super) fn overflowed_start(&self, context: &RequestContext) -> bool {
        matches!(context.operation, Operation::Stream { .. })
            && context.token.as_ref().is_some_and(|token| {
                self.current(token)
                    && self.claims[&token.thread_id].settling()
                    && self.claims[&token.thread_id].overflowed
            })
    }

    /// stop 取消未发出的启动或核实无 turn 时会直接清 settling 标记；这些出口只转发旧通知。
    /// 只扫描预算登记（每个 settling 认领至多一项），不触及 stop 的归属判断。
    pub(super) fn clear_cancelled_settling(&mut self, actions: &mut Vec<Action>) {
        let threads: Vec<_> = self
            .settling_accounting
            .keys()
            .filter(|thread| self.claims.get(*thread).is_some_and(|c| !c.settling()))
            .cloned()
            .collect();
        for thread in threads {
            let mut claim = self.claims.remove(&thread).expect("已核对取消认领");
            self.clear_held_accounting(&thread, &mut claim);
            let owner = claim.owner().cloned();
            let gone = claim.owner_gone;
            if let State::Starting { held, .. } = &mut claim.state {
                for event in std::mem::take(held) {
                    self.forward_notification(&thread, owner.as_ref(), gone, &event, actions);
                }
            }
            for request in std::mem::take(&mut claim.held_requests) {
                actions.push(Action::Dismiss { id: request.id });
            }
            self.claims.insert(thread, claim);
        }
    }

    /// review 的 turn 属于子线程，父缓冲只能转发；不得拿子轮编号提交父线程生命周期。
    pub(super) fn abandon_review_parent_buffer(
        &mut self,
        thread: &str,
        actions: &mut Vec<Action>,
    ) -> bool {
        let Some(mut claim) = self.claims.remove(thread) else {
            return false;
        };
        let incomplete = self
            .clear_held_accounting(thread, &mut claim)
            .lifecycle_incomplete;
        let owner = claim.owner().cloned().expect("审查父认领有 owner");
        if let State::Starting { held, .. } = &mut claim.state {
            for event in std::mem::take(held) {
                self.forward_notification(thread, Some(&owner), claim.owner_gone, &event, actions);
            }
        }
        for request in std::mem::take(&mut claim.held_requests) {
            actions.push(Action::Dismiss { id: request.id });
        }
        claim.resume_goal = false;
        claim.state = State::Running {
            owner,
            turn_id: String::new(),
            joined: true,
        };
        let claim_id = claim.claim_id;
        if let Some(run) = &claim.run_id {
            self.run_settled(run, true, actions);
        }
        self.claims.insert(thread.into(), claim);
        self.retire_thread_forwards(thread, claim_id, actions);
        self.finish_joined_stop(thread, claim_id, actions);
        self.detach_settled(thread, claim_id, true, actions);
        incomplete
    }

    /// 一次立即核实，失败后最多重试十次；每次间隔一秒，RPC 自带超时。
    pub(super) fn begin_joined_check(&mut self, thread: &str, actions: &mut Vec<Action>) {
        if self.closing || self.joined_checks.contains_key(thread) {
            return;
        }
        let Some(claim) = self
            .claims
            .get(thread)
            .filter(|c| c.joined() && !c.settling())
        else {
            return;
        };
        let Some(turn_id) = claim
            .turn_id()
            .filter(|id| !id.is_empty())
            .map(str::to_owned)
        else {
            actions.push(Action::Warn(format!(
                "线程 {thread} 的 joined 生命周期缺失且没有目标编号，释放认领"
            )));
            self.release_with_kind(thread, ReleaseKind::Joined, actions);
            return;
        };
        self.joined_checks.insert(
            thread.into(),
            JoinedCheck {
                claim_id: claim.claim_id,
                turn_id,
                attempt: 0,
                in_flight: false,
                read_id: 0,
            },
        );
        self.joined_check_retry(&self.token(thread).expect("joined 认领存在"), 0, actions);
    }

    pub(super) fn joined_check_retry(
        &mut self,
        token: &ClaimToken,
        attempt: u8,
        actions: &mut Vec<Action>,
    ) {
        if self.closing || !self.current(token) {
            return;
        }
        let Some(check) = self
            .joined_checks
            .get_mut(&token.thread_id)
            .filter(|c| c.claim_id == token.claim_id && c.attempt == attempt && !c.in_flight)
        else {
            return;
        };
        check.in_flight = true;
        let target = check.turn_id.clone();
        let read_id = self.begin_status_read(
            &token.thread_id,
            Some(token.clone()),
            StatusQuery::Turn { target },
            StatusCaller::JoinedCheck { attempt },
            actions,
        );
        // 读取层从不同步回调，登记编号之后才可能收到完成。
        if let Some(check) = self.joined_checks.get_mut(&token.thread_id) {
            check.read_id = read_id;
        }
    }

    pub(super) fn cancel_joined_check(&mut self, thread: &str, actions: &mut Vec<Action>) {
        if self.joined_checks.remove(thread).is_some() {
            actions.push(Action::Unschedule {
                key: TimerKey::JoinedCheck(thread.into()),
            });
        }
    }

    /// 测试同时检查缓存逐条大小、每认领总计与全局总计，避免两个计数同错仍通过。
    #[cfg(test)]
    pub(super) fn assert_held_accounting(&self) {
        let mut total = 0;
        for (thread, accounting) in &self.settling_accounting {
            let claim = self.claims.get(thread).expect("预算登记必须有对应认领");
            assert!(claim.settling(), "预算登记只属于 settling：{thread}");
            let State::Starting { held, .. } = &claim.state else {
                unreachable!()
            };
            assert_eq!(
                accounting.notifications.len(),
                held.len(),
                "通知大小必须逐条对应：{thread}"
            );
            assert_eq!(
                accounting.requests.len(),
                claim.held_requests.len(),
                "请求大小必须逐条对应：{thread}"
            );
            assert!(claim
                .held_requests
                .iter()
                .all(|r| accounting.requests.contains_key(&r.forward_id)));
            let bytes = accounting.notifications.iter().sum::<usize>()
                + accounting.requests.values().sum::<usize>();
            assert_eq!(
                bytes, claim.held_bytes,
                "逐条字节与认领总计必须一致：{thread}"
            );
            total += bytes;
        }
        assert_eq!(total, self.settling_bytes, "逐条缓存字节与全局总计必须一致");
    }
}

/// 软上限后可以不缓冲的通知：只影响进度显示和降级快照，结果可由 thread/read 补回（S1-评审第 22 节）。
/// 方法名以 `/delta` 结尾的，以及 `item/started`、`item/completed`。其余一律照收，包括生命周期通知、
/// `item/autoApprovalReview/completed`（要生成 gd_ 项）和未知方法。
pub(crate) fn droppable_while_settling(method: &str) -> bool {
    method.ends_with("/delta") || method == "item/started" || method == "item/completed"
}

fn own_started(claim: &Claim) -> bool {
    match &claim.state {
        State::Starting {
            held,
            submitted_at,
            turn_id: Some(turn_id),
            ..
        } => held.iter().skip(*submitted_at).any(|n| {
            n.method == "turn/started" && notification_turn_id(&n.params) == Some(turn_id)
        }),
        _ => false,
    }
}

/// Settlement 的候选：`held[submitted_at..]` 中 `turn/completed` 的轮次，排除本次回包的轮次，
/// 保持通知原顺序；同一轮次重复完成只列一次（重复项不会改变结果，只会占用候选上限）。
fn settlement_candidates(claim: &Claim) -> Vec<String> {
    let State::Starting {
        held,
        submitted_at,
        turn_id,
        ..
    } = &claim.state
    else {
        return Vec::new();
    };
    let mut candidates: Vec<String> = Vec::new();
    for id in held
        .iter()
        .skip(*submitted_at)
        .filter(|n| n.method == "turn/completed")
        .filter_map(|n| notification_turn_id(&n.params))
    {
        if Some(id) != turn_id.as_deref() && !candidates.iter().any(|c| c == id) {
            candidates.push(id.to_owned());
        }
    }
    candidates
}

impl Machine {
    /// Settlement 读取完成（H2 接口第 6 节）。先核对认领与读取编号，再检查 own_started：
    /// `FirstActive` 按原逻辑定案，`Carrier` 按 joined、`NoCarrier` 按自有定案；
    /// `Unknown(Changed)` 立即用最新 held 重算候选重发，不计 attempt，每次 settling 最多 3 次；
    /// 其余 `Unknown`（含 NotVisible）走有界失败重试，用尽按 joined 保守定案。
    pub fn settlement_status_done(&mut self, done: StatusDone, actions: &mut Vec<Action>) {
        let StatusCaller::Settlement { attempt } = done.caller else {
            return;
        };
        let Some(token) = done.token else { return };
        if !self.current(&token) {
            actions.push(Action::Warn("丢弃过期认领的吸收判定读结果".into()));
            return;
        }
        let thread_id = token.thread_id.clone();
        let claim = &self.claims[&thread_id];
        if !claim.settling() {
            actions.push(Action::Debug("丢弃定案之后才到达的吸收判定读结果".into()));
            return;
        }
        let progress = self.settling_accounting.get(&thread_id);
        if progress.is_none_or(|p| p.settlement_read != done.read_id) {
            actions.push(Action::Debug(format!(
                "丢弃已被取代的吸收判定读取 {}",
                done.read_id
            )));
            return;
        }
        let turn_id = claim.turn_id().map(str::to_owned);
        // 读取期间可能来了本 turn 的 started，其优先级高于服务器结果。
        if own_started(claim) {
            self.settle(&thread_id, false, turn_id, actions);
            return;
        }
        match done.result {
            StatusResult::FirstActive(active) => {
                let joined = turn_id.as_deref() != Some(active.as_str());
                self.settle(&thread_id, joined, Some(active), actions);
            }
            StatusResult::Carrier(carrier) => {
                self.settle(&thread_id, true, Some(carrier), actions);
            }
            StatusResult::NoCarrier => self.settle(&thread_id, false, turn_id, actions),
            StatusResult::Unknown(StatusUnknown::Changed)
                if progress.is_some_and(|p| p.settlement_changed < SETTLEMENT_CHANGED_REISSUES) =>
            {
                let progress = self
                    .settling_accounting
                    .get_mut(&thread_id)
                    .expect("已核对读取登记");
                progress.settlement_changed += 1;
                actions.push(Action::Debug(format!(
                    "线程 {thread_id} 吸收判定期间活动版本变化，第 {} 次重算候选并重发",
                    progress.settlement_changed
                )));
                self.begin_settlement(&thread_id, attempt, actions);
            }
            other => {
                actions.push(Action::Debug(format!(
                    "线程 {thread_id} 吸收判定第 {attempt} 次读取无法确认：{other:?}"
                )));
                self.settlement_failed(&token, attempt, actions);
            }
        }
    }

    /// JoinedCheck 读取完成（H2 接口第 6 节）：`Found{inProgress}` 取消核实；`Found{终态}` 把 notLoaded
    /// 壳交给 `run_turn_completed`（条目由结果补读接上），随即释放认领，不等补读；`Absent` 沿用
    /// 「目标缺失则释放」；其余 `Unknown` 走有界重试，用尽保守释放。
    pub fn joined_check_done(&mut self, done: StatusDone, actions: &mut Vec<Action>) {
        let StatusCaller::JoinedCheck { attempt } = done.caller else {
            return;
        };
        let Some(token) = &done.token else { return };
        let thread = &token.thread_id;
        let Some(check) = self
            .joined_checks
            .get(thread)
            .filter(|c| {
                self.current(token)
                    && c.claim_id == token.claim_id
                    && c.attempt == attempt
                    && c.in_flight
                    && c.read_id == done.read_id
            })
            .cloned()
        else {
            actions.push(Action::Debug(format!(
                "丢弃已失效的 joined 核实读取 {}",
                done.read_id
            )));
            return;
        };
        match done.result {
            StatusResult::Found {
                status: StatusTurn::InProgress,
                ..
            } => {
                self.cancel_joined_check(thread, actions);
                return;
            }
            StatusResult::Found { shell, .. } => {
                let run = self.claims[thread].run_id.clone();
                self.run_turn_completed(thread, run.as_deref(), &shell, actions);
                self.mark_ended(thread, &check.turn_id);
                self.release_with_kind(thread, ReleaseKind::Joined, actions);
                return;
            }
            StatusResult::Absent => {
                self.release_with_kind(thread, ReleaseKind::Joined, actions);
                return;
            }
            other => actions.push(Action::Debug(format!(
                "线程 {thread} 的 joined 核实第 {attempt} 次无法确认：{other:?}"
            ))),
        }
        if attempt >= JOINED_CHECK_RETRIES {
            actions.push(Action::Warn(format!(
                "线程 {thread} 的 joined 生命周期核实重试十次仍失败，保守释放认领"
            )));
            self.release_with_kind(thread, ReleaseKind::Joined, actions);
        } else {
            let check = self.joined_checks.get_mut(thread).expect("核实登记存在");
            check.attempt += 1;
            check.in_flight = false;
            actions.push(Action::Schedule {
                token: token.clone(),
                kind: TimerKind::JoinedCheckRetry {
                    attempt: attempt + 1,
                },
                delay_ms: 1000,
            });
        }
    }
}
