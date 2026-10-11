//! 关闭第 1–3 步的共享专用部分（详细设计第 5 节；上游 broker-server.ts:1288–1310）。
//!
//! core 的推进顺序（`core::begin_shutdown`，仅共享服务器且 app-server 连接仍在时）：
//! 1. [`Event::ShutdownBegin`] → [`Machine::quiesce`]：从这一刻起**不再开出任何新 turn、不再把 goal 改回 active**
//!    （规则预审第 1 条）：新的流式请求和带 cxbContinue 的 turn/start 回 `SHUTTING_DOWN`；还没发出的工作按关闭出口
//!    结算——续跑预检不再提交（#2d' 的关闭出口）、resume 屏障后尚未转发的 start 回 `SHUTTING_DOWN` 并释放、
//!    goal 恢复不再发出新的 active 写入（读回包到了也放弃）。已经发出的工作的通知、回包、settling 定案照常处理；
//! 2. 每处理完一条消息看 [`Machine::settling_count`]，为 0 或等满 5 秒进入第 3 步；
//! 3. [`Event::ShutdownStopOwn`] → [`Machine::begin_shutdown_stops`]：对自有、非 joined、非 settling 的认领，
//!    goal 为 active 的先 `thread/goal/set {status:"paused"}`，回包回来（成功或失败）后，有 turn_id 的再
//!    `turn/interrupt {threadId, turnId}`；各线程之间并发。[`Machine::shutdown_stops_pending`] 为 0 或等满 5 秒，
//!    进入第 4 步 [`Event::Shutdown`]。joined 的 turn、别的客户端的 turn、仍在 settling 的不碰；
//! 4. 之后与私有服务器相同。
//!
//! 约定：
//! - 第 3 步的请求不是 StopOp：不登记停止操作、不改运行记录（第 4 步统一标 lost/unconfirmed，
//!   broker 退出不能当作共享服务器上 turn 已停的证明）；失败只记告警；
//! - 暂停请求走既有的 goal 变更登记（`track_goal_requests` 会把 `thread/goal/set` 包成 GoalChange），
//!   回包解包后到 [`Machine::shutdown_stop_reply`]；
//! - 「自有」= `Owner::Client` 或 `Owner::Broker`（托管）且非 joined；续跑预检中的认领（`precheck`）不算，
//!   第 4 步取消；没有 turn_id 且 goal 不是 active 的（例如 AwaitingContinuation 之外的空档）什么都不发；
//! - goal 是否 active 以 `ThreadRecord.goal_active` 为准（上游 goalActiveThreads）；内部恢复及普通客户端
//!   尚未落定、可能激活 goal 的写入也按 active 处理（先发暂停，出站顺序保证其排在激活写入之后）；
//! - 第 4 步之后才到的暂停、中断回包（`closing` 为真）只记日志，不再发中断、不再结算。

use super::*;

/// 缺省 status 会创建 active goal（上游 src/goals.ts:81–101）；未知值也不能视为制动。
pub(super) fn goal_set_can_activate(params: &Option<Value>) -> bool {
    !matches!(
        params
            .as_ref()
            .and_then(|p| p.get("status"))
            .and_then(Value::as_str),
        Some("paused" | "blocked" | "complete" | "usageLimited" | "budgetLimited")
    )
}

/// 第 3 步对一个线程发出的请求所处阶段。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownStopPhase {
    PauseGoal,
    Interrupt,
}

/// 一个线程上第 3 步的进度。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShutdownStop {
    pub claim_id: u64,
    pub phase: ShutdownStopPhase,
    /// 暂停回包回来后要中断的 turn（没有就到此结束）。
    pub turn_id: Option<String>,
}

impl Machine {
    /// goal 请求已经由 stop 包装并分配唯一编号；这里只记激活可能性，不另存请求或线程副本。
    pub(super) fn track_activating_goal_changes(&mut self, actions: &[Action]) {
        for action in actions {
            if let Action::Request(context) = action {
                if context.method == "thread/goal/set" && goal_set_can_activate(&context.params) {
                    if let Operation::GoalChange { id, .. } = context.operation {
                        self.activating_goal_changes.insert(id);
                    }
                }
            }
        }
        self.activating_goal_changes
            .retain(|id| self.stops.changes.contains_key(id));
    }

    fn activating_goal_pending(&self, thread: &str) -> bool {
        self.activating_goal_changes.iter().any(|id| {
            self.stops
                .changes
                .get(id)
                .is_some_and(|change| change.thread == thread)
        })
    }

    /// 包装尚未解开时先排除旧阶段与重复回包，避免它们再次学习 goal 事实。
    pub(super) fn stale_shutdown_reply(&self, context: &RequestContext) -> bool {
        let mut operation = &context.operation;
        while let Operation::GoalChange { inner, .. } | Operation::GoalRead { inner, .. } =
            operation
        {
            operation = inner;
        }
        let Operation::ShutdownStop(phase) = operation else {
            return false;
        };
        context.token.as_ref().is_none_or(|token| {
            self.shutdown_stops
                .get(&token.thread_id)
                .is_none_or(|stop| stop.claim_id != token.claim_id || stop.phase != *phase)
        })
    }

    /// 第 1 步（共享专用）。私有服务器不调用，直接在第 4 步一并处理。
    pub(crate) fn quiesce(&mut self, actions: &mut Vec<Action>) {
        self.convergence_shutdown(actions);
        if self.shared && !self.closing && !self.quiescing {
            self.quiescing = true;
            actions.push(Action::Debug("关闭开始，停止受理新运行与目标恢复".into()));
        }
    }

    /// 仍在 settling 的认领数（第 2 步的等待条件）。
    pub fn settling_count(&self) -> usize {
        self.claims.values().filter(|c| c.settling()).count()
    }

    /// 第 3 步：发出暂停与中断，登记到 `shutdown_stops`。
    pub(crate) fn begin_shutdown_stops(&mut self, actions: &mut Vec<Action>) {
        if !self.shared || !self.quiescing || self.closing || self.shutdown_stops_started {
            return;
        }
        self.shutdown_stops_started = true;
        let mut threads: Vec<_> = self.claims.keys().cloned().collect();
        threads.sort();
        for thread in threads {
            let claim = &self.claims[&thread];
            if claim.owner().is_none() || claim.joined() || claim.settling() || claim.precheck() {
                continue;
            }
            let turn_id = claim
                .turn_id()
                .filter(|id| !id.is_empty())
                .map(str::to_owned);
            let active = self.records.get(&thread).is_some_and(|r| r.goal_active)
                || self.restore_active_pending(&thread)
                || self.activating_goal_pending(&thread);
            if !active && turn_id.is_none() {
                continue;
            }
            let stop = ShutdownStop {
                claim_id: claim.claim_id,
                phase: if active {
                    ShutdownStopPhase::PauseGoal
                } else {
                    ShutdownStopPhase::Interrupt
                },
                turn_id,
            };
            self.shutdown_stop_request(&thread, &stop, actions);
            self.shutdown_stops.insert(thread, stop);
        }
    }

    /// 第 3 步还没回包的线程数。
    pub fn shutdown_stops_pending(&self) -> usize {
        self.shutdown_stops.len()
    }

    /// 第 3 步请求的回包：暂停阶段回来后按需发中断；中断回来后移出 `shutdown_stops`。
    pub(crate) fn shutdown_stop_reply(
        &mut self,
        context: &RequestContext,
        phase: ShutdownStopPhase,
        result: Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) {
        if self.closing {
            return;
        }
        let Some(token) = &context.token else { return };
        let thread = &token.thread_id;
        let Some(stop) = self
            .shutdown_stops
            .get(thread)
            .filter(|stop| stop.claim_id == token.claim_id && stop.phase == phase)
            .cloned()
        else {
            return;
        };
        if result.is_err() {
            actions.push(Action::Warn(format!(
                "关闭期间线程 {thread} 的 {phase:?} 请求失败，继续收尾"
            )));
        }
        self.shutdown_stops.remove(thread);
        if phase == ShutdownStopPhase::PauseGoal {
            // 同代用最新轮次；已经释放或换代只保留登记时的目标，绝不碰新认领的轮次。
            let turn_id = self
                .claims
                .get(thread)
                .filter(|c| c.claim_id == stop.claim_id)
                .map(|c| {
                    if c.joined() || c.settling() || c.precheck() {
                        None
                    } else {
                        c.turn_id().filter(|id| !id.is_empty()).map(str::to_owned)
                    }
                })
                .unwrap_or(stop.turn_id);
            if turn_id.is_some() {
                let stop = ShutdownStop {
                    phase: ShutdownStopPhase::Interrupt,
                    turn_id,
                    ..stop
                };
                self.shutdown_stop_request(thread, &stop, actions);
                self.shutdown_stops.insert(thread.clone(), stop);
            }
        }
    }

    fn shutdown_stop_request(&self, thread: &str, stop: &ShutdownStop, actions: &mut Vec<Action>) {
        let (method, params) = match stop.phase {
            ShutdownStopPhase::PauseGoal => (
                "thread/goal/set",
                json!({"threadId":thread,"status":"paused"}),
            ),
            ShutdownStopPhase::Interrupt => (
                "turn/interrupt",
                json!({"threadId":thread,"turnId":stop.turn_id}),
            ),
        };
        actions.push(Action::Request(RequestContext {
            resume_retried: false,
            stream_id: None,
            review_id: None,
            client: None,
            token: Some(ClaimToken {
                thread_id: thread.into(),
                claim_id: stop.claim_id,
            }),
            operation: Operation::ShutdownStop(stop.phase),
            method: method.into(),
            params: Some(params),
        }));
    }
}
