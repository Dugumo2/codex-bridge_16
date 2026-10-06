//! 服务端请求的纯逻辑路由与撤销（详细设计 1.2、2.1、3.3、3.4、4.1、4.2、8 节）。
//!
//! 与认领共用一次 step，归属变化和请求退役因此不可被其他输入打断。
//! 编号的时间与随机部分由 core 提供；此处只登记状态、核对代号并生成动作，不依赖 tokio。

use crate::claim::{Action, ConnId, HeldRequest, Machine, Owner};
use crate::proto::{self, code};
use serde_json::Value;
use std::collections::HashMap;

#[cfg(test)]
mod tests;

pub const FORWARD_TIMEOUT_MS: u64 = 3_600_000;
const DISCONNECTED: &str = "Client disconnected while awaiting approval response";
const SHUTDOWN: &str = "Broker shutting down";
pub(crate) const TIMEOUT: &str = "Request forwarding timed out";
const DYNAMIC_TOOL: &str = "item/tool/call";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingForward {
    pub request_id: Value,
    pub method: String,
    pub params: Option<Value>,
    /// 原转发的墙钟截止时间，交接到 inbox 时沿用。
    pub deadline_ms: u64,
    pub thread_id: String,
    pub claim_id: u64,
    pub target: ConnId,
    /// 与转发编号共同核对，避免旧计时器结算后来复用编号的请求。
    pub generation: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Requests {
    pub pending: HashMap<String, PendingForward>,
    next_generation: u64,
}

impl Machine {
    /// 4.2 第 1–5 条：统一处理审批与 C2，包括未来新增的方法；动态工具按 4.1「Client 认领时不变」
    /// 沿用上游的专门规则（见 [`Self::dynamic_tool`]）。`from_held` 表示 settling 定案后回放的挂起请求。
    pub(crate) fn server_request(
        &mut self,
        request: HeldRequest,
        from_held: bool,
        actions: &mut Vec<Action>,
    ) {
        let thread_id = request
            .params
            .as_ref()
            .and_then(|p| p.get("threadId"))
            .and_then(Value::as_str);
        let Some(thread_id) = thread_id else {
            if self.shared {
                actions.push(Action::Dismiss { id: request.id });
            } else {
                method_not_found(request, actions);
            }
            return;
        };
        let Some(claim) = self.claims.get_mut(thread_id) else {
            actions.push(Action::Dismiss { id: request.id });
            return;
        };
        // #2c：续跑预检期间按 4.2 节第 1 条不答（NO_RESPONSE）。
        if claim.joined() || claim.precheck() {
            actions.push(Action::Dismiss { id: request.id });
            return;
        }
        if claim.settling() {
            let thread_id = thread_id.to_owned();
            self.hold_request(&thread_id, request, actions);
            return;
        }
        if self.closing {
            reject(request.id, SHUTDOWN, actions);
            return;
        }
        if request.method == DYNAMIC_TOOL {
            self.dynamic_tool(request, from_held, actions);
            return;
        }
        let target = match claim.owner() {
            Some(Owner::Client(conn)) if !claim.owner_gone && self.live_conns.contains(conn) => {
                Some(*conn)
            }
            _ => None,
        };
        if let Some(target) = target {
            let claim_id = claim.claim_id;
            let thread_id = thread_id.to_owned();
            self.forward(request, thread_id, claim_id, target, actions);
        } else if matches!(claim.owner(), Some(Owner::Broker { .. })) {
            // 4.2 节第 4 条：托管线程按 policy 自动应答，答不了的进 inbox（丙路）。
            self.hosted_server_request(request, actions);
        } else {
            reject_no_client(request, actions);
        }
    }

    /// 4.1 动态工具：沿用上游 broker-server.ts 677–726 行（删去 peer 分支）。托管线程回 -32601；
    /// 共享服务器上已定案的认领不答（只有 settling 期间挂起、定案后回放的才转发）；
    /// owner 在线就转发，不在线也不答（上游返回 NO_RESPONSE，不是 -32601）。
    fn dynamic_tool(&mut self, request: HeldRequest, from_held: bool, actions: &mut Vec<Action>) {
        let Some(thread_id) = request
            .params
            .as_ref()
            .and_then(|p| p.get("threadId"))
            .and_then(Value::as_str)
            .map(str::to_owned)
        else {
            actions.push(Action::Dismiss { id: request.id });
            return;
        };
        let Some(claim) = self.claims.get(&thread_id) else {
            actions.push(Action::Dismiss { id: request.id });
            return;
        };
        let claim_id = claim.claim_id;
        let target = match claim.owner() {
            Some(Owner::Broker { .. }) => {
                method_not_found(request, actions);
                return;
            }
            Some(Owner::Client(conn)) if !claim.owner_gone && self.live_conns.contains(conn) => {
                Some(*conn)
            }
            _ => None,
        };
        match target {
            Some(target) if from_held || !self.shared => {
                self.forward(request, thread_id, claim_id, target, actions)
            }
            _ => actions.push(Action::Dismiss { id: request.id }),
        }
    }

    /// 登记并发出一个转发：转发 id 由 core 预先生成，计时器带独立代号。
    fn forward(
        &mut self,
        request: HeldRequest,
        thread_id: String,
        claim_id: u64,
        target: ConnId,
        actions: &mut Vec<Action>,
    ) {
        self.requests.next_generation = self
            .requests
            .next_generation
            .checked_add(1)
            .expect("转发代号已耗尽");
        let generation = self.requests.next_generation;
        let forward_id = request.forward_id;
        self.requests.pending.insert(
            forward_id.clone(),
            PendingForward {
                request_id: request.id,
                method: request.method.clone(),
                params: request.params.clone(),
                deadline_ms: self.clock.now_ms.saturating_add(FORWARD_TIMEOUT_MS),
                thread_id,
                claim_id,
                target,
                generation,
            },
        );
        actions.push(Action::Send {
            conn: target,
            message: proto::request(
                Value::String(forward_id.clone()),
                &request.method,
                request.params,
            ),
        });
        actions.push(Action::ScheduleForward {
            forward_id,
            generation,
            delay_ms: FORWARD_TIMEOUT_MS,
        });
    }

    /// 1.2：只接受原连接；字段存在性决定优先级，显式 null 也是 result。
    pub(crate) fn forward_response(
        &mut self,
        conn: ConnId,
        id: Value,
        result: Option<Value>,
        error: Option<Value>,
        actions: &mut Vec<Action>,
    ) {
        let forward_id = proto::request_id_key(&id);
        let Some(pending) = self.requests.pending.get(&forward_id) else {
            actions.push(Action::Warn(format!(
                "收到未知或已结束的转发请求回包 {forward_id}，忽略"
            )));
            return;
        };
        if pending.target != conn {
            actions.push(Action::Warn(format!(
                "转发请求 {forward_id} 的回包来自非原目标连接 {conn}，忽略"
            )));
            return;
        }
        let pending = self
            .take_forward(&forward_id, actions)
            .expect("已核对挂起的转发");
        if let Some(result) = result {
            actions.push(Action::ServerResult {
                id: pending.request_id,
                result,
            });
        } else if let Some(error) = error {
            actions.push(Action::ServerError {
                id: pending.request_id,
                error: client_error(&error),
            });
        } else {
            reject(
                pending.request_id,
                "Malformed forwarded response: missing both 'result' and 'error'",
                actions,
            );
        }
    }

    /// 4.2 第 6 条：只结算仍挂起且代号相同的转发，迟到计时器没有副作用。
    pub(crate) fn forward_timeout(
        &mut self,
        forward_id: &str,
        generation: u64,
        actions: &mut Vec<Action>,
    ) {
        if !self
            .requests
            .pending
            .get(forward_id)
            .is_some_and(|p| p.generation == generation)
        {
            return;
        }
        let pending = self
            .take_forward(forward_id, actions)
            .expect("已核对转发计时器");
        reject(pending.request_id, TIMEOUT, actions);
    }

    /// 4.2 第 6 条：先撤销本连接的转发，再改变或释放认领，保留 joined 判定依据。
    pub(crate) fn disconnect_forwards(&mut self, conn: ConnId, actions: &mut Vec<Action>) {
        let ids: Vec<_> = self
            .requests
            .pending
            .iter()
            .filter(|(_, p)| p.target == conn)
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            let pending = self.take_forward(&id, actions).expect("本连接的转发");
            // 只有发出这个转发的那次认领仍在、且已是 joined，才静默；线程上换了认领（别的连接，
            // 或同一连接换代）时，不能拿新认领的 joined 来处置旧认领留下的请求。
            let joined_now = self
                .claims
                .get(&pending.thread_id)
                .is_some_and(|c| c.claim_id == pending.claim_id && c.joined());
            if joined_now {
                actions.push(Action::Dismiss {
                    id: pending.request_id,
                });
            } else {
                reject(pending.request_id, DISCONNECTED, actions);
            }
        }
    }

    /// joined 定案（3.4 节，上游 retireForwards 504–512 行）：这些请求本来就不归本连接回答，静默交还，
    /// 免得替真正的 owner 拒绝。认领释放时不调用它：上游 releaseThread 保留挂起的转发，owner
    /// 仍可回答，直到回答、超时、断开、resolved 或关闭，app-server 不会因此等不到结果。
    /// 只撤销 `claim_id` 这次认领自己产生的转发：同一连接上旧认领释放后留下的请求仍归它回答。
    pub(crate) fn retire_thread_forwards(
        &mut self,
        thread_id: &str,
        claim_id: u64,
        actions: &mut Vec<Action>,
    ) {
        let ids: Vec<_> = self
            .requests
            .pending
            .iter()
            .filter(|(_, p)| p.thread_id == thread_id && p.claim_id == claim_id)
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            let pending = self.take_forward(&id, actions).expect("待退役的转发");
            actions.push(Action::Dismiss {
                id: pending.request_id,
            });
        }
    }

    /// #26：别人先答时同时退役 held 与转发；后到的本地回包只会告警。
    pub(crate) fn resolve_held_request(
        &mut self,
        thread_id: &str,
        params: &Option<Value>,
        actions: &mut Vec<Action>,
    ) {
        let Some(id) = params.as_ref().and_then(|p| p.get("requestId")) else {
            return;
        };
        self.remove_held_request(thread_id, id);
        let ids: Vec<_> = self
            .requests
            .pending
            .iter()
            .filter(|(_, p)| {
                p.thread_id == thread_id
                    && proto::request_id_key(&p.request_id) == proto::request_id_key(id)
            })
            .map(|(id, _)| id.clone())
            .collect();
        for forward_id in ids {
            self.take_forward(&forward_id, actions);
        }
        actions.push(Action::Dismiss { id: id.clone() });
        // 2.3 节 B2：inbox 里对应的事项按上游已关闭处理（甲路）。
        let id = id.clone();
        self.inbox_server_resolved(thread_id, &id, actions);
    }

    /// 第 5 节：先拒绝转发再走静默释放出口，关闭错误不会被释放吞掉。
    pub(crate) fn shutdown_forwards(&mut self, actions: &mut Vec<Action>) {
        let ids: Vec<_> = self.requests.pending.keys().cloned().collect();
        for id in ids {
            let pending = self.take_forward(&id, actions).expect("关闭中的转发");
            reject(pending.request_id, SHUTDOWN, actions);
        }
    }

    pub(crate) fn take_forward(
        &mut self,
        forward_id: &str,
        actions: &mut Vec<Action>,
    ) -> Option<PendingForward> {
        let pending = self.requests.pending.remove(forward_id)?;
        actions.push(Action::CancelForwardTimer {
            forward_id: forward_id.into(),
            generation: pending.generation,
        });
        Some(pending)
    }
}

/// 无客户端可转时统一拒绝分类；inbox 容量满也复用此出口。
/// 上游 src/broker-server.ts:1234、1245：仅命令与文件审批使用该拒绝原文。
pub(crate) fn reject_no_client(request: HeldRequest, actions: &mut Vec<Action>) {
    if matches!(
        request.method.as_str(),
        "item/commandExecution/requestApproval" | "item/fileChange/requestApproval"
    ) {
        reject(
            request.id,
            "No active client to forward approval request",
            actions,
        );
    } else {
        method_not_found(request, actions);
    }
}

pub(crate) fn method_not_found(request: HeldRequest, actions: &mut Vec<Action>) {
    actions.push(Action::ServerError {
        id: request.id,
        error: proto::error_object(
            code::METHOD_NOT_FOUND,
            format!("Method not found: {}", request.method),
            None,
        ),
    });
}

fn reject(id: Value, message: &str, actions: &mut Vec<Action>) {
    actions.push(Action::ServerError {
        id,
        error: handler_error(message),
    });
}

/// broker 自己拒绝服务端请求时回给 app-server 的错误。与上游线上文本一致：上游这些拒绝都是
/// 处理函数抛错，经 rpc.ts 296–297 行包装成 -32603，文本带「Handler error: 」前缀。
pub(crate) fn handler_error(message: &str) -> Value {
    proto::error_object(
        code::INTERNAL_ERROR,
        format!("Handler error: {message}"),
        None,
    )
}

/// owner 回的错误转给 app-server 前按上游规范化（broker-server.ts 1378–1386 行、rpc.ts 296–299 行）：
/// - code 是数字就用（`-32603.0` 这类整数值也算）；不是数字时用 -32000。
///   有意偏离：非整数的 code 也按 -32000 处理（JSON-RPC 规定错误码为整数，上游会原样转发）。
/// - message 缺失或为 null 时用「Client error」；其他类型按 JavaScript `String()` 转成字符串
///   （上游 `new Error(message)`），例如 5 变成 "5"。
/// - data 原样保留（含 null）；code 为 -32603 时与上游一样加「Handler error: 」前缀。
pub(crate) fn client_error(error: &Value) -> Value {
    let code = error
        .get("code")
        .and_then(|c| {
            c.as_i64().or_else(|| {
                // i64::MAX as f64 会舍入成 2^63，上界必须用严格小于，越界值按非数字处理。
                const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;
                c.as_f64()
                    .filter(|n| n.fract() == 0.0 && *n >= -TWO_POW_63 && *n < TWO_POW_63)
                    .map(|n| n as i64)
            })
        })
        .unwrap_or(code::GENERAL_ERROR);
    let message = match error.get("message") {
        None | Some(Value::Null) => "Client error".to_owned(),
        Some(other) => proto::javascript_string(other),
    };
    let message = if code == code::INTERNAL_ERROR {
        format!("Handler error: {message}")
    } else {
        message.to_owned()
    };
    proto::error_object(code, message, error.get("data").cloned())
}
