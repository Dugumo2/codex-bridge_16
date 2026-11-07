//! SA-a 单测：事件矩阵与绑定、代际迁移、路由全序、回查队列、撤销与墓碑、发现、聚合与持久化。
use super::*;
use crate::claim::{Claim, Event, Owner, State};
use crate::test_support::subagent::SubagentMock;
use serde_json::json;

mod discovery;
mod discovery_idle;
mod evidence;
mod lineage_safety;
mod placeholder;
mod routing;
mod tree_log_review;
mod v2_pairing;

pub(super) const P: &str = "父线程";
pub(super) const R: &str = "运行甲";
pub(super) const T: &str = "父轮1";
pub(super) const C: &str = "子线程";
pub(super) const APPROVAL: &str = "item/commandExecution/requestApproval";

pub(super) fn claim(owner: Owner, turn: &str, run: &str, claim_id: u64) -> Claim {
    Claim {
        stream_id: None,
        owned_ended: Default::default(),
        owned_turns: [turn.to_owned()].into_iter().collect(),
        claim_id,
        state: State::Running {
            owner,
            turn_id: turn.into(),
            joined: false,
        },
        run_id: Some(run.into()),
        resume_goal: false,
        request_pending: false,
        owner_gone: false,
        pending_start: None,
        submitted_input: Vec::new(),
        held_requests: Vec::new(),
        held_deltas: 0,
        held_bytes: 0,
        held_dropped_items: 0,
        overflowed: false,
        settlement_reply: None,
        pending_detach: None,
    }
}

/// 父线程 P 上运行 R 的自有认领，owner 是 cxb 连接 1；连接 2 是非 cxb 连接。
pub(super) fn setup(shared: bool) -> (Machine, SubagentMock) {
    let mut m = Machine::new(shared);
    m.live_conns.extend([1, 2, 3]);
    m.cxb_conns.extend([1, 3]);
    m.claims.insert(P.into(), claim(Owner::Client(1), T, R, 1));
    m.clock.mono_ms = 1_000;
    m.clock.now_ms = 1_700_000_000_000;
    let mut mock = SubagentMock::default();
    mock.register(P, None, "v1", false, true);
    (m, mock)
}

/// 推进时间并触发到期的整体扫描、墓碑计时。
pub(super) fn advance(m: &mut Machine, mock: &mut SubagentMock, ms: u64) -> Vec<Action> {
    m.clock.mono_ms += ms;
    m.clock.now_ms += ms;
    let mut out = Vec::new();
    for _ in 0..8 {
        let mut fired = false;
        if let Some((at, generation)) = m.relations.aux.sweep {
            if at <= m.clock.mono_ms {
                out.extend(mock.inject(
                    m,
                    Event::Timer {
                        token: Machine::relation_token(),
                        kind: crate::claim::TimerKind::LookupDeadline {
                            op_id: SWEEP_OP,
                            generation,
                        },
                    },
                ));
                fired = true;
            }
        }
        if let Some((at, generation)) = m.relations.aux.tombstone_timer {
            if at <= m.clock.mono_ms {
                out.extend(mock.inject(
                    m,
                    Event::Timer {
                        token: Machine::relation_token(),
                        kind: crate::claim::TimerKind::TombstoneExpire {
                            conn_epoch: m.relations.conn_epoch,
                            generation,
                        },
                    },
                ));
                fired = true;
            }
        }
        if !fired {
            break;
        }
    }
    out
}

/// 依次注入一组事件，汇总动作。
pub(super) fn run(m: &mut Machine, mock: &mut SubagentMock, events: Vec<Event>) -> Vec<Action> {
    let mut out = Vec::new();
    for event in events {
        out.extend(mock.inject(m, event));
    }
    out
}

/// v1 派生成功且子线程首轮开始：返回全部动作。
pub(super) fn spawn_v1(
    m: &mut Machine,
    mock: &mut SubagentMock,
    child: &str,
    item: &str,
    child_turn: &str,
) -> Vec<Action> {
    mock.register(child, Some(P), "v1", false, true);
    let (started, completed) = SubagentMock::v1_spawn(P, T, item, child, false);
    let first = mock.turn_started(child, child_turn);
    run(m, mock, vec![started, completed, first])
}

/// 发给某连接的消息。
pub(super) fn sent(actions: &[Action], conn: crate::claim::ConnId) -> Vec<Value> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Send { conn: c, message } if *c == conn => Some(message.clone()),
            _ => None,
        })
        .collect()
}

/// 发给某连接的转发请求（带 id 与 method 的消息）。
pub(super) fn forwarded(actions: &[Action], conn: crate::claim::ConnId) -> Vec<Value> {
    sent(actions, conn)
        .into_iter()
        .filter(|m| m.get("id").is_some() && m.get("method").is_some())
        .collect()
}

pub(super) fn notifications(
    actions: &[Action],
    conn: crate::claim::ConnId,
    method: &str,
) -> Vec<Value> {
    sent(actions, conn)
        .into_iter()
        .filter(|m| m["method"] == method && m.get("id").is_none())
        .map(|m| m["params"].clone())
        .collect()
}

/// 对上游的回答（含 Dismiss）。
pub(super) fn upstream(actions: &[Action]) -> Vec<&Action> {
    actions
        .iter()
        .filter(|a| {
            matches!(
                a,
                Action::Dismiss { .. }
                    | Action::ServerResult { .. }
                    | Action::ServerError { .. }
                    | Action::AppReplyAcked { .. }
            )
        })
        .collect()
}

pub(super) fn app_requests<'a>(
    actions: &'a [Action],
    method: &str,
) -> Vec<&'a crate::claim::RequestContext> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Request(c) if c.method == method => Some(c),
            _ => None,
        })
        .collect()
}

pub(super) fn bound(m: &Machine, thread: &str, turn: &str) -> Option<String> {
    m.binding(thread, turn)
        .filter(|b| b.status == BindingStatus::Bound)
        .map(|b| b.run_id)
}
