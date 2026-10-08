//! H2-a 实现读取层；本批只冻结契约，不发请求、不登记计时。
use super::{Action, ClaimToken, Machine};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatusQuery {
    Busy,
    Activity,
    Turn {
        target: String,
    },
    Settlement {
        candidates: Vec<String>,
        input: Vec<String>,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusUnknown {
    Unsupported,
    Malformed,
    ErrorReply,
    PageBudget,
    DuplicateCursor,
    Changed,
    NotVisible,
    Timeout,
    Unresponsive,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusTurn {
    InProgress,
    Completed,
    Failed,
    Interrupted,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatusResult {
    NotBusy,
    Busy,
    Idle,
    Active(Vec<String>),
    Found { status: StatusTurn, shell: Value },
    Absent,
    FirstActive(String),
    Carrier(String),
    NoCarrier,
    Unknown(StatusUnknown),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatusCaller {
    ContinueCheck {
        round: u64,
    },
    StopVerify {
        tag: crate::stop::StopTag,
    },
    Reconcile {
        generation: u64,
        round: u64,
    },
    LateSettlement {
        generation: u64,
        round: u64,
    },
    Settlement {
        attempt: u8,
    },
    JoinedCheck {
        attempt: u8,
    },
    DerivedStopVerify {
        tree_op: u64,
        thread: String,
        round: u64,
    },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusDone {
    pub read_id: u64,
    pub thread: String,
    pub token: Option<ClaimToken>,
    pub caller: StatusCaller,
    pub result: StatusResult,
}
impl Machine {
    /// 0 为未登记的占位编号；H2-a 换成唯一非零编号及一次完成语义。
    pub fn begin_status_read(
        &mut self,
        _thread: &str,
        _token: Option<ClaimToken>,
        _query: StatusQuery,
        _caller: StatusCaller,
        actions: &mut Vec<Action>,
    ) -> u64 {
        actions.push(Action::Warn("H2 状态读取尚未启用".into()));
        0
    }
    pub fn status_read_done(&mut self, _done: StatusDone, actions: &mut Vec<Action>) {
        actions.push(Action::Warn("H2 状态读取完成分派尚未启用".into()));
    }
}
