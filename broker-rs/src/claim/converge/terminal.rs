//! 普通释放与树提交使用同一张终态表；关闭的 lost 由账本关闭流程处理。

use crate::runs::{InterruptReason as I, RunStatus as S, TurnStatus};
use crate::sa1b::{TerminalDisposition, TerminalInput, TerminalReason as R};
use crate::stop::tree::TreeTrigger as T;

pub(crate) fn terminal(input: &TerminalInput) -> Option<TerminalDisposition> {
    if input.reason == R::BrokerShutdown {
        return None;
    }
    let kept = input.existing_interrupt.filter(|r| {
        matches!(
            r,
            I::Stop
                | I::ClientDisconnected
                | I::GoalLoop
                | I::StallTimeout
                | I::UpstreamAccessError
        )
    });
    let new_reason = match kept {
        Some(I::StallTimeout) => Some(R::StallTimeout),
        Some(I::UpstreamAccessError) => Some(R::UpstreamAccessError),
        None if matches!(input.reason, R::StallTimeout | R::UpstreamAccessError) => {
            Some(input.reason)
        }
        _ => None,
    };
    let reason = match input.parent_status {
        Some(TurnStatus::Failed) => R::ParentFailed,
        _ if new_reason.is_some() => new_reason.expect("新停止原因已核对"),
        Some(TurnStatus::Interrupted) => R::ParentInterrupted,
        _ if input.reason == R::Abandoned => R::Abandoned,
        _ => match kept {
            Some(I::Stop) => R::UserStop,
            Some(I::ClientDisconnected) => R::OwnerGone,
            Some(I::GoalLoop) => R::NoAction,
            _ => input.reason,
        },
    };
    let (status, interrupt, trigger) = match reason {
        R::ParentFailed => (
            S::Failed,
            kept.or_else(|| {
                new_reason.map(|r| match r {
                    R::StallTimeout => I::StallTimeout,
                    R::UpstreamAccessError => I::UpstreamAccessError,
                    _ => unreachable!("仅包含新停止原因"),
                })
            }),
            Some(T::ParentFailed),
        ),
        R::ParentInterrupted => (S::Interrupted, kept, Some(T::ParentInterrupted)),
        R::UserStop => (S::Interrupted, Some(I::Stop), Some(T::UserStop)),
        R::OwnerGone => (
            S::Interrupted,
            Some(I::ClientDisconnected),
            Some(T::OwnerGone),
        ),
        R::BudgetExhausted => (
            S::GoalBlocked,
            Some(I::BudgetExhausted),
            Some(T::BudgetExhausted),
        ),
        R::BudgetIncomplete => (
            S::GoalBlocked,
            Some(I::BudgetIncomplete),
            Some(T::BudgetIncomplete),
        ),
        R::ConvergeDeadline => (
            S::Interrupted,
            Some(I::ConvergeDeadline),
            Some(T::ConvergeDeadline),
        ),
        R::WakeLimit => (S::GoalBlocked, Some(I::WakeLimit), Some(T::WakeLimit)),
        R::AutoTurnLimit => (
            S::GoalBlocked,
            Some(I::AutoTurnLimit),
            Some(T::AutoTurnLimit),
        ),
        R::NoAction => (S::Interrupted, Some(I::GoalLoop), Some(T::LoopGuard)),
        R::DiscoveryIncomplete => (
            S::Interrupted,
            Some(I::DiscoveryIncomplete),
            Some(T::DiscoveryIncomplete),
        ),
        R::OwnershipConflict => (
            S::Interrupted,
            Some(I::OwnershipConflict),
            Some(T::OwnershipConflict),
        ),
        R::WakeUnknown => (S::Interrupted, Some(I::WakeUnknown), Some(T::WakeUnknown)),
        R::WakeFailed => (S::Failed, Some(I::WakeFailed), Some(T::WakeFailed)),
        R::GoalPaused => (S::Interrupted, Some(I::GoalInactive), Some(T::GoalPaused)),
        R::GoalBlocked => (S::GoalBlocked, Some(I::GoalInactive), Some(T::GoalBlocked)),
        R::GoalBudgetLimited => (
            S::GoalBlocked,
            Some(I::BudgetExhausted),
            Some(T::GoalBudgetLimited),
        ),
        R::GoalUsageLimited => (
            S::GoalBlocked,
            Some(I::GoalInactive),
            Some(T::GoalUsageLimited),
        ),
        R::RootNoDirectInput => (
            S::Interrupted,
            Some(I::RootNoDirectInput),
            Some(T::RootNoDirectInput),
        ),
        R::StallTimeout => (S::Interrupted, Some(I::StallTimeout), Some(T::StallTimeout)),
        R::UpstreamAccessError => (
            S::Interrupted,
            Some(I::UpstreamAccessError),
            Some(T::UpstreamAccessError),
        ),
        R::Abandoned => (
            S::Interrupted,
            input.existing_interrupt.or(Some(I::Abandoned)),
            None,
        ),
        R::BrokerShutdown => unreachable!("关闭由 lost 流程处理"),
    };
    let exit_code = match status {
        S::Failed => 7,
        S::GoalBlocked => 9,
        S::Interrupted if interrupt == Some(I::StallTimeout) => 3,
        S::Interrupted if interrupt == Some(I::UpstreamAccessError) => 4,
        S::Interrupted if reason == R::NoAction => 9,
        S::Interrupted => 4,
        S::Completed => 0,
    };
    Some(TerminalDisposition {
        status,
        interrupt_reason: interrupt,
        terminal_reason: reason,
        tree_trigger: trigger,
        exit_code,
    })
}

pub(crate) fn trigger_reason(trigger: T) -> Option<R> {
    Some(match trigger {
        T::ParentFinishedEarly => return None,
        T::UserStop => R::UserStop,
        T::OwnerGone => R::OwnerGone,
        T::LoopGuard => R::NoAction,
        T::ParentFailed => R::ParentFailed,
        T::ParentInterrupted => R::ParentInterrupted,
        T::BudgetExhausted => R::BudgetExhausted,
        T::BudgetIncomplete => R::BudgetIncomplete,
        T::ConvergeDeadline => R::ConvergeDeadline,
        T::WakeLimit => R::WakeLimit,
        T::AutoTurnLimit => R::AutoTurnLimit,
        T::DiscoveryIncomplete => R::DiscoveryIncomplete,
        T::OwnershipConflict => R::OwnershipConflict,
        T::WakeUnknown => R::WakeUnknown,
        T::WakeFailed => R::WakeFailed,
        T::GoalPaused => R::GoalPaused,
        T::GoalBlocked => R::GoalBlocked,
        T::GoalBudgetLimited => R::GoalBudgetLimited,
        T::GoalUsageLimited => R::GoalUsageLimited,
        T::RootNoDirectInput => R::RootNoDirectInput,
        T::StallTimeout => R::StallTimeout,
        T::UpstreamAccessError => R::UpstreamAccessError,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_reasons_keep_failed_parent_and_precede_interrupted_or_goal_blocked() {
        for (interrupt, reason, trigger, exit_code) in [
            (I::StallTimeout, R::StallTimeout, T::StallTimeout, 3),
            (
                I::UpstreamAccessError,
                R::UpstreamAccessError,
                T::UpstreamAccessError,
                4,
            ),
        ] {
            for parent_status in [
                None,
                Some(TurnStatus::Completed),
                Some(TurnStatus::Interrupted),
            ] {
                for requested in [reason, R::GoalBlocked, R::NoAction, R::Abandoned] {
                    let result = terminal(&TerminalInput {
                        reason: requested,
                        parent_status,
                        existing_interrupt: Some(interrupt),
                    })
                    .unwrap();
                    assert_eq!(result.status, S::Interrupted);
                    assert_eq!(result.interrupt_reason, Some(interrupt));
                    assert_eq!(result.terminal_reason, reason);
                    assert_eq!(result.tree_trigger, Some(trigger));
                    assert_eq!(result.exit_code, exit_code);
                }
            }
            for existing_interrupt in [None, Some(interrupt)] {
                let result = terminal(&TerminalInput {
                    reason,
                    parent_status: Some(TurnStatus::Failed),
                    existing_interrupt,
                })
                .unwrap();
                assert_eq!(result.status, S::Failed);
                assert_eq!(result.interrupt_reason, Some(interrupt));
                assert_eq!(result.terminal_reason, R::ParentFailed);
                assert_eq!(result.exit_code, 7);
            }
            assert_eq!(trigger_reason(trigger), Some(reason));
        }
    }

    #[test]
    fn old_reasons_keep_parent_order_and_first_stopper() {
        for old in [I::Stop, I::ClientDisconnected, I::GoalLoop] {
            let result = terminal(&TerminalInput {
                reason: R::StallTimeout,
                parent_status: Some(TurnStatus::Interrupted),
                existing_interrupt: Some(old),
            })
            .unwrap();
            assert_eq!(result.terminal_reason, R::ParentInterrupted);
            assert_eq!(result.interrupt_reason, Some(old));
            assert_eq!(result.exit_code, 4);
        }
        let result = terminal(&TerminalInput {
            reason: R::GoalBlocked,
            parent_status: None,
            existing_interrupt: None,
        })
        .unwrap();
        assert_eq!(result.status, S::GoalBlocked);
        assert_eq!(result.exit_code, 9);
    }
}
