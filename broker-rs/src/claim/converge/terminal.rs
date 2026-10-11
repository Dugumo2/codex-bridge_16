//! 普通释放与树提交使用同一张终态表；关闭的 lost 由账本关闭流程处理。

use crate::runs::{InterruptReason as I, RunStatus as S, TurnStatus};
use crate::sa1b::{TerminalDisposition, TerminalInput, TerminalReason as R};
use crate::stop::tree::TreeTrigger as T;

pub(crate) fn terminal(input: &TerminalInput) -> Option<TerminalDisposition> {
    if input.reason == R::BrokerShutdown {
        return None;
    }
    let kept = input
        .existing_interrupt
        .filter(|r| matches!(r, I::Stop | I::ClientDisconnected | I::GoalLoop));
    let reason = match input.parent_status {
        Some(TurnStatus::Failed) => R::ParentFailed,
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
        R::ParentFailed => (S::Failed, kept, Some(T::ParentFailed)),
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
    })
}
