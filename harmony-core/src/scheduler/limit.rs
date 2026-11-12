use std::time::{
    Duration,
    Instant,
};

use super::time::Clock;
use harmony_luau as luau;

/// What ends a resume under a [`ResumeLimit`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResumeBound {
    Budget,
    Deadline,
    Signal,
}

/// How long each resume of a Luau task may run before it is interrupted: for its budget, and not
/// past its deadline or the raising of its signal, whichever comes first.
#[derive(Clone, Debug)]
pub struct ResumeLimit {
    pub(super) budget: Option<Duration>,
    pub(super) deadline: Option<Instant>,
    pub(super) signal: Option<luau::InterruptSignal>,
}

impl ResumeLimit {
    /// Each resume may run this long, in real time.
    pub fn budget(budget: Duration) -> Self {
        Self {
            budget: Some(budget),
            deadline: None,
            signal: None,
        }
    }

    /// No resume runs past `deadline`, by the scheduler's clock. What is left until it when a
    /// resume starts is then counted in real time.
    pub fn until(deadline: Instant) -> Self {
        Self {
            budget: None,
            deadline: Some(deadline),
            signal: None,
        }
    }

    /// This limit, with no resume running longer than `budget` either.
    pub fn with_budget(self, budget: Duration) -> Self {
        Self {
            budget: Some(self.budget.map_or(budget, |own| own.min(budget))),
            ..self
        }
    }

    /// This limit, with no resume running past `deadline` either.
    pub fn not_past(self, deadline: Instant) -> Self {
        Self {
            deadline: Some(self.deadline.map_or(deadline, |own| own.min(deadline))),
            ..self
        }
    }

    /// This limit, with a resume also ending once `signal` is raised. A limit has one signal:
    /// this one takes the place of any it had.
    pub fn or_on(self, signal: luau::InterruptSignal) -> Self {
        Self {
            signal: Some(signal),
            ..self
        }
    }

    /// What a resume starting now runs under, and which of the budget and the deadline is the
    /// one it would run into. Fails with the bound already reached for a resume that is too late
    /// to start: Luau looks for an interrupt only at calls, loops and returns, so a resume given
    /// no time would still run up to the first of them.
    pub(super) fn guard(
        &self,
        vm: &luau::Vm,
        clock: &Clock,
    ) -> std::result::Result<(luau::InterruptBudgetGuard, ResumeBound), ResumeBound> {
        if self
            .signal
            .as_ref()
            .is_some_and(luau::InterruptSignal::is_raised)
        {
            return Err(ResumeBound::Signal);
        }
        let left = self
            .deadline
            .map(|deadline| deadline.saturating_duration_since(clock.now()));
        let (time, bound) = match (self.budget, left) {
            (Some(budget), Some(left)) if left < budget => (left, ResumeBound::Deadline),
            (Some(budget), _) => (budget, ResumeBound::Budget),
            (None, left) => (left.unwrap_or(Duration::MAX), ResumeBound::Deadline),
        };
        if time.is_zero() {
            return Err(bound);
        }
        Ok((vm.interrupt_after_or_on(time, self.signal.as_ref()), bound))
    }
}
