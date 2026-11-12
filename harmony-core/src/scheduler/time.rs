use std::{
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{
        Context,
        Poll,
        Waker,
    },
    time::{
        Duration,
        Instant,
    },
};

use super::TaskId;
use anyhow::Result;
use std::cell::{
    Cell,
    RefCell,
};

/// The time a scheduler runs by: every deadline it is given and every time it reports is read
/// against this, so a test that installs a manual clock decides when each comes due.
#[derive(Clone, Default)]
pub(crate) enum Clock {
    #[default]
    Real,
    #[cfg(any(test, feature = "testing"))]
    Manual(crate::testing::ManualClock),
}

impl Clock {
    pub(super) fn now(&self) -> Instant {
        match self {
            Self::Real => Instant::now(),
            #[cfg(any(test, feature = "testing"))]
            Self::Manual(clock) => clock.now(),
        }
    }

    /// Fails for a manual clock: a thread blocked on it would wait for time only it can move.
    pub(super) fn ensure_can_wait(&self) -> Result<()> {
        match self {
            Self::Real => Ok(()),
            #[cfg(any(test, feature = "testing"))]
            Self::Manual(_) => anyhow::bail!(
                "a scheduler on a manual clock can't block until work is due: advance the clock \
                 and call `poll_ready` instead of waiting or of driving a thread to its end"
            ),
        }
    }
}

/// Every deadline the scheduler wakes something at: its delayed tasks' and its [`Sleep`]s'.
/// Shared with [`LocalScheduler`](super::LocalScheduler), which hands out [`Sleep`]s while the scheduler is borrowed for
/// polling.
pub(super) struct Timers {
    clock: Clock,
    last_id: Cell<u64>,
    waiting: RefCell<Vec<Timer>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum TimerId {
    Sleep(u64),
    Task(TaskId),
}

struct Timer {
    id: TimerId,
    deadline: Instant,
    waker: Waker,
}

impl Timers {
    pub(super) fn new(clock: Clock) -> Self {
        Self {
            clock,
            last_id: Cell::new(0),
            waiting: RefCell::default(),
        }
    }

    pub(super) fn clock(&self) -> &Clock {
        &self.clock
    }

    pub(super) fn now(&self) -> Instant {
        self.clock.now()
    }

    /// A future that completes once the clock reaches `deadline`.
    pub(super) fn sleep(self: &Rc<Self>, deadline: Instant) -> Sleep {
        let id = self.last_id.get().wrapping_add(1);
        self.last_id.set(id);
        Sleep {
            timers: self.clone(),
            id,
            deadline,
        }
    }

    /// Wakes `waker` at `deadline`, in place of what timer `id` woke when, if anything.
    pub(super) fn set(&self, id: TimerId, deadline: Instant, waker: &Waker) {
        let mut waiting = self.waiting.borrow_mut();
        match waiting.iter_mut().find(|timer| timer.id == id) {
            Some(timer) => {
                timer.deadline = deadline;
                timer.waker.clone_from(waker);
            }
            None => waiting.push(Timer {
                id,
                deadline,
                waker: waker.clone(),
            }),
        }
    }

    pub(super) fn remove(&self, id: TimerId) {
        self.waiting.borrow_mut().retain(|timer| timer.id != id);
    }

    pub(super) fn wake_due(&self, now: Instant) {
        let due = {
            let mut waiting = self.waiting.borrow_mut();
            let (due, later) = std::mem::take(&mut *waiting)
                .into_iter()
                .partition::<Vec<_>, _>(|timer| timer.deadline <= now);
            *waiting = later;
            due
        };
        for timer in due {
            timer.waker.wake();
        }
    }

    pub(super) fn next_delay(&self, now: Instant) -> Option<Duration> {
        self.waiting
            .borrow()
            .iter()
            .map(|timer| timer.deadline.saturating_duration_since(now))
            .min()
    }
}

/// Completes at its deadline; see [`LocalScheduler::sleep_until`](super::LocalScheduler::sleep_until).
pub struct Sleep {
    timers: Rc<Timers>,
    id: u64,
    deadline: Instant,
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.timers.now() >= self.deadline {
            return Poll::Ready(());
        }
        self.timers
            .set(TimerId::Sleep(self.id), self.deadline, cx.waker());
        Poll::Pending
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        self.timers.remove(TimerId::Sleep(self.id));
    }
}
