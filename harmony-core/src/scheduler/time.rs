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

/// Every deadline the scheduler wakes something at: its delayed tasks' and its [`Sleep`]s'. The
/// [`Sleep`]s share it.
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

/// Completes at its deadline; see [`Scheduler::sleep_until`](super::Scheduler::sleep_until).
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

#[cfg(test)]
mod tests {

    use crate::testing::ManualClock;

    use super::*;
    use crate::scheduler::testing::*;
    use crate::scheduler::*;

    impl Scheduler {
        fn spawn_after(
            &self,
            context: CallContext,
            delay: Duration,
            future: impl Future<Output = Result<()>> + 'static,
        ) -> TaskHandle {
            let wake_at = self.delay_until(Some(delay));
            self.spawn_boxed(context, Box::pin(future), wake_at, false)
        }
    }

    #[test]
    fn delayed_task_waits_until_wake_time() {
        let clock = ManualClock::new();
        let scheduler = Scheduler::with_manual_clock(clock.clone());
        let handle = scheduler.spawn_after(CallContext::default(), Duration::from_secs(5), async {
            Ok(())
        });
        let state = |scheduler: &Scheduler| {
            scheduler
                .snapshot(handle.id())
                .expect("task snapshot")
                .state
        };

        assert_eq!(scheduler.poll_ready(), 0);
        assert_eq!(scheduler.next_wake_delay(), Some(Duration::from_secs(5)));

        clock.advance(Duration::from_secs(5) - Duration::from_nanos(1));
        assert_eq!(scheduler.poll_ready(), 0);
        assert_eq!(state(&scheduler), TaskState::Pending);
        assert_eq!(scheduler.next_wake_delay(), Some(Duration::from_nanos(1)));

        clock.advance(Duration::from_nanos(1));
        assert_eq!(scheduler.poll_ready(), 1);
        assert_eq!(state(&scheduler), TaskState::Completed);
    }

    #[test]
    fn a_cancelled_delayed_task_leaves_nothing_to_wait_for() {
        let scheduler = Scheduler::with_manual_clock(ManualClock::new());
        let handle = scheduler.spawn_after(CallContext::default(), Duration::from_secs(5), async {
            Ok(())
        });
        assert_eq!(scheduler.next_wake_delay(), Some(Duration::from_secs(5)));

        assert!(scheduler.cancel(handle.id()));
        assert_eq!(scheduler.next_wake_delay(), None);
    }

    /// On the real clock and the real wait: nothing but the sleep's deadline ends the wait.
    #[test]
    fn a_sleep_ends_the_wait_and_wakes_its_task_at_the_deadline() {
        ending_without_a_missed_wake(|| {
            let scheduler = Scheduler::new();
            let woke = Rc::new(Cell::new(None));
            let deadline = Instant::now() + Duration::from_millis(50);
            let sleep = scheduler.sleep_until(deadline);
            scheduler.spawn_future(CallContext::default(), {
                let woke = woke.clone();
                async move {
                    sleep.await;
                    woke.set(Some(Instant::now()));
                    Ok(())
                }
            });

            run_until(&scheduler, || woke.get().is_some());
            let woke = woke.get().expect("the task woke");
            assert!(woke >= deadline, "woken {:?} early", deadline - woke);
        });
    }

    /// A task waiting on a sleep, and whether it has woken.
    fn sleeper(scheduler: &Scheduler, deadline: Instant) -> Rc<Cell<bool>> {
        let woke = Rc::new(Cell::new(false));
        let sleep = scheduler.sleep_until(deadline);
        scheduler.spawn_future(CallContext::default(), {
            let woke = woke.clone();
            async move {
                sleep.await;
                woke.set(true);
                Ok(())
            }
        });
        woke
    }

    /// A deadline is the first instant that is too late: a sleep is over at it, not after it.
    #[test]
    fn a_sleep_is_over_at_its_deadline_and_not_before() {
        let clock = ManualClock::new();
        let scheduler = Scheduler::with_manual_clock(clock.clone());
        let woke = sleeper(&scheduler, clock.now() + Duration::from_secs(5));

        scheduler.poll_ready();
        assert!(!woke.get());
        assert_eq!(scheduler.next_wake_delay(), Some(Duration::from_secs(5)));

        clock.advance(Duration::from_secs(5) - Duration::from_nanos(1));
        scheduler.poll_ready();
        scheduler.poll_ready();
        assert!(!woke.get(), "woken before the deadline");
        assert_eq!(scheduler.next_wake_delay(), Some(Duration::from_nanos(1)));

        clock.advance(Duration::from_nanos(1));
        assert!(!woke.get(), "advancing the clock ran the task");
        scheduler.poll_ready();
        assert!(woke.get(), "still asleep at the deadline");
        assert_eq!(scheduler.next_wake_delay(), None);

        let late = sleeper(&scheduler, clock.now() - Duration::from_secs(1));
        scheduler.poll_ready();
        assert!(late.get(), "a sleep past its deadline waits");
    }

    #[test]
    fn a_dropped_sleep_leaves_nothing_to_wait_for() {
        let clock = ManualClock::new();
        let scheduler = Scheduler::with_manual_clock(clock.clone());
        let mut sleep = scheduler.sleep_until(clock.now() + Duration::from_secs(3600));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(Pin::new(&mut sleep).poll(&mut cx).is_pending());
        assert_eq!(scheduler.next_wake_delay(), Some(Duration::from_secs(3600)));

        drop(sleep);
        assert_eq!(scheduler.next_wake_delay(), None);
    }

    #[test]
    #[should_panic(expected = "manual clock can't block")]
    fn a_scheduler_on_a_manual_clock_refuses_to_wait_for_work() {
        let clock = ManualClock::new();
        let scheduler = Scheduler::with_manual_clock(clock.clone());
        // Even with work due at once, which a real wait would return for.
        scheduler.spawn_future(CallContext::default(), async { Ok(()) });
        scheduler.wait_for_work(Some(clock.now()));
    }

    /// What came due by the round's start runs in the order its timers were set, whichever is due
    /// first.
    #[test]
    fn due_tasks_and_sleeps_run_in_the_order_their_timers_were_set() {
        let clock = ManualClock::new();
        let scheduler = Scheduler::with_manual_clock(clock.clone());
        let order = Rc::new(RefCell::new(Vec::new()));
        let note = |name: &'static str| {
            let order = order.clone();
            async move {
                order.borrow_mut().push(name);
                Ok(())
            }
        };
        let sleep = scheduler.sleep_until(clock.now() + Duration::from_secs(3));
        scheduler.spawn_future(CallContext::default(), {
            let woke = note("sleep");
            async move {
                sleep.await;
                woke.await
            }
        });
        scheduler.poll_ready();
        scheduler.spawn_after(CallContext::default(), Duration::from_secs(5), note("late"));
        scheduler.spawn_after(
            CallContext::default(),
            Duration::from_secs(1),
            note("early"),
        );

        clock.advance(Duration::from_secs(10));
        scheduler.poll_ready();

        assert_eq!(*order.borrow(), ["sleep", "late", "early"]);
    }
}
