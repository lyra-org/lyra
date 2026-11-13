use std::{
    future::Future,
    pin::Pin,
    rc::{
        Rc,
        Weak,
    },
    task::Poll,
    time::Instant,
};

use super::LocalLuauTaskCompletion;
use super::Scheduler;
use super::limit::ResumeBound;
use super::time::Timers;
use harmony_luau as luau;
use std::cell::Cell;

/// How [`WatchedThread::watch`] takes a thread that ended at its deadline or later, when the
/// watcher reads its result before it sees the deadline pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LateEnd {
    /// As [`Watched::Expired`]: the deadline is the first instant that is too late, for a caller
    /// promised an answer by then.
    Expired,
    /// As [`Watched::Ended`]: the deadline only keeps the thread from running for good.
    Ended,
}

/// What [`WatchedThread::watch`] came to.
#[derive(Debug)]
pub enum Watched<S> {
    /// The thread ended, with what it returned or raised.
    Ended {
        result: std::result::Result<Vec<luau::Value>, luau::RaisedError>,
        /// The bound of its [`ResumeLimit`](super::ResumeLimit) that cut it short, for a thread that failed as
        /// interrupted; see [`LocalLuauTaskCompletion::interrupted_by`].
        interrupted_by: Option<ResumeBound>,
    },
    /// The watcher was told to stop, with what told it.
    Stopped(S),
    Expired,
}

/// A Luau thread its watcher answers for, as [`Scheduler::spawn_watched_luau_thread`]
/// schedules it: however the watcher goes, the thread doesn't outlive it.
pub struct WatchedThread {
    scheduler: Weak<Scheduler>,
    timers: Rc<Timers>,
    thread: luau::Thread,
    completion: Rc<LocalLuauTaskCompletion>,
    /// Until [`Self::watch`] saw the thread end or cancelled it.
    live: Cell<bool>,
}

impl WatchedThread {
    pub(super) fn new(
        scheduler: Weak<Scheduler>,
        timers: Rc<Timers>,
        thread: luau::Thread,
        completion: Rc<LocalLuauTaskCompletion>,
    ) -> Self {
        Self {
            scheduler,
            timers,
            thread,
            completion,
            live: Cell::new(true),
        }
    }

    pub fn thread(&self) -> &luau::Thread {
        &self.thread
    }

    /// Waits for the thread to end, for `stop` or for `deadline`, by its scheduler's clock; of
    /// those ready at once, `stop` wins, then the thread's end. A thread still running when it is
    /// stopped or expires is cancelled, and this completes once its work is dropped. `late`
    /// decides how a thread that ended at `deadline` or later counts.
    pub async fn watch<S>(
        self,
        stop: impl Future<Output = S>,
        deadline: Instant,
        late: LateEnd,
    ) -> Watched<S> {
        let mut expired = self.timers.sleep(deadline);
        let mut stop = std::pin::pin!(stop);
        let outcome = std::future::poll_fn(|cx| {
            if let Poll::Ready(reason) = stop.as_mut().poll(cx) {
                return Poll::Ready(Watched::Stopped(reason));
            }
            if let Poll::Ready(result) = self.completion.poll(cx) {
                return Poll::Ready(Watched::Ended {
                    result,
                    interrupted_by: self.completion.interrupted_by(),
                });
            }
            Pin::new(&mut expired).poll(cx).map(|()| Watched::Expired)
        })
        .await;
        if !matches!(outcome, Watched::Ended { .. }) {
            self.cancel().await;
            return outcome;
        }
        self.live.set(false);
        let ended_late = self
            .completion
            .ended_at()
            .is_some_and(|ended| ended >= deadline);
        match late {
            LateEnd::Expired if ended_late => Watched::Expired,
            LateEnd::Expired | LateEnd::Ended => outcome,
        }
    }

    /// [`Self::watch`] for a watcher nothing tells to stop.
    pub async fn watch_until(
        self,
        deadline: Instant,
        late: LateEnd,
    ) -> Watched<std::convert::Infallible> {
        self.watch(std::future::pending(), deadline, late).await
    }

    async fn cancel(&self) {
        self.live.set(false);
        let settled = self
            .scheduler
            .upgrade()
            .map(|scheduler| scheduler.cancel_luau_thread_and_settle(&self.thread));
        if let Some(settled) = settled {
            settled.await;
        }
    }
}

impl Drop for WatchedThread {
    fn drop(&mut self) {
        // A watcher dropped while it watches, or that never did.
        if self.live.get()
            && let Some(scheduler) = self.scheduler.upgrade()
        {
            scheduler.cancel_luau_thread(&self.thread);
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::testing::ManualClock;

    use super::*;
    use crate::scheduler::testing::*;
    use crate::scheduler::*;

    /// Schedules a thread that returns at once, and a task that moves the clock on by a second,
    /// in the order given, and runs them in one round. Returns when the thread ended, as its
    /// completion has it, counted from the round's start.
    fn ended_after(advance_first: bool) -> luau::runtime::Result<Duration> {
        let vm = luau::Vm::new()?;
        let clock = ManualClock::new();
        let scheduler = Scheduler::with_manual_clock(clock.clone());
        let completion = Rc::new(LocalLuauTaskCompletion::default());
        let advance = |scheduler: &Scheduler| {
            let clock = clock.clone();
            scheduler.spawn_future(CallContext::default(), async move {
                clock.advance(Duration::from_secs(1));
                Ok(())
            });
        };
        if advance_first {
            advance(&scheduler);
        }
        scheduler.spawn_luau_thread_with_completion(
            CallContext::default(),
            chunk_thread(&vm, "return 42")?,
            Vec::new(),
            completion.clone(),
        );
        if !advance_first {
            advance(&scheduler);
        }

        let start = clock.now();
        assert!(completion.ended_at().is_none());
        scheduler.poll_ready();
        assert_eq!(clock.now(), start + Duration::from_secs(1));
        Ok(completion.ended_at().expect("the thread ended") - start)
    }

    /// Watches a thread that ends at its deadline, before the watcher sees the deadline pass.
    fn watched_ending_at_its_deadline(
        late: LateEnd,
    ) -> luau::runtime::Result<Watched<std::convert::Infallible>> {
        let vm = luau::Vm::new()?;
        let clock = ManualClock::new();
        let scheduler = Rc::new(Scheduler::with_manual_clock(clock.clone()));
        let deadline = clock.now() + Duration::from_secs(1);
        scheduler.spawn_future(CallContext::default(), {
            let clock = clock.clone();
            async move {
                clock.advance(Duration::from_secs(1));
                Ok(())
            }
        });
        let watched = scheduler.spawn_watched_luau_thread(
            CallContext::default(),
            chunk_thread(&vm, "return 42")?,
            Vec::new(),
            None,
        );
        let completion = watched.completion.clone();
        let outcome = Rc::new(RefCell::new(None));
        scheduler.spawn_watcher(CallContext::default(), {
            let outcome = outcome.clone();
            async move {
                let watched = watched.watch(std::future::pending(), deadline, late).await;
                *outcome.borrow_mut() = Some(watched);
                Ok(())
            }
        });

        scheduler.poll_ready();
        assert_eq!(completion.ended_at(), Some(deadline));
        assert!(outcome.borrow().is_none(), "the watch ended in the round");
        scheduler.poll_ready();
        Ok(outcome.take().expect("the watch ended"))
    }

    #[test]
    fn a_watch_takes_an_end_at_the_deadline_as_its_rule_says() -> luau::runtime::Result<()> {
        assert!(matches!(
            watched_ending_at_its_deadline(LateEnd::Expired)?,
            Watched::Expired
        ));
        assert!(matches!(
            watched_ending_at_its_deadline(LateEnd::Ended)?,
            Watched::Ended { result: Ok(values), .. }
                if matches!(values[..], [luau::Value::Number(n)] if n == 42.0)
        ));
        Ok(())
    }

    #[test]
    fn a_watch_cancels_a_thread_still_running_once_stopped() -> luau::runtime::Result<()> {
        let vm = luau::Vm::new()?;
        vm.open_standard_libraries(luau::StandardLibraries::all_supported())?;
        let clock = ManualClock::new();
        let scheduler = Rc::new(Scheduler::with_manual_clock(clock.clone()));
        let watched = scheduler.spawn_watched_luau_thread(
            CallContext::default(),
            chunk_thread(&vm, "coroutine.yield()")?,
            Vec::new(),
            None,
        );
        let completion = watched.completion.clone();
        let stop = Cancellation::new(scheduler.wake_handle());
        let deadline = clock.now() + Duration::from_secs(3600);
        let outcome = Rc::new(RefCell::new(None));
        scheduler.spawn_watcher(CallContext::default(), {
            let (outcome, stop) = (outcome.clone(), stop.clone());
            async move {
                let watched = watched
                    .watch(stop.cancelled(), deadline, LateEnd::Expired)
                    .await;
                *outcome.borrow_mut() = Some(watched);
                Ok(())
            }
        });
        scheduler.poll_ready();
        assert!(completion.ended_at().is_none(), "a yield ended the thread");

        stop.cancel();
        scheduler.poll_ready();
        let ended = completion_message(&completion).expect("the thread is cancelled");
        assert!(ended.is_err_and(|error| error.contains("cancelled")));
        assert!(matches!(outcome.take(), Some(Watched::Stopped(()))));
        Ok(())
    }

    /// The end is timed when it happens, not when its round began or ended.
    #[test]
    fn a_completion_records_the_time_its_thread_ended() -> luau::runtime::Result<()> {
        assert_eq!(ended_after(false)?, Duration::ZERO);
        assert_eq!(ended_after(true)?, Duration::from_secs(1));
        Ok(())
    }

    #[test]
    fn a_completion_records_the_time_its_thread_was_cancelled() -> luau::runtime::Result<()> {
        let vm = luau::Vm::new()?;
        vm.open_standard_libraries(luau::StandardLibraries::all_supported())?;
        let clock = ManualClock::new();
        let scheduler = Scheduler::with_manual_clock(clock.clone());
        let start = clock.now();
        let completion = Rc::new(LocalLuauTaskCompletion::default());
        let thread = chunk_thread(&vm, "coroutine.yield()")?;
        scheduler.spawn_luau_thread_with_completion(
            CallContext::default(),
            thread.clone(),
            Vec::new(),
            completion.clone(),
        );
        scheduler.poll_ready();
        assert!(completion.ended_at().is_none(), "a yield ended the thread");

        clock.advance(Duration::from_secs(3));
        assert!(completion.ended_at().is_none());
        clock.advance(Duration::from_secs(4));
        assert!(scheduler.cancel_luau_thread(&thread));
        clock.advance(Duration::from_secs(5));

        assert_eq!(completion.ended_at(), Some(start + Duration::from_secs(7)));
        let ended = completion_message(&completion).expect("the thread is cancelled");
        assert!(ended.is_err_and(|error| error.contains("cancelled")));
        Ok(())
    }
}
