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

/// A Luau thread its watcher answers for, as [`Scheduler::schedule_watched_luau_thread`]
/// schedules it: however the watcher goes, the thread doesn't outlive it.
pub struct WatchedThread {
    scheduler: Weak<Scheduler>,
    timers: Rc<Timers>,
    thread: luau::Thread,
    pub(super) completion: Rc<LocalLuauTaskCompletion>,
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
