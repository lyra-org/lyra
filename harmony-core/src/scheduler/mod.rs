use std::{
    future::Future,
    panic::{
        AssertUnwindSafe,
        catch_unwind,
        resume_unwind,
    },
    pin::Pin,
    rc::Rc,
    sync::Arc,
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

use anyhow::Result;
use harmony_luau as luau;
use harmony_luau::{
    CallContext,
    ChunkOrigin,
    TaskGroupId,
};
use std::cell::{
    Cell,
    RefCell,
};

mod limit;
mod state;
mod time;
mod wake;
mod watch;
mod work;

pub use limit::{
    ResumeBound,
    ResumeLimit,
};
use state::{
    Deferred,
    LuauTaskOptions,
    State,
};
pub(crate) use time::Clock;
pub use time::Sleep;
use time::Timers;
pub use wake::{
    Cancellation,
    SchedulerWake,
};
use wake::{
    TaskWaker,
    WakeQueue,
};
pub use watch::{
    LateEnd,
    Watched,
    WatchedThread,
};
use work::{
    Resuming,
    TaskWork,
    close_cancelled_thread,
    drop_catching_panic,
    poll_work,
};

/// Polled on the scheduler's thread only, so it may hold VM handles.
pub type TaskFuture = Pin<Box<dyn Future<Output = Result<()>> + 'static>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TaskId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskState {
    Pending,
    Completed,
    Cancelled,
    Failed,
}

#[derive(Clone, Debug)]
pub struct TaskSnapshot {
    pub id: TaskId,
    pub group: TaskGroupId,
    pub origin: ChunkOrigin,
    pub state: TaskState,
    pub error: Option<Arc<str>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TaskHandle {
    id: TaskId,
}

#[derive(Default)]
pub struct LocalLuauTaskCompletion {
    result: RefCell<Option<std::result::Result<Vec<luau::Value>, luau::RaisedError>>>,
    ended_at: Cell<Option<Instant>>,
    interrupted_by: Cell<Option<ResumeBound>>,
    waker: RefCell<Option<Waker>>,
}

impl LocalLuauTaskCompletion {
    fn complete(
        &self,
        result: std::result::Result<Vec<luau::Value>, luau::RaisedError>,
        ended_at: Instant,
    ) {
        *self.result.borrow_mut() = Some(result);
        self.ended_at.set(Some(ended_at));
        if let Some(waker) = self.waker.borrow_mut().take() {
            waker.wake();
        }
    }

    /// The task's result, once, to the one waiter there is: a second waiter takes the first's
    /// wake.
    ///
    /// # Panics
    ///
    /// When polled after it gave the result, which would otherwise wait for good.
    pub fn poll(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<Vec<luau::Value>, luau::RaisedError>> {
        if let Some(result) = self.result.borrow_mut().take() {
            return Poll::Ready(result);
        }
        assert!(
            self.ended_at.get().is_none(),
            "a task's result was already read"
        );
        *self.waker.borrow_mut() = Some(cx.waker().clone());
        Poll::Pending
    }

    /// When the task ended or was cancelled, by its scheduler's clock, however much later its
    /// result is read.
    pub fn ended_at(&self) -> Option<Instant> {
        self.ended_at.get()
    }

    /// Which bound of its [`ResumeLimit`] cut the task's last resume short, for a task that failed
    /// as interrupted: `None` for one that ended any other way, also when it failed on its own
    /// after the limit had passed or after catching the interruption. A budget no longer than what was left until the deadline as
    /// the resume began reads as the budget.
    pub fn interrupted_by(&self) -> Option<ResumeBound> {
        self.interrupted_by.get()
    }

    /// The task's result, once it ends or is cancelled, as [`Self::poll`] gives it.
    pub async fn wait(&self) -> std::result::Result<Vec<luau::Value>, luau::RaisedError> {
        std::future::poll_fn(|cx| self.poll(cx)).await
    }
}

impl TaskHandle {
    pub fn id(self) -> TaskId {
        self.id
    }
}

pub struct Scheduler {
    state: RefCell<State>,
    timers: Rc<Timers>,
    wake_queue: Arc<WakeQueue>,
    wake: SchedulerWake,
    /// Set while [`Self::poll_ready`] runs a round.
    in_round: Cell<bool>,
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::with_clock(Clock::Real)
    }
}

impl Scheduler {
    pub fn new() -> Self {
        Self::default()
    }

    /// A scheduler whose time is `clock`'s, for a test that polls it. It never blocks:
    /// [`Self::wait_for_work`] and [`Self::wait_for_wake`] panic, and what drives a thread to its
    /// end on the calling thread, such as [`crate::luau::drive_thread`], fails before running it.
    #[cfg(any(test, feature = "testing"))]
    pub fn with_manual_clock(clock: crate::testing::ManualClock) -> Self {
        Self::with_clock(Clock::Manual(clock))
    }

    pub(crate) fn with_clock(clock: Clock) -> Self {
        let timers = Rc::new(Timers::new(clock));
        let wake_queue = Arc::new(WakeQueue::default());
        Self {
            state: RefCell::new(State::new(timers.clone(), wake_queue.clone())),
            wake: SchedulerWake::of(wake_queue.clone()),
            timers,
            wake_queue,
            in_round: Cell::default(),
        }
    }

    /// The time by the scheduler's clock, which every deadline given to it is read against.
    pub fn now(&self) -> Instant {
        self.timers.now()
    }

    /// Fails on a manual clock, for a caller about to block on this scheduler.
    pub(crate) fn ensure_can_wait(&self) -> Result<()> {
        self.timers.clock().ensure_can_wait()
    }

    pub fn create_group(&self) -> TaskGroupId {
        self.state.borrow_mut().next_group()
    }

    /// Does what the state left to do once released, one step for all of it before the next:
    /// every thread is let go of, and closed for a task cancelled, then every piece of work is
    /// dropped, then every completion is told. So no completion's waker finds a thread cancelled
    /// with its own still open, or the work of a task cancelled with it still held, and none that
    /// panics keeps the rest from being told: the first panic carries on once all is done.
    fn finish(&self, deferred: impl IntoIterator<Item = Deferred>) {
        let deferred: Vec<_> = deferred.into_iter().collect();
        if deferred.is_empty() {
            return;
        }
        for deferred in &deferred {
            match deferred {
                Deferred::Settled {
                    thread: Some(thread),
                    release,
                    close,
                    ..
                } => {
                    if *release {
                        let _ = thread.clear_call_context();
                    }
                    if *close {
                        close_cancelled_thread(thread);
                    }
                }
                Deferred::Close(Some(thread)) => close_cancelled_thread(thread),
                Deferred::Settled { .. } | Deferred::Close(None) | Deferred::Drop { .. } => {}
            }
        }
        let mut reports = Vec::new();
        for deferred in deferred {
            match deferred {
                Deferred::Settled {
                    context,
                    report,
                    work,
                    ..
                } => {
                    drop_catching_panic(&context, work);
                    reports.extend(report);
                }
                Deferred::Close(_) => {}
                Deferred::Drop {
                    context,
                    work,
                    wait,
                    removed,
                } => {
                    drop_catching_panic(&context, work);
                    drop_catching_panic(&context, wait);
                    drop_catching_panic(&context, removed);
                }
            }
        }
        let now = self.now();
        let mut panicked = None;
        for report in reports {
            if let Err(panic) = catch_unwind(AssertUnwindSafe(|| report.tell(now))) {
                panicked.get_or_insert(panic);
            }
        }
        if let Some(panic) = panicked {
            resume_unwind(panic);
        }
    }

    /// Cancels every pending task in `group`, and any task later spawned into it, and closes
    /// their threads at once, as [`Self::cancel_luau_thread`] closes one. The default group is
    /// never cancelled. The task being polled, if it is in the group, is cancelled once its poll
    /// returns.
    pub fn cancel_group(&self, group: TaskGroupId) {
        let deferred = self.state.borrow_mut().cancel_group(group);
        self.finish(deferred);
    }

    /// Runs `future` as a task of `context`'s group. Like every task spawned during a round, it
    /// first runs in the next round; see [`Self::poll_ready`].
    pub fn spawn_future(
        &self,
        context: CallContext,
        future: impl Future<Output = Result<()>> + 'static,
    ) -> TaskHandle {
        self.spawn_boxed(context, Box::pin(future), None, false)
    }

    /// [`Self::spawn_future`] for a task that watches others: every round runs the watchers
    /// first, see [`Self::poll_ready`], so a thread one cancels on waking isn't resumed again.
    pub fn spawn_watcher(
        &self,
        context: CallContext,
        future: impl Future<Output = Result<()>> + 'static,
    ) -> TaskHandle {
        self.spawn_boxed(context, Box::pin(future), None, true)
    }

    fn spawn_boxed(
        &self,
        context: CallContext,
        future: TaskFuture,
        wake_at: Option<Instant>,
        watcher: bool,
    ) -> TaskHandle {
        let (handle, deferred) = {
            self.state.borrow_mut().insert_work(
                context,
                TaskWork::Future(future),
                wake_at,
                LuauTaskOptions::default(),
                watcher,
            )
        };
        // A task spawned into a cancelled group is cancelled at once.
        self.finish(deferred);
        handle
    }

    /// A future that completes once the scheduler's clock reaches `deadline`, for a task of this
    /// scheduler to wait on: the deadline ends [`Self::wait_for_work`], and the poll that follows
    /// wakes the task.
    pub fn sleep_until(&self, deadline: Instant) -> Sleep {
        self.timers.sleep(deadline)
    }

    pub fn set_luau_resume_budget(&self, budget: Option<Duration>) {
        self.state.borrow_mut().set_luau_resume_budget(budget);
    }

    pub fn spawn_luau_thread(
        &self,
        context: CallContext,
        thread: luau::Thread,
        args: Vec<luau::Value>,
    ) -> TaskHandle {
        let options = self.luau_task_options(None, None);
        self.spawn_luau_task(context, thread, args, None, options)
    }

    pub fn spawn_luau_thread_after(
        &self,
        context: CallContext,
        delay: Duration,
        thread: luau::Thread,
        args: Vec<luau::Value>,
    ) -> TaskHandle {
        let options = self.luau_task_options(None, None);
        self.spawn_luau_task(
            context,
            thread,
            args,
            self.delay_until(Some(delay)),
            options,
        )
    }

    /// Runs `thread` under the scheduler's own resume budget, reporting to `completion`.
    pub fn spawn_luau_thread_with_completion(
        &self,
        context: CallContext,
        thread: luau::Thread,
        args: Vec<luau::Value>,
        completion: Rc<LocalLuauTaskCompletion>,
    ) -> TaskHandle {
        let options = self.luau_task_options(None, Some(completion));
        self.spawn_luau_task(context, thread, args, None, options)
    }

    pub fn spawn_luau_thread_with_limit_and_completion(
        &self,
        context: CallContext,
        thread: luau::Thread,
        args: Vec<luau::Value>,
        limit: ResumeLimit,
        completion: Rc<LocalLuauTaskCompletion>,
    ) -> TaskHandle {
        let options = self.luau_task_options(Some(limit), Some(completion));
        self.spawn_luau_task(context, thread, args, None, options)
    }

    /// Runs `thread` under `limit`, or the scheduler's own resume budget without one, for a
    /// watcher to answer for.
    pub fn spawn_watched_luau_thread(
        self: &Rc<Self>,
        context: CallContext,
        thread: luau::Thread,
        args: Vec<luau::Value>,
        limit: Option<ResumeLimit>,
    ) -> WatchedThread {
        let completion = Rc::new(LocalLuauTaskCompletion::default());
        let options = self.luau_task_options(limit, Some(completion.clone()));
        self.spawn_luau_task(context, thread.clone(), args, None, options);
        WatchedThread::new(Rc::downgrade(self), self.timers.clone(), thread, completion)
    }

    /// A Luau task's options: `limit`, or the scheduler's own resume budget without one, and
    /// `completion`.
    fn luau_task_options(
        &self,
        limit: Option<ResumeLimit>,
        completion: Option<Rc<LocalLuauTaskCompletion>>,
    ) -> LuauTaskOptions {
        let default = self.state.borrow().default_luau_task_options();
        LuauTaskOptions {
            resume_limit: limit.or(default.resume_limit),
            completion,
        }
    }

    /// Runs `thread` with `args` as a task of `context`'s group, from `wake_at` if given.
    fn spawn_luau_task(
        &self,
        context: CallContext,
        thread: luau::Thread,
        args: Vec<luau::Value>,
        wake_at: Option<Instant>,
        options: LuauTaskOptions,
    ) -> TaskHandle {
        let args = self.prepare_luau_thread_start_args(&thread, args, wake_at);
        let pending_start = !thread.is_started();
        let (handle, deferred) = {
            let mut state = self.state.borrow_mut();
            let (handle, deferred) = state.insert_work(
                context,
                TaskWork::LuauThread {
                    thread: thread.clone(),
                    args,
                    pending_start,
                },
                wake_at,
                options,
                false,
            );
            state.track_thread(&thread, handle);
            (handle, deferred)
        };
        self.finish(deferred);
        handle
    }

    /// Has `thread` wait on `future`, after `delay` if one is given, for a host call it made as
    /// `context`: the task the scheduler is polling as the thread asks takes up the wait once the
    /// thread yields to it, and a thread no task is resuming, such as a coroutine Luau code
    /// resumed, waits as a task of its own, acting as itself and also belonging to `context`'s
    /// group while it waits.
    pub(crate) fn wait_on_future(
        &self,
        context: CallContext,
        thread: luau::Thread,
        future: luau::ScheduledFuture,
        delay: Option<Duration>,
    ) {
        let wake_at = self.delay_until(delay);
        let awaiting = context.task_group;
        let waiting = self
            .state
            .borrow_mut()
            .wait_in_polled_task(&thread, awaiting, future, wake_at);
        let future = match waiting {
            Ok(replaced) => {
                drop_catching_panic(&context, replaced);
                return;
            }
            Err(future) => future,
        };
        let own = thread.call_context().ok().flatten().unwrap_or_default();
        let spawned = self
            .state
            .borrow_mut()
            .spawn_borrowed_wait(own, awaiting, thread, future, wake_at);
        self.finish(spawned);
    }

    /// Whether `group`'s run has ended.
    pub fn is_group_cancelled(&self, group: TaskGroupId) -> bool {
        self.state.borrow().is_cancelled(group)
    }

    pub fn cancel(&self, id: TaskId) -> bool {
        let deferred = self.state.borrow_mut().cancel(id);
        let cancelled = deferred.is_some();
        self.finish(deferred);
        cancelled
    }

    /// Cancels `thread`'s task, and closes the thread at once: Luau code holding it can't resume
    /// it from here on. A thread that is running, or has resumed another, goes on to where it
    /// next yields, and is closed there. The task being polled is cancelled as its poll returns.
    pub fn cancel_luau_thread(&self, thread: &luau::Thread) -> bool {
        if self.state.borrow().holds_thread(thread) {
            close_cancelled_thread(thread);
        }
        let Some(handle) = self.state.borrow_mut().untrack_thread(thread) else {
            return false;
        };
        self.cancel(handle.id())
    }

    /// [`Self::cancel_luau_thread`], returning a future that completes once the thread's work is
    /// dropped: at once, unless the thread's task is the one being polled, which is cancelled as
    /// its poll returns.
    pub fn cancel_luau_thread_and_settle(
        &self,
        thread: &luau::Thread,
    ) -> impl Future<Output = ()> + use<> {
        let settled = self.state.borrow_mut().wait_for_polled_thread(thread);
        self.cancel_luau_thread(thread);
        async move {
            if let Some(settled) = settled {
                let _ = settled.await;
            }
        }
    }

    pub fn remove(&self, id: TaskId) -> bool {
        let deferred = self.state.borrow_mut().remove(id);
        let removed = deferred.is_some();
        self.finish(deferred);
        removed
    }

    pub fn luau_thread_handle(&self, thread: &luau::Thread) -> Option<TaskHandle> {
        self.state.borrow().thread_task(thread)
    }

    pub fn take_luau_thread_output(&self, thread: &luau::Thread) -> Option<Vec<luau::Value>> {
        let handle = self.luau_thread_handle(thread)?;
        self.state.borrow_mut().take_output(handle.id())
    }

    /// Runs a round: every task ready as it begins, once. The watchers run first, and what they
    /// asked for is done before any other task runs: a thread or a group one of them cancelled is
    /// dropped, along with whatever its work held, such as the threads of the calls it was waiting
    /// on. Each task runs with the scheduler free; a task started or woken during the round first
    /// runs in the next one.
    ///
    /// # Panics
    ///
    /// When called during a round of the same scheduler, as from one of its tasks.
    pub fn poll_ready(&self) -> usize {
        let _round = Round::begin(&self.in_round);
        self.timers.wake_due(self.now());
        self.state.borrow_mut().begin_round();
        self.poll_queue(true) + self.poll_queue(false)
    }

    /// Polls the round's watchers, or its other tasks, once each, and returns how many ended or
    /// went on to wait on a host call. Each is taken from the round as it is polled, so a poll
    /// that unwinds leaves the rest to the next round.
    fn poll_queue(&self, watchers: bool) -> usize {
        let mut settled = 0;
        loop {
            let next = self.state.borrow_mut().next_in_round(watchers);
            let Some(id) = next else {
                break;
            };
            if self.poll_task(id) {
                settled += 1;
            }
        }
        settled
    }

    /// Takes task `id`'s work out of its slot, polls it with the scheduler free, and puts it back
    /// or settles the task as the poll came to.
    fn poll_task(&self, id: TaskId) -> bool {
        let Some(mut taken) = self.take_work(id) else {
            return false;
        };
        let waker = TaskWaker::of(id, &self.wake_queue);
        let mut cx = Context::from_waker(&waker);
        let interrupted_by = Cell::new(None);
        let polled = poll_work(
            &taken.context,
            taken
                .work
                .as_mut()
                .expect("the work is out until the poll ends"),
            &mut cx,
            &Resuming {
                cancelled: &taken.cancelled,
                limit: taken.resume_limit.as_ref(),
                clock: self.timers.clock(),
                interrupted_by: &interrupted_by,
            },
        );
        let (context, work) = taken.put_back();
        let (counted, deferred, settled) =
            self.state
                .borrow_mut()
                .after_poll(id, context, work, polled, interrupted_by.get());
        self.finish(deferred);
        for settled in settled {
            let _ = settled.send(());
        }
        counted
    }

    /// Takes task `id`'s work out of its slot to poll it, under a guard that fails the task if the
    /// poll unwinds before the work is put back.
    fn take_work(&self, id: TaskId) -> Option<TakenWork<'_>> {
        let (context, work, resume_limit, cancelled) = self.state.borrow_mut().take_for_poll(id)?;
        Some(TakenWork {
            scheduler: self,
            id,
            context,
            work: Some(work),
            resume_limit,
            cancelled,
        })
    }

    pub fn snapshot(&self, id: TaskId) -> Option<TaskSnapshot> {
        self.state.borrow().snapshot(id)
    }

    pub fn snapshots(&self) -> Vec<TaskSnapshot> {
        self.state.borrow().snapshots()
    }

    pub fn group_snapshots(&self, group: TaskGroupId) -> Vec<TaskSnapshot> {
        self.state.borrow().group_snapshots(group)
    }

    pub fn has_pending(&self) -> bool {
        self.state.borrow().has_pending()
    }

    /// How long, by the scheduler's clock, until it has a task to poll. `None` when no task is
    /// ready or due.
    pub fn next_wake_delay(&self) -> Option<Duration> {
        if self.state.borrow().has_ready() || self.wake_queue.has_pending_wake() {
            return Some(Duration::ZERO);
        }
        self.timers.next_delay(self.now())
    }

    /// Waits until a task is woken, [`SchedulerWake::wake`] is called, or `timeout` passes.
    /// Returns at once when either happened since the last wait.
    ///
    /// # Panics
    ///
    /// On a manual clock, whose time no wait would see pass.
    pub fn wait_for_wake(&self, timeout: Option<Duration>) {
        if let Err(error) = self.ensure_can_wait() {
            panic!("{error}");
        }
        self.wake_queue.wait(timeout);
    }

    pub fn wake_handle(&self) -> SchedulerWake {
        self.wake.clone()
    }

    /// Sleeps until there is something to poll: a task is woken or due, a [`Sleep`] is due, a
    /// [`SchedulerWake`] fires, or `deadline`, the earliest among the work the caller polls
    /// itself, passes.
    ///
    /// # Panics
    ///
    /// On a manual clock, whose time no wait would see pass.
    pub fn wait_for_work(&self, deadline: Option<Instant>) {
        let now = self.now();
        let due = if self.has_pending() {
            self.next_wake_delay()
        } else {
            self.timers.next_delay(now)
        };
        let until_deadline = deadline.map(|deadline| deadline.saturating_duration_since(now));
        let timeout = match (due, until_deadline) {
            (Some(due), Some(until_deadline)) => Some(due.min(until_deadline)),
            (due, until_deadline) => due.or(until_deadline),
        };
        self.wait_for_wake(timeout);
    }

    pub fn remove_finished(&self) -> usize {
        let (removed, deferred) = self.state.borrow_mut().remove_finished();
        self.finish(deferred);
        removed
    }

    fn prepare_luau_thread_start_args(
        &self,
        thread: &luau::Thread,
        mut args: Vec<luau::Value>,
        wake_at: Option<Instant>,
    ) -> Vec<luau::Value> {
        if thread.is_started() {
            return args;
        }
        let Some(handle) = self.state.borrow().pending_luau_thread_start(thread) else {
            return args;
        };
        let Some((mut merged_args, deferred)) = self
            .state
            .borrow_mut()
            .take_pending_luau_thread_start(handle, wake_at)
        else {
            return args;
        };
        self.finish([deferred]);
        merged_args.append(&mut args);
        merged_args
    }

    /// `delay` from now, or now when that is past the last instant.
    fn delay_until(&self, delay: Option<Duration>) -> Option<Instant> {
        delay.map(|delay| {
            let now = self.now();
            now.checked_add(delay).unwrap_or(now)
        })
    }
}

/// Marks a [`Scheduler`] as running a round until dropped.
struct Round<'a>(&'a Cell<bool>);

impl<'a> Round<'a> {
    fn begin(in_round: &'a Cell<bool>) -> Self {
        assert!(
            !in_round.replace(true),
            "Scheduler::poll_ready was called during a round of the same scheduler, as from one \
             of its tasks: a task must not drive the scheduler it runs on"
        );
        Self(in_round)
    }
}

impl Drop for Round<'_> {
    fn drop(&mut self) {
        self.0.set(false);
    }
}

/// The work of the task being polled, out of its slot. A poll that unwinds before the work is put
/// back fails the task, and leaves the scheduler usable.
struct TakenWork<'a> {
    scheduler: &'a Scheduler,
    id: TaskId,
    context: CallContext,
    work: Option<TaskWork>,
    resume_limit: Option<ResumeLimit>,
    cancelled: Rc<Cell<bool>>,
}

impl TakenWork<'_> {
    /// The context the task runs as and its work, for the scheduler to put back.
    fn put_back(mut self) -> (CallContext, TaskWork) {
        let work = self.work.take().expect("the work is put back once");
        (std::mem::take(&mut self.context), work)
    }
}

impl Drop for TakenWork<'_> {
    fn drop(&mut self) {
        let Some(work) = self.work.take() else {
            return;
        };
        let Ok(mut state) = self.scheduler.state.try_borrow_mut() else {
            return;
        };
        let (failed, settled) =
            state.poll_unwound(self.id, std::mem::take(&mut self.context), work);
        drop(state);
        self.scheduler.finish(failed);
        drop(settled);
    }
}

#[cfg(test)]
pub(crate) mod testing;
#[cfg(test)]
mod tests;
