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
    thread_key,
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

pub(super) enum LuauThreadStart {
    Immediate,
    Delayed(Duration),
    Limited {
        limit: ResumeLimit,
        completion: Rc<LocalLuauTaskCompletion>,
    },
    /// Immediately, under the scheduler's own limit, reporting to the completion.
    WithCompletion(Rc<LocalLuauTaskCompletion>),
}

pub struct Scheduler {
    state: RefCell<State>,
    timers: Rc<Timers>,
    wake_queue: Arc<WakeQueue>,
    wake: SchedulerWake,
    /// Set while [`Self::poll_ready`] runs a round.
    in_round: Cell<bool>,
    pending_luau_work: RefCell<Vec<PendingLuauWork>>,
    pending_luau_cancellations: RefCell<Vec<luau::Thread>>,
    pending_group_cancellations: RefCell<Vec<TaskGroupId>>,
    /// Told once the poll under way has ended and what it deferred is done.
    poll_end: RefCell<Vec<tokio::sync::oneshot::Sender<()>>>,
    polling: Cell<bool>,
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
            pending_luau_work: RefCell::default(),
            pending_luau_cancellations: RefCell::default(),
            pending_group_cancellations: RefCell::default(),
            poll_end: RefCell::default(),
            polling: Cell::default(),
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
                    removed,
                } => {
                    drop_catching_panic(&context, work);
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
    /// never cancelled. While the scheduler polls, as from a task, the group's tasks run no
    /// further and are cancelled once the poll ends.
    pub fn cancel_group(&self, group: TaskGroupId) {
        if self.polling.get() {
            if group != TaskGroupId::default() {
                self.state.borrow_mut().mark_group_cancelled(group);
                self.pending_group_cancellations.borrow_mut().push(group);
                self.close_group(group);
            }
            return;
        }
        let deferred = self.state.borrow_mut().cancel_group(group);
        self.finish(deferred);
        if group != TaskGroupId::default() {
            // Work a poll put off and never took up, as when it unwound, goes with the group.
            self.cancel_pending_luau_work(|work| work.is_in(group));
        }
        self.flush_pending_luau_cancellations();
    }

    /// Closes the threads of `group`'s pending tasks and of the work a poll has put off for it.
    fn close_group(&self, group: TaskGroupId) {
        let mut threads = self.state.borrow().group_threads(group);
        threads.extend(
            self.pending_luau_work
                .borrow()
                .iter()
                .filter(|work| work.is_in(group))
                .map(|work| work.thread.clone()),
        );
        for thread in threads {
            close_cancelled_thread(&thread);
        }
    }

    /// Runs `future` as a task of `context`'s group, from the next round on.
    pub fn schedule_future(
        &self,
        context: CallContext,
        future: impl Future<Output = Result<()>> + 'static,
    ) {
        self.spawn_boxed(context, Box::pin(future), None, false);
    }

    /// [`Self::schedule_future`] for a task that watches others: every poll runs the watchers
    /// first, see [`Self::poll_ready`], so a thread one cancels on waking isn't resumed again.
    pub fn schedule_watcher(
        &self,
        context: CallContext,
        future: impl Future<Output = Result<()>> + 'static,
    ) {
        self.spawn_boxed(context, Box::pin(future), None, true);
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
        self.spawn_luau_thread_start(context, thread, args, LuauThreadStart::Immediate)
    }

    pub fn schedule_luau_thread(
        &self,
        context: CallContext,
        thread: luau::Thread,
        args: Vec<luau::Value>,
    ) {
        self.spawn_luau_thread_start(context, thread, args, LuauThreadStart::Immediate);
    }

    /// Runs `thread` under the scheduler's own resume budget, reporting to `completion`.
    pub fn schedule_luau_thread_with_completion(
        &self,
        context: CallContext,
        thread: luau::Thread,
        args: Vec<luau::Value>,
        completion: Rc<LocalLuauTaskCompletion>,
    ) {
        self.spawn_luau_thread_start(
            context,
            thread,
            args,
            LuauThreadStart::WithCompletion(completion),
        );
    }

    pub fn schedule_luau_thread_with_limit_and_completion(
        &self,
        context: CallContext,
        thread: luau::Thread,
        args: Vec<luau::Value>,
        limit: ResumeLimit,
        completion: Rc<LocalLuauTaskCompletion>,
    ) {
        self.spawn_luau_thread_start(
            context,
            thread,
            args,
            LuauThreadStart::Limited { limit, completion },
        );
    }

    /// Runs `thread` under `limit`, or the scheduler's own resume budget without one, for a
    /// watcher to answer for.
    pub fn schedule_watched_luau_thread(
        self: &Rc<Self>,
        context: CallContext,
        thread: luau::Thread,
        args: Vec<luau::Value>,
        limit: Option<ResumeLimit>,
    ) -> WatchedThread {
        let completion = Rc::new(LocalLuauTaskCompletion::default());
        let start = match limit {
            Some(limit) => LuauThreadStart::Limited {
                limit,
                completion: completion.clone(),
            },
            None => LuauThreadStart::WithCompletion(completion.clone()),
        };
        self.spawn_luau_thread_start(context, thread.clone(), args, start);
        WatchedThread::new(Rc::downgrade(self), self.timers.clone(), thread, completion)
    }

    fn spawn_luau_thread_start(
        &self,
        context: CallContext,
        thread: luau::Thread,
        args: Vec<luau::Value>,
        start: LuauThreadStart,
    ) -> TaskHandle {
        let wake_at = match &start {
            LuauThreadStart::Delayed(delay) => self.delay_until(Some(*delay)),
            LuauThreadStart::Immediate
            | LuauThreadStart::Limited { .. }
            | LuauThreadStart::WithCompletion(_) => None,
        };
        let args = self.prepare_luau_thread_start_args(&thread, args, wake_at);
        let pending_start = !thread.is_started();
        let (handle, deferred) = {
            let mut state = self.state.borrow_mut();
            let options = match start {
                LuauThreadStart::Immediate | LuauThreadStart::Delayed(_) => {
                    state.default_luau_task_options()
                }
                LuauThreadStart::WithCompletion(completion) => LuauTaskOptions {
                    completion: Some(completion),
                    ..state.default_luau_task_options()
                },
                LuauThreadStart::Limited { limit, completion } => LuauTaskOptions {
                    resume_limit: Some(limit),
                    completion: Some(completion),
                },
            };
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

    pub fn spawn_luau_thread_after(
        &self,
        context: CallContext,
        delay: Duration,
        thread: luau::Thread,
        args: Vec<luau::Value>,
    ) -> TaskHandle {
        self.spawn_luau_thread_start(context, thread, args, LuauThreadStart::Delayed(delay))
    }

    pub fn schedule_luau_thread_after(
        &self,
        context: CallContext,
        delay: Duration,
        thread: luau::Thread,
        args: Vec<luau::Value>,
    ) {
        self.spawn_luau_thread_start(context, thread, args, LuauThreadStart::Delayed(delay));
    }

    pub fn schedule_luau_future(
        &self,
        context: CallContext,
        thread: luau::Thread,
        future: luau::ScheduledFuture,
    ) {
        self.schedule_luau_future_with_delay(context, None, thread, future);
    }

    pub fn schedule_luau_future_after(
        &self,
        context: CallContext,
        delay: Duration,
        thread: luau::Thread,
        future: luau::ScheduledFuture,
    ) {
        self.schedule_luau_future_with_delay(context, Some(delay), thread, future);
    }

    fn schedule_luau_future_with_delay(
        &self,
        context: CallContext,
        delay: Option<Duration>,
        thread: luau::Thread,
        future: luau::ScheduledFuture,
    ) {
        let target = self.state.borrow().thread_task(&thread);
        if self.polling.get() {
            self.pending_luau_work.borrow_mut().push(PendingLuauWork {
                target,
                context,
                delay,
                thread,
                future,
            });
            return;
        }

        self.spawn_untracked_luau_future(
            context.task_group,
            self.delay_until(delay),
            thread,
            future,
        );
    }

    /// A thread no task tracks, such as a coroutine, keeps acting as itself
    /// while it awaits `future`; only its membership follows the future.
    fn spawn_untracked_luau_future(
        &self,
        awaiting: TaskGroupId,
        wake_at: Option<Instant>,
        thread: luau::Thread,
        future: luau::ScheduledFuture,
    ) {
        let context = thread.call_context().ok().flatten().unwrap_or_default();
        let deferred = {
            let mut state = self.state.borrow_mut();
            // Luau code, not the scheduler, resumed the thread that parked, so no resume will
            // take its park; the new task starts it fresh.
            state.take_parked_luau_thread(&thread);
            state
                .spawn_luau_future(context, awaiting, thread, future, wake_at)
                .1
        };
        self.finish(deferred);
    }

    /// Whether `group`'s run has ended.
    pub fn is_group_cancelled(&self, group: TaskGroupId) -> bool {
        self.state.borrow().is_cancelled(group)
    }

    pub fn cancel(&self, id: TaskId) -> bool {
        let deferred = self.state.borrow_mut().cancel(id);
        let cancelled = deferred.is_some();
        self.finish(deferred);
        self.flush_pending_luau_cancellations();
        cancelled
    }

    /// Cancels `thread`'s task, and closes the thread at once: Luau code holding it can't resume
    /// it from here on. While the scheduler polls, as from a task, the task is cancelled once the
    /// scheduler is free: before any other task runs when a watcher asks, and at the end of the
    /// poll otherwise. A thread that is running, or has resumed another, goes on to where it next
    /// yields, and is closed there.
    pub fn cancel_luau_thread(&self, thread: &luau::Thread) -> bool {
        let key = thread_key(thread);
        let scheduled = self.close_scheduled(thread);
        if self.polling.get() {
            self.mark_cancelling(thread);
            self.pending_luau_cancellations
                .borrow_mut()
                .push(thread.clone());
            return scheduled;
        }
        // Work a poll put off and never took up, as when it unwound, goes with the task.
        let pending = self.cancel_pending_luau_work(|work| thread_key(&work.thread) == key);
        let Some(handle) = self.state.borrow_mut().untrack_thread(thread) else {
            return pending;
        };
        self.cancel(handle.id()) || pending
    }

    fn mark_cancelling(&self, thread: &luau::Thread) {
        let mut state = self.state.borrow_mut();
        if let Some(handle) = state.thread_task(thread) {
            state.mark_cancelling(handle.id());
        }
    }

    /// Closes `thread` if the scheduler holds work for it, and returns whether it does.
    fn close_scheduled(&self, thread: &luau::Thread) -> bool {
        let key = thread_key(thread);
        let held = self.state.borrow().holds_thread(thread)
            || self
                .pending_luau_work
                .borrow()
                .iter()
                .any(|work| thread_key(&work.thread) == key);
        if !held {
            return false;
        }
        close_cancelled_thread(thread);
        true
    }

    /// [`Self::cancel_luau_thread`], returning a future that completes once the thread's work is
    /// dropped: at once outside a poll, and when the scheduler is free inside one.
    pub fn cancel_luau_thread_and_settle(
        &self,
        thread: &luau::Thread,
    ) -> impl Future<Output = ()> + use<> {
        self.cancel_luau_thread(thread);
        let settled = self.polling.get().then(|| {
            let (ended, settled) = tokio::sync::oneshot::channel();
            self.poll_end.borrow_mut().push(ended);
            settled
        });
        async move {
            if let Some(settled) = settled {
                let _ = settled.await;
            }
        }
    }

    /// Cancels `thread`'s task the next time the scheduler is free to, for a caller that may be
    /// running inside it, such as a destructor. The thread is closed at once, as
    /// [`Self::cancel_luau_thread`] closes it.
    pub fn schedule_cancel_luau_thread(&self, thread: luau::Thread) {
        self.close_scheduled(&thread);
        if self.polling.get() {
            self.mark_cancelling(&thread);
        }
        self.pending_luau_cancellations.borrow_mut().push(thread);
    }

    pub fn remove(&self, id: TaskId) -> bool {
        let deferred = self.state.borrow_mut().remove(id);
        let removed = deferred.is_some();
        self.finish(deferred);
        self.flush_pending_luau_cancellations();
        removed
    }

    pub fn park_luau_thread(&self, thread: &luau::Thread) {
        self.state.borrow_mut().park_luau_thread(thread);
    }

    pub fn luau_thread_handle(&self, thread: &luau::Thread) -> Option<TaskHandle> {
        self.state.borrow().thread_task(thread)
    }

    pub fn take_luau_thread_output(&self, thread: &luau::Thread) -> Option<Vec<luau::Value>> {
        let handle = self.luau_thread_handle(thread)?;
        self.state.borrow_mut().take_output(handle.id())
    }

    pub(super) fn take_parked_luau_thread(&self, thread: &luau::Thread) -> bool {
        self.state.borrow_mut().take_parked_luau_thread(thread)
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
        let watchers = self.poll_queue(true);
        self.end_poll();
        let others = self.poll_queue(false);
        self.end_poll();
        watchers + others
    }

    /// Polls the round's watchers, or its other tasks, once each, and returns how many ended or
    /// parked. Each is taken from the round as it is polled, so a poll that unwinds leaves the
    /// rest to the next round.
    fn poll_queue(&self, watchers: bool) -> usize {
        let _polling = Polling::begin(&self.polling);
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
        let cancel_asked = || self.state.borrow().cancel_asked(id);
        let polled = poll_work(
            &taken.context,
            taken
                .work
                .as_mut()
                .expect("the work is out until the poll ends"),
            &mut cx,
            &Resuming {
                cancel_asked: &cancel_asked,
                limit: taken.resume_limit.as_ref(),
                clock: self.timers.clock(),
                interrupted_by: &interrupted_by,
            },
        );
        let (context, work) = taken.put_back();
        let (counted, deferred) =
            self.state
                .borrow_mut()
                .after_poll(id, context, work, polled, interrupted_by.get());
        self.finish(deferred);
        counted
    }

    /// Takes task `id`'s work out of its slot to poll it, under a guard that fails the task if the
    /// poll unwinds before the work is put back.
    fn take_work(&self, id: TaskId) -> Option<TakenWork<'_>> {
        let (context, work, resume_limit) = self.state.borrow_mut().take_for_poll(id)?;
        Some(TakenWork {
            scheduler: self,
            id,
            context,
            work: Some(work),
            resume_limit,
        })
    }

    /// Does what was asked of the scheduler while it polled.
    fn end_poll(&self) {
        self.flush_pending_luau_futures();
        self.flush_pending_group_cancellations();
        self.flush_pending_luau_cancellations();
        self.state.borrow_mut().clear_cancelling();
        for ended in std::mem::take(&mut *self.poll_end.borrow_mut()) {
            let _ = ended.send(());
        }
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
        self.flush_pending_luau_cancellations();
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

    /// Cancels the waits a poll put off that `matches`, as their tasks would be: closes their
    /// threads, then drops the waits, and returns whether there were any.
    fn cancel_pending_luau_work(&self, matches: impl Fn(&PendingLuauWork) -> bool) -> bool {
        let (cancelled, kept) = std::mem::take(&mut *self.pending_luau_work.borrow_mut())
            .into_iter()
            .partition::<Vec<_>, _>(matches);
        *self.pending_luau_work.borrow_mut() = kept;
        // Closed before the work is dropped, as a task's is.
        for work in &cancelled {
            close_cancelled_thread(&work.thread);
        }
        let any = !cancelled.is_empty();
        for work in cancelled {
            drop_catching_panic(&work.context.clone(), work);
        }
        any
    }

    fn flush_pending_luau_futures(&self) {
        let pending = std::mem::take(&mut *self.pending_luau_work.borrow_mut());
        for PendingLuauWork {
            target,
            context,
            delay,
            thread,
            future,
        } in pending
        {
            let wake_at = self.delay_until(delay);
            // A task that is not parked, such as a finished one that borrowed a coroutine, no
            // longer carries the thread.
            let target = target.filter(|handle| self.state.borrow().is_parked(handle.id()));
            let Some(handle) = target else {
                self.spawn_untracked_luau_future(context.task_group, wake_at, thread, future);
                continue;
            };
            let replaced = self.state.borrow_mut().replace_with_luau_future(
                handle.id(),
                context.task_group,
                thread.clone(),
                future,
                wake_at,
            );
            match replaced {
                Ok(()) => self.state.borrow_mut().track_thread(&thread, handle),
                Err(refused) => {
                    // The parked thread has nothing left to resume it. Its wait is dropped once
                    // the task is cancelled, which closes the thread first.
                    self.cancel(handle.id());
                    drop_catching_panic(&context, refused);
                }
            }
        }
    }

    fn flush_pending_group_cancellations(&self) {
        let pending = std::mem::take(&mut *self.pending_group_cancellations.borrow_mut());
        for group in pending {
            self.cancel_group(group);
        }
    }

    fn flush_pending_luau_cancellations(&self) {
        let pending = std::mem::take(&mut *self.pending_luau_cancellations.borrow_mut());
        for thread in pending {
            self.cancel_luau_thread(&thread);
        }
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
        let failed = state.poll_unwound(self.id, std::mem::take(&mut self.context), work);
        drop(state);
        self.scheduler.finish([failed]);
    }
}

/// Marks a [`Scheduler`] as polling until dropped, so that a poll that unwinds doesn't leave it
/// putting off whatever it is asked from then on.
struct Polling<'a>(&'a Cell<bool>);

impl<'a> Polling<'a> {
    fn begin(polling: &'a Cell<bool>) -> Self {
        polling.set(true);
        Self(polling)
    }
}

impl Drop for Polling<'_> {
    fn drop(&mut self) {
        self.0.set(false);
    }
}

/// A wait a thread asked for during a poll, for the scheduler to take up once the poll ends.
struct PendingLuauWork {
    target: Option<TaskHandle>,
    context: CallContext,
    delay: Option<Duration>,
    thread: luau::Thread,
    future: luau::ScheduledFuture,
}

impl PendingLuauWork {
    /// Whether the task this becomes belongs to `group`: as its own, or as that of the run whose
    /// future it awaits.
    fn is_in(&self, group: TaskGroupId) -> bool {
        self.context.task_group == group
            || self
                .thread
                .call_context()
                .ok()
                .flatten()
                .is_some_and(|own| own.task_group == group)
    }
}

#[cfg(test)]
pub(crate) mod tests;
