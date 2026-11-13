use std::{
    collections::{
        HashMap,
        HashSet,
    },
    future::Future,
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
use state::State;
pub(crate) use time::Clock;
pub use time::Sleep;
use time::Timers;
pub use wake::{
    Cancellation,
    SchedulerWake,
};
pub use watch::{
    LateEnd,
    Watched,
    WatchedThread,
};
use work::{
    Detached,
    close_cancelled_thread,
    drop_catching_panic,
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

struct LuauTaskOptions {
    resume_limit: Option<ResumeLimit>,
    completion: Option<Rc<LocalLuauTaskCompletion>>,
}

impl TaskHandle {
    pub fn id(self) -> TaskId {
        self.id
    }
}

enum LuauThreadStart {
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
    cancelled_groups: Rc<RefCell<HashSet<TaskGroupId>>>,
    groups: Rc<RefCell<HashMap<TaskGroupId, HashSet<TaskId>>>>,
    thread_tasks: RefCell<HashMap<(u64, usize), TaskHandle>>,
    parked_threads: RefCell<HashSet<(u64, usize)>>,
    pending_luau_work: RefCell<Vec<PendingLuauWork>>,
    pending_luau_cancellations: RefCell<Vec<luau::Thread>>,
    pending_futures: RefCell<Vec<(CallContext, TaskFuture, bool)>>,
    cancelling: Rc<RefCell<HashSet<TaskId>>>,
    /// Work the scheduler took off its tasks while borrowed, see [`Self::drop_detached`].
    detached: Rc<RefCell<Vec<Detached>>>,
    pending_group_cancellations: RefCell<Vec<TaskGroupId>>,
    /// Told once the poll under way has ended and what it deferred is done.
    poll_end: RefCell<Vec<tokio::sync::oneshot::Sender<()>>>,
    polling: Cell<bool>,
    threads: Rc<RefCell<HashMap<TaskId, luau::Thread>>>,
    timers: Rc<Timers>,
    last_group_id: Rc<Cell<u64>>,
    /// Kept apart from the scheduler, which is borrowed while it polls.
    wake: SchedulerWake,
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
        let mut state = State::with_clock(clock);
        let detached = Rc::<RefCell<Vec<Detached>>>::default();
        state.detached = Some(detached.clone());
        Self {
            detached,
            cancelled_groups: state.cancelled_groups.clone(),
            groups: state.groups.clone(),
            last_group_id: state.last_group_id.clone(),
            wake: state.wake_handle(),
            timers: state.timers.clone(),
            cancelling: state.cancelling.clone(),
            threads: state.threads.clone(),
            state: RefCell::new(state),
            thread_tasks: RefCell::default(),
            parked_threads: RefCell::default(),
            pending_luau_work: RefCell::default(),
            pending_luau_cancellations: RefCell::default(),
            pending_futures: RefCell::default(),
            pending_group_cancellations: RefCell::default(),
            poll_end: RefCell::default(),
            polling: Cell::default(),
        }
    }

    /// The time by the scheduler's clock, which every deadline given to it is read against.
    /// Usable while the scheduler polls, as from a task.
    pub fn now(&self) -> Instant {
        self.timers.now()
    }

    /// Fails on a manual clock, for a caller about to block on this scheduler.
    pub(crate) fn ensure_can_wait(&self) -> Result<()> {
        self.timers.clock().ensure_can_wait()
    }

    pub fn create_group(&self) -> TaskGroupId {
        State::next_group(&self.last_group_id)
    }

    /// Cancels every pending task in `group`, and any task later spawned into it, and closes
    /// their threads at once, as [`Self::cancel_luau_thread`] closes one. The default group is
    /// never cancelled. While the scheduler polls, as from a task, the group's tasks run no
    /// further and are cancelled once the poll ends.
    pub fn cancel_group(&self, group: TaskGroupId) {
        if self.polling.get() {
            if group != TaskGroupId::default() {
                self.cancelled_groups.borrow_mut().insert(group);
                self.pending_group_cancellations.borrow_mut().push(group);
                self.close_group(group);
            }
            return;
        }
        self.state.borrow_mut().cancel_group_tasks(group);
        if group != TaskGroupId::default() {
            // Work a poll put off and never took up, as when it unwound, goes with the group.
            self.cancel_pending_luau_work(|work| work.is_in(group));
            let (cancelled, kept) = std::mem::take(&mut *self.pending_futures.borrow_mut())
                .into_iter()
                .partition::<Vec<_>, _>(|(context, ..)| context.task_group == group);
            *self.pending_futures.borrow_mut() = kept;
            for (context, future, _) in cancelled {
                drop_catching_panic(&context, future);
            }
        }
        self.drop_detached();
        self.flush_pending_luau_cancellations();
    }

    /// Closes the threads of `group`'s pending tasks and of the work a poll has put off for it,
    /// whoever has the scheduler borrowed.
    fn close_group(&self, group: TaskGroupId) {
        let mut threads: Vec<_> = {
            let (groups, threads) = (self.groups.borrow(), self.threads.borrow());
            groups
                .get(&group)
                .into_iter()
                .flatten()
                .filter_map(|id| threads.get(id).cloned())
                .collect()
        };
        threads.extend(
            self.pending_luau_work
                .borrow()
                .iter()
                .filter(|work| work.is_in(group))
                .map(|work| work.thread().clone()),
        );
        for thread in threads {
            close_cancelled_thread(&thread);
        }
    }

    /// Runs `future` as a task of `context`'s group. While the scheduler polls, as from another
    /// task, the task starts once the poll ends.
    pub fn schedule_future(
        &self,
        context: CallContext,
        future: impl Future<Output = Result<()>> + 'static,
    ) {
        self.schedule_boxed(context, Box::pin(future), false);
    }

    /// [`Self::schedule_future`] for a task that watches others: every poll runs the watchers
    /// first, see [`Self::poll_ready`], so a thread one cancels on waking isn't resumed again.
    pub fn schedule_watcher(
        &self,
        context: CallContext,
        future: impl Future<Output = Result<()>> + 'static,
    ) {
        self.schedule_boxed(context, Box::pin(future), true);
    }

    fn schedule_boxed(&self, context: CallContext, future: TaskFuture, watcher: bool) {
        if self.polling.get() {
            self.pending_futures
                .borrow_mut()
                .push((context, future, watcher));
            return;
        }
        {
            let mut state = self.state.borrow_mut();
            if watcher {
                state.spawn_boxed_watcher(context, future);
            } else {
                state.spawn_boxed(context, future);
            }
        }
        // A task spawned into a cancelled group is cancelled at once.
        self.drop_detached();
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
        self.schedule_luau_thread_start(context, thread, args, LuauThreadStart::Immediate);
    }

    /// Runs `thread` under the scheduler's own resume budget, reporting to `completion`.
    pub fn schedule_luau_thread_with_completion(
        &self,
        context: CallContext,
        thread: luau::Thread,
        args: Vec<luau::Value>,
        completion: Rc<LocalLuauTaskCompletion>,
    ) {
        self.schedule_luau_thread_start(
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
        self.schedule_luau_thread_start(
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
        self.schedule_luau_thread_start(context, thread.clone(), args, start);
        WatchedThread::new(Rc::downgrade(self), self.timers.clone(), thread, completion)
    }

    fn schedule_luau_thread_start(
        &self,
        context: CallContext,
        thread: luau::Thread,
        args: Vec<luau::Value>,
        start: LuauThreadStart,
    ) {
        if self.polling.get() {
            self.pending_luau_work
                .borrow_mut()
                .push(PendingLuauWork::Thread {
                    context,
                    thread,
                    args,
                    start,
                });
            return;
        }

        self.spawn_luau_thread_start(context, thread, args, start);
    }

    fn spawn_luau_thread_start(
        &self,
        context: CallContext,
        thread: luau::Thread,
        args: Vec<luau::Value>,
        start: LuauThreadStart,
    ) -> TaskHandle {
        let key = luau_thread_key(&thread);
        let wake_at = match &start {
            LuauThreadStart::Delayed(delay) => self.delay_until(Some(*delay)),
            LuauThreadStart::Immediate
            | LuauThreadStart::Limited { .. }
            | LuauThreadStart::WithCompletion(_) => None,
        };
        let args = self.prepare_luau_thread_start_args(key, &thread, args, wake_at);
        let handle = match start {
            LuauThreadStart::Immediate => {
                self.state
                    .borrow_mut()
                    .spawn_luau_thread(context, thread.clone(), args)
            }
            LuauThreadStart::Delayed(delay) => self.state.borrow_mut().spawn_luau_thread_after(
                context,
                delay,
                thread.clone(),
                args,
            ),
            LuauThreadStart::WithCompletion(completion) => {
                let options = LuauTaskOptions {
                    completion: Some(completion),
                    ..self.state.borrow().default_luau_task_options()
                };
                self.state.borrow_mut().spawn_luau_thread_with_options(
                    context,
                    thread.clone(),
                    args,
                    options,
                )
            }
            LuauThreadStart::Limited { limit, completion } => {
                self.state.borrow_mut().spawn_luau_thread_with_options(
                    context,
                    thread.clone(),
                    args,
                    LuauTaskOptions {
                        resume_limit: Some(limit),
                        completion: Some(completion),
                    },
                )
            }
        };
        self.thread_tasks.borrow_mut().insert(key, handle);
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
        self.schedule_luau_thread_start(context, thread, args, LuauThreadStart::Delayed(delay));
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
        let key = (thread.vm_id(), thread.state_id());
        let target = self.thread_tasks.borrow().get(&key).copied();
        if self.polling.get() {
            self.pending_luau_work
                .borrow_mut()
                .push(PendingLuauWork::Future {
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
        // Luau code, not the scheduler, resumed the thread that parked, so
        // no resume will take its park; the new task starts it fresh.
        self.take_parked_luau_thread(&thread);
        let context = thread.call_context().ok().flatten().unwrap_or_default();
        let handle = self.state.borrow_mut().spawn_luau_future(
            context,
            awaiting,
            thread.clone(),
            future,
            wake_at,
        );
        self.thread_tasks
            .borrow_mut()
            .insert(luau_thread_key(&thread), handle);
    }

    /// Whether `group`'s run has ended; readable while the scheduler polls.
    pub fn is_group_cancelled(&self, group: TaskGroupId) -> bool {
        self.cancelled_groups.borrow().contains(&group)
    }

    pub fn cancel(&self, id: TaskId) -> bool {
        let cancelled = { self.state.borrow_mut().cancel(id) };
        self.drop_detached();
        self.flush_pending_luau_cancellations();
        cancelled
    }

    /// Cancels `thread`'s task, and closes the thread at once: Luau code holding it can't resume
    /// it from here on. While the scheduler polls, as from a task, the task is cancelled once the
    /// scheduler is free: before any other task runs when a watcher asks, and at the end of the
    /// poll otherwise. A thread that is running, or has resumed another, goes on to where it next
    /// yields, and is closed there.
    pub fn cancel_luau_thread(&self, thread: &luau::Thread) -> bool {
        let key = luau_thread_key(thread);
        let scheduled = self.close_scheduled(thread);
        if self.polling.get() {
            if let Some(handle) = self.thread_tasks.borrow().get(&key) {
                self.cancelling.borrow_mut().insert(handle.id());
            }
            self.pending_luau_cancellations
                .borrow_mut()
                .push(thread.clone());
            return scheduled;
        }
        // Work a poll put off and never took up, as when it unwound, goes with the task.
        let pending = self.cancel_pending_luau_work(|work| luau_thread_key(work.thread()) == key);
        let Some(handle) = self.thread_tasks.borrow_mut().remove(&key) else {
            return pending;
        };
        self.cancel(handle.id()) || pending
    }

    /// Closes `thread` if the scheduler holds work for it, whoever has the scheduler borrowed, and
    /// returns whether it does.
    fn close_scheduled(&self, thread: &luau::Thread) -> bool {
        let key = luau_thread_key(thread);
        let task = self.thread_tasks.borrow().get(&key).copied();
        let held = task.is_some_and(|handle| self.threads.borrow().contains_key(&handle.id()))
            || self
                .pending_luau_work
                .borrow()
                .iter()
                .map(PendingLuauWork::thread)
                .any(|thread| luau_thread_key(thread) == key);
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
        if self.polling.get()
            && let Some(handle) = self.thread_tasks.borrow().get(&luau_thread_key(&thread))
        {
            self.cancelling.borrow_mut().insert(handle.id());
        }
        self.pending_luau_cancellations.borrow_mut().push(thread);
    }

    pub fn remove(&self, id: TaskId) -> bool {
        self.thread_tasks
            .borrow_mut()
            .retain(|_, handle| handle.id() != id);
        let removed = { self.state.borrow_mut().remove(id) };
        self.drop_detached();
        self.flush_pending_luau_cancellations();
        removed
    }

    pub fn park_luau_thread(&self, thread: &luau::Thread) {
        self.parked_threads
            .borrow_mut()
            .insert((thread.vm_id(), thread.state_id()));
    }

    pub fn luau_thread_handle(&self, thread: &luau::Thread) -> Option<TaskHandle> {
        self.thread_tasks
            .borrow()
            .get(&(thread.vm_id(), thread.state_id()))
            .copied()
    }

    pub fn take_luau_thread_output(&self, thread: &luau::Thread) -> Option<Vec<luau::Value>> {
        let handle = self.luau_thread_handle(thread)?;
        self.state
            .borrow_mut()
            .tasks
            .get_mut(&handle.id())
            .and_then(|task| task.output.take())
    }

    pub(super) fn take_parked_luau_thread(&self, thread: &luau::Thread) -> bool {
        self.parked_threads
            .borrow_mut()
            .remove(&luau_thread_key(thread))
    }

    /// Runs a round: every task ready as it begins, once. The watchers run first, and what they
    /// asked for is done before any other task runs: a thread or a group one of them cancelled is
    /// dropped, along with whatever its work held, such as the threads of the calls it was waiting
    /// on. A task started or woken during the round first runs in the next one.
    pub fn poll_ready(&self) -> usize {
        let watchers = {
            let _polling = Polling::begin(&self.polling);
            let mut state = self.state.borrow_mut();
            state.begin_round();
            state.poll_queue(true)
        };
        self.end_poll();
        let others = {
            let _polling = Polling::begin(&self.polling);
            self.state.borrow_mut().poll_queue(false)
        };
        self.end_poll();
        watchers + others
    }

    /// Drops the work the scheduler took off its tasks, now that the scheduler isn't borrowed:
    /// a destructor is free to call it, and one that panics fails nothing else.
    fn drop_detached(&self) {
        loop {
            let detached = std::mem::take(&mut *self.detached.borrow_mut());
            if detached.is_empty() {
                break;
            }
            drop(detached);
        }
    }

    /// Does what was asked of the scheduler while it was borrowed for polling.
    fn end_poll(&self) {
        self.flush_pending_luau_threads();
        self.flush_pending_futures();
        self.flush_pending_group_cancellations();
        self.flush_pending_luau_cancellations();
        self.drop_detached();
        self.cancelling.borrow_mut().clear();
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

    pub fn next_wake_delay(&self) -> Option<Duration> {
        self.state.borrow().next_wake_delay()
    }

    pub fn wait_for_wake(&self, timeout: Option<Duration>) {
        self.state.borrow().wait_for_wake(timeout);
    }

    /// Usable while the scheduler polls, as from a host call.
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
        let state = self.state.borrow();
        let now = self.now();
        let due = if state.has_pending() {
            state.next_wake_delay()
        } else {
            state.timers.next_delay(now)
        };
        let until_deadline = deadline.map(|deadline| deadline.saturating_duration_since(now));
        let timeout = match (due, until_deadline) {
            (Some(due), Some(until_deadline)) => Some(due.min(until_deadline)),
            (due, until_deadline) => due.or(until_deadline),
        };
        state.wait_for_wake(timeout);
    }

    pub fn remove_finished(&self) -> usize {
        let finished_ids = self
            .state
            .borrow()
            .snapshots()
            .into_iter()
            .filter_map(|snapshot| (snapshot.state != TaskState::Pending).then_some(snapshot.id))
            .collect::<HashSet<_>>();
        if finished_ids.is_empty() {
            return 0;
        }

        self.thread_tasks
            .borrow_mut()
            .retain(|_, handle| !finished_ids.contains(&handle.id()));
        let removed = { self.state.borrow_mut().remove_finished() };
        self.flush_pending_luau_cancellations();
        removed
    }

    fn prepare_luau_thread_start_args(
        &self,
        key: (u64, usize),
        thread: &luau::Thread,
        mut args: Vec<luau::Value>,
        wake_at: Option<Instant>,
    ) -> Vec<luau::Value> {
        if thread.is_started() {
            return args;
        }

        let Some(handle) = self.thread_tasks.borrow().get(&key).copied() else {
            return args;
        };
        let Some(mut merged_args) = self
            .state
            .borrow_mut()
            .take_pending_luau_thread_start(handle.id(), wake_at)
        else {
            return args;
        };

        self.thread_tasks.borrow_mut().remove(&key);
        merged_args.append(&mut args);
        merged_args
    }

    /// Cancels the work a poll put off that `matches`, as its tasks would be: closes its threads,
    /// tells a start's completion it was cancelled and drops the work, and returns whether there
    /// was any.
    fn cancel_pending_luau_work(&self, matches: impl Fn(&PendingLuauWork) -> bool) -> bool {
        let (cancelled, kept) = std::mem::take(&mut *self.pending_luau_work.borrow_mut())
            .into_iter()
            .partition::<Vec<_>, _>(matches);
        *self.pending_luau_work.borrow_mut() = kept;
        // Closed before the work is dropped, as a task's is.
        for work in &cancelled {
            close_cancelled_thread(work.thread());
        }
        let any = !cancelled.is_empty();
        let now = self.now();
        for work in cancelled {
            let context = match &work {
                PendingLuauWork::Thread { context, start, .. } => {
                    if let LuauThreadStart::WithCompletion(completion)
                    | LuauThreadStart::Limited { completion, .. } = start
                    {
                        completion.complete(Err("Luau task was cancelled".into()), now);
                    }
                    context.clone()
                }
                PendingLuauWork::Future { context, .. } => context.clone(),
            };
            drop_catching_panic(&context, work);
        }
        any
    }

    pub(super) fn flush_pending_luau_threads(&self) {
        let pending = std::mem::take(&mut *self.pending_luau_work.borrow_mut());
        for pending in pending {
            match pending {
                PendingLuauWork::Thread {
                    context,
                    thread,
                    args,
                    start,
                } => {
                    self.spawn_luau_thread_start(context, thread, args, start);
                }
                PendingLuauWork::Future {
                    target,
                    context,
                    delay,
                    thread,
                    future,
                } => {
                    let wake_at = self.delay_until(delay);
                    // A task that is not parked, such as a finished one that
                    // borrowed a coroutine, no longer carries the thread.
                    let target = target.filter(|handle| self.state.borrow().is_parked(handle.id()));
                    let Some(handle) = target else {
                        self.spawn_untracked_luau_future(
                            context.task_group,
                            wake_at,
                            thread,
                            future,
                        );
                        continue;
                    };
                    let replaced = self.state.borrow_mut().replace_with_luau_future(
                        handle.id(),
                        context.task_group,
                        thread.clone(),
                        future,
                        wake_at,
                    );
                    if replaced {
                        self.thread_tasks
                            .borrow_mut()
                            .insert(luau_thread_key(&thread), handle);
                    } else {
                        // The parked thread has nothing left to resume it.
                        self.cancel(handle.id());
                    }
                }
            }
        }
    }

    fn flush_pending_futures(&self) {
        let pending = std::mem::take(&mut *self.pending_futures.borrow_mut());
        for (context, future, watcher) in pending {
            self.schedule_boxed(context, future, watcher);
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

/// Marks a [`Scheduler`] as polling until dropped, so that a poll that unwinds doesn't leave
/// it putting off whatever it is asked from then on.
pub(super) struct Polling<'a>(&'a Cell<bool>);

impl<'a> Polling<'a> {
    pub(super) fn begin(polling: &'a Cell<bool>) -> Self {
        polling.set(true);
        Self(polling)
    }
}

impl Drop for Polling<'_> {
    fn drop(&mut self) {
        self.0.set(false);
    }
}

fn luau_thread_key(thread: &luau::Thread) -> (u64, usize) {
    (thread.vm_id(), thread.state_id())
}

enum PendingLuauWork {
    Thread {
        context: CallContext,
        thread: luau::Thread,
        args: Vec<luau::Value>,
        start: LuauThreadStart,
    },
    Future {
        target: Option<TaskHandle>,
        context: CallContext,
        delay: Option<Duration>,
        thread: luau::Thread,
        future: luau::ScheduledFuture,
    },
}

impl PendingLuauWork {
    fn thread(&self) -> &luau::Thread {
        match self {
            Self::Thread { thread, .. } | Self::Future { thread, .. } => thread,
        }
    }

    /// Whether the task this becomes belongs to `group`: as its own, or as that of the run
    /// whose future it awaits.
    fn is_in(&self, group: TaskGroupId) -> bool {
        match self {
            Self::Thread { context, .. } => context.task_group == group,
            Self::Future {
                context, thread, ..
            } => {
                context.task_group == group
                    || thread
                        .call_context()
                        .ok()
                        .flatten()
                        .is_some_and(|own| own.task_group == group)
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;
