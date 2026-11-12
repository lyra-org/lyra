use std::{
    collections::{
        HashMap,
        HashSet,
        VecDeque,
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
mod local;
mod time;
mod wake;
mod watch;
mod work;

pub use limit::{
    ResumeBound,
    ResumeLimit,
};
pub use local::LocalScheduler;
pub(crate) use time::Clock;
pub use time::Sleep;
use time::{
    TimerId,
    Timers,
};
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
    Detached,
    Resuming,
    TaskWork,
    WorkPoll,
    close_cancelled_thread,
    detach,
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

struct LuauTaskOptions {
    resume_limit: Option<ResumeLimit>,
    completion: Option<Rc<LocalLuauTaskCompletion>>,
}

impl TaskHandle {
    pub fn id(self) -> TaskId {
        self.id
    }
}

pub struct Scheduler {
    next_task_id: u64,
    /// Shared with [`LocalScheduler`], which creates groups while this
    /// scheduler is borrowed for polling.
    last_group_id: Rc<Cell<u64>>,
    ready: VecDeque<TaskId>,
    /// The ready tasks that watch others, which a poll runs first.
    ready_watchers: VecDeque<TaskId>,
    timers: Rc<Timers>,
    luau_resume_budget: Option<Duration>,
    wake_queue: Arc<WakeQueue>,
    tasks: HashMap<TaskId, Task>,
    /// Shared with [`LocalScheduler`], which reads it while this scheduler is borrowed for
    /// polling.
    groups: Rc<RefCell<HashMap<TaskGroupId, HashSet<TaskId>>>>,
    /// Shared with [`LocalScheduler`], which reads it while this scheduler
    /// is borrowed for polling.
    cancelled_groups: Rc<RefCell<HashSet<TaskGroupId>>>,
    /// Tasks whose cancellation waits for the poll under way to end, and which it runs no further.
    /// Shared with [`LocalScheduler`], which fills it while this scheduler is borrowed for polling.
    cancelling: Rc<RefCell<HashSet<TaskId>>>,
    /// Where a [`LocalScheduler`] has this scheduler leave the work it takes off its tasks, to
    /// drop it once this scheduler is no longer borrowed.
    detached: Option<Rc<RefCell<Vec<Detached>>>>,
    /// The Luau thread of every pending task that has one, whatever its work is at the moment:
    /// what cancelling the task closes. Shared with [`LocalScheduler`], which reads it while this
    /// scheduler is borrowed for polling.
    threads: Rc<RefCell<HashMap<TaskId, luau::Thread>>>,
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl Scheduler {
    pub fn new() -> Self {
        Self::with_clock(Clock::Real)
    }

    /// A scheduler whose time is `clock`'s, for a test that polls it: it never blocks, so
    /// [`Self::wait_for_wake`] panics.
    #[cfg(any(test, feature = "testing"))]
    pub fn with_manual_clock(clock: crate::testing::ManualClock) -> Self {
        Self::with_clock(Clock::Manual(clock))
    }

    fn with_clock(clock: Clock) -> Self {
        Self {
            next_task_id: 1,
            last_group_id: Rc::default(),
            ready: VecDeque::new(),
            ready_watchers: VecDeque::new(),
            timers: Rc::new(Timers::new(clock)),
            luau_resume_budget: None,
            wake_queue: Arc::new(WakeQueue::default()),
            tasks: HashMap::new(),
            groups: Rc::default(),
            cancelled_groups: Rc::default(),
            cancelling: Rc::default(),
            detached: None,
            threads: Rc::default(),
        }
    }

    /// The time by the scheduler's clock, which every deadline given to it is read against.
    pub fn now(&self) -> Instant {
        self.timers.now()
    }

    /// `delay` from now, or now when that is past the last instant.
    fn wake_after(&self, delay: Duration) -> Instant {
        let now = self.now();
        now.checked_add(delay).unwrap_or(now)
    }

    pub fn set_luau_resume_budget(&mut self, budget: Option<Duration>) {
        self.luau_resume_budget = budget;
    }

    pub fn create_group(&self) -> TaskGroupId {
        Self::next_group(&self.last_group_id)
    }

    fn next_group(last_group_id: &Cell<u64>) -> TaskGroupId {
        let id = last_group_id.get().saturating_add(1);
        last_group_id.set(id);
        TaskGroupId(id)
    }

    pub fn spawn(
        &mut self,
        context: CallContext,
        future: impl Future<Output = Result<()>> + 'static,
    ) -> TaskHandle {
        self.spawn_boxed(context, Box::pin(future))
    }

    pub fn spawn_boxed(&mut self, context: CallContext, future: TaskFuture) -> TaskHandle {
        self.insert_task(context, future, None)
    }

    /// Spawns a task that every poll runs ahead of the ready tasks that aren't watchers. Only
    /// [`LocalScheduler::poll_ready`] also does what the watchers asked before the others run.
    fn spawn_boxed_watcher(&mut self, context: CallContext, future: TaskFuture) -> TaskHandle {
        self.insert_work(
            context,
            TaskWork::Future(future),
            None,
            self.default_luau_task_options(),
            true,
        )
    }

    pub fn spawn_after(
        &mut self,
        context: CallContext,
        delay: Duration,
        future: impl Future<Output = Result<()>> + 'static,
    ) -> TaskHandle {
        self.spawn_boxed_after(context, delay, Box::pin(future))
    }

    pub fn spawn_boxed_after(
        &mut self,
        context: CallContext,
        delay: Duration,
        future: TaskFuture,
    ) -> TaskHandle {
        let wake_at = self.wake_after(delay);
        self.insert_task(context, future, Some(wake_at))
    }

    pub fn spawn_luau_function(
        &mut self,
        context: CallContext,
        function: &luau::Function,
        args: Vec<luau::Value>,
    ) -> luau::runtime::Result<TaskHandle> {
        let thread = function.vm().create_thread(function)?;
        Ok(self.spawn_luau_thread(context, thread, args))
    }

    pub fn spawn_luau_thread(
        &mut self,
        context: CallContext,
        thread: luau::Thread,
        args: Vec<luau::Value>,
    ) -> TaskHandle {
        self.spawn_luau_thread_with_options(context, thread, args, self.default_luau_task_options())
    }

    fn spawn_luau_thread_with_options(
        &mut self,
        context: CallContext,
        thread: luau::Thread,
        args: Vec<luau::Value>,
        options: LuauTaskOptions,
    ) -> TaskHandle {
        let pending_start = !thread.is_started();
        self.insert_work(
            context,
            TaskWork::LuauThread {
                thread,
                args,
                pending_start,
            },
            None,
            options,
            false,
        )
    }

    pub fn spawn_luau_thread_after(
        &mut self,
        context: CallContext,
        delay: Duration,
        thread: luau::Thread,
        args: Vec<luau::Value>,
    ) -> TaskHandle {
        let wake_at = self.wake_after(delay);
        let pending_start = !thread.is_started();
        self.insert_work(
            context,
            TaskWork::LuauThread {
                thread,
                args,
                pending_start,
            },
            Some(wake_at),
            self.default_luau_task_options(),
            false,
        )
    }

    /// Runs `future` for `thread`, which acts as `context`, while the task
    /// also belongs to `awaiting`, the group of the run the future acts for.
    pub fn spawn_luau_future(
        &mut self,
        context: CallContext,
        awaiting: TaskGroupId,
        thread: luau::Thread,
        future: luau::ScheduledFuture,
        wake_at: Option<Instant>,
    ) -> TaskHandle {
        let handle = self.insert_work(
            context,
            TaskWork::LuauFuture { thread, future },
            wake_at,
            self.default_luau_task_options(),
            false,
        );
        if let Some(task) = self.tasks.get_mut(&handle.id()) {
            task.borrows_thread = true;
        }
        self.await_group(handle.id(), awaiting);
        handle
    }

    /// The task keeps its thread's context and also joins `awaiting` while
    /// the future runs. Fails, leaving the task alone, unless the task is
    /// parked and both groups' runs are live.
    fn replace_with_luau_future(
        &mut self,
        id: TaskId,
        awaiting: TaskGroupId,
        thread: luau::Thread,
        future: luau::ScheduledFuture,
        wake_at: Option<Instant>,
    ) -> bool {
        if !self.is_parked(id) {
            return false;
        }
        let context = &self.tasks[&id].context;
        if self.is_cancelled(context.task_group) || self.is_cancelled(awaiting) {
            // Dropped once the caller has cancelled the task, which closes the thread first.
            detach(
                &self.detached,
                context,
                Some(TaskWork::LuauFuture { thread, future }),
            );
            return false;
        }
        self.await_group(id, awaiting);
        let task = self.tasks.get_mut(&id).expect("task checked above");
        task.work = Some(TaskWork::LuauFuture { thread, future });
        task.state = TaskState::Pending;
        task.error = None;
        task.output = None;
        task.wake_at = wake_at;
        if let Some(wake_at) = wake_at {
            self.wake_task_at(id, wake_at);
        } else {
            self.ready.push_back(id);
        }
        true
    }

    fn insert_task(
        &mut self,
        context: CallContext,
        future: TaskFuture,
        wake_at: Option<Instant>,
    ) -> TaskHandle {
        self.insert_work(
            context,
            TaskWork::Future(future),
            wake_at,
            self.default_luau_task_options(),
            false,
        )
    }

    fn insert_work(
        &mut self,
        context: CallContext,
        work: TaskWork,
        wake_at: Option<Instant>,
        options: LuauTaskOptions,
        watcher: bool,
    ) -> TaskHandle {
        let id = TaskId(self.next_task_id);
        self.next_task_id = self.next_task_id.saturating_add(1);
        let group = context.task_group;
        let handle = TaskHandle { id };

        if let TaskWork::LuauThread { thread, .. } | TaskWork::LuauFuture { thread, .. } = &work {
            self.threads.borrow_mut().insert(id, thread.clone());
        }
        self.groups
            .borrow_mut()
            .entry(group)
            .or_default()
            .insert(id);
        self.tasks.insert(
            id,
            Task {
                awaiting: None,
                borrows_thread: false,
                watcher,
                context,
                work: Some(work),
                state: TaskState::Pending,
                error: None,
                output: None,
                wake_at,
                resume_limit: options.resume_limit,
                completion: options.completion,
            },
        );
        if self.is_cancelled(group) {
            self.cancel(id);
        } else if let Some(wake_at) = wake_at {
            self.wake_task_at(id, wake_at);
        } else if watcher {
            self.ready_watchers.push_back(id);
        } else {
            self.ready.push_back(id);
        }

        handle
    }

    fn default_luau_task_options(&self) -> LuauTaskOptions {
        LuauTaskOptions {
            resume_limit: self.luau_resume_budget.map(ResumeLimit::budget),
            completion: None,
        }
    }

    fn take_pending_luau_thread_start(
        &mut self,
        id: TaskId,
        wake_at: Option<Instant>,
    ) -> Option<Vec<luau::Value>> {
        let task = self.tasks.get_mut(&id)?;
        if task.state != TaskState::Pending || !start_wake_is_not_later(wake_at, task.wake_at) {
            return None;
        }

        let Some(TaskWork::LuauThread {
            thread,
            args,
            pending_start,
            ..
        }) = task.work.as_mut()
        else {
            return None;
        };
        if !*pending_start || thread.is_started() {
            return None;
        }

        let args = std::mem::take(args);
        detach(&self.detached, &task.context, task.work.take());
        // Not closed: the thread starts again as another task.
        settle(&self.threads, id, task, TaskState::Cancelled);
        self.timers.remove(TimerId::Task(id));
        Some(args)
    }

    /// Cancels a pending task and closes its Luau thread, a coroutine the task only borrowed
    /// included, so that code still holding the thread can't resume it either.
    pub fn cancel(&mut self, id: TaskId) -> bool {
        let Some(task) = self.tasks.get_mut(&id) else {
            return false;
        };
        if task.state != TaskState::Pending {
            return false;
        }

        // Before the work is dropped, whose destructors may run code that holds the thread.
        if let Some(thread) = settle(&self.threads, id, task, TaskState::Cancelled) {
            close_cancelled_thread(&thread);
        }
        detach(&self.detached, &task.context, task.work.take());
        if let Some(completion) = task.completion.as_ref() {
            completion.complete(
                Err(format!("Luau task {} was cancelled", id.0).into()),
                self.timers.now(),
            );
        }
        self.timers.remove(TimerId::Task(id));
        true
    }

    /// Cancels every pending task in `group`, and any task later spawned
    /// into it. The default group is never cancelled.
    pub fn cancel_group(&mut self, group: TaskGroupId) -> usize {
        self.cancel_group_tasks(group).len()
    }

    fn cancel_group_tasks(&mut self, group: TaskGroupId) -> Vec<TaskId> {
        if group == TaskGroupId::default() {
            return Vec::new();
        }
        self.cancelled_groups.borrow_mut().insert(group);
        let ids: Vec<_> = self
            .groups
            .borrow()
            .get(&group)
            .into_iter()
            .flatten()
            .copied()
            .collect();
        ids.into_iter().filter(|id| self.cancel(*id)).collect()
    }

    pub fn poll_ready(&mut self) -> usize {
        self.wake(self.now());
        self.poll_queue(true) + self.poll_queue(false)
    }

    /// Makes ready the tasks that were woken or are due at `now`.
    fn wake(&mut self, now: Instant) {
        self.timers.wake_due(now);
        self.drain_wake_queue();
    }

    /// Makes task `id` ready once the clock reaches `wake_at`.
    fn wake_task_at(&self, id: TaskId, wake_at: Instant) {
        self.timers.set(
            TimerId::Task(id),
            wake_at,
            &TaskWaker::of(id, &self.wake_queue),
        );
    }

    /// Polls the ready watchers, or the other ready tasks, once each.
    fn poll_queue(&mut self, watchers: bool) -> usize {
        let mut completed = 0;

        loop {
            let next = if watchers {
                self.ready_watchers.pop_front()
            } else {
                self.ready.pop_front()
            };
            let Some(id) = next else {
                break;
            };
            let Some(task) = self.tasks.get_mut(&id) else {
                continue;
            };
            if task.state != TaskState::Pending {
                continue;
            }
            // It or its group was cancelled during this poll, which cancels the task once the poll
            // ends.
            let groups = (task.context.task_group, task.awaiting);
            let cancel_asked = || {
                self.cancelling.borrow().contains(&id)
                    || std::iter::once(groups.0)
                        .chain(groups.1)
                        .any(|group| self.cancelled_groups.borrow().contains(&group))
            };
            if cancel_asked() {
                continue;
            }
            let Some(work) = task.work.as_mut() else {
                continue;
            };

            let waker = TaskWaker::of(id, &self.wake_queue);
            let mut cx = Context::from_waker(&waker);
            let clock = self.timers.clock();
            let interrupted_by = Cell::new(None);
            let polled = poll_work(
                &task.context,
                work,
                &mut cx,
                &Resuming {
                    cancel_asked: &cancel_asked,
                    limit: task.resume_limit.as_ref(),
                    clock,
                    interrupted_by: &interrupted_by,
                },
            );
            match polled {
                WorkPoll::Completed { output } => {
                    if let Some(completion) = task.completion.as_ref() {
                        completion.complete(Ok(output.unwrap_or_default()), clock.now());
                        task.output = None;
                    } else {
                        task.output = output;
                    }
                    detach(&self.detached, &task.context, task.work.take());
                    settle(&self.threads, id, task, TaskState::Completed);
                    completed += 1;
                }
                // Stays pending, without work, until the parked call's future
                // replaces it or a refused replacement cancels it.
                WorkPoll::Parked => {
                    detach(&self.detached, &task.context, task.work.take());
                    task.wake_at = None;
                    completed += 1;
                }
                WorkPoll::Cancelled => {
                    if let Some(completion) = task.completion.as_ref() {
                        completion.complete(
                            Err(format!("Luau task {} was cancelled", id.0).into()),
                            clock.now(),
                        );
                    }
                    detach(&self.detached, &task.context, task.work.take());
                    // Not closed: other code started the thread and runs it, or it was closed as
                    // it yielded.
                    settle(&self.threads, id, task, TaskState::Cancelled);
                    completed += 1;
                }
                WorkPoll::Failed(error) => {
                    if let Some(completion) = task.completion.as_ref() {
                        completion.interrupted_by.set(interrupted_by.get());
                        completion.complete(
                            Err(luau::RaisedError {
                                kind: error.kind,
                                message: format!("Luau task {} failed: {}", id.0, error.message),
                            }),
                            clock.now(),
                        );
                    }
                    detach(&self.detached, &task.context, task.work.take());
                    settle(&self.threads, id, task, TaskState::Failed);
                    task.error = Some(Arc::from(error.message));
                    completed += 1;
                }
                WorkPoll::Yielded if task.borrows_thread => {
                    detach(&self.detached, &task.context, task.work.take());
                    settle(&self.threads, id, task, TaskState::Completed);
                    completed += 1;
                }
                WorkPoll::Yielded | WorkPoll::Pending => {}
            }
            if !matches!(task.work, Some(TaskWork::LuauFuture { .. })) {
                self.stop_awaiting(id);
            }
        }

        completed
    }

    fn drain_wake_queue(&mut self) {
        for id in self.wake_queue.drain() {
            match self.tasks.get(&id) {
                Some(task) if task.state != TaskState::Pending => {}
                Some(task) if task.watcher => self.ready_watchers.push_back(id),
                Some(_) => self.ready.push_back(id),
                None => {}
            }
        }
    }

    /// How long, by the scheduler's clock, until it has a task to poll. `None` when no task is
    /// ready or due.
    pub fn next_wake_delay(&self) -> Option<Duration> {
        if !self.ready.is_empty()
            || !self.ready_watchers.is_empty()
            || self.wake_queue.has_pending_wake()
        {
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
        if let Err(error) = self.timers.clock().ensure_can_wait() {
            panic!("{error}");
        }
        self.wake_queue.wait(timeout);
    }

    pub fn wake_handle(&self) -> SchedulerWake {
        SchedulerWake::of(self.wake_queue.clone())
    }

    /// Parked on an async call: still pending, waiting for that call's
    /// future to become its work.
    fn is_parked(&self, id: TaskId) -> bool {
        self.tasks
            .get(&id)
            .is_some_and(|task| task.state == TaskState::Pending && task.work.is_none())
    }

    fn is_cancelled(&self, group: TaskGroupId) -> bool {
        self.cancelled_groups.borrow().contains(&group)
    }

    /// A task belongs to its own group for its whole life, and also to the
    /// group of a future from another run while it awaits one. These two
    /// keep `groups` in step with that, so cancelling either group finds it.
    fn await_group(&mut self, id: TaskId, group: TaskGroupId) {
        let Some(task) = self.tasks.get_mut(&id) else {
            return;
        };
        let awaiting = (group != task.context.task_group).then_some(group);
        let previous = std::mem::replace(&mut task.awaiting, awaiting);
        if previous == awaiting {
            return;
        }
        let mut groups = self.groups.borrow_mut();
        if let Some(previous) = previous
            && let Some(group_tasks) = groups.get_mut(&previous)
        {
            group_tasks.remove(&id);
        }
        if let Some(awaiting) = awaiting {
            groups.entry(awaiting).or_default().insert(id);
            drop(groups);
            if self.is_cancelled(awaiting) {
                self.cancel(id);
            }
        }
    }

    fn stop_awaiting(&mut self, id: TaskId) {
        let Some(task) = self.tasks.get_mut(&id) else {
            return;
        };
        if let Some(previous) = task.awaiting.take()
            && let Some(group_tasks) = self.groups.borrow_mut().get_mut(&previous)
        {
            group_tasks.remove(&id);
        }
    }

    pub fn remove(&mut self, id: TaskId) -> bool {
        let Some(mut task) = self.tasks.remove(&id) else {
            return false;
        };
        self.threads.borrow_mut().remove(&id);
        detach(&self.detached, &task.context, task.work.take());

        for group in std::iter::once(task.context.task_group).chain(task.awaiting) {
            if let Some(group_tasks) = self.groups.borrow_mut().get_mut(&group) {
                group_tasks.remove(&id);
            }
        }
        self.ready.retain(|ready_id| *ready_id != id);
        self.ready_watchers.retain(|ready_id| *ready_id != id);
        self.timers.remove(TimerId::Task(id));
        true
    }

    pub fn snapshot(&self, id: TaskId) -> Option<TaskSnapshot> {
        let task = self.tasks.get(&id)?;
        Some(TaskSnapshot {
            id,
            group: task.context.task_group,
            origin: task.context.origin.clone(),
            state: task.state,
            error: task.error.clone(),
        })
    }

    pub fn group_snapshots(&self, group: TaskGroupId) -> Vec<TaskSnapshot> {
        let groups = self.groups.borrow();
        let Some(tasks) = groups.get(&group) else {
            return Vec::new();
        };

        let mut snapshots: Vec<_> = tasks.iter().filter_map(|id| self.snapshot(*id)).collect();
        snapshots.sort_by_key(|snapshot| snapshot.id.0);
        snapshots
    }

    pub fn snapshots(&self) -> Vec<TaskSnapshot> {
        let mut snapshots: Vec<_> = self
            .tasks
            .keys()
            .filter_map(|id| self.snapshot(*id))
            .collect();
        snapshots.sort_by_key(|snapshot| snapshot.id.0);
        snapshots
    }

    pub fn has_pending(&self) -> bool {
        self.tasks
            .values()
            .any(|task| task.state == TaskState::Pending)
    }

    pub fn remove_finished(&mut self) -> usize {
        let finished: Vec<_> = self
            .tasks
            .iter()
            .filter_map(|(id, task)| (task.state != TaskState::Pending).then_some(*id))
            .collect();

        let mut removed = 0;
        for id in finished {
            if self.remove(id) {
                removed += 1;
            }
        }

        removed
    }
}

fn start_wake_is_not_later(new_wake_at: Option<Instant>, old_wake_at: Option<Instant>) -> bool {
    match (new_wake_at, old_wake_at) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(new_wake_at), Some(old_wake_at)) => new_wake_at <= old_wake_at,
    }
}

/// Takes `task` out of the pending ones as `state`, and returns the thread it had. The thread
/// loses the context the task ran it with, unless the task only borrowed it and the context is
/// the thread's own.
fn settle(
    threads: &RefCell<HashMap<TaskId, luau::Thread>>,
    id: TaskId,
    task: &mut Task,
    state: TaskState,
) -> Option<luau::Thread> {
    task.state = state;
    task.wake_at = None;
    let thread = threads.borrow_mut().remove(&id);
    if let Some(thread) = &thread
        && !task.borrows_thread
    {
        let _ = thread.clear_call_context();
    }
    thread
}

struct Task {
    context: CallContext,
    /// Another run's group whose future the task awaits.
    awaiting: Option<TaskGroupId>,
    /// The task only carries a thread Luau code drives, such as a
    /// coroutine, through one wait; a plain yield hands the thread back.
    borrows_thread: bool,
    /// Polled ahead of the ready tasks that aren't.
    watcher: bool,
    work: Option<TaskWork>,
    state: TaskState,
    error: Option<Arc<str>>,
    output: Option<Vec<luau::Value>>,
    wake_at: Option<Instant>,
    resume_limit: Option<ResumeLimit>,
    completion: Option<Rc<LocalLuauTaskCompletion>>,
}

#[cfg(test)]
pub(crate) mod tests;
