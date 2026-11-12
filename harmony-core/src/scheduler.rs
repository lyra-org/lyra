use std::{
    collections::{
        HashMap,
        HashSet,
        VecDeque,
    },
    future::Future,
    panic::{
        AssertUnwindSafe,
        catch_unwind,
    },
    pin::Pin,
    rc::Rc,
    sync::{
        Arc,
        Condvar,
        Mutex,
        atomic::{
            AtomicBool,
            Ordering,
        },
    },
    task::{
        Context,
        Poll,
        Wake,
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
    fn now(&self) -> Instant {
        match self {
            Self::Real => Instant::now(),
            #[cfg(any(test, feature = "testing"))]
            Self::Manual(clock) => clock.now(),
        }
    }

    /// Fails for a manual clock: a thread blocked on it would wait for time only it can move.
    fn ensure_can_wait(&self) -> Result<()> {
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

#[derive(Default)]
pub struct LocalLuauTaskCompletion {
    result: RefCell<Option<std::result::Result<Vec<luau::Value>, luau::RaisedError>>>,
    ended_at: Cell<Option<Instant>>,
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

    /// The task's result, once it ends or is cancelled, as [`Self::poll`] gives it.
    pub async fn wait(&self) -> std::result::Result<Vec<luau::Value>, luau::RaisedError> {
        std::future::poll_fn(|cx| self.poll(cx)).await
    }
}

/// The deadlines [`Sleep`]s wait for. Shared with [`LocalScheduler`], which hands them out while
/// the scheduler is borrowed for polling.
#[derive(Default)]
struct Timers {
    clock: Clock,
    last_id: Cell<u64>,
    waiting: RefCell<Vec<Timer>>,
}

struct Timer {
    id: u64,
    deadline: Instant,
    waker: Waker,
}

impl Timers {
    fn wake_due(&self, now: Instant) {
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

    fn next_delay(&self, now: Instant) -> Option<Duration> {
        self.waiting
            .borrow()
            .iter()
            .map(|timer| timer.deadline.saturating_duration_since(now))
            .min()
    }
}

/// Completes at its deadline; see [`LocalScheduler::sleep_until`].
pub struct Sleep {
    timers: Rc<Timers>,
    id: u64,
    deadline: Instant,
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.timers.clock.now() >= self.deadline {
            return Poll::Ready(());
        }
        let mut waiting = self.timers.waiting.borrow_mut();
        match waiting.iter_mut().find(|timer| timer.id == self.id) {
            Some(timer) => timer.waker.clone_from(cx.waker()),
            None => waiting.push(Timer {
                id: self.id,
                deadline: self.deadline,
                waker: cx.waker().clone(),
            }),
        }
        Poll::Pending
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        self.timers
            .waiting
            .borrow_mut()
            .retain(|timer| timer.id != self.id);
    }
}

/// How long each resume of a Luau task may run before it is interrupted.
#[derive(Clone, Debug)]
pub enum ResumeLimit {
    /// Each resume may run this long.
    Budget(Duration),
    /// Each resume may run until `deadline`, by the scheduler's clock, or until `signal` is
    /// raised. What is left until the deadline when a resume starts is then counted in real time.
    Until {
        deadline: Instant,
        signal: luau::InterruptSignal,
    },
}

impl ResumeLimit {
    fn guard(&self, vm: &luau::Vm, clock: &Clock) -> luau::InterruptBudgetGuard {
        match self {
            Self::Budget(budget) => vm.interrupt_after(*budget),
            Self::Until { deadline, signal } => vm.interrupt_after_or_on(
                deadline.saturating_duration_since(clock.now()),
                Some(signal),
            ),
        }
    }
}

struct LuauTaskOptions {
    resume_limit: Option<ResumeLimit>,
    completion: Option<Rc<LocalLuauTaskCompletion>>,
}

enum LuauThreadStart {
    Immediate,
    Delayed(Duration),
    Limited {
        limit: ResumeLimit,
        completion: Option<Rc<LocalLuauTaskCompletion>>,
    },
    /// Immediately, under the scheduler's own limit, reporting to the completion.
    WithCompletion(Rc<LocalLuauTaskCompletion>),
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
    sleeping: Vec<(Instant, TaskId)>,
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
    threads: Rc<RefCell<HashMap<TaskId, (luau::Vm, luau::Thread)>>>,
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
            sleeping: Vec::new(),
            timers: Rc::new(Timers {
                clock,
                ..Timers::default()
            }),
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
        self.timers.clock.now()
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
    pub(crate) fn spawn_boxed_watcher(
        &mut self,
        context: CallContext,
        future: TaskFuture,
    ) -> TaskHandle {
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
        vm: luau::Vm,
        function: &luau::Function,
        args: Vec<luau::Value>,
    ) -> luau::runtime::Result<TaskHandle> {
        let thread = vm.create_thread(function)?;
        Ok(self.spawn_luau_thread(context, vm, thread, args))
    }

    pub fn spawn_luau_thread(
        &mut self,
        context: CallContext,
        vm: luau::Vm,
        thread: luau::Thread,
        args: Vec<luau::Value>,
    ) -> TaskHandle {
        self.spawn_luau_thread_with_options(
            context,
            vm,
            thread,
            args,
            self.default_luau_task_options(),
        )
    }

    fn spawn_luau_thread_with_options(
        &mut self,
        context: CallContext,
        vm: luau::Vm,
        thread: luau::Thread,
        args: Vec<luau::Value>,
        options: LuauTaskOptions,
    ) -> TaskHandle {
        let pending_start = !thread.is_started();
        self.insert_work(
            context,
            TaskWork::LuauThread {
                vm,
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
        vm: luau::Vm,
        thread: luau::Thread,
        args: Vec<luau::Value>,
    ) -> TaskHandle {
        let wake_at = self.wake_after(delay);
        let pending_start = !thread.is_started();
        self.insert_work(
            context,
            TaskWork::LuauThread {
                vm,
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
        vm: luau::Vm,
        thread: luau::Thread,
        future: luau::ScheduledFuture,
        wake_at: Option<Instant>,
    ) -> TaskHandle {
        let handle = self.insert_work(
            context,
            TaskWork::LuauFuture { vm, thread, future },
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
        vm: luau::Vm,
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
                Some(TaskWork::LuauFuture { vm, thread, future }),
            );
            return false;
        }
        self.await_group(id, awaiting);
        let task = self.tasks.get_mut(&id).expect("task checked above");
        task.work = Some(TaskWork::LuauFuture { vm, thread, future });
        task.state = TaskState::Pending;
        task.error = None;
        task.output = None;
        task.wake_at = wake_at;
        if let Some(wake_at) = wake_at {
            self.sleeping.push((wake_at, id));
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

        if let TaskWork::LuauThread { vm, thread, .. } | TaskWork::LuauFuture { vm, thread, .. } =
            &work
        {
            self.threads
                .borrow_mut()
                .insert(id, (vm.clone(), thread.clone()));
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
            self.sleeping.push((wake_at, id));
        } else if watcher {
            self.ready_watchers.push_back(id);
        } else {
            self.ready.push_back(id);
        }

        handle
    }

    fn default_luau_task_options(&self) -> LuauTaskOptions {
        LuauTaskOptions {
            resume_limit: self.luau_resume_budget.map(ResumeLimit::Budget),
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
        if let Some((vm, thread)) = settle(&self.threads, id, task, TaskState::Cancelled) {
            close_cancelled_thread(&vm, &thread);
        }
        detach(&self.detached, &task.context, task.work.take());
        if let Some(completion) = task.completion.as_ref() {
            completion.complete(
                Err(format!("Luau task {} was cancelled", id.0).into()),
                self.timers.clock.now(),
            );
        }
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
        self.wake_due(now);
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

            let waker = Waker::from(Arc::new(TaskWaker {
                id,
                queue: self.wake_queue.clone(),
            }));
            let mut cx = Context::from_waker(&waker);
            let clock = &self.timers.clock;
            let polled = poll_work(
                &task.context,
                work,
                &mut cx,
                task.resume_limit.as_ref(),
                clock,
                &Resuming {
                    cancel_asked: &cancel_asked,
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

    fn wake_due(&mut self, now: Instant) {
        let mut pending = Vec::new();
        for (wake_at, id) in self.sleeping.drain(..) {
            if wake_at <= now {
                self.ready.push_back(id);
            } else {
                pending.push((wake_at, id));
            }
        }
        self.sleeping = pending;
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

        let now = self.now();
        self.sleeping
            .iter()
            .map(|(wake_at, _)| wake_at.saturating_duration_since(now))
            .chain(self.timers.next_delay(now))
            .min()
    }

    /// Waits until a task is woken, [`SchedulerWake::wake`] is called, or `timeout` passes.
    /// Returns at once when either happened since the last wait.
    ///
    /// # Panics
    ///
    /// On a manual clock, whose time no wait would see pass.
    pub fn wait_for_wake(&self, timeout: Option<Duration>) {
        if let Err(error) = self.timers.clock.ensure_can_wait() {
            panic!("{error}");
        }
        self.wake_queue.wait(timeout);
    }

    pub fn wake_handle(&self) -> SchedulerWake {
        SchedulerWake {
            waker: Arc::new(OwnerWaker {
                queue: self.wake_queue.clone(),
            }),
        }
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
        self.sleeping.retain(|(_, sleeping_id)| *sleeping_id != id);
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

/// Ends a scheduler's wait from any thread, for work its owner polls that the scheduler doesn't
/// track. A wake between two waits is kept for the next one.
#[derive(Clone)]
pub struct SchedulerWake {
    waker: Arc<OwnerWaker>,
}

impl SchedulerWake {
    pub fn wake(&self) {
        self.waker.queue.rouse();
    }

    /// A waker that calls [`Self::wake`]. Every one of a handle's wakers wakes the same task.
    pub fn waker(&self) -> Waker {
        Waker::from(self.waker.clone())
    }

    /// Whether the scheduler's owner is blocked in a wait right now, for a test of what ends
    /// one: a wake sent once this holds reaches a thread that is asleep, not one about to sleep.
    #[cfg(any(test, feature = "testing"))]
    pub fn owner_is_waiting(&self) -> bool {
        self.waker.queue.state().waiting
    }
}

/// Wakes the scheduler's owner, where a [`TaskWaker`] wakes one of its tasks.
struct OwnerWaker {
    queue: Arc<WakeQueue>,
}

impl Wake for OwnerWaker {
    fn wake(self: Arc<Self>) {
        self.queue.rouse();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.queue.rouse();
    }
}

/// A one-way flag its holders set from any thread, for what the scheduler's owner checks between
/// waits: setting it ends the wait.
#[derive(Clone)]
pub struct Cancellation {
    inner: Arc<CancellationInner>,
    wake: SchedulerWake,
}

#[derive(Default)]
struct CancellationInner {
    cancelled: AtomicBool,
    waiting: tokio::sync::Notify,
}

impl Cancellation {
    pub fn new(wake: SchedulerWake) -> Self {
        Self {
            inner: Arc::default(),
            wake,
        }
    }

    pub fn cancel(&self) {
        self.inner.cancelled.store(true, Ordering::Release);
        self.inner.waiting.notify_waiters();
        self.wake.wake();
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }

    /// Completes once cancelled, for a task that waits on it.
    pub async fn cancelled(&self) {
        let mut waiting = std::pin::pin!(self.inner.waiting.notified());
        waiting.as_mut().enable();
        if !self.is_cancelled() {
            waiting.await;
        }
    }
}

impl std::fmt::Debug for Cancellation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cancellation")
            .field("cancelled", &self.is_cancelled())
            .finish_non_exhaustive()
    }
}

#[derive(Default)]
struct WakeQueue {
    state: Mutex<WakeState>,
    signal: Condvar,
}

#[derive(Default)]
struct WakeState {
    ids: VecDeque<TaskId>,
    /// A [`SchedulerWake`] fired since the last wait.
    roused: bool,
    /// The owner is blocked in [`WakeQueue::wait`].
    #[cfg(any(test, feature = "testing"))]
    waiting: bool,
}

impl WakeQueue {
    fn state(&self) -> std::sync::MutexGuard<'_, WakeState> {
        self.state.lock().expect("scheduler wake queue poisoned")
    }

    fn wake_task(&self, id: TaskId) {
        self.state().ids.push_back(id);
        self.signal.notify_one();
    }

    fn rouse(&self) {
        self.state().roused = true;
        self.signal.notify_one();
    }

    fn drain(&self) -> Vec<TaskId> {
        self.state().ids.drain(..).collect()
    }

    fn has_pending_wake(&self) -> bool {
        !self.state().ids.is_empty()
    }

    fn wait(&self, timeout: Option<Duration>) {
        let mut state = self.state();
        // No time to wait is no wait, which also keeps `waiting` to an owner that is asleep.
        if state.ids.is_empty() && !state.roused && timeout != Some(Duration::ZERO) {
            // Set under the lock the wait releases, so whoever reads it set finds the owner asleep.
            #[cfg(any(test, feature = "testing"))]
            {
                state.waiting = true;
            }
            state = match timeout {
                Some(timeout) => {
                    self.signal
                        .wait_timeout(state, timeout)
                        .expect("scheduler wake queue poisoned")
                        .0
                }
                None => self
                    .signal
                    .wait(state)
                    .expect("scheduler wake queue poisoned"),
            };
            #[cfg(any(test, feature = "testing"))]
            {
                state.waiting = false;
            }
        }
        state.roused = false;
    }
}

struct TaskWaker {
    id: TaskId,
    queue: Arc<WakeQueue>,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.queue.wake_task(self.id);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.queue.wake_task(self.id);
    }
}

pub struct LocalScheduler {
    scheduler: RefCell<Scheduler>,
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
    threads: Rc<RefCell<HashMap<TaskId, (luau::Vm, luau::Thread)>>>,
    timers: Rc<Timers>,
    last_group_id: Rc<Cell<u64>>,
    /// Kept apart from the scheduler, which is borrowed while it polls.
    wake: SchedulerWake,
}

impl Default for LocalScheduler {
    fn default() -> Self {
        Self::with_clock(Clock::Real)
    }
}

impl LocalScheduler {
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
        let mut scheduler = Scheduler::with_clock(clock);
        let detached = Rc::<RefCell<Vec<Detached>>>::default();
        scheduler.detached = Some(detached.clone());
        Self {
            detached,
            cancelled_groups: scheduler.cancelled_groups.clone(),
            groups: scheduler.groups.clone(),
            last_group_id: scheduler.last_group_id.clone(),
            wake: scheduler.wake_handle(),
            timers: scheduler.timers.clone(),
            cancelling: scheduler.cancelling.clone(),
            threads: scheduler.threads.clone(),
            scheduler: RefCell::new(scheduler),
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
        self.timers.clock.now()
    }

    /// Fails on a manual clock, for a caller about to block on this scheduler.
    pub(crate) fn ensure_can_wait(&self) -> Result<()> {
        self.timers.clock.ensure_can_wait()
    }

    pub fn create_group(&self) -> TaskGroupId {
        Scheduler::next_group(&self.last_group_id)
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
        self.scheduler.borrow_mut().cancel_group_tasks(group);
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
                .map(|work| {
                    let (vm, thread) = work.thread();
                    (vm.clone(), thread.clone())
                }),
        );
        for (vm, thread) in threads {
            close_cancelled_thread(&vm, &thread);
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
            let mut scheduler = self.scheduler.borrow_mut();
            if watcher {
                scheduler.spawn_boxed_watcher(context, future);
            } else {
                scheduler.spawn_boxed(context, future);
            }
        }
        // A task spawned into a cancelled group is cancelled at once.
        self.drop_detached();
    }

    /// A future that completes once the scheduler's clock reaches `deadline`, for a task of this
    /// scheduler to wait on: the deadline ends [`Self::wait_for_work`], and the poll that follows
    /// wakes the task.
    pub fn sleep_until(&self, deadline: Instant) -> Sleep {
        let id = self.timers.last_id.get().wrapping_add(1);
        self.timers.last_id.set(id);
        Sleep {
            timers: self.timers.clone(),
            id,
            deadline,
        }
    }

    pub fn set_luau_resume_budget(&self, budget: Option<Duration>) {
        self.scheduler.borrow_mut().set_luau_resume_budget(budget);
    }

    pub fn spawn_luau_thread(
        &self,
        context: CallContext,
        vm: luau::Vm,
        thread: luau::Thread,
        args: Vec<luau::Value>,
    ) -> TaskHandle {
        self.spawn_luau_thread_start(context, vm, thread, args, LuauThreadStart::Immediate)
    }

    pub fn schedule_luau_thread(
        &self,
        context: CallContext,
        vm: luau::Vm,
        thread: luau::Thread,
        args: Vec<luau::Value>,
    ) {
        self.schedule_luau_thread_start(context, vm, thread, args, LuauThreadStart::Immediate);
    }

    pub fn schedule_luau_thread_with_budget(
        &self,
        context: CallContext,
        vm: luau::Vm,
        thread: luau::Thread,
        args: Vec<luau::Value>,
        resume_budget: Duration,
    ) {
        self.schedule_luau_thread_start(
            context,
            vm,
            thread,
            args,
            LuauThreadStart::Limited {
                limit: ResumeLimit::Budget(resume_budget),
                completion: None,
            },
        );
    }

    /// Runs `thread` under the scheduler's own resume budget, reporting to `completion`.
    pub fn schedule_luau_thread_with_completion(
        &self,
        context: CallContext,
        vm: luau::Vm,
        thread: luau::Thread,
        args: Vec<luau::Value>,
        completion: Rc<LocalLuauTaskCompletion>,
    ) {
        self.schedule_luau_thread_start(
            context,
            vm,
            thread,
            args,
            LuauThreadStart::WithCompletion(completion),
        );
    }

    pub fn schedule_luau_thread_with_budget_and_completion(
        &self,
        context: CallContext,
        vm: luau::Vm,
        thread: luau::Thread,
        args: Vec<luau::Value>,
        resume_budget: Duration,
        completion: Rc<LocalLuauTaskCompletion>,
    ) {
        self.schedule_luau_thread_with_limit_and_completion(
            context,
            vm,
            thread,
            args,
            ResumeLimit::Budget(resume_budget),
            completion,
        );
    }

    pub fn schedule_luau_thread_with_limit_and_completion(
        &self,
        context: CallContext,
        vm: luau::Vm,
        thread: luau::Thread,
        args: Vec<luau::Value>,
        limit: ResumeLimit,
        completion: Rc<LocalLuauTaskCompletion>,
    ) {
        self.schedule_luau_thread_start(
            context,
            vm,
            thread,
            args,
            LuauThreadStart::Limited {
                limit,
                completion: Some(completion),
            },
        );
    }

    fn schedule_luau_thread_start(
        &self,
        context: CallContext,
        vm: luau::Vm,
        thread: luau::Thread,
        args: Vec<luau::Value>,
        start: LuauThreadStart,
    ) {
        if self.polling.get() {
            self.pending_luau_work
                .borrow_mut()
                .push(PendingLuauWork::Thread {
                    context,
                    vm,
                    thread,
                    args,
                    start,
                });
            return;
        }

        self.spawn_luau_thread_start(context, vm, thread, args, start);
    }

    fn spawn_luau_thread_start(
        &self,
        context: CallContext,
        vm: luau::Vm,
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
        let handle =
            match start {
                LuauThreadStart::Immediate => self.scheduler.borrow_mut().spawn_luau_thread(
                    context,
                    vm.clone(),
                    thread.clone(),
                    args,
                ),
                LuauThreadStart::Delayed(delay) => self
                    .scheduler
                    .borrow_mut()
                    .spawn_luau_thread_after(context, delay, vm.clone(), thread.clone(), args),
                LuauThreadStart::WithCompletion(completion) => {
                    let options = LuauTaskOptions {
                        completion: Some(completion),
                        ..self.scheduler.borrow().default_luau_task_options()
                    };
                    self.scheduler.borrow_mut().spawn_luau_thread_with_options(
                        context,
                        vm.clone(),
                        thread.clone(),
                        args,
                        options,
                    )
                }
                LuauThreadStart::Limited { limit, completion } => {
                    self.scheduler.borrow_mut().spawn_luau_thread_with_options(
                        context,
                        vm.clone(),
                        thread.clone(),
                        args,
                        LuauTaskOptions {
                            resume_limit: Some(limit),
                            completion,
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
        vm: luau::Vm,
        thread: luau::Thread,
        args: Vec<luau::Value>,
    ) -> TaskHandle {
        self.spawn_luau_thread_start(context, vm, thread, args, LuauThreadStart::Delayed(delay))
    }

    pub fn schedule_luau_thread_after(
        &self,
        context: CallContext,
        delay: Duration,
        vm: luau::Vm,
        thread: luau::Thread,
        args: Vec<luau::Value>,
    ) {
        self.schedule_luau_thread_start(context, vm, thread, args, LuauThreadStart::Delayed(delay));
    }

    pub fn schedule_luau_future(
        &self,
        context: CallContext,
        vm: luau::Vm,
        thread: luau::Thread,
        future: luau::ScheduledFuture,
    ) {
        self.schedule_luau_future_with_delay(context, None, vm, thread, future);
    }

    pub fn schedule_luau_future_after(
        &self,
        context: CallContext,
        delay: Duration,
        vm: luau::Vm,
        thread: luau::Thread,
        future: luau::ScheduledFuture,
    ) {
        self.schedule_luau_future_with_delay(context, Some(delay), vm, thread, future);
    }

    fn schedule_luau_future_with_delay(
        &self,
        context: CallContext,
        delay: Option<Duration>,
        vm: luau::Vm,
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
                    vm,
                    thread,
                    future,
                });
            return;
        }

        self.spawn_untracked_luau_future(
            context.task_group,
            self.delay_until(delay),
            vm,
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
        vm: luau::Vm,
        thread: luau::Thread,
        future: luau::ScheduledFuture,
    ) {
        // Luau code, not the scheduler, resumed the thread that parked, so
        // no resume will take its park; the new task starts it fresh.
        self.take_parked_luau_thread(&thread);
        let context = vm
            .thread_call_context(&thread)
            .ok()
            .flatten()
            .unwrap_or_default();
        let handle = self.scheduler.borrow_mut().spawn_luau_future(
            context,
            awaiting,
            vm.clone(),
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
        let cancelled = { self.scheduler.borrow_mut().cancel(id) };
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
        let pending = self.cancel_pending_luau_work(|work| luau_thread_key(work.thread().1) == key);
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
        let vm = task
            .and_then(|handle| self.threads.borrow().get(&handle.id()).cloned())
            .map(|(vm, _)| vm)
            .or_else(|| {
                self.pending_luau_work
                    .borrow()
                    .iter()
                    .map(PendingLuauWork::thread)
                    .find(|(_, thread)| luau_thread_key(thread) == key)
                    .map(|(vm, _)| vm.clone())
            });
        let Some(vm) = vm else {
            return false;
        };
        close_cancelled_thread(&vm, thread);
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
        let removed = { self.scheduler.borrow_mut().remove(id) };
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
        self.scheduler
            .borrow_mut()
            .tasks
            .get_mut(&handle.id())
            .and_then(|task| task.output.take())
    }

    fn take_parked_luau_thread(&self, thread: &luau::Thread) -> bool {
        self.parked_threads
            .borrow_mut()
            .remove(&luau_thread_key(thread))
    }

    /// Runs every ready task once. The watchers run first, and what they asked for is done before
    /// any other task runs: a thread or a group one of them cancelled is dropped, along with
    /// whatever its work held, such as the threads of the calls it was waiting on.
    pub fn poll_ready(&self) -> usize {
        let watchers = {
            let _polling = Polling::begin(&self.polling);
            let mut scheduler = self.scheduler.borrow_mut();
            scheduler.wake(self.now());
            scheduler.poll_queue(true)
        };
        self.end_poll();
        let others = {
            let _polling = Polling::begin(&self.polling);
            self.scheduler.borrow_mut().poll_queue(false)
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
        self.scheduler.borrow().snapshot(id)
    }

    pub fn snapshots(&self) -> Vec<TaskSnapshot> {
        self.scheduler.borrow().snapshots()
    }

    pub fn group_snapshots(&self, group: TaskGroupId) -> Vec<TaskSnapshot> {
        self.scheduler.borrow().group_snapshots(group)
    }

    pub fn has_pending(&self) -> bool {
        self.scheduler.borrow().has_pending()
    }

    pub fn next_wake_delay(&self) -> Option<Duration> {
        self.scheduler.borrow().next_wake_delay()
    }

    pub fn wait_for_wake(&self, timeout: Option<Duration>) {
        self.scheduler.borrow().wait_for_wake(timeout);
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
        let scheduler = self.scheduler.borrow();
        let now = self.now();
        let due = if scheduler.has_pending() {
            scheduler.next_wake_delay()
        } else {
            scheduler.timers.next_delay(now)
        };
        let until_deadline = deadline.map(|deadline| deadline.saturating_duration_since(now));
        let timeout = match (due, until_deadline) {
            (Some(due), Some(until_deadline)) => Some(due.min(until_deadline)),
            (due, until_deadline) => due.or(until_deadline),
        };
        scheduler.wait_for_wake(timeout);
    }

    pub fn remove_finished(&self) -> usize {
        let finished_ids = self
            .scheduler
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
        let removed = { self.scheduler.borrow_mut().remove_finished() };
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
            .scheduler
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
            let (vm, thread) = work.thread();
            close_cancelled_thread(vm, thread);
        }
        let any = !cancelled.is_empty();
        let now = self.now();
        for work in cancelled {
            let context = match &work {
                PendingLuauWork::Thread { context, start, .. } => {
                    if let LuauThreadStart::WithCompletion(completion)
                    | LuauThreadStart::Limited {
                        completion: Some(completion),
                        ..
                    } = start
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

    fn flush_pending_luau_threads(&self) {
        let pending = std::mem::take(&mut *self.pending_luau_work.borrow_mut());
        for pending in pending {
            match pending {
                PendingLuauWork::Thread {
                    context,
                    vm,
                    thread,
                    args,
                    start,
                } => {
                    self.spawn_luau_thread_start(context, vm, thread, args, start);
                }
                PendingLuauWork::Future {
                    target,
                    context,
                    delay,
                    vm,
                    thread,
                    future,
                } => {
                    let wake_at = self.delay_until(delay);
                    // A task that is not parked, such as a finished one that
                    // borrowed a coroutine, no longer carries the thread.
                    let target =
                        target.filter(|handle| self.scheduler.borrow().is_parked(handle.id()));
                    let Some(handle) = target else {
                        self.spawn_untracked_luau_future(
                            context.task_group,
                            wake_at,
                            vm,
                            thread,
                            future,
                        );
                        continue;
                    };
                    let replaced = self.scheduler.borrow_mut().replace_with_luau_future(
                        handle.id(),
                        context.task_group,
                        vm.clone(),
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

/// Marks a [`LocalScheduler`] as polling until dropped, so that a poll that unwinds doesn't leave
/// it putting off whatever it is asked from then on.
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

fn luau_thread_key(thread: &luau::Thread) -> (u64, usize) {
    (thread.vm_id(), thread.state_id())
}

fn start_wake_is_not_later(new_wake_at: Option<Instant>, old_wake_at: Option<Instant>) -> bool {
    match (new_wake_at, old_wake_at) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(new_wake_at), Some(old_wake_at)) => new_wake_at <= old_wake_at,
    }
}

enum PendingLuauWork {
    Thread {
        context: CallContext,
        vm: luau::Vm,
        thread: luau::Thread,
        args: Vec<luau::Value>,
        start: LuauThreadStart,
    },
    Future {
        target: Option<TaskHandle>,
        context: CallContext,
        delay: Option<Duration>,
        vm: luau::Vm,
        thread: luau::Thread,
        future: luau::ScheduledFuture,
    },
}

impl PendingLuauWork {
    fn thread(&self) -> (&luau::Vm, &luau::Thread) {
        match self {
            Self::Thread { vm, thread, .. } | Self::Future { vm, thread, .. } => (vm, thread),
        }
    }

    /// Whether the task this becomes belongs to `group`: as its own, or as that of the run
    /// whose future it awaits.
    fn is_in(&self, group: TaskGroupId) -> bool {
        match self {
            Self::Thread { context, .. } => context.task_group == group,
            Self::Future {
                context,
                vm,
                thread,
                ..
            } => {
                context.task_group == group
                    || vm
                        .thread_call_context(thread)
                        .ok()
                        .flatten()
                        .is_some_and(|own| own.task_group == group)
            }
        }
    }
}

/// Closes a thread the scheduler gives up: it will not resume it again, and neither may the Luau
/// code that holds it. One that is running, or has resumed another, is closed where it next
/// yields.
fn close_cancelled_thread(vm: &luau::Vm, thread: &luau::Thread) {
    if let Err(error) = thread.cancel(vm) {
        tracing::warn!(
            vm = thread.vm_id(),
            thread = thread.state_id(),
            %error,
            "cancelled Luau thread was left open"
        );
    }
}

/// Takes `task` out of the pending ones as `state`, and returns the thread it had. The thread
/// loses the context the task ran it with, unless the task only borrowed it and the context is
/// the thread's own.
fn settle(
    threads: &RefCell<HashMap<TaskId, (luau::Vm, luau::Thread)>>,
    id: TaskId,
    task: &mut Task,
    state: TaskState,
) -> Option<(luau::Vm, luau::Thread)> {
    task.state = state;
    task.wake_at = None;
    let thread = threads.borrow_mut().remove(&id);
    if let Some((vm, thread)) = &thread
        && !task.borrows_thread
    {
        let _ = vm.clear_thread_call_context(thread);
    }
    thread
}

/// What a poll of Luau work reads and tells of the scheduler it runs in.
struct Resuming<'a> {
    /// Whether the task or one of its groups was cancelled during this poll.
    cancel_asked: &'a dyn Fn() -> bool,
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

enum TaskWork {
    Future(TaskFuture),
    LuauThread {
        vm: luau::Vm,
        thread: luau::Thread,
        args: Vec<luau::Value>,
        pending_start: bool,
    },
    LuauFuture {
        vm: luau::Vm,
        thread: luau::Thread,
        future: luau::ScheduledFuture,
    },
}

enum WorkPoll {
    Completed {
        output: Option<Vec<luau::Value>>,
    },
    Parked,
    /// The thread yielded on its own, with nothing scheduled to resume it.
    Yielded,
    Cancelled,
    Failed(luau::RaisedError),
    Pending,
}

fn poll_work(
    context: &CallContext,
    work: &mut TaskWork,
    cx: &mut Context<'_>,
    resume_limit: Option<&ResumeLimit>,
    clock: &Clock,
    resuming: &Resuming<'_>,
) -> WorkPoll {
    match work {
        TaskWork::Future(future) => match catch_host_panic(context, || future.as_mut().poll(cx)) {
            Ok(Poll::Ready(Ok(()))) => WorkPoll::Completed { output: None },
            Ok(Poll::Ready(Err(error))) => WorkPoll::Failed(error.to_string().into()),
            Ok(Poll::Pending) => WorkPoll::Pending,
            Err(panic) => WorkPoll::Failed(panic.into()),
        },
        TaskWork::LuauThread {
            vm,
            thread,
            args,
            pending_start,
        } => {
            if *pending_start {
                if thread.is_started() {
                    return WorkPoll::Cancelled;
                }
                *pending_start = false;
            }
            let previous = match vm.set_thread_call_context(thread, context.clone()) {
                Ok(previous) => previous,
                Err(error) => return WorkPoll::Failed(error.to_string().into()),
            };
            let result = resume_luau_thread(vm, thread, args, resume_limit, clock);
            args.clear();
            poll_luau_resume(vm, thread, result, previous)
        }
        TaskWork::LuauFuture { vm, thread, future } => {
            let output = match catch_host_panic(context, || Pin::new(&mut *future).poll(cx)) {
                Ok(Poll::Ready(Ok(values))) => Ok(values),
                // Raised where the thread waits, as the call would have raised it had it not
                // waited, so a `pcall` around the call catches it.
                Ok(Poll::Ready(Err(error))) => Err(future.raised_error(&error)),
                Err(panic) => Err(future.raised_error(&luau::Error::Runtime(panic))),
                Ok(Poll::Pending) => return WorkPoll::Pending,
            };
            let (vm, thread) = (vm.clone(), thread.clone());
            // The future is done; what is left is the thread itself.
            let finished = std::mem::replace(
                work,
                TaskWork::LuauThread {
                    vm: vm.clone(),
                    thread: thread.clone(),
                    args: Vec::new(),
                    pending_start: false,
                },
            );
            drop_catching_panic(context, finished);
            // The future, or its drop, cancelled the thread it was to wake, which is closed.
            if (resuming.cancel_asked)() {
                return WorkPoll::Pending;
            }
            let previous = match vm.set_thread_call_context(&thread, context.clone()) {
                Ok(previous) => previous,
                Err(error) => return WorkPoll::Failed(error.to_string().into()),
            };
            let _guard = resume_limit.map(|limit| limit.guard(&vm, clock));
            let result = match output {
                Ok(values) => thread.resume(&vm, values.as_slice()),
                Err(error) => thread.resume_error(&vm, &error),
            };
            poll_luau_resume(&vm, &thread, result, previous)
        }
    }
}

/// Work taken off a task that ended, was cancelled or was removed. Dropping it runs host code,
/// which may panic, or call the scheduler it was a task of.
struct Detached {
    context: CallContext,
    work: Option<TaskWork>,
}

impl Drop for Detached {
    fn drop(&mut self) {
        drop_catching_panic(&self.context, self.work.take());
    }
}

/// Drops `work`, or leaves it in `detached` for whoever has the scheduler borrowed to drop.
fn detach(
    detached: &Option<Rc<RefCell<Vec<Detached>>>>,
    context: &CallContext,
    work: Option<TaskWork>,
) {
    let Some(work) = work else {
        return;
    };
    let work = Detached {
        context: context.clone(),
        work: Some(work),
    };
    if let Some(detached) = detached {
        detached.borrow_mut().push(work);
    }
}

/// Drops finished or failed work, whose destructor runs host code too, most suspectly after
/// that code panicked.
fn drop_catching_panic<T>(context: &CallContext, work: T) {
    let _ = catch_host_panic(context, move || drop(work));
}

/// Runs host code, turning a panic into an error so that it fails only the task it ran in and not
/// the thread every task shares.
fn catch_host_panic<T>(context: &CallContext, run: impl FnOnce() -> T) -> Result<T, String> {
    catch_unwind(AssertUnwindSafe(run)).map_err(|payload| {
        let message = payload
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("non-string panic payload");
        tracing::error!(
            plugin = context.origin.plugin.as_deref(),
            module = context
                .origin
                .module
                .as_ref()
                .map(|module| module.0.as_ref()),
            panic = message,
            "host task panicked"
        );
        format!("host task panicked: {message}")
    })
}

fn resume_luau_thread(
    vm: &luau::Vm,
    thread: &luau::Thread,
    args: &[luau::Value],
    limit: Option<&ResumeLimit>,
    clock: &Clock,
) -> luau::runtime::Result<luau::ThreadStatus> {
    let _guard = limit.map(|limit| limit.guard(vm, clock));
    thread.resume(vm, args)
}

/// After a resume the thread gets back the context it had before: none for a
/// thread the scheduler runs, its own for a coroutine it resumed.
fn poll_luau_resume(
    vm: &luau::Vm,
    thread: &luau::Thread,
    result: luau::runtime::Result<luau::ThreadStatus>,
    previous: Option<luau::CallContext>,
) -> WorkPoll {
    let _ = match previous {
        Some(previous) => vm.set_thread_call_context(thread, previous).map(|_| ()),
        None => vm.clear_thread_call_context(thread).map(|_| ()),
    };
    match result {
        Ok(luau::ThreadStatus::Completed(values)) => WorkPoll::Completed {
            output: Some(values),
        },
        Ok(luau::ThreadStatus::Closed) => WorkPoll::Cancelled,
        Ok(luau::ThreadStatus::Yielded(_)) => {
            if let Ok(scheduler) = vm.data().get::<LocalScheduler>()
                && scheduler.take_parked_luau_thread(thread)
            {
                WorkPoll::Parked
            } else {
                WorkPoll::Yielded
            }
        }
        Err(error) => WorkPoll::Failed((&error).into()),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{
        future::{
            pending,
            poll_fn,
        },
        sync::{
            Arc,
            atomic::{
                AtomicUsize,
                Ordering,
            },
        },
        task::Poll,
    };

    use super::*;
    use crate::testing::ManualClock;
    use harmony_luau::{
        CapabilityId,
        ModuleId,
    };

    #[test]
    fn ready_future_completes_with_origin_snapshot() {
        let mut scheduler = Scheduler::new();
        let group = scheduler.create_group();
        let context = CallContext {
            origin: ChunkOrigin {
                module: Some(ModuleId(Arc::from("harmony/task"))),
                plugin: Some(Arc::from("demo")),
                path: Some(Arc::from("plugins/demo/init.luau")),
            },
            capability: Some(CapabilityId(Arc::from("harmony.task"))),
            task_group: group,
            ..CallContext::default()
        };

        let handle = scheduler.spawn(context, async { Ok(()) });

        assert_eq!(scheduler.poll_ready(), 1);
        let snapshot = scheduler.snapshot(handle.id()).expect("task snapshot");
        assert_eq!(snapshot.state, TaskState::Completed);
        assert_eq!(snapshot.group, group);
        assert_eq!(snapshot.origin.plugin.as_deref(), Some("demo"));
    }

    #[test]
    fn panicking_future_fails_only_its_own_task() {
        let mut scheduler = Scheduler::new();
        let panicking = scheduler.spawn(CallContext::default(), async {
            panic!("upstream sent nonsense");
        });
        let healthy = scheduler.spawn(CallContext::default(), async { Ok(()) });

        assert_eq!(scheduler.poll_ready(), 2);
        let failed = scheduler.snapshot(panicking.id()).expect("panicking task");
        assert_eq!(failed.state, TaskState::Failed);
        assert!(
            failed
                .error
                .as_deref()
                .is_some_and(|error| error.contains("upstream sent nonsense")),
            "{:?}",
            failed.error
        );
        assert_eq!(
            scheduler
                .snapshot(healthy.id())
                .expect("healthy task")
                .state,
            TaskState::Completed
        );
    }

    #[test]
    fn future_panicking_again_when_dropped_fails_only_its_own_task() {
        struct PanicsTwice;
        impl Future for PanicsTwice {
            type Output = Result<()>;
            fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
                panic!("poll panicked");
            }
        }
        impl Drop for PanicsTwice {
            fn drop(&mut self) {
                if !std::thread::panicking() {
                    panic!("drop panicked");
                }
            }
        }

        let mut scheduler = Scheduler::new();
        let panicking = scheduler.spawn(CallContext::default(), PanicsTwice);
        let healthy = scheduler.spawn(CallContext::default(), async { Ok(()) });

        assert_eq!(scheduler.poll_ready(), 2);
        assert_eq!(
            scheduler
                .snapshot(panicking.id())
                .expect("panicking task")
                .state,
            TaskState::Failed
        );
        assert_eq!(
            scheduler
                .snapshot(healthy.id())
                .expect("healthy task")
                .state,
            TaskState::Completed
        );
    }

    #[test]
    fn removing_one_finished_task_preserves_other_finished_tasks() {
        let mut scheduler = Scheduler::new();
        let first = scheduler.spawn(CallContext::default(), async { Ok(()) });
        let second = scheduler.spawn(CallContext::default(), async { Ok(()) });

        assert_eq!(scheduler.poll_ready(), 2);
        assert!(scheduler.remove(first.id()));
        assert!(scheduler.snapshot(first.id()).is_none());
        assert_eq!(
            scheduler.snapshot(second.id()).expect("second task").state,
            TaskState::Completed
        );
    }

    #[test]
    fn cancelling_group_cancels_pending_tasks() {
        let mut scheduler = Scheduler::new();
        let group = scheduler.create_group();
        let context = CallContext {
            task_group: group,
            ..CallContext::default()
        };

        let first = scheduler.spawn(context, pending());
        let second = scheduler.spawn(
            CallContext {
                task_group: group,
                ..CallContext::default()
            },
            pending(),
        );

        assert_eq!(scheduler.cancel_group(group), 2);
        assert_eq!(
            scheduler.snapshot(first.id()).expect("first task").state,
            TaskState::Cancelled
        );
        assert_eq!(
            scheduler.snapshot(second.id()).expect("second task").state,
            TaskState::Cancelled
        );
    }

    #[test]
    fn local_and_inner_scheduler_groups_never_collide() {
        let scheduler = LocalScheduler::new();
        let groups = [
            scheduler.create_group(),
            scheduler.scheduler.borrow().create_group(),
            scheduler.create_group(),
            scheduler.scheduler.borrow().create_group(),
        ];

        let distinct: HashSet<_> = groups.iter().collect();
        assert_eq!(distinct.len(), groups.len());
        assert!(!groups.contains(&TaskGroupId::default()));
    }

    #[test]
    fn cancelled_group_cancels_tasks_spawned_into_it_later() {
        let mut scheduler = Scheduler::new();
        let group = scheduler.create_group();
        let other = scheduler.create_group();
        scheduler.cancel_group(group);

        let late = scheduler.spawn(
            CallContext {
                task_group: group,
                ..CallContext::default()
            },
            async { Ok(()) },
        );
        let unaffected = scheduler.spawn(
            CallContext {
                task_group: other,
                ..CallContext::default()
            },
            async { Ok(()) },
        );

        assert_eq!(scheduler.poll_ready(), 1);
        assert_eq!(
            scheduler.snapshot(late.id()).expect("late task").state,
            TaskState::Cancelled
        );
        assert_eq!(
            scheduler
                .snapshot(unaffected.id())
                .expect("other task")
                .state,
            TaskState::Completed
        );
    }

    /// A VM and a thread whose function returns nil.
    fn nil_thread() -> luau::runtime::Result<(luau::Vm, luau::Thread)> {
        let vm = luau::Vm::new()?;
        let function = vm.load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(&b"return nil"[..]),
            luau::ChunkOrigin::default(),
        ))?;
        let thread = vm.create_thread(&function)?;
        Ok((vm, thread))
    }

    /// What a parked async call leaves: a pending task with no work.
    fn park(scheduler: &mut Scheduler, id: TaskId) {
        scheduler.tasks.get_mut(&id).expect("task").work = None;
    }

    #[test]
    fn only_a_parked_task_takes_a_future() -> luau::runtime::Result<()> {
        let mut scheduler = Scheduler::new();
        let task = scheduler.spawn(CallContext::default(), async { Ok(()) });
        scheduler.poll_ready();
        let (vm, thread) = nil_thread()?;

        assert!(!scheduler.replace_with_luau_future(
            task.id(),
            TaskGroupId::default(),
            vm,
            thread,
            luau::ScheduledFuture::new(std::future::pending::<luau::runtime::Result<()>>()),
            None,
        ));
        assert_eq!(
            scheduler.snapshot(task.id()).expect("task").state,
            TaskState::Completed
        );
        Ok(())
    }

    #[test]
    fn a_task_awaiting_another_group_s_future_belongs_to_both_groups() -> luau::runtime::Result<()>
    {
        for cancel_callee in [true, false] {
            let mut scheduler = Scheduler::new();
            let caller = scheduler.create_group();
            let callee = scheduler.create_group();
            let task = scheduler.spawn(
                CallContext {
                    task_group: caller,
                    ..CallContext::default()
                },
                pending(),
            );
            let (vm, thread) = nil_thread()?;
            park(&mut scheduler, task.id());

            assert!(scheduler.replace_with_luau_future(
                task.id(),
                callee,
                vm,
                thread,
                luau::ScheduledFuture::new(std::future::pending::<luau::runtime::Result<()>>()),
                None,
            ));
            for group in [caller, callee] {
                let snapshots = scheduler.group_snapshots(group);
                assert_eq!(snapshots.len(), 1);
                assert_eq!(snapshots[0].id, task.id());
                assert_eq!(snapshots[0].group, caller);
            }

            let cancelled = if cancel_callee { callee } else { caller };
            assert_eq!(scheduler.cancel_group(cancelled), 1);
            assert_eq!(
                scheduler.snapshot(task.id()).expect("task").state,
                TaskState::Cancelled
            );
            scheduler.remove(task.id());
            assert!(scheduler.group_snapshots(caller).is_empty());
            assert!(scheduler.group_snapshots(callee).is_empty());
        }
        Ok(())
    }

    #[test]
    fn a_task_leaves_the_awaited_group_once_its_future_completes() -> luau::runtime::Result<()> {
        let mut scheduler = Scheduler::new();
        let caller = scheduler.create_group();
        let callee = scheduler.create_group();
        let (vm, thread) = nil_thread()?;
        let task = scheduler.spawn(
            CallContext {
                task_group: caller,
                ..CallContext::default()
            },
            pending(),
        );
        park(&mut scheduler, task.id());
        assert!(scheduler.replace_with_luau_future(
            task.id(),
            callee,
            vm,
            thread,
            luau::ScheduledFuture::new(async { luau::runtime::Result::Ok(()) }),
            None,
        ));

        assert_eq!(scheduler.group_snapshots(callee).len(), 1);
        scheduler.poll_ready();
        assert!(scheduler.group_snapshots(callee).is_empty());
        assert_eq!(scheduler.group_snapshots(caller).len(), 1);
        Ok(())
    }

    #[test]
    fn default_group_is_never_cancelled() {
        let mut scheduler = Scheduler::new();
        let pending_task = scheduler.spawn(CallContext::default(), pending());

        assert_eq!(scheduler.cancel_group(TaskGroupId::default()), 0);
        let later = scheduler.spawn(CallContext::default(), async { Ok(()) });

        assert_eq!(scheduler.poll_ready(), 1);
        assert_eq!(
            scheduler
                .snapshot(pending_task.id())
                .expect("pending task")
                .state,
            TaskState::Pending
        );
        assert_eq!(
            scheduler.snapshot(later.id()).expect("later task").state,
            TaskState::Completed
        );
    }

    #[test]
    fn failed_future_preserves_error_for_diagnostics() {
        let mut scheduler = Scheduler::new();
        let handle = scheduler.spawn(CallContext::default(), async { anyhow::bail!("boom") });

        assert_eq!(scheduler.poll_ready(), 1);
        let snapshot = scheduler.snapshot(handle.id()).expect("task snapshot");
        assert_eq!(snapshot.state, TaskState::Failed);
        assert!(
            snapshot
                .error
                .as_deref()
                .is_some_and(|error| error.contains("boom"))
        );
    }

    #[test]
    fn pending_future_is_polled_again_after_waker_signal() {
        let mut scheduler = Scheduler::new();
        let polls = Arc::new(AtomicUsize::new(0));
        let handle = scheduler.spawn(CallContext::default(), {
            let polls = polls.clone();
            poll_fn(move |cx| {
                if polls.fetch_add(1, Ordering::SeqCst) == 0 {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    Poll::Ready(Ok(()))
                }
            })
        });

        assert_eq!(scheduler.poll_ready(), 0);
        assert_eq!(
            scheduler
                .snapshot(handle.id())
                .expect("task snapshot")
                .state,
            TaskState::Pending
        );
        assert_eq!(scheduler.next_wake_delay(), Some(Duration::ZERO));

        assert_eq!(scheduler.poll_ready(), 1);
        assert_eq!(
            scheduler
                .snapshot(handle.id())
                .expect("task snapshot")
                .state,
            TaskState::Completed
        );
        assert_eq!(polls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn delayed_task_waits_until_wake_time() {
        let clock = ManualClock::new();
        let mut scheduler = Scheduler::with_manual_clock(clock.clone());
        let handle = scheduler.spawn_boxed_after(
            CallContext::default(),
            Duration::from_secs(5),
            Box::pin(async { Ok(()) }),
        );
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
    fn luau_root_thread_completes_on_scheduler() -> luau::runtime::Result<()> {
        let vm = luau::Vm::new()?;
        let function = vm.load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(&b"local value = ...; return value + 1"[..]),
            luau::ChunkOrigin {
                module: Some(ModuleId(Arc::from("demo/root"))),
                plugin: Some(Arc::from("demo")),
                path: Some(Arc::from("plugins/demo/init.luau")),
            },
        ))?;
        let mut scheduler = Scheduler::new();
        let group = scheduler.create_group();
        let context = CallContext {
            origin: ChunkOrigin {
                module: Some(ModuleId(Arc::from("demo/root"))),
                plugin: Some(Arc::from("demo")),
                path: Some(Arc::from("plugins/demo/init.luau")),
            },
            task_group: group,
            ..CallContext::default()
        };

        let handle = scheduler.spawn_luau_function(
            context,
            vm,
            &function,
            vec![luau::Value::Number(41.0)],
        )?;

        assert_eq!(scheduler.poll_ready(), 1);
        let snapshot = scheduler.snapshot(handle.id()).expect("task snapshot");
        assert_eq!(snapshot.state, TaskState::Completed);
        assert_eq!(snapshot.origin.plugin.as_deref(), Some("demo"));
        Ok(())
    }

    #[test]
    fn luau_root_thread_failure_is_scheduler_failure() -> luau::runtime::Result<()> {
        let vm = luau::Vm::new()?;
        vm.open_standard_libraries(luau::StandardLibraries {
            base: true,
            ..luau::StandardLibraries::none()
        })?;
        let function = vm.load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(&b"error('root failed')"[..]),
            luau::ChunkOrigin::default(),
        ))?;
        let mut scheduler = Scheduler::new();
        let handle =
            scheduler.spawn_luau_function(CallContext::default(), vm, &function, vec![])?;

        assert_eq!(scheduler.poll_ready(), 1);
        let snapshot = scheduler.snapshot(handle.id()).expect("task snapshot");
        assert_eq!(snapshot.state, TaskState::Failed);
        assert!(
            snapshot
                .error
                .as_deref()
                .is_some_and(|error| error.contains("root failed"))
        );
        Ok(())
    }

    #[test]
    fn per_task_luau_budget_does_not_leak_to_other_tasks() -> luau::runtime::Result<()> {
        let vm = luau::Vm::new()?;
        let timed_function = vm.load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(&b"while true do end"[..]),
            luau::ChunkOrigin::default(),
        ))?;
        let ordinary_function = vm.load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(
                &b"local total = 0; for value = 1, 100000 do total += value end; return total"[..],
            ),
            luau::ChunkOrigin::default(),
        ))?;
        let timed_thread = vm.create_thread(&timed_function)?;
        let ordinary_thread = vm.create_thread(&ordinary_function)?;
        let mut scheduler = Scheduler::new();
        let timed = scheduler.spawn_luau_thread_with_options(
            CallContext::default(),
            vm.clone(),
            timed_thread,
            Vec::new(),
            LuauTaskOptions {
                resume_limit: Some(ResumeLimit::Budget(Duration::ZERO)),
                completion: None,
            },
        );
        let ordinary =
            scheduler.spawn_luau_thread(CallContext::default(), vm, ordinary_thread, Vec::new());

        assert_eq!(scheduler.poll_ready(), 2);
        assert_eq!(
            scheduler.snapshot(timed.id()).expect("timed task").state,
            TaskState::Failed
        );
        assert_eq!(
            scheduler
                .snapshot(ordinary.id())
                .expect("ordinary task")
                .state,
            TaskState::Completed
        );
        Ok(())
    }

    #[test]
    fn a_resume_limited_until_a_deadline_stops_at_it_or_at_its_signal() -> luau::runtime::Result<()>
    {
        let vm = luau::Vm::new()?;
        let function = vm.load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(&b"while true do end"[..]),
            luau::ChunkOrigin::default(),
        ))?;
        let raised = luau::InterruptSignal::new();
        raised.raise();
        let mut scheduler = Scheduler::new();
        let limits = [
            ResumeLimit::Until {
                deadline: Instant::now() + Duration::from_secs(600),
                signal: raised,
            },
            ResumeLimit::Until {
                deadline: Instant::now(),
                signal: luau::InterruptSignal::new(),
            },
        ];
        let tasks: Vec<_> = limits
            .into_iter()
            .map(|limit| -> luau::runtime::Result<TaskHandle> {
                Ok(scheduler.spawn_luau_thread_with_options(
                    CallContext::default(),
                    vm.clone(),
                    vm.create_thread(&function)?,
                    Vec::new(),
                    LuauTaskOptions {
                        resume_limit: Some(limit),
                        completion: None,
                    },
                ))
            })
            .collect::<luau::runtime::Result<_>>()?;

        let started = Instant::now();
        assert_eq!(scheduler.poll_ready(), 2);
        assert!(started.elapsed() < Duration::from_secs(60));
        for task in tasks {
            assert_eq!(
                scheduler.snapshot(task.id()).expect("task").state,
                TaskState::Failed
            );
        }
        Ok(())
    }

    #[test]
    fn pending_budgeted_luau_thread_preserves_budget_and_completion() -> luau::runtime::Result<()> {
        let vm = luau::Vm::new()?;
        let function = vm.load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(&b"return 42"[..]),
            luau::ChunkOrigin::default(),
        ))?;
        let thread = vm.create_thread(&function)?;
        let scheduler = LocalScheduler::new();
        let completion = Rc::new(LocalLuauTaskCompletion::default());

        scheduler.polling.set(true);
        scheduler.schedule_luau_thread_with_budget_and_completion(
            CallContext::default(),
            vm,
            thread.clone(),
            Vec::new(),
            Duration::from_secs(1),
            completion.clone(),
        );
        scheduler.polling.set(false);
        scheduler.flush_pending_luau_threads();

        let handle = scheduler
            .luau_thread_handle(&thread)
            .expect("scheduled thread handle");
        let inner = scheduler.scheduler.borrow();
        let task = inner.tasks.get(&handle.id()).expect("scheduled task");
        assert!(matches!(
            task.resume_limit,
            Some(ResumeLimit::Budget(budget)) if budget == Duration::from_secs(1)
        ));
        assert!(Rc::ptr_eq(
            task.completion.as_ref().expect("task completion"),
            &completion
        ));
        drop(inner);

        assert_eq!(scheduler.poll_ready(), 1);
        let mut cx = Context::from_waker(Waker::noop());
        let Poll::Ready(Ok(values)) = completion.poll(&mut cx) else {
            panic!("completion should contain the Luau result");
        };
        assert!(matches!(values.as_slice(), [luau::Value::Number(42.0)]));
        Ok(())
    }

    /// Runs `test` on a thread of its own and fails if it is still running after a long while,
    /// as one whose wait missed its wake would be for good.
    pub(crate) fn ending_without_a_missed_wake(test: impl FnOnce() + Send + 'static) {
        let (done, ended) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            test();
            let _ = done.send(());
        });
        match ended.recv_timeout(Duration::from_secs(60)) {
            Ok(()) => thread.join().expect("test thread"),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                panic!("nothing ended the scheduler's wait")
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                std::panic::resume_unwind(thread.join().expect_err("the test panicked"))
            }
        }
    }

    /// Returns once the scheduler's owner is asleep in a wait.
    fn once_waiting(wake: &SchedulerWake) {
        while !wake.owner_is_waiting() {
            std::thread::yield_now();
        }
    }

    /// Runs rounds that sleep in between, as a host does, until `done`. Only a wake or a due
    /// task ends each sleep.
    fn run_until(scheduler: &LocalScheduler, done: impl Fn() -> bool) {
        loop {
            scheduler.poll_ready();
            if done() {
                return;
            }
            scheduler.wait_for_work(None);
        }
    }

    #[test]
    fn a_wake_handle_ends_a_wait_from_another_thread() {
        ending_without_a_missed_wake(|| {
            let scheduler = Scheduler::new();
            let wake = scheduler.wake_handle();
            let waker = std::thread::spawn(move || {
                once_waiting(&wake);
                wake.wake();
            });

            scheduler.wait_for_wake(None);
            waker.join().expect("waker thread");
        });
    }

    /// The task's own waker, called from another thread while the owner sleeps with nothing due,
    /// is all that ends the wait.
    #[test]
    fn a_task_s_waker_ends_a_wait_from_another_thread() {
        ending_without_a_missed_wake(|| {
            let scheduler = LocalScheduler::new();
            let (ready, readied) = tokio::sync::oneshot::channel::<()>();
            let seen = Rc::new(Cell::new(false));
            scheduler.schedule_future(CallContext::default(), {
                let seen = seen.clone();
                async move {
                    let _ = readied.await;
                    seen.set(true);
                    Ok(())
                }
            });
            scheduler.poll_ready();
            assert!(!seen.get());

            let waker = std::thread::spawn({
                let wake = scheduler.wake_handle();
                move || {
                    once_waiting(&wake);
                    let _ = ready.send(());
                }
            });
            run_until(&scheduler, || seen.get());
            waker.join().expect("waking thread");
        });
    }

    #[test]
    fn a_wake_before_the_wait_is_kept_for_it() {
        ending_without_a_missed_wake(|| {
            let scheduler = Scheduler::new();
            scheduler.wake_handle().waker().wake();

            scheduler.wait_for_wake(None);
        });
    }

    #[test]
    fn a_cancellation_ends_a_wait_for_work_without_a_deadline() {
        ending_without_a_missed_wake(|| {
            let scheduler = LocalScheduler::new();
            let cancellation = Cancellation::new(scheduler.wake_handle());
            let cancel = std::thread::spawn({
                let (cancellation, wake) = (cancellation.clone(), scheduler.wake_handle());
                move || {
                    once_waiting(&wake);
                    cancellation.cancel();
                }
            });

            scheduler.wait_for_work(None);
            assert!(cancellation.is_cancelled());
            cancel.join().expect("cancelling thread");
        });
    }

    #[test]
    fn a_task_spawns_another_while_the_scheduler_polls() {
        let scheduler = Rc::new(LocalScheduler::new());
        let ran = Rc::new(Cell::new(false));
        scheduler.schedule_future(CallContext::default(), {
            let (scheduler, ran) = (Rc::downgrade(&scheduler), ran.clone());
            async move {
                let scheduler = scheduler.upgrade().expect("the scheduler is polling");
                scheduler.schedule_future(CallContext::default(), async move {
                    ran.set(true);
                    Ok(())
                });
                Ok(())
            }
        });

        scheduler.poll_ready();
        assert!(!ran.get(), "a task spawned in a poll starts after it");
        assert_eq!(scheduler.next_wake_delay(), Some(Duration::ZERO));
        scheduler.poll_ready();
        assert!(ran.get());
    }

    /// On the real clock and the real wait: nothing but the sleep's deadline ends the wait.
    #[test]
    fn a_sleep_ends_the_wait_and_wakes_its_task_at_the_deadline() {
        ending_without_a_missed_wake(|| {
            let scheduler = LocalScheduler::new();
            let woke = Rc::new(Cell::new(None));
            let deadline = Instant::now() + Duration::from_millis(50);
            let sleep = scheduler.sleep_until(deadline);
            scheduler.schedule_future(CallContext::default(), {
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
    fn sleeper(scheduler: &LocalScheduler, deadline: Instant) -> Rc<Cell<bool>> {
        let woke = Rc::new(Cell::new(false));
        let sleep = scheduler.sleep_until(deadline);
        scheduler.schedule_future(CallContext::default(), {
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
        let scheduler = LocalScheduler::with_manual_clock(clock.clone());
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
        let scheduler = LocalScheduler::with_manual_clock(clock.clone());
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
        let scheduler = LocalScheduler::with_manual_clock(clock.clone());
        // Even with work due at once, which a real wait would return for.
        scheduler.schedule_future(CallContext::default(), async { Ok(()) });
        scheduler.wait_for_work(Some(clock.now()));
    }

    #[test]
    #[should_panic(expected = "manual clock can't block")]
    fn a_scheduler_on_a_manual_clock_refuses_to_wait_for_a_wake() {
        let scheduler = Scheduler::with_manual_clock(ManualClock::new());
        scheduler.wake_handle().wake();
        scheduler.wait_for_wake(Some(Duration::ZERO));
    }

    fn chunk_thread(vm: &luau::Vm, source: &'static str) -> luau::runtime::Result<luau::Thread> {
        vm.create_thread(&vm.load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(source.as_bytes()),
            luau::ChunkOrigin::default(),
        ))?)
    }

    /// Schedules a thread that returns at once, and a task that moves the clock on by a second,
    /// in the order given, and runs them in one round. Returns when the thread ended, as its
    /// completion has it, counted from the round's start.
    fn ended_after(advance_first: bool) -> luau::runtime::Result<Duration> {
        let vm = luau::Vm::new()?;
        let clock = ManualClock::new();
        let scheduler = LocalScheduler::with_manual_clock(clock.clone());
        let completion = Rc::new(LocalLuauTaskCompletion::default());
        let advance = |scheduler: &LocalScheduler| {
            let clock = clock.clone();
            scheduler.schedule_future(CallContext::default(), async move {
                clock.advance(Duration::from_secs(1));
                Ok(())
            });
        };
        if advance_first {
            advance(&scheduler);
        }
        scheduler.schedule_luau_thread_with_completion(
            CallContext::default(),
            vm.clone(),
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
        let scheduler = LocalScheduler::with_manual_clock(clock.clone());
        let start = clock.now();
        let completion = Rc::new(LocalLuauTaskCompletion::default());
        let thread = chunk_thread(&vm, "coroutine.yield()")?;
        scheduler.schedule_luau_thread_with_completion(
            CallContext::default(),
            vm.clone(),
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

    /// The deadline is read by the scheduler's clock, and what is left until it is enforced in
    /// real time: a clock that stands still doesn't let a resume run for good.
    #[test]
    fn a_frozen_clock_still_interrupts_a_resume_at_its_limit() -> luau::runtime::Result<()> {
        let vm = luau::Vm::new()?;
        let clock = ManualClock::new();
        let scheduler = LocalScheduler::with_manual_clock(clock.clone());
        let limits = [
            ResumeLimit::Budget(Duration::from_millis(20)),
            ResumeLimit::Until {
                deadline: clock.now() + Duration::from_millis(20),
                signal: luau::InterruptSignal::new(),
            },
        ];
        let completions = limits.map(|limit| -> luau::runtime::Result<_> {
            let completion = Rc::new(LocalLuauTaskCompletion::default());
            scheduler.schedule_luau_thread_with_limit_and_completion(
                CallContext::default(),
                vm.clone(),
                chunk_thread(&vm, "while true do end")?,
                Vec::new(),
                limit,
                completion.clone(),
            );
            Ok(completion)
        });

        let start = clock.now();
        assert_eq!(scheduler.poll_ready(), 2);
        assert_eq!(clock.now(), start);
        for completion in completions {
            let completion = completion?;
            let ended = completion_message(&completion).expect("the thread ended");
            assert!(
                ended.is_err_and(|error| error.contains(luau::INTERRUPTED_ERROR)),
                "the resume wasn't interrupted"
            );
        }
        Ok(())
    }

    #[test]
    fn a_resume_limited_until_a_deadline_already_passed_by_the_clock_is_interrupted_at_once()
    -> luau::runtime::Result<()> {
        let vm = luau::Vm::new()?;
        let clock = ManualClock::new();
        let scheduler = LocalScheduler::with_manual_clock(clock.clone());
        // An hour away in real time, and passed once the clock says so.
        let deadline = clock.now() + Duration::from_secs(3600);
        clock.advance(Duration::from_secs(3600));
        let completion = Rc::new(LocalLuauTaskCompletion::default());
        scheduler.schedule_luau_thread_with_limit_and_completion(
            CallContext::default(),
            vm.clone(),
            chunk_thread(&vm, "while true do end")?,
            Vec::new(),
            ResumeLimit::Until {
                deadline,
                signal: luau::InterruptSignal::new(),
            },
            completion.clone(),
        );

        assert_eq!(scheduler.poll_ready(), 1);
        let ended = completion_message(&completion).expect("the thread ended");
        assert!(ended.is_err_and(|error| error.contains(luau::INTERRUPTED_ERROR)));
        Ok(())
    }

    #[test]
    fn a_cancellation_wakes_the_task_waiting_on_it() {
        ending_without_a_missed_wake(|| {
            let scheduler = LocalScheduler::new();
            let cancellation = Cancellation::new(scheduler.wake_handle());
            let seen = Rc::new(Cell::new(false));
            scheduler.schedule_future(CallContext::default(), {
                let (cancellation, seen) = (cancellation.clone(), seen.clone());
                async move {
                    cancellation.cancelled().await;
                    seen.set(true);
                    Ok(())
                }
            });
            scheduler.poll_ready();
            assert!(!seen.get());

            let cancel = std::thread::spawn({
                let wake = scheduler.wake_handle();
                move || {
                    once_waiting(&wake);
                    cancellation.cancel();
                }
            });
            run_until(&scheduler, || seen.get());
            cancel.join().expect("cancelling thread");
        });
    }

    /// A thread that waits forever on a host call, whose future raises `dropped` when dropped.
    fn waiting_thread(
        vm: &luau::Vm,
        scheduler: &LocalScheduler,
        context: CallContext,
        dropped: Arc<AtomicBool>,
    ) -> luau::runtime::Result<luau::Thread> {
        struct RaiseOnDrop(Arc<AtomicBool>);
        impl Drop for RaiseOnDrop {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let thread = vm.create_thread(&vm.load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(&b"coroutine.yield()"[..]),
            luau::ChunkOrigin::default(),
        ))?)?;
        scheduler.spawn_luau_thread(context.clone(), vm.clone(), thread.clone(), Vec::new());
        scheduler.poll_ready();
        let guard = RaiseOnDrop(dropped);
        scheduler.schedule_luau_future(
            context,
            vm.clone(),
            thread.clone(),
            luau::ScheduledFuture::new(async move {
                let _guard = guard;
                pending::<luau::runtime::Result<()>>().await
            }),
        );
        Ok(thread)
    }

    fn completion_message(completion: &LocalLuauTaskCompletion) -> Option<Result<(), String>> {
        match completion.poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(result) => Some(result.map(|_| ()).map_err(|error| error.message)),
            Poll::Pending => None,
        }
    }

    #[test]
    fn a_watcher_scheduled_after_a_thread_cancels_it_before_it_starts() -> luau::runtime::Result<()>
    {
        let vm = luau::Vm::new()?;
        let scheduler = Rc::new(LocalScheduler::new());
        let thread = vm.create_thread(&vm.load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(&b"ran = true"[..]),
            luau::ChunkOrigin::default(),
        ))?)?;
        let completion = Rc::new(LocalLuauTaskCompletion::default());
        scheduler.schedule_luau_thread_with_completion(
            CallContext::default(),
            vm.clone(),
            thread.clone(),
            Vec::new(),
            completion.clone(),
        );
        scheduler.schedule_watcher(CallContext::default(), {
            let (scheduler, thread) = (Rc::downgrade(&scheduler), thread.clone());
            async move {
                scheduler
                    .upgrade()
                    .expect("the scheduler is polling")
                    .cancel_luau_thread(&thread);
                Ok(())
            }
        });

        scheduler.poll_ready();
        assert!(!ran(&vm)?, "the cancelled thread still ran");
        let ended = completion_message(&completion).expect("the thread is cancelled");
        assert!(ended.is_err_and(|error| error.contains("cancelled")));
        assert!(completion.ended_at().is_some());
        Ok(())
    }

    #[test]
    fn a_watcher_woken_after_its_thread_still_cancels_it_before_it_resumes()
    -> luau::runtime::Result<()> {
        let vm = luau::Vm::new()?;
        let scheduler = Rc::new(LocalScheduler::new());
        let thread = vm.create_thread(&vm.load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(&b"coroutine.yield()"[..]),
            luau::ChunkOrigin::default(),
        ))?)?;
        scheduler.spawn_luau_thread(
            CallContext::default(),
            vm.clone(),
            thread.clone(),
            Vec::new(),
        );
        scheduler.poll_ready();
        let (ready, readied) = tokio::sync::oneshot::channel::<()>();
        let resumed = Arc::new(AtomicBool::new(false));
        scheduler.schedule_luau_future(
            CallContext::default(),
            vm,
            thread.clone(),
            luau::ScheduledFuture::new({
                let resumed = resumed.clone();
                async move {
                    let _ = readied.await;
                    resumed.store(true, Ordering::Release);
                    Ok(())
                }
            }),
        );
        let (leave, left) = tokio::sync::oneshot::channel::<()>();
        scheduler.schedule_watcher(CallContext::default(), {
            let scheduler = Rc::downgrade(&scheduler);
            async move {
                let _ = left.await;
                scheduler
                    .upgrade()
                    .expect("the scheduler is polling")
                    .cancel_luau_thread(&thread);
                Ok(())
            }
        });
        scheduler.poll_ready();
        assert!(scheduler.has_pending());

        // The thread's wake is queued ahead of its watcher's.
        let _ = ready.send(());
        let _ = leave.send(());
        scheduler.poll_ready();
        assert!(
            !resumed.load(Ordering::Acquire),
            "the cancelled thread's call still completed"
        );
        scheduler.poll_ready();
        assert!(!resumed.load(Ordering::Acquire));
        Ok(())
    }

    /// The outer thread waits on a call that runs an inner thread, which the call cancels when
    /// dropped. The inner thread is ready in the round the outer one's watcher cancels it.
    #[test]
    fn a_watcher_s_cancel_reaches_the_threads_its_thread_was_waiting_on()
    -> luau::runtime::Result<()> {
        struct CancelOnDrop(std::rc::Weak<LocalScheduler>, luau::Thread);
        impl Drop for CancelOnDrop {
            fn drop(&mut self) {
                if let Some(scheduler) = self.0.upgrade() {
                    scheduler.schedule_cancel_luau_thread(self.1.clone());
                }
            }
        }

        let vm = luau::Vm::new()?;
        let scheduler = Rc::new(LocalScheduler::new());
        let chunk = |source: &'static [u8]| {
            vm.create_thread(&vm.load_chunk(&luau::Chunk::new(
                Arc::<[u8]>::from(source),
                luau::ChunkOrigin::default(),
            ))?)
        };
        let outer = chunk(b"coroutine.yield()")?;
        scheduler.spawn_luau_thread(
            CallContext::default(),
            vm.clone(),
            outer.clone(),
            Vec::new(),
        );
        scheduler.poll_ready();
        let (leave, left) = tokio::sync::oneshot::channel::<()>();
        scheduler.schedule_watcher(CallContext::default(), {
            let scheduler = Rc::downgrade(&scheduler);
            let outer = outer.clone();
            async move {
                let _ = left.await;
                scheduler
                    .upgrade()
                    .expect("the scheduler is polling")
                    .cancel_luau_thread(&outer);
                Ok(())
            }
        });
        scheduler.poll_ready();

        let inner = chunk(b"ran = true")?;
        scheduler.spawn_luau_thread(
            CallContext::default(),
            vm.clone(),
            inner.clone(),
            Vec::new(),
        );
        let guard = CancelOnDrop(Rc::downgrade(&scheduler), inner.clone());
        scheduler.schedule_luau_future(
            CallContext::default(),
            vm.clone(),
            outer,
            luau::ScheduledFuture::new(async move {
                let _guard = guard;
                pending::<luau::runtime::Result<()>>().await
            }),
        );
        let _ = leave.send(());
        scheduler.poll_ready();

        assert!(!ran(&vm)?, "the inner thread ran");
        Ok(())
    }

    /// A task's own panic fails the task alone. What unwinds a poll is a panic outside the tasks'
    /// code, such as one from the waker a completion wakes.
    /// A VM whose scheduler is its own, with `wait()`, a host call that never returns, `pause()`,
    /// one that returns in the next round, `cancel(thread)` and `cancel_group(id)`, and
    /// `cancel_as_it_wakes()` and `cancel_group_as_it_wakes()`, waits that cancel the thread they
    /// are about to wake, or its group.
    fn vm_with_waits() -> luau::runtime::Result<(luau::Vm, Rc<LocalScheduler>)> {
        use crate::modules::{
            FunctionSpec,
            ModuleSpec,
            install_luau_module,
        };

        let vm = luau::Vm::new()?;
        vm.open_standard_libraries(luau::StandardLibraries::all_supported())?;
        vm.data().insert(LocalScheduler::new())?;
        let module = ModuleSpec::new("demo/waits")
            .function(FunctionSpec::new("wait").call_async(Arc::new(|_frame| {
                Ok(luau::ScheduledFuture::new(pending::<
                    luau::runtime::Result<()>,
                >()))
            })))
            .function(FunctionSpec::new("pause").call_async(Arc::new(|_frame| {
                Ok(luau::ScheduledFuture::after(Duration::ZERO, async {
                    Ok(())
                }))
            })))
            .function(
                FunctionSpec::new("cancel_as_it_wakes").call_async(Arc::new(|frame| {
                    let scheduler = Rc::downgrade(&frame.vm.data().get::<LocalScheduler>()?);
                    let thread = frame.thread.clone();
                    Ok(luau::ScheduledFuture::after(Duration::ZERO, async move {
                        scheduler
                            .upgrade()
                            .expect("the scheduler is polling")
                            .cancel_luau_thread(&thread);
                        Ok(())
                    }))
                })),
            )
            .function(
                FunctionSpec::new("cancel_group_as_it_wakes").call_async(Arc::new(|frame| {
                    let scheduler = Rc::downgrade(&frame.vm.data().get::<LocalScheduler>()?);
                    let group = frame.context.task_group;
                    Ok(luau::ScheduledFuture::after(Duration::ZERO, async move {
                        scheduler
                            .upgrade()
                            .expect("the scheduler is polling")
                            .cancel_group(group);
                        Ok(())
                    }))
                })),
            )
            .function(FunctionSpec::new("cancel_group").call(|mut frame| {
                let group: f64 = frame.args.read()?;
                frame
                    .vm
                    .data()
                    .get::<LocalScheduler>()?
                    .cancel_group(TaskGroupId(group as u64));
                Ok(())
            }))
            .function(FunctionSpec::new("cancel").call(|mut frame| {
                let thread: luau::Thread = frame.args.read()?;
                frame
                    .vm
                    .data()
                    .get::<LocalScheduler>()?
                    .cancel_luau_thread(&thread);
                Ok(())
            }));
        let table = install_luau_module(&vm, &luau::ChunkOrigin::default(), &module)?;
        for name in [
            "wait",
            "pause",
            "cancel",
            "cancel_group",
            "cancel_as_it_wakes",
            "cancel_group_as_it_wakes",
        ] {
            let luau::Value::Function(function) = table.get(name)? else {
                panic!("the module has no {name}");
            };
            vm.set_global_function(name, &function)?;
        }
        let scheduler = vm.data().get::<LocalScheduler>()?;
        Ok((vm, scheduler))
    }

    fn spawn_chunk(
        vm: &luau::Vm,
        scheduler: &LocalScheduler,
        context: CallContext,
        source: &'static str,
    ) -> luau::runtime::Result<luau::Thread> {
        let thread = chunk_thread(vm, source)?;
        scheduler.spawn_luau_thread(context, vm.clone(), thread.clone(), Vec::new());
        Ok(thread)
    }

    /// Whether a chunk that sets `ran` has run in `vm`.
    fn ran(vm: &luau::Vm) -> luau::runtime::Result<bool> {
        Ok(eval(vm, "return ran")? == [luau::Value::Boolean(true)])
    }

    fn eval(vm: &luau::Vm, source: &'static str) -> luau::runtime::Result<Vec<luau::Value>> {
        vm.eval(
            Arc::<[u8]>::from(source.as_bytes()),
            luau::ChunkOrigin::default(),
        )
    }

    /// What Luau code holding `thread` makes of it: its status, and what resuming it returns.
    fn status_and_resume(
        vm: &luau::Vm,
        thread: &luau::Thread,
    ) -> luau::runtime::Result<Vec<luau::Value>> {
        vm.load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(
                &b"local co = ...; return coroutine.status(co), coroutine.resume(co)"[..],
            ),
            luau::ChunkOrigin::default(),
        ))?
        .call(vm, &[luau::Value::Thread(thread.clone())])
    }

    #[track_caller]
    fn assert_closed(vm: &luau::Vm, thread: &luau::Thread) {
        assert_eq!(
            status_and_resume(vm, thread).expect("Luau reads the thread"),
            vec![
                luau::Value::String(b"dead".to_vec()),
                luau::Value::Boolean(false),
                luau::Value::String(b"cannot resume dead coroutine".to_vec()),
            ]
        );
        assert_eq!(
            eval(vm, "return went_on").expect("Luau reads the global"),
            vec![luau::Value::Nil],
            "a cancelled thread went on"
        );
    }

    #[test]
    fn a_cancelled_thread_is_closed_wherever_it_waited() -> luau::runtime::Result<()> {
        let (vm, scheduler) = vm_with_waits()?;
        let spawn = |source| spawn_chunk(&vm, &scheduler, CallContext::default(), source);
        let yielded = spawn("coroutine.yield() went_on = true")?;
        let waiting = spawn("wait() went_on = true")?;
        scheduler.poll_ready();
        let unstarted = spawn("went_on = true")?;
        let delayed = chunk_thread(&vm, "went_on = true")?;
        scheduler.spawn_luau_thread_after(
            CallContext::default(),
            Duration::ZERO,
            vm.clone(),
            delayed.clone(),
            Vec::new(),
        );

        for thread in [&yielded, &waiting, &unstarted, &delayed] {
            assert!(scheduler.cancel_luau_thread(thread));
            assert_closed(&vm, thread);
        }
        scheduler.poll_ready();
        assert_closed(&vm, &delayed);
        Ok(())
    }

    #[test]
    fn a_thread_that_ended_or_was_never_scheduled_is_not_closed() -> luau::runtime::Result<()> {
        let (vm, scheduler) = vm_with_waits()?;
        let unscheduled = chunk_thread(&vm, "return 42")?;
        let Some(luau::Value::Thread(handed_back)) = eval(
            &vm,
            "return coroutine.create(function() coroutine.yield() return 42 end)",
        )?
        .pop() else {
            panic!("expected a thread");
        };
        // Its task ends as it yields, which hands the coroutine back to the code that made it.
        scheduler.schedule_luau_future(
            CallContext::default(),
            vm.clone(),
            handed_back.clone(),
            luau::ScheduledFuture::new(async { Ok(()) }),
        );
        scheduler.poll_ready();

        for thread in [&unscheduled, &handed_back] {
            scheduler.cancel_luau_thread(thread);
            assert_eq!(
                thread.resume(&vm, &[])?,
                luau::ThreadStatus::Completed(vec![luau::Value::Number(42.0)])
            );
        }
        Ok(())
    }

    #[test]
    fn cancelling_a_group_closes_its_threads_and_no_others() -> luau::runtime::Result<()> {
        let (vm, scheduler) = vm_with_waits()?;
        let group = scheduler.create_group();
        let in_group = || CallContext {
            task_group: group,
            ..CallContext::default()
        };
        let yielded = spawn_chunk(
            &vm,
            &scheduler,
            in_group(),
            "coroutine.yield() went_on = true",
        )?;
        let waiting = spawn_chunk(&vm, &scheduler, in_group(), "wait() went_on = true")?;
        let other = spawn_chunk(
            &vm,
            &scheduler,
            CallContext::default(),
            "coroutine.yield() return 42",
        )?;
        scheduler.poll_ready();

        scheduler.cancel_group(group);

        assert_closed(&vm, &yielded);
        assert_closed(&vm, &waiting);
        let late = spawn_chunk(&vm, &scheduler, in_group(), "went_on = true")?;
        assert_closed(&vm, &late);
        assert_eq!(
            other.resume(&vm, &[])?,
            luau::ThreadStatus::Completed(vec![luau::Value::Number(42.0)])
        );
        Ok(())
    }

    /// The watcher cancels one thread that waits and one that is ready in the same round.
    #[test]
    fn a_thread_a_watcher_cancels_during_a_poll_is_closed_before_the_poll_ends()
    -> luau::runtime::Result<()> {
        let (vm, scheduler) = vm_with_waits()?;
        let waiting = spawn_chunk(
            &vm,
            &scheduler,
            CallContext::default(),
            "wait() went_on = true",
        )?;
        scheduler.poll_ready();
        let ready = spawn_chunk(&vm, &scheduler, CallContext::default(), "went_on = true")?;
        scheduler.schedule_watcher(CallContext::default(), {
            let scheduler = Rc::downgrade(&scheduler);
            let (waiting, ready) = (waiting.clone(), ready.clone());
            async move {
                let scheduler = scheduler.upgrade().expect("the scheduler is polling");
                scheduler.cancel_luau_thread(&waiting);
                scheduler.cancel_luau_thread(&ready);
                Ok(())
            }
        });

        scheduler.poll_ready();

        assert_closed(&vm, &waiting);
        assert_closed(&vm, &ready);
        Ok(())
    }

    /// The task after them holds both threads and could resume them in the same round: each is
    /// closed as it yields, not when the poll ends, and leaves nothing behind.
    #[test]
    fn a_thread_that_cancels_itself_runs_to_its_next_yield_and_is_closed()
    -> luau::runtime::Result<()> {
        let (vm, scheduler) = vm_with_waits()?;
        let spawn = |source| spawn_chunk(&vm, &scheduler, CallContext::default(), source);
        let yielding = spawn(
            r#"
                yielding = coroutine.running()
                cancel(yielding)
                reached = 1
                coroutine.yield()
                went_on = true
            "#,
        )?;
        let waiting = spawn(
            r#"
                waiting = coroutine.running()
                cancel(waiting)
                reached += 1
                wait()
                went_on = true
            "#,
        )?;
        spawn(
            r#"
                statuses = coroutine.status(yielding) .. " " .. coroutine.status(waiting)
                resumed = coroutine.resume(yielding) or coroutine.resume(waiting)
            "#,
        )?;
        let tasks = [&yielding, &waiting]
            .map(|thread| scheduler.luau_thread_handle(thread).expect("its task").id());

        scheduler.poll_ready();

        assert_eq!(
            eval(&vm, "return reached, statuses, resumed")?,
            vec![
                luau::Value::Number(2.0),
                luau::Value::String(b"dead dead".to_vec()),
                luau::Value::Boolean(false),
            ]
        );
        assert_closed(&vm, &yielding);
        assert_closed(&vm, &waiting);
        assert!(holds_nothing(&scheduler));
        for task in tasks {
            assert_eq!(
                scheduler.snapshot(task).map(|task| task.state),
                Some(TaskState::Cancelled)
            );
        }
        Ok(())
    }

    /// Cancelled by `Thread::cancel` alone, which the scheduler doesn't hear of: the thread's
    /// task still ends, as cancelled, once the thread is closed where it yields.
    #[test]
    fn a_thread_closed_where_it_yields_ends_its_task() -> luau::runtime::Result<()> {
        let (vm, scheduler) = vm_with_waits()?;
        let cancel_thread: luau::NativeFn = Arc::new(|frame| {
            frame.thread.cancel(frame.vm)?;
            Ok(())
        });
        let cancel_thread = vm.create_function_with_options(
            luau::NativeFunctionOptions::new(luau::ChunkOrigin::default())
                .function_name("cancel_thread"),
            cancel_thread,
        )?;
        vm.set_global_function("cancel_thread", &cancel_thread)?;
        let completion = Rc::new(LocalLuauTaskCompletion::default());
        let thread = chunk_thread(&vm, "cancel_thread() coroutine.yield() went_on = true")?;
        scheduler.schedule_luau_thread_with_completion(
            CallContext::default(),
            vm.clone(),
            thread.clone(),
            Vec::new(),
            completion.clone(),
        );

        scheduler.poll_ready();

        let ended = completion_message(&completion).expect("the thread's task ended");
        assert!(ended.is_err_and(|error| error.contains("cancelled")));
        assert_closed(&vm, &thread);
        assert!(holds_nothing(&scheduler));
        Ok(())
    }

    /// The task borrows the coroutine for the one wait: cancelling it leaves the coroutine with
    /// nothing to resume it, so it is closed like a thread the scheduler runs.
    #[test]
    fn a_coroutine_cancelled_in_a_host_wait_is_closed() -> luau::runtime::Result<()> {
        let (vm, scheduler) = vm_with_waits()?;
        let group = scheduler.create_group();
        spawn_chunk(
            &vm,
            &scheduler,
            CallContext {
                task_group: group,
                ..CallContext::default()
            },
            r#"
                by_thread = coroutine.create(function() wait() went_on = true end)
                by_group = coroutine.create(function() wait() went_on = true end)
                coroutine.resume(by_thread)
                coroutine.resume(by_group)
            "#,
        )?;
        scheduler.poll_ready();
        let coroutine = |name| -> luau::runtime::Result<luau::Thread> {
            match eval(&vm, name)?.pop() {
                Some(luau::Value::Thread(thread)) => Ok(thread),
                other => panic!("expected a thread, got {other:?}"),
            }
        };
        let (by_thread, by_group) = (
            coroutine("return by_thread")?,
            coroutine("return by_group")?,
        );
        assert_eq!(
            eval(
                &vm,
                "return coroutine.status(by_thread), coroutine.status(by_group)"
            )?,
            vec![
                luau::Value::String(b"suspended".to_vec()),
                luau::Value::String(b"suspended".to_vec()),
            ]
        );

        assert!(scheduler.cancel_luau_thread(&by_thread));
        assert_closed(&vm, &by_thread);
        scheduler.cancel_group(group);
        assert_closed(&vm, &by_group);
        Ok(())
    }

    /// The coroutine's wait is a task of its own, in its group and not under the thread that
    /// started it.
    #[test]
    fn cancelling_a_thread_leaves_a_coroutine_it_started_waiting_on_a_call_of_its_own()
    -> luau::runtime::Result<()> {
        let (vm, scheduler) = vm_with_waits()?;
        let thread = spawn_chunk(
            &vm,
            &scheduler,
            CallContext::default(),
            r#"
                local started = coroutine.create(function() pause() outlived = true end)
                coroutine.resume(started)
                wait()
                went_on = true
            "#,
        )?;
        scheduler.poll_ready();

        assert!(scheduler.cancel_luau_thread(&thread));
        scheduler.poll_ready();

        assert_closed(&vm, &thread);
        assert_eq!(
            eval(&vm, "return outlived")?,
            vec![luau::Value::Boolean(true)]
        );
        Ok(())
    }

    /// The thread parks on a call in the round its group is cancelled, after it ran: the call's
    /// future never becomes its work.
    #[test]
    fn a_thread_whose_wait_is_refused_for_a_cancelled_group_is_closed() -> luau::runtime::Result<()>
    {
        let (vm, scheduler) = vm_with_waits()?;
        let group = scheduler.create_group();
        let thread = spawn_chunk(
            &vm,
            &scheduler,
            CallContext {
                task_group: group,
                ..CallContext::default()
            },
            "wait() went_on = true",
        )?;
        scheduler.schedule_future(CallContext::default(), {
            let scheduler = Rc::downgrade(&scheduler);
            async move {
                scheduler
                    .upgrade()
                    .expect("the scheduler is polling")
                    .cancel_group(group);
                Ok(())
            }
        });

        scheduler.poll_ready();

        assert_closed(&vm, &thread);
        assert!(!scheduler.has_pending());
        Ok(())
    }

    /// Work a poll put off and never took up, as when it unwound, is cancelled where it lies.
    #[test]
    fn cancelling_work_the_scheduler_has_not_taken_up_closes_its_thread()
    -> luau::runtime::Result<()> {
        let (vm, scheduler) = vm_with_waits()?;
        let unstarted = chunk_thread(&vm, "went_on = true")?;
        let suspended = chunk_thread(&vm, "coroutine.yield() went_on = true")?;
        suspended.resume(&vm, &[])?;
        let dropped = Rc::new(Cell::new(false));
        {
            let _polling = Polling::begin(&scheduler.polling);
            scheduler.schedule_luau_thread(
                CallContext::default(),
                vm.clone(),
                unstarted.clone(),
                Vec::new(),
            );
            let held = OnDrop({
                let (vm, suspended, dropped) = (vm.clone(), suspended.clone(), dropped.clone());
                move || {
                    assert!(
                        suspended.resume(&vm, &[]).is_err(),
                        "closed after its work dropped"
                    );
                    dropped.set(true);
                }
            });
            scheduler.schedule_luau_future(
                CallContext::default(),
                vm.clone(),
                suspended.clone(),
                luau::ScheduledFuture::new(async move {
                    let _held = held;
                    pending::<luau::runtime::Result<()>>().await
                }),
            );
        }

        assert!(scheduler.cancel_luau_thread(&unstarted));
        assert!(scheduler.cancel_luau_thread(&suspended));

        assert!(dropped.get());
        assert_closed(&vm, &unstarted);
        assert_closed(&vm, &suspended);
        scheduler.poll_ready();
        assert!(scheduler.snapshots().is_empty());
        Ok(())
    }

    /// Whether the scheduler holds nothing for any thread: no pending task, no wait parked or put
    /// off.
    fn holds_nothing(scheduler: &LocalScheduler) -> bool {
        !scheduler.has_pending()
            && scheduler.parked_threads.borrow().is_empty()
            && scheduler.pending_luau_work.borrow().is_empty()
    }

    /// Luau code resumed the coroutine itself while it was parked in a host wait, and the
    /// coroutine is cancelled inside that resume, outside any poll. It can't be closed while it
    /// runs, and its next yield would hand it back to Luau code and not to the scheduler: it runs
    /// on to that yield and is closed there, with nothing scheduled for a wait it asked for.
    #[test]
    fn a_thread_cancelled_while_luau_code_runs_it_is_closed_where_it_yields()
    -> luau::runtime::Result<()> {
        for yields in [
            "coroutine.yield('lost')",
            "wait()",
            "pause()",
            "pcall(coroutine.yield)",
            "pcall(wait)",
            "pcall(pause)",
            "while true do pcall(wait) end",
        ] {
            let (vm, scheduler) = vm_with_waits()?;
            let source = format!(
                r#"
                    co = coroutine.create(function()
                        wait()
                        cancel(coroutine.running())
                        reached = true
                        {yields}
                        went_on = true
                    end)
                    coroutine.resume(co)
                "#
            );
            vm.eval(
                Arc::<[u8]>::from(source.as_bytes()),
                luau::ChunkOrigin::default(),
            )?;
            assert!(scheduler.has_pending());

            let resumed = eval(
                &vm,
                "local resumed = table.pack(coroutine.resume(co)) return resumed.n, resumed[1], reached",
            )?;

            assert_eq!(
                resumed,
                [
                    luau::Value::Number(1.0),
                    luau::Value::Boolean(true),
                    luau::Value::Boolean(true)
                ],
                "{yields}"
            );
            let Some(luau::Value::Thread(co)) = eval(&vm, "return co")?.pop() else {
                panic!("expected a thread");
            };
            assert_closed(&vm, &co);
            assert!(holds_nothing(&scheduler), "{yields}");
            scheduler.poll_ready();
            assert!(
                scheduler
                    .snapshots()
                    .iter()
                    .all(|task| task.state == TaskState::Cancelled),
                "{yields}: the wait left work behind"
            );
        }
        Ok(())
    }

    /// As above inside a poll, from the thread the scheduler resumes: it resumes the parked
    /// coroutine, which is cancelled there and would yield back to that thread, which goes on.
    #[test]
    fn a_thread_cancelled_while_another_task_s_code_runs_it_is_closed_where_it_yields()
    -> luau::runtime::Result<()> {
        for yields in ["coroutine.yield('lost')", "while true do pcall(wait) end"] {
            let (vm, scheduler) = vm_with_waits()?;
            let completion = Rc::new(LocalLuauTaskCompletion::default());
            let source = format!(
                r#"
                    co = coroutine.create(function()
                        wait()
                        cancel(coroutine.running())
                        {yields}
                        went_on = true
                    end)
                    coroutine.resume(co)
                    pause()
                    resumed = table.pack(coroutine.resume(co))
                    again = coroutine.resume(co)
                    resumer_went_on = true
                "#
            );
            scheduler.schedule_luau_thread_with_completion(
                CallContext::default(),
                vm.clone(),
                vm.create_thread(&vm.load_chunk(&luau::Chunk::new(
                    Arc::<[u8]>::from(source.as_bytes()),
                    luau::ChunkOrigin::default(),
                ))?)?,
                Vec::new(),
                completion.clone(),
            );

            scheduler.poll_ready();
            scheduler.poll_ready();

            assert_eq!(completion_message(&completion), Some(Ok(())), "{yields}");
            assert_eq!(
                eval(
                    &vm,
                    "return resumed.n, resumed[1], again, coroutine.status(co), went_on, resumer_went_on"
                )?,
                vec![
                    luau::Value::Number(1.0),
                    luau::Value::Boolean(true),
                    luau::Value::Boolean(false),
                    luau::Value::String(b"dead".to_vec()),
                    luau::Value::Nil,
                    luau::Value::Boolean(true),
                ],
                "{yields}"
            );
            assert!(holds_nothing(&scheduler), "{yields}");
        }
        Ok(())
    }

    /// The scheduler resumes the coroutine it borrowed for a wait, which cancels itself and
    /// yields: it is not handed back to the code that made it.
    #[test]
    fn a_borrowed_coroutine_that_cancels_itself_is_closed_as_it_yields() -> luau::runtime::Result<()>
    {
        let (vm, scheduler) = vm_with_waits()?;
        eval(
            &vm,
            r#"
                co = coroutine.create(function()
                    pause()
                    cancel(coroutine.running())
                    reached = true
                    coroutine.yield()
                    went_on = true
                end)
                coroutine.resume(co)
            "#,
        )?;

        scheduler.poll_ready();

        assert_eq!(
            eval(&vm, "return reached")?,
            vec![luau::Value::Boolean(true)]
        );
        let Some(luau::Value::Thread(co)) = eval(&vm, "return co")?.pop() else {
            panic!("expected a thread");
        };
        assert_closed(&vm, &co);
        let cancelled = scheduler
            .snapshots()
            .iter()
            .filter(|task| task.state == TaskState::Cancelled)
            .count();
        assert_eq!(cancelled, 1);
        Ok(())
    }

    /// The canceller holds the thread and could resume it in the same breath: the thread is
    /// closed as it is cancelled, not when the poll ends.
    #[test]
    fn a_thread_cancelled_from_another_task_is_closed_before_that_task_goes_on()
    -> luau::runtime::Result<()> {
        let (vm, scheduler) = vm_with_waits()?;
        let waiting = spawn_chunk(
            &vm,
            &scheduler,
            CallContext::default(),
            "held = coroutine.running() wait() went_on = true",
        )?;
        scheduler.poll_ready();
        spawn_chunk(
            &vm,
            &scheduler,
            CallContext::default(),
            "cancel(held) status = coroutine.status(held) resumed = coroutine.resume(held)",
        )?;

        scheduler.poll_ready();

        assert_eq!(
            eval(&vm, "return status, resumed")?,
            vec![
                luau::Value::String(b"dead".to_vec()),
                luau::Value::Boolean(false),
            ]
        );
        assert_closed(&vm, &waiting);
        assert!(!scheduler.has_pending());
        Ok(())
    }

    /// The wait the thread is parked in cancels it, or its group, as it completes: the poll that
    /// finds the wait over must not go on to resume the thread.
    #[test]
    fn a_thread_cancelled_by_the_wait_that_wakes_it_is_not_resumed() -> luau::runtime::Result<()> {
        for wait in [
            "cancel_as_it_wakes() went_on = true",
            "cancel_group_as_it_wakes() went_on = true",
        ] {
            let (vm, scheduler) = vm_with_waits()?;
            let context = CallContext {
                task_group: scheduler.create_group(),
                ..CallContext::default()
            };
            let thread = spawn_chunk(&vm, &scheduler, context, wait)?;
            let task = scheduler.luau_thread_handle(&thread).expect("its task");

            scheduler.poll_ready();
            scheduler.poll_ready();

            assert_closed(&vm, &thread);
            assert_eq!(
                scheduler.snapshot(task.id()).map(|task| task.state),
                Some(TaskState::Cancelled),
                "{wait}"
            );
        }
        Ok(())
    }

    /// Sets the global `group` to `group`'s id, for `cancel_group`.
    fn set_group_global(vm: &luau::Vm, group: TaskGroupId) -> luau::runtime::Result<()> {
        vm.load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(&b"group = ..."[..]),
            luau::ChunkOrigin::default(),
        ))?
        .call(vm, &[luau::Value::Number(group.0 as f64)])?;
        Ok(())
    }

    /// The canceller holds a thread of the group and could resume it in the same breath: the
    /// group's threads are closed as it is cancelled, not when the poll ends.
    #[test]
    fn a_group_cancelled_from_a_task_closes_its_threads_before_that_task_goes_on()
    -> luau::runtime::Result<()> {
        let (vm, scheduler) = vm_with_waits()?;
        let group = scheduler.create_group();
        set_group_global(&vm, group)?;
        let in_group = || CallContext {
            task_group: group,
            ..CallContext::default()
        };
        let waiting = spawn_chunk(
            &vm,
            &scheduler,
            in_group(),
            "held = coroutine.running() wait() went_on = true",
        )?;
        let yielded = spawn_chunk(
            &vm,
            &scheduler,
            in_group(),
            "also_held = coroutine.running() coroutine.yield() went_on = true",
        )?;
        scheduler.poll_ready();
        spawn_chunk(
            &vm,
            &scheduler,
            CallContext::default(),
            r#"
                cancel_group(group)
                statuses = coroutine.status(held) .. " " .. coroutine.status(also_held)
                resumed = coroutine.resume(held) or coroutine.resume(also_held)
            "#,
        )?;

        scheduler.poll_ready();

        assert_eq!(
            eval(&vm, "return statuses, resumed")?,
            vec![
                luau::Value::String(b"dead dead".to_vec()),
                luau::Value::Boolean(false),
            ]
        );
        assert_closed(&vm, &waiting);
        assert_closed(&vm, &yielded);
        assert!(!scheduler.has_pending());
        Ok(())
    }

    /// The thread that cancels its own group is the one the scheduler is resuming: it goes on to
    /// where it yields, or calls what would wait, as one that cancels itself does. The task after
    /// it holds the thread and could resume it in the same round: it is closed by then.
    #[test]
    fn a_thread_that_cancels_its_group_runs_to_its_next_yield_and_is_closed()
    -> luau::runtime::Result<()> {
        for yields in [
            "coroutine.yield()",
            "wait()",
            "pcall(wait)",
            "while true do pcall(wait) end",
        ] {
            let (vm, scheduler) = vm_with_waits()?;
            let group = scheduler.create_group();
            set_group_global(&vm, group)?;
            let source = format!(
                r#"
                    held = coroutine.running()
                    cancel_group(group)
                    reached = true
                    {yields}
                    went_on = true
                "#
            );
            let thread = vm.create_thread(&vm.load_chunk(&luau::Chunk::new(
                Arc::<[u8]>::from(source.as_bytes()),
                luau::ChunkOrigin::default(),
            ))?)?;
            let completion = Rc::new(LocalLuauTaskCompletion::default());
            scheduler.schedule_luau_thread_with_completion(
                CallContext {
                    task_group: group,
                    ..CallContext::default()
                },
                vm.clone(),
                thread.clone(),
                Vec::new(),
                completion.clone(),
            );
            spawn_chunk(
                &vm,
                &scheduler,
                CallContext::default(),
                "status = coroutine.status(held) resumed = coroutine.resume(held)",
            )?;

            scheduler.poll_ready();

            assert_eq!(
                eval(&vm, "return reached, status, resumed")?,
                vec![
                    luau::Value::Boolean(true),
                    luau::Value::String(b"dead".to_vec()),
                    luau::Value::Boolean(false),
                ],
                "{yields}"
            );
            let ended = completion_message(&completion).expect("the thread's task ended");
            assert!(
                ended.is_err_and(|error| error.contains("cancelled")),
                "{yields}"
            );
            assert_closed(&vm, &thread);
            assert!(holds_nothing(&scheduler), "{yields}");
        }
        Ok(())
    }

    /// The cancelled thread is cancelled by a coroutine it resumed. That coroutine, and what
    /// else runs beneath the cancelled thread, is not cancelled with it: it yields back, waits on
    /// a host call of its own and is woken from it.
    #[test]
    fn the_coroutines_a_cancelled_thread_resumed_go_on() -> luau::runtime::Result<()> {
        let (vm, scheduler) = vm_with_waits()?;
        eval(
            &vm,
            r#"
                co = coroutine.create(function()
                    wait()
                    worker = coroutine.create(function()
                        cancel(co)
                        coroutine.yield("yielded")
                        pause()
                        worker_went_on = true
                    end)
                    local _, yielded = coroutine.resume(worker)
                    reached = yielded
                    coroutine.resume(worker)
                    wait()
                    went_on = true
                end)
                coroutine.resume(co)
            "#,
        )?;

        let resumed = eval(
            &vm,
            "return coroutine.resume(co), reached, coroutine.status(worker)",
        )?;

        assert_eq!(resumed[0], luau::Value::Boolean(true), "{resumed:?}");
        assert_eq!(
            resumed[1..],
            [
                luau::Value::String(b"yielded".to_vec()),
                luau::Value::String(b"suspended".to_vec()),
            ]
        );
        let Some(luau::Value::Thread(co)) = eval(&vm, "return co")?.pop() else {
            panic!("expected a thread");
        };
        assert_closed(&vm, &co);
        assert!(scheduler.has_pending(), "the worker's wait went with it");

        scheduler.poll_ready();

        assert_eq!(
            eval(&vm, "return worker_went_on, coroutine.status(worker)")?,
            vec![
                luau::Value::Boolean(true),
                luau::Value::String(b"dead".to_vec())
            ]
        );
        Ok(())
    }

    /// A poll that unwinds leaves a thread parked with its wait not yet taken up. Cancelling the
    /// thread closes it and drops the wait, which would otherwise start it again as a new task.
    #[test]
    fn a_thread_left_parked_by_a_poll_that_unwound_is_closed_with_its_wait()
    -> luau::runtime::Result<()> {
        struct PanickingWake;
        impl std::task::Wake for PanickingWake {
            fn wake(self: Arc<Self>) {
                panic!("the waker panics");
            }
        }

        let (vm, scheduler) = vm_with_waits()?;
        let parked = spawn_chunk(
            &vm,
            &scheduler,
            CallContext::default(),
            "wait() went_on = true",
        )?;
        let completion = Rc::new(LocalLuauTaskCompletion::default());
        scheduler.schedule_luau_thread_with_completion(
            CallContext::default(),
            vm.clone(),
            chunk_thread(&vm, "return 42")?,
            Vec::new(),
            completion.clone(),
        );
        let waker = Waker::from(Arc::new(PanickingWake));
        assert!(
            completion
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let poll = catch_unwind(AssertUnwindSafe(|| scheduler.poll_ready()));
        assert!(poll.is_err(), "the poll didn't unwind");
        assert_eq!(scheduler.pending_luau_work.borrow().len(), 1);

        let task = scheduler.luau_thread_handle(&parked).expect("its task");

        assert!(scheduler.cancel_luau_thread(&parked));

        assert_closed(&vm, &parked);
        assert!(scheduler.pending_luau_work.borrow().is_empty());
        let tasks = scheduler.snapshots().len();
        scheduler.poll_ready();
        assert_eq!(scheduler.snapshots().len(), tasks, "its wait became a task");
        assert_eq!(
            scheduler.snapshot(task.id()).map(|task| task.state),
            Some(TaskState::Cancelled)
        );
        Ok(())
    }

    /// Runs a round of `scheduler` that unwinds after the tasks scheduled so far ran, as one does
    /// when a completion's waker panics. What they put off is left for a later round.
    fn unwind_a_round(vm: &luau::Vm, scheduler: &LocalScheduler) -> luau::runtime::Result<()> {
        struct PanickingWake;
        impl std::task::Wake for PanickingWake {
            fn wake(self: Arc<Self>) {
                panic!("the waker panics");
            }
        }

        let completion = Rc::new(LocalLuauTaskCompletion::default());
        scheduler.schedule_luau_thread_with_completion(
            CallContext::default(),
            vm.clone(),
            chunk_thread(vm, "return 42")?,
            Vec::new(),
            completion.clone(),
        );
        let waker = Waker::from(Arc::new(PanickingWake));
        assert!(
            completion
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let poll = catch_unwind(AssertUnwindSafe(|| scheduler.poll_ready()));
        assert!(poll.is_err(), "the poll didn't unwind");
        Ok(())
    }

    /// A waker that notes it was woken.
    #[derive(Default)]
    struct NotedWake(AtomicBool);

    impl std::task::Wake for NotedWake {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::Release);
        }
    }

    /// Waits on `completion`, which is pending, and returns what notes its wake.
    fn wait_on(completion: &LocalLuauTaskCompletion) -> Arc<NotedWake> {
        let noted = Arc::new(NotedWake::default());
        let waker = Waker::from(noted.clone());
        assert!(
            completion
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        noted
    }

    /// As above for a group: what the unwound poll put off for it, a parked thread's wait,
    /// threads not yet started and a future, goes with it, and its threads are closed, at once.
    /// A start with a completion tells it the thread was cancelled, as one cancelled by itself
    /// does.
    #[test]
    fn cancelling_a_group_closes_and_drops_the_work_a_poll_that_unwound_put_off()
    -> luau::runtime::Result<()> {
        let (vm, scheduler) = vm_with_waits()?;
        let group = scheduler.create_group();
        let in_group = CallContext {
            task_group: group,
            ..CallContext::default()
        };
        let parked = spawn_chunk(&vm, &scheduler, in_group.clone(), "wait() went_on = true")?;
        let [unstarted, completed, limited, alone] =
            ["went_on = true"; 4].map(|source| chunk_thread(&vm, source).expect("a thread"));
        let completions: [Rc<LocalLuauTaskCompletion>; 3] = Default::default();
        let other = chunk_thread(&vm, "return 42")?;
        let dropped = Rc::new(Cell::new(false));
        scheduler.schedule_future(CallContext::default(), {
            let scheduler = Rc::downgrade(&scheduler);
            let (vm, other) = (vm.clone(), other.clone());
            let threads = [&unstarted, &completed, &limited, &alone].map(Clone::clone);
            let completions = completions.clone();
            let dropped = OnDrop({
                let dropped = dropped.clone();
                move || dropped.set(true)
            });
            async move {
                let scheduler = scheduler.upgrade().expect("the scheduler is polling");
                let [unstarted, completed, limited, alone] = threads;
                let [to_completed, to_limited, to_alone] = completions;
                scheduler.schedule_luau_thread(in_group.clone(), vm.clone(), unstarted, Vec::new());
                scheduler.schedule_luau_thread_with_completion(
                    in_group.clone(),
                    vm.clone(),
                    completed,
                    Vec::new(),
                    to_completed,
                );
                scheduler.schedule_luau_thread_with_limit_and_completion(
                    in_group.clone(),
                    vm.clone(),
                    limited,
                    Vec::new(),
                    ResumeLimit::Budget(Duration::from_secs(60)),
                    to_limited,
                );
                scheduler.schedule_luau_thread_with_completion(
                    CallContext::default(),
                    vm.clone(),
                    alone,
                    Vec::new(),
                    to_alone,
                );
                scheduler.schedule_luau_thread(CallContext::default(), vm, other, Vec::new());
                scheduler.schedule_future(in_group, async move {
                    let _dropped = dropped;
                    Ok(())
                });
                Ok(())
            }
        });
        unwind_a_round(&vm, &scheduler)?;
        assert_eq!(scheduler.pending_luau_work.borrow().len(), 6);
        assert_eq!(scheduler.pending_futures.borrow().len(), 1);
        let woken = completions.each_ref().map(|completion| wait_on(completion));

        assert!(scheduler.cancel_luau_thread(&alone));
        assert_closed(&vm, &alone);
        scheduler.cancel_group(group);

        for thread in [&parked, &unstarted, &completed, &limited] {
            assert_closed(&vm, thread);
        }
        for (completion, woken) in completions.iter().zip(&woken) {
            assert!(woken.0.load(Ordering::Acquire), "a waiter wasn't woken");
            let ended = completion_message(completion).expect("the start was cancelled");
            assert!(ended.is_err_and(|error| error.contains("cancelled")));
        }
        assert!(dropped.get(), "the group's future was kept");
        assert!(scheduler.pending_futures.borrow().is_empty());
        assert_eq!(scheduler.pending_luau_work.borrow().len(), 1);
        scheduler.poll_ready();
        assert_eq!(
            scheduler
                .luau_thread_handle(&other)
                .and_then(|task| scheduler.snapshot(task.id()))
                .map(|task| task.state),
            Some(TaskState::Completed),
            "another group's thread went with it"
        );
        Ok(())
    }

    /// Two of the waits the unwound poll put off for the group panic as they drop: each fails
    /// alone, and the rest of the group's work is still closed and dropped.
    #[test]
    fn cancelling_a_group_outlives_put_off_work_that_panics_as_it_drops()
    -> luau::runtime::Result<()> {
        let (vm, scheduler) = vm_with_waits()?;
        let group = scheduler.create_group();
        let in_group = CallContext {
            task_group: group,
            ..CallContext::default()
        };
        let suspended = [(); 2].map(|()| {
            let Some(luau::Value::Thread(thread)) = eval(
                &vm,
                "local co = coroutine.create(coroutine.yield) coroutine.resume(co) return co",
            )
            .expect("a coroutine")
            .pop() else {
                panic!("expected a thread");
            };
            thread
        });
        let unstarted = chunk_thread(&vm, "went_on = true")?;
        let dropped = Rc::new(Cell::new(false));
        scheduler.schedule_future(CallContext::default(), {
            let scheduler = Rc::downgrade(&scheduler);
            let (vm, suspended, unstarted) = (vm.clone(), suspended.clone(), unstarted.clone());
            let dropped = OnDrop({
                let dropped = dropped.clone();
                move || dropped.set(true)
            });
            async move {
                let scheduler = scheduler.upgrade().expect("the scheduler is polling");
                for thread in suspended {
                    let held = OnDrop(|| panic!("drop panicked"));
                    scheduler.schedule_luau_future(
                        in_group.clone(),
                        vm.clone(),
                        thread,
                        luau::ScheduledFuture::new(async move {
                            let _held = held;
                            pending::<luau::runtime::Result<()>>().await
                        }),
                    );
                }
                scheduler.schedule_luau_thread(in_group.clone(), vm, unstarted, Vec::new());
                scheduler.schedule_future(in_group, async move {
                    let _dropped = dropped;
                    Ok(())
                });
                Ok(())
            }
        });
        unwind_a_round(&vm, &scheduler)?;
        assert_eq!(scheduler.pending_luau_work.borrow().len(), 3);

        scheduler.cancel_group(group);

        for thread in suspended.iter().chain([&unstarted]) {
            assert_closed(&vm, thread);
        }
        assert!(scheduler.pending_luau_work.borrow().is_empty());
        assert!(dropped.get(), "the group's future was kept");
        Ok(())
    }

    #[test]
    fn a_poll_that_unwinds_leaves_the_scheduler_taking_work_at_once() -> luau::runtime::Result<()> {
        struct PanickingWake;
        impl std::task::Wake for PanickingWake {
            fn wake(self: Arc<Self>) {
                panic!("the waker panics");
            }
        }

        let vm = luau::Vm::new()?;
        let function = vm.load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(&b"return 42"[..]),
            luau::ChunkOrigin::default(),
        ))?;
        let thread = vm.create_thread(&function)?;
        let scheduler = LocalScheduler::new();
        let completion = Rc::new(LocalLuauTaskCompletion::default());
        scheduler.schedule_luau_thread_with_completion(
            CallContext::default(),
            vm,
            thread,
            Vec::new(),
            completion.clone(),
        );
        let waker = Waker::from(Arc::new(PanickingWake));
        assert!(
            completion
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let poll = catch_unwind(AssertUnwindSafe(|| scheduler.poll_ready()));
        assert!(poll.is_err(), "the poll didn't unwind");

        scheduler.schedule_future(CallContext::default(), async { Ok(()) });
        assert_eq!(
            scheduler.snapshots().len(),
            2,
            "the scheduler put the task off as if it were still polling"
        );
        Ok(())
    }

    #[test]
    #[should_panic(expected = "a task's result was already read")]
    fn a_completion_read_twice_panics() {
        let vm = luau::Vm::new().expect("vm");
        let function = vm
            .load_chunk(&luau::Chunk::new(
                Arc::<[u8]>::from(&b"return 42"[..]),
                luau::ChunkOrigin::default(),
            ))
            .expect("chunk");
        let thread = vm.create_thread(&function).expect("thread");
        let scheduler = LocalScheduler::new();
        let completion = Rc::new(LocalLuauTaskCompletion::default());
        scheduler.schedule_luau_thread_with_completion(
            CallContext::default(),
            vm,
            thread,
            Vec::new(),
            completion.clone(),
        );
        scheduler.poll_ready();

        let mut cx = Context::from_waker(Waker::noop());
        assert!(completion.poll(&mut cx).is_ready());
        let _ = completion.poll(&mut cx);
    }

    struct OnDrop<F: FnMut()>(F);

    impl<F: FnMut()> Drop for OnDrop<F> {
        fn drop(&mut self) {
            (self.0)();
        }
    }

    /// A thread that waits forever on a host call, whose future holds `held`.
    fn thread_holding(
        vm: &luau::Vm,
        scheduler: &LocalScheduler,
        held: impl Sized + 'static,
    ) -> luau::runtime::Result<luau::Thread> {
        let thread = vm.create_thread(&vm.load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(&b"coroutine.yield()"[..]),
            luau::ChunkOrigin::default(),
        ))?)?;
        scheduler.spawn_luau_thread(
            CallContext::default(),
            vm.clone(),
            thread.clone(),
            Vec::new(),
        );
        scheduler.poll_ready();
        scheduler.schedule_luau_future(
            CallContext::default(),
            vm.clone(),
            thread.clone(),
            luau::ScheduledFuture::new(async move {
                let _held = held;
                pending::<luau::runtime::Result<()>>().await
            }),
        );
        Ok(thread)
    }

    #[test]
    fn cancelled_work_may_call_the_scheduler_as_it_drops() -> luau::runtime::Result<()> {
        let vm = luau::Vm::new()?;
        let scheduler = Rc::new(LocalScheduler::new());
        let other = vm.create_thread(&vm.load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(&b"ran = true"[..]),
            luau::ChunkOrigin::default(),
        ))?)?;
        let group = scheduler.create_group();
        let spawned = Rc::new(Cell::new(false));
        let thread = thread_holding(
            &vm,
            &scheduler,
            OnDrop({
                let scheduler = Rc::downgrade(&scheduler);
                let (other, spawned) = (other.clone(), spawned.clone());
                move || {
                    let scheduler = scheduler.upgrade().expect("the scheduler is alive");
                    scheduler.cancel_luau_thread(&other);
                    scheduler.cancel_group(group);
                    let spawned = spawned.clone();
                    scheduler.schedule_future(CallContext::default(), async move {
                        spawned.set(true);
                        Ok(())
                    });
                }
            }),
        )?;
        scheduler.poll_ready();
        scheduler.spawn_luau_thread(
            CallContext::default(),
            vm.clone(),
            other.clone(),
            Vec::new(),
        );
        scheduler.schedule_watcher(CallContext::default(), {
            let scheduler = Rc::downgrade(&scheduler);
            async move {
                scheduler
                    .upgrade()
                    .expect("the scheduler is polling")
                    .cancel_luau_thread(&thread);
                Ok(())
            }
        });

        scheduler.poll_ready();
        assert!(!ran(&vm)?, "the thread its drop cancelled ran");
        assert!(scheduler.is_group_cancelled(group));
        assert!(spawned.get(), "the task its drop spawned didn't run");
        Ok(())
    }

    #[test]
    fn cancelled_work_panicking_as_it_drops_fails_nothing_else() -> luau::runtime::Result<()> {
        let vm = luau::Vm::new()?;
        let scheduler = Rc::new(LocalScheduler::new());
        let panicking = thread_holding(&vm, &scheduler, OnDrop(|| panic!("drop panicked")))?;
        let dropped = Rc::new(Cell::new(false));
        let quiet = thread_holding(
            &vm,
            &scheduler,
            OnDrop({
                let dropped = dropped.clone();
                move || dropped.set(true)
            }),
        )?;
        scheduler.poll_ready();
        let settled = Rc::new(Cell::new(false));
        scheduler.schedule_watcher(CallContext::default(), {
            let scheduler = Rc::downgrade(&scheduler);
            let settled = settled.clone();
            async move {
                let scheduler = scheduler.upgrade().expect("the scheduler is polling");
                let gone = scheduler.cancel_luau_thread_and_settle(&panicking);
                scheduler.cancel_luau_thread(&quiet);
                drop(scheduler);
                gone.await;
                settled.set(true);
                Ok(())
            }
        });

        run_until(&scheduler, || settled.get());
        assert!(dropped.get(), "the cancel after the panicking one was lost");
        assert!(scheduler.cancelling.borrow().is_empty());
        Ok(())
    }

    #[test]
    fn a_task_cancelling_a_thread_goes_on_once_the_thread_s_work_is_dropped()
    -> luau::runtime::Result<()> {
        let vm = luau::Vm::new()?;
        let scheduler = Rc::new(LocalScheduler::new());
        let dropped = Arc::new(AtomicBool::new(false));
        let thread = waiting_thread(&vm, &scheduler, CallContext::default(), dropped.clone())?;
        scheduler.poll_ready();
        assert!(!dropped.load(Ordering::Acquire));

        let dropped_when_settled = Rc::new(Cell::new(None));
        scheduler.schedule_future(CallContext::default(), {
            let scheduler = Rc::downgrade(&scheduler);
            let (dropped, dropped_when_settled) = (dropped.clone(), dropped_when_settled.clone());
            async move {
                let settled = scheduler
                    .upgrade()
                    .expect("the scheduler is polling")
                    .cancel_luau_thread_and_settle(&thread);
                assert!(
                    !dropped.load(Ordering::Acquire),
                    "the poll defers the cancel"
                );
                settled.await;
                dropped_when_settled.set(Some(dropped.load(Ordering::Acquire)));
                Ok(())
            }
        });

        run_until(&scheduler, || dropped_when_settled.get().is_some());
        assert_eq!(dropped_when_settled.get(), Some(true));
        Ok(())
    }

    #[test]
    fn a_group_cancelled_from_a_task_runs_no_further() -> luau::runtime::Result<()> {
        let vm = luau::Vm::new()?;
        let scheduler = Rc::new(LocalScheduler::new());
        let group = scheduler.create_group();
        let context = CallContext {
            task_group: group,
            ..CallContext::default()
        };
        let dropped = Arc::new(AtomicBool::new(false));
        waiting_thread(&vm, &scheduler, context.clone(), dropped.clone())?;
        scheduler.poll_ready();

        // The canceller runs first, and the group's own task is ready behind it.
        let ran = Rc::new(Cell::new(false));
        scheduler.schedule_future(CallContext::default(), {
            let scheduler = Rc::downgrade(&scheduler);
            async move {
                scheduler
                    .upgrade()
                    .expect("the scheduler is polling")
                    .cancel_group(group);
                Ok(())
            }
        });
        scheduler.schedule_future(context, {
            let ran = ran.clone();
            async move {
                ran.set(true);
                Ok(())
            }
        });

        scheduler.poll_ready();
        assert!(!ran.get(), "a task of the cancelled group still ran");
        assert!(dropped.load(Ordering::Acquire));
        assert!(scheduler.is_group_cancelled(group));
        assert!(!scheduler.has_pending());
        Ok(())
    }
}
