use std::{
    collections::{
        HashMap,
        HashSet,
        VecDeque,
    },
    future::Future,
    pin::Pin,
    rc::Rc,
    sync::{
        Arc,
        Condvar,
        Mutex,
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

pub type TaskFuture = Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;

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
    result: RefCell<Option<std::result::Result<Vec<luau::Value>, String>>>,
    waker: RefCell<Option<Waker>>,
}

impl LocalLuauTaskCompletion {
    fn complete(&self, result: std::result::Result<Vec<luau::Value>, String>) {
        *self.result.borrow_mut() = Some(result);
        if let Some(waker) = self.waker.borrow_mut().take() {
            waker.wake();
        }
    }

    pub fn poll(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<Vec<luau::Value>, String>> {
        if let Some(result) = self.result.borrow_mut().take() {
            return Poll::Ready(result);
        }
        *self.waker.borrow_mut() = Some(cx.waker().clone());
        Poll::Pending
    }
}

struct LuauTaskOptions {
    resume_budget: Option<Duration>,
    completion: Option<Rc<LocalLuauTaskCompletion>>,
}

enum LuauThreadStart {
    Immediate,
    Delayed(Duration),
    Budgeted {
        resume_budget: Duration,
        completion: Option<Rc<LocalLuauTaskCompletion>>,
    },
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
    sleeping: Vec<(Instant, TaskId)>,
    luau_resume_budget: Option<Duration>,
    wake_queue: Arc<WakeQueue>,
    tasks: HashMap<TaskId, Task>,
    groups: HashMap<TaskGroupId, HashSet<TaskId>>,
    /// Shared with [`LocalScheduler`], which reads it while this scheduler
    /// is borrowed for polling.
    cancelled_groups: Rc<RefCell<HashSet<TaskGroupId>>>,
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl Scheduler {
    pub fn new() -> Self {
        Self {
            next_task_id: 1,
            last_group_id: Rc::default(),
            ready: VecDeque::new(),
            sleeping: Vec::new(),
            luau_resume_budget: None,
            wake_queue: Arc::new(WakeQueue::default()),
            tasks: HashMap::new(),
            groups: HashMap::new(),
            cancelled_groups: Rc::default(),
        }
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
        future: impl Future<Output = Result<()>> + Send + 'static,
    ) -> TaskHandle {
        self.spawn_boxed(context, Box::pin(future))
    }

    pub fn spawn_boxed(&mut self, context: CallContext, future: TaskFuture) -> TaskHandle {
        self.insert_task(context, future, None)
    }

    pub fn spawn_after(
        &mut self,
        context: CallContext,
        delay: Duration,
        future: impl Future<Output = Result<()>> + Send + 'static,
    ) -> TaskHandle {
        self.spawn_boxed_after(context, delay, Box::pin(future))
    }

    pub fn spawn_boxed_after(
        &mut self,
        context: CallContext,
        delay: Duration,
        future: TaskFuture,
    ) -> TaskHandle {
        let wake_at = Instant::now()
            .checked_add(delay)
            .unwrap_or_else(Instant::now);
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
            LuauTaskOptions {
                resume_budget: self.luau_resume_budget,
                completion: None,
            },
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
        let wake_at = Instant::now()
            .checked_add(delay)
            .unwrap_or_else(Instant::now);
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
        let owner = self.tasks[&id].context.task_group;
        if self.is_cancelled(owner) || self.is_cancelled(awaiting) {
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
        )
    }

    fn insert_work(
        &mut self,
        context: CallContext,
        work: TaskWork,
        wake_at: Option<Instant>,
        options: LuauTaskOptions,
    ) -> TaskHandle {
        let id = TaskId(self.next_task_id);
        self.next_task_id = self.next_task_id.saturating_add(1);
        let group = context.task_group;
        let handle = TaskHandle { id };

        self.groups.entry(group).or_default().insert(id);
        self.tasks.insert(
            id,
            Task {
                awaiting: None,
                borrows_thread: false,
                context,
                work: Some(work),
                state: TaskState::Pending,
                error: None,
                output: None,
                wake_at,
                luau_resume_budget: options.resume_budget,
                completion: options.completion,
            },
        );
        if self.is_cancelled(group) {
            self.cancel(id);
        } else if let Some(wake_at) = wake_at {
            self.sleeping.push((wake_at, id));
        } else {
            self.ready.push_back(id);
        }

        handle
    }

    fn default_luau_task_options(&self) -> LuauTaskOptions {
        LuauTaskOptions {
            resume_budget: self.luau_resume_budget,
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
        task.work = None;
        task.state = TaskState::Cancelled;
        task.wake_at = None;
        Some(args)
    }

    pub fn cancel(&mut self, id: TaskId) -> bool {
        let Some(task) = self.tasks.get_mut(&id) else {
            return false;
        };
        if task.state != TaskState::Pending {
            return false;
        }

        task.work = None;
        task.state = TaskState::Cancelled;
        task.wake_at = None;
        if let Some(completion) = task.completion.as_ref() {
            completion.complete(Err(format!("Luau task {} was cancelled", id.0)));
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
        let Some(tasks) = self.groups.get(&group) else {
            return Vec::new();
        };

        let ids: Vec<_> = tasks.iter().copied().collect();
        ids.into_iter().filter(|id| self.cancel(*id)).collect()
    }

    pub fn poll_ready(&mut self) -> usize {
        self.poll_ready_at(Instant::now())
    }

    fn poll_ready_at(&mut self, now: Instant) -> usize {
        self.drain_wake_queue();
        self.wake_due(now);
        let mut completed = 0;

        while let Some(id) = self.ready.pop_front() {
            let Some(task) = self.tasks.get_mut(&id) else {
                continue;
            };
            if task.state != TaskState::Pending {
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
            match poll_work(&task.context, work, &mut cx, task.luau_resume_budget) {
                WorkPoll::Completed { output } => {
                    if let Some(completion) = task.completion.as_ref() {
                        completion.complete(Ok(output.unwrap_or_default()));
                        task.output = None;
                    } else {
                        task.output = output;
                    }
                    task.work = None;
                    task.state = TaskState::Completed;
                    task.wake_at = None;
                    completed += 1;
                }
                // Stays pending, without work, until the parked call's future
                // replaces it or a refused replacement cancels it.
                WorkPoll::Parked => {
                    task.work = None;
                    task.wake_at = None;
                    completed += 1;
                }
                WorkPoll::Cancelled => {
                    if let Some(completion) = task.completion.as_ref() {
                        completion.complete(Err(format!("Luau task {} was cancelled", id.0)));
                    }
                    task.work = None;
                    task.state = TaskState::Cancelled;
                    task.wake_at = None;
                    completed += 1;
                }
                WorkPoll::Failed(error) => {
                    if let Some(completion) = task.completion.as_ref() {
                        completion.complete(Err(format!("Luau task {} failed: {error}", id.0)));
                    }
                    task.work = None;
                    task.state = TaskState::Failed;
                    task.wake_at = None;
                    task.error = Some(Arc::from(error));
                    completed += 1;
                }
                WorkPoll::Yielded if task.borrows_thread => {
                    task.work = None;
                    task.state = TaskState::Completed;
                    task.wake_at = None;
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
            if self
                .tasks
                .get(&id)
                .is_some_and(|task| task.state == TaskState::Pending)
            {
                self.ready.push_back(id);
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

    pub fn next_wake_delay(&self) -> Option<Duration> {
        self.next_wake_delay_at(Instant::now())
    }

    fn next_wake_delay_at(&self, now: Instant) -> Option<Duration> {
        if !self.ready.is_empty() || self.wake_queue.has_pending_wake() {
            return Some(Duration::ZERO);
        }

        self.sleeping
            .iter()
            .map(|(wake_at, _)| wake_at.saturating_duration_since(now))
            .min()
    }

    pub fn wait_for_wake(&self, timeout: Option<Duration>) {
        self.wake_queue.wait(timeout);
    }

    fn borrows_thread(&self, id: TaskId) -> bool {
        self.tasks.get(&id).is_some_and(|task| task.borrows_thread)
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
        if let Some(previous) = previous
            && let Some(group_tasks) = self.groups.get_mut(&previous)
        {
            group_tasks.remove(&id);
        }
        if let Some(awaiting) = awaiting {
            self.groups.entry(awaiting).or_default().insert(id);
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
            && let Some(group_tasks) = self.groups.get_mut(&previous)
        {
            group_tasks.remove(&id);
        }
    }

    pub fn remove(&mut self, id: TaskId) -> bool {
        let Some(task) = self.tasks.remove(&id) else {
            return false;
        };

        for group in std::iter::once(task.context.task_group).chain(task.awaiting) {
            if let Some(group_tasks) = self.groups.get_mut(&group) {
                group_tasks.remove(&id);
            }
        }
        self.ready.retain(|ready_id| *ready_id != id);
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
        let Some(tasks) = self.groups.get(&group) else {
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

#[derive(Default)]
struct WakeQueue {
    ids: Mutex<VecDeque<TaskId>>,
    signal: Condvar,
}

impl WakeQueue {
    fn wake(&self, id: TaskId) {
        self.ids
            .lock()
            .expect("scheduler wake queue poisoned")
            .push_back(id);
        self.signal.notify_one();
    }

    fn drain(&self) -> Vec<TaskId> {
        self.ids
            .lock()
            .expect("scheduler wake queue poisoned")
            .drain(..)
            .collect()
    }

    fn has_pending_wake(&self) -> bool {
        !self
            .ids
            .lock()
            .expect("scheduler wake queue poisoned")
            .is_empty()
    }

    fn wait(&self, timeout: Option<Duration>) {
        let guard = self.ids.lock().expect("scheduler wake queue poisoned");
        if !guard.is_empty() {
            return;
        }

        match timeout {
            Some(timeout) => {
                drop(self.signal.wait_timeout(guard, timeout));
            }
            None => {
                drop(self.signal.wait(guard));
            }
        }
    }
}

struct TaskWaker {
    id: TaskId,
    queue: Arc<WakeQueue>,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.queue.wake(self.id);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.queue.wake(self.id);
    }
}

pub struct LocalScheduler {
    scheduler: RefCell<Scheduler>,
    cancelled_groups: Rc<RefCell<HashSet<TaskGroupId>>>,
    thread_tasks: RefCell<HashMap<(u64, usize), TaskHandle>>,
    task_threads: RefCell<HashMap<TaskId, (luau::Vm, luau::Thread)>>,
    parked_threads: RefCell<HashSet<(u64, usize)>>,
    pending_luau_work: RefCell<Vec<PendingLuauWork>>,
    pending_luau_cancellations: RefCell<Vec<luau::Thread>>,
    polling: Cell<bool>,
    last_group_id: Rc<Cell<u64>>,
}

impl Default for LocalScheduler {
    fn default() -> Self {
        let scheduler = Scheduler::new();
        Self {
            cancelled_groups: scheduler.cancelled_groups.clone(),
            last_group_id: scheduler.last_group_id.clone(),
            scheduler: RefCell::new(scheduler),
            thread_tasks: RefCell::default(),
            task_threads: RefCell::default(),
            parked_threads: RefCell::default(),
            pending_luau_work: RefCell::default(),
            pending_luau_cancellations: RefCell::default(),
            polling: Cell::default(),
        }
    }
}

impl LocalScheduler {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create_group(&self) -> TaskGroupId {
        Scheduler::next_group(&self.last_group_id)
    }

    /// Cancels every pending task in `group`, and any task later spawned
    /// into it. The default group is never cancelled.
    pub fn cancel_group(&self, group: TaskGroupId) -> usize {
        let cancelled = { self.scheduler.borrow_mut().cancel_group_tasks(group) };
        for id in &cancelled {
            self.clear_luau_context_for_task(*id);
        }
        self.flush_pending_luau_cancellations();
        cancelled.len()
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
            LuauThreadStart::Budgeted {
                resume_budget,
                completion: None,
            },
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
        self.schedule_luau_thread_start(
            context,
            vm,
            thread,
            args,
            LuauThreadStart::Budgeted {
                resume_budget,
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
            LuauThreadStart::Delayed(delay) => Some(
                Instant::now()
                    .checked_add(*delay)
                    .unwrap_or_else(Instant::now),
            ),
            LuauThreadStart::Immediate | LuauThreadStart::Budgeted { .. } => None,
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
                LuauThreadStart::Budgeted {
                    resume_budget,
                    completion,
                } => self.scheduler.borrow_mut().spawn_luau_thread_with_options(
                    context,
                    vm.clone(),
                    thread.clone(),
                    args,
                    LuauTaskOptions {
                        resume_budget: Some(resume_budget),
                        completion,
                    },
                ),
            };
        self.thread_tasks.borrow_mut().insert(key, handle);
        self.task_threads
            .borrow_mut()
            .insert(handle.id(), (vm, thread));
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
            delay_until(delay),
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
        // Not in `task_threads`: the thread's context is its own, so
        // finishing the task must not clear it.
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
        if cancelled {
            self.clear_luau_context_for_task(id);
        }
        self.flush_pending_luau_cancellations();
        cancelled
    }

    pub fn cancel_luau_thread(&self, thread: &luau::Thread) -> bool {
        let key = luau_thread_key(thread);
        if self.polling.get() {
            let scheduled = self.thread_tasks.borrow().contains_key(&key)
                || self
                    .pending_luau_work
                    .borrow()
                    .iter()
                    .any(|work| match work {
                        PendingLuauWork::Thread { thread, .. }
                        | PendingLuauWork::Future { thread, .. } => luau_thread_key(thread) == key,
                    });
            self.pending_luau_cancellations
                .borrow_mut()
                .push(thread.clone());
            return scheduled;
        }
        let Some(handle) = self.thread_tasks.borrow_mut().remove(&key) else {
            return self.cancel_pending_luau_thread_work(key);
        };
        self.cancel(handle.id())
    }

    pub fn schedule_cancel_luau_thread(&self, thread: luau::Thread) {
        self.pending_luau_cancellations.borrow_mut().push(thread);
    }

    pub fn remove(&self, id: TaskId) -> bool {
        self.thread_tasks
            .borrow_mut()
            .retain(|_, handle| handle.id() != id);
        self.clear_luau_context_for_task(id);
        let removed = { self.scheduler.borrow_mut().remove(id) };
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

    pub fn poll_ready(&self) -> usize {
        self.polling.set(true);
        let completed = self.scheduler.borrow_mut().poll_ready();
        self.polling.set(false);
        self.flush_pending_luau_threads();
        self.flush_pending_luau_cancellations();
        completed
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
        for id in &finished_ids {
            self.clear_luau_context_for_task(*id);
        }
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
        self.clear_luau_context_for_task(handle.id());
        merged_args.append(&mut args);
        merged_args
    }

    fn cancel_pending_luau_thread_work(&self, key: (u64, usize)) -> bool {
        let mut pending = self.pending_luau_work.borrow_mut();
        let original_len = pending.len();
        pending.retain(|work| match work {
            PendingLuauWork::Thread { thread, .. } | PendingLuauWork::Future { thread, .. } => {
                luau_thread_key(thread) != key
            }
        });
        pending.len() != original_len
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
                    let wake_at = delay_until(delay);
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
                        if !self.scheduler.borrow().borrows_thread(handle.id()) {
                            self.task_threads
                                .borrow_mut()
                                .entry(handle.id())
                                .or_insert((vm, thread));
                        }
                    } else {
                        // The parked thread has nothing left to resume it.
                        self.cancel(handle.id());
                    }
                }
            }
        }
    }

    fn flush_pending_luau_cancellations(&self) {
        let pending = std::mem::take(&mut *self.pending_luau_cancellations.borrow_mut());
        for thread in pending {
            self.cancel_luau_thread(&thread);
        }
    }

    fn clear_luau_context_for_task(&self, id: TaskId) {
        if let Some((vm, thread)) = self.task_threads.borrow_mut().remove(&id) {
            let _ = vm.clear_thread_call_context(&thread);
        }
    }
}

fn delay_until(delay: Option<Duration>) -> Option<Instant> {
    delay.map(|delay| {
        Instant::now()
            .checked_add(delay)
            .unwrap_or_else(Instant::now)
    })
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

struct Task {
    context: CallContext,
    /// Another run's group whose future the task awaits.
    awaiting: Option<TaskGroupId>,
    /// The task only carries a thread Luau code drives, such as a
    /// coroutine, through one wait; a plain yield hands the thread back.
    borrows_thread: bool,
    work: Option<TaskWork>,
    state: TaskState,
    error: Option<Arc<str>>,
    output: Option<Vec<luau::Value>>,
    wake_at: Option<Instant>,
    luau_resume_budget: Option<Duration>,
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
    Failed(String),
    Pending,
}

fn poll_work(
    context: &CallContext,
    work: &mut TaskWork,
    cx: &mut Context<'_>,
    luau_resume_budget: Option<Duration>,
) -> WorkPoll {
    match work {
        TaskWork::Future(future) => match future.as_mut().poll(cx) {
            Poll::Ready(Ok(())) => WorkPoll::Completed { output: None },
            Poll::Ready(Err(error)) => WorkPoll::Failed(error.to_string()),
            Poll::Pending => WorkPoll::Pending,
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
                Err(error) => return WorkPoll::Failed(error.to_string()),
            };
            let result = resume_luau_thread(vm, thread, args, luau_resume_budget);
            args.clear();
            poll_luau_resume(vm, thread, result, previous)
        }
        TaskWork::LuauFuture { vm, thread, future } => {
            let output = match Pin::new(&mut *future).poll(cx) {
                Poll::Ready(Ok(values)) => Ok(values),
                // Raised where the thread waits, as the call would have raised it had it not
                // waited, so a `pcall` around the call catches it.
                Poll::Ready(Err(error)) => Err(future.error_message(&error)),
                Poll::Pending => return WorkPoll::Pending,
            };
            let (vm, thread) = (vm.clone(), thread.clone());
            // The future is done; what is left is the thread itself.
            *work = TaskWork::LuauThread {
                vm: vm.clone(),
                thread: thread.clone(),
                args: Vec::new(),
                pending_start: false,
            };
            let previous = match vm.set_thread_call_context(&thread, context.clone()) {
                Ok(previous) => previous,
                Err(error) => return WorkPoll::Failed(error.to_string()),
            };
            let _guard = luau_resume_budget.map(|budget| vm.interrupt_after(budget));
            let result = match output {
                Ok(values) => thread.resume(&vm, values.as_slice()),
                Err(message) => thread.resume_error(&vm, &message),
            };
            poll_luau_resume(&vm, &thread, result, previous)
        }
    }
}

fn resume_luau_thread(
    vm: &luau::Vm,
    thread: &luau::Thread,
    args: &[luau::Value],
    budget: Option<Duration>,
) -> luau::runtime::Result<luau::ThreadStatus> {
    let _guard = budget.map(|budget| vm.interrupt_after(budget));
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
        Ok(luau::ThreadStatus::Yielded(_)) => {
            if let Ok(scheduler) = vm.data().get::<LocalScheduler>()
                && scheduler.take_parked_luau_thread(thread)
            {
                WorkPoll::Parked
            } else {
                WorkPoll::Yielded
            }
        }
        Err(error) => WorkPoll::Failed(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
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
        assert_eq!(
            scheduler.next_wake_delay_at(Instant::now()),
            Some(Duration::ZERO)
        );

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
        let mut scheduler = Scheduler::new();
        let start = Instant::now();
        let handle = scheduler.spawn_boxed_after(
            CallContext::default(),
            Duration::from_secs(5),
            Box::pin(async { Ok(()) }),
        );

        assert_eq!(scheduler.poll_ready_at(start), 0);
        assert_eq!(
            scheduler
                .snapshot(handle.id())
                .expect("task snapshot")
                .state,
            TaskState::Pending
        );

        assert_eq!(scheduler.poll_ready_at(start + Duration::from_secs(6)), 1);
        assert_eq!(
            scheduler
                .snapshot(handle.id())
                .expect("task snapshot")
                .state,
            TaskState::Completed
        );
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
                resume_budget: Some(Duration::ZERO),
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
        assert_eq!(task.luau_resume_budget, Some(Duration::from_secs(1)));
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
}
