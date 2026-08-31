use std::{
    any::{
        Any,
        TypeId,
        type_name,
    },
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
use std::cell::{
    Cell,
    RefCell,
};

pub type ScheduledFuture = Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ModuleId(pub Arc<str>);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CapabilityId(pub Arc<str>);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct TaskGroupId(pub u64);

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ChunkOrigin {
    pub module: Option<ModuleId>,
    pub plugin: Option<Arc<str>>,
    pub path: Option<Arc<str>>,
}

#[derive(Clone, Default)]
pub struct CallContext {
    pub origin: ChunkOrigin,
    pub capability: Option<CapabilityId>,
    pub caller: ContextBag,
    pub task_group: TaskGroupId,
}

#[derive(Clone, Default)]
pub struct ContextBag {
    values: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl ContextBag {
    pub fn insert<T>(&mut self, value: T)
    where
        T: Send + Sync + 'static,
    {
        self.values.insert(TypeId::of::<T>(), Arc::new(value));
    }

    pub fn insert_shared(
        &mut self,
        type_id: TypeId,
        value: Arc<dyn Any + Send + Sync>,
    ) -> Option<Arc<dyn Any + Send + Sync>> {
        self.values.insert(type_id, value)
    }

    pub fn cloned_entries(&self) -> Vec<(TypeId, Arc<dyn Any + Send + Sync>)> {
        self.values
            .iter()
            .map(|(type_id, value)| (*type_id, value.clone()))
            .collect()
    }

    pub fn get<T>(&self) -> Result<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.values
            .get(&TypeId::of::<T>())
            .cloned()
            .and_then(|value| value.downcast::<T>().ok())
            .ok_or_else(|| anyhow::anyhow!("call context does not contain {}", type_name::<T>()))
    }
}

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
    group: TaskGroupId,
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

    pub fn group(self) -> TaskGroupId {
        self.group
    }
}

pub struct Scheduler {
    next_task_id: u64,
    next_group_id: u64,
    ready: VecDeque<TaskId>,
    sleeping: Vec<(Instant, TaskId)>,
    luau_resume_budget: Option<Duration>,
    wake_queue: Arc<WakeQueue>,
    tasks: HashMap<TaskId, Task>,
    groups: HashMap<TaskGroupId, HashSet<TaskId>>,
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
            next_group_id: 1,
            ready: VecDeque::new(),
            sleeping: Vec::new(),
            luau_resume_budget: None,
            wake_queue: Arc::new(WakeQueue::default()),
            tasks: HashMap::new(),
            groups: HashMap::new(),
        }
    }

    pub fn set_luau_resume_budget(&mut self, budget: Option<Duration>) {
        self.luau_resume_budget = budget;
    }

    pub fn create_group(&mut self) -> TaskGroupId {
        let group = TaskGroupId(self.next_group_id);
        self.next_group_id = self.next_group_id.saturating_add(1);
        self.groups.entry(group).or_default();
        group
    }

    pub fn spawn(
        &mut self,
        context: CallContext,
        future: impl Future<Output = Result<()>> + Send + 'static,
    ) -> TaskHandle {
        self.spawn_boxed(context, Box::pin(future))
    }

    pub fn spawn_boxed(&mut self, context: CallContext, future: ScheduledFuture) -> TaskHandle {
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
        future: ScheduledFuture,
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

    pub fn spawn_luau_future(
        &mut self,
        context: CallContext,
        vm: luau::Vm,
        thread: luau::Thread,
        future: luau::ScheduledFuture,
    ) -> TaskHandle {
        self.insert_work(
            context,
            TaskWork::LuauFuture { vm, thread, future },
            None,
            self.default_luau_task_options(),
        )
    }

    pub fn spawn_luau_future_after(
        &mut self,
        context: CallContext,
        delay: Duration,
        vm: luau::Vm,
        thread: luau::Thread,
        future: luau::ScheduledFuture,
    ) -> TaskHandle {
        let wake_at = Instant::now()
            .checked_add(delay)
            .unwrap_or_else(Instant::now);
        self.insert_work(
            context,
            TaskWork::LuauFuture { vm, thread, future },
            Some(wake_at),
            self.default_luau_task_options(),
        )
    }

    fn replace_with_luau_future(
        &mut self,
        id: TaskId,
        context: CallContext,
        vm: luau::Vm,
        thread: luau::Thread,
        future: luau::ScheduledFuture,
        wake_at: Option<Instant>,
    ) -> bool {
        let Some(task) = self.tasks.get_mut(&id) else {
            return false;
        };

        task.context = context;
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
        future: ScheduledFuture,
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
        let handle = TaskHandle { id, group };

        self.groups.entry(group).or_default().insert(id);
        if let Some(wake_at) = wake_at {
            self.sleeping.push((wake_at, id));
        } else {
            self.ready.push_back(id);
        }
        self.tasks.insert(
            id,
            Task {
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

    pub fn cancel_group(&mut self, group: TaskGroupId) -> usize {
        let Some(tasks) = self.groups.get(&group) else {
            return 0;
        };

        let ids: Vec<_> = tasks.iter().copied().collect();
        ids.into_iter().filter(|id| self.cancel(*id)).count()
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
                WorkPoll::Parked => {
                    task.work = None;
                    task.state = TaskState::Completed;
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
                WorkPoll::Pending => {}
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

    pub fn remove(&mut self, id: TaskId) -> bool {
        let Some(task) = self.tasks.remove(&id) else {
            return false;
        };

        if let Some(group_tasks) = self.groups.get_mut(&task.context.task_group) {
            group_tasks.remove(&id);
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

#[derive(Default)]
pub struct LocalScheduler {
    scheduler: RefCell<Scheduler>,
    thread_tasks: RefCell<HashMap<(u64, usize), TaskHandle>>,
    task_threads: RefCell<HashMap<TaskId, (luau::Vm, luau::Thread)>>,
    parked_threads: RefCell<HashSet<(u64, usize)>>,
    pending_luau_work: RefCell<Vec<PendingLuauWork>>,
    pending_luau_cancellations: RefCell<Vec<luau::Thread>>,
    polling: Cell<bool>,
}

impl LocalScheduler {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create_group(&self) -> TaskGroupId {
        self.scheduler.borrow_mut().create_group()
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

        let handle = if let Some(delay) = delay {
            self.scheduler.borrow_mut().spawn_luau_future_after(
                context,
                delay,
                vm.clone(),
                thread.clone(),
                future,
            )
        } else {
            self.scheduler.borrow_mut().spawn_luau_future(
                context,
                vm.clone(),
                thread.clone(),
                future,
            )
        };
        self.thread_tasks.borrow_mut().insert(key, handle);
        self.task_threads
            .borrow_mut()
            .insert(handle.id(), (vm, thread));
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
                    let key = luau_thread_key(&thread);
                    let wake_at = delay.map(|delay| {
                        Instant::now()
                            .checked_add(delay)
                            .unwrap_or_else(Instant::now)
                    });
                    match target {
                        Some(handle) => {
                            if self.scheduler.borrow_mut().replace_with_luau_future(
                                handle.id(),
                                context,
                                vm.clone(),
                                thread.clone(),
                                future,
                                wake_at,
                            ) {
                                self.thread_tasks.borrow_mut().insert(key, handle);
                                self.task_threads
                                    .borrow_mut()
                                    .entry(handle.id())
                                    .or_insert((vm, thread));
                            }
                        }
                        None => {
                            let handle = if let Some(delay) = delay {
                                self.scheduler.borrow_mut().spawn_luau_future_after(
                                    context,
                                    delay,
                                    vm.clone(),
                                    thread.clone(),
                                    future,
                                )
                            } else {
                                self.scheduler.borrow_mut().spawn_luau_future(
                                    context,
                                    vm.clone(),
                                    thread.clone(),
                                    future,
                                )
                            };
                            self.thread_tasks.borrow_mut().insert(key, handle);
                            self.task_threads
                                .borrow_mut()
                                .insert(handle.id(), (vm, thread));
                        }
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
    work: Option<TaskWork>,
    state: TaskState,
    error: Option<Arc<str>>,
    output: Option<Vec<luau::Value>>,
    wake_at: Option<Instant>,
    luau_resume_budget: Option<Duration>,
    completion: Option<Rc<LocalLuauTaskCompletion>>,
}

enum TaskWork {
    Future(ScheduledFuture),
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
    Completed { output: Option<Vec<luau::Value>> },
    Parked,
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
            if let Err(error) = vm.set_thread_call_context(thread, luau_call_context(context)) {
                return WorkPoll::Failed(error.to_string());
            }
            let result = resume_luau_thread(vm, thread, args, luau_resume_budget);
            args.clear();
            poll_luau_resume(vm, thread, result)
        }
        TaskWork::LuauFuture { vm, thread, future } => match Pin::new(future).poll(cx) {
            Poll::Ready(Ok(values)) => {
                if let Err(error) = vm.set_thread_call_context(thread, luau_call_context(context)) {
                    return WorkPoll::Failed(error.to_string());
                }
                let result = resume_luau_thread(vm, thread, values.as_slice(), luau_resume_budget);
                poll_luau_resume(vm, thread, result)
            }
            Poll::Ready(Err(error)) => WorkPoll::Failed(error.to_string()),
            Poll::Pending => WorkPoll::Pending,
        },
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

fn luau_call_context(context: &CallContext) -> luau::CallContext {
    let mut caller = luau::ContextBag::default();
    for (type_id, value) in context.caller.cloned_entries() {
        caller.insert_shared(type_id, value);
    }
    luau::CallContext {
        origin: luau::ChunkOrigin {
            module: context
                .origin
                .module
                .as_ref()
                .map(|module| luau::ModuleId(module.0.clone())),
            plugin: context.origin.plugin.clone(),
            path: context.origin.path.clone(),
        },
        capability: context
            .capability
            .as_ref()
            .map(|capability| luau::CapabilityId(capability.0.clone())),
        caller,
        task_group: luau::TaskGroupId(context.task_group.0),
    }
}

fn poll_luau_resume(
    vm: &luau::Vm,
    thread: &luau::Thread,
    result: luau::runtime::Result<luau::ThreadStatus>,
) -> WorkPoll {
    match result {
        Ok(luau::ThreadStatus::Completed(values)) => {
            let _ = vm.clear_thread_call_context(thread);
            WorkPoll::Completed {
                output: Some(values),
            }
        }
        Ok(luau::ThreadStatus::Yielded(_)) => {
            let _ = vm.clear_thread_call_context(thread);
            if let Ok(scheduler) = vm.data().get::<LocalScheduler>()
                && scheduler.take_parked_luau_thread(thread)
            {
                WorkPoll::Parked
            } else {
                WorkPoll::Pending
            }
        }
        Err(error) => {
            let _ = vm.clear_thread_call_context(thread);
            WorkPoll::Failed(error.to_string())
        }
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
                module: Some(luau::ModuleId(Arc::from("demo/root"))),
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
