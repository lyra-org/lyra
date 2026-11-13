use std::{
    cell::Cell,
    collections::{
        HashMap,
        HashSet,
        VecDeque,
    },
    rc::Rc,
    sync::Arc,
    time::{
        Duration,
        Instant,
    },
};

use harmony_luau as luau;
use harmony_luau::{
    CallContext,
    TaskGroupId,
};
use tokio::sync::oneshot;

use super::limit::{
    ResumeBound,
    ResumeLimit,
};
use super::time::{
    TimerId,
    Timers,
};
use super::wake::{
    TaskWaker,
    WakeQueue,
};
use super::work::{
    TaskWork,
    WorkPoll,
};
use super::{
    LocalLuauTaskCompletion,
    TaskHandle,
    TaskId,
    TaskSnapshot,
    TaskState,
};

/// A Luau thread as the scheduler indexes it: by its VM and its state.
pub(super) type ThreadKey = (u64, usize);

pub(super) fn thread_key(thread: &luau::Thread) -> ThreadKey {
    (thread.vm_id(), thread.state_id())
}

/// What the scheduler knows of its tasks. Its methods run no code the scheduler doesn't control:
/// what would, such as closing a thread, telling a completion or dropping work, comes back as
/// [`Deferred`], for the scheduler to do with this released.
pub(super) struct State {
    next_task_id: u64,
    last_group_id: u64,
    ready: VecDeque<TaskId>,
    /// The ready tasks that watch others, which a round runs first.
    ready_watchers: VecDeque<TaskId>,
    /// The tasks the round under way has yet to poll: what was ready as it began.
    round: VecDeque<TaskId>,
    round_watchers: VecDeque<TaskId>,
    timers: Rc<Timers>,
    wake_queue: Arc<WakeQueue>,
    luau_resume_budget: Option<Duration>,
    tasks: HashMap<TaskId, Task>,
    groups: HashMap<TaskGroupId, HashSet<TaskId>>,
    cancelled_groups: HashSet<TaskGroupId>,
    /// The task being polled, whose work is out of its slot.
    polled: Option<Polled>,
    /// The task that last took up each thread.
    thread_tasks: HashMap<ThreadKey, TaskHandle>,
    parked_threads: HashSet<ThreadKey>,
}

/// How a Luau task runs: under what limit, and whom to tell how it ends.
#[derive(Default)]
pub(super) struct LuauTaskOptions {
    pub(super) resume_limit: Option<ResumeLimit>,
    pub(super) completion: Option<Rc<LocalLuauTaskCompletion>>,
}

struct Polled {
    id: TaskId,
    /// Set once the task is cancelled during its poll, which cancels it as the poll returns.
    cancelled: Rc<Cell<bool>>,
    /// Told once the task has settled after its poll.
    settled: Vec<oneshot::Sender<()>>,
}

/// What a round takes off a task to poll it: the context it runs as, its work, the limit it runs
/// under, and what its cancellation during the poll sets.
pub(super) type Taken = (CallContext, TaskWork, Option<ResumeLimit>, Rc<Cell<bool>>);

pub(super) type TaskResult = std::result::Result<Vec<luau::Value>, luau::RaisedError>;

/// What the scheduler is left to do once its state is released, each of which runs code it
/// doesn't control.
#[must_use]
pub(super) enum Deferred {
    /// A task that settled: the thread it had, to let go of and, for a task cancelled, to close,
    /// the completion to tell, and the work to drop.
    Settled {
        context: CallContext,
        thread: Option<luau::Thread>,
        /// The task ran the thread as its context, which the thread loses.
        release: bool,
        close: bool,
        report: Option<Report>,
        work: Option<TaskWork>,
    },
    /// The thread, if it has one, of the task being polled, closed at once; the task is cancelled
    /// as its poll returns.
    Close(Option<luau::Thread>),
    /// Work to drop, of a task that goes on or was forgotten, with the forgotten task.
    Drop {
        context: CallContext,
        work: Option<TaskWork>,
        removed: Option<Box<Task>>,
    },
}

/// How a task ended, for its completion.
pub(super) struct Report {
    completion: Rc<LocalLuauTaskCompletion>,
    result: TaskResult,
    interrupted_by: Option<ResumeBound>,
}

impl Report {
    pub(super) fn tell(self, now: Instant) {
        self.completion.interrupted_by.set(self.interrupted_by);
        self.completion.complete(self.result, now);
    }
}

impl Deferred {
    fn work(context: CallContext, work: TaskWork) -> Self {
        Self::Drop {
            context,
            work: Some(work),
            removed: None,
        }
    }
}

impl State {
    pub(super) fn new(timers: Rc<Timers>, wake_queue: Arc<WakeQueue>) -> Self {
        Self {
            next_task_id: 1,
            last_group_id: 0,
            ready: VecDeque::new(),
            ready_watchers: VecDeque::new(),
            round: VecDeque::new(),
            round_watchers: VecDeque::new(),
            timers,
            wake_queue,
            luau_resume_budget: None,
            tasks: HashMap::new(),
            groups: HashMap::new(),
            cancelled_groups: HashSet::new(),
            polled: None,
            thread_tasks: HashMap::new(),
            parked_threads: HashSet::new(),
        }
    }

    pub(super) fn set_luau_resume_budget(&mut self, budget: Option<Duration>) {
        self.luau_resume_budget = budget;
    }

    pub(super) fn next_group(&mut self) -> TaskGroupId {
        self.last_group_id = self.last_group_id.saturating_add(1);
        TaskGroupId(self.last_group_id)
    }

    pub(super) fn default_luau_task_options(&self) -> LuauTaskOptions {
        LuauTaskOptions {
            resume_limit: self.luau_resume_budget.map(ResumeLimit::budget),
            completion: None,
        }
    }

    /// Adds a task. One spawned into a cancelled group is cancelled at once, and what it leaves is
    /// returned.
    pub(super) fn insert_work(
        &mut self,
        context: CallContext,
        work: TaskWork,
        wake_at: Option<Instant>,
        options: LuauTaskOptions,
        watcher: bool,
    ) -> (TaskHandle, Option<Deferred>) {
        let id = TaskId(self.next_task_id);
        self.next_task_id = self.next_task_id.saturating_add(1);
        let group = context.task_group;
        let handle = TaskHandle { id };
        let thread = match &work {
            TaskWork::LuauThread { thread, .. } | TaskWork::LuauFuture { thread, .. } => {
                Some(thread.clone())
            }
            TaskWork::Future(_) => None,
        };
        self.groups.entry(group).or_default().insert(id);
        self.tasks.insert(
            id,
            Task {
                awaiting: None,
                borrows_thread: false,
                watcher,
                queued: false,
                context,
                thread,
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
            return (handle, self.cancel(id));
        }
        if let Some(wake_at) = wake_at {
            self.wake_task_at(id, wake_at);
        } else {
            self.enqueue(id);
        }
        (handle, None)
    }

    /// Records `handle` as the task that took up `thread`.
    pub(super) fn track_thread(&mut self, thread: &luau::Thread, handle: TaskHandle) {
        self.thread_tasks.insert(thread_key(thread), handle);
    }

    pub(super) fn untrack_thread(&mut self, thread: &luau::Thread) -> Option<TaskHandle> {
        self.thread_tasks.remove(&thread_key(thread))
    }

    pub(super) fn thread_task(&self, thread: &luau::Thread) -> Option<TaskHandle> {
        self.thread_tasks.get(&thread_key(thread)).copied()
    }

    /// The thread of pending task `id`.
    fn pending_thread(&self, id: TaskId) -> Option<&luau::Thread> {
        self.tasks
            .get(&id)
            .filter(|task| task.state == TaskState::Pending)?
            .thread
            .as_ref()
    }

    /// Whether the scheduler holds a pending task for `thread`.
    pub(super) fn holds_thread(&self, thread: &luau::Thread) -> bool {
        self.thread_task(thread)
            .is_some_and(|handle| self.pending_thread(handle.id()).is_some())
    }

    /// Runs `future` for `thread`, which acts as `context`, while the task also belongs to
    /// `awaiting`, the group of the run the future acts for.
    pub(super) fn spawn_luau_future(
        &mut self,
        context: CallContext,
        awaiting: TaskGroupId,
        thread: luau::Thread,
        future: luau::ScheduledFuture,
        wake_at: Option<Instant>,
    ) -> (TaskHandle, Option<Deferred>) {
        let options = self.default_luau_task_options();
        let (handle, deferred) = self.insert_work(
            context,
            TaskWork::LuauFuture {
                thread: thread.clone(),
                future,
            },
            wake_at,
            options,
            false,
        );
        self.track_thread(&thread, handle);
        if deferred.is_some() {
            return (handle, deferred);
        }
        if let Some(task) = self.tasks.get_mut(&handle.id()) {
            task.borrows_thread = true;
        }
        (handle, self.await_group(handle.id(), awaiting))
    }

    /// The task keeps its thread's context and also joins `awaiting` while the future runs.
    /// Fails, giving the future back, unless the task is parked and both groups' runs are live.
    pub(super) fn replace_with_luau_future(
        &mut self,
        id: TaskId,
        awaiting: TaskGroupId,
        thread: luau::Thread,
        future: luau::ScheduledFuture,
        wake_at: Option<Instant>,
    ) -> std::result::Result<(), TaskWork> {
        let work = TaskWork::LuauFuture { thread, future };
        if !self.is_parked(id) {
            return Err(work);
        }
        let group = self.tasks[&id].context.task_group;
        if self.is_cancelled(group) || self.is_cancelled(awaiting) {
            return Err(work);
        }
        let cancelled = self.await_group(id, awaiting);
        debug_assert!(cancelled.is_none(), "both groups were checked above");
        let task = self.tasks.get_mut(&id).expect("task checked above");
        task.work = Some(work);
        task.state = TaskState::Pending;
        task.error = None;
        task.output = None;
        task.wake_at = wake_at;
        if let Some(wake_at) = wake_at {
            self.wake_task_at(id, wake_at);
        } else {
            self.enqueue(id);
        }
        Ok(())
    }

    /// The task `thread`, which hasn't started, last started as, while that task hasn't started
    /// it either: what a new start of the thread may take the arguments of.
    pub(super) fn pending_luau_thread_start(&self, thread: &luau::Thread) -> Option<TaskHandle> {
        let handle = self.thread_task(thread)?;
        let task = self.tasks.get(&handle.id())?;
        if task.state != TaskState::Pending {
            return None;
        }
        match task.work.as_ref() {
            Some(TaskWork::LuauThread {
                pending_start: true,
                ..
            }) => Some(handle),
            _ => None,
        }
    }

    /// Takes the arguments of `handle`'s start, found by [`Self::pending_luau_thread_start`],
    /// whose thread is still unstarted, for a start no later than it: the task ends, and the
    /// thread starts again as another.
    pub(super) fn take_pending_luau_thread_start(
        &mut self,
        handle: TaskHandle,
        wake_at: Option<Instant>,
    ) -> Option<(Vec<luau::Value>, Deferred)> {
        let id = handle.id();
        let task = self.tasks.get_mut(&id)?;
        if task.state != TaskState::Pending || !start_wake_is_not_later(wake_at, task.wake_at) {
            return None;
        }
        let Some(TaskWork::LuauThread {
            args,
            pending_start: true,
            ..
        }) = task.work.as_mut()
        else {
            return None;
        };
        let args = std::mem::take(args);
        // Not closed: the thread starts again as another task.
        let deferred = self.settle(id, TaskState::Cancelled, None, false);
        self.thread_tasks.retain(|_, task| *task != handle);
        Some((args, deferred))
    }

    /// Cancels a pending task. Its thread, a coroutine the task only borrowed included, is to be
    /// closed so that code still holding it can't resume it either. The task being polled is
    /// only marked, and cancelled once its poll returns.
    pub(super) fn cancel(&mut self, id: TaskId) -> Option<Deferred> {
        let task = self.tasks.get(&id)?;
        if task.state != TaskState::Pending {
            return None;
        }
        if let Some(polled) = self.polled.as_ref().filter(|polled| polled.id == id) {
            polled.cancelled.set(true);
            return Some(Deferred::Close(task.thread.clone()));
        }
        let message = format!("Luau task {} was cancelled", id.0);
        Some(self.settle(
            id,
            TaskState::Cancelled,
            Some((Err(message.into()), None)),
            true,
        ))
    }

    /// Cancels every pending task in `group`, and any task later spawned into it. The default
    /// group is never cancelled.
    pub(super) fn cancel_group(&mut self, group: TaskGroupId) -> Vec<Deferred> {
        if group == TaskGroupId::default() {
            return Vec::new();
        }
        self.cancelled_groups.insert(group);
        let mut ids: Vec<_> = self
            .groups
            .get(&group)
            .into_iter()
            .flatten()
            .copied()
            .collect();
        ids.sort_unstable_by_key(|id| id.0);
        ids.into_iter().filter_map(|id| self.cancel(id)).collect()
    }

    pub(super) fn is_cancelled(&self, group: TaskGroupId) -> bool {
        self.cancelled_groups.contains(&group)
    }

    pub(super) fn park_luau_thread(&mut self, thread: &luau::Thread) {
        self.parked_threads.insert(thread_key(thread));
    }

    pub(super) fn take_parked_luau_thread(&mut self, thread: &luau::Thread) -> bool {
        self.parked_threads.remove(&thread_key(thread))
    }

    #[cfg(test)]
    pub(super) fn has_parked_threads(&self) -> bool {
        !self.parked_threads.is_empty()
    }

    /// Parked on an async call: still pending, waiting for that call's future to become its
    /// work.
    pub(super) fn is_parked(&self, id: TaskId) -> bool {
        !self.is_polled(id)
            && self
                .tasks
                .get(&id)
                .is_some_and(|task| task.state == TaskState::Pending && task.work.is_none())
    }

    /// Makes pending task `id` ready, unless it already is: a task is polled at most once a round,
    /// however often it was woken.
    fn enqueue(&mut self, id: TaskId) {
        let Some(task) = self.tasks.get_mut(&id) else {
            return;
        };
        if task.state != TaskState::Pending || task.queued {
            return;
        }
        task.queued = true;
        if task.watcher {
            self.ready_watchers.push_back(id);
        } else {
            self.ready.push_back(id);
        }
    }

    /// Makes ready the tasks that were woken, and takes every ready task into the round that
    /// begins, after any a round that unwound left. What is made ready during the round waits for
    /// the next one.
    pub(super) fn begin_round(&mut self) {
        for id in self.wake_queue.drain() {
            self.enqueue(id);
        }
        self.round_watchers.append(&mut self.ready_watchers);
        self.round.append(&mut self.ready);
    }

    /// The round's next watcher, or its next other task, to poll.
    pub(super) fn next_in_round(&mut self, watchers: bool) -> Option<TaskId> {
        let id = if watchers {
            self.round_watchers.pop_front()
        } else {
            self.round.pop_front()
        }?;
        if let Some(task) = self.tasks.get_mut(&id) {
            task.queued = false;
        }
        Some(id)
    }

    fn is_polled(&self, id: TaskId) -> bool {
        self.polled.as_ref().is_some_and(|polled| polled.id == id)
    }

    /// The task being polled, if `thread` is the thread it runs: found by the task itself, which
    /// keeps its thread for as long as it is polled, and not by what `thread` last started as.
    fn polled_with_thread(&mut self, thread: &luau::Thread) -> Option<&mut Polled> {
        let key = thread_key(thread);
        let polled = self.polled.as_mut()?;
        let runs = self
            .tasks
            .get(&polled.id)
            .and_then(|task| task.thread.as_ref())
            .is_some_and(|own| thread_key(own) == key);
        runs.then_some(polled)
    }

    /// A future that completes once the task being polled, if it runs `thread`, has settled after
    /// its poll.
    pub(super) fn wait_for_polled_thread(
        &mut self,
        thread: &luau::Thread,
    ) -> Option<oneshot::Receiver<()>> {
        let polled = self.polled_with_thread(thread)?;
        let (settled, waiting) = oneshot::channel();
        polled.settled.push(settled);
        Some(waiting)
    }

    /// Takes the work off pending task `id` to poll it, with the context and the limit it runs
    /// under, and what its cancellation during the poll sets.
    pub(super) fn take_for_poll(&mut self, id: TaskId) -> Option<Taken> {
        debug_assert!(self.polled.is_none(), "a task is already being polled");
        let task = self.tasks.get_mut(&id)?;
        if task.state != TaskState::Pending {
            return None;
        }
        let work = task.work.take()?;
        let cancelled = Rc::new(Cell::new(false));
        self.polled = Some(Polled {
            id,
            cancelled: cancelled.clone(),
            settled: Vec::new(),
        });
        Some((
            task.context.clone(),
            work,
            task.resume_limit.clone(),
            cancelled,
        ))
    }

    /// Puts back the work of the task just polled, or settles the task as the poll came to, and
    /// cancels the task if that was asked during its poll. Returns whether the poll ended or
    /// parked the task, what to finish with, and whom to tell the task has settled.
    pub(super) fn after_poll(
        &mut self,
        id: TaskId,
        context: CallContext,
        work: TaskWork,
        outcome: WorkPoll,
        interrupted_by: Option<ResumeBound>,
    ) -> (bool, Vec<Deferred>, Vec<oneshot::Sender<()>>) {
        let polled = self.polled.take().expect("a task is being polled");
        debug_assert_eq!(polled.id, id);
        let Some(task) = self.tasks.get_mut(&id) else {
            return (false, vec![Deferred::work(context, work)], polled.settled);
        };
        if task.state != TaskState::Pending {
            return (false, vec![Deferred::work(context, work)], polled.settled);
        }
        let (counted, deferred) = match outcome {
            WorkPoll::Completed { output } => {
                let result = if task.completion.is_some() {
                    Some(Ok(output.unwrap_or_default()))
                } else {
                    task.output = output;
                    None
                };
                task.work = Some(work);
                let report = result.map(|result| (result, None));
                (
                    true,
                    Some(self.settle(id, TaskState::Completed, report, false)),
                )
            }
            // Stays pending, without work, until the parked call's future replaces it or a
            // refused replacement cancels it.
            WorkPoll::Parked => {
                task.wake_at = None;
                (true, Some(Deferred::work(context, work)))
            }
            WorkPoll::Cancelled => {
                task.work = Some(work);
                let message = format!("Luau task {} was cancelled", id.0);
                // Not closed: other code started the thread and runs it, or it was closed as it
                // yielded.
                (
                    true,
                    Some(self.settle(
                        id,
                        TaskState::Cancelled,
                        Some((Err(message.into()), None)),
                        false,
                    )),
                )
            }
            WorkPoll::Failed(error) => {
                task.work = Some(work);
                task.error = Some(Arc::from(error.message.as_str()));
                let raised = luau::RaisedError {
                    kind: error.kind,
                    message: format!("Luau task {} failed: {}", id.0, error.message),
                };
                (
                    true,
                    Some(self.settle(
                        id,
                        TaskState::Failed,
                        Some((Err(raised), interrupted_by)),
                        false,
                    )),
                )
            }
            WorkPoll::Yielded if task.borrows_thread => {
                task.work = Some(work);
                (
                    true,
                    Some(self.settle(id, TaskState::Completed, None, false)),
                )
            }
            WorkPoll::Yielded | WorkPoll::Pending => {
                task.work = Some(work);
                (false, None)
            }
        };
        if !matches!(
            self.tasks.get(&id).and_then(|task| task.work.as_ref()),
            Some(TaskWork::LuauFuture { .. })
        ) {
            self.stop_awaiting(id);
        }
        let mut deferred: Vec<_> = deferred.into_iter().collect();
        if polled.cancelled.get() {
            deferred.extend(self.cancel(id));
        }
        (counted, deferred, polled.settled)
    }

    /// Fails the task whose poll unwound while its work was out of its slot. Returns what to
    /// finish with, and whom to tell the task has settled.
    pub(super) fn poll_unwound(
        &mut self,
        id: TaskId,
        context: CallContext,
        work: TaskWork,
    ) -> (Deferred, Vec<oneshot::Sender<()>>) {
        let settled = self
            .polled
            .take()
            .map(|polled| polled.settled)
            .unwrap_or_default();
        let pending = self
            .tasks
            .get(&id)
            .is_some_and(|task| task.state == TaskState::Pending);
        if !pending {
            return (Deferred::work(context, work), settled);
        }
        let message = "the scheduler's poll of the task unwound";
        let task = self.tasks.get_mut(&id).expect("task checked above");
        task.work = Some(work);
        task.error = Some(Arc::from(message));
        let raised = format!("Luau task {} failed: {message}", id.0);
        let failed = self.settle(
            id,
            TaskState::Failed,
            Some((Err(raised.into()), None)),
            true,
        );
        (failed, settled)
    }

    /// Takes `task` out of the pending ones as `state`, and returns what it leaves: its work, the
    /// thread it had, to close if `close`, and the result to tell its completion if it has one.
    /// The thread loses the context the task ran it with, unless the task only borrowed it and the
    /// context is the thread's own.
    fn settle(
        &mut self,
        id: TaskId,
        state: TaskState,
        result: Option<(TaskResult, Option<ResumeBound>)>,
        close: bool,
    ) -> Deferred {
        let task = self.tasks.get_mut(&id).expect("a task to settle");
        task.state = state;
        task.wake_at = None;
        let report = task.completion.clone().and_then(|completion| {
            result.map(|(result, interrupted_by)| Report {
                completion,
                result,
                interrupted_by,
            })
        });
        let settled = Deferred::Settled {
            context: task.context.clone(),
            thread: task.thread.take(),
            release: !task.borrows_thread,
            close,
            report,
            work: task.work.take(),
        };
        self.timers.remove(TimerId::Task(id));
        settled
    }

    /// Makes task `id` ready once the clock reaches `wake_at`.
    fn wake_task_at(&self, id: TaskId, wake_at: Instant) {
        self.timers.set(
            TimerId::Task(id),
            wake_at,
            &TaskWaker::of(id, &self.wake_queue),
        );
    }

    /// Whether a task is ready to poll, by the queues alone.
    pub(super) fn has_ready(&self) -> bool {
        [
            &self.ready,
            &self.ready_watchers,
            &self.round,
            &self.round_watchers,
        ]
        .iter()
        .any(|queue| !queue.is_empty())
    }

    /// A task belongs to its own group for its whole life, and also to the group of a future
    /// from another run while it awaits one. These two keep `groups` in step with that, so
    /// cancelling either group finds it.
    fn await_group(&mut self, id: TaskId, group: TaskGroupId) -> Option<Deferred> {
        let task = self.tasks.get_mut(&id)?;
        let awaiting = (group != task.context.task_group).then_some(group);
        let previous = std::mem::replace(&mut task.awaiting, awaiting);
        if previous == awaiting {
            return None;
        }
        if let Some(previous) = previous
            && let Some(group_tasks) = self.groups.get_mut(&previous)
        {
            group_tasks.remove(&id);
        }
        let awaiting = awaiting?;
        self.groups.entry(awaiting).or_default().insert(id);
        if self.is_cancelled(awaiting) {
            return self.cancel(id);
        }
        None
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

    /// Forgets task `id`, and returns the work it had.
    pub(super) fn remove(&mut self, id: TaskId) -> Option<Deferred> {
        let mut task = self.tasks.remove(&id)?;
        self.thread_tasks.retain(|_, handle| handle.id() != id);
        for group in std::iter::once(task.context.task_group).chain(task.awaiting) {
            if let Some(group_tasks) = self.groups.get_mut(&group) {
                group_tasks.remove(&id);
            }
        }
        for queue in [
            &mut self.ready,
            &mut self.ready_watchers,
            &mut self.round,
            &mut self.round_watchers,
        ] {
            queue.retain(|queued| *queued != id);
        }
        self.timers.remove(TimerId::Task(id));
        Some(Deferred::Drop {
            context: task.context.clone(),
            work: task.work.take(),
            removed: Some(Box::new(task)),
        })
    }

    /// Forgets every task that ended, and returns how many and the work they had.
    pub(super) fn remove_finished(&mut self) -> (usize, Vec<Deferred>) {
        let finished: Vec<_> = self
            .tasks
            .iter()
            .filter_map(|(id, task)| (task.state != TaskState::Pending).then_some(*id))
            .collect();
        let removed = finished.len();
        let deferred = finished
            .into_iter()
            .filter_map(|id| self.remove(id))
            .collect();
        (removed, deferred)
    }

    pub(super) fn take_output(&mut self, id: TaskId) -> Option<Vec<luau::Value>> {
        self.tasks.get_mut(&id)?.output.take()
    }

    pub(super) fn snapshot(&self, id: TaskId) -> Option<TaskSnapshot> {
        let task = self.tasks.get(&id)?;
        Some(TaskSnapshot {
            id,
            group: task.context.task_group,
            origin: task.context.origin.clone(),
            state: task.state,
            error: task.error.clone(),
        })
    }

    pub(super) fn group_snapshots(&self, group: TaskGroupId) -> Vec<TaskSnapshot> {
        let Some(tasks) = self.groups.get(&group) else {
            return Vec::new();
        };
        let mut snapshots: Vec<_> = tasks.iter().filter_map(|id| self.snapshot(*id)).collect();
        snapshots.sort_by_key(|snapshot| snapshot.id.0);
        snapshots
    }

    pub(super) fn snapshots(&self) -> Vec<TaskSnapshot> {
        let mut snapshots: Vec<_> = self
            .tasks
            .keys()
            .filter_map(|id| self.snapshot(*id))
            .collect();
        snapshots.sort_by_key(|snapshot| snapshot.id.0);
        snapshots
    }

    pub(super) fn has_pending(&self) -> bool {
        self.tasks
            .values()
            .any(|task| task.state == TaskState::Pending)
    }

    #[cfg(test)]
    pub(super) fn task(&self, id: TaskId) -> Option<&Task> {
        self.tasks.get(&id)
    }

    #[cfg(test)]
    pub(super) fn task_mut(&mut self, id: TaskId) -> Option<&mut Task> {
        self.tasks.get_mut(&id)
    }
}

fn start_wake_is_not_later(new_wake_at: Option<Instant>, old_wake_at: Option<Instant>) -> bool {
    match (new_wake_at, old_wake_at) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(new_wake_at), Some(old_wake_at)) => new_wake_at <= old_wake_at,
    }
}

pub(super) struct Task {
    context: CallContext,
    /// Another run's group whose future the task awaits.
    awaiting: Option<TaskGroupId>,
    /// The task only carries a thread Luau code drives, such as a coroutine, through one wait; a
    /// plain yield hands the thread back.
    borrows_thread: bool,
    /// Polled ahead of the ready tasks that aren't.
    watcher: bool,
    /// In a ready queue, or in the round under way's.
    queued: bool,
    /// The Luau thread the task runs, until it settles, whatever its work is at the moment: what
    /// cancelling the task closes.
    thread: Option<luau::Thread>,
    pub(super) work: Option<TaskWork>,
    state: TaskState,
    error: Option<Arc<str>>,
    output: Option<Vec<luau::Value>>,
    wake_at: Option<Instant>,
    pub(super) resume_limit: Option<ResumeLimit>,
    pub(super) completion: Option<Rc<LocalLuauTaskCompletion>>,
}
