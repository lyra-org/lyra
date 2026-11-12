use std::{
    collections::VecDeque,
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
        Wake,
        Waker,
    },
    time::Duration,
};

use super::TaskId;

/// Ends a scheduler's wait from any thread, for work its owner polls that the scheduler doesn't
/// track. A wake between two waits is kept for the next one.
#[derive(Clone)]
pub struct SchedulerWake {
    waker: Arc<OwnerWaker>,
}

impl SchedulerWake {
    pub(super) fn of(queue: Arc<WakeQueue>) -> Self {
        Self {
            waker: Arc::new(OwnerWaker { queue }),
        }
    }

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
pub(super) struct WakeQueue {
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

    pub(super) fn drain(&self) -> Vec<TaskId> {
        self.state().ids.drain(..).collect()
    }

    pub(super) fn has_pending_wake(&self) -> bool {
        !self.state().ids.is_empty()
    }

    pub(super) fn wait(&self, timeout: Option<Duration>) {
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

pub(super) struct TaskWaker {
    id: TaskId,
    queue: Arc<WakeQueue>,
}

impl TaskWaker {
    pub(super) fn of(id: TaskId, queue: &Arc<WakeQueue>) -> Waker {
        Waker::from(Arc::new(Self {
            id,
            queue: queue.clone(),
        }))
    }
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.queue.wake_task(self.id);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.queue.wake_task(self.id);
    }
}
