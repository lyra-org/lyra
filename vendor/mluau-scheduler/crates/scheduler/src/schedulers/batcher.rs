use crate::taskmgr::CoreActor;
use crate::taskmgr::SchedulerImpl;
use crate::taskmgr::ThreadData;
use crate::XRc;
use futures_util::stream::FuturesUnordered;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::StreamExt;
use futures_util::TryFutureExt;
use tokio_util::sync::CancellationToken;
use std::collections::VecDeque;
use std::pin::Pin;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::watch::{Receiver, Sender};
use tokio_util::time::delay_queue::Key as DelayQueueKey;
use tokio_util::time::DelayQueue;
use tokio::sync::oneshot::Sender as OneShotSender;

pub struct WaitingThread {
    delay_args: Option<mluau::MultiValue>,
    start_at: std::time::Instant,
    thread: mluau::Thread,
}

pub enum SchedulerEvent {
    // task.wait / task.delay semantics
    Wait {
        delay_args: Option<mluau::MultiValue>,
        thread: mluau::Thread,
        start_at: std::time::Instant,
        duration: std::time::Duration,
    },
    RemoveWait {
        key: DelayQueueKey,
    },
    DeferredThread {
        thread: mluau::Thread,
        args: mluau::MultiValue,
    },
    AddAsync {
        thread: mluau::Thread,
        #[cfg(feature = "send")]
        fut: Pin<Box<dyn Future<Output = mluau::Result<mluau::MultiValue>> + Send + Sync>>,
        #[cfg(not(feature = "send"))]
        fut: Pin<Box<dyn Future<Output = mluau::Result<mluau::MultiValue>>>>,
    },
    Clear {},
    Close {},
}


pub struct CoreSchedulerInner {
    cancel_token: CancellationToken,

    // tx/rx
    tx: UnboundedSender<SchedulerEvent>,

    done_tx: Sender<bool>,
    done_rx: tokio::sync::RwLock<Receiver<bool>>,

    core_actor: CoreActor,
}

#[derive(Clone)]
struct ExtThreadData {
    wait_key: DelayQueueKey,
}

/// Inner scheduler v2
#[derive(Clone)]
pub struct CoreScheduler {
    inner: XRc<CoreSchedulerInner>,
}

impl std::ops::Deref for CoreScheduler {
    type Target = CoreSchedulerInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

type Error = Box<dyn std::error::Error + Send + Sync>;

#[cfg(feature = "send")]
type Fut = Pin<Box<dyn Future<Output = Result<(mluau::Thread, mluau::Result<mluau::MultiValue>), (mluau::Thread, Box<dyn std::any::Any + Send>)>> + Send>>;
#[cfg(not(feature = "send"))]
type Fut = Pin<Box<dyn Future<Output = Result<(mluau::Thread, mluau::Result<mluau::MultiValue>), (mluau::Thread, Box<dyn std::any::Any + Send>)>>>>;

impl CoreScheduler {
    /// Creates a new task manager and spawns it
    pub async fn new(core_actor: CoreActor) -> Result<Self, Error> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let (done_tx, done_rx) = tokio::sync::watch::channel(true);
        let scheduler_inner = CoreSchedulerInner {
            core_actor,
            cancel_token: CancellationToken::new(),
            tx,
            done_tx,
            done_rx: tokio::sync::RwLock::new(done_rx),
        };

        let scheduler = CoreScheduler {
            inner: XRc::new(scheduler_inner),
        };

        {
            let (start_tx, start_rx) = tokio::sync::oneshot::channel();
            #[cfg(feature = "send")]
            {
                tokio::task::spawn({
                    let scheduler = scheduler.clone();
                    async move {
                        scheduler.run(start_tx, rx).await;
                    }
                });
            }

            #[cfg(not(feature = "send"))]
            {
                tokio::task::spawn_local({
                    let scheduler = scheduler.clone();
                    async move {
                        scheduler.run(start_tx, rx).await;
                    }
                });
            }

            start_rx.await?;
        }

        Ok(scheduler)
    }

    fn handle_event(
        &self, 
        event: SchedulerEvent,
        wait_queue: &mut DelayQueue<WaitingThread>,
        async_queue: &mut FuturesUnordered<Fut>,
        run_queue: &mut VecDeque<(mluau::Thread, Result<mluau::MultiValue, mluau::Error>)>,
    ) {
        match event {
            SchedulerEvent::Wait { delay_args, thread, start_at, duration } => {
                let key = wait_queue.insert(
                    WaitingThread {
                        delay_args,
                        start_at,
                        thread: thread.clone(),
                    },
                    duration
                );

                ThreadData::get_or_set(Some(&self.cancel_token), &thread, |data| {
                    let ext_thread_data = ExtThreadData {
                        wait_key: key,
                    };
                    *data.ext.borrow_mut() = Some(Box::new(ext_thread_data));
                });
            },
            SchedulerEvent::RemoveWait { key } => {
                wait_queue.remove(&key);
            },
            SchedulerEvent::DeferredThread { thread, args } => {
                run_queue.push_back((thread, Ok(args)));
            },
            SchedulerEvent::AddAsync { thread, fut } => {
                let thread_err = thread.clone();
                let fut = std::panic::AssertUnwindSafe(
                        fut
                        .map(move |x| (thread, x))
                    )
                    .catch_unwind()
                    .map_err(move |e| (thread_err, e));
                async_queue.push(
                    Box::pin(fut)
                );
            }
            SchedulerEvent::Clear {} => {
                wait_queue.clear();
                async_queue.clear();
                run_queue.clear();
            }
            SchedulerEvent::Close {} => {
                self.done_tx.send_replace(true);
            }
        }
    }
 
    /// Runs the task manager
    async fn run(&self, start_tx: OneShotSender<()>, mut rx: UnboundedReceiver<SchedulerEvent>) {
        let mut wait_queue: DelayQueue<WaitingThread> = DelayQueue::new();
        let mut async_queue = FuturesUnordered::new();

        let _ = start_tx.send(());

        let mut run_queue: VecDeque<(mluau::Thread, Result<mluau::MultiValue, mluau::Error>)> = VecDeque::with_capacity(1024);

        loop {
            if !self.core_actor.is_lua_valid() {
                log::trace!("Task manager is cancelled or lua is not valid, stopping task manager");
                self.done_tx.send_replace(true);
                return;
            }

            log::trace!("Task manager loop iteration");

            // Process anything in run queue now
            while let Some((thread, result)) = run_queue.pop_front() {
                if ThreadData::get_or_set(Some(&self.cancel_token), &thread, |data| {
                    data.cancel_token.is_cancelled()    
                }) {
                    continue;
                }

                self.core_actor.resume_thread(thread, result);
            }

            if async_queue.is_empty()
                && wait_queue.is_empty()
                && run_queue.is_empty()
                && rx.is_empty()
            {
                self.done_tx.send_replace(true);
            } else {
                self.done_tx.send_replace(false);
            }

            tokio::select! {
                _ = self.cancel_token.cancelled() => return,
                Some(event) = rx.recv() => {
                    self.handle_event(event, &mut wait_queue, &mut async_queue, &mut run_queue);
                }
                Some(value) = wait_queue.next() => {  
                    let thread = value.into_inner();
                    run_queue.push_back((thread.thread, match thread.delay_args {
                        Some(args) => Ok(args),
                        None => Ok(mluau::MultiValue::from_vec(vec![mluau::Value::Number(thread.start_at.elapsed().as_secs_f64())])),
                    }));
                }
                Some(res) = async_queue.next() => {
                    match res {
                        Ok((thread, result)) => {
                            run_queue.push_back((thread, result));
                        },
                        Err((thread, err)) => {
                            let error_payload = self.core_actor.create_panic_error(err);
                            run_queue.push_back((thread, Err(error_payload)));
                        }
                    }
                }
                else => {
                }
            };
        }
    }

    /// Stops the task manager
    pub fn stop(&self) {
        self.cancel_token.cancel();
    }

    /// Adds a waiting thread to the task manager
    pub fn push_event(&self, event: SchedulerEvent) {
        log::trace!("Pushing event");
        self.done_tx.send_replace(false);
        let err = self.tx.send(event);
        if let Err(e) = err {
            log::error!("Failed to push event: {e}");
        } else {
            log::trace!("Event pushed successfully");
        }
    }

    /// Cancels a thread
    pub fn cancel_thread(&self, thread: &mluau::Thread) -> Result<(), mluau::Error> {
        if let Some(existing) = ThreadData::get_existing(thread) {
            if let Some(ref ext) = *existing.ext.borrow() && let Some(key) = ext.downcast_ref::<ExtThreadData>() {
                self.push_event(SchedulerEvent::RemoveWait { key: key.wait_key });
            }
            existing.cancel_token.cancel();
        }
        thread.close()?;
        Ok(())
    }

    /// Clears the task manager queues completely
    pub fn clear(&self) {
        self.push_event(SchedulerEvent::Clear {});
    }

    pub async fn wait_till_done(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut done_rx = self.done_rx.write().await;
        done_rx.wait_for(|val| *val).await?;
        Ok(())
    }
}

impl SchedulerImpl for CoreScheduler {
    fn new(_: CoreActor) -> Self {
        unimplemented!("Use CoreScheduler::new_async instead");
    }

    fn core_actor(&self) -> &CoreActor { &self.core_actor }

    async fn new_async(core_actor: CoreActor) -> Result<Self, Error> 
    where Self: Sized {
        CoreScheduler::new(core_actor).await
    }

    fn parent_cancel_token(&self) -> Option<&CancellationToken> { Some(&self.cancel_token) }
    fn schedule_wait(&self, thread: mluau::Thread, duration: std::time::Duration) {
        self.push_event(SchedulerEvent::Wait {
            delay_args: None,
            thread,
            start_at: std::time::Instant::now(),
            duration,
        });
    }
    fn schedule_deferred(&self, thread: mluau::Thread, args: mluau::MultiValue) {
        self.push_event(SchedulerEvent::DeferredThread { thread, args });
    }
    fn schedule_delay(&self, thread: mluau::Thread, duration: std::time::Duration, args: mluau::MultiValue) {
        self.push_event(SchedulerEvent::Wait {
            delay_args: Some(args),
            thread,
            start_at: std::time::Instant::now(),
            duration,
        }); 
    }
    fn schedule_async<F>(&self, thread: mluau::Thread, fut: F) 
    where 
    F: Future<Output = mluau::Result<mluau::MultiValue>> + crate::MaybeSend + crate::MaybeSync + 'static 
    {
        self.push_event(SchedulerEvent::AddAsync { thread, fut: Box::pin(fut) });    
    }

    fn schedule_async_dyn(&self, thread: mluau::Thread, fut: Pin<Box<dyn crate::taskmgr::MaybeSendSyncFut<Output = mluau::Result<mluau::MultiValue>> + 'static>>) {
        self.push_event(SchedulerEvent::AddAsync { thread, fut });
    }

    fn clone_box(&self) -> Box<dyn SchedulerImpl> { Box::new(self.clone()) }

    fn cancel_thread(&self, thread: &mluau::Thread) -> bool {
        self.cancel_thread(thread).ok().is_some()
    }

    async fn wait_till_done(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.wait_till_done().await
    }

    fn stop(&self) {
        self.stop();
    }
}