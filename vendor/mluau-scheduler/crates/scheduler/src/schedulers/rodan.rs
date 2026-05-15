use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use futures_util::FutureExt;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use crate::{MaybeSend, MaybeSync, XRc};
use crate::taskmgr::{CoreActor, MaybeSendSyncFut, SchedulerImpl, ThreadData};


pub struct Inner {
    core_actor: CoreActor,
    cancel_token: CancellationToken,
    tracker: TaskTracker,
}


#[derive(Clone)]
pub struct CoreScheduler {
    inner: XRc<Inner>,
}

impl std::ops::Deref for CoreScheduler {
    type Target = Inner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl CoreScheduler {
    #[cfg(not(feature = "send"))]
    fn spawn<F>(&self, task: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
        {
        self.tracker.spawn_local(task)
    }

    #[cfg(feature = "send")]
    fn spawn<F>(&self, task: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.tracker.spawn(task)
    }
}

impl SchedulerImpl for CoreScheduler {
    fn new(core_actor: CoreActor) -> Self {
        Self {
            inner: XRc::new(Inner {
                core_actor,
                cancel_token: CancellationToken::new(),
                tracker: TaskTracker::new(),
            }),
        }
    }

    fn core_actor(&self) -> &CoreActor { &self.core_actor }

    fn parent_cancel_token(&self) -> Option<&CancellationToken> { Some(&self.cancel_token) }

    fn schedule_wait(&self, thread: mluau::Thread, duration: Duration) {
        let this = self.clone(); // Cheap Rc clone
        let start_at = Instant::now();
        let cancel_token = ThreadData::get_or_set(Some(&self.cancel_token), &thread, |data| data.cancel_token.clone());
        self.spawn(async move {
            tokio::select! {
                _ = tokio::time::sleep_until(start_at + duration) => {
                    this.core_actor.resume_thread(thread, Ok::<_, mluau::Error>(start_at.elapsed().as_secs_f64())); 
                }
                _ = cancel_token.cancelled() => {
                    // Task was cancelled, do nothing
                }
            }
        });
    }

    fn schedule_deferred(&self, thread: mluau::Thread, args: mluau::MultiValue) {
        let this = self.clone();
        let cancel_token = ThreadData::get_or_set(Some(&self.cancel_token), &thread, |data| data.cancel_token.clone());
        self.spawn(async move {
            tokio::select! {
                _ = tokio::task::yield_now() => {
                    this.core_actor.resume_thread(thread, Ok::<_, mluau::Error>(args)); 
                }
                _ = cancel_token.cancelled() => {
                    // Task was cancelled, do nothing
                }
            }
        });
    }

    fn schedule_delay(&self, thread: mluau::Thread, duration: Duration, args: mluau::MultiValue) {
        let this = self.clone(); // Cheap Rc clone
        let start_at = Instant::now();
        let cancel_token = ThreadData::get_or_set(Some(&self.cancel_token), &thread, |data| data.cancel_token.clone());
        self.spawn(async move {
            tokio::select! {
                _ = tokio::time::sleep_until(start_at + duration) => {
                    this.core_actor.resume_thread(thread, Ok::<_, mluau::Error>(args)); 
                }
                _ = cancel_token.cancelled() => {
                    // Task was cancelled, do nothing
                }
            }
        });
    }

    fn schedule_async<F>(&self, thread: mluau::Thread, fut: F) 
    where 
        F: Future<Output = mluau::Result<mluau::MultiValue>> + MaybeSend + MaybeSync + 'static 
    {
        let fut = std::panic::AssertUnwindSafe(fut).catch_unwind();
        let this = self.clone(); // Cheap Rc clone
        let cancel_token = ThreadData::get_or_set(Some(&self.cancel_token), &thread, |data| data.cancel_token.clone());
        self.spawn(async move {
            tokio::select! {
                res = fut => {
                    match res {
                        Ok(val) => this.core_actor.resume_thread(thread, val),
                        Err(err) => this.core_actor.resume_thread_panic(thread, err),
                    }
                }
                _ = cancel_token.cancelled() => {
                    // Task was cancelled, do nothing
                }
            }
        });
    }

    fn schedule_async_dyn(&self, thread: mluau::Thread, fut: Pin<Box<dyn MaybeSendSyncFut<Output = mluau::Result<mluau::MultiValue>> + 'static>>) {
        self.schedule_async(thread, fut);
    }

    fn clone_box(&self) -> Box<dyn SchedulerImpl> { Box::new(self.clone()) }

    fn cancel_thread(&self, thread: &mluau::Thread) -> bool {
        match ThreadData::get_existing(thread) {
            Some(data) => {
                data.cancel_token.cancel();
                true
            },
            None => false,
        }
    }

    fn stop(&self) {
        self.tracker.close();
        self.cancel_token.cancel();
    }

    async fn wait_till_done(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.tracker.close();
        self.tracker.wait().await; // Wait for all running tasks to finish
        Ok(())
    }
}