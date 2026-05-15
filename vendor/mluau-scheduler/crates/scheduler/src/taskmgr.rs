use crate::{MaybeSend, MaybeSync, XRefCell};
use crate::XRc;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use mluau::WeakLua;
use tokio::sync::oneshot::{Sender, Receiver, channel};
use tokio_util::sync::CancellationToken;

type Error = Box<dyn std::error::Error + Send + Sync>;

pub struct ThreadData {
    pub cancel_token: CancellationToken,
    pub return_tracker: ReturnTracker,
    #[cfg(feature = "send")]
    pub ext: XRefCell<Option<Box<dyn std::any::Any + Send + Sync>>>,
    #[cfg(not(feature = "send"))]
    pub ext: XRefCell<Option<Box<dyn std::any::Any>>>,
}

impl ThreadData {
    /// Returns the underlying thread data if it exists
    pub fn get_existing(th: &mluau::Thread) -> Option<XRc<ThreadData>> {
        th.thread_data::<ThreadData>()
    }

    /// Gets the ThreadData, creating it if it doesn't exist
    pub fn get_or_set<R>(parent_cancel_token: Option<&CancellationToken>, th: &mluau::Thread, f: impl FnOnce(&ThreadData) -> R) -> R {
        match th.thread_data::<ThreadData>() {
            Some(data) => (f)(&data),
            None => {
                let token = match parent_cancel_token {
                    Some(parent) => parent.child_token(),
                    None => CancellationToken::new(),
                };
                let data = ThreadData {
                    cancel_token: token,
                    return_tracker: ReturnTracker::new(),
                    ext: XRefCell::new(None),
                };
                let r = (f)(&data);
                th.set_thread_data(data).expect("internal error: failed to set thread data");
                r
            },
        }
    }
}

/// The core actor of the scheduler
/// 
/// Can be implemented by any (v2/v3 etc.) scheduler version
/// 
/// Provides abstractions such as common thread resume code etc.
#[derive(Clone)]
pub struct CoreActor {
    lua: WeakLua,
    hooks: XRc<dyn Hooks>,
}

impl CoreActor {
    /// Creates a new core actor
    pub fn new(
        lua: WeakLua,
        hooks: XRc<dyn Hooks>,
    ) -> Self {
        Self {
            lua,
            hooks,
        }
    }

    /// Returns if the underlying Lua state is still valid
    pub fn is_lua_valid(&self) -> bool {
        self.lua.strong_count() > 0
    }

    /// Helper method to create a error payload from a panicked async task
    pub fn create_panic_error(&self, err: Box<dyn std::any::Any + Send>) -> mluau::Error {
        let mut error_payload = format!("Error in async thread: {err:?}");
        if let Some(error) = err.downcast_ref::<String>() {
            error_payload = format!("Error in async thread: {error}");
        }
        if let Some(error) = err.downcast_ref::<&str>() {
            error_payload = format!("Error in async thread: {error}");
        }

        mluau::Error::external(error_payload)
    }

    /// Helper method to resume a thread with an error from a panicked async task
    pub fn resume_thread_panic(&self, thread: mluau::Thread, err: Box<dyn std::any::Any + Send>) {
        let error_payload = self.create_panic_error(err);
        self.resume_thread(thread, Err::<(), mluau::Error>(error_payload));
    }

    /// Resumes a Lua thread with the given arguments, handling cancellation and Lua state validity
    /// If args is Ok(T), resumes with values T; if Err(E), resumes with error E
    pub fn resume_thread(&self, thread: mluau::Thread, args: Result<impl mluau::IntoLuaMulti, impl mluau::IntoLua>) {
        if self.lua.strong_count() == 0 { 
            return; 
        }

        if matches!(thread.status(), mluau::ThreadStatus::Error | mluau::ThreadStatus::Finished) {
            return;
        }

        self.hooks.on_resume(&thread);
        if let Some(data) = ThreadData::get_existing(&thread) && data.return_tracker.is_tracking() {
            let result = match args {
                Ok(v) => thread.resume(v),
                Err(e) => thread.resume_error(e),
            };
            data.return_tracker.push_result(&thread, result);
        } else {
            // fast-path: we aren't tracking this thread, so no need to store the result
            let _ = match args {
                Ok(v) => thread.resume::<()>(v),
                Err(e) => thread.resume_error::<()>(e),
            };
        }
    }

    /// Resumes a thread with the given arguments and returns the result
    pub fn resume_thread_result(&self, thread: mluau::Thread, args: impl mluau::IntoLuaMulti) -> mluau::Result<mluau::MultiValue> {
        if self.lua.strong_count() == 0 { 
            return Err(mluau::Error::external("Lua state has been dropped")); 
        }

        self.hooks.on_resume(&thread);
        let result = thread.resume(args);
        if let Some(data) = ThreadData::get_existing(&thread) {
            data.return_tracker.push_result(&thread, result.clone());
        }
        result
    }
}

/// A tracker for thread return values
pub struct ReturnTracker {
    return_tracker: XRefCell<Option<Sender<mluau::Result<mluau::MultiValue>>>>
}

impl ReturnTracker {
    pub fn new() -> Self {
        Self {
            return_tracker: XRefCell::new(None)
        }
    }

    pub fn is_tracking(&self) -> bool {
        self.return_tracker.borrow().is_some()
    }

    // No-op if thread is not finished or not tracked
    pub fn push_result(&self, th: &mluau::Thread, result: mluau::Result<mluau::MultiValue>) {
        if matches!(th.status(), mluau::ThreadStatus::Error | mluau::ThreadStatus::Finished) {
            let v = self.return_tracker.borrow_mut().take();
            if let Some(tx) = v {
                let _ = tx.send(result);
            }
        }
    }

    pub fn track(&self) -> Receiver<mluau::Result<mluau::MultiValue>> {
        let (tx, rx) = channel();
        let mut tracker = self.return_tracker.borrow_mut();
        *tracker = Some(tx);
        rx
    }

    pub fn stop_track(&self) {
        let mut tracker = self.return_tracker.borrow_mut();
        *tracker = None;
    }
}


pub trait Hooks: MaybeSend + MaybeSync {
    fn on_resume(&self, _thread: &mluau::Thread) {}
}

pub struct NoopHooks;
impl Hooks for NoopHooks {}

pub trait MaybeSendSyncFut: futures_util::Future + MaybeSend + MaybeSync {}
impl<T: futures_util::Future + MaybeSend + MaybeSync> MaybeSendSyncFut for T {}

pub trait ExtState: std::any::Any + MaybeSend + MaybeSync + 'static {}
impl<T: MaybeSend + MaybeSync + 'static> ExtState for T {}

pub(crate) struct SchedulerVTable(pub(crate) Box<dyn SchedulerImpl>);

/// A trait that defines the scheduler implementation
#[allow(async_fn_in_trait)]
pub trait SchedulerImpl: MaybeSend + MaybeSync {
    /// Creates a new scheduler implementation
    /// 
    /// Note that some schedulers may require async initialization, in which case
    /// `new_async` should be used instead.
    fn new(core_actor: CoreActor) -> Self where Self: Sized;

    /// Asynchronously creates a new scheduler implementation
    async fn new_async(core_actor: CoreActor) -> Result<Self, Error> 
    where Self: Sized 
    {
        Ok(Self::new(core_actor))
    }

    /// Returns the core actor of the scheduler
    fn core_actor(&self) -> &CoreActor;

    /// Returns the underlying parent cancellation token if any
    fn parent_cancel_token(&self) -> Option<&CancellationToken>;

    /// Schedules a waiting thread
    fn schedule_wait(&self, thread: mluau::Thread, duration: Duration);

    /// Schedules a deferred thread to the front of the queue
    fn schedule_deferred(&self, thread: mluau::Thread, args: mluau::MultiValue);

    /// Schedules a delayed thread with arguments after a duration
    fn schedule_delay(&self, thread: mluau::Thread, duration: Duration, args: mluau::MultiValue);

    /// Schedules an async future to resume the thread when complete
    fn schedule_async<F>(&self, thread: mluau::Thread, fut: F) 
    where 
        Self: Sized,
        F: Future<Output = mluau::Result<mluau::MultiValue>> + MaybeSend + MaybeSync + 'static;

    /// Cancels a thread
    fn cancel_thread(&self, thread: &mluau::Thread) -> bool;
    
    /// Waits till all scheduled tasks are done
    async fn wait_till_done(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where Self: Sized;

    /// Stops the scheduler
    fn stop(&self);

    /// Spawns a thread and then proceeds to get its output properly
    /// 
    /// Note: This method is pre-provided and does not need to be implemented by the scheduler
    async fn run_in_scheduler(
        &self,
        thread: mluau::Thread,
        args: mluau::MultiValue,
    ) -> mluau::Result<mluau::MultiValue> 
    where Self: Sized
    {
        let rx = ThreadData::get_or_set(self.parent_cancel_token(), &thread, |data| data.return_tracker.track());

        let value = thread.resume(args);

        if matches!(thread.status(), mluau::ThreadStatus::Error | mluau::ThreadStatus::Finished) {
            if let Some(data) = ThreadData::get_existing(&thread) {
                data.return_tracker.stop_track();
            }
            return value;
        }

        let v = rx.await.map_err(|_| mluau::Error::external("Failed to receive thread result"))?;
        v
    }

    /// Sets up a new task manager and stores it in the provided Lua instance's app data
    /// 
    /// Note: This method is pre-provided and does not need to be implemented by the scheduler
    /// 
    /// Note that some schedulers may require async initialization, in which case
    /// `setup_async` should be used instead.
    fn setup(lua: &mluau::Lua, hooks: XRc<dyn Hooks>) -> Result<Self, Error> 
    where Self: Clone + 'static 
    {
        let inner = Self::new(CoreActor::new(lua.weak(), hooks));

        lua.set_app_data(inner.clone());
        lua.set_app_data(SchedulerVTable(inner.clone_box()));

        Ok(inner)
    }

    /// Asynchronously sets up a new task manager and stores it in the provided Lua instance's app data
    /// 
    /// Note: This method is pre-provided and does not need to be implemented by the scheduler
    async fn setup_async(lua: &mluau::Lua, hooks: XRc<dyn Hooks>) -> Result<Self, Error> 
    where Self: Clone + 'static
    {
        let inner = Self::new_async(CoreActor::new(lua.weak(), hooks)).await?;

        lua.set_app_data(inner.clone());
        lua.set_app_data(SchedulerVTable(Box::new(inner.clone())));

        Ok(inner)
    }

    /// Retrieves the task manager from the Lua instance's app data
    /// 
    /// Note: This method is pre-provided and does not need to be implemented by the scheduler
    fn get(lua: &'_ mluau::Lua) -> Self 
    where Self: Clone + 'static 
    {
        lua.app_data_ref::<Self>()
            .expect("Failed to get task manager")
            .clone()
    }

    /// Schedules an async future to resume the thread when complete
    fn schedule_async_dyn(&self, thread: mluau::Thread, fut: Pin<Box<dyn MaybeSendSyncFut<Output = mluau::Result<mluau::MultiValue>> + 'static>>);

    /// Clones the scheduler into a boxed trait object
    fn clone_box(&self) -> Box<dyn SchedulerImpl>;
}