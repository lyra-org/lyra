use std::{
    future::Future,
    panic::{
        AssertUnwindSafe,
        catch_unwind,
    },
    pin::Pin,
    task::{
        Context,
        Poll,
    },
};

use super::Scheduler;
use super::TaskFuture;
use super::limit::{
    ResumeBound,
    ResumeLimit,
};
use super::time::Clock;
use anyhow::Result;
use harmony_luau as luau;
use harmony_luau::CallContext;
use std::cell::Cell;

/// Closes a thread the scheduler gives up: it will not resume it again, and neither may the Luau
/// code that holds it. One that is running, or has resumed another, is closed where it next
/// yields.
pub(super) fn close_cancelled_thread(thread: &luau::Thread) {
    if let Err(error) = thread.cancel() {
        tracing::warn!(
            vm = thread.vm_id(),
            thread = thread.state_id(),
            %error,
            "cancelled Luau thread was left open"
        );
    }
}

/// What a poll of Luau work reads and tells of the scheduler it runs in.
pub(super) struct Resuming<'a> {
    /// Whether the task or one of its groups was cancelled during this poll.
    pub(super) cancel_asked: &'a dyn Fn() -> bool,
    pub(super) limit: Option<&'a ResumeLimit>,
    pub(super) clock: &'a Clock,
    /// Set to the bound of the limit that cut a resume short.
    pub(super) interrupted_by: &'a Cell<Option<ResumeBound>>,
}

impl Resuming<'_> {
    /// Runs `resume`, which resumes `thread`, under the limit. A thread whose limit leaves it no
    /// time isn't resumed: it is closed, and fails as one interrupted at its first instruction.
    fn resume(
        &self,
        thread: &luau::Thread,
        resume: impl FnOnce() -> luau::runtime::Result<luau::ThreadStatus>,
    ) -> luau::runtime::Result<luau::ThreadStatus> {
        let limited = match self
            .limit
            .map(|limit| (limit, limit.guard(thread.vm(), self.clock)))
        {
            Some((_, Err(reached))) => {
                close_cancelled_thread(thread);
                self.interrupted_by.set(Some(reached));
                return Err(luau::Error::Runtime(luau::INTERRUPTED_ERROR.to_string()));
            }
            Some((limit, Ok(guard))) => Some((limit, guard)),
            None => None,
        };
        let result = resume();
        // An interruption is what the interrupt raised on this limit and the thread failed with:
        // not a failure that outlasted the limit, nor one that came after the thread had caught
        // the interrupt's.
        if result.as_ref().is_err_and(luau::Error::is_interruption)
            && let Some((_, (guard, bound))) = &limited
            && let Some(cause) = guard.interrupted()
        {
            let bound = match cause {
                luau::InterruptCause::Signal => ResumeBound::Signal,
                luau::InterruptCause::Time => *bound,
            };
            self.interrupted_by.set(Some(bound));
        }
        result
    }
}

pub(super) enum TaskWork {
    Future(TaskFuture),
    LuauThread {
        thread: luau::Thread,
        args: Vec<luau::Value>,
        pending_start: bool,
    },
    LuauFuture {
        thread: luau::Thread,
        future: luau::ScheduledFuture,
    },
}

pub(super) enum WorkPoll {
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

pub(super) fn poll_work(
    context: &CallContext,
    work: &mut TaskWork,
    cx: &mut Context<'_>,
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
            let previous = match thread.set_call_context(context.clone()) {
                Ok(previous) => previous,
                Err(error) => return WorkPoll::Failed(error.to_string().into()),
            };
            let result = resuming.resume(thread, || thread.resume(args));
            args.clear();
            poll_luau_resume(thread, result, previous)
        }
        TaskWork::LuauFuture { thread, future } => {
            let output = match catch_host_panic(context, || Pin::new(&mut *future).poll(cx)) {
                Ok(Poll::Ready(Ok(values))) => Ok(values),
                // Raised where the thread waits, as the call would have raised it had it not
                // waited, so a `pcall` around the call catches it.
                Ok(Poll::Ready(Err(error))) => Err(future.raised_error(&error)),
                Err(panic) => Err(future.raised_error(&luau::Error::Runtime(panic))),
                Ok(Poll::Pending) => return WorkPoll::Pending,
            };
            let thread = thread.clone();
            // The future is done; what is left is the thread itself.
            let finished = std::mem::replace(
                work,
                TaskWork::LuauThread {
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
            let previous = match thread.set_call_context(context.clone()) {
                Ok(previous) => previous,
                Err(error) => return WorkPoll::Failed(error.to_string().into()),
            };
            let result = resuming.resume(&thread, || match output {
                Ok(values) => thread.resume(values.as_slice()),
                Err(error) => thread.resume_error(&error),
            });
            poll_luau_resume(&thread, result, previous)
        }
    }
}

/// Drops finished or failed work, whose destructor runs host code too, most suspectly after
/// that code panicked.
pub(super) fn drop_catching_panic<T>(context: &CallContext, work: T) {
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

/// After a resume the thread gets back the context it had before: none for a
/// thread the scheduler runs, its own for a coroutine it resumed.
fn poll_luau_resume(
    thread: &luau::Thread,
    result: luau::runtime::Result<luau::ThreadStatus>,
    previous: Option<luau::CallContext>,
) -> WorkPoll {
    let _ = match previous {
        Some(previous) => thread.set_call_context(previous).map(|_| ()),
        None => thread.clear_call_context().map(|_| ()),
    };
    match result {
        Ok(luau::ThreadStatus::Completed(values)) => WorkPoll::Completed {
            output: Some(values),
        },
        Ok(luau::ThreadStatus::Closed) => WorkPoll::Cancelled,
        Ok(luau::ThreadStatus::Yielded(_)) => {
            if let Ok(scheduler) = thread.vm().data().get::<Scheduler>()
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
