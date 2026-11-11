use std::time::{
    Duration,
    Instant,
};

use anyhow::{
    Result,
    bail,
};
use harmony_luau as luau;

use crate::{
    Cancellation,
    LocalScheduler,
    TaskState,
    TokioRuntimeContext,
};

pub use crate::modules::{
    LuauRequireRuntime as RequireRuntime,
    async_luau_callback as async_callback,
    install_luau_globals as install_globals,
    install_luau_module as install_module,
};

#[derive(Clone, Debug)]
pub struct ThreadDriveOptions {
    pub timeout: Duration,
    /// Ends the drive once cancelled; made from the driven scheduler's wake handle, so that
    /// cancelling also ends the drive's wait.
    pub cancellation: Option<Cancellation>,
}

impl Default for ThreadDriveOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(300),
            cancellation: None,
        }
    }
}

pub fn drive_thread(
    tokio_runtime: &TokioRuntimeContext,
    scheduler: &LocalScheduler,
    thread: &luau::Thread,
    options: ThreadDriveOptions,
) -> Result<Vec<luau::Value>> {
    // A timeout too long to add to now has no deadline to reach.
    let deadline = Instant::now().checked_add(options.timeout);
    loop {
        if options
            .cancellation
            .as_ref()
            .is_some_and(Cancellation::is_cancelled)
        {
            let task_id = scheduler
                .luau_thread_handle(thread)
                .map(|handle| handle.id());
            scheduler.cancel_luau_thread(thread);
            if let Some(task_id) = task_id {
                scheduler.remove(task_id);
            }
            bail!(
                "Luau thread {}:{} was cancelled",
                thread.vm_id(),
                thread.state_id()
            );
        }
        {
            let _guard = tokio_runtime.enter();
            scheduler.poll_ready();
        }
        let Some(handle) = scheduler.luau_thread_handle(thread) else {
            return Ok(Vec::new());
        };
        if let Some(snapshot) = scheduler.snapshot(handle.id()) {
            match snapshot.state {
                TaskState::Completed => {
                    let output = scheduler
                        .take_luau_thread_output(thread)
                        .unwrap_or_default();
                    scheduler.remove(handle.id());
                    return Ok(output);
                }
                TaskState::Failed => {
                    let error = snapshot.error.as_deref().unwrap_or("unknown error");
                    scheduler.remove(handle.id());
                    bail!("Luau task {} failed: {error}", snapshot.id.0);
                }
                TaskState::Cancelled => {
                    scheduler.remove(handle.id());
                    bail!("Luau task {} was cancelled", snapshot.id.0);
                }
                TaskState::Pending => {}
            }
        } else {
            return Ok(Vec::new());
        }

        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            break;
        }
        scheduler.wait_for_work(deadline);
    }

    let task_id = scheduler
        .luau_thread_handle(thread)
        .map(|handle| handle.id());
    scheduler.cancel_luau_thread(thread);
    if let Some(task_id) = task_id {
        scheduler.remove(task_id);
    }
    bail!(
        "Luau thread {}:{} did not complete",
        thread.vm_id(),
        thread.state_id()
    );
}

#[cfg(test)]
mod tests {
    use std::{
        sync::Arc,
        time::Duration,
    };

    use super::*;
    use crate::CallContext;

    fn runtime() -> Result<TokioRuntimeContext> {
        TokioRuntimeContext::new()
    }

    fn options(timeout: Duration) -> ThreadDriveOptions {
        ThreadDriveOptions {
            timeout,
            cancellation: None,
        }
    }

    #[test]
    fn completed_thread_returns_values_and_removes_task() -> Result<()> {
        let tokio_runtime = runtime()?;
        let vm = luau::Vm::new()?;
        let scheduler = LocalScheduler::new();
        let function = vm.load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(&b"return 42, 'done'"[..]),
            luau::ChunkOrigin::default(),
        ))?;
        let thread = vm.create_thread(&function)?;
        let handle =
            scheduler.spawn_luau_thread(CallContext::default(), vm, thread.clone(), Vec::new());

        let values = drive_thread(
            &tokio_runtime,
            &scheduler,
            &thread,
            options(Duration::from_secs(1)),
        )?;

        assert_eq!(
            values,
            vec![
                luau::Value::Number(42.0),
                luau::Value::String(b"done".to_vec())
            ]
        );
        assert!(scheduler.snapshot(handle.id()).is_none());
        Ok(())
    }

    #[test]
    fn timeout_past_instant_range_drives_without_a_deadline() -> Result<()> {
        let tokio_runtime = runtime()?;
        let vm = luau::Vm::new()?;
        let scheduler = LocalScheduler::new();
        let function = vm.load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(&b"return 1"[..]),
            luau::ChunkOrigin::default(),
        ))?;
        let thread = vm.create_thread(&function)?;
        scheduler.spawn_luau_thread(CallContext::default(), vm, thread.clone(), Vec::new());

        let values = drive_thread(&tokio_runtime, &scheduler, &thread, options(Duration::MAX))?;

        assert_eq!(values, vec![luau::Value::Number(1.0)]);
        Ok(())
    }

    #[test]
    fn failed_thread_returns_error_and_removes_task() -> Result<()> {
        let tokio_runtime = runtime()?;
        let vm = luau::Vm::new()?;
        vm.open_standard_libraries(luau::StandardLibraries {
            base: true,
            ..luau::StandardLibraries::none()
        })?;
        let scheduler = LocalScheduler::new();
        let function = vm.load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(&b"error('boom')"[..]),
            luau::ChunkOrigin::default(),
        ))?;
        let thread = vm.create_thread(&function)?;
        let handle =
            scheduler.spawn_luau_thread(CallContext::default(), vm, thread.clone(), Vec::new());

        let error = drive_thread(
            &tokio_runtime,
            &scheduler,
            &thread,
            options(Duration::from_secs(1)),
        )
        .expect_err("thread should fail");

        assert!(error.to_string().contains("boom"));
        assert!(scheduler.snapshot(handle.id()).is_none());
        Ok(())
    }

    #[test]
    fn cancelled_thread_returns_error_and_removes_task() -> Result<()> {
        let tokio_runtime = runtime()?;
        let vm = luau::Vm::new()?;
        let scheduler = LocalScheduler::new();
        let function = vm.load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(&b"return 1"[..]),
            luau::ChunkOrigin::default(),
        ))?;
        let thread = vm.create_thread(&function)?;
        let handle =
            scheduler.spawn_luau_thread(CallContext::default(), vm, thread.clone(), Vec::new());
        assert!(scheduler.cancel(handle.id()));

        let error = drive_thread(
            &tokio_runtime,
            &scheduler,
            &thread,
            options(Duration::from_secs(1)),
        )
        .expect_err("thread should be cancelled");

        assert!(error.to_string().contains("cancelled"));
        assert!(scheduler.snapshot(handle.id()).is_none());
        Ok(())
    }

    #[test]
    fn cancellation_flag_stops_and_removes_thread() -> Result<()> {
        let tokio_runtime = runtime()?;
        let vm = luau::Vm::new()?;
        let scheduler = LocalScheduler::new();
        let function = vm.load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(&b"coroutine.yield(); return 1"[..]),
            luau::ChunkOrigin::default(),
        ))?;
        let thread = vm.create_thread(&function)?;
        let handle =
            scheduler.spawn_luau_thread(CallContext::default(), vm, thread.clone(), Vec::new());
        let cancellation = Cancellation::new(scheduler.wake_handle());
        cancellation.cancel();
        let mut options = options(Duration::from_secs(1));
        options.cancellation = Some(cancellation);

        let error = drive_thread(&tokio_runtime, &scheduler, &thread, options)
            .expect_err("thread should be cancelled");

        assert!(error.to_string().contains("cancelled"));
        assert!(scheduler.snapshot(handle.id()).is_none());
        Ok(())
    }

    #[test]
    fn pending_thread_times_out() -> Result<()> {
        let tokio_runtime = runtime()?;
        let vm = luau::Vm::new()?;
        vm.open_standard_libraries(luau::StandardLibraries::all_supported())?;
        let scheduler = LocalScheduler::new();
        let function = vm.load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(&b"coroutine.yield(); return 1"[..]),
            luau::ChunkOrigin::default(),
        ))?;
        let thread = vm.create_thread(&function)?;
        let handle =
            scheduler.spawn_luau_thread(CallContext::default(), vm, thread.clone(), Vec::new());

        let error = drive_thread(
            &tokio_runtime,
            &scheduler,
            &thread,
            options(Duration::from_millis(10)),
        )
        .expect_err("thread should time out");

        assert!(error.to_string().contains("did not complete"));
        assert!(scheduler.snapshot(handle.id()).is_none());
        Ok(())
    }

    #[test]
    fn delayed_thread_completes_after_wake() -> Result<()> {
        let tokio_runtime = runtime()?;
        let vm = luau::Vm::new()?;
        let scheduler = LocalScheduler::new();
        let function = vm.load_chunk(&luau::Chunk::new(
            Arc::<[u8]>::from(&b"return 7"[..]),
            luau::ChunkOrigin::default(),
        ))?;
        let thread = vm.create_thread(&function)?;
        scheduler.spawn_luau_thread_after(
            CallContext::default(),
            Duration::from_millis(10),
            vm,
            thread.clone(),
            Vec::new(),
        );

        let values = drive_thread(
            &tokio_runtime,
            &scheduler,
            &thread,
            options(Duration::from_secs(1)),
        )?;

        assert_eq!(values, vec![luau::Value::Number(7.0)]);
        Ok(())
    }
}
