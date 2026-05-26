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
    LocalScheduler,
    TaskState,
    TokioRuntimeContext,
};

pub use crate::modules::{
    LuauRequireRuntime as RequireRuntime,
    LuauSourceCache as SourceCache,
    async_luau_callback as async_callback,
    install_luau_globals as install_globals,
    install_luau_module as install_module,
    install_luau_require as install_require,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ThreadDriveOptions {
    pub timeout: Duration,
    pub max_wait: Duration,
    pub idle_wait: Duration,
}

impl Default for ThreadDriveOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(300),
            max_wait: Duration::from_millis(25),
            idle_wait: Duration::from_millis(1),
        }
    }
}

pub fn drive_thread(
    tokio_runtime: &TokioRuntimeContext,
    scheduler: &LocalScheduler,
    thread: &luau::Thread,
    options: ThreadDriveOptions,
) -> Result<Vec<luau::Value>> {
    let deadline = Instant::now() + options.timeout;
    loop {
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

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let wait = scheduler
            .next_wake_delay()
            .unwrap_or(options.idle_wait)
            .min(options.max_wait)
            .min(remaining);
        scheduler.wait_for_wake(Some(wait));
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
            max_wait: Duration::from_millis(5),
            idle_wait: Duration::from_millis(1),
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
        assert!(scheduler.snapshot(handle.id()).is_some());
        scheduler.remove(handle.id());
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
