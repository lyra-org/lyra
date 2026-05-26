// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::time::Duration;

use anyhow::{
    Result,
    bail,
};
use harmony_core::{
    LocalScheduler,
    TaskState,
    TokioRuntimeContext,
};
use harmony_luau as luau;

pub(super) fn drive_luau_thread(
    tokio_runtime: &TokioRuntimeContext,
    scheduler: &LocalScheduler,
    thread: &luau::Thread,
) -> Result<Vec<luau::Value>> {
    // HTTP-bound providers burn iteration count via incoming wakes faster
    // than wall-clock time, so an iter cap fires before the work completes.
    const DRIVE_LUAU_THREAD_BUDGET: Duration = Duration::from_secs(300);
    let deadline = std::time::Instant::now() + DRIVE_LUAU_THREAD_BUDGET;
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
                    bail!("plugin executor task {} failed: {error}", snapshot.id.0);
                }
                TaskState::Cancelled => {
                    scheduler.remove(handle.id());
                    bail!("plugin executor task {} was cancelled", snapshot.id.0);
                }
                TaskState::Pending => {}
            }
        } else {
            return Ok(Vec::new());
        }
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        let wait = scheduler
            .next_wake_delay()
            .unwrap_or_else(|| Duration::from_millis(1))
            .min(Duration::from_millis(25))
            .min(remaining);
        scheduler.wait_for_wake(Some(wait));
    }

    bail!(
        "plugin executor thread {}:{} did not complete",
        thread.vm_id(),
        thread.state_id()
    );
}
