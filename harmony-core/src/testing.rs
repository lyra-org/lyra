// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! What a test of scheduled work needs in place of real time.

use std::{
    cell::Cell,
    rc::Rc,
    time::{
        Duration,
        Instant,
    },
};

use harmony_luau as luau;

use crate::{
    LocalScheduler,
    TokioRuntimeContext,
};

/// A scheduler's time, moved only by [`Self::advance`]. Its clones share one time, so a test keeps
/// one and gives another to [`crate::LocalScheduler::with_manual_clock`] or
/// [`crate::plugin::RuntimeBuilder::manual_clock`].
#[derive(Clone)]
pub struct ManualClock {
    now: Rc<Cell<Instant>>,
}

impl ManualClock {
    /// Starts at the real time, read once.
    pub fn new() -> Self {
        Self {
            now: Rc::new(Cell::new(Instant::now())),
        }
    }

    pub fn now(&self) -> Instant {
        self.now.get()
    }

    /// Moves the time forward. Nothing runs for it: the scheduler's next poll wakes what came due.
    ///
    /// # Panics
    ///
    /// When the time would pass what an [`Instant`] holds.
    pub fn advance(&self, duration: Duration) {
        let now = self
            .now
            .get()
            .checked_add(duration)
            .expect("a manual clock can't advance past the last Instant");
        self.now.set(now);
    }
}

impl Default for ManualClock {
    fn default() -> Self {
        Self::new()
    }
}

/// [`crate::luau::drive_thread`] for a test that decides when time passes, as one on a manual
/// clock must: runs rounds for as long as the scheduler has a task ready, and returns what
/// `thread` returned. A thread left waiting, for a delay or for work elsewhere, fails and is
/// cancelled.
pub fn drive_thread_while_ready(
    tokio_runtime: &TokioRuntimeContext,
    scheduler: &LocalScheduler,
    thread: &luau::Thread,
) -> anyhow::Result<Vec<luau::Value>> {
    loop {
        if let Some(values) = crate::luau::poll_thread(tokio_runtime, scheduler, thread)? {
            return Ok(values);
        }
        if scheduler.next_wake_delay() != Some(Duration::ZERO) {
            crate::luau::abandon_thread(scheduler, thread);
            anyhow::bail!(
                "Luau thread {}:{} waits with nothing ready to run",
                thread.vm_id(),
                thread.state_id()
            );
        }
    }
}
