// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Replaces the plugin runtime when its executor thread crashes. A crashed
//! executor's state is unknown, so the runtime is rebuilt through the reload
//! path rather than kept running.

use std::{
    collections::VecDeque,
    sync::{
        Arc,
        Mutex,
        PoisonError,
    },
    time::{
        Duration,
        Instant,
    },
};

use anyhow::{
    Context,
    Result,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::plugins::{
    bootstrap::PluginRuntime,
    executor::{
        ExecutorExitKind,
        ExecutorId,
    },
    lifecycle::PublishOutcome,
    state::GenerationState,
};

/// Restarts allowed within [`CRASH_WINDOW`]; a crash past them leaves plugins
/// stopped. A few restarts ride out a rare panic, while a plugin that crashes
/// the runtime as it starts runs out of them within minutes instead of
/// restarting forever.
const MAX_RESTARTS: usize = 3;
const CRASH_WINDOW: Duration = Duration::from_secs(10 * 60);

/// Carries the crashes of the runtimes this generation publishes to the task
/// supervising them.
pub(crate) struct RuntimeCrashes {
    tx: mpsc::UnboundedSender<ExecutorId>,
    rx: Mutex<Option<mpsc::UnboundedReceiver<ExecutorId>>>,
    observed: Mutex<Option<ExecutorId>>,
}

impl Default for RuntimeCrashes {
    fn default() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            tx,
            rx: Mutex::new(Some(rx)),
            observed: Mutex::new(None),
        }
    }
}

impl RuntimeCrashes {
    /// Reports `runtime` if its executor thread crashes, including before it
    /// was observed. Observing the same runtime again does nothing.
    pub(crate) fn observe(&self, runtime: &PluginRuntime) {
        let exit = runtime.exit();
        let id = exit.id();
        if self
            .observed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .replace(id)
            == Some(id)
        {
            return;
        }
        let tx = self.tx.clone();
        tokio::spawn(async move {
            if exit.wait().await == ExecutorExitKind::Crashed {
                let _ = tx.send(id);
            }
        });
    }

    fn take_receiver(&self) -> Option<mpsc::UnboundedReceiver<ExecutorId>> {
        self.rx
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }
}

/// Supervises the runtimes the current generation publishes, until
/// `shutdown`, which also cuts short a restart in progress.
pub(crate) fn spawn(shutdown: CancellationToken) -> Result<tokio::task::JoinHandle<()>> {
    let generation = crate::plugins::STATE.generation();
    let crashes = generation
        .crashes
        .take_receiver()
        .context("plugin runtime is already supervised")?;
    Ok(tokio::spawn(
        Supervisor::new(generation, shutdown).run(crashes),
    ))
}

#[derive(Debug, PartialEq, Eq)]
enum CrashOutcome {
    Restarted,
    /// A reload had already replaced the crashed runtime.
    Replaced,
    /// The reload ran and failed; plugins stay stopped.
    RestartFailed,
    /// Too many restarts within the window.
    GaveUp,
    ShuttingDown,
}

struct Supervisor {
    generation: Arc<GenerationState>,
    shutdown: CancellationToken,
    max_restarts: usize,
    window: Duration,
    restarts: VecDeque<Instant>,
}

impl Supervisor {
    fn new(generation: Arc<GenerationState>, shutdown: CancellationToken) -> Self {
        Self {
            generation,
            shutdown,
            max_restarts: MAX_RESTARTS,
            window: CRASH_WINDOW,
            restarts: VecDeque::new(),
        }
    }

    #[cfg(test)]
    fn with_limit(mut self, max_restarts: usize, window: Duration) -> Self {
        self.max_restarts = max_restarts;
        self.window = window;
        self
    }

    async fn run(mut self, mut crashes: mpsc::UnboundedReceiver<ExecutorId>) {
        loop {
            let crashed = tokio::select! {
                biased;
                () = self.shutdown.cancelled() => return,
                crashed = crashes.recv() => match crashed {
                    Some(crashed) => crashed,
                    None => return,
                },
            };
            self.handle_crash(crashed, Instant::now()).await;
        }
    }

    /// Handles the crash of the runtime `crashed`, reported at `now`.
    async fn handle_crash(&mut self, crashed: ExecutorId, now: Instant) -> CrashOutcome {
        tracing::error!(executor = %crashed, "plugin runtime crashed");
        while self
            .restarts
            .front()
            .is_some_and(|at| now.duration_since(*at) >= self.window)
        {
            self.restarts.pop_front();
        }
        if self.restarts.len() >= self.max_restarts {
            tracing::error!(
                restarts = self.restarts.len(),
                window_secs = self.window.as_secs(),
                "plugin runtime keeps crashing; plugins stay stopped until the next reload"
            );
            return CrashOutcome::GaveUp;
        }
        // Counted once attempted: a restart that fails or crashes again is
        // what the limit stops repeating.
        self.restarts.push_back(now);

        tracing::warn!(executor = %crashed, "restarting plugin runtime");
        match self
            .generation
            .registries
            .reload_if_published(crashed, self.shutdown.clone())
            .await
        {
            Ok(PublishOutcome::Published) => {
                tracing::info!("plugin runtime restarted after a crash");
                CrashOutcome::Restarted
            }
            Ok(PublishOutcome::Replaced) => {
                self.restarts.pop_back();
                tracing::info!(executor = %crashed, "crashed plugin runtime was already replaced");
                CrashOutcome::Replaced
            }
            Ok(PublishOutcome::ShuttingDown) => CrashOutcome::ShuttingDown,
            Err(_) => {
                tracing::error!("plugin runtime was not restarted; plugins stay stopped");
                CrashOutcome::RestartFailed
            }
        }
    }
}

#[cfg(test)]
#[path = "supervisor/tests.rs"]
mod tests;
