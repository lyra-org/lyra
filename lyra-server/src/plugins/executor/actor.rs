// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            AtomicU64,
            Ordering,
        },
        mpsc,
    },
    thread,
    time::Duration,
};

use anyhow::{
    Context,
    Result,
};
use harmony_core::{
    ModuleSpec,
    plugin::{
        PluginLoadError,
        PluginManifest,
    },
};

use futures::future::BoxFuture;
use tokio_util::sync::CancellationToken;

use super::messages::PluginExecutorCommand;
use super::{
    ApiHandlerRequest,
    ApiHandlerResponse,
    MetadataRefreshRequest,
    MetadataRefreshResult,
    MixHandlerRequest,
    MixHandlerResult,
    PluginExecutor,
    PluginSourceRun,
    SimilarReleasesDispatchRequest,
    SimilarReleasesDispatchResult,
    WebSocketStartRequest,
};
use crate::services::playback_sessions::PlaybackUpdatePayload;
use crate::services::plugin_dispatch::{
    DispatchNotStarted,
    PluginDispatch,
};

const EXECUTOR_QUEUE_CAPACITY: usize = 256;
/// Slots for lifecycle commands, apart from the ones dispatched handlers hold
/// for their whole run, so a saturated runtime can still be stopped.
const EXECUTOR_CONTROL_CAPACITY: usize = 16;

#[cfg(test)]
#[path = "actor/tests.rs"]
mod dispatch_tests;

#[derive(Clone)]
struct PluginExecutorSender {
    tx: mpsc::SyncSender<QueuedPluginExecutorCommand>,
    slots: Arc<tokio::sync::Semaphore>,
    capacity: u32,
    draining: Arc<AtomicBool>,
    control_slots: Arc<tokio::sync::Semaphore>,
}

impl PluginExecutorSender {
    async fn send(&self, command: PluginExecutorCommand) -> Result<()> {
        self.send_in(&self.slots, command).await
    }

    async fn send_control(&self, command: PluginExecutorCommand) -> Result<()> {
        self.send_in(&self.control_slots, command).await
    }

    async fn send_in(
        &self,
        slots: &Arc<tokio::sync::Semaphore>,
        command: PluginExecutorCommand,
    ) -> Result<()> {
        let permit = slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| DispatchNotStarted)?;
        self.enqueue(command, permit)
    }

    fn try_send(&self, command: PluginExecutorCommand) -> Result<()> {
        let permit = self.slots.clone().try_acquire_owned().map_err(|error| {
            if self.draining.load(Ordering::Acquire) {
                anyhow::Error::new(DispatchNotStarted)
            } else {
                anyhow::Error::new(error).context("plugin executor queue is full or unavailable")
            }
        })?;
        self.enqueue(command, permit)
    }

    fn enqueue(
        &self,
        command: PluginExecutorCommand,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<()> {
        self.tx
            .try_send(QueuedPluginExecutorCommand {
                command,
                _permit: permit,
            })
            .map_err(|_| DispatchNotStarted.into())
    }
}

struct QueuedPluginExecutorCommand {
    command: PluginExecutorCommand,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl QueuedPluginExecutorCommand {
    #[cfg(test)]
    fn into_command(self) -> PluginExecutorCommand {
        let Self { command, _permit } = self;
        drop(_permit);
        command
    }
}

/// How waiting for a runtime's calls to finish ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DrainOutcome {
    /// No call was left running or waiting.
    Drained,
    /// Calls were still running at the limit.
    TimedOut,
    Cancelled,
    /// The executor thread ended, taking its calls with it.
    Exited,
}

/// One executor thread, unique for the life of the process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ExecutorId(u64);

impl ExecutorId {
    fn next() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

impl std::fmt::Display for ExecutorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// How an executor thread ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExecutorExitKind {
    /// It shut down, or every handle to it was dropped.
    Stopped,
    /// It panicked, leaving its runtime state unknown.
    Crashed,
}

/// Watches one executor thread for its end, without keeping it alive.
#[derive(Clone)]
pub(crate) struct ExecutorExit {
    id: ExecutorId,
    crashed: tokio::sync::watch::Receiver<bool>,
}

impl ExecutorExit {
    pub(crate) fn id(&self) -> ExecutorId {
        self.id
    }

    pub(crate) async fn wait(mut self) -> ExecutorExitKind {
        match self.crashed.wait_for(|crashed| *crashed).await {
            Ok(_) => ExecutorExitKind::Crashed,
            Err(_) => ExecutorExitKind::Stopped,
        }
    }
}

/// The executor thread reports a crash on the sender; dropping it unsent
/// reports a stop.
fn exit_channel() -> (tokio::sync::watch::Sender<bool>, ExecutorExit) {
    let (crashed_tx, crashed) = tokio::sync::watch::channel(false);
    (
        crashed_tx,
        ExecutorExit {
            id: ExecutorId::next(),
            crashed,
        },
    )
}

#[derive(Clone)]
pub(crate) struct PluginExecutorHandle {
    tx: PluginExecutorSender,
    /// Interrupts the streams of a run that is asked to end, before the executor reads the ask.
    streams: Arc<super::stream::StreamStops>,
    /// Ends the entrypoint runs of a runtime asked to shut down, which hold its thread.
    entrypoints: Arc<std::sync::atomic::AtomicBool>,
    exit: ExecutorExit,
}

impl PluginExecutorHandle {
    pub(crate) fn discover_from_plugins_dir_with_db_and_modules(
        plugins_dir: impl Into<PathBuf>,
        server_info: crate::plugins::server::ServerInfo,
        auth_capabilities: crate::plugins::auth::AuthCapabilities,
        db: lyra_db::DbAsync,
        module_overrides: Vec<ModuleSpec>,
        settings: Option<crate::services::state::SettingsHandle>,
    ) -> Result<(Self, Vec<PluginLoadError>)> {
        let plugins_dir = plugins_dir.into();
        Self::spawn(move || {
            let mut stores = super::stores::PluginModuleStores::with_db(db);
            stores.server_settings = settings;
            PluginExecutor::discover_from_plugins_dir_with_stores(
                plugins_dir,
                server_info,
                auth_capabilities,
                stores,
                module_overrides,
            )
        })
    }

    /// Runs the executor `build` makes on its own thread.
    pub(crate) fn spawn(
        build: impl FnOnce() -> Result<(PluginExecutor, Vec<PluginLoadError>)> + Send + 'static,
    ) -> Result<(Self, Vec<PluginLoadError>)> {
        let (tx, rx) = plugin_executor_channel(EXECUTOR_QUEUE_CAPACITY);
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (crashed, exit) = exit_channel();

        thread::Builder::new()
            .name("lyra-plugin-executor".to_string())
            .spawn(move || {
                match build().and_then(|(runtime, errors)| {
                    let streams = super::stream::stream_stops(&runtime.vm)?;
                    let entrypoints = runtime.entrypoint_cancellation();
                    Ok((runtime, errors, streams, entrypoints))
                }) {
                    Ok((runtime, errors, streams, entrypoints)) => {
                        if ready_tx.send(Ok((errors, streams, entrypoints))).is_err() {
                            return;
                        }
                        run_plugin_executor_thread(runtime, rx, crashed);
                    }
                    Err(error) => {
                        let _ = ready_tx.send(Err(error));
                    }
                }
            })
            .context("spawn plugin executor thread")?;

        let (errors, streams, entrypoints) = ready_rx
            .recv()
            .context("plugin executor thread exited during startup")??;
        Ok((
            Self {
                tx,
                streams,
                entrypoints,
                exit,
            },
            errors,
        ))
    }

    pub(crate) fn id(&self) -> ExecutorId {
        self.exit.id
    }

    pub(crate) fn exit(&self) -> ExecutorExit {
        self.exit.clone()
    }

    pub(crate) async fn plugin_manifests(&self) -> Result<Vec<PluginManifest>> {
        self.request_control(PluginExecutorCommand::PluginManifests)
            .await
    }

    pub(crate) async fn has_plugin(&self, plugin_id: &str) -> Result<bool> {
        self.request_control(|reply| PluginExecutorCommand::HasPlugin {
            plugin_id: plugin_id.to_string(),
            reply,
        })
        .await
    }

    #[cfg(test)]
    pub(crate) async fn exec_plugin(&self, plugin_id: &str) -> Result<()> {
        self.request_control(|reply| PluginExecutorCommand::ExecPlugin {
            plugin_id: plugin_id.to_string(),
            reply,
        })
        .await
    }

    /// Panics the executor thread outside any plugin call. Fails once it has.
    #[cfg(test)]
    pub(crate) async fn inject_panic(&self) -> Result<()> {
        self.request_control(PluginExecutorCommand::Panic).await
    }

    pub(crate) async fn verify_plugin_manifest(&self, plugin_id: &str) -> Result<()> {
        self.request_control(|reply| PluginExecutorCommand::VerifyPluginManifest {
            plugin_id: plugin_id.to_string(),
            reply,
        })
        .await
    }

    pub(crate) async fn stop_plugin(&self, plugin_id: &str) -> Result<()> {
        self.streams.stop(Some(plugin_id));
        self.request_control(|reply| PluginExecutorCommand::StopPlugin {
            plugin_id: plugin_id.to_string(),
            reply,
        })
        .await
    }

    pub(crate) async fn restart_plugin(&self, plugin_id: &str) -> Result<()> {
        self.streams.stop(Some(plugin_id));
        self.request_control(|reply| PluginExecutorCommand::RestartPlugin {
            plugin_id: plugin_id.to_string(),
            reply,
        })
        .await
    }

    /// The ids of the plugins whose entrypoint failed.
    pub(crate) async fn exec_all(&self) -> Result<Vec<String>> {
        self.request_control(PluginExecutorCommand::ExecAll).await
    }

    /// Cancels every plugin's tasks and stops the executor thread. Replies once
    /// the runtime is dropped; later requests through any clone fail.
    pub(crate) async fn shutdown(&self) -> Result<()> {
        self.cancel_entrypoints();
        self.request_control(PluginExecutorCommand::Shutdown).await
    }

    /// Lets the calls holding or already waiting for a slot finish, up to `limit`, and fails every
    /// call that hasn't started by the end. Later dispatches fail with [`DispatchNotStarted`].
    /// Stops waiting once `cancelled` fires or the executor thread ends.
    pub(crate) async fn drain(
        &self,
        limit: Duration,
        cancelled: &CancellationToken,
    ) -> DrainOutcome {
        self.tx.draining.store(true, Ordering::Release);
        let all_slots = self.tx.slots.acquire_many(self.tx.capacity);
        tokio::pin!(all_slots);
        // Neither the acquire nor its slots may be released before the close: they would go to
        // the calls queued behind it, which would start only to be cancelled.
        let (outcome, _slots) = tokio::select! {
            biased;
            () = cancelled.cancelled() => (DrainOutcome::Cancelled, None),
            _ = self.exit().wait() => (DrainOutcome::Exited, None),
            slots = &mut all_slots => (DrainOutcome::Drained, slots.ok()),
            () = tokio::time::sleep(limit) => (DrainOutcome::TimedOut, None),
        };
        self.tx.slots.close();
        outcome
    }

    #[cfg(test)]
    pub(crate) fn slots_in_use(&self) -> usize {
        self.tx.capacity as usize - self.tx.slots.available_permits()
    }

    /// Fails the entrypoints running and yet to run, and stops every stream,
    /// so a runtime that is to shut down soon frees its thread for the ask.
    pub(crate) fn cancel_entrypoints(&self) {
        self.entrypoints
            .store(true, std::sync::atomic::Ordering::Release);
        self.streams.stop(None);
    }

    pub(crate) async fn dispatch_api_handler(
        &self,
        request: ApiHandlerRequest,
    ) -> Result<ApiHandlerResponse> {
        self.request_async(|reply| PluginExecutorCommand::ApiHandler { request, reply })
            .await
    }

    /// Resolves once the chunk finishes.
    pub(crate) async fn run_source(&self, request: PluginSourceRun) -> Result<()> {
        self.request_async(|reply| PluginExecutorCommand::RunPluginSource { request, reply })
            .await
    }

    pub(crate) async fn start_websocket(&self, request: WebSocketStartRequest) -> Result<()> {
        self.request_async(|reply| PluginExecutorCommand::StartWebSocket { request, reply })
            .await
    }

    async fn request_async<T>(
        &self,
        build: impl FnOnce(tokio::sync::oneshot::Sender<Result<T>>) -> PluginExecutorCommand,
    ) -> Result<T>
    where
        T: Send + 'static,
    {
        let (reply, rx) = tokio::sync::oneshot::channel();
        self.tx.send(build(reply)).await?;
        rx.await
            .context("plugin executor thread dropped response")?
    }

    async fn request_control<T>(
        &self,
        build: impl FnOnce(tokio::sync::oneshot::Sender<Result<T>>) -> PluginExecutorCommand,
    ) -> Result<T>
    where
        T: Send + 'static,
    {
        let (reply, rx) = tokio::sync::oneshot::channel();
        self.tx.send_control(build(reply)).await?;
        rx.await
            .context("plugin executor thread dropped response")?
    }
}

impl PluginDispatch for PluginExecutorHandle {
    fn dispatch_metadata_refresh(
        &self,
        request: MetadataRefreshRequest,
    ) -> BoxFuture<'_, Result<MetadataRefreshResult>> {
        Box::pin(
            self.request_async(|reply| PluginExecutorCommand::MetadataRefresh { request, reply }),
        )
    }

    fn dispatch_mix_handler(
        &self,
        request: MixHandlerRequest,
    ) -> BoxFuture<'_, Result<MixHandlerResult>> {
        Box::pin(self.request_async(|reply| PluginExecutorCommand::MixHandler { request, reply }))
    }

    fn dispatch_similar_releases(
        &self,
        request: SimilarReleasesDispatchRequest,
    ) -> BoxFuture<'_, Result<SimilarReleasesDispatchResult>> {
        Box::pin(
            self.request_async(|reply| PluginExecutorCommand::SimilarReleases { request, reply }),
        )
    }

    fn dispatch_playback_update(&self, payload: PlaybackUpdatePayload) -> Result<()> {
        self.tx
            .try_send(PluginExecutorCommand::PlaybackUpdate(payload))
    }

    fn dispatch_listen_update(
        &self,
        payload: crate::services::listens::ListenUpdatePayload,
    ) -> Result<()> {
        self.tx
            .try_send(PluginExecutorCommand::ListenUpdate(payload))
    }
}

fn plugin_executor_channel(
    capacity: usize,
) -> (
    PluginExecutorSender,
    mpsc::Receiver<QueuedPluginExecutorCommand>,
) {
    let (tx, rx) = mpsc::sync_channel(capacity + EXECUTOR_CONTROL_CAPACITY);
    (
        PluginExecutorSender {
            tx,
            slots: Arc::new(tokio::sync::Semaphore::new(capacity)),
            capacity: capacity as u32,
            draining: Arc::default(),
            control_slots: Arc::new(tokio::sync::Semaphore::new(EXECUTOR_CONTROL_CAPACITY)),
        },
        rx,
    )
}

/// Serves commands until a shutdown, or until they panic. A panicked runtime's
/// state is unknown, so it serves no more: it is released and reported as
/// crashed for a fresh one to replace. A panic while shutting down reports a
/// stop, as the runtime was ending anyway.
fn run_plugin_executor_thread(
    runtime: PluginExecutor,
    rx: mpsc::Receiver<QueuedPluginExecutorCommand>,
    crashed: tokio::sync::watch::Sender<bool>,
) {
    let served = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        serve_plugin_executor_commands(&runtime, &rx)
    }));
    match served {
        Ok(shutdown_reply) => {
            drop(crashed);
            let result = runtime.shutdown();
            drop(rx);
            if let Some(reply) = shutdown_reply {
                let _ = reply.send(result);
            }
        }
        Err(panic) => {
            tracing::error!(
                panic = panic_message(&*panic),
                "plugin executor thread panicked"
            );
            // Fails the requests still queued.
            drop(rx);
            runtime.release_after_panic();
            crashed.send_replace(true);
        }
    }
}

fn panic_message(panic: &(dyn std::any::Any + Send)) -> &str {
    panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload")
}

/// The reply to the shutdown that ended the loop, if one did.
fn serve_plugin_executor_commands(
    runtime: &PluginExecutor,
    rx: &mpsc::Receiver<QueuedPluginExecutorCommand>,
) -> Option<tokio::sync::oneshot::Sender<Result<()>>> {
    loop {
        runtime.poll_background_tasks();
        let wait = runtime
            .next_scheduler_delay()
            .unwrap_or_else(|| Duration::from_millis(100))
            .min(Duration::from_millis(25));
        match rx.recv_timeout(wait) {
            Ok(QueuedPluginExecutorCommand {
                command: PluginExecutorCommand::Shutdown(reply),
                ..
            }) => break Some(reply),
            Ok(QueuedPluginExecutorCommand { command, _permit }) => {
                handle_plugin_executor_command(runtime, command, _permit)
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break None,
        }
    }
}

fn handle_plugin_executor_command(
    runtime: &PluginExecutor,
    command: PluginExecutorCommand,
    permit: tokio::sync::OwnedSemaphorePermit,
) {
    match command {
        PluginExecutorCommand::PluginManifests(reply) => {
            reply_if_open(reply, || Ok(runtime.plugin_manifests()));
        }
        PluginExecutorCommand::HasPlugin { plugin_id, reply } => {
            reply_if_open(reply, || Ok(runtime.has_plugin(&plugin_id)));
        }
        #[cfg(test)]
        PluginExecutorCommand::ExecPlugin { plugin_id, reply } => {
            reply_if_open(reply, || runtime.exec_plugin(&plugin_id));
        }
        #[cfg(test)]
        PluginExecutorCommand::Panic(_reply) => {
            panic!("injected plugin executor panic");
        }
        PluginExecutorCommand::VerifyPluginManifest { plugin_id, reply } => {
            reply_if_open(reply, || runtime.verify_plugin_manifest(&plugin_id));
        }
        PluginExecutorCommand::StopPlugin { plugin_id, reply } => {
            reply_if_open(reply, || runtime.stop_plugin(&plugin_id));
        }
        PluginExecutorCommand::RestartPlugin { plugin_id, reply } => {
            reply_if_open(reply, || runtime.restart_plugin(&plugin_id));
        }
        PluginExecutorCommand::ExecAll(reply) => {
            reply_if_open(reply, || Ok(runtime.exec_all()));
        }
        PluginExecutorCommand::Shutdown(_) => {
            unreachable!("the executor loop handles shutdown")
        }
        PluginExecutorCommand::MixHandler { request, reply } => {
            runtime.start_mix_handler(request, reply, permit);
        }
        PluginExecutorCommand::MetadataRefresh { request, reply } => {
            runtime.start_metadata_refresh(request, reply, permit);
        }
        PluginExecutorCommand::SimilarReleases { request, reply } => {
            if request.cancellation.is_cancelled() {
                reply_cancelled_similar_releases(reply);
                return;
            }
            runtime.start_similar_releases(request, reply, permit);
        }
        PluginExecutorCommand::ApiHandler { request, reply } => {
            runtime.start_api_handler(request, reply, permit);
        }
        PluginExecutorCommand::StartWebSocket { request, reply } => {
            reply_if_open(reply, || runtime.start_websocket(request));
        }
        PluginExecutorCommand::PlaybackUpdate(payload) => {
            if let Err(error) = runtime.dispatch_playback_update(payload) {
                tracing::warn!(error = %error, "plugin playback on_update dispatch failed");
            }
        }
        PluginExecutorCommand::ListenUpdate(payload) => {
            if let Err(error) = runtime.dispatch_listen_update(payload) {
                tracing::warn!(error = %error, "plugin listen on_update dispatch failed");
            }
        }
        PluginExecutorCommand::RunPluginSource { request, reply } => {
            runtime.start_plugin_source(request, reply, permit);
        }
    }
}

fn reply_if_open<T>(
    reply: tokio::sync::oneshot::Sender<Result<T>>,
    operation: impl FnOnce() -> Result<T>,
) {
    if !reply.is_closed() {
        let _ = reply.send(operation());
    }
}

fn reply_cancelled_similar_releases(
    reply: tokio::sync::oneshot::Sender<Result<SimilarReleasesDispatchResult>>,
) {
    reply_if_open(reply, || {
        Err(anyhow::anyhow!("metadata handler dispatch was cancelled"))
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handle_with_capacity(
        capacity: usize,
    ) -> (
        PluginExecutorHandle,
        mpsc::Receiver<QueuedPluginExecutorCommand>,
    ) {
        let (tx, rx) = plugin_executor_channel(capacity);
        (
            PluginExecutorHandle {
                tx,
                streams: Default::default(),
                entrypoints: Default::default(),
                exit: exit_channel().1,
            },
            rx,
        )
    }

    fn playback_update() -> crate::services::playback_sessions::PlaybackUpdatePayload {
        crate::services::playback_sessions::PlaybackUpdatePayload {
            event: "progress".to_string(),
            state: lyra_db::PlaybackState::Playing,
            playback_session_public_id: "playback-public".to_string(),
            track_public_id: "track-public".to_string(),
            user_public_id: "user-public".to_string(),
            library_public_id: None,
            plugin_id: None,
            client_name: None,
            position_ms: 42,
            duration_ms: Some(100),
            rate: 1.0,
            counts_as_listen: true,
            played_ms: 42,
            updated_at_ms: 10,
        }
    }

    fn mix_request() -> MixHandlerRequest {
        MixHandlerRequest {
            handler: crate::services::plugin_dispatch::PluginCallbackHandle {
                vm_id: 1,
                handler_id: 1,
            },
            seeds: Vec::new(),
            query: None,
            limit: None,
            options: serde_json::Map::new(),
        }
    }

    #[tokio::test]
    async fn dispatch_waits_for_queue_capacity() -> Result<()> {
        let (handle, rx) = handle_with_capacity(1);
        handle.dispatch_playback_update(playback_update())?;

        let request = tokio::spawn({
            let handle = handle.clone();
            async move { handle.dispatch_mix_handler(mix_request()).await }
        });
        tokio::task::yield_now().await;
        assert!(!request.is_finished());

        drop(rx.recv()?.into_command());
        let queued =
            tokio::task::spawn_blocking(move || rx.recv_timeout(Duration::from_secs(1))).await??;
        let PluginExecutorCommand::MixHandler { reply, .. } = queued.into_command() else {
            panic!("expected queued mix handler request");
        };
        let _ = reply.send(Ok(MixHandlerResult { tracks: Vec::new() }));

        assert!(request.await??.tracks.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn control_commands_skip_slots_held_by_dispatches() -> Result<()> {
        let (handle, rx) = handle_with_capacity(1);
        handle.dispatch_playback_update(playback_update())?;
        // The dispatch keeps its slot for as long as its handler would run.
        let running_dispatch = rx.recv()?;

        let stop = tokio::spawn({
            let handle = handle.clone();
            async move { handle.stop_plugin("demo").await }
        });
        let queued =
            tokio::task::spawn_blocking(move || rx.recv_timeout(Duration::from_secs(1))).await??;
        let PluginExecutorCommand::StopPlugin { plugin_id, reply } = queued.into_command() else {
            panic!("expected queued stop request");
        };
        assert_eq!(plugin_id, "demo");
        let _ = reply.send(Ok(()));
        stop.await??;
        drop(running_dispatch);
        Ok(())
    }

    #[tokio::test]
    async fn an_ended_executor_ends_a_drain_with_slots_still_out() -> Result<()> {
        let (handle, _rx) = handle_with_capacity(1);
        handle.dispatch_playback_update(playback_update())?;

        let outcome = tokio::time::timeout(
            Duration::from_secs(2),
            handle.drain(Duration::from_secs(60), &CancellationToken::new()),
        )
        .await?;

        assert_eq!(outcome, DrainOutcome::Exited);
        Ok(())
    }

    #[test]
    fn playback_update_remains_lossy_when_queue_is_full() -> Result<()> {
        let (handle, rx) = handle_with_capacity(1);
        let (occupied_reply, _occupied_rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .try_send(PluginExecutorCommand::PluginManifests(occupied_reply))?;

        let payload = playback_update();
        let error = handle
            .dispatch_playback_update(payload.clone())
            .unwrap_err();
        assert!(error.to_string().contains("queue is full"));
        assert!(!DispatchNotStarted::caused(&error));

        drop(rx.recv()?.into_command());
        handle.dispatch_playback_update(payload)?;
        assert!(matches!(
            rx.recv()?.into_command(),
            PluginExecutorCommand::PlaybackUpdate(_)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_similar_releases_gets_explicit_error() -> Result<()> {
        let (reply, rx) = tokio::sync::oneshot::channel();
        reply_cancelled_similar_releases(reply);

        let error = rx.await?.unwrap_err();
        assert_eq!(error.to_string(), "metadata handler dispatch was cancelled");
        Ok(())
    }
}
