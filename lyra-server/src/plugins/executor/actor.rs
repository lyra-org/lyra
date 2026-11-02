// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    path::PathBuf,
    sync::{
        Arc,
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
use crate::services::plugin_dispatch::PluginDispatch;

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
            .context("plugin executor queue is unavailable")?;
        self.enqueue(command, permit)
    }

    fn try_send(&self, command: PluginExecutorCommand) -> Result<()> {
        let permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .context("plugin executor queue is full or unavailable")?;
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
            .context("plugin executor queue is unavailable")
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

#[derive(Clone)]
pub(crate) struct PluginExecutorHandle {
    tx: PluginExecutorSender,
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

        thread::Builder::new()
            .name("lyra-plugin-executor".to_string())
            .spawn(move || match build() {
                Ok((runtime, errors)) => {
                    if ready_tx.send(Ok(errors)).is_err() {
                        return;
                    }
                    run_plugin_executor_thread(runtime, rx);
                }
                Err(error) => {
                    let _ = ready_tx.send(Err(error));
                }
            })
            .context("spawn plugin executor thread")?;

        let errors = ready_rx
            .recv()
            .context("plugin executor thread exited during startup")??;
        Ok((Self { tx }, errors))
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

    pub(crate) async fn verify_plugin_manifest(&self, plugin_id: &str) -> Result<()> {
        self.request_control(|reply| PluginExecutorCommand::VerifyPluginManifest {
            plugin_id: plugin_id.to_string(),
            reply,
        })
        .await
    }

    pub(crate) async fn stop_plugin(&self, plugin_id: &str) -> Result<()> {
        self.request_control(|reply| PluginExecutorCommand::StopPlugin {
            plugin_id: plugin_id.to_string(),
            reply,
        })
        .await
    }

    pub(crate) async fn restart_plugin(&self, plugin_id: &str) -> Result<()> {
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
        self.request_control(PluginExecutorCommand::Shutdown).await
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
            control_slots: Arc::new(tokio::sync::Semaphore::new(EXECUTOR_CONTROL_CAPACITY)),
        },
        rx,
    )
}

fn run_plugin_executor_thread(
    runtime: PluginExecutor,
    rx: mpsc::Receiver<QueuedPluginExecutorCommand>,
) {
    let shutdown_reply = loop {
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
                handle_plugin_executor_command(&runtime, command, _permit)
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break None,
        }
    };
    let result = runtime.shutdown();
    drop(rx);
    if let Some(reply) = shutdown_reply {
        let _ = reply.send(result);
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
        (PluginExecutorHandle { tx }, rx)
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
        let _ = reply.send(Ok(MixHandlerResult {
            track_ids: Vec::new(),
        }));

        assert!(request.await??.track_ids.is_empty());
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
