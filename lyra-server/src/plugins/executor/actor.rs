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

use super::messages::PluginExecutorCommand;
use super::{
    ApiHandlerRequest,
    ApiHandlerResponse,
    MetadataRefreshRequest,
    MetadataRefreshResult,
    MixHandlerRequest,
    MixHandlerResult,
    PluginExecutor,
    SimilarReleasesDispatchRequest,
    SimilarReleasesDispatchResult,
    WebSocketStartRequest,
};

const EXECUTOR_QUEUE_CAPACITY: usize = 256;

#[cfg(test)]
#[path = "actor/tests.rs"]
mod dispatch_tests;

#[derive(Clone)]
struct PluginExecutorSender {
    tx: mpsc::SyncSender<QueuedPluginExecutorCommand>,
    slots: Arc<tokio::sync::Semaphore>,
}

impl PluginExecutorSender {
    async fn send(&self, command: PluginExecutorCommand) -> Result<()> {
        let permit = self
            .slots
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
    id: u64,
    tx: PluginExecutorSender,
}

impl PluginExecutorHandle {
    pub(crate) fn discover_from_plugins_dir_with_db_and_modules(
        plugins_dir: impl Into<PathBuf>,
        server_info: crate::plugins::server::ServerInfo,
        auth_capabilities: crate::plugins::auth::AuthCapabilities,
        db: crate::plugins::db::DbAsync,
        module_overrides: Vec<ModuleSpec>,
        settings: Option<crate::SettingsHandle>,
    ) -> Result<(Self, Vec<PluginLoadError>)> {
        let plugins_dir = plugins_dir.into();
        let (tx, rx) = plugin_executor_channel(EXECUTOR_QUEUE_CAPACITY);
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);

        thread::Builder::new()
            .name("lyra-plugin-executor".to_string())
            .spawn(move || {
                let mut stores = super::stores::PluginModuleStores::with_db(db);
                stores.server_settings = settings;
                match PluginExecutor::discover_from_plugins_dir_with_stores(
                    plugins_dir,
                    server_info,
                    auth_capabilities,
                    stores,
                    module_overrides,
                ) {
                    Ok((runtime, errors)) => {
                        let vm_id = runtime.vm.id();
                        if ready_tx.send(Ok((errors, vm_id))).is_err() {
                            return;
                        }
                        run_plugin_executor_thread(runtime, rx);
                    }
                    Err(error) => {
                        let _ = ready_tx.send(Err(error));
                    }
                }
            })
            .context("spawn plugin executor thread")?;

        let (errors, id) = ready_rx
            .recv()
            .context("plugin executor thread exited during startup")??;
        Ok((Self { id, tx }, errors))
    }

    pub(crate) fn same_instance(&self, other: &Self) -> bool {
        self.id == other.id
    }

    pub(crate) fn vm_id(&self) -> u64 {
        self.id
    }

    pub(crate) async fn plugin_manifests(&self) -> Result<Vec<PluginManifest>> {
        self.request_async(PluginExecutorCommand::PluginManifests)
            .await
    }

    pub(crate) async fn has_plugin(&self, plugin_id: &str) -> Result<bool> {
        self.request_async(|reply| PluginExecutorCommand::HasPlugin {
            plugin_id: plugin_id.to_string(),
            reply,
        })
        .await
    }

    pub(crate) async fn exec_plugin(&self, plugin_id: &str) -> Result<()> {
        self.request_async(|reply| PluginExecutorCommand::ExecPlugin {
            plugin_id: plugin_id.to_string(),
            reply,
        })
        .await
    }

    pub(crate) async fn exec_all(&self) -> Result<()> {
        self.request_async(PluginExecutorCommand::ExecAll).await
    }

    pub(crate) fn dispatch_playback_update(
        &self,
        payload: crate::services::playback_sessions::PlaybackUpdatePayload,
    ) -> Result<()> {
        self.tx
            .try_send(PluginExecutorCommand::PlaybackUpdate(payload))
    }

    pub(crate) async fn dispatch_mix_handler(
        &self,
        request: MixHandlerRequest,
    ) -> Result<MixHandlerResult> {
        self.request_async(|reply| PluginExecutorCommand::MixHandler { request, reply })
            .await
    }

    pub(crate) async fn dispatch_metadata_refresh(
        &self,
        request: MetadataRefreshRequest,
    ) -> Result<MetadataRefreshResult> {
        self.request_async(|reply| PluginExecutorCommand::MetadataRefresh { request, reply })
            .await
    }

    pub(crate) async fn dispatch_similar_releases(
        &self,
        request: SimilarReleasesDispatchRequest,
    ) -> Result<SimilarReleasesDispatchResult> {
        self.request_async(|reply| PluginExecutorCommand::SimilarReleases { request, reply })
            .await
    }

    pub(crate) async fn dispatch_api_handler(
        &self,
        request: ApiHandlerRequest,
    ) -> Result<ApiHandlerResponse> {
        self.request_async(|reply| PluginExecutorCommand::ApiHandler { request, reply })
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
}

fn plugin_executor_channel(
    capacity: usize,
) -> (
    PluginExecutorSender,
    mpsc::Receiver<QueuedPluginExecutorCommand>,
) {
    let (tx, rx) = mpsc::sync_channel(capacity);
    (
        PluginExecutorSender {
            tx,
            slots: Arc::new(tokio::sync::Semaphore::new(capacity)),
        },
        rx,
    )
}

fn run_plugin_executor_thread(
    runtime: PluginExecutor,
    rx: mpsc::Receiver<QueuedPluginExecutorCommand>,
) {
    loop {
        runtime.poll_background_tasks();
        let wait = runtime
            .next_scheduler_delay()
            .unwrap_or_else(|| Duration::from_millis(100))
            .min(Duration::from_millis(25));
        match rx.recv_timeout(wait) {
            Ok(QueuedPluginExecutorCommand { command, _permit }) => {
                handle_plugin_executor_command(&runtime, command, _permit)
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
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
        PluginExecutorCommand::ExecPlugin { plugin_id, reply } => {
            reply_if_open(reply, || runtime.exec_plugin(&plugin_id));
        }
        PluginExecutorCommand::ExecAll(reply) => {
            reply_if_open(reply, || runtime.exec_all());
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
        (PluginExecutorHandle { id: 1, tx }, rx)
    }

    fn playback_update() -> crate::services::playback_sessions::PlaybackUpdatePayload {
        crate::services::playback_sessions::PlaybackUpdatePayload {
            event: "progress".to_string(),
            state: crate::plugins::db::PlaybackState::Playing,
            playback_session_public_id: "playback-public".to_string(),
            track_public_id: "track-public".to_string(),
            user_public_id: "user-public".to_string(),
            library_public_id: None,
            position_ms: 42,
            duration_ms: Some(100),
            activity_ms: 42,
            qualifies_single_listen: false,
            updated_at_ms: 10,
        }
    }

    #[tokio::test]
    async fn request_waits_for_queue_capacity() -> Result<()> {
        let (handle, rx) = handle_with_capacity(1);
        let (occupied_reply, _occupied_rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .try_send(PluginExecutorCommand::PluginManifests(occupied_reply))?;

        let request = tokio::spawn({
            let handle = handle.clone();
            async move { handle.plugin_manifests().await }
        });
        tokio::task::yield_now().await;
        assert!(!request.is_finished());

        drop(rx.recv()?.into_command());
        let queued =
            tokio::task::spawn_blocking(move || rx.recv_timeout(Duration::from_secs(1))).await??;
        let PluginExecutorCommand::PluginManifests(reply) = queued.into_command() else {
            panic!("expected queued plugin manifests request");
        };
        let _ = reply.send(Ok(Vec::new()));

        assert!(request.await??.is_empty());
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
