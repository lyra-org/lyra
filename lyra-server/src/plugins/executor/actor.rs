// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    path::PathBuf,
    sync::mpsc,
    thread,
    time::Duration,
};

use anyhow::{
    Context,
    Result,
};
use harmony_core::{
    ModuleSpec,
    PluginLoadError,
    PluginManifest,
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
    WebSocketStartRequest,
};

#[derive(Clone)]
pub(crate) struct PluginExecutorHandle {
    tx: mpsc::Sender<PluginExecutorCommand>,
}

impl PluginExecutorHandle {
    pub(crate) fn discover_from_plugins_dir_with_db_and_modules(
        plugins_dir: impl Into<PathBuf>,
        server_info: crate::plugins::server::ServerInfo,
        db: crate::plugins::db::DbAsync,
        module_overrides: Vec<ModuleSpec>,
    ) -> Result<(Self, Vec<PluginLoadError>)> {
        let plugins_dir = plugins_dir.into();
        let (tx, rx) = mpsc::channel();
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);

        thread::Builder::new()
            .name("lyra-plugin-executor".to_string())
            .spawn(move || {
                match PluginExecutor::discover_from_plugins_dir_with_db_and_modules(
                    plugins_dir,
                    server_info,
                    db,
                    module_overrides,
                ) {
                    Ok((runtime, errors)) => {
                        if ready_tx.send(Ok(errors)).is_err() {
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

        let errors = ready_rx
            .recv()
            .context("plugin executor thread exited during startup")??;
        Ok((Self { tx }, errors))
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
            .send(PluginExecutorCommand::PlaybackUpdate(payload))
            .context("plugin executor thread is unavailable")
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
        self.tx
            .send(build(reply))
            .context("plugin executor thread is unavailable")?;
        rx.await
            .context("plugin executor thread dropped response")?
    }
}

fn run_plugin_executor_thread(runtime: PluginExecutor, rx: mpsc::Receiver<PluginExecutorCommand>) {
    loop {
        runtime.poll_background_tasks();
        let wait = runtime
            .next_scheduler_delay()
            .unwrap_or_else(|| Duration::from_millis(100))
            .min(Duration::from_millis(25));
        match rx.recv_timeout(wait) {
            Ok(command) => handle_plugin_executor_command(&runtime, command),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
}

fn handle_plugin_executor_command(runtime: &PluginExecutor, command: PluginExecutorCommand) {
    match command {
        PluginExecutorCommand::PluginManifests(reply) => {
            let _ = reply.send(Ok(runtime.plugin_manifests()));
        }
        PluginExecutorCommand::HasPlugin { plugin_id, reply } => {
            let _ = reply.send(Ok(runtime.has_plugin(&plugin_id)));
        }
        PluginExecutorCommand::ExecPlugin { plugin_id, reply } => {
            let _ = reply.send(runtime.exec_plugin(&plugin_id));
        }
        PluginExecutorCommand::ExecAll(reply) => {
            let _ = reply.send(runtime.exec_all());
        }
        PluginExecutorCommand::MixHandler { request, reply } => {
            let _ = reply.send(runtime.dispatch_mix_handler(request));
        }
        PluginExecutorCommand::MetadataRefresh { request, reply } => {
            let _ = reply.send(runtime.dispatch_metadata_refresh(request));
        }
        PluginExecutorCommand::ApiHandler { request, reply } => {
            let _ = reply.send(runtime.dispatch_api_handler(request));
        }
        PluginExecutorCommand::StartWebSocket { request, reply } => {
            let _ = reply.send(runtime.start_websocket(request));
        }
        PluginExecutorCommand::PlaybackUpdate(payload) => {
            if let Err(error) = runtime.dispatch_playback_update(payload) {
                tracing::warn!(error = %error, "plugin playback on_update dispatch failed");
            }
        }
    }
}
