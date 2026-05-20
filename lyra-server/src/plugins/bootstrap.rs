// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    path::PathBuf,
    sync::Arc,
};

use anyhow::Context;
use anyhow::Result;
use harmony_core::PluginManifest;

use crate::plugins::api as plugin_api;
use crate::{
    STATE,
    plugins::lifecycle::PluginId,
    services,
};

#[derive(Clone)]
pub(crate) enum PluginRuntime {
    Executor(crate::plugins::executor::PluginExecutorHandle),
}

impl PluginRuntime {
    pub(crate) fn plugin_manifests(&self) -> Result<Vec<PluginManifest>> {
        match self {
            Self::Executor(runtime) => runtime.plugin_manifests(),
        }
    }

    pub(crate) fn has_plugin(&self, plugin_id: &str) -> Result<bool> {
        match self {
            Self::Executor(runtime) => runtime.has_plugin(plugin_id),
        }
    }

    pub(crate) async fn exec_all(&self) -> Result<()> {
        match self {
            Self::Executor(runtime) => runtime.exec_all(),
        }
    }

    pub(crate) async fn exec_plugin(&self, plugin_id: &str) -> Result<()> {
        match self {
            Self::Executor(runtime) => runtime.exec_plugin(plugin_id),
        }
    }

    pub(crate) fn dispatch_mix_handler(
        &self,
        request: crate::plugins::executor::MixHandlerRequest,
    ) -> Result<crate::plugins::executor::MixHandlerResult> {
        match self {
            Self::Executor(runtime) => runtime.dispatch_mix_handler(request),
        }
    }

    pub(crate) fn dispatch_metadata_refresh(
        &self,
        request: crate::plugins::executor::MetadataRefreshRequest,
    ) -> Result<crate::plugins::executor::MetadataRefreshResult> {
        match self {
            Self::Executor(runtime) => runtime.dispatch_metadata_refresh(request),
        }
    }
}

pub(crate) async fn initialize_harmony() -> Result<PluginRuntime> {
    let plugins_dir = std::env::var_os("LYRA_PLUGINS_DIR")
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| PathBuf::from("plugins"));

    {
        let server_info = crate::plugins::server::load_server_info()
            .await
            .context("build server info for plugin executor")?;
        let (runtime, errors) =
            crate::plugins::executor::PluginExecutorHandle::discover_from_plugins_dir_with_db(
                plugins_dir,
                server_info,
                STATE.db.get(),
            )?;
        for error in errors {
            tracing::warn!(error = %error, "plugin discovery error");
        }
        let runtime = PluginRuntime::Executor(runtime);
        STATE
            .plugin_manifests
            .replace(Arc::from(runtime.plugin_manifests()?));
        Ok(runtime)
    }
}

pub(crate) fn publish_runtime(runtime: PluginRuntime) {
    STATE.plugin_runtime.replace(Some(runtime));
}

pub(crate) async fn exec_for_capture(runtime: PluginRuntime) -> Result<()> {
    runtime.exec_all().await?;
    deduplicate_artists_after_plugin_init().await;
    Ok(())
}

pub(crate) async fn finalize_startup() -> Result<()> {
    deduplicate_artists_after_plugin_init().await;
    crate::plugins::runtime::freeze_registry().await;
    services::clear_cover_search_cache().await;

    plugin_api::finalize().await?;
    tracing::info!("plugin routes are now active");
    Ok(())
}

pub(crate) async fn teardown_loaded_plugins() {
    for manifest in STATE.plugin_manifests.get().iter() {
        match PluginId::new(manifest.id.clone()) {
            Ok(plugin_id) => {
                tracing::debug!(plugin_id = %plugin_id, "tearing down plugin registries");
                STATE
                    .plugin_registries
                    .teardown_plugin(&plugin_id, false)
                    .await;
            }
            Err(err) => {
                tracing::warn!(
                    plugin_id = %manifest.id,
                    error = %err,
                    "skipping plugin teardown for invalid manifest id"
                );
            }
        }
    }
}

async fn deduplicate_artists_after_plugin_init() {
    let mut db_write = STATE.db.write().await;
    if let Err(err) = services::deduplicate_artists_by_external_id(&mut db_write) {
        tracing::warn!(error = %err, "artist deduplication failed after plugin init");
    }
}
