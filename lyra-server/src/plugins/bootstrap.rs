// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    path::PathBuf,
    sync::Arc,
};

use crate::plugins::api as plugin_api;
use crate::{
    STATE,
    plugins::lifecycle::PluginId,
    services,
};
use anyhow::{
    Context,
    Result,
};

pub(crate) type PluginRuntime = crate::plugins::executor::PluginExecutorHandle;

pub(crate) fn plugins_dir() -> PathBuf {
    std::env::var_os("LYRA_PLUGINS_DIR")
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| PathBuf::from("plugins"))
}

pub(crate) async fn initialize_harmony() -> Result<PluginRuntime> {
    let plugins_dir = plugins_dir();

    {
        let server_info = crate::plugins::server::load_server_info()
            .await
            .context("build server info for plugin executor")?;
        let (runtime, errors) = crate::plugins::executor::PluginExecutorHandle::discover_from_plugins_dir_with_db_and_modules(
            plugins_dir,
            server_info,
            STATE.db.get(),
            Vec::new(),
        )?;
        for error in errors {
            tracing::warn!(error = %error, "plugin discovery error");
        }
        STATE
            .plugin_manifests
            .replace(Arc::from(runtime.plugin_manifests().await?));
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
    crate::plugins::settings::freeze_registry().await;
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
