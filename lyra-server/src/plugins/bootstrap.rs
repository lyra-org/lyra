// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    path::PathBuf,
    sync::Arc,
};

use anyhow::Result;
use harmony_core::Harmony;

use crate::{
    STATE,
    plugins::{
        api as plugin_api,
        lifecycle::PluginId,
    },
    services,
};

pub(crate) fn initialize_harmony() -> Result<Arc<Harmony>> {
    let plugins_dir = std::env::var_os("LYRA_PLUGINS_DIR")
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| PathBuf::from("plugins"));

    let harmony = Arc::new(Harmony::new(
        STATE.lua.get(),
        "/",
        crate::plugins::docs::runtime_modules().into(),
        crate::plugins::globals::plugin_globals().into(),
        Some(crate::plugins::globals::caller_resolver()),
        Some(plugins_dir),
    )?);
    STATE
        .plugin_manifests
        .replace(Arc::from(harmony.plugin_manifests()));
    Ok(harmony)
}

pub(crate) fn publish_runtime(harmony: Arc<Harmony>) {
    STATE.plugin_runtime.replace(Some(harmony));
}

pub(crate) async fn exec_for_capture(harmony: Arc<Harmony>) -> Result<()> {
    harmony.exec_all().await?;
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
