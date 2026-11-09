// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::Arc;

use crate::plugins::api as plugin_api;
use crate::{
    services,
    services::STATE,
    services::plugin_scope::PluginId,
};
use anyhow::{
    Context,
    Result,
};

pub(crate) type PluginRuntime = crate::plugins::executor::PluginExecutorHandle;

pub(crate) async fn initialize_harmony() -> Result<PluginRuntime> {
    let plugins_dir = STATE.boot.get().plugins_dir.clone();

    {
        let server_info = crate::plugins::server::load_server_info()
            .await
            .context("build server info for plugin executor")?;
        let auth_capabilities =
            crate::plugins::auth::AuthCapabilities::from_config(&STATE.config().auth);
        let (runtime, errors) = tokio::task::block_in_place(|| {
            warn_orphaned_installs(&plugins_dir);
            crate::plugins::executor::PluginExecutorHandle::discover_from_plugins_dir_with_db_and_modules(
                plugins_dir,
                server_info,
                auth_capabilities,
                STATE.db.get(),
                Vec::new(),
                Some(STATE.settings.clone()),
            )
        })?;
        for error in errors {
            tracing::warn!(error = %error, "plugin discovery error");
        }
        crate::plugins::STATE
            .generation()
            .manifests
            .replace(Arc::from(runtime.plugin_manifests().await?));
        Ok(runtime)
    }
}

/// Warns of each plugin an interrupted install left only as its previous
/// installation, which installs refuse to replace.
fn warn_orphaned_installs(plugins_dir: &std::path::Path) {
    match harmony_repository::orphaned_installs(plugins_dir) {
        Ok(orphaned) => {
            for (id, previous) in orphaned {
                tracing::warn!(plugin = %id, previous = %previous.display(), "plugin is not installed, but its previous installation remains");
            }
        }
        Err(error) => tracing::warn!(%error, "could not check for interrupted plugin installs"),
    }
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
    for manifest in crate::plugins::STATE.generation().manifests.get().iter() {
        match PluginId::new(manifest.id.clone()) {
            Ok(plugin_id) => {
                tracing::debug!(plugin_id = %plugin_id, "tearing down plugin registries");
                crate::plugins::STATE
                    .generation()
                    .registries
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
    let declared_artist_ids = services::providers::declared_artist_ids().await;
    let mut db_write = STATE.db.write().await;
    if let Err(err) = services::deduplicate_artists_by_external_id(
        &mut db_write,
        &declared_artist_ids,
        &STATE.generation().merge_journal,
    ) {
        tracing::warn!(error = %err, "artist deduplication failed after plugin init");
    }
}
