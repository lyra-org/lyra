// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::{
        HashMap,
        HashSet,
    },
    path::{
        Path,
        PathBuf,
    },
};

use agdb::{
    DbAny,
    DbId,
};

use crate::services::{
    EntityType,
    STATE,
    covers::{
        CoverPaths,
        configured_covers_root,
    },
    options::provider_options,
    plugin_dispatch::PluginCallbackHandle,
    providers::provider_registry,
};

use lyra_db::ProviderConfig;

mod artist_covers;
mod dispatch;
mod entity;
mod library;
#[cfg(test)]
mod tests;

pub(crate) use artist_covers::{
    ListedArtist,
    sync_artist_covers,
};
pub(crate) use entity::refresh_entity_metadata;
#[cfg(test)]
pub(in crate::services::providers) use entity::resolve_library_id_for_entity;
pub(crate) use library::{
    LibraryRefreshOptions,
    refresh_library_metadata,
    refresh_library_metadata_after_others,
    refresh_release_metadata_for_scan,
    release_refresh_provider_ids,
};

use super::{
    ActiveProviders,
    ProviderServiceError,
    dedup::deduplicate_releases_by_external_id,
};

async fn cover_path_buffers_for_library(
    library_db_id: Option<DbId>,
) -> Result<(Option<PathBuf>, PathBuf), ProviderServiceError> {
    let library_root = if let Some(library_db_id) = library_db_id {
        let db = STATE.db.read().await;
        lyra_db::libraries::get_by_id(&db, library_db_id)?.map(|library| library.path)
    } else {
        None
    };
    Ok((library_root, configured_covers_root()))
}

fn cover_paths<'a>(library_root: &'a Option<PathBuf>, covers_root: &'a Path) -> CoverPaths<'a> {
    CoverPaths {
        library_root: library_root.as_deref(),
        covers_root,
    }
}

/// A provider's refresh handler, with its sync filter when the refresh applies filters.
struct RefreshCallback {
    provider_id: String,
    handler: PluginCallbackHandle,
    filter: Option<PluginCallbackHandle>,
}

async fn read_active_providers() -> anyhow::Result<ActiveProviders> {
    let db = STATE.db.read().await;
    super::active_providers(&db)
}

async fn refresh_callbacks_for(
    providers: impl IntoIterator<Item = &ProviderConfig>,
    entity_type: EntityType,
    include_sync_filters: bool,
) -> Vec<RefreshCallback> {
    let registry = provider_registry().read_owned().await;
    providers
        .into_iter()
        .filter_map(|provider| {
            let handler = *registry.get_refresh_callback(&provider.provider_id, entity_type)?;
            let filter = if include_sync_filters {
                registry
                    .get_sync_filter_callback(&provider.provider_id, entity_type)
                    .copied()
            } else {
                None
            };
            Some(RefreshCallback {
                provider_id: provider.provider_id.clone(),
                handler,
                filter,
            })
        })
        .collect()
}

/// Each handler's provider's options, checked for every provider before any of them runs.
async fn passed_provider_options(
    callbacks: &[RefreshCallback],
    passed_options: &HashMap<String, String>,
) -> Result<Vec<serde_json::Map<String, serde_json::Value>>, ProviderServiceError> {
    let registry = provider_registry().read_owned().await;
    callbacks
        .iter()
        .map(|callback| {
            provider_options(
                &callback.provider_id,
                registry.get_options(&callback.provider_id),
                passed_options,
            )
            .map_err(ProviderServiceError::InvalidOption)
        })
        .collect()
}

fn context_with_options(
    mut context: serde_json::Value,
    options: serde_json::Map<String, serde_json::Value>,
) -> serde_json::Value {
    if options.is_empty() {
        return context;
    }
    if let serde_json::Value::Object(object) = &mut context {
        object.insert("options".to_string(), serde_json::Value::Object(options));
    }
    context
}

async fn deduplicate_release_scope(library_db_id: DbId, providers_called: &[String]) {
    let provider_scope: HashSet<String> = providers_called.iter().cloned().collect();
    let mut db_write = STATE.db.write().await;
    deduplicate_release_scope_locked(&mut db_write, library_db_id, &provider_scope);
}

fn deduplicate_release_scope_locked(
    db_write: &mut DbAny,
    library_db_id: DbId,
    provider_scope: &HashSet<String>,
) {
    let (declared_release_ids, unique_track_id_pairs) = {
        let registry = futures::executor::block_on(provider_registry().read_owned());
        (
            registry.declared_ids(EntityType::Release),
            registry.unique_track_id_pairs(),
        )
    };
    if !declared_release_ids.has_unique() {
        return;
    }
    if let Err(err) = deduplicate_releases_by_external_id(
        db_write,
        library_db_id,
        &declared_release_ids,
        &unique_track_id_pairs,
        Some(provider_scope),
    ) {
        tracing::warn!(
            library_db_id = library_db_id.0,
            error = %format_args!("{err:#}"),
            "release deduplication by external id failed during metadata refresh callback"
        );
    }
}
