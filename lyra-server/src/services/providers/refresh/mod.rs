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
    libraries::{
        SyncRunProgress,
        SyncStageKey,
    },
    metadata::id_links::DeclaredIds,
    options::provider_options,
    providers::ProviderRegistry,
};

use lyra_db::ProviderConfig;
use tokio::sync::RwLock;

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
    ScanRefreshPlan,
    deduplicate_after_providers,
    refresh_library_metadata,
    refresh_library_metadata_after_others,
    refresh_release_metadata_for_scan,
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

const PROVIDER_NOT_REGISTERED: &str = "provider_not_registered";

/// The providers a pass found without a handler, so each is logged once. They stay in the pass:
/// during a reload every provider briefly looks unregistered.
#[derive(Default)]
struct UnregisteredProviders(std::sync::Mutex<HashSet<String>>);

impl UnregisteredProviders {
    fn note(&self, provider_id: &str) {
        let first = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(provider_id.to_string());
        if first {
            tracing::warn!(
                provider_id,
                "provider has no handler registered; skipping its items until it has one"
            );
        }
    }

    async fn skip_not_registered(
        &self,
        provider_id: &str,
        progress: &SyncRunProgress,
        work_id: u64,
    ) {
        self.note(provider_id);
        progress
            .skip_work(
                work_id,
                SyncStageKey::ProviderRefresh,
                PROVIDER_NOT_REGISTERED,
            )
            .await;
    }
}

async fn read_active_providers() -> anyhow::Result<ActiveProviders> {
    let db = STATE.db.read().await;
    super::active_providers(&db)
}

/// The providers with a refresh handler for `entity_type`, by id: a pass looks their handlers up
/// at each call, so it follows them through a plugin reload.
fn refresh_providers_for<'a>(
    registry: &ProviderRegistry,
    providers: impl IntoIterator<Item = &'a ProviderConfig>,
    entity_type: EntityType,
) -> Vec<String> {
    providers
        .into_iter()
        .filter(|provider| registry.has_refresh_callback(&provider.provider_id, entity_type))
        .map(|provider| provider.provider_id.clone())
        .collect()
}

/// Each provider's options, checked for every provider before any of them runs.
fn passed_provider_options(
    registry: &ProviderRegistry,
    provider_ids: &[String],
    passed_options: &HashMap<String, String>,
) -> Result<Vec<serde_json::Map<String, serde_json::Value>>, ProviderServiceError> {
    provider_ids
        .iter()
        .map(|provider_id| {
            provider_options(
                provider_id,
                registry.get_options(provider_id),
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

/// What release deduplication reads of the providers' declarations, taken before the database
/// write lock.
struct ReleaseDedupDeclarations {
    release_ids: DeclaredIds,
    unique_track_id_pairs: HashSet<(String, String)>,
}

impl ReleaseDedupDeclarations {
    fn of(registry: &ProviderRegistry) -> Self {
        Self {
            release_ids: registry.declared_ids(EntityType::Release),
            unique_track_id_pairs: registry.unique_track_id_pairs(),
        }
    }
}

async fn deduplicate_release_scope(
    registry: &RwLock<ProviderRegistry>,
    library_db_id: DbId,
    providers_called: &[String],
) {
    let provider_scope: HashSet<String> = providers_called.iter().cloned().collect();
    let declarations = ReleaseDedupDeclarations::of(&*registry.read().await);
    let mut db_write = STATE.db.write().await;
    deduplicate_release_scope_locked(&mut db_write, library_db_id, &provider_scope, &declarations);
}

/// Returns whether a release may have been merged away, which a failure partway also leaves open.
fn deduplicate_release_scope_locked(
    db_write: &mut DbAny,
    library_db_id: DbId,
    provider_scope: &HashSet<String>,
    declarations: &ReleaseDedupDeclarations,
) -> bool {
    if !declarations.release_ids.has_unique() {
        return false;
    }
    match deduplicate_releases_by_external_id(
        db_write,
        library_db_id,
        &declarations.release_ids,
        &declarations.unique_track_id_pairs,
        Some(provider_scope),
        &STATE.generation().merge_journal,
    ) {
        Ok(merged) => merged > 0,
        Err(err) => {
            tracing::warn!(
                library_db_id = library_db_id.0,
                error = %format_args!("{err:#}"),
                "release deduplication by external id failed during metadata refresh callback"
            );
            true
        }
    }
}
