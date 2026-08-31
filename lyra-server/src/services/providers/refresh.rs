// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::{
        HashMap,
        HashSet,
    },
    path::PathBuf,
    time::Duration,
};

use agdb::{
    DbAny,
    DbId,
};
use anyhow::Context;

use crate::{
    STATE,
    db::{
        self,
        ProviderConfig,
    },
    plugins::executor::MetadataRefreshRequest,
    services::{
        EntityType,
        covers::{
            CoverPaths,
            CoverScope,
            CoverSyncOptions,
            configured_covers_root,
            resolve_cover_for_artist_id,
            resolve_cover_for_release_id,
            sync_and_persist_covers_for_library,
            sync_artist_cover,
            sync_release_cover_for_tracks,
            upsert_artist_cover_metadata,
            upsert_release_cover_metadata,
        },
        deduplicate_artists_by_external_id,
        entities::{
            EntityContextError,
            build_entity_provider_context,
            build_release_context,
        },
        libraries::{
            SyncRunProgress,
            SyncStageKey,
            SyncTotalState,
            SyncWorkDetails,
        },
        options::coerce_option_value,
        providers::{
            ProviderCallStage,
            ProviderCallbackHandle,
            library_refresh_locks,
            provider_registry,
            with_provider_call,
        },
    },
};

use super::{
    EntityRefreshMode,
    EntityRefreshResult,
    ProviderServiceError,
    dedup::deduplicate_releases_by_external_id,
};

const DEFAULT_METADATA_REFRESH_TIMEOUT: Duration = Duration::from_secs(60);

struct LibraryRefreshGuard {
    library_db_id: DbId,
}

impl Drop for LibraryRefreshGuard {
    fn drop(&mut self) {
        let library_db_id = self.library_db_id;
        tokio::task::spawn(async move {
            library_refresh_locks()
                .lock_owned()
                .await
                .remove(&library_db_id);
        });
    }
}

pub(crate) struct LibraryRefreshOptions<'a> {
    pub(crate) replace_cover: bool,
    pub(crate) force_refresh: bool,
    pub(crate) apply_sync_filters: bool,
    pub(crate) provider_id: Option<&'a str>,
}

pub(super) fn resolve_library_id_for_entity(
    db: &DbAny,
    node_id: DbId,
) -> anyhow::Result<Option<DbId>> {
    let libraries = db::libraries::get_for_entity(db, node_id)?;
    Ok(libraries
        .into_iter()
        .filter_map(|library| library.db_id)
        .min_by_key(|id| id.0))
}

pub(crate) async fn refresh_entity_metadata(
    node_id: DbId,
    refresh_mode: EntityRefreshMode,
) -> Result<EntityRefreshResult, ProviderServiceError> {
    let passed_options = match &refresh_mode {
        EntityRefreshMode::MetadataOnly => HashMap::new(),
        EntityRefreshMode::WithReleaseArtifacts { options, .. } => options.clone(),
    };

    let (entity_type, context, library_db_id) = {
        let db = STATE.db.read().await;
        let library_db_id = resolve_library_id_for_entity(&db, node_id)?;
        let (entity_type, context) = build_entity_provider_context(&db, node_id, library_db_id)
            .map_err(|err| match err {
                EntityContextError::EntityNotFound(id) => ProviderServiceError::EntityNotFound(id),
                EntityContextError::Internal(err) => ProviderServiceError::Internal(err),
            })?;
        (entity_type, context, library_db_id)
    };

    let providers = enabled_providers().await?;
    let handlers = refresh_callbacks_for(&providers, entity_type, None, false).await;
    let mut providers_called = Vec::new();
    for (provider_id, handler, _) in handlers {
        let context = context_with_options(context.clone(), &provider_id, &passed_options).await;
        dispatch_refresh_callback(&provider_id, handler.handler_id, context).await?;
        providers_called.push(provider_id);
    }

    if entity_type == EntityType::Release
        && let Some(library_db_id) = library_db_id
        && !providers_called.is_empty()
    {
        deduplicate_release_scope(library_db_id, &providers_called).await;
    }

    if !providers_called.is_empty()
        && let EntityRefreshMode::WithReleaseArtifacts {
            replace_cover,
            force_refresh,
            ..
        } = refresh_mode
    {
        sync_cover_for_refreshed_entity(
            node_id,
            entity_type,
            library_db_id,
            CoverSyncOptions {
                replace_existing: replace_cover,
                force_refresh,
            },
        )
        .await?;
    }

    Ok(EntityRefreshResult {
        entity_type,
        providers_called,
    })
}

pub(crate) async fn refresh_library_metadata(
    library_db_id: DbId,
    options: &LibraryRefreshOptions<'_>,
) -> Result<usize, ProviderServiceError> {
    {
        let mut locks = library_refresh_locks().lock_owned().await;
        if !locks.insert(library_db_id) {
            return Err(ProviderServiceError::RefreshAlreadyRunning(library_db_id.0));
        }
    }

    let _guard = LibraryRefreshGuard { library_db_id };
    refresh_library_metadata_inner(library_db_id, options).await
}

pub(crate) async fn refresh_library_metadata_with_progress(
    library_db_id: DbId,
    options: &LibraryRefreshOptions<'_>,
    progress: Option<SyncRunProgress>,
) -> Result<usize, ProviderServiceError> {
    {
        let mut locks = library_refresh_locks().lock_owned().await;
        if !locks.insert(library_db_id) {
            return Err(ProviderServiceError::RefreshAlreadyRunning(library_db_id.0));
        }
    }

    let _guard = LibraryRefreshGuard { library_db_id };
    refresh_library_metadata_inner_with_progress(library_db_id, options, progress).await
}

pub(crate) async fn release_refresh_provider_ids(
    provider_filter: Option<&str>,
    include_sync_filters: bool,
) -> Result<Vec<String>, ProviderServiceError> {
    let providers = enabled_providers().await?;
    Ok(refresh_callbacks_for(
        &providers,
        EntityType::Release,
        provider_filter,
        include_sync_filters,
    )
    .await
    .into_iter()
    .map(|(provider_id, _, _)| provider_id)
    .collect())
}

pub(crate) async fn refresh_release_metadata_for_scan_with_progress(
    library_db_id: DbId,
    release_id: DbId,
    options: &LibraryRefreshOptions<'_>,
    progress: Option<SyncRunProgress>,
    release_public_id: Option<String>,
    release_title: Option<String>,
) -> Result<usize, ProviderServiceError> {
    refresh_release_metadata_inner_with_progress(
        library_db_id,
        release_id,
        options,
        progress,
        release_public_id,
        release_title,
    )
    .await
}

async fn refresh_release_metadata_inner_with_progress(
    library_db_id: DbId,
    release_id: DbId,
    options: &LibraryRefreshOptions<'_>,
    progress: Option<SyncRunProgress>,
    release_public_id: Option<String>,
    release_title: Option<String>,
) -> Result<usize, ProviderServiceError> {
    {
        let db = STATE.db.read().await;
        db::libraries::get_by_id(&db, library_db_id)?
            .ok_or(ProviderServiceError::LibraryNotFound(library_db_id.0))?;
        if db::releases::get_by_id(&db, release_id)?.is_none() {
            return Err(ProviderServiceError::EntityNotFound(release_id.0));
        }
    }

    let providers = enabled_providers().await?;
    let handlers = refresh_callbacks_for(
        &providers,
        EntityType::Release,
        options.provider_id,
        options.apply_sync_filters,
    )
    .await;
    let mut providers_called = Vec::new();
    let mut context: Option<serde_json::Value> = None;
    let mut dirty = true;

    for (provider_id, handler, filter) in &handlers {
        if let Some(progress) = &progress
            && let Err(err) = progress.check_cancelled().await
        {
            return Err(ProviderServiceError::Internal(err.into()));
        }
        if dirty || context.is_none() {
            let rebuilt = {
                let db = STATE.db.read().await;
                match build_release_context(&db, release_id, Some(library_db_id)) {
                    Ok(context) => Some(context),
                    Err(err) => {
                        if providers_called.is_empty() {
                            return Err(ProviderServiceError::Internal(err));
                        }
                        tracing::debug!(
                            library_db_id = library_db_id.0,
                            release_db_id = release_id.0,
                            error = %err,
                            "release disappeared during scan provider refresh"
                        );
                        None
                    }
                }
            };
            let Some(rebuilt) = rebuilt else {
                break;
            };
            context = Some(rebuilt);
            dirty = false;
        }

        let Some(context) = context.as_ref() else {
            continue;
        };
        let details = SyncWorkDetails::release(
            "release_provider_refresh",
            release_public_id.clone(),
            release_title.clone(),
        )
        .provider(provider_id.clone());
        let work_id = if let Some(progress) = &progress {
            progress
                .start_work(SyncStageKey::ProviderRefresh, details.clone())
                .await
        } else {
            0
        };
        let context = context_with_options(context.clone(), provider_id, &HashMap::new()).await;
        let should_run = if let Some(filter) = filter {
            match dispatch_sync_filter_callback(provider_id, filter.handler_id, context.clone())
                .await
            {
                Ok(value) => value,
                Err(err) => {
                    tracing::warn!(
                        provider_id,
                        release_db_id = release_id.0,
                        error = %err,
                        "sync filter failed, skipping release for provider"
                    );
                    false
                }
            }
        } else {
            true
        };
        if !should_run {
            if let Some(progress) = &progress {
                progress
                    .skip_work(work_id, SyncStageKey::ProviderRefresh, "sync_filter")
                    .await;
            }
            continue;
        }

        match dispatch_refresh_callback(provider_id, handler.handler_id, context).await {
            Ok(()) => {
                providers_called.push(provider_id.clone());
                dirty = true;
                if let Some(progress) = &progress {
                    progress
                        .complete_work(work_id, SyncStageKey::ProviderRefresh)
                        .await;
                }
            }
            Err(err) => {
                if let Some(progress) = &progress {
                    progress
                        .fail_work(
                            work_id,
                            SyncStageKey::ProviderRefresh,
                            details,
                            err.to_string(),
                        )
                        .await;
                }
                tracing::warn!(
                    provider_id,
                    release_db_id = release_id.0,
                    error = %err,
                    "provider refresh handler failed during scan release refresh"
                );
                continue;
            }
        }

        let mut db_write = STATE.db.write().await;
        if let Err(err) = deduplicate_artists_by_external_id(&mut db_write) {
            tracing::warn!(
                provider_id,
                release_db_id = release_id.0,
                error = %err,
                "artist deduplication failed during scan release refresh"
            );
        }
        deduplicate_release_scope_locked(
            &mut db_write,
            library_db_id,
            &HashSet::from([provider_id.to_string()]),
        );
    }

    Ok(usize::from(!providers_called.is_empty()))
}

async fn refresh_library_metadata_inner(
    library_db_id: DbId,
    options: &LibraryRefreshOptions<'_>,
) -> Result<usize, ProviderServiceError> {
    refresh_library_metadata_inner_with_progress(library_db_id, options, None).await
}

async fn refresh_library_metadata_inner_with_progress(
    library_db_id: DbId,
    options: &LibraryRefreshOptions<'_>,
    progress: Option<SyncRunProgress>,
) -> Result<usize, ProviderServiceError> {
    let releases = {
        let db = STATE.db.read().await;
        db::libraries::get_by_id(&db, library_db_id)?
            .ok_or(ProviderServiceError::LibraryNotFound(library_db_id.0))?;
        db::releases::get(&db, library_db_id)?
    };

    let providers = enabled_providers().await?;
    let handlers = refresh_callbacks_for(
        &providers,
        EntityType::Release,
        options.provider_id,
        options.apply_sync_filters,
    )
    .await;
    let release_count = releases
        .iter()
        .filter(|release| release.db_id.is_some())
        .count() as u64;
    if let Some(progress) = &progress {
        progress
            .add_stage_total(
                SyncStageKey::ProviderRefresh,
                release_count * handlers.len() as u64,
                SyncTotalState::Final,
            )
            .await;
        if release_count > 0 {
            progress
                .add_stage_total(SyncStageKey::ProviderCover, 1, SyncTotalState::Final)
                .await;
        }
        progress.set_determinate().await;
    }
    let mut refreshed_releases = HashSet::new();
    let mut context_cache: HashMap<DbId, serde_json::Value> = HashMap::new();
    let mut dirty_releases = HashSet::new();
    let mut release_infos: HashMap<DbId, (Option<String>, Option<String>)> = HashMap::new();

    for release in &releases {
        let Some(node_id) = release.db_id.clone().map(Into::<DbId>::into) else {
            continue;
        };
        let ctx = {
            let db = STATE.db.read().await;
            let public_id = db::lookup::find_id_by_db_id(&db, node_id)?;
            release_infos.insert(node_id, (public_id, Some(release.release_title.clone())));
            build_release_context(&db, node_id, Some(library_db_id))?
        };
        context_cache.insert(node_id, ctx);
    }

    for (provider_id, handler, filter) in &handlers {
        let mut pass_touched = HashSet::new();
        for release in &releases {
            let Some(node_id) = release.db_id.clone().map(Into::<DbId>::into) else {
                continue;
            };
            if let Some(progress) = &progress
                && let Err(err) = progress.check_cancelled().await
            {
                return Err(ProviderServiceError::Internal(err.into()));
            }

            if dirty_releases.remove(&node_id) {
                let ctx = {
                    let db = STATE.db.read().await;
                    build_release_context(&db, node_id, Some(library_db_id))?
                };
                context_cache.insert(node_id, ctx);
            }
            let Some(context) = context_cache.get(&node_id) else {
                continue;
            };
            let (release_public_id, release_title) =
                release_infos.get(&node_id).cloned().unwrap_or_default();
            let details = SyncWorkDetails::release(
                "release_provider_refresh",
                release_public_id,
                release_title,
            )
            .provider(provider_id.clone());
            let work_id = if let Some(progress) = &progress {
                progress
                    .start_work(SyncStageKey::ProviderRefresh, details.clone())
                    .await
            } else {
                0
            };
            let context = context_with_options(context.clone(), provider_id, &HashMap::new()).await;

            let should_run = if let Some(filter) = filter {
                match dispatch_sync_filter_callback(provider_id, filter.handler_id, context.clone())
                    .await
                {
                    Ok(value) => value,
                    Err(err) => {
                        tracing::warn!(
                            provider_id,
                            release_db_id = node_id.0,
                            error = %err,
                            "sync filter failed, skipping release for provider"
                        );
                        false
                    }
                }
            } else {
                true
            };
            if !should_run {
                if let Some(progress) = &progress {
                    progress
                        .skip_work(work_id, SyncStageKey::ProviderRefresh, "sync_filter")
                        .await;
                }
                continue;
            }

            match dispatch_refresh_callback(provider_id, handler.handler_id, context).await {
                Ok(()) => {
                    refreshed_releases.insert(node_id);
                    dirty_releases.insert(node_id);
                    pass_touched.insert(node_id);
                    if let Some(progress) = &progress {
                        progress
                            .complete_work(work_id, SyncStageKey::ProviderRefresh)
                            .await;
                    }
                }
                Err(err) => {
                    if let Some(progress) = &progress {
                        progress
                            .fail_work(
                                work_id,
                                SyncStageKey::ProviderRefresh,
                                details,
                                err.to_string(),
                            )
                            .await;
                    }
                    tracing::warn!(
                        provider_id,
                        release_db_id = node_id.0,
                        error = ?err,
                        "provider refresh handler failed during library refresh"
                    );
                }
            }
        }

        if pass_touched.is_empty() {
            continue;
        }

        let mut db_write = STATE.db.write().await;
        if let Err(err) = deduplicate_artists_by_external_id(&mut db_write) {
            tracing::warn!(
                provider_id,
                error = %err,
                "artist deduplication failed during library refresh"
            );
        }
        deduplicate_release_scope_locked(
            &mut db_write,
            library_db_id,
            &HashSet::from([provider_id.to_string()]),
        );
    }

    if release_count > 0 {
        let work_id = if let Some(progress) = &progress {
            progress
                .start_work(
                    SyncStageKey::ProviderCover,
                    SyncWorkDetails::new("library_provider_cover_refresh"),
                )
                .await
        } else {
            0
        };
        if refreshed_releases.is_empty() {
            if let Some(progress) = &progress {
                progress
                    .skip_work(
                        work_id,
                        SyncStageKey::ProviderCover,
                        "no_refreshed_releases",
                    )
                    .await;
            }
        } else {
            match sync_library_covers_after_refresh(library_db_id, options).await {
                Ok(_count) => {
                    if let Some(progress) = &progress {
                        progress
                            .complete_work(work_id, SyncStageKey::ProviderCover)
                            .await;
                    }
                }
                Err(err) => {
                    if let Some(progress) = &progress {
                        progress
                            .fail_work(
                                work_id,
                                SyncStageKey::ProviderCover,
                                SyncWorkDetails::new("library_provider_cover_refresh"),
                                err.to_string(),
                            )
                            .await;
                    }
                    return Err(err);
                }
            }
        }
    }

    Ok(refreshed_releases.len())
}

async fn cover_path_buffers_for_library(
    library_db_id: Option<DbId>,
) -> Result<(Option<PathBuf>, Option<PathBuf>), ProviderServiceError> {
    let library_root = if let Some(library_db_id) = library_db_id {
        let db = STATE.db.read().await;
        db::libraries::get_by_id(&db, library_db_id)?.map(|library| library.path)
    } else {
        None
    };
    Ok((library_root, configured_covers_root()))
}

fn cover_paths<'a>(
    library_root: &'a Option<PathBuf>,
    covers_root: &'a Option<PathBuf>,
) -> CoverPaths<'a> {
    CoverPaths {
        library_root: library_root.as_deref(),
        covers_root: covers_root.as_deref(),
    }
}

async fn sync_cover_for_refreshed_entity(
    node_id: DbId,
    entity_type: EntityType,
    library_db_id: Option<DbId>,
    options: CoverSyncOptions,
) -> Result<(), ProviderServiceError> {
    let (library_root, covers_root) = cover_path_buffers_for_library(library_db_id).await?;
    let paths = cover_paths(&library_root, &covers_root);

    match entity_type {
        EntityType::Release => {
            let Some((release, tracks, artists)) = ({
                let db = STATE.db.read().await;
                db::releases::get_by_id(&db, node_id)?.map(|release| {
                    let tracks = db::tracks::get_direct(&db, node_id).unwrap_or_default();
                    let artists = db::artists::get(&db, node_id).unwrap_or_default();
                    (release, tracks, artists)
                })
            }) else {
                return Ok(());
            };

            let synced = sync_release_cover_for_tracks(
                &STATE.db.get(),
                &tracks,
                &release,
                &artists,
                paths,
                options,
            )
            .await
            .map_err(ProviderServiceError::Internal)?;
            let resolved = {
                let db = STATE.db.read().await;
                resolve_cover_for_release_id(&db, node_id, paths)?
            };
            if let Some(cover_path) = resolved {
                upsert_release_cover_metadata(&STATE.db.get(), node_id, &cover_path)
                    .await
                    .map_err(ProviderServiceError::Internal)?;
            }
            tracing::debug!(
                release_id = node_id.0,
                synced,
                "release cover sync completed after metadata refresh"
            );
        }
        EntityType::Artist => {
            let Some(artist) = ({
                let db = STATE.db.read().await;
                db::artists::get_by_id(&db, node_id)?
            }) else {
                return Ok(());
            };

            let synced = sync_artist_cover(&STATE.db.get(), &artist, paths, options, None)
                .await
                .map_err(ProviderServiceError::Internal)?;
            let resolved = {
                let db = STATE.db.read().await;
                resolve_cover_for_artist_id(&db, node_id, paths)?
            };
            if let Some(cover_path) = resolved {
                upsert_artist_cover_metadata(&STATE.db.get(), node_id, &cover_path)
                    .await
                    .map_err(ProviderServiceError::Internal)?;
            }
            tracing::debug!(
                artist_id = node_id.0,
                synced,
                "artist cover sync completed after metadata refresh"
            );
        }
        EntityType::Track => {}
    }

    Ok(())
}

async fn sync_library_covers_after_refresh(
    library_db_id: DbId,
    options: &LibraryRefreshOptions<'_>,
) -> Result<usize, ProviderServiceError> {
    let (library_root, covers_root) = cover_path_buffers_for_library(Some(library_db_id)).await?;
    let paths = cover_paths(&library_root, &covers_root);
    let cover_options = CoverSyncOptions {
        replace_existing: options.replace_cover,
        force_refresh: options.force_refresh,
    };
    let release_count = sync_and_persist_covers_for_library(
        &STATE.db.get(),
        paths,
        library_db_id,
        cover_options,
        options.provider_id,
        CoverScope::Release,
    )
    .await
    .map_err(ProviderServiceError::Internal)?;
    let artist_count = sync_and_persist_covers_for_library(
        &STATE.db.get(),
        paths,
        library_db_id,
        cover_options,
        options.provider_id,
        CoverScope::Artist,
    )
    .await
    .map_err(ProviderServiceError::Internal)?;
    tracing::debug!(
        library_db_id = library_db_id.0,
        release_count,
        artist_count,
        "library cover sync completed after metadata refresh"
    );
    Ok(release_count + artist_count)
}

async fn enabled_providers() -> anyhow::Result<Vec<ProviderConfig>> {
    let db = STATE.db.read().await;
    super::enabled_provider_configs_by_priority(&db, None)
}

async fn refresh_callbacks_for(
    providers: &[ProviderConfig],
    entity_type: EntityType,
    provider_filter: Option<&str>,
    include_sync_filters: bool,
) -> Vec<(
    String,
    ProviderCallbackHandle,
    Option<ProviderCallbackHandle>,
)> {
    let registry = provider_registry().read_owned().await;
    providers
        .iter()
        .filter(|provider| provider_filter.is_none_or(|id| provider.provider_id == id))
        .filter_map(|provider| {
            let handler = registry
                .get_refresh_callback(&provider.provider_id, entity_type)?
                .clone();
            let filter = if include_sync_filters {
                registry
                    .get_sync_filter_callback(&provider.provider_id, entity_type)
                    .cloned()
            } else {
                None
            };
            Some((provider.provider_id.clone(), handler, filter))
        })
        .collect()
}

async fn context_with_options(
    mut context: serde_json::Value,
    provider_id: &str,
    passed_options: &HashMap<String, String>,
) -> serde_json::Value {
    if passed_options.is_empty() {
        return context;
    }

    let options = {
        let registry = provider_registry().read_owned().await;
        let declared = registry.get_options(provider_id);
        passed_options
            .iter()
            .filter_map(|(key, raw_value)| {
                declared.iter().find(|decl| decl.name == *key).map(|decl| {
                    (
                        key.clone(),
                        coerce_option_value(raw_value, &decl.option_type),
                    )
                })
            })
            .collect::<serde_json::Map<_, _>>()
    };
    if options.is_empty() {
        return context;
    }
    if let serde_json::Value::Object(object) = &mut context {
        object.insert("options".to_string(), serde_json::Value::Object(options));
    }
    context
}

async fn dispatch_refresh_callback(
    provider_id: &str,
    handler_id: u64,
    context: serde_json::Value,
) -> Result<(), ProviderServiceError> {
    with_provider_call(
        provider_id,
        ProviderCallStage::MetadataRefresh,
        || async move {
            let runtime = match STATE.generation().plugin_runtime.get() {
                Some(runtime) => runtime,
                None => {
                    return Err(anyhow::anyhow!("plugin runtime is not initialized"));
                }
            };
            tokio::time::timeout(
                DEFAULT_METADATA_REFRESH_TIMEOUT,
                runtime.dispatch_metadata_refresh(MetadataRefreshRequest {
                    handler_id,
                    context,
                }),
            )
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "metadata refresh handler timed out after {} ms",
                    DEFAULT_METADATA_REFRESH_TIMEOUT.as_millis()
                )
            })?
            .map(|_| ())
        },
    )
    .await
    .map_err(ProviderServiceError::Internal)
}

async fn dispatch_sync_filter_callback(
    provider_id: &str,
    handler_id: u64,
    context: serde_json::Value,
) -> anyhow::Result<bool> {
    with_provider_call(
        provider_id,
        ProviderCallStage::MetadataRefresh,
        || async move {
            let runtime = STATE
                .generation()
                .plugin_runtime
                .get()
                .context("plugin runtime is not initialized")?;
            let result = tokio::time::timeout(
                DEFAULT_METADATA_REFRESH_TIMEOUT,
                runtime.dispatch_metadata_refresh(MetadataRefreshRequest {
                    handler_id,
                    context,
                }),
            )
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "sync filter handler timed out after {} ms",
                    DEFAULT_METADATA_REFRESH_TIMEOUT.as_millis()
                )
            })??;
            Ok(result
                .values
                .first()
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false))
        },
    )
    .await
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
    let (unique_release_id_pairs, unique_track_id_pairs) = {
        let registry = futures::executor::block_on(provider_registry().read_owned());
        (
            registry.unique_id_pairs(EntityType::Release),
            registry.unique_track_id_pairs(),
        )
    };
    if unique_release_id_pairs.is_empty() {
        return;
    }
    if let Err(err) = deduplicate_releases_by_external_id(
        db_write,
        library_db_id,
        &unique_release_id_pairs,
        &unique_track_id_pairs,
        Some(provider_scope),
    ) {
        tracing::warn!(
            library_db_id = library_db_id.0,
            error = %err,
            "release deduplication by external id failed during metadata refresh callback"
        );
    }
}
