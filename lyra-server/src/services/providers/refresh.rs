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
    time::Duration,
};

use agdb::{
    DbAny,
    DbId,
};
use anyhow::Context;

use crate::{
    services::STATE,
    services::plugin_dispatch::{
        MetadataRefreshRequest,
        PluginCallbackHandle,
    },
    services::{
        EntityType,
        covers::{
            CoverPaths,
            CoverSyncOptions,
            configured_covers_root,
            forget_gone_covers,
            record_local_release_cover,
            refresh_gone_covers_for_library,
            sync_and_persist_release_covers_for_library,
            sync_artist_cover,
            sync_artist_covers_for_library,
            sync_release_cover_for_tracks,
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
        options::provider_options,
        providers::{
            ProviderCallStage,
            declared_artist_ids,
            id_schemes,
            library_refresh_locks,
            provider_registry,
            with_provider_call,
        },
    },
};

use lyra_db::ProviderConfig;

use super::{
    ActiveProviders,
    EntityRefreshMode,
    EntityRefreshResult,
    ProviderServiceError,
    RefreshTarget,
    dedup::deduplicate_releases_by_external_id,
};

const DEFAULT_METADATA_REFRESH_TIMEOUT: Duration = Duration::from_secs(60);

/// How long past a handler's deadline the server still waits for the plugin
/// runtime's answer. The runtime answers a late handler only once it has
/// cancelled its thread, so the provider's call lock, which keeps one
/// provider's refreshes from overlapping, is released only after that.
const METADATA_REFRESH_DEADLINE_GRACE: Duration = Duration::from_secs(5);

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
    Ok(lyra_db::libraries::get_for_entity(db, node_id)?
        .into_iter()
        .find_map(|library| library.db_id))
}

pub(crate) async fn refresh_entity_metadata(
    target: RefreshTarget,
    refresh_mode: EntityRefreshMode,
) -> Result<EntityRefreshResult, ProviderServiceError> {
    let node_id = target.db_id;
    let passed_options = match &refresh_mode {
        EntityRefreshMode::MetadataOnly { options }
        | EntityRefreshMode::WithReleaseArtifacts { options, .. } => options.clone(),
    };

    let schemes = id_schemes().await;
    let (entity_type, context, library_db_id) = {
        let db = STATE.db.read().await;
        if !target.holds(&db)? {
            return Err(ProviderServiceError::EntityNotFound(node_id.0));
        }
        let library_db_id = resolve_library_id_for_entity(&db, node_id)?;
        let (entity_type, context) =
            build_entity_provider_context(&db, node_id, library_db_id, &schemes).map_err(
                |err| match err {
                    EntityContextError::EntityNotFound(id) => {
                        ProviderServiceError::EntityNotFound(id)
                    }
                    EntityContextError::Internal(err) => ProviderServiceError::Internal(err),
                },
            )?;
        (entity_type, context, library_db_id)
    };

    let providers = read_active_providers().await?;
    let handlers = refresh_callbacks_for(&providers, entity_type, false).await;
    let options = passed_provider_options(&handlers, &passed_options).await?;
    let mut providers_called = Vec::new();
    for ((provider_id, handler, _), options) in handlers.into_iter().zip(options) {
        let context = context_with_options(context.clone(), options);
        dispatch_refresh_callback(&provider_id, handler, context).await?;
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
        // The providers may have merged the entity away, and its id gone to another node.
        if !target.holds(&*STATE.db.read().await)? {
            tracing::debug!(
                entity_id = %target.public_id,
                "entity gone after its refresh; skipping its cover sync"
            );
            return Ok(EntityRefreshResult {
                entity_type,
                providers_called,
            });
        }
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
    let providers = read_active_providers().await?.only(provider_filter);
    let handlers =
        refresh_callbacks_for(&providers, EntityType::Release, include_sync_filters).await;
    Ok(handlers
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
        lyra_db::libraries::get_by_id(&db, library_db_id)?
            .ok_or(ProviderServiceError::LibraryNotFound(library_db_id.0))?;
        if lyra_db::releases::get_by_id(&db, release_id)?.is_none() {
            return Err(ProviderServiceError::EntityNotFound(release_id.0));
        }
    }

    let providers = read_active_providers().await?.only(options.provider_id);
    let handlers =
        refresh_callbacks_for(&providers, EntityType::Release, options.apply_sync_filters).await;
    let schemes = id_schemes().await;
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
                match build_release_context(&db, release_id, Some(library_db_id), &schemes) {
                    Ok(context) => Some(context),
                    Err(err) => {
                        if providers_called.is_empty() {
                            return Err(ProviderServiceError::Internal(err));
                        }
                        tracing::debug!(
                            library_db_id = library_db_id.0,
                            release_db_id = release_id.0,
                            error = %format_args!("{err:#}"),
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
        let context = context.clone();
        let should_run = if let Some(filter) = filter {
            match dispatch_sync_filter_callback(provider_id, *filter, context.clone()).await {
                Ok(value) => value,
                Err(err) => {
                    tracing::warn!(
                        provider_id,
                        release_db_id = release_id.0,
                        error = %format_args!("{err:#}"),
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

        match dispatch_refresh_callback(provider_id, *handler, context).await {
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
                            format!("{err:#}"),
                        )
                        .await;
                }
                tracing::warn!(
                    provider_id,
                    release_db_id = release_id.0,
                    error = %format_args!("{err:#}"),
                    "provider refresh handler failed during scan release refresh"
                );
                continue;
            }
        }

        let declared_artist_ids = declared_artist_ids().await;
        let mut db_write = STATE.db.write().await;
        if let Err(err) = deduplicate_artists_by_external_id(&mut db_write, &declared_artist_ids) {
            tracing::warn!(
                provider_id,
                release_db_id = release_id.0,
                error = %format_args!("{err:#}"),
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
        lyra_db::libraries::get_by_id(&db, library_db_id)?
            .ok_or(ProviderServiceError::LibraryNotFound(library_db_id.0))?;
        lyra_db::releases::get(&db, library_db_id)?
    };

    let providers = read_active_providers().await?.only(options.provider_id);
    let handlers =
        refresh_callbacks_for(&providers, EntityType::Release, options.apply_sync_filters).await;
    let schemes = id_schemes().await;
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
            let public_id = lyra_db::lookup::find_id_by_db_id(&db, node_id)?;
            release_infos.insert(node_id, (public_id, Some(release.release_title.clone())));
            build_release_context(&db, node_id, Some(library_db_id), &schemes)?
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
                    build_release_context(&db, node_id, Some(library_db_id), &schemes)?
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
            let context = context.clone();

            let should_run = if let Some(filter) = filter {
                match dispatch_sync_filter_callback(provider_id, *filter, context.clone()).await {
                    Ok(value) => value,
                    Err(err) => {
                        tracing::warn!(
                            provider_id,
                            release_db_id = node_id.0,
                            error = %format_args!("{err:#}"),
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

            match dispatch_refresh_callback(provider_id, *handler, context).await {
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
                                format!("{err:#}"),
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

        let declared_artist_ids = declared_artist_ids().await;
        let mut db_write = STATE.db.write().await;
        if let Err(err) = deduplicate_artists_by_external_id(&mut db_write, &declared_artist_ids) {
            tracing::warn!(
                provider_id,
                error = %format_args!("{err:#}"),
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
                                format!("{err:#}"),
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
                lyra_db::releases::get_by_id(&db, node_id)?.map(|release| {
                    let tracks = lyra_db::tracks::get_direct(&db, node_id).unwrap_or_default();
                    let artists = lyra_db::artists::get(&db, node_id).unwrap_or_default();
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
            record_local_release_cover(&STATE.db.get(), node_id, paths.library_root)
                .await
                .map_err(ProviderServiceError::Internal)?;
            tracing::debug!(
                release_id = node_id.0,
                synced,
                "release cover sync completed after metadata refresh"
            );
        }
        EntityType::Artist => {
            let Some(artist) = ({
                let db = STATE.db.read().await;
                lyra_db::artists::get_by_id(&db, node_id)?
            }) else {
                return Ok(());
            };

            let synced = sync_artist_cover(&STATE.db.get(), &artist, paths, options, None)
                .await
                .map_err(ProviderServiceError::Internal)?;
            forget_gone_covers(&STATE.db.get(), node_id)
                .await
                .map_err(ProviderServiceError::Internal)?;
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
    let release_count = sync_and_persist_release_covers_for_library(
        &STATE.db.get(),
        paths,
        library_db_id,
        cover_options,
        options.provider_id,
    )
    .await
    .map_err(ProviderServiceError::Internal)?;
    let artist_count = sync_artist_covers_for_library(
        &STATE.db.get(),
        paths,
        library_db_id,
        cover_options,
        options.provider_id,
    )
    .await
    .map_err(ProviderServiceError::Internal)?;
    refresh_gone_covers_for_library(&STATE.db.get(), library_db_id, paths.library_root)
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

async fn read_active_providers() -> anyhow::Result<ActiveProviders> {
    let db = STATE.db.read().await;
    super::active_providers(&db)
}

async fn refresh_callbacks_for(
    providers: impl IntoIterator<Item = &ProviderConfig>,
    entity_type: EntityType,
    include_sync_filters: bool,
) -> Vec<(String, PluginCallbackHandle, Option<PluginCallbackHandle>)> {
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
            Some((provider.provider_id.clone(), handler, filter))
        })
        .collect()
}

/// Each handler's provider's options, checked for every provider before any of them runs.
async fn passed_provider_options(
    handlers: &[(String, PluginCallbackHandle, Option<PluginCallbackHandle>)],
    passed_options: &HashMap<String, String>,
) -> Result<Vec<serde_json::Map<String, serde_json::Value>>, ProviderServiceError> {
    let registry = provider_registry().read_owned().await;
    handlers
        .iter()
        .map(|(provider_id, _, _)| {
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

async fn dispatch_refresh_callback(
    provider_id: &str,
    handler: PluginCallbackHandle,
    context: serde_json::Value,
) -> Result<(), ProviderServiceError> {
    with_provider_call(
        provider_id,
        ProviderCallStage::MetadataRefresh,
        || async move {
            let runtime = match STATE.generation().plugin_dispatch.get() {
                Some(runtime) => runtime,
                None => {
                    return Err(anyhow::anyhow!("plugin runtime is not initialized"));
                }
            };
            tokio::time::timeout(
                DEFAULT_METADATA_REFRESH_TIMEOUT + METADATA_REFRESH_DEADLINE_GRACE,
                runtime.dispatch_metadata_refresh(MetadataRefreshRequest {
                    handler,
                    context,
                    deadline: std::time::Instant::now() + DEFAULT_METADATA_REFRESH_TIMEOUT,
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
    handler: PluginCallbackHandle,
    context: serde_json::Value,
) -> anyhow::Result<bool> {
    with_provider_call(
        provider_id,
        ProviderCallStage::MetadataRefresh,
        || async move {
            let runtime = STATE
                .generation()
                .plugin_dispatch
                .get()
                .context("plugin runtime is not initialized")?;
            let result = tokio::time::timeout(
                DEFAULT_METADATA_REFRESH_TIMEOUT + METADATA_REFRESH_DEADLINE_GRACE,
                runtime.dispatch_metadata_refresh(MetadataRefreshRequest {
                    handler,
                    context,
                    deadline: std::time::Instant::now() + DEFAULT_METADATA_REFRESH_TIMEOUT,
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

#[cfg(test)]
mod tests {
    use crate::services::testing::{
        FakePluginDispatch,
        FakeResponse,
        init_default_test_state,
        runtime_test_lock,
    };

    #[tokio::test(start_paused = true)]
    async fn a_late_handler_ends_with_the_runtimes_answer() {
        let _guard = runtime_test_lock().await;
        init_default_test_state().expect("test state");
        let dispatch = FakePluginDispatch::install();
        let handler = dispatch.add(FakeResponse::UntilDeadline);

        let error =
            super::dispatch_refresh_callback("late-provider", handler, serde_json::json!({}))
                .await
                .expect_err("a handler past its deadline fails");

        assert!(
            format!("{error:#}").contains("plugin handler deadline expired"),
            "the runtime, having cancelled the handler, answers before the server stops waiting: {error:#}"
        );
    }
}
