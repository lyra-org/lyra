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
            artist_cover_wanted,
            configured_covers_root,
            forget_gone_covers,
            record_local_release_cover,
            refresh_gone_covers_for_library,
            sync_and_persist_release_covers_for_library,
            sync_artist_cover,
            sync_release_cover_for_tracks,
        },
        deduplicate_artists_by_external_id,
        entities::{
            EntityContextError,
            build_entity_provider_context,
            build_release_context,
        },
        libraries::{
            SyncRunControlError,
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
    EntityRefreshOptions,
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
    /// The covers the refreshed releases and artists fetch after the providers ran.
    pub(crate) covers: CoverSyncOptions,
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
    options: EntityRefreshOptions,
) -> Result<EntityRefreshResult, ProviderServiceError> {
    let node_id = target.db_id;

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
    let provider_options = passed_provider_options(&handlers, &options.provider_options).await?;
    let mut providers_called = Vec::new();
    for ((provider_id, handler, _), provider_options) in handlers.into_iter().zip(provider_options)
    {
        let context = context_with_options(context.clone(), provider_options);
        dispatch_refresh_callback(&provider_id, handler, context).await?;
        providers_called.push(provider_id);
    }

    if entity_type == EntityType::Release
        && let Some(library_db_id) = library_db_id
        && !providers_called.is_empty()
    {
        deduplicate_release_scope(library_db_id, &providers_called).await;
    }

    if !providers_called.is_empty() && !options.covers.skips() {
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
        sync_cover_for_refreshed_entity(node_id, entity_type, library_db_id, options.covers)
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

/// How often a refresh waiting for another refresh of its library checks again.
const LIBRARY_REFRESH_LOCK_POLL: Duration = Duration::from_millis(250);

/// [`refresh_library_metadata_with_progress`], first waiting for another refresh of the library,
/// such as a provider sync's, to end. The wait ends early when the run is cancelled.
pub(crate) async fn refresh_library_metadata_after_others(
    library_db_id: DbId,
    options: &LibraryRefreshOptions<'_>,
    progress: Option<SyncRunProgress>,
) -> Result<usize, ProviderServiceError> {
    loop {
        if library_refresh_locks()
            .lock_owned()
            .await
            .insert(library_db_id)
        {
            break;
        }
        if let Some(progress) = &progress
            && let Err(err) = progress.check_cancelled().await
        {
            return Err(ProviderServiceError::Internal(err.into()));
        }
        tokio::time::sleep(LIBRARY_REFRESH_LOCK_POLL).await;
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

/// Refreshes a release a scan found new or changed from every enabled provider, without sync
/// filters.
pub(crate) async fn refresh_release_metadata_for_scan_with_progress(
    library_db_id: DbId,
    release_id: DbId,
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

    let providers = read_active_providers().await?;
    let handlers = refresh_callbacks_for(&providers, EntityType::Release, false).await;
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
        // Providers aren't asked about releases none of whose files are there.
        let availability = crate::services::catalog::Availability::of(&db)?;
        lyra_db::releases::get(&db, library_db_id)?
            .into_iter()
            .filter(|release| {
                release
                    .db_id
                    .clone()
                    .map(DbId::from)
                    .is_some_and(|id| availability.release(id))
            })
            .collect::<Vec<_>>()
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
        if release_count > 0 && !options.covers.skips() {
            // The artist cover pass finalises the totals once it adds its own.
            progress
                .add_stage_total(SyncStageKey::ProviderCover, 1, SyncTotalState::Final)
                .await;
        } else {
            progress.set_determinate().await;
        }
    }
    let mut refreshed_releases = HashSet::new();
    let mut context_cache: HashMap<DbId, serde_json::Value> = HashMap::new();
    // Every release's context is built on its first pass and rebuilt after a provider refreshed
    // it.
    let mut dirty_releases: HashSet<DbId> = releases
        .iter()
        .filter_map(|release| release.db_id.clone().map(Into::<DbId>::into))
        .collect();
    // Releases without a context: gone or merged away (`None`), or failing to build one.
    let mut unavailable: HashMap<DbId, Option<String>> = HashMap::new();

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
                let rebuilt = {
                    let db = STATE.db.read().await;
                    // agdb reuses ids, so the node must still be this release.
                    if lyra_db::lookup::find_id_by_db_id(&db, node_id)?.as_deref()
                        != Some(release.id.as_str())
                    {
                        Err(None)
                    } else {
                        build_release_context(&db, node_id, Some(library_db_id), &schemes)
                            .map_err(|err| Some(format!("{err:#}")))
                    }
                };
                match rebuilt {
                    Ok(context) => {
                        context_cache.insert(node_id, context);
                    }
                    Err(reason) => {
                        context_cache.remove(&node_id);
                        unavailable.insert(node_id, reason);
                    }
                }
            }
            let details = SyncWorkDetails::release(
                "release_provider_refresh",
                Some(release.id.clone()),
                Some(release.release_title.clone()),
            )
            .provider(provider_id.clone());
            let work_id = if let Some(progress) = &progress {
                progress
                    .start_work(SyncStageKey::ProviderRefresh, details.clone())
                    .await
            } else {
                0
            };
            let Some(context) = context_cache.get(&node_id) else {
                match unavailable.get(&node_id).cloned().flatten() {
                    Some(error) => {
                        tracing::warn!(
                            provider_id,
                            release_db_id = node_id.0,
                            error = %error,
                            "release context failed during library refresh"
                        );
                        if let Some(progress) = &progress {
                            progress
                                .fail_work(work_id, SyncStageKey::ProviderRefresh, details, error)
                                .await;
                        }
                    }
                    None => {
                        if let Some(progress) = &progress {
                            progress
                                .skip_work(work_id, SyncStageKey::ProviderRefresh, "release_gone")
                                .await;
                        }
                    }
                }
                continue;
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

    if let Some(progress) = &progress
        && let Err(err) = progress.check_cancelled().await
    {
        return Err(ProviderServiceError::Internal(err.into()));
    }
    if release_count > 0 && !options.covers.skips() {
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
        match sync_library_covers_after_refresh(library_db_id, options).await {
            Ok(_count) => {
                if let Some(progress) = &progress {
                    progress
                        .complete_work(work_id, SyncStageKey::ProviderCover)
                        .await;
                }
            }
            Err(err) => {
                let error = format!("{:#}", err.into_anyhow());
                tracing::warn!(
                    library_db_id = library_db_id.0,
                    error = %error,
                    "library cover sync failed after metadata refresh"
                );
                if let Some(progress) = &progress {
                    progress
                        .fail_work(
                            work_id,
                            SyncStageKey::ProviderCover,
                            SyncWorkDetails::new("library_provider_cover_refresh"),
                            error,
                        )
                        .await;
                }
            }
        }

        let artists = {
            let db = STATE.db.read().await;
            lyra_db::artists::get_by_library(&db, library_db_id)
        };
        let artists = match artists {
            Ok(artists) => ListedArtist::list(artists),
            Err(err) => {
                tracing::warn!(
                    library_db_id = library_db_id.0,
                    error = %format_args!("{err:#}"),
                    "library refresh could not list the library's artists"
                );
                Vec::new()
            }
        };
        let covers_root = configured_covers_root();
        sync_artist_covers(
            &artists,
            CoverPaths {
                library_root: None,
                covers_root: &covers_root,
            },
            options.covers,
            options.provider_id,
            progress.as_ref(),
        )
        .await
        .map_err(|err| ProviderServiceError::Internal(err.into()))?;
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

/// The library's release covers, then its covers whose file is gone, so the artist pass after
/// fetches those again.
async fn sync_library_covers_after_refresh(
    library_db_id: DbId,
    options: &LibraryRefreshOptions<'_>,
) -> Result<usize, ProviderServiceError> {
    let (library_root, covers_root) = cover_path_buffers_for_library(Some(library_db_id)).await?;
    let paths = cover_paths(&library_root, &covers_root);
    let release_count = sync_and_persist_release_covers_for_library(
        &STATE.db.get(),
        paths,
        library_db_id,
        options.covers,
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
        "library release cover sync completed after metadata refresh"
    );
    Ok(release_count)
}

/// An artist a cover pass fetches the cover of, as it was listed.
pub(crate) struct ListedArtist {
    db_id: DbId,
    id: String,
    artist_name: String,
}

impl ListedArtist {
    pub(crate) fn list(artists: Vec<lyra_db::Artist>) -> Vec<Self> {
        artists
            .into_iter()
            .filter_map(|artist| {
                Some(Self {
                    db_id: artist.db_id.map(DbId::from)?,
                    id: artist.id,
                    artist_name: artist.artist_name,
                })
            })
            .collect()
    }
}

/// An artist refresh handler, with the options its provider gets by default.
struct ArtistRefresher {
    provider_id: String,
    handler: PluginCallbackHandle,
    options: serde_json::Map<String, serde_json::Value>,
}

/// The artist refresh handlers of the active providers `provider_filter` allows that also look
/// artist covers up, in provider order: the refreshes whose ids those lookups read. Other
/// providers' artist refreshes are left to the refreshes that run them.
async fn artist_refreshers(provider_filter: Option<&str>) -> Vec<ArtistRefresher> {
    let providers = match read_active_providers().await {
        Ok(providers) => providers.only(provider_filter),
        Err(err) => {
            tracing::warn!(error = %format_args!("{err:#}"), "could not list the active providers");
            return Vec::new();
        }
    };
    let providers = {
        let registry = provider_registry().read_owned().await;
        providers
            .into_iter()
            .filter(|provider| {
                registry
                    .get_cover_handler(&provider.provider_id, EntityType::Artist)
                    .is_some()
            })
            .collect::<Vec<_>>()
    };
    let handlers = refresh_callbacks_for(&providers, EntityType::Artist, false).await;
    let options = match passed_provider_options(&handlers, &HashMap::new()).await {
        Ok(options) => options,
        Err(err) => {
            tracing::warn!(
                error = %format_args!("{:#}", err.into_anyhow()),
                "could not read the artist refresh handlers' options"
            );
            return Vec::new();
        }
    };
    handlers
        .into_iter()
        .zip(options)
        .map(|((provider_id, handler, _), options)| ArtistRefresher {
            provider_id,
            handler,
            options,
        })
        .collect()
}

/// The listed artist as it is now, or none once it is gone or its id went to another node.
async fn current_artist(listed: &ListedArtist) -> anyhow::Result<Option<lyra_db::Artist>> {
    let db = STATE.db.read().await;
    Ok(lyra_db::artists::get_by_id(&db, listed.db_id)?.filter(|artist| artist.id == listed.id))
}

/// Fetches the artist's cover when an automatic sync would, after its providers refreshed it, so
/// the cover handlers see the ids they settled on. A failing refresh leaves the stored ids to
/// the cover handlers. Returns whether it got a cover, or none once the artist is gone; a
/// cancelled run ends it between handlers or before the fetch, with the cancellation as its error.
async fn refresh_and_sync_artist_cover(
    listed: &ListedArtist,
    refreshers: &[ArtistRefresher],
    schemes: &super::IdSchemes,
    paths: CoverPaths<'_>,
    options: CoverSyncOptions,
    provider_filter: Option<&str>,
    progress: Option<&SyncRunProgress>,
) -> anyhow::Result<Option<bool>> {
    let check_cancelled = || async {
        match progress {
            Some(progress) => progress.check_cancelled().await,
            None => Ok(()),
        }
    };
    if current_artist(listed).await?.is_none() {
        return Ok(None);
    }
    if !artist_cover_wanted(&STATE.db.get(), listed.db_id, &listed.id, options).await? {
        return Ok(Some(false));
    }
    let context = {
        let db = STATE.db.read().await;
        build_entity_provider_context(&db, listed.db_id, None, schemes).map(|(_, context)| context)
    };
    match context {
        Ok(context) => {
            for refresher in refreshers {
                check_cancelled().await?;
                let context = context_with_options(context.clone(), refresher.options.clone());
                if let Err(err) =
                    dispatch_refresh_callback(&refresher.provider_id, refresher.handler, context)
                        .await
                {
                    tracing::warn!(
                        provider_id = %refresher.provider_id,
                        artist_id = %listed.id,
                        error = %format_args!("{:#}", err.into_anyhow()),
                        "artist refresh before its cover failed"
                    );
                }
            }
        }
        Err(err) => tracing::warn!(
            artist_id = %listed.id,
            error = %err,
            "artist context before its cover failed"
        ),
    }
    check_cancelled().await?;
    let Some(artist) = current_artist(listed).await? else {
        return Ok(None);
    };
    Ok(Some(
        sync_artist_cover(&STATE.db.get(), &artist, paths, options, provider_filter).await?,
    ))
}

/// Fetches the covers of `artists` from the providers, one after another, reporting one
/// `provider_cover` unit for each, then finalises the run's totals, as the last work to count.
/// Each artist an automatic sync would fetch a cover for is refreshed first by its providers.
/// An artist gone meanwhile is skipped, and one failing leaves the rest to run; only
/// cancellation ends the pass early. Returns how many got a cover.
pub(crate) async fn sync_artist_covers(
    artists: &[ListedArtist],
    paths: CoverPaths<'_>,
    options: CoverSyncOptions,
    provider_filter: Option<&str>,
    progress: Option<&SyncRunProgress>,
) -> Result<usize, SyncRunControlError> {
    if let Some(progress) = progress {
        progress
            .add_stage_total(
                SyncStageKey::ProviderCover,
                artists.len() as u64,
                SyncTotalState::Final,
            )
            .await;
        progress.set_determinate().await;
    }
    let refreshers = artist_refreshers(provider_filter).await;
    let schemes = id_schemes().await;
    let mut synced = 0;
    for listed in artists {
        if let Some(progress) = progress {
            progress.check_cancelled().await?;
        }
        let details = SyncWorkDetails::artist(
            "artist_provider_cover",
            Some(listed.id.clone()),
            Some(listed.artist_name.clone()),
        );
        let work_id = if let Some(progress) = progress {
            progress
                .start_work(SyncStageKey::ProviderCover, details.clone())
                .await
        } else {
            0
        };
        let result = refresh_and_sync_artist_cover(
            listed,
            &refreshers,
            &schemes,
            paths,
            options,
            provider_filter,
            progress,
        )
        .await;
        if let Err(err) = &result
            && err.downcast_ref::<SyncRunControlError>().is_some()
        {
            return Err(SyncRunControlError::Cancelled);
        }
        match result {
            Ok(Some(got_cover)) => {
                synced += usize::from(got_cover);
                if let Some(progress) = progress {
                    progress
                        .complete_work(work_id, SyncStageKey::ProviderCover)
                        .await;
                }
            }
            Ok(None) => {
                if let Some(progress) = progress {
                    progress
                        .skip_work(work_id, SyncStageKey::ProviderCover, "artist_gone")
                        .await;
                }
            }
            Err(err) => {
                let error = format!("{err:#}");
                tracing::warn!(
                    artist_id = %listed.id,
                    error = %error,
                    "artist cover sync failed"
                );
                if let Some(progress) = progress {
                    progress
                        .fail_work(work_id, SyncStageKey::ProviderCover, details, error)
                        .await;
                }
            }
        }
    }
    if let Some(progress) = progress {
        progress.check_cancelled().await?;
    }
    Ok(synced)
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
    use std::sync::Arc;

    use lyra_db::{
        fixtures::{
            connect,
            insert_artist,
            insert_release,
        },
        test_db::insert_library,
    };
    use tokio::sync::Semaphore;

    use super::{
        CoverSyncOptions,
        EntityType,
        LibraryRefreshOptions,
        PluginCallbackHandle,
        STATE,
        provider_registry,
    };
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

    /// Registers `provider_id` with `handler` as its release refresh handler.
    async fn register_release_refresher(
        provider_id: &str,
        handler: crate::services::plugin_dispatch::PluginCallbackHandle,
    ) -> anyhow::Result<()> {
        lyra_db::providers::register(&mut *STATE.db.write().await, provider_id)?;
        let registry = provider_registry();
        let mut registry = registry.write().await;
        registry.register(
            crate::services::plugin_scope::PluginId::new(format!("{provider_id}_plugin").as_str())?,
            provider_id.to_string(),
            None,
        )?;
        registry.set_refresh_callback(provider_id, EntityType::Release, handler);
        Ok(())
    }

    fn called_ids(dispatch: &FakePluginDispatch, handler: PluginCallbackHandle) -> Vec<String> {
        dispatch
            .calls(handler)
            .iter()
            .map(|context| context["id"].as_str().expect("release id").to_string())
            .collect()
    }

    async fn wait_for_calls(
        dispatch: &FakePluginDispatch,
        handler: PluginCallbackHandle,
        n: usize,
    ) {
        for _ in 0..500 {
            if dispatch.calls(handler).len() >= n {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("handler was not called {n} times");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_release_merged_away_between_providers_is_passed_over() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let dispatch = FakePluginDispatch::install();
        let gate = Arc::new(Semaphore::new(0));
        let first = dispatch.add(FakeResponse::Held(
            gate.clone(),
            Box::new(FakeResponse::Value(serde_json::Value::Null)),
        ));
        let second = dispatch.add(FakeResponse::Value(serde_json::Value::Null));
        register_release_refresher("first", first).await?;
        register_release_refresher("second", second).await?;
        let library = {
            let mut db = STATE.db.write().await;
            let library = insert_library(&mut db, "Library", "/library")?;
            for title in ["One", "Two"] {
                let release = insert_release(&mut db, title)?;
                connect(&mut db, library, release)?;
            }
            library
        };

        let refresh = tokio::spawn(async move {
            let options = LibraryRefreshOptions {
                covers: CoverSyncOptions::SKIP,
                apply_sync_filters: false,
                provider_id: None,
            };
            super::refresh_library_metadata(library, &options).await
        });
        // The first provider refreshes one release; it is merged away while the provider
        // refreshes the other.
        wait_for_calls(&dispatch, first, 1).await;
        gate.add_permits(1);
        wait_for_calls(&dispatch, first, 2).await;
        let first_calls = called_ids(&dispatch, first);
        {
            let mut db = STATE.db.write().await;
            let gone = lyra_db::lookup::find_node_id_by_id(&*db, &first_calls[0])?
                .expect("release exists");
            db.exec_mut(agdb::QueryBuilder::remove().ids(gone).query())?;
        }
        gate.add_permits(1);

        let refreshed = refresh.await?.map_err(|err| err.into_anyhow())?;
        assert_eq!(refreshed, 2);
        assert_eq!(
            called_ids(&dispatch, second),
            [first_calls[1].clone()],
            "the second provider passes over the merged release"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_refresh_syncs_covers_even_when_no_provider_refreshed_a_release() -> anyhow::Result<()>
    {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let library = {
            let mut db = STATE.db.write().await;
            let library_id = insert_library(&mut db, "Covers", "/covers-library")?;
            let release = insert_release(&mut db, "Release")?;
            connect(&mut db, library_id, release)?;
            lyra_db::libraries::get_by_id(&db, library_id)?.expect("library")
        };
        let progress = crate::services::libraries::begin_test_run(&library).await;
        let options = LibraryRefreshOptions {
            covers: CoverSyncOptions::default(),
            apply_sync_filters: false,
            provider_id: None,
        };
        let refreshed = super::refresh_library_metadata_with_progress(
            library.db_id.expect("db id"),
            &options,
            Some(progress.clone()),
        )
        .await
        .map_err(|err| err.into_anyhow())?;
        let run = crate::services::libraries::get_sync_run(progress.run_id())
            .await?
            .expect("run");
        crate::services::libraries::end_test_run(&progress).await;

        assert_eq!(refreshed, 0);
        assert_eq!(
            (run.progress.completed_units, run.progress.total_units),
            (1, 1),
            "the cover step ran"
        );
        Ok(())
    }

    fn listed(db: &agdb::DbAny, artist_id: agdb::DbId) -> anyhow::Result<super::ListedArtist> {
        let artist = lyra_db::artists::get_by_id(db, artist_id)?.expect("artist");
        Ok(super::ListedArtist::list(vec![artist])
            .pop()
            .expect("listed"))
    }

    async fn empty_test_library(name: &str) -> anyhow::Result<lyra_db::Library> {
        let mut db = STATE.db.write().await;
        let library_id = insert_library(&mut db, name, &format!("/{name}"))?;
        Ok(lyra_db::libraries::get_by_id(&db, library_id)?.expect("library"))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_artist_pass_skips_gone_or_replaced_artists_and_runs_on_past_a_failure()
    -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let dispatch = FakePluginDispatch::install();
        let handler = dispatch
            .register_artist_cover_handler(
                "artist_covers",
                FakeResponse::Value(serde_json::Value::Null),
            )
            .await?;
        let artists = {
            let mut db = STATE.db.write().await;
            let first = insert_artist(&mut db, "First")?;
            let unreadable = db
                .exec_mut(
                    agdb::QueryBuilder::insert()
                        .nodes()
                        .values([[("db_element_id", "Artist").into()]])
                        .query(),
                )?
                .ids()[0];
            let merged = insert_artist(&mut db, "Merged")?;
            let merged = listed(&db, merged)?;
            db.exec_mut(agdb::QueryBuilder::remove().ids(merged.db_id).query())?;
            let newcomer = insert_artist(&mut db, "Newcomer")?;
            assert_eq!(newcomer, merged.db_id, "agdb reuses the merged artist's id");
            let gone = insert_artist(&mut db, "Gone")?;
            let gone = listed(&db, gone)?;
            let last = insert_artist(&mut db, "Last")?;
            // Removed last, so no insert reuses its id.
            db.exec_mut(agdb::QueryBuilder::remove().ids(gone.db_id).query())?;
            vec![
                listed(&db, first)?,
                super::ListedArtist {
                    db_id: unreadable,
                    id: "unreadable".to_string(),
                    artist_name: "Unreadable".to_string(),
                },
                merged,
                gone,
                listed(&db, last)?,
            ]
        };
        let library = empty_test_library("artist-pass").await?;
        let progress = crate::services::libraries::begin_test_run(&library).await;
        let covers_root = tempfile::tempdir()?;

        let synced = super::sync_artist_covers(
            &artists,
            super::cover_paths(&None, covers_root.path()),
            CoverSyncOptions::default(),
            None,
            Some(&progress),
        )
        .await?;
        let run = crate::services::libraries::get_sync_run(progress.run_id())
            .await?
            .expect("run");
        crate::services::libraries::end_test_run(&progress).await;

        assert_eq!(synced, 0);
        assert_eq!(dispatch.artist_names(handler), ["First", "Last"]);
        let units = &run.progress;
        assert_eq!(
            (
                units.completed_units,
                units.failed_units,
                units.skipped_units,
                units.total_units
            ),
            (2, 1, 2, 5),
            "one unit per artist, every one settled"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_pass_refreshes_the_artists_it_fetches_a_cover_for_with_their_cover_providers()
    -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let dispatch = FakePluginDispatch::install();
        dispatch
            .register_artist_cover_handler(
                "refreshing_covers",
                FakeResponse::Value(serde_json::Value::Null),
            )
            .await?;
        let refresher = dispatch.add(FakeResponse::Value(serde_json::Value::Null));
        let coverless = dispatch.add(FakeResponse::Value(serde_json::Value::Null));
        lyra_db::providers::register(&mut *STATE.db.write().await, "coverless")?;
        {
            let registry = provider_registry();
            let mut registry = registry.write().await;
            registry.set_refresh_callback("refreshing_covers", EntityType::Artist, refresher);
            registry.register(
                crate::services::plugin_scope::PluginId::new("coverless_plugin")?,
                "coverless".to_string(),
                None,
            )?;
            registry.set_refresh_callback("coverless", EntityType::Artist, coverless);
        }
        let artists = {
            let mut db = STATE.db.write().await;
            let covered = insert_artist(&mut db, "Covered")?;
            db.transaction_mut(|t| {
                lyra_db::covers::upsert(
                    t,
                    covered,
                    lyra_db::covers::CoverSource::Manual,
                    lyra_db::Cover {
                        db_id: None,
                        id: nanoid::nanoid!(),
                        path: crate::services::testing::existing_cover_path(),
                        mime_type: "image/png".to_string(),
                        hash: "covered".to_string(),
                        blurhash: None,
                        provider_id: None,
                    },
                )
            })?;
            let bare = insert_artist(&mut db, "Bare")?;
            vec![listed(&db, covered)?, listed(&db, bare)?]
        };
        let covers_root = tempfile::tempdir()?;

        super::sync_artist_covers(
            &artists,
            super::cover_paths(&None, covers_root.path()),
            CoverSyncOptions::default(),
            None,
            None,
        )
        .await?;

        assert_eq!(
            dispatch.artist_names(refresher),
            ["Bare"],
            "an artist keeping its cover is not refreshed"
        );
        assert!(
            dispatch.calls(coverless).is_empty(),
            "a provider without artist covers is not refreshed for them"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_run_cancelled_during_an_artists_refresh_fetches_no_cover() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let dispatch = FakePluginDispatch::install();
        let cover = dispatch
            .register_artist_cover_handler(
                "refresh_then_cover",
                FakeResponse::Value(serde_json::Value::Null),
            )
            .await?;
        let gate = Arc::new(Semaphore::new(0));
        let refresher = dispatch.add(FakeResponse::Held(
            gate.clone(),
            Box::new(FakeResponse::Value(serde_json::Value::Null)),
        ));
        provider_registry().write().await.set_refresh_callback(
            "refresh_then_cover",
            EntityType::Artist,
            refresher,
        );
        let artist = {
            let mut db = STATE.db.write().await;
            let artist = insert_artist(&mut db, "Artist")?;
            listed(&db, artist)?
        };
        let library = empty_test_library("cancelled-refresh-pass").await?;
        let progress = crate::services::libraries::begin_test_run(&library).await;
        let covers_root = tempfile::tempdir()?;

        let pass = tokio::spawn({
            let progress = progress.clone();
            let covers_root = covers_root.path().to_path_buf();
            async move {
                super::sync_artist_covers(
                    &[artist],
                    super::cover_paths(&None, &covers_root),
                    CoverSyncOptions::default(),
                    None,
                    Some(&progress),
                )
                .await
            }
        });
        for _ in 0..500 {
            if !dispatch.calls(refresher).is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(dispatch.calls(refresher).len(), 1, "the refresh started");
        crate::services::libraries::cancel_sync_run(progress.run_id()).await?;
        gate.add_permits(1);
        let result = pass.await?;
        crate::services::libraries::end_test_run(&progress).await;

        assert!(
            matches!(
                result,
                Err(crate::services::libraries::SyncRunControlError::Cancelled)
            ),
            "{result:?}"
        );
        assert!(dispatch.calls(cover).is_empty(), "no cover is looked up");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_artist_pass_stops_when_the_run_is_cancelled() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let dispatch = FakePluginDispatch::install();
        let handler = dispatch
            .register_artist_cover_handler(
                "cancelled_covers",
                FakeResponse::Value(serde_json::Value::Null),
            )
            .await?;
        let artist = {
            let mut db = STATE.db.write().await;
            let artist = insert_artist(&mut db, "Artist")?;
            listed(&db, artist)?
        };
        let library = empty_test_library("cancelled-artist-pass").await?;
        let progress = crate::services::libraries::begin_test_run(&library).await;
        crate::services::libraries::cancel_sync_run(progress.run_id()).await?;
        let covers_root = tempfile::tempdir()?;

        let result = super::sync_artist_covers(
            &[artist],
            super::cover_paths(&None, covers_root.path()),
            CoverSyncOptions::default(),
            None,
            Some(&progress),
        )
        .await;
        crate::services::libraries::end_test_run(&progress).await;

        assert!(
            matches!(
                result,
                Err(crate::services::libraries::SyncRunControlError::Cancelled)
            ),
            "{result:?}"
        );
        assert!(dispatch.calls(handler).is_empty());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_run_cancelled_during_the_last_artist_ends_cancelled() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let dispatch = FakePluginDispatch::install();
        let gate = Arc::new(Semaphore::new(0));
        let handler = dispatch
            .register_artist_cover_handler(
                "held_covers",
                FakeResponse::Held(
                    gate.clone(),
                    Box::new(FakeResponse::Value(serde_json::Value::Null)),
                ),
            )
            .await?;
        let artist = {
            let mut db = STATE.db.write().await;
            let artist = insert_artist(&mut db, "Artist")?;
            listed(&db, artist)?
        };
        let library = empty_test_library("held-artist-pass").await?;
        let progress = crate::services::libraries::begin_test_run(&library).await;
        let covers_root = tempfile::tempdir()?;

        let pass = tokio::spawn({
            let progress = progress.clone();
            let covers_root = covers_root.path().to_path_buf();
            async move {
                super::sync_artist_covers(
                    &[artist],
                    super::cover_paths(&None, &covers_root),
                    CoverSyncOptions::default(),
                    None,
                    Some(&progress),
                )
                .await
            }
        });
        for _ in 0..500 {
            if !dispatch.calls(handler).is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(dispatch.calls(handler).len(), 1, "the lookup started");
        crate::services::libraries::cancel_sync_run(progress.run_id()).await?;
        gate.add_permits(1);
        let result = pass.await?;
        crate::services::libraries::end_test_run(&progress).await;

        assert!(
            matches!(
                result,
                Err(crate::services::libraries::SyncRunControlError::Cancelled)
            ),
            "{result:?}"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_library_refresh_passes_its_track_only_artists() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let dispatch = FakePluginDispatch::install();
        let handler = dispatch
            .register_artist_cover_handler(
                "library_artist_covers",
                FakeResponse::Value(serde_json::Value::Null),
            )
            .await?;
        let library = {
            let mut db = STATE.db.write().await;
            let library_id = insert_library(&mut db, "Artists", "/artists-library")?;
            let release = insert_release(&mut db, "Release")?;
            connect(&mut db, library_id, release)?;
            let track = lyra_db::fixtures::insert_track(&mut db, "Track")?;
            connect(&mut db, release, track)?;
            let main = insert_artist(&mut db, "Main")?;
            let featured = insert_artist(&mut db, "Featured")?;
            lyra_db::test_db::connect_artist(&mut db, release, main)?;
            lyra_db::test_db::connect_artist(&mut db, track, featured)?;
            lyra_db::libraries::get_by_id(&db, library_id)?.expect("library")
        };
        let progress = crate::services::libraries::begin_test_run(&library).await;
        let options = LibraryRefreshOptions {
            covers: CoverSyncOptions::default(),
            apply_sync_filters: false,
            provider_id: None,
        };

        super::refresh_library_metadata_with_progress(
            library.db_id.expect("db id"),
            &options,
            Some(progress.clone()),
        )
        .await
        .map_err(|err| err.into_anyhow())?;
        let run = crate::services::libraries::get_sync_run(progress.run_id())
            .await?
            .expect("run");
        crate::services::libraries::end_test_run(&progress).await;

        assert_eq!(dispatch.artist_names(handler), ["Main", "Featured"]);
        assert_eq!(
            (run.progress.completed_units, run.progress.total_units),
            (3, 3),
            "the release covers' unit and one per artist"
        );
        Ok(())
    }
}
