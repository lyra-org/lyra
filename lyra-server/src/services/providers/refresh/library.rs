// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::{
        HashMap,
        HashSet,
    },
    time::Duration,
};

use agdb::DbId;

use crate::services::{
    EntityType,
    STATE,
    covers::{
        CoverPaths,
        CoverSyncOptions,
        configured_covers_root,
        refresh_gone_covers_for_library,
        sync_and_persist_release_covers_for_library,
    },
    deduplicate_artists_by_external_id,
    entities::build_release_context,
    libraries::{
        SyncRunProgress,
        SyncStageKey,
        SyncTotalState,
        SyncWorkDetails,
    },
    providers::{
        ProviderServiceError,
        declared_artist_ids,
        id_schemes,
        library_refresh_locks,
    },
};

use super::{
    RefreshCallback,
    artist_covers::{
        ListedArtist,
        sync_artist_covers,
    },
    cover_path_buffers_for_library,
    cover_paths,
    deduplicate_release_scope_locked,
    dispatch::{
        dispatch_refresh_callback,
        dispatch_sync_filter_callback,
    },
    read_active_providers,
    refresh_callbacks_for,
};

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

/// Refreshes the library's releases, then their covers, unless another refresh of it is running.
pub(crate) async fn refresh_library_metadata(
    library_db_id: DbId,
    options: &LibraryRefreshOptions<'_>,
    progress: SyncRunProgress,
) -> Result<usize, ProviderServiceError> {
    {
        let mut locks = library_refresh_locks().lock_owned().await;
        if !locks.insert(library_db_id) {
            return Err(ProviderServiceError::RefreshAlreadyRunning(library_db_id.0));
        }
    }

    let _guard = LibraryRefreshGuard { library_db_id };
    refresh_locked_library(library_db_id, options, progress).await
}

/// How often a refresh waiting for another refresh of its library checks again.
const LIBRARY_REFRESH_LOCK_POLL: Duration = Duration::from_millis(250);

/// [`refresh_library_metadata`], first waiting for another refresh of the library,
/// such as a provider sync's, to end. The wait ends early when the work is cancelled.
pub(crate) async fn refresh_library_metadata_after_others(
    library_db_id: DbId,
    options: &LibraryRefreshOptions<'_>,
    progress: SyncRunProgress,
) -> Result<usize, ProviderServiceError> {
    loop {
        if library_refresh_locks()
            .lock_owned()
            .await
            .insert(library_db_id)
        {
            break;
        }
        if let Err(err) = progress.check_cancelled() {
            return Err(ProviderServiceError::Internal(err.into()));
        }
        tokio::time::sleep(LIBRARY_REFRESH_LOCK_POLL).await;
    }

    let _guard = LibraryRefreshGuard { library_db_id };
    refresh_locked_library(library_db_id, options, progress).await
}

pub(crate) async fn release_refresh_provider_ids(
    provider_filter: Option<&str>,
    include_sync_filters: bool,
) -> Result<Vec<String>, ProviderServiceError> {
    let providers = read_active_providers().await?.only(provider_filter);
    let callbacks =
        refresh_callbacks_for(&providers, EntityType::Release, include_sync_filters).await;
    Ok(callbacks
        .into_iter()
        .map(|callback| callback.provider_id)
        .collect())
}

/// Refreshes a release a scan found new or changed from every enabled provider, without sync
/// filters, returning the providers that refreshed it. The scan merges what they found with
/// [`deduplicate_after_providers`] once the refreshes running beside this one are done, so what
/// runs after them reads what the merge left.
pub(crate) async fn refresh_release_metadata_for_scan(
    library_db_id: DbId,
    release_id: DbId,
    progress: SyncRunProgress,
    release_public_id: Option<String>,
    release_title: Option<String>,
) -> Result<Vec<String>, ProviderServiceError> {
    {
        let db = STATE.db.read().await;
        lyra_db::libraries::get_by_id(&db, library_db_id)?
            .ok_or(ProviderServiceError::LibraryNotFound(library_db_id.0))?;
        if lyra_db::releases::get_by_id(&db, release_id)?.is_none() {
            return Err(ProviderServiceError::EntityNotFound(release_id.0));
        }
    }

    let providers = read_active_providers().await?;
    let callbacks = refresh_callbacks_for(&providers, EntityType::Release, false).await;
    let schemes = id_schemes().await;
    let mut providers_called = Vec::new();
    let mut context: Option<serde_json::Value> = None;
    let mut dirty = true;

    for callback in &callbacks {
        let provider_id = &callback.provider_id;
        if progress.is_cancelled() {
            break;
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
        let refreshed = refresh_release_with_provider(
            callback,
            context.clone(),
            release_id,
            &progress,
            details,
        )
        .await;
        if !refreshed {
            continue;
        }
        providers_called.push(provider_id.clone());
        dirty = true;
    }

    Ok(providers_called)
}

async fn refresh_locked_library(
    library_db_id: DbId,
    options: &LibraryRefreshOptions<'_>,
    progress: SyncRunProgress,
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
    let callbacks =
        refresh_callbacks_for(&providers, EntityType::Release, options.apply_sync_filters).await;
    let schemes = id_schemes().await;
    let release_count = releases
        .iter()
        .filter(|release| release.db_id.is_some())
        .count() as u64;
    progress
        .add_stage_total(
            SyncStageKey::ProviderRefresh,
            release_count * callbacks.len() as u64,
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

    for callback in &callbacks {
        let provider_id = &callback.provider_id;
        let mut pass_touched = HashSet::new();
        for release in &releases {
            let Some(node_id) = release.db_id.clone().map(Into::<DbId>::into) else {
                continue;
            };
            if let Err(err) = progress.check_cancelled() {
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
            let Some(context) = context_cache.get(&node_id) else {
                let work_id = progress
                    .start_work(SyncStageKey::ProviderRefresh, details.clone())
                    .await;
                match unavailable.get(&node_id).cloned().flatten() {
                    Some(error) => {
                        progress
                            .fail_work(work_id, SyncStageKey::ProviderRefresh, details, error)
                            .await;
                    }
                    None => {
                        progress
                            .skip_work(work_id, SyncStageKey::ProviderRefresh, "release_gone")
                            .await;
                    }
                }
                continue;
            };
            let refreshed = refresh_release_with_provider(
                callback,
                context.clone(),
                node_id,
                &progress,
                details,
            )
            .await;
            if refreshed {
                refreshed_releases.insert(node_id);
                dirty_releases.insert(node_id);
                pass_touched.insert(node_id);
            }
        }

        if pass_touched.is_empty() {
            continue;
        }

        deduplicate_after_providers(&HashSet::from([provider_id.clone()]), library_db_id).await;
    }

    if let Err(err) = progress.check_cancelled() {
        return Err(ProviderServiceError::Internal(err.into()));
    }
    if release_count > 0 && !options.covers.skips() {
        let work_id = progress
            .start_work(
                SyncStageKey::ProviderCover,
                SyncWorkDetails::new("library_provider_cover_refresh"),
            )
            .await;
        match sync_library_covers_after_refresh(library_db_id, options, &progress).await {
            Ok(_count) => {
                progress
                    .complete_work(work_id, SyncStageKey::ProviderCover)
                    .await;
            }
            Err(err) if err.is_cancellation() => return Err(err),
            Err(err) => {
                let error = format!("{:#}", err.into_anyhow());
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
            &progress,
        )
        .await
        .map_err(|err| ProviderServiceError::Internal(err.into()))?;
    }

    Ok(refreshed_releases.len())
}

/// Merges what the providers' refreshes made duplicates of, unless none of them refreshed
/// anything.
pub(crate) async fn deduplicate_after_providers(
    provider_ids: &HashSet<String>,
    library_db_id: DbId,
) {
    if provider_ids.is_empty() {
        return;
    }
    let declared_artist_ids = declared_artist_ids().await;
    let mut db_write = STATE.db.write().await;
    if let Err(err) = deduplicate_artists_by_external_id(&mut db_write, &declared_artist_ids) {
        tracing::warn!(
            ?provider_ids,
            error = %format_args!("{err:#}"),
            "artist deduplication failed during metadata refresh"
        );
    }
    deduplicate_release_scope_locked(&mut db_write, library_db_id, provider_ids);
}

/// One unit of work: asks a provider about a release, unless its sync filter turns the release
/// down. Returns whether the provider refreshed the release.
async fn refresh_release_with_provider(
    callback: &RefreshCallback,
    context: serde_json::Value,
    release_id: DbId,
    progress: &SyncRunProgress,
    details: SyncWorkDetails,
) -> bool {
    let provider_id = callback.provider_id.as_str();
    let work_id = progress
        .start_work(SyncStageKey::ProviderRefresh, details.clone())
        .await;
    let should_run = match callback.filter {
        Some(filter) => dispatch_sync_filter_callback(provider_id, filter, context.clone())
            .await
            .unwrap_or_else(|err| {
                tracing::warn!(
                    provider_id,
                    release_db_id = release_id.0,
                    error = %format_args!("{err:#}"),
                    "sync filter failed, skipping release for provider"
                );
                false
            }),
        None => true,
    };
    if !should_run {
        progress
            .skip_work(work_id, SyncStageKey::ProviderRefresh, "sync_filter")
            .await;
        return false;
    }

    match dispatch_refresh_callback(provider_id, callback.handler, context).await {
        Ok(()) => {
            progress
                .complete_work(work_id, SyncStageKey::ProviderRefresh)
                .await;
            true
        }
        Err(err) => {
            progress
                .fail_work(
                    work_id,
                    SyncStageKey::ProviderRefresh,
                    details,
                    format!("{err:#}"),
                )
                .await;
            false
        }
    }
}

/// The library's release covers, then its covers whose file is gone, so the artist pass after
/// fetches those again.
async fn sync_library_covers_after_refresh(
    library_db_id: DbId,
    options: &LibraryRefreshOptions<'_>,
    progress: &SyncRunProgress,
) -> Result<usize, ProviderServiceError> {
    let (library_root, covers_root) = cover_path_buffers_for_library(Some(library_db_id)).await?;
    let paths = cover_paths(&library_root, &covers_root);
    let release_count = sync_and_persist_release_covers_for_library(
        &STATE.db.get(),
        paths,
        library_db_id,
        options.covers,
        options.provider_id,
        progress.cancellation(),
    )
    .await
    .map_err(ProviderServiceError::Internal)?;
    progress
        .check_cancelled()
        .map_err(|err| ProviderServiceError::Internal(err.into()))?;
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
