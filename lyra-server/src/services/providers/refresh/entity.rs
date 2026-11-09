// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.
use agdb::{
    DbAny,
    DbId,
};

use crate::services::{
    EntityType,
    STATE,
    covers::{
        AudioFolders,
        CoverSyncOptions,
        ReleaseCoverSubject,
        forget_gone_covers,
        record_local_release_cover,
        sync_artist_cover,
        sync_release_cover_for_tracks,
    },
    entities::{
        EntityContextError,
        build_entity_provider_context,
    },
    plugin_dispatch::CallSource,
    providers::{
        EntityRefreshOptions,
        EntityRefreshResult,
        ProviderCallError,
        ProviderServiceError,
        RefreshTarget,
        registry_for,
    },
};

use super::{
    context_with_options,
    cover_path_buffers_for_library,
    cover_paths,
    deduplicate_release_scope,
    dispatch::dispatch_refresh,
    passed_provider_options,
    read_active_providers,
    refresh_providers_for,
};

pub(in crate::services::providers) fn resolve_library_id_for_entity(
    db: &DbAny,
    node_id: DbId,
) -> anyhow::Result<Option<DbId>> {
    Ok(lyra_db::libraries::get_for_entity(db, node_id)?
        .into_iter()
        .find_map(|library| library.db_id))
}

pub(crate) async fn refresh_entity_metadata<S: CallSource>(
    source: &S,
    target: RefreshTarget,
    options: EntityRefreshOptions,
) -> Result<EntityRefreshResult, ProviderServiceError> {
    let node_id = target.db_id;

    let schemes = registry_for(source).await.read().await.id_schemes();
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
    let (refreshers, provider_options) = {
        let registry = registry_for(source).await;
        let registry = registry.read().await;
        let refreshers = refresh_providers_for(&registry, &providers, entity_type);
        let provider_options =
            passed_provider_options(&registry, &refreshers, &options.provider_options)?;
        (refreshers, provider_options)
    };
    let mut providers_called = Vec::new();
    for (provider_id, provider_options) in refreshers.into_iter().zip(provider_options) {
        let context = context_with_options(context.clone(), provider_options);
        match dispatch_refresh(source, &provider_id, entity_type, context).await {
            Ok(()) => providers_called.push(provider_id),
            Err(ProviderCallError::NotRegistered) => tracing::warn!(
                provider_id,
                "provider has no handler registered; skipping it for this refresh"
            ),
            // What it saved first stays, so what follows the providers still runs for it.
            Err(ProviderCallError::TargetGone(err)) => {
                tracing::debug!(
                    provider_id,
                    entity_id = %target.public_id,
                    error = %format_args!("{err:#}"),
                    "a refresh's target went away during its handler"
                );
                providers_called.push(provider_id);
            }
            Err(ProviderCallError::Failed(err)) => {
                return Err(ProviderServiceError::Internal(err));
            }
        }
    }

    if entity_type == EntityType::Release
        && let Some(library_db_id) = library_db_id
        && !providers_called.is_empty()
    {
        deduplicate_release_scope(
            &*registry_for(source).await,
            library_db_id,
            &providers_called,
        )
        .await;
    }

    if !providers_called.is_empty() && !options.covers.skips() {
        sync_cover_for_refreshed_entity(
            source,
            &target,
            entity_type,
            library_db_id,
            options.covers,
        )
        .await?;
    }

    Ok(EntityRefreshResult {
        entity_type,
        providers_called,
    })
}

/// Fetches the covers of a refreshed entity, unless its target no longer holds, which the read
/// here checks under the guard it reads with.
async fn sync_cover_for_refreshed_entity<S: CallSource>(
    source: &S,
    target: &RefreshTarget,
    entity_type: EntityType,
    library_db_id: Option<DbId>,
    options: CoverSyncOptions,
) -> Result<(), ProviderServiceError> {
    let node_id = target.db_id;
    let holds = |db: &DbAny| -> anyhow::Result<bool> {
        let holds = target.holds(db)?;
        if !holds {
            tracing::debug!(
                entity_id = %target.public_id,
                "entity gone after its refresh; skipping its cover sync"
            );
        }
        Ok(holds)
    };
    let (library_root, covers_root) = cover_path_buffers_for_library(library_db_id).await?;
    let paths = cover_paths(&library_root, &covers_root);

    match entity_type {
        EntityType::Release => {
            let Some((release, tracks, artists, folders)) = ({
                let db = STATE.db.read().await;
                let release = if holds(&db)? {
                    lyra_db::releases::get_by_id(&db, node_id)?
                } else {
                    None
                };
                let folders = release
                    .as_ref()
                    .map(|_| AudioFolders::of_release(&*db, node_id))
                    .transpose()
                    .map_err(ProviderServiceError::Internal)?;
                release.zip(folders).map(|(release, folders)| {
                    let tracks = lyra_db::tracks::get_direct(&db, node_id).unwrap_or_default();
                    let artists = lyra_db::artists::get(&db, node_id).unwrap_or_default();
                    (release, tracks, artists, std::sync::Arc::new(folders))
                })
            }) else {
                return Ok(());
            };

            let synced = sync_release_cover_for_tracks(
                source,
                &STATE.db.get(),
                &folders,
                ReleaseCoverSubject {
                    release: &release,
                    tracks: &tracks,
                    artists: &artists,
                },
                paths,
                options,
            )
            .await
            .map_err(ProviderServiceError::Internal)?;
            record_local_release_cover(&STATE.db.get(), &folders, node_id, paths.library_root)
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
                if holds(&db)? {
                    lyra_db::artists::get_by_id(&db, node_id)?
                } else {
                    None
                }
            }) else {
                return Ok(());
            };

            let synced = sync_artist_cover(source, &STATE.db.get(), &artist, paths, options, None)
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
