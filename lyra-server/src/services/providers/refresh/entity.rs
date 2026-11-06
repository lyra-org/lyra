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
        CoverSyncOptions,
        forget_gone_covers,
        record_local_release_cover,
        sync_artist_cover,
        sync_release_cover_for_tracks,
    },
    entities::{
        EntityContextError,
        build_entity_provider_context,
    },
    providers::{
        EntityRefreshOptions,
        EntityRefreshResult,
        ProviderServiceError,
        RefreshTarget,
        id_schemes,
    },
};

use super::{
    context_with_options,
    cover_path_buffers_for_library,
    cover_paths,
    deduplicate_release_scope,
    dispatch::dispatch_refresh_callback,
    passed_provider_options,
    read_active_providers,
    refresh_callbacks_for,
};

pub(in crate::services::providers) fn resolve_library_id_for_entity(
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
    let callbacks = refresh_callbacks_for(&providers, entity_type, false).await;
    let provider_options = passed_provider_options(&callbacks, &options.provider_options).await?;
    let mut providers_called = Vec::new();
    for (callback, provider_options) in callbacks.into_iter().zip(provider_options) {
        let context = context_with_options(context.clone(), provider_options);
        dispatch_refresh_callback(&callback.provider_id, callback.handler, context).await?;
        providers_called.push(callback.provider_id);
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
