// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
};

use agdb::{
    DbAny,
    DbId,
};
use mlua::LuaSerdeExt;

use crate::{
    STATE,
    db::{
        self,
        ProviderConfig,
    },
    plugins::LUA_SERIALIZE_OPTIONS,
    services::EntityType,
    services::providers::{
        LIBRARY_REFRESH_LOCKS,
        PROVIDER_REGISTRY,
    },
    services::{
        CoverPaths,
        CoverSyncOptions,
        covers::{
            configured_covers_root,
            resolve_cover_for_release_id,
            sync_release_cover_for_tracks,
        },
        deduplicate_artists_by_external_id,
        entities::{
            EntityContextError,
            build_entity_provider_context,
            build_release_context,
        },
        resolve_release_covers,
        sync_release_cover_metadata_from_resolved,
        sync_release_covers_for_library,
        upsert_release_cover_metadata,
    },
};

use super::{
    EntityRefreshMode,
    EntityRefreshResult,
    ProviderServiceError,
    dedup::deduplicate_releases_by_external_id,
};

use crate::services::options::coerce_option_value;

struct LibraryRefreshGuard {
    library_db_id: DbId,
}

impl Drop for LibraryRefreshGuard {
    fn drop(&mut self) {
        let library_db_id = self.library_db_id;
        tokio::task::spawn(async move {
            LIBRARY_REFRESH_LOCKS.lock().await.remove(&library_db_id);
        });
    }
}

pub(super) async fn enabled_providers() -> anyhow::Result<Vec<ProviderConfig>> {
    let db = STATE.db.read().await;
    let providers = db::providers::get(&db)?
        .into_iter()
        .filter(|provider| provider.enabled)
        .collect::<Vec<_>>();
    Ok(providers)
}

async fn refresh_handlers(
    providers: &[ProviderConfig],
    entity_type: EntityType,
) -> Vec<(String, crate::plugins::lifecycle::PluginFunctionHandle)> {
    let registry = PROVIDER_REGISTRY.read().await;
    providers
        .iter()
        .filter_map(|provider| {
            registry
                .get_refresh_handler(&provider.provider_id, entity_type)
                .map(|handler| (provider.provider_id.clone(), handler.clone()))
        })
        .collect()
}

pub(super) fn resolve_library_id_for_entity(
    db: &DbAny,
    node_id: DbId,
) -> anyhow::Result<Option<DbId>> {
    let libraries = db::libraries::get_for_entity(db, node_id)?;
    Ok(libraries
        .into_iter()
        .filter_map(|l| l.db_id)
        .min_by_key(|id| id.0))
}

async fn refresh_entity_metadata_inner(
    node_id: DbId,
    refresh_mode: EntityRefreshMode,
) -> Result<EntityRefreshResult, ProviderServiceError> {
    let (cover_sync_options, passed_options) = match &refresh_mode {
        EntityRefreshMode::MetadataOnly => (None, HashMap::new()),
        EntityRefreshMode::WithReleaseArtifacts {
            replace_cover,
            force_refresh,
            options,
        } => (
            Some(CoverSyncOptions {
                replace_existing: *replace_cover,
                force_refresh: *force_refresh,
            }),
            options.clone(),
        ),
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
    let handlers = refresh_handlers(&providers, entity_type).await;

    let declared_options = {
        let registry = PROVIDER_REGISTRY.read().await;
        handlers
            .iter()
            .map(|(pid, _)| (pid.clone(), registry.get_options(pid).to_vec()))
            .collect::<HashMap<String, Vec<_>>>()
    };

    let mut providers_called = Vec::new();
    for (provider_id, handler) in handlers {
        let Some(lua) = handler.try_upgrade_lua() else {
            tracing::warn!(
                provider_id = %provider_id,
                "provider refresh handler's lua instance is no longer valid, skipping"
            );
            continue;
        };
        let call_ctx = lua
            .to_value_with(&context, LUA_SERIALIZE_OPTIONS)
            .map_err(anyhow::Error::from)?;
        if !passed_options.is_empty() {
            if let mlua::Value::Table(ref ctx_table) = call_ctx {
                let declared = declared_options
                    .get(&provider_id)
                    .map(|v| v.as_slice())
                    .unwrap_or(&[]);
                let options_table = lua.create_table().map_err(anyhow::Error::from)?;
                for (key, raw_value) in &passed_options {
                    if let Some(decl) = declared.iter().find(|d| d.name == *key) {
                        let coerced = coerce_option_value(raw_value, &decl.option_type);
                        let lua_val = lua.to_value(&coerced).map_err(anyhow::Error::from)?;
                        options_table
                            .set(key.as_str(), lua_val)
                            .map_err(anyhow::Error::from)?;
                    }
                }
                ctx_table
                    .set("options", options_table)
                    .map_err(anyhow::Error::from)?;
            }
        }
        handler
            .call_async::<_, ()>(call_ctx)
            .await
            .map_err(anyhow::Error::from)?;
        providers_called.push(provider_id);
    }

    if entity_type == EntityType::Release
        && let Some(cover_sync_options) = cover_sync_options
    {
        let covers_root = configured_covers_root();
        let mut resolved_cover_path = None;

        let (release_entity, tracks, artists, library_root) = {
            let db = STATE.db.read().await;
            let library_root = if let Some(library_db_id) = library_db_id {
                db::libraries::get_by_id(&db, library_db_id)?.map(|library| library.directory)
            } else {
                None
            };
            let release = db::releases::get_by_id(&db, node_id)?;
            let tracks = db::tracks::get(&db, node_id)?;
            let artists = db::artists::get(&db, node_id)?;
            (release, tracks, artists, library_root)
        };

        let cover_paths = CoverPaths {
            library_root: library_root.as_deref(),
            covers_root: covers_root.as_deref(),
        };

        if let Some(release) = release_entity {
            if let Err(err) = sync_release_cover_for_tracks(
                &STATE.db.get(),
                &tracks,
                &release,
                &artists,
                cover_paths,
                cover_sync_options,
            )
            .await
            {
                tracing::warn!(
                    release_db_id = node_id.0,
                    error = %err,
                    "cover sync failed during release entity refresh"
                );
            }

            let db = STATE.db.read().await;
            match resolve_cover_for_release_id(&db, node_id, cover_paths) {
                Ok(path) => {
                    resolved_cover_path = path;
                }
                Err(err) => {
                    tracing::warn!(
                        release_db_id = node_id.0,
                        error = %err,
                        "cover resolution failed during release entity refresh"
                    );
                }
            }
        }

        if let Some(cover_path) = resolved_cover_path {
            if let Err(err) =
                upsert_release_cover_metadata(&STATE.db.get(), node_id, &cover_path).await
            {
                tracing::warn!(
                    release_db_id = node_id.0,
                    cover_path = %cover_path.display(),
                    error = %err,
                    "cover metadata upsert failed during release entity refresh"
                );
            }
        }
    }

    if entity_type == EntityType::Release
        && let Some(library_db_id) = library_db_id
        && !providers_called.is_empty()
    {
        let (unique_release_id_pairs, unique_track_id_pairs) = {
            let registry = PROVIDER_REGISTRY.read().await;
            (
                registry.unique_id_pairs(EntityType::Release),
                registry.unique_track_id_pairs(),
            )
        };
        if !unique_release_id_pairs.is_empty() {
            let provider_scope: HashSet<String> = providers_called.iter().cloned().collect();
            let mut db_write = STATE.db.write().await;
            if let Err(err) = deduplicate_releases_by_external_id(
                &mut db_write,
                library_db_id,
                &unique_release_id_pairs,
                &unique_track_id_pairs,
                Some(&provider_scope),
            ) {
                tracing::warn!(
                    library_db_id = library_db_id.0,
                    node_id = node_id.0,
                    error = %err,
                    "release deduplication by external id failed during entity refresh"
                );
            }
        }
    }

    Ok(EntityRefreshResult {
        entity_type,
        providers_called,
    })
}

pub(crate) async fn refresh_entity_metadata(
    node_id: DbId,
    refresh_mode: EntityRefreshMode,
) -> Result<EntityRefreshResult, ProviderServiceError> {
    refresh_entity_metadata_inner(node_id, refresh_mode).await
}

pub(crate) struct LibraryRefreshOptions<'a> {
    pub(crate) replace_cover: bool,
    pub(crate) force_refresh: bool,
    pub(crate) apply_sync_filters: bool,
    pub(crate) provider_id: Option<&'a str>,
}

pub(crate) async fn refresh_library_metadata(
    library_db_id: DbId,
    options: &LibraryRefreshOptions<'_>,
) -> Result<usize, ProviderServiceError> {
    {
        let mut locks = LIBRARY_REFRESH_LOCKS.lock().await;
        if !locks.insert(library_db_id) {
            return Err(ProviderServiceError::RefreshAlreadyRunning(library_db_id.0));
        }
    }

    let _guard = LibraryRefreshGuard { library_db_id };
    refresh_library_metadata_inner(library_db_id, options).await
}

async fn refresh_library_metadata_inner(
    library_db_id: DbId,
    options: &LibraryRefreshOptions<'_>,
) -> Result<usize, ProviderServiceError> {
    let (library, releases) = {
        let db = STATE.db.read().await;
        let library = db::libraries::get_by_id(&db, library_db_id)?
            .ok_or(ProviderServiceError::LibraryNotFound(library_db_id.0))?;
        let releases = db::releases::get(&db, library_db_id)?;
        (library, releases)
    };

    let providers = enabled_providers().await?;
    let provider_handlers: Vec<(
        String,
        crate::plugins::lifecycle::PluginFunctionHandle,
        Option<crate::plugins::lifecycle::PluginFunctionHandle>,
    )> = {
        let registry = PROVIDER_REGISTRY.read().await;
        providers
            .iter()
            .filter(|p| options.provider_id.is_none_or(|id| p.provider_id == id))
            .filter_map(|provider| {
                let handler = registry
                    .get_refresh_handler(&provider.provider_id, EntityType::Release)?
                    .clone();
                let filter = if options.apply_sync_filters {
                    registry
                        .get_sync_filter(&provider.provider_id, EntityType::Release)
                        .cloned()
                } else {
                    None
                };
                Some((provider.provider_id.clone(), handler, filter))
            })
            .collect()
    };
    let (unique_release_id_pairs, unique_track_id_pairs) = {
        let registry = PROVIDER_REGISTRY.read().await;
        (
            registry.unique_id_pairs(EntityType::Release),
            registry.unique_track_id_pairs(),
        )
    };
    let mut refreshed_releases: HashSet<DbId> = HashSet::new();
    let mut context_cache: HashMap<DbId, serde_json::Value> = HashMap::new();
    let mut dirty_releases: HashSet<DbId> = HashSet::new();

    for release in &releases {
        let Some(node_id) = release.db_id.clone().map(Into::<DbId>::into) else {
            continue;
        };
        let ctx = {
            let db = STATE.db.read().await;
            build_release_context(&db, node_id, library.db_id)?
        };
        context_cache.insert(node_id, ctx);
    }

    // Providers outer, albums inner: each provider completes a full pass before
    // the next starts, so downstream providers see upstream writes.
    for (provider_id, handler, filter) in &provider_handlers {
        let Some(lua) = handler.try_upgrade_lua() else {
            tracing::warn!(
                provider_id,
                "provider refresh handler's lua instance is no longer valid, skipping"
            );
            continue;
        };
        let mut pass_touched: HashSet<DbId> = HashSet::new();
        for release in &releases {
            let Some(node_id) = release.db_id.clone().map(Into::<DbId>::into) else {
                continue;
            };

            if dirty_releases.remove(&node_id) {
                let ctx = {
                    let db = STATE.db.read().await;
                    build_release_context(&db, node_id, library.db_id)?
                };
                context_cache.insert(node_id, ctx);
            }
            let Some(context) = context_cache.get(&node_id) else {
                continue;
            };
            let lua_ctx = lua
                .to_value_with(context, LUA_SERIALIZE_OPTIONS)
                .map_err(anyhow::Error::from)?;

            if let Some(filter) = filter {
                match filter.call_async::<_, bool>(lua_ctx.clone()).await {
                    Ok(false) => continue,
                    Ok(true) => {}
                    Err(err) => {
                        tracing::warn!(
                            provider_id,
                            release_db_id = node_id.0,
                            error = %err,
                            "sync filter failed, skipping release for provider"
                        );
                        continue;
                    }
                }
            }

            match handler.call_async::<_, ()>(lua_ctx).await {
                Ok(()) => {
                    refreshed_releases.insert(node_id);
                    dirty_releases.insert(node_id);
                    pass_touched.insert(node_id);
                }
                Err(err) => {
                    tracing::warn!(
                        provider_id,
                        release_db_id = node_id.0,
                        error = %err,
                        "provider refresh handler failed during library refresh"
                    );
                }
            }
        }

        if pass_touched.is_empty() {
            continue;
        }

        {
            let mut db_write = STATE.db.write().await;
            if let Err(err) = deduplicate_artists_by_external_id(&mut db_write) {
                tracing::warn!(
                    provider_id,
                    error = %err,
                    "artist deduplication failed during library refresh"
                );
            }
            if !unique_release_id_pairs.is_empty() {
                let scope = HashSet::from([provider_id.clone()]);
                if let Err(err) = deduplicate_releases_by_external_id(
                    &mut db_write,
                    library_db_id,
                    &unique_release_id_pairs,
                    &unique_track_id_pairs,
                    Some(&scope),
                ) {
                    tracing::warn!(
                        library_db_id = library_db_id.0,
                        provider_id,
                        error = %err,
                        "release deduplication by external id failed during library refresh"
                    );
                }
            }
        }
    }

    let cover_sync_options = CoverSyncOptions {
        replace_existing: options.replace_cover,
        force_refresh: options.force_refresh,
    };
    let covers_root = configured_covers_root();
    let cover_paths = CoverPaths {
        library_root: Some(library.directory.as_path()),
        covers_root: covers_root.as_deref(),
    };
    if let Err(err) = sync_release_covers_for_library(
        &STATE.db.get(),
        cover_paths,
        library_db_id,
        cover_sync_options,
    )
    .await
    {
        tracing::warn!(
            library_db_id = library_db_id.0,
            error = %err,
            "cover sync failed during library refresh"
        );
    }

    let resolved = {
        let db_read = STATE.db.read().await;
        match resolve_release_covers(&db_read, library_db_id, cover_paths) {
            Ok(r) => r,
            Err(err) => {
                tracing::warn!(
                    library_db_id = library_db_id.0,
                    error = %err,
                    "cover resolution failed during library refresh"
                );
                Vec::new()
            }
        }
    };

    if !resolved.is_empty() {
        if let Err(err) =
            sync_release_cover_metadata_from_resolved(&STATE.db.get(), &resolved).await
        {
            tracing::warn!(
                library_db_id = library_db_id.0,
                error = %err,
                "cover metadata sync failed during library refresh"
            );
        }
    }

    Ok(refreshed_releases.len())
}
