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
            CoverScope,
            configured_covers_root,
            resolve_cover_for_artist_id,
            resolve_cover_for_release_id,
            sync_and_persist_covers_for_library,
            sync_artist_cover,
            sync_release_cover_for_tracks,
        },
        deduplicate_artists_by_external_id,
        entities::{
            EntityContextError,
            build_entity_provider_context,
            build_release_context,
        },
        upsert_artist_cover_metadata,
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

async fn sync_library_cover_scope(
    library_db_id: DbId,
    cover_paths: CoverPaths<'_>,
    cover_sync_options: CoverSyncOptions,
    provider_filter: Option<&str>,
    scope: CoverScope,
) {
    if let Err(err) = sync_and_persist_covers_for_library(
        &STATE.db.get(),
        cover_paths,
        library_db_id,
        cover_sync_options,
        provider_filter,
        scope,
    )
    .await
    {
        tracing::warn!(
            library_db_id = library_db_id.0,
            scope = scope.as_str(),
            error = %err,
            "cover sync failed during library refresh"
        );
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

    if let Some(cover_sync_options) = cover_sync_options {
        match entity_type {
            EntityType::Release => {
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
            EntityType::Artist => {
                let covers_root = configured_covers_root();
                let cover_paths = CoverPaths {
                    // Artist covers are provider-managed and not library-scoped.
                    library_root: None,
                    covers_root: covers_root.as_deref(),
                };
                let artist_entity = {
                    let db = STATE.db.read().await;
                    db::artists::get_by_id(&db, node_id)?
                };

                if let Some(artist) = artist_entity {
                    if let Err(err) = sync_artist_cover(
                        &STATE.db.get(),
                        &artist,
                        cover_paths,
                        cover_sync_options,
                        None,
                    )
                    .await
                    {
                        tracing::warn!(
                            artist_db_id = node_id.0,
                            error = %err,
                            "cover sync failed during artist entity refresh"
                        );
                    }

                    let resolved_cover_path = {
                        let db = STATE.db.read().await;
                        match resolve_cover_for_artist_id(&db, node_id, cover_paths) {
                            Ok(path) => path,
                            Err(err) => {
                                tracing::warn!(
                                    artist_db_id = node_id.0,
                                    error = %err,
                                    "cover resolution failed during artist entity refresh"
                                );
                                None
                            }
                        }
                    };

                    if let Some(cover_path) = resolved_cover_path {
                        if let Err(err) =
                            upsert_artist_cover_metadata(&STATE.db.get(), node_id, &cover_path)
                                .await
                        {
                            tracing::warn!(
                                artist_db_id = node_id.0,
                                cover_path = %cover_path.display(),
                                error = %err,
                                "cover metadata upsert failed during artist entity refresh"
                            );
                        }
                    }
                }
            }
            EntityType::Track => {
                tracing::debug!(
                    track_db_id = node_id.0,
                    "skipping cover sync for track entity refresh"
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
    sync_library_cover_scope(
        library_db_id,
        cover_paths,
        cover_sync_options,
        options.provider_id,
        CoverScope::Release,
    )
    .await;

    let artist_cover_paths = CoverPaths {
        library_root: None,
        covers_root: covers_root.as_deref(),
    };
    sync_library_cover_scope(
        library_db_id,
        artist_cover_paths,
        cover_sync_options,
        options.provider_id,
        CoverScope::Artist,
    )
    .await;

    Ok(refreshed_releases.len())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        fs,
        io::Cursor,
        path::PathBuf,
        sync::Arc,
        time::{
            SystemTime,
            UNIX_EPOCH,
        },
    };

    use agdb::DbId;
    use anyhow::Context;
    use axum::{
        Router,
        extract::Path as AxumPath,
        http::{
            HeaderValue,
            StatusCode,
            header::CONTENT_TYPE,
        },
        response::IntoResponse,
        routing::get,
    };
    use harmony_core::LuaFunctionAsyncExt;
    use image::{
        DynamicImage,
        ImageBuffer,
        ImageFormat,
        Rgba,
    };
    use mlua as mluau;
    use mlua::Lua;
    use mlua::chunk;
    use nanoid::nanoid;
    use tokio::{
        net::TcpListener,
        task::JoinHandle,
    };

    use crate::{
        STATE,
        db,
        plugins::metadata,
        testing::{
            LibraryFixtureConfig,
            PreparedFixture,
            initialize_runtime,
            prepare_fixture,
            runtime_test_lock,
        },
    };

    struct TestDirGuard(PathBuf);

    impl Drop for TestDirGuard {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn setup_metadata_module(lua: &Lua) -> anyhow::Result<()> {
        let table = (metadata::get_module().setup)(lua)?;
        lua.globals().set("metadata", table)?;
        Ok(())
    }

    fn make_test_png(color: [u8; 4]) -> anyhow::Result<Vec<u8>> {
        let image = DynamicImage::ImageRgba8(ImageBuffer::from_pixel(1, 1, Rgba::<u8>(color)));
        let mut bytes = Cursor::new(Vec::new());
        image.write_to(&mut bytes, ImageFormat::Png)?;
        Ok(bytes.into_inner())
    }

    async fn spawn_cover_server(
        responses: HashMap<String, Vec<u8>>,
    ) -> anyhow::Result<(String, JoinHandle<()>)> {
        let responses = Arc::new(responses);
        let app = Router::new().route(
            "/{*path}",
            get({
                let responses = responses.clone();
                move |AxumPath(path): AxumPath<String>| {
                    let responses = responses.clone();
                    async move {
                        if let Some(bytes) = responses.get(&path) {
                            (
                                [(CONTENT_TYPE, HeaderValue::from_static("image/png"))],
                                bytes.clone(),
                            )
                                .into_response()
                        } else {
                            StatusCode::NOT_FOUND.into_response()
                        }
                    }
                }
            }),
        );

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok((format!("http://{addr}"), handle))
    }

    async fn initialize_runtime_with_covers() -> anyhow::Result<(TestDirGuard, PathBuf)> {
        let root = std::env::temp_dir().join(format!(
            "lyra-library-refresh-cover-test-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        let music_dir = root.join("music");
        let covers_dir = root.join("covers");
        fs::create_dir_all(&music_dir)?;
        fs::create_dir_all(&covers_dir)?;

        initialize_runtime(&LibraryFixtureConfig {
            directory: music_dir.clone(),
            language: None,
            country: None,
        })
        .await?;

        let mut config = STATE.config.get().as_ref().clone();
        config.covers_path = Some(covers_dir.clone());
        STATE.config.replace(Arc::new(config));

        Ok((TestDirGuard(root), music_dir))
    }

    async fn prepare_single_artist_fixture(
        music_dir: &std::path::Path,
    ) -> anyhow::Result<PreparedFixture> {
        let track_path = music_dir
            .join("Selected Artist")
            .join("Selected Album")
            .join("01 Track.flac");
        prepare_fixture(
            &LibraryFixtureConfig {
                directory: music_dir.to_path_buf(),
                language: None,
                country: None,
            },
            vec![lyra_metadata::RawTrackTags {
                file_path: track_path.to_string_lossy().into_owned(),
                album: Some("Selected Album".to_string()),
                album_artists: vec!["Selected Artist".to_string()],
                artists: vec!["Selected Artist".to_string()],
                title: Some("Track 1".to_string()),
                date: Some("2024-01-01".to_string()),
                copyright: None,
                genre: None,
                label: None,
                catalog_number: None,
                disc: Some(1),
                disc_total: Some(1),
                track: Some(1),
                track_total: Some(1),
                duration_ms: 60_000,
                sample_rate_hz: None,
                channel_count: None,
                bit_depth: None,
                bitrate_bps: None,
            }],
        )
        .await
    }

    #[tokio::test]
    async fn library_refresh_syncs_artist_covers_and_respects_provider_filter() -> anyhow::Result<()>
    {
        let _guard = runtime_test_lock().await;
        let (_test_dir, music_dir) = initialize_runtime_with_covers().await?;
        setup_metadata_module(STATE.lua.get().as_ref())?;

        let selected_release_png = make_test_png([0, 255, 0, 255])?;
        let selected_artist_png = make_test_png([0, 0, 255, 255])?;
        let ignored_release_png = make_test_png([255, 0, 0, 255])?;
        let ignored_artist_png = make_test_png([255, 255, 0, 255])?;
        let (server_root, server_handle) = spawn_cover_server(HashMap::from([
            (
                "selected-release.png".to_string(),
                selected_release_png.clone(),
            ),
            (
                "selected-artist.png".to_string(),
                selected_artist_png.clone(),
            ),
            (
                "ignored-release.png".to_string(),
                ignored_release_png.clone(),
            ),
            ("ignored-artist.png".to_string(), ignored_artist_png.clone()),
        ]))
        .await?;

        let selected_provider_id = format!("selected-{}", nanoid!());
        let ignored_provider_id = format!("ignored-{}", nanoid!());
        let lua_selected_provider_id = selected_provider_id.clone();
        let lua_ignored_provider_id = ignored_provider_id.clone();

        let register_fn = STATE
            .lua
            .get()
            .load(chunk! {
                local ET = metadata.EntityType

                local selected = metadata.Provider.new($lua_selected_provider_id)
                selected:refresh(ET.Release, function(_ctx) end)
                selected:cover(ET.Release, {}, function(_ctx)
                    return $server_root .. "/selected-release.png"
                end)
                selected:cover(ET.Artist, {}, function(_ctx)
                    return $server_root .. "/selected-artist.png"
                end)

                local ignored = metadata.Provider.new($lua_ignored_provider_id)
                ignored:cover(ET.Release, {}, function(_ctx)
                    return $server_root .. "/ignored-release.png"
                end)
                ignored:cover(ET.Artist, {}, function(_ctx)
                    return $server_root .. "/ignored-artist.png"
                end)
            })
            .set_name(&harmony_core::format_plugin_chunk_name("test", "init"))
            .into_function()?;
        register_fn.call_async::<()>(()).await?;

        {
            let mut db_write = STATE.db.write().await;
            db::providers::update_priority(&mut db_write, &selected_provider_id, 10)?;
            db::providers::update_priority(&mut db_write, &ignored_provider_id, 100)?;
        }

        let prepared = prepare_single_artist_fixture(&music_dir).await?;
        let library_db_id = DbId(prepared.library_id);
        let release_db_id = DbId(prepared.release_id);

        let refreshed = super::refresh_library_metadata(
            library_db_id,
            &super::LibraryRefreshOptions {
                replace_cover: false,
                force_refresh: true,
                apply_sync_filters: false,
                provider_id: Some(&selected_provider_id),
            },
        )
        .await?;
        assert_eq!(refreshed, 1);

        let (artist_db_id, release_cover_path, artist_cover_path) = {
            let db_read = STATE.db.read().await;
            let artists = db::artists::get_by_library(&db_read, library_db_id)?;
            let artist_db_id = artists
                .first()
                .and_then(|artist| artist.db_id.clone().map(Into::<DbId>::into))
                .context("expected one library artist")?;

            let release_cover_path = db::covers::get(&db_read, release_db_id)?
                .map(|cover| cover.path)
                .context("release cover metadata missing after library refresh")?;
            let artist_cover_path = db::covers::get(&db_read, artist_db_id)?
                .map(|cover| cover.path)
                .context("artist cover metadata missing after library refresh")?;

            (artist_db_id, release_cover_path, artist_cover_path)
        };

        let release_bytes = fs::read(&release_cover_path)?;
        let artist_bytes = fs::read(&artist_cover_path)?;
        assert_eq!(release_bytes, selected_release_png);
        assert_eq!(artist_bytes, selected_artist_png);
        assert_ne!(release_bytes, ignored_release_png);
        assert_ne!(artist_bytes, ignored_artist_png);

        let covers_root = STATE
            .config
            .get()
            .covers_path
            .clone()
            .context("covers_path should be configured for test")?;
        assert_eq!(
            PathBuf::from(release_cover_path),
            covers_root
                .join(release_db_id.0.to_string())
                .join("cover.png")
        );
        assert_eq!(
            PathBuf::from(artist_cover_path),
            covers_root
                .join("artists")
                .join(artist_db_id.0.to_string())
                .join("cover.png")
        );

        server_handle.abort();
        Ok(())
    }
}
