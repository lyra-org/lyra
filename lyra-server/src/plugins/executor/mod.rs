// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod capabilities;
mod messages;
mod parsers;
use self::capabilities::ManifestCapabilityPolicy;
pub(crate) use self::messages::{
    ApiHandlerRequest,
    ApiHandlerResponse,
    ApiResponseBody,
    MetadataRefreshRequest,
    MetadataRefreshResult,
    MixHandlerRequest,
    MixHandlerResult,
    WebSocketStartRequest,
    WebSocketState,
};
use self::messages::{
    PluginExecutorCommand,
    TaskIdKey,
};
use self::parsers::{
    api_context_value,
    mix_context_value,
    parse_api_response,
    parse_mix_result,
    websocket_reader_value,
    websocket_sender_value,
};

use std::{
    cell::RefCell,
    collections::{
        HashMap,
        HashSet,
    },
    fs,
    path::PathBuf,
    sync::{
        Arc,
        mpsc,
    },
    thread,
    time::Duration,
};

use anyhow::{
    Context,
    Result,
    bail,
};
#[cfg(test)]
use harmony_core::MemorySourceLoader;
use harmony_core::{
    CallContext,
    ChunkOrigin,
    FilesystemSourceLoader,
    LoadedPlugin,
    LocalScheduler,
    LuauRequireRuntime,
    ModuleId,
    ModuleSpec,
    PluginLoadError,
    PluginManager,
    PluginManifest,
    SourceLoader,
    TaskState,
    install_luau_globals,
    install_luau_require,
};
use harmony_luau as luau;

pub(crate) struct PluginExecutor {
    vm: luau::Vm,
    plugins: Arc<[LoadedPlugin]>,
    tokio_runtime: ExecutorTokioRuntime,
    websocket_tasks: RefCell<HashMap<TaskIdKey, Arc<WebSocketState>>>,
}

struct ExecutorTokioRuntime {
    handle: tokio::runtime::Handle,
    owned: Option<tokio::runtime::Runtime>,
}

impl ExecutorTokioRuntime {
    fn new() -> Result<Self> {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            return Ok(Self {
                handle,
                owned: None,
            });
        }

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .context("create plugin executor Tokio runtime")?;
        let handle = runtime.handle().clone();
        Ok(Self {
            handle,
            owned: Some(runtime),
        })
    }

    fn enter(&self) -> tokio::runtime::EnterGuard<'_> {
        self.handle.enter()
    }
}

impl Drop for ExecutorTokioRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.owned.take() {
            runtime.shutdown_background();
        }
    }
}

#[derive(Clone)]
pub(crate) struct PluginExecutorHandle {
    tx: mpsc::Sender<PluginExecutorCommand>,
}

impl PluginExecutorHandle {
    pub(crate) fn discover_from_plugins_dir_with_db(
        plugins_dir: impl Into<PathBuf>,
        server_info: crate::plugins::server::ServerInfo,
        db: crate::plugins::db::DbAsync,
    ) -> Result<(Self, Vec<PluginLoadError>)> {
        Self::discover_from_plugins_dir_with_db_and_modules(
            plugins_dir,
            server_info,
            db,
            Vec::new(),
        )
    }

    pub(crate) fn discover_from_plugins_dir_with_db_and_modules(
        plugins_dir: impl Into<PathBuf>,
        server_info: crate::plugins::server::ServerInfo,
        db: crate::plugins::db::DbAsync,
        module_overrides: Vec<ModuleSpec>,
    ) -> Result<(Self, Vec<PluginLoadError>)> {
        let plugins_dir = plugins_dir.into();
        let (tx, rx) = mpsc::channel();
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);

        thread::Builder::new()
            .name("lyra-plugin-executor".to_string())
            .spawn(move || {
                match PluginExecutor::discover_from_plugins_dir_with_db_and_modules(
                    plugins_dir,
                    server_info,
                    db,
                    module_overrides,
                ) {
                    Ok((runtime, errors)) => {
                        if ready_tx.send(Ok(errors)).is_err() {
                            return;
                        }
                        run_plugin_executor_thread(runtime, rx);
                    }
                    Err(error) => {
                        let _ = ready_tx.send(Err(error));
                    }
                }
            })
            .context("spawn plugin executor thread")?;

        let errors = ready_rx
            .recv()
            .context("plugin executor thread exited during startup")??;
        Ok((Self { tx }, errors))
    }

    pub(crate) fn plugin_manifests(&self) -> Result<Vec<PluginManifest>> {
        self.request(PluginExecutorCommand::PluginManifests)
    }

    pub(crate) fn has_plugin(&self, plugin_id: &str) -> Result<bool> {
        self.request(|reply| PluginExecutorCommand::HasPlugin {
            plugin_id: plugin_id.to_string(),
            reply,
        })
    }

    pub(crate) fn exec_plugin(&self, plugin_id: &str) -> Result<()> {
        self.request(|reply| PluginExecutorCommand::ExecPlugin {
            plugin_id: plugin_id.to_string(),
            reply,
        })
    }

    pub(crate) fn exec_all(&self) -> Result<()> {
        self.request(PluginExecutorCommand::ExecAll)
    }

    pub(crate) fn dispatch_playback_update(
        &self,
        payload: crate::services::playback_sessions::PlaybackUpdatePayload,
    ) -> Result<()> {
        self.tx
            .send(PluginExecutorCommand::PlaybackUpdate(payload))
            .context("plugin executor thread is unavailable")
    }

    pub(crate) fn dispatch_mix_handler(
        &self,
        request: MixHandlerRequest,
    ) -> Result<MixHandlerResult> {
        self.request(|reply| PluginExecutorCommand::MixHandler { request, reply })
    }

    pub(crate) fn dispatch_metadata_refresh(
        &self,
        request: MetadataRefreshRequest,
    ) -> Result<MetadataRefreshResult> {
        self.request(|reply| PluginExecutorCommand::MetadataRefresh { request, reply })
    }

    pub(crate) fn dispatch_api_handler(
        &self,
        request: ApiHandlerRequest,
    ) -> Result<ApiHandlerResponse> {
        self.request(|reply| PluginExecutorCommand::ApiHandler { request, reply })
    }

    pub(crate) fn start_websocket(&self, request: WebSocketStartRequest) -> Result<()> {
        self.request(|reply| PluginExecutorCommand::StartWebSocket { request, reply })
    }

    fn request<T>(
        &self,
        build: impl FnOnce(mpsc::Sender<Result<T>>) -> PluginExecutorCommand,
    ) -> Result<T>
    where
        T: Send + 'static,
    {
        let (reply, rx) = mpsc::channel();
        self.tx
            .send(build(reply))
            .context("plugin executor thread is unavailable")?;
        rx.recv()
            .context("plugin executor thread dropped response")?
    }
}

fn run_plugin_executor_thread(runtime: PluginExecutor, rx: mpsc::Receiver<PluginExecutorCommand>) {
    loop {
        runtime.poll_background_tasks();
        let wait = runtime
            .next_scheduler_delay()
            .unwrap_or_else(|| Duration::from_millis(100))
            .min(Duration::from_millis(25));
        match rx.recv_timeout(wait) {
            Ok(command) => handle_plugin_executor_command(&runtime, command),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
}

fn handle_plugin_executor_command(runtime: &PluginExecutor, command: PluginExecutorCommand) {
    match command {
        PluginExecutorCommand::PluginManifests(reply) => {
            let _ = reply.send(Ok(runtime.plugin_manifests()));
        }
        PluginExecutorCommand::HasPlugin { plugin_id, reply } => {
            let _ = reply.send(Ok(runtime.has_plugin(&plugin_id)));
        }
        PluginExecutorCommand::ExecPlugin { plugin_id, reply } => {
            let _ = reply.send(runtime.exec_plugin(&plugin_id));
        }
        PluginExecutorCommand::ExecAll(reply) => {
            let _ = reply.send(runtime.exec_all());
        }
        PluginExecutorCommand::MixHandler { request, reply } => {
            let _ = reply.send(runtime.dispatch_mix_handler(request));
        }
        PluginExecutorCommand::MetadataRefresh { request, reply } => {
            let _ = reply.send(runtime.dispatch_metadata_refresh(request));
        }
        PluginExecutorCommand::ApiHandler { request, reply } => {
            let _ = reply.send(runtime.dispatch_api_handler(request));
        }
        PluginExecutorCommand::StartWebSocket { request, reply } => {
            let _ = reply.send(runtime.start_websocket(request));
        }
        PluginExecutorCommand::PlaybackUpdate(payload) => {
            if let Err(error) = runtime.dispatch_playback_update(payload) {
                tracing::warn!(error = %error, "plugin playback on_update dispatch failed");
            }
        }
    }
}

impl PluginExecutor {
    #[cfg(test)]
    pub(crate) fn with_manifests(manifests: Arc<[harmony_core::PluginManifest]>) -> Result<Self> {
        Self::with_runtime_state(manifests, default_server_info())
    }

    #[cfg(test)]
    pub(crate) fn with_runtime_state(
        manifests: Arc<[harmony_core::PluginManifest]>,
        server_info: crate::plugins::server::ServerInfo,
    ) -> Result<Self> {
        Self::with_loader(
            manifests,
            server_info,
            crate::plugins::artists::ArtistsModuleStore::empty(),
            crate::plugins::chromaprint::ChromaprintModuleStore::empty(),
            crate::plugins::covers::CoversModuleStore::empty(),
            crate::plugins::ids::IdsLookupModuleStore::empty(),
            crate::plugins::tags::TagsModuleStore::empty(),
            crate::plugins::datastore::DataStoreModuleStore::empty(),
            crate::plugins::entities::EntitiesModuleStore::empty(),
            crate::plugins::entries::EntriesModuleStore::empty(),
            crate::plugins::favorites::FavoritesModuleStore::empty(),
            crate::plugins::genres::GenresModuleStore::empty(),
            crate::plugins::libraries::LibrariesModuleStore::empty(),
            crate::plugins::listens::ListensModuleStore::empty(),
            crate::plugins::metadata::MetadataModuleStore::empty(),
            crate::plugins::playback_sessions::PlaybackSessionsModuleStore::empty(),
            crate::plugins::playback_sources::PlaybackSourcesModuleStore::empty(),
            crate::plugins::playlists::PlaylistsModuleStore::empty(),
            crate::plugins::releases::ReleasesModuleStore::empty(),
            crate::plugins::runtime::PluginSettingsModuleStore::empty(),
            crate::plugins::track_sources::TrackSourcesModuleStore::empty(),
            crate::plugins::tracks::TracksModuleStore::empty(),
            crate::plugins::users::UsersModuleStore::empty(),
            MemorySourceLoader::new(),
        )
    }

    #[cfg(test)]
    pub(crate) fn with_database(
        manifests: Arc<[harmony_core::PluginManifest]>,
        server_info: crate::plugins::server::ServerInfo,
        db: crate::plugins::db::DbAsync,
    ) -> Result<Self> {
        Self::with_loader(
            manifests,
            server_info,
            crate::plugins::artists::ArtistsModuleStore::with_db(db.clone()),
            crate::plugins::chromaprint::ChromaprintModuleStore::with_db(db.clone()),
            crate::plugins::covers::CoversModuleStore::with_db(db.clone()),
            crate::plugins::ids::IdsLookupModuleStore::with_db(db.clone()),
            crate::plugins::tags::TagsModuleStore::with_db(db.clone()),
            crate::plugins::datastore::DataStoreModuleStore::with_db(db.clone()),
            crate::plugins::entities::EntitiesModuleStore::with_db(db.clone()),
            crate::plugins::entries::EntriesModuleStore::with_db(db.clone()),
            crate::plugins::favorites::FavoritesModuleStore::with_db(db.clone()),
            crate::plugins::genres::GenresModuleStore::with_db(db.clone()),
            crate::plugins::libraries::LibrariesModuleStore::with_db(db.clone()),
            crate::plugins::listens::ListensModuleStore::with_db(db.clone()),
            crate::plugins::metadata::MetadataModuleStore::with_db(db.clone()),
            crate::plugins::playback_sessions::PlaybackSessionsModuleStore::with_db(db.clone()),
            crate::plugins::playback_sources::PlaybackSourcesModuleStore::with_db(db.clone()),
            crate::plugins::playlists::PlaylistsModuleStore::with_db(db.clone()),
            crate::plugins::releases::ReleasesModuleStore::with_db(db.clone()),
            crate::plugins::runtime::PluginSettingsModuleStore::with_db(db.clone()),
            crate::plugins::track_sources::TrackSourcesModuleStore::with_db(db.clone()),
            crate::plugins::tracks::TracksModuleStore::with_db(db.clone()),
            crate::plugins::users::UsersModuleStore::with_db(db),
            MemorySourceLoader::new(),
        )
    }

    pub(crate) fn with_filesystem_sources(
        manifests: Arc<[harmony_core::PluginManifest]>,
        server_info: crate::plugins::server::ServerInfo,
        source_root: impl Into<std::path::PathBuf>,
        plugins_dir: impl Into<std::path::PathBuf>,
    ) -> Result<Self> {
        Self::with_loader(
            manifests,
            server_info,
            crate::plugins::artists::ArtistsModuleStore::empty(),
            crate::plugins::chromaprint::ChromaprintModuleStore::empty(),
            crate::plugins::covers::CoversModuleStore::empty(),
            crate::plugins::ids::IdsLookupModuleStore::empty(),
            crate::plugins::tags::TagsModuleStore::empty(),
            crate::plugins::datastore::DataStoreModuleStore::empty(),
            crate::plugins::entities::EntitiesModuleStore::empty(),
            crate::plugins::entries::EntriesModuleStore::empty(),
            crate::plugins::favorites::FavoritesModuleStore::empty(),
            crate::plugins::genres::GenresModuleStore::empty(),
            crate::plugins::libraries::LibrariesModuleStore::empty(),
            crate::plugins::listens::ListensModuleStore::empty(),
            crate::plugins::metadata::MetadataModuleStore::empty(),
            crate::plugins::playback_sessions::PlaybackSessionsModuleStore::empty(),
            crate::plugins::playback_sources::PlaybackSourcesModuleStore::empty(),
            crate::plugins::playlists::PlaylistsModuleStore::empty(),
            crate::plugins::releases::ReleasesModuleStore::empty(),
            crate::plugins::runtime::PluginSettingsModuleStore::empty(),
            crate::plugins::track_sources::TrackSourcesModuleStore::empty(),
            crate::plugins::tracks::TracksModuleStore::empty(),
            crate::plugins::users::UsersModuleStore::empty(),
            FilesystemSourceLoader::new(source_root, plugins_dir),
        )
    }

    #[cfg(test)]
    pub(crate) fn discover_from_plugins_dir(
        plugins_dir: impl Into<PathBuf>,
        server_info: crate::plugins::server::ServerInfo,
    ) -> Result<(Self, Vec<PluginLoadError>)> {
        Self::discover_from_plugins_dir_with_ids_store(
            plugins_dir,
            server_info,
            crate::plugins::artists::ArtistsModuleStore::empty(),
            crate::plugins::chromaprint::ChromaprintModuleStore::empty(),
            crate::plugins::covers::CoversModuleStore::empty(),
            crate::plugins::ids::IdsLookupModuleStore::empty(),
            crate::plugins::tags::TagsModuleStore::empty(),
            crate::plugins::datastore::DataStoreModuleStore::empty(),
            crate::plugins::entities::EntitiesModuleStore::empty(),
            crate::plugins::entries::EntriesModuleStore::empty(),
            crate::plugins::favorites::FavoritesModuleStore::empty(),
            crate::plugins::genres::GenresModuleStore::empty(),
            crate::plugins::libraries::LibrariesModuleStore::empty(),
            crate::plugins::listens::ListensModuleStore::empty(),
            crate::plugins::metadata::MetadataModuleStore::empty(),
            crate::plugins::playback_sessions::PlaybackSessionsModuleStore::empty(),
            crate::plugins::playback_sources::PlaybackSourcesModuleStore::empty(),
            crate::plugins::playlists::PlaylistsModuleStore::empty(),
            crate::plugins::releases::ReleasesModuleStore::empty(),
            crate::plugins::runtime::PluginSettingsModuleStore::empty(),
            crate::plugins::track_sources::TrackSourcesModuleStore::empty(),
            crate::plugins::tracks::TracksModuleStore::empty(),
            crate::plugins::users::UsersModuleStore::empty(),
            Vec::new(),
        )
    }

    #[cfg(test)]
    pub(crate) fn discover_from_plugins_dir_with_db(
        plugins_dir: impl Into<PathBuf>,
        server_info: crate::plugins::server::ServerInfo,
        db: crate::plugins::db::DbAsync,
    ) -> Result<(Self, Vec<PluginLoadError>)> {
        Self::discover_from_plugins_dir_with_db_and_modules(
            plugins_dir,
            server_info,
            db,
            Vec::new(),
        )
    }

    pub(crate) fn discover_from_plugins_dir_with_db_and_modules(
        plugins_dir: impl Into<PathBuf>,
        server_info: crate::plugins::server::ServerInfo,
        db: crate::plugins::db::DbAsync,
        module_overrides: Vec<ModuleSpec>,
    ) -> Result<(Self, Vec<PluginLoadError>)> {
        Self::discover_from_plugins_dir_with_ids_store(
            plugins_dir,
            server_info,
            crate::plugins::artists::ArtistsModuleStore::with_db(db.clone()),
            crate::plugins::chromaprint::ChromaprintModuleStore::with_db(db.clone()),
            crate::plugins::covers::CoversModuleStore::with_db(db.clone()),
            crate::plugins::ids::IdsLookupModuleStore::with_db(db.clone()),
            crate::plugins::tags::TagsModuleStore::with_db(db.clone()),
            crate::plugins::datastore::DataStoreModuleStore::with_db(db.clone()),
            crate::plugins::entities::EntitiesModuleStore::with_db(db.clone()),
            crate::plugins::entries::EntriesModuleStore::with_db(db.clone()),
            crate::plugins::favorites::FavoritesModuleStore::with_db(db.clone()),
            crate::plugins::genres::GenresModuleStore::with_db(db.clone()),
            crate::plugins::libraries::LibrariesModuleStore::with_db(db.clone()),
            crate::plugins::listens::ListensModuleStore::with_db(db.clone()),
            crate::plugins::metadata::MetadataModuleStore::with_db(db.clone()),
            crate::plugins::playback_sessions::PlaybackSessionsModuleStore::with_db(db.clone()),
            crate::plugins::playback_sources::PlaybackSourcesModuleStore::with_db(db.clone()),
            crate::plugins::playlists::PlaylistsModuleStore::with_db(db.clone()),
            crate::plugins::releases::ReleasesModuleStore::with_db(db.clone()),
            crate::plugins::runtime::PluginSettingsModuleStore::with_db(db.clone()),
            crate::plugins::track_sources::TrackSourcesModuleStore::with_db(db.clone()),
            crate::plugins::tracks::TracksModuleStore::with_db(db.clone()),
            crate::plugins::users::UsersModuleStore::with_db(db),
            module_overrides,
        )
    }

    fn discover_from_plugins_dir_with_ids_store(
        plugins_dir: impl Into<PathBuf>,
        server_info: crate::plugins::server::ServerInfo,
        artists_store: crate::plugins::artists::ArtistsModuleStore,
        chromaprint_store: crate::plugins::chromaprint::ChromaprintModuleStore,
        covers_store: crate::plugins::covers::CoversModuleStore,
        ids_lookup_store: crate::plugins::ids::IdsLookupModuleStore,
        tags_store: crate::plugins::tags::TagsModuleStore,
        datastore_store: crate::plugins::datastore::DataStoreModuleStore,
        entities_store: crate::plugins::entities::EntitiesModuleStore,
        entries_store: crate::plugins::entries::EntriesModuleStore,
        favorites_store: crate::plugins::favorites::FavoritesModuleStore,
        genres_store: crate::plugins::genres::GenresModuleStore,
        libraries_store: crate::plugins::libraries::LibrariesModuleStore,
        listens_store: crate::plugins::listens::ListensModuleStore,
        metadata_store: crate::plugins::metadata::MetadataModuleStore,
        playback_sessions_store: crate::plugins::playback_sessions::PlaybackSessionsModuleStore,
        playback_sources_store: crate::plugins::playback_sources::PlaybackSourcesModuleStore,
        playlists_store: crate::plugins::playlists::PlaylistsModuleStore,
        releases_store: crate::plugins::releases::ReleasesModuleStore,
        settings_store: crate::plugins::runtime::PluginSettingsModuleStore,
        track_sources_store: crate::plugins::track_sources::TrackSourcesModuleStore,
        tracks_store: crate::plugins::tracks::TracksModuleStore,
        users_store: crate::plugins::users::UsersModuleStore,
        module_overrides: Vec<ModuleSpec>,
    ) -> Result<(Self, Vec<PluginLoadError>)> {
        let plugins_dir = plugins_dir.into();
        let mut plugin_manager = PluginManager::new(plugins_dir.clone());
        let errors = plugin_manager.discover_plugins(&plugin_scope_ids())?;
        let plugins = Arc::<[LoadedPlugin]>::from(plugin_manager.topological_order());
        let manifests = Arc::<[PluginManifest]>::from(
            plugins
                .iter()
                .map(|plugin| plugin.manifest.clone())
                .collect::<Vec<_>>(),
        );
        let runtime = Self::with_loader_and_plugins(
            manifests,
            server_info,
            FilesystemSourceLoader::new("/", plugins_dir),
            plugins,
            artists_store,
            chromaprint_store,
            covers_store,
            ids_lookup_store,
            tags_store,
            datastore_store,
            entities_store,
            entries_store,
            favorites_store,
            genres_store,
            libraries_store,
            listens_store,
            metadata_store,
            playback_sessions_store,
            playback_sources_store,
            playlists_store,
            releases_store,
            settings_store,
            track_sources_store,
            tracks_store,
            users_store,
            module_overrides,
        )?;
        Ok((runtime, errors))
    }

    fn with_loader<L>(
        manifests: Arc<[harmony_core::PluginManifest]>,
        server_info: crate::plugins::server::ServerInfo,
        artists_store: crate::plugins::artists::ArtistsModuleStore,
        chromaprint_store: crate::plugins::chromaprint::ChromaprintModuleStore,
        covers_store: crate::plugins::covers::CoversModuleStore,
        ids_lookup_store: crate::plugins::ids::IdsLookupModuleStore,
        tags_store: crate::plugins::tags::TagsModuleStore,
        datastore_store: crate::plugins::datastore::DataStoreModuleStore,
        entities_store: crate::plugins::entities::EntitiesModuleStore,
        entries_store: crate::plugins::entries::EntriesModuleStore,
        favorites_store: crate::plugins::favorites::FavoritesModuleStore,
        genres_store: crate::plugins::genres::GenresModuleStore,
        libraries_store: crate::plugins::libraries::LibrariesModuleStore,
        listens_store: crate::plugins::listens::ListensModuleStore,
        metadata_store: crate::plugins::metadata::MetadataModuleStore,
        playback_sessions_store: crate::plugins::playback_sessions::PlaybackSessionsModuleStore,
        playback_sources_store: crate::plugins::playback_sources::PlaybackSourcesModuleStore,
        playlists_store: crate::plugins::playlists::PlaylistsModuleStore,
        releases_store: crate::plugins::releases::ReleasesModuleStore,
        settings_store: crate::plugins::runtime::PluginSettingsModuleStore,
        track_sources_store: crate::plugins::track_sources::TrackSourcesModuleStore,
        tracks_store: crate::plugins::tracks::TracksModuleStore,
        users_store: crate::plugins::users::UsersModuleStore,
        loader: L,
    ) -> Result<Self>
    where
        L: SourceLoader + 'static,
    {
        Self::with_loader_and_plugins(
            manifests,
            server_info,
            loader,
            Arc::from(Vec::<LoadedPlugin>::new()),
            artists_store,
            chromaprint_store,
            covers_store,
            ids_lookup_store,
            tags_store,
            datastore_store,
            entities_store,
            entries_store,
            favorites_store,
            genres_store,
            libraries_store,
            listens_store,
            metadata_store,
            playback_sessions_store,
            playback_sources_store,
            playlists_store,
            releases_store,
            settings_store,
            track_sources_store,
            tracks_store,
            users_store,
            Vec::new(),
        )
    }

    fn with_loader_and_plugins<L>(
        manifests: Arc<[harmony_core::PluginManifest]>,
        server_info: crate::plugins::server::ServerInfo,
        loader: L,
        plugins: Arc<[LoadedPlugin]>,
        artists_store: crate::plugins::artists::ArtistsModuleStore,
        chromaprint_store: crate::plugins::chromaprint::ChromaprintModuleStore,
        covers_store: crate::plugins::covers::CoversModuleStore,
        ids_lookup_store: crate::plugins::ids::IdsLookupModuleStore,
        tags_store: crate::plugins::tags::TagsModuleStore,
        datastore_store: crate::plugins::datastore::DataStoreModuleStore,
        entities_store: crate::plugins::entities::EntitiesModuleStore,
        entries_store: crate::plugins::entries::EntriesModuleStore,
        favorites_store: crate::plugins::favorites::FavoritesModuleStore,
        genres_store: crate::plugins::genres::GenresModuleStore,
        libraries_store: crate::plugins::libraries::LibrariesModuleStore,
        listens_store: crate::plugins::listens::ListensModuleStore,
        metadata_store: crate::plugins::metadata::MetadataModuleStore,
        playback_sessions_store: crate::plugins::playback_sessions::PlaybackSessionsModuleStore,
        playback_sources_store: crate::plugins::playback_sources::PlaybackSourcesModuleStore,
        playlists_store: crate::plugins::playlists::PlaylistsModuleStore,
        releases_store: crate::plugins::releases::ReleasesModuleStore,
        settings_store: crate::plugins::runtime::PluginSettingsModuleStore,
        track_sources_store: crate::plugins::track_sources::TrackSourcesModuleStore,
        tracks_store: crate::plugins::tracks::TracksModuleStore,
        users_store: crate::plugins::users::UsersModuleStore,
        module_overrides: Vec<ModuleSpec>,
    ) -> Result<Self>
    where
        L: SourceLoader + 'static,
    {
        let vm = luau::Vm::new()?;
        vm.open_standard_libraries(luau::StandardLibraries::all_supported())?;
        vm.data().insert(LocalScheduler::new())?;
        vm.data()
            .insert(crate::plugins::runtime::PluginManifestModuleStore::new(
                manifests.clone(),
            ))?;
        vm.data()
            .insert(crate::plugins::server::ServerInfoModuleStore::new(
                server_info,
            ))?;
        vm.data()
            .insert(crate::plugins::auth::AuthCapabilitiesModuleStore::new(
                default_auth_capabilities(),
            ))?;
        vm.data().insert(artists_store)?;
        vm.data().insert(chromaprint_store)?;
        vm.data().insert(covers_store)?;
        vm.data().insert(ids_lookup_store)?;
        vm.data().insert(tags_store)?;
        vm.data().insert(datastore_store)?;
        vm.data().insert(entities_store)?;
        vm.data().insert(entries_store)?;
        vm.data().insert(favorites_store)?;
        vm.data().insert(genres_store)?;
        vm.data().insert(libraries_store)?;
        vm.data().insert(listens_store)?;
        vm.data().insert(metadata_store)?;
        vm.data()
            .insert(crate::plugins::metadata::MetadataCallbackRegistry::new())?;
        vm.data()
            .insert(crate::plugins::mix::MixCallbackRegistry::new())?;
        vm.data().insert(playback_sessions_store)?;
        vm.data()
            .insert(crate::plugins::playback_sessions::PlaybackUpdateCallbackStore::new())?;
        vm.data().insert(playback_sources_store)?;
        vm.data().insert(playlists_store)?;
        vm.data().insert(releases_store)?;
        vm.data().insert(settings_store)?;
        vm.data().insert(track_sources_store)?;
        vm.data().insert(tracks_store)?;
        vm.data().insert(users_store)?;

        let require = LuauRequireRuntime::new(
            loader,
            ManifestCapabilityPolicy::from_manifests(manifests.clone()),
        );
        register_generic_modules(&require, module_overrides)?;
        vm.data().insert(require)?;
        vm.data()
            .insert(crate::plugins::api::ApiRouteStore::new())?;

        for globals in harmony_globals::plugin_log_global_specs() {
            install_luau_globals(&vm, &ChunkOrigin::default(), &globals)?;
        }

        let tokio_runtime = ExecutorTokioRuntime::new()?;

        Ok(Self {
            vm,
            plugins,
            tokio_runtime,
            websocket_tasks: RefCell::new(HashMap::new()),
        })
    }

    pub(crate) fn plugin_manifests(&self) -> Vec<PluginManifest> {
        let mut manifests = self
            .plugins
            .iter()
            .map(|plugin| plugin.manifest.clone())
            .collect::<Vec<_>>();
        manifests.sort_by(|left, right| left.id.cmp(&right.id));
        manifests
    }

    pub(crate) fn has_plugin(&self, plugin_id: &str) -> bool {
        self.plugins
            .iter()
            .any(|plugin| plugin.manifest.id == plugin_id)
    }

    pub(crate) fn exec_plugin(&self, plugin_id: &str) -> Result<()> {
        let plugin = self
            .plugins
            .iter()
            .find(|plugin| plugin.manifest.id == plugin_id)
            .ok_or_else(|| anyhow::anyhow!("plugin not found: {plugin_id}"))?;
        self.exec_loaded_plugin(plugin)
    }

    pub(crate) fn exec_all(&self) -> Result<()> {
        for plugin in self.plugins.iter() {
            match self.exec_loaded_plugin(plugin) {
                Ok(()) => tracing::debug!("plugin '{}' executed", plugin.manifest.id),
                Err(error) => tracing::warn!("plugin '{}' error: {error}", plugin.manifest.id),
            }
        }
        Ok(())
    }

    fn exec_loaded_plugin(&self, plugin: &LoadedPlugin) -> Result<()> {
        let bytes = fs::read(&plugin.entrypoint_path).with_context(|| {
            format!(
                "load plugin '{}' entrypoint from {}",
                plugin.manifest.id,
                plugin.entrypoint_path.display()
            )
        })?;
        self.run_plugin_source(
            plugin.manifest.id.as_str(),
            plugin.manifest.entrypoint.as_str(),
            bytes,
        )
    }

    #[cfg(test)]
    pub(crate) fn eval_plugin_source(
        &self,
        plugin_id: impl Into<Arc<str>>,
        path: impl Into<Arc<str>>,
        source: impl Into<Arc<[u8]>>,
    ) -> Result<Vec<luau::Value>> {
        let origin = plugin_origin(plugin_id, path);
        self.eval_plugin_source_with_call_context(
            source,
            CallContext {
                origin,
                ..CallContext::default()
            },
        )
    }

    pub(crate) fn run_plugin_source(
        &self,
        plugin_id: impl Into<Arc<str>>,
        path: impl Into<Arc<str>>,
        source: impl Into<Arc<[u8]>>,
    ) -> Result<()> {
        let origin = plugin_origin(plugin_id, path);
        self.run_plugin_source_with_call_context(
            source,
            CallContext {
                origin,
                ..CallContext::default()
            },
        )
    }

    fn run_plugin_source_with_call_context(
        &self,
        source: impl Into<Arc<[u8]>>,
        context: CallContext,
    ) -> Result<()> {
        self.eval_plugin_source_with_call_context(source, context)
            .map(|_| ())
    }

    fn eval_plugin_source_with_call_context(
        &self,
        source: impl Into<Arc<[u8]>>,
        context: CallContext,
    ) -> Result<Vec<luau::Value>> {
        let origin = context.origin.clone();
        install_luau_require(&self.vm, &origin)?;

        let function = self
            .vm
            .load_chunk(&luau::Chunk::new(source, luau_origin(&origin)))?;
        let thread = self.vm.create_thread(&function)?;
        let scheduler = self.vm.data().get::<LocalScheduler>()?;
        scheduler.spawn_luau_thread(context, self.vm.clone(), thread.clone(), Vec::new());
        drive_luau_thread(&self.tokio_runtime, &scheduler, &thread)
    }

    pub(crate) fn dispatch_playback_update(
        &self,
        payload: crate::services::playback_sessions::PlaybackUpdatePayload,
    ) -> Result<()> {
        let callbacks = self
            .vm
            .data()
            .get::<crate::plugins::playback_sessions::PlaybackUpdateCallbackStore>()?;
        let handlers = callbacks.snapshot();
        if handlers.is_empty() {
            return Ok(());
        }

        let payload_value = harmony_json::json_to_luau_owned(serde_json::to_value(&payload)?, 0)?;
        let scheduler = self.vm.data().get::<LocalScheduler>()?;
        for handler in handlers {
            let thread = self.vm.create_thread(&handler.function)?;
            scheduler.spawn_luau_thread(
                handler.context.clone(),
                self.vm.clone(),
                thread.clone(),
                vec![payload_value.clone()],
            );
            if let Err(error) = drive_luau_thread(&self.tokio_runtime, &scheduler, &thread) {
                tracing::warn!(
                    playback_session_public_id = %payload.playback_session_public_id,
                    event = %payload.event,
                    plugin_id = %handler.plugin_id,
                    error = %error,
                    "playback on_update callback failed"
                );
            }
        }
        Ok(())
    }

    pub(crate) fn dispatch_mix_handler(
        &self,
        request: MixHandlerRequest,
    ) -> Result<MixHandlerResult> {
        let handlers = self
            .vm
            .data()
            .get::<crate::plugins::mix::MixCallbackRegistry>()?;
        let handler = handlers
            .get(request.handler_id)
            .ok_or_else(|| anyhow::anyhow!("mix handler {} not found", request.handler_id))?;
        let ctx = mix_context_value(&request)?;
        let thread = self.vm.create_thread(&handler.function)?;
        let scheduler = self.vm.data().get::<LocalScheduler>()?;
        scheduler.spawn_luau_thread(
            handler.context.clone(),
            self.vm.clone(),
            thread.clone(),
            vec![ctx],
        );
        let values = drive_luau_thread(&self.tokio_runtime, &scheduler, &thread)?;
        parse_mix_result(&self.vm, &handler.mixer_id, values)
    }

    pub(crate) fn dispatch_metadata_refresh(
        &self,
        request: MetadataRefreshRequest,
    ) -> Result<MetadataRefreshResult> {
        let handlers = self
            .vm
            .data()
            .get::<crate::plugins::metadata::MetadataCallbackRegistry>()?;
        let handler = handlers
            .get(request.handler_id)
            .ok_or_else(|| anyhow::anyhow!("metadata handler {} not found", request.handler_id))?;
        let ctx = harmony_json::json_to_luau_owned(request.context, 0)?;
        let thread = self.vm.create_thread(&handler.function)?;
        let scheduler = self.vm.data().get::<LocalScheduler>()?;
        scheduler.spawn_luau_thread(
            handler.context.clone(),
            self.vm.clone(),
            thread.clone(),
            vec![ctx],
        );
        let values = drive_luau_thread(&self.tokio_runtime, &scheduler, &thread)?
            .iter()
            .map(|value| harmony_json::luau_to_json(&self.vm, value, 0).map_err(anyhow::Error::new))
            .collect::<Result<Vec<_>>>()?;
        Ok(MetadataRefreshResult { values })
    }

    pub(crate) fn dispatch_api_handler(
        &self,
        request: ApiHandlerRequest,
    ) -> Result<ApiHandlerResponse> {
        let routes = self.vm.data().get::<crate::plugins::api::ApiRouteStore>()?;
        let handler = routes
            .get(request.handler_id)
            .ok_or_else(|| anyhow::anyhow!("API handler {} not found", request.handler_id))?;
        let ctx = api_context_value(&request)?;
        let thread = self.vm.create_thread(&handler.handler)?;
        let mut context = handler.context.clone();
        if let Some(auth) = request.auth.as_ref() {
            context.caller.insert(auth.principal.clone());
        }
        let scheduler = self.vm.data().get::<LocalScheduler>()?;
        scheduler.spawn_luau_thread(context, self.vm.clone(), thread.clone(), vec![ctx]);
        let values = drive_luau_thread(&self.tokio_runtime, &scheduler, &thread)?;
        parse_api_response(&self.vm, &request, values)
    }

    pub(crate) fn start_websocket(&self, request: WebSocketStartRequest) -> Result<()> {
        let routes = self.vm.data().get::<crate::plugins::api::ApiRouteStore>()?;
        let handler = routes
            .get(request.handler_id)
            .ok_or_else(|| anyhow::anyhow!("websocket handler {} not found", request.handler_id))?;
        let reader =
            websocket_reader_value(&handler.context, request.inbound, request.state.clone());
        let sender =
            websocket_sender_value(&handler.context, request.outbound, request.state.clone());
        let auth_principal = request.auth.as_ref().map(|auth| auth.principal.clone());
        let ctx = api_context_value(&ApiHandlerRequest {
            handler_id: request.handler_id,
            plugin_id: request.plugin_id,
            method: request.method,
            path: request.path,
            headers: request.headers,
            query: request.query,
            params: request.params,
            body: Vec::new(),
            auth: request.auth,
        })?;
        let thread = self.vm.create_thread(&handler.handler)?;
        let mut context = handler.context.clone();
        if let Some(principal) = auth_principal {
            context.caller.insert(principal);
        }
        let scheduler = self.vm.data().get::<LocalScheduler>()?;
        let handle = scheduler.spawn_luau_thread(
            context,
            self.vm.clone(),
            thread,
            vec![reader, sender, ctx],
        );
        self.websocket_tasks
            .borrow_mut()
            .insert(TaskIdKey(handle.id().0), request.state);
        Ok(())
    }

    fn poll_background_tasks(&self) {
        let Ok(scheduler) = self.vm.data().get::<LocalScheduler>() else {
            return;
        };
        {
            let _guard = self.tokio_runtime.enter();
            scheduler.poll_ready();
        }
        let snapshots = scheduler.snapshots();
        let mut completed_websockets = Vec::new();
        for snapshot in snapshots {
            if snapshot.state != TaskState::Pending {
                let key = TaskIdKey(snapshot.id.0);
                if let Some(state) = self.websocket_tasks.borrow().get(&key) {
                    state.request_close();
                    completed_websockets.push(key);
                }
            }
        }
        scheduler.remove_finished();
        if !completed_websockets.is_empty() {
            let mut tasks = self.websocket_tasks.borrow_mut();
            for key in completed_websockets {
                tasks.remove(&key);
            }
        }
    }

    fn next_scheduler_delay(&self) -> Option<Duration> {
        let scheduler = self.vm.data().get::<LocalScheduler>().ok()?;
        if !scheduler.has_pending() {
            return None;
        }
        scheduler.next_wake_delay()
    }
}

fn register_generic_modules(
    require: &LuauRequireRuntime,
    module_overrides: Vec<ModuleSpec>,
) -> Result<()> {
    let mut specs = plugin_module_specs();
    for override_spec in module_overrides {
        if let Some(existing) = specs.iter_mut().find(|spec| spec.id == override_spec.id) {
            *existing = override_spec;
        } else {
            specs.push(override_spec);
        }
    }

    for spec in specs {
        require.register(spec)?;
    }
    Ok(())
}

fn plugin_module_specs() -> Vec<ModuleSpec> {
    vec![
        crate::plugins::api::module_spec(),
        crate::plugins::artists::module_spec(),
        crate::plugins::auth::module_spec(),
        crate::plugins::chromaprint::module_spec(),
        crate::plugins::covers::module_spec(),
        harmony_crypt::module_spec(),
        crate::plugins::datastore::module_spec(),
        crate::plugins::entities::module_spec(),
        crate::plugins::entries::module_spec(),
        crate::plugins::favorites::module_spec(),
        crate::plugins::genres::module_spec(),
        harmony_http::module_spec(),
        crate::plugins::ids::module_spec(),
        crate::plugins::images::module_spec(),
        crate::plugins::labels::module_spec(),
        crate::plugins::libraries::module_spec(),
        crate::plugins::listens::module_spec(),
        harmony_json::module_spec(),
        crate::plugins::lyrics::module_spec(),
        crate::plugins::metadata::module_spec(),
        crate::plugins::mix::module_spec(),
        harmony_net::module_spec(),
        crate::plugins::playback_sessions::module_spec(),
        crate::plugins::playback_sources::module_spec(),
        crate::plugins::playlists::module_spec(),
        crate::plugins::releases::module_spec(),
        crate::plugins::server::module_spec(),
        harmony_task::module_spec(),
        crate::plugins::tags::module_spec(),
        crate::plugins::track_sources::module_spec(),
        crate::plugins::tracks::module_spec(),
        crate::plugins::users::module_spec(),
        crate::plugins::runtime::module_spec(),
    ]
}

fn plugin_scope_ids() -> HashSet<Arc<str>> {
    plugin_module_specs()
        .into_iter()
        .filter_map(|spec| spec.capability.map(|capability| capability.0))
        .collect()
}

pub(crate) fn plugin_scope_ids_for_test() -> Vec<String> {
    let mut scopes = plugin_scope_ids()
        .into_iter()
        .map(|scope| scope.to_string())
        .collect::<Vec<_>>();
    scopes.sort();
    scopes
}

fn default_auth_capabilities() -> crate::plugins::auth::AuthCapabilities {
    crate::plugins::auth::AuthCapabilities {
        enabled: false,
        allow_default_login_when_disabled: true,
        default_username: "default".to_string(),
    }
}

#[cfg(test)]
fn default_server_info() -> crate::plugins::server::ServerInfo {
    crate::plugins::server::ServerInfo {
        id: "raw-runtime".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        commit_hash: env!("LYRA_GIT_HASH").to_string(),
        hostname: "localhost".to_string(),
        port: 0,
        published_url: None,
        setup_complete: false,
    }
}

fn plugin_origin(plugin_id: impl Into<Arc<str>>, path: impl Into<Arc<str>>) -> ChunkOrigin {
    let plugin = plugin_id.into();
    let path = path.into();
    ChunkOrigin {
        module: Some(ModuleId(Arc::from(format!("plugins/{plugin}/{path}")))),
        plugin: Some(plugin.clone()),
        path: Some(Arc::from(format!("plugins/{plugin}/{path}"))),
    }
}

fn luau_origin(origin: &ChunkOrigin) -> luau::ChunkOrigin {
    luau::ChunkOrigin {
        module: origin
            .module
            .as_ref()
            .map(|module| luau::ModuleId(module.0.clone())),
        plugin: origin.plugin.clone(),
        path: origin.path.clone(),
    }
}

fn drive_luau_thread(
    tokio_runtime: &ExecutorTokioRuntime,
    scheduler: &LocalScheduler,
    thread: &luau::Thread,
) -> Result<Vec<luau::Value>> {
    // HTTP-bound providers burn iteration count via incoming wakes faster
    // than wall-clock time, so an iter cap fires before the work completes.
    const DRIVE_LUAU_THREAD_BUDGET: Duration = Duration::from_secs(300);
    let deadline = std::time::Instant::now() + DRIVE_LUAU_THREAD_BUDGET;
    loop {
        {
            let _guard = tokio_runtime.enter();
            scheduler.poll_ready();
        }
        let Some(handle) = scheduler.luau_thread_handle(thread) else {
            return Ok(Vec::new());
        };
        if let Some(snapshot) = scheduler.snapshot(handle.id()) {
            match snapshot.state {
                TaskState::Completed => {
                    let output = scheduler
                        .take_luau_thread_output(thread)
                        .unwrap_or_default();
                    scheduler.remove_finished();
                    return Ok(output);
                }
                TaskState::Failed => {
                    let error = snapshot.error.as_deref().unwrap_or("unknown error");
                    scheduler.remove_finished();
                    bail!("plugin executor task {} failed: {error}", snapshot.id.0);
                }
                TaskState::Cancelled => {
                    scheduler.remove_finished();
                    bail!("plugin executor task {} was cancelled", snapshot.id.0);
                }
                TaskState::Pending => {}
            }
        } else {
            return Ok(Vec::new());
        }
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        let wait = scheduler
            .next_wake_delay()
            .unwrap_or_else(|| Duration::from_millis(1))
            .min(Duration::from_millis(25))
            .min(remaining);
        scheduler.wait_for_wake(Some(wait));
    }

    bail!(
        "plugin executor thread {}:{} did not complete",
        thread.vm_id(),
        thread.state_id()
    );
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
