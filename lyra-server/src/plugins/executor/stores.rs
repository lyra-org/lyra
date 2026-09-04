// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use anyhow::Result;
use harmony_luau as luau;

pub(super) struct PluginModuleStores {
    pub(super) server_settings: Option<crate::SettingsHandle>,
    artists: crate::plugins::artists::ArtistsModuleStore,
    chromaprint: crate::plugins::chromaprint::ChromaprintModuleStore,
    covers: crate::plugins::covers::CoversModuleStore,
    ids_lookup: crate::plugins::ids::IdsLookupModuleStore,
    tags: crate::plugins::tags::TagsModuleStore,
    datastore: crate::plugins::datastore::DataStoreModuleStore,
    entities: crate::plugins::entities::EntitiesModuleStore,
    entries: crate::plugins::entries::EntriesModuleStore,
    favorites: crate::plugins::favorites::FavoritesModuleStore,
    genres: crate::plugins::genres::GenresModuleStore,
    libraries: crate::plugins::libraries::LibrariesModuleStore,
    listens: crate::plugins::listens::ListensModuleStore,
    metadata: crate::plugins::metadata::MetadataModuleStore,
    playback_sessions: crate::plugins::playback_sessions::PlaybackSessionsModuleStore,
    playback_sources: crate::plugins::playback_sources::PlaybackSourcesModuleStore,
    playlists: crate::plugins::playlists::PlaylistsModuleStore,
    releases: crate::plugins::releases::ReleasesModuleStore,
    settings: crate::plugins::settings::PluginSettingsModuleStore,
    track_sources: crate::plugins::track_sources::TrackSourcesModuleStore,
    tracks: crate::plugins::tracks::TracksModuleStore,
    users: crate::plugins::users::UsersModuleStore,
}

impl PluginModuleStores {
    pub(super) fn empty() -> Self {
        Self {
            server_settings: None,
            artists: crate::plugins::artists::ArtistsModuleStore::empty(),
            chromaprint: crate::plugins::chromaprint::ChromaprintModuleStore::empty(),
            covers: crate::plugins::covers::CoversModuleStore::empty(),
            ids_lookup: crate::plugins::ids::IdsLookupModuleStore::empty(),
            tags: crate::plugins::tags::TagsModuleStore::empty(),
            datastore: crate::plugins::datastore::DataStoreModuleStore::empty(),
            entities: crate::plugins::entities::EntitiesModuleStore::empty(),
            entries: crate::plugins::entries::EntriesModuleStore::empty(),
            favorites: crate::plugins::favorites::FavoritesModuleStore::empty(),
            genres: crate::plugins::genres::GenresModuleStore::empty(),
            libraries: crate::plugins::libraries::LibrariesModuleStore::empty(),
            listens: crate::plugins::listens::ListensModuleStore::empty(),
            metadata: crate::plugins::metadata::MetadataModuleStore::empty(),
            playback_sessions:
                crate::plugins::playback_sessions::PlaybackSessionsModuleStore::empty(),
            playback_sources: crate::plugins::playback_sources::PlaybackSourcesModuleStore::empty(),
            playlists: crate::plugins::playlists::PlaylistsModuleStore::empty(),
            releases: crate::plugins::releases::ReleasesModuleStore::empty(),
            settings: crate::plugins::settings::PluginSettingsModuleStore::empty(),
            track_sources: crate::plugins::track_sources::TrackSourcesModuleStore::empty(),
            tracks: crate::plugins::tracks::TracksModuleStore::empty(),
            users: crate::plugins::users::UsersModuleStore::empty(),
        }
    }

    pub(super) fn with_db(db: crate::plugins::db::DbAsync) -> Self {
        Self {
            server_settings: None,
            artists: crate::plugins::artists::ArtistsModuleStore::with_db(db.clone()),
            chromaprint: crate::plugins::chromaprint::ChromaprintModuleStore::with_db(db.clone()),
            covers: crate::plugins::covers::CoversModuleStore::with_db(db.clone()),
            ids_lookup: crate::plugins::ids::IdsLookupModuleStore::with_db(db.clone()),
            tags: crate::plugins::tags::TagsModuleStore::with_db(db.clone()),
            datastore: crate::plugins::datastore::DataStoreModuleStore::with_db(db.clone()),
            entities: crate::plugins::entities::EntitiesModuleStore::with_db(db.clone()),
            entries: crate::plugins::entries::EntriesModuleStore::with_db(db.clone()),
            favorites: crate::plugins::favorites::FavoritesModuleStore::with_db(db.clone()),
            genres: crate::plugins::genres::GenresModuleStore::with_db(db.clone()),
            libraries: crate::plugins::libraries::LibrariesModuleStore::with_db(db.clone()),
            listens: crate::plugins::listens::ListensModuleStore::with_db(db.clone()),
            metadata: crate::plugins::metadata::MetadataModuleStore::with_db(db.clone()),
            playback_sessions:
                crate::plugins::playback_sessions::PlaybackSessionsModuleStore::with_db(db.clone()),
            playback_sources: crate::plugins::playback_sources::PlaybackSourcesModuleStore::with_db(
                db.clone(),
            ),
            playlists: crate::plugins::playlists::PlaylistsModuleStore::with_db(db.clone()),
            releases: crate::plugins::releases::ReleasesModuleStore::with_db(db.clone()),
            settings: crate::plugins::settings::PluginSettingsModuleStore::with_db(db.clone()),
            track_sources: crate::plugins::track_sources::TrackSourcesModuleStore::with_db(
                db.clone(),
            ),
            tracks: crate::plugins::tracks::TracksModuleStore::with_db(db.clone()),
            users: crate::plugins::users::UsersModuleStore::with_db(db),
        }
    }

    pub(super) fn install_into(self, vm: &luau::Vm) -> Result<()> {
        vm.data().insert(self.artists)?;
        vm.data().insert(self.chromaprint)?;
        vm.data().insert(self.covers)?;
        vm.data().insert(self.ids_lookup)?;
        vm.data().insert(self.tags)?;
        vm.data().insert(self.datastore)?;
        vm.data().insert(self.entities)?;
        vm.data().insert(self.entries)?;
        vm.data().insert(self.favorites)?;
        vm.data().insert(self.genres)?;
        vm.data().insert(self.libraries)?;
        vm.data().insert(self.listens)?;
        vm.data().insert(self.metadata)?;
        vm.data().insert(self.playback_sessions)?;
        vm.data().insert(self.playback_sources)?;
        vm.data().insert(self.playlists)?;
        vm.data().insert(self.releases)?;
        vm.data().insert(self.settings)?;
        vm.data().insert(self.track_sources)?;
        vm.data().insert(self.tracks)?;
        vm.data().insert(self.users)?;
        Ok(())
    }
}
