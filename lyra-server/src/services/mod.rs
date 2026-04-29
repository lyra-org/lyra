// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::fmt;

use schemars::JsonSchema;
use serde::Serialize;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, JsonSchema, Serialize)]
#[serde(rename_all = "lowercase")]
#[harmony_macros::enumeration]
pub(crate) enum EntityType {
    Release,
    Artist,
    Track,
}

harmony_macros::compile!(type_path = EntityType, variants = true);

impl EntityType {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Release => "release",
            Self::Artist => "artist",
            Self::Track => "track",
        }
    }
}

impl fmt::Display for EntityType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub(crate) mod artists;
pub(crate) mod auth;
pub(crate) mod cors;
pub(crate) mod covers;
pub(crate) mod entities;
pub(crate) mod entries;
pub(crate) mod favorites;
pub(crate) mod hls;
pub(crate) mod libraries;
pub(crate) mod metadata;
pub(crate) mod mix;
pub(crate) mod options;
pub(crate) mod origin;
pub(crate) mod playback_sessions;
pub(crate) mod playback_sources;
pub(crate) mod playlists;
pub(crate) mod plugin_settings;
pub(crate) mod providers;
pub(crate) mod releases;
pub(crate) mod remote;
pub(crate) mod startup;
pub(crate) mod storage_monitor;
pub(crate) mod tags;
pub(crate) mod tracks;

pub(crate) use libraries::{
    LibrarySyncState,
    StartLibrarySyncResult,
    get_library_sync_state,
    start_library_sync,
    wait_for_running_library_syncs,
};
pub(crate) use metadata::cleanup::deduplicate_artists_by_external_id;
pub(crate) use providers::{
    EntityRefreshMode,
    LibraryRefreshOptions,
    refresh_library_metadata,
    run_provider_sync,
};

pub(crate) use covers::providers::{
    NormalizedProviderArtistSearchResult,
    NormalizedProviderReleaseSearchResult,
    NormalizedProviderSearchResult,
    NormalizedProviderTrackSearchResult,
    ProviderSearchError,
    ProviderSearchRequest,
    search_provider,
};
pub(crate) use covers::{
    CoverPaths,
    CoverSyncOptions,
    clear_cover_search_cache,
    upsert_artist_cover_metadata,
    upsert_release_cover_metadata,
};
