// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::fmt;

use serde::Serialize;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum EntityType {
    Release,
    Artist,
    Track,
}

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
pub(crate) mod boot;
pub(crate) mod catalog;
pub(crate) mod covers;
pub(crate) mod delivery;
pub(crate) mod entities;
pub(crate) mod entries;
pub(crate) mod favorites;
pub(crate) mod hls;
pub(crate) mod libraries;
pub(crate) mod listens;
pub(crate) mod maintenance;
pub(crate) mod metadata;
pub(crate) mod mix;
pub(crate) mod opaque_ids;
pub(crate) mod options;
pub(crate) mod pagination;
pub(crate) mod playback_sessions;
pub(crate) mod playback_sources;
pub(crate) mod playbacks;
pub(crate) mod playlists;
pub(crate) mod plugin_dispatch;
pub(crate) mod plugin_scope;
pub(crate) mod providers;
pub(crate) mod rate_limit;
pub(crate) mod ratings;
pub(crate) mod releases;
pub(crate) mod remote;
pub(crate) mod search;
pub(crate) mod settings;
pub(crate) mod setup;
pub(crate) mod shutdown;
pub(crate) mod state;
mod system;
pub(crate) mod tags;
pub(crate) mod testing;
pub(crate) mod timestamps;
pub(crate) mod tracks;
pub(crate) mod transcoding;

pub(crate) use state::STATE;
pub(crate) use system::SystemContext;

pub(crate) use libraries::{
    LibraryRefreshRunOptions,
    LibrarySyncStatus,
    SyncRunEvent,
    SyncRunStartResponse,
    SyncRunSummary,
    cancel_sync_run,
    get_sync_run,
    start_library_sync,
    subscribe_sync_run_events,
    sync_run_events_after,
    wait_for_running_library_syncs,
};
pub(crate) use metadata::cleanup::deduplicate_artists_by_external_id;
pub(crate) use providers::{
    EntityRefreshMode,
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
};
