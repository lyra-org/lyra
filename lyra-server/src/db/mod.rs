// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

pub(crate) mod api_keys;
pub(crate) mod artists;
pub(crate) mod bootstrap;
pub(crate) mod covers;
pub(crate) mod credits;
pub(crate) mod cue;
pub(crate) mod datastore;
pub(crate) mod entities;
pub(crate) mod entries;
pub(crate) mod favorites;
pub(crate) mod genres;
pub(crate) mod graph;
pub(crate) mod ids;
pub(crate) mod indexes;
pub(crate) mod labels;
pub(crate) mod libraries;
pub(crate) mod listens;
pub(crate) mod lookup;
pub(crate) mod lyrics;
pub(crate) mod metadata;
pub(crate) mod mixers;
pub(crate) mod playback_sessions;
pub(crate) mod playlists;
pub(crate) mod providers;
pub(crate) mod releases;
pub(crate) mod roles;
pub(crate) mod server;
pub(crate) mod settings;
pub(crate) mod tags;
#[cfg(test)]
pub(crate) mod test_db;
pub(crate) mod track_sources;
pub(crate) mod tracks;
pub(crate) mod users;

use std::{
    cmp::Ordering,
    collections::HashSet,
    sync::Arc,
};

use agdb::{
    DbAny,
    DbAnyTransactionMut,
    DbError,
    DbId,
    Query,
    QueryMut,
    QueryResult,
};
use tokio::sync::RwLock;

pub(crate) trait DbAccess {
    fn exec<T: Query>(&self, query: T) -> Result<QueryResult, DbError>;
    fn exec_mut<T: QueryMut>(&mut self, query: T) -> Result<QueryResult, DbError>;
}

impl DbAccess for DbAny {
    fn exec<T: Query>(&self, query: T) -> Result<QueryResult, DbError> {
        DbAny::exec(self, query)
    }

    fn exec_mut<T: QueryMut>(&mut self, query: T) -> Result<QueryResult, DbError> {
        DbAny::exec_mut(self, query)
    }
}

impl DbAccess for tokio::sync::RwLockReadGuard<'_, DbAny> {
    fn exec<T: Query>(&self, query: T) -> Result<QueryResult, DbError> {
        DbAny::exec(self, query)
    }

    fn exec_mut<T: QueryMut>(&mut self, query: T) -> Result<QueryResult, DbError> {
        _ = query;
        unreachable!("exec_mut called on read guard")
    }
}

impl DbAccess for tokio::sync::RwLockWriteGuard<'_, DbAny> {
    fn exec<T: Query>(&self, query: T) -> Result<QueryResult, DbError> {
        DbAny::exec(self, query)
    }

    fn exec_mut<T: QueryMut>(&mut self, query: T) -> Result<QueryResult, DbError> {
        DbAny::exec_mut(self, query)
    }
}

impl DbAccess for tokio::sync::OwnedRwLockReadGuard<DbAny> {
    fn exec<T: Query>(&self, query: T) -> Result<QueryResult, DbError> {
        DbAny::exec(self, query)
    }

    fn exec_mut<T: QueryMut>(&mut self, query: T) -> Result<QueryResult, DbError> {
        // read guards are only passed to read-only functions; this is unreachable
        _ = query;
        unreachable!("exec_mut called on read guard")
    }
}

impl DbAccess for tokio::sync::OwnedRwLockWriteGuard<DbAny> {
    fn exec<T: Query>(&self, query: T) -> Result<QueryResult, DbError> {
        DbAny::exec(self, query)
    }

    fn exec_mut<T: QueryMut>(&mut self, query: T) -> Result<QueryResult, DbError> {
        DbAny::exec_mut(self, query)
    }
}

impl DbAccess for DbAnyTransactionMut<'_> {
    fn exec<T: Query>(&self, query: T) -> Result<QueryResult, DbError> {
        DbAnyTransactionMut::exec(self, query)
    }

    fn exec_mut<T: QueryMut>(&mut self, query: T) -> Result<QueryResult, DbError> {
        DbAnyTransactionMut::exec_mut(self, query)
    }
}

pub(crate) use ids::{
    NodeId,
    ResolveId,
};

#[derive(Clone, Copy, Debug)]
pub(crate) enum SortDirection {
    Ascending,
    Descending,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum SortKey {
    SortName,
    Name,
    DateCreated,
    ReleaseDate,
    TrackNumber,
    DiscNumber,
    Duration,
    DbId,
}

impl SortKey {
    pub(crate) fn from_token(token: &str) -> Option<Self> {
        match token {
            "sortname" => Some(Self::SortName),
            "name" => Some(Self::Name),
            "datecreated" => Some(Self::DateCreated),
            "releasedate" => Some(Self::ReleaseDate),
            "track" => Some(Self::TrackNumber),
            "disc" => Some(Self::DiscNumber),
            "duration" => Some(Self::Duration),
            "id" => Some(Self::DbId),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SortSpec {
    pub key: SortKey,
    pub direction: SortDirection,
}

#[derive(Clone, Debug, thiserror::Error)]
pub(crate) enum SortSpecParseError {
    #[error("unsupported sort_order value: {0}")]
    UnsupportedSortOrder(String),
    #[error("unsupported sort_by value(s): {}", .0.join(", "))]
    UnsupportedSortByValues(Vec<String>),
}

#[derive(Clone, Debug)]
pub(crate) struct ListOptions {
    pub sort: Vec<SortSpec>,
    pub offset: Option<u64>,
    pub limit: Option<u64>,
    pub search_term: Option<String>,
}

pub(crate) struct PagedResult<T> {
    pub entries: Vec<T>,
    pub total_count: u64,
    pub offset: u64,
}

/// Deduplicate a slice of `DbId`s, discarding non-positive IDs and preserving
/// insertion order. Used by batch-fetch helpers in tracks, artists, and covers.
pub(crate) fn dedup_positive_ids(ids: &[DbId]) -> Vec<DbId> {
    let mut unique = Vec::new();
    let mut seen = HashSet::new();
    for id in ids {
        if id.0 > 0 && seen.insert(*id) {
            unique.push(*id);
        }
    }
    unique
}

/// Compare two `Option<T>` values with nil-last semantics:
/// `Some` values sort before `None`, matching the Lua behavior.
pub(crate) fn compare_option<T: Ord>(a: &Option<T>, b: &Option<T>) -> Ordering {
    match (a, b) {
        (Some(a), Some(b)) => a.cmp(b),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

/// Apply direction to an ordering.
pub(crate) fn apply_direction(ord: Ordering, direction: SortDirection) -> Ordering {
    match direction {
        SortDirection::Ascending => ord,
        SortDirection::Descending => ord.reverse(),
    }
}

/// Parse a sort direction from a raw string, returning an error for unrecognised
/// values when `strict` is `true`.
pub(crate) fn parse_sort_direction(
    raw: Option<String>,
    strict: bool,
) -> Result<SortDirection, SortSpecParseError> {
    match raw {
        None => Ok(SortDirection::Ascending),
        Some(raw) if raw.eq_ignore_ascii_case("ascending") => Ok(SortDirection::Ascending),
        Some(raw) if raw.eq_ignore_ascii_case("descending") => Ok(SortDirection::Descending),
        Some(raw) => {
            if strict {
                Err(SortSpecParseError::UnsupportedSortOrder(raw))
            } else {
                Ok(SortDirection::Ascending)
            }
        }
    }
}

pub(crate) fn parse_sort_specs_tokens<F>(
    sort_by: Option<Vec<String>>,
    direction: SortDirection,
    is_supported_key: F,
    strict: bool,
) -> Result<Vec<SortSpec>, SortSpecParseError>
where
    F: Fn(SortKey) -> bool,
{
    let mut sort = Vec::new();
    let mut unknown = Vec::new();

    if let Some(values) = sort_by {
        for value in values {
            for entry in value.split(',') {
                let entry = entry.trim();
                if entry.is_empty() {
                    continue;
                }

                let token = entry.to_ascii_lowercase();
                let Some(key) = SortKey::from_token(&token) else {
                    if strict {
                        unknown.push(token);
                    }
                    continue;
                };

                if is_supported_key(key) {
                    sort.push(SortSpec { key, direction });
                } else if strict {
                    unknown.push(token);
                }
            }
        }
    }

    if strict && !unknown.is_empty() {
        return Err(SortSpecParseError::UnsupportedSortByValues(unknown));
    }

    Ok(sort)
}

pub(crate) use artists::Artist;
pub(crate) use artists::ArtistType;
pub(crate) use artists::CreditedArtist;
pub(crate) use covers::Cover;
pub(crate) use credits::Credit;
pub(crate) use credits::CreditType;
pub(crate) use cue::{
    CueSheet,
    CueTrack,
};
pub(crate) use datastore::DataStore;
pub(crate) use entries::Entry;
pub(crate) use libraries::Library;
pub(crate) use listens::Listen;
pub(crate) use lyrics::Lyrics;
pub(crate) use metadata::layers::MetadataLayer;
pub(crate) use playback_sessions::{
    EvictedPlayback,
    PlaybackSession,
    PlaybackState,
};
pub(crate) use playlists::Playlist;
pub(crate) use providers::ProviderConfig;
pub(crate) use providers::external_ids;
pub(crate) use providers::external_ids::IdSource;
pub(crate) use releases::Release;
pub(crate) use releases::ReleaseType;
pub(crate) use roles::Permission;
pub(crate) use tags::Tag;
pub(crate) use track_sources::TrackSource;
pub(crate) use tracks::Track;
pub(crate) use users::{
    Session,
    User,
};

pub(crate) type DbAsync = Arc<RwLock<DbAny>>;
pub(crate) use bootstrap::create;

pub fn is_supported_extension(path: &std::path::Path) -> bool {
    entries::classify_file_kind(path).is_some()
}
