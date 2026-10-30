// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Lyra database: the agdb schema and every query against it.
#![cfg_attr(all(test, feature = "nightly"), feature(test))]

pub mod api_keys;
pub mod artists;
pub mod bootstrap;
pub mod chains;
pub mod covers;
pub mod credits;
pub mod cue;
pub mod datastore;
pub mod entities;
pub mod entries;
pub mod favorites;
pub mod fixtures;
pub mod genres;
pub mod graph;
pub(crate) mod ids;
pub mod indexes;
pub mod labels;
pub mod libraries;
pub mod listens;
pub mod lookup;
pub mod lyrics;
pub mod metadata;
pub(crate) mod migration;
pub mod mixers;
pub mod playback_sessions;
pub mod playbacks;
pub mod playlists;
pub mod plugin_repositories;
pub mod process_lock;
pub mod providers;
pub mod ratings;
pub mod releases;
pub mod roles;
pub mod search;
pub mod server;
pub mod settings;
pub mod sync_runs;
pub mod tags;
#[cfg(any(test, feature = "testing"))]
pub mod test_db;
pub mod track_sources;
pub mod tracks;
pub mod users;

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
    DbType,
    DbValue,
    Query,
    QueryBuilder,
    QueryMut,
    QueryResult,
};
use tokio::sync::RwLock;

pub trait DbAccess {
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

/// Atomically clears absent optional fields before replacing a typed element.
pub(crate) fn replace_element_in_transaction<'a, T: DbType>(
    db: &mut DbAnyTransactionMut<'_>,
    db_id: DbId,
    optional_fields: impl IntoIterator<Item = (&'a str, bool)>,
    element: &T,
) -> anyhow::Result<()> {
    let absent_keys = optional_fields
        .into_iter()
        .filter(|(_, is_absent)| *is_absent)
        .map(|(key, _)| DbValue::from(key))
        .collect::<Vec<_>>();
    if !absent_keys.is_empty() {
        db.exec_mut(
            QueryBuilder::remove()
                .values(absent_keys)
                .ids(db_id)
                .query(),
        )?;
    }
    db.exec_mut(QueryBuilder::insert().element(element).query())?;
    Ok(())
}

/// Whether `error` reports a poisoned database, which only reopening recovers.
pub fn is_poisoned(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<DbError>()
            .is_some_and(|error| error.ty == agdb::DbErrorType::Poisoned)
    })
}

pub use ids::{
    NodeId,
    ResolveId,
};

/// Deduplicate a slice of `DbId`s, discarding non-positive IDs and preserving
/// insertion order. Used by batch-fetch helpers in tracks, artists, and covers.
pub fn dedup_positive_ids(ids: &[DbId]) -> Vec<DbId> {
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
pub fn compare_option<T: Ord>(a: &Option<T>, b: &Option<T>) -> Ordering {
    match (a, b) {
        (Some(a), Some(b)) => a.cmp(b),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

pub use artists::Artist;
pub use artists::ArtistType;
pub use artists::CreditedArtist;
pub use artists::relations::ArtistRelationType;
pub use covers::Cover;
pub use credits::Credit;
pub use credits::CreditType;
pub use cue::{
    CueSheet,
    CueTrack,
};
pub use datastore::DataStore;
pub use entries::Entry;
pub use libraries::Library;
pub use listens::Listen;
pub use lyrics::Lyrics;
pub use metadata::custom_fields::ProviderCustomFields;
pub use metadata::fields::MetadataField;
pub use metadata::layers::MetadataLayer;
pub use playback_sessions::{
    EvictedPlayback,
    PlaybackSession,
    PlaybackState,
};
pub use playlists::Playlist;
pub use providers::ProviderConfig;
pub use providers::external_ids::IdSource;
pub use releases::Release;
pub use roles::Permission;
pub use tags::Tag;
pub use track_sources::TrackSource;
pub use tracks::Track;
pub use users::{
    Session,
    User,
};

pub type DbAsync = Arc<RwLock<DbAny>>;
pub use bootstrap::{
    Created,
    create,
};
pub use providers::external_ids;

pub fn is_supported_extension(path: &std::path::Path) -> bool {
    entries::classify_file_kind(path).is_some()
}

#[cfg(test)]
mod tests {
    use agdb::{
        DbError,
        DbErrorType,
    };
    use anyhow::Context;

    use super::is_poisoned;

    #[test]
    fn is_poisoned_finds_poisoned_error_behind_context() {
        let error = Err::<(), _>(DbError::db(DbErrorType::Poisoned, "poisoned"))
            .context("recording listen")
            .unwrap_err();
        assert!(is_poisoned(&error));
    }

    #[test]
    fn is_poisoned_ignores_other_db_errors() {
        let error = anyhow::Error::from(DbError::db(DbErrorType::NotFound, "missing"));
        assert!(!is_poisoned(&error));
    }
}
