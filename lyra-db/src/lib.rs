// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Lyra database: the agdb schema and every query against it.
#![cfg_attr(all(test, feature = "nightly"), feature(test))]

pub mod api_keys;
pub mod artists;
pub mod bootstrap;
pub mod catalog;
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
    sync::{
        Arc,
        atomic::{
            AtomicU64,
            Ordering as AtomicOrdering,
        },
    },
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
use tokio::sync::{
    OwnedRwLockReadGuard,
    OwnedRwLockWriteGuard,
    RwLock,
    RwLockReadGuard,
    RwLockWriteGuard,
    TryLockError,
};

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

/// The shared database, with a stamp that tells readers whether what they derived from the
/// [`catalog`] is still current. A [`DbAsync::write`] guard makes the change count odd when it
/// first hands out mutable access and even again when it drops, so the count is even under every
/// read guard and differs after any write that may have changed the catalog, rolled back or not.
#[derive(Clone)]
pub struct DbAsync {
    db: Arc<RwLock<DbAny>>,
    changes: Arc<AtomicU64>,
    address: usize,
    serial: u64,
}

impl std::fmt::Debug for DbAsync {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbAsync")
            .field("serial", &self.serial)
            .field("changes", &self.changes())
            .finish_non_exhaustive()
    }
}

impl DbAsync {
    pub fn new(db: DbAny) -> Self {
        static SERIALS: AtomicU64 = AtomicU64::new(0);
        let db = Arc::new(RwLock::new(db));
        let address = db
            .try_read()
            .map(|db| std::ptr::from_ref::<DbAny>(&db) as usize)
            .expect("a new lock is free");
        Self {
            db,
            changes: Arc::new(AtomicU64::new(0)),
            address,
            serial: SERIALS.fetch_add(1, AtomicOrdering::Relaxed),
        }
    }

    pub async fn read(&self) -> RwLockReadGuard<'_, DbAny> {
        self.db.read().await
    }

    pub async fn read_owned(self) -> OwnedRwLockReadGuard<DbAny> {
        self.db.read_owned().await
    }

    pub fn try_read(&self) -> Result<RwLockReadGuard<'_, DbAny>, TryLockError> {
        self.db.try_read()
    }

    pub async fn write(&self) -> DbWriteGuard<'_> {
        DbWriteGuard {
            guard: self.db.write().await,
            changes: Some(&self.changes),
            touched: false,
        }
    }

    /// A write lock for changes that leave the [`catalog`] as it is, such as a user's playback
    /// progress, listens and session times; they do not count as changes.
    pub async fn write_outside_catalog(&self) -> DbWriteGuard<'_> {
        DbWriteGuard {
            guard: self.db.write().await,
            changes: None,
            touched: false,
        }
    }

    pub async fn write_owned(self) -> OwnedDbWriteGuard {
        OwnedDbWriteGuard {
            guard: self.db.write_owned().await,
            changes: Some(self.changes),
            touched: false,
        }
    }

    pub async fn write_outside_catalog_owned(self) -> OwnedDbWriteGuard {
        OwnedDbWriteGuard {
            guard: self.db.write_owned().await,
            changes: None,
            touched: false,
        }
    }

    pub fn try_write(&self) -> Result<DbWriteGuard<'_>, TryLockError> {
        Ok(DbWriteGuard {
            guard: self.db.try_write()?,
            changes: Some(&self.changes),
            touched: false,
        })
    }

    /// The database's identity and its change count. Under a read lock the count is even and
    /// covers every change the reader sees; an odd count means a write guard holding mutable
    /// access is open, and whatever is derived under it must not be kept.
    pub fn stamp(&self) -> (u64, u64) {
        (self.serial, self.changes())
    }

    fn changes(&self) -> u64 {
        self.changes.load(AtomicOrdering::Acquire)
    }

    /// Whether `db` is the database behind this handle.
    pub fn holds(&self, db: &DbAny) -> bool {
        std::ptr::from_ref(db) as usize == self.address
    }

    /// The database while no other handle shares it, counted as a change.
    pub fn unshared_mut(&mut self) -> Option<&mut DbAny> {
        let db = Arc::get_mut(&mut self.db).map(RwLock::get_mut)?;
        self.changes.fetch_add(2, AtomicOrdering::AcqRel);
        Some(db)
    }
}

/// A write lock on a [`DbAsync`]; mutable access through it counts as a change unless the lock
/// was taken outside the catalog.
pub struct DbWriteGuard<'a> {
    guard: RwLockWriteGuard<'a, DbAny>,
    changes: Option<&'a AtomicU64>,
    /// Whether the guard has handed out mutable access, leaving the change count odd.
    touched: bool,
}

/// An owned write lock on a [`DbAsync`]; mutable access through it counts as a change unless the
/// lock was taken outside the catalog.
pub struct OwnedDbWriteGuard {
    guard: OwnedRwLockWriteGuard<DbAny>,
    changes: Option<Arc<AtomicU64>>,
    touched: bool,
}

macro_rules! counted_write_guard {
    ($guard:ty) => {
        impl std::ops::Deref for $guard {
            type Target = DbAny;

            fn deref(&self) -> &DbAny {
                &self.guard
            }
        }

        impl std::ops::DerefMut for $guard {
            fn deref_mut(&mut self) -> &mut DbAny {
                if !self.touched
                    && let Some(changes) = &self.changes
                {
                    changes.fetch_add(1, AtomicOrdering::AcqRel);
                    self.touched = true;
                }
                &mut self.guard
            }
        }

        // Runs before the lock is released, so no reader sees the odd count.
        impl Drop for $guard {
            fn drop(&mut self) {
                if self.touched
                    && let Some(changes) = &self.changes
                {
                    changes.fetch_add(1, AtomicOrdering::AcqRel);
                }
            }
        }

        impl DbAccess for $guard {
            fn exec<T: Query>(&self, query: T) -> Result<QueryResult, DbError> {
                DbAny::exec(self, query)
            }

            fn exec_mut<T: QueryMut>(&mut self, query: T) -> Result<QueryResult, DbError> {
                DbAny::exec_mut(self, query)
            }
        }
    };
}

counted_write_guard!(DbWriteGuard<'_>);
counted_write_guard!(OwnedDbWriteGuard);

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

    use super::{
        DbAsync,
        is_poisoned,
    };

    #[test]
    fn the_change_count_is_odd_only_while_a_write_guard_holds_mutable_access() -> anyhow::Result<()>
    {
        let mut shared = DbAsync::new(super::test_db::new_test_db()?);
        let (_, start) = shared.stamp();
        assert_eq!(start % 2, 0);

        drop(shared.try_write()?);
        assert_eq!(
            shared.stamp().1,
            start,
            "an untouched guard changes nothing"
        );

        let mut guard = shared.try_write()?;
        super::fixtures::insert_track(&mut guard, "One")?;
        let (_, open) = shared.stamp();
        assert_eq!(open % 2, 1);
        super::fixtures::insert_track(&mut guard, "Two")?;
        assert_eq!(shared.stamp().1, open, "one guard is one change");
        drop(guard);
        let (_, written) = shared.stamp();
        assert_eq!(written % 2, 0);
        assert!(written > start);

        let mut owned = shared.try_write()?;
        let _ = owned.transaction_mut(|t| -> anyhow::Result<()> {
            super::DbAccess::exec_mut(t, agdb::QueryBuilder::insert().nodes().count(1).query())?;
            anyhow::bail!("rolled back")
        });
        drop(owned);
        let (_, rolled_back) = shared.stamp();
        assert!(rolled_back > written, "a rolled-back write still counts");
        assert_eq!(rolled_back % 2, 0);

        assert!(shared.unshared_mut().is_some());
        let (_, unshared) = shared.stamp();
        assert!(unshared > rolled_back);
        assert_eq!(unshared % 2, 0);
        Ok(())
    }

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
