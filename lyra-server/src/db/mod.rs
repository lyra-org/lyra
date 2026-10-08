// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

macro_rules! impl_luau_enum_userdata {
    ($ty:path, $name:literal, [$($variant:ident),+ $(,)?]) => {
        impl ::harmony_luau::LuauTypeInfo for $ty {
            fn luau_type() -> ::harmony_luau::LuauType {
                ::harmony_luau::LuauType::literal($name)
            }
        }

        impl ::harmony_luau::DescribeUserData for $ty {
            fn class_descriptor() -> ::harmony_luau::ClassDescriptor {
                ::harmony_luau::ClassDescriptor {
                    name: $name,
                    description: None,
                    fields: vec![
                        $(
                            ::harmony_luau::FieldDescriptor {
                                name: stringify!($variant),
                                ty: <Self as ::harmony_luau::LuauTypeInfo>::luau_type(),
                                description: None,
                            }
                        ),+
                    ],
                    methods: vec![],
                }
            }
        }
    };
}

macro_rules! impl_luau_record_userdata {
    (
        $ty:path,
        $name:literal,
        fields { $($field:tt : $field_ty:ty as $key:literal),* $(,)? },
        methods { $($method:ident($arg:ident : $arg_ty:ty)),* $(,)? }
    ) => {
        impl ::harmony_luau::LuauTypeInfo for $ty {
            fn luau_type() -> ::harmony_luau::LuauType {
                ::harmony_luau::LuauType::literal($name)
            }
        }

        impl ::harmony_luau::DescribeUserData for $ty {
            fn class_descriptor() -> ::harmony_luau::ClassDescriptor {
                ::harmony_luau::ClassDescriptor {
                    name: $name,
                    description: None,
                    fields: vec![
                        $(
                            ::harmony_luau::FieldDescriptor {
                                name: $key,
                                ty: <$field_ty as ::harmony_luau::LuauTypeInfo>::luau_type(),
                                description: None,
                            }
                        ),*
                    ],
                    methods: vec![
                        $(
                            ::harmony_luau::MethodDescriptor {
                                name: stringify!($method),
                                description: None,
                                params: vec![
                                    ::harmony_luau::ParameterDescriptor {
                                        name: stringify!($arg),
                                        ty: <$arg_ty as ::harmony_luau::LuauTypeInfo>::luau_type(),
                                        description: None,
                                        variadic: false,
                                    }
                                ],
                                returns: vec![],
                                yields: false,
                                kind: ::harmony_luau::MethodKind::Instance,
                            }
                        ),*
                    ],
                }
            }
        }
    };
}

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
pub(crate) mod fixtures;
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
pub(crate) mod playbacks;
pub(crate) mod playlists;
pub(crate) mod plugin;
pub(crate) mod plugin_repositories;
pub(crate) mod process_lock;
pub(crate) mod providers;
pub(crate) mod ratings;
pub(crate) mod releases;
pub(crate) mod roles;
pub(crate) mod search;
pub(crate) mod server;
pub(crate) mod settings;
pub(crate) mod sync_runs;
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
    DbType,
    DbValue,
    Query,
    QueryBuilder,
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
pub(crate) fn is_poisoned(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<DbError>()
            .is_some_and(|error| error.ty == agdb::DbErrorType::Poisoned)
    })
}

pub(crate) use ids::{
    NodeId,
    ResolveId,
};

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

pub(crate) use artists::Artist;
pub(crate) use artists::ArtistType;
pub(crate) use artists::CreditedArtist;
pub(crate) use artists::relations::ArtistRelationType;
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
pub(crate) use metadata::custom_fields::ProviderCustomFields;
pub(crate) use metadata::fields::MetadataField;
pub(crate) use metadata::layers::MetadataLayer;
pub(crate) use playback_sessions::{
    EvictedPlayback,
    PlaybackSession,
    PlaybackState,
};
pub(crate) use playlists::Playlist;
pub(crate) use providers::ProviderConfig;
pub(crate) use providers::external_ids::IdSource;
pub(crate) use releases::Release;
pub(crate) use roles::Permission;
pub(crate) use tags::Tag;
pub(crate) use track_sources::TrackSource;
pub(crate) use tracks::Track;
pub(crate) use users::{
    Session,
    User,
};

pub(crate) type DbAsync = Arc<RwLock<DbAny>>;
pub(crate) use bootstrap::{
    Created,
    create,
};
pub(crate) use providers::external_ids;

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
