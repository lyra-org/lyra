// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::Arc;

use agdb::{
    CountComparison,
    DbAny,
    QueryBuilder,
};
use tokio::sync::RwLock;

use super::{
    DbAsync,
    compact,
    indexes,
    process_lock::{
        self,
        DbProcessLock,
        LockMode,
    },
};
use crate::config::{
    DbConfig,
    DbKind,
};

/// Result of `create`: opened DB plus the lock guard for its on-disk file.
pub(crate) struct Created {
    pub(crate) db: DbAsync,
    pub(crate) lock: Option<DbProcessLock>,
}

pub(crate) const ROOT_COLLECTION_ALIASES: &[&str] = &[
    "api_keys",
    "users",
    "sessions",
    "playback_sessions",
    "listens",
    "libraries",
    "entries",
    "releases",
    "covers",
    "credits",
    "tracks",
    "track_sources",
    "cue_sheets",
    "cue_tracks",
    "artists",
    "tags",
    "datastore",
    "providers",
    "metadata_layers",
    "mixers",
    "external_ids",
    "lyrics",
    "playlists",
    "genres",
    "labels",
    "release_labels",
    "settings",
    "user_settings",
    "server",
    "roles",
];

const CORE_INDEXES: &[&str] = &[
    "id",
    "scan_name",
    "label_scan_name",
    "tag",
    "tag_owner_name", // composite "{owner_db_id}:{normalized_name}" for O(log N) tag lookup
    // Required for library uniqueness lookups: `select().elements::<T>().search()`
    // filters output but doesn't stop traversal, so without these the checks
    // walk the entire libraries subgraph on every create/update.
    "name_key",
    "directory_key",
    "provider_id",
    "track_id",
    "state",
    "session_alias",
    "username",
    "key_hash",
    "token_hash",
    "identity",
    "id_value",
];

pub(crate) fn open(kind: DbKind, db_path: &str) -> anyhow::Result<DbAny> {
    Ok(match kind {
        DbKind::Memory => DbAny::new_memory(db_path.as_ref())?,
        DbKind::File => DbAny::new_file(db_path.as_ref())?,
        DbKind::Mmap => DbAny::new(db_path.as_ref())?,
    })
}

pub(crate) fn initialize_root_aliases(db: &mut DbAny, aliases: &[&str]) -> anyhow::Result<()> {
    let mut root_aliases = vec!["root"];
    for alias in aliases {
        if *alias != "root" && !root_aliases.contains(alias) {
            root_aliases.push(*alias);
        }
    }
    let root_edge_targets = root_aliases
        .iter()
        .copied()
        .filter(|alias| *alias != "root")
        .collect::<Vec<_>>();

    db.transaction_mut(|t| -> anyhow::Result<()> {
        t.exec_mut(
            QueryBuilder::insert()
                .nodes()
                .aliases(root_aliases.clone())
                .query(),
        )?;
        t.exec_mut(
            QueryBuilder::remove()
                .search()
                .from("root")
                .where_()
                .edge()
                .and()
                .distance(CountComparison::Equal(1))
                .query(),
        )?;
        if !root_edge_targets.is_empty() {
            t.exec_mut(
                QueryBuilder::insert()
                    .edges()
                    .from("root")
                    .to(root_edge_targets.clone())
                    .query(),
            )?;
        }

        Ok(())
    })
}

pub(crate) fn initialize(db: &mut DbAny) -> anyhow::Result<()> {
    initialize_root_aliases(db, ROOT_COLLECTION_ALIASES)?;
    indexes::ensure_indexes(db, CORE_INDEXES)?;
    Ok(())
}

/// Server-side path: lock + pre-open compaction (mmap) + open + schema init.
pub(crate) fn create(config: &DbConfig) -> anyhow::Result<Created> {
    let lock = process_lock::acquire(config, LockMode::Blocking)?;
    compact::pre_open(config)?;

    let db_path = config.path.to_string_lossy();
    if !matches!(config.kind, DbKind::Memory) {
        tracing::info!(
            path = %config.path.display(),
            kind = kind_label(config.kind),
            "opening db (may apply WAL recovery)"
        );
    } else {
        tracing::debug!(
            path = %config.path.display(),
            "opening in-memory db"
        );
    }
    let mut db = open(config.kind, db_path.as_ref())?;
    initialize(&mut db)?;
    Ok(Created {
        db: Arc::new(RwLock::new(db)),
        lock,
    })
}

fn kind_label(kind: DbKind) -> &'static str {
    match kind {
        DbKind::Memory => "memory",
        DbKind::File => "file",
        DbKind::Mmap => "mmap",
    }
}

/// In-memory placeholder for `DbHandle::reset_with`. `temp_dir()`-anchored
/// because `DbMemory::new()` loads from file if one exists; `nanoid` for
/// per-call uniqueness even if external serialization breaks down.
pub(crate) fn placeholder() -> anyhow::Result<DbAsync> {
    let placeholder_path =
        std::env::temp_dir().join(format!("lyra-db-placeholder-{}", nanoid::nanoid!()));
    let db = open(DbKind::Memory, placeholder_path.to_string_lossy().as_ref())?;
    Ok(Arc::new(RwLock::new(db)))
}
