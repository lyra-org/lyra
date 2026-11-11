// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::path::PathBuf;

use agdb::{
    CountComparison,
    DbAny,
    QueryBuilder,
};
use serde::Deserialize;

use super::{
    DbAccess,
    DbAsync,
    indexes,
    migration::{
        self,
        Migration,
        Plan,
    },
    process_lock::{
        self,
        DbProcessLock,
        LockMode,
    },
};

const DEFAULT_DB_FILE_NAME: &str = "lyra.db";

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DbKind {
    Memory,
    File,
    #[default]
    Mmap,
}

impl DbKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::File => "file",
            Self::Mmap => "mmap",
        }
    }
}

#[derive(Clone)]
pub struct DbConfig {
    pub kind: DbKind,
    pub path: PathBuf,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            kind: DbKind::default(),
            path: PathBuf::from(DEFAULT_DB_FILE_NAME),
        }
    }
}

/// Result of `create`: opened DB ready for application use.
pub struct Created {
    pub db: DbAsync,
    pub lock: Option<DbProcessLock>,
}

impl Created {
    /// The database while nothing else shares it yet, for setup that must
    /// finish before it is published.
    pub fn unshared_mut(&mut self) -> Option<&mut DbAny> {
        self.db.unshared_mut()
    }
}

pub const ROOT_COLLECTION_ALIASES: &[&str] = &[
    "users",
    "playbacks",
    "playback_sessions",
    "libraries",
    "releases",
    "chains",
    "tracks",
    "track_sources",
    "cue_sheets",
    "cue_tracks",
    "missing_entries",
    "artists",
    "datastore",
    "providers",
    "mixers",
    "playlists",
    "genres",
    "labels",
    "plugin_settings",
    "server",
    "roles",
    "sync_runs",
    "plugin_repositories",
];

const CORE_INDEXES: &[&str] = &[
    "id",
    "scan_name",
    super::artists::SCAN_ALIAS_HOLDER,
    "label_scan_name",
    "tag_owner_name", // composite "{owner_public_id}:{normalized_name}" for O(log N) tag lookup
    // Required for library uniqueness lookups: `select().elements::<T>().search()`
    // filters output but doesn't stop traversal, so without these the checks
    // walk the entire libraries subgraph on every create/update.
    "name_key",
    "path_key",
    "username",
    "key_hash",
    "token_hash",
    "identity",
    "id_value",
    "play_id",
];

pub fn open(kind: DbKind, db_path: &str) -> anyhow::Result<DbAny> {
    Ok(match kind {
        DbKind::Memory => DbAny::new_memory(db_path)?,
        DbKind::File => DbAny::new_file(db_path)?,
        DbKind::Mmap => DbAny::new(db_path)?,
    })
}

pub(crate) fn initialize_root_aliases(
    db: &mut impl DbAccess,
    aliases: &[&str],
) -> anyhow::Result<()> {
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

    db.exec_mut(QueryBuilder::insert().nodes().aliases(root_aliases).query())?;
    db.exec_mut(
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
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("root")
                .to(root_edge_targets)
                .query(),
        )?;
    }
    Ok(())
}

#[cfg(any(test, feature = "testing"))]
pub(crate) fn initialize(db: &mut DbAny) -> anyhow::Result<()> {
    initialize_with(db, &migration::TO_CURRENT)
}

#[cfg(any(test, feature = "testing"))]
pub(crate) fn initialize_with(db: &mut DbAny, migration: &Migration) -> anyhow::Result<()> {
    let plan = migration.plan(db)?;
    set_up(db, migration, &plan)
}

/// Adds the current root aliases and indexes, whatever the plan, then runs the
/// plan's migration steps on them, in one transaction so a failure changes
/// nothing.
fn set_up(db: &mut DbAny, migration: &Migration, plan: &Plan) -> anyhow::Result<()> {
    db.transaction_mut(|t| -> anyhow::Result<()> {
        initialize_root_aliases(t, ROOT_COLLECTION_ALIASES)?;
        indexes::ensure_indexes(t, CORE_INDEXES)?;
        migration.apply(t, plan)
    })
}

/// Fails unless `db`, opened without `create`, is on this version with nothing
/// left to migrate or stamp.
pub fn ensure_current(db: &DbAny) -> anyhow::Result<()> {
    migration::TO_CURRENT.ensure_current(db)
}

/// Server-side path: open + schema init.
pub fn create(config: &DbConfig) -> anyhow::Result<Created> {
    create_with(config, &migration::TO_CURRENT)
}

pub(crate) fn create_with(config: &DbConfig, migration: &Migration) -> anyhow::Result<Created> {
    let lock = process_lock::acquire(config, LockMode::Blocking)?;
    let db_path = config.path.to_string_lossy();
    tracing::info!(
        path = %config.path.display(),
        kind = kind_label(config.kind),
        "opening db"
    );
    let mut db = open(config.kind, db_path.as_ref())?;
    let plan = migration.plan(&db)?;
    if let Plan::Migrate { from, steps, .. } = &plan {
        drop(db);
        migration::back_up(config, from, *steps)?;
        db = open(config.kind, db_path.as_ref())?;
    }
    set_up(&mut db, migration, &plan)?;
    optimize_storage(&mut db, config);
    Ok(Created {
        db: DbAsync::new(db),
        lock,
    })
}

fn optimize_storage(db: &mut DbAny, config: &DbConfig) {
    if matches!(config.kind, DbKind::Memory) {
        return;
    }

    tracing::debug!(
        path = %config.path.display(),
        kind = kind_label(config.kind),
        "optimizing db storage"
    );
    if let Err(err) = db.optimize_storage() {
        tracing::warn!(
            error = %err,
            path = %config.path.display(),
            "failed to optimize db storage"
        );
    }
}

fn kind_label(kind: DbKind) -> &'static str {
    match kind {
        DbKind::Memory => "memory",
        DbKind::File => "file",
        DbKind::Mmap => "mmap",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_db::TestDb;

    #[test]
    fn initialize_is_repeatable_on_current_layout() -> anyhow::Result<()> {
        let mut db = TestDb::initialized()?.into_inner();
        initialize(&mut db)?;
        Ok(())
    }
}
