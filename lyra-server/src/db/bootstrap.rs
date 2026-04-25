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
    indexes,
};
use crate::config::{
    DbConfig,
    DbKind,
};

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

pub(crate) fn create(config: &DbConfig) -> anyhow::Result<DbAsync> {
    let db_path = config.path.to_string_lossy();
    let mut db = open(config.kind, db_path.as_ref())?;
    initialize(&mut db)?;
    Ok(Arc::new(RwLock::new(db)))
}
