// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::Arc;

use agdb::DbId;
use harmony_core::LuaAsyncExt;
use mlua::{
    ExternalResult,
    Lua,
    Result,
    Table,
    Value,
};

use crate::{
    STATE,
    db,
    plugins::parse_ids,
};

fn resolve_container(entry: &db::Entry) -> Option<String> {
    entry
        .full_path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.trim().to_ascii_lowercase())
        .filter(|ext| !ext.is_empty())
}

fn resolve_primary_container(
    db: &agdb::DbAny,
    track_db_id: DbId,
) -> anyhow::Result<Option<String>> {
    let Some(source) = db::track_sources::get_primary_by_track(db, track_db_id)? else {
        return Ok(None);
    };
    let Some(source_id) = source.db_id else {
        return Ok(None);
    };
    let Some(entry_db_id) = db::track_sources::get_entry_id(db, source_id)? else {
        return Ok(None);
    };
    let Some(entry) = db::entries::get_by_id(db, entry_db_id)? else {
        return Ok(None);
    };
    Ok(resolve_container(&entry))
}

struct TrackSourcesModule;

#[harmony_macros::module(
    plugin_scoped,
    name = "TrackSources",
    local = "track_sources",
    path = "lyra/track_sources"
)]
impl TrackSourcesModule {
    /// Returns the primary source key for a track.
    pub(crate) async fn get_primary_source_key(
        _plugin_id: Option<Arc<str>>,
        track_id: i64,
    ) -> Result<Option<String>> {
        if track_id <= 0 {
            return Ok(None);
        }

        let db = STATE.db.read().await;
        let Some(source) =
            db::track_sources::get_primary_by_track(&db, DbId(track_id)).into_lua_err()?
        else {
            return Ok(None);
        };

        Ok(Some(source.source_key).filter(|value| !value.trim().is_empty()))
    }

    /// Returns the primary container for a track.
    pub(crate) async fn get_primary_container(
        _plugin_id: Option<Arc<str>>,
        track_id: i64,
    ) -> Result<Option<String>> {
        if track_id <= 0 {
            return Ok(None);
        }

        let db = STATE.db.read().await;
        let container = resolve_primary_container(&db, DbId(track_id)).into_lua_err()?;
        Ok(container)
    }

    /// Returns primary containers for many tracks.
    #[harmony(args(track_ids: Vec<u64>), returns(std::collections::BTreeMap<u64, Option<String>>))]
    pub(crate) async fn get_primary_containers(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        track_ids: Table,
    ) -> Result<Table> {
        let track_ids = parse_ids(track_ids)?;
        let db = STATE.db.read().await;

        let table = lua.create_table()?;
        for track_id in track_ids {
            let container = resolve_primary_container(&db, track_id).into_lua_err()?;
            if let Some(container) = container {
                table.set(track_id.0, container)?;
            } else {
                table.set(track_id.0, Value::Nil)?;
            }
        }

        Ok(table)
    }
}

crate::plugins::plugin_surface_exports!(
    TrackSourcesModule,
    "lyra.track_sources",
    "Read and modify the source files backing tracks.",
    Medium
);

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::resolve_container;
    use crate::db::Entry;
    use crate::db::entries::EntryKind;

    #[test]
    fn resolve_container_normalizes_extensions() {
        let entry = Entry {
            db_id: None,
            id: "entry-1".to_string(),
            full_path: PathBuf::from("/tmp/Track.FLAC"),
            kind: EntryKind::File,
            file_kind: Some("audio".to_string()),
            name: "Track.FLAC".to_string(),
            hash: None,
            size: 0,
            mtime: 0,
            ctime: 0,
        };

        assert_eq!(resolve_container(&entry).as_deref(), Some("flac"));
    }

    #[test]
    fn resolve_container_ignores_missing_extensions() {
        let entry = Entry {
            db_id: None,
            id: "entry-2".to_string(),
            full_path: PathBuf::from("/tmp/track"),
            kind: EntryKind::File,
            file_kind: Some("audio".to_string()),
            name: "track".to_string(),
            hash: None,
            size: 0,
            mtime: 0,
            ctime: 0,
        };

        assert_eq!(resolve_container(&entry), None);
    }
}
