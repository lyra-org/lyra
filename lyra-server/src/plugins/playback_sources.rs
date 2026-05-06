// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::DbId;
use harmony_core::LuaAsyncExt;
use mlua::{
    ExternalResult,
    Lua,
    Result,
    Table,
    Value,
};
use std::collections::HashSet;

use crate::{
    STATE,
    plugins::caller::RequestCaller,
    plugins::db::{
        self,
        NodeId,
        Permission,
        ResolveId,
    },
    services::playback_sources as playback_source_service,
};

#[harmony_macros::interface]
struct EntryInfo {
    db_id: Option<NodeId>,
    id: String,
    full_path: Option<String>,
    kind: String,
    name: String,
    hash: Option<String>,
    size: u64,
    mtime: u64,
}

#[harmony_macros::interface]
struct PlaybackSourceInfo {
    track_id: u64,
    source_id: u64,
    source_kind: String,
    source_key: String,
    is_primary: bool,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    is_virtual: bool,
    entry: Option<EntryInfo>,
}

use super::entry_to_table;

fn source_to_table(
    lua: &Lua,
    source: playback_source_service::PlaybackSource,
    include_entry: bool,
    include_full_path: bool,
) -> Result<Table> {
    let playback_source_service::PlaybackSource {
        track_db_id,
        source_id,
        source_kind,
        source_key,
        is_primary,
        start_ms,
        end_ms,
        entry,
        ..
    } = source;

    let source_table = lua.create_table()?;
    source_table.set("track_id", track_db_id.0)?;
    source_table.set("source_id", source_id.0)?;
    source_table.set("source_kind", source_kind)?;
    source_table.set("source_key", source_key)?;
    source_table.set("is_primary", is_primary)?;
    source_table.set("start_ms", start_ms)?;
    source_table.set("end_ms", end_ms)?;
    source_table.set("is_virtual", start_ms.is_some() || end_ms.is_some())?;
    if include_entry {
        if let Some(entry) = entry {
            source_table.set("entry", entry_to_table(lua, entry, include_full_path)?)?;
        } else {
            source_table.set("entry", Value::Nil)?;
        }
    }

    Ok(source_table)
}

struct PlaybackSourcesModule;

#[harmony_macros::module(
    plugin_scoped,
    name = "PlaybackSources",
    local = "playback_sources",
    path = "lyra/playback_sources",
    interfaces(EntryInfo, PlaybackSourceInfo)
)]
impl PlaybackSourcesModule {
    /// Returns resolved playback sources for tracks matching the given scope.
    #[harmony(args(id: Option<ResolveId>, include_entry: Option<bool>), returns(Vec<PlaybackSourceInfo>))]
    pub(crate) async fn get(
        lua: Lua,
        #[harmony_context] caller: RequestCaller,
        id: Option<ResolveId>,
        include_entry: Option<bool>,
    ) -> Result<Table> {
        let resolve_id = id.unwrap_or_else(|| ResolveId::alias("tracks"));
        let include_entry = include_entry.unwrap_or(false);
        let db = STATE.db.read().await;
        let include_full_path =
            db::roles::has_permission(&caller.principal.permissions, Permission::ManageLibraries);
        let query_id = resolve_id
            .to_query_id(&db)
            .into_lua_err()?
            .ok_or_else(|| mlua::Error::runtime("could not resolve scope"))?;
        let tracks = db::tracks::get(&db, query_id).into_lua_err()?;

        let rows = lua.create_table()?;
        let mut index = 1usize;
        for track in tracks {
            let Some(track_id) = track.db_id.map(DbId::from) else {
                continue;
            };
            if !crate::routes::entity_accessible_to_principal(&db, &caller.principal, track_id)
                .into_lua_err()?
            {
                continue;
            }
            let Some(source) =
                playback_source_service::resolve(&db, track_id, include_entry).into_lua_err()?
            else {
                continue;
            };
            let source_table = source_to_table(&lua, source, include_entry, include_full_path)?;

            rows.set(index, source_table)?;
            index += 1;
        }

        Ok(rows)
    }

    /// Returns resolved playback sources for many tracks.
    #[harmony(args(track_ids: Vec<u64>, include_entry: Option<bool>), returns(std::collections::BTreeMap<u64, Option<PlaybackSourceInfo>>))]
    pub(crate) async fn get_many(
        lua: Lua,
        #[harmony_context] caller: RequestCaller,
        track_ids: Table,
        include_entry: Option<bool>,
    ) -> Result<Table> {
        let include_entry = include_entry.unwrap_or(false);
        let mut unique_track_ids = Vec::new();
        let mut seen = HashSet::new();
        for value in track_ids.sequence_values::<i64>() {
            let raw_id = value?;
            if raw_id <= 0 {
                continue;
            }
            let track_db_id = DbId(raw_id);
            if seen.insert(track_db_id) {
                unique_track_ids.push(track_db_id);
            }
        }

        let db = STATE.db.read().await;
        let include_full_path =
            db::roles::has_permission(&caller.principal.permissions, Permission::ManageLibraries);
        let rows = lua.create_table()?;
        for track_id in unique_track_ids {
            if !crate::routes::entity_accessible_to_principal(&db, &caller.principal, track_id)
                .into_lua_err()?
            {
                rows.set(track_id.0, Value::Nil)?;
                continue;
            }
            let Some(source) =
                playback_source_service::resolve(&db, track_id, include_entry).into_lua_err()?
            else {
                rows.set(track_id.0, Value::Nil)?;
                continue;
            };
            let source_table = source_to_table(&lua, source, include_entry, include_full_path)?;
            rows.set(track_id.0, source_table)?;
        }

        Ok(rows)
    }
}

crate::plugins::plugin_surface_exports!(
    PlaybackSourcesModule,
    "lyra.playback_sources",
    "Read and modify playback sources.",
    Medium
);
