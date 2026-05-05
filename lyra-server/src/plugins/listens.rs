// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;
use std::sync::Arc;

use agdb::DbId;
use harmony_core::LuaAsyncExt;
use mlua::{
    Lua,
    Result,
    Table,
};

use crate::{
    STATE,
    db,
    plugins::parse_ids,
    services::{
        playback_sessions,
        providers::PROVIDER_REGISTRY,
    },
};

struct ResolvedStats {
    counts: HashMap<DbId, u64>,
    last_played: HashMap<DbId, u64>,
}

async fn resolve_stats(
    track_ids: &[DbId],
    user_db_id: Option<DbId>,
    merge_unique_external_ids: bool,
) -> Result<ResolvedStats> {
    let db = STATE.db.read().await;

    if !merge_unique_external_ids {
        let stats =
            db::listens::get_stats(&db, track_ids, user_db_id).map_err(mlua::Error::external)?;
        let mut counts = HashMap::new();
        let mut last_played = HashMap::new();
        for s in stats {
            counts.insert(s.db_id, s.count);
            if let Some(lp) = s.last_played {
                last_played.insert(s.db_id, lp);
            }
        }
        return Ok(ResolvedStats {
            counts,
            last_played,
        });
    }

    let unique_track_id_pairs = {
        let registry = PROVIDER_REGISTRY.read().await;
        registry.unique_track_id_pairs()
    };

    let mut requested_merged_ids: Vec<(DbId, Vec<DbId>)> = Vec::new();
    let mut merged_unique_ids = std::collections::HashSet::new();

    for track_id in track_ids {
        let merged_ids = playback_sessions::resolve_merged_track_ids_for_play_count(
            &db,
            *track_id,
            &unique_track_id_pairs,
        )
        .map_err(mlua::Error::external)?;
        for merged_id in &merged_ids {
            merged_unique_ids.insert(*merged_id);
        }
        requested_merged_ids.push((*track_id, merged_ids));
    }

    let merged_track_ids = merged_unique_ids.into_iter().collect::<Vec<_>>();
    let merged_stats = db::listens::get_stats(&db, &merged_track_ids, user_db_id)
        .map_err(mlua::Error::external)?;
    let merged_by_id: HashMap<DbId, &db::listens::ListenStats> =
        merged_stats.iter().map(|s| (s.db_id, s)).collect();

    let mut counts = HashMap::new();
    let mut last_played = HashMap::new();
    for (requested_id, merged_ids) in requested_merged_ids {
        let mut total_count = 0u64;
        let mut max_last_played: Option<u64> = None;
        for merged_id in merged_ids {
            if let Some(s) = merged_by_id.get(&merged_id) {
                total_count = total_count.saturating_add(s.count);
                if let Some(lp) = s.last_played {
                    if lp > max_last_played.unwrap_or(0) {
                        max_last_played = Some(lp);
                    }
                }
            }
        }
        counts.insert(requested_id, total_count);
        if let Some(lp) = max_last_played {
            last_played.insert(requested_id, lp);
        }
    }

    Ok(ResolvedStats {
        counts,
        last_played,
    })
}

fn dbid_map_to_table(lua: &Lua, map: &HashMap<DbId, u64>) -> Result<Table> {
    let table = lua.create_table()?;
    for (id, value) in map {
        table.set(id.0, *value)?;
    }
    Ok(table)
}

struct ListensModule;

#[harmony_macros::module(
    plugin_scoped,
    name = "Listens",
    local = "listens",
    path = "lyra/listens"
)]
impl ListensModule {
    /// Returns the listen count for a track.
    pub(crate) async fn get_count(
        _plugin_id: Option<Arc<str>>,
        track_id: i64,
        user_id: Option<i64>,
        merge_unique_external_ids: Option<bool>,
    ) -> Result<u64> {
        if track_id <= 0 {
            return Err(mlua::Error::runtime("track_id must be a positive id"));
        }

        let user_db_id = user_id.map(DbId);
        let merge = merge_unique_external_ids.unwrap_or(false);
        let track_db_id = DbId(track_id);
        let stats = resolve_stats(&[track_db_id], user_db_id, merge).await?;
        Ok(*stats.counts.get(&track_db_id).unwrap_or(&0))
    }

    /// Returns listen counts for many tracks.
    #[harmony(args(track_ids: Vec<u64>, user_id: Option<i64>, merge_unique_external_ids: Option<bool>), returns(std::collections::BTreeMap<u64, u64>))]
    pub(crate) async fn get_counts(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        track_ids: Table,
        user_id: Option<i64>,
        merge_unique_external_ids: Option<bool>,
    ) -> Result<Table> {
        let track_ids = parse_ids(track_ids)?;
        let user_db_id = user_id.map(DbId);
        let merge = merge_unique_external_ids.unwrap_or(false);
        let stats = resolve_stats(&track_ids, user_db_id, merge).await?;
        dbid_map_to_table(&lua, &stats.counts)
    }

    /// Returns both counts and last-played timestamps in a single scan.
    #[harmony(args(track_ids: Vec<u64>, user_id: Option<i64>, merge_unique_external_ids: Option<bool>), returns(std::collections::BTreeMap<String, std::collections::BTreeMap<u64, u64>>))]
    pub(crate) async fn get_stats(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        track_ids: Table,
        user_id: Option<i64>,
        merge_unique_external_ids: Option<bool>,
    ) -> Result<Table> {
        let track_ids = parse_ids(track_ids)?;
        let user_db_id = user_id.map(DbId);
        let merge = merge_unique_external_ids.unwrap_or(false);
        let stats = resolve_stats(&track_ids, user_db_id, merge).await?;
        let counts = dbid_map_to_table(&lua, &stats.counts)?;
        let last_played = dbid_map_to_table(&lua, &stats.last_played)?;
        let result = lua.create_table()?;
        result.set("counts", counts)?;
        result.set("last_played", last_played)?;
        Ok(result)
    }
}

crate::plugins::plugin_surface_exports!(
    ListensModule,
    "lyra.listens",
    "Read and record listen history.",
    Medium
);
