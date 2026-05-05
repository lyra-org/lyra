// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
};
use std::sync::Arc;

use agdb::{
    DbAny,
    DbId,
    QueryId,
};
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
    db::ResolveId,
    db::{
        self,
        Cover,
    },
    plugins::parse_ids,
};

#[harmony_macros::interface]
struct CoverInfo {
    path: String,
    mime_type: String,
    hash: String,
    blurhash: Option<String>,
    release_id: u64,
}

enum CoverValidity {
    Valid(Cover),
    NotFound,
    Unavailable,
}

fn check_cover_validity(cover: Cover) -> CoverValidity {
    match std::fs::metadata(&cover.path) {
        Ok(meta) if meta.is_file() => CoverValidity::Valid(cover),
        Ok(_) => CoverValidity::NotFound, // exists but not a regular file
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => CoverValidity::NotFound,
        Err(_) => CoverValidity::Unavailable,
    }
}

fn resolve_persisted_cover(
    db: &DbAny,
    item_id: DbId,
    stale_owners: &mut Vec<DbId>,
) -> anyhow::Result<Option<(DbId, Cover)>> {
    let resolved = resolve_persisted_covers(db, &[item_id], stale_owners)?;
    Ok(resolved.into_values().next())
}

fn resolve_persisted_covers(
    db: &DbAny,
    item_ids: &[DbId],
    stale_owners: &mut Vec<DbId>,
) -> anyhow::Result<HashMap<DbId, (DbId, Cover)>> {
    let mut unique_ids = Vec::new();
    let mut seen = HashSet::new();
    for item_id in item_ids {
        if item_id.0 <= 0 {
            continue;
        }
        if seen.insert(*item_id) {
            unique_ids.push(*item_id);
        }
    }

    let direct_covers = db::covers::get_many(db, &unique_ids)?;

    let mut resolved = HashMap::new();
    let mut unresolved = Vec::new();
    for item_id in unique_ids {
        if let Some(cover) = direct_covers.get(&item_id) {
            match check_cover_validity(cover.clone()) {
                CoverValidity::Valid(valid) => {
                    resolved.insert(item_id, (item_id, valid));
                    continue;
                }
                CoverValidity::NotFound => stale_owners.push(item_id),
                CoverValidity::Unavailable => {}
            }
        }
        unresolved.push(item_id);
    }

    if unresolved.is_empty() {
        return Ok(resolved);
    }

    let track_releases = db::releases::get_by_tracks(db, &unresolved)?;
    let mut release_ids = Vec::new();
    let mut seen_release_ids = HashSet::new();
    for related_releases in track_releases.values() {
        for release in related_releases {
            let Some(release_id) = release.db_id.clone().map(Into::<DbId>::into) else {
                continue;
            };
            if seen_release_ids.insert(release_id) {
                release_ids.push(release_id);
            }
        }
    }

    let covers_by_release = db::covers::get_many(db, &release_ids)?;

    for item_id in unresolved {
        let Some(mut releases) = track_releases.get(&item_id).cloned() else {
            continue;
        };
        releases
            .sort_by_key(|release| release.db_id.clone().map(Into::<DbId>::into).map(|id| id.0));

        for release in releases {
            let Some(release_id) = release.db_id.clone().map(Into::<DbId>::into) else {
                continue;
            };
            let Some(cover) = covers_by_release.get(&release_id) else {
                continue;
            };
            match check_cover_validity(cover.clone()) {
                CoverValidity::Valid(valid) => {
                    resolved.insert(item_id, (release_id, valid));
                    break;
                }
                CoverValidity::NotFound => stale_owners.push(release_id),
                CoverValidity::Unavailable => {}
            }
        }
    }

    Ok(resolved)
}

/// Re-validates under write lock to avoid racing with concurrent upserts.
async fn remove_still_stale_covers(stale_owners: Vec<DbId>) {
    let mut seen = HashSet::new();
    let mut db_write = STATE.db.write().await;
    for owner_id in stale_owners {
        if !seen.insert(owner_id) {
            continue;
        }
        let Ok(Some(cover)) = db::covers::get(&*db_write, owner_id) else {
            continue;
        };
        if matches!(check_cover_validity(cover), CoverValidity::NotFound) {
            let _ = db::covers::remove(&mut *db_write, owner_id);
        }
    }
}

fn cover_to_lua(lua: &Lua, owner_db_id: DbId, cover: Cover) -> Result<Value> {
    let table = lua.create_table()?;
    table.set("path", cover.path)?;
    table.set("mime_type", cover.mime_type)?;
    table.set("hash", cover.hash)?;
    table.set("blurhash", cover.blurhash)?;
    table.set("release_id", owner_db_id.0)?;
    Ok(Value::Table(table))
}

struct CoversModule;

#[harmony_macros::module(
    plugin_scoped,
    name = "Covers",
    local = "covers",
    path = "lyra/covers",
    interfaces(CoverInfo)
)]
impl CoversModule {
    /// Returns a resolved cover for an entity.
    #[harmony(returns(Option<CoverInfo>))]
    pub(crate) async fn get(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        id: ResolveId,
    ) -> Result<Value> {
        let (resolved_cover, stale_owners) = {
            let db = STATE.db.read().await;
            let query_id = id.to_query_id(&db).into_lua_err()?;
            let Some(QueryId::Id(item_id)) = query_id else {
                return Ok(Value::Nil);
            };
            let mut stale_owners = Vec::new();
            let result = resolve_persisted_cover(&db, item_id, &mut stale_owners).into_lua_err()?;
            (result, stale_owners)
        };

        if !stale_owners.is_empty() {
            remove_still_stale_covers(stale_owners).await;
        }

        let Some((release_id, cover)) = resolved_cover else {
            return Ok(Value::Nil);
        };

        cover_to_lua(&lua, release_id, cover)
    }

    /// Returns resolved covers for many entities.
    #[harmony(args(ids: Vec<u64>), returns(std::collections::BTreeMap<u64, Option<CoverInfo>>))]
    pub(crate) async fn get_many(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        ids: Table,
    ) -> Result<Table> {
        let item_ids = parse_ids(ids)?;
        let (resolved, stale_owners) = {
            let db = STATE.db.read().await;
            let mut stale_owners = Vec::new();
            let result =
                resolve_persisted_covers(&db, &item_ids, &mut stale_owners).into_lua_err()?;
            (result, stale_owners)
        };

        if !stale_owners.is_empty() {
            remove_still_stale_covers(stale_owners).await;
        }

        let table = lua.create_table()?;
        for item_id in item_ids {
            let Some((owner_id, cover)) = resolved.get(&item_id).cloned() else {
                table.set(item_id.0, Value::Nil)?;
                continue;
            };
            table.set(item_id.0, cover_to_lua(&lua, owner_id, cover)?)?;
        }

        Ok(table)
    }
}

crate::plugins::plugin_surface_exports!(
    CoversModule,
    "lyra.covers",
    "Read and modify cover art.",
    Low
);

#[cfg(test)]
mod tests {
    use super::cover_to_lua;
    use crate::{
        db::Cover,
        plugins::parse_ids,
    };
    use agdb::DbId;
    use mlua::Value;

    #[test]
    fn parse_ids_deduplicates_positive_ids_for_cover_queries() {
        let lua = mlua::Lua::new();
        let ids = lua.create_table().unwrap();
        ids.set(1, 4).unwrap();
        ids.set(2, 0).unwrap();
        ids.set(3, 4).unwrap();
        ids.set(4, 9).unwrap();

        assert_eq!(parse_ids(ids).unwrap(), vec![DbId(4), DbId(9)]);
    }

    #[test]
    fn cover_to_lua_sets_release_owner_id() {
        let lua = mlua::Lua::new();
        let value = cover_to_lua(
            &lua,
            DbId(12),
            Cover {
                db_id: None,
                id: "cover-1".to_string(),
                path: "cover.jpg".to_string(),
                mime_type: "image/jpeg".to_string(),
                hash: "abc123".to_string(),
                blurhash: Some("blurhash".to_string()),
            },
        )
        .unwrap();

        let Value::Table(table) = value else {
            panic!("cover_to_lua did not return a table");
        };
        assert_eq!(table.get::<i64>("release_id").unwrap(), 12);
        assert_eq!(table.get::<String>("path").unwrap(), "cover.jpg");
    }
}
