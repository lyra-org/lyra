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
    LuaSerdeExt,
    Result,
    Table,
    Value,
};
use serde::Serialize;

use crate::{
    STATE,
    db::{
        self,
        NodeId,
    },
    plugins::parse_ids,
    services::tags as tag_service,
};

/// `lyra/tags` plugin bindings. Plugins are fully trusted — callers must scope to the request
/// principal; the host does not verify `user_id`. Tag names are normalized via
/// [`crate::db::tags::normalize_tag_name`]; return values use the canonical form.
struct TagsModule;

#[derive(Debug, Serialize)]
#[harmony_macros::interface]
struct TagInfo {
    db_id: Option<NodeId>,
    id: String,
    tag: String,
    color: String,
    created_at_ms: i64,
}

fn tag_to_info(tag: db::Tag) -> TagInfo {
    TagInfo {
        db_id: tag.db_id,
        id: tag.id,
        tag: tag.tag,
        color: tag.color,
        created_at_ms: tag.created_at_ms,
    }
}

#[harmony_macros::module(
    plugin_scoped,
    name = "Tags",
    local = "tags",
    path = "lyra/tags",
    interfaces(TagInfo)
)]
impl TagsModule {
    /// Returns the canonical tag name. `color` is ignored on reuse.
    pub(crate) async fn add(
        _plugin_id: Option<Arc<str>>,
        user_id: NodeId,
        target_id: NodeId,
        tag: String,
        color: String,
    ) -> Result<String> {
        let user_db_id: DbId = user_id.into();
        let target_db_id: DbId = target_id.into();

        let mut db = STATE.db.write().await;
        let (_, canonical) =
            tag_service::create_by_db_id(&mut db, user_db_id, target_db_id, &tag, &color)
                .into_lua_err()?;
        Ok(canonical)
    }

    /// No visibility gate.
    pub(crate) async fn remove(
        _plugin_id: Option<Arc<str>>,
        user_id: NodeId,
        target_id: NodeId,
        tag: String,
    ) -> Result<()> {
        let user_db_id: DbId = user_id.into();
        let target_db_id: DbId = target_id.into();

        let mut db = STATE.db.write().await;
        tag_service::remove_target_by_db_id(&mut db, user_db_id, target_db_id, &tag).into_lua_err()
    }

    pub(crate) async fn has(
        _plugin_id: Option<Arc<str>>,
        user_id: NodeId,
        target_id: NodeId,
        tag: String,
    ) -> Result<bool> {
        let user_db_id: DbId = user_id.into();
        let target_db_id: DbId = target_id.into();

        let db = STATE.db.read().await;
        tag_service::has_target_by_db_id(&db, user_db_id, target_db_id, &tag).into_lua_err()
    }

    /// Batch check. Cap 1024.
    #[harmony(args(user_id: u64, target_ids: Vec<u64>, tag: String), returns(std::collections::BTreeMap<u64, bool>))]
    pub(crate) async fn has_many(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        user_id: NodeId,
        target_ids: Table,
        tag: String,
    ) -> Result<Table> {
        let user_db_id: DbId = user_id.into();
        let ids = parse_ids(target_ids)?;
        let result = {
            let db = STATE.db.read().await;
            tag_service::has_targets_by_db_id(&db, user_db_id, &ids, &tag).into_lua_err()?
        };

        let table = lua.create_table()?;
        for id in ids {
            let has = result.get(&id).copied().unwrap_or(false);
            table.set(id.0, has)?;
        }
        Ok(table)
    }

    #[harmony(returns(Vec<TagInfo>))]
    pub(crate) async fn get_for_target(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        user_id: NodeId,
        target_id: NodeId,
    ) -> Result<Value> {
        let user_db_id: DbId = user_id.into();
        let target_db_id: DbId = target_id.into();

        let db = STATE.db.read().await;
        let tags =
            tag_service::get_for_target_by_db_id(&db, user_db_id, target_db_id).into_lua_err()?;
        let infos: Vec<TagInfo> = tags.into_iter().map(tag_to_info).collect();
        lua.to_value_with(&infos, crate::plugins::LUA_SERIALIZE_OPTIONS)
    }

    #[harmony(args(user_id: u64, target_ids: Vec<u64>), returns(std::collections::BTreeMap<u64, Vec<TagInfo>>))]
    pub(crate) async fn get_for_targets_many(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        user_id: NodeId,
        target_ids: Table,
    ) -> Result<Table> {
        let user_db_id: DbId = user_id.into();
        let ids = parse_ids(target_ids)?;
        let result = {
            let db = STATE.db.read().await;
            tag_service::get_for_targets_many_by_db_id(&db, user_db_id, &ids).into_lua_err()?
        };

        let table = lua.create_table()?;
        for id in ids {
            let infos: Vec<TagInfo> = result
                .get(&id)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(tag_to_info)
                .collect();
            table.set(
                id.0,
                lua.to_value_with(&infos, crate::plugins::LUA_SERIALIZE_OPTIONS)?,
            )?;
        }
        Ok(table)
    }

    /// Errs above the server cap.
    pub(crate) async fn get_tagged(
        _plugin_id: Option<Arc<str>>,
        user_id: NodeId,
        tag: String,
    ) -> Result<Vec<NodeId>> {
        let user_db_id: DbId = user_id.into();
        let db = STATE.db.read().await;
        let (ids, _canonical) = tag_service::get_tagged(&db, user_db_id, &tag).into_lua_err()?;
        Ok(ids.into_iter().map(Into::into).collect())
    }
}

crate::plugins::plugin_surface_exports!(
    TagsModule,
    "lyra.tags",
    "Read and modify user-defined tags.",
    Medium
);
