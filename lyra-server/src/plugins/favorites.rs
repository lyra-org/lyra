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
};

use crate::{
    STATE,
    db::{
        NodeId,
        favorites::FavoriteKind,
    },
    plugins::parse_ids,
    services::favorites as favorite_service,
};

/// `lyra/favorites` plugin bindings. Plugins are fully trusted — callers must scope to the
/// request principal; the host does not verify `user_id`.
struct FavoritesModule;

fn parse_kind(value: &str) -> Result<FavoriteKind> {
    FavoriteKind::try_from(value).map_err(|err| mlua::Error::runtime(err.to_string()))
}

#[harmony_macros::module(
    plugin_scoped,
    name = "Favorites",
    local = "favorites",
    path = "lyra/favorites"
)]
impl FavoritesModule {
    pub(crate) async fn add(
        _plugin_id: Option<Arc<str>>,
        user_id: NodeId,
        target_id: NodeId,
    ) -> Result<bool> {
        let user_db_id: DbId = user_id.into();
        let target_db_id: DbId = target_id.into();

        let mut db = STATE.db.write().await;
        let outcome =
            favorite_service::add_by_db_id(&mut db, user_db_id, target_db_id).into_lua_err()?;
        Ok(matches!(
            outcome,
            favorite_service::MutationOutcome::Applied(_)
        ))
    }

    pub(crate) async fn remove(
        _plugin_id: Option<Arc<str>>,
        user_id: NodeId,
        target_id: NodeId,
    ) -> Result<bool> {
        let user_db_id: DbId = user_id.into();
        let target_db_id: DbId = target_id.into();

        let mut db = STATE.db.write().await;
        let outcome =
            favorite_service::remove_by_db_id(&mut db, user_db_id, target_db_id).into_lua_err()?;
        Ok(matches!(
            outcome,
            favorite_service::MutationOutcome::Applied(_)
        ))
    }

    pub(crate) async fn has(
        _plugin_id: Option<Arc<str>>,
        user_id: NodeId,
        target_id: NodeId,
    ) -> Result<bool> {
        let user_db_id: DbId = user_id.into();
        let target_db_id: DbId = target_id.into();

        let db = STATE.db.read().await;
        favorite_service::has_by_db_id(&db, user_db_id, target_db_id).into_lua_err()
    }

    /// Batch check. Cap 1024.
    #[harmony(args(user_id: u64, target_ids: Vec<u64>), returns(std::collections::BTreeMap<u64, bool>))]
    pub(crate) async fn has_many(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        user_id: NodeId,
        target_ids: Table,
    ) -> Result<Table> {
        let user_db_id: DbId = user_id.into();
        let ids = parse_ids(target_ids)?;
        let result = {
            let db = STATE.db.read().await;
            favorite_service::has_many_by_db_id(&db, user_db_id, &ids).into_lua_err()?
        };

        let table = lua.create_table()?;
        for id in ids {
            let favored = result.get(&id).copied().unwrap_or(false);
            table.set(id.0, favored)?;
        }
        Ok(table)
    }

    /// Errs above the server cap.
    pub(crate) async fn list_ids(
        _plugin_id: Option<Arc<str>>,
        user_id: NodeId,
        entity: String,
    ) -> Result<Vec<NodeId>> {
        let user_db_id: DbId = user_id.into();
        let kind = parse_kind(&entity)?;

        let db = STATE.db.read().await;
        let ids = favorite_service::list_ids(&db, user_db_id, kind).into_lua_err()?;
        Ok(ids.into_iter().map(Into::into).collect())
    }
}

crate::plugins::plugin_surface_exports!(
    FavoritesModule,
    "lyra.favorites",
    "Read and modify the current user's favorited items.",
    Medium
);
