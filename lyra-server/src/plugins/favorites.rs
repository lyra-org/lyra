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
};

use crate::{
    STATE,
    plugins::db::{
        self,
        NodeId,
        favorites::FavoriteKind,
    },
    plugins::{
        caller::RequestCaller,
        parse_ids,
    },
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
        #[harmony_context] caller: RequestCaller,
        user_id: NodeId,
        target_id: NodeId,
    ) -> Result<bool> {
        if DbId::from(user_id) != caller.principal.user_db_id {
            return Ok(false);
        }
        let target_db_id: DbId = target_id.into();

        let mut db = STATE.db.write().await;
        let Some(target_public_id) =
            db::lookup::find_id_by_db_id(&*db, target_db_id).into_lua_err()?
        else {
            return Ok(false);
        };
        let outcome =
            favorite_service::add_for_principal(&mut db, &caller.principal, &target_public_id)
                .into_lua_err()?;
        Ok(matches!(
            outcome,
            favorite_service::MutationOutcome::Applied(_)
        ))
    }

    pub(crate) async fn remove(
        #[harmony_context] caller: RequestCaller,
        user_id: NodeId,
        target_id: NodeId,
    ) -> Result<bool> {
        if DbId::from(user_id) != caller.principal.user_db_id {
            return Ok(false);
        }
        let target_db_id: DbId = target_id.into();

        let mut db = STATE.db.write().await;
        let outcome =
            favorite_service::remove_by_db_id(&mut db, caller.principal.user_db_id, target_db_id)
                .into_lua_err()?;
        Ok(matches!(
            outcome,
            favorite_service::MutationOutcome::Applied(_)
        ))
    }

    pub(crate) async fn has(
        #[harmony_context] caller: RequestCaller,
        user_id: NodeId,
        target_id: NodeId,
    ) -> Result<bool> {
        if DbId::from(user_id) != caller.principal.user_db_id {
            return Ok(false);
        }
        let target_db_id: DbId = target_id.into();

        let db = STATE.db.read().await;
        let Some(target_public_id) =
            db::lookup::find_id_by_db_id(&*db, target_db_id).into_lua_err()?
        else {
            return Ok(false);
        };
        favorite_service::has_for_principal(&db, &caller.principal, &target_public_id)
            .into_lua_err()
    }

    /// Batch check. Cap 1024.
    #[harmony(args(user_id: u64, target_ids: Vec<u64>), returns(std::collections::BTreeMap<u64, bool>))]
    pub(crate) async fn has_many(
        lua: Lua,
        #[harmony_context] caller: RequestCaller,
        user_id: NodeId,
        target_ids: Table,
    ) -> Result<Table> {
        let ids = parse_ids(target_ids)?;
        let db = STATE.db.read().await;
        let result = if DbId::from(user_id) == caller.principal.user_db_id {
            let public_ids_by_db_id = db::lookup::find_ids_by_db_ids(&*db, &ids).into_lua_err()?;
            let public_ids = ids
                .iter()
                .filter_map(|id| public_ids_by_db_id.get(id).cloned())
                .collect::<Vec<_>>();
            favorite_service::has_many_for_principal(&db, &caller.principal, &public_ids)
                .into_lua_err()?
        } else {
            std::collections::HashMap::new()
        };

        let table = lua.create_table()?;
        for id in ids {
            let favored = db::lookup::find_id_by_db_id(&*db, id)
                .into_lua_err()?
                .and_then(|public_id| result.get(&public_id).copied())
                .unwrap_or(false);
            table.set(id.0, favored)?;
        }
        Ok(table)
    }

    /// Errs above the server cap.
    pub(crate) async fn list_ids(
        #[harmony_context] caller: RequestCaller,
        user_id: NodeId,
        entity: String,
    ) -> Result<Vec<NodeId>> {
        if DbId::from(user_id) != caller.principal.user_db_id {
            return Ok(Vec::new());
        }
        let kind = parse_kind(&entity)?;

        let db = STATE.db.read().await;
        let ids =
            favorite_service::list_ids(&db, caller.principal.user_db_id, kind).into_lua_err()?;
        let mut visible_ids = Vec::new();
        for id in ids {
            if kind == FavoriteKind::Playlist
                || crate::routes::entity_accessible_to_principal(&db, &caller.principal, id)
                    .into_lua_err()?
            {
                visible_ids.push(id.into());
            }
        }
        Ok(visible_ids)
    }
}

crate::plugins::plugin_surface_exports!(
    FavoritesModule,
    "lyra.favorites",
    "Read and modify the current user's favorited items.",
    Medium
);
