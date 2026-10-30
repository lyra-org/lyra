// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
use std::collections::HashMap;

use agdb::DbId;
use harmony_luau as luau;
use lyra_db::favorites::FavoriteKind;

use crate::{
    plugins::{
        args::UniqueIds,
        db::Db,
    },
    services::{
        auth::Principal,
        favorites as favorite_service,
    },
};

pub(crate) struct FavoritesModule;

#[harmony_macros::module(id = "lyra/favorites", docs_feature = "docgen")]
impl FavoritesModule {
    async fn add(
        target_id: i64,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<bool> {
        let mut db = db.write().await;
        let outcome = favorite_service::add_for_principal(&mut db, &principal, DbId(target_id))?;
        Ok(outcome == favorite_service::MutationOutcome::Applied)
    }

    async fn remove(
        target_id: i64,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<bool> {
        let mut db = db.write().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let outcome = favorite_service::remove(&mut db, user_db_id, DbId(target_id))?;
        Ok(outcome == favorite_service::MutationOutcome::Applied)
    }

    async fn has(
        target_id: i64,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<bool> {
        let db = db.read().await;
        Ok(favorite_service::has_for_principal(
            &db,
            &principal,
            DbId(target_id),
        )?)
    }

    #[harmony(returns(HashMap<u64, bool>))]
    async fn has_many(
        target_ids: UniqueIds,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let target_ids = target_ids.0;
        let db = db.read().await;
        let favorited = favorite_service::favorited_for_principal(&db, &principal, &target_ids)?;

        let mut table = luau::OwnedTable::with_entry_capacity(0, 0, target_ids.len());
        for id in target_ids {
            table.set_key(id.0, favorited.contains(&id));
        }
        Ok(table)
    }

    #[harmony(returns(Vec<i64>))]
    async fn list_ids(
        entity: String,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let kind =
            FavoriteKind::try_from(entity.as_str()).map_err(crate::plugins::runtime_error)?;
        let db = db.read().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let ids = favorite_service::list_ids(&db, user_db_id, kind)?;
        let mut visible_ids = Vec::new();
        for id in ids {
            if kind == FavoriteKind::Playlist
                || crate::services::auth::access::entity_accessible(&db, &principal, id)?
            {
                visible_ids.push(id);
            }
        }
        Ok(luau::OwnedTable::from_array(
            visible_ids.into_iter().map(|id| id.0),
        ))
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    use harmony_luau::DescribeModule;

    harmony_luau::render_definition_file_with_support(
        &FavoritesModule::module_descriptor(),
        &[],
        &[],
        &[],
    )
}
