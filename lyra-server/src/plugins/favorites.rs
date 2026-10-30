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
        let Some(public_target_id) = lyra_db::lookup::find_id_by_db_id(&*db, DbId(target_id))?
        else {
            return Ok(false);
        };
        let outcome = favorite_service::add_for_principal(&mut db, &principal, &public_target_id)?;
        Ok(matches!(
            outcome,
            favorite_service::MutationOutcome::Applied(_)
        ))
    }

    async fn remove(
        target_id: i64,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<bool> {
        let mut db = db.write().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let outcome = favorite_service::remove_by_db_id(&mut db, user_db_id, DbId(target_id))?;
        Ok(matches!(
            outcome,
            favorite_service::MutationOutcome::Applied(_)
        ))
    }

    async fn has(
        target_id: i64,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<bool> {
        let db = db.read().await;
        let Some(public_target_id) = lyra_db::lookup::find_id_by_db_id(&*db, DbId(target_id))?
        else {
            return Ok(false);
        };
        Ok(favorite_service::has_for_principal(
            &db,
            &principal,
            &public_target_id,
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
        let public_ids_by_db_id = lyra_db::lookup::find_ids_by_db_ids(&*db, &target_ids)?;
        let public_ids = target_ids
            .iter()
            .filter_map(|id| public_ids_by_db_id.get(id).cloned())
            .collect::<Vec<_>>();
        let result = favorite_service::has_many_for_principal(&db, &principal, &public_ids)?;

        let mut table = luau::OwnedTable::with_entry_capacity(0, 0, target_ids.len());
        for id in target_ids {
            let favored = public_ids_by_db_id
                .get(&id)
                .and_then(|public_id| result.get(public_id).copied())
                .unwrap_or(false);
            table.set_key(id.0, favored);
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
