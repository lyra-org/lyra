// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
use std::collections::HashMap;

use harmony_luau as luau;
use lyra_db::favorites::FavoriteKind;

use crate::{
    plugins::{
        args::UniqueIds,
        db::Db,
        ids::{
            self,
            Resolved,
        },
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
        target_id: String,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<bool> {
        let mut db = db.write().await;
        let viewer = ids::viewer_of(&*db, &principal)?;
        let Some(target) = ids::visible_db_id(&db, &viewer, &target_id)? else {
            return Ok(false);
        };
        let outcome = favorite_service::add_for_principal(&mut db, &principal, target)?;
        Ok(outcome == favorite_service::MutationOutcome::Applied)
    }

    #[harmony(
        description = "Unfavorites the target. Returns false when it is not a track, release, artist or playlist the caller can see; a favorite of one the caller can no longer see is still removed, and still answers false."
    )]
    async fn remove(
        target_id: String,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<bool> {
        let mut db = db.write().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let Some(target) = ids::db_id(&*db, &target_id)? else {
            return Ok(false);
        };
        let visible = ids::visible_db_id(&db, &ids::viewer_of(&*db, &principal)?, &target_id)?;
        let outcome = favorite_service::remove(&mut db, user_db_id, target)?;
        Ok(visible.is_some() && outcome == favorite_service::MutationOutcome::Applied)
    }

    #[harmony(
        description = "When the caller first favorited each of the targets, in Unix milliseconds, keyed by id. Unfavorited targets and ones the caller cannot see are absent.",
        returns(HashMap<String, i64>)
    )]
    async fn get(
        target_ids: UniqueIds,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let db = db.read().await;
        let targets = Resolved::of(&*db, target_ids.0)?;
        let favorited =
            favorite_service::favorited_at_for_principal(&db, &principal, targets.db_ids())?;
        Ok(targets.keyed(favorited))
    }

    #[harmony(returns(Vec<String>))]
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
        ids::public_id_list(&*db, &visible_ids)
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
