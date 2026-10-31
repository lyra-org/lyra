// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
use std::collections::HashMap;

use harmony_luau as luau;
use lyra_db::ratings::{
    MAX_VALUE,
    MIN_VALUE,
    RatingValue,
};

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
        ratings as rating_service,
    },
};

pub(crate) struct RatingsModule;

#[harmony_macros::module(
    id = "lyra/ratings",
    description = "The dispatch principal's 1-5 ratings of tracks, releases and artists, and their averages across users.",
    docs_feature = "docgen"
)]
impl RatingsModule {
    #[harmony(
        description = "Sets the caller's rating of the target. Returns false when the target is not a track, release or artist the caller can see. Raises on a rating outside 1-5."
    )]
    async fn set(
        target_id: String,
        rating: i64,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<bool> {
        let value = u8::try_from(rating)
            .ok()
            .and_then(RatingValue::new)
            .ok_or_else(|| {
                crate::plugins::runtime_error(format!(
                    "rating must be an integer from {MIN_VALUE} to {MAX_VALUE}, got {rating}"
                ))
            })?;
        let mut db = db.write().await;
        let viewer = ids::viewer_of(&*db, &principal)?;
        let Some(target) = ids::visible_db_id(&db, &viewer, &target_id)? else {
            return Ok(false);
        };
        let outcome = rating_service::set_for_principal(&mut db, &principal, target, value)?;
        Ok(outcome == rating_service::MutationOutcome::Applied)
    }

    #[harmony(
        description = "Clears the caller's rating of the target. Returns false when it is not a track, release or artist the caller can see; a rating of one the caller can no longer see is still cleared, and still answers false."
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
        let outcome = rating_service::remove(&mut db, user_db_id, target)?;
        Ok(visible.is_some() && outcome == rating_service::MutationOutcome::Applied)
    }

    #[harmony(
        description = "The caller's ratings of the targets, keyed by id. Unrated targets and ones the caller cannot see are absent.",
        returns(HashMap<String, u8>)
    )]
    async fn get(
        target_ids: UniqueIds,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let db = db.read().await;
        let targets = Resolved::of(&*db, target_ids.0)?;
        let values = rating_service::values_for_principal(&db, &principal, targets.db_ids())?;
        Ok(targets.keyed(
            values
                .into_iter()
                .map(|(id, value)| (id, i64::from(value.get()))),
        ))
    }

    #[harmony(
        description = "Every user's average rating of the targets, keyed by id. Unrated targets, ones the caller cannot see, and every target while the server's instance statistics are off are absent.",
        returns(HashMap<String, f64>)
    )]
    async fn average(
        target_ids: UniqueIds,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let db = db.read().await;
        let targets = Resolved::of(&*db, target_ids.0)?;
        let averages = rating_service::averages_for_principal(&db, &principal, targets.db_ids())?;
        Ok(targets.keyed(averages))
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    use harmony_luau::DescribeModule;

    harmony_luau::render_definition_file_with_support(
        &RatingsModule::module_descriptor(),
        &[],
        &[],
        &[],
    )
}
