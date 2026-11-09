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
        ratings::{
            self as rating_service,
            RatingUpdatePayload,
        },
    },
};

pub(crate) struct RatingsModule;

crate::plugins::update_callbacks::update_handler!(
    RatingUpdateHandler,
    "RatingUpdatePayload",
    "Rating update callback."
);

/// [`RatingUpdatePayload`] as described to Luau.
#[cfg(feature = "docgen")]
struct LuauRatingUpdatePayload;

#[cfg(feature = "docgen")]
impl harmony_luau::DescribeInterface for LuauRatingUpdatePayload {
    fn interface_descriptor() -> harmony_luau::InterfaceDescriptor {
        use harmony_luau::{
            FieldDescriptor,
            LuauType,
            LuauTypeInfo,
        };

        harmony_luau::InterfaceDescriptor {
            name: "RatingUpdatePayload",
            description: Some("One change to one user's ratings."),
            fields: vec![
                FieldDescriptor {
                    name: "event",
                    ty: LuauType::literal("\"set\" | \"removed\""),
                    description: Some(
                        "Whether the user rated the target, anew or again, or cleared its rating.",
                    ),
                },
                FieldDescriptor {
                    name: "user_id",
                    ty: String::luau_type(),
                    description: Some("The user's public id."),
                },
                FieldDescriptor {
                    name: "target_id",
                    ty: String::luau_type(),
                    description: Some("The target's public id."),
                },
                FieldDescriptor {
                    name: "target_type",
                    ty: LuauType::literal("\"track\" | \"release\" | \"artist\""),
                    description: Some("What the target is."),
                },
                FieldDescriptor {
                    name: "rating",
                    ty: Option::<u8>::luau_type(),
                    description: Some("The rating a `set` left, 1-5. Absent for `removed`."),
                },
            ],
        }
    }
}

const _: fn(&RatingUpdatePayload) = |value| {
    let RatingUpdatePayload {
        event: _,
        user_id: _,
        target_id: _,
        target_type: _,
        rating: _,
    } = value;
};

#[harmony_macros::module(
    id = "lyra/ratings",
    description = "The dispatch principal's 1-5 ratings of tracks, releases and artists, their averages across users, and every user's ratings as they change: `on_update` delivers all users' changes, not only the caller's, and needs the `lyra.ratings.all` scope too.",
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
        crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let Some(target) = ids::db_id(&*db, &target_id)? else {
            return Ok(false);
        };
        let outcome = rating_service::remove_for_principal(&mut db, &principal, target)?;
        Ok(outcome == rating_service::MutationOutcome::Applied)
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

    #[harmony(
        description = "Registers a callback for every user's rating changes made through `set` and `remove`, by any plugin or the REST API. An update follows the commit, also when `set` leaves the same rating; a refused call, or a `remove` of what had no rating, sends none. Ratings that move when entities merge, or go when their target or user is deleted, send none. The target may be one the plugin's own callers cannot see. Delivery is best effort: updates are dropped while the plugin runtime is saturated. Needs the `lyra.ratings.all` scope too.",
        args(handler: RatingUpdateHandler)
    )]
    fn on_update(
        vm: &luau::Vm,
        context: &luau::CallContext,
        handler: luau::Function,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        crate::plugins::update_callbacks::register::<RatingUpdatePayload>(
            vm,
            context,
            handler,
            "ratings.on_update",
        )
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    use harmony_luau::{
        DescribeInterface,
        DescribeModule,
        DescribeTypeAlias,
    };

    harmony_luau::render_definition_file_with_support(
        &RatingsModule::module_descriptor(),
        &[RatingUpdateHandler::type_alias_descriptor()],
        &[LuauRatingUpdatePayload::interface_descriptor()],
        &[],
    )
}
