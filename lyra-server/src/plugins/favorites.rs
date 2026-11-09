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
        favorites::{
            self as favorite_service,
            FavoriteUpdatePayload,
        },
    },
};

pub(crate) struct FavoritesModule;

crate::plugins::update_callbacks::update_handler!(
    FavoriteUpdateHandler,
    "FavoriteUpdatePayload",
    "Favorite update callback."
);

/// [`FavoriteUpdatePayload`] as described to Luau.
#[cfg(feature = "docgen")]
struct LuauFavoriteUpdatePayload;

#[cfg(feature = "docgen")]
impl harmony_luau::DescribeInterface for LuauFavoriteUpdatePayload {
    fn interface_descriptor() -> harmony_luau::InterfaceDescriptor {
        use harmony_luau::{
            FieldDescriptor,
            LuauType,
            LuauTypeInfo,
        };

        harmony_luau::InterfaceDescriptor {
            name: "FavoriteUpdatePayload",
            description: Some("One change to one user's favorites."),
            fields: vec![
                FieldDescriptor {
                    name: "event",
                    ty: LuauType::literal("\"added\" | \"removed\""),
                    description: Some(
                        "Whether the user favorited the target, anew or again, or unfavorited it.",
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
                    ty: LuauType::literal("\"track\" | \"release\" | \"artist\" | \"playlist\""),
                    description: Some("What the target is."),
                },
            ],
        }
    }
}

const _: fn(&FavoriteUpdatePayload) = |value| {
    let FavoriteUpdatePayload {
        event: _,
        user_id: _,
        target_id: _,
        target_type: _,
    } = value;
};

#[harmony_macros::module(
    id = "lyra/favorites",
    description = "The dispatch principal's favorite tracks, releases, artists and playlists, and every user's favorites as they change: `on_update` delivers all users' changes, not only the caller's, and needs the `lyra.favorites.all` scope too.",
    docs_feature = "docgen"
)]
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
        crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let Some(target) = ids::db_id(&*db, &target_id)? else {
            return Ok(false);
        };
        let outcome = favorite_service::remove_for_principal(&mut db, &principal, target)?;
        Ok(outcome == favorite_service::MutationOutcome::Applied)
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
        let viewer = ids::viewer_of(&*db, &principal)?;
        let favorited = favorite_service::favorited_at_seen_by(&db, &viewer, targets.db_ids())?;
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
        let viewer = ids::viewer_of(&*db, &principal)?;
        let mut visible_ids = Vec::new();
        for id in ids {
            let visible = if kind == FavoriteKind::Playlist {
                viewer.sees_playlist(&*db, id)?
            } else {
                crate::services::auth::access::entity_accessible(&db, &principal, id)?
            };
            if visible {
                visible_ids.push(id);
            }
        }
        ids::public_id_list(&*db, &visible_ids)
    }

    #[harmony(
        description = "Registers a callback for every user's favorite changes made through `add` and `remove`, by any plugin or the REST API. An update follows the commit, also when `add` favorites a target again; a refused call, or a `remove` of what was no favorite, sends none. Favorites that move when entities merge, or go when their target or user is deleted, send none. The target may be one the plugin's own callers cannot see. Delivery is best effort: updates are dropped while the plugin runtime is saturated. Needs the `lyra.favorites.all` scope too.",
        args(handler: FavoriteUpdateHandler)
    )]
    fn on_update(
        vm: &luau::Vm,
        context: &luau::CallContext,
        handler: luau::Function,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        crate::plugins::update_callbacks::register::<FavoriteUpdatePayload>(
            vm,
            context,
            handler,
            "favorites.on_update",
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
        &FavoritesModule::module_descriptor(),
        &[FavoriteUpdateHandler::type_alias_descriptor()],
        &[LuauFavoriteUpdatePayload::interface_descriptor()],
        &[],
    )
}
