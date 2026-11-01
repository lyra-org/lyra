// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.
#[cfg(feature = "docgen")]
use std::collections::HashMap;

use harmony_luau as luau;
use harmony_luau::{
    DescribeInterface,
    FieldDescriptor,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
};
use serde::Serialize;

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
        tags as tag_service,
    },
};
/// `lyra/tags` plugin bindings, scoped to the dispatch principal. Tag names are normalized via
/// [`lyra_db::tags::normalize_tag_name`]; return values use the canonical form.
pub(crate) struct TagsModule;

#[derive(Debug, Serialize)]
struct TagInfo {
    id: String,
    tag: String,
    color: String,
    created_at_ms: i64,
}

fn tag_to_info(tag: lyra_db::Tag) -> TagInfo {
    TagInfo {
        id: tag.id,
        tag: tag.tag,
        color: tag.color,
        created_at_ms: tag.created_at_ms,
    }
}

#[harmony_macros::module(id = "lyra/tags", docs_feature = "docgen")]
impl TagsModule {
    #[harmony(
        description = "Tags the target and returns the canonical tag name; color is ignored on reuse. Returns nil when the target is not a track, release, artist or playlist the caller can see."
    )]
    async fn add(
        target_id: String,
        tag: String,
        color: String,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<Option<String>> {
        let mut db = db.write().await;
        let user_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let tag =
            tag_service::NewTag::parse(&tag, &color).map_err(crate::plugins::runtime_error)?;
        let viewer = ids::viewer_of(&*db, &principal)?;
        let Some(target) = ids::visible_db_id(&db, &viewer, &target_id)? else {
            return Ok(None);
        };
        match tag_service::create(&mut db, user_id, target, &tag) {
            Ok(_) => Ok(Some(tag.into_name())),
            Err(tag_service::TagServiceError::NotTargetable) => Ok(None),
            Err(error) => Err(crate::plugins::runtime_error(error)),
        }
    }

    #[harmony(description = "Removes a tag from a target.")]
    async fn remove(
        target_id: String,
        tag: String,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<()> {
        let mut db = db.write().await;
        let user_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let Some(target) = ids::db_id(&*db, &target_id)? else {
            return Ok(());
        };
        tag_service::remove_target(&mut db, user_id, target, &tag)
            .map_err(crate::plugins::runtime_error)
    }

    #[harmony(description = "Returns whether a target has a tag.")]
    async fn has(
        target_id: String,
        tag: String,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<bool> {
        let db = db.read().await;
        let user_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let viewer = ids::viewer_of(&*db, &principal)?;
        let Some(target) = ids::visible_db_id(&db, &viewer, &target_id)? else {
            return Ok(false);
        };
        tag_service::has_target(&db, user_id, target, &tag).map_err(crate::plugins::runtime_error)
    }

    #[harmony(
        description = "Whether each of the targets has the tag, keyed by id. Targets that are missing, hidden from the caller, or not taggable are absent.",
        returns(HashMap<String, bool>)
    )]
    async fn has_many(
        target_ids: UniqueIds,
        tag: String,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let db = db.read().await;
        let user_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let targets = Resolved::of(&*db, target_ids.0)?;
        let result = tag_service::has_targets(&db, user_id, targets.db_ids(), &tag)
            .map_err(crate::plugins::runtime_error)?;
        let viewer = ids::viewer_of(&*db, &principal)?;
        Ok(targets.keyed(ids::seen_by(&db, &viewer, result)?))
    }

    #[harmony(
        description = "Returns tags for a target.",
        returns(Vec<TagInfo>)
    )]
    async fn get_for_target(
        target_id: String,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let db = db.read().await;
        let user_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let viewer = ids::viewer_of(&*db, &principal)?;
        let Some(target) = ids::visible_db_id(&db, &viewer, &target_id)? else {
            return Ok(luau::OwnedTable::new());
        };
        let tags = tag_service::get_for_target(&db, user_id, target)
            .map_err(crate::plugins::runtime_error)?;
        Ok(tag_info_array(tags.into_iter().map(tag_to_info).collect()))
    }

    #[harmony(
        description = "The tags of each of the targets, keyed by id. Targets that are missing, hidden from the caller, or not taggable are absent.",
        returns(HashMap<String, Vec<TagInfo>>)
    )]
    async fn get_for_targets_many(
        target_ids: UniqueIds,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let db = db.read().await;
        let user_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let targets = Resolved::of(&*db, target_ids.0)?;
        let result = tag_service::get_for_targets_many(&db, user_id, targets.db_ids())
            .map_err(crate::plugins::runtime_error)?;
        let viewer = ids::viewer_of(&*db, &principal)?;
        let result = ids::seen_by(&db, &viewer, result)?;
        Ok(targets.keyed(result.into_iter().map(|(id, tags)| {
            (
                id,
                tag_info_array(tags.into_iter().map(tag_to_info).collect()),
            )
        })))
    }

    #[harmony(
        description = "Returns target IDs tagged with the given tag.",
        returns(Vec<String>)
    )]
    async fn get_tagged(
        tag: String,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let db = db.read().await;
        let user_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let (tagged, _canonical) =
            tag_service::get_tagged(&db, user_id, &tag).map_err(crate::plugins::runtime_error)?;
        let viewer = ids::viewer_of(&*db, &principal)?;
        let tagged = viewer
            .visible(&db, tagged)
            .map_err(crate::plugins::runtime_error)?;
        ids::public_id_list(&*db, &tagged)
    }
}

fn tag_info_array(tags: Vec<TagInfo>) -> luau::OwnedTable {
    luau::OwnedTable::from_array(tags.into_iter().map(tag_info_table))
}

fn tag_info_table(tag: TagInfo) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(0, 4);
    table.set_field("id", tag.id);
    table.set_field("tag", tag.tag);
    table.set_field("color", tag.color);
    table.set_field("created_at_ms", tag.created_at_ms);
    table
}

impl LuauTypeInfo for TagInfo {
    fn luau_type() -> LuauType {
        LuauType::literal("TagInfo")
    }
}

impl DescribeInterface for TagInfo {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("TagInfo", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "id",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "tag",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "color",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "created_at_ms",
                ty: i64::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    use harmony_luau::DescribeModule;

    harmony_luau::render_definition_file_with_support(
        &TagsModule::module_descriptor(),
        &[],
        &[TagInfo::interface_descriptor()],
        &[],
    )
}
