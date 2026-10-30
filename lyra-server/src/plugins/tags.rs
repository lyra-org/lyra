// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.
#[cfg(feature = "docgen")]
use std::collections::HashMap;

use agdb::DbId;
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
        types::LuauNodeId,
    },
    services::{
        auth::Principal,
        tags as tag_service,
    },
};
use lyra_db::NodeId;

/// `lyra/tags` plugin bindings, scoped to the dispatch principal. Tag names are normalized via
/// [`lyra_db::tags::normalize_tag_name`]; return values use the canonical form.
pub(crate) struct TagsModule;

#[derive(Debug, Serialize)]
struct TagInfo {
    db_id: Option<NodeId>,
    id: String,
    tag: String,
    color: String,
    created_at_ms: i64,
}

fn tag_to_info(tag: lyra_db::Tag) -> TagInfo {
    TagInfo {
        db_id: tag.db_id,
        id: tag.id,
        tag: tag.tag,
        color: tag.color,
        created_at_ms: tag.created_at_ms,
    }
}

/// A target id argument, taken as given.
struct TargetId(DbId);

impl<'vm> luau::FromLuau<'vm> for TargetId {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        Ok(Self(DbId(reader.read()?)))
    }
}

impl LuauTypeInfo for TargetId {
    fn luau_type() -> LuauType {
        LuauNodeId::luau_type()
    }
}

#[harmony_macros::module(id = "lyra/tags", docs_feature = "docgen")]
impl TagsModule {
    #[harmony(description = "Returns the canonical tag name. color is ignored on reuse.")]
    async fn add(
        target_id: TargetId,
        tag: String,
        color: String,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<String> {
        let mut db = db.write().await;
        let user_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let (_, canonical) =
            tag_service::create_by_db_id(&mut db, user_id, target_id.0, &tag, &color)
                .map_err(crate::plugins::runtime_error)?;
        Ok(canonical)
    }

    #[harmony(description = "Removes a tag from a target.")]
    async fn remove(
        target_id: TargetId,
        tag: String,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<()> {
        let mut db = db.write().await;
        let user_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        tag_service::remove_target_by_db_id(&mut db, user_id, target_id.0, &tag)
            .map_err(crate::plugins::runtime_error)
    }

    #[harmony(description = "Returns whether a target has a tag.")]
    async fn has(
        target_id: TargetId,
        tag: String,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<bool> {
        let db = db.read().await;
        let user_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        tag_service::has_target_by_db_id(&db, user_id, target_id.0, &tag)
            .map_err(crate::plugins::runtime_error)
    }

    #[harmony(
        description = "Batch check. Cap 1024.",
        returns(HashMap<u64, bool>)
    )]
    async fn has_many(
        target_ids: UniqueIds,
        tag: String,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let target_ids = target_ids.0;
        let db = db.read().await;
        let user_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let result = tag_service::has_targets_by_db_id(&db, user_id, &target_ids, &tag)
            .map_err(crate::plugins::runtime_error)?;
        Ok(luau::OwnedTable::from_entries(target_ids.into_iter().map(
            |id| (id.0, result.get(&id).copied().unwrap_or(false)),
        )))
    }

    #[harmony(
        description = "Returns tags for a target.",
        returns(Vec<TagInfo>)
    )]
    async fn get_for_target(
        target_id: TargetId,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let db = db.read().await;
        let user_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let tags = tag_service::get_for_target_by_db_id(&db, user_id, target_id.0)
            .map_err(crate::plugins::runtime_error)?;
        Ok(tag_info_array(tags.into_iter().map(tag_to_info).collect()))
    }

    #[harmony(
        description = "Returns tags for many targets.",
        returns(HashMap<u64, Vec<TagInfo>>)
    )]
    async fn get_for_targets_many(
        target_ids: UniqueIds,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let target_ids = target_ids.0;
        let db = db.read().await;
        let user_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let mut result = tag_service::get_for_targets_many_by_db_id(&db, user_id, &target_ids)
            .map_err(crate::plugins::runtime_error)?;
        Ok(luau::OwnedTable::from_entries(target_ids.into_iter().map(
            |id| {
                let tags = result.remove(&id).unwrap_or_default();
                (
                    id.0,
                    tag_info_array(tags.into_iter().map(tag_to_info).collect()),
                )
            },
        )))
    }

    #[harmony(
        description = "Returns target IDs tagged with the given tag.",
        returns(Vec<LuauNodeId>)
    )]
    async fn get_tagged(
        tag: String,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let db = db.read().await;
        let user_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let (ids, _canonical) =
            tag_service::get_tagged(&db, user_id, &tag).map_err(crate::plugins::runtime_error)?;
        Ok(luau::OwnedTable::from_array(ids.into_iter().map(|id| id.0)))
    }
}

fn tag_info_array(tags: Vec<TagInfo>) -> luau::OwnedTable {
    luau::OwnedTable::from_array(tags.into_iter().map(tag_info_table))
}

fn tag_info_table(tag: TagInfo) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(0, 5);
    table.set_field(
        "db_id",
        tag.db_id
            .map(|id| {
                let id: DbId = id.into();
                luau::Value::from(id.0)
            })
            .unwrap_or(luau::Value::Nil),
    );
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
                name: "db_id",
                ty: Option::<crate::plugins::types::LuauNodeId>::luau_type(),
                description: None,
            },
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
