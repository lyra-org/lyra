// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    fmt,
    sync::Arc,
};

use agdb::DbId;
use harmony_core::{
    Danger,
    Module,
    ModuleBuilder,
    ModuleExport,
};
use harmony_luau::{
    DescribeInterface,
    ModuleFunctionDescriptor,
};
use mlua::{
    ExternalResult,
    Lua,
    LuaSerdeExt,
    Result,
    Table,
    Value,
};
use serde::Serialize;

use crate::{
    STATE,
    plugins::db::{
        self,
        NodeId,
    },
    plugins::parse_ids,
    services::tags as tag_service,
};

/// `lyra/tags` plugin bindings. Plugins are fully trusted — callers must scope to the request
/// principal; the host does not verify `user_id`. Tag names are normalized via
/// [`crate::plugins::db::tags::normalize_tag_name`]; return values use the canonical form.
#[derive(Debug, Serialize)]
#[harmony_macros::interface]
struct TagInfo {
    db_id: Option<NodeId>,
    id: String,
    tag: String,
    color: String,
    created_at_ms: i64,
}

fn tag_to_info(tag: db::Tag) -> TagInfo {
    TagInfo {
        db_id: tag.db_id,
        id: tag.id,
        tag: tag.tag,
        color: tag.color,
        created_at_ms: tag.created_at_ms,
    }
}

async fn add(
    _plugin_id: Option<Arc<str>>,
    user_id: NodeId,
    target_id: NodeId,
    tag: String,
    color: String,
) -> Result<String> {
    let user_db_id: DbId = user_id.into();
    let target_db_id: DbId = target_id.into();

    let mut db = STATE.db.write().await;
    let (_, canonical) =
        tag_service::create_by_db_id(&mut db, user_db_id, target_db_id, &tag, &color)
            .into_lua_err()?;
    Ok(canonical)
}

async fn remove(
    _plugin_id: Option<Arc<str>>,
    user_id: NodeId,
    target_id: NodeId,
    tag: String,
) -> Result<()> {
    let user_db_id: DbId = user_id.into();
    let target_db_id: DbId = target_id.into();

    let mut db = STATE.db.write().await;
    tag_service::remove_target_by_db_id(&mut db, user_db_id, target_db_id, &tag).into_lua_err()
}

async fn has(
    _plugin_id: Option<Arc<str>>,
    user_id: NodeId,
    target_id: NodeId,
    tag: String,
) -> Result<bool> {
    let user_db_id: DbId = user_id.into();
    let target_db_id: DbId = target_id.into();

    let db = STATE.db.read().await;
    tag_service::has_target_by_db_id(&db, user_db_id, target_db_id, &tag).into_lua_err()
}

async fn has_many(
    lua: Lua,
    _plugin_id: Option<Arc<str>>,
    user_id: NodeId,
    target_ids: Table,
    tag: String,
) -> Result<Table> {
    let user_db_id: DbId = user_id.into();
    let ids = parse_ids(target_ids)?;
    let result = {
        let db = STATE.db.read().await;
        tag_service::has_targets_by_db_id(&db, user_db_id, &ids, &tag).into_lua_err()?
    };

    let table = lua.create_table()?;
    for id in ids {
        let has = result.get(&id).copied().unwrap_or(false);
        table.set(id.0, has)?;
    }
    Ok(table)
}

async fn get_for_target(
    lua: Lua,
    _plugin_id: Option<Arc<str>>,
    user_id: NodeId,
    target_id: NodeId,
) -> Result<Value> {
    let user_db_id: DbId = user_id.into();
    let target_db_id: DbId = target_id.into();

    let db = STATE.db.read().await;
    let tags =
        tag_service::get_for_target_by_db_id(&db, user_db_id, target_db_id).into_lua_err()?;
    let infos: Vec<TagInfo> = tags.into_iter().map(tag_to_info).collect();
    lua.to_value_with(&infos, crate::plugins::LUA_SERIALIZE_OPTIONS)
}

async fn get_for_targets_many(
    lua: Lua,
    _plugin_id: Option<Arc<str>>,
    user_id: NodeId,
    target_ids: Table,
) -> Result<Table> {
    let user_db_id: DbId = user_id.into();
    let ids = parse_ids(target_ids)?;
    let result = {
        let db = STATE.db.read().await;
        tag_service::get_for_targets_many_by_db_id(&db, user_db_id, &ids).into_lua_err()?
    };

    let table = lua.create_table()?;
    for id in ids {
        let infos: Vec<TagInfo> = result
            .get(&id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(tag_to_info)
            .collect();
        table.set(
            id.0,
            lua.to_value_with(&infos, crate::plugins::LUA_SERIALIZE_OPTIONS)?,
        )?;
    }
    Ok(table)
}

async fn get_tagged(
    _plugin_id: Option<Arc<str>>,
    user_id: NodeId,
    tag: String,
) -> Result<Vec<NodeId>> {
    let user_db_id: DbId = user_id.into();
    let db = STATE.db.read().await;
    let (ids, _canonical) = tag_service::get_tagged(&db, user_db_id, &tag).into_lua_err()?;
    Ok(ids.into_iter().map(Into::into).collect())
}

fn module_export() -> ModuleExport {
    ModuleBuilder::empty("lyra/tags", "Tags", "tags")
        .scope_id(
            "lyra.tags",
            "Read and modify user-defined tags.",
            Danger::Medium,
        )
        .with_interface(TagInfo::interface_descriptor())
        .async_function_with_prelude(
            ModuleFunctionDescriptor::new("add")
                .description("Returns the canonical tag name. `color` is ignored on reuse.")
                .param_type::<NodeId>("user_id")
                .param_type::<NodeId>("target_id")
                .param_type::<String>("tag")
                .param_type::<String>("color")
                .returns::<String>()
                .yields(),
            |lua| harmony_core::resolve_caller(lua),
            |_, plugin_id, (user_id, target_id, tag, color)| async move {
                add(plugin_id, user_id, target_id, tag, color).await
            },
        )
        .async_function_with_prelude(
            ModuleFunctionDescriptor::new("remove")
                .description("No visibility gate.")
                .param_type::<NodeId>("user_id")
                .param_type::<NodeId>("target_id")
                .param_type::<String>("tag")
                .returns::<()>()
                .yields(),
            |lua| harmony_core::resolve_caller(lua),
            |_, plugin_id, (user_id, target_id, tag)| async move {
                remove(plugin_id, user_id, target_id, tag).await
            },
        )
        .async_function_with_prelude(
            ModuleFunctionDescriptor::new("has")
                .param_type::<NodeId>("user_id")
                .param_type::<NodeId>("target_id")
                .param_type::<String>("tag")
                .returns::<bool>()
                .yields(),
            |lua| harmony_core::resolve_caller(lua),
            |_, plugin_id, (user_id, target_id, tag)| async move {
                has(plugin_id, user_id, target_id, tag).await
            },
        )
        .async_function_with_prelude(
            ModuleFunctionDescriptor::new("has_many")
                .description("Batch check. Cap 1024.")
                .param_type::<u64>("user_id")
                .param_type::<Vec<u64>>("target_ids")
                .param_type::<String>("tag")
                .returns::<std::collections::BTreeMap<u64, bool>>()
                .yields(),
            |lua| harmony_core::resolve_caller(lua),
            |lua, plugin_id, (user_id, target_ids, tag)| async move {
                has_many(lua, plugin_id, user_id, target_ids, tag).await
            },
        )
        .async_function_with_prelude(
            ModuleFunctionDescriptor::new("get_for_target")
                .param_type::<NodeId>("user_id")
                .param_type::<NodeId>("target_id")
                .returns::<Vec<TagInfo>>()
                .yields(),
            |lua| harmony_core::resolve_caller(lua),
            |lua, plugin_id, (user_id, target_id)| async move {
                get_for_target(lua, plugin_id, user_id, target_id).await
            },
        )
        .async_function_with_prelude(
            ModuleFunctionDescriptor::new("get_for_targets_many")
                .param_type::<u64>("user_id")
                .param_type::<Vec<u64>>("target_ids")
                .returns::<std::collections::BTreeMap<u64, Vec<TagInfo>>>()
                .yields(),
            |lua| harmony_core::resolve_caller(lua),
            |lua, plugin_id, (user_id, target_ids)| async move {
                get_for_targets_many(lua, plugin_id, user_id, target_ids).await
            },
        )
        .async_function_with_prelude(
            ModuleFunctionDescriptor::new("get_tagged")
                .description("Errs above the server cap.")
                .param_type::<NodeId>("user_id")
                .param_type::<String>("tag")
                .returns::<Vec<NodeId>>()
                .yields(),
            |lua| harmony_core::resolve_caller(lua),
            |_, plugin_id, (user_id, tag)| async move { get_tagged(plugin_id, user_id, tag).await },
        )
        .build()
}

pub(crate) fn get_module() -> Module {
    module_export().into_module()
}

pub(crate) fn render_luau_definition() -> std::result::Result<String, fmt::Error> {
    module_export().render_luau_definition()
}
