// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.
use std::collections::HashSet;
use std::sync::Arc;

use agdb::DbId;
use harmony_core::{
    FunctionSpec,
    ModuleExport,
    ModuleSpec,
};
use harmony_luau as luau;
use harmony_luau::{
    DescribeInterface,
    FieldDescriptor,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
};
#[cfg(any(feature = "docgen", test))]
use harmony_luau::{
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    render_definition_file_with_support,
};
use serde::Serialize;

use crate::{
    plugins::db::{
        self,
        NodeId,
    },
    services::tags as tag_service,
};

/// `lyra/tags` plugin bindings. Plugins are fully trusted — callers must scope to the request
/// principal; the host does not verify `user_id`. Tag names are normalized via
/// [`crate::plugins::db::tags::normalize_tag_name`]; return values use the canonical form.
struct TagsModule;

#[derive(Debug, Serialize)]
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

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/tags")
        .capability("lyra.tags")
        .function(add_spec())
        .function(remove_spec())
        .function(has_spec())
        .function(has_many_spec())
        .function(get_for_target_spec())
        .function(get_for_targets_many_spec())
        .function(get_tagged_spec())
        .install(|_| Ok(ModuleExport::new(TagsModule)))
}

fn add_spec() -> FunctionSpec {
    let spec = FunctionSpec::async_fn("add")
        .named_arg::<NodeId>("user_id")
        .named_arg::<NodeId>("target_id")
        .named_arg::<String>("tag")
        .named_arg::<String>("color")
        .returns::<String>();
    spec.call_async(Arc::new(add_callback))
}

fn remove_spec() -> FunctionSpec {
    let spec = FunctionSpec::async_fn("remove")
        .named_arg::<NodeId>("user_id")
        .named_arg::<NodeId>("target_id")
        .named_arg::<String>("tag");
    spec.call_async(Arc::new(remove_callback))
}

fn has_spec() -> FunctionSpec {
    let spec = FunctionSpec::async_fn("has")
        .named_arg::<NodeId>("user_id")
        .named_arg::<NodeId>("target_id")
        .named_arg::<String>("tag")
        .returns::<bool>();
    spec.call_async(Arc::new(has_callback))
}

fn has_many_spec() -> FunctionSpec {
    let spec = FunctionSpec::async_fn("has_many")
        .named_arg::<NodeId>("user_id")
        .named_arg::<Vec<u64>>("target_ids")
        .named_arg::<String>("tag")
        .returns::<std::collections::BTreeMap<u64, bool>>();
    spec.call_async(Arc::new(has_many_callback))
}

fn get_for_target_spec() -> FunctionSpec {
    let spec = FunctionSpec::async_fn("get_for_target")
        .named_arg::<NodeId>("user_id")
        .named_arg::<NodeId>("target_id")
        .returns::<Vec<TagInfo>>();
    spec.call_async(Arc::new(get_for_target_callback))
}

fn get_for_targets_many_spec() -> FunctionSpec {
    let spec = FunctionSpec::async_fn("get_for_targets_many")
        .named_arg::<NodeId>("user_id")
        .named_arg::<Vec<u64>>("target_ids")
        .returns::<std::collections::BTreeMap<u64, Vec<TagInfo>>>();
    spec.call_async(Arc::new(get_for_targets_many_callback))
}

fn get_tagged_spec() -> FunctionSpec {
    let spec = FunctionSpec::async_fn("get_tagged")
        .named_arg::<NodeId>("user_id")
        .named_arg::<String>("tag")
        .returns::<Vec<NodeId>>();
    spec.call_async(Arc::new(get_tagged_callback))
}
fn add_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let user_id = read_db_id_arg(&mut frame.args, "user_id")?;
    let target_id = read_db_id_arg(&mut frame.args, "target_id")?;
    let tag: String = frame.args.read_named("tag")?;
    let color: String = frame.args.read_named("color")?;
    let store = frame.vm.data().get::<TagsModuleStore>()?.as_ref().clone();
    Ok(luau::ScheduledFuture::new(async move {
        let canonical = store.add(user_id, target_id, tag, color).await?;
        Ok(luau::Value::String(canonical.into_bytes()))
    }))
}
fn remove_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let user_id = read_db_id_arg(&mut frame.args, "user_id")?;
    let target_id = read_db_id_arg(&mut frame.args, "target_id")?;
    let tag: String = frame.args.read_named("tag")?;
    let store = frame.vm.data().get::<TagsModuleStore>()?.as_ref().clone();
    Ok(luau::ScheduledFuture::new(async move {
        store.remove(user_id, target_id, tag).await?;
        Ok(())
    }))
}
fn has_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let user_id = read_db_id_arg(&mut frame.args, "user_id")?;
    let target_id = read_db_id_arg(&mut frame.args, "target_id")?;
    let tag: String = frame.args.read_named("tag")?;
    let store = frame.vm.data().get::<TagsModuleStore>()?.as_ref().clone();
    Ok(luau::ScheduledFuture::new(async move {
        let has_tag = store.has(user_id, target_id, tag).await?;
        Ok(luau::Value::Boolean(has_tag))
    }))
}
fn has_many_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let user_id = read_db_id_arg(&mut frame.args, "user_id")?;
    let target_ids: luau::Table = frame.args.read_named("target_ids")?;
    let target_ids = parse_db_ids(frame.vm, &target_ids)?;
    let tag: String = frame.args.read_named("tag")?;
    let store = frame.vm.data().get::<TagsModuleStore>()?.as_ref().clone();
    Ok(luau::ScheduledFuture::new(async move {
        let result = store.has_many(user_id, target_ids.clone(), tag).await?;
        let mut table = luau::OwnedTable::with_entry_capacity(0, 0, target_ids.len());
        for id in target_ids {
            table.set_key(
                luau::Value::Number(id.0 as f64),
                luau::Value::Boolean(result.get(&id).copied().unwrap_or(false)),
            );
        }
        Ok(luau::Value::TableData(table))
    }))
}
fn get_for_target_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let user_id = read_db_id_arg(&mut frame.args, "user_id")?;
    let target_id = read_db_id_arg(&mut frame.args, "target_id")?;
    let store = frame.vm.data().get::<TagsModuleStore>()?.as_ref().clone();
    Ok(luau::ScheduledFuture::new(async move {
        let tags = store.get_for_target(user_id, target_id).await?;
        Ok(luau::Value::TableData(tag_info_array(tags)))
    }))
}
fn get_for_targets_many_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let user_id = read_db_id_arg(&mut frame.args, "user_id")?;
    let target_ids: luau::Table = frame.args.read_named("target_ids")?;
    let target_ids = parse_db_ids(frame.vm, &target_ids)?;
    let store = frame.vm.data().get::<TagsModuleStore>()?.as_ref().clone();
    Ok(luau::ScheduledFuture::new(async move {
        let mut result = store
            .get_for_targets_many(user_id, target_ids.clone())
            .await?;
        let mut table = luau::OwnedTable::with_entry_capacity(0, 0, target_ids.len());
        for id in target_ids {
            table.set_key(
                luau::Value::Number(id.0 as f64),
                luau::Value::TableData(tag_info_array(result.remove(&id).unwrap_or_default())),
            );
        }
        Ok(luau::Value::TableData(table))
    }))
}
fn get_tagged_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let user_id = read_db_id_arg(&mut frame.args, "user_id")?;
    let tag: String = frame.args.read_named("tag")?;
    let store = frame.vm.data().get::<TagsModuleStore>()?.as_ref().clone();
    Ok(luau::ScheduledFuture::new(async move {
        let ids = store.get_tagged(user_id, tag).await?;
        Ok(luau::Value::TableData(db_id_array(ids)))
    }))
}
#[derive(Clone, Default)]
pub(crate) struct TagsModuleStore {
    db: Option<db::DbAsync>,
}
impl TagsModuleStore {
    pub(crate) fn empty() -> Self {
        Self { db: None }
    }

    pub(crate) fn with_db(db: db::DbAsync) -> Self {
        Self { db: Some(db) }
    }

    async fn add(
        &self,
        user_id: DbId,
        target_id: DbId,
        tag: String,
        color: String,
    ) -> luau::runtime::Result<String> {
        let db = self.db()?;
        let mut db = db.write().await;
        let (_, canonical) =
            tag_service::create_by_db_id(&mut db, user_id, target_id, &tag, &color)
                .map_err(crate::plugins::runtime_error)?;
        Ok(canonical)
    }

    async fn remove(
        &self,
        user_id: DbId,
        target_id: DbId,
        tag: String,
    ) -> luau::runtime::Result<()> {
        let db = self.db()?;
        let mut db = db.write().await;
        tag_service::remove_target_by_db_id(&mut db, user_id, target_id, &tag)
            .map_err(crate::plugins::runtime_error)
    }

    async fn has(
        &self,
        user_id: DbId,
        target_id: DbId,
        tag: String,
    ) -> luau::runtime::Result<bool> {
        let db = self.db()?;
        let db = db.read().await;
        tag_service::has_target_by_db_id(&db, user_id, target_id, &tag)
            .map_err(crate::plugins::runtime_error)
    }

    async fn has_many(
        &self,
        user_id: DbId,
        target_ids: Vec<DbId>,
        tag: String,
    ) -> luau::runtime::Result<std::collections::HashMap<DbId, bool>> {
        let db = self.db()?;
        let db = db.read().await;
        tag_service::has_targets_by_db_id(&db, user_id, &target_ids, &tag)
            .map_err(crate::plugins::runtime_error)
    }

    async fn get_for_target(
        &self,
        user_id: DbId,
        target_id: DbId,
    ) -> luau::runtime::Result<Vec<TagInfo>> {
        let db = self.db()?;
        let db = db.read().await;
        let tags = tag_service::get_for_target_by_db_id(&db, user_id, target_id)
            .map_err(crate::plugins::runtime_error)?;
        Ok(tags.into_iter().map(tag_to_info).collect())
    }

    async fn get_for_targets_many(
        &self,
        user_id: DbId,
        target_ids: Vec<DbId>,
    ) -> luau::runtime::Result<std::collections::HashMap<DbId, Vec<TagInfo>>> {
        let db = self.db()?;
        let db = db.read().await;
        let result = tag_service::get_for_targets_many_by_db_id(&db, user_id, &target_ids)
            .map_err(crate::plugins::runtime_error)?;
        Ok(result
            .into_iter()
            .map(|(id, tags)| (id, tags.into_iter().map(tag_to_info).collect()))
            .collect())
    }

    async fn get_tagged(&self, user_id: DbId, tag: String) -> luau::runtime::Result<Vec<DbId>> {
        let db = self.db()?;
        let db = db.read().await;
        let (ids, _canonical) =
            tag_service::get_tagged(&db, user_id, &tag).map_err(crate::plugins::runtime_error)?;
        Ok(ids)
    }

    fn db(&self) -> luau::runtime::Result<db::DbAsync> {
        self.db
            .clone()
            .ok_or_else(|| luau::Error::Runtime("lyra/tags database is unavailable".to_string()))
    }
}
fn tag_info_array(tags: Vec<TagInfo>) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(tags.len(), 0);
    for tag in tags {
        table.push_array(luau::Value::TableData(tag_info_table(tag)));
    }
    table
}
fn tag_info_table(tag: TagInfo) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(0, 5);
    table.set_field(
        "db_id",
        tag.db_id
            .map(|id| {
                let id: DbId = id.into();
                luau::Value::Integer(id.0)
            })
            .unwrap_or(luau::Value::Nil),
    );
    table.set_field("id", luau::Value::String(tag.id.into_bytes()));
    table.set_field("tag", luau::Value::String(tag.tag.into_bytes()));
    table.set_field("color", luau::Value::String(tag.color.into_bytes()));
    table.set_field("created_at_ms", luau::Value::Integer(tag.created_at_ms));
    table
}
fn db_id_array(ids: Vec<DbId>) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(ids.len(), 0);
    for id in ids {
        table.push_array(luau::Value::Integer(id.0));
    }
    table
}
fn read_db_id_arg(
    args: &mut luau::ArgReader<'_>,
    name: &'static str,
) -> luau::runtime::Result<DbId> {
    Ok(DbId(args.read_named::<i64>(name)?))
}
fn parse_db_ids(vm: &luau::Vm, table: &luau::Table) -> luau::runtime::Result<Vec<DbId>> {
    let mut entries = table
        .pairs_raw(vm)?
        .into_iter()
        .filter_map(|(key, value)| Some((sequence_index(key)?, integer_value(value)?)))
        .collect::<Vec<_>>();
    entries.sort_by_key(|(index, _)| *index);

    let mut ids = Vec::new();
    let mut seen = HashSet::new();
    for (_, id_value) in entries {
        if id_value <= 0 {
            continue;
        }
        let id = DbId(id_value);
        if seen.insert(id) {
            ids.push(id);
        }
    }
    Ok(ids)
}
fn sequence_index(value: luau::Value) -> Option<i64> {
    match value {
        luau::Value::Integer(value) if value > 0 => Some(value),
        luau::Value::Number(value) if value.is_finite() && value.fract() == 0.0 && value > 0.0 => {
            Some(value as i64)
        }
        _ => None,
    }
}
fn integer_value(value: luau::Value) -> Option<i64> {
    match value {
        luau::Value::Integer(value) => Some(value),
        luau::Value::Number(value) if value.is_finite() && value.fract() == 0.0 => {
            Some(value as i64)
        }
        _ => None,
    }
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
                ty: Option::<NodeId>::luau_type(),
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

#[cfg(any(feature = "docgen", test))]
fn param(name: &'static str, ty: LuauType) -> ParameterDescriptor {
    ParameterDescriptor {
        name,
        ty,
        description: None,
        variadic: false,
    }
}

#[cfg(any(feature = "docgen", test))]
fn module_descriptor() -> ModuleDescriptor {
    ModuleDescriptor {
        name: "Tags",
        local_name: "tags",
        description: None,
        fields: Vec::new(),
        functions: vec![
            ModuleFunctionDescriptor {
                path: vec!["add"],
                description: Some("Returns the canonical tag name. color is ignored on reuse."),
                params: vec![
                    param("user_id", NodeId::luau_type()),
                    param("target_id", NodeId::luau_type()),
                    param("tag", String::luau_type()),
                    param("color", String::luau_type()),
                ],
                returns: vec![String::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["remove"],
                description: Some("Removes a tag from a target."),
                params: vec![
                    param("user_id", NodeId::luau_type()),
                    param("target_id", NodeId::luau_type()),
                    param("tag", String::luau_type()),
                ],
                returns: Vec::new(),
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["has"],
                description: Some("Returns whether a target has a tag."),
                params: vec![
                    param("user_id", NodeId::luau_type()),
                    param("target_id", NodeId::luau_type()),
                    param("tag", String::luau_type()),
                ],
                returns: vec![bool::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["has_many"],
                description: Some("Batch check. Cap 1024."),
                params: vec![
                    param("user_id", NodeId::luau_type()),
                    param("target_ids", Vec::<u64>::luau_type()),
                    param("tag", String::luau_type()),
                ],
                returns: vec![LuauType::map(u64::luau_type(), bool::luau_type())],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_for_target"],
                description: Some("Returns tags for a target."),
                params: vec![
                    param("user_id", NodeId::luau_type()),
                    param("target_id", NodeId::luau_type()),
                ],
                returns: vec![Vec::<TagInfo>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_for_targets_many"],
                description: Some("Returns tags for many targets."),
                params: vec![
                    param("user_id", NodeId::luau_type()),
                    param("target_ids", Vec::<u64>::luau_type()),
                ],
                returns: vec![LuauType::map(u64::luau_type(), Vec::<TagInfo>::luau_type())],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_tagged"],
                description: Some("Returns target IDs tagged with the given tag."),
                params: vec![
                    param("user_id", NodeId::luau_type()),
                    param("tag", String::luau_type()),
                ],
                returns: vec![Vec::<NodeId>::luau_type()],
                yields: true,
            },
        ],
    }
}

#[cfg(any(feature = "docgen", test))]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &module_descriptor(),
        &[],
        &[TagInfo::interface_descriptor()],
        &[],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposes_handwritten_module_spec() {
        let spec = module_spec();

        assert_eq!(spec.id.0.as_ref(), "lyra/tags");
        assert_eq!(spec.capability.as_ref().unwrap().0.as_ref(), "lyra.tags");
        assert_eq!(spec.functions.len(), 7);
        assert!(spec.functions.iter().all(|function| function.yields));
    }

    #[test]
    fn renders_tags_module_definition() {
        let rendered = render_luau_definition().expect("render lyra/tags docs");

        assert!(rendered.contains("@class Tags"));
        assert!(rendered.contains("@interface TagInfo"));
        assert!(rendered.contains(
            "function tags.add(user_id: number, target_id: number, tag: string, color: string): string"
        ));
        assert!(rendered.contains(
            "function tags.has_many(user_id: number, target_ids: {number}, tag: string): { [number]: boolean }"
        ));
        assert!(rendered.contains(
            "function tags.get_for_targets_many(user_id: number, target_ids: {number}): { [number]: {TagInfo} }"
        ));
    }
}
