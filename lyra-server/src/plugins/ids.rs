// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.
use std::{
    collections::HashSet,
    sync::Arc,
};

use harmony_core::{
    FunctionSpec,
    ModuleExport,
    ModuleSpec,
};
use harmony_luau as luau;
#[cfg(any(feature = "docgen", test))]
use harmony_luau::{
    LuauType,
    LuauTypeInfo,
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    render_definition_file_with_support,
};

use agdb::DbId;

use crate::plugins::db;

struct IdsModule;

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/ids")
        .capability("lyra.ids")
        .function(get_id_spec())
        .function(get_ids_spec())
        .function(get_db_id_spec())
        .function(get_db_ids_spec())
        .install(|_| Ok(ModuleExport::new(IdsModule)))
}

fn get_id_spec() -> FunctionSpec {
    let spec = FunctionSpec::async_fn("get_id")
        .arg_name("db_id")
        .args::<i64>()
        .returns::<Option<String>>();
    spec.call_async(Arc::new(get_id_callback))
}

fn get_ids_spec() -> FunctionSpec {
    let spec = FunctionSpec::async_fn("get_ids")
        .arg_name("db_ids")
        .args::<Vec<u64>>()
        .returns::<std::collections::BTreeMap<u64, Option<String>>>();
    spec.call_async(Arc::new(get_ids_callback))
}

fn get_db_id_spec() -> FunctionSpec {
    let spec = FunctionSpec::async_fn("get_db_id")
        .arg_name("id")
        .args::<String>()
        .returns::<Option<i64>>();
    spec.call_async(Arc::new(get_db_id_callback))
}

fn get_db_ids_spec() -> FunctionSpec {
    let spec = FunctionSpec::async_fn("get_db_ids")
        .arg_name("ids")
        .args::<Vec<String>>()
        .returns::<std::collections::BTreeMap<String, Option<u64>>>();
    spec.call_async(Arc::new(get_db_ids_callback))
}
fn get_id_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let db_id: i64 = frame.args.read_named("db_id")?;
    let store = frame
        .vm
        .data()
        .get::<IdsLookupModuleStore>()?
        .as_ref()
        .clone();
    Ok(luau::ScheduledFuture::new(async move {
        store.find_id_by_db_id(DbId(db_id)).await
    }))
}
fn get_ids_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let table: luau::Table = frame.args.read_named("db_ids")?;
    let ids = parse_db_ids(frame.vm, &table)?;
    let store = frame
        .vm
        .data()
        .get::<IdsLookupModuleStore>()?
        .as_ref()
        .clone();
    Ok(luau::ScheduledFuture::new(async move {
        let resolved = store.find_ids_by_db_ids(&ids).await?;

        let mut table = luau::OwnedTable::with_entry_capacity(0, 0, ids.len());
        for id in ids {
            table.set_key(
                luau::Value::Number(id.0 as f64),
                resolved
                    .get(&id)
                    .map(|value| luau::Value::String(value.clone().into_bytes()))
                    .unwrap_or(luau::Value::Nil),
            );
        }
        Ok(table)
    }))
}
fn get_db_id_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let id: String = frame.args.read_named("id")?;
    let store = frame
        .vm
        .data()
        .get::<IdsLookupModuleStore>()?
        .as_ref()
        .clone();
    Ok(luau::ScheduledFuture::new(async move {
        Ok(store.find_node_id_by_id(&id).await?.map(|id| id.0))
    }))
}
fn get_db_ids_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let table: luau::Table = frame.args.read_named("ids")?;
    let ids = parse_strings(frame.vm, &table)?;
    let store = frame
        .vm
        .data()
        .get::<IdsLookupModuleStore>()?
        .as_ref()
        .clone();
    Ok(luau::ScheduledFuture::new(async move {
        let str_refs = ids.iter().map(String::as_str).collect::<Vec<_>>();
        let resolved = store.find_node_ids_by_ids(&str_refs).await?;

        let mut table = luau::OwnedTable::with_capacity(0, ids.len());
        for id in ids {
            table.set_field(
                id.as_str(),
                resolved
                    .get(&id)
                    .map(|db_id| luau::Value::Integer(db_id.0))
                    .unwrap_or(luau::Value::Nil),
            );
        }
        Ok(table)
    }))
}
#[derive(Clone, Default)]
pub(crate) struct IdsLookupModuleStore {
    db: Option<db::DbAsync>,
}
impl IdsLookupModuleStore {
    pub(crate) fn empty() -> Self {
        Self { db: None }
    }

    pub(crate) fn with_db(db: db::DbAsync) -> Self {
        Self { db: Some(db) }
    }

    async fn find_id_by_db_id(&self, db_id: DbId) -> luau::runtime::Result<Option<String>> {
        let Some(db) = &self.db else {
            return Ok(None);
        };
        let db = db.read().await;
        db::lookup::find_id_by_db_id(&*db, db_id).map_err(crate::plugins::runtime_error)
    }

    async fn find_ids_by_db_ids(
        &self,
        ids: &[DbId],
    ) -> luau::runtime::Result<std::collections::HashMap<DbId, String>> {
        let Some(db) = &self.db else {
            return Ok(std::collections::HashMap::new());
        };
        let db = db.read().await;
        db::lookup::find_ids_by_db_ids(&*db, ids).map_err(crate::plugins::runtime_error)
    }

    async fn find_node_id_by_id(&self, id: &str) -> luau::runtime::Result<Option<DbId>> {
        let Some(db) = &self.db else {
            return Ok(None);
        };
        let db = db.read().await;
        db::lookup::find_node_id_by_id(&*db, id).map_err(crate::plugins::runtime_error)
    }

    async fn find_node_ids_by_ids(
        &self,
        ids: &[&str],
    ) -> luau::runtime::Result<std::collections::HashMap<String, DbId>> {
        let Some(db) = &self.db else {
            return Ok(std::collections::HashMap::new());
        };
        let db = db.read().await;
        db::lookup::find_node_ids_by_ids(&*db, ids).map_err(crate::plugins::runtime_error)
    }
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
fn parse_strings(vm: &luau::Vm, table: &luau::Table) -> luau::runtime::Result<Vec<String>> {
    let mut entries = Vec::new();
    for (key, value) in table.pairs_raw(vm)? {
        let Some(index) = sequence_index(key) else {
            continue;
        };
        let luau::Value::String(bytes) = value else {
            continue;
        };
        let id = String::from_utf8(bytes)
            .map_err(|error| luau::Error::Runtime(format!("id must be valid UTF-8: {error}")))?;
        entries.push((index, id));
    }
    entries.sort_by_key(|(index, _)| *index);

    let mut ids = Vec::new();
    let mut seen = HashSet::new();
    for (_, id) in entries {
        let trimmed = id.trim().to_string();
        if !trimmed.is_empty() && seen.insert(trimmed.clone()) {
            ids.push(trimmed);
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
        name: "Ids",
        local_name: "ids",
        description: None,
        fields: Vec::new(),
        functions: vec![
            ModuleFunctionDescriptor {
                path: vec!["get_id"],
                description: Some(
                    "Returns the public nanoid string for a given numeric database ID, or nil if not found.",
                ),
                params: vec![param("db_id", i64::luau_type())],
                returns: vec![Option::<String>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_ids"],
                description: Some("Returns public nanoid strings for many numeric database IDs."),
                params: vec![param("db_ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(
                    u64::luau_type(),
                    Option::<String>::luau_type(),
                )],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_db_id"],
                description: Some(
                    "Returns the numeric database ID for a given public nanoid string, or nil if not found.",
                ),
                params: vec![param("id", String::luau_type())],
                returns: vec![Option::<i64>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_db_ids"],
                description: Some("Returns numeric database IDs for many public nanoid strings."),
                params: vec![param("ids", Vec::<String>::luau_type())],
                returns: vec![LuauType::map(
                    String::luau_type(),
                    Option::<u64>::luau_type(),
                )],
                yields: true,
            },
        ],
    }
}

#[cfg(any(feature = "docgen", test))]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(&module_descriptor(), &[], &[], &[])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposes_handwritten_module_spec() {
        let spec = module_spec();

        assert_eq!(spec.id.0.as_ref(), "lyra/ids");
        assert_eq!(spec.capability.as_ref().unwrap().0.as_ref(), "lyra.ids");
        assert_eq!(spec.functions.len(), 4);
        assert!(spec.functions.iter().all(|function| function.yields));
    }

    #[test]
    fn renders_ids_module_definition() {
        let rendered = render_luau_definition().expect("render lyra/ids docs");

        assert!(rendered.contains("@class Ids"));
        assert!(rendered.contains("function ids.get_id(db_id: number): string?"));
        assert!(rendered.contains("function ids.get_ids(db_ids: {number}): { [number]: string? }"));
        assert!(rendered.contains("function ids.get_db_id(id: string): number?"));
        assert!(rendered.contains("function ids.get_db_ids(ids: {string}): { [string]: number? }"));
    }
}
