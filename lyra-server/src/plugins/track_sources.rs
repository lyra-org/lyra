// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashSet;

use agdb::DbId;
use harmony_core::{
    FunctionSpec,
    ModuleExport,
    ModuleSpec,
};
use harmony_luau as luau;
use harmony_luau::{
    IntoLuauReturn,
    LuauType,
    LuauTypeInfo,
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    render_definition_file_with_support,
};

use crate::plugins::db::{
    self,
    DbAsync,
};

#[derive(Clone, Default)]
pub(crate) struct TrackSourcesModuleStore {
    db: Option<DbAsync>,
}

impl TrackSourcesModuleStore {
    pub(crate) fn empty() -> Self {
        Self { db: None }
    }

    pub(crate) fn with_db(db: DbAsync) -> Self {
        Self { db: Some(db) }
    }

    fn db(&self) -> luau::runtime::Result<DbAsync> {
        self.db.clone().ok_or_else(|| {
            crate::plugins::runtime_error(
                "lyra/track_sources requires a database-backed plugin executor",
            )
        })
    }
}

struct TrackSourcesModule;

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/track_sources")
        .capability("lyra.track_sources")
        .function(get_primary_source_key_spec())
        .function(get_primary_container_spec())
        .function(get_primary_containers_spec())
        .install(|_| Ok(ModuleExport::new(TrackSourcesModule)))
}

fn get_primary_source_key_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get_primary_source_key")
        .arg_name("track_id")
        .args::<i64>()
        .returns::<Option<String>>()
        .call_async(std::sync::Arc::new(get_primary_source_key_callback))
}

fn get_primary_container_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get_primary_container")
        .arg_name("track_id")
        .args::<i64>()
        .returns::<Option<String>>()
        .call_async(std::sync::Arc::new(get_primary_container_callback))
}

fn get_primary_containers_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get_primary_containers")
        .arg_name("track_ids")
        .args::<Vec<u64>>()
        .returns::<luau::Table>()
        .call_async(std::sync::Arc::new(get_primary_containers_callback))
}

fn get_primary_source_key_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let track_id: i64 = frame.args.read_named("track_id")?;
    if track_id <= 0 {
        return Ok(luau::ScheduledFuture::new(async { Ok(luau::Value::Nil) }));
    }

    let store = frame
        .vm
        .data()
        .get::<TrackSourcesModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let source = db::track_sources::get_primary_by_track(&db, DbId(track_id))
            .map_err(crate::plugins::runtime_error)?;
        let source_key = source
            .map(|source| source.source_key)
            .filter(|value| !value.trim().is_empty());
        source_key.into_luau_return()
    }))
}

fn get_primary_container_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let track_id: i64 = frame.args.read_named("track_id")?;
    if track_id <= 0 {
        return Ok(luau::ScheduledFuture::new(async { Ok(luau::Value::Nil) }));
    }

    let store = frame
        .vm
        .data()
        .get::<TrackSourcesModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let container = resolve_primary_container(&db, DbId(track_id))
            .map_err(crate::plugins::runtime_error)?;
        container.into_luau_return()
    }))
}

fn get_primary_containers_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let ids_table: luau::Table = frame.args.read_named("track_ids")?;
    let ids = parse_db_ids(frame.vm, &ids_table)?;
    let store = frame
        .vm
        .data()
        .get::<TrackSourcesModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let mut table = luau::OwnedTable::with_entry_capacity(0, 0, ids.len());
        for id in &ids {
            let container =
                resolve_primary_container(&db, *id).map_err(crate::plugins::runtime_error)?;
            let value = container
                .map(|value| luau::Value::String(value.into_bytes()))
                .unwrap_or(luau::Value::Nil);
            crate::plugins::set_owned_db_id_key(&mut table, *id, value);
        }
        Ok(luau::Value::TableData(table))
    }))
}

fn resolve_primary_container(
    db: &agdb::DbAny,
    track_db_id: DbId,
) -> anyhow::Result<Option<String>> {
    let Some(source) = db::track_sources::get_primary_by_track(db, track_db_id)? else {
        return Ok(None);
    };
    let Some(source_id) = source.db_id else {
        return Ok(None);
    };
    let Some(entry_db_id) = db::track_sources::get_entry_id(db, source_id)? else {
        return Ok(None);
    };
    let Some(entry) = db::entries::get_by_id(db, entry_db_id)? else {
        return Ok(None);
    };
    Ok(entry
        .full_path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.trim().to_ascii_lowercase())
        .filter(|ext| !ext.is_empty()))
}

fn parse_db_ids(vm: &luau::Vm, table: &luau::Table) -> luau::runtime::Result<Vec<DbId>> {
    let mut values = Vec::new();
    for (key, value) in table.pairs_raw(vm)? {
        let Some(index) = array_index(key) else {
            continue;
        };
        let Some(id) = db_id_value(value)? else {
            continue;
        };
        values.push((index, id));
    }
    values.sort_by_key(|(index, _)| *index);

    let mut ids = Vec::new();
    let mut seen = HashSet::new();
    for (_, id) in values {
        if seen.insert(id) {
            ids.push(id);
        }
    }
    Ok(ids)
}

fn array_index(value: luau::Value) -> Option<i64> {
    match value {
        luau::Value::Integer(value) if value > 0 => Some(value),
        luau::Value::Number(value) if value.is_finite() && value.fract() == 0.0 && value > 0.0 => {
            Some(value as i64)
        }
        _ => None,
    }
}

fn db_id_value(value: luau::Value) -> luau::runtime::Result<Option<DbId>> {
    match value {
        luau::Value::Integer(value) if value > 0 => Ok(Some(DbId(value))),
        luau::Value::Number(value) if value.is_finite() && value.fract() == 0.0 && value > 0.0 => {
            Ok(Some(DbId(value as i64)))
        }
        luau::Value::Integer(_) | luau::Value::Number(_) => Ok(None),
        other => Err(crate::plugins::runtime_error(format!(
            "id entries must be positive integers, got {}",
            other.type_name()
        ))),
    }
}

fn param(name: &'static str, ty: LuauType) -> ParameterDescriptor {
    ParameterDescriptor {
        name,
        ty,
        description: None,
        variadic: false,
    }
}

fn module_descriptor() -> ModuleDescriptor {
    ModuleDescriptor {
        name: "TrackSources",
        local_name: "track_sources",
        description: None,
        fields: Vec::new(),
        functions: vec![
            ModuleFunctionDescriptor {
                path: vec!["get_primary_source_key"],
                description: None,
                params: vec![param("track_id", i64::luau_type())],
                returns: vec![Option::<String>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_primary_container"],
                description: None,
                params: vec![param("track_id", i64::luau_type())],
                returns: vec![Option::<String>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_primary_containers"],
                description: None,
                params: vec![param("track_ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(
                    u64::luau_type(),
                    Option::<String>::luau_type(),
                )],
                yields: true,
            },
        ],
    }
}

pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(&module_descriptor(), &[], &[], &[])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_track_sources_module_definition() {
        let rendered = render_luau_definition().expect("render lyra/track_sources docs");

        assert!(rendered.contains("@class TrackSources"));
        assert!(
            rendered.contains(
                "function track_sources.get_primary_source_key(track_id: number): string?"
            )
        );
        assert!(
            rendered.contains(
                "function track_sources.get_primary_container(track_id: number): string?"
            )
        );
        assert!(rendered.contains(
            "function track_sources.get_primary_containers(track_ids: {number}): { [number]: string? }"
        ));
    }
}
