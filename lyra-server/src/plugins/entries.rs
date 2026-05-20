// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbId,
    QueryId,
};
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
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    render_definition_file_with_support,
};
use serde::Serialize;

use crate::plugins::db::{
    self,
    DbAsync,
    ResolveId,
};

#[derive(Clone, Default)]
pub(crate) struct EntriesModuleStore {
    db: Option<DbAsync>,
}

impl EntriesModuleStore {
    pub(crate) fn empty() -> Self {
        Self { db: None }
    }

    pub(crate) fn with_db(db: DbAsync) -> Self {
        Self { db: Some(db) }
    }

    fn db(&self) -> luau::runtime::Result<DbAsync> {
        self.db.clone().ok_or_else(|| {
            crate::plugins::runtime_error("lyra/entries requires a database-backed plugin executor")
        })
    }
}

struct EntriesModule;

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/entries")
        .capability("lyra.entries")
        .function(get_spec())
        .install(|_| Ok(ModuleExport::new(EntriesModule)))
}

fn get_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get")
        .arg_name("id")
        .args::<Option<ResolveId>>()
        .returns::<Vec<EntryRecord>>()
        .call_async_native(std::sync::Arc::new(get_callback))
}

fn get_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<luau::ScheduledFuture> {
    let id = frame
        .args
        .read_optional_named::<luau::Value>("id")?
        .map(parse_resolve_id)
        .transpose()?;
    let store = frame
        .vm
        .data()
        .get::<EntriesModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;

    Ok(Box::pin(async move {
        let db = db.read().await;
        let entries = match id {
            None => db::entries::get(&db, "libraries").map_err(crate::plugins::runtime_error)?,
            Some(resolve_id) => {
                let query_id = resolve_id
                    .to_query_id(&db)
                    .map_err(crate::plugins::runtime_error)?
                    .ok_or_else(|| crate::plugins::runtime_error("could not resolve id"))?;
                match query_id {
                    QueryId::Id(node_id) => {
                        if db::tracks::get_by_id(&db, node_id)
                            .map_err(crate::plugins::runtime_error)?
                            .is_some()
                        {
                            db::entries::get_by_track(&db, node_id)
                                .map_err(crate::plugins::runtime_error)?
                        } else {
                            db::entries::get(&db, QueryId::Id(node_id))
                                .map_err(crate::plugins::runtime_error)?
                        }
                    }
                    other => db::entries::get(&db, other).map_err(crate::plugins::runtime_error)?,
                }
            }
        };
        let entries = entries
            .into_iter()
            .map(EntryRecord::from)
            .collect::<Vec<_>>();
        Ok(vec![crate::plugins::serializable_to_luau_owned(entries)?])
    }))
}

#[derive(Serialize)]
pub(crate) struct EntryRecord {
    db_id: Option<i64>,
    id: String,
    full_path: Option<String>,
    kind: String,
    name: String,
    hash: Option<String>,
    size: u64,
    mtime: u64,
}

impl From<db::Entry> for EntryRecord {
    fn from(entry: db::Entry) -> Self {
        Self {
            db_id: entry.db_id.map(|id| id.0),
            id: entry.id,
            full_path: None,
            kind: entry.kind.to_string(),
            name: entry.name,
            hash: entry.hash,
            size: entry.size,
            mtime: entry.mtime,
        }
    }
}

fn parse_resolve_id(value: luau::Value) -> luau::runtime::Result<ResolveId> {
    match value {
        luau::Value::Integer(value) => Ok(ResolveId::DbId(DbId(value))),
        luau::Value::Number(value) if value.is_finite() && value.fract() == 0.0 => {
            Ok(ResolveId::DbId(DbId(value as i64)))
        }
        luau::Value::String(bytes) => {
            let text = String::from_utf8(bytes).map_err(crate::plugins::runtime_error)?;
            if db::ROOT_COLLECTION_ALIASES.contains(&text.as_str()) {
                Ok(ResolveId::Alias(text))
            } else {
                Ok(ResolveId::Nanoid(text))
            }
        }
        other => Err(crate::plugins::runtime_error(format!(
            "expected integer or string id, got {}",
            other.type_name()
        ))),
    }
}

impl LuauTypeInfo for EntryRecord {
    fn luau_type() -> LuauType {
        LuauType::named("EntryInfo")
    }
}

impl DescribeInterface for EntryRecord {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("EntryInfo", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "db_id",
                ty: Option::<i64>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "id",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "full_path",
                ty: Option::<String>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "kind",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "name",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "hash",
                ty: Option::<String>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "size",
                ty: u64::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "mtime",
                ty: u64::luau_type(),
                description: None,
            },
        ]);
        descriptor
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

fn resolve_id_type() -> LuauType {
    LuauType::union(vec![i64::luau_type(), String::luau_type()])
}

fn module_descriptor() -> ModuleDescriptor {
    ModuleDescriptor {
        name: "Entries",
        local_name: "entries",
        description: None,
        fields: Vec::new(),
        functions: vec![ModuleFunctionDescriptor {
            path: vec!["get"],
            description: None,
            params: vec![param("id", LuauType::optional(resolve_id_type()))],
            returns: vec![Vec::<EntryRecord>::luau_type()],
            yields: true,
        }],
    }
}

pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &module_descriptor(),
        &[],
        &[EntryRecord::interface_descriptor()],
        &[],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_entries_module_definition() {
        let rendered = render_luau_definition().expect("render lyra/entries docs");

        assert!(rendered.contains("@interface EntryInfo"));
        assert!(rendered.contains("full_path: string?"));
        assert!(rendered.contains("@class Entries"));
        assert!(rendered.contains("function entries.get(id: (number | string)?): {EntryInfo}"));
    }
}
