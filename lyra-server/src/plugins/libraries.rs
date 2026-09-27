// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

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
    IntoLuauReturn,
    LuauType,
    LuauTypeInfo,
};
#[cfg(feature = "docgen")]
use harmony_luau::{
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    render_definition_file_with_support,
};
use serde::Serialize;

use crate::plugins::args;
use crate::plugins::db::{
    self,
    DbAsync,
    ResolveId,
};
use crate::services::auth::Principal;
use crate::services::auth::access::{
    self,
    LibraryFull,
    LibraryView,
};

#[derive(Clone, Default)]
pub(crate) struct LibrariesModuleStore {
    db: Option<DbAsync>,
}

impl LibrariesModuleStore {
    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        Self { db: None }
    }

    pub(crate) fn with_db(db: DbAsync) -> Self {
        Self { db: Some(db) }
    }

    fn db(&self) -> luau::runtime::Result<DbAsync> {
        self.db.clone().ok_or_else(|| {
            crate::plugins::runtime_error(
                "lyra/libraries requires a database-backed plugin executor",
            )
        })
    }
}

struct LibrariesModule;

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/libraries")
        .capability("lyra.libraries")
        .function(list_spec())
        .function(get_for_entity_spec())
        .function(get_for_entities_spec())
        .install(|_| Ok(ModuleExport::new(LibrariesModule)))
}

fn list_spec() -> FunctionSpec {
    FunctionSpec::async_fn("list")
        .arg_name("id")
        .args::<Option<ResolveId>>()
        .returns::<Vec<LibraryRecord>>()
        .call_async(std::sync::Arc::new(list_callback))
}

fn get_for_entity_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get_for_entity")
        .arg_name("entity_id")
        .args::<i64>()
        .returns::<Vec<LibraryRecord>>()
        .call_async(std::sync::Arc::new(get_for_entity_callback))
}

fn get_for_entities_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get_for_entities")
        .arg_name("entity_ids")
        .args::<Vec<u64>>()
        .returns::<luau::Table>()
        .call_async(std::sync::Arc::new(get_for_entities_callback))
}

fn list_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let id = frame
        .args
        .read_optional_named::<luau::Value>("id")?
        .map(args::resolve_id)
        .transpose()?;
    let store = frame
        .vm
        .data()
        .get::<LibrariesModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;
    let principal = caller_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let libraries: Vec<LibraryRecord> = match (principal.as_ref(), id) {
            (Some(principal), None) => access::libraries(&db, principal)
                .map_err(crate::plugins::runtime_error)?
                .into_iter()
                .map(LibraryRecord::from)
                .collect::<Vec<_>>(),
            (None, None) => {
                access::system_libraries(&db, &crate::services::libraries::system_context())
                    .map_err(crate::plugins::runtime_error)?
                    .into_iter()
                    .map(LibraryRecord::from)
                    .collect::<Vec<_>>()
            }
            (principal, Some(resolve_id)) => {
                let query_id = resolve_id
                    .to_query_id(&db)
                    .map_err(crate::plugins::runtime_error)?
                    .ok_or_else(|| crate::plugins::runtime_error("could not resolve id"))?;
                match (principal, query_id) {
                    (Some(principal), QueryId::Id(id)) => access::library_by_id(&db, principal, id)
                        .map_err(crate::plugins::runtime_error)?
                        .into_iter()
                        .map(LibraryRecord::from)
                        .collect::<Vec<_>>(),
                    (None, QueryId::Id(id)) => access::system_library_by_id(
                        &db,
                        &crate::services::libraries::system_context(),
                        id,
                    )
                    .map_err(crate::plugins::runtime_error)?
                    .into_iter()
                    .map(LibraryRecord::from)
                    .collect::<Vec<_>>(),
                    (Some(principal), QueryId::Alias(alias)) => {
                        access::libraries_by_alias(&db, principal, alias.as_str())
                            .map_err(crate::plugins::runtime_error)?
                            .into_iter()
                            .map(LibraryRecord::from)
                            .collect::<Vec<_>>()
                    }
                    (None, QueryId::Alias(alias)) => access::system_libraries_by_alias(
                        &db,
                        &crate::services::libraries::system_context(),
                        alias.as_str(),
                    )
                    .map_err(crate::plugins::runtime_error)?
                    .into_iter()
                    .map(LibraryRecord::from)
                    .collect::<Vec<_>>(),
                }
            }
        };
        harmony_luau::serializable_to_luau_owned(libraries)
    }))
}

fn get_for_entity_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let entity_id: i64 = frame.args.read_named("entity_id")?;
    let store = frame
        .vm
        .data()
        .get::<LibrariesModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;
    let principal = caller_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let libraries = if let Some(principal) = principal {
            access::libraries_for_entity(&db, &principal, DbId(entity_id))
                .map_err(crate::plugins::runtime_error)?
                .into_iter()
                .map(LibraryRecord::from)
                .collect::<Vec<_>>()
        } else {
            access::system_libraries_for_entity(
                &db,
                &crate::services::libraries::system_context(),
                DbId(entity_id),
            )
            .map_err(crate::plugins::runtime_error)?
            .into_iter()
            .map(LibraryRecord::from)
            .collect::<Vec<_>>()
        };
        harmony_luau::serializable_to_luau_owned(libraries)
    }))
}

fn get_for_entities_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let ids_table: luau::Table = frame.args.read_named("entity_ids")?;
    let ids = args::unique_ids(frame.vm, &ids_table)?;
    let store = frame
        .vm
        .data()
        .get::<LibrariesModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;
    let principal = caller_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let libraries: HashMap<DbId, LibraryRecord> = if let Some(principal) = principal {
            access::libraries_for_entities(&db, &principal, &ids)
                .map_err(crate::plugins::runtime_error)?
                .into_iter()
                .map(|(id, library)| (id, LibraryRecord::from(library)))
                .collect::<HashMap<_, _>>()
        } else {
            access::system_libraries_for_entities(
                &db,
                &crate::services::libraries::system_context(),
                &ids,
            )
            .map_err(crate::plugins::runtime_error)?
            .into_iter()
            .map(|(id, library)| (id, LibraryRecord::from(library)))
            .collect::<HashMap<_, _>>()
        };
        let mut table = luau::OwnedTable::with_entry_capacity(0, 0, ids.len());
        for id in ids {
            let value = libraries
                .get(&id)
                .cloned()
                .map(harmony_luau::serializable_to_luau_owned)
                .transpose()?
                .unwrap_or(luau::Value::Nil);
            table.set_key(luau::Value::from(id.0), value);
        }
        table.into_luau_return()
    }))
}

#[derive(Clone, Serialize)]
pub(crate) struct LibraryRecord {
    db_id: Option<i64>,
    id: String,
    name: String,
    path: Option<String>,
    language: Option<String>,
    country: Option<String>,
}

impl From<db::libraries::Library> for LibraryRecord {
    fn from(library: db::libraries::Library) -> Self {
        Self {
            db_id: library.db_id.map(|id| id.0),
            id: library.id,
            name: library.name,
            path: Some(library.path.to_string_lossy().to_string()),
            language: library.language,
            country: library.country,
        }
    }
}

impl From<LibraryView> for LibraryRecord {
    fn from(library: LibraryView) -> Self {
        Self {
            db_id: library.db_id.map(|id| id.0),
            id: library.id,
            name: library.name,
            path: None,
            language: library.language,
            country: library.country,
        }
    }
}

impl From<LibraryFull> for LibraryRecord {
    fn from(library: LibraryFull) -> Self {
        Self {
            db_id: library.db_id.map(|id| id.0),
            id: library.id,
            name: library.name,
            path: Some(library.path.to_string_lossy().to_string()),
            language: library.language,
            country: library.country,
        }
    }
}

fn caller_principal(context: &luau::CallContext) -> luau::runtime::Result<Option<Principal>> {
    crate::plugins::auth::dispatch_principal(context)
}

impl LuauTypeInfo for LibraryRecord {
    fn luau_type() -> LuauType {
        LuauType::named("Library")
    }
}

impl DescribeInterface for LibraryRecord {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("Library", None);
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
                name: "name",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "path",
                ty: Option::<String>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "language",
                ty: Option::<String>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "country",
                ty: Option::<String>::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

#[cfg(feature = "docgen")]
fn param(name: &'static str, ty: LuauType) -> ParameterDescriptor {
    ParameterDescriptor {
        name,
        ty,
        description: None,
        variadic: false,
    }
}

#[cfg(feature = "docgen")]
fn module_descriptor() -> ModuleDescriptor {
    ModuleDescriptor {
        name: "Libraries",
        local_name: "libraries",
        description: None,
        fields: Vec::new(),
        functions: vec![
            ModuleFunctionDescriptor {
                path: vec!["list"],
                description: None,
                params: vec![param("id", LuauType::optional(args::resolve_id_type()))],
                returns: vec![Vec::<LibraryRecord>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_for_entity"],
                description: None,
                params: vec![param("entity_id", i64::luau_type())],
                returns: vec![Vec::<LibraryRecord>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_for_entities"],
                description: None,
                params: vec![param("entity_ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(u64::luau_type(), LibraryRecord::luau_type())],
                yields: true,
            },
        ],
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &module_descriptor(),
        &[],
        &[LibraryRecord::interface_descriptor()],
        &[],
    )
}
