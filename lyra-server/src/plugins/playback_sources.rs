// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashSet,
    sync::Arc,
};

use agdb::DbId;
use harmony_core::{
    FunctionSpec,
    ModuleExport,
    ModuleSpec,
};
use harmony_luau as luau;
#[cfg(feature = "docgen")]
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

use crate::{
    plugins::db::{
        self,
        DbAsync,
        ResolveId,
    },
    services::playback_sources as playback_source_service,
};

#[derive(Clone, Default)]
pub(crate) struct PlaybackSourcesModuleStore {
    db: Option<DbAsync>,
}

impl PlaybackSourcesModuleStore {
    pub(crate) fn empty() -> Self {
        Self { db: None }
    }

    pub(crate) fn with_db(db: DbAsync) -> Self {
        Self { db: Some(db) }
    }

    fn db(&self) -> luau::runtime::Result<DbAsync> {
        self.db.clone().ok_or_else(|| {
            crate::plugins::runtime_error(
                "lyra/playback_sources requires a database-backed plugin executor",
            )
        })
    }
}

struct PlaybackSourcesModule;

#[cfg(feature = "docgen")]
struct EntryInfo;

#[cfg(feature = "docgen")]
struct PlaybackSourceInfo;

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/playback_sources")
        .capability("lyra.playback_sources")
        .function(get_spec())
        .function(get_many_spec())
        .install(|_| Ok(ModuleExport::new(PlaybackSourcesModule)))
}

fn get_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("id")
        .args::<Option<ResolveId>>()
        .arg_name("include_entry")
        .args::<Option<bool>>()
        .returns::<luau::Table>()
        .call_async(Arc::new(get_callback))
}

fn get_many_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get_many")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("track_ids")
        .args::<luau::Table>()
        .arg_name("include_entry")
        .args::<Option<bool>>()
        .returns::<luau::Table>()
        .call_async(Arc::new(get_many_callback))
}

fn get_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let id = frame
        .args
        .read_optional_named::<luau::Value>("id")?
        .map(parse_resolve_id)
        .transpose()?
        .unwrap_or_else(|| ResolveId::alias("tracks"));
    let include_entry = frame
        .args
        .read_optional_named::<bool>("include_entry")?
        .unwrap_or(false);
    let store = frame
        .vm
        .data()
        .get::<PlaybackSourcesModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let include_full_path =
            db::roles::has_permission(&principal.permissions, db::Permission::ManageLibraries);
        let query_id = id
            .to_query_id(&db)
            .map_err(crate::plugins::runtime_error)?
            .ok_or_else(|| crate::plugins::runtime_error("could not resolve scope"))?;
        let tracks = db::tracks::get(&db, query_id).map_err(crate::plugins::runtime_error)?;

        let mut rows = luau::OwnedTable::new();
        for track in tracks {
            let Some(track_id) = track.db_id.map(DbId::from) else {
                continue;
            };
            if !crate::services::auth::access::entity_accessible(&db, &principal, track_id)
                .map_err(crate::plugins::runtime_error)?
            {
                continue;
            }
            let Some(source) = playback_source_service::resolve(&db, track_id, include_entry)
                .map_err(crate::plugins::runtime_error)?
            else {
                continue;
            };
            rows.push_array(luau::Value::TableData(source_to_table(
                source,
                include_entry,
                include_full_path,
            )));
        }

        Ok(luau::Value::TableData(rows))
    }))
}

fn get_many_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let track_ids: luau::Table = frame.args.read_named("track_ids")?;
    let track_ids = parse_db_ids(frame.vm, &track_ids)?;
    let include_entry = frame
        .args
        .read_optional_named::<bool>("include_entry")?
        .unwrap_or(false);
    let store = frame
        .vm
        .data()
        .get::<PlaybackSourcesModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let include_full_path =
            db::roles::has_permission(&principal.permissions, db::Permission::ManageLibraries);
        let mut rows = luau::OwnedTable::with_entry_capacity(0, 0, track_ids.len());
        for track_id in track_ids {
            let value =
                if crate::services::auth::access::entity_accessible(&db, &principal, track_id)
                    .map_err(crate::plugins::runtime_error)?
                {
                    playback_source_service::resolve(&db, track_id, include_entry)
                        .map_err(crate::plugins::runtime_error)?
                        .map(|source| {
                            luau::Value::TableData(source_to_table(
                                source,
                                include_entry,
                                include_full_path,
                            ))
                        })
                        .unwrap_or(luau::Value::Nil)
                } else {
                    luau::Value::Nil
                };
            rows.set_key(luau::Value::Integer(track_id.0), value.clone());
            rows.set_key(luau::Value::Number(track_id.0 as f64), value);
        }

        Ok(luau::Value::TableData(rows))
    }))
}

fn source_to_table(
    source: playback_source_service::PlaybackSource,
    include_entry: bool,
    include_full_path: bool,
) -> luau::OwnedTable {
    let playback_source_service::PlaybackSource {
        track_db_id,
        source_id,
        source_kind,
        source_key,
        is_primary,
        start_ms,
        end_ms,
        entry,
        ..
    } = source;

    let mut table = luau::OwnedTable::with_capacity(0, 9);
    table.set_field("track_id", luau::Value::Integer(track_db_id.0));
    table.set_field("source_id", luau::Value::Integer(source_id.0));
    table.set_field("source_kind", luau::Value::String(source_kind.into_bytes()));
    table.set_field("source_key", luau::Value::String(source_key.into_bytes()));
    table.set_field("is_primary", luau::Value::Boolean(is_primary));
    table.set_field("start_ms", optional_u64(start_ms));
    table.set_field("end_ms", optional_u64(end_ms));
    table.set_field(
        "is_virtual",
        luau::Value::Boolean(start_ms.is_some() || end_ms.is_some()),
    );
    if include_entry {
        table.set_field(
            "entry",
            entry
                .map(|entry| luau::Value::TableData(entry_to_table(entry, include_full_path)))
                .unwrap_or(luau::Value::Nil),
        );
    }
    table
}

fn entry_to_table(entry: db::Entry, include_full_path: bool) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(0, 8);
    table.set_field(
        "db_id",
        entry
            .db_id
            .map(|id| luau::Value::Integer(id.0))
            .unwrap_or(luau::Value::Nil),
    );
    table.set_field("id", luau::Value::String(entry.id.into_bytes()));
    table.set_field(
        "full_path",
        if include_full_path {
            luau::Value::String(entry.full_path.to_string_lossy().into_owned().into_bytes())
        } else {
            luau::Value::Nil
        },
    );
    table.set_field(
        "kind",
        luau::Value::String(entry.kind.to_string().into_bytes()),
    );
    table.set_field("name", luau::Value::String(entry.name.into_bytes()));
    table.set_field(
        "hash",
        entry
            .hash
            .map(|hash| luau::Value::String(hash.into_bytes()))
            .unwrap_or(luau::Value::Nil),
    );
    table.set_field("size", luau::Value::Integer(saturating_i64(entry.size)));
    table.set_field("mtime", luau::Value::Integer(saturating_i64(entry.mtime)));
    table
}

fn optional_u64(value: Option<u64>) -> luau::Value {
    value
        .map(|value| luau::Value::Integer(saturating_i64(value)))
        .unwrap_or(luau::Value::Nil)
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

fn saturating_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

#[cfg(feature = "docgen")]
impl LuauTypeInfo for EntryInfo {
    fn luau_type() -> LuauType {
        LuauType::named("EntryInfo")
    }
}

#[cfg(feature = "docgen")]
impl DescribeInterface for EntryInfo {
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

#[cfg(feature = "docgen")]
impl LuauTypeInfo for PlaybackSourceInfo {
    fn luau_type() -> LuauType {
        LuauType::named("PlaybackSourceInfo")
    }
}

#[cfg(feature = "docgen")]
impl DescribeInterface for PlaybackSourceInfo {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("PlaybackSourceInfo", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "track_id",
                ty: i64::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "source_id",
                ty: i64::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "source_kind",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "source_key",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "is_primary",
                ty: bool::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "start_ms",
                ty: Option::<u64>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "end_ms",
                ty: Option::<u64>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "is_virtual",
                ty: bool::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "entry",
                ty: Option::<EntryInfo>::luau_type(),
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
fn resolve_id_type() -> LuauType {
    LuauType::union(vec![i64::luau_type(), String::luau_type()])
}

#[cfg(feature = "docgen")]
fn module_descriptor() -> ModuleDescriptor {
    ModuleDescriptor {
        name: "PlaybackSources",
        local_name: "playback_sources",
        description: None,
        fields: Vec::new(),
        functions: vec![
            ModuleFunctionDescriptor {
                path: vec!["get"],
                description: None,
                params: vec![
                    param("id", LuauType::optional(resolve_id_type())),
                    param("include_entry", Option::<bool>::luau_type()),
                ],
                returns: vec![Vec::<PlaybackSourceInfo>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_many"],
                description: None,
                params: vec![
                    param("track_ids", Vec::<u64>::luau_type()),
                    param("include_entry", Option::<bool>::luau_type()),
                ],
                returns: vec![LuauType::map(
                    u64::luau_type(),
                    Option::<PlaybackSourceInfo>::luau_type(),
                )],
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
        &[
            EntryInfo::interface_descriptor(),
            PlaybackSourceInfo::interface_descriptor(),
        ],
        &[],
    )
}
