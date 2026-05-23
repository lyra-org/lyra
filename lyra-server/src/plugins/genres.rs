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
use serde::{
    Deserialize,
    Serialize,
};

use crate::plugins::db::{
    self,
    DbAsync,
    genres::{
        ResolveExternalId,
        ResolveGenre,
    },
};

#[derive(Clone, Default)]
pub(crate) struct GenresModuleStore {
    db: Option<DbAsync>,
}

impl GenresModuleStore {
    pub(crate) fn empty() -> Self {
        Self { db: None }
    }

    pub(crate) fn with_db(db: DbAsync) -> Self {
        Self { db: Some(db) }
    }

    fn db(&self) -> luau::runtime::Result<DbAsync> {
        self.db.clone().ok_or_else(|| {
            crate::plugins::runtime_error("lyra/genres requires a database-backed plugin executor")
        })
    }
}

struct GenresModule;

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/genres")
        .capability("lyra.genres")
        .function(add_spec())
        .function(resolve_spec())
        .function(add_parent_spec())
        .function(get_by_id_spec())
        .function(find_by_name_spec())
        .function(get_parents_spec())
        .function(get_children_spec())
        .function(get_releases_spec())
        .function(get_releases_many_spec())
        .function(get_for_release_spec())
        .function(get_for_releases_many_spec())
        .install(|_| Ok(ModuleExport::new(GenresModule)))
}

fn add_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("add")
        .named_arg::<i64>("release_id")
        .named_arg::<luau::Table>("request")
        .returns::<i64>()
        .call(add_callback)
}

fn resolve_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("resolve")
        .arg_name("request")
        .args::<luau::Table>()
        .returns::<i64>()
        .call(resolve_callback)
}

fn add_parent_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("add_parent")
        .named_arg::<i64>("child_id")
        .named_arg::<i64>("parent_id")
        .returns::<()>()
        .call(add_parent_callback)
}

fn get_by_id_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("get_by_id")
        .arg_name("genre_id")
        .args::<i64>()
        .returns::<Option<GenreRecord>>()
        .call(get_by_id_callback)
}

fn find_by_name_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("find_by_name")
        .arg_name("name")
        .args::<String>()
        .returns::<Option<GenreRecord>>()
        .call(find_by_name_callback)
}

fn get_parents_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("get_parents")
        .arg_name("genre_id")
        .args::<i64>()
        .returns::<Vec<GenreRecord>>()
        .call(get_parents_callback)
}

fn get_children_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("get_children")
        .arg_name("genre_id")
        .args::<i64>()
        .returns::<Vec<GenreRecord>>()
        .call(get_children_callback)
}

fn get_releases_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("get_releases")
        .arg_name("genre_id")
        .args::<i64>()
        .returns::<Vec<i64>>()
        .call(get_releases_callback)
}

fn get_releases_many_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("get_releases_many")
        .arg_name("genre_ids")
        .args::<Vec<u64>>()
        .returns::<luau::Table>()
        .call(get_releases_many_callback)
}

fn get_for_release_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("get_for_release")
        .arg_name("release_id")
        .args::<i64>()
        .returns::<Vec<GenreRecord>>()
        .call(get_for_release_callback)
}

fn get_for_releases_many_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("get_for_releases_many")
        .arg_name("release_ids")
        .args::<Vec<u64>>()
        .returns::<luau::Table>()
        .call(get_for_releases_many_callback)
}

fn add_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let release_id: i64 = frame.args.read_named("release_id")?;
    let request_table: luau::Table = frame.args.read_named("request")?;
    let request = genre_request_from_table(frame.vm, request_table)?;
    let store = frame.vm.data().get::<GenresModuleStore>()?.as_ref().clone();
    let db = store.db()?;

    let genre_id = futures::executor::block_on(async {
        let mut db = db.write().await;
        let release_id = DbId(release_id);
        let is_locked = db::releases::get_by_id(&db, release_id)
            .map_err(crate::plugins::runtime_error)?
            .is_some_and(|release| release.locked.unwrap_or(false));
        let genre_id =
            resolve_genre_from_request(&mut db, &request).map_err(crate::plugins::runtime_error)?;
        if !is_locked {
            db::genres::link_to_release(&mut db, genre_id, release_id)
                .map_err(crate::plugins::runtime_error)?;
        }
        Ok::<_, luau::Error>(genre_id)
    })?;

    frame.returns.write(luau::Value::Integer(genre_id.0))
}

fn resolve_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let request_table: luau::Table = frame.args.read_named("request")?;
    let request = genre_request_from_table(frame.vm, request_table)?;
    let store = frame.vm.data().get::<GenresModuleStore>()?.as_ref().clone();
    let db = store.db()?;

    let genre_id = futures::executor::block_on(async {
        let mut db = db.write().await;
        resolve_genre_from_request(&mut db, &request).map_err(crate::plugins::runtime_error)
    })?;

    frame.returns.write(luau::Value::Integer(genre_id.0))
}

fn add_parent_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let child_id: i64 = frame.args.read_named("child_id")?;
    let parent_id: i64 = frame.args.read_named("parent_id")?;
    let store = frame.vm.data().get::<GenresModuleStore>()?.as_ref().clone();
    let db = store.db()?;

    futures::executor::block_on(async {
        let mut db = db.write().await;
        db::genres::link_to_parent(&mut db, DbId(child_id), DbId(parent_id))
            .map_err(crate::plugins::runtime_error)
    })?;

    Ok(())
}

fn get_by_id_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let genre_id: i64 = frame.args.read_named("genre_id")?;
    let store = frame.vm.data().get::<GenresModuleStore>()?.as_ref().clone();
    let db = store.db()?;

    let genre = futures::executor::block_on(async {
        let db = db.read().await;
        db::genres::get_by_id(&db, DbId(genre_id)).map_err(crate::plugins::runtime_error)
    })?;

    write_optional_genre(&mut frame.returns, genre)
}

fn find_by_name_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let name: String = frame.args.read_named("name")?;
    let store = frame.vm.data().get::<GenresModuleStore>()?.as_ref().clone();
    let db = store.db()?;

    let trimmed = name.trim().to_string();
    if trimmed.is_empty() {
        return frame.returns.write(luau::Value::Nil);
    }

    let genre = futures::executor::block_on(async {
        let db = db.read().await;
        let Some(db_id) =
            db::genres::find_by_name(&db, &trimmed).map_err(crate::plugins::runtime_error)?
        else {
            return Ok(None);
        };
        db::genres::get_by_id(&db, db_id).map_err(crate::plugins::runtime_error)
    })?;

    write_optional_genre(&mut frame.returns, genre)
}

fn get_parents_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let genre_id: i64 = frame.args.read_named("genre_id")?;
    let store = frame.vm.data().get::<GenresModuleStore>()?.as_ref().clone();
    let db = store.db()?;

    let genres = futures::executor::block_on(async {
        let db = db.read().await;
        db::genres::get_parents(&db, DbId(genre_id)).map_err(crate::plugins::runtime_error)
    })?;

    frame
        .returns
        .write(harmony_luau::serializable_to_luau_owned(
            genres
                .into_iter()
                .map(GenreRecord::from)
                .collect::<Vec<_>>(),
        )?)
}

fn get_children_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let genre_id: i64 = frame.args.read_named("genre_id")?;
    let store = frame.vm.data().get::<GenresModuleStore>()?.as_ref().clone();
    let db = store.db()?;

    let genres = futures::executor::block_on(async {
        let db = db.read().await;
        db::genres::get_children(&db, DbId(genre_id)).map_err(crate::plugins::runtime_error)
    })?;

    frame
        .returns
        .write(harmony_luau::serializable_to_luau_owned(
            genres
                .into_iter()
                .map(GenreRecord::from)
                .collect::<Vec<_>>(),
        )?)
}

fn get_releases_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let genre_id: i64 = frame.args.read_named("genre_id")?;
    let store = frame.vm.data().get::<GenresModuleStore>()?.as_ref().clone();
    let db = store.db()?;

    let release_ids = futures::executor::block_on(async {
        let db = db.read().await;
        db::genres::get_releases(&db, DbId(genre_id)).map_err(crate::plugins::runtime_error)
    })?;

    frame
        .returns
        .write(harmony_luau::serializable_to_luau_owned(
            release_ids.into_iter().map(|id| id.0).collect::<Vec<_>>(),
        )?)
}

fn get_releases_many_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let ids_table: luau::Table = frame.args.read_named("genre_ids")?;
    let ids = parse_db_ids(frame.vm, &ids_table)?;
    let store = frame.vm.data().get::<GenresModuleStore>()?.as_ref().clone();
    let db = store.db()?;

    let releases = futures::executor::block_on(async {
        let db = db.read().await;
        db::genres::get_releases_many(&db, &ids).map_err(crate::plugins::runtime_error)
    })?;

    let table = frame.vm.create_table_with_capacity(0, ids.len() as i32)?;
    for id in ids {
        let release_ids = releases
            .get(&id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|release_id| release_id.0)
            .collect::<Vec<_>>();
        set_db_id_key(
            frame.vm,
            &table,
            id,
            harmony_luau::serializable_to_luau_owned(release_ids)?,
        )?;
    }
    frame.returns.write(table)
}

fn get_for_release_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let release_id: i64 = frame.args.read_named("release_id")?;
    let store = frame.vm.data().get::<GenresModuleStore>()?.as_ref().clone();
    let db = store.db()?;

    let genres = futures::executor::block_on(async {
        let db = db.read().await;
        db::genres::get_for_release(&db, DbId(release_id)).map_err(crate::plugins::runtime_error)
    })?;

    frame
        .returns
        .write(harmony_luau::serializable_to_luau_owned(
            genres
                .into_iter()
                .map(GenreRecord::from)
                .collect::<Vec<_>>(),
        )?)
}

fn get_for_releases_many_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let ids_table: luau::Table = frame.args.read_named("release_ids")?;
    let ids = parse_db_ids(frame.vm, &ids_table)?;
    let store = frame.vm.data().get::<GenresModuleStore>()?.as_ref().clone();
    let db = store.db()?;

    let genres = futures::executor::block_on(async {
        let db = db.read().await;
        db::genres::get_for_releases_many(&db, &ids).map_err(crate::plugins::runtime_error)
    })?;

    let table = frame.vm.create_table_with_capacity(0, ids.len() as i32)?;
    for id in ids {
        let value = genres
            .get(&id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(GenreRecord::from)
            .collect::<Vec<_>>();
        set_db_id_key(
            frame.vm,
            &table,
            id,
            harmony_luau::serializable_to_luau_owned(value)?,
        )?;
    }
    frame.returns.write(table)
}

#[derive(Debug, Deserialize)]
struct GenreExternalId {
    provider_id: String,
    id_type: String,
    id: String,
}

#[derive(Debug, Deserialize)]
struct GenreAliasInput {
    name: String,
    locale: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GenreAddRequest {
    name: String,
    external_id: Option<GenreExternalId>,
    aliases: Option<Vec<GenreAliasInput>>,
}

fn genre_request_from_table(
    vm: &luau::Vm,
    table: luau::Table,
) -> luau::runtime::Result<GenreAddRequest> {
    let value = harmony_json::luau_to_json(vm, &luau::Value::Table(table), 0)?;
    serde_json::from_value(value).map_err(crate::plugins::runtime_error)
}

fn resolve_genre_from_request(
    db: &mut agdb::DbAny,
    request: &GenreAddRequest,
) -> anyhow::Result<DbId> {
    let aliases_owned = request
        .aliases
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|alias| (alias.name.clone(), alias.locale.clone()))
        .collect::<Vec<_>>();
    let aliases_refs = aliases_owned
        .iter()
        .map(|(name, locale)| (name.as_str(), locale.as_deref()))
        .collect::<Vec<_>>();
    let external_id = request
        .external_id
        .as_ref()
        .map(|external_id| ResolveExternalId {
            provider_id: &external_id.provider_id,
            id_type: &external_id.id_type,
            id_value: &external_id.id,
        });

    db::genres::resolve(
        db,
        &ResolveGenre {
            name: &request.name,
            aliases: &aliases_refs,
            external_id,
        },
    )
}

#[derive(Serialize)]
pub(crate) struct GenreRecord {
    db_id: Option<i64>,
    id: String,
    name: String,
}

impl From<db::genres::Genre> for GenreRecord {
    fn from(genre: db::genres::Genre) -> Self {
        Self {
            db_id: genre.db_id.map(DbId::from).map(|id| id.0),
            id: genre.id,
            name: genre.name,
        }
    }
}

fn write_optional_genre(
    returns: &mut luau::ReturnWriter<'_>,
    genre: Option<db::genres::Genre>,
) -> luau::runtime::Result<()> {
    match genre {
        Some(genre) => returns.write(harmony_luau::serializable_to_luau_owned(
            GenreRecord::from(genre),
        )?),
        None => returns.write(luau::Value::Nil),
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

fn set_db_id_key(
    vm: &luau::Vm,
    table: &luau::Table,
    key: DbId,
    value: luau::Value,
) -> luau::runtime::Result<()> {
    match i32::try_from(key.0) {
        Ok(integer_key) => {
            table.set_integer_raw(vm, integer_key, value.clone())?;
            table.set_key_raw(vm, luau::Value::Integer(key.0), value)
        }
        Err(_) => {
            table.set_key_raw(vm, luau::Value::Integer(key.0), value.clone())?;
            table.set_raw(vm, &key.0.to_string(), value)
        }
    }
}

impl LuauTypeInfo for GenreRecord {
    fn luau_type() -> LuauType {
        LuauType::named("GenreInfo")
    }
}

impl DescribeInterface for GenreRecord {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("GenreInfo", None);
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
        ]);
        descriptor
    }
}

impl LuauTypeInfo for GenreExternalId {
    fn luau_type() -> LuauType {
        LuauType::named("GenreExternalId")
    }
}

impl DescribeInterface for GenreExternalId {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("GenreExternalId", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "provider_id",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "id_type",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "id",
                ty: String::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for GenreAliasInput {
    fn luau_type() -> LuauType {
        LuauType::named("GenreAliasInput")
    }
}

impl DescribeInterface for GenreAliasInput {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("GenreAliasInput", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "name",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "locale",
                ty: Option::<String>::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for GenreAddRequest {
    fn luau_type() -> LuauType {
        LuauType::named("GenreAddRequest")
    }
}

impl DescribeInterface for GenreAddRequest {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("GenreAddRequest", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "name",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "external_id",
                ty: Option::<GenreExternalId>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "aliases",
                ty: Option::<Vec<GenreAliasInput>>::luau_type(),
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
        name: "Genres",
        local_name: "genres",
        description: None,
        fields: Vec::new(),
        functions: vec![
            ModuleFunctionDescriptor {
                path: vec!["add"],
                description: None,
                params: vec![
                    param("release_id", i64::luau_type()),
                    param("request", GenreAddRequest::luau_type()),
                ],
                returns: vec![i64::luau_type()],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["resolve"],
                description: None,
                params: vec![param("request", GenreAddRequest::luau_type())],
                returns: vec![i64::luau_type()],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["add_parent"],
                description: None,
                params: vec![
                    param("child_id", i64::luau_type()),
                    param("parent_id", i64::luau_type()),
                ],
                returns: Vec::new(),
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_by_id"],
                description: None,
                params: vec![param("genre_id", i64::luau_type())],
                returns: vec![Option::<GenreRecord>::luau_type()],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["find_by_name"],
                description: None,
                params: vec![param("name", String::luau_type())],
                returns: vec![Option::<GenreRecord>::luau_type()],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_parents"],
                description: None,
                params: vec![param("genre_id", i64::luau_type())],
                returns: vec![Vec::<GenreRecord>::luau_type()],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_children"],
                description: None,
                params: vec![param("genre_id", i64::luau_type())],
                returns: vec![Vec::<GenreRecord>::luau_type()],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_releases"],
                description: None,
                params: vec![param("genre_id", i64::luau_type())],
                returns: vec![Vec::<i64>::luau_type()],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_releases_many"],
                description: None,
                params: vec![param("genre_ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(u64::luau_type(), Vec::<i64>::luau_type())],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_for_release"],
                description: None,
                params: vec![param("release_id", i64::luau_type())],
                returns: vec![Vec::<GenreRecord>::luau_type()],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_for_releases_many"],
                description: None,
                params: vec![param("release_ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(
                    u64::luau_type(),
                    Vec::<GenreRecord>::luau_type(),
                )],
                yields: false,
            },
        ],
    }
}

#[cfg(any(feature = "docgen", test))]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &module_descriptor(),
        &[],
        &[
            GenreRecord::interface_descriptor(),
            GenreExternalId::interface_descriptor(),
            GenreAliasInput::interface_descriptor(),
            GenreAddRequest::interface_descriptor(),
        ],
        &[],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_genres_module_definition() {
        let rendered = render_luau_definition().expect("render lyra/genres docs");

        assert!(rendered.contains("@interface GenreInfo"));
        assert!(rendered.contains("@interface GenreExternalId"));
        assert!(rendered.contains("@interface GenreAliasInput"));
        assert!(rendered.contains("@interface GenreAddRequest"));
        assert!(rendered.contains("aliases: {GenreAliasInput}?"));
        assert!(rendered.contains("@class Genres"));
        assert!(
            rendered.contains(
                "function genres.add(release_id: number, request: GenreAddRequest): number"
            )
        );
        assert!(
            rendered.contains("function genres.add_parent(child_id: number, parent_id: number)")
        );
        assert!(rendered.contains(
            "function genres.get_for_releases_many(release_ids: {number}): { [number]: {GenreInfo} }"
        ));
    }
}
