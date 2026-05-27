// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::{
        HashMap,
        HashSet,
    },
    sync::Arc,
};

use agdb::{
    DbAny,
    DbId,
    QueryId,
};
use harmony_core::{
    FunctionSpec,
    ModuleExport,
    ModuleSpec,
};
use harmony_luau as luau;
#[cfg(any(feature = "docgen", test))]
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
        Cover,
        DbAsync,
        ResolveId,
    },
    services::auth::Principal,
};

#[derive(Clone, Default)]
pub(crate) struct CoversModuleStore {
    db: Option<DbAsync>,
}

impl CoversModuleStore {
    pub(crate) fn empty() -> Self {
        Self { db: None }
    }

    pub(crate) fn with_db(db: DbAsync) -> Self {
        Self { db: Some(db) }
    }

    fn db(&self) -> luau::runtime::Result<DbAsync> {
        self.db.clone().ok_or_else(|| {
            crate::plugins::runtime_error("lyra/covers requires a database-backed plugin executor")
        })
    }
}

enum CoverValidity {
    Valid(Cover),
    NotFound,
    Unavailable,
}

struct CoversModule;

#[cfg(any(feature = "docgen", test))]
struct CoverInfo;

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/covers")
        .capability("lyra.covers")
        .function(get_spec())
        .function(get_many_spec())
        .install(|_| Ok(ModuleExport::new(CoversModule)))
}

fn get_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get")
        .context::<Principal>()
        .arg_name("id")
        .args::<ResolveId>()
        .returns::<Option<luau::Table>>()
        .call_async(Arc::new(get_callback))
}

fn get_many_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get_many")
        .context::<Principal>()
        .arg_name("ids")
        .args::<luau::Table>()
        .returns::<luau::Table>()
        .call_async(Arc::new(get_many_callback))
}

fn get_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let id = parse_resolve_id(frame.args.read_named::<luau::Value>("id")?)?;
    let store = frame.vm.data().get::<CoversModuleStore>()?.as_ref().clone();
    let db = store.db()?;
    let principal = (*frame.context.caller.get::<Principal>()?).clone();

    Ok(luau::ScheduledFuture::new(async move {
        let (resolved_cover, stale_owners) = {
            let db_read = db.read().await;
            let Some(QueryId::Id(item_id)) = id
                .to_query_id(&db_read)
                .map_err(crate::plugins::runtime_error)?
            else {
                return Ok(luau::Value::Nil);
            };
            if !crate::routes::entity_accessible_to_principal(&*db_read, &principal, item_id)
                .map_err(crate::plugins::runtime_error)?
            {
                return Ok(luau::Value::Nil);
            }
            let mut stale_owners = Vec::new();
            let result = resolve_persisted_cover(&db_read, item_id, &mut stale_owners)
                .map_err(crate::plugins::runtime_error)?;
            (result, stale_owners)
        };

        if !stale_owners.is_empty() {
            remove_still_stale_covers(db.clone(), stale_owners).await?;
        }

        let Some((owner_id, cover)) = resolved_cover else {
            return Ok(luau::Value::Nil);
        };
        Ok(luau::Value::TableData(cover_to_table(owner_id, cover)))
    }))
}

fn get_many_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let ids: luau::Table = frame.args.read_named("ids")?;
    let item_ids = parse_db_ids(frame.vm, &ids)?;
    let store = frame.vm.data().get::<CoversModuleStore>()?.as_ref().clone();
    let db = store.db()?;
    let principal = (*frame.context.caller.get::<Principal>()?).clone();

    Ok(luau::ScheduledFuture::new(async move {
        let (resolved, stale_owners) = {
            let db_read = db.read().await;
            let mut stale_owners = Vec::new();
            let mut accessible_ids = Vec::new();
            for item_id in &item_ids {
                if crate::routes::entity_accessible_to_principal(&*db_read, &principal, *item_id)
                    .map_err(crate::plugins::runtime_error)?
                {
                    accessible_ids.push(*item_id);
                }
            }
            let result = resolve_persisted_covers(&db_read, &accessible_ids, &mut stale_owners)
                .map_err(crate::plugins::runtime_error)?;
            (result, stale_owners)
        };

        if !stale_owners.is_empty() {
            remove_still_stale_covers(db.clone(), stale_owners).await?;
        }

        let mut table = luau::OwnedTable::with_entry_capacity(0, 0, item_ids.len());
        for item_id in item_ids {
            let value = resolved
                .get(&item_id)
                .cloned()
                .map(|(owner_id, cover)| luau::Value::TableData(cover_to_table(owner_id, cover)))
                .unwrap_or(luau::Value::Nil);
            table.set_key(luau::Value::Integer(item_id.0), value.clone());
            table.set_key(luau::Value::Number(item_id.0 as f64), value);
        }

        Ok(luau::Value::TableData(table))
    }))
}

fn check_cover_validity(cover: Cover) -> CoverValidity {
    match std::fs::metadata(&cover.path) {
        Ok(meta) if meta.is_file() => CoverValidity::Valid(cover),
        Ok(_) => CoverValidity::NotFound,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => CoverValidity::NotFound,
        Err(_) => CoverValidity::Unavailable,
    }
}

fn resolve_persisted_cover(
    db: &DbAny,
    item_id: DbId,
    stale_owners: &mut Vec<DbId>,
) -> anyhow::Result<Option<(DbId, Cover)>> {
    let resolved = resolve_persisted_covers(db, &[item_id], stale_owners)?;
    Ok(resolved.into_values().next())
}

fn resolve_persisted_covers(
    db: &DbAny,
    item_ids: &[DbId],
    stale_owners: &mut Vec<DbId>,
) -> anyhow::Result<HashMap<DbId, (DbId, Cover)>> {
    let mut unique_ids = Vec::new();
    let mut seen = HashSet::new();
    for item_id in item_ids {
        if item_id.0 <= 0 {
            continue;
        }
        if seen.insert(*item_id) {
            if db::entities::get_element_type(db, *item_id)?.is_none() {
                continue;
            }
            unique_ids.push(*item_id);
        }
    }

    let direct_covers = db::covers::get_many(db, &unique_ids)?;

    let mut resolved = HashMap::new();
    let mut unresolved = Vec::new();
    for item_id in unique_ids {
        if let Some(cover) = direct_covers.get(&item_id) {
            match check_cover_validity(cover.clone()) {
                CoverValidity::Valid(valid) => {
                    resolved.insert(item_id, (item_id, valid));
                    continue;
                }
                CoverValidity::NotFound => stale_owners.push(item_id),
                CoverValidity::Unavailable => {}
            }
        }
        unresolved.push(item_id);
    }

    if unresolved.is_empty() {
        return Ok(resolved);
    }

    let track_releases = db::releases::get_by_tracks(db, &unresolved)?;
    let mut release_ids = Vec::new();
    let mut seen_release_ids = HashSet::new();
    for related_releases in track_releases.values() {
        for release in related_releases {
            let Some(release_id) = release.db_id.clone().map(Into::<DbId>::into) else {
                continue;
            };
            if seen_release_ids.insert(release_id) {
                release_ids.push(release_id);
            }
        }
    }

    let covers_by_release = db::covers::get_many(db, &release_ids)?;

    for item_id in unresolved {
        let Some(mut releases) = track_releases.get(&item_id).cloned() else {
            continue;
        };
        releases
            .sort_by_key(|release| release.db_id.clone().map(Into::<DbId>::into).map(|id| id.0));

        for release in releases {
            let Some(release_id) = release.db_id.clone().map(Into::<DbId>::into) else {
                continue;
            };
            let Some(cover) = covers_by_release.get(&release_id) else {
                continue;
            };
            match check_cover_validity(cover.clone()) {
                CoverValidity::Valid(valid) => {
                    resolved.insert(item_id, (release_id, valid));
                    break;
                }
                CoverValidity::NotFound => stale_owners.push(release_id),
                CoverValidity::Unavailable => {}
            }
        }
    }

    Ok(resolved)
}

async fn remove_still_stale_covers(
    db: DbAsync,
    stale_owners: Vec<DbId>,
) -> luau::runtime::Result<()> {
    let mut seen = HashSet::new();
    let mut db_write = db.write().await;
    for owner_id in stale_owners {
        if !seen.insert(owner_id) {
            continue;
        }
        let Ok(Some(cover)) = db::covers::get(&*db_write, owner_id) else {
            continue;
        };
        if matches!(check_cover_validity(cover), CoverValidity::NotFound) {
            let _ = db::covers::remove(&mut *db_write, owner_id);
        }
    }
    Ok(())
}

fn cover_to_table(owner_db_id: DbId, cover: Cover) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(0, 5);
    table.set_field("path", luau::Value::String(cover.path.into_bytes()));
    table.set_field(
        "mime_type",
        luau::Value::String(cover.mime_type.into_bytes()),
    );
    table.set_field("hash", luau::Value::String(cover.hash.into_bytes()));
    table.set_field(
        "blurhash",
        cover
            .blurhash
            .map(|value| luau::Value::String(value.into_bytes()))
            .unwrap_or(luau::Value::Nil),
    );
    table.set_field("release_id", luau::Value::Integer(owner_db_id.0));
    table
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

#[cfg(any(feature = "docgen", test))]
impl LuauTypeInfo for CoverInfo {
    fn luau_type() -> LuauType {
        LuauType::named("CoverInfo")
    }
}

#[cfg(any(feature = "docgen", test))]
impl DescribeInterface for CoverInfo {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("CoverInfo", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "path",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "mime_type",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "hash",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "blurhash",
                ty: Option::<String>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "release_id",
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
fn resolve_id_type() -> LuauType {
    LuauType::union(vec![i64::luau_type(), String::luau_type()])
}

#[cfg(any(feature = "docgen", test))]
fn module_descriptor() -> ModuleDescriptor {
    ModuleDescriptor {
        name: "Covers",
        local_name: "covers",
        description: None,
        fields: Vec::new(),
        functions: vec![
            ModuleFunctionDescriptor {
                path: vec!["get"],
                description: None,
                params: vec![param("id", resolve_id_type())],
                returns: vec![Option::<CoverInfo>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_many"],
                description: None,
                params: vec![param("ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(
                    u64::luau_type(),
                    Option::<CoverInfo>::luau_type(),
                )],
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
        &[CoverInfo::interface_descriptor()],
        &[],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_covers_module_definition() {
        let rendered = render_luau_definition().expect("render lyra/covers docs");

        assert!(rendered.contains("@interface CoverInfo"));
        assert!(rendered.contains("blurhash: string?"));
        assert!(rendered.contains("@class Covers"));
        assert!(rendered.contains("@yields"));
        assert!(rendered.contains("function covers.get(id: number | string): CoverInfo?"));
        assert!(
            rendered.contains("function covers.get_many(ids: {number}): { [number]: CoverInfo? }")
        );
    }
}
