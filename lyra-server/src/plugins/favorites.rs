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
        favorites::FavoriteKind,
    },
    services::favorites as favorite_service,
};

#[derive(Clone, Default)]
pub(crate) struct FavoritesModuleStore {
    db: Option<DbAsync>,
}

impl FavoritesModuleStore {
    pub(crate) fn empty() -> Self {
        Self { db: None }
    }

    pub(crate) fn with_db(db: DbAsync) -> Self {
        Self { db: Some(db) }
    }

    fn db(&self) -> luau::runtime::Result<DbAsync> {
        self.db.clone().ok_or_else(|| {
            crate::plugins::runtime_error(
                "lyra/favorites requires a database-backed plugin executor",
            )
        })
    }
}

struct FavoritesModule;

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/favorites")
        .capability("lyra.favorites")
        .function(add_spec())
        .function(remove_spec())
        .function(has_spec())
        .function(has_many_spec())
        .function(list_ids_spec())
        .install(|_| Ok(ModuleExport::new(FavoritesModule)))
}

fn add_spec() -> FunctionSpec {
    FunctionSpec::async_fn("add")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("user_id")
        .args::<i64>()
        .arg_name("target_id")
        .args::<i64>()
        .returns::<bool>()
        .call_async(Arc::new(add_callback))
}

fn remove_spec() -> FunctionSpec {
    FunctionSpec::async_fn("remove")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("user_id")
        .args::<i64>()
        .arg_name("target_id")
        .args::<i64>()
        .returns::<bool>()
        .call_async(Arc::new(remove_callback))
}

fn has_spec() -> FunctionSpec {
    FunctionSpec::async_fn("has")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("user_id")
        .args::<i64>()
        .arg_name("target_id")
        .args::<i64>()
        .returns::<bool>()
        .call_async(Arc::new(has_callback))
}

fn has_many_spec() -> FunctionSpec {
    FunctionSpec::async_fn("has_many")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("user_id")
        .args::<i64>()
        .arg_name("target_ids")
        .args::<luau::Table>()
        .returns::<luau::Table>()
        .call_async(Arc::new(has_many_callback))
}

fn list_ids_spec() -> FunctionSpec {
    FunctionSpec::async_fn("list_ids")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("user_id")
        .args::<i64>()
        .arg_name("entity")
        .args::<String>()
        .returns::<luau::Table>()
        .call_async(Arc::new(list_ids_callback))
}

fn add_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let user_id = DbId(frame.args.read_named::<i64>("user_id")?);
    let target_id = DbId(frame.args.read_named::<i64>("target_id")?);
    let store = frame
        .vm
        .data()
        .get::<FavoritesModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        if user_id != principal.user_db_id {
            return Ok(luau::Value::Boolean(false));
        }

        let mut db = db.write().await;
        let Some(public_target_id) =
            db::lookup::find_id_by_db_id(&*db, target_id).map_err(crate::plugins::runtime_error)?
        else {
            return Ok(luau::Value::Boolean(false));
        };
        let outcome = favorite_service::add_for_principal(&mut db, &principal, &public_target_id)
            .map_err(crate::plugins::runtime_error)?;
        Ok(luau::Value::Boolean(matches!(
            outcome,
            favorite_service::MutationOutcome::Applied(_)
        )))
    }))
}

fn remove_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let user_id = DbId(frame.args.read_named::<i64>("user_id")?);
    let target_id = DbId(frame.args.read_named::<i64>("target_id")?);
    let store = frame
        .vm
        .data()
        .get::<FavoritesModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        if user_id != principal.user_db_id {
            return Ok(luau::Value::Boolean(false));
        }

        let mut db = db.write().await;
        let outcome = favorite_service::remove_by_db_id(&mut db, principal.user_db_id, target_id)
            .map_err(crate::plugins::runtime_error)?;
        Ok(luau::Value::Boolean(matches!(
            outcome,
            favorite_service::MutationOutcome::Applied(_)
        )))
    }))
}

fn has_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let user_id = DbId(frame.args.read_named::<i64>("user_id")?);
    let target_id = DbId(frame.args.read_named::<i64>("target_id")?);
    let store = frame
        .vm
        .data()
        .get::<FavoritesModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        if user_id != principal.user_db_id {
            return Ok(luau::Value::Boolean(false));
        }

        let db = db.read().await;
        let Some(public_target_id) =
            db::lookup::find_id_by_db_id(&*db, target_id).map_err(crate::plugins::runtime_error)?
        else {
            return Ok(luau::Value::Boolean(false));
        };
        let favored = favorite_service::has_for_principal(&db, &principal, &public_target_id)
            .map_err(crate::plugins::runtime_error)?;
        Ok(luau::Value::Boolean(favored))
    }))
}

fn has_many_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let user_id = DbId(frame.args.read_named::<i64>("user_id")?);
    let target_ids: luau::Table = frame.args.read_named("target_ids")?;
    let target_ids = parse_db_ids(frame.vm, &target_ids)?;
    let store = frame
        .vm
        .data()
        .get::<FavoritesModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let result = if user_id == principal.user_db_id {
            let public_ids_by_db_id = db::lookup::find_ids_by_db_ids(&*db, &target_ids)
                .map_err(crate::plugins::runtime_error)?;
            let public_ids = target_ids
                .iter()
                .filter_map(|id| public_ids_by_db_id.get(id).cloned())
                .collect::<Vec<_>>();
            favorite_service::has_many_for_principal(&db, &principal, &public_ids)
                .map_err(crate::plugins::runtime_error)?
        } else {
            std::collections::HashMap::new()
        };

        let mut table = luau::OwnedTable::with_entry_capacity(0, 0, target_ids.len());
        for id in target_ids {
            let favored = db::lookup::find_id_by_db_id(&*db, id)
                .map_err(crate::plugins::runtime_error)?
                .and_then(|public_id| result.get(&public_id).copied())
                .unwrap_or(false);
            let value = luau::Value::Boolean(favored);
            table.set_key(luau::Value::Integer(id.0), value.clone());
            table.set_key(luau::Value::Number(id.0 as f64), value);
        }
        Ok(luau::Value::TableData(table))
    }))
}

fn list_ids_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let user_id = DbId(frame.args.read_named::<i64>("user_id")?);
    let entity: String = frame.args.read_named("entity")?;
    let kind = parse_kind(&entity)?;
    let store = frame
        .vm
        .data()
        .get::<FavoritesModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        if user_id != principal.user_db_id {
            return Ok(luau::Value::TableData(db_id_array(Vec::new())));
        }

        let db = db.read().await;
        let ids = favorite_service::list_ids(&db, principal.user_db_id, kind)
            .map_err(crate::plugins::runtime_error)?;
        let mut visible_ids = Vec::new();
        for id in ids {
            if kind == FavoriteKind::Playlist
                || crate::services::auth::access::entity_accessible(&db, &principal, id)
                    .map_err(crate::plugins::runtime_error)?
            {
                visible_ids.push(id);
            }
        }
        Ok(luau::Value::TableData(db_id_array(visible_ids)))
    }))
}

fn parse_kind(value: &str) -> luau::runtime::Result<FavoriteKind> {
    FavoriteKind::try_from(value).map_err(crate::plugins::runtime_error)
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

fn db_id_array(ids: Vec<DbId>) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(ids.len(), 0);
    for id in ids {
        table.push_array(luau::Value::Integer(id.0));
    }
    table
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
        name: "Favorites",
        local_name: "favorites",
        description: None,
        fields: Vec::new(),
        functions: vec![
            ModuleFunctionDescriptor {
                path: vec!["add"],
                description: None,
                params: vec![
                    param("user_id", i64::luau_type()),
                    param("target_id", i64::luau_type()),
                ],
                returns: vec![bool::luau_type()],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["remove"],
                description: None,
                params: vec![
                    param("user_id", i64::luau_type()),
                    param("target_id", i64::luau_type()),
                ],
                returns: vec![bool::luau_type()],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["has"],
                description: None,
                params: vec![
                    param("user_id", i64::luau_type()),
                    param("target_id", i64::luau_type()),
                ],
                returns: vec![bool::luau_type()],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["has_many"],
                description: None,
                params: vec![
                    param("user_id", i64::luau_type()),
                    param("target_ids", Vec::<u64>::luau_type()),
                ],
                returns: vec![LuauType::map(u64::luau_type(), bool::luau_type())],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["list_ids"],
                description: None,
                params: vec![
                    param("user_id", i64::luau_type()),
                    param("entity", String::luau_type()),
                ],
                returns: vec![Vec::<i64>::luau_type()],
                yields: false,
            },
        ],
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(&module_descriptor(), &[], &[], &[])
}
