// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::Arc;

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

use crate::plugins::args;
use crate::{
    plugins::db,
    services::favorites as favorite_service,
};
use lyra_db::favorites::FavoriteKind;

struct FavoritesModule;

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/favorites")
        .function(add_spec())
        .function(remove_spec())
        .function(has_spec())
        .function(has_many_spec())
        .function(list_ids_spec())
        .install(|_| Ok(ModuleExport::new(FavoritesModule)))
}

fn add_spec() -> FunctionSpec {
    FunctionSpec::new("add")
        .arg_name("target_id")
        .call_async(Arc::new(add_callback))
}

fn remove_spec() -> FunctionSpec {
    FunctionSpec::new("remove")
        .arg_name("target_id")
        .call_async(Arc::new(remove_callback))
}

fn has_spec() -> FunctionSpec {
    FunctionSpec::new("has")
        .arg_name("target_id")
        .call_async(Arc::new(has_callback))
}

fn has_many_spec() -> FunctionSpec {
    FunctionSpec::new("has_many")
        .arg_name("target_ids")
        .call_async(Arc::new(has_many_callback))
}

fn list_ids_spec() -> FunctionSpec {
    FunctionSpec::new("list_ids")
        .arg_name("entity")
        .call_async(Arc::new(list_ids_callback))
}

fn add_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let target_id = DbId(frame.args.read_named::<i64>("target_id")?);
    let db = db::get(frame.vm)?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let mut db = db.write().await;
        let Some(public_target_id) = lyra_db::lookup::find_id_by_db_id(&*db, target_id)? else {
            return Ok(false);
        };
        let outcome = favorite_service::add_for_principal(&mut db, &principal, &public_target_id)?;
        Ok(matches!(
            outcome,
            favorite_service::MutationOutcome::Applied(_)
        ))
    }))
}

fn remove_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let target_id = DbId(frame.args.read_named::<i64>("target_id")?);
    let db = db::get(frame.vm)?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let mut db = db.write().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let outcome = favorite_service::remove_by_db_id(&mut db, user_db_id, target_id)?;
        Ok(matches!(
            outcome,
            favorite_service::MutationOutcome::Applied(_)
        ))
    }))
}

fn has_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let target_id = DbId(frame.args.read_named::<i64>("target_id")?);
    let db = db::get(frame.vm)?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let Some(public_target_id) = lyra_db::lookup::find_id_by_db_id(&*db, target_id)? else {
            return Ok(false);
        };
        let favored = favorite_service::has_for_principal(&db, &principal, &public_target_id)?;
        Ok(favored)
    }))
}

fn has_many_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let target_ids: luau::Table = frame.args.read_named("target_ids")?;
    let target_ids = args::unique_ids(frame.vm, &target_ids)?;
    let db = db::get(frame.vm)?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let public_ids_by_db_id = lyra_db::lookup::find_ids_by_db_ids(&*db, &target_ids)?;
        let public_ids = target_ids
            .iter()
            .filter_map(|id| public_ids_by_db_id.get(id).cloned())
            .collect::<Vec<_>>();
        let result = favorite_service::has_many_for_principal(&db, &principal, &public_ids)?;

        let mut table = luau::OwnedTable::with_entry_capacity(0, 0, target_ids.len());
        for id in target_ids {
            let favored = public_ids_by_db_id
                .get(&id)
                .and_then(|public_id| result.get(public_id).copied())
                .unwrap_or(false);
            table.set_key(id.0, favored);
        }
        Ok(table)
    }))
}

fn list_ids_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let entity: String = frame.args.read_named("entity")?;
    let kind = parse_kind(&entity)?;
    let db = db::get(frame.vm)?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let ids = favorite_service::list_ids(&db, user_db_id, kind)?;
        let mut visible_ids = Vec::new();
        for id in ids {
            if kind == FavoriteKind::Playlist
                || crate::services::auth::access::entity_accessible(&db, &principal, id)?
            {
                visible_ids.push(id);
            }
        }
        Ok(db_id_array(visible_ids))
    }))
}

fn parse_kind(value: &str) -> luau::runtime::Result<FavoriteKind> {
    FavoriteKind::try_from(value).map_err(crate::plugins::runtime_error)
}

fn db_id_array(ids: Vec<DbId>) -> luau::OwnedTable {
    luau::OwnedTable::from_array(ids.into_iter().map(|id| id.0))
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
                params: vec![param("target_id", i64::luau_type())],
                returns: vec![bool::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["remove"],
                description: None,
                params: vec![param("target_id", i64::luau_type())],
                returns: vec![bool::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["has"],
                description: None,
                params: vec![param("target_id", i64::luau_type())],
                returns: vec![bool::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["has_many"],
                description: None,
                params: vec![param("target_ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(u64::luau_type(), bool::luau_type())],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["list_ids"],
                description: None,
                params: vec![param("entity", String::luau_type())],
                returns: vec![Vec::<i64>::luau_type()],
                yields: true,
            },
        ],
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(&module_descriptor(), &[], &[], &[])
}
