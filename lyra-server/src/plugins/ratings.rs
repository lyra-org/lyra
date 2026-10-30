// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::Arc;

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
use lyra_db::ratings::{
    MAX_VALUE,
    MIN_VALUE,
    RatingValue,
};

use crate::{
    plugins::{
        args,
        db,
    },
    services::ratings as rating_service,
};

struct RatingsModule;

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/ratings")
        .function(set_spec())
        .function(remove_spec())
        .function(get_spec())
        .install(|_| Ok(ModuleExport::new(RatingsModule)))
}

fn set_spec() -> FunctionSpec {
    FunctionSpec::new("set")
        .arg_name("target_id")
        .arg_name("rating")
        .call_async(Arc::new(set_callback))
}

fn remove_spec() -> FunctionSpec {
    FunctionSpec::new("remove")
        .arg_name("target_id")
        .call_async(Arc::new(remove_callback))
}

fn get_spec() -> FunctionSpec {
    FunctionSpec::new("get")
        .arg_name("target_ids")
        .call_async(Arc::new(get_callback))
}

fn set_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let target_id = args::positive_id(frame.args.read_named::<i64>("target_id")?, "target_id")?;
    let rating = frame.args.read_named::<i64>("rating")?;
    let value = u8::try_from(rating)
        .ok()
        .and_then(RatingValue::new)
        .ok_or_else(|| {
            crate::plugins::runtime_error(format!(
                "rating must be an integer from {MIN_VALUE} to {MAX_VALUE}, got {rating}"
            ))
        })?;
    let db = db::get(frame.vm)?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let mut db = db.write().await;
        let Some(public_target_id) = lyra_db::lookup::find_id_by_db_id(&*db, target_id)
            .map_err(crate::plugins::runtime_error)?
        else {
            return Ok(luau::Value::Boolean(false));
        };
        let outcome =
            rating_service::set_for_principal(&mut db, &principal, &public_target_id, value)
                .map_err(crate::plugins::runtime_error)?;
        Ok(luau::Value::Boolean(matches!(
            outcome,
            rating_service::MutationOutcome::Applied(_)
        )))
    }))
}

fn remove_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let target_id = args::positive_id(frame.args.read_named::<i64>("target_id")?, "target_id")?;
    let db = db::get(frame.vm)?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let mut db = db.write().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let Some(public_target_id) = lyra_db::lookup::find_id_by_db_id(&*db, target_id)
            .map_err(crate::plugins::runtime_error)?
        else {
            return Ok(luau::Value::Boolean(false));
        };
        let outcome = rating_service::remove(&mut db, user_db_id, &public_target_id)
            .map_err(crate::plugins::runtime_error)?;
        Ok(luau::Value::Boolean(matches!(
            outcome,
            rating_service::MutationOutcome::Applied(_)
        )))
    }))
}

fn get_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let target_ids: luau::Table = frame.args.read_named("target_ids")?;
    let target_ids = args::unique_ids(frame.vm, &target_ids)?;
    let db = db::get(frame.vm)?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let public_ids_by_db_id = lyra_db::lookup::find_ids_by_db_ids(&*db, &target_ids)
            .map_err(crate::plugins::runtime_error)?;
        let public_ids = public_ids_by_db_id.values().cloned().collect::<Vec<_>>();
        let values = rating_service::values_for_principal(&db, &principal, &public_ids)
            .map_err(crate::plugins::runtime_error)?;

        let mut table = luau::OwnedTable::with_entry_capacity(0, 0, target_ids.len());
        for (id, public_id) in public_ids_by_db_id {
            if let Some(value) = values.get(&public_id).copied().flatten() {
                table.set_key(
                    luau::Value::from(id.0),
                    luau::Value::from(i64::from(value.get())),
                );
            }
        }
        Ok(luau::Value::TableData(table))
    }))
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
        name: "Ratings",
        local_name: "ratings",
        description: Some("The dispatch principal's 1-5 ratings of tracks, releases and artists."),
        fields: Vec::new(),
        functions: vec![
            ModuleFunctionDescriptor {
                path: vec!["set"],
                description: Some(
                    "Sets the caller's rating of the target. Returns false when the target is not a track, release or artist the caller can see. Raises on a rating outside 1-5.",
                ),
                params: vec![
                    param("target_id", u64::luau_type()),
                    param("rating", u8::luau_type()),
                ],
                returns: vec![bool::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["remove"],
                description: Some(
                    "Clears the caller's rating of the target, even one the caller can no longer see. Returns false when the target is not a track, release or artist.",
                ),
                params: vec![param("target_id", u64::luau_type())],
                returns: vec![bool::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get"],
                description: Some(
                    "The caller's ratings of the targets, keyed by id. Unrated targets and ones the caller cannot see are absent.",
                ),
                params: vec![param("target_ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(u64::luau_type(), u8::luau_type())],
                yields: true,
            },
        ],
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(&module_descriptor(), &[], &[], &[])
}
