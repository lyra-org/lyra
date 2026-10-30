// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::DbId;
use harmony_core::{
    FunctionSpec,
    ModuleExport,
    ModuleSpec,
};
use harmony_luau as luau;
use harmony_luau::IntoLuauReturn;
#[cfg(feature = "docgen")]
use harmony_luau::{
    LuauType,
    LuauTypeInfo,
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    render_definition_file_with_support,
};

use crate::plugins::db;

struct TrackSourcesModule;

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/track_sources")
        .function(get_primary_source_key_spec())
        .install(|_| Ok(ModuleExport::new(TrackSourcesModule)))
}

fn get_primary_source_key_spec() -> FunctionSpec {
    FunctionSpec::new("get_primary_source_key")
        .arg_name("track_id")
        .call_async(std::sync::Arc::new(get_primary_source_key_callback))
}

fn get_primary_source_key_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let track_id: i64 = frame.args.read_named("track_id")?;
    if track_id <= 0 {
        return Ok(luau::ScheduledFuture::new(async { Ok(luau::Value::Nil) }));
    }

    let db = db::scoped(&frame)?;

    Ok(luau::ScheduledFuture::new(async move {
        let (db, viewer) = db.read().await?;
        if !viewer
            .sees_entity(&*db, DbId(track_id))
            .map_err(crate::plugins::runtime_error)?
        {
            return None::<String>.into_luau_return();
        }
        let source = lyra_db::track_sources::get_primary_by_track(&db, DbId(track_id))
            .map_err(crate::plugins::runtime_error)?;
        let source_key = source
            .map(|source| source.source_key)
            .filter(|value| !value.trim().is_empty());
        source_key.into_luau_return()
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
        name: "TrackSources",
        local_name: "track_sources",
        description: None,
        fields: Vec::new(),
        functions: vec![ModuleFunctionDescriptor {
            path: vec!["get_primary_source_key"],
            description: None,
            params: vec![param("track_id", i64::luau_type())],
            returns: vec![Option::<String>::luau_type()],
            yields: true,
        }],
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(&module_descriptor(), &[], &[], &[])
}
