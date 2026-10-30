// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use harmony_core::{
    FunctionSpec,
    ModuleExport,
    ModuleSpec,
};
use harmony_luau as luau;
#[cfg(feature = "docgen")]
use harmony_luau::{
    DescribeInterface,
    LuauType,
    LuauTypeInfo,
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    render_definition_file_with_support,
};

use crate::plugins::catalog;
use crate::plugins::types::EntryInfo;
use crate::services::entries::{
    self,
    PlacedEntry,
};

struct EntriesModule;

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/entries")
        .function(get_spec())
        .function(by_parent_spec())
        .function(by_track_spec())
        .install(|_| Ok(ModuleExport::new(EntriesModule)))
}

fn get_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("ids")
        .args::<Vec<u64>>()
        .returns::<luau::Table>()
        .call_async(std::sync::Arc::new(get_callback))
}

fn by_parent_spec() -> FunctionSpec {
    FunctionSpec::async_fn("by_parent")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("parent_ids")
        .args::<Vec<u64>>()
        .returns::<luau::Table>()
        .call_async(std::sync::Arc::new(by_parent_callback))
}

fn by_track_spec() -> FunctionSpec {
    FunctionSpec::async_fn("by_track")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("track_ids")
        .args::<Vec<u64>>()
        .returns::<luau::Table>()
        .call_async(std::sync::Arc::new(by_track_callback))
}

fn get_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    catalog::keyed_lookup_shown(&mut frame, "ids", entries::get, EntryInfo::shown_to)
}

fn by_parent_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    catalog::keyed_lookup_shown(&mut frame, "parent_ids", entries::by_parent, show_all)
}

fn by_track_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    catalog::keyed_lookup_shown(&mut frame, "track_ids", entries::by_track, show_all)
}

fn show_all(
    entries: Vec<PlacedEntry>,
    viewer: &crate::services::catalog::Viewer,
) -> Vec<EntryInfo> {
    entries
        .into_iter()
        .map(|entry| EntryInfo::shown_to(entry, viewer))
        .collect()
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
    let entries = || LuauType::array(EntryInfo::luau_type());
    ModuleDescriptor {
        name: "Entries",
        local_name: "entries",
        description: None,
        fields: Vec::new(),
        functions: vec![
            ModuleFunctionDescriptor {
                path: vec!["get"],
                description: Some(
                    "The entries with these ids that the caller can see, keyed by id.",
                ),
                params: vec![param("ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(u64::luau_type(), EntryInfo::luau_type())],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["by_parent"],
                description: Some(
                    "The entries directly under each of these libraries and directory entries that the caller can see, by name, keyed by parent id.",
                ),
                params: vec![param("parent_ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(u64::luau_type(), entries())],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["by_track"],
                description: Some(
                    "The entries holding the audio of each of these tracks that the caller can see, keyed by track id.",
                ),
                params: vec![param("track_ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(u64::luau_type(), entries())],
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
        &[EntryInfo::interface_descriptor()],
        &[],
    )
}
