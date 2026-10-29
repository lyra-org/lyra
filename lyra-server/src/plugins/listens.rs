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
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    render_definition_file_with_support,
};
use harmony_luau::{
    DescribeTypeAlias,
    FunctionParameter,
    LuauType,
    LuauTypeInfo,
    TypeAliasDescriptor,
};

use crate::plugins::args;
use crate::{
    plugins::db,
    services::{
        auth::Principal,
        listens::ListenUpdatePayload,
        playback_sessions,
        providers::provider_registry,
    },
};
use lyra_db::DbAsync;

struct ListensModule;
struct ListenUpdateHandler;

impl LuauTypeInfo for ListenUpdateHandler {
    fn luau_type() -> LuauType {
        LuauType::literal("ListenUpdateHandler")
    }
}

impl DescribeTypeAlias for ListenUpdateHandler {
    fn type_alias_descriptor() -> TypeAliasDescriptor {
        TypeAliasDescriptor::new(
            "ListenUpdateHandler",
            LuauType::function(
                vec![FunctionParameter {
                    name: Some("payload"),
                    ty: LuauType::literal("ListenUpdatePayload"),
                    variadic: false,
                }],
                vec![],
            ),
            Some("Listen update callback."),
        )
    }
}

struct ResolvedStats {
    counts: HashMap<DbId, u64>,
    last_played: HashMap<DbId, u64>,
}

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/listens")
        .capability("lyra.listens")
        .function(get_stats_spec())
        .function(on_update_spec())
        .install(|_| Ok(ModuleExport::new(ListensModule)))
}

fn get_stats_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get_stats")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("track_ids")
        .args::<luau::Table>()
        .arg_name("merge_unique_external_ids")
        .args::<Option<bool>>()
        .returns::<luau::Table>()
        .call_async(Arc::new(get_stats_callback))
}

fn on_update_spec() -> FunctionSpec {
    FunctionSpec::async_fn("on_update")
        .arg_name("handler")
        .args::<ListenUpdateHandler>()
        .call_async(Arc::new(on_update_callback))
}

fn on_update_callback(
    frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    crate::plugins::update_callbacks::register::<ListenUpdatePayload>(frame, "listens.on_update")
}

fn get_stats_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let track_ids: luau::Table = frame.args.read_named("track_ids")?;
    let track_ids = args::unique_ids(frame.vm, &track_ids)?;
    let merge = frame
        .args
        .read_optional_named::<bool>("merge_unique_external_ids")?
        .unwrap_or(false);
    let db = db::get(frame.vm)?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let stats = resolve_stats(db, &track_ids, &principal, merge).await?;
        let mut table = luau::OwnedTable::with_capacity(0, 2);
        table.set_field(
            "counts",
            luau::Value::TableData(dbid_map_to_table(&stats.counts)),
        );
        table.set_field(
            "last_played",
            luau::Value::TableData(dbid_map_to_table(&stats.last_played)),
        );
        Ok(luau::Value::TableData(table))
    }))
}

async fn resolve_stats(
    db: DbAsync,
    track_ids: &[DbId],
    principal: &Principal,
    merge_unique_external_ids: bool,
) -> luau::runtime::Result<ResolvedStats> {
    let db = db.read().await;
    let viewer_db_id = crate::plugins::auth::require_user_db_id(principal, &db)?;

    if !merge_unique_external_ids {
        let mut counts = HashMap::new();
        let mut last_played = HashMap::new();
        let mut accessible_track_ids = Vec::new();
        for track_id in track_ids {
            counts.insert(*track_id, 0);
            if crate::services::auth::access::entity_accessible(&db, principal, *track_id)
                .map_err(crate::plugins::runtime_error)?
            {
                accessible_track_ids.push(*track_id);
            }
        }

        let stats = lyra_db::listens::get_stats(&db, &accessible_track_ids, viewer_db_id)
            .map_err(crate::plugins::runtime_error)?;
        for stat in stats {
            counts.insert(stat.db_id, stat.count);
            if let Some(last) = stat.last_played {
                last_played.insert(stat.db_id, last);
            }
        }
        return Ok(ResolvedStats {
            counts,
            last_played,
        });
    }

    let unique_track_id_pairs = {
        let registry = provider_registry().read_owned().await;
        registry.unique_track_id_pairs()
    };

    let mut requested_merged_ids = Vec::new();
    let mut merged_unique_ids = HashSet::new();
    let mut counts = HashMap::new();

    for track_id in track_ids {
        counts.insert(*track_id, 0);
        if !crate::services::auth::access::entity_accessible(&db, principal, *track_id)
            .map_err(crate::plugins::runtime_error)?
        {
            continue;
        }
        let merged_ids = playback_sessions::resolve_merged_track_ids_for_play_count(
            &db,
            *track_id,
            &unique_track_id_pairs,
        )
        .map_err(crate::plugins::runtime_error)?;
        let mut accessible_merged_ids = Vec::new();
        for merged_id in merged_ids {
            if crate::services::auth::access::entity_accessible(&db, principal, merged_id)
                .map_err(crate::plugins::runtime_error)?
            {
                merged_unique_ids.insert(merged_id);
                accessible_merged_ids.push(merged_id);
            }
        }
        requested_merged_ids.push((*track_id, accessible_merged_ids));
    }

    let merged_track_ids = merged_unique_ids.into_iter().collect::<Vec<_>>();
    let merged_stats = lyra_db::listens::get_stats(&db, &merged_track_ids, viewer_db_id)
        .map_err(crate::plugins::runtime_error)?;
    let merged_by_id: HashMap<DbId, &lyra_db::listens::ListenStats> =
        merged_stats.iter().map(|stat| (stat.db_id, stat)).collect();

    let mut last_played = HashMap::new();
    for (requested_id, merged_ids) in requested_merged_ids {
        let mut total_count = 0u64;
        let mut max_last_played: Option<u64> = None;
        for merged_id in merged_ids {
            if let Some(stat) = merged_by_id.get(&merged_id) {
                total_count = total_count.saturating_add(stat.count);
                if let Some(last) = stat.last_played
                    && last > max_last_played.unwrap_or(0)
                {
                    max_last_played = Some(last);
                }
            }
        }
        counts.insert(requested_id, total_count);
        if let Some(last) = max_last_played {
            last_played.insert(requested_id, last);
        }
    }

    Ok(ResolvedStats {
        counts,
        last_played,
    })
}

fn dbid_map_to_table(map: &HashMap<DbId, u64>) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_entry_capacity(0, 0, map.len());
    for (id, value) in map {
        let value = luau::Value::from(*value);
        table.set_key(luau::Value::from(id.0), value);
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
        name: "Listens",
        local_name: "listens",
        description: Some(
            "Listen counts of the dispatch principal, and every user's listens as they are recorded: like `playback_sessions.on_update` for playbacks, `on_update` delivers all users' listens, not only the caller's.",
        ),
        fields: Vec::new(),
        functions: vec![
            ModuleFunctionDescriptor {
                path: vec!["get_stats"],
                description: None,
                params: vec![
                    param("track_ids", Vec::<u64>::luau_type()),
                    param("merge_unique_external_ids", Option::<bool>::luau_type()),
                ],
                returns: vec![LuauType::map(
                    String::luau_type(),
                    LuauType::map(u64::luau_type(), u64::luau_type()),
                )],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["on_update"],
                description: Some(
                    "Registers a callback for every user's listens. A `recorded` update arrives once for each commit that stored listens, right after it, carrying all of that commit's listens for one user: one for a live play, up to a whole submitted batch. Each listen is announced once. Delivery is best effort: updates are dropped while the plugin runtime is saturated.",
                ),
                params: vec![param("handler", ListenUpdateHandler::luau_type())],
                returns: vec![],
                yields: true,
            },
        ],
    }
}

/// [`ListenUpdatePayload`] as described to Luau.
#[cfg(feature = "docgen")]
struct LuauListenUpdatePayload;

/// [`crate::services::listens::ListenPayload`] as described to Luau.
#[cfg(feature = "docgen")]
struct LuauListenPayload;

#[cfg(feature = "docgen")]
fn described_field(name: &'static str, ty: LuauType, description: &'static str) -> FieldDescriptor {
    FieldDescriptor {
        name,
        ty,
        description: Some(description),
    }
}

#[cfg(feature = "docgen")]
impl DescribeInterface for LuauListenUpdatePayload {
    fn interface_descriptor() -> InterfaceDescriptor {
        InterfaceDescriptor {
            name: "ListenUpdatePayload",
            description: Some("One user's listens recorded together, such as a submitted batch."),
            fields: vec![
                described_field(
                    "event",
                    LuauType::literal("\"recorded\""),
                    "What happened to the listens.",
                ),
                described_field("user_id", String::luau_type(), "The listener's public id."),
                described_field(
                    "listens",
                    LuauType::array(LuauType::named("ListenPayload")),
                    "The listens, at least one.",
                ),
            ],
        }
    }
}

#[cfg(feature = "docgen")]
impl DescribeInterface for LuauListenPayload {
    fn interface_descriptor() -> InterfaceDescriptor {
        InterfaceDescriptor {
            name: "ListenPayload",
            description: None,
            fields: vec![
                described_field("listen_id", String::luau_type(), "The listen's public id."),
                described_field("track_id", String::luau_type(), "The track's public id."),
                described_field(
                    "started_at_ms",
                    u64::luau_type(),
                    "When the play started, in Unix milliseconds.",
                ),
                described_field(
                    "played_ms",
                    u64::luau_type(),
                    "Time the track actually played when the listen was recorded.",
                ),
            ],
        }
    }
}

const _: fn(&ListenUpdatePayload) = |value| {
    let ListenUpdatePayload {
        event: _,
        user_id: _,
        listens,
    } = value;
    for crate::services::listens::ListenPayload {
        listen_id: _,
        track_id: _,
        started_at_ms: _,
        played_ms: _,
    } in listens
    {}
};

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &module_descriptor(),
        &[ListenUpdateHandler::type_alias_descriptor()],
        &[
            LuauListenUpdatePayload::interface_descriptor(),
            LuauListenPayload::interface_descriptor(),
        ],
        &[],
    )
}
