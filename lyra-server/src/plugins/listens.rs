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
    LuauType,
    LuauTypeInfo,
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    render_definition_file_with_support,
};

use crate::plugins::args;
use crate::{
    plugins::db::{
        self,
        DbAsync,
    },
    services::{
        auth::Principal,
        playback_sessions,
        providers::provider_registry,
    },
};

#[derive(Clone, Default)]
pub(crate) struct ListensModuleStore {
    db: Option<DbAsync>,
}

impl ListensModuleStore {
    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        Self { db: None }
    }

    pub(crate) fn with_db(db: DbAsync) -> Self {
        Self { db: Some(db) }
    }

    fn db(&self) -> luau::runtime::Result<DbAsync> {
        self.db.clone().ok_or_else(|| {
            crate::plugins::runtime_error("lyra/listens requires a database-backed plugin executor")
        })
    }
}

struct ListensModule;

struct ResolvedStats {
    counts: HashMap<DbId, u64>,
    last_played: HashMap<DbId, u64>,
}

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/listens")
        .capability("lyra.listens")
        .function(get_stats_spec())
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

fn get_stats_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let track_ids: luau::Table = frame.args.read_named("track_ids")?;
    let track_ids = args::unique_ids(frame.vm, &track_ids)?;
    let merge = frame
        .args
        .read_optional_named::<bool>("merge_unique_external_ids")?
        .unwrap_or(false);
    let store = frame
        .vm
        .data()
        .get::<ListensModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;
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

        let stats = db::listens::get_stats(&db, &accessible_track_ids, viewer_db_id)
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
    let merged_stats = db::listens::get_stats(&db, &merged_track_ids, viewer_db_id)
        .map_err(crate::plugins::runtime_error)?;
    let merged_by_id: HashMap<DbId, &db::listens::ListenStats> =
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
        description: Some("Listen counts of the dispatch principal."),
        fields: Vec::new(),
        functions: vec![ModuleFunctionDescriptor {
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
        }],
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(&module_descriptor(), &[], &[], &[])
}
