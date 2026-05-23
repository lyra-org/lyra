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
#[cfg(any(feature = "docgen", test))]
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
    },
    services::{
        auth::Principal,
        playback_sessions,
        providers::PROVIDER_REGISTRY,
    },
};

#[derive(Clone, Default)]
pub(crate) struct ListensModuleStore {
    db: Option<DbAsync>,
}

impl ListensModuleStore {
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
        .function(get_count_spec())
        .function(get_counts_spec())
        .function(get_stats_spec())
        .install(|_| Ok(ModuleExport::new(ListensModule)))
}

fn get_count_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get_count")
        .context::<Principal>()
        .arg_name("track_id")
        .args::<i64>()
        .arg_name("user_id")
        .args::<Option<i64>>()
        .arg_name("merge_unique_external_ids")
        .args::<Option<bool>>()
        .returns::<u64>()
        .call_async(Arc::new(get_count_callback))
}

fn get_counts_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get_counts")
        .context::<Principal>()
        .arg_name("track_ids")
        .args::<luau::Table>()
        .arg_name("user_id")
        .args::<Option<i64>>()
        .arg_name("merge_unique_external_ids")
        .args::<Option<bool>>()
        .returns::<luau::Table>()
        .call_async(Arc::new(get_counts_callback))
}

fn get_stats_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get_stats")
        .context::<Principal>()
        .arg_name("track_ids")
        .args::<luau::Table>()
        .arg_name("user_id")
        .args::<Option<i64>>()
        .arg_name("merge_unique_external_ids")
        .args::<Option<bool>>()
        .returns::<luau::Table>()
        .call_async(Arc::new(get_stats_callback))
}

fn get_count_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let track_id: i64 = frame.args.read_named("track_id")?;
    if track_id <= 0 {
        return Err(crate::plugins::runtime_error(
            "track_id must be a positive id",
        ));
    }
    let user_db_id = frame.args.read_optional_named::<i64>("user_id")?.map(DbId);
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
    let principal = (*frame.context.caller.get::<Principal>()?).clone();

    Ok(luau::ScheduledFuture::new(async move {
        let track_db_id = DbId(track_id);
        let stats = resolve_stats(db, &[track_db_id], &principal, user_db_id, merge).await?;
        let count = stats.counts.get(&track_db_id).copied().unwrap_or(0);
        Ok(luau::Value::Integer(saturating_i64(count)))
    }))
}

fn get_counts_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let track_ids: luau::Table = frame.args.read_named("track_ids")?;
    let track_ids = parse_db_ids(frame.vm, &track_ids)?;
    let user_db_id = frame.args.read_optional_named::<i64>("user_id")?.map(DbId);
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
    let principal = (*frame.context.caller.get::<Principal>()?).clone();

    Ok(luau::ScheduledFuture::new(async move {
        let stats = resolve_stats(db, &track_ids, &principal, user_db_id, merge).await?;
        Ok(luau::Value::TableData(dbid_map_to_table(&stats.counts)))
    }))
}

fn get_stats_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let track_ids: luau::Table = frame.args.read_named("track_ids")?;
    let track_ids = parse_db_ids(frame.vm, &track_ids)?;
    let user_db_id = frame.args.read_optional_named::<i64>("user_id")?.map(DbId);
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
    let principal = (*frame.context.caller.get::<Principal>()?).clone();

    Ok(luau::ScheduledFuture::new(async move {
        let stats = resolve_stats(db, &track_ids, &principal, user_db_id, merge).await?;
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
    user_db_id: Option<DbId>,
    merge_unique_external_ids: bool,
) -> luau::runtime::Result<ResolvedStats> {
    let db = db.read().await;

    if !merge_unique_external_ids {
        let mut counts = HashMap::new();
        let mut last_played = HashMap::new();
        let mut accessible_track_ids = Vec::new();
        for track_id in track_ids {
            counts.insert(*track_id, 0);
            if crate::routes::entity_accessible_to_principal(&db, principal, *track_id)
                .map_err(crate::plugins::runtime_error)?
            {
                accessible_track_ids.push(*track_id);
            }
        }

        let stats = db::listens::get_stats(&db, &accessible_track_ids, user_db_id)
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
        let registry = PROVIDER_REGISTRY.read().await;
        registry.unique_track_id_pairs()
    };

    let mut requested_merged_ids = Vec::new();
    let mut merged_unique_ids = HashSet::new();
    let mut counts = HashMap::new();

    for track_id in track_ids {
        counts.insert(*track_id, 0);
        if !crate::routes::entity_accessible_to_principal(&db, principal, *track_id)
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
            if crate::routes::entity_accessible_to_principal(&db, principal, merged_id)
                .map_err(crate::plugins::runtime_error)?
            {
                merged_unique_ids.insert(merged_id);
                accessible_merged_ids.push(merged_id);
            }
        }
        requested_merged_ids.push((*track_id, accessible_merged_ids));
    }

    let merged_track_ids = merged_unique_ids.into_iter().collect::<Vec<_>>();
    let merged_stats = db::listens::get_stats(&db, &merged_track_ids, user_db_id)
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

fn dbid_map_to_table(map: &HashMap<DbId, u64>) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_entry_capacity(0, 0, map.len());
    for (id, value) in map {
        let value = luau::Value::Integer(saturating_i64(*value));
        table.set_key(luau::Value::Integer(id.0), value.clone());
        table.set_key(luau::Value::Number(id.0 as f64), value);
    }
    table
}

fn saturating_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
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
        name: "Listens",
        local_name: "listens",
        description: None,
        fields: Vec::new(),
        functions: vec![
            ModuleFunctionDescriptor {
                path: vec!["get_count"],
                description: None,
                params: vec![
                    param("track_id", i64::luau_type()),
                    param("user_id", Option::<i64>::luau_type()),
                    param("merge_unique_external_ids", Option::<bool>::luau_type()),
                ],
                returns: vec![u64::luau_type()],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_counts"],
                description: None,
                params: vec![
                    param("track_ids", Vec::<u64>::luau_type()),
                    param("user_id", Option::<i64>::luau_type()),
                    param("merge_unique_external_ids", Option::<bool>::luau_type()),
                ],
                returns: vec![LuauType::map(u64::luau_type(), u64::luau_type())],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_stats"],
                description: None,
                params: vec![
                    param("track_ids", Vec::<u64>::luau_type()),
                    param("user_id", Option::<i64>::luau_type()),
                    param("merge_unique_external_ids", Option::<bool>::luau_type()),
                ],
                returns: vec![LuauType::map(
                    String::luau_type(),
                    LuauType::map(u64::luau_type(), u64::luau_type()),
                )],
                yields: false,
            },
        ],
    }
}

#[cfg(any(feature = "docgen", test))]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(&module_descriptor(), &[], &[], &[])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_listens_module_definition() {
        let rendered = render_luau_definition().expect("render lyra/listens docs");

        assert!(rendered.contains("@class Listens"));
        assert!(rendered.contains(
            "function listens.get_count(track_id: number, user_id: number?, merge_unique_external_ids: boolean?): number"
        ));
        assert!(rendered.contains(
            "function listens.get_counts(track_ids: {number}, user_id: number?, merge_unique_external_ids: boolean?): { [number]: number }"
        ));
        assert!(rendered.contains(
            "function listens.get_stats(track_ids: {number}, user_id: number?, merge_unique_external_ids: boolean?): { [string]: { [number]: number } }"
        ));
    }
}
