// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::fmt;

use harmony_core::{
    Danger,
    Module,
    ModuleBuilder,
    ModuleExport,
};
use harmony_luau::{
    DescribeInterface,
    ModuleFunctionDescriptor,
};
use mlua::{
    ExternalResult,
    Lua,
    Result,
    Table,
};

use crate::{
    STATE,
    plugins::db::Release,
    plugins::db::ReleaseType,
    plugins::db::ResolveId,
    plugins::{
        PluginSortOrder,
        caller::RequestCaller,
        paged_result_to_table,
        parse_ids,
        parse_list_options,
    },
    services::releases as release_service,
};

#[harmony_macros::interface]
struct ReleaseQueryOptions {
    scope: Option<ResolveId>,
    artist_ids: Option<Vec<u64>>,
    sort_by: Option<Vec<String>>,
    sort_order: Option<PluginSortOrder>,
    offset: Option<u64>,
    limit: Option<u64>,
    search_term: Option<String>,
}

#[harmony_macros::interface]
struct ReleaseQueryResult {
    entities: Vec<Release>,
    total_count: u64,
    offset: u64,
}

/// Lists releases related to the given scope, or all releases by default.
async fn list(caller: RequestCaller, scope: Option<ResolveId>) -> Result<Vec<Release>> {
    let db = STATE.db.read().await;
    let query_id = match scope {
        Some(id) => id.to_query_id(&db).into_lua_err()?,
        None => None,
    };
    let mut releases = release_service::get(&db, query_id).into_lua_err()?;
    retain_accessible_releases(&db, &caller.principal, &mut releases).into_lua_err()?;

    Ok(releases)
}

/// Queries releases with pagination, sorting, and optional artist filters.
async fn query(lua: Lua, caller: RequestCaller, opts: Table) -> Result<Table> {
    let scope: Option<ResolveId> = opts.get("scope")?;
    let artist_ids: Option<Table> = opts.get("artist_ids")?;
    let list_options = parse_list_options(&opts)?;

    let db = STATE.db.read().await;
    let scope = match scope {
        Some(id) => id.to_query_id(&db).into_lua_err()?,
        None => None,
    };
    let result = if let Some(artist_ids) = artist_ids {
        let artist_ids = parse_ids(artist_ids)?;
        if artist_ids.is_empty() {
            release_service::query(&db, scope, &list_options).into_lua_err()?
        } else {
            release_service::query_by_artists(&db, &artist_ids, scope, &list_options)
                .into_lua_err()?
        }
    } else {
        release_service::query(&db, scope, &list_options).into_lua_err()?
    };
    let mut entries = result.entries;
    retain_accessible_releases(&db, &caller.principal, &mut entries).into_lua_err()?;
    paged_result_to_table(
        &lua,
        crate::plugins::db::PagedResult {
            total_count: entries.len() as u64,
            offset: result.offset,
            entries,
        },
    )
}

async fn get_by_artist(
    caller: RequestCaller,
    artist_id: crate::plugins::db::NodeId,
) -> Result<Vec<Release>> {
    let db = STATE.db.read().await;
    let artist_db_id = agdb::DbId::from(artist_id);
    if !crate::routes::entity_accessible_to_principal(&db, &caller.principal, artist_db_id)
        .into_lua_err()?
    {
        return Ok(Vec::new());
    }
    let mut releases =
        crate::plugins::db::releases::get_by_artist(&db, artist_db_id).into_lua_err()?;
    retain_accessible_releases(&db, &caller.principal, &mut releases).into_lua_err()?;
    Ok(releases)
}

async fn get_appearances(
    caller: RequestCaller,
    artist_id: crate::plugins::db::NodeId,
) -> Result<Vec<Release>> {
    let db = STATE.db.read().await;
    let artist_db_id = agdb::DbId::from(artist_id);
    if !crate::routes::entity_accessible_to_principal(&db, &caller.principal, artist_db_id)
        .into_lua_err()?
    {
        return Ok(Vec::new());
    }
    let mut releases = release_service::get_appearances(&db, artist_db_id).into_lua_err()?;
    retain_accessible_releases(&db, &caller.principal, &mut releases).into_lua_err()?;
    Ok(releases)
}

/// Lists related releases for each owner id.
async fn list_many(lua: Lua, caller: RequestCaller, ids: Table) -> Result<Table> {
    let ids = parse_ids(ids)?;
    let db = STATE.db.read().await;
    let related = release_service::get_many_by_track(&db, &ids).into_lua_err()?;
    let table = lua.create_table()?;
    for id in ids {
        let mut releases = related.get(&id).cloned().unwrap_or_default();
        retain_accessible_releases(&db, &caller.principal, &mut releases).into_lua_err()?;
        table.set(id.0, releases)?;
    }
    Ok(table)
}

fn retain_accessible_releases(
    db: &agdb::DbAny,
    principal: &crate::services::auth::Principal,
    releases: &mut Vec<Release>,
) -> anyhow::Result<()> {
    let mut retained = Vec::with_capacity(releases.len());
    for release in releases.drain(..) {
        let Some(release_db_id) = release.db_id.clone().map(agdb::DbId::from) else {
            continue;
        };
        if crate::routes::entity_accessible_to_principal(db, principal, release_db_id)? {
            retained.push(release);
        }
    }
    *releases = retained;
    Ok(())
}

fn module_export() -> ModuleExport {
    ModuleBuilder::empty("lyra/releases", "Releases", "releases")
        .scope_id(
            "lyra.releases",
            "Read and modify album releases.",
            Danger::Medium,
        )
        .with_interface(ReleaseQueryOptions::interface_descriptor())
        .with_interface(ReleaseQueryResult::interface_descriptor())
        .class::<Release>()
        .class::<ReleaseType>()
        .async_function_with_prelude(
            ModuleFunctionDescriptor::new("list")
                .description(
                    "Lists releases related to the given scope, or all releases by default.",
                )
                .param_type::<Option<ResolveId>>("scope")
                .returns::<Vec<Release>>()
                .yields(),
            crate::plugins::caller::request_caller_from_stack,
            |_, caller, scope| async move {
                let caller = caller?;
                list(caller, scope).await
            },
        )
        .async_function_with_prelude(
            ModuleFunctionDescriptor::new("query")
                .description(
                    "Queries releases with pagination, sorting, and optional artist filters.",
                )
                .param_type::<ReleaseQueryOptions>("opts")
                .returns::<ReleaseQueryResult>()
                .yields(),
            crate::plugins::caller::request_caller_from_stack,
            |lua, caller, opts| async move {
                let caller = caller?;
                query(lua, caller, opts).await
            },
        )
        .async_function_with_prelude(
            ModuleFunctionDescriptor::new("get_by_artist")
                .param_type::<crate::plugins::db::NodeId>("artist_id")
                .returns::<Vec<Release>>()
                .yields(),
            crate::plugins::caller::request_caller_from_stack,
            |_, caller, artist_id| async move {
                let caller = caller?;
                get_by_artist(caller, artist_id).await
            },
        )
        .async_function_with_prelude(
            ModuleFunctionDescriptor::new("get_appearances")
                .param_type::<crate::plugins::db::NodeId>("artist_id")
                .returns::<Vec<Release>>()
                .yields(),
            crate::plugins::caller::request_caller_from_stack,
            |_, caller, artist_id| async move {
                let caller = caller?;
                get_appearances(caller, artist_id).await
            },
        )
        .async_function_with_prelude(
            ModuleFunctionDescriptor::new("list_many")
                .description("Lists related releases for each owner id.")
                .param_type::<Vec<u64>>("ids")
                .returns::<std::collections::BTreeMap<u64, Vec<Release>>>()
                .yields(),
            crate::plugins::caller::request_caller_from_stack,
            |lua, caller, ids| async move {
                let caller = caller?;
                list_many(lua, caller, ids).await
            },
        )
        .build()
}

pub(crate) fn get_module() -> Module {
    module_export().into_module()
}

pub(crate) fn render_luau_definition() -> std::result::Result<String, fmt::Error> {
    module_export().render_luau_definition()
}
