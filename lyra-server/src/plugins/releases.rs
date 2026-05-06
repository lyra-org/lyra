// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use harmony_core::LuaAsyncExt;
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

struct ReleasesModule;

#[harmony_macros::module(
    plugin_scoped,
    name = "Releases",
    local = "releases",
    path = "lyra/releases",
    interfaces(ReleaseQueryOptions, ReleaseQueryResult),
    classes(Release, ReleaseType)
)]
impl ReleasesModule {
    /// Lists releases related to the given scope, or all releases by default.
    pub(crate) async fn list(
        #[harmony_context] caller: RequestCaller,
        scope: Option<ResolveId>,
    ) -> Result<Vec<Release>> {
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
    #[harmony(args(opts: ReleaseQueryOptions), returns(ReleaseQueryResult))]
    pub(crate) async fn query(
        lua: Lua,
        #[harmony_context] caller: RequestCaller,
        opts: Table,
    ) -> Result<Table> {
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

    pub(crate) async fn get_by_artist(
        #[harmony_context] caller: RequestCaller,
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

    pub(crate) async fn get_appearances(
        #[harmony_context] caller: RequestCaller,
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
    #[harmony(args(ids: Vec<u64>), returns(std::collections::BTreeMap<u64, Vec<Release>>))]
    pub(crate) async fn list_many(
        lua: Lua,
        #[harmony_context] caller: RequestCaller,
        ids: Table,
    ) -> Result<Table> {
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

crate::plugins::plugin_surface_exports!(
    ReleasesModule,
    "lyra.releases",
    "Read and modify album releases.",
    Medium
);
