// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use crate::{
    STATE,
    plugins::caller::RequestCaller,
    plugins::db::{
        self,
        Artist,
        ArtistType,
        CreditType,
        Permission,
        Release,
        ReleaseType,
        ResolveId,
        Track,
    },
    services::auth::Principal,
    services::entities::{
        ArtistCreditSource,
        ArtistProjectionIncludes,
        ArtistProjectionInfo,
        ArtistProjectionKind,
        CreditProjectionInfo,
        CreditedArtistProjectionInfo,
        EntityInclude,
        EntityLookupHints,
        EntityProjectionInfo,
        ProjectionEntryInfo,
        ReleaseProjectionIncludes,
        ReleaseProjectionInfo,
        ReleaseProjectionKind,
        ReleaseProjectionTrack,
        TrackProjectionIncludes,
        TrackProjectionInfo,
        TrackProjectionKind,
        project_entities,
        project_entity,
    },
};
use agdb::{
    DbId,
    QueryId,
};
use harmony_core::LuaAsyncExt;
use harmony_luau::{
    DescribeTypeAlias,
    LuauType,
    LuauTypeInfo,
    TypeAliasDescriptor,
};
use mlua::{
    ExternalResult,
    FromLua,
    IntoLua,
    Lua,
    Result,
    Table,
    Value,
};
use std::collections::HashSet;

#[harmony_macros::interface]
struct EntityQueryRequest {
    id: ResolveId,
    include: Option<EntityIncludeSelector>,
    library_id: Option<i64>,
}

#[harmony_macros::interface]
struct EntityQueryManyRequest {
    ids: Vec<ResolveId>,
    include: Option<EntityIncludeSelector>,
    library_id: Option<i64>,
}

struct EntityIncludeSelector;

impl LuauTypeInfo for EntityIncludeSelector {
    fn luau_type() -> LuauType {
        LuauType::union(vec![
            String::luau_type(),
            LuauType::array(String::luau_type()),
        ])
    }
}

impl DescribeTypeAlias for EntityIncludeSelector {
    fn type_alias_descriptor() -> TypeAliasDescriptor {
        TypeAliasDescriptor::new(
            "EntityIncludeSelector",
            Self::luau_type(),
            Some("Entity include selector as a string or array of strings."),
        )
    }
}

fn normalize_includes(include_values: Vec<String>) -> Result<Vec<EntityInclude>> {
    let mut parsed = Vec::new();
    let mut seen = HashSet::new();

    for raw in include_values {
        let include_key = EntityInclude::parse(&raw).ok_or_else(|| {
            let valid: Vec<&str> = EntityInclude::ALL.iter().map(|i| i.as_key()).collect();
            mlua::Error::runtime(format!(
                "unknown include '{}'; expected one of: {}",
                raw,
                valid.join(", ")
            ))
        })?;
        if seen.insert(include_key) {
            parsed.push(include_key);
        }
    }

    Ok(parsed)
}

fn parse_include_values(include_value: Value) -> Result<Vec<String>> {
    match include_value {
        Value::Nil => Ok(Vec::new()),
        Value::String(value) => Ok(vec![value.to_str()?.to_string()]),
        Value::Table(values) => {
            let mut include_values = Vec::new();
            for value in values.sequence_values::<Value>() {
                match value? {
                    Value::String(raw) => include_values.push(raw.to_str()?.to_string()),
                    _ => {
                        return Err(mlua::Error::runtime(
                            "include entries must be strings when include is an array",
                        ));
                    }
                }
            }
            Ok(include_values)
        }
        _ => Err(mlua::Error::runtime(
            "include must be a string or an array of strings",
        )),
    }
}

fn parse_query_many_ids(lua: &Lua, ids_value: Value) -> Result<Vec<(String, ResolveId)>> {
    let ids_table = match ids_value {
        Value::Table(table) => table,
        Value::Nil => {
            return Err(mlua::Error::runtime("missing required field: ids"));
        }
        _ => {
            return Err(mlua::Error::runtime(
                "ids must be an array of integer or string values",
            ));
        }
    };

    let mut ids = Vec::new();
    let mut seen = HashSet::new();
    for value in ids_table.sequence_values::<Value>() {
        let raw = value?;
        let resolve_id = ResolveId::from_lua(raw.clone(), lua)?;
        let key = match raw {
            Value::Integer(id) => {
                if id <= 0 {
                    continue;
                }
                id.to_string()
            }
            Value::String(value) => value.to_str()?.to_string(),
            _ => continue,
        };

        if seen.insert(key.clone()) {
            ids.push((key, resolve_id));
        }
    }

    Ok(ids)
}

fn parse_library_id(request_table: &Table) -> Result<Option<DbId>> {
    match request_table.get::<Option<i64>>("library_id")? {
        Some(id) if id > 0 => Ok(Some(DbId(id))),
        Some(_) => Err(mlua::Error::runtime(
            "library_id must be a positive integer when provided",
        )),
        None => Ok(None),
    }
}

fn parse_query_request(
    lua: &Lua,
    request_table: &Table,
) -> Result<(ResolveId, Vec<EntityInclude>, Option<DbId>)> {
    let id_value: Value = request_table.get("id")?;
    if matches!(id_value, Value::Nil) {
        return Err(mlua::Error::runtime("missing required field: id"));
    }

    let id = ResolveId::from_lua(id_value, lua)?;
    let include_value: Value = request_table.get("include")?;
    let includes = normalize_includes(parse_include_values(include_value)?)?;
    let library_db_id = parse_library_id(request_table)?;

    Ok((id, includes, library_db_id))
}

fn parse_query_many_request(
    lua: &Lua,
    request_table: &Table,
) -> Result<(Vec<(String, ResolveId)>, Vec<EntityInclude>, Option<DbId>)> {
    let ids = parse_query_many_ids(lua, request_table.get("ids")?)?;
    let include_value: Value = request_table.get("include")?;
    let includes = normalize_includes(parse_include_values(include_value)?)?;
    let library_db_id = parse_library_id(request_table)?;

    Ok((ids, includes, library_db_id))
}

fn filter_includes_for_principal(
    includes: Vec<EntityInclude>,
    principal: &Principal,
) -> Vec<EntityInclude> {
    if db::roles::has_permission(&principal.permissions, Permission::ManageLibraries) {
        return includes;
    }
    includes
        .into_iter()
        .filter(|include| *include != EntityInclude::Entries)
        .collect()
}

fn ensure_projection_scope_accessible(
    db: &impl db::DbAccess,
    principal: &Principal,
    query_id: QueryId,
    library_id: Option<DbId>,
) -> Result<Option<QueryId>> {
    if let Some(library_id) = library_id {
        if db::libraries::accessible_by_id(db, principal, library_id)
            .into_lua_err()?
            .is_none()
        {
            return Ok(None);
        }
    }
    if let QueryId::Id(entity_db_id) = query_id {
        if !crate::routes::entity_accessible_to_principal(db, principal, entity_db_id)
            .into_lua_err()?
        {
            return Ok(None);
        }
    }
    Ok(Some(query_id))
}

async fn query_projection_info(
    lua: &Lua,
    request_table: &Table,
    principal: &Principal,
) -> Result<EntityProjectionInfo> {
    let (resolve_id, includes, library_id) = parse_query_request(lua, request_table)?;
    let includes = filter_includes_for_principal(includes, principal);
    let db = STATE.db.read().await;
    let query_id = resolve_id
        .to_query_id(&db)
        .into_lua_err()?
        .ok_or_else(|| mlua::Error::runtime("could not resolve id"))?;
    let query_id = ensure_projection_scope_accessible(&*db, principal, query_id, library_id)?
        .ok_or_else(|| mlua::Error::runtime("could not resolve id"))?;
    project_entity(&db, query_id, &includes, library_id).into_lua_err()
}

fn expect_release_projection(projection: EntityProjectionInfo) -> Result<ReleaseProjectionInfo> {
    match projection {
        EntityProjectionInfo::Release(release) => Ok(release),
        EntityProjectionInfo::Track(_) => Err(mlua::Error::runtime(
            "requested entity is a track, not a release",
        )),
        EntityProjectionInfo::Artist(_) => Err(mlua::Error::runtime(
            "requested entity is an artist, not a release",
        )),
    }
}

fn expect_track_projection(projection: EntityProjectionInfo) -> Result<TrackProjectionInfo> {
    match projection {
        EntityProjectionInfo::Track(track) => Ok(track),
        EntityProjectionInfo::Release(_) => Err(mlua::Error::runtime(
            "requested entity is a release, not a track",
        )),
        EntityProjectionInfo::Artist(_) => Err(mlua::Error::runtime(
            "requested entity is an artist, not a track",
        )),
    }
}

fn expect_artist_projection(projection: EntityProjectionInfo) -> Result<ArtistProjectionInfo> {
    match projection {
        EntityProjectionInfo::Artist(artist) => Ok(artist),
        EntityProjectionInfo::Release(_) => Err(mlua::Error::runtime(
            "requested entity is a release, not an artist",
        )),
        EntityProjectionInfo::Track(_) => Err(mlua::Error::runtime(
            "requested entity is a track, not an artist",
        )),
    }
}

struct EntitiesModule;

#[harmony_macros::module(
    plugin_scoped,
    name = "Entities",
    local = "entities",
    path = "lyra/entities",
    aliases(
        EntityProjectionInfo,
        EntityIncludeSelector,
        ReleaseProjectionKind,
        TrackProjectionKind,
        ArtistProjectionKind
    ),
    interfaces(
        EntityLookupHints,
        ProjectionEntryInfo,
        CreditProjectionInfo,
        CreditedArtistProjectionInfo,
        ReleaseProjectionTrack,
        ReleaseProjectionIncludes,
        TrackProjectionIncludes,
        ArtistProjectionIncludes,
        ReleaseProjectionInfo,
        TrackProjectionInfo,
        ArtistProjectionInfo,
        EntityQueryRequest,
        EntityQueryManyRequest
    ),
    classes(
        Release,
        ReleaseType,
        Artist,
        ArtistType,
        Track,
        CreditType,
        ArtistCreditSource
    )
)]
impl EntitiesModule {
    /// Queries a single typed entity projection with optional related includes.
    #[harmony(args(request: EntityQueryRequest), returns(EntityProjectionInfo))]
    pub(crate) async fn query(
        lua: Lua,
        #[harmony_context] caller: RequestCaller,
        request_table: Table,
    ) -> Result<Value> {
        let projection = query_projection_info(&lua, &request_table, &caller.principal).await?;
        projection.into_lua(&lua)
    }

    /// Queries a release projection with optional related includes.
    #[harmony(args(request: EntityQueryRequest), returns(ReleaseProjectionInfo))]
    pub(crate) async fn query_release(
        lua: Lua,
        #[harmony_context] caller: RequestCaller,
        request_table: Table,
    ) -> Result<Value> {
        let projection = expect_release_projection(
            query_projection_info(&lua, &request_table, &caller.principal).await?,
        )?;
        projection.into_lua(&lua)
    }

    /// Queries a track projection with optional related includes.
    #[harmony(args(request: EntityQueryRequest), returns(TrackProjectionInfo))]
    pub(crate) async fn query_track(
        lua: Lua,
        #[harmony_context] caller: RequestCaller,
        request_table: Table,
    ) -> Result<Value> {
        let projection = expect_track_projection(
            query_projection_info(&lua, &request_table, &caller.principal).await?,
        )?;
        projection.into_lua(&lua)
    }

    /// Queries an artist projection with optional related includes.
    #[harmony(args(request: EntityQueryRequest), returns(ArtistProjectionInfo))]
    pub(crate) async fn query_artist(
        lua: Lua,
        #[harmony_context] caller: RequestCaller,
        request_table: Table,
    ) -> Result<Value> {
        let projection = expect_artist_projection(
            query_projection_info(&lua, &request_table, &caller.principal).await?,
        )?;
        projection.into_lua(&lua)
    }

    /// Returns the element type string for a given id (e.g. "Library", "Release", "Artist", "Track").
    #[harmony(returns(Option<String>))]
    pub(crate) async fn get_type(
        #[harmony_context] caller: RequestCaller,
        id: ResolveId,
    ) -> Result<Option<String>> {
        let db = STATE.db.read().await;
        let db_id = id.to_db_id(&db).into_lua_err()?;
        match db_id {
            Some(id) => {
                if !crate::routes::entity_accessible_to_principal(&db, &caller.principal, id)
                    .into_lua_err()?
                {
                    return Ok(None);
                }
                crate::plugins::db::entities::get_element_type(&db, id).into_lua_err()
            }
            None => Ok(None),
        }
    }

    /// Queries many typed entity projections keyed by the requested id strings.
    #[harmony(args(request: EntityQueryManyRequest), returns(std::collections::BTreeMap<String, EntityProjectionInfo>))]
    pub(crate) async fn query_many(
        lua: Lua,
        #[harmony_context] caller: RequestCaller,
        request_table: Table,
    ) -> Result<Table> {
        let (ids, includes, library_id) = parse_query_many_request(&lua, &request_table)?;
        let includes = filter_includes_for_principal(includes, &caller.principal);
        let db = STATE.db.read().await;

        let mut keys = Vec::with_capacity(ids.len());
        let mut query_ids = Vec::with_capacity(ids.len());
        if let Some(library_id) = library_id
            && db::libraries::accessible_by_id(&*db, &caller.principal, library_id)
                .into_lua_err()?
                .is_none()
        {
            return lua.create_table();
        }
        for (key, resolve_id) in ids {
            let qid = resolve_id
                .to_query_id(&db)
                .into_lua_err()?
                .ok_or_else(|| mlua::Error::runtime("could not resolve id"))?;
            let Some(qid) =
                ensure_projection_scope_accessible(&*db, &caller.principal, qid, library_id)?
            else {
                continue;
            };
            keys.push(key);
            query_ids.push(qid);
        }

        let projections = project_entities(&db, query_ids, &includes, library_id).into_lua_err()?;

        let result = lua.create_table()?;
        for (key, projection) in keys.into_iter().zip(projections.into_iter()) {
            result.set(key, projection.into_lua(&lua)?)?;
        }

        Ok(result)
    }
}

pub(crate) fn get_module() -> harmony_core::Module {
    let mut m = EntitiesModule::module();
    m.scope = harmony_core::Scope {
        id: "lyra.entities".into(),
        description: "Schema-level helpers for library entity types.",
        danger: harmony_core::Danger::Negligible,
    };
    let inner = m.setup;
    m.setup = std::sync::Arc::new(move |lua: &Lua| {
        let table = inner(lua)?;
        table.set("CreditType", lua.create_proxy::<CreditType>()?)?;
        table.set(
            "ArtistCreditSource",
            lua.create_proxy::<ArtistCreditSource>()?,
        )?;
        Ok(table)
    });
    m
}

pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    EntitiesModule::render_luau_definition()
}
