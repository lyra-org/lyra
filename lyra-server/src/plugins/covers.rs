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

use agdb::{
    DbAny,
    DbId,
    QueryId,
};
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
    LuauType,
    LuauTypeInfo,
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    render_definition_file_with_support,
};

use crate::plugins::args;
use crate::plugins::db::{
    self,
    Cover,
    DbAsync,
    ResolveId,
};
use crate::services::catalog::Viewer;

enum CoverValidity {
    Valid(Cover),
    NotFound,
    Unavailable,
}

struct CoversModule;

#[cfg(feature = "docgen")]
struct CoverInfo;

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/covers")
        .capability("lyra.covers")
        .function(get_spec())
        .function(get_many_spec())
        .function(get_playlist_sources_spec())
        .function(get_playlist_sources_many_spec())
        .function(artwork_spec())
        .function(artwork_many_spec())
        .function(artwork_playlist_sources_spec())
        .install(|_| Ok(ModuleExport::new(CoversModule)))
}

fn get_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("id")
        .args::<ResolveId>()
        .returns::<Option<luau::Table>>()
        .call_async(Arc::new(|frame| cover_callback(frame, db::scoped)))
}

fn get_many_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get_many")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("ids")
        .args::<luau::Table>()
        .returns::<luau::Table>()
        .call_async(Arc::new(|frame| covers_callback(frame, db::scoped)))
}

fn get_playlist_sources_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get_playlist_sources")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("id")
        .args::<ResolveId>()
        .returns::<luau::Table>()
        .call_async(Arc::new(|frame| {
            playlist_sources_callback(frame, db::scoped)
        }))
}

fn get_playlist_sources_many_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get_playlist_sources_many")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("ids")
        .args::<luau::Table>()
        .returns::<luau::Table>()
        .call_async(Arc::new(get_playlist_sources_many_callback))
}

fn artwork_spec() -> FunctionSpec {
    FunctionSpec::async_fn("artwork")
        .arg_name("id")
        .args::<ResolveId>()
        .returns::<Option<luau::Table>>()
        .call_async(Arc::new(|frame| cover_callback(frame, db::system)))
}

fn artwork_many_spec() -> FunctionSpec {
    FunctionSpec::async_fn("artwork_many")
        .arg_name("ids")
        .args::<luau::Table>()
        .returns::<luau::Table>()
        .call_async(Arc::new(|frame| covers_callback(frame, db::system)))
}

fn artwork_playlist_sources_spec() -> FunctionSpec {
    FunctionSpec::async_fn("artwork_playlist_sources")
        .arg_name("id")
        .args::<ResolveId>()
        .returns::<luau::Table>()
        .call_async(Arc::new(|frame| {
            playlist_sources_callback(frame, db::system)
        }))
}

type Scope = fn(&luau::AsyncCallFrame<'_>) -> luau::runtime::Result<db::ViewerDb>;

fn cover_callback(
    mut frame: luau::AsyncCallFrame<'_>,
    scope: Scope,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let id = args::resolve_id(frame.args.read_named::<luau::Value>("id")?)?;
    let db = scope(&frame)?;

    Ok(luau::ScheduledFuture::new(async move {
        let (resolved_cover, stale_owners) = {
            let (db_read, viewer) = db.read().await?;
            let Some(QueryId::Id(item_id)) = id
                .to_query_id(&db_read)
                .map_err(crate::plugins::runtime_error)?
            else {
                return Ok(luau::Value::Nil);
            };
            if !viewer
                .sees(&db_read, item_id)
                .map_err(crate::plugins::runtime_error)?
            {
                return Ok(luau::Value::Nil);
            }
            let mut stale_owners = Vec::new();
            let result = resolve_persisted_cover(&db_read, item_id, &mut stale_owners)
                .map_err(crate::plugins::runtime_error)?;
            (result, stale_owners)
        };

        if !stale_owners.is_empty() {
            remove_still_stale_covers(db.handle(), stale_owners).await?;
        }

        let Some((owner_id, cover)) = resolved_cover else {
            return Ok(luau::Value::Nil);
        };
        Ok(luau::Value::TableData(cover_to_table(owner_id, cover)))
    }))
}

fn covers_callback(
    mut frame: luau::AsyncCallFrame<'_>,
    scope: Scope,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let ids: luau::Table = frame.args.read_named("ids")?;
    let item_ids = args::unique_ids(frame.vm, &ids)?;
    let db = scope(&frame)?;

    Ok(luau::ScheduledFuture::new(async move {
        let (resolved, stale_owners) = {
            let (db_read, viewer) = db.read().await?;
            let mut visible_ids = Vec::with_capacity(item_ids.len());
            for item_id in &item_ids {
                if viewer
                    .sees(&db_read, *item_id)
                    .map_err(crate::plugins::runtime_error)?
                {
                    visible_ids.push(*item_id);
                }
            }
            let mut stale_owners = Vec::new();
            let result = resolve_persisted_covers(&db_read, &visible_ids, &mut stale_owners)
                .map_err(crate::plugins::runtime_error)?;
            (result, stale_owners)
        };

        if !stale_owners.is_empty() {
            remove_still_stale_covers(db.handle(), stale_owners).await?;
        }

        let mut table = luau::OwnedTable::with_entry_capacity(0, 0, item_ids.len());
        for item_id in item_ids {
            let value = resolved
                .get(&item_id)
                .cloned()
                .map(|(owner_id, cover)| luau::Value::TableData(cover_to_table(owner_id, cover)))
                .unwrap_or(luau::Value::Nil);
            table.set_key(luau::Value::from(item_id.0), value);
        }

        Ok(luau::Value::TableData(table))
    }))
}

fn playlist_sources_callback(
    mut frame: luau::AsyncCallFrame<'_>,
    scope: Scope,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let id = args::resolve_id(frame.args.read_named::<luau::Value>("id")?)?;
    let db = scope(&frame)?;

    Ok(luau::ScheduledFuture::new(async move {
        let (sources, stale_owners) = {
            let (db_read, viewer) = db.read().await?;
            let Some(QueryId::Id(playlist_id)) = id
                .to_query_id(&db_read)
                .map_err(crate::plugins::runtime_error)?
            else {
                return Ok(luau::Value::TableData(luau::OwnedTable::with_capacity(
                    0, 0,
                )));
            };
            let mut stale_owners = Vec::new();
            let result =
                resolve_playlist_sources(&db_read, &viewer, playlist_id, &mut stale_owners)
                    .map_err(crate::plugins::runtime_error)?;
            (result, stale_owners)
        };

        if !stale_owners.is_empty() {
            remove_still_stale_covers(db.handle(), stale_owners).await?;
        }

        Ok(luau::Value::TableData(sources_to_table(sources)))
    }))
}

fn get_playlist_sources_many_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let ids: luau::Table = frame.args.read_named("ids")?;
    let playlist_ids = args::unique_ids(frame.vm, &ids)?;
    let db = db::scoped(&frame)?;

    Ok(luau::ScheduledFuture::new(async move {
        let (resolved, stale_owners) = {
            let (db_read, viewer) = db.read().await?;
            let mut stale_owners = Vec::new();
            let mut resolved = Vec::with_capacity(playlist_ids.len());
            for playlist_id in &playlist_ids {
                let sources =
                    resolve_playlist_sources(&db_read, &viewer, *playlist_id, &mut stale_owners)
                        .map_err(crate::plugins::runtime_error)?;
                resolved.push((*playlist_id, sources));
            }
            (resolved, stale_owners)
        };

        if !stale_owners.is_empty() {
            remove_still_stale_covers(db.handle(), stale_owners).await?;
        }

        let mut table = luau::OwnedTable::with_entry_capacity(0, 0, resolved.len());
        for (playlist_id, sources) in resolved {
            table.set_key(
                luau::Value::from(playlist_id.0),
                luau::Value::TableData(sources_to_table(sources)),
            );
        }

        Ok(luau::Value::TableData(table))
    }))
}

const PLAYLIST_SOURCE_LIMIT: usize = 4;

/// The first valid covers of a playlist's collage sources the viewer can see, in display order.
fn resolve_playlist_sources(
    db: &DbAny,
    viewer: &Viewer,
    playlist_id: DbId,
    stale_owners: &mut Vec<DbId>,
) -> anyhow::Result<Vec<(DbId, Cover)>> {
    let mut sources = Vec::new();
    if !viewer.sees_playlist(db, playlist_id)? {
        return Ok(sources);
    }
    for (_, release_id) in db::covers::display::playlist_cover_sources(db, playlist_id)? {
        if !viewer.sees_entity(db, release_id)? {
            continue;
        }
        if let Some(source) = resolve_persisted_cover(db, release_id, stale_owners)? {
            sources.push(source);
            if sources.len() == PLAYLIST_SOURCE_LIMIT {
                break;
            }
        }
    }
    Ok(sources)
}

fn sources_to_table(sources: Vec<(DbId, Cover)>) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(sources.len(), 0);
    for (owner_id, cover) in sources {
        table.push_array(luau::Value::TableData(cover_to_table(owner_id, cover)));
    }
    table
}

fn check_cover_validity(cover: Cover) -> CoverValidity {
    match std::fs::metadata(&cover.path) {
        Ok(meta) if meta.is_file() => CoverValidity::Valid(cover),
        Ok(_) => CoverValidity::NotFound,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => CoverValidity::NotFound,
        Err(_) => CoverValidity::Unavailable,
    }
}

fn resolve_persisted_cover(
    db: &DbAny,
    item_id: DbId,
    stale_owners: &mut Vec<DbId>,
) -> anyhow::Result<Option<(DbId, Cover)>> {
    let resolved = resolve_persisted_covers(db, &[item_id], stale_owners)?;
    Ok(resolved.into_values().next())
}

fn resolve_persisted_covers(
    db: &DbAny,
    item_ids: &[DbId],
    stale_owners: &mut Vec<DbId>,
) -> anyhow::Result<HashMap<DbId, (DbId, Cover)>> {
    let mut unique_ids = Vec::new();
    let mut seen = HashSet::new();
    for item_id in item_ids {
        if item_id.0 <= 0 {
            continue;
        }
        if seen.insert(*item_id) {
            if db::entities::get_element_type(db, *item_id)?.is_none() {
                continue;
            }
            unique_ids.push(*item_id);
        }
    }

    let direct_covers = db::covers::get_many(db, &unique_ids)?;

    let mut resolved = HashMap::new();
    let mut unresolved = Vec::new();
    for item_id in unique_ids {
        if let Some(cover) = direct_covers.get(&item_id) {
            match check_cover_validity(cover.clone()) {
                CoverValidity::Valid(valid) => {
                    resolved.insert(item_id, (item_id, valid));
                    continue;
                }
                CoverValidity::NotFound => stale_owners.push(item_id),
                CoverValidity::Unavailable => {}
            }
        }
        unresolved.push(item_id);
    }

    if unresolved.is_empty() {
        return Ok(resolved);
    }

    let track_releases = db::releases::get_by_tracks(db, &unresolved)?;
    let mut release_ids = Vec::new();
    let mut seen_release_ids = HashSet::new();
    for related_releases in track_releases.values() {
        for release in related_releases {
            let Some(release_id) = release.db_id.clone().map(Into::<DbId>::into) else {
                continue;
            };
            if seen_release_ids.insert(release_id) {
                release_ids.push(release_id);
            }
        }
    }

    let covers_by_release = db::covers::get_many(db, &release_ids)?;

    for item_id in unresolved {
        let Some(mut releases) = track_releases.get(&item_id).cloned() else {
            continue;
        };
        releases
            .sort_by_key(|release| release.db_id.clone().map(Into::<DbId>::into).map(|id| id.0));

        for release in releases {
            let Some(release_id) = release.db_id.clone().map(Into::<DbId>::into) else {
                continue;
            };
            let Some(cover) = covers_by_release.get(&release_id) else {
                continue;
            };
            match check_cover_validity(cover.clone()) {
                CoverValidity::Valid(valid) => {
                    resolved.insert(item_id, (release_id, valid));
                    break;
                }
                CoverValidity::NotFound => stale_owners.push(release_id),
                CoverValidity::Unavailable => {}
            }
        }
    }

    Ok(resolved)
}

async fn remove_still_stale_covers(
    db: DbAsync,
    stale_owners: Vec<DbId>,
) -> luau::runtime::Result<()> {
    let mut seen = HashSet::new();
    let mut db_write = db.write().await;
    for owner_id in stale_owners {
        if !seen.insert(owner_id) {
            continue;
        }
        let Ok(Some(cover)) = db::covers::get(&*db_write, owner_id) else {
            continue;
        };
        if matches!(check_cover_validity(cover), CoverValidity::NotFound) {
            let _ = db::covers::remove(&mut *db_write, owner_id);
        }
    }
    Ok(())
}

fn cover_to_table(owner_db_id: DbId, cover: Cover) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(0, 5);
    table.set_field("path", luau::Value::String(cover.path.into_bytes()));
    table.set_field(
        "mime_type",
        luau::Value::String(cover.mime_type.into_bytes()),
    );
    table.set_field("hash", luau::Value::String(cover.hash.into_bytes()));
    table.set_field(
        "blurhash",
        cover
            .blurhash
            .map(|value| luau::Value::String(value.into_bytes()))
            .unwrap_or(luau::Value::Nil),
    );
    table.set_field("release_id", luau::Value::from(owner_db_id.0));
    table
}

#[cfg(feature = "docgen")]
impl LuauTypeInfo for CoverInfo {
    fn luau_type() -> LuauType {
        LuauType::named("CoverInfo")
    }
}

#[cfg(feature = "docgen")]
impl DescribeInterface for CoverInfo {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("CoverInfo", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "path",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "mime_type",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "hash",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "blurhash",
                ty: Option::<String>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "release_id",
                ty: i64::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
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
        name: "Covers",
        local_name: "covers",
        description: None,
        fields: Vec::new(),
        functions: vec![
            ModuleFunctionDescriptor {
                path: vec!["get"],
                description: Some(
                    "Returns an item's own cover, or for a track without one, its release's cover. Scoped to the caller's libraries and visible playlists; a client dispatch without a resolved credential is refused.",
                ),
                params: vec![param("id", args::resolve_id_type())],
                returns: vec![Option::<CoverInfo>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_many"],
                description: Some(
                    "`get` for several ids, keyed by db id. Scoped to the caller's libraries and visible playlists; a client dispatch without a resolved credential is refused.",
                ),
                params: vec![param("ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(
                    u64::luau_type(),
                    Option::<CoverInfo>::luau_type(),
                )],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_playlist_sources"],
                description: Some(
                    "Returns up to four release covers that make up a playlist's collage, in display order. Unknown or non-playlist ids yield an empty array. Scoped to the caller's libraries and visible playlists; a client dispatch without a resolved credential is refused.",
                ),
                params: vec![param("id", args::resolve_id_type())],
                returns: vec![Vec::<CoverInfo>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_playlist_sources_many"],
                description: Some(
                    "Returns the collage covers of several playlists keyed by playlist db id. Scoped to the caller's libraries and visible playlists; a client dispatch without a resolved credential is refused.",
                ),
                params: vec![param("ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(
                    u64::luau_type(),
                    Vec::<CoverInfo>::luau_type(),
                )],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["artwork"],
                description: Some(
                    "`get` read as the system. Ignores library and playlist visibility, including private playlists, so it is only for public artwork routes, which must serve nothing from it but the image bytes.",
                ),
                params: vec![param("id", args::resolve_id_type())],
                returns: vec![Option::<CoverInfo>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["artwork_many"],
                description: Some(
                    "`get_many` read as the system. Ignores library and playlist visibility, including private playlists, so it is only for public artwork routes, which must serve nothing from it but the image bytes.",
                ),
                params: vec![param("ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(
                    u64::luau_type(),
                    Option::<CoverInfo>::luau_type(),
                )],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["artwork_playlist_sources"],
                description: Some(
                    "`get_playlist_sources` read as the system. Ignores library and playlist visibility, including private playlists, so it is only for public artwork routes, which must serve nothing from it but the image bytes.",
                ),
                params: vec![param("id", args::resolve_id_type())],
                returns: vec![Vec::<CoverInfo>::luau_type()],
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
        &[CoverInfo::interface_descriptor()],
        &[],
    )
}
