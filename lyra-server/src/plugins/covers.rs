// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
};

use agdb::{
    DbAny,
    DbId,
    QueryId,
};
use harmony_luau as luau;
#[cfg(feature = "docgen")]
use harmony_luau::{
    DescribeInterface,
    FieldDescriptor,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
};

use crate::plugins::{
    args::{
        ResolveIdArg,
        UniqueIds,
    },
    db::{
        SystemDb,
        ViewerDb,
    },
};
use crate::services::catalog::Viewer;
use lyra_db::Cover;
use lyra_db::DbAsync;

enum CoverValidity {
    Valid(Cover),
    NotFound,
    Unavailable,
}

pub(crate) struct CoversModule;

#[cfg(feature = "docgen")]
struct CoverInfo;

#[harmony_macros::module(id = "lyra/covers", docs_feature = "docgen")]
impl CoversModule {
    #[harmony(
        description = "Returns an item's own cover, or for a track without one, its release's cover. Scoped to the caller's libraries and visible playlists; a client dispatch without a resolved credential is refused.",
        returns(Option<CoverInfo>)
    )]
    async fn get(id: ResolveIdArg, #[context] db: ViewerDb) -> luau::runtime::Result<luau::Value> {
        cover(&db, id).await
    }

    #[harmony(
        description = "`get` for several ids, keyed by db id. Scoped to the caller's libraries and visible playlists; a client dispatch without a resolved credential is refused.",
        returns(HashMap<u64, Option<CoverInfo>>)
    )]
    async fn get_many(
        ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        covers(&db, ids).await
    }

    #[harmony(
        description = "Returns up to four release covers that make up a playlist's collage, in display order. Unknown or non-playlist ids yield an empty array. Scoped to the caller's libraries and visible playlists; a client dispatch without a resolved credential is refused.",
        returns(Vec<CoverInfo>)
    )]
    async fn get_playlist_sources(
        id: ResolveIdArg,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        playlist_sources(&db, id).await
    }

    #[harmony(
        description = "Returns the collage covers of several playlists keyed by playlist db id. Scoped to the caller's libraries and visible playlists; a client dispatch without a resolved credential is refused.",
        returns(HashMap<u64, Vec<CoverInfo>>)
    )]
    async fn get_playlist_sources_many(
        ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let playlist_ids = ids.0;
        let (resolved, stale_owners) = {
            let (db_read, viewer) = db.read().await?;
            let mut stale_owners = Vec::new();
            let mut resolved = Vec::with_capacity(playlist_ids.len());
            for playlist_id in &playlist_ids {
                let sources =
                    resolve_playlist_sources(&db_read, &viewer, *playlist_id, &mut stale_owners)?;
                resolved.push((*playlist_id, sources));
            }
            (resolved, stale_owners)
        };

        if !stale_owners.is_empty() {
            remove_still_stale_covers(db.handle(), stale_owners).await?;
        }

        let table = luau::OwnedTable::from_entries(
            resolved
                .into_iter()
                .map(|(playlist_id, sources)| (playlist_id.0, sources_to_table(sources))),
        );

        Ok(table)
    }

    #[harmony(
        description = "`get` read as the system. Ignores library and playlist visibility, including private playlists, so it is only for public artwork routes, which must serve nothing from it but the image bytes.",
        returns(Option<CoverInfo>)
    )]
    async fn artwork(
        id: ResolveIdArg,
        #[context] db: SystemDb,
    ) -> luau::runtime::Result<luau::Value> {
        cover(&db, id).await
    }

    #[harmony(
        description = "`get_many` read as the system. Ignores library and playlist visibility, including private playlists, so it is only for public artwork routes, which must serve nothing from it but the image bytes.",
        returns(HashMap<u64, Option<CoverInfo>>)
    )]
    async fn artwork_many(
        ids: UniqueIds,
        #[context] db: SystemDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        covers(&db, ids).await
    }

    #[harmony(
        description = "`get_playlist_sources` read as the system. Ignores library and playlist visibility, including private playlists, so it is only for public artwork routes, which must serve nothing from it but the image bytes.",
        returns(Vec<CoverInfo>)
    )]
    async fn artwork_playlist_sources(
        id: ResolveIdArg,
        #[context] db: SystemDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        playlist_sources(&db, id).await
    }
}

async fn cover(db: &ViewerDb, id: ResolveIdArg) -> luau::runtime::Result<luau::Value> {
    let id = id.0;
    let (resolved_cover, stale_owners) = {
        let (db_read, viewer) = db.read().await?;
        let Some(QueryId::Id(item_id)) = id.to_query_id(&db_read)? else {
            return Ok(luau::Value::Nil);
        };
        if !viewer.sees(&db_read, item_id)? {
            return Ok(luau::Value::Nil);
        }
        let mut stale_owners = Vec::new();
        let result = resolve_persisted_cover(&db_read, item_id, &mut stale_owners)?;
        (result, stale_owners)
    };

    if !stale_owners.is_empty() {
        remove_still_stale_covers(db.handle(), stale_owners).await?;
    }

    let Some((owner_id, cover)) = resolved_cover else {
        return Ok(luau::Value::Nil);
    };
    Ok(cover_to_table(owner_id, cover).into())
}

async fn covers(db: &ViewerDb, ids: UniqueIds) -> luau::runtime::Result<luau::OwnedTable> {
    let item_ids = ids.0;
    let (resolved, stale_owners) = {
        let (db_read, viewer) = db.read().await?;
        let mut visible_ids = Vec::with_capacity(item_ids.len());
        for item_id in &item_ids {
            if viewer.sees(&db_read, *item_id)? {
                visible_ids.push(*item_id);
            }
        }
        let mut stale_owners = Vec::new();
        let result = resolve_persisted_covers(&db_read, &visible_ids, &mut stale_owners)?;
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
        table.set_key(item_id.0, value);
    }

    Ok(table)
}

async fn playlist_sources(
    db: &ViewerDb,
    id: ResolveIdArg,
) -> luau::runtime::Result<luau::OwnedTable> {
    let id = id.0;
    let (sources, stale_owners) = {
        let (db_read, viewer) = db.read().await?;
        let Some(QueryId::Id(playlist_id)) = id.to_query_id(&db_read)? else {
            return Ok(luau::OwnedTable::with_capacity(0, 0));
        };
        let mut stale_owners = Vec::new();
        let result = resolve_playlist_sources(&db_read, &viewer, playlist_id, &mut stale_owners)?;
        (result, stale_owners)
    };

    if !stale_owners.is_empty() {
        remove_still_stale_covers(db.handle(), stale_owners).await?;
    }

    Ok(sources_to_table(sources))
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
    for (_, release_id) in lyra_db::covers::display::playlist_cover_sources(db, playlist_id)? {
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
    luau::OwnedTable::from_array(
        sources
            .into_iter()
            .map(|(owner_id, cover)| cover_to_table(owner_id, cover)),
    )
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
            if lyra_db::entities::get_element_type(db, *item_id)?.is_none() {
                continue;
            }
            unique_ids.push(*item_id);
        }
    }

    let direct_covers = lyra_db::covers::get_many(db, &unique_ids)?;

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

    let track_releases = lyra_db::releases::get_by_tracks(db, &unresolved)?;
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

    let covers_by_release = lyra_db::covers::get_many(db, &release_ids)?;

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
        let Ok(Some(cover)) = lyra_db::covers::get(&*db_write, owner_id) else {
            continue;
        };
        if matches!(check_cover_validity(cover), CoverValidity::NotFound) {
            let _ = lyra_db::covers::remove(&mut *db_write, owner_id);
        }
    }
    Ok(())
}

fn cover_to_table(owner_db_id: DbId, cover: Cover) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(0, 5);
    table.set_field("path", cover.path);
    table.set_field("mime_type", cover.mime_type);
    table.set_field("hash", cover.hash);
    table.set_field(
        "blurhash",
        cover
            .blurhash
            .map(|value| luau::Value::String(value.into_bytes()))
            .unwrap_or(luau::Value::Nil),
    );
    table.set_field("release_id", owner_db_id.0);
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
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    use harmony_luau::DescribeModule;

    harmony_luau::render_definition_file_with_support(
        &CoversModule::module_descriptor(),
        &[],
        &[CoverInfo::interface_descriptor()],
        &[],
    )
}
