// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbAny,
    DbId,
    QueryBuilder,
    QueryId,
};
use anyhow::bail;
use nanoid::nanoid;

use crate::db::{
    self,
    Playlist,
};

#[derive(Clone, Debug)]
pub(crate) struct PlaylistTrackLink {
    pub(crate) entry_db_id: DbId,
    pub(crate) entry_id: String,
    pub(crate) track_db_id: DbId,
    pub(crate) position: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct CreatePlaylistRequest {
    pub(crate) user_db_id: DbId,
    pub(crate) name: String,
    pub(crate) description: Option<String>,
    pub(crate) is_public: Option<bool>,
    pub(crate) created_at: Option<u64>,
    pub(crate) updated_at: Option<u64>,
}

#[derive(Clone, Debug)]
pub(crate) struct UpdatePlaylistRequest {
    pub(crate) playlist_id: QueryId,
    pub(crate) name: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) is_public: Option<bool>,
    pub(crate) updated_at: Option<u64>,
}

fn resolve_optional_id(db: &DbAny, query_id: QueryId) -> anyhow::Result<Option<DbId>> {
    match query_id {
        QueryId::Id(db_id) => Ok(Some(db_id)),
        QueryId::Alias(alias) => {
            if let Ok(parsed) = alias.trim().parse::<i64>()
                && parsed > 0
            {
                return Ok(Some(DbId(parsed)));
            }

            let result = db.exec(QueryBuilder::select().ids(alias.as_str()).query())?;
            let ids = result.ids();
            match ids.as_slice() {
                [node_id] => Ok(Some(*node_id)),
                [] => Ok(None),
                _ => bail!("entity alias resolves to multiple ids: {alias}"),
            }
        }
    }
}

pub(crate) fn resolve_id(db: &DbAny, query_id: QueryId) -> anyhow::Result<DbId> {
    let alias = match &query_id {
        QueryId::Alias(alias) => Some(alias.clone()),
        QueryId::Id(_) => None,
    };
    resolve_optional_id(db, query_id)?.ok_or_else(|| match alias {
        Some(alias) => anyhow::anyhow!("entity alias not found: {alias}"),
        None => anyhow::anyhow!("entity not found"),
    })
}

fn validate_name(raw_name: &str) -> anyhow::Result<String> {
    let trimmed = raw_name.trim().to_string();
    if trimmed.is_empty() {
        bail!("playlist name cannot be empty");
    }
    Ok(trimmed)
}

pub(crate) fn list(db: &DbAny) -> anyhow::Result<Vec<Playlist>> {
    db::playlists::get(db)
}

pub(crate) fn get(db: &DbAny, query_id: QueryId) -> anyhow::Result<Option<Playlist>> {
    let Some(playlist_db_id) = resolve_optional_id(db, query_id)? else {
        return Ok(None);
    };
    db::playlists::get_by_id(db, playlist_db_id)
}

pub(crate) fn get_by_user(db: &DbAny, user_db_id: DbId) -> anyhow::Result<Vec<Playlist>> {
    db::playlists::get_by_user(db, user_db_id)
}

pub(crate) fn get_owner(db: &DbAny, playlist_id: QueryId) -> anyhow::Result<Option<DbId>> {
    let Some(playlist_db_id) = resolve_optional_id(db, playlist_id)? else {
        return Ok(None);
    };
    db::playlists::get_owner(db, playlist_db_id)
}

pub(crate) fn get_tracks(
    db: &DbAny,
    playlist_id: QueryId,
) -> anyhow::Result<Vec<PlaylistTrackLink>> {
    let playlist_db_id = resolve_id(db, playlist_id)?;

    let playlist_tracks = db::playlists::get_tracks(db, playlist_db_id)?;
    let edge_ids: Vec<DbId> = playlist_tracks.iter().map(|track| track.edge_id).collect();
    let track_ids = db::playlists::resolve_edge_targets(db, &edge_ids)?;

    let mut links = Vec::with_capacity(playlist_tracks.len());
    for (playlist_track, track_db_id) in playlist_tracks.into_iter().zip(track_ids) {
        links.push(PlaylistTrackLink {
            entry_db_id: playlist_track.edge_id,
            entry_id: playlist_track.entry_id,
            track_db_id,
            position: playlist_track.position,
        });
    }

    Ok(links)
}

pub(crate) fn get_tracks_many(
    db: &DbAny,
    playlist_db_ids: &[DbId],
) -> anyhow::Result<std::collections::HashMap<DbId, Vec<PlaylistTrackLink>>> {
    let raw = db::playlists::get_tracks_many(db, playlist_db_ids)?;
    let mut result = std::collections::HashMap::new();
    for (playlist_id, playlist_tracks) in raw {
        let edge_ids: Vec<DbId> = playlist_tracks.iter().map(|t| t.edge_id).collect();
        let track_ids = db::playlists::resolve_edge_targets(db, &edge_ids)?;
        let mut links = Vec::with_capacity(playlist_tracks.len());
        for (pt, track_db_id) in playlist_tracks.into_iter().zip(track_ids) {
            links.push(PlaylistTrackLink {
                entry_db_id: pt.edge_id,
                entry_id: pt.entry_id,
                track_db_id,
                position: pt.position,
            });
        }
        result.insert(playlist_id, links);
    }
    Ok(result)
}

pub(crate) fn create(db: &mut DbAny, request: &CreatePlaylistRequest) -> anyhow::Result<DbId> {
    let playlist = Playlist {
        db_id: None,
        id: nanoid!(),
        name: validate_name(&request.name)?,
        description: request.description.clone(),
        is_public: request.is_public,
        created_at: request.created_at,
        updated_at: request.updated_at,
    };

    db::playlists::create(db, &playlist, request.user_db_id)
}

pub(crate) fn delete(db: &mut DbAny, playlist_id: QueryId) -> anyhow::Result<Option<Playlist>> {
    let Some(playlist_db_id) = resolve_optional_id(db, playlist_id)? else {
        return Ok(None);
    };
    let Some(playlist) = db::playlists::get_by_id(db, playlist_db_id)? else {
        return Ok(None);
    };

    db::playlists::delete(db, playlist_db_id)?;
    Ok(Some(playlist))
}

pub(crate) fn update(
    db: &mut DbAny,
    request: &UpdatePlaylistRequest,
) -> anyhow::Result<Option<Playlist>> {
    let Some(playlist_db_id) = resolve_optional_id(db, request.playlist_id.clone())? else {
        return Ok(None);
    };
    let Some(mut playlist) = db::playlists::get_by_id(db, playlist_db_id)? else {
        return Ok(None);
    };

    if let Some(name) = &request.name {
        playlist.name = validate_name(name)?;
    }
    if let Some(description) = &request.description {
        playlist.description = Some(description.clone());
    }
    if let Some(is_public) = request.is_public {
        playlist.is_public = Some(is_public);
    }
    if let Some(updated_at) = request.updated_at {
        playlist.updated_at = Some(updated_at);
    }

    db::playlists::update(db, &playlist)?;
    Ok(Some(playlist))
}

pub(crate) fn add_track(
    db: &mut DbAny,
    playlist_id: QueryId,
    track_db_id: QueryId,
) -> anyhow::Result<db::playlists::PlaylistTrack> {
    let playlist_db_id = resolve_id(db, playlist_id)?;
    let track_db_id = resolve_id(db, track_db_id)?;
    db.transaction_mut(|t| db::playlists::add_track(t, playlist_db_id, track_db_id))
}

pub(crate) fn add_tracks(
    db: &mut DbAny,
    playlist_id: QueryId,
    track_ids: &[QueryId],
) -> anyhow::Result<Vec<PlaylistTrackLink>> {
    let playlist_db_id = resolve_id(db, playlist_id)?;
    let mut resolved_track_ids = Vec::with_capacity(track_ids.len());
    for track_id in track_ids {
        let track_db_id = resolve_id(db, track_id.clone())?;
        if db::tracks::get_by_id(db, track_db_id)?.is_none() {
            bail!("track not found: {}", track_db_id.0);
        }
        resolved_track_ids.push(track_db_id);
    }

    let results =
        db.transaction_mut(|t| db::playlists::add_tracks(t, playlist_db_id, &resolved_track_ids))?;
    Ok(results
        .into_iter()
        .zip(resolved_track_ids)
        .map(|(result, track_db_id)| PlaylistTrackLink {
            entry_db_id: result.edge_id,
            entry_id: result.entry_id,
            track_db_id,
            position: result.position,
        })
        .collect())
}

pub(crate) fn remove_track(db: &mut DbAny, entry_db_id: QueryId) -> anyhow::Result<()> {
    let entry_db_id = resolve_id(db, entry_db_id)?;
    db::playlists::remove_track(db, entry_db_id)
}

pub(crate) fn remove_tracks(
    db: &mut DbAny,
    playlist_id: QueryId,
    entry_ids: &[QueryId],
) -> anyhow::Result<Vec<PlaylistTrackLink>> {
    let playlist_db_id = resolve_id(db, playlist_id)?;
    let existing_tracks = get_tracks(db, QueryId::Id(playlist_db_id))?;
    let mut removed = Vec::with_capacity(entry_ids.len());

    for entry_id in entry_ids {
        let entry_db_id = resolve_id(db, entry_id.clone())?;
        let track = existing_tracks
            .iter()
            .find(|track| track.entry_db_id == entry_db_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("playlist entry not found: {}", entry_db_id.0))?;
        removed.push(track);
    }

    for track in &removed {
        db::playlists::remove_track(db, track.entry_db_id)?;
    }

    Ok(removed)
}

pub(crate) fn move_track(
    db: &mut DbAny,
    playlist_id: QueryId,
    entry_id: QueryId,
    new_position: u64,
) -> anyhow::Result<()> {
    let playlist_db_id = resolve_id(db, playlist_id)?;
    let entry_db_id = resolve_id(db, entry_id)?;
    db.transaction_mut(|t| db::playlists::move_track(t, playlist_db_id, entry_db_id, new_position))
}
