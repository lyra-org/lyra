// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbId,
    QueryId,
};
use aide::axum::{
    ApiRouter,
    routing::{
        delete_with,
        get_with,
        patch_with,
        post_with,
    },
};
use aide::transform::TransformOperation;
use axum::{
    Json,
    extract::Path,
    extract::Query,
    http::{
        HeaderMap,
        StatusCode,
    },
};
use schemars::JsonSchema;
use serde::{
    Deserialize,
    Serialize,
};
use std::time::{
    SystemTime,
    UNIX_EPOCH,
};

use crate::{
    STATE,
    db::{
        self,
        Playlist,
    },
    routes::AppError,
    routes::deserialize_inc,
    routes::responses::{
        ArtistResponse,
        ReleaseResponse,
        TrackResponse,
    },
    services::{
        auth::require_principal,
        entities::resolve_track_artists,
        playlists,
    },
};

#[derive(Deserialize, JsonSchema)]
struct CreatePlaylistRequest {
    name: String,
    description: Option<String>,
    is_public: Option<bool>,
}

#[derive(Deserialize, JsonSchema)]
struct UpdatePlaylistRequest {
    name: Option<String>,
    description: Option<Option<String>>,
    is_public: Option<bool>,
}

#[derive(Deserialize, JsonSchema)]
struct AddPlaylistTracksRequest {
    track_ids: Vec<String>,
}

#[derive(Deserialize, JsonSchema)]
struct RemovePlaylistTracksRequest {
    entry_ids: Vec<String>,
}

#[derive(Deserialize, JsonSchema)]
struct MovePlaylistTrackRequest {
    new_position: u64,
}

#[derive(Serialize, JsonSchema)]
struct PlaylistResponse {
    id: String,
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    is_public: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    tracks: Option<Vec<PlaylistTrackResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    created_at: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    updated_at: Option<u64>,
}

#[derive(Serialize, JsonSchema)]
struct PlaylistTrackResponse {
    entry_id: String,
    track: TrackResponse,
    #[serde(skip_serializing_if = "Option::is_none")]
    artists: Option<Vec<ArtistResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    release: Option<ReleaseResponse>,
    position: u64,
}

#[derive(Deserialize, JsonSchema)]
struct PlaylistQuery {
    #[schemars(description = "Comma-separated or repeated values: tracks, artists, releases.")]
    #[serde(default, deserialize_with = "deserialize_inc")]
    inc: Option<Vec<String>>,
}

#[derive(Clone, Copy)]
struct PlaylistInc {
    tracks: bool,
    artists: bool,
    releases: bool,
}

fn parse_inc(inc: Option<Vec<String>>) -> Result<PlaylistInc, AppError> {
    let values = super::parse_inc_values(inc, &["tracks", "artists", "releases"])?;
    let mut result = PlaylistInc {
        tracks: false,
        artists: false,
        releases: false,
    };
    for value in values {
        match value.as_str() {
            "tracks" => result.tracks = true,
            "artists" => result.artists = true,
            "releases" => result.releases = true,
            _ => {}
        }
    }

    // artists or releases imply tracks
    if result.artists || result.releases {
        result.tracks = true;
    }

    Ok(result)
}

fn now_epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn resolve_owner_id(db: &agdb::DbAny, owner_db_id: Option<DbId>) -> anyhow::Result<Option<String>> {
    match owner_db_id {
        Some(db_id) => db::lookup::find_id_by_db_id(db, db_id),
        None => Ok(None),
    }
}

fn playlist_to_response(
    db: &agdb::DbAny,
    playlist: Playlist,
    tracks: Option<Vec<PlaylistTrackResponse>>,
    owner_db_id: Option<DbId>,
) -> anyhow::Result<PlaylistResponse> {
    let owner_id = resolve_owner_id(db, owner_db_id)?;
    Ok(PlaylistResponse {
        id: playlist.id,
        name: playlist.name,
        description: playlist.description,
        is_public: playlist.is_public.unwrap_or(false),
        tracks,
        owner_id,
        created_at: playlist.created_at,
        updated_at: playlist.updated_at,
    })
}

async fn require_playlist_owner(
    headers: &HeaderMap,
    playlist_db_id: DbId,
) -> Result<crate::services::auth::Principal, AppError> {
    let principal = require_principal(headers).await?;
    let db = STATE.db.read().await;
    let owner_db_id = db::playlists::get_owner(&db, playlist_db_id)?;
    if owner_db_id != Some(principal.user_db_id) {
        return Err(AppError::forbidden("you do not own this playlist"));
    }
    Ok(principal)
}

fn build_track_response(
    db: &agdb::DbAny,
    track: db::Track,
    track_db_id: DbId,
    entry_id: String,
    position: u64,
    inc: PlaylistInc,
) -> anyhow::Result<PlaylistTrackResponse> {
    let artists: Option<Vec<ArtistResponse>> = if inc.artists {
        Some(
            resolve_track_artists(db, track_db_id)?
                .into_iter()
                .map(Into::into)
                .collect(),
        )
    } else {
        None
    };
    let release: Option<ReleaseResponse> = if inc.releases {
        db::releases::get_by_track(db, track_db_id)?
            .into_iter()
            .next()
            .map(ReleaseResponse::from)
    } else {
        None
    };
    Ok(PlaylistTrackResponse {
        entry_id,
        track: track.into(),
        artists,
        release,
        position,
    })
}

fn build_tracks(
    db: &agdb::DbAny,
    playlist_db_id: DbId,
    inc: PlaylistInc,
) -> anyhow::Result<Vec<PlaylistTrackResponse>> {
    let playlist_tracks = playlists::get_tracks(db, QueryId::Id(playlist_db_id))?;
    let mut items = Vec::with_capacity(playlist_tracks.len());
    for playlist_track in playlist_tracks {
        let Some(track) = db::tracks::get_by_id(db, playlist_track.track_db_id)? else {
            continue;
        };
        items.push(build_track_response(
            db,
            track,
            playlist_track.track_db_id,
            playlist_track.entry_id,
            playlist_track.position,
            inc,
        )?);
    }
    Ok(items)
}

async fn create_playlist(
    headers: HeaderMap,
    Json(request): Json<CreatePlaylistRequest>,
) -> Result<(StatusCode, Json<PlaylistResponse>), AppError> {
    let principal = require_principal(&headers).await?;

    let name = request.name.trim().to_string();
    if name.is_empty() {
        return Err(AppError::bad_request("playlist name cannot be empty"));
    }

    let now = now_epoch();
    let mut db = STATE.db.write().await;
    let playlist_db_id = playlists::create(
        &mut db,
        &playlists::CreatePlaylistRequest {
            user_db_id: principal.user_db_id,
            name,
            description: request.description,
            is_public: request.is_public,
            created_at: Some(now),
            updated_at: Some(now),
        },
    )
    .map_err(|err| AppError::bad_request(err.to_string()))?;

    let created = db::playlists::get_by_id(&db, playlist_db_id)?
        .ok_or_else(|| AppError::not_found("playlist not found after creation"))?;

    Ok((
        StatusCode::CREATED,
        Json(playlist_to_response(
            &db,
            created,
            None,
            Some(principal.user_db_id),
        )?),
    ))
}

async fn get_playlists(
    headers: HeaderMap,
    Query(query): Query<PlaylistQuery>,
) -> Result<Json<Vec<PlaylistResponse>>, AppError> {
    let principal = require_principal(&headers).await?;
    let inc = parse_inc(query.inc)?;

    let db = &*STATE.db.read().await;
    let playlists = playlists::get_by_user(db, principal.user_db_id)?;

    let mut response = Vec::with_capacity(playlists.len());
    for playlist in playlists {
        let playlist_db_id: DbId = playlist
            .db_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("playlist missing db_id"))?
            .into();
        let tracks = if inc.tracks {
            Some(build_tracks(db, playlist_db_id, inc)?)
        } else {
            None
        };
        let owner_db_id = playlists::get_owner(db, QueryId::Id(playlist_db_id))?;
        response.push(playlist_to_response(db, playlist, tracks, owner_db_id)?);
    }

    Ok(Json(response))
}

async fn get_playlist(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<PlaylistQuery>,
) -> Result<Json<PlaylistResponse>, AppError> {
    let principal = require_principal(&headers).await?;
    let inc = parse_inc(query.inc)?;

    let db = &*STATE.db.read().await;
    let playlist_db_id = db::lookup::find_node_id_by_id(db, &id)?
        .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
    let playlist = db::playlists::get_by_id(db, playlist_db_id)?
        .ok_or_else(|| AppError::not_found(format!("Playlist not found: {}", id)))?;

    // Check access: owner or public
    let owner_db_id = playlists::get_owner(db, QueryId::Id(playlist_db_id))?;
    let is_owner = owner_db_id == Some(principal.user_db_id);
    let is_public = playlist.is_public.unwrap_or(false);
    if !is_owner && !is_public {
        return Err(AppError::not_found(format!("Playlist not found: {}", id)));
    }

    let tracks = if inc.tracks {
        Some(build_tracks(db, playlist_db_id, inc)?)
    } else {
        None
    };

    Ok(Json(playlist_to_response(
        db,
        playlist,
        tracks,
        owner_db_id,
    )?))
}

async fn update_playlist(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<UpdatePlaylistRequest>,
) -> Result<Json<PlaylistResponse>, AppError> {
    let playlist_db_id = {
        let db = STATE.db.read().await;
        db::lookup::find_node_id_by_id(&*db, &id)?
            .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?
    };
    let _principal = require_playlist_owner(&headers, playlist_db_id).await?;

    let mut db = STATE.db.write().await;
    let playlist = playlists::update(
        &mut db,
        &playlists::UpdatePlaylistRequest {
            playlist_id: QueryId::Id(playlist_db_id),
            name: request.name,
            description: request.description.flatten(),
            is_public: request.is_public,
            updated_at: Some(now_epoch()),
        },
    )
    .map_err(|err| AppError::bad_request(err.to_string()))?
    .ok_or_else(|| AppError::not_found(format!("Playlist not found: {}", id)))?;

    let owner_db_id = playlists::get_owner(&db, QueryId::Id(playlist_db_id))?;
    Ok(Json(playlist_to_response(
        &db,
        playlist,
        None,
        owner_db_id,
    )?))
}

async fn delete_playlist(
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<StatusCode, AppError> {
    let playlist_db_id = {
        let db = STATE.db.read().await;
        db::lookup::find_node_id_by_id(&*db, &id)?
            .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?
    };
    let _principal = require_playlist_owner(&headers, playlist_db_id).await?;

    let mut db = STATE.db.write().await;
    playlists::delete(&mut db, QueryId::Id(playlist_db_id))?
        .ok_or_else(|| AppError::not_found(format!("Playlist not found: {}", id)))?;

    Ok(StatusCode::NO_CONTENT)
}

async fn add_playlist_tracks(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<AddPlaylistTracksRequest>,
) -> Result<Json<Vec<PlaylistTrackResponse>>, AppError> {
    let playlist_db_id = {
        let db = STATE.db.read().await;
        db::lookup::find_node_id_by_id(&*db, &id)?
            .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?
    };
    let _principal = require_playlist_owner(&headers, playlist_db_id).await?;

    if request.track_ids.is_empty() {
        return Err(AppError::bad_request("track_ids cannot be empty"));
    }

    let mut db = STATE.db.write().await;
    let results = playlists::add_tracks(
        &mut db,
        QueryId::Id(playlist_db_id),
        &request
            .track_ids
            .into_iter()
            .map(QueryId::Alias)
            .collect::<Vec<_>>(),
    )
    .map_err(|err| {
        let message = err.to_string();
        if message.starts_with("track not found") {
            AppError::not_found(message)
        } else {
            AppError::from(err)
        }
    })?;

    let no_inc = PlaylistInc {
        tracks: true,
        artists: false,
        releases: false,
    };
    let mut added = Vec::with_capacity(results.len());
    for playlist_track in results {
        let track = db::tracks::get_by_id(&db, playlist_track.track_db_id)?
            .ok_or_else(|| AppError::not_found("playlist track target missing"))?;
        added.push(build_track_response(
            &db,
            track,
            playlist_track.track_db_id,
            playlist_track.entry_id,
            playlist_track.position,
            no_inc,
        )?);
    }

    Ok(Json(added))
}

async fn remove_playlist_entries(
    headers: HeaderMap,
    id: String,
    entry_ids: Vec<String>,
) -> Result<Json<Vec<PlaylistTrackResponse>>, AppError> {
    let playlist_db_id = {
        let db = STATE.db.read().await;
        db::lookup::find_node_id_by_id(&*db, &id)?
            .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?
    };
    let _principal = require_playlist_owner(&headers, playlist_db_id).await?;

    if entry_ids.is_empty() {
        return Err(AppError::bad_request("entry_ids cannot be empty"));
    }

    let mut db = STATE.db.write().await;
    let removed_tracks = playlists::remove_tracks(
        &mut db,
        QueryId::Id(playlist_db_id),
        &entry_ids
            .into_iter()
            .map(QueryId::Alias)
            .collect::<Vec<_>>(),
    )
    .map_err(|err| {
        let message = err.to_string();
        if message.starts_with("playlist entry not found") {
            AppError::not_found(message)
        } else {
            AppError::from(err)
        }
    })?;

    let mut removed = Vec::new();
    for playlist_track in removed_tracks {
        let track = db::tracks::get_by_id(&db, playlist_track.track_db_id)?.ok_or_else(|| {
            AppError::from(anyhow::anyhow!(
                "removed playlist entry '{}' references missing track {}",
                playlist_track.entry_id,
                playlist_track.track_db_id.0
            ))
        })?;
        removed.push(PlaylistTrackResponse {
            entry_id: playlist_track.entry_id,
            track: track.into(),
            artists: None,
            release: None,
            position: playlist_track.position,
        });
    }

    Ok(Json(removed))
}

async fn remove_playlist_tracks(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<RemovePlaylistTracksRequest>,
) -> Result<Json<Vec<PlaylistTrackResponse>>, AppError> {
    remove_playlist_entries(headers, id, request.entry_ids).await
}

async fn delete_playlist_track(
    headers: HeaderMap,
    Path((id, entry_id)): Path<(String, String)>,
) -> Result<Json<Vec<PlaylistTrackResponse>>, AppError> {
    remove_playlist_entries(headers, id, vec![entry_id]).await
}

async fn move_playlist_track(
    headers: HeaderMap,
    Path((id, entry_id)): Path<(String, String)>,
    Json(request): Json<MovePlaylistTrackRequest>,
) -> Result<Json<Vec<PlaylistTrackResponse>>, AppError> {
    let playlist_db_id = {
        let db = STATE.db.read().await;
        db::lookup::find_node_id_by_id(&*db, &id)?
            .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?
    };
    let _principal = require_playlist_owner(&headers, playlist_db_id).await?;

    let mut db = STATE.db.write().await;
    playlists::move_track(
        &mut db,
        QueryId::Id(playlist_db_id),
        QueryId::Alias(entry_id.clone()),
        request.new_position,
    )
    .map_err(|err| {
        let message = err.to_string();
        if message.contains("alias not found") {
            AppError::not_found(format!("Playlist entry not found: {entry_id}"))
        } else {
            AppError::from(err)
        }
    })?;

    let no_inc = PlaylistInc {
        tracks: true,
        artists: false,
        releases: false,
    };
    let items = build_tracks(&db, playlist_db_id, no_inc)?;
    Ok(Json(items))
}

fn create_playlist_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Create playlist")
        .description("Creates a new playlist owned by the authenticated user.")
        .response::<201, Json<PlaylistResponse>>()
}

fn list_playlists_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List playlists")
        .description("Returns playlists owned by the authenticated user. Use `inc` to include tracks, artists, releases.")
}

fn get_playlist_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get playlist by ID")
        .description("Returns a single playlist. Use `inc=tracks,artists,releases` to include track details. 404 if not found or not accessible.")
}

fn update_playlist_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Update playlist")
        .description("Updates playlist metadata. Only the playlist owner can update.")
}

fn delete_playlist_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Delete playlist")
        .description("Deletes a playlist. Only the playlist owner can delete.")
        .response::<204, ()>()
}

fn add_tracks_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Add tracks to playlist")
        .description("Adds one or more tracks to the end of the playlist. Returns the added items without artists or release details.")
}

fn remove_tracks_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Remove tracks from playlist")
        .description("Removes one or more tracks by their entry IDs. Returns the removed items.")
}

fn delete_track_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Remove track from playlist")
        .description("Removes one playlist track entry by ID. Returns the removed item.")
}

fn move_track_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Move track within playlist")
        .description("Moves a playlist track entry to a new position. Returns the full updated item list without artists or release details.")
}

pub fn playlist_routes() -> ApiRouter {
    ApiRouter::new()
        .api_route("/", post_with(create_playlist, create_playlist_docs))
        .api_route("/", get_with(get_playlists, list_playlists_docs))
        .api_route("/{id}", get_with(get_playlist, get_playlist_docs))
        .api_route("/{id}", patch_with(update_playlist, update_playlist_docs))
        .api_route("/{id}", delete_with(delete_playlist, delete_playlist_docs))
        .api_route(
            "/{id}/tracks",
            post_with(add_playlist_tracks, add_tracks_docs),
        )
        .api_route(
            "/{id}/tracks/remove",
            post_with(remove_playlist_tracks, remove_tracks_docs),
        )
        .api_route(
            "/{id}/tracks/{entry_id}",
            delete_with(delete_playlist_track, delete_track_docs),
        )
        .api_route(
            "/{id}/tracks/{entry_id}",
            patch_with(move_playlist_track, move_track_docs),
        )
}

#[cfg(test)]
mod tests {
    use agdb::{
        DbAny,
        DbId,
        QueryBuilder,
    };
    use anyhow::anyhow;
    use nanoid::nanoid;

    use crate::db::test_db::{
        connect,
        insert_artist,
        insert_release,
        insert_track,
        new_test_db,
    };

    use super::*;

    fn create_test_user(db: &mut DbAny) -> anyhow::Result<DbId> {
        let user_db_id = db
            .exec_mut(
                QueryBuilder::insert()
                    .nodes()
                    .values([[("username", "playlist-test-user").into()]])
                    .query(),
            )?
            .ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("users")
                .to(user_db_id)
                .query(),
        )?;
        Ok(user_db_id)
    }

    #[test]
    fn build_track_response_uses_release_artist_fallback() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_test_user(&mut db)?;
        let release_db_id = insert_release(&mut db, "Fallback Release")?;
        let track_db_id = insert_track(&mut db, "Track Missing Direct Artist")?;
        let artist_db_id = insert_artist(&mut db, "Fallback Artist")?;

        connect(&mut db, release_db_id, track_db_id)?;
        connect(&mut db, release_db_id, artist_db_id)?;

        let playlist = Playlist {
            db_id: None,
            id: nanoid!(),
            name: "Playlist".to_string(),
            description: None,
            is_public: Some(false),
            created_at: Some(1),
            updated_at: Some(1),
        };
        let playlist_db_id = db::playlists::create(&mut db, &playlist, user_db_id)?;
        let pt =
            db.transaction_mut(|t| db::playlists::add_track(t, playlist_db_id, track_db_id))?;
        let track = db::tracks::get_by_id(&db, track_db_id)?
            .ok_or_else(|| anyhow!("track missing after insert"))?;

        let response = build_track_response(
            &db,
            track,
            track_db_id,
            pt.entry_id,
            pt.position,
            PlaylistInc {
                tracks: true,
                artists: true,
                releases: false,
            },
        )?;

        let artists = response
            .artists
            .ok_or_else(|| anyhow!("playlist track artists missing"))?;
        assert_eq!(artists.len(), 1);
        assert_eq!(artists[0].name, "Fallback Artist");

        Ok(())
    }
}
