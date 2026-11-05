// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbId,
    QueryId,
};
#[cfg(feature = "docgen")]
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
use axum::{
    Router,
    routing::{
        delete,
        get,
        patch,
        post,
    },
};
use serde::{
    Deserialize,
    Serialize,
};
use std::{
    collections::HashMap,
    time::{
        SystemTime,
        UNIX_EPOCH,
    },
};

use crate::routes::request::bearer_credential;
use crate::services::catalog::SortSpec;
use crate::services::playlists::SortKey as PlaylistSortKey;
use crate::{
    routes::responses::{
        ArtistResponse,
        CoverResponse,
        PageResponse,
        ReleaseResponse,
        TrackChains,
        TrackResponse,
    },
    routes::{
        AppError,
        covers as route_covers,
        deserialize_inc,
        double_option,
    },
    services::STATE,
    services::{
        auth::{
            Principal,
            require_principal,
        },
        covers::shown::CoverTarget,
        entities::resolve_track_artists,
        pagination::SnapshotKey,
        playlists,
    },
};
use lyra_db::Playlist;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct CreatePlaylistRequest {
    name: String,
    description: Option<String>,
    is_public: Option<bool>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct UpdatePlaylistRequest {
    name: Option<String>,
    #[serde(default, deserialize_with = "double_option")]
    description: Option<Option<String>>,
    is_public: Option<bool>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct AddPlaylistTracksRequest {
    track_ids: Vec<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "0-based index to insert the tracks at, in order. Positions past the end append; omit to append."
        )
    )]
    #[serde(default)]
    position: Option<u64>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct RemovePlaylistTracksRequest {
    entry_ids: Vec<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct MovePlaylistTrackRequest {
    position: u64,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct PlaylistResponse {
    id: String,
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    is_public: bool,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Number of entries in the playlist.")
    )]
    track_count: u64,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Summed duration of the playlist's entries in milliseconds. Entries with no known duration, and entries whose track is not accessible to the caller, contribute nothing."
        )
    )]
    total_duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    tracks: Option<Vec<PlaylistTrackResponse>>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Present when requested via `inc=covers`; `null` when no current display cover is visible to the caller."
        )
    )]
    #[serde(skip_serializing_if = "Option::is_none")]
    cover: Option<Option<CoverResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Playlist creation time as an RFC3339 timestamp.")
    )]
    created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Playlist update time as an RFC3339 timestamp.")
    )]
    updated_at: Option<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct PlaylistTrackResponse {
    entry_id: String,
    track: TrackResponse,
    #[serde(skip_serializing_if = "Option::is_none")]
    artists: Option<Vec<ArtistResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    release: Option<ReleaseResponse>,
    position: u64,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct PlaylistQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Comma-separated or repeated values: tracks, artists, releases, covers."
        )
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    inc: Option<Vec<String>>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct PlaylistListQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Comma-separated or repeated values: tracks, artists, releases, covers."
        )
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    inc: Option<Vec<String>>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Optional text query matched against playlist names; each of its words must start a word, ignoring case, accents, apostrophes and punctuation; no character is an operator, and a word in a script written without spaces, such as Japanese, matches anywhere in a word."
        )
    )]
    query: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Comma-separated or repeated values: name, created_at, updated_at, track_count, total_duration, favorited_at, relevance, id."
        )
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    sort_by: Option<Vec<String>>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Sort direction: ascending or descending.")
    )]
    sort_order: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Optional user ID. Keeps only that user's playlists: all of them for the authenticated user, the public ones for anyone else."
        )
    )]
    owner_id: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Optional: `true` keeps only playlists the authenticated user has favorited, `false` only those they have not."
        )
    )]
    #[serde(default, deserialize_with = "super::deserialize_optional_scalar")]
    favorite: Option<bool>,
    #[serde(flatten)]
    page: super::PageQuery,
}

#[derive(Clone, Copy)]
struct PlaylistInc {
    tracks: bool,
    artists: bool,
    releases: bool,
    covers: bool,
}

impl PlaylistInc {
    /// What the entry-mutating handlers echo back: bare tracks, no relations.
    const TRACKS_ONLY: Self = Self {
        tracks: true,
        artists: false,
        releases: false,
        covers: false,
    };
}

fn parse_inc(inc: Option<Vec<String>>) -> Result<PlaylistInc, AppError> {
    let values = super::parse_inc_values(inc, &["tracks", "artists", "releases", "covers"])?;
    let mut result = PlaylistInc {
        tracks: false,
        artists: false,
        releases: false,
        covers: false,
    };
    for value in values {
        match value.as_str() {
            "tracks" => result.tracks = true,
            "artists" => result.artists = true,
            "releases" => result.releases = true,
            "covers" => result.covers = true,
            _ => {}
        }
    }

    // artists or releases imply tracks
    if result.artists || result.releases {
        result.tracks = true;
    }

    Ok(result)
}

fn parse_playlist_sort_specs(
    sort_by: Option<Vec<String>>,
    sort_order: Option<String>,
) -> Result<SortSpec<PlaylistSortKey>, AppError> {
    super::parse_route_sort_specs(
        sort_by,
        sort_order,
        PlaylistSortKey::parse,
        "name, created_at, updated_at, track_count, total_duration, favorited_at, relevance, id",
    )
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// The user an `owner_id` filter names.
fn resolve_owner_filter(db: &agdb::DbAny, owner_id: &str) -> Result<DbId, AppError> {
    let not_found = || AppError::not_found(format!("User not found: {owner_id}"));
    let owner_db_id = lyra_db::lookup::find_node_id_by_id(db, owner_id)?.ok_or_else(not_found)?;
    lyra_db::users::get_by_id(db, owner_db_id)?.ok_or_else(not_found)?;
    Ok(owner_db_id)
}

/// A 400 for an [`playlists::InvalidPlaylistRequest`], and `err` as it maps otherwise.
fn invalid_request(err: anyhow::Error) -> AppError {
    if err.is::<playlists::InvalidPlaylistRequest>() {
        AppError::bad_request(err.to_string())
    } else {
        AppError::from(err)
    }
}

fn resolve_owner_id(db: &agdb::DbAny, owner_db_id: Option<DbId>) -> anyhow::Result<Option<String>> {
    match owner_db_id {
        Some(db_id) => lyra_db::lookup::find_id_by_db_id(db, db_id),
        None => Ok(None),
    }
}

fn playlist_to_response(
    playlist: Playlist,
    tracks: Option<Vec<PlaylistTrackResponse>>,
    cover: Option<Option<CoverResponse>>,
    owner_id: Option<String>,
    summary: playlists::PlaylistSummary,
) -> PlaylistResponse {
    PlaylistResponse {
        id: playlist.id,
        name: playlist.name,
        description: playlist.description,
        is_public: playlist.is_public.unwrap_or(false),
        track_count: summary.track_count,
        total_duration_ms: summary.total_duration_ms,
        tracks,
        cover,
        owner_id,
        created_at: playlist
            .created_at_ms
            .map(crate::services::timestamps::unix_ms_to_rfc3339_u64),
        updated_at: playlist
            .updated_at_ms
            .map(crate::services::timestamps::unix_ms_to_rfc3339_u64),
    }
}

/// Summary for a single playlist, for the handlers that return one.
fn playlist_summary(
    db: &agdb::DbAny,
    principal: &Principal,
    playlist_db_id: DbId,
) -> anyhow::Result<playlists::PlaylistSummary> {
    Ok(playlists::summaries(db, principal, &[playlist_db_id])?
        .get(&playlist_db_id)
        .copied()
        .unwrap_or_default())
}

/// Resolves `id` to a playlist the principal owns, under the guard that acts on it.
fn require_owned_playlist(
    db: &impl lyra_db::DbAccess,
    principal: &Principal,
    id: &str,
) -> Result<DbId, AppError> {
    let playlist_db_id = lyra_db::lookup::find_node_id_by_id(db, id)?
        .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
    if !crate::services::auth::access::playlist_owned(db, principal, playlist_db_id)? {
        return Err(AppError::forbidden("you do not own this playlist"));
    }
    Ok(playlist_db_id)
}

/// Resolves `id` to a playlist the principal may delete, under the guard that deletes it.
fn require_deletable_playlist(
    db: &impl lyra_db::DbAccess,
    principal: &Principal,
    id: &str,
) -> Result<DbId, AppError> {
    let playlist_db_id = lyra_db::lookup::find_node_id_by_id(db, id)?
        .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
    if !crate::services::auth::access::playlist_deletable(db, principal, playlist_db_id)? {
        return Err(AppError::forbidden("you may not delete this playlist"));
    }
    Ok(playlist_db_id)
}

fn build_track_response_unchecked(
    db: &agdb::DbAny,
    track: TrackResponse,
    track_db_id: DbId,
    entry_id: String,
    position: u64,
    inc: PlaylistInc,
) -> anyhow::Result<PlaylistTrackResponse> {
    let availability = crate::services::catalog::Availability::of(db)?;
    let artists: Option<Vec<ArtistResponse>> = if inc.artists {
        Some(
            resolve_track_artists(db, track_db_id)?
                .into_iter()
                .map(|artist| ArtistResponse::new(artist, &availability))
                .collect(),
        )
    } else {
        None
    };
    let release: Option<ReleaseResponse> = if inc.releases {
        lyra_db::releases::get_by_track(db, track_db_id)?
            .into_iter()
            .next()
            .map(|release| ReleaseResponse::new(release, &availability))
    } else {
        None
    };
    Ok(PlaylistTrackResponse {
        entry_id,
        track,
        artists,
        release,
        position,
    })
}

fn build_track_response(
    db: &agdb::DbAny,
    principal: &Principal,
    track: TrackResponse,
    track_db_id: DbId,
    entry_id: String,
    position: u64,
    inc: PlaylistInc,
) -> anyhow::Result<PlaylistTrackResponse> {
    if !crate::services::auth::access::entity_accessible(db, principal, track_db_id)? {
        return Ok(PlaylistTrackResponse {
            entry_id,
            track: TrackResponse::unavailable(track.id),
            artists: inc
                .artists
                .then(|| vec![ArtistResponse::unavailable(String::new())]),
            release: inc
                .releases
                .then(|| ReleaseResponse::unavailable(String::new())),
            position,
        });
    }
    build_track_response_unchecked(db, track, track_db_id, entry_id, position, inc)
}

fn build_tracks(
    db: &agdb::DbAny,
    principal: &Principal,
    playlist_db_id: DbId,
    inc: PlaylistInc,
) -> anyhow::Result<Vec<PlaylistTrackResponse>> {
    let mut playlist_tracks = Vec::new();
    let mut tracks = Vec::new();
    for playlist_track in playlists::get_tracks(db, QueryId::Id(playlist_db_id))? {
        if let Some(track) = lyra_db::tracks::get_by_id(db, playlist_track.track_db_id)? {
            playlist_tracks.push(playlist_track);
            tracks.push(track);
        }
    }
    let responses = TrackChains::responses(db, tracks)?;
    let mut items = Vec::with_capacity(responses.len());
    for (playlist_track, track) in playlist_tracks.into_iter().zip(responses) {
        items.push(build_track_response(
            db,
            principal,
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
    let principal = require_principal(bearer_credential(&headers)).await?;

    let name = request.name.trim().to_string();
    if name.is_empty() {
        return Err(AppError::bad_request("playlist name cannot be empty"));
    }

    let now = now_ms();
    let mut db = STATE.db.write().await;
    let user_db_id = principal.require(&db)?;
    let (_, created) = playlists::create(
        &mut db,
        &playlists::CreatePlaylistRequest {
            user_db_id,
            name,
            description: request.description,
            is_public: request.is_public,
            created_at_ms: Some(now),
            updated_at_ms: Some(now),
            track_db_ids: Vec::new(),
        },
    )
    .map_err(|err| AppError::bad_request(err.to_string()))?;

    Ok((
        StatusCode::CREATED,
        Json(playlist_to_response(
            created,
            None,
            None,
            Some(principal.user_public_id),
            playlists::PlaylistSummary::default(),
        )),
    ))
}

async fn get_playlists(
    headers: HeaderMap,
    Query(query): Query<PlaylistListQuery>,
) -> Result<Json<PageResponse<PlaylistResponse>>, AppError> {
    let PlaylistListQuery {
        inc,
        query,
        sort_by,
        sort_order,
        owner_id,
        favorite,
        page,
    } = query;
    let principal = require_principal(bearer_credential(&headers)).await?;
    let inc = parse_inc(inc)?;
    let search_term = super::parse_text_query(query);
    let page_request = page.resolve_snapshot();
    let snapshot_key = SnapshotKey::builder(&principal.user_public_id, "playlists")
        .field(search_term.as_deref())
        .values(sort_by.as_deref())
        .field(sort_order.as_deref())
        .field(owner_id.as_deref())
        .flag(favorite)
        .finish();
    let sort = parse_playlist_sort_specs(sort_by, sort_order)?;

    let db = &*STATE.db.read().await;
    let owner = owner_id
        .as_deref()
        .map(|owner_id| resolve_owner_filter(db, owner_id))
        .transpose()?;
    let (page_playlists, next_cursor, mut summaries) = if let Some(page) =
        page_request.resume(&snapshot_key)?
    {
        let playlists_page = super::load_snapshot_items(
            db,
            &page.item_ids,
            lyra_db::playlists::get_by_id,
            |db, playlist_db_id| {
                crate::services::auth::access::playlist_accessible(db, &principal, playlist_db_id)
            },
        )?;
        (playlists_page, page.next_cursor, HashMap::new())
    } else {
        let (mut playlists_page, summaries) = playlists::list_visible(
            db,
            &principal,
            playlists::PlaylistFilter {
                owner,
                favorite,
                ..playlists::PlaylistFilter::default()
            },
            &sort,
            search_term.as_deref(),
        )
        .map_err(invalid_request)?;
        let page = page_request.start(
            &snapshot_key,
            playlists_page
                .iter()
                .map(|playlist| playlist.id.clone())
                .collect(),
        )?;
        playlists_page.truncate(page.item_ids.len());
        (playlists_page, page.next_cursor, summaries)
    };

    let playlist_db_ids = page_playlists
        .iter()
        .filter_map(|playlist| playlist.db_id.clone().map(DbId::from))
        .collect::<Vec<_>>();
    // The sort pass already summarised these unless it sorted on a field that
    // did not need them.
    if !playlist_db_ids.iter().all(|id| summaries.contains_key(id)) {
        summaries = playlists::summaries(db, &principal, &playlist_db_ids)?;
    }
    let mut owners = playlists::owner_ids(db, &playlist_db_ids)?;
    let covers = if inc.covers {
        let targets = page_playlists
            .iter()
            .map(CoverTarget::Playlist)
            .collect::<Vec<_>>();
        route_covers::resolve_principal_covers(db, &principal, &targets)?
    } else {
        HashMap::new()
    };

    let mut items = Vec::with_capacity(page_playlists.len());
    for playlist in page_playlists {
        let playlist_db_id: DbId = playlist
            .db_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("playlist missing db_id"))?
            .into();
        let tracks = if inc.tracks {
            Some(build_tracks(db, &principal, playlist_db_id, inc)?)
        } else {
            None
        };
        let cover = route_covers::cover_field(&covers, inc.covers, Some(playlist_db_id));
        items.push(playlist_to_response(
            playlist,
            tracks,
            cover,
            owners.remove(&playlist_db_id),
            summaries.get(&playlist_db_id).copied().unwrap_or_default(),
        ));
    }

    Ok(Json(PageResponse { items, next_cursor }))
}

async fn get_playlist(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<PlaylistQuery>,
) -> Result<Json<PlaylistResponse>, AppError> {
    let principal = require_principal(bearer_credential(&headers)).await?;
    let inc = parse_inc(query.inc)?;

    let db = &*STATE.db.read().await;
    let playlist_db_id = lyra_db::lookup::find_node_id_by_id(db, &id)?
        .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
    let playlist = lyra_db::playlists::get_by_id(db, playlist_db_id)?
        .ok_or_else(|| AppError::not_found(format!("Playlist not found: {}", id)))?;

    // Check access: owner or public
    let owner_db_id = playlists::get_owner(db, QueryId::Id(playlist_db_id))?;
    if !crate::services::auth::access::playlist_accessible(db, &principal, playlist_db_id)? {
        return Err(AppError::not_found(format!("Playlist not found: {}", id)));
    }

    let tracks = if inc.tracks {
        Some(build_tracks(db, &principal, playlist_db_id, inc)?)
    } else {
        None
    };
    let cover = if inc.covers {
        let covers = route_covers::resolve_principal_covers(
            db,
            &principal,
            &[CoverTarget::Playlist(&playlist)],
        )?;
        route_covers::cover_field(&covers, true, Some(playlist_db_id))
    } else {
        None
    };

    Ok(Json(playlist_to_response(
        playlist,
        tracks,
        cover,
        resolve_owner_id(db, owner_db_id)?,
        playlist_summary(db, &principal, playlist_db_id)?,
    )))
}

async fn update_playlist(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<UpdatePlaylistRequest>,
) -> Result<Json<PlaylistResponse>, AppError> {
    let principal = require_principal(bearer_credential(&headers)).await?;

    let mut db = STATE.db.write().await;
    let playlist_db_id = require_owned_playlist(&db, &principal, &id)?;
    let playlist = playlists::update(
        &mut db,
        &playlists::UpdatePlaylistRequest {
            playlist_id: QueryId::Id(playlist_db_id),
            name: request.name,
            description: request.description,
            is_public: request.is_public,
            updated_at_ms: Some(now_ms()),
        },
    )
    .map_err(|err| AppError::bad_request(err.to_string()))?
    .ok_or_else(|| AppError::not_found(format!("Playlist not found: {}", id)))?;

    let summary = playlist_summary(&db, &principal, playlist_db_id)?;
    Ok(Json(playlist_to_response(
        playlist,
        None,
        None,
        Some(principal.user_public_id),
        summary,
    )))
}

async fn delete_playlist(
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<StatusCode, AppError> {
    let principal = require_principal(bearer_credential(&headers)).await?;

    let mut db = STATE.db.write().await;
    let playlist_db_id = require_deletable_playlist(&db, &principal, &id)?;
    playlists::delete(&mut db, QueryId::Id(playlist_db_id))?
        .ok_or_else(|| AppError::not_found(format!("Playlist not found: {}", id)))?;

    Ok(StatusCode::NO_CONTENT)
}

async fn add_playlist_tracks(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<AddPlaylistTracksRequest>,
) -> Result<Json<Vec<PlaylistTrackResponse>>, AppError> {
    let principal = require_principal(bearer_credential(&headers)).await?;

    if request.track_ids.is_empty() {
        return Err(AppError::bad_request("track_ids cannot be empty"));
    }
    playlists::check_id_count("track ids", request.track_ids.len()).map_err(invalid_request)?;

    let mut db = STATE.db.write().await;
    let playlist_db_id = require_owned_playlist(&db, &principal, &id)?;

    let mut track_query_ids = Vec::with_capacity(request.track_ids.len());
    for track_id in request.track_ids {
        let track_db_id = lyra_db::lookup::find_node_id_by_id(&*db, &track_id)?
            .ok_or_else(|| AppError::not_found(format!("track not found: {track_id}")))?;
        crate::services::auth::access::require_entity_accessible(
            &*db,
            &principal,
            track_db_id,
            || AppError::not_found(format!("track not found: {track_id}")),
        )?;
        track_query_ids.push(QueryId::Id(track_db_id));
    }

    let results = playlists::add_tracks(
        &mut db,
        QueryId::Id(playlist_db_id),
        &track_query_ids,
        request.position,
    )
    .map_err(|err| {
        let message = err.to_string();
        if message.starts_with("track not found") {
            AppError::not_found(message)
        } else {
            invalid_request(err)
        }
    })?;

    let mut tracks = Vec::with_capacity(results.len());
    for playlist_track in &results {
        tracks.push(
            lyra_db::tracks::get_by_id(&db, playlist_track.track_db_id)?
                .ok_or_else(|| AppError::not_found("playlist track target missing"))?,
        );
    }
    let responses = TrackChains::responses(&*db, tracks)?;
    let mut added = Vec::with_capacity(responses.len());
    for (playlist_track, track) in results.into_iter().zip(responses) {
        added.push(build_track_response(
            &db,
            &principal,
            track,
            playlist_track.track_db_id,
            playlist_track.entry_id,
            playlist_track.position,
            PlaylistInc::TRACKS_ONLY,
        )?);
    }

    Ok(Json(added))
}

async fn remove_playlist_entries(
    headers: HeaderMap,
    id: String,
    entry_ids: Vec<String>,
) -> Result<Json<Vec<PlaylistTrackResponse>>, AppError> {
    let principal = require_principal(bearer_credential(&headers)).await?;

    if entry_ids.is_empty() {
        return Err(AppError::bad_request("entry_ids cannot be empty"));
    }
    playlists::check_id_count("entry ids", entry_ids.len()).map_err(invalid_request)?;

    let mut db = STATE.db.write().await;
    let playlist_db_id = require_owned_playlist(&db, &principal, &id)?;

    let removed_tracks =
        playlists::remove_tracks(&mut db, playlist_db_id, &entry_ids).map_err(|err| {
            let message = err.to_string();
            if message.starts_with("playlist entry not found") {
                AppError::not_found(message)
            } else {
                invalid_request(err)
            }
        })?;

    let mut tracks = Vec::with_capacity(removed_tracks.len());
    for playlist_track in &removed_tracks {
        tracks.push(
            lyra_db::tracks::get_by_id(&db, playlist_track.track_db_id)?.ok_or_else(|| {
                AppError::from(anyhow::anyhow!(
                    "removed playlist entry '{}' references missing track {}",
                    playlist_track.entry_id,
                    playlist_track.track_db_id.0
                ))
            })?,
        );
    }
    let responses = TrackChains::responses(&*db, tracks)?;
    let mut removed = Vec::with_capacity(responses.len());
    for (playlist_track, track) in removed_tracks.into_iter().zip(responses) {
        removed.push(build_track_response(
            &db,
            &principal,
            track,
            playlist_track.track_db_id,
            playlist_track.entry_id,
            playlist_track.position,
            PlaylistInc::TRACKS_ONLY,
        )?);
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
    let principal = require_principal(bearer_credential(&headers)).await?;

    let mut db = STATE.db.write().await;
    let playlist_db_id = require_owned_playlist(&db, &principal, &id)?;
    playlists::move_track(&mut db, playlist_db_id, &entry_id, request.position).map_err(|err| {
        let message = err.to_string();
        if message.starts_with("playlist entry not found") {
            AppError::not_found(message)
        } else {
            AppError::from(err)
        }
    })?;

    let items = build_tracks(&db, &principal, playlist_db_id, PlaylistInc::TRACKS_ONLY)?;
    Ok(Json(items))
}

#[cfg(feature = "docgen")]
fn create_playlist_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Create playlist")
        .description("Creates a new playlist owned by the authenticated user.")
        .response::<201, Json<PlaylistResponse>>()
}

#[cfg(feature = "docgen")]
fn list_playlists_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List playlists")
        .description("Returns a cursor-paginated page of the playlists the authenticated user can see: their own and every public playlist. `owner_id` keeps one user's playlists; pass the authenticated user's ID for their own. `favorite=true` keeps only playlists the user has favorited and `favorite=false` the rest. `sort_by` supports `name`, `created_at`, `updated_at`, `track_count`, `total_duration`, `favorited_at` (when the user first favorited the playlist; unfavorited playlists sort last in both directions), `relevance` (how well the name matches `query`, which it needs), and `id`. Without `sort_by`, a `query` ranks by relevance and anything else is by name. Track counts and durations are always included. Use `inc` to include tracks, artists, releases, covers.")
}

#[cfg(feature = "docgen")]
fn get_playlist_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get playlist by ID")
        .description("Returns a single playlist. Use `inc=tracks,artists,releases,covers` to include track or cover details. 404 if not found or not accessible.")
}

#[cfg(feature = "docgen")]
fn update_playlist_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Update playlist")
        .description("Updates playlist metadata. Only the playlist owner can update.")
}

#[cfg(feature = "docgen")]
fn delete_playlist_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Delete playlist")
        .description("Deletes a playlist. Its owner and admins can delete it.")
        .response::<204, ()>()
}

#[cfg(feature = "docgen")]
fn add_tracks_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Add tracks to playlist")
        .description("Adds one or more tracks to the playlist, in order, at `position` or at the end. Returns the added items without artists or release details.")
}

#[cfg(feature = "docgen")]
fn remove_tracks_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Remove tracks from playlist")
        .description("Removes one or more tracks by their entry IDs. Returns the removed items.")
}

#[cfg(feature = "docgen")]
fn delete_track_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Remove track from playlist")
        .description("Removes one playlist track entry by ID. Returns the removed item.")
}

#[cfg(feature = "docgen")]
fn move_track_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Move track within playlist")
        .description("Moves a playlist track entry to a new position. `position` is a 0-based index into the list after the moved entry is removed. Returns the full updated item list without artists or release details.")
}

pub fn playlist_routes() -> Router {
    Router::new()
        .route("/", post(create_playlist))
        .route("/", get(get_playlists))
        .route("/{id}", get(get_playlist))
        .route("/{id}", patch(update_playlist))
        .route("/{id}", delete(delete_playlist))
        .route("/{id}/tracks", post(add_playlist_tracks))
        .route("/{id}/tracks/remove", post(remove_playlist_tracks))
        .route("/{id}/tracks/{entry_id}", delete(delete_playlist_track))
        .route("/{id}/tracks/{entry_id}", patch(move_playlist_track))
        .merge(route_covers::owner_cover_routes::<
            route_covers::PlaylistCovers,
        >())
}

#[cfg(feature = "docgen")]
pub(crate) fn playlist_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::{
        delete_with,
        get_with,
        patch_with,
        post_with,
    };

    aide::axum::ApiRouter::new()
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
        .merge(route_covers::owner_cover_openapi_routes::<
            route_covers::PlaylistCovers,
        >())
}

#[cfg(test)]
mod tests {
    use agdb::{
        DbAny,
        DbId,
    };

    use super::UpdatePlaylistRequest;

    /// `Some(None)` must be reachable from the wire. A plain
    /// `Option<Option<T>>` derive maps JSON `null` to the outer `None`, making
    /// an explicit clear indistinguishable from an omitted field.
    #[test]
    fn update_playlist_request_distinguishes_null_from_absent_description() {
        let cleared: UpdatePlaylistRequest =
            serde_json::from_str(r#"{"description":null}"#).expect("null body parses");
        assert_eq!(cleared.description, Some(None), "explicit null clears");

        let absent: UpdatePlaylistRequest =
            serde_json::from_str(r#"{"name":"n"}"#).expect("absent body parses");
        assert_eq!(
            absent.description, None,
            "omitted field leaves it untouched"
        );

        let set: UpdatePlaylistRequest =
            serde_json::from_str(r#"{"description":"d"}"#).expect("value body parses");
        assert_eq!(set.description, Some(Some("d".to_string())));
    }

    use anyhow::anyhow;
    use nanoid::nanoid;

    use lyra_db::fixtures::{
        connect,
        insert_artist,
        insert_release,
        insert_track,
    };
    use lyra_db::test_db::new_test_db;

    use super::*;

    fn create_test_user(db: &mut DbAny) -> anyhow::Result<DbId> {
        lyra_db::test_db::insert_user(db, "playlist-test-user")
    }

    fn seed_playlist(db: &mut DbAny, user_db_id: DbId, name: &str) -> anyhow::Result<DbId> {
        playlists::create(
            db,
            &playlists::CreatePlaylistRequest {
                user_db_id,
                name: name.to_string(),
                description: None,
                is_public: None,
                created_at_ms: None,
                updated_at_ms: None,
                track_db_ids: Vec::new(),
            },
        )
        .map(|(playlist_db_id, _)| playlist_db_id)
    }

    async fn session_headers(user_db_id: DbId) -> anyhow::Result<HeaderMap> {
        let session =
            crate::services::testing::create_session(user_db_id, Default::default()).await?;
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {}", session.token).parse()?,
        );
        Ok(headers)
    }

    struct RecycleOutcome<T> {
        playlist_db_id: DbId,
        /// The attacker's entries before the request, in order.
        entry_ids: Vec<String>,
        victim: Option<VictimPlaylist>,
        output: T,
    }

    /// The playlist a victim creates in the attacker's recycled `DbId`, with its entry ids.
    struct VictimPlaylist {
        db_id: DbId,
        entry_ids: Vec<String>,
    }

    /// What the attacker's request acts on: their own playlist, a track they may add, and the
    /// first of the playlist's two entries.
    struct AttackerRequest {
        headers: HeaderMap,
        playlist_id: String,
        track_id: String,
        entry_id: String,
    }

    fn seed_entries(db: &mut DbAny, playlist_db_id: DbId, track_db_id: DbId) -> Vec<String> {
        (0..2)
            .map(|_| {
                playlists::add_track(db, QueryId::Id(playlist_db_id), QueryId::Id(track_db_id))
                    .expect("seed playlist entry")
                    .entry_id
            })
            .collect()
    }

    /// Runs `request` as an attacker against their own playlist while, at gap `gap`, the
    /// playlist is deleted and a victim creates one that takes its `DbId`.
    async fn run_against_recycled_playlist<T>(
        gap: Option<usize>,
        request: impl AsyncFnOnce(AttackerRequest) -> T,
    ) -> anyhow::Result<(bool, RecycleOutcome<T>)> {
        crate::plugins::testing::init_default_test_state()?;
        let (attacker, victim, track_db_id, playlist_db_id, entry_ids, request_input) = {
            let mut db = STATE.db.write().await;
            lyra_db::roles::ensure_builtin_roles(&mut db)?;
            let attacker = lyra_db::test_db::insert_user(&mut db, "attacker")?;
            lyra_db::roles::ensure_user_has_role(
                &mut db,
                attacker,
                lyra_db::roles::BUILTIN_ADMIN_ROLE,
            )?;
            let victim = lyra_db::test_db::insert_user(&mut db, "victim")?;
            let track_db_id = insert_track(&mut db, "Shared Track")?;
            let playlist_db_id = seed_playlist(&mut db, attacker, "Attacker's")?;
            let entry_ids = seed_entries(&mut db, playlist_db_id, track_db_id);
            let public_id = |db_id| {
                lyra_db::lookup::find_id_by_db_id(&*db, db_id)?
                    .ok_or_else(|| anyhow!("public id missing for {db_id:?}"))
            };
            let playlist_id = public_id(playlist_db_id)?;
            let track_id = public_id(track_db_id)?;
            (
                attacker,
                victim,
                track_db_id,
                playlist_db_id,
                entry_ids,
                (playlist_id, track_id),
            )
        };
        let (playlist_id, track_id) = request_input;
        let headers = session_headers(attacker).await?;

        let mut victim_playlist = None;
        let (reached, output) = crate::services::testing::run_with_recycle_at(
            gap,
            request(AttackerRequest {
                headers,
                playlist_id,
                track_id,
                entry_id: entry_ids[0].clone(),
            }),
            |db| {
                playlists::delete(db, QueryId::Id(playlist_db_id))
                    .expect("delete attacker playlist")
                    .expect("attacker playlist exists");
                let recycled = seed_playlist(db, victim, "Victim's").expect("seed victim playlist");
                assert_eq!(
                    recycled, playlist_db_id,
                    "agdb must reuse the playlist DbId"
                );
                victim_playlist = Some(VictimPlaylist {
                    db_id: recycled,
                    entry_ids: seed_entries(db, recycled, track_db_id),
                });
            },
        )
        .await;
        Ok((
            reached,
            RecycleOutcome {
                playlist_db_id,
                entry_ids,
                victim: victim_playlist,
                output,
            },
        ))
    }

    fn status_of<T>(result: Result<T, AppError>) -> Result<(), StatusCode> {
        result
            .map(|_| ())
            .map_err(|error| axum::response::IntoResponse::into_response(error).status())
    }

    async fn rename(request: AttackerRequest) -> Result<(), StatusCode> {
        status_of(
            update_playlist(
                request.headers,
                Path(request.playlist_id),
                Json(UpdatePlaylistRequest {
                    name: Some("Renamed".to_string()),
                    description: None,
                    is_public: None,
                }),
            )
            .await,
        )
    }

    async fn delete(request: AttackerRequest) -> Result<(), StatusCode> {
        status_of(delete_playlist(request.headers, Path(request.playlist_id)).await)
    }

    async fn add_track(request: AttackerRequest) -> Result<(), StatusCode> {
        status_of(
            add_playlist_tracks(
                request.headers,
                Path(request.playlist_id),
                Json(AddPlaylistTracksRequest {
                    track_ids: vec![request.track_id],
                    position: None,
                }),
            )
            .await,
        )
    }

    async fn remove_entry(request: AttackerRequest) -> Result<(), StatusCode> {
        status_of(
            remove_playlist_entries(request.headers, request.playlist_id, vec![request.entry_id])
                .await,
        )
    }

    async fn move_entry(request: AttackerRequest) -> Result<(), StatusCode> {
        status_of(
            move_playlist_track(
                request.headers,
                Path((request.playlist_id, request.entry_id)),
                Json(MovePlaylistTrackRequest { position: 1 }),
            )
            .await,
        )
    }

    /// Runs the recycle at every gap `request` has.
    async fn for_each_recycled_gap(
        request: impl AsyncFn(AttackerRequest) -> Result<(), StatusCode>,
        check: impl Fn(usize, &agdb::DbAny, &VictimPlaylist) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        crate::services::testing::for_each_db_gap(
            async |gap| run_against_recycled_playlist(gap, &request).await,
            async |gap, outcome| {
                let victim = outcome
                    .victim
                    .ok_or_else(|| anyhow!("victim playlist not created at gap {gap}"))?;
                check(gap, &*STATE.db.read().await, &victim)
            },
        )
        .await
    }

    fn entry_ids(db: &agdb::DbAny, playlist_db_id: DbId) -> anyhow::Result<Vec<String>> {
        Ok(lyra_db::playlists::get_tracks(db, playlist_db_id)?
            .into_iter()
            .map(|track| track.entry_id)
            .collect())
    }

    /// The victim's playlist keeps exactly the entries it was created with.
    fn victim_entries_untouched(
        gap: usize,
        db: &agdb::DbAny,
        victim: &VictimPlaylist,
    ) -> anyhow::Result<()> {
        assert_eq!(
            entry_ids(db, victim.db_id)?,
            victim.entry_ids,
            "victim entries changed at gap {gap}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn update_playlist_never_renames_a_playlist_that_took_a_recycled_id() -> anyhow::Result<()>
    {
        let _guard = crate::services::testing::runtime_test_lock().await;
        let (_, control) = run_against_recycled_playlist(None, rename).await?;
        assert_eq!(control.output, Ok(()));
        let renamed =
            lyra_db::playlists::get_by_id(&*STATE.db.read().await, control.playlist_db_id)?
                .ok_or_else(|| anyhow!("control playlist missing"))?;
        assert_eq!(renamed.name, "Renamed");

        for_each_recycled_gap(rename, |gap, db, victim| {
            let victim_playlist = lyra_db::playlists::get_by_id(db, victim.db_id)?
                .ok_or_else(|| anyhow!("victim playlist missing after gap {gap}"))?;
            assert_eq!(victim_playlist.name, "Victim's", "renamed at gap {gap}");
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn delete_playlist_never_deletes_a_playlist_that_took_a_recycled_id() -> anyhow::Result<()>
    {
        let _guard = crate::services::testing::runtime_test_lock().await;
        let (_, control) = run_against_recycled_playlist(None, delete).await?;
        assert_eq!(control.output, Ok(()));
        assert!(
            lyra_db::playlists::get_by_id(&*STATE.db.read().await, control.playlist_db_id)?
                .is_none()
        );

        for_each_recycled_gap(delete, |gap, db, victim| {
            assert!(
                lyra_db::playlists::get_by_id(db, victim.db_id)?.is_some(),
                "deleted at gap {gap}"
            );
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn add_playlist_tracks_never_adds_to_a_playlist_that_took_a_recycled_id()
    -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        let (_, control) = run_against_recycled_playlist(None, add_track).await?;
        assert_eq!(control.output, Ok(()));
        assert_eq!(
            entry_ids(&*STATE.db.read().await, control.playlist_db_id)?.len(),
            3
        );

        for_each_recycled_gap(add_track, victim_entries_untouched).await
    }

    #[tokio::test]
    async fn remove_playlist_entries_never_removes_from_a_playlist_that_took_a_recycled_id()
    -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        let (_, control) = run_against_recycled_playlist(None, remove_entry).await?;
        assert_eq!(control.output, Ok(()));
        assert_eq!(
            entry_ids(&*STATE.db.read().await, control.playlist_db_id)?,
            control.entry_ids[1..]
        );

        for_each_recycled_gap(remove_entry, victim_entries_untouched).await
    }

    #[tokio::test]
    async fn move_playlist_track_never_moves_within_a_playlist_that_took_a_recycled_id()
    -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        let (_, control) = run_against_recycled_playlist(None, move_entry).await?;
        assert_eq!(control.output, Ok(()));
        let moved = [control.entry_ids[1].clone(), control.entry_ids[0].clone()];
        assert_eq!(
            entry_ids(&*STATE.db.read().await, control.playlist_db_id)?,
            moved
        );

        for_each_recycled_gap(move_entry, victim_entries_untouched).await
    }

    #[test]
    fn parse_playlist_sort_specs_rejects_unknown_keys() -> anyhow::Result<()> {
        use crate::services::catalog::Direction::Descending;

        let specs = parse_playlist_sort_specs(
            Some(vec!["track_count,name".to_string()]),
            Some("descending".to_string()),
        )
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert_eq!(specs.len(), 2);
        assert_eq!(
            specs,
            [
                (PlaylistSortKey::TrackCount, Descending),
                (PlaylistSortKey::Name, Descending),
            ]
        );

        let err = parse_playlist_sort_specs(Some(vec!["bogus".to_string()]), None)
            .expect_err("unknown sort key should be rejected");
        assert!(format!("{err:?}").contains("bogus"));

        Ok(())
    }

    #[test]
    fn parse_inc_accepts_covers_and_implies_tracks() -> anyhow::Result<()> {
        let inc = parse_inc(Some(vec!["covers".to_string()]))
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert!(inc.covers);
        assert!(!inc.tracks);

        let inc = parse_inc(Some(vec!["releases".to_string()]))
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert!(inc.tracks, "releases should imply tracks");

        Ok(())
    }

    #[test]
    fn snapshot_key_separates_collection_shaping_inputs() {
        let key = |search: Option<&str>, sort_by: Option<&[String]>, order: Option<&str>| {
            SnapshotKey::builder("user", "playlists")
                .field(search)
                .values(sort_by)
                .field(order)
                .finish()
        };
        let name = [String::from("name")];
        let count = [String::from("track_count")];
        let registry = crate::services::pagination::SnapshotRegistry::default();
        let items = || vec!["a".to_string(), "b".to_string()];

        // A cursor minted under one shaping must not resume under another.
        let page = registry
            .start(&key(None, Some(&name), Some("ascending")), items(), 1)
            .expect("first page");
        let cursor = page.next_cursor.expect("cursor");
        for other in [
            key(Some("jazz"), Some(&name), Some("ascending")),
            key(None, Some(&count), Some("ascending")),
            key(None, Some(&name), Some("descending")),
        ] {
            assert!(
                registry.resume(&other, &cursor, 1).is_err(),
                "cursor must be bound to search term, sort key, and direction"
            );
        }
        assert!(
            registry
                .resume(&key(None, Some(&name), Some("ascending")), &cursor, 1)
                .is_ok(),
            "identical shaping must resume"
        );
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
            created_at_ms: Some(1),
            updated_at_ms: Some(1),
        };
        let playlist_db_id = lyra_db::playlists::create(&mut db, &playlist, user_db_id)?;
        let pt =
            db.transaction_mut(|t| lyra_db::playlists::add_track(t, playlist_db_id, track_db_id))?;
        let track = lyra_db::tracks::get_by_id(&db, track_db_id)?
            .ok_or_else(|| anyhow!("track missing after insert"))?;
        let chains = TrackChains::of(&db, [&track])?;

        let response = build_track_response_unchecked(
            &db,
            chains.response(track),
            track_db_id,
            pt.entry_id,
            pt.position,
            PlaylistInc {
                artists: true,
                ..PlaylistInc::TRACKS_ONLY
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
