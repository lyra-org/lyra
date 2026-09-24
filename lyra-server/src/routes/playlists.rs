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
    cmp::Ordering,
    collections::HashMap,
    time::{
        SystemTime,
        UNIX_EPOCH,
    },
};

use crate::{
    STATE,
    db::{
        self,
        Playlist,
        SortDirection,
    },
    routes::responses::{
        ArtistResponse,
        CoverResponse,
        PageResponse,
        ReleaseResponse,
        TrackResponse,
    },
    routes::{
        AppError,
        covers as route_covers,
        deserialize_inc,
        double_option,
    },
    services::{
        auth::{
            Principal,
            require_principal,
        },
        covers as cover_services,
        entities::resolve_track_artists,
        pagination::SnapshotKey,
        playlists,
    },
};

const PLAYLIST_BULK_TRACK_HARD_CAP: usize = 500;

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
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct RemovePlaylistTracksRequest {
    entry_ids: Vec<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct MovePlaylistTrackRequest {
    new_position: u64,
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
        schemars(description = "Optional fuzzy text query matched against playlist names.")
    )]
    query: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Comma-separated or repeated values: name, created_at, updated_at, track_count, total_duration, id."
        )
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    sort_by: Option<Vec<String>>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Sort direction: ascending or descending.")
    )]
    sort_order: Option<String>,
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

#[derive(Clone, Copy, Debug)]
enum PlaylistRouteSortKey {
    Name,
    CreatedAt,
    UpdatedAt,
    TrackCount,
    TotalDuration,
    Id,
}

type PlaylistRouteSortSpec = super::RouteSortSpec<PlaylistRouteSortKey>;

fn default_playlist_sort() -> Vec<PlaylistRouteSortSpec> {
    vec![PlaylistRouteSortSpec {
        key: PlaylistRouteSortKey::Name,
        direction: SortDirection::Ascending,
    }]
}

fn parse_playlist_sort_specs(
    sort_by: Option<Vec<String>>,
    sort_order: Option<String>,
) -> Result<Vec<PlaylistRouteSortSpec>, AppError> {
    super::parse_route_sort_specs(
        sort_by,
        sort_order,
        |token| match token {
            "name" => Some(PlaylistRouteSortKey::Name),
            "created_at" => Some(PlaylistRouteSortKey::CreatedAt),
            "updated_at" => Some(PlaylistRouteSortKey::UpdatedAt),
            "track_count" => Some(PlaylistRouteSortKey::TrackCount),
            "total_duration" => Some(PlaylistRouteSortKey::TotalDuration),
            "id" => Some(PlaylistRouteSortKey::Id),
            _ => None,
        },
        "name, created_at, updated_at, track_count, total_duration, id",
    )
}

fn playlist_sort_needs_summaries(sort: &[PlaylistRouteSortSpec]) -> bool {
    sort.iter().any(|spec| {
        matches!(
            spec.key,
            PlaylistRouteSortKey::TrackCount | PlaylistRouteSortKey::TotalDuration
        )
    })
}

struct PlaylistRouteSortEntry {
    playlist: Playlist,
    sort_name: String,
    summary: playlists::PlaylistSummary,
    match_score: u32,
}

fn compare_playlist_route_field(
    a: &PlaylistRouteSortEntry,
    b: &PlaylistRouteSortEntry,
    key: PlaylistRouteSortKey,
) -> Ordering {
    match key {
        PlaylistRouteSortKey::Name => a
            .sort_name
            .cmp(&b.sort_name)
            .then_with(|| a.playlist.name.cmp(&b.playlist.name)),
        PlaylistRouteSortKey::CreatedAt => {
            db::compare_option(&a.playlist.created_at, &b.playlist.created_at)
        }
        PlaylistRouteSortKey::UpdatedAt => {
            db::compare_option(&a.playlist.updated_at, &b.playlist.updated_at)
        }
        PlaylistRouteSortKey::TrackCount => a.summary.track_count.cmp(&b.summary.track_count),
        PlaylistRouteSortKey::TotalDuration => a
            .summary
            .total_duration_ms
            .cmp(&b.summary.total_duration_ms),
        PlaylistRouteSortKey::Id => a.playlist.id.cmp(&b.playlist.id),
    }
}

fn compare_playlist_route_entries(
    a: &PlaylistRouteSortEntry,
    b: &PlaylistRouteSortEntry,
    sort: &[PlaylistRouteSortSpec],
) -> Ordering {
    for spec in sort {
        let ord = db::apply_direction(compare_playlist_route_field(a, b, spec.key), spec.direction);
        if ord != Ordering::Equal {
            return ord;
        }
    }

    b.match_score
        .cmp(&a.match_score)
        .then_with(|| a.sort_name.cmp(&b.sort_name))
        .then_with(|| a.playlist.name.cmp(&b.playlist.name))
        .then_with(|| a.playlist.id.cmp(&b.playlist.id))
}

/// Ordered playlists for the caller, plus any summaries computed along the way
/// so the caller does not have to recompute them for the page.
fn query_playlist_route_items(
    db: &agdb::DbAny,
    principal: &Principal,
    sort: &[PlaylistRouteSortSpec],
    search_term: Option<&str>,
) -> anyhow::Result<(Vec<Playlist>, HashMap<DbId, playlists::PlaylistSummary>)> {
    let playlists_for_user = playlists::get_by_user(db, principal.require(db)?)?;
    let mut entries = playlists_for_user
        .into_iter()
        .map(|playlist| PlaylistRouteSortEntry {
            sort_name: playlist.name.to_lowercase(),
            playlist,
            summary: playlists::PlaylistSummary::default(),
            match_score: 0,
        })
        .collect::<Vec<_>>();

    // Filter before summarising: discarded playlists should not be walked.
    if let Some(term) = search_term {
        db::search::fuzzy_filter(
            &mut entries,
            term,
            |entry| entry.playlist.name.as_str(),
            |entry, score| entry.match_score = score,
        );
    }

    let summaries = if playlist_sort_needs_summaries(sort) {
        let playlist_db_ids = entries
            .iter()
            .filter_map(|entry| entry.playlist.db_id.clone().map(DbId::from))
            .collect::<Vec<_>>();
        let summaries = playlists::summaries(db, principal, &playlist_db_ids)?;
        for entry in &mut entries {
            entry.summary = entry
                .playlist
                .db_id
                .clone()
                .map(DbId::from)
                .and_then(|playlist_db_id| summaries.get(&playlist_db_id).copied())
                .unwrap_or_default();
        }
        summaries
    } else {
        HashMap::new()
    };

    entries.sort_by(|a, b| compare_playlist_route_entries(a, b, sort));
    Ok((
        entries.into_iter().map(|entry| entry.playlist).collect(),
        summaries,
    ))
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
        created_at: playlist.created_at.map(super::unix_secs_to_rfc3339_u64),
        updated_at: playlist.updated_at.map(super::unix_secs_to_rfc3339_u64),
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
    db: &impl db::DbAccess,
    principal: &Principal,
    id: &str,
) -> Result<DbId, AppError> {
    let playlist_db_id = db::lookup::find_node_id_by_id(db, id)?
        .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
    if !crate::services::auth::access::playlist_owned(db, principal, playlist_db_id)? {
        return Err(AppError::forbidden("you do not own this playlist"));
    }
    Ok(playlist_db_id)
}

fn build_track_response_unchecked(
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

fn build_track_response(
    db: &agdb::DbAny,
    principal: &Principal,
    track: db::Track,
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
    let playlist_tracks = playlists::get_tracks(db, QueryId::Id(playlist_db_id))?;
    let mut items = Vec::with_capacity(playlist_tracks.len());
    for playlist_track in playlist_tracks {
        let Some(track) = db::tracks::get_by_id(db, playlist_track.track_db_id)? else {
            continue;
        };
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
    let principal = require_principal(&headers).await?;

    let name = request.name.trim().to_string();
    if name.is_empty() {
        return Err(AppError::bad_request("playlist name cannot be empty"));
    }

    let now = now_epoch();
    let mut db = STATE.db.write().await;
    let user_db_id = principal.require(&db)?;
    let playlist_db_id = playlists::create(
        &mut db,
        &playlists::CreatePlaylistRequest {
            user_db_id,
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
        page,
    } = query;
    let principal = require_principal(&headers).await?;
    let inc = parse_inc(inc)?;
    let search_term = super::parse_text_query(query);
    let page_request = page.resolve_snapshot();
    let snapshot_key = SnapshotKey::builder(&principal.user_public_id, "playlists")
        .field(search_term.as_deref())
        .values(sort_by.as_deref())
        .field(sort_order.as_deref())
        .finish();
    let mut sort = parse_playlist_sort_specs(sort_by, sort_order)?;
    if sort.is_empty() {
        sort = default_playlist_sort();
    }

    let db = &*STATE.db.read().await;
    let (page_playlists, next_cursor, mut summaries) =
        if let Some(page) = page_request.resume(&snapshot_key)? {
            let user_db_id = principal.require(db)?;
            let playlists_page = super::load_snapshot_items(
                db,
                &page.item_ids,
                db::playlists::get_by_id,
                |db, playlist_db_id| {
                    Ok(db::playlists::get_owner(db, playlist_db_id)? == Some(user_db_id))
                },
            )?;
            (playlists_page, page.next_cursor, HashMap::new())
        } else {
            let (mut playlists_page, summaries) =
                query_playlist_route_items(db, &principal, &sort, search_term.as_deref())?;
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
    let covers = if inc.covers {
        Some(cover_services::display::playlists::covers_for_playlists(
            db,
            &principal,
            &page_playlists,
        )?)
    } else {
        None
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
        let cover = covers.as_ref().map(|covers| {
            covers
                .get(&playlist_db_id)
                .cloned()
                .map(route_covers::cover_to_response)
        });
        items.push(playlist_to_response(
            playlist,
            tracks,
            cover,
            Some(principal.user_public_id.clone()),
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
    let principal = require_principal(&headers).await?;
    let inc = parse_inc(query.inc)?;

    let db = &*STATE.db.read().await;
    let playlist_db_id = db::lookup::find_node_id_by_id(db, &id)?
        .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
    let playlist = db::playlists::get_by_id(db, playlist_db_id)?
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
        Some(
            cover_services::display::playlists::cover_for_playlist(db, &principal, &playlist)?
                .map(route_covers::cover_to_response),
        )
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
    let principal = require_principal(&headers).await?;

    let mut db = STATE.db.write().await;
    let playlist_db_id = require_owned_playlist(&db, &principal, &id)?;
    let playlist = playlists::update(
        &mut db,
        &playlists::UpdatePlaylistRequest {
            playlist_id: QueryId::Id(playlist_db_id),
            name: request.name,
            description: request.description,
            is_public: request.is_public,
            updated_at: Some(now_epoch()),
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
    let principal = require_principal(&headers).await?;

    let mut db = STATE.db.write().await;
    let playlist_db_id = require_owned_playlist(&db, &principal, &id)?;
    playlists::delete(&mut db, QueryId::Id(playlist_db_id))?
        .ok_or_else(|| AppError::not_found(format!("Playlist not found: {}", id)))?;

    Ok(StatusCode::NO_CONTENT)
}

async fn add_playlist_tracks(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<AddPlaylistTracksRequest>,
) -> Result<Json<Vec<PlaylistTrackResponse>>, AppError> {
    let principal = require_principal(&headers).await?;

    if request.track_ids.is_empty() {
        return Err(AppError::bad_request("track_ids cannot be empty"));
    }
    if request.track_ids.len() > PLAYLIST_BULK_TRACK_HARD_CAP {
        return Err(AppError::bad_request(format!(
            "track_ids cap exceeded: {} > {PLAYLIST_BULK_TRACK_HARD_CAP}",
            request.track_ids.len(),
        )));
    }

    let mut db = STATE.db.write().await;
    let playlist_db_id = require_owned_playlist(&db, &principal, &id)?;

    let mut track_query_ids = Vec::with_capacity(request.track_ids.len());
    for track_id in request.track_ids {
        let track_db_id = db::lookup::find_node_id_by_id(&*db, &track_id)?
            .ok_or_else(|| AppError::not_found(format!("track not found: {track_id}")))?;
        crate::services::auth::access::require_entity_accessible(
            &*db,
            &principal,
            track_db_id,
            || AppError::not_found(format!("track not found: {track_id}")),
        )?;
        track_query_ids.push(QueryId::Id(track_db_id));
    }

    let results = playlists::add_tracks(&mut db, QueryId::Id(playlist_db_id), &track_query_ids)
        .map_err(|err| {
            let message = err.to_string();
            if message.starts_with("track not found") {
                AppError::not_found(message)
            } else {
                AppError::from(err)
            }
        })?;

    let mut added = Vec::with_capacity(results.len());
    for playlist_track in results {
        let track = db::tracks::get_by_id(&db, playlist_track.track_db_id)?
            .ok_or_else(|| AppError::not_found("playlist track target missing"))?;
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
    let principal = require_principal(&headers).await?;

    if entry_ids.is_empty() {
        return Err(AppError::bad_request("entry_ids cannot be empty"));
    }
    if entry_ids.len() > PLAYLIST_BULK_TRACK_HARD_CAP {
        return Err(AppError::bad_request(format!(
            "entry_ids cap exceeded: {} > {PLAYLIST_BULK_TRACK_HARD_CAP}",
            entry_ids.len(),
        )));
    }

    let mut db = STATE.db.write().await;
    let playlist_db_id = require_owned_playlist(&db, &principal, &id)?;

    let removed_tracks =
        playlists::remove_tracks(&mut db, playlist_db_id, &entry_ids).map_err(|err| {
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
    let principal = require_principal(&headers).await?;

    let mut db = STATE.db.write().await;
    let playlist_db_id = require_owned_playlist(&db, &principal, &id)?;
    playlists::move_track(&mut db, playlist_db_id, &entry_id, request.new_position).map_err(
        |err| {
            let message = err.to_string();
            if message.starts_with("playlist entry not found") {
                AppError::not_found(message)
            } else {
                AppError::from(err)
            }
        },
    )?;

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
        .description("Returns a cursor-paginated page of playlists owned by the authenticated user. Track counts and durations are always included. Use `inc` to include tracks, artists, releases, covers.")
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
        .description("Deletes a playlist. Only the playlist owner can delete.")
        .response::<204, ()>()
}

#[cfg(feature = "docgen")]
fn add_tracks_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Add tracks to playlist")
        .description("Adds one or more tracks to the end of the playlist. Returns the added items without artists or release details.")
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
        .description("Moves a playlist track entry to a new position. `new_position` is a 0-based index into the list after the moved entry is removed. Returns the full updated item list without artists or release details.")
}

pub fn playlist_routes() -> Router {
    Router::new()
        .route("/", post(create_playlist))
        .route("/", get(get_playlists))
        .route("/{id}", get(get_playlist))
        .route("/{id}", patch(update_playlist))
        .route("/{id}", delete(delete_playlist))
        .route("/{id}/mix", get(super::mix::get_playlist_mix))
        .route("/{id}/tracks", post(add_playlist_tracks))
        .route("/{id}/tracks/remove", post(remove_playlist_tracks))
        .route("/{id}/tracks/{entry_id}", delete(delete_playlist_track))
        .route("/{id}/tracks/{entry_id}", patch(move_playlist_track))
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
            "/{id}/mix",
            get_with(super::mix::get_playlist_mix, super::mix::playlist_mix_docs),
        )
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

    use crate::db::test_db::{
        connect,
        insert_artist,
        insert_release,
        insert_track,
        new_test_db,
    };

    use super::*;

    fn create_test_user(db: &mut DbAny) -> anyhow::Result<DbId> {
        db::test_db::insert_user(db, "playlist-test-user")
    }

    fn test_principal(db: &mut DbAny) -> anyhow::Result<(DbId, Principal)> {
        let user_db_id = create_test_user(db)?;
        Ok((
            user_db_id,
            Principal::for_user(
                db,
                user_db_id,
                vec![db::Permission::Admin],
                std::collections::HashSet::new(),
            ),
        ))
    }

    fn seed_playlist(db: &mut DbAny, user_db_id: DbId, name: &str) -> anyhow::Result<DbId> {
        playlists::create(
            db,
            &playlists::CreatePlaylistRequest {
                user_db_id,
                name: name.to_string(),
                description: None,
                is_public: None,
                created_at: None,
                updated_at: None,
            },
        )
    }

    #[test]
    fn query_playlist_route_items_rejects_principal_whose_db_id_was_recycled() -> anyhow::Result<()>
    {
        let mut db = db::test_db::new_test_db()?;
        let (alice, stale) = test_principal(&mut db)?;
        let replacement = db::test_db::recycle_user(&mut db, alice, "replacement")?;
        seed_playlist(&mut db, replacement, "Replacement's Playlist")?;

        let error = query_playlist_route_items(&db, &stale, &default_playlist_sort(), None)
            .expect_err("a deleted user's principal must not list its replacement's playlists");
        assert!(matches!(
            error.downcast_ref::<crate::services::auth::AuthError>(),
            Some(crate::services::auth::AuthError::InvalidBearerCredential)
        ));
        Ok(())
    }

    async fn session_headers(user_db_id: DbId) -> anyhow::Result<HeaderMap> {
        let session = crate::testing::create_session(user_db_id, Default::default()).await?;
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
        crate::testing::init_default_test_state()?;
        let (attacker, victim, track_db_id, playlist_db_id, entry_ids, request_input) = {
            let mut db = STATE.db.write().await;
            db::roles::ensure_builtin_roles(&mut db)?;
            let attacker = db::test_db::insert_user(&mut db, "attacker")?;
            db::roles::ensure_user_has_role(&mut db, attacker, db::roles::BUILTIN_ADMIN_ROLE)?;
            let victim = db::test_db::insert_user(&mut db, "victim")?;
            let track_db_id = insert_track(&mut db, "Shared Track")?;
            let playlist_db_id = seed_playlist(&mut db, attacker, "Attacker's")?;
            let entry_ids = seed_entries(&mut db, playlist_db_id, track_db_id);
            let public_id = |db_id| {
                db::lookup::find_id_by_db_id(&*db, db_id)?
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
        let (reached, output) = crate::testing::run_with_recycle_at(
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
                Json(MovePlaylistTrackRequest { new_position: 1 }),
            )
            .await,
        )
    }

    /// Runs the recycle at every gap `request` has.
    async fn for_each_recycled_gap(
        request: impl AsyncFn(AttackerRequest) -> Result<(), StatusCode>,
        check: impl Fn(usize, &agdb::DbAny, &VictimPlaylist) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        crate::testing::for_each_db_gap(
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
        Ok(db::playlists::get_tracks(db, playlist_db_id)?
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
        let _guard = crate::testing::runtime_test_lock().await;
        let (_, control) = run_against_recycled_playlist(None, rename).await?;
        assert_eq!(control.output, Ok(()));
        let renamed = db::playlists::get_by_id(&*STATE.db.read().await, control.playlist_db_id)?
            .ok_or_else(|| anyhow!("control playlist missing"))?;
        assert_eq!(renamed.name, "Renamed");

        for_each_recycled_gap(rename, |gap, db, victim| {
            let victim_playlist = db::playlists::get_by_id(db, victim.db_id)?
                .ok_or_else(|| anyhow!("victim playlist missing after gap {gap}"))?;
            assert_eq!(victim_playlist.name, "Victim's", "renamed at gap {gap}");
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn delete_playlist_never_deletes_a_playlist_that_took_a_recycled_id() -> anyhow::Result<()>
    {
        let _guard = crate::testing::runtime_test_lock().await;
        let (_, control) = run_against_recycled_playlist(None, delete).await?;
        assert_eq!(control.output, Ok(()));
        assert!(
            db::playlists::get_by_id(&*STATE.db.read().await, control.playlist_db_id)?.is_none()
        );

        for_each_recycled_gap(delete, |gap, db, victim| {
            assert!(
                db::playlists::get_by_id(db, victim.db_id)?.is_some(),
                "deleted at gap {gap}"
            );
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn add_playlist_tracks_never_adds_to_a_playlist_that_took_a_recycled_id()
    -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
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
        let _guard = crate::testing::runtime_test_lock().await;
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
        let _guard = crate::testing::runtime_test_lock().await;
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
        let specs = parse_playlist_sort_specs(
            Some(vec!["track_count,name".to_string()]),
            Some("descending".to_string()),
        )
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert_eq!(specs.len(), 2);
        assert!(matches!(specs[0].key, PlaylistRouteSortKey::TrackCount));
        assert!(matches!(specs[1].key, PlaylistRouteSortKey::Name));
        assert!(matches!(specs[0].direction, SortDirection::Descending));

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
    fn query_playlist_route_items_sorts_by_track_count() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let (user_db_id, principal) = test_principal(&mut db)?;
        let empty = seed_playlist(&mut db, user_db_id, "Zebra")?;
        let filled = seed_playlist(&mut db, user_db_id, "Alpha")?;
        let track = insert_track(&mut db, "Track")?;
        playlists::add_tracks(&mut db, QueryId::Id(filled), &[QueryId::Id(track)])?;

        let sort = parse_playlist_sort_specs(
            Some(vec!["track_count".to_string()]),
            Some("descending".to_string()),
        )
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        let (ordered, summaries) = query_playlist_route_items(&db, &principal, &sort, None)?;
        assert_eq!(
            ordered.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(),
            ["Alpha", "Zebra"]
        );
        assert_eq!(summaries.get(&filled).map(|s| s.track_count), Some(1));

        // Default sort is by name, so the empty playlist comes second.
        let (ordered, summaries) =
            query_playlist_route_items(&db, &principal, &default_playlist_sort(), None)?;
        assert_eq!(
            ordered.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(),
            ["Alpha", "Zebra"]
        );
        assert!(
            summaries.is_empty(),
            "name sort should not pay for summaries"
        );
        assert_eq!(ordered.len(), 2);
        let _ = empty;

        Ok(())
    }

    #[test]
    fn query_playlist_route_items_filters_by_search_term() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let (user_db_id, principal) = test_principal(&mut db)?;
        seed_playlist(&mut db, user_db_id, "Late Night Jazz")?;
        seed_playlist(&mut db, user_db_id, "Morning Rock")?;

        let (ordered, _) =
            query_playlist_route_items(&db, &principal, &default_playlist_sort(), Some("jazz"))?;
        assert_eq!(
            ordered.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(),
            ["Late Night Jazz"]
        );

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
            created_at: Some(1),
            updated_at: Some(1),
        };
        let playlist_db_id = db::playlists::create(&mut db, &playlist, user_db_id)?;
        let pt =
            db.transaction_mut(|t| db::playlists::add_track(t, playlist_db_id, track_db_id))?;
        let track = db::tracks::get_by_id(&db, track_db_id)?
            .ok_or_else(|| anyhow!("track missing after insert"))?;

        let response = build_track_response_unchecked(
            &db,
            track,
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
