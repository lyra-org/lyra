// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbAny,
    DbId,
};
#[cfg(feature = "docgen")]
use aide::transform::TransformOperation;
use axum::{
    Json,
    body::Bytes,
    extract::{
        Path,
        Query,
    },
    http::{
        HeaderMap,
        HeaderValue,
        StatusCode,
        header,
    },
    response::{
        IntoResponse,
        Response,
    },
};
use axum::{
    Router,
    routing::{
        delete,
        get,
        post,
        put,
    },
};
use serde::{
    Deserialize,
    Serialize,
};
use url::form_urlencoded;

use crate::{
    STATE,
    db::{
        self,
        SortDirection,
        SortKey,
    },
    routes::AppError,
    routes::{
        covers as route_covers,
        deserialize_inc,
        responses::{
            LyricsLineResponse,
            LyricsResponse,
            LyricsWordResponse,
            PageResponse,
            ReleaseResponse,
            TrackResponse,
        },
    },
    services::{
        auth::{
            Principal,
            media_tokens::{
                MEDIA_TOKEN_IDLE_TTL_SECONDS,
                MediaTokenPurpose,
                issue_media_token,
            },
            require_authenticated,
            require_permission,
        },
        metadata::lyrics as lyrics_service,
        pagination::SnapshotKey,
        tracks as track_service,
    },
};
use std::{
    cmp::Ordering,
    collections::HashMap,
};

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct TrackQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Comma-separated or repeated values: releases, artists, release_covers, artist_covers."
        )
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    inc: Option<Vec<String>>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct TrackListQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Comma-separated or repeated values: releases, artists, release_covers, artist_covers."
        )
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    inc: Option<Vec<String>>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Optional fuzzy text query matched against track titles.")
    )]
    query: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Optional public library ID to scope returned tracks.")
    )]
    library_id: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Optional public release ID to scope returned tracks.")
    )]
    release_id: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Comma-separated or repeated values: sort_name, name, date_created, last_played_at, listen_count, duration, id. When release_id is present, disc and track are also supported."
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

pub(crate) struct TrackListOptions {
    inc: Option<Vec<String>>,
    query: Option<String>,
    library_id: Option<String>,
    release_id: Option<String>,
    sort_by: Option<Vec<String>>,
    sort_order: Option<String>,
    page_request: super::SnapshotPageRequest,
}

#[derive(Clone, Copy)]
pub(crate) struct TrackRouteIncludes {
    pub(crate) service: track_service::TrackIncludes,
    pub(crate) release_covers: bool,
    pub(crate) artist_covers: bool,
}

pub(crate) fn parse_inc(inc: Option<Vec<String>>) -> Result<TrackRouteIncludes, AppError> {
    let values = super::parse_inc_values(
        inc,
        &["releases", "artists", "release_covers", "artist_covers"],
    )?;
    let mut result = TrackRouteIncludes {
        service: track_service::TrackIncludes {
            releases: false,
            artists: false,
        },
        release_covers: false,
        artist_covers: false,
    };
    for value in values {
        match value.as_str() {
            "releases" => result.service.releases = true,
            "artists" => result.service.artists = true,
            "release_covers" => result.release_covers = true,
            "artist_covers" => result.artist_covers = true,
            _ => {}
        }
    }
    Ok(result)
}

#[derive(Clone, Copy, Debug)]
enum TrackRouteSortKey {
    Field(SortKey),
    ListenCount,
    LastPlayedAt,
}

type TrackRouteSortSpec = super::RouteSortSpec<TrackRouteSortKey>;

fn default_track_sort(release_scoped: bool) -> Vec<TrackRouteSortSpec> {
    if release_scoped {
        return vec![
            TrackRouteSortSpec {
                key: TrackRouteSortKey::Field(SortKey::DiscNumber),
                direction: SortDirection::Ascending,
            },
            TrackRouteSortSpec {
                key: TrackRouteSortKey::Field(SortKey::TrackNumber),
                direction: SortDirection::Ascending,
            },
            TrackRouteSortSpec {
                key: TrackRouteSortKey::Field(SortKey::SortName),
                direction: SortDirection::Ascending,
            },
            TrackRouteSortSpec {
                key: TrackRouteSortKey::Field(SortKey::DbId),
                direction: SortDirection::Ascending,
            },
        ];
    }

    vec![TrackRouteSortSpec {
        key: TrackRouteSortKey::Field(SortKey::SortName),
        direction: SortDirection::Ascending,
    }]
}

fn is_supported_track_sort_key(key: SortKey, release_scoped: bool) -> bool {
    match key {
        SortKey::SortName
        | SortKey::Name
        | SortKey::DateCreated
        | SortKey::Duration
        | SortKey::DbId => true,
        SortKey::DiscNumber | SortKey::TrackNumber => release_scoped,
        SortKey::ReleaseDate => false,
    }
}

fn track_sort_supported_values(release_scoped: bool) -> &'static str {
    if release_scoped {
        "sort_name, name, date_created, last_played_at, listen_count, disc, track, duration, id"
    } else {
        "sort_name, name, date_created, last_played_at, listen_count, duration, id"
    }
}

fn parse_track_sort_specs(
    sort_by: Option<Vec<String>>,
    sort_order: Option<String>,
    release_scoped: bool,
) -> Result<Vec<TrackRouteSortSpec>, AppError> {
    super::parse_route_sort_specs(
        sort_by,
        sort_order,
        |token| match token {
            "listen_count" => Some(TrackRouteSortKey::ListenCount),
            "last_played_at" => Some(TrackRouteSortKey::LastPlayedAt),
            _ => SortKey::from_token(token)
                .filter(|key| is_supported_track_sort_key(*key, release_scoped))
                .map(TrackRouteSortKey::Field),
        },
        track_sort_supported_values(release_scoped),
    )
}

fn resolve_optional_release_filter(
    db: &impl db::DbAccess,
    principal: &Principal,
    release_id: Option<&str>,
) -> Result<Option<DbId>, AppError> {
    let Some(release_id) = release_id else {
        return Ok(None);
    };
    let release_id = release_id.trim();
    if release_id.is_empty() {
        return Err(AppError::bad_request("release_id cannot be empty"));
    }

    let release_db_id = db::lookup::find_node_id_by_id(db, release_id)?
        .ok_or_else(|| AppError::not_found(format!("Release not found: {release_id}")))?;
    db::releases::get_by_id(db, release_db_id)?
        .ok_or_else(|| AppError::not_found(format!("Release not found: {release_id}")))?;
    super::require_entity_accessible(db, principal, release_db_id, || {
        AppError::not_found(format!("Release not found: {release_id}"))
    })?;
    Ok(Some(release_db_id))
}

fn release_belongs_to_library(
    db: &impl db::DbAccess,
    release_db_id: DbId,
    library_db_id: DbId,
) -> anyhow::Result<bool> {
    Ok(db::libraries::get_by_release(db, release_db_id)?
        .into_iter()
        .any(|library| library.db_id == Some(library_db_id)))
}

struct TrackRouteSortEntry {
    track: db::Track,
    lower_title: String,
    lower_sort_title: Option<String>,
    db_id: Option<i64>,
    date_created: Option<u64>,
    disc_number: Option<u32>,
    track_number: Option<u32>,
    duration: Option<u64>,
    listen_count: u64,
    last_played_at: Option<u64>,
    match_score: u32,
}

impl TrackRouteSortEntry {
    fn new(track: db::Track, listen_stats: Option<&db::listens::ListenStats>) -> Self {
        Self {
            lower_title: track.track_title.to_lowercase(),
            lower_sort_title: track.sort_title.as_ref().map(|value| value.to_lowercase()),
            db_id: track.db_id.as_ref().map(|id| DbId::from(id.clone()).0),
            date_created: track.ctime.or(track.created_at),
            disc_number: track.disc,
            track_number: track.track,
            duration: track.duration_ms,
            listen_count: listen_stats.map(|stats| stats.count).unwrap_or(0),
            last_played_at: listen_stats.and_then(|stats| stats.last_played),
            track,
            match_score: 0,
        }
    }
}

fn compare_track_route_field(
    a: &TrackRouteSortEntry,
    b: &TrackRouteSortEntry,
    key: TrackRouteSortKey,
) -> Ordering {
    match key {
        TrackRouteSortKey::Field(SortKey::SortName) => a
            .lower_sort_title
            .as_deref()
            .unwrap_or(a.lower_title.as_str())
            .cmp(
                b.lower_sort_title
                    .as_deref()
                    .unwrap_or(b.lower_title.as_str()),
            ),
        TrackRouteSortKey::Field(SortKey::Name) => a.lower_title.cmp(&b.lower_title),
        TrackRouteSortKey::Field(SortKey::DateCreated) => {
            db::compare_option(&a.date_created, &b.date_created)
        }
        TrackRouteSortKey::Field(SortKey::TrackNumber) => {
            db::compare_option(&a.track_number, &b.track_number)
        }
        TrackRouteSortKey::Field(SortKey::DiscNumber) => {
            a.disc_number.unwrap_or(1).cmp(&b.disc_number.unwrap_or(1))
        }
        TrackRouteSortKey::Field(SortKey::Duration) => db::compare_option(&a.duration, &b.duration),
        TrackRouteSortKey::Field(SortKey::DbId) => db::compare_option(&a.db_id, &b.db_id),
        TrackRouteSortKey::ListenCount => a.listen_count.cmp(&b.listen_count),
        TrackRouteSortKey::LastPlayedAt => db::compare_option(&a.last_played_at, &b.last_played_at),
        TrackRouteSortKey::Field(SortKey::ReleaseDate) => Ordering::Equal,
    }
}

fn compare_track_route_entries(
    a: &TrackRouteSortEntry,
    b: &TrackRouteSortEntry,
    sort: &[TrackRouteSortSpec],
) -> Ordering {
    for spec in sort {
        let ord = db::apply_direction(compare_track_route_field(a, b, spec.key), spec.direction);
        if ord != Ordering::Equal {
            return ord;
        }
    }

    b.match_score
        .cmp(&a.match_score)
        .then_with(|| a.lower_title.cmp(&b.lower_title))
        .then_with(|| db::compare_option(&a.db_id, &b.db_id))
}

fn track_sort_needs_listens(sort: &[TrackRouteSortSpec]) -> bool {
    sort.iter().any(|spec| {
        matches!(
            spec.key,
            TrackRouteSortKey::ListenCount | TrackRouteSortKey::LastPlayedAt
        )
    })
}

fn query_track_route_items(
    db: &DbAny,
    tracks: Vec<db::Track>,
    sort: &[TrackRouteSortSpec],
    search_term: Option<&str>,
    user_db_id: DbId,
) -> anyhow::Result<Vec<db::Track>> {
    let listen_stats: HashMap<DbId, db::listens::ListenStats> = if track_sort_needs_listens(sort) {
        let track_ids: Vec<DbId> = tracks
            .iter()
            .filter_map(|track| track.db_id.clone().map(DbId::from))
            .collect();
        db::listens::get_stats_for_user_tracks(db, &track_ids, user_db_id)?
            .into_iter()
            .map(|stats| (stats.db_id, stats))
            .collect()
    } else {
        HashMap::new()
    };
    let mut entries: Vec<TrackRouteSortEntry> = tracks
        .into_iter()
        .map(|track| {
            let track_db_id = track.db_id.clone().map(DbId::from);
            TrackRouteSortEntry::new(track, track_db_id.and_then(|id| listen_stats.get(&id)))
        })
        .collect();

    if let Some(term) = search_term {
        db::search::fuzzy_filter(
            &mut entries,
            term,
            |entry| entry.track.track_title.as_str(),
            |entry, score| entry.match_score = score,
        );
    }

    entries.sort_by(|a, b| compare_track_route_entries(a, b, sort));
    Ok(entries.into_iter().map(|entry| entry.track).collect())
}

fn release_to_response(
    db: &impl db::DbAccess,
    release: db::Release,
    include_cover: bool,
) -> anyhow::Result<ReleaseResponse> {
    let cover = if include_cover {
        match release.db_id.clone().map(DbId::from) {
            Some(release_db_id) => route_covers::build_cover_response(db, release_db_id, true)?,
            None => Some(None),
        }
    } else {
        None
    };
    let mut response = ReleaseResponse::from(release);
    response.cover = cover;
    Ok(response)
}

pub(crate) fn track_detail_to_response(
    db: &impl db::DbAccess,
    detail: track_service::TrackDetails,
    includes: TrackRouteIncludes,
) -> anyhow::Result<TrackResponse> {
    let artist_covers = match detail.artists.as_ref() {
        Some(artists) if includes.artist_covers => Some(db::covers::get_many(
            db,
            &super::db_ids_from_credited_artists(artists),
        )?),
        _ => None,
    };
    let releases = detail
        .releases
        .map(|releases| {
            releases
                .into_iter()
                .map(|release| release_to_response(db, release, includes.release_covers))
                .collect::<anyhow::Result<Vec<_>>>()
        })
        .transpose()?;
    Ok(TrackResponse {
        id: detail.track.id.clone(),
        title: detail.track.track_title,
        sort_title: detail.track.sort_title,
        year: detail.track.year,
        disc: detail.track.disc,
        disc_total: detail.track.disc_total,
        track: detail.track.track,
        track_total: detail.track.track_total,
        duration_ms: detail.track.duration_ms,
        releases,
        artists: detail
            .artists
            .map(|v| super::credited_artist_responses(v, artist_covers.as_ref())),
    })
}

pub(crate) async fn list_track_responses(
    principal: &Principal,
    options: TrackListOptions,
) -> Result<PageResponse<TrackResponse>, AppError> {
    let TrackListOptions {
        inc,
        query,
        library_id,
        release_id,
        sort_by,
        sort_order,
        page_request,
    } = options;
    let db = &*STATE.db.read().await;
    let includes = parse_inc(inc)?;
    let search_term = super::parse_text_query(query);
    let snapshot_key = SnapshotKey::builder(&principal.user_public_id, "tracks")
        .field(search_term.as_deref())
        .field(library_id.as_deref())
        .field(release_id.as_deref())
        .values(sort_by.as_deref())
        .field(sort_order.as_deref())
        .finish();
    let library_scope =
        super::resolve_optional_library_filter(db, principal, library_id.as_deref())?;
    let release_scope = resolve_optional_release_filter(db, principal, release_id.as_deref())?;
    let mut sort = parse_track_sort_specs(sort_by, sort_order, release_scope.is_some())?;
    if sort.is_empty() {
        sort = default_track_sort(release_scope.is_some());
    }
    let (tracks, next_cursor) = if let Some(page) = page_request.resume(&snapshot_key)? {
        let tracks = super::load_snapshot_items(
            db,
            &page.item_ids,
            db::tracks::get_by_id,
            |db, track_db_id| super::entity_accessible_to_principal(db, principal, track_db_id),
        )?;
        (tracks, page.next_cursor)
    } else {
        let accessible_tracks = match (release_scope, library_scope) {
            (Some(release_db_id), Some(library_db_id))
                if !release_belongs_to_library(db, release_db_id, library_db_id)? =>
            {
                Vec::new()
            }
            (Some(release_db_id), _) => db::tracks::get_by_releases(db, &[release_db_id])?,
            (None, Some(library_db_id)) => db::tracks::get_by_library(db, library_db_id)?,
            (None, None) => {
                let tracks = db::tracks::get(db, "tracks")?;
                let mut accessible_tracks = Vec::with_capacity(tracks.len());
                for track in tracks {
                    let Some(track_db_id) = track.db_id.clone().map(agdb::DbId::from) else {
                        continue;
                    };
                    if super::entity_accessible_to_principal(db, principal, track_db_id)? {
                        accessible_tracks.push(track);
                    }
                }
                accessible_tracks
            }
        };
        let mut tracks = query_track_route_items(
            db,
            accessible_tracks,
            &sort,
            search_term.as_deref(),
            principal.user_db_id,
        )?;
        let page = page_request.start(
            &snapshot_key,
            tracks.iter().map(|track| track.id.clone()).collect(),
        )?;
        tracks.truncate(page.item_ids.len());
        (tracks, page.next_cursor)
    };
    let details = track_service::list_details_for_tracks(db, includes.service, tracks)?;

    let mut items = Vec::with_capacity(details.len());
    for detail in details {
        items.push(track_detail_to_response(db, detail, includes)?);
    }
    Ok(PageResponse { items, next_cursor })
}

pub(crate) async fn get_track_response(
    principal: &Principal,
    id: String,
    inc: Option<Vec<String>>,
) -> Result<TrackResponse, AppError> {
    let db = &*STATE.db.read().await;
    let includes = parse_inc(inc)?;
    let track_db_id = db::lookup::find_node_id_by_id(db, &id)?
        .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
    super::require_entity_accessible(db, principal, track_db_id, || {
        AppError::not_found(format!("Track not found: {id}"))
    })?;
    let detail = track_service::get_details(db, track_db_id, includes.service)?
        .ok_or_else(|| AppError::not_found(format!("Track not found: {}", id)))?;

    Ok(track_detail_to_response(db, detail, includes)?)
}

fn add_optional_query_pair<T: ToString>(
    pairs: &mut Vec<(&'static str, String)>,
    key: &'static str,
    value: Option<T>,
) {
    if let Some(value) = value {
        pairs.push((key, value.to_string()));
    }
}

fn add_optional_string_query_pair(
    pairs: &mut Vec<(&'static str, String)>,
    key: &'static str,
    value: Option<&String>,
) {
    let Some(value) = value
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    else {
        return;
    };
    pairs.push((key, value.to_string()));
}

fn build_media_url(path: String, pairs: Vec<(&'static str, String)>) -> String {
    let mut serializer = form_urlencoded::Serializer::new(String::new());
    for (key, value) in pairs {
        serializer.append_pair(key, &value);
    }
    let query = serializer.finish();
    format!("{path}?{query}")
}

fn media_url_common_pairs(
    media_token: &str,
    query: &PlaybackUrlQuery,
) -> Vec<(&'static str, String)> {
    let mut pairs = vec![("media_token", media_token.to_string())];
    add_optional_query_pair(&mut pairs, "bitrate_bps", query.bitrate_bps);
    add_optional_query_pair(&mut pairs, "sample_rate_hz", query.sample_rate_hz);
    add_optional_query_pair(&mut pairs, "channels", query.channels);
    add_optional_query_pair(&mut pairs, "prefer_vbr", query.prefer_vbr);
    add_optional_query_pair(&mut pairs, "start_offset_ms", query.start_offset_ms);
    pairs
}

fn build_stream_url(id: &str, token: &str, query: &PlaybackUrlQuery) -> String {
    let mut pairs = media_url_common_pairs(token, query);
    add_optional_string_query_pair(&mut pairs, "format", query.format.as_ref());
    add_optional_string_query_pair(&mut pairs, "codec", query.codec.as_ref());
    build_media_url(format!("/api/stream/{id}"), pairs)
}

fn build_hls_url(id: &str, token: &str, query: &PlaybackUrlQuery) -> String {
    let mut pairs = media_url_common_pairs(token, query);
    add_optional_string_query_pair(&mut pairs, "codec", query.hls_codec.as_ref());
    build_media_url(format!("/api/stream/{id}/hls.m3u8"), pairs)
}

fn build_download_url(id: &str, token: &str, query: &PlaybackUrlQuery) -> String {
    let mut pairs = media_url_common_pairs(token, query);
    add_optional_string_query_pair(&mut pairs, "format", query.format.as_ref());
    add_optional_string_query_pair(&mut pairs, "codec", query.codec.as_ref());
    build_media_url(format!("/api/download/{id}"), pairs)
}

fn validate_playback_url_query(query: &PlaybackUrlQuery) -> Result<(), AppError> {
    if matches!(query.bitrate_bps, Some(0)) {
        return Err(AppError::bad_request(
            "bitrate_bps must be greater than zero",
        ));
    }
    if matches!(query.sample_rate_hz, Some(0)) {
        return Err(AppError::bad_request(
            "sample_rate_hz must be greater than zero",
        ));
    }
    if matches!(query.channels, Some(0)) {
        return Err(AppError::bad_request("channels must be greater than zero"));
    }

    Ok(())
}

fn validate_playback_url_output(
    query: &PlaybackUrlQuery,
    source: &super::serve::ValidatedTrackSource,
) -> Result<(), AppError> {
    let stream_request = super::serve::validate_request(query.format.clone(), query.codec.clone())?;
    let stream_format = super::serve::resolve_output_format(
        stream_request.format,
        &stream_request.preferred_codecs,
        source.entry_format,
        &source.full_path,
        true,
    )?;
    if !stream_format.supports_streaming() {
        return Err(AppError::bad_request(format!(
            "Format '{}' does not support streaming. Use /api/download or choose a streamable format (mp3, flac, wav, ogg, webm, aac, opus, aiff).",
            stream_format.extension()
        )));
    }

    super::serve::validate_request(None, query.hls_codec.clone())?;

    Ok(())
}

async fn create_track_playback_url(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<PlaybackUrlQuery>,
) -> Result<Json<PlaybackUrlResponse>, AppError> {
    validate_playback_url_query(&query)?;
    let principal = require_authenticated(&headers).await?;

    let track_db_id = {
        let db = &*STATE.db.read().await;
        let track_db_id = db::lookup::find_node_id_by_id(db, &id)?
            .ok_or_else(|| AppError::not_found(format!("Track not found: {id}")))?;
        super::require_entity_accessible(db, &principal, track_db_id, || {
            AppError::not_found(format!("Track not found: {id}"))
        })?;
        track_db_id
    };

    let source = super::serve::validate_and_get_track_source(track_db_id).await?;
    validate_playback_url_output(&query, &source)?;

    let stream_token = issue_media_token(track_db_id, MediaTokenPurpose::Stream);
    let hls_token = issue_media_token(track_db_id, MediaTokenPurpose::HlsPlaylist);
    let mut expires_at = stream_token.expires_at.min(hls_token.expires_at);
    let download_url = if require_permission(&principal, db::Permission::Download).is_ok() {
        let download_token = issue_media_token(track_db_id, MediaTokenPurpose::Download);
        expires_at = expires_at.min(download_token.expires_at);
        Some(build_download_url(&id, &download_token.token, &query))
    } else {
        None
    };

    Ok(Json(PlaybackUrlResponse {
        stream_url: build_stream_url(&id, &stream_token.token, &query),
        hls_url: build_hls_url(&id, &hls_token.token, &query),
        download_url,
        expires_at: super::unix_secs_to_rfc3339_i64(expires_at),
        idle_expires_after_seconds: MEDIA_TOKEN_IDLE_TTL_SECONDS,
    }))
}

async fn get_tracks(
    headers: HeaderMap,
    Query(list_query): Query<TrackListQuery>,
) -> Result<Json<PageResponse<TrackResponse>>, AppError> {
    let TrackListQuery {
        inc,
        query,
        library_id,
        release_id,
        sort_by,
        sort_order,
        page,
    } = list_query;
    let page = page.resolve_snapshot();
    let principal = require_authenticated(&headers).await?;
    Ok(Json(
        list_track_responses(
            &principal,
            TrackListOptions {
                inc,
                query,
                library_id,
                release_id,
                sort_by,
                sort_order,
                page_request: page,
            },
        )
        .await?,
    ))
}

async fn get_track(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<TrackQuery>,
) -> Result<Json<TrackResponse>, AppError> {
    let principal = require_authenticated(&headers).await?;
    Ok(Json(get_track_response(&principal, id, query.inc).await?))
}

#[cfg(feature = "docgen")]
fn list_tracks_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List tracks").description(
        "Returns tracks as `{ items, next_cursor }`. Supported query parameters: `inc`, `query`, `library_id`, `release_id`, `sort_by`, `sort_order`, `limit`, `cursor`. `library_id` scopes results to tracks belonging to that public library ID. `release_id` scopes results to one public release ID and defaults ordering to album order: disc, track, sort name, id. `sort_by` supports `sort_name`, `name`, `date_created`, `last_played_at`, `listen_count`, `duration`, and `id`; when `release_id` is present it also supports `disc` and `track`. `sort_order` supports `ascending` and `descending`. `limit` defaults to 100 and is capped at 500. Drive pagination from `next_cursor`; it is `null` on the last page. `query` is a fuzzy text match against track titles. Use `inc` to include releases and/or artists. When `inc=releases,release_covers`, nested release metadata includes a public cover image URL. When `inc=artists`, each artist carries a `credit` object with `type`, `detail`, and `source`; add `artist_covers` to include public artist image metadata. An artist may appear multiple times with different credits. Artists without direct track credits inherit from the release (`source: release`).",
    )
}

#[cfg(feature = "docgen")]
fn get_track_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get track by ID").description(
        "Returns a single track. 404 if not found. Use `inc` to include releases and/or artists. When `inc=releases,release_covers`, nested release metadata includes a public cover image URL. When `inc=artists`, each artist carries a `credit` object with `type`, `detail`, and `source`; add `artist_covers` to include public artist image metadata. An artist may appear multiple times with different credits. Artists without direct track credits inherit from the release (`source: release`).",
    )
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct LyricsQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Output format: `json` (default), `plain`, or `lrc`. `lrc` returns 406 when no stored candidate has synced content meeting the selector's coverage threshold, even if `json`/`plain` would succeed for the same track."
        )
    )]
    format: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Preferred language as ISO-639-2 (e.g. 'eng', 'jpn'). When no stored lyric matches this language, the server falls back to the best available lyric regardless of language; inspect `language` on the response to tell whether the preference was honoured."
        )
    )]
    language: Option<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct LyricsWriteQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Language for raw LRC and plain text uploads. Defaults to `und`.")
    )]
    language: Option<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct PlaybackUrlQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Optional stream output format (e.g. mp3, flac, wav, ogg, webm, aac, opus)."
        )
    )]
    format: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Optional ordered stream codec preferences (e.g. opus,aac or pcm_s24le,pcm_s16le)."
        )
    )]
    codec: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Optional ordered HLS codec preferences for `hls_url` (for example: copy,aac or aac,flac)."
        )
    )]
    hls_codec: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Target bitrate cap in bits per second. Applied to generated stream, HLS, and download URLs."
        )
    )]
    bitrate_bps: Option<u32>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Target sample rate in Hz.")
    )]
    sample_rate_hz: Option<u32>,
    #[cfg_attr(feature = "docgen", schemars(description = "Target channel count."))]
    channels: Option<u32>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Prefer VBR for lossy transcodes when the selected encoder supports it."
        )
    )]
    prefer_vbr: Option<bool>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Per-request playback start offset in milliseconds.")
    )]
    start_offset_ms: Option<u64>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct PlaybackUrlResponse {
    stream_url: String,
    hls_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    download_url: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Absolute media-token expiration as an RFC3339 timestamp.")
    )]
    expires_at: String,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Media tokens also expire after this many seconds without use.")
    )]
    idle_expires_after_seconds: u64,
}

async fn get_track_lyrics(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<LyricsQuery>,
) -> Result<Response, AppError> {
    let principal = require_authenticated(&headers).await?;
    let db = &*STATE.db.read().await;

    // Same 404 body whether the track or the lyrics are missing — hides which
    // stage failed from an authenticated caller trying to enumerate.
    let not_found = || AppError::not_found(format!("No lyrics for track: {id}"));

    let track_db_id = db::lookup::find_node_id_by_id(db, &id)?.ok_or_else(not_found)?;
    super::require_entity_accessible(db, &principal, track_db_id, not_found)?;
    let track = db::tracks::get_by_id(db, track_db_id)?.ok_or_else(not_found)?;

    let format = query
        .format
        .as_deref()
        .map(str::to_ascii_lowercase)
        .unwrap_or_else(|| "json".to_string());
    if !matches!(format.as_str(), "json" | "plain" | "lrc") {
        return Err(AppError::bad_request(format!(
            "Unsupported lyrics format: {format}. Supported: json, plain, lrc."
        )));
    }
    let require_synced = format == "lrc";

    let candidates = db::lyrics::get_for_track(db, track_db_id)?;
    let providers = db::providers::get(db)?;
    let language_hint = query.language.as_deref();

    let winner = lyrics_service::pick_preferred(
        &candidates,
        &providers,
        language_hint,
        track.duration_ms,
        require_synced,
    )
    .ok_or_else(|| {
        if require_synced {
            AppError::not_acceptable(format!(
                "LRC format requires synced lyrics; none available for track: {id}"
            ))
        } else {
            not_found()
        }
    })?;

    let Some(winner_db_id) = winner.db_id.clone().map(Into::into) else {
        return Err(not_found());
    };
    let detail = db::lyrics::get_detail(db, winner_db_id)?.ok_or_else(not_found)?;

    match format.as_str() {
        "json" => Ok(lyrics_response_json(detail).into_response()),
        "plain" => Ok(lyrics_response_plain(detail).into_response()),
        "lrc" => Ok(lyrics_response_lrc(detail).into_response()),
        _ => unreachable!("format validated above"),
    }
}

async fn put_track_lyrics(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<LyricsWriteQuery>,
    body: Bytes,
) -> Result<Json<LyricsResponse>, AppError> {
    let principal = require_authenticated(&headers).await?;
    let now = lyrics_service::now_ms().map_err(lyrics_upload_error_to_app_error)?;
    let content_type = request_content_type(&headers)?;
    if !matches!(
        content_type,
        "application/json" | "application/lrc" | "text/x-lrc" | "text/plain"
    ) {
        return Err(AppError::unsupported_media_type(format!(
            "unsupported lyrics Content-Type: {content_type}. Supported: application/json, application/lrc, text/x-lrc, text/plain"
        )));
    }
    let input = lyrics_service::input_from_upload(content_type, &body, query.language, now)
        .map_err(lyrics_upload_error_to_app_error)?;

    let mut db = STATE.db.write().await;
    let track_db_id = db::lookup::find_node_id_by_id(&*db, &id)?
        .ok_or_else(|| AppError::not_found(format!("Track not found: {id}")))?;
    super::require_entity_accessible(&*db, &principal, track_db_id, || {
        AppError::not_found(format!("Track not found: {id}"))
    })?;
    let detail = lyrics_service::upsert_user_lyrics(&mut db, &id, input)
        .map_err(lyrics_upload_error_to_app_error)?;

    Ok(lyrics_response_json(detail))
}

async fn delete_track_lyrics(
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<StatusCode, AppError> {
    let principal = require_authenticated(&headers).await?;
    let mut db = STATE.db.write().await;
    let track_db_id = db::lookup::find_node_id_by_id(&*db, &id)?
        .ok_or_else(|| AppError::not_found(format!("Track not found: {id}")))?;
    super::require_entity_accessible(&*db, &principal, track_db_id, || {
        AppError::not_found(format!("Track not found: {id}"))
    })?;
    lyrics_service::delete_user_lyrics_for_track(&mut db, &id)
        .map_err(lyrics_upload_error_to_app_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn refresh_track_lyrics(
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<StatusCode, AppError> {
    let principal = require_authenticated(&headers).await?;

    let track_db_id = {
        let db = &*STATE.db.read().await;
        let track_db_id = db::lookup::find_node_id_by_id(db, &id)?
            .ok_or_else(|| AppError::not_found(format!("No track: {id}")))?;
        super::require_entity_accessible(db, &principal, track_db_id, || {
            AppError::not_found(format!("No track: {id}"))
        })?;
        track_db_id
    };

    lyrics_service::providers::dispatch_for_track(track_db_id, true).await?;
    Ok(StatusCode::NO_CONTENT)
}

fn lyrics_upload_error_to_app_error(error: lyrics_service::LyricsUploadError) -> AppError {
    match error {
        lyrics_service::LyricsUploadError::BadRequest(message) => AppError::bad_request(message),
        lyrics_service::LyricsUploadError::NotFound(message) => AppError::not_found(message),
        lyrics_service::LyricsUploadError::Internal(error) => AppError::from(error),
    }
}

fn request_content_type(headers: &HeaderMap) -> Result<&str, AppError> {
    let raw = headers
        .get(header::CONTENT_TYPE)
        .ok_or_else(|| AppError::unsupported_media_type("missing Content-Type"))?
        .to_str()
        .map_err(|_| AppError::unsupported_media_type("invalid Content-Type"))?;
    Ok(raw.split(';').next().unwrap_or("").trim())
}

fn lyrics_response_json(detail: db::lyrics::LyricsDetail) -> Json<LyricsResponse> {
    let db::lyrics::LyricsDetail { lyrics, lines } = detail;
    let response_lines = lines
        .into_iter()
        .map(|line| LyricsLineResponse {
            ts_ms: line.line.ts_ms,
            text: line.line.text,
            words: line
                .words
                .into_iter()
                .map(|word| LyricsWordResponse {
                    ts_ms: word.ts_ms,
                    char_start: word.char_start,
                    char_end: word.char_end,
                })
                .collect(),
        })
        .collect();

    Json(LyricsResponse {
        id: lyrics.id,
        provider_id: lyrics.provider_id,
        language: lyrics.language,
        origin: lyrics.origin.into(),
        plain_text: lyrics.plain_text,
        has_word_cues: lyrics.has_word_cues,
        updated_at: super::unix_ms_to_rfc3339_u64(lyrics.updated_at),
        lines: response_lines,
    })
}

fn lyrics_response_plain(detail: db::lyrics::LyricsDetail) -> Response {
    let body = if !detail.lyrics.plain_text.is_empty() {
        detail.lyrics.plain_text
    } else {
        detail
            .lines
            .iter()
            .map(|line| line.line.text.as_str())
            .collect::<Vec<_>>()
            .join("\n")
    };
    plain_text_response("text/plain; charset=utf-8", body)
}

/// `[mm:ss.xx]` caps the minute field at 99; anything above is clamped so
/// a slipped bogus timestamp can't break the emitted LRC grammar.
const LRC_MAX_TS_MS: u64 = 99 * 60 * 1000 + 59 * 1000 + 990;

fn lyrics_response_lrc(detail: db::lyrics::LyricsDetail) -> Response {
    let mut body = String::new();
    for line in &detail.lines {
        // Skip untimed prologue lines — would render stacked at [00:00.00]
        // alongside real synced lines in every LRC client.
        if line.line.ts_ms == 0 {
            continue;
        }
        let ts_ms = line.line.ts_ms.min(LRC_MAX_TS_MS);
        let total_centis = ts_ms / 10;
        let minutes = total_centis / (60 * 100);
        let seconds = (total_centis / 100) % 60;
        let centis = total_centis % 100;
        body.push_str(&format!(
            "[{minutes:02}:{seconds:02}.{centis:02}]{text}\n",
            text = line.line.text,
        ));
    }
    plain_text_response("application/lrc; charset=utf-8", body)
}

fn plain_text_response(content_type: &'static str, body: String) -> Response {
    let mut response = (StatusCode::OK, body).into_response();
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    response
}

#[cfg(feature = "docgen")]
fn get_track_lyrics_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get track lyrics").description(
        "Returns the best-matching lyrics for a track, selected from all stored providers. \
         `?format=json|plain|lrc` controls the response format (default json). Use `?language=` to \
         prefer an ISO-639-2 language; when no stored lyric matches the requested language, \
         the server falls back to the best available lyric; the `language` field on the \
         response reveals what was actually served. `LyricsWordResponse.char_start` / \
         `char_end` are Unicode-scalar (code point) offsets into the line's `text`, not \
         byte offsets. Returns 404 when no lyrics are stored; 406 when `lrc` is requested \
         but no candidate has synced content meeting the selector's coverage threshold.",
    )
}

#[cfg(feature = "docgen")]
fn put_track_lyrics_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Write track lyrics").description(
        "Creates or replaces the authenticated user's global lyrics override for a track. \
         The request body is selected by `Content-Type`: `application/json` accepts structured \
         lyrics JSON, `application/lrc` and `text/x-lrc` accept raw LRC text, and `text/plain` \
         stores non-timestamped plain text. Raw uploads use `?language=` for the stored language, \
         defaulting to `und`. All formats \
         store `origin=user` and provider `user`, making the result preferred over plugin lyrics.",
    )
}

#[cfg(feature = "docgen")]
fn delete_track_lyrics_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Delete user track lyrics").description(
        "Deletes the user-authored lyrics override for a track. Plugin/provider lyrics are left \
         intact, so future reads may fall back to provider lyrics. Idempotent: returns 204 even \
         when the track has no user-authored lyrics.",
    ).response::<204, ()>()
}

#[cfg(feature = "docgen")]
fn refresh_track_lyrics_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Refresh track lyrics")
        .description(
            "Re-runs every registered lyrics provider for the track with `force_refresh=true`, \
         bypassing each provider's negative cache. Awaits all dispatches before returning. \
         Returns 204 once dispatch completes; the caller should then GET the lyrics to read \
         what was stored. 404 if the track does not exist.",
        )
        .response::<204, ()>()
}

#[cfg(feature = "docgen")]
fn create_track_playback_url_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Create playable track URLs").description(
        "Returns browser-friendly stream and HLS URLs containing scoped media tokens for the track. \
         The caller must authenticate with a bearer session token or API key and have access to the \
         track. Returned media tokens are limited to this track and endpoint purpose, expire after \
         a fixed maximum lifetime, and also expire after an idle period. `download_url` is included \
         only when the caller has download permission.",
    )
}

pub fn track_routes() -> Router {
    Router::new()
        .route("/", get(get_tracks))
        .route("/{id}", get(get_track))
        .route("/{id}/mix", get(super::mix::get_track_mix))
        .route("/{id}/playback-url", post(create_track_playback_url))
        .route("/{id}/lyrics", get(get_track_lyrics))
        .route("/{id}/lyrics", put(put_track_lyrics))
        .route("/{id}/lyrics", delete(delete_track_lyrics))
        .route("/{id}/lyrics/refresh", post(refresh_track_lyrics))
}

#[cfg(feature = "docgen")]
pub(crate) fn track_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::{
        delete_with,
        get_with,
        post_with,
        put_with,
    };

    aide::axum::ApiRouter::new()
        .api_route("/", get_with(get_tracks, list_tracks_docs))
        .api_route("/{id}", get_with(get_track, get_track_docs))
        .api_route(
            "/{id}/mix",
            get_with(super::mix::get_track_mix, super::mix::track_mix_docs),
        )
        .api_route(
            "/{id}/playback-url",
            post_with(create_track_playback_url, create_track_playback_url_docs),
        )
        .api_route(
            "/{id}/lyrics",
            get_with(get_track_lyrics, get_track_lyrics_docs),
        )
        .api_route(
            "/{id}/lyrics",
            put_with(put_track_lyrics, put_track_lyrics_docs),
        )
        .api_route(
            "/{id}/lyrics",
            delete_with(delete_track_lyrics, delete_track_lyrics_docs),
        )
        .api_route(
            "/{id}/lyrics/refresh",
            post_with(refresh_track_lyrics, refresh_track_lyrics_docs),
        )
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use agdb::{
        DbAny,
        DbId,
    };

    use super::*;
    use crate::{
        db::test_db::{
            connect,
            connect_artist,
            insert_artist,
            insert_library,
            insert_release,
            insert_track,
            new_test_db,
        },
        services::auth::Principal,
        testing::{
            LibraryFixtureConfig,
            initialize_runtime,
            runtime_test_lock,
        },
    };
    use nanoid::nanoid;

    async fn setup_route_test() -> anyhow::Result<()> {
        initialize_runtime(&LibraryFixtureConfig {
            directory: std::path::PathBuf::from("."),
            language: None,
            country: None,
        })
        .await
    }

    fn admin_principal(accessible_library_ids: HashSet<String>) -> Principal {
        Principal {
            user_db_id: DbId(1),
            user_public_id: "admin".to_string(),
            username: "admin".to_string(),
            permissions: vec![db::Permission::Admin],
            role_name: Some("admin".to_string()),
            accessible_library_ids,
        }
    }

    fn update_track_position(
        db: &mut DbAny,
        track_db_id: DbId,
        disc: Option<u32>,
        track_number: Option<u32>,
    ) -> anyhow::Result<()> {
        let mut track = db::tracks::get_by_id(db, track_db_id)?
            .ok_or_else(|| anyhow::anyhow!("track missing"))?;
        track.disc = disc;
        track.track = track_number;
        db::tracks::update(db, &track)
    }

    fn record_listen(
        db: &mut DbAny,
        user_db_id: DbId,
        track_db_id: DbId,
        listened_at_ms: u64,
    ) -> anyhow::Result<()> {
        let track = db::tracks::get_by_id(db, track_db_id)?
            .ok_or_else(|| anyhow::anyhow!("track missing"))?;
        let listen = db::Listen {
            db_id: None,
            id: nanoid!(),
            track_public_id: track.id,
            position_ms: 0,
            duration_ms: Some(180_000),
            activity_ms: 180_000,
            state: db::PlaybackState::Completed,
            listened_at_ms,
            created_at_ms: listened_at_ms,
        };
        let session = db::PlaybackSession {
            db_id: None,
            id: nanoid!(),
            position_ms: 0,
            duration_ms: Some(180_000),
            activity_ms: Some(180_000),
            last_position_ms: None,
            state: db::PlaybackState::Completed,
            listen_recorded: Some(true),
            updated_at_ms: listened_at_ms,
            created_at_ms: listened_at_ms,
        };
        db::listens::create_and_mark_recorded(db, &listen, track_db_id, user_db_id, &session)
    }

    fn insert_cover_for(db: &mut DbAny, owner_db_id: DbId) -> anyhow::Result<db::Cover> {
        db::covers::upsert(
            db,
            owner_db_id,
            db::Cover {
                db_id: None,
                id: nanoid!(),
                path: "/music/release/cover.jpg".to_string(),
                mime_type: "image/jpeg".to_string(),
                hash: "a".repeat(64),
                blurhash: Some("LKO2?U%2Tw=w]~RBVZRi};RPxuwH".to_string()),
            },
        )
    }

    #[test]
    fn parse_inc_accepts_nested_covers() -> anyhow::Result<()> {
        let includes = parse_inc(Some(vec![
            "artists,releases,release_covers,artist_covers".to_string(),
        ]))
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert!(includes.service.artists);
        assert!(includes.service.releases);
        assert!(includes.release_covers);
        assert!(includes.artist_covers);
        Ok(())
    }

    #[test]
    fn parse_track_sort_specs_accepts_global_supported_values() -> anyhow::Result<()> {
        let specs = match parse_track_sort_specs(
            Some(vec![
                "sort_name,name".to_string(),
                "date_created,last_played_at,listen_count,duration,id".to_string(),
            ]),
            Some("descending".to_string()),
            false,
        ) {
            Ok(specs) => specs,
            Err(_) => return Err(anyhow::anyhow!("expected valid track sort specs")),
        };

        assert_eq!(specs.len(), 7);
        assert!(matches!(
            specs[0].key,
            TrackRouteSortKey::Field(SortKey::SortName)
        ));
        assert!(matches!(
            specs[1].key,
            TrackRouteSortKey::Field(SortKey::Name)
        ));
        assert!(matches!(
            specs[2].key,
            TrackRouteSortKey::Field(SortKey::DateCreated)
        ));
        assert!(matches!(specs[3].key, TrackRouteSortKey::LastPlayedAt));
        assert!(matches!(specs[4].key, TrackRouteSortKey::ListenCount));
        assert!(matches!(
            specs[5].key,
            TrackRouteSortKey::Field(SortKey::Duration)
        ));
        assert!(matches!(
            specs[6].key,
            TrackRouteSortKey::Field(SortKey::DbId)
        ));
        assert!(
            specs
                .iter()
                .all(|spec| matches!(spec.direction, SortDirection::Descending))
        );
        Ok(())
    }

    #[test]
    fn query_track_route_items_sorts_by_listen_count() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = db::users::create(&mut db, &db::test_db::test_user("track-sort-user")?)?;
        let one_listen = insert_track(&mut db, "One Listen")?;
        let two_listens = insert_track(&mut db, "Two Listens")?;
        record_listen(&mut db, user_db_id, one_listen, 1_000)?;
        record_listen(&mut db, user_db_id, two_listens, 2_000)?;
        record_listen(&mut db, user_db_id, two_listens, 3_000)?;
        let tracks = vec![
            db::tracks::get_by_id(&db, one_listen)?.ok_or_else(|| anyhow::anyhow!("missing"))?,
            db::tracks::get_by_id(&db, two_listens)?.ok_or_else(|| anyhow::anyhow!("missing"))?,
        ];

        let tracks = query_track_route_items(
            &db,
            tracks,
            &[TrackRouteSortSpec {
                key: TrackRouteSortKey::ListenCount,
                direction: SortDirection::Descending,
            }],
            None,
            user_db_id,
        )?;

        let titles: Vec<String> = tracks.into_iter().map(|track| track.track_title).collect();
        assert_eq!(titles, vec!["Two Listens", "One Listen"]);
        Ok(())
    }

    #[test]
    fn parse_track_sort_specs_scopes_disc_and_track_to_release_filter() -> anyhow::Result<()> {
        assert!(parse_track_sort_specs(Some(vec!["disc,track".to_string()]), None, false).is_err());

        let specs = match parse_track_sort_specs(Some(vec!["disc,track".to_string()]), None, true) {
            Ok(specs) => specs,
            Err(_) => return Err(anyhow::anyhow!("expected release-scoped track sort specs")),
        };
        assert_eq!(specs.len(), 2);
        assert!(matches!(
            specs[0].key,
            TrackRouteSortKey::Field(SortKey::DiscNumber)
        ));
        assert!(matches!(
            specs[1].key,
            TrackRouteSortKey::Field(SortKey::TrackNumber)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn list_track_responses_scopes_by_library_id() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let (visible_library_id, hidden_library_id) = {
            let mut db = STATE.db.write().await;
            let visible_library =
                insert_library(&mut db, "Visible Tracks", "/tmp/lyra-visible-tracks")?;
            let hidden_library =
                insert_library(&mut db, "Hidden Tracks", "/tmp/lyra-hidden-tracks")?;
            let visible_release = insert_release(&mut db, "Visible Track Release")?;
            let hidden_release = insert_release(&mut db, "Hidden Track Release")?;
            let visible_track = insert_track(&mut db, "Visible Track")?;
            let hidden_track = insert_track(&mut db, "Hidden Track")?;
            connect(&mut db, visible_library, visible_release)?;
            connect(&mut db, visible_release, visible_track)?;
            connect(&mut db, hidden_library, hidden_release)?;
            connect(&mut db, hidden_release, hidden_track)?;

            let visible_library_id = db::libraries::get_by_id(&db, visible_library)?
                .ok_or_else(|| anyhow::anyhow!("visible library missing"))?
                .id;
            let hidden_library_id = db::libraries::get_by_id(&db, hidden_library)?
                .ok_or_else(|| anyhow::anyhow!("hidden library missing"))?
                .id;
            (visible_library_id, hidden_library_id)
        };
        let principal = admin_principal(HashSet::from([
            visible_library_id.clone(),
            hidden_library_id,
        ]));

        let page = list_track_responses(
            &principal,
            TrackListOptions {
                inc: None,
                query: None,
                library_id: Some(visible_library_id),
                release_id: None,
                sort_by: None,
                sort_order: None,
                page_request: super::super::SnapshotPageRequest::first_page(100),
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].title, "Visible Track");
        assert!(page.next_cursor.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn list_track_responses_hydrates_release_covers() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let cover_id = {
            let mut db = STATE.db.write().await;
            let release = insert_release(&mut db, "Covered Release")?;
            let track = insert_track(&mut db, "Covered Track")?;
            connect(&mut db, release, track)?;
            insert_cover_for(&mut db, release)?.id
        };
        let principal = admin_principal(HashSet::new());

        let page = list_track_responses(
            &principal,
            TrackListOptions {
                inc: Some(vec!["releases,release_covers".to_string()]),
                query: None,
                library_id: None,
                release_id: None,
                sort_by: None,
                sort_order: None,
                page_request: super::super::SnapshotPageRequest::first_page(100),
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert_eq!(page.items.len(), 1);
        let release = page.items[0]
            .releases
            .as_ref()
            .and_then(|releases| releases.first())
            .ok_or_else(|| anyhow::anyhow!("expected nested release"))?;
        let cover = release
            .cover
            .as_ref()
            .and_then(|cover| cover.as_ref())
            .ok_or_else(|| anyhow::anyhow!("expected nested release cover"))?;
        assert_eq!(cover.id, cover_id);
        assert_eq!(
            cover.url,
            format!("/api/covers/{}?v={}", cover.id, cover.hash)
        );
        Ok(())
    }

    #[tokio::test]
    async fn get_track_response_hydrates_release_covers() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let (track_public_id, cover_id) = {
            let mut db = STATE.db.write().await;
            let release = insert_release(&mut db, "Detail Covered Release")?;
            let track = insert_track(&mut db, "Detail Covered Track")?;
            connect(&mut db, release, track)?;
            let cover_id = insert_cover_for(&mut db, release)?.id;
            let track_public_id = db::tracks::get_by_id(&db, track)?
                .ok_or_else(|| anyhow::anyhow!("track missing"))?
                .id;
            (track_public_id, cover_id)
        };
        let principal = admin_principal(HashSet::new());

        let track = get_track_response(
            &principal,
            track_public_id,
            Some(vec!["releases,release_covers".to_string()]),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        let release = track
            .releases
            .as_ref()
            .and_then(|releases| releases.first())
            .ok_or_else(|| anyhow::anyhow!("expected nested release"))?;
        let cover = release
            .cover
            .as_ref()
            .and_then(|cover| cover.as_ref())
            .ok_or_else(|| anyhow::anyhow!("expected nested release cover"))?;
        assert_eq!(cover.id, cover_id);
        Ok(())
    }

    #[tokio::test]
    async fn get_track_response_hydrates_artist_covers() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let (track_public_id, cover_id) = {
            let mut db = STATE.db.write().await;
            let track = insert_track(&mut db, "Artist Covered Track")?;
            let artist = insert_artist(&mut db, "Covered Artist")?;
            connect_artist(&mut db, track, artist)?;
            let cover_id = insert_cover_for(&mut db, artist)?.id;
            let track_public_id = db::tracks::get_by_id(&db, track)?
                .ok_or_else(|| anyhow::anyhow!("track missing"))?
                .id;
            (track_public_id, cover_id)
        };
        let principal = admin_principal(HashSet::new());

        let track = get_track_response(
            &principal,
            track_public_id,
            Some(vec!["artists,artist_covers".to_string()]),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        let artist = track
            .artists
            .as_ref()
            .and_then(|artists| artists.first())
            .ok_or_else(|| anyhow::anyhow!("expected nested artist"))?;
        let cover = artist
            .cover
            .as_ref()
            .and_then(Option::as_ref)
            .ok_or_else(|| anyhow::anyhow!("expected nested artist cover"))?;
        assert_eq!(cover.id, cover_id);
        Ok(())
    }

    #[tokio::test]
    async fn list_track_responses_release_id_defaults_to_album_order() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let release_public_id = {
            let mut db = STATE.db.write().await;
            let release = insert_release(&mut db, "Ordered Release")?;
            let other_release = insert_release(&mut db, "Other Release")?;
            let disc_one_second = insert_track(&mut db, "Disc One Second")?;
            let disc_one_first = insert_track(&mut db, "Disc One First")?;
            let disc_two_first = insert_track(&mut db, "Disc Two First")?;
            let other_track = insert_track(&mut db, "Other Track")?;

            update_track_position(&mut db, disc_one_second, Some(1), Some(2))?;
            update_track_position(&mut db, disc_one_first, None, Some(1))?;
            update_track_position(&mut db, disc_two_first, Some(2), Some(1))?;
            update_track_position(&mut db, other_track, Some(1), Some(1))?;

            connect(&mut db, release, disc_one_second)?;
            connect(&mut db, release, disc_one_first)?;
            connect(&mut db, release, disc_two_first)?;
            connect(&mut db, other_release, other_track)?;

            db::releases::get_by_id(&db, release)?
                .ok_or_else(|| anyhow::anyhow!("release missing"))?
                .id
        };
        let principal = admin_principal(HashSet::new());

        let page = list_track_responses(
            &principal,
            TrackListOptions {
                inc: None,
                query: None,
                library_id: None,
                release_id: Some(release_public_id),
                sort_by: None,
                sort_order: None,
                page_request: super::super::SnapshotPageRequest::first_page(100),
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        let titles: Vec<String> = page.items.into_iter().map(|track| track.title).collect();
        assert_eq!(
            titles,
            vec!["Disc One First", "Disc One Second", "Disc Two First"]
        );
        assert!(page.next_cursor.is_none());
        Ok(())
    }

    #[test]
    fn build_stream_url_encodes_media_token_and_options() {
        let query = PlaybackUrlQuery {
            format: Some("mp3".to_string()),
            codec: Some("copy,mp3".to_string()),
            hls_codec: Some("aac".to_string()),
            bitrate_bps: Some(128_000),
            sample_rate_hz: Some(44_100),
            channels: Some(2),
            prefer_vbr: Some(true),
            start_offset_ms: Some(12_345),
        };

        let url = build_stream_url("track/id", "token value", &query);

        assert!(url.starts_with("/api/stream/track/id?"));
        assert!(url.contains("media_token=token+value"));
        assert!(url.contains("format=mp3"));
        assert!(url.contains("codec=copy%2Cmp3"));
        assert!(url.contains("bitrate_bps=128000"));
        assert!(url.contains("sample_rate_hz=44100"));
        assert!(url.contains("channels=2"));
        assert!(url.contains("prefer_vbr=true"));
        assert!(url.contains("start_offset_ms=12345"));
        assert!(!url.contains("hls_codec"));
    }
}

#[cfg(all(test, feature = "nightly"))]
mod benches {
    extern crate test;

    use agdb::{
        DbAny,
        DbId,
    };
    use nanoid::nanoid;
    use test::{
        Bencher,
        black_box,
    };

    use super::*;
    use crate::db::test_db::{
        insert_track,
        new_test_db,
        test_user,
    };

    struct TrackSortBench {
        db: DbAny,
        user_db_id: DbId,
        tracks: Vec<db::Track>,
    }

    fn record_listen(db: &mut DbAny, user_db_id: DbId, track_db_id: DbId, listened_at_ms: u64) {
        let track = db::tracks::get_by_id(db, track_db_id)
            .unwrap()
            .expect("track exists");
        let listen = db::Listen {
            db_id: None,
            id: nanoid!(),
            track_public_id: track.id,
            position_ms: 0,
            duration_ms: Some(180_000),
            activity_ms: 180_000,
            state: db::PlaybackState::Completed,
            listened_at_ms,
            created_at_ms: listened_at_ms,
        };
        let session = db::PlaybackSession {
            db_id: None,
            id: nanoid!(),
            position_ms: 0,
            duration_ms: Some(180_000),
            activity_ms: Some(180_000),
            last_position_ms: None,
            state: db::PlaybackState::Completed,
            listen_recorded: Some(true),
            updated_at_ms: listened_at_ms,
            created_at_ms: listened_at_ms,
        };
        db::listens::create_and_mark_recorded(db, &listen, track_db_id, user_db_id, &session)
            .unwrap();
    }

    fn seed_track_sort_bench(track_count: usize, listens_per_track: usize) -> TrackSortBench {
        let mut db = new_test_db().unwrap();
        let user_db_id =
            db::users::create(&mut db, &test_user("track-sort-bench").unwrap()).unwrap();
        let mut tracks = Vec::with_capacity(track_count);
        for i in 0..track_count {
            let track_db_id = insert_track(&mut db, &format!("Track {i:04}")).unwrap();
            for listen_idx in 0..listens_per_track {
                record_listen(
                    &mut db,
                    user_db_id,
                    track_db_id,
                    ((i * listens_per_track + listen_idx) as u64) * 1_000,
                );
            }
            tracks.push(
                db::tracks::get_by_id(&db, track_db_id)
                    .unwrap()
                    .expect("track exists"),
            );
        }

        TrackSortBench {
            db,
            user_db_id,
            tracks,
        }
    }

    #[bench]
    fn route_sort_tracks_sort_name_1000(b: &mut Bencher) {
        let setup = seed_track_sort_bench(1_000, 0);
        let sort = default_track_sort(false);
        b.iter(|| {
            query_track_route_items(
                &setup.db,
                black_box(setup.tracks.clone()),
                &sort,
                None,
                setup.user_db_id,
            )
            .unwrap()
        });
    }

    #[bench]
    fn route_sort_tracks_listen_count_1000_tracks_3000_listens(b: &mut Bencher) {
        let setup = seed_track_sort_bench(1_000, 3);
        let sort = vec![TrackRouteSortSpec {
            key: TrackRouteSortKey::ListenCount,
            direction: SortDirection::Descending,
        }];
        b.iter(|| {
            query_track_route_items(
                &setup.db,
                black_box(setup.tracks.clone()),
                &sort,
                None,
                setup.user_db_id,
            )
            .unwrap()
        });
    }
}
