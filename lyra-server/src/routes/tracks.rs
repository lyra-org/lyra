// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod lyrics;

use agdb::{
    DbAny,
    DbId,
};
#[cfg(feature = "docgen")]
use aide::transform::TransformOperation;
use axum::{
    Json,
    extract::{
        Path,
        Query,
    },
    http::HeaderMap,
};
use axum::{
    Router,
    routing::{
        get,
        post,
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
        ratings::RatingFilterQuery,
        responses::{
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
    rating: RatingFilterQuery,
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
    rating_filter: db::ratings::RatingFilter,
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
    crate::services::auth::access::require_entity_accessible(db, principal, release_db_id, || {
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
        rating_filter,
        page_request,
    } = options;
    let db = &*STATE.db.read().await;
    let includes = parse_inc(inc)?;
    let search_term = super::parse_text_query(query);
    let (min_rating, max_rating) = rating_filter.bounds();
    let min_rating_context = min_rating.map(|value| value.to_string());
    let max_rating_context = max_rating.map(|value| value.to_string());
    let snapshot_key = SnapshotKey::builder(&principal.user_public_id, "tracks")
        .field(search_term.as_deref())
        .field(library_id.as_deref())
        .field(release_id.as_deref())
        .values(sort_by.as_deref())
        .field(sort_order.as_deref())
        .field(min_rating_context.as_deref())
        .field(max_rating_context.as_deref())
        .finish();
    let library_scope = crate::services::auth::access::resolve_optional_library_filter(
        db,
        principal,
        library_id.as_deref(),
    )?;
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
            |db, track_db_id| {
                crate::services::auth::access::entity_accessible(db, principal, track_db_id)
            },
        )?;
        (tracks, page.next_cursor)
    } else {
        let mut accessible_tracks = match (release_scope, library_scope) {
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
                    if crate::services::auth::access::entity_accessible(db, principal, track_db_id)?
                    {
                        accessible_tracks.push(track);
                    }
                }
                accessible_tracks
            }
        };
        if !rating_filter.is_empty() {
            let rated_target_ids =
                db::ratings::target_ids_matching(db, principal.user_db_id, rating_filter)?;
            accessible_tracks.retain(|track| {
                track
                    .db_id
                    .clone()
                    .map(DbId::from)
                    .is_some_and(|db_id| rated_target_ids.contains(&db_id))
            });
        }
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
    crate::services::auth::access::require_entity_accessible(db, principal, track_db_id, || {
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

fn playback_transcode_knobs(query: &PlaybackUrlQuery) -> super::serve::TranscodeKnobs {
    super::serve::TranscodeKnobs {
        bitrate_bps: query.bitrate_bps,
        sample_rate_hz: query.sample_rate_hz,
        channels: query.channels,
        prefer_vbr: query.prefer_vbr,
    }
}

// Runs before `require_authenticated`, so zero-value rejections stay 400 instead of
// becoming 401. The decision domain remains the single authority for the validation.
fn validate_playback_url_query(query: &PlaybackUrlQuery) -> Result<(), AppError> {
    playback_transcode_knobs(query).validate().map(|_| ())
}

/// Which transports can serve this request.
///
/// HLS is not represented because it always survives: the playlist handler
/// transcodes whatever the source is, and a codec it cannot honor is rejected
/// here as a request error rather than as an absent transport.
#[derive(Debug)]
struct PlaybackTransports {
    stream: bool,
}

/// Resolves the request once against the source and reports which transports can
/// carry it.
///
/// Request-level failures — unparseable or incompatible `format`/`codec`/
/// `hls_codec`, a start offset past the source, a codec that cannot produce the
/// requested container — fail the whole request. Only transport *capability*
/// outcomes, such as a container that cannot be streamed, remove a transport.
fn resolve_playback_transports(
    query: &PlaybackUrlQuery,
    source: super::serve::ValidatedTrackSource,
) -> Result<PlaybackTransports, AppError> {
    let stream_request = super::serve::validate_request(query.format.clone(), query.codec.clone())?;
    let source = super::serve::apply_request_start_offset(source, query.start_offset_ms)?;
    let knobs = playback_transcode_knobs(query);

    // `Download` here means "no transport constraint": it resolves the request without
    // rejecting containers that only /api/download can carry, so streamability becomes an
    // availability answer instead of a whole-request failure. Every other way the request
    // can fail still fails it, for every transport.
    let decision = super::serve::resolve_delivery(
        &stream_request,
        &source,
        knobs,
        super::serve::DeliveryTarget::Download,
    )?;

    // Parsed last so an unusable hls_codec never outranks a stream-request error, then
    // checked against the real profile so an hls_url is only minted when the playlist
    // handler would answer the same way.
    let hls_request = super::serve::validate_request(None, query.hls_codec.clone())?;
    super::serve::resolve_hls_profile(&source, &hls_request, knobs)?;

    Ok(PlaybackTransports {
        stream: decision.output_format.supports_streaming(),
    })
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
        crate::services::auth::access::require_entity_accessible(
            db,
            &principal,
            track_db_id,
            || AppError::not_found(format!("Track not found: {id}")),
        )?;
        track_db_id
    };

    let source = super::serve::validate_and_get_track_source(track_db_id).await?;
    let transports = resolve_playback_transports(&query, source)?;

    // HLS always survives, which is what keeps `expires_at` total and the response
    // non-empty; tokens for the other transports are only minted when they are usable.
    let hls_token = issue_media_token(track_db_id, MediaTokenPurpose::HlsPlaylist);
    let mut expires_at = hls_token.expires_at;
    let hls_url = build_hls_url(&id, &hls_token.token, &query);

    let stream_url = if transports.stream {
        let stream_token = issue_media_token(track_db_id, MediaTokenPurpose::Stream);
        expires_at = expires_at.min(stream_token.expires_at);
        Some(build_stream_url(&id, &stream_token.token, &query))
    } else {
        None
    };

    let download_url = if require_permission(&principal, db::Permission::Download).is_ok() {
        let download_token = issue_media_token(track_db_id, MediaTokenPurpose::Download);
        expires_at = expires_at.min(download_token.expires_at);
        Some(build_download_url(&id, &download_token.token, &query))
    } else {
        None
    };

    Ok(Json(PlaybackUrlResponse {
        stream_url,
        hls_url,
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
        rating,
        page,
    } = list_query;
    let page = page.resolve_snapshot();
    let principal = require_authenticated(&headers).await?;
    let rating_filter = rating.parse()?;
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
                rating_filter,
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
        "Returns tracks as `{ items, next_cursor }`. Supported query parameters: `inc`, `query`, `library_id`, `release_id`, `sort_by`, `sort_order`, `min_rating`, `max_rating`, `limit`, `cursor`. `min_rating` and `max_rating` filter tracks by the authenticated user's inclusive personal rating range; either bound excludes unrated tracks. `library_id` scopes results to tracks belonging to that public library ID. `release_id` scopes results to one public release ID and defaults ordering to album order: disc, track, sort name, id. `sort_by` supports `sort_name`, `name`, `date_created`, `last_played_at`, `listen_count`, `duration`, and `id`; when `release_id` is present it also supports `disc` and `track`. `sort_order` supports `ascending` and `descending`. `limit` defaults to 100 and is capped at 500. Drive pagination from `next_cursor`; it is `null` on the last page. `query` is a fuzzy text match against track titles. Use `inc` to include releases and/or artists. When `inc=releases,release_covers`, nested release metadata includes a public cover image URL. When `inc=artists`, each artist carries a `credit` object with `type`, `detail`, and `source`; add `artist_covers` to include public artist image metadata. An artist may appear multiple times with different credits. Artists without direct track credits inherit from the release (`source: release`).",
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
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Absent when the resolved container cannot be streamed (m4a, alac, caf)."
        )
    )]
    stream_url: Option<String>,
    hls_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Absent when the caller lacks download permission.")
    )]
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

#[cfg(feature = "docgen")]
fn create_track_playback_url_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Create playable track URLs").description(
        "Returns browser-friendly stream and HLS URLs containing scoped media tokens for the track. \
         The caller must authenticate with a bearer session token or API key and have access to the \
         track. Returned media tokens are limited to this track and endpoint purpose, expire after \
         a fixed maximum lifetime, and also expire after an idle period.\n\n\
         The request is resolved once against the track's source, and a transport is omitted when \
         it cannot carry the result. `stream_url` is omitted when the final resolved container does \
         not support streaming (m4a, alac, caf); use `hls_url` or `download_url` instead. \
         `download_url` is omitted when the caller lacks download permission. `hls_url` is always \
         present: HLS transcodes any source, and a track whose duration is not yet known returns a \
         retryable 503 from the playlist endpoint rather than being omitted here. A token is minted \
         only for a transport that is present, and `expires_at` is the earliest expiry among them.\n\n\
         Problems with the request itself are still errors rather than omissions: an unsupported or \
         unparseable `format`, `codec`, or `hls_codec`, a zero-valued knob, a `start_offset_ms` past \
         the end of the source, and a `codec` that cannot produce the requested `format` all return \
         400.",
    )
}

pub fn track_routes() -> Router {
    Router::new()
        .route("/", get(get_tracks))
        .route("/{id}", get(get_track))
        .route("/{id}/mix", get(super::mix::get_track_mix))
        .route("/{id}/playback-url", post(create_track_playback_url))
        .merge(lyrics::routes())
}

#[cfg(feature = "docgen")]
pub(crate) fn track_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::{
        get_with,
        post_with,
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
        .merge(lyrics::openapi_routes())
}

#[cfg(test)]
async fn setup_route_test() -> anyhow::Result<()> {
    crate::testing::initialize_runtime(&crate::testing::LibraryFixtureConfig {
        directory: std::path::PathBuf::from("."),
        language: None,
        country: None,
    })
    .await
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
        testing::runtime_test_lock,
    };
    use nanoid::nanoid;

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
            client_name: None,
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
        db.transaction_mut(|t| {
            db::covers::upsert(
                t,
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
        })
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
                rating_filter: db::ratings::RatingFilter::default(),
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
    async fn list_track_responses_filters_personal_ratings_and_binds_cursor() -> anyhow::Result<()>
    {
        use axum::{
            http::StatusCode,
            response::IntoResponse,
        };

        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let (principal, four_star_id, five_star_id) = {
            let mut db = STATE.db.write().await;
            let user_db_id =
                db::users::create(&mut db, &db::test_db::test_user("track-rating-user")?)?;
            let other_user_db_id =
                db::users::create(&mut db, &db::test_db::test_user("other-track-rating-user")?)?;
            let visible_library =
                insert_library(&mut db, "Rated Tracks", "/tmp/lyra-rated-tracks")?;
            let hidden_library = insert_library(
                &mut db,
                "Hidden Rated Tracks",
                "/tmp/lyra-hidden-rated-tracks",
            )?;
            let visible_release = insert_release(&mut db, "Rated Track Release")?;
            let hidden_release = insert_release(&mut db, "Hidden Rated Track Release")?;
            let four_star = insert_track(&mut db, "Four Star")?;
            let five_star = insert_track(&mut db, "Five Star")?;
            let low_rating = insert_track(&mut db, "Low Rating")?;
            let unrated = insert_track(&mut db, "Unrated")?;
            let other_user_rating = insert_track(&mut db, "Other User Rating")?;
            let hidden_rating = insert_track(&mut db, "Hidden Rating")?;

            connect(&mut db, visible_library, visible_release)?;
            for track in [four_star, five_star, low_rating, unrated, other_user_rating] {
                connect(&mut db, visible_release, track)?;
            }
            connect(&mut db, hidden_library, hidden_release)?;
            connect(&mut db, hidden_release, hidden_rating)?;

            for (target, value) in [
                (four_star, 4),
                (five_star, 5),
                (low_rating, 3),
                (hidden_rating, 5),
            ] {
                db::ratings::upsert(
                    &mut *db,
                    user_db_id,
                    target,
                    db::ratings::RatingKind::Track,
                    db::ratings::RatingValue::new(value).unwrap(),
                    1,
                )?;
            }
            db::ratings::upsert(
                &mut *db,
                other_user_db_id,
                other_user_rating,
                db::ratings::RatingKind::Track,
                db::ratings::RatingValue::new(5).unwrap(),
                1,
            )?;

            let visible_library_id = db::libraries::get_by_id(&db, visible_library)?
                .ok_or_else(|| anyhow::anyhow!("visible library missing"))?
                .id;
            let principal = Principal {
                user_db_id,
                user_public_id: "track-rating-user".to_string(),
                username: "track-rating-user".to_string(),
                permissions: Vec::new(),
                role_name: Some("user".to_string()),
                accessible_library_ids: HashSet::from([visible_library_id]),
            };
            let four_star_id = db::tracks::get_by_id(&db, four_star)?.unwrap().id;
            let five_star_id = db::tracks::get_by_id(&db, five_star)?.unwrap().id;
            (principal, four_star_id, five_star_id)
        };
        let rating_filter = db::ratings::RatingFilter::new(
            db::ratings::RatingValue::new(4),
            db::ratings::RatingValue::new(5),
        )
        .unwrap();

        let first_page = list_track_responses(
            &principal,
            TrackListOptions {
                inc: None,
                query: None,
                library_id: None,
                release_id: None,
                sort_by: None,
                sort_order: None,
                rating_filter,
                page_request: super::super::PageQuery {
                    limit: Some(1),
                    cursor: None,
                }
                .resolve_snapshot(),
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        let cursor = first_page
            .next_cursor
            .clone()
            .ok_or_else(|| anyhow::anyhow!("expected another rated track page"))?;
        let second_page = list_track_responses(
            &principal,
            TrackListOptions {
                inc: None,
                query: None,
                library_id: None,
                release_id: None,
                sort_by: None,
                sort_order: None,
                rating_filter,
                page_request: super::super::PageQuery {
                    limit: Some(1),
                    cursor: Some(cursor.clone()),
                }
                .resolve_snapshot(),
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        let returned_ids: HashSet<String> = first_page
            .items
            .into_iter()
            .chain(second_page.items)
            .map(|track| track.id)
            .collect();
        assert_eq!(
            returned_ids,
            HashSet::from([four_star_id, five_star_id.clone()])
        );

        let exact_five = db::ratings::RatingFilter::new(
            db::ratings::RatingValue::new(5),
            db::ratings::RatingValue::new(5),
        )
        .unwrap();
        let mismatch = match list_track_responses(
            &principal,
            TrackListOptions {
                inc: None,
                query: None,
                library_id: None,
                release_id: None,
                sort_by: None,
                sort_order: None,
                rating_filter: exact_five,
                page_request: super::super::PageQuery {
                    limit: Some(1),
                    cursor: Some(cursor),
                }
                .resolve_snapshot(),
            },
        )
        .await
        {
            Ok(_) => return Err(anyhow::anyhow!("changed rating bounds accepted the cursor")),
            Err(err) => err.into_response(),
        };
        assert_eq!(mismatch.status(), StatusCode::CONFLICT);

        let exact_page = list_track_responses(
            &principal,
            TrackListOptions {
                inc: None,
                query: None,
                library_id: None,
                release_id: None,
                sort_by: None,
                sort_order: None,
                rating_filter: exact_five,
                page_request: super::super::SnapshotPageRequest::first_page(100),
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert_eq!(
            exact_page
                .items
                .into_iter()
                .map(|track| track.id)
                .collect::<Vec<_>>(),
            vec![five_star_id],
        );
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
                rating_filter: db::ratings::RatingFilter::default(),
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
                rating_filter: db::ratings::RatingFilter::default(),
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

    fn playback_url_query(
        format: Option<&str>,
        codec: Option<&str>,
        hls_codec: Option<&str>,
    ) -> PlaybackUrlQuery {
        PlaybackUrlQuery {
            format: format.map(str::to_string),
            codec: codec.map(str::to_string),
            hls_codec: hls_codec.map(str::to_string),
            bitrate_bps: None,
            sample_rate_hz: None,
            channels: None,
            prefer_vbr: None,
            start_offset_ms: None,
        }
    }

    fn flac_playback_source() -> crate::routes::serve::ValidatedTrackSource {
        crate::routes::serve::ValidatedTrackSource {
            source_id: DbId(2),
            source_public_id: "source-pub".to_string(),
            track_public_id: "track-pub".to_string(),
            input_path: "track.flac".to_string(),
            entry_format: Some(lyra_ffmpeg::AudioFormat::Flac),
            source_codec: Some(lyra_ffmpeg::AudioCodec::Flac),
            full_path: std::path::PathBuf::from("track.flac"),
            duration_ms: Some(20_000),
            start_ms: None,
            end_ms: None,
            source_bitrate_bps: Some(900_000),
            source_sample_rate_hz: Some(96_000),
            source_channels: Some(2),
        }
    }

    #[test]
    fn resolve_playback_transports_accepts_a_resolvable_stream_request() {
        let transports = resolve_playback_transports(
            &playback_url_query(Some("mp3"), None, None),
            flac_playback_source(),
        )
        .expect("a plain mp3 transcode request stays valid");
        assert!(transports.stream);
    }

    #[test]
    fn resolve_playback_transports_rejects_copy_that_cannot_produce_the_requested_format() {
        // Deliberate widening: this used to pass validation and hand back a stream_url that
        // failed with this exact status and message on the first GET. Fail fast instead.
        let err = resolve_playback_transports(
            &playback_url_query(Some("mp3"), Some("copy"), None),
            flac_playback_source(),
        )
        .expect_err("copy cannot produce mp3 from a flac source");
        assert_eq!(
            format!("{err:?}"),
            "AppError(400 Bad Request: Requested codecs [copy] are not compatible with \
             format 'mp3'. Supported codecs: [mp3])"
        );
    }

    #[test]
    fn resolve_playback_transports_omits_stream_for_non_streamable_formats() {
        // Previously a whole-request 400. A non-streamable container is a transport
        // capability answer, not a bad request: hls and download still work.
        let transports = resolve_playback_transports(
            &playback_url_query(Some("m4a"), None, None),
            flac_playback_source(),
        )
        .expect("m4a is downloadable even though it cannot be streamed");
        assert!(!transports.stream);
    }

    #[test]
    fn resolve_playback_transports_omits_stream_for_non_streamable_sources() {
        // The live bug: with no query parameters at all, an m4a track used to 400 outright.
        let mut source = flac_playback_source();
        source.entry_format = Some(lyra_ffmpeg::AudioFormat::M4a);
        source.source_codec = Some(lyra_ffmpeg::AudioCodec::Alac);
        source.input_path = "track.m4a".to_string();
        source.full_path = std::path::PathBuf::from("track.m4a");

        let transports = resolve_playback_transports(&playback_url_query(None, None, None), source)
            .expect("an m4a source must still yield hls and download urls");
        assert!(!transports.stream);
    }

    #[test]
    fn resolve_playback_transports_answers_the_question_the_stream_endpoint_asks() {
        // With `codec=copy,<x>` and a surviving knob, the provisional copy container and
        // final transcode container differ. The minted URL must follow the final answer.
        let downmix = |codec: &str| PlaybackUrlQuery {
            channels: Some(1),
            ..playback_url_query(None, Some(codec), None)
        };

        let mut m4a = flac_playback_source();
        m4a.entry_format = Some(lyra_ffmpeg::AudioFormat::M4a);
        m4a.source_codec = Some(lyra_ffmpeg::AudioCodec::Alac);
        let transports = resolve_playback_transports(&downmix("copy,mp3"), m4a)
            .expect("the request is fine for download");
        assert!(
            transports.stream,
            "the final mp3 container is streamable, so a stream_url should be minted"
        );

        let mut mp3 = flac_playback_source();
        mp3.entry_format = Some(lyra_ffmpeg::AudioFormat::Mp3);
        mp3.source_codec = Some(lyra_ffmpeg::AudioCodec::Mp3);
        let transports = resolve_playback_transports(&downmix("copy,aac"), mp3)
            .expect("the request is fine for download");
        assert!(
            !transports.stream,
            "the final m4a container is not streamable, so no stream_url should be minted"
        );
    }

    #[test]
    fn resolve_playback_transports_rejects_an_hls_codec_the_playlist_cannot_honor() {
        // opus parses as a codec name but has no HLS profile, so the minted hls_url would
        // have failed at GET time with this same message.
        let err = resolve_playback_transports(
            &playback_url_query(None, None, Some("opus")),
            flac_playback_source(),
        )
        .expect_err("opus is not an HLS codec");
        assert_eq!(
            format!("{err:?}"),
            "AppError(400 Bad Request: Unsupported HLS codec. Supported values: copy, aac, alac, flac.)"
        );
    }

    #[test]
    fn resolve_playback_transports_accepts_hls_copy_for_a_copyable_source() {
        let transports = resolve_playback_transports(
            &playback_url_query(None, None, Some("copy")),
            flac_playback_source(),
        )
        .expect("a flac source can be segmented by stream copy");
        assert!(transports.stream);
    }

    #[test]
    fn resolve_playback_transports_reports_stream_request_errors_before_hls_codec_errors() {
        let err = resolve_playback_transports(
            &playback_url_query(Some("mp3"), Some("copy"), Some("opus")),
            flac_playback_source(),
        )
        .expect_err("both are broken");
        assert!(
            format!("{err:?}").contains("are not compatible with format 'mp3'"),
            "the stream request error must keep outranking the hls codec error"
        );
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
            client_name: None,
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
