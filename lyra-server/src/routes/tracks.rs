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

use crate::routes::request::bearer_credential;
use crate::services::delivery::{
    self,
    DeliveryDescription,
};

use crate::{
    routes::AppError,
    routes::{
        PersonalFilterQuery,
        covers as route_covers,
        deserialize_inc,
        responses::{
            EntryResponse,
            PageResponse,
            ReleaseResponse,
            TrackChains,
            TrackResponse,
        },
    },
    services::STATE,
    services::{
        auth::{
            Principal,
            access::TrackPurpose,
            media_tokens::{
                DeliveryOptions,
                MEDIA_TOKEN_IDLE_TTL_SECONDS,
                issue_media_token,
            },
            require_auth,
            require_authenticated,
        },
        catalog::{
            self,
            tracks::{
                TrackFilter,
                Tracks,
            },
        },
        covers::{
            ResolvedCover,
            shown::CoverTarget,
        },
        entries::{
            self as entry_service,
            PlacedEntry,
        },
        tracks as track_service,
    },
};
use std::collections::HashMap;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct TrackQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Comma-separated or repeated values: releases, artists, entries, release_covers, artist_covers."
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
            description = "Comma-separated or repeated values: releases, artists, entries, release_covers, artist_covers."
        )
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    inc: Option<Vec<String>>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Optional text query matched against track titles and sort titles, the tracks' artists, and the titles and artists of the releases holding them that the caller can see; each of its words must start a word, ignoring case, accents, apostrophes and punctuation; no character is an operator, and a word in a script written without spaces, such as Japanese, matches anywhere in a word. Exact names rank first, then names starting with the query, then names holding every word, then names holding some, then matches only through related items."
        )
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
            description = "Comma-separated or repeated values: name, sort_name, date_created, year, duration, disc, track, release_title, artist_name, release_artist_name, listen_count, last_played_at, rating, rated_at, favorited_at, bookmarked_at, relevance (with query), random, id."
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
    personal: PersonalFilterQuery,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Optional: `true` keeps only tracks the authenticated user has bookmarked, `false` only those they have not."
        )
    )]
    #[serde(default, deserialize_with = "super::deserialize_optional_scalar")]
    bookmarked: Option<bool>,
    #[serde(flatten)]
    available: super::AvailableQuery,
    #[serde(flatten)]
    page: super::PageQuery,
    #[serde(flatten)]
    seed: super::SeedQuery,
}

pub(crate) struct TrackListOptions {
    inc: Option<Vec<String>>,
    query: Option<String>,
    library_id: Option<String>,
    release_id: Option<String>,
    sort_by: Option<Vec<String>>,
    sort_order: Option<String>,
    personal: catalog::PersonalFilter,
    bookmarked: Option<bool>,
    available: Option<bool>,
    page_request: super::PageRequest,
}

impl TrackListOptions {
    /// The options `GET /api/tracks?query=` runs with only `inc`, personal filters and paging, so
    /// that endpoint carries on a search's list at later offsets.
    pub(crate) fn for_query(
        inc: Option<Vec<String>>,
        query: String,
        personal: catalog::PersonalFilter,
        available: Option<bool>,
        page_request: super::PageRequest,
    ) -> Self {
        Self {
            inc,
            query: Some(query),
            library_id: None,
            release_id: None,
            sort_by: None,
            sort_order: None,
            personal,
            bookmarked: None,
            available,
            page_request,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct TrackRouteIncludes {
    pub(crate) service: track_service::TrackIncludes,
    pub(crate) entries: bool,
    pub(crate) release_covers: bool,
    pub(crate) artist_covers: bool,
}

pub(crate) fn parse_inc(inc: Option<Vec<String>>) -> Result<TrackRouteIncludes, AppError> {
    let values = super::parse_inc_values(
        inc,
        &[
            "releases",
            "artists",
            "entries",
            "release_covers",
            "artist_covers",
        ],
    )?;
    let mut result = TrackRouteIncludes {
        service: track_service::TrackIncludes {
            releases: false,
            artists: false,
        },
        entries: false,
        release_covers: false,
        artist_covers: false,
    };
    for value in values {
        match value.as_str() {
            "releases" => result.service.releases = true,
            "artists" => result.service.artists = true,
            "entries" => result.entries = true,
            "release_covers" => result.release_covers = true,
            "artist_covers" => result.artist_covers = true,
            _ => {}
        }
    }
    Ok(result)
}

fn resolve_optional_release_filter(
    db: &impl lyra_db::DbAccess,
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

    let release_db_id = lyra_db::lookup::find_node_id_by_id(db, release_id)?
        .ok_or_else(|| AppError::not_found(format!("Release not found: {release_id}")))?;
    lyra_db::releases::get_by_id(db, release_db_id)?
        .ok_or_else(|| AppError::not_found(format!("Release not found: {release_id}")))?;
    crate::services::auth::access::require_entity_accessible(db, principal, release_db_id, || {
        AppError::not_found(format!("Release not found: {release_id}"))
    })?;
    Ok(Some(release_db_id))
}

fn release_to_response(
    availability: &crate::services::catalog::Availability,
    release: lyra_db::Release,
    covers: &HashMap<DbId, ResolvedCover>,
    include_cover: bool,
) -> ReleaseResponse {
    let cover =
        route_covers::cover_field(covers, include_cover, release.db_id.clone().map(DbId::from));
    let mut response = ReleaseResponse::new(release, availability);
    response.cover = cover;
    response
}

/// The responses of `details`, with the covers included resolved together.
pub(crate) fn track_responses(
    db: &DbAny,
    principal: &Principal,
    details: Vec<track_service::TrackDetails>,
    includes: TrackRouteIncludes,
    chains: &TrackChains,
) -> Result<Vec<TrackResponse>, AppError> {
    let mut targets = Vec::new();
    for detail in &details {
        if includes.release_covers
            && let Some(releases) = &detail.releases
        {
            targets.extend(
                releases
                    .iter()
                    .filter_map(|release| release.db_id.clone().map(DbId::from))
                    .map(CoverTarget::Release),
            );
        }
        if includes.artist_covers
            && let Some(artists) = &detail.artists
        {
            targets.extend(
                super::db_ids_from_credited_artists(artists)
                    .into_iter()
                    .map(CoverTarget::Artist),
            );
        }
    }
    let covers = route_covers::resolve_principal_covers(db, principal, &targets)?;
    let track_entries = if includes.entries {
        let viewer = catalog::Viewer::user(db, principal.clone())?;
        let track_db_ids = details
            .iter()
            .filter_map(|detail| detail.track.db_id.clone().map(DbId::from))
            .collect::<Vec<_>>();
        entry_service::by_track(db, &viewer, &track_db_ids)?
    } else {
        HashMap::new()
    };
    let include_entry_paths = lyra_db::roles::has_permission(
        &principal.permissions,
        lyra_db::Permission::ManageLibraries,
    );
    Ok(details
        .into_iter()
        .map(|detail| {
            let entries = includes
                .entries
                .then(|| entry_responses(&detail.track, &track_entries, include_entry_paths));
            track_detail_to_response(detail, entries, &covers, includes, chains)
        })
        .collect())
}

fn entry_responses(
    track: &lyra_db::Track,
    track_entries: &HashMap<DbId, Vec<PlacedEntry>>,
    include_full_path: bool,
) -> Vec<EntryResponse> {
    track
        .db_id
        .clone()
        .map(DbId::from)
        .and_then(|track_db_id| track_entries.get(&track_db_id))
        .into_iter()
        .flatten()
        .map(|placed| EntryResponse::from_entry(placed.entry.clone(), include_full_path))
        .collect()
}

fn track_detail_to_response(
    detail: track_service::TrackDetails,
    entries: Option<Vec<EntryResponse>>,
    covers: &HashMap<DbId, ResolvedCover>,
    includes: TrackRouteIncludes,
    chains: &TrackChains,
) -> TrackResponse {
    let releases = detail.releases.map(|releases| {
        releases
            .into_iter()
            .map(|release| {
                release_to_response(
                    chains.availability(),
                    release,
                    covers,
                    includes.release_covers,
                )
            })
            .collect()
    });
    TrackResponse {
        available: Some(chains.available(&detail.track)),
        chain: chains.get(&detail.track),
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
        artists: detail.artists.map(|v| {
            super::credited_artist_responses(
                chains.availability(),
                v,
                covers,
                includes.artist_covers,
            )
        }),
        entries,
    }
}

pub(crate) async fn list_track_responses(
    principal: &Principal,
    options: TrackListOptions,
) -> Result<PageResponse<TrackResponse>, AppError> {
    track_page(&*STATE.db.read().await, principal, options)
}

pub(crate) fn track_page(
    db: &DbAny,
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
        personal,
        bookmarked,
        available,
        page_request,
    } = options;
    let includes = parse_inc(inc)?;
    let search_term = super::parse_text_query(query);
    let library = crate::services::auth::access::resolve_optional_library_filter(
        db,
        principal,
        library_id.as_deref(),
    )?;
    let release = resolve_optional_release_filter(db, principal, release_id.as_deref())?;
    let sort = super::parse_catalog_sort(sort_by, sort_order)?;
    let viewer = catalog::Viewer::user(db, principal.clone())?;
    let mut query = catalog::Query::<Tracks>::new(TrackFilter {
        library,
        releases: release.map(|release| vec![release]),
        personal,
        bookmarked,
        available,
        ..TrackFilter::default()
    });
    query.search = search_term;
    query.sort = sort;
    page_request
        .catalog(db, &viewer, query)?
        .try_map_items(|tracks| {
            let tracks = tracks.into_iter().map(|listed| listed.item).collect();
            let details = track_service::list_details_for_tracks(db, includes.service, tracks)?;
            let chains = TrackChains::of(db, details.iter().map(|detail| &detail.track))?;
            track_responses(db, principal, details, includes, &chains)
        })
}

pub(crate) async fn get_track_response(
    principal: &Principal,
    id: String,
    inc: Option<Vec<String>>,
) -> Result<TrackResponse, AppError> {
    let db = &*STATE.db.read().await;
    let includes = parse_inc(inc)?;
    let track_db_id = lyra_db::lookup::find_node_id_by_id(db, &id)?
        .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
    crate::services::auth::access::require_entity_accessible(db, principal, track_db_id, || {
        AppError::not_found(format!("Track not found: {id}"))
    })?;
    let detail = track_service::get_details(db, track_db_id, includes.service)?
        .ok_or_else(|| AppError::not_found(format!("Track not found: {}", id)))?;

    let chains = TrackChains::of(db, [&detail.track])?;
    let mut responses = track_responses(db, principal, vec![detail], includes, &chains)?;
    Ok(responses.remove(0))
}

fn build_media_url(
    path: String,
    token: &str,
    query: &PlaybackUrlQuery,
    wait_for_transcode: bool,
) -> String {
    let mut serializer = form_urlencoded::Serializer::new(String::new());
    serializer.append_pair("media_token", token);
    if let Some(start_offset_ms) = query.start_offset_ms {
        serializer.append_pair("start_offset_ms", &start_offset_ms.to_string());
    }
    if wait_for_transcode {
        serializer.append_pair("wait_for_transcode", "true");
    }
    format!("{path}?{}", serializer.finish())
}

fn build_stream_url(id: &str, token: &str, query: &PlaybackUrlQuery) -> String {
    build_media_url(
        format!("/api/stream/{id}"),
        token,
        query,
        query.wait_for_transcode,
    )
}

fn build_hls_url(id: &str, token: &str, query: &PlaybackUrlQuery) -> String {
    build_media_url(format!("/api/stream/{id}/hls.m3u8"), token, query, false)
}

fn build_download_url(id: &str, token: &str, query: &PlaybackUrlQuery) -> String {
    build_media_url(format!("/api/download/{id}"), token, query, false)
}

fn non_empty(value: Option<&String>) -> Option<String> {
    value
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

/// The delivery the stream and download tokens are issued for.
fn file_delivery_options(query: &PlaybackUrlQuery) -> DeliveryOptions {
    DeliveryOptions {
        format: non_empty(query.format.as_ref()),
        codec: non_empty(query.codec.as_ref()),
        copy_profiles: query.copy_profiles.clone(),
        bitrate_bps: query.bitrate_bps,
        sample_rate_hz: query.sample_rate_hz,
        channels: query.channels,
        bit_depth: query.bit_depth,
        prefer_vbr: query.prefer_vbr,
    }
}

/// The delivery the HLS token is issued for.
fn hls_delivery_options(query: &PlaybackUrlQuery) -> DeliveryOptions {
    DeliveryOptions {
        format: None,
        codec: non_empty(query.hls_codec.as_ref()),
        copy_profiles: None,
        bit_depth: None,
        ..file_delivery_options(query)
    }
}

fn playback_transcode_knobs(query: &PlaybackUrlQuery) -> delivery::TranscodeKnobs {
    delivery::TranscodeKnobs {
        bitrate_bps: query.bitrate_bps,
        sample_rate_hz: query.sample_rate_hz,
        channels: query.channels,
        bit_depth: query.bit_depth,
        prefer_vbr: query.prefer_vbr,
    }
}

// Runs before `require_authenticated`, so zero-value rejections stay 400 instead of
// becoming 401. The decision domain remains the single authority for the validation.
fn validate_playback_url_query(query: &PlaybackUrlQuery) -> Result<(), AppError> {
    playback_transcode_knobs(query).validate()?;
    Ok(())
}

/// What the stream and download transports would deliver for this request.
///
/// HLS is not represented because it always survives: the playlist handler
/// transcodes whatever the source is, and a codec it cannot honor is rejected
/// here as a request error rather than as an absent transport.
#[derive(Debug)]
struct PlaybackTransports {
    /// Absent when the resolved container cannot be streamed.
    stream: Option<DeliveryDescription>,
    download: DeliveryDescription,
}

/// Resolves the request once against the source and reports what each transport
/// would deliver.
///
/// Request-level failures — unparseable or incompatible `format`/`codec`/
/// `hls_codec`, a start offset past the source, a codec that cannot produce the
/// requested container — fail the whole request. Only transport *capability*
/// outcomes, such as a container that cannot be streamed, remove a transport.
fn resolve_playback_transports(
    query: &PlaybackUrlQuery,
    source: delivery::ValidatedTrackSource,
) -> Result<PlaybackTransports, AppError> {
    let stream_request = delivery::validate_request(
        query.format.clone(),
        query.codec.clone(),
        query.copy_profiles.clone(),
    )?;
    let source = delivery::apply_request_start_offset(source, query.start_offset_ms)?;
    let knobs = playback_transcode_knobs(query);

    // The download resolution puts no streaming constraint on the container, so
    // streamability becomes an availability answer instead of a whole-request failure.
    // Every other way the request can fail still fails it, for every transport.
    let download = delivery::resolve_delivery(
        &stream_request,
        &source,
        knobs,
        delivery::DeliveryTarget::Download,
    )?;
    let stream = download
        .is_streamable()
        .then(|| {
            delivery::resolve_delivery(
                &stream_request,
                &source,
                knobs,
                delivery::DeliveryTarget::Stream,
            )
        })
        .transpose()?;

    // Parsed last so an unusable hls_codec never outranks a stream-request error, then
    // checked against the real profile so an hls_url is only minted when the playlist
    // handler would answer the same way.
    let hls_request = delivery::validate_request(None, query.hls_codec.clone(), None)?;
    delivery::resolve_hls_profile(&source, &hls_request, knobs)?;

    Ok(PlaybackTransports {
        stream: stream.map(|decision| decision.describe(&source)),
        download: download.describe(&source),
    })
}

async fn create_track_playback_url(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<PlaybackUrlQuery>,
) -> Result<Json<PlaybackUrlResponse>, AppError> {
    validate_playback_url_query(&query)?;
    let auth = require_auth(bearer_credential(&headers)).await?;
    let holder = auth.holder();

    let source = delivery::validate_and_get_track_source(
        &id,
        &delivery::TrackAccess::Credential {
            holder: holder.clone(),
            purpose: TrackPurpose::Stream,
        },
    )
    .await?;
    let version = source.version.clone();
    let transports = resolve_playback_transports(&query, source)?;
    let issue = |purpose, options| {
        issue_media_token(&id, purpose, holder.clone(), version.clone(), options)
    };

    // HLS always survives, which is what keeps `expires_at` total and the response
    // non-empty; tokens for the other transports are only minted when they are usable.
    let hls_token = issue(TrackPurpose::HlsPlaylist, hls_delivery_options(&query));
    let mut expires_at = hls_token.expires_at;
    let hls_url = build_hls_url(&id, &hls_token.token, &query);

    let (stream_url, stream_delivery) = match transports.stream {
        Some(delivery) => {
            let stream_token = issue(TrackPurpose::Stream, file_delivery_options(&query));
            expires_at = expires_at.min(stream_token.expires_at);
            (
                Some(build_stream_url(&id, &stream_token.token, &query)),
                Some(delivery),
            )
        }
        None => (None, None),
    };

    let (download_url, download_delivery) =
        if delivery::authorize_track_purpose(&auth.principal, TrackPurpose::Download).is_ok() {
            let download_token = issue(TrackPurpose::Download, file_delivery_options(&query));
            expires_at = expires_at.min(download_token.expires_at);
            (
                Some(build_download_url(&id, &download_token.token, &query)),
                Some(transports.download),
            )
        } else {
            (None, None)
        };

    Ok(Json(PlaybackUrlResponse {
        stream_url,
        stream_delivery,
        hls_url,
        download_url,
        download_delivery,
        expires_at: crate::services::timestamps::unix_secs_to_rfc3339_i64(expires_at),
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
        personal,
        bookmarked,
        available,
        page,
        seed,
    } = list_query;
    let page = page.resolve().seeded(seed)?;
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    let personal = personal.parse()?;
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
                personal,
                bookmarked,
                available: available.available,
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
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    Ok(Json(get_track_response(&principal, id, query.inc).await?))
}

#[cfg(feature = "docgen")]
fn list_tracks_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List tracks").description(
        "Returns tracks as `{ items, total, offset }`, where `total` counts every track the filters keep. Supported query parameters: `inc`, `query`, `library_id`, `release_id`, `sort_by`, `sort_order`, `rated`, `min_rating`, `max_rating`, `favorite`, `listened`, `bookmarked`, `available`, `limit`, `offset`, `seed`. `available=true` keeps only tracks that can play and `available=false` only those whose files are all missing; tracks report it as `available`. `min_rating` and `max_rating` filter tracks by the authenticated user's inclusive personal rating range; either bound excludes unrated tracks. `rated=true` keeps only tracks the user has rated and `rated=false` only those they have not; `rated=false` cannot be combined with a rating bound. `favorite=true` keeps only tracks the user has favorited and `favorite=false` the rest. `listened=true` keeps only tracks the user has listened to at least once and `listened=false` the rest. `bookmarked=true` keeps only tracks the user has bookmarked and `bookmarked=false` the rest. `library_id` scopes results to tracks belonging to that public library ID. `release_id` scopes results to one public release ID and defaults ordering to album order: disc, track, sort name, id. `sort_by` supports `name`, `sort_name`, `date_created`, `year`, `duration`, `disc`, `track`, `release_title` (the title of the track's release), `artist_name` (the first credited artist), `release_artist_name` (the first artist credited on the track's release), `listen_count`, `last_played_at`, `rating` (the authenticated user's personal rating), `rated_at` (when the authenticated user last set their rating), `favorited_at` (when they first favorited the track), `bookmarked_at` (when they last set their bookmark of the track), `relevance` (with `query`), `random`, and `id`. `random` moves the tracks of a chain as one, keeping them together and in chain order in either direction. `sort_order` supports `ascending` and `descending` and applies to every key. Tracks without a value for a key sort last in both directions, except missing disc and track numbers, which sort first. Ties fall back to sort name, name, then id. `limit` defaults to 100 and is capped at 500. `offset` skips that many items and defaults to 0; every page is read from the catalog as it is at that request, and an `offset` at or past the end returns no items. A `random` sort also returns its `seed`: send it back with later offsets to page through the same shuffle, or leave it out for a new one. `query` matches track titles and sort titles, the tracks' artists, and the titles and artists of the releases holding them that the caller can see; each of its words must start a word, ignoring case, accents, apostrophes and punctuation; no character is an operator, and a word in a script written without spaces, such as Japanese, matches anywhere in a word. It defaults ordering to relevance: exact names rank first, then names starting with the query, then names holding every word, then names holding some, then matches only through related items. A track in a chain carries `chain` with the chain's `id` and the track's 0-based `position` in it. Use `inc` to include releases, artists, and/or entries. When `inc=entries`, each track carries the files holding its audio that the caller can see, by name, `[]` when it can see none, and `full_path` is included only for authenticated users with ManageLibraries permission. When `inc=releases,release_covers`, nested release metadata includes a public cover image URL. When `inc=artists`, each artist carries a `credit` object with `type`, `detail`, and `source`; add `artist_covers` to include public artist image metadata. An artist may appear multiple times with different credits. Artists without direct track credits inherit from the release (`source: release`).",
    )
}

#[cfg(feature = "docgen")]
fn get_track_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get track by ID").description(
        "Returns a single track. 404 if not found. A track in a chain carries `chain` with the chain's `id` and the track's 0-based `position` in it. Use `inc` to include releases, artists, and/or entries. When `inc=entries`, each track carries the files holding its audio that the caller can see, by name, `[]` when it can see none, and `full_path` is included only for authenticated users with ManageLibraries permission. When `inc=releases,release_covers`, nested release metadata includes a public cover image URL. When `inc=artists`, each artist carries a `credit` object with `type`, `detail`, and `source`; add `artist_covers` to include public artist image metadata. An artist may appear multiple times with different credits. Artists without direct track credits inherit from the release (`source: release`).",
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
            description = "Optional comma-separated `container/codec` pairs the client plays as they are (e.g. m4a/aac,ogg/opus,flac/flac), named as `source.container` and `source.codec` of a delivery description. When given, `copy` sends the source unconverted to `stream_url` and `download_url` only if its pair is listed, so a source whose codec is unknown is always converted, and an empty list copies nothing; otherwise any source may be copied. `hls_url` ignores it."
        )
    )]
    copy_profiles: Option<String>,
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
            description = "Bit depth cap for lossless output of `stream_url` and `download_url`: 16 or 24. A deeper source is re-encoded, in its own lossless codec when it would otherwise be sent as it is; ignored for lossy output and when the source is no deeper. `hls_url` ignores it."
        )
    )]
    bit_depth: Option<u32>,
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
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Adds `wait_for_transcode=true` to `stream_url`, so a transcode still being encoded is sent only once it is complete, with its length and byte ranges. Defaults to false."
        )
    )]
    #[serde(default)]
    wait_for_transcode: bool,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "What `stream_url` delivers as minted; present exactly when it is. A `start_offset_ms` added per request can still turn a passthrough into a transcode."
        )
    )]
    stream_delivery: Option<DeliveryDescription>,
    hls_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Absent when the caller lacks download permission.")
    )]
    download_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "What `download_url` delivers as minted; present exactly when it is. A `start_offset_ms` added per request can still turn a passthrough into a transcode."
        )
    )]
    download_delivery: Option<DeliveryDescription>,
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
         a fixed maximum lifetime, and also expire after an idle period. A token acts for the user \
         and credential that requested it: it stops working once that session or API key is \
         revoked, the user is removed, or the user loses access to the track, and it answers \
         410 Gone once the track's source file has changed since it was issued. A token also fixes \
         the delivery it was issued for: the URLs carry no format, codec or transcode settings, a \
         request that adds any is rejected, and only `start_offset_ms` and, on `stream_url`, \
         `wait_for_transcode` may change per request.\n\n\
         `stream_delivery` and `download_delivery` describe what their URLs deliver without starting \
         anything: whether the source file is served as it is or transcoded and why, and the \
         delivered container, codec, content type, bitrate, sample rate, channels and bit depth. They describe \
         the URLs as minted: the start offset stays free per request for seeking, and an offset \
         turns a source served as it is into a transcode of the rest of it.\n\n\
         The request is resolved once against the track's source, and a transport is omitted when \
         it cannot carry the result. `stream_url` is omitted when the request encodes into a container \
         that does not support streaming (caf); use `hls_url` or `download_url` instead. \
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

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct RemapRequest {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Public id of the available track whose file the missing one now is."
        )
    )]
    target_id: String,
}

async fn remap_track(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<RemapRequest>,
) -> Result<Json<TrackResponse>, AppError> {
    use crate::services::metadata::remap::{
        RemapError,
        remap,
    };

    let principal = require_authenticated(bearer_credential(&headers)).await?;
    remap(&principal, &id, &request.target_id)
        .await
        .map_err(|err| match err {
            RemapError::NotFound(id) => AppError::not_found(format!("Track not found: {id}")),
            RemapError::Forbidden(message) => AppError::forbidden(message),
            RemapError::Conflict(message) => AppError::conflict(message),
            RemapError::Internal(err) => err.into(),
        })?;
    Ok(Json(get_track_response(&principal, id, None).await?))
}

#[cfg(feature = "docgen")]
fn remap_track_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Remap a missing track").description(
        "Makes the available track `target_id` the file of this missing track, as when a file was retagged and moved between two syncs. This track keeps its id, favorites, ratings, tags, listens, bookmarks, playlist entries, lyrics and covers, and takes the target's file, release, chain place and tags' metadata; its manual edits stay, filled by the target's. The target's own favorites, ratings, tags, listens, bookmarks, playlist entries, queued places, lyrics and covers move to it, keeping this track's where a user has both, and the target is removed, its id with it. Returns this track. Answers 409 when this track can still play, the target can't, they are the same track, or a sync, refresh or purge of either library is running; 403 unless the caller manages both tracks' libraries.",
    )
}

pub fn track_routes() -> Router {
    Router::new()
        .route("/", get(get_tracks))
        .route("/{id}", get(get_track))
        .route("/{id}/playback-url", post(create_track_playback_url))
        .route("/{id}/remap", post(remap_track))
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
            "/{id}/playback-url",
            post_with(create_track_playback_url, create_track_playback_url_docs),
        )
        .api_route("/{id}/remap", post_with(remap_track, remap_track_docs))
        .merge(lyrics::openapi_routes())
}

#[cfg(test)]
async fn setup_route_test() -> anyhow::Result<()> {
    crate::plugins::testing::init_library_test_state(std::path::PathBuf::from(".")).await
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
        services::auth::Principal,
        services::testing::runtime_test_lock,
    };
    use lyra_db::fixtures::{
        connect,
        insert_artist,
        insert_file_entry,
        insert_release,
        insert_track,
    };
    use lyra_db::nanoid;
    use lyra_db::test_db::{
        connect_artist,
        insert_library,
    };

    async fn admin_principal(accessible_library_ids: HashSet<String>) -> Principal {
        let mut db = STATE.db.write().await;
        let user_db_id = lyra_db::test_db::insert_user(&mut db, "admin").expect("insert admin");
        Principal::for_user(
            &*db,
            user_db_id,
            vec![lyra_db::Permission::Admin],
            accessible_library_ids,
        )
    }

    fn update_track_position(
        db: &mut DbAny,
        track_db_id: DbId,
        disc: Option<u32>,
        track_number: Option<u32>,
    ) -> anyhow::Result<()> {
        let mut track = lyra_db::tracks::get_by_id(db, track_db_id)?
            .ok_or_else(|| anyhow::anyhow!("track missing"))?;
        track.disc = disc;
        track.track = track_number;
        lyra_db::tracks::update(db, &track)
    }

    fn insert_cover_for(db: &mut DbAny, owner_db_id: DbId) -> anyhow::Result<lyra_db::Cover> {
        db.transaction_mut(|t| {
            lyra_db::covers::upsert(
                t,
                owner_db_id,
                lyra_db::covers::CoverSource::Local,
                lyra_db::Cover {
                    db_id: None,
                    id: nanoid(),
                    path: crate::services::testing::existing_cover_path(),
                    mime_type: "image/jpeg".to_string(),
                    hash: "a".repeat(64),
                    blurhash: Some("LKO2?U%2Tw=w]~RBVZRi};RPxuwH".to_string()),
                    provider_id: None,
                },
            )
        })
        .map(|upserted| upserted.cover)
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

    /// A track whose files sit in a shown and a hidden library, one whose only file is hidden,
    /// and principals limited to the shown library.
    struct EntryFixture {
        shared: String,
        hidden_only: String,
        manager: Principal,
        listener: Principal,
    }

    async fn entry_fixture() -> anyhow::Result<EntryFixture> {
        setup_route_test().await?;
        let mut db = STATE.db.write().await;
        let user_db_id = lyra_db::test_db::insert_user(&mut db, "entry-viewer")?;
        let shown = insert_library(&mut db, "Shown", "/music/shown")?;
        let hidden = insert_library(&mut db, "Hidden", "/music/hidden")?;
        let shown_public_id = lyra_db::libraries::get_by_id(&*db, shown)?
            .ok_or_else(|| anyhow::anyhow!("library missing"))?
            .id;

        let shared = insert_track(&mut db, "Shared")?;
        let hidden_only = insert_track(&mut db, "Hidden Only")?;
        let release = insert_release(&mut db, "Entry Release")?;
        connect(&mut db, shown, release)?;
        connect(&mut db, release, shared)?;
        connect(&mut db, release, hidden_only)?;
        for (track, library, name) in [
            (shared, shown, "z.flac"),
            (shared, shown, "a.flac"),
            (shared, hidden, "hidden.flac"),
            (hidden_only, hidden, "other.flac"),
        ] {
            let entry = insert_file_entry(&mut db, library, name)?;
            connect(&mut db, track, entry)?;
        }

        let principal = |permissions| {
            Principal::for_user(
                &*db,
                user_db_id,
                permissions,
                HashSet::from([shown_public_id.clone()]),
            )
        };
        let manager = principal(vec![lyra_db::Permission::ManageLibraries]);
        let listener = principal(Vec::new());
        Ok(EntryFixture {
            shared: lyra_db::tracks::get_by_id(&*db, shared)?
                .ok_or_else(|| anyhow::anyhow!("track missing"))?
                .id,
            hidden_only: lyra_db::tracks::get_by_id(&*db, hidden_only)?
                .ok_or_else(|| anyhow::anyhow!("track missing"))?
                .id,
            manager,
            listener,
        })
    }

    async fn get_track_json(
        principal: &Principal,
        id: &str,
        inc: Option<&str>,
    ) -> anyhow::Result<serde_json::Value> {
        let response = get_track_response(
            principal,
            id.to_string(),
            inc.map(|inc| vec![inc.to_string()]),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        Ok(serde_json::to_value(response)?)
    }

    #[tokio::test]
    async fn track_entries_show_paths_only_to_library_managers() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let fixture = entry_fixture().await?;

        let managed = get_track_json(&fixture.manager, &fixture.shared, Some("entries")).await?;
        let entries = managed["entries"].as_array().expect("entries are an array");
        let names = entries
            .iter()
            .map(|entry| entry["name"].as_str().expect("entry has a name"))
            .collect::<Vec<_>>();
        assert_eq!(names, ["a.flac", "z.flac"]);
        assert_eq!(entries[0]["full_path"], "/music/a.flac");

        let listened = get_track_json(&fixture.listener, &fixture.shared, Some("entries")).await?;
        let entries = listened["entries"]
            .as_array()
            .expect("entries are an array");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0]["name"], "a.flac");
        assert!(entries[0].get("full_path").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn track_entries_are_empty_when_every_file_is_hidden_and_omitted_unasked()
    -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let fixture = entry_fixture().await?;

        let requested =
            get_track_json(&fixture.listener, &fixture.hidden_only, Some("entries")).await?;
        assert_eq!(requested["entries"], serde_json::json!([]));
        let unasked = get_track_json(&fixture.listener, &fixture.hidden_only, None).await?;
        assert!(unasked.get("entries").is_none());

        let page = list_track_responses(
            &fixture.manager,
            TrackListOptions {
                inc: Some(vec!["entries".to_string()]),
                query: None,
                library_id: None,
                release_id: None,
                sort_by: None,
                sort_order: None,
                personal: catalog::PersonalFilter::default(),
                bookmarked: None,
                page_request: super::super::PageRequest::first(100),
                available: None,
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        let counts = page
            .items
            .iter()
            .map(|track| {
                (
                    track.title.as_str(),
                    track.entries.as_ref().map(std::vec::Vec::len),
                )
            })
            .collect::<HashMap<_, _>>();
        assert_eq!(counts["Shared"], Some(2));
        assert_eq!(counts["Hidden Only"], Some(0));
        Ok(())
    }

    #[tokio::test]
    async fn list_track_responses_orders_query_matches_by_relevance() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        {
            let mut db = STATE.db.write().await;
            for title in ["A Blue", "Blue"] {
                insert_track(&mut db, title)?;
            }
        }
        let principal = admin_principal(HashSet::new()).await;

        let page = list_track_responses(
            &principal,
            TrackListOptions {
                inc: None,
                query: Some("blue".to_string()),
                library_id: None,
                release_id: None,
                sort_by: None,
                sort_order: None,
                personal: catalog::PersonalFilter::default(),
                bookmarked: None,
                page_request: super::super::PageRequest::first(100),
                available: None,
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        let titles: Vec<String> = page.items.into_iter().map(|track| track.title).collect();
        assert_eq!(titles, vec!["Blue", "A Blue"]);
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

            let visible_library_id = lyra_db::libraries::get_by_id(&db, visible_library)?
                .ok_or_else(|| anyhow::anyhow!("visible library missing"))?
                .id;
            let hidden_library_id = lyra_db::libraries::get_by_id(&db, hidden_library)?
                .ok_or_else(|| anyhow::anyhow!("hidden library missing"))?
                .id;
            (visible_library_id, hidden_library_id)
        };
        let principal = admin_principal(HashSet::from([
            visible_library_id.clone(),
            hidden_library_id,
        ]))
        .await;

        let page = list_track_responses(
            &principal,
            TrackListOptions {
                inc: None,
                query: None,
                library_id: Some(visible_library_id),
                release_id: None,
                sort_by: None,
                sort_order: None,
                personal: catalog::PersonalFilter::default(),
                bookmarked: None,
                page_request: super::super::PageRequest::first(100),
                available: None,
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].title, "Visible Track");
        assert_eq!(page.total, page.items.len() as u64);
        Ok(())
    }

    #[tokio::test]
    async fn list_track_responses_rejects_principal_whose_db_id_was_recycled() -> anyhow::Result<()>
    {
        use axum::{
            http::StatusCode,
            response::IntoResponse,
        };

        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let stale = {
            let mut db = STATE.db.write().await;
            let alice = lyra_db::test_db::insert_user(&mut db, "recycled-rater")?;
            let library =
                insert_library(&mut db, "Recycled Ratings", "/tmp/lyra-recycled-ratings")?;
            let release = insert_release(&mut db, "Recycled Rating Release")?;
            let track = insert_track(&mut db, "Rated By Replacement")?;
            connect(&mut db, library, release)?;
            connect(&mut db, release, track)?;
            let library_id = lyra_db::libraries::get_by_id(&db, library)?
                .ok_or_else(|| anyhow::anyhow!("library missing"))?
                .id;
            let stale = Principal::for_user(&*db, alice, Vec::new(), HashSet::from([library_id]));
            let replacement = lyra_db::test_db::recycle_user(&mut db, alice, "replacement-rater")?;
            lyra_db::ratings::upsert(
                &mut *db,
                replacement,
                track,
                lyra_db::ratings::RatingValue::new(5).unwrap(),
                1,
            )?;
            stale
        };

        let result = list_track_responses(
            &stale,
            TrackListOptions {
                inc: None,
                query: None,
                library_id: None,
                release_id: None,
                sort_by: None,
                sort_order: None,
                personal: catalog::PersonalFilter {
                    rating: lyra_db::ratings::RatingFilter::new(
                        None,
                        lyra_db::ratings::RatingValue::new(5),
                        lyra_db::ratings::RatingValue::new(5),
                    )
                    .unwrap(),
                    ..catalog::PersonalFilter::default()
                },
                bookmarked: None,
                page_request: super::super::PageRequest::first(100),
                available: None,
            },
        )
        .await;
        let Err(error) = result else {
            anyhow::bail!("a deleted user's principal listed tracks by its replacement's ratings");
        };
        assert_eq!(error.into_response().status(), StatusCode::UNAUTHORIZED);
        Ok(())
    }

    #[tokio::test]
    async fn list_track_responses_filters_personal_ratings_across_pages() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let (principal, four_star_id, five_star_id) = {
            let mut db = STATE.db.write().await;
            let user_db_id = lyra_db::users::create(
                &mut db,
                &lyra_db::fixtures::test_user("track-rating-user")?,
            )?;
            let other_user_db_id = lyra_db::users::create(
                &mut db,
                &lyra_db::fixtures::test_user("other-track-rating-user")?,
            )?;
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
                lyra_db::ratings::upsert(
                    &mut *db,
                    user_db_id,
                    target,
                    lyra_db::ratings::RatingValue::new(value).unwrap(),
                    1,
                )?;
            }
            lyra_db::ratings::upsert(
                &mut *db,
                other_user_db_id,
                other_user_rating,
                lyra_db::ratings::RatingValue::new(5).unwrap(),
                1,
            )?;

            let visible_library_id = lyra_db::libraries::get_by_id(&db, visible_library)?
                .ok_or_else(|| anyhow::anyhow!("visible library missing"))?
                .id;
            let principal = Principal::for_user(
                &*db,
                user_db_id,
                Vec::new(),
                HashSet::from([visible_library_id]),
            );
            let four_star_id = lyra_db::tracks::get_by_id(&db, four_star)?.unwrap().id;
            let five_star_id = lyra_db::tracks::get_by_id(&db, five_star)?.unwrap().id;
            (principal, four_star_id, five_star_id)
        };
        let rating_filter = lyra_db::ratings::RatingFilter::new(
            None,
            lyra_db::ratings::RatingValue::new(4),
            lyra_db::ratings::RatingValue::new(5),
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
                personal: catalog::PersonalFilter {
                    rating: rating_filter,
                    ..catalog::PersonalFilter::default()
                },
                bookmarked: None,
                page_request: super::super::PageQuery {
                    limit: Some(1),
                    offset: None,
                }
                .resolve(),
                available: None,
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert_eq!(first_page.total, 2);
        let second_page = list_track_responses(
            &principal,
            TrackListOptions {
                inc: None,
                query: None,
                library_id: None,
                release_id: None,
                sort_by: None,
                sort_order: None,
                personal: catalog::PersonalFilter {
                    rating: rating_filter,
                    ..catalog::PersonalFilter::default()
                },
                bookmarked: None,
                page_request: super::super::PageQuery {
                    limit: Some(1),
                    offset: Some(1),
                }
                .resolve(),
                available: None,
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

        let exact_five = lyra_db::ratings::RatingFilter::new(
            None,
            lyra_db::ratings::RatingValue::new(5),
            lyra_db::ratings::RatingValue::new(5),
        )
        .unwrap();
        let exact_page = list_track_responses(
            &principal,
            TrackListOptions {
                inc: None,
                query: None,
                library_id: None,
                release_id: None,
                sort_by: None,
                sort_order: None,
                personal: catalog::PersonalFilter {
                    rating: exact_five,
                    ..catalog::PersonalFilter::default()
                },
                bookmarked: None,
                page_request: super::super::PageRequest::first(100),
                available: None,
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
        let principal = admin_principal(HashSet::new()).await;

        let page = list_track_responses(
            &principal,
            TrackListOptions {
                inc: Some(vec!["releases,release_covers".to_string()]),
                query: None,
                library_id: None,
                release_id: None,
                sort_by: None,
                sort_order: None,
                personal: catalog::PersonalFilter::default(),
                bookmarked: None,
                page_request: super::super::PageRequest::first(100),
                available: None,
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
            let track_public_id = lyra_db::tracks::get_by_id(&db, track)?
                .ok_or_else(|| anyhow::anyhow!("track missing"))?
                .id;
            (track_public_id, cover_id)
        };
        let principal = admin_principal(HashSet::new()).await;

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
            let track_public_id = lyra_db::tracks::get_by_id(&db, track)?
                .ok_or_else(|| anyhow::anyhow!("track missing"))?
                .id;
            (track_public_id, cover_id)
        };
        let principal = admin_principal(HashSet::new()).await;

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

            lyra_db::releases::get_by_id(&db, release)?
                .ok_or_else(|| anyhow::anyhow!("release missing"))?
                .id
        };
        let principal = admin_principal(HashSet::new()).await;

        let page = list_track_responses(
            &principal,
            TrackListOptions {
                inc: None,
                query: None,
                library_id: None,
                release_id: Some(release_public_id),
                sort_by: None,
                sort_order: None,
                personal: catalog::PersonalFilter::default(),
                bookmarked: None,
                page_request: super::super::PageRequest::first(100),
                available: None,
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        let titles: Vec<String> = page.items.into_iter().map(|track| track.title).collect();
        assert_eq!(
            titles,
            vec!["Disc One First", "Disc One Second", "Disc Two First"]
        );
        assert_eq!(page.total, 3);
        Ok(())
    }

    fn set_sort_title(db: &mut DbAny, track_db_id: DbId, sort_title: &str) -> anyhow::Result<()> {
        let mut track = lyra_db::tracks::get_by_id(db, track_db_id)?
            .ok_or_else(|| anyhow::anyhow!("track missing"))?;
        track.sort_title = Some(sort_title.to_string());
        lyra_db::tracks::update(db, &track)
    }

    async fn list_titles(
        principal: &Principal,
        release_id: Option<String>,
        sort_by: Option<&str>,
        sort_order: Option<&str>,
    ) -> anyhow::Result<Vec<String>> {
        let page = list_track_responses(
            principal,
            TrackListOptions {
                inc: None,
                query: None,
                library_id: None,
                release_id,
                sort_by: sort_by.map(|value| vec![value.to_string()]),
                sort_order: sort_order.map(str::to_string),
                personal: catalog::PersonalFilter::default(),
                bookmarked: None,
                page_request: super::super::PageRequest::first(100),
                available: None,
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        Ok(page.items.into_iter().map(|track| track.title).collect())
    }

    #[tokio::test]
    async fn list_track_responses_defaults_to_sort_name_then_name_then_id() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        {
            let mut db = STATE.db.write().await;
            insert_track(&mut db, "Charlie")?;
            let zulu = insert_track(&mut db, "Zulu")?;
            set_sort_title(&mut db, zulu, "Alpha")?;
            insert_track(&mut db, "Beta")?;
            insert_track(&mut db, "beta")?;
            for title in ["Mike", "Lima"] {
                let track = insert_track(&mut db, title)?;
                set_sort_title(&mut db, track, "Delta")?;
            }
        }
        let principal = admin_principal(HashSet::new()).await;

        assert_eq!(
            list_titles(&principal, None, None, None).await?,
            vec!["Zulu", "Beta", "beta", "Charlie", "Lima", "Mike"]
        );
        assert_eq!(
            list_titles(&principal, None, Some("sort_name"), Some("descending")).await?,
            vec!["Lima", "Mike", "Charlie", "Beta", "beta", "Zulu"],
            "ties keep ascending order when the requested key is descending"
        );
        Ok(())
    }

    #[tokio::test]
    async fn list_track_responses_sorts_missing_disc_and_track_first() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let release_public_id = {
            let mut db = STATE.db.write().await;
            let release = insert_release(&mut db, "Positions")?;
            for (title, disc, track_number) in [
                ("Known", Some(1), Some(1)),
                ("No Disc", None, Some(5)),
                ("No Track", Some(1), None),
            ] {
                let track = insert_track(&mut db, title)?;
                update_track_position(&mut db, track, disc, track_number)?;
                connect(&mut db, release, track)?;
            }
            lyra_db::releases::get_by_id(&db, release)?
                .ok_or_else(|| anyhow::anyhow!("release missing"))?
                .id
        };
        let principal = admin_principal(HashSet::new()).await;

        for direction in ["ascending", "descending"] {
            let titles = list_titles(
                &principal,
                Some(release_public_id.clone()),
                Some("disc,track"),
                Some(direction),
            )
            .await?;
            assert_eq!(titles[..2], ["No Disc", "No Track"], "{direction}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn list_track_responses_sorts_never_played_last_in_both_directions() -> anyhow::Result<()>
    {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let principal = {
            let mut db = STATE.db.write().await;
            let user_db_id = lyra_db::test_db::insert_user(&mut db, "never-played")?;
            let early = insert_track(&mut db, "Early")?;
            let late = insert_track(&mut db, "Late")?;
            insert_track(&mut db, "Never")?;
            lyra_db::fixtures::insert_listen(&mut db, user_db_id, early, 1_000)?;
            lyra_db::fixtures::insert_listen(&mut db, user_db_id, late, 2_000)?;
            Principal::for_user(
                &*db,
                user_db_id,
                vec![lyra_db::Permission::Admin],
                HashSet::new(),
            )
        };

        assert_eq!(
            list_titles(&principal, None, Some("last_played_at"), Some("ascending")).await?,
            vec!["Early", "Late", "Never"]
        );
        assert_eq!(
            list_titles(&principal, None, Some("last_played_at"), Some("descending")).await?,
            vec!["Late", "Early", "Never"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn list_track_responses_sorts_by_listen_count() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let principal = {
            let mut db = STATE.db.write().await;
            let user_db_id = lyra_db::test_db::insert_user(&mut db, "track-sort-user")?;
            let one_listen = insert_track(&mut db, "One Listen")?;
            let two_listens = insert_track(&mut db, "Two Listens")?;
            lyra_db::fixtures::insert_listen(&mut db, user_db_id, one_listen, 1_000)?;
            lyra_db::fixtures::insert_listen(&mut db, user_db_id, two_listens, 2_000)?;
            lyra_db::fixtures::insert_listen(&mut db, user_db_id, two_listens, 3_000)?;
            Principal::for_user(
                &*db,
                user_db_id,
                vec![lyra_db::Permission::Admin],
                HashSet::new(),
            )
        };

        assert_eq!(
            list_titles(&principal, None, Some("listen_count"), Some("descending")).await?,
            vec!["Two Listens", "One Listen"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn list_track_responses_sorts_by_rating_with_unrated_last() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let principal = {
            let mut db = STATE.db.write().await;
            let user_db_id = lyra_db::test_db::insert_user(&mut db, "track-rating-user")?;
            insert_track(&mut db, "Unrated")?;
            for (title, value) in [("Low", 2), ("High", 5)] {
                let track = insert_track(&mut db, title)?;
                lyra_db::ratings::upsert(
                    &mut db,
                    user_db_id,
                    track,
                    lyra_db::ratings::RatingValue::new(value).unwrap(),
                    1_000,
                )?;
            }
            Principal::for_user(
                &*db,
                user_db_id,
                vec![lyra_db::Permission::Admin],
                HashSet::new(),
            )
        };

        assert_eq!(
            list_titles(&principal, None, Some("rating"), Some("descending")).await?,
            vec!["High", "Low", "Unrated"]
        );
        assert_eq!(
            list_titles(&principal, None, Some("rating"), Some("ascending")).await?,
            vec!["Low", "High", "Unrated"]
        );
        Ok(())
    }

    async fn filtered_titles(
        principal: &Principal,
        rating: lyra_db::ratings::RatingFilter,
        favorite: Option<bool>,
        listened: Option<bool>,
    ) -> anyhow::Result<Vec<String>> {
        let page = list_track_responses(
            principal,
            TrackListOptions {
                inc: None,
                query: None,
                library_id: None,
                release_id: None,
                sort_by: None,
                sort_order: None,
                personal: catalog::PersonalFilter {
                    favorite,
                    listened,
                    rating,
                },
                bookmarked: None,
                page_request: super::super::PageRequest::first(100),
                available: None,
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        Ok(page.items.into_iter().map(|track| track.title).collect())
    }

    #[tokio::test]
    async fn list_track_responses_filters_by_rated_favorite_and_listened() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let principal = {
            let mut db = STATE.db.write().await;
            let user_db_id = lyra_db::test_db::insert_user(&mut db, "track-personal-filter-user")?;
            let other_user_db_id =
                lyra_db::test_db::insert_user(&mut db, "track-personal-filter-other")?;
            let rated = insert_track(&mut db, "Rated")?;
            let favorite = insert_track(&mut db, "Favorite")?;
            let listened = insert_track(&mut db, "Listened")?;
            let others = insert_track(&mut db, "Others")?;
            lyra_db::ratings::upsert(
                &mut db,
                user_db_id,
                rated,
                lyra_db::ratings::RatingValue::new(3).unwrap(),
                1_000,
            )?;
            lyra_db::favorites::add(
                &mut db,
                user_db_id,
                favorite,
                lyra_db::favorites::FavoriteKind::Track,
                1_000,
            )?;
            lyra_db::fixtures::insert_listen(&mut db, user_db_id, listened, 1_000)?;
            lyra_db::ratings::upsert(
                &mut db,
                other_user_db_id,
                others,
                lyra_db::ratings::RatingValue::new(5).unwrap(),
                1_000,
            )?;
            lyra_db::favorites::add(
                &mut db,
                other_user_db_id,
                others,
                lyra_db::favorites::FavoriteKind::Track,
                1_000,
            )?;
            lyra_db::fixtures::insert_listen(&mut db, other_user_db_id, others, 1_000)?;
            Principal::for_user(
                &*db,
                user_db_id,
                vec![lyra_db::Permission::Admin],
                HashSet::new(),
            )
        };
        let rated = |rated| lyra_db::ratings::RatingFilter::new(Some(rated), None, None).unwrap();
        let any = lyra_db::ratings::RatingFilter::Any;

        assert_eq!(
            filtered_titles(&principal, rated(true), None, None).await?,
            vec!["Rated"]
        );
        assert_eq!(
            filtered_titles(&principal, rated(false), None, None).await?,
            vec!["Favorite", "Listened", "Others"]
        );
        assert_eq!(
            filtered_titles(&principal, any, Some(true), None).await?,
            vec!["Favorite"]
        );
        assert_eq!(
            filtered_titles(&principal, any, Some(false), None).await?,
            vec!["Listened", "Others", "Rated"]
        );
        assert_eq!(
            filtered_titles(&principal, any, None, Some(true)).await?,
            vec!["Listened"]
        );
        assert_eq!(
            filtered_titles(&principal, rated(false), Some(false), Some(false)).await?,
            vec!["Others"]
        );
        Ok(())
    }

    #[test]
    fn track_list_query_reads_personal_filters_from_the_query_string() {
        let uri: axum::http::Uri = "/?rated=false&favorite=true&listened=false&limit=5"
            .parse()
            .expect("valid uri");
        let query = Query::<TrackListQuery>::try_from_uri(&uri)
            .expect("query parses")
            .0;
        assert_eq!(
            query.personal.parse().unwrap(),
            catalog::PersonalFilter {
                favorite: Some(true),
                listened: Some(false),
                rating: lyra_db::ratings::RatingFilter::Unrated,
            },
        );

        let uri: axum::http::Uri = "/?rated=false&min_rating=2".parse().expect("valid uri");
        let query = Query::<TrackListQuery>::try_from_uri(&uri)
            .expect("query parses")
            .0;
        assert!(query.personal.parse().is_err());
    }

    #[tokio::test]
    async fn list_track_responses_sorts_by_rated_at_and_favorited_at() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let principal = {
            let mut db = STATE.db.write().await;
            let user_db_id = lyra_db::test_db::insert_user(&mut db, "track-timestamp-sort-user")?;
            insert_track(&mut db, "Neither")?;
            let early = insert_track(&mut db, "Early")?;
            let late = insert_track(&mut db, "Late")?;
            let rate = |db: &mut DbAny, track, at| {
                lyra_db::ratings::upsert(
                    db,
                    user_db_id,
                    track,
                    lyra_db::ratings::RatingValue::new(4).unwrap(),
                    at,
                )
            };
            rate(&mut db, early, 1_000)?;
            rate(&mut db, late, 2_000)?;
            for (track, at) in [(late, 1_000), (early, 2_000)] {
                lyra_db::favorites::add(
                    &mut db,
                    user_db_id,
                    track,
                    lyra_db::favorites::FavoriteKind::Track,
                    at,
                )?;
            }
            Principal::for_user(
                &*db,
                user_db_id,
                vec![lyra_db::Permission::Admin],
                HashSet::new(),
            )
        };

        assert_eq!(
            list_titles(&principal, None, Some("rated_at"), Some("descending")).await?,
            vec!["Late", "Early", "Neither"]
        );
        assert_eq!(
            list_titles(&principal, None, Some("favorited_at"), Some("descending")).await?,
            vec!["Early", "Late", "Neither"]
        );
        assert_eq!(
            list_titles(&principal, None, Some("favorited_at"), Some("ascending")).await?,
            vec!["Late", "Early", "Neither"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn list_track_responses_filter_and_sort_by_bookmarks() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let principal = {
            let mut db = STATE.db.write().await;
            let user_db_id = lyra_db::test_db::insert_user(&mut db, "track-bookmark-user")?;
            let other_user = lyra_db::test_db::insert_user(&mut db, "track-bookmark-other")?;
            insert_track(&mut db, "Neither")?;
            let early = insert_track(&mut db, "Early")?;
            let late = insert_track(&mut db, "Late")?;
            let others = insert_track(&mut db, "Others")?;
            lyra_db::bookmarks::upsert(&mut *db, user_db_id, late, 0, None, 1_000)?;
            lyra_db::bookmarks::upsert(&mut *db, user_db_id, early, 0, None, 1_500)?;
            lyra_db::bookmarks::upsert(&mut *db, user_db_id, late, 10, None, 2_000)?;
            lyra_db::bookmarks::upsert(&mut *db, other_user, others, 0, None, 3_000)?;
            Principal::for_user(
                &*db,
                user_db_id,
                vec![lyra_db::Permission::Admin],
                HashSet::new(),
            )
        };
        let titles = |bookmarked, sort_by: Option<&str>| {
            let principal = principal.clone();
            let sort_by = sort_by.map(|key| vec![key.to_string()]);
            async move {
                let page = list_track_responses(
                    &principal,
                    TrackListOptions {
                        inc: None,
                        query: None,
                        library_id: None,
                        release_id: None,
                        sort_by,
                        sort_order: Some("descending".to_string()),
                        personal: catalog::PersonalFilter::default(),
                        bookmarked,
                        page_request: super::super::PageRequest::first(100),
                        available: None,
                    },
                )
                .await
                .map_err(|err| anyhow::anyhow!("{err:?}"))?;
                anyhow::Ok(
                    page.items
                        .into_iter()
                        .map(|track| track.title)
                        .collect::<Vec<_>>(),
                )
            }
        };

        assert_eq!(
            titles(Some(true), Some("bookmarked_at")).await?,
            vec!["Late", "Early"]
        );
        assert_eq!(titles(Some(false), None).await?, vec!["Neither", "Others"]);
        assert_eq!(
            titles(None, Some("bookmarked_at")).await?,
            vec!["Late", "Early", "Neither", "Others"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn list_track_responses_rejects_unknown_sort_keys() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;
        let principal = admin_principal(HashSet::new()).await;

        let error = list_titles(&principal, None, Some("release_date"), None)
            .await
            .expect_err("tracks have no release_date key");
        assert!(
            error
                .to_string()
                .contains("Supported keys: name, sort_name")
        );
        Ok(())
    }

    #[tokio::test]
    async fn list_track_responses_later_page_leaves_out_tracks_made_hidden() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let (principal, library, release) = {
            let mut db = STATE.db.write().await;
            let user_db_id = lyra_db::test_db::insert_user(&mut db, "resume-access")?;
            let library = insert_library(&mut db, "Resume Access", "/tmp/lyra-resume-access")?;
            let release = insert_release(&mut db, "Resume Release")?;
            connect(&mut db, library, release)?;
            for title in ["First", "Second"] {
                let track = insert_track(&mut db, title)?;
                connect(&mut db, release, track)?;
            }
            let library_id = lyra_db::libraries::get_by_id(&db, library)?
                .ok_or_else(|| anyhow::anyhow!("library missing"))?
                .id;
            let principal =
                Principal::for_user(&*db, user_db_id, Vec::new(), HashSet::from([library_id]));
            (principal, library, release)
        };
        let page_options = |offset: u64| TrackListOptions {
            inc: None,
            query: None,
            library_id: None,
            release_id: None,
            sort_by: None,
            sort_order: None,
            personal: catalog::PersonalFilter::default(),
            bookmarked: None,
            page_request: super::super::PageQuery {
                limit: Some(1),
                offset: Some(offset),
            }
            .resolve(),
            available: None,
        };

        let first = list_track_responses(&principal, page_options(0))
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert_eq!(first.items.len(), 1);
        assert_eq!(first.total, 2);

        {
            let mut db = STATE.db.write().await;
            lyra_db::graph::remove_edges_between(&mut *db, library, release)?;
        }

        let second = list_track_responses(&principal, page_options(1))
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert!(second.items.is_empty());
        assert_eq!(second.total, 0);
        assert_eq!(second.offset, 0);
        Ok(())
    }

    /// A release of `titles` in a library only the returned user can see.
    async fn paged_fixture(name: &str, titles: &[String]) -> anyhow::Result<Principal> {
        let mut db = STATE.db.write().await;
        let user_db_id = lyra_db::test_db::insert_user(&mut db, name)?;
        let library = insert_library(&mut db, name, &format!("/tmp/lyra-{name}"))?;
        let release = insert_release(&mut db, name)?;
        connect(&mut db, library, release)?;
        for title in titles {
            let track = insert_track(&mut db, title)?;
            connect(&mut db, release, track)?;
        }
        let library_id = lyra_db::libraries::get_by_id(&db, library)?
            .ok_or_else(|| anyhow::anyhow!("library missing"))?
            .id;
        Ok(Principal::for_user(
            &*db,
            user_db_id,
            Vec::new(),
            HashSet::from([library_id]),
        ))
    }

    async fn track_page_at(
        principal: &Principal,
        query: &str,
    ) -> anyhow::Result<PageResponse<TrackResponse>> {
        let uri: axum::http::Uri = format!("/api/tracks?{query}").parse()?;
        let Query(TrackListQuery {
            sort_by,
            page,
            seed,
            ..
        }) = Query::<TrackListQuery>::try_from_uri(&uri)?;
        list_track_responses(
            principal,
            TrackListOptions {
                inc: None,
                query: None,
                library_id: None,
                release_id: None,
                sort_by,
                sort_order: None,
                personal: catalog::PersonalFilter::default(),
                bookmarked: None,
                page_request: page
                    .resolve()
                    .seeded(seed)
                    .map_err(|err| anyhow::anyhow!("{err:?}"))?,
                available: None,
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))
    }

    fn titles(page: &PageResponse<TrackResponse>) -> Vec<String> {
        page.items.iter().map(|track| track.title.clone()).collect()
    }

    #[tokio::test]
    async fn list_track_responses_pages_by_offset_once_and_in_order() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;
        let expected = (0..7).map(|n| format!("Paged {n}")).collect::<Vec<_>>();
        let principal = paged_fixture("offset-pages", &expected).await?;
        // Tracks the viewer cannot see never reach its items or its total.
        paged_fixture("offset-pages-hidden", &["Hidden".to_string()]).await?;

        let mut seen = Vec::new();
        for offset in [0, 3, 6] {
            let page = track_page_at(&principal, &format!("limit=3&offset={offset}")).await?;
            assert_eq!(page.total, 7);
            assert_eq!(page.offset, offset);
            assert!(page.seed.is_none());
            seen.extend(titles(&page));
        }
        assert_eq!(seen, expected);

        for offset in [7, 8, 1_000_000] {
            let page = track_page_at(&principal, &format!("limit=3&offset={offset}")).await?;
            assert!(page.items.is_empty());
            assert_eq!(page.total, 7);
            assert_eq!(page.offset, 7);
        }
        Ok(())
    }

    #[tokio::test]
    async fn list_track_responses_random_sort_pages_through_the_seeded_shuffle()
    -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;
        let expected = (0..40)
            .map(|n| format!("Shuffled {n:02}"))
            .collect::<Vec<_>>();
        let principal = paged_fixture("seeded-shuffle", &expected).await?;

        let whole = track_page_at(&principal, "sort_by=random").await?;
        let seed = whole
            .seed
            .ok_or_else(|| anyhow::anyhow!("a random sort returns its seed"))?;
        assert!(seed <= crate::routes::pagination::MAX_SEED);
        let whole = titles(&whole);
        assert_ne!(whole, expected);

        let mut paged = Vec::new();
        for offset in [0, 15, 30] {
            let page = track_page_at(
                &principal,
                &format!("sort_by=random&seed={seed}&limit=15&offset={offset}"),
            )
            .await?;
            assert_eq!(page.seed, Some(seed));
            assert_eq!(page.total, 40);
            paged.extend(titles(&page));
        }
        assert_eq!(paged, whole);

        let other = track_page_at(&principal, &format!("sort_by=random&seed={}", seed ^ 1)).await?;
        assert_ne!(titles(&other), whole);
        let mut sorted = titles(&other);
        sorted.sort();
        assert_eq!(sorted, expected);

        let unseeded = track_page_at(&principal, "sort_by=random").await?;
        assert_ne!(unseeded.seed, Some(seed));

        let too_large = track_page_at(&principal, "sort_by=random&seed=9007199254740992").await;
        assert!(too_large.is_err());
        Ok(())
    }

    #[test]
    fn build_stream_url_carries_only_the_token_start_offset_and_wait() {
        let query = PlaybackUrlQuery {
            format: Some("mp3".to_string()),
            codec: Some("copy,mp3".to_string()),
            copy_profiles: Some("mp3/mp3".to_string()),
            hls_codec: Some("aac".to_string()),
            bitrate_bps: Some(128_000),
            sample_rate_hz: Some(44_100),
            channels: Some(2),
            bit_depth: None,
            prefer_vbr: Some(true),
            start_offset_ms: Some(12_345),
            wait_for_transcode: true,
        };

        let url = build_stream_url("track/id", "token value", &query);

        assert_eq!(
            url,
            "/api/stream/track/id?media_token=token+value&start_offset_ms=12345&wait_for_transcode=true"
        );
        assert_eq!(
            build_download_url("track/id", "token", &query),
            "/api/download/track/id?media_token=token&start_offset_ms=12345"
        );
        assert_eq!(
            file_delivery_options(&query),
            DeliveryOptions {
                format: Some("mp3".to_string()),
                codec: Some("copy,mp3".to_string()),
                copy_profiles: Some("mp3/mp3".to_string()),
                bitrate_bps: Some(128_000),
                sample_rate_hz: Some(44_100),
                channels: Some(2),
                bit_depth: None,
                prefer_vbr: Some(true),
            }
        );
        assert_eq!(hls_delivery_options(&query).codec.as_deref(), Some("aac"));
        assert_eq!(hls_delivery_options(&query).format, None);
        assert_eq!(hls_delivery_options(&query).copy_profiles, None);
    }

    fn playback_url_query(
        format: Option<&str>,
        codec: Option<&str>,
        hls_codec: Option<&str>,
    ) -> PlaybackUrlQuery {
        PlaybackUrlQuery {
            format: format.map(str::to_string),
            codec: codec.map(str::to_string),
            copy_profiles: None,
            hls_codec: hls_codec.map(str::to_string),
            bitrate_bps: None,
            sample_rate_hz: None,
            channels: None,
            bit_depth: None,
            prefer_vbr: None,
            start_offset_ms: None,
            wait_for_transcode: false,
        }
    }

    fn flac_playback_source() -> crate::services::delivery::ValidatedTrackSource {
        crate::services::delivery::ValidatedTrackSource {
            source_id: DbId(2),
            source_public_id: "source-pub".to_string(),
            version: crate::services::playback_sources::SourceVersion::fixed("source-pub"),
            track_public_id: "track-pub".to_string(),
            input_path: "track.flac".to_string(),
            entry_format: Some(lyra_ffmpeg::AudioFormat::Flac),
            source_codec: Some(lyra_ffmpeg::AudioCodec::Flac),
            full_path: std::path::PathBuf::from("track.flac"),
            duration_ms: Some(20_000),
            start_ms: None,
            end_ms: None,
            format: lyra_db::track_sources::SourceFormat {
                sample_rate_hz: Some(96_000),
                channel_count: Some(2),
                bitrate_bps: Some(900_000),
                ..Default::default()
            },
        }
    }

    #[test]
    fn resolve_playback_transports_accepts_a_resolvable_stream_request() {
        let transports = resolve_playback_transports(
            &playback_url_query(Some("mp3"), None, None),
            flac_playback_source(),
        )
        .expect("a plain mp3 transcode request stays valid");
        assert!(transports.stream.is_some());
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
            &playback_url_query(Some("caf"), None, None),
            flac_playback_source(),
        )
        .expect("caf is downloadable even though it cannot be streamed");
        assert!(transports.stream.is_none());
    }

    #[test]
    fn resolve_playback_transports_streams_an_unchanged_source_in_any_container() {
        let mut source = flac_playback_source();
        source.entry_format = Some(lyra_ffmpeg::AudioFormat::M4a);
        source.source_codec = Some(lyra_ffmpeg::AudioCodec::Alac);
        source.input_path = "track.m4a".to_string();
        source.full_path = std::path::PathBuf::from("track.m4a");

        let transports = resolve_playback_transports(&playback_url_query(None, None, None), source)
            .expect("an m4a source resolves");
        assert!(
            transports.stream.is_some(),
            "the original m4a file streams as it is"
        );
    }

    #[test]
    fn resolve_playback_transports_converts_a_source_outside_the_copy_profiles() {
        let mut source = flac_playback_source();
        source.entry_format = Some(lyra_ffmpeg::AudioFormat::M4a);
        source.source_codec = Some(lyra_ffmpeg::AudioCodec::Alac);
        source.format.container = Some(lyra_db::track_sources::SourceContainer::M4a);
        source.format.codec = Some(lyra_db::track_sources::SourceCodec::Alac);
        let query = PlaybackUrlQuery {
            copy_profiles: Some("m4a/aac".to_string()),
            ..playback_url_query(None, Some("copy,aac"), Some("copy"))
        };

        let transports = resolve_playback_transports(&query, source).expect("an alac m4a resolves");
        let stream = transports.stream.expect("the m4a streams");
        assert!(stream.transcode);
        assert_eq!(stream.codec, Some("aac"));
        assert!(transports.download.transcode);
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
            transports.stream.is_some(),
            "the final mp3 container is streamable, so a stream_url should be minted"
        );
    }

    #[test]
    fn resolve_playback_transports_rejects_an_hls_codec_the_playlist_cannot_honor() {
        // vorbis parses as a codec name but has no HLS profile, so the minted hls_url would
        // have failed at GET time with this same message.
        let err = resolve_playback_transports(
            &playback_url_query(None, None, Some("vorbis")),
            flac_playback_source(),
        )
        .expect_err("vorbis is not an HLS codec");
        assert_eq!(
            format!("{err:?}"),
            "AppError(400 Bad Request: Unsupported HLS codec. Supported values: copy, aac, alac, flac, opus, mp3.)"
        );
    }

    #[test]
    fn resolve_playback_transports_accepts_hls_copy_for_a_copyable_source() {
        let transports = resolve_playback_transports(
            &playback_url_query(None, None, Some("copy")),
            flac_playback_source(),
        )
        .expect("a flac source can be segmented by stream copy");
        assert!(transports.stream.is_some());
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

    use agdb::DbAny;
    use test::{
        Bencher,
        black_box,
    };

    use super::*;
    use crate::services::catalog::tracks::TrackKey;
    use lyra_db::fixtures::{
        insert_track,
        test_user,
    };
    use lyra_db::test_db::new_test_db;

    struct TrackSortBench {
        db: DbAny,
        viewer: catalog::Viewer,
    }

    fn seed_track_sort_bench(track_count: usize, listens_per_track: usize) -> TrackSortBench {
        let mut db = new_test_db().unwrap();
        let user_db_id =
            lyra_db::users::create(&mut db, &test_user("track-sort-bench").unwrap()).unwrap();
        for i in 0..track_count {
            let track_db_id = insert_track(&mut db, &format!("Track {i:04}")).unwrap();
            for listen_idx in 0..listens_per_track {
                lyra_db::fixtures::insert_listen(
                    &mut db,
                    user_db_id,
                    track_db_id,
                    ((i * listens_per_track + listen_idx) as u64) * 1_000,
                )
                .unwrap();
            }
        }

        let principal = Principal::for_user(
            &db,
            user_db_id,
            vec![lyra_db::Permission::Admin],
            Default::default(),
        );
        let viewer = catalog::Viewer::user(&db, principal).unwrap();
        TrackSortBench { db, viewer }
    }

    fn bench_order(b: &mut Bencher, setup: &TrackSortBench, sort: catalog::SortSpec<TrackKey>) {
        let mut query = catalog::Query::<Tracks>::new(TrackFilter::default());
        query.sort = sort;
        b.iter(|| catalog::order(&setup.db, &setup.viewer, black_box(&query)).unwrap());
    }

    #[bench]
    fn route_sort_tracks_sort_name_1000(b: &mut Bencher) {
        let setup = seed_track_sort_bench(1_000, 0);
        bench_order(b, &setup, Vec::new());
    }

    #[bench]
    fn route_sort_tracks_listen_count_1000_tracks_3000_listens(b: &mut Bencher) {
        let setup = seed_track_sort_bench(1_000, 3);
        bench_order(
            b,
            &setup,
            vec![(TrackKey::ListenCount, catalog::Direction::Descending)],
        );
    }
}
