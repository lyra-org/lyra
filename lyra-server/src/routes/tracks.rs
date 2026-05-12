// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use aide::axum::{
    ApiRouter,
    routing::{
        delete_with,
        get_with,
        post_with,
        put_with,
    },
};
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
use schemars::JsonSchema;
use serde::{
    Deserialize,
    Serialize,
};
use url::form_urlencoded;

use crate::{
    STATE,
    db::{
        self,
        ListOptions,
        SortDirection,
        SortKey,
        SortSpec,
    },
    routes::AppError,
    routes::deserialize_inc,
    routes::responses::{
        LyricsLineResponse,
        LyricsResponse,
        LyricsWordResponse,
        PageResponse,
        ReleaseResponse,
        TrackResponse,
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
        tracks as track_service,
    },
};

#[derive(Deserialize, JsonSchema)]
struct TrackQuery {
    #[schemars(description = "Comma-separated or repeated values: releases, artists.")]
    #[serde(default, deserialize_with = "deserialize_inc")]
    inc: Option<Vec<String>>,
}

#[derive(Deserialize, JsonSchema)]
struct TrackListQuery {
    #[schemars(description = "Comma-separated or repeated values: releases, artists.")]
    #[serde(default, deserialize_with = "deserialize_inc")]
    inc: Option<Vec<String>>,
    #[schemars(description = "Optional fuzzy text query matched against track titles.")]
    query: Option<String>,
    #[schemars(description = "Optional public library ID to scope returned tracks.")]
    library_id: Option<String>,
    #[serde(flatten)]
    page: super::PageQuery,
}

fn parse_inc(inc: Option<Vec<String>>) -> Result<track_service::TrackIncludes, AppError> {
    let values = super::parse_inc_values(inc, &["releases", "artists"])?;
    let mut result = track_service::TrackIncludes {
        releases: false,
        artists: false,
    };
    for value in values {
        match value.as_str() {
            "releases" => result.releases = true,
            "artists" => result.artists = true,
            _ => {}
        }
    }
    Ok(result)
}

fn default_track_sort() -> Vec<SortSpec> {
    vec![SortSpec {
        key: SortKey::SortName,
        direction: SortDirection::Ascending,
    }]
}

fn track_detail_to_response(
    _db: &impl db::DbAccess,
    detail: track_service::TrackDetails,
) -> anyhow::Result<TrackResponse> {
    let releases = detail
        .releases
        .map(|v| v.into_iter().map(ReleaseResponse::from).collect());
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
            .map(|v| v.into_iter().map(Into::into).collect()),
    })
}

pub(crate) async fn list_track_responses(
    principal: &Principal,
    inc: Option<Vec<String>>,
    query: Option<String>,
    library_id: Option<String>,
    page_options: super::PageOptions,
) -> Result<PageResponse<TrackResponse>, AppError> {
    let db = &*STATE.db.read().await;
    let includes = parse_inc(inc)?;
    let search_term = super::parse_text_query(query);
    let options = ListOptions {
        sort: default_track_sort(),
        offset: None,
        limit: None,
        search_term,
    };
    let library_scope =
        super::resolve_optional_library_filter(db, principal, library_id.as_deref())?;

    let accessible_tracks = match library_scope {
        Some(library_db_id) => db::tracks::get_by_library(db, library_db_id)?,
        None => {
            let tracks = db::tracks::get(db, "tracks")?;
            let mut accessible_tracks = Vec::with_capacity(tracks.len());
            for track in tracks {
                let Some(track_db_id) = track.db_id.clone().map(agdb::DbId::from) else {
                    continue;
                };
                if !super::entity_accessible_to_principal(db, principal, track_db_id)? {
                    continue;
                }
                accessible_tracks.push(track);
            }
            accessible_tracks
        }
    };

    let page = db::tracks::query_items(
        accessible_tracks,
        &ListOptions {
            offset: Some(page_options.offset),
            limit: Some(page_options.limit),
            ..options
        },
    );
    let next_cursor = super::next_page_cursor(page.offset, page.entries.len(), page.total_count);
    let details = track_service::list_details_for_tracks(db, includes, page.entries)?;

    let mut items = Vec::with_capacity(details.len());
    for detail in details {
        items.push(track_detail_to_response(db, detail)?);
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
    let detail = track_service::get_details(db, track_db_id, includes)?
        .ok_or_else(|| AppError::not_found(format!("Track not found: {}", id)))?;

    Ok(track_detail_to_response(db, detail)?)
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
        page,
    } = list_query;
    let page = page.resolve()?;
    let principal = require_authenticated(&headers).await?;
    Ok(Json(
        list_track_responses(&principal, inc, query, library_id, page).await?,
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

fn list_tracks_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List tracks").description(
        "Returns tracks as `{ items, next_cursor }`. Supported query parameters: `inc`, `query`, `library_id`, `limit`, `cursor`. `library_id` scopes results to tracks belonging to that public library ID. `limit` defaults to 100 and is capped at 500. Drive pagination from `next_cursor`; it is `null` on the last page. `query` is a fuzzy text match against track titles. Use `inc` to include releases and/or artists. When `inc=artists`, each artist carries a `credit` object with `type`, `detail`, and `source`. An artist may appear multiple times with different credits. Artists without direct track credits inherit from the release (`source: release`).",
    )
}

fn get_track_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get track by ID").description(
        "Returns a single track. 404 if not found. Use `inc` to include releases and/or artists. When `inc=artists`, each artist carries a `credit` object with `type`, `detail`, and `source`. An artist may appear multiple times with different credits. Artists without direct track credits inherit from the release (`source: release`).",
    )
}

#[derive(Deserialize, JsonSchema)]
struct LyricsQuery {
    #[schemars(
        description = "Output format: `json` (default), `plain`, or `lrc`. `lrc` returns 406 when no stored candidate has synced content meeting the selector's coverage threshold, even if `json`/`plain` would succeed for the same track."
    )]
    format: Option<String>,
    #[schemars(
        description = "Preferred language as ISO-639-2 (e.g. 'eng', 'jpn'). When no stored lyric matches this language, the server falls back to the best available lyric regardless of language; inspect `language` on the response to tell whether the preference was honoured."
    )]
    language: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct LyricsWriteQuery {
    #[schemars(description = "Language for raw LRC and plain text uploads. Defaults to `und`.")]
    language: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct PlaybackUrlQuery {
    #[schemars(
        description = "Optional stream output format (e.g. mp3, flac, wav, ogg, webm, aac, opus)."
    )]
    format: Option<String>,
    #[schemars(
        description = "Optional ordered stream codec preferences (e.g. opus,aac or pcm_s24le,pcm_s16le)."
    )]
    codec: Option<String>,
    #[schemars(
        description = "Optional ordered HLS codec preferences for `hls_url` (for example: copy,aac or aac,flac)."
    )]
    hls_codec: Option<String>,
    #[schemars(
        description = "Target bitrate cap in bits per second. Applied to generated stream, HLS, and download URLs."
    )]
    bitrate_bps: Option<u32>,
    #[schemars(description = "Target sample rate in Hz.")]
    sample_rate_hz: Option<u32>,
    #[schemars(description = "Target channel count.")]
    channels: Option<u32>,
    #[schemars(
        description = "Prefer VBR for lossy transcodes when the selected encoder supports it."
    )]
    prefer_vbr: Option<bool>,
    #[schemars(description = "Per-request playback start offset in milliseconds.")]
    start_offset_ms: Option<u64>,
}

#[derive(Serialize, JsonSchema)]
struct PlaybackUrlResponse {
    stream_url: String,
    hls_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    download_url: Option<String>,
    #[schemars(description = "Absolute media-token expiration as an RFC3339 timestamp.")]
    expires_at: String,
    #[schemars(description = "Media tokens also expire after this many seconds without use.")]
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

fn delete_track_lyrics_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Delete user track lyrics").description(
        "Deletes the user-authored lyrics override for a track. Plugin/provider lyrics are left \
         intact, so future reads may fall back to provider lyrics. Idempotent: returns 204 even \
         when the track has no user-authored lyrics.",
    ).response::<204, ()>()
}

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

fn create_track_playback_url_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Create playable track URLs").description(
        "Returns browser-friendly stream and HLS URLs containing scoped media tokens for the track. \
         The caller must authenticate with a bearer session token or API key and have access to the \
         track. Returned media tokens are limited to this track and endpoint purpose, expire after \
         a fixed maximum lifetime, and also expire after an idle period. `download_url` is included \
         only when the caller has download permission.",
    )
}

pub fn track_routes() -> ApiRouter {
    ApiRouter::new()
        .api_route("/", get_with(get_tracks, list_tracks_docs))
        .api_route("/{id}", get_with(get_track, get_track_docs))
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

    use agdb::DbId;

    use super::*;
    use crate::{
        db::test_db::{
            connect,
            insert_library,
            insert_release,
            insert_track,
        },
        services::auth::Principal,
        testing::{
            LibraryFixtureConfig,
            initialize_runtime,
            runtime_test_lock,
        },
    };

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
            None,
            None,
            Some(visible_library_id),
            super::super::PageOptions {
                limit: 100,
                offset: 0,
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].title, "Visible Track");
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
