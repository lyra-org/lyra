// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use aide::axum::{
    ApiRouter,
    routing::{
        delete_with,
        get_with,
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
use serde::Deserialize;

use crate::{
    STATE,
    db::{
        self,
        ListOptions,
    },
    routes::AppError,
    routes::deserialize_inc,
    routes::responses::{
        LyricsLineResponse,
        LyricsResponse,
        LyricsWordResponse,
        ReleaseResponse,
        TrackResponse,
    },
    services::{
        auth::require_authenticated,
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
    inc: Option<Vec<String>>,
    query: Option<String>,
) -> Result<Vec<TrackResponse>, AppError> {
    let db = &*STATE.db.read().await;
    let includes = parse_inc(inc)?;
    let options = ListOptions {
        sort: Vec::new(),
        offset: None,
        limit: None,
        search_term: super::parse_text_query(query),
    };
    let details = track_service::list_details(db, includes, &options)?;

    details
        .into_iter()
        .map(|d| track_detail_to_response(db, d))
        .collect::<anyhow::Result<Vec<_>>>()
        .map_err(AppError::from)
}

pub(crate) async fn get_track_response(
    id: String,
    inc: Option<Vec<String>>,
) -> Result<TrackResponse, AppError> {
    let db = &*STATE.db.read().await;
    let includes = parse_inc(inc)?;
    let track_db_id = db::lookup::find_node_id_by_id(db, &id)?
        .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
    let detail = track_service::get_details(db, track_db_id, includes)?
        .ok_or_else(|| AppError::not_found(format!("Track not found: {}", id)))?;

    Ok(track_detail_to_response(db, detail)?)
}

async fn get_tracks(
    headers: HeaderMap,
    Query(list_query): Query<TrackListQuery>,
) -> Result<Json<Vec<TrackResponse>>, AppError> {
    let _principal = require_authenticated(&headers).await?;
    Ok(Json(
        list_track_responses(list_query.inc, list_query.query).await?,
    ))
}

async fn get_track(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<TrackQuery>,
) -> Result<Json<TrackResponse>, AppError> {
    let _principal = require_authenticated(&headers).await?;
    Ok(Json(get_track_response(id, query.inc).await?))
}

fn list_tracks_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List tracks").description(
        "Returns tracks. Supported query parameters: `inc`, `query`. `query` is a fuzzy text match against track titles. Use `inc` to include releases and/or artists. When `inc=artists`, each artist carries a `credit` object with `type`, `detail`, and `source`. An artist may appear multiple times with different credits. Artists without direct track credits inherit from the release (`source: release`).",
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

async fn get_track_lyrics(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<LyricsQuery>,
) -> Result<Response, AppError> {
    let _principal = require_authenticated(&headers).await?;
    let db = &*STATE.db.read().await;

    // Same 404 body whether the track or the lyrics are missing — hides which
    // stage failed from an authenticated caller trying to enumerate.
    let not_found = || AppError::not_found(format!("No lyrics for track: {id}"));

    let track_db_id = db::lookup::find_node_id_by_id(db, &id)?.ok_or_else(not_found)?;
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
    let _principal = require_authenticated(&headers).await?;
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
    let detail = lyrics_service::upsert_user_lyrics(&mut db, &id, input)
        .map_err(lyrics_upload_error_to_app_error)?;

    Ok(lyrics_response_json(detail))
}

async fn delete_track_lyrics(
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<StatusCode, AppError> {
    let _principal = require_authenticated(&headers).await?;
    let mut db = STATE.db.write().await;
    lyrics_service::delete_user_lyrics_for_track(&mut db, &id)
        .map_err(lyrics_upload_error_to_app_error)?;
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
        updated_at: lyrics.updated_at,
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

pub fn track_routes() -> ApiRouter {
    ApiRouter::new()
        .api_route("/", get_with(get_tracks, list_tracks_docs))
        .api_route("/{id}", get_with(get_track, get_track_docs))
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
}
