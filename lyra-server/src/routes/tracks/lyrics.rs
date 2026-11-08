// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
use aide::transform::TransformOperation;
use axum::{
    Json,
    Router,
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
    routing::{
        delete,
        get,
        post,
        put,
    },
};
use serde::Deserialize;

use crate::routes::request::bearer_credential;
use crate::{
    routes::{
        AppError,
        responses::{
            LyricsCandidateSummaryResponse,
            LyricsCandidatesResponse,
            LyricsLineResponse,
            LyricsRefreshResponse,
            LyricsResponse,
            LyricsScopeResponse,
            LyricsSourceResponse,
            LyricsWordResponse,
        },
    },
    services::STATE,
    services::{
        auth::{
            require_authenticated,
            require_permission,
        },
        metadata::lyrics as lyrics_service,
    },
};

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
            description = "Preferred language as an ISO-639-1/3 code or language name (e.g. 'en', 'jpn', 'Japanese'). When no stored lyric matches, the server falls back to the best available lyric; inspect the normalized returned `language`."
        )
    )]
    language: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Optional metadata provider ID. Provider selection is request-local and bypasses automatic personal/shared precedence."
        )
    )]
    provider: Option<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct LyricsCandidatesQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Eligibility format: `json` (default), `plain`, or `lrc`.")
    )]
    format: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Preferred language as an ISO-639-1/3 code or language name, as on `GET /api/tracks/{id}/lyrics`. Orders matching candidates first below personal and shared manual lyrics; omits none."
        )
    )]
    language: Option<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct LyricsWriteQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Language code or name for raw LRC and plain text uploads. Normalized to lowercase ISO-639-3; defaults to `und`."
        )
    )]
    language: Option<String>,
}

async fn get_track_lyrics(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<LyricsQuery>,
) -> Result<Response, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    let db = &*STATE.db.read().await;
    let not_found = || AppError::not_found(format!("No lyrics for track: {id}"));

    let track_db_id = lyra_db::lookup::find_node_id_by_id(db, &id)?.ok_or_else(not_found)?;
    crate::services::auth::access::require_entity_accessible(
        db,
        &principal,
        track_db_id,
        not_found,
    )?;
    let track = lyra_db::tracks::get_by_id(db, track_db_id)?.ok_or_else(not_found)?;

    let format = parse_lyrics_format(query.format.as_deref())?;
    let provider_filter =
        crate::routes::providers::parse_provider_filter(query.provider.as_deref())?;
    let require_synced = format == "lrc";
    let candidates = lyrics_service::visible_for_track(db, track_db_id, Some(&principal))?;
    let providers = crate::services::providers::active_providers(db)?;
    let language_hint = lyrics_service::normalize_language_hint(query.language.as_deref())
        .map_err(|error| AppError::bad_request(error.to_string()))?;

    let ranking = lyrics_service::Ranking {
        provider_id: provider_filter,
        language_hint: language_hint.as_deref(),
        require_synced,
    };
    let winner =
        lyrics_service::pick_preferred(&candidates, &providers, ranking, track.duration_ms)
            .ok_or_else(|| {
                let selected_without_format = require_synced
                    && lyrics_service::pick_preferred(
                        &candidates,
                        &providers,
                        lyrics_service::Ranking {
                            require_synced: false,
                            ..ranking
                        },
                        track.duration_ms,
                    )
                    .is_some();
                if selected_without_format {
                    AppError::not_acceptable(format!(
                        "Selected lyrics do not have meaningful synced content for track: {id}"
                    ))
                } else {
                    not_found()
                }
            })?;

    let Some(winner_db_id) = winner.db_id.clone().map(Into::into) else {
        return Err(not_found());
    };
    let detail = lyra_db::lyrics::get_detail(db, winner_db_id)?.ok_or_else(not_found)?;

    match format.as_str() {
        "json" => Ok(lyrics_response_json(detail).into_response()),
        "plain" => Ok(lyrics_response_plain(detail).into_response()),
        "lrc" => Ok(lyrics_response_lrc(detail).into_response()),
        _ => unreachable!("format validated above"),
    }
}

async fn get_track_lyrics_candidates(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<LyricsCandidatesQuery>,
) -> Result<Json<LyricsCandidatesResponse>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    let db = &*STATE.db.read().await;
    let not_found = || AppError::not_found(format!("Track not found: {id}"));
    let track_db_id = lyra_db::lookup::find_node_id_by_id(db, &id)?.ok_or_else(not_found)?;
    crate::services::auth::access::require_entity_accessible(
        db,
        &principal,
        track_db_id,
        not_found,
    )?;
    let track = lyra_db::tracks::get_by_id(db, track_db_id)?
        .ok_or_else(|| AppError::not_found(format!("Track not found: {id}")))?;

    let format = parse_lyrics_format(query.format.as_deref())?;
    let require_synced = format == "lrc";
    let language_hint = lyrics_service::normalize_language_hint(query.language.as_deref())
        .map_err(|error| AppError::bad_request(error.to_string()))?;
    let candidates = lyrics_service::visible_for_track(db, track_db_id, Some(&principal))?;
    let providers = crate::services::providers::active_providers(db)?;
    let ranking = lyrics_service::Ranking {
        language_hint: language_hint.as_deref(),
        require_synced,
        ..lyrics_service::Ranking::default()
    };
    let items = lyrics_service::rank(&candidates, &providers, ranking, track.duration_ms)
        .into_iter()
        .map(|lyrics| lyrics_candidate_summary(lyrics, track.duration_ms))
        .collect();
    Ok(Json(LyricsCandidatesResponse { items }))
}

async fn put_personal_track_lyrics(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<LyricsWriteQuery>,
    body: Bytes,
) -> Result<Json<LyricsResponse>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    let now = lyrics_service::now_ms().map_err(lyrics_upload_error_to_app_error)?;
    let content_type = request_content_type(&headers)?;
    validate_upload_content_type(content_type)?;
    let input = lyrics_service::input_from_upload(content_type, &body, query.language, now)
        .map_err(lyrics_upload_error_to_app_error)?;

    let mut db = STATE.db.write().await;
    let track_db_id = lyra_db::lookup::find_node_id_by_id(&*db, &id)?
        .ok_or_else(|| AppError::not_found(format!("Track not found: {id}")))?;
    crate::services::auth::access::require_entity_accessible(
        &*db,
        &principal,
        track_db_id,
        || AppError::not_found(format!("Track not found: {id}")),
    )?;
    let detail = lyrics_service::upsert_personal_lyrics_by_db_id(
        &mut db,
        track_db_id,
        &id,
        &principal,
        input,
    )
    .map_err(lyrics_upload_error_to_app_error)?;

    Ok(lyrics_response_json(detail))
}

async fn delete_personal_track_lyrics(
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<StatusCode, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    let mut db = STATE.db.write().await;
    let Some(track_db_id) = lyra_db::lookup::find_node_id_by_id(&*db, &id)? else {
        return Ok(StatusCode::NO_CONTENT);
    };
    lyrics_service::delete_personal_lyrics_for_track_by_db_id(&mut db, track_db_id, &principal)
        .map_err(lyrics_upload_error_to_app_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn put_shared_track_lyrics(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<LyricsWriteQuery>,
    body: Bytes,
) -> Result<Json<LyricsResponse>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    require_permission(&principal, lyra_db::Permission::ManageMetadata)?;
    let now = lyrics_service::now_ms().map_err(lyrics_upload_error_to_app_error)?;
    let content_type = request_content_type(&headers)?;
    validate_upload_content_type(content_type)?;
    let input = lyrics_service::input_from_upload(content_type, &body, query.language, now)
        .map_err(lyrics_upload_error_to_app_error)?;
    let mut db = STATE.db.write().await;
    let track_db_id = lyra_db::lookup::find_node_id_by_id(&*db, &id)?
        .ok_or_else(|| AppError::not_found(format!("Track not found: {id}")))?;
    crate::services::auth::access::require_entity_accessible(
        &*db,
        &principal,
        track_db_id,
        || AppError::not_found(format!("Track not found: {id}")),
    )?;
    let detail = lyrics_service::upsert_shared_lyrics_by_db_id(&mut db, track_db_id, input)
        .map_err(lyrics_upload_error_to_app_error)?;
    Ok(lyrics_response_json(detail))
}

async fn delete_shared_track_lyrics(
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<StatusCode, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    require_permission(&principal, lyra_db::Permission::ManageMetadata)?;
    let mut db = STATE.db.write().await;
    let track_db_id = lyra_db::lookup::find_node_id_by_id(&*db, &id)?
        .ok_or_else(|| AppError::not_found(format!("Track not found: {id}")))?;
    crate::services::auth::access::require_entity_accessible(
        &*db,
        &principal,
        track_db_id,
        || AppError::not_found(format!("Track not found: {id}")),
    )?;
    lyrics_service::delete_shared_lyrics_for_track_by_db_id(&mut db, track_db_id)
        .map_err(lyrics_upload_error_to_app_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn refresh_track_lyrics(
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<LyricsRefreshResponse>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    require_permission(&principal, lyra_db::Permission::SyncMetadata)?;
    let track_db_id = {
        let db = &*STATE.db.read().await;
        let track_db_id = lyra_db::lookup::find_node_id_by_id(db, &id)?
            .ok_or_else(|| AppError::not_found(format!("No track: {id}")))?;
        crate::services::auth::access::require_entity_accessible(
            db,
            &principal,
            track_db_id,
            || AppError::not_found(format!("No track: {id}")),
        )?;
        track_db_id
    };

    let outcome =
        lyrics_service::providers::dispatch_for_track_identity(track_db_id, Some(&id), true)
            .await?;
    for (provider_id, error) in &outcome.failed {
        tracing::warn!(
            provider = provider_id.as_ref(),
            track = %id,
            error = %format!("{error:#}"),
            "lyrics refresh failed for provider"
        );
    }
    Ok(Json(LyricsRefreshResponse {
        failed_providers: outcome
            .failed
            .iter()
            .map(|(provider_id, _)| provider_id.to_string())
            .collect(),
        in_progress_providers: outcome
            .in_progress
            .iter()
            .map(ToString::to_string)
            .collect(),
    }))
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

fn validate_upload_content_type(content_type: &str) -> Result<(), AppError> {
    if matches!(
        content_type,
        "application/json" | "application/lrc" | "text/x-lrc" | "text/plain"
    ) {
        Ok(())
    } else {
        Err(AppError::unsupported_media_type(format!(
            "unsupported lyrics Content-Type: {content_type}. Supported: application/json, application/lrc, text/x-lrc, text/plain"
        )))
    }
}

fn parse_lyrics_format(raw: Option<&str>) -> Result<String, AppError> {
    let format = raw
        .map(str::to_ascii_lowercase)
        .unwrap_or_else(|| "json".to_string());
    if matches!(format.as_str(), "json" | "plain" | "lrc") {
        Ok(format)
    } else {
        Err(AppError::bad_request(format!(
            "Unsupported lyrics format: {format}. Supported: json, plain, lrc."
        )))
    }
}

fn lyrics_candidate_summary(
    lyrics: &lyra_db::Lyrics,
    duration_ms: Option<u64>,
) -> LyricsCandidateSummaryResponse {
    LyricsCandidateSummaryResponse {
        id: lyrics.id.clone(),
        provider_id: lyrics.provider_id.clone(),
        provider_lyrics_id: lyrics.provider_lyrics_id.clone(),
        language: lyrics.language.clone(),
        scope: LyricsScopeResponse::from_lyrics(lyrics),
        source: LyricsSourceResponse::from_lyrics(lyrics),
        lrc_available: lyrics_service::has_meaningful_synced(lyrics, duration_ms),
        has_word_cues: lyrics.has_word_cues,
        updated_at: crate::services::timestamps::unix_ms_to_rfc3339_u64(lyrics.updated_at_ms),
    }
}

fn lyrics_response_json(detail: lyra_db::lyrics::LyricsDetail) -> Json<LyricsResponse> {
    let lyra_db::lyrics::LyricsDetail { lyrics, lines } = detail;
    let scope = LyricsScopeResponse::from_lyrics(&lyrics);
    let source = LyricsSourceResponse::from_lyrics(&lyrics);
    let provider_id = lyrics.provider_id.clone();
    let provider_lyrics_id = lyrics.provider_lyrics_id.clone();
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
        provider_id,
        provider_lyrics_id,
        language: lyrics.language,
        scope,
        source,
        plain_text: lyrics.plain_text,
        has_word_cues: lyrics.has_word_cues,
        updated_at: crate::services::timestamps::unix_ms_to_rfc3339_u64(lyrics.updated_at_ms),
        lines: response_lines,
    })
}

fn lyrics_response_plain(detail: lyra_db::lyrics::LyricsDetail) -> Response {
    let language = detail.lyrics.language.clone();
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
    plain_text_response("text/plain; charset=utf-8", body, &language)
}

const LRC_MAX_TS_MS: u64 = 99 * 60 * 1000 + 59 * 1000 + 990;

fn lyrics_response_lrc(detail: lyra_db::lyrics::LyricsDetail) -> Response {
    let language = detail.lyrics.language.clone();
    let mut body = String::new();
    for line in &detail.lines {
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
    plain_text_response("application/lrc; charset=utf-8", body, &language)
}

fn plain_text_response(content_type: &'static str, body: String, language: &str) -> Response {
    let mut response = (StatusCode::OK, body).into_response();
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    if let Ok(value) = HeaderValue::from_str(language) {
        response
            .headers_mut()
            .insert(header::CONTENT_LANGUAGE, value);
    }
    response
}

pub(super) fn routes() -> Router {
    Router::new()
        .route("/{id}/lyrics", get(get_track_lyrics))
        .route("/{id}/lyrics/candidates", get(get_track_lyrics_candidates))
        .route("/{id}/lyrics/personal", put(put_personal_track_lyrics))
        .route(
            "/{id}/lyrics/personal",
            delete(delete_personal_track_lyrics),
        )
        .route("/{id}/lyrics/shared", put(put_shared_track_lyrics))
        .route("/{id}/lyrics/shared", delete(delete_shared_track_lyrics))
        .route("/{id}/lyrics/refresh", post(refresh_track_lyrics))
}

#[cfg(feature = "docgen")]
pub(super) fn openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::{
        delete_with,
        get_with,
        post_with,
        put_with,
    };

    aide::axum::ApiRouter::new()
        .api_route(
            "/{id}/lyrics",
            get_with(get_track_lyrics, get_track_lyrics_docs),
        )
        .api_route(
            "/{id}/lyrics/candidates",
            get_with(
                get_track_lyrics_candidates,
                get_track_lyrics_candidates_docs,
            ),
        )
        .api_route(
            "/{id}/lyrics/personal",
            put_with(put_personal_track_lyrics, put_personal_track_lyrics_docs),
        )
        .api_route(
            "/{id}/lyrics/personal",
            delete_with(
                delete_personal_track_lyrics,
                delete_personal_track_lyrics_docs,
            ),
        )
        .api_route(
            "/{id}/lyrics/shared",
            put_with(put_shared_track_lyrics, put_shared_track_lyrics_docs),
        )
        .api_route(
            "/{id}/lyrics/shared",
            delete_with(delete_shared_track_lyrics, delete_shared_track_lyrics_docs),
        )
        .api_route(
            "/{id}/lyrics/refresh",
            post_with(refresh_track_lyrics, refresh_track_lyrics_docs),
        )
}

#[cfg(feature = "docgen")]
fn get_track_lyrics_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get track lyrics").description(
        "Returns the best visible stored lyrics for a track. Automatic precedence is the caller's \
         personal manual lyrics, shared manual lyrics, then enabled providers; a personal lyric \
         remains preferred even when it does not match `language`. \
         `?format=json|plain|lrc` controls the response format (default json). Use `?language=` to \
         prefer an ISO-639-1/3 code or language name; when no stored lyric matches, \
         the server falls back to the best available lyric; the `language` field on the \
         response and `Content-Language` on text responses reveal what was actually served. \
         `provider=<provider_id>` selects that enabled provider for this request and bypasses \
         automatic manual precedence. `LyricsWordResponse.char_start` / \
         `char_end` are Unicode-scalar (code point) offsets into the line's `text`, not \
         byte offsets. Returns 404 when no lyrics are stored; 406 when `lrc` is requested \
         but no candidate has synced content meeting the selector's coverage threshold.",
    )
}

#[cfg(feature = "docgen")]
fn get_track_lyrics_candidates_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List visible lyrics candidates").description(
        "Enumerates stored candidates currently visible and eligible for the caller, best first. \
         For the same `language` and `format`, and with no `provider`, the order matches \
         `GET /api/tracks/{id}/lyrics`, whose response is the first candidate. The list contains \
         enabled-provider rows, the shared manual row, and only the caller's personal row. \
         `format=lrc` omits candidates without meaningful synced coverage. Returns 400 for an \
         unsupported `format` or an unrecognized `language`, and 404 if the track does not exist \
         or is hidden from the caller.",
    )
}

#[cfg(feature = "docgen")]
fn put_personal_track_lyrics_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Write personal track lyrics").description(
        "Creates or replaces the authenticated user's personal lyrics for a visible track. \
         The request body is selected by `Content-Type`: `application/json` accepts structured \
         lyrics JSON, `application/lrc` and `text/x-lrc` accept raw LRC text, and `text/plain` \
         stores non-timestamped plain text. Raw uploads use `?language=` for the stored language, \
         defaulting to `und`. All formats use a server-controlled singleton identity and are \
         visible only to their owner.",
    )
}

#[cfg(feature = "docgen")]
fn delete_personal_track_lyrics_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Delete personal track lyrics")
        .description(
            "Deletes only the caller's personal lyrics. The operation remains available after the \
             caller loses track visibility and is idempotent.",
        )
        .response::<204, ()>()
}

#[cfg(feature = "docgen")]
fn put_shared_track_lyrics_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Write shared track lyrics").description(
        "Creates or replaces the track's singleton shared manual lyrics. Requires \
         `ManageMetadata` and track access. The server controls the manual identity, and the \
         REST surface cannot impersonate a metadata provider.",
    )
}

#[cfg(feature = "docgen")]
fn delete_shared_track_lyrics_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Delete shared track lyrics")
        .description(
            "Deletes only the shared manual lyrics. Requires `ManageMetadata` and track access; \
             personal and provider rows are left intact. Idempotent.",
        )
        .response::<204, ()>()
}

#[cfg(feature = "docgen")]
fn refresh_track_lyrics_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Refresh track lyrics").description(
        "Re-runs every registered lyrics provider for the track with `force_refresh=true`, \
         bypassing each provider's negative cache. Awaits all dispatches before returning. \
         Requires `SyncMetadata`, returning 403 without it, and 404 if the track does not exist \
         or is hidden from the caller. The response carries no lyrics; read what was stored from \
         `GET /api/tracks/{id}/lyrics/candidates` or `GET /api/tracks/{id}/lyrics`. \
         A 200 can list every provider in `failed_providers`. Both lists empty means no errors \
         were reported, not that every provider returned lyrics: misses, rate-limited results, \
         rejected hits, unmet handler requirements, and cancelled handlers are not reported.",
    )
}

#[cfg(test)]
mod tests {
    use agdb::DbId;

    use super::*;
    use crate::services::testing::runtime_test_lock;
    use lyra_db::fixtures::{
        connect,
        insert_release,
        insert_track,
    };
    use lyra_db::test_db::insert_library;

    async fn bearer_headers_for_user(user_db_id: DbId) -> anyhow::Result<HeaderMap> {
        let session =
            crate::services::testing::create_session(user_db_id, Default::default()).await?;
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            format!("Bearer {}", session.token).parse()?,
        );
        Ok(headers)
    }

    fn lyrics_upload_headers(mut headers: HeaderMap) -> HeaderMap {
        headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plain"));
        headers
    }

    fn route_ok<T>(result: Result<T, AppError>) -> anyhow::Result<T> {
        result.map_err(|error| anyhow::anyhow!("{error:?}"))
    }

    fn route_error_status<T>(result: Result<T, AppError>) -> anyhow::Result<StatusCode> {
        match result {
            Ok(_) => anyhow::bail!("route unexpectedly succeeded"),
            Err(error) => Ok(error.into_response().status()),
        }
    }

    fn create_user_with_permissions(
        db: &mut agdb::DbAny,
        username: &str,
        permissions: Vec<lyra_db::Permission>,
    ) -> anyhow::Result<DbId> {
        let user_id = lyra_db::users::create(db, &lyra_db::fixtures::test_user(username)?)?;
        let role_name = format!("{username}-role");
        lyra_db::roles::create(
            db,
            &lyra_db::roles::Role {
                db_id: None,
                id: nanoid::nanoid!(),
                name: role_name.clone(),
                permissions,
            },
        )?;
        lyra_db::roles::ensure_user_has_role(db, user_id, &role_name)?;
        Ok(user_id)
    }

    /// A 4 s track in its own library, returning its ids and the library's.
    fn insert_library_track(
        db: &mut agdb::DbAny,
        name: &str,
    ) -> anyhow::Result<(DbId, String, DbId)> {
        let library = insert_library(db, name, &format!("/tmp/lyra-{name}"))?;
        let release = insert_release(db, name)?;
        let track = insert_track(db, name)?;
        connect(db, library, release)?;
        connect(db, release, track)?;
        let mut track_row = lyra_db::tracks::get_by_id(db, track)?
            .ok_or_else(|| anyhow::anyhow!("track missing"))?;
        track_row.duration_ms = Some(4_000);
        lyra_db::tracks::update(db, &track_row)?;
        Ok((track, track_row.id, library))
    }

    fn grant_library(db: &mut agdb::DbAny, user: DbId, library: DbId) -> anyhow::Result<()> {
        lyra_db::libraries::grant_access(
            db,
            user,
            library,
            lyra_db::libraries::AccessKind::ReadWrite,
        )
    }

    /// Synced lyrics from an enabled provider at `position`.
    fn insert_provider_lyrics(
        db: &mut agdb::DbAny,
        track: DbId,
        provider_id: &str,
        position: u32,
        language: &str,
        words: Vec<lyra_db::lyrics::WordInput>,
    ) -> anyhow::Result<()> {
        lyra_db::providers::upsert(
            db,
            &lyra_db::ProviderConfig {
                db_id: None,
                provider_id: provider_id.to_string(),
                position,
                enabled: true,
            },
        )?;
        lyra_db::lyrics::upsert_from_plugin(
            db,
            track,
            lyra_db::lyrics::LyricsInput {
                language: language.to_string(),
                plain_text: format!("{provider_id} lyrics"),
                lines: vec![
                    lyra_db::lyrics::LineInput {
                        ts_ms: 1_000,
                        text: "line one".to_string(),
                        words,
                    },
                    lyra_db::lyrics::LineInput {
                        ts_ms: 3_000,
                        text: "line two".to_string(),
                        words: Vec::new(),
                    },
                ],
                last_checked_at_ms: 1,
            },
            format!("{provider_id}-row"),
            provider_id,
            Some(4_000),
        )?;
        Ok(())
    }

    async fn install_lyrics_handler(
        provider_id: &str,
        callback: crate::services::plugin_dispatch::PluginCallbackHandle,
    ) -> anyhow::Result<()> {
        lyra_db::providers::register(&mut *STATE.db.write().await, provider_id)?;
        let registry = crate::services::providers::provider_registry();
        let mut registry = registry.write().await;
        registry.register(
            crate::services::plugin_scope::PluginId::new("lyrics_routes_plugin")?,
            provider_id.to_string(),
            None,
        )?;
        registry
            .set_lyrics_handler(
                provider_id,
                crate::services::providers::ProviderLyricsSpec::new(
                    std::time::Duration::from_secs(5),
                    Default::default(),
                    callback,
                ),
            )
            .ok_or_else(|| anyhow::anyhow!("lyrics handler not stored"))?;
        Ok(())
    }

    async fn candidates(
        headers: &HeaderMap,
        track_id: &str,
        language: Option<&str>,
    ) -> Result<Json<LyricsCandidatesResponse>, AppError> {
        get_track_lyrics_candidates(
            headers.clone(),
            Path(track_id.to_string()),
            Query(LyricsCandidatesQuery {
                format: None,
                language: language.map(str::to_string),
            }),
        )
        .await
    }

    #[tokio::test]
    async fn refresh_requires_sync_metadata_before_track_access() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        super::super::setup_route_test().await?;
        let (track_id, listener, hidden_syncer) = {
            let mut db = STATE.db.write().await;
            lyra_db::roles::ensure_builtin_roles(&mut db)?;
            let (_, track_id, library) = insert_library_track(&mut db, "refresh-access")?;
            let listener = lyra_db::users::create(
                &mut db,
                &lyra_db::fixtures::test_user("refresh-listener")?,
            )?;
            grant_library(&mut db, listener, library)?;
            let hidden_syncer = create_user_with_permissions(
                &mut db,
                "refresh-hidden-syncer",
                vec![lyra_db::Permission::SyncMetadata],
            )?;
            (track_id, listener, hidden_syncer)
        };

        let listener_headers = bearer_headers_for_user(listener).await?;
        for id in [track_id.clone(), "missing-track-public-id".to_string()] {
            assert_eq!(
                route_error_status(refresh_track_lyrics(listener_headers.clone(), Path(id)).await)?,
                StatusCode::FORBIDDEN
            );
        }
        assert_eq!(
            route_error_status(
                refresh_track_lyrics(
                    bearer_headers_for_user(hidden_syncer).await?,
                    Path(track_id),
                )
                .await
            )?,
            StatusCode::NOT_FOUND
        );
        Ok(())
    }

    #[tokio::test]
    async fn refresh_reports_failed_and_in_progress_providers() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        super::super::setup_route_test().await?;
        let dispatch = crate::services::testing::FakePluginDispatch::install();
        let (track, track_id, syncer) = {
            let mut db = STATE.db.write().await;
            lyra_db::roles::ensure_builtin_roles(&mut db)?;
            let (track, track_id, library) = insert_library_track(&mut db, "refresh-outcome")?;
            let syncer = create_user_with_permissions(
                &mut db,
                "refresh-syncer",
                vec![lyra_db::Permission::SyncMetadata],
            )?;
            grant_library(&mut db, syncer, library)?;
            (track, track_id, syncer)
        };
        let failing = dispatch.add(crate::services::testing::FakeResponse::Error(
            "upstream 503".to_string(),
        ));
        let busy = dispatch.add(crate::services::testing::FakeResponse::Stall);
        let quiet = dispatch.add(crate::services::testing::FakeResponse::Value(
            serde_json::json!({ "kind": "miss" }),
        ));
        install_lyrics_handler("refresh_failing", failing).await?;
        install_lyrics_handler("refresh_busy", busy).await?;
        install_lyrics_handler("refresh_quiet", quiet).await?;
        crate::services::providers::lyrics_in_flight()
            .lock()
            .expect("in-flight mutex")
            .insert((track.0, std::sync::Arc::from("refresh_busy")));

        let headers = bearer_headers_for_user(syncer).await?;
        let Json(response) = route_ok(refresh_track_lyrics(headers, Path(track_id)).await)?;
        assert_eq!(
            serde_json::to_value(&response)?,
            serde_json::json!({
                "failed_providers": ["refresh_failing"],
                "in_progress_providers": ["refresh_busy"],
            })
        );
        assert!(dispatch.calls(busy).is_empty());
        assert_eq!(dispatch.calls(quiet).len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn candidates_follow_language_like_get_lyrics() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        super::super::setup_route_test().await?;
        let (track_id, reader) = {
            let mut db = STATE.db.write().await;
            lyra_db::roles::ensure_builtin_roles(&mut db)?;
            let (track, track_id, library) = insert_library_track(&mut db, "candidates-language")?;
            let reader = lyra_db::users::create(
                &mut db,
                &lyra_db::fixtures::test_user("candidates-reader")?,
            )?;
            grant_library(&mut db, reader, library)?;
            insert_provider_lyrics(&mut db, track, "first_provider", 0, "eng", Vec::new())?;
            insert_provider_lyrics(
                &mut db,
                track,
                "second_provider",
                1,
                "jpn",
                vec![lyra_db::lyrics::WordInput {
                    ts_ms: 1_000,
                    char_start: 0,
                    char_end: 4,
                }],
            )?;
            (track_id, reader)
        };
        let headers = bearer_headers_for_user(reader).await?;

        for (language, expected_first) in [
            (None, "first_provider"),
            (Some("eng"), "first_provider"),
            (Some("Japanese"), "second_provider"),
        ] {
            let Json(listed) = route_ok(candidates(&headers, &track_id, language).await)?;
            assert_eq!(listed.items.len(), 2);
            assert_eq!(listed.items[0].provider_id.as_deref(), Some(expected_first));

            let served = route_ok(
                get_track_lyrics(
                    headers.clone(),
                    Path(track_id.clone()),
                    Query(LyricsQuery {
                        format: None,
                        language: language.map(str::to_string),
                        provider: None,
                    }),
                )
                .await,
            )?;
            let served: serde_json::Value = serde_json::from_slice(
                &axum::body::to_bytes(served.into_body(), usize::MAX).await?,
            )?;
            assert_eq!(served["id"], listed.items[0].id);
        }

        let Json(listed) = route_ok(candidates(&headers, &track_id, None).await)?;
        let word_cues = |provider: &str| {
            listed
                .items
                .iter()
                .find(|item| item.provider_id.as_deref() == Some(provider))
                .map(|item| item.has_word_cues)
        };
        assert_eq!(word_cues("first_provider"), Some(false));
        assert_eq!(word_cues("second_provider"), Some(true));
        Ok(())
    }

    #[tokio::test]
    async fn candidates_reject_bad_language_after_the_access_check() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        super::super::setup_route_test().await?;
        let (track_id, reader, outsider) = {
            let mut db = STATE.db.write().await;
            lyra_db::roles::ensure_builtin_roles(&mut db)?;
            let (_, track_id, library) = insert_library_track(&mut db, "candidates-bad-language")?;
            let reader = lyra_db::users::create(
                &mut db,
                &lyra_db::fixtures::test_user("bad-language-reader")?,
            )?;
            let outsider = lyra_db::users::create(
                &mut db,
                &lyra_db::fixtures::test_user("bad-language-outsider")?,
            )?;
            grant_library(&mut db, reader, library)?;
            (track_id, reader, outsider)
        };

        for (user, expected) in [
            (reader, StatusCode::BAD_REQUEST),
            (outsider, StatusCode::NOT_FOUND),
        ] {
            let headers = bearer_headers_for_user(user).await?;
            assert_eq!(
                route_error_status(candidates(&headers, &track_id, Some("not-a-language")).await)?,
                expected
            );
        }
        Ok(())
    }

    #[test]
    fn language_hints_follow_locale_normalization() {
        assert_eq!(
            lyrics_service::normalize_language_hint(Some("Japanese")).unwrap(),
            Some("jpn".to_string())
        );
        assert_eq!(
            lyrics_service::normalize_language_hint(Some("en")).unwrap(),
            Some("eng".to_string())
        );
        assert!(lyrics_service::normalize_language_hint(Some("not-a-language")).is_err());
    }

    #[tokio::test]
    async fn routes_enforce_ownership_visibility_and_shared_permissions() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        super::super::setup_route_test().await?;

        let (track_db_id, track_public_id, library_db_id, alice_id, bob_id, admin_id) = {
            let mut db = STATE.db.write().await;
            lyra_db::roles::ensure_builtin_roles(&mut db)?;
            let library = insert_library(&mut db, "Lyrics Routes", "/tmp/lyra-lyrics-routes")?;
            let release = insert_release(&mut db, "Lyrics Release")?;
            let track = insert_track(&mut db, "Lyrics Track")?;
            connect(&mut db, library, release)?;
            connect(&mut db, release, track)?;
            let mut track_row = lyra_db::tracks::get_by_id(&db, track)?
                .ok_or_else(|| anyhow::anyhow!("track missing"))?;
            track_row.duration_ms = Some(4_000);
            lyra_db::tracks::update(&mut db, &track_row)?;

            let alice = lyra_db::fixtures::test_user("lyrics-alice")?;
            let bob = lyra_db::fixtures::test_user("lyrics-bob")?;
            let admin = lyra_db::fixtures::test_user("lyrics-admin")?;
            let alice_id = lyra_db::users::create(&mut db, &alice)?;
            let bob_id = lyra_db::users::create(&mut db, &bob)?;
            let admin_id = lyra_db::users::create(&mut db, &admin)?;
            lyra_db::libraries::grant_access(
                &mut db,
                alice_id,
                library,
                lyra_db::libraries::AccessKind::ReadWrite,
            )?;
            lyra_db::libraries::grant_access(
                &mut db,
                bob_id,
                library,
                lyra_db::libraries::AccessKind::ReadWrite,
            )?;
            lyra_db::roles::ensure_user_has_role(
                &mut db,
                admin_id,
                lyra_db::roles::BUILTIN_ADMIN_ROLE,
            )?;

            lyra_db::providers::upsert(
                &mut db,
                &lyra_db::ProviderConfig {
                    db_id: None,
                    provider_id: "route-provider".to_string(),
                    position: 0,
                    enabled: true,
                },
            )?;
            lyra_db::lyrics::upsert_from_plugin(
                &mut db,
                track,
                lyra_db::lyrics::LyricsInput {
                    language: "eng".to_string(),
                    plain_text: "provider lyrics".to_string(),
                    lines: vec![
                        lyra_db::lyrics::LineInput {
                            ts_ms: 1_000,
                            text: "provider one".to_string(),
                            words: Vec::new(),
                        },
                        lyra_db::lyrics::LineInput {
                            ts_ms: 3_000,
                            text: "provider two".to_string(),
                            words: Vec::new(),
                        },
                    ],
                    last_checked_at_ms: 1,
                },
                "provider-row".to_string(),
                "route-provider",
                Some(4_000),
            )?;
            (track, track_row.id, library, alice_id, bob_id, admin_id)
        };

        let alice_headers = bearer_headers_for_user(alice_id).await?;
        let bob_headers = bearer_headers_for_user(bob_id).await?;
        let admin_headers = bearer_headers_for_user(admin_id).await?;

        let _ = route_ok(
            put_personal_track_lyrics(
                lyrics_upload_headers(alice_headers.clone()),
                Path(track_public_id.clone()),
                Query(LyricsWriteQuery {
                    language: Some("French".to_string()),
                }),
                Bytes::from_static(b"alice lyrics"),
            )
            .await,
        )?;
        let _ = route_ok(
            put_personal_track_lyrics(
                lyrics_upload_headers(bob_headers.clone()),
                Path(track_public_id.clone()),
                Query(LyricsWriteQuery {
                    language: Some("eng".to_string()),
                }),
                Bytes::from_static(b"bob lyrics"),
            )
            .await,
        )?;

        let forbidden = match put_shared_track_lyrics(
            lyrics_upload_headers(alice_headers.clone()),
            Path(track_public_id.clone()),
            Query(LyricsWriteQuery {
                language: Some("eng".to_string()),
            }),
            Bytes::from_static(b"shared lyrics"),
        )
        .await
        {
            Ok(_) => anyhow::bail!("ordinary user wrote shared lyrics"),
            Err(error) => error.into_response(),
        };
        assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);

        let _ = route_ok(
            put_shared_track_lyrics(
                lyrics_upload_headers(admin_headers),
                Path(track_public_id.clone()),
                Query(LyricsWriteQuery {
                    language: Some("eng".to_string()),
                }),
                Bytes::from_static(b"shared lyrics"),
            )
            .await,
        )?;

        let Json(candidates) = route_ok(
            get_track_lyrics_candidates(
                alice_headers.clone(),
                Path(track_public_id.clone()),
                Query(LyricsCandidatesQuery {
                    format: None,
                    language: None,
                }),
            )
            .await,
        )?;
        assert_eq!(candidates.items.len(), 3);
        assert!(
            matches!(
                (
                    candidates.items[0].scope,
                    candidates.items[1].source,
                    candidates.items[2].source,
                ),
                (
                    LyricsScopeResponse::Personal,
                    LyricsSourceResponse::Manual,
                    LyricsSourceResponse::Provider,
                )
            ),
            "candidates come best first"
        );

        let provider_response = route_ok(
            get_track_lyrics(
                alice_headers.clone(),
                Path(track_public_id.clone()),
                Query(LyricsQuery {
                    format: Some("json".to_string()),
                    language: None,
                    provider: Some("route-provider".to_string()),
                }),
            )
            .await,
        )?;
        let provider_body = axum::body::to_bytes(provider_response.into_body(), usize::MAX).await?;
        let provider_json: serde_json::Value = serde_json::from_slice(&provider_body)?;
        assert_eq!(provider_json["plain_text"], "provider lyrics");
        assert_eq!(provider_json["provider_id"], "route-provider");
        assert_eq!(provider_json["provider_lyrics_id"], "provider-row");
        assert_ne!(provider_json["id"], "provider-row");

        let fallback_response = route_ok(
            get_track_lyrics(
                alice_headers.clone(),
                Path(track_public_id.clone()),
                Query(LyricsQuery {
                    format: Some("plain".to_string()),
                    language: Some("Japanese".to_string()),
                    provider: None,
                }),
            )
            .await,
        )?;
        assert_eq!(
            fallback_response
                .headers()
                .get(header::CONTENT_LANGUAGE)
                .unwrap(),
            "fra"
        );

        let Json(lrc_candidates) = route_ok(
            get_track_lyrics_candidates(
                alice_headers.clone(),
                Path(track_public_id.clone()),
                Query(LyricsCandidatesQuery {
                    format: Some("lrc".to_string()),
                    language: None,
                }),
            )
            .await,
        )?;
        assert_eq!(lrc_candidates.items.len(), 1);
        assert_eq!(
            lrc_candidates.items[0].provider_id.as_deref(),
            Some("route-provider")
        );
        assert_eq!(
            lrc_candidates.items[0].provider_lyrics_id.as_deref(),
            Some("provider-row")
        );

        {
            let mut db = STATE.db.write().await;
            lyra_db::libraries::revoke_access(&mut db, alice_id, library_db_id)?;
        }
        assert_eq!(
            route_ok(
                delete_personal_track_lyrics(alice_headers.clone(), Path(track_public_id.clone()))
                    .await
            )?,
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            route_ok(
                delete_personal_track_lyrics(
                    alice_headers,
                    Path("missing-track-public-id".to_string()),
                )
                .await
            )?,
            StatusCode::NO_CONTENT
        );
        let db = STATE.db.read().await;
        assert!(lyra_db::lyrics::find_personal(&*db, track_db_id, alice_id)?.is_none());
        assert!(lyra_db::lyrics::find_personal(&*db, track_db_id, bob_id)?.is_some());
        Ok(())
    }
}
