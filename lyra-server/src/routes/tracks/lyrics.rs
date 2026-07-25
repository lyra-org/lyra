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

use crate::{
    STATE,
    db,
    routes::{
        AppError,
        responses::{
            LyricsCandidateSummaryResponse,
            LyricsCandidatesResponse,
            LyricsLineResponse,
            LyricsResponse,
            LyricsScopeResponse,
            LyricsSourceResponse,
            LyricsWordResponse,
        },
    },
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
    let principal = require_authenticated(&headers).await?;
    let db = &*STATE.db.read().await;
    let not_found = || AppError::not_found(format!("No lyrics for track: {id}"));

    let track_db_id = db::lookup::find_node_id_by_id(db, &id)?.ok_or_else(not_found)?;
    crate::services::auth::access::require_entity_accessible(
        db,
        &principal,
        track_db_id,
        not_found,
    )?;
    let track = db::tracks::get_by_id(db, track_db_id)?.ok_or_else(not_found)?;

    let format = parse_lyrics_format(query.format.as_deref())?;
    if let Some(provider_id) = query.provider.as_deref() {
        db::lyrics::validate_provider_id(provider_id)
            .map_err(|error| AppError::bad_request(error.to_string()))?;
    }
    let require_synced = format == "lrc";
    let candidates =
        db::lyrics::get_visible_for_track(db, track_db_id, Some(&principal.user_public_id))?;
    let providers = db::providers::get(db)?;
    let language_hint = lyrics_service::normalize_language_hint(query.language.as_deref())
        .map_err(|error| AppError::bad_request(error.to_string()))?;

    let winner = lyrics_service::pick_preferred(
        &candidates,
        &providers,
        Some(&principal.user_public_id),
        query.provider.as_deref(),
        language_hint.as_deref(),
        track.duration_ms,
        require_synced,
    )
    .ok_or_else(|| {
        let selected_without_format = require_synced
            && lyrics_service::pick_preferred(
                &candidates,
                &providers,
                Some(&principal.user_public_id),
                query.provider.as_deref(),
                language_hint.as_deref(),
                track.duration_ms,
                false,
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
    let detail = db::lyrics::get_detail(db, winner_db_id)?.ok_or_else(not_found)?;

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
    let principal = require_authenticated(&headers).await?;
    let db = &*STATE.db.read().await;
    let not_found = || AppError::not_found(format!("Track not found: {id}"));
    let track_db_id = db::lookup::find_node_id_by_id(db, &id)?.ok_or_else(not_found)?;
    crate::services::auth::access::require_entity_accessible(
        db,
        &principal,
        track_db_id,
        not_found,
    )?;
    let track = db::tracks::get_by_id(db, track_db_id)?
        .ok_or_else(|| AppError::not_found(format!("Track not found: {id}")))?;

    let format = parse_lyrics_format(query.format.as_deref())?;
    let require_synced = format == "lrc";
    let candidates =
        db::lyrics::get_visible_for_track(db, track_db_id, Some(&principal.user_public_id))?;
    let providers = db::providers::get(db)?;
    let mut items: Vec<_> = lyrics_service::eligible_candidates(
        &candidates,
        &providers,
        Some(&principal.user_public_id),
        require_synced,
        track.duration_ms,
    )
    .into_iter()
    .map(|lyrics| lyrics_candidate_summary(lyrics, track.duration_ms))
    .collect();
    items.sort_by(|a, b| {
        a.provider_id
            .cmp(&b.provider_id)
            .then_with(|| a.id.cmp(&b.id))
    });
    Ok(Json(LyricsCandidatesResponse { items }))
}

async fn put_personal_track_lyrics(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<LyricsWriteQuery>,
    body: Bytes,
) -> Result<Json<LyricsResponse>, AppError> {
    let principal = require_authenticated(&headers).await?;
    let now = lyrics_service::now_ms().map_err(lyrics_upload_error_to_app_error)?;
    let content_type = request_content_type(&headers)?;
    validate_upload_content_type(content_type)?;
    let input = lyrics_service::input_from_upload(content_type, &body, query.language, now)
        .map_err(lyrics_upload_error_to_app_error)?;

    let mut db = STATE.db.write().await;
    let track_db_id = db::lookup::find_node_id_by_id(&*db, &id)?
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
        &principal.user_public_id,
        input,
    )
    .map_err(lyrics_upload_error_to_app_error)?;

    Ok(lyrics_response_json(detail))
}

async fn delete_personal_track_lyrics(
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<StatusCode, AppError> {
    let principal = require_authenticated(&headers).await?;
    let mut db = STATE.db.write().await;
    let Some(track_db_id) = db::lookup::find_node_id_by_id(&*db, &id)? else {
        return Ok(StatusCode::NO_CONTENT);
    };
    lyrics_service::delete_personal_lyrics_for_track_by_db_id(
        &mut db,
        track_db_id,
        &principal.user_public_id,
    )
    .map_err(lyrics_upload_error_to_app_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn put_shared_track_lyrics(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<LyricsWriteQuery>,
    body: Bytes,
) -> Result<Json<LyricsResponse>, AppError> {
    let principal = require_authenticated(&headers).await?;
    require_permission(&principal, db::Permission::ManageMetadata)?;
    let now = lyrics_service::now_ms().map_err(lyrics_upload_error_to_app_error)?;
    let content_type = request_content_type(&headers)?;
    validate_upload_content_type(content_type)?;
    let input = lyrics_service::input_from_upload(content_type, &body, query.language, now)
        .map_err(lyrics_upload_error_to_app_error)?;
    let mut db = STATE.db.write().await;
    let track_db_id = db::lookup::find_node_id_by_id(&*db, &id)?
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
    let principal = require_authenticated(&headers).await?;
    require_permission(&principal, db::Permission::ManageMetadata)?;
    let mut db = STATE.db.write().await;
    let track_db_id = db::lookup::find_node_id_by_id(&*db, &id)?
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
) -> Result<StatusCode, AppError> {
    let principal = require_authenticated(&headers).await?;
    let track_db_id = {
        let db = &*STATE.db.read().await;
        let track_db_id = db::lookup::find_node_id_by_id(db, &id)?
            .ok_or_else(|| AppError::not_found(format!("No track: {id}")))?;
        crate::services::auth::access::require_entity_accessible(
            db,
            &principal,
            track_db_id,
            || AppError::not_found(format!("No track: {id}")),
        )?;
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
    lyrics: &db::Lyrics,
    duration_ms: Option<u64>,
) -> LyricsCandidateSummaryResponse {
    LyricsCandidateSummaryResponse {
        id: lyrics.id.clone(),
        provider_id: lyrics.provider_id.clone(),
        language: lyrics.language.clone(),
        scope: LyricsScopeResponse::from_lyrics(lyrics),
        source: LyricsSourceResponse::from_lyrics(lyrics),
        lrc_available: lyrics_service::has_meaningful_synced(lyrics, duration_ms),
        updated_at: super::super::unix_ms_to_rfc3339_u64(lyrics.updated_at),
    }
}

fn lyrics_response_json(detail: db::lyrics::LyricsDetail) -> Json<LyricsResponse> {
    let db::lyrics::LyricsDetail { lyrics, lines } = detail;
    let scope = LyricsScopeResponse::from_lyrics(&lyrics);
    let source = LyricsSourceResponse::from_lyrics(&lyrics);
    let provider_id = lyrics.provider_id.clone();
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
        language: lyrics.language,
        scope,
        source,
        plain_text: lyrics.plain_text,
        has_word_cues: lyrics.has_word_cues,
        updated_at: super::super::unix_ms_to_rfc3339_u64(lyrics.updated_at),
        lines: response_lines,
    })
}

fn lyrics_response_plain(detail: db::lyrics::LyricsDetail) -> Response {
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

fn lyrics_response_lrc(detail: db::lyrics::LyricsDetail) -> Response {
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
        "Enumerates stored candidates currently visible and eligible for the caller. The list \
         contains enabled-provider rows, the shared manual row, and only the caller's personal \
         row. `format=lrc` omits candidates without meaningful synced coverage.",
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
    op.summary("Refresh track lyrics")
        .description(
            "Re-runs every registered lyrics provider for the track with `force_refresh=true`, \
             bypassing each provider's negative cache. Awaits all dispatches before returning. \
             Returns 204 once dispatch completes; the caller should then GET the lyrics to read \
             what was stored. 404 if the track does not exist.",
        )
        .response::<204, ()>()
}

#[cfg(test)]
mod tests {
    use agdb::DbId;

    use super::*;
    use crate::{
        db::test_db::{
            connect,
            insert_library,
            insert_release,
            insert_track,
        },
        testing::runtime_test_lock,
    };

    async fn bearer_headers_for_user(user_db_id: DbId) -> anyhow::Result<HeaderMap> {
        let session = crate::services::auth::sessions::create_session_for_user(
            user_db_id,
            Default::default(),
        )
        .await?;
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

    #[test]
    fn text_responses_report_returned_language() {
        let response = plain_text_response("text/plain; charset=utf-8", "hello".into(), "fra");
        assert_eq!(
            response.headers().get(header::CONTENT_LANGUAGE).unwrap(),
            "fra"
        );
    }

    #[tokio::test]
    async fn routes_enforce_ownership_visibility_and_shared_permissions() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        super::super::setup_route_test().await?;

        let (track_db_id, track_public_id, library_db_id, alice_id, bob_id, admin_id) = {
            let mut db = STATE.db.write().await;
            db::roles::ensure_builtin_roles(&mut db)?;
            let library = insert_library(&mut db, "Lyrics Routes", "/tmp/lyra-lyrics-routes")?;
            let release = insert_release(&mut db, "Lyrics Release")?;
            let track = insert_track(&mut db, "Lyrics Track")?;
            connect(&mut db, library, release)?;
            connect(&mut db, release, track)?;
            let mut track_row = db::tracks::get_by_id(&db, track)?
                .ok_or_else(|| anyhow::anyhow!("track missing"))?;
            track_row.duration_ms = Some(4_000);
            db::tracks::update(&mut db, &track_row)?;

            let alice = db::test_db::test_user("lyrics-alice")?;
            let bob = db::test_db::test_user("lyrics-bob")?;
            let admin = db::test_db::test_user("lyrics-admin")?;
            let alice_id = db::users::create(&mut db, &alice)?;
            let bob_id = db::users::create(&mut db, &bob)?;
            let admin_id = db::users::create(&mut db, &admin)?;
            db::libraries::grant_access(
                &mut db,
                alice_id,
                library,
                db::libraries::AccessKind::ReadWrite,
            )?;
            db::libraries::grant_access(
                &mut db,
                bob_id,
                library,
                db::libraries::AccessKind::ReadWrite,
            )?;
            db::roles::ensure_user_has_role(&mut db, admin_id, db::roles::BUILTIN_ADMIN_ROLE)?;

            db::providers::upsert(
                &mut db,
                &db::ProviderConfig {
                    db_id: None,
                    provider_id: "route-provider".to_string(),
                    display_name: "Route Provider".to_string(),
                    priority: 50,
                    enabled: true,
                },
            )?;
            db::lyrics::upsert_from_plugin(
                &mut db,
                track,
                db::lyrics::LyricsInput {
                    language: "eng".to_string(),
                    plain_text: "provider lyrics".to_string(),
                    lines: vec![
                        db::lyrics::LineInput {
                            ts_ms: 1_000,
                            text: "provider one".to_string(),
                            words: Vec::new(),
                        },
                        db::lyrics::LineInput {
                            ts_ms: 3_000,
                            text: "provider two".to_string(),
                            words: Vec::new(),
                        },
                    ],
                    last_checked_at: 1,
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
                Query(LyricsCandidatesQuery { format: None }),
            )
            .await,
        )?;
        assert_eq!(candidates.items.len(), 3);
        assert_eq!(
            candidates
                .items
                .iter()
                .filter(|candidate| matches!(candidate.scope, LyricsScopeResponse::Personal))
                .count(),
            1
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
                }),
            )
            .await,
        )?;
        assert_eq!(lrc_candidates.items.len(), 1);
        assert_eq!(
            lrc_candidates.items[0].provider_id.as_deref(),
            Some("route-provider")
        );

        {
            let mut db = STATE.db.write().await;
            db::libraries::revoke_access(&mut db, alice_id, library_db_id)?;
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
        assert!(
            db::lyrics::find_personal(
                &*db,
                track_db_id,
                &db::users::get_by_id(&*db, alice_id)?.unwrap().id
            )?
            .is_none()
        );
        assert!(
            db::lyrics::find_personal(
                &*db,
                track_db_id,
                &db::users::get_by_id(&*db, bob_id)?.unwrap().id
            )?
            .is_some()
        );
        Ok(())
    }
}
