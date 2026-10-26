// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod decision;
mod download;
mod hls;
mod ranged_file;
mod stream;

pub(crate) use decision::{
    DeliveryTarget,
    TranscodeKnobs,
    ValidatedRequest,
    apply_transcode_policy,
    resolve_delivery,
    validate_request,
};
pub use download::download_routes;
pub(crate) use download::download_track_response;
pub(crate) use hls::{
    HlsPlaylistOptions,
    resolve_hls_profile,
    serve_hls_playlist_for_track,
};
pub(crate) use ranged_file::build_ranged_file_body;
pub(crate) use stream::stream_track_response;

use axum::Router;

use agdb::DbId;
use axum::{
    body::Body,
    http::{
        HeaderMap,
        Response,
        StatusCode,
        header,
    },
};
use lyra_ffmpeg::{
    AudioCodec,
    AudioFormat,
};
use std::path::{
    Path as FsPath,
    PathBuf,
};
use std::time::{
    SystemTime,
    UNIX_EPOCH,
};

use crate::{
    STATE,
    db,
    routes::AppError,
    services::{
        auth::{
            Principal,
            media_tokens::{
                MediaTokenAccess,
                MediaTokenError,
                MediaTokenPurpose,
                validate_media_token,
            },
            require_authenticated,
            require_permission,
        },
        hls::state::HlsSessionHolder,
        playback_sources::{
            self as playback_source_service,
            SourceVersion,
        },
    },
};

#[derive(Debug)]
pub struct ValidatedTrackSource {
    pub source_id: DbId,
    pub source_public_id: String,
    pub version: SourceVersion,
    pub track_public_id: String,
    pub input_path: String,
    pub entry_format: Option<AudioFormat>,
    pub source_codec: Option<AudioCodec>,
    pub full_path: PathBuf,
    pub duration_ms: Option<u64>,
    pub start_ms: Option<u64>,
    pub end_ms: Option<u64>,
    pub source_bitrate_bps: Option<u32>,
    pub source_sample_rate_hz: Option<u32>,
    pub source_channels: Option<u32>,
}

#[derive(Clone, Default)]
pub(crate) struct ServeTrackOptions {
    pub(crate) format: Option<String>,
    pub(crate) codec: Option<String>,
    pub(crate) bitrate_bps: Option<u32>,
    pub(crate) sample_rate_hz: Option<u32>,
    pub(crate) channels: Option<u32>,
    pub(crate) prefer_vbr: Option<bool>,
    pub(crate) start_offset_ms: Option<u64>,
}

pub(crate) fn source_range_duration_ms(start_ms: Option<u64>, end_ms: Option<u64>) -> Option<u64> {
    match (start_ms, end_ms) {
        (Some(start_ms), Some(end_ms)) if end_ms > start_ms => Some(end_ms - start_ms),
        _ => None,
    }
}

pub fn apply_request_start_offset(
    mut source: ValidatedTrackSource,
    start_offset_ms: Option<u64>,
) -> Result<ValidatedTrackSource, AppError> {
    let Some(start_offset_ms) = start_offset_ms else {
        return Ok(source);
    };

    if start_offset_ms == 0 {
        return Ok(source);
    }

    let current_start_ms = source.start_ms.unwrap_or(0);
    let next_start_ms = current_start_ms
        .checked_add(start_offset_ms)
        .ok_or_else(|| AppError::bad_request("start_offset_ms is too large"))?;

    if let Some(end_ms) = source.end_ms {
        if next_start_ms >= end_ms {
            return Err(AppError::bad_request(
                "start_offset_ms exceeds the available source duration",
            ));
        }
    } else if let Some(duration_ms) = source.duration_ms.filter(|duration_ms| *duration_ms > 0) {
        if start_offset_ms >= duration_ms {
            return Err(AppError::bad_request(
                "start_offset_ms exceeds the available source duration",
            ));
        }

        source.duration_ms = Some(duration_ms - start_offset_ms);
    }

    source.start_ms = Some(next_start_ms);
    Ok(source)
}

/// Checks that `principal` may be served a track for `purpose`.
pub(crate) fn authorize_track_purpose(
    principal: &Principal,
    purpose: MediaTokenPurpose,
) -> Result<(), AppError> {
    match purpose {
        MediaTokenPurpose::Stream | MediaTokenPurpose::HlsPlaylist => Ok(()),
        MediaTokenPurpose::Download => {
            require_permission(principal, db::Permission::Download).map_err(Into::into)
        }
    }
}

/// How a request reached a track. A principal's library access is checked under the guard that
/// resolves the track's source. A media token was bound to the track's public id when it was
/// issued; its holder is checked like a principal under that guard, and it only serves the source
/// file it was issued against. A public plugin route serves any track, as its protocol does.
#[derive(Debug)]
pub(crate) enum TrackAccess {
    Principal(Principal),
    MediaToken(MediaTokenAccess),
    PublicPluginRoute,
}

impl TrackAccess {
    /// Who an HLS session created through this access serves.
    pub(crate) fn session_holder(&self) -> HlsSessionHolder {
        match self {
            Self::Principal(principal) => HlsSessionHolder::Principal(principal.clone()),
            Self::MediaToken(token) => HlsSessionHolder::MediaToken(token.holder.clone()),
            Self::PublicPluginRoute => HlsSessionHolder::Public,
        }
    }
}

/// Checks, for each HLS segment, that the session's holder may still be served the track: the
/// user still exists, a media token's credential still authenticates them, and they can still
/// see the track.
pub(super) async fn require_session_access(
    holder: &HlsSessionHolder,
    track_public_id: &str,
) -> Result<(), AppError> {
    if matches!(holder, HlsSessionHolder::Public) {
        return Ok(());
    }
    let db = &*STATE.db.read().await;
    let not_found = || AppError::not_found("HLS session not found");
    let principal = match holder {
        HlsSessionHolder::Public => return Ok(()),
        HlsSessionHolder::Principal(principal) => {
            principal.require(db)?;
            std::borrow::Cow::Borrowed(principal)
        }
        HlsSessionHolder::MediaToken(holder) => {
            std::borrow::Cow::Owned(holder.principal(db)?.ok_or_else(|| {
                AppError::unauthorized("media token holder is no longer signed in")
            })?)
        }
    };
    let track_db_id = db::lookup::find_node_id_by_id(db, track_public_id)?.ok_or_else(not_found)?;
    crate::services::auth::access::require_entity_accessible(
        db,
        &principal,
        track_db_id,
        not_found,
    )?;
    Ok(())
}

fn media_token_error_to_app_error(err: MediaTokenError) -> AppError {
    match err {
        MediaTokenError::Invalid => AppError::unauthorized("invalid media token"),
        MediaTokenError::Expired => AppError::unauthorized("media token expired"),
    }
}

fn validate_track_media_token(
    media_token: Option<&str>,
    purpose: MediaTokenPurpose,
    track_id: &str,
) -> Result<MediaTokenAccess, MediaTokenError> {
    let Some(media_token) = media_token.map(str::trim).filter(|token| !token.is_empty()) else {
        return Err(MediaTokenError::Invalid);
    };
    validate_media_token(media_token, purpose, track_id)
}

/// The principal a request to serve a track for `purpose` must be, without a media token.
async fn authorize_track_request(
    headers: &HeaderMap,
    purpose: MediaTokenPurpose,
) -> Result<Principal, AppError> {
    let principal = require_authenticated(headers).await?;
    authorize_track_purpose(&principal, purpose)?;
    Ok(principal)
}

/// Grants access to a track for `purpose` through a media token bound to it, or else the
/// request's credential. A failed media token reports its own error over the credential's.
pub(crate) async fn require_track_access(
    headers: &HeaderMap,
    media_token: Option<&str>,
    purpose: MediaTokenPurpose,
    track_id: &str,
) -> Result<TrackAccess, AppError> {
    match validate_track_media_token(media_token, purpose, track_id) {
        Ok(access) => Ok(TrackAccess::MediaToken(access)),
        Err(token_error) if media_token.is_some() => authorize_track_request(headers, purpose)
            .await
            .map(TrackAccess::Principal)
            .map_err(|_| media_token_error_to_app_error(token_error)),
        Err(_) => authorize_track_request(headers, purpose)
            .await
            .map(TrackAccess::Principal),
    }
}

/// Resolves the track's playable source, checking `access` under the same guard.
pub(crate) async fn validate_and_get_track_source(
    track_id: &str,
    access: &TrackAccess,
) -> Result<ValidatedTrackSource, AppError> {
    let db = &*STATE.db.read().await;
    let not_found = || AppError::not_found(format!("Track not found: {track_id}"));
    let track_db_id = db::lookup::find_node_id_by_id(db, track_id)?.ok_or_else(not_found)?;
    match access {
        TrackAccess::Principal(principal) => {
            principal.require(db)?;
            crate::services::auth::access::require_entity_accessible(
                db,
                principal,
                track_db_id,
                not_found,
            )?;
        }
        TrackAccess::MediaToken(token) => {
            let principal = token.holder.principal(db)?.ok_or_else(|| {
                AppError::unauthorized("media token holder is no longer signed in")
            })?;
            authorize_track_purpose(&principal, token.purpose)?;
            crate::services::auth::access::require_entity_accessible(
                db,
                &principal,
                track_db_id,
                not_found,
            )?;
        }
        TrackAccess::PublicPluginRoute => {}
    }
    let track = db::tracks::get_by_id(db, track_db_id)?.ok_or_else(not_found)?;

    let source = playback_source_service::resolve(db, track_db_id, false)?.ok_or_else(|| {
        AppError::not_found(format!("Playable source not found for track: {track_id}"))
    })?;
    let source_public_id =
        db::lookup::find_id_by_db_id(db, source.source_id)?.ok_or_else(|| {
            AppError::not_found(format!(
                "Playable source has no public id: {}",
                source.source_id.0
            ))
        })?;
    let version =
        SourceVersion::read(source_public_id.clone(), &source.full_path).map_err(|err| {
            tracing::warn!(
                track_id,
                path = %source.full_path.display(),
                error = %err,
                "track source file is unreadable"
            );
            AppError::not_found(format!("Track source file not found for track: {track_id}"))
        })?;
    if let TrackAccess::MediaToken(token) = access
        && token.source != version
    {
        return Err(AppError::gone(
            "the track's source changed since this media token was issued",
        ));
    }

    Ok(ValidatedTrackSource {
        source_id: source.source_id,
        source_public_id,
        version,
        track_public_id: track.id.clone(),
        input_path: source.input_path,
        entry_format: playback_source_service::transcoder_format(source.format),
        source_codec: source
            .format
            .codec
            .and_then(playback_source_service::transcoder_codec),
        full_path: source.full_path,
        duration_ms: track.duration_ms,
        start_ms: source.start_ms,
        end_ms: source.end_ms,
        source_bitrate_bps: track.bitrate_bps,
        source_sample_rate_hz: track.sample_rate_hz,
        source_channels: track.channel_count,
    })
}

/// The body of a `HEAD` response whose `GET` body has no known length. Its size is unknown
/// rather than zero, so no `Content-Length: 0` is derived from it.
fn head_body() -> Body {
    Body::from_stream(futures::stream::empty::<Result<bytes::Bytes, std::io::Error>>())
}

pub fn temp_output_path(track_id: &str, format: AudioFormat) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "lyra-download-{}-{}.{}",
        track_id,
        nanos,
        format.extension()
    ))
}

async fn file_response_internal(
    path: &FsPath,
    content_type: &str,
    headers: &HeaderMap,
    cleanup_path: Option<PathBuf>,
    cache_control: &str,
) -> Result<Response<Body>, AppError> {
    let ranged = build_ranged_file_body(
        path,
        headers.get(header::RANGE),
        StatusCode::OK,
        cleanup_path,
    )
    .await?;

    if ranged.status == StatusCode::RANGE_NOT_SATISFIABLE {
        let mut response = Response::builder().status(ranged.status);
        if let Some(content_range) = ranged.content_range {
            response = response.header(header::CONTENT_RANGE, content_range);
        }
        return Ok(response.body(Body::empty())?);
    }

    let mut response = Response::builder()
        .status(ranged.status)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_LENGTH, ranged.content_length)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CACHE_CONTROL, cache_control);

    if cache_control == "no-cache, no-store, must-revalidate" {
        response = response
            .header(header::PRAGMA, "no-cache")
            .header(header::EXPIRES, "0");
    }

    if let Some(content_range) = ranged.content_range {
        response = response.header(header::CONTENT_RANGE, content_range);
    }

    Ok(response.body(ranged.body)?)
}

pub async fn file_response(
    path: &FsPath,
    content_type: &str,
    headers: &HeaderMap,
) -> Result<Response<Body>, AppError> {
    file_response_internal(
        path,
        content_type,
        headers,
        None,
        "no-cache, no-store, must-revalidate",
    )
    .await
}

pub(crate) async fn file_response_with_cache_control(
    path: &FsPath,
    content_type: &str,
    headers: &HeaderMap,
    cache_control: &str,
) -> Result<Response<Body>, AppError> {
    file_response_internal(path, content_type, headers, None, cache_control).await
}

pub async fn temp_file_response(
    path: &FsPath,
    content_type: &str,
    headers: &HeaderMap,
) -> Result<Response<Body>, AppError> {
    file_response_internal(
        path,
        content_type,
        headers,
        Some(path.to_path_buf()),
        "no-cache, no-store, must-revalidate",
    )
    .await
}

pub fn stream_routes() -> Router {
    stream::stream_routes().merge(hls::hls_routes())
}

#[cfg(feature = "docgen")]
pub(crate) fn stream_openapi_routes() -> aide::axum::ApiRouter {
    stream::stream_openapi_routes().merge(hls::hls_openapi_routes())
}

#[cfg(feature = "docgen")]
pub(crate) fn download_openapi_routes() -> aide::axum::ApiRouter {
    download::download_openapi_routes()
}

#[cfg(test)]
mod tests {
    use super::{
        ServeTrackOptions,
        ValidatedTrackSource,
        apply_request_start_offset,
        require_track_access,
    };
    use std::{
        path::PathBuf,
        time::{
            SystemTime,
            UNIX_EPOCH,
        },
    };

    use axum::http::{
        HeaderMap,
        Method,
        StatusCode,
        header::AUTHORIZATION,
    };
    use lyra_ffmpeg::{
        AudioCodec,
        AudioFormat,
    };
    use nanoid::nanoid;

    use crate::{
        STATE,
        db::{
            self,
            Permission,
            User,
            roles::Role,
        },
        services::auth::media_tokens::{
            MediaTokenPurpose,
            issue_unbound_media_token,
        },
        testing::{
            LibraryFixtureConfig,
            initialize_runtime,
            runtime_test_lock,
        },
    };
    use axum::response::IntoResponse;

    async fn initialize_test_runtime() -> anyhow::Result<PathBuf> {
        let test_dir = std::env::temp_dir().join(format!(
            "lyra-serve-auth-test-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        std::fs::create_dir_all(&test_dir)?;
        initialize_runtime(&LibraryFixtureConfig {
            directory: test_dir.clone(),
            language: None,
            country: None,
        })
        .await?;
        Ok(test_dir)
    }

    async fn create_user_with_permissions(
        username: &str,
        permissions: Vec<Permission>,
    ) -> anyhow::Result<HeaderMap> {
        let user_db_id = {
            let mut db = STATE.db.write().await;
            db::roles::ensure_builtin_roles(&mut db)?;
            let user_db_id = db::users::create(
                &mut db,
                &User {
                    db_id: None,
                    id: nanoid!(),
                    username: username.to_string(),
                    password: "unused".to_string(),
                },
            )?;
            let role_name = if permissions.is_empty() {
                db::roles::BUILTIN_USER_ROLE.to_string()
            } else {
                let role_name = format!("download-test-{}", nanoid!());
                db::roles::create(
                    &mut db,
                    &Role {
                        db_id: None,
                        id: nanoid!(),
                        name: role_name.clone(),
                        permissions,
                    },
                )?;
                role_name
            };
            db::roles::ensure_user_has_role(&mut db, user_db_id, &role_name)?;
            let library_ids = db::libraries::get(&*db)?
                .into_iter()
                .filter_map(|library| library.db_id)
                .collect::<Vec<_>>();
            for library_db_id in library_ids {
                db::libraries::grant_access(
                    &mut db,
                    user_db_id,
                    library_db_id,
                    db::libraries::AccessKind::ReadWrite,
                )?;
            }
            user_db_id
        };

        let session = crate::testing::create_session(user_db_id, Default::default()).await?;
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            format!("Bearer {}", session.token)
                .parse()
                .expect("valid auth header"),
        );
        Ok(headers)
    }

    #[tokio::test]
    async fn download_access_rejects_user_without_download_permission() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let headers = create_user_with_permissions("listener", vec![]).await?;

        let status = require_track_access(&headers, None, MediaTokenPurpose::Download, "unused")
            .await
            .expect_err("user without permission should be rejected")
            .into_response()
            .status();
        assert_eq!(status, StatusCode::FORBIDDEN);

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test]
    async fn download_access_allows_user_with_download_permission() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let headers =
            create_user_with_permissions("downloader", vec![Permission::Download]).await?;

        let access = require_track_access(&headers, None, MediaTokenPurpose::Download, "unused")
            .await
            .expect("user with download permission should be allowed");

        let super::TrackAccess::Principal(principal) = access else {
            panic!("bearer access should resolve a principal");
        };
        assert_eq!(principal.username, "downloader");

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    /// The query of the URL `POST /api/tracks/{id}/playback-url` mints for `transport`.
    async fn playback_url_query(
        track_id: &str,
        headers: &HeaderMap,
        transport: &str,
    ) -> anyhow::Result<String> {
        use axum::body::{
            Body,
            to_bytes,
        };
        use tower::ServiceExt;

        let response = crate::routes::tracks::track_routes()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(format!("/{track_id}/playback-url"))
                    .header(AUTHORIZATION, headers[AUTHORIZATION].clone())
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await?)?;
        let url = body[transport]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("{transport} missing"))?;
        url.split_once('?')
            .map(|(_, query)| query.to_string())
            .ok_or_else(|| anyhow::anyhow!("{transport} has no query"))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn media_token_does_not_follow_a_recycled_track_id() -> anyhow::Result<()> {
        use axum::body::Body;
        use tower::ServiceExt;

        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = create_user_with_permissions("token-holder", vec![]).await?;
        let query = playback_url_query(&track_id, &headers, "hls_url").await?;

        let recycled_track_id = {
            let mut db = STATE.db.write().await;
            let track_db_id = db::lookup::find_node_id_by_id(&*db, &track_id)?
                .ok_or_else(|| anyhow::anyhow!("track missing"))?;
            db.exec_mut(agdb::QueryBuilder::remove().ids(track_db_id).query())?;
            let recycled = db::test_db::insert_track(&mut db, "Recycled")?;
            assert_eq!(recycled, track_db_id, "agdb must reuse the track DbId");
            db::lookup::find_id_by_db_id(&*db, recycled)?
                .ok_or_else(|| anyhow::anyhow!("recycled track has no public id"))?
        };

        let response = super::stream_routes()
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!("/{recycled_track_id}/hls.m3u8?{query}"))
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn track_source_rejects_a_deleted_principal() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = create_user_with_permissions("departed", vec![]).await?;
        let principal = crate::services::auth::require_authenticated(&headers).await?;
        {
            let mut db = STATE.db.write().await;
            let user_db_id = principal.require(&*db)?;
            db.transaction_mut(|t| db::users::delete_user(t, user_db_id))?;
        }

        let status = super::validate_and_get_track_source(
            &track_id,
            &super::TrackAccess::Principal(principal),
        )
        .await
        .expect_err("a deleted principal must not resolve a track source")
        .into_response()
        .status();
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test]
    async fn require_stream_access_allows_matching_media_token_without_bearer() -> anyhow::Result<()>
    {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;

        let track_id = "track-123";
        let token = issue_unbound_media_token(track_id, MediaTokenPurpose::Stream);
        require_track_access(
            &HeaderMap::new(),
            Some(&token.token),
            MediaTokenPurpose::Stream,
            track_id,
        )
        .await
        .expect("matching stream media token should grant access");

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test]
    async fn require_hls_playlist_access_rejects_wrong_media_token_purpose() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;

        let track_id = "track-123";
        let token = issue_unbound_media_token(track_id, MediaTokenPurpose::Stream);
        let status = require_track_access(
            &HeaderMap::new(),
            Some(&token.token),
            MediaTokenPurpose::HlsPlaylist,
            track_id,
        )
        .await
        .expect_err("stream token should not authorize HLS playlist creation")
        .into_response()
        .status();
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[test]
    fn apply_request_start_offset_reduces_duration_for_full_track_sources() {
        let source = ValidatedTrackSource {
            source_id: agdb::DbId(2),
            source_public_id: "source-pub-2".to_string(),
            version: crate::services::playback_sources::SourceVersion::fixed("source-pub-2"),
            track_public_id: "track-pub-test".to_string(),
            input_path: "track.flac".to_string(),
            entry_format: Some(AudioFormat::Flac),
            source_codec: Some(AudioCodec::Flac),
            full_path: PathBuf::from("track.flac"),
            duration_ms: Some(20_000),
            start_ms: None,
            end_ms: None,
            source_bitrate_bps: Some(900_000),
            source_sample_rate_hz: Some(96_000),
            source_channels: Some(2),
        };

        let offset_source =
            apply_request_start_offset(source, Some(5_000)).expect("offset should be applied");
        assert_eq!(offset_source.start_ms, Some(5_000));
        assert_eq!(offset_source.duration_ms, Some(15_000));
    }

    #[test]
    fn apply_request_start_offset_stacks_on_existing_source_range() {
        let source = ValidatedTrackSource {
            source_id: agdb::DbId(2),
            source_public_id: "source-pub-2".to_string(),
            version: crate::services::playback_sources::SourceVersion::fixed("source-pub-2"),
            track_public_id: "track-pub-test".to_string(),
            input_path: "track.flac".to_string(),
            entry_format: Some(AudioFormat::Flac),
            source_codec: Some(AudioCodec::Flac),
            full_path: PathBuf::from("track.flac"),
            duration_ms: Some(20_000),
            start_ms: Some(10_000),
            end_ms: Some(30_000),
            source_bitrate_bps: Some(900_000),
            source_sample_rate_hz: Some(96_000),
            source_channels: Some(2),
        };

        let offset_source =
            apply_request_start_offset(source, Some(5_000)).expect("offset should stack");
        assert_eq!(offset_source.start_ms, Some(15_000));
        assert_eq!(offset_source.end_ms, Some(30_000));
    }

    #[test]
    fn apply_request_start_offset_rejects_offsets_past_available_duration() {
        let source = ValidatedTrackSource {
            source_id: agdb::DbId(2),
            source_public_id: "source-pub-2".to_string(),
            version: crate::services::playback_sources::SourceVersion::fixed("source-pub-2"),
            track_public_id: "track-pub-test".to_string(),
            input_path: "track.flac".to_string(),
            entry_format: Some(AudioFormat::Flac),
            source_codec: Some(AudioCodec::Flac),
            full_path: PathBuf::from("track.flac"),
            duration_ms: Some(20_000),
            start_ms: Some(10_000),
            end_ms: Some(30_000),
            source_bitrate_bps: Some(900_000),
            source_sample_rate_hz: Some(96_000),
            source_channels: Some(2),
        };

        let err = apply_request_start_offset(source, Some(20_000))
            .expect_err("offset equal to the available range must be rejected");
        assert_eq!(err.into_response().status(), StatusCode::BAD_REQUEST);
    }

    async fn prepare_streamable_track(test_dir: &std::path::Path) -> anyhow::Result<String> {
        let fixture_src = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/assets/metadata/integration_track.flac");
        let fixture_dst = test_dir.join("integration_track.flac");
        tokio::fs::copy(&fixture_src, &fixture_dst).await?;

        let (tag, properties, _) = crate::services::metadata::read_audio_tags(fixture_dst.clone())?;
        let fixture_str = fixture_dst.to_string_lossy().to_string();
        let mapping_config = crate::services::metadata::mapping::default_config();
        let raw_tags = crate::services::metadata::mapping::apply_mapping(
            &tag,
            &properties,
            &fixture_str,
            &mapping_config,
        );

        let fixture = crate::testing::prepare_fixture(
            &LibraryFixtureConfig {
                directory: test_dir.to_path_buf(),
                language: None,
                country: None,
            },
            vec![raw_tags],
        )
        .await?;
        let track_db_id = *fixture
            .track_ids
            .first()
            .ok_or_else(|| anyhow::anyhow!("prepare_fixture produced no track ids"))?;
        db::lookup::find_id_by_db_id(&*STATE.db.read().await, agdb::DbId(track_db_id))?
            .ok_or_else(|| anyhow::anyhow!("fixture track has no public id"))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn download_serves_resolved_access_without_request_credentials() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers =
            create_user_with_permissions("plugin-downloader", vec![Permission::Download]).await?;
        let principal = crate::services::auth::require_authenticated(&headers).await?;

        let response = super::download::download_track_response(
            &Method::GET,
            &HeaderMap::new(),
            &track_id,
            &super::TrackAccess::Principal(principal),
            ServeTrackOptions::default(),
        )
        .await
        .map_err(|error| {
            anyhow::anyhow!("download failed: {:?}", error.into_response().status())
        })?;
        assert_eq!(response.status(), StatusCode::OK);

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stream_direct_copy_response_advertises_byte_range_support() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = HeaderMap::new();

        let response = super::stream::stream_track_response(
            &Method::GET,
            &headers,
            &track_id,
            &super::TrackAccess::PublicPluginRoute,
            ServeTrackOptions::default(),
        )
        .await
        .map_err(|err| anyhow::anyhow!("stream failed: {err:?}"))?;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::ACCEPT_RANGES)
                .and_then(|v| v.to_str().ok()),
            Some("bytes"),
            "direct-copy responses must continue to advertise byte-range support"
        );

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stream_forced_transcode_advertises_no_byte_ranges() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = HeaderMap::new();

        let response = super::stream::stream_track_response(
            &Method::GET,
            &headers,
            &track_id,
            &super::TrackAccess::PublicPluginRoute,
            ServeTrackOptions {
                format: Some("mp3".to_string()),
                bitrate_bps: Some(96_000),
                ..ServeTrackOptions::default()
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("stream failed: {err:?}"))?;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::ACCEPT_RANGES)
                .and_then(|v| v.to_str().ok()),
            Some("none"),
            "transcoded responses must advertise Accept-Ranges: none so clients don't request byte ranges"
        );
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::TRANSFER_ENCODING)
                .and_then(|v| v.to_str().ok()),
            Some("chunked"),
        );

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    async fn send(
        method: Method,
        uri: String,
    ) -> anyhow::Result<(StatusCode, HeaderMap, axum::body::Bytes)> {
        use tower::ServiceExt;

        let response = super::stream_routes()
            .oneshot(
                axum::http::Request::builder()
                    .method(method)
                    .uri(uri)
                    .body(axum::body::Body::empty())?,
            )
            .await?;
        let status = response.status();
        let headers = response.headers().clone();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
        Ok((status, headers, body))
    }

    fn bearer_token(headers: &HeaderMap) -> String {
        headers[AUTHORIZATION]
            .to_str()
            .expect("ascii header")
            .trim_start_matches("Bearer ")
            .to_string()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn media_token_stops_working_once_its_session_is_revoked() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = create_user_with_permissions("revoked-listener", vec![]).await?;
        let query = playback_url_query(&track_id, &headers, "stream_url").await?;

        let (status, _, _) = send(Method::GET, format!("/{track_id}?{query}")).await?;
        assert_eq!(status, StatusCode::OK);

        assert!(crate::services::auth::logout_with_token(Some(&bearer_token(&headers))).await?);
        let (status, _, _) = send(Method::GET, format!("/{track_id}?{query}")).await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn media_token_stops_working_once_its_user_is_deleted() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = create_user_with_permissions("departing-listener", vec![]).await?;
        let query = playback_url_query(&track_id, &headers, "stream_url").await?;
        let principal = crate::services::auth::require_authenticated(&headers).await?;
        {
            let mut db = STATE.db.write().await;
            let user_db_id = principal.require(&*db)?;
            db.transaction_mut(|t| db::users::delete_user(t, user_db_id))?;
        }

        let (status, _, _) = send(Method::GET, format!("/{track_id}?{query}")).await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    async fn user_db_id(headers: &HeaderMap) -> anyhow::Result<agdb::DbId> {
        let principal = crate::services::auth::require_authenticated(headers).await?;
        Ok(principal.require(&*STATE.db.read().await)?)
    }

    fn bearer(secret: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            format!("Bearer {secret}")
                .parse()
                .expect("valid auth header"),
        );
        headers
    }

    async fn send_download(uri: String) -> anyhow::Result<StatusCode> {
        use tower::ServiceExt;

        let response = super::download_routes()
            .oneshot(
                axum::http::Request::builder()
                    .uri(uri)
                    .body(axum::body::Body::empty())?,
            )
            .await?;
        Ok(response.status())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn media_token_stops_working_once_its_api_key_is_revoked() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = create_user_with_permissions("key-listener", vec![]).await?;
        let principal = crate::services::auth::require_authenticated(&headers).await?;
        let key = crate::services::auth::api_keys::create_api_key(&principal, "phone")
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        let query = playback_url_query(&track_id, &bearer(&key.key), "stream_url").await?;

        let (status, _, _) = send(Method::GET, format!("/{track_id}?{query}")).await?;
        assert_eq!(status, StatusCode::OK);

        assert!(
            crate::services::auth::api_keys::revoke_api_key_for_user(&principal, &key.id).await?
        );
        let (status, _, _) = send(Method::GET, format!("/{track_id}?{query}")).await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn download_token_stops_working_once_download_permission_is_removed() -> anyhow::Result<()>
    {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers =
            create_user_with_permissions("demoted-downloader", vec![Permission::Download]).await?;
        let query = playback_url_query(&track_id, &headers, "download_url").await?;
        assert_eq!(
            send_download(format!("/{track_id}?{query}")).await?,
            StatusCode::OK
        );

        let user = user_db_id(&headers).await?;
        db::roles::ensure_user_has_role(
            &mut *STATE.db.write().await,
            user,
            db::roles::BUILTIN_USER_ROLE,
        )?;
        assert_eq!(
            send_download(format!("/{track_id}?{query}")).await?,
            StatusCode::FORBIDDEN
        );

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn media_token_stops_working_once_library_access_is_removed() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = create_user_with_permissions("excluded-listener", vec![]).await?;
        let query = playback_url_query(&track_id, &headers, "stream_url").await?;

        let user = user_db_id(&headers).await?;
        {
            let mut db = STATE.db.write().await;
            let libraries = db::libraries::get(&*db)?
                .into_iter()
                .filter_map(|library| library.db_id)
                .collect::<Vec<_>>();
            for library in libraries {
                db::libraries::revoke_access(&mut *db, user, library)?;
            }
        }

        let (status, _, _) = send(Method::GET, format!("/{track_id}?{query}")).await?;
        assert_eq!(status, StatusCode::NOT_FOUND);

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn hls_token_is_gone_once_its_source_file_changes() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = create_user_with_permissions("stale-hls-listener", vec![]).await?;
        let query = playback_url_query(&track_id, &headers, "hls_url").await?;
        append_to_source(&test_dir)?;

        let (status, _, _) = send(Method::GET, format!("/{track_id}/hls.m3u8?{query}")).await?;
        assert_eq!(status, StatusCode::GONE);

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn hls_segments_stop_once_the_session_holder_signs_out() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = create_user_with_permissions("hls-leaver", vec![]).await?;
        let query = playback_url_query(&track_id, &headers, "hls_url").await?;

        let (status, _, playlist) =
            send(Method::GET, format!("/{track_id}/hls.m3u8?{query}")).await?;
        assert_eq!(status, StatusCode::OK);
        let segment = std::str::from_utf8(&playlist)?
            .lines()
            .find_map(|line| line.strip_prefix("/api/stream"))
            .ok_or_else(|| anyhow::anyhow!("playlist has no segment"))?
            .to_string();
        let (status, _, _) = send(Method::GET, segment.clone()).await?;
        assert_eq!(status, StatusCode::OK);

        assert!(crate::services::auth::logout_with_token(Some(&bearer_token(&headers))).await?);
        let (status, _, _) = send(Method::GET, segment).await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        crate::services::hls::state::test_helpers::reset_hls_state_for_test().await;
        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn hls_starts_a_new_job_once_the_source_file_changes() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        crate::services::hls::state::test_helpers::reset_hls_state_for_test().await;
        let playlist = || async {
            super::serve_hls_playlist_for_track(
                &Method::GET,
                &track_id,
                &super::TrackAccess::PublicPluginRoute,
                super::HlsPlaylistOptions {
                    codec: None,
                    bitrate_bps: None,
                    sample_rate_hz: None,
                    channels: None,
                    prefer_vbr: None,
                    start_offset_ms: None,
                },
            )
            .await
            .map_err(|err| anyhow::anyhow!("playlist failed: {err:?}"))
        };

        playlist().await?;
        playlist().await?;
        assert_eq!(
            crate::services::hls::state::hls_registry_counts().await.0,
            1,
            "an unchanged source shares its job"
        );
        append_to_source(&test_dir)?;
        playlist().await?;
        assert_eq!(
            crate::services::hls::state::hls_registry_counts().await.0,
            2,
            "a changed source never reuses the old file's job"
        );

        crate::services::hls::state::test_helpers::reset_hls_state_for_test().await;
        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    fn append_to_source(test_dir: &std::path::Path) -> anyhow::Result<()> {
        let path = test_dir.join("integration_track.flac");
        let mut bytes = std::fs::read(&path)?;
        bytes.extend_from_slice(&[0; 16]);
        std::fs::write(&path, bytes)?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn media_token_is_gone_once_its_source_file_changes() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = create_user_with_permissions("stale-listener", vec![]).await?;
        let query = playback_url_query(&track_id, &headers, "stream_url").await?;

        let path = test_dir.join("integration_track.flac");
        let mut bytes = std::fs::read(&path)?;
        bytes.extend_from_slice(&[0; 16]);
        std::fs::write(&path, bytes)?;

        let (status, _, _) = send(Method::GET, format!("/{track_id}?{query}")).await?;
        assert_eq!(status, StatusCode::GONE);

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stream_head_answers_like_get_without_a_body() -> anyhow::Result<()> {
        use axum::http::header::{
            ACCEPT_RANGES,
            CONTENT_LENGTH,
            CONTENT_TYPE,
        };

        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = create_user_with_permissions("head-listener", vec![]).await?;
        let token_query = playback_url_query(&track_id, &headers, "stream_url").await?;
        let uri = |query: &str| format!("/{track_id}?{token_query}{query}");

        let (get_status, get_headers, get_body) = send(Method::GET, uri("")).await?;
        let (head_status, head_headers, head_body) = send(Method::HEAD, uri("")).await?;
        assert_eq!(get_status, StatusCode::OK);
        assert_eq!(head_status, get_status);
        for name in [CONTENT_TYPE, CONTENT_LENGTH, ACCEPT_RANGES] {
            assert_eq!(head_headers.get(&name), get_headers.get(&name), "{name}");
        }
        assert_eq!(
            head_headers[CONTENT_LENGTH].to_str()?,
            get_body.len().to_string()
        );
        assert!(head_body.is_empty());

        let (status, headers, body) =
            send(Method::HEAD, uri("&format=mp3&bitrate_bps=96000")).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers[CONTENT_TYPE], "audio/mpeg");
        assert_eq!(headers[ACCEPT_RANGES], "none");
        assert!(
            headers.get(CONTENT_LENGTH).is_none(),
            "a transcode's length is unknown, not zero"
        );
        assert!(body.is_empty());

        let (get_status, _, _) = send(Method::GET, uri("&format=mp3&bitrate_bps=0")).await?;
        let (head_status, _, _) = send(Method::HEAD, uri("&format=mp3&bitrate_bps=0")).await?;
        assert_eq!(get_status, StatusCode::BAD_REQUEST);
        assert_eq!(head_status, get_status);

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn hls_playlist_head_starts_no_transcode() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let registry_before = crate::services::hls::state::hls_registry_counts().await;

        let response = super::serve_hls_playlist_for_track(
            &Method::HEAD,
            &track_id,
            &super::TrackAccess::PublicPluginRoute,
            super::HlsPlaylistOptions {
                codec: None,
                bitrate_bps: None,
                sample_rate_hz: None,
                channels: None,
                prefer_vbr: None,
                start_offset_ms: None,
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("playlist failed: {err:?}"))?;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[axum::http::header::CONTENT_TYPE],
            "application/x-mpegurl"
        );
        assert_eq!(
            crate::services::hls::state::hls_registry_counts().await,
            registry_before,
            "HEAD must not create an HLS job or session"
        );

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stream_transcodes_m4a_as_fragmented_mp4() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;

        let response = super::stream::stream_track_response(
            &Method::GET,
            &HeaderMap::new(),
            &track_id,
            &super::TrackAccess::PublicPluginRoute,
            ServeTrackOptions {
                format: Some("m4a".to_string()),
                codec: Some("aac".to_string()),
                ..ServeTrackOptions::default()
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("stream failed: {err:?}"))?;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("audio/mp4"),
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
        assert!(
            body.windows(4).any(|atom| atom == b"moof"),
            "a streamed m4a must be fragmented"
        );

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stream_forces_transcode_when_a_knob_asks_for_a_downward_change() -> anyhow::Result<()>
    {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = HeaderMap::new();

        // The fixture is 44.1 kHz, so 22.05 kHz is a real downsample.
        let response = super::stream::stream_track_response(
            &Method::GET,
            &headers,
            &track_id,
            &super::TrackAccess::PublicPluginRoute,
            ServeTrackOptions {
                format: Some("flac".to_string()),
                sample_rate_hz: Some(22_050),
                ..ServeTrackOptions::default()
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("stream failed: {err:?}"))?;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::ACCEPT_RANGES)
                .and_then(|v| v.to_str().ok()),
            Some("none"),
            "a sample-rate knob below the source must force the transcoded (chunked) path even when the output format matches the source"
        );
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::TRANSFER_ENCODING)
                .and_then(|v| v.to_str().ok()),
            Some("chunked"),
        );

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stream_keeps_direct_copy_when_knobs_ask_for_no_downward_change() -> anyhow::Result<()>
    {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = HeaderMap::new();

        // The fixture is 44.1 kHz mono. Asking for exactly that, or for more, asks for no
        // change we are willing to make, so the knobs must not destroy the passthrough.
        for (sample_rate_hz, channels) in [(44_100, 1), (48_000, 2)] {
            let response = super::stream::stream_track_response(
                &Method::GET,
                &headers,
                &track_id,
                &super::TrackAccess::PublicPluginRoute,
                ServeTrackOptions {
                    format: Some("flac".to_string()),
                    sample_rate_hz: Some(sample_rate_hz),
                    channels: Some(channels),
                    ..ServeTrackOptions::default()
                },
            )
            .await
            .map_err(|err| anyhow::anyhow!("stream failed: {err:?}"))?;

            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response
                    .headers()
                    .get(axum::http::header::ACCEPT_RANGES)
                    .and_then(|v| v.to_str().ok()),
                Some("bytes"),
                "sample_rate_hz={sample_rate_hz} channels={channels} asks for no downsample or downmix, so the source must still be served byte-exact"
            );
        }

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stream_restores_direct_copy_when_policy_zeroes_cap() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = HeaderMap::new();

        let response = super::stream::stream_track_response(
            &Method::GET,
            &headers,
            &track_id,
            &super::TrackAccess::PublicPluginRoute,
            ServeTrackOptions {
                format: Some("flac".to_string()),
                bitrate_bps: Some(96_000),
                ..ServeTrackOptions::default()
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("stream failed: {err:?}"))?;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::ACCEPT_RANGES)
                .and_then(|v| v.to_str().ok()),
            Some("bytes"),
            "lossless codec + bitrate cap should land back on direct-copy once the policy drops the cap, rather than re-encoding wastefully"
        );

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stream_rejects_zero_bitrate_with_400() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = HeaderMap::new();

        let result = super::stream::stream_track_response(
            &Method::GET,
            &headers,
            &track_id,
            &super::TrackAccess::PublicPluginRoute,
            ServeTrackOptions {
                format: Some("mp3".to_string()),
                bitrate_bps: Some(0),
                ..ServeTrackOptions::default()
            },
        )
        .await;

        let status = result
            .expect_err("bitrate_bps=0 must surface a policy rejection")
            .into_response()
            .status();
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }
}
