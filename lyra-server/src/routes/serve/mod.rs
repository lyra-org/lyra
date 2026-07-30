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
pub(crate) use download::{
    DownloadTrackRequest,
    download_track_response,
};
pub(crate) use hls::{
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
                MediaTokenError,
                MediaTokenPurpose,
                validate_media_token,
            },
            require_authenticated,
            require_download,
        },
        playback_sources as playback_source_service,
    },
};

#[derive(Debug)]
pub struct ValidatedTrackSource {
    pub source_id: DbId,
    pub source_public_id: String,
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

pub(crate) async fn require_download_access(headers: &HeaderMap) -> Result<Principal, AppError> {
    require_download(headers).await.map_err(Into::into)
}

enum TrackAccess {
    Principal(Principal),
    MediaToken,
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
    track_db_id: DbId,
) -> Result<(), MediaTokenError> {
    let Some(media_token) = media_token.map(str::trim).filter(|token| !token.is_empty()) else {
        return Err(MediaTokenError::Invalid);
    };
    validate_media_token(media_token, purpose, track_db_id)
}

async fn require_authenticated_track_access(
    headers: &HeaderMap,
    track_db_id: DbId,
) -> Result<Principal, AppError> {
    let principal = require_authenticated(headers).await?;
    {
        let db = STATE.db.read().await;
        crate::services::auth::access::require_entity_accessible(
            &*db,
            &principal,
            track_db_id,
            || AppError::not_found(format!("Track not found: {}", track_db_id.0)),
        )?;
    }
    Ok(principal)
}

pub(crate) async fn require_stream_access(
    headers: &HeaderMap,
    media_token: Option<&str>,
    track_db_id: DbId,
) -> Result<(), AppError> {
    match validate_track_media_token(media_token, MediaTokenPurpose::Stream, track_db_id) {
        Ok(()) => return Ok(()),
        Err(token_error) if media_token.is_some() => {
            match require_authenticated_track_access(headers, track_db_id).await {
                Ok(_) => return Ok(()),
                Err(_) => return Err(media_token_error_to_app_error(token_error)),
            }
        }
        Err(_) => {}
    }

    require_authenticated_track_access(headers, track_db_id)
        .await
        .map(|_| ())
}

pub(crate) async fn require_hls_playlist_access(
    headers: &HeaderMap,
    media_token: Option<&str>,
    track_db_id: DbId,
) -> Result<(), AppError> {
    match validate_track_media_token(media_token, MediaTokenPurpose::HlsPlaylist, track_db_id) {
        Ok(()) => return Ok(()),
        Err(token_error) if media_token.is_some() => {
            match require_authenticated_track_access(headers, track_db_id).await {
                Ok(_) => return Ok(()),
                Err(_) => return Err(media_token_error_to_app_error(token_error)),
            }
        }
        Err(_) => {}
    }

    require_authenticated_track_access(headers, track_db_id)
        .await
        .map(|_| ())
}

async fn require_download_track_access(
    headers: &HeaderMap,
    media_token: Option<&str>,
    track_db_id: DbId,
) -> Result<TrackAccess, AppError> {
    match validate_track_media_token(media_token, MediaTokenPurpose::Download, track_db_id) {
        Ok(()) => return Ok(TrackAccess::MediaToken),
        Err(token_error) if media_token.is_some() => match require_download_access(headers).await {
            Ok(principal) => return Ok(TrackAccess::Principal(principal)),
            Err(_) => return Err(media_token_error_to_app_error(token_error)),
        },
        Err(_) => {}
    }

    require_download_access(headers)
        .await
        .map(TrackAccess::Principal)
}

pub async fn validate_and_get_track_source(
    track_db_id: DbId,
) -> Result<ValidatedTrackSource, AppError> {
    let db = &*STATE.db.read().await;
    let track = db::tracks::get_by_id(db, track_db_id)?
        .ok_or_else(|| AppError::not_found(format!("Track not found: {}", track_db_id.0)))?;

    let source = playback_source_service::resolve(db, track_db_id, false)?.ok_or_else(|| {
        AppError::not_found(format!(
            "Playable source not found for track: {}",
            track_db_id.0
        ))
    })?;
    if !source.full_path.is_file() {
        return Err(AppError::not_found(format!(
            "Track source file not found: {}",
            source.full_path.to_string_lossy()
        )));
    }

    let source_public_id =
        db::lookup::find_id_by_db_id(db, source.source_id)?.ok_or_else(|| {
            AppError::not_found(format!(
                "Playable source has no public id: {}",
                source.source_id.0
            ))
        })?;

    Ok(ValidatedTrackSource {
        source_id: source.source_id,
        source_public_id,
        track_public_id: track.id.clone(),
        input_path: source.input_path,
        entry_format: source.entry_format,
        source_codec: source
            .entry_format
            .and_then(|entry_format| entry_format.inferred_codec(track.bit_depth)),
        full_path: source.full_path,
        duration_ms: track.duration_ms,
        start_ms: source.start_ms,
        end_ms: source.end_ms,
        source_bitrate_bps: track.bitrate_bps,
        source_sample_rate_hz: track.sample_rate_hz,
        source_channels: track.channel_count,
    })
}

pub fn temp_output_path(track_db_id: DbId, format: AudioFormat) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "lyra-download-{}-{}.{}",
        track_db_id.0,
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
        require_download_access,
        require_hls_playlist_access,
        require_stream_access,
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
        services::auth::{
            media_tokens::{
                MediaTokenPurpose,
                issue_media_token,
            },
            sessions,
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

        let session = sessions::create_session_for_user(user_db_id, Default::default()).await?;
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
    async fn require_download_access_rejects_user_without_download_permission() -> anyhow::Result<()>
    {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let headers = create_user_with_permissions("listener", vec![]).await?;

        let status = require_download_access(&headers)
            .await
            .expect_err("user without permission should be rejected")
            .into_response()
            .status();
        assert_eq!(status, StatusCode::FORBIDDEN);

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test]
    async fn require_download_access_allows_user_with_download_permission() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let headers =
            create_user_with_permissions("downloader", vec![Permission::Download]).await?;

        let principal = require_download_access(&headers)
            .await
            .expect("user with download permission should be allowed");

        assert_eq!(principal.username, "downloader");

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test]
    async fn require_stream_access_allows_matching_media_token_without_bearer() -> anyhow::Result<()>
    {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;

        let track_id = agdb::DbId(123);
        let token = issue_media_token(track_id, MediaTokenPurpose::Stream);
        require_stream_access(&HeaderMap::new(), Some(&token.token), track_id)
            .await
            .expect("matching stream media token should grant access");

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test]
    async fn require_hls_playlist_access_rejects_wrong_media_token_purpose() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;

        let track_id = agdb::DbId(123);
        let token = issue_media_token(track_id, MediaTokenPurpose::Stream);
        let status = require_hls_playlist_access(&HeaderMap::new(), Some(&token.token), track_id)
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

    async fn prepare_streamable_track(test_dir: &std::path::Path) -> anyhow::Result<i64> {
        let fixture_src = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/assets/metadata/integration_track.flac");
        let fixture_dst = test_dir.join("integration_track.flac");
        tokio::fs::copy(&fixture_src, &fixture_dst).await?;

        let (tag, tagged_file) = crate::services::metadata::read_audio_tags(fixture_dst.clone())?;
        let fixture_str = fixture_dst.to_string_lossy().to_string();
        let mapping_config = crate::services::metadata::mapping::default_config();
        let raw_tags = crate::services::metadata::extract_raw_tags_from_lofty(
            &tag,
            &tagged_file,
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
        let track_id = *fixture
            .track_ids
            .first()
            .ok_or_else(|| anyhow::anyhow!("prepare_fixture produced no track ids"))?;
        Ok(track_id)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stream_direct_copy_response_advertises_byte_range_support() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = HeaderMap::new();

        let response = super::stream::stream_track_response(
            &headers,
            agdb::DbId(track_id),
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
            &headers,
            agdb::DbId(track_id),
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

    #[tokio::test(flavor = "multi_thread")]
    async fn stream_forces_transcode_when_a_knob_asks_for_a_downward_change() -> anyhow::Result<()>
    {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = HeaderMap::new();

        // The fixture is 44.1 kHz, so 22.05 kHz is a real downsample.
        let response = super::stream::stream_track_response(
            &headers,
            agdb::DbId(track_id),
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
                &headers,
                agdb::DbId(track_id),
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
            &headers,
            agdb::DbId(track_id),
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
            &headers,
            agdb::DbId(track_id),
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
