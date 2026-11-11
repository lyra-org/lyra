// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod download;
mod hls;
mod ranged_file;
mod stream;
mod transcoded;

pub use download::download_routes;
pub(crate) use hls::serve_hls_playlist_for_track;
use ranged_file::{
    RangedFileBody,
    build_ranged_body,
    requested_range,
};
pub(crate) use ranged_file::{
    build_ranged_file_body,
    settle_if_range,
};

use axum::Router;

use axum::{
    body::{
        Body,
        HttpBody as _,
    },
    http::{
        HeaderMap,
        HeaderValue,
        Method,
        Response,
        StatusCode,
        header,
    },
};
use futures::StreamExt as _;
use std::path::Path as FsPath;

use crate::routes::request::bearer_credential;
use crate::{
    routes::AppError,
    services::{
        auth::{
            access::TrackPurpose,
            media_tokens::{
                MediaTokenAccess,
                MediaTokenError,
                validate_media_token,
            },
            require_auth,
        },
        delivery::{
            DeliveryTarget,
            ServeTrackOptions,
            TrackAccess,
            ValidatedTrackSource,
            activity,
            resolve_track_delivery,
        },
        playback_sources::SourceVersion,
    },
};

fn media_token_error_to_app_error(err: MediaTokenError) -> AppError {
    match err {
        MediaTokenError::Invalid => AppError::unauthorized("invalid media token"),
        MediaTokenError::Expired => AppError::unauthorized("media token expired"),
    }
}

fn validate_track_media_token(
    media_token: Option<&str>,
    purpose: TrackPurpose,
    track_id: &str,
) -> Result<MediaTokenAccess, MediaTokenError> {
    let Some(media_token) = media_token.map(str::trim).filter(|token| !token.is_empty()) else {
        return Err(MediaTokenError::Invalid);
    };
    validate_media_token(media_token, purpose, track_id)
}

/// Grants access to a track for `purpose` through a media token bound to it, or else the
/// request's credential. A failed media token reports its own error when the request has no
/// credential that authenticates. What the purpose requires is checked where the track's source
/// is resolved.
pub(crate) async fn require_track_access(
    headers: &HeaderMap,
    media_token: Option<&str>,
    purpose: TrackPurpose,
    track_id: &str,
) -> Result<TrackAccess, AppError> {
    let token_error = match validate_track_media_token(media_token, purpose, track_id) {
        Ok(access) => return Ok(TrackAccess::MediaToken(access)),
        Err(error) => error,
    };
    let auth =
        require_auth(bearer_credential(headers))
            .await
            .map_err(|error| match media_token {
                Some(_) => media_token_error_to_app_error(token_error),
                None => error.into(),
            })?;
    Ok(TrackAccess::Credential {
        holder: auth.holder(),
        purpose,
    })
}

/// Serves a track to `target` as `GET` would. A `HEAD` request gets the same decision and
/// headers without starting FFmpeg, with the length when the transcode is already cached. A stream
/// that waits for its transcode is sent only once the transcode is complete.
pub(crate) async fn track_response(
    method: &Method,
    headers: &HeaderMap,
    track_id: &str,
    access: &TrackAccess,
    options: ServeTrackOptions,
    target: DeliveryTarget,
    wait_for_transcode: bool,
) -> Result<Response<Body>, AppError> {
    let (source, delivery) = resolve_track_delivery(track_id, access, options, target).await?;
    let response = if delivery.direct_passthrough {
        source_file_response(&source, delivery.content_type, headers).await?
    } else {
        transcoded::transcoded_response(
            method,
            headers,
            access.caller(),
            source,
            delivery,
            target,
            wait_for_transcode,
        )
        .await?
    };
    Ok(response.map(counted_as_playing))
}

/// `body`, counted as playing while it is sent. An empty body is left as
/// it is, so its known length still reaches the response headers.
fn counted_as_playing(body: Body) -> Body {
    if body.is_end_stream() {
        return body;
    }
    let playing = activity::begin_stream();
    Body::from_stream(body.into_data_stream().map(move |frame| {
        let _playing = &playing;
        frame
    }))
}

/// The body of a `HEAD` response whose `GET` body has no known length. Its size is unknown
/// rather than zero, so no `Content-Length: 0` is derived from it.
fn head_body() -> Body {
    Body::from_stream(futures::stream::empty::<Result<bytes::Bytes, std::io::Error>>())
}

pub(super) const NO_STORE: &str = "no-cache, no-store, must-revalidate";

/// A response carrying `ranged`, the body of a file served with byte ranges.
fn ranged_response(
    ranged: RangedFileBody,
    content_type: &str,
    cache_control: &str,
) -> Result<Response<Body>, AppError> {
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

    if cache_control == NO_STORE {
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
    file_response_with_cache_control(path, content_type, headers, NO_STORE).await
}

/// The range `headers` ask for of a body Lyra serves. Lyra sets no validators, so a range under
/// an `If-Range` is never served.
fn own_range(headers: &HeaderMap) -> Option<&HeaderValue> {
    requested_range(headers, &HeaderMap::new())
}

pub(crate) async fn file_response_with_cache_control(
    path: &FsPath,
    content_type: &str,
    headers: &HeaderMap,
    cache_control: &str,
) -> Result<Response<Body>, AppError> {
    let range = own_range(headers);
    let ranged = build_ranged_file_body(path, range, StatusCode::OK).await?;
    ranged_response(ranged, content_type, cache_control)
}

/// Serves the source file itself. It is served from the handle its version is read from, so what
/// is sent is the file the request was authorized for, never one that replaced it since.
async fn source_file_response(
    source: &ValidatedTrackSource,
    content_type: &str,
    headers: &HeaderMap,
) -> Result<Response<Body>, AppError> {
    let file = tokio::fs::File::open(&source.full_path).await?;
    let metadata = file.metadata().await?;
    let version = SourceVersion::of(source.version.source_public_id.clone(), &metadata)?;
    if version != source.version {
        return Err(AppError::conflict(
            "the track's source changed while it was being served; retry",
        ));
    }
    open_file_response(file, metadata.len(), content_type, headers)
}

/// Serves an already open file of `size` bytes, as [`file_response`] serves a path.
fn open_file_response(
    file: tokio::fs::File,
    size: u64,
    content_type: &str,
    headers: &HeaderMap,
) -> Result<Response<Body>, AppError> {
    open_file_response_with_cache_control(file, size, content_type, headers, NO_STORE)
}

pub(crate) fn open_file_response_with_cache_control(
    file: tokio::fs::File,
    size: u64,
    content_type: &str,
    headers: &HeaderMap,
    cache_control: &str,
) -> Result<Response<Body>, AppError> {
    let range = own_range(headers);
    let ranged = build_ranged_body(file, size, range, StatusCode::OK)?;
    ranged_response(ranged, content_type, cache_control)
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
pub(crate) mod tests {
    use super::{
        AppError,
        require_track_access,
    };
    use crate::routes::request::bearer_credential;
    use crate::services::auth::caller::{
        Caller,
        ClientAddress,
    };
    use crate::services::delivery::{
        ServeTrackOptions,
        ValidatedTrackSource,
        apply_request_start_offset,
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
    use lyra_db::nanoid;
    use lyra_ffmpeg::{
        AudioCodec,
        AudioFormat,
    };

    use crate::{
        plugins::testing::init_library_test_state,
        services::STATE,
        services::auth::{
            access::TrackPurpose,
            media_tokens::issue_unbound_media_token,
        },
        services::testing::runtime_test_lock,
        testing::LibraryFixtureConfig,
    };
    use axum::response::IntoResponse;
    use lyra_db::{
        Library,
        Permission,
        User,
        roles::Role,
    };

    pub(crate) async fn initialize_test_runtime() -> anyhow::Result<PathBuf> {
        let test_dir = std::env::temp_dir().join(format!(
            "lyra-serve-auth-test-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        std::fs::create_dir_all(&test_dir)?;
        init_library_test_state(test_dir.clone()).await?;
        Ok(test_dir)
    }

    pub(crate) async fn create_user_with_permissions(
        username: &str,
        permissions: Vec<Permission>,
    ) -> anyhow::Result<HeaderMap> {
        let user_db_id = {
            let mut db = STATE.db.write().await;
            lyra_db::roles::ensure_builtin_roles(&mut db)?;
            let user_db_id = lyra_db::users::create(
                &mut db,
                &User {
                    db_id: None,
                    id: nanoid(),
                    username: username.to_string(),
                    password: "unused".to_string(),
                },
            )?;
            let role_name = if permissions.is_empty() {
                lyra_db::roles::BUILTIN_USER_ROLE.to_string()
            } else {
                let role_name = format!("download-test-{}", nanoid());
                lyra_db::roles::create(
                    &mut db,
                    &Role {
                        db_id: None,
                        id: nanoid(),
                        name: role_name.clone(),
                        permissions,
                    },
                )?;
                role_name
            };
            lyra_db::roles::ensure_user_has_role(&mut db, user_db_id, &role_name)?;
            let library_ids = lyra_db::libraries::get(&*db)?
                .into_iter()
                .filter_map(|library| library.db_id)
                .collect::<Vec<_>>();
            for library_db_id in library_ids {
                lyra_db::libraries::grant_access(
                    &mut db,
                    user_db_id,
                    library_db_id,
                    lyra_db::libraries::AccessKind::ReadWrite,
                )?;
            }
            user_db_id
        };

        let session =
            crate::services::testing::create_session(user_db_id, Default::default()).await?;
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            format!("Bearer {}", session.token)
                .parse()
                .expect("valid auth header"),
        );
        Ok(headers)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn download_access_rejects_user_without_download_permission() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = create_user_with_permissions("listener", vec![]).await?;

        let access = require_track_access(&headers, None, TrackPurpose::Download, &track_id)
            .await
            .map_err(|_| anyhow::anyhow!("a signed-in caller is granted access to check"))?;
        for (requested, case) in [
            (track_id.as_str(), "an existing track"),
            (
                "missing-track-id",
                "a missing track, so a refusal does not reveal which exist",
            ),
        ] {
            let status = AppError::from(
                crate::services::delivery::validate_and_get_track_source(requested, &access)
                    .await
                    .expect_err("user without permission should be rejected"),
            )
            .into_response()
            .status();
            assert_eq!(status, StatusCode::FORBIDDEN, "{case}");
        }

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn download_access_allows_user_with_download_permission() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers =
            create_user_with_permissions("downloader", vec![Permission::Download]).await?;

        let access = require_track_access(&headers, None, TrackPurpose::Download, &track_id)
            .await
            .expect("user with download permission should be allowed");
        crate::services::delivery::validate_and_get_track_source(&track_id, &access)
            .await
            .map_err(|error| anyhow::anyhow!("a downloader resolves the source: {error:?}"))?;

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    /// The body of `POST /api/tracks/{id}/playback-url?{options}`.
    async fn playback_urls(
        track_id: &str,
        headers: &HeaderMap,
        options: &str,
    ) -> anyhow::Result<serde_json::Value> {
        use axum::body::{
            Body,
            to_bytes,
        };
        use tower::ServiceExt;

        let response = crate::routes::tracks::track_routes()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(format!("/{track_id}/playback-url?{options}"))
                    .header(AUTHORIZATION, headers[AUTHORIZATION].clone())
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        Ok(serde_json::from_slice(
            &to_bytes(response.into_body(), usize::MAX).await?,
        )?)
    }

    /// The query of the URL `POST /api/tracks/{id}/playback-url?{options}` mints for `transport`.
    async fn playback_url_query(
        track_id: &str,
        headers: &HeaderMap,
        transport: &str,
        options: &str,
    ) -> anyhow::Result<String> {
        let body = playback_urls(track_id, headers, options).await?;
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
        let query = playback_url_query(&track_id, &headers, "hls_url", "").await?;

        let recycled_track_id = {
            let mut db = STATE.db.write().await;
            let track_db_id = lyra_db::lookup::find_node_id_by_id(&*db, &track_id)?
                .ok_or_else(|| anyhow::anyhow!("track missing"))?;
            db.exec_mut(agdb::QueryBuilder::remove().ids(track_db_id).query())?;
            let recycled = lyra_db::fixtures::insert_track(&mut db, "Recycled")?;
            assert_eq!(recycled, track_db_id, "agdb must reuse the track DbId");
            lyra_db::lookup::find_id_by_db_id(&*db, recycled)?
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
        let auth = crate::services::auth::require_auth(bearer_credential(&headers)).await?;
        {
            let mut db = STATE.db.write().await;
            let user_db_id = auth.principal.require(&*db)?;
            db.transaction_mut(|t| lyra_db::users::delete_user(t, user_db_id))?;
        }

        let status = AppError::from(
            crate::services::delivery::validate_and_get_track_source(
                &track_id,
                &crate::services::delivery::TrackAccess::Credential {
                    holder: auth.holder(),
                    purpose: TrackPurpose::Stream,
                },
            )
            .await
            .expect_err("a deleted principal must not resolve a track source"),
        )
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
        let token = issue_unbound_media_token(track_id, TrackPurpose::Stream);
        require_track_access(
            &HeaderMap::new(),
            Some(&token.token),
            TrackPurpose::Stream,
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
        let token = issue_unbound_media_token(track_id, TrackPurpose::Stream);
        let status = require_track_access(
            &HeaderMap::new(),
            Some(&token.token),
            TrackPurpose::HlsPlaylist,
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
            format: lyra_db::track_sources::SourceFormat {
                sample_rate_hz: Some(96_000),
                channel_count: Some(2),
                bitrate_bps: Some(900_000),
                ..Default::default()
            },
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
            format: lyra_db::track_sources::SourceFormat {
                sample_rate_hz: Some(96_000),
                channel_count: Some(2),
                bitrate_bps: Some(900_000),
                ..Default::default()
            },
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
            format: lyra_db::track_sources::SourceFormat {
                sample_rate_hz: Some(96_000),
                channel_count: Some(2),
                bitrate_bps: Some(900_000),
                ..Default::default()
            },
        };

        let err = AppError::from(
            apply_request_start_offset(source, Some(20_000))
                .expect_err("offset equal to the available range must be rejected"),
        );
        assert_eq!(err.into_response().status(), StatusCode::BAD_REQUEST);
    }

    pub(crate) async fn prepare_streamable_track(
        test_dir: &std::path::Path,
    ) -> anyhow::Result<String> {
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
        lyra_db::lookup::find_id_by_db_id(&*STATE.db.read().await, agdb::DbId(track_db_id))?
            .ok_or_else(|| anyhow::anyhow!("fixture track has no public id"))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn download_serves_resolved_access_without_request_credentials() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers =
            create_user_with_permissions("plugin-downloader", vec![Permission::Download]).await?;
        let auth = crate::services::auth::require_auth(bearer_credential(&headers)).await?;

        let response = super::track_response(
            &Method::GET,
            &HeaderMap::new(),
            &track_id,
            &crate::services::delivery::TrackAccess::Credential {
                holder: auth.holder(),
                purpose: TrackPurpose::Download,
            },
            ServeTrackOptions::default(),
            crate::services::delivery::DeliveryTarget::Download,
            false,
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

        let response = super::track_response(
            &Method::GET,
            &headers,
            &track_id,
            &crate::services::delivery::TrackAccess::PublicPluginRoute(Caller::Client(
                ClientAddress::default(),
            )),
            ServeTrackOptions::default(),
            crate::services::delivery::DeliveryTarget::Stream,
            false,
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

        let response = super::track_response(
            &Method::GET,
            &headers,
            &track_id,
            &crate::services::delivery::TrackAccess::PublicPluginRoute(Caller::Client(
                ClientAddress::default(),
            )),
            ServeTrackOptions {
                format: Some("mp3".to_string()),
                bitrate_bps: Some(96_000),
                ..ServeTrackOptions::default()
            },
            crate::services::delivery::DeliveryTarget::Stream,
            false,
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
    async fn stream_waiting_for_transcode_sends_it_complete() -> anyhow::Result<()> {
        use axum::http::header::{
            ACCEPT_RANGES,
            CONTENT_LENGTH,
            CONTENT_RANGE,
            RANGE,
        };

        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let serve = |method: Method, range: Option<&'static str>| {
            let track_id = track_id.clone();
            async move {
                let mut headers = HeaderMap::new();
                if let Some(range) = range {
                    headers.insert(RANGE, range.parse()?);
                }
                let response = super::track_response(
                    &method,
                    &headers,
                    &track_id,
                    &crate::services::delivery::TrackAccess::PublicPluginRoute(Caller::Client(
                        ClientAddress::default(),
                    )),
                    ServeTrackOptions {
                        format: Some("mp3".to_string()),
                        bitrate_bps: Some(64_000),
                        ..ServeTrackOptions::default()
                    },
                    crate::services::delivery::DeliveryTarget::Stream,
                    true,
                )
                .await
                .map_err(|err| anyhow::anyhow!("stream failed: {err:?}"))?;
                let (parts, body) = response.into_parts();
                let body = axum::body::to_bytes(body, usize::MAX).await?;
                anyhow::Ok((parts.status, parts.headers, body))
            }
        };
        let cached_files = || {
            std::fs::read_dir(&STATE.boot.get().transcode_cache_dir)
                .map(|dir| dir.count())
                .unwrap_or(0)
        };

        let (status, head, _) = serve(Method::HEAD, None).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(head[ACCEPT_RANGES], "bytes");
        assert!(head.get(CONTENT_LENGTH).is_none());
        assert_eq!(cached_files(), 0, "HEAD must not start an encode");

        let (status, partial_headers, partial) = serve(Method::GET, Some("bytes=10-19")).await?;
        assert_eq!(status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(partial_headers[ACCEPT_RANGES], "bytes");
        assert_eq!(partial_headers[CONTENT_LENGTH], "10");

        let (status, full_headers, full) = serve(Method::GET, None).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(full_headers[ACCEPT_RANGES], "bytes");
        assert_eq!(full_headers[CONTENT_LENGTH], full.len().to_string());
        assert_eq!(
            partial_headers[CONTENT_RANGE],
            format!("bytes 10-19/{}", full.len())
        );
        assert_eq!(&partial[..], &full[10..20]);

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    async fn send(
        method: Method,
        uri: String,
    ) -> anyhow::Result<(StatusCode, HeaderMap, axum::body::Bytes)> {
        send_with_range(method, uri, None).await
    }

    async fn send_with_range(
        method: Method,
        uri: String,
        range: Option<&str>,
    ) -> anyhow::Result<(StatusCode, HeaderMap, axum::body::Bytes)> {
        use tower::ServiceExt;

        let mut request = axum::http::Request::builder().method(method).uri(uri);
        if let Some(range) = range {
            request = request.header(axum::http::header::RANGE, range);
        }
        let response = super::stream_routes()
            .oneshot(request.body(axum::body::Body::empty())?)
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
        let query = playback_url_query(&track_id, &headers, "stream_url", "").await?;

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
        let query = playback_url_query(&track_id, &headers, "stream_url", "").await?;
        let principal =
            crate::services::auth::require_authenticated(bearer_credential(&headers)).await?;
        {
            let mut db = STATE.db.write().await;
            let user_db_id = principal.require(&*db)?;
            db.transaction_mut(|t| lyra_db::users::delete_user(t, user_db_id))?;
        }

        let (status, _, _) = send(Method::GET, format!("/{track_id}?{query}")).await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    async fn user_db_id(headers: &HeaderMap) -> anyhow::Result<agdb::DbId> {
        let principal =
            crate::services::auth::require_authenticated(bearer_credential(headers)).await?;
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
        let principal =
            crate::services::auth::require_authenticated(bearer_credential(&headers)).await?;
        let key = crate::services::auth::api_keys::create_api_key(&principal, "phone")
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        let query = playback_url_query(&track_id, &bearer(&key.key), "stream_url", "").await?;

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
        let query = playback_url_query(&track_id, &headers, "download_url", "").await?;
        assert_eq!(
            send_download(format!("/{track_id}?{query}")).await?,
            StatusCode::OK
        );

        let user = user_db_id(&headers).await?;
        lyra_db::roles::ensure_user_has_role(
            &mut *STATE.db.write().await,
            user,
            lyra_db::roles::BUILTIN_USER_ROLE,
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
        let query = playback_url_query(&track_id, &headers, "stream_url", "").await?;

        let user = user_db_id(&headers).await?;
        {
            let mut db = STATE.db.write().await;
            let libraries = lyra_db::libraries::get(&*db)?
                .into_iter()
                .filter_map(|library| library.db_id)
                .collect::<Vec<_>>();
            for library in libraries {
                lyra_db::libraries::revoke_access(&mut *db, user, library)?;
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
        let query = playback_url_query(&track_id, &headers, "hls_url", "").await?;
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
        let query = playback_url_query(&track_id, &headers, "hls_url", "").await?;

        let (status, _, playlist) =
            send(Method::GET, format!("/{track_id}/hls.m3u8?{query}")).await?;
        assert_eq!(status, StatusCode::OK);
        let segment = first_hls_segment(&playlist)?;
        let (status, _, _) = send(Method::GET, segment.clone()).await?;
        assert_eq!(status, StatusCode::OK);

        assert!(crate::services::auth::logout_with_token(Some(&bearer_token(&headers))).await?);
        let (status, _, _) = send(Method::GET, segment).await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        crate::services::hls::state::test_helpers::reset_hls_state_for_test().await;
        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    /// The path, under the stream routes, of the first segment `playlist` lists.
    pub(crate) fn first_hls_segment(playlist: &[u8]) -> anyhow::Result<String> {
        Ok(std::str::from_utf8(playlist)?
            .lines()
            .find_map(|line| line.strip_prefix("/api/stream"))
            .ok_or_else(|| anyhow::anyhow!("playlist has no segment"))?
            .to_string())
    }

    pub(crate) async fn hls_segment_status(segment: &str) -> anyhow::Result<StatusCode> {
        Ok(send(Method::GET, segment.to_string()).await?.0)
    }

    /// Starts an HLS session with the request's own credential, and returns the path of its
    /// first segment once that has been served.
    async fn start_bearer_hls(track_id: &str, headers: &HeaderMap) -> anyhow::Result<String> {
        use tower::ServiceExt;

        let response = super::stream_routes()
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!("/{track_id}/hls.m3u8"))
                    .header(AUTHORIZATION, headers[AUTHORIZATION].clone())
                    .body(axum::body::Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let playlist = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
        let segment = first_hls_segment(&playlist)?;
        assert_eq!(hls_segment_status(&segment).await?, StatusCode::OK);
        Ok(segment)
    }

    fn bearer_headers(bearer: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            format!("Bearer {bearer}")
                .parse()
                .expect("valid auth header"),
        );
        headers
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bearer_hls_segments_stop_once_the_session_holder_signs_out() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = create_user_with_permissions("bearer-hls-leaver", vec![]).await?;
        let segment = start_bearer_hls(&track_id, &headers).await?;

        assert!(crate::services::auth::logout_with_token(Some(&bearer_token(&headers))).await?);
        assert_eq!(
            hls_segment_status(&segment).await?,
            StatusCode::UNAUTHORIZED
        );

        crate::services::hls::state::test_helpers::reset_hls_state_for_test().await;
        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_refused_hls_segment_leaves_the_session_idle() -> anyhow::Result<()> {
        use crate::services::hls::state::HLS_SESSIONS;

        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        crate::services::hls::state::test_helpers::reset_hls_state_for_test().await;
        let headers = create_user_with_permissions("hls-poller", vec![]).await?;
        let segment = start_bearer_hls(&track_id, &headers).await?;
        let last_access = || async {
            let sessions = HLS_SESSIONS.read().await;
            let mut sessions = sessions.values();
            let session = sessions.next().expect("the session is still attached");
            assert!(sessions.next().is_none());
            session.last_access
        };

        assert!(crate::services::auth::logout_with_token(Some(&bearer_token(&headers))).await?);
        let served_at = last_access().await;
        assert_eq!(
            hls_segment_status(&segment).await?,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(last_access().await, served_at);

        crate::services::hls::state::test_helpers::reset_hls_state_for_test().await;
        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bearer_hls_segments_stop_once_the_api_key_is_revoked() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let session = create_user_with_permissions("hls-key-owner", vec![]).await?;
        let principal =
            crate::services::auth::require_authenticated(bearer_credential(&session)).await?;
        let key = crate::services::auth::api_keys::create_api_key(&principal, "player").await?;
        let segment = start_bearer_hls(&track_id, &bearer_headers(&key.key)).await?;

        assert!(
            crate::services::auth::api_keys::revoke_api_key_for_user(&principal, &key.id).await?
        );
        assert_eq!(
            hls_segment_status(&segment).await?,
            StatusCode::UNAUTHORIZED
        );

        crate::services::hls::state::test_helpers::reset_hls_state_for_test().await;
        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bearer_hls_segments_stop_once_library_access_is_removed() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = create_user_with_permissions("hls-shut-out", vec![]).await?;
        let principal =
            crate::services::auth::require_authenticated(bearer_credential(&headers)).await?;
        let segment = start_bearer_hls(&track_id, &headers).await?;

        {
            let mut db = STATE.db.write().await;
            let user_db_id = principal.require(&*db)?;
            let library_ids = lyra_db::libraries::get(&*db)?
                .into_iter()
                .filter_map(|library| library.db_id)
                .collect::<Vec<_>>();
            for library_db_id in library_ids {
                lyra_db::libraries::revoke_access(&mut db, user_db_id, library_db_id)?;
            }
        }
        assert_eq!(hls_segment_status(&segment).await?, StatusCode::NOT_FOUND);

        crate::services::hls::state::test_helpers::reset_hls_state_for_test().await;
        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn default_user_hls_segments_stop_once_auth_is_enabled() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let mut config = STATE.config().as_ref().clone();
        config.auth.enabled = false;
        crate::services::testing::publish_config(config);
        crate::services::auth::ensure_default_user(&STATE.config()).await?;
        let default_db_id = crate::services::auth::require_authenticated(None)
            .await?
            .require(&*STATE.db.read().await)?;
        let session =
            crate::services::testing::create_session(default_db_id, Default::default()).await?;
        let segment = start_bearer_hls(&track_id, &bearer_headers(&session.token)).await?;

        let mut config = STATE.config().as_ref().clone();
        config.auth.enabled = true;
        crate::services::testing::publish_config(config);
        assert_eq!(
            hls_segment_status(&segment).await?,
            StatusCode::UNAUTHORIZED
        );

        crate::services::hls::state::test_helpers::reset_hls_state_for_test().await;
        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn track_source_rejects_a_signed_out_credential() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = create_user_with_permissions("signed-out", vec![]).await?;
        let auth = crate::services::auth::require_auth(bearer_credential(&headers)).await?;
        let access = crate::services::delivery::TrackAccess::Credential {
            holder: auth.holder(),
            purpose: TrackPurpose::Stream,
        };
        crate::services::delivery::validate_and_get_track_source(&track_id, &access)
            .await
            .map_err(|_| anyhow::anyhow!("a signed-in credential resolves the track's source"))?;

        assert!(crate::services::auth::logout_with_token(Some(&bearer_token(&headers))).await?);
        let status = AppError::from(
            crate::services::delivery::validate_and_get_track_source(&track_id, &access)
                .await
                .expect_err("a signed-out credential must not resolve a track source"),
        )
        .into_response()
        .status();
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn track_source_rejects_a_download_once_download_permission_is_removed()
    -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers =
            create_user_with_permissions("demoted-mid-request", vec![Permission::Download]).await?;
        let access = require_track_access(&headers, None, TrackPurpose::Download, &track_id)
            .await
            .map_err(|_| anyhow::anyhow!("a caller who may download is granted access"))?;

        let user = user_db_id(&headers).await?;
        lyra_db::roles::ensure_user_has_role(
            &mut *STATE.db.write().await,
            user,
            lyra_db::roles::BUILTIN_USER_ROLE,
        )?;
        let status = AppError::from(
            crate::services::delivery::validate_and_get_track_source(&track_id, &access)
                .await
                .expect_err("a demoted caller must not resolve a track source to download"),
        )
        .into_response()
        .status();
        assert_eq!(status, StatusCode::FORBIDDEN);

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
                &crate::services::delivery::TrackAccess::PublicPluginRoute(Caller::Client(
                    ClientAddress::default(),
                )),
                crate::services::delivery::HlsPlaylistOptions {
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
        let query = playback_url_query(&track_id, &headers, "stream_url", "").await?;

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
    async fn a_track_file_replaced_by_a_link_out_of_the_library_is_not_served() -> anyhow::Result<()>
    {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = create_user_with_permissions("link-listener", vec![]).await?;
        let query = playback_url_query(&track_id, &headers, "stream_url", "").await?;

        let outside = tempfile::tempdir()?;
        let config = outside.path().join("config.json");
        std::fs::write(&config, "secret")?;
        let path = test_dir.join("integration_track.flac");
        std::fs::remove_file(&path)?;
        std::os::unix::fs::symlink(&config, &path)?;

        let (status, _, body) = send(Method::GET, format!("/{track_id}?{query}")).await?;
        assert_eq!(status, StatusCode::GONE);
        assert!(!body.windows(6).any(|window| window == b"secret"));

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_linked_folder_plays_once_its_library_follows_links() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        prepare_streamable_track(&test_dir).await?;
        let outside = tempfile::tempdir()?;
        let target = outside.path().join("elsewhere.flac");
        std::fs::copy(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests/assets/metadata/integration_track.flac"),
            &target,
        )?;
        std::os::unix::fs::symlink(outside.path(), test_dir.join("linked"))?;
        let library = lyra_db::libraries::get(&*STATE.db.read().await)?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("library missing"))?;
        let library = STATE.db.write().await.transaction_mut(|t| {
            lyra_db::libraries::update(
                t,
                &Library {
                    follow_links: Some(true),
                    ..library
                },
            )
            .map_err(anyhow::Error::from)
        })?;
        crate::services::libraries::sync_library(&STATE.db.get(), &library).await?;
        let track_id = {
            let db = STATE.db.read().await;
            let entry = lyra_db::entries::load_existing(
                &*db,
                library.db_id.expect("stored library has an id"),
            )?
            .into_iter()
            .find(|entry| entry.full_path == test_dir.join("linked/elsewhere.flac"))
            .ok_or_else(|| anyhow::anyhow!("the linked file was not scanned"))?;
            lyra_db::tracks::get_by_entry(&db, entry.db_id.expect("stored entry has an id"))?
                .into_iter()
                .next()
                .ok_or_else(|| anyhow::anyhow!("the linked file has no track"))?
                .id
        };
        let headers = create_user_with_permissions("linked-listener", vec![]).await?;
        let query = playback_url_query(&track_id, &headers, "stream_url", "").await?;

        let (status, _, body) = send(Method::GET, format!("/{track_id}?{query}")).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, std::fs::read(&target)?);

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_followed_link_serves_its_target_under_a_media_token() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let outside = tempfile::tempdir()?;
        let target = outside.path().join("elsewhere.flac");
        let path = test_dir.join("integration_track.flac");
        std::fs::copy(&path, &target)?;
        std::fs::remove_file(&path)?;
        std::os::unix::fs::symlink(&target, &path)?;
        {
            let mut db = STATE.db.write().await;
            let library = lyra_db::libraries::get(&*db)?
                .into_iter()
                .next()
                .ok_or_else(|| anyhow::anyhow!("library missing"))?;
            db.transaction_mut(|t| {
                lyra_db::libraries::update(
                    t,
                    &Library {
                        follow_links: Some(true),
                        ..library
                    },
                )
                .map_err(anyhow::Error::from)
            })?;
        }
        let headers = create_user_with_permissions("followed-listener", vec![]).await?;
        let query = playback_url_query(&track_id, &headers, "stream_url", "").await?;

        let (status, _, body) = send(Method::GET, format!("/{track_id}?{query}")).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, std::fs::read(&target)?);

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stream_serves_a_finished_transcode_from_the_cache() -> anyhow::Result<()> {
        use axum::http::header::{
            ACCEPT_RANGES,
            CONTENT_LENGTH,
            CONTENT_RANGE,
            TRANSFER_ENCODING,
        };

        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = create_user_with_permissions("cache-listener", vec![]).await?;
        let token_query = playback_url_query(
            &track_id,
            &headers,
            "stream_url",
            "format=mp3&bitrate_bps=96000",
        )
        .await?;
        let uri = format!("/{track_id}?{token_query}");

        let (status, first_headers, encoded) = send(Method::GET, uri.clone()).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(first_headers[ACCEPT_RANGES], "none");
        assert_eq!(first_headers[TRANSFER_ENCODING], "chunked");
        assert!(!encoded.is_empty());

        let (status, cached_headers, cached) = send(Method::GET, uri.clone()).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(cached_headers[ACCEPT_RANGES], "bytes");
        assert_eq!(cached_headers[CONTENT_LENGTH], encoded.len().to_string());
        assert_eq!(cached, encoded);

        let (status, head_headers, _) = send(Method::HEAD, uri.clone()).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(head_headers[CONTENT_LENGTH], encoded.len().to_string());

        let (status, range_headers, range) =
            send_with_range(Method::GET, uri, Some("bytes=10-19")).await?;
        assert_eq!(status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            range_headers[CONTENT_RANGE],
            format!("bytes 10-19/{}", encoded.len())
        );
        assert_eq!(&range[..], &encoded[10..20]);

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_user_at_the_transcode_limit_is_refused_new_encodes() -> anyhow::Result<()> {
        use crate::services::transcoding::capacity::try_acquire_for;

        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = create_user_with_permissions("busy-listener", vec![]).await?;
        let user = crate::services::auth::require_authenticated(bearer_credential(&headers))
            .await?
            .user_public_id;
        let cached = playback_url_query(
            &track_id,
            &headers,
            "stream_url",
            "format=mp3&bitrate_bps=96000",
        )
        .await?;
        let cached = format!("/{track_id}?{cached}");
        let uncached = playback_url_query(
            &track_id,
            &headers,
            "stream_url",
            "format=mp3&bitrate_bps=64000",
        )
        .await?;
        let uncached = format!("/{track_id}?{uncached}");
        let original = format!(
            "/{track_id}?{}",
            playback_url_query(&track_id, &headers, "stream_url", "").await?
        );
        let (status, _, _) = send(Method::GET, cached.clone()).await?;
        assert_eq!(status, StatusCode::OK);

        let limit = STATE.config().transcoding.max_concurrent_per_user;
        let held = (0..limit)
            .map(|_| try_acquire_for(&Caller::User(user.clone())))
            .collect::<Result<Vec<_>, _>>()?;

        let (status, headers, _) = send(Method::GET, uncached).await?;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert!(headers.contains_key(axum::http::header::RETRY_AFTER));

        let (status, _, _) = send(Method::GET, cached).await?;
        assert_eq!(status, StatusCode::OK, "a cached transcode needs no permit");
        let (status, _, _) = send(Method::GET, original).await?;
        assert_eq!(status, StatusCode::OK, "the original file needs no permit");

        drop(held);
        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn public_route_transcodes_count_against_whoever_they_are_accounted_to()
    -> anyhow::Result<()> {
        use crate::services::transcoding::capacity::try_acquire_for;

        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let limit = STATE.config().transcoding.max_concurrent_per_user;
        let held = (0..limit)
            .map(|_| try_acquire_for(&Caller::User("busy-user".to_string())))
            .collect::<Result<Vec<_>, _>>()?;
        let mp3 = || ServeTrackOptions {
            format: Some("mp3".to_string()),
            ..ServeTrackOptions::default()
        };

        let refused = super::track_response(
            &Method::GET,
            &HeaderMap::new(),
            &track_id,
            &crate::services::delivery::TrackAccess::PublicPluginRoute(Caller::User(
                "busy-user".to_string(),
            )),
            mp3(),
            crate::services::delivery::DeliveryTarget::Stream,
            false,
        )
        .await
        .expect_err("the caller's allowance is used up")
        .into_response();
        assert_eq!(refused.status(), StatusCode::TOO_MANY_REQUESTS);

        let other_client = super::track_response(
            &Method::GET,
            &HeaderMap::new(),
            &track_id,
            &crate::services::delivery::TrackAccess::PublicPluginRoute(Caller::Client(
                ClientAddress::new("10.0.0.9".parse().expect("valid ip")),
            )),
            mp3(),
            crate::services::delivery::DeliveryTarget::Stream,
            false,
        )
        .await
        .map_err(|err| anyhow::anyhow!("stream failed: {err:?}"))?;
        assert_eq!(other_client.status(), StatusCode::OK);
        let _ = axum::body::to_bytes(other_client.into_body(), usize::MAX).await?;

        drop(held);
        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_cached_flac_transcode_has_a_complete_streaminfo() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = create_user_with_permissions("flac-listener", vec![]).await?;
        let token_query = playback_url_query(
            &track_id,
            &headers,
            "stream_url",
            "format=flac&sample_rate_hz=22050",
        )
        .await?;
        let uri = format!("/{track_id}?{token_query}");

        let (status, _, _) = send(Method::GET, uri.clone()).await?;
        assert_eq!(status, StatusCode::OK);
        let (status, _, flac) = send(Method::GET, uri).await?;
        assert_eq!(status, StatusCode::OK);

        assert_eq!(&flac[..4], b"fLaC");
        assert_eq!(flac[4] & 0x7f, 0, "the first metadata block is STREAMINFO");
        let streaminfo = &flac[8..42];
        let total_samples = (u64::from(streaminfo[13] & 0x0f) << 32)
            | u64::from(u32::from_be_bytes([
                streaminfo[14],
                streaminfo[15],
                streaminfo[16],
                streaminfo[17],
            ]));
        assert!(total_samples > 0, "STREAMINFO carries the sample count");
        assert!(
            streaminfo[18..34].iter().any(|byte| *byte != 0),
            "STREAMINFO carries the audio's MD5"
        );

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn playback_url_describes_what_each_url_delivers() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers =
            create_user_with_permissions("describing-listener", vec![Permission::Download]).await?;

        let original = playback_urls(&track_id, &headers, "").await?;
        let delivery = &original["stream_delivery"];
        assert_eq!(delivery["transcode"], false);
        assert_eq!(delivery["reasons"], serde_json::json!([]));
        assert_eq!(delivery["container"], "flac");
        assert_eq!(delivery["codec"], "flac");
        assert_eq!(delivery["content_type"], "audio/flac");
        assert_eq!(delivery["sample_rate_hz"], 44_100);
        assert_eq!(delivery["bit_depth"], 16);
        assert_eq!(delivery["source"]["container"], "flac");
        assert_eq!(original["download_delivery"]["transcode"], false);

        let capped = playback_urls(&track_id, &headers, "bit_depth=16").await?;
        assert_eq!(
            capped["stream_delivery"]["transcode"], false,
            "a cap the 16-bit source already meets keeps it as it is"
        );

        let mp3 = playback_urls(&track_id, &headers, "format=mp3&bitrate_bps=96000").await?;
        let delivery = &mp3["stream_delivery"];
        assert_eq!(delivery["transcode"], true);
        assert_eq!(
            delivery["reasons"],
            serde_json::json!(["container", "bitrate"])
        );
        assert_eq!(delivery["container"], "mp3");
        assert_eq!(delivery["codec"], "mp3");
        assert_eq!(delivery["content_type"], "audio/mpeg");
        assert_eq!(delivery["bitrate_bps"], 96_000);
        assert_eq!(delivery["bit_depth"], serde_json::Value::Null);
        assert_eq!(delivery["source"]["codec"], "flac");

        let caf = playback_urls(&track_id, &headers, "format=caf").await?;
        assert!(caf.get("stream_url").is_none());
        assert!(caf.get("stream_delivery").is_none());
        assert_eq!(caf["download_delivery"]["container"], "caf");

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_media_token_serves_only_the_delivery_it_was_issued_for() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = create_user_with_permissions("bound-listener", vec![]).await?;
        let mp3_query = playback_url_query(
            &track_id,
            &headers,
            "stream_url",
            "format=mp3&bitrate_bps=96000",
        )
        .await?;

        let (status, headers_out, _) =
            send(Method::GET, format!("/{track_id}?{mp3_query}")).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers_out[axum::http::header::CONTENT_TYPE], "audio/mpeg");

        let (status, _, _) =
            send(Method::GET, format!("/{track_id}?{mp3_query}&format=flac")).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _, _) = send(
            Method::GET,
            format!("/{track_id}?{mp3_query}&start_offset_ms=100"),
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "the start offset stays per request");

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn track_responses_keep_their_length_headers_over_http() -> anyhow::Result<()> {
        use axum::http::header::{
            CONTENT_LENGTH,
            RANGE,
            TRANSFER_ENCODING,
        };

        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = create_user_with_permissions("http-listener", vec![]).await?;
        let token_query = playback_url_query(&track_id, &headers, "stream_url", "").await?;
        let mp3_query = playback_url_query(
            &track_id,
            &headers,
            "stream_url",
            "format=mp3&bitrate_bps=96000",
        )
        .await?;
        let (_, _, file) = send(Method::GET, format!("/{track_id}?{token_query}")).await?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let base = format!("http://{}/{track_id}", listener.local_addr()?);
        tokio::spawn(async move { axum::serve(listener, super::stream_routes()).await });
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client = reqwest::Client::new();
        let direct = format!("{base}?{token_query}");
        let length = |response: &reqwest::Response| {
            response
                .headers()
                .get(CONTENT_LENGTH)
                .map(|value| value.to_str().unwrap_or_default().to_string())
        };

        let full = client.get(&direct).send().await?;
        assert_eq!(length(&full), Some(file.len().to_string()));
        assert!(full.headers().get(TRANSFER_ENCODING).is_none());
        assert_eq!(full.bytes().await?, file);
        let ranged = client
            .get(&direct)
            .header(RANGE, "bytes=0-9")
            .send()
            .await?;
        assert_eq!(ranged.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(length(&ranged), Some("10".to_string()));
        let head = client.head(&direct).send().await?;
        assert_eq!(length(&head), Some(file.len().to_string()));
        let beyond = format!("bytes={}-", file.len() + 10);
        let unsatisfiable = client.get(&direct).header(RANGE, beyond).send().await?;
        assert_eq!(unsatisfiable.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(length(&unsatisfiable), Some("0".to_string()));
        let transcode_head = client.head(format!("{base}?{mp3_query}")).send().await?;
        assert_eq!(transcode_head.status(), StatusCode::OK);
        assert_eq!(
            length(&transcode_head),
            None,
            "a transcode's length is unknown"
        );

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
        let token_query = playback_url_query(&track_id, &headers, "stream_url", "").await?;
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

        let mp3_query = playback_url_query(
            &track_id,
            &headers,
            "stream_url",
            "format=mp3&bitrate_bps=96000",
        )
        .await?;
        let (status, headers, body) =
            send(Method::HEAD, format!("/{track_id}?{mp3_query}")).await?;
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

    fn hls_options() -> crate::services::delivery::HlsPlaylistOptions {
        crate::services::delivery::HlsPlaylistOptions {
            codec: None,
            bitrate_bps: None,
            sample_rate_hz: None,
            channels: None,
            prefer_vbr: None,
            start_offset_ms: None,
        }
    }

    /// Takes every transcoding permit, one user at a time.
    fn take_all_permits() -> Vec<crate::services::transcoding::capacity::TranscodePermit> {
        let global = STATE.config().transcoding.max_concurrent;
        (0..global)
            .map(|user| {
                crate::services::transcoding::capacity::try_acquire_for(&Caller::User(format!(
                    "holder-{user}"
                )))
                .expect("a permit is free")
            })
            .collect()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_cancelled_hls_permit_wait_does_not_block_later_requests() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        crate::services::hls::state::test_helpers::reset_hls_state_for_test().await;

        let held = take_all_permits();
        let waiting = tokio::spawn({
            let track_id = track_id.clone();
            async move {
                super::serve_hls_playlist_for_track(
                    &Method::GET,
                    &track_id,
                    &crate::services::delivery::TrackAccess::PublicPluginRoute(Caller::Client(
                        ClientAddress::default(),
                    )),
                    hls_options(),
                )
                .await
                .map(|_| ())
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(!waiting.is_finished(), "the job waits for a permit");
        waiting.abort();
        let _ = waiting.await;
        drop(held);

        let response = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            super::serve_hls_playlist_for_track(
                &Method::GET,
                &track_id,
                &crate::services::delivery::TrackAccess::PublicPluginRoute(Caller::Client(
                    ClientAddress::default(),
                )),
                hls_options(),
            ),
        )
        .await
        .map_err(|_| anyhow::anyhow!("a later request must not wait on the cancelled one"))?
        .map_err(|err| anyhow::anyhow!("playlist failed: {err:?}"))?;
        assert_eq!(response.status(), StatusCode::OK);

        crate::services::hls::state::test_helpers::reset_hls_state_for_test().await;
        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_hls_job_releases_its_permit_once_its_transcode_stops() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        crate::services::hls::state::test_helpers::reset_hls_state_for_test().await;

        super::serve_hls_playlist_for_track(
            &Method::GET,
            &track_id,
            &crate::services::delivery::TrackAccess::PublicPluginRoute(Caller::Client(
                ClientAddress::default(),
            )),
            hls_options(),
        )
        .await
        .map_err(|err| anyhow::anyhow!("playlist failed: {err:?}"))?;
        assert_eq!(
            crate::services::hls::state::hls_registry_counts().await.0,
            1
        );

        let started = std::time::Instant::now();
        let held = loop {
            let global = STATE.config().transcoding.max_concurrent;
            let held = (0..global)
                .map_while(|user| {
                    crate::services::transcoding::capacity::try_acquire_for(&Caller::User(format!(
                        "holder-{user}"
                    )))
                    .ok()
                })
                .collect::<Vec<_>>();
            if held.len() == global as usize {
                break held;
            }
            drop(held);
            assert!(
                started.elapsed() < std::time::Duration::from_secs(5),
                "the finished HLS transcode still holds its permit"
            );
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        };
        assert_eq!(
            crate::services::hls::state::hls_registry_counts().await.0,
            1,
            "the job itself lives on to serve its segments"
        );
        drop(held);

        crate::services::hls::state::test_helpers::reset_hls_state_for_test().await;
        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_hls_token_serves_only_the_delivery_it_was_issued_for() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = create_user_with_permissions("bound-hls-listener", vec![]).await?;
        let query = playback_url_query(&track_id, &headers, "hls_url", "").await?;

        let (status, _, _) = send(
            Method::GET,
            format!("/{track_id}/hls.m3u8?{query}&codec=flac"),
        )
        .await?;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        crate::services::hls::state::test_helpers::reset_hls_state_for_test().await;
        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_missing_source_file_is_reported_without_its_path() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        std::fs::remove_file(test_dir.join("integration_track.flac"))?;

        let err = crate::services::delivery::describe_stream(
            &track_id,
            &crate::services::delivery::TrackAccess::PublicPluginRoute(Caller::Client(
                ClientAddress::default(),
            )),
            ServeTrackOptions::default(),
        )
        .await
        .map_err(AppError::from)
        .expect_err("the source file is gone");
        let message = err.to_string();
        assert!(message.starts_with("410"), "{message}");
        assert!(
            !message.contains(&*test_dir.to_string_lossy()),
            "the message must not reveal where the library lives: {message}"
        );

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
            &crate::services::delivery::TrackAccess::PublicPluginRoute(Caller::Client(
                ClientAddress::default(),
            )),
            crate::services::delivery::HlsPlaylistOptions {
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

        let response = super::track_response(
            &Method::GET,
            &HeaderMap::new(),
            &track_id,
            &crate::services::delivery::TrackAccess::PublicPluginRoute(Caller::Client(
                ClientAddress::default(),
            )),
            ServeTrackOptions {
                format: Some("m4a".to_string()),
                codec: Some("aac".to_string()),
                ..ServeTrackOptions::default()
            },
            crate::services::delivery::DeliveryTarget::Stream,
            false,
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
        let response = super::track_response(
            &Method::GET,
            &headers,
            &track_id,
            &crate::services::delivery::TrackAccess::PublicPluginRoute(Caller::Client(
                ClientAddress::default(),
            )),
            ServeTrackOptions {
                format: Some("flac".to_string()),
                sample_rate_hz: Some(22_050),
                ..ServeTrackOptions::default()
            },
            crate::services::delivery::DeliveryTarget::Stream,
            false,
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
            let response = super::track_response(
                &Method::GET,
                &headers,
                &track_id,
                &crate::services::delivery::TrackAccess::PublicPluginRoute(Caller::Client(
                    ClientAddress::default(),
                )),
                ServeTrackOptions {
                    format: Some("flac".to_string()),
                    sample_rate_hz: Some(sample_rate_hz),
                    channels: Some(channels),
                    ..ServeTrackOptions::default()
                },
                crate::services::delivery::DeliveryTarget::Stream,
                false,
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

        let response = super::track_response(
            &Method::GET,
            &headers,
            &track_id,
            &crate::services::delivery::TrackAccess::PublicPluginRoute(Caller::Client(
                ClientAddress::default(),
            )),
            ServeTrackOptions {
                format: Some("flac".to_string()),
                bitrate_bps: Some(96_000),
                ..ServeTrackOptions::default()
            },
            crate::services::delivery::DeliveryTarget::Stream,
            false,
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

        let result = super::track_response(
            &Method::GET,
            &headers,
            &track_id,
            &crate::services::delivery::TrackAccess::PublicPluginRoute(Caller::Client(
                ClientAddress::default(),
            )),
            ServeTrackOptions {
                format: Some("mp3".to_string()),
                bitrate_bps: Some(0),
                ..ServeTrackOptions::default()
            },
            crate::services::delivery::DeliveryTarget::Stream,
            false,
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

    #[tokio::test(flavor = "multi_thread")]
    async fn stream_rejects_an_unsupported_bit_depth_with_400() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = HeaderMap::new();

        let result = super::track_response(
            &Method::GET,
            &headers,
            &track_id,
            &crate::services::delivery::TrackAccess::PublicPluginRoute(Caller::Client(
                ClientAddress::default(),
            )),
            ServeTrackOptions {
                bit_depth: Some(20),
                ..ServeTrackOptions::default()
            },
            crate::services::delivery::DeliveryTarget::Stream,
            false,
        )
        .await;

        let status = result
            .expect_err("bit_depth=20 must surface a policy rejection")
            .into_response()
            .status();
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }
}
