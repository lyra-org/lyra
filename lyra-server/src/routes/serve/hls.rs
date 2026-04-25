// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use aide::axum::{
    ApiRouter,
    routing::get_with,
};
use aide::transform::TransformOperation;
use axum::{
    body::Body,
    extract::{
        Path,
        Query,
    },
    http::{
        HeaderMap,
        Response,
        header,
    },
};
use schemars::JsonSchema;
use serde::Deserialize;
use std::{
    io::ErrorKind,
    path::Path as FsPath,
    time::{
        Duration,
        Instant,
    },
};
use tokio::time::sleep;

use crate::{
    STATE,
    db,
    routes::AppError,
    services::hls::{
        codec::{
            HlsCodecProfile,
            hls_media_content_type,
        },
        signing::{
            HlsSegmentQuery,
            hls_signed_segment_query,
            rewrite_playlist_segments,
            validate_signed_segment_query,
        },
        state::{
            HLS_JOBS,
            HLS_SESSIONS,
            attach_session_to_job,
            authorize_hls_segment_session,
            detach_hls_session,
            generate_hls_session_id,
            get_or_create_hls_job,
            hls_job_key,
            hls_registry_counts,
        },
    },
};
use agdb::DbId;

use super::{
    file_response,
    require_download_access,
    validate_and_get_track_source,
    validate_request,
};

const HLS_PLAYLIST_WAIT_TIMEOUT: Duration = Duration::from_secs(2);
const HLS_SEGMENT_WAIT_TIMEOUT: Duration = Duration::from_secs(2);
const HLS_FILE_POLL_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Deserialize, JsonSchema)]
struct HlsQuery {
    #[schemars(description = "Optional HLS audio codec (aac, alac, flac).")]
    codec: Option<String>,
}

fn sanitize_segment_name(segment: &str) -> Result<&str, AppError> {
    if segment.is_empty()
        || segment.len() > 128
        || segment.contains('/')
        || segment.contains('\\')
        || segment.contains("..")
    {
        return Err(AppError::bad_request("invalid HLS segment path"));
    }

    if !segment
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_'))
    {
        return Err(AppError::bad_request("invalid HLS segment path"));
    }

    Ok(segment)
}

async fn wait_for_generated_playlist(
    playlist_path: &FsPath,
    timeout: Duration,
) -> Result<Option<String>, std::io::Error> {
    let deadline = Instant::now() + timeout;
    loop {
        match tokio::fs::read_to_string(playlist_path).await {
            Ok(playlist) if !playlist.trim().is_empty() => return Ok(Some(playlist)),
            Ok(_) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }

        if Instant::now() >= deadline {
            return Ok(None);
        }

        sleep(HLS_FILE_POLL_INTERVAL).await;
    }
}

async fn wait_for_generated_segment(
    segment_path: &FsPath,
    timeout: Duration,
) -> Result<bool, std::io::Error> {
    let deadline = Instant::now() + timeout;
    loop {
        match tokio::fs::metadata(segment_path).await {
            Ok(metadata) if metadata.is_file() => return Ok(true),
            Ok(_) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }

        if Instant::now() >= deadline {
            return Ok(false);
        }

        sleep(HLS_FILE_POLL_INTERVAL).await;
    }
}

async fn resolve_generated_playlist(
    track_db_id: DbId,
    session_id: &str,
    playlist_path: &FsPath,
    playlist_wait_started: Instant,
    wait_result: Result<Option<String>, std::io::Error>,
) -> Result<String, AppError> {
    match wait_result {
        Ok(Some(playlist)) => Ok(playlist),
        Ok(None) => {
            let playlist_wait_ms = playlist_wait_started.elapsed().as_millis() as u64;
            let _ = detach_hls_session(session_id).await;
            tracing::warn!(
                track_db_id = track_db_id.0,
                %session_id,
                playlist_wait_ms,
                "playlist generation for HLS timed out before first response"
            );
            Err(AppError::service_unavailable(
                "HLS playlist is still being generated",
            ))
        }
        Err(err) => {
            let playlist_wait_ms = playlist_wait_started.elapsed().as_millis() as u64;
            tracing::warn!(
                track_db_id = track_db_id.0,
                %session_id,
                path = %playlist_path.display(),
                playlist_wait_ms,
                error = %err,
                "generated HLS playlist could not be read"
            );
            let _ = detach_hls_session(session_id).await;
            Err(err.into())
        }
    }
}

async fn get_hls_playlist(
    headers: HeaderMap,
    Path(track_id): Path<String>,
    Query(query): Query<HlsQuery>,
) -> Result<Response<Body>, AppError> {
    let track_db_id = {
        let db = STATE.db.read().await;
        db::lookup::find_node_id_by_id(&*db, &track_id)?
            .ok_or_else(|| AppError::not_found(format!("not found: {track_id}")))?
    };
    serve_hls_playlist_for_track(&headers, track_db_id, query.codec).await
}

pub(crate) async fn serve_hls_playlist_for_track(
    headers: &HeaderMap,
    track_db_id: DbId,
    codec: Option<String>,
) -> Result<Response<Body>, AppError> {
    let request_started = Instant::now();
    let principal = require_download_access(headers).await?;

    let source = validate_and_get_track_source(track_db_id).await?;
    let validated = validate_request(None, codec)?;
    let profile = HlsCodecProfile::from_requested(validated.codec)?;

    let session_id = generate_hls_session_id();
    let job_key = hls_job_key(
        track_db_id,
        source.source_id,
        source.start_ms,
        source.end_ms,
        profile,
    );

    let reused_job = get_or_create_hls_job(&job_key, &source.input_path, profile).await?;
    let playlist_path =
        attach_session_to_job(&session_id, principal.user_db_id, track_db_id, job_key).await?;

    let playlist_wait_started = Instant::now();
    let playlist_wait_result =
        wait_for_generated_playlist(&playlist_path, HLS_PLAYLIST_WAIT_TIMEOUT).await;
    let playlist = resolve_generated_playlist(
        track_db_id,
        &session_id,
        &playlist_path,
        playlist_wait_started,
        playlist_wait_result,
    )
    .await?;

    let signed_query = hls_signed_segment_query(track_db_id, &session_id);
    let playlist = rewrite_playlist_segments(&playlist, track_db_id, &session_id, &signed_query);

    let response = Response::builder()
        .header(header::CONTENT_TYPE, "application/x-mpegurl")
        .header(header::CACHE_CONTROL, "no-cache, no-store, must-revalidate")
        .header(header::PRAGMA, "no-cache")
        .header(header::EXPIRES, "0")
        .body(Body::from(playlist))?;

    let playlist_wait_ms = playlist_wait_started.elapsed().as_millis() as u64;
    let startup_latency_ms = request_started.elapsed().as_millis() as u64;
    let (active_jobs, active_sessions) = hls_registry_counts().await;
    tracing::info!(
        track_db_id = track_db_id.0,
        source_db_id = source.source_id.0,
        %session_id,
        codec = profile.ffmpeg_codec_str,
        startup_latency_ms,
        playlist_wait_ms,
        active_jobs,
        active_sessions,
        shared_job_reused = reused_job,
        "served HLS playlist"
    );

    Ok(response)
}

async fn get_hls_segment(
    headers: HeaderMap,
    Path((track_db_id, session_id, segment)): Path<(i64, String, String)>,
    Query(query): Query<HlsSegmentQuery>,
) -> Result<Response<Body>, AppError> {
    let track_db_id = DbId(track_db_id);

    let segment = sanitize_segment_name(&segment)?;
    let signed_request_is_valid = validate_signed_segment_query(track_db_id, &session_id, &query);
    let principal = if signed_request_is_valid {
        None
    } else {
        Some(require_download_access(&headers).await?)
    };

    let job_key = {
        let mut sessions = HLS_SESSIONS.write().await;
        let session = sessions
            .get_mut(&session_id)
            .ok_or_else(|| AppError::not_found("HLS session not found"))?;

        authorize_hls_segment_session(session, track_db_id, principal.map(|p| p.user_db_id))?;

        session.last_access = Instant::now();
        session.job_key.clone()
    };

    let segment_dir = {
        let jobs = HLS_JOBS.read().await;
        jobs.get(&job_key)
            .ok_or_else(|| AppError::not_found("HLS session not found"))?
            .dir_path
            .clone()
    };

    let segment_path = segment_dir.join(segment);
    let segment_wait_started = Instant::now();
    let segment_ready = wait_for_generated_segment(&segment_path, HLS_SEGMENT_WAIT_TIMEOUT).await?;
    let segment_wait_ms = segment_wait_started.elapsed().as_millis() as u64;
    if !segment_ready {
        tracing::warn!(
            track_db_id = track_db_id.0,
            %session_id,
            segment,
            segment_wait_ms,
            "segment request for HLS did not resolve to a generated segment"
        );
        return Err(AppError::not_found("HLS segment not found"));
    }

    if segment_wait_ms > HLS_FILE_POLL_INTERVAL.as_millis() as u64 {
        tracing::debug!(
            track_db_id = track_db_id.0,
            %session_id,
            segment,
            segment_wait_ms,
            "segment for HLS required wait before serving"
        );
    }

    file_response(
        &segment_path,
        hls_media_content_type(&segment_path),
        &headers,
    )
    .await
}

fn hls_playlist_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Create HLS playlist")
        .description(
            "Generates an HLS event playlist for a track and returns M3U8 with segment URLs under `/api/stream/by-db-id/{track_db_id}/hls/{session_id}/...`. Segment URLs include short-lived signed query tokens for client compatibility when auth headers are not forwarded. The optional `codec` query parameter supports `aac` (default), `alac`, and `flac`.",
        )
}

fn hls_segment_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get HLS segment")
        .description(
            "Serves an HLS segment generated from `/api/stream/{track_id}/hls.m3u8`. Segment URLs use the internal numeric `track_db_id` emitted in the playlist. Accepts either the session owner via bearer auth or a valid short-lived signed URL token.",
        )
}

pub(crate) fn hls_routes() -> ApiRouter {
    ApiRouter::new()
        .api_route(
            "/{track_id}/hls.m3u8",
            get_with(get_hls_playlist, hls_playlist_docs),
        )
        .api_route(
            "/by-db-id/{track_db_id}/hls/{session_id}/{segment}",
            get_with(get_hls_segment, hls_segment_docs),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::hls::{
        signing::hls_sign_segment_token,
        state::{
            HlsSession,
            hls_job_key,
            test_helpers::*,
        },
    };
    use agdb::DbId;
    use axum::{
        body::to_bytes,
        extract::{
            Path,
            Query,
        },
        http::{
            HeaderMap,
            StatusCode,
        },
    };
    use lyra_ffmpeg::AudioCodec;
    use std::time::{
        Duration,
        Instant,
        SystemTime,
        UNIX_EPOCH,
    };

    #[test]
    fn sanitize_segment_accepts_safe_names() {
        assert!(matches!(
            sanitize_segment_name("segment-00001.ts"),
            Ok("segment-00001.ts")
        ));
    }

    #[test]
    fn sanitize_segment_rejects_path_traversal() {
        assert!(sanitize_segment_name("../segment.ts").is_err());
        assert!(sanitize_segment_name("foo/bar.ts").is_err());
    }

    #[tokio::test]
    async fn wait_for_generated_playlist_returns_before_timeout_when_created() {
        let test_dir = unique_test_dir("lyra-hls-playlist-wait-test");
        tokio::fs::create_dir_all(&test_dir)
            .await
            .expect("test dir created");
        let playlist_path = test_dir.join("index.m3u8");
        let writer_path = playlist_path.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(120)).await;
            tokio::fs::write(writer_path, "#EXTM3U\n#EXTINF:6.0,\nsegment-00001.ts\n")
                .await
                .expect("playlist written");
        });

        let playlist = wait_for_generated_playlist(&playlist_path, Duration::from_secs(2))
            .await
            .expect("playlist wait succeeded");
        assert!(playlist.is_some(), "playlist should become available");

        let _ = tokio::fs::remove_dir_all(&test_dir).await;
    }

    #[tokio::test]
    async fn wait_for_generated_segment_succeeds_when_segment_appears() {
        let test_dir = unique_test_dir("lyra-hls-segment-wait-test");
        tokio::fs::create_dir_all(&test_dir)
            .await
            .expect("test dir created");
        let segment_path = test_dir.join("segment-00001.ts");
        let writer_path = segment_path.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(120)).await;
            tokio::fs::write(writer_path, b"segment-data")
                .await
                .expect("segment written");
        });

        let ready = wait_for_generated_segment(&segment_path, Duration::from_secs(2))
            .await
            .expect("segment wait succeeded");
        assert!(ready, "segment should become available");

        let _ = tokio::fs::remove_dir_all(&test_dir).await;
    }

    #[tokio::test]
    async fn get_hls_segment_allows_valid_signed_query_without_auth_header() {
        let _guard = HLS_TEST_MUTEX.lock().await;
        reset_hls_state_for_test().await;

        let track_db_id = DbId(812);
        let session_id = "signed-session".to_string();
        let segment_name = "segment-00001.ts";
        let test_dir = unique_test_dir("lyra-hls-signed-segment-test");
        tokio::fs::create_dir_all(&test_dir)
            .await
            .expect("test dir created");
        tokio::fs::write(test_dir.join(segment_name), b"signed-bytes")
            .await
            .expect("segment created");

        let profile = HlsCodecProfile::from_requested(Some(AudioCodec::Aac)).expect("aac profile");
        let job_key = hls_job_key(track_db_id, DbId(9991), None, None, profile);
        let mut job = build_test_job(test_dir.clone(), test_dir.join("index.m3u8"));
        job.session_ids.insert(session_id.clone());
        {
            let mut jobs = HLS_JOBS.write().await;
            jobs.insert(job_key.clone(), job);
        }
        {
            let mut sessions = HLS_SESSIONS.write().await;
            sessions.insert(
                session_id.clone(),
                HlsSession {
                    user_db_id: DbId(99),
                    track_db_id,
                    job_key,
                    last_access: Instant::now(),
                },
            );
        }

        let exp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("valid timestamp")
            .as_secs()
            + 30;
        let sig = hls_sign_segment_token(track_db_id, &session_id, exp);
        let response = match get_hls_segment(
            HeaderMap::new(),
            Path((track_db_id.0, session_id.clone(), segment_name.to_string())),
            Query(HlsSegmentQuery {
                exp: Some(exp),
                sig: Some(sig),
            }),
        )
        .await
        {
            Ok(response) => response,
            Err(_) => panic!("signed segment request should succeed"),
        };

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("segment body read");
        assert_eq!(&body[..], b"signed-bytes");

        reset_hls_state_for_test().await;
    }
}
