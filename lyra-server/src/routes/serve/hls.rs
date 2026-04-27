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
    fmt::Write as _,
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
            HLS_SEGMENT_TIME_SECONDS,
            HlsCodecProfile,
            hls_media_content_type,
        },
        signing::{
            HlsSegmentQuery,
            hls_signed_segment_query,
            validate_signed_segment_query,
        },
        state::{
            HLS_JOBS,
            HLS_SESSIONS,
            attach_session_to_job,
            authorize_hls_segment_session,
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

const HLS_SEGMENT_WAIT_TIMEOUT: Duration = Duration::from_secs(2);
const HLS_INITIAL_SEGMENT_WAIT_TIMEOUT: Duration = Duration::from_secs(10);
const HLS_FILE_POLL_INTERVAL: Duration = Duration::from_millis(50);
const HLS_PLAYLIST_VERSION_MPEGTS: u32 = 6;
const HLS_PLAYLIST_VERSION_FMP4: u32 = 7;
const HLS_DURATION_MISMATCH_WARN_THRESHOLD_MS: u64 = 10;

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

fn source_range_duration_ms(start_ms: Option<u64>, end_ms: Option<u64>) -> Option<u64> {
    match (start_ms, end_ms) {
        (Some(start_ms), Some(end_ms)) if end_ms > start_ms => Some(end_ms - start_ms),
        _ => None,
    }
}

fn resolve_hls_playlist_duration_ms(
    track_duration_ms: Option<u64>,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
) -> Option<u64> {
    if let Some(duration_ms) = source_range_duration_ms(start_ms, end_ms) {
        return Some(duration_ms);
    }

    track_duration_ms.filter(|duration_ms| *duration_ms > 0)
}

fn hls_playlist_version(profile: HlsCodecProfile) -> u32 {
    if profile.init_filename.is_some() {
        HLS_PLAYLIST_VERSION_FMP4
    } else {
        HLS_PLAYLIST_VERSION_MPEGTS
    }
}

fn hls_target_duration_seconds(duration_ms: u64) -> u64 {
    let max_segment_ms = duration_ms.min(u64::from(HLS_SEGMENT_TIME_SECONDS) * 1000);
    max_segment_ms.max(1).div_ceil(1000)
}

fn hls_segment_count(duration_ms: u64) -> u64 {
    duration_ms.div_ceil(u64::from(HLS_SEGMENT_TIME_SECONDS) * 1000)
}

fn parse_hls_segment_index(segment: &str) -> Option<u64> {
    let stem = segment.strip_prefix("segment-")?;
    let digits = stem.split_once('.')?.0;
    digits.parse().ok()
}

fn is_initial_hls_segment(segment: &str) -> bool {
    segment == "init.mp4" || matches!(parse_hls_segment_index(segment), Some(0))
}

fn hls_segment_wait_timeout(segment: &str) -> Duration {
    if is_initial_hls_segment(segment) {
        HLS_INITIAL_SEGMENT_WAIT_TIMEOUT
    } else {
        HLS_SEGMENT_WAIT_TIMEOUT
    }
}

fn build_hls_segment_uri(
    track_db_id: DbId,
    session_id: &str,
    segment_name: &str,
    signed_query: &str,
) -> String {
    format!(
        "/api/stream/by-db-id/{}/hls/{session_id}/{segment_name}{signed_query}",
        track_db_id.0
    )
}

fn build_hls_media_playlist(
    track_db_id: DbId,
    session_id: &str,
    duration_ms: u64,
    profile: HlsCodecProfile,
) -> String {
    let segment_ms = u64::from(HLS_SEGMENT_TIME_SECONDS) * 1000;
    let segment_count = hls_segment_count(duration_ms);
    let signed_query = hls_signed_segment_query(track_db_id, session_id);
    let mut playlist = String::with_capacity(256 + (segment_count as usize * 128));

    let _ = writeln!(playlist, "#EXTM3U");
    let _ = writeln!(playlist, "#EXT-X-VERSION:{}", hls_playlist_version(profile));
    let _ = writeln!(
        playlist,
        "#EXT-X-TARGETDURATION:{}",
        hls_target_duration_seconds(duration_ms)
    );
    let _ = writeln!(playlist, "#EXT-X-MEDIA-SEQUENCE:0");
    let _ = writeln!(playlist, "#EXT-X-PLAYLIST-TYPE:VOD");
    let _ = writeln!(playlist, "#EXT-X-INDEPENDENT-SEGMENTS");

    if let Some(init_filename) = profile.init_filename {
        let init_uri = build_hls_segment_uri(track_db_id, session_id, init_filename, &signed_query);
        let _ = writeln!(playlist, "#EXT-X-MAP:URI=\"{init_uri}\"");
    }

    for segment_index in 0..segment_count {
        let segment_start_ms = segment_index * segment_ms;
        let segment_duration_ms = duration_ms.saturating_sub(segment_start_ms).min(segment_ms);
        let segment_name = format!("segment-{segment_index:05}.{}", profile.segment_extension);
        let segment_uri =
            build_hls_segment_uri(track_db_id, session_id, &segment_name, &signed_query);
        let _ = writeln!(
            playlist,
            "#EXTINF:{:.6},",
            segment_duration_ms as f64 / 1000.0
        );
        let _ = writeln!(playlist, "{segment_uri}");
    }

    let _ = writeln!(playlist, "#EXT-X-ENDLIST");

    playlist
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
    if let (Some(track_duration_ms), Some(range_duration_ms)) = (
        source.duration_ms.filter(|duration_ms| *duration_ms > 0),
        source_range_duration_ms(source.start_ms, source.end_ms),
    ) && track_duration_ms.abs_diff(range_duration_ms) >= HLS_DURATION_MISMATCH_WARN_THRESHOLD_MS
    {
        tracing::warn!(
            track_db_id = track_db_id.0,
            source_db_id = source.source_id.0,
            track_duration_ms,
            range_duration_ms,
            "HLS playlist duration differs from source range; using source range duration"
        );
    }
    let duration_ms =
        resolve_hls_playlist_duration_ms(source.duration_ms, source.start_ms, source.end_ms)
            .ok_or_else(|| {
                AppError::service_unavailable("HLS requires a known positive track duration")
            })?;
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
    let playlist_segment_count = hls_segment_count(duration_ms);
    attach_session_to_job(
        &session_id,
        principal.user_db_id,
        track_db_id,
        playlist_segment_count,
        job_key,
    )
    .await?;
    let playlist = build_hls_media_playlist(track_db_id, &session_id, duration_ms, profile);

    let response = Response::builder()
        .header(header::CONTENT_TYPE, "application/x-mpegurl")
        .header(header::CACHE_CONTROL, "no-cache, no-store, must-revalidate")
        .header(header::PRAGMA, "no-cache")
        .header(header::EXPIRES, "0")
        .body(Body::from(playlist))?;

    let startup_latency_ms = request_started.elapsed().as_millis() as u64;
    let (active_jobs, active_sessions) = hls_registry_counts().await;
    tracing::info!(
        track_db_id = track_db_id.0,
        source_db_id = source.source_id.0,
        %session_id,
        codec = profile.ffmpeg_codec_str,
        duration_ms,
        playlist_segment_count,
        startup_latency_ms,
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
        (session.job_key.clone(), session.playlist_segment_count)
    };
    let (job_key, playlist_segment_count) = job_key;

    let segment_dir = {
        let jobs = HLS_JOBS.read().await;
        jobs.get(&job_key)
            .ok_or_else(|| AppError::not_found("HLS session not found"))?
            .dir_path
            .clone()
    };

    let segment_path = segment_dir.join(segment);
    let segment_wait_started = Instant::now();
    let segment_ready =
        wait_for_generated_segment(&segment_path, hls_segment_wait_timeout(segment)).await?;
    let segment_wait_ms = segment_wait_started.elapsed().as_millis() as u64;
    if !segment_ready {
        let requested_segment_index = parse_hls_segment_index(segment);
        let final_advertised_segment_missing = requested_segment_index
            .map(|segment_index| segment_index + 1 == playlist_segment_count)
            .unwrap_or(false);

        tracing::warn!(
            track_db_id = track_db_id.0,
            %session_id,
            segment,
            segment_wait_ms,
            playlist_segment_count,
            requested_segment_index,
            "segment request for HLS did not resolve to a generated segment"
        );
        if final_advertised_segment_missing {
            tracing::warn!(
                track_db_id = track_db_id.0,
                %session_id,
                segment,
                playlist_segment_count,
                "final advertised HLS segment was not generated; possible playlist/segment drift"
            );
        }
        if is_initial_hls_segment(segment) {
            return Err(AppError::service_unavailable(
                "HLS segment is still being generated",
            ));
        }
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
            "Generates an HLS VOD media playlist for a finite track and returns M3U8 with segment URLs under `/api/stream/by-db-id/{track_db_id}/hls/{session_id}/...`. Segment URLs include short-lived signed query tokens for client compatibility when auth headers are not forwarded. The optional `codec` query parameter supports `aac` (default), `alac`, and `flac`.",
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

    #[test]
    fn resolve_hls_playlist_duration_prefers_source_range_over_track_duration() {
        assert_eq!(
            resolve_hls_playlist_duration_ms(Some(21_452), Some(1_000), Some(99_000)),
            Some(98_000)
        );
    }

    #[test]
    fn resolve_hls_playlist_duration_falls_back_to_source_range() {
        assert_eq!(
            resolve_hls_playlist_duration_ms(None, Some(12_000), Some(18_250)),
            Some(6_250)
        );
        assert_eq!(
            resolve_hls_playlist_duration_ms(None, Some(12_000), None),
            None
        );
    }

    #[test]
    fn build_hls_media_playlist_uses_vod_markers_and_signed_segment_urls() {
        let profile = HlsCodecProfile::from_requested(Some(AudioCodec::Aac)).expect("aac profile");
        let playlist = build_hls_media_playlist(DbId(99), "sess", 21_452, profile);

        assert!(playlist.contains("#EXT-X-VERSION:6"));
        assert!(playlist.contains("#EXT-X-PLAYLIST-TYPE:VOD"));
        assert!(playlist.contains("#EXT-X-INDEPENDENT-SEGMENTS"));
        assert!(playlist.contains("#EXT-X-ENDLIST"));
        assert!(playlist.contains("#EXTINF:3.452000,"));
        assert!(playlist.contains("/api/stream/by-db-id/99/hls/sess/segment-00000.ts?exp="));
    }

    #[test]
    fn build_hls_media_playlist_uses_init_map_for_fmp4_profiles() {
        let profile =
            HlsCodecProfile::from_requested(Some(AudioCodec::Alac)).expect("alac profile");
        let playlist = build_hls_media_playlist(DbId(99), "sess", 21_452, profile);

        assert!(playlist.contains("#EXT-X-VERSION:7"));
        assert!(
            playlist.contains("#EXT-X-MAP:URI=\"/api/stream/by-db-id/99/hls/sess/init.mp4?exp=")
        );
        assert!(playlist.contains("/api/stream/by-db-id/99/hls/sess/segment-00000.m4s?exp="));
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
                    playlist_segment_count: 1,
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
