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
            HlsOutputConfig,
            hls_media_content_type,
        },
        state::{
            HLS_JOBS,
            HLS_SESSIONS,
            HlsJobKey,
            attach_session_to_job,
            generate_hls_session_id,
            get_or_create_hls_job,
            hls_registry_counts,
        },
    },
};
use agdb::DbId;

use super::{
    apply_request_start_offset,
    apply_transcode_policy,
    file_response,
    source_range_duration_ms,
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
    #[schemars(
        description = "Scoped media token returned by `POST /api/tracks/{id}/playback-url`."
    )]
    media_token: Option<String>,
    #[schemars(
        description = "Optional ordered HLS audio codec preferences (for example: copy,aac or aac,flac)."
    )]
    codec: Option<String>,
    #[schemars(
        description = "Target bitrate cap in bits per second. Applied for lossy outputs when below the source bitrate; ignored for lossless codecs or when above source."
    )]
    bitrate_bps: Option<u32>,
    #[schemars(description = "Target sample rate in Hz. Triggers transcoding when supplied.")]
    sample_rate_hz: Option<u32>,
    #[schemars(description = "Target channel count. Triggers transcoding when supplied.")]
    channels: Option<u32>,
    #[schemars(
        description = "Prefer VBR for lossy HLS transcodes when the selected encoder supports it."
    )]
    prefer_vbr: Option<bool>,
    #[schemars(description = "Per-request playback start offset in milliseconds.")]
    start_offset_ms: Option<u64>,
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

fn build_hls_segment_uri(session_id: &str, segment_name: &str) -> String {
    format!("/api/stream/hls/{session_id}/{segment_name}")
}

fn bitrate_kbps_from_bps(bitrate_bps: u32) -> u32 {
    bitrate_bps.saturating_add(999) / 1000
}

// Bare native HLS requests still default to AAC transcoding. Stream copy is only
// considered when the request explicitly asks for copy or already targets the
// source codec, and only if the remaining output limits do not require audio
// changes.
fn hls_copy_is_eligible_for_request(
    source: &super::ValidatedTrackSource,
    copy_requested: bool,
    requested_codec: Option<lyra_ffmpeg::AudioCodec>,
    bitrate_bps: Option<u32>,
    sample_rate_hz: Option<u32>,
    channels: Option<u32>,
) -> bool {
    let Some(source_codec) = source.source_codec else {
        return false;
    };

    if !matches!(
        source_codec,
        lyra_ffmpeg::AudioCodec::Aac
            | lyra_ffmpeg::AudioCodec::Alac
            | lyra_ffmpeg::AudioCodec::Flac
    ) {
        return false;
    }

    if !copy_requested && requested_codec != Some(source_codec) {
        return false;
    }

    if let Some(requested_codec) = requested_codec
        && requested_codec != source_codec
    {
        return false;
    }

    if let Some(requested_bitrate_bps) = bitrate_bps {
        let Some(source_bitrate_bps) = source.source_bitrate_bps else {
            return false;
        };
        if requested_bitrate_bps < source_bitrate_bps {
            return false;
        }
    }

    if let Some(requested_sample_rate_hz) = sample_rate_hz {
        let Some(source_sample_rate_hz) = source.source_sample_rate_hz else {
            return false;
        };
        if requested_sample_rate_hz < source_sample_rate_hz {
            return false;
        }
    }

    if let Some(requested_channels) = channels {
        let Some(source_channels) = source.source_channels else {
            return false;
        };
        if requested_channels < source_channels {
            return false;
        }
    }

    true
}

fn resolve_hls_audio_bitrate_kbps(
    profile: HlsCodecProfile,
    bitrate_bps: Option<u32>,
) -> Option<u32> {
    if profile.is_copy {
        return None;
    }

    matches!(profile.codec, lyra_ffmpeg::AudioCodec::Aac).then(|| {
        bitrate_bps
            .map(bitrate_kbps_from_bps)
            .unwrap_or(crate::services::hls::codec::HLS_AUDIO_BITRATE_KBPS)
    })
}

fn build_hls_media_playlist(
    session_id: &str,
    duration_ms: u64,
    profile: HlsCodecProfile,
) -> String {
    let segment_ms = u64::from(HLS_SEGMENT_TIME_SECONDS) * 1000;
    let segment_count = hls_segment_count(duration_ms);
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
        let init_uri = build_hls_segment_uri(session_id, init_filename);
        let _ = writeln!(playlist, "#EXT-X-MAP:URI=\"{init_uri}\"");
    }

    for segment_index in 0..segment_count {
        let segment_start_ms = segment_index * segment_ms;
        let segment_duration_ms = duration_ms.saturating_sub(segment_start_ms).min(segment_ms);
        let segment_name = format!("segment-{segment_index:05}.{}", profile.segment_extension);
        let segment_uri = build_hls_segment_uri(session_id, &segment_name);
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
    super::require_hls_playlist_access(&headers, query.media_token.as_deref(), track_db_id).await?;
    serve_hls_playlist_for_track(
        track_db_id,
        query.codec,
        query.bitrate_bps,
        query.sample_rate_hz,
        query.channels,
        query.prefer_vbr,
        query.start_offset_ms,
    )
    .await
}

pub(crate) async fn serve_hls_playlist_for_track(
    track_db_id: DbId,
    codec: Option<String>,
    bitrate_bps: Option<u32>,
    sample_rate_hz: Option<u32>,
    channels: Option<u32>,
    prefer_vbr: Option<bool>,
    start_offset_ms: Option<u64>,
) -> Result<Response<Body>, AppError> {
    let request_started = Instant::now();
    let source = apply_request_start_offset(
        validate_and_get_track_source(track_db_id).await?,
        start_offset_ms,
    )?;
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
    let copy_requested = validated
        .preferred_codecs
        .iter()
        .any(|codec| matches!(codec, lyra_ffmpeg::AudioCodec::Copy));
    let requested_hls_codec = validated
        .preferred_codecs
        .iter()
        .copied()
        .find(|codec| !matches!(codec, lyra_ffmpeg::AudioCodec::Copy));
    let profile = if hls_copy_is_eligible_for_request(
        &source,
        copy_requested,
        requested_hls_codec,
        bitrate_bps,
        sample_rate_hz,
        channels,
    ) {
        HlsCodecProfile::for_copy_source(
            source
                .source_codec
                .expect("copy eligibility requires an inferred source codec"),
        )?
    } else {
        HlsCodecProfile::from_requested_codecs(&validated.preferred_codecs)?
    };
    let policy = apply_transcode_policy(
        bitrate_bps,
        sample_rate_hz,
        channels,
        prefer_vbr,
        profile.codec,
        source.source_bitrate_bps,
    )?;
    let audio_bitrate_kbps = resolve_hls_audio_bitrate_kbps(profile, policy.bitrate_bps);
    let output_sample_rate_hz = if profile.is_copy {
        None
    } else {
        policy.sample_rate_hz
    };
    let output_channels = if profile.is_copy {
        None
    } else {
        policy.channels
    };
    let output_prefer_vbr = if profile.is_copy {
        false
    } else {
        policy.prefer_vbr
    };

    let session_id = generate_hls_session_id();
    let output = HlsOutputConfig::new(
        profile,
        audio_bitrate_kbps,
        output_sample_rate_hz,
        output_channels,
        output_prefer_vbr,
    );
    let job_key = HlsJobKey::new(
        source.track_public_id.clone(),
        source.source_public_id.clone(),
        source.start_ms,
        source.end_ms,
        output,
    );

    let reused_job = get_or_create_hls_job(&job_key, &source.input_path).await?;
    let playlist_segment_count = hls_segment_count(duration_ms);
    attach_session_to_job(&session_id, playlist_segment_count, job_key).await?;
    let playlist = build_hls_media_playlist(&session_id, duration_ms, profile);

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
        codec = ?profile.codec,
        copy = profile.is_copy,
        audio_bitrate_kbps,
        sample_rate_hz = output_sample_rate_hz,
        channels = output_channels,
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
    Path((session_id, segment)): Path<(String, String)>,
) -> Result<Response<Body>, AppError> {
    let segment = sanitize_segment_name(&segment)?;
    let (job_key, playlist_segment_count) = {
        let mut sessions = HLS_SESSIONS.write().await;
        let session = sessions
            .get_mut(&session_id)
            .ok_or_else(|| AppError::not_found("HLS session not found"))?;

        session.last_access = Instant::now();
        (session.job_key.clone(), session.playlist_segment_count)
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
    let segment_ready =
        wait_for_generated_segment(&segment_path, hls_segment_wait_timeout(segment)).await?;
    let segment_wait_ms = segment_wait_started.elapsed().as_millis() as u64;
    if !segment_ready {
        let requested_segment_index = parse_hls_segment_index(segment);
        let final_advertised_segment_missing = requested_segment_index
            .map(|segment_index| segment_index + 1 == playlist_segment_count)
            .unwrap_or(false);

        tracing::warn!(
            track_public_id = %job_key.track_public_id(),
            %session_id,
            segment,
            segment_wait_ms,
            playlist_segment_count,
            requested_segment_index,
            "segment request for HLS did not resolve to a generated segment"
        );
        if final_advertised_segment_missing {
            tracing::warn!(
                track_public_id = %job_key.track_public_id(),
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
            track_public_id = %job_key.track_public_id(),
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
            "Generates an HLS VOD media playlist for a finite track and returns M3U8 with segment URLs under `/api/stream/hls/{session_id}/...`. Requires either bearer authentication with access to the track or a scoped `media_token` from `POST /api/tracks/{track_id}/playback-url`. The optional `codec` query parameter supports `aac` (default), `alac`, `flac`, and `copy` when the source audio is already HLS-compatible and the request does not require audio changes.",
        )
}

fn hls_segment_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get HLS segment").description(
        "Serves a public HLS segment generated from `/api/stream/{track_id}/hls.m3u8`.",
    )
}

pub(crate) fn hls_routes() -> ApiRouter {
    ApiRouter::new()
        .api_route(
            "/{track_id}/hls.m3u8",
            get_with(get_hls_playlist, hls_playlist_docs),
        )
        .api_route(
            "/hls/{session_id}/{segment}",
            get_with(get_hls_segment, hls_segment_docs),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::hls::state::{
        HlsJobKey,
        HlsSession,
        test_helpers::*,
    };
    use axum::{
        body::to_bytes,
        extract::Path,
        http::{
            HeaderMap,
            StatusCode,
        },
    };
    use lyra_ffmpeg::AudioCodec;
    use std::time::{
        Duration,
        Instant,
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
    fn build_hls_media_playlist_uses_vod_markers_and_public_segment_urls() {
        let profile = HlsCodecProfile::from_requested(Some(AudioCodec::Aac)).expect("aac profile");
        let playlist = build_hls_media_playlist("sess", 21_452, profile);

        assert!(playlist.contains("#EXT-X-VERSION:6"));
        assert!(playlist.contains("#EXT-X-PLAYLIST-TYPE:VOD"));
        assert!(playlist.contains("#EXT-X-INDEPENDENT-SEGMENTS"));
        assert!(playlist.contains("#EXT-X-ENDLIST"));
        assert!(playlist.contains("#EXTINF:3.452000,"));
        assert!(playlist.contains("/api/stream/hls/sess/segment-00000.ts"));
        assert!(!playlist.contains("?exp="));
    }

    #[test]
    fn build_hls_media_playlist_uses_init_map_for_fmp4_profiles() {
        let profile =
            HlsCodecProfile::from_requested(Some(AudioCodec::Alac)).expect("alac profile");
        let playlist = build_hls_media_playlist("sess", 21_452, profile);

        assert!(playlist.contains("#EXT-X-VERSION:7"));
        assert!(playlist.contains("#EXT-X-MAP:URI=\"/api/stream/hls/sess/init.mp4\""));
        assert!(playlist.contains("/api/stream/hls/sess/segment-00000.m4s"));
        assert!(!playlist.contains("?exp="));
    }

    #[tokio::test]
    async fn get_hls_segment_allows_public_session_without_auth_header() {
        let _guard = HLS_TEST_MUTEX.lock().await;
        reset_hls_state_for_test().await;

        let track_public_id = "track-pub-812".to_string();
        let session_id = "public-session".to_string();
        let segment_name = "segment-00001.ts";
        let test_dir = unique_test_dir("lyra-hls-public-segment-test");
        tokio::fs::create_dir_all(&test_dir)
            .await
            .expect("test dir created");
        tokio::fs::write(test_dir.join(segment_name), b"public-bytes")
            .await
            .expect("segment created");

        let profile = HlsCodecProfile::from_requested(Some(AudioCodec::Aac)).expect("aac profile");
        let job_key = HlsJobKey::new(
            track_public_id.clone(),
            "source-pub-9991".to_string(),
            None,
            None,
            HlsOutputConfig::new(
                profile,
                Some(crate::services::hls::codec::HLS_AUDIO_BITRATE_KBPS),
                None,
                None,
                false,
            ),
        );
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
                    playlist_segment_count: 1,
                    job_key,
                    last_access: Instant::now(),
                },
            );
        }

        let response = match get_hls_segment(
            HeaderMap::new(),
            Path((session_id.clone(), segment_name.to_string())),
        )
        .await
        {
            Ok(response) => response,
            Err(_) => panic!("public segment request should succeed"),
        };

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("segment body read");
        assert_eq!(&body[..], b"public-bytes");

        reset_hls_state_for_test().await;
    }
}
