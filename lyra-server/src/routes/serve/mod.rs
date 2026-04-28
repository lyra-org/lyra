// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod download;
mod hls;
mod ranged_file;
mod stream;

pub use download::download_routes;
pub(crate) use download::download_track_response;
pub(crate) use hls::serve_hls_playlist_for_track;
pub(crate) use ranged_file::build_ranged_file_body;
pub(crate) use stream::stream_track_response;

use aide::axum::ApiRouter;

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
    AudioVbrMode,
    Output,
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
            require_download,
        },
        playback_sources as playback_source_service,
    },
};

#[derive(Debug)]
pub struct ValidatedTrackSource {
    pub source_id: DbId,
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

    Ok(ValidatedTrackSource {
        source_id: source.source_id,
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

pub struct ValidatedRequest {
    pub format: Option<AudioFormat>,
    pub preferred_codecs: Vec<AudioCodec>,
}

fn parse_preferred_codecs(codec: Option<String>) -> Result<Vec<AudioCodec>, AppError> {
    let Some(codec) = codec else {
        return Ok(Vec::new());
    };

    let mut preferred_codecs = Vec::new();
    for raw_codec in codec.split(',') {
        let trimmed = raw_codec.trim();
        if trimmed.is_empty() {
            continue;
        }
        let parsed = AudioCodec::parse(trimmed).ok_or_else(|| {
            AppError::bad_request(format!(
                "Unsupported codec: {}. Supported codecs: {:?}",
                trimmed,
                lyra_ffmpeg::SUPPORTED_CODECS
            ))
        })?;
        preferred_codecs.push(parsed);
    }

    Ok(preferred_codecs)
}

fn codec_names(codecs: &[AudioCodec]) -> String {
    codecs
        .iter()
        .map(AudioCodec::as_str)
        .collect::<Vec<_>>()
        .join(", ")
}

fn incompatible_codecs_error(
    output_format: AudioFormat,
    preferred_codecs: &[AudioCodec],
) -> AppError {
    AppError::bad_request(format!(
        "Requested codecs [{}] are not compatible with format '{}'. Supported codecs: [{}]",
        codec_names(preferred_codecs),
        output_format.extension(),
        codec_names(output_format.compatible_codecs())
    ))
}

pub fn validate_request(
    format: Option<String>,
    codec: Option<String>,
) -> Result<ValidatedRequest, AppError> {
    let format = match format {
        Some(fmt) => {
            let parsed = AudioFormat::parse(&fmt).ok_or_else(|| {
                AppError::bad_request(format!(
                    "Unsupported format: {}. Supported formats: {:?}",
                    fmt,
                    lyra_ffmpeg::SUPPORTED_FORMATS
                ))
            })?;
            Some(parsed)
        }
        None => None,
    };
    let preferred_codecs = parse_preferred_codecs(codec)?;
    Ok(ValidatedRequest {
        format,
        preferred_codecs,
    })
}

pub fn resolve_output_format(
    requested_format: Option<AudioFormat>,
    preferred_codecs: &[AudioCodec],
    entry_format: Option<AudioFormat>,
    entry_path: &FsPath,
    allow_copy: bool,
) -> Result<AudioFormat, AppError> {
    if let Some(fmt) = requested_format {
        return Ok(fmt);
    }

    if allow_copy
        && matches!(preferred_codecs.first(), Some(AudioCodec::Copy))
        && let Some(entry_format) = entry_format
    {
        return Ok(entry_format);
    }

    for codec in preferred_codecs {
        if matches!(codec, AudioCodec::Copy) {
            continue;
        }
        if let Some(fmt) = codec.preferred_format() {
            return Ok(fmt);
        }
    }

    entry_format.ok_or_else(|| {
        AppError::bad_request(format!(
            "Track source has unsupported format: {}",
            entry_path.to_string_lossy()
        ))
    })
}

pub fn resolve_codec(
    preferred_codecs: &[AudioCodec],
    output_format: AudioFormat,
    entry_format: Option<AudioFormat>,
    allow_copy: bool,
) -> Result<AudioCodec, AppError> {
    if !preferred_codecs.is_empty() {
        for codec in preferred_codecs {
            if matches!(codec, AudioCodec::Copy) {
                if allow_copy && Some(output_format) == entry_format {
                    return Ok(AudioCodec::Copy);
                }
                continue;
            }
            if output_format.supports_codec(*codec) {
                return Ok(*codec);
            }
        }

        return Err(incompatible_codecs_error(output_format, preferred_codecs));
    }

    if allow_copy && Some(output_format) == entry_format {
        return Ok(AudioCodec::Copy);
    }

    Ok(output_format.default_codec())
}

#[derive(Debug, Clone)]
pub struct TranscodePolicy {
    pub bitrate_bps: Option<u32>,
    pub sample_rate_hz: Option<u32>,
    pub channels: Option<u32>,
    pub prefer_vbr: bool,
}

pub fn apply_transcode_policy(
    requested_bitrate_bps: Option<u32>,
    requested_sample_rate_hz: Option<u32>,
    requested_channels: Option<u32>,
    prefer_vbr: Option<bool>,
    output_codec: AudioCodec,
    source_bitrate_bps: Option<u32>,
) -> Result<TranscodePolicy, AppError> {
    let bitrate_bps = match requested_bitrate_bps {
        None => None,
        Some(0) => {
            return Err(AppError::bad_request(
                "bitrate_bps must be greater than zero",
            ));
        }
        Some(bps) => {
            if output_codec.is_lossless() {
                tracing::info!(
                    target: "transcode_policy",
                    codec = ?output_codec,
                    requested_bps = bps,
                    "bitrate cap ignored for lossless codec"
                );
                None
            } else if let Some(source) = source_bitrate_bps
                && bps > source
            {
                // source bitrate is the average for VBR sources; peaks may exceed it and are not preserved here.
                tracing::info!(
                    target: "transcode_policy",
                    codec = ?output_codec,
                    requested_bps = bps,
                    source_bps = source,
                    "cap above source bitrate; dropping cap so quality is not inflated"
                );
                None
            } else if let Some(min) = output_codec.min_bitrate_bps()
                && bps < min
            {
                tracing::info!(
                    target: "transcode_policy",
                    codec = ?output_codec,
                    requested_bps = bps,
                    clamped_bps = min,
                    "bitrate below codec minimum; clamping"
                );
                Some(min)
            } else {
                Some(bps)
            }
        }
    };

    let sample_rate_hz = match requested_sample_rate_hz {
        None => None,
        Some(0) => {
            return Err(AppError::bad_request(
                "sample_rate_hz must be greater than zero",
            ));
        }
        Some(hz) => match output_codec.native_sample_rate_hz() {
            Some(native) if hz != native => {
                tracing::info!(
                    target: "transcode_policy",
                    codec = ?output_codec,
                    requested_hz = hz,
                    delivered_hz = native,
                    "codec substitutes sample rate; delivering native rate"
                );
                Some(native)
            }
            _ => Some(hz),
        },
    };

    let channels = match requested_channels {
        None => None,
        Some(0) => {
            return Err(AppError::bad_request("channels must be greater than zero"));
        }
        Some(ch) => Some(ch),
    };

    Ok(TranscodePolicy {
        bitrate_bps,
        sample_rate_hz,
        channels,
        prefer_vbr: prefer_vbr.unwrap_or(false),
    })
}

fn apply_lossy_rate_control(
    output: Output,
    codec: AudioCodec,
    bitrate_bps: Option<u32>,
    channels: Option<u32>,
    prefer_vbr: bool,
) -> Output {
    let bitrate_kbps = bitrate_bps
        .map(|bps| bps.saturating_add(999) / 1000)
        .filter(|kbps| *kbps > 0)
        .unwrap_or(192);

    if prefer_vbr
        && let Some(mode) = codec.vbr_mode(
            bitrate_bps.unwrap_or(bitrate_kbps.saturating_mul(1000)),
            channels.unwrap_or(2),
        )
    {
        return match mode {
            AudioVbrMode::Quality(quality) => output.set_audio_global_quality(quality),
            AudioVbrMode::Abr => output
                .set_audio_codec_opt("abr", "1")
                .set_audio_codec_opt("b", format!("{bitrate_kbps}k")),
        };
    }

    output.set_audio_codec_opt("b", format!("{bitrate_kbps}k"))
}

pub fn configure_output(
    output: Output,
    format: AudioFormat,
    codec: AudioCodec,
    policy: &TranscodePolicy,
) -> Output {
    let mut output = output.audio_format(format).codec(codec);
    if !matches!(codec, AudioCodec::Copy) && !codec.is_lossless() {
        output = apply_lossy_rate_control(
            output,
            codec,
            policy.bitrate_bps,
            policy.channels,
            policy.prefer_vbr,
        );
    }
    if let Some(hz) = policy.sample_rate_hz {
        output = output.set_audio_sample_rate(hz as i32);
    }
    if let Some(ch) = policy.channels {
        output = output.set_audio_channels(ch as i32);
    }
    output
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
        .header(header::CACHE_CONTROL, "no-cache, no-store, must-revalidate")
        .header(header::PRAGMA, "no-cache")
        .header(header::EXPIRES, "0");

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
    file_response_internal(path, content_type, headers, None).await
}

pub async fn temp_file_response(
    path: &FsPath,
    content_type: &str,
    headers: &HeaderMap,
) -> Result<Response<Body>, AppError> {
    file_response_internal(path, content_type, headers, Some(path.to_path_buf())).await
}

pub fn stream_routes() -> ApiRouter {
    stream::stream_routes().merge(hls::hls_routes())
}

#[cfg(test)]
mod tests {
    use super::{
        ValidatedTrackSource,
        apply_request_start_offset,
        configure_output,
        require_download_access,
        resolve_codec,
        resolve_output_format,
        validate_request,
    };
    use std::{
        path::Path,
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
        Output,
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
        services::auth::sessions,
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
            user_db_id
        };

        let session = sessions::create_session_for_user(user_db_id).await?;
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

    fn policy_passthrough(
        bitrate_bps: Option<u32>,
        sample_rate_hz: Option<u32>,
        channels: Option<u32>,
    ) -> super::TranscodePolicy {
        super::TranscodePolicy {
            bitrate_bps,
            sample_rate_hz,
            channels,
            prefer_vbr: false,
        }
    }

    #[test]
    fn configure_output_defaults_bitrate_to_192_kbps_when_unset() {
        let output = configure_output(
            Output::with_callback(|_| 0),
            AudioFormat::Mp3,
            AudioCodec::Mp3,
            &policy_passthrough(None, None, None),
        );
        assert_eq!(
            output.get_audio_codec_opts().get("b"),
            Some(&"192k".to_string()),
            "default bitrate should be 192 kbps when none is supplied"
        );
        assert_eq!(output.get_audio_sample_rate(), None);
        assert_eq!(output.get_audio_channels(), None);
    }

    #[test]
    fn configure_output_applies_supplied_bitrate_sample_rate_and_channels() {
        let output = configure_output(
            Output::with_callback(|_| 0),
            AudioFormat::Opus,
            AudioCodec::Opus,
            &policy_passthrough(Some(96_000), Some(48_000), Some(2)),
        );
        assert_eq!(
            output.get_audio_codec_opts().get("b"),
            Some(&"96k".to_string()),
            "96_000 bps should round-trip to 96 kbps"
        );
        assert_eq!(output.get_audio_sample_rate(), Some(48_000));
        assert_eq!(output.get_audio_channels(), Some(2));
    }

    #[test]
    fn configure_output_rounds_bitrate_upward_to_the_nearest_kbps() {
        let output = configure_output(
            Output::with_callback(|_| 0),
            AudioFormat::Mp3,
            AudioCodec::Mp3,
            &policy_passthrough(Some(127_500), None, None),
        );
        assert_eq!(
            output.get_audio_codec_opts().get("b"),
            Some(&"128k".to_string()),
            "127_500 bps should ceil to 128 kbps"
        );
    }

    #[test]
    fn configure_output_uses_vbr_when_preferred() {
        let output = configure_output(
            Output::with_callback(|_| 0),
            AudioFormat::Mp3,
            AudioCodec::Mp3,
            &super::TranscodePolicy {
                bitrate_bps: Some(192_000),
                sample_rate_hz: None,
                channels: Some(2),
                prefer_vbr: true,
            },
        );
        assert_eq!(output.get_audio_global_quality(), Some(2));
        assert!(output.get_audio_codec_opts().get("b").is_none());
    }

    #[test]
    fn policy_rejects_zero_bitrate() {
        let err = super::apply_transcode_policy(Some(0), None, None, None, AudioCodec::Mp3, None)
            .expect_err("bitrate_bps=0 must fail fast, not silently fall back to the default");
        assert_eq!(err.into_response().status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn policy_rejects_zero_sample_rate() {
        let err = super::apply_transcode_policy(None, Some(0), None, None, AudioCodec::Mp3, None)
            .expect_err("sample_rate_hz=0 must fail fast");
        assert_eq!(err.into_response().status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn policy_rejects_zero_channels() {
        let err = super::apply_transcode_policy(None, None, Some(0), None, AudioCodec::Mp3, None)
            .expect_err("channels=0 must fail fast");
        assert_eq!(err.into_response().status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn policy_clamps_bitrate_below_codec_minimum() {
        let policy =
            super::apply_transcode_policy(Some(1), None, None, None, AudioCodec::Mp3, None)
                .expect("below-minimum bitrate should clamp, not reject");
        assert_eq!(
            policy.bitrate_bps,
            Some(AudioCodec::Mp3.min_bitrate_bps().unwrap())
        );
    }

    #[test]
    fn policy_drops_bitrate_cap_for_lossless_codec() {
        let policy =
            super::apply_transcode_policy(Some(96_000), None, None, None, AudioCodec::Flac, None)
                .expect("lossless codec ignores bitrate cap");
        assert_eq!(
            policy.bitrate_bps, None,
            "flac output must drop the bitrate cap entirely rather than advertise a cap it cannot honor"
        );
    }

    #[test]
    fn policy_rewrites_opus_sample_rate_to_native_48000() {
        let policy =
            super::apply_transcode_policy(None, Some(44_100), None, None, AudioCodec::Opus, None)
                .expect("opus substitutes non-48kHz sample rates");
        assert_eq!(
            policy.sample_rate_hz,
            Some(48_000),
            "opus substitutes internally; advertise what we deliver"
        );
    }

    #[test]
    fn policy_passes_matching_opus_sample_rate_through() {
        let policy =
            super::apply_transcode_policy(None, Some(48_000), None, None, AudioCodec::Opus, None)
                .expect("48kHz request for opus should pass through");
        assert_eq!(policy.sample_rate_hz, Some(48_000));
    }

    #[test]
    fn policy_passes_bitrate_through_when_source_unknown() {
        let policy =
            super::apply_transcode_policy(Some(192_000), None, None, None, AudioCodec::Mp3, None)
                .expect("source_bitrate_bps=None must not block the cap");
        assert_eq!(
            policy.bitrate_bps,
            Some(192_000),
            "when source bitrate is unknown, the requested cap flows through untouched"
        );
    }

    #[test]
    fn policy_drops_bitrate_cap_above_source_bitrate() {
        let policy = super::apply_transcode_policy(
            Some(320_000),
            None,
            None,
            None,
            AudioCodec::Mp3,
            Some(128_000),
        )
        .expect("cap above source should not inflate quality");
        assert_eq!(
            policy.bitrate_bps, None,
            "a 320 kbps cap on a 128 kbps source should drop to no cap so we don't upsample quality"
        );
    }

    #[test]
    fn policy_retains_bitrate_cap_below_source_bitrate() {
        let policy = super::apply_transcode_policy(
            Some(96_000),
            None,
            None,
            None,
            AudioCodec::Mp3,
            Some(320_000),
        )
        .expect("legitimate cap below source should pass through");
        assert_eq!(policy.bitrate_bps, Some(96_000));
    }

    #[test]
    fn policy_preserves_untouched_knobs_for_lossy_passthrough_values() {
        let policy = super::apply_transcode_policy(
            Some(128_000),
            Some(44_100),
            Some(2),
            None,
            AudioCodec::Mp3,
            None,
        )
        .expect("in-range lossy values should pass through");
        assert_eq!(policy.bitrate_bps, Some(128_000));
        assert_eq!(policy.sample_rate_hz, Some(44_100));
        assert_eq!(policy.channels, Some(2));
    }

    #[test]
    fn validate_request_parses_ordered_codec_preferences() {
        let validated = validate_request(
            Some("webm".to_string()),
            Some("copy, opus,vorbis".to_string()),
        )
        .expect("ordered codec preferences should parse");
        assert_eq!(validated.format, Some(AudioFormat::Webm));
        assert_eq!(
            validated.preferred_codecs,
            vec![AudioCodec::Copy, AudioCodec::Opus, AudioCodec::Vorbis]
        );
    }

    #[test]
    fn resolve_output_format_uses_next_preference_when_copy_is_disallowed() {
        let entry_path = Path::new("track.flac");
        let preferred_codecs = vec![AudioCodec::Copy, AudioCodec::Opus];
        assert_eq!(
            resolve_output_format(
                None,
                &preferred_codecs,
                Some(AudioFormat::Flac),
                entry_path,
                true
            )
            .expect("copy-allowed output format"),
            AudioFormat::Flac
        );
        assert_eq!(
            resolve_output_format(
                None,
                &preferred_codecs,
                Some(AudioFormat::Flac),
                entry_path,
                false
            )
            .expect("copy-disallowed output format"),
            AudioFormat::Opus
        );
    }

    #[test]
    fn resolve_codec_matches_first_compatible_codec_for_requested_format() {
        let preferred_codecs = vec![AudioCodec::Opus, AudioCodec::Flac];
        let codec = resolve_codec(
            &preferred_codecs,
            AudioFormat::Ogg,
            Some(AudioFormat::Flac),
            true,
        )
        .expect("first compatible codec should be selected");
        assert_eq!(codec, AudioCodec::Opus);
    }

    #[test]
    fn resolve_codec_accepts_24_bit_pcm_for_wav() {
        let preferred_codecs = vec![AudioCodec::PcmS24Le];
        let codec = resolve_codec(
            &preferred_codecs,
            AudioFormat::Wav,
            Some(AudioFormat::Flac),
            true,
        )
        .expect("24-bit PCM should be valid for wav");
        assert_eq!(codec, AudioCodec::PcmS24Le);
    }

    #[test]
    fn resolve_codec_rejects_incompatible_requested_codec_list() {
        let err = resolve_codec(
            &[AudioCodec::Copy],
            AudioFormat::Mp3,
            Some(AudioFormat::Flac),
            false,
        )
        .expect_err("copy cannot satisfy an explicit mp3 transcode request");
        assert_eq!(err.into_response().status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn resolve_codec_prefers_copy_for_matching_mp3_source_before_transcoding() {
        let codec = resolve_codec(
            &[AudioCodec::Copy, AudioCodec::Mp3],
            AudioFormat::Mp3,
            Some(AudioFormat::Mp3),
            true,
        )
        .expect("mp3 source should preserve copy before mp3 transcode");
        assert_eq!(codec, AudioCodec::Copy);
    }

    #[test]
    fn resolve_codec_falls_back_to_mp3_transcode_when_source_is_not_mp3() {
        let codec = resolve_codec(
            &[AudioCodec::Copy, AudioCodec::Mp3],
            AudioFormat::Mp3,
            Some(AudioFormat::Flac),
            true,
        )
        .expect("non-mp3 source should fall back to mp3 transcode");
        assert_eq!(codec, AudioCodec::Mp3);
    }

    #[test]
    fn apply_request_start_offset_reduces_duration_for_full_track_sources() {
        let source = ValidatedTrackSource {
            source_id: agdb::DbId(2),
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

    async fn prepare_streamable_track(test_dir: &PathBuf) -> anyhow::Result<i64> {
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
                directory: test_dir.clone(),
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
        let headers = create_user_with_permissions("streamer", vec![Permission::Download]).await?;

        let response = super::stream::stream_track_response(
            &headers,
            agdb::DbId(track_id),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
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
        let headers = create_user_with_permissions("streamer", vec![Permission::Download]).await?;

        let response = super::stream::stream_track_response(
            &headers,
            agdb::DbId(track_id),
            Some("mp3".to_string()),
            None,
            Some(96_000),
            None,
            None,
            None,
            None,
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
    async fn stream_forces_transcode_when_knob_supplied_with_matching_format() -> anyhow::Result<()>
    {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = create_user_with_permissions("streamer", vec![Permission::Download]).await?;

        let response = super::stream::stream_track_response(
            &headers,
            agdb::DbId(track_id),
            Some("flac".to_string()),
            None,
            None,
            Some(48_000),
            None,
            None,
            None,
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
            "a sample-rate knob alone must still force the transcoded (chunked) path even when the output format matches the source"
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
    async fn stream_restores_direct_copy_when_policy_zeroes_cap() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = create_user_with_permissions("streamer", vec![Permission::Download]).await?;

        let response = super::stream::stream_track_response(
            &headers,
            agdb::DbId(track_id),
            Some("flac".to_string()),
            None,
            Some(96_000),
            None,
            None,
            None,
            None,
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
        let headers = create_user_with_permissions("streamer", vec![Permission::Download]).await?;

        let result = super::stream::stream_track_response(
            &headers,
            agdb::DbId(track_id),
            Some("mp3".to_string()),
            None,
            Some(0),
            None,
            None,
            None,
            None,
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
