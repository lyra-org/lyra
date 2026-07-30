// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use lyra_ffmpeg::{
    AudioCodec,
    AudioFormat,
    AudioVbrMode,
    Output,
};
use std::path::Path as FsPath;

use crate::routes::AppError;

use super::ValidatedTrackSource;

const DEFAULT_LOSSY_BITRATE_KBPS: u32 = 192;
const DEFAULT_VBR_CHANNELS: u32 = 2;

pub(crate) struct ValidatedRequest {
    pub(crate) format: Option<AudioFormat>,
    pub(crate) preferred_codecs: Vec<AudioCodec>,
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

pub(crate) fn validate_request(
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

fn resolve_output_format(
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

fn resolve_codec(
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
pub(crate) struct TranscodePolicy {
    /// The cap that constrains the delivery decision. `None` means the request
    /// asked for no reduction, so it must not force a re-encode.
    pub(crate) bitrate_bps: Option<u32>,
    /// The bitrate the encoder should actually target. A cap that is dropped for
    /// asking too much still pins this to the source, so dropping a cap never
    /// silently inflates the encode up to the default.
    pub(crate) encoder_bitrate_bps: Option<u32>,
    pub(crate) sample_rate_hz: Option<u32>,
    pub(crate) channels: Option<u32>,
    pub(crate) prefer_vbr: bool,
}

/// Per-request transcode knobs as supplied by the caller, before policy clamping.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct TranscodeKnobs {
    pub(crate) bitrate_bps: Option<u32>,
    pub(crate) sample_rate_hz: Option<u32>,
    pub(crate) channels: Option<u32>,
    pub(crate) prefer_vbr: Option<bool>,
}

impl TranscodeKnobs {
    pub(crate) fn validate(self) -> Result<Self, AppError> {
        if matches!(self.bitrate_bps, Some(0)) {
            return Err(AppError::bad_request(
                "bitrate_bps must be greater than zero",
            ));
        }
        if matches!(self.sample_rate_hz, Some(0)) {
            return Err(AppError::bad_request(
                "sample_rate_hz must be greater than zero",
            ));
        }
        if matches!(self.channels, Some(0)) {
            return Err(AppError::bad_request("channels must be greater than zero"));
        }

        Ok(self)
    }
}

/// The source-side audio characteristics a policy is normalized against.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SourceAudio {
    pub(crate) bitrate_bps: Option<u32>,
    pub(crate) sample_rate_hz: Option<u32>,
    pub(crate) channels: Option<u32>,
    /// Whether the source is a known *lossy* codec, and therefore whether
    /// `bitrate_bps` is a quality ceiling worth encoding to.
    ///
    /// A lossless source's bitrate says how large the file is, not how much
    /// quality a lossy encoder could use. An unknown codec is treated the same
    /// as lossless: not pinnable.
    pub(crate) lossy: bool,
}

impl From<&ValidatedTrackSource> for SourceAudio {
    fn from(source: &ValidatedTrackSource) -> Self {
        Self {
            bitrate_bps: source.source_bitrate_bps,
            sample_rate_hz: source.source_sample_rate_hz,
            channels: source.source_channels,
            lossy: source
                .source_codec
                .is_some_and(|codec| !codec.is_lossless()),
        }
    }
}

fn clamp_to_codec_minimum(output_codec: AudioCodec, bitrate_bps: u32) -> u32 {
    let Some(min) = output_codec
        .min_bitrate_bps()
        .filter(|min| bitrate_bps < *min)
    else {
        return bitrate_bps;
    };

    tracing::info!(
        target: "transcode_policy",
        codec = ?output_codec,
        requested_bps = bitrate_bps,
        clamped_bps = min,
        "bitrate below codec minimum; clamping"
    );
    min
}

fn clamp_to_codec_maximum(output_codec: AudioCodec, bitrate_bps: u32, channels: u32) -> u32 {
    let Some(max) = output_codec
        .max_bitrate_bps(channels)
        .filter(|max| bitrate_bps > *max)
    else {
        return bitrate_bps;
    };

    tracing::info!(
        target: "transcode_policy",
        codec = ?output_codec,
        requested_bps = bitrate_bps,
        channels,
        clamped_bps = max,
        "bitrate above codec maximum; clamping"
    );
    max
}

fn clamp_to_codec_bitrate_range(output_codec: AudioCodec, bitrate_bps: u32, channels: u32) -> u32 {
    clamp_to_codec_maximum(
        output_codec,
        clamp_to_codec_minimum(output_codec, bitrate_bps),
        channels,
    )
}

/// Normalizes the requested knobs against the source and the output codec.
///
/// A knob survives only when honoring it would change the audio downward. A
/// request at or above the source asks for no change we are willing to make —
/// we never upsample, upmix, or inflate a bitrate — so it is dropped, which is
/// what lets an equal-valued request stay a bit-exact passthrough. When the
/// source value is unknown the knob is kept, because a no-op cannot be proven.
pub(crate) fn apply_transcode_policy(
    knobs: TranscodeKnobs,
    output_codec: AudioCodec,
    source: SourceAudio,
) -> Result<TranscodePolicy, AppError> {
    let knobs = knobs.validate()?;

    let sample_rate_hz = match knobs.sample_rate_hz {
        None => None,
        Some(hz)
            if source
                .sample_rate_hz
                .is_some_and(|source_hz| hz >= source_hz) =>
        {
            tracing::info!(
                target: "transcode_policy",
                codec = ?output_codec,
                requested_hz = hz,
                source_hz = source.sample_rate_hz,
                "requested sample rate is not below the source; delivering the source rate"
            );
            None
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

    let channels = match knobs.channels {
        None => None,
        Some(ch) if source.channels.is_some_and(|source_ch| ch >= source_ch) => {
            tracing::info!(
                target: "transcode_policy",
                codec = ?output_codec,
                requested_channels = ch,
                source_channels = source.channels,
                "requested channel count is not below the source; delivering the source channels"
            );
            None
        }
        Some(ch) => Some(ch),
    };
    let encoder_channels = channels
        .or(source.channels)
        .filter(|channels| *channels > 0)
        .unwrap_or(1);

    let (bitrate_bps, encoder_bitrate_bps) = match knobs.bitrate_bps {
        None => (None, None),
        Some(bps) => {
            if output_codec.is_lossless() {
                tracing::info!(
                    target: "transcode_policy",
                    codec = ?output_codec,
                    requested_bps = bps,
                    "bitrate cap ignored for lossless codec"
                );
                (None, None)
            } else if let Some(source_bps) = source.bitrate_bps
                && bps >= source_bps
            {
                // source bitrate is the average for VBR sources; peaks may exceed it and are not preserved here.
                let delivered_bps = source.lossy.then(|| {
                    clamp_to_codec_bitrate_range(
                        output_codec,
                        bps.min(source_bps),
                        encoder_channels,
                    )
                });
                tracing::info!(
                    target: "transcode_policy",
                    codec = ?output_codec,
                    requested_bps = bps,
                    source_bps,
                    lossy_source = source.lossy,
                    delivered_bps,
                    "cap at or above source bitrate; it no longer constrains the decision, and the encoder targets the source when the source is lossy"
                );
                (None, delivered_bps)
            } else {
                let delivered_bps =
                    clamp_to_codec_bitrate_range(output_codec, bps, encoder_channels);
                (Some(delivered_bps), Some(delivered_bps))
            }
        }
    };

    Ok(TranscodePolicy {
        bitrate_bps,
        encoder_bitrate_bps,
        sample_rate_hz,
        channels,
        prefer_vbr: knobs.prefer_vbr.unwrap_or(false),
    })
}

/// Which endpoint a decision is being resolved for. Decides both whether the
/// output container has to be streamable and which content type is delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeliveryTarget {
    Stream,
    Download,
}

impl DeliveryTarget {
    fn is_streaming(self) -> bool {
        matches!(self, Self::Stream)
    }

    fn content_type(self, output_format: AudioFormat) -> &'static str {
        output_format.mime_type(self.is_streaming())
    }
}

/// The rate-control directive handed to the encoder. Exactly one of these
/// applies, so a bitrate is never recorded for an output that carries none.
///
/// `TargetBitrate` names the directive, not the outcome: it sets a target
/// bitrate and nothing else, so an encoder that is variable-bitrate by default
/// (libopus, for one) stays variable. It is not a claim of constant bitrate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RateControl {
    TargetBitrate { bitrate_kbps: u32 },
    Abr { bitrate_kbps: u32 },
    Quality(i32),
}

/// Encoder settings as they are actually handed to ffmpeg, after the lossy
/// rate-control defaults and the VBR gate have been applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EncoderSettings {
    /// `None` when the codec carries no rate control (copy or lossless).
    pub(crate) rate_control: Option<RateControl>,
    pub(crate) sample_rate_hz: Option<u32>,
    pub(crate) channels: Option<u32>,
}

impl EncoderSettings {
    fn resolve(codec: AudioCodec, policy: &TranscodePolicy) -> Self {
        let rate_controlled = !matches!(codec, AudioCodec::Copy) && !codec.is_lossless();
        let rate_control = rate_controlled.then(|| {
            let bitrate_kbps = policy
                .encoder_bitrate_bps
                .map(|bps| bps.saturating_add(999) / 1000)
                .filter(|kbps| *kbps > 0)
                .unwrap_or(DEFAULT_LOSSY_BITRATE_KBPS);
            let vbr_mode = if policy.prefer_vbr {
                codec.vbr_mode(
                    policy
                        .encoder_bitrate_bps
                        .unwrap_or(bitrate_kbps.saturating_mul(1000)),
                    policy.channels.unwrap_or(DEFAULT_VBR_CHANNELS),
                )
            } else {
                None
            };

            match vbr_mode {
                Some(AudioVbrMode::Quality(quality)) => RateControl::Quality(quality),
                Some(AudioVbrMode::Abr) => RateControl::Abr { bitrate_kbps },
                None => RateControl::TargetBitrate { bitrate_kbps },
            }
        });

        Self {
            rate_control,
            sample_rate_hz: policy.sample_rate_hz,
            channels: policy.channels,
        }
    }
}

/// The single resolved answer to "what are we delivering, and how".
#[derive(Debug, Clone, Copy)]
pub(crate) struct DeliveryDecision {
    pub(crate) output_format: AudioFormat,
    pub(crate) codec: AudioCodec,
    pub(crate) content_type: &'static str,
    /// The source file can be served verbatim; no ffmpeg run is needed.
    pub(crate) direct_passthrough: bool,
    pub(crate) encoder: EncoderSettings,
}

impl DeliveryDecision {
    pub(crate) fn configure_output(&self, output: Output) -> Output {
        let mut output = output.audio_format(self.output_format).codec(self.codec);
        output = match self.encoder.rate_control {
            Some(RateControl::Quality(quality)) => output.audio_global_quality(quality),
            Some(RateControl::Abr { bitrate_kbps }) => output.audio_abr_bitrate_kbps(bitrate_kbps),
            Some(RateControl::TargetBitrate { bitrate_kbps }) => {
                output.audio_bitrate_kbps(bitrate_kbps)
            }
            None => output,
        };
        if let Some(hz) = self.encoder.sample_rate_hz {
            output = output.sample_rate_hz(hz);
        }
        if let Some(ch) = self.encoder.channels {
            output = output.channels(ch);
        }
        output
    }
}

/// Resolves the delivery decision for a source that already has any request
/// start offset applied.
///
/// Resolution runs twice: once allowing `copy`, and — when `copy` won that
/// first pass — once more with `copy` off, because a trimmed source or any
/// transcode knob rules `copy` out. The copy-off answer is also what the
/// transcode policy is derived from, so a bitrate cap is validated against the
/// codec that would actually encode it.
pub(crate) fn resolve_delivery(
    validated: &ValidatedRequest,
    source: &ValidatedTrackSource,
    knobs: TranscodeKnobs,
    target: DeliveryTarget,
) -> Result<DeliveryDecision, AppError> {
    let copy_output_format = resolve_output_format(
        validated.format,
        &validated.preferred_codecs,
        source.entry_format,
        &source.full_path,
        true,
    )?;

    let copy_codec = resolve_codec(
        &validated.preferred_codecs,
        copy_output_format,
        source.entry_format,
        true,
    )?;
    let copy_selected = matches!(copy_codec, AudioCodec::Copy);

    let transcode_output_format = if copy_selected {
        resolve_output_format(
            validated.format,
            &validated.preferred_codecs,
            source.entry_format,
            &source.full_path,
            false,
        )?
    } else {
        copy_output_format
    };
    let transcode_codec = if copy_selected {
        resolve_codec(
            &validated.preferred_codecs,
            transcode_output_format,
            source.entry_format,
            false,
        )?
    } else {
        copy_codec
    };

    let policy = apply_transcode_policy(knobs, transcode_codec, source.into())?;
    let trimmed = source.start_ms.is_some() || source.end_ms.is_some();
    let forcing_transcode = policy.bitrate_bps.is_some()
        || policy.sample_rate_hz.is_some()
        || policy.channels.is_some();
    let copy_ruled_out = trimmed || forcing_transcode;

    let (output_format, codec) = if copy_selected && copy_ruled_out {
        (transcode_output_format, transcode_codec)
    } else {
        (copy_output_format, copy_codec)
    };

    if target.is_streaming() && !output_format.supports_streaming() {
        return Err(AppError::bad_request(format!(
            "Format '{}' does not support streaming. Use /api/download or choose a streamable format (mp3, flac, wav, ogg, webm, aac, opus, aiff).",
            output_format.extension()
        )));
    }

    let direct_passthrough = matches!(codec, AudioCodec::Copy)
        && source.entry_format == Some(output_format)
        && !copy_ruled_out;

    Ok(DeliveryDecision {
        output_format,
        codec,
        content_type: target.content_type(output_format),
        direct_passthrough,
        encoder: EncoderSettings::resolve(codec, &policy),
    })
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_LOSSY_BITRATE_KBPS,
        DeliveryDecision,
        DeliveryTarget,
        EncoderSettings,
        RateControl,
        SourceAudio,
        TranscodeKnobs,
        TranscodePolicy,
        apply_transcode_policy,
        resolve_codec,
        resolve_delivery,
        resolve_output_format,
        validate_request,
    };
    use crate::routes::serve::ValidatedTrackSource;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use lyra_ffmpeg::{
        AudioCodec,
        AudioFormat,
        Output,
    };
    use std::path::{
        Path,
        PathBuf,
    };

    fn policy_passthrough(
        bitrate_bps: Option<u32>,
        sample_rate_hz: Option<u32>,
        channels: Option<u32>,
    ) -> TranscodePolicy {
        TranscodePolicy {
            bitrate_bps,
            encoder_bitrate_bps: bitrate_bps,
            sample_rate_hz,
            channels,
            prefer_vbr: false,
        }
    }

    fn configure(format: AudioFormat, codec: AudioCodec, policy: &TranscodePolicy) -> Output {
        DeliveryDecision {
            output_format: format,
            codec,
            content_type: DeliveryTarget::Stream.content_type(format),
            direct_passthrough: false,
            encoder: EncoderSettings::resolve(codec, policy),
        }
        .configure_output(Output::with_callback(|_| 0))
    }

    fn flac_source(start_ms: Option<u64>, end_ms: Option<u64>) -> ValidatedTrackSource {
        ValidatedTrackSource {
            source_id: agdb::DbId(2),
            source_public_id: "source-pub-2".to_string(),
            track_public_id: "track-pub-test".to_string(),
            input_path: "track.flac".to_string(),
            entry_format: Some(AudioFormat::Flac),
            source_codec: Some(AudioCodec::Flac),
            full_path: PathBuf::from("track.flac"),
            duration_ms: Some(20_000),
            start_ms,
            end_ms,
            source_bitrate_bps: Some(900_000),
            source_sample_rate_hz: Some(96_000),
            source_channels: Some(2),
        }
    }

    #[test]
    fn configure_output_defaults_bitrate_to_192_kbps_when_unset() {
        let output = configure(
            AudioFormat::Mp3,
            AudioCodec::Mp3,
            &policy_passthrough(None, None, None),
        );
        assert_eq!(output.configured_audio_bitrate_kbps(), Some(192));
        assert_eq!(output.configured_sample_rate_hz(), None);
        assert_eq!(output.configured_channels(), None);
    }

    #[test]
    fn configure_output_applies_supplied_bitrate_sample_rate_and_channels() {
        let output = configure(
            AudioFormat::Opus,
            AudioCodec::Opus,
            &policy_passthrough(Some(96_000), Some(48_000), Some(2)),
        );
        assert_eq!(output.configured_audio_bitrate_kbps(), Some(96));
        assert_eq!(output.configured_sample_rate_hz(), Some(48_000));
        assert_eq!(output.configured_channels(), Some(2));
    }

    #[test]
    fn configure_output_rounds_bitrate_upward_to_the_nearest_kbps() {
        let output = configure(
            AudioFormat::Mp3,
            AudioCodec::Mp3,
            &policy_passthrough(Some(127_500), None, None),
        );
        assert_eq!(output.configured_audio_bitrate_kbps(), Some(128));
    }

    #[test]
    fn configure_output_uses_vbr_when_preferred() {
        let output = configure(
            AudioFormat::Mp3,
            AudioCodec::Mp3,
            &TranscodePolicy {
                bitrate_bps: Some(192_000),
                encoder_bitrate_bps: Some(192_000),
                sample_rate_hz: None,
                channels: Some(2),
                prefer_vbr: true,
            },
        );
        assert_eq!(output.configured_audio_global_quality(), Some(2));
        assert_eq!(output.configured_audio_bitrate_kbps(), None);
    }

    #[test]
    fn configure_output_feeds_the_default_bitrate_into_the_vbr_gate() {
        let policy = TranscodePolicy {
            bitrate_bps: None,
            encoder_bitrate_bps: None,
            sample_rate_hz: None,
            channels: None,
            prefer_vbr: true,
        };
        assert_eq!(
            EncoderSettings::resolve(AudioCodec::Mp3, &policy).rate_control,
            Some(RateControl::Quality(2)),
            "the 192 kbps default must drive the VBR gate when no bitrate is requested, and the quality-mode result must not also carry a bitrate the encoder never receives"
        );

        let output = configure(AudioFormat::Mp3, AudioCodec::Mp3, &policy);
        assert_eq!(output.configured_audio_global_quality(), Some(2));
        assert_eq!(output.configured_audio_bitrate_kbps(), None);
    }

    #[test]
    fn configure_output_uses_abr_when_the_codec_selects_it() {
        let policy = TranscodePolicy {
            bitrate_bps: Some(64_000),
            encoder_bitrate_bps: Some(64_000),
            sample_rate_hz: None,
            channels: Some(2),
            prefer_vbr: true,
        };
        assert_eq!(
            EncoderSettings::resolve(AudioCodec::Mp3, &policy).rate_control,
            Some(RateControl::Abr { bitrate_kbps: 64 })
        );

        let output = configure(AudioFormat::Mp3, AudioCodec::Mp3, &policy);
        assert_eq!(output.configured_audio_bitrate_kbps(), Some(64));
        assert!(output.configured_audio_abr());
        assert_eq!(output.configured_audio_global_quality(), None);
    }

    #[test]
    fn configure_output_falls_back_to_a_plain_target_bitrate_when_the_codec_has_no_vbr_mode() {
        let policy = TranscodePolicy {
            bitrate_bps: Some(128_000),
            encoder_bitrate_bps: Some(128_000),
            sample_rate_hz: None,
            channels: Some(2),
            prefer_vbr: true,
        };
        assert_eq!(
            EncoderSettings::resolve(AudioCodec::Opus, &policy).rate_control,
            Some(RateControl::TargetBitrate { bitrate_kbps: 128 })
        );

        let output = configure(AudioFormat::Opus, AudioCodec::Opus, &policy);
        assert_eq!(output.configured_audio_bitrate_kbps(), Some(128));
        assert!(!output.configured_audio_abr());
        assert_eq!(output.configured_audio_global_quality(), None);
    }

    #[test]
    fn configure_output_skips_rate_control_for_lossless_codecs() {
        let policy = TranscodePolicy {
            bitrate_bps: Some(96_000),
            encoder_bitrate_bps: Some(96_000),
            sample_rate_hz: Some(48_000),
            channels: None,
            prefer_vbr: true,
        };
        assert_eq!(
            EncoderSettings::resolve(AudioCodec::Flac, &policy).rate_control,
            None
        );

        let output = configure(AudioFormat::Flac, AudioCodec::Flac, &policy);
        assert_eq!(output.configured_audio_bitrate_kbps(), None);
        assert_eq!(output.configured_audio_global_quality(), None);
        assert_eq!(
            output.configured_sample_rate_hz(),
            Some(48_000),
            "sample rate still applies to lossless outputs"
        );
    }

    #[test]
    fn configure_output_skips_rate_control_for_copy() {
        let policy = TranscodePolicy {
            bitrate_bps: Some(96_000),
            encoder_bitrate_bps: Some(96_000),
            sample_rate_hz: None,
            channels: None,
            prefer_vbr: true,
        };
        assert_eq!(
            EncoderSettings::resolve(AudioCodec::Copy, &policy).rate_control,
            None
        );

        let output = configure(AudioFormat::Flac, AudioCodec::Copy, &policy);
        assert_eq!(output.configured_audio_bitrate_kbps(), None);
        assert_eq!(output.configured_audio_global_quality(), None);
    }

    fn knobs(
        bitrate_bps: Option<u32>,
        sample_rate_hz: Option<u32>,
        channels: Option<u32>,
    ) -> TranscodeKnobs {
        TranscodeKnobs {
            bitrate_bps,
            sample_rate_hz,
            channels,
            prefer_vbr: None,
        }
    }

    #[test]
    fn policy_rejects_zero_bitrate() {
        let err = apply_transcode_policy(
            knobs(Some(0), None, None),
            AudioCodec::Mp3,
            SourceAudio::default(),
        )
        .expect_err("bitrate_bps=0 must fail fast, not silently fall back to the default");
        assert_eq!(err.into_response().status(), StatusCode::BAD_REQUEST);

        let err = TranscodeKnobs {
            bitrate_bps: Some(0),
            ..TranscodeKnobs::default()
        }
        .validate()
        .expect_err("standalone validation must use the same error");
        assert_eq!(
            format!("{err:?}"),
            "AppError(400 Bad Request: bitrate_bps must be greater than zero)"
        );
    }

    #[test]
    fn policy_rejects_zero_sample_rate() {
        let err = apply_transcode_policy(
            knobs(None, Some(0), None),
            AudioCodec::Mp3,
            SourceAudio::default(),
        )
        .expect_err("sample_rate_hz=0 must fail fast");
        assert_eq!(
            format!("{err:?}"),
            "AppError(400 Bad Request: sample_rate_hz must be greater than zero)"
        );
    }

    #[test]
    fn policy_rejects_zero_channels() {
        let err = apply_transcode_policy(
            knobs(None, None, Some(0)),
            AudioCodec::Mp3,
            SourceAudio::default(),
        )
        .expect_err("channels=0 must fail fast");
        assert_eq!(
            format!("{err:?}"),
            "AppError(400 Bad Request: channels must be greater than zero)"
        );
    }

    #[test]
    fn policy_clamps_bitrate_below_codec_minimum() {
        let policy = apply_transcode_policy(
            knobs(Some(1), None, None),
            AudioCodec::Mp3,
            SourceAudio::default(),
        )
        .expect("below-minimum bitrate should clamp, not reject");
        assert_eq!(
            policy.bitrate_bps,
            Some(AudioCodec::Mp3.min_bitrate_bps().unwrap())
        );
    }

    #[test]
    fn policy_drops_bitrate_cap_for_lossless_codec() {
        let policy = apply_transcode_policy(
            knobs(Some(96_000), None, None),
            AudioCodec::Flac,
            SourceAudio::default(),
        )
        .expect("lossless codec ignores bitrate cap");
        assert_eq!(
            policy.bitrate_bps, None,
            "flac output must drop the bitrate cap entirely rather than advertise a cap it cannot honor"
        );
    }

    #[test]
    fn policy_rewrites_opus_sample_rate_to_native_48000() {
        let policy = apply_transcode_policy(
            knobs(None, Some(44_100), None),
            AudioCodec::Opus,
            SourceAudio {
                sample_rate_hz: Some(96_000),
                ..SourceAudio::default()
            },
        )
        .expect("opus substitutes non-48kHz sample rates");
        assert_eq!(
            policy.sample_rate_hz,
            Some(48_000),
            "opus substitutes internally; advertise what we deliver"
        );
    }

    #[test]
    fn policy_passes_matching_opus_sample_rate_through() {
        let policy = apply_transcode_policy(
            knobs(None, Some(48_000), None),
            AudioCodec::Opus,
            SourceAudio {
                sample_rate_hz: Some(96_000),
                ..SourceAudio::default()
            },
        )
        .expect("48kHz request for opus should pass through");
        assert_eq!(policy.sample_rate_hz, Some(48_000));
    }

    #[test]
    fn policy_passes_bitrate_through_when_source_unknown() {
        let policy = apply_transcode_policy(
            knobs(Some(192_000), None, None),
            AudioCodec::Mp3,
            SourceAudio::default(),
        )
        .expect("unknown source bitrate must not block the cap");
        assert_eq!(
            policy.bitrate_bps,
            Some(192_000),
            "when source bitrate is unknown, the requested cap flows through untouched"
        );
        assert_eq!(policy.encoder_bitrate_bps, Some(192_000));
    }

    #[test]
    fn policy_drops_bitrate_cap_above_source_bitrate() {
        let policy = apply_transcode_policy(
            knobs(Some(320_000), None, None),
            AudioCodec::Mp3,
            SourceAudio {
                bitrate_bps: Some(128_000),
                ..SourceAudio::default()
            },
        )
        .expect("cap above source should not inflate quality");
        assert_eq!(
            policy.bitrate_bps, None,
            "a 320 kbps cap on a 128 kbps source should drop to no cap so we don't upsample quality"
        );
    }

    #[test]
    fn policy_drops_bitrate_cap_equal_to_source_bitrate() {
        let policy = apply_transcode_policy(
            knobs(Some(128_000), None, None),
            AudioCodec::Mp3,
            SourceAudio {
                bitrate_bps: Some(128_000),
                ..SourceAudio::default()
            },
        )
        .expect("a cap equal to the source asks for no change");
        assert_eq!(
            policy.bitrate_bps, None,
            "an exactly-equal cap changes nothing, so it must not survive to force a re-encode"
        );
    }

    #[test]
    fn policy_pins_the_encoder_to_the_source_when_the_cap_is_dropped() {
        // Dropping the cap must not mean "fall back to the 192 kbps default": a source that
        // cannot supply more quality must not be re-encoded upward.
        for requested in [Some(96_000), Some(128_000), Some(900_000)] {
            let policy = apply_transcode_policy(
                knobs(requested, None, None),
                AudioCodec::Vorbis,
                SourceAudio {
                    bitrate_bps: Some(96_000),
                    lossy: true,
                    ..SourceAudio::default()
                },
            )
            .expect("a cap at or above the source is dropped, not rejected");
            assert_eq!(
                policy.bitrate_bps, None,
                "requested={requested:?} must not constrain the decision"
            );
            assert_eq!(
                policy.encoder_bitrate_bps,
                Some(96_000),
                "requested={requested:?} must still encode at the source bitrate"
            );
            assert_eq!(
                EncoderSettings::resolve(AudioCodec::Vorbis, &policy).rate_control,
                Some(RateControl::TargetBitrate { bitrate_kbps: 96 }),
                "requested={requested:?} must reach the encoder as 96 kbps, not the 192 kbps default"
            );
        }
    }

    #[test]
    fn policy_leaves_a_dropped_cap_at_the_default_for_a_lossless_source() {
        // A lossless source's bitrate measures file size, not quality headroom. Pinning a
        // lossy encoder to it would target absurd rates — a 24/96 FLAC runs several Mbps,
        // and even a safe encoder clamp would not make that a meaningful quality target.
        for source in [
            SourceAudio {
                bitrate_bps: Some(4_600_000),
                lossy: false,
                ..SourceAudio::default()
            },
            // An unknown source codec is treated the same: not pinnable.
            SourceAudio {
                bitrate_bps: Some(4_600_000),
                ..SourceAudio::default()
            },
        ] {
            let policy = apply_transcode_policy(
                knobs(Some(5_000_000), None, None),
                AudioCodec::Opus,
                source,
            )
            .expect("the cap is dropped, not rejected");
            assert_eq!(policy.bitrate_bps, None);
            assert_eq!(
                policy.encoder_bitrate_bps, None,
                "lossy={} must fall back to the default rather than pinning to the source",
                source.lossy
            );
            assert_eq!(
                EncoderSettings::resolve(AudioCodec::Opus, &policy).rate_control,
                Some(RateControl::TargetBitrate {
                    bitrate_kbps: DEFAULT_LOSSY_BITRATE_KBPS
                }),
            );
        }

        // The same request against a lossy source of the same bitrate does pin, so the
        // distinction is the source's codec and nothing else.
        let policy = apply_transcode_policy(
            knobs(Some(5_000_000), None, None),
            AudioCodec::Opus,
            SourceAudio {
                bitrate_bps: Some(320_000),
                channels: Some(2),
                lossy: true,
                ..SourceAudio::default()
            },
        )
        .expect("the cap is dropped, not rejected");
        assert_eq!(policy.encoder_bitrate_bps, Some(320_000));
    }

    #[test]
    fn policy_clamps_a_dropped_cap_up_to_the_codec_minimum() {
        let policy = apply_transcode_policy(
            knobs(Some(40_000), None, None),
            AudioCodec::Mp3,
            SourceAudio {
                bitrate_bps: Some(20_000),
                lossy: true,
                ..SourceAudio::default()
            },
        )
        .expect("a cap above a very low source is dropped");
        assert_eq!(policy.bitrate_bps, None);
        assert_eq!(
            policy.encoder_bitrate_bps,
            Some(AudioCodec::Mp3.min_bitrate_bps().unwrap()),
            "the source bitrate is below what mp3 can encode, so the minimum still applies"
        );
    }

    #[test]
    fn policy_leaves_the_encoder_unset_for_lossless_output() {
        let policy = apply_transcode_policy(
            knobs(Some(96_000), None, None),
            AudioCodec::Flac,
            SourceAudio {
                bitrate_bps: Some(900_000),
                ..SourceAudio::default()
            },
        )
        .expect("lossless ignores the cap");
        assert_eq!(policy.bitrate_bps, None);
        assert_eq!(
            policy.encoder_bitrate_bps, None,
            "a lossless encoder takes no bitrate at all"
        );
    }

    #[test]
    fn policy_retains_bitrate_cap_below_source_bitrate() {
        let policy = apply_transcode_policy(
            knobs(Some(96_000), None, None),
            AudioCodec::Mp3,
            SourceAudio {
                bitrate_bps: Some(320_000),
                ..SourceAudio::default()
            },
        )
        .expect("legitimate cap below source should pass through");
        assert_eq!(policy.bitrate_bps, Some(96_000));
        assert_eq!(policy.encoder_bitrate_bps, Some(96_000));
    }

    #[test]
    fn policy_drops_sample_rate_and_channels_that_are_not_below_the_source() {
        let source = SourceAudio {
            bitrate_bps: None,
            sample_rate_hz: Some(44_100),
            channels: Some(2),
            lossy: true,
        };

        let equal =
            apply_transcode_policy(knobs(None, Some(44_100), Some(2)), AudioCodec::Mp3, source)
                .expect("equal values ask for no change");
        assert_eq!(equal.sample_rate_hz, None);
        assert_eq!(equal.channels, None);

        let above =
            apply_transcode_policy(knobs(None, Some(96_000), Some(6)), AudioCodec::Mp3, source)
                .expect("above-source values ask for upsampling and upmixing");
        assert_eq!(
            above.sample_rate_hz, None,
            "we never upsample, so the knob is a no-op and must not force a re-encode"
        );
        assert_eq!(above.channels, None, "we never upmix");
    }

    #[test]
    fn policy_retains_sample_rate_and_channels_below_the_source() {
        let policy = apply_transcode_policy(
            knobs(None, Some(22_050), Some(1)),
            AudioCodec::Mp3,
            SourceAudio {
                bitrate_bps: None,
                sample_rate_hz: Some(44_100),
                channels: Some(2),
                lossy: true,
            },
        )
        .expect("downward requests are honored");
        assert_eq!(policy.sample_rate_hz, Some(22_050));
        assert_eq!(policy.channels, Some(1));
    }

    #[test]
    fn policy_retains_sample_rate_and_channels_when_the_source_is_unknown() {
        let policy = apply_transcode_policy(
            knobs(None, Some(48_000), Some(2)),
            AudioCodec::Mp3,
            SourceAudio::default(),
        )
        .expect("an unprovable no-op must be honored rather than dropped");
        assert_eq!(
            policy.sample_rate_hz,
            Some(48_000),
            "without a known source rate we cannot prove the knob is a no-op, so it stands"
        );
        assert_eq!(policy.channels, Some(2));
    }

    #[test]
    fn policy_preserves_untouched_knobs_for_lossy_passthrough_values() {
        let policy = apply_transcode_policy(
            knobs(Some(128_000), Some(44_100), Some(2)),
            AudioCodec::Mp3,
            SourceAudio {
                bitrate_bps: Some(320_000),
                sample_rate_hz: Some(96_000),
                channels: Some(6),
                lossy: true,
            },
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
    fn content_type_follows_the_delivery_target_for_every_format() {
        for format in [
            AudioFormat::Mp3,
            AudioFormat::Flac,
            AudioFormat::Wav,
            AudioFormat::Ogg,
            AudioFormat::Webm,
            AudioFormat::Aac,
            AudioFormat::M4a,
            AudioFormat::Opus,
            AudioFormat::Aiff,
            AudioFormat::Alac,
            AudioFormat::Caf,
            AudioFormat::Wma,
        ] {
            assert_eq!(
                DeliveryTarget::Stream.content_type(format),
                format.mime_type(true),
                "{format:?}"
            );
            assert_eq!(
                DeliveryTarget::Download.content_type(format),
                format.mime_type(false),
                "{format:?}"
            );
        }

        // aac is the one format whose mime type depends on the flag, which is exactly the
        // fact the target already carries; pin that the two targets stay distinct.
        assert_eq!(
            DeliveryTarget::Stream.content_type(AudioFormat::Aac),
            "audio/aac"
        );
        assert_eq!(
            DeliveryTarget::Download.content_type(AudioFormat::Aac),
            "audio/mp4"
        );
    }

    #[test]
    fn resolve_delivery_reports_direct_passthrough_for_untouched_source() {
        let source = flac_source(None, None);
        let validated = validate_request(None, None).expect("empty request parses");
        let decision = resolve_delivery(
            &validated,
            &source,
            TranscodeKnobs::default(),
            DeliveryTarget::Stream,
        )
        .expect("flac source streams as-is");

        assert!(decision.direct_passthrough);
        assert_eq!(decision.output_format, AudioFormat::Flac);
        assert_eq!(decision.codec, AudioCodec::Copy);
        assert_eq!(decision.encoder.rate_control, None);
        assert_eq!(decision.content_type, "audio/flac");
    }

    #[test]
    fn resolve_delivery_drops_passthrough_when_a_knob_forces_transcoding() {
        let source = flac_source(None, None);
        let validated = validate_request(None, None).expect("empty request parses");
        let decision = resolve_delivery(
            &validated,
            &source,
            TranscodeKnobs {
                sample_rate_hz: Some(48_000),
                ..TranscodeKnobs::default()
            },
            DeliveryTarget::Download,
        )
        .expect("sample rate knob forces a re-encode");

        assert!(!decision.direct_passthrough);
        assert_eq!(decision.codec, AudioCodec::Flac);
        assert_eq!(decision.encoder.sample_rate_hz, Some(48_000));
        assert_eq!(
            decision.encoder.rate_control, None,
            "lossless re-encodes carry no bitrate"
        );
    }

    #[test]
    fn resolve_delivery_drops_passthrough_for_cue_ranges() {
        let source = flac_source(Some(10_000), Some(30_000));
        let validated = validate_request(None, None).expect("empty request parses");
        let decision = resolve_delivery(
            &validated,
            &source,
            TranscodeKnobs::default(),
            DeliveryTarget::Download,
        )
        .expect("cue-derived ranges must be re-encoded");

        assert!(!decision.direct_passthrough);
        assert_eq!(decision.codec, AudioCodec::Flac);
    }

    #[test]
    fn resolve_delivery_rejects_a_non_streamable_final_format() {
        let source = flac_source(None, None);
        // This is otherwise a valid transcode decision; only the stream target cannot carry it.
        let validated = validate_request(Some("m4a".to_string()), Some("aac".to_string()))
            .expect("m4a + aac parses");
        assert!(AudioFormat::M4a.supports_codec(AudioCodec::Aac));

        let err = resolve_delivery(
            &validated,
            &source,
            TranscodeKnobs::default(),
            DeliveryTarget::Stream,
        )
        .expect_err("m4a cannot be streamed");
        assert_eq!(
            format!("{err:?}"),
            "AppError(400 Bad Request: Format 'm4a' does not support streaming. \
             Use /api/download or choose a streamable format \
             (mp3, flac, wav, ogg, webm, aac, opus, aiff).)"
        );

        resolve_delivery(
            &validated,
            &source,
            TranscodeKnobs::default(),
            DeliveryTarget::Download,
        )
        .expect("the same request is fine for download");
    }

    #[test]
    fn resolve_delivery_enforces_streamability_of_the_final_container() {
        // `codec=copy,<x>` is where the provisional copy container and final container can
        // diverge after a surviving knob rules copy out. The final answer controls whether
        // the stream endpoint can serve the request.
        let mut m4a = flac_source(None, None);
        m4a.entry_format = Some(AudioFormat::M4a);
        m4a.source_codec = Some(AudioCodec::Alac);
        let validated = validate_request(None, Some("copy,mp3".to_string())).expect("parses");
        let downmix = TranscodeKnobs {
            channels: Some(1),
            ..TranscodeKnobs::default()
        };

        let decision = resolve_delivery(&validated, &m4a, downmix, DeliveryTarget::Download)
            .expect("m4a downloads fine");
        assert_eq!(decision.output_format, AudioFormat::Mp3);
        resolve_delivery(&validated, &m4a, downmix, DeliveryTarget::Stream)
            .expect("the final mp3 container is streamable");

        let mut mp3 = flac_source(None, None);
        mp3.entry_format = Some(AudioFormat::Mp3);
        mp3.source_codec = Some(AudioCodec::Mp3);
        let validated = validate_request(None, Some("copy,aac".to_string())).expect("parses");

        let decision = resolve_delivery(&validated, &mp3, downmix, DeliveryTarget::Download)
            .expect("mp3 downloads fine");
        assert_eq!(decision.output_format, AudioFormat::M4a);
        resolve_delivery(&validated, &mp3, downmix, DeliveryTarget::Stream)
            .expect_err("the final m4a container is not streamable");
    }

    #[test]
    fn resolve_delivery_clamps_wma_source_bitrate_to_the_opus_channel_limit() {
        let mut source = flac_source(None, None);
        source.entry_format = Some(AudioFormat::Wma);
        source.source_codec = Some(AudioCodec::Wma);
        source.source_bitrate_bps = Some(768_000);
        source.source_channels = Some(2);
        let validated = validate_request(None, Some("opus".to_string())).expect("opus parses");

        let decision = resolve_delivery(
            &validated,
            &source,
            TranscodeKnobs {
                bitrate_bps: Some(900_000),
                channels: Some(1),
                ..TranscodeKnobs::default()
            },
            DeliveryTarget::Download,
        )
        .expect("the WMA source can be transcoded to Opus");

        assert_eq!(decision.output_format, AudioFormat::Opus);
        assert_eq!(decision.codec, AudioCodec::Opus);
        assert_eq!(decision.encoder.channels, Some(1));
        assert_eq!(
            decision.encoder.rate_control,
            Some(RateControl::TargetBitrate { bitrate_kbps: 256 })
        );
    }

    #[test]
    fn resolve_delivery_reports_effective_lossy_encoder_settings() {
        let source = flac_source(None, None);
        let validated =
            validate_request(Some("mp3".to_string()), None).expect("mp3 request parses");
        let decision = resolve_delivery(
            &validated,
            &source,
            TranscodeKnobs {
                prefer_vbr: Some(true),
                ..TranscodeKnobs::default()
            },
            DeliveryTarget::Stream,
        )
        .expect("mp3 transcode resolves");

        assert_eq!(decision.codec, AudioCodec::Mp3);
        assert_eq!(decision.encoder.rate_control, Some(RateControl::Quality(2)));
        assert_eq!(decision.content_type, "audio/mpeg");
    }
}
