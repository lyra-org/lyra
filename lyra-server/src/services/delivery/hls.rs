// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use lyra_ffmpeg::AudioCodec;

use super::{
    DeliveryError,
    TrackAccess,
    TranscodeKnobs,
    ValidatedRequest,
    ValidatedTrackSource,
    apply_request_start_offset,
    apply_transcode_policy,
    reject_unbound_options,
    source_range_duration_ms,
    validate_and_get_track_source,
    validate_request,
};
use crate::services::{
    auth::media_tokens::DeliveryOptions,
    hls::codec::{
        HLS_AUDIO_BITRATE_KBPS,
        HlsCodecProfile,
        HlsOutputConfig,
    },
};

const HLS_DURATION_MISMATCH_WARN_THRESHOLD_MS: u64 = 10;

#[derive(Clone, Debug, Default)]
pub(crate) struct HlsPlaylistOptions {
    pub(crate) codec: Option<String>,
    pub(crate) bitrate_bps: Option<u32>,
    pub(crate) sample_rate_hz: Option<u32>,
    pub(crate) channels: Option<u32>,
    pub(crate) prefer_vbr: Option<bool>,
    pub(crate) start_offset_ms: Option<u64>,
}

impl HlsPlaylistOptions {
    /// The options a request through `access` is served with; see
    /// [`super::ServeTrackOptions::bound_to`].
    pub(crate) fn bound_to(self, access: &TrackAccess) -> Result<Self, DeliveryError> {
        let TrackAccess::MediaToken(token) = access else {
            return Ok(self);
        };
        reject_unbound_options(&DeliveryOptions {
            format: None,
            codec: self.codec,
            copy_profiles: None,
            bitrate_bps: self.bitrate_bps,
            sample_rate_hz: self.sample_rate_hz,
            channels: self.channels,
            prefer_vbr: self.prefer_vbr,
        })?;
        let options = token.options.clone();
        Ok(Self {
            codec: options.codec,
            bitrate_bps: options.bitrate_bps,
            sample_rate_hz: options.sample_rate_hz,
            channels: options.channels,
            prefer_vbr: options.prefer_vbr,
            start_offset_ms: self.start_offset_ms,
        })
    }
}

/// What a track's media playlist serves: the source it segments, for how long, and the output
/// its segments are encoded to.
pub(crate) struct HlsRendition {
    pub(crate) source: ValidatedTrackSource,
    pub(crate) duration_ms: u64,
    pub(crate) output: HlsOutputConfig,
}

/// The rendition a media playlist of `track_id` through `access` with `options` serves.
pub(crate) async fn resolve_hls(
    track_id: &str,
    access: &TrackAccess,
    options: HlsPlaylistOptions,
) -> Result<HlsRendition, DeliveryError> {
    let HlsPlaylistOptions {
        codec,
        bitrate_bps,
        sample_rate_hz,
        channels,
        prefer_vbr,
        start_offset_ms,
    } = options;
    let source = apply_request_start_offset(
        validate_and_get_track_source(track_id, access).await?,
        start_offset_ms,
    )?;
    if let (Some(track_duration_ms), Some(range_duration_ms)) = (
        source.duration_ms.filter(|duration_ms| *duration_ms > 0),
        source_range_duration_ms(source.start_ms, source.end_ms),
    ) && track_duration_ms.abs_diff(range_duration_ms) >= HLS_DURATION_MISMATCH_WARN_THRESHOLD_MS
    {
        tracing::warn!(
            track_id,
            source_db_id = source.source_id.0,
            track_duration_ms,
            range_duration_ms,
            "HLS playlist duration differs from source range; using source range duration"
        );
    }
    let duration_ms =
        resolve_hls_playlist_duration_ms(source.duration_ms, source.start_ms, source.end_ms)
            .ok_or_else(|| {
                DeliveryError::service_unavailable("HLS requires a known positive track duration")
            })?;
    let validated = validate_request(None, codec, None)?;
    let knobs = TranscodeKnobs {
        bitrate_bps,
        sample_rate_hz,
        channels,
        prefer_vbr,
    };
    let profile = resolve_hls_profile(&source, &validated, knobs)?;
    let policy = apply_transcode_policy(knobs, profile.codec, (&source).into())?;
    // The encoder-side value, not the constraining cap: a cap dropped for asking at or
    // above the source must still target the source rather than the HLS default.
    let audio_bitrate_kbps = resolve_hls_audio_bitrate_kbps(profile, policy.encoder_bitrate_bps);
    let output = if profile.is_copy {
        HlsOutputConfig::new(profile, audio_bitrate_kbps, None, None, false)
    } else {
        HlsOutputConfig::new(
            profile,
            audio_bitrate_kbps,
            policy.sample_rate_hz,
            policy.channels,
            policy.prefer_vbr,
        )
    };
    Ok(HlsRendition {
        source,
        duration_ms,
        output,
    })
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

fn bitrate_kbps_from_bps(bitrate_bps: u32) -> u32 {
    bitrate_bps.saturating_add(999) / 1000
}

// Bare native HLS requests still default to AAC transcoding. Stream copy is only
// considered when the request explicitly asks for copy or already targets the
// source codec, and only if the remaining output limits do not require audio
// changes.
fn hls_copy_is_eligible_for_request(
    source: &ValidatedTrackSource,
    copy_requested: bool,
    requested_codec: Option<AudioCodec>,
    bitrate_bps: Option<u32>,
    sample_rate_hz: Option<u32>,
    channels: Option<u32>,
) -> bool {
    let Some(source_codec) = source.source_codec else {
        return false;
    };

    if HlsCodecProfile::for_copy_source(source_codec).is_err() {
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
        let Some(source_bitrate_bps) = source.format.bitrate_bps else {
            return false;
        };
        if requested_bitrate_bps < source_bitrate_bps {
            return false;
        }
    }

    if let Some(requested_sample_rate_hz) = sample_rate_hz {
        let Some(source_sample_rate_hz) = source.format.sample_rate_hz else {
            return false;
        };
        if requested_sample_rate_hz < source_sample_rate_hz {
            return false;
        }
    }

    if let Some(requested_channels) = channels {
        let Some(source_channels) = source.format.channel_count else {
            return false;
        };
        if requested_channels < source_channels {
            return false;
        }
    }

    true
}

/// Picks the HLS codec profile for a request: stream copy when the source can be
/// segmented untouched, otherwise the requested (or default AAC) encoder.
///
/// `POST /api/tracks/{id}/playback-url` calls this at mint time so an `hls_url`
/// is only handed out when the same question answers the same way at GET time.
pub(crate) fn resolve_hls_profile(
    source: &ValidatedTrackSource,
    validated: &ValidatedRequest,
    knobs: TranscodeKnobs,
) -> Result<HlsCodecProfile, DeliveryError> {
    let copy_requested = validated
        .preferred_codecs
        .iter()
        .any(|codec| matches!(codec, AudioCodec::Copy));
    let requested_hls_codec = validated
        .preferred_codecs
        .iter()
        .copied()
        .find(|codec| !matches!(codec, AudioCodec::Copy));

    let profile = if hls_copy_is_eligible_for_request(
        source,
        copy_requested,
        requested_hls_codec,
        knobs.bitrate_bps,
        knobs.sample_rate_hz,
        knobs.channels,
    ) {
        HlsCodecProfile::for_copy_source(
            source
                .source_codec
                .expect("copy eligibility requires an inferred source codec"),
        )
    } else {
        HlsCodecProfile::from_requested_codecs(&validated.preferred_codecs)
    };
    profile.map_err(|err| DeliveryError::bad_request(err.to_string()))
}

fn resolve_hls_audio_bitrate_kbps(
    profile: HlsCodecProfile,
    bitrate_bps: Option<u32>,
) -> Option<u32> {
    if profile.is_copy {
        return None;
    }

    (!profile.codec.is_lossless()).then(|| {
        bitrate_bps
            .map(bitrate_kbps_from_bps)
            .unwrap_or(HLS_AUDIO_BITRATE_KBPS)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hls_transcode_bitrate_follows_the_source_when_a_cap_is_dropped() {
        let profile =
            HlsCodecProfile::from_requested(Some(AudioCodec::Aac)).expect("aac is an hls profile");
        // A lossy source: only then is its bitrate a target the encoder should pin to.
        let source = super::super::decision::SourceAudio {
            bitrate_bps: Some(96_000),
            lossy: true,
            ..super::super::decision::SourceAudio::default()
        };

        // A cap at the source is dropped, but the encoder must still target 96 kbps rather
        // than falling back to the 192 kbps HLS default.
        let policy = apply_transcode_policy(
            TranscodeKnobs {
                bitrate_bps: Some(96_000),
                ..TranscodeKnobs::default()
            },
            profile.codec,
            source,
        )
        .expect("policy");
        assert_eq!(policy.bitrate_bps, None);
        assert_eq!(
            resolve_hls_audio_bitrate_kbps(profile, policy.encoder_bitrate_bps),
            Some(96),
        );

        // With no cap at all the default still applies.
        let policy = apply_transcode_policy(TranscodeKnobs::default(), profile.codec, source)
            .expect("policy");
        assert_eq!(
            resolve_hls_audio_bitrate_kbps(profile, policy.encoder_bitrate_bps),
            Some(HLS_AUDIO_BITRATE_KBPS),
        );

        // A stream-copy profile still carries no bitrate.
        let copy = HlsCodecProfile::for_copy_source(AudioCodec::Aac).expect("copy");
        assert_eq!(resolve_hls_audio_bitrate_kbps(copy, Some(96_000)), None);
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
}
