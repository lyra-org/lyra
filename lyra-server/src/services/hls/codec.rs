// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use lyra_ffmpeg::{
    AudioCodec,
    Output,
};
use std::path::Path as FsPath;

use super::HlsError;

pub(crate) const HLS_SEGMENT_TIME_SECONDS: u32 = 6;
pub(crate) const HLS_AUDIO_BITRATE_KBPS: u32 = 192;
pub(crate) const HLS_START_NUMBER: u32 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct HlsCodecProfile {
    pub(crate) codec: AudioCodec,
    pub(crate) ffmpeg_codec_str: &'static str,
    pub(crate) segment_type: &'static str,
    pub(crate) segment_extension: &'static str,
    pub(crate) init_filename: Option<&'static str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct HlsOutputConfig {
    pub(crate) profile: HlsCodecProfile,
    pub(crate) audio_bitrate_kbps: Option<u32>,
    pub(crate) sample_rate_hz: Option<u32>,
    pub(crate) channels: Option<u32>,
}

impl HlsOutputConfig {
    pub(crate) fn new(
        profile: HlsCodecProfile,
        audio_bitrate_kbps: Option<u32>,
        sample_rate_hz: Option<u32>,
        channels: Option<u32>,
    ) -> Self {
        Self {
            profile,
            audio_bitrate_kbps,
            sample_rate_hz,
            channels,
        }
    }
}

impl HlsCodecProfile {
    pub(crate) fn from_requested(codec: Option<AudioCodec>) -> Result<Self, HlsError> {
        match codec.unwrap_or(AudioCodec::Aac) {
            AudioCodec::Aac => Ok(Self {
                codec: AudioCodec::Aac,
                ffmpeg_codec_str: AudioCodec::Aac.ffmpeg_encoder().expect("aac has encoder"),
                segment_type: "mpegts",
                segment_extension: "ts",
                init_filename: None,
            }),
            AudioCodec::Alac => Ok(Self {
                codec: AudioCodec::Alac,
                ffmpeg_codec_str: AudioCodec::Alac.ffmpeg_encoder().expect("alac has encoder"),
                segment_type: "fmp4",
                segment_extension: "m4s",
                init_filename: Some("init.mp4"),
            }),
            AudioCodec::Flac => Ok(Self {
                codec: AudioCodec::Flac,
                ffmpeg_codec_str: AudioCodec::Flac.ffmpeg_encoder().expect("flac has encoder"),
                segment_type: "fmp4",
                segment_extension: "m4s",
                init_filename: Some("init.mp4"),
            }),
            _ => Err(HlsError::UnsupportedCodec),
        }
    }

    pub(crate) fn from_requested_codecs(preferred_codecs: &[AudioCodec]) -> Result<Self, HlsError> {
        if preferred_codecs.is_empty() {
            return Self::from_requested(None);
        }

        for codec in preferred_codecs {
            if let Ok(profile) = Self::from_requested(Some(*codec)) {
                return Ok(profile);
            }
        }

        Err(HlsError::UnsupportedCodec)
    }
}

pub(crate) fn build_hls_output(
    playlist_path: &FsPath,
    segment_pattern: &FsPath,
    config: HlsOutputConfig,
) -> Output {
    let mut output = Output::new(playlist_path.to_string_lossy().into_owned())
        .set_format("hls")
        .set_audio_codec(config.profile.ffmpeg_codec_str)
        .set_format_opt("hls_time", HLS_SEGMENT_TIME_SECONDS.to_string())
        .set_format_opt("hls_playlist_type", "vod")
        .set_format_opt("hls_list_size", "0")
        .set_format_opt("start_number", HLS_START_NUMBER.to_string())
        .set_format_opt("hls_flags", "independent_segments+temp_file")
        .set_format_opt("hls_segment_type", config.profile.segment_type)
        .set_format_opt(
            "hls_segment_filename",
            segment_pattern.to_string_lossy().into_owned(),
        );

    if let Some(audio_bitrate_kbps) = config.audio_bitrate_kbps {
        output = output.set_audio_codec_opt("b", format!("{audio_bitrate_kbps}k"));
    }

    if let Some(sample_rate_hz) = config.sample_rate_hz {
        output = output.set_audio_sample_rate(sample_rate_hz as i32);
    }

    if let Some(channels) = config.channels {
        output = output.set_audio_channels(channels as i32);
    }

    if let Some(init_filename) = config.profile.init_filename {
        output = output.set_format_opt("hls_fmp4_init_filename", init_filename);
    }

    output
}

pub(crate) fn hls_media_content_type(segment_path: &FsPath) -> &'static str {
    match segment_path.extension().and_then(|ext| ext.to_str()) {
        Some(ext) if ext.eq_ignore_ascii_case("ts") => "video/mp2t",
        Some(ext) if ext.eq_ignore_ascii_case("m4s") => "video/iso.segment",
        Some(ext) if ext.eq_ignore_ascii_case("mp4") => "audio/mp4",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lyra_ffmpeg::AudioCodec;

    #[test]
    fn codec_profile_defaults_to_aac() {
        let profile = HlsCodecProfile::from_requested_codecs(&[]).expect("default codec profile");
        assert!(matches!(profile.codec, AudioCodec::Aac));
        assert_eq!(profile.segment_type, "mpegts");
        assert_eq!(profile.segment_extension, "ts");
        assert!(profile.init_filename.is_none());
    }

    #[test]
    fn codec_profile_supports_fmp4_codecs() {
        let alac =
            HlsCodecProfile::from_requested_codecs(&[AudioCodec::Alac]).expect("alac profile");
        assert_eq!(alac.segment_type, "fmp4");
        assert_eq!(alac.segment_extension, "m4s");
        assert_eq!(alac.init_filename, Some("init.mp4"));

        let flac =
            HlsCodecProfile::from_requested_codecs(&[AudioCodec::Flac]).expect("flac profile");
        assert_eq!(flac.segment_type, "fmp4");
        assert_eq!(flac.segment_extension, "m4s");
        assert_eq!(flac.init_filename, Some("init.mp4"));
    }

    #[test]
    fn codec_profile_rejects_unsupported_codecs() {
        assert!(HlsCodecProfile::from_requested_codecs(&[AudioCodec::Opus]).is_err());
        assert!(HlsCodecProfile::from_requested_codecs(&[AudioCodec::Copy]).is_err());
    }

    #[test]
    fn codec_profile_chooses_first_supported_codec_from_preferences() {
        let profile = HlsCodecProfile::from_requested_codecs(&[
            AudioCodec::Opus,
            AudioCodec::Aac,
            AudioCodec::Flac,
        ])
        .expect("first supported codec should be selected");
        assert!(matches!(profile.codec, AudioCodec::Aac));
    }

    #[test]
    fn hls_media_content_type_uses_segment_extension() {
        assert_eq!(
            hls_media_content_type(std::path::Path::new("segment-00001.ts")),
            "video/mp2t"
        );
        assert_eq!(
            hls_media_content_type(std::path::Path::new("segment-00001.m4s")),
            "video/iso.segment"
        );
        assert_eq!(
            hls_media_content_type(std::path::Path::new("init.mp4")),
            "audio/mp4"
        );
    }

    #[test]
    fn hls_output_uses_vod_playlists_for_stored_audio() {
        let profile =
            HlsCodecProfile::from_requested_codecs(&[AudioCodec::Aac]).expect("aac profile");
        let output = build_hls_output(
            std::path::Path::new("index.m3u8"),
            std::path::Path::new("segment-%05d.ts"),
            HlsOutputConfig::new(profile, Some(HLS_AUDIO_BITRATE_KBPS), None, None),
        );

        assert_eq!(
            output
                .get_format_opts()
                .get("hls_playlist_type")
                .map(String::as_str),
            Some("vod")
        );
        assert_eq!(
            output
                .get_format_opts()
                .get("start_number")
                .map(String::as_str),
            Some("0")
        );
        assert_eq!(
            output.get_audio_codec_opts().get("b").map(String::as_str),
            Some("192k")
        );
    }

    #[test]
    fn hls_output_applies_requested_bitrate_sample_rate_and_channels() {
        let profile =
            HlsCodecProfile::from_requested_codecs(&[AudioCodec::Aac]).expect("aac profile");
        let output = build_hls_output(
            std::path::Path::new("index.m3u8"),
            std::path::Path::new("segment-%05d.ts"),
            HlsOutputConfig::new(profile, Some(96), Some(44_100), Some(2)),
        );

        assert_eq!(
            output.get_audio_codec_opts().get("b").map(String::as_str),
            Some("96k")
        );
        assert_eq!(output.get_audio_sample_rate(), Some(44_100));
        assert_eq!(output.get_audio_channels(), Some(2));
    }
}
