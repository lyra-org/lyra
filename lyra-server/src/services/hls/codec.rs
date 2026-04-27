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

#[derive(Clone, Copy)]
pub(crate) struct HlsCodecProfile {
    pub(crate) codec: AudioCodec,
    pub(crate) ffmpeg_codec_str: &'static str,
    pub(crate) segment_type: &'static str,
    pub(crate) segment_extension: &'static str,
    pub(crate) init_filename: Option<&'static str>,
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
}

pub(crate) fn build_hls_output(
    playlist_path: &FsPath,
    segment_pattern: &FsPath,
    profile: HlsCodecProfile,
) -> Output {
    let mut output = Output::new(playlist_path.to_string_lossy().into_owned())
        .set_format("hls")
        .set_audio_codec(profile.ffmpeg_codec_str)
        .set_format_opt("hls_time", HLS_SEGMENT_TIME_SECONDS.to_string())
        .set_format_opt("hls_playlist_type", "vod")
        .set_format_opt("hls_list_size", "0")
        .set_format_opt("start_number", HLS_START_NUMBER.to_string())
        .set_format_opt("hls_flags", "independent_segments+temp_file")
        .set_format_opt("hls_segment_type", profile.segment_type)
        .set_format_opt(
            "hls_segment_filename",
            segment_pattern.to_string_lossy().into_owned(),
        );

    if matches!(profile.codec, AudioCodec::Aac) {
        output = output.set_audio_codec_opt("b", format!("{HLS_AUDIO_BITRATE_KBPS}k"));
    }

    if let Some(init_filename) = profile.init_filename {
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
        let profile = HlsCodecProfile::from_requested(None).expect("default codec profile");
        assert!(matches!(profile.codec, AudioCodec::Aac));
        assert_eq!(profile.segment_type, "mpegts");
        assert_eq!(profile.segment_extension, "ts");
        assert!(profile.init_filename.is_none());
    }

    #[test]
    fn codec_profile_supports_fmp4_codecs() {
        let alac = HlsCodecProfile::from_requested(Some(AudioCodec::Alac)).expect("alac profile");
        assert_eq!(alac.segment_type, "fmp4");
        assert_eq!(alac.segment_extension, "m4s");
        assert_eq!(alac.init_filename, Some("init.mp4"));

        let flac = HlsCodecProfile::from_requested(Some(AudioCodec::Flac)).expect("flac profile");
        assert_eq!(flac.segment_type, "fmp4");
        assert_eq!(flac.segment_extension, "m4s");
        assert_eq!(flac.init_filename, Some("init.mp4"));
    }

    #[test]
    fn codec_profile_rejects_unsupported_codecs() {
        assert!(HlsCodecProfile::from_requested(Some(AudioCodec::Opus)).is_err());
        assert!(HlsCodecProfile::from_requested(Some(AudioCodec::Copy)).is_err());
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
        let profile = HlsCodecProfile::from_requested(Some(AudioCodec::Aac)).expect("aac profile");
        let output = build_hls_output(
            std::path::Path::new("index.m3u8"),
            std::path::Path::new("segment-%05d.ts"),
            profile,
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
    }
}
