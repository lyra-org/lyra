// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

/// Writes MP4 as a header with no samples followed by one-second fragments, so a client can
/// play it while it is still being encoded.
/// A placeholder Xing header reads as a zero-frame file until the encode finishes and rewrites it.
const STREAMED_MP3_MUXER_OPTS: &[(&str, &str)] = &[("write_xing", "0")];
const FRAGMENTED_MP4_MUXER_OPTS: &[(&str, &str)] = &[
    ("movflags", "empty_moov+delay_moov+default_base_moof"),
    ("frag_duration", "1000000"),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AudioFormat {
    Mp3,
    Flac,
    Wav,
    Ogg,
    Webm,
    Aac,
    M4a,
    Opus,
    Aiff,
    Alac,
    Caf,
    Wma,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AudioCodec {
    Copy,
    Mp3,
    Flac,
    Aac,
    Alac,
    Opus,
    Vorbis,
    PcmS16Le,
    PcmS16Be,
    PcmS24Le,
    PcmS24Be,
    Wma,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AudioVbrMode {
    Quality(i32),
    Abr,
}

impl AudioFormat {
    /// The name [`AudioFormat::parse`] accepts for this format.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Mp3 => "mp3",
            Self::Flac => "flac",
            Self::Wav => "wav",
            Self::Ogg => "ogg",
            Self::Webm => "webm",
            Self::Aac => "aac",
            Self::M4a => "m4a",
            Self::Opus => "opus",
            Self::Aiff => "aiff",
            Self::Alac => "alac",
            Self::Caf => "caf",
            Self::Wma => "wma",
        }
    }

    /// Also reads the other names of MP4, Ogg and WebM audio files as those formats.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "mp3" => Some(Self::Mp3),
            "flac" => Some(Self::Flac),
            "wav" => Some(Self::Wav),
            "ogg" | "oga" => Some(Self::Ogg),
            "webm" | "webma" => Some(Self::Webm),
            "aac" => Some(Self::Aac),
            "m4a" | "mp4" | "m4b" | "m4p" | "m4r" => Some(Self::M4a),
            "opus" => Some(Self::Opus),
            "aiff" => Some(Self::Aiff),
            "alac" => Some(Self::Alac),
            "caf" => Some(Self::Caf),
            "wma" => Some(Self::Wma),
            _ => None,
        }
    }

    pub(crate) fn muxer(&self, streaming: bool) -> &'static str {
        match self {
            Self::Mp3 => "mp3",
            Self::Flac => "flac",
            Self::Wav => "wav",
            Self::Ogg | Self::Opus => "ogg",
            Self::Webm => "webm",
            Self::M4a | Self::Alac => "ipod",
            Self::Aac => {
                if streaming {
                    "adts"
                } else {
                    "ipod"
                }
            }
            Self::Wma => "asf",
            Self::Aiff => "aiff",
            Self::Caf => "caf",
        }
    }

    pub fn default_codec(&self) -> AudioCodec {
        match self {
            Self::Mp3 => AudioCodec::Mp3,
            Self::Flac => AudioCodec::Flac,
            Self::Wav => AudioCodec::PcmS16Le,
            Self::Ogg => AudioCodec::Vorbis,
            Self::Webm => AudioCodec::Opus,
            Self::Aac | Self::M4a => AudioCodec::Aac,
            Self::Opus => AudioCodec::Opus,
            Self::Aiff => AudioCodec::PcmS16Be,
            Self::Alac => AudioCodec::Alac,
            Self::Caf => AudioCodec::Copy,
            Self::Wma => AudioCodec::Wma,
        }
    }

    pub fn mime_type(&self, streaming: bool) -> &'static str {
        match self {
            Self::Mp3 => "audio/mpeg",
            Self::Flac => "audio/flac",
            Self::Wav => "audio/wav",
            Self::Ogg => "audio/ogg",
            Self::Webm => "audio/webm",
            Self::M4a | Self::Alac => "audio/mp4",
            Self::Aac => {
                if streaming {
                    "audio/aac"
                } else {
                    "audio/mp4"
                }
            }
            Self::Wma => "audio/x-ms-wma",
            Self::Aiff => "audio/aiff",
            Self::Opus => "audio/ogg",
            Self::Caf => "audio/x-caf",
        }
    }

    pub fn supports_streaming(&self) -> bool {
        !matches!(self, Self::Caf)
    }

    /// Muxer options a streamed output needs because it is read before the muxer can seek back to
    /// finish its header.
    pub(crate) fn streaming_muxer_opts(&self) -> &'static [(&'static str, &'static str)] {
        match self {
            Self::Mp3 => STREAMED_MP3_MUXER_OPTS,
            Self::M4a | Self::Alac => FRAGMENTED_MP4_MUXER_OPTS,
            _ => &[],
        }
    }

    pub fn extension(&self) -> &'static str {
        match self {
            Self::Mp3 => "mp3",
            Self::Flac => "flac",
            Self::Wav => "wav",
            Self::Ogg => "ogg",
            Self::Webm => "webm",
            Self::Aac | Self::M4a | Self::Alac => "m4a",
            Self::Opus => "opus",
            Self::Aiff => "aiff",
            Self::Caf => "caf",
            Self::Wma => "wma",
        }
    }

    pub fn compatible_codecs(&self) -> &'static [AudioCodec] {
        match self {
            Self::Mp3 => &[AudioCodec::Mp3],
            Self::Flac => &[AudioCodec::Flac],
            Self::Wav => &[AudioCodec::PcmS16Le, AudioCodec::PcmS24Le],
            Self::Ogg => &[AudioCodec::Vorbis, AudioCodec::Opus, AudioCodec::Flac],
            Self::Webm => &[AudioCodec::Opus, AudioCodec::Vorbis],
            Self::Aac => &[AudioCodec::Aac],
            Self::M4a => &[AudioCodec::Aac, AudioCodec::Alac],
            Self::Opus => &[AudioCodec::Opus],
            Self::Aiff => &[AudioCodec::PcmS16Be, AudioCodec::PcmS24Be],
            Self::Alac => &[AudioCodec::Alac],
            Self::Caf => &[AudioCodec::Copy],
            Self::Wma => &[AudioCodec::Wma],
        }
    }

    pub fn supports_codec(&self, codec: AudioCodec) -> bool {
        self.compatible_codecs().contains(&codec)
    }
}

impl AudioCodec {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "copy" => Some(Self::Copy),
            "mp3" => Some(Self::Mp3),
            "flac" => Some(Self::Flac),
            "aac" => Some(Self::Aac),
            "alac" => Some(Self::Alac),
            "opus" => Some(Self::Opus),
            "vorbis" => Some(Self::Vorbis),
            "pcm_s16le" => Some(Self::PcmS16Le),
            "pcm_s16be" => Some(Self::PcmS16Be),
            "pcm_s24le" => Some(Self::PcmS24Le),
            "pcm_s24be" => Some(Self::PcmS24Be),
            "wma" => Some(Self::Wma),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Copy => "copy",
            Self::Mp3 => "mp3",
            Self::Flac => "flac",
            Self::Aac => "aac",
            Self::Alac => "alac",
            Self::Opus => "opus",
            Self::Vorbis => "vorbis",
            Self::PcmS16Le => "pcm_s16le",
            Self::PcmS16Be => "pcm_s16be",
            Self::PcmS24Le => "pcm_s24le",
            Self::PcmS24Be => "pcm_s24be",
            Self::Wma => "wma",
        }
    }

    pub fn ffmpeg_encoder(&self) -> Option<&'static str> {
        match self {
            Self::Copy => None,
            Self::Mp3 => Some("libmp3lame"),
            Self::Flac => Some("flac"),
            Self::Aac => Some("aac"),
            Self::Alac => Some("alac"),
            Self::Opus => Some("libopus"),
            Self::Vorbis => Some("libvorbis"),
            Self::PcmS16Le => Some("pcm_s16le"),
            Self::PcmS16Be => Some("pcm_s16be"),
            Self::PcmS24Le => Some("pcm_s24le"),
            Self::PcmS24Be => Some("pcm_s24be"),
            Self::Wma => Some("wmav2"),
        }
    }

    pub fn preferred_format(&self) -> Option<AudioFormat> {
        match self {
            Self::Copy => None,
            Self::Mp3 => Some(AudioFormat::Mp3),
            Self::Flac => Some(AudioFormat::Flac),
            Self::Aac | Self::Alac => Some(AudioFormat::M4a),
            Self::Opus => Some(AudioFormat::Opus),
            Self::Vorbis => Some(AudioFormat::Ogg),
            Self::PcmS16Le => Some(AudioFormat::Wav),
            Self::PcmS16Be => Some(AudioFormat::Aiff),
            Self::PcmS24Le => Some(AudioFormat::Wav),
            Self::PcmS24Be => Some(AudioFormat::Aiff),
            Self::Wma => Some(AudioFormat::Wma),
        }
    }

    pub fn is_lossless(&self) -> bool {
        matches!(
            self,
            Self::Flac
                | Self::Alac
                | Self::PcmS16Le
                | Self::PcmS16Be
                | Self::PcmS24Le
                | Self::PcmS24Be
        )
    }

    /// The bit depth this codec stores `source_bits`-deep audio at: FLAC and ALAC keep 16 bits
    /// up to 16 and 24 above, PCM codecs have their own, and lossy codecs and copy have none.
    pub fn encoded_bit_depth(&self, source_bits: u32) -> Option<u32> {
        match self {
            Self::Flac | Self::Alac => Some(if source_bits <= 16 { 16 } else { 24 }),
            Self::PcmS16Le | Self::PcmS16Be => Some(16),
            Self::PcmS24Le | Self::PcmS24Be => Some(24),
            Self::Copy | Self::Mp3 | Self::Aac | Self::Opus | Self::Vorbis | Self::Wma => None,
        }
    }

    /// This codec storing samples of at most `bits`: a 24-bit PCM codec becomes its 16-bit
    /// counterpart below 24 bits; every other codec stays as it is.
    pub fn capped_to_bit_depth(self, bits: u32) -> Self {
        match self {
            Self::PcmS24Le if bits < 24 => Self::PcmS16Le,
            Self::PcmS24Be if bits < 24 => Self::PcmS16Be,
            codec => codec,
        }
    }

    pub fn min_bitrate_bps(&self) -> Option<u32> {
        match self {
            Self::Mp3 => Some(32_000),
            Self::Aac => Some(32_000),
            Self::Opus => Some(6_000),
            Self::Vorbis => Some(45_000),
            Self::Wma => Some(32_000),
            Self::Flac
            | Self::Alac
            | Self::PcmS16Le
            | Self::PcmS16Be
            | Self::PcmS24Le
            | Self::PcmS24Be
            | Self::Copy => None,
        }
    }

    /// Returns a hard encoder bitrate ceiling when the selected encoder has one.
    pub fn max_bitrate_bps(&self, channels: u32) -> Option<u32> {
        match self {
            Self::Opus => Some(256_000_u32.saturating_mul(channels.max(1))),
            Self::Mp3
            | Self::Aac
            | Self::Vorbis
            | Self::Wma
            | Self::Flac
            | Self::Alac
            | Self::PcmS16Le
            | Self::PcmS16Be
            | Self::PcmS24Le
            | Self::PcmS24Be
            | Self::Copy => None,
        }
    }

    pub fn native_sample_rate_hz(&self) -> Option<u32> {
        match self {
            Self::Opus => Some(48_000),
            _ => None,
        }
    }

    pub fn vbr_mode(&self, bitrate_bps: u32, channels: u32) -> Option<AudioVbrMode> {
        let bitrate_per_channel = bitrate_bps / channels.max(1);

        match self {
            Self::Aac => Some(AudioVbrMode::Quality(match bitrate_per_channel {
                0..=31_999 => 1,
                32_000..=47_999 => 2,
                48_000..=63_999 => 3,
                64_000..=95_999 => 4,
                _ => 5,
            })),
            Self::Mp3 => {
                if (48_001..122_500).contains(&bitrate_per_channel) {
                    Some(AudioVbrMode::Quality(match bitrate_per_channel {
                        0..=63_999 => 6,
                        64_000..=87_999 => 4,
                        88_000..=111_999 => 2,
                        _ => 0,
                    }))
                } else {
                    Some(AudioVbrMode::Abr)
                }
            }
            Self::Vorbis => Some(AudioVbrMode::Quality(match bitrate_per_channel {
                0..=39_999 => 0,
                40_000..=55_999 => 2,
                56_000..=79_999 => 4,
                80_000..=111_999 => 6,
                _ => 8,
            })),
            _ => None,
        }
    }
}

pub const SUPPORTED_FORMATS: &[&str] = &[
    "mp3", "flac", "wav", "ogg", "webm", "aac", "m4a", "opus", "aiff", "alac", "caf", "wma",
];

pub const SUPPORTED_CODECS: &[&str] = &[
    "copy",
    "mp3",
    "flac",
    "aac",
    "alac",
    "opus",
    "vorbis",
    "pcm_s16le",
    "pcm_s16be",
    "pcm_s24le",
    "pcm_s24be",
    "wma",
];

#[cfg(test)]
mod tests {
    use super::{
        AudioCodec,
        AudioFormat,
        AudioVbrMode,
    };

    #[test]
    fn format_names_parse_back() {
        for name in super::SUPPORTED_FORMATS {
            let format = AudioFormat::parse(name).expect("supported formats parse");
            assert_eq!(format.as_str(), *name);
        }
    }

    #[test]
    fn container_aliases_parse_as_their_format() {
        for (name, format) in [
            ("mp4", AudioFormat::M4a),
            ("M4B", AudioFormat::M4a),
            ("m4p", AudioFormat::M4a),
            ("m4r", AudioFormat::M4a),
            ("oga", AudioFormat::Ogg),
            ("webma", AudioFormat::Webm),
        ] {
            assert_eq!(AudioFormat::parse(name), Some(format), "{name}");
        }
    }

    #[test]
    fn webm_supports_opus_and_vorbis() {
        assert!(AudioFormat::Webm.supports_codec(AudioCodec::Opus));
        assert!(AudioFormat::Webm.supports_codec(AudioCodec::Vorbis));
        assert!(!AudioFormat::Webm.supports_codec(AudioCodec::Flac));
        assert_eq!(AudioFormat::Webm.mime_type(true), "audio/webm");
    }

    #[test]
    fn wav_and_aiff_support_24_bit_pcm() {
        assert!(AudioFormat::Wav.supports_codec(AudioCodec::PcmS24Le));
        assert!(AudioFormat::Aiff.supports_codec(AudioCodec::PcmS24Be));
        assert_eq!(
            AudioCodec::PcmS24Le.preferred_format(),
            Some(AudioFormat::Wav)
        );
        assert_eq!(
            AudioCodec::PcmS24Be.preferred_format(),
            Some(AudioFormat::Aiff)
        );
    }

    #[test]
    fn opus_maximum_bitrate_scales_with_channel_count() {
        assert_eq!(AudioCodec::Opus.max_bitrate_bps(0), Some(256_000));
        assert_eq!(AudioCodec::Opus.max_bitrate_bps(1), Some(256_000));
        assert_eq!(AudioCodec::Opus.max_bitrate_bps(2), Some(512_000));
        assert_eq!(AudioCodec::Mp3.max_bitrate_bps(2), None);
    }

    #[test]
    fn ogg_supports_flac() {
        assert!(AudioFormat::Ogg.supports_codec(AudioCodec::Flac));
    }

    #[test]
    fn vbr_mode_uses_codec_specific_mapping() {
        assert_eq!(
            AudioCodec::Aac.vbr_mode(128_000, 2),
            Some(AudioVbrMode::Quality(4))
        );
        assert_eq!(
            AudioCodec::Mp3.vbr_mode(320_000, 2),
            Some(AudioVbrMode::Abr)
        );
        assert_eq!(
            AudioCodec::Vorbis.vbr_mode(160_000, 2),
            Some(AudioVbrMode::Quality(6))
        );
        assert_eq!(AudioCodec::Flac.vbr_mode(160_000, 2), None);
    }
}
