// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

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

impl AudioFormat {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "mp3" => Some(Self::Mp3),
            "flac" => Some(Self::Flac),
            "wav" => Some(Self::Wav),
            "ogg" => Some(Self::Ogg),
            "webm" => Some(Self::Webm),
            "aac" => Some(Self::Aac),
            "m4a" => Some(Self::M4a),
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
            Self::Opus => "audio/opus",
            Self::Caf => "audio/x-caf",
        }
    }

    pub fn supports_streaming(&self) -> bool {
        !matches!(self, Self::M4a | Self::Alac | Self::Caf)
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

    pub fn native_sample_rate_hz(&self) -> Option<u32> {
        match self {
            Self::Opus => Some(48_000),
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
    };

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
    fn ogg_supports_flac() {
        assert!(AudioFormat::Ogg.supports_codec(AudioCodec::Flac));
    }
}
