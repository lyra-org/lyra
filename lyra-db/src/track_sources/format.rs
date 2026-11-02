// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! What a track source's audio file is, as read from its contents when it was scanned.

use agdb::{
    DbError,
    DbTypeMarker,
    DbValue,
};

macro_rules! source_format_enum {
    ($(#[$meta:meta])* $name:ident { $($variant:ident => $value:literal,)+ }) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, DbTypeMarker)]
        pub enum $name {
            $($variant,)+
        }

        impl $name {
            /// Every value, as stored and as plugins see it.
            pub const VALUES: &[&str] = &[$($value,)+];

            pub fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value,)+
                }
            }

            pub fn parse(value: &str) -> Option<Self> {
                match value {
                    $($value => Some(Self::$variant),)+
                    _ => None,
                }
            }

            fn from_db_str(value: &str) -> Result<Self, DbError> {
                Self::parse(value).ok_or_else(|| {
                    DbError::serialization(
                        agdb::DbErrorType::TypeError,
                        format!(concat!("invalid ", stringify!($name), " value '{}'"), value),
                    )
                })
            }
        }

        impl From<$name> for DbValue {
            fn from(value: $name) -> Self {
                Self::from(value.as_str())
            }
        }

        impl From<&$name> for DbValue {
            fn from(value: &$name) -> Self {
                (*value).into()
            }
        }

        impl TryFrom<DbValue> for $name {
            type Error = DbError;

            fn try_from(value: DbValue) -> Result<Self, Self::Error> {
                Self::from_db_str(value.string()?)
            }
        }
    };
}

source_format_enum! {
    /// The file format holding the audio.
    SourceContainer {
        Aac => "aac",
        Aiff => "aiff",
        Ape => "ape",
        Flac => "flac",
        M4a => "m4a",
        Mp3 => "mp3",
        Mpc => "mpc",
        Ogg => "ogg",
        Wav => "wav",
        WavPack => "wavpack",
    }
}

source_format_enum! {
    /// The encoding of the audio inside its container, named as FFmpeg names codecs.
    SourceCodec {
        Aac => "aac",
        Alac => "alac",
        Ape => "ape",
        Flac => "flac",
        Mp1 => "mp1",
        Mp2 => "mp2",
        Mp3 => "mp3",
        Musepack7 => "musepack7",
        Musepack8 => "musepack8",
        Opus => "opus",
        PcmF32Le => "pcm_f32le",
        PcmF64Le => "pcm_f64le",
        PcmS8 => "pcm_s8",
        PcmS16Be => "pcm_s16be",
        PcmS16Le => "pcm_s16le",
        PcmS24Be => "pcm_s24be",
        PcmS24Le => "pcm_s24le",
        PcmS32Be => "pcm_s32be",
        PcmS32Le => "pcm_s32le",
        PcmU8 => "pcm_u8",
        Speex => "speex",
        Vorbis => "vorbis",
        WavPack => "wavpack",
    }
}

/// A track source's container, codec and audio properties, each absent when the scan couldn't
/// tell. A CUE segment shares its file's.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SourceFormat {
    pub container: Option<SourceContainer>,
    pub codec: Option<SourceCodec>,
    pub sample_rate_hz: Option<u32>,
    pub channel_count: Option<u32>,
    pub bit_depth: Option<u32>,
    pub bitrate_bps: Option<u32>,
}
