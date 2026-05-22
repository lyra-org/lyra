// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;
use std::path::Path;

use crate::audio::{
    AudioCodec,
    AudioFormat,
};
use ffmpeg_sys_next::AVSampleFormat;

pub(crate) type WriteCallback = Box<dyn FnMut(&[u8]) -> WriteResult + Send>;
pub(crate) type SeekCallback = Box<dyn FnMut(SeekRequest) -> SeekResult + Send>;

const CHROMAPRINT_SWR_OPTS: &[(&str, &str)] = &[
    ("filter_size", "16"),
    ("phase_shift", "8"),
    ("linear_interp", "1"),
    ("cutoff", "0.8"),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SampleFormat {
    S16,
}

impl SampleFormat {
    pub(crate) fn as_ffmpeg(self) -> AVSampleFormat {
        match self {
            Self::S16 => AVSampleFormat::AV_SAMPLE_FMT_S16,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HlsSegmentType {
    MpegTs,
    FragmentedMp4,
}

impl HlsSegmentType {
    pub(crate) fn as_ffmpeg(self) -> &'static str {
        match self {
            Self::MpegTs => "mpegts",
            Self::FragmentedMp4 => "fmp4",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteResult {
    Wrote(usize),
    Finished,
    Error(String),
}

impl WriteResult {
    pub fn error(message: impl Into<String>) -> Self {
        Self::Error(message.into())
    }
}

impl From<usize> for WriteResult {
    fn from(value: usize) -> Self {
        Self::Wrote(value)
    }
}

impl From<std::io::Result<usize>> for WriteResult {
    fn from(value: std::io::Result<usize>) -> Self {
        match value {
            Ok(bytes) => Self::Wrote(bytes),
            Err(err) => Self::Error(err.to_string()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeekRequest {
    Start(i64),
    Current(i64),
    End(i64),
    Size,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SeekResult {
    Position(i64),
    Unseekable,
    Unsupported,
    Error(String),
}

impl SeekResult {
    pub fn error(message: impl Into<String>) -> Self {
        Self::Error(message.into())
    }
}

impl From<i64> for SeekResult {
    fn from(value: i64) -> Self {
        Self::Position(value)
    }
}

impl From<std::io::Result<u64>> for SeekResult {
    fn from(value: std::io::Result<u64>) -> Self {
        match value {
            Ok(position) if position <= i64::MAX as u64 => Self::Position(position as i64),
            Ok(_) => Self::Error("seek position exceeds i64::MAX".to_string()),
            Err(err) => Self::Error(err.to_string()),
        }
    }
}

pub struct Output {
    pub(crate) url: Option<String>,
    pub(crate) write_callback: Option<WriteCallback>,
    pub(crate) seek_callback: Option<SeekCallback>,
    pub(crate) format: Option<String>,
    pub(crate) format_kind: Option<AudioFormat>,
    pub(crate) audio_codec: Option<String>,
    pub(crate) audio_codec_kind: Option<AudioCodec>,
    pub(crate) audio_codec_opts: HashMap<String, String>,
    pub(crate) audio_bitrate_kbps: Option<u32>,
    pub(crate) audio_abr: bool,
    pub(crate) audio_global_quality: Option<i32>,
    pub(crate) format_opts: HashMap<String, String>,
    pub(crate) swr_opts: HashMap<String, String>,
    pub(crate) audio_sample_rate: Option<i32>,
    pub(crate) audio_channels: Option<i32>,
    pub(crate) audio_sample_fmt: Option<SampleFormat>,
    pub(crate) is_streaming: bool,
}

impl Output {
    pub fn new(url: impl AsRef<Path>) -> Self {
        Self {
            url: Some(path_to_string(url)),
            write_callback: None,
            seek_callback: None,
            format: None,
            format_kind: None,
            audio_codec: None,
            audio_codec_kind: None,
            audio_codec_opts: HashMap::new(),
            audio_bitrate_kbps: None,
            audio_abr: false,
            audio_global_quality: None,
            format_opts: HashMap::new(),
            swr_opts: HashMap::new(),
            audio_sample_rate: None,
            audio_channels: None,
            audio_sample_fmt: None,
            is_streaming: false,
        }
    }

    pub fn with_callback<F, R>(mut write_callback: F) -> Self
    where
        F: FnMut(&[u8]) -> R + Send + 'static,
        R: Into<WriteResult> + 'static,
    {
        Self {
            url: None,
            write_callback: Some(Box::new(move |buf| write_callback(buf).into())),
            seek_callback: None,
            format: None,
            format_kind: None,
            audio_codec: None,
            audio_codec_kind: None,
            audio_codec_opts: HashMap::new(),
            audio_bitrate_kbps: None,
            audio_abr: false,
            audio_global_quality: None,
            format_opts: HashMap::new(),
            swr_opts: HashMap::new(),
            audio_sample_rate: None,
            audio_channels: None,
            audio_sample_fmt: None,
            is_streaming: false,
        }
    }

    pub fn set_seek_callback<F, R>(mut self, mut seek_callback: F) -> Self
    where
        F: FnMut(SeekRequest) -> R + Send + 'static,
        R: Into<SeekResult> + 'static,
    {
        self.seek_callback = Some(Box::new(move |request| seek_callback(request).into()));
        self
    }

    pub(crate) fn set_raw_format(mut self, format: impl Into<String>) -> Self {
        self.format = Some(format.into());
        self.format_kind = None;
        self
    }

    pub fn audio_global_quality(mut self, quality: i32) -> Self {
        self.audio_global_quality = Some(quality);
        self.audio_bitrate_kbps = None;
        self.audio_abr = false;
        self.audio_codec_opts.remove("abr");
        self.audio_codec_opts.remove("b");
        self
    }

    pub fn sample_rate_hz(mut self, sample_rate: u32) -> Self {
        self.audio_sample_rate = i32::try_from(sample_rate).ok();
        self
    }

    pub fn channels(mut self, channels: u32) -> Self {
        self.audio_channels = i32::try_from(channels).ok();
        self
    }

    pub(crate) fn set_audio_sample_format(mut self, sample_fmt: SampleFormat) -> Self {
        self.audio_sample_fmt = Some(sample_fmt);
        self
    }

    pub(crate) fn set_raw_format_opt(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.format_opts.insert(key.into(), value.into());
        self
    }

    pub(crate) fn set_raw_swr_opt(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.swr_opts.insert(key.into(), value.into());
        self
    }

    pub fn audio_format(mut self, format: AudioFormat) -> Self {
        let muxer = format.muxer(self.is_streaming);
        self.format = Some(muxer.to_string());
        self.format_kind = Some(format);

        if self.audio_codec.is_none() {
            let default_codec = format.default_codec();
            self.audio_codec_kind = Some(default_codec);
            if let Some(encoder) = default_codec.ffmpeg_encoder() {
                self.audio_codec = Some(encoder.to_string());
            }
        }
        self
    }

    pub fn codec(mut self, codec: AudioCodec) -> Self {
        if let Some(encoder) = codec.ffmpeg_encoder() {
            self.audio_codec = Some(encoder.to_string());
        } else {
            self.audio_codec = None;
        }
        self.audio_codec_kind = Some(codec);
        self
    }

    pub fn audio_bitrate_kbps(mut self, kbps: u32) -> Self {
        self.audio_bitrate_kbps = Some(kbps);
        self.audio_abr = false;
        self.audio_global_quality = None;
        self.audio_codec_opts.remove("abr");
        self.audio_codec_opts
            .insert("b".to_string(), format!("{}k", kbps));
        self
    }

    pub fn audio_abr_bitrate_kbps(mut self, kbps: u32) -> Self {
        self.audio_bitrate_kbps = Some(kbps);
        self.audio_abr = true;
        self.audio_global_quality = None;
        self.audio_codec_opts
            .insert("abr".to_string(), "1".to_string());
        self.audio_codec_opts
            .insert("b".to_string(), format!("{}k", kbps));
        self
    }

    pub fn streaming(mut self) -> Self {
        self.is_streaming = true;
        self
    }

    pub fn pcm_s16le_callback<F, R>(write_callback: F, sample_rate_hz: u32, channels: u32) -> Self
    where
        F: FnMut(&[u8]) -> R + Send + 'static,
        R: Into<WriteResult> + 'static,
    {
        CHROMAPRINT_SWR_OPTS.iter().fold(
            Self::with_callback(write_callback)
                .set_raw_format("s16le")
                .codec(AudioCodec::PcmS16Le)
                .sample_rate_hz(sample_rate_hz)
                .channels(channels)
                .set_audio_sample_format(SampleFormat::S16),
            |output, (key, value)| output.set_raw_swr_opt(*key, *value),
        )
    }

    pub fn hls_vod(
        playlist_path: impl AsRef<Path>,
        segment_pattern: impl AsRef<Path>,
        segment_type: HlsSegmentType,
        segment_time_seconds: u32,
        start_number: u32,
    ) -> Self {
        Self::new(playlist_path)
            .set_raw_format("hls")
            .set_raw_format_opt("hls_time", segment_time_seconds.to_string())
            .set_raw_format_opt("hls_playlist_type", "vod")
            .set_raw_format_opt("hls_list_size", "0")
            .set_raw_format_opt("start_number", start_number.to_string())
            .set_raw_format_opt("hls_flags", "independent_segments+temp_file")
            .set_raw_format_opt("hls_segment_type", segment_type.as_ffmpeg())
            .set_raw_format_opt("hls_segment_filename", path_to_string(segment_pattern))
    }

    pub fn hls_fmp4_init_filename(mut self, init_filename: impl Into<String>) -> Self {
        self.format_opts
            .insert("hls_fmp4_init_filename".to_string(), init_filename.into());
        self
    }

    pub fn configured_audio_bitrate_kbps(&self) -> Option<u32> {
        self.audio_bitrate_kbps
    }

    pub fn configured_audio_abr(&self) -> bool {
        self.audio_abr
    }

    pub fn configured_audio_global_quality(&self) -> Option<i32> {
        self.audio_global_quality
    }

    pub fn configured_sample_rate_hz(&self) -> Option<u32> {
        self.audio_sample_rate
            .and_then(|sample_rate| u32::try_from(sample_rate).ok())
    }

    pub fn configured_channels(&self) -> Option<u32> {
        self.audio_channels
            .and_then(|channels| u32::try_from(channels).ok())
    }

    pub(crate) fn validate(&self) -> crate::Result<()> {
        if self.url.is_none() && self.write_callback.is_none() {
            return Err(crate::Error::InvalidOutputSpec(
                "output must have a file URL or write callback".to_string(),
            ));
        }
        if self.url.is_some() && self.write_callback.is_some() {
            return Err(crate::Error::InvalidOutputSpec(
                "output cannot use both a file URL and write callback".to_string(),
            ));
        }
        if let Some(format) = self.format_kind {
            if self.is_streaming && !format.supports_streaming() {
                return Err(crate::Error::InvalidOutputSpec(format!(
                    "format '{}' does not support streaming",
                    format.extension()
                )));
            }
            if let Some(codec) = self.audio_codec_kind
                && !matches!(codec, AudioCodec::Copy)
                && !format.supports_codec(codec)
            {
                return Err(crate::Error::InvalidOutputSpec(format!(
                    "codec '{}' is not compatible with format '{}'",
                    codec.as_str(),
                    format.extension()
                )));
            }
        }
        if let Some(sample_rate) = self.audio_sample_rate
            && sample_rate <= 0
        {
            return Err(crate::Error::InvalidOutputSpec(
                "audio sample rate must be greater than zero".to_string(),
            ));
        }
        if let Some(channels) = self.audio_channels
            && channels <= 0
        {
            return Err(crate::Error::InvalidOutputSpec(
                "audio channel count must be greater than zero".to_string(),
            ));
        }
        validate_no_nul("format name", self.format.as_deref())?;
        validate_no_nul("output url", self.url.as_deref())?;
        validate_no_nul("codec name", self.audio_codec.as_deref())?;
        validate_option_map("codec option", &self.audio_codec_opts)?;
        validate_option_map("format option", &self.format_opts)?;
        validate_option_map("swr option", &self.swr_opts)?;
        Ok(())
    }
}

fn path_to_string(path: impl AsRef<Path>) -> String {
    path.as_ref().to_string_lossy().into_owned()
}

fn validate_no_nul(field: &'static str, value: Option<&str>) -> crate::Result<()> {
    if value.is_some_and(|value| value.contains('\0')) {
        return Err(crate::Error::InvalidCString { field });
    }
    Ok(())
}

fn validate_option_map(kind: &'static str, opts: &HashMap<String, String>) -> crate::Result<()> {
    for (key, value) in opts {
        if key.contains('\0') {
            return Err(crate::Error::InvalidCString {
                field: match kind {
                    "codec option" => "codec option key",
                    "format option" => "format option key",
                    "swr option" => "swr option key",
                    _ => "option key",
                },
            });
        }
        if value.contains('\0') {
            return Err(crate::Error::InvalidCString {
                field: match kind {
                    "codec option" => "codec option value",
                    "format option" => "format option value",
                    "swr option" => "swr option value",
                    _ => "option value",
                },
            });
        }
    }
    Ok(())
}
