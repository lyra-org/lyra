// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

use crate::audio::{
    AudioCodec,
    AudioFormat,
};
use ffmpeg_sys_next::AVSampleFormat;

pub type WriteCallback = Box<dyn FnMut(&[u8]) -> i32 + Send>;
pub type SeekCallback = Box<dyn FnMut(i64, i32) -> i64 + Send>;

pub struct Output {
    pub(crate) url: Option<String>,
    pub(crate) write_callback: Option<WriteCallback>,
    pub(crate) seek_callback: Option<SeekCallback>,
    pub(crate) format: Option<String>,
    pub(crate) audio_codec: Option<String>,
    pub(crate) audio_codec_opts: HashMap<String, String>,
    pub(crate) format_opts: HashMap<String, String>,
    pub(crate) swr_opts: HashMap<String, String>,
    pub(crate) audio_sample_rate: Option<i32>,
    pub(crate) audio_channels: Option<i32>,
    pub(crate) audio_sample_fmt: Option<AVSampleFormat>,
    pub(crate) is_streaming: bool,
}

impl Output {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: Some(url.into()),
            write_callback: None,
            seek_callback: None,
            format: None,
            audio_codec: None,
            audio_codec_opts: HashMap::new(),
            format_opts: HashMap::new(),
            swr_opts: HashMap::new(),
            audio_sample_rate: None,
            audio_channels: None,
            audio_sample_fmt: None,
            is_streaming: false,
        }
    }

    pub fn with_callback<F>(write_callback: F) -> Self
    where
        F: FnMut(&[u8]) -> i32 + Send + 'static,
    {
        Self {
            url: None,
            write_callback: Some(Box::new(write_callback)),
            seek_callback: None,
            format: None,
            audio_codec: None,
            audio_codec_opts: HashMap::new(),
            format_opts: HashMap::new(),
            swr_opts: HashMap::new(),
            audio_sample_rate: None,
            audio_channels: None,
            audio_sample_fmt: None,
            is_streaming: false,
        }
    }

    pub fn set_seek_callback<F>(mut self, seek_callback: F) -> Self
    where
        F: FnMut(i64, i32) -> i64 + Send + 'static,
    {
        self.seek_callback = Some(Box::new(seek_callback));
        self
    }

    pub fn set_format(mut self, format: impl Into<String>) -> Self {
        self.format = Some(format.into());
        self
    }

    pub fn set_audio_codec(mut self, codec: impl Into<String>) -> Self {
        self.audio_codec = Some(codec.into());
        self
    }

    pub fn set_audio_codec_opt(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.audio_codec_opts.insert(key.into(), value.into());
        self
    }

    pub fn set_audio_sample_rate(mut self, sample_rate: i32) -> Self {
        self.audio_sample_rate = Some(sample_rate);
        self
    }

    pub fn set_audio_channels(mut self, channels: i32) -> Self {
        self.audio_channels = Some(channels);
        self
    }

    pub fn set_audio_sample_fmt(mut self, sample_fmt: AVSampleFormat) -> Self {
        self.audio_sample_fmt = Some(sample_fmt);
        self
    }

    pub fn set_format_opt(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.format_opts.insert(key.into(), value.into());
        self
    }

    pub fn set_swr_opt(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.swr_opts.insert(key.into(), value.into());
        self
    }

    pub fn audio_format(mut self, format: AudioFormat) -> Self {
        let muxer = format.muxer(self.is_streaming);
        self.format = Some(muxer.to_string());

        if self.audio_codec.is_none() {
            let default_codec = format.default_codec();
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
        self
    }

    pub fn bitrate(mut self, kbps: u32) -> Self {
        self.audio_codec_opts
            .insert("b".to_string(), format!("{}k", kbps));
        self
    }

    pub fn streaming(mut self) -> Self {
        self.is_streaming = true;
        self
    }

    pub fn get_audio_codec_opts(&self) -> &HashMap<String, String> {
        &self.audio_codec_opts
    }

    pub fn get_format_opts(&self) -> &HashMap<String, String> {
        &self.format_opts
    }

    pub fn get_audio_sample_rate(&self) -> Option<i32> {
        self.audio_sample_rate
    }

    pub fn get_audio_channels(&self) -> Option<i32> {
        self.audio_channels
    }
}
