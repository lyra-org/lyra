// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::ffi::CStr;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to open input: {0}")]
    OpenInput(String),
    #[error("failed to find stream info")]
    FindStreamInfo,
    #[error("failed to allocate output context")]
    AllocOutputContext,
    #[error("failed to open output: {0}")]
    OpenOutput(String),
    #[error("failed to allocate codec context: {0}")]
    AllocCodecContext(String),
    #[error("failed to find encoder: {0}")]
    FindEncoder(String),
    #[error("failed to open encoder: {0}")]
    OpenEncoder(String),
    #[error("failed to allocate stream")]
    AllocStream,
    #[error("failed to copy codec parameters")]
    CopyCodecParams,
    #[error("failed to write header")]
    WriteHeader,
    #[error("failed to write trailer")]
    WriteTrailer,
    #[error("failed to write frame")]
    WriteFrame,
    #[error("failed to allocate avio context")]
    AllocAvio,
    #[error("invalid {field}: contains NUL")]
    InvalidCString { field: &'static str },
    #[error("ffmpeg error: {0}")]
    Ffmpeg(i32),
    #[error("no audio stream found")]
    NoAudioStream,
    #[error("no streams to process")]
    NoStreams,
    #[error("thread join error")]
    ThreadJoin,
    #[error("operation timed out")]
    Timeout,
}

pub type Result<T> = std::result::Result<T, Error>;

pub fn av_error_string(err: i32) -> String {
    let mut buf = [0i8; 256];
    unsafe {
        ffmpeg_sys_next::av_strerror(err, buf.as_mut_ptr(), buf.len());
        CStr::from_ptr(buf.as_ptr()).to_string_lossy().into_owned()
    }
}
