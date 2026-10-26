// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#![deny(unsafe_op_in_unsafe_fn)]

mod audio;
mod context;
mod error;
mod output;

pub use audio::{
    AudioCodec,
    AudioFormat,
    AudioVbrMode,
    SUPPORTED_CODECS,
    SUPPORTED_FORMATS,
};
pub use context::{
    FfmpegContext,
    FfmpegHandle,
};
pub use error::{
    Error,
    Result,
};
pub use output::{
    HlsSegmentType,
    Output,
    SeekRequest,
    SeekResult,
    WriteResult,
};

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_INPUT: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../lyra-server/tests/assets/metadata/integration_track.flac"
    );

    #[test]
    fn rejects_incompatible_typed_format_and_codec() {
        let output = Output::new("/tmp/lyra-ffmpeg-invalid.mp3")
            .audio_format(AudioFormat::Mp3)
            .codec(AudioCodec::Flac);

        let error = match FfmpegContext::builder()
            .input(TEST_INPUT)
            .output(output)
            .build()
        {
            Ok(_) => panic!("incompatible typed output should fail before starting ffmpeg"),
            Err(error) => error,
        };

        assert!(matches!(error, Error::InvalidOutputSpec(_)));
    }

    #[test]
    fn rejects_invalid_sample_rate_before_start() {
        let output = Output::new("/tmp/lyra-ffmpeg-invalid.mp3")
            .audio_format(AudioFormat::Mp3)
            .sample_rate_hz(0);

        let error = match FfmpegContext::builder()
            .input(TEST_INPUT)
            .output(output)
            .build()
        {
            Ok(_) => panic!("invalid sample rate should fail before starting ffmpeg"),
            Err(error) => error,
        };

        assert!(matches!(error, Error::InvalidOutputSpec(_)));
    }

    #[test]
    fn hls_vod_owns_raw_muxer_options_internally() {
        let output = Output::hls_vod(
            "/tmp/index.m3u8",
            "/tmp/segment-%05d.ts",
            HlsSegmentType::MpegTs,
            6,
            0,
        );

        assert_eq!(output.format.as_deref(), Some("hls"));
        assert_eq!(
            output
                .format_opts
                .get("hls_playlist_type")
                .map(String::as_str),
            Some("vod")
        );
        assert_eq!(
            output.format_opts.get("start_number").map(String::as_str),
            Some("0")
        );
        assert_eq!(
            output
                .format_opts
                .get("hls_segment_type")
                .map(String::as_str),
            Some("mpegts")
        );
        assert_eq!(
            output
                .format_opts
                .get("hls_segment_filename")
                .map(String::as_str),
            Some("/tmp/segment-%05d.ts")
        );
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires FFmpeg C APIs")]
    fn streams_m4a_as_fragmented_mp4_to_an_unseekable_sink() {
        let written = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
        let sink = std::sync::Arc::clone(&written);
        for codec in [AudioCodec::Aac, AudioCodec::Alac] {
            sink.lock().expect("sink lock").clear();
            let sink = std::sync::Arc::clone(&sink);
            let output = Output::with_callback(move |buf: &[u8]| {
                sink.lock().expect("sink lock").extend_from_slice(buf);
                buf.len()
            })
            .streaming()
            .set_seek_callback(|request| match request {
                SeekRequest::Size => SeekResult::Unsupported,
                _ => SeekResult::Unseekable,
            })
            .audio_format(AudioFormat::M4a)
            .codec(codec);

            FfmpegContext::builder()
                .input(TEST_INPUT)
                .output(output)
                .build()
                .expect("streamed m4a output should build")
                .start()
                .expect("transcode should start")
                .wait()
                .expect("streamed m4a should finish without seeking");

            let bytes = written.lock().expect("sink lock").clone();
            let position = |atom: &[u8]| bytes.windows(atom.len()).position(|w| w == atom);
            let moov = position(b"moov").expect("an mp4 header");
            let moof = position(b"moof").expect("a fragment");
            assert!(
                moov < moof,
                "{codec:?}: the header must precede the samples"
            );
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires FFmpeg C APIs")]
    fn reports_callback_error() {
        let output = Output::with_callback(|_| WriteResult::error("sink failed"))
            .audio_format(AudioFormat::Mp3)
            .codec(AudioCodec::Mp3)
            .audio_bitrate_kbps(192);

        let context = FfmpegContext::builder()
            .input(TEST_INPUT)
            .output(output)
            .build()
            .expect("callback output should build");

        let error = context
            .start()
            .expect("transcode should start")
            .wait()
            .expect_err("callback error should stop transcode");

        assert!(matches!(error, Error::OutputCallback(message) if message == "sink failed"));
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires FFmpeg C APIs")]
    fn reports_callback_panic() {
        let output = Output::with_callback(|_| -> WriteResult {
            panic!("sink panic");
        })
        .audio_format(AudioFormat::Mp3)
        .codec(AudioCodec::Mp3)
        .audio_bitrate_kbps(192);

        let context = FfmpegContext::builder()
            .input(TEST_INPUT)
            .output(output)
            .build()
            .expect("callback output should build");

        let error = context
            .start()
            .expect("transcode should start")
            .wait()
            .expect_err("callback panic should stop transcode");

        assert!(matches!(error, Error::OutputCallbackPanic));
    }
}
