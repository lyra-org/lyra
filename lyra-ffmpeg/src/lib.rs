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
    use std::sync::mpsc;

    const TEST_INPUT: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../lyra-server/tests/assets/metadata/integration_track.flac"
    );

    fn transcode_to_file(output_path: &str, format: AudioFormat, codec: AudioCodec, min_size: u64) {
        std::fs::remove_file(output_path).ok();

        let output = Output::new(output_path)
            .audio_format(format)
            .codec(codec)
            .audio_bitrate_kbps(192);

        let context = FfmpegContext::builder()
            .input(TEST_INPUT)
            .output(output)
            .build()
            .expect("Failed to build context");

        let handle = context.start().expect("Failed to start");
        handle.wait().expect("Failed to wait");

        assert!(
            std::path::Path::new(output_path).exists(),
            "Output file should exist"
        );
        let metadata = std::fs::metadata(output_path).expect("Failed to get metadata");
        assert!(
            metadata.len() > min_size,
            "Output file should be at least {} bytes, got {}",
            min_size,
            metadata.len()
        );

        std::fs::remove_file(output_path).ok();
    }

    fn transcode_to_callback(format: AudioFormat, codec: AudioCodec, min_size: usize) {
        let (tx, rx) = mpsc::channel::<Vec<u8>>();

        let write_callback = move |buf: &[u8]| -> usize {
            let len = buf.len();
            tx.send(buf.to_vec()).ok();
            len
        };

        let output = Output::with_callback(write_callback)
            .audio_format(format)
            .codec(codec)
            .audio_bitrate_kbps(192);

        let context = FfmpegContext::builder()
            .input(TEST_INPUT)
            .output(output)
            .build()
            .expect("Failed to build context");

        let handle = context.start().expect("Failed to start");
        handle.wait().expect("Failed to wait");

        let mut total_bytes = 0;
        while let Ok(chunk) = rx.try_recv() {
            total_bytes += chunk.len();
        }

        assert!(
            total_bytes > min_size,
            "Should have received at least {} bytes, got {}",
            min_size,
            total_bytes
        );
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires FFmpeg C APIs")]
    fn test_transcode_to_mp3_file() {
        transcode_to_file(
            "/tmp/lyra-ffmpeg-test.mp3",
            AudioFormat::Mp3,
            AudioCodec::Mp3,
            20000,
        );
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires FFmpeg C APIs")]
    fn test_transcode_to_mp3_callback() {
        transcode_to_callback(AudioFormat::Mp3, AudioCodec::Mp3, 20000);
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires FFmpeg C APIs")]
    fn test_transcode_to_flac_file() {
        transcode_to_file(
            "/tmp/lyra-ffmpeg-test.flac",
            AudioFormat::Flac,
            AudioCodec::Flac,
            10000,
        );
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires FFmpeg C APIs")]
    fn test_transcode_to_flac_callback() {
        transcode_to_callback(AudioFormat::Flac, AudioCodec::Flac, 10000);
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires FFmpeg C APIs")]
    fn test_transcode_to_wav_file() {
        transcode_to_file(
            "/tmp/lyra-ffmpeg-test.wav",
            AudioFormat::Wav,
            AudioCodec::PcmS16Le,
            80000,
        );
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires FFmpeg C APIs")]
    fn test_transcode_to_wav_callback() {
        transcode_to_callback(AudioFormat::Wav, AudioCodec::PcmS16Le, 80000);
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires FFmpeg C APIs")]
    fn test_transcode_to_ogg_file() {
        transcode_to_file(
            "/tmp/lyra-ffmpeg-test.ogg",
            AudioFormat::Ogg,
            AudioCodec::Vorbis,
            10000,
        );
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires FFmpeg C APIs")]
    fn test_transcode_to_ogg_callback() {
        transcode_to_callback(AudioFormat::Ogg, AudioCodec::Vorbis, 10000);
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires FFmpeg C APIs")]
    fn test_transcode_to_aac_file() {
        transcode_to_file(
            "/tmp/lyra-ffmpeg-test.m4a",
            AudioFormat::M4a,
            AudioCodec::Aac,
            10000,
        );
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires FFmpeg C APIs")]
    fn test_transcode_to_aac_callback() {
        let (tx, rx) = mpsc::channel::<Vec<u8>>();

        let write_callback = move |buf: &[u8]| -> usize {
            let len = buf.len();
            tx.send(buf.to_vec()).ok();
            len
        };

        let output = Output::with_callback(write_callback)
            .streaming()
            .audio_format(AudioFormat::Aac)
            .codec(AudioCodec::Aac)
            .audio_bitrate_kbps(192);

        let context = FfmpegContext::builder()
            .input(TEST_INPUT)
            .output(output)
            .build()
            .expect("Failed to build context");

        let handle = context.start().expect("Failed to start");
        handle.wait().expect("Failed to wait");

        let mut total_bytes = 0;
        while let Ok(chunk) = rx.try_recv() {
            total_bytes += chunk.len();
        }

        assert!(
            total_bytes > 10000,
            "Should have received at least 10000 bytes, got {}",
            total_bytes
        );
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires FFmpeg C APIs")]
    fn test_transcode_to_opus_file() {
        transcode_to_file(
            "/tmp/lyra-ffmpeg-test.opus",
            AudioFormat::Opus,
            AudioCodec::Opus,
            5000,
        );
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires FFmpeg C APIs")]
    fn test_transcode_to_opus_callback() {
        transcode_to_callback(AudioFormat::Opus, AudioCodec::Opus, 5000);
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires FFmpeg C APIs")]
    fn test_transcode_to_aiff_file() {
        transcode_to_file(
            "/tmp/lyra-ffmpeg-test.aiff",
            AudioFormat::Aiff,
            AudioCodec::PcmS16Be,
            80000,
        );
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires FFmpeg C APIs")]
    fn test_transcode_to_aiff_callback() {
        transcode_to_callback(AudioFormat::Aiff, AudioCodec::PcmS16Be, 80000);
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires FFmpeg C APIs")]
    fn test_transcode_to_alac_file() {
        transcode_to_file(
            "/tmp/lyra-ffmpeg-test-alac.m4a",
            AudioFormat::Alac,
            AudioCodec::Alac,
            30000,
        );
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires FFmpeg C APIs")]
    fn test_remux_to_caf_file() {
        let output_path = "/tmp/lyra-ffmpeg-test.caf";
        std::fs::remove_file(output_path).ok();

        let output = Output::new(output_path).audio_format(AudioFormat::Caf);

        let context = FfmpegContext::builder()
            .input(TEST_INPUT)
            .output(output)
            .build()
            .expect("Failed to build context");

        let handle = context.start().expect("Failed to start");
        handle.wait().expect("Failed to wait");

        assert!(
            std::path::Path::new(output_path).exists(),
            "Output file should exist"
        );
        let metadata = std::fs::metadata(output_path).expect("Failed to get metadata");
        assert!(
            metadata.len() > 10000,
            "Output file should be at least 10KB, got {}",
            metadata.len()
        );

        std::fs::remove_file(output_path).ok();
    }

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
