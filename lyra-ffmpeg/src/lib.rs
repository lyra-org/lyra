// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

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
pub use output::Output;

pub use ffmpeg_sys_next::{
    AVERROR,
    AVERROR_EOF,
    AVSEEK_SIZE,
    AVSampleFormat,
    ENOSYS,
    ESPIPE,
};

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    const TEST_INPUT: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../lyra-server/tests/assets/metadata/integration_track.flac"
    );

    fn transcode_to_file(output_path: &str, format: &str, codec: &str, min_size: u64) {
        std::fs::remove_file(output_path).ok();

        let output = Output::new(output_path)
            .set_format(format)
            .set_audio_codec(codec)
            .set_audio_codec_opt("b", "192k");

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

    fn transcode_to_callback(format: &str, codec: &str, min_size: usize) {
        let (tx, rx) = mpsc::channel::<Vec<u8>>();

        let write_callback = move |buf: &[u8]| -> i32 {
            let len = buf.len() as i32;
            tx.send(buf.to_vec()).ok();
            len
        };

        let output = Output::with_callback(write_callback)
            .set_format(format)
            .set_audio_codec(codec)
            .set_audio_codec_opt("b", "192k");

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
    fn test_transcode_to_mp3_file() {
        transcode_to_file("/tmp/lyra-ffmpeg-test.mp3", "mp3", "libmp3lame", 20000);
    }

    #[test]
    fn test_transcode_to_mp3_callback() {
        transcode_to_callback("mp3", "libmp3lame", 20000);
    }

    #[test]
    fn test_transcode_to_flac_file() {
        transcode_to_file("/tmp/lyra-ffmpeg-test.flac", "flac", "flac", 10000);
    }

    #[test]
    fn test_transcode_to_flac_callback() {
        transcode_to_callback("flac", "flac", 10000);
    }

    #[test]
    fn test_transcode_to_wav_file() {
        transcode_to_file("/tmp/lyra-ffmpeg-test.wav", "wav", "pcm_s16le", 80000);
    }

    #[test]
    fn test_transcode_to_wav_callback() {
        transcode_to_callback("wav", "pcm_s16le", 80000);
    }

    #[test]
    fn test_transcode_to_ogg_file() {
        transcode_to_file("/tmp/lyra-ffmpeg-test.ogg", "ogg", "libvorbis", 10000);
    }

    #[test]
    fn test_transcode_to_ogg_callback() {
        transcode_to_callback("ogg", "libvorbis", 10000);
    }

    #[test]
    fn test_transcode_to_aac_file() {
        transcode_to_file("/tmp/lyra-ffmpeg-test.m4a", "ipod", "aac", 10000);
    }

    #[test]
    fn test_transcode_to_aac_callback() {
        let (tx, rx) = mpsc::channel::<Vec<u8>>();

        let write_callback = move |buf: &[u8]| -> i32 {
            let len = buf.len() as i32;
            tx.send(buf.to_vec()).ok();
            len
        };

        let output = Output::with_callback(write_callback)
            .set_format("adts")
            .set_audio_codec("aac")
            .set_audio_codec_opt("b", "192k");

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
    fn test_transcode_to_opus_file() {
        transcode_to_file("/tmp/lyra-ffmpeg-test.opus", "opus", "libopus", 5000);
    }

    #[test]
    fn test_transcode_to_opus_callback() {
        transcode_to_callback("opus", "libopus", 5000);
    }

    #[test]
    fn test_transcode_to_aiff_file() {
        transcode_to_file("/tmp/lyra-ffmpeg-test.aiff", "aiff", "pcm_s16be", 80000);
    }

    #[test]
    fn test_transcode_to_aiff_callback() {
        transcode_to_callback("aiff", "pcm_s16be", 80000);
    }

    #[test]
    fn test_transcode_to_alac_file() {
        transcode_to_file("/tmp/lyra-ffmpeg-test-alac.m4a", "ipod", "alac", 30000);
    }

    #[test]
    fn test_remux_to_caf_file() {
        let output_path = "/tmp/lyra-ffmpeg-test.caf";
        std::fs::remove_file(output_path).ok();

        let output = Output::new(output_path).set_format("caf");

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
}
