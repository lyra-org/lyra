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

    /// A half-second stereo 44.1 kHz WAV of `bits`-bit samples, written under the temp dir.
    fn pcm_wav(name: &str, bits: u16) -> std::path::PathBuf {
        let (channels, rate) = (2u16, 44_100u32);
        let bytes_per_sample = bits / 8;
        let block_align = channels * bytes_per_sample;
        let frames = rate / 2;
        let mut data = Vec::with_capacity(frames as usize * block_align as usize);
        for frame in 0..frames {
            let sample = ((frame as f64 * 0.05).sin() * f64::from(1u32 << (bits - 2))) as i32;
            for _ in 0..channels {
                data.extend_from_slice(&sample.to_le_bytes()[..bytes_per_sample as usize]);
            }
        }
        let mut wav = Vec::new();
        wav.extend_from_slice(b"RIFF");
        wav.extend_from_slice(&(36 + data.len() as u32).to_le_bytes());
        wav.extend_from_slice(b"WAVEfmt ");
        wav.extend_from_slice(&16u32.to_le_bytes());
        wav.extend_from_slice(&1u16.to_le_bytes());
        wav.extend_from_slice(&channels.to_le_bytes());
        wav.extend_from_slice(&rate.to_le_bytes());
        wav.extend_from_slice(&(rate * u32::from(block_align)).to_le_bytes());
        wav.extend_from_slice(&block_align.to_le_bytes());
        wav.extend_from_slice(&bits.to_le_bytes());
        wav.extend_from_slice(b"data");
        wav.extend_from_slice(&(data.len() as u32).to_le_bytes());
        wav.extend_from_slice(&data);
        let path =
            std::env::temp_dir().join(format!("lyra-ffmpeg-{name}-{}.wav", std::process::id()));
        std::fs::write(&path, wav).expect("the wav is written");
        path
    }

    fn encode(input: &std::path::Path, output: Output) {
        FfmpegContext::builder()
            .input(input.to_string_lossy())
            .output(output)
            .build()
            .expect("the output builds")
            .start()
            .expect("the transcode starts")
            .wait()
            .expect("the transcode finishes");
    }

    /// The bits per sample a FLAC file's STREAMINFO records.
    fn flac_bits_per_sample(path: &std::path::Path) -> u8 {
        let flac = std::fs::read(path).expect("the flac is read");
        assert_eq!(&flac[..4], b"fLaC");
        let streaminfo = &flac[8..42];
        (((streaminfo[12] & 0x01) << 4) | (streaminfo[13] >> 4)) + 1
    }

    /// The bit depth an MP4's ALAC magic cookie records.
    fn alac_bit_depth(bytes: &[u8]) -> u8 {
        bytes
            .windows(14)
            .find(|window| {
                &window[..8] == b"alac\0\0\0\0"
                    && u32::from_be_bytes([window[8], window[9], window[10], window[11]]) == 4096
            })
            .map(|cookie| cookie[13])
            .expect("an ALAC magic cookie")
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires FFmpeg C APIs")]
    fn flac_keeps_the_source_bit_depth_unless_capped() {
        let source = pcm_wav("flac-depth", 24);
        for (bit_depth, expected) in [(None, 24), (Some(16), 16), (Some(24), 24)] {
            let target = source.with_extension(format!("{bit_depth:?}.flac"));
            let mut output = Output::new(&target)
                .audio_format(AudioFormat::Flac)
                .codec(AudioCodec::Flac);
            if let Some(bits) = bit_depth {
                output = output.bit_depth(bits);
            }
            encode(&source, output);
            assert_eq!(
                flac_bits_per_sample(&target),
                expected,
                "bit_depth {bit_depth:?}"
            );
            let _ = std::fs::remove_file(target);
        }
        let _ = std::fs::remove_file(source);
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires FFmpeg C APIs")]
    fn alac_keeps_a_16_bit_source_at_16_bits() {
        let wav = pcm_wav("alac-depth", 24);
        let deep_flac = wav.with_extension("flac");
        encode(
            &wav,
            Output::new(&deep_flac)
                .audio_format(AudioFormat::Flac)
                .codec(AudioCodec::Flac),
        );
        for (source, expected) in [
            (std::path::PathBuf::from(TEST_INPUT), 16),
            (deep_flac.clone(), 24),
        ] {
            let target = wav.with_extension(format!("{expected}.m4a"));
            encode(
                &source,
                Output::new(&target)
                    .audio_format(AudioFormat::M4a)
                    .codec(AudioCodec::Alac),
            );
            let m4a = std::fs::read(&target).expect("the m4a is read");
            assert_eq!(alac_bit_depth(&m4a), expected, "{}", source.display());
            let _ = std::fs::remove_file(target);
        }
        let _ = std::fs::remove_file(deep_flac);
        let _ = std::fs::remove_file(wav);
    }

    /// The sample rate of the first MPEG audio frame in `mp3`, past any ID3v2 tag.
    fn mp3_sample_rate(mp3: &[u8]) -> u32 {
        let start = if mp3.starts_with(b"ID3") {
            let size = mp3[6..10]
                .iter()
                .fold(0usize, |size, byte| (size << 7) | usize::from(byte & 0x7f));
            10 + size
        } else {
            0
        };
        let header = mp3[start..]
            .windows(4)
            .find(|window| window[0] == 0xff && window[1] & 0xe0 == 0xe0)
            .expect("an MPEG audio frame");
        let base = [44_100, 48_000, 32_000][usize::from((header[2] >> 2) & 0x03)];
        match (header[1] >> 3) & 0x03 {
            0b11 => base,
            0b10 => base / 2,
            0b00 => base / 4,
            version => panic!("reserved MPEG version {version}"),
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires FFmpeg C APIs")]
    fn mp3_stays_at_or_below_a_sample_rate_cap() {
        for (cap, expected) in [
            (None, 44_100),
            (Some(23_000), 22_050),
            (Some(32_000), 32_000),
        ] {
            let target = std::env::temp_dir().join(format!(
                "lyra-ffmpeg-mp3-rate-{cap:?}-{}.mp3",
                std::process::id()
            ));
            let mut output = Output::new(&target)
                .audio_format(AudioFormat::Mp3)
                .codec(AudioCodec::Mp3)
                .audio_bitrate_kbps(64);
            if let Some(cap) = cap {
                output = output.sample_rate_hz(cap);
            }
            encode(std::path::Path::new(TEST_INPUT), output);
            let mp3 = std::fs::read(&target).expect("the mp3 is read");
            assert_eq!(mp3_sample_rate(&mp3), expected, "cap {cap:?}");
            let _ = std::fs::remove_file(target);
        }
    }
}
