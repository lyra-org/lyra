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
    SUPPORTED_SAMPLE_RATES_HZ,
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
    fn rejects_an_unsupported_sample_rate_before_start() {
        let (min, max) = SUPPORTED_SAMPLE_RATES_HZ.into_inner();
        for rate in [0, 1, min - 1, max + 1, u32::MAX] {
            let output = Output::new("/tmp/lyra-ffmpeg-invalid.wav")
                .audio_format(AudioFormat::Wav)
                .sample_rate_hz(rate);

            let error = match FfmpegContext::builder()
                .input(TEST_INPUT)
                .output(output)
                .build()
            {
                Ok(_) => panic!("{rate} Hz should fail before starting ffmpeg"),
                Err(error) => error,
            };

            assert!(matches!(error, Error::InvalidOutputSpec(_)), "{rate} Hz");
        }
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

    /// A half-second WAV of `bits`-bit samples, written under the temp dir. Each channel carries a
    /// sine of its own frequency, advancing `0.05 * (channel + 1)` radians a sample.
    fn pcm_wav(name: &str, bits: u16, channels: u16, rate: u32) -> std::path::PathBuf {
        let bytes_per_sample = bits / 8;
        let block_align = channels * bytes_per_sample;
        let frames = rate / 2;
        let mut data = Vec::with_capacity(frames as usize * block_align as usize);
        for frame in 0..frames {
            for channel in 0..channels {
                let phase = frame as f64 * 0.05 * f64::from(channel + 1);
                let sample = (phase.sin() * f64::from(1u32 << (bits - 2))) as i32;
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
        let source = pcm_wav("flac-depth", 24, 2, 44_100);
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
        let wav = pcm_wav("alac-depth", 24, 2, 44_100);
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

    const ASSETS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/assets");

    fn asset(name: &str) -> std::path::PathBuf {
        std::path::Path::new(ASSETS).join(name)
    }

    fn transcode(input: &std::path::Path, output: Output, end_ms: Option<u64>) -> Result<()> {
        FfmpegContext::builder()
            .input(input.to_string_lossy())
            .output(output)
            .end_ms(end_ms)
            .build()
            .expect("the output builds")
            .start()
            .expect("the transcode starts")
            .wait()
    }

    /// `input` decoded to 16-bit PCM at `rate` and `channels`, as fingerprinting reads it.
    fn decode_pcm(
        input: &std::path::Path,
        rate: u32,
        channels: u32,
        end_ms: Option<u64>,
    ) -> Result<Vec<u8>> {
        let written = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
        let sink = std::sync::Arc::clone(&written);
        let output = Output::pcm_s16le_callback(
            move |buf: &[u8]| {
                sink.lock().expect("sink lock").extend_from_slice(buf);
                buf.len()
            },
            rate,
            channels,
        );
        transcode(input, output, end_ms)?;
        Ok(std::mem::take(&mut *written.lock().expect("sink lock")))
    }

    fn wav_channels(path: &std::path::Path) -> u16 {
        let wav = std::fs::read(path).expect("the wav is read");
        u16::from_le_bytes([wav[22], wav[23]])
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires FFmpeg C APIs")]
    fn transcodes_planar_audio_with_more_than_eight_channels() {
        let wav = pcm_wav("ten-channels", 16, 10, 48_000);
        let vorbis = wav.with_extension("ogg");
        encode(
            &wav,
            Output::new(&vorbis)
                .audio_format(AudioFormat::Ogg)
                .codec(AudioCodec::Vorbis),
        );
        let pcm = decode_pcm(&vorbis, 48_000, 10, None).expect("the vorbis decodes");
        let samples: Vec<i16> = pcm
            .as_chunks::<2>()
            .0
            .iter()
            .map(|&sample| i16::from_le_bytes(sample))
            .collect();
        for channel in 0..10 {
            let crossings = samples
                .chunks_exact(10)
                .map(|frame| frame[channel] >= 0)
                .collect::<Vec<_>>()
                .windows(2)
                .filter(|pair| pair[0] != pair[1])
                .count() as f64;
            let expected = 24_000.0 * 0.05 * (channel + 1) as f64 / std::f64::consts::PI;
            assert!(
                (crossings / expected - 1.0).abs() < 0.03,
                "channel {channel} crossed zero {crossings} times, not about {expected}"
            );
        }

        let malformed = wav.with_extension("malformed.wav");
        let result = transcode(
            &asset("35-channels.aac"),
            Output::new(&malformed).audio_format(AudioFormat::Wav),
            None,
        );
        assert!(
            matches!(result, Err(Error::FfmpegOperation { .. })),
            "a corrupt 35-channel stream fails after its first frame: {result:?}"
        );

        for path in [wav, vorbis, malformed] {
            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires FFmpeg C APIs")]
    fn follows_channel_layout_and_sample_rate_changes_mid_stream() {
        let parts = [("joined-stereo", 2, 48_000), ("joined-mono", 1, 22_050)];
        let mut joined = Vec::new();
        for (name, channels, rate) in parts {
            let wav = pcm_wav(name, 16, channels, rate);
            let mp3 = wav.with_extension("mp3");
            encode(
                &wav,
                Output::new(&mp3)
                    .audio_format(AudioFormat::Mp3)
                    .codec(AudioCodec::Mp3)
                    .audio_bitrate_kbps(32)
                    .set_raw_format_opt("id3v2_version", "0")
                    .set_raw_format_opt("write_xing", "0"),
            );
            joined.extend(std::fs::read(&mp3).expect("the mp3 is read"));
            let _ = std::fs::remove_file(wav);
            let _ = std::fs::remove_file(mp3);
        }
        let path =
            std::env::temp_dir().join(format!("lyra-ffmpeg-joined-{}.mp3", std::process::id()));
        std::fs::write(&path, joined).expect("the joined mp3 is written");

        let pcm = decode_pcm(&path, 44_100, 2, None).expect("the joined mp3 decodes");
        let _ = std::fs::remove_file(path);
        let seconds = pcm.len() as f64 / 4.0 / 44_100.0;
        assert!(
            (0.95..1.15).contains(&seconds),
            "two half-second parts decoded to {seconds} s"
        );

        decode_pcm(&asset("layout-change.aac"), 11_025, 1, None)
            .expect("a stream changing its channel count decodes");
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires FFmpeg C APIs")]
    fn transcodes_a_source_with_a_custom_channel_order() {
        let target = std::env::temp_dir().join(format!(
            "lyra-ffmpeg-custom-order-{}.wav",
            std::process::id()
        ));
        encode(
            &asset("custom-channel-order.mov"),
            Output::new(&target).audio_format(AudioFormat::Wav),
        );
        assert_eq!(wav_channels(&target), 2);
        let _ = std::fs::remove_file(target);
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires FFmpeg C APIs")]
    fn encodes_a_wav_without_a_channel_layout_to_mp3() {
        let wav = pcm_wav("unspecified-layout", 16, 2, 44_100);
        let mp3 = wav.with_extension("mp3");
        encode(
            &wav,
            Output::new(&mp3)
                .audio_format(AudioFormat::Mp3)
                .codec(AudioCodec::Mp3),
        );
        let decoded = decode_pcm(&mp3, 44_100, 2, None).expect("the mp3 decodes");
        assert!(!decoded.is_empty());
        let _ = std::fs::remove_file(wav);
        let _ = std::fs::remove_file(mp3);
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires FFmpeg C APIs")]
    fn keeps_the_resampler_tail_at_the_end_of_the_input() {
        let wav = pcm_wav("resampled-tail", 16, 2, 48_000);
        let whole = decode_pcm(&wav, 44_100, 2, None).expect("the wav decodes");
        let cut = decode_pcm(&wav, 44_100, 2, Some(250)).expect("the wav decodes");
        let _ = std::fs::remove_file(wav);
        assert_eq!(whole.len() / 4, 22_050);
        assert_eq!(cut.len() / 4, 11_025);
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires FFmpeg C APIs")]
    fn refuses_input_below_the_minimum_sample_rate() {
        let target =
            std::env::temp_dir().join(format!("lyra-ffmpeg-1hz-{}.flac", std::process::id()));
        let error = transcode(
            &asset("1hz.aac"),
            Output::new(&target)
                .audio_format(AudioFormat::Flac)
                .codec(AudioCodec::Flac)
                .sample_rate_hz(96_000),
            None,
        )
        .expect_err("a 1 Hz input is refused");
        let _ = std::fs::remove_file(target);
        assert!(
            matches!(error, Error::UnsupportedInputSampleRate(1)),
            "{error}"
        );
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires FFmpeg C APIs")]
    fn refuses_input_above_the_maximum_sample_rate() {
        let wav = pcm_wav("1mhz", 16, 2, 1_000_000);
        let target = wav.with_extension("flac");
        let error = transcode(
            &wav,
            Output::new(&target)
                .audio_format(AudioFormat::Flac)
                .sample_rate_hz(1_000),
            None,
        )
        .expect_err("a 1 MHz input is refused");
        let _ = std::fs::remove_file(wav);
        let _ = std::fs::remove_file(target);
        assert!(
            matches!(error, Error::UnsupportedInputSampleRate(1_000_000)),
            "{error}"
        );
    }

    #[test]
    #[cfg_attr(miri, ignore = "requires FFmpeg C APIs")]
    fn refuses_input_without_a_known_sample_rate() {
        let target = std::env::temp_dir().join(format!(
            "lyra-ffmpeg-unknown-rate-{}.wav",
            std::process::id()
        ));
        let error = transcode(
            &asset("unknown-rate.aac"),
            Output::new(&target).audio_format(AudioFormat::Wav),
            None,
        )
        .expect_err("an input without a known rate is refused");
        let _ = std::fs::remove_file(target);
        assert!(
            matches!(error, Error::UnsupportedInputSampleRate(0)),
            "{error}"
        );
    }
}
