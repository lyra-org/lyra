// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod fft;
mod fingerprint;

use std::{
    path::Path,
    sync::{
        Arc,
        Mutex,
    },
    time::Duration,
};

use base64::{
    Engine,
    engine::general_purpose::URL_SAFE_NO_PAD as BASE64,
};
use lyra_ffmpeg::{
    AVSampleFormat,
    FfmpegContext,
    Output,
};
use thiserror::Error;

pub use fingerprint::{
    BANDS_LEN,
    Chroma,
    FFT_FRAME_SIZE,
    FingerprintCalculator,
    HAMMING,
    SAMPLE_RATE,
    SAMPLE_RATE_STRING,
    WINDOW_SIZE,
    compress,
};

const DEFAULT_DURATION_SECS: u32 = 120;
const BYTES_PER_SAMPLE: usize = 2; // S16 = 16-bit
const CHANNELS: usize = 1;
const BYTES_PER_FRAME: usize = BYTES_PER_SAMPLE * CHANNELS;

#[derive(Error, Debug)]
pub enum Error {
    #[error("ffmpeg error: {0}")]
    Ffmpeg(#[from] lyra_ffmpeg::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Computes a compressed Chromaprint fingerprint for an audio file.
///
/// Returns `(fingerprint, duration_secs)` where `duration_secs` is the actual
/// duration of the full audio file in seconds (not capped by the fingerprint
/// duration limit). Minimum returned duration is 1.
pub fn compute_fingerprint_from_file(
    path: &Path,
    duration_secs: Option<u32>,
    timeout: Option<Duration>,
) -> Result<(String, u32), Error> {
    let (raw, total_duration) = compute_raw_fingerprint_from_file(path, duration_secs, timeout)?;
    let compressed = compress(&raw);
    Ok((BASE64.encode(&compressed), total_duration))
}

/// Computes a raw (uncompressed) Chromaprint fingerprint for an audio file.
///
/// Returns `(fingerprint, duration_secs)` where `duration_secs` is the actual
/// duration of the full audio file in seconds. Minimum returned duration is 1.
pub fn compute_raw_fingerprint_from_file(
    path: &Path,
    duration_secs: Option<u32>,
    timeout: Option<Duration>,
) -> Result<(Vec<u32>, u32), Error> {
    let duration = duration_secs.unwrap_or(DEFAULT_DURATION_SECS);
    let (pcm_bytes, total_bytes) = decode_pcm_bytes(path, duration, timeout)?;
    let samples = pcm_bytes_to_i16(&pcm_bytes);
    let total_duration = (total_bytes / (SAMPLE_RATE as usize * BYTES_PER_FRAME)).max(1) as u32;
    Ok((
        compute_fingerprint_from_samples(&samples, Some(duration)),
        total_duration,
    ))
}

pub fn compute_fingerprint_from_samples(samples: &[i16], duration_secs: Option<u32>) -> Vec<u32> {
    let duration = duration_secs.unwrap_or(DEFAULT_DURATION_SECS);
    let target_samples = duration as usize * SAMPLE_RATE as usize;
    let usable_samples = samples.len().min(target_samples);
    let samples = &samples[..usable_samples];

    let hop = WINDOW_SIZE / 3;
    let hamming = &*HAMMING;
    let mut fft_frame = [0.0f32; FFT_FRAME_SIZE];
    let mut fft_real = [0.0f32; WINDOW_SIZE];
    let mut fft_imag = [0.0f32; WINDOW_SIZE];
    let fft = &*fft::FFT;

    let mut chroma = Chroma::new();
    let mut calculator = FingerprintCalculator::new();

    let mut offset = 0;
    while offset + WINDOW_SIZE <= samples.len() {
        fft.power_spectrum(
            &samples[offset..offset + WINDOW_SIZE],
            hamming,
            &mut fft_real,
            &mut fft_imag,
            &mut fft_frame,
        );

        if let Some(features) = chroma.filter(&fft_frame) {
            calculator.add_features(features);
        }

        offset += hop;
    }

    calculator.into_fingerprint()
}

fn decode_pcm_bytes(
    path: &Path,
    duration_secs: u32,
    timeout: Option<Duration>,
) -> Result<(Vec<u8>, usize), Error> {
    let pcm_bytes: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let write_callback = {
        let pcm_bytes = Arc::clone(&pcm_bytes);
        move |buf: &[u8]| -> i32 {
            let mut locked = pcm_bytes.lock().unwrap();
            locked.extend_from_slice(buf);
            buf.len() as i32
        }
    };

    let output = Output::with_callback(write_callback)
        .set_format("s16le")
        .set_audio_codec("pcm_s16le")
        .set_audio_sample_rate(SAMPLE_RATE as i32)
        .set_audio_channels(CHANNELS as i32)
        .set_audio_sample_fmt(AVSampleFormat::AV_SAMPLE_FMT_S16)
        .set_swr_opt("filter_size", "16")
        .set_swr_opt("phase_shift", "8")
        .set_swr_opt("linear_interp", "1")
        .set_swr_opt("cutoff", "0.8");

    let context = FfmpegContext::builder()
        .input(path.to_string_lossy().into_owned())
        .output(output)
        .build()?;

    match timeout {
        Some(t) => context.start()?.wait_timeout(t)?,
        None => context.start()?.wait()?,
    }

    let bytes = pcm_bytes.lock().unwrap();
    let total_bytes = bytes.len();
    let max_bytes = duration_secs as usize * SAMPLE_RATE as usize * BYTES_PER_FRAME;
    let mut output = bytes.clone();
    if output.len() > max_bytes {
        output.truncate(max_bytes);
    }

    Ok((output, total_bytes))
}

fn pcm_bytes_to_i16(bytes: &[u8]) -> Vec<i16> {
    let mut samples = Vec::with_capacity(bytes.len() / 2);
    for chunk in bytes.chunks_exact(2) {
        samples.push(i16::from_le_bytes([chunk[0], chunk[1]]));
    }
    samples
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprints_deterministic_samples() {
        let samples = synthetic_samples(30);
        let fingerprint = compute_fingerprint_from_samples(&samples, Some(30));

        assert_eq!(fingerprint.len(), 221);
        assert_eq!(fingerprint_checksum(&fingerprint), 0xb03797b3012bf8aa);
    }

    fn synthetic_samples(duration_secs: u32) -> Vec<i16> {
        let len = SAMPLE_RATE as usize * duration_secs as usize;
        (0..len)
            .map(|i| {
                let a = (i as f32 * 0.011).sin() * 16_000.0;
                let b = (i as f32 * 0.037).sin() * 8_000.0;
                (a + b).clamp(i16::MIN as f32, i16::MAX as f32) as i16
            })
            .collect()
    }

    fn fingerprint_checksum(fingerprint: &[u32]) -> u64 {
        fingerprint
            .iter()
            .fold(0xcbf29ce484222325u64, |hash, value| {
                (hash ^ *value as u64).wrapping_mul(0x100000001b3)
            })
    }
}
