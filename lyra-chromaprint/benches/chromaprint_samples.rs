#![feature(test)]

extern crate test;

use std::sync::{
    Arc,
    Mutex,
    OnceLock,
};

use lyra_chromaprint::{
    SAMPLE_RATE,
    compute_fingerprint_from_samples,
};
use lyra_ffmpeg::{
    AVSampleFormat,
    FfmpegContext,
    Output,
};
use test::{
    Bencher,
    black_box,
};

const BYTES_PER_SAMPLE: usize = 2;
const TRACKS: &[&str] = &[
    "/home/blue/Library/01_Road of Resistance.flac",
    "/home/blue/Library/Da-iCE - SCENE (2023-05-24) [WEB FLAC]/1.02. Funky Jumping.flac",
    "/home/blue/Library/Hiroyuki Sawano - Attack on Titan S2 OST [FLAC] (PCCG1615)/Disc 1/01 - Barricades.flac",
    "/home/blue/Library/MYTH & ROID - eYe's/03 - Paradisus-Paradoxum.flac",
    "/home/blue/Library/Persona 4 Dancing All Night/disc 1/01 Dance!.flac",
];

struct TrackSamples {
    path: &'static str,
    samples: Vec<i16>,
}

static TRACK_SAMPLES: OnceLock<Vec<TrackSamples>> = OnceLock::new();

fn track_samples() -> &'static [TrackSamples] {
    TRACK_SAMPLES
        .get_or_init(|| {
            TRACKS
                .iter()
                .map(|&path| TrackSamples {
                    path,
                    samples: decode_samples(path),
                })
                .collect()
        })
        .as_slice()
}

fn decode_samples(path: &str) -> Vec<i16> {
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
        .set_audio_channels(1)
        .set_audio_sample_fmt(AVSampleFormat::AV_SAMPLE_FMT_S16)
        .set_swr_opt("filter_size", "16")
        .set_swr_opt("phase_shift", "8")
        .set_swr_opt("linear_interp", "1")
        .set_swr_opt("cutoff", "0.8");

    let context = FfmpegContext::builder()
        .input(path.to_owned())
        .output(output)
        .build()
        .unwrap();

    context.start().unwrap().wait().unwrap();

    let bytes = pcm_bytes.lock().unwrap();
    bytes
        .chunks_exact(BYTES_PER_SAMPLE)
        .map(|chunk| i16::from_le_bytes([chunk[0], chunk[1]]))
        .collect()
}

#[bench]
fn fingerprint_library_tracks(bench: &mut Bencher) {
    let tracks = track_samples();

    bench.iter(|| {
        for track in tracks {
            let fingerprint =
                compute_fingerprint_from_samples(black_box(track.samples.as_slice()), Some(120));
            black_box((track.path, fingerprint));
        }
    });
}
