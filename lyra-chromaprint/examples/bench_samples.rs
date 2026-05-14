// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    hint::black_box,
    time::Instant,
};

fn main() {
    let iterations = std::env::args()
        .nth(1)
        .and_then(|arg| arg.parse::<usize>().ok())
        .unwrap_or(20);
    let seconds = 120usize;
    let sample_count = lyra_chromaprint::SAMPLE_RATE as usize * seconds;
    let samples = (0..sample_count)
        .map(|i| {
            let a = ((i * 31) % 65536) as i32 - 32768;
            let b = ((i * 131) % 32768) as i32 - 16384;
            ((a / 2) + (b / 2)) as i16
        })
        .collect::<Vec<_>>();

    let warmup = lyra_chromaprint::compute_fingerprint_from_samples(black_box(&samples), Some(120));
    black_box(warmup);

    let started = Instant::now();
    let mut total_len = 0usize;
    let mut checksum = 0u32;
    for _ in 0..iterations {
        let fingerprint =
            lyra_chromaprint::compute_fingerprint_from_samples(black_box(&samples), Some(120));
        total_len += fingerprint.len();
        checksum ^= fingerprint.iter().fold(0, |acc, value| acc ^ value);
        black_box(&fingerprint);
    }
    let elapsed = started.elapsed();
    println!(
        "iterations={iterations} elapsed_ms={} avg_ms={:.3} total_len={total_len} checksum={checksum}",
        elapsed.as_millis(),
        elapsed.as_secs_f64() * 1000.0 / iterations as f64,
    );
}
