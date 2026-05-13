// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#![allow(dead_code)]

use std::f64::consts::PI;
use std::sync::LazyLock;

pub type Float = f32;
pub const SAMPLE_RATE: u32 = 11025;
pub const SAMPLE_RATE_STRING: &str = "11025";
pub const WINDOW_SIZE: usize = 4096;
pub const FFT_FRAME_SIZE: usize = 1 + WINDOW_SIZE / 2;
pub const BANDS_LEN: usize = 12;

const BUFFER_ROWS: usize = 8;
const COEFFICIENTS: [Float; 5] = [0.25, 0.75, 1.0, 0.75, 0.25];
const MIN_FREQ: f64 = 28.0;
const MAX_FREQ: f64 = 3520.0;

pub static HAMMING: LazyLock<[Float; WINDOW_SIZE]> = LazyLock::new(|| {
    let mut result = [0.0; WINDOW_SIZE];
    let denom = (WINDOW_SIZE - 1) as f64;
    for (i, elem) in result.iter_mut().enumerate() {
        let value = 0.54 - 0.46 * (2.0 * PI * (i as f64) / denom).cos();
        *elem = value as Float;
    }
    result
});

struct ChromaTable {
    min_index: usize,
    max_index: usize,
    notes: [u8; WINDOW_SIZE],
}

static CHROMA_TABLE: LazyLock<ChromaTable> = LazyLock::new(|| {
    let min_index = freq_to_index(MIN_FREQ).max(1);
    let max_index = (WINDOW_SIZE / 2).min(freq_to_index(MAX_FREQ));
    let mut notes = [0u8; WINDOW_SIZE];

    for (i, note_slot) in notes.iter_mut().enumerate().take(max_index).skip(min_index) {
        let freq = index_to_freq(i as f64);
        let octave = freq_to_octave(freq);
        let note = (BANDS_LEN as f64) * (octave - octave.floor());
        *note_slot = note.floor() as u8;
    }

    ChromaTable {
        min_index,
        max_index,
        notes,
    }
});

pub struct Chroma {
    buffer_len: usize,
    buffer_i: usize,
    buffer: [[Float; BANDS_LEN]; BUFFER_ROWS],
    result_buffer: [Float; BANDS_LEN],
}

impl Chroma {
    pub fn new() -> Self {
        Self {
            buffer_len: 1,
            buffer_i: 0,
            buffer: [[0.0; BANDS_LEN]; BUFFER_ROWS],
            result_buffer: [0.0; BANDS_LEN],
        }
    }

    pub fn filter(&mut self, fft_frame: &[Float; FFT_FRAME_SIZE]) -> Option<&[Float; BANDS_LEN]> {
        let table = &*CHROMA_TABLE;
        {
            let buf = &mut self.buffer[self.buffer_i];
            buf.fill(0.0);
            for (&note, &energy) in table.notes[table.min_index..table.max_index]
                .iter()
                .zip(fft_frame[table.min_index..table.max_index].iter())
            {
                debug_assert!(energy.is_finite());
                let idx = note as usize;
                buf[idx] += energy;
                debug_assert!(buf[idx].is_finite());
            }
            self.buffer_i = (self.buffer_i + 1) % BUFFER_ROWS;
        }

        if self.buffer_len >= COEFFICIENTS.len() {
            let offset = (self.buffer_i + BUFFER_ROWS - COEFFICIENTS.len()) % BUFFER_ROWS;
            self.result_buffer.fill(0.0);
            for i in 0..BANDS_LEN {
                let mut out = 0.0;
                for (j, coefficient) in COEFFICIENTS.iter().enumerate() {
                    out += self.buffer[(offset + j) % BUFFER_ROWS][i] * coefficient;
                    debug_assert!(out.is_finite());
                }
                self.result_buffer[i] = out;
            }
            Self::normalize(&mut self.result_buffer);
            Some(&self.result_buffer)
        } else {
            self.buffer_len += 1;
            None
        }
    }

    fn normalize(features: &mut [Float; BANDS_LEN]) {
        let norm = Self::euclidean_norm(features);
        if norm < 0.01 {
            features.fill(0.0);
        } else {
            for feature in features.iter_mut() {
                *feature /= norm;
            }
        }
    }

    fn euclidean_norm(features: &[Float; BANDS_LEN]) -> Float {
        let mut squares: Float = 0.0;
        for &feature in features {
            squares += feature * feature;
        }
        debug_assert!(squares.is_finite());
        squares.sqrt()
    }
}

impl Default for Chroma {
    fn default() -> Self {
        Self::new()
    }
}

struct RollingIntegralImage {
    data: [Float; RollingIntegralImage::DATA_SIZE],
    num_rows: usize,
}

impl RollingIntegralImage {
    const MAX_ROWS: usize = 256 + 1;
    const NUM_COLUMNS: usize = BANDS_LEN;
    const DATA_SIZE: usize = Self::MAX_ROWS * Self::NUM_COLUMNS;

    fn new() -> Self {
        Self {
            data: [0.0; Self::DATA_SIZE],
            num_rows: 0,
        }
    }

    fn add_row(&mut self, features: &[Float; BANDS_LEN]) {
        let current_start = (self.num_rows % Self::MAX_ROWS) * Self::NUM_COLUMNS;
        let mut accum: Float = 0.0;
        for (i, &feature) in features.iter().enumerate() {
            debug_assert!(feature.is_finite());
            accum += feature;
            self.data[current_start + i] = accum;
        }

        if self.num_rows > 0 {
            let last_start = ((self.num_rows - 1) % Self::MAX_ROWS) * Self::NUM_COLUMNS;
            for i in 0..Self::NUM_COLUMNS {
                let last = self.data[last_start + i];
                self.data[current_start + i] += last;
            }
        }
        self.num_rows += 1;
    }

    fn row(&self, i: usize) -> &[Float] {
        let start = (i % Self::MAX_ROWS) * Self::NUM_COLUMNS;
        let end = start + Self::NUM_COLUMNS;
        &self.data[start..end]
    }

    fn area(&self, r1: usize, c1: usize, r2: usize, c2: usize) -> Float {
        debug_assert!(r1 <= self.num_rows);
        debug_assert!(r2 <= self.num_rows);

        if self.num_rows > Self::MAX_ROWS {
            debug_assert!(r1 > self.num_rows - Self::MAX_ROWS);
            debug_assert!(r2 > self.num_rows - Self::MAX_ROWS);
        }

        debug_assert!(c1 <= Self::NUM_COLUMNS);
        debug_assert!(c2 <= Self::NUM_COLUMNS);

        if r1 == r2 || c1 == c2 {
            return 0.0;
        }

        debug_assert!(r2 > r1);
        debug_assert!(c2 > c1);

        if r1 == 0 {
            let row = self.row(r2 - 1);
            if c1 == 0 {
                row[c2 - 1]
            } else {
                row[c2 - 1] - row[c1 - 1]
            }
        } else {
            let row1 = self.row(r1 - 1);
            let row2 = self.row(r2 - 1);
            if c1 == 0 {
                row2[c2 - 1] - row1[c2 - 1]
            } else {
                row2[c2 - 1] - row1[c2 - 1] - row2[c1 - 1] + row1[c1 - 1]
            }
        }
    }

    fn classify(&self, classifier: &Classifier, offset: usize) -> u8 {
        let value = self.apply_filter(&classifier.filter, offset);
        classifier.quantizer.quantize(value)
    }

    fn apply_filter(&self, filter: &Filter, x: usize) -> Float {
        match filter.filter_type {
            0 => self.filter0(
                x,
                filter.y as usize,
                filter.width as usize,
                filter.height as usize,
            ),
            1 => self.filter1(
                x,
                filter.y as usize,
                filter.width as usize,
                filter.height as usize,
            ),
            2 => self.filter2(
                x,
                filter.y as usize,
                filter.width as usize,
                filter.height as usize,
            ),
            3 => self.filter3(
                x,
                filter.y as usize,
                filter.width as usize,
                filter.height as usize,
            ),
            4 => self.filter4(
                x,
                filter.y as usize,
                filter.width as usize,
                filter.height as usize,
            ),
            5 => self.filter5(
                x,
                filter.y as usize,
                filter.width as usize,
                filter.height as usize,
            ),
            _ => unreachable!(),
        }
    }

    fn filter0(&self, x: usize, y: usize, w: usize, h: usize) -> Float {
        debug_assert!(w >= 1);
        debug_assert!(h >= 1);

        let a = self.area(x, y, x + w, y + h);
        let b = 0.0;

        Self::subtract_log(a, b)
    }

    fn filter1(&self, x: usize, y: usize, w: usize, h: usize) -> Float {
        debug_assert!(w >= 1);
        debug_assert!(h >= 1);

        let h_2 = h / 2;

        let a = self.area(x, y + h_2, x + w, y + h);
        let b = self.area(x, y, x + w, y + h_2);

        Self::subtract_log(a, b)
    }

    fn filter2(&self, x: usize, y: usize, w: usize, h: usize) -> Float {
        debug_assert!(w >= 1);
        debug_assert!(h >= 1);

        let w_2 = w / 2;

        let a = self.area(x + w_2, y, x + w, y + h);
        let b = self.area(x, y, x + w_2, y + h);

        Self::subtract_log(a, b)
    }

    fn filter3(&self, x: usize, y: usize, w: usize, h: usize) -> Float {
        debug_assert!(w >= 1);
        debug_assert!(h >= 1);

        let w_2 = w / 2;
        let h_2 = h / 2;

        let a = self.area(x, y + h_2, x + w_2, y + h) + self.area(x + w_2, y, x + w, y + h_2);
        let b = self.area(x, y, x + w_2, y + h_2) + self.area(x + w_2, y + h_2, x + w, y + h);

        Self::subtract_log(a, b)
    }

    fn filter4(&self, x: usize, y: usize, w: usize, h: usize) -> Float {
        debug_assert!(w >= 1);
        debug_assert!(h >= 1);

        let h_3 = h / 3;

        let a = self.area(x, y + h_3, x + w, y + 2 * h_3);
        let b = self.area(x, y, x + w, y + h_3) + self.area(x, y + 2 * h_3, x + w, y + h);

        Self::subtract_log(a, b)
    }

    fn filter5(&self, x: usize, y: usize, w: usize, h: usize) -> Float {
        debug_assert!(w >= 1);
        debug_assert!(h >= 1);

        let w_3 = w / 3;

        let a = self.area(x + w_3, y, x + 2 * w_3, y + h);
        let b = self.area(x, y, x + w_3, y + h) + self.area(x + 2 * w_3, y, x + w, y + h);

        Self::subtract_log(a, b)
    }

    fn subtract_log(a: Float, b: Float) -> Float {
        let r = ((1.0 + a) / (1.0 + b)).ln();
        debug_assert!(r.is_finite());
        r
    }
}

pub struct FingerprintCalculator {
    image: RollingIntegralImage,
    fingerprint: Vec<u32>,
}

impl FingerprintCalculator {
    pub fn new() -> Self {
        Self {
            image: RollingIntegralImage::new(),
            fingerprint: Vec::new(),
        }
    }

    pub fn add_features(&mut self, features: &[Float; BANDS_LEN]) {
        self.image.add_row(features);
        if self.image.num_rows >= MAX_FILTER_WIDTH {
            let sub_fingerprint =
                self.calculate_sub_fingerprint(self.image.num_rows - MAX_FILTER_WIDTH);
            self.fingerprint.push(sub_fingerprint);
        }
    }

    pub fn fingerprint(&self) -> &[u32] {
        &self.fingerprint
    }

    pub fn into_fingerprint(self) -> Vec<u32> {
        self.fingerprint
    }

    fn calculate_sub_fingerprint(&self, offset: usize) -> u32 {
        const GRAY_CODES: [u8; 4] = [0, 1, 3, 2];
        let mut bits: u32 = 0;
        for classifier in CLASSIFIERS.iter() {
            let value = self.image.classify(classifier, offset) as usize;
            bits = (bits << 2) | GRAY_CODES[value] as u32;
        }
        bits
    }
}

impl Default for FingerprintCalculator {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Copy, Clone)]
struct Classifier {
    filter: Filter,
    quantizer: Quantizer,
}

impl Classifier {
    const fn new(filter_type: u8, y: u8, height: u8, width: u8, t: [Float; 3]) -> Self {
        Self {
            filter: Filter::new(filter_type, y, height, width),
            quantizer: Quantizer { t },
        }
    }
}

#[derive(Copy, Clone)]
struct Filter {
    filter_type: u8,
    y: u8,
    height: u8,
    width: u8,
}

impl Filter {
    const fn new(filter_type: u8, y: u8, height: u8, width: u8) -> Self {
        Self {
            filter_type,
            y,
            height,
            width,
        }
    }
}

#[derive(Copy, Clone)]
struct Quantizer {
    t: [Float; 3],
}

impl Quantizer {
    fn quantize(self, value: Float) -> u8 {
        if value < self.t[1] {
            if value < self.t[0] { 0 } else { 1 }
        } else if value < self.t[2] {
            2
        } else {
            3
        }
    }
}

const CLASSIFIERS: [Classifier; 16] = [
    Classifier::new(0, 4, 3, 15, [1.98215, 2.35817, 2.63523]),
    Classifier::new(4, 4, 6, 15, [-1.03809, -0.651211, -0.282167]),
    Classifier::new(1, 0, 4, 16, [-0.298702, 0.119262, 0.558497]),
    Classifier::new(3, 8, 2, 12, [-0.105439, 0.0153946, 0.135898]),
    Classifier::new(3, 4, 4, 8, [-0.142891, 0.0258736, 0.200632]),
    Classifier::new(4, 0, 3, 5, [-0.826319, -0.590612, -0.368214]),
    Classifier::new(1, 2, 2, 9, [-0.557409, -0.233035, 0.0534525]),
    Classifier::new(2, 7, 3, 4, [-0.0646826, 0.00620476, 0.0784847]),
    Classifier::new(2, 6, 2, 16, [-0.192387, -0.029699, 0.215855]),
    Classifier::new(2, 1, 3, 2, [-0.0397818, -0.00568076, 0.0292026]),
    Classifier::new(5, 10, 1, 15, [-0.53823, -0.369934, -0.190235]),
    Classifier::new(3, 6, 2, 10, [-0.124877, 0.0296483, 0.139239]),
    Classifier::new(2, 1, 1, 14, [-0.101475, 0.0225617, 0.231971]),
    Classifier::new(3, 5, 6, 4, [-0.0799915, -0.00729616, 0.063262]),
    Classifier::new(1, 9, 2, 12, [-0.272556, 0.019424, 0.302559]),
    Classifier::new(3, 4, 2, 14, [-0.164292, -0.0321188, 0.0846339]),
];

const MAX_FILTER_WIDTH: usize = 16;

struct Compress {
    normal_bits: Vec<u8>,
    exceptional_bits: Vec<u8>,
}

impl Compress {
    fn new() -> Self {
        Self {
            normal_bits: Vec::new(),
            exceptional_bits: Vec::new(),
        }
    }

    fn process_sub_fingerprint(&mut self, elem: u32) {
        let normal_bits = 3u8;
        let max_normal_value = (1 << normal_bits) - 1;

        let mut bit: u8 = 1;
        let mut last_bit: u8 = 0;
        let mut x = elem;
        while x != 0 {
            if (x & 1) != 0 {
                let value = bit - last_bit;
                if value >= max_normal_value {
                    self.normal_bits.push(max_normal_value);
                    self.exceptional_bits.push(value - max_normal_value);
                } else {
                    self.normal_bits.push(value);
                }
                last_bit = bit;
            }
            x >>= 1;
            bit += 1;
        }
        self.normal_bits.push(0);
    }
}

pub fn compress(fingerprint: &[u32]) -> Vec<u8> {
    let mut c = Compress::new();

    if !fingerprint.is_empty() {
        c.normal_bits.reserve(fingerprint.len());
        c.exceptional_bits.reserve(fingerprint.len() / 10);
        c.process_sub_fingerprint(fingerprint[0]);
        for (prev, cur) in fingerprint.iter().zip(fingerprint.iter().skip(1)) {
            c.process_sub_fingerprint(cur ^ prev);
        }
    }

    let result_len = 4
        + packed_int_array_size(3, c.normal_bits.len())
        + packed_int_array_size(5, c.exceptional_bits.len());
    let mut result = vec![0u8; result_len];

    let algorithm = 1;
    let len = fingerprint.len();
    result[0] = algorithm;
    result[1] = ((len >> 16) & 0xff) as u8;
    result[2] = ((len >> 8) & 0xff) as u8;
    result[3] = (len & 0xff) as u8;

    let mut offset = 4;
    offset += pack_int3_array(&mut result[offset..], &c.normal_bits);
    offset += pack_int5_array(&mut result[offset..], &c.exceptional_bits);
    debug_assert_eq!(offset, result.len());

    result
}

fn packed_int_array_size(bits: usize, size: usize) -> usize {
    (size * bits).div_ceil(8)
}

fn pack_int3_array(destination: &mut [u8], source: &[u8]) -> usize {
    pack_int_array(destination, source, 3)
}

fn pack_int5_array(destination: &mut [u8], source: &[u8]) -> usize {
    pack_int_array(destination, source, 5)
}

fn pack_int_array(destination: &mut [u8], source: &[u8], bits: u8) -> usize {
    let required = packed_int_array_size(bits as usize, source.len());
    debug_assert!(destination.len() >= required);
    if required == 0 {
        return 0;
    }

    destination[..required].fill(0);
    let mask = (1u32 << bits as u32) - 1;
    let mut bit_index = 0usize;

    for &value in source {
        let mut v = (value as u32) & mask;
        for _ in 0..bits {
            if (v & 1) != 0 {
                let byte_index = bit_index / 8;
                let bit_in_byte = bit_index % 8;
                destination[byte_index] |= 1u8 << bit_in_byte;
            }
            v >>= 1;
            bit_index += 1;
        }
    }

    required
}

fn freq_to_index(freq: f64) -> usize {
    (WINDOW_SIZE as f64 * freq / SAMPLE_RATE as f64).round() as usize
}

fn index_to_freq(i: f64) -> f64 {
    i * SAMPLE_RATE as f64 / WINDOW_SIZE as f64
}

fn freq_to_octave(freq: f64) -> f64 {
    let base = 440.0 / 16.0;
    (freq / base).log2()
}
