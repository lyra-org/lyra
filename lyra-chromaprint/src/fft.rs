// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    f32::consts::PI,
    sync::LazyLock,
};

use crate::fingerprint::{
    BANDS_LEN,
    CHROMA_MAX_INDEX,
    CHROMA_MIN_INDEX,
    FFT_FRAME_SIZE,
    Float,
    WINDOW_SIZE,
};

pub const WORK_LEN: usize = WINDOW_SIZE / 2;
const WORK_BITS: u32 = WORK_LEN.trailing_zeros();

pub static FFT: LazyLock<RealFft> = LazyLock::new(RealFft::new);

pub struct RealFft {
    bit_reversal: [u16; WORK_LEN],
    fft_twiddles: [(Float, Float); WORK_LEN / 2],
    split_twiddles: [(Float, Float); FFT_FRAME_SIZE],
}

impl RealFft {
    fn new() -> Self {
        let mut bit_reversal = [0; WORK_LEN];
        for (i, slot) in bit_reversal.iter_mut().enumerate() {
            *slot = (i.reverse_bits() >> (usize::BITS - WORK_BITS)) as u16;
        }

        let mut fft_twiddles = [(0.0, 0.0); WORK_LEN / 2];
        for (i, slot) in fft_twiddles.iter_mut().enumerate() {
            let angle = -2.0 * PI * (i as Float) / (WORK_LEN as Float);
            let (sin, cos) = angle.sin_cos();
            *slot = (cos, sin);
        }

        let mut split_twiddles = [(0.0, 0.0); FFT_FRAME_SIZE];
        for (i, slot) in split_twiddles.iter_mut().enumerate() {
            let angle = -2.0 * PI * (i as Float) / (WINDOW_SIZE as Float);
            let (sin, cos) = angle.sin_cos();
            *slot = (cos, sin);
        }

        Self {
            bit_reversal,
            fft_twiddles,
            split_twiddles,
        }
    }

    pub fn chroma_power_spectrum(
        &self,
        samples: &[i16],
        hamming: &[Float; WINDOW_SIZE],
        chroma_notes: &[u8; WINDOW_SIZE],
        real: &mut [Float; WORK_LEN],
        imag: &mut [Float; WORK_LEN],
        bands: &mut [Float; BANDS_LEN],
    ) {
        for i in 0..WORK_LEN {
            let even = i * 2;
            let j = self.bit_reversal[i] as usize;
            real[j] = (samples[even] as Float / 32768.0) * hamming[even];
            imag[j] = (samples[even + 1] as Float / 32768.0) * hamming[even + 1];
        }

        self.process_complex(real, imag);
        self.split_chroma(real, imag, chroma_notes, bands);
    }

    #[cfg(test)]
    pub fn power_spectrum(
        &self,
        samples: &[i16],
        hamming: &[Float; WINDOW_SIZE],
        real: &mut [Float; WORK_LEN],
        imag: &mut [Float; WORK_LEN],
        powers: &mut [Float; FFT_FRAME_SIZE],
    ) {
        for i in 0..WORK_LEN {
            let even = i * 2;
            let j = self.bit_reversal[i] as usize;
            real[j] = (samples[even] as Float / 32768.0) * hamming[even];
            imag[j] = (samples[even + 1] as Float / 32768.0) * hamming[even + 1];
        }

        self.process_complex(real, imag);
        self.split_real_fft(real, imag, powers);
    }

    fn process_complex(&self, real: &mut [Float; WORK_LEN], imag: &mut [Float; WORK_LEN]) {
        self.process_two_radix2_stages::<4>(real, imag);
        self.process_two_radix2_stages::<16>(real, imag);
        self.process_two_radix2_stages::<64>(real, imag);
        self.process_two_radix2_stages::<256>(real, imag);
        self.process_two_radix2_stages::<1024>(real, imag);
        self.process_radix2_stage::<WORK_LEN>(real, imag);
    }

    fn process_two_radix2_stages<const LEN: usize>(
        &self,
        real: &mut [Float; WORK_LEN],
        imag: &mut [Float; WORK_LEN],
    ) {
        let quarter = LEN / 4;
        let half = LEN / 2;
        let twiddle_step = WORK_LEN / LEN;

        for start in (0..WORK_LEN).step_by(LEN) {
            let i0 = start;
            let i1 = i0 + quarter;
            let i2 = i0 + half;
            let i3 = i2 + quarter;

            let a_re = real[i0];
            let a_im = imag[i0];
            let b_re = real[i1];
            let b_im = imag[i1];
            let c_re = real[i2];
            let c_im = imag[i2];
            let d_re = real[i3];
            let d_im = imag[i3];

            let first_even_re = a_re + b_re;
            let first_even_im = a_im + b_im;
            let first_odd_re = a_re - b_re;
            let first_odd_im = a_im - b_im;

            let second_even_re = c_re + d_re;
            let second_even_im = c_im + d_im;
            let second_odd_re = c_re - d_re;
            let second_odd_im = c_im - d_im;

            real[i0] = first_even_re + second_even_re;
            imag[i0] = first_even_im + second_even_im;
            real[i2] = first_even_re - second_even_re;
            imag[i2] = first_even_im - second_even_im;
            real[i1] = first_odd_re + second_odd_im;
            imag[i1] = first_odd_im - second_odd_re;
            real[i3] = first_odd_re - second_odd_im;
            imag[i3] = first_odd_im + second_odd_re;

            for j in 1..quarter {
                let twiddle_i = j * twiddle_step;
                let (stage2_re, stage2_im) = self.fft_twiddles[twiddle_i];
                let (stage1_re, stage1_im) = self.fft_twiddles[twiddle_i * 2];

                let i0 = start + j;
                let i1 = i0 + quarter;
                let i2 = i0 + half;
                let i3 = i2 + quarter;

                let b_re = real[i1];
                let b_im = imag[i1];
                let b_rotated_re = stage1_re * b_re - stage1_im * b_im;
                let b_rotated_im = stage1_re * b_im + stage1_im * b_re;

                let d_re = real[i3];
                let d_im = imag[i3];
                let d_rotated_re = stage1_re * d_re - stage1_im * d_im;
                let d_rotated_im = stage1_re * d_im + stage1_im * d_re;

                let a_re = real[i0];
                let a_im = imag[i0];
                let c_re = real[i2];
                let c_im = imag[i2];

                let first_even_re = a_re + b_rotated_re;
                let first_even_im = a_im + b_rotated_im;
                let first_odd_re = a_re - b_rotated_re;
                let first_odd_im = a_im - b_rotated_im;

                let second_even_re = c_re + d_rotated_re;
                let second_even_im = c_im + d_rotated_im;
                let second_odd_re = c_re - d_rotated_re;
                let second_odd_im = c_im - d_rotated_im;

                let second_even_rotated_re =
                    stage2_re * second_even_re - stage2_im * second_even_im;
                let second_even_rotated_im =
                    stage2_re * second_even_im + stage2_im * second_even_re;

                let second_odd_twiddle_re = stage2_im;
                let second_odd_twiddle_im = -stage2_re;
                let second_odd_rotated_re =
                    second_odd_twiddle_re * second_odd_re - second_odd_twiddle_im * second_odd_im;
                let second_odd_rotated_im =
                    second_odd_twiddle_re * second_odd_im + second_odd_twiddle_im * second_odd_re;

                real[i0] = first_even_re + second_even_rotated_re;
                imag[i0] = first_even_im + second_even_rotated_im;
                real[i2] = first_even_re - second_even_rotated_re;
                imag[i2] = first_even_im - second_even_rotated_im;
                real[i1] = first_odd_re + second_odd_rotated_re;
                imag[i1] = first_odd_im + second_odd_rotated_im;
                real[i3] = first_odd_re - second_odd_rotated_re;
                imag[i3] = first_odd_im - second_odd_rotated_im;
            }
        }
    }

    fn process_radix2_stage<const LEN: usize>(
        &self,
        real: &mut [Float; WORK_LEN],
        imag: &mut [Float; WORK_LEN],
    ) {
        let half = LEN / 2;
        let twiddle_step = WORK_LEN / LEN;

        for start in (0..WORK_LEN).step_by(LEN) {
            for j in 0..half {
                let (twiddle_re, twiddle_im) = self.fft_twiddles[j * twiddle_step];
                let even = start + j;
                let odd = even + half;

                let odd_re = real[odd];
                let odd_im = imag[odd];
                let rotated_re = twiddle_re * odd_re - twiddle_im * odd_im;
                let rotated_im = twiddle_re * odd_im + twiddle_im * odd_re;

                let even_re = real[even];
                let even_im = imag[even];
                real[even] = even_re + rotated_re;
                imag[even] = even_im + rotated_im;
                real[odd] = even_re - rotated_re;
                imag[odd] = even_im - rotated_im;
            }
        }
    }

    #[cfg(test)]
    fn split_real_fft(
        &self,
        real: &[Float; WORK_LEN],
        imag: &[Float; WORK_LEN],
        powers: &mut [Float; FFT_FRAME_SIZE],
    ) {
        let dc = real[0] + imag[0];
        let nyquist = real[0] - imag[0];
        powers[0] = dc * dc;
        powers[WORK_LEN] = nyquist * nyquist;

        for k in 1..CHROMA_MAX_INDEX {
            let energy = self.split_bin_power(real, imag, k);
            powers[k] = energy;
        }
    }

    fn split_chroma(
        &self,
        real: &[Float; WORK_LEN],
        imag: &[Float; WORK_LEN],
        notes: &[u8; WINDOW_SIZE],
        bands: &mut [Float; BANDS_LEN],
    ) {
        bands.fill(0.0);

        for k in CHROMA_MIN_INDEX..CHROMA_MAX_INDEX {
            bands[notes[k] as usize] += self.split_bin_power(real, imag, k);
        }
    }

    fn split_bin_power(
        &self,
        real: &[Float; WORK_LEN],
        imag: &[Float; WORK_LEN],
        k: usize,
    ) -> Float {
        let mirror = WORK_LEN - k;
        let sum_re = real[k] + real[mirror];
        let sum_im = imag[k] - imag[mirror];
        let diff_re = real[k] - real[mirror];
        let diff_im = imag[k] + imag[mirror];

        let (twiddle_re, twiddle_im) = self.split_twiddles[k];
        let rotated_re = twiddle_re * diff_re - twiddle_im * diff_im;
        let rotated_im = twiddle_re * diff_im + twiddle_im * diff_re;

        let re = 0.5 * (sum_re + rotated_im);
        let im = 0.5 * (sum_im - rotated_re);
        re * re + im * im
    }
}
