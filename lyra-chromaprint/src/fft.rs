use std::{
    f32::consts::PI,
    sync::LazyLock,
};

use crate::fingerprint::{
    FFT_FRAME_SIZE,
    Float,
    WINDOW_SIZE,
};

const RADIX4_STAGES: usize = WINDOW_SIZE.trailing_zeros() as usize / 2;

pub static FFT: LazyLock<Fft> = LazyLock::new(Fft::new);

pub struct Fft {
    digit_reversal: [u16; WINDOW_SIZE],
    twiddles: [(Float, Float); WINDOW_SIZE / 2],
}

impl Fft {
    fn new() -> Self {
        let mut digit_reversal = [0; WINDOW_SIZE];
        for (i, slot) in digit_reversal.iter_mut().enumerate() {
            *slot = reverse_base4_digits(i) as u16;
        }

        let mut twiddles = [(0.0, 0.0); WINDOW_SIZE / 2];
        for (i, slot) in twiddles.iter_mut().enumerate() {
            let angle = -2.0 * PI * (i as Float) / (WINDOW_SIZE as Float);
            let (sin, cos) = angle.sin_cos();
            *slot = (cos, sin);
        }

        Self {
            digit_reversal,
            twiddles,
        }
    }

    pub fn power_spectrum(
        &self,
        samples: &[i16],
        hamming: &[Float; WINDOW_SIZE],
        real: &mut [Float; WINDOW_SIZE],
        imag: &mut [Float; WINDOW_SIZE],
        fft_frame: &mut [Float; FFT_FRAME_SIZE],
    ) {
        for i in 0..WINDOW_SIZE {
            let j = self.digit_reversal[i] as usize;
            real[j] = (samples[i] as Float / 32768.0) * hamming[i];
            imag[j] = 0.0;
        }

        let mut len = 4;
        for _ in 0..RADIX4_STAGES {
            let quarter = len / 4;
            let twiddle_step = WINDOW_SIZE / len;

            for start in (0..WINDOW_SIZE).step_by(len) {
                for j in 0..quarter {
                    let a = start + j;
                    let b = a + quarter;
                    let c = b + quarter;
                    let d = c + quarter;

                    let (b_re, b_im) = rotate(real[b], imag[b], self.twiddles[j * twiddle_step]);
                    let (c_re, c_im) =
                        rotate(real[c], imag[c], self.twiddles[2 * j * twiddle_step]);
                    let (d_re, d_im) =
                        rotate(real[d], imag[d], self.twiddles[3 * j * twiddle_step]);

                    let a_re = real[a];
                    let a_im = imag[a];

                    let ac_sum_re = a_re + c_re;
                    let ac_sum_im = a_im + c_im;
                    let ac_diff_re = a_re - c_re;
                    let ac_diff_im = a_im - c_im;
                    let bd_sum_re = b_re + d_re;
                    let bd_sum_im = b_im + d_im;
                    let bd_diff_re = b_re - d_re;
                    let bd_diff_im = b_im - d_im;

                    real[a] = ac_sum_re + bd_sum_re;
                    imag[a] = ac_sum_im + bd_sum_im;
                    real[b] = ac_diff_re + bd_diff_im;
                    imag[b] = ac_diff_im - bd_diff_re;
                    real[c] = ac_sum_re - bd_sum_re;
                    imag[c] = ac_sum_im - bd_sum_im;
                    real[d] = ac_diff_re - bd_diff_im;
                    imag[d] = ac_diff_im + bd_diff_re;
                }
            }

            len *= 4;
        }

        for i in 0..FFT_FRAME_SIZE {
            fft_frame[i] = real[i] * real[i] + imag[i] * imag[i];
        }
    }
}

fn reverse_base4_digits(mut value: usize) -> usize {
    let mut reversed = 0;
    for _ in 0..RADIX4_STAGES {
        reversed = (reversed << 2) | (value & 0b11);
        value >>= 2;
    }
    reversed
}

fn rotate(real: Float, imag: Float, (twiddle_re, twiddle_im): (Float, Float)) -> (Float, Float) {
    (
        twiddle_re * real - twiddle_im * imag,
        twiddle_re * imag + twiddle_im * real,
    )
}
