// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! BlurHash encoding, following the reference algorithm at
//! <https://github.com/woltapp/blurhash>.

use std::f32::consts::PI;

use image::RgbaImage;

const COMPONENTS_X: u32 = 4;
const COMPONENTS_Y: u32 = 3;

const BASE83: &[u8; 83] =
    b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz#$%*+,-.:;=?@[]^_{|}~";

pub(super) fn encode(image: &RgbaImage) -> String {
    let linear: [f32; 256] = std::array::from_fn(|value| srgb_to_linear(value as u8));
    let mut factors = Vec::with_capacity((COMPONENTS_X * COMPONENTS_Y) as usize);
    for component_y in 0..COMPONENTS_Y {
        for component_x in 0..COMPONENTS_X {
            factors.push(factor(image, &linear, component_x, component_y));
        }
    }
    let (average, details) = (factors[0], &factors[1..]);

    let peak = details
        .iter()
        .flatten()
        .fold(0.0_f32, |peak, value| peak.max(value.abs()));
    let quantised_peak = (peak * 166.0 - 0.5).floor().clamp(0.0, 82.0) as u32;
    let scale = (quantised_peak + 1) as f32 / 166.0;

    let mut hash = String::with_capacity(6 + 2 * details.len());
    push_base83(&mut hash, (COMPONENTS_X - 1) + (COMPONENTS_Y - 1) * 9, 1);
    push_base83(&mut hash, quantised_peak, 1);
    let [red, green, blue] = average.map(linear_to_srgb);
    push_base83(&mut hash, (red << 16) | (green << 8) | blue, 4);
    for detail in details {
        let [red, green, blue] = detail.map(|value| quantise_detail(value / scale));
        push_base83(&mut hash, red * 19 * 19 + green * 19 + blue, 2);
    }
    hash
}

/// The image's linear colour weighted by one cosine basis function.
fn factor(image: &RgbaImage, linear: &[f32; 256], component_x: u32, component_y: u32) -> [f32; 3] {
    let (width, height) = image.dimensions();
    let cosines = |component: u32, len: u32| -> Vec<f32> {
        let step = PI * component as f32 / len as f32;
        (0..len).map(|at| (step * at as f32).cos()).collect()
    };
    let (across, down) = (cosines(component_x, width), cosines(component_y, height));

    let mut sum = [0.0_f32; 3];
    for (y, row) in image.rows().enumerate() {
        for (x, pixel) in row.enumerate() {
            let basis = across[x] * down[y];
            for (channel, value) in sum.iter_mut().zip(pixel.0) {
                *channel += basis * linear[value as usize];
            }
        }
    }

    let normalisation = if (component_x, component_y) == (0, 0) {
        1.0
    } else {
        2.0
    };
    let scale = normalisation / (width * height) as f32;
    sum.map(|channel| channel * scale)
}

fn srgb_to_linear(value: u8) -> f32 {
    let value = value as f32 / 255.0;
    if value <= 0.04045 {
        value / 12.92
    } else {
        ((value + 0.055) / 1.055).powf(2.4)
    }
}

fn linear_to_srgb(value: f32) -> u32 {
    let value = value.clamp(0.0, 1.0);
    let srgb = if value <= 0.003_130_8 {
        value * 12.92
    } else {
        1.055 * value.powf(1.0 / 2.4) - 0.055
    };
    (srgb * 255.0 + 0.5) as u32
}

/// A detail component in `-1.0..=1.0` as one of 19 levels, finer near zero.
fn quantise_detail(value: f32) -> u32 {
    let curved = value.abs().sqrt().copysign(value);
    (curved * 9.0 + 9.5).floor().clamp(0.0, 18.0) as u32
}

fn push_base83(hash: &mut String, value: u32, digits: u32) {
    for digit in (0..digits).rev() {
        hash.push(BASE83[(value / 83_u32.pow(digit) % 83) as usize] as char);
    }
}

#[cfg(test)]
mod tests {
    use image::{
        Rgba,
        RgbaImage,
    };

    use super::encode;

    fn gradient(width: u32, height: u32) -> RgbaImage {
        RgbaImage::from_fn(width, height, |x, y| {
            Rgba([
                (x * 255 / width) as u8,
                (y * 255 / height) as u8,
                ((x * 7 + y * 13) % 256) as u8,
                255,
            ])
        })
    }

    #[test]
    fn a_flat_image_has_no_detail() {
        let black = RgbaImage::from_pixel(16, 9, Rgba([0, 0, 0, 255]));

        assert_eq!(encode(&black), format!("L00000{}", "fQ".repeat(11)));
    }

    #[test]
    fn encodes_average_colour_and_detail() {
        assert_eq!(encode(&gradient(128, 96)), "L$HLl22Y$5SghpazfPf7gcfQfQfQ");
    }
}
