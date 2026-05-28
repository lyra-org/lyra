use std::io::Cursor;

use anyhow::{
    Context,
    Result,
};
use image::{
    Rgb,
    RgbImage,
    codecs::jpeg::JpegEncoder,
};
use palette::Oklab;

use crate::{
    ArtworkOptions,
    palette::{
        PaletteColor,
        lab_to_rgb,
    },
};

const ANCHORS: &[(f32, f32)] = &[
    (0.14, 0.16),
    (0.84, 0.18),
    (0.22, 0.84),
    (0.86, 0.78),
    (0.50, 0.48),
];

pub(crate) fn render_gradient(colors: &[PaletteColor], options: &ArtworkOptions) -> RgbImage {
    let width = options.width.max(1);
    let height = options.height.max(1);
    let anchors = anchor_colors(colors, options);
    let mut image = RgbImage::new(width, height);

    for y in 0..height {
        let ny = if height > 1 {
            y as f32 / (height - 1) as f32
        } else {
            0.0
        };
        for x in 0..width {
            let nx = if width > 1 {
                x as f32 / (width - 1) as f32
            } else {
                0.0
            };
            let mut l = 0.0;
            let mut a = 0.0;
            let mut b = 0.0;
            let mut total = 0.0;

            for (anchor_index, (ax, ay, color)) in anchors.iter().enumerate() {
                let dx = nx - ax;
                let dy = ny - ay;
                let distance = (dx * dx + dy * dy).sqrt();
                let radius = if anchor_index == 0 { 0.86 } else { 0.48 };
                let weight = (1.0 - (distance / radius).min(1.0)).powf(2.2) + 0.02;
                l += color.lab.l * weight;
                a += color.lab.a * weight;
                b += color.lab.b * weight;
                total += weight;
            }

            let mut lab = Oklab::new(l / total, a / total, b / total);
            let edge = ((nx - 0.5).hypot(ny - 0.5) / 0.72).clamp(0.0, 1.0);
            lab.l = (lab.l - edge * 0.075 + noise(options, x, y) * 0.018).clamp(0.02, 0.96);

            image.put_pixel(x, y, Rgb(lab_to_rgb(lab)));
        }
    }

    image
}

pub(crate) fn encode_jpeg(image: &RgbImage, quality: u8) -> Result<Vec<u8>> {
    let mut cursor = Cursor::new(Vec::new());
    let mut encoder = JpegEncoder::new_with_quality(&mut cursor, quality);
    encoder
        .encode(
            image.as_raw(),
            image.width(),
            image.height(),
            image::ExtendedColorType::Rgb8,
        )
        .context("failed to encode generated artwork as JPEG")?;
    Ok(cursor.into_inner())
}

pub(crate) fn image_blurhash(image: &RgbImage) -> Option<String> {
    let rgba = image::DynamicImage::ImageRgb8(image.clone())
        .thumbnail(128, 128)
        .to_rgba8();
    blurhash::encode(4, 3, rgba.width(), rgba.height(), rgba.as_raw()).ok()
}

fn anchor_colors(
    colors: &[PaletteColor],
    options: &ArtworkOptions,
) -> Vec<(f32, f32, PaletteColor)> {
    let mut anchors = Vec::new();
    for (index, color) in colors.iter().enumerate() {
        let (mut x, mut y) = ANCHORS[index % ANCHORS.len()];
        x = (x + (deterministic_unit(options, index as u32, 0) - 0.5) * 0.08).clamp(0.04, 0.96);
        y = (y + (deterministic_unit(options, index as u32, 1) - 0.5) * 0.08).clamp(0.04, 0.96);
        anchors.push((x, y, *color));
    }
    anchors
}

fn noise(options: &ArtworkOptions, x: u32, y: u32) -> f32 {
    deterministic_unit(options, x.wrapping_mul(73856093), y.wrapping_mul(19349663)) - 0.5
}

fn deterministic_unit(options: &ArtworkOptions, a: u32, b: u32) -> f32 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(options.seed.as_bytes());
    hasher.update(&options.renderer_version.to_le_bytes());
    hasher.update(&a.to_le_bytes());
    hasher.update(&b.to_le_bytes());
    let hash = hasher.finalize();
    let value = u32::from_le_bytes(hash.as_bytes()[0..4].try_into().expect("hash slice length"));
    value as f32 / u32::MAX as f32
}
