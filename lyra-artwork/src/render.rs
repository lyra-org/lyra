use std::io::Cursor;

use anyhow::{
    Context,
    Result,
};
use image::{
    DynamicImage,
    GenericImageView,
    Rgb,
    RgbImage,
    codecs::jpeg::JpegEncoder,
    imageops::FilterType,
};
use palette::Oklab;

use crate::{
    ArtworkSource,
    ArtworkOptions,
    palette::{
        PaletteColor,
        chroma,
        lab_to_rgb,
        rgb_to_lab,
    },
};

const FIELD_SIZE: u32 = 96;
const SOURCE_SIZE: u32 = 48;

struct SourceField {
    anchor: (f32, f32),
    image: RgbImage,
}

pub(crate) fn render_color_field(
    sources: &[&ArtworkSource],
    colors: &[PaletteColor],
    options: &ArtworkOptions,
) -> RgbImage {
    let width = options.width.max(1);
    let height = options.height.max(1);
    let fields = source_fields(sources, options);
    let base = colors
        .first()
        .map(|color| color.lab)
        .unwrap_or_else(|| Oklab::new(0.12, 0.0, 0.0));
    let accent = colors.get(1).map(|color| color.lab).unwrap_or(base);
    let mut low_res = RgbImage::new(FIELD_SIZE, FIELD_SIZE);

    for y in 0..FIELD_SIZE {
        let ny = if FIELD_SIZE > 1 {
            y as f32 / (FIELD_SIZE - 1) as f32
        } else {
            0.0
        };
        for x in 0..FIELD_SIZE {
            let nx = if FIELD_SIZE > 1 {
                x as f32 / (FIELD_SIZE - 1) as f32
            } else {
                0.0
            };

            let mut l = base.l * 0.18;
            let mut a = base.a * 0.18;
            let mut b = base.b * 0.18;
            let mut total = 0.18;

            for (index, field) in fields.iter().enumerate() {
                let dx = nx - field.anchor.0;
                let dy = ny - field.anchor.1;
                let distance_sq = dx * dx + dy * dy;
                let spread = field_spread(fields.len(), index);
                let spatial_weight = (-distance_sq / (2.0 * spread * spread)).exp();
                let sample = sample_field(field, nx, ny);
                let color_weight = if chroma(sample) < 0.025 { 0.75 } else { 1.0 };
                let weight = spatial_weight * color_weight;
                l += sample.l * weight;
                a += sample.a * weight;
                b += sample.b * weight;
                total += weight;
            }

            let mut lab = Oklab::new(l / total, a / total, b / total);
            let sweep = diagonal_sweep(nx, ny);
            lab = mix_lab(lab, accent, sweep * 0.12);
            lab.a *= 1.08;
            lab.b *= 1.08;
            let edge = ((nx - 0.5).hypot(ny - 0.5) / 0.72).clamp(0.0, 1.0);
            lab.l = (lab.l - edge * 0.085 + ribbon * 0.035 + noise(options, x, y) * 0.012)
                .clamp(0.02, 0.96);

            low_res.put_pixel(x, y, Rgb(lab_to_rgb(lab)));
        }
    }

    image::imageops::resize(&low_res, width, height, FilterType::CatmullRom)
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

fn source_fields(sources: &[&ArtworkSource], options: &ArtworkOptions) -> Vec<SourceField> {
    sources
        .iter()
        .enumerate()
        .map(|(index, source)| SourceField {
            anchor: source_anchor(sources.len(), index, options),
            image: source_color_field(&source.image),
        })
        .collect()
}

fn source_color_field(image: &DynamicImage) -> RgbImage {
    let (width, height) = image.dimensions();
    let side = width.min(height).max(1);
    let x = width.saturating_sub(side) / 2;
    let y = height.saturating_sub(side) / 2;
    let cropped = image.crop_imm(x, y, side, side);
    let small = cropped.resize_exact(SOURCE_SIZE, SOURCE_SIZE, FilterType::CatmullRom);
    small.blur(7.0).to_rgb8()
}

fn source_anchor(count: usize, index: usize, options: &ArtworkOptions) -> (f32, f32) {
    let anchors: &[(f32, f32)] = match count {
        0 | 1 => &[(0.5, 0.5)],
        2 => &[(0.28, 0.36), (0.72, 0.64)],
        3 => &[(0.24, 0.24), (0.78, 0.32), (0.42, 0.78)],
        _ => &[(0.22, 0.24), (0.78, 0.22), (0.24, 0.78), (0.78, 0.76)],
    };
    let (x, y) = anchors[index % anchors.len()];
    let jitter_x = (deterministic_unit(options, index as u32, 11) - 0.5) * 0.08;
    let jitter_y = (deterministic_unit(options, index as u32, 29) - 0.5) * 0.08;
    ((x + jitter_x).clamp(0.12, 0.88), (y + jitter_y).clamp(0.12, 0.88))
}

fn field_spread(count: usize, index: usize) -> f32 {
    match count {
        0 | 1 => 0.9,
        2 => 0.54,
        3 => {
            if index == 2 {
                0.48
            } else {
                0.5
            }
        }
        _ => 0.43,
    }
}

fn sample_field(field: &SourceField, nx: f32, ny: f32) -> Oklab {
    let sx = (nx * 0.74 + field.anchor.0 * 0.26).clamp(0.0, 1.0);
    let sy = (ny * 0.74 + field.anchor.1 * 0.26).clamp(0.0, 1.0);
    let x = (sx * (SOURCE_SIZE - 1) as f32).round() as u32;
    let y = (sy * (SOURCE_SIZE - 1) as f32).round() as u32;
    let pixel = field.image.get_pixel(x, y).0;
    rgb_to_lab(pixel)
}

fn noise(options: &ArtworkOptions, x: u32, y: u32) -> f32 {
    deterministic_unit(options, x.wrapping_mul(73856093), y.wrapping_mul(19349663)) - 0.5
}

fn mix_lab(a: Oklab, b: Oklab, amount: f32) -> Oklab {
    let t = amount.clamp(0.0, 1.0);
    Oklab::new(
        a.l + (b.l - a.l) * t,
        a.a + (b.a - a.a) * t,
        a.b + (b.b - a.b) * t,
    )
}

fn smoothstep(value: f32) -> f32 {
    let t = value.clamp(0.0, 1.0);
    t * t * (3.0 - 2.0 * t)
}

fn diagonal_sweep(nx: f32, ny: f32) -> f32 {
    let distance = (ny - (0.2 + nx * 0.56)).abs();
    smoothstep(1.0 - distance / 0.22).powf(1.7)
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
