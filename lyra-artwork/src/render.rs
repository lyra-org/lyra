use std::{
    f32::consts::FRAC_PI_4,
    io::Cursor,
};

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
        chroma,
        lab_to_rgb,
    },
};

const MIN_RENDER_SIZE: u32 = 1;

#[derive(Clone, Copy)]
struct Plane {
    color: Oklab,
    angle: f32,
    offset: f32,
    softness: f32,
    strength: f32,
}

#[derive(Clone, Copy)]
struct Band {
    color: Oklab,
    angle: f32,
    offset: f32,
    width: f32,
    softness: f32,
    strength: f32,
}

#[derive(Clone, Copy)]
struct RoleColors {
    base: Oklab,
    primary: Oklab,
    support: Oklab,
    contrast: Oklab,
    highlight: Oklab,
}

pub(crate) fn render_gradient_card(colors: &[PaletteColor], options: &ArtworkOptions) -> RgbImage {
    let width = options.width.max(MIN_RENDER_SIZE);
    let height = options.height.max(MIN_RENDER_SIZE);
    let roles = role_colors(colors);
    let composition = composition_angle(options);
    let planes = [
        Plane {
            color: roles.support,
            angle: composition,
            offset: -0.34,
            softness: 0.42,
            strength: 0.46,
        },
        Plane {
            color: roles.contrast,
            angle: composition + FRAC_PI_4 * 0.92,
            offset: 0.12,
            softness: 0.3,
            strength: 0.64,
        },
        Plane {
            color: roles.primary,
            angle: composition - FRAC_PI_4 * 0.7,
            offset: 0.42,
            softness: 0.34,
            strength: 0.32,
        },
    ];
    let bands = [
        Band {
            color: roles.contrast,
            angle: composition + 0.08,
            offset: -0.05,
            width: 0.18,
            softness: 0.075,
            strength: 0.42,
        },
        Band {
            color: roles.highlight,
            angle: composition - 0.34,
            offset: 0.32,
            width: 0.085,
            softness: 0.055,
            strength: 0.38,
        },
        Band {
            color: roles.primary,
            angle: composition + 0.68,
            offset: -0.48,
            width: 0.16,
            softness: 0.1,
            strength: 0.22,
        },
    ];

    let mut image = RgbImage::new(width, height);
    for y in 0..height {
        let ny = norm(y, height);
        for x in 0..width {
            let nx = norm(x, width);
            let mut lab = background(roles.base, roles.primary, nx, ny, composition);

            for plane in planes {
                let amount = half_plane_amount(nx, ny, plane);
                lab = mix_lab(lab, plane.color, amount);
            }

            for band in bands {
                let amount = band_amount(nx, ny, band);
                lab = mix_lab(lab, band.color, amount);
            }

            lab = apply_corner_glow(lab, roles.support, roles.highlight, nx, ny);
            lab = apply_vignette(lab, roles.base, nx, ny);
            lab = apply_grain(lab, options, x, y);
            lab = polish(lab);

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

fn role_colors(colors: &[PaletteColor]) -> RoleColors {
    let fallback = Oklab::new(0.12, 0.0, 0.0);
    let labs = colors.iter().map(|color| color.lab).collect::<Vec<_>>();
    let base_source = labs.first().copied().unwrap_or(fallback);
    let primary_index = usize::from(labs.len() > 1);
    let primary_source = labs.get(primary_index).copied().unwrap_or(base_source);
    let contrast_index = role_index(&labs, |lab| {
        lab_distance(*lab, primary_source) * 1.75
            + lab_distance(*lab, base_source) * 0.65
            + chroma(*lab) * 0.55
    })
    .unwrap_or(primary_index);
    let highlight_index =
        role_index(&labs, |lab| lab.l + chroma(*lab) * 0.16).unwrap_or(primary_index);
    let support_index = labs
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != primary_index && *index != contrast_index)
        .max_by(|(_, a), (_, b)| compare_f32(chroma(**a), chroma(**b)))
        .map(|(index, _)| index)
        .unwrap_or(highlight_index);

    RoleColors {
        base: shade_role(base_source),
        primary: mid_role(primary_source),
        support: mid_role(labs.get(support_index).copied().unwrap_or(primary_source)),
        contrast: mid_role(labs.get(contrast_index).copied().unwrap_or(primary_source)),
        highlight: highlight_role(labs.get(highlight_index).copied().unwrap_or(primary_source)),
    }
}

fn role_index(labs: &[Oklab], score: impl Fn(&Oklab) -> f32) -> Option<usize> {
    labs.iter()
        .enumerate()
        .max_by(|(_, a), (_, b)| compare_f32(score(a), score(b)))
        .map(|(index, _)| index)
}

fn shade_role(mut lab: Oklab) -> Oklab {
    lab.l = (lab.l * 0.58).clamp(0.055, 0.24);
    lab.a *= 0.9;
    lab.b *= 0.9;
    lab
}

fn mid_role(mut lab: Oklab) -> Oklab {
    lab.l = lab.l.clamp(0.26, 0.72);
    if chroma(lab) > 0.025 {
        lab.a *= 1.08;
        lab.b *= 1.08;
    }
    lab
}

fn highlight_role(mut lab: Oklab) -> Oklab {
    lab.l = (lab.l + 0.1).clamp(0.54, 0.86);
    if chroma(lab) > 0.025 {
        lab.a *= 1.04;
        lab.b *= 1.04;
    }
    lab
}

fn background(base: Oklab, primary: Oklab, nx: f32, ny: f32, angle: f32) -> Oklab {
    let ramp = projected(nx, ny, angle);
    let center_light = 1.0 - ((nx - 0.48).hypot(ny - 0.42) / 0.78).clamp(0.0, 1.0);
    let amount = (smoothstep((ramp + 0.74) / 1.48) * 0.5 + center_light * 0.1).clamp(0.0, 0.58);
    let mut lab = mix_lab(base, primary, amount);
    lab.l = (lab.l + center_light * 0.035).clamp(0.0, 1.0);
    lab
}

fn half_plane_amount(nx: f32, ny: f32, plane: Plane) -> f32 {
    let distance = projected(nx, ny, plane.angle) - plane.offset;
    smoothstep(distance / plane.softness + 0.5) * plane.strength
}

fn band_amount(nx: f32, ny: f32, band: Band) -> f32 {
    let distance = (projected(nx, ny, band.angle) - band.offset).abs();
    let core = 1.0 - smoothstep((distance - band.width * 0.5) / band.softness);
    core.clamp(0.0, 1.0).powf(1.2) * band.strength
}

fn apply_corner_glow(mut lab: Oklab, secondary: Oklab, accent: Oklab, nx: f32, ny: f32) -> Oklab {
    let top_right = radial(nx, ny, 0.86, 0.12, 0.76).powf(1.3) * 0.2;
    let lower_left = radial(nx, ny, 0.16, 0.86, 0.68).powf(1.45) * 0.16;
    lab = mix_lab(lab, accent, top_right);
    mix_lab(lab, secondary, lower_left)
}

fn apply_vignette(mut lab: Oklab, base: Oklab, nx: f32, ny: f32) -> Oklab {
    let edge = ((nx - 0.5).hypot(ny - 0.5) / 0.72).clamp(0.0, 1.0);
    let amount = smoothstep(edge) * 0.36;
    lab = mix_lab(lab, base, amount * 0.45);
    lab.l = (lab.l - amount * 0.085).clamp(0.0, 1.0);
    lab
}

fn apply_grain(mut lab: Oklab, options: &ArtworkOptions, x: u32, y: u32) -> Oklab {
    let grain = deterministic_unit(options, x.wrapping_mul(73856093), y.wrapping_mul(19349663));
    lab.l = (lab.l + (grain - 0.5) * 0.018).clamp(0.0, 1.0);
    lab
}

fn polish(mut lab: Oklab) -> Oklab {
    let c = chroma(lab);
    if c > 0.015 {
        let boost = 1.0 + (0.08 - c).max(0.0) * 1.2;
        lab.a *= boost;
        lab.b *= boost;
    }
    lab.l = lab.l.clamp(0.035, 0.92);
    lab
}

fn projected(nx: f32, ny: f32, angle: f32) -> f32 {
    let x = nx - 0.5;
    let y = ny - 0.5;
    x * angle.cos() + y * angle.sin()
}

fn radial(nx: f32, ny: f32, cx: f32, cy: f32, radius: f32) -> f32 {
    (1.0 - ((nx - cx).hypot(ny - cy) / radius).clamp(0.0, 1.0)).max(0.0)
}

fn norm(value: u32, size: u32) -> f32 {
    if size <= 1 {
        0.5
    } else {
        value as f32 / (size - 1) as f32
    }
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

fn composition_angle(options: &ArtworkOptions) -> f32 {
    let amount = deterministic_unit(options, 5, 17);
    -0.72 + (amount - 0.5) * 0.28
}

fn lab_distance(a: Oklab, b: Oklab) -> f32 {
    let dl = a.l - b.l;
    let da = a.a - b.a;
    let db = a.b - b.b;
    (dl * dl + da * da + db * db).sqrt()
}

fn compare_f32(a: f32, b: f32) -> std::cmp::Ordering {
    a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal)
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
