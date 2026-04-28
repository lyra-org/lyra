// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::path::Path;

use anyhow::{
    Context,
    Result,
    anyhow,
};
use image::GenericImageView;

use super::{
    CoverTransformOptions,
    TransformedCoverImage,
};

pub(crate) const COVER_EXTENSIONS: [&str; 4] = ["jpg", "jpeg", "png", "webp"];

pub(crate) fn normalize_extension(ext: &str) -> Option<&'static str> {
    match ext.to_ascii_lowercase().as_str() {
        "jpg" => Some("jpg"),
        "jpeg" => Some("jpeg"),
        "png" => Some("png"),
        "webp" => Some("webp"),
        _ => None,
    }
}

pub(crate) fn image_format_from_extension(ext: &str) -> Option<image::ImageFormat> {
    match ext {
        "jpg" | "jpeg" => Some(image::ImageFormat::Jpeg),
        "png" => Some(image::ImageFormat::Png),
        "webp" => Some(image::ImageFormat::WebP),
        _ => None,
    }
}

pub(crate) fn mime_from_extension(ext: &str) -> &'static str {
    match ext {
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        "webp" => "image/webp",
        _ => "application/octet-stream",
    }
}

pub(crate) fn extension_from_image_format(format: image::ImageFormat) -> Option<&'static str> {
    match format {
        image::ImageFormat::Jpeg => Some("jpg"),
        image::ImageFormat::Png => Some("png"),
        image::ImageFormat::WebP => Some("webp"),
        _ => None,
    }
}

pub(crate) fn parse_cover_image_format(raw_format: &str) -> Option<image::ImageFormat> {
    let normalized = normalize_extension(raw_format.trim())?;
    image_format_from_extension(normalized)
}

pub(crate) fn cover_mime_from_path(path: &Path) -> &'static str {
    let ext = path
        .extension()
        .and_then(|value| value.to_str())
        .and_then(normalize_extension);

    ext.map(mime_from_extension)
        .unwrap_or("application/octet-stream")
}

fn cover_image_format_from_path(path: &Path) -> Option<image::ImageFormat> {
    let ext = path
        .extension()
        .and_then(|value| value.to_str())
        .and_then(normalize_extension)?;
    image_format_from_extension(ext)
}

fn cover_mime_from_format(format: image::ImageFormat) -> &'static str {
    extension_from_image_format(format)
        .map(mime_from_extension)
        .unwrap_or("application/octet-stream")
}

fn xxh3_128_hex_bytes(bytes: &[u8]) -> String {
    format!("{:032x}", xxh3::hash128_with_seed(bytes, 0))
}

fn target_cover_size(
    source_width: u32,
    source_height: u32,
    max_width: Option<u32>,
    max_height: Option<u32>,
) -> (u32, u32) {
    if max_width.is_none() && max_height.is_none() {
        return (source_width, source_height);
    }

    let mut scale = 1.0_f64;
    if let Some(width_limit) = max_width {
        scale = (width_limit as f64 / source_width as f64).min(scale);
    }
    if let Some(height_limit) = max_height {
        scale = (height_limit as f64 / source_height as f64).min(scale);
    }

    if scale >= 1.0 {
        return (source_width, source_height);
    }

    let target_width = (source_width as f64 * scale).floor().max(1.0) as u32;
    let target_height = (source_height as f64 * scale).floor().max(1.0) as u32;
    (target_width.max(1), target_height.max(1))
}

pub(crate) fn transform_cover_image(
    path: &Path,
    options: &CoverTransformOptions,
) -> Result<TransformedCoverImage> {
    let image = image::open(path)
        .with_context(|| format!("failed to open cover image '{}'", path.display()))?;
    let source_size = image.dimensions();
    let (target_width, target_height) = target_cover_size(
        source_size.0,
        source_size.1,
        options.max_width,
        options.max_height,
    );

    let resized = if target_width == source_size.0 && target_height == source_size.1 {
        image
    } else {
        image.thumbnail(target_width, target_height)
    };

    let format = options
        .format
        .or_else(|| cover_image_format_from_path(path))
        .ok_or_else(|| anyhow!("unsupported cover image format: {}", path.display()))?;

    let mut cursor = std::io::Cursor::new(Vec::<u8>::new());
    match format {
        image::ImageFormat::Jpeg => {
            let mut encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(
                &mut cursor,
                options.quality.unwrap_or(90),
            );
            let rgb = resized.to_rgb8();
            encoder
                .encode(
                    rgb.as_raw(),
                    rgb.width(),
                    rgb.height(),
                    image::ExtendedColorType::Rgb8,
                )
                .with_context(|| {
                    format!("failed to encode cover image '{}' as JPEG", path.display())
                })?;
        }
        _ => {
            resized.write_to(&mut cursor, format).with_context(|| {
                format!(
                    "failed to encode cover image '{}' in requested format",
                    path.display()
                )
            })?;
        }
    }

    Ok(TransformedCoverImage {
        bytes: cursor.into_inner(),
        mime_type: cover_mime_from_format(format),
    })
}

pub(crate) fn cover_hash(path: &Path) -> Option<String> {
    let bytes = std::fs::read(path).ok()?;
    Some(xxh3_128_hex_bytes(&bytes))
}

pub(crate) fn cover_blurhash(path: &Path) -> Option<String> {
    let image = image::open(path).ok()?;
    let (width, height) = image.dimensions();
    if width == 0 || height == 0 {
        return None;
    }
    let small = image.thumbnail(128, 128);
    let (sw, sh) = small.dimensions();
    let bytes = small.to_rgba8();
    blurhash::encode(4, 3, sw, sh, bytes.as_raw()).ok()
}
