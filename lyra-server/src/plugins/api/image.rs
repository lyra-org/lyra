// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::io::Cursor;

use anyhow::{
    Context,
    Result,
    bail,
};
use image::{
    ExtendedColorType,
    GenericImageView,
    ImageFormat,
    codecs::jpeg::JpegEncoder,
};

pub(super) const DEFAULT_IMAGE_QUALITY: u8 = 90;

#[derive(Debug, Clone, Copy)]
pub(super) struct ParsedImageTransformOptions {
    pub(super) format: Option<ImageFormat>,
    pub(super) quality: Option<u8>,
    pub(super) max_width: Option<u32>,
    pub(super) max_height: Option<u32>,
}

pub(super) fn parse_image_transform_options(
    value: Option<&serde_json::Value>,
) -> Result<Option<ParsedImageTransformOptions>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let object = value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("image transform options must be an object"))?;
    let format = match object.get("format").and_then(serde_json::Value::as_str) {
        Some(raw_format) => Some(
            parse_image_format(raw_format)
                .ok_or_else(|| anyhow::anyhow!("unsupported image format: {raw_format}"))?,
        ),
        None => None,
    };
    let quality = parse_u8_json(object.get("quality"))?;
    let max_width = super::parse_u32_json(object.get("max_width"))?;
    let max_height = super::parse_u32_json(object.get("max_height"))?;
    if format.is_none() && quality.is_none() && max_width.is_none() && max_height.is_none() {
        return Ok(None);
    }
    Ok(Some(ParsedImageTransformOptions {
        format,
        quality,
        max_width,
        max_height,
    }))
}

pub(super) fn parse_u8_json(value: Option<&serde_json::Value>) -> Result<Option<u8>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let Some(value) = value.as_u64() else {
        bail!("expected unsigned integer option");
    };
    Ok(Some(
        u8::try_from(value).context("integer option out of range")?,
    ))
}

pub(super) fn parse_image_format(raw_format: &str) -> Option<ImageFormat> {
    match raw_format.trim().to_ascii_lowercase().as_str() {
        "jpg" => Some(ImageFormat::Jpeg),
        "png" => Some(ImageFormat::Png),
        "webp" => Some(ImageFormat::WebP),
        _ => None,
    }
}

pub(super) fn target_image_size(
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

pub(super) fn transform_image(
    path: &str,
    options: &ParsedImageTransformOptions,
) -> Result<(Vec<u8>, ImageFormat)> {
    let image = image::open(path).with_context(|| format!("failed to open image '{path}'"))?;
    let source_size = image.dimensions();
    let (target_width, target_height) = target_image_size(
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
        .or_else(|| ImageFormat::from_path(path).ok())
        .unwrap_or(ImageFormat::Jpeg);

    let mut cursor = Cursor::new(Vec::<u8>::new());
    match format {
        ImageFormat::Jpeg => {
            let mut encoder = JpegEncoder::new_with_quality(
                &mut cursor,
                options.quality.unwrap_or(DEFAULT_IMAGE_QUALITY),
            );
            let rgb = resized.to_rgb8();
            encoder
                .encode(
                    rgb.as_raw(),
                    rgb.width(),
                    rgb.height(),
                    ExtendedColorType::Rgb8,
                )
                .with_context(|| format!("failed to encode image '{path}' as JPEG"))?;
        }
        _ => {
            resized
                .write_to(&mut cursor, format)
                .with_context(|| format!("failed to encode image '{path}' as requested format"))?;
        }
    }
    Ok((cursor.into_inner(), format))
}

pub(super) fn image_format_mime(format: ImageFormat) -> &'static str {
    match format {
        ImageFormat::Jpeg => "image/jpeg",
        ImageFormat::Png => "image/png",
        ImageFormat::WebP => "image/webp",
        _ => "application/octet-stream",
    }
}
