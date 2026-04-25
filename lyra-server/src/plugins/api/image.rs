// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::io::Cursor;

use anyhow::{
    Context,
    Result,
};
use image::{
    ExtendedColorType,
    GenericImageView,
    ImageFormat,
    codecs::jpeg::JpegEncoder,
};
use mlua::Table;

const DEFAULT_IMAGE_QUALITY: u8 = 90;

#[derive(Debug, Clone, Copy)]
pub(crate) struct ParsedImageTransformOptions {
    pub(crate) format: Option<ImageFormat>,
    pub(crate) quality: Option<u8>,
    pub(crate) max_width: Option<u32>,
    pub(crate) max_height: Option<u32>,
}

pub(super) fn parse_image_format(raw_format: &str) -> Option<ImageFormat> {
    let normalized = raw_format.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "jpg" => Some(ImageFormat::Jpeg),
        "png" => Some(ImageFormat::Png),
        "webp" => Some(ImageFormat::WebP),
        _ => None,
    }
}

pub(super) fn parse_image_transform_options(
    options: Option<Table>,
) -> mlua::Result<Option<ParsedImageTransformOptions>> {
    let Some(options) = options else {
        return Ok(None);
    };

    let format = match options.get::<Option<String>>("format")? {
        Some(raw_format) => {
            let Some(format) = parse_image_format(&raw_format) else {
                return Err(mlua::Error::runtime(format!(
                    "unsupported image format: {raw_format}"
                )));
            };
            Some(format)
        }
        None => None,
    };

    let quality = options.get::<Option<u8>>("quality")?;
    let max_width = options.get::<Option<u32>>("max_width")?;
    let max_height = options.get::<Option<u32>>("max_height")?;

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

fn target_image_size(
    source_width: u32,
    source_height: u32,
    max_width: Option<u32>,
    max_height: Option<u32>,
) -> (u32, u32) {
    let has_max_width = max_width.is_some();
    let has_max_height = max_height.is_some();
    if !has_max_width && !has_max_height {
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
