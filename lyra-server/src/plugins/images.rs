// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    io::Cursor,
    path::PathBuf,
    sync::Arc,
};

use anyhow::{
    Context,
    Result,
    anyhow,
};
use harmony_core::LuaAsyncExt;
use image::{
    DynamicImage,
    GenericImageView,
    RgbImage,
    codecs::jpeg::JpegEncoder,
};
use mlua::{
    ExternalResult,
    Lua,
    Value,
};
use serde::Deserialize;

const CACHE_DIR: &str = ".lyra/cache/images";
const DEFAULT_QUALITY: u8 = 90;

#[derive(Deserialize)]
#[harmony_macros::interface]
struct ComposeOptions {
    sources: Vec<String>,
    width: Option<u32>,
    height: Option<u32>,
    quality: Option<u8>,
}

#[harmony_macros::interface]
struct ComposeResult {
    path: String,
    hash: String,
    mime_type: String,
}

fn cache_dir() -> PathBuf {
    PathBuf::from(CACHE_DIR)
}

fn ensure_cache_dir() -> Result<PathBuf> {
    let dir = cache_dir();
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("failed to create image cache dir: {}", dir.display()))?;
    Ok(dir)
}

/// Build a deterministic cache key from source paths, layout parameters,
/// quality, and source file mtimes (for invalidation on cover replacement).
fn cache_key(sources: &[String], width: u32, height: u32, quality: u8) -> String {
    let mut hasher = blake3::Hasher::new();
    for source in sources {
        hasher.update(source.as_bytes());
        hasher.update(b"\0");
        // Include mtime so the cache invalidates when a cover file is replaced.
        let mtime = std::fs::metadata(source)
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        hasher.update(&mtime.to_le_bytes());
    }
    hasher.update(&width.to_le_bytes());
    hasher.update(&height.to_le_bytes());
    hasher.update(&[quality]);
    hasher.finalize().to_hex().to_string()
}

/// Compose a grid of images from the given source paths.
fn compose_grid(sources: &[String], width: u32, height: u32, quality: u8) -> Result<Vec<u8>> {
    if sources.is_empty() {
        return Err(anyhow!("no source images provided"));
    }

    let images: Vec<DynamicImage> = sources
        .iter()
        .filter_map(|path| match image::open(path) {
            Ok(img) => Some(img),
            Err(err) => {
                tracing::warn!(path, %err, "skipping unloadable image source in compose");
                None
            }
        })
        .collect();

    if images.is_empty() {
        return Err(anyhow!("no valid source images could be loaded"));
    }

    // For 3 images, duplicate the first to fill the 4th slot.
    let mut grid_images: Vec<&DynamicImage> = images.iter().collect();
    if grid_images.len() == 3 {
        grid_images.push(&images[0]);
    }

    let count = grid_images.len();
    let (cols, rows) = match count {
        1 => (1, 1),
        2 => (2, 1),
        _ => (2, 2),
    };

    let cell_width = width / cols;
    let cell_height = height / rows;
    let mut canvas = RgbImage::new(width, height);

    for (i, img) in grid_images.iter().take((cols * rows) as usize).enumerate() {
        let col = (i as u32) % cols;
        let row = (i as u32) / cols;
        let x_offset = col * cell_width;
        let y_offset = row * cell_height;

        // Crop-to-fit: resize to cover the cell, then center-crop.
        let (src_w, src_h) = img.dimensions();
        let scale = (cell_width as f64 / src_w as f64).max(cell_height as f64 / src_h as f64);
        let scaled_w = (src_w as f64 * scale).ceil() as u32;
        let scaled_h = (src_h as f64 * scale).ceil() as u32;
        let resized = img.resize_exact(scaled_w, scaled_h, image::imageops::FilterType::Triangle);
        let crop_x = scaled_w.saturating_sub(cell_width) / 2;
        let crop_y = scaled_h.saturating_sub(cell_height) / 2;

        let rgb = resized.to_rgb8();
        for py in 0..cell_height.min(scaled_h) {
            for px in 0..cell_width.min(scaled_w) {
                let src_x = crop_x + px;
                let src_y = crop_y + py;
                if src_x < scaled_w && src_y < scaled_h {
                    let dest_x = x_offset + px;
                    let dest_y = y_offset + py;
                    if dest_x < width && dest_y < height {
                        canvas.put_pixel(dest_x, dest_y, *rgb.get_pixel(src_x, src_y));
                    }
                }
            }
        }
    }

    let mut cursor = Cursor::new(Vec::new());
    let mut encoder = JpegEncoder::new_with_quality(&mut cursor, quality);
    encoder
        .encode(
            canvas.as_raw(),
            width,
            height,
            image::ExtendedColorType::Rgb8,
        )
        .context("failed to encode composed image")?;

    Ok(cursor.into_inner())
}

/// File-content hash for ETag.
fn file_hash(data: &[u8]) -> String {
    blake3::hash(data).to_hex().to_string()
}

struct ImagesModule;

#[harmony_macros::module(
    plugin_scoped,
    name = "Images",
    local = "images",
    path = "lyra/images",
    interfaces(ComposeOptions, ComposeResult)
)]
impl ImagesModule {
    /// Compose multiple source images into a single grid image.
    /// Returns a cached result with path, hash, and mime_type.
    #[harmony(args(options: ComposeOptions), returns(ComposeResult))]
    pub(crate) async fn compose(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        options: Value,
    ) -> mlua::Result<Value> {
        let options: ComposeOptions = crate::plugins::from_lua_json_value(&lua, options)?;
        let sources: Vec<String> = options
            .sources
            .into_iter()
            .filter(|p| !p.is_empty())
            .collect();

        if sources.is_empty() {
            return Err(mlua::Error::runtime(
                "sources must be a non-empty array of file paths",
            ));
        }

        let width: u32 = options.width.unwrap_or(600);
        let height: u32 = options.height.unwrap_or(600);
        let quality: u8 = options.quality.unwrap_or(DEFAULT_QUALITY);

        let key = cache_key(&sources, width, height, quality);
        let cache_path = cache_dir().join(format!("{key}.jpg"));

        // Check cache first.
        if cache_path.is_file() {
            if let Ok(data) = std::fs::read(&cache_path) {
                let hash = file_hash(&data);
                let table = lua.create_table()?;
                table.set("path", cache_path.to_string_lossy().to_string())?;
                table.set("hash", hash)?;
                table.set("mime_type", "image/jpeg")?;
                return Ok(Value::Table(table));
            }
        }

        let sources_owned = sources.clone();
        let cache_path_owned = cache_path.clone();

        let (_data, hash) = tokio::task::spawn_blocking(move || -> Result<(Vec<u8>, String)> {
            let data = compose_grid(&sources_owned, width, height, quality)?;
            let hash = file_hash(&data);
            ensure_cache_dir()?;
            // Atomic write: write to temp file then rename to avoid
            // concurrent writers producing a corrupt cached file.
            let tmp_path =
                cache_path_owned.with_extension(format!("tmp.{:?}", std::thread::current().id()));
            std::fs::write(&tmp_path, &data).with_context(|| {
                format!(
                    "failed to write composed image to temp file: {}",
                    tmp_path.display()
                )
            })?;
            std::fs::rename(&tmp_path, &cache_path_owned).with_context(|| {
                format!(
                    "failed to rename temp file to cache: {}",
                    cache_path_owned.display()
                )
            })?;
            Ok((data, hash))
        })
        .await
        .map_err(|err| mlua::Error::runtime(format!("compose task failed: {err}")))?
        .into_lua_err()?;

        let table = lua.create_table()?;
        table.set("path", cache_path.to_string_lossy().to_string())?;
        table.set("hash", hash)?;
        table.set("mime_type", "image/jpeg")?;
        Ok(Value::Table(table))
    }
}

crate::plugins::plugin_surface_exports!(
    ImagesModule,
    "lyra.images",
    "Read and manipulate images.",
    Low
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_key_is_deterministic() {
        let sources = vec!["a.jpg".to_string(), "b.jpg".to_string()];
        let key1 = cache_key(&sources, 600, 600, 90);
        let key2 = cache_key(&sources, 600, 600, 90);
        assert_eq!(key1, key2);
    }

    #[test]
    fn cache_key_differs_for_different_sources() {
        let a = cache_key(&["a.jpg".to_string()], 600, 600, 90);
        let b = cache_key(&["b.jpg".to_string()], 600, 600, 90);
        assert_ne!(a, b);
    }

    #[test]
    fn cache_key_differs_for_different_quality() {
        let sources = vec!["a.jpg".to_string()];
        let a = cache_key(&sources, 600, 600, 90);
        let b = cache_key(&sources, 600, 600, 75);
        assert_ne!(a, b);
    }

    #[test]
    fn cache_key_differs_for_different_dimensions() {
        let sources = vec!["a.jpg".to_string()];
        let a = cache_key(&sources, 600, 600, 90);
        let b = cache_key(&sources, 300, 300, 90);
        assert_ne!(a, b);
    }
}
