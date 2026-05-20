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
use harmony_core::{
    FunctionSpec,
    ModuleExport,
    ModuleSpec,
};
use harmony_luau as luau;
use harmony_luau::{
    DescribeInterface,
    FieldDescriptor,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    render_definition_file_with_support,
};
use image::{
    DynamicImage,
    GenericImageView,
    RgbImage,
    codecs::jpeg::JpegEncoder,
};
use serde::Deserialize;

const CACHE_DIR: &str = ".lyra/cache/images";
const DEFAULT_QUALITY: u8 = 90;

#[derive(Deserialize)]
struct ComposeOptions {
    sources: Vec<String>,
    width: Option<u32>,
    height: Option<u32>,
    quality: Option<u8>,
}

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

async fn compose_impl(options: ComposeOptions) -> Result<ComposeResult> {
    let sources: Vec<String> = options
        .sources
        .into_iter()
        .filter(|p| !p.is_empty())
        .collect();

    if sources.is_empty() {
        return Err(anyhow!("sources must be a non-empty array of file paths"));
    }

    let width: u32 = options.width.unwrap_or(600);
    let height: u32 = options.height.unwrap_or(600);
    let quality: u8 = options.quality.unwrap_or(DEFAULT_QUALITY);

    let key = cache_key(&sources, width, height, quality);
    let cache_path = cache_dir().join(format!("{key}.jpg"));

    // Check cache first.
    if cache_path.is_file() {
        if let Ok(data) = std::fs::read(&cache_path) {
            return Ok(ComposeResult {
                path: cache_path.to_string_lossy().to_string(),
                hash: file_hash(&data),
                mime_type: "image/jpeg".to_string(),
            });
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
    .map_err(|err| anyhow!("compose task failed: {err}"))??;

    Ok(ComposeResult {
        path: cache_path.to_string_lossy().to_string(),
        hash,
        mime_type: "image/jpeg".to_string(),
    })
}
fn compose_callback(
    mut frame: luau::CallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let options_table: luau::Table = frame.args.read_named("options")?;
    let options = parse_compose_options(frame.vm, &options_table)?;
    Ok(Box::pin(async move {
        let result = compose_impl(options)
            .await
            .map_err(|error| luau::Error::Runtime(error.to_string()))?;
        Ok(vec![luau::Value::TableData(result.into_luau_table())])
    }))
}
fn parse_compose_options(
    vm: &luau::Vm,
    table: &luau::Table,
) -> luau::runtime::Result<ComposeOptions> {
    Ok(ComposeOptions {
        sources: parse_sources(vm, table)?,
        width: parse_optional_u32(vm, table, "width")?,
        height: parse_optional_u32(vm, table, "height")?,
        quality: parse_optional_u8(vm, table, "quality")?,
    })
}
fn parse_sources(vm: &luau::Vm, table: &luau::Table) -> luau::runtime::Result<Vec<String>> {
    let luau::Value::Table(sources) = table.get_raw(vm, "sources")? else {
        return Err(luau::Error::Runtime(
            "images.compose options.sources must be an array".to_string(),
        ));
    };

    let mut indexed = Vec::new();
    for (key, value) in sources.pairs_raw(vm)? {
        let index = match key {
            luau::Value::Integer(value) if value > 0 => value,
            luau::Value::Number(value) if value > 0.0 => value as i64,
            _ => continue,
        };
        let luau::Value::String(value) = value else {
            return Err(luau::Error::Runtime(
                "images.compose options.sources entries must be strings".to_string(),
            ));
        };
        indexed.push((
            index,
            String::from_utf8(value).map_err(|error| {
                luau::Error::Runtime(format!(
                    "images.compose options.sources entries must be valid UTF-8: {error}"
                ))
            })?,
        ));
    }
    indexed.sort_by_key(|(index, _)| *index);
    Ok(indexed.into_iter().map(|(_, value)| value).collect())
}
fn parse_optional_u32(
    vm: &luau::Vm,
    table: &luau::Table,
    field: &'static str,
) -> luau::runtime::Result<Option<u32>> {
    match table.get_raw(vm, field)? {
        luau::Value::Nil => Ok(None),
        luau::Value::Integer(value) if value >= 0 && value <= i64::from(u32::MAX) => {
            Ok(Some(value as u32))
        }
        luau::Value::Number(value) if value >= 0.0 && value <= f64::from(u32::MAX) => {
            Ok(Some(value as u32))
        }
        other => Err(luau::Error::Runtime(format!(
            "images.compose options.{field} must be a non-negative number, got {}",
            other.type_name()
        ))),
    }
}
fn parse_optional_u8(
    vm: &luau::Vm,
    table: &luau::Table,
    field: &'static str,
) -> luau::runtime::Result<Option<u8>> {
    parse_optional_u32(vm, table, field)?.map_or(Ok(None), |value| {
        u8::try_from(value).map(Some).map_err(|_| {
            luau::Error::Runtime(format!("images.compose options.{field} must fit in u8"))
        })
    })
}

impl ComposeResult {
    fn into_luau_table(self) -> luau::OwnedTable {
        let mut table = luau::OwnedTable::with_capacity(0, 3);
        table.set_field("path", luau::Value::String(self.path.into_bytes()));
        table.set_field("hash", luau::Value::String(self.hash.into_bytes()));
        table.set_field(
            "mime_type",
            luau::Value::String(self.mime_type.into_bytes()),
        );
        table
    }
}

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/images")
        .capability("lyra.images")
        .function(compose_spec())
        .install(|_| Ok(ModuleExport::new(ImagesModule)))
}

fn compose_spec() -> FunctionSpec {
    let spec = FunctionSpec::async_fn("compose")
        .arg_name("options")
        .args::<ComposeOptions>()
        .returns::<ComposeResult>();
    spec.call_async_native(Arc::new(compose_callback))
}

impl LuauTypeInfo for ComposeOptions {
    fn luau_type() -> LuauType {
        LuauType::literal("ComposeOptions")
    }
}

impl DescribeInterface for ComposeOptions {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("ComposeOptions", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "sources",
                ty: Vec::<String>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "width",
                ty: Option::<u32>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "height",
                ty: Option::<u32>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "quality",
                ty: Option::<u8>::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for ComposeResult {
    fn luau_type() -> LuauType {
        LuauType::literal("ComposeResult")
    }
}

impl DescribeInterface for ComposeResult {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("ComposeResult", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "path",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "hash",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "mime_type",
                ty: String::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

fn module_descriptor() -> ModuleDescriptor {
    ModuleDescriptor {
        name: "Images",
        local_name: "images",
        description: None,
        fields: Vec::new(),
        functions: vec![ModuleFunctionDescriptor {
            path: vec!["compose"],
            description: Some(
                "Compose multiple source images into a single grid image. Returns a cached result with path, hash, and mime_type.",
            ),
            params: vec![ParameterDescriptor {
                name: "options",
                ty: ComposeOptions::luau_type(),
                description: None,
                variadic: false,
            }],
            returns: vec![ComposeResult::luau_type()],
            yields: true,
        }],
    }
}

pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &module_descriptor(),
        &[],
        &[
            ComposeOptions::interface_descriptor(),
            ComposeResult::interface_descriptor(),
        ],
        &[],
    )
}

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

    #[test]
    fn exposes_handwritten_module_spec() {
        let spec = module_spec();

        assert_eq!(spec.id.0.as_ref(), "lyra/images");
        assert_eq!(spec.capability.as_ref().unwrap().0.as_ref(), "lyra.images");
        assert_eq!(spec.functions.len(), 1);
        assert_eq!(spec.functions[0].name.as_ref(), "compose");
        assert!(spec.functions[0].yields);
    }

    #[test]
    fn renders_images_module_definition() {
        let rendered = render_luau_definition().expect("render lyra/images docs");

        assert!(rendered.contains("@class Images"));
        assert!(rendered.contains("@interface ComposeOptions"));
        assert!(rendered.contains("@interface ComposeResult"));
        assert!(
            rendered.contains("function images.compose(options: ComposeOptions): ComposeResult")
        );
    }
}
