// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod cache;
mod header;

use std::{
    fs::File,
    io::{
        BufReader,
        Cursor,
        Read,
        Seek,
    },
    num::NonZeroUsize,
    path::Path,
    sync::{
        Arc,
        LazyLock,
    },
    time::Duration,
};

use anyhow::{
    Context,
    Result,
    bail,
    ensure,
};
use image::GenericImageView;
use tokio::sync::Semaphore;

pub(crate) use self::cache::{
    TransformCache,
    forget_cover_transforms,
};
use self::{
    cache::Variant,
    header::{
        CoverHeader,
        read_cover_header,
    },
};
use super::{
    CoverRendition,
    CoverTransformError,
    CoverTransformOptions,
};

pub(crate) const COVER_EXTENSIONS: [&str; 4] = ["jpg", "jpeg", "png", "webp"];

fn normalize_extension(ext: &str) -> Option<&'static str> {
    match ext.to_ascii_lowercase().as_str() {
        "jpg" => Some("jpg"),
        "jpeg" => Some("jpeg"),
        "png" => Some("png"),
        "webp" => Some("webp"),
        _ => None,
    }
}

fn image_format_from_extension(ext: &str) -> Option<image::ImageFormat> {
    match ext {
        "jpg" | "jpeg" => Some(image::ImageFormat::Jpeg),
        "png" => Some(image::ImageFormat::Png),
        "webp" => Some(image::ImageFormat::WebP),
        _ => None,
    }
}

fn mime_from_extension(ext: &str) -> &'static str {
    match ext {
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        "webp" => "image/webp",
        _ => "application/octet-stream",
    }
}

fn extension_from_image_format(format: image::ImageFormat) -> Option<&'static str> {
    match format {
        image::ImageFormat::Jpeg => Some("jpg"),
        image::ImageFormat::Png => Some("png"),
        image::ImageFormat::WebP => Some("webp"),
        _ => None,
    }
}

/// The largest cover file read, downloaded or uploaded.
pub(crate) const MAX_COVER_BYTES: usize = 20 * 1024 * 1024;
/// The most pixels a cover decodes with, about 6300x6300.
const MAX_COVER_PIXELS: u64 = 40 * 1024 * 1024;
/// What the PNG decoder may keep of the chunks beside the pixels, such as a compressed colour
/// profile or text, which it would otherwise expand without bound.
const PNG_METADATA_BYTES: u64 = 8 * 1024 * 1024;

/// The header of a cover image of `len` bytes, refused when the image is over the cover limits.
fn bounded_cover_header<R: Read + Seek>(reader: &mut R, len: u64) -> Result<CoverHeader> {
    ensure!(
        len <= MAX_COVER_BYTES as u64,
        "the image is {len} bytes, over the {MAX_COVER_BYTES} byte limit"
    );
    let header = read_cover_header(reader)?;
    let pixels = u64::from(header.width) * u64::from(header.height);
    ensure!(
        pixels <= MAX_COVER_PIXELS,
        "the image is {}x{}, over the {MAX_COVER_PIXELS} pixel limit",
        header.width,
        header.height
    );
    Ok(header)
}

fn open_cover_file(path: &Path) -> Result<(File, u64)> {
    let file = File::open(path)
        .with_context(|| format!("failed to open cover image '{}'", path.display()))?;
    let len = file
        .metadata()
        .with_context(|| format!("failed to read cover image '{}'", path.display()))?
        .len();
    Ok((file, len))
}

/// The header of a cover file, read without the rest of it.
fn probe_cover_file(path: &Path) -> Result<CoverHeader> {
    let (file, len) = open_cover_file(path)?;
    bounded_cover_header(&mut BufReader::new(file), len)
        .with_context(|| format!("failed to read cover image '{}'", path.display()))
}

/// The bytes of a cover file no larger than the cover size limit.
fn read_cover_file(path: &Path) -> Result<Vec<u8>> {
    let (file, len) = open_cover_file(path)?;
    ensure!(
        len <= MAX_COVER_BYTES as u64,
        "cover image '{}' is {len} bytes, over the {MAX_COVER_BYTES} byte limit",
        path.display()
    );
    let mut bytes = Vec::new();
    file.take(MAX_COVER_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .with_context(|| format!("failed to read cover image '{}'", path.display()))?;
    Ok(bytes)
}

/// Decodes a JPEG, PNG or WebP image within the cover limits, which its header must meet before
/// any of it decodes.
fn decode_cover(bytes: &[u8]) -> Result<(CoverHeader, image::DynamicImage)> {
    let header = bounded_cover_header(&mut Cursor::new(bytes), bytes.len() as u64)?;
    let mut limits = image::Limits::default();
    limits.max_image_width = Some(header.width);
    limits.max_image_height = Some(header.height);
    let decoded = match header.format {
        image::ImageFormat::Png => {
            png_decoder(bytes, limits).and_then(image::DynamicImage::from_decoder)
        }
        format => {
            let mut reader = image::ImageReader::with_format(Cursor::new(bytes), format);
            reader.limits(limits);
            reader.decode()
        }
    };
    let image =
        decoded.with_context(|| format!("cover image does not decode as {:?}", header.format))?;
    Ok((header, image))
}

/// The PNG decoder takes `max_alloc` as its allowance for the chunks beside the pixels.
fn png_decoder(
    bytes: &[u8],
    mut limits: image::Limits,
) -> image::ImageResult<image::codecs::png::PngDecoder<Cursor<&[u8]>>> {
    limits.max_alloc = Some(PNG_METADATA_BYTES);
    image::codecs::png::PngDecoder::with_limits(Cursor::new(bytes), limits)
}

/// The extension for a cover image read from its bytes, whatever its source
/// claims it is. The bytes must decode in full as a JPEG, PNG or WebP image
/// within the cover limits.
pub(crate) fn validate_cover_image(bytes: &[u8]) -> Result<&'static str> {
    let (header, _) = decode_cover(bytes)?;
    Ok(extension_from_image_format(header.format).expect("cover formats have extensions"))
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

/// Why a cover that meets the cover limits is served as it is rather than transformed: the
/// decoders size animation frames and lossless entropy codes from the bitstream alone.
fn transform_refusal(header: &CoverHeader) -> Option<&'static str> {
    if header.animated {
        Some("the image is animated")
    } else if header.lossless {
        Some("the image is lossless WebP")
    } else {
        None
    }
}

fn transform_cover_file(path: &Path, options: &CoverTransformOptions) -> Result<Vec<u8>> {
    let bytes = read_cover_file(path)?;
    let (header, image) = decode_cover(&bytes)
        .with_context(|| format!("failed to decode cover image '{}'", path.display()))?;
    drop(bytes);
    if let Some(refusal) = transform_refusal(&header) {
        bail!("{refusal}");
    }
    let (target_width, target_height) = target_cover_size(
        header.width,
        header.height,
        options.max_width,
        options.max_height,
    );

    let resized = if (target_width, target_height) == image.dimensions() {
        image
    } else {
        image.thumbnail(target_width, target_height)
    };

    let format = options.format.unwrap_or(header.format);
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

    Ok(cursor.into_inner())
}

/// Transforms run on at most one blocking thread per two cores across the server, leaving the
/// rest for streaming and scans.
static TRANSFORM_SLOTS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| {
    let cores = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
    Arc::new(Semaphore::new((cores / 2).max(1)))
});

/// How long a transform waits for a slot before the request is refused as busy.
const TRANSFORM_SLOT_WAIT: Duration = Duration::from_secs(5);

/// The cover file with content `hash` as the options ask for it. The file itself is served when
/// they would change nothing, and when the image is over the cover limits, would not transform
/// safely, or fails to.
pub(crate) async fn render_cover(
    path: &Path,
    hash: &str,
    options: CoverTransformOptions,
) -> Result<CoverRendition, CoverTransformError> {
    let cache = TransformCache::current();
    render_cover_on(
        &cache,
        &TRANSFORM_SLOTS,
        TRANSFORM_SLOT_WAIT,
        path,
        hash,
        options,
    )
    .await
}

/// A cached variant is served without a slot. The slot and the variant's flight move into the
/// blocking task, so a caller that stops waiting frees neither while the work still runs.
async fn render_cover_on(
    cache: &Arc<TransformCache>,
    slots: &Arc<Semaphore>,
    wait: Duration,
    path: &Path,
    hash: &str,
    options: CoverTransformOptions,
) -> Result<CoverRendition, CoverTransformError> {
    let source_path = path.to_path_buf();
    let probed = tokio::task::spawn_blocking(move || probe_cover_file(&source_path))
        .await
        .context("cover probe task failed")?;
    let header = match probed {
        Ok(header) => header,
        Err(err) => {
            tracing::debug!(error = %format!("{err:#}"), "serving cover untransformed");
            return Ok(CoverRendition::Original {
                mime_type: cover_mime_from_path(path),
            });
        }
    };
    let original = || CoverRendition::Original {
        mime_type: cover_mime_from_format(header.format),
    };
    if let Some(refusal) = transform_refusal(&header) {
        tracing::debug!(path = %path.display(), refusal, "serving cover untransformed");
        return Ok(original());
    }
    let Some(variant) = Variant::of(&header, &options) else {
        return Ok(original());
    };
    let Some(cached) = cache.path(hash, &variant) else {
        tracing::warn!(
            hash,
            "serving cover untransformed: its hash names no cache entry"
        );
        return Ok(original());
    };
    let mime_type = cover_mime_from_format(variant.format);
    let served = |(file, len)| CoverRendition::Cached {
        file,
        len,
        mime_type,
    };
    // What a request serves without transforming: the stored variant, or the original when the
    // variant failed before, nothing can be stored, or the stored one cannot be opened.
    let settled = || {
        let (cache, cached) = (Arc::clone(cache), cached.clone());
        async move {
            let opened = tokio::task::spawn_blocking({
                let (cache, cached) = (Arc::clone(&cache), cached.clone());
                move || cache.open(&cached)
            })
            .await
            .context("cover cache task failed")
            .and_then(|opened| opened.context("failed to open a transformed cover"));
            match opened {
                Ok(Some(stored)) => Some(served(stored)),
                Ok(None) if cache.failed(&cached) || cache.unwritable() => Some(original()),
                Ok(None) => None,
                Err(err) => {
                    tracing::warn!(error = %format!("{err:#}"), "serving cover untransformed");
                    Some(original())
                }
            }
        }
    };
    if let Some(rendition) = settled().await {
        return Ok(rendition);
    }
    let flight = cache.flight(&cached).await;
    if let Some(rendition) = settled().await {
        return Ok(rendition);
    }

    let permit = tokio::time::timeout(wait, Arc::clone(slots).acquire_owned())
        .await
        .map_err(|_| CoverTransformError::Busy)?
        .expect("the cover transform semaphore is never closed");
    let source_path = path.to_path_buf();
    let target = cached.clone();
    let task_cache = Arc::clone(cache);
    let transformed = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let _flight = flight;
        let bytes = transform_cover_file(&source_path, &variant.options()).inspect_err(|_| {
            task_cache.record_failure(&target);
        })?;
        let len = bytes.len() as u64;
        // A failed write is logged by the cache, once for its backoff.
        anyhow::Ok(
            task_cache
                .store(&target, &bytes)
                .ok()
                .map(|file| (file, len)),
        )
    })
    .await
    .context("cover transform task failed")?;
    match transformed {
        Ok(Some(stored)) => Ok(served(stored)),
        Ok(None) => Ok(original()),
        Err(err) => {
            tracing::warn!(error = %format!("{err:#}"), "serving cover untransformed");
            Ok(original())
        }
    }
}

pub(crate) fn read_cover_hash(path: &Path) -> anyhow::Result<String> {
    let bytes =
        std::fs::read(path).with_context(|| format!("failed to read cover {}", path.display()))?;
    Ok(xxh3_128_hex_bytes(&bytes))
}

pub(crate) fn cover_blurhash(path: &Path) -> Option<String> {
    if transform_refusal(&probe_cover_file(path).ok()?).is_some() {
        return None;
    }
    let (_, image) = decode_cover(&read_cover_file(path).ok()?).ok()?;
    let small = image.thumbnail(128, 128);
    let (sw, sh) = small.dimensions();
    let bytes = small.to_rgba8();
    blurhash::encode(4, 3, sw, sh, bytes.as_raw()).ok()
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::{
        header::fixtures::*,
        *,
    };

    fn encoded(format: image::ImageFormat, width: u32, height: u32) -> Vec<u8> {
        let mut cursor = std::io::Cursor::new(Vec::new());
        image::DynamicImage::new_rgb8(width, height)
            .write_to(&mut cursor, format)
            .expect("encode test image");
        cursor.into_inner()
    }

    fn write_cover(dir: &tempfile::TempDir, name: &str, bytes: &[u8]) -> std::path::PathBuf {
        let path = dir.path().join(name);
        std::fs::write(&path, bytes).expect("write test cover");
        path
    }

    fn options(
        format: Option<image::ImageFormat>,
        max_width: Option<u32>,
        max_height: Option<u32>,
    ) -> CoverTransformOptions {
        CoverTransformOptions {
            format,
            quality: Some(50),
            max_width,
            max_height,
        }
    }

    const HASH: &str = "0123456789abcdef0123456789abcdef";

    async fn render_on(
        cache: &Arc<TransformCache>,
        slots: &Arc<Semaphore>,
        path: &Path,
        options: CoverTransformOptions,
    ) -> Result<CoverRendition, CoverTransformError> {
        render_cover_on(cache, slots, Duration::from_millis(10), path, HASH, options).await
    }

    /// Renders with one free slot, which a transform must leave free again.
    async fn render(
        cache: &Arc<TransformCache>,
        path: &Path,
        options: CoverTransformOptions,
    ) -> CoverRendition {
        let slots = Arc::new(Semaphore::new(1));
        let rendered = render_on(cache, &slots, path, options)
            .await
            .expect("renders");
        assert_eq!(slots.available_permits(), 1);
        assert_eq!(cache::testing::flights(cache), 0);
        rendered
    }

    fn is_original(rendition: &CoverRendition, mime: &str) -> bool {
        matches!(rendition, CoverRendition::Original { mime_type } if *mime_type == mime)
    }

    /// The bytes of a cached rendition, read from the handle it was served with.
    fn cached_bytes(rendition: CoverRendition) -> Option<Vec<u8>> {
        match rendition {
            CoverRendition::Cached { mut file, len, .. } => {
                let mut bytes = Vec::new();
                file.read_to_end(&mut bytes).expect("reads cached cover");
                assert_eq!(bytes.len() as u64, len);
                Some(bytes)
            }
            CoverRendition::Original { .. } => None,
        }
    }

    #[test]
    fn validate_cover_image_reads_the_format_from_the_bytes() {
        for (format, extension) in [
            (image::ImageFormat::Jpeg, "jpg"),
            (image::ImageFormat::Png, "png"),
            (image::ImageFormat::WebP, "webp"),
        ] {
            assert_eq!(
                validate_cover_image(&encoded(format, 1, 1)).ok(),
                Some(extension)
            );
        }
    }

    #[test]
    fn validate_cover_image_rejects_other_content() {
        for bytes in [
            &b"GIF89a\x01\x00\x01\x00"[..],
            b"<!doctype html><title>x</title>",
            b"{\"secret\":true}",
            b"",
        ] {
            assert!(validate_cover_image(bytes).is_err(), "{bytes:?}");
        }
    }

    #[test]
    fn validate_cover_image_rejects_image_signatures_over_other_content() {
        let mut fake_jpeg = vec![0xFF, 0xD8, 0xFF];
        fake_jpeg.extend_from_slice(b"secret data, not an image");
        let mut fake_png = encoded(image::ImageFormat::Png, 1, 1)[..33].to_vec();
        fake_png.extend_from_slice(b"secret data, not an image");
        let png = encoded(image::ImageFormat::Png, 64, 64);
        let truncated_png = &png[..png.len() / 2];

        for bytes in [&fake_jpeg[..], &fake_png, truncated_png] {
            assert!(validate_cover_image(bytes).is_err(), "{bytes:?}");
        }
    }

    #[test]
    fn covers_over_the_pixel_limit_are_refused_from_their_headers() {
        let dir = tempfile::tempdir().expect("tempdir");
        for (name, bytes) in [
            ("tall.png", png_header(1, 50_000_000)),
            ("wide.jpg", jpeg_header(6_500, 6_500)),
            ("big.webp", webp(&[(b"VP8 ", &vp8_frame(16_000, 16_000))])),
        ] {
            let path = write_cover(&dir, name, &bytes);
            assert!(validate_cover_image(&bytes).is_err(), "{name}");
            assert!(probe_cover_file(&path).is_err(), "{name}");
            assert!(cover_blurhash(&path).is_none(), "{name}");
        }
        assert!(validate_cover_image(&jpeg_header(6_000, 6_000)).is_err());
        assert!(bounded_cover_header(&mut Cursor::new(jpeg_header(6_000, 6_000)), 64).is_ok());
    }

    #[test]
    fn blurhash_skips_covers_that_transforms_serve_as_they_are() {
        let dir = tempfile::tempdir().expect("tempdir");
        let png = write_cover(&dir, "cover.png", &encoded(image::ImageFormat::Png, 4, 4));
        let lossless = write_cover(&dir, "cover.webp", &encoded(image::ImageFormat::WebP, 4, 4));

        assert!(cover_blurhash(&png).is_some());
        assert!(probe_cover_file(&lossless).expect("reads header").lossless);
        assert!(decode_cover(&std::fs::read(&lossless).expect("read")).is_ok());
        assert!(cover_blurhash(&lossless).is_none());
    }

    #[test]
    fn cover_files_over_the_size_limit_are_refused_before_they_are_read() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_cover(&dir, "cover.png", &encoded(image::ImageFormat::Png, 4, 4));
        File::options()
            .write(true)
            .open(&path)
            .and_then(|file| file.set_len(MAX_COVER_BYTES as u64 + 1))
            .expect("extend test cover");

        assert!(probe_cover_file(&path).is_err());
        assert!(read_cover_file(&path).is_err());
        assert!(cover_blurhash(&path).is_none());
    }

    #[test]
    fn png_metadata_beyond_its_allowance_is_dropped() {
        let mut profile = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::best());
        profile
            .write_all(&vec![0; PNG_METADATA_BYTES as usize + 1])
            .expect("compress profile");
        let mut iccp = b"icc\0\0".to_vec();
        iccp.extend_from_slice(&profile.finish().expect("compress profile"));
        let png = encoded(image::ImageFormat::Png, 4, 4);
        let mut bytes = png[..33].to_vec();
        png_chunk(&mut bytes, b"iCCP", &iccp);
        bytes.extend_from_slice(&png[33..]);

        use image::ImageDecoder;
        let unbounded = image::codecs::png::PngDecoder::new(Cursor::new(&bytes))
            .and_then(|mut decoder| decoder.icc_profile())
            .expect("reads profile");
        let bounded = png_decoder(&bytes, image::Limits::default())
            .and_then(|mut decoder| decoder.icc_profile())
            .expect("reads profile");

        assert!(unbounded.is_some_and(|profile| profile.len() as u64 > PNG_METADATA_BYTES));
        assert!(bounded.is_none());
        assert!(validate_cover_image(&bytes).is_ok());
    }

    #[test]
    fn cover_files_are_read_by_their_content_not_their_extension() {
        let dir = tempfile::tempdir().expect("tempdir");
        let png = write_cover(&dir, "cover.jpg", &encoded(image::ImageFormat::Png, 4, 2));
        let text = write_cover(&dir, "folder.jpg", b"secret data, not an image");

        let header = probe_cover_file(&png).expect("probes");
        assert_eq!(
            (header.format, header.width, header.height),
            (image::ImageFormat::Png, 4, 2)
        );
        let resized =
            transform_cover_file(&png, &options(None, Some(2), None)).expect("transforms");
        assert_eq!(
            read_cover_header(&mut Cursor::new(resized))
                .map(|header| header.format)
                .ok(),
            Some(image::ImageFormat::Png)
        );
        assert!(probe_cover_file(&text).is_err());
        assert!(cover_blurhash(&text).is_none());
    }

    #[tokio::test]
    async fn covers_that_would_not_transform_safely_are_served_as_they_are() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = cache::testing::cache(&dir.path().join("cache"), 1 << 20);
        let mut apng = png_start(8, 8);
        png_chunk(&mut apng, b"acTL", &[0, 0, 0, 2, 0, 0, 0, 0]);
        png_chunk(&mut apng, b"IDAT", &[]);
        let canvas = vp8x(0, 8, 8);
        let mismatched_frame = webp(&[(b"VP8X", &canvas), (b"VP8 ", &vp8_frame(16_000, 16_000))]);
        let late_lossless_alpha = webp(&[
            (b"VP8X", &vp8x(0b1_0000, 8, 8)),
            (b"VP8 ", &vp8_frame(8, 8)),
            (b"ALPH", &[1]),
        ]);
        let png = encoded(image::ImageFormat::Png, 8, 8);
        let truncated_png = &png[..png.len() / 2];

        for (name, bytes, mime) in [
            ("apng.png", &apng[..], "image/png"),
            (
                "animated.webp",
                &webp(&[(b"VP8X", &vp8x(0b10, 8, 8))]),
                "image/webp",
            ),
            (
                "lossless.webp",
                &encoded(image::ImageFormat::WebP, 8, 8),
                "image/webp",
            ),
            (
                "extended-lossless.webp",
                &webp(&[(b"VP8X", &canvas), (b"VP8L", &vp8l_header(8, 8))]),
                "image/webp",
            ),
            ("mismatched.webp", &mismatched_frame, "image/webp"),
            ("late-alpha.webp", &late_lossless_alpha, "image/webp"),
            ("huge.jpg", &jpeg_header(7_000, 7_000), "image/jpeg"),
            ("truncated.png", truncated_png, "image/png"),
            ("text.jpg", b"secret data, not an image", "image/jpeg"),
        ] {
            let path = write_cover(&dir, name, bytes);
            let rendered = render(
                &cache,
                &path,
                options(Some(image::ImageFormat::Jpeg), Some(4), None),
            )
            .await;
            assert!(is_original(&rendered, mime), "{name}");
        }
    }

    #[tokio::test]
    async fn failed_transforms_are_not_tried_again() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = cache::testing::cache(&dir.path().join("cache"), 1 << 20);
        let png = encoded(image::ImageFormat::Png, 200, 200);
        let path = write_cover(&dir, "cover.png", &png[..png.len() / 2]);
        let resize = || options(None, Some(100), None);

        assert!(is_original(
            &render(&cache, &path, resize()).await,
            "image/png"
        ));
        let no_slots = Arc::new(Semaphore::new(0));
        let again = render_on(&cache, &no_slots, &path, resize()).await;
        assert!(matches!(again, Ok(ref rendered) if is_original(rendered, "image/png")));
    }

    #[tokio::test]
    async fn cached_transforms_are_served_without_a_slot() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = cache::testing::cache(&dir.path().join("cache"), 1 << 20);
        let path = write_cover(
            &dir,
            "cover.png",
            &encoded(image::ImageFormat::Png, 200, 200),
        );

        let rendered = render(&cache, &path, options(None, Some(100), None)).await;
        let served = cached_bytes(rendered).expect("is cached");
        let stored = dir.path().join("cache").join(HASH).join("128x0.png");
        assert_eq!(std::fs::read(&stored).ok(), Some(served));
        let header = probe_cover_file(&stored).expect("stored a cover");
        assert_eq!((header.width, header.height), (128, 128));

        let no_slots = Arc::new(Semaphore::new(0));
        let again = render_on(&cache, &no_slots, &path, options(None, Some(120), None))
            .await
            .expect("renders");
        assert_eq!(cached_bytes(again), std::fs::read(&stored).ok());
    }

    #[tokio::test]
    async fn concurrent_misses_of_one_variant_transform_once() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = cache::testing::cache(&dir.path().join("cache"), 1 << 20);
        let path = write_cover(
            &dir,
            "cover.png",
            &encoded(image::ImageFormat::Png, 200, 200),
        );
        let slots = Arc::new(Semaphore::new(1));
        let held = Arc::clone(&slots).acquire_owned().await.expect("slot");

        let spawn = |wait: Duration| {
            let (cache, slots, path) = (Arc::clone(&cache), Arc::clone(&slots), path.clone());
            tokio::spawn(async move {
                let options = options(None, Some(100), None);
                render_cover_on(&cache, &slots, wait, &path, HASH, options).await
            })
        };
        let first = spawn(Duration::from_secs(10));
        tokio::time::sleep(Duration::from_millis(50)).await;
        // Waiting for a slot of its own, the second would be refused as busy.
        let second = spawn(Duration::from_millis(10));
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(held);

        for rendered in [first.await, second.await] {
            let rendered = rendered.expect("joins").expect("renders");
            assert!(cached_bytes(rendered).is_some());
        }
        assert_eq!(cache::testing::flights(&cache), 0);
        assert_eq!(slots.available_permits(), 1);
    }

    #[tokio::test]
    async fn noop_renders_serve_the_file_without_a_transform_slot() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = cache::testing::cache(&dir.path().join("cache"), 1 << 20);
        let path = write_cover(
            &dir,
            "cover.png",
            &encoded(image::ImageFormat::Png, 200, 100),
        );
        let no_slots = Arc::new(Semaphore::new(0));

        for noop in [
            options(None, None, None),
            options(Some(image::ImageFormat::Png), Some(200), None),
            options(None, Some(150), Some(100)),
            options(None, Some(4096), None),
        ] {
            let rendered = render_on(&cache, &no_slots, &path, noop.clone()).await;
            assert!(
                matches!(rendered, Ok(ref rendered) if is_original(rendered, "image/png")),
                "{noop:?}"
            );
        }
    }

    #[tokio::test]
    async fn transforms_without_a_free_slot_are_busy() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = cache::testing::cache(&dir.path().join("cache"), 1 << 20);
        let path = write_cover(
            &dir,
            "cover.png",
            &encoded(image::ImageFormat::Png, 200, 200),
        );
        let no_slots = Arc::new(Semaphore::new(0));
        let resize = || options(None, Some(100), None);

        let busy = render_on(&cache, &no_slots, &path, resize()).await;
        assert!(matches!(busy, Err(CoverTransformError::Busy)));
        assert_eq!(cache::testing::flights(&cache), 0);
        let rendered = render(&cache, &path, resize()).await;
        assert!(cached_bytes(rendered).is_some());
    }

    #[tokio::test]
    async fn variants_evicted_after_they_are_found_are_still_served() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = cache::testing::cache(&dir.path().join("cache"), 1);
        let path = write_cover(
            &dir,
            "cover.png",
            &encoded(image::ImageFormat::Png, 200, 200),
        );
        let small = || options(None, Some(64), None);

        let first = render(&cache, &path, small()).await;
        let found = render(&cache, &path, small()).await;
        // Stored past the cap, the next variant evicts the one both requests hold.
        let next = render(&cache, &path, options(None, Some(128), None)).await;
        let evicted = dir.path().join("cache").join(HASH).join("64x0.png");
        assert!(!evicted.exists());

        for rendered in [first, found, next] {
            let bytes = cached_bytes(rendered).expect("is cached");
            assert!(read_cover_header(&mut Cursor::new(bytes)).is_ok());
        }
        let again = render(&cache, &path, small()).await;
        assert!(cached_bytes(again).is_some());
    }

    #[tokio::test]
    async fn failed_writes_stop_transforms_for_the_backoff() {
        let dir = tempfile::tempdir().expect("tempdir");
        let blocked = dir.path().join("cache");
        std::fs::write(&blocked, b"a file where the cache directory goes").expect("block");
        let path = write_cover(
            &dir,
            "cover.png",
            &encoded(image::ImageFormat::Png, 200, 200),
        );
        let no_slots = Arc::new(Semaphore::new(0));
        let resize = || options(None, Some(100), None);

        let cache = cache::testing::cache_with_backoff(&blocked, Duration::from_secs(3600));
        assert!(is_original(
            &render(&cache, &path, resize()).await,
            "image/png"
        ));
        let other = options(None, Some(50), None);
        for options in [resize(), other] {
            let again = render_on(&cache, &no_slots, &path, options).await;
            assert!(matches!(again, Ok(ref rendered) if is_original(rendered, "image/png")));
        }

        let expired = cache::testing::cache_with_backoff(&blocked, Duration::ZERO);
        assert!(is_original(
            &render(&expired, &path, resize()).await,
            "image/png"
        ));
        let retried = render_on(&expired, &no_slots, &path, resize()).await;
        assert!(matches!(retried, Err(CoverTransformError::Busy)));
    }
}
