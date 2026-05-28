//! Derived artwork generation for Lyra.

mod palette;
mod render;

use anyhow::Result;
use image::DynamicImage;

const DEFAULT_SIZE: u32 = 600;
const DEFAULT_MAX_SOURCES: usize = 4;
const MIME_JPEG: &str = "image/jpeg";

#[derive(Clone)]
pub struct ArtworkSource {
    pub stable_id: String,
    pub content_hash: String,
    pub image: DynamicImage,
}

pub struct ArtworkOptions {
    pub seed: String,
    pub width: u32,
    pub height: u32,
    pub renderer_version: u32,
    pub max_sources: usize,
}

impl ArtworkOptions {
    pub fn new(seed: impl Into<String>) -> Self {
        Self {
            seed: seed.into(),
            width: DEFAULT_SIZE,
            height: DEFAULT_SIZE,
            renderer_version: 1,
            max_sources: DEFAULT_MAX_SOURCES,
        }
    }
}

pub struct GeneratedArtwork {
    pub bytes: Vec<u8>,
    pub mime_type: &'static str,
    pub hash: String,
    pub blurhash: Option<String>,
    pub palette: Vec<[u8; 3]>,
}

pub fn generate_gradient_artwork(
    sources: &[ArtworkSource],
    options: &ArtworkOptions,
) -> Result<Option<GeneratedArtwork>> {
    if sources.is_empty() || options.width == 0 || options.height == 0 || options.max_sources == 0
    {
        return Ok(None);
    }

    let selected = select_sources(sources, options);
    if selected.is_empty() {
        return Ok(None);
    }

    let colors = palette::select_palette(&selected, &options.seed);
    if colors.is_empty() {
        return Ok(None);
    }

    let image = render::render_gradient(&colors, options);
    let bytes = render::encode_jpeg(&image, 92)?;
    let hash = blake3::hash(&bytes).to_hex().to_string();
    let blurhash = render::image_blurhash(&image);

    Ok(Some(GeneratedArtwork {
        bytes,
        mime_type: MIME_JPEG,
        hash,
        blurhash,
        palette: colors.into_iter().map(|color| color.rgb).collect(),
    }))
}

fn select_sources<'a>(
    sources: &'a [ArtworkSource],
    options: &ArtworkOptions,
) -> Vec<&'a ArtworkSource> {
    let mut keyed = sources.iter().collect::<Vec<_>>();
    keyed.sort_by(|a, b| {
        a.stable_id
            .cmp(&b.stable_id)
            .then_with(|| a.content_hash.cmp(&b.content_hash))
    });

    if keyed.len() <= options.max_sources {
        return keyed;
    }

    keyed.sort_by_key(|source| {
        let mut hasher = blake3::Hasher::new();
        hasher.update(options.seed.as_bytes());
        hasher.update(&options.renderer_version.to_le_bytes());
        hasher.update(source.stable_id.as_bytes());
        hasher.update(source.content_hash.as_bytes());
        let hash = hasher.finalize();
        u128::from_le_bytes(hash.as_bytes()[0..16].try_into().expect("hash slice length"))
    });
    keyed.truncate(options.max_sources);
    keyed
}

#[cfg(test)]
mod tests {
    use super::*;
    use image::{
        DynamicImage,
        Rgb,
        RgbImage,
    };

    fn solid_source(id: &str, rgb: [u8; 3]) -> ArtworkSource {
        let image = RgbImage::from_pixel(32, 32, Rgb(rgb));
        ArtworkSource {
            stable_id: id.to_string(),
            content_hash: format!("{:02x}{:02x}{:02x}", rgb[0], rgb[1], rgb[2]),
            image: DynamicImage::ImageRgb8(image),
        }
    }

    #[test]
    fn empty_sources_return_none() -> Result<()> {
        let result = generate_gradient_artwork(&[], &ArtworkOptions::new("genre"))?;

        assert!(result.is_none());
        Ok(())
    }

    #[test]
    fn single_source_still_generates_artwork() -> Result<()> {
        let sources = vec![solid_source("a", [220, 40, 80])];
        let result = generate_gradient_artwork(&sources, &ArtworkOptions::new("genre"))?
            .expect("expected artwork");

        assert_eq!(result.mime_type, "image/jpeg");
        assert_eq!(&result.bytes[0..2], &[0xff, 0xd8]);
        assert!(result.blurhash.is_some());
        assert!(result.palette.len() >= 3);
        Ok(())
    }

    #[test]
    fn output_is_deterministic_for_same_inputs() -> Result<()> {
        let sources = vec![
            solid_source("a", [220, 40, 80]),
            solid_source("b", [20, 160, 220]),
            solid_source("c", [240, 200, 80]),
            solid_source("d", [80, 220, 120]),
        ];
        let options = ArtworkOptions::new("genre");

        let first = generate_gradient_artwork(&sources, &options)?.expect("expected artwork");
        let second = generate_gradient_artwork(&sources, &options)?.expect("expected artwork");

        assert_eq!(first.hash, second.hash);
        assert_eq!(first.bytes, second.bytes);
        assert_eq!(first.blurhash, second.blurhash);
        assert_eq!(first.palette, second.palette);
        Ok(())
    }

    #[test]
    fn source_order_does_not_change_output() -> Result<()> {
        let mut forward = vec![
            solid_source("a", [220, 40, 80]),
            solid_source("b", [20, 160, 220]),
            solid_source("c", [240, 200, 80]),
            solid_source("d", [80, 220, 120]),
        ];
        let mut reversed = forward.clone();
        reversed.reverse();
        let options = ArtworkOptions::new("genre");

        let first = generate_gradient_artwork(&forward, &options)?.expect("expected artwork");
        let second = generate_gradient_artwork(&reversed, &options)?.expect("expected artwork");

        assert_eq!(first.hash, second.hash);
        assert_eq!(first.palette, second.palette);
        forward.reverse();
        assert_ne!(forward[0].stable_id, reversed[0].stable_id);
        Ok(())
    }
}
