//! Derived artwork generation for Lyra.
#![cfg_attr(all(test, feature = "nightly"), feature(test))]

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
    if sources.is_empty() || options.width == 0 || options.height == 0 || options.max_sources == 0 {
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

    let image = render::render_color_field(&selected, &colors, options);
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
        u128::from_le_bytes(
            hash.as_bytes()[0..16]
                .try_into()
                .expect("hash slice length"),
        )
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
        let forward = vec![
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
        Ok(())
    }
}

#[cfg(all(test, feature = "nightly"))]
mod benches {
    extern crate test;

    use image::{
        DynamicImage,
        Rgb,
        RgbImage,
    };
    use test::{
        Bencher,
        black_box,
    };

    use super::*;

    fn synthetic_cover(id: &str, base: [u8; 3], accent: [u8; 3]) -> ArtworkSource {
        let mut image = RgbImage::new(512, 512);
        for y in 0..512 {
            for x in 0..512 {
                let fx = x as f32 / 511.0;
                let fy = y as f32 / 511.0;
                let wave = ((fx * 11.0).sin() * (fy * 7.0).cos() * 0.5 + 0.5) * 0.18;
                let radial = (1.0 - ((fx - 0.72).hypot(fy - 0.28) / 0.82).min(1.0)) * 0.55;
                let blend = (fx * 0.22 + fy * 0.18 + wave + radial).clamp(0.0, 1.0);
                let rgb = [
                    mix_channel(base[0], accent[0], blend),
                    mix_channel(base[1], accent[1], blend),
                    mix_channel(base[2], accent[2], blend),
                ];
                image.put_pixel(x, y, Rgb(rgb));
            }
        }

        ArtworkSource {
            stable_id: id.to_string(),
            content_hash: format!(
                "{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                base[0], base[1], base[2], accent[0], accent[1], accent[2]
            ),
            image: DynamicImage::ImageRgb8(image),
        }
    }

    fn mix_channel(a: u8, b: u8, blend: f32) -> u8 {
        (f32::from(a) * (1.0 - blend) + f32::from(b) * blend).round() as u8
    }

    fn bench_sources() -> Vec<ArtworkSource> {
        vec![
            synthetic_cover("cover-a", [22, 28, 62], [225, 59, 109]),
            synthetic_cover("cover-b", [12, 76, 92], [243, 190, 74]),
            synthetic_cover("cover-c", [77, 32, 91], [73, 211, 164]),
            synthetic_cover("cover-d", [102, 41, 27], [96, 154, 236]),
        ]
    }

    #[bench]
    fn generate_gradient_artwork_four_sources_600(b: &mut Bencher) {
        let sources = bench_sources();
        let options = ArtworkOptions::new("genre-bench");

        b.iter(|| {
            generate_gradient_artwork(black_box(&sources), black_box(&options))
                .unwrap()
                .unwrap()
        });
    }

    #[bench]
    fn generate_gradient_artwork_four_sources_256(b: &mut Bencher) {
        let sources = bench_sources();
        let mut options = ArtworkOptions::new("genre-bench");
        options.width = 256;
        options.height = 256;

        b.iter(|| {
            generate_gradient_artwork(black_box(&sources), black_box(&options))
                .unwrap()
                .unwrap()
        });
    }
}
