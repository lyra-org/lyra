use std::{
    env,
    fs,
    path::PathBuf,
};

use anyhow::{
    Context,
    Result,
    anyhow,
};
use lyra_artwork::{
    ArtworkOptions,
    ArtworkSource,
    generate_gradient_artwork,
};

fn main() -> Result<()> {
    let mut args = env::args_os().skip(1).collect::<Vec<_>>();
    if args.len() < 2 {
        return Err(anyhow!(
            "usage: cargo run -p lyra-artwork --example generate -- <output.jpg> <source>..."
        ));
    }

    let output = PathBuf::from(args.remove(0));
    let mut sources = Vec::new();
    for raw_path in args {
        let path = PathBuf::from(raw_path);
        let bytes = fs::read(&path)
            .with_context(|| format!("failed to read source image {}", path.display()))?;
        let image = image::load_from_memory(&bytes)
            .with_context(|| format!("failed to decode source image {}", path.display()))?;
        sources.push(ArtworkSource {
            stable_id: path.to_string_lossy().into_owned(),
            content_hash: blake3::hash(&bytes).to_hex().to_string(),
            image,
        });
    }

    let options = ArtworkOptions::new("lyra-artwork-example");
    let generated =
        generate_gradient_artwork(&sources, &options)?.ok_or_else(|| anyhow!("no artwork generated"))?;

    fs::write(&output, &generated.bytes)
        .with_context(|| format!("failed to write {}", output.display()))?;
    println!("path={}", output.display());
    println!("mime_type={}", generated.mime_type);
    println!("hash={}", generated.hash);
    println!("blurhash={}", generated.blurhash.unwrap_or_default());
    println!("palette={:?}", generated.palette);

    Ok(())
}
