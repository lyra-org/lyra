// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Cover image headers, read without the image data: the JPEG decoder reads its whole input before
//! it looks at the header.

use std::io::{
    self,
    Read,
    Seek,
    SeekFrom,
};

use anyhow::{
    Context,
    Result,
    anyhow,
    bail,
    ensure,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct CoverHeader {
    pub(super) format: image::ImageFormat,
    pub(super) width: u32,
    pub(super) height: u32,
    /// An APNG or animated WebP.
    pub(super) animated: bool,
    /// A WebP whose image or alpha is lossless-coded.
    pub(super) lossless: bool,
}

const PNG_SIGNATURE: [u8; 8] = *b"\x89PNG\r\n\x1a\n";

/// The header of the JPEG, PNG or WebP image `reader` starts with.
pub(super) fn read_cover_header<R: Read + Seek>(reader: &mut R) -> Result<CoverHeader> {
    let magic: [u8; 12] =
        read_array(reader).map_err(|_| anyhow!("not a JPEG, PNG or WebP image"))?;
    let header = if magic.starts_with(&[0xFF, 0xD8, 0xFF]) {
        read_jpeg(reader)?
    } else if magic.starts_with(&PNG_SIGNATURE) {
        read_png(reader, &magic)?
    } else if &magic[..4] == b"RIFF" && &magic[8..] == b"WEBP" {
        let riff_end = 8 + u64::from(u32::from_le_bytes(magic[4..8].try_into()?));
        read_webp(reader, riff_end)?
    } else {
        bail!("not a JPEG, PNG or WebP image");
    };
    ensure!(
        header.width > 0 && header.height > 0,
        "the image has no pixels"
    );
    Ok(header)
}

fn read_array<const N: usize, R: Read>(reader: &mut R) -> Result<[u8; N]> {
    let mut bytes = [0u8; N];
    reader
        .read_exact(&mut bytes)
        .context("the image header is truncated")?;
    Ok(bytes)
}

fn still(format: image::ImageFormat, width: u32, height: u32) -> CoverHeader {
    CoverHeader {
        format,
        width,
        height,
        animated: false,
        lossless: false,
    }
}

fn read_jpeg<R: Read + Seek>(reader: &mut R) -> Result<CoverHeader> {
    reader.seek(SeekFrom::Start(2))?;
    loop {
        // Stray bytes between segments are skipped, as lenient decoders do, then fill bytes.
        let [mut marker] = read_array(reader)?;
        while marker != 0xFF {
            [marker] = read_array(reader)?;
        }
        while marker == 0xFF {
            [marker] = read_array(reader)?;
        }
        match marker {
            0x00 | 0x01 | 0xD0..=0xD7 => continue,
            0xD8..=0xDA => bail!("the JPEG has no frame header before its first scan"),
            _ => {}
        }
        let length = u16::from_be_bytes(read_array(reader)?);
        ensure!(length >= 2, "the JPEG has a malformed segment");
        if matches!(marker, 0xC0..=0xCF) && !matches!(marker, 0xC4 | 0xC8 | 0xCC) {
            ensure!(length >= 8, "the JPEG frame header is truncated");
            let [_precision, h1, h0, w1, w0] = read_array(reader)?;
            return Ok(still(
                image::ImageFormat::Jpeg,
                u32::from(u16::from_be_bytes([w1, w0])),
                u32::from(u16::from_be_bytes([h1, h0])),
            ));
        }
        reader.seek_relative(i64::from(length) - 2)?;
    }
}

/// The PNG's header chunk, and whether an animation control chunk comes before its image data.
fn read_png<R: Read + Seek>(reader: &mut R, magic: &[u8; 12]) -> Result<CoverHeader> {
    let ihdr: [u8; 21] = read_array(reader)?;
    ensure!(
        magic[8..] == 13u32.to_be_bytes() && &ihdr[..4] == b"IHDR",
        "the PNG does not start with its header chunk"
    );
    let mut header = still(
        image::ImageFormat::Png,
        u32::from_be_bytes(ihdr[4..8].try_into()?),
        u32::from_be_bytes(ihdr[8..12].try_into()?),
    );
    loop {
        let [l0, l1, l2, l3, t0, t1, t2, t3] = read_array(reader)?;
        match &[t0, t1, t2, t3] {
            b"acTL" => header.animated = true,
            b"IDAT" | b"IEND" => return Ok(header),
            _ => {}
        }
        reader.seek_relative(i64::from(u32::from_be_bytes([l0, l1, l2, l3])) + 4)?;
    }
}

const WEBP_ANIMATION: u8 = 0b0000_0010;
const WEBP_ALPHA_LOSSLESS: u8 = 1;

fn read_webp<R: Read + Seek>(reader: &mut R, riff_end: u64) -> Result<CoverHeader> {
    let (fourcc, len) = read_chunk_header(reader).context("the image header is truncated")?;
    match &fourcc {
        b"VP8 " => {
            let (width, height) = read_vp8_frame(reader, len)?;
            Ok(still(image::ImageFormat::WebP, width, height))
        }
        b"VP8L" => {
            let (width, height) = read_vp8l_header(reader, len)?;
            Ok(CoverHeader {
                lossless: true,
                ..still(image::ImageFormat::WebP, width, height)
            })
        }
        b"VP8X" => read_extended_webp(reader, len, riff_end),
        _ => bail!("the WebP has no image chunk"),
    }
}

/// An extended WebP's canvas and every top-level chunk, since the decoder reads more than the
/// first image chunk: it takes the first chunk of each kind from anywhere in the file, and those
/// of the first animation frame even when the canvas is not animated. Any frame or lossless chunk
/// marks the image, and the first lossy frame, the one decoded, must fill the canvas: the decoder
/// decodes it at the size its own header gives before it compares the two.
fn read_extended_webp<R: Read + Seek>(
    reader: &mut R,
    vp8x_len: u32,
    riff_end: u64,
) -> Result<CoverHeader> {
    ensure!(vp8x_len >= 10, "the WebP extended header is truncated");
    let [flags, _, _, _, w0, w1, w2, h0, h1, h2] = read_array(reader)?;
    let mut header = still(
        image::ImageFormat::WebP,
        u32::from_le_bytes([w0, w1, w2, 0]) + 1,
        u32::from_le_bytes([h0, h1, h2, 0]) + 1,
    );
    if flags & WEBP_ANIMATION != 0 {
        header.animated = true;
        return Ok(header);
    }
    reader.seek_relative(padded(vp8x_len) as i64 - 10)?;
    let mut position = 12 + 8 + padded(vp8x_len);
    let mut has_image = false;
    let mut has_frame = false;
    while position + 8 <= riff_end {
        // The decoder ends the chunks where the file does, whatever the RIFF size says.
        let (fourcc, len) = match read_chunk_header(reader) {
            Ok(chunk) => chunk,
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err.into()),
        };
        let consumed = match &fourcc {
            b"VP8 " if !has_frame => {
                let frame = read_vp8_frame(reader, len)?;
                ensure!(
                    frame == (header.width, header.height),
                    "the WebP image is {}x{}, not its {}x{} canvas",
                    frame.0,
                    frame.1,
                    header.width,
                    header.height
                );
                has_frame = true;
                has_image = true;
                VP8_FRAME_HEADER_LEN
            }
            b"VP8 " => 0,
            b"VP8L" => {
                header.lossless = true;
                has_image = true;
                0
            }
            b"ALPH" => {
                ensure!(len >= 1, "the WebP alpha chunk is empty");
                let [alpha] = read_array(reader)?;
                header.lossless |= alpha & 0b11 == WEBP_ALPHA_LOSSLESS;
                1
            }
            b"ANMF" => {
                header.animated = true;
                0
            }
            _ => 0,
        };
        reader.seek_relative(padded(len) as i64 - consumed)?;
        position += 8 + padded(len);
    }
    ensure!(has_image, "the WebP has no image chunk");
    Ok(header)
}

fn padded(len: u32) -> u64 {
    u64::from(len) + u64::from(len & 1)
}

fn read_chunk_header<R: Read>(reader: &mut R) -> io::Result<([u8; 4], u32)> {
    let mut bytes = [0u8; 8];
    reader.read_exact(&mut bytes)?;
    let [f0, f1, f2, f3, l0, l1, l2, l3] = bytes;
    Ok(([f0, f1, f2, f3], u32::from_le_bytes([l0, l1, l2, l3])))
}

const VP8_FRAME_HEADER_LEN: i64 = 10;

fn read_vp8_frame<R: Read>(reader: &mut R, len: u32) -> Result<(u32, u32)> {
    ensure!(len >= 10, "the WebP frame header is truncated");
    let [tag, _, _, s0, s1, s2, w0, w1, h0, h1] = read_array(reader)?;
    ensure!(
        tag & 1 == 0 && [s0, s1, s2] == [0x9D, 0x01, 0x2A],
        "the WebP does not start with a key frame"
    );
    Ok((
        u32::from(u16::from_le_bytes([w0, w1]) & 0x3FFF),
        u32::from(u16::from_le_bytes([h0, h1]) & 0x3FFF),
    ))
}

fn read_vp8l_header<R: Read>(reader: &mut R, len: u32) -> Result<(u32, u32)> {
    ensure!(len >= 5, "the lossless WebP header is truncated");
    let [signature, b0, b1, b2, b3] = read_array(reader)?;
    ensure!(signature == 0x2F, "the lossless WebP signature is invalid");
    let bits = u32::from_le_bytes([b0, b1, b2, b3]);
    Ok(((bits & 0x3FFF) + 1, ((bits >> 14) & 0x3FFF) + 1))
}

#[cfg(test)]
pub(super) mod fixtures {
    //! Image headers without their image data, for sizes too large to encode in a test.

    use super::PNG_SIGNATURE;

    pub(in super::super) fn png_chunk(bytes: &mut Vec<u8>, fourcc: &[u8; 4], payload: &[u8]) {
        bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        bytes.extend_from_slice(fourcc);
        bytes.extend_from_slice(payload);
        let mut crc = flate2::Crc::new();
        crc.update(fourcc);
        crc.update(payload);
        bytes.extend_from_slice(&crc.sum().to_be_bytes());
    }

    /// A PNG's signature and header chunk.
    pub(in super::super) fn png_start(width: u32, height: u32) -> Vec<u8> {
        let mut bytes = PNG_SIGNATURE.to_vec();
        let mut ihdr = width.to_be_bytes().to_vec();
        ihdr.extend_from_slice(&height.to_be_bytes());
        ihdr.extend_from_slice(&[8, 2, 0, 0, 0]);
        png_chunk(&mut bytes, b"IHDR", &ihdr);
        bytes
    }

    /// A PNG that stops at its image data.
    pub(in super::super) fn png_header(width: u32, height: u32) -> Vec<u8> {
        let mut bytes = png_start(width, height);
        png_chunk(&mut bytes, b"IDAT", &[]);
        bytes
    }

    pub(in super::super) fn jpeg_header(width: u16, height: u16) -> Vec<u8> {
        let mut bytes = vec![0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x04, 0x00, 0x00];
        bytes.extend_from_slice(&[0xFF, 0xC2, 0x00, 0x0B, 8]);
        bytes.extend_from_slice(&height.to_be_bytes());
        bytes.extend_from_slice(&width.to_be_bytes());
        bytes.extend_from_slice(&[1, 1, 0x11, 0]);
        bytes
    }

    /// A WebP file of `chunks`, each a fourcc and its payload.
    pub(in super::super) fn webp(chunks: &[(&[u8; 4], &[u8])]) -> Vec<u8> {
        let mut body = b"WEBP".to_vec();
        for (fourcc, payload) in chunks {
            body.extend_from_slice(*fourcc);
            body.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            body.extend_from_slice(payload);
            if payload.len() % 2 == 1 {
                body.push(0);
            }
        }
        let mut bytes = b"RIFF".to_vec();
        bytes.extend_from_slice(&(body.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&body);
        bytes
    }

    pub(in super::super) fn vp8x(flags: u8, width: u32, height: u32) -> Vec<u8> {
        let mut payload = vec![flags, 0, 0, 0];
        payload.extend_from_slice(&(width - 1).to_le_bytes()[..3]);
        payload.extend_from_slice(&(height - 1).to_le_bytes()[..3]);
        payload
    }

    /// The header of a lossy key frame, without the frame.
    pub(in super::super) fn vp8_frame(width: u16, height: u16) -> Vec<u8> {
        let mut payload = vec![0x10, 0x02, 0x00, 0x9D, 0x01, 0x2A];
        payload.extend_from_slice(&width.to_le_bytes());
        payload.extend_from_slice(&height.to_le_bytes());
        payload
    }

    pub(in super::super) fn vp8l_header(width: u32, height: u32) -> Vec<u8> {
        let bits = (width - 1) | ((height - 1) << 14);
        let mut payload = vec![0x2F];
        payload.extend_from_slice(&bits.to_le_bytes());
        payload
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::{
        fixtures::*,
        *,
    };

    fn header_of(bytes: &[u8]) -> Result<CoverHeader> {
        read_cover_header(&mut Cursor::new(bytes))
    }

    fn encoded(image: image::DynamicImage, format: image::ImageFormat) -> Vec<u8> {
        let mut cursor = Cursor::new(Vec::new());
        image
            .write_to(&mut cursor, format)
            .expect("encode test image");
        cursor.into_inner()
    }

    #[test]
    fn headers_of_encoded_images_match_their_pixels() {
        let image = image::DynamicImage::new_rgb8(6, 4);
        for (format, lossless) in [
            (image::ImageFormat::Jpeg, false),
            (image::ImageFormat::Png, false),
            (image::ImageFormat::WebP, true),
        ] {
            assert_eq!(
                header_of(&encoded(image.clone(), format)).expect("reads header"),
                CoverHeader {
                    lossless,
                    ..still(format, 6, 4)
                }
            );
        }
    }

    #[test]
    fn headers_alone_give_large_dimensions() {
        assert_eq!(
            header_of(&jpeg_header(60_000, 50_000)).ok(),
            Some(still(image::ImageFormat::Jpeg, 60_000, 50_000))
        );
        assert_eq!(
            header_of(&png_header(100_000, 100_000)).ok(),
            Some(still(image::ImageFormat::Png, 100_000, 100_000))
        );
        assert_eq!(
            header_of(&webp(&[(b"VP8 ", &vp8_frame(16_000, 16_000))])).ok(),
            Some(still(image::ImageFormat::WebP, 16_000, 16_000))
        );
    }

    #[test]
    fn animations_are_marked() {
        let mut apng = png_start(4, 4);
        png_chunk(&mut apng, b"acTL", &[0, 0, 0, 2, 0, 0, 0, 0]);
        png_chunk(&mut apng, b"IDAT", &[]);
        let animated_webp = webp(&[(b"VP8X", &vp8x(WEBP_ANIMATION, 4, 4))]);

        for bytes in [apng, animated_webp] {
            assert!(header_of(&bytes).expect("reads header").animated);
        }
    }

    #[test]
    fn lossless_webp_images_are_marked() {
        let canvas = vp8x(0, 4, 4);
        for (bytes, lossless) in [
            (webp(&[(b"VP8L", &vp8l_header(4, 4))]), true),
            (
                webp(&[(b"VP8X", &canvas), (b"VP8L", &vp8l_header(4, 4))]),
                true,
            ),
            (
                webp(&[
                    (b"VP8X", &canvas),
                    (b"ALPH", &[WEBP_ALPHA_LOSSLESS]),
                    (b"VP8 ", &vp8_frame(4, 4)),
                ]),
                true,
            ),
            (
                webp(&[
                    (b"VP8X", &canvas),
                    (b"ALPH", &[0]),
                    (b"VP8 ", &vp8_frame(4, 4)),
                ]),
                false,
            ),
            (webp(&[(b"VP8 ", &vp8_frame(4, 4))]), false),
        ] {
            assert_eq!(header_of(&bytes).expect("reads header").lossless, lossless);
        }
    }

    #[test]
    fn chunks_after_the_first_image_chunk_mark_the_image() {
        const WEBP_ALPHA: u8 = 0b0001_0000;
        let canvas = vp8x(WEBP_ALPHA, 4, 4);
        let frame = vp8_frame(4, 4);
        let mut anmf = vec![0; 16];
        anmf.extend_from_slice(b"VP8L");
        anmf.extend_from_slice(&5u32.to_le_bytes());
        anmf.extend_from_slice(&vp8l_header(4, 4));
        anmf.push(0);

        let alpha_after = webp(&[
            (b"VP8X", &canvas),
            (b"VP8 ", &frame),
            (b"ALPH", &[WEBP_ALPHA_LOSSLESS]),
        ]);
        let lossless_after = webp(&[
            (b"VP8X", &canvas),
            (b"VP8 ", &frame),
            (b"VP8L", &vp8l_header(4, 4)),
        ]);
        let frame_after = webp(&[(b"VP8X", &canvas), (b"VP8 ", &frame), (b"ANMF", &anmf)]);

        assert!(header_of(&alpha_after).expect("reads header").lossless);
        assert!(header_of(&lossless_after).expect("reads header").lossless);
        assert!(header_of(&frame_after).expect("reads header").animated);
    }

    #[test]
    fn extended_webp_chunks_end_with_the_file() {
        let mut bytes = webp(&[(b"VP8X", &vp8x(0, 4, 4)), (b"VP8 ", &vp8_frame(4, 4))]);
        let riff_size = u32::from_le_bytes(bytes[4..8].try_into().expect("riff size"));
        bytes[4..8].copy_from_slice(&(riff_size + 64).to_le_bytes());

        assert_eq!(
            header_of(&bytes).ok(),
            Some(still(image::ImageFormat::WebP, 4, 4))
        );
    }

    #[test]
    fn extended_webp_frames_must_fill_their_canvas() {
        let fits = webp(&[
            (b"VP8X", &vp8x(0, 64, 64)),
            (b"ICCP", b"profile"),
            (b"VP8 ", &vp8_frame(64, 64)),
        ]);
        let larger = webp(&[
            (b"VP8X", &vp8x(0, 64, 64)),
            (b"VP8 ", &vp8_frame(16_000, 16_000)),
        ]);

        assert_eq!(
            header_of(&fits).ok(),
            Some(still(image::ImageFormat::WebP, 64, 64))
        );
        assert!(header_of(&larger).is_err());
    }

    #[test]
    fn other_content_has_no_header() {
        for bytes in [
            &b"GIF89a\x01\x00\x01\x00\x00\x00"[..],
            b"\xFF\xD8\xFF secret data, not an image",
            &png_start(4, 4),
            &webp(&[(b"VP8X", &vp8x(0, 4, 4))]),
            b"",
        ] {
            assert!(header_of(bytes).is_err(), "{bytes:?}");
        }
    }
}
