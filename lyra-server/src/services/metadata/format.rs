// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! A source file's container and codec, from what lofty read in its contents.

use lofty::{
    file::FileType,
    iff::{
        aiff::{
            AiffCompressionType,
            AiffProperties,
        },
        wav::{
            WavFormat,
            WavProperties,
        },
    },
    mp4::{
        Mp4Codec,
        Mp4Properties,
    },
    mpeg::{
        Layer,
        MpegProperties,
    },
    musepack::MpcProperties,
};

use crate::db::track_sources::{
    SourceCodec,
    SourceContainer,
    SourceFormat,
};

fn format(container: SourceContainer, codec: Option<SourceCodec>) -> SourceFormat {
    SourceFormat {
        container: Some(container),
        codec,
    }
}

/// The format of a file type whose codec the type alone settles. Types that carry several codecs
/// get only their container; read their properties with the functions below instead.
pub(super) fn of_file_type(file_type: FileType) -> SourceFormat {
    match file_type {
        FileType::Aac => format(SourceContainer::Aac, Some(SourceCodec::Aac)),
        FileType::Aiff => format(SourceContainer::Aiff, None),
        FileType::Ape => format(SourceContainer::Ape, Some(SourceCodec::Ape)),
        FileType::Flac => format(SourceContainer::Flac, Some(SourceCodec::Flac)),
        FileType::Mpeg => format(SourceContainer::Mp3, None),
        FileType::Mp4 => format(SourceContainer::M4a, None),
        FileType::Mpc => format(SourceContainer::Mpc, None),
        FileType::Opus => format(SourceContainer::Ogg, Some(SourceCodec::Opus)),
        FileType::Vorbis => format(SourceContainer::Ogg, Some(SourceCodec::Vorbis)),
        FileType::Speex => format(SourceContainer::Ogg, Some(SourceCodec::Speex)),
        FileType::Wav => format(SourceContainer::Wav, None),
        FileType::WavPack => format(SourceContainer::WavPack, Some(SourceCodec::WavPack)),
        _ => SourceFormat::default(),
    }
}

pub(super) fn of_mp4(properties: &Mp4Properties) -> SourceFormat {
    let codec = properties.codec().and_then(|codec| match codec {
        Mp4Codec::AAC => Some(SourceCodec::Aac),
        Mp4Codec::ALAC => Some(SourceCodec::Alac),
        Mp4Codec::MP3 => Some(SourceCodec::Mp3),
        Mp4Codec::FLAC => Some(SourceCodec::Flac),
        _ => None,
    });
    format(SourceContainer::M4a, codec)
}

pub(super) fn of_mpeg(properties: &MpegProperties) -> SourceFormat {
    let codec = match properties.layer() {
        Layer::Layer1 => SourceCodec::Mp1,
        Layer::Layer2 => SourceCodec::Mp2,
        Layer::Layer3 => SourceCodec::Mp3,
    };
    format(SourceContainer::Mp3, Some(codec))
}

pub(super) fn of_wav(properties: &WavProperties) -> SourceFormat {
    let codec = match (properties.format(), properties.bit_depth()) {
        (WavFormat::PCM, 8) => Some(SourceCodec::PcmU8),
        (WavFormat::PCM, 16) => Some(SourceCodec::PcmS16Le),
        (WavFormat::PCM, 24) => Some(SourceCodec::PcmS24Le),
        (WavFormat::PCM, 32) => Some(SourceCodec::PcmS32Le),
        (WavFormat::IEEE_FLOAT, 32) => Some(SourceCodec::PcmF32Le),
        (WavFormat::IEEE_FLOAT, 64) => Some(SourceCodec::PcmF64Le),
        _ => None,
    };
    format(SourceContainer::Wav, codec)
}

pub(super) fn of_aiff(properties: &AiffProperties) -> SourceFormat {
    let little_endian = match properties.compression_type() {
        None | Some(AiffCompressionType::None) => false,
        Some(AiffCompressionType::sowt) => true,
        Some(_) => return format(SourceContainer::Aiff, None),
    };
    let codec = match (properties.sample_size(), little_endian) {
        (8, _) => Some(SourceCodec::PcmS8),
        (16, false) => Some(SourceCodec::PcmS16Be),
        (16, true) => Some(SourceCodec::PcmS16Le),
        (24, false) => Some(SourceCodec::PcmS24Be),
        (24, true) => Some(SourceCodec::PcmS24Le),
        (32, false) => Some(SourceCodec::PcmS32Be),
        (32, true) => Some(SourceCodec::PcmS32Le),
        _ => None,
    };
    format(SourceContainer::Aiff, codec)
}

pub(super) fn of_mpc(properties: &MpcProperties) -> SourceFormat {
    let codec = match properties {
        MpcProperties::Sv8(_) => Some(SourceCodec::Musepack8),
        MpcProperties::Sv7(_) => Some(SourceCodec::Musepack7),
        MpcProperties::Sv4to6(_) => None,
    };
    format(SourceContainer::Mpc, codec)
}

#[cfg(test)]
mod tests {
    use std::path::{
        Path,
        PathBuf,
    };

    use crate::db::track_sources::{
        SourceCodec,
        SourceContainer,
        SourceFormat,
    };
    use crate::services::metadata::read_source_format;

    fn asset(name: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/assets/metadata")
            .join(name)
    }

    fn wav(format_tag: u16, bits: u16) -> Vec<u8> {
        let channels = 2u16;
        let rate = 44_100u32;
        let block_align = channels * bits / 8;
        let data = vec![0u8; block_align as usize * 64];
        let mut fmt = Vec::new();
        fmt.extend(format_tag.to_le_bytes());
        fmt.extend(channels.to_le_bytes());
        fmt.extend(rate.to_le_bytes());
        fmt.extend((rate * u32::from(block_align)).to_le_bytes());
        fmt.extend(block_align.to_le_bytes());
        fmt.extend(bits.to_le_bytes());
        let mut body = b"WAVE".to_vec();
        body.extend(b"fmt ");
        body.extend((fmt.len() as u32).to_le_bytes());
        body.extend(fmt);
        body.extend(b"data");
        body.extend((data.len() as u32).to_le_bytes());
        body.extend(data);
        let mut file = b"RIFF".to_vec();
        file.extend((body.len() as u32).to_le_bytes());
        file.extend(body);
        file
    }

    fn aiff(bits: u16) -> Vec<u8> {
        let channels = 2u16;
        let frames = 64u32;
        let mut comm = Vec::new();
        comm.extend(channels.to_be_bytes());
        comm.extend(frames.to_be_bytes());
        comm.extend(bits.to_be_bytes());
        // 44100 as an 80-bit IEEE 754 extended float.
        comm.extend([0x40, 0x0E, 0xAC, 0x44, 0, 0, 0, 0, 0, 0]);
        let data = vec![0u8; (frames * u32::from(channels * bits / 8)) as usize];
        let mut body = b"AIFF".to_vec();
        body.extend(b"COMM");
        body.extend((comm.len() as u32).to_be_bytes());
        body.extend(comm);
        body.extend(b"SSND");
        body.extend((data.len() as u32 + 8).to_be_bytes());
        body.extend([0u8; 8]);
        body.extend(data);
        let mut file = b"FORM".to_vec();
        file.extend((body.len() as u32).to_be_bytes());
        file.extend(body);
        file
    }

    fn read_written(name: &str, bytes: &[u8]) -> anyhow::Result<SourceFormat> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join(name);
        std::fs::write(&path, bytes)?;
        read_source_format(&path)
    }

    fn expect(container: SourceContainer, codec: SourceCodec) -> SourceFormat {
        SourceFormat {
            container: Some(container),
            codec: Some(codec),
        }
    }

    #[test]
    fn reads_format_from_contents() -> anyhow::Result<()> {
        assert_eq!(
            read_source_format(&asset("integration_track.flac"))?,
            expect(SourceContainer::Flac, SourceCodec::Flac)
        );
        assert_eq!(
            read_source_format(&asset("integration_track.opus"))?,
            expect(SourceContainer::Ogg, SourceCodec::Opus)
        );
        Ok(())
    }

    #[test]
    fn ignores_the_extension() -> anyhow::Result<()> {
        let bytes = std::fs::read(asset("integration_track.opus"))?;
        assert_eq!(
            read_written("mislabeled.mp3", &bytes)?,
            expect(SourceContainer::Ogg, SourceCodec::Opus)
        );
        Ok(())
    }

    #[test]
    fn reads_pcm_layout_from_wav_and_aiff() -> anyhow::Result<()> {
        assert_eq!(
            read_written("a.wav", &wav(1, 16))?,
            expect(SourceContainer::Wav, SourceCodec::PcmS16Le)
        );
        assert_eq!(
            read_written("b.wav", &wav(1, 24))?,
            expect(SourceContainer::Wav, SourceCodec::PcmS24Le)
        );
        assert_eq!(
            read_written("c.wav", &wav(3, 32))?,
            expect(SourceContainer::Wav, SourceCodec::PcmF32Le)
        );
        assert_eq!(
            read_written("d.aiff", &aiff(16))?,
            expect(SourceContainer::Aiff, SourceCodec::PcmS16Be)
        );
        assert_eq!(
            read_written("e.aiff", &aiff(24))?,
            expect(SourceContainer::Aiff, SourceCodec::PcmS24Be)
        );
        Ok(())
    }
}
