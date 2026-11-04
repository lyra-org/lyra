// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The options plugins give `stream_track`, `download_track` and `hls_playlist` responses, and
//! `playback_sources.decide`.

use crate::{
    plugins::{
        args,
        executor::ApiResponseKind,
    },
    services::delivery::{
        HlsPlaylistOptions,
        ServeTrackOptions,
    },
};
use harmony_luau as luau;
use harmony_luau::StringEnum;
#[cfg(feature = "docgen")]
use harmony_luau::{
    DescribeTypeAlias,
    FieldDescriptor,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
    TypeAliasDescriptor,
};

#[harmony_macros::string_enum(description = "An audio format Lyra delivers.")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DeliveryFormat {
    Mp3,
    Flac,
    Wav,
    Ogg,
    Webm,
    Aac,
    M4a,
    Opus,
    Aiff,
    Alac,
    Caf,
    Wma,
}

#[harmony_macros::string_enum(
    description = "An audio codec Lyra delivers; \"copy\" keeps the source's audio as it is."
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DeliveryCodec {
    Copy,
    Mp3,
    Flac,
    Aac,
    Alac,
    Opus,
    Vorbis,
    #[harmony(rename = "pcm_s16le")]
    PcmS16Le,
    #[harmony(rename = "pcm_s16be")]
    PcmS16Be,
    #[harmony(rename = "pcm_s24le")]
    PcmS24Le,
    #[harmony(rename = "pcm_s24be")]
    PcmS24Be,
    Wma,
}

const SERVE_FIELDS: &[&str] = &[
    "format",
    "preferred_codecs",
    "bitrate_bps",
    "sample_rate_hz",
    "channels",
    "bit_depth",
    "prefer_vbr",
    "start_offset_ms",
];

const STREAM_FIELDS: &[&str] = &[
    "format",
    "preferred_codecs",
    "bitrate_bps",
    "sample_rate_hz",
    "channels",
    "bit_depth",
    "prefer_vbr",
    "start_offset_ms",
    "wait_for_transcode",
    "public",
];

const HLS_FIELDS: &[&str] = &[
    "preferred_codecs",
    "bitrate_bps",
    "sample_rate_hz",
    "channels",
    "prefer_vbr",
    "start_offset_ms",
];

/// A `stream_track` response's options: whether it skips the caller's track access check, whether
/// it waits for its transcode to complete, and what it serves.
#[derive(Clone, Debug, Default)]
pub(crate) struct TrackStreamRequest {
    pub(crate) public: bool,
    pub(crate) wait_for_transcode: bool,
    pub(crate) options: ServeTrackOptions,
}

/// The options of a track response, read as it will be served.
#[derive(Clone, Debug)]
pub(crate) enum TrackResponseOptions {
    Stream(TrackStreamRequest),
    Download(ServeTrackOptions),
    Hls(HlsPlaylistOptions),
}

/// `options`, the options table of `owner`, such as `stream_track options`; nil reads as no
/// options.
fn options_table(options: &luau::Value, owner: &str) -> luau::runtime::Result<Option<luau::Table>> {
    match options {
        luau::Value::Nil => Ok(None),
        luau::Value::Table(table) => Ok(Some(table.clone())),
        other => Err(crate::plugins::runtime_error(format!(
            "{owner} must be a table, got {}",
            other.type_name()
        ))),
    }
}

/// The options of a `stream_track` response, or `playback_sources.decide`, read from `options`;
/// `owner` names them in errors.
pub(crate) fn stream_request(
    options: &luau::Value,
    owner: &str,
) -> luau::runtime::Result<TrackStreamRequest> {
    let Some(table) = options_table(options, owner)? else {
        return Ok(TrackStreamRequest::default());
    };
    args::reject_unknown_fields(&table, owner, STREAM_FIELDS)?;
    Ok(TrackStreamRequest {
        public: args::optional_bool(&table, "public", owner)?.unwrap_or(false),
        wait_for_transcode: args::optional_bool(&table, "wait_for_transcode", owner)?
            .unwrap_or(false),
        options: serve_options(&table, owner)?,
    })
}

/// The options of a `download_track` response, read from `options`.
pub(crate) fn download_options(
    options: &luau::Value,
    owner: &str,
) -> luau::runtime::Result<ServeTrackOptions> {
    let Some(table) = options_table(options, owner)? else {
        return Ok(ServeTrackOptions::default());
    };
    args::reject_unknown_fields(&table, owner, SERVE_FIELDS)?;
    serve_options(&table, owner)
}

/// The options of an `hls_playlist` response, read from `options`.
pub(crate) fn hls_options(
    options: &luau::Value,
    owner: &str,
) -> luau::runtime::Result<HlsPlaylistOptions> {
    let Some(table) = options_table(options, owner)? else {
        return Ok(HlsPlaylistOptions::default());
    };
    args::reject_unknown_fields(&table, owner, HLS_FIELDS)?;
    Ok(HlsPlaylistOptions {
        codec: codecs(&table, owner)?,
        bitrate_bps: optional_u32(&table, "bitrate_bps", owner)?,
        sample_rate_hz: optional_u32(&table, "sample_rate_hz", owner)?,
        channels: optional_u32(&table, "channels", owner)?,
        prefer_vbr: args::optional_bool(&table, "prefer_vbr", owner)?,
        start_offset_ms: optional_u64(&table, "start_offset_ms", owner)?,
    })
}

/// The options of a track response of `kind`, read from `options`.
pub(crate) fn track_response_options(
    kind: ApiResponseKind,
    options: &luau::Value,
) -> luau::runtime::Result<TrackResponseOptions> {
    let owner = format!("{kind} options");
    match kind {
        ApiResponseKind::StreamTrack => {
            stream_request(options, &owner).map(TrackResponseOptions::Stream)
        }
        ApiResponseKind::DownloadTrack => {
            download_options(options, &owner).map(TrackResponseOptions::Download)
        }
        ApiResponseKind::HlsPlaylist => hls_options(options, &owner).map(TrackResponseOptions::Hls),
        other => Err(crate::plugins::runtime_error(format!(
            "{other} is not a track response"
        ))),
    }
}

fn serve_options(table: &luau::Table, owner: &str) -> luau::runtime::Result<ServeTrackOptions> {
    Ok(ServeTrackOptions {
        format: args::optional_string_enum::<DeliveryFormat>(table, "format", owner)?
            .map(|format| format.as_str().to_string()),
        codec: codecs(table, owner)?,
        copy_profiles: None,
        bitrate_bps: optional_u32(table, "bitrate_bps", owner)?,
        sample_rate_hz: optional_u32(table, "sample_rate_hz", owner)?,
        channels: optional_u32(table, "channels", owner)?,
        bit_depth: optional_u32(table, "bit_depth", owner)?,
        prefer_vbr: args::optional_bool(table, "prefer_vbr", owner)?,
        start_offset_ms: optional_u64(table, "start_offset_ms", owner)?,
    })
}

fn optional_u64(table: &luau::Table, key: &str, owner: &str) -> luau::runtime::Result<Option<u64>> {
    let Some(value) = args::optional_i64(table, key, owner)? else {
        return Ok(None);
    };
    u64::try_from(value).map(Some).map_err(|_| {
        crate::plugins::runtime_error(format!(
            "{owner}.{key} must be a non-negative integer, got {value}"
        ))
    })
}

fn optional_u32(table: &luau::Table, key: &str, owner: &str) -> luau::runtime::Result<Option<u32>> {
    let Some(value) = optional_u64(table, key, owner)? else {
        return Ok(None);
    };
    u32::try_from(value).map(Some).map_err(|_| {
        crate::plugins::runtime_error(format!("{owner}.{key} must be at most {}", u32::MAX))
    })
}

/// `preferred_codecs` in order, joined as delivery reads them; none for an empty list.
fn codecs(table: &luau::Table, owner: &str) -> luau::runtime::Result<Option<String>> {
    let codecs = match table.get("preferred_codecs")? {
        luau::Value::Nil => return Ok(None),
        luau::Value::Table(codecs) => codecs,
        other => {
            return Err(crate::plugins::runtime_error(format!(
                "{owner}.preferred_codecs must be an array of codecs, got {}",
                other.type_name()
            )));
        }
    };
    let codecs = args::array_values(&codecs)
        .map_err(|_| {
            crate::plugins::runtime_error(format!(
                "{owner}.preferred_codecs must be an array of codecs"
            ))
        })?
        .into_iter()
        .map(|(index, codec)| {
            let name = format!("{owner}.preferred_codecs[{index}]");
            match codec {
                luau::Value::String(codec) => {
                    args::string_enum_value::<DeliveryCodec>(&codec, &name)
                        .map(|codec| codec.as_str())
                }
                other => Err(crate::plugins::runtime_error(format!(
                    "{name} must be a string, got {}",
                    other.type_name()
                ))),
            }
        })
        .collect::<luau::runtime::Result<Vec<_>>>()?;
    Ok((!codecs.is_empty()).then(|| codecs.join(",")))
}

/// `TrackServeOptions`, the options of `download_track`, with the unions its fields name.
#[cfg(feature = "docgen")]
pub(crate) fn track_serve_options_interface() -> InterfaceDescriptor {
    serve_interface("TrackServeOptions")
}

/// `TrackStreamOptions`, the options of `stream_track` and `playback_sources.decide`: those of
/// `TrackServeOptions`, `wait_for_transcode` and `public`.
#[cfg(feature = "docgen")]
pub(crate) fn track_stream_options_interface() -> InterfaceDescriptor {
    let mut descriptor = serve_interface("TrackStreamOptions");
    descriptor.fields.push(option_field(
        "wait_for_transcode",
        Option::<bool>::luau_type(),
        "Sends a transcode still being encoded only once it is complete, with its length and byte ranges, rather than streaming it as it is written. Playback starts only after the whole encode, however long it takes. A HEAD request never waits, so it answers without a length while the transcode is unfinished. Defaults to false.",
    ));
    descriptor.fields.push(option_field(
        "public",
        Option::<bool>::luau_type(),
        "Streams any track without checking the caller's access, for routes whose protocol streams publicly. Defaults to false.",
    ));
    descriptor
}

#[cfg(feature = "docgen")]
fn serve_interface(name: &'static str) -> InterfaceDescriptor {
    let mut descriptor = options_interface(name);
    descriptor.fields.insert(
        0,
        option_field(
            "format",
            Option::<DeliveryFormat>::luau_type(),
            "The format to deliver. Defaults to the first preferred codec's format, or else the source's.",
        ),
    );
    let after_channels = descriptor
        .fields
        .iter()
        .position(|field| field.name == "channels")
        .map_or(descriptor.fields.len(), |index| index + 1);
    descriptor.fields.insert(
        after_channels,
        option_field(
            "bit_depth",
            Option::<u32>::luau_type(),
            "The most bits per sample of lossless output: 16 or 24. A deeper source is re-encoded, in its own lossless codec when it would otherwise be sent as it is. Ignored for lossy output and when the source is no deeper.",
        ),
    );
    descriptor
}

/// `HlsServeOptions`, the options of `hls_playlist`.
#[cfg(feature = "docgen")]
pub(crate) fn hls_serve_options_interface() -> InterfaceDescriptor {
    options_interface("HlsServeOptions")
}

/// The unions the option interfaces name.
#[cfg(feature = "docgen")]
pub(crate) fn delivery_aliases() -> Vec<TypeAliasDescriptor> {
    vec![
        DeliveryFormat::type_alias_descriptor(),
        DeliveryCodec::type_alias_descriptor(),
    ]
}

#[cfg(feature = "docgen")]
fn options_interface(name: &'static str) -> InterfaceDescriptor {
    let mut descriptor = InterfaceDescriptor::new(name, None);
    descriptor.fields.extend([
        option_field(
            "preferred_codecs",
            Option::<Vec<DeliveryCodec>>::luau_type(),
            "Codecs to encode with, in order of preference. Defaults to the format's codec.",
        ),
        plain_field("bitrate_bps", Option::<u32>::luau_type()),
        plain_field("sample_rate_hz", Option::<u32>::luau_type()),
        plain_field("channels", Option::<u32>::luau_type()),
        plain_field("prefer_vbr", Option::<bool>::luau_type()),
        plain_field("start_offset_ms", Option::<u64>::luau_type()),
    ]);
    descriptor
}

#[cfg(feature = "docgen")]
fn option_field(name: &'static str, ty: LuauType, description: &'static str) -> FieldDescriptor {
    FieldDescriptor {
        name,
        ty,
        description: Some(description),
    }
}

#[cfg(feature = "docgen")]
fn plain_field(name: &'static str, ty: LuauType) -> FieldDescriptor {
    FieldDescriptor {
        name,
        ty,
        description: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delivery_unions_name_what_delivery_supports() {
        assert_eq!(DeliveryFormat::VALUES, lyra_ffmpeg::SUPPORTED_FORMATS);
        assert_eq!(DeliveryCodec::VALUES, lyra_ffmpeg::SUPPORTED_CODECS);
    }
}
