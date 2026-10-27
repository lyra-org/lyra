// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The options plugins give `stream_track` and `stream_hls` responses.

use anyhow::{
    Context,
    Result,
    bail,
};

use crate::services::delivery::ServeTrackOptions;

pub(crate) fn join_preferred_codecs(preferred_codecs: Option<Vec<String>>) -> Option<String> {
    preferred_codecs.and_then(|preferred_codecs| {
        if preferred_codecs.is_empty() {
            None
        } else {
            Some(preferred_codecs.join(","))
        }
    })
}

#[derive(Default)]
pub(crate) struct ParsedTrackServeOptions {
    pub(crate) format: Option<String>,
    pub(crate) preferred_codecs: Option<Vec<String>>,
    pub(crate) bitrate_bps: Option<u32>,
    pub(crate) sample_rate_hz: Option<u32>,
    pub(crate) channels: Option<u32>,
    pub(crate) prefer_vbr: Option<bool>,
    pub(crate) start_offset_ms: Option<u64>,
}

#[derive(Default)]
pub(crate) struct ParsedHlsServeOptions {
    pub(crate) preferred_codecs: Option<Vec<String>>,
    pub(crate) bitrate_bps: Option<u32>,
    pub(crate) sample_rate_hz: Option<u32>,
    pub(crate) channels: Option<u32>,
    pub(crate) prefer_vbr: Option<bool>,
    pub(crate) start_offset_ms: Option<u64>,
}

pub(crate) fn parse_track_serve_options(
    value: Option<&serde_json::Value>,
) -> Result<ParsedTrackServeOptions> {
    let Some(value) = value else {
        return Ok(ParsedTrackServeOptions::default());
    };
    let object = value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("track serve options must be an object"))?;
    Ok(ParsedTrackServeOptions {
        format: object
            .get("format")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string),
        preferred_codecs: parse_string_list_json(object.get("preferred_codecs")),
        bitrate_bps: parse_u32_json(object.get("bitrate_bps"))?,
        sample_rate_hz: parse_u32_json(object.get("sample_rate_hz"))?,
        channels: parse_u32_json(object.get("channels"))?,
        prefer_vbr: object
            .get("prefer_vbr")
            .and_then(serde_json::Value::as_bool),
        start_offset_ms: parse_u64_json(object.get("start_offset_ms"))?,
    })
}

/// A `stream_track` response's options: whether it skips the caller's track access check, and
/// what it serves.
pub(crate) fn stream_track_options(
    value: Option<&serde_json::Value>,
) -> Result<(bool, ServeTrackOptions)> {
    let public = parse_public_stream(value)?;
    let options = parse_track_serve_options(value)?;
    Ok((
        public,
        ServeTrackOptions {
            format: options.format,
            codec: join_preferred_codecs(options.preferred_codecs),
            bitrate_bps: options.bitrate_bps,
            sample_rate_hz: options.sample_rate_hz,
            channels: options.channels,
            prefer_vbr: options.prefer_vbr,
            start_offset_ms: options.start_offset_ms,
        },
    ))
}

/// Whether a stream response asked to skip the caller's track access check.
fn parse_public_stream(value: Option<&serde_json::Value>) -> Result<bool> {
    let Some(public) = value.and_then(|value| value.get("public")) else {
        return Ok(false);
    };
    public
        .as_bool()
        .ok_or_else(|| anyhow::anyhow!("stream option public must be a boolean"))
}

pub(crate) fn parse_hls_serve_options(
    value: Option<&serde_json::Value>,
) -> Result<ParsedHlsServeOptions> {
    let Some(value) = value else {
        return Ok(ParsedHlsServeOptions::default());
    };
    let object = value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("HLS serve options must be an object"))?;
    Ok(ParsedHlsServeOptions {
        preferred_codecs: parse_string_list_json(object.get("preferred_codecs")),
        bitrate_bps: parse_u32_json(object.get("bitrate_bps"))?,
        sample_rate_hz: parse_u32_json(object.get("sample_rate_hz"))?,
        channels: parse_u32_json(object.get("channels"))?,
        prefer_vbr: object
            .get("prefer_vbr")
            .and_then(serde_json::Value::as_bool),
        start_offset_ms: parse_u64_json(object.get("start_offset_ms"))?,
    })
}

fn parse_string_list_json(value: Option<&serde_json::Value>) -> Option<Vec<String>> {
    let values = value?.as_array()?;
    let values = values
        .iter()
        .filter_map(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    if values.is_empty() {
        None
    } else {
        Some(values)
    }
}

pub(crate) fn parse_u32_json(value: Option<&serde_json::Value>) -> Result<Option<u32>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let Some(value) = value.as_u64() else {
        bail!("expected unsigned integer option");
    };
    Ok(Some(
        u32::try_from(value).context("integer option out of range")?,
    ))
}

fn parse_u64_json(value: Option<&serde_json::Value>) -> Result<Option<u64>> {
    let Some(value) = value else {
        return Ok(None);
    };
    value
        .as_u64()
        .map(Some)
        .ok_or_else(|| anyhow::anyhow!("expected unsigned integer option"))
}
