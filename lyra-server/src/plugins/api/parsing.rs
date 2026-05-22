// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use super::*;

pub(super) fn join_preferred_codecs(preferred_codecs: Option<Vec<String>>) -> Option<String> {
    preferred_codecs.and_then(|preferred_codecs| {
        if preferred_codecs.is_empty() {
            None
        } else {
            Some(preferred_codecs.join(","))
        }
    })
}

#[derive(Default)]
pub(super) struct ParsedTrackServeOptions {
    pub(super) format: Option<String>,
    pub(super) preferred_codecs: Option<Vec<String>>,
    pub(super) bitrate_bps: Option<u32>,
    pub(super) sample_rate_hz: Option<u32>,
    pub(super) channels: Option<u32>,
    pub(super) prefer_vbr: Option<bool>,
    pub(super) start_offset_ms: Option<u64>,
}

#[derive(Default)]
pub(super) struct ParsedHlsServeOptions {
    pub(super) preferred_codecs: Option<Vec<String>>,
    pub(super) bitrate_bps: Option<u32>,
    pub(super) sample_rate_hz: Option<u32>,
    pub(super) channels: Option<u32>,
    pub(super) prefer_vbr: Option<bool>,
    pub(super) start_offset_ms: Option<u64>,
}

pub(super) fn parse_track_serve_options(
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

pub(super) fn parse_hls_serve_options(
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

pub(super) fn parse_string_list_json(value: Option<&serde_json::Value>) -> Option<Vec<String>> {
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

pub(super) fn parse_u32_json(value: Option<&serde_json::Value>) -> Result<Option<u32>> {
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

pub(super) fn parse_u64_json(value: Option<&serde_json::Value>) -> Result<Option<u64>> {
    let Some(value) = value else {
        return Ok(None);
    };
    value
        .as_u64()
        .map(Some)
        .ok_or_else(|| anyhow::anyhow!("expected unsigned integer option"))
}

pub(super) fn infer_content_type(path: &str) -> &'static str {
    let extension = FsPath::new(path)
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase());
    match extension.as_deref() {
        Some("aac") => "audio/aac",
        Some("aiff") | Some("aif") => "audio/aiff",
        Some("flac") => "audio/flac",
        Some("m4a") | Some("m4b") | Some("mp4") => "audio/mp4",
        Some("mp3") => "audio/mpeg",
        Some("ogg") => "audio/ogg",
        Some("opus") => "audio/ogg",
        Some("webm") => "audio/webm",
        Some("wav") => "audio/wav",
        Some("m3u8") => "application/x-mpegurl",
        _ => "application/octet-stream",
    }
}
