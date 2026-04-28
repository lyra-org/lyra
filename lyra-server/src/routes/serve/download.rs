// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use aide::axum::{
    ApiRouter,
    routing::get_with,
};
use aide::transform::TransformOperation;
use axum::{
    body::Body,
    extract::{
        Path,
        Query,
    },
    http::{
        HeaderMap,
        Response,
    },
};
use lyra_ffmpeg::{
    AudioCodec,
    FfmpegContext,
    Output,
};
use schemars::JsonSchema;
use serde::Deserialize;

use crate::routes::AppError;

use super::{
    apply_request_start_offset,
    apply_transcode_policy,
    configure_output,
    file_response,
    require_download_access,
    resolve_codec,
    resolve_output_format,
    temp_file_response,
    temp_output_path,
    validate_and_get_track_source,
    validate_request,
};

#[derive(Deserialize, JsonSchema)]
struct DownloadQuery {
    #[schemars(
        description = "Optional output format (e.g. mp3, flac, wav, ogg, webm, m4a, alac)."
    )]
    format: Option<String>,
    #[schemars(
        description = "Optional ordered audio codec preferences (e.g. opus,aac or pcm_s24be,pcm_s16be)."
    )]
    codec: Option<String>,
    #[schemars(
        description = "Target bitrate cap in bits per second. Applied for lossy outputs when below the source bitrate; ignored for lossless codecs or when above source."
    )]
    bitrate_bps: Option<u32>,
    #[schemars(description = "Target sample rate in Hz. Triggers transcoding when supplied.")]
    sample_rate_hz: Option<u32>,
    #[schemars(description = "Target channel count. Triggers transcoding when supplied.")]
    channels: Option<u32>,
    #[schemars(
        description = "Prefer VBR for lossy transcodes when the selected encoder supports it."
    )]
    prefer_vbr: Option<bool>,
    #[schemars(description = "Per-request playback start offset in milliseconds.")]
    start_offset_ms: Option<u64>,
}

async fn get_download(
    Path(track_id): Path<String>,
    Query(query): Query<DownloadQuery>,
    headers: HeaderMap,
) -> Result<Response<Body>, AppError> {
    let track_db_id = {
        let db = crate::STATE.db.read().await;
        crate::db::lookup::find_node_id_by_id(&*db, &track_id)?
            .ok_or_else(|| AppError::not_found(format!("not found: {track_id}")))?
    };
    download_track_response(
        &headers,
        track_db_id,
        query.format,
        query.codec,
        query.bitrate_bps,
        query.sample_rate_hz,
        query.channels,
        query.prefer_vbr,
        query.start_offset_ms,
    )
    .await
}

pub(crate) async fn download_track_response(
    headers: &HeaderMap,
    track_db_id: agdb::DbId,
    format: Option<String>,
    codec: Option<String>,
    bitrate_bps: Option<u32>,
    sample_rate_hz: Option<u32>,
    channels: Option<u32>,
    prefer_vbr: Option<bool>,
    start_offset_ms: Option<u64>,
) -> Result<Response<Body>, AppError> {
    let _principal = require_download_access(headers).await?;
    let validated = validate_request(format, codec)?;
    let source = apply_request_start_offset(
        validate_and_get_track_source(track_db_id).await?,
        start_offset_ms,
    )?;

    let initial_output_format = resolve_output_format(
        validated.format,
        &validated.preferred_codecs,
        source.entry_format,
        &source.full_path,
        true,
    )?;

    let initial_codec = resolve_codec(
        &validated.preferred_codecs,
        initial_output_format,
        source.entry_format,
        true,
    )?;
    let provisional_output_format = if matches!(initial_codec, AudioCodec::Copy) {
        resolve_output_format(
            validated.format,
            &validated.preferred_codecs,
            source.entry_format,
            &source.full_path,
            false,
        )?
    } else {
        initial_output_format
    };
    let provisional_codec = if matches!(initial_codec, AudioCodec::Copy) {
        resolve_codec(
            &validated.preferred_codecs,
            provisional_output_format,
            source.entry_format,
            false,
        )?
    } else {
        initial_codec
    };
    let policy = apply_transcode_policy(
        bitrate_bps,
        sample_rate_hz,
        channels,
        prefer_vbr,
        provisional_codec,
        source.source_bitrate_bps,
    )?;
    let forcing_transcode = policy.bitrate_bps.is_some()
        || policy.sample_rate_hz.is_some()
        || policy.channels.is_some();

    let mut output_format = initial_output_format;
    let mut codec = initial_codec;
    if (source.start_ms.is_some() || source.end_ms.is_some() || forcing_transcode)
        && matches!(codec, AudioCodec::Copy)
    {
        output_format = provisional_output_format;
        codec = provisional_codec;
    }
    let output_mime = output_format.mime_type(false);

    if matches!(codec, AudioCodec::Copy)
        && source.entry_format == Some(output_format)
        && source.start_ms.is_none()
        && source.end_ms.is_none()
        && !forcing_transcode
    {
        return file_response(&source.full_path, output_mime, headers).await;
    }

    let temp_path = temp_output_path(track_db_id, output_format);
    let temp_path_string = temp_path.to_string_lossy().into_owned();
    let output = configure_output(Output::new(temp_path_string), output_format, codec, &policy);

    let context = FfmpegContext::builder()
        .input(source.input_path)
        .start_ms(source.start_ms)
        .end_ms(source.end_ms)
        .output(output)
        .build()?;

    let result =
        tokio::task::spawn_blocking(move || context.start().map(|handle| handle.wait())).await?;
    match result {
        Err(e) => {
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(e.into());
        }
        Ok(Err(e)) => {
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(e.into());
        }
        Ok(Ok(())) => {}
    }

    let response = temp_file_response(&temp_path, output_mime, headers).await;
    if response.is_err() {
        let _ = tokio::fs::remove_file(&temp_path).await;
    }

    response
}

fn download_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Download audio")
        .description(
            "Downloads audio for the track ID, including cue-derived virtual segments, optionally transcoded to the requested format or codec. Supports all formats including m4a, alac, and caf. Returns a complete file with byte-range support.",
        )
}

pub fn download_routes() -> ApiRouter {
    ApiRouter::new().api_route("/{track_id}", get_with(get_download, download_docs))
}
