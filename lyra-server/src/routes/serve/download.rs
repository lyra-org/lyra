// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
use aide::transform::TransformOperation;
use axum::{
    Router,
    routing::get,
};
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
    FfmpegContext,
    Output,
};
use serde::Deserialize;

use crate::routes::AppError;

use super::{
    DeliveryTarget,
    ServeTrackOptions,
    TranscodeKnobs,
    apply_request_start_offset,
    file_response,
    resolve_delivery,
    temp_file_response,
    temp_output_path,
    validate_and_get_track_source,
    validate_request,
};

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct DownloadQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Scoped download token returned by `POST /api/tracks/{id}/playback-url`."
        )
    )]
    media_token: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Optional output format (e.g. mp3, flac, wav, ogg, webm, m4a, alac)."
        )
    )]
    format: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Optional ordered audio codec preferences (e.g. opus,aac or pcm_s24be,pcm_s16be)."
        )
    )]
    codec: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Target bitrate cap in bits per second. Applied for lossy outputs when below the source bitrate; ignored for lossless codecs or when above source."
        )
    )]
    bitrate_bps: Option<u32>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Target sample rate in Hz. Triggers transcoding when supplied.")
    )]
    sample_rate_hz: Option<u32>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Target channel count. Triggers transcoding when supplied.")
    )]
    channels: Option<u32>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Prefer VBR for lossy transcodes when the selected encoder supports it."
        )
    )]
    prefer_vbr: Option<bool>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Per-request playback start offset in milliseconds.")
    )]
    start_offset_ms: Option<u64>,
}

pub(crate) struct DownloadTrackRequest {
    pub(crate) output: ServeTrackOptions,
    pub(crate) media_token: Option<String>,
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
        DownloadTrackRequest {
            output: ServeTrackOptions {
                format: query.format,
                codec: query.codec,
                bitrate_bps: query.bitrate_bps,
                sample_rate_hz: query.sample_rate_hz,
                channels: query.channels,
                prefer_vbr: query.prefer_vbr,
                start_offset_ms: query.start_offset_ms,
            },
            media_token: query.media_token,
        },
    )
    .await
}

pub(crate) async fn download_track_response(
    headers: &HeaderMap,
    track_db_id: agdb::DbId,
    request: DownloadTrackRequest,
) -> Result<Response<Body>, AppError> {
    let DownloadTrackRequest {
        output:
            ServeTrackOptions {
                format,
                codec,
                bitrate_bps,
                sample_rate_hz,
                channels,
                prefer_vbr,
                start_offset_ms,
            },
        media_token,
    } = request;
    match super::require_download_track_access(headers, media_token.as_deref(), track_db_id).await?
    {
        super::TrackAccess::Principal(principal) => {
            let db = crate::STATE.db.read().await;
            crate::services::auth::access::require_entity_accessible(
                &*db,
                &principal,
                track_db_id,
                || AppError::not_found(format!("Track not found: {}", track_db_id.0)),
            )?;
        }
        super::TrackAccess::MediaToken => {}
    }
    let validated = validate_request(format, codec)?;
    let source = apply_request_start_offset(
        validate_and_get_track_source(track_db_id).await?,
        start_offset_ms,
    )?;

    let delivery = resolve_delivery(
        &validated,
        &source,
        TranscodeKnobs {
            bitrate_bps,
            sample_rate_hz,
            channels,
            prefer_vbr,
        },
        DeliveryTarget::Download,
    )?;

    if delivery.direct_passthrough {
        return file_response(&source.full_path, delivery.content_type, headers).await;
    }

    let temp_path = temp_output_path(track_db_id, delivery.output_format);
    let temp_path_string = temp_path.to_string_lossy().into_owned();
    let output = delivery.configure_output(Output::new(temp_path_string));

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

    let response = temp_file_response(&temp_path, delivery.content_type, headers).await;
    if response.is_err() {
        let _ = tokio::fs::remove_file(&temp_path).await;
    }

    response
}

#[cfg(feature = "docgen")]
fn download_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Download audio")
        .description(
            "Downloads audio for the track ID, including cue-derived virtual segments, optionally transcoded to the requested format or codec. Requires bearer authentication with download permission or a scoped download `media_token` from `POST /api/tracks/{track_id}/playback-url`. Supports all formats including m4a, alac, and caf. Returns a complete file with byte-range support.",
        )
}

pub fn download_routes() -> Router {
    Router::new().route("/{track_id}", get(get_download))
}

#[cfg(feature = "docgen")]
pub(crate) fn download_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::get_with;

    aide::axum::ApiRouter::new().api_route("/{track_id}", get_with(get_download, download_docs))
}
