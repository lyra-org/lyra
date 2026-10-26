// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use crate::services::auth::media_tokens::MediaTokenPurpose;
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
        Method,
        Response,
    },
};
use serde::Deserialize;

use crate::routes::AppError;

use super::{
    DeliveryDescription,
    DeliveryTarget,
    ServeTrackOptions,
    TranscodeKnobs,
    ValidatedTrackSource,
    apply_request_start_offset,
    decision::DeliveryDecision,
    resolve_delivery,
    source_file_response,
    transcoded::transcoded_response,
    validate_and_get_track_source,
    validate_request,
};

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct StreamQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Scoped media token returned by `POST /api/tracks/{id}/playback-url`. It fixes the format, codec and transcode settings, so a request with one passes none of them; only `start_offset_ms` may vary."
        )
    )]
    media_token: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Optional output format (e.g. mp3, flac, wav, ogg, webm, aac, opus)."
        )
    )]
    format: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Optional ordered audio codec preferences (e.g. opus,aac or pcm_s24le,pcm_s16le)."
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

async fn get_stream(
    method: Method,
    headers: HeaderMap,
    Path(track_id): Path<String>,
    Query(query): Query<StreamQuery>,
) -> Result<Response<Body>, AppError> {
    let access = super::require_track_access(
        &headers,
        query.media_token.as_deref(),
        MediaTokenPurpose::Stream,
        &track_id,
    )
    .await?;
    stream_track_response(
        &method,
        &headers,
        &track_id,
        &access,
        ServeTrackOptions {
            format: query.format,
            codec: query.codec,
            bitrate_bps: query.bitrate_bps,
            sample_rate_hz: query.sample_rate_hz,
            channels: query.channels,
            prefer_vbr: query.prefer_vbr,
            start_offset_ms: query.start_offset_ms,
        }
        .bound_to(&access)?,
    )
    .await
}

/// Serves a track as `GET` would. A `HEAD` request gets the same decision and headers without
/// starting FFmpeg, with the length when the transcode is already cached.
pub(crate) async fn stream_track_response(
    method: &Method,
    headers: &HeaderMap,
    track_id: &str,
    access: &super::TrackAccess,
    options: ServeTrackOptions,
) -> Result<Response<Body>, AppError> {
    let (source, delivery) = resolve_stream(track_id, access, options).await?;
    if delivery.direct_passthrough {
        return source_file_response(&source, delivery.content_type, headers).await;
    }
    transcoded_response(
        method,
        headers,
        access.capacity_account(),
        source,
        delivery,
        DeliveryTarget::Stream,
    )
    .await
}

/// What [`stream_track_response`] would deliver for the same request, without serving it.
pub(crate) async fn describe_stream(
    track_id: &str,
    access: &super::TrackAccess,
    options: ServeTrackOptions,
) -> Result<DeliveryDescription, AppError> {
    let (source, delivery) = resolve_stream(track_id, access, options).await?;
    Ok(delivery.describe(&source))
}

async fn resolve_stream(
    track_id: &str,
    access: &super::TrackAccess,
    options: ServeTrackOptions,
) -> Result<(ValidatedTrackSource, DeliveryDecision), AppError> {
    let ServeTrackOptions {
        format,
        codec,
        bitrate_bps,
        sample_rate_hz,
        channels,
        prefer_vbr,
        start_offset_ms,
    } = options;
    let validated = validate_request(format, codec)?;
    let source = apply_request_start_offset(
        validate_and_get_track_source(track_id, access).await?,
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
        DeliveryTarget::Stream,
    )?;
    Ok((source, delivery))
}

#[cfg(feature = "docgen")]
fn stream_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Stream audio")
        .description(
            "Streams audio data for the track ID, including cue-derived virtual segments, optionally transcoded to the requested format or codec. Requires either bearer authentication with access to the track or a scoped `media_token` from `POST /api/tracks/{track_id}/playback-url`. The original file streams in any container; transcoded output can be mp3, flac, wav, ogg, webm, aac, m4a, opus, or aiff. Direct-copy stream responses and finished transcodes support byte ranges and report their length; a transcode still being encoded streams chunked, without ranges, as it is written. Transcodes are cached briefly, so repeat and ranged requests for the same output are served from the finished file. HLS playback is available via `/api/stream/{track_id}/hls.m3u8`.",
        )
}

pub fn stream_routes() -> Router {
    Router::new().route("/{track_id}", get(get_stream))
}

#[cfg(feature = "docgen")]
pub(crate) fn stream_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::get_with;

    aide::axum::ApiRouter::new().api_route("/{track_id}", get_with(get_stream, stream_docs))
}
