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

use crate::{
    routes::AppError,
    services::delivery::{
        DeliveryTarget,
        ServeTrackOptions,
    },
};

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct DownloadQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Scoped download token returned by `POST /api/tracks/{id}/playback-url`. It fixes the format, codec and transcode settings, so a request with one passes none of them; only `start_offset_ms` may vary."
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
            description = "Optional comma-separated `container/codec` pairs the client plays as they are (e.g. m4a/aac,ogg/opus,flac/flac), named as `source.container` and `source.codec` of a delivery description. When given, `copy` sends the source unconverted only if its pair is listed, so a source whose codec is unknown is always converted, and an empty list copies nothing; otherwise any source may be copied."
        )
    )]
    copy_profiles: Option<String>,
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
            description = "Bit depth cap for lossless output: 16 or 24. A deeper source is re-encoded, in its own lossless codec when it would otherwise be sent as it is; ignored for lossy output and when the source is no deeper."
        )
    )]
    bit_depth: Option<u32>,
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

async fn get_download(
    method: Method,
    Path(track_id): Path<String>,
    Query(query): Query<DownloadQuery>,
    headers: HeaderMap,
) -> Result<Response<Body>, AppError> {
    let access = super::require_track_access(
        &headers,
        query.media_token.as_deref(),
        MediaTokenPurpose::Download,
        &track_id,
    )
    .await?;
    super::track_response(
        &method,
        &headers,
        &track_id,
        &access,
        ServeTrackOptions {
            format: query.format,
            codec: query.codec,
            copy_profiles: query.copy_profiles,
            bitrate_bps: query.bitrate_bps,
            sample_rate_hz: query.sample_rate_hz,
            channels: query.channels,
            bit_depth: query.bit_depth,
            prefer_vbr: query.prefer_vbr,
            start_offset_ms: query.start_offset_ms,
        }
        .bound_to(&access)?,
        DeliveryTarget::Download,
        false,
    )
    .await
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
