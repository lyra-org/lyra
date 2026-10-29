// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! How a track's audio is delivered: who may be served it, which source file it is read from,
//! and whether that file is served as it is or transcoded, and how.

mod access;
pub(crate) mod activity;
pub(crate) mod decision;
mod hls;
mod options;
mod source;

pub(crate) use access::{
    TrackAccess,
    authorize_track_purpose,
    require_plugin_track_access,
    require_session_access,
    stream_track_access,
};
pub(crate) use decision::{
    DeliveryDecision,
    DeliveryDescription,
    DeliveryTarget,
    TranscodeKnobs,
    ValidatedRequest,
    apply_transcode_policy,
    resolve_delivery,
    validate_request,
};
pub(crate) use hls::{
    HlsPlaylistOptions,
    HlsRendition,
    resolve_hls,
    resolve_hls_profile,
};
pub(crate) use options::{
    ServeTrackOptions,
    reject_unbound_options,
};
pub(crate) use source::{
    ValidatedTrackSource,
    apply_request_start_offset,
    source_range_duration_ms,
    validate_and_get_track_source,
};

use crate::services::auth::AuthError;

#[derive(Debug, thiserror::Error)]
pub(crate) enum DeliveryError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    Unauthorized(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Gone(String),
    #[error("{0}")]
    ServiceUnavailable(String),
    #[error(transparent)]
    Auth(#[from] AuthError),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl DeliveryError {
    pub(crate) fn bad_request(message: impl Into<String>) -> Self {
        Self::BadRequest(message.into())
    }

    pub(crate) fn unauthorized(message: impl Into<String>) -> Self {
        Self::Unauthorized(message.into())
    }

    pub(crate) fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound(message.into())
    }

    pub(crate) fn gone(message: impl Into<String>) -> Self {
        Self::Gone(message.into())
    }

    pub(crate) fn service_unavailable(message: impl Into<String>) -> Self {
        Self::ServiceUnavailable(message.into())
    }
}

/// What a stream of `track_id` through `access` with `options` would deliver, without serving it.
pub(crate) async fn describe_stream(
    track_id: &str,
    access: &TrackAccess,
    options: ServeTrackOptions,
) -> Result<DeliveryDescription, DeliveryError> {
    let (source, delivery) =
        resolve_track_delivery(track_id, access, options, DeliveryTarget::Stream).await?;
    Ok(delivery.describe(&source))
}

/// The source a delivery of `track_id` to `target` through `access` reads, and how it is
/// delivered.
pub(crate) async fn resolve_track_delivery(
    track_id: &str,
    access: &TrackAccess,
    options: ServeTrackOptions,
    target: DeliveryTarget,
) -> Result<(ValidatedTrackSource, DeliveryDecision), DeliveryError> {
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
        target,
    )?;
    Ok((source, delivery))
}
