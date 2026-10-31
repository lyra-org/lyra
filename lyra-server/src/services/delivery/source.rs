// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::DbId;
use lyra_ffmpeg::{
    AudioCodec,
    AudioFormat,
};
use std::path::PathBuf;

use super::{
    DeliveryError,
    TrackAccess,
    authorize_track_purpose,
};
use crate::services::{
    STATE,
    playback_sources::{
        self as playback_source_service,
        SourceVersion,
    },
};

#[derive(Debug)]
pub(crate) struct ValidatedTrackSource {
    pub(crate) source_id: DbId,
    pub(crate) source_public_id: String,
    pub(crate) version: SourceVersion,
    pub(crate) track_public_id: String,
    pub(crate) input_path: String,
    pub(crate) entry_format: Option<AudioFormat>,
    pub(crate) source_codec: Option<AudioCodec>,
    pub(crate) full_path: PathBuf,
    pub(crate) duration_ms: Option<u64>,
    pub(crate) start_ms: Option<u64>,
    pub(crate) end_ms: Option<u64>,
    /// The source file's format and audio properties, as scanned.
    pub(crate) format: lyra_db::track_sources::SourceFormat,
}

pub(crate) fn source_range_duration_ms(start_ms: Option<u64>, end_ms: Option<u64>) -> Option<u64> {
    match (start_ms, end_ms) {
        (Some(start_ms), Some(end_ms)) if end_ms > start_ms => Some(end_ms - start_ms),
        _ => None,
    }
}

pub(crate) fn apply_request_start_offset(
    mut source: ValidatedTrackSource,
    start_offset_ms: Option<u64>,
) -> Result<ValidatedTrackSource, DeliveryError> {
    let Some(start_offset_ms) = start_offset_ms else {
        return Ok(source);
    };

    if start_offset_ms == 0 {
        return Ok(source);
    }

    let current_start_ms = source.start_ms.unwrap_or(0);
    let next_start_ms = current_start_ms
        .checked_add(start_offset_ms)
        .ok_or_else(|| DeliveryError::bad_request("start_offset_ms is too large"))?;

    if let Some(end_ms) = source.end_ms {
        if next_start_ms >= end_ms {
            return Err(DeliveryError::bad_request(
                "start_offset_ms exceeds the available source duration",
            ));
        }
    } else if let Some(duration_ms) = source.duration_ms.filter(|duration_ms| *duration_ms > 0) {
        if start_offset_ms >= duration_ms {
            return Err(DeliveryError::bad_request(
                "start_offset_ms exceeds the available source duration",
            ));
        }

        source.duration_ms = Some(duration_ms - start_offset_ms);
    }

    source.start_ms = Some(next_start_ms);
    Ok(source)
}

/// Resolves the track's playable source, checking `access` under the same guard.
pub(crate) async fn validate_and_get_track_source(
    track_id: &str,
    access: &TrackAccess,
) -> Result<ValidatedTrackSource, DeliveryError> {
    let db = &*STATE.db.read().await;
    let not_found = || DeliveryError::not_found(format!("Track not found: {track_id}"));
    let track_db_id = lyra_db::lookup::find_node_id_by_id(db, track_id)?.ok_or_else(not_found)?;
    match access {
        TrackAccess::Principal(principal) => {
            principal.require(db)?;
            crate::services::auth::access::require_entity_accessible(
                db,
                principal,
                track_db_id,
                not_found,
            )?;
        }
        TrackAccess::MediaToken(token) => {
            let principal = token.holder.principal(db)?.ok_or_else(|| {
                DeliveryError::unauthorized("media token holder is no longer signed in")
            })?;
            authorize_track_purpose(&principal, token.purpose)?;
            crate::services::auth::access::require_entity_accessible(
                db,
                &principal,
                track_db_id,
                not_found,
            )?;
        }
        TrackAccess::PublicPluginRoute(_) => {}
    }
    let track = lyra_db::tracks::get_by_id(db, track_db_id)?.ok_or_else(not_found)?;

    let source = playback_source_service::resolve(db, &track, false)?.ok_or_else(|| {
        DeliveryError::not_found(format!("Playable source not found for track: {track_id}"))
    })?;
    let source_public_id = source.source_id;
    let version =
        SourceVersion::read(source_public_id.clone(), &source.full_path).map_err(|err| {
            tracing::warn!(
                track_id,
                path = %source.full_path.display(),
                error = %err,
                "track source file is unreadable"
            );
            DeliveryError::not_found(format!("Track source file not found for track: {track_id}"))
        })?;
    if let TrackAccess::MediaToken(token) = access
        && token.source != version
    {
        return Err(DeliveryError::gone(
            "the track's source changed since this media token was issued",
        ));
    }

    Ok(ValidatedTrackSource {
        source_id: source.source_db_id,
        source_public_id,
        version,
        track_public_id: track.id.clone(),
        input_path: source.input_path,
        entry_format: playback_source_service::transcoder_format(source.format),
        source_codec: source
            .format
            .codec
            .and_then(playback_source_service::transcoder_codec),
        full_path: source.full_path,
        duration_ms: track.duration_ms,
        start_ms: source.start_ms,
        end_ms: source.end_ms,
        format: source.format,
    })
}
