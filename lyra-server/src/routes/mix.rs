// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

#[cfg(feature = "docgen")]
use aide::transform::TransformOperation;
use axum::{
    Json,
    extract::Query,
    http::HeaderMap,
};
use axum::{
    Router,
    routing::get,
};
use serde::Deserialize;

use crate::{
    STATE,
    db,
    routes::{
        self,
        AppError,
        responses::TrackResponse,
    },
    services::{
        auth::require_principal,
        mix,
    },
};

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct MixQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Seed track ID to generate a mix from.")
    )]
    seed_track: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Seed release ID to generate a mix from.")
    )]
    seed_release: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Seed artist ID to generate a mix from.")
    )]
    seed_artist: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Seed genre ID to generate a mix from.")
    )]
    seed_genre: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Seed playlist ID to generate a mix from.")
    )]
    seed_playlist: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Seed from recent listen history.")
    )]
    #[serde(default)]
    seed_recent: bool,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Maximum number of tracks to return (default 200).")
    )]
    limit: Option<usize>,
    #[serde(flatten)]
    #[cfg_attr(feature = "docgen", schemars(skip))]
    extra: HashMap<String, String>,
}

async fn get_mix(
    headers: HeaderMap,
    Query(query): Query<MixQuery>,
) -> Result<Json<Vec<TrackResponse>>, AppError> {
    let principal = require_principal(&headers).await?;

    let seed_count = query.seed_track.is_some() as u8
        + query.seed_release.is_some() as u8
        + query.seed_artist.is_some() as u8
        + query.seed_genre.is_some() as u8
        + query.seed_playlist.is_some() as u8
        + query.seed_recent as u8;
    if seed_count == 0 {
        return Err(AppError::bad_request(
            "one of seed_track, seed_release, seed_artist, seed_genre, seed_playlist, or seed_recent is required".to_string(),
        ));
    }
    if seed_count > 1 {
        return Err(AppError::bad_request(
            "provide exactly one of seed_track, seed_release, seed_artist, seed_genre, seed_playlist, or seed_recent"
                .to_string(),
        ));
    }
    if let Some(limit) = query.limit {
        if limit == 0 {
            return Err(AppError::bad_request("limit must be > 0".to_string()));
        }
        if limit > mix::MAX_LIMIT {
            return Err(AppError::bad_request(format!(
                "limit must be <= {}, got {limit}",
                mix::MAX_LIMIT
            )));
        }
    }

    let options = mix::MixOptions {
        limit: query.limit,
        user_db_id: Some(principal.user_db_id),
        extra: sanitize_extra(query.extra),
    };

    // Service enforces existence + type; `verify_id_stable` catches DbId reuse.
    let result = if let Some(ref id) = query.seed_track {
        let db_id = resolve_accessible_seed_id(id, "track", &principal).await?;
        let mix_result = mix::from_track(db_id, &options).await?;
        verify_id_stable(id, db_id, "track").await?;
        mix_result.ok_or_else(|| AppError::not_found(format!("track not found: {id}")))?
    } else if let Some(ref id) = query.seed_release {
        let db_id = resolve_accessible_seed_id(id, "release", &principal).await?;
        let mix_result = mix::from_release(db_id, &options).await?;
        verify_id_stable(id, db_id, "release").await?;
        mix_result.ok_or_else(|| AppError::not_found(format!("release not found: {id}")))?
    } else if let Some(ref id) = query.seed_artist {
        let db_id = resolve_accessible_seed_id(id, "artist", &principal).await?;
        let mix_result = mix::from_artist(db_id, &options).await?;
        verify_id_stable(id, db_id, "artist").await?;
        mix_result.ok_or_else(|| AppError::not_found(format!("artist not found: {id}")))?
    } else if let Some(ref id) = query.seed_genre {
        let db_id = resolve_seed_id(id, "genre").await?;
        let mix_result = mix::from_genre(db_id, &options).await?;
        verify_id_stable(id, db_id, "genre").await?;
        mix_result.ok_or_else(|| AppError::not_found(format!("genre not found: {id}")))?
    } else if let Some(ref id) = query.seed_playlist {
        let db_id = resolve_seed_id(id, "playlist").await?;
        {
            let db = STATE.db.read().await;
            if !routes::playlist_accessible_to_principal(&*db, &principal, db_id)? {
                return Err(AppError::not_found(format!("playlist not found: {id}")));
            }
        }
        let mix_result = mix::from_playlist(db_id, &options).await?;
        verify_id_stable(id, db_id, "playlist").await?;
        mix_result.ok_or_else(|| AppError::not_found(format!("playlist not found: {id}")))?
    } else if query.seed_recent {
        // Seed is the authenticated user — no public-id round-trip.
        mix::from_recent_listens(principal.user_db_id, &options)
            .await?
            .ok_or_else(|| AppError::not_found("user not found".to_string()))?
    } else {
        unreachable!()
    };

    let result = filter_accessible_tracks(&principal, result).await?;
    let responses: Vec<TrackResponse> = result.into_iter().map(TrackResponse::from).collect();
    Ok(Json(responses))
}

/// 404s on unknown public id. Type-mismatch is the service layer's job.
async fn resolve_seed_id(id: &str, label: &str) -> Result<agdb::DbId, AppError> {
    let db = &*STATE.db.read().await;
    db::lookup::find_node_id_by_id(db, id)?
        .ok_or_else(|| AppError::not_found(format!("{label} not found: {id}")))
}

async fn resolve_accessible_seed_id(
    id: &str,
    label: &str,
    principal: &crate::services::auth::Principal,
) -> Result<agdb::DbId, AppError> {
    let db = &*STATE.db.read().await;
    let db_id = db::lookup::find_node_id_by_id(db, id)?
        .ok_or_else(|| AppError::not_found(format!("{label} not found: {id}")))?;
    routes::require_entity_accessible(db, principal, db_id, || {
        AppError::not_found(format!("{label} not found: {id}"))
    })?;
    Ok(db_id)
}

async fn filter_accessible_tracks(
    principal: &crate::services::auth::Principal,
    tracks: Vec<db::Track>,
) -> anyhow::Result<Vec<db::Track>> {
    let db = &*STATE.db.read().await;
    let mut filtered = Vec::with_capacity(tracks.len());
    for track in tracks {
        let Some(track_db_id) = track.db_id.clone().map(agdb::DbId::from) else {
            continue;
        };
        if routes::entity_accessible_to_principal(db, principal, track_db_id)? {
            filtered.push(track);
        }
    }
    Ok(filtered)
}

/// Asserts the id still maps to `expected_db_id` — agdb reuses DbIds, so
/// a delete+create during dispatch could redirect the response.
async fn verify_id_stable(
    public_id: &str,
    expected_db_id: agdb::DbId,
    label: &str,
) -> Result<(), AppError> {
    let db = &*STATE.db.read().await;
    match db::lookup::find_node_id_by_id(db, public_id)? {
        Some(now) if now == expected_db_id => Ok(()),
        _ => Err(AppError::not_found(format!(
            "{label} not found: {public_id}"
        ))),
    }
}

const KNOWN_QUERY_KEYS: &[&str] = &[
    "seed_track",
    "seed_release",
    "seed_artist",
    "seed_genre",
    "seed_playlist",
    "seed_recent",
    "limit",
];
const MAX_EXTRA_KEYS: usize = 20;
const MAX_EXTRA_KEY_LEN: usize = 64;
const MAX_EXTRA_VALUE_LEN: usize = 256;

fn sanitize_extra(mut extra: HashMap<String, String>) -> HashMap<String, String> {
    extra.retain(|key, value| {
        !KNOWN_QUERY_KEYS.contains(&key.as_str())
            && key.len() <= MAX_EXTRA_KEY_LEN
            && value.len() <= MAX_EXTRA_VALUE_LEN
    });
    if extra.len() > MAX_EXTRA_KEYS {
        let keys_to_keep: Vec<String> = extra.keys().take(MAX_EXTRA_KEYS).cloned().collect();
        extra.retain(|key, _| keys_to_keep.contains(key));
    }
    extra
}

#[cfg(feature = "docgen")]
fn mix_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Generate mix")
        .description("Returns a shuffled list of tracks that share genres with the seed item. Provide exactly one of seed_track, seed_release, seed_artist, seed_genre, seed_playlist, or seed_recent.")
}

pub fn mix_routes() -> Router {
    Router::new().route("/", get(get_mix))
}

#[cfg(feature = "docgen")]
pub(crate) fn mix_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::get_with;

    aide::axum::ApiRouter::new().api_route("/", get_with(get_mix, mix_docs))
}
