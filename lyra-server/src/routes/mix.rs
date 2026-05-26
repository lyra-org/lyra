// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

#[cfg(feature = "docgen")]
use aide::transform::TransformOperation;
use axum::{
    Json,
    extract::{
        Path,
        Query,
    },
    http::HeaderMap,
};
use serde::Deserialize;

use crate::{
    STATE,
    db,
    routes::{
        self,
        AppError,
        responses::TrackResponse,
        tracks as route_tracks,
    },
    services::{
        auth::{
            Principal,
            require_principal,
        },
        mix::{
            self,
            MixSeed,
        },
        tracks as track_service,
    },
};

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
pub(crate) struct MixQueryParams {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Maximum number of tracks to return (default 200).")
    )]
    limit: Option<usize>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Comma-separated or repeated values: releases, artists, release_covers."
        )
    )]
    #[serde(default, deserialize_with = "routes::deserialize_inc")]
    inc: Option<Vec<String>>,
    #[serde(flatten)]
    #[cfg_attr(feature = "docgen", schemars(skip))]
    extra: HashMap<String, String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
pub(crate) struct TrackMixQueryParams {
    #[serde(flatten)]
    base: MixQueryParams,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Pin the seed track at index 0 (default true). Pass `false` for a fully shuffled unpinned mix."
        )
    )]
    instant: Option<bool>,
}

/// Track-seeded mixes default to instant (seed pinned first).
fn track_seed_use_instant(instant: Option<bool>) -> bool {
    instant.unwrap_or(true)
}

fn validate_limit(limit: Option<usize>) -> Result<(), AppError> {
    if let Some(limit) = limit {
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
    Ok(())
}

fn mix_options(query: &MixQueryParams, principal: &Principal) -> mix::MixOptions {
    mix::MixOptions {
        limit: query.limit,
        viewer: Some(principal.user_db_id),
        extra: sanitize_extra(query.extra.clone()),
    }
}

async fn render_mix_response(
    principal: &Principal,
    query: &MixQueryParams,
    tracks: Vec<db::Track>,
) -> Result<Json<Vec<TrackResponse>>, AppError> {
    let tracks = filter_accessible_tracks(principal, tracks).await?;
    let includes = route_tracks::parse_inc(query.inc.clone())?;
    let responses: Vec<TrackResponse> = {
        let db = &*STATE.db.read().await;
        let details = track_service::list_details_for_tracks(db, includes.service, tracks)?;
        details
            .into_iter()
            .map(|detail| route_tracks::track_detail_to_response(db, detail, includes))
            .collect::<anyhow::Result<Vec<_>>>()?
    };
    Ok(Json(responses))
}

pub(crate) async fn get_track_mix(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<TrackMixQueryParams>,
) -> Result<Json<Vec<TrackResponse>>, AppError> {
    let principal = require_principal(&headers).await?;
    validate_limit(query.base.limit)?;

    let db_id = resolve_accessible_seed_id(&id, "track", &principal).await?;
    let options = mix_options(&query.base, &principal);
    let mix_result = if track_seed_use_instant(query.instant) {
        mix::instant_mix_from_audio(db_id, &options).await?
    } else {
        mix::from_seed(MixSeed::Track(db_id), &options).await?
    };
    verify_id_stable(&id, db_id, "track").await?;
    let tracks = mix_result.ok_or_else(|| AppError::not_found(format!("track not found: {id}")))?;
    render_mix_response(&principal, &query.base, tracks).await
}

pub(crate) async fn get_release_mix(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<MixQueryParams>,
) -> Result<Json<Vec<TrackResponse>>, AppError> {
    let principal = require_principal(&headers).await?;
    validate_limit(query.limit)?;

    let db_id = resolve_accessible_seed_id(&id, "release", &principal).await?;
    let options = mix_options(&query, &principal);
    let mix_result = mix::from_seed(MixSeed::Release(db_id), &options).await?;
    verify_id_stable(&id, db_id, "release").await?;
    let tracks =
        mix_result.ok_or_else(|| AppError::not_found(format!("release not found: {id}")))?;
    render_mix_response(&principal, &query, tracks).await
}

pub(crate) async fn get_artist_mix(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<MixQueryParams>,
) -> Result<Json<Vec<TrackResponse>>, AppError> {
    let principal = require_principal(&headers).await?;
    validate_limit(query.limit)?;

    let db_id = resolve_accessible_seed_id(&id, "artist", &principal).await?;
    let options = mix_options(&query, &principal);
    let mix_result = mix::from_seed(MixSeed::Artist(db_id), &options).await?;
    verify_id_stable(&id, db_id, "artist").await?;
    let tracks =
        mix_result.ok_or_else(|| AppError::not_found(format!("artist not found: {id}")))?;
    render_mix_response(&principal, &query, tracks).await
}

pub(crate) async fn get_genre_mix(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<MixQueryParams>,
) -> Result<Json<Vec<TrackResponse>>, AppError> {
    let principal = require_principal(&headers).await?;
    validate_limit(query.limit)?;

    let db_id = resolve_seed_id(&id, "genre").await?;
    let options = mix_options(&query, &principal);
    let mix_result = mix::from_seed(MixSeed::Genre(db_id), &options).await?;
    verify_id_stable(&id, db_id, "genre").await?;
    let tracks = mix_result.ok_or_else(|| AppError::not_found(format!("genre not found: {id}")))?;
    render_mix_response(&principal, &query, tracks).await
}

pub(crate) async fn get_playlist_mix(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<MixQueryParams>,
) -> Result<Json<Vec<TrackResponse>>, AppError> {
    let principal = require_principal(&headers).await?;
    validate_limit(query.limit)?;

    let db_id = resolve_seed_id(&id, "playlist").await?;
    {
        let db = STATE.db.read().await;
        if !routes::playlist_accessible_to_principal(&*db, &principal, db_id)? {
            return Err(AppError::not_found(format!("playlist not found: {id}")));
        }
    }
    let options = mix_options(&query, &principal);
    let mix_result = mix::from_seed(MixSeed::Playlist(db_id), &options).await?;
    verify_id_stable(&id, db_id, "playlist").await?;
    let tracks =
        mix_result.ok_or_else(|| AppError::not_found(format!("playlist not found: {id}")))?;
    render_mix_response(&principal, &query, tracks).await
}

pub(crate) async fn get_me_mix(
    headers: HeaderMap,
    Query(query): Query<MixQueryParams>,
) -> Result<Json<Vec<TrackResponse>>, AppError> {
    let principal = require_principal(&headers).await?;
    validate_limit(query.limit)?;

    let options = mix_options(&query, &principal);
    let tracks = mix::from_seed(
        MixSeed::Recent {
            user_db_id: principal.user_db_id,
        },
        &options,
    )
    .await?
    .ok_or_else(|| AppError::not_found("user not found".to_string()))?;
    render_mix_response(&principal, &query, tracks).await
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
    principal: &Principal,
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
    principal: &Principal,
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

const KNOWN_QUERY_KEYS: &[&str] = &["limit", "inc", "instant"];
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
const MIX_DESCRIPTION: &str = "Returns a shuffled list of tracks similar to the seed. Use `inc` to hydrate nested data: `releases`, `artists`, and `release_covers` (covers require `releases`).";

#[cfg(feature = "docgen")]
const TRACK_MIX_DESCRIPTION: &str = "Returns a shuffled list of tracks similar to the seed. Use `inc` to hydrate nested data: `releases`, `artists`, and `release_covers` (covers require `releases`). Defaults to instant mode (`instant=true`): the seed track is pinned at index 0, then similar tracks follow; pass `instant=false` for a fully shuffled unpinned mix.";

#[cfg(feature = "docgen")]
const ME_MIX_DESCRIPTION: &str = "Returns a shuffled list of tracks similar to the seed. Use `inc` to hydrate nested data: `releases`, `artists`, and `release_covers` (covers require `releases`). Seeds from the authenticated user's recent listen history.";

#[cfg(feature = "docgen")]
pub(crate) fn track_mix_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Generate mix from track")
        .description(TRACK_MIX_DESCRIPTION)
}

#[cfg(feature = "docgen")]
pub(crate) fn release_mix_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Generate mix from release")
        .description(MIX_DESCRIPTION)
}

#[cfg(feature = "docgen")]
pub(crate) fn artist_mix_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Generate mix from artist")
        .description(MIX_DESCRIPTION)
}

#[cfg(feature = "docgen")]
pub(crate) fn genre_mix_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Generate mix from genre")
        .description(MIX_DESCRIPTION)
}

#[cfg(feature = "docgen")]
pub(crate) fn playlist_mix_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Generate mix from playlist")
        .description(MIX_DESCRIPTION)
}

#[cfg(feature = "docgen")]
pub(crate) fn me_mix_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Generate mix from recent listens")
        .description(ME_MIX_DESCRIPTION)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn track_seed_use_instant_defaults_true() {
        assert!(track_seed_use_instant(None));
        assert!(track_seed_use_instant(Some(true)));
        assert!(!track_seed_use_instant(Some(false)));
    }
}
