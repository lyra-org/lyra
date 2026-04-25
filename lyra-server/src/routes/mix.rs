// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

use aide::axum::{
    ApiRouter,
    routing::get_with,
};
use aide::transform::TransformOperation;
use axum::{
    Json,
    extract::Query,
    http::HeaderMap,
};
use schemars::JsonSchema;
use serde::Deserialize;

use crate::{
    STATE,
    db,
    routes::AppError,
    routes::responses::TrackResponse,
    services::{
        auth::require_principal,
        mix,
    },
};

#[derive(Deserialize, JsonSchema)]
struct MixQuery {
    #[schemars(description = "Seed track ID to generate a mix from.")]
    seed_track: Option<String>,
    #[schemars(description = "Seed release ID to generate a mix from.")]
    seed_release: Option<String>,
    #[schemars(description = "Seed artist ID to generate a mix from.")]
    seed_artist: Option<String>,
    #[schemars(description = "Seed from recent listen history.")]
    #[serde(default)]
    seed_recent: bool,
    #[schemars(description = "Maximum number of tracks to return (default 200).")]
    limit: Option<usize>,
    #[serde(flatten)]
    #[schemars(skip)]
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
        + query.seed_recent as u8;
    if seed_count == 0 {
        return Err(AppError::bad_request(
            "one of seed_track, seed_release, seed_artist, or seed_recent is required".to_string(),
        ));
    }
    if seed_count > 1 {
        return Err(AppError::bad_request(
            "provide exactly one of seed_track, seed_release, seed_artist, or seed_recent"
                .to_string(),
        ));
    }

    let options = mix::MixOptions {
        limit: query.limit,
        user_db_id: Some(principal.user_db_id),
        extra: sanitize_extra(query.extra),
    };

    let tracks = if let Some(ref id) = query.seed_track {
        let db_id = {
            let db = &*STATE.db.read().await;
            let db_id = db::lookup::find_node_id_by_id(db, id)?
                .ok_or_else(|| AppError::not_found(format!("track not found: {id}")))?;
            db::tracks::get_by_id(db, db_id)?
                .ok_or_else(|| AppError::not_found(format!("track not found: {id}")))?;
            db_id
        };
        mix::from_track(db_id, &options).await?
    } else if let Some(ref id) = query.seed_release {
        let db_id = {
            let db = &*STATE.db.read().await;
            let db_id = db::lookup::find_node_id_by_id(db, id)?
                .ok_or_else(|| AppError::not_found(format!("release not found: {id}")))?;
            db::releases::get_by_id(db, db_id)?
                .ok_or_else(|| AppError::not_found(format!("release not found: {id}")))?;
            db_id
        };
        mix::from_release(db_id, &options).await?
    } else if let Some(ref id) = query.seed_artist {
        let db_id = {
            let db = &*STATE.db.read().await;
            let db_id = db::lookup::find_node_id_by_id(db, id)?
                .ok_or_else(|| AppError::not_found(format!("artist not found: {id}")))?;
            db::artists::get_by_id(db, db_id)?
                .ok_or_else(|| AppError::not_found(format!("artist not found: {id}")))?;
            db_id
        };
        mix::from_artist(db_id, &options).await?
    } else if query.seed_recent {
        mix::from_recent_listens(principal.user_db_id, &options).await?
    } else {
        unreachable!()
    };

    let responses: Vec<TrackResponse> = tracks.into_iter().map(TrackResponse::from).collect();
    Ok(Json(responses))
}

const KNOWN_QUERY_KEYS: &[&str] = &[
    "seed_track",
    "seed_release",
    "seed_artist",
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

fn mix_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Generate mix")
        .description("Returns a shuffled list of tracks that share genres with the seed item. Provide exactly one of seed_track, seed_release, seed_artist, or seed_recent.")
}

pub fn mix_routes() -> ApiRouter {
    ApiRouter::new().api_route("/", get_with(get_mix, mix_docs))
}
