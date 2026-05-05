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
    Json,
    extract::Query,
    http::HeaderMap,
};
use schemars::JsonSchema;
use serde::{
    Deserialize,
    Serialize,
};
use std::collections::HashSet;

use crate::{
    STATE,
    db,
    routes::AppError,
    services::{
        auth::require_principal,
        playback_sessions as playback_service,
        providers::PROVIDER_REGISTRY,
    },
};

#[derive(Deserialize, JsonSchema)]
struct ListenCountQuery {
    #[schemars(description = "Track ID to count listens for.")]
    track_id: String,
    #[schemars(
        description = "When true, also counts listens for tracks that share a provider-declared unique track external ID."
    )]
    merge_unique_external_ids: Option<bool>,
}

#[derive(Serialize, JsonSchema)]
struct ListenCountResponse {
    #[schemars(description = "Track ID that was counted.")]
    track_id: String,
    #[schemars(description = "Authenticated user ID used for scoping the count.")]
    user_id: String,
    #[schemars(description = "Number of listens for this user and track.")]
    listen_count: u64,
}

async fn get_listen_count(
    headers: HeaderMap,
    Query(query): Query<ListenCountQuery>,
) -> Result<Json<ListenCountResponse>, AppError> {
    let principal = require_principal(&headers).await?;

    let merge_unique_external_ids = query.merge_unique_external_ids.unwrap_or(false);
    let unique_track_id_pairs = if merge_unique_external_ids {
        let registry = PROVIDER_REGISTRY.read().await;
        registry.unique_track_id_pairs()
    } else {
        HashSet::new()
    };

    let db = STATE.db.read().await;
    let track_db_id = db::lookup::find_node_id_by_id(&*db, &query.track_id)?
        .ok_or_else(|| AppError::not_found(format!("Track not found: {}", query.track_id)))?;

    let count_track_ids = if merge_unique_external_ids {
        playback_service::resolve_merged_track_ids_for_play_count(
            &db,
            track_db_id,
            &unique_track_id_pairs,
        )?
    } else {
        vec![track_db_id]
    };

    let counts = db::listens::get_counts(&db, &count_track_ids, Some(principal.user_db_id))?;
    let listen_count: u64 = counts.values().copied().sum();

    let user_id = db::lookup::find_id_by_db_id(&*db, principal.user_db_id)?
        .ok_or_else(|| anyhow::anyhow!("user entity missing id"))?;

    Ok(Json(ListenCountResponse {
        track_id: query.track_id,
        user_id,
        listen_count,
    }))
}

fn get_listen_count_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get listen count").description(
        "Returns the authenticated user's listen count for a track. Use `merge_unique_external_ids=true` to merge counts across tracks that share provider-declared unique track IDs.",
    )
}

pub fn listen_routes() -> ApiRouter {
    ApiRouter::new().api_route("/count", get_with(get_listen_count, get_listen_count_docs))
}
