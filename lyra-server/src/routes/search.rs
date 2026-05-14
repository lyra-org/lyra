// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

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
use serde::{
    Deserialize,
    Serialize,
};

use crate::{
    STATE,
    routes::AppError,
    services::{
        auth::require_authenticated,
        search as search_service,
    },
};

const MAX_QUERY_LEN: usize = 256;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct SearchQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Fuzzy text query matched against tracks, artists, and releases. \
        Required, 1-256 characters after trimming."
        )
    )]
    query: String,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Optional per-entity result cap. Defaults to 20, capped at 50.")
    )]
    limit: Option<u64>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
pub struct SearchTitleHit {
    pub id: String,
    pub title: String,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
pub struct SearchArtistHit {
    pub id: String,
    pub name: String,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
pub struct SearchResponse {
    pub tracks: Vec<SearchTitleHit>,
    pub artists: Vec<SearchArtistHit>,
    pub releases: Vec<SearchTitleHit>,
}

async fn search(
    headers: HeaderMap,
    Query(params): Query<SearchQuery>,
) -> Result<Json<SearchResponse>, AppError> {
    let principal = require_authenticated(&headers).await?;

    let trimmed = params.query.trim();
    if trimmed.is_empty() {
        return Err(AppError::bad_request("query must not be empty"));
    }
    if trimmed.chars().count() > MAX_QUERY_LEN {
        return Err(AppError::bad_request(format!(
            "query exceeds {MAX_QUERY_LEN} characters"
        )));
    }

    let options = search_service::SearchOptions::new(trimmed.to_string(), params.limit);

    // CPU-bound: each entity's `query` walks the full root collection in memory.
    let guard = STATE.db.read().await;
    let results = tokio::task::spawn_blocking(move || {
        search_service::search_accessible(&guard, &principal, &options)
    })
    .await
    .map_err(|err| AppError::from(anyhow::anyhow!("search task failed: {err}")))??;

    Ok(Json(SearchResponse {
        tracks: results
            .tracks
            .into_iter()
            .map(|hit| SearchTitleHit {
                id: hit.id,
                title: hit.title,
            })
            .collect(),
        artists: results
            .artists
            .into_iter()
            .map(|hit| SearchArtistHit {
                id: hit.id,
                name: hit.name,
            })
            .collect(),
        releases: results
            .releases
            .into_iter()
            .map(|hit| SearchTitleHit {
                id: hit.id,
                title: hit.title,
            })
            .collect(),
    }))
}

#[cfg(feature = "docgen")]
fn search_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Cross-entity fuzzy search").description(
        "Returns up to `limit` matching tracks, artists, and releases for `query`. Each branch \
         is a minimal autocomplete shape (`id` + title/name); use the per-entity resource \
         endpoints for detail and for paging within a single type. `limit` defaults to 20, \
         capped at 50. `query` is 1-256 characters after trimming, matched case-insensitively. \
         Per-token modifiers: `'foo` exact, `^foo` prefix, `foo$` suffix, `!foo` negation, `\\` \
         escapes a leading modifier; plain tokens are fuzzy.",
    )
}

pub fn search_routes() -> Router {
    Router::new().route("/", get(search))
}

#[cfg(feature = "docgen")]
pub(crate) fn search_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::get_with;

    aide::axum::ApiRouter::new().api_route("/", get_with(search, search_docs))
}
