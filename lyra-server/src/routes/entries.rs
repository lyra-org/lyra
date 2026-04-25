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
    extract::{
        Path,
        Query,
    },
    http::HeaderMap,
};
use schemars::JsonSchema;
use serde::Deserialize;

use crate::{
    STATE,
    db::{
        self,
        Permission,
    },
    routes::AppError,
    routes::deserialize_inc,
    routes::responses::{
        EntryResponse,
        ReleaseResponse,
    },
    services::{
        auth::require_authenticated,
        entries::{
            EntryDetails,
            EntryIncludes,
            get_details,
            list_details,
        },
    },
};

#[derive(Deserialize, JsonSchema)]
struct EntryQuery {
    #[schemars(description = "Comma-separated or repeated values: tracks, releases, artists.")]
    #[serde(default, deserialize_with = "deserialize_inc")]
    inc: Option<Vec<String>>,
}

fn parse_inc(inc: Option<Vec<String>>) -> Result<EntryIncludes, AppError> {
    let values = super::parse_inc_values(inc, &["tracks", "releases", "artists"])?;
    let mut result = EntryIncludes {
        tracks: false,
        releases: false,
        artists: false,
    };
    for value in values {
        match value.as_str() {
            "tracks" => result.tracks = true,
            "releases" => result.releases = true,
            "artists" => result.artists = true,
            _ => {}
        }
    }
    Ok(result)
}

fn detail_to_entry_response(
    _db: &impl db::DbAccess,
    detail: EntryDetails,
    include_full_path: bool,
) -> anyhow::Result<EntryResponse> {
    let mut response = EntryResponse::from_entry(detail.entry, include_full_path);
    response.tracks = detail
        .tracks
        .map(|v| v.into_iter().map(Into::into).collect());
    response.releases = detail
        .releases
        .map(|v| v.into_iter().map(ReleaseResponse::from).collect());
    response.artists = detail
        .artists
        .map(|v| v.into_iter().map(Into::into).collect());
    Ok(response)
}

async fn get_entries(
    headers: HeaderMap,
    Query(query): Query<EntryQuery>,
) -> Result<Json<Vec<EntryResponse>>, AppError> {
    let principal = require_authenticated(&headers).await?;
    let include_full_path =
        db::roles::has_permission(&principal.permissions, Permission::ManageLibraries);
    let include = parse_inc(query.inc)?;
    let db = &*STATE.db.read().await;
    let details = list_details(db, include)?;
    let response: Vec<EntryResponse> = details
        .into_iter()
        .map(|d| detail_to_entry_response(db, d, include_full_path))
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(Json(response))
}

async fn get_entry(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<EntryQuery>,
) -> Result<Json<EntryResponse>, AppError> {
    let principal = require_authenticated(&headers).await?;
    let include_full_path =
        db::roles::has_permission(&principal.permissions, Permission::ManageLibraries);
    let include = parse_inc(query.inc)?;
    let db = &*STATE.db.read().await;
    let detail = get_details(db, &id, include)?;
    Ok(Json(detail_to_entry_response(
        db,
        detail,
        include_full_path,
    )?))
}

fn list_entries_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List entries").description(
        "Returns entries. `full_path` is included only for authenticated users with ManageLibraries permission. Use `inc` to include tracks, releases, and/or artists.",
    )
}

fn get_entry_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get entry by ID").description(
        "Returns a single entry. `full_path` is included only for authenticated users with ManageLibraries permission. 404 if not found. Use `inc` to include tracks, releases, and/or artists.",
    )
}

pub fn entry_routes() -> ApiRouter {
    ApiRouter::new()
        .api_route("/", get_with(get_entries, list_entries_docs))
        .api_route("/{id}", get_with(get_entry, get_entry_docs))
}
