// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use aide::axum::{
    ApiRouter,
    routing::{
        get_with,
        patch_with,
    },
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
    db,
    routes::AppError,
    routes::deserialize_inc,
    routes::responses::{
        ArtistRelationResponse,
        ArtistResponse,
        RelatedArtistResponse,
        RelationDirectionResponse,
        ReleaseResponse,
    },
    services::{
        artists as artist_service,
        auth::require_authenticated,
    },
};

#[derive(Deserialize, JsonSchema)]
struct ArtistQuery {
    #[schemars(description = "Comma-separated or repeated values: releases, tracks, relations.")]
    #[serde(default, deserialize_with = "deserialize_inc")]
    inc: Option<Vec<String>>,
}

#[derive(Deserialize, JsonSchema)]
struct ArtistUpdateRequest {
    #[schemars(description = "Updated artist name.")]
    name: Option<String>,
    #[schemars(description = "Updated sort name; set to null to clear.")]
    sort_name: Option<Option<String>>,
    #[schemars(description = "Updated description; set to null to clear.")]
    description: Option<Option<String>>,
}

fn parse_inc(inc: Option<Vec<String>>) -> Result<artist_service::ArtistIncludes, AppError> {
    let values = super::parse_inc_values(inc, &["releases", "tracks", "relations"])?;
    let mut result = artist_service::ArtistIncludes {
        releases: false,
        tracks: false,
        relations: false,
    };
    for value in values {
        match value.as_str() {
            "releases" => result.releases = true,
            "tracks" => result.tracks = true,
            "relations" => result.relations = true,
            _ => {}
        }
    }
    Ok(result)
}

fn artist_detail_to_response(
    _db: &impl db::DbAccess,
    detail: artist_service::ArtistDetails,
) -> anyhow::Result<ArtistResponse> {
    let releases = detail
        .releases
        .map(|v| v.into_iter().map(ReleaseResponse::from).collect());
    let relations = detail.relations.map(|v| {
        v.into_iter()
            .map(|r| ArtistRelationResponse {
                relation_type: r.relation_type,
                attributes: r.attributes,
                direction: match r.direction {
                    artist_service::RelationDirection::Incoming => {
                        RelationDirectionResponse::Incoming
                    }
                    artist_service::RelationDirection::Outgoing => {
                        RelationDirectionResponse::Outgoing
                    }
                },
                artist: RelatedArtistResponse {
                    id: r.artist.id,
                    name: r.artist.artist_name,
                    artist_type: r.artist.artist_type,
                },
            })
            .collect()
    });
    Ok(ArtistResponse {
        id: detail.artist.id,
        name: detail.artist.artist_name,
        sort_name: detail.artist.sort_name,
        description: detail.artist.description,
        verified: detail.artist.verified,
        credit: None,
        releases,
        tracks: detail
            .tracks
            .map(|v| v.into_iter().map(Into::into).collect()),
        relations,
    })
}

pub(crate) async fn list_artist_responses(
    inc: Option<Vec<String>>,
) -> Result<Vec<ArtistResponse>, AppError> {
    let db = &*STATE.db.read().await;
    let includes = parse_inc(inc)?;
    let details = artist_service::list_details(db, includes)?;

    details
        .into_iter()
        .map(|d| artist_detail_to_response(db, d))
        .collect::<anyhow::Result<Vec<_>>>()
        .map_err(AppError::from)
}

pub(crate) async fn get_artist_response(
    id: String,
    inc: Option<Vec<String>>,
) -> Result<ArtistResponse, AppError> {
    let db = &*STATE.db.read().await;
    let includes = parse_inc(inc)?;
    let artist_db_id = db::lookup::find_node_id_by_id(db, &id)?
        .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
    let detail = artist_service::get_details(db, artist_db_id, includes)?
        .ok_or_else(|| AppError::not_found(format!("Artist not found: {}", id)))?;

    Ok(artist_detail_to_response(db, detail)?)
}

async fn get_artists(
    headers: HeaderMap,
    Query(query): Query<ArtistQuery>,
) -> Result<Json<Vec<ArtistResponse>>, AppError> {
    let _principal = require_authenticated(&headers).await?;
    Ok(Json(list_artist_responses(query.inc).await?))
}

async fn get_artist(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<ArtistQuery>,
) -> Result<Json<ArtistResponse>, AppError> {
    let _principal = require_authenticated(&headers).await?;
    Ok(Json(get_artist_response(id, query.inc).await?))
}

async fn update_artist(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(update): Json<ArtistUpdateRequest>,
) -> Result<Json<ArtistResponse>, AppError> {
    let _principal = require_authenticated(&headers).await?;

    if update.name.is_none() && update.sort_name.is_none() && update.description.is_none() {
        return Err(AppError::bad_request("no artist fields provided"));
    }

    let ArtistUpdateRequest {
        name: update_name,
        sort_name: update_sort_name,
        description: update_description,
    } = update;

    let mut db = STATE.db.write().await;
    let artist_db_id = db::lookup::find_node_id_by_id(&*db, &id)?
        .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
    if let Some(name) = update_name.as_ref()
        && name.trim().is_empty()
    {
        return Err(AppError::bad_request("name cannot be empty"));
    }

    if let Some(Some(sort_name)) = update_sort_name.as_ref()
        && sort_name.trim().is_empty()
    {
        return Err(AppError::bad_request("sort_name cannot be empty"));
    }

    if let Some(Some(description)) = update_description.as_ref()
        && description.trim().is_empty()
    {
        return Err(AppError::bad_request("description cannot be empty"));
    }

    let updated = artist_service::update(
        &mut db,
        artist_db_id,
        update_name,
        update_sort_name,
        update_description,
    )?
    .ok_or_else(|| AppError::not_found(format!("Artist not found: {}", id)))?;

    Ok(Json(ArtistResponse {
        id: updated.id,
        name: updated.artist_name,
        sort_name: updated.sort_name,
        description: updated.description,
        verified: updated.verified,
        credit: None,
        releases: None,
        tracks: None,
        relations: None,
    }))
}

fn list_artists_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List artists").description(
        "Returns artists. Use `inc` to include releases, tracks, and/or relations. The `credit` field is not present on artist-level responses; it only appears when artists are included via track or release endpoints.",
    )
}

fn get_artist_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get artist by ID").description(
        "Returns a single artist. 404 if not found. Use `inc` to include releases, tracks, and/or relations. The `credit` field is not present on artist-level responses; it only appears when artists are included via track or release endpoints.",
    )
}

fn update_artist_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Update artist")
        .description("Updates artist name, sort name, and description. Set sort_name or description to null to clear.")
}

pub fn artist_routes() -> ApiRouter {
    ApiRouter::new()
        .api_route("/", get_with(get_artists, list_artists_docs))
        .api_route("/{id}", get_with(get_artist, get_artist_docs))
        .api_route("/{id}", patch_with(update_artist, update_artist_docs))
}
