// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use aide::{
    axum::{
        ApiRouter,
        routing::get_with,
    },
    transform::TransformOperation,
};
use axum::{
    body::Body,
    extract::{
        Path,
        Query,
    },
    http::{
        HeaderMap,
        Response,
        header,
    },
};
use schemars::JsonSchema;
use serde::{
    Deserialize,
    Serialize,
};
use std::path::{
    Path as FsPath,
    PathBuf,
};

use crate::{
    STATE,
    db,
    routes::{
        AppError,
        responses::CoverResponse,
        serve::file_response_with_cache_control,
    },
    services::covers,
};

const PUBLIC_COVER_CACHE_CONTROL: &str = "public, max-age=31536000, immutable";

#[derive(Serialize, JsonSchema)]
#[non_exhaustive]
pub struct CoverSearchCandidateResponse {
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub width: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub height: Option<u32>,
}

#[derive(Serialize, JsonSchema)]
#[non_exhaustive]
pub struct ProviderCoverSearchResponse {
    pub provider_id: String,
    pub candidates: Vec<CoverSearchCandidateResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_index: Option<u32>,
}

#[derive(Deserialize, JsonSchema, Default)]
pub(crate) struct CoverQuery {
    #[schemars(description = "Optional output image format (jpg, png, webp).")]
    pub(crate) format: Option<String>,
    #[schemars(description = "Optional output quality (0-100).")]
    pub(crate) quality: Option<u8>,
    #[schemars(description = "Optional maximum output width in pixels.")]
    pub(crate) max_width: Option<u32>,
    #[schemars(description = "Optional maximum output height in pixels.")]
    pub(crate) max_height: Option<u32>,
}

#[derive(Deserialize, JsonSchema, Default)]
pub(crate) struct CoverSearchQuery {
    #[schemars(description = "Optional provider ID to limit cover search results.")]
    pub(crate) provider: Option<String>,
    #[serde(default)]
    #[schemars(description = "Bypass cached provider cover resolution and refresh it.")]
    pub(crate) force_refresh: bool,
}

pub(crate) fn parse_cover_transform_options(
    query: &CoverQuery,
) -> Result<Option<covers::CoverTransformOptions>, AppError> {
    let format = match query.format.as_deref() {
        Some(raw) => Some(covers::parse_cover_image_format(raw).ok_or_else(|| {
            AppError::bad_request(format!(
                "Unsupported image format: {}. Supported formats: jpg, png, webp",
                raw
            ))
        })?),
        None => None,
    };

    if let Some(quality) = query.quality
        && quality > 100
    {
        return Err(AppError::bad_request("quality must be between 0 and 100"));
    }

    if query.max_width == Some(0) {
        return Err(AppError::bad_request("max_width must be greater than 0"));
    }

    if query.max_height == Some(0) {
        return Err(AppError::bad_request("max_height must be greater than 0"));
    }

    let options = covers::CoverTransformOptions {
        format,
        quality: query.quality,
        max_width: query.max_width,
        max_height: query.max_height,
    };

    if options.is_empty() {
        return Ok(None);
    }

    Ok(Some(options))
}

pub(crate) fn public_cover_url(id: &str, hash: &str) -> String {
    format!("/api/covers/{id}?v={hash}")
}

pub(crate) fn cover_to_response(cover: db::Cover) -> CoverResponse {
    let url = public_cover_url(&cover.id, &cover.hash);
    CoverResponse {
        id: cover.id,
        url,
        mime_type: cover.mime_type,
        hash: cover.hash,
        blurhash: cover.blurhash,
    }
}

pub(crate) fn build_cover_response(
    db: &impl db::DbAccess,
    owner_db_id: agdb::DbId,
    include_covers: bool,
) -> anyhow::Result<Option<Option<CoverResponse>>> {
    if !include_covers {
        return Ok(None);
    }

    Ok(Some(
        db::covers::get(db, owner_db_id)?.map(cover_to_response),
    ))
}

pub(crate) fn map_provider_cover_search_results(
    found: Vec<covers::ProviderCoverSearchResult>,
) -> Vec<ProviderCoverSearchResponse> {
    found
        .into_iter()
        .map(|result| ProviderCoverSearchResponse {
            provider_id: result.provider_id,
            candidates: result
                .candidates
                .into_iter()
                .map(|candidate| CoverSearchCandidateResponse {
                    url: candidate.url,
                    width: candidate.width,
                    height: candidate.height,
                })
                .collect(),
            selected_index: result.selected_index,
        })
        .collect()
}

async fn serve_public_cover_response(
    path: &FsPath,
    transform_options: Option<covers::CoverTransformOptions>,
    request_headers: &HeaderMap,
) -> Result<Response<Body>, AppError> {
    if let Some(options) = transform_options {
        let cover_path = path.to_path_buf();
        let transformed = tokio::task::spawn_blocking(move || {
            covers::transform_cover_image(&cover_path, &options)
        })
        .await
        .map_err(|err| AppError::from(anyhow::anyhow!("cover transform task failed: {err}")))??;

        return Response::builder()
            .header(header::CONTENT_TYPE, transformed.mime_type)
            .header(header::CONTENT_LENGTH, transformed.bytes.len().to_string())
            .header(header::CACHE_CONTROL, PUBLIC_COVER_CACHE_CONTROL)
            .body(Body::from(transformed.bytes))
            .map_err(AppError::from);
    }

    let content_type = covers::cover_mime_from_path(path);
    file_response_with_cache_control(
        path,
        content_type,
        request_headers,
        PUBLIC_COVER_CACHE_CONTROL,
    )
    .await
}

async fn get_public_cover(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<CoverQuery>,
) -> Result<Response<Body>, AppError> {
    let transform_options = parse_cover_transform_options(&query)?;
    let cover = {
        let db = STATE.db.read().await;
        db::covers::get_by_public_id(&*db, &id)?
            .ok_or_else(|| AppError::not_found(format!("Cover not found: {id}")))?
    };
    let path = PathBuf::from(&cover.path);
    if !path.is_file() {
        return Err(AppError::not_found(format!("Cover not found: {id}")));
    }

    serve_public_cover_response(&path, transform_options, &headers).await
}

fn get_public_cover_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get cover image").description(
        "Returns a public cover image by cover ID. Cover metadata returns URLs with a `v` query parameter derived from the cover hash for cache versioning; clients should treat the full URL as opaque. Supports optional transform parameters: `format`, `quality`, `max_width`, and `max_height`.",
    )
}

pub fn cover_routes() -> ApiRouter {
    ApiRouter::new().api_route("/{id}", get_with(get_public_cover, get_public_cover_docs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cover_to_response_sets_public_url() {
        let response = cover_to_response(db::Cover {
            db_id: None,
            id: "cover-1".to_string(),
            path: "/music/cover.jpg".to_string(),
            mime_type: "image/jpeg".to_string(),
            hash: "a".repeat(64),
            blurhash: None,
        });

        assert_eq!(response.id, "cover-1");
        assert_eq!(
            response.url,
            format!("/api/covers/cover-1?v={}", "a".repeat(64))
        );
    }
}
