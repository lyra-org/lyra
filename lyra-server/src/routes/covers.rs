// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use axum::{
    body::Body,
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
use std::path::Path;

use crate::{
    routes::{
        AppError,
        serve::file_response,
    },
    services::covers,
};

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

pub(crate) async fn serve_cover_response(
    path: &Path,
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
            .header(header::CACHE_CONTROL, "no-cache, no-store, must-revalidate")
            .header(header::PRAGMA, "no-cache")
            .header(header::EXPIRES, "0")
            .body(Body::from(transformed.bytes))
            .map_err(AppError::from);
    }

    let content_type = covers::cover_mime_from_path(path);
    file_response(path, content_type, request_headers).await
}
