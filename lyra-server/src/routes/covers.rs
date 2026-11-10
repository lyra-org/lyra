// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
use aide::transform::TransformOperation;
use axum::{
    Json,
    Router,
    body::Body,
    extract::{
        DefaultBodyLimit,
        Path,
        Query,
    },
    http::{
        HeaderMap,
        Response,
        header,
    },
    routing::{
        get,
        put,
    },
};
use serde::{
    Deserialize,
    Serialize,
};
use std::{
    collections::HashMap,
    path::PathBuf,
};

use agdb::{
    DbAny,
    DbId,
};

use crate::{
    routes::{
        AppError,
        request::{
            UnreadBody,
            bearer_credential,
        },
        responses::CoverResponse,
        serve::{
            file_response_with_cache_control,
            open_file_response_with_cache_control,
        },
    },
    services::STATE,
    services::{
        auth::{
            Principal,
            require_authenticated,
        },
        catalog::Viewer,
        covers::{
            self,
            ResolvedCover,
            display::CoverFilter,
            selection::{
                self,
                CoverCandidates,
                CoverOwnerKind,
            },
            shown::{
                self,
                CoverTarget,
            },
            stale,
        },
        libraries::MediaRoots,
    },
};
use lyra_db::covers::CoverSource;

const PUBLIC_COVER_CACHE_CONTROL: &str = "public, max-age=31536000, immutable";

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[non_exhaustive]
pub struct CoverSearchCandidateResponse {
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub width: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub height: Option<u32>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[non_exhaustive]
pub struct ProviderCoverSearchResponse {
    pub provider_id: String,
    pub candidates: Vec<CoverSearchCandidateResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_index: Option<u32>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize, Default)]
pub(crate) struct CoverQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Optional output image format (jpg, png, webp).")
    )]
    pub(crate) format: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Optional output quality (0-100).")
    )]
    pub(crate) quality: Option<u8>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Optional maximum output width in pixels.")
    )]
    pub(crate) max_width: Option<u32>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Optional maximum output height in pixels.")
    )]
    pub(crate) max_height: Option<u32>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize, Default)]
pub(crate) struct CoverSearchQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Optional provider ID to limit cover search results. Must be a valid provider ID: non-empty ASCII with no whitespace."
        )
    )]
    pub(crate) provider: Option<String>,
    #[serde(default)]
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Look covers up again instead of using cached lookups.")
    )]
    pub(crate) bypass_cover_cache: bool,
}

impl CoverSearchQuery {
    pub(crate) fn provider_filter(&self) -> Result<Option<&str>, AppError> {
        super::providers::parse_provider_filter(self.provider.as_deref())
    }
}

pub(crate) fn public_cover_url(id: &str, hash: &str) -> String {
    format!("/api/covers/{id}?v={hash}")
}

pub(crate) fn cover_to_response(cover: lyra_db::Cover) -> CoverResponse {
    let url = public_cover_url(&cover.id, &cover.hash);
    CoverResponse {
        id: cover.id,
        url,
        mime_type: cover.mime_type,
        hash: cover.hash,
        blurhash: cover.blurhash,
    }
}

/// The covers `targets` show through `filter`, keyed by target id.
pub(crate) fn resolve_covers(
    db: &DbAny,
    filter: &CoverFilter<'_>,
    targets: &[CoverTarget<'_>],
) -> Result<HashMap<DbId, ResolvedCover>, AppError> {
    Ok(shown::resolve(db, filter, targets)?)
}

/// The covers `targets` show to `principal`, keyed by target id. The principal's viewer is looked
/// up only when a target's cover may come from releases.
pub(crate) fn resolve_principal_covers(
    db: &DbAny,
    principal: &Principal,
    targets: &[CoverTarget<'_>],
) -> Result<HashMap<DbId, ResolvedCover>, AppError> {
    if !targets.iter().any(CoverTarget::derived) {
        return resolve_covers(db, &CoverFilter::new(&Viewer::System), targets);
    }
    let viewer = Viewer::user(db, principal.clone())?;
    resolve_covers(db, &CoverFilter::new(&viewer), targets)
}

/// A response's `cover` field for `db_id`: left out unless `include`, else its cover in `covers`.
pub(crate) fn cover_field(
    covers: &HashMap<DbId, ResolvedCover>,
    include: bool,
    db_id: Option<DbId>,
) -> Option<Option<CoverResponse>> {
    include.then(|| {
        db_id
            .and_then(|db_id| covers.get(&db_id))
            .map(|resolved| cover_to_response(resolved.cover.clone()))
    })
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

/// The cover with public id `id`, as `options` ask and ranged as `request_headers` ask. A cover
/// whose file is confirmed gone is removed, and reads as not found as a missing cover does.
pub(crate) async fn serve_cover(
    id: &str,
    options: covers::CoverTransformOptions,
    request_headers: &HeaderMap,
) -> Result<Response<Body>, AppError> {
    let not_found = || AppError::not_found(format!("Cover not found: {id}"));
    let (cover, roots) = {
        let db = STATE.db.read().await;
        let cover = lyra_db::covers::get_by_public_id(&*db, id)?.ok_or_else(not_found)?;
        (cover, MediaRoots::for_principal(&*db, None)?)
    };
    let path = PathBuf::from(&cover.path);
    if !path.is_file() {
        if stale::confirmed_gone(&path) {
            stale::remove_stale_cover(&mut *STATE.db.write().await, &cover.id)?;
        }
        return Err(not_found());
    }
    let path = roots
        .and_covers(covers::configured_covers_root())
        .for_covers()
        .resolve_off_thread(path)
        .await
        .map_err(|_| not_found())?;

    let content_type = if options.is_empty() {
        covers::cover_mime_from_path(&path)
    } else {
        match covers::render_cover(&path, &cover.hash, options).await? {
            covers::CoverRendition::Original { mime_type } => mime_type,
            covers::CoverRendition::Cached {
                file,
                len,
                mime_type,
            } => {
                return open_file_response_with_cache_control(
                    tokio::fs::File::from_std(file),
                    len,
                    mime_type,
                    request_headers,
                    PUBLIC_COVER_CACHE_CONTROL,
                );
            }
        }
    };
    file_response_with_cache_control(
        &path,
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
    let format = query
        .format
        .as_deref()
        .map(|raw| {
            covers::parse_cover_image_format(raw).ok_or_else(|| {
                AppError::bad_request(format!(
                    "Unsupported image format: {raw}. Supported formats: {}",
                    covers::COVER_EXTENSIONS.join(", ")
                ))
            })
        })
        .transpose()?;
    let options = covers::CoverTransformOptions::new(
        format,
        query.quality.map(u32::from),
        query.max_width,
        query.max_height,
    )?;
    serve_cover(&id, options, &headers).await
}

#[cfg(feature = "docgen")]
fn get_public_cover_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get cover image").description(
        "Returns a public cover image by cover ID. Cover metadata returns URLs with a `v` query parameter derived from the cover hash for cache versioning; clients should treat the full URL as opaque. Supports optional transform parameters: `format`, `quality`, `max_width`, and `max_height`. Bounds snap up to 64, 128, 256, 512, 1024 or 2048 pixels, and larger ones serve the full size; JPEG quality snaps up to 50, 75, 90 or 100. Images that cannot be transformed safely, such as animated or lossless WebP, are served unchanged.",
    )
}

pub fn cover_routes() -> Router {
    Router::new().route("/{id}", get(get_public_cover))
}

#[cfg(feature = "docgen")]
pub(crate) fn cover_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::get_with;

    aide::axum::ApiRouter::new()
        .api_route("/{id}", get_with(get_public_cover, get_public_cover_docs))
}

/// An entity whose cover users choose, mounted under its own collection path.
pub(crate) trait CoverOwnerRoute: Send + Sync + 'static {
    const KIND: CoverOwnerKind;
    /// The entity's name in documentation, in lowercase.
    #[cfg(feature = "docgen")]
    const NAME: &'static str;
}

pub(crate) struct ReleaseCovers;
pub(crate) struct ArtistCovers;
pub(crate) struct PlaylistCovers;

impl CoverOwnerRoute for ReleaseCovers {
    const KIND: CoverOwnerKind = CoverOwnerKind::Release;
    #[cfg(feature = "docgen")]
    const NAME: &'static str = "release";
}

impl CoverOwnerRoute for ArtistCovers {
    const KIND: CoverOwnerKind = CoverOwnerKind::Artist;
    #[cfg(feature = "docgen")]
    const NAME: &'static str = "artist";
}

impl CoverOwnerRoute for PlaylistCovers {
    const KIND: CoverOwnerKind = CoverOwnerKind::Playlist;
    #[cfg(feature = "docgen")]
    const NAME: &'static str = "playlist";
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StoredCoverSource {
    Local,
    Provider,
    Manual,
}

impl From<StoredCoverSource> for CoverSource {
    fn from(source: StoredCoverSource) -> Self {
        match source {
            StoredCoverSource::Local => Self::Local,
            StoredCoverSource::Provider => Self::Provider,
            StoredCoverSource::Manual => Self::Manual,
        }
    }
}

impl From<CoverSource> for StoredCoverSource {
    fn from(source: CoverSource) -> Self {
        match source {
            CoverSource::Local => Self::Local,
            CoverSource::Provider => Self::Provider,
            CoverSource::Manual => Self::Manual,
        }
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CoverCandidateSource {
    Local,
    Provider,
    Manual,
    Generated,
}

impl From<Option<CoverSource>> for CoverCandidateSource {
    fn from(source: Option<CoverSource>) -> Self {
        match source {
            Some(CoverSource::Local) => Self::Local,
            Some(CoverSource::Provider) => Self::Provider,
            Some(CoverSource::Manual) => Self::Manual,
            None => Self::Generated,
        }
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[non_exhaustive]
pub struct CoverCandidateResponse {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Where the cover came from: `local` (an image beside the audio files in the library), `provider` (a provider's cover handler), `manual` (uploaded by a user), or `generated` (a playlist's cover picked from its releases' covers)."
        )
    )]
    pub source: CoverCandidateSource,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Whether this is the cover the entity shows.")
    )]
    pub active: bool,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Whether the image file is there. A cover whose file is confirmed gone is still listed, and still `active` if it would show, until serving its image, or a sync of an unlocked owner, removes it together with any pin to it."
        )
    )]
    pub available: bool,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "The provider that supplied a `provider` cover.")
    )]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<String>,
    pub cover: CoverResponse,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[non_exhaustive]
pub struct CoverCandidatesResponse {
    pub candidates: Vec<CoverCandidateResponse>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "The source pinned as the one to show; `null` when the shown cover follows the automatic order: manual, then provider, then local."
        )
    )]
    pub selection: Option<StoredCoverSource>,
}

impl From<CoverCandidates> for CoverCandidatesResponse {
    fn from(found: CoverCandidates) -> Self {
        Self {
            candidates: found
                .candidates
                .into_iter()
                .map(|candidate| CoverCandidateResponse {
                    source: candidate.source.into(),
                    active: candidate.active,
                    available: candidate.available,
                    provider_id: candidate.cover.provider_id.clone(),
                    cover: cover_to_response(candidate.cover),
                })
                .collect(),
            selection: found.selection.map(Into::into),
        }
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
pub(crate) struct ProviderCoverRequest {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "The provider whose cover search returned `url`.")
    )]
    pub(crate) provider_id: String,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "A candidate URL from the provider's cover search results.")
    )]
    pub(crate) url: String,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
pub(crate) struct CoverSelectionRequest {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "The stored source to show.")
    )]
    pub(crate) source: StoredCoverSource,
}

const UPLOAD_CONTENT_TYPES: [&str; 3] = ["image/jpeg", "image/png", "image/webp"];

fn require_image_content_type(headers: &HeaderMap) -> Result<(), AppError> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .ok_or_else(|| AppError::unsupported_media_type("missing Content-Type"))?
        .to_str()
        .map_err(|_| AppError::unsupported_media_type("invalid Content-Type"))?;
    let content_type = content_type.split(';').next().unwrap_or("").trim();
    if UPLOAD_CONTENT_TYPES
        .iter()
        .any(|allowed| content_type.eq_ignore_ascii_case(allowed))
    {
        return Ok(());
    }
    Err(AppError::unsupported_media_type(format!(
        "unsupported cover Content-Type: {content_type}. Supported: {}",
        UPLOAD_CONTENT_TYPES.join(", ")
    )))
}

async fn get_cover_candidates<K: CoverOwnerRoute>(
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<CoverCandidatesResponse>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    let found = selection::candidates(&STATE.db.get(), &principal, K::KIND, &id).await?;
    Ok(Json(found.into()))
}

async fn put_manual_cover<K: CoverOwnerRoute>(
    headers: HeaderMap,
    Path(id): Path<String>,
    body: UnreadBody,
) -> Result<Json<CoverCandidatesResponse>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    require_image_content_type(&headers)?;
    let body = body.read().await?;
    let found =
        selection::set_manual(&STATE.db.get(), &principal, K::KIND, &id, body.to_vec()).await?;
    Ok(Json(found.into()))
}

async fn delete_manual_cover<K: CoverOwnerRoute>(
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<CoverCandidatesResponse>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    let found = selection::remove_manual(&STATE.db.get(), &principal, K::KIND, &id).await?;
    Ok(Json(found.into()))
}

async fn put_provider_cover<K: CoverOwnerRoute>(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<ProviderCoverRequest>,
) -> Result<Json<CoverCandidatesResponse>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    let provider_id = super::providers::parse_provider_filter(Some(&request.provider_id))?
        .ok_or_else(|| AppError::bad_request("provider_id is required"))?;
    let found = selection::set_provider(
        &STATE.db.get(),
        &principal,
        K::KIND,
        &id,
        provider_id,
        &request.url,
    )
    .await?;
    Ok(Json(found.into()))
}

async fn put_cover_selection<K: CoverOwnerRoute>(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<CoverSelectionRequest>,
) -> Result<Json<CoverCandidatesResponse>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    let found = selection::select(
        &STATE.db.get(),
        &principal,
        K::KIND,
        &id,
        Some(request.source.into()),
    )
    .await?;
    Ok(Json(found.into()))
}

async fn delete_cover_selection<K: CoverOwnerRoute>(
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<CoverCandidatesResponse>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    let found = selection::select(&STATE.db.get(), &principal, K::KIND, &id, None).await?;
    Ok(Json(found.into()))
}

#[cfg(feature = "docgen")]
fn write_permission<K: CoverOwnerRoute>() -> &'static str {
    match K::KIND {
        CoverOwnerKind::Playlist => "Only the playlist's owner may change its cover.",
        _ => "Requires ManageMetadata, and the entity must be visible to the caller.",
    }
}

#[cfg(feature = "docgen")]
fn get_cover_candidates_docs<K: CoverOwnerRoute>(op: TransformOperation) -> TransformOperation {
    let generated = if matches!(K::KIND, CoverOwnerKind::Playlist) {
        " A playlist also lists its `generated` cover, picked from the covers of its releases the caller can see; it shows when the playlist has no manual cover."
    } else {
        ""
    };
    op.summary(&format!("List {} cover candidates", K::NAME))
        .description(&format!(
            "Returns the {name}'s stored covers, at most one per source, as `{{ candidates, selection }}`. Each candidate carries its `source`, whether it is `active` (the cover `inc=covers` returns), whether its image file is `available`, the `provider_id` of a provider cover, and its `cover` with the public image URL. The {name} shows its pinned `selection`, else its manual cover, else its provider cover, else its local cover. Listings read the stored covers without looking at their files; a cover whose file is confirmed gone is removed, with any pin to it, when its image is served or, unless the {name} is locked, when a sync finds it; the {name} then shows its next cover.{generated} 404 if the {name} is not found or not visible.",
            name = K::NAME
        ))
}

#[cfg(feature = "docgen")]
fn put_manual_cover_docs<K: CoverOwnerRoute>(op: TransformOperation) -> TransformOperation {
    let pins = if matches!(K::KIND, CoverOwnerKind::Playlist) {
        "The playlist shows it in place of its generated cover."
    } else {
        "It replaces any earlier manual cover and is pinned as the selection."
    };
    op.summary(&format!("Upload a {} cover", K::NAME))
        .description(&format!(
            "Stores the request body as the {name}'s manual cover. The body is the raw image, up to 20 MiB, with `Content-Type` `image/jpeg`, `image/png` or `image/webp`; the bytes must decode as one of those formats whatever the header says (400 otherwise, 415 for another Content-Type). {pins} {permission} Returns the updated candidates.",
            name = K::NAME,
            permission = write_permission::<K>(),
        ))
}

#[cfg(feature = "docgen")]
fn delete_manual_cover_docs<K: CoverOwnerRoute>(op: TransformOperation) -> TransformOperation {
    op.summary(&format!("Remove a {} manual cover", K::NAME))
        .description(&format!(
            "Removes the {name}'s manual cover, and the selection when it was pinned to it; the other stored covers stay. 404 when there is no manual cover. {permission} Returns the updated candidates.",
            name = K::NAME,
            permission = write_permission::<K>(),
        ))
}

#[cfg(feature = "docgen")]
fn put_provider_cover_docs<K: CoverOwnerRoute>(op: TransformOperation) -> TransformOperation {
    op.summary(&format!("Use a provider cover for a {}", K::NAME))
        .description(&format!(
            "Request body (JSON): `{{ provider_id, url }}`. Downloads `url` as the {name}'s provider cover, replacing only the stored provider cover, and pins it as the selection. `url` must be one of the candidates `POST /api/{name}s/{{id}}/covers/search` returns for that provider (400 otherwise, and for a provider without a {name} cover handler); 502 when the download fails. {permission} Returns the updated candidates.",
            name = K::NAME,
            permission = write_permission::<K>(),
        ))
}

#[cfg(feature = "docgen")]
fn put_cover_selection_docs<K: CoverOwnerRoute>(op: TransformOperation) -> TransformOperation {
    op.summary(&format!("Pin a {} cover source", K::NAME))
        .description(&format!(
            "Request body (JSON): `{{ source }}` with `local`, `provider` or `manual`. Pins that stored cover as the one the {name} shows; automatic scans and provider syncs never change the pin. 409 when the {name} has no cover from that source. {permission} Returns the updated candidates.",
            name = K::NAME,
            permission = write_permission::<K>(),
        ))
}

#[cfg(feature = "docgen")]
fn delete_cover_selection_docs<K: CoverOwnerRoute>(op: TransformOperation) -> TransformOperation {
    op.summary(&format!("Clear a {} cover pin", K::NAME))
        .description(&format!(
            "Clears the pinned source, so the {name} shows its manual cover, else its provider cover, else its local cover. {permission} Returns the updated candidates.",
            name = K::NAME,
            permission = write_permission::<K>(),
        ))
}

fn manual_cover_route<K: CoverOwnerRoute>() -> axum::routing::MethodRouter {
    put(put_manual_cover::<K>)
        .delete(delete_manual_cover::<K>)
        .layer(DefaultBodyLimit::max(covers::MAX_COVER_BYTES))
}

/// The cover choice routes of an entity collection, under `/{id}/covers`.
pub(crate) fn owner_cover_routes<K: CoverOwnerRoute>() -> Router {
    let router = Router::new()
        .route("/{id}/covers/candidates", get(get_cover_candidates::<K>))
        .route("/{id}/covers/manual", manual_cover_route::<K>());
    if matches!(K::KIND, CoverOwnerKind::Playlist) {
        return router;
    }
    router
        .route("/{id}/covers/provider", put(put_provider_cover::<K>))
        .route(
            "/{id}/covers/selection",
            put(put_cover_selection::<K>).delete(delete_cover_selection::<K>),
        )
}

#[cfg(feature = "docgen")]
pub(crate) fn owner_cover_openapi_routes<K: CoverOwnerRoute>() -> aide::axum::ApiRouter {
    use aide::axum::routing::{
        get_with,
        put_with,
    };

    let router = aide::axum::ApiRouter::new()
        .api_route(
            "/{id}/covers/candidates",
            get_with(get_cover_candidates::<K>, get_cover_candidates_docs::<K>),
        )
        .api_route(
            "/{id}/covers/manual",
            put_with(put_manual_cover::<K>, put_manual_cover_docs::<K>)
                .delete_with(delete_manual_cover::<K>, delete_manual_cover_docs::<K>),
        );
    if matches!(K::KIND, CoverOwnerKind::Playlist) {
        return router;
    }
    router
        .api_route(
            "/{id}/covers/provider",
            put_with(put_provider_cover::<K>, put_provider_cover_docs::<K>),
        )
        .api_route(
            "/{id}/covers/selection",
            put_with(put_cover_selection::<K>, put_cover_selection_docs::<K>).delete_with(
                delete_cover_selection::<K>,
                delete_cover_selection_docs::<K>,
            ),
        )
}

#[cfg(test)]
mod tests {
    use std::path::Path as FsPath;

    use super::*;

    #[test]
    fn cover_search_body_names_the_cache_bypass() {
        let query: CoverSearchQuery =
            serde_json::from_str(r#"{"provider":"demo","bypass_cover_cache":true}"#)
                .expect("parses");
        assert!(query.bypass_cover_cache);
        assert_eq!(query.provider.as_deref(), Some("demo"));
        let defaults: CoverSearchQuery = serde_json::from_str("{}").expect("parses");
        assert!(!defaults.bypass_cover_cache);
    }

    #[test]
    fn cover_to_response_sets_public_url() {
        let response = cover_to_response(lyra_db::Cover {
            db_id: None,
            id: "cover-1".to_string(),
            path: "/music/cover.jpg".to_string(),
            mime_type: "image/jpeg".to_string(),
            hash: "a".repeat(64),
            blurhash: None,
            provider_id: None,
        });

        assert_eq!(response.id, "cover-1");
        assert_eq!(
            response.url,
            format!("/api/covers/cover-1?v={}", "a".repeat(64))
        );
    }

    #[tokio::test]
    async fn serving_removes_a_cover_only_once_its_file_is_confirmed_gone() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let dir = tempfile::tempdir()?;
        let (gone, unmounted) = {
            let mut db = STATE.db.write().await;
            let mut stored = Vec::new();
            for (title, path) in [
                ("Gone", dir.path().join("gone.png")),
                ("Unmounted", dir.path().join("unmounted/cover.png")),
            ] {
                let release = lyra_db::fixtures::insert_release(&mut db, title)?;
                let cover = db.transaction_mut(|t| {
                    lyra_db::covers::upsert(
                        t,
                        release,
                        CoverSource::Local,
                        lyra_db::Cover {
                            db_id: None,
                            id: lyra_db::nanoid(),
                            path: path.to_string_lossy().into_owned(),
                            mime_type: "image/png".to_string(),
                            hash: title.to_string(),
                            blurhash: None,
                            provider_id: None,
                        },
                    )
                })?;
                stored.push((release, cover.cover.id));
            }
            (stored[0].clone(), stored[1].clone())
        };

        for (_, cover_id) in [&gone, &unmounted] {
            let served = get_public_cover(
                HeaderMap::new(),
                Path(cover_id.clone()),
                Query(CoverQuery::default()),
            )
            .await;
            assert!(served.is_err(), "nothing is served without its file");
        }
        let db = STATE.db.read().await;
        assert!(lyra_db::covers::get(&*db, gone.0)?.is_none());
        assert!(
            lyra_db::covers::get(&*db, unmounted.0)?.is_some(),
            "an unmounted directory is no evidence"
        );
        Ok(())
    }

    async fn admin_headers() -> anyhow::Result<HeaderMap> {
        let user_db_id = {
            let mut db = STATE.db.write().await;
            lyra_db::roles::ensure_builtin_roles(&mut db)?;
            let user_db_id =
                lyra_db::users::create(&mut db, &lyra_db::fixtures::test_user("admin")?)?;
            lyra_db::roles::ensure_user_has_role(
                &mut db,
                user_db_id,
                lyra_db::roles::BUILTIN_ADMIN_ROLE,
            )?;
            user_db_id
        };
        let session =
            crate::services::testing::create_session(user_db_id, Default::default()).await?;
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            format!("Bearer {}", session.token).parse()?,
        );
        Ok(headers)
    }

    async fn release_id() -> anyhow::Result<String> {
        let release_db_id =
            lyra_db::fixtures::insert_release(&mut *STATE.db.write().await, "Release")?;
        Ok(
            lyra_db::releases::get_by_id(&*STATE.db.read().await, release_db_id)?
                .expect("release exists")
                .id,
        )
    }

    fn upload(
        headers: &HeaderMap,
        uri: &str,
        content_type: &str,
        body: Vec<u8>,
    ) -> anyhow::Result<axum::http::Request<Body>> {
        let mut request = axum::http::Request::builder()
            .method(axum::http::Method::PUT)
            .uri(uri)
            .header(header::CONTENT_TYPE, content_type);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        Ok(request.body(Body::from(body))?)
    }

    #[tokio::test]
    async fn uploads_get_a_larger_body_limit_and_need_an_image_type() -> anyhow::Result<()> {
        use tower::ServiceExt;

        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let headers = admin_headers().await?;
        let id = release_id().await?;
        // The server-wide limit, which the upload route raises for itself.
        let app = owner_cover_routes::<ReleaseCovers>().layer(DefaultBodyLimit::max(256 * 1024));
        let uri = format!("/{id}/covers/manual");

        let response = app
            .clone()
            .oneshot(upload(&headers, &uri, "image/png", vec![0; 512 * 1024])?)
            .await?;
        assert_eq!(
            response.status(),
            axum::http::StatusCode::BAD_REQUEST,
            "a body over the server-wide limit reaches image validation"
        );

        let response = app
            .clone()
            .oneshot(upload(
                &headers,
                &uri,
                "image/png",
                vec![0; covers::MAX_COVER_BYTES + 1],
            )?)
            .await?;
        assert_eq!(response.status(), axum::http::StatusCode::PAYLOAD_TOO_LARGE);

        let response = app
            .oneshot(upload(&headers, &uri, "text/plain", vec![0; 16])?)
            .await?;
        assert_eq!(
            response.status(),
            axum::http::StatusCode::UNSUPPORTED_MEDIA_TYPE
        );
        Ok(())
    }

    #[tokio::test]
    async fn uploads_are_refused_before_their_body_is_read() -> anyhow::Result<()> {
        use std::sync::atomic::Ordering;

        use tower::ServiceExt;

        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let id = release_id().await?;
        let (body, read) = crate::routes::request::watched_body();
        let request = axum::http::Request::builder()
            .method(axum::http::Method::PUT)
            .uri(format!("/{id}/covers/manual"))
            .header(header::CONTENT_TYPE, "image/png")
            .body(body)?;

        let response = owner_cover_routes::<ReleaseCovers>()
            .oneshot(request)
            .await?;

        assert_eq!(response.status(), axum::http::StatusCode::UNAUTHORIZED);
        assert!(!read.load(Ordering::SeqCst), "the body stays unread");
        Ok(())
    }

    #[tokio::test]
    async fn playlists_have_no_selection_routes() -> anyhow::Result<()> {
        use tower::ServiceExt;

        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let headers = admin_headers().await?;
        let mut request = axum::http::Request::builder()
            .method(axum::http::Method::DELETE)
            .uri("/playlist-1/covers/selection");
        for (name, value) in &headers {
            request = request.header(name, value);
        }
        let response = owner_cover_routes::<PlaylistCovers>()
            .oneshot(request.body(Body::empty())?)
            .await?;
        assert_eq!(response.status(), axum::http::StatusCode::NOT_FOUND);
        Ok(())
    }

    #[tokio::test]
    async fn transformed_covers_are_served_from_the_cache_until_the_cover_goes()
    -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("cover.png");
        image::DynamicImage::new_rgb8(200, 200).save(&path)?;
        let hash = "0123456789abcdef0123456789abcdef";
        let cover_id = {
            let mut db = STATE.db.write().await;
            lyra_db::test_db::insert_library(
                &mut db,
                "Covers",
                dir.path().to_str().expect("UTF-8 temp path"),
            )?;
            let release = lyra_db::fixtures::insert_release(&mut db, "Release")?;
            db.transaction_mut(|t| {
                lyra_db::covers::upsert(
                    t,
                    release,
                    CoverSource::Local,
                    lyra_db::Cover {
                        db_id: None,
                        id: lyra_db::nanoid(),
                        path: path.to_string_lossy().into_owned(),
                        mime_type: "image/png".to_string(),
                        hash: hash.to_string(),
                        blurhash: None,
                        provider_id: None,
                    },
                )
            })?
            .cover
            .id
        };
        let query = || CoverQuery {
            max_width: Some(100),
            ..CoverQuery::default()
        };

        let response = get_public_cover(HeaderMap::new(), Path(cover_id.clone()), Query(query()))
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
        let served = image::load_from_memory(&body)?;
        assert_eq!((served.width(), served.height()), (128, 128));
        let cached = STATE.boot.get().cover_cache_dir.join(hash);
        assert_eq!(std::fs::read(cached.join("128x0.png"))?, body);

        let mut range = HeaderMap::new();
        range.insert(header::RANGE, "bytes=0-3".parse()?);
        let response = get_public_cover(range.clone(), Path(cover_id.clone()), Query(query()))
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert_eq!(response.status(), axum::http::StatusCode::PARTIAL_CONTENT);
        range.insert(header::IF_RANGE, "\"tag\"".parse()?);
        let response = get_public_cover(range, Path(cover_id.clone()), Query(query()))
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert_eq!(
            response.status(),
            axum::http::StatusCode::OK,
            "Lyra sets no validator an If-Range could match"
        );
        assert_eq!(
            axum::body::to_bytes(response.into_body(), usize::MAX).await?,
            body
        );

        for invalid in [
            CoverQuery {
                quality: Some(101),
                ..query()
            },
            CoverQuery {
                format: Some("gif".to_string()),
                ..query()
            },
        ] {
            let refused =
                get_public_cover(HeaderMap::new(), Path(cover_id.clone()), Query(invalid))
                    .await
                    .map(|_| ())
                    .expect_err("invalid options are refused");
            assert_eq!(
                axum::response::IntoResponse::into_response(refused).status(),
                axum::http::StatusCode::BAD_REQUEST
            );
        }

        std::fs::remove_file(&path)?;
        let gone = get_public_cover(HeaderMap::new(), Path(cover_id), Query(query())).await;
        assert!(gone.is_err());
        assert!(!cached.exists(), "a removed cover's transforms go with it");
        Ok(())
    }

    /// A stored local cover at `path` in a library at `root`, returning the cover's public id.
    async fn stored_library_cover(
        root: &FsPath,
        path: &FsPath,
        follow_links: bool,
    ) -> anyhow::Result<String> {
        let mut db = STATE.db.write().await;
        let library = lyra_db::test_db::insert_library(
            &mut db,
            "Covers",
            root.to_str().expect("UTF-8 temp path"),
        )?;
        let release = lyra_db::fixtures::insert_release(&mut db, "Release")?;
        db.transaction_mut(|t| {
            let library =
                lyra_db::libraries::get_by_id(t, library)?.expect("library was just created");
            lyra_db::libraries::update(
                t,
                &lyra_db::Library {
                    follow_links: Some(follow_links),
                    ..library
                },
            )?;
            let upserted = lyra_db::covers::upsert(
                t,
                release,
                CoverSource::Local,
                lyra_db::Cover {
                    db_id: None,
                    id: lyra_db::nanoid(),
                    path: path.to_string_lossy().into_owned(),
                    mime_type: "image/png".to_string(),
                    hash: "0123456789abcdef0123456789abcdef".to_string(),
                    blurhash: None,
                    provider_id: None,
                },
            )?;
            Ok(upserted.cover.id)
        })
    }

    #[tokio::test]
    async fn a_stored_cover_retargeted_out_of_the_library_is_not_served() -> anyhow::Result<()> {
        use axum::response::IntoResponse;

        for follow_links in [false, true] {
            let _guard = crate::services::testing::runtime_test_lock().await;
            crate::services::testing::init_default_test_state()?;
            let (library, outside) = (tempfile::tempdir()?, tempfile::tempdir()?);
            let cover = library.path().join("cover.png");
            image::DynamicImage::new_rgb8(4, 4).save(&cover)?;
            let cover_id = stored_library_cover(library.path(), &cover, follow_links).await?;
            let serve = |query| get_public_cover(HeaderMap::new(), Path(cover_id.clone()), query);

            let response = serve(Query(CoverQuery::default()))
                .await
                .map_err(|err| anyhow::anyhow!("{err:?}"))?;
            assert_eq!(response.status(), axum::http::StatusCode::OK);

            let config = outside.path().join("config.json");
            std::fs::write(&config, "secret")?;
            std::fs::remove_file(&cover)?;
            std::os::unix::fs::symlink(&config, &cover)?;

            for query in [
                CoverQuery::default(),
                CoverQuery {
                    max_width: Some(64),
                    ..CoverQuery::default()
                },
            ] {
                let refused = serve(Query(query))
                    .await
                    .map(|_| ())
                    .expect_err("a link to config.json is refused")
                    .into_response();
                assert_eq!(refused.status(), axum::http::StatusCode::NOT_FOUND);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn a_cover_link_to_audio_outside_every_library_is_not_served() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (library, outside) = (tempfile::tempdir()?, tempfile::tempdir()?);
        let target = outside.path().join("private.flac");
        std::fs::write(&target, "audio")?;
        let cover = library.path().join("cover.jpg");
        std::os::unix::fs::symlink(&target, &cover)?;
        let cover_id = stored_library_cover(library.path(), &cover, true).await?;

        let served = get_public_cover(
            HeaderMap::new(),
            Path(cover_id),
            Query(CoverQuery::default()),
        )
        .await;
        assert!(served.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn a_cover_link_out_of_the_library_needs_the_setting() -> anyhow::Result<()> {
        for follow_links in [false, true] {
            let _guard = crate::services::testing::runtime_test_lock().await;
            crate::services::testing::init_default_test_state()?;
            let (library, outside) = (tempfile::tempdir()?, tempfile::tempdir()?);
            let target = outside.path().join("cover.png");
            image::DynamicImage::new_rgb8(4, 4).save(&target)?;
            let cover = library.path().join("cover.png");
            std::os::unix::fs::symlink(&target, &cover)?;
            let cover_id = stored_library_cover(library.path(), &cover, follow_links).await?;

            let served = get_public_cover(
                HeaderMap::new(),
                Path(cover_id),
                Query(CoverQuery::default()),
            )
            .await;
            assert_eq!(served.is_ok(), follow_links);
        }
        Ok(())
    }
}
