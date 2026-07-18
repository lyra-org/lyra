// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
use aide::transform::TransformOperation;
use axum::{
    Json,
    extract::{
        Path,
        Query,
    },
    http::{
        HeaderMap,
        StatusCode,
    },
};
use axum::{
    Router,
    routing::{
        delete,
        get,
        post,
        put,
    },
};
use serde::{
    Deserialize,
    Serialize,
};
use std::collections::HashMap;

use crate::{
    STATE,
    db::favorites::FavoriteKind,
    routes::{
        AppError,
        responses::PageResponse,
    },
    services::{
        auth::require_authenticated,
        favorites as favorite_service,
        pagination::SnapshotKey,
    },
};

const CHECK_HARD_CAP: usize = 500;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum EntityParam {
    Track,
    Release,
    Artist,
    Playlist,
}

impl From<EntityParam> for FavoriteKind {
    fn from(value: EntityParam) -> Self {
        match value {
            EntityParam::Track => Self::Track,
            EntityParam::Release => Self::Release,
            EntityParam::Artist => Self::Artist,
            EntityParam::Playlist => Self::Playlist,
        }
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct FavoriteStateResponseQuery {}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct FavoriteStateResponse {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Whether this target is favorited by the authenticated user.")
    )]
    favorited: bool,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct CheckRequest {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Target IDs to check. Maximum 500.")
    )]
    target_ids: Vec<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct CheckResponse {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Dense `{ [id]: bool }`. Missing and non-visible IDs map to `false`; \
                       validate client-side if you need to distinguish typos."
        )
    )]
    favorited: HashMap<String, bool>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct ListQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Entity kind to filter on. Required.")
    )]
    entity: EntityParam,
    #[serde(flatten)]
    page: super::PageQuery,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct FavoriteItem {
    target_id: String,
    entity: String,
    first_favorited_at: String,
    last_refreshed_at: String,
}

fn favorite_item_from_list_item(item: favorite_service::ListItem) -> FavoriteItem {
    FavoriteItem {
        target_id: item.target_id,
        entity: item.kind.as_str().to_string(),
        first_favorited_at: super::unix_ms_to_rfc3339_i64(item.first_favorited_at_ms),
        last_refreshed_at: super::unix_ms_to_rfc3339_i64(item.last_refreshed_at_ms),
    }
}

async fn put_favorite(
    headers: HeaderMap,
    Path(target_id): Path<String>,
) -> Result<StatusCode, AppError> {
    let principal = require_authenticated(&headers).await?;
    if !looks_like_public_id(&target_id) {
        return Err(AppError::bad_request(format!(
            "malformed target id: {target_id}"
        )));
    }

    let mut db = STATE.db.write().await;
    match favorite_service::add_for_principal(&mut db, &principal, &target_id)? {
        favorite_service::MutationOutcome::Applied(_) => Ok(StatusCode::NO_CONTENT),
        favorite_service::MutationOutcome::NotTargetable => Err(AppError::not_found(format!(
            "favorite target not found: {target_id}"
        ))),
    }
}

async fn delete_favorite(
    headers: HeaderMap,
    Path(target_id): Path<String>,
) -> Result<StatusCode, AppError> {
    let principal = require_authenticated(&headers).await?;
    if !looks_like_public_id(&target_id) {
        return Err(AppError::bad_request(format!(
            "malformed target id: {target_id}"
        )));
    }

    let mut db = STATE.db.write().await;
    match favorite_service::remove(&mut db, principal.user_db_id, &target_id)? {
        favorite_service::MutationOutcome::Applied(_) => Ok(StatusCode::NO_CONTENT),
        favorite_service::MutationOutcome::NotTargetable => Err(AppError::not_found(format!(
            "favorite target not found: {target_id}"
        ))),
    }
}

async fn get_favorite_state(
    headers: HeaderMap,
    Path(target_id): Path<String>,
    Query(_): Query<FavoriteStateResponseQuery>,
) -> Result<Json<FavoriteStateResponse>, AppError> {
    let principal = require_authenticated(&headers).await?;
    if !looks_like_public_id(&target_id) {
        return Err(AppError::bad_request(format!(
            "malformed target id: {target_id}"
        )));
    }

    let db = STATE.db.read().await;
    let favorited = favorite_service::has_for_principal(&db, &principal, &target_id)?;
    Ok(Json(FavoriteStateResponse { favorited }))
}

async fn check_favorites(
    headers: HeaderMap,
    Json(request): Json<CheckRequest>,
) -> Result<Json<CheckResponse>, AppError> {
    let principal = require_authenticated(&headers).await?;

    if request.target_ids.len() > CHECK_HARD_CAP {
        return Err(AppError::bad_request(format!(
            "check cap exceeded: {} > {CHECK_HARD_CAP}",
            request.target_ids.len(),
        )));
    }

    let db = STATE.db.read().await;
    let favorited = favorite_service::has_many_for_principal(&db, &principal, &request.target_ids)?;
    Ok(Json(CheckResponse { favorited }))
}

async fn list_favorites(
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> Result<Json<PageResponse<FavoriteItem>>, AppError> {
    let principal = require_authenticated(&headers).await?;
    let page_request = query.page.resolve_snapshot();
    let kind = FavoriteKind::from(query.entity);
    let snapshot_key = SnapshotKey::builder(&principal.user_public_id, "favorites")
        .field(Some(kind.as_str()))
        .finish();

    let db = STATE.db.read().await;
    let (list_items, next_cursor) = if let Some(page) = page_request.resume(&snapshot_key)? {
        (
            favorite_service::hydrate_snapshot(&db, &principal, kind, &page.item_ids)?,
            page.next_cursor,
        )
    } else {
        let mut list_items = favorite_service::list(&db, &principal, kind)?;
        let page = page_request.start(
            &snapshot_key,
            list_items.iter().map(|item| item.snapshot_id()).collect(),
        )?;
        list_items.truncate(page.item_ids.len());
        (list_items, page.next_cursor)
    };

    let items: Vec<FavoriteItem> = list_items
        .into_iter()
        .map(favorite_item_from_list_item)
        .collect();

    Ok(Json(PageResponse { items, next_cursor }))
}

fn looks_like_public_id(candidate: &str) -> bool {
    let len = candidate.len();
    if !(6..=64).contains(&len) {
        return false;
    }
    candidate
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

#[cfg(feature = "docgen")]
fn put_favorite_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Mark a target as favorited")
        .description(
            "Marks a target as favorited. Idempotent: repeated PUT requests refresh \
         `last_refreshed_at` but leave `first_favorited_at` unchanged, so paginated \
         lists are not reordered. To bump a favorite's position, DELETE then PUT. Returns \
         204 on success, 400 for malformed IDs, and 404 when the target is not a track, \
         release, artist, or visible playlist.",
        )
        .response::<204, ()>()
}

#[cfg(feature = "docgen")]
fn delete_favorite_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Unmark a target as favorited")
        .description(
            "Unmarks a target as favorited. Idempotent: returns 204 whether an edge existed \
         or not, including for private playlists the authenticated user can no longer read. \
         Returns 400 for malformed IDs and 404 only when the target is not a supported kind.",
        )
        .response::<204, ()>()
}

#[cfg(feature = "docgen")]
fn get_favorite_state_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Check target favorite state").description(
        "Returns `{ favorited: bool }`. Returns `false` for missing, unsupported, or \
             non-visible targets.",
    )
}

#[cfg(feature = "docgen")]
fn check_favorites_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Check target favorite states").description(
        "Checks up to 500 target IDs and returns `{ [id]: bool }`. Missing, unsupported, and \
         non-visible IDs all map to `false`; validate IDs client-side if you need to \
         distinguish typos.",
    )
}

#[cfg(feature = "docgen")]
fn list_favorites_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List favorites").description(
        "Returns `{ items, next_cursor }` for one entity kind, ordered by creation time \
         descending. The first page creates a bounded snapshot; continuation pages retain that \
         order while rechecking current target visibility. Removed or newly hidden targets can \
         make `items.len() < limit`. Repeat `entity` with the cursor and drive iteration from \
         `next_cursor`.",
    )
}

pub fn favorite_routes() -> Router {
    Router::new()
        .route("/", get(list_favorites))
        .route("/check", post(check_favorites))
        .route("/{target_id}", put(put_favorite))
        .route("/{target_id}", delete(delete_favorite))
        .route("/{target_id}", get(get_favorite_state))
}

#[cfg(feature = "docgen")]
pub(crate) fn favorite_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::{
        get_with,
        post_with,
        put_with,
    };

    aide::axum::ApiRouter::new()
        .api_route("/", get_with(list_favorites, list_favorites_docs))
        .api_route("/check", post_with(check_favorites, check_favorites_docs))
        .api_route("/{target_id}", put_with(put_favorite, put_favorite_docs))
        .api_route(
            "/{target_id}",
            aide::axum::routing::delete_with(delete_favorite, delete_favorite_docs),
        )
        .api_route(
            "/{target_id}",
            get_with(get_favorite_state, get_favorite_state_docs),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn looks_like_public_id_accepts_nanoid_shape() {
        assert!(looks_like_public_id("V1StGXR8_Z5jdHi6B-myT"));
        assert!(looks_like_public_id("abc123"));
    }

    #[test]
    fn looks_like_public_id_rejects_malformed() {
        assert!(!looks_like_public_id(""));
        assert!(!looks_like_public_id("abc"));
        assert!(!looks_like_public_id("has spaces"));
        assert!(!looks_like_public_id("has/slashes"));
        assert!(!looks_like_public_id(&"x".repeat(100)));
    }

    #[test]
    fn favorite_item_from_list_item_formats_fields() {
        let entry = favorite_service::ListItem {
            edge_db_id: agdb::DbId(1),
            target_id: "tr-current".to_string(),
            kind: FavoriteKind::Track,
            first_favorited_at_ms: 1,
            last_refreshed_at_ms: 2,
        };
        let item = favorite_item_from_list_item(entry);
        assert_eq!(item.target_id, "tr-current");
        assert_eq!(item.entity, "track");
        assert_eq!(item.first_favorited_at, "1970-01-01T00:00:00.001Z");
        assert_eq!(item.last_refreshed_at, "1970-01-01T00:00:00.002Z");
    }

    #[test]
    fn list_query_requires_entity() {
        let result: Result<ListQuery, _> =
            serde_json::from_value(serde_json::json!({ "limit": 10 }));
        assert!(result.is_err(), "entity must be required");
    }
}
