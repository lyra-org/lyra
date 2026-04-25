// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use aide::axum::{
    ApiRouter,
    routing::{
        delete_with,
        get_with,
        post_with,
        put_with,
    },
};
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
use base64::{
    Engine,
    engine::general_purpose::URL_SAFE_NO_PAD,
};
use schemars::JsonSchema;
use serde::{
    Deserialize,
    Serialize,
};
use std::collections::HashMap;

use crate::{
    STATE,
    db::{
        self,
        favorites::{
            Cursor,
            FavoriteKind,
            LIST_HARD_LIMIT,
        },
    },
    routes::AppError,
    services::{
        auth::require_authenticated,
        favorites as favorite_service,
    },
};

const DEFAULT_LIST_LIMIT: u64 = 100;
const CHECK_HARD_CAP: usize = 500;

#[derive(Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
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

#[derive(Deserialize, JsonSchema)]
struct FavoriteStateResponseQuery {}

#[derive(Serialize, JsonSchema)]
struct FavoriteStateResponse {
    #[schemars(description = "Whether this target is favorited by the authenticated user.")]
    favorited: bool,
}

#[derive(Deserialize, JsonSchema)]
struct CheckRequest {
    #[schemars(description = "Target IDs to check. Maximum 500.")]
    target_ids: Vec<String>,
}

#[derive(Serialize, JsonSchema)]
struct CheckResponse {
    #[schemars(
        description = "Dense `{ [id]: bool }`. Missing and non-visible IDs map to `false`; \
                       validate client-side if you need to distinguish typos."
    )]
    favorited: HashMap<String, bool>,
}

#[derive(Deserialize, JsonSchema)]
struct ListQuery {
    #[schemars(description = "Entity kind to filter on. Required.")]
    entity: EntityParam,
    #[schemars(description = "Page size. Default 100, cap 500.")]
    limit: Option<u64>,
    #[schemars(description = "Opaque cursor from the previous page's `next_cursor`.")]
    cursor: Option<String>,
}

#[derive(Serialize, JsonSchema)]
struct ListResponse {
    items: Vec<FavoriteItem>,
    #[schemars(description = "Opaque cursor; `null` on the last page. Sole termination signal.")]
    next_cursor: Option<String>,
}

#[derive(Serialize, JsonSchema)]
struct FavoriteItem {
    target_id: String,
    entity: String,
    first_favorited_at_ms: i64,
    last_refreshed_at_ms: i64,
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
    match favorite_service::add(&mut db, principal.user_db_id, &target_id)? {
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
    let favorited = favorite_service::has(&db, principal.user_db_id, &target_id)?;
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
    let favorited = favorite_service::has_many(&db, principal.user_db_id, &request.target_ids)?;
    Ok(Json(CheckResponse { favorited }))
}

async fn list_favorites(
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> Result<Json<ListResponse>, AppError> {
    let principal = require_authenticated(&headers).await?;
    let limit = query
        .limit
        .unwrap_or(DEFAULT_LIST_LIMIT)
        .clamp(1, LIST_HARD_LIMIT);
    let cursor = query.cursor.as_deref().map(decode_cursor).transpose()?;

    let kind = FavoriteKind::from(query.entity);

    let db = STATE.db.read().await;
    let page = favorite_service::list(&db, principal.user_db_id, kind, limit, cursor)?;

    let items = page
        .edges
        .into_iter()
        .map(|edge| {
            let target_public_id = db::lookup::find_id_by_db_id(&*db, edge.target_db_id)?
                .ok_or_else(|| {
                    anyhow::anyhow!("favorite target missing public id: {}", edge.target_db_id.0)
                })?;
            Ok::<_, AppError>(FavoriteItem {
                target_id: target_public_id,
                entity: edge.kind.as_str().to_string(),
                first_favorited_at_ms: edge.first_favorited_at_ms,
                last_refreshed_at_ms: edge.last_refreshed_at_ms,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Json(ListResponse {
        items,
        next_cursor: page.next_cursor.map(encode_cursor),
    }))
}

fn looks_like_public_id(candidate: &str) -> bool {
    let len = candidate.len();
    if len < 6 || len > 64 {
        return false;
    }
    candidate
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

fn encode_cursor(cursor: Cursor) -> String {
    let mut buf = [0u8; 16];
    buf[..8].copy_from_slice(&cursor.first_favorited_at_ms.to_be_bytes());
    buf[8..].copy_from_slice(&cursor.target_db_id.to_be_bytes());
    URL_SAFE_NO_PAD.encode(buf)
}

// base64(16 bytes) = 22 chars. Length-check before decoding to bound allocation.
const CURSOR_BASE64_LEN: usize = 22;

fn decode_cursor(raw: &str) -> Result<Cursor, AppError> {
    if raw.len() != CURSOR_BASE64_LEN {
        return Err(AppError::bad_request("malformed cursor"));
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(raw.as_bytes())
        .map_err(|_| AppError::bad_request("malformed cursor"))?;
    if bytes.len() != 16 {
        return Err(AppError::bad_request("malformed cursor"));
    }
    let first_favorited_at_ms = i64::from_be_bytes(bytes[..8].try_into().unwrap());
    let target_db_id = i64::from_be_bytes(bytes[8..].try_into().unwrap());
    Ok(Cursor {
        first_favorited_at_ms,
        target_db_id,
    })
}

fn put_favorite_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Mark a target as favorited")
        .description(
            "Marks a target as favorited. Idempotent: repeated PUT requests refresh \
         `last_refreshed_at_ms` but leave `first_favorited_at_ms` unchanged, so paginated \
         lists are not reordered. To bump a favorite's position, DELETE then PUT. Returns \
         204 on success, 400 for malformed IDs, and 404 when the target is not a track, \
         release, artist, or visible playlist.",
        )
        .response::<204, ()>()
}

fn delete_favorite_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Unmark a target as favorited")
        .description(
            "Unmarks a target as favorited. Idempotent: returns 204 whether an edge existed \
         or not, including for private playlists the authenticated user can no longer read. \
         Returns 400 for malformed IDs and 404 only when the target is not a supported kind.",
        )
        .response::<204, ()>()
}

fn get_favorite_state_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Check target favorite state").description(
        "Returns `{ favorited: bool }`. Returns `false` for missing, unsupported, or \
             non-visible targets.",
    )
}

fn check_favorites_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Check target favorite states").description(
        "Checks up to 500 target IDs and returns `{ [id]: bool }`. Missing, unsupported, and \
         non-visible IDs all map to `false`; validate IDs client-side if you need to \
         distinguish typos.",
    )
}

fn list_favorites_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List favorites").description(
        "Returns favorites for one entity kind, paginated by `first_favorited_at_ms DESC` \
         with an opaque cursor. `next_cursor` is the only termination signal; visibility \
         filters can drop rows from any page, so `items.len() < limit` is not reliable.",
    )
}

pub fn favorite_routes() -> ApiRouter {
    ApiRouter::new()
        .api_route("/", get_with(list_favorites, list_favorites_docs))
        .api_route("/check", post_with(check_favorites, check_favorites_docs))
        .api_route("/{target_id}", put_with(put_favorite, put_favorite_docs))
        .api_route(
            "/{target_id}",
            delete_with(delete_favorite, delete_favorite_docs),
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
    fn cursor_roundtrip() {
        let cursor = Cursor {
            first_favorited_at_ms: 1_700_000_000_000,
            target_db_id: 42,
        };
        let encoded = encode_cursor(cursor);
        let decoded = decode_cursor(&encoded).expect("cursor roundtrips");
        assert_eq!(decoded, cursor);
    }

    #[test]
    fn decode_cursor_rejects_malformed() {
        assert!(decode_cursor("not-base64!").is_err());
        assert!(decode_cursor("c2hvcnQ").is_err()); // base64 of "short", < 16 bytes
    }

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
    fn list_query_requires_entity() {
        let result: Result<ListQuery, _> =
            serde_json::from_value(serde_json::json!({ "limit": 10 }));
        assert!(result.is_err(), "entity must be required");
    }
}
