// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use aide::axum::{
    ApiRouter,
    routing::{
        delete_with,
        get_with,
        patch_with,
        post_with,
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

use crate::{
    STATE,
    db::{
        self,
        tags::{
            LIST_HARD_LIMIT,
            TagListCursor,
            TargetListCursor,
        },
    },
    routes::AppError,
    services::{
        auth::require_authenticated,
        tags::{
            self as tag_service,
            CreateResult,
            TagServiceError,
        },
    },
};

const DEFAULT_LIST_LIMIT: u64 = 100;
const LIST_CURSOR_BASE64_LEN: usize = 22; // base64 of (i64, i64) = 16 bytes
const TARGET_CURSOR_BASE64_LEN: usize = 11; // base64 of (i64) = 8 bytes

#[derive(Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
enum TagColor {
    Red,
    Orange,
    Yellow,
    Green,
    Blue,
    Purple,
    Pink,
    Gray,
}

impl TagColor {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Red => "red",
            Self::Orange => "orange",
            Self::Yellow => "yellow",
            Self::Green => "green",
            Self::Blue => "blue",
            Self::Purple => "purple",
            Self::Pink => "pink",
            Self::Gray => "gray",
        }
    }
}

#[derive(Deserialize, JsonSchema)]
struct CreateTagRequest {
    #[schemars(description = "Tag name. Normalized server-side; see endpoint description.")]
    tag: String,
    #[schemars(
        description = "Tag color. Used only on create; ignored on reuse (PATCH to recolor)."
    )]
    color: TagColor,
    #[schemars(description = "Track, release, artist, or playlist ID to attach.")]
    target_id: String,
}

#[derive(Deserialize, JsonSchema)]
struct UpdateTagRequest {
    #[schemars(description = "New tag name. Normalized server-side; returns 409 on collision.")]
    tag: Option<String>,
    #[schemars(description = "New tag color.")]
    color: Option<TagColor>,
}

#[derive(Serialize, JsonSchema)]
struct TagResponse {
    id: String,
    tag: String,
    color: String,
    created_at_ms: i64,
}

fn tag_to_response(tag: db::Tag) -> TagResponse {
    TagResponse {
        id: tag.id,
        tag: tag.tag,
        color: tag.color,
        created_at_ms: tag.created_at_ms,
    }
}

#[derive(Deserialize, JsonSchema)]
struct ListQuery {
    #[schemars(description = "Page size. Default 100, cap 500.")]
    limit: Option<u64>,
    #[schemars(description = "Opaque cursor from the previous page's `next_cursor`.")]
    cursor: Option<String>,
}

#[derive(Serialize, JsonSchema)]
struct TagListResponse {
    items: Vec<TagResponse>,
    #[schemars(description = "Opaque cursor; `null` on the last page. Sole termination signal.")]
    next_cursor: Option<String>,
}

#[derive(Serialize, JsonSchema)]
struct TargetListResponse {
    target_ids: Vec<String>,
    #[schemars(description = "Opaque cursor; `null` on the last page. Sole termination signal.")]
    next_cursor: Option<String>,
}

#[derive(Serialize, JsonSchema)]
struct TargetStateResponse {
    #[schemars(description = "Whether this target is attached to the tag.")]
    tagged: bool,
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

fn encode_list_cursor(cursor: TagListCursor) -> String {
    let mut buf = [0u8; 16];
    buf[..8].copy_from_slice(&cursor.created_at_ms.to_be_bytes());
    buf[8..].copy_from_slice(&cursor.tag_db_id.to_be_bytes());
    URL_SAFE_NO_PAD.encode(buf)
}

fn decode_list_cursor(raw: &str) -> Result<TagListCursor, AppError> {
    if raw.len() != LIST_CURSOR_BASE64_LEN {
        return Err(AppError::bad_request("malformed cursor"));
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(raw.as_bytes())
        .map_err(|_| AppError::bad_request("malformed cursor"))?;
    if bytes.len() != 16 {
        return Err(AppError::bad_request("malformed cursor"));
    }
    let created_at_ms = i64::from_be_bytes(bytes[..8].try_into().unwrap());
    let tag_db_id = i64::from_be_bytes(bytes[8..].try_into().unwrap());
    Ok(TagListCursor {
        created_at_ms,
        tag_db_id,
    })
}

fn encode_target_cursor(cursor: TargetListCursor) -> String {
    URL_SAFE_NO_PAD.encode(cursor.target_db_id.to_be_bytes())
}

fn decode_target_cursor(raw: &str) -> Result<TargetListCursor, AppError> {
    if raw.len() != TARGET_CURSOR_BASE64_LEN {
        return Err(AppError::bad_request("malformed cursor"));
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(raw.as_bytes())
        .map_err(|_| AppError::bad_request("malformed cursor"))?;
    if bytes.len() != 8 {
        return Err(AppError::bad_request("malformed cursor"));
    }
    Ok(TargetListCursor {
        target_db_id: i64::from_be_bytes(bytes[..8].try_into().unwrap()),
    })
}

impl From<TagServiceError> for AppError {
    fn from(err: TagServiceError) -> Self {
        match err {
            TagServiceError::BadTagName(e) => AppError::bad_request(e.to_string()),
            TagServiceError::EmptyColor => AppError::bad_request("color cannot be empty"),
            TagServiceError::NotTargetable => {
                AppError::not_found("tag target not found or not accessible")
            }
            TagServiceError::NotFound => AppError::not_found("tag not found"),
            TagServiceError::RenameConflict => {
                AppError::conflict("tag name already exists for this user")
            }
            TagServiceError::EmptyPatch => AppError::bad_request(
                "empty patch body — at least one of `tag` or `color` must be provided",
            ),
            TagServiceError::Internal(err) => AppError::from(err),
        }
    }
}

async fn create_tag(
    headers: HeaderMap,
    Json(request): Json<CreateTagRequest>,
) -> Result<StatusCode, AppError> {
    let principal = require_authenticated(&headers).await?;
    if !looks_like_public_id(&request.target_id) {
        return Err(AppError::bad_request(format!(
            "malformed target id: {}",
            request.target_id
        )));
    }

    let mut db = STATE.db.write().await;
    let outcome = tag_service::create(
        &mut db,
        principal.user_db_id,
        &request.target_id,
        &request.tag,
        request.color.as_str(),
    )?;
    match outcome {
        CreateResult::Created => Ok(StatusCode::CREATED),
        CreateResult::Reused => Ok(StatusCode::NO_CONTENT),
    }
}

async fn list_tags(
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> Result<Json<TagListResponse>, AppError> {
    let principal = require_authenticated(&headers).await?;
    let limit = query
        .limit
        .unwrap_or(DEFAULT_LIST_LIMIT)
        .clamp(1, LIST_HARD_LIMIT);
    let cursor = query
        .cursor
        .as_deref()
        .map(decode_list_cursor)
        .transpose()?;

    let db = STATE.db.read().await;
    let page = tag_service::list_for_user(&db, principal.user_db_id, limit, cursor)?;

    Ok(Json(TagListResponse {
        items: page.tags.into_iter().map(tag_to_response).collect(),
        next_cursor: page.next_cursor.map(encode_list_cursor),
    }))
}

async fn get_tag(
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<TagResponse>, AppError> {
    let principal = require_authenticated(&headers).await?;
    if !looks_like_public_id(&id) {
        return Err(AppError::bad_request(format!("malformed tag id: {id}")));
    }

    let db = STATE.db.read().await;
    let tag = tag_service::get_by_public_id(&db, principal.user_db_id, &id)?;
    Ok(Json(tag_to_response(tag)))
}

async fn list_tag_targets(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<ListQuery>,
) -> Result<Json<TargetListResponse>, AppError> {
    let principal = require_authenticated(&headers).await?;
    if !looks_like_public_id(&id) {
        return Err(AppError::bad_request(format!("malformed tag id: {id}")));
    }
    let limit = query
        .limit
        .unwrap_or(DEFAULT_LIST_LIMIT)
        .clamp(1, LIST_HARD_LIMIT);
    let cursor = query
        .cursor
        .as_deref()
        .map(decode_target_cursor)
        .transpose()?;

    let db = STATE.db.read().await;
    let tag_db_id = tag_service::resolve_owned_tag_id(&db, principal.user_db_id, &id)?;
    let page = tag_service::list_targets(&db, principal.user_db_id, tag_db_id, limit, cursor)?;

    let mut target_ids = Vec::with_capacity(page.target_db_ids.len());
    for db_id in page.target_db_ids {
        if let Some(public_id) = db::lookup::find_id_by_db_id(&*db, db_id)? {
            target_ids.push(public_id);
        }
    }

    Ok(Json(TargetListResponse {
        target_ids,
        next_cursor: page.next_cursor.map(encode_target_cursor),
    }))
}

async fn get_tag_target_state(
    headers: HeaderMap,
    Path((id, target_id)): Path<(String, String)>,
) -> Result<Json<TargetStateResponse>, AppError> {
    let principal = require_authenticated(&headers).await?;
    if !looks_like_public_id(&id) {
        return Err(AppError::bad_request(format!("malformed tag id: {id}")));
    }
    if !looks_like_public_id(&target_id) {
        return Err(AppError::bad_request(format!(
            "malformed target id: {target_id}"
        )));
    }

    let db = STATE.db.read().await;
    let tag_db_id = tag_service::resolve_owned_tag_id(&db, principal.user_db_id, &id)?;
    let tagged =
        tag_service::has_target_by_tag_id(&db, principal.user_db_id, tag_db_id, &target_id)?;
    Ok(Json(TargetStateResponse { tagged }))
}

async fn update_tag(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<UpdateTagRequest>,
) -> Result<Json<TagResponse>, AppError> {
    let principal = require_authenticated(&headers).await?;
    if !looks_like_public_id(&id) {
        return Err(AppError::bad_request(format!("malformed tag id: {id}")));
    }

    let mut db = STATE.db.write().await;
    let tag_db_id = tag_service::resolve_owned_tag_id(&db, principal.user_db_id, &id)?;
    let tag = tag_service::update(
        &mut db,
        principal.user_db_id,
        tag_db_id,
        request.tag.as_deref(),
        request.color.map(|c| c.as_str()).as_deref(),
    )?;
    Ok(Json(tag_to_response(tag)))
}

async fn delete_tag(headers: HeaderMap, Path(id): Path<String>) -> Result<StatusCode, AppError> {
    let principal = require_authenticated(&headers).await?;
    if !looks_like_public_id(&id) {
        return Err(AppError::bad_request(format!("malformed tag id: {id}")));
    }

    let mut db = STATE.db.write().await;
    let tag_db_id = tag_service::resolve_owned_tag_id(&db, principal.user_db_id, &id)?;
    tag_service::delete(&mut db, principal.user_db_id, tag_db_id)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_tag_target(
    headers: HeaderMap,
    Path((id, target_id)): Path<(String, String)>,
) -> Result<StatusCode, AppError> {
    let principal = require_authenticated(&headers).await?;
    if !looks_like_public_id(&id) {
        return Err(AppError::bad_request(format!("malformed tag id: {id}")));
    }
    if !looks_like_public_id(&target_id) {
        return Err(AppError::bad_request(format!(
            "malformed target id: {target_id}"
        )));
    }

    let mut db = STATE.db.write().await;
    let tag_db_id = tag_service::resolve_owned_tag_id(&db, principal.user_db_id, &id)?;
    tag_service::remove_target_by_tag_id(&mut db, principal.user_db_id, tag_db_id, &target_id)?;
    Ok(StatusCode::NO_CONTENT)
}

fn create_tag_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Attach tag to target")
        .description(
            "Creates or reuses a tag and attaches it to a target. Returns 201 when a tag is \
         created and 204 when an existing tag is reused. On reuse, `color` is ignored; use \
         PATCH to recolor. \
         Tag names are normalized (invisibles stripped, `White_Space` trimmed, NFC, \
         case-sensitive); control characters or names over 128 codepoints return 400. Target \
         must be a track, release, artist, or visible playlist; otherwise returns 404.",
        )
        .response::<201, ()>()
        .response::<204, ()>()
}

fn list_tags_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List tags").description(
        "Returns the authenticated user's tags, paginated by `created_at_ms` descending with \
         a stable tiebreaker. Renaming does not reorder tags. `next_cursor` is the only \
         termination signal.",
    )
}

fn get_tag_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get tag")
        .description("Returns one of the authenticated user's tags. Returns 404 if not found or owned by another user.")
}

fn list_tag_targets_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List tag targets").description(
        "Returns paginated public target IDs in a stable cursor order. Non-visible targets are \
         filtered out; underlying edges persist and reappear when visibility is restored. \
         `next_cursor` is the only termination signal.",
    )
}

fn get_tag_target_state_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Check tag target state").description(
        "Returns `{ tagged: bool }`. Returns `false` for missing, unsupported, or \
         non-visible targets. Returns 404 if the tag is not owned by the authenticated user.",
    )
}

fn update_tag_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Update tag")
        .description("Renames and/or recolors one of the authenticated user's tags. Request body: `{tag?, color?}`; at least one field is required. Returns 409 on rename collisions.")
}

fn delete_tag_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Delete tag").response::<204, ()>()
}

fn delete_tag_target_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Detach tag from target")
        .description(
            "Detaches a target from one of the authenticated user's tags. Idempotent for valid \
         tag and target resources. No visibility gate is applied: authenticated users can \
         remove their tag edge even when the target later becomes non-visible.",
        )
        .response::<204, ()>()
}

pub fn tag_routes() -> ApiRouter {
    ApiRouter::new()
        .api_route("/", post_with(create_tag, create_tag_docs))
        .api_route("/", get_with(list_tags, list_tags_docs))
        .api_route("/{id}", get_with(get_tag, get_tag_docs))
        .api_route("/{id}", patch_with(update_tag, update_tag_docs))
        .api_route("/{id}", delete_with(delete_tag, delete_tag_docs))
        .api_route(
            "/{id}/targets",
            get_with(list_tag_targets, list_tag_targets_docs),
        )
        .api_route(
            "/{id}/targets/{target_id}",
            get_with(get_tag_target_state, get_tag_target_state_docs),
        )
        .api_route(
            "/{id}/targets/{target_id}",
            delete_with(delete_tag_target, delete_tag_target_docs),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_cursor_roundtrip() {
        let cursor = TagListCursor {
            created_at_ms: 1_700_000_000_000,
            tag_db_id: 42,
        };
        let encoded = encode_list_cursor(cursor);
        let decoded = decode_list_cursor(&encoded).expect("roundtrip");
        assert_eq!(decoded, cursor);
    }

    #[test]
    fn target_cursor_roundtrip() {
        let cursor = TargetListCursor { target_db_id: 42 };
        let encoded = encode_target_cursor(cursor);
        let decoded = decode_target_cursor(&encoded).expect("roundtrip");
        assert_eq!(decoded, cursor);
    }

    #[test]
    fn list_cursor_rejects_wrong_length() {
        assert!(decode_list_cursor("shortcursor").is_err());
        assert!(decode_list_cursor(&"x".repeat(1000)).is_err());
    }

    #[test]
    fn target_cursor_rejects_wrong_length() {
        assert!(decode_target_cursor("shortcursor").is_err());
        assert!(decode_target_cursor(&"x".repeat(1000)).is_err());
    }
}
