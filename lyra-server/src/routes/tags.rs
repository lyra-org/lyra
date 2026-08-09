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
        patch,
        post,
    },
};
use serde::{
    Deserialize,
    Serialize,
};

use crate::{
    STATE,
    db,
    routes::{
        AppError,
        responses::PageResponse,
    },
    services::{
        auth::require_authenticated,
        pagination::SnapshotKey,
        tags::{
            self as tag_service,
            CreateResult,
            TagServiceError,
        },
    },
};

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Deserialize, Serialize)]
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

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct CreateTagRequest {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Tag name. Normalized server-side; see endpoint description.")
    )]
    tag: String,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Tag color. Used only on create; ignored on reuse (PATCH to recolor)."
        )
    )]
    color: TagColor,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Track, release, artist, or playlist ID to attach.")
    )]
    target_id: String,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct UpdateTagRequest {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "New tag name. Normalized server-side; returns 409 on collision.")
    )]
    tag: Option<String>,
    #[cfg_attr(feature = "docgen", schemars(description = "New tag color."))]
    color: Option<TagColor>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct TagResponse {
    id: String,
    tag: String,
    color: String,
    created_at: String,
}

fn tag_to_response(tag: db::Tag) -> TagResponse {
    TagResponse {
        id: tag.id,
        tag: tag.tag,
        color: tag.color,
        created_at: super::unix_ms_to_rfc3339_i64(tag.created_at_ms),
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct ListQuery {
    #[serde(flatten)]
    page: super::PageQuery,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct TargetStateResponse {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Whether this target is attached to the tag.")
    )]
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
) -> Result<Json<PageResponse<TagResponse>>, AppError> {
    let principal = require_authenticated(&headers).await?;
    let page_request = query.page.resolve_snapshot();
    let snapshot_key = SnapshotKey::builder(&principal.user_public_id, "tags").finish();

    let db = STATE.db.read().await;
    let (tags, next_cursor) = if let Some(page) = page_request.resume(&snapshot_key)? {
        (
            tag_service::hydrate_tag_snapshot(&db, principal.user_db_id, &page.item_ids)?,
            page.next_cursor,
        )
    } else {
        let mut tags = tag_service::list_for_user(&db, principal.user_db_id)?;
        let page = page_request.start(
            &snapshot_key,
            tags.iter().map(|tag| tag.id.clone()).collect(),
        )?;
        tags.truncate(page.item_ids.len());
        (tags, page.next_cursor)
    };

    Ok(Json(PageResponse {
        items: tags.into_iter().map(tag_to_response).collect(),
        next_cursor,
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
) -> Result<Json<PageResponse<String>>, AppError> {
    let principal = require_authenticated(&headers).await?;
    if !looks_like_public_id(&id) {
        return Err(AppError::bad_request(format!("malformed tag id: {id}")));
    }
    let page_request = query.page.resolve_snapshot();
    let snapshot_key = SnapshotKey::builder(&principal.user_public_id, "tag-targets")
        .field(Some(&id))
        .finish();

    let db = STATE.db.read().await;
    let tag_db_id = tag_service::resolve_owned_tag_id(&db, principal.user_db_id, &id)?;
    let (targets, next_cursor) = if let Some(page) = page_request.resume(&snapshot_key)? {
        (
            tag_service::hydrate_target_snapshot(
                &db,
                principal.user_db_id,
                tag_db_id,
                &page.item_ids,
            )?,
            page.next_cursor,
        )
    } else {
        let mut targets = tag_service::list_targets(&db, principal.user_db_id, tag_db_id)?;
        let page = page_request.start(
            &snapshot_key,
            targets.iter().map(|target| target.snapshot_id()).collect(),
        )?;
        targets.truncate(page.item_ids.len());
        (targets, page.next_cursor)
    };

    Ok(Json(PageResponse {
        items: targets.into_iter().map(|target| target.target_id).collect(),
        next_cursor,
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
        request.color.map(TagColor::as_str),
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

#[cfg(feature = "docgen")]
fn create_tag_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Attach tag to target")
        .description(
            "Creates or reuses a tag and attaches it to a target. Returns 201 when a tag is \
         created and 204 when an existing tag is reused. On reuse, `color` is ignored; use \
         PATCH to recolor. \
         Tag names are normalized (invisibles stripped, `White_Space` trimmed, NFC, \
         case-sensitive); control characters or names over 128 codepoints return 400. Target \
         must be an accessible track, release, artist, or visible playlist; otherwise returns 404.",
        )
        .response::<201, ()>()
        .response::<204, ()>()
}

#[cfg(feature = "docgen")]
fn list_tags_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List tags").description(
        "Returns `{ items, next_cursor }` for the authenticated user's tags, ordered by creation \
         time descending with a stable tiebreaker. The first page creates a bounded snapshot, so \
         later renames and inserts do not shift continuation pages. `next_cursor` is the only \
         termination signal.",
    )
}

#[cfg(feature = "docgen")]
fn get_tag_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get tag")
        .description("Returns one of the authenticated user's tags. Returns 404 if not found or owned by another user.")
}

#[cfg(feature = "docgen")]
fn list_tag_targets_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List tag targets").description(
        "Returns public target IDs as `{ items, next_cursor }` in a bounded snapshot order. \
         Continuation pages recheck edge existence and target visibility, so removed or newly \
         hidden targets are skipped. `next_cursor` is the only termination signal.",
    )
}

#[cfg(feature = "docgen")]
fn get_tag_target_state_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Check tag target state").description(
        "Returns `{ tagged: bool }`. Returns `false` for missing, unsupported, or \
         non-visible targets. Returns 404 if the tag is not owned by the authenticated user.",
    )
}

#[cfg(feature = "docgen")]
fn update_tag_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Update tag")
        .description("Renames and/or recolors one of the authenticated user's tags. Request body: `{tag?, color?}`; at least one field is required. Returns 409 on rename collisions.")
}

#[cfg(feature = "docgen")]
fn delete_tag_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Delete tag").response::<204, ()>()
}

#[cfg(feature = "docgen")]
fn delete_tag_target_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Detach tag from target")
        .description(
            "Detaches a target from one of the authenticated user's tags. Idempotent for valid \
         tag and target resources. The tag remains available when its final target is detached. \
         No visibility gate is applied: authenticated users can remove their tag edge even when \
         the target later becomes non-visible.",
        )
        .response::<204, ()>()
}

pub fn tag_routes() -> Router {
    Router::new()
        .route("/", post(create_tag))
        .route("/", get(list_tags))
        .route("/{id}", get(get_tag))
        .route("/{id}", patch(update_tag))
        .route("/{id}", delete(delete_tag))
        .route("/{id}/targets", get(list_tag_targets))
        .route("/{id}/targets/{target_id}", get(get_tag_target_state))
        .route("/{id}/targets/{target_id}", delete(delete_tag_target))
}

#[cfg(feature = "docgen")]
pub(crate) fn tag_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::{
        delete_with,
        get_with,
        patch_with,
        post_with,
    };

    aide::axum::ApiRouter::new()
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
