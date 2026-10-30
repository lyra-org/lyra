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

use crate::routes::request::bearer_credential;
use crate::routes::targets::{
    resolve_target,
    validate_target_id,
};
use crate::{
    routes::{
        AppError,
        responses::PageResponse,
    },
    services::STATE,
    services::{
        auth::require_authenticated,
        pagination::SnapshotKey,
        tags::{
            self as tag_service,
            CreateResult,
            NewTag,
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

fn tag_to_response(tag: lyra_db::Tag) -> TagResponse {
    TagResponse {
        id: tag.id,
        tag: tag.tag,
        color: tag.color,
        created_at: crate::services::timestamps::unix_ms_to_rfc3339_i64(tag.created_at_ms),
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
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    validate_target_id(&request.target_id)?;

    let mut db = STATE.db.write().await;
    let user_db_id = principal.require(&db)?;
    let tag = NewTag::parse(&request.tag, request.color.as_str())?;
    let target_db_id = resolve_target(&*db, &request.target_id, || {
        TagServiceError::NotTargetable.into()
    })?;
    let outcome = tag_service::create(&mut db, user_db_id, target_db_id, &tag)?;
    match outcome {
        CreateResult::Created => Ok(StatusCode::CREATED),
        CreateResult::Reused => Ok(StatusCode::NO_CONTENT),
    }
}

async fn list_tags(
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> Result<Json<PageResponse<TagResponse>>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    let page_request = query.page.resolve_snapshot();
    let snapshot_key = SnapshotKey::builder(&principal.user_public_id, "tags").finish();

    let db = STATE.db.read().await;
    let user_db_id = principal.require(&db)?;
    let (tags, next_cursor) = if let Some(page) = page_request.resume(&snapshot_key)? {
        (
            tag_service::hydrate_tag_snapshot(&db, user_db_id, &page.item_ids)?,
            page.next_cursor,
        )
    } else {
        let mut tags = tag_service::list_for_user(&db, user_db_id)?;
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
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    if !crate::services::opaque_ids::is_public_id(&id) {
        return Err(AppError::bad_request(format!("malformed tag id: {id}")));
    }

    let db = STATE.db.read().await;
    let user_db_id = principal.require(&db)?;
    let tag = tag_service::get_by_public_id(&db, user_db_id, &id)?;
    Ok(Json(tag_to_response(tag)))
}

async fn list_tag_targets(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<ListQuery>,
) -> Result<Json<PageResponse<String>>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    if !crate::services::opaque_ids::is_public_id(&id) {
        return Err(AppError::bad_request(format!("malformed tag id: {id}")));
    }
    let page_request = query.page.resolve_snapshot();
    let snapshot_key = SnapshotKey::builder(&principal.user_public_id, "tag-targets")
        .field(Some(&id))
        .finish();

    let db = STATE.db.read().await;
    let user_db_id = principal.require(&db)?;
    let tag_db_id = tag_service::resolve_owned_tag_id(&db, user_db_id, &id)?;
    let (targets, next_cursor) = if let Some(page) = page_request.resume(&snapshot_key)? {
        (
            tag_service::hydrate_target_snapshot(&db, user_db_id, tag_db_id, &page.item_ids)?,
            page.next_cursor,
        )
    } else {
        let mut targets = tag_service::list_targets(&db, user_db_id, tag_db_id)?;
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
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    if !crate::services::opaque_ids::is_public_id(&id) {
        return Err(AppError::bad_request(format!("malformed tag id: {id}")));
    }
    validate_target_id(&target_id)?;

    let db = STATE.db.read().await;
    let user_db_id = principal.require(&db)?;
    let tag = tag_service::get_by_public_id(&db, user_db_id, &id)?;
    let tagged = match lyra_db::lookup::find_node_id_by_id(&*db, &target_id)? {
        Some(target_db_id) => tag_service::has_target(&db, user_db_id, target_db_id, &tag.tag)?,
        None => false,
    };
    Ok(Json(TargetStateResponse { tagged }))
}

async fn update_tag(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<UpdateTagRequest>,
) -> Result<Json<TagResponse>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    if !crate::services::opaque_ids::is_public_id(&id) {
        return Err(AppError::bad_request(format!("malformed tag id: {id}")));
    }

    let mut db = STATE.db.write().await;
    let user_db_id = principal.require(&db)?;
    let tag_db_id = tag_service::resolve_owned_tag_id(&db, user_db_id, &id)?;
    let tag = tag_service::update(
        &mut db,
        user_db_id,
        tag_db_id,
        request.tag.as_deref(),
        request.color.map(TagColor::as_str),
    )?;
    Ok(Json(tag_to_response(tag)))
}

async fn delete_tag(headers: HeaderMap, Path(id): Path<String>) -> Result<StatusCode, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    if !crate::services::opaque_ids::is_public_id(&id) {
        return Err(AppError::bad_request(format!("malformed tag id: {id}")));
    }

    let mut db = STATE.db.write().await;
    let user_db_id = principal.require(&db)?;
    let tag_db_id = tag_service::resolve_owned_tag_id(&db, user_db_id, &id)?;
    tag_service::delete(&mut db, user_db_id, tag_db_id)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_tag_target(
    headers: HeaderMap,
    Path((id, target_id)): Path<(String, String)>,
) -> Result<StatusCode, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    if !crate::services::opaque_ids::is_public_id(&id) {
        return Err(AppError::bad_request(format!("malformed tag id: {id}")));
    }
    validate_target_id(&target_id)?;

    let mut db = STATE.db.write().await;
    let user_db_id = principal.require(&db)?;
    let tag = tag_service::get_by_public_id(&db, user_db_id, &id)?;
    let target_db_id = resolve_target(&*db, &target_id, || TagServiceError::NotTargetable.into())?;
    tag_service::remove_target(&mut db, user_db_id, target_db_id, &tag.tag)?;
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

pub(super) fn me_tag_routes() -> Router {
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
pub(super) fn me_tag_openapi_routes() -> aide::axum::ApiRouter {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        plugins::testing::init_library_test_state,
        services::testing::runtime_test_lock,
    };
    use axum::{
        body::{
            Body,
            to_bytes,
        },
        http::{
            Request,
            header::{
                AUTHORIZATION,
                CONTENT_TYPE,
            },
        },
    };
    use lyra_db::fixtures::{
        connect,
        insert_track,
    };
    use lyra_db::test_db::insert_library;
    use tower::ServiceExt;

    async fn send(
        routes: &Router,
        method: &str,
        uri: &str,
        authorization: &str,
        body: Option<serde_json::Value>,
    ) -> anyhow::Result<(StatusCode, String)> {
        let request = Request::builder()
            .method(method)
            .uri(uri)
            .header(AUTHORIZATION, authorization);
        let request = match body {
            Some(body) => request
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&body)?))?,
            None => request.body(Body::empty())?,
        };
        let response = routes.clone().oneshot(request).await?;
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        Ok((status, String::from_utf8(body.to_vec())?))
    }

    #[tokio::test]
    async fn tag_target_routes_resolve_public_ids() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_library_test_state(std::path::PathBuf::from(".")).await?;

        let (user_db_id, track_id, user_id) = {
            let mut db = STATE.db.write().await;
            lyra_db::roles::ensure_builtin_roles(&mut db)?;
            let user_db_id = lyra_db::users::create(
                &mut db,
                &lyra_db::fixtures::test_user("tags-route-admin")?,
            )?;
            lyra_db::roles::ensure_user_has_role(
                &mut db,
                user_db_id,
                lyra_db::roles::BUILTIN_ADMIN_ROLE,
            )?;
            let library = insert_library(&mut db, "Tags", "/tmp/lyra-tags-route")?;
            let track = insert_track(&mut db, "Tagged Track")?;
            connect(&mut db, library, track)?;
            (
                user_db_id,
                lyra_db::lookup::find_id_by_db_id(&db, track)?.unwrap(),
                lyra_db::lookup::find_id_by_db_id(&db, user_db_id)?.unwrap(),
            )
        };
        let session =
            crate::services::testing::create_session(user_db_id, Default::default()).await?;
        let auth = format!("Bearer {}", session.token);
        let routes = me_tag_routes();
        let missing_id = "missing-tag-target";
        let not_targetable = (
            StatusCode::NOT_FOUND,
            "Error: tag target not found or not accessible".to_string(),
        );
        let tag_not_found = (StatusCode::NOT_FOUND, "Error: tag not found".to_string());
        let create_body = |target_id: &str| {
            Some(serde_json::json!({ "tag": "Mood", "color": "blue", "target_id": target_id }))
        };

        assert_eq!(
            send(&routes, "POST", "/", &auth, create_body("bad!id")).await?,
            (
                StatusCode::BAD_REQUEST,
                "Error: malformed target id: bad!id".to_string()
            ),
        );
        for target in [missing_id, user_id.as_str()] {
            assert_eq!(
                send(&routes, "POST", "/", &auth, create_body(target)).await?,
                not_targetable,
            );
        }
        assert_eq!(
            send(
                &routes,
                "POST",
                "/",
                &auth,
                Some(serde_json::json!({
                    "tag": "bad\u{0}name",
                    "color": "blue",
                    "target_id": missing_id,
                })),
            )
            .await?
            .0,
            StatusCode::BAD_REQUEST,
            "the tag name is validated before the target resolves",
        );
        assert_eq!(
            send(&routes, "POST", "/", &auth, create_body(&track_id))
                .await?
                .0,
            StatusCode::CREATED,
        );
        assert_eq!(
            send(&routes, "POST", "/", &auth, create_body(&track_id))
                .await?
                .0,
            StatusCode::NO_CONTENT,
        );
        let tag_id = {
            let db = STATE.db.read().await;
            lyra_db::tags::list_for_user(&db, user_db_id)?
                .pop()
                .expect("tag created")
                .id
        };

        let state = |target: &str| format!("/{tag_id}/targets/{target}");
        for (target, tagged) in [
            (missing_id, false),
            (user_id.as_str(), false),
            (track_id.as_str(), true),
        ] {
            assert_eq!(
                send(&routes, "GET", &state(target), &auth, None).await?,
                (StatusCode::OK, format!(r#"{{"tagged":{tagged}}}"#)),
            );
        }
        assert_eq!(
            send(
                &routes,
                "GET",
                &format!("/missing-tag/targets/{track_id}"),
                &auth,
                None
            )
            .await?,
            tag_not_found,
        );

        for target in [missing_id, user_id.as_str()] {
            assert_eq!(
                send(&routes, "DELETE", &state(target), &auth, None).await?,
                not_targetable,
            );
        }
        assert_eq!(
            send(
                &routes,
                "DELETE",
                &format!("/missing-tag/targets/{missing_id}"),
                &auth,
                None
            )
            .await?,
            tag_not_found,
        );
        for _ in 0..2 {
            assert_eq!(
                send(&routes, "DELETE", &state(&track_id), &auth, None)
                    .await?
                    .0,
                StatusCode::NO_CONTENT,
            );
        }
        assert_eq!(
            send(&routes, "GET", &state(&track_id), &auth, None).await?,
            (StatusCode::OK, r#"{"tagged":false}"#.to_string()),
        );
        Ok(())
    }

    #[tokio::test]
    async fn tag_target_routes_hide_foreign_tags_and_hidden_targets() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_library_test_state(std::path::PathBuf::from(".")).await?;

        let (owner, viewer, visible, visible_id, hidden_id, tag_id) = {
            let mut db = STATE.db.write().await;
            lyra_db::roles::ensure_builtin_roles(&mut db)?;
            let owner = lyra_db::users::create(
                &mut db,
                &lyra_db::fixtures::test_user("tags-route-owner")?,
            )?;
            let viewer = lyra_db::users::create(
                &mut db,
                &lyra_db::fixtures::test_user("tags-route-viewer")?,
            )?;
            let visible_library =
                insert_library(&mut db, "Visible", "/tmp/lyra-tags-route-visible")?;
            let hidden_library = insert_library(&mut db, "Hidden", "/tmp/lyra-tags-route-hidden")?;
            for user in [owner, viewer] {
                lyra_db::libraries::grant_access(
                    &mut *db,
                    user,
                    visible_library,
                    lyra_db::libraries::AccessKind::ReadWrite,
                )?;
            }
            let visible = insert_track(&mut db, "Visible Track")?;
            let hidden = insert_track(&mut db, "Hidden Track")?;
            connect(&mut db, visible_library, visible)?;
            connect(&mut db, hidden_library, hidden)?;
            tag_service::create(&mut db, owner, visible, &NewTag::parse("Mood", "blue")?)?;
            let tag_id = lyra_db::tags::list_for_user(&db, owner)?
                .pop()
                .expect("tag created")
                .id;
            (
                owner,
                viewer,
                visible,
                lyra_db::lookup::find_id_by_db_id(&db, visible)?.unwrap(),
                lyra_db::lookup::find_id_by_db_id(&db, hidden)?.unwrap(),
                tag_id,
            )
        };
        let session = crate::services::testing::create_session(viewer, Default::default()).await?;
        let auth = format!("Bearer {}", session.token);
        let routes = me_tag_routes();
        let tag_not_found = (StatusCode::NOT_FOUND, "Error: tag not found".to_string());

        for method in ["GET", "DELETE"] {
            assert_eq!(
                send(
                    &routes,
                    method,
                    &format!("/{tag_id}/targets/{visible_id}"),
                    &auth,
                    None
                )
                .await?,
                tag_not_found,
                "{method} on another user's tag",
            );
        }
        assert_eq!(
            send(
                &routes,
                "POST",
                "/",
                &auth,
                Some(serde_json::json!({ "tag": "Mood", "color": "blue", "target_id": hidden_id })),
            )
            .await?,
            (
                StatusCode::NOT_FOUND,
                "Error: tag target not found or not accessible".to_string()
            ),
        );
        assert_eq!(
            send(
                &routes,
                "POST",
                "/",
                &auth,
                Some(
                    serde_json::json!({ "tag": "Mood", "color": "blue", "target_id": visible_id })
                ),
            )
            .await?
            .0,
            StatusCode::CREATED,
        );

        let db = STATE.db.read().await;
        assert!(lyra_db::tags::has_target(&*db, owner, visible, "Mood")?);
        Ok(())
    }
}
