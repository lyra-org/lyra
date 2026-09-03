// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
use aide::transform::TransformOperation;
use axum::{
    Json,
    Router,
    extract::Path,
    http::HeaderMap,
    routing::{
        get,
        post,
    },
};

use crate::{
    STATE,
    db,
    routes::AppError,
    services::{
        auth::{
            Principal,
            access,
            require_manage_metadata,
        },
        metadata::editing::{
            self,
            MetadataEditingError,
            model::{
                MetadataApplyRequest,
                MetadataPreviewRequest,
                MetadataPreviewResponse,
                MetadataSnapshot,
            },
        },
    },
};

fn require_entity_access(
    db: &impl db::DbAccess,
    principal: &Principal,
    public_id: &str,
) -> Result<agdb::DbId, AppError> {
    let entity_id = db::lookup::find_node_id_by_id(db, public_id)?
        .ok_or_else(|| AppError::not_found(format!("Entity not found: {public_id}")))?;
    let accessible = if db::artists::get_by_id(db, entity_id)?.is_some() {
        access::artist_accessible(db, principal, entity_id)?
    } else {
        access::entity_accessible(db, principal, entity_id)?
    };
    if !accessible {
        return Err(AppError::not_found(format!(
            "Entity not found: {public_id}"
        )));
    }
    Ok(entity_id)
}

fn editing_error(error: MetadataEditingError) -> AppError {
    match error {
        MetadataEditingError::BadRequest(message) => AppError::bad_request(message),
        MetadataEditingError::EntityNotFound(id) => {
            AppError::not_found(format!("Entity not found: {id}"))
        }
        MetadataEditingError::Conflict(conflicts) => AppError::json_conflict(serde_json::json!({
            "code": "metadata_edit_conflict",
            "message": "metadata changed after preview",
            "conflicts": conflicts,
        })),
        MetadataEditingError::Internal(error) => AppError::from(error),
    }
}

async fn get_entity_metadata(
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<MetadataSnapshot>, AppError> {
    let principal = require_manage_metadata(&headers).await?;
    let db = STATE.db.read().await;
    let entity_id = require_entity_access(&*db, &principal, &id)?;
    editing::get_snapshot(&db, &principal, entity_id)
        .map(Json)
        .map_err(editing_error)
}

async fn preview_entity_metadata(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<MetadataPreviewRequest>,
) -> Result<Json<MetadataPreviewResponse>, AppError> {
    let principal = require_manage_metadata(&headers).await?;
    let db = STATE.db.read().await;
    let entity_id = require_entity_access(&*db, &principal, &id)?;
    editing::preview(&db, &principal, entity_id, &request)
        .map(Json)
        .map_err(editing_error)
}

async fn apply_entity_metadata(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<MetadataApplyRequest>,
) -> Result<Json<MetadataSnapshot>, AppError> {
    let principal = require_manage_metadata(&headers).await?;
    let mut db = STATE.db.write().await;
    let entity_id = require_entity_access(&*db, &principal, &id)?;
    editing::apply(&mut db, &principal, entity_id, &request)
        .map(Json)
        .map_err(editing_error)
}

#[cfg(feature = "docgen")]
fn get_entity_metadata_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get editable entity metadata").description(
        "Returns the canonical editable metadata projection for a release, track, or artist. The keys in `fields` are the authoritative editable inventory; each entry carries the current `value` and its `source`, `manual` when the field is manually owned and `resolved` otherwise. Omitted fields are unavailable. Requires ManageMetadata.",
    )
}

#[cfg(feature = "docgen")]
fn preview_entity_metadata_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Preview an entity metadata edit").description(
        "Validates and normalizes a proposed edit without modifying metadata. Returns the authoritative field diff and an opaque, short-lived preview ID required by PATCH. Requires ManageMetadata.",
    )
}

#[cfg(feature = "docgen")]
fn apply_entity_metadata_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Apply a previewed entity metadata edit").description(
        "Consumes a short-lived preview ID and atomically applies that exact release, track, or artist metadata edit when every previewed field value and source still matches. Returns the fresh canonical metadata snapshot, or 409 with machine-readable field conflicts when metadata changed after preview. Requires ManageMetadata.",
    )
}

pub(crate) fn entity_metadata_routes() -> Router {
    Router::new()
        .route(
            "/{id}/metadata",
            get(get_entity_metadata).patch(apply_entity_metadata),
        )
        .route("/{id}/metadata/preview", post(preview_entity_metadata))
}

#[cfg(feature = "docgen")]
pub(crate) fn entity_metadata_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::{
        get_with,
        post_with,
    };

    aide::axum::ApiRouter::new()
        .api_route(
            "/{id}/metadata",
            get_with(get_entity_metadata, get_entity_metadata_docs)
                .patch_with(apply_entity_metadata, apply_entity_metadata_docs),
        )
        .api_route(
            "/{id}/metadata/preview",
            post_with(preview_entity_metadata, preview_entity_metadata_docs),
        )
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{
            Body,
            to_bytes,
        },
        http::{
            Request,
            StatusCode,
            header::{
                AUTHORIZATION,
                CONTENT_TYPE,
            },
        },
    };
    use nanoid::nanoid;
    use tower::ServiceExt;

    use super::*;
    use crate::{
        db::test_db::{
            connect_artist,
            insert_artist,
            insert_library,
            insert_release,
        },
        services::auth::sessions,
        testing::{
            LibraryFixtureConfig,
            initialize_runtime,
            runtime_test_lock,
        },
    };

    #[tokio::test]
    async fn metadata_routes_hide_and_preserve_inaccessible_relations() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        initialize_runtime(&LibraryFixtureConfig {
            directory: std::path::PathBuf::from("."),
            language: None,
            country: None,
        })
        .await?;

        let (user_id, source_public_id, hidden_public_id, source_id) = {
            let mut db = STATE.db.write().await;
            db::roles::ensure_builtin_roles(&mut db)?;
            let user_id = db::users::create(
                &mut db,
                &db::test_db::test_user("entity-metadata-relation-manager")?,
            )?;
            let role_name = format!("metadata-manager-{}", nanoid!());
            db::roles::create(
                &mut db,
                &db::roles::Role {
                    db_id: None,
                    id: nanoid!(),
                    name: role_name.clone(),
                    permissions: vec![db::Permission::ManageMetadata],
                },
            )?;
            db::roles::ensure_user_has_role(&mut db, user_id, &role_name)?;

            let visible_library_id =
                insert_library(&mut db, "Visible Metadata", "/tmp/metadata-route-visible")?;
            let hidden_library_id =
                insert_library(&mut db, "Hidden Metadata", "/tmp/metadata-route-hidden")?;
            db::libraries::grant_access(
                &mut db,
                user_id,
                visible_library_id,
                db::libraries::AccessKind::ReadWrite,
            )?;
            let visible_release_id = insert_release(&mut db, "Visible Release")?;
            let hidden_release_id = insert_release(&mut db, "Hidden Release")?;
            db::graph::ensure_owned_edge(&mut db, visible_library_id, visible_release_id)?;
            db::graph::ensure_owned_edge(&mut db, hidden_library_id, hidden_release_id)?;
            let source_id = insert_artist(&mut db, "Visible Source")?;
            let hidden_id = insert_artist(&mut db, "Hidden Target")?;
            connect_artist(&mut db, visible_release_id, source_id)?;
            connect_artist(&mut db, hidden_release_id, hidden_id)?;
            db::artists::relations::link(
                &mut db,
                source_id,
                hidden_id,
                db::ArtistRelationType::MemberOf,
                Some("hidden detail".to_string()),
            )?;
            let source_public_id = db::artists::get_by_id(&db, source_id)?
                .expect("source exists")
                .id;
            let hidden_public_id = db::artists::get_by_id(&db, hidden_id)?
                .expect("target exists")
                .id;
            (user_id, source_public_id, hidden_public_id, source_id)
        };
        let session = sessions::create_session_for_user(user_id, Default::default()).await?;
        let authorization = format!("Bearer {}", session.token);
        let routes = entity_metadata_routes();

        let response = routes
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/{source_public_id}/metadata"))
                    .header(AUTHORIZATION, &authorization)
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let snapshot: serde_json::Value = serde_json::from_slice(&body)?;
        assert!(snapshot["fields"].get("relations").is_none());
        assert!(!String::from_utf8_lossy(&body).contains(&hidden_public_id));

        let preview_response = routes
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/{source_public_id}/metadata/preview"))
                    .header(AUTHORIZATION, &authorization)
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&serde_json::json!({
                        "changes": [{
                            "field": "relations",
                            "operation": "set",
                            "value": []
                        }]
                    }))?))?,
            )
            .await?;
        let preview_status = preview_response.status();
        let preview_body = to_bytes(preview_response.into_body(), usize::MAX).await?;
        assert_eq!(
            preview_status,
            StatusCode::BAD_REQUEST,
            "{}",
            String::from_utf8_lossy(&preview_body),
        );

        let hidden_response = routes
            .oneshot(
                Request::builder()
                    .uri(format!("/{hidden_public_id}/metadata"))
                    .header(AUTHORIZATION, authorization)
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(hidden_response.status(), StatusCode::NOT_FOUND);
        let db = STATE.db.read().await;
        assert_eq!(
            db::artists::relations::get_relations_from(&db, source_id, None)?.len(),
            1,
        );
        Ok(())
    }
}
