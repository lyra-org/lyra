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
    routes::AppError,
    services::STATE,
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
                MetadataFieldDiff,
                MetadataPreviewRequest,
                MetadataSnapshot,
            },
        },
    },
};

fn require_entity_access(
    db: &impl lyra_db::DbAccess,
    principal: &Principal,
    public_id: &str,
) -> Result<agdb::DbId, AppError> {
    let entity_id = lyra_db::lookup::find_node_id_by_id(db, public_id)?
        .ok_or_else(|| AppError::not_found(format!("Entity not found: {public_id}")))?;
    let accessible = if lyra_db::artists::get_by_id(db, entity_id)?.is_some() {
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
        MetadataEditingError::Conflict(current) => AppError::json_conflict(serde_json::json!({
            "code": "metadata_edit_conflict",
            "message": "metadata changed after preview",
            "current": current,
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
    editing::get_snapshot(&db, entity_id)
        .map(Json)
        .map_err(editing_error)
}

async fn preview_entity_metadata(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<MetadataPreviewRequest>,
) -> Result<Json<Vec<MetadataFieldDiff>>, AppError> {
    let principal = require_manage_metadata(&headers).await?;
    let db = STATE.db.read().await;
    let entity_id = require_entity_access(&*db, &principal, &id)?;
    editing::preview(&db, entity_id, &request)
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
    editing::apply(&mut db, entity_id, &request)
        .map(Json)
        .map_err(editing_error)
}

#[cfg(feature = "docgen")]
fn get_entity_metadata_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get editable entity metadata").description(
        "Returns the canonical editable metadata projection for a release, track, or artist. The keys in `fields` are the authoritative editable inventory; each entry carries the current `value` and its `source`, `manual` when a manual edit supplies the field and `resolved` otherwise. An artist's `relations` are the relations into it, such as a band's members or a character's voice actors, each naming the relating `artist_id`. Requires ManageMetadata.",
    )
}

#[cfg(feature = "docgen")]
fn preview_entity_metadata_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Preview an entity metadata edit").description(
        "Validates and normalizes a proposed edit without modifying metadata and returns the resulting field diff: one entry per field whose value or source would change, each with its `before` and `after` state. Edits with no effect, invalid values, and references that do not resolve are rejected with 400. Requires ManageMetadata.",
    )
}

#[cfg(feature = "docgen")]
fn apply_entity_metadata_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Apply a previewed entity metadata edit").description(
        "Atomically applies a release, track, or artist metadata edit. The body carries the `changes` and the `expected` diff returned by preview. The diff is recomputed against current metadata; on a match the edit is applied and the fresh snapshot is returned. Responds 400 for anything preview rejects or an `expected` entry whose field is not in `changes`, and 409 with `current` (the recomputed diff, possibly empty) when it differs from `expected`. Requires ManageMetadata.",
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
        plugins::testing::init_library_test_state,
        services::testing::runtime_test_lock,
    };
    use lyra_db::fixtures::{
        insert_artist,
        insert_release,
    };
    use lyra_db::test_db::{
        connect_artist,
        insert_library,
    };

    async fn send(
        routes: &Router,
        authorization: &str,
        method: &str,
        uri: String,
        body: Option<serde_json::Value>,
    ) -> anyhow::Result<(StatusCode, serde_json::Value)> {
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
        Ok((status, serde_json::from_slice(&body).unwrap_or_default()))
    }

    fn set(field: &str, value: serde_json::Value) -> serde_json::Value {
        serde_json::json!({"changes": [{"field": field, "operation": "set", "value": value}]})
    }

    #[tokio::test]
    async fn library_scoped_managers_reference_artists_and_labels_from_any_library()
    -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_library_test_state(std::path::PathBuf::from(".")).await?;

        let public_id = |db: &agdb::DbAny, id: agdb::DbId| -> anyhow::Result<String> {
            let element = db
                .exec(agdb::QueryBuilder::select().values(["id"]).ids(id).query())?
                .elements
                .remove(0);
            Ok(element.values[0].value.string()?.clone())
        };
        let (user_id, ids, release_id, outsider_id) = {
            let mut db = STATE.db.write().await;
            lyra_db::roles::ensure_builtin_roles(&mut db)?;
            let user_id = lyra_db::users::create(
                &mut db,
                &lyra_db::fixtures::test_user("entity-metadata-reference-manager")?,
            )?;
            let role_name = format!("metadata-manager-{}", nanoid!());
            lyra_db::roles::create(
                &mut db,
                &lyra_db::roles::Role {
                    db_id: None,
                    id: nanoid!(),
                    name: role_name.clone(),
                    permissions: vec![lyra_db::Permission::ManageMetadata],
                },
            )?;
            lyra_db::roles::ensure_user_has_role(&mut db, user_id, &role_name)?;

            let visible_library_id =
                insert_library(&mut db, "Visible Metadata", "/tmp/metadata-route-visible")?;
            let other_library_id =
                insert_library(&mut db, "Other Metadata", "/tmp/metadata-route-other")?;
            lyra_db::libraries::grant_access(
                &mut db,
                user_id,
                visible_library_id,
                lyra_db::libraries::AccessKind::ReadWrite,
            )?;
            let release_id = insert_release(&mut db, "Visible Release")?;
            let other_release_id = insert_release(&mut db, "Other Release")?;
            lyra_db::graph::ensure_owned_edge(&mut db, visible_library_id, release_id)?;
            lyra_db::graph::ensure_owned_edge(&mut db, other_library_id, other_release_id)?;
            let character_id = insert_artist(&mut db, "Character")?;
            let actor_id = insert_artist(&mut db, "Uncredited Actor")?;
            let outsider_id = insert_artist(&mut db, "Outsider")?;
            connect_artist(&mut db, release_id, character_id)?;
            connect_artist(&mut db, other_release_id, outsider_id)?;
            lyra_db::artists::relations::link(
                &mut db,
                actor_id,
                character_id,
                lyra_db::ArtistRelationType::VoiceActor,
                None,
            )?;
            let label_id = lyra_db::labels::resolve(
                &mut db,
                &lyra_db::labels::ResolveLabel {
                    name: "Outside Label",
                    external_id: None,
                },
            )?;
            db.transaction_mut(|t| {
                lyra_db::labels::sync_release_label_links_inside_tx(
                    t,
                    other_release_id,
                    &[lyra_db::labels::LabelLinkInput {
                        label_id,
                        catalog_number: None,
                    }],
                )
            })?;
            let ids = [
                character_id,
                actor_id,
                outsider_id,
                label_id,
                release_id,
                other_release_id,
            ]
            .map(|id| public_id(&db, id))
            .into_iter()
            .collect::<anyhow::Result<Vec<_>>>()?;
            (user_id, ids, release_id, outsider_id)
        };
        let [character, actor, outsider, label, release, other_release] =
            <[String; 6]>::try_from(ids).expect("six ids");
        let session = crate::services::testing::create_session(user_id, Default::default()).await?;
        let authorization = format!("Bearer {}", session.token);
        let routes = entity_metadata_routes();

        // A character's full cast, including a voice actor no release credits.
        let (status, snapshot) = send(
            &routes,
            &authorization,
            "GET",
            format!("/{character}/metadata"),
            None,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            snapshot["fields"]["relations"]["value"][0]["artist_id"],
            serde_json::json!(actor)
        );

        // An artist credited only in another library can be credited here.
        let credit = set(
            "credits",
            serde_json::json!([{"artist_id": outsider, "type": "artist", "detail": null}]),
        );
        let (status, expected) = send(
            &routes,
            &authorization,
            "POST",
            format!("/{release}/metadata/preview"),
            Some(credit.clone()),
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "{expected}");
        let mut apply_body = credit;
        apply_body["expected"] = expected;
        let (status, body) = send(
            &routes,
            &authorization,
            "PATCH",
            format!("/{release}/metadata"),
            Some(apply_body),
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        {
            let db = STATE.db.read().await;
            let credited: Vec<agdb::DbId> = lyra_db::artists::get(&*db, release_id)?
                .into_iter()
                .filter_map(|artist| artist.db_id.map(agdb::DbId::from))
                .collect();
            assert_eq!(credited, vec![outsider_id]);
        }

        // So can a label only another library's releases carry.
        let (status, body) = send(
            &routes,
            &authorization,
            "POST",
            format!("/{release}/metadata/preview"),
            Some(set(
                "labels",
                serde_json::json!([{"id": label, "catalog_number": null}]),
            )),
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "{body}");

        // References that name no artist or label are still rejected.
        for change in [
            set(
                "credits",
                serde_json::json!([{"artist_id": "missing", "type": "artist", "detail": null}]),
            ),
            set(
                "credits",
                serde_json::json!([{"artist_id": other_release, "type": "artist", "detail": null}]),
            ),
            set(
                "labels",
                serde_json::json!([{"id": "missing", "catalog_number": null}]),
            ),
        ] {
            let (status, body) = send(
                &routes,
                &authorization,
                "POST",
                format!("/{release}/metadata/preview"),
                Some(change),
            )
            .await?;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        }

        // The edited entity itself must still be accessible.
        let (status, _) = send(
            &routes,
            &authorization,
            "GET",
            format!("/{other_release}/metadata"),
            None,
        )
        .await?;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) = send(
            &routes,
            &authorization,
            "POST",
            format!("/{other_release}/metadata/preview"),
            Some(set("release_title", serde_json::json!("Renamed"))),
        )
        .await?;
        assert_eq!(status, StatusCode::NOT_FOUND);
        Ok(())
    }
}
