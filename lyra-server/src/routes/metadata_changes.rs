// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
use aide::transform::TransformOperation;
use axum::{
    Json,
    Router,
    http::{
        HeaderMap,
        StatusCode,
    },
    routing::post,
};
use serde::{
    Deserialize,
    Serialize,
};
use serde_json::{
    Value,
    json,
};

use crate::routes::AppError;
use crate::routes::request::bearer_credential;
use crate::services::{
    auth::require_manage_metadata,
    metadata::changes::{
        self,
        ChangeError,
        ChangeSetError,
        model::{
            ChangePreview,
            ChangeResult,
            MetadataChange,
        },
    },
};

impl From<ChangeSetError> for AppError {
    fn from(error: ChangeSetError) -> Self {
        match error {
            ChangeSetError::Invalid(message) => Self::bad_request(message),
            ChangeSetError::Forbidden(message) => Self::forbidden(message),
            ChangeSetError::Internal(error) => Self::from(error),
            ChangeSetError::Failed { index, error } => failed_change(index, error),
        }
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct ChangeFailureResponse {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "The 0-based position in the set of the operation that failed.")
    )]
    index: usize,
    message: String,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "`metadata_change_stale` when `expected` no longer holds.")
    )]
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<&'static str>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "With `code`, what a new preview would report for `expected`.")
    )]
    #[serde(skip_serializing_if = "Option::is_none")]
    current: Option<Value>,
}

fn failed_change(index: usize, error: ChangeError) -> AppError {
    let message = error.to_string();
    let (status, code, current) = match error {
        ChangeError::Invalid(_) => (StatusCode::BAD_REQUEST, None, None),
        ChangeError::NotFound(_) => (StatusCode::NOT_FOUND, None, None),
        ChangeError::Forbidden(_) => (StatusCode::FORBIDDEN, None, None),
        ChangeError::Conflict(_) => (StatusCode::CONFLICT, None, None),
        ChangeError::Stale { current, .. } => (
            StatusCode::CONFLICT,
            Some("metadata_change_stale"),
            Some(current),
        ),
        ChangeError::Internal(error) => return AppError::from(error),
    };
    AppError::json(
        status,
        json!(ChangeFailureResponse {
            index,
            message,
            code,
            current,
        }),
    )
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChangeSetRequest {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "The operations in the order they apply, at most 50.")
    )]
    changes: Vec<MetadataChange>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct ChangeSetPreviewResponse {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "One report per operation, in the set's order.")
    )]
    results: Vec<ChangePreview>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct ChangeSetResponse {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "One result per operation, in the set's order.")
    )]
    results: Vec<ChangeResult>,
}

async fn preview_metadata_changes(
    headers: HeaderMap,
    Json(request): Json<ChangeSetRequest>,
) -> Result<Json<ChangeSetPreviewResponse>, AppError> {
    let principal = require_manage_metadata(bearer_credential(&headers)).await?;
    let results = changes::preview(&principal, &request.changes).await?;
    Ok(Json(ChangeSetPreviewResponse { results }))
}

async fn apply_metadata_changes(
    headers: HeaderMap,
    Json(request): Json<ChangeSetRequest>,
) -> Result<Json<ChangeSetResponse>, AppError> {
    let principal = require_manage_metadata(bearer_credential(&headers)).await?;
    let results = changes::apply(&principal, &request.changes).await?;
    Ok(Json(ChangeSetResponse { results }))
}

#[cfg(feature = "docgen")]
fn preview_metadata_changes_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Preview a metadata change set").description(
        "Takes the body of `POST /api/metadata/changes`, ignoring every `expected`, changes nothing and reports each operation in `results`: `edit_metadata` its field `diff`, `set_external_id` and `inherit_external_id` the `current` record, `merge` its counts and warnings with the `manual_fills` it would take, and `rename_genre` the `current_name`. Each operation is reported against the data as it stands: a preview does not model the earlier operations of the set, so an operation that relies on one, or that an earlier one would invalidate, reads differently here than when it applies. It binds nothing, as applying checks everything again. An operation that would have no effect fails the set. The first failing operation answers for the set with its own status, as in `POST /api/metadata/changes`: 400, 403, 404 or 409, and a JSON body with its `index` and `message`. An empty set, more than 50 operations, or more than 50 losers across the set is a plain 400. Requires ManageMetadata.",
    )
    .response::<400, Json<ChangeFailureResponse>>()
    .response::<403, Json<ChangeFailureResponse>>()
    .response::<404, Json<ChangeFailureResponse>>()
    .response::<409, Json<ChangeFailureResponse>>()
}

#[cfg(feature = "docgen")]
fn apply_metadata_changes_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Apply a metadata change set").description(
        "Applies `changes` in order in one transaction, or none of them, and returns one entry per operation in `results`, as things stood right after that operation. Merges can't be undone. Each operation resolves what it names and is checked against what the operations before it staged, so one naming an entity an earlier one merged away reads 404, and a rename to a name an earlier one took reads 409.\n\nEach operation but `remove_external_id_alias` requires `expected`, what preview reported for it: for `edit_metadata` the `diff`, for `set_external_id` and `inherit_external_id` the `current` record (`null` for none), for `merge` the `manual_fills`, and for `rename_genre` the `current_name`. It is compared with the staged state just before the operation runs, ahead of any check of what the operation itself would do, so an operation that an earlier one changes the state of needs that state as its `expected`, not the preview's. A mismatch is a 409 with `code` `metadata_change_stale` and `current`, what a new preview would report in place of `expected`.\n\nThe first failing operation rolls the set back and answers with its own status and a JSON body with its `index` and `message`: 400 for a missing `expected`, an invalid value, an operation with no effect, a merge of an entity into itself or with one of another kind; 403 when the caller reaches only some of the libraries a `merge`, `rename_genre` or `remove_external_id_alias` affects; 404 when the caller reaches an entity through no library, as for an entity that does not exist, or for a missing manual ID or alias; 409 for a stale `expected`, a name another genre goes by, releases in different libraries or holding the same track by a unique ID, or a sync, refresh or purge running on an affected library. Only a set holding a `merge` or `rename_genre` looks at runs, and then for every operation in it; other sets apply during a run. An empty set, more than 50 operations, or more than 50 losers across the set is a plain 400. Requires ManageMetadata, and access to one library of what `edit_metadata`, `set_external_id` and `inherit_external_id` change, or to every library a `merge`, `rename_genre` or `remove_external_id_alias` affects.",
    )
    .response::<400, Json<ChangeFailureResponse>>()
    .response::<403, Json<ChangeFailureResponse>>()
    .response::<404, Json<ChangeFailureResponse>>()
    .response::<409, Json<ChangeFailureResponse>>()
}

pub(crate) fn metadata_change_routes() -> Router {
    Router::new()
        .route("/changes", post(apply_metadata_changes))
        .route("/changes/preview", post(preview_metadata_changes))
}

#[cfg(feature = "docgen")]
pub(crate) fn metadata_change_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::post_with;

    aide::axum::ApiRouter::new()
        .api_route(
            "/changes",
            post_with(apply_metadata_changes, apply_metadata_changes_docs),
        )
        .api_route(
            "/changes/preview",
            post_with(preview_metadata_changes, preview_metadata_changes_docs),
        )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use agdb::{
        DbAny,
        DbId,
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
        insert_artist,
        insert_release,
        insert_track,
    };
    use lyra_db::nanoid;
    use lyra_db::test_db::insert_library;
    use tower::ServiceExt;

    use super::*;
    use crate::{
        plugins::testing::init_library_test_state,
        routes::entity_routes,
        services::{
            STATE,
            metadata::{
                layers::json::resolved_fields_from_json,
                merging::apply_merged_metadata_to_entity,
            },
            testing::{
                create_session,
                runtime_test_lock,
            },
        },
    };

    fn routes() -> Router {
        Router::new()
            .nest(
                "/entities",
                entity_routes().merge(crate::routes::entity_metadata_routes()),
            )
            .nest("/metadata", metadata_change_routes())
    }

    async fn send(
        authorization: &str,
        method: &str,
        uri: String,
        body: Option<Value>,
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
        let response = routes().oneshot(request).await?;
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        Ok((status, String::from_utf8(body.to_vec())?))
    }

    async fn post_changes(
        authorization: &str,
        path: &str,
        changes: Vec<Value>,
    ) -> anyhow::Result<(StatusCode, String)> {
        send(
            authorization,
            "POST",
            path.to_string(),
            Some(json!({ "changes": changes })),
        )
        .await
    }

    async fn preview_one(
        authorization: &str,
        change: Value,
    ) -> anyhow::Result<(StatusCode, String)> {
        post_changes(authorization, "/metadata/changes/preview", vec![change]).await
    }

    /// What a preview reported, as `expected` of the operation it reported on.
    fn expected_of(report: &Value) -> Option<&Value> {
        match report["action"].as_str() {
            Some("edit_metadata") => report.get("diff"),
            Some("set_external_id" | "inherit_external_id") => report.get("current"),
            Some("merge") => report.get("manual_fills"),
            Some("rename_genre") => report.get("current_name"),
            _ => None,
        }
    }

    /// Previews one operation and, if that passes, applies it with what the preview reported.
    async fn apply_one(
        authorization: &str,
        mut change: Value,
    ) -> anyhow::Result<(StatusCode, String)> {
        let (status, body) = preview_one(authorization, change.clone()).await?;
        if status != StatusCode::OK {
            return Ok((status, body));
        }
        let preview: Value = serde_json::from_str(&body)?;
        if let Some(expected) = expected_of(&preview["results"][0]) {
            change["expected"] = expected.clone();
        }
        post_changes(authorization, "/metadata/changes", vec![change]).await
    }

    fn merge(winner: &str, losers: &[&str]) -> Value {
        json!({"action": "merge", "winner_id": winner, "loser_ids": losers})
    }

    fn rename(genre: &str, name: &str) -> Value {
        json!({"action": "rename_genre", "genre_id": genre, "name": name})
    }

    fn set_id(entity: &str) -> Value {
        json!({
            "action": "set_external_id",
            "entity_id": entity,
            "provider_id": "provider",
            "id_type": "release_id",
            "id_value": "x",
        })
    }

    fn inherit_id(entity: &str) -> Value {
        json!({
            "action": "inherit_external_id",
            "entity_id": entity,
            "provider_id": "provider",
            "id_type": "release_id",
        })
    }

    fn remove_alias(entity: &str, id_type: &str, value: &str) -> Value {
        json!({
            "action": "remove_external_id_alias",
            "entity_id": entity,
            "provider_id": "provider",
            "id_type": id_type,
            "id_value": value,
        })
    }

    async fn merge_status(
        authorization: &str,
        winner: &str,
        losers: &[&str],
    ) -> anyhow::Result<(StatusCode, String)> {
        apply_one(authorization, merge(winner, losers)).await
    }

    fn public_id(db: &DbAny, id: DbId) -> String {
        lyra_db::lookup::find_id_by_db_id(db, id)
            .expect("lookup")
            .expect("public id")
    }

    fn user_with(db: &mut DbAny, permissions: Vec<lyra_db::Permission>) -> anyhow::Result<DbId> {
        lyra_db::roles::ensure_builtin_roles(db)?;
        let user = lyra_db::users::create(
            db,
            &lyra_db::fixtures::test_user(&format!("merge-{}", nanoid()))?,
        )?;
        let role = format!("merge-role-{}", nanoid());
        lyra_db::roles::create(
            db,
            &lyra_db::roles::Role {
                db_id: None,
                id: nanoid(),
                name: role.clone(),
                permissions,
            },
        )?;
        lyra_db::roles::ensure_user_has_role(db, user, &role)?;
        Ok(user)
    }

    fn admin_user(db: &mut DbAny) -> anyhow::Result<DbId> {
        let user = user_with(db, Vec::new())?;
        lyra_db::roles::ensure_user_has_role(db, user, lyra_db::roles::BUILTIN_ADMIN_ROLE)?;
        Ok(user)
    }

    async fn bearer(user: DbId) -> anyhow::Result<String> {
        let session = create_session(user, Default::default()).await?;
        Ok(format!("Bearer {}", session.token))
    }

    fn release_in(db: &mut DbAny, library: DbId, fields: Value) -> anyhow::Result<DbId> {
        let release = insert_release(db, "Release")?;
        lyra_db::graph::ensure_owned_edge(db, library, release)?;
        layer(db, release, "local", fields)?;
        Ok(release)
    }

    fn layer(db: &mut DbAny, entity: DbId, source_id: &str, fields: Value) -> anyhow::Result<()> {
        let fields = resolved_fields_from_json(db, fields);
        lyra_db::metadata::layers::upsert(
            db,
            entity,
            &lyra_db::MetadataLayer::new(source_id, fields, 1),
        )?;
        apply_merged_metadata_to_entity(db, entity)
    }

    fn genre(db: &DbAny, name: &str) -> String {
        let id = lyra_db::genres::find_by_name(db, name)
            .expect("lookup")
            .expect("genre");
        public_id(db, id)
    }

    fn label(db: &DbAny, name: &str) -> String {
        let id = lyra_db::labels::find_by_name(db, name)
            .expect("lookup")
            .expect("label");
        public_id(db, id)
    }

    /// A manager of one library beside another, holding genres and labels of each reach.
    struct Scene {
        manager: String,
        admin: String,
        hidden_library: String,
        visible: String,
        both: String,
        hidden: String,
        layer_only: String,
        no_release: String,
        visible_label: String,
        both_label: String,
        hidden_label: String,
    }

    async fn scene() -> anyhow::Result<Scene> {
        let (manager, admin, ids) = {
            let mut db = STATE.db.write().await;
            let manager = user_with(&mut db, vec![lyra_db::Permission::ManageMetadata])?;
            let admin = admin_user(&mut db)?;
            let visible_library = insert_library(&mut db, "Visible", "/merge-visible")?;
            let hidden_library = insert_library(&mut db, "Hidden Secret", "/merge-hidden")?;
            lyra_db::libraries::grant_access(
                &mut db,
                manager,
                visible_library,
                lyra_db::libraries::AccessKind::ReadWrite,
            )?;
            lyra_db::providers::upsert(
                &mut db,
                &lyra_db::ProviderConfig {
                    db_id: None,
                    provider_id: "provider".to_string(),
                    position: 0,
                    enabled: true,
                },
            )?;
            release_in(
                &mut db,
                visible_library,
                json!({
                    "genres": ["Visible", "Both", "Layer Only"],
                    "labels": [{"name": "Visible Label"}, {"name": "Both Label"}],
                }),
            )?;
            release_in(
                &mut db,
                hidden_library,
                json!({
                    "genres": ["Both", "Hidden"],
                    "labels": [{"name": "Both Label"}, {"name": "Hidden Label"}],
                }),
            )?;
            // Only a layer that loses to the provider's names this genre there.
            let shadowed = release_in(&mut db, hidden_library, json!({"genres": ["Layer Only"]}))?;
            layer(&mut db, shadowed, "provider", json!({"genres": ["Hidden"]}))?;
            let no_release = lyra_db::genres::resolve_by_name(&mut *db, "No Release")?;
            let ids = (
                public_id(&db, hidden_library),
                genre(&db, "Visible"),
                genre(&db, "Both"),
                genre(&db, "Hidden"),
                genre(&db, "Layer Only"),
                public_id(&db, no_release),
                label(&db, "Visible Label"),
                label(&db, "Both Label"),
                label(&db, "Hidden Label"),
            );
            (manager, admin, ids)
        };
        Ok(Scene {
            manager: bearer(manager).await?,
            admin: bearer(admin).await?,
            hidden_library: ids.0,
            visible: ids.1,
            both: ids.2,
            hidden: ids.3,
            layer_only: ids.4,
            no_release: ids.5,
            visible_label: ids.6,
            both_label: ids.7,
            hidden_label: ids.8,
        })
    }

    #[tokio::test]
    async fn a_merge_needs_every_library_it_reaches() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_library_test_state(std::path::PathBuf::from(".")).await?;
        let scene = scene().await?;

        for (winner, loser) in [
            (&scene.visible, &scene.both),
            (&scene.both, &scene.visible),
            (&scene.visible, &scene.layer_only),
            (&scene.visible_label, &scene.both_label),
        ] {
            let (status, body) = merge_status(&scene.manager, winner, &[loser]).await?;
            assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
            assert!(!body.contains("Hidden") && !body.contains(&scene.hidden_library));
        }
        for (winner, loser) in [
            (&scene.visible, &scene.hidden),
            (&scene.hidden, &scene.visible),
            (&scene.visible, &scene.no_release),
            (&scene.visible_label, &scene.hidden_label),
        ] {
            let (status, body) = merge_status(&scene.manager, winner, &[loser]).await?;
            assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        }

        let (status, body) =
            merge_status(&scene.admin, &scene.no_release, &[&scene.hidden]).await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        let outcome: Value = serde_json::from_str(&body)?;
        let outcome = &outcome["results"][0];
        assert_eq!(outcome["action"], "merge");
        assert_eq!(outcome["kind"], "genre");
        assert_eq!(outcome["winner_id"], json!(scene.no_release));
        assert_eq!(outcome["counts"]["losers"], 1);
        Ok(())
    }

    #[tokio::test]
    async fn a_merge_counts_an_artists_layers_through_the_owning_artists_libraries()
    -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_library_test_state(std::path::PathBuf::from(".")).await?;
        let (manager, artist, duplicate) = {
            let mut db = STATE.db.write().await;
            let manager = user_with(&mut db, vec![lyra_db::Permission::ManageMetadata])?;
            let visible = insert_library(&mut db, "Visible", "/merge-artist-visible")?;
            let hidden = insert_library(&mut db, "Hidden", "/merge-artist-hidden")?;
            lyra_db::libraries::grant_access(
                &mut db,
                manager,
                visible,
                lyra_db::libraries::AccessKind::ReadWrite,
            )?;
            let credit = |artist_id| {
                BTreeMap::from([(
                    lyra_db::CreditType::Artist,
                    vec![lyra_db::metadata::layers::edges::LayerCreditInput {
                        artist_id,
                        detail: None,
                    }],
                )])
            };
            let artist = insert_artist(&mut db, "Artist")?;
            let duplicate = insert_artist(&mut db, "Duplicate")?;
            let band = insert_artist(&mut db, "Band")?;
            for (library, credited) in [(visible, artist), (visible, duplicate), (hidden, band)] {
                let release = insert_release(&mut db, "Release")?;
                lyra_db::graph::ensure_owned_edge(&mut *db, library, release)?;
                lyra_db::metadata::layers::upsert(
                    &mut db,
                    release,
                    &lyra_db::MetadataLayer::new("local", Default::default(), 1)
                        .with_credits(credit(credited)),
                )?;
                apply_merged_metadata_to_entity(&mut db, release)?;
            }
            // Only the band's own layer relates this artist to the hidden library.
            lyra_db::metadata::layers::upsert(
                &mut db,
                band,
                &lyra_db::MetadataLayer::new("local", Default::default(), 1).with_relations(
                    BTreeMap::from([(
                        lyra_db::ArtistRelationType::MemberOf,
                        vec![lyra_db::metadata::layers::edges::LayerRelationInput {
                            artist_id: duplicate,
                            attributes: None,
                        }],
                    )]),
                ),
            )?;
            (manager, public_id(&db, artist), public_id(&db, duplicate))
        };
        let manager = bearer(manager).await?;

        let (status, body) = merge_status(&manager, &artist, &[&duplicate]).await?;

        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        Ok(())
    }

    #[tokio::test]
    async fn merging_takes_manage_metadata() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_library_test_state(std::path::PathBuf::from(".")).await?;
        let scene = scene().await?;
        let listener = {
            let mut db = STATE.db.write().await;
            user_with(&mut db, Vec::new())?
        };
        let listener = bearer(listener).await?;

        for path in ["/metadata/changes/preview", "/metadata/changes"] {
            for change in [
                merge(&scene.visible, &[&scene.both]),
                rename(&scene.visible, "New"),
            ] {
                let (status, _) = post_changes(&listener, path, vec![change]).await?;
                assert_eq!(status, StatusCode::FORBIDDEN);
            }
        }
        Ok(())
    }

    fn insert_running_run(db: &mut DbAny, library_id: &str) -> anyhow::Result<()> {
        lyra_db::sync_runs::create(
            db,
            &lyra_db::sync_runs::SyncRunRecord {
                db_id: None,
                id: nanoid(),
                library_id: library_id.to_string(),
                kind: "library_sync".to_string(),
                status: "running".to_string(),
                started_at_ms: 1,
                updated_at_ms: 1,
                finished_at_ms: None,
                error: None,
                cancellation_requested: false,
                sequence: 1,
                progress_mode: "indeterminate".to_string(),
                total_state: "discovering".to_string(),
                completed_units: 0,
                failed_units: 0,
                skipped_units: 0,
                total_units: 0,
                current_stage: None,
                current_subject: None,
                active_units: 0,
                failure_count: 0,
            },
        )?;
        Ok(())
    }

    #[tokio::test]
    async fn an_active_run_on_any_affected_library_is_a_conflict() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_library_test_state(std::path::PathBuf::from(".")).await?;
        let scene = scene().await?;
        insert_running_run(&mut *STATE.db.write().await, &scene.hidden_library)?;

        let (status, body) = merge_status(&scene.admin, &scene.visible, &[&scene.both]).await?;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        let (status, body) =
            preview_one(&scene.admin, merge(&scene.visible, &[&scene.both])).await?;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        let (status, body) = post_changes(
            &scene.admin,
            "/metadata/changes",
            vec![{
                let mut change = rename(&scene.both, "New");
                change["expected"] = json!("Both");
                change
            }],
        )
        .await?;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");

        let (status, body) =
            merge_status(&scene.admin, &scene.visible, &[&scene.no_release]).await?;
        assert_eq!(
            status,
            StatusCode::OK,
            "another library's run stops nothing: {body}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn releases_merge_only_inside_identical_libraries_without_shared_tracks()
    -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_library_test_state(std::path::PathBuf::from(".")).await?;
        let (admin, winner, elsewhere, same_track, fine) = {
            let mut db = STATE.db.write().await;
            let admin = admin_user(&mut db)?;
            let library = insert_library(&mut db, "A", "/merge-release-a")?;
            let other = insert_library(&mut db, "B", "/merge-release-b")?;
            let winner = release_in(&mut db, library, json!({}))?;
            let elsewhere = release_in(&mut db, other, json!({}))?;
            let same_track = release_in(&mut db, library, json!({}))?;
            let fine = release_in(&mut db, library, json!({}))?;
            for release in [winner, same_track] {
                let track = insert_track(&mut db, "Track")?;
                lyra_db::releases::link_track(&mut *db, release, track)?;
                lyra_db::external_ids::upsert(
                    &mut db,
                    track,
                    "provider",
                    "recording_id",
                    "shared",
                    lyra_db::IdSource::Resolved,
                )?;
            }
            (
                admin,
                public_id(&db, winner),
                public_id(&db, elsewhere),
                public_id(&db, same_track),
                public_id(&db, fine),
            )
        };
        {
            use crate::services::{
                EntityType,
                plugin_scope::PluginId,
                providers::{
                    ProviderIdSpec,
                    provider_registry,
                },
            };
            let mut registry = provider_registry().write_owned().await;
            registry.register(
                PluginId::new("demo").expect("plugin id"),
                "provider".to_string(),
                None,
            )?;
            registry
                .set_id_registration(
                    "provider",
                    ProviderIdSpec {
                        id_type: "recording_id".to_string(),
                        entity: EntityType::Track,
                        unique: true,
                        scheme: "example:recording".to_string(),
                        label: None,
                    },
                    || None,
                )
                .map_err(anyhow::Error::msg)?;
        }
        let admin = bearer(admin).await?;

        for loser in [&elsewhere, &same_track] {
            let (status, body) = merge_status(&admin, &winner, &[loser]).await?;
            assert_eq!(status, StatusCode::CONFLICT, "{body}");
        }
        let (status, body) = merge_status(&admin, &winner, &[&fine]).await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        Ok(())
    }

    #[tokio::test]
    async fn a_preview_reports_and_changes_nothing() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_library_test_state(std::path::PathBuf::from(".")).await?;
        let scene = scene().await?;

        let (status, body) =
            preview_one(&scene.admin, merge(&scene.visible, &[&scene.hidden])).await?;

        assert_eq!(status, StatusCode::OK, "{body}");
        let preview: Value = serde_json::from_str(&body)?;
        let preview = &preview["results"][0];
        assert_eq!(preview["kind"], "genre");
        assert_eq!(preview["counts"]["releases"], 2);
        assert_eq!(preview["kept_external_ids"], json!([]));
        assert_eq!(preview["track_collisions"], json!([]));
        assert_eq!(preview["locked_ids"], json!([]));
        let (status, _) = merge_status(&scene.admin, &scene.visible, &[&scene.hidden]).await?;
        assert_eq!(status, StatusCode::OK, "the loser survived the preview");
        Ok(())
    }

    #[tokio::test]
    async fn a_merged_id_shows_as_an_alias_until_it_is_deleted() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_library_test_state(std::path::PathBuf::from(".")).await?;
        let (admin, winner, loser, winner_db_id) = {
            let mut db = STATE.db.write().await;
            let admin = admin_user(&mut db)?;
            let winner = insert_artist(&mut db, "Winner")?;
            let loser = insert_artist(&mut db, "Loser")?;
            for (artist, value) in [(winner, "a"), (loser, "b/c")] {
                lyra_db::external_ids::upsert(
                    &mut db,
                    artist,
                    "provider",
                    "artist_id",
                    value,
                    lyra_db::IdSource::Resolved,
                )?;
            }
            (admin, public_id(&db, winner), public_id(&db, loser), winner)
        };
        let admin = bearer(admin).await?;
        let (status, body) = merge_status(&admin, &winner, &[&loser]).await?;
        assert_eq!(status, StatusCode::OK, "{body}");

        let (status, body) = send(
            &admin,
            "GET",
            format!("/entities/{winner}/external-ids"),
            None,
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        let ids: Value = serde_json::from_str(&body)?;
        assert_eq!(
            ids,
            json!([
                {"provider_id": "provider", "id_type": "artist_id", "id_value": "a", "source": "resolved"},
                {"provider_id": "provider", "id_type": "artist_id", "id_value": "b/c", "source": "alias"},
            ])
        );

        let removal = remove_alias(&winner, "artist_id", "b/c");
        let (status, body) = apply_one(&admin, removal.clone()).await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, _) = apply_one(&admin, removal).await?;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let db = STATE.db.read().await;
        assert_eq!(
            lyra_db::external_ids::get_owner(&*db, "provider", "artist_id", "b/c", Some("Artist"))?,
            None
        );
        assert_eq!(
            lyra_db::external_ids::get_owner(&*db, "provider", "artist_id", "a", Some("Artist"))?,
            Some(winner_db_id)
        );
        Ok(())
    }

    #[tokio::test]
    async fn deleting_an_alias_takes_the_merge_access_rule() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_library_test_state(std::path::PathBuf::from(".")).await?;
        let scene = scene().await?;
        let (status, _) =
            apply_one(&scene.manager, remove_alias(&scene.hidden, "genre_id", "x")).await?;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) =
            apply_one(&scene.manager, remove_alias(&scene.both, "genre_id", "x")).await?;
        assert_eq!(status, StatusCode::FORBIDDEN);
        Ok(())
    }

    #[tokio::test]
    async fn external_ids_and_locks_need_one_reachable_library() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_library_test_state(std::path::PathBuf::from(".")).await?;
        let scene = scene().await?;
        let (shared, hidden) = {
            let mut db = STATE.db.write().await;
            let release_of = |db: &DbAny, genre: &str| -> anyhow::Result<DbId> {
                let genre = lyra_db::lookup::find_node_id_by_id(db, genre)?
                    .ok_or_else(|| anyhow::anyhow!("genre missing"))?;
                lyra_db::genres::get_releases(db, genre)?
                    .first()
                    .copied()
                    .ok_or_else(|| anyhow::anyhow!("release missing"))
            };
            let shared = release_of(&db, &scene.visible)?;
            let hidden = release_of(&db, &scene.hidden)?;
            let hidden_library = lyra_db::lookup::find_node_id_by_id(&*db, &scene.hidden_library)?
                .ok_or_else(|| anyhow::anyhow!("library missing"))?;
            lyra_db::graph::ensure_owned_edge(&mut *db, hidden_library, shared)?;
            (public_id(&db, shared), public_id(&db, hidden))
        };
        let ids = |id: &str| format!("/entities/{id}/external-ids");
        let lock = |id: &str| format!("/entities/{id}/lock");

        for (method, uri) in [
            ("GET", ids(&hidden)),
            ("GET", ids(&scene.hidden)),
            ("PUT", lock(&hidden)),
            ("DELETE", lock(&hidden)),
        ] {
            let (status, _) = send(&scene.manager, method, uri.clone(), None).await?;
            assert_eq!(status, StatusCode::NOT_FOUND, "{method} {uri}");
        }
        for change in [set_id(&hidden), inherit_id(&hidden)] {
            let (status, _) = apply_one(&scene.manager, change.clone()).await?;
            assert_eq!(status, StatusCode::NOT_FOUND, "{change}");
        }

        let (status, body) = apply_one(&scene.manager, set_id(&shared)).await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        for (method, uri) in [("GET", ids(&shared)), ("GET", ids(&scene.both))] {
            let (status, body) = send(&scene.manager, method, uri.clone(), None).await?;
            assert_eq!(status, StatusCode::OK, "{method} {uri}: {body}");
        }
        let (status, body) = apply_one(&scene.manager, inherit_id(&shared)).await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        for (method, uri) in [("PUT", lock(&shared)), ("DELETE", lock(&shared))] {
            let (status, body) = send(&scene.manager, method, uri.clone(), None).await?;
            assert_eq!(status, StatusCode::OK, "{method} {uri}: {body}");
        }

        let (status, body) = apply_one(&scene.admin, set_id(&hidden)).await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        Ok(())
    }

    #[tokio::test]
    async fn renaming_a_genre_answers_by_the_rules() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_library_test_state(std::path::PathBuf::from(".")).await?;
        let scene = scene().await?;
        for (who, id, name, expected) in [
            (
                &scene.manager,
                &scene.visible,
                "   ",
                StatusCode::BAD_REQUEST,
            ),
            (&scene.manager, &scene.hidden, "New", StatusCode::NOT_FOUND),
            (&scene.manager, &scene.both, "New", StatusCode::FORBIDDEN),
            (
                &scene.manager,
                &scene.layer_only,
                "New",
                StatusCode::FORBIDDEN,
            ),
            (
                &scene.manager,
                &scene.visible,
                "HIDDEN",
                StatusCode::CONFLICT,
            ),
            (
                &scene.admin,
                &scene.visible_label,
                "New",
                StatusCode::NOT_FOUND,
            ),
        ] {
            let (status, body) = apply_one(who, rename(id, name)).await?;
            assert_eq!(status, expected, "{id} {name}: {body}");
        }

        let (status, body) = apply_one(&scene.manager, rename(&scene.visible, " Seen ")).await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        let results: Value = serde_json::from_str(&body)?;
        assert_eq!(
            results,
            json!({"results": [
                {"action": "rename_genre", "genre_id": scene.visible, "name": "Seen"},
            ]})
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_failing_operation_answers_with_its_index_status_and_message() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_library_test_state(std::path::PathBuf::from(".")).await?;
        let scene = scene().await?;
        let named = |name: &str, change: Value| {
            let mut change = change;
            change["expected"] = json!(name);
            change
        };
        let fine = named("Visible", rename(&scene.visible, "Seen"));

        for (failing, status, message) in [
            (
                named("No Release", rename(&scene.no_release, "  ")),
                StatusCode::BAD_REQUEST,
                "a genre's name can't be blank",
            ),
            (
                named("Both", rename(&scene.both, "New")),
                StatusCode::FORBIDDEN,
                "this change reaches a library you can't access",
            ),
            (
                named("Hidden", rename(&scene.hidden, "New")),
                StatusCode::NOT_FOUND,
                "Entity not found",
            ),
            (
                named("Visible Label", rename(&scene.visible_label, "New")),
                StatusCode::NOT_FOUND,
                "Entity not found",
            ),
        ] {
            let (got, body) = post_changes(
                &scene.manager,
                "/metadata/changes",
                vec![fine.clone(), failing.clone()],
            )
            .await?;
            assert_eq!(got, status, "{failing}: {body}");
            let failure: Value = serde_json::from_str(&body)?;
            assert_eq!(failure["index"], 1, "{body}");
            assert!(
                failure["message"]
                    .as_str()
                    .is_some_and(|m| m.contains(message)),
                "{body}"
            );
        }
        let genre = lyra_db::genres::find_by_name(&*STATE.db.read().await, "Visible")?;
        assert!(genre.is_some(), "the first rename rolled back");

        let (status, body) = post_changes(
            &scene.manager,
            "/metadata/changes",
            vec![named("Other", rename(&scene.visible, "Seen"))],
        )
        .await?;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        let failure: Value = serde_json::from_str(&body)?;
        assert_eq!(failure["index"], 0);
        assert_eq!(failure["code"], "metadata_change_stale");
        assert_eq!(failure["current"], "Visible");
        Ok(())
    }

    #[tokio::test]
    async fn a_set_applies_on_a_partly_reachable_entity_only_what_one_library_allows()
    -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_library_test_state(std::path::PathBuf::from(".")).await?;
        let scene = scene().await?;
        let (shared, duplicate) = {
            let mut db = STATE.db.write().await;
            let genre = lyra_db::lookup::find_node_id_by_id(&*db, &scene.visible)?
                .ok_or_else(|| anyhow::anyhow!("genre missing"))?;
            let shared = lyra_db::genres::get_releases(&*db, genre)?[0];
            let hidden_library = lyra_db::lookup::find_node_id_by_id(&*db, &scene.hidden_library)?
                .ok_or_else(|| anyhow::anyhow!("library missing"))?;
            lyra_db::graph::ensure_owned_edge(&mut *db, hidden_library, shared)?;
            let duplicate = insert_release(&mut db, "Duplicate")?;
            lyra_db::graph::ensure_owned_edge(&mut *db, hidden_library, duplicate)?;
            (public_id(&db, shared), public_id(&db, duplicate))
        };
        let title = |id: &str| {
            json!({
                "action": "edit_metadata",
                "entity_id": id,
                "edits": [{"field": "release_title", "operation": "set", "value": "Edited"}],
            })
        };

        let (status, body) = apply_one(&scene.manager, title(&shared)).await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, body) = apply_one(&scene.manager, set_id(&shared)).await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, _) = apply_one(&scene.manager, title(&duplicate)).await?;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) =
            apply_one(&scene.manager, remove_alias(&shared, "release_id", "kept")).await?;
        assert_eq!(status, StatusCode::FORBIDDEN);
        let (status, _) = merge_status(&scene.manager, &shared, &[&duplicate]).await?;
        assert_eq!(status, StatusCode::NOT_FOUND);
        Ok(())
    }

    #[tokio::test]
    async fn metadata_reach_follows_layer_references_and_skips_genres() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_library_test_state(std::path::PathBuf::from(".")).await?;
        let scene = scene().await?;
        let (referenced, unreferenced) = {
            let mut db = STATE.db.write().await;
            let genre = lyra_db::lookup::find_node_id_by_id(&*db, &scene.visible)?
                .ok_or_else(|| anyhow::anyhow!("genre missing"))?;
            let release = lyra_db::genres::get_releases(&*db, genre)?[0];
            let referenced = insert_artist(&mut db, "Referenced")?;
            let unreferenced = insert_artist(&mut db, "Unreferenced")?;
            // Left unapplied, so the layer is all that ties the artist to the release.
            lyra_db::metadata::layers::upsert(
                &mut db,
                release,
                &lyra_db::MetadataLayer::new("provider", Default::default(), 1).with_credits(
                    BTreeMap::from([(
                        lyra_db::CreditType::Artist,
                        vec![lyra_db::metadata::layers::edges::LayerCreditInput {
                            artist_id: referenced,
                            detail: None,
                        }],
                    )]),
                ),
            )?;
            (public_id(&db, referenced), public_id(&db, unreferenced))
        };
        let metadata = |id: &str| format!("/entities/{id}/metadata");
        let edit = |id: &str| {
            json!({
                "action": "edit_metadata",
                "entity_id": id,
                "edits": [{"field": "artist_name", "operation": "set", "value": "Edited"}],
            })
        };

        let (status, body) = send(&scene.manager, "GET", metadata(&referenced), None).await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, body) = apply_one(&scene.manager, edit(&referenced)).await?;
        assert_eq!(status, StatusCode::OK, "{body}");

        let (status, _) = send(&scene.manager, "GET", metadata(&unreferenced), None).await?;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) = apply_one(&scene.manager, edit(&unreferenced)).await?;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, body) = send(&scene.manager, "GET", metadata(&scene.visible), None).await?;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(body.contains(&scene.visible), "{body}");
        Ok(())
    }

    #[tokio::test]
    async fn the_set_itself_is_checked_first() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_library_test_state(std::path::PathBuf::from(".")).await?;
        let scene = scene().await?;

        for path in ["/metadata/changes/preview", "/metadata/changes"] {
            let (status, _) = post_changes(&scene.admin, path, Vec::new()).await?;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            let too_many = vec![rename(&scene.visible, "New"); 51];
            let (status, _) = post_changes(&scene.admin, path, too_many).await?;
            assert_eq!(status, StatusCode::BAD_REQUEST);
        }
        let (status, _) = send(
            &scene.admin,
            "POST",
            "/metadata/changes".to_string(),
            Some(json!({"changes": [rename(&scene.visible, "New")], "extra": true})),
        )
        .await?;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

        let (status, body) = post_changes(
            &scene.admin,
            "/metadata/changes",
            vec![rename(&scene.visible, "New")],
        )
        .await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "applying needs expected");
        assert_eq!(serde_json::from_str::<Value>(&body)?["index"], 0);
        Ok(())
    }
}
