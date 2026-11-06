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
    http::{
        HeaderMap,
        StatusCode,
    },
    routing::{
        delete,
        post,
    },
};
use serde::{
    Deserialize,
    Serialize,
};

use crate::routes::AppError;
use crate::routes::request::bearer_credential;
use crate::services::{
    auth::require_manage_metadata,
    metadata::entity_merge::{
        self,
        EntityMergeError,
        MergeCounts,
        MergeOutcome,
        MergePreview,
    },
};

impl From<EntityMergeError> for AppError {
    fn from(error: EntityMergeError) -> Self {
        match error {
            EntityMergeError::Invalid(message) => Self::bad_request(message),
            EntityMergeError::NotFound(id) => Self::not_found(format!("Entity not found: {id}")),
            EntityMergeError::Forbidden(message) => Self::forbidden(message),
            EntityMergeError::Conflict(message) => Self::conflict(message),
            EntityMergeError::Internal(error) => Self::from(error),
        }
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct MergeRequest {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "The entities to merge into this one, at most 50, of its kind.")
    )]
    loser_ids: Vec<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct MergeCountsResponse {
    losers: usize,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Releases credited to or linked with the losers.")
    )]
    releases: usize,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Tracks credited to the losers, or on the losing releases.")
    )]
    tracks: usize,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Parent and child genres of the losers.")
    )]
    relatives: usize,
}

impl From<MergeCounts> for MergeCountsResponse {
    fn from(counts: MergeCounts) -> Self {
        Self {
            losers: counts.losers,
            releases: counts.releases,
            tracks: counts.tracks,
            relatives: counts.relatives,
        }
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct MergeResponse {
    winner_id: String,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "artist, release, genre or label.")
    )]
    kind: &'static str,
    counts: MergeCountsResponse,
}

impl From<MergeOutcome> for MergeResponse {
    fn from(outcome: MergeOutcome) -> Self {
        Self {
            winner_id: outcome.winner_id,
            kind: outcome.kind.as_str(),
            counts: outcome.counts.into(),
        }
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct KeptExternalIdResponse {
    provider_id: String,
    id_type: String,
    id_value: String,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct TrackCollisionResponse {
    disc: u32,
    track: u32,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct MergePreviewResponse {
    #[serde(flatten)]
    outcome: MergeResponse,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Differing external IDs the winner would keep as aliases.")
    )]
    kept_external_ids: Vec<KeptExternalIdResponse>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Disc and track numbers more than one of the releases hold. They don't block the merge, as split discs share them."
        )
    )]
    track_collisions: Vec<TrackCollisionResponse>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "The locked entities among the winner and losers. A merge overrides a loser's lock."
        )
    )]
    locked_ids: Vec<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "The manual keys the winner has empty and would take from a loser. They take effect on a locked winner too."
        )
    )]
    manual_fills: Vec<String>,
}

impl From<MergePreview> for MergePreviewResponse {
    fn from(preview: MergePreview) -> Self {
        Self {
            outcome: preview.outcome.into(),
            kept_external_ids: preview
                .kept_external_ids
                .into_iter()
                .map(|alias| KeptExternalIdResponse {
                    provider_id: alias.provider_id,
                    id_type: alias.id_type,
                    id_value: alias.id_value,
                })
                .collect(),
            track_collisions: preview
                .track_collisions
                .into_iter()
                .map(|(disc, track)| TrackCollisionResponse { disc, track })
                .collect(),
            locked_ids: preview.locked_ids,
            manual_fills: preview.manual_fills,
        }
    }
}

async fn merge_entities(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<MergeRequest>,
) -> Result<Json<MergeResponse>, AppError> {
    let principal = require_manage_metadata(bearer_credential(&headers)).await?;
    let outcome = entity_merge::merge(&principal, &id, &request.loser_ids).await?;
    Ok(Json(outcome.into()))
}

async fn preview_entity_merge(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<MergeRequest>,
) -> Result<Json<MergePreviewResponse>, AppError> {
    let principal = require_manage_metadata(bearer_credential(&headers)).await?;
    let preview = entity_merge::preview(&principal, &id, &request.loser_ids).await?;
    Ok(Json(preview.into()))
}

async fn remove_external_id_alias(
    headers: HeaderMap,
    Path((id, provider_id, id_type, value)): Path<(String, String, String, String)>,
) -> Result<StatusCode, AppError> {
    let principal = require_manage_metadata(bearer_credential(&headers)).await?;
    entity_merge::remove_external_id_alias(&principal, &id, &provider_id, &id_type, &value).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(feature = "docgen")]
fn merge_entities_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Merge duplicate entities").description(
        "Merges `loser_ids` into this artist, release, genre or label in one transaction, which can't be undone. What is attached to the losers moves here, and they are deleted: their public IDs stop resolving, while their names and differing external IDs keep finding this entity, so later scans and provider refreshes don't bring them back. Responds 400 for an empty list, more than 50 losers, this entity among them, or a loser of another kind; 404 when the caller reaches an entity through no library; 403 when through only some of the libraries the merge reaches; and 409 while a sync, refresh or purge runs on one of them, for releases in different libraries, or for releases holding the same track by a unique ID. Requires ManageMetadata and access to every library the merge reaches.",
    )
}

#[cfg(feature = "docgen")]
fn preview_entity_merge_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Preview an entity merge").description(
        "Runs every check a merge runs and reports what it would do, changing nothing: the merge returns these counts, and checks again when it runs.",
    )
}

#[cfg(feature = "docgen")]
fn remove_external_id_alias_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Remove a kept external ID").description(
        "Removes a value kept from a merged duplicate, so that it stops finding this entity. Responds 404 when the entity holds no such alias. Requires ManageMetadata and access to every library the entity reaches.",
    )
}

pub(crate) fn entity_merge_routes() -> Router {
    Router::new()
        .route("/{id}/merge", post(merge_entities))
        .route("/{id}/merge/preview", post(preview_entity_merge))
        .route(
            "/{id}/external-ids/{provider_id}/{id_type}/aliases/{value}",
            delete(remove_external_id_alias),
        )
}

#[cfg(feature = "docgen")]
pub(crate) fn entity_merge_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::{
        delete_with,
        post_with,
    };

    aide::axum::ApiRouter::new()
        .api_route(
            "/{id}/merge",
            post_with(merge_entities, merge_entities_docs),
        )
        .api_route(
            "/{id}/merge/preview",
            post_with(preview_entity_merge, preview_entity_merge_docs),
        )
        .api_route(
            "/{id}/external-ids/{provider_id}/{id_type}/aliases/{value}",
            delete_with(remove_external_id_alias, remove_external_id_alias_docs),
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
    use lyra_db::test_db::insert_library;
    use nanoid::nanoid;
    use serde_json::{
        Value,
        json,
    };
    use tower::ServiceExt;

    use super::*;
    use crate::{
        plugins::testing::init_library_test_state,
        routes::{
            entity_routes,
            genre_routes,
        },
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
            .nest("/entities", entity_routes().merge(entity_merge_routes()))
            .nest("/genres", genre_routes())
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

    async fn merge_status(
        authorization: &str,
        winner: &str,
        losers: &[&str],
    ) -> anyhow::Result<(StatusCode, String)> {
        send(
            authorization,
            "POST",
            format!("/entities/{winner}/merge"),
            Some(json!({ "loser_ids": losers })),
        )
        .await
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
            &lyra_db::fixtures::test_user(&format!("merge-{}", nanoid!()))?,
        )?;
        let role = format!("merge-role-{}", nanoid!());
        lyra_db::roles::create(
            db,
            &lyra_db::roles::Role {
                db_id: None,
                id: nanoid!(),
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

        let (status, _) = merge_status(&listener, &scene.visible, &[&scene.both]).await?;
        assert_eq!(status, StatusCode::FORBIDDEN);
        let (status, _) = send(
            &listener,
            "PATCH",
            format!("/genres/{}", scene.visible),
            Some(json!({"name": "New"})),
        )
        .await?;
        assert_eq!(status, StatusCode::FORBIDDEN);
        Ok(())
    }

    fn insert_running_run(db: &mut DbAny, library_id: &str) -> anyhow::Result<()> {
        lyra_db::sync_runs::create(
            db,
            &lyra_db::sync_runs::SyncRunRecord {
                db_id: None,
                id: nanoid!(),
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
        let (status, body) = send(
            &scene.admin,
            "POST",
            format!("/entities/{}/merge/preview", scene.visible),
            Some(json!({"loser_ids": [scene.both]})),
        )
        .await?;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        let (status, body) = send(
            &scene.admin,
            "PATCH",
            format!("/genres/{}", scene.both),
            Some(json!({"name": "New"})),
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

        let (status, body) = send(
            &scene.admin,
            "POST",
            format!("/entities/{}/merge/preview", scene.visible),
            Some(json!({"loser_ids": [scene.hidden]})),
        )
        .await?;

        assert_eq!(status, StatusCode::OK, "{body}");
        let preview: Value = serde_json::from_str(&body)?;
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

        let uri = format!("/entities/{winner}/external-ids/provider/artist_id/aliases/b%2Fc");
        let (status, body) = send(&admin, "DELETE", uri.clone(), None).await?;
        assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
        let (status, _) = send(&admin, "DELETE", uri, None).await?;
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
        let path =
            |genre: &str| format!("/entities/{genre}/external-ids/provider/genre_id/aliases/x");

        let (status, _) = send(&scene.manager, "DELETE", path(&scene.hidden), None).await?;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) = send(&scene.manager, "DELETE", path(&scene.both), None).await?;
        assert_eq!(status, StatusCode::FORBIDDEN);
        Ok(())
    }

    #[tokio::test]
    async fn renaming_a_genre_answers_by_the_rules() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_library_test_state(std::path::PathBuf::from(".")).await?;
        let scene = scene().await?;
        let rename = |authorization: String, id: String, name: &'static str| async move {
            send(
                &authorization,
                "PATCH",
                format!("/genres/{id}"),
                Some(json!({ "name": name })),
            )
            .await
        };

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
            let (status, body) = rename(who.clone(), id.clone(), name).await?;
            assert_eq!(status, expected, "{id} {name}: {body}");
        }

        let (status, body) = rename(scene.manager.clone(), scene.visible.clone(), " Seen ").await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        let genre: Value = serde_json::from_str(&body)?;
        assert_eq!(genre, json!({"id": scene.visible, "name": "Seen"}));
        Ok(())
    }
}
