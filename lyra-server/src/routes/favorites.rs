// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::DbId;
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
        favorites as favorite_service,
        pagination::SnapshotKey,
    },
};
use lyra_db::favorites::FavoriteKind;

const CHECK_TARGET_CAP: usize = 500;

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
        schemars(
            description = "Target IDs to check. At most 500 submitted entries, counted before duplicate IDs are collapsed."
        )
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
                       empty input returns an empty map and duplicate IDs collapse to one key."
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
        first_favorited_at: crate::services::timestamps::unix_ms_to_rfc3339_i64(
            item.first_favorited_at_ms,
        ),
        last_refreshed_at: crate::services::timestamps::unix_ms_to_rfc3339_i64(
            item.last_refreshed_at_ms,
        ),
    }
}

async fn put_favorite(
    headers: HeaderMap,
    Path(target_id): Path<String>,
) -> Result<StatusCode, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    validate_target_id(&target_id)?;

    let mut db = STATE.db.write().await;
    let Some(target_db_id) = lyra_db::lookup::find_node_id_by_id(&*db, &target_id)? else {
        principal.require(&db)?;
        return Err(target_not_found(&target_id));
    };
    let outcome = favorite_service::add_for_principal(&mut db, &principal, target_db_id)?;
    mutation_response(outcome, &target_id)
}

async fn delete_favorite(
    headers: HeaderMap,
    Path(target_id): Path<String>,
) -> Result<StatusCode, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    validate_target_id(&target_id)?;

    let mut db = STATE.db.write().await;
    let user_db_id = principal.require(&db)?;
    let target_db_id = resolve_target(&*db, &target_id, || target_not_found(&target_id))?;
    let outcome = favorite_service::remove(&mut db, user_db_id, target_db_id)?;
    mutation_response(outcome, &target_id)
}

fn mutation_response(
    outcome: favorite_service::MutationOutcome,
    target_id: &str,
) -> Result<StatusCode, AppError> {
    match outcome {
        favorite_service::MutationOutcome::Applied => Ok(StatusCode::NO_CONTENT),
        favorite_service::MutationOutcome::NotTargetable => Err(target_not_found(target_id)),
    }
}

fn target_not_found(target_id: &str) -> AppError {
    AppError::not_found(format!("favorite target not found: {target_id}"))
}

async fn get_favorite_state(
    headers: HeaderMap,
    Path(target_id): Path<String>,
    Query(_): Query<FavoriteStateResponseQuery>,
) -> Result<Json<FavoriteStateResponse>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    validate_target_id(&target_id)?;

    let db = STATE.db.read().await;
    let favorited = match lyra_db::lookup::find_node_id_by_id(&*db, &target_id)? {
        Some(target_db_id) => favorite_service::has_for_principal(&db, &principal, target_db_id)?,
        None => {
            principal.require(&db)?;
            false
        }
    };
    Ok(Json(FavoriteStateResponse { favorited }))
}

async fn check_favorites(
    headers: HeaderMap,
    Json(request): Json<CheckRequest>,
) -> Result<Json<CheckResponse>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;

    if request.target_ids.len() > CHECK_TARGET_CAP {
        return Err(AppError::bad_request(format!(
            "favorite check cap exceeded: {} > {}",
            request.target_ids.len(),
            CHECK_TARGET_CAP,
        )));
    }

    let db = STATE.db.read().await;
    let mut favorited: HashMap<String, bool> = request
        .target_ids
        .iter()
        .map(|target_id| (target_id.clone(), false))
        .collect();
    let target_ids: Vec<&str> = favorited.keys().map(String::as_str).collect();
    let resolved = lyra_db::lookup::find_node_ids_by_ids(&*db, &target_ids)?;
    let target_db_ids: Vec<DbId> = resolved.values().copied().collect();
    let favorited_db_ids =
        favorite_service::favorited_at_for_principal(&db, &principal, &target_db_ids)?;
    for (target_id, target_db_id) in resolved {
        if favorited_db_ids.contains_key(&target_db_id) {
            favorited.insert(target_id, true);
        }
    }
    Ok(Json(CheckResponse { favorited }))
}

async fn list_favorites(
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> Result<Json<PageResponse<FavoriteItem>>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
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
        "Checks up to 500 submitted target IDs, counted before duplicate IDs are collapsed, and \
         returns a dense `{ favorited: { [submitted_id]: bool } }` map. Empty input returns an \
         empty map and duplicate IDs collapse to one key. Malformed, missing, unsupported, and \
         non-visible IDs all map to `false`. Requests containing more than 500 submitted IDs \
         return 400.",
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

pub(super) fn me_favorite_routes() -> Router {
    Router::new()
        .route("/", get(list_favorites))
        .route("/check", post(check_favorites))
        .route("/{target_id}", put(put_favorite))
        .route("/{target_id}", delete(delete_favorite))
        .route("/{target_id}", get(get_favorite_state))
}

#[cfg(feature = "docgen")]
pub(super) fn me_favorite_openapi_routes() -> aide::axum::ApiRouter {
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
        response::IntoResponse,
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
    async fn favorite_routes_resolve_public_ids() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_library_test_state(std::path::PathBuf::from(".")).await?;

        let (user_db_id, track_id, user_id) = {
            let mut db = STATE.db.write().await;
            lyra_db::roles::ensure_builtin_roles(&mut db)?;
            let user_db_id = lyra_db::users::create(
                &mut db,
                &lyra_db::fixtures::test_user("favorites-route-admin")?,
            )?;
            lyra_db::roles::ensure_user_has_role(
                &mut db,
                user_db_id,
                lyra_db::roles::BUILTIN_ADMIN_ROLE,
            )?;
            let library = insert_library(&mut db, "Favorites", "/tmp/lyra-favorites-route")?;
            let track = insert_track(&mut db, "Favorite Track")?;
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
        let routes = me_favorite_routes();
        let missing_id = "missing-favorite-target";

        for method in ["PUT", "DELETE"] {
            assert_eq!(
                send(&routes, method, "/bad!id", &auth, None).await?,
                (
                    StatusCode::BAD_REQUEST,
                    "Error: malformed target id: bad!id".to_string()
                ),
            );
            for target in [missing_id, user_id.as_str()] {
                assert_eq!(
                    send(&routes, method, &format!("/{target}"), &auth, None).await?,
                    (
                        StatusCode::NOT_FOUND,
                        format!("Error: favorite target not found: {target}")
                    ),
                );
            }
        }
        for target in [missing_id, user_id.as_str(), track_id.as_str()] {
            assert_eq!(
                send(&routes, "GET", &format!("/{target}"), &auth, None).await?,
                (StatusCode::OK, r#"{"favorited":false}"#.to_string()),
            );
        }

        assert_eq!(
            send(&routes, "PUT", &format!("/{track_id}"), &auth, None)
                .await?
                .0,
            StatusCode::NO_CONTENT,
        );
        assert_eq!(
            send(&routes, "GET", &format!("/{track_id}"), &auth, None).await?,
            (StatusCode::OK, r#"{"favorited":true}"#.to_string()),
        );
        let (status, body) = send(
            &routes,
            "POST",
            "/check",
            &auth,
            Some(serde_json::json!({
                "target_ids": [track_id, track_id, missing_id, "bad/id", user_id],
            })),
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&body)?,
            serde_json::json!({
                "favorited": {
                    track_id.clone(): true,
                    missing_id: false,
                    "bad/id": false,
                    user_id: false,
                },
            }),
        );

        assert_eq!(
            send(&routes, "DELETE", &format!("/{track_id}"), &auth, None)
                .await?
                .0,
            StatusCode::NO_CONTENT,
        );
        assert_eq!(
            send(&routes, "GET", &format!("/{track_id}"), &auth, None).await?,
            (StatusCode::OK, r#"{"favorited":false}"#.to_string()),
        );
        Ok(())
    }

    #[tokio::test]
    async fn favorite_routes_hide_targets_outside_the_callers_libraries() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_library_test_state(std::path::PathBuf::from(".")).await?;

        let (viewer, visible_id, hidden_id) = {
            let mut db = STATE.db.write().await;
            lyra_db::roles::ensure_builtin_roles(&mut db)?;
            let viewer = lyra_db::users::create(
                &mut db,
                &lyra_db::fixtures::test_user("favorites-route-viewer")?,
            )?;
            let visible_library =
                insert_library(&mut db, "Visible", "/tmp/lyra-favorites-route-visible")?;
            let hidden_library =
                insert_library(&mut db, "Hidden", "/tmp/lyra-favorites-route-hidden")?;
            lyra_db::libraries::grant_access(
                &mut *db,
                viewer,
                visible_library,
                lyra_db::libraries::AccessKind::ReadWrite,
            )?;
            let visible = insert_track(&mut db, "Visible Track")?;
            let hidden = insert_track(&mut db, "Hidden Track")?;
            connect(&mut db, visible_library, visible)?;
            connect(&mut db, hidden_library, hidden)?;
            lyra_db::favorites::add(
                &mut db,
                viewer,
                hidden,
                lyra_db::favorites::FavoriteKind::Track,
                1,
            )?;
            (
                viewer,
                lyra_db::lookup::find_id_by_db_id(&db, visible)?.unwrap(),
                lyra_db::lookup::find_id_by_db_id(&db, hidden)?.unwrap(),
            )
        };
        let session = crate::services::testing::create_session(viewer, Default::default()).await?;
        let auth = format!("Bearer {}", session.token);
        let routes = me_favorite_routes();

        assert_eq!(
            send(&routes, "PUT", &format!("/{hidden_id}"), &auth, None).await?,
            (
                StatusCode::NOT_FOUND,
                format!("Error: favorite target not found: {hidden_id}")
            ),
        );
        assert_eq!(
            send(&routes, "GET", &format!("/{hidden_id}"), &auth, None).await?,
            (StatusCode::OK, r#"{"favorited":false}"#.to_string()),
        );
        assert_eq!(
            send(&routes, "PUT", &format!("/{visible_id}"), &auth, None)
                .await?
                .0,
            StatusCode::NO_CONTENT,
        );
        let (status, body) = send(
            &routes,
            "POST",
            "/check",
            &auth,
            Some(serde_json::json!({ "target_ids": [visible_id, hidden_id] })),
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&body)?,
            serde_json::json!({ "favorited": { visible_id: true, hidden_id.clone(): false } }),
        );
        assert_eq!(
            send(&routes, "DELETE", &format!("/{hidden_id}"), &auth, None)
                .await?
                .0,
            StatusCode::NO_CONTENT,
            "remove has no visibility gate",
        );
        let db = STATE.db.read().await;
        let hidden = lyra_db::lookup::find_node_id_by_id(&*db, &hidden_id)?.unwrap();
        assert!(!lyra_db::favorites::has(&*db, viewer, hidden)?);
        Ok(())
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

    #[tokio::test]
    async fn check_favorites_enforces_http_cap_before_deduplication() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_library_test_state(std::path::PathBuf::from(".")).await?;

        let user_db_id = {
            let mut db = STATE.db.write().await;
            lyra_db::users::create(
                &mut db,
                &lyra_db::fixtures::test_user("favorites-check-user")?,
            )?
        };
        let session =
            crate::services::testing::create_session(user_db_id, Default::default()).await?;
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            format!("Bearer {}", session.token).parse().unwrap(),
        );
        let missing_id = "missing-favorite-target".to_string();

        let Json(exact_cap) = check_favorites(
            headers.clone(),
            Json(CheckRequest {
                target_ids: vec![missing_id.clone(); CHECK_TARGET_CAP],
            }),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert_eq!(exact_cap.favorited.len(), 1);
        assert_eq!(exact_cap.favorited.get(&missing_id), Some(&false));

        let over_cap = match check_favorites(
            headers,
            Json(CheckRequest {
                target_ids: vec![missing_id; CHECK_TARGET_CAP + 1],
            }),
        )
        .await
        {
            Ok(_) => return Err(anyhow::anyhow!("favorite check accepted 501 IDs")),
            Err(err) => err.into_response(),
        };
        assert_eq!(over_cap.status(), StatusCode::BAD_REQUEST);
        Ok(())
    }
}
