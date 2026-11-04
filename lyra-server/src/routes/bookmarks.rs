// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
use aide::transform::TransformOperation;
use axum::{
    Json,
    Router,
    extract::{
        Path,
        rejection::JsonRejection,
    },
    http::{
        HeaderMap,
        StatusCode,
    },
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
    routes::{
        request::bearer_credential,
        targets::{
            dense_lookup,
            resolve_target,
            validate_target_id,
        },
    },
    services::{
        STATE,
        auth::require_authenticated,
        bookmarks::{
            self as bookmark_service,
            BookmarkInput,
        },
    },
};
use lyra_db::bookmarks::Bookmark;

use super::AppError;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct SetBookmarkRequest {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Position in the track in milliseconds, a non-negative integer up to 2^53 - 1. A position past the track's known duration is stored as the duration."
        )
    )]
    position_ms: i64,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Optional note of at most 1000 characters. Empty or absent stores no comment."
        )
    )]
    #[serde(default)]
    comment: Option<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct BookmarkResponse {
    position_ms: u64,
    comment: Option<String>,
    created_at_ms: i64,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "When the bookmark was last set.")
    )]
    updated_at_ms: i64,
}

impl From<Bookmark> for BookmarkResponse {
    fn from(bookmark: Bookmark) -> Self {
        Self {
            position_ms: bookmark.position_ms,
            comment: bookmark.comment,
            created_at_ms: bookmark.created_at_ms,
            updated_at_ms: bookmark.updated_at_ms,
        }
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct BookmarkStateResponse {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "The authenticated user's bookmark, or null when the track has none or is missing, not a track, or not visible."
        )
    )]
    bookmark: Option<BookmarkResponse>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct CheckBookmarksRequest {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Track IDs to check. At most 500 submitted entries, counted before duplicate IDs are collapsed."
        )
    )]
    track_ids: Vec<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct CheckBookmarksResponse {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Dense `{ [submitted_id]: bookmark | null }` map. Duplicate IDs collapse to one key."
        )
    )]
    bookmarks: HashMap<String, Option<BookmarkResponse>>,
}

async fn set_bookmark(
    headers: HeaderMap,
    Path(track_id): Path<String>,
    body: Result<Json<SetBookmarkRequest>, JsonRejection>,
) -> Result<StatusCode, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    validate_target_id(&track_id)?;
    let Json(request) = body.map_err(|rejection| AppError::bad_request(rejection.body_text()))?;
    let input = BookmarkInput::new(request.position_ms, request.comment)
        .map_err(|err| AppError::bad_request(err.to_string()))?;

    let mut db = STATE.db.write().await;
    let track_db_id = resolve_target(&*db, &track_id, || track_not_found(&track_id))?;
    let outcome = bookmark_service::set_for_principal(&mut db, &principal, track_db_id, &input)?;
    mutation_response(outcome, &track_id)
}

async fn get_bookmark(
    headers: HeaderMap,
    Path(track_id): Path<String>,
) -> Result<Json<BookmarkStateResponse>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    validate_target_id(&track_id)?;

    let db = STATE.db.read().await;
    let bookmark = match lyra_db::lookup::find_node_id_by_id(&*db, &track_id)? {
        Some(track_db_id) => bookmark_service::get_for_principal(&db, &principal, track_db_id)?,
        None => None,
    };
    Ok(Json(BookmarkStateResponse {
        bookmark: bookmark.map(BookmarkResponse::from),
    }))
}

async fn delete_bookmark(
    headers: HeaderMap,
    Path(track_id): Path<String>,
) -> Result<StatusCode, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    validate_target_id(&track_id)?;

    let mut db = STATE.db.write().await;
    principal.require(&db)?;
    let track_db_id = resolve_target(&*db, &track_id, || track_not_found(&track_id))?;
    let outcome = bookmark_service::remove_for_principal(&mut db, &principal, track_db_id)?;
    mutation_response(outcome, &track_id)
}

async fn check_bookmarks(
    headers: HeaderMap,
    Json(request): Json<CheckBookmarksRequest>,
) -> Result<Json<CheckBookmarksResponse>, AppError> {
    let bookmarks = dense_lookup(
        &headers,
        request.track_ids,
        "bookmark check",
        |db, principal, ids| {
            Ok(bookmark_service::for_principal(db, principal, ids)?
                .into_iter()
                .map(|(id, bookmark)| (id, BookmarkResponse::from(bookmark)))
                .collect())
        },
    )
    .await?;
    Ok(Json(CheckBookmarksResponse { bookmarks }))
}

fn mutation_response(
    outcome: bookmark_service::MutationOutcome,
    track_id: &str,
) -> Result<StatusCode, AppError> {
    match outcome {
        bookmark_service::MutationOutcome::Applied => Ok(StatusCode::NO_CONTENT),
        bookmark_service::MutationOutcome::NotTargetable => Err(track_not_found(track_id)),
    }
}

fn track_not_found(track_id: &str) -> AppError {
    AppError::not_found(format!("bookmark track not found: {track_id}"))
}

#[cfg(feature = "docgen")]
fn set_bookmark_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Set a bookmark")
        .description(
            "Creates or replaces the authenticated user's bookmark of a visible track: a \
             position in milliseconds and an optional comment. Replacing keeps \
             `created_at_ms` and sets `updated_at_ms`. A position past the track's known \
             duration is stored as the duration. Returns 204 on success, 400 for malformed \
             IDs, a negative or non-integer position, or a comment over 1000 characters, \
             and 404 for missing, non-track, or non-visible targets.",
        )
        .response::<204, ()>()
}

#[cfg(feature = "docgen")]
fn get_bookmark_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get a bookmark").description(
        "Returns `{ bookmark: { position_ms, comment, created_at_ms, updated_at_ms } | null }`. \
         Missing, non-track, non-visible, and unbookmarked targets all produce `null`.",
    )
}

#[cfg(feature = "docgen")]
fn delete_bookmark_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Delete a bookmark")
        .description(
            "Deletes the authenticated user's bookmark of a track. The operation is idempotent \
             for tracks. Returns 404 for missing or non-track targets and tracks the user \
             cannot see; the bookmark of a track they can no longer see is still deleted, so \
             it cannot become undeletable after library access changes.",
        )
        .response::<204, ()>()
}

#[cfg(feature = "docgen")]
fn check_bookmarks_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Check bookmarks for tracks").description(
        "Checks up to 500 submitted track IDs, counted before duplicate IDs are collapsed, and \
         returns a dense `{ bookmarks: { [submitted_id]: bookmark | null } }` map. Missing, \
         malformed, non-track, non-visible, and unbookmarked targets all map to `null`; an \
         empty input returns an empty map. Malformed JSON and requests containing more than \
         500 submitted IDs return 400.",
    )
}

pub(super) fn me_bookmark_routes() -> Router {
    Router::new()
        .route("/check", post(check_bookmarks))
        .route("/{track_id}", put(set_bookmark))
        .route("/{track_id}", get(get_bookmark))
        .route("/{track_id}", delete(delete_bookmark))
}

#[cfg(feature = "docgen")]
pub(super) fn me_bookmark_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::{
        delete_with,
        get_with,
        post_with,
        put_with,
    };

    aide::axum::ApiRouter::new()
        .api_route("/check", post_with(check_bookmarks, check_bookmarks_docs))
        .api_route("/{track_id}", put_with(set_bookmark, set_bookmark_docs))
        .api_route("/{track_id}", get_with(get_bookmark, get_bookmark_docs))
        .api_route(
            "/{track_id}",
            delete_with(delete_bookmark, delete_bookmark_docs),
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
        insert_release,
        insert_track,
    };
    use lyra_db::test_db::insert_library;
    use tower::ServiceExt;

    async fn send(
        routes: &Router,
        authorization: &str,
        method: &str,
        uri: &str,
        body: Option<serde_json::Value>,
    ) -> anyhow::Result<(StatusCode, serde_json::Value)> {
        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .header(AUTHORIZATION, authorization);
        let body = match body {
            Some(body) => {
                request = request.header(CONTENT_TYPE, "application/json");
                Body::from(serde_json::to_vec(&body)?)
            }
            None => Body::empty(),
        };
        let response = routes.clone().oneshot(request.body(body)?).await?;
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await?;
        let value = if bytes.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
        };
        Ok((status, value))
    }

    #[tokio::test]
    async fn bookmark_routes_set_read_check_and_delete() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_library_test_state(std::path::PathBuf::from(".")).await?;

        let (user_db_id, track_id, release_id) = {
            let mut db = STATE.db.write().await;
            lyra_db::roles::ensure_builtin_roles(&mut db)?;
            let user_db_id =
                lyra_db::users::create(&mut db, &lyra_db::fixtures::test_user("bookmarks-admin")?)?;
            lyra_db::roles::ensure_user_has_role(
                &mut db,
                user_db_id,
                lyra_db::roles::BUILTIN_ADMIN_ROLE,
            )?;
            let library = insert_library(&mut db, "Bookmarks", "/tmp/lyra-bookmarks-route")?;
            let release = insert_release(&mut db, "Release")?;
            let track = insert_track(&mut db, "Bookmarked Track")?;
            connect(&mut db, library, release)?;
            connect(&mut db, library, track)?;
            (
                user_db_id,
                lyra_db::lookup::find_id_by_db_id(&db, track)?.unwrap(),
                lyra_db::lookup::find_id_by_db_id(&db, release)?.unwrap(),
            )
        };
        let session =
            crate::services::testing::create_session(user_db_id, Default::default()).await?;
        let authorization = format!("Bearer {}", session.token);
        let routes = me_bookmark_routes();
        let track_uri = format!("/{track_id}");

        for body in [
            serde_json::json!({ "position_ms": -1 }),
            serde_json::json!({ "position_ms": 1.5 }),
            serde_json::json!({ "position_ms": 0, "comment": "a".repeat(1001) }),
        ] {
            let (status, _) = send(&routes, &authorization, "PUT", &track_uri, Some(body)).await?;
            assert_eq!(status, StatusCode::BAD_REQUEST);
        }
        let (status, _) = send(
            &routes,
            &authorization,
            "PUT",
            &format!("/{release_id}"),
            Some(serde_json::json!({ "position_ms": 0 })),
        )
        .await?;
        assert_eq!(status, StatusCode::NOT_FOUND);

        let (status, _) = send(
            &routes,
            &authorization,
            "PUT",
            &track_uri,
            Some(serde_json::json!({ "position_ms": 42_000, "comment": "chorus" })),
        )
        .await?;
        assert_eq!(status, StatusCode::NO_CONTENT);

        let (status, body) = send(&routes, &authorization, "GET", &track_uri, None).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["bookmark"]["position_ms"], 42_000);
        assert_eq!(body["bookmark"]["comment"], "chorus");
        assert_eq!(
            body["bookmark"]["created_at_ms"],
            body["bookmark"]["updated_at_ms"]
        );

        let (status, body) = send(
            &routes,
            &authorization,
            "POST",
            "/check",
            Some(serde_json::json!({ "track_ids": [track_id, release_id, "bad/id"] })),
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["bookmarks"][&track_id]["position_ms"], 42_000);
        assert_eq!(body["bookmarks"][&release_id], serde_json::Value::Null);
        assert_eq!(body["bookmarks"]["bad/id"], serde_json::Value::Null);
        assert_eq!(body["bookmarks"].as_object().unwrap().len(), 3);

        let (status, _) = send(&routes, &authorization, "DELETE", &track_uri, None).await?;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, _) = send(&routes, &authorization, "DELETE", &track_uri, None).await?;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (_, body) = send(&routes, &authorization, "GET", &track_uri, None).await?;
        assert_eq!(body["bookmark"], serde_json::Value::Null);
        let (status, _) = send(
            &routes,
            &authorization,
            "DELETE",
            &format!("/{release_id}"),
            None,
        )
        .await?;
        assert_eq!(status, StatusCode::NOT_FOUND);
        Ok(())
    }
}
