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
    dense_lookup,
    resolve_target,
    validate_target_id,
};
use crate::{
    services::STATE,
    services::{
        auth::require_authenticated,
        ratings as rating_service,
    },
};
use lyra_db::ratings::{
    InvalidRatingFilter,
    MAX_VALUE,
    MIN_VALUE,
    RatingValue,
};

use super::AppError;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Default, Deserialize)]
pub(crate) struct RatingFilterQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Optional: `true` keeps only entities the authenticated user has rated, `false` only those they have not. `false` cannot be combined with `min_rating` or `max_rating`."
        )
    )]
    #[serde(default, deserialize_with = "super::deserialize_optional_scalar")]
    rated: Option<bool>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Optional inclusive minimum personal rating. Must be an integer from 1 through 5. Unrated entities are excluded."
        )
    )]
    #[serde(default, deserialize_with = "super::deserialize_optional_scalar")]
    min_rating: Option<i64>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Optional inclusive maximum personal rating. Must be an integer from 1 through 5. Unrated entities are excluded."
        )
    )]
    #[serde(default, deserialize_with = "super::deserialize_optional_scalar")]
    max_rating: Option<i64>,
}

impl RatingFilterQuery {
    #[cfg(test)]
    pub(crate) fn from_bounds(min_rating: Option<i64>, max_rating: Option<i64>) -> Self {
        Self {
            rated: None,
            min_rating,
            max_rating,
        }
    }

    pub(crate) fn parse(self) -> Result<lyra_db::ratings::RatingFilter, AppError> {
        let min = self
            .min_rating
            .map(|value| parse_rating_bound("min_rating", value))
            .transpose()?;
        let max = self
            .max_rating
            .map(|value| parse_rating_bound("max_rating", value))
            .transpose()?;
        lyra_db::ratings::RatingFilter::new(self.rated, min, max).map_err(|err| match err {
            InvalidRatingFilter::Inverted => {
                AppError::bad_request("min_rating must not exceed max_rating")
            }
            InvalidRatingFilter::BoundsOnUnrated => AppError::bad_request(
                "min_rating and max_rating cannot be combined with rated=false",
            ),
        })
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct AverageRatingsRequest {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Target IDs to average. At most 500 submitted entries, counted before duplicate IDs are collapsed."
        )
    )]
    target_ids: Vec<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct AverageRatingsResponse {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Dense `{ [submitted_id]: number | null }` map of unrounded means over every user's 1-5 rating. Duplicate IDs collapse to one key."
        )
    )]
    averages: HashMap<String, Option<f64>>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct SetRatingRequest {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Personal rating from 1 through 5, inclusive.")
    )]
    rating: i64,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct RatingStateResponse {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "The authenticated user's 1-5 rating, or null when the target is unrated, missing, unsupported, or not visible."
        )
    )]
    rating: Option<u8>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct CheckRatingsRequest {
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
struct CheckRatingsResponse {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Dense `{ [submitted_id]: 1..5 | null }` map. Duplicate IDs collapse to one key."
        )
    )]
    ratings: HashMap<String, Option<u8>>,
}

async fn set_rating(
    headers: HeaderMap,
    Path(target_id): Path<String>,
    Json(request): Json<SetRatingRequest>,
) -> Result<StatusCode, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    validate_target_id(&target_id)?;
    let rating = parse_rating(request.rating)?;

    let mut db = STATE.db.write().await;
    let target_db_id = resolve_target(&*db, &target_id, || target_not_found(&target_id))?;
    let outcome = rating_service::set_for_principal(&mut db, &principal, target_db_id, rating)?;
    mutation_response(outcome, &target_id)
}

async fn get_rating(
    headers: HeaderMap,
    Path(target_id): Path<String>,
) -> Result<Json<RatingStateResponse>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    validate_target_id(&target_id)?;

    let db = STATE.db.read().await;
    let rating = match lyra_db::lookup::find_node_id_by_id(&*db, &target_id)? {
        Some(target_db_id) => {
            rating_service::get_for_principal(&db, &principal, target_db_id)?.map(RatingValue::get)
        }
        None => None,
    };
    Ok(Json(RatingStateResponse { rating }))
}

async fn delete_rating(
    headers: HeaderMap,
    Path(target_id): Path<String>,
) -> Result<StatusCode, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    validate_target_id(&target_id)?;

    let mut db = STATE.db.write().await;
    principal.require(&db)?;
    let target_db_id = resolve_target(&*db, &target_id, || target_not_found(&target_id))?;
    let outcome = rating_service::remove_for_principal(&mut db, &principal, target_db_id)?;
    mutation_response(outcome, &target_id)
}

fn mutation_response(
    outcome: rating_service::MutationOutcome,
    target_id: &str,
) -> Result<StatusCode, AppError> {
    match outcome {
        rating_service::MutationOutcome::Applied => Ok(StatusCode::NO_CONTENT),
        rating_service::MutationOutcome::NotTargetable => Err(target_not_found(target_id)),
    }
}

fn target_not_found(target_id: &str) -> AppError {
    AppError::not_found(format!("rating target not found: {target_id}"))
}

async fn check_ratings(
    headers: HeaderMap,
    Json(request): Json<CheckRatingsRequest>,
) -> Result<Json<CheckRatingsResponse>, AppError> {
    let ratings = dense_lookup(
        &headers,
        request.target_ids,
        "rating check",
        |db, principal, ids| {
            Ok(rating_service::values_for_principal(db, principal, ids)?
                .into_iter()
                .map(|(id, value)| (id, value.get()))
                .collect())
        },
    )
    .await?;
    Ok(Json(CheckRatingsResponse { ratings }))
}

async fn average_ratings(
    headers: HeaderMap,
    Json(request): Json<AverageRatingsRequest>,
) -> Result<Json<AverageRatingsResponse>, AppError> {
    let averages = dense_lookup(
        &headers,
        request.target_ids,
        "average rating",
        rating_service::averages_for_principal,
    )
    .await?;
    Ok(Json(AverageRatingsResponse { averages }))
}

fn parse_rating(raw: i64) -> Result<RatingValue, AppError> {
    parse_rating_bound("rating", raw)
}

fn parse_rating_bound(name: &str, raw: i64) -> Result<RatingValue, AppError> {
    u8::try_from(raw)
        .ok()
        .and_then(RatingValue::new)
        .ok_or_else(|| {
            AppError::bad_request(format!(
                "{name} must be an integer from {MIN_VALUE} through {MAX_VALUE}"
            ))
        })
}

#[cfg(feature = "docgen")]
fn set_rating_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Set a personal rating")
        .description(
            "Creates or replaces the authenticated user's 1-5 rating for a visible track, \
             release, or artist. Playlists are not ratable, though they can be favorited. \
             Returns 204 on success, 400 for malformed IDs or ratings, and 404 for missing, \
             unsupported, or non-visible targets.",
        )
        .response::<204, ()>()
}

#[cfg(feature = "docgen")]
fn get_rating_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get personal rating state").description(
        "Returns `{ rating: number | null }`. Missing, unsupported, non-visible, and unrated \
         targets all produce `null`.",
    )
}

#[cfg(feature = "docgen")]
fn delete_rating_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Delete a personal rating")
        .description(
            "Deletes the authenticated user's rating. The operation is idempotent for supported \
             targets. Returns 404 for missing targets, unsupported kinds and targets the user \
             cannot see; the rating of a target they can no longer see is still deleted, so it \
             cannot become undeletable after library access changes.",
        )
        .response::<204, ()>()
}

#[cfg(feature = "docgen")]
fn check_ratings_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Check personal ratings for targets").description(
        "Checks up to 500 submitted target IDs, counted before duplicate IDs are collapsed, and \
         returns a dense `{ ratings: { [submitted_id]: 1..5 | null } }` map. Mixed track, \
         release, and artist IDs are supported. Missing, malformed, unsupported, non-visible, \
         other-user-only, and unrated targets all map to `null`; an empty input \
         returns an empty map. Unlike singular rating routes, malformed individual target IDs do \
         not return 400. Malformed JSON and requests containing more than 500 submitted IDs \
         return 400.",
    )
}

#[cfg(feature = "docgen")]
fn average_ratings_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Average ratings across users").description(
        "Averages every user's rating of up to 500 submitted target IDs, counted before \
         duplicate IDs are collapsed, and returns a dense \
         `{ averages: { [submitted_id]: number | null } }` map of unrounded means. Mixed track, \
         release, and artist IDs are supported. Missing, malformed, unsupported, non-visible, \
         and unrated targets map to `null`, as does every target while the `instance_stats` \
         server setting is off. Malformed JSON and requests containing more than 500 \
         submitted IDs return 400.",
    )
}

pub fn rating_routes() -> Router {
    Router::new().route("/averages", post(average_ratings))
}

#[cfg(feature = "docgen")]
pub(crate) fn rating_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::post_with;

    aide::axum::ApiRouter::new().api_route(
        "/averages",
        post_with(average_ratings, average_ratings_docs),
    )
}

pub(super) fn me_rating_routes() -> Router {
    Router::new()
        .route("/check", post(check_ratings))
        .route("/{target_id}", put(set_rating))
        .route("/{target_id}", get(get_rating))
        .route("/{target_id}", delete(delete_rating))
}

#[cfg(feature = "docgen")]
pub(super) fn me_rating_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::{
        delete_with,
        get_with,
        post_with,
        put_with,
    };

    aide::axum::ApiRouter::new()
        .api_route("/check", post_with(check_ratings, check_ratings_docs))
        .api_route("/{target_id}", put_with(set_rating, set_rating_docs))
        .api_route("/{target_id}", get_with(get_rating, get_rating_docs))
        .api_route(
            "/{target_id}",
            delete_with(delete_rating, delete_rating_docs),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::targets::CHECK_TARGET_CAP;
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

    #[test]
    fn rating_filters_parse_when_flattened_into_a_query_string() {
        #[derive(Deserialize)]
        struct ListQuery {
            #[serde(flatten)]
            rating: RatingFilterQuery,
        }

        let parse = |query: &str| {
            let uri: axum::http::Uri = format!("/?{query}").parse().expect("valid uri");
            axum::extract::Query::<ListQuery>::try_from_uri(&uri).map(|query| query.0.rating)
        };
        let rating = parse("min_rating=2&max_rating=4").expect("bounds parse");
        assert_eq!((rating.min_rating, rating.max_rating), (Some(2), Some(4)));
        let rating = parse("min_rating=").expect("a blank bound parses");
        assert_eq!((rating.min_rating, rating.max_rating), (None, None));
        assert!(parse("min_rating=high").is_err());
        assert_eq!(
            parse("rated=false").expect("rated parses").rated,
            Some(false)
        );
        assert!(parse("rated=maybe").is_err());
    }

    #[test]
    fn rating_bounds_are_inclusive() {
        assert_eq!(parse_rating(1).unwrap().get(), 1);
        assert_eq!(parse_rating(5).unwrap().get(), 5);
        assert!(parse_rating(0).is_err());
        assert!(parse_rating(6).is_err());
        assert!(parse_rating(-1).is_err());
    }

    #[test]
    fn rating_filter_parses_optional_inclusive_bounds() {
        assert!(RatingFilterQuery::default().parse().unwrap().is_any());
        assert_eq!(
            RatingFilterQuery {
                rated: None,
                min_rating: Some(1),
                max_rating: None,
            }
            .parse()
            .unwrap()
            .bounds(),
            (Some(1), None),
        );
        assert_eq!(
            RatingFilterQuery {
                rated: None,
                min_rating: None,
                max_rating: Some(5),
            }
            .parse()
            .unwrap()
            .bounds(),
            (None, Some(5)),
        );
        assert_eq!(
            RatingFilterQuery {
                rated: None,
                min_rating: Some(3),
                max_rating: Some(3),
            }
            .parse()
            .unwrap()
            .bounds(),
            (Some(3), Some(3)),
        );
    }

    #[test]
    fn rating_filter_rejects_bounds_on_unrated() {
        assert_eq!(
            RatingFilterQuery {
                rated: Some(false),
                min_rating: None,
                max_rating: None,
            }
            .parse()
            .unwrap(),
            lyra_db::ratings::RatingFilter::Unrated,
        );
        assert!(
            RatingFilterQuery {
                rated: Some(false),
                min_rating: Some(1),
                max_rating: None,
            }
            .parse()
            .is_err()
        );
    }

    #[test]
    fn rating_filter_rejects_invalid_bounds() {
        assert!(
            RatingFilterQuery {
                rated: None,
                min_rating: Some(0),
                max_rating: None,
            }
            .parse()
            .is_err()
        );
        assert!(
            RatingFilterQuery {
                rated: None,
                min_rating: None,
                max_rating: Some(6),
            }
            .parse()
            .is_err()
        );
        assert!(
            RatingFilterQuery {
                rated: None,
                min_rating: Some(4),
                max_rating: Some(3),
            }
            .parse()
            .is_err()
        );
    }

    #[tokio::test]
    async fn check_ratings_route_is_authenticated_and_enforces_raw_cap() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_library_test_state(std::path::PathBuf::from(".")).await?;

        let (user_db_id, track_id) = {
            let mut db = STATE.db.write().await;
            lyra_db::roles::ensure_builtin_roles(&mut db)?;
            let user_db_id = lyra_db::users::create(
                &mut db,
                &lyra_db::fixtures::test_user("ratings-check-admin")?,
            )?;
            lyra_db::roles::ensure_user_has_role(
                &mut db,
                user_db_id,
                lyra_db::roles::BUILTIN_ADMIN_ROLE,
            )?;
            let library = insert_library(&mut db, "Ratings", "/tmp/lyra-ratings-check-route")?;
            let track = insert_track(&mut db, "Checked Track")?;
            connect(&mut db, library, track)?;
            lyra_db::ratings::upsert(
                &mut *db,
                user_db_id,
                track,
                lyra_db::ratings::RatingValue::new(2).unwrap(),
                100,
            )?;
            (
                user_db_id,
                lyra_db::lookup::find_id_by_db_id(&db, track)?.unwrap(),
            )
        };
        let session =
            crate::services::testing::create_session(user_db_id, Default::default()).await?;
        let authorization = format!("Bearer {}", session.token);
        let routes = me_rating_routes();

        let response = routes
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/check")
                    .header(CONTENT_TYPE, "application/json")
                    .header(AUTHORIZATION, &authorization)
                    .body(Body::from(serde_json::to_vec(&serde_json::json!({
                        "target_ids": [track_id.clone(), track_id.clone(), "missing-id", "bad/id"],
                    }))?))?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "application/json",
        );
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body)?,
            serde_json::json!({
                "ratings": {
                    track_id.clone(): 2,
                    "missing-id": null,
                    "bad/id": null,
                },
            }),
        );

        let exact_cap = routes
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/check")
                    .header(CONTENT_TYPE, "application/json")
                    .header(AUTHORIZATION, &authorization)
                    .body(Body::from(serde_json::to_vec(&serde_json::json!({
                        "target_ids": vec![track_id.clone(); CHECK_TARGET_CAP],
                    }))?))?,
            )
            .await?;
        assert_eq!(exact_cap.status(), StatusCode::OK);

        let over_cap = routes
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/check")
                    .header(CONTENT_TYPE, "application/json")
                    .header(AUTHORIZATION, &authorization)
                    .body(Body::from(serde_json::to_vec(&serde_json::json!({
                        "target_ids": vec![track_id; CHECK_TARGET_CAP + 1],
                    }))?))?,
            )
            .await?;
        assert_eq!(over_cap.status(), StatusCode::BAD_REQUEST);

        let unauthenticated = routes
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/check")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"target_ids":[]}"#))?,
            )
            .await?;
        assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);

        let malformed_json = routes
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/check")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from("{"))?,
            )
            .await?;
        assert_eq!(malformed_json.status(), StatusCode::BAD_REQUEST);
        Ok(())
    }

    #[tokio::test]
    async fn average_ratings_route_is_dense_capped_and_gated_by_instance_stats()
    -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_library_test_state(std::path::PathBuf::from(".")).await?;

        let (user_db_id, rated_id, unrated_id) = {
            let mut db = STATE.db.write().await;
            lyra_db::roles::ensure_builtin_roles(&mut db)?;
            let user_db_id = lyra_db::users::create(
                &mut db,
                &lyra_db::fixtures::test_user("ratings-average-admin")?,
            )?;
            lyra_db::roles::ensure_user_has_role(
                &mut db,
                user_db_id,
                lyra_db::roles::BUILTIN_ADMIN_ROLE,
            )?;
            let other_user_db_id = lyra_db::users::create(
                &mut db,
                &lyra_db::fixtures::test_user("ratings-average-other")?,
            )?;
            let library = insert_library(&mut db, "Averages", "/tmp/lyra-ratings-average-route")?;
            let rated = insert_track(&mut db, "Rated Track")?;
            let unrated = insert_track(&mut db, "Unrated Track")?;
            connect(&mut db, library, rated)?;
            connect(&mut db, library, unrated)?;
            for (user, value) in [(user_db_id, 2), (other_user_db_id, 5)] {
                lyra_db::ratings::upsert(
                    &mut *db,
                    user,
                    rated,
                    lyra_db::ratings::RatingValue::new(value).unwrap(),
                    100,
                )?;
            }
            (
                user_db_id,
                lyra_db::lookup::find_id_by_db_id(&db, rated)?.unwrap(),
                lyra_db::lookup::find_id_by_db_id(&db, unrated)?.unwrap(),
            )
        };
        let session =
            crate::services::testing::create_session(user_db_id, Default::default()).await?;
        let authorization = format!("Bearer {}", session.token);
        let routes = rating_routes();
        let request = |target_ids: serde_json::Value| -> anyhow::Result<Request<Body>> {
            Ok(Request::builder()
                .method("POST")
                .uri("/averages")
                .header(CONTENT_TYPE, "application/json")
                .header(AUTHORIZATION, &authorization)
                .body(Body::from(serde_json::to_vec(
                    &serde_json::json!({ "target_ids": target_ids }),
                )?))?)
        };
        let targets = serde_json::json!([
            rated_id.clone(),
            rated_id.clone(),
            unrated_id.clone(),
            "missing-id"
        ]);

        let response = routes.clone().oneshot(request(targets.clone())?).await?;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body)?,
            serde_json::json!({
                "averages": {
                    rated_id.clone(): 3.5,
                    unrated_id.clone(): null,
                    "missing-id": null,
                },
            }),
        );

        let over_cap = routes
            .clone()
            .oneshot(request(serde_json::json!(vec![
                rated_id.clone();
                CHECK_TARGET_CAP + 1
            ]))?)
            .await?;
        assert_eq!(over_cap.status(), StatusCode::BAD_REQUEST);

        let unauthenticated = routes
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/averages")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"target_ids":[]}"#))?,
            )
            .await?;
        assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);

        let mut config = STATE.config().as_ref().clone();
        config.instance_stats = false;
        crate::services::testing::publish_config(config);
        let response = routes.oneshot(request(targets)?).await?;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body)?,
            serde_json::json!({
                "averages": {
                    rated_id: null,
                    unrated_id: null,
                    "missing-id": null,
                },
            }),
        );
        Ok(())
    }
}
