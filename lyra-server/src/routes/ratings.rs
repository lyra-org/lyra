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
        put,
    },
};
use serde::{
    Deserialize,
    Serialize,
};

use crate::{
    STATE,
    db::ratings::{
        MAX_VALUE,
        MIN_VALUE,
        RatingValue,
    },
    services::{
        auth::require_authenticated,
        ratings as rating_service,
    },
};

use super::AppError;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Default, Deserialize)]
pub(crate) struct RatingFilterQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Optional inclusive minimum personal rating. Must be an integer from 1 through 5. Unrated entities are excluded."
        )
    )]
    min_rating: Option<i64>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Optional inclusive maximum personal rating. Must be an integer from 1 through 5. Unrated entities are excluded."
        )
    )]
    max_rating: Option<i64>,
}

impl RatingFilterQuery {
    #[cfg(test)]
    pub(crate) fn from_bounds(min_rating: Option<i64>, max_rating: Option<i64>) -> Self {
        Self {
            min_rating,
            max_rating,
        }
    }

    pub(crate) fn parse(self) -> Result<crate::db::ratings::RatingFilter, AppError> {
        let min = self
            .min_rating
            .map(|value| parse_rating_bound("min_rating", value))
            .transpose()?;
        let max = self
            .max_rating
            .map(|value| parse_rating_bound("max_rating", value))
            .transpose()?;
        crate::db::ratings::RatingFilter::new(min, max)
            .map_err(|_| AppError::bad_request("min_rating must not exceed max_rating"))
    }
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

async fn set_rating(
    headers: HeaderMap,
    Path(target_id): Path<String>,
    Json(request): Json<SetRatingRequest>,
) -> Result<StatusCode, AppError> {
    let principal = require_authenticated(&headers).await?;
    validate_target_id(&target_id)?;
    let rating = parse_rating(request.rating)?;

    let mut db = STATE.db.write().await;
    match rating_service::set_for_principal(&mut db, &principal, &target_id, rating)? {
        rating_service::MutationOutcome::Applied(_) => Ok(StatusCode::NO_CONTENT),
        rating_service::MutationOutcome::NotTargetable => Err(AppError::not_found(format!(
            "rating target not found: {target_id}"
        ))),
    }
}

async fn get_rating(
    headers: HeaderMap,
    Path(target_id): Path<String>,
) -> Result<Json<RatingStateResponse>, AppError> {
    let principal = require_authenticated(&headers).await?;
    validate_target_id(&target_id)?;

    let db = STATE.db.read().await;
    let rating =
        rating_service::get_for_principal(&db, &principal, &target_id)?.map(RatingValue::get);
    Ok(Json(RatingStateResponse { rating }))
}

async fn delete_rating(
    headers: HeaderMap,
    Path(target_id): Path<String>,
) -> Result<StatusCode, AppError> {
    let principal = require_authenticated(&headers).await?;
    validate_target_id(&target_id)?;

    let mut db = STATE.db.write().await;
    match rating_service::remove(&mut db, principal.user_db_id, &target_id)? {
        rating_service::MutationOutcome::Applied(_) => Ok(StatusCode::NO_CONTENT),
        rating_service::MutationOutcome::NotTargetable => Err(AppError::not_found(format!(
            "rating target not found: {target_id}"
        ))),
    }
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

fn validate_target_id(target_id: &str) -> Result<(), AppError> {
    let valid_len = (6..=64).contains(&target_id.len());
    let valid_chars = target_id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
    if valid_len && valid_chars {
        return Ok(());
    }
    Err(AppError::bad_request(format!(
        "malformed target id: {target_id}"
    )))
}

#[cfg(feature = "docgen")]
fn set_rating_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Set a personal rating")
        .description(
            "Creates or replaces the authenticated user's 1-5 rating for a visible track, \
             release, or artist. Returns 204 on success, 400 for malformed IDs or ratings, \
             and 404 for missing, unsupported, or non-visible targets.",
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
             targets and bypasses the visibility gate so ratings cannot become undeletable after \
             library access changes.",
        )
        .response::<204, ()>()
}

pub fn rating_routes() -> Router {
    Router::new()
        .route("/{target_id}", put(set_rating))
        .route("/{target_id}", get(get_rating))
        .route("/{target_id}", delete(delete_rating))
}

#[cfg(feature = "docgen")]
pub(crate) fn rating_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::{
        delete_with,
        get_with,
        put_with,
    };

    aide::axum::ApiRouter::new()
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
        assert!(RatingFilterQuery::default().parse().unwrap().is_empty());
        assert_eq!(
            RatingFilterQuery {
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
    fn rating_filter_rejects_invalid_bounds() {
        assert!(
            RatingFilterQuery {
                min_rating: Some(0),
                max_rating: None,
            }
            .parse()
            .is_err()
        );
        assert!(
            RatingFilterQuery {
                min_rating: None,
                max_rating: Some(6),
            }
            .parse()
            .is_err()
        );
        assert!(
            RatingFilterQuery {
                min_rating: Some(4),
                max_rating: Some(3),
            }
            .parse()
            .is_err()
        );
    }

    #[test]
    fn target_id_validation_matches_public_id_shape() {
        assert!(validate_target_id("abc123").is_ok());
        assert!(validate_target_id("V1StGXR8_Z5jdHi6B-myT").is_ok());
        assert!(validate_target_id("short").is_err());
        assert!(validate_target_id("has/slash").is_err());
    }
}
