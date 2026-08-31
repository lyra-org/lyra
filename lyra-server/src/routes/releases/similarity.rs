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
    http::HeaderMap,
};
use serde::Deserialize;

use crate::{
    STATE,
    db::{
        self,
        Permission,
    },
    routes::{
        AppError,
        deserialize_inc,
        responses::ReleaseResponse,
    },
    services::{
        auth::require_authenticated,
        releases,
    },
};

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
pub(super) struct SimilarReleasesQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Maximum number of releases to return (default 20, maximum 100).")
    )]
    #[serde(
        default,
        deserialize_with = "crate::routes::deserialize_optional_usize"
    )]
    pub(super) limit: Option<usize>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Comma-separated or repeated values: artists, tracks, track_artists, entries, covers, artist_covers, genres."
        )
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    pub(super) inc: Option<Vec<String>>,
}

pub(super) async fn get_similar_releases(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<SimilarReleasesQuery>,
) -> Result<Json<Vec<ReleaseResponse>>, AppError> {
    let principal = require_authenticated(&headers).await?;
    let limit = query
        .limit
        .unwrap_or(releases::DEFAULT_SIMILAR_RELEASE_LIMIT);
    if limit == 0 {
        return Err(AppError::bad_request("limit must be > 0".to_string()));
    }
    if limit > releases::MAX_SIMILAR_RELEASE_LIMIT {
        return Err(AppError::bad_request(format!(
            "limit must be <= {}, got {limit}",
            releases::MAX_SIMILAR_RELEASE_LIMIT
        )));
    }
    let (includes, include_covers, include_genres, include_artist_covers) =
        super::parse_release_includes(query.inc)?;
    let include_entry_paths =
        db::roles::has_permission(&principal.permissions, Permission::ManageLibraries);

    let seed_db_id = {
        let db = STATE.db.read().await;
        let seed_db_id = db::lookup::find_node_id_by_id(&*db, &id)?
            .ok_or_else(|| AppError::not_found(format!("Release not found: {id}")))?;
        crate::services::auth::access::require_entity_accessible(
            &*db,
            &principal,
            seed_db_id,
            || AppError::not_found(format!("Release not found: {id}")),
        )?;
        if db::releases::get_by_id(&*db, seed_db_id)?.is_none() {
            return Err(AppError::not_found(format!("Release not found: {id}")));
        }
        seed_db_id
    };
    let options = releases::SimilarReleaseOptions {
        limit,
        accessible_library_ids: (!principal.permissions.contains(&Permission::Admin))
            .then(|| principal.accessible_library_ids.clone()),
    };
    let found = releases::similar(seed_db_id, &options)
        .await?
        .ok_or_else(|| AppError::not_found(format!("Release not found: {id}")))?;

    let db = STATE.db.read().await;
    if db::lookup::find_node_id_by_id(&*db, &id)? != Some(seed_db_id)
        || db::releases::get_by_id(&*db, seed_db_id)?.is_none()
        || !crate::services::auth::access::entity_accessible(&*db, &principal, seed_db_id)?
    {
        return Err(AppError::not_found(format!("Release not found: {id}")));
    }
    let mut accessible = Vec::with_capacity(found.len());
    for release in found {
        let Some(release_db_id) = release.db_id.clone().map(agdb::DbId::from) else {
            continue;
        };
        if db::lookup::find_node_id_by_id(&*db, &release.id)? == Some(release_db_id)
            && crate::services::auth::access::entity_accessible(&*db, &principal, release_db_id)?
            && let Some(current) = db::releases::get_by_id(&*db, release_db_id)?
        {
            accessible.push(current);
        }
    }
    let details = releases::list_details_for_releases(&db, includes, accessible)?;
    let responses = details
        .into_iter()
        .map(|detail| {
            super::detail_to_release_response(
                &db,
                detail,
                include_covers,
                include_artist_covers,
                include_genres,
                include_entry_paths,
            )
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(Json(responses))
}

#[cfg(feature = "docgen")]
pub(super) fn get_similar_releases_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get similar releases").description(
        "Returns an ordered JSON array of ordinary releases selected by enabled metadata providers. Providers are consulted in configured priority order; their external release references are resolved against the local library, access-filtered, deduplicated, and capped by `limit`. Returns 404 when the seed release is missing or inaccessible, and `[]` when no local matches are available. `limit` defaults to 20 and is capped at 100. Supported `inc` values: `artists`, `tracks`, `track_artists`, `entries`, `covers`, `artist_covers`, and `genres`.",
    )
}
