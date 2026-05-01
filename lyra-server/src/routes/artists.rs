// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use aide::axum::{
    ApiRouter,
    routing::{
        get_with,
        patch_with,
        post_with,
    },
};
use aide::transform::TransformOperation;
use axum::{
    Json,
    extract::{
        Path,
        Query,
    },
    http::HeaderMap,
};
use schemars::JsonSchema;
use serde::{
    Deserialize,
    Serialize,
};

use crate::{
    STATE,
    db::{
        self,
        ListOptions,
    },
    routes::AppError,
    routes::{
        covers as route_covers,
        deserialize_inc,
        responses::{
            ArtistRelationResponse,
            ArtistResponse,
            RelatedArtistResponse,
            RelationDirectionResponse,
            ReleaseResponse,
        },
    },
    services::{
        artists as artist_service,
        auth::require_authenticated,
        covers,
    },
};

#[derive(Deserialize, JsonSchema)]
struct ArtistQuery {
    #[schemars(description = "Comma-separated or repeated values: releases, tracks, relations.")]
    #[serde(default, deserialize_with = "deserialize_inc")]
    inc: Option<Vec<String>>,
}

#[derive(Deserialize, JsonSchema)]
struct ArtistListQuery {
    #[schemars(description = "Comma-separated or repeated values: releases, tracks, relations.")]
    #[serde(default, deserialize_with = "deserialize_inc")]
    inc: Option<Vec<String>>,
    #[schemars(description = "Optional fuzzy text query matched against artist names.")]
    query: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct ArtistUpdateRequest {
    #[schemars(description = "Updated artist name.")]
    name: Option<String>,
    #[schemars(description = "Updated sort name; set to null to clear.")]
    sort_name: Option<Option<String>>,
    #[schemars(description = "Updated description; set to null to clear.")]
    description: Option<Option<String>>,
}

#[derive(Serialize, JsonSchema)]
#[non_exhaustive]
pub struct ArtistCoverSearchResponse {
    pub artist_id: String,
    pub results: Vec<route_covers::ProviderCoverSearchResponse>,
}

fn parse_inc(inc: Option<Vec<String>>) -> Result<artist_service::ArtistIncludes, AppError> {
    let values = super::parse_inc_values(inc, &["releases", "tracks", "relations"])?;
    let mut result = artist_service::ArtistIncludes {
        releases: false,
        tracks: false,
        relations: false,
    };
    for value in values {
        match value.as_str() {
            "releases" => result.releases = true,
            "tracks" => result.tracks = true,
            "relations" => result.relations = true,
            _ => {}
        }
    }
    Ok(result)
}

fn artist_detail_to_response(
    _db: &impl db::DbAccess,
    detail: artist_service::ArtistDetails,
) -> anyhow::Result<ArtistResponse> {
    let releases = detail
        .releases
        .map(|v| v.into_iter().map(ReleaseResponse::from).collect());
    let relations = detail.relations.map(|v| {
        v.into_iter()
            .map(|r| ArtistRelationResponse {
                relation_type: r.relation_type,
                attributes: r.attributes,
                direction: match r.direction {
                    artist_service::RelationDirection::Incoming => {
                        RelationDirectionResponse::Incoming
                    }
                    artist_service::RelationDirection::Outgoing => {
                        RelationDirectionResponse::Outgoing
                    }
                },
                artist: RelatedArtistResponse {
                    id: r.artist.id,
                    name: r.artist.artist_name,
                    artist_type: r.artist.artist_type,
                },
            })
            .collect()
    });
    Ok(ArtistResponse {
        id: detail.artist.id,
        name: detail.artist.artist_name,
        sort_name: detail.artist.sort_name,
        description: detail.artist.description,
        verified: detail.artist.verified,
        credit: None,
        releases,
        tracks: detail
            .tracks
            .map(|v| v.into_iter().map(Into::into).collect()),
        relations,
    })
}

pub(crate) async fn list_artist_responses(
    inc: Option<Vec<String>>,
    query: Option<String>,
) -> Result<Vec<ArtistResponse>, AppError> {
    let db = &*STATE.db.read().await;
    let includes = parse_inc(inc)?;
    let options = ListOptions {
        sort: Vec::new(),
        offset: None,
        limit: None,
        search_term: super::parse_text_query(query),
    };
    let details = artist_service::list_details(db, includes, &options)?;

    details
        .into_iter()
        .map(|d| artist_detail_to_response(db, d))
        .collect::<anyhow::Result<Vec<_>>>()
        .map_err(AppError::from)
}

pub(crate) async fn get_artist_response(
    id: String,
    inc: Option<Vec<String>>,
) -> Result<ArtistResponse, AppError> {
    let db = &*STATE.db.read().await;
    let includes = parse_inc(inc)?;
    let artist_db_id = db::lookup::find_node_id_by_id(db, &id)?
        .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
    let detail = artist_service::get_details(db, artist_db_id, includes)?
        .ok_or_else(|| AppError::not_found(format!("Artist not found: {}", id)))?;

    Ok(artist_detail_to_response(db, detail)?)
}

async fn get_artists(
    headers: HeaderMap,
    Query(list_query): Query<ArtistListQuery>,
) -> Result<Json<Vec<ArtistResponse>>, AppError> {
    let _principal = require_authenticated(&headers).await?;
    Ok(Json(
        list_artist_responses(list_query.inc, list_query.query).await?,
    ))
}

async fn get_artist(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<ArtistQuery>,
) -> Result<Json<ArtistResponse>, AppError> {
    let _principal = require_authenticated(&headers).await?;
    Ok(Json(get_artist_response(id, query.inc).await?))
}

async fn get_artist_cover(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<route_covers::CoverQuery>,
) -> Result<axum::http::Response<axum::body::Body>, AppError> {
    let _principal = require_authenticated(&headers).await?;

    let transform_options = route_covers::parse_cover_transform_options(&query)?;
    let covers_root = covers::configured_covers_root();
    let (artist_db_id, mut cover, needs_metadata_upsert) = {
        let db = STATE.db.read().await;
        let artist_db_id = db::lookup::find_node_id_by_id(&*db, &id)?
            .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
        if db::artists::get_by_id(&db, artist_db_id)?.is_none() {
            return Err(AppError::not_found(format!("Artist not found: {}", id)));
        }

        let cover_paths = covers::CoverPaths {
            library_root: None,
            covers_root: covers_root.as_deref(),
        };

        let resolved = covers::resolve_cover_for_artist_id(&db, artist_db_id, cover_paths)?;
        let Some(cover) = resolved else {
            return Err(AppError::not_found(format!(
                "Cover not found for artist: {}",
                id
            )));
        };

        let db_cover = db::covers::get(&db, artist_db_id)?;
        let resolved_path = cover.to_string_lossy().into_owned();
        let needs_upsert = db_cover.is_none_or(|stored| stored.path != resolved_path);

        (artist_db_id, cover, needs_upsert)
    };

    if needs_metadata_upsert {
        let cover_paths = covers::CoverPaths {
            library_root: None,
            covers_root: covers_root.as_deref(),
        };
        match {
            let db = STATE.db.read().await;
            covers::resolve_cover_for_artist_id(&db, artist_db_id, cover_paths)
        } {
            Ok(Some(latest_cover)) => {
                cover = latest_cover;
            }
            Ok(None) => {}
            Err(err) => {
                tracing::warn!(
                    artist_id = artist_db_id.0,
                    error = %err,
                    "failed to re-resolve artist cover before metadata upsert"
                );
            }
        }
        if let Err(err) =
            covers::upsert_artist_cover_metadata(&STATE.db.get(), artist_db_id, &cover).await
        {
            tracing::warn!(
                artist_id = artist_db_id.0,
                cover_path = %cover.display(),
                error = %err,
                "failed to persist cover metadata while serving artist cover"
            );
        }
    }

    route_covers::serve_cover_response(&cover, transform_options, &headers).await
}

async fn search_artist_covers(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(query): Json<route_covers::CoverSearchQuery>,
) -> Result<Json<ArtistCoverSearchResponse>, AppError> {
    let _principal = require_authenticated(&headers).await?;

    let artist_db_id = {
        let db = STATE.db.read().await;
        let artist_db_id = db::lookup::find_node_id_by_id(&*db, &id)?
            .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
        if db::artists::get_by_id(&db, artist_db_id)?.is_none() {
            return Err(AppError::not_found(format!("Artist not found: {}", id)));
        }
        artist_db_id
    };

    let provider_filter = query.provider.as_deref();
    let found =
        covers::search_artist_cover_candidates(artist_db_id, provider_filter, query.force_refresh)
            .await?;
    let results = route_covers::map_provider_cover_search_results(found);

    Ok(Json(ArtistCoverSearchResponse {
        artist_id: id,
        results,
    }))
}

async fn update_artist(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(update): Json<ArtistUpdateRequest>,
) -> Result<Json<ArtistResponse>, AppError> {
    let _principal = require_authenticated(&headers).await?;

    if update.name.is_none() && update.sort_name.is_none() && update.description.is_none() {
        return Err(AppError::bad_request("no artist fields provided"));
    }

    let ArtistUpdateRequest {
        name: update_name,
        sort_name: update_sort_name,
        description: update_description,
    } = update;

    let mut db = STATE.db.write().await;
    let artist_db_id = db::lookup::find_node_id_by_id(&*db, &id)?
        .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
    if let Some(name) = update_name.as_ref()
        && name.trim().is_empty()
    {
        return Err(AppError::bad_request("name cannot be empty"));
    }

    if let Some(Some(sort_name)) = update_sort_name.as_ref()
        && sort_name.trim().is_empty()
    {
        return Err(AppError::bad_request("sort_name cannot be empty"));
    }

    if let Some(Some(description)) = update_description.as_ref()
        && description.trim().is_empty()
    {
        return Err(AppError::bad_request("description cannot be empty"));
    }

    let updated = artist_service::update(
        &mut db,
        artist_db_id,
        update_name,
        update_sort_name,
        update_description,
    )?
    .ok_or_else(|| AppError::not_found(format!("Artist not found: {}", id)))?;

    Ok(Json(ArtistResponse {
        id: updated.id,
        name: updated.artist_name,
        sort_name: updated.sort_name,
        description: updated.description,
        verified: updated.verified,
        credit: None,
        releases: None,
        tracks: None,
        relations: None,
    }))
}

fn list_artists_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List artists").description(
        "Returns artists. Supported query parameters: `inc`, `query`. `query` is a fuzzy text match against artist names. Use `inc` to include releases, tracks, and/or relations. The `credit` field is not present on artist-level responses; it only appears when artists are included via track or release endpoints.",
    )
}

fn get_artist_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get artist by ID").description(
        "Returns a single artist. 404 if not found. Use `inc` to include releases, tracks, and/or relations. The `credit` field is not present on artist-level responses; it only appears when artists are included via track or release endpoints.",
    )
}

fn get_artist_cover_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get artist cover").description(
        "Returns the artist cover image for an artist. Supports optional transform parameters: `format`, `quality`, `max_width`, and `max_height`.",
    )
}

fn search_artist_covers_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Search artist cover candidates").description(
        "Returns provider cover candidates for an artist. Request body (JSON): `{ provider?, force_refresh? }`; \
        `force_refresh=true` bypasses cached provider cover resolution. Providers may return \
        width, height, and selected_index for automatic selection.",
    )
}

fn update_artist_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Update artist")
        .description("Updates artist name, sort name, and description. Set sort_name or description to null to clear.")
}

pub fn artist_routes() -> ApiRouter {
    ApiRouter::new()
        .api_route("/", get_with(get_artists, list_artists_docs))
        .api_route("/{id}", get_with(get_artist, get_artist_docs))
        .api_route(
            "/{id}/cover",
            get_with(get_artist_cover, get_artist_cover_docs),
        )
        .api_route(
            "/{id}/covers/search",
            post_with(search_artist_covers, search_artist_covers_docs),
        )
        .api_route("/{id}", patch_with(update_artist, update_artist_docs))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::test_db::insert_artist,
        services::auth::sessions,
        testing::{
            LibraryFixtureConfig,
            initialize_runtime,
            runtime_test_lock,
        },
    };
    use axum::{
        Json,
        extract::Path,
        http::{
            HeaderMap,
            header::AUTHORIZATION,
        },
    };
    use std::{
        path::PathBuf,
        time::{
            SystemTime,
            UNIX_EPOCH,
        },
    };

    struct TestDirGuard(PathBuf);

    impl Drop for TestDirGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    async fn initialize_test_runtime() -> anyhow::Result<TestDirGuard> {
        let test_dir = std::env::temp_dir().join(format!(
            "lyra-artist-routes-test-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        std::fs::create_dir_all(&test_dir)?;
        initialize_runtime(&LibraryFixtureConfig {
            directory: test_dir.clone(),
            language: None,
            country: None,
        })
        .await?;
        Ok(TestDirGuard(test_dir))
    }

    async fn create_authenticated_headers(username: &str) -> anyhow::Result<HeaderMap> {
        let user_db_id = {
            let mut db = STATE.db.write().await;
            db::users::create(&mut db, &db::users::test_user(username)?)?
        };

        let session = sessions::create_session_for_user(user_db_id).await?;
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            format!("Bearer {}", session.token)
                .parse()
                .expect("valid auth header"),
        );
        Ok(headers)
    }

    #[tokio::test]
    async fn search_artist_covers_returns_empty_results_without_providers() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let _test_dir = initialize_test_runtime().await?;

        let artist_id = {
            let mut db = STATE.db.write().await;
            let artist_db_id = insert_artist(&mut db, "Coverless Artist")?;
            db::artists::get_by_id(&db, artist_db_id)?
                .expect("artist should exist")
                .id
        };

        let headers = create_authenticated_headers("artist-cover-tester").await?;
        let Json(response) = search_artist_covers(
            headers,
            Path(artist_id.clone()),
            Json(route_covers::CoverSearchQuery::default()),
        )
        .await
        .map_err(|err| anyhow::anyhow!("search_artist_covers failed: {err:?}"))?;

        assert_eq!(response.artist_id, artist_id);
        assert!(response.results.is_empty());
        Ok(())
    }
}
