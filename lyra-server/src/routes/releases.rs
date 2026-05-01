// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbAny,
    DbId,
};
use aide::axum::{
    ApiRouter,
    routing::{
        get_with,
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
        Permission,
        SortKey,
        SortSpec,
        SortSpecParseError,
        parse_sort_direction,
        parse_sort_specs_tokens,
    },
    routes::AppError,
    routes::{
        covers as route_covers,
        deserialize_inc,
        responses::{
            EntryResponse,
            ReleaseCoverResponse,
            ReleaseResponse,
            TrackResponse,
        },
    },
    services::{
        auth::require_authenticated,
        covers,
        releases,
    },
};

#[derive(Serialize, JsonSchema)]
#[non_exhaustive]
pub struct ReleaseCoverSearchResponse {
    pub release_id: String,
    pub results: Vec<route_covers::ProviderCoverSearchResponse>,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct ReleaseListQuery {
    #[schemars(
        description = "Comma-separated or repeated values: artists, tracks, track_artists, entries, covers, genres."
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    pub(crate) inc: Option<Vec<String>>,
    #[schemars(description = "Optional text query matched against release titles.")]
    pub(crate) query: Option<String>,
    #[schemars(description = "Optional exact release year filter derived from `release_date`.")]
    pub(crate) year: Option<u32>,
    #[schemars(description = "Comma-separated or repeated genre values.")]
    #[serde(default, deserialize_with = "deserialize_inc")]
    pub(crate) genre: Option<Vec<String>>,
    #[schemars(
        description = "Comma-separated or repeated values: sortname, name, datecreated, releasedate, id."
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    pub(crate) sort_by: Option<Vec<String>>,
    #[schemars(description = "Sort order for all sort keys: ascending or descending.")]
    pub(crate) sort_order: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct ReleaseQuery {
    #[schemars(
        description = "Comma-separated or repeated values: artists, tracks, entries, covers, genres."
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    pub(crate) inc: Option<Vec<String>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ReleaseInc {
    pub(crate) artists: bool,
    pub(crate) tracks: bool,
    pub(crate) track_artists: bool,
    pub(crate) entries: bool,
    pub(crate) covers: bool,
    pub(crate) genres: bool,
}

pub(crate) fn parse_inc(inc: Option<Vec<String>>) -> Result<ReleaseInc, AppError> {
    let values = super::parse_inc_values(
        inc,
        &[
            "artists",
            "tracks",
            "track_artists",
            "entries",
            "covers",
            "genres",
        ],
    )?;
    let mut result = ReleaseInc {
        artists: false,
        tracks: false,
        track_artists: false,
        entries: false,
        covers: false,
        genres: false,
    };
    for value in values {
        match value.as_str() {
            "artists" => result.artists = true,
            "tracks" => result.tracks = true,
            "track_artists" => result.track_artists = true,
            "entries" => result.entries = true,
            "covers" => result.covers = true,
            "genres" => result.genres = true,
            _ => {}
        }
    }
    Ok(result)
}

pub(crate) fn parse_release_includes(
    inc: Option<Vec<String>>,
) -> Result<(releases::ReleaseIncludes, bool, bool), AppError> {
    let parsed = parse_inc(inc)?;
    let includes = releases::ReleaseIncludes {
        artists: parsed.artists,
        tracks: parsed.tracks,
        track_artists: parsed.track_artists,
        entries: parsed.entries,
    };

    Ok((includes, parsed.covers, parsed.genres))
}

fn is_supported_release_sort_key(key: SortKey) -> bool {
    matches!(
        key,
        SortKey::SortName
            | SortKey::Name
            | SortKey::DateCreated
            | SortKey::ReleaseDate
            | SortKey::DbId
    )
}

fn parse_sort_specs(
    sort_by: Option<Vec<String>>,
    sort_order: Option<String>,
) -> Result<Vec<SortSpec>, AppError> {
    let direction = parse_sort_direction(sort_order, true).map_err(|err| match err {
        SortSpecParseError::UnsupportedSortOrder(raw) => AppError::bad_request(format!(
            "Unsupported sort_order value: {}. Supported values: ascending, descending",
            raw
        )),
        other => AppError::bad_request(other.to_string()),
    })?;
    parse_sort_specs_tokens(sort_by, direction, is_supported_release_sort_key, true).map_err(
        |err| match err {
            SortSpecParseError::UnsupportedSortByValues(values) => {
                AppError::bad_request(format!(
                    "Unsupported sort_by value(s): {}. Supported values: sortname, name, datecreated, releasedate, id",
                    values.join(", ")
                ))
            }
            other => AppError::bad_request(other.to_string()),
        },
    )
}

fn parse_genre_filter(genre: Option<Vec<String>>) -> Vec<String> {
    let mut values = Vec::new();
    if let Some(entries) = genre {
        for entry in entries {
            for token in entry.split(',') {
                let token = token.trim();
                if token.is_empty() {
                    continue;
                }
                values.push(token.to_string());
            }
        }
    }
    values
}

fn parse_text_query(query: Option<String>) -> Option<String> {
    query.and_then(|value| {
        let value = value.trim();
        if value.is_empty() {
            None
        } else {
            Some(value.to_string())
        }
    })
}

pub(crate) fn build_cover_response(
    db: &DbAny,
    release_db_id: DbId,
    include_covers: bool,
) -> anyhow::Result<Option<Option<ReleaseCoverResponse>>> {
    if !include_covers {
        return Ok(None);
    }

    let cover = db::covers::get(db, release_db_id)?.map(|cover| ReleaseCoverResponse {
        mime_type: cover.mime_type,
        hash: cover.hash,
        blurhash: cover.blurhash,
    });

    Ok(Some(cover))
}

pub(crate) fn detail_to_release_response(
    db: &DbAny,
    detail: releases::ReleaseDetails,
    include_covers: bool,
    include_genres: bool,
    include_entry_paths: bool,
) -> anyhow::Result<ReleaseResponse> {
    let entries = detail.entries.map(|entries| {
        entries
            .into_iter()
            .map(|entry| EntryResponse::from_entry(entry, include_entry_paths))
            .collect::<Vec<EntryResponse>>()
    });

    let cover = build_cover_response(db, detail.release_db_id, include_covers)?;
    let genres = if include_genres {
        db::genres::get_names_for_release(db, detail.release_db_id)?
    } else {
        None
    };

    Ok(ReleaseResponse {
        id: detail.release.id,
        title: detail.release.release_title,
        sort_title: detail.release.sort_title,
        release_date: detail.release.release_date,
        genres,
        cover,
        artists: detail
            .artists
            .map(|v| v.into_iter().map(Into::into).collect()),
        tracks: detail.tracks.map(|tracks| {
            tracks
                .into_iter()
                .map(|track| {
                    let artists = detail.track_artists.as_ref().and_then(|m| {
                        let db_id = track.db_id.clone().map(DbId::from)?;
                        Some(m.get(&db_id)?.iter().cloned().map(Into::into).collect())
                    });
                    let mut resp = TrackResponse::from(track);
                    resp.artists = artists;
                    resp
                })
                .collect()
        }),
        entries,
    })
}

async fn get_releases(
    headers: HeaderMap,
    Query(list_query): Query<ReleaseListQuery>,
) -> Result<Json<Vec<ReleaseResponse>>, AppError> {
    let principal = require_authenticated(&headers).await?;
    let include_entry_paths =
        db::roles::has_permission(&principal.permissions, Permission::ManageLibraries);

    let db = &*STATE.db.read().await;
    let (includes, include_covers, include_genres) = parse_release_includes(list_query.inc)?;
    let list_options = ListOptions {
        sort: parse_sort_specs(list_query.sort_by, list_query.sort_order)?,
        offset: None,
        limit: None,
        search_term: parse_text_query(list_query.query),
    };
    let filters = releases::ReleaseListFilters {
        year: list_query.year,
        genres: parse_genre_filter(list_query.genre),
    };
    let details = releases::list_details_with_options(db, includes, list_options, filters)?;

    let mut response: Vec<ReleaseResponse> = Vec::with_capacity(details.len());
    for detail in details {
        response.push(detail_to_release_response(
            db,
            detail,
            include_covers,
            include_genres,
            include_entry_paths,
        )?);
    }

    Ok(Json(response))
}

async fn get_release(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<ReleaseQuery>,
) -> Result<Json<ReleaseResponse>, AppError> {
    let principal = require_authenticated(&headers).await?;
    let include_entry_paths =
        db::roles::has_permission(&principal.permissions, Permission::ManageLibraries);

    let db = &*STATE.db.read().await;
    let (includes, include_covers, include_genres) = parse_release_includes(query.inc)?;
    let release_db_id = db::lookup::find_node_id_by_id(db, &id)?
        .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
    let detail = releases::get_details(db, release_db_id, includes)?
        .ok_or_else(|| AppError::not_found(format!("Release not found: {}", id)))?;

    Ok(Json(detail_to_release_response(
        db,
        detail,
        include_covers,
        include_genres,
        include_entry_paths,
    )?))
}

async fn get_release_cover(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<route_covers::CoverQuery>,
) -> Result<axum::http::Response<axum::body::Body>, AppError> {
    let _principal = require_authenticated(&headers).await?;

    let transform_options = route_covers::parse_cover_transform_options(&query)?;
    let covers_root = covers::configured_covers_root();
    let (release_db_id, library_root, mut cover, needs_metadata_upsert) = {
        let db = STATE.db.read().await;
        let release_db_id = db::lookup::find_node_id_by_id(&*db, &id)?
            .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
        let library_root = db::libraries::get_by_release(&db, release_db_id)?
            .into_iter()
            .next()
            .map(|library| library.path);
        let cover_paths = covers::CoverPaths {
            library_root: library_root.as_deref(),
            covers_root: covers_root.as_deref(),
        };

        let resolved = covers::resolve_cover_for_release_id(&db, release_db_id, cover_paths)?;
        let Some(cover) = resolved else {
            return Err(AppError::not_found(format!(
                "Cover not found for release: {}",
                id
            )));
        };

        let db_cover = db::covers::get(&db, release_db_id)?;
        let resolved_path = cover.to_string_lossy().into_owned();
        let needs_upsert = db_cover.is_none_or(|stored| stored.path != resolved_path);

        (release_db_id, library_root, cover, needs_upsert)
    };

    if needs_metadata_upsert {
        let cover_paths = covers::CoverPaths {
            library_root: library_root.as_deref(),
            covers_root: covers_root.as_deref(),
        };
        match {
            let db = STATE.db.read().await;
            covers::resolve_cover_for_release_id(&db, release_db_id, cover_paths)
        } {
            Ok(Some(latest_cover)) => {
                cover = latest_cover;
            }
            Ok(None) => {}
            Err(err) => {
                tracing::warn!(
                    release_id = release_db_id.0,
                    error = %err,
                    "failed to re-resolve release cover before metadata upsert"
                );
            }
        }
        if let Err(err) =
            covers::upsert_release_cover_metadata(&STATE.db.get(), release_db_id, &cover).await
        {
            tracing::warn!(
                release_id = release_db_id.0,
                cover_path = %cover.display(),
                error = %err,
                "failed to persist cover metadata while serving release cover"
            );
        }
    }

    route_covers::serve_cover_response(&cover, transform_options, &headers).await
}

async fn search_release_covers(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(query): Json<route_covers::CoverSearchQuery>,
) -> Result<Json<ReleaseCoverSearchResponse>, AppError> {
    let _principal = require_authenticated(&headers).await?;

    let release_db_id = {
        let db = STATE.db.read().await;
        let release_db_id = db::lookup::find_node_id_by_id(&*db, &id)?
            .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
        if db::releases::get_by_id(&db, release_db_id)?.is_none() {
            return Err(AppError::not_found(format!("Release not found: {}", id)));
        }
        release_db_id
    };

    let provider_filter = query.provider.as_deref();
    let found = covers::search_release_cover_candidates(
        release_db_id,
        provider_filter,
        query.force_refresh,
    )
    .await?;
    let results = route_covers::map_provider_cover_search_results(found);

    Ok(Json(ReleaseCoverSearchResponse {
        release_id: id,
        results,
    }))
}

fn list_releases_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List releases").description(
        "Returns releases. Supported query parameters: `inc`, `query`, `year`, `genre`, `sort_by`, `sort_order`. Supported `inc` values: `artists`, `tracks`, `track_artists`, `entries`, `covers`, `genres`. When `inc=artists`, each artist carries a `credit` object with `type`, `detail`, and `source`. An artist may appear multiple times with different credits (for example, artist and producer). Track artists without direct credits inherit from the release (`source: release`). When `inc=entries`, `full_path` is included only for authenticated users with ManageLibraries permission.",
    )
}

fn get_release_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get release by ID").description(
        "Returns a single release. 404 if not found. Use `inc` to include artists, tracks, track_artists, entries, covers, and/or genres. When `inc=artists`, each artist carries a `credit` object with `type`, `detail`, and `source`. An artist may appear multiple times with different credits. Track artists without direct credits inherit from the release (`source: release`). When `inc=entries`, `full_path` is included only for authenticated users with ManageLibraries permission.",
    )
}

fn get_release_cover_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get release cover").description(
        "Returns the release cover image for a release. Supports optional transform parameters: `format`, `quality`, `max_width`, and `max_height`.",
    )
}

fn search_release_covers_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Search release cover candidates").description(
        "Returns provider cover candidates for a release. Request body (JSON): `{ provider?, force_refresh? }`; \
        `force_refresh=true` bypasses cached provider cover resolution. Providers may return \
        width, height, and selected_index for automatic selection.",
    )
}

pub fn release_routes() -> ApiRouter {
    ApiRouter::new()
        .api_route("/", get_with(get_releases, list_releases_docs))
        .api_route("/{id}", get_with(get_release, get_release_docs))
        .api_route(
            "/{id}/cover",
            get_with(get_release_cover, get_release_cover_docs),
        )
        .api_route(
            "/{id}/covers/search",
            post_with(search_release_covers, search_release_covers_docs),
        )
}

#[cfg(test)]
mod tests {
    use agdb::{
        DbAny,
        QueryBuilder,
    };
    use axum::{
        body::to_bytes,
        http::StatusCode,
        response::IntoResponse,
    };

    use crate::db::SortDirection;
    use crate::db::test_db::TestDb;

    use super::*;
    use nanoid::nanoid;

    fn new_test_db() -> anyhow::Result<DbAny> {
        Ok(TestDb::new()?.into_inner())
    }

    fn insert_release_node(db: &mut DbAny) -> anyhow::Result<DbId> {
        let result = db.exec_mut(QueryBuilder::insert().nodes().count(1).query())?;
        result
            .ids()
            .first()
            .copied()
            .ok_or_else(|| anyhow::anyhow!("release insert returned no id"))
    }

    fn insert_cover_for_release(db: &mut DbAny, release_db_id: DbId) -> anyhow::Result<()> {
        let cover = db::Cover {
            db_id: None,
            id: nanoid!(),
            path: "/music/release/cover.jpg".to_string(),
            mime_type: "image/jpeg".to_string(),
            hash: "a".repeat(64),
            blurhash: Some("LKO2?U%2Tw=w]~RBVZRi};RPxuwH".to_string()),
        };

        let result = db.exec_mut(QueryBuilder::insert().element(&cover).query())?;
        let cover_id = result
            .ids()
            .first()
            .copied()
            .ok_or_else(|| anyhow::anyhow!("cover insert returned no id"))?;

        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(release_db_id)
                .to(cover_id)
                .query(),
        )?;

        Ok(())
    }

    #[test]
    fn parse_inc_accepts_covers() {
        let parsed = match parse_inc(Some(vec!["artists,covers".to_string()])) {
            Ok(value) => value,
            Err(_) => panic!("covers inc should parse"),
        };
        assert!(parsed.artists);
        assert!(!parsed.tracks);
        assert!(!parsed.entries);
        assert!(parsed.covers);
    }

    #[tokio::test]
    async fn parse_inc_error_mentions_covers() -> anyhow::Result<()> {
        let err = parse_inc(Some(vec!["unknown".to_string()])).expect_err("expected parse error");
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let text = std::str::from_utf8(&body)?;
        assert!(
            text.contains(
                "Supported values: artists, tracks, track_artists, entries, covers, genres"
            )
        );
        Ok(())
    }

    #[test]
    fn parse_sort_specs_accepts_supported_values() -> anyhow::Result<()> {
        let specs = match parse_sort_specs(
            Some(vec!["sortname,name".to_string(), "releasedate".to_string()]),
            Some("descending".to_string()),
        ) {
            Ok(specs) => specs,
            Err(_) => return Err(anyhow::anyhow!("expected valid sort specs")),
        };
        assert_eq!(specs.len(), 3);
        assert!(matches!(specs[0].key, SortKey::SortName));
        assert!(matches!(specs[1].key, SortKey::Name));
        assert!(matches!(specs[2].key, SortKey::ReleaseDate));
        assert!(
            specs
                .iter()
                .all(|spec| matches!(spec.direction, SortDirection::Descending))
        );
        Ok(())
    }

    #[tokio::test]
    async fn parse_sort_specs_rejects_unsupported_values() -> anyhow::Result<()> {
        let err = parse_sort_specs(
            Some(vec!["duration,unknown".to_string()]),
            Some("ascending".to_string()),
        )
        .expect_err("expected sort parse error");
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let text = std::str::from_utf8(&body)?;
        assert!(text.contains("Supported values: sortname, name, datecreated, releasedate, id"));
        Ok(())
    }

    #[tokio::test]
    async fn parse_sort_specs_rejects_invalid_sort_order() -> anyhow::Result<()> {
        let err = parse_sort_specs(Some(vec!["name".to_string()]), Some("upward".to_string()))
            .expect_err("expected sort_order parse error");
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let text = std::str::from_utf8(&body)?;
        assert!(text.contains("Supported values: ascending, descending"));
        Ok(())
    }

    #[test]
    fn parse_text_query_trims_and_ignores_empty_values() {
        assert_eq!(
            parse_text_query(Some("  blue  ".to_string())),
            Some("blue".to_string())
        );
        assert!(parse_text_query(Some("   ".to_string())).is_none());
        assert!(parse_text_query(None).is_none());
    }

    #[test]
    fn parse_genre_filter_splits_and_trims_values() {
        let genres = parse_genre_filter(Some(vec![
            "rock, jazz".to_string(),
            "electronic".to_string(),
        ]));
        assert_eq!(genres, vec!["rock", "jazz", "electronic"]);
    }

    #[test]
    fn parse_cover_transform_options_accepts_common_values() -> anyhow::Result<()> {
        let query = route_covers::CoverQuery {
            format: Some("webp".to_string()),
            quality: Some(85),
            max_width: Some(640),
            max_height: Some(640),
        };

        let options = match route_covers::parse_cover_transform_options(&query) {
            Ok(options) => options,
            Err(_) => return Err(anyhow::anyhow!("expected valid transform options")),
        }
        .ok_or_else(|| anyhow::anyhow!("expected transform options"))?;
        assert_eq!(options.format, Some(image::ImageFormat::WebP));
        assert_eq!(options.quality, Some(85));
        assert_eq!(options.max_width, Some(640));
        assert_eq!(options.max_height, Some(640));
        Ok(())
    }

    #[test]
    fn parse_cover_transform_options_empty_is_none() -> anyhow::Result<()> {
        let query = route_covers::CoverQuery::default();
        let options = match route_covers::parse_cover_transform_options(&query) {
            Ok(options) => options,
            Err(_) => return Err(anyhow::anyhow!("expected empty transform options")),
        };
        assert!(options.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn parse_cover_transform_options_rejects_invalid_format() -> anyhow::Result<()> {
        let query = route_covers::CoverQuery {
            format: Some("gif".to_string()),
            quality: None,
            max_width: None,
            max_height: None,
        };

        let err = route_covers::parse_cover_transform_options(&query)
            .expect_err("expected invalid format error");
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let text = std::str::from_utf8(&body)?;
        assert!(text.contains("Supported formats: jpg, png, webp"));
        Ok(())
    }

    #[tokio::test]
    async fn parse_cover_transform_options_rejects_invalid_quality_and_bounds() -> anyhow::Result<()>
    {
        let query = route_covers::CoverQuery {
            format: Some("jpg".to_string()),
            quality: Some(101),
            max_width: Some(0),
            max_height: None,
        };

        let err = route_covers::parse_cover_transform_options(&query)
            .expect_err("expected validation error");
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let text = std::str::from_utf8(&body)?;
        assert!(
            text.contains("quality must be between 0 and 100")
                || text.contains("max_width must be greater than 0")
        );
        Ok(())
    }

    #[tokio::test]
    async fn parse_cover_transform_options_rejects_zero_bounds() -> anyhow::Result<()> {
        let query = route_covers::CoverQuery {
            format: None,
            quality: None,
            max_width: Some(0),
            max_height: Some(320),
        };

        let err =
            route_covers::parse_cover_transform_options(&query).expect_err("expected bounds error");
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let text = std::str::from_utf8(&body)?;
        assert!(text.contains("max_width must be greater than 0"));
        Ok(())
    }

    #[test]
    fn release_response_omits_cover_field_when_not_requested() -> anyhow::Result<()> {
        let response = ReleaseResponse {
            id: String::new(),
            title: "Test Release".to_string(),
            sort_title: None,
            artists: None,
            tracks: None,
            entries: None,
            release_date: None,
            genres: None,
            cover: None,
        };

        let value = serde_json::to_value(response)?;
        assert!(value.get("cover").is_none());
        Ok(())
    }

    #[test]
    fn release_response_serializes_cover_as_null() -> anyhow::Result<()> {
        let response = ReleaseResponse {
            id: String::new(),
            title: "Test Release".to_string(),
            sort_title: None,
            artists: None,
            tracks: None,
            entries: None,
            release_date: None,
            genres: None,
            cover: Some(None),
        };

        let value = serde_json::to_value(response)?;
        assert!(value.get("cover").is_some_and(serde_json::Value::is_null));
        Ok(())
    }

    #[test]
    fn release_response_serializes_cover_object() -> anyhow::Result<()> {
        let response = ReleaseResponse {
            id: String::new(),
            title: "Test Release".to_string(),
            sort_title: None,
            artists: None,
            tracks: None,
            entries: None,
            release_date: None,
            genres: None,
            cover: Some(Some(ReleaseCoverResponse {
                mime_type: "image/jpeg".to_string(),
                hash: "b".repeat(64),
                blurhash: Some("LKO2?U%2Tw=w]~RBVZRi};RPxuwH".to_string()),
            })),
        };

        let value = serde_json::to_value(response)?;
        let cover = value
            .get("cover")
            .and_then(serde_json::Value::as_object)
            .ok_or_else(|| anyhow::anyhow!("missing cover object"))?;
        assert_eq!(
            cover.get("mime_type"),
            Some(&serde_json::json!("image/jpeg"))
        );
        assert_eq!(cover.get("hash"), Some(&serde_json::json!("b".repeat(64))));
        Ok(())
    }

    #[test]
    fn build_cover_response_omits_when_not_requested() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_db_id = insert_release_node(&mut db)?;
        let cover = build_cover_response(&db, release_db_id, false)?;
        assert!(cover.is_none());
        Ok(())
    }

    #[test]
    fn build_cover_response_returns_null_when_missing() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_db_id = insert_release_node(&mut db)?;
        let cover = build_cover_response(&db, release_db_id, true)?;
        assert!(matches!(cover, Some(None)));
        Ok(())
    }

    #[test]
    fn build_cover_response_returns_cover_metadata() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_db_id = insert_release_node(&mut db)?;
        insert_cover_for_release(&mut db, release_db_id)?;

        let cover = build_cover_response(&db, release_db_id, true)?
            .flatten()
            .ok_or_else(|| anyhow::anyhow!("expected cover metadata"))?;

        assert_eq!(cover.mime_type, "image/jpeg");
        assert_eq!(cover.hash, "a".repeat(64));
        Ok(())
    }
}
