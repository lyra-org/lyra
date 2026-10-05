// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbAny,
    DbId,
};
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
use axum::{
    Router,
    routing::{
        get,
        post,
    },
};
use serde::{
    Deserialize,
    Serialize,
};

use crate::{
    STATE,
    db::{
        self,
        Permission,
    },
    routes::AppError,
    routes::{
        covers as route_covers,
        deserialize_inc,
        ratings::RatingFilterQuery,
        responses::{
            EntryResponse,
            PageResponse,
            ReleaseResponse,
            TrackResponse,
        },
    },
    services::{
        auth::require_authenticated,
        catalog::{
            self,
            pipeline::Catalog,
            releases::{
                ReleaseFilter,
                Releases,
            },
        },
        covers,
        pagination::SnapshotKey,
        releases,
    },
};

mod similarity;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[non_exhaustive]
pub struct ReleaseCoverSearchResponse {
    pub release_id: String,
    pub results: Vec<route_covers::ProviderCoverSearchResponse>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
pub(crate) struct ReleaseListQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Comma-separated or repeated values: artists, tracks, track_artists, entries, covers, artist_covers, genres."
        )
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    pub(crate) inc: Option<Vec<String>>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Optional text query matched against release titles.")
    )]
    pub(crate) query: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Optional exact release year filter derived from `release_date`.")
    )]
    pub(crate) year: Option<u32>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Optional public library ID to scope returned releases.")
    )]
    pub(crate) library_id: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Comma-separated or repeated public genre IDs.")
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    pub(crate) genre_id: Option<Vec<String>>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Comma-separated or repeated values: name, sort_name, date_created, release_date, year, total_duration, track_count, artist_name, listen_count, last_played_at, relevance (with query), random, id."
        )
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    pub(crate) sort_by: Option<Vec<String>>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Sort order for all sort keys: ascending or descending.")
    )]
    pub(crate) sort_order: Option<String>,
    #[serde(flatten)]
    pub(crate) rating: RatingFilterQuery,
    #[serde(flatten)]
    pub(crate) page: super::PageQuery,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
pub(crate) struct ReleaseQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Comma-separated or repeated values: artists, tracks, track_artists, entries, covers, artist_covers, genres."
        )
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
    pub(crate) artist_covers: bool,
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
            "artist_covers",
            "genres",
        ],
    )?;
    let mut result = ReleaseInc {
        artists: false,
        tracks: false,
        track_artists: false,
        entries: false,
        covers: false,
        artist_covers: false,
        genres: false,
    };
    for value in values {
        match value.as_str() {
            "artists" => result.artists = true,
            "tracks" => result.tracks = true,
            "track_artists" => result.track_artists = true,
            "entries" => result.entries = true,
            "covers" => result.covers = true,
            "artist_covers" => result.artist_covers = true,
            "genres" => result.genres = true,
            _ => {}
        }
    }
    Ok(result)
}

pub(crate) fn parse_release_includes(
    inc: Option<Vec<String>>,
) -> Result<(releases::ReleaseIncludes, bool, bool, bool), AppError> {
    let parsed = parse_inc(inc)?;
    let includes = releases::ReleaseIncludes {
        artists: parsed.artists,
        tracks: parsed.tracks,
        track_artists: parsed.track_artists,
        entries: parsed.entries,
    };

    Ok((includes, parsed.covers, parsed.genres, parsed.artist_covers))
}

fn parse_genre_id_filter(genre_id: Option<Vec<String>>) -> Vec<String> {
    let mut values = Vec::new();
    if let Some(entries) = genre_id {
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

fn resolve_genre_id_filter(
    db: &impl db::DbAccess,
    genre_ids: &[String],
) -> Result<Vec<DbId>, AppError> {
    let mut resolved = Vec::new();
    for genre_id in genre_ids {
        let genre_db_id = db::lookup::find_node_id_by_id(db, genre_id)?
            .ok_or_else(|| AppError::not_found(format!("Genre not found: {genre_id}")))?;
        db::genres::get_by_id(db, genre_db_id)?
            .ok_or_else(|| AppError::not_found(format!("Genre not found: {genre_id}")))?;
        resolved.push(genre_db_id);
    }
    Ok(resolved)
}

pub(crate) fn detail_to_release_response(
    db: &DbAny,
    detail: releases::ReleaseDetails,
    include_covers: bool,
    include_artist_covers: bool,
    include_genres: bool,
    include_entry_paths: bool,
) -> anyhow::Result<ReleaseResponse> {
    let artist_covers = if include_artist_covers {
        let mut artist_db_ids = Vec::new();
        if let Some(artists) = detail.artists.as_ref() {
            artist_db_ids.extend(super::db_ids_from_credited_artists(artists));
        }
        if let Some(track_artists) = detail.track_artists.as_ref() {
            for artists in track_artists.values() {
                artist_db_ids.extend(super::db_ids_from_credited_artists(artists));
            }
        }
        Some(db::covers::get_many(db, &artist_db_ids)?)
    } else {
        None
    };
    let entries = detail.entries.map(|entries| {
        entries
            .into_iter()
            .map(|entry| EntryResponse::from_entry(entry, include_entry_paths))
            .collect::<Vec<EntryResponse>>()
    });

    let cover = route_covers::build_cover_response(db, detail.release_db_id, include_covers)?;
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
            .map(|v| super::credited_artist_responses(v, artist_covers.as_ref())),
        tracks: detail.tracks.map(|tracks| {
            tracks
                .into_iter()
                .map(|track| {
                    let artists = detail.track_artists.as_ref().and_then(|m| {
                        let db_id = track.db_id.clone().map(DbId::from)?;
                        Some(super::credited_artist_responses(
                            m.get(&db_id)?.clone(),
                            artist_covers.as_ref(),
                        ))
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
) -> Result<Json<PageResponse<ReleaseResponse>>, AppError> {
    let ReleaseListQuery {
        inc,
        query,
        year,
        library_id,
        genre_id,
        sort_by,
        sort_order,
        rating,
        page,
    } = list_query;
    let page_request = page.resolve_snapshot();
    let principal = require_authenticated(&headers).await?;
    let rating_filter = rating.parse()?;
    let include_entry_paths =
        db::roles::has_permission(&principal.permissions, Permission::ManageLibraries);

    let db = &*STATE.db.read().await;
    let (includes, include_covers, include_genres, include_artist_covers) =
        parse_release_includes(inc)?;
    let search_term = super::parse_text_query(query);
    let year_context = year.map(|year| year.to_string());
    let (min_rating, max_rating) = rating_filter.bounds();
    let min_rating_context = min_rating.map(|value| value.to_string());
    let max_rating_context = max_rating.map(|value| value.to_string());
    let snapshot_key = SnapshotKey::builder(&principal.user_public_id, "releases")
        .field(search_term.as_deref())
        .field(year_context.as_deref())
        .field(library_id.as_deref())
        .values(genre_id.as_deref())
        .values(sort_by.as_deref())
        .field(sort_order.as_deref())
        .field(min_rating_context.as_deref())
        .field(max_rating_context.as_deref())
        .finish();
    let sort = super::parse_catalog_sort(sort_by, sort_order)?;
    let library = crate::services::auth::access::resolve_optional_library_filter(
        db,
        &principal,
        library_id.as_deref(),
    )?;

    let (release_items, next_cursor) = if let Some(page) = page_request.resume(&snapshot_key)? {
        let release_items = super::load_snapshot_items(
            db,
            &page.item_ids,
            db::releases::get_by_id,
            |db, release_db_id| {
                crate::services::auth::access::entity_accessible(db, &principal, release_db_id)
            },
        )?;
        (release_items, page.next_cursor)
    } else {
        let genres = resolve_genre_id_filter(db, &parse_genre_id_filter(genre_id))?;
        let viewer = catalog::Viewer::user(db, principal.clone())?;
        let mut query = catalog::Query::<Releases>::new(ReleaseFilter {
            library,
            genres,
            years: year.into_iter().collect(),
            rating: rating_filter,
            ..ReleaseFilter::default()
        });
        query.search = search_term;
        query.sort = sort;
        query.seed = rand::random();
        let ids = catalog::order(db, &viewer, &query)?;
        let page = page_request.start(&snapshot_key, super::public_ids(db, &ids)?)?;
        (
            Releases::hydrate(db, &ids[..page.item_ids.len()])?,
            page.next_cursor,
        )
    };
    let details = releases::list_details_for_releases(db, includes, release_items)?;

    let mut items: Vec<ReleaseResponse> = Vec::with_capacity(details.len());
    for detail in details {
        items.push(detail_to_release_response(
            db,
            detail,
            include_covers,
            include_artist_covers,
            include_genres,
            include_entry_paths,
        )?);
    }

    Ok(Json(PageResponse { items, next_cursor }))
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
    let (includes, include_covers, include_genres, include_artist_covers) =
        parse_release_includes(query.inc)?;
    let release_db_id = db::lookup::find_node_id_by_id(db, &id)?
        .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
    crate::services::auth::access::require_entity_accessible(
        db,
        &principal,
        release_db_id,
        || AppError::not_found(format!("Release not found: {id}")),
    )?;
    let detail = releases::get_details(db, release_db_id, includes)?
        .ok_or_else(|| AppError::not_found(format!("Release not found: {}", id)))?;

    Ok(Json(detail_to_release_response(
        db,
        detail,
        include_covers,
        include_artist_covers,
        include_genres,
        include_entry_paths,
    )?))
}

async fn search_release_covers(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(query): Json<route_covers::CoverSearchQuery>,
) -> Result<Json<ReleaseCoverSearchResponse>, AppError> {
    let principal = require_authenticated(&headers).await?;

    {
        let db = STATE.db.read().await;
        let release_db_id = db::lookup::find_node_id_by_id(&*db, &id)?
            .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
        crate::services::auth::access::require_entity_accessible(
            &*db,
            &principal,
            release_db_id,
            || AppError::not_found(format!("Release not found: {id}")),
        )?;
        if db::releases::get_by_id(&db, release_db_id)?.is_none() {
            return Err(AppError::not_found(format!("Release not found: {}", id)));
        }
    }

    let provider_filter = query.provider.as_deref();
    let found =
        covers::search_release_cover_candidates(&id, provider_filter, query.force_refresh).await?;
    let results = route_covers::map_provider_cover_search_results(found);

    Ok(Json(ReleaseCoverSearchResponse {
        release_id: id,
        results,
    }))
}

#[cfg(feature = "docgen")]
fn list_releases_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List releases").description(
        "Returns releases as `{ items, next_cursor }`. Supported query parameters: `inc`, `query`, `year`, `library_id`, `genre_id`, `sort_by`, `sort_order`, `min_rating`, `max_rating`, `limit`, `cursor`. `min_rating` and `max_rating` filter releases by the authenticated user's inclusive personal rating range; either bound excludes unrated releases. `library_id` scopes results to releases belonging to that public library ID. `genre_id` filters by one or more public genre IDs. `query` is a fuzzy text match against release titles and defaults ordering to relevance. `sort_by` supports `sort_name`, `name`, `date_created`, `release_date`, `last_played_at`, `listen_count`, `total_duration`, and `id`; `sort_order` supports `ascending` and `descending`. `limit` defaults to 100 and is capped at 500. Drive pagination from `next_cursor`; it is `null` on the last page. Supported `inc` values: `artists`, `tracks`, `track_artists`, `entries`, `covers`, `artist_covers`, `genres`. When `inc=covers`, cover metadata includes a public image URL. When `inc=artists`, each artist carries a `credit` object with `type`, `detail`, and `source`; add `artist_covers` to include public artist image metadata. An artist may appear multiple times with different credits (for example, artist and producer). Track artists without direct credits inherit from the release (`source: release`). When `inc=entries`, `full_path` is included only for authenticated users with ManageLibraries permission.",
    )
}

#[cfg(feature = "docgen")]
fn get_release_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get release by ID").description(
        "Returns a single release. 404 if not found. Use `inc` to include artists, tracks, track_artists, entries, covers, artist_covers, and/or genres. When `inc=covers`, cover metadata includes a public image URL. When `inc=artists`, each artist carries a `credit` object with `type`, `detail`, and `source`; add `artist_covers` to include public artist image metadata. An artist may appear multiple times with different credits. Track artists without direct credits inherit from the release (`source: release`). When `inc=entries`, `full_path` is included only for authenticated users with ManageLibraries permission.",
    )
}

#[cfg(feature = "docgen")]
fn search_release_covers_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Search release cover candidates").description(
        "Returns provider cover candidates for a release. Request body (JSON): `{ provider?, force_refresh? }`; \
        `force_refresh=true` bypasses cached provider cover resolution. Providers may return \
        width, height, and selected_index for automatic selection.",
    )
}

pub fn release_routes() -> Router {
    Router::new()
        .route("/", get(get_releases))
        .route("/{id}", get(get_release))
        .route("/{id}/similar", get(similarity::get_similar_releases))
        .route("/{id}/mix", get(super::mix::get_release_mix))
        .route("/{id}/covers/search", post(search_release_covers))
}

#[cfg(feature = "docgen")]
pub(crate) fn release_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::{
        get_with,
        post_with,
    };

    aide::axum::ApiRouter::new()
        .api_route("/", get_with(get_releases, list_releases_docs))
        .api_route("/{id}", get_with(get_release, get_release_docs))
        .api_route(
            "/{id}/similar",
            get_with(
                similarity::get_similar_releases,
                similarity::get_similar_releases_docs,
            ),
        )
        .api_route(
            "/{id}/mix",
            get_with(super::mix::get_release_mix, super::mix::release_mix_docs),
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
        http::{
            HeaderMap,
            StatusCode,
            header::AUTHORIZATION,
        },
        response::IntoResponse,
    };

    use crate::db::test_db::{
        TestDb,
        connect,
        insert_library,
        insert_release as insert_test_release,
        insert_track,
    };

    use crate::testing::{
        LibraryFixtureConfig,
        initialize_runtime,
        runtime_test_lock,
    };

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

    async fn setup_route_test() -> anyhow::Result<()> {
        initialize_runtime(&LibraryFixtureConfig {
            directory: std::path::PathBuf::from("."),
            language: None,
            country: None,
        })
        .await
    }

    async fn create_admin_auth(username: &str) -> anyhow::Result<(HeaderMap, DbId)> {
        let user_db_id = {
            let mut db = STATE.db.write().await;
            db::roles::ensure_builtin_roles(&mut db)?;
            let user_db_id = db::users::create(&mut db, &db::test_db::test_user(username)?)?;
            db::roles::ensure_user_has_role(&mut db, user_db_id, db::roles::BUILTIN_ADMIN_ROLE)?;
            user_db_id
        };

        let session = crate::testing::create_session(user_db_id, Default::default()).await?;
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            format!("Bearer {}", session.token)
                .parse()
                .expect("valid auth header"),
        );
        Ok((headers, user_db_id))
    }

    async fn create_admin_headers(username: &str) -> anyhow::Result<HeaderMap> {
        Ok(create_admin_auth(username).await?.0)
    }

    #[test]
    fn parse_inc_accepts_covers() {
        let parsed = match parse_inc(Some(vec!["artists,covers,artist_covers".to_string()])) {
            Ok(value) => value,
            Err(_) => panic!("covers inc should parse"),
        };
        assert!(parsed.artists);
        assert!(!parsed.tracks);
        assert!(!parsed.entries);
        assert!(parsed.covers);
        assert!(parsed.artist_covers);
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
                "Supported values: artists, tracks, track_artists, entries, covers, artist_covers, genres"
            )
        );
        Ok(())
    }

    #[test]
    fn parse_genre_id_filter_splits_and_trims_values() {
        let genre_ids = parse_genre_id_filter(Some(vec![
            "genre-rock, genre-jazz".to_string(),
            "genre-electronic".to_string(),
        ]));
        assert_eq!(
            genre_ids,
            vec!["genre-rock", "genre-jazz", "genre-electronic"]
        );
    }

    fn set_release_fields(
        db: &mut DbAny,
        release_db_id: DbId,
        sort_title: Option<&str>,
        release_date: Option<&str>,
    ) -> anyhow::Result<()> {
        let mut release = db::releases::get_by_id(db, release_db_id)?
            .ok_or_else(|| anyhow::anyhow!("release missing"))?;
        release.sort_title = sort_title.map(str::to_string);
        release.release_date = release_date.map(str::to_string);
        db::releases::update(db, &release)
    }

    async fn list_release_titles(
        headers: HeaderMap,
        sort_by: Option<&str>,
        sort_order: Option<&str>,
    ) -> anyhow::Result<Vec<String>> {
        let Json(page) = get_releases(
            headers,
            Query(ReleaseListQuery {
                inc: None,
                query: None,
                year: None,
                library_id: None,
                genre_id: None,
                sort_by: sort_by.map(|value| vec![value.to_string()]),
                sort_order: sort_order.map(str::to_string),
                rating: RatingFilterQuery::default(),
                page: super::super::PageQuery::default(),
            }),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        Ok(page
            .items
            .into_iter()
            .map(|release| release.title)
            .collect())
    }

    #[tokio::test]
    async fn get_releases_defaults_to_sort_name_then_name_then_id() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        {
            let mut db = STATE.db.write().await;
            insert_test_release(&mut db, "Charlie")?;
            let zulu = insert_test_release(&mut db, "Zulu")?;
            set_release_fields(&mut db, zulu, Some("Alpha"), None)?;
            insert_test_release(&mut db, "Beta")?;
            insert_test_release(&mut db, "beta")?;
            for title in ["Mike", "Lima"] {
                let release = insert_test_release(&mut db, title)?;
                set_release_fields(&mut db, release, Some("Delta"), None)?;
            }
        }
        let headers = create_admin_headers("release-default-admin").await?;

        assert_eq!(
            list_release_titles(headers.clone(), None, None).await?,
            vec!["Zulu", "Beta", "beta", "Charlie", "Lima", "Mike"]
        );
        assert_eq!(
            list_release_titles(headers, Some("sort_name"), Some("descending")).await?,
            vec!["Lima", "Mike", "Charlie", "Beta", "beta", "Zulu"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn get_releases_resume_rechecks_access() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let (user_db_id, library, releases) = {
            let mut db = STATE.db.write().await;
            let user_db_id = db::test_db::insert_user(&mut db, "release-resume")?;
            let library = insert_library(&mut db, "Release Resume", "/tmp/lyra-release-resume")?;
            db::libraries::grant_access(
                &mut *db,
                user_db_id,
                library,
                db::libraries::AccessKind::ReadWrite,
            )?;
            let mut releases = Vec::new();
            for title in ["First", "Second"] {
                let release = insert_test_release(&mut db, title)?;
                connect(&mut db, library, release)?;
                releases.push(release);
            }
            (user_db_id, library, releases)
        };
        let session = crate::testing::create_session(user_db_id, Default::default()).await?;
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            format!("Bearer {}", session.token)
                .parse()
                .expect("valid auth header"),
        );
        let page = |cursor: Option<String>| {
            Query(ReleaseListQuery {
                inc: None,
                query: None,
                year: None,
                library_id: None,
                genre_id: None,
                sort_by: None,
                sort_order: None,
                rating: RatingFilterQuery::default(),
                page: super::super::PageQuery {
                    limit: Some(1),
                    cursor,
                },
            })
        };

        let Json(first) = get_releases(headers.clone(), page(None))
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert_eq!(first.items.len(), 1);
        let cursor = first
            .next_cursor
            .ok_or_else(|| anyhow::anyhow!("expected a second page"))?;

        {
            let mut db = STATE.db.write().await;
            for release in releases {
                db::graph::remove_edges_between(&mut *db, library, release)?;
            }
        }

        let Json(second) = get_releases(headers, page(Some(cursor)))
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert!(second.items.is_empty());
        assert!(second.next_cursor.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn get_releases_orders_partial_release_dates() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        {
            let mut db = STATE.db.write().await;
            for (title, date) in [
                ("Day", "2004-05-06"),
                ("Next Year", "2005"),
                ("Year", "2004"),
                ("Earlier", "1999-12-31"),
                ("Month", "2004-05"),
            ] {
                let release = insert_test_release(&mut db, title)?;
                set_release_fields(&mut db, release, None, Some(date))?;
            }
        }
        let headers = create_admin_headers("release-date-admin").await?;

        assert_eq!(
            list_release_titles(headers.clone(), Some("release_date"), None).await?,
            vec!["Earlier", "Year", "Month", "Day", "Next Year"]
        );
        assert_eq!(
            list_release_titles(headers, Some("release_date"), Some("descending")).await?,
            vec!["Next Year", "Day", "Month", "Year", "Earlier"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn get_releases_sorts_missing_release_dates_last_in_both_directions() -> anyhow::Result<()>
    {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        {
            let mut db = STATE.db.write().await;
            for (title, date) in [
                ("Undated", None),
                ("Old", Some("1990")),
                ("New", Some("2020")),
            ] {
                let release = insert_test_release(&mut db, title)?;
                set_release_fields(&mut db, release, None, date)?;
            }
        }
        let headers = create_admin_headers("release-undated-admin").await?;

        assert_eq!(
            list_release_titles(headers.clone(), Some("release_date"), Some("ascending")).await?,
            vec!["Old", "New", "Undated"]
        );
        assert_eq!(
            list_release_titles(headers, Some("release_date"), Some("descending")).await?,
            vec!["New", "Old", "Undated"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn get_releases_orders_query_matches_by_relevance() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        {
            let mut db = STATE.db.write().await;
            let library = insert_library(&mut db, "Relevance", "/tmp/lyra-relevance")?;
            for title in ["A b l u e", "Blue"] {
                let release = insert_test_release(&mut db, title)?;
                connect(&mut db, library, release)?;
            }
        }
        let headers = create_admin_headers("release-relevance-admin").await?;

        let Json(page) = get_releases(
            headers,
            Query(ReleaseListQuery {
                inc: None,
                query: Some("blue".to_string()),
                year: None,
                library_id: None,
                genre_id: None,
                sort_by: None,
                sort_order: None,
                rating: RatingFilterQuery::default(),
                page: super::super::PageQuery::default(),
            }),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        let titles: Vec<String> = page
            .items
            .into_iter()
            .map(|release| release.title)
            .collect();
        assert_eq!(titles, vec!["Blue", "A b l u e"]);
        Ok(())
    }

    #[tokio::test]
    async fn get_releases_scopes_by_library_id() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let visible_library_id = {
            let mut db = STATE.db.write().await;
            let visible_library =
                insert_library(&mut db, "Visible Releases", "/tmp/lyra-visible-releases")?;
            let hidden_library =
                insert_library(&mut db, "Hidden Releases", "/tmp/lyra-hidden-releases")?;
            let visible_release = insert_test_release(&mut db, "Visible Release")?;
            let hidden_release = insert_test_release(&mut db, "Hidden Release")?;
            connect(&mut db, visible_library, visible_release)?;
            connect(&mut db, hidden_library, hidden_release)?;

            db::libraries::get_by_id(&db, visible_library)?
                .ok_or_else(|| anyhow::anyhow!("visible library missing"))?
                .id
        };
        let headers = create_admin_headers("release-scope-admin").await?;

        let Json(page) = get_releases(
            headers,
            Query(ReleaseListQuery {
                inc: None,
                query: None,
                year: None,
                library_id: Some(visible_library_id),
                genre_id: None,
                sort_by: None,
                sort_order: None,
                rating: RatingFilterQuery::default(),
                page: super::super::PageQuery::default(),
            }),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].title, "Visible Release");
        assert!(page.next_cursor.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn get_similar_releases_returns_empty_array_without_handlers() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let release_id = {
            let mut db = STATE.db.write().await;
            let library = insert_library(&mut db, "Similar", "/tmp/lyra-similar")?;
            let release_db_id = insert_test_release(&mut db, "Seed Release")?;
            connect(&mut db, library, release_db_id)?;
            db::releases::get_by_id(&*db, release_db_id)?
                .ok_or_else(|| anyhow::anyhow!("seed release missing"))?
                .id
        };
        let headers = create_admin_headers("similar-release-admin").await?;

        let Json(items) = similarity::get_similar_releases(
            headers,
            Path(release_id),
            Query(similarity::SimilarReleasesQuery {
                limit: None,
                inc: None,
            }),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert!(items.is_empty());
        Ok(())
    }

    /// Lists releases similar to a fresh seed as a fresh admin, deleted at gap `gap`.
    async fn similar_releases_as_deleted_caller(
        gap: Option<usize>,
    ) -> anyhow::Result<(bool, axum::http::StatusCode)> {
        let release_id = {
            let mut db = STATE.db.write().await;
            let library = insert_library(&mut db, "Similar", "/tmp/lyra-similar")?;
            let release_db_id = insert_test_release(&mut db, "Seed Release")?;
            connect(&mut db, library, release_db_id)?;
            db::releases::get_by_id(&*db, release_db_id)?
                .ok_or_else(|| anyhow::anyhow!("seed release missing"))?
                .id
        };
        let (headers, user_db_id) =
            create_admin_auth(&format!("similar-caller-{}", nanoid::nanoid!())).await?;
        let (reached, result) = crate::testing::run_with_recycle_at(
            gap,
            similarity::get_similar_releases(
                headers,
                Path(release_id),
                Query(similarity::SimilarReleasesQuery {
                    limit: None,
                    inc: None,
                }),
            ),
            |db| {
                db.transaction_mut(|t| db::users::delete_user(t, user_db_id))
                    .expect("delete caller");
            },
        )
        .await;
        let status = result.map_or_else(
            |error| axum::response::IntoResponse::into_response(error).status(),
            |_| axum::http::StatusCode::OK,
        );
        Ok((reached, status))
    }

    #[tokio::test]
    async fn get_similar_releases_rejects_a_caller_deleted_mid_request() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;
        let (_, control) = similar_releases_as_deleted_caller(None).await?;
        assert_eq!(control, axum::http::StatusCode::OK);
        crate::testing::for_each_db_gap(similar_releases_as_deleted_caller, async |gap, status| {
            assert_eq!(status, axum::http::StatusCode::UNAUTHORIZED, "gap {gap}");
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn get_releases_filters_top_level_personal_rating() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;
        let (headers, user_db_id) = create_admin_auth("release-rating-admin").await?;

        {
            let mut db = STATE.db.write().await;
            let rated_release = insert_test_release(&mut db, "Rated Release")?;
            let unrated_release = insert_test_release(&mut db, "Unrated Release")?;
            let five_star_release = insert_test_release(&mut db, "Five Star Release")?;
            let unrated_track = insert_track(&mut db, "Unrated Nested Track")?;
            let rated_track = insert_track(&mut db, "Rated Nested Track")?;
            connect(&mut db, rated_release, unrated_track)?;
            connect(&mut db, unrated_release, rated_track)?;
            db::ratings::upsert(
                &mut *db,
                user_db_id,
                rated_release,
                db::ratings::RatingKind::Release,
                db::ratings::RatingValue::new(4).unwrap(),
                1,
            )?;
            db::ratings::upsert(
                &mut *db,
                user_db_id,
                five_star_release,
                db::ratings::RatingKind::Release,
                db::ratings::RatingValue::new(5).unwrap(),
                1,
            )?;
            db::ratings::upsert(
                &mut *db,
                user_db_id,
                rated_track,
                db::ratings::RatingKind::Track,
                db::ratings::RatingValue::new(4).unwrap(),
                1,
            )?;
        }

        let Json(page) = get_releases(
            headers,
            Query(ReleaseListQuery {
                inc: Some(vec!["tracks".to_string()]),
                query: None,
                year: None,
                library_id: None,
                genre_id: None,
                sort_by: None,
                sort_order: None,
                rating: RatingFilterQuery::from_bounds(Some(4), Some(4)),
                page: super::super::PageQuery::default(),
            }),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].title, "Rated Release");
        assert_eq!(
            page.items[0]
                .tracks
                .as_ref()
                .and_then(|tracks| tracks.first())
                .map(|track| track.title.as_str()),
            Some("Unrated Nested Track"),
        );
        Ok(())
    }

    #[tokio::test]
    async fn get_releases_filters_by_genre_id() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let rock_genre_id = {
            let mut db = STATE.db.write().await;
            let rock_release = insert_test_release(&mut db, "Rock Release")?;
            let jazz_release = insert_test_release(&mut db, "Jazz Release")?;
            db::genres::sync_release_genres(&mut *db, rock_release, &["Rock".to_string()])?;
            db::genres::sync_release_genres(&mut *db, jazz_release, &["Jazz".to_string()])?;
            db::genres::get_for_release(&*db, rock_release)?
                .into_iter()
                .next()
                .ok_or_else(|| anyhow::anyhow!("rock genre missing"))?
                .id
        };
        let headers = create_admin_headers("release-genre-filter-admin").await?;

        let Json(page) = get_releases(
            headers,
            Query(ReleaseListQuery {
                inc: None,
                query: None,
                year: None,
                library_id: None,
                genre_id: Some(vec![rock_genre_id]),
                sort_by: None,
                sort_order: None,
                rating: RatingFilterQuery::default(),
                page: super::super::PageQuery::default(),
            }),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].title, "Rock Release");
        assert!(page.next_cursor.is_none());
        Ok(())
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
    fn build_cover_response_omits_when_not_requested() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_db_id = insert_release_node(&mut db)?;
        let cover = route_covers::build_cover_response(&db, release_db_id, false)?;
        assert!(cover.is_none());
        Ok(())
    }

    #[test]
    fn build_cover_response_returns_null_when_missing() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_db_id = insert_release_node(&mut db)?;
        let cover = route_covers::build_cover_response(&db, release_db_id, true)?;
        assert!(matches!(cover, Some(None)));
        Ok(())
    }

    #[test]
    fn build_cover_response_returns_cover_metadata() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_db_id = insert_release_node(&mut db)?;
        insert_cover_for_release(&mut db, release_db_id)?;

        let cover = route_covers::build_cover_response(&db, release_db_id, true)?
            .flatten()
            .ok_or_else(|| anyhow::anyhow!("expected cover metadata"))?;

        assert_eq!(
            cover.url,
            format!("/api/covers/{}?v={}", cover.id, cover.hash)
        );
        assert_eq!(cover.mime_type, "image/jpeg");
        assert_eq!(cover.hash, "a".repeat(64));
        Ok(())
    }
}

#[cfg(all(test, feature = "nightly"))]
mod benches {
    extern crate test;

    use agdb::{
        DbAny,
        DbId,
    };
    use nanoid::nanoid;
    use test::{
        Bencher,
        black_box,
    };

    use super::*;
    use crate::db::test_db::{
        connect,
        insert_release as insert_test_release,
        insert_track,
        new_test_db,
        test_user,
    };
    use crate::services::catalog::releases::ReleaseKey;

    struct ReleaseSortBench {
        db: DbAny,
        viewer: catalog::Viewer,
    }

    fn update_track_duration(db: &mut DbAny, track_db_id: DbId, duration_ms: u64) {
        let mut track = db::tracks::get_by_id(db, track_db_id)
            .unwrap()
            .expect("track exists");
        track.duration_ms = Some(duration_ms);
        db::tracks::update(db, &track).unwrap();
    }

    fn record_listen(db: &mut DbAny, user_db_id: DbId, track_db_id: DbId, listened_at_ms: u64) {
        let track = db::tracks::get_by_id(db, track_db_id)
            .unwrap()
            .expect("track exists");
        let listen = db::Listen {
            db_id: None,
            id: nanoid!(),
            track_public_id: track.id,
            position_ms: 0,
            duration_ms: Some(180_000),
            activity_ms: 180_000,
            state: db::PlaybackState::Completed,
            listened_at_ms,
            created_at_ms: listened_at_ms,
        };
        let session = db::PlaybackSession {
            db_id: None,
            id: nanoid!(),
            client_name: None,
            position_ms: 0,
            duration_ms: Some(180_000),
            activity_ms: Some(180_000),
            last_position_ms: None,
            state: db::PlaybackState::Completed,
            listen_recorded: Some(true),
            updated_at_ms: listened_at_ms,
            created_at_ms: listened_at_ms,
        };
        db::listens::create_and_mark_recorded(db, &listen, track_db_id, user_db_id, &session)
            .unwrap();
    }

    fn seed_release_sort_bench(
        release_count: usize,
        tracks_per_release: usize,
        listens_per_track: usize,
    ) -> ReleaseSortBench {
        let mut db = new_test_db().unwrap();
        let user_db_id =
            db::users::create(&mut db, &test_user("release-sort-bench").unwrap()).unwrap();
        for release_idx in 0..release_count {
            let release_db_id =
                insert_test_release(&mut db, &format!("Release {release_idx:04}")).unwrap();
            for track_idx in 0..tracks_per_release {
                let track_db_id = insert_track(
                    &mut db,
                    &format!("Release {release_idx:04} Track {track_idx:02}"),
                )
                .unwrap();
                update_track_duration(
                    &mut db,
                    track_db_id,
                    60_000 + ((release_idx + track_idx) % 300) as u64 * 1_000,
                );
                for listen_idx in 0..listens_per_track {
                    record_listen(
                        &mut db,
                        user_db_id,
                        track_db_id,
                        ((release_idx * tracks_per_release * listens_per_track)
                            + (track_idx * listens_per_track)
                            + listen_idx) as u64
                            * 1_000,
                    );
                }
                connect(&mut db, release_db_id, track_db_id).unwrap();
            }
        }

        let principal = crate::services::auth::Principal::for_user(
            &db,
            user_db_id,
            vec![db::Permission::Admin],
            Default::default(),
        );
        let viewer = catalog::Viewer::user(&db, principal).unwrap();
        ReleaseSortBench { db, viewer }
    }

    fn bench_order(b: &mut Bencher, setup: &ReleaseSortBench, sort: catalog::SortSpec<ReleaseKey>) {
        let mut query = catalog::Query::<Releases>::new(ReleaseFilter::default());
        query.sort = sort;
        b.iter(|| catalog::order(&setup.db, &setup.viewer, black_box(&query)).unwrap());
    }

    #[bench]
    fn route_sort_releases_sort_name_500(b: &mut Bencher) {
        let setup = seed_release_sort_bench(500, 0, 0);
        bench_order(b, &setup, Vec::new());
    }

    #[bench]
    fn route_sort_releases_total_duration_500_releases_4000_tracks(b: &mut Bencher) {
        let setup = seed_release_sort_bench(500, 8, 0);
        bench_order(
            b,
            &setup,
            vec![(ReleaseKey::TotalDuration, catalog::Direction::Descending)],
        );
    }

    #[bench]
    fn route_sort_releases_listen_count_500_releases_4000_listens(b: &mut Bencher) {
        let setup = seed_release_sort_bench(500, 8, 1);
        bench_order(
            b,
            &setup,
            vec![(ReleaseKey::ListenCount, catalog::Direction::Descending)],
        );
    }
}
