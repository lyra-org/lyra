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
    http::HeaderMap,
};
use axum::{
    Router,
    routing::get,
};
use serde::{
    Deserialize,
    Serialize,
};

use crate::routes::request::bearer_credential;
use crate::{
    routes::{
        AppError,
        covers as route_covers,
        deserialize_inc,
        parse_inc_values,
        responses::{
            CoverResponse,
            PageResponse,
        },
    },
    services::STATE,
    services::{
        auth::require_authenticated,
        catalog::{
            self,
            genres::{
                GenreFilter,
                Genres,
            },
            pipeline::Catalog,
        },
        covers::{
            self as cover_services,
            display::{
                CoverFilter,
                Listens,
            },
        },
        pagination::SnapshotKey,
    },
};
use lyra_db::genres;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct GenreResponse {
    id: String,
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    parents: Option<Vec<GenreSummary>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    children: Option<Vec<GenreSummary>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cover: Option<Option<CoverResponse>>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct GenreSummary {
    id: String,
    name: String,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct GenreListQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Comma-separated or repeated values: covers.")
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    inc: Option<Vec<String>>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Optional text query matched against genre names and aliases; each of its words must start a word, ignoring case, accents, apostrophes and punctuation; no character is an operator, and a word in a script written without spaces, such as Japanese, matches anywhere in a word. Exact names rank first, then names starting with the query, then names holding every word, then names holding some, then matches only through related items."
        )
    )]
    query: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Optional public library ID to scope returned genres.")
    )]
    library_id: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Comma-separated or repeated values: name, release_count, track_count, total_duration, listen_count, last_played_at, relevance (with query), random, id."
        )
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    sort_by: Option<Vec<String>>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Sort direction: ascending or descending.")
    )]
    sort_order: Option<String>,
    #[serde(flatten)]
    page: super::PageQuery,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct GenreQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Comma-separated or repeated values: parents, children.")
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    inc: Option<Vec<String>>,
}

struct GenreInc {
    parents: bool,
    children: bool,
    covers: bool,
}

fn parse_genre_inc(inc: Option<Vec<String>>) -> Result<GenreInc, AppError> {
    let values = parse_inc_values(inc, &["parents", "children", "covers"])?;
    let mut result = GenreInc {
        parents: false,
        children: false,
        covers: false,
    };
    for value in values {
        match value.as_str() {
            "parents" => result.parents = true,
            "children" => result.children = true,
            "covers" => result.covers = true,
            _ => {}
        }
    }
    Ok(result)
}

fn genre_to_summary(genre: genres::Genre) -> GenreSummary {
    GenreSummary {
        id: genre.id,
        name: genre.name,
    }
}

fn genre_to_response(genre: genres::Genre) -> GenreResponse {
    GenreResponse {
        id: genre.id,
        name: genre.name,
        parents: None,
        children: None,
        cover: None,
    }
}

fn genre_accessible_to_principal(
    db: &impl lyra_db::DbAccess,
    principal: &crate::services::auth::Principal,
    genre_db_id: DbId,
) -> anyhow::Result<bool> {
    for release_db_id in genres::get_releases(db, genre_db_id)? {
        if crate::services::auth::access::entity_accessible(db, principal, release_db_id)? {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn list_genres(
    headers: HeaderMap,
    Query(query): Query<GenreListQuery>,
) -> Result<Json<PageResponse<GenreResponse>>, AppError> {
    let GenreListQuery {
        inc,
        query,
        library_id,
        sort_by,
        sort_order,
        page,
    } = query;
    let page_request = page.resolve_snapshot();
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    let inc = parse_genre_inc(inc)?;
    let search_term = super::parse_text_query(query);

    let db = &*STATE.db.read().await;
    let snapshot_key = SnapshotKey::builder(&principal.user_public_id, "genres")
        .field(search_term.as_deref())
        .field(library_id.as_deref())
        .values(sort_by.as_deref())
        .field(sort_order.as_deref())
        .finish();
    let library_scope = crate::services::auth::access::resolve_optional_library_filter(
        db,
        &principal,
        library_id.as_deref(),
    )?;
    let sort = super::parse_catalog_sort(sort_by, sort_order)?;
    let viewer = catalog::Viewer::user(db, principal.clone())?;
    let (genres, next_cursor) = if let Some(page) = page_request.resume(&snapshot_key)? {
        let genres = super::load_snapshot_items(
            db,
            &page.item_ids,
            genres::get_by_id,
            |db, genre_db_id| genre_accessible_to_principal(db, &principal, genre_db_id),
        )?;
        (genres, page.next_cursor)
    } else {
        let mut query = catalog::Query::<Genres>::new(GenreFilter {
            library: library_scope,
            ..GenreFilter::default()
        });
        query.search = search_term;
        query.sort = sort;
        query.seed = rand::random();
        let ids = catalog::order(db, &viewer, &query)?;
        let page = page_request.start(&snapshot_key, super::public_ids(db, &ids)?)?;
        let genres = Genres::hydrate(db, &ids[..page.item_ids.len()])?;
        (genres, page.next_cursor)
    };
    let covers = if inc.covers {
        Some(cover_services::display::genres::covers_for_genres(
            db,
            &mut CoverFilter::new(&viewer).in_library(library_scope),
            &genres,
            Listens::Viewer,
        )?)
    } else {
        None
    };

    let items: Vec<GenreResponse> = genres
        .into_iter()
        .map(|genre| {
            let genre_db_id = genre.db_id.clone().map(DbId::from);
            let mut response = genre_to_response(genre);
            if inc.covers {
                response.cover = Some(
                    genre_db_id
                        .and_then(|genre_db_id| covers.as_ref()?.get(&genre_db_id).cloned())
                        .map(|resolved| route_covers::cover_to_response(resolved.cover)),
                );
            }
            response
        })
        .collect();

    Ok(Json(PageResponse { items, next_cursor }))
}

async fn get_genre(
    headers: HeaderMap,
    Path(id): Path<String>,
    axum::extract::Query(query): axum::extract::Query<GenreQuery>,
) -> Result<Json<GenreResponse>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    let inc = parse_genre_inc(query.inc)?;

    let db = &*STATE.db.read().await;
    let genre_db_id = lyra_db::lookup::find_node_id_by_id(db, &id)?
        .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
    let genre = genres::get_by_id(db, genre_db_id)?
        .ok_or_else(|| AppError::not_found(format!("Genre not found: {id}")))?;
    if !genre_accessible_to_principal(db, &principal, genre_db_id)? {
        return Err(AppError::not_found(format!("Genre not found: {id}")));
    }

    let parents = if inc.parents {
        Some(
            genres::get_parents(db, genre_db_id)?
                .into_iter()
                .map(genre_to_summary)
                .collect(),
        )
    } else {
        None
    };

    let children = if inc.children {
        Some(
            genres::get_children(db, genre_db_id)?
                .into_iter()
                .map(genre_to_summary)
                .collect(),
        )
    } else {
        None
    };
    let cover = if inc.covers {
        let viewer = catalog::Viewer::user(db, principal.clone())?;
        Some(
            cover_services::display::genres::cover_for_genre(
                db,
                &mut CoverFilter::new(&viewer),
                &genre,
                Listens::Viewer,
            )?
            .map(|resolved| route_covers::cover_to_response(resolved.cover)),
        )
    } else {
        None
    };

    Ok(Json(GenreResponse {
        id: genre.id,
        name: genre.name,
        parents,
        children,
        cover,
    }))
}

#[cfg(feature = "docgen")]
fn list_genres_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List genres").description(
        "Returns genres as `{ items, next_cursor }`. Supported query parameters: `inc`, `query`, `library_id`, `sort_by`, `sort_order`, `limit`, `cursor`. Results are limited to genres attached to releases visible to the authenticated user. `library_id` scopes results to releases belonging to that public library ID. `sort_by` supports `name`, `last_played_at`, `listen_count`, `release_count`, `track_count`, `total_duration`, and `id`; `sort_order` supports `ascending` and `descending`. `limit` defaults to 100 and is capped at 500. Drive pagination from `next_cursor`; it is `null` on the last page. `query` matches genre names and aliases; each of its words must start a word, ignoring case, accents, apostrophes and punctuation; no character is an operator, and a word in a script written without spaces, such as Japanese, matches anywhere in a word. Exact names rank first, then names starting with the query, then names holding every word, then names holding some, then matches only through related items. `inc=covers` includes personalized display cover metadata.",
    )
}

#[cfg(feature = "docgen")]
fn get_genre_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get genre by ID")
        .description("Returns a single genre. Use `inc=parents,children,covers` to include hierarchy and personalized display cover metadata.")
}

pub fn genre_routes() -> Router {
    Router::new()
        .route("/", get(list_genres))
        .route("/{id}", get(get_genre))
}

#[cfg(feature = "docgen")]
pub(crate) fn genre_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::get_with;

    aide::axum::ApiRouter::new()
        .api_route("/", get_with(list_genres, list_genres_docs))
        .api_route("/{id}", get_with(get_genre, get_genre_docs))
}

#[cfg(test)]
mod tests {
    use agdb::DbAny;
    use axum::{
        http::{
            HeaderMap,
            StatusCode,
            header::AUTHORIZATION,
        },
        response::IntoResponse,
    };

    use crate::{
        plugins::testing::init_library_test_state,
        services::testing::runtime_test_lock,
    };
    use lyra_db::fixtures::insert_release as insert_test_release;
    use lyra_db::fixtures::{
        connect,
        insert_track,
    };
    use lyra_db::test_db::insert_library;

    use super::*;

    async fn setup_route_test() -> anyhow::Result<()> {
        init_library_test_state(std::path::PathBuf::from(".")).await
    }

    async fn create_admin_headers(username: &str) -> anyhow::Result<HeaderMap> {
        let user_db_id = {
            let mut db = STATE.db.write().await;
            lyra_db::roles::ensure_builtin_roles(&mut db)?;
            let user_db_id =
                lyra_db::users::create(&mut db, &lyra_db::fixtures::test_user(username)?)?;
            lyra_db::roles::ensure_user_has_role(
                &mut db,
                user_db_id,
                lyra_db::roles::BUILTIN_ADMIN_ROLE,
            )?;
            user_db_id
        };

        create_headers_for_user(user_db_id).await
    }

    async fn create_headers_for_user(user_db_id: DbId) -> anyhow::Result<HeaderMap> {
        let session =
            crate::services::testing::create_session(user_db_id, Default::default()).await?;
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            format!("Bearer {}", session.token)
                .parse()
                .expect("valid auth header"),
        );
        Ok(headers)
    }

    fn insert_cover_for_release(
        db: &mut DbAny,
        release_db_id: DbId,
        cover_id: &str,
    ) -> anyhow::Result<lyra_db::Cover> {
        db.transaction_mut(|t| {
            lyra_db::covers::upsert(
                t,
                release_db_id,
                lyra_db::covers::CoverSource::Local,
                lyra_db::Cover {
                    db_id: None,
                    id: cover_id.to_string(),
                    path: format!("/music/{cover_id}.jpg"),
                    mime_type: "image/jpeg".to_string(),
                    hash: "a".repeat(64),
                    blurhash: None,
                    provider_id: None,
                },
            )
        })
        .map(|upserted| upserted.cover)
    }

    #[test]
    fn parse_genre_inc_accepts_covers() -> anyhow::Result<()> {
        let inc = parse_genre_inc(Some(vec!["parents,covers".to_string()]))
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert!(inc.parents);
        assert!(inc.covers);
        assert!(!inc.children);
        Ok(())
    }

    #[tokio::test]
    async fn list_genres_scopes_by_library_id() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let visible_library_id = {
            let mut db = STATE.db.write().await;
            let visible_library =
                insert_library(&mut db, "Visible Genres", "/tmp/lyra-visible-genres")?;
            let hidden_library =
                insert_library(&mut db, "Hidden Genres", "/tmp/lyra-hidden-genres")?;
            let visible_release = insert_test_release(&mut db, "Visible Rock")?;
            let hidden_release = insert_test_release(&mut db, "Hidden Jazz")?;
            connect(&mut db, visible_library, visible_release)?;
            connect(&mut db, hidden_library, hidden_release)?;
            lyra_db::genres::sync_release_genres(&mut *db, visible_release, &["Rock".to_string()])?;
            lyra_db::genres::sync_release_genres(&mut *db, hidden_release, &["Jazz".to_string()])?;

            lyra_db::libraries::get_by_id(&db, visible_library)?
                .ok_or_else(|| anyhow::anyhow!("visible library missing"))?
                .id
        };
        let headers = create_admin_headers("genre-scope-admin").await?;

        let Json(genres) = list_genres(
            headers,
            Query(GenreListQuery {
                inc: None,
                query: None,
                library_id: Some(visible_library_id),
                sort_by: None,
                sort_order: None,
                page: Default::default(),
            }),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert_eq!(genres.items.len(), 1);
        assert_eq!(genres.items[0].name, "Rock");
        assert!(genres.next_cursor.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn list_genres_cursor_keeps_query_snapshot_stable() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let library_id = {
            let mut db = STATE.db.write().await;
            let library = insert_library(&mut db, "Genre Search", "/tmp/lyra-genre-search")?;
            for genre_name in ["Ambient", "Acid Jazz", "Rock"] {
                let release =
                    insert_test_release(&mut db, &format!("{genre_name} Search Release"))?;
                connect(&mut db, library, release)?;
                lyra_db::genres::sync_release_genres(&mut *db, release, &[genre_name.to_string()])?;
            }

            lyra_db::libraries::get_by_id(&db, library)?
                .ok_or_else(|| anyhow::anyhow!("genre search library missing"))?
                .id
        };
        let headers = create_admin_headers("genre-search-admin").await?;

        let Json(first_page) = list_genres(
            headers.clone(),
            Query(GenreListQuery {
                inc: None,
                query: Some("a".to_string()),
                library_id: Some(library_id.clone()),
                sort_by: None,
                sort_order: None,
                page: crate::routes::PageQuery {
                    limit: Some(1),
                    cursor: None,
                },
            }),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert_eq!(first_page.items.len(), 1);
        assert_eq!(first_page.items[0].name, "Acid Jazz");
        let cursor = first_page
            .next_cursor
            .ok_or_else(|| anyhow::anyhow!("expected another matching genre"))?;

        {
            let mut db = STATE.db.write().await;
            let library_db_id = lyra_db::lookup::find_node_id_by_id(&db, &library_id)?
                .ok_or_else(|| anyhow::anyhow!("genre search library missing"))?;
            let release = insert_test_release(&mut db, "Aardvark Search Release")?;
            connect(&mut db, library_db_id, release)?;
            lyra_db::genres::sync_release_genres(&mut *db, release, &["Aardvark".to_string()])?;
        }

        let Json(second_page) = list_genres(
            headers,
            Query(GenreListQuery {
                inc: None,
                query: Some("a".to_string()),
                library_id: Some(library_id),
                sort_by: None,
                sort_order: None,
                page: crate::routes::PageQuery {
                    limit: Some(1),
                    cursor: Some(cursor),
                },
            }),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert_eq!(second_page.items.len(), 1);
        assert_eq!(second_page.items[0].name, "Ambient");
        assert!(second_page.next_cursor.is_none());
        Ok(())
    }

    async fn list_genre_names(
        headers: HeaderMap,
        query: Option<&str>,
        sort_by: Option<&str>,
        sort_order: Option<&str>,
    ) -> anyhow::Result<Vec<String>> {
        let Json(genres) = list_genres(
            headers,
            Query(GenreListQuery {
                inc: None,
                query: query.map(str::to_string),
                library_id: None,
                sort_by: sort_by.map(|value| vec![value.to_string()]),
                sort_order: sort_order.map(str::to_string),
                page: Default::default(),
            }),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        Ok(genres.items.into_iter().map(|genre| genre.name).collect())
    }

    fn insert_genre_release(
        db: &mut DbAny,
        library: DbId,
        genre_name: &str,
    ) -> anyhow::Result<DbId> {
        let release = insert_test_release(db, &format!("{genre_name} Release"))?;
        connect(db, library, release)?;
        lyra_db::genres::sync_release_genres(db, release, &[genre_name.to_string()])?;
        Ok(release)
    }

    #[tokio::test]
    async fn list_genres_defaults_to_name_and_admins_see_every_library() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        {
            let mut db = STATE.db.write().await;
            let granted = insert_library(&mut db, "Granted Genres", "/tmp/lyra-granted-genres")?;
            let other = insert_library(&mut db, "Other Genres", "/tmp/lyra-other-genres")?;
            insert_genre_release(&mut db, granted, "Techno")?;
            insert_genre_release(&mut db, other, "Ambient")?;
            insert_genre_release(&mut db, other, "Jazz")?;
        }
        let headers = create_admin_headers("genre-default-admin").await?;

        assert_eq!(
            list_genre_names(headers.clone(), None, None, None).await?,
            vec!["Ambient", "Jazz", "Techno"]
        );
        assert_eq!(
            list_genre_names(headers, None, Some("name"), Some("descending")).await?,
            vec!["Techno", "Jazz", "Ambient"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn list_genres_breaks_name_ties_by_exact_name_then_id() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        {
            let mut db = STATE.db.write().await;
            let library = insert_library(&mut db, "Genre Ties", "/tmp/lyra-genre-ties")?;
            let mut genres = Vec::new();
            for genre_name in ["Jazz", "Metal", "Rock"] {
                let release = insert_genre_release(&mut db, library, genre_name)?;
                genres.push(
                    lyra_db::genres::get_for_release(&*db, release)?
                        .into_iter()
                        .find_map(|genre| genre.db_id.map(DbId::from))
                        .ok_or_else(|| anyhow::anyhow!("genre missing"))?,
                );
            }
            for (genre_db_id, (name, id)) in genres.into_iter().zip([
                ("Rock", "genre-tie-m"),
                ("ROCK", "genre-tie-z"),
                ("Rock", "genre-tie-a"),
            ]) {
                db.exec_mut(
                    agdb::QueryBuilder::insert()
                        .values_uniform([
                            ("name", name).into(),
                            ("scan_name", "rock").into(),
                            ("id", id).into(),
                        ])
                        .ids(genre_db_id)
                        .query(),
                )?;
            }
        }
        let headers = create_admin_headers("genre-tie-admin").await?;

        let Json(genres) = list_genres(
            headers,
            Query(GenreListQuery {
                inc: None,
                query: None,
                library_id: None,
                sort_by: None,
                sort_order: None,
                page: Default::default(),
            }),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        let ids: Vec<String> = genres.items.into_iter().map(|genre| genre.id).collect();
        assert_eq!(ids, vec!["genre-tie-z", "genre-tie-m", "genre-tie-a"]);
        Ok(())
    }

    #[tokio::test]
    async fn list_genres_orders_query_matches_by_relevance() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        {
            let mut db = STATE.db.write().await;
            let library = insert_library(&mut db, "Genre Relevance", "/tmp/lyra-genre-relevance")?;
            for name in ["A Blue", "Blue"] {
                insert_genre_release(&mut db, library, name)?;
            }
        }
        let headers = create_admin_headers("genre-relevance-admin").await?;

        assert_eq!(
            list_genre_names(headers, Some("blue"), None, None).await?,
            vec!["Blue", "A Blue"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn list_genres_without_library_id_uses_accessible_libraries() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let user_db_id = {
            let mut db = STATE.db.write().await;
            let user_db_id =
                lyra_db::users::create(&mut db, &lyra_db::fixtures::test_user("genre-listener")?)?;
            let visible_library =
                insert_library(&mut db, "Accessible Genres", "/tmp/lyra-accessible-genres")?;
            let hidden_library = insert_library(
                &mut db,
                "Inaccessible Genres",
                "/tmp/lyra-inaccessible-genres",
            )?;
            let visible_release = insert_test_release(&mut db, "Accessible Rock")?;
            let hidden_release = insert_test_release(&mut db, "Inaccessible Jazz")?;
            connect(&mut db, visible_library, visible_release)?;
            connect(&mut db, hidden_library, hidden_release)?;
            lyra_db::libraries::grant_access(
                &mut *db,
                user_db_id,
                visible_library,
                lyra_db::libraries::AccessKind::ReadWrite,
            )?;
            lyra_db::genres::sync_release_genres(&mut *db, visible_release, &["Rock".to_string()])?;
            lyra_db::genres::sync_release_genres(&mut *db, hidden_release, &["Jazz".to_string()])?;
            user_db_id
        };
        let headers = create_headers_for_user(user_db_id).await?;

        let Json(genres) = list_genres(
            headers,
            Query(GenreListQuery {
                inc: None,
                query: None,
                library_id: None,
                sort_by: None,
                sort_order: None,
                page: Default::default(),
            }),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert_eq!(genres.items.len(), 1);
        assert_eq!(genres.items[0].name, "Rock");
        assert!(genres.next_cursor.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn get_genre_requires_an_accessible_release() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let (user_db_id, visible_genre_id, hidden_genre_id) = {
            let mut db = STATE.db.write().await;
            let user_db_id = lyra_db::users::create(
                &mut db,
                &lyra_db::fixtures::test_user("genre-detail-user")?,
            )?;
            let visible_library = insert_library(
                &mut db,
                "Visible Genre Detail",
                "/tmp/lyra-visible-genre-detail",
            )?;
            let hidden_library = insert_library(
                &mut db,
                "Hidden Genre Detail",
                "/tmp/lyra-hidden-genre-detail",
            )?;
            let visible_release = insert_test_release(&mut db, "Visible Genre Detail Release")?;
            let hidden_release = insert_test_release(&mut db, "Hidden Genre Detail Release")?;
            connect(&mut db, visible_library, visible_release)?;
            connect(&mut db, hidden_library, hidden_release)?;
            lyra_db::libraries::grant_access(
                &mut *db,
                user_db_id,
                visible_library,
                lyra_db::libraries::AccessKind::ReadWrite,
            )?;
            lyra_db::genres::sync_release_genres(
                &mut *db,
                visible_release,
                &["Visible Detail Genre".to_string()],
            )?;
            lyra_db::genres::sync_release_genres(
                &mut *db,
                hidden_release,
                &["Hidden Detail Genre".to_string()],
            )?;
            let visible_genre_id = lyra_db::genres::get_for_release(&*db, visible_release)?
                .into_iter()
                .find(|genre| genre.name == "Visible Detail Genre")
                .ok_or_else(|| anyhow::anyhow!("visible genre missing"))?
                .id;
            let hidden_genre_id = lyra_db::genres::get_for_release(&*db, hidden_release)?
                .into_iter()
                .find(|genre| genre.name == "Hidden Detail Genre")
                .ok_or_else(|| anyhow::anyhow!("hidden genre missing"))?
                .id;
            (user_db_id, visible_genre_id, hidden_genre_id)
        };
        let headers = create_headers_for_user(user_db_id).await?;

        let Json(visible_genre) = get_genre(
            headers.clone(),
            Path(visible_genre_id),
            Query(GenreQuery { inc: None }),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert_eq!(visible_genre.name, "Visible Detail Genre");

        let hidden_result = get_genre(
            headers,
            Path(hidden_genre_id),
            Query(GenreQuery { inc: None }),
        )
        .await;
        let Err(err) = hidden_result else {
            return Err(anyhow::anyhow!("hidden genre detail should be opaque"));
        };
        assert_eq!(err.into_response().status(), StatusCode::NOT_FOUND);
        Ok(())
    }

    #[tokio::test]
    async fn list_genres_includes_random_cover_when_listens_are_weak() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let (user_db_id, cover_id) = {
            let mut db = STATE.db.write().await;
            let user_db_id = lyra_db::users::create(
                &mut db,
                &lyra_db::fixtures::test_user("genre-random-cover")?,
            )?;
            let library = insert_library(&mut db, "Genre Random", "/tmp/lyra-genre-random")?;
            let release = insert_test_release(&mut db, "Random Rock")?;
            connect(&mut db, library, release)?;
            lyra_db::libraries::grant_access(
                &mut *db,
                user_db_id,
                library,
                lyra_db::libraries::AccessKind::ReadWrite,
            )?;
            lyra_db::genres::sync_release_genres(&mut *db, release, &["Rock".to_string()])?;
            let cover_id = insert_cover_for_release(&mut db, release, "genre-random-cover")?.id;
            (user_db_id, cover_id)
        };
        let headers = create_headers_for_user(user_db_id).await?;

        let Json(genres) = list_genres(
            headers,
            Query(GenreListQuery {
                inc: Some(vec!["covers".to_string()]),
                query: None,
                library_id: None,
                sort_by: None,
                sort_order: None,
                page: Default::default(),
            }),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert_eq!(genres.items.len(), 1);
        let cover = genres.items[0]
            .cover
            .as_ref()
            .and_then(|cover| cover.as_ref())
            .ok_or_else(|| anyhow::anyhow!("expected random cover"))?;
        assert_eq!(cover.id, cover_id);
        Ok(())
    }

    #[tokio::test]
    async fn list_genres_prefers_personal_cover_when_user_signal_is_enough() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let (user_db_id, expected_cover_id) = {
            let mut db = STATE.db.write().await;
            let user_db_id = lyra_db::users::create(
                &mut db,
                &lyra_db::fixtures::test_user("genre-personal-cover")?,
            )?;
            let library = insert_library(&mut db, "Genre Personal", "/tmp/lyra-genre-personal")?;
            let release_a = insert_test_release(&mut db, "Personal A")?;
            let release_b = insert_test_release(&mut db, "Personal B")?;
            let track_a = insert_track(&mut db, "Personal A Track")?;
            let track_b = insert_track(&mut db, "Personal B Track")?;
            connect(&mut db, library, release_a)?;
            connect(&mut db, library, release_b)?;
            connect(&mut db, release_a, track_a)?;
            connect(&mut db, release_b, track_b)?;
            lyra_db::libraries::grant_access(
                &mut *db,
                user_db_id,
                library,
                lyra_db::libraries::AccessKind::ReadWrite,
            )?;
            lyra_db::genres::sync_release_genres(&mut *db, release_a, &["Rock".to_string()])?;
            lyra_db::genres::sync_release_genres(&mut *db, release_b, &["Rock".to_string()])?;
            insert_cover_for_release(&mut db, release_a, "genre-personal-a")?;
            let expected_cover_id =
                insert_cover_for_release(&mut db, release_b, "genre-personal-b")?.id;
            lyra_db::fixtures::insert_listen(&mut db, user_db_id, track_b, 1_000)?;
            lyra_db::fixtures::insert_listen(&mut db, user_db_id, track_b, 2_000)?;
            lyra_db::fixtures::insert_listen(&mut db, user_db_id, track_b, 3_000)?;
            (user_db_id, expected_cover_id)
        };
        let headers = create_headers_for_user(user_db_id).await?;

        let Json(genres) = list_genres(
            headers,
            Query(GenreListQuery {
                inc: Some(vec!["covers".to_string()]),
                query: None,
                library_id: None,
                sort_by: None,
                sort_order: None,
                page: Default::default(),
            }),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        let cover = genres.items[0]
            .cover
            .as_ref()
            .and_then(|cover| cover.as_ref())
            .ok_or_else(|| anyhow::anyhow!("expected personal cover"))?;
        assert_eq!(cover.id, expected_cover_id);
        Ok(())
    }

    #[tokio::test]
    async fn list_genres_uses_instance_cover_when_user_signal_is_weak() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let (request_user_db_id, expected_cover_id) = {
            let mut db = STATE.db.write().await;
            let request_user_db_id = lyra_db::users::create(
                &mut db,
                &lyra_db::fixtures::test_user("genre-instance-request")?,
            )?;
            let library = insert_library(&mut db, "Genre Instance", "/tmp/lyra-genre-instance")?;
            let release_a = insert_test_release(&mut db, "Instance A")?;
            let release_b = insert_test_release(&mut db, "Instance B")?;
            let track_a = insert_track(&mut db, "Instance A Track")?;
            let track_b = insert_track(&mut db, "Instance B Track")?;
            connect(&mut db, library, release_a)?;
            connect(&mut db, library, release_b)?;
            connect(&mut db, release_a, track_a)?;
            connect(&mut db, release_b, track_b)?;
            lyra_db::libraries::grant_access(
                &mut *db,
                request_user_db_id,
                library,
                lyra_db::libraries::AccessKind::ReadWrite,
            )?;
            lyra_db::genres::sync_release_genres(&mut *db, release_a, &["Rock".to_string()])?;
            lyra_db::genres::sync_release_genres(&mut *db, release_b, &["Rock".to_string()])?;
            insert_cover_for_release(&mut db, release_a, "genre-instance-a")?;
            let expected_cover_id =
                insert_cover_for_release(&mut db, release_b, "genre-instance-b")?.id;

            for user_idx in 0..3 {
                let user_db_id = lyra_db::users::create(
                    &mut db,
                    &lyra_db::fixtures::test_user(&format!("genre-instance-listener-{user_idx}"))?,
                )?;
                for listen_idx in 0..4 {
                    lyra_db::fixtures::insert_listen(
                        &mut db,
                        user_db_id,
                        track_b,
                        1_000 + (user_idx * 10 + listen_idx) as u64,
                    )?;
                }
            }

            (request_user_db_id, expected_cover_id)
        };
        let headers = create_headers_for_user(request_user_db_id).await?;

        let Json(genres) = list_genres(
            headers,
            Query(GenreListQuery {
                inc: Some(vec!["covers".to_string()]),
                query: None,
                library_id: None,
                sort_by: None,
                sort_order: None,
                page: Default::default(),
            }),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        let cover = genres.items[0]
            .cover
            .as_ref()
            .and_then(|cover| cover.as_ref())
            .ok_or_else(|| anyhow::anyhow!("expected instance cover"))?;
        assert_eq!(cover.id, expected_cover_id);
        Ok(())
    }

    #[tokio::test]
    async fn list_genres_does_not_leak_hidden_display_cover() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let user_db_id = {
            let mut db = STATE.db.write().await;
            let user_db_id = lyra_db::users::create(
                &mut db,
                &lyra_db::fixtures::test_user("genre-hidden-cover")?,
            )?;
            let visible_library = insert_library(
                &mut db,
                "Genre Visible Covers",
                "/tmp/lyra-genre-visible-covers",
            )?;
            let hidden_library = insert_library(
                &mut db,
                "Genre Hidden Covers",
                "/tmp/lyra-genre-hidden-covers",
            )?;
            let visible_release = insert_test_release(&mut db, "Visible Rock")?;
            let hidden_release = insert_test_release(&mut db, "Hidden Rock")?;
            connect(&mut db, visible_library, visible_release)?;
            connect(&mut db, hidden_library, hidden_release)?;
            lyra_db::libraries::grant_access(
                &mut *db,
                user_db_id,
                visible_library,
                lyra_db::libraries::AccessKind::ReadWrite,
            )?;
            lyra_db::genres::sync_release_genres(&mut *db, visible_release, &["Rock".to_string()])?;
            lyra_db::genres::sync_release_genres(&mut *db, hidden_release, &["Rock".to_string()])?;
            insert_cover_for_release(&mut db, hidden_release, "genre-hidden-cover")?;
            user_db_id
        };
        let headers = create_headers_for_user(user_db_id).await?;

        let Json(genres) = list_genres(
            headers,
            Query(GenreListQuery {
                inc: Some(vec!["covers".to_string()]),
                query: None,
                library_id: None,
                sort_by: None,
                sort_order: None,
                page: Default::default(),
            }),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert_eq!(genres.items.len(), 1);
        assert!(matches!(genres.items[0].cover, Some(None)));
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
    use crate::services::catalog::genres::GenreKey;
    use lyra_db::fixtures::insert_release as insert_test_release;
    use lyra_db::fixtures::{
        connect,
        insert_track,
        test_user,
    };
    use lyra_db::test_db::new_test_db;

    struct GenreSortBench {
        db: DbAny,
        viewer: catalog::Viewer,
    }

    fn seed_genre_sort_bench(
        genre_count: usize,
        releases_per_genre: usize,
        tracks_per_release: usize,
        listens_per_track: usize,
    ) -> GenreSortBench {
        let mut db = new_test_db().unwrap();
        let user_db_id =
            lyra_db::users::create(&mut db, &test_user("genre-sort-bench").unwrap()).unwrap();
        for genre_idx in 0..genre_count {
            let genre_name = format!("Genre {genre_idx:04}");
            for release_idx in 0..releases_per_genre {
                let release_db_id = insert_test_release(
                    &mut db,
                    &format!("Genre {genre_idx:04} Release {release_idx:02}"),
                )
                .unwrap();
                lyra_db::genres::sync_release_genres(
                    &mut db,
                    release_db_id,
                    std::slice::from_ref(&genre_name),
                )
                .unwrap();
                for track_idx in 0..tracks_per_release {
                    let track_db_id = insert_track(
                        &mut db,
                        &format!(
                            "Genre {genre_idx:04} Release {release_idx:02} Track {track_idx:02}"
                        ),
                    )
                    .unwrap();
                    for listen_idx in 0..listens_per_track {
                        lyra_db::fixtures::insert_listen(
                            &mut db,
                            user_db_id,
                            track_db_id,
                            ((genre_idx * releases_per_genre * tracks_per_release)
                                + (release_idx * tracks_per_release)
                                + track_idx
                                + listen_idx) as u64
                                * 1_000,
                        )
                        .unwrap();
                    }
                    connect(&mut db, release_db_id, track_db_id).unwrap();
                }
            }
        }

        let principal = crate::services::auth::Principal::for_user(
            &db,
            user_db_id,
            vec![lyra_db::Permission::Admin],
            Default::default(),
        );
        let viewer = catalog::Viewer::user(&db, principal).unwrap();
        GenreSortBench { db, viewer }
    }

    fn bench_order(b: &mut Bencher, setup: &GenreSortBench, sort: catalog::SortSpec<GenreKey>) {
        let mut query = catalog::Query::<Genres>::new(GenreFilter::default());
        query.sort = sort;
        b.iter(|| catalog::order(&setup.db, &setup.viewer, black_box(&query)).unwrap());
    }

    #[bench]
    fn route_sort_genres_name_100(b: &mut Bencher) {
        let setup = seed_genre_sort_bench(100, 1, 0, 0);
        bench_order(b, &setup, Vec::new());
    }

    #[bench]
    fn route_sort_genres_track_count_100_genres_4000_tracks(b: &mut Bencher) {
        let setup = seed_genre_sort_bench(100, 5, 8, 0);
        bench_order(
            b,
            &setup,
            vec![(GenreKey::TrackCount, catalog::Direction::Descending)],
        );
    }

    #[bench]
    fn route_sort_genres_listen_count_100_genres_4000_listens(b: &mut Bencher) {
        let setup = seed_genre_sort_bench(100, 5, 8, 1);
        bench_order(
            b,
            &setup,
            vec![(GenreKey::ListenCount, catalog::Direction::Descending)],
        );
    }
}
