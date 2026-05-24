// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashSet;

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
    routing::get,
};
use serde::{
    Deserialize,
    Serialize,
};

use crate::{
    STATE,
    db::{
        self,
        genres,
    },
    routes::{
        AppError,
        deserialize_inc,
        parse_inc_values,
    },
    services::auth::require_authenticated,
};

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct GenreResponse {
    id: String,
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    parents: Option<Vec<GenreSummary>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    children: Option<Vec<GenreSummary>>,
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
        schemars(description = "Optional public library ID to scope returned genres.")
    )]
    library_id: Option<String>,
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
}

fn parse_genre_inc(inc: Option<Vec<String>>) -> Result<GenreInc, AppError> {
    let values = parse_inc_values(inc, &["parents", "children"])?;
    let mut result = GenreInc {
        parents: false,
        children: false,
    };
    for value in values {
        match value.as_str() {
            "parents" => result.parents = true,
            "children" => result.children = true,
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
    }
}

fn release_ids_for_library(db: &DbAny, library_db_id: DbId) -> anyhow::Result<Vec<DbId>> {
    Ok(db::releases::get_direct(db, library_db_id)?
        .into_iter()
        .filter_map(|release| release.db_id.map(DbId::from))
        .collect())
}

fn release_ids_for_accessible_libraries(
    db: &DbAny,
    accessible_library_ids: &HashSet<String>,
) -> anyhow::Result<Vec<DbId>> {
    let mut release_ids = Vec::new();
    let mut seen_release_ids = HashSet::new();
    for library_id in accessible_library_ids {
        let Some(library_db_id) = db::lookup::find_node_id_by_id(db, library_id)? else {
            continue;
        };
        if db::libraries::get_by_id(db, library_db_id)?.is_none() {
            continue;
        }
        for release_id in release_ids_for_library(db, library_db_id)? {
            if seen_release_ids.insert(release_id) {
                release_ids.push(release_id);
            }
        }
    }
    Ok(release_ids)
}

fn genres_for_release_ids(
    db: &impl db::DbAccess,
    release_ids: &[DbId],
) -> anyhow::Result<Vec<genres::Genre>> {
    let genres_by_release = genres::get_for_releases_many(db, release_ids)?;
    let mut scoped_genres = Vec::new();
    let mut seen_genre_ids = HashSet::new();

    for release_id in release_ids {
        let Some(release_genres) = genres_by_release.get(release_id) else {
            continue;
        };
        for genre in release_genres {
            let Some(genre_db_id) = genre.db_id.clone().map(DbId::from) else {
                continue;
            };
            if seen_genre_ids.insert(genre_db_id) {
                scoped_genres.push(genre.clone());
            }
        }
    }

    scoped_genres.sort_by(|a, b| {
        a.scan_name
            .cmp(&b.scan_name)
            .then_with(|| a.name.cmp(&b.name))
            .then_with(|| a.id.cmp(&b.id))
    });
    Ok(scoped_genres)
}

async fn list_genres(
    headers: HeaderMap,
    Query(query): Query<GenreListQuery>,
) -> Result<Json<Vec<GenreResponse>>, AppError> {
    let principal = require_authenticated(&headers).await?;

    let db = &*STATE.db.read().await;
    let library_scope =
        super::resolve_optional_library_filter(db, &principal, query.library_id.as_deref())?;
    let all_genres = match library_scope {
        Some(library_db_id) => {
            let release_ids = release_ids_for_library(db, library_db_id)?;
            genres_for_release_ids(db, &release_ids)?
        }
        None => {
            let release_ids =
                release_ids_for_accessible_libraries(db, &principal.accessible_library_ids)?;
            genres_for_release_ids(db, &release_ids)?
        }
    };

    let responses: Vec<GenreResponse> = all_genres.into_iter().map(genre_to_response).collect();

    Ok(Json(responses))
}

async fn get_genre(
    headers: HeaderMap,
    Path(id): Path<String>,
    axum::extract::Query(query): axum::extract::Query<GenreQuery>,
) -> Result<Json<GenreResponse>, AppError> {
    let _principal = require_authenticated(&headers).await?;
    let inc = parse_genre_inc(query.inc)?;

    let db = &*STATE.db.read().await;
    let genre_db_id = db::lookup::find_node_id_by_id(db, &id)?
        .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
    let genre = genres::get_by_id(db, genre_db_id)?
        .ok_or_else(|| AppError::not_found(format!("Genre not found: {id}")))?;

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

    Ok(Json(GenreResponse {
        id: genre.id,
        name: genre.name,
        parents,
        children,
    }))
}

#[cfg(feature = "docgen")]
fn list_genres_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List genres").description(
        "Returns genres attached to releases visible to the authenticated user. `library_id` scopes results to releases belonging to that public library ID.",
    )
}

#[cfg(feature = "docgen")]
fn get_genre_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get genre by ID")
        .description("Returns a single genre. Use `inc=parents,children` to include hierarchy.")
}

pub fn genre_routes() -> Router {
    Router::new()
        .route("/", get(list_genres))
        .route("/{id}", get(get_genre))
}

#[cfg(test)]
mod tests {
    use axum::http::{
        HeaderMap,
        header::AUTHORIZATION,
    };

    use crate::{
        db::test_db::{
            connect,
            insert_library,
            insert_release as insert_test_release,
        },
        services::auth::sessions,
        testing::{
            LibraryFixtureConfig,
            initialize_runtime,
            runtime_test_lock,
        },
    };

    use super::*;

    async fn setup_route_test() -> anyhow::Result<()> {
        initialize_runtime(&LibraryFixtureConfig {
            directory: std::path::PathBuf::from("."),
            language: None,
            country: None,
        })
        .await
    }

    async fn create_admin_headers(username: &str) -> anyhow::Result<HeaderMap> {
        let user_db_id = {
            let mut db = STATE.db.write().await;
            db::roles::ensure_builtin_roles(&mut db)?;
            let user_db_id = db::users::create(&mut db, &db::test_db::test_user(username)?)?;
            db::roles::ensure_user_has_role(&mut db, user_db_id, db::roles::BUILTIN_ADMIN_ROLE)?;
            user_db_id
        };

        create_headers_for_user(user_db_id).await
    }

    async fn create_headers_for_user(user_db_id: DbId) -> anyhow::Result<HeaderMap> {
        let session = sessions::create_session_for_user(user_db_id, Default::default()).await?;
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
            db::genres::sync_release_genres(&mut *db, visible_release, &["Rock".to_string()])?;
            db::genres::sync_release_genres(&mut *db, hidden_release, &["Jazz".to_string()])?;

            db::libraries::get_by_id(&db, visible_library)?
                .ok_or_else(|| anyhow::anyhow!("visible library missing"))?
                .id
        };
        let headers = create_admin_headers("genre-scope-admin").await?;

        let Json(genres) = list_genres(
            headers,
            Query(GenreListQuery {
                library_id: Some(visible_library_id),
            }),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert_eq!(genres.len(), 1);
        assert_eq!(genres[0].name, "Rock");
        Ok(())
    }

    #[tokio::test]
    async fn list_genres_without_library_id_uses_accessible_libraries() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let user_db_id = {
            let mut db = STATE.db.write().await;
            let user_db_id =
                db::users::create(&mut db, &db::test_db::test_user("genre-listener")?)?;
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
            db::libraries::grant_access(
                &mut *db,
                user_db_id,
                visible_library,
                db::libraries::AccessKind::ReadWrite,
            )?;
            db::genres::sync_release_genres(&mut *db, visible_release, &["Rock".to_string()])?;
            db::genres::sync_release_genres(&mut *db, hidden_release, &["Jazz".to_string()])?;
            user_db_id
        };
        let headers = create_headers_for_user(user_db_id).await?;

        let Json(genres) = list_genres(headers, Query(GenreListQuery { library_id: None }))
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert_eq!(genres.len(), 1);
        assert_eq!(genres[0].name, "Rock");
        Ok(())
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn genre_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::get_with;

    aide::axum::ApiRouter::new()
        .api_route("/", get_with(list_genres, list_genres_docs))
        .api_route("/{id}", get_with(get_genre, get_genre_docs))
}
