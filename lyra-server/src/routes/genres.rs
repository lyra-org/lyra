// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use aide::axum::{
    ApiRouter,
    routing::get_with,
};
use aide::transform::TransformOperation;
use axum::{
    Json,
    extract::Path,
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
        genres,
    },
    routes::{
        AppError,
        deserialize_inc,
        parse_inc_values,
    },
    services::auth::require_authenticated,
};

#[derive(Serialize, JsonSchema)]
struct GenreResponse {
    id: String,
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    parents: Option<Vec<GenreSummary>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    children: Option<Vec<GenreSummary>>,
}

#[derive(Serialize, JsonSchema)]
struct GenreSummary {
    id: String,
    name: String,
}

#[derive(Deserialize, JsonSchema)]
struct GenreQuery {
    #[schemars(description = "Comma-separated or repeated values: parents, children.")]
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

async fn list_genres(headers: HeaderMap) -> Result<Json<Vec<GenreResponse>>, AppError> {
    let _principal = require_authenticated(&headers).await?;

    let db = &*STATE.db.read().await;
    let all_genres = genres::get_all(db)?;

    let responses: Vec<GenreResponse> = all_genres
        .into_iter()
        .map(|genre| GenreResponse {
            id: genre.id,
            name: genre.name,
            parents: None,
            children: None,
        })
        .collect();

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

fn list_genres_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List genres").description("Returns all genres.")
}

fn get_genre_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get genre by ID")
        .description("Returns a single genre. Use `inc=parents,children` to include hierarchy.")
}

pub fn genre_routes() -> ApiRouter {
    ApiRouter::new()
        .api_route("/", get_with(list_genres, list_genres_docs))
        .api_route("/{id}", get_with(get_genre, get_genre_docs))
}
