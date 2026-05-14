// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
use aide::transform::TransformOperation;
use axum::{
    Json,
    extract::Path,
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
        labels,
    },
    routes::{
        self,
        AppError,
        deserialize_inc,
        parse_inc_values,
    },
    services::auth::require_authenticated,
};

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct LabelResponse {
    id: String,
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    releases: Option<Vec<LabelReleaseSummary>>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct LabelReleaseSummary {
    id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    catalog_number: Option<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct LabelQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Comma-separated or repeated values: releases.")
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    inc: Option<Vec<String>>,
}

struct LabelInc {
    releases: bool,
}

fn parse_label_inc(inc: Option<Vec<String>>) -> Result<LabelInc, AppError> {
    let values = parse_inc_values(inc, &["releases"])?;
    let mut result = LabelInc { releases: false };
    for value in values {
        if value == "releases" {
            result.releases = true;
        }
    }
    Ok(result)
}

async fn list_labels(headers: HeaderMap) -> Result<Json<Vec<LabelResponse>>, AppError> {
    let principal = require_authenticated(&headers).await?;

    let db = &*STATE.db.read().await;
    let all = labels::get_all(db)?;

    let mut responses = Vec::with_capacity(all.len());
    for label in all {
        let Some(label_db_id) = label.db_id.clone().map(agdb::DbId::from) else {
            continue;
        };
        let release_pairs = labels::get_releases_with_catalog(db, label_db_id)?;
        let mut has_accessible_release = false;
        for (release_db_id, _) in &release_pairs {
            if routes::entity_accessible_to_principal(db, &principal, *release_db_id)? {
                has_accessible_release = true;
                break;
            }
        }
        if !has_accessible_release {
            continue;
        }
        responses.push(LabelResponse {
            id: label.id,
            name: label.name,
            releases: None,
        });
    }

    Ok(Json(responses))
}

async fn get_label(
    headers: HeaderMap,
    Path(id): Path<String>,
    axum::extract::Query(query): axum::extract::Query<LabelQuery>,
) -> Result<Json<LabelResponse>, AppError> {
    let principal = require_authenticated(&headers).await?;
    let inc = parse_label_inc(query.inc)?;

    let db = &*STATE.db.read().await;
    let label_db_id = db::lookup::find_node_id_by_id(db, &id)?
        .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
    let label = labels::get_by_id(db, label_db_id)?
        .ok_or_else(|| AppError::not_found(format!("Label not found: {id}")))?;
    let pairs = labels::get_releases_with_catalog(db, label_db_id)?;
    let mut accessible_pairs = Vec::new();
    for (release_db_id, catalog_number) in pairs {
        if routes::entity_accessible_to_principal(db, &principal, release_db_id)? {
            accessible_pairs.push((release_db_id, catalog_number));
        }
    }
    if accessible_pairs.is_empty() {
        return Err(AppError::not_found(format!("Label not found: {id}")));
    }

    let releases = if inc.releases {
        let release_db_ids: Vec<_> = accessible_pairs.iter().map(|(id, _)| *id).collect();
        let release_ids_by_id = db::lookup::find_ids_by_db_ids(db, &release_db_ids)?;
        let summaries: Vec<LabelReleaseSummary> = accessible_pairs
            .into_iter()
            .filter_map(|(release_db_id, catalog_number)| {
                release_ids_by_id
                    .get(&release_db_id)
                    .cloned()
                    .map(|id| LabelReleaseSummary { id, catalog_number })
            })
            .collect();
        Some(summaries)
    } else {
        None
    };

    Ok(Json(LabelResponse {
        id: label.id,
        name: label.name,
        releases,
    }))
}

#[cfg(feature = "docgen")]
fn list_labels_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List labels")
        .description("Returns all record labels.")
}

#[cfg(feature = "docgen")]
fn get_label_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get label by ID").description(
        "Returns a single record label. Use `inc=releases` to include the releases linked to the label (with catalog numbers).",
    )
}

pub fn label_routes() -> Router {
    Router::new()
        .route("/", get(list_labels))
        .route("/{id}", get(get_label))
}

#[cfg(feature = "docgen")]
pub(crate) fn label_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::get_with;

    aide::axum::ApiRouter::new()
        .api_route("/", get_with(list_labels, list_labels_docs))
        .api_route("/{id}", get_with(get_label, get_label_docs))
}
