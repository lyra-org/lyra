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
        labels,
    },
    routes::{
        AppError,
        deserialize_inc,
        parse_inc_values,
    },
    services::auth::require_authenticated,
};

#[derive(Serialize, JsonSchema)]
struct LabelResponse {
    id: String,
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    releases: Option<Vec<LabelReleaseSummary>>,
}

#[derive(Serialize, JsonSchema)]
struct LabelReleaseSummary {
    id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    catalog_number: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct LabelQuery {
    #[schemars(description = "Comma-separated or repeated values: releases.")]
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
    let _principal = require_authenticated(&headers).await?;

    let db = &*STATE.db.read().await;
    let all = labels::get_all(db)?;

    let responses: Vec<LabelResponse> = all
        .into_iter()
        .map(|label| LabelResponse {
            id: label.id,
            name: label.name,
            releases: None,
        })
        .collect();

    Ok(Json(responses))
}

async fn get_label(
    headers: HeaderMap,
    Path(id): Path<String>,
    axum::extract::Query(query): axum::extract::Query<LabelQuery>,
) -> Result<Json<LabelResponse>, AppError> {
    let _principal = require_authenticated(&headers).await?;
    let inc = parse_label_inc(query.inc)?;

    let db = &*STATE.db.read().await;
    let label_db_id = db::lookup::find_node_id_by_id(db, &id)?
        .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
    let label = labels::get_by_id(db, label_db_id)?
        .ok_or_else(|| AppError::not_found(format!("Label not found: {id}")))?;

    let releases = if inc.releases {
        let pairs = labels::get_releases_with_catalog(db, label_db_id)?;
        let release_db_ids: Vec<_> = pairs.iter().map(|(id, _)| *id).collect();
        let release_ids_by_id = db::lookup::find_ids_by_db_ids(db, &release_db_ids)?;
        let summaries: Vec<LabelReleaseSummary> = pairs
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

fn list_labels_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List labels")
        .description("Returns all record labels.")
}

fn get_label_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get label by ID").description(
        "Returns a single record label. Use `inc=releases` to include the releases linked to the label (with catalog numbers).",
    )
}

pub fn label_routes() -> ApiRouter {
    ApiRouter::new()
        .api_route("/", get_with(list_labels, list_labels_docs))
        .api_route("/{id}", get_with(get_label, get_label_docs))
}
