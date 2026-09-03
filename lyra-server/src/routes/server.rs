// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
use aide::transform::TransformOperation;
use axum::Json;
use axum::{
    Router,
    routing::get,
};
use serde::Serialize;

use crate::{
    STATE,
    db,
};

use super::AppError;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct ServerInfoResponse {
    server_id: String,
    version: String,
    published_url: Option<String>,
    setup_complete: bool,
}

async fn get_server_info() -> Result<Json<ServerInfoResponse>, AppError> {
    let db = STATE.db.read().await;
    let info =
        db::server::get(&db)?.ok_or_else(|| AppError::not_found("server info not initialized"))?;

    let config = STATE.config.get();
    let setup_complete = db::roles::has_non_default_admin(&db)?;

    Ok(Json(ServerInfoResponse {
        server_id: info.id,
        version: env!("CARGO_PKG_VERSION").to_string(),
        published_url: config.published_url.clone(),
        setup_complete,
    }))
}

#[cfg(feature = "docgen")]
fn get_server_info_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get public server info")
        .description("Returns server identity and setup status.")
}

pub fn server_routes() -> Router {
    Router::new().route("/public", get(get_server_info))
}

#[cfg(feature = "docgen")]
pub(crate) fn server_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::get_with;

    aide::axum::ApiRouter::new()
        .api_route("/public", get_with(get_server_info, get_server_info_docs))
}
