// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use aide::axum::{
    ApiRouter,
    routing::get_with,
};
use aide::transform::TransformOperation;
use axum::Json;
use schemars::JsonSchema;
use serde::Serialize;

use crate::{
    STATE,
    db,
};

use super::AppError;

#[derive(Serialize, JsonSchema)]
struct ServerInfoResponse {
    server_id: String,
    version: String,
    setup_complete: bool,
}

async fn get_server_info() -> Result<Json<ServerInfoResponse>, AppError> {
    let db = STATE.db.read().await;
    let info =
        db::server::get(&db)?.ok_or_else(|| AppError::not_found("server info not initialized"))?;

    let default_username = &STATE.config.get().auth.default_username;
    let setup_complete = db::roles::has_non_default_admin(&db, default_username)?;

    Ok(Json(ServerInfoResponse {
        server_id: info.id,
        version: env!("CARGO_PKG_VERSION").to_string(),
        setup_complete,
    }))
}

fn get_server_info_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get public server info")
        .description("Returns server identity and setup status.")
}

pub fn server_routes() -> ApiRouter {
    ApiRouter::new().api_route("/public", get_with(get_server_info, get_server_info_docs))
}
