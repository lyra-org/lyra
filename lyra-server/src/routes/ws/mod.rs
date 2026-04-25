// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashSet;
use std::sync::Arc;

use axum::{
    Router,
    extract::{
        Query,
        WebSocketUpgrade,
    },
    http::HeaderMap,
    response::Response,
    routing::get,
};
use serde::Deserialize;
use tokio::sync::Notify;

use crate::services::{
    auth,
    origin,
    remote::{
        connection,
        constants::MAX_MESSAGE_SIZE,
        registry,
    },
};

use super::AppError;
use super::registry as route_registry;

#[derive(Deserialize)]
struct WsQuery {
    token: Option<String>,
    session_key: String,
}

async fn ws_upgrade(
    headers: HeaderMap,
    Query(query): Query<WsQuery>,
    ws: WebSocketUpgrade,
) -> Result<Response, AppError> {
    let resolved = auth::resolve_auth_from_bearer(query.token.as_deref())
        .await?
        .ok_or_else(|| AppError::unauthorized("invalid or missing token"))?;

    if matches!(resolved.credential, auth::AuthCredential::ApiKey { .. }) {
        return Err(AppError::forbidden(
            "api key credentials are not accepted on the websocket upgrade; use a session token",
        ));
    }

    let principal = resolved.into_principal();

    let session_key = query.session_key.trim().to_string();
    if session_key.is_empty() {
        return Err(AppError::bad_request("session_key must be non-empty"));
    }

    origin::validate(&headers).map_err(AppError::forbidden)?;

    let token = query.token.clone();
    let user_db_id = principal.user_db_id;

    Ok(ws
        .max_message_size(MAX_MESSAGE_SIZE)
        .on_upgrade(move |socket| async move {
            let cancel = Arc::new(Notify::new());
            let result =
                match registry::register(user_db_id, session_key.clone(), cancel.clone()).await {
                    Ok(result) => result,
                    Err(err) => {
                        tracing::warn!(
                            user_db_id = user_db_id.0,
                            session_key = %session_key,
                            error = %err,
                            "websocket registration rejected"
                        );
                        return;
                    }
                };

            if let Some(evicted) = &result.evicted {
                tracing::info!(
                    evicted_connection_id = evicted.connection_id,
                    session_key = %evicted.session_key,
                    "evicted duplicate connection"
                );
            }

            tracing::info!(
                connection_id = result.connection_id,
                user_db_id = user_db_id.0,
                session_key = %session_key,
                "websocket connected"
            );

            connection::run(
                socket,
                result.connection_id,
                user_db_id,
                cancel,
                token,
                result.command_rx,
            )
            .await;
        }))
}

async fn asyncapi_spec() -> axum::Json<asyncapi_rust::AsyncApiSpec> {
    use crate::services::remote::messages::WsApiSpec;
    axum::Json(WsApiSpec::asyncapi_spec())
}

fn ws_route() -> Router {
    Router::new()
        .route("/ws", get(ws_upgrade))
        .route("/api/asyncapi.json", get(asyncapi_spec))
}

pub(crate) fn install(app: Router) -> (Router, HashSet<route_registry::RouteKey>) {
    let app = app.merge(ws_route());
    let reserved: HashSet<route_registry::RouteKey> = [
        route_registry::RouteKey::new("GET", "/ws").expect("core ws route key"),
        route_registry::RouteKey::new("GET", "/api/asyncapi.json")
            .expect("core asyncapi route key"),
    ]
    .into_iter()
    .collect();
    (app, reserved)
}
