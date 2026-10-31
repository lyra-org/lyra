// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
use aide::transform::TransformOperation;
use axum::{
    Json,
    Router,
    http::HeaderMap,
    routing::get,
};

use crate::routes::request::bearer_credential;
use crate::{
    routes::AppError,
    services::{
        auth::require_principal,
        playback_sessions,
        remote::connections::{
            self,
            Connection,
        },
    },
};

async fn list_connections(headers: HeaderMap) -> Result<Json<Vec<Connection>>, AppError> {
    let principal = require_principal(bearer_credential(&headers)).await?;
    let now_ms = playback_sessions::now_ms()?;
    let connections = connections::list(&principal, now_ms)
        .await
        .map_err(anyhow::Error::from)?;
    Ok(Json(connections.iter().map(|c| c.resource()).collect()))
}

#[cfg(feature = "docgen")]
fn list_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List connections").description(
        "Returns the authenticated user's open WebSocket connections, at most eight, oldest first, including idle ones. `playback_id` is the playback a connection drives, or `null` when it is idle. Use `id` as a remote control command's `target`. A socket that lists connections after its `connected` event and applies the `connection_updated` and `connection_closed` events from then on stays current.",
    )
}

pub(super) fn me_connection_routes() -> Router {
    Router::new().route("/", get(list_connections))
}

#[cfg(feature = "docgen")]
pub(super) fn me_connection_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::get_with;

    aide::axum::ApiRouter::new().api_route("/", get_with(list_connections, list_docs))
}

#[cfg(test)]
mod tests {
    use axum::http::header::AUTHORIZATION;

    use super::*;
    use crate::services::STATE;
    use crate::services::remote::registry;

    #[tokio::test]
    async fn lists_the_callers_idle_connections() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (user_db_id, user_public_id, other_public_id) = {
            let mut db = STATE.db.write().await;
            let user = lyra_db::fixtures::test_user("connections-owner")?;
            let other = lyra_db::fixtures::test_user("connections-other")?;
            let user_db_id = lyra_db::users::create(&mut db, &user)?;
            lyra_db::users::create(&mut db, &other)?;
            (user_db_id, user.id, other.id)
        };
        let session =
            crate::services::testing::create_session(user_db_id, Default::default()).await?;
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, format!("Bearer {}", session.token).parse()?);
        let register = |user: String| {
            registry::register(
                user,
                Some("Speaker".to_string()),
                "speaker".to_string(),
                tokio::sync::watch::channel(None).0,
            )
        };
        let own = register(user_public_id)
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let foreign = register(other_public_id)
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;

        let Json(connections) = list_connections(headers)
            .await
            .map_err(|error| anyhow::anyhow!("{error:?}"))?;
        let json = serde_json::to_value(&connections)?;

        registry::unregister(own.connection_id).await;
        registry::unregister(foreign.connection_id).await;
        let items = json.as_array().expect("a list");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["id"], own.public_id.as_str());
        assert_eq!(items[0]["client_name"], "Speaker");
        assert_eq!(items[0]["supported_commands"], serde_json::json!([]));
        assert_eq!(items[0]["playback_id"], serde_json::Value::Null);
        assert_eq!(items[0]["remote_control_degraded"], false);
        assert!(items[0].get("session_key").is_none());
        Ok(())
    }
}
