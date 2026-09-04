// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use axum::Router;
use tower_http::cors::{
    AllowHeaders,
    AllowMethods,
    AllowOrigin,
    CorsLayer,
};

use crate::STATE;

/// Applies CORS with the origin list read from the current config on every
/// request, so `cors.allowed_origins` changes take effect without a restart.
/// An empty list approves nothing; `*` approves any origin.
///
/// The layer is always installed. With an empty list the predicate approves
/// nothing, so preflight requests are answered without an allow-origin header
/// and every response still carries `Vary`.
pub(crate) fn apply(router: Router) -> Router {
    let layer = CorsLayer::new()
        .allow_methods(AllowMethods::mirror_request())
        .allow_headers(AllowHeaders::mirror_request())
        .allow_origin(AllowOrigin::predicate(|origin, _| {
            STATE
                .config()
                .cors
                .allowed_origins
                .iter()
                .any(|allowed| allowed == "*" || allowed.as_bytes() == origin.as_bytes())
        }));

    router.layer(layer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::to_bytes,
        http::{
            Method,
            StatusCode,
            header::{
                ACCESS_CONTROL_ALLOW_HEADERS,
                ACCESS_CONTROL_ALLOW_METHODS,
                ACCESS_CONTROL_ALLOW_ORIGIN,
                ACCESS_CONTROL_REQUEST_HEADERS,
                ACCESS_CONTROL_REQUEST_METHOD,
                ORIGIN,
                VARY,
            },
        },
        routing::get,
    };
    use tokio::sync::MutexGuard;
    use tower::ServiceExt;

    use crate::config::Config;
    use crate::testing::{
        init_default_test_state,
        publish_config,
        runtime_test_lock,
    };

    /// Publishes `origins` as the live CORS list; hold the guard for the
    /// rest of the test.
    async fn publish_origins(origins: &[&str]) -> anyhow::Result<MutexGuard<'static, ()>> {
        let guard = runtime_test_lock().await;
        init_default_test_state()?;
        publish_config(config_with_origins(origins));
        Ok(guard)
    }

    fn config_with_origins(origins: &[&str]) -> Config {
        let mut config = Config::for_tests();
        config.cors.allowed_origins = origins.iter().map(|origin| origin.to_string()).collect();
        config
    }

    fn test_router() -> Router {
        apply(
            Router::new()
                .route("/ok", get(|| async { "ok" }))
                .route("/err", get(|| async { StatusCode::INTERNAL_SERVER_ERROR })),
        )
    }

    async fn allowed_origin_for(router: &Router, origin: &str) -> anyhow::Result<Option<String>> {
        let response = router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::GET)
                    .uri("/ok")
                    .header(ORIGIN, origin)
                    .body(axum::body::Body::empty())?,
            )
            .await?;
        Ok(response
            .headers()
            .get(ACCESS_CONTROL_ALLOW_ORIGIN)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string))
    }

    #[tokio::test]
    async fn empty_origin_list_leaves_cors_disabled() -> anyhow::Result<()> {
        let _guard = publish_origins(&[]).await?;
        assert_eq!(
            allowed_origin_for(&test_router(), "http://localhost:8080").await?,
            None
        );
        Ok(())
    }

    #[tokio::test]
    async fn allowed_origin_is_approved_for_normal_responses() -> anyhow::Result<()> {
        let _guard = publish_origins(&["http://localhost:8080"]).await?;
        let response = test_router()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::GET)
                    .uri("/ok")
                    .header(ORIGIN, "http://localhost:8080")
                    .body(axum::body::Body::empty())?,
            )
            .await?;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(ACCESS_CONTROL_ALLOW_ORIGIN)
                .and_then(|value| value.to_str().ok()),
            Some("http://localhost:8080")
        );
        assert!(
            response
                .headers()
                .get(VARY)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.contains("origin"))
        );
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"ok");
        Ok(())
    }

    #[tokio::test]
    async fn disallowed_origin_is_not_approved() -> anyhow::Result<()> {
        let _guard = publish_origins(&["http://localhost:8080"]).await?;
        assert_eq!(
            allowed_origin_for(&test_router(), "http://evil.test").await?,
            None
        );
        Ok(())
    }

    #[tokio::test]
    async fn wildcard_origin_allows_any_origin() -> anyhow::Result<()> {
        let _guard = publish_origins(&["*"]).await?;
        assert_eq!(
            allowed_origin_for(&test_router(), "http://localhost:8080").await?,
            Some("http://localhost:8080".to_string())
        );
        Ok(())
    }

    #[tokio::test]
    async fn published_config_changes_apply_without_rebuilding_router() -> anyhow::Result<()> {
        let _guard = publish_origins(&["http://localhost:8080"]).await?;
        let router = test_router();
        assert_eq!(
            allowed_origin_for(&router, "http://localhost:8080").await?,
            Some("http://localhost:8080".to_string())
        );
        assert_eq!(allowed_origin_for(&router, "http://app.test").await?, None);

        publish_config(config_with_origins(&["http://app.test"]));
        assert_eq!(
            allowed_origin_for(&router, "http://localhost:8080").await?,
            None
        );
        assert_eq!(
            allowed_origin_for(&router, "http://app.test").await?,
            Some("http://app.test".to_string())
        );

        publish_config(config_with_origins(&[]));
        assert_eq!(allowed_origin_for(&router, "http://app.test").await?, None);
        Ok(())
    }

    #[tokio::test]
    async fn preflight_mirrors_requested_method_and_headers() -> anyhow::Result<()> {
        let _guard = publish_origins(&["http://localhost:8080"]).await?;
        let response = test_router()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::OPTIONS)
                    .uri("/ok")
                    .header(ORIGIN, "http://localhost:8080")
                    .header(ACCESS_CONTROL_REQUEST_METHOD, "PATCH")
                    .header(ACCESS_CONTROL_REQUEST_HEADERS, "authorization, x-client")
                    .body(axum::body::Body::empty())?,
            )
            .await?;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(ACCESS_CONTROL_ALLOW_ORIGIN)
                .and_then(|value| value.to_str().ok()),
            Some("http://localhost:8080")
        );
        assert_eq!(
            response
                .headers()
                .get(ACCESS_CONTROL_ALLOW_METHODS)
                .and_then(|value| value.to_str().ok()),
            Some("PATCH")
        );
        assert_eq!(
            response
                .headers()
                .get(ACCESS_CONTROL_ALLOW_HEADERS)
                .and_then(|value| value.to_str().ok()),
            Some("authorization, x-client")
        );
        Ok(())
    }

    #[tokio::test]
    async fn cors_headers_are_added_to_error_responses() -> anyhow::Result<()> {
        let _guard = publish_origins(&["http://localhost:8080"]).await?;
        let response = test_router()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::GET)
                    .uri("/err")
                    .header(ORIGIN, "http://localhost:8080")
                    .body(axum::body::Body::empty())?,
            )
            .await?;

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            response
                .headers()
                .get(ACCESS_CONTROL_ALLOW_ORIGIN)
                .and_then(|value| value.to_str().ok()),
            Some("http://localhost:8080")
        );
        Ok(())
    }
}
