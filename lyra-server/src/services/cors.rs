// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use axum::{
    Router,
    http::HeaderValue,
};
use tower_http::cors::{
    AllowHeaders,
    AllowMethods,
    Any,
    CorsLayer,
};

use crate::config::Config;

pub(crate) fn apply(router: Router, config: &Config) -> Router {
    let origins = &config.cors.allowed_origins;
    if origins.is_empty() {
        return router;
    }

    let layer = CorsLayer::new()
        .allow_methods(AllowMethods::mirror_request())
        .allow_headers(AllowHeaders::mirror_request());

    let layer = if origins.iter().any(|origin| origin == "*") {
        layer.allow_origin(Any)
    } else {
        let origins = origins
            .iter()
            .map(|origin| {
                HeaderValue::from_str(origin)
                    .expect("cors.allowed_origins is validated while loading config")
            })
            .collect::<Vec<_>>();
        layer.allow_origin(origins)
    };

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
    use tower::ServiceExt;

    fn config_with_origins(origins: &[&str]) -> Config {
        let mut config = Config::default();
        config.cors.allowed_origins = origins.iter().map(|origin| origin.to_string()).collect();
        config
    }

    fn test_router(config: &Config) -> Router {
        apply(
            Router::new()
                .route("/ok", get(|| async { "ok" }))
                .route("/err", get(|| async { StatusCode::INTERNAL_SERVER_ERROR })),
            config,
        )
    }

    #[tokio::test]
    async fn empty_origin_list_leaves_cors_disabled() -> anyhow::Result<()> {
        let response = test_router(&Config::default())
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::GET)
                    .uri("/ok")
                    .header(ORIGIN, "http://localhost:8080")
                    .body(axum::body::Body::empty())?,
            )
            .await?;

        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response
                .headers()
                .get(ACCESS_CONTROL_ALLOW_ORIGIN)
                .is_none()
        );
        Ok(())
    }

    #[tokio::test]
    async fn allowed_origin_is_approved_for_normal_responses() -> anyhow::Result<()> {
        let config = config_with_origins(&["http://localhost:8080"]);
        let response = test_router(&config)
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
        let config = config_with_origins(&["http://localhost:8080"]);
        let response = test_router(&config)
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::GET)
                    .uri("/ok")
                    .header(ORIGIN, "http://evil.test")
                    .body(axum::body::Body::empty())?,
            )
            .await?;

        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response
                .headers()
                .get(ACCESS_CONTROL_ALLOW_ORIGIN)
                .is_none()
        );
        Ok(())
    }

    #[tokio::test]
    async fn wildcard_origin_is_explicit_allow_any() -> anyhow::Result<()> {
        let config = config_with_origins(&["*"]);
        let response = test_router(&config)
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::GET)
                    .uri("/ok")
                    .header(ORIGIN, "http://localhost:8080")
                    .body(axum::body::Body::empty())?,
            )
            .await?;

        assert_eq!(
            response
                .headers()
                .get(ACCESS_CONTROL_ALLOW_ORIGIN)
                .and_then(|value| value.to_str().ok()),
            Some("*")
        );
        Ok(())
    }

    #[tokio::test]
    async fn preflight_mirrors_requested_method_and_headers() -> anyhow::Result<()> {
        let config = config_with_origins(&["http://localhost:8080"]);
        let response = test_router(&config)
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
        let config = config_with_origins(&["http://localhost:8080"]);
        let response = test_router(&config)
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
