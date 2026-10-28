// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashSet,
    future::IntoFuture,
    net::SocketAddr,
    time::Duration,
};

#[cfg(feature = "docgen")]
use aide::openapi::{
    OpenApi,
    ReferenceOr,
    SecurityScheme,
};
use anyhow::Result;
use axum::{
    Router,
    extract::DefaultBodyLimit,
    routing::get,
};
use tokio::{
    net::TcpListener,
    time::timeout,
};
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;

use crate::{
    plugins::api::RouteKey,
    services::settings::server::Config,
};

const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) struct CoreApi {
    pub(crate) router: Router,
    pub(crate) reservations: HashSet<RouteKey>,
}

pub(crate) fn build_core_api() -> Result<CoreApi> {
    let app = Router::new()
        .nest("/api/server", super::server_routes())
        .nest("/api/users", super::user_routes())
        .nest("/api/me", me_routes())
        .nest("/api/roles", super::role_routes())
        .nest("/api/libraries", super::library_routes())
        .nest("/api/covers", super::cover_routes())
        .nest("/api/releases", super::release_routes())
        .nest("/api/artists", super::artist_routes())
        .nest("/api/entries", super::entry_routes())
        .nest("/api/genres", super::genre_routes())
        .nest("/api/labels", super::label_routes())
        .nest("/api/metadata", super::metadata_routes())
        .nest("/api/tracks", super::track_routes())
        .nest("/api/listens", super::listen_routes())
        .nest("/api/playlists", super::playlist_routes())
        .nest("/api/stream", super::stream_routes())
        .nest("/api/download", super::download_routes())
        .nest("/api/providers", super::provider_routes())
        .nest("/api/mixers", super::mixer_routes())
        .nest(
            "/api/entities",
            super::entity_routes().merge(super::entity_metadata_routes()),
        )
        .nest("/api/plugins", super::plugin_routes())
        .nest("/api/sync", super::sync_routes())
        .nest("/api/search", super::search_routes());

    let (app, ws_reserved) = super::install_websocket(app);

    let mut reservations = collect_core_routes()?;
    reservations.extend(ws_reserved);

    Ok(CoreApi {
        router: app,
        reservations,
    })
}

#[cfg(feature = "docgen")]
pub(crate) fn build_openapi_spec() -> OpenApi {
    let mut api = OpenApi::default();

    let _ = aide::axum::ApiRouter::new()
        .nest("/api/server", super::server::server_openapi_routes())
        .nest("/api/users", super::users::user_openapi_routes())
        .nest("/api/me", me_openapi_routes())
        .nest("/api/roles", super::roles::role_openapi_routes())
        .nest("/api/libraries", super::libraries::library_openapi_routes())
        .nest("/api/covers", super::covers::cover_openapi_routes())
        .nest("/api/releases", super::releases::release_openapi_routes())
        .nest("/api/artists", super::artists::artist_openapi_routes())
        .nest("/api/entries", super::entries::entry_openapi_routes())
        .nest("/api/genres", super::genres::genre_openapi_routes())
        .nest("/api/labels", super::labels::label_openapi_routes())
        .nest("/api/metadata", super::metadata::metadata_openapi_routes())
        .nest("/api/tracks", super::tracks::track_openapi_routes())
        .nest("/api/listens", super::listens::listen_openapi_routes())
        .nest(
            "/api/playlists",
            super::playlists::playlist_openapi_routes(),
        )
        .nest("/api/stream", super::serve::stream_openapi_routes())
        .nest("/api/download", super::serve::download_openapi_routes())
        .nest(
            "/api/providers",
            super::providers::provider_openapi_routes(),
        )
        .nest("/api/mixers", super::mixers::mixer_openapi_routes())
        .nest(
            "/api/entities",
            super::providers::entity_openapi_routes()
                .merge(super::entity_metadata::entity_metadata_openapi_routes()),
        )
        .nest("/api/plugins", super::plugins::plugin_openapi_routes())
        .nest("/api/sync", super::sync::sync_openapi_routes())
        .nest("/api/search", super::search::search_openapi_routes())
        .finish_api(&mut api);

    configure_rest_openapi(&mut api);
    api
}

fn me_routes() -> Router {
    Router::new()
        .merge(super::users::me_account_routes())
        .route("/mix", get(super::mix::get_me_mix))
        .route("/listens", get(super::listens::get_me_listens))
        .nest("/favorites", super::favorites::me_favorite_routes())
        .nest("/ratings", super::ratings::me_rating_routes())
        .nest("/tags", super::tags::me_tag_routes())
        .nest("/playbacks", super::playbacks::me_playback_routes())
        .nest("/plugins", super::plugins::me_plugin_settings_routes())
}

#[cfg(feature = "docgen")]
fn me_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::get_with;

    aide::axum::ApiRouter::new()
        .merge(super::users::me_account_openapi_routes())
        .api_route(
            "/mix",
            get_with(super::mix::get_me_mix, super::mix::me_mix_docs),
        )
        .api_route(
            "/listens",
            get_with(
                super::listens::get_me_listens,
                super::listens::get_me_listens_docs,
            ),
        )
        .nest("/favorites", super::favorites::me_favorite_openapi_routes())
        .nest("/ratings", super::ratings::me_rating_openapi_routes())
        .nest("/tags", super::tags::me_tag_openapi_routes())
        .nest("/playbacks", super::playbacks::me_playback_openapi_routes())
        .nest(
            "/plugins",
            super::plugins::me_plugin_settings_openapi_routes(),
        )
}

// Sharply below axum's 2MB default to bound CPU/RAM amplification on auth'd POSTs.
const REQUEST_BODY_LIMIT_BYTES: usize = 256 * 1024;

pub(crate) async fn serve_app(
    app: Router,
    config: &Config,
    listener: TcpListener,
    shutdown_token: CancellationToken,
) -> Result<()> {
    tracing::info!("listening on {}", listener.local_addr()?);
    let app = super::rate_limit::apply(app, config);
    let app = app.layer(DefaultBodyLimit::max(REQUEST_BODY_LIMIT_BYTES));
    let app = super::cors::apply(app);
    let app = app.layer(TraceLayer::new_for_http());
    let signal_token = shutdown_token.clone();
    let signal_handle = tokio::spawn(async move {
        shutdown_signal().await;
        signal_token.cancel();
    });

    let server = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_token.clone().cancelled_owned())
    .into_future();
    tokio::pin!(server);

    tokio::select! {
        result = &mut server => {
            signal_handle.abort();
            result?;
            return Ok(());
        }
        _ = shutdown_token.cancelled() => {}
    }

    match timeout(GRACEFUL_SHUTDOWN_TIMEOUT, &mut server).await {
        Ok(result) => result?,
        Err(_) => {
            tracing::warn!(
                timeout_ms = GRACEFUL_SHUTDOWN_TIMEOUT.as_millis() as u64,
                "server graceful drain deadline elapsed; forcing shutdown"
            );
        }
    }
    signal_handle.abort();

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();

    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to register SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = sigterm.recv() => {}
        }
    }

    #[cfg(not(unix))]
    ctrl_c.await.ok();

    tracing::info!("shutdown signal received, draining connections");
}

fn collect_core_routes() -> Result<HashSet<RouteKey>> {
    CORE_ROUTE_RESERVATIONS
        .iter()
        .map(|(method, path)| RouteKey::new(*method, *path))
        .collect()
}

#[cfg(feature = "docgen")]
const BEARER_AUTH_SCHEME: &str = "bearerAuth";

#[cfg(feature = "docgen")]
fn configure_rest_openapi(api: &mut OpenApi) {
    api.info.title = "Lyra Server REST API".to_string();
    api.info.version = env!("CARGO_PKG_VERSION").to_string();
    api.info.description = Some(
        "REST endpoints use bearer authentication with either a session token or an API key, \
         except for explicitly public setup, login, cover image, and HLS segment endpoints."
            .to_string(),
    );

    api.components
        .get_or_insert_with(Default::default)
        .security_schemes
        .insert(
            BEARER_AUTH_SCHEME.to_string(),
            ReferenceOr::Item(SecurityScheme::Http {
                scheme: "bearer".to_string(),
                bearer_format: Some("session token or API key".to_string()),
                description: Some(
                    "Use `Authorization: Bearer <token>`. The token may be a login session token \
                     or an API key, subject to endpoint credential restrictions."
                        .to_string(),
                ),
                extensions: Default::default(),
            }),
        );

    let Some(paths) = api.paths.as_mut() else {
        return;
    };

    for (path, item) in &mut paths.paths {
        let ReferenceOr::Item(item) = item else {
            continue;
        };
        let tag = path
            .strip_prefix("/api/")
            .and_then(|rest| rest.split('/').next())
            .filter(|tag| !tag.is_empty())
            .map(ToString::to_string);

        macro_rules! annotate {
            ($method:literal, $operation:expr) => {
                if let Some(operation) = $operation.as_mut() {
                    if let Some(tag) = &tag
                        && !operation.tags.iter().any(|existing| existing == tag)
                    {
                        operation.tags.push(tag.clone());
                    }

                    if !is_public_rest_operation($method, path)
                        && !operation
                            .security
                            .iter()
                            .any(|requirement| requirement.contains_key(BEARER_AUTH_SCHEME))
                    {
                        operation.security.push(
                            [(BEARER_AUTH_SCHEME.to_string(), Vec::new())]
                                .into_iter()
                                .collect(),
                        );
                    }
                }
            };
        }

        annotate!("GET", item.get);
        annotate!("PUT", item.put);
        annotate!("POST", item.post);
        annotate!("DELETE", item.delete);
        annotate!("PATCH", item.patch);
        annotate!("HEAD", item.head);
        annotate!("OPTIONS", item.options);
        annotate!("TRACE", item.trace);
    }
}

#[cfg(feature = "docgen")]
fn is_public_rest_operation(method: &str, path: &str) -> bool {
    matches!(
        (method, path),
        ("GET", "/api/server/public")
            | ("POST", "/api/users/login")
            | ("GET", "/api/covers/{id}")
            | ("GET", "/api/stream/hls/{session_id}/{segment}")
    )
}

const CORE_ROUTE_RESERVATIONS: &[(&str, &str)] = &[
    ("GET", "/api/server/public"),
    ("GET", "/api/server/settings"),
    ("PATCH", "/api/server/settings"),
    ("DELETE", "/api/server/settings"),
    ("GET", "/api/users/"),
    ("POST", "/api/users/"),
    ("DELETE", "/api/users/{user_id}"),
    ("PUT", "/api/users/{user_id}/password"),
    ("PUT", "/api/users/{user_id}/role"),
    ("POST", "/api/users/login"),
    ("GET", "/api/me/"),
    ("PATCH", "/api/me/"),
    ("GET", "/api/me/mix"),
    ("GET", "/api/me/api-keys"),
    ("POST", "/api/me/api-keys"),
    ("DELETE", "/api/me/api-keys/{api_key_id}"),
    ("GET", "/api/me/listens"),
    ("GET", "/api/me/favorites/"),
    ("POST", "/api/me/favorites/check"),
    ("GET", "/api/me/favorites/{target_id}"),
    ("PUT", "/api/me/favorites/{target_id}"),
    ("DELETE", "/api/me/favorites/{target_id}"),
    ("POST", "/api/me/ratings/check"),
    ("GET", "/api/me/ratings/{target_id}"),
    ("PUT", "/api/me/ratings/{target_id}"),
    ("DELETE", "/api/me/ratings/{target_id}"),
    ("GET", "/api/me/tags/"),
    ("POST", "/api/me/tags/"),
    ("GET", "/api/me/tags/{id}"),
    ("PATCH", "/api/me/tags/{id}"),
    ("DELETE", "/api/me/tags/{id}"),
    ("GET", "/api/me/tags/{id}/targets"),
    ("GET", "/api/me/tags/{id}/targets/{target_id}"),
    ("DELETE", "/api/me/tags/{id}/targets/{target_id}"),
    ("GET", "/api/me/playbacks/"),
    ("POST", "/api/me/playbacks/"),
    ("GET", "/api/me/playbacks/{id}"),
    ("DELETE", "/api/me/playbacks/{id}"),
    ("GET", "/api/me/playbacks/{id}/queue"),
    ("PUT", "/api/me/playbacks/{id}/queue"),
    ("POST", "/api/me/playbacks/{id}/progress"),
    ("GET", "/api/me/plugins/settings"),
    ("GET", "/api/me/plugins/{plugin_id}/settings"),
    ("PATCH", "/api/me/plugins/{plugin_id}/settings"),
    ("DELETE", "/api/me/plugins/{plugin_id}/settings"),
    ("GET", "/api/roles/"),
    ("POST", "/api/roles/"),
    ("PATCH", "/api/roles/{role_id}"),
    ("DELETE", "/api/roles/{role_id}"),
    ("GET", "/api/libraries/"),
    ("POST", "/api/libraries/"),
    ("PATCH", "/api/libraries/{id}"),
    ("DELETE", "/api/libraries/{id}"),
    ("POST", "/api/libraries/{id}/refresh"),
    ("GET", "/api/libraries/{id}/sync"),
    ("POST", "/api/libraries/{id}/sync"),
    ("GET", "/api/libraries/{id}/access"),
    ("POST", "/api/libraries/{id}/access"),
    ("DELETE", "/api/libraries/{id}/access/{user_id}"),
    ("GET", "/api/covers/{id}"),
    ("GET", "/api/releases/"),
    ("GET", "/api/releases/{id}"),
    ("GET", "/api/releases/{id}/mix"),
    ("POST", "/api/releases/{id}/covers/search"),
    ("GET", "/api/artists/"),
    ("GET", "/api/artists/{id}"),
    ("GET", "/api/artists/{id}/mix"),
    ("POST", "/api/artists/{id}/covers/search"),
    ("GET", "/api/entries/"),
    ("GET", "/api/entries/{id}"),
    ("GET", "/api/genres/"),
    ("GET", "/api/genres/{id}"),
    ("GET", "/api/genres/{id}/mix"),
    ("GET", "/api/labels/"),
    ("GET", "/api/labels/{id}"),
    ("GET", "/api/metadata/mapping"),
    ("PUT", "/api/metadata/mapping"),
    ("POST", "/api/metadata/mapping/preview"),
    ("GET", "/api/tracks/"),
    ("GET", "/api/tracks/{id}"),
    ("GET", "/api/tracks/{id}/mix"),
    ("POST", "/api/tracks/{id}/playback-url"),
    ("GET", "/api/tracks/{id}/lyrics"),
    ("GET", "/api/tracks/{id}/lyrics/candidates"),
    ("PUT", "/api/tracks/{id}/lyrics/personal"),
    ("DELETE", "/api/tracks/{id}/lyrics/personal"),
    ("PUT", "/api/tracks/{id}/lyrics/shared"),
    ("DELETE", "/api/tracks/{id}/lyrics/shared"),
    ("POST", "/api/tracks/{id}/lyrics/refresh"),
    ("GET", "/api/listens/"),
    ("GET", "/api/playlists/"),
    ("POST", "/api/playlists/"),
    ("GET", "/api/playlists/{id}"),
    ("GET", "/api/playlists/{id}/mix"),
    ("PATCH", "/api/playlists/{id}"),
    ("DELETE", "/api/playlists/{id}"),
    ("POST", "/api/playlists/{id}/tracks"),
    ("POST", "/api/playlists/{id}/tracks/remove"),
    ("PATCH", "/api/playlists/{id}/tracks/{entry_id}"),
    ("DELETE", "/api/playlists/{id}/tracks/{entry_id}"),
    ("GET", "/api/stream/{track_id}"),
    ("GET", "/api/stream/{track_id}/hls.m3u8"),
    ("GET", "/api/stream/hls/{session_id}/{segment}"),
    ("GET", "/api/download/{track_id}"),
    ("GET", "/api/providers/"),
    ("POST", "/api/providers/{provider_id}/search"),
    ("PATCH", "/api/providers/{provider_id}"),
    ("POST", "/api/providers/{provider_id}/sync"),
    ("GET", "/api/mixers/"),
    ("PATCH", "/api/mixers/{mixer_id}"),
    ("GET", "/api/entities/{id}/external-ids"),
    (
        "PUT",
        "/api/entities/{id}/external-ids/{provider_id}/{id_type}",
    ),
    ("PUT", "/api/entities/{id}/lock"),
    ("DELETE", "/api/entities/{id}/lock"),
    ("POST", "/api/entities/{id}/refresh"),
    ("GET", "/api/entities/{id}/metadata"),
    ("PATCH", "/api/entities/{id}/metadata"),
    ("POST", "/api/entities/{id}/metadata/preview"),
    ("GET", "/api/plugins/"),
    ("GET", "/api/plugins/settings"),
    ("POST", "/api/plugins/{plugin_id}/restart"),
    ("GET", "/api/plugins/{plugin_id}/settings"),
    ("PATCH", "/api/plugins/{plugin_id}/settings"),
    ("DELETE", "/api/plugins/{plugin_id}/settings"),
    ("GET", "/api/sync/runs/{run_id}"),
    ("GET", "/api/sync/runs/{run_id}/events"),
    ("POST", "/api/sync/runs/{run_id}/cancel"),
    ("GET", "/api/search/"),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_api_builds_and_reserves_runtime_routes_without_docs_endpoints() {
        let core_api = build_core_api().expect("build core API");

        assert!(
            core_api
                .reservations
                .contains(&RouteKey::new("GET", "/ws").expect("ws route key"))
        );
        assert!(core_api.reservations.contains(
            &RouteKey::new("POST", "/api/me/ratings/check").expect("ratings check route key")
        ));
        assert!(
            !core_api
                .reservations
                .contains(&RouteKey::new("GET", "/api/openapi.json").expect("openapi route key"))
        );
        assert!(
            !core_api
                .reservations
                .contains(&RouteKey::new("GET", "/api/asyncapi.json").expect("asyncapi route key"))
        );
    }

    #[cfg(feature = "docgen")]
    #[test]
    fn ratings_check_is_documented_and_authenticated() {
        let api = build_openapi_spec();
        let paths = api.paths.as_ref().expect("OpenAPI paths");
        let path = paths
            .paths
            .get("/api/me/ratings/check")
            .expect("ratings check path");
        let ReferenceOr::Item(path) = path else {
            panic!("ratings check path should be inline");
        };
        let operation = path.post.as_ref().expect("ratings check POST operation");
        assert!(
            operation
                .security
                .iter()
                .any(|requirement| requirement.contains_key(BEARER_AUTH_SCHEME)),
            "ratings check must require bearer auth",
        );
    }
}
