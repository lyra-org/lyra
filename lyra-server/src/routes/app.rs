// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashSet;

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
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::{
    plugins::api::RouteKey,
    services::settings::server::Config,
};

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
        .nest("/api/ratings", super::rating_routes())
        .nest("/api/playlists", super::playlist_routes())
        .nest("/api/stream", super::stream_routes())
        .nest("/api/download", super::download_routes())
        .nest("/api/providers", super::provider_routes())
        .nest("/api/mix", super::mix_routes())
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
        .nest("/api/ratings", super::ratings::rating_openapi_routes())
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
        .nest("/api/mix", super::mix::mix_openapi_routes())
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
        .route(
            "/listens",
            get(super::listens::get_me_listens).post(super::listens::submit_me_listens),
        )
        .nest("/favorites", super::favorites::me_favorite_routes())
        .nest("/ratings", super::ratings::me_rating_routes())
        .nest("/bookmarks", super::bookmarks::me_bookmark_routes())
        .nest("/tags", super::tags::me_tag_routes())
        .nest("/playbacks", super::playbacks::me_playback_routes())
        .nest("/connections", super::connections::me_connection_routes())
        .nest("/plugins", super::plugins::me_plugin_settings_routes())
}

#[cfg(feature = "docgen")]
fn me_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::get_with;

    aide::axum::ApiRouter::new()
        .merge(super::users::me_account_openapi_routes())
        .api_route(
            "/listens",
            get_with(
                super::listens::get_me_listens,
                super::listens::get_me_listens_docs,
            )
            .post_with(
                super::listens::submit_me_listens,
                super::listens::submit_me_listens_docs,
            ),
        )
        .nest("/favorites", super::favorites::me_favorite_openapi_routes())
        .nest("/ratings", super::ratings::me_rating_openapi_routes())
        .nest("/bookmarks", super::bookmarks::me_bookmark_openapi_routes())
        .nest("/tags", super::tags::me_tag_openapi_routes())
        .nest("/playbacks", super::playbacks::me_playback_openapi_routes())
        .nest(
            "/connections",
            super::connections::me_connection_openapi_routes(),
        )
        .nest(
            "/plugins",
            super::plugins::me_plugin_settings_openapi_routes(),
        )
}

// Sharply below axum's 2MB default to bound CPU/RAM amplification on auth'd POSTs.
pub(super) const REQUEST_BODY_LIMIT_BYTES: usize = 256 * 1024;

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
    let app = super::trace::apply(app);
    let signal_token = shutdown_token.clone();
    let signal_handle = tokio::spawn(async move {
        shutdown_signal().await;
        signal_token.cancel();
    });

    super::listener::serve(
        listener,
        app,
        super::listener::ConnectionLimits::for_process(),
        config.rate_limit.trusted_proxies.iter().copied(),
        shutdown_token,
    )
    .await;
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
    ("GET", "/api/me/api-keys"),
    ("POST", "/api/me/api-keys"),
    ("DELETE", "/api/me/api-keys/{api_key_id}"),
    ("GET", "/api/me/listens"),
    ("POST", "/api/me/listens"),
    ("POST", "/api/me/favorites/check"),
    ("GET", "/api/me/favorites/{target_id}"),
    ("PUT", "/api/me/favorites/{target_id}"),
    ("DELETE", "/api/me/favorites/{target_id}"),
    ("POST", "/api/me/ratings/check"),
    ("GET", "/api/me/ratings/{target_id}"),
    ("PUT", "/api/me/ratings/{target_id}"),
    ("DELETE", "/api/me/ratings/{target_id}"),
    ("POST", "/api/me/bookmarks/check"),
    ("GET", "/api/me/bookmarks/{track_id}"),
    ("PUT", "/api/me/bookmarks/{track_id}"),
    ("DELETE", "/api/me/bookmarks/{track_id}"),
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
    ("GET", "/api/me/connections/"),
    ("POST", "/api/me/connections/tickets"),
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
    ("POST", "/api/libraries/{id}/purge"),
    ("GET", "/api/libraries/{id}/sync"),
    ("POST", "/api/libraries/{id}/sync"),
    ("GET", "/api/libraries/{id}/access"),
    ("POST", "/api/libraries/{id}/access"),
    ("DELETE", "/api/libraries/{id}/access/{user_id}"),
    ("GET", "/api/covers/{id}"),
    ("GET", "/api/releases/"),
    ("GET", "/api/releases/{id}"),
    ("POST", "/api/releases/{id}/covers/search"),
    ("GET", "/api/releases/{id}/covers/candidates"),
    ("PUT", "/api/releases/{id}/covers/manual"),
    ("DELETE", "/api/releases/{id}/covers/manual"),
    ("PUT", "/api/releases/{id}/covers/provider"),
    ("PUT", "/api/releases/{id}/covers/selection"),
    ("DELETE", "/api/releases/{id}/covers/selection"),
    ("GET", "/api/artists/"),
    ("GET", "/api/artists/{id}"),
    ("POST", "/api/artists/{id}/covers/search"),
    ("GET", "/api/artists/{id}/covers/candidates"),
    ("PUT", "/api/artists/{id}/covers/manual"),
    ("DELETE", "/api/artists/{id}/covers/manual"),
    ("PUT", "/api/artists/{id}/covers/provider"),
    ("PUT", "/api/artists/{id}/covers/selection"),
    ("DELETE", "/api/artists/{id}/covers/selection"),
    ("GET", "/api/entries/"),
    ("GET", "/api/entries/{id}"),
    ("GET", "/api/genres/"),
    ("GET", "/api/genres/{id}"),
    ("GET", "/api/labels/"),
    ("GET", "/api/labels/{id}"),
    ("GET", "/api/metadata/mapping"),
    ("PUT", "/api/metadata/mapping"),
    ("POST", "/api/metadata/mapping/preview"),
    ("POST", "/api/metadata/changes"),
    ("POST", "/api/metadata/changes/preview"),
    ("GET", "/api/tracks/"),
    ("GET", "/api/tracks/{id}"),
    ("POST", "/api/tracks/{id}/playback-url"),
    ("POST", "/api/tracks/{id}/remap"),
    ("GET", "/api/tracks/{id}/lyrics"),
    ("GET", "/api/tracks/{id}/lyrics/candidates"),
    ("PUT", "/api/tracks/{id}/lyrics/personal"),
    ("DELETE", "/api/tracks/{id}/lyrics/personal"),
    ("PUT", "/api/tracks/{id}/lyrics/shared"),
    ("DELETE", "/api/tracks/{id}/lyrics/shared"),
    ("POST", "/api/tracks/{id}/lyrics/refresh"),
    ("GET", "/api/listens/"),
    ("POST", "/api/ratings/averages"),
    ("GET", "/api/playlists/"),
    ("POST", "/api/playlists/"),
    ("GET", "/api/playlists/{id}"),
    ("PATCH", "/api/playlists/{id}"),
    ("DELETE", "/api/playlists/{id}"),
    ("POST", "/api/playlists/{id}/tracks"),
    ("POST", "/api/playlists/{id}/tracks/remove"),
    ("PATCH", "/api/playlists/{id}/tracks/{entry_id}"),
    ("DELETE", "/api/playlists/{id}/tracks/{entry_id}"),
    ("GET", "/api/playlists/{id}/covers/candidates"),
    ("PUT", "/api/playlists/{id}/covers/manual"),
    ("DELETE", "/api/playlists/{id}/covers/manual"),
    ("GET", "/api/stream/{track_id}"),
    ("GET", "/api/stream/{track_id}/hls.m3u8"),
    ("GET", "/api/stream/hls/{session_id}/{segment}"),
    ("GET", "/api/download/{track_id}"),
    ("GET", "/api/providers/"),
    ("POST", "/api/providers/{provider_id}/search"),
    ("PATCH", "/api/providers/{provider_id}"),
    ("POST", "/api/providers/{provider_id}/sync"),
    ("POST", "/api/mix/"),
    ("GET", "/api/mixers/"),
    ("PATCH", "/api/mixers/{mixer_id}"),
    ("GET", "/api/entities/{id}/external-ids"),
    ("PUT", "/api/entities/{id}/lock"),
    ("DELETE", "/api/entities/{id}/lock"),
    ("POST", "/api/entities/{id}/refresh"),
    ("GET", "/api/entities/{id}/metadata"),
    ("GET", "/api/plugins/"),
    ("GET", "/api/plugins/settings"),
    ("POST", "/api/plugins/{plugin_id}/restart"),
    ("GET", "/api/plugins/{plugin_id}/settings"),
    ("PATCH", "/api/plugins/{plugin_id}/settings"),
    ("DELETE", "/api/plugins/{plugin_id}/settings"),
    ("GET", "/api/sync/events"),
    ("GET", "/api/sync/runs/{run_id}"),
    ("POST", "/api/sync/runs/{run_id}/cancel"),
    ("GET", "/api/sync/runs/{run_id}/failures"),
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
        for (method, path) in [
            ("PUT", "/api/releases/{id}/covers/manual"),
            ("DELETE", "/api/artists/{id}/covers/selection"),
            ("GET", "/api/playlists/{id}/covers/candidates"),
        ] {
            assert!(
                core_api
                    .reservations
                    .contains(&RouteKey::new(method, path).expect("cover route key")),
                "{method} {path}"
            );
        }
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
    fn rating_lookups_are_documented_and_authenticated() {
        let api = build_openapi_spec();
        let paths = api.paths.as_ref().expect("OpenAPI paths");
        for route in ["/api/me/ratings/check", "/api/ratings/averages"] {
            let path = paths.paths.get(route).expect(route);
            let ReferenceOr::Item(path) = path else {
                panic!("{route} should be inline");
            };
            let operation = path.post.as_ref().expect(route);
            assert!(
                operation
                    .security
                    .iter()
                    .any(|requirement| requirement.contains_key(BEARER_AUTH_SCHEME)),
                "{route} must require bearer auth",
            );
        }
    }

    #[cfg(feature = "docgen")]
    #[test]
    fn fixed_plugin_route_segments_are_reserved_plugin_ids() {
        let api = build_openapi_spec();
        let paths = api.paths.as_ref().expect("OpenAPI paths");
        for path in paths.paths.keys() {
            let Some(rest) = path
                .strip_prefix("/api/plugins/")
                .or_else(|| path.strip_prefix("/api/me/plugins/"))
            else {
                continue;
            };
            let segment = rest.split('/').next().unwrap_or_default();
            if segment.is_empty() || segment.starts_with('{') {
                continue;
            }
            assert!(
                super::super::RESERVED_PLUGIN_IDS.contains(&segment),
                "{path} shadows plugin id '{segment}', which is not reserved"
            );
        }
    }
}
