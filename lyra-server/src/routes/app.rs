// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashSet,
    sync::Arc,
};

use aide::{
    axum::ApiRouter,
    openapi::{
        OpenApi,
        ReferenceOr,
        SecurityScheme,
    },
};
use anyhow::Result;
use axum::{
    Json,
    Router,
    routing::get,
};

use super::registry::RouteKey;

pub(crate) struct CoreApi {
    pub(crate) router: Router,
    pub(crate) reservations: HashSet<RouteKey>,
}

pub(crate) fn build_core_api() -> Result<CoreApi> {
    let mut api = OpenApi::default();

    let app = ApiRouter::new()
        .nest("/api/server", super::server_routes())
        .nest("/api/users", super::user_routes())
        .nest("/api/me", super::me_routes())
        .nest("/api/roles", super::role_routes())
        .nest("/api/libraries", super::library_routes())
        .nest("/api/releases", super::release_routes())
        .nest("/api/artists", super::artist_routes())
        .nest("/api/entries", super::entry_routes())
        .nest("/api/genres", super::genre_routes())
        .nest("/api/labels", super::label_routes())
        .nest("/api/favorites", super::favorite_routes())
        .nest("/api/metadata", super::metadata_routes())
        .nest("/api/tags", super::tag_routes())
        .nest("/api/tracks", super::track_routes())
        .nest("/api/playback-sessions", super::playback_session_routes())
        .nest("/api/listens", super::listen_routes())
        .nest("/api/playlists", super::playlist_routes())
        .nest("/api/stream", super::stream_routes())
        .nest("/api/download", super::download_routes())
        .nest("/api/providers", super::provider_routes())
        .nest("/api/entities", super::entity_routes())
        .nest("/api/plugins", super::plugin_routes())
        .nest("/api/mix", super::mix_routes())
        .nest("/api/search", super::search_routes())
        .finish_api(&mut api);
    configure_rest_openapi(&mut api);

    let (app, ws_reserved) = super::install_websocket(app);

    let api = Arc::new(api);
    let app = app.route(
        "/api/openapi.json",
        get({
            let api = api.clone();
            move || async move { Json(api.as_ref().clone()) }
        }),
    );

    let mut reservations = collect_core_routes(api.as_ref())?;
    reservations.extend(ws_reserved);

    Ok(CoreApi {
        router: app,
        reservations,
    })
}

fn collect_core_routes(api: &OpenApi) -> Result<HashSet<RouteKey>> {
    const METHODS: &[&str] = &["get", "post", "put", "patch", "delete", "head", "options"];

    let mut routes = HashSet::new();
    let serialized = serde_json::to_value(api)?;
    let paths = serialized
        .get("paths")
        .and_then(serde_json::Value::as_object)
        .cloned()
        .unwrap_or_default();

    for (path, item) in paths {
        let Some(item) = item.as_object() else {
            continue;
        };
        for method in METHODS {
            if item.contains_key(*method) {
                routes.insert(RouteKey::new(method.to_ascii_uppercase(), &path)?);
            }
        }
    }

    routes.insert(RouteKey::new("GET", "/api/openapi.json")?);
    Ok(routes)
}

const BEARER_AUTH_SCHEME: &str = "bearerAuth";

fn configure_rest_openapi(api: &mut OpenApi) {
    api.info.title = "Lyra Server REST API".to_string();
    api.info.version = env!("CARGO_PKG_VERSION").to_string();
    api.info.description = Some(
        "REST endpoints use bearer authentication with either a session token or an API key, \
         except for explicitly public setup/login endpoints."
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

fn is_public_rest_operation(method: &str, path: &str) -> bool {
    matches!(
        (method, path),
        ("GET", "/api/server/public") | ("POST", "/api/users/login")
    )
}
