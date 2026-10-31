// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
use aide::transform::TransformOperation;
use axum::{
    Json,
    Router,
    extract::{
        Query,
        rejection::JsonRejection,
    },
    http::HeaderMap,
};
use serde::Deserialize;

use crate::routes::request::bearer_credential;
use crate::{
    routes::{
        self,
        AppError,
        responses::{
            TrackChains,
            TrackResponse,
        },
        tracks as route_tracks,
    },
    services::STATE,
    services::{
        auth::{
            Principal,
            require_principal,
        },
        mix::{
            self,
            MixRequestFields,
            MixViewer,
        },
        tracks as track_service,
    },
};

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
pub(crate) struct MixQueryParams {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Comma-separated or repeated values: releases, artists, release_covers, artist_covers."
        )
    )]
    #[serde(default, deserialize_with = "routes::deserialize_inc")]
    inc: Option<Vec<String>>,
}

async fn post_mix(
    headers: HeaderMap,
    Query(query): Query<MixQueryParams>,
    body: Result<Json<MixRequestFields>, JsonRejection>,
) -> Result<Json<Vec<TrackResponse>>, AppError> {
    let principal = require_principal(bearer_credential(&headers)).await?;
    let Json(body) = body.map_err(|rejection| AppError::bad_request(rejection.body_text()))?;
    let includes = route_tracks::parse_inc(query.inc)?;
    let tracks = mix::mix(&body.into_request(), &MixViewer::for_principal(&principal)).await?;

    // Hydrated under a guard of its own, so tracks deleted or hidden since the mix drop here.
    let db = &*STATE.db.read().await;
    let tracks = filter_accessible_tracks(db, &principal, tracks)?;
    let details = track_service::list_details_for_tracks(db, includes.service, tracks)?;
    let chains = TrackChains::of(db, details.iter().map(|detail| &detail.track))?;
    let responses = details
        .into_iter()
        .map(|detail| route_tracks::track_detail_to_response(db, detail, includes, &chains))
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(Json(responses))
}

/// Keeps the mixer's tracks that still exist under their public id and that the principal can
/// reach, checked under the guard that hydrates them.
fn filter_accessible_tracks(
    db: &agdb::DbAny,
    principal: &Principal,
    tracks: Vec<lyra_db::Track>,
) -> anyhow::Result<Vec<lyra_db::Track>> {
    let mut filtered = Vec::with_capacity(tracks.len());
    for track in tracks {
        let Some(track_db_id) = track.db_id.clone().map(agdb::DbId::from) else {
            continue;
        };
        if lyra_db::lookup::find_node_id_by_id(db, &track.id)? == Some(track_db_id)
            && crate::services::auth::access::entity_accessible(db, principal, track_db_id)?
        {
            filtered.push(track);
        }
    }
    Ok(filtered)
}

#[cfg(feature = "docgen")]
fn post_mix_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Generate mix").description(
        "Returns tracks mixed from `seeds`, the caller's recent listens (`from_history`), or a free-text `q`. Seeds are public IDs of tracks, releases, artists, genres or playlists; `{ \"id\": ..., \"exclude\": true }` excludes a seed, mixing away from it, and a request needs at least one seed that isn't excluded, `from_history` or `q`. Every seed, excluded or not, must exist and be visible to the caller, or the request fails with 404.\n\nThe `blend` shape (default) returns tracks like the seeds. Tracks of one chain stay together and in chain order, and `pin_seed` leads with a single track seed. The `path` shape takes exactly two seeds, neither excluded, and returns the tracks leading from the first to the second in the mixer's order, up to 2000 tracks; tracks the caller can't see are left out, chains are not regrouped, and `limit` is only a step hint.\n\nEnabled mixers are tried in priority order, and only those whose declared handler takes the request's shape, seed types, explicit seed count, excluded seeds and query; the first non-empty result wins. `from_history` asks for track seeds, and the caller's recent listens fill each mixer's seeds beyond the explicit ones, most recent first. `GET /api/mixers` lists what each mixer and the built-in mixer take. Mixer `options` are keyed `<mixer_id>.<name>` and must have the option's declared type. `mixer` pins one mixer with no fallback. A request no mixer can serve fails with 400 naming what is unsupported.\n\nUse `inc` to hydrate nested data: `releases`, `artists`, `release_covers` (covers require `releases`), and `artist_covers` (covers require `artists`).",
    )
}

pub fn mix_routes() -> Router {
    use axum::routing::post;

    Router::new().route("/", post(post_mix))
}

#[cfg(feature = "docgen")]
pub(crate) fn mix_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::post_with;

    aide::axum::ApiRouter::new().api_route("/", post_with(post_mix, post_mix_docs))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use axum::{
        body::{
            Body,
            to_bytes,
        },
        http::{
            Request,
            StatusCode,
            header::{
                AUTHORIZATION,
                CONTENT_TYPE,
            },
        },
    };
    use lyra_db::{
        fixtures::{
            connect,
            insert_artist,
            insert_release,
            insert_track,
        },
        test_db::{
            connect_artist,
            insert_library,
        },
    };
    use tower::ServiceExt;

    use super::*;
    use crate::services::{
        mix::MixShape,
        testing::runtime_test_lock,
    };

    async fn post(
        authorization: &str,
        body: impl Into<Body>,
    ) -> anyhow::Result<(StatusCode, String)> {
        let response = mix_routes()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/")
                    .header(AUTHORIZATION, authorization)
                    .header(CONTENT_TYPE, "application/json")
                    .body(body.into())?,
            )
            .await?;
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        Ok((status, String::from_utf8(body.to_vec())?))
    }

    async fn post_json(
        authorization: &str,
        body: serde_json::Value,
    ) -> anyhow::Result<(StatusCode, String)> {
        post(authorization, body.to_string()).await
    }

    /// A user who sees only a visible library, an artist credited on a release in it and on one
    /// in a hidden library, and a track in the hidden one.
    struct Catalog {
        authorization: String,
        artist: String,
        hidden_track: String,
    }

    async fn catalog(_test_dir: &tempfile::TempDir) -> anyhow::Result<Catalog> {
        crate::plugins::testing::init_library_test_state(_test_dir.path().to_path_buf()).await?;
        let (user_db_id, artist, hidden_track) = {
            let mut db = STATE.db.write().await;
            lyra_db::roles::ensure_builtin_roles(&mut db)?;
            let user_db_id =
                lyra_db::users::create(&mut db, &lyra_db::fixtures::test_user("mix-viewer")?)?;
            let visible_library = insert_library(&mut db, "Visible Mix", "/tmp/lyra-visible-mix")?;
            let hidden_library = insert_library(&mut db, "Hidden Mix", "/tmp/lyra-hidden-mix")?;
            lyra_db::libraries::grant_access(
                &mut *db,
                user_db_id,
                visible_library,
                lyra_db::libraries::AccessKind::ReadWrite,
            )?;

            let artist = insert_artist(&mut db, "Release Credit Mix Artist")?;
            let visible_release = insert_release(&mut db, "Visible Mix Release")?;
            let hidden_release = insert_release(&mut db, "Hidden Mix Release")?;
            connect(&mut db, visible_library, visible_release)?;
            connect(&mut db, hidden_library, hidden_release)?;
            connect_artist(&mut db, visible_release, artist)?;
            connect_artist(&mut db, hidden_release, artist)?;

            for index in 0..55 {
                let track = insert_track(&mut db, &format!("Visible Track {index:02}"))?;
                connect(&mut db, visible_release, track)?;
            }
            let mut hidden_track = None;
            for index in 0..60 {
                let track = insert_track(&mut db, &format!("Hidden Track {index:02}"))?;
                connect(&mut db, hidden_release, track)?;
                hidden_track.get_or_insert(track);
            }
            let public_id = |id| {
                lyra_db::lookup::find_id_by_db_id(&*db, id)
                    .map(|id| id.expect("entity has a public id"))
            };
            (
                user_db_id,
                public_id(artist)?,
                public_id(hidden_track.expect("a hidden track"))?,
            )
        };
        let session =
            crate::services::testing::create_session(user_db_id, Default::default()).await?;
        Ok(Catalog {
            authorization: format!("Bearer {}", session.token),
            artist,
            hidden_track,
        })
    }

    #[tokio::test]
    async fn artist_blends_filter_release_tracks_before_shuffle_and_limit() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = tempfile::tempdir()?;
        let catalog = catalog(&test_dir).await?;

        for _ in 0..2 {
            let (status, tracks) = post_json(
                &catalog.authorization,
                serde_json::json!({ "seeds": [catalog.artist], "limit": 50 }),
            )
            .await?;
            assert_eq!(status, StatusCode::OK, "{tracks}");
            let tracks: serde_json::Value = serde_json::from_str(&tracks)?;
            let tracks = tracks.as_array().expect("mix response is an array");
            let ids = tracks
                .iter()
                .map(|track| {
                    let title = track["title"].as_str().expect("track title is a string");
                    assert!(
                        title.starts_with("Visible Track"),
                        "unexpected title: {title}"
                    );
                    track["id"]
                        .as_str()
                        .expect("track id is a string")
                        .to_string()
                })
                .collect::<HashSet<_>>();
            assert_eq!(tracks.len(), 50);
            assert_eq!(ids.len(), 50);
        }
        Ok(())
    }

    #[tokio::test]
    async fn mix_requests_fail_as_bad_requests_or_missing_seeds() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = tempfile::tempdir()?;
        let catalog = catalog(&test_dir).await?;
        let authorization = catalog.authorization.as_str();
        let (status, body) = post_json(authorization, serde_json::json!({})).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("needs a seed"), "{body}");

        let (status, _) = post(authorization, "{\"seeds\": 7}").await?;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) = post_json(
            authorization,
            serde_json::json!({ "seeds": [catalog.artist], "limit": 0 }),
        )
        .await?;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let (status, body) = post_json(
            authorization,
            serde_json::json!({ "seeds": [catalog.artist, catalog.artist], "shape": "path" }),
        )
        .await?;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("twice"), "{body}");

        let (status, _) = post_json(
            authorization,
            serde_json::json!({ "seeds": [catalog.hidden_track] }),
        )
        .await?;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) = post_json(
            authorization,
            serde_json::json!({
                "seeds": [catalog.artist, { "id": catalog.hidden_track, "exclude": true }],
            }),
        )
        .await?;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "a seed to push away from is checked too"
        );

        let (status, body) = post_json(
            authorization,
            serde_json::json!({ "seeds": [catalog.artist], "q": "rain" }),
        )
        .await?;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body, "Error: no mixer can serve this mix: query");
        Ok(())
    }

    fn body(json: serde_json::Value) -> serde_json::Result<MixRequestFields> {
        serde_json::from_value(json)
    }

    #[test]
    fn mix_bodies_take_bare_and_excluded_seeds() -> serde_json::Result<()> {
        let request = body(serde_json::json!({
            "seeds": ["a", { "id": "b", "exclude": true }, { "id": "c" }],
            "shape": "path",
            "q": "rain",
        }))?
        .into_request();

        let seeds = request
            .seeds
            .iter()
            .map(|seed| (seed.id.as_str(), seed.exclude))
            .collect::<Vec<_>>();
        assert_eq!(seeds, [("a", false), ("b", true), ("c", false)]);
        assert_eq!(request.shape, MixShape::Path);
        assert_eq!(request.query(), Some("rain"));
        Ok(())
    }

    #[test]
    fn mix_bodies_reject_unknown_fields() {
        assert!(body(serde_json::json!({ "seeds": ["a"], "instant": true })).is_err());
        assert!(body(serde_json::json!({ "seeds": [{ "id": "a", "kind": "track" }] })).is_err());
    }
}
