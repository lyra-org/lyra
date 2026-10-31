// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbAny,
    DbId,
};
#[cfg(feature = "docgen")]
use aide::transform::TransformOperation;
use axum::{
    Json,
    extract::Path,
    http::HeaderMap,
};
use serde::{
    Deserialize,
    Serialize,
};

use crate::routes::request::bearer_credential;
use crate::services::releases::chains::{
    self,
    PublicChain,
    ReplaceChainsError,
};
use crate::{
    routes::AppError,
    services::{
        STATE,
        auth::{
            Principal,
            require_authenticated,
            require_permission,
        },
    },
};
use lyra_db::Permission;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
pub struct ReleaseChainsResponse {
    pub chains: Vec<ReleaseChainResponse>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
pub struct ReleaseChainResponse {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Opaque chain ID; it may change whenever the chains are replaced.")
    )]
    pub id: String,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Public IDs of the chain's tracks, in play order.")
    )]
    pub track_ids: Vec<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
pub(super) struct ReplaceReleaseChainsRequest {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Every chain the release keeps, each the public IDs of its tracks in play order."
        )
    )]
    chains: Vec<Vec<String>>,
}

/// The release `id` names, if it is one the principal can see.
fn visible_release(db: &DbAny, principal: &Principal, id: &str) -> Result<DbId, AppError> {
    let not_found = || AppError::not_found(format!("Release not found: {id}"));
    let release_db_id = lyra_db::lookup::find_node_id_by_id(db, id)?.ok_or_else(not_found)?;
    crate::services::auth::access::require_entity_accessible(
        db,
        principal,
        release_db_id,
        not_found,
    )?;
    lyra_db::releases::get_by_id(db, release_db_id)?.ok_or_else(not_found)?;
    Ok(release_db_id)
}

fn chains_response(chains: Vec<PublicChain>) -> ReleaseChainsResponse {
    ReleaseChainsResponse {
        chains: chains
            .into_iter()
            .map(|chain| ReleaseChainResponse {
                id: chain.id,
                track_ids: chain.track_ids,
            })
            .collect(),
    }
}

pub(super) async fn get_release_chains(
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<ReleaseChainsResponse>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    let db = &*STATE.db.read().await;
    let release_db_id = visible_release(db, &principal, &id)?;
    let chains = lyra_db::chains::get_for_release(db, release_db_id)?;
    Ok(Json(chains_response(chains::public_chains(db, chains)?)))
}

pub(super) async fn replace_release_chains(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<ReplaceReleaseChainsRequest>,
) -> Result<Json<ReleaseChainsResponse>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    require_permission(&principal, Permission::ManageMetadata)?;
    let mut db = STATE.db.write().await;
    let release_db_id = visible_release(&db, &principal, &id)?;

    let chains =
        chains::replace(&mut db, &principal, release_db_id, &request.chains).map_err(|error| {
            match error {
                ReplaceChainsError::ReleaseNotFound => {
                    AppError::not_found(format!("Release not found: {id}"))
                }
                ReplaceChainsError::TrackNotOnRelease(track_id) => {
                    AppError::bad_request(format!("track {track_id} is not on release {id}"))
                }
                ReplaceChainsError::Internal(error) => error.into(),
                error => AppError::bad_request(error.to_string()),
            }
        })?;
    Ok(Json(chains_response(chains)))
}

#[cfg(feature = "docgen")]
pub(super) fn get_release_chains_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List release chains").description(
        "Returns the release's track chains as `{ chains }`, ordered by where each chain's first track sits on the release. A chain contains tracks from one release that Lyra keeps together and in order when Lyra decides the play order, such as in random track sorts and mixes. 404 if the release is not found or not visible.",
    )
}

#[cfg(feature = "docgen")]
pub(super) fn replace_release_chains_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Replace release chains").description(
        "Replaces every chain on the release with the request body's `chains`, each listing the public IDs of its tracks in play order, and returns the stored chains as `GET` does. `{ \"chains\": [] }` clears them. Chain IDs may change on every replace. Responds 400 when a chain has fewer than two tracks, a track is listed more than once, a track is not on the release, or a track is already in a chain on another release, and 404 if the release is not found or not visible. Requires ManageMetadata.",
    )
}

#[cfg(test)]
mod tests {
    use axum::{
        Router,
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
    use nanoid::nanoid;
    use serde_json::{
        Value,
        json,
    };
    use tower::ServiceExt;

    use crate::{
        plugins::testing::init_library_test_state,
        routes::{
            releases::release_routes,
            tracks::track_routes,
        },
        services::{
            STATE,
            testing::runtime_test_lock,
        },
    };
    use lyra_db::{
        fixtures::{
            insert_release,
            insert_track,
        },
        test_db::insert_library,
    };

    struct Fixture {
        routes: Router,
        manager: String,
        viewer: String,
        outsider: String,
        release: String,
        tracks: Vec<String>,
        other_track: String,
        hidden_track: String,
    }

    async fn send(
        routes: &Router,
        authorization: &str,
        method: &str,
        uri: String,
        body: Option<Value>,
    ) -> anyhow::Result<(StatusCode, Value)> {
        let request = Request::builder()
            .method(method)
            .uri(uri)
            .header(AUTHORIZATION, authorization);
        let request = match body {
            Some(body) => request
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&body)?))?,
            None => request.body(Body::empty())?,
        };
        let response = routes.clone().oneshot(request).await?;
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        Ok((
            status,
            serde_json::from_slice(&body)
                .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&body).into_owned())),
        ))
    }

    async fn authorization(user_db_id: agdb::DbId) -> anyhow::Result<String> {
        let session =
            crate::services::testing::create_session(user_db_id, Default::default()).await?;
        Ok(format!("Bearer {}", session.token))
    }

    /// Urn, whose "Libera" parts make a chain, beside another release in the same library.
    async fn fixture() -> anyhow::Result<Fixture> {
        init_library_test_state(std::path::PathBuf::from(".")).await?;
        let (users, release, tracks, other_track, hidden_track) = {
            let mut db = STATE.db.write().await;
            lyra_db::roles::ensure_builtin_roles(&mut db)?;
            let role = format!("chain-manager-{}", nanoid!());
            lyra_db::roles::create(
                &mut db,
                &lyra_db::roles::Role {
                    db_id: None,
                    id: nanoid!(),
                    name: role.clone(),
                    permissions: vec![lyra_db::Permission::ManageMetadata],
                },
            )?;
            let library = insert_library(&mut db, "Chains", "/tmp/lyra-route-chains")?;
            let mut users = Vec::new();
            for name in ["chain-manager", "chain-viewer", "chain-outsider"] {
                users.push(lyra_db::users::create(
                    &mut db,
                    &lyra_db::fixtures::test_user(name)?,
                )?);
            }
            lyra_db::roles::ensure_user_has_role(&mut db, users[0], &role)?;
            for &user in &users[..2] {
                lyra_db::libraries::grant_access(
                    &mut db,
                    user,
                    library,
                    lyra_db::libraries::AccessKind::ReadWrite,
                )?;
            }

            let public_id = |db: &agdb::DbAny, id| -> anyhow::Result<String> {
                Ok(lyra_db::lookup::find_id_by_db_id(db, id)?.expect("public id"))
            };
            let urn = insert_release(&mut db, "Urn")?;
            let portal = insert_release(&mut db, "Portal of I")?;
            let mut tracks = Vec::new();
            for (number, title) in [
                "Libera (Part I) – Saturnine Spheres",
                "Libera (Part II) – Ascent of Burning Moths",
                "Eyrie",
                "Urn (Part I) – And Within the Void We Are Breathless",
            ]
            .into_iter()
            .enumerate()
            {
                let track_db_id = insert_track(&mut db, title)?;
                let mut track = lyra_db::tracks::get_by_id(&*db, track_db_id)?.expect("track");
                track.disc = Some(1);
                track.track = Some(number as u32 + 1);
                lyra_db::tracks::update(&mut db, &track)?;
                lyra_db::releases::link_track(&mut *db, urn, track_db_id)?;
                tracks.push(public_id(&db, track_db_id)?);
            }
            let other = insert_track(&mut db, "Tapestry of the Starless Abstract")?;
            lyra_db::releases::link_track(&mut *db, portal, other)?;
            for release in [urn, portal] {
                lyra_db::graph::ensure_owned_edge(&mut *db, library, release)?;
            }
            let private =
                insert_library(&mut db, "Private Chains", "/tmp/lyra-route-chains-private")?;
            let sealed = insert_release(&mut db, "Sealed")?;
            let hidden = insert_track(&mut db, "Sealed Track")?;
            lyra_db::releases::link_track(&mut *db, sealed, hidden)?;
            lyra_db::graph::ensure_owned_edge(&mut *db, private, sealed)?;
            (
                users,
                public_id(&db, urn)?,
                tracks,
                public_id(&db, other)?,
                public_id(&db, hidden)?,
            )
        };
        Ok(Fixture {
            routes: Router::new()
                .nest("/releases", release_routes())
                .nest("/tracks", track_routes()),
            manager: authorization(users[0]).await?,
            viewer: authorization(users[1]).await?,
            outsider: authorization(users[2]).await?,
            release,
            tracks,
            other_track,
            hidden_track,
        })
    }

    #[tokio::test]
    async fn replaced_chains_read_back_on_the_release_and_its_tracks() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let f = fixture().await?;
        let t = &f.tracks;
        let chains_uri = format!("/releases/{}/chains", f.release);

        let (status, empty) = send(&f.routes, &f.viewer, "GET", chains_uri.clone(), None).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(empty, json!({ "chains": [] }));

        let (status, put) = send(
            &f.routes,
            &f.manager,
            "PUT",
            chains_uri.clone(),
            Some(json!({ "chains": [[t[3], t[2]], [t[0], t[1]]] })),
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        let chains = put["chains"].as_array().expect("chains");
        assert_eq!(chains.len(), 2);
        assert_eq!(chains[0]["track_ids"], json!([t[0], t[1]]));
        assert_eq!(chains[1]["track_ids"], json!([t[3], t[2]]));
        let libera = chains[0]["id"].as_str().expect("chain id").to_string();

        let (status, get) = send(&f.routes, &f.viewer, "GET", chains_uri.clone(), None).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(get, put);

        let (_, track) = send(
            &f.routes,
            &f.viewer,
            "GET",
            format!("/tracks/{}", t[1]),
            None,
        )
        .await?;
        assert_eq!(track["chain"], json!({ "id": libera, "position": 1 }));
        let (_, release) = send(
            &f.routes,
            &f.viewer,
            "GET",
            format!("/releases/{}?inc=tracks", f.release),
            None,
        )
        .await?;
        let positions = release["tracks"]
            .as_array()
            .expect("release tracks")
            .iter()
            .map(|track| (track["id"].clone(), track["chain"]["position"].clone()))
            .collect::<Vec<_>>();
        assert!(positions.contains(&(json!(t[0]), json!(0))));
        assert!(positions.contains(&(json!(t[2]), json!(1))));
        let (_, listed) = send(
            &f.routes,
            &f.viewer,
            "GET",
            format!("/tracks?release_id={}", f.release),
            None,
        )
        .await?;
        let listed = listed["items"].as_array().expect("tracks");
        assert_eq!(listed.len(), 4);
        assert!(listed.iter().all(|track| track.get("chain").is_some()));

        let (status, cleared) = send(
            &f.routes,
            &f.manager,
            "PUT",
            chains_uri.clone(),
            Some(json!({ "chains": [] })),
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(cleared, json!({ "chains": [] }));
        let (_, track) = send(
            &f.routes,
            &f.viewer,
            "GET",
            format!("/tracks/{}", t[1]),
            None,
        )
        .await?;
        assert!(track.get("chain").is_none(), "{track}");
        Ok(())
    }

    #[tokio::test]
    async fn chain_writes_check_permission_visibility_and_invariants() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let f = fixture().await?;
        let t = &f.tracks;
        let chains_uri = format!("/releases/{}/chains", f.release);
        let body = |chains: Value| Some(json!({ "chains": chains }));

        let (status, _) = send(
            &f.routes,
            &f.viewer,
            "PUT",
            chains_uri.clone(),
            body(json!([[t[0], t[1]]])),
        )
        .await?;
        assert_eq!(status, StatusCode::FORBIDDEN);

        for chains in [
            json!([[t[0]]]),
            json!([[t[0], t[0]]]),
            json!([[t[0], t[1]], [t[1], t[2]]]),
            json!([[t[0], f.other_track]]),
            json!([[t[0], "no-such-track"]]),
        ] {
            let (status, error) = send(
                &f.routes,
                &f.manager,
                "PUT",
                chains_uri.clone(),
                body(chains.clone()),
            )
            .await?;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{chains}: {error}");
        }
        let (_, stored) = send(&f.routes, &f.viewer, "GET", chains_uri.clone(), None).await?;
        assert_eq!(stored, json!({ "chains": [] }));

        // A track the caller can't see answers as one that doesn't exist, before the set's shape
        // is checked, so the error can't tell them apart.
        for missing in [f.hidden_track.as_str(), "no-such-track"] {
            let (status, error) = send(
                &f.routes,
                &f.manager,
                "PUT",
                chains_uri.clone(),
                body(json!([[missing]])),
            )
            .await?;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{missing}: {error}");
            assert_eq!(
                error,
                json!(format!(
                    "Error: track {missing} is not on release {}",
                    f.release
                )),
            );
        }

        let (status, _) = send(&f.routes, &f.outsider, "GET", chains_uri.clone(), None).await?;
        assert_eq!(status, StatusCode::NOT_FOUND);
        for method in ["GET", "PUT"] {
            let (status, _) = send(
                &f.routes,
                &f.manager,
                method,
                "/releases/no-such-release/chains".to_string(),
                body(json!([])),
            )
            .await?;
            assert_eq!(status, StatusCode::NOT_FOUND, "{method}");
        }
        let (status, _) = send(
            &f.routes,
            &f.manager,
            "GET",
            format!("/releases/{}/chains", t[0]),
            None,
        )
        .await?;
        assert_eq!(status, StatusCode::NOT_FOUND, "a track is not a release");
        Ok(())
    }
}
