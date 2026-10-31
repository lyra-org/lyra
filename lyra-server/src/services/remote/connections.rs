// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! A user's WebSocket connections joined with the playback each one drives.

use std::collections::HashMap;

use agdb::{
    DbAny,
    DbId,
};
use serde::{
    Deserialize,
    Serialize,
};

use super::constants::RemoteAction;
use super::registry::{
    self,
    ConnectionId,
    ConnectionSnapshot,
};
use crate::services::STATE;
use crate::services::auth::Principal;
use crate::services::playback_sessions::{
    self,
    PlaybackScopeKey,
};
use crate::services::playbacks::{
    self,
    CurrentSession,
    PlaybackError,
};

const NATIVE_PLUGIN_ID: &str = "native";

/// One of the caller's open WebSocket connections.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Connection {
    /// Addresses the connection as a remote control `target`.
    pub(crate) id: String,
    pub(crate) client_name: Option<String>,
    /// The remote control commands the connection declared it accepts.
    pub(crate) supported_commands: Vec<RemoteAction>,
    /// The playback the connection is driving, or `null` when it is idle.
    pub(crate) playback_id: Option<String>,
    /// A command sent to the connection went unacknowledged by a progress report for too long.
    pub(crate) remote_control_degraded: bool,
    pub(crate) connected_at: String,
}

/// The playback a connection drives, with the track session it is reporting.
#[derive(Clone, Debug)]
pub(crate) struct ConnectionPlayback {
    pub(crate) playback_id: String,
    pub(crate) current: CurrentSession,
}

#[derive(Clone, Debug)]
pub(crate) struct UserConnection {
    pub(crate) id: String,
    pub(crate) session_key: String,
    pub(crate) client_name: Option<String>,
    pub(crate) supported_commands: Vec<RemoteAction>,
    pub(crate) playback: Option<ConnectionPlayback>,
    pub(crate) remote_control_degraded: bool,
    pub(crate) connected_at_ms: u64,
    connection_id: ConnectionId,
    bound_at_ms: u64,
}

impl UserConnection {
    pub(crate) fn resource(&self) -> Connection {
        Connection {
            id: self.id.clone(),
            client_name: self.client_name.clone(),
            supported_commands: self.supported_commands.clone(),
            playback_id: self
                .playback
                .as_ref()
                .map(|playback| playback.playback_id.clone()),
            remote_control_degraded: self.remote_control_degraded,
            connected_at: crate::services::timestamps::unix_ms_to_rfc3339_u64(self.connected_at_ms),
        }
    }
}

/// The principal's connections, oldest first, each with the playback it drives when the
/// principal can see that playback.
pub(crate) async fn list(
    principal: &Principal,
    now_ms: u64,
) -> Result<Vec<UserConnection>, PlaybackError> {
    let snapshots = registry::list_user_connections(&principal.user_public_id).await;
    let db = STATE.db.read().await;
    join(&db, principal, snapshots, now_ms)
}

/// Joins registry snapshots of the principal's connections with the playbacks their native
/// scopes are bound to. Snapshots of other users' connections are left out.
pub(crate) fn join(
    db: &DbAny,
    principal: &Principal,
    snapshots: Vec<ConnectionSnapshot>,
    now_ms: u64,
) -> Result<Vec<UserConnection>, PlaybackError> {
    let bound = snapshots
        .into_iter()
        .filter(|snapshot| snapshot.user_public_id == principal.user_public_id)
        .map(|snapshot| {
            let scope_key = scope_key(&snapshot);
            let scope = playback_sessions::get_playback_session(&scope_key);
            let degraded = playback_sessions::is_remote_control_degraded(&scope_key, now_ms);
            (snapshot, scope, degraded)
        })
        .collect::<Vec<_>>();

    let playback_by_session = if bound.iter().any(|(_, scope, _)| {
        scope
            .as_ref()
            .is_some_and(|s| s.current_playback_session_id.is_some())
    }) {
        visible_playbacks_by_current_session(db, principal, now_ms)?
    } else {
        HashMap::new()
    };

    let mut connections = Vec::with_capacity(bound.len());
    for (snapshot, scope, remote_control_degraded) in bound {
        let playback = match scope
            .as_ref()
            .and_then(|scope| scope.current_playback_session_id)
            .and_then(|session_id| Some((session_id, playback_by_session.get(&session_id)?)))
        {
            Some((session_id, (playback_db_id, playback_id))) => {
                playbacks::get_visible_detail(db, *playback_db_id, principal, now_ms)?
                    .and_then(|detail| detail.current_session)
                    .filter(|current| current.playback_session_id == session_id)
                    .map(|current| ConnectionPlayback {
                        playback_id: playback_id.clone(),
                        current,
                    })
            }
            None => None,
        };
        connections.push(UserConnection {
            id: snapshot.public_id,
            session_key: snapshot.session_key,
            client_name: snapshot.client_name,
            supported_commands: snapshot.supported_commands,
            playback,
            remote_control_degraded,
            connected_at_ms: snapshot.connected_at_ms,
            connection_id: snapshot.connection_id,
            bound_at_ms: scope.map_or(0, |scope| scope.updated_at_ms),
        });
    }
    connections.sort_by(|left, right| {
        left.connected_at_ms
            .cmp(&right.connected_at_ms)
            .then_with(|| left.connection_id.cmp(&right.connection_id))
    });
    Ok(connections)
}

/// The connection driving the playback. When several are bound to it, the one whose scope
/// changed last.
pub(crate) fn controller<'a>(
    connections: &'a [UserConnection],
    playback_id: &str,
) -> Option<&'a UserConnection> {
    connections
        .iter()
        .filter(|connection| {
            connection
                .playback
                .as_ref()
                .is_some_and(|playback| playback.playback_id == playback_id)
        })
        .max_by_key(|connection| (connection.bound_at_ms, connection.connection_id))
}

fn scope_key(snapshot: &ConnectionSnapshot) -> PlaybackScopeKey<'_> {
    PlaybackScopeKey {
        plugin_id: NATIVE_PLUGIN_ID,
        user_public_id: &snapshot.user_public_id,
        session_key: &snapshot.session_key,
    }
}

fn visible_playbacks_by_current_session(
    db: &DbAny,
    principal: &Principal,
    now_ms: u64,
) -> Result<HashMap<DbId, (DbId, String)>, PlaybackError> {
    let projections = playbacks::list_visible_projections(db, principal, false, now_ms)?;
    let playback_ids = projections
        .iter()
        .map(|projection| projection.db_id)
        .collect::<Vec<_>>();
    let current = lyra_db::playbacks::current_session_ids(db, &playback_ids)?;
    Ok(projections
        .into_iter()
        .filter_map(|projection| {
            let session_id = *current.get(&projection.db_id)?;
            Some((session_id, (projection.db_id, projection.id)))
        })
        .collect())
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::services::playback_sessions::PlaybackMutation;
    use crate::services::playbacks::QueueSnapshot;
    use crate::services::testing;
    use lyra_db::PlaybackState;
    use tokio::sync::watch;

    pub(in crate::services::remote) struct TestUser {
        pub(in crate::services::remote) db_id: DbId,
        pub(in crate::services::remote) principal: Principal,
    }

    pub(in crate::services::remote) async fn init() -> anyhow::Result<()> {
        testing::init_library_test_state(std::path::PathBuf::from(".")).await?;
        for connection in registry::list_connections().await {
            registry::unregister(connection.connection_id).await;
        }
        Ok(())
    }

    pub(in crate::services::remote) async fn user(name: &str) -> anyhow::Result<TestUser> {
        let db_id = {
            let mut db = STATE.db.write().await;
            let user = lyra_db::fixtures::test_user(&format!("{name}-{}", nanoid::nanoid!()))?;
            let db_id = lyra_db::users::create(&mut db, &user)?;
            lyra_db::roles::ensure_builtin_roles(&mut db)?;
            lyra_db::roles::ensure_user_has_role(
                &mut db,
                db_id,
                lyra_db::roles::BUILTIN_ADMIN_ROLE,
            )?;
            db_id
        };
        Ok(TestUser {
            db_id,
            principal: testing::user_principal(db_id).await?,
        })
    }

    pub(in crate::services::remote) async fn connect(
        user: &TestUser,
        session_key: &str,
    ) -> anyhow::Result<registry::RegisterResult> {
        registry::register(
            user.principal.user_public_id.clone(),
            Some(format!("{session_key} client")),
            session_key.to_string(),
            watch::channel(None).0,
        )
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))
    }

    pub(in crate::services::remote) fn scope<'a>(
        user: &'a TestUser,
        session_key: &'a str,
    ) -> PlaybackScopeKey<'a> {
        PlaybackScopeKey {
            plugin_id: NATIVE_PLUGIN_ID,
            user_public_id: &user.principal.user_public_id,
            session_key,
        }
    }

    /// Creates a playback for the user and binds the session key's native scope to it.
    pub(in crate::services::remote) async fn drive_new_playback(
        user: &TestUser,
        session_key: &str,
    ) -> anyhow::Result<(String, DbId)> {
        let now_ms = playback_sessions::now_ms()?;
        let mut db = STATE.db.write().await;
        let track_db_id = lyra_db::fixtures::insert_track(&mut db, "Track")?;
        let track_id =
            lyra_db::lookup::find_id_by_db_id(&*db, track_db_id)?.expect("track has a public id");
        let queue =
            playbacks::validate_queue(&*db, &user.principal, QueueSnapshot::single(track_id))?;
        let update = playbacks::create_playback(
            &mut db,
            playbacks::CreatePlaybackRequest {
                id: nanoid::nanoid!(),
                play_id: format!("connection-play-{}", nanoid::nanoid!(8)),
                user_db_id: user.db_id,
                client_name: None,
                queue,
                mutation: PlaybackMutation {
                    position_ms: Some(0),
                    duration_ms: Some(100_000),
                    state: Some(PlaybackState::Playing),
                },
                now_ms,
            },
        )?;
        drop(db);
        playback_sessions::bind_current_playback_session_scope(
            &scope(user, session_key),
            update.session.playback_session_id,
            update.session.playback_session_public_id.clone(),
            now_ms,
        );
        Ok((update.playback.id, update.session.playback_session_id))
    }

    #[tokio::test]
    async fn list_joins_each_connection_of_the_principal_with_its_playback() -> anyhow::Result<()> {
        let _guard = testing::runtime_test_lock().await;
        init().await?;
        let alice = user("alice").await?;
        let bob = user("bob").await?;
        let idle = connect(&alice, "idle").await?;
        let driving = connect(&alice, "driving").await?;
        let bobs = connect(&bob, "driving").await?;
        let (playback_id, _) = drive_new_playback(&alice, "driving").await?;

        let now_ms = playback_sessions::now_ms()?;
        let listed = list(&alice.principal, now_ms).await?;
        let resources = listed
            .iter()
            .map(UserConnection::resource)
            .collect::<Vec<_>>();

        assert_eq!(
            resources.iter().map(|c| c.id.as_str()).collect::<Vec<_>>(),
            [idle.public_id.as_str(), driving.public_id.as_str()],
        );
        assert_eq!(resources[0].playback_id, None);
        assert_eq!(
            resources[1].playback_id.as_deref(),
            Some(playback_id.as_str())
        );
        assert_eq!(resources[1].client_name.as_deref(), Some("driving client"));
        assert_eq!(
            controller(&listed, &playback_id).map(|c| c.id.as_str()),
            Some(driving.public_id.as_str())
        );
        let json = serde_json::to_value(&resources[1])?;
        assert!(json.get("session_key").is_none());
        assert!(json["connected_at"].is_string());

        assert!(
            list(&bob.principal, now_ms).await?[0].playback.is_none(),
            "a scope bound under another user's session key must not leak across users",
        );

        for connection in [idle, driving, bobs] {
            registry::unregister(connection.connection_id).await;
        }
        Ok(())
    }
}
