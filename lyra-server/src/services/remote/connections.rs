// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The connection resource: a user's WebSocket connections joined with the playback each drives.

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

/// The playback a connection drives and the track session it reports for it.
#[derive(Clone, Debug)]
pub(crate) struct ConnectionPlayback {
    pub(crate) playback_id: String,
    pub(crate) session_id: DbId,
}

#[derive(Clone, Debug)]
pub(crate) struct UserConnection {
    pub(crate) id: String,
    pub(crate) session_key: String,
    pub(crate) client_name: Option<String>,
    pub(crate) supported_commands: Vec<RemoteAction>,
    pub(crate) playback: Option<ConnectionPlayback>,
    pub(crate) remote_control_degraded: bool,
    /// When the connection degrades unless it acknowledges the command it was sent; `None` once
    /// degraded or with nothing pending.
    pub(crate) degrades_at_ms: Option<u64>,
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
            playback_id: self.playback_id().map(str::to_string),
            remote_control_degraded: self.remote_control_degraded,
            connected_at: crate::services::timestamps::unix_ms_to_rfc3339_u64(self.connected_at_ms),
        }
    }

    pub(crate) fn playback_id(&self) -> Option<&str> {
        self.playback
            .as_ref()
            .map(|playback| playback.playback_id.as_str())
    }

    /// The track session the connection reports for its playback, if it still exists.
    pub(crate) fn current_session(&self, db: &DbAny) -> anyhow::Result<Option<CurrentSession>> {
        let Some(session_id) = self.playback.as_ref().map(|playback| playback.session_id) else {
            return Ok(None);
        };
        let Some(playback) = lyra_db::playback_sessions::get_by_id(db, session_id)? else {
            return Ok(None);
        };
        let Some(track_db_id) = lyra_db::playback_sessions::get_track_id(db, session_id)? else {
            return Ok(None);
        };
        let Some(track_public_id) = lyra_db::lookup::find_id_by_db_id(db, track_db_id)? else {
            return Ok(None);
        };
        Ok(Some(CurrentSession {
            track_db_id,
            track_public_id,
            playback,
        }))
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
            let degraded_at_ms = playback_sessions::remote_control_degraded_at_ms(&scope_key);
            (snapshot, scope, degraded_at_ms)
        })
        .collect::<Vec<_>>();

    let playback_by_session = if bound.iter().any(|(_, scope, _)| {
        scope
            .as_ref()
            .is_some_and(|scope| scope.current_playback_session_id.is_some())
    }) {
        visible_playbacks_by_current_session(db, principal, now_ms)?
    } else {
        HashMap::new()
    };

    let mut connections = bound
        .into_iter()
        .map(|(snapshot, scope, degraded_at_ms)| {
            let playback = scope
                .as_ref()
                .and_then(|scope| scope.current_playback_session_id)
                .and_then(|session_id| {
                    Some(ConnectionPlayback {
                        playback_id: playback_by_session.get(&session_id)?.clone(),
                        session_id,
                    })
                });
            UserConnection {
                id: snapshot.public_id,
                session_key: snapshot.session_key,
                client_name: snapshot.client_name,
                supported_commands: snapshot.supported_commands,
                playback,
                remote_control_degraded: degraded_at_ms.is_some_and(|at_ms| now_ms >= at_ms),
                degrades_at_ms: degraded_at_ms.filter(|at_ms| now_ms < *at_ms),
                connected_at_ms: snapshot.connected_at_ms,
                connection_id: snapshot.connection_id,
                bound_at_ms: scope.map_or(0, |scope| scope.updated_at_ms),
            }
        })
        .collect::<Vec<_>>();
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
        .filter(|connection| connection.playback_id() == Some(playback_id))
        .max_by_key(|connection| (connection.bound_at_ms, connection.connection_id))
}

fn scope_key(snapshot: &ConnectionSnapshot) -> PlaybackScopeKey<'_> {
    PlaybackScopeKey::native(&snapshot.user_public_id, &snapshot.session_key)
}

/// The public id of each playback the principal can see, keyed by its current track session.
fn visible_playbacks_by_current_session(
    db: &DbAny,
    principal: &Principal,
    now_ms: u64,
) -> Result<HashMap<DbId, String>, PlaybackError> {
    Ok(
        playbacks::list_visible_projections_with_current(db, principal, false, now_ms)?
            .into_iter()
            .filter_map(|(projection, session_id)| Some((session_id?, projection.id)))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::remote::testing as fixture;
    use crate::services::testing;

    #[tokio::test]
    async fn list_joins_each_connection_of_the_principal_with_its_playback() -> anyhow::Result<()> {
        let _guard = testing::runtime_test_lock().await;
        fixture::init().await?;
        let alice = fixture::user("alice").await?;
        let bob = fixture::user("bob").await?;
        let idle = fixture::connect(&alice, "idle").await?;
        let driving = fixture::connect(&alice, "driving").await?;
        let bobs = fixture::connect(&bob, "driving").await?;
        let (playback_id, _) = fixture::drive_new_playback(&alice, "driving").await?;

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
