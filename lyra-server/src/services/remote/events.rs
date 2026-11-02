// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The events the server sends a connection.

use std::collections::{
    HashMap,
    HashSet,
};

use super::connections::{
    self,
    UserConnection,
};
use super::constants::RemoteAction;
use super::messages::{
    ConnectionClosed,
    EventMessage,
    OutgoingMessage,
    SyncRequired,
};
use crate::services::auth::Principal;
use crate::services::playback_sessions::{
    self,
    PlaybackUpdatePayload,
};

fn playback_event_visible_to_connection(
    payload: &PlaybackUpdatePayload,
    user_public_id: &str,
    accessible_library_ids: &HashSet<String>,
) -> bool {
    payload.user_public_id == user_public_id
        && payload
            .library_public_id
            .as_ref()
            .is_some_and(|library_id| accessible_library_ids.contains(library_id))
}

pub(crate) fn mark_sync_required_pending(sync_required_pending: &mut bool) -> bool {
    if *sync_required_pending {
        false
    } else {
        *sync_required_pending = true;
        true
    }
}

/// The event a connection is sent for a playback update, when it may see it.
pub(crate) fn playback_event_message(
    payload: PlaybackUpdatePayload,
    user_public_id: &str,
    accessible_library_ids: &HashSet<String>,
) -> Option<OutgoingMessage> {
    playback_event_visible_to_connection(&payload, user_public_id, accessible_library_ids)
        .then_some(OutgoingMessage::Event(EventMessage::PlaybackStateChanged(
            payload,
        )))
}

/// Tells a connection that missed playback events to resync its state via REST, since they
/// can't be replayed.
pub(crate) fn sync_required_message() -> OutgoingMessage {
    OutgoingMessage::Event(EventMessage::SyncRequired(SyncRequired {}))
}

/// The user's connections now, for a socket's connection events; `None` when they can't be read,
/// such as once the user is gone.
pub(crate) async fn current_connections(principal: &Principal) -> Option<Vec<UserConnection>> {
    let now_ms = playback_sessions::now_ms().ok()?;
    connections::list(principal, now_ms)
        .await
        .inspect_err(
            |error| tracing::debug!(error = %error, "could not read the user's connections"),
        )
        .ok()
}

/// What a connection event announces about a connection. Its other fields don't change.
#[derive(Debug, PartialEq)]
struct Announced {
    supported_commands: Vec<RemoteAction>,
    playback_id: Option<String>,
    remote_control_degraded: bool,
}

impl From<&UserConnection> for Announced {
    fn from(connection: &UserConnection) -> Self {
        Self {
            supported_commands: connection.supported_commands.clone(),
            playback_id: connection.playback_id().map(str::to_string),
            remote_control_degraded: connection.remote_control_degraded,
        }
    }
}

/// How one socket's client sees its user's connections: as they were when it connected, then as
/// the events sent since describe them. A client that lists connections after `connected` and
/// applies the events from then on stays current.
#[derive(Default)]
pub(crate) struct ConnectionEvents {
    seen: HashMap<String, Announced>,
    /// The earliest time one of the connections last read degrades with nothing else changing.
    next_degradation_at_ms: Option<u64>,
}

fn next_degradation_at_ms(connections: &[UserConnection]) -> Option<u64> {
    connections
        .iter()
        .filter_map(|connection| connection.degrades_at_ms)
        .min()
}

impl ConnectionEvents {
    /// When to read the connections again for a degradation that time alone brings about.
    pub(crate) fn next_degradation_at_ms(&self) -> Option<u64> {
        self.next_degradation_at_ms
    }

    /// The `connected` event describing the socket's own connection, or `None` when it is not
    /// among `connections`, as when a newer one with its session key replaced it.
    pub(crate) fn connected(
        &mut self,
        own_id: &str,
        connections: &[UserConnection],
    ) -> Option<OutgoingMessage> {
        let own = connections
            .iter()
            .find(|connection| connection.id == own_id)?;
        self.seen = connections
            .iter()
            .map(|connection| (connection.id.clone(), Announced::from(connection)))
            .collect();
        self.next_degradation_at_ms = next_degradation_at_ms(connections);
        Some(OutgoingMessage::Event(EventMessage::Connected(
            own.resource(),
        )))
    }

    /// The events that bring the client from what it has seen to `connections`: a
    /// `connection_closed` for each connection gone, then a `connection_updated` for each that
    /// is new or announces something different.
    pub(crate) fn changes(&mut self, connections: &[UserConnection]) -> Vec<OutgoingMessage> {
        self.next_degradation_at_ms = next_degradation_at_ms(connections);
        let mut closed = self
            .seen
            .keys()
            .filter(|id| !connections.iter().any(|connection| &connection.id == *id))
            .cloned()
            .collect::<Vec<_>>();
        closed.sort();
        let mut events = closed
            .into_iter()
            .map(|id| {
                self.seen.remove(&id);
                OutgoingMessage::Event(EventMessage::ConnectionClosed(ConnectionClosed { id }))
            })
            .collect::<Vec<_>>();
        for connection in connections {
            let announced = Announced::from(connection);
            if self.seen.get(&connection.id) != Some(&announced) {
                self.seen.insert(connection.id.clone(), announced);
                events.push(OutgoingMessage::Event(EventMessage::ConnectionUpdated(
                    connection.resource(),
                )));
            }
        }
        events
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::remote::registry;
    use crate::services::remote::testing as fixture;
    use lyra_db::PlaybackState;

    fn json(messages: Vec<OutgoingMessage>) -> Vec<serde_json::Value> {
        messages
            .into_iter()
            .map(|message| serde_json::to_value(message).unwrap())
            .collect()
    }

    async fn changes(
        events: &mut ConnectionEvents,
        user: &fixture::TestUser,
    ) -> Vec<serde_json::Value> {
        let connections = current_connections(&user.principal)
            .await
            .expect("connections are readable");
        json(events.changes(&connections))
    }

    #[tokio::test]
    async fn connection_events_announce_only_what_a_connection_changes() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        fixture::init().await?;
        let alice = fixture::user("alice").await?;
        let phone = fixture::connect(&alice, "phone").await?;
        let mut events = ConnectionEvents::default();

        let connections = current_connections(&alice.principal).await.unwrap();
        let connected = serde_json::to_value(
            events
                .connected(&phone.public_id, &connections)
                .expect("own connection is listed"),
        )?;
        assert_eq!(connected["type"], "event");
        assert_eq!(connected["event"], "connected");
        assert_eq!(connected["data"]["id"], phone.public_id.as_str());
        assert!(changes(&mut events, &alice).await.is_empty());

        let speaker = fixture::connect(&alice, "speaker").await?;
        let opened = changes(&mut events, &alice).await;
        assert_eq!(opened.len(), 1);
        assert_eq!(opened[0]["event"], "connection_updated");
        assert_eq!(opened[0]["data"]["id"], speaker.public_id.as_str());
        assert!(opened[0]["data"]["playback_id"].is_null());

        registry::set_supported_commands(
            speaker.connection_id,
            [RemoteAction::Pause].into_iter().collect(),
        )
        .await;
        let declared = changes(&mut events, &alice).await;
        assert_eq!(declared.len(), 1);
        assert_eq!(
            declared[0]["data"]["supported_commands"],
            serde_json::json!(["pause"])
        );

        let (playback_id, first_session) = fixture::drive_new_playback(&alice, "speaker").await?;
        let attached = changes(&mut events, &alice).await;
        assert_eq!(attached.len(), 1);
        assert_eq!(attached[0]["data"]["playback_id"], playback_id.as_str());

        let next_session = fixture::play_again(&alice, "speaker", &playback_id).await?;
        assert_ne!(next_session, first_session);
        assert!(
            changes(&mut events, &alice).await.is_empty(),
            "a new track session of the same playback is no change to the connection",
        );
        let scope =
            crate::services::playbacks::bindings::get(&fixture::connection_key(&alice, "speaker"))
                .expect("the reporter stays bound");
        assert_eq!(scope.current_playback_session_id, Some(next_session));
        assert_eq!(
            scope.previous_playback_session_id, None,
            "the replaced session is cleared"
        );

        crate::services::playbacks::bindings::clear(&fixture::connection_key(&alice, "speaker"));
        let detached = changes(&mut events, &alice).await;
        assert_eq!(detached.len(), 1);
        assert!(detached[0]["data"]["playback_id"].is_null());

        registry::unregister(speaker.connection_id).await;
        assert_eq!(
            changes(&mut events, &alice).await,
            [serde_json::json!({
                "type": "event",
                "event": "connection_closed",
                "data": { "id": speaker.public_id },
            })]
        );

        registry::unregister(phone.connection_id).await;
        Ok(())
    }

    #[tokio::test]
    async fn a_replaced_connection_closes_before_its_replacement_is_announced() -> anyhow::Result<()>
    {
        let _guard = crate::services::testing::runtime_test_lock().await;
        fixture::init().await?;
        let alice = fixture::user("alice").await?;
        let phone = fixture::connect(&alice, "phone").await?;
        let speaker = fixture::connect(&alice, "speaker").await?;
        let mut events = ConnectionEvents::default();
        let connections = current_connections(&alice.principal).await.unwrap();
        events.connected(&phone.public_id, &connections);

        let replacement = fixture::connect(&alice, "speaker").await?;
        let replaced = changes(&mut events, &alice).await;
        assert_eq!(replaced.len(), 2);
        assert_eq!(replaced[0]["event"], "connection_closed");
        assert_eq!(replaced[0]["data"]["id"], speaker.public_id.as_str());
        assert_eq!(replaced[1]["event"], "connection_updated");
        assert_eq!(replaced[1]["data"]["id"], replacement.public_id.as_str());

        assert!(
            events.connected(&speaker.public_id, &connections).is_some(),
            "a stale list still names the replaced connection",
        );
        let now = current_connections(&alice.principal).await.unwrap();
        assert!(
            ConnectionEvents::default()
                .connected(&speaker.public_id, &now)
                .is_none(),
            "a replaced connection is never announced as connected",
        );

        registry::unregister(phone.connection_id).await;
        registry::unregister(replacement.connection_id).await;
        Ok(())
    }

    fn notified(changes: &mut tokio::sync::watch::Receiver<()>) -> bool {
        let changed = changes.has_changed().expect("the user's sender is alive");
        changes.mark_unchanged();
        changed
    }

    async fn changes_at(
        events: &mut ConnectionEvents,
        user: &fixture::TestUser,
        now_ms: u64,
    ) -> Vec<serde_json::Value> {
        let connections = connections::list(&user.principal, now_ms).await.unwrap();
        json(events.changes(&connections))
    }

    #[tokio::test]
    async fn only_a_different_playback_notifies() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        fixture::init().await?;
        let alice = fixture::user("alice").await?;
        let mut alice_changes =
            playback_sessions::watch_connection_changes(&alice.principal.user_public_id);
        let speaker = fixture::connect(&alice, "speaker").await?;
        let (first, _) = fixture::drive_new_playback(&alice, "speaker").await?;
        notified(&mut alice_changes);

        fixture::play_again(&alice, "speaker", &first).await?;
        assert!(
            !notified(&mut alice_changes),
            "a new track session of the same playback sends no notice"
        );

        let mut events = ConnectionEvents::default();
        let connections = current_connections(&alice.principal).await.unwrap();
        events.connected(&speaker.public_id, &connections);
        let (second, _) = fixture::drive_new_playback(&alice, "speaker").await?;
        assert!(notified(&mut alice_changes), "switching playback notifies");
        let switched = changes(&mut events, &alice).await;
        assert_eq!(switched.len(), 1);
        assert_eq!(switched[0]["data"]["playback_id"], second.as_str());

        registry::unregister(speaker.connection_id).await;
        Ok(())
    }

    #[tokio::test]
    async fn degradation_and_its_acknowledgement_are_announced() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        fixture::init().await?;
        let alice = fixture::user("alice").await?;
        let mut alice_changes =
            playback_sessions::watch_connection_changes(&alice.principal.user_public_id);
        let speaker = fixture::connect(&alice, "speaker").await?;
        let (playback_id, _) = fixture::drive_new_playback(&alice, "speaker").await?;
        let mut events = ConnectionEvents::default();
        let connections = current_connections(&alice.principal).await.unwrap();
        events.connected(&speaker.public_id, &connections);
        assert_eq!(events.next_degradation_at_ms(), None);

        fixture::play_again(&alice, "speaker", &playback_id).await?;
        notified(&mut alice_changes);
        fixture::play_again(&alice, "speaker", &playback_id).await?;
        assert!(
            !notified(&mut alice_changes),
            "progress with no command pending sends no notice"
        );

        let dispatched_at_ms = playback_sessions::now_ms()?;
        crate::services::playbacks::bindings::mark_command_dispatched(
            &fixture::connection_key(&alice, "speaker"),
            dispatched_at_ms,
        );
        assert!(
            notified(&mut alice_changes),
            "a pending command notifies its deadline"
        );
        assert!(
            changes_at(&mut events, &alice, dispatched_at_ms)
                .await
                .is_empty()
        );
        let degrades_at_ms = dispatched_at_ms + 30_000;
        assert_eq!(events.next_degradation_at_ms(), Some(degrades_at_ms));

        let degraded = changes_at(&mut events, &alice, degrades_at_ms).await;
        assert_eq!(
            degraded.len(),
            1,
            "the deadline alone degrades the connection"
        );
        assert_eq!(degraded[0]["event"], "connection_updated");
        assert_eq!(degraded[0]["data"]["remote_control_degraded"], true);
        assert_eq!(events.next_degradation_at_ms(), None);
        assert!(
            !notified(&mut alice_changes),
            "degrading needs no notice; the socket's timer reads it"
        );

        fixture::play_again(&alice, "speaker", &playback_id).await?;
        assert!(
            notified(&mut alice_changes),
            "acknowledging a pending command notifies"
        );
        let acknowledged = changes(&mut events, &alice).await;
        assert_eq!(acknowledged.len(), 1);
        assert_eq!(acknowledged[0]["data"]["remote_control_degraded"], false);

        registry::unregister(speaker.connection_id).await;
        Ok(())
    }

    #[tokio::test]
    async fn stopping_the_current_playback_drops_its_pending_command() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        fixture::init().await?;
        let alice = fixture::user("alice").await?;
        let speaker = fixture::connect(&alice, "speaker").await?;
        fixture::drive_new_playback(&alice, "speaker").await?;
        let (second, _) = fixture::drive_new_playback(&alice, "speaker").await?;
        let mut events = ConnectionEvents::default();
        let connections = current_connections(&alice.principal).await.unwrap();
        events.connected(&speaker.public_id, &connections);
        let mut alice_changes =
            playback_sessions::watch_connection_changes(&alice.principal.user_public_id);

        let dispatched_at_ms = playback_sessions::now_ms()?;
        crate::services::playbacks::bindings::mark_command_dispatched(
            &fixture::connection_key(&alice, "speaker"),
            dispatched_at_ms,
        );
        fixture::stop(&alice, "speaker", &second).await?;
        assert!(notified(&mut alice_changes));
        let scope =
            crate::services::playbacks::bindings::get(&fixture::connection_key(&alice, "speaker"))
                .expect("the earlier playback keeps the scope");
        assert_eq!(scope.current_playback_session_id, None);
        assert_eq!(scope.command_dispatched_at_ms, None);

        let later = changes_at(&mut events, &alice, dispatched_at_ms + 30_000).await;
        assert_eq!(later.len(), 1, "only the detach is announced");
        assert!(later[0]["data"]["playback_id"].is_null());
        assert_eq!(later[0]["data"]["remote_control_degraded"], false);
        assert_eq!(events.next_degradation_at_ms(), None);

        registry::unregister(speaker.connection_id).await;
        Ok(())
    }

    #[tokio::test]
    async fn removing_a_degraded_scope_with_its_last_previous_session_notifies()
    -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        fixture::init().await?;
        let alice = fixture::user("alice").await?;
        let speaker = fixture::connect(&alice, "speaker").await?;
        let (_, first_session) = fixture::drive_new_playback(&alice, "speaker").await?;
        let (second, _) = fixture::drive_new_playback(&alice, "speaker").await?;
        fixture::stop(&alice, "speaker", &second).await?;
        let key = fixture::connection_key(&alice, "speaker");
        let dispatched_at_ms = playback_sessions::now_ms()?;
        crate::services::playbacks::bindings::mark_command_dispatched(&key, dispatched_at_ms);
        let mut events = ConnectionEvents::default();
        let degraded_at = connections::list(&alice.principal, dispatched_at_ms + 30_000).await?;
        events.connected(&speaker.public_id, &degraded_at);
        assert!(degraded_at[0].remote_control_degraded);
        let mut alice_changes =
            playback_sessions::watch_connection_changes(&alice.principal.user_public_id);

        let first_session_public_id = {
            let db = crate::services::STATE.db.read().await;
            lyra_db::playback_sessions::get_by_id(&*db, first_session)?
                .expect("first session exists")
                .id
        };
        crate::services::playbacks::bindings::clear_session(
            first_session,
            &first_session_public_id,
        );
        assert!(crate::services::playbacks::bindings::get(&key).is_none());
        assert!(
            notified(&mut alice_changes),
            "removing the scope with its previous session notifies"
        );
        let recovered = changes_at(&mut events, &alice, dispatched_at_ms + 30_000).await;
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0]["data"]["remote_control_degraded"], false);

        registry::unregister(speaker.connection_id).await;
        Ok(())
    }

    #[tokio::test]
    async fn connection_changes_notify_only_that_users_sockets() -> anyhow::Result<()> {
        use crate::services::playback_sessions::watch_connection_changes;
        let _guard = crate::services::testing::runtime_test_lock().await;
        fixture::init().await?;
        let alice = fixture::user("alice").await?;
        let bob = fixture::user("bob").await?;
        let mut alice_changes = watch_connection_changes(&alice.principal.user_public_id);
        let bob_changes = watch_connection_changes(&bob.principal.user_public_id);
        let mut notified = || {
            let changed = alice_changes.has_changed().unwrap();
            alice_changes.mark_unchanged();
            changed
        };

        let phone = fixture::connect(&alice, "phone").await?;
        assert!(notified(), "registering notifies");
        registry::set_supported_commands(phone.connection_id, Default::default()).await;
        assert!(notified(), "declaring capabilities notifies");
        fixture::drive_new_playback(&alice, "phone").await?;
        assert!(notified(), "attaching a playback notifies");
        registry::unregister(phone.connection_id).await;
        assert!(notified(), "closing notifies");
        assert!(!bob_changes.has_changed().unwrap());
        Ok(())
    }

    fn payload(user_public_id: &str, library_public_id: Option<&str>) -> PlaybackUpdatePayload {
        PlaybackUpdatePayload {
            event: "updated".to_string(),
            state: PlaybackState::Playing,
            playback_session_public_id: "session".to_string(),
            track_public_id: "track".to_string(),
            user_public_id: user_public_id.to_string(),
            library_public_id: library_public_id.map(str::to_string),
            plugin_id: None,
            client_name: None,
            position_ms: 0,
            duration_ms: Some(1_000),
            rate: 1.0,
            counts_as_listen: true,
            played_ms: 0,
            updated_at_ms: 0,
        }
    }

    #[test]
    fn playback_event_visibility_filters_by_user_and_library_access() {
        let accessible = HashSet::from(["library-a".to_string()]);

        assert!(playback_event_visible_to_connection(
            &payload("user-a", Some("library-a")),
            "user-a",
            &accessible,
        ));
        assert!(!playback_event_visible_to_connection(
            &payload("user-b", Some("library-a")),
            "user-a",
            &accessible,
        ));
        assert!(!playback_event_visible_to_connection(
            &payload("user-a", Some("library-b")),
            "user-a",
            &accessible,
        ));
        assert!(!playback_event_visible_to_connection(
            &payload("user-a", None),
            "user-a",
            &accessible,
        ));
    }

    #[test]
    fn sync_required_lag_signal_is_coalesced_until_reset() {
        let mut pending = false;

        assert!(mark_sync_required_pending(&mut pending));
        assert!(!mark_sync_required_pending(&mut pending));

        pending = false;
        assert!(mark_sync_required_pending(&mut pending));
    }
}
