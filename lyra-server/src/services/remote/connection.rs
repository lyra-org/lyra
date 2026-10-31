// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
};

use tokio::task::JoinSet;

use super::connections::{
    self,
    Connection,
};
use super::constants::RemoteAction;
use super::handoffs;
use super::messages::{
    ClientCommand,
    ConnectionClosed,
    EventMessage,
    ForwardedCommand,
    OutgoingMessage,
    ResponseMessage,
    SyncRequired,
};
use super::registry::{
    self,
    ConnectionId,
};
use super::signals::{
    self,
    ConnectionSignal,
};

use crate::services::STATE;
use crate::services::auth::{
    self,
    AuthError,
    Principal,
};
use crate::services::playback_sessions as playbacks;
use crate::services::playback_sessions::{
    PlaybackUpdatePayload,
    unsubscribe_playback_events,
};

const NATIVE_SOURCE_ID: &str = "native";
const MAX_IN_FLIGHT_COMMANDS: usize = 16;

fn extract_id(text: &str) -> String {
    #[derive(serde::Deserialize)]
    struct IdOnly {
        #[serde(default)]
        id: String,
    }
    serde_json::from_str::<IdOnly>(text)
        .map(|v| v.id)
        .unwrap_or_default()
}

async fn handle_message(cmd: ClientCommand, connection_id: ConnectionId) -> OutgoingMessage {
    let response = handle_command(cmd, connection_id).await;
    OutgoingMessage::Response(response)
}

fn is_detached_command(command: &ClientCommand) -> bool {
    matches!(command, ClientCommand::HandoffQueue(_))
}

fn spawn_handoff_response(
    responses: &mut JoinSet<OutgoingMessage>,
    command: ClientCommand,
    connection_id: ConnectionId,
) {
    debug_assert!(is_detached_command(&command));
    responses.spawn(handle_message(command, connection_id));
}

pub(crate) async fn abort_handoff_responses(responses: &mut JoinSet<OutgoingMessage>) {
    responses.abort_all();
    while responses.join_next().await.is_some() {}
}

async fn handle_command(cmd: ClientCommand, connection_id: ConnectionId) -> ResponseMessage {
    match cmd {
        ClientCommand::DeclareCapabilities { id, commands } => {
            handle_declare_capabilities(id, commands, connection_id).await
        }
        ClientCommand::Play(_)
        | ClientCommand::Pause(_)
        | ClientCommand::Unpause(_)
        | ClientCommand::Stop(_)
        | ClientCommand::Seek(_)
        | ClientCommand::NextTrack(_)
        | ClientCommand::PreviousTrack(_)
        | ClientCommand::SetVolume(_)
        | ClientCommand::HandoffQueue(_) => handle_remote_control(cmd, connection_id).await,
    }
}

async fn handle_declare_capabilities(
    id: String,
    commands: Vec<RemoteAction>,
    connection_id: ConnectionId,
) -> ResponseMessage {
    let set: std::collections::HashSet<RemoteAction> = commands.into_iter().collect();
    if registry::set_supported_commands(connection_id, set).await {
        ResponseMessage::ok(id)
    } else {
        ResponseMessage::error(id, "connection not found")
    }
}

async fn handle_remote_control(cmd: ClientCommand, connection_id: ConnectionId) -> ResponseMessage {
    let id = cmd.id().to_string();
    let action = match cmd.remote_action() {
        Some(a) => a,
        None => return ResponseMessage::error(id, "not a remote control action"),
    };
    let target_str = match cmd.target() {
        Some(t) => t.to_string(),
        None => {
            return ResponseMessage::error(id, "target is required for remote control commands");
        }
    };

    let (source, target_snap) =
        registry::get_connection_and_target(connection_id, &target_str).await;

    let Some(source) = source else {
        return ResponseMessage::error(id, "source connection not found");
    };
    let Some(target_snap) = target_snap else {
        return ResponseMessage::error(id, "target connection not found");
    };

    if target_snap.connection_id == connection_id {
        return ResponseMessage::error(id, "cannot target self");
    }

    if source.user_public_id != target_snap.user_public_id {
        return ResponseMessage::error(id, "not authorized to control target");
    }

    if !target_snap.supported_commands.contains(&action) {
        return ResponseMessage::error(id, format!("target does not support command: {action:?}"));
    }

    if let ClientCommand::HandoffQueue(command) = &cmd {
        return match handoffs::dispatch_and_wait(
            Some(&source),
            &target_snap,
            &command.playback_id,
            command.queue_revision,
        )
        .await
        {
            Ok(()) => ResponseMessage::ok(id),
            Err(error) => ResponseMessage::error(id, error),
        };
    }

    let from = Some(source.public_id.clone());
    let forwarded = match &cmd {
        ClientCommand::Seek(c) => ForwardedCommand::Seek {
            from,
            position_ms: c.position_ms,
        },
        ClientCommand::SetVolume(c) => ForwardedCommand::SetVolume {
            from,
            level: c.level.clamp(0.0, 1.0),
        },
        ClientCommand::Play(_) => ForwardedCommand::Play { from },
        ClientCommand::Pause(_) => ForwardedCommand::Pause { from },
        ClientCommand::Unpause(_) => ForwardedCommand::Unpause { from },
        ClientCommand::Stop(_) => ForwardedCommand::Stop { from },
        ClientCommand::NextTrack(_) => ForwardedCommand::NextTrack { from },
        ClientCommand::PreviousTrack(_) => ForwardedCommand::PreviousTrack { from },
        ClientCommand::DeclareCapabilities { .. } => {
            return ResponseMessage::error(id, "not a remote control action");
        }
        ClientCommand::HandoffQueue(_) => {
            return ResponseMessage::error(id, "not a direct remote control action");
        }
    };

    let forwarded = OutgoingMessage::Command(forwarded);

    match registry::send_to_connection(target_snap.connection_id, forwarded).await {
        Ok(()) => {
            if let Ok(now_ms) = playbacks::now_ms() {
                let scope_key = playbacks::PlaybackScopeKey {
                    plugin_id: NATIVE_SOURCE_ID,
                    user_public_id: &target_snap.user_public_id,
                    session_key: &target_snap.session_key,
                };
                playbacks::mark_command_dispatched(&scope_key, now_ms);
            }
            ResponseMessage::ok(id)
        }
        Err(err) => ResponseMessage::error(id, err),
    }
}

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

pub(crate) enum AuthStatus {
    /// Carries the re-resolved library access set so a long-lived connection
    /// picks up grants and revocations made after it was established.
    Valid(HashSet<String>),
    Revoked,
    Error,
}

pub(crate) async fn check_auth(
    token: &Option<String>,
    expected_user_public_id: &str,
) -> AuthStatus {
    match auth::resolve_auth_from_bearer(token.as_deref()).await {
        Ok(Some(auth)) => {
            if auth.principal.user_public_id == expected_user_public_id {
                AuthStatus::Valid(auth.principal.accessible_library_ids)
            } else {
                AuthStatus::Revoked
            }
        }
        Ok(None) => AuthStatus::Revoked,
        Err(AuthError::SessionExpired) => AuthStatus::Revoked,
        Err(_) => AuthStatus::Error,
    }
}

async fn pause_playing_scope_for_disconnected_connection(handle: &registry::ConnectionHandle) {
    let now_ms = match playbacks::now_ms() {
        Ok(now_ms) => now_ms,
        Err(err) => {
            tracing::warn!(
                connection_id = handle.connection_id,
                user_public_id = %handle.user_public_id,
                session_key = %handle.session_key,
                error = %err,
                "failed to timestamp disconnected playback scope"
            );
            return;
        }
    };

    let update = {
        let mut db = STATE.db.write().await;
        playbacks::pause_playing_scopes_on_disconnect(
            &mut db,
            &handle.user_public_id,
            &handle.session_key,
            now_ms,
        )
    };

    match update {
        Ok((paused_playbacks, evicted_playbacks)) => {
            playbacks::dispatch_evicted_updates(evicted_playbacks);
            for playback in paused_playbacks {
                playbacks::dispatch_playback_update(&playback, "progress".to_string());
            }
        }
        Err(err) => {
            tracing::warn!(
                connection_id = handle.connection_id,
                user_public_id = %handle.user_public_id,
                session_key = %handle.session_key,
                error = %err,
                "failed to pause disconnected playback scope"
            );
        }
    }
}

/// Keeps a new registration only if its user still exists. A delete that commits before this
/// read has already swept the registry, and one that commits after it sweeps this connection, so
/// no registration outlives its user.
pub(crate) async fn confirm_registration(
    connection_id: ConnectionId,
    user_public_id: &str,
) -> anyhow::Result<bool> {
    let user = lyra_db::users::get_by_public_id(&*STATE.db.read().await, user_public_id);
    if !matches!(user, Ok(Some(_))) {
        registry::unregister(connection_id).await;
    }
    Ok(user?.is_some())
}

/// The response to a text message from a connection, unless the command it carries was handed
/// to `handoff_responses` to answer once it completes.
pub(crate) async fn respond(
    text: &str,
    handoff_responses: &mut JoinSet<OutgoingMessage>,
    connection_id: ConnectionId,
) -> Option<OutgoingMessage> {
    Some(match serde_json::from_str::<ClientCommand>(text) {
        Ok(msg)
            if is_detached_command(&msg) && handoff_responses.len() < MAX_IN_FLIGHT_COMMANDS =>
        {
            spawn_handoff_response(handoff_responses, msg, connection_id);
            return None;
        }
        Ok(msg) if is_detached_command(&msg) => {
            let id = extract_id(text);
            OutgoingMessage::Response(ResponseMessage::error(id, "too many handoffs in flight"))
        }
        Ok(msg) => handle_message(msg, connection_id).await,
        Err(err) => {
            let id = extract_id(text);
            OutgoingMessage::Response(ResponseMessage::error(
                id,
                format!("malformed message: {err}"),
            ))
        }
    })
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

/// What a socket last told its client about each of the user's connections, so a signal that
/// leaves a connection looking the same sends nothing.
#[derive(Default)]
pub(crate) struct ConnectionViews {
    sent: HashMap<String, Connection>,
}

async fn current_connections(principal: &Principal) -> Option<Vec<Connection>> {
    let now_ms = playbacks::now_ms().ok()?;
    match connections::list(principal, now_ms).await {
        Ok(connections) => Some(connections.iter().map(|c| c.resource()).collect()),
        Err(error) => {
            tracing::debug!(error = %error, "could not resolve connections for an event");
            None
        }
    }
}

/// The `connected` event for a socket that just registered, which also records how every other
/// connection of the user looks now.
pub(crate) async fn connected_message(
    public_id: &str,
    principal: &Principal,
    views: &mut ConnectionViews,
) -> Option<OutgoingMessage> {
    views.sent = current_connections(principal)
        .await?
        .into_iter()
        .map(|connection| (connection.id.clone(), connection))
        .collect();
    let own = views.sent.get(public_id)?.clone();
    Some(OutgoingMessage::Event(EventMessage::Connected(own)))
}

/// The event a signal about one of the user's connections turns into, unless the client already
/// sees the connection this way.
pub(crate) async fn connection_signal_message(
    signal: ConnectionSignal,
    principal: &Principal,
    views: &mut ConnectionViews,
) -> Option<OutgoingMessage> {
    match signal {
        ConnectionSignal::Closed { id } => {
            views.sent.remove(&id);
            Some(OutgoingMessage::Event(EventMessage::ConnectionClosed(
                ConnectionClosed { id },
            )))
        }
        ConnectionSignal::Changed { session_key } => {
            let snapshot = registry::list_user_connections(&principal.user_public_id)
                .await
                .into_iter()
                .find(|snapshot| snapshot.session_key == session_key)?;
            let now_ms = playbacks::now_ms().ok()?;
            let db = STATE.db.read().await;
            let connection = match connections::join(&db, principal, vec![snapshot], now_ms) {
                Ok(connections) => connections.into_iter().next()?.resource(),
                Err(error) => {
                    tracing::debug!(error = %error, "could not resolve a changed connection");
                    return None;
                }
            };
            drop(db);
            if views.sent.get(&connection.id) == Some(&connection) {
                return None;
            }
            views.sent.insert(connection.id.clone(), connection.clone());
            Some(OutgoingMessage::Event(EventMessage::ConnectionUpdated(
                connection,
            )))
        }
    }
}

/// Releases what a closed connection held and pauses the playback it was driving.
pub(crate) async fn disconnect(connection_id: ConnectionId, user_public_id: &str) {
    unsubscribe_playback_events(user_public_id);
    signals::unsubscribe(user_public_id);
    let handle = registry::unregister(connection_id).await;
    if let Some(handle) = &handle {
        tracing::info!(
            connection_id,
            user_public_id = %handle.user_public_id,
            session_key = %handle.session_key,
            "websocket disconnected"
        );
        pause_playing_scope_for_disconnected_connection(handle).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lyra_db::PlaybackState;
    use tokio::sync::watch;

    #[test]
    fn only_handoff_queue_is_detached() {
        let handoff: ClientCommand = serde_json::from_str(
            r#"{"action":"handoff_queue","id":"handoff","target":"target","playback_id":"playback","queue_revision":1}"#,
        )
        .unwrap();
        let ordinary: ClientCommand = serde_json::from_str(
            r#"{"action":"declare_capabilities","id":"ordinary","commands":[]}"#,
        )
        .unwrap();

        assert!(is_detached_command(&handoff));
        assert!(!is_detached_command(&ordinary));
    }

    #[tokio::test]
    async fn pending_handoff_preserves_inline_command_liveness_and_order() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        for connection in registry::list_connections().await {
            registry::unregister(connection.connection_id).await;
        }
        let registered = registry::register(
            "user".to_string(),
            None,
            "source".to_string(),
            watch::channel(None).0,
        )
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let connection_id = registered.connection_id;
        let mut handoff_responses = JoinSet::new();
        handoff_responses.spawn(std::future::pending::<OutgoingMessage>());

        let mut response_ids = Vec::new();
        for id in ["first", "second"] {
            let response = tokio::time::timeout(
                std::time::Duration::from_millis(100),
                handle_message(
                    ClientCommand::DeclareCapabilities {
                        id: id.to_string(),
                        commands: Vec::new(),
                    },
                    connection_id,
                ),
            )
            .await
            .expect("inline command should not wait for detached handoff");
            let OutgoingMessage::Response(response) = response else {
                anyhow::bail!("ordinary command should produce a response");
            };
            response_ids.push(response.id);
        }
        assert_eq!(response_ids, ["first", "second"]);

        abort_handoff_responses(&mut handoff_responses).await;
        registry::unregister(connection_id).await;
        drop(registered.command_rx);
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
            position_ms: 0,
            duration_ms: Some(1_000),
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

    /// A valid auth recheck returns the principal's current library access set.
    #[tokio::test]
    async fn check_auth_valid_carries_fresh_accessible_library_ids() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_library_test_state(std::path::PathBuf::from(".")).await?;

        let (user_db_id, user_public_id) = {
            let mut db = STATE.db.write().await;
            let user = lyra_db::fixtures::test_user("ws-access")?;
            let user_public_id = user.id.clone();
            let user_db_id = lyra_db::users::create(&mut db, &user)?;
            (user_db_id, user_public_id)
        };
        let token = crate::services::testing::create_session(user_db_id, Default::default())
            .await?
            .token;

        assert!(matches!(
            check_auth(&Some(token.clone()), &user_public_id).await,
            AuthStatus::Valid(ref ids) if ids.is_empty()
        ));

        let library_id = {
            let mut db = STATE.db.write().await;
            let path =
                std::path::PathBuf::from(format!("/tmp/lyra-ws-access-{}", nanoid::nanoid!()));
            let insert = lyra_db::libraries::LibraryInsert {
                id: nanoid::nanoid!(),
                name: "WS Access".to_string(),
                path_key: lyra_db::libraries::path_key_for(&path),
                path,
                language: None,
                country: None,
            };
            db.transaction_mut(|t| -> anyhow::Result<String> {
                Ok(lyra_db::libraries::create_with_creator(t, insert, user_db_id)?.id)
            })?
        };

        let AuthStatus::Valid(refreshed) = check_auth(&Some(token), &user_public_id).await else {
            anyhow::bail!("session should still be valid");
        };
        assert!(refreshed.contains(&library_id));
        Ok(())
    }

    #[tokio::test]
    async fn disconnect_pause_ignores_the_user_that_reused_the_db_id() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_library_test_state(std::path::PathBuf::from(".")).await?;
        for connection in registry::list_connections().await {
            registry::unregister(connection.connection_id).await;
        }

        let alice = lyra_db::fixtures::test_user("alice")?;
        let alice_public_id = alice.id.clone();
        let alice_db_id = {
            let mut db = STATE.db.write().await;
            lyra_db::users::create(&mut db, &alice)?
        };
        let alice_connection = registry::register(
            alice_public_id,
            None,
            "device".to_string(),
            watch::channel(None).0,
        )
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;

        let bob_playback = {
            let mut db = STATE.db.write().await;
            let bob_db_id = lyra_db::test_db::recycle_user(&mut db, alice_db_id, "bob")?;
            let track_db_id = lyra_db::fixtures::insert_track(&mut db, "Bob's Track")?;
            playbacks::report_playback_session_with_cleanup(
                &mut db,
                playbacks::SessionPlaybackReportRequest {
                    plugin_id: NATIVE_SOURCE_ID,
                    user_db_id: bob_db_id,
                    session_key: "device",
                    track_db_id,
                    play_id: None,
                    client_name: None,
                    mutation: playbacks::PlaybackMutation {
                        position_ms: Some(0),
                        duration_ms: Some(200_000),
                        state: Some(PlaybackState::Playing),
                    },
                    now_ms: playbacks::now_ms()?,
                    active_event: playbacks::ActiveEvent::Started,
                },
            )?
            .playback
            .expect("bob's playback starts")
        };

        let handle = registry::unregister(alice_connection.connection_id)
            .await
            .expect("alice's connection is registered");
        pause_playing_scope_for_disconnected_connection(&handle).await;

        let db = STATE.db.read().await;
        let persisted =
            lyra_db::playback_sessions::get_by_id(&*db, bob_playback.playback_session_id)?
                .expect("bob's playback persists");
        assert_eq!(
            persisted.state,
            PlaybackState::Playing,
            "a deleted user's disconnect must not pause the DbId's next owner"
        );
        Ok(())
    }

    #[tokio::test]
    async fn confirm_registration_drops_connections_of_deleted_users() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        for connection in registry::list_connections().await {
            registry::unregister(connection.connection_id).await;
        }
        let (kept_public_id, deleted_public_id) = {
            let mut db = STATE.db.write().await;
            let kept = lyra_db::fixtures::test_user("still-here")?;
            let deleted = lyra_db::fixtures::test_user("deleted-mid-upgrade")?;
            lyra_db::users::create(&mut db, &kept)?;
            let deleted_db_id = lyra_db::users::create(&mut db, &deleted)?;
            lyra_db::users::delete_user(&mut db, deleted_db_id)?;
            (kept.id, deleted.id)
        };
        let kept = registry::register(
            kept_public_id.clone(),
            None,
            "device".to_string(),
            watch::channel(None).0,
        )
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let late = registry::register(
            deleted_public_id.clone(),
            None,
            "device".to_string(),
            watch::channel(None).0,
        )
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;

        assert!(confirm_registration(kept.connection_id, &kept_public_id).await?);
        assert!(!confirm_registration(late.connection_id, &deleted_public_id).await?);
        let remaining = registry::list_connections()
            .await
            .into_iter()
            .map(|connection| connection.connection_id)
            .collect::<Vec<_>>();
        assert_eq!(remaining, [kept.connection_id]);

        registry::unregister(kept.connection_id).await;
        Ok(())
    }

    fn drain(rx: &mut tokio::sync::broadcast::Receiver<ConnectionSignal>) -> Vec<ConnectionSignal> {
        std::iter::from_fn(|| rx.try_recv().ok()).collect()
    }

    fn changed(session_key: &str) -> ConnectionSignal {
        ConnectionSignal::Changed {
            session_key: session_key.to_string(),
        }
    }

    fn event(message: Option<OutgoingMessage>) -> Option<serde_json::Value> {
        message.map(|message| serde_json::to_value(message).unwrap())
    }

    #[tokio::test]
    async fn connection_signals_reach_only_the_same_users_sockets() -> anyhow::Result<()> {
        use super::connections::tests as fixture;
        let _guard = crate::services::testing::runtime_test_lock().await;
        fixture::init().await?;
        let alice = fixture::user("alice").await?;
        let bob = fixture::user("bob").await?;
        let mut alice_rx = signals::subscribe(&alice.principal.user_public_id);
        let mut bob_rx = signals::subscribe(&bob.principal.user_public_id);

        let phone = fixture::connect(&alice, "phone").await?;
        assert_eq!(drain(&mut alice_rx), [changed("phone")]);

        handle_declare_capabilities(
            "caps".into(),
            vec![RemoteAction::Pause],
            phone.connection_id,
        )
        .await;
        assert_eq!(drain(&mut alice_rx), [changed("phone")]);

        let (_, session_id) = fixture::drive_new_playback(&alice, "phone").await?;
        assert_eq!(drain(&mut alice_rx), [changed("phone")]);
        playbacks::bind_current_playback_session_scope(
            &fixture::scope(&alice, "phone"),
            session_id,
            "unchanged".to_string(),
            playbacks::now_ms()?,
        );
        assert!(
            drain(&mut alice_rx).is_empty(),
            "rebinding the same session is not an attach"
        );
        playbacks::clear_playback_session_scope(&fixture::scope(&alice, "phone"));
        assert_eq!(drain(&mut alice_rx), [changed("phone")]);

        let replacement = fixture::connect(&alice, "phone").await?;
        assert_eq!(
            drain(&mut alice_rx),
            [
                ConnectionSignal::Closed {
                    id: phone.public_id.clone()
                },
                changed("phone"),
            ],
            "a duplicate session key evicts the old connection before announcing the new one",
        );
        let phone = replacement;

        registry::unregister(phone.connection_id).await;
        assert_eq!(
            drain(&mut alice_rx),
            [ConnectionSignal::Closed {
                id: phone.public_id.clone()
            }]
        );
        assert!(drain(&mut bob_rx).is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn sockets_hear_of_connections_only_when_they_look_different() -> anyhow::Result<()> {
        use super::connections::tests as fixture;
        let _guard = crate::services::testing::runtime_test_lock().await;
        fixture::init().await?;
        let alice = fixture::user("alice").await?;
        let phone = fixture::connect(&alice, "phone").await?;
        let mut views = ConnectionViews::default();

        let connected =
            event(connected_message(&phone.public_id, &alice.principal, &mut views).await)
                .expect("connected event");
        assert_eq!(connected["type"], "event");
        assert_eq!(connected["event"], "connected");
        assert_eq!(connected["data"]["id"], phone.public_id.as_str());
        assert!(connected["data"]["playback_id"].is_null());

        let speaker = fixture::connect(&alice, "speaker").await?;
        let updated = event(
            connection_signal_message(changed("speaker"), &alice.principal, &mut views).await,
        )
        .expect("a new connection is announced");
        assert_eq!(updated["event"], "connection_updated");
        assert_eq!(updated["data"]["id"], speaker.public_id.as_str());
        assert!(
            connection_signal_message(changed("speaker"), &alice.principal, &mut views)
                .await
                .is_none(),
            "a connection that looks the same is not announced again",
        );

        let (playback_id, _) = fixture::drive_new_playback(&alice, "speaker").await?;
        let attached = event(
            connection_signal_message(changed("speaker"), &alice.principal, &mut views).await,
        )
        .expect("attaching a playback is announced");
        assert_eq!(attached["data"]["playback_id"], playback_id.as_str());

        assert!(
            connection_signal_message(changed("tablet"), &alice.principal, &mut views)
                .await
                .is_none(),
            "a session key without a connection announces nothing",
        );

        let closed = event(
            connection_signal_message(
                ConnectionSignal::Closed {
                    id: speaker.public_id.clone(),
                },
                &alice.principal,
                &mut views,
            )
            .await,
        )
        .expect("closing is announced");
        assert_eq!(
            closed,
            serde_json::json!({
                "type": "event",
                "event": "connection_closed",
                "data": { "id": speaker.public_id },
            })
        );

        registry::unregister(phone.connection_id).await;
        registry::unregister(speaker.connection_id).await;
        Ok(())
    }

    #[tokio::test]
    async fn forwarded_commands_name_the_source_by_its_public_id() -> anyhow::Result<()> {
        use super::connections::tests as fixture;
        let _guard = crate::services::testing::runtime_test_lock().await;
        fixture::init().await?;
        let alice = fixture::user("alice").await?;
        let bob = fixture::user("bob").await?;
        let remote = fixture::connect(&alice, "remote").await?;
        let mut speaker = fixture::connect(&alice, "speaker").await?;
        let bobs = fixture::connect(&bob, "speaker").await?;
        for connection in [&remote, &speaker, &bobs] {
            handle_declare_capabilities(
                "caps".into(),
                vec![RemoteAction::Pause],
                connection.connection_id,
            )
            .await;
        }
        let pause = |target: &str| {
            ClientCommand::Pause(super::super::messages::RemoteControlCommand {
                id: "pause".to_string(),
                target: target.to_string(),
            })
        };

        let response = handle_command(pause(&speaker.public_id), remote.connection_id).await;
        assert!(response.error.is_none(), "{:?}", response.error);
        let OutgoingMessage::Command(ForwardedCommand::Pause { from }) =
            speaker.command_rx.try_recv()?
        else {
            anyhow::bail!("speaker should receive the pause");
        };
        assert_eq!(from.as_deref(), Some(remote.public_id.as_str()));

        let own = handle_command(pause(&remote.public_id), remote.connection_id).await;
        assert_eq!(own.error.as_deref(), Some("cannot target self"));
        let foreign = handle_command(pause(&bobs.public_id), remote.connection_id).await;
        assert_eq!(
            foreign.error.as_deref(),
            Some("not authorized to control target")
        );

        for connection in [remote, speaker, bobs] {
            registry::unregister(connection.connection_id).await;
        }
        Ok(())
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
