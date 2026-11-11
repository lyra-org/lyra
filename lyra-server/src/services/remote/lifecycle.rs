// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! What happens around a connection's life: its registration, auth rechecks and close.

use std::collections::HashSet;

use super::registry::{
    self,
    ConnectionId,
};
use crate::services::STATE;
use crate::services::auth;
use crate::services::playback_sessions::{
    self as playbacks,
    unsubscribe_playback_events,
};
use crate::services::playbacks::bindings::{
    self,
    ConnectionKey,
};

pub(crate) enum AuthStatus {
    /// Carries the re-resolved library access set so a long-lived connection
    /// picks up grants and revocations made after it was established.
    Valid(HashSet<String>),
    Revoked,
    Error,
}

pub(crate) async fn check_auth(holder: &auth::CredentialHolder) -> AuthStatus {
    let principal = {
        let db = STATE.db.read().await;
        holder.principal(&*db)
    };
    match principal {
        Ok(Some(principal)) => {
            if let auth::AuthCredential::Session { public_id } = holder.credential() {
                auth::sessions::touch_last_seen(public_id).await;
            }
            AuthStatus::Valid(principal.accessible_library_ids)
        }
        Ok(None) => AuthStatus::Revoked,
        Err(error) => {
            tracing::warn!(error = %error, "websocket auth check failed");
            AuthStatus::Error
        }
    }
}

/// A connection taken out of the registry, with the epoch its session key's binding had just
/// before. A report binding the key after that changes the epoch, so the close leaves it alone.
struct ClosedConnection {
    handle: registry::ConnectionHandle,
    binding_epoch: Option<u64>,
}

async fn close(connection_id: ConnectionId) -> Option<ClosedConnection> {
    close_then(connection_id, async {}).await
}

/// [`close`], running `after_removal` once the connection has left the registry.
async fn close_then(
    connection_id: ConnectionId,
    after_removal: impl Future<Output = ()>,
) -> Option<ClosedConnection> {
    let (handle, binding_epoch) = registry::unregister_with(connection_id, |handle| {
        bindings::get(&ConnectionKey::new(
            &handle.user_public_id,
            &handle.session_key,
        ))
        .map(|binding| binding.current_binding_epoch)
    })
    .await?;
    after_removal.await;
    Some(ClosedConnection {
        handle,
        binding_epoch,
    })
}

async fn pause_playback_of_disconnected_connection(closed: &ClosedConnection) {
    let handle = &closed.handle;
    let now_ms = match playbacks::now_ms() {
        Ok(now_ms) => now_ms,
        Err(err) => {
            tracing::warn!(
                connection_id = handle.connection_id,
                user_public_id = %handle.user_public_id,
                session_key = %handle.session_key,
                error = %err,
                "failed to timestamp disconnected connection"
            );
            return;
        }
    };

    let update = {
        let mut db = STATE.db.write().await;
        crate::services::playbacks::pause_disconnected_connection(
            &mut db,
            &handle.user_public_id,
            &handle.session_key,
            closed.binding_epoch,
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
                "failed to pause playback of disconnected connection"
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

/// Releases what a closed connection held and pauses the playback it was driving.
pub(crate) async fn disconnect(connection_id: ConnectionId, user_public_id: &str) {
    unsubscribe_playback_events(user_public_id);
    if let Some(closed) = close(connection_id).await {
        tracing::info!(
            connection_id,
            user_public_id = %closed.handle.user_public_id,
            session_key = %closed.handle.session_key,
            "websocket disconnected"
        );
        pause_playback_of_disconnected_connection(&closed).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lyra_db::PlaybackState;
    use tokio::sync::watch;

    /// A valid auth recheck returns the principal's current library access set.
    #[tokio::test]
    async fn check_auth_valid_carries_fresh_accessible_library_ids() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_library_test_state(std::path::PathBuf::from(".")).await?;

        let user_db_id = {
            let mut db = STATE.db.write().await;
            lyra_db::users::create(&mut db, &lyra_db::fixtures::test_user("ws-access")?)?
        };
        let token = crate::services::testing::create_session(user_db_id, Default::default())
            .await?
            .token;
        let holder = auth::require_auth(Some(&token)).await?.holder();

        assert!(matches!(
            check_auth(&holder).await,
            AuthStatus::Valid(ref ids) if ids.is_empty()
        ));

        let library_id = {
            let mut db = STATE.db.write().await;
            let path =
                std::path::PathBuf::from(format!("/tmp/lyra-ws-access-{}", lyra_db::nanoid()));
            let insert = lyra_db::libraries::LibraryInsert {
                id: lyra_db::nanoid(),
                name: "WS Access".to_string(),
                path_key: lyra_db::libraries::path_key_for(&path),
                path,
                language: None,
                country: None,
                follow_links: false,
            };
            db.transaction_mut(|t| -> anyhow::Result<String> {
                Ok(lyra_db::libraries::create_with_creator(t, insert, user_db_id)?.id)
            })?
        };

        let AuthStatus::Valid(refreshed) = check_auth(&holder).await else {
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
            let track_id = lyra_db::lookup::find_id_by_db_id(&*db, track_db_id)?.expect("track id");
            let bob_public_id =
                lyra_db::lookup::find_id_by_db_id(&*db, bob_db_id)?.expect("bob's id");
            crate::services::playbacks::create_playback(
                &mut db,
                crate::services::playbacks::CreatePlaybackRequest {
                    id: lyra_db::nanoid(),
                    play_id: lyra_db::nanoid(),
                    user_db_id: bob_db_id,
                    client_name: None,
                    queue: crate::services::playbacks::ValidatedQueue {
                        snapshot: crate::services::playbacks::QueueSnapshot::single(track_id),
                        current_track_db_id: track_db_id,
                    },
                    mutation: playbacks::PlaybackMutation {
                        position_ms: Some(0),
                        duration_ms: Some(200_000),
                        state: Some(PlaybackState::Playing),
                        ..playbacks::PlaybackMutation::default()
                    },
                    now_ms: playbacks::now_ms()?,
                    reporting_connection: Some(crate::services::playbacks::ReportingConnection {
                        user_public_id: bob_public_id,
                        session_key: "device".to_string(),
                    }),
                },
            )?
            .session
        };

        let closed = close(alice_connection.connection_id)
            .await
            .expect("alice's connection is registered");
        pause_playback_of_disconnected_connection(&closed).await;

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
    async fn a_report_once_the_close_began_keeps_its_playback_playing() -> anyhow::Result<()> {
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
        let connection = registry::register(
            alice_public_id.clone(),
            None,
            "device".to_string(),
            watch::channel(None).0,
        )
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let play = |db: &mut agdb::DbAny, now_ms: u64| -> anyhow::Result<_> {
            let track_db_id = lyra_db::fixtures::insert_track(db, "Track")?;
            let track_id = lyra_db::lookup::find_id_by_db_id(&*db, track_db_id)?.expect("track id");
            Ok(crate::services::playbacks::create_playback(
                db,
                crate::services::playbacks::CreatePlaybackRequest {
                    id: lyra_db::nanoid(),
                    play_id: lyra_db::nanoid(),
                    user_db_id: alice_db_id,
                    client_name: None,
                    queue: crate::services::playbacks::ValidatedQueue {
                        snapshot: crate::services::playbacks::QueueSnapshot::single(track_id),
                        current_track_db_id: track_db_id,
                    },
                    mutation: playbacks::PlaybackMutation {
                        position_ms: Some(0),
                        duration_ms: Some(200_000),
                        state: Some(PlaybackState::Playing),
                        ..playbacks::PlaybackMutation::default()
                    },
                    now_ms,
                    reporting_connection: Some(crate::services::playbacks::ReportingConnection {
                        user_public_id: alice_public_id.clone(),
                        session_key: "device".to_string(),
                    }),
                },
            )?
            .session)
        };
        let now_ms = playbacks::now_ms()?;
        play(&mut *STATE.db.write().await, now_ms)?;

        let mut after_close = None;
        let closed = close_then(connection.connection_id, async {
            after_close = Some(play(&mut *STATE.db.write().await, now_ms + 1));
        })
        .await
        .expect("the connection is registered");
        let after_close = after_close.expect("the report ran")?;
        pause_playback_of_disconnected_connection(&closed).await;

        let db = STATE.db.read().await;
        let state = |session: &playbacks::PlaybackRecord| -> anyhow::Result<_> {
            Ok(
                lyra_db::playback_sessions::get_by_id(&*db, session.playback_session_id)?
                    .expect("the session persists")
                    .state,
            )
        };
        assert_eq!(
            state(&after_close)?,
            PlaybackState::Playing,
            "a report accepted after the close is not paused by it"
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
}
