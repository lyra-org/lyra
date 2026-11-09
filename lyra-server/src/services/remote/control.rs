// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Remote control of a user's connections, by another connection or on the user's behalf.

use super::constants::{
    MAX_PLAY_TRACK_IDS,
    PlayPlacement,
    RemoteAction,
};
use super::handoffs;
use super::messages::{
    ForwardedCommand,
    OutgoingMessage,
};
use super::registry::{
    self,
    ConnectionId,
    ConnectionSnapshot,
};
use crate::services::STATE;
use crate::services::playback_sessions as playbacks;
use crate::services::playbacks::{
    self as queues,
    bindings,
};

/// Who sends a remote control command.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Controller<'a> {
    /// A connection, which may not target itself and is named as the command's source.
    Connection(ConnectionId),
    /// The user with this public id, acting without a connection.
    User(&'a str),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RemoteCommand {
    Play,
    /// `track_ids` holds 1 to [`MAX_PLAY_TRACK_IDS`] ids, and `start_index` indexes it.
    PlayTracks {
        track_ids: Vec<String>,
        placement: PlayPlacement,
        start_index: Option<u64>,
        start_position_ms: Option<u64>,
    },
    Pause,
    Unpause,
    Stop,
    Seek {
        position_ms: u64,
    },
    NextTrack,
    PreviousTrack,
    /// The level is clamped to `0.0..=1.0`.
    SetVolume {
        level: f32,
    },
    HandoffQueue {
        playback_id: String,
        queue_revision: u64,
    },
}

impl RemoteCommand {
    pub(crate) fn action(&self) -> RemoteAction {
        match self {
            Self::Play => RemoteAction::Play,
            Self::PlayTracks { .. } => RemoteAction::PlayTracks,
            Self::Pause => RemoteAction::Pause,
            Self::Unpause => RemoteAction::Unpause,
            Self::Stop => RemoteAction::Stop,
            Self::Seek { .. } => RemoteAction::Seek,
            Self::NextTrack => RemoteAction::NextTrack,
            Self::PreviousTrack => RemoteAction::PreviousTrack,
            Self::SetVolume { .. } => RemoteAction::SetVolume,
            Self::HandoffQueue { .. } => RemoteAction::HandoffQueue,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ControlError {
    #[error("level must be a number")]
    InvalidLevel,
    #[error("{0}")]
    InvalidPlayTracks(String),
    #[error("a track is missing or not visible to the target's user")]
    TrackUnavailable,
    #[error("source connection not found")]
    SourceNotFound,
    #[error("target connection not found")]
    TargetNotFound,
    #[error("cannot target self")]
    SelfTarget,
    #[error("not authorized to control target")]
    NotAuthorized,
    #[error("target does not support command: {}", .0.as_str())]
    Unsupported(RemoteAction),
    #[error("user not found")]
    UserNotFound,
    #[error("could not queue message for target connection")]
    TargetBusy,
    #[error("{0}")]
    Handoff(String),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

/// Sends `command` to the controller's connection with the public id `target`, and returns once
/// the target has the command queued, or for a handoff once the target applied it.
pub(crate) async fn send(
    controller: Controller<'_>,
    target: &str,
    command: RemoteCommand,
) -> Result<(), ControlError> {
    if let RemoteCommand::SetVolume { level } = command
        && level.is_nan()
    {
        return Err(ControlError::InvalidLevel);
    }
    if let RemoteCommand::PlayTracks {
        track_ids,
        start_index,
        ..
    } = &command
    {
        check_play_tracks_shape(track_ids, *start_index)?;
    }
    let (source, target) = resolve(controller, target).await?;
    let action = command.action();
    if !target.supported_commands.contains(&action) {
        return Err(ControlError::Unsupported(action));
    }

    let from = source.as_ref().map(|source| source.public_id.clone());
    let forwarded = match command {
        RemoteCommand::HandoffQueue {
            playback_id,
            queue_revision,
        } => {
            return handoffs::dispatch_and_wait(
                source.as_ref(),
                &target,
                &playback_id,
                queue_revision,
            )
            .await
            .map_err(ControlError::Handoff);
        }
        RemoteCommand::Play => ForwardedCommand::Play { from },
        RemoteCommand::PlayTracks {
            track_ids,
            placement,
            start_index,
            start_position_ms,
        } => ForwardedCommand::PlayTracks {
            from,
            track_ids,
            placement,
            start_index,
            start_position_ms,
        },
        RemoteCommand::Pause => ForwardedCommand::Pause { from },
        RemoteCommand::Unpause => ForwardedCommand::Unpause { from },
        RemoteCommand::Stop => ForwardedCommand::Stop { from },
        RemoteCommand::Seek { position_ms } => ForwardedCommand::Seek { from, position_ms },
        RemoteCommand::NextTrack => ForwardedCommand::NextTrack { from },
        RemoteCommand::PreviousTrack => ForwardedCommand::PreviousTrack { from },
        RemoteCommand::SetVolume { level } => ForwardedCommand::SetVolume {
            from,
            level: level.clamp(0.0, 1.0),
        },
    };

    // Held through the send, so a user deleted meanwhile commands nothing. Handoffs verify their
    // owner under their own guard.
    let db = STATE.db.read().await;
    let target_user_db_id = lyra_db::users::find_db_id_by_public_id(&*db, &target.user_public_id)?
        .ok_or(ControlError::UserNotFound)?;
    // Only after `resolve`, so a controller that may not command the target learns nothing about
    // the tracks it named.
    if let ForwardedCommand::PlayTracks { track_ids, .. } = &forwarded
        && !queues::tracks_accessible_to_user(&*db, target_user_db_id, track_ids)
            .map_err(anyhow::Error::from)?
    {
        return Err(ControlError::TrackUnavailable);
    }
    registry::send_to_connection(target.connection_id, OutgoingMessage::Command(forwarded)).await?;
    drop(db);

    if let Ok(now_ms) = playbacks::now_ms() {
        bindings::mark_command_dispatched(
            &bindings::ConnectionKey::new(&target.user_public_id, &target.session_key),
            now_ms,
        );
    }
    Ok(())
}

fn check_play_tracks_shape(
    track_ids: &[String],
    start_index: Option<u64>,
) -> Result<(), ControlError> {
    if track_ids.is_empty() {
        return Err(ControlError::InvalidPlayTracks(
            "track_ids cannot be empty".to_string(),
        ));
    }
    if track_ids.len() > MAX_PLAY_TRACK_IDS {
        return Err(ControlError::InvalidPlayTracks(format!(
            "play_tracks takes at most {MAX_PLAY_TRACK_IDS} track ids, got {}",
            track_ids.len()
        )));
    }
    if let Some(start_index) = start_index
        && start_index >= track_ids.len() as u64
    {
        return Err(ControlError::InvalidPlayTracks(format!(
            "start_index {start_index} is outside track_ids length {}",
            track_ids.len()
        )));
    }
    Ok(())
}

/// The controlling connection, if any, and the target it may control.
async fn resolve(
    controller: Controller<'_>,
    target: &str,
) -> Result<(Option<ConnectionSnapshot>, ConnectionSnapshot), ControlError> {
    let (source, target, user_public_id) = match controller {
        Controller::Connection(connection_id) => {
            let (source, target) = registry::get_connection_and_target(connection_id, target).await;
            let source = source.ok_or(ControlError::SourceNotFound)?;
            let target = target.ok_or(ControlError::TargetNotFound)?;
            if target.connection_id == source.connection_id {
                return Err(ControlError::SelfTarget);
            }
            let user_public_id = source.user_public_id.clone();
            (Some(source), target, user_public_id)
        }
        Controller::User(user_public_id) => {
            let target = registry::resolve_public_id(target)
                .await
                .ok_or(ControlError::TargetNotFound)?;
            (None, target, user_public_id.to_string())
        }
    };
    if target.user_public_id != user_public_id {
        return Err(ControlError::NotAuthorized);
    }
    Ok((source, target))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::services::remote::testing as fixture;

    async fn declare(connection: &registry::RegisterResult, commands: &[RemoteAction]) {
        assert!(
            registry::set_supported_commands(
                connection.connection_id,
                commands.iter().copied().collect::<HashSet<_>>(),
            )
            .await
        );
    }

    #[tokio::test]
    async fn connections_control_only_their_users_other_connections() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        fixture::init().await?;
        let alice = fixture::user("alice").await?;
        let bob = fixture::user("bob").await?;
        let remote = fixture::connect(&alice, "remote").await?;
        let mut speaker = fixture::connect(&alice, "speaker").await?;
        let bobs = fixture::connect(&bob, "speaker").await?;
        for connection in [&remote, &speaker, &bobs] {
            declare(connection, &[RemoteAction::Pause]).await;
        }
        let pause = |target: &str| {
            let target = target.to_string();
            async move {
                send(
                    Controller::Connection(remote.connection_id),
                    &target,
                    RemoteCommand::Pause,
                )
                .await
            }
        };

        pause(&speaker.public_id).await?;
        let OutgoingMessage::Command(ForwardedCommand::Pause { from }) =
            speaker.command_rx.try_recv()?
        else {
            anyhow::bail!("speaker should receive the pause");
        };
        assert_eq!(from.as_deref(), Some(remote.public_id.as_str()));

        assert!(matches!(
            pause(&remote.public_id).await,
            Err(ControlError::SelfTarget)
        ));
        assert!(matches!(
            pause(&bobs.public_id).await,
            Err(ControlError::NotAuthorized)
        ));
        assert!(matches!(
            pause("missing").await,
            Err(ControlError::TargetNotFound)
        ));

        for connection in [remote, speaker, bobs] {
            registry::unregister(connection.connection_id).await;
        }
        Ok(())
    }

    #[tokio::test]
    async fn users_control_their_connections_without_a_source() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        fixture::init().await?;
        let alice = fixture::user("alice").await?;
        let bob = fixture::user("bob").await?;
        let mut speaker = fixture::connect(&alice, "speaker").await?;
        let bobs = fixture::connect(&bob, "speaker").await?;
        for connection in [&speaker, &bobs] {
            declare(connection, &[RemoteAction::SetVolume]).await;
        }
        let alice_controls = Controller::User(&alice.principal.user_public_id);

        send(
            alice_controls,
            &speaker.public_id,
            RemoteCommand::SetVolume { level: 2.0 },
        )
        .await?;
        let OutgoingMessage::Command(ForwardedCommand::SetVolume { from, level }) =
            speaker.command_rx.try_recv()?
        else {
            anyhow::bail!("speaker should receive the volume");
        };
        assert_eq!((from, level), (None, 1.0));

        let nan = send(
            alice_controls,
            &speaker.public_id,
            RemoteCommand::SetVolume { level: f32::NAN },
        )
        .await;
        assert!(matches!(nan, Err(ControlError::InvalidLevel)));
        assert!(speaker.command_rx.try_recv().is_err());
        let foreign = send(
            alice_controls,
            &bobs.public_id,
            RemoteCommand::SetVolume { level: 0.5 },
        )
        .await;
        assert!(matches!(foreign, Err(ControlError::NotAuthorized)));

        for connection in [speaker, bobs] {
            registry::unregister(connection.connection_id).await;
        }
        Ok(())
    }

    #[tokio::test]
    async fn targets_accept_only_declared_commands() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        fixture::init().await?;
        let alice = fixture::user("alice").await?;
        let mut speaker = fixture::connect(&alice, "speaker").await?;
        declare(&speaker, &[RemoteAction::Pause]).await;

        let error = send(
            Controller::User(&alice.principal.user_public_id),
            &speaker.public_id,
            RemoteCommand::Stop,
        )
        .await
        .expect_err("stop was not declared");
        assert!(matches!(
            error,
            ControlError::Unsupported(RemoteAction::Stop)
        ));
        assert_eq!(error.to_string(), "target does not support command: stop");
        assert!(speaker.command_rx.try_recv().is_err());

        registry::unregister(speaker.connection_id).await;
        Ok(())
    }

    fn play_tracks(track_ids: &[&str]) -> RemoteCommand {
        RemoteCommand::PlayTracks {
            track_ids: track_ids.iter().map(ToString::to_string).collect(),
            placement: PlayPlacement::Next,
            start_index: None,
            start_position_ms: None,
        }
    }

    #[tokio::test]
    async fn play_tracks_forwards_only_to_a_target_that_declared_it() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        fixture::init().await?;
        let alice = fixture::user("alice").await?;
        let remote = fixture::connect(&alice, "remote").await?;
        let mut speaker = fixture::connect(&alice, "speaker").await?;
        let track = fixture::track("Track").await?;
        declare(&speaker, &[RemoteAction::Pause]).await;

        let source = Controller::Connection(remote.connection_id);
        let undeclared = send(source, &speaker.public_id, play_tracks(&[&track])).await;
        assert!(matches!(
            undeclared,
            Err(ControlError::Unsupported(RemoteAction::PlayTracks))
        ));
        assert!(speaker.command_rx.try_recv().is_err());

        declare(&speaker, &[RemoteAction::PlayTracks]).await;
        send(
            source,
            &speaker.public_id,
            RemoteCommand::PlayTracks {
                track_ids: vec![track.clone(), track.clone()],
                placement: PlayPlacement::Now,
                start_index: Some(1),
                start_position_ms: Some(2500),
            },
        )
        .await?;
        let OutgoingMessage::Command(ForwardedCommand::PlayTracks {
            from,
            track_ids,
            placement,
            start_index,
            start_position_ms,
        }) = speaker.command_rx.try_recv()?
        else {
            anyhow::bail!("speaker should receive the tracks");
        };
        assert_eq!(from.as_deref(), Some(remote.public_id.as_str()));
        assert_eq!(track_ids, [track.clone(), track]);
        assert_eq!(placement, PlayPlacement::Now);
        assert_eq!((start_index, start_position_ms), (Some(1), Some(2500)));

        for connection in [remote, speaker] {
            registry::unregister(connection.connection_id).await;
        }
        Ok(())
    }

    #[tokio::test]
    async fn play_tracks_rejects_malformed_commands() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        fixture::init().await?;
        let alice = fixture::user("alice").await?;
        let mut speaker = fixture::connect(&alice, "speaker").await?;
        let track = fixture::track("Track").await?;
        declare(&speaker, &[RemoteAction::PlayTracks]).await;
        let controller = Controller::User(&alice.principal.user_public_id);

        let too_many = vec![track.as_str(); MAX_PLAY_TRACK_IDS + 1];
        for (command, expected) in [
            (play_tracks(&[]), "track_ids cannot be empty".to_string()),
            (
                play_tracks(&too_many),
                format!(
                    "play_tracks takes at most {MAX_PLAY_TRACK_IDS} track ids, got {}",
                    MAX_PLAY_TRACK_IDS + 1
                ),
            ),
            (
                RemoteCommand::PlayTracks {
                    track_ids: vec![track.clone()],
                    placement: PlayPlacement::Now,
                    start_index: Some(1),
                    start_position_ms: None,
                },
                "start_index 1 is outside track_ids length 1".to_string(),
            ),
        ] {
            let error = send(controller, &speaker.public_id, command)
                .await
                .expect_err("the command is malformed");
            assert!(matches!(error, ControlError::InvalidPlayTracks(_)));
            assert_eq!(error.to_string(), expected);
        }

        let at_the_limit = vec![track.as_str(); MAX_PLAY_TRACK_IDS];
        send(controller, &speaker.public_id, play_tracks(&at_the_limit)).await?;
        assert!(speaker.command_rx.try_recv().is_ok());
        assert!(speaker.command_rx.try_recv().is_err());

        registry::unregister(speaker.connection_id).await;
        Ok(())
    }

    #[tokio::test]
    async fn play_tracks_needs_every_track_visible_to_the_target_user() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        fixture::init().await?;
        let listener = fixture::listener("listener").await?;
        let admin = fixture::user("admin").await?;
        let mut speaker = fixture::connect(&listener, "speaker").await?;
        let mut admins = fixture::connect(&admin, "speaker").await?;
        declare(&speaker, &[RemoteAction::PlayTracks]).await;
        declare(&admins, &[RemoteAction::PlayTracks]).await;
        let hidden = fixture::track("Hidden").await?;

        let controller = Controller::User(&listener.principal.user_public_id);
        for id in [hidden.as_str(), "missing"] {
            let error = send(controller, &speaker.public_id, play_tracks(&[id]))
                .await
                .expect_err("the listener can't play it");
            assert!(matches!(error, ControlError::TrackUnavailable));
            assert_eq!(
                error.to_string(),
                "a track is missing or not visible to the target's user"
            );
        }
        assert!(speaker.command_rx.try_recv().is_err());

        send(
            Controller::User(&admin.principal.user_public_id),
            &admins.public_id,
            play_tracks(&[&hidden]),
        )
        .await?;
        assert!(admins.command_rx.try_recv().is_ok());

        for connection in [speaker, admins] {
            registry::unregister(connection.connection_id).await;
        }
        Ok(())
    }

    #[tokio::test]
    async fn play_tracks_tells_a_foreign_controller_nothing_about_the_tracks() -> anyhow::Result<()>
    {
        let _guard = crate::services::testing::runtime_test_lock().await;
        fixture::init().await?;
        let listener = fixture::listener("listener").await?;
        let admin = fixture::user("admin").await?;
        let mut speaker = fixture::connect(&listener, "speaker").await?;
        let mut admins = fixture::connect(&admin, "speaker").await?;
        declare(&speaker, &[RemoteAction::PlayTracks]).await;
        declare(&admins, &[RemoteAction::PlayTracks]).await;
        let track = fixture::track("Track").await?;

        for (controller, target) in [
            (&admin.principal.user_public_id, &speaker.public_id),
            (&listener.principal.user_public_id, &admins.public_id),
        ] {
            for id in [track.as_str(), "missing"] {
                let error = send(Controller::User(controller), target, play_tracks(&[id]))
                    .await
                    .expect_err("another user's connection");
                assert!(matches!(error, ControlError::NotAuthorized));
            }
        }
        assert!(speaker.command_rx.try_recv().is_err());
        assert!(admins.command_rx.try_recv().is_err());

        for connection in [speaker, admins] {
            registry::unregister(connection.connection_id).await;
        }
        Ok(())
    }

    #[tokio::test]
    async fn handoffs_wait_for_the_handoff_to_finish() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        fixture::init().await?;
        let alice = fixture::user("alice").await?;
        let remote = fixture::connect(&alice, "remote").await?;
        let mut speaker = fixture::connect(&alice, "speaker").await?;
        declare(&speaker, &[RemoteAction::HandoffQueue]).await;
        let (playback_id, _) = fixture::drive_new_playback(&alice, "remote").await?;

        let handoff = tokio::spawn({
            let target = speaker.public_id.clone();
            let playback_id = playback_id.clone();
            let source = remote.connection_id;
            async move {
                send(
                    Controller::Connection(source),
                    &target,
                    RemoteCommand::HandoffQueue {
                        playback_id,
                        queue_revision: 1,
                    },
                )
                .await
            }
        });
        let Some(OutgoingMessage::Command(ForwardedCommand::HandoffQueue {
            from,
            playback_id: handed_off,
            queue_revision,
            ..
        })) = speaker.command_rx.recv().await
        else {
            anyhow::bail!("speaker should receive the handoff");
        };
        assert_eq!(from.as_deref(), Some(remote.public_id.as_str()));
        assert_eq!(
            (handed_off.as_str(), queue_revision),
            (playback_id.as_str(), 1)
        );
        assert!(!handoff.is_finished(), "the handoff waits for the target");

        assert_eq!(handoffs::fail_for_playback(&playback_id).await, 1);
        let result = handoff.await?;
        assert!(
            matches!(&result, Err(ControlError::Handoff(message)) if message == "playback deleted during handoff"),
            "{result:?}"
        );

        for connection in [remote, speaker] {
            registry::unregister(connection.connection_id).await;
        }
        Ok(())
    }
}
