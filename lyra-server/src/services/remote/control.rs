// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Remote control of a user's connections, by another connection or on the user's behalf.

use super::constants::RemoteAction;
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
    if lyra_db::users::find_db_id_by_public_id(&*db, &target.user_public_id)?.is_none() {
        return Err(ControlError::UserNotFound);
    }
    registry::send_to_connection(target.connection_id, OutgoingMessage::Command(forwarded)).await?;
    drop(db);

    if let Ok(now_ms) = playbacks::now_ms() {
        let scope_key =
            playbacks::PlaybackScopeKey::native(&target.user_public_id, &target.session_key);
        playbacks::mark_command_dispatched(&scope_key, now_ms);
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
