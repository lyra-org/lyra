// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The commands a connection sends and their responses.

use tokio::task::JoinSet;

use super::constants::RemoteAction;
use super::control::{
    self,
    Controller,
    RemoteCommand,
};
use super::messages::{
    ClientCommand,
    OutgoingMessage,
    ResponseMessage,
};
use super::registry::{
    self,
    ConnectionId,
};

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
    let (id, target, command) = match cmd {
        ClientCommand::DeclareCapabilities { id, commands } => {
            return handle_declare_capabilities(id, commands, connection_id).await;
        }
        ClientCommand::Play(c) => (c.id, c.target, RemoteCommand::Play),
        ClientCommand::PlayTracks(c) => (
            c.id,
            c.target,
            RemoteCommand::PlayTracks {
                track_ids: c.track_ids,
                placement: c.placement,
                start_index: c.start_index,
                start_position_ms: c.start_position_ms,
            },
        ),
        ClientCommand::Pause(c) => (c.id, c.target, RemoteCommand::Pause),
        ClientCommand::Unpause(c) => (c.id, c.target, RemoteCommand::Unpause),
        ClientCommand::Stop(c) => (c.id, c.target, RemoteCommand::Stop),
        ClientCommand::Seek(c) => (
            c.id,
            c.target,
            RemoteCommand::Seek {
                position_ms: c.position_ms,
            },
        ),
        ClientCommand::NextTrack(c) => (c.id, c.target, RemoteCommand::NextTrack),
        ClientCommand::PreviousTrack(c) => (c.id, c.target, RemoteCommand::PreviousTrack),
        ClientCommand::SetVolume(c) => {
            (c.id, c.target, RemoteCommand::SetVolume { level: c.level })
        }
        ClientCommand::HandoffQueue(c) => (
            c.id,
            c.target,
            RemoteCommand::HandoffQueue {
                playback_id: c.playback_id,
                queue_revision: c.queue_revision,
            },
        ),
    };
    match control::send(Controller::Connection(connection_id), &target, command).await {
        Ok(()) => ResponseMessage::ok(id),
        Err(error) => ResponseMessage::error(id, error.to_string()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::remote::messages::ForwardedCommand;
    use crate::services::remote::testing as fixture;
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
        crate::services::testing::init_default_test_state()?;
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

    #[tokio::test]
    async fn forwarded_commands_name_the_source_by_its_public_id() -> anyhow::Result<()> {
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
}
