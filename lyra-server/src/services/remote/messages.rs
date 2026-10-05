// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use crate::services::remote::constants::RemoteAction;
#[cfg(feature = "docgen")]
use asyncapi_rust::{
    AsyncApi,
    ToAsyncApiMessage,
};
use serde::{
    Deserialize,
    Serialize,
};
use serde_json::Value;

/// Typed command envelope — each action carries its own payload shape.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "docgen", derive(ToAsyncApiMessage))]
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub(crate) enum ClientCommand {
    #[cfg_attr(
        feature = "docgen",
        asyncapi(summary = "Declare which remote control commands this connection supports")
    )]
    DeclareCapabilities {
        id: String,
        commands: Vec<RemoteAction>,
    },
    #[cfg_attr(feature = "docgen", asyncapi(summary = "Start playback on target"))]
    Play(RemoteControlCommand),
    #[cfg_attr(feature = "docgen", asyncapi(summary = "Pause playback on target"))]
    Pause(RemoteControlCommand),
    #[cfg_attr(feature = "docgen", asyncapi(summary = "Resume playback on target"))]
    Unpause(RemoteControlCommand),
    #[cfg_attr(feature = "docgen", asyncapi(summary = "Stop playback on target"))]
    Stop(RemoteControlCommand),
    #[cfg_attr(feature = "docgen", asyncapi(summary = "Seek to position on target"))]
    Seek(SeekCommand),
    #[cfg_attr(feature = "docgen", asyncapi(summary = "Skip to next track on target"))]
    NextTrack(RemoteControlCommand),
    #[cfg_attr(
        feature = "docgen",
        asyncapi(summary = "Go to previous track on target")
    )]
    PreviousTrack(RemoteControlCommand),
    #[cfg_attr(feature = "docgen", asyncapi(summary = "Set volume level on target"))]
    SetVolume(SetVolumeCommand),
    #[cfg_attr(
        feature = "docgen",
        asyncapi(summary = "Ask target to fetch and apply an exact durable playback queue")
    )]
    HandoffQueue(HandoffQueueCommand),
}

impl ClientCommand {
    pub(crate) fn id(&self) -> &str {
        match self {
            Self::DeclareCapabilities { id, .. } => id,
            Self::Play(c)
            | Self::Pause(c)
            | Self::Unpause(c)
            | Self::Stop(c)
            | Self::NextTrack(c)
            | Self::PreviousTrack(c) => &c.id,
            Self::Seek(c) => &c.id,
            Self::SetVolume(c) => &c.id,
            Self::HandoffQueue(c) => &c.id,
        }
    }

    pub(crate) fn remote_action(&self) -> Option<RemoteAction> {
        match self {
            Self::Play(_) => Some(RemoteAction::Play),
            Self::Pause(_) => Some(RemoteAction::Pause),
            Self::Unpause(_) => Some(RemoteAction::Unpause),
            Self::Stop(_) => Some(RemoteAction::Stop),
            Self::Seek(_) => Some(RemoteAction::Seek),
            Self::NextTrack(_) => Some(RemoteAction::NextTrack),
            Self::PreviousTrack(_) => Some(RemoteAction::PreviousTrack),
            Self::SetVolume(_) => Some(RemoteAction::SetVolume),
            Self::HandoffQueue(_) => Some(RemoteAction::HandoffQueue),
            Self::DeclareCapabilities { .. } => None,
        }
    }

    pub(crate) fn target(&self) -> Option<&str> {
        match self {
            Self::Play(c)
            | Self::Pause(c)
            | Self::Unpause(c)
            | Self::Stop(c)
            | Self::NextTrack(c)
            | Self::PreviousTrack(c) => Some(&c.target),
            Self::Seek(c) => Some(&c.target),
            Self::SetVolume(c) => Some(&c.target),
            Self::HandoffQueue(c) => Some(&c.target),
            Self::DeclareCapabilities { .. } => None,
        }
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct RemoteControlCommand {
    pub(crate) id: String,
    pub(crate) target: String,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SeekCommand {
    pub(crate) id: String,
    pub(crate) target: String,
    pub(crate) position_ms: u64,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SetVolumeCommand {
    pub(crate) id: String,
    pub(crate) target: String,
    pub(crate) level: f32,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct HandoffQueueCommand {
    pub(crate) id: String,
    pub(crate) target: String,
    pub(crate) playback_id: String,
    pub(crate) queue_revision: u64,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "docgen", derive(ToAsyncApiMessage))]
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum OutgoingMessage {
    #[cfg_attr(feature = "docgen", asyncapi(summary = "Response to a client command"))]
    Response(ResponseMessage),
    #[cfg_attr(
        feature = "docgen",
        asyncapi(summary = "Server-initiated playback state event")
    )]
    Event(EventMessage),
    #[cfg_attr(
        feature = "docgen",
        asyncapi(summary = "Remote control command forwarded from another connection")
    )]
    Command(ForwardedCommand),
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ForwardedCommand {
    Play {
        #[serde(skip_serializing_if = "Option::is_none")]
        from: Option<u64>,
    },
    Pause {
        #[serde(skip_serializing_if = "Option::is_none")]
        from: Option<u64>,
    },
    Unpause {
        #[serde(skip_serializing_if = "Option::is_none")]
        from: Option<u64>,
    },
    Stop {
        #[serde(skip_serializing_if = "Option::is_none")]
        from: Option<u64>,
    },
    Seek {
        #[serde(skip_serializing_if = "Option::is_none")]
        from: Option<u64>,
        position_ms: u64,
    },
    NextTrack {
        #[serde(skip_serializing_if = "Option::is_none")]
        from: Option<u64>,
    },
    PreviousTrack {
        #[serde(skip_serializing_if = "Option::is_none")]
        from: Option<u64>,
    },
    SetVolume {
        #[serde(skip_serializing_if = "Option::is_none")]
        from: Option<u64>,
        level: f32,
    },
    HandoffQueue {
        #[serde(skip_serializing_if = "Option::is_none")]
        from: Option<u64>,
        playback_id: String,
        queue_revision: u64,
        /// Opaque correlation token whose exact progress report completes the handoff.
        handoff_token: String,
    },
}

/// Response to a client command, correlated by `id`.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ResponseMessage {
    pub(crate) id: String,
    pub(crate) status: ResponseStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ResponseStatus {
    Ok,
    Error,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct EventMessage {
    pub(crate) event: String,
    pub(crate) data: Value,
}

impl ResponseMessage {
    pub(crate) fn ok(id: String) -> Self {
        Self {
            id,
            status: ResponseStatus::Ok,
            error: None,
        }
    }

    pub(crate) fn error(id: String, error: impl Into<String>) -> Self {
        Self {
            id,
            status: ResponseStatus::Error,
            error: Some(error.into()),
        }
    }
}

/// AsyncAPI specification for the WebSocket remote control protocol.
#[cfg(feature = "docgen")]
#[allow(clippy::duplicated_attributes)]
#[derive(AsyncApi)]
#[asyncapi(
    title = "Lyra WebSocket Remote Control",
    version = "0.1.0",
    description = "WebSocket protocol for native playback session reporting and remote control between connections."
)]
#[asyncapi_server(
    name = "default",
    host = "localhost:3000",
    protocol = "ws",
    pathname = "/ws",
    description = "Lyra server"
)]
#[asyncapi_channel(name = "remote_control", address = "/ws")]
#[asyncapi_operation(name = "clientMessage", action = "send", channel = "remote_control")]
#[asyncapi_operation(name = "serverMessage", action = "receive", channel = "remote_control")]
#[asyncapi_messages(ClientCommand, OutgoingMessage)]
pub(crate) struct WsApiSpec;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_pause_command() {
        let json = r#"{"action":"pause","id":"abc","target":"2"}"#;
        let cmd: ClientCommand = serde_json::from_str(json).unwrap();
        assert_eq!(cmd.id(), "abc");
        assert_eq!(cmd.remote_action(), Some(RemoteAction::Pause));
        assert_eq!(cmd.target(), Some("2"));
    }

    #[test]
    fn serialize_forwarded_command_omits_from_when_none() {
        let msg = OutgoingMessage::Command(ForwardedCommand::Pause { from: None });
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["action"], "pause");
        assert!(json.get("from").is_none());
    }

    #[test]
    fn forwarded_commands_reject_mismatched_payloads() {
        for json in [
            r#"{"type":"command","action":"seek","from":1}"#,
            r#"{"type":"command","action":"pause","from":1,"position_ms":30000}"#,
        ] {
            assert!(
                serde_json::from_str::<OutgoingMessage>(json).is_err(),
                "mismatched payload must fail: {json}"
            );
        }
    }
}
