// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use crate::services::playback_sessions::PlaybackUpdatePayload;
use crate::services::remote::connections::Connection;
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
        asyncapi(summary = "Server-initiated event, told apart by `event`")
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
        from: Option<String>,
    },
    Pause {
        #[serde(skip_serializing_if = "Option::is_none")]
        from: Option<String>,
    },
    Unpause {
        #[serde(skip_serializing_if = "Option::is_none")]
        from: Option<String>,
    },
    Stop {
        #[serde(skip_serializing_if = "Option::is_none")]
        from: Option<String>,
    },
    Seek {
        #[serde(skip_serializing_if = "Option::is_none")]
        from: Option<String>,
        position_ms: u64,
    },
    NextTrack {
        #[serde(skip_serializing_if = "Option::is_none")]
        from: Option<String>,
    },
    PreviousTrack {
        #[serde(skip_serializing_if = "Option::is_none")]
        from: Option<String>,
    },
    SetVolume {
        #[serde(skip_serializing_if = "Option::is_none")]
        from: Option<String>,
        level: f32,
    },
    HandoffQueue {
        #[serde(skip_serializing_if = "Option::is_none")]
        from: Option<String>,
        playback_id: String,
        queue_revision: u64,
        /// The current track's play, which the target continues by reporting progress with it.
        #[serde(skip_serializing_if = "Option::is_none")]
        play_id: Option<String>,
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
#[serde(tag = "event", content = "data", rename_all = "snake_case")]
pub(crate) enum EventMessage {
    /// A playback the connection may see changed state.
    PlaybackStateChanged(PlaybackUpdatePayload),
    /// Events were dropped; refetch state over REST.
    SyncRequired(SyncRequired),
    /// Sent first on every connection, describing the connection itself.
    Connected(Connection),
    /// One of the user's connections opened, declared its capabilities, started or stopped
    /// driving a playback, or had its remote control degrade or recover.
    ConnectionUpdated(Connection),
    /// One of the user's connections closed.
    ConnectionClosed(ConnectionClosed),
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct SyncRequired {}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ConnectionClosed {
    pub(crate) id: String,
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
    // The derive requires a literal; `lyra_server::asyncapi_spec` sets the package version.
    version = "0.0.0",
    description = "WebSocket protocol for native playback session reporting and remote control between connections. Connect to `/ws?ticket=<ticket>&session_key=<key>` with a ticket from `POST /api/me/connections/tickets`."
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
        let ClientCommand::Pause(cmd) = serde_json::from_str(json).unwrap() else {
            panic!("expected a pause command");
        };
        assert_eq!(cmd.id, "abc");
        assert_eq!(cmd.target, "2");
    }

    #[test]
    fn events_keep_the_event_and_data_envelope() {
        let closed = OutgoingMessage::Event(EventMessage::ConnectionClosed(ConnectionClosed {
            id: "conn".to_string(),
        }));
        assert_eq!(
            serde_json::to_value(&closed).unwrap(),
            serde_json::json!({
                "type": "event",
                "event": "connection_closed",
                "data": { "id": "conn" },
            })
        );
        let sync = OutgoingMessage::Event(EventMessage::SyncRequired(SyncRequired {}));
        assert_eq!(
            serde_json::to_value(&sync).unwrap(),
            serde_json::json!({ "type": "event", "event": "sync_required", "data": {} })
        );
    }

    #[test]
    fn serialize_forwarded_command_carries_the_public_source_id() {
        let msg = OutgoingMessage::Command(ForwardedCommand::Pause {
            from: Some("source-id".to_string()),
        });
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["from"], "source-id");
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
            r#"{"type":"command","action":"seek","from":"a"}"#,
            r#"{"type":"command","action":"pause","from":"a","position_ms":30000}"#,
        ] {
            assert!(
                serde_json::from_str::<OutgoingMessage>(json).is_err(),
                "mismatched payload must fail: {json}"
            );
        }
    }
}
