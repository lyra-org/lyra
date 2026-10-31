// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use harmony_luau as luau;
#[cfg(feature = "docgen")]
use harmony_luau::FunctionParameter;
use harmony_luau::{
    DescribeInterface,
    FieldDescriptor,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
};
#[cfg(feature = "docgen")]
use harmony_luau::{
    DescribeModule,
    DescribeTypeAlias,
    TypeAliasDescriptor,
    render_definition_file_with_support,
};
use serde::{
    Deserialize,
    Serialize,
};

use crate::plugins::args;
use crate::{
    plugins::{
        db::Db,
        types::{
            LuauPlaybackEvent,
            LuauPlaybackState,
        },
    },
    services::{
        auth::Principal,
        playback_sessions::{
            self as playbacks,
            PlaybackScopeKey,
        },
        remote::{
            connections,
            constants::RemoteAction,
            messages::{
                ForwardedCommand,
                OutgoingMessage,
            },
            registry,
        },
    },
};

#[cfg(feature = "docgen")]
struct PlaybackUpdateHandler;

#[derive(Clone, Debug, Deserialize)]
struct PlaybackSessionReportRequest {
    session_key: String,
    track_id: i64,
    play_id: Option<String>,
    event: Option<playbacks::PlaybackEvent>,
    position_ms: Option<u64>,
    duration_ms: Option<u64>,
    state: Option<lyra_db::PlaybackState>,
}

#[derive(Clone, Debug, Deserialize)]
struct SendCommandRequest {
    connection_id: String,
    action: String,
    position_ms: Option<u64>,
    level: Option<f32>,
    playback_id: Option<String>,
    queue_revision: Option<u64>,
}

#[derive(Serialize)]
struct PlaybackInfo {
    track_public_id: String,
    position_ms: u64,
    duration_ms: Option<u64>,
    state: String,
}

#[derive(Serialize)]
struct ConnectionInfo {
    id: String,
    session_key: String,
    client_name: Option<String>,
    supported_commands: Vec<String>,
    playback_id: Option<String>,
    playback: Option<PlaybackInfo>,
    remote_control_degraded: bool,
    connected_at_ms: u64,
}

#[cfg(feature = "docgen")]
impl LuauTypeInfo for PlaybackUpdateHandler {
    fn luau_type() -> LuauType {
        LuauType::literal("PlaybackUpdateHandler")
    }
}

#[cfg(feature = "docgen")]
impl PlaybackUpdateHandler {
    fn callback_type() -> LuauType {
        LuauType::function(
            vec![FunctionParameter {
                name: Some("payload"),
                ty: LuauType::literal("PlaybackUpdatePayload"),
                variadic: false,
            }],
            vec![],
        )
    }
}

#[cfg(feature = "docgen")]
impl DescribeTypeAlias for PlaybackUpdateHandler {
    fn type_alias_descriptor() -> TypeAliasDescriptor {
        TypeAliasDescriptor::new(
            "PlaybackUpdateHandler",
            Self::callback_type(),
            Some("Playback update callback."),
        )
    }
}

pub(crate) struct PlaybackSessionsModule;

#[harmony_macros::module(id = "lyra/playback_sessions", docs_feature = "docgen")]
impl PlaybackSessionsModule {
    #[harmony(
        description = "Registers a callback for every user's per-track playback accounting updates. Needs the `lyra.playback_sessions.all` scope too.",
        args(handler: PlaybackUpdateHandler)
    )]
    fn on_update(
        vm: &luau::Vm,
        context: &luau::CallContext,
        handler: luau::Function,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        crate::plugins::update_callbacks::register::<playbacks::PlaybackUpdatePayload>(
            vm,
            context,
            handler,
            "playback_sessions.on_update",
        )
    }

    #[harmony(
        description = "Reports an observation for a plugin's player context. Returns (public /api/me/playbacks ID, nil) when accepted, (nil, nil) when ignored, or (nil, rejection reason) when admission is refused. Internal failures raise an error. Track changes retain the context ID; stopped contexts expire after five minutes without an accepted current observation.",
        returns(Option<String>, Option<String>)
    )]
    async fn report_session(
        request: args::Json<PlaybackSessionReportRequest>,
        #[context] db: Db,
        #[context] principal: Principal,
        #[context] origin: luau::ChunkOrigin,
    ) -> luau::runtime::Result<(Option<String>, Option<String>)> {
        let request = request.0;
        let plugin_id = origin.plugin.ok_or_else(|| {
            luau::Error::Runtime(
                "playback_sessions.report_session must be called from plugin Luau code".into(),
            )
        })?;
        let session_key = require_non_empty_string(request.session_key, "session_key")?;
        let track_db_id = args::positive_id(request.track_id, "track_id")?;
        if let Some(play_id) = request.play_id.as_deref()
            && !crate::services::listens::is_play_id(play_id)
        {
            return Ok((None, Some(format!("malformed play_id: {play_id}"))));
        }
        let active_event = playbacks::classify_active_event(request.event)
            .map_err(crate::plugins::runtime_error)?;
        let current_ms = playbacks::now_ms().map_err(crate::plugins::runtime_error)?;
        let mutation = playback_mutation(request.position_ms, request.duration_ms, request.state);

        let mut db = db.write_outside_catalog().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        if !crate::services::auth::access::entity_accessible(&db, &principal, track_db_id)? {
            return Err(crate::plugins::runtime_error("track not found"));
        }

        let update = match playbacks::report_playback_session_with_cleanup(
            &mut db,
            playbacks::SessionPlaybackReportRequest {
                plugin_id: &plugin_id,
                user_db_id,
                session_key: &session_key,
                track_db_id,
                play_id: request.play_id.as_deref(),
                client_name: None,
                mutation,
                now_ms: current_ms,
                active_event,
            },
        ) {
            Ok(update) => update,
            Err(playbacks::PlaybackServiceError::BadRequest(message)) => {
                return Ok((None, Some(message)));
            }
            Err(error) => return Err(crate::plugins::runtime_error(error)),
        };
        let dispatch_caller = format!("plugin:{plugin_id}");
        playbacks::dispatch_evicted_updates_for_caller(
            dispatch_caller.clone(),
            update.evicted_playbacks,
        );

        let playbacks::OptionalPlaybackUpdateResult {
            playback, event, ..
        } = update;
        let Some(playback) = playback else {
            return Ok((None, None));
        };

        let playback_id = crate::services::playbacks::reported_id(
            &db,
            user_db_id,
            &plugin_id,
            &session_key,
            current_ms,
        )
        .map_err(crate::plugins::runtime_error)?
        .ok_or_else(|| crate::plugins::runtime_error("reported playback context not found"))?;
        let event_label = event
            .map(|value| value.to_string())
            .unwrap_or_else(|| active_event.to_string());
        drop(db);
        playbacks::dispatch_playback_update_for_caller(dispatch_caller, &playback, event_label);
        Ok((Some(playback_id), None))
    }

    #[harmony(
        description = "Lists the dispatch principal's open WebSocket connections, oldest first, with the playback each drives.",
        returns(Vec<ConnectionInfo>)
    )]
    async fn list_connections(
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::Value> {
        let snapshots = registry::list_user_connections(&principal.user_public_id).await;
        let now_ms = playbacks::now_ms().map_err(crate::plugins::runtime_error)?;
        let db = db.read().await;
        crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let connections = connections::join(&db, &principal, snapshots, now_ms)
            .map_err(crate::plugins::runtime_error)?;
        let mut result = Vec::with_capacity(connections.len());
        for connection in connections {
            let playback = connection
                .current_session(&db)
                .map_err(crate::plugins::runtime_error)?
                .map(|current| PlaybackInfo {
                    track_public_id: current.track_public_id,
                    position_ms: current.playback.position_ms,
                    duration_ms: current.playback.duration_ms,
                    state: playback_state_string(current.playback.state),
                });
            result.push(ConnectionInfo {
                playback_id: connection.playback_id().map(str::to_string),
                playback,
                id: connection.id,
                session_key: connection.session_key,
                client_name: connection.client_name,
                supported_commands: connection
                    .supported_commands
                    .iter()
                    .map(remote_action_string)
                    .collect(),
                remote_control_degraded: connection.remote_control_degraded,
                connected_at_ms: connection.connected_at_ms,
            });
        }
        drop(db);
        harmony_luau::serializable_to_luau_owned(&result)
    }

    #[harmony(description = "Sends a remote control command to a connection.")]
    async fn send_command(
        request: args::Json<SendCommandRequest>,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<()> {
        let request = request.0;
        // Held through the send, so a user deleted meanwhile commands nothing. Handoffs verify
        // their owner under their own guard.
        let db = db.read().await;
        crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let connection_id = require_non_empty_string(request.connection_id, "connection_id")?;
        let action_str = require_non_empty_string(request.action, "action")?;
        let action: RemoteAction = serde_json::from_value(serde_json::Value::String(
            action_str.clone(),
        ))
        .map_err(|_| crate::plugins::runtime_error(format!("unknown action: {action_str}")))?;

        let target = registry::resolve_public_id(&connection_id)
            .await
            .ok_or_else(|| crate::plugins::runtime_error("connection not found"))?;

        if principal.user_public_id != target.user_public_id {
            return Err(crate::plugins::runtime_error(
                "not authorized to control target",
            ));
        }
        if !target.supported_commands.contains(&action) {
            return Err(crate::plugins::runtime_error(format!(
                "target does not support command: {action_str}"
            )));
        }

        if action == RemoteAction::HandoffQueue {
            let playback_public_id = request.playback_id.as_deref().ok_or_else(|| {
                crate::plugins::runtime_error("playback_id required for handoff_queue")
            })?;
            let queue_revision = request.queue_revision.ok_or_else(|| {
                crate::plugins::runtime_error("queue_revision required for handoff_queue")
            })?;
            drop(db);
            crate::services::remote::handoffs::dispatch_and_wait(
                None,
                &target,
                playback_public_id,
                queue_revision,
            )
            .await
            .map_err(crate::plugins::runtime_error)?;
            return Ok(());
        }

        let forwarded = match action {
            RemoteAction::Seek => {
                let position_ms = request.position_ms.ok_or_else(|| {
                    crate::plugins::runtime_error("position_ms required for seek")
                })?;
                ForwardedCommand::Seek {
                    from: None,
                    position_ms,
                }
            }
            RemoteAction::SetVolume => {
                let level = request.level.ok_or_else(|| {
                    crate::plugins::runtime_error("level required for set_volume")
                })?;
                ForwardedCommand::SetVolume {
                    from: None,
                    level: level.clamp(0.0, 1.0),
                }
            }
            RemoteAction::Play => ForwardedCommand::Play { from: None },
            RemoteAction::Pause => ForwardedCommand::Pause { from: None },
            RemoteAction::Unpause => ForwardedCommand::Unpause { from: None },
            RemoteAction::Stop => ForwardedCommand::Stop { from: None },
            RemoteAction::NextTrack => ForwardedCommand::NextTrack { from: None },
            RemoteAction::PreviousTrack => ForwardedCommand::PreviousTrack { from: None },
            RemoteAction::HandoffQueue => unreachable!("handoff returns before direct dispatch"),
        };

        let forwarded = OutgoingMessage::Command(forwarded);
        registry::send_to_connection(target.connection_id, forwarded)
            .await
            .map_err(|error| {
                crate::plugins::runtime_error(format!("command dispatch failed: {error}"))
            })?;
        drop(db);

        if let Ok(now_ms) = playbacks::now_ms() {
            let scope_key = PlaybackScopeKey::native(&target.user_public_id, &target.session_key);
            playbacks::mark_command_dispatched(&scope_key, now_ms);
        }

        Ok(())
    }
}

fn playback_mutation(
    position_ms: Option<u64>,
    duration_ms: Option<u64>,
    state: Option<lyra_db::PlaybackState>,
) -> playbacks::PlaybackMutation {
    playbacks::PlaybackMutation {
        position_ms,
        duration_ms,
        state,
    }
}

fn require_non_empty_string(value: String, field_name: &str) -> luau::runtime::Result<String> {
    let value = value.trim().to_string();
    if value.is_empty() {
        return Err(crate::plugins::runtime_error(format!(
            "{field_name} must be a non-empty string"
        )));
    }
    Ok(value)
}

fn remote_action_string(action: &RemoteAction) -> String {
    serde_json::to_value(action)
        .ok()
        .and_then(|value| value.as_str().map(String::from))
        .unwrap_or_default()
}

fn playback_state_string(state: lyra_db::PlaybackState) -> String {
    serde_json::to_value(state)
        .ok()
        .and_then(|value| value.as_str().map(String::from))
        .unwrap_or_default()
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &PlaybackSessionsModule::module_descriptor(),
        &[
            PlaybackUpdateHandler::type_alias_descriptor(),
            LuauPlaybackEvent::type_alias_descriptor(),
            LuauPlaybackState::type_alias_descriptor(),
        ],
        &[
            LuauPlaybackUpdatePayload::interface_descriptor(),
            PlaybackSessionReportRequest::interface_descriptor(),
            SendCommandRequest::interface_descriptor(),
            ConnectionInfo::interface_descriptor(),
            PlaybackInfo::interface_descriptor(),
        ],
        &[],
    )
}

impl LuauTypeInfo for PlaybackSessionReportRequest {
    fn luau_type() -> LuauType {
        LuauType::literal("PlaybackSessionReportRequest")
    }
}

impl DescribeInterface for PlaybackSessionReportRequest {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new(
            "PlaybackSessionReportRequest",
            Some(
                "An observation keyed by the authenticated user, calling plugin, and external player session. A report with a new `play_id` starts a new track session; a report for a `play_id` whose session was replaced is ignored while the server keeps that session. Without a `play_id`, reports of one track belong to one play until a `started` event after that play counted.",
            ),
        );
        descriptor.fields.extend(playback_request_fields([
            FieldDescriptor {
                name: "session_key",
                ty: String::luau_type(),
                description: Some("Stable external player identity, retained across track changes and authentication-token refresh."),
            },
            FieldDescriptor {
                name: "track_id",
                ty: i64::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "play_id",
                ty: Option::<String>::luau_type(),
                description: Some(crate::services::listens::PLAY_ID_DESCRIPTION),
            },
            FieldDescriptor {
                name: "event",
                ty: Option::<LuauPlaybackEvent>::luau_type(),
                description: None,
            },
        ]));
        descriptor
    }
}

impl LuauTypeInfo for SendCommandRequest {
    fn luau_type() -> LuauType {
        LuauType::literal("SendCommandRequest")
    }
}

impl DescribeInterface for SendCommandRequest {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("SendCommandRequest", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "connection_id",
                ty: String::luau_type(),
                description: Some("The target connection's `id`."),
            },
            FieldDescriptor {
                name: "action",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "position_ms",
                ty: Option::<u64>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "level",
                ty: Option::<f32>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "playback_id",
                ty: Option::<String>::luau_type(),
                description: Some("Durable playback ID required for handoff_queue."),
            },
            FieldDescriptor {
                name: "queue_revision",
                ty: Option::<u64>::luau_type(),
                description: Some("Exact queue revision required for handoff_queue."),
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for PlaybackInfo {
    fn luau_type() -> LuauType {
        LuauType::literal("PlaybackInfo")
    }
}

impl DescribeInterface for PlaybackInfo {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("PlaybackInfo", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "track_public_id",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "position_ms",
                ty: u64::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "duration_ms",
                ty: Option::<u64>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "state",
                ty: String::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for ConnectionInfo {
    fn luau_type() -> LuauType {
        LuauType::literal("ConnectionInfo")
    }
}

impl DescribeInterface for ConnectionInfo {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new(
            "ConnectionInfo",
            Some("One of the dispatch principal's open WebSocket connections."),
        );
        descriptor.fields.extend([
            FieldDescriptor {
                name: "id",
                ty: String::luau_type(),
                description: Some("Public connection id, as `send_command` takes it."),
            },
            FieldDescriptor {
                name: "session_key",
                ty: String::luau_type(),
                description: Some("The session key the client chose when it connected."),
            },
            FieldDescriptor {
                name: "client_name",
                ty: Option::<String>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "supported_commands",
                ty: Vec::<String>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "playback_id",
                ty: Option::<String>::luau_type(),
                description: Some("The playback the connection drives; nil when it is idle or the playback is hidden from the principal."),
            },
            FieldDescriptor {
                name: "playback",
                ty: Option::<PlaybackInfo>::luau_type(),
                description: Some("The track session of `playback_id`."),
            },
            FieldDescriptor {
                name: "remote_control_degraded",
                ty: bool::luau_type(),
                description: Some("A command sent to the connection went unacknowledged for too long."),
            },
            FieldDescriptor {
                name: "connected_at_ms",
                ty: u64::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

fn playback_request_fields<const N: usize>(leading: [FieldDescriptor; N]) -> Vec<FieldDescriptor> {
    let mut fields = leading.into_iter().collect::<Vec<_>>();
    fields.extend([
        FieldDescriptor {
            name: "position_ms",
            ty: Option::<u64>::luau_type(),
            description: None,
        },
        FieldDescriptor {
            name: "duration_ms",
            ty: Option::<u64>::luau_type(),
            description: None,
        },
        FieldDescriptor {
            name: "state",
            ty: Option::<LuauPlaybackState>::luau_type(),
            description: None,
        },
    ]);
    fields
}

const _: fn(&crate::services::playback_sessions::PlaybackUpdatePayload) = |value| {
    let _ = (
        &value.event,
        &value.state,
        &value.playback_session_public_id,
        &value.track_public_id,
        &value.user_public_id,
        &value.library_public_id,
        &value.position_ms,
        &value.duration_ms,
        &value.played_ms,
        &value.updated_at_ms,
    );
};

/// [`crate::services::playback_sessions::PlaybackUpdatePayload`] as described to Luau.
#[cfg(feature = "docgen")]
pub(crate) struct LuauPlaybackUpdatePayload;

#[cfg(feature = "docgen")]
impl LuauTypeInfo for LuauPlaybackUpdatePayload {
    fn luau_type() -> LuauType {
        LuauType::literal("PlaybackUpdatePayload")
    }
}

#[cfg(feature = "docgen")]
impl DescribeInterface for LuauPlaybackUpdatePayload {
    fn interface_descriptor() -> InterfaceDescriptor {
        let field = |name: &'static str, ty: LuauType| FieldDescriptor {
            name,
            ty,
            description: None,
        };
        InterfaceDescriptor {
            name: "PlaybackUpdatePayload",
            description: None,
            fields: vec![
                field("event", String::luau_type()),
                field("state", LuauPlaybackState::luau_type()),
                field("playback_session_public_id", String::luau_type()),
                field("track_public_id", String::luau_type()),
                field("user_public_id", String::luau_type()),
                field("library_public_id", Option::<String>::luau_type()),
                field("position_ms", u64::luau_type()),
                field("duration_ms", Option::<u64>::luau_type()),
                field("played_ms", u64::luau_type()),
                field("updated_at_ms", u64::luau_type()),
            ],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_command_descriptor_includes_durable_handoff_queue() {
        let descriptor = SendCommandRequest::interface_descriptor();
        let playback_id = descriptor
            .fields
            .iter()
            .find(|field| field.name == "playback_id")
            .expect("playback_id must be exposed to Luau");
        let queue_revision = descriptor
            .fields
            .iter()
            .find(|field| field.name == "queue_revision")
            .expect("queue_revision must be exposed to Luau");

        assert_eq!(playback_id.ty, Option::<String>::luau_type());
        assert_eq!(queue_revision.ty, Option::<u64>::luau_type());
    }
}
