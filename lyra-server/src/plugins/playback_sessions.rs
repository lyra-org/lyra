// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

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

use harmony_core::UserDataBinding;

#[cfg(feature = "docgen")]
use crate::plugins::api::WebSocketSenderType;
use crate::plugins::args;
use crate::plugins::executor::ApiWebSocketSender;
use crate::services::playbacks::holds::HeldSession;
use crate::{
    plugins::{
        auth::OwnersQuery,
        db::Db,
        types::{
            LuauPlaybackEvent,
            LuauPlaybackState,
            LuauRemoteAction,
        },
    },
    services::{
        auth::{
            Principal,
            owners,
        },
        playback_sessions as playbacks,
        remote::{
            connections,
            constants::RemoteAction,
            control::{
                self,
                Controller,
                RemoteCommand,
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
    track_id: String,
    play_id: Option<String>,
    client_name: Option<String>,
    event: Option<playbacks::PlaybackEvent>,
    position_ms: Option<u64>,
    duration_ms: Option<u64>,
    state: Option<lyra_db::PlaybackState>,
}

#[derive(Clone, Debug, Deserialize)]
struct SendCommandRequest {
    connection_id: String,
    action: RemoteAction,
    position_ms: Option<u64>,
    level: Option<f32>,
    playback_id: Option<String>,
    queue_revision: Option<u64>,
}

#[derive(Serialize)]
struct ActivePlaybackInfo {
    id: String,
    play_id: String,
    user_id: String,
    username: String,
    track_id: String,
    position_ms: u64,
    duration_ms: Option<u64>,
    state: lyra_db::PlaybackState,
    started_at_ms: u64,
    updated_at_ms: u64,
    client_name: Option<String>,
    plugin_id: Option<String>,
    session_key: Option<String>,
    connection_id: Option<String>,
}

#[derive(Serialize)]
struct PlaybackInfo {
    track_id: String,
    position_ms: u64,
    duration_ms: Option<u64>,
    state: lyra_db::PlaybackState,
}

#[derive(Serialize)]
struct ConnectionInfo {
    id: String,
    user_id: String,
    username: String,
    session_key: String,
    client_name: Option<String>,
    supported_commands: Vec<RemoteAction>,
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
        description = "Reports an observation for a plugin's player context. Returns (public /api/me/playbacks ID, nil) when accepted, (nil, nil) when ignored, or (nil, rejection reason) when admission is refused. Internal failures raise an error. Track changes retain the context ID; stopped contexts expire after five minutes without an accepted current observation. A position past the duration reads as the end. Without a `play_id`, only a `started` observation begins a new play of the track playing; any other observation after its play stopped resumes that play, so it counts at most one listen. A play left playing when another play replaces it, or when its context expires, is credited for the time since its last report, at most what was left of its track; only the listen this records is announced. A replaced play pauses there, so a late report credits it nothing more, and its start resumes it.",
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
        if let Some(play_id) = request.play_id.as_deref()
            && !crate::services::listens::is_play_id(play_id)
        {
            return Ok((None, Some(format!("malformed play_id: {play_id}"))));
        }
        let client_name = crate::services::auth::sessions::tidy_client_name(request.client_name);
        let active_event = playbacks::classify_active_event(request.event)
            .map_err(crate::plugins::runtime_error)?;
        let current_ms = playbacks::now_ms().map_err(crate::plugins::runtime_error)?;
        let mutation = playback_mutation(request.position_ms, request.duration_ms, request.state);

        let mut db = db.write_outside_catalog().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let track_db_id =
            crate::plugins::ids::visible_to(&db, &principal, &request.track_id, "Track")?
                .ok_or_else(|| {
                    crate::plugins::runtime_error(format!("track not found: {}", request.track_id))
                })?;

        let outcome = match crate::services::playbacks::report_reported_playback(
            &mut db,
            crate::services::playbacks::ReportedPlaybackRequest {
                plugin_id: &plugin_id,
                user_db_id,
                session_key: &session_key,
                track_db_id,
                play_id: request.play_id.as_deref(),
                client_name,
                mutation,
                now_ms: current_ms,
                active_event,
            },
        ) {
            Ok(outcome) => outcome,
            Err(crate::services::playbacks::PlaybackError::Session(
                playbacks::PlaybackServiceError::BadRequest(message),
            )) => {
                return Ok((None, Some(message)));
            }
            Err(error) => return Err(crate::plugins::runtime_error(error)),
        };
        drop(db);
        let dispatch_caller = format!("plugin:{plugin_id}");
        let update = match outcome {
            crate::services::playbacks::ReportOutcome::Applied(update) => *update,
            crate::services::playbacks::ReportOutcome::Ignored(evicted_playbacks) => {
                playbacks::dispatch_evicted_updates_for_caller(dispatch_caller, evicted_playbacks);
                return Ok((None, None));
            }
        };
        playbacks::dispatch_evicted_updates_for_caller(
            dispatch_caller.clone(),
            update.evicted_playbacks,
        );
        playbacks::dispatch_playback_update_for_caller(
            dispatch_caller,
            &update.session,
            update.event,
        );
        Ok((Some(update.playback.id), None))
    }

    #[harmony(
        description = "Holds the player session `session_key` the plugin reports playback for, as `report_session` names it, for the dispatch principal while the websocket `sender` stays open. When the websocket closes and no other open websocket holds the session, what the plugin reported for it pauses, as a closed connection of Lyra's own clients pauses its playback: a playing track session pauses at its last reported position; the one it replaced paused when it was replaced. A report accepted for the session after the websocket closed keeps what it reported, and the playback stays for the player to resume. Does nothing once the websocket is closed.",
        args(sender: WebSocketSenderType, session_key: String)
    )]
    fn hold_session(
        vm: &luau::Vm,
        sender: luau::UserData,
        session_key: String,
        #[context] principal: Principal,
        #[context] origin: luau::ChunkOrigin,
    ) -> luau::runtime::Result<()> {
        let plugin_id = origin.plugin.ok_or_else(|| {
            luau::Error::Runtime(
                "playback_sessions.hold_session must be called from plugin Luau code".into(),
            )
        })?;
        let sender = ApiWebSocketSender::class().read_userdata(vm, &sender)?;
        let session_key = require_non_empty_string(session_key, "session_key")?;
        sender.state().hold_session(HeldSession {
            plugin_id: plugin_id.to_string(),
            user_public_id: principal.user_public_id,
            session_key,
        });
        Ok(())
    }

    #[harmony(
        description = "Lists open WebSocket connections of Lyra's own clients, oldest first, with the playback each drives. Without a query, lists the dispatch principal's own; another user's or all users' require the manage_users permission, and an admin's also the admin permission. All users' leaves out the admins' without it. A playback whose current track the principal can't see is left out of its connection. Needs the `lyra.playback_sessions.all` scope too.",
        returns(Vec<ConnectionInfo>)
    )]
    async fn list_connections(
        query: Option<OwnersQuery>,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::Value> {
        let query = query.unwrap_or_default();
        let owners = query.owners("list_connections")?;
        let snapshots = registry::list_connections().await;
        let now_ms = playbacks::now_ms().map_err(crate::plugins::runtime_error)?;
        let db = db.read().await;
        let (viewer_db_id, users) = owned_users(&db, &principal, owners)?;
        let usernames = users
            .iter()
            .map(|user| (user.id.as_str(), user.username.as_str()))
            .collect::<HashMap<_, _>>();
        let connections =
            connections::join_owned(&db, viewer_db_id, &owner_ids(&users), snapshots, now_ms)
                .map_err(crate::plugins::runtime_error)?;
        let mut result = Vec::with_capacity(connections.len());
        for connection in connections {
            let playback = connection
                .current_session(&db)
                .map_err(crate::plugins::runtime_error)?
                .map(|current| PlaybackInfo {
                    track_id: current.track_public_id,
                    position_ms: current.playback.position_ms,
                    duration_ms: current.playback.duration_ms,
                    state: current.playback.state,
                });
            result.push(ConnectionInfo {
                playback_id: connection.playback_id().map(str::to_string),
                playback,
                id: connection.id,
                username: usernames
                    .get(connection.user_id.as_str())
                    .copied()
                    .unwrap_or_default()
                    .to_string(),
                user_id: connection.user_id,
                session_key: connection.session_key,
                client_name: connection.client_name,
                supported_commands: connection.supported_commands,
                remote_control_degraded: connection.remote_control_degraded,
                connected_at_ms: connection.connected_at_ms,
            });
        }
        drop(db);
        harmony_luau::serializable_to_luau_owned(&result)
    }

    #[harmony(
        description = "Lists active playbacks, from Lyra's own clients and every plugin, most recently reported first: those whose current track is playing, paused or buffering and was reported within the last five minutes. Without a query, lists the dispatch principal's own; another user's or all users' require the manage_users permission, and an admin's also the admin permission. All users' leaves out the admins' without it. Playbacks whose current track the principal can't see are left out. Needs the `lyra.playback_sessions.all` scope too.",
        returns(Vec<ActivePlaybackInfo>)
    )]
    async fn list_playbacks(
        query: Option<OwnersQuery>,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::Value> {
        let query = query.unwrap_or_default();
        let owners = query.owners("list_playbacks")?;
        let snapshots = registry::list_connections().await;
        let now_ms = playbacks::now_ms().map_err(crate::plugins::runtime_error)?;
        let db = db.read().await;
        let (viewer_db_id, users) = owned_users(&db, &principal, owners)?;
        let connections =
            connections::join_owned(&db, viewer_db_id, &owner_ids(&users), snapshots, now_ms)
                .map_err(crate::plugins::runtime_error)?;
        let all_users = matches!(owners, owners::Owners::All);
        let active =
            crate::services::playbacks::list_active(&db, &principal, &users, all_users, now_ms)
                .map_err(crate::plugins::runtime_error)?;
        drop(db);
        let result = active
            .into_iter()
            .map(|active| {
                let connection = connections::controller(&connections, &active.playback_id);
                ActivePlaybackInfo {
                    session_key: active
                        .reported_session_key
                        .or_else(|| connection.map(|connection| connection.session_key.clone())),
                    connection_id: connection.map(|connection| connection.id.clone()),
                    id: active.playback_id,
                    play_id: active.session.play_id,
                    user_id: active.user_id,
                    username: active.username,
                    track_id: active.track_id,
                    position_ms: active.session.position_ms,
                    duration_ms: active.session.duration_ms,
                    state: active.session.state,
                    started_at_ms: active.session.created_at_ms,
                    updated_at_ms: active.session.updated_at_ms,
                    client_name: active.session.client_name,
                    plugin_id: active.session.plugin_id,
                }
            })
            .collect::<Vec<_>>();
        harmony_luau::serializable_to_luau_owned(&result)
    }

    #[harmony(description = "Sends a remote control command to a connection.")]
    async fn send_command(
        request: args::Json<SendCommandRequest>,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<()> {
        let request = request.0;
        crate::plugins::auth::require_user_db_id(&principal, &*db.read().await)?;
        let connection_id = require_non_empty_string(request.connection_id, "connection_id")?;
        let command = match request.action {
            RemoteAction::Play => RemoteCommand::Play,
            RemoteAction::Pause => RemoteCommand::Pause,
            RemoteAction::Unpause => RemoteCommand::Unpause,
            RemoteAction::Stop => RemoteCommand::Stop,
            RemoteAction::Seek => RemoteCommand::Seek {
                position_ms: required(request.position_ms, "position_ms", "seek")?,
            },
            RemoteAction::NextTrack => RemoteCommand::NextTrack,
            RemoteAction::PreviousTrack => RemoteCommand::PreviousTrack,
            RemoteAction::SetVolume => RemoteCommand::SetVolume {
                level: required(request.level, "level", "set_volume")?,
            },
            RemoteAction::HandoffQueue => RemoteCommand::HandoffQueue {
                playback_id: required(request.playback_id, "playback_id", "handoff_queue")?,
                queue_revision: required(
                    request.queue_revision,
                    "queue_revision",
                    "handoff_queue",
                )?,
            },
        };
        control::send(
            Controller::User(&principal.user_public_id),
            &connection_id,
            command,
        )
        .await
        .map_err(crate::plugins::runtime_error)
    }
}

fn required<T>(value: Option<T>, field: &str, action: &str) -> luau::runtime::Result<T> {
    value.ok_or_else(|| crate::plugins::runtime_error(format!("{field} required for {action}")))
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

/// The verified principal's database id, and the users `owners` covers whose playback it may read.
fn owned_users(
    db: &agdb::DbAny,
    principal: &Principal,
    owners: owners::Owners<'_>,
) -> luau::runtime::Result<(agdb::DbId, Vec<lyra_db::User>)> {
    let viewer_db_id = crate::plugins::auth::require_user_db_id(principal, db)?;
    let users = owners::resolve(db, principal, viewer_db_id, owners)
        .map_err(crate::plugins::runtime_error)?;
    Ok((viewer_db_id, users))
}

fn owner_ids(users: &[lyra_db::User]) -> Vec<(agdb::DbId, &str)> {
    users
        .iter()
        .filter_map(|user| Some((user.db_id?, user.id.as_str())))
        .collect()
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

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &PlaybackSessionsModule::module_descriptor(),
        &[
            PlaybackUpdateHandler::type_alias_descriptor(),
            LuauPlaybackEvent::type_alias_descriptor(),
            LuauPlaybackState::type_alias_descriptor(),
            LuauRemoteAction::type_alias_descriptor(),
        ],
        &[
            LuauPlaybackUpdatePayload::interface_descriptor(),
            PlaybackSessionReportRequest::interface_descriptor(),
            SendCommandRequest::interface_descriptor(),
            ConnectionInfo::interface_descriptor(),
            PlaybackInfo::interface_descriptor(),
            ActivePlaybackInfo::interface_descriptor(),
            crate::plugins::api::websocket_sender_interface(),
            OwnersQuery::interface_descriptor(),
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
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "play_id",
                ty: Option::<String>::luau_type(),
                description: Some(crate::services::listens::PLAY_ID_DESCRIPTION),
            },
            FieldDescriptor {
                name: "client_name",
                ty: Option::<String>::luau_type(),
                description: Some("Label of the reporting app or device. Control and invisible characters are stripped and it is cut to 128 characters; nothing left reports none. Kept from the report that starts a track session."),
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
                ty: LuauRemoteAction::luau_type(),
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

impl LuauTypeInfo for ActivePlaybackInfo {
    fn luau_type() -> LuauType {
        LuauType::literal("ActivePlaybackInfo")
    }
}

impl DescribeInterface for ActivePlaybackInfo {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new(
            "ActivePlaybackInfo",
            Some("A playback and the track session it plays now."),
        );
        descriptor.fields.extend([
            FieldDescriptor {
                name: "id",
                ty: String::luau_type(),
                description: Some("Public playback id."),
            },
            FieldDescriptor {
                name: "play_id",
                ty: String::luau_type(),
                description: Some("The play of the current track, as `report_session` takes it."),
            },
            FieldDescriptor {
                name: "user_id",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "username",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "track_id",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "position_ms",
                ty: u64::luau_type(),
                description: Some("The position at the last report."),
            },
            FieldDescriptor {
                name: "duration_ms",
                ty: Option::<u64>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "state",
                ty: LuauPlaybackState::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "started_at_ms",
                ty: u64::luau_type(),
                description: Some("When the track session started."),
            },
            FieldDescriptor {
                name: "updated_at_ms",
                ty: u64::luau_type(),
                description: Some("When the track session last changed."),
            },
            FieldDescriptor {
                name: "client_name",
                ty: Option::<String>::luau_type(),
                description: Some("Label of the app or device playing, if it gave one."),
            },
            FieldDescriptor {
                name: "plugin_id",
                ty: Option::<String>::luau_type(),
                description: Some("The plugin that reports the playback; nil for Lyra's own clients."),
            },
            FieldDescriptor {
                name: "session_key",
                ty: Option::<String>::luau_type(),
                description: Some("The reporting plugin's session key, or that of the connection driving the playback; nil for neither."),
            },
            FieldDescriptor {
                name: "connection_id",
                ty: Option::<String>::luau_type(),
                description: Some("The open WebSocket connection of Lyra's own clients driving the playback, as `send_command` takes it; nil when none does."),
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
                name: "track_id",
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
                ty: LuauPlaybackState::luau_type(),
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
            Some("An open WebSocket connection of Lyra's own clients."),
        );
        descriptor.fields.extend([
            FieldDescriptor {
                name: "id",
                ty: String::luau_type(),
                description: Some("Public connection id, as `send_command` takes it."),
            },
            FieldDescriptor {
                name: "user_id",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "username",
                ty: String::luau_type(),
                description: None,
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
                ty: Vec::<LuauRemoteAction>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "playback_id",
                ty: Option::<String>::luau_type(),
                description: Some("The playback the connection drives; nil when it is idle or its current track is hidden from the principal."),
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

/// [`crate::services::playback_sessions::PlaybackUpdatePayload`] as plugins read it, each id
/// named for what it identifies. The payload keeps its own names on the remote-control socket.
#[derive(Serialize)]
pub(crate) struct PlaybackUpdate<'a> {
    event: &'a str,
    state: lyra_db::PlaybackState,
    playback_session_id: &'a str,
    track_id: &'a str,
    user_id: &'a str,
    library_id: Option<&'a str>,
    plugin_id: Option<&'a str>,
    client_name: Option<&'a str>,
    position_ms: u64,
    duration_ms: Option<u64>,
    played_ms: u64,
    updated_at_ms: u64,
}

impl<'a> From<&'a crate::services::playback_sessions::PlaybackUpdatePayload>
    for PlaybackUpdate<'a>
{
    fn from(payload: &'a crate::services::playback_sessions::PlaybackUpdatePayload) -> Self {
        Self {
            event: &payload.event,
            state: payload.state,
            playback_session_id: &payload.playback_session_public_id,
            track_id: &payload.track_public_id,
            user_id: &payload.user_public_id,
            library_id: payload.library_public_id.as_deref(),
            plugin_id: payload.plugin_id.as_deref(),
            client_name: payload.client_name.as_deref(),
            position_ms: payload.position_ms,
            duration_ms: payload.duration_ms,
            played_ms: payload.played_ms,
            updated_at_ms: payload.updated_at_ms,
        }
    }
}

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
                field("playback_session_id", String::luau_type()),
                field("track_id", String::luau_type()),
                field("user_id", String::luau_type()),
                field("library_id", Option::<String>::luau_type()),
                FieldDescriptor {
                    name: "plugin_id",
                    ty: Option::<String>::luau_type(),
                    description: Some(
                        "The plugin that reported the play; nil for Lyra's own clients.",
                    ),
                },
                FieldDescriptor {
                    name: "client_name",
                    ty: Option::<String>::luau_type(),
                    description: Some("Label of the app or device playing, if it gave one."),
                },
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
