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
#[cfg(feature = "docgen")]
use crate::services::playback_sessions::{
    live_window_description,
    reported_context_expiry_description,
};
use crate::services::playbacks::socket_claims::ClaimedSession;
use crate::{
    plugins::{
        auth::OwnersQuery,
        db::Db,
        errors::{
            ErrorKind,
            kinded,
        },
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
                ControlError,
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
    rate: Option<f64>,
    counts_as_listen: Option<bool>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct QueueSaveRequest {
    client_name: String,
    track_ids: Vec<String>,
    current_index: u64,
    position_ms: Option<u64>,
}

/// A queue-backed playback's queue as the caller can play it.
#[derive(Serialize)]
struct LatestQueueInfo {
    playback_id: String,
    track_ids: Vec<String>,
    current_index: u64,
    position_ms: u64,
    client_name: Option<String>,
    plugin_id: Option<String>,
    updated_at_ms: u64,
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
    effective_position_ms: u64,
    duration_ms: Option<u64>,
    state: lyra_db::PlaybackState,
    rate: f64,
    counts_as_listen: bool,
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
    effective_position_ms: u64,
    duration_ms: Option<u64>,
    state: lyra_db::PlaybackState,
    rate: f64,
    counts_as_listen: bool,
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

crate::plugins::limits::module_limits!(&[crate::plugins::limits::Limit {
    name: "MAX_QUEUE_TRACK_IDS",
    value: crate::services::playbacks::QUEUE_ITEM_HARD_CAP,
    description: "The most track ids `save_queue` takes.",
}]);

#[harmony_macros::module(
    id = "lyra/playback_sessions",
    docs_feature = "docgen",
    extend = install_limits,
    describe = describe_limits
)]
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
        description = concat!(
            "Reports an observation for a plugin's player context. Returns (public /api/me/playbacks ID, nil) when accepted, (nil, nil) when ignored, or (nil, rejection reason) when admission is refused. Raises an error for a malformed `play_id`, and a `conflict` error when the report gives its play a `counts_as_listen` other than the one its start declared. Internal failures raise an error. Track changes retain the context ID. ",
            reported_context_expiry_description!(),
            " ",
            live_window_description!(),
            " A position past the duration reads as the end. Without a `play_id`, only a `started` observation begins a new play of the track playing; any other observation after its play stopped resumes that play, so it counts at most one listen. A play left playing is credited for the time from its last report to when another play replaced it, or, when its context expires, to when it stopped being live, where it stops, at most what was left of its track; only the listen this records is announced. A replaced play pauses there, so a late report credits it nothing more, and its start resumes it."
        ),
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
            return Err(crate::plugins::runtime_error(format!(
                "malformed play_id: {play_id}"
            )));
        }
        let client_name = crate::services::auth::sessions::tidy_client_name(request.client_name);
        let active_event = playbacks::classify_active_event(request.event)
            .map_err(crate::plugins::runtime_error)?;
        let current_ms = playbacks::now_ms().map_err(crate::plugins::runtime_error)?;
        let rate_permille = request
            .rate
            .map(playbacks::rate_permille)
            .transpose()
            .map_err(crate::plugins::runtime_error)?;
        let mutation = playbacks::PlaybackMutation {
            position_ms: request.position_ms,
            duration_ms: request.duration_ms,
            state: request.state,
            rate_permille,
            counts_as_listen: request.counts_as_listen,
        };

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
            Err(crate::services::playbacks::PlaybackError::Session(
                error @ playbacks::PlaybackServiceError::CountsAsListenConflict { .. },
            )) => return Err(kinded(ErrorKind::Conflict, error)),
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
        sender.state().claim_session(ClaimedSession {
            plugin_id: plugin_id.to_string(),
            user_public_id: principal.user_public_id,
            session_key,
        });
        Ok(())
    }

    #[harmony(
        description = "Lists open WebSocket connections of Lyra's own clients, oldest first, with the playback each drives. Without a query, lists the dispatch principal's own; another user's or all users' require the manage_users permission and every permission that user holds. All users' leaves out those the principal lacks a permission of. A playback whose current track the principal can't see is left out of its connection. Needs the `lyra.playback_sessions.all` scope too.",
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
                    effective_position_ms: playbacks::current_position_ms(
                        &current.playback,
                        now_ms,
                    ),
                    duration_ms: current.playback.duration_ms,
                    state: current.playback.state,
                    rate: playbacks::rate_multiple(current.playback.rate_permille),
                    counts_as_listen: current.playback.counts_as_listen,
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
        description = concat!(
            "Lists the dispatch principal's live playbacks, from Lyra's own clients and every plugin, most recently reported first. ",
            live_window_description!(),
            " Playbacks whose current track the principal can't see are left out."
        ),
        returns(Vec<ActivePlaybackInfo>)
    )]
    async fn list_playbacks(
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::Value> {
        list_active_playbacks(db, principal, owners::Owners::Principal).await
    }

    #[harmony(
        description = "Lists another user's live playbacks, with `user_id`, or every user's, with `all_users`, as `list_playbacks` lists the dispatch principal's; the query must give one of them. Another user's or all users' require the manage_users permission and every permission that user holds. All users' leaves out those the principal lacks a permission of. Playbacks whose current track the principal can't see are left out. Needs the `lyra.playback_sessions.all` scope too.",
        returns(Vec<ActivePlaybackInfo>)
    )]
    async fn list_user_playbacks(
        query: OwnersQuery,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::Value> {
        let owners = query.named_owners("list_user_playbacks")?;
        list_active_playbacks(db, principal, owners).await
    }

    #[harmony(
        description = "Saves the queue the caller's client `client_name` of this plugin plays, as a playback of the caller's that any client can list and resume, and returns its public /api/me/playbacks id. Each client of the plugin saves to a playback of its own, until another client resumes it; the next save then starts a new one. The saved track stays stopped at `position_ms`, within the track, and counts no listen; resuming it starts a new play. Saving the queue the playback has, at the same `current_index`, only moves the position, so a queue holding a track since hidden from the caller can still be saved again. Any other queue replaces the playback's, keeping its repeat and shuffle, and raises, saving nothing, when a track is missing or hidden from the caller, or past `MAX_QUEUE_TRACK_IDS` track ids.",
        returns(String)
    )]
    async fn save_queue(
        request: args::Json<QueueSaveRequest>,
        #[context] db: Db,
        #[context] principal: Principal,
        #[context] vm: luau::Vm,
        #[context] context: luau::CallContext,
    ) -> luau::runtime::Result<String> {
        let request = request.0;
        let plugin_id =
            crate::plugins::current_plugin(&vm, &context, "playback_sessions.save_queue")?;
        let max = crate::services::playbacks::QUEUE_ITEM_HARD_CAP;
        if request.track_ids.len() > max {
            return Err(crate::plugins::runtime_error(format!(
                "save_queue takes at most {max} track ids, got {}",
                request.track_ids.len()
            )));
        }
        let now_ms = playbacks::now_ms().map_err(crate::plugins::runtime_error)?;
        let mut db = db.write_outside_catalog().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let saved = crate::services::playbacks::save_queue(
            &mut db,
            crate::services::playbacks::SaveQueueRequest {
                principal: &principal,
                user_db_id,
                plugin_id: plugin_id.as_str(),
                client_name: crate::services::auth::sessions::tidy_client_name(Some(
                    request.client_name,
                )),
                track_ids: request.track_ids,
                current_index: request.current_index,
                position_ms: request.position_ms.unwrap_or(0),
                now_ms,
            },
        )
        .map_err(crate::plugins::runtime_error)?;
        // Still under the write guard, so no later queue change can start a handoff this one
        // would fail.
        if let Some(revision) = saved.changed_revision {
            crate::services::remote::handoffs::fail_for_playback_revision(
                &saved.playback_id,
                revision,
            )
            .await;
        }
        drop(db);
        Ok(saved.playback_id)
    }

    #[harmony(
        description = "Deletes the playback the caller's client `client_name` of this plugin saved its queue to with `save_queue`, and returns whether there was one. A playback another client has since resumed is left alone, and the client's next save starts a new one.",
        returns(bool)
    )]
    async fn remove_queue(
        client_name: String,
        #[context] db: Db,
        #[context] principal: Principal,
        #[context] vm: luau::Vm,
        #[context] context: luau::CallContext,
    ) -> luau::runtime::Result<bool> {
        let plugin_id =
            crate::plugins::current_plugin(&vm, &context, "playback_sessions.remove_queue")?;
        let client_name = crate::services::auth::sessions::tidy_client_name(Some(client_name));
        let mut db = db.write_outside_catalog().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let deleted = crate::services::playbacks::remove_saved_queue(
            &mut db,
            user_db_id,
            plugin_id.as_str(),
            client_name.as_deref(),
        )
        .map_err(crate::plugins::runtime_error)?;
        let Some(deleted) = deleted else {
            return Ok(false);
        };
        deleted.finish().await;
        drop(db);
        Ok(true)
    }

    #[harmony(
        description = "The caller's most recently updated playback with a server-managed queue, from any client or plugin, as the caller can play it: its tracks the caller can see, with the current track moved to the next track kept, else the last one before it, from its start, when it is hidden or gone. Playbacks left with no track are skipped; nil when none is left.",
        returns(Option<LatestQueueInfo>)
    )]
    async fn latest_queue(
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::Value> {
        let now_ms = playbacks::now_ms().map_err(crate::plugins::runtime_error)?;
        let db = db.read().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let latest = crate::services::playbacks::latest_playable(&db, user_db_id, now_ms)
            .map_err(crate::plugins::runtime_error)?;
        drop(db);
        let Some(latest) = latest else {
            return Ok(luau::Value::Nil);
        };
        harmony_luau::serializable_to_luau_owned(&LatestQueueInfo {
            playback_id: latest.playback_id,
            track_ids: latest.queue.snapshot.track_ids,
            current_index: latest.queue.snapshot.current_index,
            position_ms: latest.queue.position_ms,
            client_name: latest.client_name,
            plugin_id: latest.plugin_id,
            updated_at_ms: latest.updated_at_ms,
        })
    }

    #[harmony(
        description = "Sends a remote control command to a connection. Raises a `not_found` error when the source or target connection or its user is missing, and a `forbidden` error when the caller may not control the target."
    )]
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
        .map_err(control_error)
    }
}

fn control_error(error: ControlError) -> luau::Error {
    match error {
        ControlError::SourceNotFound
        | ControlError::TargetNotFound
        | ControlError::UserNotFound => kinded(ErrorKind::NotFound, error),
        ControlError::NotAuthorized => kinded(ErrorKind::Forbidden, error),
        error => crate::plugins::runtime_error(error),
    }
}

/// The live playbacks of the users `owners` covers, as the principal may read them.
async fn list_active_playbacks(
    db: Db,
    principal: Principal,
    owners: owners::Owners<'_>,
) -> luau::runtime::Result<luau::Value> {
    let snapshots = registry::list_connections().await;
    let now_ms = playbacks::now_ms().map_err(crate::plugins::runtime_error)?;
    let db = db.read().await;
    let all_users = matches!(owners, owners::Owners::All);
    let (viewer_db_id, users) = owned_users(&db, &principal, owners)?;
    let connections =
        connections::join_owned(&db, viewer_db_id, &owner_ids(&users), snapshots, now_ms)
            .map_err(crate::plugins::runtime_error)?;
    let active =
        crate::services::playbacks::list_active(&db, &principal, &users, all_users, now_ms)
            .map_err(crate::plugins::runtime_error)?;
    drop(db);
    let result = active
        .into_iter()
        .map(|active| {
            let connection = connections::controller(&connections, &active.playback_id);
            let effective_position_ms = playbacks::current_position_ms(&active.session, now_ms);
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
                effective_position_ms,
                duration_ms: active.session.duration_ms,
                state: active.session.state,
                rate: playbacks::rate_multiple(active.session.rate_permille),
                counts_as_listen: active.session.counts_as_listen,
                started_at_ms: active.session.created_at_ms,
                updated_at_ms: active.session.updated_at_ms,
                client_name: active.session.client_name,
                plugin_id: active.session.plugin_id,
            }
        })
        .collect::<Vec<_>>();
    harmony_luau::serializable_to_luau_owned(&result)
}

fn required<T>(value: Option<T>, field: &str, action: &str) -> luau::runtime::Result<T> {
    value.ok_or_else(|| crate::plugins::runtime_error(format!("{field} required for {action}")))
}

/// The verified principal's database id, and the users `owners` covers whose playback it may read.
fn owned_users(
    db: &agdb::DbAny,
    principal: &Principal,
    owners: owners::Owners<'_>,
) -> luau::runtime::Result<(agdb::DbId, Vec<lyra_db::User>)> {
    let viewer_db_id = crate::plugins::auth::require_user_db_id(principal, db)?;
    let current = principal
        .current(db)
        .map_err(crate::plugins::errors::auth_error)?;
    let users = owners::resolve(db, &current, viewer_db_id, owners)
        .map_err(crate::plugins::errors::auth_error)?;
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
            QueueSaveRequest::interface_descriptor(),
            LatestQueueInfo::interface_descriptor(),
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

impl LuauTypeInfo for QueueSaveRequest {
    fn luau_type() -> LuauType {
        LuauType::literal("QueueSaveRequest")
    }
}

impl DescribeInterface for QueueSaveRequest {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("QueueSaveRequest", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "client_name",
                ty: String::luau_type(),
                description: Some("The client of the plugin saving, such as a Subsonic client's `c`. Control and invisible characters are stripped and it is cut to 128 characters; nothing left names the plugin's client without a name."),
            },
            FieldDescriptor {
                name: "track_ids",
                ty: Vec::<String>::luau_type(),
                description: Some("The queue, in order and with repeats."),
            },
            FieldDescriptor {
                name: "current_index",
                ty: u64::luau_type(),
                description: Some("0-based index of the current track in `track_ids`."),
            },
            FieldDescriptor {
                name: "position_ms",
                ty: Option::<u64>::luau_type(),
                description: Some("Position in the current track; defaults to 0."),
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for LatestQueueInfo {
    fn luau_type() -> LuauType {
        LuauType::literal("LatestQueueInfo")
    }
}

impl DescribeInterface for LatestQueueInfo {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new(
            "LatestQueueInfo",
            Some("A playback's queue as the caller can play it."),
        );
        descriptor.fields.extend([
            FieldDescriptor {
                name: "playback_id",
                ty: String::luau_type(),
                description: Some("Public playback id."),
            },
            FieldDescriptor {
                name: "track_ids",
                ty: Vec::<String>::luau_type(),
                description: Some("The queue's tracks the caller can see, in order and with repeats."),
            },
            FieldDescriptor {
                name: "current_index",
                ty: u64::luau_type(),
                description: Some("0-based index of the current track in `track_ids`."),
            },
            FieldDescriptor {
                name: "position_ms",
                ty: u64::luau_type(),
                description: Some("Where to resume the current track: its play's position now, or 0 when the current track moved or completed."),
            },
            FieldDescriptor {
                name: "client_name",
                ty: Option::<String>::luau_type(),
                description: Some("The client of the current track's play, if it gave one."),
            },
            FieldDescriptor {
                name: "plugin_id",
                ty: Option::<String>::luau_type(),
                description: Some("The plugin that saved or reported the current track's play; nil for Lyra's own clients."),
            },
            FieldDescriptor {
                name: "updated_at_ms",
                ty: u64::luau_type(),
                description: Some("When the playback last changed."),
            },
        ]);
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
                name: "effective_position_ms",
                ty: u64::luau_type(),
                description: Some("The position now: `position_ms` advanced while playing, at its rate, by the time since it was reported, within the duration."),
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
                name: "rate",
                ty: f64::luau_type(),
                description: Some("Playback speed as a multiple of normal speed."),
            },
            FieldDescriptor {
                name: "counts_as_listen",
                ty: bool::luau_type(),
                description: Some("Whether the play may count as a listen, as the report that started it declared."),
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
                description: Some("The position at the last report."),
            },
            FieldDescriptor {
                name: "effective_position_ms",
                ty: u64::luau_type(),
                description: Some("The position now: `position_ms` advanced while playing, at its rate, by the time since it was reported, within the duration."),
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
                name: "rate",
                ty: f64::luau_type(),
                description: Some("Playback speed as a multiple of normal speed."),
            },
            FieldDescriptor {
                name: "counts_as_listen",
                ty: bool::luau_type(),
                description: Some("Whether the play may count as a listen, as the report that started it declared."),
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
        FieldDescriptor {
            name: "rate",
            ty: Option::<f64>::luau_type(),
            description: Some(playbacks::RATE_DESCRIPTION),
        },
        FieldDescriptor {
            name: "counts_as_listen",
            ty: Option::<bool>::luau_type(),
            description: Some(playbacks::COUNTS_AS_LISTEN_DESCRIPTION),
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
    rate: f64,
    counts_as_listen: bool,
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
            rate: payload.rate,
            counts_as_listen: payload.counts_as_listen,
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
                FieldDescriptor {
                    name: "rate",
                    ty: f64::luau_type(),
                    description: Some("Playback speed as a multiple of normal speed."),
                },
                FieldDescriptor {
                    name: "counts_as_listen",
                    ty: bool::luau_type(),
                    description: Some(
                        "Whether the play may count as a listen, as the report that started it declared.",
                    ),
                },
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
