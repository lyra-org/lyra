// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    cell::RefCell,
    sync::Arc,
};

use agdb::DbId;
use harmony_core::{
    CallContext,
    CapabilityId,
    ChunkOrigin,
    FunctionSpec,
    ModuleExport,
    ModuleId,
    ModuleSpec,
};
use harmony_luau as luau;
use harmony_luau::{
    DescribeInterface,
    DescribeTypeAlias,
    FieldDescriptor,
    FunctionParameter,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
    TypeAliasDescriptor,
};
#[cfg(feature = "docgen")]
use harmony_luau::{
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    render_definition_file_with_support,
};
use serde::{
    Deserialize,
    Serialize,
    de::DeserializeOwned,
};

#[cfg(feature = "docgen")]
use crate::services::playback_sessions::PlaybackUpdatePayload;
use crate::{
    plugins::db::{
        self,
        DbAsync,
    },
    plugins::lifecycle::PluginId,
    services::{
        playback_sessions::{
            self as playbacks,
            PlaybackScopeKey,
        },
        remote::{
            constants::RemoteAction,
            messages::{
                ForwardedCommand,
                ForwardedCommandData,
                OutgoingMessage,
            },
            registry,
        },
    },
};

#[derive(Clone, Default)]
pub(crate) struct PlaybackSessionsModuleStore {
    db: Option<DbAsync>,
}

impl PlaybackSessionsModuleStore {
    pub(crate) fn empty() -> Self {
        Self { db: None }
    }

    pub(crate) fn with_db(db: DbAsync) -> Self {
        Self { db: Some(db) }
    }

    fn db(&self) -> luau::runtime::Result<DbAsync> {
        self.db.clone().ok_or_else(|| {
            crate::plugins::runtime_error(
                "lyra/playback_sessions requires a database-backed plugin executor",
            )
        })
    }
}

#[derive(Clone)]
pub(crate) struct RegisteredPlaybackUpdateCallback {
    pub(crate) plugin_id: Arc<str>,
    pub(crate) context: CallContext,
    pub(crate) function: luau::Function,
}

#[derive(Default)]
pub(crate) struct PlaybackUpdateCallbackStore {
    handlers: RefCell<Vec<RegisteredPlaybackUpdateCallback>>,
}

impl PlaybackUpdateCallbackStore {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    fn add(&self, handler: RegisteredPlaybackUpdateCallback) {
        self.handlers.borrow_mut().push(handler);
    }

    pub(crate) fn snapshot(&self) -> Vec<RegisteredPlaybackUpdateCallback> {
        self.handlers.borrow().clone()
    }
}

struct PlaybackSessionsModule;
struct PlaybackUpdateHandler;

#[derive(Clone, Debug, Deserialize)]
struct PlaybackStartRequest {
    track_id: i64,
    user_id: i64,
    position_ms: Option<u64>,
    duration_ms: Option<u64>,
    state: Option<db::PlaybackState>,
}

#[derive(Clone, Debug, Deserialize)]
struct PlaybackReportRequest {
    playback_session_id: i64,
    position_ms: Option<u64>,
    duration_ms: Option<u64>,
    state: Option<db::PlaybackState>,
}

#[derive(Clone, Debug, Deserialize)]
struct PlaybackSessionReportRequest {
    plugin_id: String,
    user_id: i64,
    session_key: String,
    track_id: i64,
    event: Option<playbacks::PlaybackEvent>,
    position_ms: Option<u64>,
    duration_ms: Option<u64>,
    state: Option<db::PlaybackState>,
}

#[derive(Clone, Debug, Deserialize)]
struct PlaybackSessionClearRequest {
    plugin_id: String,
    user_id: i64,
    session_key: String,
}

#[derive(Clone, Debug, Deserialize)]
struct SendCommandRequest {
    user_id: i64,
    target_token: String,
    action: String,
    position_ms: Option<u64>,
    level: Option<f32>,
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
    token: String,
    session_key: String,
    supported_commands: Vec<String>,
    playback: Option<PlaybackInfo>,
    degraded: bool,
}

impl LuauTypeInfo for PlaybackUpdateHandler {
    fn luau_type() -> LuauType {
        LuauType::literal("PlaybackUpdateHandler")
    }
}

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

impl DescribeTypeAlias for PlaybackUpdateHandler {
    fn type_alias_descriptor() -> TypeAliasDescriptor {
        TypeAliasDescriptor::new(
            "PlaybackUpdateHandler",
            Self::callback_type(),
            Some("Playback update callback."),
        )
    }
}

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/playback_sessions")
        .capability("lyra.playback_sessions")
        .function(on_update_spec())
        .function(report_spec())
        .function(start_spec())
        .function(report_session_spec())
        .function(clear_session_spec())
        .function(list_connections_spec())
        .function(send_command_spec())
        .install(|_| Ok(ModuleExport::new(PlaybackSessionsModule)))
}

fn on_update_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("on_update")
        .arg_name("handler")
        .args::<PlaybackUpdateHandler>()
        .call(on_update_callback)
}

fn report_spec() -> FunctionSpec {
    FunctionSpec::async_fn("report")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("request")
        .args::<PlaybackReportRequest>()
        .call_async(Arc::new(report_callback))
}

fn start_spec() -> FunctionSpec {
    FunctionSpec::async_fn("start")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("request")
        .args::<PlaybackStartRequest>()
        .returns::<i64>()
        .call_async(Arc::new(start_callback))
}

fn report_session_spec() -> FunctionSpec {
    FunctionSpec::async_fn("report_session")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("request")
        .args::<PlaybackSessionReportRequest>()
        .returns::<Option<i64>>()
        .call_async(Arc::new(report_session_callback))
}

fn clear_session_spec() -> FunctionSpec {
    FunctionSpec::async_fn("clear_session")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("request")
        .args::<PlaybackSessionClearRequest>()
        .call_async(Arc::new(clear_session_callback))
}

fn list_connections_spec() -> FunctionSpec {
    FunctionSpec::async_fn("list_connections")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("user_id")
        .args::<i64>()
        .returns::<Vec<ConnectionInfo>>()
        .call_async(Arc::new(list_connections_callback))
}

fn send_command_spec() -> FunctionSpec {
    FunctionSpec::async_fn("send_command")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("request")
        .args::<SendCommandRequest>()
        .call_async(Arc::new(send_command_callback))
}

fn on_update_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let function: luau::Function = frame.args.read_named("handler")?;
    let plugin_id = frame.context.origin.plugin.clone().ok_or_else(|| {
        luau::Error::Runtime(
            "playback_sessions.on_update must be called from plugin Luau code".into(),
        )
    })?;
    let validated_plugin_id =
        PluginId::new(plugin_id.to_string()).map_err(crate::plugins::runtime_error)?;
    futures::executor::block_on(async {
        let generation = crate::STATE.generation();
        let _registration = generation
            .plugin_registries
            .ensure_registrations_open(&validated_plugin_id)
            .await
            .map_err(crate::plugins::runtime_error)?;
        Ok::<(), luau::Error>(())
    })?;

    let callbacks = frame.vm.data().get::<PlaybackUpdateCallbackStore>()?;
    callbacks.add(RegisteredPlaybackUpdateCallback {
        plugin_id,
        context: call_context_to_core(&frame.context),
        function,
    });
    Ok(())
}

fn report_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let request_value: luau::Value = frame.args.read_named("request")?;
    let request: PlaybackReportRequest = from_luau_json(frame.vm, &request_value)?;
    let store = frame
        .vm
        .data()
        .get::<PlaybackSessionsModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;
    let plugin_id = frame.context.origin.plugin.clone().ok_or_else(|| {
        luau::Error::Runtime("playback_sessions.report must be called from plugin Luau code".into())
    })?;

    Ok(luau::ScheduledFuture::new(async move {
        let playback_session_id =
            require_positive_id(request.playback_session_id, "playback_session_id")?;
        let mutation = playback_mutation(request.position_ms, request.duration_ms, request.state);
        let current_ms = playbacks::now_ms().map_err(crate::plugins::runtime_error)?;

        let mut db = db.write().await;
        let track_db_id = db::playback_sessions::get_track_id(&db, playback_session_id)
            .map_err(crate::plugins::runtime_error)?
            .ok_or_else(|| crate::plugins::runtime_error("playback session not found"))?;
        if !crate::services::auth::access::entity_accessible(&db, &principal, track_db_id)
            .map_err(crate::plugins::runtime_error)?
        {
            return Err(crate::plugins::runtime_error("playback session not found"));
        }
        let update = playbacks::report_playback_with_cleanup(
            &mut db,
            playbacks::ReportPlaybackRequest {
                playback_session_id,
                user_db_id: Some(principal.user_db_id),
                mutation,
                now_ms: current_ms,
                activity_policy: playbacks::ActivityPolicy::PlayingOnly,
                active_event: playbacks::ActiveEvent::Progress,
            },
        )
        .map_err(crate::plugins::runtime_error)?;
        let dispatch_caller = format!("plugin:{plugin_id}");
        playbacks::dispatch_evicted_updates_for_caller(
            dispatch_caller.clone(),
            update.evicted_playbacks,
        );

        drop(db);
        playbacks::dispatch_playback_update_for_caller(
            dispatch_caller,
            &update.playback,
            update.event,
        );
        Ok(())
    }))
}

fn start_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let request_value: luau::Value = frame.args.read_named("request")?;
    let request: PlaybackStartRequest = from_luau_json(frame.vm, &request_value)?;
    let store = frame
        .vm
        .data()
        .get::<PlaybackSessionsModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;
    let plugin_id = frame.context.origin.plugin.clone().ok_or_else(|| {
        luau::Error::Runtime("playback_sessions.start must be called from plugin Luau code".into())
    })?;

    Ok(luau::ScheduledFuture::new(async move {
        let track_db_id = require_positive_id(request.track_id, "track_id")?;
        let user_db_id = require_positive_id(request.user_id, "user_id")?;
        if user_db_id != principal.user_db_id {
            return Err(crate::plugins::runtime_error("user not found"));
        }
        let mutation = playback_mutation(request.position_ms, request.duration_ms, request.state);
        let current_ms = playbacks::now_ms().map_err(crate::plugins::runtime_error)?;

        let mut db = db.write().await;
        if !crate::services::auth::access::entity_accessible(&db, &principal, track_db_id)
            .map_err(crate::plugins::runtime_error)?
        {
            return Err(crate::plugins::runtime_error("track not found"));
        }
        let update = playbacks::start_playback_with_cleanup(
            &mut db,
            playbacks::StartPlaybackRequest {
                track_db_id,
                user_db_id,
                mutation,
                now_ms: current_ms,
                active_event: playbacks::ActiveEvent::Started,
            },
        )
        .map_err(crate::plugins::runtime_error)?;
        let dispatch_caller = format!("plugin:{plugin_id}");
        playbacks::dispatch_evicted_updates_for_caller(
            dispatch_caller.clone(),
            update.evicted_playbacks,
        );

        let playback_session_id = update.playback.playback_session_id.0;
        drop(db);
        playbacks::dispatch_playback_update_for_caller(
            dispatch_caller,
            &update.playback,
            update.event,
        );
        Ok(luau::Value::Integer(playback_session_id))
    }))
}

fn report_session_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let request_value: luau::Value = frame.args.read_named("request")?;
    let request: PlaybackSessionReportRequest = from_luau_json(frame.vm, &request_value)?;
    let store = frame
        .vm
        .data()
        .get::<PlaybackSessionsModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;
    let plugin_id = frame.context.origin.plugin.clone().ok_or_else(|| {
        luau::Error::Runtime(
            "playback_sessions.report_session must be called from plugin Luau code".into(),
        )
    })?;

    Ok(luau::ScheduledFuture::new(async move {
        let _ = require_non_empty_string(request.plugin_id, "plugin_id")?;
        let user_db_id = require_positive_id(request.user_id, "user_id")?;
        if user_db_id != principal.user_db_id {
            return Err(crate::plugins::runtime_error("user not found"));
        }
        let session_key = require_non_empty_string(request.session_key, "session_key")?;
        let track_db_id = require_positive_id(request.track_id, "track_id")?;
        let active_event = playbacks::classify_active_event(request.event)
            .map_err(crate::plugins::runtime_error)?;
        let current_ms = playbacks::now_ms().map_err(crate::plugins::runtime_error)?;
        let mutation = playback_mutation(request.position_ms, request.duration_ms, request.state);

        let mut db = db.write().await;
        if !crate::services::auth::access::entity_accessible(&db, &principal, track_db_id)
            .map_err(crate::plugins::runtime_error)?
        {
            return Err(crate::plugins::runtime_error("track not found"));
        }

        let update = playbacks::report_playback_session_with_cleanup(
            &mut db,
            playbacks::SessionPlaybackReportRequest {
                plugin_id: &plugin_id,
                user_db_id,
                session_key: &session_key,
                track_db_id,
                mutation,
                now_ms: current_ms,
                active_event,
                stale_ttl_ms: playbacks::ACTIVE_SESSION_TTL_MS,
            },
        )
        .map_err(crate::plugins::runtime_error)?;
        let dispatch_caller = format!("plugin:{plugin_id}");
        playbacks::dispatch_evicted_updates_for_caller(
            dispatch_caller.clone(),
            update.evicted_playbacks,
        );

        let playbacks::OptionalPlaybackUpdateResult {
            playback, event, ..
        } = update;
        let Some(playback) = playback else {
            return Ok(luau::Value::Nil);
        };

        let playback_session_id = playback.playback_session_id.0;
        let event_label = event
            .map(|value| value.to_string())
            .unwrap_or_else(|| active_event.to_string());
        drop(db);
        playbacks::dispatch_playback_update_for_caller(dispatch_caller, &playback, event_label);
        Ok(luau::Value::Integer(playback_session_id))
    }))
}

fn clear_session_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let request_value: luau::Value = frame.args.read_named("request")?;
    let request: PlaybackSessionClearRequest = from_luau_json(frame.vm, &request_value)?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;
    let plugin_id = frame.context.origin.plugin.clone().ok_or_else(|| {
        luau::Error::Runtime(
            "playback_sessions.clear_session must be called from plugin Luau code".into(),
        )
    })?;

    Ok(luau::ScheduledFuture::new(async move {
        let _ = require_non_empty_string(request.plugin_id, "plugin_id")?;
        let user_db_id = require_positive_id(request.user_id, "user_id")?;
        if user_db_id != principal.user_db_id {
            return Ok(());
        }
        let session_key = require_non_empty_string(request.session_key, "session_key")?;
        let scope = PlaybackScopeKey {
            plugin_id: &plugin_id,
            user_db_id,
            session_key: &session_key,
        };
        playbacks::clear_playback_session_scope(&scope);
        Ok(())
    }))
}

fn list_connections_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let user_id: i64 = frame.args.read_named("user_id")?;
    let store = frame
        .vm
        .data()
        .get::<PlaybackSessionsModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let user_db_id = require_positive_id(user_id, "user_id")?;
        if user_db_id != principal.user_db_id {
            return Ok(empty_array_value());
        }

        let connections = registry::list_connections().await;
        let now_ms = playbacks::now_ms().map_err(crate::plugins::runtime_error)?;

        let (user_public_id, playbacks_list) = {
            let db = db.read().await;
            let user_public_id = resolve_user_public_id(&db, user_db_id)?;
            let playbacks_list = playbacks::list_playbacks(&db, user_db_id)
                .map_err(crate::plugins::runtime_error)?;
            (user_public_id, playbacks_list)
        };

        let mut result = Vec::new();
        for conn in &connections {
            if conn.user_public_id != user_public_id {
                continue;
            }

            let scope_key = PlaybackScopeKey {
                plugin_id: "native",
                user_db_id: conn.user_db_id,
                session_key: &conn.session_key,
            };

            let playback =
                playbacks::get_playback_session(&scope_key).and_then(|scope| {
                    let session_id = scope.current_playback_session_id?;
                    let record = playbacks_list
                        .iter()
                        .find(|playback| playback.playback_session_id == session_id)?;
                    if record.library_public_id.as_ref().is_some_and(|library_id| {
                        principal.accessible_library_ids.contains(library_id)
                    }) {
                        Some(PlaybackInfo {
                            track_public_id: record.track_public_id.clone(),
                            position_ms: record.playback.position_ms,
                            duration_ms: record.playback.duration_ms,
                            state: playback_state_string(record.playback.state),
                        })
                    } else {
                        None
                    }
                });

            let degraded = playbacks::is_remote_control_degraded(&scope_key, now_ms);
            let supported_commands = conn
                .supported_commands
                .iter()
                .map(remote_action_string)
                .collect();

            result.push(ConnectionInfo {
                token: conn.token.clone(),
                session_key: conn.session_key.clone(),
                supported_commands,
                playback,
                degraded,
            });
        }

        let value = harmony_luau::serializable_to_luau_owned(&result)?;
        Ok(value)
    }))
}

fn send_command_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let request_value: luau::Value = frame.args.read_named("request")?;
    let request: SendCommandRequest = from_luau_json(frame.vm, &request_value)?;
    let store = frame
        .vm
        .data()
        .get::<PlaybackSessionsModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let user_db_id = require_positive_id(request.user_id, "user_id")?;
        if user_db_id != principal.user_db_id {
            return Err(crate::plugins::runtime_error(
                "not authorized to control target",
            ));
        }
        let target_token = require_non_empty_string(request.target_token, "target_token")?;
        let action_str = require_non_empty_string(request.action, "action")?;
        let action: RemoteAction = serde_json::from_value(serde_json::Value::String(
            action_str.clone(),
        ))
        .map_err(|_| crate::plugins::runtime_error(format!("unknown action: {action_str}")))?;

        let target = registry::resolve_token(&target_token)
            .await
            .ok_or_else(|| crate::plugins::runtime_error("connection not found"))?;

        let request_user_public_id = {
            let db = db.read().await;
            resolve_user_public_id(&db, user_db_id)?
        };
        if request_user_public_id != target.user_public_id {
            return Err(crate::plugins::runtime_error(
                "not authorized to control target",
            ));
        }
        if !target.supported_commands.contains(&action) {
            return Err(crate::plugins::runtime_error(format!(
                "target does not support command: {action_str}"
            )));
        }

        let data = match action {
            RemoteAction::Seek => {
                let position_ms = request.position_ms.ok_or_else(|| {
                    crate::plugins::runtime_error("position_ms required for seek")
                })?;
                ForwardedCommandData::Seek { position_ms }
            }
            RemoteAction::SetVolume => {
                let level = request.level.ok_or_else(|| {
                    crate::plugins::runtime_error("level required for set_volume")
                })?;
                ForwardedCommandData::Volume {
                    level: level.clamp(0.0, 1.0),
                }
            }
            _ => ForwardedCommandData::Simple,
        };

        let forwarded = OutgoingMessage::Command(ForwardedCommand {
            action,
            from: None,
            data,
        });
        registry::send_to_connection(target.connection_id, forwarded)
            .await
            .map_err(|error| {
                crate::plugins::runtime_error(format!("command delivery failed: {error}"))
            })?;

        if let Ok(now_ms) = playbacks::now_ms() {
            let scope_key = PlaybackScopeKey {
                plugin_id: "native",
                user_db_id: target.user_db_id,
                session_key: &target.session_key,
            };
            playbacks::mark_command_dispatched(&scope_key, now_ms);
        }

        Ok(())
    }))
}

fn call_context_to_core(context: &luau::CallContext) -> CallContext {
    let mut caller = harmony_core::ContextBag::default();
    for (type_id, value) in context.caller.cloned_entries() {
        caller.insert_shared(type_id, value);
    }

    CallContext {
        origin: ChunkOrigin {
            module: context
                .origin
                .module
                .as_ref()
                .map(|module| ModuleId(module.0.clone())),
            plugin: context.origin.plugin.clone(),
            path: context.origin.path.clone(),
        },
        capability: context
            .capability
            .as_ref()
            .map(|capability| CapabilityId(capability.0.clone())),
        caller,
        task_group: harmony_core::TaskGroupId(context.task_group.0),
    }
}

fn from_luau_json<T>(vm: &luau::Vm, value: &luau::Value) -> luau::runtime::Result<T>
where
    T: DeserializeOwned,
{
    serde_json::from_value(harmony_serde::luau_to_json(vm, value, 0)?)
        .map_err(crate::plugins::runtime_error)
}

fn playback_mutation(
    position_ms: Option<u64>,
    duration_ms: Option<u64>,
    state: Option<db::PlaybackState>,
) -> playbacks::PlaybackMutation {
    playbacks::PlaybackMutation {
        position_ms,
        duration_ms,
        state,
    }
}

fn require_positive_id(value: i64, field_name: &str) -> luau::runtime::Result<DbId> {
    if value <= 0 {
        return Err(crate::plugins::runtime_error(format!(
            "{field_name} must be a positive id"
        )));
    }
    Ok(DbId(value))
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

fn resolve_user_public_id(
    db: &impl db::DbAccess,
    user_db_id: DbId,
) -> luau::runtime::Result<String> {
    let user = db::users::get_by_id(db, user_db_id).map_err(crate::plugins::runtime_error)?;
    user.map(|user| user.id)
        .ok_or_else(|| crate::plugins::runtime_error("not authorized to control target"))
}

fn remote_action_string(action: &RemoteAction) -> String {
    serde_json::to_value(action)
        .ok()
        .and_then(|value| value.as_str().map(String::from))
        .unwrap_or_default()
}

fn playback_state_string(state: db::PlaybackState) -> String {
    serde_json::to_value(state)
        .ok()
        .and_then(|value| value.as_str().map(String::from))
        .unwrap_or_default()
}

fn empty_array_value() -> luau::Value {
    luau::Value::TableData(luau::OwnedTable::with_capacity(0, 0))
}

#[cfg(feature = "docgen")]
fn param(name: &'static str, ty: LuauType) -> ParameterDescriptor {
    ParameterDescriptor {
        name,
        ty,
        description: None,
        variadic: false,
    }
}

#[cfg(feature = "docgen")]
fn module_descriptor() -> ModuleDescriptor {
    ModuleDescriptor {
        name: "PlaybackSessions",
        local_name: "playback_sessions",
        description: None,
        fields: Vec::new(),
        functions: vec![
            ModuleFunctionDescriptor {
                path: vec!["on_update"],
                description: Some("Registers a callback for playback updates."),
                params: vec![param("handler", PlaybackUpdateHandler::luau_type())],
                returns: vec![],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["report"],
                description: Some("Reports progress for an existing playback session."),
                params: vec![param("request", PlaybackReportRequest::luau_type())],
                returns: vec![],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["start"],
                description: Some("Starts a playback session and returns its id."),
                params: vec![param("request", PlaybackStartRequest::luau_type())],
                returns: vec![i64::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["report_session"],
                description: Some(
                    "Reports plugin-scoped playback session progress and returns the session id when the playback remains active.",
                ),
                params: vec![param("request", PlaybackSessionReportRequest::luau_type())],
                returns: vec![Option::<i64>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["clear_session"],
                description: Some("Clears a plugin-scoped playback session."),
                params: vec![param("request", PlaybackSessionClearRequest::luau_type())],
                returns: vec![],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["list_connections"],
                description: Some(
                    "Lists active connections for the given user with their playback state.",
                ),
                params: vec![param("user_id", i64::luau_type())],
                returns: vec![Vec::<ConnectionInfo>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["send_command"],
                description: Some("Sends a remote control command to a connection."),
                params: vec![param("request", SendCommandRequest::luau_type())],
                returns: vec![],
                yields: true,
            },
        ],
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &module_descriptor(),
        &[
            PlaybackUpdateHandler::type_alias_descriptor(),
            playbacks::PlaybackEvent::type_alias_descriptor(),
        ],
        &[
            PlaybackUpdatePayload::interface_descriptor(),
            PlaybackStartRequest::interface_descriptor(),
            PlaybackReportRequest::interface_descriptor(),
            PlaybackSessionReportRequest::interface_descriptor(),
            PlaybackSessionClearRequest::interface_descriptor(),
            SendCommandRequest::interface_descriptor(),
            ConnectionInfo::interface_descriptor(),
            PlaybackInfo::interface_descriptor(),
        ],
        &[],
    )
}

impl LuauTypeInfo for PlaybackStartRequest {
    fn luau_type() -> LuauType {
        LuauType::literal("PlaybackStartRequest")
    }
}

impl DescribeInterface for PlaybackStartRequest {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("PlaybackStartRequest", None);
        descriptor.fields.extend(playback_request_fields([
            FieldDescriptor {
                name: "track_id",
                ty: i64::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "user_id",
                ty: i64::luau_type(),
                description: None,
            },
        ]));
        descriptor
    }
}

impl LuauTypeInfo for PlaybackReportRequest {
    fn luau_type() -> LuauType {
        LuauType::literal("PlaybackReportRequest")
    }
}

impl DescribeInterface for PlaybackReportRequest {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("PlaybackReportRequest", None);
        descriptor
            .fields
            .extend(playback_request_fields([FieldDescriptor {
                name: "playback_session_id",
                ty: i64::luau_type(),
                description: None,
            }]));
        descriptor
    }
}

impl LuauTypeInfo for PlaybackSessionReportRequest {
    fn luau_type() -> LuauType {
        LuauType::literal("PlaybackSessionReportRequest")
    }
}

impl DescribeInterface for PlaybackSessionReportRequest {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("PlaybackSessionReportRequest", None);
        descriptor.fields.extend(playback_request_fields([
            FieldDescriptor {
                name: "plugin_id",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "user_id",
                ty: i64::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "session_key",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "track_id",
                ty: i64::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "event",
                ty: Option::<playbacks::PlaybackEvent>::luau_type(),
                description: None,
            },
        ]));
        descriptor
    }
}

impl LuauTypeInfo for PlaybackSessionClearRequest {
    fn luau_type() -> LuauType {
        LuauType::literal("PlaybackSessionClearRequest")
    }
}

impl DescribeInterface for PlaybackSessionClearRequest {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("PlaybackSessionClearRequest", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "plugin_id",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "user_id",
                ty: i64::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "session_key",
                ty: String::luau_type(),
                description: None,
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
                name: "user_id",
                ty: i64::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "target_token",
                ty: String::luau_type(),
                description: None,
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
        let mut descriptor = InterfaceDescriptor::new("ConnectionInfo", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "token",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "session_key",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "supported_commands",
                ty: Vec::<String>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "playback",
                ty: Option::<PlaybackInfo>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "degraded",
                ty: bool::luau_type(),
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
            ty: Option::<db::PlaybackState>::luau_type(),
            description: None,
        },
    ]);
    fields
}
