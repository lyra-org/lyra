// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::Arc;

use harmony_core::LuaAsyncExt;
use harmony_luau::{
    DescribeTypeAlias,
    FunctionParameter,
    LuauType,
    LuauTypeInfo,
    TypeAliasDescriptor,
};
use mlua::{
    Function,
    Lua,
    Result,
    Table,
};
use serde::Deserialize;

use crate::{
    STATE,
    plugins::db::PlaybackState,
    plugins::{
        caller::request_caller,
        from_lua_json_value,
        lifecycle::{
            PluginFunctionHandle,
            PluginId,
        },
    },
    services::playback_sessions::{
        self as playbacks,
        ActiveEvent,
        PLAYBACK_CALLBACK_REGISTRY,
        PlaybackEvent,
        PlaybackServiceError,
        PlaybackUpdatePayload,
        dispatch_evicted_updates_for_caller,
        dispatch_playback_update_for_caller,
    },
};

mod remote;
mod sessions;

#[harmony_macros::interface]
#[derive(Clone, Debug, Deserialize)]
struct PlaybackStartRequest {
    track_id: i64,
    user_id: i64,
    position_ms: Option<u64>,
    duration_ms: Option<u64>,
    state: Option<PlaybackState>,
}

#[harmony_macros::interface]
#[derive(Clone, Debug, Deserialize)]
struct PlaybackReportRequest {
    playback_session_id: i64,
    position_ms: Option<u64>,
    duration_ms: Option<u64>,
    state: Option<PlaybackState>,
}

#[harmony_macros::interface]
#[derive(Clone, Debug, Deserialize)]
struct PlaybackSessionReportRequest {
    plugin_id: String,
    user_id: i64,
    session_key: String,
    track_id: i64,
    event: Option<PlaybackEvent>,
    position_ms: Option<u64>,
    duration_ms: Option<u64>,
    state: Option<PlaybackState>,
}

#[harmony_macros::interface]
#[derive(Clone, Debug, Deserialize)]
struct PlaybackSessionClearRequest {
    plugin_id: String,
    user_id: i64,
    session_key: String,
}

struct PlaybackUpdateHandler;

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

use crate::plugins::require_positive_id;

fn playback_mutation(
    position_ms: Option<u64>,
    duration_ms: Option<u64>,
    state: Option<PlaybackState>,
) -> playbacks::PlaybackMutation {
    playbacks::PlaybackMutation {
        position_ms,
        duration_ms,
        state,
    }
}

fn playback_service_error_to_lua(error: PlaybackServiceError) -> mlua::Error {
    match error {
        PlaybackServiceError::BadRequest(message) | PlaybackServiceError::NotFound(message) => {
            mlua::Error::runtime(message)
        }
        PlaybackServiceError::Internal(error) => mlua::Error::external(error),
    }
}

async fn register_on_update(plugin_id: PluginId, handler: Function) -> Result<()> {
    let _registration = STATE
        .plugin_registries
        .ensure_registrations_open(&plugin_id)
        .await?;
    let counter = STATE.plugin_registries.inflight_counter(&plugin_id).await;
    let handle = PluginFunctionHandle::new(plugin_id, counter, handler);
    let mut registry = PLAYBACK_CALLBACK_REGISTRY.write().await;
    registry.add_update_handler(handle);
    Ok(())
}

struct PlaybackSessionsModule;

#[harmony_macros::module(
    plugin_scoped,
    name = "PlaybackSessions",
    local = "playback_sessions",
    path = "lyra/playback_sessions",
    aliases(PlaybackUpdateHandler),
    interfaces(
        PlaybackUpdatePayload,
        PlaybackStartRequest,
        PlaybackReportRequest,
        PlaybackSessionReportRequest,
        PlaybackSessionClearRequest,
        remote::SendCommandRequest,
        remote::ConnectionInfo,
        remote::PlaybackInfo
    )
)]
impl PlaybackSessionsModule {
    /// Registers a callback for playback updates.
    #[harmony(args(handler: PlaybackUpdateHandler))]
    pub(crate) async fn on_update(
        _lua: Lua,
        plugin_id: Option<Arc<str>>,
        handler: Function,
    ) -> Result<()> {
        let plugin_id = plugin_id
            .map(|raw| PluginId::new(raw).map_err(mlua::Error::external))
            .transpose()?;
        let plugin_id = plugin_id.ok_or_else(|| {
            mlua::Error::runtime("playback_sessions.on_update must be called from plugin Lua code")
        })?;
        register_on_update(plugin_id, handler).await
    }

    /// Reports progress for an existing playback session.
    #[harmony(args(request: PlaybackReportRequest))]
    pub(crate) async fn report(
        lua: Lua,
        plugin_id: Option<Arc<str>>,
        request_table: Table,
    ) -> Result<()> {
        let caller = request_caller(plugin_id)?;
        let request: PlaybackReportRequest =
            from_lua_json_value(&lua, mlua::Value::Table(request_table))?;
        let playback_session_id =
            require_positive_id(request.playback_session_id, "playback_session_id")?;
        let mutation = playback_mutation(request.position_ms, request.duration_ms, request.state);

        let current_ms = playbacks::now_ms().map_err(playback_service_error_to_lua)?;

        let mut db = STATE.db.write().await;
        let track_db_id =
            crate::plugins::db::playback_sessions::get_track_id(&*db, playback_session_id)
                .map_err(mlua::Error::external)?
                .ok_or_else(|| mlua::Error::runtime("playback session not found"))?;
        if !crate::routes::entity_accessible_to_principal(&*db, &caller.principal, track_db_id)
            .map_err(mlua::Error::external)?
        {
            return Err(mlua::Error::runtime("playback session not found"));
        }
        let update = playbacks::report_playback_with_cleanup(
            &mut db,
            playbacks::ReportPlaybackRequest {
                playback_session_id,
                user_db_id: Some(caller.principal.user_db_id),
                mutation,
                now_ms: current_ms,
                activity_policy: playbacks::ActivityPolicy::PlayingOnly,
                active_event: ActiveEvent::Progress,
            },
        )
        .map_err(playback_service_error_to_lua)?;
        let dispatch_caller = format!("plugin:{}", caller.plugin_id);
        dispatch_evicted_updates_for_caller(dispatch_caller.clone(), update.evicted_playbacks);

        drop(db);
        dispatch_playback_update_for_caller(dispatch_caller, &update.playback, update.event);
        Ok(())
    }

    /// Starts a playback session and returns its id.
    #[harmony(args(request: PlaybackStartRequest))]
    pub(crate) async fn start(
        lua: Lua,
        plugin_id: Option<Arc<str>>,
        request_table: Table,
    ) -> Result<i64> {
        let caller = request_caller(plugin_id)?;
        let request: PlaybackStartRequest =
            from_lua_json_value(&lua, mlua::Value::Table(request_table))?;
        let track_db_id = require_positive_id(request.track_id, "track_id")?;
        let user_db_id = require_positive_id(request.user_id, "user_id")?;
        if user_db_id != caller.principal.user_db_id {
            return Err(mlua::Error::runtime("user not found"));
        }
        let mutation = playback_mutation(request.position_ms, request.duration_ms, request.state);
        let current_ms = playbacks::now_ms().map_err(playback_service_error_to_lua)?;

        let mut db = STATE.db.write().await;
        if !crate::routes::entity_accessible_to_principal(&*db, &caller.principal, track_db_id)
            .map_err(mlua::Error::external)?
        {
            return Err(mlua::Error::runtime("track not found"));
        }
        let update = playbacks::start_playback_with_cleanup(
            &mut db,
            playbacks::StartPlaybackRequest {
                track_db_id,
                user_db_id,
                mutation,
                now_ms: current_ms,
                active_event: ActiveEvent::Started,
            },
        )
        .map_err(playback_service_error_to_lua)?;
        let dispatch_caller = format!("plugin:{}", caller.plugin_id);
        dispatch_evicted_updates_for_caller(dispatch_caller.clone(), update.evicted_playbacks);
        drop(db);

        let playback_session_id = update.playback.playback_session_id.0;
        dispatch_playback_update_for_caller(dispatch_caller, &update.playback, update.event);
        Ok(playback_session_id)
    }

    /// Reports plugin-scoped playback session progress and returns the session id when the playback remains active.
    #[harmony(args(request: PlaybackSessionReportRequest), returns(Option<i64>))]
    pub(crate) async fn report_session(
        lua: Lua,
        plugin_id: Option<Arc<str>>,
        request_table: Table,
    ) -> Result<Option<i64>> {
        let caller = request_caller(plugin_id)?;
        sessions::report_session(lua, &caller, request_table).await
    }

    /// Clears a plugin-scoped playback session.
    #[harmony(args(request: PlaybackSessionClearRequest))]
    pub(crate) async fn clear_session(
        lua: Lua,
        plugin_id: Option<Arc<str>>,
        request_table: Table,
    ) -> Result<()> {
        let caller = request_caller(plugin_id)?;
        sessions::clear_session(&lua, &caller, request_table)
    }

    /// Lists active connections for the given user with their playback state.
    #[harmony(args(user_id: i64), returns(Vec<remote::ConnectionInfo>))]
    pub(crate) async fn list_connections(
        lua: Lua,
        plugin_id: Option<Arc<str>>,
        user_id: i64,
    ) -> Result<mlua::Value> {
        let caller = request_caller(plugin_id)?;
        remote::list_connections(lua, &caller.principal, user_id).await
    }

    /// Sends a remote control command to a connection.
    #[harmony(args(request: remote::SendCommandRequest))]
    pub(crate) async fn send_command(
        lua: Lua,
        plugin_id: Option<Arc<str>>,
        request_table: Table,
    ) -> Result<()> {
        let caller = request_caller(plugin_id)?;
        remote::send_command(lua, &caller.principal, request_table).await
    }
}

crate::plugins::plugin_surface_exports!(
    PlaybackSessionsModule,
    "lyra.playback_sessions",
    "Read and modify active playback sessions.",
    Medium
);

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        Mutex,
    };
    use std::time::Duration;

    use tokio::sync::oneshot;
    use tokio::time::timeout;

    use mlua::LuaSerdeExt;

    use super::*;
    use crate::STATE;

    #[tokio::test]
    async fn dispatch_update_invokes_registered_handler() -> anyhow::Result<()> {
        {
            let mut registry = PLAYBACK_CALLBACK_REGISTRY.write().await;
            registry.clear_all_handlers();
        }

        let (tx, rx) = oneshot::channel::<PlaybackUpdatePayload>();
        let tx = Arc::new(Mutex::new(Some(tx)));

        let handler =
            STATE
                .lua
                .get()
                .create_async_function(move |lua: mlua::Lua, value: mlua::Value| {
                    let tx = tx.clone();
                    async move {
                        let payload: PlaybackUpdatePayload = lua.from_value(value)?;
                        if let Some(sender) = tx.lock().expect("poisoned test mutex").take() {
                            let _ = sender.send(payload);
                        }
                        Ok(())
                    }
                })?;

        let plugin_id = PluginId::new("test_plugin")?;
        let counter = STATE.plugin_registries.inflight_counter(&plugin_id).await;
        let handle = PluginFunctionHandle::new(plugin_id, counter, handler);
        {
            let mut registry = PLAYBACK_CALLBACK_REGISTRY.write().await;
            registry.add_update_handler(handle);
        }

        let expected = PlaybackUpdatePayload {
            event: "progress".to_string(),
            state: crate::plugins::db::PlaybackState::Playing,
            playback_session_public_id: "ps-pub-42".to_string(),
            track_public_id: "tr-pub-7".to_string(),
            user_public_id: "us-pub-1".to_string(),
            library_public_id: Some("lib-pub-1".to_string()),
            position_ms: 12_345,
            duration_ms: Some(67_890),
            activity_ms: 2_000,
            qualifies_single_listen: false,
            updated_at_ms: 1_700_000_000_000,
        };

        let playback = playbacks::PlaybackRecord {
            playback_session_id: agdb::DbId(42),
            playback_session_public_id: expected.playback_session_public_id.clone(),
            track_db_id: agdb::DbId(7),
            track_public_id: expected.track_public_id.clone(),
            user_db_id: agdb::DbId(1),
            user_public_id: expected.user_public_id.clone(),
            library_public_id: expected.library_public_id.clone(),
            playback: playbacks::PlaybackSession {
                db_id: Some(agdb::DbId(42)),
                id: expected.playback_session_public_id.clone(),
                position_ms: expected.position_ms,
                duration_ms: expected.duration_ms,
                activity_ms: Some(expected.activity_ms),
                last_position_ms: None,
                state: expected.state,
                listen_recorded: None,
                updated_at_ms: expected.updated_at_ms,
                created_at_ms: expected.updated_at_ms,
            },
        };
        dispatch_playback_update_for_caller(
            "plugin-playback-session-dispatch-test",
            &playback,
            expected.event.clone(),
        );

        let received = timeout(Duration::from_secs(1), rx).await??;
        assert_eq!(received.event, expected.event);
        assert_eq!(received.state, expected.state);
        assert_eq!(
            received.playback_session_public_id,
            expected.playback_session_public_id
        );
        assert_eq!(received.track_public_id, expected.track_public_id);
        assert_eq!(received.user_public_id, expected.user_public_id);
        assert_eq!(received.library_public_id, expected.library_public_id);
        assert_eq!(received.position_ms, expected.position_ms);
        assert_eq!(received.duration_ms, expected.duration_ms);
        assert_eq!(received.activity_ms, expected.activity_ms);
        assert_eq!(
            received.qualifies_single_listen,
            expected.qualifies_single_listen
        );
        assert_eq!(received.updated_at_ms, expected.updated_at_ms);

        let mut registry = PLAYBACK_CALLBACK_REGISTRY.write().await;
        registry.clear_all_handlers();

        Ok(())
    }
}
