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
    db::PlaybackState,
    plugins::{
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
        dispatch_evicted_updates,
        dispatch_playback_update,
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
        _plugin_id: Option<Arc<str>>,
        request_table: Table,
    ) -> Result<()> {
        let request: PlaybackReportRequest =
            from_lua_json_value(&lua, mlua::Value::Table(request_table))?;
        let playback_session_id =
            require_positive_id(request.playback_session_id, "playback_session_id")?;
        let mutation = playback_mutation(request.position_ms, request.duration_ms, request.state);

        let current_ms = playbacks::now_ms().map_err(playback_service_error_to_lua)?;

        let mut db = STATE.db.write().await;
        let update = playbacks::report_playback_with_cleanup(
            &mut db,
            playbacks::ReportPlaybackRequest {
                playback_session_id,
                user_db_id: None,
                mutation,
                now_ms: current_ms,
                activity_policy: playbacks::ActivityPolicy::PlayingOnly,
                active_event: ActiveEvent::Progress,
            },
        )
        .map_err(playback_service_error_to_lua)?;
        dispatch_evicted_updates(update.evicted_playbacks);

        drop(db);
        dispatch_playback_update(&update.playback, update.event);
        Ok(())
    }

    /// Starts a playback session and returns its id.
    #[harmony(args(request: PlaybackStartRequest))]
    pub(crate) async fn start(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        request_table: Table,
    ) -> Result<i64> {
        let request: PlaybackStartRequest =
            from_lua_json_value(&lua, mlua::Value::Table(request_table))?;
        let track_db_id = require_positive_id(request.track_id, "track_id")?;
        let user_db_id = require_positive_id(request.user_id, "user_id")?;
        let mutation = playback_mutation(request.position_ms, request.duration_ms, request.state);
        let current_ms = playbacks::now_ms().map_err(playback_service_error_to_lua)?;

        let mut db = STATE.db.write().await;
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
        dispatch_evicted_updates(update.evicted_playbacks);
        drop(db);

        let playback_session_id = update.playback.playback_session_id.0;
        dispatch_playback_update(&update.playback, update.event);
        Ok(playback_session_id)
    }

    /// Reports plugin-scoped playback session progress and returns the session id when the playback remains active.
    #[harmony(args(request: PlaybackSessionReportRequest), returns(Option<i64>))]
    pub(crate) async fn report_session(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        request_table: Table,
    ) -> Result<Option<i64>> {
        sessions::report_session(lua, request_table).await
    }

    /// Clears a plugin-scoped playback session.
    #[harmony(args(request: PlaybackSessionClearRequest))]
    pub(crate) fn clear_session(lua: &Lua, request_table: Table) -> Result<()> {
        sessions::clear_session(lua, request_table)
    }

    /// Lists active connections for the given user with their playback state.
    #[harmony(args(user_id: i64), returns(Vec<remote::ConnectionInfo>))]
    pub(crate) async fn list_connections(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        user_id: i64,
    ) -> Result<mlua::Value> {
        remote::list_connections(lua, user_id).await
    }

    /// Sends a remote control command to a connection.
    #[harmony(args(request: remote::SendCommandRequest))]
    pub(crate) async fn send_command(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        request_table: Table,
    ) -> Result<()> {
        remote::send_command(lua, request_table).await
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
    use crate::services::playback_sessions::dispatch_update;

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
            state: crate::db::PlaybackState::Playing,
            playback_session_id: 42,
            track_id: 7,
            user_id: 1,
            position_ms: 12_345,
            duration_ms: Some(67_890),
            activity_ms: 2_000,
            qualifies_single_listen: false,
            updated_at_ms: 1_700_000_000_000,
        };

        dispatch_update(expected.clone());

        let received = timeout(Duration::from_secs(1), rx).await??;
        assert_eq!(received.event, expected.event);
        assert_eq!(received.state, expected.state);
        assert_eq!(received.playback_session_id, expected.playback_session_id);
        assert_eq!(received.track_id, expected.track_id);
        assert_eq!(received.user_id, expected.user_id);
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
