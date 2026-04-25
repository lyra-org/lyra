// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use mlua::{
    Lua,
    Result,
    Table,
    Value,
};
use serde::{
    Deserialize,
    Serialize,
};

use crate::{
    STATE,
    plugins::{
        LUA_SERIALIZE_OPTIONS,
        from_lua_json_value,
        require_non_empty_string,
        require_positive_id,
    },
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

use mlua::LuaSerdeExt;

#[harmony_macros::interface]
#[derive(Clone, Debug, Deserialize)]
pub(super) struct SendCommandRequest {
    user_id: i64,
    target_token: String,
    action: String,
    position_ms: Option<u64>,
    level: Option<f32>,
}

#[harmony_macros::interface]
#[derive(Serialize)]
pub(super) struct PlaybackInfo {
    track_id: i64,
    position_ms: u64,
    duration_ms: Option<u64>,
    state: String,
}

#[harmony_macros::interface]
#[derive(Serialize)]
pub(super) struct ConnectionInfo {
    token: String,
    session_key: String,
    supported_commands: Vec<String>,
    playback: Option<PlaybackInfo>,
    degraded: bool,
}

fn action_to_string(action: &RemoteAction) -> String {
    serde_json::to_value(action)
        .ok()
        .and_then(|v| v.as_str().map(String::from))
        .unwrap_or_default()
}

pub(super) async fn list_connections(lua: Lua, user_id: i64) -> Result<Value> {
    let user_db_id = require_positive_id(user_id, "user_id")?;
    let connections = registry::list_connections().await;
    let now_ms = playbacks::now_ms().map_err(mlua::Error::external)?;

    let playbacks_list = {
        let db = STATE.db.read().await;
        playbacks::list_playbacks(&db, user_db_id).map_err(mlua::Error::external)?
    };

    let mut result = Vec::new();
    for conn in &connections {
        if conn.user_db_id != user_db_id {
            continue;
        }

        let scope_key = PlaybackScopeKey {
            plugin_id: "native",
            user_db_id: conn.user_db_id,
            session_key: &conn.session_key,
        };

        let playback = playbacks::get_playback_session(&scope_key).and_then(|scope| {
            let session_id = scope.current_playback_session_id?;
            let record = playbacks_list
                .iter()
                .find(|p| p.playback_session_id == session_id)?;
            Some(PlaybackInfo {
                track_id: record.track_db_id.0,
                position_ms: record.playback.position_ms,
                duration_ms: record.playback.duration_ms,
                state: action_state_string(record.playback.state),
            })
        });

        let degraded = playbacks::is_remote_control_degraded(&scope_key, now_ms);

        let commands: Vec<String> = conn
            .supported_commands
            .iter()
            .map(action_to_string)
            .collect();

        result.push(ConnectionInfo {
            token: conn.token.clone(),
            session_key: conn.session_key.clone(),
            supported_commands: commands,
            playback,
            degraded,
        });
    }

    lua.to_value_with(&result, LUA_SERIALIZE_OPTIONS)
}

fn action_state_string(state: crate::db::PlaybackState) -> String {
    serde_json::to_value(state)
        .ok()
        .and_then(|v| v.as_str().map(String::from))
        .unwrap_or_default()
}

pub(super) async fn send_command(_lua: Lua, request_table: Table) -> Result<()> {
    let request: SendCommandRequest =
        from_lua_json_value(&_lua, mlua::Value::Table(request_table))?;
    let user_db_id = require_positive_id(request.user_id, "user_id")?;
    let target_token = require_non_empty_string(request.target_token, "target_token")?;
    let action_str = require_non_empty_string(request.action, "action")?;

    let action: RemoteAction =
        serde_json::from_value(serde_json::Value::String(action_str.clone()))
            .map_err(|_| mlua::Error::runtime(format!("unknown action: {action_str}")))?;

    let target = registry::resolve_token(&target_token)
        .await
        .ok_or_else(|| mlua::Error::runtime("connection not found"))?;

    if target.user_db_id != user_db_id {
        return Err(mlua::Error::runtime("not authorized to control target"));
    }

    if !target.supported_commands.contains(&action) {
        return Err(mlua::Error::runtime(format!(
            "target does not support command: {action_str}"
        )));
    }

    let data = match action {
        RemoteAction::Seek => {
            let position_ms = request
                .position_ms
                .ok_or_else(|| mlua::Error::runtime("position_ms required for seek"))?;
            ForwardedCommandData::Seek { position_ms }
        }
        RemoteAction::SetVolume => {
            let level = request
                .level
                .ok_or_else(|| mlua::Error::runtime("level required for set_volume"))?;
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
        .map_err(|e| mlua::Error::runtime(format!("command delivery failed: {e}")))?;

    if let Ok(now_ms) = playbacks::now_ms() {
        let scope_key = PlaybackScopeKey {
            plugin_id: "native",
            user_db_id: target.user_db_id,
            session_key: &target.session_key,
        };
        playbacks::mark_command_dispatched(&scope_key, now_ms);
    }

    Ok(())
}
