// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use mlua::{
    Lua,
    Result,
    Table,
};

use crate::{
    STATE,
    services::playback_sessions::{
        self as playbacks,
        dispatch_evicted_updates,
        dispatch_playback_update,
    },
};

use super::{
    PlaybackSessionClearRequest,
    PlaybackSessionReportRequest,
    playback_mutation,
    playback_service_error_to_lua,
};

use crate::plugins::{
    from_lua_json_value,
    require_non_empty_string,
    require_positive_id,
};

pub(crate) async fn report_session(lua: Lua, request_table: Table) -> Result<Option<i64>> {
    let request: PlaybackSessionReportRequest =
        from_lua_json_value(&lua, mlua::Value::Table(request_table))?;
    let plugin_id = require_non_empty_string(request.plugin_id, "plugin_id")?;
    let user_db_id = require_positive_id(request.user_id, "user_id")?;
    let session_key = require_non_empty_string(request.session_key, "session_key")?;
    let track_db_id = require_positive_id(request.track_id, "track_id")?;
    let event = request.event;
    let active_event =
        playbacks::classify_active_event(event).map_err(playback_service_error_to_lua)?;
    let current_ms = playbacks::now_ms().map_err(playback_service_error_to_lua)?;
    let mutation = playback_mutation(request.position_ms, request.duration_ms, request.state);

    let mut db = STATE.db.write().await;
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
    .map_err(playback_service_error_to_lua)?;
    dispatch_evicted_updates(update.evicted_playbacks);

    let playbacks::OptionalPlaybackUpdateResult {
        playback, event, ..
    } = update;

    let Some(playback) = playback else {
        return Ok(None);
    };

    drop(db);
    let event_label = event
        .map(|value| value.to_string())
        .unwrap_or_else(|| active_event.to_string());
    dispatch_playback_update(&playback, event_label);
    Ok(Some(playback.playback_session_id.0))
}

pub(crate) fn clear_session(lua: &Lua, request_table: Table) -> Result<()> {
    let request: PlaybackSessionClearRequest =
        from_lua_json_value(lua, mlua::Value::Table(request_table))?;
    let plugin_id = require_non_empty_string(request.plugin_id, "plugin_id")?;
    let user_db_id = require_positive_id(request.user_id, "user_id")?;
    let session_key = require_non_empty_string(request.session_key, "session_key")?;

    playbacks::clear_playback_session(&plugin_id, user_db_id, &session_key);
    Ok(())
}
