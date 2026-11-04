local fixture = ...

local auth = require("@lyra/auth")
local playback_sessions = require("@lyra/playback_sessions")

local function act_as(session_key: string)
	assert(auth.resolve_auth(fixture.sessions[session_key]) ~= nil, `acting as {session_key}`)
end

local function report(session_key: string, track: string, state: playback_sessions.PlaybackState?)
	local reported, rejection = playback_sessions.report_session({
		session_key = session_key,
		track_id = fixture.ids[track],
		client_name = "Phone",
		position_ms = 5000,
		duration_ms = 200000,
		state = state or "playing",
		event = if state == "stopped" then "stopped" else "started",
	})
	assert(reported and rejection == nil, `{session_key}: accepted, got {rejection}`)
end

act_as("listener_phone")
report("phone", "song")
report("tablet", "secret")
local listed_own = playback_sessions.list_playbacks()
assert(#listed_own == 2, `the listener sees both of their own, got {#listed_own}`)
act_as("admin_web")
report("desk", "song")

act_as("manager_web")
assert(#playback_sessions.list_playbacks() == 0, "the manager plays nothing")

-- manage_users reaches every non-admin's playback, leaving out tracks the manager can't see.
local everyone = playback_sessions.list_user_playbacks({ all_users = true })
assert(#everyone == 1, `one playback the manager may see, got {#everyone}`)
local phone = everyone[1]
assert(phone.user_id == fixture.users.listener)
assert(phone.username ~= "")
assert(phone.track_id == fixture.ids.song)
assert(phone.play_id ~= "")
assert(phone.state == "playing")
assert(phone.position_ms == 5000 and phone.duration_ms == 200000)
assert(phone.effective_position_ms >= 5000 and phone.effective_position_ms < 200000, "it advances while playing")
assert(phone.client_name == "Phone")
assert(phone.plugin_id == "test")
assert(phone.session_key == "phone")
assert(phone.connection_id == nil, "no connection of Lyra's own clients drives it")
assert(phone.started_at_ms > 0 and phone.updated_at_ms >= phone.started_at_ms)
assert(#playback_sessions.list_user_playbacks({ user_id = fixture.users.listener }) == 1)

local ok, err = pcall(playback_sessions.list_user_playbacks, { user_id = fixture.users.admin })
assert(not ok and string.find(tostring(err), "Admin permission required", 1, true), tostring(err))

-- Another user's or every user's must be named; the caller's own come from list_playbacks.
ok, err = pcall(playback_sessions.list_user_playbacks, {})
assert(not ok and string.find(tostring(err), "takes user_id or all_users", 1, true), tostring(err))

-- Connections follow the same rules.
local connections = playback_sessions.list_connections({ all_users = true })
assert(#connections == 1, `the listener's connection alone, got {#connections}`)
assert(connections[1].id == fixture.connections.listener_speaker)
assert(connections[1].user_id == fixture.users.listener)
assert(connections[1].client_name == "Speaker")

-- A stopped playback is no longer active.
act_as("listener_phone")
report("phone", "song", "stopped")
act_as("manager_web")
assert(#playback_sessions.list_user_playbacks({ all_users = true }) == 0, "the stopped playback is gone")

-- An admin reaches everyone's.
act_as("admin_web")
local all = playback_sessions.list_user_playbacks({ all_users = true })
local tracks: { [string]: string } = {}
for _, playback in all do
	tracks[playback.session_key :: string] = playback.track_id
end
assert(#all == 2 and tracks.tablet == fixture.ids.secret and tracks.desk == fixture.ids.song)
assert(#playback_sessions.list_connections({ all_users = true }) == 2)
