local fixture = ...

local errors = require("@lyra/errors")
local playback_sessions = require("@lyra/playback_sessions")

local function rejects(f: (...any) -> ...any, message: string, ...: any)
	local ok, err = pcall(f, ...)
	assert(not ok, `rejected: {message}`)
	assert(string.find(tostring(err), message, 1, true), `error {err}`)
	return errors.kind(err)
end

local connections = playback_sessions.list_connections()
assert(#connections == 2, `only the principal's connections are listed, got {#connections}`)

local phone, speaker = connections[1], connections[2]
assert(phone.id == fixture.connections.phone, "oldest first")
assert(phone.user_id == fixture.users.owner and phone.username ~= "", "the connection's user")
assert(phone.session_key == "phone", "the session key defaults to the fixture key")
assert(phone.client_name == "Phone")
assert(#phone.supported_commands == 0)
assert(phone.playback_id == nil and phone.playback == nil, "an idle connection drives nothing")
assert(phone.remote_control_degraded == false)
assert(phone.connected_at_ms > 0)

assert(speaker.id == fixture.connections.speaker)
assert(speaker.session_key == "living-room")
assert(speaker.client_name == nil)
local commands: { [playback_sessions.RemoteAction]: boolean } = {}
for _, command in speaker.supported_commands do
	commands[command] = true
end
assert(commands.pause and commands.seek and #speaker.supported_commands == 2, "declared commands")

playback_sessions.send_command({ connection_id = speaker.id, action = "pause" })
playback_sessions.send_command({ connection_id = speaker.id, action = "seek", position_ms = 1000 })

assert(
	rejects(
		playback_sessions.send_command,
		"target does not support command: stop",
		{ connection_id = speaker.id, action = "stop" }
	) == nil
)
assert(
	rejects(
		playback_sessions.send_command,
		"not authorized to control target",
		{ connection_id = fixture.connections.elsewhere, action = "pause" }
	) == "forbidden"
)
assert(
	rejects(
		playback_sessions.send_command,
		"target connection not found",
		{ connection_id = "missing", action = "pause" }
	) == "not_found"
)
assert(
	rejects(playback_sessions.list_connections, "ManageUsers permission required", { all_users = true }) == "forbidden"
)
rejects(
	playback_sessions.list_connections,
	"list_connections takes user_id or all_users, not both",
	{ user_id = fixture.users.other, all_users = true }
)
assert(#playback_sessions.list_connections({ user_id = fixture.users.owner }) == 2, "the principal's own by id")

rejects(playback_sessions.send_command, "connection not found", { connection_id = "missing", action = "pause" })
rejects(
	playback_sessions.send_command,
	"unknown variant `rewind`, expected one of `play`, `pause`",
	{ connection_id = speaker.id, action = "rewind" :: any }
)
