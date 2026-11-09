local fixture = ...
local errors = require("@lyra/errors")
local playback_sessions = require("@lyra/playback_sessions")
local tracks = require("@lyra/tracks")

local ids = fixture.ids
local speaker = fixture.connections.speaker

local function send(request: any): (boolean, any)
	request.action = request.action or "play_tracks"
	request.connection_id = request.connection_id or speaker
	return pcall(playback_sessions.send_command, request)
end

local function fails_with(message: string, kind: string?, request: any)
	local ok, err = send(request)
	assert(not ok, `should fail: {message}`)
	assert(string.find(tostring(err), message, 1, true), tostring(err))
	assert(errors.kind(err) == kind, tostring(errors.kind(err)))
end

assert(send({ track_ids = { ids.song, ids.song }, placement = "now", start_index = 1, start_position_ms = 2000 }))
assert(send({ track_ids = { ids.song }, placement = "next" }))

fails_with("missing field `track_ids`", "invalid", { placement = "now" })
fails_with("missing field `placement`", "invalid", { track_ids = { ids.song } })
fails_with("unknown variant `sideways`", "invalid", { track_ids = { ids.song }, placement = "sideways" })
fails_with("track_ids cannot be empty", "invalid", { track_ids = {}, placement = "now" })
fails_with(
	`at most {playback_sessions.MAX_PLAY_TRACK_IDS} track ids`,
	"invalid",
	{ track_ids = table.create(playback_sessions.MAX_PLAY_TRACK_IDS + 1, ids.song), placement = "now" }
)
fails_with("start_index 1 is outside", "invalid", { track_ids = { ids.song }, placement = "now", start_index = 1 })

-- A value with no JSON form never reaches the request's decoding, so it carries no kind.
local function no_json_form(request: any): (boolean, any)
	return pcall(playback_sessions.send_command, request)
end
local function_ok, function_err = no_json_form({ action = "pause", connection_id = speaker, extra = print })
assert(not function_ok and errors.kind(function_err) == nil, tostring(function_err))

local unavailable = "a track is missing or not visible to the target's user"
fails_with(unavailable, "not_found", { track_ids = { ids.song, ids.secret }, placement = "now" })
fails_with(unavailable, "not_found", { track_ids = { "missing" }, placement = "now" })
fails_with(
	"target does not support command: play_tracks",
	"conflict",
	{ connection_id = fixture.connections.knob, track_ids = { ids.song }, placement = "now" }
)
fails_with(
	"not authorized to control target",
	"forbidden",
	{ connection_id = fixture.connections.elsewhere, track_ids = { ids.secret }, placement = "now" }
)

local visible = tracks.visible_to(fixture.users.owner, { ids.song, ids.secret, "missing", ids.song })
assert(visible[ids.song] == true, "a track the user sees")
assert(visible[ids.secret] == nil and visible.missing == nil, "hidden and missing tracks are absent")
assert(next(tracks.visible_to(fixture.users.owner, {})) == nil, "an empty list")
assert(tracks.visible_to(fixture.users.owner, table.create(2000, ids.song))[ids.song], "any number of ids")

local ok, err = pcall(tracks.visible_to, fixture.users.other, { ids.song })
assert(not ok and errors.kind(err) == "forbidden", "another user's needs manage_users")
