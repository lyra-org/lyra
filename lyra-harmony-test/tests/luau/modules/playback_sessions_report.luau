local fixture = ...

local playback_sessions = require("@lyra/playback_sessions")
local task = require("@harmony/task")

local updates: { [string]: playback_sessions.PlaybackUpdatePayload } = {}
playback_sessions.on_update(function(update: playback_sessions.PlaybackUpdatePayload)
	updates[update.track_id] = update
end)

local function report(session_key: string, client_name: string?): string
	local track_id = fixture.ids[session_key]
	local id, rejection = playback_sessions.report_session({
		session_key = session_key,
		track_id = track_id,
		client_name = client_name,
		position_ms = 0,
		duration_ms = 200000,
		state = "playing",
		event = "started",
	})
	assert(id and rejection == nil, `{session_key}: accepted, got {rejection}`)
	return track_id
end

-- A client name is tidied rather than refused, and a name with nothing left reports none.
local named = {
	[report("song", "  Kitchen\u{200B} Speaker  ")] = "Kitchen Speaker",
	[report("other", "Hall\nSpeaker\u{85}")] = "HallSpeaker",
	[report("porch", string.rep("x", 129))] = string.rep("x", 128),
	[report("attic", "\t\u{200B}")] = false,
	[report("cellar")] = false,
}

task.wait(0.1)
for id, expected in named do
	local update = updates[id]
	assert(update, `an update for {id}`)
	assert(update.client_name == (expected or nil), `client name {update.client_name}, expected {expected}`)
	assert(update.plugin_id == "test", `the reporting plugin is recorded, got {update.plugin_id}`)
end
