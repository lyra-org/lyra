local fixture = ...

local playback_sessions = require("@lyra/playback_sessions")
local task = require("@harmony/task")

local updates: { playback_sessions.PlaybackUpdatePayload } = {}
playback_sessions.on_update(function(update: playback_sessions.PlaybackUpdatePayload)
	table.insert(updates, update)
end)

local function report(session_key: string, track_id: string, client_name: string?): (string?, string?)
	return playback_sessions.report_session({
		session_key = session_key,
		track_id = track_id,
		client_name = client_name,
		position_ms = 0,
		duration_ms = 200000,
		state = "playing",
		event = "started",
	})
end

local id, rejection = report("kitchen", fixture.ids.song, "  Kitchen\u{200B} Speaker  ")
assert(id and rejection == nil, `accepted, got {rejection}`)

local _, control = report("hall", fixture.ids.other, "Hall\nSpeaker")
assert(control == "client_name cannot contain control characters", `rejected, got {control}`)
local _, long = report("hall", fixture.ids.other, string.rep("x", 129))
assert(long == "client_name cannot exceed 128 characters", `rejected, got {long}`)

task.wait(0.1)
assert(#updates == 1, `one accepted report, got {#updates}`)
local update = updates[1]
assert(update.track_id == fixture.ids.song)
assert(update.plugin_id == "test", `the reporting plugin is recorded, got {update.plugin_id}`)
