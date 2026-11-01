local fixture, harness = ...

local playback_sessions = require("@lyra/playback_sessions")
local task = require("@harmony/task")

local sockets = harness.sockets

local updates: { playback_sessions.PlaybackUpdatePayload } = {}
playback_sessions.on_update(function(update: playback_sessions.PlaybackUpdatePayload)
	table.insert(updates, update)
end)

local function report(session_key: string, track_id: string, event: playback_sessions.PlaybackEvent)
	local reported, rejection = playback_sessions.report_session({
		session_key = session_key,
		track_id = track_id,
		position_ms = 1000,
		duration_ms = 200000,
		state = "playing",
		event = event,
	})
	assert(reported and rejection == nil, `accepted, got {rejection}`)
end

local function settle(): { playback_sessions.PlaybackUpdatePayload }
	task.wait(0.1)
	local seen = table.clone(updates)
	table.clear(updates)
	return seen
end

local kitchen = sockets:open()
local spare = sockets:open()
playback_sessions.hold_session(kitchen, "kitchen")
playback_sessions.hold_session(spare, " kitchen ")
report("kitchen", fixture.ids.song, "started")
report("hall", fixture.ids.other, "started")
settle()

-- While another socket holds the session, closing one pauses nothing.
sockets:close(spare)
local paused = settle()
assert(#paused == 0, `the other socket still holds the session, got {#paused} updates`)

sockets:close(kitchen)
paused = settle()
assert(#paused == 1, `only the held session pauses, got {#paused} updates`)
assert(paused[1].track_id == fixture.ids.song)
assert(paused[1].state == "paused", `paused, got {paused[1].state}`)
assert(paused[1].event == "progress", `a progress update, got {paused[1].event}`)
assert(paused[1].position_ms == 1000, `at its last reported position, got {paused[1].position_ms}`)

-- A closed socket holds nothing.
playback_sessions.hold_session(kitchen, "hall")
sockets:close(kitchen)
assert(#settle() == 0, "the hall plays on")

local ok, err = pcall(function()
	playback_sessions.hold_session(sockets:open(), "  ")
end)
assert(not ok and string.find(tostring(err), "session_key must be a non-empty string", 1, true), tostring(err))
