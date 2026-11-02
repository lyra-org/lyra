local fixture = ...

local playback_sessions = require("@lyra/playback_sessions")
local task = require("@harmony/task")

local updates: { playback_sessions.PlaybackUpdatePayload } = {}
playback_sessions.on_update(function(update: playback_sessions.PlaybackUpdatePayload)
	table.insert(updates, update)
end)

local function report(event: playback_sessions.PlaybackEvent, rate: number?)
	local id, rejection = playback_sessions.report_session({
		session_key = "phone",
		track_id = fixture.ids.song,
		play_id = "rate-test-play-0001",
		position_ms = 10000,
		duration_ms = 200000,
		state = "playing",
		event = event,
		rate = rate,
	})
	assert(id and rejection == nil, `accepted, got {rejection}`)
end

local function listed(): playback_sessions.ActivePlaybackInfo
	local playing = playback_sessions.list_playbacks()[1]
	assert(playing, "the play is listed")
	return playing
end

report("started", 2)
assert(listed().rate == 2, `the play runs at its reported rate, got {listed().rate}`)
report("progress")
assert(listed().rate == 2, "a report without a rate keeps it")
report("progress", 0.5)
assert(listed().rate == 0.5, "a report changes the rate")

for _, rate in { 0.1, 4.5 } do
	local ok, err = pcall(report, "progress", rate)
	assert(not ok and string.find(tostring(err), "rate must be from 0.25 to 4", 1, true), tostring(err))
end
assert(listed().rate == 0.5, "a refused rate changes nothing")

task.wait(0.1)
assert(#updates > 0 and updates[#updates].rate == 0.5, "updates carry the rate")
