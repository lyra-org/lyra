local fixture = ...

local errors = require("@lyra/errors")
local playback_sessions = require("@lyra/playback_sessions")
local task = require("@harmony/task")

local updates: { playback_sessions.PlaybackUpdatePayload } = {}
playback_sessions.on_update(function(update: playback_sessions.PlaybackUpdatePayload)
	table.insert(updates, update)
end)

local function report(play_id: string, event: playback_sessions.PlaybackEvent, counts_as_listen: boolean?)
	return playback_sessions.report_session({
		session_key = "phone",
		track_id = fixture.ids.song,
		play_id = play_id,
		position_ms = 0,
		duration_ms = 200000,
		state = "playing",
		event = event,
		counts_as_listen = counts_as_listen,
	})
end

local function listed(): playback_sessions.ActivePlaybackInfo
	local playing = playback_sessions.list_playbacks()[1]
	assert(playing, "the play is listed")
	return playing
end

-- A play declared not to count still plays.
local id, rejection = report("sleep-play-0000001", "started", false)
assert(id and rejection == nil, `accepted, got {rejection}`)
assert(listed().counts_as_listen == false and listed().state == "playing")

id, rejection = report("sleep-play-0000001", "progress")
assert(id and rejection == nil, `a report may leave it out, got {rejection}`)
assert(listed().counts_as_listen == false, "leaving it out keeps it")

local ok, err = pcall(report, "sleep-play-0000001", "progress", true)
assert(not ok and errors.kind(err) == "conflict", tostring(err))
assert(string.find(tostring(err), "counts_as_listen is false", 1, true), tostring(err))
assert(listed().counts_as_listen == false, "the refused report changes nothing")

-- A new play counts unless it says otherwise.
id, rejection = report("next-play-00000001", "started")
assert(id and rejection == nil, `accepted, got {rejection}`)
assert(listed().counts_as_listen == true)

task.wait(0.1)
assert(#updates >= 3, `updates are still sent, got {#updates}`)
assert(updates[1].counts_as_listen == false and updates[#updates].counts_as_listen == true)
