local fixture = ...
local item_ids = require("../../protocol/items/identifiers")
local listens = require("@lyra/listens")
local playback_sessions = require("@lyra/playback_sessions")
local session = require("../../protocol/session")
local task = require("@harmony/task")

-- Finamp's reports, in the order it sends them: it builds the previous track's stop after
-- switching its current track, so the stop carries the next track's start, and its last stop
-- carries the player's raw position, which can pass the track's end.

local AUTHORIZATION =
	`MediaBrowser Token="{fixture.api_keys.phone}", Client="Finamp", Device="Pixel 8",DeviceId="dev1", Version="0.9.20"`

local updates: { [string]: playback_sessions.PlaybackUpdatePayload } = {}
playback_sessions.on_update(function(update: playback_sessions.PlaybackUpdatePayload)
	updates[update.track_id] = update
end)

local function report(event: string, track: string, position_ms: number, start_ticks: number, paused: boolean?): number
	local path = if event == "started"
		then "/Sessions/Playing"
		elseif event == "stopped" then "/Sessions/Playing/Stopped"
		else "/Sessions/Playing/Progress"
	local body = {
		CanSeek = true,
		ItemId = item_ids.encode(fixture.ids[track]),
		PlaySessionId = `play-{track}`,
		SessionId = "dev1",
		IsPaused = paused == true,
		IsMuted = false,
		PositionTicks = position_ms * 10000,
		PlaybackStartTimeTicks = start_ticks,
		VolumeLevel = 100,
		PlayMethod = "DirectPlay",
		RepeatMode = "RepeatNone",
		PlaybackOrder = "Default",
		PlaylistItemId = `queue-{track}`,
	}
	return session.handle_playback_report_route(
		{
			plugin_id = "jellyfin",
			request = {
				method = "POST",
				path = path,
				headers = { authorization = AUTHORIZATION },
				query = {},
				body_raw = "",
				json = body,
			},
			params = {},
		} :: any,
		event
	).status
end

local function host_play(track: string): any?
	for _, playing in playback_sessions.list_playbacks() do
		if playing.track_id == fixture.ids[track] then
			return playing
		end
	end
	return nil
end

local function expect(status: number, step: string)
	assert(status == 204, `{step}: accepted, got {status}`)
end

local t1 = os.time() * 1000 * 10000
local t2 = t1 + 10000000

expect(report("started", "first", 0, t1), "start first")
expect(report("progress", "first", 10000, t1), "progress first")
expect(report("progress", "first", 12000, t1, true), "pause first")
expect(report("progress", "first", 12000, t1), "resume first")
expect(report("stopped", "first", 20000, t2), "stop first, carrying second's start")
assert(host_play("first") == nil, "the stop ends the first play")
expect(report("started", "second", 0, t2), "start second")
expect(report("progress", "second", 150000, t2), "progress second")
assert(host_play("second"), "the second play is playing")
expect(report("stopped", "second", 200040, t2), "stop second past its end")
assert(host_play("second") == nil, "the stop past the end ends the second play")

task.wait(0.1)
local ended = updates[fixture.ids.second]
assert(ended, "the second play's end is announced")
assert(ended.state == "completed", `a stop at the end completes the play, got {ended.state}`)
assert(ended.position_ms == 200000, `the position reads as the end, got {ended.position_ms}`)

-- A play whose stop never arrived is credited for what it played when the next one starts.
local t3 = t2 + 10000000
expect(report("started", "short", 0, t3), "start short")
task.wait(0.6)
expect(report("started", "after", 0, t3 + 10000000), "start after, without stopping short")
local counted = listens.get({ fixture.ids.short })[fixture.ids.short]
assert(counted and counted.count == 1, "the short play counts as a listen")
task.wait(0.1)
local short = updates[fixture.ids.short]
assert(
	short and short.event == "started",
	`crediting the replaced play announces only its listen, got {if short then short.event else nil}`
)
