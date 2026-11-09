local fixture = ...
local item_ids = require("../../protocol/items/identifiers")
local listens = require("@lyra/listens")
local session = require("../../protocol/session")
local task = require("@harmony/task")

-- Jellify's reports, in the order it sends them: at two thirds of a track it sends a stop at the
-- track's full length while the track keeps playing, it reports progress only on a pause, resume
-- or seek, and it never stops a track it moves on from, only starts the next.

local AUTHORIZATION =
	`MediaBrowser Client="Jellify", Device="Pixel", DeviceId="jd1", Version="1.0", Token="{fixture.api_keys.phone}"`

local function send(path: string, event: string, body: { [string]: any }): number
	local status = session.handle_playback_report_route(
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
	assert(status == 204, `{event} {path}: accepted, got {status}`)
	return status
end

local function item(track: string): string
	return item_ids.encode(fixture.ids[track])
end

local function start(track: string)
	send("/Sessions/Playing", "started", {
		PlaySessionId = "1",
		ItemId = item(track),
		PositionTicks = 0,
		PlayMethod = "DirectPlay",
	})
end

local function stop(track: string, position_ms: number)
	send("/Sessions/Playing/Stopped", "stopped", {
		SessionId = "1",
		ItemId = item(track),
		PositionTicks = position_ms * 10000,
	})
end

local function progress(track: string, position_ms: number, paused: boolean)
	send("/Sessions/Playing/Progress", "progress", {
		PlaySessionId = "1",
		ItemId = item(track),
		PositionTicks = position_ms * 10000,
		IsPaused = paused,
		PlayMethod = "DirectPlay",
	})
end

local function listen_count(track: string): number
	local stats = listens.get({ fixture.ids[track] })[fixture.ids[track]]
	return if stats then stats.count else 0
end

-- An early stop, then a pause and resume of the same track, then the next track's start: one play.
start("first")
task.wait(3.4)
stop("first", 5000)
task.wait(0.2)
progress("first", 3600, true)
task.wait(0.1)
progress("first", 3600, false)
task.wait(1.3)
start("other")
assert(listen_count("first") == 1, `the first track counts once, got {listen_count("first")}`)

-- A track skipped before its early stop still counts, once the next track's start replaces it.
start("brief")
task.wait(1.5)
start("after")
assert(listen_count("brief") == 1, `the skipped track counts, got {listen_count("brief")}`)
