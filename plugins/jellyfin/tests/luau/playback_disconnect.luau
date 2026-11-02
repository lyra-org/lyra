local fixture, harness = ...
local active_sessions = require("../../protocol/active_sessions")
local item_ids = require("../../protocol/items/identifiers")
local playback = require("../../protocol/items/playback")
local playback_sessions = require("@lyra/playback_sessions")
local session = require("../../protocol/session")

local sockets = harness.sockets

-- As upstream, a session whose socket closed is gone, and the host pauses what it played; a later
-- report from the same device brings it back.

local AUTHORIZATION =
	`MediaBrowser Client="Finamp", Device="Pixel 8", DeviceId="phone", Version="0.9.1", Token="{fixture.api_keys.phone}"`

local function request(path: string, json: any?): any
	return {
		plugin_id = "jellyfin",
		request = {
			method = if json then "POST" else "GET",
			path = path,
			headers = { authorization = AUTHORIZATION },
			query = {},
			body_raw = "",
			json = json,
		},
		params = {},
	}
end

local function report(event: string, position_ticks: number, track: string?, start_ticks: number?): number
	local body = {
		ItemId = item_ids.encode_jellyfin_item_id(fixture.ids[track or "song"]),
		PositionTicks = position_ticks,
		PlaybackStartTimeTicks = start_ticks,
	}
	return session.handle_playback_report_route(request("/Sessions/Playing", body), event).status
end

local function stop_unnamed(start_ticks: number?): number
	local body = { PlaybackStartTimeTicks = start_ticks }
	return session.handle_playback_report_route(request("/Sessions/Playing/Stopped", body), "stopped").status
end

local principal = session.require_jellyfin_principal(request("/Sessions"))
assert(principal, "the api key authenticates")

local function listed(): any?
	local sessions = active_sessions.list_visible(principal)
	assert(#sessions <= 1, `one device, got {#sessions}`)
	return sessions[1]
end

local function host_state(): string?
	local playing = playback_sessions.list_playbacks()[1]
	return if playing then playing.state else nil
end

local function host_track(): string?
	local playing = playback_sessions.list_playbacks()[1]
	return if playing then playing.track_id else nil
end

local started = report("started", 0)
assert(started == 204, `the report is accepted, got {started}`)
local reporting = listed()
assert(
	reporting and reporting.DeviceId == "phone",
	`the device is listed, got {if reporting then reporting.DeviceId else nil}`
)
assert(reporting.NowPlayingItem.Id == item_ids.encode_jellyfin_item_id(fixture.ids.song))
assert(reporting.PlayState.IsPaused == false)
assert(reporting.SupportsRemoteControl == false, "a session without a socket takes no commands")
local session_key = reporting.Id

-- A socket opening after the session reported holds what the session last reported.
local socket = sockets:open()
playback_sessions.hold_session(socket, session_key)
sockets:close(socket)
playback.end_session(session_key)
assert(listed() == nil, "a disconnected session is gone")
assert(host_state() == "paused", `the host pauses its playback, got {host_state()}`)
playback.end_session("unknown-session")

-- Without a socket holding the session, nothing pauses it on the session's behalf.
assert(report("progress", 40000000) == 204)
playback.end_session(session_key)
assert(host_state() == "playing", `a session without a socket plays on, got {host_state()}`)

-- A reconnect holds the session before the socket it replaces closes, so it plays on.
local replaced = sockets:open()
playback_sessions.hold_session(replaced, session_key)
local reconnected = sockets:open()
playback_sessions.hold_session(reconnected, session_key)
sockets:close(replaced)
assert(host_state() == "playing", `the reconnected socket holds the session, got {host_state()}`)

local resumed = report("progress", 50000000)
assert(resumed == 204, `the device reports again, got {resumed}`)
local back = listed()
assert(back and back.Id == session_key, "the resumed session is listed again")
assert(back.PlayState.IsPaused == false, "the resumed session plays")
-- A playing position advances from the report.
local ticks = back.PlayState.PositionTicks
assert(ticks >= 50000000 and ticks < 60000000, `the position advances from the report, got {ticks}`)
assert(host_state() == "playing", `the host plays it again, got {host_state()}`)

-- A stop that can't name its item ends what the session played.
local stopped = stop_unnamed()
assert(stopped == 204, `the stop is accepted, got {stopped}`)
assert(listed() == nil and host_state() == nil, "nothing is left playing")

-- A late stop of a play another replaced leaves the replacement playing, even unnamed.
assert(report("started", 0, "song", 100) == 204)
assert(report("started", 0, "other", 200) == 204)
assert(stop_unnamed(100) == 204, "the stale stop is accepted")
assert(host_track() == fixture.ids.other, `the replacement stays current, got {host_track()}`)
assert(host_state() == "playing", `the replacement plays on, got {host_state()}`)
assert(stop_unnamed(200) == 204)
assert(host_state() == nil, `the current play's stop ends it, got {host_state()}`)
