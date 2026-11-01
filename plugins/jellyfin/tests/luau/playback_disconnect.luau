local fixture, harness = ...
local item_ids = require("../../protocol/items/identifiers")
local playback = require("../../protocol/items/playback")
local playback_sessions = require("@lyra/playback_sessions")
local session = require("../../protocol/session")

local sockets = harness.sockets

-- A session whose socket closed is no longer listed as playing, and its playback can be picked up
-- again by a later report from the same device.

local function report(event: string, position_ticks: number): number
	local response = session.handle_playback_report_route(
		{
			plugin_id = "jellyfin",
			request = {
				method = "POST",
				path = "/Sessions/Playing",
				headers = {
					authorization = `MediaBrowser Client="Finamp", Device="Pixel 8", DeviceId="phone", Version="0.9.1", Token="{fixture.api_keys.phone}"`,
				},
				query = {},
				body_raw = "",
				json = { ItemId = item_ids.encode_jellyfin_item_id(fixture.ids.song), PositionTicks = position_ticks },
			},
			params = {},
		} :: any,
		event
	)
	return response.status
end

local started = report("started", 0)
assert(started == 204, `the report is accepted, got {started}`)
local session_key: string? = nil
for key in playback.list_client_playbacks() do
	session_key = key
end
assert(session_key, "the reporting session is playing")

local socket = sockets:open()
playback_sessions.hold_session(socket, session_key)
sockets:close(socket)
playback.forget_client_playback(session_key)
assert(next(playback.list_client_playbacks()) == nil, "a disconnected session plays nothing")
playback.forget_client_playback("unknown-session")

local resumed = report("progress", 50000000)
assert(resumed == 204, `the device reports again, got {resumed}`)
assert(playback.list_client_playbacks()[session_key], "the resumed session is playing again")
