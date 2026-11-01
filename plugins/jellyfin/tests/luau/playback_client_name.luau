local fixture = ...
local item_ids = require("../../protocol/items/identifiers")
local session = require("../../protocol/session")

-- A playback report names the client by the device the Jellyfin client sent. A device name Lyra
-- can't store is dropped rather than refusing the report.

local function report(device: string): (number, any)
	local response = session.handle_playback_report_route(
		{
			plugin_id = "jellyfin",
			request = {
				method = "POST",
				path = "/Sessions/Playing",
				headers = {
					authorization = `MediaBrowser Client="Finamp", Device="{device}", DeviceId="phone", Version="0.9.1", Token="{fixture.api_keys.phone}"`,
				},
				query = {},
				body_raw = "",
				json = { ItemId = item_ids.encode_jellyfin_item_id(fixture.ids.song), PositionTicks = 0 },
			},
			params = {},
		} :: any,
		"started"
	)
	return response.status, response.body
end

local accepted = report("Pixel 8")
assert(accepted == 204, `the report is accepted, got {accepted}`)

for _, device in { string.rep("x", 129), "Pixel\n8", "Pixel\u{85}8" } do
	local status, body = report(device)
	assert(status == 204, `an unusable device name still reports, got {status}: {body and body.Message}`)
end
