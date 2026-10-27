local fixture = ...
local item_ids = require("../../protocol/items/identifiers")
local session = require("../../protocol/session")

-- As upstream, a report naming no item succeeds and records nothing; a track the caller can't see
-- reads as one that doesn't exist. Only a missing body is a client error.

local function report(item_id: string?, event: string?): number
	local body = if item_id then { ItemId = item_id, PositionTicks = 0 } else nil
	local response = session.handle_playback_report_route(
		{
			plugin_id = "jellyfin",
			request = {
				method = "POST",
				path = "/Sessions/Playing",
				headers = { authorization = `MediaBrowser Token="{fixture.api_keys.phone}"` },
				query = {},
				body_raw = "",
				json = body,
			},
			params = {},
		} :: any,
		event or "started"
	)
	return response.status
end

local hidden_id = item_ids.encode_jellyfin_item_id(fixture.ids.hidden)
for _, event in { "started", "progress", "stopped" } do
	local hidden = report(hidden_id, event)
	assert(hidden == 204, `{event}: a track the caller can't see is accepted, got {hidden}`)
	local unknown = report(string.rep("a", 32), event)
	assert(unknown == hidden, `{event}: it answers as an unknown item does, got {unknown}`)
end
local missing = report(nil)
assert(missing == 400, `a missing body is a client error, got {missing}`)
