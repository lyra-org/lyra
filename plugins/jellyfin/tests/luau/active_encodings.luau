local fixture = ...
local session = require("../../protocol/session")

local function stop(token: string?, query): number
	local response = session.handle_stop_encoding_route({
		plugin_id = "jellyfin",
		request = {
			method = "DELETE",
			path = "/",
			headers = if token then { authorization = `MediaBrowser Token="{token}"` } else {},
			query = query,
			body_raw = "",
			json = nil,
		},
		params = {},
	} :: any)
	return response.status
end

local token = fixture.api_keys.phone
local both = { deviceId = { "phone" }, playSessionId = { "abc" } }
assert(stop(nil, both) == 401)
assert(stop(token, {}) == 400)
assert(stop(token, { deviceId = { "phone" } }) == 400)
assert(stop(token, { playSessionId = { "abc" } }) == 400)
assert(stop(token, { deviceId = { " " }, playSessionId = { "abc" } }) == 400)
assert(stop(token, both) == 204)
assert(stop(token, { DeviceId = { "phone" }, PlaySessionId = { "abc" } }) == 204)
