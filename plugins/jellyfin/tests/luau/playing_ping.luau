local fixture = ...
local session = require("../../protocol/session")

local function ping(token: string?, query): number
	local response = session.handle_playback_ping_route({
		plugin_id = "jellyfin",
		request = {
			method = "POST",
			path = "/",
			headers = if token then { authorization = `Bearer {token}` } else {},
			query = query,
			body_raw = "",
			json = nil,
		},
		params = {},
	} :: any)
	return response.status
end

local token = fixture.api_keys.phone
assert(ping(nil, { playSessionId = { "abc" } }) == 401)
assert(ping(token, {}) == 400)
assert(ping(token, { playSessionId = { "abc" } }) == 204)
assert(ping(token, { PlaySessionId = { "abc" } }) == 204)
