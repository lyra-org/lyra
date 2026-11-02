local fixture = ...

local api = require("@lyra/api")
local bitrate_test = require("../../routes/bitrate_test")

local function request(api_key: string?, size: string?)
	local query = { api_key = if api_key then { api_key } else nil, Size = if size then { size } else nil }
	return bitrate_test.handle({
		plugin_id = "jellyfin",
		request = {
			method = "GET",
			path = "/Playback/BitrateTest",
			headers = {},
			query = query,
			body_raw = "",
		},
		params = {},
	} :: any)
end

-- The server generates the filler as it sends it, up to upstream's 100 MB.
local default = request(fixture.api_keys.phone)
assert(default.kind == "filler" and default.size_bytes == 102400, `default {default.kind} {default.size_bytes}`)
assert(default.headers["content-type"] == "application/octet-stream")
local largest = request(fixture.api_keys.phone, "100000000")
assert(largest.kind == "filler" and largest.size_bytes == 100000000, "upstream's largest size")
assert(api.MAX_FILLER_BYTES >= largest.size_bytes, "the server serves upstream's largest size")

for _, size in { "0", "100000001", "1.5", "big" } do
	local refused = request(fixture.api_keys.phone, size)
	assert(refused.status == 400, `size {size} status {refused.status}`)
end
assert(request(nil).status == 401, "the test needs a signed-in caller")
