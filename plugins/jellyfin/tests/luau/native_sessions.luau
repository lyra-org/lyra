local fixture = ...
local active_sessions = require("../../protocol/active_sessions")
local session = require("../../protocol/session")

-- A native connection lists under the name its client gave, and under its session key and Lyra
-- without one.

local principal = session.require_jellyfin_principal({
	plugin_id = "jellyfin",
	request = {
		method = "GET",
		path = "/Sessions",
		headers = { authorization = `MediaBrowser Token="{fixture.api_keys.phone}"` },
		query = {},
		body_raw = "",
		json = nil,
	},
	params = {},
} :: any)
assert(principal, "the api key authenticates")

local by_id: { [string]: any } = {}
for _, dto in active_sessions.list_visible(principal) do
	by_id[(dto :: any).Id] = dto
end

local named = by_id[fixture.connections.speaker]
assert(named, "the named connection is listed")
assert(named.DeviceName == "Kitchen Speaker", `device name {named.DeviceName}`)
assert(named.Client == "Kitchen Speaker", `client {named.Client}`)
assert(named.DeviceId == "speaker")

local unnamed = by_id[fixture.connections.unnamed]
assert(unnamed, "the unnamed connection is listed")
assert(unnamed.DeviceName == "living-room", `device name {unnamed.DeviceName}`)
assert(unnamed.Client == "Lyra", `client {unnamed.Client}`)
