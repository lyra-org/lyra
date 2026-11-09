local fixture = ...
local item_ids = require("../../protocol/items/identifiers")
local session = require("../../protocol/session")

-- Lyra names a Jellyfin client by its app and the device it names, at login and in playback
-- reports. The server tidies the name, so any device name reports.

local function context(authorization: string, path: string?, json: any?): any
	return {
		plugin_id = "jellyfin",
		request = {
			method = "POST",
			path = path or "/",
			headers = { authorization = authorization, ["user-agent"] = " Finamp/0.9.1 " },
			query = {},
			body_raw = "",
			json = json,
		},
		params = {},
	}
end

local function header(device: string?, token: string?): string
	local device_part = if device then `, Device="{device}"` else ""
	local token_part = if token then `, Token="{token}"` else ""
	return `MediaBrowser Client="Finamp"{device_part}, DeviceId="phone", Version="0.9.1"{token_part}`
end

local function label(authorization: string): string?
	local identity = session.resolve_client_identity(context(authorization))
	return if identity then identity.label else nil
end

local key = fixture.api_keys.phone
assert(
	label(header("Pixel 8", key)) == "phone · Pixel 8",
	`an api key names the app, got {label(header("Pixel 8", key))}`
)
-- An api key client that names no device lists under the server, as upstream, but isn't labelled by it.
assert(label(header(nil, key)) == "phone", `got {label(header(nil, key))}`)
assert(label(`MediaBrowser Client="Finamp", Device="Finamp", Token="{key}"`) == "phone · Finamp")

local user_agent, client_name = session.login_metadata(context(header("Pixel 8")))
assert(user_agent == "Finamp/0.9.1", `user agent {user_agent}`)
assert(client_name == "Finamp · Pixel 8", `login client name {client_name}`)
local _, same = session.login_metadata(context(`MediaBrowser Client="Finamp", Device="Finamp", DeviceId="phone"`))
assert(same == "Finamp", `a device named as its app is named once, got {same}`)
local _, deviceless = session.login_metadata(context(`MediaBrowser Client="Finamp", DeviceId="phone"`))
assert(deviceless == "Finamp", `got {deviceless}`)
local _, anonymous = session.login_metadata(context(`MediaBrowser DeviceId="phone"`))
assert(anonymous == nil, `got {anonymous}`)

local body = { ItemId = item_ids.encode(fixture.ids.song), PositionTicks = 0 }
for _, device in { "Pixel 8", string.rep("x", 129), "Pixel\n8", "Pixel\u{85}8" } do
	local response =
		session.handle_playback_report_route(context(header(device, key), "/Sessions/Playing", body), "started")
	assert(response.status == 204, `{device} reports, got {response.status}: {response.body and response.body.Message}`)
end
