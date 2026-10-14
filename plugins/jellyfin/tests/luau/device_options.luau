local fixture = ...
local preferences = require("../../protocol/preferences")
local session = require("../../protocol/session")

local function context(token: string?, query, json)
	return {
		plugin_id = "jellyfin",
		request = {
			method = if json then "POST" else "GET",
			path = "/",
			headers = if token then { authorization = `Bearer {token}` } else {},
			query = query or {},
			body_raw = if json then "{}" else "",
			json = json,
		},
		params = {},
	} :: any
end

-- Device options are an administrator's, as every upstream device route.
assert((session.require_admin(context(nil)) :: any).status == 401)
assert((session.require_admin(context(fixture.api_keys.listener_key)) :: any).status == 403)
assert(session.require_admin(context(fixture.api_keys.admin_key)) == nil)

local phone = { id = { "Zmlu/YW1w+==" } }

-- A device without options is not found until a custom name is set.
assert(preferences.device_options_get(context(nil, phone)).status == 404)
assert(preferences.device_options_get(context(nil)).status == 400)
assert(preferences.device_options_set(context(nil, nil, { CustomName = "Kitchen" })).status == 400)
assert(preferences.device_options_set(context(nil, phone, { CustomName = 5 })).status == 400)

assert(preferences.device_options_set(context(nil, phone, { CustomName = "Kitchen", Id = 9 })).status == 204)
local stored = preferences.device_options_get(context(nil, { Id = { "Zmlu/YW1w+==" } }))
assert(stored.status == 200)
local body = stored.body :: any
assert(body.DeviceId == "Zmlu/YW1w+==" and body.CustomName == "Kitchen" and body.Id == nil)
assert(preferences.device_custom_name("Zmlu/YW1w+==") == "Kitchen")

-- Clearing the name keeps the device's options.
assert(preferences.device_options_set(context(nil, phone, {})).status == 204)
local cleared = preferences.device_options_get(context(nil, phone))
assert(cleared.status == 200 and (cleared.body :: any).CustomName == nil)
assert(preferences.device_custom_name("Zmlu/YW1w+==") == nil)
