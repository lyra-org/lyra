local fixture = ...
local active_sessions = require("../../protocol/active_sessions")
local preferences = require("../../protocol/preferences")
local session = require("../../protocol/session")

local function context(token: string?, query, json)
	return {
		plugin_id = "jellyfin",
		request = {
			method = if json then "POST" else "GET",
			path = "/",
			headers = if token then { authorization = `MediaBrowser Token="{token}"` } else {},
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

-- A session shows its device's custom name, as upstream, and otherwise the name the client sent.
local principal: any = { user_id = fixture.users.admin, username = "admin" }
local identity: any = { client = "Finamp", device = "Pixel", device_id = "Zmlu/YW1w+==", version = "1" }
local function session_info(): any
	return session.build_session_info("session", principal, identity, "2026-01-01T00:00:00Z")
end

local unnamed = session_info()
assert(unnamed.DeviceName == "Pixel" and unnamed.HasCustomDeviceName == false)
assert(preferences.device_options_set(context(nil, phone, { CustomName = "" })).status == 204)
local blank = session_info()
assert(blank.DeviceName == "Pixel" and blank.HasCustomDeviceName == false, "an empty name is no custom name")
assert(preferences.device_options_set(context(nil, phone, { CustomName = " \t" })).status == 204)
local spaces = session_info()
assert(spaces.DeviceName == "Pixel" and spaces.HasCustomDeviceName == false, "nor is a whitespace-only one")
assert(preferences.device_custom_name("Zmlu/YW1w+==") == " \t", "the device's options keep it as sent")
assert(preferences.session_device_name("Zmlu/YW1w+==") == nil)
assert(preferences.device_options_set(context(nil, phone, { CustomName = "Kitchen" })).status == 204)
local named = session_info()
assert(named.DeviceName == "Kitchen" and named.HasCustomDeviceName == true)
assert(named.DeviceId == "Zmlu/YW1w+==", "the id stays the client's")

-- A live session takes the new name, and the client's own again once the name is cleared.
local live: any = { device_id = "Zmlu/YW1w+==", client_device_name = "Pixel", info = unnamed }
local other: any = { device_id = "other", info = { DeviceName = "Other", HasCustomDeviceName = false } }
active_sessions.entries.live = live
active_sessions.entries.other = other
active_sessions.apply_device_name("Zmlu/YW1w+==", "Kitchen")
assert(live.info.DeviceName == "Kitchen" and live.info.HasCustomDeviceName == true)
assert(other.info.DeviceName == "Other" and other.info.HasCustomDeviceName == false, "another device is untouched")
active_sessions.apply_device_name("Zmlu/YW1w+==", nil)
assert(live.info.DeviceName == "Pixel" and live.info.HasCustomDeviceName == false)
active_sessions.entries.live = nil
active_sessions.entries.other = nil
