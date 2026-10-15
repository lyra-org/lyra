local fixture = ...
local auth = require("@lyra/auth")

local item_ids = require("../../protocol/items/identifiers")
local preferences = require("../../protocol/preferences")
local session = require("../../protocol/session")

local function context(token: string?, query)
	return {
		plugin_id = "jellyfin",
		request = {
			method = "GET",
			path = "/",
			headers = if token then { authorization = `Bearer {token}` } else {},
			query = query or {},
			body_raw = "",
			json = nil,
		},
		params = {},
	}
end

local function remember(key: string, client: string, device: string, device_id: string, version: string)
	auth.set_session_data(fixture.sessions[key], {
		client = client,
		device = device,
		device_id = device_id,
		version = version,
	})
end

remember("listener_phone", "Finamp", "Pixel", "phone", "0.9.1")
remember("admin_web", "Jellyfin Web", "Firefox", "web", "10.11.0")
remember("listener_tablet", "Finamp", "Tab", "tablet", "0.9.0")
remember("admin_phone", "Finamp", "Pixel", "phone", "0.8.0")

local listener_id = fixture.users.listener
-- Options belong to the device id, whoever logged in with it.
local saved = preferences.device_options_set({
	plugin_id = "jellyfin",
	request = {
		method = "POST",
		path = "/",
		headers = {},
		query = { id = { "phone" } },
		body_raw = '{"CustomName":"Kitchen"}',
		json = { CustomName = "Kitchen" },
	},
	params = {},
} :: any)
assert(saved.status == 204)

local admin_key = fixture.api_keys.admin_key
local listener_key = fixture.api_keys.listener_key

-- Devices are an administrator's view.
assert(session.devices_response(context(nil) :: any).status == 401)
assert(session.devices_response(context(listener_key) :: any).status == 403)
assert(session.device_info_response(context(listener_key, { id = { "phone" } }) :: any).status == 403)

-- One device per Jellyfin login across users, most recently active first.
local listed = session.devices_response(context(admin_key) :: any)
assert(listed.kind == "json")
local body = listed.body :: any
assert(body.TotalRecordCount == 4 and body.StartIndex == 0 and #body.Items == 4)
local order = {}
for _, device in body.Items do
	table.insert(order, `{device.Id}@{device.LastUserName}`)
end
assert(table.concat(order, ",") == "phone@listener,web@admin,tablet@listener,phone@admin", table.concat(order, ","))

local phone = body.Items[1]
assert(phone.Name == "Pixel" and phone.AppName == "Finamp" and phone.AppVersion == "0.9.1")
assert(phone.LastUserId == item_ids.encode_jellyfin_item_id(listener_id))
assert(phone.DateLastActivity == "1970-01-01T00:05:00Z")
assert(phone.CustomName == "Kitchen")
assert(body.Items[2].CustomName == nil and body.Items[3].CustomName == nil)
assert(body.Items[4].CustomName == "Kitchen")

-- Info is the newest login of a device.
local info = session.device_info_response(context(admin_key, { id = { "phone" } }) :: any)
assert(info.kind == "json")
local device = info.body :: any
assert(device.Id == "phone" and device.LastUserName == "listener" and device.AppVersion == "0.9.1")
assert(session.device_info_response(context(admin_key, { Id = { "tablet" } }) :: any).status == 200)
assert(session.device_info_response(context(admin_key, { id = { "missing" } }) :: any).status == 404)
assert(session.device_info_response(context(admin_key) :: any).status == 400)

-- Deleting a device logs out each of its logins, for every user; an unknown id deletes nothing.
local function delete(token: string?, query)
	local ctx = context(token, query) :: any
	ctx.request.method = "DELETE"
	return session.delete_devices_response(ctx)
end
local phone_logins = { fixture.sessions.listener_phone, fixture.sessions.admin_phone }
assert(delete(listener_key, { id = { "phone" } }).status == 403)
assert(delete(admin_key, { id = { "phone", "missing" } }).status == 400)
assert(delete(admin_key, { id = { "lyra" } }).status == 400)
for _, token in phone_logins do
	assert(auth.resolve_auth(token) ~= nil)
end
assert(delete(admin_key).status == 204)

assert(delete(admin_key, { Id = { "phone" } }).status == 204)
for _, token in phone_logins do
	assert(auth.resolve_auth(token) == nil)
end
assert(auth.resolve_auth(fixture.sessions.listener_tablet) ~= nil)
assert(auth.resolve_auth(fixture.sessions.listener_lyra) ~= nil)
local remaining = (session.devices_response(context(admin_key) :: any).body :: any).Items
local remaining_ids = {}
for _, entry in remaining do
	remaining_ids[entry.Id] = true
end
assert(#remaining == 2 and remaining_ids.web and remaining_ids.tablet)
