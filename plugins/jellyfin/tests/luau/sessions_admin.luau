local fixture = ...
local active_sessions = require("../../protocol/active_sessions")
local item_ids = require("../../protocol/items/identifiers")
local session = require("../../protocol/session")

-- As upstream, an admin sees every user's sessions and anyone else their own: here a player
-- reporting over HTTP and connections of Lyra's own clients.

local function request(api_key: string, json: any?): any
	return {
		plugin_id = "jellyfin",
		request = {
			method = if json then "POST" else "GET",
			path = if json then "/Sessions/Playing" else "/Sessions",
			headers = {
				authorization = `MediaBrowser Client="Finamp", Device="Pixel 8", DeviceId="phone", Version="0.9.1", Token="{api_key}"`,
			},
			query = {},
			body_raw = "",
			json = json,
		},
		params = {},
	}
end

local body = { ItemId = item_ids.encode_jellyfin_item_id(fixture.ids.song), PositionTicks = 0 }
local reported = session.handle_playback_report_route(request(fixture.api_keys.phone, body), "started")
assert(reported.status == 204, `the listener's report is accepted, got {reported.status}`)

local function sessions_of(api_key: string, controllable_user_id: string?): { [string]: any }
	local principal = session.require_jellyfin_principal(request(api_key))
	assert(principal, "the api key authenticates")
	local by_device: { [string]: any } = {}
	for _, dto in active_sessions.list_visible(principal, controllable_user_id) do
		by_device[(dto :: any).DeviceId] = dto
	end
	return by_device
end

local listener_id = item_ids.encode_jellyfin_item_id(fixture.users.listener)
local admin_view = sessions_of(fixture.api_keys.admin_key)
local phone = admin_view.phone
assert(phone and phone.UserId == listener_id, "the admin sees the listener's player")
assert(phone.NowPlayingItem.Id == item_ids.encode_jellyfin_item_id(fixture.ids.song))
assert(phone.NowPlayingItem.Name == "Song")
local speaker = admin_view.speaker
assert(speaker and speaker.Id == fixture.connections.speaker and speaker.UserId == listener_id)
assert(speaker.DeviceName == "Kitchen Speaker" and speaker.SupportsRemoteControl == true)
assert(admin_view.desk, "and their own connection")

local listener_view = sessions_of(fixture.api_keys.phone)
assert(listener_view.phone and listener_view.speaker, "the listener sees their own sessions")
assert(listener_view.desk == nil, "and not the admin's")

local controllable = sessions_of(fixture.api_keys.admin_key, fixture.users.listener)
assert(controllable.speaker and controllable.phone == nil, "only sessions taking commands are controllable")
