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

local body = { ItemId = item_ids.encode(fixture.ids.song), PositionTicks = 0 }
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

local function active_controllable(api_key: string, user_id: string): { any }
	local list: { any } = {}
	for _, dto in sessions_of(api_key, user_id) do
		table.insert(list, dto)
	end
	return list
end

local listener_id = item_ids.encode(fixture.users.listener)
local admin_view = sessions_of(fixture.api_keys.admin_key)
local phone = admin_view.phone
assert(phone and phone.UserId == listener_id, "the admin sees the listener's player")
assert(phone.NowPlayingItem.Id == item_ids.encode(fixture.ids.song))
assert(phone.NowPlayingItem.Name == "Song")
local speaker = admin_view.speaker
assert(speaker and speaker.Id == fixture.connections.speaker and speaker.UserId == listener_id)
assert(speaker.DeviceName == "Kitchen Speaker")
assert(speaker.SupportsRemoteControl == false, "another user's connection can't be commanded")
assert(admin_view.desk, "and their own connection")

local listener_view = sessions_of(fixture.api_keys.phone)
assert(listener_view.phone and listener_view.speaker, "the listener sees their own sessions")
assert(listener_view.desk == nil, "and not the admin's")

assert(
	#active_controllable(fixture.api_keys.admin_key, fixture.users.listener) == 0,
	"nothing of the listener's for the admin"
)

local own = sessions_of(fixture.api_keys.phone, fixture.users.listener)
assert(own.speaker and own.speaker.SupportsRemoteControl == true, "the listener controls their own connection")
assert(own.speaker.SupportsMediaControl == true and own.speaker.Capabilities.SupportsMediaControl == true)
assert(own.phone == nil, "a session taking no commands isn't controllable")
assert(listener_view.knob.SupportsRemoteControl == false, "a connection that can't take Play is no cast target")
assert(listener_view.knob.Capabilities.SupportsMediaControl == false)
assert(own.knob == nil, "and isn't controllable")

-- A client parsing the list needs these on every session and every play state, or the whole list
-- fails.
for device, dto in sessions_of(fixture.api_keys.admin_key) do
	assert(dto.UserId ~= nil, `{device}: UserId`)
	assert(type(dto.LastActivityDate) == "string", `{device}: LastActivityDate`)
	assert(type(dto.SupportsMediaControl) == "boolean", `{device}: SupportsMediaControl`)
	assert(type(dto.SupportsRemoteControl) == "boolean", `{device}: SupportsRemoteControl`)
	assert(dto.IsActive == true, `{device}: IsActive`)
	assert(dto.HasCustomDeviceName == false, `{device}: HasCustomDeviceName`)
	assert(type(dto.PlayState.CanSeek) == "boolean", `{device}: CanSeek`)
	assert(type(dto.PlayState.IsPaused) == "boolean", `{device}: IsPaused`)
	assert(dto.PlayState.IsMuted == false, `{device}: IsMuted`)
	assert(dto.PlayState.RepeatMode == "RepeatNone", `{device}: RepeatMode`)
	assert(dto.PlayState.PlaybackOrder == "Default", `{device}: PlaybackOrder`)
	assert(type(dto.SupportedCommands) == "table", `{device}: SupportedCommands`)
	assert(type(dto.PlayableMediaTypes) == "table", `{device}: PlayableMediaTypes`)
	-- Only `GeneralCommandType` names are valid here; playstate commands are one of them.
	for _, command in dto.SupportedCommands do
		assert(command == "Play" or command == "PlayState" or command == "SetVolume", `{device}: {command}`)
	end
end
assert(admin_view.knob.PlayState.PositionTicks == nil, "an idle session's play state has no position")
assert(table.find(admin_view.knob.SupportedCommands, "PlayState"), "playstate commands are declared as one")
assert(not table.find(admin_view.knob.SupportedCommands, "Play"), "and aren't Play")
assert(admin_view["Web Player"] or admin_view[fixture.ids.web], "the playback with no connection is listed")
assert(speaker.SupportsMediaControl == false and table.find(speaker.SupportedCommands, "Play"))
assert(admin_view.knob.SupportsMediaControl == false)
assert(phone.SupportsMediaControl == false and phone.PlayState ~= nil)
