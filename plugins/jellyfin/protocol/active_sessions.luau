--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local datastore = require("@lyra/datastore")
local errors = require("@lyra/errors")
local playback_sessions = require("@lyra/playback_sessions")
local tracks = require("@lyra/tracks")

local constants = require("../constants")
local item_ids = require("./items/identifiers")
local playback = require("./items/playback")
local session = require("./session")
local websocket = require("./websocket")

local JELLYFIN_PLUGIN_ID = constants.JELLYFIN_PLUGIN_ID
local SERVER_ID = constants.SERVER_ID

type JsonValue = api.JsonValue
type JsonObject = { [string]: JsonValue }
type ActivePlayback = playback_sessions.ActivePlaybackInfo
type Connection = playback_sessions.ConnectionInfo
type Principal = session.Principal

export type SessionEntry = {
	sender: api.ApiWebSocketSender,
	session_id: string,
	user_id: string,
	-- Kept current by the socket's re-auth loop, alongside its dispatch principal.
	principal: Principal,
	device_id: string,
	info: JsonObject,
	capabilities: JsonObject?,
	subscribed_sessions: boolean,
	-- os.clock() of the last Sessions push.
	last_sessions_push_s: number?,
	last_activity: string,
	last_playback_event_s: number?,
	-- os.clock() of the socket's last KeepAlive.
	last_keep_alive_s: number,
	-- Rises with every socket; the lowest of a user's is their oldest.
	connection_order: number,
	-- Closed by the cap. Its sender reads as open until the host acts on the close.
	evicted: boolean,
}

-- Per-listing state: the NowPlayingItem tables whose track names are filled in one batched lookup
-- at the end.
type Listing = {
	now_playing_tracks: { [JsonObject]: string },
}

local module = {}

-- Live Jellyfin sockets keyed by session id.
local entries: { [string]: SessionEntry } = {}
module.entries = entries

-- Goes beyond upstream: every plugin shares one Luau VM, so one account can't hold unlimited sockets.
local MAX_SOCKETS_PER_USER = 32
module.MAX_SOCKETS_PER_USER = MAX_SOCKETS_PER_USER

local last_connection_order = 0

function module.next_connection_order(): number
	last_connection_order += 1
	return last_connection_order
end

-- Returns nil on miss, scope mismatch, or closed sender. Mismatch is logged so
-- a hash collision is distinguishable from the normal miss. A closed entry
-- stays for its socket handler to release.
function module.get_owned(session_id: string?, principal: Principal): SessionEntry?
	if not session_id then
		return nil
	end
	local entry = entries[session_id]
	if not entry then
		return nil
	end
	if entry.user_id ~= principal.user_id then
		warn(`session user_id mismatch: principal={principal.user_id}, entry={entry.user_id}, session_id={session_id}`)
		return nil
	end
	if entry.sender:is_closed() then
		return nil
	end
	return entry
end

-- Upstream keeps a device's capabilities across its sessions, and a client declares before opening
-- its socket. Without the declaration a session is no cast target.
local CAPABILITIES_STORE_NAME = "session_capabilities"
local CAPABILITIES_TTL_MS = 30 * 24 * 60 * 60 * 1000

local capabilities_store: datastore.DataStore? = nil

local function get_capabilities_store(): datastore.DataStore
	if capabilities_store == nil then
		capabilities_store = datastore.get_or_create(CAPABILITIES_STORE_NAME)
	end
	return capabilities_store :: datastore.DataStore
end

-- Replaces what the session declared, on its open socket and for any socket it opens later.
function module.declare_capabilities(session_id: string, principal: Principal, capabilities: JsonObject)
	local function apply()
		local entry = module.get_owned(session_id, principal)
		if entry then
			entry.capabilities = capabilities
		end
	end
	apply()
	get_capabilities_store():set(
		session_id,
		{ user_id = principal.user_id, capabilities = capabilities },
		{ ttl_ms = CAPABILITIES_TTL_MS }
	)
	-- The write yields, and a socket that reconnected meanwhile may have restored the older one.
	apply()
end

-- Gives an admitted socket its session's last declaration. Runs after admission so one arriving
-- during the read, which reaches the socket directly, isn't overwritten by an older one.
function module.restore_capabilities(entry: SessionEntry)
	local ok, stored = pcall(function()
		return get_capabilities_store():get(entry.session_id)
	end)
	if not ok then
		warn(`could not read declared capabilities (user_id={entry.user_id}): {tostring(stored)}`)
		return
	end
	if entry.capabilities ~= nil or type(stored) ~= "table" then
		return
	end
	local declaration = stored :: { [string]: JsonValue }
	if declaration.user_id == entry.user_id and type(declaration.capabilities) == "table" then
		entry.capabilities = declaration.capabilities :: JsonObject
	end
end

local function close_socket(entry: SessionEntry)
	pcall(function()
		entry.sender:close()
	end)
end

-- Makes `entry` its session's socket, closing the one it replaces and, over the cap, the user's
-- oldest. Evicted sockets end through their own handlers. Nothing here may yield: concurrent
-- connects would each pass the count.
function module.admit(entry: SessionEntry)
	local replaced = entries[entry.session_id]
	local live: { SessionEntry } = {}
	for _, other in entries do
		if
			other ~= replaced
			and other.user_id == entry.user_id
			and not other.evicted
			and not other.sender:is_closed()
		then
			table.insert(live, other)
		end
	end

	local excess = #live + 1 - MAX_SOCKETS_PER_USER
	if excess > 0 then
		table.sort(live, function(left: SessionEntry, right: SessionEntry): boolean
			return left.connection_order < right.connection_order
		end)
		for index = 1, excess do
			live[index].evicted = true
			close_socket(live[index])
		end
	end

	if replaced then
		close_socket(replaced)
	end
	entries[entry.session_id] = entry
end

local JELLYFIN_COMMAND_MAP: { [playback_sessions.RemoteAction]: string } = {
	play = "PlayState",
	play_tracks = "Play",
	pause = "PlayState",
	unpause = "PlayState",
	stop = "PlayState",
	seek = "PlayState",
	next_track = "PlayState",
	previous_track = "PlayState",
	set_volume = "SetVolume",
}

local PLAYABLE_MEDIA_TYPES: { JsonValue } = { "Audio" }

local function to_jellyfin_supported_commands(commands: { playback_sessions.RemoteAction }): { string }
	local result = {}
	local seen: { [string]: boolean } = {}
	for _, cmd in commands do
		local mapped = JELLYFIN_COMMAND_MAP[cmd]
		if mapped and not seen[mapped] then
			seen[mapped] = true
			table.insert(result, mapped)
		end
	end
	return result
end

type Track = tracks.Track

local function resolve_now_playing_names(listing: Listing)
	local track_ids: { string } = {}
	local seen: { [string]: boolean } = {}
	for _, track_id in listing.now_playing_tracks do
		if not seen[track_id] then
			seen[track_id] = true
			table.insert(track_ids, track_id)
		end
	end
	if #track_ids == 0 then
		return
	end
	local names: { [string]: string } = {}
	local ok, found = pcall(tracks.get, track_ids)
	if ok and found then
		for _, track: Track? in found do
			if track then
				names[track.id] = track.track_title
			end
		end
	end
	for now_playing, track_id in listing.now_playing_tracks do
		now_playing.Name = names[track_id] or ""
	end
end

local function set_now_playing(listing: Listing, dto: JsonObject, playing: ActivePlayback)
	local now_playing: JsonObject = {
		Id = item_ids.encode(playing.track_id),
		Name = "",
		Type = "Audio",
	}
	listing.now_playing_tracks[now_playing] = playing.track_id
	if playing.duration_ms then
		now_playing.RunTimeTicks = playing.duration_ms * 10000
	end
	dto.NowPlayingItem = now_playing
	dto.PlayState = {
		PositionTicks = playing.effective_position_ms * 10000,
		IsPaused = playing.state == "paused",
		IsMuted = false,
		CanSeek = true,
		RepeatMode = "RepeatNone",
		PlaybackOrder = "Default",
	}
end

-- The playback reported last, of two that may report for one device.
local function latest(left: ActivePlayback?, right: ActivePlayback?): ActivePlayback?
	if left and right then
		return if right.updated_at_ms > left.updated_at_ms then right else left
	end
	return left or right
end

local function device_key(user_id: string, session_key: string): string
	return `{#user_id}:{user_id}{session_key}`
end

local function set_activity(dto: JsonObject, activity_iso: string)
	dto.LastActivityDate = activity_iso
	dto.LastPlaybackCheckIn = activity_iso
end

-- A session for a connection of Lyra's own clients, which takes the commands it declares from its
-- own user alone.
local function connection_dto(conn: Connection, viewer: Principal): JsonObject
	local supported_commands = to_jellyfin_supported_commands(conn.supported_commands)
	-- A cast target must take Play, which a connection can only with `play_tracks`.
	local takes_play = false
	for _, command in conn.supported_commands do
		takes_play = takes_play or command == "play_tracks"
	end
	local cmd_list: { JsonValue } = {}
	for _, cmd in supported_commands do
		table.insert(cmd_list, cmd)
	end
	local controllable = takes_play and conn.user_id == viewer.user_id
	local capabilities: JsonObject = {
		SupportsMediaControl = takes_play,
		SupportedCommands = cmd_list,
		PlayableMediaTypes = PLAYABLE_MEDIA_TYPES,
	}
	local dto: JsonObject = {
		Id = conn.id,
		UserId = item_ids.encode(conn.user_id),
		UserName = conn.username,
		DeviceId = conn.session_key,
		DeviceName = conn.client_name or conn.session_key,
		Client = conn.client_name or "Lyra",
		ServerId = SERVER_ID,
		IsActive = true,
		SupportsRemoteControl = controllable,
		SupportsMediaControl = controllable,
		HasCustomDeviceName = false,
		PlayableMediaTypes = PLAYABLE_MEDIA_TYPES,
		SupportedCommands = cmd_list,
		Capabilities = capabilities,
		PlayState = session.idle_play_state(),
	}
	set_activity(dto, session.iso_timestamp_ms(conn.connected_at_ms))
	return dto
end

-- A session for a player that reports its playback without a socket, which takes no commands.
local function playback_dto(playing: ActivePlayback): JsonObject
	local reporter = if playing.plugin_id == JELLYFIN_PLUGIN_ID and playing.session_key
		then playback.reporter(playing.session_key)
		else nil
	local dto: JsonObject
	if reporter then
		dto = table.clone(reporter.session_info)
	else
		local device = playing.session_key or playing.id
		dto = {
			Id = device,
			UserId = item_ids.encode(playing.user_id),
			UserName = playing.username,
			DeviceId = device,
			DeviceName = playing.client_name or device,
			Client = playing.client_name or "Lyra",
			ServerId = SERVER_ID,
			IsActive = true,
			HasCustomDeviceName = false,
			PlayableMediaTypes = PLAYABLE_MEDIA_TYPES,
			SupportedCommands = {},
		}
	end
	dto.SupportsRemoteControl = false
	dto.SupportsMediaControl = false
	set_activity(dto, session.iso_timestamp_ms(playing.updated_at_ms))
	return dto
end

-- Whether the Jellyfin session reporting this plugin's playback closed its socket since: as
-- upstream, a session whose socket closed is gone.
local function reporter_ended(playing: ActivePlayback): boolean
	local reporter = if playing.session_key then playback.reporter(playing.session_key) else nil
	return reporter ~= nil and reporter.ended
end

-- Every session the principal can see: this plugin's sockets, the players that report playback to
-- it over HTTP alone, connections of Lyra's own clients, and what Lyra's own clients and other
-- plugins play without either.
local function list_sessions(principal: Principal, all_users: boolean, listing: Listing): { JsonObject }
	local query: playback_sessions.OwnersQuery? = if all_users then { all_users = true } else nil
	local connections = playback_sessions.list_connections(query)
	local active = if all_users
		then playback_sessions.list_user_playbacks({ all_users = true })
		else playback_sessions.list_playbacks()

	local connection_by_device: { [string]: Connection } = {}
	for _, conn in connections do
		connection_by_device[device_key(conn.user_id, conn.session_key)] = conn
	end
	-- This plugin's playback by device, and Lyra's own clients' by the connection driving it.
	local reported_by_device: { [string]: ActivePlayback } = {}
	local driven_by_connection: { [string]: ActivePlayback } = {}
	for _, playing in active do
		if playing.plugin_id == JELLYFIN_PLUGIN_ID and playing.session_key then
			reported_by_device[device_key(playing.user_id, playing.session_key)] = playing
		elseif playing.connection_id then
			driven_by_connection[playing.connection_id] = playing
		end
	end
	local function paired_report(device: string): ActivePlayback?
		local playing = reported_by_device[device]
		return if playing and not reporter_ended(playing) then playing else nil
	end

	local result: { JsonObject } = {}
	local listed: { [string]: boolean } = {}

	-- Native clients opt into pairing by sending a Jellyfin session ID as their session key.
	for _, entry in entries do
		-- Skip foreign users and zombies (closed sender pre-cleanup).
		if entry.sender:is_closed() or (not all_users and entry.user_id ~= principal.user_id) then
			continue
		end
		local device = device_key(entry.user_id, entry.session_id)
		listed[device] = true
		local dto: JsonObject = table.clone(entry.info)
		set_activity(dto, entry.last_activity)
		local capabilities = entry.capabilities
		if capabilities then
			dto.Capabilities = capabilities
			dto.SupportsMediaControl = capabilities.SupportsMediaControl == true
			dto.SupportsRemoteControl = capabilities.SupportsMediaControl == true
			dto.SupportedCommands = capabilities.SupportedCommands or ({} :: { JsonValue })
			dto.PlayableMediaTypes = capabilities.PlayableMediaTypes or ({} :: { JsonValue })
		end
		local conn = connection_by_device[device]
		local playing = latest(reported_by_device[device], if conn then driven_by_connection[conn.id] else nil)
		if playing then
			set_now_playing(listing, dto, playing)
		end
		table.insert(result, dto)
	end

	-- A native connection paired with a player that reports over HTTP lists once, under the
	-- connection's controllable id.
	for _, conn in connections do
		local device = device_key(conn.user_id, conn.session_key)
		if listed[device] then
			continue
		end
		listed[device] = true
		local dto = connection_dto(conn, principal)
		local playing = latest(driven_by_connection[conn.id], paired_report(device))
		if playing then
			set_now_playing(listing, dto, playing)
			set_activity(dto, session.iso_timestamp_ms(math.max(conn.connected_at_ms, playing.updated_at_ms)))
		end
		table.insert(result, dto)
	end

	for _, playing in active do
		if playing.connection_id then
			continue
		end
		if playing.plugin_id == JELLYFIN_PLUGIN_ID and playing.session_key then
			local device = device_key(playing.user_id, playing.session_key)
			if listed[device] or reporter_ended(playing) then
				continue
			end
			listed[device] = true
		end
		local dto = playback_dto(playing)
		set_now_playing(listing, dto, playing)
		table.insert(result, dto)
	end

	return result
end

-- The one listing behind both GET /Sessions and the socket push. Admins see every user's sessions
-- and others their own, as upstream. The host verifies the dispatch principal first, so a deleted
-- user's listing fails. `device_id` keeps one device's sessions, in any case as upstream.
function module.list_visible(principal: Principal, controllable_user_id: string?, device_id: string?): { JsonValue }
	if controllable_user_id and not session.can_control_user(principal, controllable_user_id) then
		return {}
	end

	local device = if device_id then string.lower(device_id) else nil
	local listing: Listing = { now_playing_tracks = {} }
	local result: { JsonValue } = {}
	for _, dto in list_sessions(principal, session.is_admin(principal), listing) do
		local controllable = controllable_user_id == nil or dto.SupportsRemoteControl == true
		local on_device = device == nil or (type(dto.DeviceId) == "string" and string.lower(dto.DeviceId) == device)
		if controllable and on_device then
			table.insert(result, dto)
		end
	end
	resolve_now_playing_names(listing)
	return result
end

-- Must run in the socket's own dispatch so the host acts as the socket's principal. A principal
-- that no longer verifies closes the socket rather than failing every push.
function module.push(entry: SessionEntry)
	entry.last_sessions_push_s = os.clock()
	xpcall(function()
		local list = module.list_visible(entry.principal, nil)
		entry.sender:send(websocket.encode("Sessions", list, nil))
	end, function(err)
		if errors.kind(err) == "unauthenticated" then
			entry.sender:close()
			return
		end
		warn(`sessions push failed (user_id={entry.user_id}): {err}`)
	end)
end

return module
