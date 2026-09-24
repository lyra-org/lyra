--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local ids = require("@lyra/ids")
local playback_sessions = require("@lyra/playback_sessions")
local tracks = require("@lyra/tracks")
local users = require("@lyra/users")

local constants = require("../constants")
local item_ids = require("./items/identifiers")
local playback = require("./items/playback")
local session = require("./session")
local websocket = require("./websocket")

local SERVER_ID = constants.SERVER_ID

type JsonValue = api.JsonValue
type JsonObject = { [string]: JsonValue }
type ClientPlayback = playback.ClientPlayback
type Principal = session.Principal
type PublicUser = users.PublicUser

type UserIdentity = {
	user_id: string,
	username: string,
}

export type SessionEntry = {
	sender: api.ApiWebSocketSender,
	session_id: string,
	user_id: string,
	-- Kept current by the socket's re-auth loop, alongside its dispatch principal.
	principal: Principal,
	device_id: string,
	info: JsonObject,
	capabilities: JsonObject?,
	-- Keys set by the most recent /Capabilities/Full. Non-Full merges must
	-- skip these so Full stays authoritative and removal-via-Full works.
	full_capability_keys: { [string]: boolean },
	subscribed_sessions: boolean,
	-- os.clock() of the last Sessions push.
	last_sessions_push_s: number?,
	last_activity: string,
	last_playback_event_s: number?,
}

-- Per-listing state: every source's client playback, and the NowPlayingItem tables whose track
-- names are filled in one batched lookup at the end.
type Listing = {
	client_playbacks: { [string]: ClientPlayback },
	now_playing_tracks: { [JsonObject]: string },
}

local module = {}

-- Live Jellyfin sockets keyed by session id.
local entries: { [string]: SessionEntry } = {}
module.entries = entries

local JELLYFIN_COMMAND_MAP: { [string]: string } = {
	play = "Play",
	pause = "Pause",
	unpause = "Unpause",
	stop = "Stop",
	seek = "Seek",
	next_track = "NextTrack",
	previous_track = "PreviousTrack",
	set_volume = "SetVolume",
}

local function to_jellyfin_supported_commands(commands: { string }): { string }
	local result = {}
	for _, cmd in commands do
		local mapped = JELLYFIN_COMMAND_MAP[cmd]
		if mapped then
			table.insert(result, mapped)
		end
	end
	return result
end

type Track = tracks.Track

-- Names come from tracks whose public id still matches, so a recycled `DbId` names nothing.
local function resolve_now_playing_names(listing: Listing)
	local public_ids: { string } = {}
	local seen: { [string]: boolean } = {}
	for _, track_public_id in listing.now_playing_tracks do
		if not seen[track_public_id] then
			seen[track_public_id] = true
			table.insert(public_ids, track_public_id)
		end
	end
	if #public_ids == 0 then
		return
	end
	local names: { [string]: string } = {}
	local ids_ok, db_ids = pcall(ids.get_db_ids, public_ids)
	if ids_ok and db_ids then
		local track_db_ids: { number } = {}
		for _, track_public_id in public_ids do
			local db_id = db_ids[track_public_id]
			if db_id then
				table.insert(track_db_ids, db_id)
			end
		end
		local ok, found = pcall(tracks.get_by_ids, track_db_ids)
		if ok and found then
			for _, track: Track? in found do
				if track then
					names[track.id] = track.track_title
				end
			end
		end
	end
	for now_playing, track_public_id in listing.now_playing_tracks do
		now_playing.Name = names[track_public_id] or ""
	end
end

local function build_now_playing(
	listing: Listing,
	track_public_id: string?,
	position_ms: number?,
	duration_ms: number?,
	is_paused: boolean
): (JsonObject, JsonObject)
	local now_playing: JsonObject = {
		Id = item_ids.encode_jellyfin_item_id(track_public_id),
		Name = "",
		Type = "Audio",
	}
	if track_public_id then
		listing.now_playing_tracks[now_playing] = track_public_id
	end
	if duration_ms then
		now_playing.RunTimeTicks = duration_ms * 10000
	end
	local play_state: JsonObject = {
		PositionTicks = (position_ms or 0) * 10000,
		IsPaused = is_paused,
		CanSeek = true,
	}
	return now_playing, play_state
end

local function native_now_playing(listing: Listing, info: playback_sessions.PlaybackInfo): (JsonObject, JsonObject)
	return build_now_playing(listing, info.track_public_id, info.position_ms, info.duration_ms, info.state == "paused")
end

local function client_now_playing(listing: Listing, client: ClientPlayback): (JsonObject, JsonObject)
	return build_now_playing(listing, client.track_public_id, client.position_ms, client.duration_ms, client.is_paused)
end

local function build_sessions_dto_list(
	user_id: string,
	username: string,
	connections: { playback_sessions.ConnectionInfo },
	listing: Listing
): { JsonValue }
	local conn_by_session_key: { [string]: playback_sessions.ConnectionInfo } = {}
	for _, conn in connections do
		conn_by_session_key[conn.session_key] = conn
	end
	local own_playbacks: { [string]: ClientPlayback } = {}
	for session_key, client in listing.client_playbacks do
		if client.user_id == user_id then
			own_playbacks[session_key] = client
		end
	end

	local jellyfin_user_id = item_ids.encode_jellyfin_item_id(user_id)
	local result: { JsonValue } = {}
	local seen_session_keys: { [string]: boolean } = {}

	for _, entry in entries do
		-- Skip foreign users and zombies (closed sender pre-cleanup).
		if entry.user_id ~= user_id or entry.sender:is_closed() then
			continue
		end
		-- Native clients opt into pairing by sending this Jellyfin session ID as their opaque session_key.
		local join_key = entry.session_id
		seen_session_keys[join_key] = true
		local dto: JsonObject = table.clone(entry.info)
		dto.LastActivityDate = entry.last_activity
		dto.LastPlaybackCheckIn = entry.last_activity
		if entry.capabilities then
			dto.Capabilities = entry.capabilities
		end
		local conn = conn_by_session_key[join_key]
		local conn_playback = if conn then conn.playback else nil
		local client_playback = own_playbacks[join_key]
		-- Native playback carries no report time to compare, so a paired native client wins.
		if conn_playback then
			dto.NowPlayingItem, dto.PlayState = native_now_playing(listing, conn_playback)
		elseif client_playback then
			dto.NowPlayingItem, dto.PlayState = client_now_playing(listing, client_playback)
		end
		table.insert(result, dto)
	end

	-- Clients that report playback over HTTP without a socket can't take remote commands. A socket
	-- reporter without a live socket is gone. A native connection paired with the session lists
	-- once, below, under its own controllable id.
	local paired_playbacks: { [string]: ClientPlayback } = {}
	for session_key, client in own_playbacks do
		if seen_session_keys[session_key] or client.via_socket then
			continue
		end
		seen_session_keys[session_key] = true
		if conn_by_session_key[session_key] then
			paired_playbacks[session_key] = client
			continue
		end
		local dto: JsonObject = table.clone(client.session_info)
		dto.SupportsRemoteControl = false
		dto.NowPlayingItem, dto.PlayState = client_now_playing(listing, client)
		table.insert(result, dto)
	end

	-- Non-WS clients (playback-only) still need to surface here.
	for _, conn in connections do
		if seen_session_keys[conn.session_key] and not paired_playbacks[conn.session_key] then
			continue
		end
		local playable_types: { JsonValue } = { "Audio" }
		local legacy: JsonObject = {
			Id = conn.token,
			UserId = jellyfin_user_id,
			UserName = username,
			DeviceId = conn.session_key,
			DeviceName = conn.session_key,
			Client = "Lyra",
			ServerId = SERVER_ID,
			IsActive = true,
			SupportsRemoteControl = #conn.supported_commands > 0,
			PlayableMediaTypes = playable_types,
		}
		local conn_playback = conn.playback
		local paired_playback = paired_playbacks[conn.session_key]
		if conn_playback then
			legacy.NowPlayingItem, legacy.PlayState = native_now_playing(listing, conn_playback)
		elseif paired_playback then
			legacy.NowPlayingItem, legacy.PlayState = client_now_playing(listing, paired_playback)
		end
		local supported_commands = to_jellyfin_supported_commands(conn.supported_commands)
		local cmd_list: { JsonValue } = {}
		for _, cmd in supported_commands do
			table.insert(cmd_list, cmd)
		end
		legacy.Capabilities = {
			SupportsMediaControl = #supported_commands > 0,
			SupportedCommands = cmd_list,
			PlayableMediaTypes = playable_types,
		}
		table.insert(result, legacy)
	end

	return result
end

local function list_all_user_identities(fallback: UserIdentity): { UserIdentity }
	local result: { UserIdentity } = {}
	local seen: { [string]: boolean } = {}
	for _, user: PublicUser in users.list() do
		if not seen[user.user_id] then
			seen[user.user_id] = true
			table.insert(result, {
				user_id = user.user_id,
				username = user.username,
			})
		end
	end
	if not seen[fallback.user_id] then
		table.insert(result, fallback)
	end
	return result
end

local function session_supports_remote_control(dto: JsonValue): boolean
	if type(dto) ~= "table" then
		return false
	end
	return (dto :: JsonObject).SupportsRemoteControl == true
end

local function append_sessions_for_user(
	result: { JsonValue },
	user_id: string,
	username: string,
	connections: { playback_sessions.ConnectionInfo },
	listing: Listing,
	require_remote_control: boolean
)
	for _, dto in build_sessions_dto_list(user_id, username, connections, listing) do
		if not require_remote_control or session_supports_remote_control(dto) then
			table.insert(result, dto)
		end
	end
end

-- The server lists only the dispatch principal's own native connections, so every other user's
-- sessions come from this plugin's WebSocket registry alone.
local function list_all_sessions(
	own: UserIdentity,
	own_connections: { playback_sessions.ConnectionInfo },
	listing: Listing,
	require_remote_control: boolean
): { JsonValue }
	local result: { JsonValue } = {}
	for _, user in list_all_user_identities(own) do
		local connections = if user.user_id == own.user_id then own_connections else {}
		append_sessions_for_user(result, user.user_id, user.username, connections, listing, require_remote_control)
	end
	return result
end

-- The one listing behind both GET /Sessions and the socket push. The host scopes native connections
-- to the dispatch principal and verifies it first, so a deleted user's listing fails.
function module.list_visible(principal: Principal, controllable_user_id: string?): { JsonValue }
	if controllable_user_id and not session.can_control_user(principal, controllable_user_id) then
		return {}
	end

	local require_remote_control = controllable_user_id ~= nil
	local own_connections = playback_sessions.list_connections()
	local listing: Listing = {
		client_playbacks = playback.list_client_playbacks(),
		now_playing_tracks = {},
	}
	local result: { JsonValue }
	if session.is_admin(principal) then
		result = list_all_sessions({
			user_id = principal.user_id,
			username = principal.username,
		}, own_connections, listing, require_remote_control)
	else
		result = {}
		append_sessions_for_user(
			result,
			principal.user_id,
			principal.username,
			own_connections,
			listing,
			require_remote_control
		)
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
		local message = tostring(err)
		if string.find(message, "invalid bearer credential", 1, true) then
			entry.sender:close()
			return
		end
		warn(`sessions push failed (user_id={entry.user_id}): {message}`)
	end)
end

return module
