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
local session = require("./session")
local websocket = require("./websocket")

local SERVER_ID = constants.SERVER_ID

type JsonValue = api.JsonValue
type JsonObject = { [string]: JsonValue }
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
	last_activity: string,
	last_playback_event_s: number?,
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

local function resolve_track_name(track_id: number): string
	local ok, result = pcall(tracks.get_by_ids, { track_id })
	if not ok or not result then
		return ""
	end
	local track: Track? = result[track_id]
	if track then
		return track.track_title
	end
	return ""
end

local function build_now_playing(playback: playback_sessions.PlaybackInfo): (JsonObject, JsonObject)
	local track_db_id = ids.get_db_id(playback.track_public_id)
	local now_playing: JsonObject = {
		Id = item_ids.encode_jellyfin_item_id(playback.track_public_id),
		Name = if track_db_id then resolve_track_name(track_db_id) else "",
		Type = "Audio",
	}
	if playback.duration_ms then
		now_playing.RunTimeTicks = playback.duration_ms * 10000
	end
	local play_state: JsonObject = {
		PositionTicks = playback.position_ms * 10000,
		IsPaused = playback.state == "paused",
		CanSeek = true,
	}
	return now_playing, play_state
end

local function build_sessions_dto_list(
	user_id: string,
	username: string,
	connections: { playback_sessions.ConnectionInfo }
): { JsonValue }
	local conn_by_session_key: { [string]: playback_sessions.ConnectionInfo } = {}
	for _, conn in connections do
		conn_by_session_key[conn.session_key] = conn
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
		if conn_playback then
			local now_playing, play_state = build_now_playing(conn_playback)
			dto.NowPlayingItem = now_playing
			dto.PlayState = play_state
		end
		table.insert(result, dto)
	end

	-- Non-WS clients (playback-only) still need to surface here.
	for _, conn in connections do
		if seen_session_keys[conn.session_key] then
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
		if conn_playback then
			local now_playing, play_state = build_now_playing(conn_playback)
			legacy.NowPlayingItem = now_playing
			legacy.PlayState = play_state
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
	require_remote_control: boolean
)
	for _, dto in build_sessions_dto_list(user_id, username, connections) do
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
	require_remote_control: boolean
): { JsonValue }
	local result: { JsonValue } = {}
	for _, user in list_all_user_identities(own) do
		local connections = if user.user_id == own.user_id then own_connections else {}
		append_sessions_for_user(result, user.user_id, user.username, connections, require_remote_control)
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
	if session.is_admin(principal) then
		return list_all_sessions({
			user_id = principal.user_id,
			username = principal.username,
		}, own_connections, require_remote_control)
	end

	local result: { JsonValue } = {}
	append_sessions_for_user(result, principal.user_id, principal.username, own_connections, require_remote_control)
	return result
end

-- Must run in the socket's own dispatch so the host acts as the socket's principal.
function module.push(entry: SessionEntry)
	xpcall(function()
		local list = module.list_visible(entry.principal, nil)
		entry.sender:send(websocket.encode("Sessions", list, nil))
	end, function(err)
		warn(`sessions push failed (user_id={entry.user_id}): {tostring(err)}`)
	end)
end

return module
