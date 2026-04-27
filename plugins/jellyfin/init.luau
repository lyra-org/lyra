--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local auth = require("@lyra/auth")
local libraries = require("@lyra/libraries")
local lyrics = require("@lyra/lyrics")
local playback_sessions = require("@lyra/playback_sessions")
local playlists = require("@lyra/playlists")
local plugins = require("@lyra/plugins")
local server = require("@lyra/server")
local tracks = require("@lyra/tracks")
local users = require("@lyra/users")

local crypt = require("@harmony/crypt")
local task = require("@harmony/task")

local constants = require("@self/constants")
local config = plugins.declare_settings(constants.declare_settings)
local server_info = server.info()
constants.configure_settings(config)
constants.configure_server_id(server_info.id)
constants.configure_auth_enabled(auth.capabilities().enabled)

local base = require("@self/protocol/items/common")
local catalog = require("@self/protocol/items/catalog")
local discovery = require("@self/protocol/discovery")
local lyrics_protocol = require("@self/protocol/lyrics")
local playback = require("@self/protocol/items/playback")
local preferences = require("@self/protocol/preferences")
local session = require("@self/protocol/session")
local statics = require("@self/protocol/statics")
local websocket = require("@self/protocol/websocket")

-- Represents the last known supported Jellyfin server version.
local SERVER_VERSION = "10.11.0"
local SERVER_ID = constants.SERVER_ID
local P = constants.ROUTE_PREFIX

type ApiContext = api.ApiContext
type ApiQueryParams = api.ApiQueryParams
type ApiResponse = api.ApiResponse
type JsonValue = api.JsonValue
type JsonObject = { [string]: JsonValue }
type ResolvedAuth = auth.ResolvedAuth
type LyricsInfo = lyrics.LyricsInfo
type PlaylistCreateRequest = playlists.PlaylistCreateRequest
type PlaylistUpdateRequest = playlists.PlaylistUpdateRequest
type PlaybackPayload = { [string]: string | number | boolean? }
type PublicUser = users.PublicUser

type UserIdentity = {
	user_id: number,
	username: string,
}

type PublicSystemInfo = {
	ServerName: string,
	Version: string,
	ProductName: string,
	Id: string,
	LocalAddress: string,
	PublicAddress: string?,
	StartupWizardCompleted: boolean,
}

type SystemInfo = PublicSystemInfo & {
	HasPendingRestart: boolean,
	IsShuttingDown: boolean,
	SupportsLibraryMonitor: boolean,
	WebSocketPortNumber: number,
	CompletedInstallations: {},
	CastReceiverApplications: {},
	CanSelfRestart: boolean,
	CanLaunchWebBrowser: boolean,
}

type VirtualFolderInfo = {
	Name: string,
	Locations: { string },
	CollectionType: string,
	ItemId: string,
	RefreshStatus: string,
}

type SessionEntry = {
	sender: api.ApiWebSocketSender,
	session_id: string,
	user_id: number,
	is_admin: boolean,
	device_id: string,
	-- Matches playback_sessions' own session_key formula so broadcast-time
	-- joins can pair WS entries with playback connections.
	auth_session_id: number?,
	info: JsonObject,
	capabilities: JsonObject?,
	-- Keys set by the most recent /Capabilities/Full. Non-Full merges must
	-- skip these so Full stays authoritative and removal-via-Full works.
	full_capability_keys: { [string]: boolean },
	subscribed_sessions: boolean,
	last_activity: string,
	last_playback_event_s: number?,
}

type SyncPlayQueueItem = {
	ItemId: string,
	PlaylistItemId: string,
}

type SyncPlayParticipant = {
	session_id: string,
	user_id: number,
	username: string,
	buffering: boolean,
	ready: boolean,
	ignore_wait: boolean,
	ping: number,
	when: string?,
	position_ticks: number?,
	is_playing: boolean?,
}

type SyncPlayGroup = {
	id: string,
	name: string,
	state: string,
	participants: { [string]: SyncPlayParticipant },
	queue: { SyncPlayQueueItem },
	current_index: number,
	position_ticks: number,
	repeat_mode: string,
	shuffle_mode: string,
	resume_playing: boolean,
	last_updated_at: string,
	last_queue_update: string,
}

type NativeConnectionTarget = {
	user_id: number,
	connection: playback_sessions.ConnectionInfo,
}

local sessions: { [string]: SessionEntry } = {}
local syncplay_groups: { [string]: SyncPlayGroup } = {}
local syncplay_session_to_group: { [string]: SyncPlayGroup } = {}
local syncplay_active_users: { [number]: number } = {}
local syncplay_guid_counter = 0

local JELLYFIN_WS_REAUTH_INTERVAL_SECONDS = 60
local JELLYFIN_WS_REAUTH_MAX_ERRORS = 5
-- Jellyfin's ForceKeepAlive Data is the interval (seconds) after which the
-- server considers a client dead; clients echo KeepAlive at half that rate.
local JELLYFIN_WS_KEEPALIVE_SECONDS = 60
-- Rate-limit inbound PlaybackProgress so a misbehaving client can't flood the
-- DB. Start / Stopped go through unthrottled. Mirrors Jellyfin's own ~10s
-- progress cadence.
local JELLYFIN_WS_PROGRESS_MIN_INTERVAL_SECONDS = 10

-- Mirrors playback_sessions.resolve_playback_session_key so broadcast joins
-- pair WS entries with native playback connections by the same key.
local function connection_join_key(entry: SessionEntry): string
	if entry.auth_session_id then
		return string.format("auth:%d", entry.auth_session_id)
	end
	return string.format("device:%d:%s", entry.user_id, entry.device_id)
end

-- Returns nil on miss, scope mismatch, or closed sender (evicting the
-- closed entry). Mismatch is logged so a hash collision is distinguishable
-- from the normal miss.
local function get_owned_session(session_id: string?, principal: session.Principal): SessionEntry?
	if not session_id then
		return nil
	end
	local entry = sessions[session_id]
	if not entry then
		return nil
	end
	if entry.user_id ~= principal.user_id then
		print(`session user_id mismatch: principal={principal.user_id}, entry={entry.user_id}, session_id={session_id}`)
		return nil
	end
	if entry.sender:is_closed() then
		sessions[session_id] = nil
		return nil
	end
	return entry
end

local SYNCPLAY_EMPTY_GUID = "00000000-0000-0000-0000-000000000000"
local SYNCPLAY_DEFAULT_GROUP_NAME = "SyncPlay Group"
local SYNCPLAY_QUEUE_MODES = { "Queue", "QueueNext" }
local SYNCPLAY_REPEAT_MODES = { "RepeatOne", "RepeatAll", "RepeatNone" }
local SYNCPLAY_SHUFFLE_MODES = { "Sorted", "Shuffle" }

local function syncplay_now(): string
	return session.current_iso_timestamp()
end

local function syncplay_format_guid(hex: string): string
	local stripped = string.gsub(hex, "-", "")
	local normalized = string.lower(stripped)
	if #normalized < 32 then
		normalized ..= string.rep("0", 32 - #normalized)
	end
	normalized = string.sub(normalized, 1, 32)
	return string.format(
		"%s-%s-%s-%s-%s",
		string.sub(normalized, 1, 8),
		string.sub(normalized, 9, 12),
		string.sub(normalized, 13, 16),
		string.sub(normalized, 17, 20),
		string.sub(normalized, 21, 32)
	)
end

local function syncplay_new_guid(): string
	local ok, random_hex = pcall(crypt.random, 16)
	if ok and type(random_hex) == "string" then
		return syncplay_format_guid(random_hex)
	end

	syncplay_guid_counter += 1
	local fallback = crypt.hash("xxh3_128", `{os.time()}:{syncplay_guid_counter}`)
	return syncplay_format_guid(fallback)
end

local function syncplay_normalize_guid(value: JsonValue?): string?
	if type(value) ~= "string" then
		return nil
	end
	local text = base.trim(value)
	if not text then
		return nil
	end
	local stripped = string.gsub(text, "-", "")
	local normalized = string.lower(stripped)
	if #normalized ~= 32 or not string.match(normalized, "^[0-9a-f]+$") then
		return nil
	end
	return syncplay_format_guid(normalized)
end

local function syncplay_body(ctx: ApiContext?): JsonObject
	local request = if ctx then ctx.request else nil
	local request_json = if request then request.json else nil
	if type(request_json) == "table" then
		return request_json :: JsonObject
	end
	return {}
end

local function syncplay_number(value: JsonValue?, default_value: number?): number?
	if type(value) == "number" then
		if value ~= value then
			return default_value
		end
		return math.floor(value)
	end
	if type(value) == "string" then
		local parsed = tonumber(base.trim(value) or "")
		if parsed and parsed == parsed then
			return math.floor(parsed)
		end
	end
	return default_value
end

local function syncplay_bool(value: JsonValue?, default_value: boolean): boolean
	if type(value) == "boolean" then
		return value
	end
	if type(value) == "string" then
		local normalized = string.lower(value)
		if normalized == "true" or normalized == "1" then
			return true
		end
		if normalized == "false" or normalized == "0" then
			return false
		end
	end
	return default_value
end

local function syncplay_enum(value: JsonValue?, values: { string }, default_value: string): string
	if type(value) == "number" then
		local index = math.floor(value) + 1
		return values[index] or default_value
	end
	if type(value) == "string" then
		local trimmed = base.trim(value)
		if not trimmed then
			return default_value
		end
		local numeric = tonumber(trimmed)
		if numeric then
			local index = math.floor(numeric) + 1
			return values[index] or default_value
		end
		local normalized = string.lower(trimmed)
		for _, candidate in values do
			if string.lower(candidate) == normalized then
				return candidate
			end
		end
	end
	return default_value
end

local function syncplay_mark_updated(group: SyncPlayGroup)
	group.last_updated_at = syncplay_now()
end

local function syncplay_mark_queue_updated(group: SyncPlayGroup)
	local now = syncplay_now()
	group.last_updated_at = now
	group.last_queue_update = now
end

local function syncplay_item_accessible(_principal: session.Principal, item_id: string): boolean
	local db_id = base.decode_jellyfin_item_id(item_id)
	return playback.resolve_track_for_playback(db_id) ~= nil
end

local function syncplay_group_accessible(principal: session.Principal, group: SyncPlayGroup): boolean
	for _, queue_item in group.queue do
		if not syncplay_item_accessible(principal, queue_item.ItemId) then
			return false
		end
	end
	return true
end

local function syncplay_parse_queue_items(
	principal: session.Principal,
	value: JsonValue?
): ({ SyncPlayQueueItem }?, string?)
	if type(value) ~= "table" then
		return nil, "missing item ids"
	end

	local result: { SyncPlayQueueItem } = {}
	for _, raw_item_id: JsonValue in value :: { JsonValue } do
		local item_id = syncplay_normalize_guid(raw_item_id)
		if not item_id then
			return nil, "invalid item id"
		end
		if not syncplay_item_accessible(principal, item_id) then
			return nil, "item not found"
		end
		table.insert(result, {
			ItemId = item_id,
			PlaylistItemId = syncplay_new_guid(),
		})
	end
	return result, nil
end

local function syncplay_parse_guid_list(value: JsonValue?): ({ string }?, string?)
	if type(value) ~= "table" then
		return nil, "missing playlist item ids"
	end

	local result: { string } = {}
	for _, raw_id: JsonValue in value :: { JsonValue } do
		local id = syncplay_normalize_guid(raw_id)
		if not id then
			return nil, "invalid playlist item id"
		end
		table.insert(result, id)
	end
	return result, nil
end

local function syncplay_session_entry(ctx: ApiContext?, principal: session.Principal): (SessionEntry?, ApiResponse?)
	local identity = session.resolve_client_identity(ctx)
	if not identity then
		return nil, base.bad_request("missing Client/DeviceId")
	end

	local session_id = session.build_session_id(principal, identity)
	local entry = get_owned_session(session_id, principal)
	if not entry then
		return nil, base.not_found("session not found")
	end
	return entry, nil
end

local function syncplay_participant_names(group: SyncPlayGroup): { JsonValue }
	local names: { string } = {}
	local seen: { [string]: boolean } = {}
	for _, participant in group.participants do
		if not seen[participant.username] then
			seen[participant.username] = true
			table.insert(names, participant.username)
		end
	end
	table.sort(names)

	local result: { JsonValue } = {}
	for _, name in names do
		table.insert(result, name)
	end
	return result
end

local function syncplay_group_info(group: SyncPlayGroup): JsonObject
	return {
		GroupId = group.id,
		GroupName = group.name,
		State = group.state,
		Participants = syncplay_participant_names(group),
		LastUpdatedAt = group.last_updated_at,
	}
end

local function syncplay_update(group_id: string, update_type: string, data: JsonValue): JsonObject
	return {
		GroupId = group_id,
		Type = update_type,
		Data = data,
	}
end

local function syncplay_send_entry(entry: SessionEntry, message_type: string, data: JsonValue): boolean
	if entry.sender:is_closed() then
		if sessions[entry.session_id] == entry then
			sessions[entry.session_id] = nil
		end
		return false
	end

	local ok = pcall(function()
		entry.sender:send(websocket.encode(message_type, data, nil))
	end)
	if not ok then
		if sessions[entry.session_id] == entry then
			sessions[entry.session_id] = nil
		end
		return false
	end
	return true
end

local function syncplay_send_group_update_entry(
	entry: SessionEntry,
	group_id: string,
	update_type: string,
	data: JsonValue
)
	syncplay_send_entry(entry, "SyncPlayGroupUpdate", syncplay_update(group_id, update_type, data))
end

local function syncplay_send_filter(
	group: SyncPlayGroup,
	from_session_id: string,
	filter: string,
	message_type: string,
	data: JsonValue
)
	for session_id, participant in group.participants do
		local should_send = filter == "all"
			or (filter == "current" and session_id == from_session_id)
			or (filter == "except" and session_id ~= from_session_id)
			or (filter == "ready" and not participant.buffering and not participant.ignore_wait)
		if not should_send then
			continue
		end

		local entry = sessions[session_id]
		if entry then
			syncplay_send_entry(entry, message_type, data)
		end
	end
end

local function syncplay_broadcast_group_update(
	group: SyncPlayGroup,
	from_entry: SessionEntry,
	filter: string,
	update_type: string,
	data: JsonValue
)
	syncplay_send_filter(
		group,
		from_entry.session_id,
		filter,
		"SyncPlayGroupUpdate",
		syncplay_update(group.id, update_type, data)
	)
end

local function syncplay_playlist_json(group: SyncPlayGroup): { JsonValue }
	local result: { JsonValue } = {}
	for _, item in group.queue do
		local item_json: JsonObject = {
			ItemId = item.ItemId,
			PlaylistItemId = item.PlaylistItemId,
		}
		table.insert(result, item_json)
	end
	return result
end

local function syncplay_play_queue_update(group: SyncPlayGroup, reason: string): JsonObject
	return {
		Reason = reason,
		LastUpdate = group.last_queue_update,
		Playlist = syncplay_playlist_json(group),
		PlayingItemIndex = group.current_index,
		StartPositionTicks = group.position_ticks,
		IsPlaying = group.state == "Playing",
		ShuffleMode = group.shuffle_mode,
		RepeatMode = group.repeat_mode,
	}
end

local function syncplay_broadcast_queue_update(group: SyncPlayGroup, from_entry: SessionEntry, reason: string)
	syncplay_mark_queue_updated(group)
	syncplay_broadcast_group_update(group, from_entry, "all", "PlayQueue", syncplay_play_queue_update(group, reason))
end

local function syncplay_current_item(group: SyncPlayGroup): SyncPlayQueueItem?
	if group.current_index < 0 then
		return nil
	end
	return group.queue[group.current_index + 1]
end

local function syncplay_current_playlist_item_id(group: SyncPlayGroup): string
	local item = syncplay_current_item(group)
	return if item then item.PlaylistItemId else SYNCPLAY_EMPTY_GUID
end

local function syncplay_command(
	group: SyncPlayGroup,
	command: string,
	position_ticks: number?,
	when: string?
): JsonObject
	local emitted_at = syncplay_now()
	return {
		GroupId = group.id,
		PlaylistItemId = syncplay_current_playlist_item_id(group),
		When = when or emitted_at,
		Command = command,
		PositionTicks = position_ticks or group.position_ticks,
		EmittedAt = emitted_at,
	}
end

local function syncplay_broadcast_command(
	group: SyncPlayGroup,
	from_entry: SessionEntry,
	filter: string,
	command: string,
	position_ticks: number?,
	when: string?
)
	syncplay_send_filter(
		group,
		from_entry.session_id,
		filter,
		"SyncPlayCommand",
		syncplay_command(group, command, position_ticks, when)
	)
end

local function syncplay_broadcast_state(group: SyncPlayGroup, from_entry: SessionEntry, reason: string)
	syncplay_mark_updated(group)
	syncplay_broadcast_group_update(group, from_entry, "all", "StateUpdate", {
		State = group.state,
		Reason = reason,
	})
end

local function syncplay_increment_active_user(user_id: number)
	syncplay_active_users[user_id] = (syncplay_active_users[user_id] or 0) + 1
end

local function syncplay_decrement_active_user(user_id: number)
	local count = syncplay_active_users[user_id]
	if not count then
		return
	end
	if count <= 1 then
		syncplay_active_users[user_id] = nil
	else
		syncplay_active_users[user_id] = count - 1
	end
end

local function syncplay_add_participant(group: SyncPlayGroup, entry: SessionEntry)
	local existing = group.participants[entry.session_id]
	if not existing then
		syncplay_increment_active_user(entry.user_id)
	end

	group.participants[entry.session_id] = {
		session_id = entry.session_id,
		user_id = entry.user_id,
		username = tostring(entry.info.UserName or entry.user_id),
		buffering = false,
		ready = true,
		ignore_wait = false,
		ping = 0,
		when = nil,
		position_ticks = nil,
		is_playing = nil,
	}
	syncplay_session_to_group[entry.session_id] = group
	syncplay_mark_updated(group)
end

local function syncplay_group_is_empty(group: SyncPlayGroup): boolean
	for _ in group.participants do
		return false
	end
	return true
end

local function syncplay_remove_participant(group: SyncPlayGroup, entry: SessionEntry)
	local participant = group.participants[entry.session_id]
	if not participant then
		return
	end

	group.participants[entry.session_id] = nil
	if syncplay_session_to_group[entry.session_id] == group then
		syncplay_session_to_group[entry.session_id] = nil
	end
	syncplay_decrement_active_user(participant.user_id)
	syncplay_mark_updated(group)

	if syncplay_group_is_empty(group) then
		syncplay_groups[group.id] = nil
	end
end

local function syncplay_leave_entry(entry: SessionEntry, notify: boolean)
	local group = syncplay_session_to_group[entry.session_id]
	if not group then
		if notify then
			syncplay_send_group_update_entry(entry, SYNCPLAY_EMPTY_GUID, "NotInGroup", "")
		end
		return
	end

	local participant = group.participants[entry.session_id]
	local username = if participant then participant.username else tostring(entry.info.UserName or entry.user_id)
	if notify then
		syncplay_send_group_update_entry(entry, group.id, "GroupLeft", group.id)
	end
	syncplay_remove_participant(group, entry)
	if notify and syncplay_groups[group.id] ~= nil then
		syncplay_broadcast_group_update(group, entry, "except", "UserLeft", username)
	end
end

local function syncplay_new_group(entry: SessionEntry, group_name: string?): SyncPlayGroup
	if syncplay_session_to_group[entry.session_id] then
		syncplay_leave_entry(entry, true)
	end

	local now = syncplay_now()
	local id = syncplay_new_guid()
	local group: SyncPlayGroup = {
		id = id,
		name = group_name or SYNCPLAY_DEFAULT_GROUP_NAME,
		state = "Idle",
		participants = {},
		queue = {},
		current_index = -1,
		position_ticks = 0,
		repeat_mode = "RepeatNone",
		shuffle_mode = "Sorted",
		resume_playing = false,
		last_updated_at = now,
		last_queue_update = now,
	}
	syncplay_groups[id] = group
	syncplay_add_participant(group, entry)
	syncplay_send_group_update_entry(entry, group.id, "GroupJoined", syncplay_group_info(group))
	return group
end

local function syncplay_set_all_buffering(group: SyncPlayGroup, buffering: boolean)
	for _, participant in group.participants do
		participant.buffering = buffering
		participant.ready = not buffering
	end
end

local function syncplay_group_ready(group: SyncPlayGroup): boolean
	for _, participant in group.participants do
		if not participant.ignore_wait and participant.buffering then
			return false
		end
	end
	return true
end

local function syncplay_clamp_position(value: number?): number
	if not value or value < 0 then
		return 0
	end
	return value
end

local function syncplay_set_current_index(group: SyncPlayGroup, index: number)
	if index < 0 or index >= #group.queue then
		group.current_index = -1
	else
		group.current_index = index
	end
end

local function syncplay_find_playlist_item(group: SyncPlayGroup, playlist_item_id: string): number?
	for index, item in group.queue do
		if item.PlaylistItemId == playlist_item_id then
			return index - 1
		end
	end
	return nil
end

local function syncplay_known_current_item(group: SyncPlayGroup, playlist_item_id: string?): boolean
	if not playlist_item_id or playlist_item_id == SYNCPLAY_EMPTY_GUID then
		return true
	end
	return playlist_item_id == syncplay_current_playlist_item_id(group)
end

local function syncplay_transition_if_ready(
	group: SyncPlayGroup,
	from_entry: SessionEntry,
	is_playing: boolean?,
	when: string?
)
	if not syncplay_group_ready(group) then
		syncplay_broadcast_state(group, from_entry, "Ready")
		return
	end

	local next_state = if is_playing == true or group.resume_playing then "Playing" else "Paused"
	group.state = next_state
	local command = if next_state == "Playing" then "Unpause" else "Pause"
	syncplay_broadcast_command(group, from_entry, "all", command, group.position_ticks, when)
	syncplay_broadcast_state(group, from_entry, "Ready")
end

local function syncplay_require_group(
	ctx: ApiContext?
): (session.Principal?, SessionEntry?, SyncPlayGroup?, ApiResponse?)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return nil, nil, nil, session.unauthorized()
	end

	local entry, entry_err = syncplay_session_entry(ctx, principal)
	if entry_err then
		return principal, nil, nil, entry_err
	end
	local group = syncplay_session_to_group[(entry :: SessionEntry).session_id]
	if not group then
		syncplay_send_group_update_entry(entry :: SessionEntry, SYNCPLAY_EMPTY_GUID, "NotInGroup", "")
		return principal, entry, nil, api.response.empty(204)
	end
	return principal, entry, group, nil
end

api.websocket(P .. "/socket", function(reader, sender, ctx)
	local resolved_auth, token = session.require_jellyfin_auth(ctx)
	if not resolved_auth or not token then
		return
	end

	local identity = session.resolve_client_identity(ctx)
	if not identity then
		print(`socket rejected: missing Client/DeviceId`)
		return
	end

	local principal = resolved_auth.principal
	local session_id = session.build_session_id(principal, identity)
	local iso_now = session.current_iso_timestamp()
	local info = session.build_session_info(session_id, principal, identity, iso_now)
	local entry: SessionEntry = {
		sender = sender,
		session_id = session_id,
		user_id = principal.user_id,
		is_admin = session.is_admin(principal),
		device_id = identity.device_id,
		auth_session_id = resolved_auth.credential.session_id,
		info = info,
		capabilities = nil,
		full_capability_keys = {},
		subscribed_sessions = false,
		last_activity = iso_now,
		last_playback_event_s = nil,
	}

	local existing = sessions[session_id]
	if existing then
		if existing.sender:is_closed() then
			-- Stale zombie from a handler that hasn't run cleanup yet; drop
			-- regardless of whose it was so a legitimate reconnect isn't
			-- blocked by a dead foreign slot.
			sessions[session_id] = nil
		elseif existing.user_id ~= principal.user_id then
			-- Different-user means a collision on the id function; refuse to
			-- overwrite another user's live socket.
			print(
				`session id collision; refusing overwrite: existing.user_id={existing.user_id}, new.user_id={principal.user_id}`
			)
			return
		else
			pcall(function()
				existing.sender:close()
			end)
		end
	end
	sessions[session_id] = entry

	pcall(function()
		sender:send(websocket.encode("ForceKeepAlive", JELLYFIN_WS_KEEPALIVE_SECONDS, nil))
	end)

	-- Public WS routes skip the driver's built-in reauth, so the plugin
	-- owns revocation. Outer xpcall ensures a throw can't silently disable
	-- enforcement for the rest of the connection.
	local reauth_thread = task.spawn(function()
		xpcall(function()
			local consecutive_errors = 0
			while not sender:is_closed() do
				task.wait(JELLYFIN_WS_REAUTH_INTERVAL_SECONDS)
				if sender:is_closed() then
					break
				end
				local ok, result = pcall(auth.resolve_auth, token)
				if ok then
					if result == nil then
						sender:close()
						break
					end
					local refreshed = result :: ResolvedAuth
					if refreshed.principal.user_id ~= principal.user_id then
						sender:close()
						break
					end
					entry.is_admin = session.is_admin(refreshed.principal)
					entry.info.UserName = refreshed.principal.username
					consecutive_errors = 0
				else
					consecutive_errors += 1
					if consecutive_errors >= JELLYFIN_WS_REAUTH_MAX_ERRORS then
						print(`re-auth failed repeatedly, closing (user_id={principal.user_id}): {tostring(result)}`)
						sender:close()
						break
					end
				end
			end
		end, function(err)
			-- Can't safely restart without risking runaway-loop exceptions,
			-- so close the socket and surface the failure.
			print(`re-auth thread crashed, closing (user_id={principal.user_id}): {tostring(err)}`)
			pcall(function()
				sender:close()
			end)
		end)
	end)

	-- Keep cleanup reachable on handler throw.
	xpcall(function()
		while true do
			local frame = reader:recv()
			if frame == nil then
				break
			end
			local envelope = websocket.decode(frame)
			if not envelope then
				continue
			end
			entry.last_activity = session.current_iso_timestamp()
			local mt = envelope.MessageType
			if mt == "KeepAlive" then
				pcall(function()
					sender:send(websocket.encode("KeepAlive", JELLYFIN_WS_KEEPALIVE_SECONDS, envelope.MessageId))
				end)
			elseif mt == "SessionsStart" then
				entry.subscribed_sessions = true
			elseif mt == "SessionsStop" then
				entry.subscribed_sessions = false
			-- Malformed branches bump the timer too so a garbage flood can't
			-- amplify the log itself — one diagnostic per window.
			elseif mt == "PlaybackStart" then
				if session.handle_ws_playback_event(resolved_auth, envelope.Data, "started", identity.device_id) then
					entry.last_playback_event_s = os.time()
				else
					print(`discarded malformed PlaybackStart (user_id={principal.user_id})`)
					entry.last_playback_event_s = os.time()
				end
			elseif mt == "PlaybackProgress" then
				local now_s = os.time()
				local last_s = entry.last_playback_event_s
				if last_s and now_s - last_s < JELLYFIN_WS_PROGRESS_MIN_INTERVAL_SECONDS then
					continue
				end
				if session.handle_ws_playback_event(resolved_auth, envelope.Data, "progress", identity.device_id) then
					entry.last_playback_event_s = now_s
				else
					print(`discarded malformed PlaybackProgress (user_id={principal.user_id})`)
					entry.last_playback_event_s = now_s
				end
			elseif mt == "PlaybackStopped" then
				if session.handle_ws_playback_event(resolved_auth, envelope.Data, "stopped", identity.device_id) then
					entry.last_playback_event_s = os.time()
				else
					print(`discarded malformed PlaybackStopped (user_id={principal.user_id})`)
					entry.last_playback_event_s = os.time()
				end
			end
		end
	end, function(err)
		print(`socket handler error (user_id={principal.user_id}): {tostring(err)}`)
	end)

	task.cancel(reauth_thread)

	-- A concurrent reconnect may have replaced our slot; only clear it if
	-- it still points at our sender.
	if sessions[session_id] and sessions[session_id].sender == sender then
		if syncplay_session_to_group[entry.session_id] then
			syncplay_leave_entry(entry, true)
		end
		sessions[session_id] = nil
	end
end, "public")

local function public_system_info<T>(extra: T): T & PublicSystemInfo
	local public_address: string? = if server_info.published_url then `{server_info.published_url}{P}` else nil
	local info: T & PublicSystemInfo = extra
	info.ServerName = "Lyra"
	info.Version = SERVER_VERSION
	info.ProductName = "Lyra"
	info.Id = SERVER_ID
	info.LocalAddress = `http://{server_info.hostname}:{server_info.port}{P}`
	info.PublicAddress = public_address
	info.StartupWizardCompleted = true
	return info
end

local ROOT_ITEM_ID = string.rep("0", constants.ITEM_ID_HEX_LENGTH)
local AUTH_PROVIDER_ID = "Lyra"

local function find_public_user_by_id(user_id: number): PublicUser?
	local listed_users = users.list()
	if listed_users == nil then
		return nil
	end

	for _, user in listed_users do
		if user.user_id == user_id then
			return user
		end
	end

	return nil
end

local function root_folder_json(user_id: number): JsonObject
	local views = catalog.to_music_view_items()
	return {
		Id = ROOT_ITEM_ID,
		Name = "Root",
		Type = "Folder",
		IsFolder = true,
		ServerId = SERVER_ID,
		ChildCount = #views,
		LocationType = "FileSystem",
		MediaType = "Unknown",
		UserData = base.user_data_to_json(base.get_user_data({
			key = "Root",
			item_hex_id = ROOT_ITEM_ID,
			user_id = user_id,
			include_favorite = false,
			include_play_count = false,
		})),
	}
end

local function empty_query_result(): JsonObject
	return {
		Items = {},
		TotalRecordCount = 0,
		StartIndex = 0,
	}
end

local function item_count_for(principal: session.Principal, include_item_type: string, is_favorite: boolean): number
	local options = catalog.with_user_query_options(catalog.coerce_items_query(nil), principal.user_id)
	options.include_item_types = { include_item_type }
	options.limit = 0
	options.is_favorite = is_favorite

	local ok, _items, total_count = pcall(catalog.list_items, options)
	if not ok or type(total_count) ~= "number" then
		return 0
	end
	return total_count
end

api.post(P .. "/Users/AuthenticateByName", function(ctx: ApiContext)
	local credentials, parse_err = session.parse_authenticate_request(ctx)
	if not credentials then
		return parse_err or base.bad_request("invalid authentication request body")
	end

	local login_result = auth.login(credentials.username, credentials.password)
	if not login_result then
		return session.unauthorized()
	end

	local token = base.trim(login_result.token)
	if not token then
		return session.unauthorized()
	end
	login_result.token = token

	return api.response.json(200, session.to_authentication_result(login_result))
end, "public", true)
api.get(P .. "/Users/Public", function(): ApiResponse
	-- TODO: Match Jellyfin public-user filtering (hidden/disabled, device visibility, and remote-access gating) instead of returning all users.
	local listed_users = users.list()
	if listed_users == nil then
		return api.response.json(200, {})
	end

	local public_users = {}
	for _, user in listed_users do
		table.insert(public_users, session.to_jellyfin_user(user))
	end

	return api.response.json(200, public_users)
end, "public", true)
api.get(P .. "/Users", function(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local result: { JsonValue } = {}
	for _, user in users.list() do
		table.insert(
			result,
			session.to_jellyfin_user({
				user_id = user.user_id,
				username = user.username,
			})
		)
	end

	return api.response.json(200, result)
end, nil, true)
api.get(P .. "/Users/{id}", function(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local raw_user_id = if params then params.id else nil
	local user_id = base.decode_jellyfin_item_id(raw_user_id)
	if not user_id then
		return base.not_found("user not found")
	end
	if user_id ~= principal.user_id and not session.is_admin(principal) then
		return session.unauthorized()
	end
	if user_id == principal.user_id then
		return api.response.json(200, session.to_jellyfin_principal_user(principal))
	end

	local user = find_public_user_by_id(user_id)
	if not user then
		return base.not_found("user not found")
	end
	return api.response.json(200, session.to_jellyfin_user(user))
end, nil, true)
api.get(P .. "/QuickConnect/Enabled", function(): ApiResponse
	return api.response.json(200, false)
end, "public", true)
api.post(P .. "/Users/ForgotPassword", function(): ApiResponse
	return api.response.json(200, {
		Action = "ContactAdmin",
		PinFile = nil,
		PinExpirationDate = nil,
	})
end, "public", true)
api.post(P .. "/Users/ForgotPassword/Pin", function(): ApiResponse
	return api.response.json(200, {
		Success = false,
		UsersReset = {},
	})
end, "public", true)
api.post(P .. "/Sessions/Logout", function(ctx: ApiContext)
	local resolved_auth, token = session.require_jellyfin_auth(ctx)
	if not resolved_auth or not token then
		return session.unauthorized()
	end

	auth.logout_session(token)

	return api.response.empty(204)
end, nil, true)
api.get(P .. "/Auth/Providers", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local providers: { JsonValue } = {
		{
			Name = "Lyra",
			Id = AUTH_PROVIDER_ID,
		},
	}
	return api.response.json(200, providers)
end, nil, true)
api.get(P .. "/Auth/PasswordResetProviders", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local providers: { JsonValue } = {
		{
			Name = "Lyra",
			Id = AUTH_PROVIDER_ID,
		},
	}
	return api.response.json(200, providers)
end, nil, true)
api.get(P .. "/System/Info/Public", function(): ApiResponse
	return api.response.json(200, public_system_info({}))
end, "public", true)
api.get(P .. "/System/Info", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	return api.response.json(
		200,
		public_system_info({
			HasPendingRestart = false,
			IsShuttingDown = false,
			SupportsLibraryMonitor = false,
			WebSocketPortNumber = server_info.port,
			CompletedInstallations = {},
			CastReceiverApplications = {},
			CanSelfRestart = true,
			CanLaunchWebBrowser = false,
		})
	)
end, nil, true)
api.get(P .. "/System/Ping", function(): ApiResponse
	return api.response.json(200, "Lyra")
end, "public", true)
api.post(P .. "/System/Ping", function(): ApiResponse
	return api.response.json(200, "Lyra")
end, "public", true)
api.get(P .. "/System/Endpoint", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local in_network = base.is_local_network_request(ctx)
	return api.response.json(200, {
		IsLocal = in_network,
		IsInNetwork = in_network,
	})
end, nil, true)
api.get(P .. "/Playback/BitrateTest", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	return api.response.text(200, "", { ["content-type"] = "application/octet-stream" })
end, nil, true)
api.get(P .. "/Library/VirtualFolders", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local ok, libs = pcall(libraries.list, nil)
	if not ok or not libs then
		return api.response.json(200, {})
	end

	local folders: { JsonValue } = {}
	for _, lib in libs do
		local hex_id = base.nanoid_to_hex(lib.id)
		if hex_id then
			local folder: JsonValue = {
				Name = lib.name,
				Locations = { lib.directory },
				CollectionType = "music",
				ItemId = hex_id,
				RefreshStatus = "",
			}
			table.insert(folders, folder)
		end
	end

	return api.response.json(200, folders)
end, nil, true)
api.get(P .. "/Library/MediaFolders", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local views = catalog.to_music_view_items()
	return api.response.json(200, catalog.build_item_query_result(views, #views, 0))
end, nil, true)
api.get(P .. "/Channels", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	return api.response.json(200, empty_query_result())
end, nil, true)
api.get(P .. "/Devices", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	return api.response.json(200, {
		Items = {},
		TotalRecordCount = 0,
	})
end, nil, true)
api.get(P .. "/Users/Me", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	return api.response.json(200, session.to_jellyfin_principal_user(principal))
end, nil, true)
local function respond_with_user_views(ctx: ApiContext): ApiResponse
	local query = if ctx and ctx.request then ctx.request.query else nil
	local options = catalog.coerce_items_query(query)
	local views = catalog.to_music_view_items()
	views = catalog.apply_search_filter(views, options.search_term)
	catalog.apply_sort(views, options)
	local items, total_count, start_index = base.paginate_items(views, options.start_index, options.limit)
	return api.response.json(200, catalog.build_item_query_result(items, total_count, start_index))
end

api.get(P .. "/Users/{id}/Views", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	if not session.require_matching_user_scope_from_id_param(ctx, principal) then
		return session.unauthorized()
	end

	return respond_with_user_views(ctx)
end, nil, true)
api.get(P .. "/UserViews", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return respond_with_user_views(ctx)
end, nil, true)
api.get(P .. "/UserViews/GroupingOptions", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	return api.response.json(200, {})
end, nil, true)
api.get(P .. "/Users/{userId}/GroupingOptions", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return api.response.json(200, {})
end, nil, true)
local function handle_root_folder_route(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return api.response.json(200, root_folder_json(principal.user_id))
end

api.get(P .. "/Items/Root", handle_root_folder_route, nil, true)
api.get(P .. "/Users/{userId}/Items/Root", handle_root_folder_route, nil, true)
local function respond_with_items_listing(ctx: ApiContext, principal: session.Principal): ApiResponse
	local query = if ctx and ctx.request then ctx.request.query else nil
	local options = catalog.with_user_query_options(catalog.coerce_items_query(query), principal.user_id)

	-- Jellyfin returns the user root's direct children (library views) when ParentId/Ids are absent and Recursive isn't true.
	if options.parent_id == nil and #options.ids == 0 and options.recursive == false then
		local views = catalog.to_music_view_items()
		views = catalog.apply_search_filter(views, options.search_term)
		catalog.apply_sort(views, options)
		local items, total_count, start_index = base.paginate_items(views, options.start_index, options.limit)
		return api.response.json(200, catalog.build_item_query_result(items, total_count, start_index))
	end

	local items, total_count, start_index = catalog.list_items(options)
	return api.response.json(200, catalog.build_item_query_result(items, total_count, start_index))
end

api.get(P .. "/Users/{userId}/Items", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return respond_with_items_listing(ctx, principal)
end, nil, true)
api.get(P .. "/Items", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return respond_with_items_listing(ctx, principal)
end, nil, true)
local function handle_similar_items_route(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local raw_item_id = if params then params.itemId else nil
	if not raw_item_id then
		return base.bad_request("missing item id")
	end

	local item_id = base.decode_jellyfin_item_id(raw_item_id)
	if not item_id then
		return base.not_found("item not found")
	end

	local query = if ctx and ctx.request then ctx.request.query else nil
	local options = catalog.with_user_query_options(catalog.coerce_items_query(query), principal.user_id)
	local items, total_count, start_index, found = catalog.list_similar_items(item_id, options)
	if not found then
		return base.not_found("item not found")
	end

	return api.response.json(200, catalog.build_item_query_result(items, total_count, start_index))
end

api.get(P .. "/Artists/{itemId}/Similar", handle_similar_items_route, nil, true)
api.get(P .. "/Items/{itemId}/Similar", handle_similar_items_route, nil, true)
api.get(P .. "/Albums/{itemId}/Similar", handle_similar_items_route, nil, true)
local function respond_with_latest_items(ctx: ApiContext, principal: session.Principal): ApiResponse
	local query = if ctx and ctx.request then ctx.request.query else nil
	local options = catalog.with_user_query_options(catalog.coerce_items_query(query), principal.user_id)
	local items = catalog.list_latest_items(options)
	return api.response.json(200, catalog.items_to_json(items))
end

api.get(P .. "/Users/{userId}/Items/Latest", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return respond_with_latest_items(ctx, principal)
end, nil, true)
api.get(P .. "/Users/{userId}/Items/{itemId}", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local raw_item_id = if params then params.itemId else nil
	if not raw_item_id then
		return base.bad_request("missing item id")
	end

	local item = catalog.find_item_by_id(base.decode_jellyfin_item_id(raw_item_id), principal.user_id)
	if not item then
		return base.not_found("item not found")
	end

	return api.response.json(200, catalog.item_to_json(item))
end, nil, true)
api.get(P .. "/Items/{id}", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local raw_id = if params then params.id else nil
	if not raw_id then
		return base.bad_request("missing item id")
	end
	if raw_id == ROOT_ITEM_ID then
		return api.response.json(200, root_folder_json(principal.user_id))
	end

	local item = catalog.find_item_by_id(base.decode_jellyfin_item_id(raw_id), principal.user_id)
	if not item then
		return base.not_found("item not found")
	end

	return api.response.json(200, catalog.item_to_json(item))
end, nil, true)

local function handle_favorite_item_route(ctx: ApiContext, should_favorite: boolean)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local item_id = if params then params.itemId else nil
	if not item_id then
		return base.bad_request("missing item id")
	end

	local target_id = base.decode_jellyfin_item_id(item_id)
	if target_id == nil then
		return base.not_found("item not found")
	end

	local item = catalog.find_item_by_id(target_id, principal.user_id)
	if not item then
		return base.not_found("item not found")
	end

	local update_ok = if should_favorite
		then base.add_favorite(principal.user_id, target_id)
		else base.remove_favorite(principal.user_id, target_id)
	if not update_ok then
		return base.bad_request(if should_favorite then "failed to add favorite" else "failed to remove favorite")
	end

	return api.response.json(
		200,
		base.user_data_to_json(base.get_user_data({
			item_id = target_id,
			user_id = principal.user_id,
			include_favorite = true,
			include_play_count = true,
		}))
	)
end

api.post(P .. "/UserFavoriteItems/{itemId}", function(ctx: ApiContext)
	return handle_favorite_item_route(ctx, true)
end, nil, true)
api.post(P .. "/Users/{userId}/FavoriteItems/{itemId}", function(ctx: ApiContext)
	return handle_favorite_item_route(ctx, true)
end, nil, true)
api.delete(P .. "/UserFavoriteItems/{itemId}", function(ctx: ApiContext)
	return handle_favorite_item_route(ctx, false)
end, nil, true)
api.delete(P .. "/Users/{userId}/FavoriteItems/{itemId}", function(ctx: ApiContext)
	return handle_favorite_item_route(ctx, false)
end, nil, true)
api.get(P .. "/Playlists/{playlistId}/Items", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	local playlist, playlist_err = session.resolve_playlist_for_route(ctx)
	if not playlist then
		return playlist_err or base.bad_request("playlist not found")
	end

	local can_view, view_err = session.ensure_playlist_visible(playlist, principal)
	if not can_view then
		return view_err or base.bad_request("playlist not accessible")
	end

	local query = if ctx and ctx.request then ctx.request.query else nil
	local options = catalog.with_user_query_options(catalog.coerce_items_query(query), principal.user_id)
	local items, total_count, start_index = catalog.list_playlist_track_items(playlist, options)
	return api.response.json(200, catalog.build_item_query_result(items, total_count, start_index))
end, nil, true)
api.post(P .. "/Playlists", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local request_payload, parse_err = session.parse_playlist_request_body(ctx)
	if not request_payload then
		return parse_err or base.bad_request("invalid playlist request body")
	end
	if not session.ensure_user_scope_matches_principal(request_payload.user_id, principal) then
		return session.unauthorized()
	end

	local playlist_name = request_payload.name or "New Playlist"
	local create_request: PlaylistCreateRequest = {
		user_id = principal.user_id,
		name = playlist_name,
		is_public = if request_payload.is_public ~= nil then request_payload.is_public else false,
		created_at = session.current_unix_seconds(),
		updated_at = session.current_unix_seconds(),
	}

	local ok, playlist_id = pcall(playlists.create, create_request)
	if not ok then
		return base.bad_request("failed to create playlist")
	end

	local resolved_playlist_id: number = playlist_id :: number
	for _, track_id in request_payload.ids do
		if playback.resolve_track_for_playback(track_id) then
			pcall(playlists.add_track, resolved_playlist_id, track_id)
		end
	end

	local jellyfin_playlist_id = base.encode_jellyfin_item_id(playlist_id)
	if not jellyfin_playlist_id then
		return base.bad_request("failed to encode playlist id")
	end
	return api.response.json(200, {
		Id = jellyfin_playlist_id,
	})
end, nil, true)
api.post(P .. "/Playlists/{playlistId}/Items", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	local playlist, playlist_err = session.resolve_playlist_for_route(ctx)
	if not playlist then
		return playlist_err or base.bad_request("playlist not found")
	end

	local can_edit, edit_err = session.ensure_playlist_editable(playlist, principal)
	if not can_edit then
		return edit_err or base.bad_request("playlist is read-only")
	end

	local query = if ctx and ctx.request then ctx.request.query else nil
	local track_ids = session.parse_playlist_query_track_ids(query)
	if #track_ids == 0 then
		return base.bad_request("missing ids query parameter")
	end

	for _, track_id in track_ids do
		if playback.resolve_track_for_playback(track_id) then
			local playlist_db_id = playlist.db_id
			if playlist_db_id ~= nil then
				pcall(playlists.add_track, playlist_db_id, track_id)
			end
		end
	end

	return api.response.empty(204)
end, nil, true)
api.delete(P .. "/Playlists/{playlistId}/Items", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local playlist, playlist_err = session.resolve_playlist_for_route(ctx)
	if not playlist then
		return playlist_err or base.bad_request("playlist not found")
	end

	local can_edit, edit_err = session.ensure_playlist_editable(playlist, principal)
	if not can_edit then
		return edit_err or base.bad_request("playlist is read-only")
	end

	local query = if ctx and ctx.request then ctx.request.query else nil
	local entry_ids = session.parse_playlist_query_entry_ids(query)
	if #entry_ids == 0 then
		return base.bad_request("missing entryIds query parameter")
	end

	for _, entry_id in entry_ids do
		pcall(playlists.remove_track, entry_id)
	end

	return api.response.empty(204)
end, nil, true)
api.post(P .. "/Playlists/{playlistId}", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local playlist, playlist_err = session.resolve_playlist_for_route(ctx)
	if not playlist then
		return playlist_err or base.bad_request("playlist not found")
	end

	local can_edit, edit_err = session.ensure_playlist_editable(playlist, principal)
	if not can_edit then
		return edit_err or base.bad_request("playlist is read-only")
	end

	local request_payload, parse_err = session.parse_playlist_request_body(ctx)
	if not request_payload then
		return parse_err or base.bad_request("invalid playlist request body")
	end
	if not session.ensure_user_scope_matches_principal(request_payload.user_id, principal) then
		return session.unauthorized()
	end
	local playlist_db_id = playlist.db_id
	if playlist_db_id == nil then
		return base.bad_request("playlist not found")
	end

	local update_request: PlaylistUpdateRequest = {
		playlist_id = playlist_db_id,
		name = request_payload.name,
		is_public = request_payload.is_public,
		updated_at = session.current_unix_seconds(),
	}
	local ok = pcall(playlists.update, update_request)
	if not ok then
		return base.bad_request("failed to update playlist")
	end

	return api.response.empty(204)
end, nil, true)
api.get(P .. "/Playlists/{playlistId}", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local playlist, playlist_err = session.resolve_playlist_for_route(ctx)
	if not playlist then
		return playlist_err or base.bad_request("playlist not found")
	end

	local can_view, view_err = session.ensure_playlist_visible(playlist, principal)
	if not can_view then
		return view_err or base.bad_request("playlist not accessible")
	end

	return api.response.json(200, session.playlist_info_to_json(session.to_playlist_info(playlist)))
end, nil, true)
api.get(P .. "/Playlists/{playlistId}/Users/{userId}", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local playlist, playlist_err = session.resolve_playlist_for_route(ctx)
	if not playlist then
		return playlist_err or base.bad_request("playlist not found")
	end

	local can_view, view_err = session.ensure_playlist_visible(playlist, principal)
	if not can_view then
		return view_err or base.bad_request("playlist not accessible")
	end

	local params = if ctx then ctx.params else nil
	local requested_user_id = if params then session.normalize_optional_user_id(params.userId) else nil
	if not requested_user_id then
		return base.bad_request("missing user id")
	end

	local requester_can_edit = catalog.playlist_can_edit(playlist, principal.user_id)
	if base.decode_jellyfin_item_id(requested_user_id) ~= principal.user_id and not requester_can_edit then
		return session.unauthorized()
	end

	return api.response.json(
		200,
		session.playlist_user_permission_to_json(
			session.to_playlist_user_permission(playlist, base.decode_jellyfin_item_id(requested_user_id) or 0)
		)
	)
end, nil, true)
api.get(P .. "/Playlists/{playlistId}/Users", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local playlist, playlist_err = session.resolve_playlist_for_route(ctx)
	if not playlist then
		return playlist_err or base.bad_request("playlist not found")
	end

	local can_edit, edit_err = session.ensure_playlist_editable(playlist, principal)
	if not can_edit then
		return edit_err or base.bad_request("playlist is read-only")
	end

	local owner_id = catalog.playlist_owner_id(playlist)
	local users = {}
	if owner_id then
		table.insert(users, session.to_playlist_user_permission(playlist, owner_id))
	end

	return api.response.json(200, session.playlist_users_to_json(users))
end, nil, true)
api.get(P .. "/Artists", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local query = if ctx and ctx.request then ctx.request.query else nil
	local options = catalog.with_user_query_options(catalog.coerce_items_query(query), principal.user_id)
	local items, total_count, start_index = catalog.list_artist_items(options)
	return api.response.json(200, catalog.build_item_query_result(items, total_count, start_index))
end, nil, true)
api.get(P .. "/Artists/AlbumArtists", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	local query = if ctx and ctx.request then ctx.request.query else nil
	local options = catalog.with_user_query_options(catalog.coerce_items_query(query), principal.user_id)
	local items, total_count, start_index = catalog.list_artist_items(options)
	return api.response.json(200, catalog.build_item_query_result(items, total_count, start_index))
end, nil, true)
local function handle_genres_route(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local query = if ctx and ctx.request then ctx.request.query else nil
	local options = catalog.with_user_query_options(catalog.coerce_items_query(query), principal.user_id)
	local items, total_count, start_index = catalog.list_genre_items(options)
	return api.response.json(200, catalog.build_item_query_result(items, total_count, start_index))
end

api.get(P .. "/Genres", function(ctx: ApiContext)
	return handle_genres_route(ctx)
end, nil, true)
api.get(P .. "/MusicGenres", function(ctx: ApiContext)
	return handle_genres_route(ctx)
end, nil, true)
api.get(P .. "/Items/{imageId}/Images/Primary", function(ctx: ApiContext)
	return session.handle_primary_image_request(ctx)
end, nil, true)
api.head(P .. "/Items/{imageId}/Images/Primary", function(ctx: ApiContext)
	return session.handle_primary_image_request(ctx)
end, nil, true)
-- Merge rule: Full wins on collision, non-Full fills gaps. Without a live
-- WS entry there's nowhere to stash caps; 204 silently so non-WS clients
-- aren't broken.
local function handle_capabilities_post(ctx: ApiContext, body_is_object: boolean): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local identity = session.resolve_client_identity(ctx)
	if not identity then
		return api.response.empty(204)
	end

	local session_id = session.build_session_id(principal, identity)
	local entry = get_owned_session(session_id, principal)
	if not entry then
		return api.response.empty(204)
	end

	if body_is_object then
		local request = if ctx then ctx.request else nil
		local request_json = if request then request.json else nil
		if request_json ~= nil and type(request_json) ~= "table" then
			return base.bad_request("invalid capabilities request body")
		end
		local parsed = session.parse_client_capabilities(request_json)
		if parsed then
			entry.capabilities = parsed
			local full_keys: { [string]: boolean } = {}
			for key in parsed do
				full_keys[key] = true
			end
			entry.full_capability_keys = full_keys
		end
	else
		local request = if ctx then ctx.request else nil
		local query = if request then request.query else nil
		local partial = session.parse_client_capabilities_query(query)
		if partial and next(partial) ~= nil then
			local merged: JsonObject = if entry.capabilities then table.clone(entry.capabilities) else {}
			for key, value in partial do
				if not entry.full_capability_keys[key] then
					merged[key] = value
				end
			end
			entry.capabilities = merged
		end
	end

	return api.response.empty(204)
end

api.post(P .. "/Sessions/Capabilities", function(ctx: ApiContext)
	return handle_capabilities_post(ctx, false)
end, nil, true)
api.post(P .. "/Sessions/Capabilities/Full", function(ctx: ApiContext)
	return handle_capabilities_post(ctx, true)
end, nil, true)
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

local function handle_send_command_error(err: unknown): ApiResponse
	if string.find(tostring(err), "not found") then
		return base.not_found("session not found")
	end
	if string.find(tostring(err), "not authorized") then
		return session.unauthorized()
	end
	return api.response.empty(500)
end

-- Remote-control commands aren't idempotent, so scope mismatch surfaces as
-- 401 and send failure as 404 rather than collapsing to 204.
local function resolve_ws_entry_for_dispatch(
	principal: session.Principal,
	session_id: string?
): (SessionEntry?, ApiResponse?)
	if not session_id then
		return nil, nil
	end
	local entry = sessions[session_id]
	if not entry then
		return nil, nil
	end
	if entry.sender:is_closed() then
		sessions[session_id] = nil
		return nil, nil
	end
	if not session.can_control_user(principal, entry.user_id) then
		print(`dispatch scope mismatch: principal={principal.user_id}, entry={entry.user_id}, session_id={session_id}`)
		return nil, session.unauthorized()
	end
	return entry, nil
end

local function dispatch_to_ws(principal: session.Principal, session_id: string?, envelope_text: string): ApiResponse?
	local entry, resolve_err = resolve_ws_entry_for_dispatch(principal, session_id)
	if resolve_err then
		return resolve_err
	end
	if not entry then
		return nil
	end
	local target_session_id = session_id :: string
	local ok = pcall(function()
		entry.sender:send(envelope_text)
	end)
	if not ok then
		if sessions[target_session_id] == entry then
			sessions[target_session_id] = nil
		end
		return base.not_found("session not found")
	end
	return api.response.empty(204)
end

local function build_now_playing(playback: playback_sessions.PlaybackInfo): (JsonObject, JsonObject)
	local now_playing: JsonObject = {
		Id = base.encode_jellyfin_item_id(playback.track_id),
		Name = resolve_track_name(playback.track_id),
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
	user_id: number,
	username: string,
	connections: { playback_sessions.ConnectionInfo }
): { JsonValue }
	local conn_by_session_key: { [string]: playback_sessions.ConnectionInfo } = {}
	for _, conn in connections do
		conn_by_session_key[conn.session_key] = conn
	end

	local jellyfin_user_id = base.encode_jellyfin_item_id(user_id)
	local result: { JsonValue } = {}
	local seen_session_keys: { [string]: boolean } = {}

	for _, entry in sessions do
		-- Skip foreign users and zombies (closed sender pre-cleanup).
		if entry.user_id ~= user_id or entry.sender:is_closed() then
			continue
		end
		local join_key = connection_join_key(entry)
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

local function list_all_user_identities(fallback: UserIdentity?): { UserIdentity }
	local result: { UserIdentity } = {}
	local seen: { [number]: boolean } = {}
	for _, user: PublicUser in users.list() do
		if not seen[user.user_id] then
			seen[user.user_id] = true
			table.insert(result, {
				user_id = user.user_id,
				username = user.username,
			})
		end
	end
	if fallback and not seen[fallback.user_id] then
		table.insert(result, fallback)
	end
	return result
end

local function visible_user_identities(principal: session.Principal): { UserIdentity }
	if session.is_admin(principal) then
		return list_all_user_identities({
			user_id = principal.user_id,
			username = principal.username,
		})
	end
	return {
		{
			user_id = principal.user_id,
			username = principal.username,
		},
	}
end

local function session_supports_remote_control(dto: JsonValue): boolean
	if type(dto) ~= "table" then
		return false
	end
	return (dto :: JsonObject).SupportsRemoteControl == true
end

local function append_sessions_for_user(
	result: { JsonValue },
	user_id: number,
	username: string,
	require_remote_control: boolean
)
	local connections = playback_sessions.list_connections(user_id)
	for _, dto in build_sessions_dto_list(user_id, username, connections) do
		if not require_remote_control or session_supports_remote_control(dto) then
			table.insert(result, dto)
		end
	end
end

local function list_all_sessions(fallback: UserIdentity?, require_remote_control: boolean): { JsonValue }
	local result: { JsonValue } = {}
	for _, user in list_all_user_identities(fallback) do
		append_sessions_for_user(result, user.user_id, user.username, require_remote_control)
	end
	return result
end

local function list_visible_sessions(principal: session.Principal, controllable_user_id: number?): { JsonValue }
	if controllable_user_id and not session.can_control_user(principal, controllable_user_id) then
		return {}
	end

	local require_remote_control = controllable_user_id ~= nil
	if session.is_admin(principal) then
		return list_all_sessions({
			user_id = principal.user_id,
			username = principal.username,
		}, require_remote_control)
	end

	local result: { JsonValue } = {}
	append_sessions_for_user(result, principal.user_id, principal.username, require_remote_control)
	return result
end

local function find_native_connection_for_user(
	user_id: number,
	session_id: string,
	session_key: string?
): NativeConnectionTarget?
	for _, conn in playback_sessions.list_connections(user_id) do
		if conn.token == session_id or (session_key ~= nil and conn.session_key == session_key) then
			return {
				user_id = user_id,
				connection = conn,
			}
		end
	end
	return nil
end

local function resolve_native_target(
	principal: session.Principal,
	session_id: string,
	ws_entry: SessionEntry?
): (NativeConnectionTarget?, ApiResponse?)
	if ws_entry then
		if not session.can_control_user(principal, ws_entry.user_id) then
			return nil, session.unauthorized()
		end
		return find_native_connection_for_user(ws_entry.user_id, session_id, connection_join_key(ws_entry)), nil
	end

	for _, user in visible_user_identities(principal) do
		local target = find_native_connection_for_user(user.user_id, session_id, nil)
		if target then
			return target, nil
		end
	end
	return nil, nil
end

local function dispatch_to_native(
	principal: session.Principal,
	session_id: string,
	action: string,
	position_ms: number?,
	level: number?
): ApiResponse
	local target, target_err = resolve_native_target(principal, session_id, nil)
	if target_err then
		return target_err
	end
	if not target then
		return base.not_found("session not found")
	end

	local cmd_request: playback_sessions.SendCommandRequest = {
		user_id = target.user_id,
		target_token = target.connection.token,
		action = action,
	}
	if position_ms ~= nil then
		cmd_request.position_ms = position_ms
	end
	if level ~= nil then
		cmd_request.level = level
	end
	local ok, err = pcall(playback_sessions.send_command, cmd_request)
	if not ok then
		return handle_send_command_error(err)
	end
	return api.response.empty(204)
end

api.get(P .. "/Sessions", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local request = if ctx then ctx.request else nil
	local query = if request then request.query else nil
	local controllable = base.parse_query_string(query, { "controllableByUserId", "ControllableByUserId" }, nil)
	local controllable_user_id = if controllable then base.decode_jellyfin_item_id(controllable) else nil
	if controllable and not controllable_user_id then
		return api.response.json(200, {})
	end

	return api.response.json(200, list_visible_sessions(principal, controllable_user_id))
end, nil, true)

api.get(P .. "/SyncPlay/List", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local result: { JsonValue } = {}
	for _, group in syncplay_groups do
		if syncplay_group_accessible(principal, group) then
			table.insert(result, syncplay_group_info(group))
		end
	end
	return api.response.json(200, result)
end, nil, true)

api.get(P .. "/SyncPlay/{id}", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local group_id = syncplay_normalize_guid(if params then params.id else nil)
	if not group_id then
		return base.not_found("syncplay group not found")
	end
	local group = syncplay_groups[group_id]
	if not group or not syncplay_group_accessible(principal, group) then
		return base.not_found("syncplay group not found")
	end
	return api.response.json(200, syncplay_group_info(group))
end, nil, true)

api.post(P .. "/SyncPlay/New", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local entry, entry_err = syncplay_session_entry(ctx, principal)
	if entry_err then
		return entry_err
	end

	local body = syncplay_body(ctx)
	local group_name = if type(body.GroupName) == "string" then base.trim(body.GroupName :: string) else nil
	local group = syncplay_new_group(entry :: SessionEntry, group_name)
	return api.response.json(200, syncplay_group_info(group))
end, nil, true)

api.post(P .. "/SyncPlay/Join", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local entry, entry_err = syncplay_session_entry(ctx, principal)
	if entry_err then
		return entry_err
	end

	local body = syncplay_body(ctx)
	local group_id = syncplay_normalize_guid(body.GroupId)
	if not group_id then
		return base.bad_request("missing GroupId")
	end

	local group = syncplay_groups[group_id]
	if not group then
		syncplay_send_group_update_entry(entry :: SessionEntry, SYNCPLAY_EMPTY_GUID, "GroupDoesNotExist", "")
		return api.response.empty(204)
	end
	if not syncplay_group_accessible(principal, group) then
		syncplay_send_group_update_entry(entry :: SessionEntry, group.id, "LibraryAccessDenied", "")
		return api.response.empty(204)
	end

	local existing_group = syncplay_session_to_group[(entry :: SessionEntry).session_id]
	if existing_group and existing_group.id ~= group.id then
		syncplay_leave_entry(entry :: SessionEntry, true)
	end

	syncplay_add_participant(group, entry :: SessionEntry)
	syncplay_send_group_update_entry(entry :: SessionEntry, group.id, "GroupJoined", syncplay_group_info(group))
	syncplay_broadcast_group_update(
		group,
		entry :: SessionEntry,
		"except",
		"UserJoined",
		tostring((entry :: SessionEntry).info.UserName or "")
	)
	if #group.queue > 0 then
		syncplay_send_group_update_entry(
			entry :: SessionEntry,
			group.id,
			"PlayQueue",
			syncplay_play_queue_update(group, "NewPlaylist")
		)
		if group.state == "Playing" then
			syncplay_send_entry(entry :: SessionEntry, "SyncPlayCommand", syncplay_command(group, "Unpause", nil, nil))
		elseif group.state == "Paused" then
			syncplay_send_entry(entry :: SessionEntry, "SyncPlayCommand", syncplay_command(group, "Pause", nil, nil))
		end
	end
	return api.response.empty(204)
end, nil, true)

api.post(P .. "/SyncPlay/Leave", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local entry, entry_err = syncplay_session_entry(ctx, principal)
	if entry_err then
		return entry_err
	end
	syncplay_leave_entry(entry :: SessionEntry, true)
	return api.response.empty(204)
end, nil, true)

api.post(P .. "/SyncPlay/SetNewQueue", function(ctx: ApiContext)
	local principal, entry, group, group_err = syncplay_require_group(ctx)
	if group_err then
		return group_err
	end

	local body = syncplay_body(ctx)
	local queue, parse_err = syncplay_parse_queue_items(principal :: session.Principal, body.PlayingQueue)
	if not queue then
		return base.bad_request(parse_err)
	end
	local playing_position = syncplay_number(body.PlayingItemPosition, 0) or 0
	local start_ticks = syncplay_clamp_position(syncplay_number(body.StartPositionTicks, 0))

	local group_value = group :: SyncPlayGroup
	group_value.queue = queue
	syncplay_set_current_index(group_value, playing_position)
	group_value.position_ticks = start_ticks
	group_value.resume_playing = true
	group_value.state = "Waiting"
	syncplay_set_all_buffering(group_value, true)
	syncplay_broadcast_queue_update(group_value, entry :: SessionEntry, "NewPlaylist")
	syncplay_broadcast_state(group_value, entry :: SessionEntry, "Play")
	return api.response.empty(204)
end, nil, true)

api.post(P .. "/SyncPlay/SetPlaylistItem", function(ctx: ApiContext)
	local _principal, entry, group, group_err = syncplay_require_group(ctx)
	if group_err then
		return group_err
	end

	local body = syncplay_body(ctx)
	local playlist_item_id = syncplay_normalize_guid(body.PlaylistItemId)
	if not playlist_item_id then
		return base.bad_request("missing PlaylistItemId")
	end

	local index = syncplay_find_playlist_item(group :: SyncPlayGroup, playlist_item_id)
	if index then
		local group_value = group :: SyncPlayGroup
		group_value.current_index = index
		group_value.position_ticks = 0
		group_value.resume_playing = true
		group_value.state = "Waiting"
		syncplay_set_all_buffering(group_value, true)
		syncplay_broadcast_queue_update(group_value, entry :: SessionEntry, "SetCurrentItem")
		syncplay_broadcast_state(group_value, entry :: SessionEntry, "SetPlaylistItem")
	end
	return api.response.empty(204)
end, nil, true)

api.post(P .. "/SyncPlay/RemoveFromPlaylist", function(ctx: ApiContext)
	local _principal, entry, group, group_err = syncplay_require_group(ctx)
	if group_err then
		return group_err
	end

	local body = syncplay_body(ctx)
	local clear_playlist = syncplay_bool(body.ClearPlaylist, false)
	local clear_playing_item = syncplay_bool(body.ClearPlayingItem, false)
	local group_value = group :: SyncPlayGroup
	local current_item = syncplay_current_item(group_value)

	if clear_playlist then
		if clear_playing_item or not current_item then
			group_value.queue = {}
			group_value.current_index = -1
			group_value.position_ticks = 0
			group_value.state = "Idle"
		else
			group_value.queue = { current_item }
			group_value.current_index = 0
		end
	else
		local ids, parse_err = syncplay_parse_guid_list(body.PlaylistItemIds)
		if not ids then
			return base.bad_request(parse_err)
		end
		local remove_lookup: { [string]: boolean } = {}
		for _, id in ids do
			remove_lookup[id] = true
		end

		local new_queue: { SyncPlayQueueItem } = {}
		local current_playlist_id = if current_item then current_item.PlaylistItemId else nil
		for _, item in group_value.queue do
			if not remove_lookup[item.PlaylistItemId] then
				table.insert(new_queue, item)
			end
		end
		group_value.queue = new_queue
		if current_playlist_id and not remove_lookup[current_playlist_id] then
			local new_index = syncplay_find_playlist_item(group_value, current_playlist_id)
			group_value.current_index = new_index or -1
		elseif #group_value.queue > 0 then
			group_value.current_index = math.min(group_value.current_index, #group_value.queue - 1)
			if group_value.current_index < 0 then
				group_value.current_index = 0
			end
			group_value.position_ticks = 0
		else
			group_value.current_index = -1
			group_value.position_ticks = 0
			group_value.state = "Idle"
		end
	end

	syncplay_broadcast_queue_update(group_value, entry :: SessionEntry, "RemoveItems")
	if group_value.state == "Idle" then
		syncplay_broadcast_command(group_value, entry :: SessionEntry, "all", "Stop", group_value.position_ticks, nil)
		syncplay_broadcast_state(group_value, entry :: SessionEntry, "Stop")
	end
	return api.response.empty(204)
end, nil, true)

api.post(P .. "/SyncPlay/MovePlaylistItem", function(ctx: ApiContext)
	local _principal, entry, group, group_err = syncplay_require_group(ctx)
	if group_err then
		return group_err
	end

	local body = syncplay_body(ctx)
	local playlist_item_id = syncplay_normalize_guid(body.PlaylistItemId)
	if not playlist_item_id then
		return base.bad_request("missing PlaylistItemId")
	end

	local group_value = group :: SyncPlayGroup
	local old_index = syncplay_find_playlist_item(group_value, playlist_item_id)
	if not old_index then
		return api.response.empty(204)
	end

	local current_item = syncplay_current_item(group_value)
	local item = group_value.queue[old_index + 1]
	table.remove(group_value.queue, old_index + 1)
	local new_index = syncplay_number(body.NewIndex, #group_value.queue) or #group_value.queue
	new_index = math.max(0, math.min(new_index, #group_value.queue))
	table.insert(group_value.queue, new_index + 1, item)
	if current_item then
		group_value.current_index = syncplay_find_playlist_item(group_value, current_item.PlaylistItemId) or -1
	end
	syncplay_broadcast_queue_update(group_value, entry :: SessionEntry, "MoveItem")
	return api.response.empty(204)
end, nil, true)

api.post(P .. "/SyncPlay/Queue", function(ctx: ApiContext)
	local principal, entry, group, group_err = syncplay_require_group(ctx)
	if group_err then
		return group_err
	end

	local body = syncplay_body(ctx)
	local new_items, parse_err = syncplay_parse_queue_items(principal :: session.Principal, body.ItemIds)
	if not new_items then
		return base.bad_request(parse_err)
	end

	local mode = syncplay_enum(body.Mode, SYNCPLAY_QUEUE_MODES, "Queue")
	local group_value = group :: SyncPlayGroup
	if mode == "QueueNext" and group_value.current_index >= 0 then
		local insert_index = group_value.current_index + 1
		for offset, item in new_items do
			table.insert(group_value.queue, insert_index + offset, item)
		end
	else
		for _, item in new_items do
			table.insert(group_value.queue, item)
		end
	end
	syncplay_broadcast_queue_update(
		group_value,
		entry :: SessionEntry,
		if mode == "QueueNext" then "QueueNext" else "Queue"
	)
	return api.response.empty(204)
end, nil, true)

api.post(P .. "/SyncPlay/Unpause", function(ctx: ApiContext)
	local _principal, entry, group, group_err = syncplay_require_group(ctx)
	if group_err then
		return group_err
	end

	local group_value = group :: SyncPlayGroup
	group_value.state = "Playing"
	group_value.resume_playing = true
	syncplay_broadcast_command(group_value, entry :: SessionEntry, "all", "Unpause", group_value.position_ticks, nil)
	syncplay_broadcast_state(group_value, entry :: SessionEntry, "Unpause")
	return api.response.empty(204)
end, nil, true)

api.post(P .. "/SyncPlay/Pause", function(ctx: ApiContext)
	local _principal, entry, group, group_err = syncplay_require_group(ctx)
	if group_err then
		return group_err
	end

	local group_value = group :: SyncPlayGroup
	group_value.state = "Paused"
	group_value.resume_playing = false
	syncplay_broadcast_command(group_value, entry :: SessionEntry, "all", "Pause", group_value.position_ticks, nil)
	syncplay_broadcast_state(group_value, entry :: SessionEntry, "Pause")
	return api.response.empty(204)
end, nil, true)

api.post(P .. "/SyncPlay/Stop", function(ctx: ApiContext)
	local _principal, entry, group, group_err = syncplay_require_group(ctx)
	if group_err then
		return group_err
	end

	local group_value = group :: SyncPlayGroup
	group_value.state = "Idle"
	group_value.resume_playing = false
	group_value.position_ticks = 0
	syncplay_broadcast_command(group_value, entry :: SessionEntry, "all", "Stop", group_value.position_ticks, nil)
	syncplay_broadcast_state(group_value, entry :: SessionEntry, "Stop")
	return api.response.empty(204)
end, nil, true)

api.post(P .. "/SyncPlay/Seek", function(ctx: ApiContext)
	local _principal, entry, group, group_err = syncplay_require_group(ctx)
	if group_err then
		return group_err
	end

	local body = syncplay_body(ctx)
	local position_ticks = syncplay_number(body.PositionTicks, nil)
	if position_ticks == nil then
		return base.bad_request("missing PositionTicks")
	end

	local group_value = group :: SyncPlayGroup
	group_value.position_ticks = syncplay_clamp_position(position_ticks)
	syncplay_broadcast_command(group_value, entry :: SessionEntry, "all", "Seek", group_value.position_ticks, nil)
	syncplay_broadcast_state(group_value, entry :: SessionEntry, "Seek")
	return api.response.empty(204)
end, nil, true)

api.post(P .. "/SyncPlay/Buffering", function(ctx: ApiContext)
	local _principal, entry, group, group_err = syncplay_require_group(ctx)
	if group_err then
		return group_err
	end

	local body = syncplay_body(ctx)
	local group_value = group :: SyncPlayGroup
	local playlist_item_id = syncplay_normalize_guid(body.PlaylistItemId)
	if not syncplay_known_current_item(group_value, playlist_item_id) then
		syncplay_send_group_update_entry(
			entry :: SessionEntry,
			group_value.id,
			"PlayQueue",
			syncplay_play_queue_update(group_value, "SetCurrentItem")
		)
		return api.response.empty(204)
	end

	local participant = group_value.participants[(entry :: SessionEntry).session_id]
	if participant then
		participant.buffering = true
		participant.ready = false
		participant.when = if type(body.When) == "string" then body.When :: string else nil
		participant.position_ticks = syncplay_number(body.PositionTicks, participant.position_ticks)
		participant.is_playing = syncplay_bool(body.IsPlaying, participant.is_playing == true)
		group_value.position_ticks = syncplay_clamp_position(participant.position_ticks or group_value.position_ticks)
		group_value.resume_playing = participant.is_playing == true
	end
	group_value.state = "Waiting"
	syncplay_broadcast_state(group_value, entry :: SessionEntry, "Buffer")
	return api.response.empty(204)
end, nil, true)

api.post(P .. "/SyncPlay/Ready", function(ctx: ApiContext)
	local _principal, entry, group, group_err = syncplay_require_group(ctx)
	if group_err then
		return group_err
	end

	local body = syncplay_body(ctx)
	local group_value = group :: SyncPlayGroup
	local playlist_item_id = syncplay_normalize_guid(body.PlaylistItemId)
	if not syncplay_known_current_item(group_value, playlist_item_id) then
		syncplay_send_group_update_entry(
			entry :: SessionEntry,
			group_value.id,
			"PlayQueue",
			syncplay_play_queue_update(group_value, "SetCurrentItem")
		)
		local participant = group_value.participants[(entry :: SessionEntry).session_id]
		if participant then
			participant.buffering = true
			participant.ready = false
		end
		return api.response.empty(204)
	end

	local participant = group_value.participants[(entry :: SessionEntry).session_id]
	local is_playing = syncplay_bool(body.IsPlaying, group_value.resume_playing)
	local when = if type(body.When) == "string" then body.When :: string else nil
	if participant then
		participant.buffering = false
		participant.ready = true
		participant.when = when
		participant.position_ticks = syncplay_number(body.PositionTicks, participant.position_ticks)
		participant.is_playing = is_playing
		group_value.position_ticks = syncplay_clamp_position(participant.position_ticks or group_value.position_ticks)
	end
	syncplay_transition_if_ready(group_value, entry :: SessionEntry, is_playing, when)
	return api.response.empty(204)
end, nil, true)

api.post(P .. "/SyncPlay/SetIgnoreWait", function(ctx: ApiContext)
	local _principal, entry, group, group_err = syncplay_require_group(ctx)
	if group_err then
		return group_err
	end

	local body = syncplay_body(ctx)
	local group_value = group :: SyncPlayGroup
	local participant = group_value.participants[(entry :: SessionEntry).session_id]
	if participant then
		participant.ignore_wait = syncplay_bool(body.IgnoreWait, false)
	end
	if group_value.state == "Waiting" then
		syncplay_transition_if_ready(group_value, entry :: SessionEntry, nil, nil)
	end
	return api.response.empty(204)
end, nil, true)

api.post(P .. "/SyncPlay/NextItem", function(ctx: ApiContext)
	local _principal, entry, group, group_err = syncplay_require_group(ctx)
	if group_err then
		return group_err
	end

	local body = syncplay_body(ctx)
	local group_value = group :: SyncPlayGroup
	local playlist_item_id = syncplay_normalize_guid(body.PlaylistItemId)
	if not syncplay_known_current_item(group_value, playlist_item_id) then
		return api.response.empty(204)
	end

	if #group_value.queue == 0 then
		return api.response.empty(204)
	end
	if group_value.repeat_mode ~= "RepeatOne" then
		if group_value.current_index < #group_value.queue - 1 then
			group_value.current_index += 1
		elseif group_value.repeat_mode == "RepeatAll" then
			group_value.current_index = 0
		else
			return api.response.empty(204)
		end
	end
	group_value.position_ticks = 0
	group_value.state = "Waiting"
	group_value.resume_playing = true
	syncplay_set_all_buffering(group_value, true)
	syncplay_broadcast_queue_update(group_value, entry :: SessionEntry, "NextItem")
	syncplay_broadcast_state(group_value, entry :: SessionEntry, "NextItem")
	return api.response.empty(204)
end, nil, true)

api.post(P .. "/SyncPlay/PreviousItem", function(ctx: ApiContext)
	local _principal, entry, group, group_err = syncplay_require_group(ctx)
	if group_err then
		return group_err
	end

	local body = syncplay_body(ctx)
	local group_value = group :: SyncPlayGroup
	local playlist_item_id = syncplay_normalize_guid(body.PlaylistItemId)
	if not syncplay_known_current_item(group_value, playlist_item_id) then
		return api.response.empty(204)
	end

	if #group_value.queue == 0 then
		return api.response.empty(204)
	end
	if group_value.repeat_mode ~= "RepeatOne" then
		if group_value.current_index > 0 then
			group_value.current_index -= 1
		elseif group_value.repeat_mode == "RepeatAll" then
			group_value.current_index = #group_value.queue - 1
		else
			return api.response.empty(204)
		end
	end
	group_value.position_ticks = 0
	group_value.state = "Waiting"
	group_value.resume_playing = true
	syncplay_set_all_buffering(group_value, true)
	syncplay_broadcast_queue_update(group_value, entry :: SessionEntry, "PreviousItem")
	syncplay_broadcast_state(group_value, entry :: SessionEntry, "PreviousItem")
	return api.response.empty(204)
end, nil, true)

api.post(P .. "/SyncPlay/SetRepeatMode", function(ctx: ApiContext)
	local _principal, entry, group, group_err = syncplay_require_group(ctx)
	if group_err then
		return group_err
	end

	local body = syncplay_body(ctx)
	local group_value = group :: SyncPlayGroup
	group_value.repeat_mode = syncplay_enum(body.Mode, SYNCPLAY_REPEAT_MODES, group_value.repeat_mode)
	syncplay_broadcast_queue_update(group_value, entry :: SessionEntry, "RepeatMode")
	return api.response.empty(204)
end, nil, true)

api.post(P .. "/SyncPlay/SetShuffleMode", function(ctx: ApiContext)
	local _principal, entry, group, group_err = syncplay_require_group(ctx)
	if group_err then
		return group_err
	end

	local body = syncplay_body(ctx)
	local group_value = group :: SyncPlayGroup
	group_value.shuffle_mode = syncplay_enum(body.Mode, SYNCPLAY_SHUFFLE_MODES, group_value.shuffle_mode)
	syncplay_broadcast_queue_update(group_value, entry :: SessionEntry, "ShuffleMode")
	return api.response.empty(204)
end, nil, true)

api.post(P .. "/SyncPlay/Ping", function(ctx: ApiContext)
	local _principal, entry, group, group_err = syncplay_require_group(ctx)
	if group_err then
		return group_err
	end

	local body = syncplay_body(ctx)
	local participant = (group :: SyncPlayGroup).participants[(entry :: SessionEntry).session_id]
	if participant then
		participant.ping = syncplay_number(body.Ping, participant.ping) or participant.ping
	end
	return api.response.empty(204)
end, nil, true)

-- Jellyfin clients handle Pause/Unpause, not toggle — resolve PlayPause
-- against last-known state. Nil means nothing to toggle; caller should 204.
local function resolve_play_pause(principal: session.Principal, session_id: string): (string?, ApiResponse?)
	local entry, entry_err = resolve_ws_entry_for_dispatch(principal, session_id)
	if entry_err then
		return nil, entry_err
	end
	local target, target_err = resolve_native_target(principal, session_id, entry)
	if target_err then
		return nil, target_err
	end
	local playback_info = if target then target.connection.playback else nil
	if playback_info then
		local resolved = if playback_info.state == "paused" then "Unpause" else "Pause"
		return resolved, nil
	end

	return nil, nil
end

api.post(P .. "/Sessions/{sessionId}/Playing/{command}", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local session_id = if params then params.sessionId else nil
	local command = if params then params.command else nil
	if not session_id or not command then
		return base.bad_request("missing session id or command")
	end

	local request = if ctx then ctx.request else nil
	local query = if request then request.query else nil

	local resolved_command = command
	local seek_ticks: number? = nil
	if command == "PlayPause" then
		local resolved, resolve_err = resolve_play_pause(principal, session_id)
		if resolve_err then
			return resolve_err
		end
		if not resolved then
			-- Nothing to toggle; avoid sending a spurious Pause on a stopped player.
			return api.response.empty(204)
		end
		resolved_command = resolved
	elseif command == "Seek" then
		seek_ticks = if query then tonumber(query.seekPositionTicks or query.SeekPositionTicks) else nil
		if not seek_ticks then
			return base.bad_request("seekPositionTicks is required for Seek")
		end
	end

	local data: JsonObject = { Command = resolved_command }
	if seek_ticks then
		data.SeekPositionTicks = seek_ticks
	end
	local ws_response = dispatch_to_ws(principal, session_id, websocket.encode("Playstate", data))
	if ws_response then
		return ws_response
	end

	local native_action_map: { [string]: string } = {
		Pause = "pause",
		Unpause = "unpause",
		Stop = "stop",
		NextTrack = "next_track",
		PreviousTrack = "previous_track",
		Seek = "seek",
	}
	local action = native_action_map[resolved_command]
	if not action then
		return api.response.empty(204)
	end

	local position_ms: number? = nil
	if seek_ticks then
		position_ms = math.floor(seek_ticks / 10000)
	end
	return dispatch_to_native(principal, session_id, action, position_ms, nil)
end, nil, true)
api.post(P .. "/Sessions/{sessionId}/Command/{command}", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local session_id = if params then params.sessionId else nil
	local command = if params then params.command else nil
	if not session_id or not command then
		return base.bad_request("missing session id or command")
	end

	-- Route-param variant has no body, so Arguments is always empty here;
	-- parameterised commands like SetVolume need POST /Sessions/{id}/Command.
	local ws_response = dispatch_to_ws(
		principal,
		session_id,
		websocket.encode("GeneralCommand", {
			Name = command,
			Arguments = {} :: JsonObject,
			ControllingUserId = base.encode_jellyfin_item_id(principal.user_id),
		})
	)
	if ws_response then
		return ws_response
	end
	return api.response.empty(204)
end, nil, true)
api.post(P .. "/Sessions/{sessionId}/Command", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local session_id = if params then params.sessionId else nil
	if not session_id then
		return base.bad_request("missing session id")
	end

	local request = if ctx then ctx.request else nil
	local request_json = if request then request.json else nil
	if type(request_json) ~= "table" then
		return base.bad_request("invalid command request body")
	end
	local body = request_json :: JsonObject

	local command_name = body.Name
	if type(command_name) ~= "string" then
		return base.bad_request("missing command Name")
	end
	local arguments: JsonObject = {}
	local raw_arguments = body.Arguments
	if type(raw_arguments) == "table" then
		arguments = raw_arguments :: JsonObject
	end

	local ws_response = dispatch_to_ws(
		principal,
		session_id,
		websocket.encode("GeneralCommand", {
			Name = command_name,
			Arguments = arguments,
			ControllingUserId = base.encode_jellyfin_item_id(principal.user_id),
		})
	)
	if ws_response then
		return ws_response
	end

	if command_name == "SetVolume" then
		local volume = tonumber(arguments.Volume)
		if volume then
			return dispatch_to_native(principal, session_id, "set_volume", nil, volume / 100)
		end
	end
	return api.response.empty(204)
end, nil, true)
api.post(P .. "/Sessions/{sessionId}/Playing", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local session_id = if params then params.sessionId else nil
	if not session_id then
		return base.bad_request("missing session id")
	end

	local request = if ctx then ctx.request else nil
	local query = if request then request.query else nil
	local request_json = if request then request.json else nil
	local body: JsonObject = if type(request_json) == "table" then request_json :: JsonObject else {}

	local play_command = base.parse_query_string(query, { "playCommand", "PlayCommand" }, nil)
		or (if type(body.PlayCommand) == "string" then body.PlayCommand :: string else "PlayNow")

	local raw_item_ids: JsonValue = body.ItemIds
	local item_ids: { JsonValue } = {}
	if type(raw_item_ids) == "table" then
		for _, value: JsonValue in raw_item_ids :: { JsonValue } do
			if type(value) == "string" then
				table.insert(item_ids, value)
			end
		end
	else
		local csv = base.parse_query_string(query, { "itemIds", "ItemIds" }, nil)
		if csv then
			for _, part in string.split(csv, ",") do
				local trimmed = base.trim(part)
				if trimmed then
					table.insert(item_ids, trimmed)
				end
			end
		end
	end
	if #item_ids == 0 then
		return base.bad_request("missing ItemIds")
	end

	local data: JsonObject = {
		PlayCommand = play_command,
		ItemIds = item_ids,
	}
	local start_ticks = base.parse_query_int(query, { "startPositionTicks", "StartPositionTicks" }, nil, 0, nil)
		or (if type(body.StartPositionTicks) == "number" then body.StartPositionTicks :: number else nil)
	if start_ticks then
		data.StartPositionTicks = start_ticks
	end
	local start_index = base.parse_query_int(query, { "startIndex", "StartIndex" }, nil, 0, nil)
		or (if type(body.StartIndex) == "number" then body.StartIndex :: number else nil)
	if start_index then
		data.StartIndex = start_index
	end

	local ws_response = dispatch_to_ws(principal, session_id, websocket.encode("Play", data))
	if ws_response then
		return ws_response
	end
	return base.not_found("session not found")
end, nil, true)
api.post(P .. "/Sessions/Playing", function(ctx: ApiContext)
	return session.handle_playback_report_route(ctx, "started")
end, nil, true)
api.post(P .. "/Sessions/Playing/Progress", function(ctx: ApiContext)
	return session.handle_playback_report_route(ctx, "progress")
end, nil, true)
api.post(P .. "/Sessions/Playing/Stopped", function(ctx: ApiContext)
	return session.handle_playback_report_route(ctx, "stopped")
end, nil, true)
api.get(P .. "/Items/{id}/PlaybackInfo", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	local item_id, track, resolve_err = playback.resolve_playback_info_item(ctx)
	if resolve_err then
		return resolve_err or base.bad_request("invalid playback item")
	end

	local request = if ctx then ctx.request else nil
	local play_session_id = playback.resolve_requested_play_session_id(request, nil)
	local response = playback.build_playback_info(item_id, track, play_session_id, nil, nil)
	return api.response.json(200, response)
end, nil, true)
api.get(P .. "/Items/{itemId}/File", function(ctx: ApiContext)
	return session.handle_item_file_stream_request(ctx)
end, nil, true)
api.get(P .. "/Items/{itemId}/Download", function(ctx: ApiContext)
	return session.handle_item_file_stream_request(ctx)
end, nil, true)

-- Returns (value, err) so the handler can distinguish "absent" (value=nil, err=nil)
-- from "present but malformed/negative" (value=nil, err=string). Zero is returned
-- as-is so the server-side policy sees the value and can reject it.
local function extract_integer_param(raw: string?): (number?, string?)
	if type(raw) ~= "string" or #raw == 0 then
		return nil, nil
	end
	local value = tonumber(raw)
	if value == nil or value ~= value then
		return nil, `must be an integer; got "{raw}"`
	end
	if value < 0 then
		return nil, `must not be negative; got {raw}`
	end
	return math.floor(value), nil
end

local function parse_preferred_codec(raw: string?): string?
	if type(raw) ~= "string" then
		return nil
	end
	for token in string.gmatch(raw, "[^,]+") do
		local trimmed = base.trim(token)
		if trimmed and #trimmed > 0 then
			return string.lower(trimmed)
		end
	end
	return nil
end

local function is_hls_transcoding_protocol(raw: string?): boolean
	local value = base.trim(raw)
	return value ~= nil and string.lower(value) == "hls"
end

local function jellyfin_boolean(raw: string?): boolean?
	if type(raw) ~= "string" then
		return nil
	end
	local lower = string.lower(raw)
	if lower == "true" or lower == "1" then
		return true
	end
	if lower == "false" or lower == "0" then
		return false
	end
	return nil
end

api.get(P .. "/Audio/{itemId}/Lyrics", function(ctx: ApiContext): ApiResponse
	local track_id, item, resolve_err = lyrics_protocol.resolve_audio_item(ctx)
	if resolve_err then
		return resolve_err
	end
	if track_id == nil or item == nil then
		return base.not_found("item not found")
	end

	local ok, lyric_info = pcall(lyrics.get, track_id, nil, false)
	if not ok then
		return api.response.empty(500)
	end
	if lyric_info == nil then
		return base.not_found("lyrics not found")
	end

	return api.response.json(200, lyrics_protocol.to_jellyfin_lyric_dto(item, lyric_info :: LyricsInfo))
end, nil, true)
api.post(P .. "/Audio/{itemId}/Lyrics", function(ctx: ApiContext): ApiResponse
	local track_id, item, resolve_err = lyrics_protocol.resolve_audio_item(ctx)
	if resolve_err then
		return resolve_err
	end
	if track_id == nil or item == nil then
		return base.not_found("item not found")
	end

	local body = if ctx and ctx.request then ctx.request.body_raw else nil
	if type(body) ~= "string" or #body == 0 then
		return base.bad_request("No lyrics uploaded")
	end

	local content_type, content_type_err = lyrics_protocol.resolve_upload_content_type(ctx)
	if content_type_err then
		return content_type_err
	end
	if not content_type then
		return base.bad_request("Extension is required on filename")
	end

	local ok, lyric_info = pcall(lyrics.upsert_user_override, track_id, {
		content_type = content_type,
		body = body,
		language = nil,
	})
	if not ok then
		return base.bad_request(tostring(lyric_info))
	end

	return api.response.json(200, lyrics_protocol.to_jellyfin_lyric_dto(item, lyric_info :: LyricsInfo))
end, nil, true)
api.delete(P .. "/Audio/{itemId}/Lyrics", function(ctx: ApiContext): ApiResponse
	local track_id, _item, resolve_err = lyrics_protocol.resolve_audio_item(ctx)
	if resolve_err then
		return resolve_err
	end
	if track_id == nil then
		return base.not_found("item not found")
	end

	local ok = pcall(lyrics.delete_user_override_for_track, track_id)
	if not ok then
		return api.response.empty(500)
	end
	return api.response.empty(204)
end, nil, true)
api.get(P .. "/Audio/{itemId}/RemoteSearch/Lyrics", function(ctx: ApiContext): ApiResponse
	local _track_id, _item, resolve_err = lyrics_protocol.resolve_audio_item(ctx)
	if resolve_err then
		return resolve_err
	end
	return api.response.json(200, {})
end, nil, true)
api.post(P .. "/Audio/{itemId}/RemoteSearch/Lyrics/{lyricId}", function(ctx: ApiContext): ApiResponse
	local _track_id, _item, resolve_err = lyrics_protocol.resolve_audio_item(ctx)
	if resolve_err then
		return resolve_err
	end
	return base.not_found("lyrics not found")
end, nil, true)
api.get(P .. "/Providers/Lyrics/{lyricId}", function(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	return base.not_found("lyrics not found")
end, nil, true)

local function handle_audio_stream_route(ctx: ApiContext): ApiResponse
	local query = if ctx and ctx.request then ctx.request.query else nil
	local request_path = if ctx and ctx.request then ctx.request.path else nil
	local normalized_request_path = if type(request_path) == "string" then string.lower(request_path) else nil
	local raw_static = if query then (query.static or query.Static) else nil
	local raw_container = if query then (query.container or query.Container) else nil
	local static_direct_copy = jellyfin_boolean(raw_static) == true

	local raw_codec = if query then (query.audioCodec or query.AudioCodec) else nil
	local raw_transcoding_container = if query then (query.transcodingContainer or query.TranscodingContainer) else nil
	local raw_max_bitrate = if query then (query.MaxStreamingBitrate or query.maxStreamingBitrate) else nil
	local raw_audio_bitrate = if query then (query.audioBitRate or query.AudioBitRate) else nil
	local raw_sample_rate = if query then (query.audioSampleRate or query.AudioSampleRate) else nil
	local raw_bit_depth = if query then (query.maxAudioBitDepth or query.MaxAudioBitDepth) else nil
	local raw_channels = if query then (query.maxAudioChannels or query.MaxAudioChannels) else nil
	local raw_auto_copy = if query then (query.enableAutoStreamCopy or query.EnableAutoStreamCopy) else nil
	local raw_enable_direct_play = if query then (query.enableDirectPlay or query.EnableDirectPlay) else nil
	local raw_enable_transcoding = if query then (query.enableTranscoding or query.EnableTranscoding) else nil
	local raw_transcoding_protocol = if query then (query.transcodingProtocol or query.TranscodingProtocol) else nil

	local codec = parse_preferred_codec(raw_codec)
	local transcoding_container = parse_preferred_codec(raw_transcoding_container)
	local hls_requested = is_hls_transcoding_protocol(raw_transcoding_protocol)
	-- MaxStreamingBitrate wins over audioBitRate: the former is the broader cap and
	-- what Jellyfin clients send when they want to communicate a bandwidth ceiling.
	local max_bitrate_value, max_bitrate_err = extract_integer_param(raw_max_bitrate)
	if max_bitrate_err then
		return base.bad_request(`MaxStreamingBitrate {max_bitrate_err}`)
	end
	local audio_bitrate_value, audio_bitrate_err = extract_integer_param(raw_audio_bitrate)
	if audio_bitrate_err then
		return base.bad_request(`audioBitRate {audio_bitrate_err}`)
	end
	local bitrate_bps = max_bitrate_value or audio_bitrate_value

	local sample_rate_hz, sample_rate_err = extract_integer_param(raw_sample_rate)
	if sample_rate_err then
		return base.bad_request(`audioSampleRate {sample_rate_err}`)
	end
	local bit_depth, bit_depth_err = extract_integer_param(raw_bit_depth)
	if bit_depth_err then
		return base.bad_request(`maxAudioBitDepth {bit_depth_err}`)
	end
	local channels, channels_err = extract_integer_param(raw_channels)
	if channels_err then
		return base.bad_request(`maxAudioChannels {channels_err}`)
	end
	local forbid_stream_copy = jellyfin_boolean(raw_auto_copy) == false
	local direct_play_enabled = jellyfin_boolean(raw_enable_direct_play) ~= false
	local transcoding_enabled = jellyfin_boolean(raw_enable_transcoding) ~= false

	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local item_id = if params then params.itemId else nil
	if not item_id then
		return base.bad_request("missing item id")
	end

	local track = playback.resolve_track_for_playback(base.decode_jellyfin_item_id(item_id))
	if not track then
		return base.not_found("item not found")
	end
	local track_id = track.db_id
	if track_id == nil or track_id <= 0 then
		return base.not_found("item not found")
	end

	local explicit_stream_path = string.lower(`{P}/Audio/{item_id}/stream`)
	if static_direct_copy and normalized_request_path == explicit_stream_path then
		return session.handle_item_file_stream_request(ctx)
	end

	local media_source = playback.build_audio_media_source(item_id, track)
	if
		playback.audio_supports_direct_play_from_container_csv(
			media_source,
			raw_container,
			bitrate_bps,
			sample_rate_hz,
			bit_depth,
			channels,
			direct_play_enabled,
			forbid_stream_copy
		)
	then
		return session.handle_item_file_stream_request(ctx)
	end

	if not transcoding_enabled then
		if static_direct_copy then
			return session.handle_item_file_stream_request(ctx)
		end
		return api.response.json(415, {
			Message = "transcoding is not supported; request with static=true or supply transcode parameters",
		})
	end

	if hls_requested then
		local options: api.HlsServeOptions = {
			codec = codec or "aac",
			bitrate_bps = bitrate_bps,
			sample_rate_hz = sample_rate_hz,
			channels = channels,
		}
		return api.response.hls_playlist(track_id, options)
	end

	local effective_codec: string? = if codec then codec elseif transcoding_container == nil then "mp3" else nil

	local options: api.TrackServeOptions = {
		format = transcoding_container,
		codec = effective_codec,
		bitrate_bps = bitrate_bps,
		sample_rate_hz = sample_rate_hz,
		channels = channels,
	}

	return api.response.stream_track(track_id, options)
end

api.get(P .. "/Audio/{itemId}/universal", function(ctx: ApiContext)
	return handle_audio_stream_route(ctx)
end, nil, true)
api.head(P .. "/Audio/{itemId}/universal", function(ctx: ApiContext)
	return handle_audio_stream_route(ctx)
end, nil, true)
api.get(P .. "/Audio/{itemId}/stream", function(ctx: ApiContext)
	return handle_audio_stream_route(ctx)
end, nil, true)
api.head(P .. "/Audio/{itemId}/stream", function(ctx: ApiContext)
	return handle_audio_stream_route(ctx)
end, nil, true)

type ResolvedAudioHlsPlaylistRequest = {
	track: Track,
	track_id: number,
	query: ApiQueryParams?,
	options: api.HlsServeOptions,
}

local DEFAULT_AUDIO_HLS_BANDWIDTH_BPS = 192000
local MIN_AUDIO_HLS_AAC_BANDWIDTH_BPS = 32000

local function parse_audio_hls_codec(query: ApiQueryParams?): string
	local raw_codec = if query then (query.audioCodec or query.AudioCodec or query.codec) else nil
	local trimmed_codec = if type(raw_codec) == "string" and #raw_codec > 0 then string.lower(raw_codec) else nil
	return if trimmed_codec then trimmed_codec else "aac"
end

local function encode_hls_query_component(value: string): string
	local encoded = string.gsub(value, "[^%w%-_%.~]", function(char)
		return string.format("%%%02X", string.byte(char))
	end)
	return encoded
end

local function build_audio_hls_master_query_string(query: ApiQueryParams?, codec: string): string
	local keys: { string } = {}
	if query then
		for key in query do
			if key ~= "audioCodec" and key ~= "AudioCodec" and key ~= "codec" then
				table.insert(keys, key)
			end
		end
	end
	table.sort(keys)

	local parts: { string } = {
		`audioCodec={encode_hls_query_component(codec)}`,
	}
	for _, key in keys do
		local value = if query then query[key] else nil
		if type(value) == "string" then
			table.insert(parts, `{encode_hls_query_component(key)}={encode_hls_query_component(value)}`)
		end
	end

	return table.concat(parts, "&")
end

local function codec_string_for_audio_hls(codec: string): string?
	if codec == "aac" then
		return "mp4a.40.2"
	end
	if codec == "flac" then
		return "fLaC"
	end
	if codec == "alac" then
		return "alac"
	end
	return nil
end

local function is_supported_audio_hls_codec(codec: string): boolean
	return codec == "aac" or codec == "alac" or codec == "flac"
end

local function resolve_audio_hls_query_options(query: ApiQueryParams?): (api.HlsServeOptions?, ApiResponse?)
	local codec = parse_audio_hls_codec(query)
	if not is_supported_audio_hls_codec(codec) then
		return nil, base.bad_request("unsupported hls codec; supported values: aac, alac, flac")
	end

	local raw_audio_bitrate = if query then (query.audioBitRate or query.AudioBitRate) else nil
	local raw_max_bitrate = if query then (query.maxStreamingBitrate or query.MaxStreamingBitrate) else nil
	local raw_sample_rate = if query then (query.audioSampleRate or query.AudioSampleRate) else nil
	local raw_channels = if query then (query.maxAudioChannels or query.MaxAudioChannels) else nil

	local requested_audio_bitrate, requested_audio_bitrate_err = extract_integer_param(raw_audio_bitrate)
	if requested_audio_bitrate_err then
		return nil, base.bad_request(`audioBitRate {requested_audio_bitrate_err}`)
	end
	local requested_max_bitrate, requested_max_bitrate_err = extract_integer_param(raw_max_bitrate)
	if requested_max_bitrate_err then
		return nil, base.bad_request(`MaxStreamingBitrate {requested_max_bitrate_err}`)
	end
	local sample_rate_hz, sample_rate_err = extract_integer_param(raw_sample_rate)
	if sample_rate_err then
		return nil, base.bad_request(`audioSampleRate {sample_rate_err}`)
	end
	local channels, channels_err = extract_integer_param(raw_channels)
	if channels_err then
		return nil, base.bad_request(`maxAudioChannels {channels_err}`)
	end

	return {
		codec = codec,
		bitrate_bps = requested_max_bitrate or requested_audio_bitrate,
		sample_rate_hz = sample_rate_hz,
		channels = channels,
	},
		nil
end

local function resolve_audio_hls_variant_bandwidth(track: Track, options: api.HlsServeOptions): number
	local codec = options.codec or "aac"
	local requested_bitrate = options.bitrate_bps
	local track_bitrate = if track.bitrate_bps and track.bitrate_bps > 0 then track.bitrate_bps else nil

	if codec == "aac" then
		if requested_bitrate ~= nil then
			if track_bitrate ~= nil and requested_bitrate > track_bitrate then
				return DEFAULT_AUDIO_HLS_BANDWIDTH_BPS
			end
			if requested_bitrate < MIN_AUDIO_HLS_AAC_BANDWIDTH_BPS then
				return MIN_AUDIO_HLS_AAC_BANDWIDTH_BPS
			end
			return requested_bitrate
		end
		return DEFAULT_AUDIO_HLS_BANDWIDTH_BPS
	end

	return track_bitrate or DEFAULT_AUDIO_HLS_BANDWIDTH_BPS
end

local function build_audio_hls_master_playlist(
	track: Track,
	query: ApiQueryParams?,
	options: api.HlsServeOptions
): string
	local codec = options.codec or "aac"
	local bandwidth = resolve_audio_hls_variant_bandwidth(track, options)
	local codec_string = codec_string_for_audio_hls(codec)
	local stream_inf = `#EXT-X-STREAM-INF:BANDWIDTH={bandwidth},AVERAGE-BANDWIDTH={bandwidth}`
	if codec_string then
		stream_inf ..= `,CODECS="{codec_string}"`
	end

	local variant_uri = `main.m3u8?{build_audio_hls_master_query_string(query, codec)}`
	return table.concat({
		"#EXTM3U",
		stream_inf,
		variant_uri,
		"",
	}, "\n")
end

local function resolve_audio_hls_playlist_request(ctx: ApiContext): (ResolvedAudioHlsPlaylistRequest?, ApiResponse?)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return nil, session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return nil, session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local item_id = if params then params.itemId else nil
	if not item_id then
		return nil, base.bad_request("missing item id")
	end

	local track = playback.resolve_track_for_playback(base.decode_jellyfin_item_id(item_id))
	if not track then
		return nil, base.not_found("item not found")
	end
	local track_id = track.db_id
	if track_id == nil or track_id <= 0 then
		return nil, base.not_found("item not found")
	end

	local query = if ctx and ctx.request then ctx.request.query else nil
	local options, options_err = resolve_audio_hls_query_options(query)
	if options == nil then
		return nil, options_err or base.bad_request("invalid hls options")
	end

	return {
		track = track,
		track_id = track_id,
		query = query,
		options = options,
	}, nil
end

local function handle_audio_hls_master_playlist_route(ctx: ApiContext): ApiResponse
	local request, err = resolve_audio_hls_playlist_request(ctx)
	if request == nil then
		return err or base.bad_request("invalid hls request")
	end

	return api.response.text(200, build_audio_hls_master_playlist(request.track, request.query, request.options), {
		["content-type"] = "application/x-mpegurl",
	})
end

local function handle_audio_hls_main_playlist_route(ctx: ApiContext): ApiResponse
	local request, err = resolve_audio_hls_playlist_request(ctx)
	if request == nil then
		return err or base.bad_request("invalid hls request")
	end

	return api.response.hls_playlist(request.track_id, request.options)
end

api.get(P .. "/Audio/{itemId}/master.m3u8", handle_audio_hls_master_playlist_route, nil, true)
api.head(P .. "/Audio/{itemId}/master.m3u8", handle_audio_hls_master_playlist_route, nil, true)
api.get(P .. "/Audio/{itemId}/main.m3u8", handle_audio_hls_main_playlist_route, nil, true)
api.head(P .. "/Audio/{itemId}/main.m3u8", handle_audio_hls_main_playlist_route, nil, true)
api.post(P .. "/Items/{id}/PlaybackInfo", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local request = if ctx then ctx.request else nil
	local request_json = if request then request.json else nil
	if request_json ~= nil and type(request_json) ~= "table" then
		return base.bad_request("invalid playback info request body")
	end
	local request_object = if type(request_json) == "table" then request_json :: JsonObject else nil

	local requested_user_id = if request_object then request_object.UserId else nil
	if type(requested_user_id) == "string" and base.decode_jellyfin_item_id(requested_user_id) ~= principal.user_id then
		return session.unauthorized()
	end

	local item_id, track, resolve_err = playback.resolve_playback_info_item(ctx)
	if resolve_err then
		return resolve_err or base.bad_request("invalid playback item")
	end

	local playback_request = if request_object then request_object :: PlaybackPayload else nil
	local play_session_id = playback.resolve_requested_play_session_id(request, playback_request)
	local requested_media_source_id = playback.resolve_requested_media_source_id(request, request_object)
	local response =
		playback.build_playback_info(item_id, track, play_session_id, request_object, requested_media_source_id)
	return api.response.json(200, response)
end, nil, true)

api.get(P .. "/Items/Latest", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end
	return respond_with_latest_items(ctx, principal)
end, nil, true)
api.get(P .. "/Users/{userId}/Items/Resume", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return api.response.json(200, empty_query_result())
end, nil, true)
api.get(P .. "/UserItems/Resume", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return api.response.json(200, empty_query_result())
end, nil, true)
api.get(P .. "/Shows/NextUp", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return api.response.json(200, empty_query_result())
end, nil, true)
api.get(P .. "/Items/Suggestions", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return api.response.json(200, empty_query_result())
end, nil, true)
api.get(P .. "/Users/{userId}/Suggestions", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return api.response.json(200, empty_query_result())
end, nil, true)
api.get(P .. "/Items/Counts", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	local query = if ctx and ctx.request then ctx.request.query else nil
	local options = catalog.coerce_items_query(query)
	local is_favorite = options.is_favorite == true
	return api.response.json(200, {
		MovieCount = 0,
		SeriesCount = 0,
		EpisodeCount = 0,
		AlbumCount = item_count_for(principal, "MusicAlbum", is_favorite),
		SongCount = item_count_for(principal, "Audio", is_favorite),
		MusicVideoCount = 0,
		BoxSetCount = 0,
		BookCount = 0,
	})
end, nil, true)
api.get(P .. "/Items/Filters", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return api.response.json(200, {
		Genres = {},
		Tags = {},
		OfficialRatings = {},
		Years = {},
	})
end, nil, true)
api.get(P .. "/Items/Filters2", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return api.response.json(200, {
		Genres = {},
		Tags = {},
	})
end, nil, true)
api.get(P .. "/Items/{id}/Ancestors", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local raw_id = if params then params.id else nil
	if not raw_id then
		return base.bad_request("missing item id")
	end
	local item_id = base.decode_jellyfin_item_id(raw_id)
	if not item_id then
		return base.not_found("item not found")
	end

	local chain, item_found = catalog.ancestors_chain(item_id, principal.user_id)
	if not item_found then
		return base.not_found("item not found")
	end

	local result: { api.JsonValue } = {}
	for _, parent in chain do
		table.insert(result, catalog.item_to_json(parent))
	end
	return api.response.json(200, result)
end, nil, true)
api.get(P .. "/Items/{itemId}/Intros", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	return api.response.json(200, empty_query_result())
end, nil, true)
local function handle_empty_item_array_route(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return api.response.json(200, {})
end

api.get(P .. "/Items/{itemId}/LocalTrailers", handle_empty_item_array_route, nil, true)
api.get(P .. "/Users/{userId}/Items/{itemId}/LocalTrailers", handle_empty_item_array_route, nil, true)
api.get(P .. "/Items/{itemId}/SpecialFeatures", handle_empty_item_array_route, nil, true)
api.get(P .. "/Users/{userId}/Items/{itemId}/SpecialFeatures", handle_empty_item_array_route, nil, true)
api.get(P .. "/Artists/{name}", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local name = if params then params.name else nil
	if not name or name == "" then
		return base.bad_request("missing artist name")
	end

	local found = catalog.find_artist_by_name(name, principal.user_id)
	if not found then
		return base.not_found("artist not found")
	end
	return api.response.json(200, catalog.item_to_json(found))
end, nil, true)
local function handle_genre_by_name_route(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local name = if params then (params.name or params.genreName) else nil
	if not name or name == "" then
		return base.bad_request("missing genre name")
	end

	local found = catalog.find_genre_by_name(name, principal.user_id)
	if not found then
		return base.not_found("genre not found")
	end
	return api.response.json(200, catalog.item_to_json(found))
end

api.get(P .. "/Genres/{genreName}", handle_genre_by_name_route, nil, true)
api.get(P .. "/MusicGenres/{genreName}", handle_genre_by_name_route, nil, true)
api.get(P .. "/GetUtcTime", function(): ApiResponse
	return statics.utc_time_response()
end, "public", true)
api.get(P .. "/Localization/Cultures", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	return statics.cultures_response()
end, nil, true)
api.get(P .. "/Localization/Countries", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	return statics.countries_response()
end, nil, true)
api.get(P .. "/Localization/ParentalRatings", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	return statics.parental_ratings_response()
end, nil, true)
api.get(P .. "/Branding/Configuration", function(): ApiResponse
	return statics.branding_configuration_response()
end, "public", true)
api.get(P .. "/Branding/Css", function(): ApiResponse
	return statics.branding_css_response()
end, "public", true)
api.get(P .. "/Branding/Css.css", function(): ApiResponse
	return statics.branding_css_response()
end, "public", true)
api.get(P .. "/Devices/Info", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	return session.devices_info_response(ctx, principal.user_id)
end, nil, true)
api.get(P .. "/Devices/Options", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	return preferences.device_options_get(ctx, principal.user_id)
end, nil, true)
api.post(P .. "/Devices/Options", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	return preferences.device_options_set(ctx, principal.user_id)
end, nil, true)
api.get(P .. "/DisplayPreferences/{displayPreferencesId}", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local prefs_id = if params then params.displayPreferencesId else nil
	if not prefs_id or prefs_id == "" then
		return base.bad_request("missing displayPreferencesId")
	end
	return preferences.display_prefs_get(ctx, principal.user_id, prefs_id)
end, nil, true)
api.post(P .. "/DisplayPreferences/{displayPreferencesId}", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local prefs_id = if params then params.displayPreferencesId else nil
	if not prefs_id or prefs_id == "" then
		return base.bad_request("missing displayPreferencesId")
	end
	return preferences.display_prefs_set(ctx, principal.user_id, prefs_id)
end, nil, true)
local function handle_missing_image_route(ctx: ApiContext): ApiResponse
	return statics.image_not_found_response()
end

local NON_PRIMARY_IMAGE_TYPES = {
	"Backdrop",
	"Logo",
	"Thumb",
	"Art",
	"Banner",
	"Disc",
	"Box",
	"BoxRear",
	"Menu",
	"Chapter",
	"Screenshot",
	"Profile",
}
for _, image_type in NON_PRIMARY_IMAGE_TYPES do
	api.get(P .. "/Items/{imageId}/Images/" .. image_type, handle_missing_image_route, nil, true)
	api.head(P .. "/Items/{imageId}/Images/" .. image_type, handle_missing_image_route, nil, true)
	api.get(P .. "/Items/{imageId}/Images/" .. image_type .. "/{imageIndex}", handle_missing_image_route, nil, true)
	api.head(P .. "/Items/{imageId}/Images/" .. image_type .. "/{imageIndex}", handle_missing_image_route, nil, true)
end

discovery.start(server_info)

local JELLYFIN_SESSIONS_BROADCAST_SECONDS = 2
local JELLYFIN_SESSIONS_BROADCAST_MAX_ERRORS = 5

-- One list per visibility bucket, shared across matching subscribers. Outer
-- xpcall covers task.wait; the consecutive-error counter stops a tight spin if
-- scheduling itself is the fault.
task.spawn(function()
	local consecutive_errors = 0
	while true do
		local tick_ok = xpcall(function()
			task.wait(JELLYFIN_SESSIONS_BROADCAST_SECONDS)
			local by_user: { [number]: { username: string, senders: { api.ApiWebSocketSender } } } = {}
			local admin_senders: { api.ApiWebSocketSender } = {}
			local admin_fallback: UserIdentity? = nil
			for _, entry in sessions do
				if not entry.subscribed_sessions or entry.sender:is_closed() then
					continue
				end
				if entry.is_admin then
					table.insert(admin_senders, entry.sender)
					if not admin_fallback then
						admin_fallback = {
							user_id = entry.user_id,
							username = tostring(entry.info.UserName or ""),
						}
					end
					continue
				end
				local bucket = by_user[entry.user_id]
				if not bucket then
					bucket = { username = tostring(entry.info.UserName or ""), senders = {} }
					by_user[entry.user_id] = bucket
				end
				table.insert(bucket.senders, entry.sender)
			end
			for user_id, bucket in by_user do
				-- One user's render failure must not strand the rest; only
				-- scheduler-level throws reach the outer counter.
				xpcall(function()
					local connections = playback_sessions.list_connections(user_id)
					local list = build_sessions_dto_list(user_id, bucket.username, connections)
					local text = websocket.encode("Sessions", list, nil)
					for _, sender in bucket.senders do
						pcall(function()
							sender:send(text)
						end)
					end
				end, function(err)
					print(`sessions broadcast failed for user {user_id}: {tostring(err)}`)
				end)
			end
			if #admin_senders > 0 then
				xpcall(function()
					local list = list_all_sessions(admin_fallback, false)
					local text = websocket.encode("Sessions", list, nil)
					for _, sender in admin_senders do
						pcall(function()
							sender:send(text)
						end)
					end
				end, function(err)
					print(`sessions broadcast failed for admins: {tostring(err)}`)
				end)
			end
		end, function(err)
			print(`sessions broadcast tick failed: {tostring(err)}`)
		end)
		if tick_ok then
			consecutive_errors = 0
		else
			consecutive_errors += 1
			if consecutive_errors >= JELLYFIN_SESSIONS_BROADCAST_MAX_ERRORS then
				print(`sessions broadcast disabled after {consecutive_errors} consecutive failures`)
				break
			end
		end
	end
end)

print("loaded Jellyfin plugin")
