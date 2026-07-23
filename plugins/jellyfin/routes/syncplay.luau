--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")

local crypt = require("@harmony/crypt")

local constants = require("../constants")
local item_ids = require("../protocol/items/identifiers")
local playback = require("../protocol/items/playback")
local requests = require("../protocol/items/requests")
local responses = require("../protocol/items/responses")
local session = require("../protocol/session")
local websocket = require("../protocol/websocket")

local P = constants.ROUTE_PREFIX

type ApiContext = api.ApiContext
type ApiResponse = api.ApiResponse
type JsonValue = api.JsonValue
type JsonObject = { [string]: JsonValue }

export type SessionEntry = {
	sender: api.ApiWebSocketSender,
	session_id: string,
	user_id: number,
	is_admin: boolean,
	device_id: string,
	auth_session_id: number?,
	info: JsonObject,
	capabilities: JsonObject?,
	full_capability_keys: { [string]: boolean },
	subscribed_sessions: boolean,
	last_activity: string,
	last_playback_event_s: number?,
}

type OwnedSessionResolver = (string?, session.Principal) -> SessionEntry?
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

local syncplay_groups: { [string]: SyncPlayGroup } = {}
local syncplay_session_to_group: { [string]: SyncPlayGroup } = {}
local syncplay_active_users: { [number]: number } = {}
local syncplay_guid_counter = 0
local sessions: { [string]: SessionEntry } = {}
local get_owned_session: OwnedSessionResolver = function()
	return nil
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
	local text = requests.trim(value)
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
		local parsed = tonumber(requests.trim(value) or "")
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
		local trimmed = requests.trim(value)
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
	local db_id = item_ids.decode_jellyfin_item_id(item_id)
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
		return nil, responses.bad_request("missing Client/DeviceId")
	end

	local session_id = session.build_session_id(principal, identity)
	local entry = get_owned_session(session_id, principal)
	if not entry then
		return nil, responses.not_found("session not found")
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

local module = {}

function module.leave_entry(entry: SessionEntry, notify: boolean)
	syncplay_leave_entry(entry, notify)
end

function module.disconnect_entry(entry: SessionEntry)
	if syncplay_session_to_group[entry.session_id] then
		syncplay_leave_entry(entry, true)
	end
end

function module.register(session_entries: { [string]: SessionEntry }, owned_session_resolver: OwnedSessionResolver)
	sessions = session_entries
	get_owned_session = owned_session_resolver
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
			return responses.not_found("syncplay group not found")
		end
		local group = syncplay_groups[group_id]
		if not group or not syncplay_group_accessible(principal, group) then
			return responses.not_found("syncplay group not found")
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
		local group_name = if type(body.GroupName) == "string" then requests.trim(body.GroupName :: string) else nil
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
			return responses.bad_request("missing GroupId")
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
				syncplay_send_entry(
					entry :: SessionEntry,
					"SyncPlayCommand",
					syncplay_command(group, "Unpause", nil, nil)
				)
			elseif group.state == "Paused" then
				syncplay_send_entry(
					entry :: SessionEntry,
					"SyncPlayCommand",
					syncplay_command(group, "Pause", nil, nil)
				)
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
			return responses.bad_request(parse_err)
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
			return responses.bad_request("missing PlaylistItemId")
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
				return responses.bad_request(parse_err)
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
			syncplay_broadcast_command(
				group_value,
				entry :: SessionEntry,
				"all",
				"Stop",
				group_value.position_ticks,
				nil
			)
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
			return responses.bad_request("missing PlaylistItemId")
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
			return responses.bad_request(parse_err)
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
		syncplay_broadcast_command(
			group_value,
			entry :: SessionEntry,
			"all",
			"Unpause",
			group_value.position_ticks,
			nil
		)
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
			return responses.bad_request("missing PositionTicks")
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
			group_value.position_ticks =
				syncplay_clamp_position(participant.position_ticks or group_value.position_ticks)
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
			group_value.position_ticks =
				syncplay_clamp_position(participant.position_ticks or group_value.position_ticks)
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
end

return module
