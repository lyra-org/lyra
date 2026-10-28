--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")

local crypt = require("@harmony/crypt")
local task = require("@harmony/task")

local active_sessions = require("../protocol/active_sessions")
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

type SessionEntry = active_sessions.SessionEntry

type OwnedSessionResolver = (string?, session.Principal) -> SessionEntry?
type SyncPlayQueueItem = {
	ItemId: string,
	PlaylistItemId: string,
}

type SyncPlayParticipant = {
	session_id: string,
	user_id: string,
	username: string,
	buffering: boolean,
	ready: boolean,
	ignore_wait: boolean,
	ping: number,
}

type SyncPlayGroup = {
	id: string,
	name: string,
	state: string,
	participants: { [string]: SyncPlayParticipant },
	queue: { SyncPlayQueueItem },
	current_index: number,
	-- The group's clock: the position playback last started or stopped at, and when, in unix
	-- milliseconds. A start is scheduled ahead. Read the position with `syncplay_position_ticks`.
	position_ticks: number,
	last_activity_ms: number,
	-- How long the current item runs, when known; a position a member asks for can't pass it.
	run_time_ticks: number?,
	repeat_mode: string,
	shuffle_mode: string,
	resume_playing: boolean,
	-- Gives up waiting for the members that haven't reported ready, while the group waits.
	wait_timer: thread?,
	last_updated_at: string,
	last_queue_update: string,
}

local syncplay_groups: { [string]: SyncPlayGroup } = {}
local syncplay_session_to_group: { [string]: SyncPlayGroup } = {}
local syncplay_active_users: { [string]: number } = {}
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
local SYNCPLAY_DEFAULT_PING = 500
local SYNCPLAY_MAX_PING = 10000
-- How far, in milliseconds, a client's reported time may be off before it is ignored.
local SYNCPLAY_TIME_SYNC_OFFSET = 2000
local TICKS_PER_MILLISECOND = 10000

local module = {}

-- How long, in seconds, a group waits for its members to report ready.
module.group_wait_seconds = 30

-- os.time() counts whole seconds, so the wall clock in milliseconds follows the monotonic
-- os.clock() at the latest offset the whole-second readings allow.
local syncplay_clock_offset = -math.huge

-- The wall clock in unix milliseconds, which tests may replace.
function module.now_ms(): number
	local clock = os.clock()
	local earliest = os.time() - clock
	if syncplay_clock_offset < earliest or syncplay_clock_offset >= earliest + 1 then
		syncplay_clock_offset = earliest
	end
	return math.floor((clock + syncplay_clock_offset) * 1000)
end

local function syncplay_now_ms(): number
	return module.now_ms()
end

local function syncplay_timestamp(unix_ms: number): string
	unix_ms = math.floor(unix_ms)
	local seconds = math.floor(unix_ms / 1000)
	local whole = string.sub(os.date("!%Y-%m-%dT%H:%M:%SZ", seconds) :: string, 1, -2)
	return `{whole}.{string.format("%03d", unix_ms - seconds * 1000)}Z`
end

local function syncplay_now(): string
	return syncplay_timestamp(syncplay_now_ms())
end

-- Days from 1970-01-01 to a proleptic Gregorian date.
local function syncplay_days_from_civil(year: number, month: number, day: number): number
	if month <= 2 then
		year -= 1
	end
	local era = math.floor(year / 400)
	local year_of_era = year - era * 400
	local day_of_year = math.floor((153 * ((month + 9) % 12) + 2) / 5) + day - 1
	local day_of_era = year_of_era * 365 + math.floor(year_of_era / 4) - math.floor(year_of_era / 100) + day_of_year
	return era * 146097 + day_of_era - 719468
end

-- A client's ISO 8601 time, in unix milliseconds.
function module.parse_timestamp(value: JsonValue?): number?
	if type(value) ~= "string" then
		return nil
	end
	local year, month, day, hour, minute, second, fraction, zone =
		string.match(value, "^(%d%d%d%d)-(%d%d)-(%d%d)T(%d%d):(%d%d):(%d%d)(%.?%d*)(.*)$")
	if not year then
		return nil
	end
	local zone_text = zone :: string
	local fraction_text = fraction :: string
	local offset_minutes = 0
	if zone_text ~= "" and zone_text ~= "Z" then
		local sign, zone_hour, zone_minute = string.match(zone_text, "^([+-])(%d%d):?(%d%d)$")
		if not sign then
			return nil
		end
		offset_minutes = (tonumber(zone_hour) :: number) * 60 + (tonumber(zone_minute) :: number)
		if sign == "-" then
			offset_minutes = -offset_minutes
		end
	end
	local days = syncplay_days_from_civil(tonumber(year) :: number, tonumber(month) :: number, tonumber(day) :: number)
	local seconds = days * 86400
		+ (tonumber(hour) :: number) * 3600
		+ (tonumber(minute) :: number) * 60
		+ (tonumber(second) :: number)
		- offset_minutes * 60
	local fraction_ms = if #fraction_text > 1 then math.floor((tonumber("0" .. fraction_text) :: number) * 1000) else 0
	return seconds * 1000 + fraction_ms
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
			or (filter == "ready" and not participant.buffering)
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
	from_session_id: string,
	filter: string,
	update_type: string,
	data: JsonValue
)
	syncplay_send_filter(
		group,
		from_session_id,
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

-- Where the group's playback is: the clock's position, advanced while playing by the time played
-- since it started. A start still scheduled ahead hasn't advanced it.
local function syncplay_position_ticks(group: SyncPlayGroup, now_ms: number?): number
	if group.state ~= "Playing" then
		return group.position_ticks
	end
	local elapsed_ms = math.max((now_ms or syncplay_now_ms()) - group.last_activity_ms, 0)
	return group.position_ticks + elapsed_ms * TICKS_PER_MILLISECOND
end

-- Stops the clock where playback is.
local function syncplay_settle(group: SyncPlayGroup)
	local now_ms = syncplay_now_ms()
	group.position_ticks = syncplay_position_ticks(group, now_ms)
	group.last_activity_ms = now_ms
end

-- Sets the clock to `position_ticks` from now.
local function syncplay_set_position(group: SyncPlayGroup, position_ticks: number)
	group.position_ticks = position_ticks
	group.last_activity_ms = syncplay_now_ms()
end

-- A position a member asks for, within the current item. Without a known run time, as for an
-- item Lyra hasn't read the length of, only the start bounds it.
local function syncplay_sanitize_position(group: SyncPlayGroup, position_ticks: number?): number
	return math.clamp(position_ticks or 0, 0, group.run_time_ticks or math.huge)
end

local function syncplay_play_queue_update(group: SyncPlayGroup, reason: string): JsonObject
	return {
		Reason = reason,
		LastUpdate = group.last_queue_update,
		Playlist = syncplay_playlist_json(group),
		PlayingItemIndex = group.current_index,
		StartPositionTicks = syncplay_position_ticks(group),
		IsPlaying = group.state == "Playing",
		ShuffleMode = group.shuffle_mode,
		RepeatMode = group.repeat_mode,
	}
end

local function syncplay_broadcast_queue_update(group: SyncPlayGroup, from_session_id: string, reason: string)
	syncplay_mark_queue_updated(group)
	syncplay_broadcast_group_update(
		group,
		from_session_id,
		"all",
		"PlayQueue",
		syncplay_play_queue_update(group, reason)
	)
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

-- How long the current item runs, in ticks, when Lyra knows it.
local function syncplay_current_run_time(group: SyncPlayGroup): number?
	local item = syncplay_current_item(group)
	local track = if item
		then playback.resolve_track_for_playback(item_ids.decode_jellyfin_item_id(item.ItemId))
		else nil
	local duration_ms = if track then track.duration_ms else nil
	return if duration_ms then duration_ms * TICKS_PER_MILLISECOND else nil
end

-- Starts the current item over, as a new current item does.
local function syncplay_restart_item(group: SyncPlayGroup)
	group.run_time_ticks = syncplay_current_run_time(group)
	syncplay_set_position(group, 0)
end

-- As upstream, a command carries the clock itself: members play from its position at its time,
-- unless `when_ms` schedules the command for another time.
local function syncplay_command(group: SyncPlayGroup, command: string, when_ms: number?): JsonObject
	return {
		GroupId = group.id,
		PlaylistItemId = syncplay_current_playlist_item_id(group),
		When = syncplay_timestamp(when_ms or group.last_activity_ms),
		Command = command,
		PositionTicks = group.position_ticks,
		EmittedAt = syncplay_timestamp(syncplay_now_ms()),
	}
end

local function syncplay_broadcast_command(
	group: SyncPlayGroup,
	from_session_id: string,
	filter: string,
	command: string
)
	syncplay_send_filter(group, from_session_id, filter, "SyncPlayCommand", syncplay_command(group, command))
end

local function syncplay_broadcast_state(group: SyncPlayGroup, from_session_id: string, reason: string)
	syncplay_mark_updated(group)
	syncplay_broadcast_group_update(group, from_session_id, "all", "StateUpdate", {
		State = group.state,
		Reason = reason,
	})
end

local function syncplay_group_ready(group: SyncPlayGroup): boolean
	for _, participant in group.participants do
		if not participant.ignore_wait and participant.buffering then
			return false
		end
	end
	return true
end

local function syncplay_cancel_wait(group: SyncPlayGroup)
	local timer = group.wait_timer
	group.wait_timer = nil
	if timer then
		task.cancel(timer)
	end
end

local function syncplay_set_state(group: SyncPlayGroup, state: string)
	group.state = state
	if state ~= "Waiting" then
		syncplay_cancel_wait(group)
	end
end

local function syncplay_highest_ping(group: SyncPlayGroup): number
	local highest: number? = nil
	for _, participant in group.participants do
		highest = math.max(highest or participant.ping, participant.ping)
	end
	return highest or SYNCPLAY_DEFAULT_PING
end

-- As upstream, playback starts far enough ahead for the command to reach every member.
local function syncplay_resume_delay_ms(group: SyncPlayGroup): number
	return math.max(syncplay_highest_ping(group) * 2, SYNCPLAY_DEFAULT_PING)
end

local function syncplay_unpause(group: SyncPlayGroup, from_session_id: string)
	syncplay_settle(group)
	syncplay_set_state(group, "Playing")
	group.resume_playing = true
	group.last_activity_ms += syncplay_resume_delay_ms(group)
	syncplay_broadcast_command(group, from_session_id, "all", "Unpause")
	syncplay_broadcast_state(group, from_session_id, "Unpause")
end

local function syncplay_pause(group: SyncPlayGroup, from_session_id: string)
	syncplay_settle(group)
	syncplay_set_state(group, "Paused")
	group.resume_playing = false
	syncplay_broadcast_command(group, from_session_id, "all", "Pause")
	syncplay_broadcast_state(group, from_session_id, "Pause")
end

local syncplay_wait_timed_out: (group: SyncPlayGroup) -> ()

-- Keeps the wait timer running while the group waits for a member. A running timer covers the
-- whole waiting period, so members reporting buffering again don't push it back; only resetting
-- every member starts a new period.
local function syncplay_update_wait(group: SyncPlayGroup, new_period: boolean)
	if group.state ~= "Waiting" or syncplay_group_ready(group) then
		syncplay_cancel_wait(group)
		return
	end
	if group.wait_timer ~= nil and not new_period then
		return
	end
	syncplay_cancel_wait(group)
	local timer: thread
	timer = task.delay(module.group_wait_seconds, function()
		if group.wait_timer == timer then
			group.wait_timer = nil
			syncplay_wait_timed_out(group)
		end
	end)
	group.wait_timer = timer
end

local function syncplay_set_all_buffering(group: SyncPlayGroup, buffering: boolean)
	for _, participant in group.participants do
		participant.buffering = buffering
		participant.ready = not buffering
	end
	syncplay_update_wait(group, buffering)
end

-- As upstream, a group that waited too long stops waiting for the members that aren't ready: it
-- starts playing when it was resuming, and pauses where the wait started otherwise.
function syncplay_wait_timed_out(group: SyncPlayGroup)
	if syncplay_groups[group.id] ~= group or group.state ~= "Waiting" then
		return
	end
	local blocking: string? = nil
	for session_id, participant in group.participants do
		if participant.buffering and not participant.ignore_wait then
			blocking = session_id
			break
		end
	end
	if not blocking then
		return
	end
	syncplay_set_all_buffering(group, false)
	if group.resume_playing then
		syncplay_unpause(group, blocking)
	else
		syncplay_pause(group, blocking)
	end
end

local function syncplay_increment_active_user(user_id: string)
	syncplay_active_users[user_id] = (syncplay_active_users[user_id] or 0) + 1
end

local function syncplay_decrement_active_user(user_id: string)
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
		ping = SYNCPLAY_DEFAULT_PING,
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
	syncplay_update_wait(group, false)

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
		syncplay_broadcast_group_update(group, entry.session_id, "except", "UserLeft", username)
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
		last_activity_ms = syncplay_now_ms(),
		run_time_ticks = nil,
		resume_playing = false,
		last_updated_at = now,
		last_queue_update = now,
	}
	syncplay_groups[id] = group
	syncplay_add_participant(group, entry)
	syncplay_send_group_update_entry(entry, group.id, "GroupJoined", syncplay_group_info(group))
	return group
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

-- Ends the wait once every member is ready. As upstream, when the member that was waited for
-- is behind the group, the others resume as it catches up; otherwise everyone resumes together.
local function syncplay_transition_if_ready(
	group: SyncPlayGroup,
	from_session_id: string,
	is_playing: boolean?,
	lag_ms: number?
)
	if not syncplay_group_ready(group) then
		syncplay_broadcast_state(group, from_session_id, "Ready")
		return
	end

	if is_playing ~= true and not group.resume_playing then
		syncplay_settle(group)
		syncplay_set_state(group, "Paused")
		syncplay_broadcast_command(group, from_session_id, "all", "Pause")
		syncplay_broadcast_state(group, from_session_id, "Ready")
		return
	end

	local delay_ms = syncplay_resume_delay_ms(group)
	local filter = "all"
	if lag_ms and lag_ms > syncplay_highest_ping(group) * 2 then
		delay_ms = lag_ms
		if is_playing then
			filter = "except"
		end
	end
	syncplay_settle(group)
	syncplay_set_state(group, "Playing")
	group.last_activity_ms += delay_ms
	syncplay_broadcast_command(group, from_session_id, filter, "Unpause")
	syncplay_broadcast_state(group, from_session_id, "Ready")
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

function module.leave_entry(entry: SessionEntry, notify: boolean)
	syncplay_leave_entry(entry, notify)
end

function module.disconnect_entry(entry: SessionEntry)
	if syncplay_session_to_group[entry.session_id] then
		syncplay_leave_entry(entry, true)
	end
end

-- The SyncPlay route handlers, by method and path below the route prefix.
module.handlers = {} :: { [string]: (ApiContext) -> ApiResponse }

local function route(method: api.ApiMethod, path: string, handler: (ApiContext) -> ApiResponse)
	module.handlers[`{method} {path}`] = handler
	api.route(method, P .. path, handler, "public", true)
end

function module.register(session_entries: { [string]: SessionEntry }, owned_session_resolver: OwnedSessionResolver)
	sessions = session_entries
	get_owned_session = owned_session_resolver
	route("GET", "/SyncPlay/List", function(ctx: ApiContext)
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
	end)

	route("GET", "/SyncPlay/{id}", function(ctx: ApiContext)
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
	end)

	route("POST", "/SyncPlay/New", function(ctx: ApiContext)
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
	end)

	route("POST", "/SyncPlay/Join", function(ctx: ApiContext)
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
			(entry :: SessionEntry).session_id,
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
				syncplay_send_entry(entry :: SessionEntry, "SyncPlayCommand", syncplay_command(group, "Unpause"))
			elseif group.state == "Paused" then
				syncplay_send_entry(entry :: SessionEntry, "SyncPlayCommand", syncplay_command(group, "Pause"))
			end
		end
		return api.response.empty(204)
	end)

	route("POST", "/SyncPlay/Leave", function(ctx: ApiContext)
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
	end)

	route("POST", "/SyncPlay/SetNewQueue", function(ctx: ApiContext)
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
		group_value.run_time_ticks = syncplay_current_run_time(group_value)
		syncplay_set_position(group_value, start_ticks)
		group_value.resume_playing = true
		syncplay_set_state(group_value, "Waiting")
		syncplay_set_all_buffering(group_value, true)
		syncplay_broadcast_queue_update(group_value, (entry :: SessionEntry).session_id, "NewPlaylist")
		syncplay_broadcast_state(group_value, (entry :: SessionEntry).session_id, "Play")
		return api.response.empty(204)
	end)

	route("POST", "/SyncPlay/SetPlaylistItem", function(ctx: ApiContext)
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
			syncplay_restart_item(group_value)
			group_value.resume_playing = true
			syncplay_set_state(group_value, "Waiting")
			syncplay_set_all_buffering(group_value, true)
			syncplay_broadcast_queue_update(group_value, (entry :: SessionEntry).session_id, "SetCurrentItem")
			syncplay_broadcast_state(group_value, (entry :: SessionEntry).session_id, "SetPlaylistItem")
		end
		return api.response.empty(204)
	end)

	route("POST", "/SyncPlay/RemoveFromPlaylist", function(ctx: ApiContext)
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
				syncplay_restart_item(group_value)
				syncplay_set_state(group_value, "Idle")
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
				syncplay_restart_item(group_value)
			else
				group_value.current_index = -1
				syncplay_restart_item(group_value)
				syncplay_set_state(group_value, "Idle")
			end
		end

		syncplay_broadcast_queue_update(group_value, (entry :: SessionEntry).session_id, "RemoveItems")
		if group_value.state == "Idle" then
			syncplay_broadcast_command(group_value, (entry :: SessionEntry).session_id, "all", "Stop")
			syncplay_broadcast_state(group_value, (entry :: SessionEntry).session_id, "Stop")
		end
		return api.response.empty(204)
	end)

	route("POST", "/SyncPlay/MovePlaylistItem", function(ctx: ApiContext)
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
		syncplay_broadcast_queue_update(group_value, (entry :: SessionEntry).session_id, "MoveItem")
		return api.response.empty(204)
	end)

	route("POST", "/SyncPlay/Queue", function(ctx: ApiContext)
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
			(entry :: SessionEntry).session_id,
			if mode == "QueueNext" then "QueueNext" else "Queue"
		)
		return api.response.empty(204)
	end)

	route("POST", "/SyncPlay/Unpause", function(ctx: ApiContext)
		local _principal, entry, group, group_err = syncplay_require_group(ctx)
		if group_err then
			return group_err
		end

		local group_value = group :: SyncPlayGroup
		if group_value.state == "Playing" then
			-- As upstream, the group is already playing and the member only lost track of it.
			syncplay_send_entry(entry :: SessionEntry, "SyncPlayCommand", syncplay_command(group_value, "Unpause"))
		else
			syncplay_unpause(group_value, (entry :: SessionEntry).session_id)
		end
		return api.response.empty(204)
	end)

	route("POST", "/SyncPlay/Pause", function(ctx: ApiContext)
		local _principal, entry, group, group_err = syncplay_require_group(ctx)
		if group_err then
			return group_err
		end

		syncplay_pause(group :: SyncPlayGroup, (entry :: SessionEntry).session_id)
		return api.response.empty(204)
	end)

	route("POST", "/SyncPlay/Stop", function(ctx: ApiContext)
		local _principal, entry, group, group_err = syncplay_require_group(ctx)
		if group_err then
			return group_err
		end

		local group_value = group :: SyncPlayGroup
		syncplay_set_state(group_value, "Idle")
		group_value.resume_playing = false
		syncplay_set_position(group_value, 0)
		syncplay_broadcast_command(group_value, (entry :: SessionEntry).session_id, "all", "Stop")
		syncplay_broadcast_state(group_value, (entry :: SessionEntry).session_id, "Stop")
		return api.response.empty(204)
	end)

	route("POST", "/SyncPlay/Seek", function(ctx: ApiContext)
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
		syncplay_set_position(group_value, syncplay_sanitize_position(group_value, position_ticks))
		syncplay_broadcast_command(group_value, (entry :: SessionEntry).session_id, "all", "Seek")
		syncplay_broadcast_state(group_value, (entry :: SessionEntry).session_id, "Seek")
		return api.response.empty(204)
	end)

	route("POST", "/SyncPlay/Buffering", function(ctx: ApiContext)
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
		if group_value.state == "Idle" then
			syncplay_send_entry(entry :: SessionEntry, "SyncPlayCommand", syncplay_command(group_value, "Stop"))
			return api.response.empty(204)
		end

		-- As upstream, a playing group stops where it is and resumes once everyone is ready; a paused
		-- one stays paused.
		local previous_state = group_value.state
		if previous_state == "Playing" then
			syncplay_settle(group_value)
			group_value.resume_playing = true
		elseif previous_state == "Paused" then
			group_value.resume_playing = false
		end
		syncplay_set_state(group_value, "Waiting")

		local session_id = (entry :: SessionEntry).session_id
		local participant = group_value.participants[session_id]
		if participant then
			participant.buffering = true
			participant.ready = false
		end
		syncplay_update_wait(group_value, false)
		if previous_state == "Playing" then
			syncplay_broadcast_command(group_value, session_id, "ready", "Pause")
		elseif not group_value.resume_playing then
			syncplay_send_entry(entry :: SessionEntry, "SyncPlayCommand", syncplay_command(group_value, "Pause"))
		end
		syncplay_broadcast_state(group_value, session_id, "Buffer")
		return api.response.empty(204)
	end)

	route("POST", "/SyncPlay/Ready", function(ctx: ApiContext)
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
			syncplay_update_wait(group_value, false)
			return api.response.empty(204)
		end

		local participant = group_value.participants[(entry :: SessionEntry).session_id]
		local is_playing = syncplay_bool(body.IsPlaying, group_value.resume_playing)
		local position_ticks = syncplay_number(body.PositionTicks, nil)

		-- How far the member is behind the group, counting the time since it reported, unless its
		-- clock is too far off to trust.
		local reported_ms = module.parse_timestamp(body.When)
		local elapsed_ms = if is_playing and reported_ms then syncplay_now_ms() - reported_ms else 0
		if math.abs(elapsed_ms) > SYNCPLAY_TIME_SYNC_OFFSET then
			elapsed_ms = 0
		end
		local group_ticks = syncplay_position_ticks(group_value)
		local client_ticks = syncplay_sanitize_position(group_value, position_ticks or group_ticks)
		local lag_ms = (group_ticks - client_ticks) / TICKS_PER_MILLISECOND - elapsed_ms

		if participant then
			participant.buffering = false
			participant.ready = true
		end
		syncplay_update_wait(group_value, false)
		syncplay_transition_if_ready(group_value, (entry :: SessionEntry).session_id, is_playing, lag_ms)
		return api.response.empty(204)
	end)

	route("POST", "/SyncPlay/SetIgnoreWait", function(ctx: ApiContext)
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
		syncplay_update_wait(group_value, false)
		if group_value.state == "Waiting" then
			syncplay_transition_if_ready(group_value, (entry :: SessionEntry).session_id, nil, nil)
		end
		return api.response.empty(204)
	end)

	route("POST", "/SyncPlay/NextItem", function(ctx: ApiContext)
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
		syncplay_restart_item(group_value)
		syncplay_set_state(group_value, "Waiting")
		group_value.resume_playing = true
		syncplay_set_all_buffering(group_value, true)
		syncplay_broadcast_queue_update(group_value, (entry :: SessionEntry).session_id, "NextItem")
		syncplay_broadcast_state(group_value, (entry :: SessionEntry).session_id, "NextItem")
		return api.response.empty(204)
	end)

	route("POST", "/SyncPlay/PreviousItem", function(ctx: ApiContext)
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
		syncplay_restart_item(group_value)
		syncplay_set_state(group_value, "Waiting")
		group_value.resume_playing = true
		syncplay_set_all_buffering(group_value, true)
		syncplay_broadcast_queue_update(group_value, (entry :: SessionEntry).session_id, "PreviousItem")
		syncplay_broadcast_state(group_value, (entry :: SessionEntry).session_id, "PreviousItem")
		return api.response.empty(204)
	end)

	route("POST", "/SyncPlay/SetRepeatMode", function(ctx: ApiContext)
		local _principal, entry, group, group_err = syncplay_require_group(ctx)
		if group_err then
			return group_err
		end

		local body = syncplay_body(ctx)
		local group_value = group :: SyncPlayGroup
		group_value.repeat_mode = syncplay_enum(body.Mode, SYNCPLAY_REPEAT_MODES, group_value.repeat_mode)
		syncplay_broadcast_queue_update(group_value, (entry :: SessionEntry).session_id, "RepeatMode")
		return api.response.empty(204)
	end)

	route("POST", "/SyncPlay/SetShuffleMode", function(ctx: ApiContext)
		local _principal, entry, group, group_err = syncplay_require_group(ctx)
		if group_err then
			return group_err
		end

		local body = syncplay_body(ctx)
		local group_value = group :: SyncPlayGroup
		group_value.shuffle_mode = syncplay_enum(body.Mode, SYNCPLAY_SHUFFLE_MODES, group_value.shuffle_mode)
		syncplay_broadcast_queue_update(group_value, (entry :: SessionEntry).session_id, "ShuffleMode")
		return api.response.empty(204)
	end)

	route("POST", "/SyncPlay/Ping", function(ctx: ApiContext)
		local _principal, entry, group, group_err = syncplay_require_group(ctx)
		if group_err then
			return group_err
		end

		local body = syncplay_body(ctx)
		local participant = (group :: SyncPlayGroup).participants[(entry :: SessionEntry).session_id]
		if participant then
			local ping = syncplay_number(body.Ping, participant.ping) or participant.ping
			participant.ping = math.clamp(ping, 0, SYNCPLAY_MAX_PING)
		end
		return api.response.empty(204)
	end)
end

return module
